#![cfg(not(feature = "serial"))]

//! `StealableTaskPool` 共享刷新状态和 multi-thread worker owner 协议的独立标准专项测试。
//!
//! 被测生产入口：
//! - `StealableTaskPool::with`、`MultiTaskRuntimeBuilder::build`；
//! - `AsyncTaskPool::{push_local, try_pop, try_pop_all}`；
//! - `AsyncTaskPoolExt::clone_thread_waker`；
//! - timer/no-timer multi-thread worker 的真实 `try_pop_by_weight` 调用链。
//!
//! 测试边界：
//! - 只通过公开 runtime/pool API 驱动生产实现，不复制刷新或 owner 校验逻辑；
//! - 不使用 `block_on`，异步工作只由 runtime `spawn`/`spawn_local` 推进；
//! - channel 只负责测试进程观察完成状态，不参与生产侧调度或唤醒协议；
//! - 所有等待均有硬截止；任务必须 exact-once，超时、重复、丢失和 worker panic 都失败；
//! - test harness 必须使用 `--test-threads=1`，测试内部并发是被测条件而不是 harness 并发。
//!
//! 安全工具：
//! `test_stealable_pool_refresh_concurrency_exact_once` 是正确 ABI TSan 的上游独立复现入口。
//! 普通构建验证功能，TSan 构建验证共享访问不存在 data race；两者不能互相替代。

use crossbeam_utils::atomic::AtomicCell;
use pi_async_rt::rt::{
    multi_thread::{
        ComputationalTaskPool, MultiTaskRuntime, MultiTaskRuntimeBuilder, StealableTaskPool,
    },
    startup_global_time_loop, AsyncRuntime, AsyncTaskPool, AsyncTaskPoolExt,
};
use quanta::Instant as QInstant;
use std::{
    collections::HashSet,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
        mpsc,
        Arc, Barrier, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

static CONCURRENCY_TEST_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

/// 构建真实、固定 worker 数的可窃取多线程 runtime。
///
/// `timer_interval` 为 `Some` 时覆盖 `timer_work_loop`，为 `None` 时覆盖 `work_loop`。
/// 构建会启动 worker 线程，时间/空间复杂度均为 O(W)，W 为 `worker_size`。本 helper 不实现
/// 任何生产调度或同步；它只固定公开 builder 参数，且不阻塞等待任务完成。
fn new_stealable_runtime(
    worker_size: usize,
    refresh_interval_ms: usize,
    timer_interval: Option<usize>,
) -> MultiTaskRuntime<()> {
    let pool = StealableTaskPool::with(worker_size, 4096, [1, 1], refresh_interval_ms);
    let mut builder = MultiTaskRuntimeBuilder::new(pool)
        .thread_prefix("Stealable-Concurrency-Test")
        .thread_stack_size(2 * 1024 * 1024)
        .init_worker_size(worker_size)
        .set_worker_limit(worker_size, worker_size)
        .set_timeout(100);
    if let Some(interval) = timer_interval {
        builder = builder.set_timer_interval(interval);
    }

    builder.build()
}

/// 构建真实、固定 worker 数的计算型多线程 runtime，用于复验共享 owner helper 的另一实现。
///
/// `ComputationalTaskPool::new` 可能按既有语义提高过小的请求值，因此 builder 必须读取生产
/// pool 的 `worker_len()` 并启动等量 worker，禁止制造存在无人消费 slot 的无效夹具。该 helper
/// 仅调用生产构造器/builder；无测试侧队列、worker 或 owner 逻辑。复杂度 O(W)，会创建 W 个
/// OS worker，除此之外不阻塞、不执行用户 future。
fn new_computational_runtime(
    worker_size: usize,
) -> MultiTaskRuntime<(), ComputationalTaskPool<()>> {
    let pool = ComputationalTaskPool::new(worker_size);
    let actual_worker_size = pool.worker_len();
    MultiTaskRuntimeBuilder::new(pool)
        .thread_prefix("Computational-Owner-Test")
        .thread_stack_size(2 * 1024 * 1024)
        .init_worker_size(actual_worker_size)
        .set_worker_limit(actual_worker_size, actual_worker_size)
        .set_timeout(10_000)
        .build()
}

/// 由多个外部线程并发提交空任务，并客观验证每个任务只执行一次。
///
/// 返回采样到的生产 worker 名称集合。测试状态只在任务完成后记录，不替代 runtime 队列。
/// 时间复杂度 O(N)，空间复杂度 O(N)，N 为 `total`；每个等待都有 `TEST_TIMEOUT` 硬截止。
fn run_external_exact_once(
    runtime: &MultiTaskRuntime<()>,
    total: usize,
    producer_threads: usize,
) -> HashSet<String> {
    let seen = Arc::new((0..total).map(|_| AtomicU8::new(0)).collect::<Vec<_>>());
    let completed = Arc::new(AtomicUsize::new(0));
    let duplicate_count = Arc::new(AtomicUsize::new(0));
    let worker_names = Arc::new(Mutex::new(HashSet::new()));
    let barrier = Arc::new(Barrier::new(producer_threads + 1));
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    let sample_stride = (total / 128).max(1);
    let mut producers = Vec::with_capacity(producer_threads);

    for producer_index in 0..producer_threads {
        let runtime = runtime.clone();
        let seen = seen.clone();
        let completed = completed.clone();
        let duplicate_count = duplicate_count.clone();
        let worker_names = worker_names.clone();
        let barrier = barrier.clone();
        let done_tx = done_tx.clone();
        let begin = producer_index * total / producer_threads;
        let end = (producer_index + 1) * total / producer_threads;

        producers.push(thread::spawn(move || {
            barrier.wait();
            for task_index in begin..end {
                let seen = seen.clone();
                let completed = completed.clone();
                let duplicate_count = duplicate_count.clone();
                let worker_names = worker_names.clone();
                let done_tx = done_tx.clone();
                runtime
                    .spawn(async move {
                        if seen[task_index].fetch_add(1, Ordering::AcqRel) != 0 {
                            duplicate_count.fetch_add(1, Ordering::Relaxed);
                        }
                        if task_index % sample_stride == 0 {
                            let name = thread::current().name().unwrap_or("unnamed").to_string();
                            worker_names.lock().unwrap().insert(name);
                        }
                        if completed.fetch_add(1, Ordering::AcqRel) + 1 == total {
                            let _ = done_tx.send(());
                        }
                    })
                    .unwrap();
            }
        }));
    }

    barrier.wait();
    for producer in producers {
        producer.join().unwrap();
    }
    done_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("external exact-once workload timed out");

    assert_eq!(completed.load(Ordering::Acquire), total);
    assert_eq!(duplicate_count.load(Ordering::Acquire), 0);
    assert!(seen.iter().all(|entry| entry.load(Ordering::Acquire) == 1));

    let observed_workers = worker_names.lock().unwrap().clone();
    observed_workers
}

/// 在 runtime worker 内通过生产 `spawn_local` 产生 internal 任务并验证 exact-once。
///
/// 根任务仅负责调用公开 API；`yield_now` 让根任务协作式推进，不使用同步阻塞等待。完成 channel
/// 只观察最终子任务计数。时间复杂度 O(N)，空间复杂度 O(N)，测试本身不实现 internal queue。
fn run_internal_exact_once(
    runtime: &MultiTaskRuntime<()>,
    total: usize,
    root_tasks: usize,
) -> HashSet<String> {
    let seen = Arc::new((0..total).map(|_| AtomicU8::new(0)).collect::<Vec<_>>());
    let completed = Arc::new(AtomicUsize::new(0));
    let duplicate_count = Arc::new(AtomicUsize::new(0));
    let armed = Arc::new(AtomicUsize::new(0));
    let start = Arc::new(AtomicBool::new(false));
    let worker_names = Arc::new(Mutex::new(HashSet::new()));
    let (done_tx, done_rx) = mpsc::sync_channel(1);
    let sample_stride = (total / 128).max(1);

    for root_index in 0..root_tasks {
        let runtime_for_root = runtime.clone();
        let seen = seen.clone();
        let completed = completed.clone();
        let duplicate_count = duplicate_count.clone();
        let armed = armed.clone();
        let start = start.clone();
        let worker_names = worker_names.clone();
        let done_tx = done_tx.clone();
        let begin = root_index * total / root_tasks;
        let end = (root_index + 1) * total / root_tasks;

        runtime
            .spawn(async move {
                armed.fetch_add(1, Ordering::Release);
                while !start.load(Ordering::Acquire) {
                    runtime_for_root.yield_now().await;
                }

                for task_index in begin..end {
                    let seen = seen.clone();
                    let completed = completed.clone();
                    let duplicate_count = duplicate_count.clone();
                    let worker_names = worker_names.clone();
                    let done_tx = done_tx.clone();
                    runtime_for_root
                        .spawn_local(async move {
                            if seen[task_index].fetch_add(1, Ordering::AcqRel) != 0 {
                                duplicate_count.fetch_add(1, Ordering::Relaxed);
                            }
                            if task_index % sample_stride == 0 {
                                let name =
                                    thread::current().name().unwrap_or("unnamed").to_string();
                                worker_names.lock().unwrap().insert(name);
                            }
                            if completed.fetch_add(1, Ordering::AcqRel) + 1 == total {
                                let _ = done_tx.send(());
                            }
                        })
                        .unwrap();

                    if task_index % 64 == 0 {
                        runtime_for_root.yield_now().await;
                    }
                }
            })
            .unwrap();
    }

    let armed_deadline = Instant::now() + TEST_TIMEOUT;
    while armed.load(Ordering::Acquire) != root_tasks {
        assert!(Instant::now() < armed_deadline, "internal root tasks did not arm");
        thread::yield_now();
    }
    start.store(true, Ordering::Release);

    done_rx
        .recv_timeout(TEST_TIMEOUT)
        .expect("internal exact-once workload timed out");
    assert_eq!(completed.load(Ordering::Acquire), total);
    assert_eq!(duplicate_count.load(Ordering::Acquire), 0);
    assert!(seen.iter().all(|entry| entry.load(Ordering::Acquire) == 1));

    let observed_workers = worker_names.lock().unwrap().clone();
    observed_workers
}

/// `SPC-F-001` 至 `SPC-F-004`：覆盖真实共享刷新窗口、external/internal 和 timer worker。
///
/// 修复前普通构建应满足功能断言，而正确 ABI TSan 应在相同生产调用链报告 `last_time` race；
/// 修复后普通构建和 TSan 都必须通过。至少观察两个真实 worker，避免退化成单 worker 伪验证。
#[test]
fn test_stealable_pool_refresh_concurrency_exact_once() {
    let _test_lock = CONCURRENCY_TEST_LOCK.lock();
    let _time_loop = startup_global_time_loop(1);
    let runtime = new_stealable_runtime(8, 1, Some(1));

    thread::sleep(Duration::from_millis(20));
    let external_workers = run_external_exact_once(&runtime, 16_384, 8);
    let internal_workers = run_internal_exact_once(&runtime, 8_192, 32);

    assert!(
        external_workers.len() >= 2,
        "external workload used fewer than two workers: {:?}",
        external_workers
    );
    assert!(
        internal_workers.len() >= 2,
        "internal workload used fewer than two workers: {:?}",
        internal_workers
    );
}

/// `SPC-F-005` 至 `SPC-F-008`：验证 pool owner fail-fast 与合法跨 runtime fallback。
///
/// wrong-pool 调用只执行到 production owner guard；`catch_unwind` 仅观察 panic，不提供保护逻辑。
/// 合法 `runtime_b.spawn_local` 必须继续在 B 的公共队列执行。Stealable 与 Computational 两种
/// 内置 multi-thread pool 都覆盖，防止共享 helper 只修一条静态分支。
#[test]
fn test_multi_thread_pool_owner_guard_and_cross_runtime_fallback() {
    let _test_lock = CONCURRENCY_TEST_LOCK.lock();
    let runtime_a = new_stealable_runtime(1, 3000, None);
    let runtime_b = new_stealable_runtime(1, 3000, None);
    let pool_b = runtime_b.shared_pool();
    let (guard_tx, guard_rx) = mpsc::sync_channel(1);

    assert_eq!(pool_b.get_thread_id(), usize::MAX);
    let (thread_id_tx, thread_id_rx) = mpsc::sync_channel(1);
    let pool_b_for_thread_id = pool_b.clone();
    runtime_b
        .spawn(async move {
            thread_id_tx.send(pool_b_for_thread_id.get_thread_id()).unwrap();
        })
        .unwrap();
    let packed_thread_id = thread_id_rx.recv_timeout(TEST_TIMEOUT).unwrap();
    assert_eq!(packed_thread_id >> 32, runtime_b.get_id());

    thread::sleep(Duration::from_millis(20));
    runtime_a
        .spawn(async move {
            let pop_panicked = catch_unwind(AssertUnwindSafe(|| {
                let _ = pool_b.try_pop();
            }))
            .is_err();
            let pop_all_panicked = catch_unwind(AssertUnwindSafe(|| {
                let _ = pool_b.try_pop_all();
            }))
            .is_err();
            let waker_panicked = catch_unwind(AssertUnwindSafe(|| {
                let _ = pool_b.clone_thread_waker();
            }))
            .is_err();
            guard_tx
                .send((pop_panicked, pop_all_panicked, waker_panicked))
                .unwrap();
        })
        .unwrap();
    assert_eq!(
        guard_rx.recv_timeout(TEST_TIMEOUT).unwrap(),
        (true, true, true),
        "StealableTaskPool wrong-owner access was not rejected"
    );

    let (fallback_tx, fallback_rx) = mpsc::sync_channel(1);
    let runtime_b_for_fallback = runtime_b.clone();
    let fallback_pool_b = runtime_b.shared_pool();
    let expected_runtime_b_id = runtime_b.get_id();
    runtime_a
        .spawn(async move {
            runtime_b_for_fallback
                .spawn_local(async move {
                    fallback_tx
                        .send(fallback_pool_b.get_thread_id() >> 32)
                        .unwrap();
                })
                .unwrap();
        })
        .unwrap();
    assert_eq!(
        fallback_rx.recv_timeout(TEST_TIMEOUT).unwrap(),
        expected_runtime_b_id,
        "StealableTaskPool cross-runtime fallback executed on the source runtime"
    );

    let (priority_tx, priority_rx) = mpsc::sync_channel(1);
    let runtime_b_for_priority = runtime_b.clone();
    runtime_b
        .spawn(async move {
            runtime_b_for_priority
                .spawn_priority(usize::MAX, async move {
                    priority_tx.send(()).unwrap();
                })
                .unwrap();
        })
        .unwrap();
    priority_rx.recv_timeout(TEST_TIMEOUT).unwrap();

    let computational_a = new_computational_runtime(1);
    let computational_b = new_computational_runtime(1);
    let computational_pool_b = computational_b.shared_pool();
    let (computational_tx, computational_rx) = mpsc::sync_channel(1);

    assert_eq!(computational_pool_b.get_thread_id(), usize::MAX);
    thread::sleep(Duration::from_millis(20));
    computational_a
        .spawn(async move {
            let pop_panicked = catch_unwind(AssertUnwindSafe(|| {
                let _ = computational_pool_b.try_pop();
            }))
            .is_err();
            let pop_all_panicked = catch_unwind(AssertUnwindSafe(|| {
                let _ = computational_pool_b.try_pop_all();
            }))
            .is_err();
            let waker_panicked = catch_unwind(AssertUnwindSafe(|| {
                let _ = computational_pool_b.clone_thread_waker();
            }))
            .is_err();
            computational_tx
                .send((pop_panicked, pop_all_panicked, waker_panicked))
                .unwrap();
        })
        .unwrap();
    assert_eq!(
        computational_rx.recv_timeout(TEST_TIMEOUT).unwrap(),
        (true, true, true),
        "ComputationalTaskPool wrong-owner access was not rejected"
    );

    let (computational_valid_tx, computational_valid_rx) = mpsc::sync_channel(1);
    let computational_b_for_local = computational_b.clone();
    computational_b
        .spawn(async move {
            computational_b_for_local
                .spawn_local(async move {
                    computational_valid_tx.send(()).unwrap();
                })
                .unwrap();
        })
        .unwrap();
    computational_valid_rx
        .recv_timeout(TEST_TIMEOUT)
        .unwrap();

    let (computational_fallback_tx, computational_fallback_rx) = mpsc::sync_channel(1);
    let computational_b_for_fallback = computational_b.clone();
    let computational_fallback_pool_b = computational_b.shared_pool();
    let expected_computational_b_id = computational_b.get_id();
    computational_a
        .spawn(async move {
            computational_b_for_fallback
                .spawn_local(async move {
                    computational_fallback_tx
                        .send(computational_fallback_pool_b.get_thread_id() >> 32)
                        .unwrap();
                })
                .unwrap();
        })
        .unwrap();
    assert_eq!(
        computational_fallback_rx
            .recv_timeout(TEST_TIMEOUT)
            .unwrap(),
        expected_computational_b_id,
        "ComputationalTaskPool cross-runtime fallback executed on the source runtime"
    );

    let (computational_priority_tx, computational_priority_rx) = mpsc::sync_channel(1);
    let computational_b_for_priority = computational_b.clone();
    computational_b
        .spawn(async move {
            computational_b_for_priority
                .spawn_priority(usize::MAX, async move {
                    computational_priority_tx.send(()).unwrap();
                })
                .unwrap();
        })
        .unwrap();
    computational_priority_rx
        .recv_timeout(TEST_TIMEOUT)
        .unwrap();
}

/// `SPC-F-001` 至 `SPC-F-004`、`SPC-F-011`：覆盖 1/2/4/8 worker 与 no-timer 工作循环。
///
/// 每轮都通过真实 external `spawn` 和 worker 内 `spawn_local` 提交 exact-once 任务；不同 worker
/// 数的 runtime 顺序创建，验证 TLS pool identity 不会串用。测试不模拟 worker 退出或调度，
/// 不要求 OS 在短 burst 中平均分配任务，只要求无丢失、重复、panic 和永久 pending。
#[test]
fn test_stealable_pool_worker_count_matrix_without_timer() {
    let _test_lock = CONCURRENCY_TEST_LOCK.lock();

    for worker_size in [1usize, 2, 4, 8] {
        let runtime = new_stealable_runtime(worker_size, 3000, None);
        thread::sleep(Duration::from_millis(10));

        let external_workers = run_external_exact_once(&runtime, 2_048, worker_size.min(8));
        let internal_workers =
            run_internal_exact_once(&runtime, 1_024, (worker_size * 2).max(1));

        assert!(!external_workers.is_empty());
        assert!(!internal_workers.is_empty());
    }
}

/// `SPC-F-005`：两个独立 pool/runtime 在并行 external burst 下状态不得交叉。
///
/// 两个 OS 驱动线程各自调用同一个公开负载 helper；任务标记、完成 channel 和 runtime 完全
/// 独立。该用例验证真实并行执行及 exact-once，不通过测试夹具实现 pool 隔离。
#[test]
fn test_two_stealable_runtimes_run_independently_in_parallel() {
    let _test_lock = CONCURRENCY_TEST_LOCK.lock();
    let runtime_a = new_stealable_runtime(4, 1, None);
    let runtime_b = new_stealable_runtime(4, 1, None);
    thread::sleep(Duration::from_millis(20));

    let drive_a = thread::spawn(move || run_external_exact_once(&runtime_a, 8_192, 4));
    let drive_b = thread::spawn(move || run_external_exact_once(&runtime_b, 8_192, 4));
    let workers_a = drive_a.join().unwrap();
    let workers_b = drive_b.join().unwrap();

    assert!(!workers_a.is_empty());
    assert!(!workers_b.is_empty());
}

/// `SPC-F-010`：冻结 x86_64 生产目标上的无锁性能前提。
///
/// 该测试验证直接依赖提供的真实 `AtomicCell<QInstant>` 能力，不复制原子实现。它是纯只读、
/// O(1)、无副作用、不阻塞且不分配；非 x86_64 目标只要求正确性，不在本用例承诺 lock-free。
#[test]
fn test_qinstant_atomic_cell_is_lock_free_on_x86_64() {
    let _test_lock = CONCURRENCY_TEST_LOCK.lock();
    #[cfg(target_arch = "x86_64")]
    assert!(AtomicCell::<QInstant>::is_lock_free());
}
