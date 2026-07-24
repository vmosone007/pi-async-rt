#![feature(test)]

extern crate test;

use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder, StealableTaskPool},
    AsyncRuntime,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc, Arc, Barrier, Condvar, Mutex,
    },
    task::{Context, Poll},
    thread,
    time::{Duration, Instant},
};
use test::{black_box, Bencher};

const WORKER_SIZE: usize = 8;
const PRODUCER_SIZE: usize = 8;
const SLEEP_TIMEOUT_MS: u64 = 10;
const DEFAULT_TASKS: usize = 100_000;
const DEFAULT_IN_FLIGHT: usize = 32_768;
const DEFAULT_SAMPLES: usize = 2048;
const HARNESS_TASKS: usize = 1000;
const WORKLOAD_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone, Copy)]
enum WorkloadKind {
    Ready,
    YieldOnce,
    WakeBurst8,
    WakeBurst64,
}

impl WorkloadKind {
    fn name(self) -> &'static str {
        match self {
            WorkloadKind::Ready => "ready",
            WorkloadKind::YieldOnce => "yield_once",
            WorkloadKind::WakeBurst8 => "wake_burst_8",
            WorkloadKind::WakeBurst64 => "wake_burst_64",
        }
    }

    fn expected_polls(self, tasks: usize) -> usize {
        match self {
            WorkloadKind::Ready => tasks,
            WorkloadKind::YieldOnce
            | WorkloadKind::WakeBurst8
            | WorkloadKind::WakeBurst64 => tasks * 2,
        }
    }

    fn wake_count(self) -> usize {
        match self {
            WorkloadKind::Ready => 0,
            WorkloadKind::YieldOnce => 1,
            WorkloadKind::WakeBurst8 => 8,
            WorkloadKind::WakeBurst64 => 64,
        }
    }
}

struct WorkloadState {
    completed: AtomicUsize,
    in_flight: AtomicUsize,
    polls: AtomicUsize,
    done: Mutex<bool>,
    done_cv: Condvar,
}

impl WorkloadState {
    fn new() -> Self {
        WorkloadState {
            completed: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            polls: AtomicUsize::new(0),
            done: Mutex::new(false),
            done_cv: Condvar::new(),
        }
    }
}

/// 用于测量生产自唤醒路径的两次轮询 Future。
///
/// 首次轮询执行一次合法 `wake_by_ref` 并返回 `Pending`；第二次轮询返回 `Ready`。
/// 本 Future 不包含队列或重试逻辑。时间和空间复杂度均为 O(1)，不分配、不阻塞，
/// 全部调度行为均由 `pi_async_rt` 提供。
struct YieldOnce {
    yielded: bool,
    state: Arc<WorkloadState>,
}

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.state.polls.fetch_add(1, Ordering::AcqRel);
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// 用于验证重复唤醒合并能力的两次轮询 Future。
///
/// 本夹具只服务 WakeBurst8/64，不参与单次 self-wake 基线。首次轮询发出固定数量的
/// 合法唤醒并返回 `Pending`，第二次返回 `Ready`。它不提供任何队列、合并或重试
/// 功能，生产运行时必须独立保证重复唤醒只形成一个后续轮询义务。
struct WakeBurst {
    yielded: bool,
    wake_count: usize,
    state: Arc<WorkloadState>,
}

impl Future for WakeBurst {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.state.polls.fetch_add(1, Ordering::AcqRel);
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            for _ in 0..self.wake_count {
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        }
    }
}

struct WorkloadResult {
    elapsed: Duration,
    polls: usize,
    latencies: Vec<Duration>,
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// 构造被测的真实生产运行时。
///
/// 工作线程和提交线程数量固定为 8，以保证可比性。本辅助函数分配真实可窃取队列
/// 并启动真实工作线程，不包含模拟调度器。构造成本有意排除在负载计时之外。
fn new_runtime() -> MultiTaskRuntime<()> {
    let pool = StealableTaskPool::with(WORKER_SIZE, 65_535, [1, 1], 10);
    MultiTaskRuntimeBuilder::new(pool)
        .thread_prefix("Async-Task-Scheduling-Bench")
        .thread_stack_size(2 * 1024 * 1024)
        .init_worker_size(WORKER_SIZE)
        .set_worker_limit(WORKER_SIZE, WORKER_SIZE)
        .set_timeout(SLEEP_TIMEOUT_MS)
        .build()
}

/// 为基准提交线程预留一个在途任务名额。
///
/// `state` 是本轮基准独占的统计状态，`maximum` 是严格大于零的在途任务上限。
/// 未达到上限时，本函数通过弱比较交换把计数准确增加一；达到上限时只读取计数
/// 并让出当前操作系统线程，避免多个提交线程在同一原子变量上持续执行无效写入。
/// 任务完成路径负责执行一次对应的减一，因此成功返回与后续减一必须一一配对。
///
/// 本函数仅约束基准生成负载的速度，不实现、不模拟也不修补生产运行时的入队、
/// 唤醒或轮询行为。无竞争时的时间复杂度为 O(1)，竞争时重试次数取决于并发调度；
/// 空间复杂度为 O(1)，不分配内存、不持锁、不阻塞在内核等待原语上，但达到上限时
/// 可能反复让出提交线程。它不是纯函数，也不幂等，会修改共享在途计数；依靠原子
/// 读改写保证线程安全，不访问任务对象，不引入额外的异步安全或内存安全边界。
fn reserve_in_flight(state: &WorkloadState, maximum: usize) {
    loop {
        let current = state.in_flight.load(Ordering::Acquire);
        if current >= maximum {
            thread::yield_now();
            continue;
        }
        if state
            .in_flight
            .compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            return;
        }
    }
}

/// 在真实运行时上执行并发外部提交线程。
///
/// 入参：
/// - `total`：逻辑任务的准确数量，必须大于零；
/// - `maximum_in_flight`：尚未完成任务数量的上限；
/// - `sample_count`：端到端延迟的近似采样数量；
/// - `kind`：一次轮询的 `Ready` 任务或两次轮询的自唤醒任务。
///
/// 辅助函数返回墙钟时间、准确 Future 轮询次数和采样的提交到完成延迟。时间
/// 复杂度为 O(total)，基准状态空间为 O(maximum_in_flight + samples)。它会创建真实
/// 任务和提交线程，因此非纯且非幂等。提交线程等待有界且发生在运行时工作线程
/// 之外；完成等待具有硬超时。
fn run_external_workload(
    runtime: &MultiTaskRuntime<()>,
    total: usize,
    maximum_in_flight: usize,
    sample_count: usize,
    kind: WorkloadKind,
) -> WorkloadResult {
    assert!(total > 0);
    let maximum_in_flight = maximum_in_flight.max(PRODUCER_SIZE);
    let state = Arc::new(WorkloadState::new());
    let barrier = Arc::new(Barrier::new(PRODUCER_SIZE + 1));
    let (sample_tx, sample_rx) = mpsc::channel();
    let sample_stride = (total / sample_count.max(1)).max(1);
    let mut producers = Vec::with_capacity(PRODUCER_SIZE);

    for producer_index in 0..PRODUCER_SIZE {
        let runtime = runtime.clone();
        let state = state.clone();
        let barrier = barrier.clone();
        let sample_tx = sample_tx.clone();
        let begin = producer_index * total / PRODUCER_SIZE;
        let end = (producer_index + 1) * total / PRODUCER_SIZE;

        producers.push(thread::spawn(move || {
            barrier.wait();

            for task_index in begin..end {
                reserve_in_flight(&state, maximum_in_flight);
                let task_state = state.clone();
                let sample = if task_index % sample_stride == 0 {
                    Some((sample_tx.clone(), Instant::now()))
                } else {
                    None
                };

                runtime
                    .spawn(async move {
                        match kind {
                            WorkloadKind::Ready => {
                                task_state.polls.fetch_add(1, Ordering::AcqRel);
                            },
                            WorkloadKind::YieldOnce => {
                                YieldOnce {
                                    yielded: false,
                                    state: task_state.clone(),
                                }
                                .await;
                            },
                            WorkloadKind::WakeBurst8
                            | WorkloadKind::WakeBurst64 => {
                                WakeBurst {
                                    yielded: false,
                                    wake_count: kind.wake_count(),
                                    state: task_state.clone(),
                                }
                                .await;
                            },
                        }

                        if let Some((sample_tx, started)) = sample {
                            let _ = sample_tx.send(started.elapsed());
                        }
                        task_state.in_flight.fetch_sub(1, Ordering::AcqRel);
                        if task_state.completed.fetch_add(1, Ordering::AcqRel) + 1 == total {
                            let mut done = task_state.done.lock().unwrap();
                            *done = true;
                            task_state.done_cv.notify_one();
                        }
                    })
                    .unwrap();
            }
        }));
    }

    drop(sample_tx);
    let started = Instant::now();
    barrier.wait();

    let mut done = state.done.lock().unwrap();
    while !*done {
        let (next_done, wait) = state
            .done_cv
            .wait_timeout(done, WORKLOAD_TIMEOUT)
            .unwrap();
        done = next_done;
        if wait.timed_out() {
            panic!(
                "async task scheduling benchmark timed out: kind={}, completed={}, total={}",
                kind.name(),
                state.completed.load(Ordering::Acquire),
                total
            );
        }
    }
    drop(done);

    for producer in producers {
        producer.join().unwrap();
    }

    let mut latencies = Vec::with_capacity(sample_count.min(total));
    while let Ok(latency) = sample_rx.try_recv() {
        latencies.push(latency);
    }

    WorkloadResult {
        elapsed: started.elapsed(),
        polls: state.polls.load(Ordering::Acquire),
        latencies,
    }
}

async fn reserve_internal_in_flight(
    runtime: &MultiTaskRuntime<()>,
    state: &WorkloadState,
    maximum: usize,
) {
    loop {
        let previous = state.in_flight.fetch_add(1, Ordering::AcqRel);
        if previous < maximum {
            return;
        }
        state.in_flight.fetch_sub(1, Ordering::AcqRel);
        runtime.yield_now().await;
    }
}

/// 在生产运行时内部执行 8 个提交 Future。
///
/// 每个提交 Future 通过 `spawn_local` 提交子任务，因此本地/内部路由、工作线程
/// 调度、唤醒合并和轮询均由运行时负责。基准辅助函数只限制未完成子任务数量并记录
/// 完成。时间复杂度为 O(total)，状态空间为 O(maximum_in_flight + samples)，工作线程
/// 上不执行阻塞操作。资源竞争使用生产 `yield_now`。
fn run_internal_workload(
    runtime: &MultiTaskRuntime<()>,
    total: usize,
    maximum_in_flight: usize,
    sample_count: usize,
    kind: WorkloadKind,
) -> WorkloadResult {
    assert!(total > 0);
    let maximum_in_flight = maximum_in_flight.max(PRODUCER_SIZE);
    let state = Arc::new(WorkloadState::new());
    let start = Arc::new(AtomicBool::new(false));
    let (sample_tx, sample_rx) = mpsc::channel();
    let (producer_done_tx, producer_done_rx) = mpsc::channel();
    let sample_stride = (total / sample_count.max(1)).max(1);

    for producer_index in 0..PRODUCER_SIZE {
        let runtime_for_root = runtime.clone();
        let state = state.clone();
        let start = start.clone();
        let sample_tx = sample_tx.clone();
        let producer_done_tx = producer_done_tx.clone();
        let begin = producer_index * total / PRODUCER_SIZE;
        let end = (producer_index + 1) * total / PRODUCER_SIZE;

        runtime
            .spawn(async move {
                while !start.load(Ordering::Acquire) {
                    runtime_for_root.yield_now().await;
                }

                for task_index in begin..end {
                    reserve_internal_in_flight(
                        &runtime_for_root,
                        &state,
                        maximum_in_flight,
                    )
                    .await;
                    let task_state = state.clone();
                    let sample = if task_index % sample_stride == 0 {
                        Some((sample_tx.clone(), Instant::now()))
                    } else {
                        None
                    };

                    runtime_for_root
                        .spawn_local(async move {
                            match kind {
                                WorkloadKind::Ready => {
                                    task_state.polls.fetch_add(1, Ordering::AcqRel);
                                },
                                WorkloadKind::YieldOnce => {
                                    YieldOnce {
                                        yielded: false,
                                        state: task_state.clone(),
                                    }
                                    .await;
                                },
                                WorkloadKind::WakeBurst8
                                | WorkloadKind::WakeBurst64 => {
                                    WakeBurst {
                                        yielded: false,
                                        wake_count: kind.wake_count(),
                                        state: task_state.clone(),
                                    }
                                    .await;
                                },
                            }

                            if let Some((sample_tx, started)) = sample {
                                let _ = sample_tx.send(started.elapsed());
                            }
                            task_state.in_flight.fetch_sub(1, Ordering::AcqRel);
                            if task_state.completed.fetch_add(1, Ordering::AcqRel) + 1 == total {
                                let mut done = task_state.done.lock().unwrap();
                                *done = true;
                                task_state.done_cv.notify_one();
                            }
                        })
                        .unwrap();
                }

                producer_done_tx.send(()).unwrap();
            })
            .unwrap();
    }

    drop(sample_tx);
    drop(producer_done_tx);
    let started = Instant::now();
    start.store(true, Ordering::Release);

    let mut done = state.done.lock().unwrap();
    while !*done {
        let (next_done, wait) = state
            .done_cv
            .wait_timeout(done, WORKLOAD_TIMEOUT)
            .unwrap();
        done = next_done;
        if wait.timed_out() {
            panic!(
                "internal async task scheduling benchmark timed out: kind={}, completed={}, total={}",
                kind.name(),
                state.completed.load(Ordering::Acquire),
                total
            );
        }
    }
    drop(done);

    for _ in 0..PRODUCER_SIZE {
        producer_done_rx.recv_timeout(WORKLOAD_TIMEOUT).unwrap();
    }

    let mut latencies = Vec::with_capacity(sample_count.min(total));
    while let Ok(latency) = sample_rx.try_recv() {
        latencies.push(latency);
    }

    WorkloadResult {
        elapsed: started.elapsed(),
        polls: state.polls.load(Ordering::Acquire),
        latencies,
    }
}

fn percentile(sorted: &[Duration], percentile: usize, denominator: usize) -> Duration {
    let index = ((sorted.len() - 1) * percentile / denominator).min(sorted.len() - 1);
    sorted[index]
}

fn print_result(
    source: &str,
    kind: WorkloadKind,
    total: usize,
    result: &mut WorkloadResult,
) {
    result.latencies.sort_unstable();
    let tasks_per_second = total as f64 / result.elapsed.as_secs_f64();
    let nanoseconds_per_task = result.elapsed.as_nanos() as f64 / total as f64;
    let expected_polls = kind.expected_polls(total);

    assert_eq!(
        result.polls,
        expected_polls,
        "unexpected future poll count for {} workload",
        kind.name()
    );

    println!(
        "async_task_scheduling source={} kind={} workers={} producers={} tasks={} elapsed={:?} \
         tasks_per_second={:.0} nanoseconds_per_task={:.2} polls={} polls_per_task={:.2} \
         wakes_per_task={}",
        source,
        kind.name(),
        WORKER_SIZE,
        PRODUCER_SIZE,
        total,
        result.elapsed,
        tasks_per_second,
        nanoseconds_per_task,
        result.polls,
        result.polls as f64 / total as f64,
        kind.wake_count()
    );

    if !result.latencies.is_empty() {
        println!(
            "async_task_scheduling_latency source={} kind={} samples={} p50={:?} p90={:?} \
             p99={:?} p99_9={:?} max={:?}",
            source,
            kind.name(),
            result.latencies.len(),
            percentile(&result.latencies, 50, 100),
            percentile(&result.latencies, 90, 100),
            percentile(&result.latencies, 99, 100),
            percentile(&result.latencies, 999, 1000),
            result.latencies[result.latencies.len() - 1]
        );
    }
}

fn run_bencher(b: &mut Bencher, kind: WorkloadKind) {
    let runtime = new_runtime();

    b.iter(|| {
        let result = run_external_workload(
            &runtime,
            HARNESS_TASKS,
            DEFAULT_IN_FLIGHT.min(HARNESS_TASKS),
            DEFAULT_SAMPLES.min(HARNESS_TASKS),
            kind,
        );
        black_box((result.elapsed, result.polls, result.latencies.len()));
    });

    let total = env_usize("PI_ASYNC_RT_TASK_SCHEDULING_TASKS", DEFAULT_TASKS);
    let maximum_in_flight = env_usize(
        "PI_ASYNC_RT_TASK_SCHEDULING_IN_FLIGHT",
        DEFAULT_IN_FLIGHT,
    );
    let sample_count = env_usize(
        "PI_ASYNC_RT_TASK_SCHEDULING_SAMPLES",
        DEFAULT_SAMPLES,
    );
    let mut result = run_external_workload(
        &runtime,
        total,
        maximum_in_flight,
        sample_count,
        kind,
    );
    print_result("external", kind, total, &mut result);
    let _ = runtime.close();
}

fn run_internal_bencher(b: &mut Bencher, kind: WorkloadKind) {
    let runtime = new_runtime();

    b.iter(|| {
        let result = run_internal_workload(
            &runtime,
            HARNESS_TASKS,
            DEFAULT_IN_FLIGHT.min(HARNESS_TASKS),
            DEFAULT_SAMPLES.min(HARNESS_TASKS),
            kind,
        );
        black_box((result.elapsed, result.polls, result.latencies.len()));
    });

    let total = env_usize("PI_ASYNC_RT_TASK_SCHEDULING_TASKS", DEFAULT_TASKS);
    let maximum_in_flight = env_usize(
        "PI_ASYNC_RT_TASK_SCHEDULING_IN_FLIGHT",
        DEFAULT_IN_FLIGHT,
    );
    let sample_count = env_usize(
        "PI_ASYNC_RT_TASK_SCHEDULING_SAMPLES",
        DEFAULT_SAMPLES,
    );
    let mut result = run_internal_workload(
        &runtime,
        total,
        maximum_in_flight,
        sample_count,
        kind,
    );
    print_result("internal", kind, total, &mut result);
    let _ = runtime.close();
}

#[bench]
fn bench_multi_thread_external_ready_task_8x8(b: &mut Bencher) {
    run_bencher(b, WorkloadKind::Ready);
}

#[bench]
fn bench_multi_thread_external_yield_once_task_8x8(b: &mut Bencher) {
    run_bencher(b, WorkloadKind::YieldOnce);
}

#[bench]
fn bench_multi_thread_internal_ready_task_8x8(b: &mut Bencher) {
    run_internal_bencher(b, WorkloadKind::Ready);
}

#[bench]
fn bench_multi_thread_internal_yield_once_task_8x8(b: &mut Bencher) {
    run_internal_bencher(b, WorkloadKind::YieldOnce);
}

#[bench]
fn bench_multi_thread_external_wake_burst_8_task_8x8(b: &mut Bencher) {
    run_bencher(b, WorkloadKind::WakeBurst8);
}

#[bench]
fn bench_multi_thread_external_wake_burst_64_task_8x8(b: &mut Bencher) {
    run_bencher(b, WorkloadKind::WakeBurst64);
}
