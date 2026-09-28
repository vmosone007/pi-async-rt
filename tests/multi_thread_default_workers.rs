#![cfg(target_os = "linux")]

//! 内置计算型池固定分片与默认 worker 数的真实公开 API 回归。
//!
//! 普通入口逐例创建子进程，因为当前多线程运行时不支持关闭已启动的 worker。
//! 子进程只提交真实异步任务并从 channel 观察结果，不实现任务队列或唤醒逻辑。

use pi_async_rt::rt::{
    multi_thread::{ComputationalTaskPool, MultiTaskRuntime, MultiTaskRuntimeBuilder, StealableTaskPool},
    startup_global_time_loop, AsyncRuntime, AsyncRuntimeBuilder, AsyncTaskPool, AsyncTaskPoolExt,
};
use futures::channel::oneshot;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::ErrorKind,
    process::{Command, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

const PREFIX: &str = "CDW";
const DEADLINE: Duration = Duration::from_secs(5);

/// 与生产默认策略独立地固定公开物理核数口径；Linux 计算型池另有最少两槽位下限。
fn default_workers() -> usize {
    num_cpus::get_physical().saturating_add(1)
}

/// 从真实 Linux 线程清单按名称逐个计数，避免同名线程被集合去重。
fn actual_worker_names() -> BTreeMap<String, usize> {
    let mut names = BTreeMap::new();
    for entry in fs::read_dir("/proc/self/task").unwrap() {
        let entry = entry.unwrap();
        match fs::read_to_string(entry.path().join("comm")) {
            Ok(name) if name.trim().starts_with("CDW-") => {
                *names.entry(name.trim().to_owned()).or_insert(0) += 1;
            }
            Ok(_) => (),
            Err(error) if error.kind() == ErrorKind::NotFound => (),
            Err(error) => panic!("读取 worker 名称失败: {}", error),
        }
    }
    names
}

/// 验证 worker 名称和实际线程数量；等待启动有界，额外线程或重名立即失败。
fn assert_worker_count(workers: usize) -> BTreeSet<String> {
    let expected: BTreeMap<_, _> = (0..workers)
        .map(|index| (format!("{}-{}", PREFIX, index), 1))
        .collect();
    let deadline = Instant::now() + DEADLINE;
    loop {
        let names = actual_worker_names();
        assert!(names.iter().all(|(name, count)| expected.get(name) == Some(count)),
                "出现额外或重名 worker: {:?}", names);
        if names == expected {
            return expected.keys().cloned().collect();
        }
        assert!(Instant::now() < deadline, "实际 worker {:?}，预期 {:?}", names, expected);
        thread::sleep(Duration::from_millis(5));
    }
}

/// 通过真实公开 runtime 提交每槽位一个任务，断言 exact-once、owner 线程与队列归零。
/// `timer` 同时覆盖无定时器 loop 与 `timeout(1)` 的定时器 loop，不替代生产调度。
fn assert_real_progress<P>(runtime: MultiTaskRuntime<(), P>, slots: usize, workers: usize, timer: bool)
where
    P: AsyncTaskPoolExt<()> + AsyncTaskPool<(), Pool = P>,
{
    let _global_time_loop = if timer { startup_global_time_loop(1) } else { None };
    assert_eq!(runtime.worker_len(), slots, "公开 worker_len 返回池槽位数");
    let (sender, receiver) = mpsc::channel();
    for task_id in 0..slots {
        let sender = sender.clone();
        let runtime_for_timeout = runtime.clone();
        runtime
            .spawn(async move {
                if timer && task_id % 2 == 0 {
                    runtime_for_timeout.timeout(1).await;
                }
                sender
                    .send((task_id, thread::current().name().unwrap().to_owned()))
                    .unwrap();
            })
            .unwrap();
    }
    drop(sender);

    let deadline = Instant::now() + DEADLINE;
    let mut completed = BTreeMap::new();
    while completed.len() < slots {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let (task_id, worker) = receiver
            .recv_timeout(remaining)
            .unwrap_or_else(|error| panic!("任务未全部完成: {:?}, 原因: {:?}", completed, error));
        assert!(task_id < slots, "收到非法任务编号");
        assert!(completed.insert(task_id, worker).is_none(), "任务重复完成");
    }
    assert_eq!(completed.keys().copied().collect::<Vec<_>>(), (0..slots).collect::<Vec<_>>());

    let expected_workers = assert_worker_count(workers);
    for worker in completed.values() {
        assert!(expected_workers.contains(worker), "任务在错误线程运行: {}", worker);
    }
    assert_eq!(runtime.len(), 0, "所有入队任务必须被消费");
}

/// 仅构造真实可窃取任务池；该 helper 不创建队列替身或运行任务。
fn stealable(slots: usize) -> StealableTaskPool<()> {
    StealableTaskPool::with(slots, 32, [1, 1], 3000)
}

/// 顺序启动独立子进程，隔离不可关闭的 worker；每例 12 秒硬截止并检查退出码。
#[test]
fn test_all_worker_cases_in_isolated_processes() {
    let cases = [
        "test_case_computational_implicit_default",
        "test_case_computational_default_pool",
        "test_case_computational_zero_fallback",
        "test_case_computational_minimum_and_explicit_size",
        "test_case_computational_explicit_over_capacity_cap",
        "test_case_computational_explicit_underprovision_boundary",
        "test_case_stealable_custom_capacity",
        "test_case_stealable_public_none",
        "test_case_stealable_public_zero",
        "test_case_stealable_public_explicit_one",
        "test_case_concurrent_spawn_timeout_and_cross_runtime_reply",
        "test_case_invalid_stealable_zero_slots",
    ];

    for case in cases {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .arg("--ignored")
            .arg("--exact")
            .arg(case)
            .arg("--test-threads=1")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(12);
        loop {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("{} 超过独立进程硬截止", case);
            }
            thread::sleep(Duration::from_millis(10));
        }
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "{} 失败\nstdout:\n{}\nstderr:\n{}",
            case,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
}

/// 红线：逻辑核数的固定分片池交给默认 builder；当前机器 16 槽位必须驱动 16 worker。
#[test]
#[ignore = "由标准入口在独立进程中执行"]
fn test_case_computational_implicit_default() {
    let slots = num_cpus::get().clamp(2, 32);
    let pool = ComputationalTaskPool::new(slots);
    let slots = pool.worker_len();
    let runtime = MultiTaskRuntimeBuilder::new(pool).thread_prefix(PREFIX).build();
    assert_real_progress(runtime, slots, slots, false);
}

/// 四个外部生产者提交 128 项任务，组合计算型池 timeout 与可窃取池 oneshot 回填。
/// 所有任务必须 exact-once 完成；这是有界功能验证，不采集吞吐或延迟指标。
#[test]
#[ignore = "由标准入口在独立进程中执行"]
fn test_case_concurrent_spawn_timeout_and_cross_runtime_reply() {
    let _global_time_loop = startup_global_time_loop(1);
    let computational = MultiTaskRuntimeBuilder::new(ComputationalTaskPool::<()>::new(4))
        .thread_prefix(PREFIX)
        .set_timer_interval(1)
        .build();
    let remote = MultiTaskRuntimeBuilder::new(stealable(4))
        .thread_prefix("CDR")
        .init_worker_size(4)
        .set_worker_limit(4, 4)
        .build();
    let (sender, receiver) = mpsc::channel();
    let producers: Vec<_> = (0..4)
        .map(|producer| {
            let computational = computational.clone();
            let remote = remote.clone();
            let sender = sender.clone();
            thread::spawn(move || {
                for sequence in 0..32 {
                    let task_id = producer * 32 + sequence;
                    let (reply_sender, reply_receiver) = oneshot::channel();
                    let remote = remote.clone();
                    let computational_for_timeout = computational.clone();
                    let sender = sender.clone();
                    computational
                        .spawn(async move {
                            if task_id % 2 == 0 {
                                computational_for_timeout.timeout(1).await;
                            }
                            remote
                                .spawn(async move {
                                    reply_sender.send(task_id).unwrap();
                                })
                                .unwrap();
                            let result = reply_receiver.await.unwrap();
                            sender
                                .send((result, thread::current().name().unwrap().to_owned()))
                                .unwrap();
                        })
                        .unwrap();
                }
            })
        })
        .collect();
    for producer in producers {
        producer.join().unwrap();
    }
    drop(sender);

    let deadline = Instant::now() + DEADLINE;
    let mut completed = BTreeMap::new();
    while completed.len() < 128 {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let (task_id, worker) = receiver
            .recv_timeout(remaining)
            .unwrap_or_else(|error| panic!("跨运行时回填未完成: {:?}, 原因: {:?}", completed, error));
        assert!(task_id < 128, "收到非法任务编号");
        assert!(completed.insert(task_id, worker).is_none(), "任务重复完成");
    }
    assert_eq!(completed.keys().copied().collect::<Vec<_>>(), (0..128).collect::<Vec<_>>());
    let expected_workers = assert_worker_count(4);
    for worker in completed.values() {
        assert!(expected_workers.contains(worker), "任务在错误线程运行: {}", worker);
    }
    assert_eq!(computational.len(), 0, "计算型池必须全部消费");
    assert_eq!(remote.len(), 0, "回填池必须全部消费");
}

/// 默认计算型池沿用物理核数加一及最少两槽位规则，默认 builder 覆盖全部槽位。
#[test]
#[ignore = "由标准入口在独立进程中执行"]
fn test_case_computational_default_pool() {
    let pool = ComputationalTaskPool::<()>::default();
    let slots = pool.worker_len();
    assert_eq!(slots, default_workers().max(2));
    let runtime = MultiTaskRuntimeBuilder::new(pool).thread_prefix(PREFIX).build();
    assert_real_progress(runtime, slots, slots, false);
}

/// 两次零值设置仍回到固定池槽位数；`set_worker_limit(0, 0)` 后定时器任务也有进度。
#[test]
#[ignore = "由标准入口在独立进程中执行"]
fn test_case_computational_zero_fallback() {
    let slots = num_cpus::get().clamp(2, 32);
    let pool = ComputationalTaskPool::new(slots);
    let slots = pool.worker_len();
    let runtime = MultiTaskRuntimeBuilder::new(pool)
        .thread_prefix(PREFIX)
        .init_worker_size(0)
        .init_worker_size(0)
        .set_worker_limit(0, 0)
        .set_timer_interval(1)
        .build();
    assert_real_progress(runtime, slots, slots, true);
}

/// 非法进度装配只观察原有正数配置，不提交任务，也不把它当作生产 Bug 红线。
#[test]
#[ignore = "由标准入口在独立进程中执行"]
fn test_case_computational_explicit_underprovision_boundary() {
    let runtime = MultiTaskRuntimeBuilder::new(ComputationalTaskPool::<()>::new(4))
        .thread_prefix(PREFIX)
        .init_worker_size(1)
        .set_worker_limit(1, 1)
        .build();
    assert_eq!(runtime.worker_len(), 4, "显式配置不得更改池槽位数");
    assert_worker_count(1);
    assert_eq!(runtime.len(), 0, "无效进度装配不提交任务");
}

/// 验证计算型池 0/1/2/3 的槽位下限和显式合法两 worker 装配，不扩张正数语义。
#[test]
#[ignore = "由标准入口在独立进程中执行"]
fn test_case_computational_minimum_and_explicit_size() {
    for (requested, actual) in [(0, 2), (1, 2), (2, 2), (3, 3)] {
        assert_eq!(ComputationalTaskPool::<()>::new(requested).worker_len(), actual);
    }
    let pool = ComputationalTaskPool::new(2);
    let runtime = MultiTaskRuntimeBuilder::new(pool)
        .thread_prefix(PREFIX)
        .init_worker_size(2)
        .set_worker_limit(2, 2)
        .set_timer_interval(1)
        .build();
    assert_real_progress(runtime, 2, 2, true);
}

/// 显式 `usize::MAX` 仍由 `build` 收敛到两槽位，不引起越界、巨大分配或任务丢失。
#[test]
#[ignore = "由标准入口在独立进程中执行"]
fn test_case_computational_explicit_over_capacity_cap() {
    let runtime = MultiTaskRuntimeBuilder::new(ComputationalTaskPool::<()>::new(2))
        .thread_prefix(PREFIX)
        .init_worker_size(usize::MAX)
        .set_worker_limit(usize::MAX, usize::MAX)
        .build();
    assert_real_progress(runtime, 2, 2, false);
}

/// 可窃取池槽位可多于默认 worker；额外槽位的任务仍经既有偷取流程完成。
#[test]
#[ignore = "由标准入口在独立进程中执行"]
fn test_case_stealable_custom_capacity() {
    let workers = default_workers();
    let pool = stealable(workers + 1);
    let runtime = MultiTaskRuntimeBuilder::new(pool).thread_prefix(PREFIX).build();
    assert_real_progress(runtime, workers + 1, workers, false);
}

/// `default_multi_thread(None)` 保持物理核数加一和 timer 任务语义。
#[test]
#[ignore = "由标准入口在独立进程中执行"]
fn test_case_stealable_public_none() {
    let workers = default_workers();
    let runtime = AsyncRuntimeBuilder::<()>::default_multi_thread(
        Some(PREFIX), None, None, Some(10));
    assert_real_progress(runtime, workers, workers, true);
}

/// `default_multi_thread(Some(0))` 仍使用可窃取池旧的默认兜底，不构造零槽位池。
#[test]
#[ignore = "由标准入口在独立进程中执行"]
fn test_case_stealable_public_zero() {
    let workers = default_workers();
    let runtime = AsyncRuntimeBuilder::<()>::default_multi_thread(
        Some(PREFIX), None, Some(0), Some(10));
    assert_real_progress(runtime, workers, workers, true);
}

/// `default_multi_thread(Some(1))` 保持单 worker/单槽位的显式正数语义。
#[test]
#[ignore = "由标准入口在独立进程中执行"]
fn test_case_stealable_public_explicit_one() {
    let runtime = AsyncRuntimeBuilder::<()>::default_multi_thread(
        Some(PREFIX), None, Some(1), Some(10));
    assert_real_progress(runtime, 1, 1, true);
}

/// 直接构造零槽位可窃取池仍按原接口 panic，不由 builder 默默修正。
#[test]
#[ignore = "由标准入口在独立进程中执行"]
fn test_case_invalid_stealable_zero_slots() {
    let result = std::panic::catch_unwind(|| stealable(0));
    assert!(result.is_err(), "零槽位的既有 panic 边界不得改变");
}
