#![cfg(not(target_arch = "wasm32"))]

//! MET-IMPLEMENTATION-001公开API专项；各场景由同一入口严格顺序运行隔离子进程。
//!
//! 使用真实runtime／原spawn／send／Runner／timeout，不补生产采样、通知或截止。
//! trace场景的默认no-op Provider仅验证API／来源／状态控制，不作为真实SDK聚合或
//! OTLP交付证据；后两项另需真实专项。测试无block_on、基准或大规模任务。

use pi_async_rt::{runtime_metrics::*, rt::{AsyncRuntime,
    single_thread::SingleTaskRunner, worker_thread::WorkerTaskRunner}};
#[cfg(feature = "trace")]
use pi_async_rt::rt::{
    serial_local_thread::{LocalTaskRunner, LocalTaskRuntime},
    multi_thread::{MultiTaskRuntimeBuilder, StealableTaskPool}};
use std::{process::{Child, Command, Stdio}, thread, time::{Duration, Instant}};
#[cfg(feature = "trace")]
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

const CASE_ENV: &str = "PI_ASYNC_RT_METRICS_CASE";
const CASE_LIMIT: Duration = Duration::from_secs(15);

/// 只管理真实测试进程，退出／失败／展开均尽力回收，不通过裸PID误杀其它进程。
struct CaseChild(Child);
impl CaseChild {
    fn wait(&mut self, deadline: Instant) {
        loop {
            assert!(Instant::now() < deadline, "指标专项子进程超过截止");
            if let Some(status) = self.0.try_wait().unwrap() {
                assert!(Instant::now() < deadline, "观察子进程退出时已过截止");
                assert!(status.success(), "指标专项失败: {}", status);
                return;
            }
            thread::sleep(Duration::from_millis(5));
        }
    }
}
impl Drop for CaseChild {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(Some(_))) { return; }
        if let Err(error) = self.0.kill() { eprintln!("终止指标专项子进程失败：{}", error); }
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match self.0.try_wait() {
                Ok(Some(_)) => return,
                Err(error) => { eprintln!("回收指标专项子进程失败：{}", error); return; }
                Ok(None) => (),
            }
            if Instant::now() >= deadline {
                eprintln!("指标专项子进程回收超过截止");
                return;
            }
            thread::sleep(Duration::from_millis(5));
        }
    }
}

/// 仅安排fixture隔离，子进程串行独占，整个目标也有绝对截止，不能逐例无限延长。
#[test]
fn test_runtime_metrics_contract_matrix() {
    if let Ok(case) = std::env::var(CASE_ENV) {
        let _clock = pi_async_rt::rt::startup_global_time_loop(1).expect("每个隔离进程只初始化一次时钟");
        eprintln!("指标专项场景：{}", case);
        match case.as_str() {
            "registration" => registration(),
            "support" => support(),
            "configuration" => configuration(),
            #[cfg(feature = "trace")]
            "direct" => direct(),
            #[cfg(feature = "trace")]
            "local" => local(),
            #[cfg(feature = "trace")]
            "round" => round(),
            #[cfg(feature = "trace")]
            "concurrent" => concurrent(),
            #[cfg(feature = "trace")]
            "multi" => multi(),
            _ => panic!("未知指标专项场景"),
        }
        return;
    }
    let target_deadline = Instant::now() + Duration::from_secs(180);
    #[cfg(not(feature = "trace"))]
    let cases = ["registration", "support", "configuration"];
    #[cfg(feature = "trace")]
    let cases = ["registration", "support", "configuration", "direct", "local", "round", "concurrent", "multi"];
    for case in cases {
        let deadline = (Instant::now() + CASE_LIMIT).min(target_deadline);
        assert!(Instant::now() < deadline);
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "test_runtime_metrics_contract_matrix", "--nocapture", "--test-threads=1"])
            .env(CASE_ENV, case).stdin(Stdio::null()).stdout(Stdio::inherit()).stderr(Stdio::inherit())
            .spawn().unwrap();
        CaseChild(child).wait(deadline);
    }
}

/// RG-T06：公开名称输入、合并超长、同核多名／Clone／trait对象及同名异核。
/// 失败不得恢复退休核心；不读取私有表或在夹具中补别名、来源和生命周期行为。
fn registration() {
    let runner = SingleTaskRunner::<()>::default();
    let runtime = runner.startup().unwrap();
    for invalid in ["", " ", "\t", "\u{2003}", "a\0b", "a\n", "a\u{7f}"] {
        assert_eq!(register_runtime_metrics(&runtime, invalid), Err(RuntimeMetricRegistrationError::InvalidName));
    }
    for invalid in ["x".repeat(257), "界".repeat(86)] {
        assert_eq!(register_runtime_metrics(&runtime, &invalid), Err(RuntimeMetricRegistrationError::NameTooLong));
    }
    let name = "x".repeat(256);
    let id = register_runtime_metrics(&runtime, &name).unwrap();
    assert!(id.as_u64() > 0 && id.as_u64() <= i64::MAX as u64);
    assert_eq!(runtime.clone().register_metrics(&name), Ok(id));
    assert_eq!(register_runtime_metrics(&runtime, "other"), Err(RuntimeMetricRegistrationError::NameTooLong));
    let other = SingleTaskRunner::<()>::default().into_local();
    let other_id = register_runtime_metrics(&other, &name).unwrap();
    assert_ne!(id, other_id);
    assert!(unregister_runtime_metrics(id));
    assert!(!unregister_runtime_metrics(id));
    assert_eq!(register_runtime_metrics(&runtime, &name), Ok(id));
    assert!(unregister_runtime_metrics(id));
    assert!(unregister_runtime_metrics(other_id));
    let shared = SingleTaskRunner::<()>::default().into_local();
    let object: &dyn RuntimeMetricRegistrable = &shared;
    let shared_id = register_runtime_metrics(object, "rpc").unwrap();
    for alias in ["storage", "rpc", "RPC", " rpc", "rpc ", "网关"] {
        assert_eq!(shared.clone().register_metrics(alias), Ok(shared_id));
    }
    assert!(unregister_runtime_metrics(shared_id));
    assert_eq!(register_runtime_metrics(object, &"x".repeat(255)), Err(RuntimeMetricRegistrationError::NameTooLong));
    assert!(!unregister_runtime_metrics(shared_id), "失败不能恢复已注销核心");
    assert_eq!(register_runtime_metrics(object, "storage"), Ok(shared_id));
    assert!(unregister_runtime_metrics(shared_id));
    assert_eq!(runtime.len(), 0);
    assert_eq!(shared.len(), 0);
}

/// Worker与内部Single共享身份，类别由首次登记固定；serial类型只返回明确拒绝。
fn support() {
    let worker_runner = WorkerTaskRunner::<()>::default();
    let worker = worker_runner.get_runtime();
    let id = register_runtime_metrics(&worker, "worker").unwrap();
    assert_eq!(register_runtime_metrics(worker.get_worker_runtime(), "worker"), Ok(id));
    assert_eq!(register_runtime_metrics(worker.get_worker_runtime(), "direct"), Ok(id));
    assert_eq!(register_runtime_metrics(&worker.clone(), "wrapper"), Ok(id));
    assert!(unregister_runtime_metrics(id));
    assert_eq!(register_runtime_metrics(worker.get_worker_runtime(), "direct"), Ok(id));
    assert!(unregister_runtime_metrics(id));
    let serial = pi_async_rt::rt::serial_single_thread::SingleTaskRunner::<()>::default().into_local();
    let serial_worker = pi_async_rt::rt::serial_worker_thread::WorkerTaskRunner::<()>::default().get_runtime();
    for name in ["serial".to_owned(), String::new(), "x".repeat(257)] {
        assert_eq!(register_runtime_metrics(&serial, &name),
            Err(RuntimeMetricRegistrationError::ThreadBoundRuntimeRequiresOwnerBridge));
        assert_eq!(register_runtime_metrics(&serial_worker, &name),
            Err(RuntimeMetricRegistrationError::ThreadBoundRuntimeRequiresOwnerBridge));
    }
}

/// trace关闭优先失败且不投递；trace开启检查未初始化、非法标识、幂等、冲突和溢出。
fn configuration() {
    #[cfg(not(feature = "trace"))]
    {
        assert_eq!(init_runtime_metrics_reporting(""), Err(RuntimeMetricReportingInitError::ReportingDisabled));
        assert!(matches!(collect_and_report_runtime_metrics(RuntimeMetricCollectionOptions::default()),
            Err(RuntimeMetricCollectionError::ReportingDisabled)));
    }
    #[cfg(feature = "trace")]
    {
        assert!(matches!(collect_and_report_runtime_metrics(RuntimeMetricCollectionOptions::default()),
            Err(RuntimeMetricCollectionError::ReportingNotInitialized)));
        for invalid in ["", " ", "a\0b"] {
            assert_eq!(init_runtime_metrics_reporting(invalid), Err(RuntimeMetricReportingInitError::InvalidProcessInstance));
        }
        assert_eq!(init_runtime_metrics_reporting(&"x".repeat(257)), Err(RuntimeMetricReportingInitError::ProcessInstanceTooLong));
        init_runtime_metrics_reporting("test-process").unwrap();
        init_runtime_metrics_reporting("test-process").unwrap();
        assert_eq!(init_runtime_metrics_reporting("another-process"), Err(RuntimeMetricReportingInitError::AlreadyInitialized));
        let empty = collect_and_report_runtime_metrics(RuntimeMetricCollectionOptions::default()).unwrap();
        assert!(empty.instances().is_empty());
        let overflow = RuntimeMetricCollectionOptions::new(Duration::MAX, Duration::from_secs(1)).unwrap();
        assert!(matches!(collect_and_report_runtime_metrics(overflow), Err(RuntimeMetricCollectionError::DeadlineOverflow)));
    }
}

#[cfg(feature = "trace")]
fn item(report: &RuntimeMetricCollectionReport, id: RuntimeMetricId) -> &RuntimeMetricInstanceReport {
    report.instances().iter().find(|item| item.id() == id).expect("报告必须包含指定真实核心")
}

#[cfg(feature = "trace")]
fn collect() -> RuntimeMetricCollectionReport {
    collect_and_report_runtime_metrics(RuntimeMetricCollectionOptions::default()).unwrap()
}

/// 普通getter原值对照、既有raw门面spawn／timeout组合、报告独立生命期及退休不补零。
#[cfg(feature = "trace")]
fn direct() {
    init_runtime_metrics_reporting("test-process").unwrap();
    let runner = SingleTaskRunner::<()>::default();
    let runtime = runner.startup().unwrap();
    let id = register_runtime_metrics(&runtime, "single").unwrap();
    let completed = Arc::new(AtomicUsize::new(0));
    for _ in 0..2 {
        let completed = completed.clone();
        runtime.spawn(async move { completed.fetch_add(1, Ordering::Relaxed); }).unwrap();
    }
    let facade = runtime.to_local_runtime();
    assert_eq!(facade.get_id(), runtime.get_id());
    let completed_copy = completed.clone();
    facade.spawn(async move { completed_copy.fetch_add(1, Ordering::Relaxed); }).unwrap();
    // 主线程是本例Single的推进者；即使该源可直读，也按统一契约由外部线程采集。
    let report = thread::spawn(collect).join().unwrap();
    assert_eq!(item(&report, id).counts(), Some((runtime.len(), Some(runtime.wait_len()))));
    assert_eq!(item(&report, id).counts(), Some((3, Some(0))));
    for _ in 0..3 { runner.run_once().unwrap(); }
    assert_eq!(completed.load(Ordering::Relaxed), 3);
    let copy = runtime.clone();
    let done = completed.clone();
    runtime.spawn(async move { copy.timeout(0).await; copy.timeout(1).await; done.fetch_add(1, Ordering::Relaxed); }).unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while completed.load(Ordering::Relaxed) != 4 {
        assert!(Instant::now() < deadline);
        runner.run_once().unwrap();
        thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(item(&thread::spawn(collect).join().unwrap(), id).counts(), Some((0, Some(0))));
    assert!(unregister_runtime_metrics(id));
    let retired = thread::spawn(collect).join().unwrap();
    assert_eq!(item(&retired, id).status(), RuntimeMetricInstanceStatus::Unregistered);
    assert_eq!(item(&retired, id).counts(), None);
    assert_eq!(item(&report, id).name(), "single");
    assert_eq!(item(&report, id).counts(), Some((3, Some(0))));
}

#[cfg(feature = "trace")]
fn wait_request<O: Default + 'static>(runtime: &LocalTaskRuntime<O>) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while runtime.len() == 0 {
        assert!(Instant::now() < deadline, "真实采样请求没有入队");
        thread::yield_now();
    }
}

/// 全局登记→原send→本地owner读取→同步回复，含超时／重登记跨周期同许可和恢复。
#[cfg(feature = "trace")]
fn local() {
    init_runtime_metrics_reporting("test-process").unwrap();
    let runner = LocalTaskRunner::<()>::new();
    let runtime = runner.get_runtime();
    let id = register_runtime_metrics(&runtime, "local").unwrap();
    let options = RuntimeMetricCollectionOptions::new(Duration::from_millis(20), Duration::from_secs(1)).unwrap();
    let timeout = thread::spawn(move || collect_and_report_runtime_metrics(options).unwrap()).join().unwrap();
    assert_eq!(item(&timeout, id).status(), RuntimeMetricInstanceStatus::DeadlineExceeded);
    assert_eq!(runtime.len(), 1);
    assert!(unregister_runtime_metrics(id));
    assert_eq!(register_runtime_metrics(&runtime, "local"), Ok(id));
    let in_flight = thread::spawn(collect).join().unwrap();
    assert_eq!(item(&in_flight, id).status(), RuntimeMetricInstanceStatus::RequestInFlight);
    assert_eq!(runtime.len(), 1);
    runner.poll();
    runner.run_once();
    let receiving = thread::spawn(|| {
        collect_and_report_runtime_metrics(RuntimeMetricCollectionOptions::new(Duration::from_secs(2), Duration::from_secs(3)).unwrap()).unwrap()
    });
    wait_request(&runtime);
    runner.poll();
    runner.run_once();
    let report = receiving.join().unwrap();
    assert_eq!(item(&report, id).counts(), Some((0, None)));
    assert_eq!(item(&report, id).status(), RuntimeMetricInstanceStatus::RecordInvoked);
    assert_eq!((report.active_count(), report.sampled_count()), (1, 1));
    assert!(unregister_runtime_metrics(id));
}

/// 整轮截止裁剪第一个实例预算；后续本地和普通来源明确未尝试，不能补假成功。
#[cfg(feature = "trace")]
fn round() {
    init_runtime_metrics_reporting("test-process").unwrap();
    let first = LocalTaskRunner::<()>::new();
    let second = LocalTaskRunner::<()>::new();
    let regular = SingleTaskRunner::<()>::default().into_local();
    let a = register_runtime_metrics(&first.get_runtime(), "first").unwrap();
    let b = register_runtime_metrics(&second.get_runtime(), "second").unwrap();
    let c = register_runtime_metrics(&regular, "third").unwrap();
    let report = thread::spawn(|| collect_and_report_runtime_metrics(
        RuntimeMetricCollectionOptions::new(Duration::from_secs(1), Duration::from_millis(20)).unwrap()).unwrap()).join().unwrap();
    assert_eq!(item(&report, a).status(), RuntimeMetricInstanceStatus::DeadlineExceeded);
    for id in [b, c] { assert_eq!(item(&report, id).status(), RuntimeMetricInstanceStatus::RoundDeadlineSkipped); }
    assert_eq!((report.active_count(), report.sampled_count(), report.count(RuntimeMetricInstanceStatus::RoundDeadlineSkipped)), (3, 0, 2));
    assert_eq!(second.get_runtime().len(), 0);
    first.poll();
    first.run_once();
    for id in [a, b, c] { assert!(unregister_runtime_metrics(id)); }
}

/// 第一外部线程等待本地回复时，第二外部线程立即被拒绝，真实队列仍仅一个请求。
#[cfg(feature = "trace")]
fn concurrent() {
    init_runtime_metrics_reporting("test-process").unwrap();
    let runner = LocalTaskRunner::<()>::new();
    let runtime = runner.get_runtime();
    let id = register_runtime_metrics(&runtime, "local").unwrap();
    let first = thread::spawn(|| collect_and_report_runtime_metrics(
        RuntimeMetricCollectionOptions::new(Duration::from_secs(2), Duration::from_secs(3)).unwrap()).unwrap());
    wait_request(&runtime);
    let other = thread::spawn(|| collect_and_report_runtime_metrics(RuntimeMetricCollectionOptions::default())).join().unwrap();
    assert!(matches!(other, Err(RuntimeMetricCollectionError::CollectionInProgress)));
    assert_eq!(runtime.len(), 1);
    runner.poll();
    runner.run_once();
    assert_eq!(item(&first.join().unwrap(), id).status(), RuntimeMetricInstanceStatus::RecordInvoked);
    assert!(unregister_runtime_metrics(id));
}

/// 两worker真实多线程、timer／raw门面组合；任务完成并稳定后严格对照原getter。
/// worker无法关闭，必须在隔离子进程运行，不把跨用例残留当正常回收。
#[cfg(feature = "trace")]
fn multi() {
    init_runtime_metrics_reporting("test-process").unwrap();
    let runtime = MultiTaskRuntimeBuilder::new(StealableTaskPool::<()>::with(2, 64, [1, 1], 10))
        .thread_prefix("Metric-Test").init_worker_size(2).set_worker_limit(2, 2).set_timer_interval(1).build();
    let id = register_runtime_metrics(&runtime, "multi").unwrap();
    assert_eq!(register_runtime_metrics(&runtime.clone(), "io"), Ok(id));
    assert_eq!(register_runtime_metrics(&runtime, "multi"), Ok(id));
    let (sender, receiver) = std::sync::mpsc::channel();
    let copy = runtime.clone();
    runtime.to_local_runtime().spawn(async move {
        copy.timeout(0).await;
        copy.timeout(1).await;
        sender.send(()).unwrap();
    }).unwrap();
    receiver.recv_timeout(Duration::from_secs(3)).unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while runtime.len() != 0 || runtime.wait_len() != 0 {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
    let report = collect();
    assert_eq!(item(&report, id).counts(), Some((0, Some(0))));
    assert_eq!(item(&report, id).kind(), RuntimeMetricKind::Multi);
    assert_eq!(report.instances().len(), 1);
    assert_eq!(item(&report, id).name(), "multi");
    assert_eq!(item(&report, id).names().collect::<Vec<_>>(), ["multi", "io"]);
    assert!(unregister_runtime_metrics(id));
}
