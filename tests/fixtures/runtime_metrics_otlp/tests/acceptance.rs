#![cfg(not(target_arch = "wasm32"))]

//! MET-OTLP-FIXTURE-001：真实公开入口→官方SDK聚合→官方OTLP→TCP接收／官方解码验收。
//!
//! 只覆盖多个已注册实例的实际上报，不替代独立权限／并发／异常／安全／性能专项。
//! 唯一父入口严格顺序运行一个隔离子进程；不调用block_on，不直接record／补采样。
//! 下文主线程指拥有三个Runner的场景测试线程，不是libtest框架的主线程。
//! 合同及三图：docs/RUNTIME_METRICS_OTLP_ACCEPTANCE_FIXTURE_DESIGN.md。

#[path = "support/transport.rs"]
mod transport;
#[path = "support/assertions.rs"]
mod assertions;

use assertions::{assert_export, ExpectedInstance};
use pi_async_rt::{runtime_metrics::*, rt::{AsyncRuntime,
    serial_local_thread::LocalTaskRunner, single_thread::SingleTaskRunner}};
use std::{process::{Child, Command, Stdio}, sync::{Arc, atomic::{AtomicUsize, Ordering},
    mpsc::{self, TryRecvError}}, thread, time::{Duration, Instant}};
use transport::OtlpHost;

const CHILD_ENV: &str = "PI_ASYNC_RT_OTLP_ACCEPTANCE_CHILD";
const PROCESS: &str = "OTLP验收-独立子进程";
const SINGLE_NAME: &str = "网关-收发";
const LOCAL_NAME: &str = "本地-收发";
const SINGLE_ALIAS: &str = "RPC";
const LOCAL_ALIAS: &str = "业务";
// 期望使用独立字面量，不从生产报告或标签构造实际期望。
const SINGLE_REPORTED: &str = "网关-收发-RPC";
const LOCAL_REPORTED: &str = "本地-收发-业务";

/// 只回收所持真实Child，不以裸PID定位其它进程；截止后不允许观察迟到成功。
struct OwnedChild(Child);
impl OwnedChild {
    fn wait(&mut self, deadline: Instant) {
        loop {
            assert!(Instant::now() < deadline, "OTLP验收子进程超过60秒截止");
            if let Some(status) = self.0.try_wait().unwrap() {
                assert!(Instant::now() < deadline, "观察退出时已超过成功截止");
                assert!(status.success(), "OTLP验收失败：{}", status);
                return;
            }
            thread::sleep(Duration::from_millis(5));
        }
    }
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(Some(_))) { return; }
        if let Err(error) = self.0.kill() { eprintln!("终止OTLP子进程失败：{}", error); }
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match self.0.try_wait() {
                Ok(Some(_)) => return,
                Err(error) => { eprintln!("回收OTLP子进程失败：{}", error); return; }
                Ok(None) => (),
            }
            if Instant::now() >= deadline { eprintln!("回收OTLP子进程超过5秒"); return; }
            thread::sleep(Duration::from_millis(5));
        }
    }
}

/// A01～A04：三实例各两名仍只有八点；同名不同ID、非零／零／None及非首名恢复。
/// 父进程不建SDK／runtime／时钟，子进程唯一初始化一次时钟。只修改子进程OTEL环境。
#[test]
fn test_registered_runtime_metrics_reach_otlp_receiver() {
    if let Some(value) = std::env::var_os(CHILD_ENV) {
        assert_eq!(value.as_os_str(), std::ffi::OsStr::new("1"));
        let _clock = pi_async_rt::rt::startup_global_time_loop(1).expect("子进程仅初始化一次时钟");
        acceptance();
        return;
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut command = Command::new(std::env::current_exe().unwrap());
    command.args(["--exact", "test_registered_runtime_metrics_reach_otlp_receiver",
        "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, "1").stdin(Stdio::null()).stdout(Stdio::inherit()).stderr(Stdio::inherit());
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("OTEL_") { command.env_remove(key); }
    }
    OwnedChild(command.spawn().unwrap()).wait(deadline);
}

/// 主线程拥有原Runner，采集只在专用外部线程调用；观察通道不参与生产的单次回复。
/// 推进仅用原poll／run_once，不手动唤醒、回填或修改截止。4秒观测失败即失败，不重试。
fn collect(local: &LocalTaskRunner<()>) -> RuntimeMetricCollectionReport {
    let (sender, receiver) = mpsc::sync_channel(1);
    let task = thread::Builder::new().name("Metrics-External-Collector".into()).spawn(move || {
        let options = RuntimeMetricCollectionOptions::new(Duration::from_secs(2), Duration::from_secs(3)).unwrap();
        let result = collect_and_report_runtime_metrics(options);
        let _ = sender.try_send(result);
    }).unwrap();
    let deadline = Instant::now() + Duration::from_secs(4);
    let result = loop {
        assert!(Instant::now() < deadline, "真实采集未按时返回");
        local.poll();
        local.run_once();
        match receiver.try_recv() {
            Ok(result) => {
                assert!(Instant::now() < deadline, "不能接受迟观察完成");
                break result;
            }
            Err(TryRecvError::Empty) => thread::sleep(Duration::from_millis(1)),
            Err(TryRecvError::Disconnected) => panic!("真实采集线程无结果退出"),
        }
    };
    task.join().expect("真实采集线程必须正常退出");
    result.expect("三实例合法推进必须全部完成本轮采集")
}

/// 只读Report对照独立已知队列状态；不能把Report当作实际OTLP期望的生成器。
fn assert_sample(report: &RuntimeMetricCollectionReport, id: RuntimeMetricId, name: &str,
    kind: RuntimeMetricKind, status: RuntimeMetricInstanceStatus, counts: Option<(usize, Option<usize>)>)
{
    let mut found = report.instances().iter().filter(|item| item.id() == id);
    let item = found.next().expect("报告必须包含此真实登记身份");
    assert!(found.next().is_none());
    assert_eq!(item.name(), name);
    let names: Vec<_> = match kind {
        RuntimeMetricKind::Single => vec![SINGLE_NAME, SINGLE_ALIAS],
        RuntimeMetricKind::Local => vec![LOCAL_NAME, LOCAL_ALIAS],
        _ => panic!("本夹具没有其它运行时类别"),
    };
    assert_eq!(item.names().collect::<Vec<_>>(), names);
    assert_eq!(item.kind(), kind);
    assert_eq!(item.status(), status);
    assert_eq!(item.counts(), counts);
}

/// 三实例汇总须与独立活动／退休数一致，其余五种失败状态均不允许被成功数掩盖。
fn assert_summary(report: &RuntimeMetricCollectionReport, active: usize, retired: usize) {
    assert_eq!(report.instances().len(), 3);
    assert_eq!((report.active_count(), report.retired_count(), report.sampled_count()), (active, retired, active));
    assert_eq!(report.count(RuntimeMetricInstanceStatus::RecordInvoked), active);
    assert_eq!(report.count(RuntimeMetricInstanceStatus::Unregistered), retired);
    for status in [RuntimeMetricInstanceStatus::RequestInFlight, RuntimeMetricInstanceStatus::DeadlineExceeded,
        RuntimeMetricInstanceStatus::ReplyClosed, RuntimeMetricInstanceStatus::RoundDeadlineSkipped,
        RuntimeMetricInstanceStatus::MetricValueOutOfRange]
    { assert_eq!(report.count(status), 0); }
}

fn acceptance() {
    let host = OtlpHost::new();
    init_runtime_metrics_reporting(PROCESS).unwrap();
    let process = format!("{}-{}", PROCESS, std::process::id());
    let first_runner = SingleTaskRunner::<()>::default();
    let first = first_runner.startup().unwrap();
    let second_runner = SingleTaskRunner::<()>::default();
    let second = second_runner.startup().unwrap();
    let local_runner = LocalTaskRunner::<()>::new();
    let local = local_runner.get_runtime();
    assert!(collect(&local_runner).instances().is_empty(), "构建不应隐式登记");
    let first_id = register_runtime_metrics(&first, SINGLE_NAME).unwrap();
    let second_id = register_runtime_metrics(&second, SINGLE_NAME).unwrap();
    let local_id = register_runtime_metrics(&local, LOCAL_NAME).unwrap();
    assert_eq!(register_runtime_metrics(&first.clone(), SINGLE_ALIAS), Ok(first_id));
    assert_eq!(register_runtime_metrics(&second, SINGLE_ALIAS), Ok(second_id));
    assert_eq!(register_runtime_metrics(&local, LOCAL_ALIAS), Ok(local_id));
    assert_eq!(register_runtime_metrics(&first, SINGLE_ALIAS), Ok(first_id));
    assert_ne!(first_id, second_id);
    assert_ne!(first_id, local_id);
    assert_ne!(second_id, local_id);

    // spawn_timing先排注册包装任务；分别由原Runner执行，才能形成两个真实到期项。
    // 一小时deadline远大于子进程硬截止，准备步骤结束后不再有包装任务留在即时队列。
    for _ in 0..2 { second.spawn_timing(async {}, 3_600_000).unwrap(); }
    for _ in 0..2 { second_runner.run_once().unwrap(); }
    assert_eq!((second.len(), second.wait_len()), (0, 2));
    let first_done = Arc::new(AtomicUsize::new(0));
    let second_done = Arc::new(AtomicUsize::new(0));
    for _ in 0..3 {
        let done = first_done.clone();
        first.spawn(async move { done.fetch_add(1, Ordering::Relaxed); }).unwrap();
    }
    for _ in 0..2 {
        let done = second_done.clone();
        second.spawn(async move { done.fetch_add(1, Ordering::Relaxed); }).unwrap();
    }
    assert_eq!((first.len(), first.wait_len(), second.len(), second.wait_len(), local.len()), (3, 0, 2, 2, 0));
    let report = collect(&local_runner);
    assert_summary(&report, 3, 0);
    assert_sample(&report, first_id, SINGLE_NAME, RuntimeMetricKind::Single,
        RuntimeMetricInstanceStatus::RecordInvoked, Some((3, Some(0))));
    assert_sample(&report, second_id, SINGLE_NAME, RuntimeMetricKind::Single,
        RuntimeMetricInstanceStatus::RecordInvoked, Some((2, Some(2))));
    assert_sample(&report, local_id, LOCAL_NAME, RuntimeMetricKind::Local,
        RuntimeMetricInstanceStatus::RecordInvoked, Some((0, None)));
    assert_eq!((first_done.load(Ordering::Relaxed), second_done.load(Ordering::Relaxed), local.len()), (0, 0, 0));
    assert_export(&host.flush(), &process, &[
        ExpectedInstance { id: first_id.as_u64(), name: SINGLE_REPORTED, len: 3, wait_len: Some(0), state: 1 },
        ExpectedInstance { id: second_id.as_u64(), name: SINGLE_REPORTED, len: 2, wait_len: Some(2), state: 1 },
        ExpectedInstance { id: local_id.as_u64(), name: LOCAL_REPORTED, len: 0, wait_len: None, state: 1 },
    ]);
    eprintln!("OTLP-A01通过：三实例各两名／仍八点／同展示名不同ID／完整宿主标记-PID");

    assert!(unregister_runtime_metrics(first_id));
    second_runner.run_once().unwrap();
    assert_eq!((second.len(), second.wait_len(), second_done.load(Ordering::Relaxed)), (1, 2, 1));
    let report = collect(&local_runner);
    assert_summary(&report, 2, 1);
    assert_sample(&report, first_id, SINGLE_NAME, RuntimeMetricKind::Single,
        RuntimeMetricInstanceStatus::Unregistered, None);
    assert_sample(&report, second_id, SINGLE_NAME, RuntimeMetricKind::Single,
        RuntimeMetricInstanceStatus::RecordInvoked, Some((1, Some(2))));
    assert_sample(&report, local_id, LOCAL_NAME, RuntimeMetricKind::Local,
        RuntimeMetricInstanceStatus::RecordInvoked, Some((0, None)));
    assert_export(&host.flush(), &process, &[
        ExpectedInstance { id: first_id.as_u64(), name: SINGLE_REPORTED, len: 3, wait_len: Some(0), state: 0 },
        ExpectedInstance { id: second_id.as_u64(), name: SINGLE_REPORTED, len: 1, wait_len: Some(2), state: 1 },
        ExpectedInstance { id: local_id.as_u64(), name: LOCAL_REPORTED, len: 0, wait_len: None, state: 1 },
    ]);
    eprintln!("OTLP-A02通过：注销状态／累计旧值不补零／最新值非求和");

    assert_eq!(register_runtime_metrics(&first, SINGLE_ALIAS), Ok(first_id));
    for _ in 0..3 { first_runner.run_once().unwrap(); }
    second_runner.run_once().unwrap();
    assert_eq!((first_done.load(Ordering::Relaxed), second_done.load(Ordering::Relaxed)), (3, 2));
    assert_eq!((first.len(), first.wait_len(), second.len(), second.wait_len(), local.len()), (0, 0, 0, 2, 0));
    let report = collect(&local_runner);
    assert_summary(&report, 3, 0);
    assert_sample(&report, first_id, SINGLE_NAME, RuntimeMetricKind::Single,
        RuntimeMetricInstanceStatus::RecordInvoked, Some((0, Some(0))));
    assert_sample(&report, second_id, SINGLE_NAME, RuntimeMetricKind::Single,
        RuntimeMetricInstanceStatus::RecordInvoked, Some((0, Some(2))));
    assert_sample(&report, local_id, LOCAL_NAME, RuntimeMetricKind::Local,
        RuntimeMetricInstanceStatus::RecordInvoked, Some((0, None)));
    let final_expected = [
        ExpectedInstance { id: first_id.as_u64(), name: SINGLE_REPORTED, len: 0, wait_len: Some(0), state: 1 },
        ExpectedInstance { id: second_id.as_u64(), name: SINGLE_REPORTED, len: 0, wait_len: Some(2), state: 1 },
        ExpectedInstance { id: local_id.as_u64(), name: LOCAL_REPORTED, len: 0, wait_len: None, state: 1 },
    ];
    assert_export(&host.flush(), &process, &final_expected);
    eprintln!("OTLP-A03通过：非首名恢复全部名称／同ID／即时任务精确完成／固定系列覆盖");

    for id in [first_id, second_id, local_id] { assert!(unregister_runtime_metrics(id)); }
    assert_eq!(local.len(), 0, "真正采样任务已在owner执行完毕");
    // 注销后不再采集；SDK最终导出只能是最后记录，不得假定注销自动发状态0。
    assert_export(&host.shutdown(), &process, &final_expected);
    eprintln!("OTLP-A04通过：官方最终导出／无隐式采集／宿主正常关闭");
}
