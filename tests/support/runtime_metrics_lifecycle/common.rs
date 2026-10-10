//! MET-L-HELPER-001：只做有界观测、原Runner驱动和独立断言，不实现生产指标能力。
//!
//! 场景单向消费本模块；本模块不依赖场景、SDK探针或父入口，不回调它们的状态。
//! 采集观察通道不是生产单次回复，不回填或修改许可；不接受上层状态回调。
//! 具体接口／成本／失败边界见docs/RUNTIME_METRICS_LIFECYCLE_TEST_PLAN.md的辅助合同。

use crossbeam_channel::{bounded, Receiver, TryRecvError};
use pi_async_rt::{runtime_metrics::*, rt::{AsyncRuntime, serial_local_thread::{LocalTaskRunner, LocalTaskRuntime}}};
use std::{any::Any, sync::{Weak, atomic::{AtomicUsize, Ordering}}, thread, time::{Duration, Instant}};

pub(super) const LIMIT: Duration = Duration::from_secs(3);

/// 非零生产预算；只构造原Options，不替生产计算截止或投递任务。
pub(super) fn options() -> RuntimeMetricCollectionOptions {
    RuntimeMetricCollectionOptions::new(Duration::from_secs(2), LIMIT).unwrap()
}

/// 在不承担Runner推进的线程执行原采集，4秒内观测结果后才join；父截止额外兜底。
/// 容量1通道只运输整个公开Report，不访问或代填本库内部的单次采样回复。
pub(super) fn collect_external() -> RuntimeMetricCollectionReport {
    let (sender, receiver) = bounded(1);
    let collector = thread::spawn(move || {
        sender.try_send(collect_and_report_runtime_metrics(options())).unwrap();
    });
    let result = receive(&receiver, Instant::now() + Duration::from_secs(4));
    collector.join().unwrap();
    result.unwrap()
}

/// 仅owner执行原poll／run_once；4秒内观察完整Report，不代填生产回复或手动notify。
pub(super) fn collect_native<O: Default + 'static>(runner: &LocalTaskRunner<O>)
    -> RuntimeMetricCollectionReport
{
    let (sender, receiver) = bounded(1);
    let collector = thread::spawn(move || {
        sender.try_send(collect_and_report_runtime_metrics(options())).unwrap();
    });
    let deadline = Instant::now() + Duration::from_secs(4);
    let result = loop {
        assert!(Instant::now() < deadline, "原本地采集未按时完成");
        runner.poll();
        runner.run_once();
        match receiver.try_recv() {
            Ok(result) => {
                assert!(Instant::now() < deadline, "过时结果不能算按时完成");
                break result;
            }
            Err(TryRecvError::Empty) => thread::sleep(Duration::from_millis(1)),
            Err(TryRecvError::Disconnected) => panic!("外部采集未返回结果"),
        }
    };
    collector.join().unwrap();
    result.unwrap()
}

/// 内部观测循环；闭包仅由本模块的窄只读接口建立，不回调上层场景。
fn wait_until(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + LIMIT;
    loop {
        assert!(Instant::now() < deadline, "观测等待超过截止");
        if ready() {
            assert!(Instant::now() < deadline, "不能接受迟到观测");
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
}

/// 仅本地唯一owner可调用，等待原send公共入队；后台Runner启动后禁止异线程调用。
pub(super) fn wait_local_request<O: Default + 'static>(runtime: &LocalTaskRuntime<O>) {
    wait_until(|| runtime.len() == 1);
}

/// 只读原线程安全getter，不消费任务，不把队列为空解释为所有Pending任务结束。
pub(super) fn wait_idle<O: Default + 'static, R: AsyncRuntime<O>>(runtime: &R) {
    wait_until(|| runtime.len() == 0 && runtime.wait_len() == 0);
}

/// 只观察原运行状态原子，不读取后台本地UnsafeCell队列。
pub(super) fn wait_running<O: Default + 'static>(runtime: &LocalTaskRuntime<O>) {
    wait_until(|| runtime.is_running());
}

/// 观察夹具输出的实际析构数，不主动析构输出或释放生产采样许可。
pub(super) fn wait_drops(counter: &AtomicUsize, expected: usize) {
    wait_until(|| counter.load(Ordering::Acquire) == expected);
}

/// Weak只观测实际资源释放，不清队列或代替原关闭；不声称join第三方线程。
pub(super) fn wait_released<T>(resource: &Weak<T>) {
    wait_until(|| resource.upgrade().is_none());
}

/// 夹具数据通道的有界接收；deadline只限制观测，不传给生产或改变其截止。
pub(super) fn receive<T>(receiver: &Receiver<T>, deadline: Instant) -> T {
    assert!(Instant::now() < deadline, "观测截止已到");
    let value = receiver.recv_deadline(deadline).expect("观测通道未按时返回");
    assert!(Instant::now() < deadline, "观测完成时已过截止");
    value
}

/// 期望由场景已知状态提供；检查唯一身份及全部只读字段，不从实际Report生成期望。
pub(super) fn assert_sample(report: &RuntimeMetricCollectionReport, id: RuntimeMetricId,
    name: &str, kind: RuntimeMetricKind, status: RuntimeMetricInstanceStatus,
    counts: Option<(usize, Option<usize>)>)
{
    let mut found = report.instances().iter().filter(|item| item.id() == id);
    let item = found.next().expect("报告缺少真实登记身份");
    assert!(found.next().is_none(), "同一身份不能重复");
    assert_eq!((item.name(), item.kind(), item.status(), item.counts()), (name, kind, status, counts));
}

/// 本批正常场景的独立汇总；五种失败状态必须为0，不能被成功／退休总数掩盖。
pub(super) fn assert_summary(report: &RuntimeMetricCollectionReport, active: usize, retired: usize) {
    assert_eq!(report.instances().len(), active + retired);
    assert_eq!((report.active_count(), report.retired_count(), report.sampled_count()), (active, retired, active));
    assert_eq!(report.count(RuntimeMetricInstanceStatus::RecordInvoked), active);
    assert_eq!(report.count(RuntimeMetricInstanceStatus::Unregistered), retired);
    for status in [RuntimeMetricInstanceStatus::RequestInFlight, RuntimeMetricInstanceStatus::DeadlineExceeded,
        RuntimeMetricInstanceStatus::ReplyClosed, RuntimeMetricInstanceStatus::RoundDeadlineSkipped,
        RuntimeMetricInstanceStatus::MetricValueOutOfRange]
    { assert_eq!(report.count(status), 0); }
    assert!(report.instances().windows(2).all(|pair| pair[0].id() < pair[1].id()));
}

/// 只有指定载荷才是有效panic故障注入；夹具断言或其它panic必须使专项失败。
pub(super) fn assert_injected_panic<T>(result: Result<T, Box<dyn Any + Send>>, expected: &str) {
    let payload = match result { Err(payload) => payload, Ok(_) => panic!("指定panic没有发生") };
    let actual = payload.downcast_ref::<&str>().copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str));
    assert_eq!(actual, Some(expected), "非指定panic不能冒充已注入故障");
}
