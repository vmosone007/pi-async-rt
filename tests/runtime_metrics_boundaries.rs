#![cfg(all(feature = "trace", not(target_arch = "wasm32")))]

//! MET-F-B01／02：原SingleTaskRunner＋合法外部任务池的指标／截止／展开专项。
//!
//! 自定义池只保存真实AsyncTask，不实现轮询、唤醒合并、指标来源、截止或许可。
//! getter故障是外部受控输入，生产采集和guard负责真实判定／恢复；不造假时钟。
//! 每子例唯一原时钟，父15秒／整目标180秒；完整边界见最终验证计划。

use crossbeam_channel::{bounded, Receiver, Sender};
use crossbeam_queue::SegQueue;
use pi_async_rt::{runtime_metrics::*, rt::{alloc_rt_uid, AsyncRuntime,
    AsyncTask, AsyncTaskPool, AsyncTaskPoolExt, single_thread::SingleTaskRunner}};
use std::{any::Any, io, process::{Child, Command, Stdio}, sync::{Arc, atomic::{AtomicU8, Ordering}},
    thread, time::{Duration, Instant}, vec::IntoIter};

const LIMIT: Duration = Duration::from_secs(3);
const PANIC: &str = "故障注入：自定义任务池len";
const CASE_ENV: &str = "PI_ASYNC_RT_METRICS_BOUNDARY_CASE";

/// 故障门只归外部getter，不涉及生产指标控制状态；正常路径只读原真实队列。
struct ReadControl {
    action: AtomicU8,
    entered: (Sender<()>, Receiver<()>),
    release: (Sender<()>, Receiver<()>),
}
impl Default for ReadControl {
    fn default() -> Self { Self { action: AtomicU8::new(0), entered: bounded(1), release: bounded(1) } }
}

struct ExternalPool {
    id: usize,
    queue: SegQueue<Arc<AsyncTask<ExternalPool, usize>>>,
    control: Arc<ReadControl>,
}
impl Default for ExternalPool {
    fn default() -> Self { Self { id: alloc_rt_uid() << 32, queue: SegQueue::new(), control: Arc::new(ReadControl::default()) } }
}
impl AsyncTaskPool<usize> for ExternalPool {
    type Pool = Self;
    fn get_thread_id(&self) -> usize { self.id }
    fn len(&self) -> usize {
        match self.control.action.swap(0, Ordering::AcqRel) {
            0 => (),
            1 => {
                self.control.entered.0.try_send(()).unwrap();
                receive(&self.control.release.1, Instant::now() + LIMIT);
            }
            2 => std::panic::panic_any(PANIC),
            _ => panic!("未知getter故障配置"),
        }
        self.queue.len()
    }
    fn push(&self, task: Arc<AsyncTask<Self::Pool, usize>>) -> io::Result<()> { self.queue.push(task); Ok(()) }
    fn push_local(&self, task: Arc<AsyncTask<Self::Pool, usize>>) -> io::Result<()> { self.push(task) }
    fn push_priority(&self, _: usize, task: Arc<AsyncTask<Self::Pool, usize>>) -> io::Result<()> { self.push(task) }
    fn push_keep(&self, task: Arc<AsyncTask<Self::Pool, usize>>) -> io::Result<()> { self.push(task) }
    fn try_pop(&self) -> Option<Arc<AsyncTask<Self::Pool, usize>>> { self.queue.pop() }
    fn try_pop_all(&self) -> IntoIter<Arc<AsyncTask<Self::Pool, usize>>> {
        let mut tasks = Vec::new();
        while let Some(task) = self.queue.pop() { tasks.push(task); }
        tasks.into_iter()
    }
    fn get_thread_waker(&self) -> Option<&Arc<(std::sync::atomic::AtomicBool, parking_lot::Mutex<()>, parking_lot::Condvar)>> { None }
}
impl AsyncTaskPoolExt<usize> for ExternalPool {}

/// 仅观测夹具结果，绝对截止前后复核；不是生产单次回复通道。
fn receive<T>(receiver: &Receiver<T>, deadline: Instant) -> T {
    assert!(Instant::now() < deadline);
    let result = receiver.recv_deadline(deadline).expect("边界观测超过截止");
    assert!(Instant::now() < deadline);
    result
}

/// 采集只在不负责推进Runner的外部线程；限时观测结果后才join，不补生产能力。
fn collect(options: RuntimeMetricCollectionOptions) -> RuntimeMetricCollectionReport {
    let (sender, receiver) = bounded(1);
    let collector = thread::spawn(move || sender.try_send(collect_and_report_runtime_metrics(options)).unwrap());
    let result = receive(&receiver, Instant::now() + LIMIT);
    collector.join().unwrap();
    result.unwrap()
}

/// 单源完整字段和七码汇总，预期来自场景而非复制生产报告。
fn assert_report(report: &RuntimeMetricCollectionReport, id: RuntimeMetricId,
    status: RuntimeMetricInstanceStatus, counts: Option<(usize, Option<usize>)>)
{
    assert_eq!(report.instances().len(), 1);
    let item = &report.instances()[0];
    assert_eq!((item.id(), item.name(), item.kind(), item.status(), item.counts()),
        (id, "外部任务池", RuntimeMetricKind::Single, status, counts));
    for other in [RuntimeMetricInstanceStatus::Unregistered, RuntimeMetricInstanceStatus::RecordInvoked,
        RuntimeMetricInstanceStatus::RequestInFlight, RuntimeMetricInstanceStatus::DeadlineExceeded,
        RuntimeMetricInstanceStatus::ReplyClosed, RuntimeMetricInstanceStatus::RoundDeadlineSkipped,
        RuntimeMetricInstanceStatus::MetricValueOutOfRange]
    { assert_eq!(report.count(other), usize::from(other == status)); }
    assert_eq!(report.active_count(), usize::from(status != RuntimeMetricInstanceStatus::Unregistered));
    assert_eq!(report.retired_count(), usize::from(status == RuntimeMetricInstanceStatus::Unregistered));
    assert_eq!(report.sampled_count(), usize::from(counts.is_some()));
}

fn custom_pool() {
    init_runtime_metrics_reporting("边界-外部池").unwrap();
    let runner = SingleTaskRunner::<usize, ExternalPool>::new(ExternalPool::default());
    let runtime = runner.startup().unwrap();
    let weak = Arc::downgrade(&runtime.shared_pool());
    let object: &dyn RuntimeMetricRegistrable = &runtime;
    let id = register_runtime_metrics(object, "外部任务池").unwrap();
    let (sender, receiver) = bounded(3);
    let first = sender.clone();
    runtime.spawn(async move { first.try_send(1).unwrap(); 1 }).unwrap();
    let second = sender.clone();
    runtime.spawn_local(async move { second.try_send(2).unwrap(); 2 }).unwrap();
    runtime.spawn_priority(10, async move { sender.try_send(3).unwrap(); 3 }).unwrap();
    assert_report(&collect(RuntimeMetricCollectionOptions::default()), id,
        RuntimeMetricInstanceStatus::RecordInvoked, Some((3, Some(0))));
    for _ in 0..3 { runner.run_once().unwrap(); }
    let deadline = Instant::now() + LIMIT;
    let values: Vec<_> = (0..3).map(|_| receive(&receiver, deadline)).collect();
    assert_eq!(values, [1, 2, 3]);
    let (sender, receiver) = bounded(1);
    let timed = runtime.clone();
    runtime.spawn(async move { timed.timeout(0).await; timed.timeout(1).await; sender.try_send(7).unwrap(); 7 }).unwrap();
    let deadline = Instant::now() + LIMIT;
    loop {
        assert!(Instant::now() < deadline);
        runner.run_once().unwrap();
        if let Ok(value) = receiver.try_recv() { assert_eq!(value, 7); break; }
        thread::yield_now();
    }
    assert_eq!((runtime.len(), runtime.wait_len()), (0, 0));
    assert_report(&collect(RuntimeMetricCollectionOptions::default()), id,
        RuntimeMetricInstanceStatus::RecordInvoked, Some((0, Some(0))));
    assert!(unregister_runtime_metrics(id));
    drop(runtime);
    drop(runner);
    assert!(weak.upgrade().is_none());
    assert_report(&collect(RuntimeMetricCollectionOptions::default()), id,
        RuntimeMetricInstanceStatus::Unregistered, None);
}

fn getter_boundaries() {
    init_runtime_metrics_reporting("边界-getter").unwrap();
    let runner = SingleTaskRunner::<usize, ExternalPool>::new(ExternalPool::default());
    let runtime = runner.startup().unwrap();
    let control = runtime.shared_pool().control.clone();
    let id = register_runtime_metrics(&runtime, "外部任务池").unwrap();
    runtime.spawn(async { 17 }).unwrap();
    let options = RuntimeMetricCollectionOptions::new(Duration::from_millis(5), LIMIT).unwrap();
    control.action.store(1, Ordering::Release);
    let (sender, receiver) = bounded(1);
    let collector = thread::spawn(move || sender.try_send(collect_and_report_runtime_metrics(options)).unwrap());
    receive(&control.entered.1, Instant::now() + LIMIT);
    // 已进入len后再等待20毫秒，必晚于此前发放的5毫秒实例预算；不伪造相等时刻。
    thread::sleep(Duration::from_millis(20));
    control.release.0.try_send(()).unwrap();
    let report = receive(&receiver, Instant::now() + LIMIT).unwrap();
    collector.join().unwrap();
    assert_report(&report, id, RuntimeMetricInstanceStatus::DeadlineExceeded, None);
    assert_eq!(runtime.len(), 1);
    assert_report(&collect(RuntimeMetricCollectionOptions::default()), id,
        RuntimeMetricInstanceStatus::RecordInvoked, Some((1, Some(0))));

    control.action.store(2, Ordering::Release);
    let (sender, receiver) = bounded(1);
    let collector = thread::spawn(move || {
        sender.try_send(std::panic::catch_unwind(|| collect_and_report_runtime_metrics(RuntimeMetricCollectionOptions::default()))).unwrap();
    });
    let failed: Result<_, Box<dyn Any + Send>> = receive(&receiver, Instant::now() + LIMIT);
    collector.join().unwrap();
    let payload = match failed { Err(payload) => payload, Ok(_) => panic!("指定getter故障未发生") };
    assert_eq!(payload.downcast_ref::<&str>().copied(), Some(PANIC));
    assert_report(&collect(RuntimeMetricCollectionOptions::default()), id,
        RuntimeMetricInstanceStatus::RecordInvoked, Some((1, Some(0))));
    runner.run_once().unwrap();
    assert_eq!(runtime.len(), 0);
    assert!(unregister_runtime_metrics(id));
}

struct OwnedChild(Child);
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(Some(_))) { return; }
        if let Err(error) = self.0.kill() { eprintln!("终止边界子进程失败：{}", error); }
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            match self.0.try_wait() {
                Ok(Some(_)) => return,
                Err(error) => { eprintln!("回收边界子进程失败：{}", error); return; }
                Ok(None) => thread::sleep(Duration::from_millis(5)),
            }
        }
        eprintln!("边界子进程回收超过5秒");
    }
}

/// 两场景串行自启动，每子进程唯一原时钟，成功前后复查15秒／整目标180秒。
#[test]
fn test_external_pool_and_getter_boundary_matrix() {
    if let Ok(case) = std::env::var(CASE_ENV) {
        let _clock = pi_async_rt::rt::startup_global_time_loop(1).expect("每子例唯一时钟");
        eprintln!("指标边界场景：{}", case);
        match case.as_str() { "custom_pool" => custom_pool(), "getter" => getter_boundaries(), _ => panic!("未知边界场景") }
        eprintln!("指标边界场景通过：{}", case);
        return;
    }
    let target_deadline = Instant::now() + Duration::from_secs(180);
    for case in ["custom_pool", "getter"] {
        let deadline = (Instant::now() + Duration::from_secs(15)).min(target_deadline);
        let mut child = OwnedChild(Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "test_external_pool_and_getter_boundary_matrix", "--nocapture", "--test-threads=1"])
            .env(CASE_ENV, case).stdin(Stdio::null()).stdout(Stdio::inherit()).stderr(Stdio::inherit()).spawn().unwrap());
        loop {
            assert!(Instant::now() < deadline);
            if let Some(status) = child.0.try_wait().unwrap() {
                assert!(Instant::now() < deadline);
                assert!(status.success(), "边界场景失败：{}", status);
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }
    }
}
