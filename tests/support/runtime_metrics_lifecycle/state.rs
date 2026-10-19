//! MET-L-STATE-001：真实登记、旧快照、Worker及泛型场景，只经公开API消费原能力。
//!
//! 单向依赖common观测叶，不依赖SDK故障探针或本地Poller场景。
//! Weak只观察原pool存活，不清生产队列或替生产释放强引用；数量期望独立建立。

use super::common::{assert_sample, assert_summary, collect_external, options, receive,
    wait_idle, wait_local_request, wait_released, LIMIT};
use crossbeam_channel::bounded;
use pi_async_rt::{runtime_metrics::*, rt::{AsyncRuntime, AsyncValue,
    single_thread::SingleTaskRunner, serial_local_thread::LocalTaskRunner,
    worker_thread::WorkerTaskRunner, multi_thread::{ComputationalTaskPool, MultiTaskRuntimeBuilder}}};
use std::{collections::BTreeSet, sync::Arc, thread, time::Instant};

/// 固定小批量并发门仅归本模块，避免公共观测叶反向执行场景回调。
/// 所有线程已就绪后同时放行，共用3秒截止；不以重试／无截止Barrier制造通过。
fn parallel<T: Send + 'static>(count: usize,
    operation: impl Fn(usize) -> T + Send + Sync + 'static) -> Vec<T>
{
    assert!(count > 0 && count <= 8);
    let deadline = Instant::now() + LIMIT;
    let (ready_tx, ready_rx) = bounded(count);
    let (start_tx, start_rx) = bounded(count);
    let (done_tx, done_rx) = bounded(count);
    let operation = Arc::new(operation);
    let mut threads = Vec::with_capacity(count);
    for index in 0..count {
        let (ready, start, done, operation) = (ready_tx.clone(), start_rx.clone(), done_tx.clone(), operation.clone());
        threads.push(thread::spawn(move || {
            ready.try_send(index).unwrap();
            receive(&start, deadline);
            done.try_send((index, operation(index))).unwrap();
        }));
    }
    drop(ready_tx);
    drop(done_tx);
    let mut ready = BTreeSet::new();
    for _ in 0..count { assert!(ready.insert(receive(&ready_rx, deadline))); }
    assert_eq!(ready, (0..count).collect::<BTreeSet<_>>());
    for _ in 0..count { start_tx.try_send(()).unwrap(); }
    let mut results = Vec::with_capacity(count);
    for _ in 0..count { results.push(receive(&done_rx, deadline)); }
    results.sort_by_key(|(index, _)| *index);
    assert_eq!(results.iter().map(|(index, _)| *index).collect::<Vec<_>>(), (0..count).collect::<Vec<_>>());
    for thread in threads { thread.join().unwrap(); }
    results.into_iter().map(|(_, result)| result).collect()
}

/// MET-L01：8方冷操作并发，每项只调用一次；同核唯一、一次注销、不同核不合并。
/// 所有源都是真实空Single，没有虚构计数或执行线程；注销后报告不保活原pool。
pub(super) fn registry() {
    init_runtime_metrics_reporting("生命周期-登记").unwrap();
    let runtime = SingleTaskRunner::<()>::default().into_local();
    let weak = Arc::downgrade(&runtime.shared_pool());
    let shared = runtime.clone();
    let ids = parallel(8, move |_| register_runtime_metrics(&shared, "共同核心").unwrap());
    let id = ids[0];
    assert!(ids.iter().all(|&other| other == id));
    let report = collect_external();
    assert_summary(&report, 1, 0);
    assert_sample(&report, id, "共同核心", RuntimeMetricKind::Single,
        RuntimeMetricInstanceStatus::RecordInvoked, Some((0, Some(0))));
    let removed = parallel(8, move |_| unregister_runtime_metrics(id));
    assert_eq!(removed.iter().filter(|&&removed| removed).count(), 1);
    assert_eq!(removed.iter().filter(|&&removed| !removed).count(), 7);
    assert_eq!(register_runtime_metrics(&runtime, "共同核心"), Ok(id));
    assert_eq!(register_runtime_metrics(&runtime, "另一名称"), Ok(id));
    let aliases = collect_external();
    assert_summary(&aliases, 1, 0);
    assert_eq!(aliases.instances()[0].names().collect::<Vec<_>>(), ["共同核心", "另一名称"]);
    assert_eq!(report.instances()[0].names().collect::<Vec<_>>(), ["共同核心"], "原报告快照不能随登记改变");
    assert!(unregister_runtime_metrics(id));

    let cores: Arc<Vec<_>> = Arc::new((0..8).map(|_| SingleTaskRunner::<()>::default().into_local()).collect());
    let pools: Vec<_> = cores.iter().map(|runtime| Arc::downgrade(&runtime.shared_pool())).collect();
    let shared = cores.clone();
    let different = parallel(8, move |index| register_runtime_metrics(&shared[index], "同名异核").unwrap());
    let unique: BTreeSet<_> = different.iter().copied().collect();
    assert_eq!(unique.len(), 8);
    assert!(!unique.contains(&id));
    let report = collect_external();
    assert_summary(&report, 8, 1);
    assert_sample(&report, id, "共同核心", RuntimeMetricKind::Single,
        RuntimeMetricInstanceStatus::Unregistered, None);
    for &other in &different {
        assert_sample(&report, other, "同名异核", RuntimeMetricKind::Single,
            RuntimeMetricInstanceStatus::RecordInvoked, Some((0, Some(0))));
        assert!(unregister_runtime_metrics(other));
    }
    drop(cores);
    drop(runtime);
    assert!(weak.upgrade().is_none());
    assert!(pools.iter().all(|pool| pool.upgrade().is_none()));
    let retired = collect_external();
    assert_summary(&retired, 0, 9);
    for item in retired.instances() { assert_eq!(item.counts(), None); }
}

/// MET-L02：真实本地等待使旧轮持有快照，注销／新登记在其间完成。
/// 旧源仍可采样；采集返回后旧pool释放，但Report仍可读取名称／数量。
pub(super) fn snapshot() {
    init_runtime_metrics_reporting("生命周期-快照").unwrap();
    let local_runner = LocalTaskRunner::<()>::new();
    let local = local_runner.get_runtime();
    let local_id = register_runtime_metrics(&local, "旧轮本地").unwrap();
    let old_runtime = SingleTaskRunner::<()>::default().into_local();
    let old_pool = Arc::downgrade(&old_runtime.shared_pool());
    let old_id = register_runtime_metrics(&old_runtime, "旧轮单线程").unwrap();
    drop(old_runtime);
    assert!(old_pool.upgrade().is_some(), "显式登记应强保活原核心");
    let (done_tx, done_rx) = bounded(1);
    let collector = thread::spawn(move || {
        done_tx.try_send(collect_and_report_runtime_metrics(options()).unwrap()).unwrap();
    });
    // 主线程是本地唯一owner；此时它没有poll，读取len只观测原send的公共入队。
    wait_local_request(&local);
    assert!(unregister_runtime_metrics(old_id));
    assert!(old_pool.upgrade().is_some(), "旧快照尚在采集，不应提前释放来源");
    let new_runtime = SingleTaskRunner::<()>::default().into_local();
    let new_pool = Arc::downgrade(&new_runtime.shared_pool());
    let new_id = register_runtime_metrics(&new_runtime, "新轮单线程").unwrap();
    assert!(local_id < old_id && old_id < new_id);
    local_runner.poll();
    local_runner.run_once();
    let old = receive(&done_rx, Instant::now() + LIMIT);
    collector.join().unwrap();
    assert_summary(&old, 2, 0);
    assert_sample(&old, local_id, "旧轮本地", RuntimeMetricKind::Local,
        RuntimeMetricInstanceStatus::RecordInvoked, Some((0, None)));
    assert_sample(&old, old_id, "旧轮单线程", RuntimeMetricKind::Single,
        RuntimeMetricInstanceStatus::RecordInvoked, Some((0, Some(0))));
    assert!(!old.instances().iter().any(|item| item.id() == new_id));
    assert!(old_pool.upgrade().is_none(), "返回的报告不能保活旧来源");
    assert!(unregister_runtime_metrics(local_id));
    let current = collect_external();
    assert_summary(&current, 1, 2);
    assert_sample(&current, local_id, "旧轮本地", RuntimeMetricKind::Local,
        RuntimeMetricInstanceStatus::Unregistered, None);
    assert_sample(&current, old_id, "旧轮单线程", RuntimeMetricKind::Single,
        RuntimeMetricInstanceStatus::Unregistered, None);
    assert_sample(&current, new_id, "新轮单线程", RuntimeMetricKind::Single,
        RuntimeMetricInstanceStatus::RecordInvoked, Some((0, Some(0))));
    assert!(unregister_runtime_metrics(new_id));
    drop(new_runtime);
    assert!(new_pool.upgrade().is_none());
    assert_eq!(old.instances()[1].name(), "旧轮单线程");
    assert_eq!(old.instances()[1].counts(), Some((0, Some(0))));
}

/// MET-L04：原Worker真实启动，只由原run推动；原timeout和AsyncValue保持组合可用。
/// 同核Worker／Single别名不制造第二身份；关闭后观察原pool释放，不手动唤醒线程。
pub(super) fn worker() {
    init_runtime_metrics_reporting("生命周期-工作者").unwrap();
    let runner = WorkerTaskRunner::<usize>::default();
    let runner_for_loop = runner.clone();
    let runtime = runner.startup("Metrics-Lifecycle-Worker", 2 * 1024 * 1024, 10, None,
        move || {
            let started = Instant::now();
            (runner_for_loop.run().unwrap() == 0, started.elapsed())
        }, || 0);
    let pool = Arc::downgrade(&runtime.shared_pool());
    let id = register_runtime_metrics(&runtime, "真实工作者").unwrap();
    assert_eq!(register_runtime_metrics(runtime.get_worker_runtime(), "真实工作者"), Ok(id));
    let value = AsyncValue::<usize>::new();
    let waiting = value.clone();
    let copy = runtime.clone();
    let (ready_tx, ready_rx) = bounded(1);
    let (done_tx, done_rx) = bounded(1);
    runtime.spawn(async move {
        copy.timeout(0).await;
        copy.timeout(1).await;
        ready_tx.try_send(()).unwrap();
        let answer = waiting.await;
        done_tx.try_send(answer).unwrap();
        answer
    }).unwrap();
    receive(&ready_rx, Instant::now() + LIMIT);
    value.set(73);
    assert_eq!(receive(&done_rx, Instant::now() + LIMIT), 73);
    wait_idle(&runtime);
    let report = collect_external();
    assert_summary(&report, 1, 0);
    assert_sample(&report, id, "真实工作者", RuntimeMetricKind::Worker,
        RuntimeMetricInstanceStatus::RecordInvoked, Some((0, Some(0))));
    assert!(unregister_runtime_metrics(id));
    assert!(runtime.close());
    assert!(!runtime.close());
    drop(runtime);
    wait_released(&pool);
    assert_eq!(report.instances()[0].kind(), RuntimeMetricKind::Worker);
}

/// MET-L05：usize输出及非默认内置P，不把运行时固定为Output=()或默认可窃取池。
/// 任务值的精确全集独立断言；单线程原local／raw组合和计算型2worker均真实执行。
/// 多线程原close不支持停止worker，必须保持子进程隔离，不伪造线程回收。
pub(super) fn generic() {
    init_runtime_metrics_reporting("生命周期-泛型").unwrap();
    let runner = SingleTaskRunner::<usize>::default();
    let runtime = runner.startup().unwrap();
    let id = register_runtime_metrics(&runtime, "泛型单线程").unwrap();
    let (sender, receiver) = bounded(3);
    let first = sender.clone();
    runtime.spawn(async move { first.try_send(1).unwrap(); 1 }).unwrap();
    let second = sender.clone();
    runtime.spawn_local(async move { second.try_send(2).unwrap(); 2 }).unwrap();
    let facade = runtime.to_local_runtime();
    facade.spawn(async move { sender.try_send(3).unwrap(); 3 }).unwrap();
    let before = collect_external();
    assert_summary(&before, 1, 0);
    assert_sample(&before, id, "泛型单线程", RuntimeMetricKind::Single,
        RuntimeMetricInstanceStatus::RecordInvoked, Some((3, Some(0))));
    for _ in 0..3 { runner.run_once().unwrap(); }
    let deadline = Instant::now() + LIMIT;
    let values: BTreeSet<_> = (0..3).map(|_| receive(&receiver, deadline)).collect();
    assert_eq!(values, [1, 2, 3].iter().copied().collect::<BTreeSet<_>>());
    assert_eq!(runtime.len(), 0);
    assert!(unregister_runtime_metrics(id));

    let multi = MultiTaskRuntimeBuilder::new(ComputationalTaskPool::<usize>::new(2))
        .thread_prefix("Metrics-Lifecycle-Computational").init_worker_size(2).set_worker_limit(2, 2)
        .set_timer_interval(1).build();
    assert_eq!(multi.worker_len(), 2);
    let multi_id = register_runtime_metrics(&multi, "计算型泛型").unwrap();
    let (sender, receiver) = bounded(8);
    for index in 0..8 {
        let sender = sender.clone();
        multi.spawn(async move { sender.try_send(index).unwrap(); index }).unwrap();
    }
    drop(sender);
    let deadline = Instant::now() + LIMIT;
    let values: BTreeSet<_> = (0..8).map(|_| receive(&receiver, deadline)).collect();
    assert_eq!(values, (0..8).collect::<BTreeSet<_>>());
    wait_idle(&multi);
    let report = collect_external();
    assert_summary(&report, 1, 1);
    assert_sample(&report, id, "泛型单线程", RuntimeMetricKind::Single,
        RuntimeMetricInstanceStatus::Unregistered, None);
    assert_sample(&report, multi_id, "计算型泛型", RuntimeMetricKind::Multi,
        RuntimeMetricInstanceStatus::RecordInvoked, Some((0, Some(0))));
    assert!(unregister_runtime_metrics(multi_id));
}
