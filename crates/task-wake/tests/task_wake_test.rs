//! 固定目标（WakeTarget）的公开交付（dispatch）与本地入口（local route）行为验证。
use pi_async_task::{Runnable, TaskThread};
use pi_async_task_queue::{BatchAttempt, ExternalPushError, ExternalPushErrorKind, ExternalTaskQueue};
use pi_async_task_wake::{DispatchFailure, DispatchRecovery, WakeTarget};

/// 明确关闭的外部队列（external queue）；不接纳或执行任务（Task）。
#[derive(Clone)]
struct ClosedExternal;
impl ExternalTaskQueue for ClosedExternal {
    fn try_push(&self, task: Runnable) -> Result<(), ExternalPushError> {
        Err(ExternalPushError { task, cause: ExternalPushErrorKind::Closed })
    }
    fn try_take_batch(&self) -> BatchAttempt { BatchAttempt::Closed }
    fn len(&self) -> usize { 0 }
}

/// 保留失败原值（Runnable）供测试宿主（host）在原线程（thread）继续处理。
struct Recovery(std::sync::mpsc::Sender<(Runnable, DispatchFailure)>);
impl DispatchRecovery for Recovery {
    fn retain(&self, task: Runnable, reason: DispatchFailure) {
        self.0.send((task, reason)).unwrap_or_else(|_| panic!("恢复通道（recovery channel）已关闭"));
    }
}

/// @test T33/T42；@api WakeTarget::bind_local及LocalQueueGuard::drop。
/// 成功装配（bind）后正常释放，不把关闭目标（target close）混同所属域退出。
#[test]
fn test_binding_guard_drop_closes_only_its_target() {
    let owner = TaskThread::new();
    let queue = std::rc::Rc::new(std::cell::RefCell::new(pi_async_task_queue::TaskQueue::new(&owner)));
    let (tx, _rx) = std::sync::mpsc::channel();
    let target = WakeTarget::new(&owner, ClosedExternal, Recovery(tx));
    let guard = target.bind_local(&owner, queue).unwrap_or_else(|_| panic!("装配（bind）失败"));
    drop(guard);
    assert!(target.is_closed());
    assert!(!owner.is_closed());
}

/// @test T33/T36/T40/T42；@api WakeTarget::bind_local与LocalQueueGuard。
/// 同域唯一入口（local route）拒绝重复装配，释放仅关闭本目标，后续新目标可装配。
#[test]
fn test_local_binding_is_unique_and_guard_drop_allows_new_target() {
    use std::{cell::RefCell, rc::Rc};
    use pi_async_task_wake::BindError;
    let owner = TaskThread::new();
    let queue = Rc::new(RefCell::new(pi_async_task_queue::TaskQueue::new(&owner)));
    let (tx, _rx) = std::sync::mpsc::channel();
    let target = WakeTarget::new(&owner, ClosedExternal, Recovery(tx.clone()));
    let guard = target.bind_local(&owner, queue.clone()).unwrap_or_else(|_| panic!("装配（bind）失败"));
    assert_eq!(guard.target().owner_id(), owner.id());
    assert_eq!(guard.len(), Ok(0));
    assert!(matches!(target.bind_local(&owner, queue.clone()), Err(BindError::AlreadyBound)));
    let next = WakeTarget::new(&owner, ClosedExternal, Recovery(tx));
    assert!(matches!(next.bind_local(&owner, queue.clone()), Err(BindError::AlreadyBound)));
    drop(guard);
    assert!(target.is_closed());
    assert!(!owner.is_closed());
    let guard = next.bind_local(&owner, queue).unwrap_or_else(|_| panic!("重新装配（rebind）失败"));
    assert_eq!(guard.len(), Ok(0));
    drop(guard);
}

/// 暂存清理持有（Runnable），宿主（host）在所属线程（thread）关闭后取回。
struct Delivery(std::sync::mpsc::Sender<Runnable>);
impl pi_async_task::Schedule for Delivery {
    fn schedule(&self, task: Runnable) { self.0.send(task).unwrap_or_else(|_| panic!("清理通道（cleanup channel）关闭")); }
}

/// @test T37/T38/T39；@api LocalQueueGuard::push_front/push_back/pop_front。
/// 已接纳任务（admitted task）显式头尾插保序，关闭后仍能取回既有持有（Runnable）。
#[test]
fn test_explicit_local_delivery_orders_tasks_and_drains_after_close() {
    use pi_async_task::{create_local, TaskOptions};
    let owner = TaskThread::new();
    let queue = std::rc::Rc::new(std::cell::RefCell::new(pi_async_task_queue::TaskQueue::new(&owner)));
    let (tx, rx) = std::sync::mpsc::channel();
    let (recover, _recovered) = std::sync::mpsc::channel();
    let target = WakeTarget::new(&owner, ClosedExternal, Recovery(recover));
    let mut guard = target.bind_local(&owner, queue).unwrap_or_else(|_| panic!("装配（bind）失败"));
    let a = create_local(&owner, std::future::pending::<()>(), Delivery(tx.clone()), (), TaskOptions::intrusive()).unwrap_or_else(|_| panic!("创建（creation）失败"));
    let b = create_local(&owner, std::future::pending::<()>(), Delivery(tx), (), TaskOptions::intrusive()).unwrap_or_else(|_| panic!("创建（creation）失败"));
    let _a_registration = a.runnable.admit(&owner).unwrap_or_else(|_| panic!("接纳（admit）失败"));
    let _b_registration = b.runnable.admit(&owner).unwrap_or_else(|_| panic!("接纳（admit）失败"));
    let ids = [a.runnable.id(), b.runnable.id()];
    guard.push_back(a.runnable).unwrap_or_else(|_| panic!("尾插（push_back）失败"));
    guard.push_front(b.runnable).unwrap_or_else(|_| panic!("头插（push_front）失败"));
    assert_eq!(guard.len(), Ok(2));
    target.close();
    let first = guard.pop_front().unwrap().unwrap();
    let second = guard.pop_front().unwrap().unwrap();
    assert_eq!([first.id(), second.id()], [ids[1], ids[0]]);
    assert!(guard.pop_front().unwrap().is_none());
    owner.close();
    drop(first);
    drop(second);
    while let Ok(task) = rx.try_recv() { drop(task); }
}

/// @test T34/T35；@api WakeTarget::try_submit/waker_for及Schedule。
/// 未接纳业务（unadmitted work）拒绝，接纳后本地尾插；原生唤醒器（Waker）仍唤醒同一任务（Task）。
#[test]
fn test_target_local_submission_and_native_waker_keep_original_task() {
    use pi_async_task::{create_local, TaskOptions};
    use pi_async_task::definitions::RunOutcome;
    let owner = TaskThread::new();
    let queue = std::rc::Rc::new(std::cell::RefCell::new(pi_async_task_queue::TaskQueue::new(&owner)));
    let (tx, rx) = std::sync::mpsc::channel();
    let target = WakeTarget::new(&owner, ClosedExternal, Recovery(tx));
    let mut guard = target.bind_local(&owner, queue).unwrap_or_else(|_| panic!("装配（bind）失败"));
    let parts = create_local(&owner, std::future::pending::<()>(), target.clone(), (), TaskOptions::intrusive()).unwrap_or_else(|_| panic!("创建（creation）失败"));
    let id = parts.runnable.id();
    let rejected = match target.try_submit(parts.runnable) { Err(error) => error, Ok(_) => panic!("未接纳业务（unadmitted work）必须拒绝") };
    assert_eq!(rejected.cause, DispatchFailure::WrongOwner);
    assert_eq!(rejected.task.id(), id);
    let _registration = rejected.task.admit(&owner).unwrap_or_else(|_| panic!("接纳（admit）失败"));
    target.try_submit(rejected.task).unwrap_or_else(|_| panic!("交付（submit）失败"));
    assert_eq!(guard.len(), Ok(1));
    assert!(matches!(guard.pop_front().unwrap().unwrap().run(&owner), Ok(RunOutcome::Pending)));
    let waker = target.waker_for(&parts.control).unwrap();
    waker.wake_by_ref();
    assert_eq!(guard.len(), Ok(1));
    let task = guard.pop_front().unwrap().unwrap();
    assert_eq!(task.id(), id);
    owner.close();
    drop(task);
    assert!(rx.try_recv().is_err());
}

/// @test T34；@api WakeTarget::try_submit。
/// 没有本地入口（local route）时，外部拒绝（external rejection）保留原持有与明确原因。
#[test]
fn test_external_rejection_returns_original_runnable() {
    let owner = TaskThread::new();
    let (tx, _rx) = std::sync::mpsc::channel();
    let (recover, _recovered) = std::sync::mpsc::channel();
    let target = WakeTarget::new(&owner, ClosedExternal, Recovery(recover));
    let parts = pi_async_task::create_local(&owner, std::future::pending::<()>(), Delivery(tx), (), pi_async_task::TaskOptions::intrusive()).unwrap_or_else(|_| panic!("创建（creation）失败"));
    let id = parts.runnable.id();
    let _registration = parts.runnable.admit(&owner).unwrap_or_else(|_| panic!("接纳（admit）失败"));
    let error = match target.try_submit(parts.runnable) { Err(error) => error, Ok(_) => panic!("外部队列（external queue）必须拒绝") };
    assert_eq!(error.cause, DispatchFailure::External(ExternalPushErrorKind::Closed));
    assert_eq!(error.task.id(), id);
    owner.close();
    drop(error.task);
}

/// @test T34/T41；@api WakeTarget::try_submit与LocalQueueGuard::import_external_once。
/// 默认外部任务队列（external task queue）接管跨线程提交，批次按顺序追加本地尾部。
#[test]
fn test_remote_delivery_imports_one_batch_after_local_tail() {
    use pi_async_task::{create_local, TaskOptions};
    use pi_async_task_queue::{ExternalQueueManager, MpscQueueOptions, MpscTaskQueue};
    use pi_async_task_wake::ImportOutcome;
    let owner = TaskThread::new();
    let queue = std::rc::Rc::new(std::cell::RefCell::new(pi_async_task_queue::TaskQueue::new(&owner)));
    let external_owner = MpscTaskQueue::new(MpscQueueOptions::default()).unwrap();
    let external = external_owner.handle();
    let (tx, _rx) = std::sync::mpsc::channel();
    let target = WakeTarget::new(&owner, external.clone(), Recovery(tx));
    let mut guard = target.bind_local(&owner, queue.clone()).unwrap_or_else(|_| panic!("装配（bind）失败"));
    let a = create_local(&owner, std::future::pending::<()>(), target.clone(), (), TaskOptions::intrusive_local_and_mpsc()).unwrap_or_else(|_| panic!("创建（creation）失败"));
    let b = create_local(&owner, std::future::pending::<()>(), target.clone(), (), TaskOptions::intrusive_local_and_mpsc()).unwrap_or_else(|_| panic!("创建（creation）失败"));
    let c = create_local(&owner, std::future::pending::<()>(), target.clone(), (), TaskOptions::intrusive_local_and_mpsc()).unwrap_or_else(|_| panic!("创建（creation）失败"));
    let registrations = [a.runnable.admit(&owner).unwrap_or_else(|_| panic!("接纳（admit）失败")), b.runnable.admit(&owner).unwrap_or_else(|_| panic!("接纳（admit）失败")), c.runnable.admit(&owner).unwrap_or_else(|_| panic!("接纳（admit）失败"))];
    let ids = [a.runnable.id(), b.runnable.id(), c.runnable.id()];
    target.try_submit(a.runnable).unwrap_or_else(|_| panic!("交付（submit）失败"));
    let remote = target.clone();
    std::thread::spawn(move || {
        remote.try_submit(b.runnable).unwrap_or_else(|_| panic!("远程交付（remote dispatch）失败"));
        remote.try_submit(c.runnable).unwrap_or_else(|_| panic!("远程交付（remote dispatch）失败"));
    }).join().unwrap();
    assert_eq!(external.len(), 2);
    assert_eq!(guard.len(), Ok(1));
    let borrowed = queue.borrow_mut();
    assert!(matches!(guard.import_external_once(), ImportOutcome::Busy));
    assert_eq!(external.len(), 2);
    drop(borrowed);
    assert!(matches!(guard.import_external_once(), ImportOutcome::Imported(2)));
    assert_eq!(external.len(), 0);
    let tasks = [guard.pop_front().unwrap().unwrap(), guard.pop_front().unwrap().unwrap(), guard.pop_front().unwrap().unwrap()];
    assert_eq!(tasks.each_ref().map(Runnable::id), ids);
    owner.close();
    drop(tasks);
    drop(registrations);
}

/// @test T41；@api LocalQueueGuard::import_external_once。
/// 本地借用（borrow）冲突立即Busy，释放后保留外部Closed分类，不重试。
#[test]
fn test_import_checks_local_access_before_external_attempt() {
    use pi_async_task_wake::ImportOutcome;
    let owner = TaskThread::new();
    let queue = std::rc::Rc::new(std::cell::RefCell::new(pi_async_task_queue::TaskQueue::new(&owner)));
    let (tx, _rx) = std::sync::mpsc::channel();
    let target = WakeTarget::new(&owner, ClosedExternal, Recovery(tx));
    let mut guard = target.bind_local(&owner, queue.clone()).unwrap_or_else(|_| panic!("装配（bind）失败"));
    let borrowed = queue.borrow_mut();
    assert!(matches!(guard.import_external_once(), ImportOutcome::Busy));
    drop(borrowed);
    assert!(matches!(guard.import_external_once(), ImportOutcome::Closed));
}

/// @test T34/T119；@api Runnable::drop/WakeTarget的Schedule连接。
/// 未接纳退出清理（unadmitted cleanup）沿同目标返回原线程（thread），不轮询（poll）或隐式接纳（admit）。
#[test]
fn test_unadmitted_abandonment_routes_cleanup_without_polling() {
    use pi_async_task::definitions::{ExecutionStatus, RunOutcome, TerminationKind};
    struct NeverPoll(std::rc::Rc<std::cell::Cell<usize>>);
    impl std::future::Future for NeverPoll {
        type Output = ();
        fn poll(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<()> { panic!("清理（cleanup）不得轮询（poll）"); }
    }
    impl Drop for NeverPoll { fn drop(&mut self) { self.0.set(self.0.get() + 1); } }
    let owner = TaskThread::new();
    let drops = std::rc::Rc::new(std::cell::Cell::new(0));
    let queue = std::rc::Rc::new(std::cell::RefCell::new(pi_async_task_queue::TaskQueue::new(&owner)));
    let (tx, rx) = std::sync::mpsc::channel();
    let target = WakeTarget::new(&owner, ClosedExternal, Recovery(tx));
    let mut guard = target.bind_local(&owner, queue).unwrap_or_else(|_| panic!("装配（bind）失败"));
    let parts = pi_async_task::create_local(&owner, NeverPoll(drops.clone()), target.clone(), (), pi_async_task::TaskOptions::intrusive()).unwrap_or_else(|_| panic!("创建（creation）失败"));
    let id = parts.runnable.id();
    drop(parts.runnable);
    assert_eq!(drops.get(), 0);
    let cleanup = guard.pop_front().unwrap().unwrap();
    assert_eq!(cleanup.id(), id);
    assert!(!cleanup.snapshot().admitted);
    assert_eq!(cleanup.snapshot().execution, ExecutionStatus::Exiting);
    assert!(matches!(cleanup.run(&owner), Ok(RunOutcome::Finished(TerminationKind::Abandoned))));
    assert_eq!(drops.get(), 1);
    assert_eq!(owner.live_tasks(), 0);
    assert!(rx.try_recv().is_err());
}

/// @test T31/T34；@api WakeTarget::close及Schedule。
/// 关闭拒绝（closed rejection）的清理持有（Runnable）交恢复接口（DispatchRecovery），由宿主（host）原线程（thread）实际清理。
#[test]
fn test_closed_target_recovery_keeps_cleanup_for_owner() {
    let owner = TaskThread::new();
    let (tx, rx) = std::sync::mpsc::channel();
    let target = WakeTarget::new(&owner, ClosedExternal, Recovery(tx));
    let parts = pi_async_task::create_local(&owner, std::future::pending::<()>(), target.clone(), (), pi_async_task::TaskOptions::plain()).unwrap_or_else(|_| panic!("创建（creation）失败"));
    let id = parts.runnable.id();
    target.close();
    std::thread::spawn(move || drop(parts.runnable)).join().unwrap();
    let (cleanup, cause) = rx.try_recv().unwrap();
    assert_eq!(cause, DispatchFailure::Closed);
    assert_eq!(cleanup.id(), id);
    assert!(!cleanup.snapshot().admitted);
    assert!(cleanup.run(&owner).is_ok());
    assert_eq!(owner.live_tasks(), 0);
    assert!(rx.try_recv().is_err());
}

/// @test T34；@api WakeTarget::try_submit。
/// 同线程（thread）借用冲突时回送到同一外部任务队列（external task queue）。
#[test]
fn test_local_borrow_conflict_falls_back_to_same_external_queue() {
    use pi_async_task_queue::{ExternalQueueManager, MpscQueueOptions, MpscTaskQueue};
    let owner = TaskThread::new();
    let queue = std::rc::Rc::new(std::cell::RefCell::new(pi_async_task_queue::TaskQueue::new(&owner)));
    let external_owner = MpscTaskQueue::new(MpscQueueOptions::default()).unwrap();
    let external = external_owner.handle();
    let (tx, _rx) = std::sync::mpsc::channel();
    let target = WakeTarget::new(&owner, external.clone(), Recovery(tx));
    let _guard = target.bind_local(&owner, queue.clone()).unwrap_or_else(|_| panic!("装配（bind）失败"));
    let parts = pi_async_task::create_local(&owner, std::future::pending::<()>(), target.clone(), (), pi_async_task::TaskOptions::intrusive_local_and_mpsc()).unwrap_or_else(|_| panic!("创建（creation）失败"));
    let _registration = parts.runnable.admit(&owner).unwrap_or_else(|_| panic!("接纳（admit）失败"));
    let id = parts.runnable.id();
    let borrowed = queue.borrow_mut();
    target.try_submit(parts.runnable).unwrap_or_else(|_| panic!("回退交付（fallback dispatch）失败"));
    assert_eq!(external.len(), 1);
    drop(borrowed);
    assert_eq!(queue.borrow().len(), 0);
    let batch = match external.try_take_batch() { BatchAttempt::Ready(batch) => batch, _ => panic!("原值（Runnable）丢失") };
    let task = batch.into_iter().next().unwrap();
    assert_eq!(task.id(), id);
    owner.close();
    drop(task);
}

/// @test T27/T28/T29/T30/T31/T32；@api WakeTarget::new/clone/owner_id/external/close/is_closed。
/// 目标克隆（Clone）共享固定身份、关闭状态（closed state）及原共享句柄（shared handle）。
#[test]
fn test_target_clones_share_identity_and_permanent_close() {
    let owner = TaskThread::new();
    let (tx, _rx) = std::sync::mpsc::channel();
    let target = WakeTarget::new(&owner, ClosedExternal, Recovery(tx));
    assert_eq!(target.owner_id(), owner.id());
    assert!(!target.is_closed());
    let clone = target.clone();
    assert_eq!(clone.owner_id(), owner.id());
    assert!(std::ptr::eq(target.external(), clone.external()));
    target.close();
    clone.close();
    assert!(target.is_closed());
    assert!(clone.is_closed());
    assert!(!owner.is_closed());
    owner.close();
    let (tx, _rx) = std::sync::mpsc::channel();
    assert!(WakeTarget::new(&owner, ClosedExternal, Recovery(tx)).is_closed());
}
