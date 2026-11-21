//! 任务工厂（TaskFactory）只组织原创建能力（creation capabilities），不隐式调度（schedule）或通知（notify）。
use pi_async_runtime_core::task::{TaskFactory, TaskNotifier};
use pi_async_task::{definitions::QueueSupport, Runnable, Schedule};
use pi_async_task_queue::notify::QueueNotifier;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

/// 记录原调度能力（schedule capability）的克隆（clone）及释放（drop）；本组只构造工厂（factory），不得调度（schedule）任何任务（Task）。
struct ScheduleProbe { clones: Arc<AtomicUsize>, drops: Arc<AtomicUsize> }
impl Clone for ScheduleProbe {
    fn clone(&self) -> Self {
        self.clones.fetch_add(1, Ordering::Relaxed);
        Self { clones: self.clones.clone(), drops: self.drops.clone() }
    }
}
impl Drop for ScheduleProbe { fn drop(&mut self) { self.drops.fetch_add(1, Ordering::Relaxed); } }
impl Schedule for ScheduleProbe {
    fn schedule(&self, _: Runnable) { panic!("构造工厂不得调度（factory construction must not schedule）"); }
}

/// 仅观察通知（notification）是否发生；不替代任何任务推进（task progress）或来源接收（source acceptance）。
#[derive(Clone)]
struct NotificationProbe(Arc<AtomicUsize>);
impl QueueNotifier for NotificationProbe { fn notify(&self) { self.0.fetch_add(1, Ordering::Relaxed); } }
impl TaskNotifier for NotificationProbe {}

/// 验证 TaskFactory::try_new 分配独立非零服务身份（runtime identity），保存输入能力（capabilities）且不克隆（clone）或通知（notify）。
/// 工厂释放（factory drop）归还原调度能力（schedule capability），不创建任务（Task）或启动服务（service）。
#[test]
fn test_task_factory_prepares_identity_without_starting_work() {
    let clones = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let notifications = Arc::new(AtomicUsize::new(0));
    let first = TaskFactory::try_new(ScheduleProbe { clones: clones.clone(), drops: drops.clone() },
        NotificationProbe(notifications.clone()), QueueSupport::Plain).unwrap();
    let second = TaskFactory::try_new(ScheduleProbe { clones: clones.clone(), drops: drops.clone() },
        NotificationProbe(notifications.clone()), QueueSupport::IntrusiveLocalAndMpsc).unwrap();
    assert_ne!(first.runtime_id(), second.runtime_id());
    assert_ne!(first.runtime_id().get(), 0);
    assert_eq!(clones.load(Ordering::Relaxed), 0);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    assert_eq!(notifications.load(Ordering::Relaxed), 0);
    drop(first); drop(second);
    assert_eq!(drops.load(Ordering::Relaxed), 2);
}

/// 验证 TaskFactory::with_notifier 复用原服务身份（runtime identity），只克隆原调度能力（Schedule），不分配新身份（identity）或通知（notify）。
/// 原工厂（factory）先释放后，新组合（composition）仍保有其原能力（capability）；各份最终恰一次释放（drop）。
#[test]
fn test_task_factory_with_notifier_preserves_original_identity() {
    let clones = Arc::new(AtomicUsize::new(0)); let drops = Arc::new(AtomicUsize::new(0));
    let first_notifications = Arc::new(AtomicUsize::new(0)); let second_notifications = Arc::new(AtomicUsize::new(0));
    let factory = TaskFactory::try_new(ScheduleProbe { clones: clones.clone(), drops: drops.clone() },
        NotificationProbe(first_notifications.clone()), QueueSupport::IntrusiveLocalAndMpsc).unwrap();
    let id = factory.runtime_id();
    let combined = factory.with_notifier(NotificationProbe(second_notifications.clone()));
    assert_eq!(combined.runtime_id(), id);
    assert_eq!(clones.load(Ordering::Relaxed), 1);
    assert_eq!(first_notifications.load(Ordering::Relaxed), 0);
    assert_eq!(second_notifications.load(Ordering::Relaxed), 0);
    drop(factory); assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(combined.runtime_id(), id);
    drop(combined); assert_eq!(drops.load(Ordering::Relaxed), 2);
}

/// 验证 TaskFactory::clone 保持原服务身份（runtime identity）及原能力（capabilities），不隐式通知（notify）或生成新服务（service）。
#[test]
fn test_task_factory_clone_keeps_original_identity_and_capabilities() {
    let clones = Arc::new(AtomicUsize::new(0)); let drops = Arc::new(AtomicUsize::new(0));
    let notifications = Arc::new(AtomicUsize::new(0));
    let factory = TaskFactory::try_new(ScheduleProbe { clones: clones.clone(), drops: drops.clone() },
        NotificationProbe(notifications.clone()), QueueSupport::Plain).unwrap();
    let copy = factory.clone();
    assert_eq!(factory.runtime_id(), copy.runtime_id());
    assert_eq!(clones.load(Ordering::Relaxed), 1);
    assert_eq!(notifications.load(Ordering::Relaxed), 0);
    drop(factory); assert_eq!(drops.load(Ordering::Relaxed), 1);
    drop(copy); assert_eq!(drops.load(Ordering::Relaxed), 2);
}

/// 原异步计算（Future）的有限观察值（probe），含禁止移动标记（PhantomPinned），不要求业务类型（business type）支持调试（Debug）或克隆（Clone）。
struct CreateProbe { polls: Arc<AtomicUsize>, drops: Arc<AtomicUsize>, _pinned: std::marker::PhantomPinned }
impl std::future::Future for CreateProbe {
    type Output = Result<u32, &'static str>;
    fn poll(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        self.as_ref().get_ref().polls.fetch_add(1, Ordering::Relaxed);
        std::task::Poll::Ready(Err("business error"))
    }
}
impl Drop for CreateProbe { fn drop(&mut self) { self.drops.fetch_add(1, Ordering::Relaxed); } }

/// 原附加数据（metadata）的身份（identity）与释放（drop）观察；盒装原值（Box）用于比较移动后地址（address），不要求克隆（Clone）。
struct MetadataProbe { value: Box<usize>, drops: Arc<AtomicUsize> }
impl Drop for MetadataProbe { fn drop(&mut self) { self.drops.fetch_add(1, Ordering::Relaxed); } }

/// 验证 TaskFactory::create 使用原四类队列能力（queue capabilities）和原附加数据（metadata），创建（creation）不轮询（poll）、接纳（admit）、通知（notify）或提交（submit）。
/// 放弃唯一待提交持有（UnsubmittedTask）由原生机制（native mechanism）恰一次清理（cleanup），保留结果句柄（result handle）不拖延异步计算（Future）的释放（drop）。
#[test]
fn test_task_factory_create_keeps_original_task_and_defers_execution() {
    use pi_async_runtime_core::TaskConfig;
    use pi_async_task::definitions::{CancelRequestStatus, FutureDropStatus, TaskPhase, TerminationKind};
    for queue in [QueueSupport::Plain, QueueSupport::IntrusiveLocal, QueueSupport::IntrusiveMpsc, QueueSupport::IntrusiveLocalAndMpsc] {
        let clones = Arc::new(AtomicUsize::new(0)); let schedule_drops = Arc::new(AtomicUsize::new(0));
        let notifications = Arc::new(AtomicUsize::new(0));
        let polls = Arc::new(AtomicUsize::new(0)); let future_drops = Arc::new(AtomicUsize::new(0));
        let metadata_drops = Arc::new(AtomicUsize::new(0));
        let factory = TaskFactory::try_new(ScheduleProbe { clones: clones.clone(), drops: schedule_drops.clone() },
            NotificationProbe(notifications.clone()), queue).unwrap();
        let metadata = MetadataProbe { value: Box::new(17), drops: metadata_drops.clone() };
        let original = std::ptr::from_ref(metadata.value.as_ref());
        let mut config = TaskConfig::new(metadata); config.capture_panic_backtrace = true;
        let (task, handle) = factory.create(CreateProbe { polls: polls.clone(), drops: future_drops.clone(), _pinned: std::marker::PhantomPinned }, config).unwrap();
        assert_eq!(task.id(), handle.id());
        assert_eq!(handle.snapshot().id, task.id());
        assert_eq!(handle.snapshot().phase, TaskPhase::Preparing);
        assert_eq!(handle.snapshot().owner, None);
        assert!(!handle.snapshot().admitted);
        assert_eq!(handle.snapshot().cancellation, CancelRequestStatus::NotRecorded);
        assert_eq!(std::ptr::from_ref(handle.metadata().value.as_ref()), original);
        assert_eq!(*handle.metadata().value, 17);
        assert_eq!(clones.load(Ordering::Relaxed), 1);
        assert_eq!(polls.load(Ordering::Relaxed), 0); assert_eq!(future_drops.load(Ordering::Relaxed), 0);
        assert_eq!(notifications.load(Ordering::Relaxed), 0);
        drop(task);
        assert_eq!(future_drops.load(Ordering::Relaxed), 1);
        assert_eq!(handle.snapshot().termination, Some(TerminationKind::Abandoned));
        assert_eq!(handle.snapshot().future_drop, FutureDropStatus::Confirmed);
        assert_eq!(metadata_drops.load(Ordering::Relaxed), 0);
        drop(handle);
        assert_eq!(metadata_drops.load(Ordering::Relaxed), 1);
        assert_eq!(schedule_drops.load(Ordering::Relaxed), 1);
        drop(factory);
        assert_eq!(schedule_drops.load(Ordering::Relaxed), 2);
        assert_eq!(polls.load(Ordering::Relaxed), 0); assert_eq!(notifications.load(Ordering::Relaxed), 0);
    }
}

/// 验证 TaskHandle::control_handle 经原控制克隆（control clone）保持同一任务身份（TaskId）与只读事实（snapshot）。
/// 克隆（clone）及普通释放（drop）不取消（cancel）或调度（schedule）；原值（Future）由唯一待提交持有（UnsubmittedTask）的原生放弃（native abandon）清理。
#[test]
fn test_control_clone_preserves_identity_without_cancellation_or_future_retention() {
    use pi_async_runtime_core::TaskConfig;
    use pi_async_task::definitions::{CancelRequestStatus, FutureDropStatus};
    let clones = Arc::new(AtomicUsize::new(0)); let schedule_drops = Arc::new(AtomicUsize::new(0));
    let notifications = Arc::new(AtomicUsize::new(0));
    let polls = Arc::new(AtomicUsize::new(0)); let drops = Arc::new(AtomicUsize::new(0));
    let factory = TaskFactory::try_new(ScheduleProbe { clones: clones.clone(), drops: schedule_drops },
        NotificationProbe(notifications.clone()), QueueSupport::Plain).unwrap();
    let (task, handle) = factory.create(CreateProbe { polls: polls.clone(), drops: drops.clone(), _pinned: std::marker::PhantomPinned }, TaskConfig::new(())).unwrap();
    let control = handle.control_handle();
    let copy = control.clone();
    let query = copy.query_handle();
    assert_eq!(task.id(), control.id()); assert_eq!(control.id(), copy.id()); assert_eq!(query.id(), task.id());
    assert_eq!(copy.snapshot().cancellation, CancelRequestStatus::NotRecorded);
    assert_eq!(copy.snapshot().future_drop, FutureDropStatus::Unconfirmed);
    drop(control); drop(handle);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    assert_eq!(copy.snapshot().cancellation, CancelRequestStatus::NotRecorded);
    drop(task);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(copy.snapshot().future_drop, FutureDropStatus::Confirmed);
    assert_eq!(query.snapshot().future_drop, FutureDropStatus::Confirmed);
    assert_eq!(polls.load(Ordering::Relaxed), 0); assert_eq!(notifications.load(Ordering::Relaxed), 0);
    assert_eq!(clones.load(Ordering::Relaxed), 1);
    drop(copy); drop(query);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

/// !Send／!Unpin 原本地异步计算（local Future）及 !Send 输出（output）；只由真实所属域（TaskThread）负责释放（drop）。
struct LocalCreateProbe {
    value: std::rc::Rc<std::cell::Cell<usize>>,
    polls: std::rc::Rc<std::cell::Cell<usize>>,
    drops: std::rc::Rc<std::cell::Cell<usize>>,
    thread: std::thread::ThreadId,
    _pinned: std::marker::PhantomPinned,
}
impl std::future::Future for LocalCreateProbe {
    type Output = std::rc::Rc<std::cell::Cell<usize>>;
    fn poll(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let this = self.as_ref().get_ref();
        assert_eq!(this.thread, std::thread::current().id());
        this.polls.set(this.polls.get() + 1);
        std::task::Poll::Ready(this.value.clone())
    }
}
impl Drop for LocalCreateProbe {
    fn drop(&mut self) { assert_eq!(self.thread, std::thread::current().id()); self.drops.set(self.drops.get() + 1); }
}

/// 验证 TaskFactory::create_local 不要求原值（Future）或输出（output）满足 Send／Unpin，不依赖当前公共环境（current public context）。
/// 原所属域（TaskThread）在创建时登记清理责任（cleanup responsibility），关闭（close）恰一次清理；已关闭拒绝（OwnerClosed）返还原值与完整配置（config）。
#[test]
fn test_task_factory_local_creation_and_closed_rejection_keep_original_values() {
    use pi_async_runtime_core::{CreateErrorKind, TaskConfig};
    use pi_async_task::{definitions::{BuildErrorKind, FutureDropStatus}, TaskThread};
    use std::{cell::Cell, rc::Rc};
    let clones = Arc::new(AtomicUsize::new(0)); let schedule_drops = Arc::new(AtomicUsize::new(0));
    let notifications = Arc::new(AtomicUsize::new(0));
    let factory = TaskFactory::try_new(ScheduleProbe { clones: clones.clone(), drops: schedule_drops.clone() },
        NotificationProbe(notifications.clone()), QueueSupport::IntrusiveLocalAndMpsc).unwrap();
    let owner = TaskThread::new();
    let value = Rc::new(Cell::new(7)); let polls = Rc::new(Cell::new(0)); let drops = Rc::new(Cell::new(0));
    let (task, handle) = factory.create_local(&owner, LocalCreateProbe { value: value.clone(), polls: polls.clone(), drops: drops.clone(),
        thread: std::thread::current().id(), _pinned: std::marker::PhantomPinned }, TaskConfig::new(23_u32)).unwrap();
    assert_eq!(task.id(), handle.id()); assert_eq!(task.snapshot().owner, Some(owner.id()));
    assert!(!task.snapshot().admitted); assert_eq!(owner.live_tasks(), 1);
    assert_eq!(*handle.metadata(), 23); assert_eq!(polls.get(), 0); assert_eq!(drops.get(), 0);
    owner.close();
    assert_eq!(owner.live_tasks(), 0); assert_eq!(drops.get(), 1);
    assert_eq!(handle.snapshot().future_drop, FutureDropStatus::Confirmed);
    drop(task); drop(handle);
    assert_eq!(drops.get(), 1);
    let metadata_drops = Arc::new(AtomicUsize::new(0));
    let metadata = MetadataProbe { value: Box::new(29), drops: metadata_drops.clone() };
    let original_metadata = std::ptr::from_ref(metadata.value.as_ref());
    let mut config = TaskConfig::new(metadata); config.capture_panic_backtrace = true;
    let rejected = factory.create_local(&owner, LocalCreateProbe { value: value.clone(), polls: polls.clone(), drops: drops.clone(),
        thread: std::thread::current().id(), _pinned: std::marker::PhantomPinned }, config).err().unwrap();
    let (future, config, cause) = rejected.into_parts();
    assert!(matches!(cause, CreateErrorKind::Task(BuildErrorKind::OwnerClosed)));
    assert!(Rc::ptr_eq(&future.value, &value)); assert_eq!(future.value.get(), 7);
    assert_eq!(std::ptr::from_ref(config.metadata.value.as_ref()), original_metadata);
    assert_eq!(*config.metadata.value, 29); assert!(config.capture_panic_backtrace);
    assert_eq!(polls.get(), 0); assert_eq!(drops.get(), 1); assert_eq!(metadata_drops.load(Ordering::Relaxed), 0);
    assert_eq!(clones.load(Ordering::Relaxed), 2); assert_eq!(schedule_drops.load(Ordering::Relaxed), 2);
    assert_eq!(notifications.load(Ordering::Relaxed), 0);
    drop(future); drop(config);
    assert_eq!(drops.get(), 2); assert_eq!(metadata_drops.load(Ordering::Relaxed), 1);
    drop(factory); assert_eq!(schedule_drops.load(Ordering::Relaxed), 3);
}

/// 通知能力（notifier）无需解除固定（Unpin）；公开结果句柄（TaskHandle）只对原结果持有（JoinHandle）作安全投影（safe projection）。
#[derive(Clone)]
struct PinnedNotifier { calls: Arc<AtomicUsize>, _pinned: std::marker::PhantomPinned }
impl QueueNotifier for PinnedNotifier { fn notify(&self) { self.calls.fetch_add(1, Ordering::Relaxed); } }
impl TaskNotifier for PinnedNotifier {}

/// 验证 TaskHandle::poll 仅领取原结果（result），未提交时为挂起（Pending）且不隐式推进（progress）。
/// 普通放弃（Abandoned）、本地关闭取消（Cancelled）及重复领取（AlreadyTaken）逐项保持原分类（classification）；通知能力（notifier）无需 Unpin。
#[test]
fn test_task_handle_wait_does_not_submit_and_preserves_native_errors() {
    use pi_async_runtime_core::TaskConfig;
    use pi_async_task::{TaskError, TaskThread};
    use std::{future::Future, pin::Pin, task::{Context, Poll, Waker}, cell::Cell, rc::Rc};
    let clones = Arc::new(AtomicUsize::new(0)); let schedule_drops = Arc::new(AtomicUsize::new(0));
    let notifications = Arc::new(AtomicUsize::new(0));
    let polls = Arc::new(AtomicUsize::new(0)); let drops = Arc::new(AtomicUsize::new(0));
    let factory = TaskFactory::try_new(ScheduleProbe { clones, drops: schedule_drops },
        PinnedNotifier { calls: notifications.clone(), _pinned: std::marker::PhantomPinned }, QueueSupport::Plain).unwrap();
    let (task, mut handle) = factory.create(CreateProbe { polls: polls.clone(), drops: drops.clone(), _pinned: std::marker::PhantomPinned }, TaskConfig::new(())).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(Pin::new(&mut handle).poll(&mut cx).is_pending());
    assert_eq!(polls.load(Ordering::Relaxed), 0); assert_eq!(drops.load(Ordering::Relaxed), 0);
    assert_eq!(notifications.load(Ordering::Relaxed), 0);
    drop(task);
    assert!(matches!(Pin::new(&mut handle).poll(&mut cx), Poll::Ready(Err(TaskError::Abandoned))));
    assert!(matches!(Pin::new(&mut handle).poll(&mut cx), Poll::Ready(Err(TaskError::AlreadyTaken))));
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    let owner = TaskThread::new(); let local_polls = Rc::new(Cell::new(0)); let local_drops = Rc::new(Cell::new(0));
    let (task, mut handle) = factory.create_local(&owner, LocalCreateProbe { value: Rc::new(Cell::new(1)), polls: local_polls.clone(), drops: local_drops.clone(),
        thread: std::thread::current().id(), _pinned: std::marker::PhantomPinned }, TaskConfig::new(())).unwrap();
    assert!(Pin::new(&mut handle).poll(&mut cx).is_pending());
    assert_eq!(local_polls.get(), 0); assert_eq!(local_drops.get(), 0);
    owner.close();
    assert!(matches!(Pin::new(&mut handle).poll(&mut cx), Poll::Ready(Err(TaskError::Cancelled))));
    assert!(matches!(Pin::new(&mut handle).poll(&mut cx), Poll::Ready(Err(TaskError::AlreadyTaken))));
    drop(task); drop(handle);
    assert_eq!(local_polls.get(), 0); assert_eq!(local_drops.get(), 1);
    assert_eq!(notifications.load(Ordering::Relaxed), 0);
}

/// 同步测试宿主（synchronous test host）的真实外部身份（external waiter identity）；本用例不轮询（poll）等待值（wait future）。
struct ExternalIdentity { reads: AtomicUsize }
impl pi_async_task::ExternalWaitSource for ExternalIdentity {
    fn is_alive(&self) -> bool { self.reads.fetch_add(1, Ordering::Relaxed); true }
}

/// 验证 TaskControl::cancel_and_wait 与 cancel_and_wait_from 的构造（construction）惰性（lazy）及未轮询释放（unpolled drop）。
/// 原取消状态（cancellation state）、通知（notification）及宿主身份查询（host identity query）均不因构造（construction）变化。
#[test]
fn test_cancel_wait_construction_and_unpolled_drop_do_not_cancel() {
    use pi_async_runtime_core::TaskConfig;
    use pi_async_task::{ExternalWaiter, definitions::CancelRequestStatus};
    let notifications = Arc::new(AtomicUsize::new(0));
    let polls = Arc::new(AtomicUsize::new(0)); let drops = Arc::new(AtomicUsize::new(0));
    let factory = TaskFactory::try_new(ScheduleProbe { clones: Arc::new(AtomicUsize::new(0)), drops: Arc::new(AtomicUsize::new(0)) },
        NotificationProbe(notifications.clone()), QueueSupport::Plain).unwrap();
    let (task, handle) = factory.create(CreateProbe { polls: polls.clone(), drops: drops.clone(), _pinned: std::marker::PhantomPinned }, TaskConfig::new(())).unwrap();
    let control = handle.control_handle();
    let host = Arc::new(ExternalIdentity { reads: AtomicUsize::new(0) });
    let external = ExternalWaiter::new(host.clone());
    let ordinary = control.cancel_and_wait();
    let with_external = control.cancel_and_wait_from(&external);
    assert_eq!(control.snapshot().cancellation, CancelRequestStatus::NotRecorded);
    assert_eq!(host.reads.load(Ordering::Relaxed), 0); assert_eq!(notifications.load(Ordering::Relaxed), 0);
    assert_eq!(polls.load(Ordering::Relaxed), 0); assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(ordinary); drop(with_external);
    assert_eq!(control.snapshot().cancellation, CancelRequestStatus::NotRecorded);
    assert_eq!(host.reads.load(Ordering::Relaxed), 0); assert_eq!(notifications.load(Ordering::Relaxed), 0);
    drop(task); drop(handle); drop(control);
    assert_eq!(drops.load(Ordering::Relaxed), 1); assert_eq!(polls.load(Ordering::Relaxed), 0);
}

/// 取消通知（cancellation notification）仅借原快照（snapshot）观察取消已记录（Recorded）；异常通知（panicking notification）不得重试（retry）。
#[derive(Clone)]
struct CancelNotificationProbe { notifications: Arc<AtomicUsize>, snapshots: Arc<AtomicUsize>, panic: bool }
impl QueueNotifier for CancelNotificationProbe {
    fn notify(&self) { self.notifications.fetch_add(1, Ordering::Relaxed); }
}
impl TaskNotifier for CancelNotificationProbe {
    fn notify_cancel(&self, snapshot: impl FnOnce() -> pi_async_task::definitions::TaskSnapshot) {
        assert_eq!(snapshot().cancellation, pi_async_task::definitions::CancelRequestStatus::Recorded);
        self.snapshots.fetch_add(1, Ordering::Relaxed);
        self.notify();
        if self.panic { std::panic::panic_any(654_u32); }
    }
}

/// 验证 TaskControl::cancel 先记录原取消事实（native cancellation fact）再通知（notify），重复请求（repeated request）不重复清理（cleanup）。
/// 通知异常（notification panic）传播原载荷（payload），不再次通知或将已记录事实（recorded fact）回滚（rollback）；清理仍由唯一原持有（Runnable）放弃负责。
#[test]
fn test_control_cancel_notifies_after_native_fact_and_does_not_retry_panics() {
    use pi_async_runtime_core::TaskConfig;
    use pi_async_task::definitions::CancelRequestStatus;
    for panic in [false, true] {
        let notifications = Arc::new(AtomicUsize::new(0)); let snapshots = Arc::new(AtomicUsize::new(0));
        let polls = Arc::new(AtomicUsize::new(0)); let drops = Arc::new(AtomicUsize::new(0));
        let factory = TaskFactory::try_new(ScheduleProbe { clones: Arc::new(AtomicUsize::new(0)), drops: Arc::new(AtomicUsize::new(0)) },
            CancelNotificationProbe { notifications: notifications.clone(), snapshots: snapshots.clone(), panic }, QueueSupport::Plain).unwrap();
        let (task, handle) = factory.create(CreateProbe { polls: polls.clone(), drops: drops.clone(), _pinned: std::marker::PhantomPinned }, TaskConfig::new(())).unwrap();
        let control = handle.control_handle();
        for expected in [1, 2] {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| control.cancel()));
            if panic { assert_eq!(result.unwrap_err().downcast_ref::<u32>(), Some(&654)); }
            else { assert!(result.is_ok()); }
            assert_eq!(notifications.load(Ordering::Relaxed), expected);
            assert_eq!(snapshots.load(Ordering::Relaxed), expected);
            assert_eq!(control.snapshot().cancellation, CancelRequestStatus::Recorded);
            assert_eq!(drops.load(Ordering::Relaxed), 0); assert_eq!(polls.load(Ordering::Relaxed), 0);
        }
        drop(task); drop(handle); drop(control);
        assert_eq!(drops.load(Ordering::Relaxed), 1); assert_eq!(polls.load(Ordering::Relaxed), 0);
        assert_eq!(notifications.load(Ordering::Relaxed), 2);
    }
}

/// 只在观测特性（observe feature）下，原生取消记录回调（native cancellation-record callback）传播一次真实异常（panic）。
#[cfg(feature = "observe")]
struct PanicOnCancel;
#[cfg(feature = "observe")]
impl pi_async_task::task::TaskObserver for PanicOnCancel {
    fn event(&self, _: pi_async_task::definitions::TaskId, event: pi_async_task::task::TaskEvent) {
        if event == pi_async_task::task::TaskEvent::CancelRecorded { std::panic::panic_any(987_u32); }
    }
}

/// 验证原生取消（native cancellation）在记录事实后因原观测回调（observer callback）异常时，CancelNotifyGuard 接续一次通知（notification）。
/// 原恐慌载荷（panic payload）保持，守卫（guard）不伪造清理完成（cleanup completion）；仅特性（feature）明确启用时执行。
#[cfg(feature = "observe")]
#[test]
fn test_control_cancel_guard_notifies_when_native_observer_panics() {
    use pi_async_runtime_core::TaskConfig;
    let notifications = Arc::new(AtomicUsize::new(0)); let snapshots = Arc::new(AtomicUsize::new(0));
    let polls = Arc::new(AtomicUsize::new(0)); let drops = Arc::new(AtomicUsize::new(0));
    let factory = TaskFactory::try_new(ScheduleProbe { clones: Arc::new(AtomicUsize::new(0)), drops: Arc::new(AtomicUsize::new(0)) },
        CancelNotificationProbe { notifications: notifications.clone(), snapshots: snapshots.clone(), panic: false }, QueueSupport::Plain).unwrap();
    let mut config = TaskConfig::new(()); config.observer = Some(Arc::new(PanicOnCancel));
    let (task, handle) = factory.create(CreateProbe { polls: polls.clone(), drops: drops.clone(), _pinned: std::marker::PhantomPinned }, config).unwrap();
    let control = handle.control_handle();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| control.cancel()));
    assert_eq!(result.unwrap_err().downcast_ref::<u32>(), Some(&987));
    assert_eq!(notifications.load(Ordering::Relaxed), 1); assert_eq!(snapshots.load(Ordering::Relaxed), 1);
    assert_eq!(drops.load(Ordering::Relaxed), 0); assert_eq!(polls.load(Ordering::Relaxed), 0);
    drop(task); drop(handle); drop(control);
    assert_eq!(drops.load(Ordering::Relaxed), 1); assert_eq!(notifications.load(Ordering::Relaxed), 1);
}

/// 合法外部等待者首次挂起时通知取消一次；再次挂起只登记唤醒器（Waker）。
#[test]
fn test_cancel_wait_polls_native_once_and_notifies_only_on_first_pending() {
    use pi_async_runtime_core::TaskConfig;
    use std::future::Future;
    use std::task::Poll;
    use pi_async_task::definitions::{CancelRequestStatus, CancellationError, CancellationOutcome};
    use pi_async_task::ExternalWaiter;

    let notifications = Arc::new(AtomicUsize::new(0));
    let snapshots = Arc::new(AtomicUsize::new(0));
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let factory = TaskFactory::try_new(
        ScheduleProbe { clones: Arc::new(AtomicUsize::new(0)), drops: Arc::new(AtomicUsize::new(0)) },
        CancelNotificationProbe { notifications: notifications.clone(), snapshots: snapshots.clone(), panic: false },
        QueueSupport::Plain,
    ).unwrap();
    let (task, handle) = factory.create(
        CreateProbe { polls: polls.clone(), drops: drops.clone(), _pinned: std::marker::PhantomPinned },
        TaskConfig::new(()),
    ).unwrap();
    let control = handle.control_handle();
    assert_eq!(control.snapshot().cancellation, CancelRequestStatus::NotRecorded);
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());

    {
        let mut unknown = std::pin::pin!(control.cancel_and_wait());
        for _ in 0..2 {
            assert_eq!(unknown.as_mut().poll(&mut cx), Poll::Ready(CancellationOutcome::Failed(CancellationError::UnknownWaiter)));
        }
        assert_eq!(control.snapshot().cancellation, CancelRequestStatus::NotRecorded);
        assert_eq!(notifications.load(Ordering::Relaxed), 0);
        assert_eq!(snapshots.load(Ordering::Relaxed), 0);
    }

    let source = Arc::new(ExternalIdentity { reads: AtomicUsize::new(0) });
    let external = ExternalWaiter::new(source.clone());
    let mut wait = std::pin::pin!(control.cancel_and_wait_from(&external));
    for reads in [1, 2] {
        assert_eq!(wait.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(source.reads.load(Ordering::Relaxed), reads);
        assert_eq!(control.snapshot().cancellation, CancelRequestStatus::Recorded);
        assert_eq!(snapshots.load(Ordering::Relaxed), 1);
        assert_eq!(notifications.load(Ordering::Relaxed), 1);
    }

    drop(task);
    assert_eq!(wait.as_mut().poll(&mut cx), Poll::Ready(CancellationOutcome::Failed(CancellationError::TargetAbandoned)));
    assert_eq!(notifications.load(Ordering::Relaxed), 1);
    drop(control);
    drop(handle);
    assert_eq!(polls.load(Ordering::Relaxed), 0, "本用例不推进业务异步计算（Future）");
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

/// 提交成功：同一原Runnable交给来源一次，随后通知一次；身份不变。
#[test]
fn test_submit_delivers_same_task_once_and_notifies_after_acceptance() {
    use pi_async_runtime_core::task::SubmissionMode;
    use pi_async_runtime_core::TaskConfig;
    let notifications = Arc::new(AtomicUsize::new(0));
    let factory = TaskFactory::try_new(
        ScheduleProbe { clones: Arc::new(AtomicUsize::new(0)), drops: Arc::new(AtomicUsize::new(0)) },
        NotificationProbe(notifications.clone()),
        QueueSupport::Plain,
    ).unwrap();
    let (task, handle) = factory.create(
        CreateProbe { polls: Arc::new(AtomicUsize::new(0)), drops: Arc::new(AtomicUsize::new(0)), _pinned: std::marker::PhantomPinned },
        TaskConfig::new(()),
    ).unwrap();
    let id = task.id();
    let mut pushed = Vec::new();
    factory.submit(task, SubmissionMode::Ordinary, |runnable| {
        assert_eq!(notifications.load(Ordering::Relaxed), 0);
        pushed.push(runnable.id());
        Ok(())
    }).expect("同源普通提交必须被接受");
    assert_eq!(pushed, [id], "只交付一次且身份不变");
    assert_eq!(notifications.load(Ordering::Relaxed), 1, "接收后才通知一次");
    drop(handle);
}

/// 来源拒绝：返还同一原任务与原因，不通知；可再次提交。
#[test]
fn test_submit_rejection_returns_same_task_and_cause_without_notification() {
    use pi_async_runtime_core::task::SubmissionMode;
    use pi_async_runtime_core::{SubmitErrorKind, TaskConfig};
    let notifications = Arc::new(AtomicUsize::new(0));
    let factory = TaskFactory::try_new(
        ScheduleProbe { clones: Arc::new(AtomicUsize::new(0)), drops: Arc::new(AtomicUsize::new(0)) },
        NotificationProbe(notifications.clone()),
        QueueSupport::Plain,
    ).unwrap();
    let (task, handle) = factory.create(
        CreateProbe { polls: Arc::new(AtomicUsize::new(0)), drops: Arc::new(AtomicUsize::new(0)), _pinned: std::marker::PhantomPinned },
        TaskConfig::new(()),
    ).unwrap();
    let id = task.id();
    let error = factory.submit(task, SubmissionMode::Ordinary, |runnable| {
        Err((runnable, SubmitErrorKind::External(pi_async_task_queue::external::ExternalPushErrorKind::Busy)))
    }).expect_err("来源拒绝必须返还错误");
    assert!(matches!(error.cause, SubmitErrorKind::External(pi_async_task_queue::external::ExternalPushErrorKind::Busy)));
    let (returned, _) = error.into_parts();
    assert_eq!(returned.id(), id, "必须返还同一原任务");
    assert_eq!(notifications.load(Ordering::Relaxed), 0, "拒绝不得通知");
    drop(returned);
    drop(handle);
}

/// 跨服务提交被拒：不调用来源、不通知，原任务原样返还。
#[test]
fn test_submit_rejects_foreign_runtime_without_touching_source() {
    use pi_async_runtime_core::task::SubmissionMode;
    use pi_async_runtime_core::{SubmitErrorKind, TaskConfig};
    let first = TaskFactory::try_new(
        ScheduleProbe { clones: Arc::new(AtomicUsize::new(0)), drops: Arc::new(AtomicUsize::new(0)) },
        NotificationProbe(Arc::new(AtomicUsize::new(0))),
        QueueSupport::Plain,
    ).unwrap();
    let second = TaskFactory::try_new(
        ScheduleProbe { clones: Arc::new(AtomicUsize::new(0)), drops: Arc::new(AtomicUsize::new(0)) },
        NotificationProbe(Arc::new(AtomicUsize::new(0))),
        QueueSupport::Plain,
    ).unwrap();
    let (task, handle) = first.create(
        CreateProbe { polls: Arc::new(AtomicUsize::new(0)), drops: Arc::new(AtomicUsize::new(0)), _pinned: std::marker::PhantomPinned },
        TaskConfig::new(()),
    ).unwrap();
    let touched = Arc::new(AtomicUsize::new(0));
    let probe = touched.clone();
    let error = second.submit(task, SubmissionMode::Ordinary, move |runnable| {
        probe.fetch_add(1, Ordering::Relaxed);
        drop(runnable);
        Ok(())
    });
    let error = match error {
        Err(error) => error,
        Ok(()) => panic!("跨服务提交必须被拒绝"),
    };
    assert!(matches!(error.cause, SubmitErrorKind::WrongRuntime));
    assert_eq!(touched.load(Ordering::Relaxed), 0, "拒绝前不得调用来源");
    drop(error.into_parts());
    drop(handle);
}

