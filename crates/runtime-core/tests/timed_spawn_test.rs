//! 定时任务派发（T101/T102）与带只读查询登记的定时派发（T88/T89）公开行为测试。
//! 被测入口：
//! - pi_async_runtime_core::time::timeout::{spawn_task, spawn_local_task}
//! - pi_async_runtime_core::time::task_support::{spawn_task_with_query, spawn_local_task_with_query}
//! 仅经公开接口（API）与受控 TimerAccess 观测真实顺序、错误归属、取消接续、拒绝恢复及异常边界。

use pi_async_runtime_core::definitions::{
    CreateErrorKind, RuntimeId, SubmitErrorKind, TimerError,
};
use pi_async_runtime_core::task::{
    SpawnError, SubmissionMode, TaskConfig, TaskFactory, TaskNotifier, TaskQuery,
};
use pi_async_runtime_core::time::access::{
    Confirmation, TimerAccess, TimerCancelRequestOutcome,
};
use pi_async_runtime_core::time::entry::TimerEntry;
use pi_async_runtime_core::time::registration::{TimerRegistration, TimerRegistrationKind};
use pi_async_runtime_core::time::task_support::{
    spawn_local_task_with_query, spawn_task_with_query,
};
use pi_async_runtime_core::time::timeout::{spawn_local_task, spawn_task};
use pi_async_task::definitions::{QueueSupport, TaskId};
use pi_async_task::{Runnable, Schedule, TaskThread};
use pi_async_task_queue::notify::QueueNotifier;
use pi_async_task_queue::timer::{TimerQueueError, TimerSubmitError};
use std::any::Any;
use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::thread::{self, ThreadId};
use std::time::Duration;

// 这些用例在各自的测试线程中同步观察派发。传给生产接口的句柄只携带编号，
// 实际记录留在原线程。先结束记录借用，再执行后续调度、析构或断言之外的操作。
std::thread_local! {
    static RUNTIME_CORE_TIMED_SPAWN_TEST_SLOTS: RefCell<Vec<Option<Box<dyn Any>>>> =
        const { RefCell::new(Vec::new()) };
}

struct FixtureSlot {
    thread: ThreadId,
    index: usize,
}

impl Drop for FixtureSlot {
    fn drop(&mut self) {
        assert_eq!(self.thread, thread::current().id());
        let removed = RUNTIME_CORE_TIMED_SPAWN_TEST_SLOTS
            .try_with(|slots| slots.borrow_mut()[self.index].take());
        drop(removed);
    }
}

struct FixtureValue<T> {
    slot: Arc<FixtureSlot>,
    value_type: std::marker::PhantomData<fn() -> T>,
}

impl<T: 'static> FixtureValue<T> {
    fn new(value: T) -> Self {
        let index = RUNTIME_CORE_TIMED_SPAWN_TEST_SLOTS.with_borrow_mut(|slots| {
            let index = slots.len();
            slots.push(Some(Box::new(value)));
            index
        });
        Self {
            slot: Arc::new(FixtureSlot { thread: thread::current().id(), index }),
            value_type: std::marker::PhantomData,
        }
    }

    fn with<R>(&self, operation: impl FnOnce(&mut T) -> R) -> R {
        assert_eq!(self.slot.thread, thread::current().id());
        RUNTIME_CORE_TIMED_SPAWN_TEST_SLOTS.with_borrow_mut(|slots| {
            let value = slots[self.slot.index].as_mut().and_then(|value| value.downcast_mut::<T>())
                .expect("访问本用例的原记录");
            operation(value)
        })
    }
}

impl<T> Clone for FixtureValue<T> {
    fn clone(&self) -> Self {
        Self { slot: self.slot.clone(), value_type: std::marker::PhantomData }
    }
}

impl<T: Default + 'static> Default for FixtureValue<T> {
    fn default() -> Self {
        Self::new(T::default())
    }
}

#[derive(Clone, Default)]
struct TestSchedule {
    scheduled: FixtureValue<Vec<Runnable>>,
}

impl Schedule for TestSchedule {
    fn schedule(&self, runnable: Runnable) {
        self.scheduled.with(|values| values.push(runnable));
    }
}

#[derive(Clone)]
struct TestNotifier {
    events: FixtureValue<Vec<&'static str>>,
    notifications: Arc<AtomicUsize>,
    panic_on_notify: Arc<AtomicBool>,
}

impl TestNotifier {
    fn new(events: FixtureValue<Vec<&'static str>>) -> Self {
        Self {
            events,
            notifications: Arc::new(AtomicUsize::new(0)),
            panic_on_notify: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl QueueNotifier for TestNotifier {
    fn notify(&self) {
        self.events.with(|values| values.push("notify"));
        self.notifications.fetch_add(1, Ordering::Relaxed);
        if self.panic_on_notify.load(Ordering::Acquire) {
            panic!("TEST_NOTIFY_PANIC");
        }
    }
}

impl TaskNotifier for TestNotifier {}

struct TestKey(usize);

#[derive(Clone, Copy)]
enum RegisterAction {
    Accept,
    RejectBusy,
    RejectCapacity,
}

#[derive(Clone)]
struct TestTimerAccess {
    id: RuntimeId,
    events: FixtureValue<Vec<&'static str>>,
    deadlines: FixtureValue<Vec<Duration>>,
    prepares: FixtureValue<Vec<TimerRegistrationKind>>,
    shared_entries: FixtureValue<Vec<TimerEntry<TestKey, Arc<TimerRegistration<TestKey>>>>>,
    local_entries: FixtureValue<Vec<TimerEntry<TestKey, Arc<TimerRegistration<TestKey>>>>>,
    cancel_requests: FixtureValue<Vec<usize>>,
    deferred_cancels: Arc<AtomicUsize>,
    fail_deadline: Arc<AtomicBool>,
    fail_prepare: Arc<AtomicBool>,
    shared_action: FixtureValue<RegisterAction>,
    local_action: FixtureValue<RegisterAction>,
    cancel_busy: Arc<AtomicBool>,
    next_key: Arc<AtomicUsize>,
}

impl TestTimerAccess {
    fn new(id: RuntimeId, events: FixtureValue<Vec<&'static str>>) -> Self {
        Self {
            id,
            events,
            deadlines: FixtureValue::default(),
            prepares: FixtureValue::default(),
            shared_entries: FixtureValue::default(),
            local_entries: FixtureValue::default(),
            cancel_requests: FixtureValue::default(),
            deferred_cancels: Arc::new(AtomicUsize::new(0)),
            fail_deadline: Arc::new(AtomicBool::new(false)),
            fail_prepare: Arc::new(AtomicBool::new(false)),
            shared_action: FixtureValue::new(RegisterAction::Accept),
            local_action: FixtureValue::new(RegisterAction::Accept),
            cancel_busy: Arc::new(AtomicBool::new(false)),
            next_key: Arc::new(AtomicUsize::new(1)),
        }
    }
}

impl TimerAccess for TestTimerAccess {
    type Instant = u64;
    type Key = TestKey;
    type Registration = Arc<TimerRegistration<TestKey>>;

    fn runtime_id(&self) -> RuntimeId {
        self.id
    }

    fn deadline_after(&self, duration: Duration) -> pi_result::RawResult<Self::Instant, TimerError> {
        self.events.with(|values| values.push("deadline"));
        self.deadlines.with(|values| values.push(duration));
        if self.fail_deadline.load(Ordering::Acquire) {
            return Err(TimerError::ServiceClosed);
        }
        Ok(100 + duration.as_millis() as u64)
    }

    fn prepare_registration(
        &self,
        kind: TimerRegistrationKind,
    ) -> pi_result::RawResult<Self::Registration, TimerError> {
        self.events.with(|values| values.push("prepare"));
        self.prepares.with(|values| values.push(kind));
        if self.fail_prepare.load(Ordering::Acquire) {
            return Err(TimerError::RegistrationPreparation(
                pi_result::Error::new(pi_result::ErrorKind::Unavailable)
                    .attach("模拟准备失败"),
            ));
        }
        Ok(Arc::new(TimerRegistration::new(kind)))
    }

    fn try_register(
        &self,
        _deadline: Self::Instant,
        item: TimerEntry<Self::Key, Self::Registration>,
    ) -> pi_result::RawResult<Self::Key, TimerSubmitError<TimerEntry<Self::Key, Self::Registration>>> {
        self.events.with(|values| values.push("try_register"));
        match self.shared_action.with(|action| *action) {
            RegisterAction::Accept => {
                let key = self.next_key.fetch_add(1, Ordering::Relaxed);
                self.shared_entries.with(|values| values.push(item));
                Ok(TestKey(key))
            }
            RegisterAction::RejectBusy => Err(TimerSubmitError {
                item,
                cause: TimerQueueError::Busy,
            }),
            RegisterAction::RejectCapacity => Err(TimerSubmitError {
                item,
                cause: TimerQueueError::Capacity,
            }),
        }
    }

    fn try_register_local(
        &self,
        _deadline: Self::Instant,
        item: TimerEntry<Self::Key, Self::Registration>,
    ) -> pi_result::RawResult<Self::Key, TimerSubmitError<TimerEntry<Self::Key, Self::Registration>>> {
        self.events.with(|values| values.push("try_register_local"));
        match self.local_action.with(|action| *action) {
            RegisterAction::Accept => {
                let key = self.next_key.fetch_add(1, Ordering::Relaxed);
                self.local_entries.with(|values| values.push(item));
                Ok(TestKey(key))
            }
            RegisterAction::RejectBusy => Err(TimerSubmitError {
                item,
                cause: TimerQueueError::Busy,
            }),
            RegisterAction::RejectCapacity => Err(TimerSubmitError {
                item,
                cause: TimerQueueError::Capacity,
            }),
        }
    }

    fn request_cancel(
        &self,
        key: &Self::Key,
    ) -> pi_result::RawResult<TimerCancelRequestOutcome, TimerQueueError> {
        self.events.with(|values| values.push("request_cancel"));
        self.cancel_requests.with(|values| values.push(key.0));
        if self.cancel_busy.load(Ordering::Acquire) {
            Err(TimerQueueError::Busy)
        } else {
            Ok(TimerCancelRequestOutcome::Recorded)
        }
    }

    fn defer_cancel(
        &self,
        registration: Self::Registration,
        _error: Option<TimerQueueError>,
        confirmation: Confirmation<'_, Self::Key>,
    ) {
        self.events.with(|values| values.push("defer_cancel"));
        self.deferred_cancels.fetch_add(1, Ordering::Relaxed);
        let _ = confirmation.deferred(registration.as_ref());
    }
}

struct TrackedFuture<T> {
    value: Option<T>,
    drops: Arc<AtomicUsize>,
    tag: usize,
}

impl<T: Unpin> Future for TrackedFuture<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<T> {
        Poll::Ready(self.get_mut().value.take().expect("只求值一次"))
    }
}

impl<T> Drop for TrackedFuture<T> {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

fn build_fixture() -> (
    TaskFactory<TestSchedule, TestNotifier>,
    TestTimerAccess,
    TestNotifier,
    FixtureValue<Vec<&'static str>>,
) {
    let events = FixtureValue::default();
    let notifier = TestNotifier::new(events.clone());
    let factory = TaskFactory::try_new(
        TestSchedule::default(),
        notifier.clone(),
        QueueSupport::Plain,
    )
    .expect("构造任务工厂");
    let access = TestTimerAccess::new(factory.runtime_id(), events.clone());
    (factory, access, notifier, events)
}

fn poll_handle_ready<F: Future + Unpin>(mut fut: F) -> F::Output {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    match Pin::new(&mut fut).poll(&mut cx) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("期望已就绪"),
    }
}

/// T101/T88：RuntimeId 不匹配及非法正时长（0 < d < 1ms）必须在准备前拒绝并原样返还 F 与 config。
#[test]
fn test_spawn_task_rejects_wrong_runtime_id_and_invalid_duration_preserving_inputs() {
    let (factory, _same_access, _notifier, events) = build_fixture();
    let other_id = RuntimeId::try_new().unwrap();
    let wrong_access = TestTimerAccess::new(other_id, events.clone());
    let drops = Arc::new(AtomicUsize::new(0));
    let fut = TrackedFuture { value: Some(42usize), drops: drops.clone(), tag: 77 };

    let err = match spawn_task(&factory, wrong_access.clone(), fut, Duration::from_millis(5), TaskConfig::new(99u32)) {
        Ok(_) => panic!("不同 RuntimeId 必须拒绝"),
        Err(e) => e,
    };
    match err {
        SpawnError::Create(ce) => {
            assert_eq!(ce.future.tag, 77);
            assert_eq!(ce.config.metadata, 99u32);
            assert!(matches!(ce.cause, CreateErrorKind::Timer(TimerError::Queue(TimerQueueError::WrongQueue))));
            drop(ce);
        }
        SpawnError::Submit { .. } => panic!("不应进入提交阶段"),
    }
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert!(events.with(|values| values.is_empty()), "RuntimeId 不匹配不得触达时钟或登记");

    // 非法正时长 999us：对 spawn_task_with_query 同样验证不调用 register
    let (factory2, access2, _, events2) = build_fixture();
    let reg_called = Arc::new(AtomicUsize::new(0));
    let reg_flag = reg_called.clone();
    let fut2 = TrackedFuture { value: Some(1usize), drops: drops.clone(), tag: 88 };
    let err2 = match spawn_task_with_query(
        &factory2,
        access2,
        fut2,
        Duration::from_nanos(999_999),
        TaskConfig::new(12u32),
        move |_| { reg_flag.fetch_add(1, Ordering::Relaxed); },
    ) {
        Ok(_) => panic!("不足 1ms 的正时长必须拒绝"),
        Err(e) => e,
    };
    match err2 {
        SpawnError::Create(ce) => {
            assert_eq!(ce.future.tag, 88);
            assert_eq!(ce.config.metadata, 12u32);
            assert!(matches!(
                ce.cause,
                CreateErrorKind::Timer(TimerError::InvalidDuration {
                    value,
                    min_positive,
                    max: None,
                }) if value == Duration::from_nanos(999_999) && min_positive == Duration::from_millis(1)
            ));
        }
        SpawnError::Submit { .. } => panic!("非法时长不应进入提交阶段"),
    }
    assert_eq!(reg_called.load(Ordering::Relaxed), 0, "准备失败不得调用 register");
    assert!(events2.with(|values| values.is_empty()));
}

/// T101：零时长仍真实定时登记；大于 1s 的正时长同样允许；到期交付后同一 Runnable 正常执行并产出结果。
#[test]
fn test_spawn_task_zero_delay_and_large_delay_register_and_deliver_same_task() {
    let (factory, access, _notifier, events) = build_fixture();
    let owner = TaskThread::new();
    let drops = Arc::new(AtomicUsize::new(0));

    let handle_zero = spawn_task(
        &factory,
        access.clone(),
        TrackedFuture { value: Some(100usize), drops: drops.clone(), tag: 1 },
        Duration::ZERO,
        TaskConfig::new("zero"),
    )
    .expect("0 时长必须真实定时登记成功");
    assert_eq!(*handle_zero.metadata(), "zero");

    let handle_large = spawn_task(
        &factory,
        access.clone(),
        TrackedFuture { value: Some(200usize), drops: drops.clone(), tag: 2 },
        Duration::from_secs(5),
        TaskConfig::new("five-sec"),
    )
    .expect("超过 1s 的正时长不得设上限");

    assert_eq!(access.deadlines.with(|values| values.clone()).as_slice(), &[Duration::ZERO, Duration::from_secs(5)]);
    assert_eq!(access.prepares.with(|values| values.clone()).as_slice(), &[TimerRegistrationKind::Task, TimerRegistrationKind::Task]);
    assert_eq!(access.shared_entries.with(|values| values.len()), 2);
    assert_eq!(
        events.with(|values| values.clone()).as_slice(),
        &["deadline", "prepare", "try_register", "notify", "deadline", "prepare", "try_register", "notify"]
    );

    let entry1 = access.shared_entries.with(|values| values.remove(0));
    let r1 = entry1.into_due().expect("到期交付必须交出原 Runnable");
    assert_eq!(r1.id(), handle_zero.id());
    let _reg1 = r1.admit(&owner).expect("首次接纳");
    let _ = r1.run(&owner);
    assert_eq!(poll_handle_ready(handle_zero).expect("结果成功"), 100);

    let entry2 = access.shared_entries.with(|values| values.remove(0));
    let r2 = entry2.into_due().expect("到期交付必须交出原 Runnable");
    assert_eq!(r2.id(), handle_large.id());
    let _reg2 = r2.admit(&owner).expect("首次接纳");
    let _ = r2.run(&owner);
    assert_eq!(poll_handle_ready(handle_large).expect("结果成功"), 200);
    assert_eq!(drops.load(Ordering::Relaxed), 2);
}

/// T101：deadline_after 与 prepare_registration 失败均归还原 F 与 config，不创建任务。
#[test]
fn test_spawn_task_prepare_or_deadline_failure_preserves_inputs() {
    let (factory, access, _, _) = build_fixture();
    access.fail_deadline.store(true, Ordering::Release);
    let drops = Arc::new(AtomicUsize::new(0));
    let err = match spawn_task(
        &factory,
        access.clone(),
        TrackedFuture { value: Some(7usize), drops: drops.clone(), tag: 31 },
        Duration::from_millis(2),
        TaskConfig::new(55u8),
    ) {
        Ok(_) => panic!("deadline_after 失败"),
        Err(e) => e,
    };
    match err {
        SpawnError::Create(ce) => {
            assert_eq!(ce.future.tag, 31);
            assert_eq!(ce.config.metadata, 55u8);
            assert!(matches!(ce.cause, CreateErrorKind::Timer(TimerError::ServiceClosed)));
        }
        SpawnError::Submit { .. } => panic!("不应创建任务"),
    }
    assert!(access.prepares.with(|values| values.is_empty()), "取时失败后不得准备登记");

    access.fail_deadline.store(false, Ordering::Release);
    access.fail_prepare.store(true, Ordering::Release);
    let err2 = match spawn_task(
        &factory,
        access.clone(),
        TrackedFuture { value: Some(8usize), drops: drops.clone(), tag: 32 },
        Duration::from_millis(2),
        TaskConfig::new(66u8),
    ) {
        Ok(_) => panic!("prepare_registration 失败"),
        Err(e) => e,
    };
    match err2 {
        SpawnError::Create(ce) => {
            assert_eq!(ce.future.tag, 32);
            assert_eq!(ce.config.metadata, 66u8);
            assert!(matches!(ce.cause, CreateErrorKind::Timer(TimerError::RegistrationPreparation(_))));
        }
        SpawnError::Submit { .. } => panic!("不应创建任务"),
    }
    assert_eq!(access.shared_entries.with(|values| values.len()), 0);
}

/// T101：来源明确拒绝必须 recover 同一 Runnable，以 DirectFailure 结束登记，返还 UnsubmittedTask 与 TaskHandle 且仍可普通提交。
#[test]
fn test_spawn_task_queue_rejection_recovers_unsubmitted_task_and_handle() {
    let (factory, access, notifier, _) = build_fixture();
    access.shared_action.with(|action| *action = RegisterAction::RejectBusy);
    let owner = TaskThread::new();
    let drops = Arc::new(AtomicUsize::new(0));

    let err = match spawn_task(
        &factory,
        access.clone(),
        TrackedFuture { value: Some(321usize), drops: drops.clone(), tag: 9 },
        Duration::from_millis(10),
        TaskConfig::new("reject-me"),
    ) {
        Ok(_) => panic!("来源拒绝必须返回 SpawnError::Submit"),
        Err(e) => e,
    };

    let (unsubmitted, handle) = match err {
        SpawnError::Submit { error, handle } => {
            assert!(matches!(error.cause, SubmitErrorKind::Timer(TimerQueueError::Busy)));
            assert_eq!(error.task.id(), handle.id());
            (error.task, handle)
        }
        SpawnError::Create(_) => panic!("应进入提交阶段并返还已创建任务"),
    };
    assert_eq!(notifier.notifications.load(Ordering::Relaxed), 0, "明确拒绝不得通知");
    assert_eq!(access.cancel_requests.with(|values| values.len()), 0, "明确拒绝不得发起取消");
    assert_eq!(drops.load(Ordering::Relaxed), 0, "明确拒绝不得提前释放原 Future");

    // 同一 UnsubmittedTask 仍可按普通提交正常执行
    let mut recovered: Option<Runnable> = None;
    factory
        .submit(unsubmitted, SubmissionMode::Ordinary, |r| {
            recovered = Some(r);
            Ok(())
        })
        .expect("普通提交必须接受恢复的原任务");
    let runnable = recovered.unwrap();
    let _reg = runnable.admit(&owner).expect("接纳");
    let _ = runnable.run(&owner);
    assert_eq!(poll_handle_ready(handle).expect("结果"), 321);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

/// T101：接受后任务取消经 TimedTaskNotifier 转发同源键；接受后 notify 恐慌由守卫触发 Detached 取消。
#[test]
fn test_spawn_task_cancel_and_post_accept_notify_panic_guard() {
    let (factory, access, notifier, _) = build_fixture();
    let drops = Arc::new(AtomicUsize::new(0));

    let handle = spawn_task(
        &factory,
        access.clone(),
        TrackedFuture { value: Some(1usize), drops: drops.clone(), tag: 1 },
        Duration::from_millis(10),
        TaskConfig::<()>::default(),
    )
    .expect("提交成功");
    handle.control_handle().cancel();
    assert_eq!(access.cancel_requests.with(|values| values.clone()).as_slice(), &[1], "取消必须经 TimedTaskNotifier 转发键 1");

    // 接受后 notify 恐慌：守卫必须触发 Detached 取消转发键 2
    notifier.panic_on_notify.store(true, Ordering::Release);
    let panic_res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = spawn_task(
            &factory,
            access.clone(),
            TrackedFuture { value: Some(2usize), drops: drops.clone(), tag: 2 },
            Duration::from_millis(10),
            TaskConfig::<()>::default(),
        );
    }));
    assert!(panic_res.is_err(), "notify 恐慌不得被吞掉");
    assert_eq!(
        access.cancel_requests.with(|values| values.clone()).as_slice(),
        &[1, 2],
        "接受后 notify 恐慌时守卫必须撤销键 2 的定时登记"
    );
}

/// T102：本地定时派发支持非 Send Future 与非 Send 输出，走 try_register_local，拒绝时返还 LocalOutput handle。
#[test]
fn test_spawn_local_task_supports_non_send_and_uses_try_register_local() {
    let (factory, access, _, _) = build_fixture();
    let owner = TaskThread::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let local_marker = Rc::new(777usize);

    let handle = spawn_local_task(
        &factory,
        &owner,
        access.clone(),
        TrackedFuture { value: Some(Rc::clone(&local_marker)), drops: drops.clone(), tag: 5 },
        Duration::ZERO,
        TaskConfig::new("local-ok"),
    )
    .expect("本地定时派发成功");
    assert_eq!(access.shared_entries.with(|values| values.len()), 0, "不得走共享 try_register");
    assert_eq!(access.local_entries.with(|values| values.len()), 1, "必须走本地 try_register_local");

    let entry = access.local_entries.with(|values| values.remove(0));
    let runnable = entry.into_due().unwrap();
    let _reg = runnable.admit(&owner).unwrap();
    let _ = runnable.run(&owner);
    let out = poll_handle_ready(handle).unwrap();
    assert_eq!(*out, 777);

    // 本地拒绝：保原 UnsubmittedTask 与 LocalOutput handle
    access.local_action.with(|action| *action = RegisterAction::RejectCapacity);
    let err = match spawn_local_task(
        &factory,
        &owner,
        access.clone(),
        TrackedFuture { value: Some(Rc::clone(&local_marker)), drops: drops.clone(), tag: 6 },
        Duration::from_millis(3),
        TaskConfig::new("local-reject"),
    ) {
        Ok(_) => panic!("本地来源拒绝"),
        Err(e) => e,
    };
    match err {
        SpawnError::Submit { error, handle } => {
            assert!(matches!(error.cause, SubmitErrorKind::Timer(TimerQueueError::Capacity)));
            assert_eq!(error.task.id(), handle.id());
            drop(error);
            drop(handle);
        }
        SpawnError::Create(_) => panic!("应返回 SpawnError::Submit"),
    }
    owner.close();
    assert_eq!(owner.live_tasks(), 0);
    assert_eq!(drops.load(Ordering::Relaxed), 2);
}

/// T88：spawn_task_with_query 严格在创建后、来源可见前调用 register 一次；register 恐慌不入来源且按 Abandoned 清理。
#[test]
fn test_spawn_task_with_query_order_and_register_panic_cleanup() {
    let (factory, access, notifier, events) = build_fixture();
    let drops = Arc::new(AtomicUsize::new(0));
    let recorded_query: RefCell<Option<TaskQuery>> = RefCell::new(None);
    let rq = &recorded_query;
    let ev = events.clone();

    let handle = spawn_task_with_query(
        &factory,
        access.clone(),
        TrackedFuture { value: Some(55usize), drops: drops.clone(), tag: 10 },
        Duration::ZERO,
        TaskConfig::new("query-ok"),
        move |query| {
            ev.with(|values| values.push("register"));
            *rq.borrow_mut() = Some(query);
        },
    )
    .expect("带查询登记的定时派发成功");

    assert_eq!(
        events.with(|values| values.clone()).as_slice(),
        &["deadline", "prepare", "register", "try_register", "notify"],
        "register 必须在 try_register 之前恰调用一次"
    );
    let q = recorded_query.borrow().clone().expect("必须收到 TaskQuery");
    assert_eq!(q.id(), handle.id());

    // register 恐慌：不得调用 try_register 或 notify，原 Future 经 Abandoned 路径释放一次，panic 原样传播
    events.with(|values| values.clear());
    let panic_drops = Arc::new(AtomicUsize::new(0));
    let ev2 = events.clone();
    let before_entries = access.shared_entries.with(|values| values.len());
    let before_notify = notifier.notifications.load(Ordering::Relaxed);
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = spawn_task_with_query(
            &factory,
            access.clone(),
            TrackedFuture { value: Some(66usize), drops: panic_drops.clone(), tag: 11 },
            Duration::from_millis(5),
            TaskConfig::<()>::default(),
            move |_query| {
                ev2.with(|values| values.push("register_panic"));
                panic!("REGISTER_CALLBACK_PANIC");
            },
        );
    }));
    let payload = caught.expect_err("register 恐慌不得被捕获吞掉");
    assert_eq!(payload.downcast_ref::<&str>(), Some(&"REGISTER_CALLBACK_PANIC"));
    assert_eq!(events.with(|values| values.clone()).as_slice(), &["deadline", "prepare", "register_panic"]);
    assert_eq!(access.shared_entries.with(|values| values.len()), before_entries, "register 恐慌后不得提交来源");
    assert_eq!(notifier.notifications.load(Ordering::Relaxed), before_notify, "register 恐慌后不得通知");
    assert_eq!(panic_drops.load(Ordering::Relaxed), 1, "未提交原任务必须按原 Abandoned 路径释放原 Future");
}

/// T89：spawn_local_task_with_query 对非 Send 任务在本地来源可见前登记查询；register 恐慌在所属线程合法清理。
#[test]
fn test_spawn_local_task_with_query_order_and_register_panic_cleanup() {
    let (factory, access, _, events) = build_fixture();
    let owner = TaskThread::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let rc = Rc::new(909usize);
    let seen_id: RefCell<Option<TaskId>> = RefCell::new(None);
    let sid = &seen_id;
    let ev = events.clone();

    let handle = spawn_local_task_with_query(
        &factory,
        &owner,
        access.clone(),
        TrackedFuture { value: Some(Rc::clone(&rc)), drops: drops.clone(), tag: 20 },
        Duration::from_millis(4),
        TaskConfig::new("local-query"),
        move |query| {
            ev.with(|values| values.push("register"));
            *sid.borrow_mut() = Some(query.id());
        },
    )
    .expect("本地带查询定时派发成功");

    assert_eq!(
        events.with(|values| values.clone()).as_slice(),
        &["deadline", "prepare", "register", "try_register_local", "notify"]
    );
    assert_eq!(*seen_id.borrow(), Some(handle.id()));

    // register 恐慌：仍在当前所属线程释放非 Send Future，不进入本地来源
    events.with(|values| values.clear());
    let panic_drops = Arc::new(AtomicUsize::new(0));
    let ev2 = events.clone();
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = spawn_local_task_with_query(
            &factory,
            &owner,
            access.clone(),
            TrackedFuture { value: Some(Rc::clone(&rc)), drops: panic_drops.clone(), tag: 21 },
            Duration::ZERO,
            TaskConfig::<()>::default(),
            move |_| {
                ev2.with(|values| values.push("register_panic"));
                panic!("LOCAL_REGISTER_PANIC");
            },
        );
    }));
    assert!(caught.is_err());
    assert_eq!(events.with(|values| values.clone()).as_slice(), &["deadline", "prepare", "register_panic"]);
    assert_eq!(access.local_entries.with(|values| values.len()), 1, "恐慌项不得进入本地来源");
    owner.close();
    assert_eq!(owner.live_tasks(), 0);
    assert_eq!(panic_drops.load(Ordering::Relaxed), 1, "本地非 Send Future 须由原 TaskThread 在所属线程清理一次");
}
