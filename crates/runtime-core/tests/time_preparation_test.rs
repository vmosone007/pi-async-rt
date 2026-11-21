//! 计时组合（time composition）的惰性准备（lazy preparation），只经公开接口（public API）观察资源持有与副作用。
use pi_async_runtime_core::{RuntimeId, TimerError};
use pi_async_runtime_core::time::{TimerAccess, TimerEntry, TimerRegistration, TimerRegistrationKind, TimeoutOn};
use pi_async_task_queue::timer::{TimerQueueError, TimerSubmitError};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::time::Duration;

/// 未发布且不可克隆（non-Clone）的键（key），含地址固定标记（PhantomPinned）；本组不会制造原键（key）。
struct PreparationKey(std::marker::PhantomPinned);

/// 计时访问（TimerAccess）的边界观察器（boundary probe）；本组构造（construction）不允许调用任何计时操作（timer operation）。
/// 任何意外调用（unexpected call）均严格失败；不提供来源接收（source acceptance）、取消（cancellation）或完成（completion）能力。
struct PreparationAccess {
    id: RuntimeId,
    calls: Arc<AtomicUsize>,
    clones: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}
impl PreparationAccess {
    fn forbidden<T>(&self) -> T {
        self.calls.fetch_add(1, Ordering::Relaxed);
        panic!("本组准备不得调用计时来源（preparation must not call timer source）");
    }
}
impl Clone for PreparationAccess {
    fn clone(&self) -> Self {
        self.clones.fetch_add(1, Ordering::Relaxed);
        Self { id: self.id, calls: self.calls.clone(), clones: self.clones.clone(), drops: self.drops.clone() }
    }
}
impl Drop for PreparationAccess {
    fn drop(&mut self) { self.drops.fetch_add(1, Ordering::Relaxed); }
}
impl TimerAccess for PreparationAccess {
    type Instant = u64;
    type Key = PreparationKey;
    type Registration = Arc<TimerRegistration<PreparationKey>>;
    fn runtime_id(&self) -> RuntimeId { self.calls.fetch_add(1, Ordering::Relaxed); self.id }
    fn deadline_after(&self, _: Duration) -> Result<u64, TimerError> { self.forbidden() }
    fn prepare_registration(&self, _: TimerRegistrationKind) -> Result<Self::Registration, TimerError> { self.forbidden() }
    fn try_register(&self, _: u64, _: TimerEntry<Self::Key, Self::Registration>)
        -> Result<Self::Key, TimerSubmitError<TimerEntry<Self::Key, Self::Registration>>> { self.forbidden() }
    fn try_register_local(&self, _: u64, _: TimerEntry<Self::Key, Self::Registration>)
        -> Result<Self::Key, TimerSubmitError<TimerEntry<Self::Key, Self::Registration>>> { self.forbidden() }
    fn request_cancel(
        &self,
        _: &Self::Key,
    ) -> Result<pi_async_runtime_core::time::access::TimerCancelRequestOutcome, TimerQueueError> {
        self.forbidden()
    }
    fn defer_cancel(
        &self,
        _: Self::Registration,
        _: Option<TimerQueueError>,
        _: pi_async_runtime_core::time::access::Confirmation<'_, Self::Key>,
    ) { self.forbidden::<()>(); }
}

/// 借用本地原值（borrowed local Future），不满足Send／Sync／Unpin／'static；释放（drop）和轮询（poll）均可观察。
struct BorrowedPreparationFuture<'a> {
    polls: &'a std::cell::Cell<usize>,
    drops: &'a std::cell::Cell<usize>,
    _local: std::rc::Rc<()>,
    _pinned: std::marker::PhantomPinned,
}
impl std::future::Future for BorrowedPreparationFuture<'_> {
    type Output = ();
    fn poll(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        self.polls.set(self.polls.get() + 1);
        std::task::Poll::Ready(())
    }
}
impl Drop for BorrowedPreparationFuture<'_> {
    fn drop(&mut self) { self.drops.set(self.drops.get() + 1); }
}

/// 验证 TimeoutOn::new 只接管原异步计算（Future）和计时访问（TimerAccess），不取时、准备登记（registration）或轮询（poll）。
/// 零、纳秒、毫秒及超大时长（duration）在构造（construction）时均只保存；未轮询释放（unpolled drop）恰一次归还原值。
#[test]
fn test_timeout_on_construction_is_lazy_and_accepts_borrowed_pinned_future() {
    let calls = Arc::new(AtomicUsize::new(0));
    let clones = Arc::new(AtomicUsize::new(0));
    let access_drops = Arc::new(AtomicUsize::new(0));
    let polls = std::cell::Cell::new(0);
    let drops = std::cell::Cell::new(0);
    for duration in [Duration::ZERO, Duration::from_nanos(1), Duration::from_millis(1), Duration::MAX] {
        let access = PreparationAccess { id: RuntimeId::try_new().unwrap(), calls: calls.clone(), clones: clones.clone(), drops: access_drops.clone() };
        let before = drops.get();
        let combined = TimeoutOn::new(access, BorrowedPreparationFuture {
            polls: &polls, drops: &drops, _local: std::rc::Rc::new(()), _pinned: std::marker::PhantomPinned,
        }, duration);
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(clones.load(Ordering::Relaxed), 0);
        assert_eq!(polls.get(), 0);
        assert_eq!(drops.get(), before);
        drop(combined);
        assert_eq!(drops.get(), before + 1);
    }
    assert_eq!(access_drops.load(Ordering::Relaxed), 4);
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert_eq!(clones.load(Ordering::Relaxed), 0);
    assert_eq!(polls.get(), 0);
}

/// 原通知能力（notification capability）的有限观察值（probe）；普通通知（notification）不触达计时来源（timer source）。
struct PreparationNotifier { notifications: Arc<AtomicUsize>, clones: Arc<AtomicUsize>, drops: Arc<AtomicUsize> }
impl Clone for PreparationNotifier {
    fn clone(&self) -> Self {
        self.clones.fetch_add(1, Ordering::Relaxed);
        Self { notifications: self.notifications.clone(), clones: self.clones.clone(), drops: self.drops.clone() }
    }
}
impl Drop for PreparationNotifier {
    fn drop(&mut self) { self.drops.fetch_add(1, Ordering::Relaxed); }
}
impl pi_async_task_queue::notify::QueueNotifier for PreparationNotifier {
    fn notify(&self) { self.notifications.fetch_add(1, Ordering::Relaxed); }
}
impl pi_async_runtime_core::task::TaskNotifier for PreparationNotifier {}

/// 验证 TimedTaskNotifier::new 只接管原通知（notifier）、计时访问（access）及登记（registration）。
/// 原弱引用（Weak）观察同一登记的持有期（lifetime），没有新建登记、克隆（clone）或来源调用（source call）。
#[test]
fn test_timed_notifier_construction_preserves_original_registration_without_work() {
    use pi_async_runtime_core::time::TimedTaskNotifier;
    use pi_async_task_queue::notify::QueueNotifier;
    let calls = Arc::new(AtomicUsize::new(0)); let clones = Arc::new(AtomicUsize::new(0));
    let access_drops = Arc::new(AtomicUsize::new(0)); let notifier_drops = Arc::new(AtomicUsize::new(0));
    let notifications = Arc::new(AtomicUsize::new(0)); let notifier_clones = Arc::new(AtomicUsize::new(0));
    let registration = Arc::new(TimerRegistration::<PreparationKey>::new(TimerRegistrationKind::Task));
    let weak = Arc::downgrade(&registration);
    let combined = TimedTaskNotifier::new(PreparationNotifier { notifications: notifications.clone(), clones: notifier_clones.clone(), drops: notifier_drops.clone() },
        PreparationAccess { id: RuntimeId::try_new().unwrap(), calls: calls.clone(), clones: clones.clone(), drops: access_drops.clone() }, registration);
    assert!(weak.upgrade().is_some());
    assert_eq!(calls.load(Ordering::Relaxed), 0); assert_eq!(notifications.load(Ordering::Relaxed), 0);
    assert_eq!(clones.load(Ordering::Relaxed), 0); assert_eq!(notifier_clones.load(Ordering::Relaxed), 0);
    combined.notify(); assert_eq!(notifications.load(Ordering::Relaxed), 1); assert_eq!(calls.load(Ordering::Relaxed), 0);
    drop(combined);
    assert!(weak.upgrade().is_none());
    assert_eq!(access_drops.load(Ordering::Relaxed), 1); assert_eq!(notifier_drops.load(Ordering::Relaxed), 1);
}

/// 验证 TimedTaskNotifier::clone 保持同一登记（registration）和原通知目标（notification target），只克隆原能力（capabilities）。
/// 不要求原键（key）Clone；原对象释放（drop）后同登记由副本保活，副本通知（notify）仍只到原目标，不取时或新增登记。
#[test]
fn test_timed_notifier_clone_shares_original_registration_and_capabilities() {
    use pi_async_runtime_core::time::TimedTaskNotifier;
    use pi_async_task_queue::notify::QueueNotifier;
    let calls = Arc::new(AtomicUsize::new(0)); let clones = Arc::new(AtomicUsize::new(0));
    let access_drops = Arc::new(AtomicUsize::new(0)); let notifier_drops = Arc::new(AtomicUsize::new(0));
    let notifications = Arc::new(AtomicUsize::new(0)); let notifier_clones = Arc::new(AtomicUsize::new(0));
    let registration = Arc::new(TimerRegistration::<PreparationKey>::new(TimerRegistrationKind::Task));
    let weak = Arc::downgrade(&registration);
    let combined = TimedTaskNotifier::new(PreparationNotifier { notifications: notifications.clone(), clones: notifier_clones.clone(), drops: notifier_drops.clone() },
        PreparationAccess { id: RuntimeId::try_new().unwrap(), calls: calls.clone(), clones: clones.clone(), drops: access_drops.clone() }, registration);
    let copy = combined.clone();
    assert_eq!(clones.load(Ordering::Relaxed), 1); assert_eq!(notifier_clones.load(Ordering::Relaxed), 1);
    assert_eq!(calls.load(Ordering::Relaxed), 0); assert_eq!(notifications.load(Ordering::Relaxed), 0);
    assert_eq!(weak.strong_count(), 2);
    drop(combined);
    assert!(weak.upgrade().is_some());
    assert_eq!(access_drops.load(Ordering::Relaxed), 1); assert_eq!(notifier_drops.load(Ordering::Relaxed), 1);
    copy.notify(); assert_eq!(notifications.load(Ordering::Relaxed), 1); assert_eq!(calls.load(Ordering::Relaxed), 0);
    drop(copy);
    assert!(weak.upgrade().is_none());
    assert_eq!(access_drops.load(Ordering::Relaxed), 2); assert_eq!(notifier_drops.load(Ordering::Relaxed), 2);
}

/// 验证 Sleep::new 对零时长和非法时长（invalid duration）无计时来源副作用，
/// 首次轮询（poll）给出准确结果，再次轮询拒绝。观察器不会代替来源取时或登记。
#[test]
fn test_sleep_zero_and_invalid_duration_resolve_without_timer_source() {
    use pi_async_runtime_core::time::Sleep;
    use std::{future::Future, pin::Pin, task::{Context, Poll, Waker}};

    let calls = Arc::new(AtomicUsize::new(0));
    let clones = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let mut cx = Context::from_waker(Waker::noop());
    for duration in [Duration::ZERO, Duration::from_nanos(999_999), Duration::from_nanos(1_000_000_001)] {
        let access = PreparationAccess {
            id: RuntimeId::try_new().unwrap(),
            calls: Arc::clone(&calls),
            clones: Arc::clone(&clones),
            drops: Arc::clone(&drops),
        };
        let mut sleep = Sleep::new(access, duration);
        assert_eq!(calls.load(Ordering::Relaxed), 0);
        assert_eq!(clones.load(Ordering::Relaxed), 0);
        if duration.is_zero() {
            assert!(matches!(Pin::new(&mut sleep).poll(&mut cx), Poll::Ready(Ok(()))));
        } else {
            assert!(matches!(Pin::new(&mut sleep).poll(&mut cx),
                Poll::Ready(Err(TimerError::InvalidDuration { value, min_positive, max: Some(max) }))
                    if value == duration && min_positive == Duration::from_millis(1) && max == Duration::from_secs(1)));
        }
        let again = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = Pin::new(&mut sleep).poll(&mut cx);
        }));
        assert!(again.is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }
    assert_eq!(drops.load(Ordering::Relaxed), 3);
}

/// 验证 TimeoutOn::poll 的业务优先（business-first）分支原地释放借用且不可移动的异步计算（Future）。
/// 正时长若被错误地先用于计时，观察器会直接失败；返回后保留组合值仍能观察到业务值已经析构（drop）。
#[test]
fn test_timeout_on_ready_business_drops_in_place_without_preparing_timer() {
    use std::{future::Future, task::{Context, Poll, Waker}};
    let calls = Arc::new(AtomicUsize::new(0));
    let access = PreparationAccess {
        id: RuntimeId::try_new().unwrap(),
        calls: Arc::clone(&calls),
        clones: Arc::new(AtomicUsize::new(0)),
        drops: Arc::new(AtomicUsize::new(0)),
    };
    let polls = std::cell::Cell::new(0);
    let drops = std::cell::Cell::new(0);
    let future = BorrowedPreparationFuture {
        polls: &polls,
        drops: &drops,
        _local: std::rc::Rc::new(()),
        _pinned: std::marker::PhantomPinned,
    };
    let mut timer = std::pin::pin!(TimeoutOn::new(access, future, Duration::from_secs(2)));
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!((polls.get(), drops.get()), (0, 0));
    assert!(matches!(timer.as_mut().poll(&mut cx), Poll::Ready(Ok(()))));
    assert_eq!((polls.get(), drops.get()), (1, 1));
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    let again = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| timer.as_mut().poll(&mut cx)));
    assert!(again.is_err());
    assert_eq!((polls.get(), drops.get()), (1, 1));
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

/// 通过 TimeoutOn::poll 观察不可移动异步计算（Future）的实际轮询（poll）地址与析构（drop）地址。
/// 业务析构故意触发恐慌（panic）；用例核对原异常载荷（panic payload）和唯一析构，且观察器拒绝所有计时来源操作。
#[test]
fn test_timeout_on_ready_propagates_business_drop_panic_without_moving_future() {
    use std::{
        cell::Cell,
        future::Future,
        marker::PhantomPinned,
        panic::{catch_unwind, panic_any, AssertUnwindSafe},
        pin::Pin,
        task::{Context, Poll, Waker},
    };

    #[derive(Debug)]
    struct BusinessDropPanic;

    /// 只记录轮询（poll）与析构（drop）时的地址，从不解引用原指针。
    struct PinnedBusiness<'a> {
        poll_address: &'a Cell<Option<*const ()>>,
        drop_address: &'a Cell<Option<*const ()>>,
        drops: &'a Cell<usize>,
        panic_on_drop: &'a Cell<bool>,
        _pinned: PhantomPinned,
    }

    impl Future for PinnedBusiness<'_> {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            self.poll_address.set(Some(std::ptr::from_ref(self.as_ref().get_ref()).cast()));
            Poll::Ready(())
        }
    }

    impl Drop for PinnedBusiness<'_> {
        fn drop(&mut self) {
            self.drop_address.set(Some(std::ptr::from_ref(&*self).cast()));
            self.drops.set(self.drops.get() + 1);
            if self.panic_on_drop.replace(false) {
                panic_any(BusinessDropPanic);
            }
        }
    }

    let calls = Arc::new(AtomicUsize::new(0));
    let access = PreparationAccess {
        id: RuntimeId::try_new().unwrap(),
        calls: Arc::clone(&calls),
        clones: Arc::new(AtomicUsize::new(0)),
        drops: Arc::new(AtomicUsize::new(0)),
    };
    let poll_address = Cell::new(None);
    let drop_address = Cell::new(None);
    let drops = Cell::new(0);
    let panic_on_drop = Cell::new(true);
    {
        let future = PinnedBusiness {
            poll_address: &poll_address,
            drop_address: &drop_address,
            drops: &drops,
            panic_on_drop: &panic_on_drop,
            _pinned: PhantomPinned,
        };
        let mut combined = std::pin::pin!(TimeoutOn::new(access, future, Duration::from_secs(2)));
        let mut cx = Context::from_waker(Waker::noop());
        let result = catch_unwind(AssertUnwindSafe(|| combined.as_mut().poll(&mut cx)));
        // 即使轮询未及时析构（drop），也让该错误表现为断言失败，而非测试退出时的第二次恐慌（panic）。
        panic_on_drop.set(false);
        let panic = result.unwrap_err();
        assert!(panic.is::<BusinessDropPanic>());
        assert!(poll_address.get().is_some());
        assert_eq!(drop_address.get(), poll_address.get());
        assert_eq!(drops.get(), 1);
    }
    assert_eq!(drops.get(), 1);
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}
