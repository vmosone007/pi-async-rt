//! 计时等待（timeout）的构造、控制句柄（control handle）与未开始结束，只经公开接口观察。
use crate::definitions::{RuntimeId, TimerError};
use crate::time::access::{Confirmation, TimerAccess, TimerCancelRequestOutcome};
use crate::time::entry::TimerEntry;
use crate::time::registration::{TimerRegistration, TimerRegistrationKind};
use crate::time::Timeout;
use pi_async_task_queue::timer::{TimerQueueError, TimerSubmitError};
use std::sync::{atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering}, Arc, OnceLock};
use std::time::Duration;

/// 原键（key）；只作身份关联。
struct ProbeKey(usize);

/// 记录准备次数与种类、克隆次数的计时访问（TimerAccess）。
struct ProbeTimer {
    id: RuntimeId,
    prepares: Arc<AtomicUsize>,
    deadlines: Arc<AtomicUsize>,
    registers: Arc<AtomicUsize>,
    requests: Arc<AtomicUsize>,
    defers: Arc<AtomicUsize>,
    fail: Arc<AtomicBool>,
    kept: Arc<OnceLock<TimerEntry<ProbeKey, Arc<TimerRegistration<ProbeKey>>>>>,
    kind: Arc<AtomicU8>,
    clones: Arc<AtomicUsize>,
    failure: Option<&'static str>,
    panic_on_clone: Arc<AtomicBool>,
}
impl TimerAccess for ProbeTimer {
    type Instant = u64;
    type Key = ProbeKey;
    type Registration = Arc<TimerRegistration<ProbeKey>>;
    fn runtime_id(&self) -> RuntimeId { self.id }
    fn deadline_after(&self, _: Duration) -> pi_result::RawResult<Self::Instant, TimerError> {
        self.deadlines.fetch_add(1, Ordering::Relaxed);
        Ok(7)
    }
    fn prepare_registration(&self, kind: TimerRegistrationKind) -> pi_result::RawResult<Self::Registration, TimerError> {
        self.prepares.fetch_add(1, Ordering::Relaxed);
        self.kind.store(match kind { TimerRegistrationKind::Wait => 1, TimerRegistrationKind::Task => 2 }, Ordering::Relaxed);
        if let Some(reason) = self.failure {
            return Err(TimerError::RegistrationPreparation(
                pi_result::Error::new(pi_result::ErrorKind::Unavailable).attach(reason),
            ));
        }
        Ok(Arc::new(TimerRegistration::new(kind)))
    }
    fn try_register(&self, _: Self::Instant, entry: TimerEntry<Self::Key, Self::Registration>)
        -> pi_result::RawResult<Self::Key, TimerSubmitError<TimerEntry<Self::Key, Self::Registration>>> {
        self.registers.fetch_add(1, Ordering::Relaxed);
        // 真实来源会保管入口直到到期或移除；测试同样持有，避免提前结束登记。
        assert!(self.kept.set(entry).is_ok(), "同一等待只能登记一次");
        Ok(ProbeKey(11))
    }
    fn try_register_local(&self, _: Self::Instant, _: TimerEntry<Self::Key, Self::Registration>)
        -> pi_result::RawResult<Self::Key, TimerSubmitError<TimerEntry<Self::Key, Self::Registration>>> {
        panic!("构造阶段不得本地登记（must not register locally）")
    }
    fn request_cancel(&self, _: &Self::Key) -> pi_result::RawResult<TimerCancelRequestOutcome, TimerQueueError> {
        self.requests.fetch_add(1, Ordering::Relaxed);
        if self.fail.load(Ordering::Relaxed) {
            Err(TimerQueueError::Busy)
        } else {
            Ok(TimerCancelRequestOutcome::Recorded)
        }
    }
    fn defer_cancel(&self, registration: Self::Registration, _: Option<TimerQueueError>, confirmation: Confirmation<'_, Self::Key>) {
        self.defers.fetch_add(1, Ordering::Relaxed);
        let _ = confirmation.deferred(registration.as_ref());
    }
}
impl Clone for ProbeTimer {
    fn clone(&self) -> Self {
        if self.panic_on_clone.swap(false, Ordering::Relaxed) {
            std::panic::panic_any(114_2u32);
        }
        self.clones.fetch_add(1, Ordering::Relaxed);
        Self {
            id: self.id,
            prepares: self.prepares.clone(),
            deadlines: self.deadlines.clone(),
            registers: self.registers.clone(),
            requests: self.requests.clone(),
            defers: self.defers.clone(),
            fail: self.fail.clone(),
            kept: self.kept.clone(),
            kind: self.kind.clone(),
            clones: self.clones.clone(),
            failure: self.failure,
            panic_on_clone: self.panic_on_clone.clone(),
        }
    }
}

/// 构造探针；不触发任何来源操作。
fn probe(failure: Option<&'static str>) -> (ProbeTimer, Arc<AtomicUsize>, Arc<AtomicU8>, Arc<AtomicUsize>) {
    let prepares = Arc::new(AtomicUsize::new(0));
    let kind = Arc::new(AtomicU8::new(0));
    let clones = Arc::new(AtomicUsize::new(0));
    let timer = ProbeTimer {
        id: RuntimeId::try_new().expect("运行时身份（runtime id）"),
        prepares: prepares.clone(),
        deadlines: Arc::new(AtomicUsize::new(0)),
        registers: Arc::new(AtomicUsize::new(0)),
        requests: Arc::new(AtomicUsize::new(0)),
        defers: Arc::new(AtomicUsize::new(0)),
        fail: Arc::new(AtomicBool::new(false)),
        kept: Arc::new(OnceLock::new()),
        kind: kind.clone(),
        clones: clones.clone(),
        failure,
        panic_on_clone: Arc::new(AtomicBool::new(false)),
    };
    (timer, prepares, kind, clones)
}

/// 访问对象克隆（Clone）发生恐慌（panic）后，下一次真实取消仍能转交给原来源。
#[test]
fn test_cancel_can_retry_after_timer_access_clone_panics() {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::{future::Future, pin::Pin, task::{Context, Poll, Waker}};

    let (timer, _, _, _) = probe(None);
    let mut timeout = Timeout::new(timer.clone(), Duration::from_millis(5));
    let control = timeout.control_handle();
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(Pin::new(&mut timeout).poll(&mut cx), Poll::Pending));
    assert_eq!(timer.registers.load(Ordering::Relaxed), 1);

    timer.panic_on_clone.store(true, Ordering::Relaxed);
    let payload = catch_unwind(AssertUnwindSafe(|| control.cancel())).unwrap_err();
    assert_eq!(payload.downcast_ref::<u32>(), Some(&114_2));
    assert_eq!(timer.requests.load(Ordering::Relaxed), 0);

    assert!(matches!(control.cancel(), Ok(true)));
    assert_eq!(timer.requests.load(Ordering::Relaxed), 1);
    assert_eq!(timer.defers.load(Ordering::Relaxed), 0);
}

/// 零时长不取时、不准备登记；控制句柄（control handle）仍可取得与克隆。
#[test]
fn test_zero_timeout_prepares_nothing_and_keeps_control_usable() {
    let (timer, prepares, kind, _) = probe(None);
    let timeout = Timeout::new(timer, Duration::ZERO);
    assert_eq!(prepares.load(Ordering::Relaxed), 0, "零时长不准备登记");
    assert_eq!(kind.load(Ordering::Relaxed), 0);
    let control = timeout.control_handle();
    let clone = control.clone();
    drop(clone);
    drop(control);
    drop(timeout);
    assert_eq!(prepares.load(Ordering::Relaxed), 0, "构造与结束都不得触达来源");
}

/// 正时长不足1ms属入口非法：不准备登记，错误保留到首次轮询。
#[test]
fn test_invalid_positive_duration_defers_error_without_source() {
    let (timer, prepares, _, _) = probe(None);
    let timeout = Timeout::new(timer, Duration::from_nanos(999_999));
    assert_eq!(prepares.load(Ordering::Relaxed), 0, "非法时长不得准备登记");
    let _control = timeout.control_handle();
    drop(timeout);
    assert_eq!(prepares.load(Ordering::Relaxed), 0);
}

/// 正时长只准备一次等待登记，不取时；控制与克隆不重复准备。
#[test]
fn test_positive_timeout_prepares_wait_once_and_controls_do_not_repare() {
    let (timer, prepares, kind, clones) = probe(None);
    let timeout = Timeout::new(timer, Duration::from_millis(5));
    assert_eq!(prepares.load(Ordering::Relaxed), 1, "构造只准备一次");
    assert_eq!(kind.load(Ordering::Relaxed), 1, "等待用途（Wait）");
    let control = timeout.control_handle();
    let clone = control.clone();
    assert_eq!(prepares.load(Ordering::Relaxed), 1, "控制与克隆不得重新准备");
    assert!(clones.load(Ordering::Relaxed) >= 2, "控制与克隆只复制原访问");
    drop(clone);
    drop(control);
    drop(timeout);
    assert_eq!(prepares.load(Ordering::Relaxed), 1, "未开始的结束不得重新准备");
}

/// 准备失败保留原错误且不 panic；控制句柄仍安全，结束不触达来源。
#[test]
fn test_preparation_failure_is_kept_and_drop_stays_local() {
    let (timer, prepares, _, _) = probe(Some("登记准备（registration preparation）失败"));
    let timeout = Timeout::new(timer, Duration::from_millis(3));
    assert_eq!(prepares.load(Ordering::Relaxed), 1);
    let control = timeout.control_handle();
    drop(control);
    drop(timeout);
    assert_eq!(prepares.load(Ordering::Relaxed), 1, "结束不得重试准备");
}

/// 零时长首次轮询只让出一次并安排再唤醒，第二次轮询成功；全程不触达来源。
#[test]
fn test_zero_timeout_yields_once_then_ready_without_timer_source() {
    use std::future::Future;
    use std::sync::atomic::AtomicUsize;
    struct CountingWake(AtomicUsize);
    impl std::task::Wake for CountingWake {
        fn wake(self: Arc<Self>) { self.0.fetch_add(1, Ordering::Relaxed); }
    }
    let (timer, prepares, _, _) = probe(None);
    let mut timeout = std::pin::pin!(Timeout::new(timer, Duration::ZERO));
    let wake = Arc::new(CountingWake(AtomicUsize::new(0)));
    let waker = std::task::Waker::from(wake.clone());
    let mut cx = std::task::Context::from_waker(&waker);
    assert!(timeout.as_mut().poll(&mut cx).is_pending(), "零时长先让出一次");
    assert_eq!(wake.0.load(Ordering::Relaxed), 1, "必须安排一次再唤醒");
    assert!(matches!(timeout.as_mut().poll(&mut cx), std::task::Poll::Ready(Ok(()))), "第二次轮询成功");
    assert_eq!(prepares.load(Ordering::Relaxed), 0, "零时长不准备登记");
}

/// 非法正时长的错误只在首次轮询交出一次，之后再次轮询明确恐慌。
#[test]
fn test_invalid_duration_error_is_delivered_once_then_panics() {
    use std::future::Future;
    let (timer, prepares, _, _) = probe(None);
    let mut timeout = std::pin::pin!(Timeout::new(timer, Duration::from_nanos(1)));
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    match timeout.as_mut().poll(&mut cx) {
        std::task::Poll::Ready(Err(TimerError::InvalidDuration { value, min_positive, max })) => {
            assert_eq!(value, Duration::from_nanos(1));
            assert_eq!(min_positive, Duration::from_millis(1));
            assert!(max.is_none(), "本入口没有固定上限");
        }
        _ => panic!("首次轮询必须交出原非法时长错误"),
    }
    assert_eq!(prepares.load(Ordering::Relaxed), 0);
    let again = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = timeout.as_mut().poll(&mut cx);
    }));
    assert!(again.is_err(), "完成后再次轮询必须明确恐慌（panic）");
}

/// 正时长首次轮询只取时一次并登记一次，随后返回Pending且不重取时。
#[test]
fn test_positive_timeout_registers_once_and_then_waits() {
    use std::future::Future;
    let (timer, prepares, _, _) = probe(None);
    let mut timeout = std::pin::pin!(Timeout::new(timer, Duration::from_millis(4)));
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(timeout.as_mut().poll(&mut cx).is_pending(), "登记后等待原结果");
    assert_eq!(prepares.load(Ordering::Relaxed), 1, "只准备一次");
    assert!(timeout.as_mut().poll(&mut cx).is_pending(), "结果未到仍等待");
    assert_eq!(prepares.load(Ordering::Relaxed), 1, "再次轮询不得重新准备");
}

/// 零时长与非法时长都没有有效登记：控制取消返回false且不触达来源。
#[test]
fn test_control_cancel_without_valid_registration_returns_false() {
    let (timer, prepares, _, _) = probe(None);
    let timeout = Timeout::new(timer, Duration::ZERO);
    let control = timeout.control_handle();
    assert!(matches!(control.cancel(), Ok(false)), "零时长控制不可取消");
    drop(control);
    drop(timeout);
    assert_eq!(prepares.load(Ordering::Relaxed), 0);

    let (timer, prepares, _, _) = probe(None);
    let timeout = Timeout::new(timer, Duration::from_nanos(9));
    let control = timeout.control_handle();
    assert!(matches!(control.cancel(), Ok(false)), "准备失败／非法控制不可取消");
    drop(control);
    drop(timeout);
    assert_eq!(prepares.load(Ordering::Relaxed), 0);
}

/// 尚未开始（未取时、无键）的取消只记录兴趣，由等待值本地完成为Cancelled。
#[test]
fn test_control_cancel_before_start_finishes_locally_as_cancelled() {
    use std::future::Future;
    let (timer, prepares, _, _) = probe(None);
    let requests = timer.requests.clone();
    let mut timeout = std::pin::pin!(Timeout::new(timer, Duration::from_millis(6)));
    let control = timeout.as_ref().control_handle();
    assert!(matches!(control.cancel(), Ok(true)), "原登记已记录取消，不要求已有来源键");
    assert_eq!(requests.load(Ordering::Relaxed), 0, "尚未登记不调用来源取消");
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(matches!(timeout.as_mut().poll(&mut cx), std::task::Poll::Ready(Err(TimerError::Cancelled))),
        "预先取消由等待值本地结束");
    assert!(matches!(control.cancel(), Ok(false)), "等待结束后原控制不再记录请求");
    assert_eq!(requests.load(Ordering::Relaxed), 0);
    assert_eq!(prepares.load(Ordering::Relaxed), 1, "登记准备发生在构造时");
}

/// 已登记（键就绪）时可取消：返回Ok(true)并只尝试一次来源取消。
#[test]
fn test_control_cancel_after_registration_reaches_source_once() {
    use std::future::Future;
    let (timer, _, _, _) = probe(None);
    let requests = timer.requests.clone();
    let mut timeout = std::pin::pin!(Timeout::new(timer, Duration::from_millis(7)));
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(timeout.as_mut().poll(&mut cx).is_pending(), "首次轮询完成登记");
    let control = timeout.as_ref().control_handle();
    assert!(matches!(control.cancel(), Ok(true)), "已登记项可取消");
    assert_eq!(requests.load(Ordering::Relaxed), 1, "只尝试一次来源取消");
}

/// 来源拒绝取消时把原错误映射交调用者，责任由预备接续保留。
#[test]
fn test_control_cancel_surfaces_original_source_error() {
    use std::future::Future;
    let (timer, _, _, _) = probe(None);
    timer.fail.store(true, Ordering::Relaxed);
    let mut timeout = std::pin::pin!(Timeout::new(timer, Duration::from_millis(8)));
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(timeout.as_mut().poll(&mut cx).is_pending());
    let control = timeout.as_ref().control_handle();
    match control.cancel() {
        Err(TimerError::Queue(TimerQueueError::Busy)) => {}
        _ => panic!("必须原样交出原来源错误"),
    }
    drop(control);
    assert!(matches!(timeout.as_mut().poll(&mut cx), std::task::Poll::Pending), "取消请求不结束等待");
}
