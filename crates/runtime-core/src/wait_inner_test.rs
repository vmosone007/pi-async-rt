//! 带超时等待（wait with timeout）的业务优先（business-first）映射，只经公开组合观察。
use crate::definitions::{RuntimeId, TimerError, WaitTimeoutError};
use crate::time::{RuntimeTime, Sleep, Timeout, TimeoutOn, TimerAccess, TimerEntry, TimerRegistrationKind};
use crate::time::access::TimerCancelRequestOutcome;
use crate::wait::RuntimeWait;
use pi_async_task_queue::timer::{TimerQueueError, TimerSubmitError};
use pi_async_wait::WaitError;
use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

/// 计时访问（TimerAccess）的禁止调用探针：只有业务优先路径，任何取时／登记都会失败。
#[derive(Clone)]
struct NoTimer {
    id: RuntimeId,
    calls: Arc<AtomicUsize>,
}
impl NoTimer {
    fn forbidden<T>(&self) -> T {
        self.calls.fetch_add(1, Ordering::Relaxed);
        panic!("业务优先路径不得触达计时来源（must not touch timer source）")
    }
}
impl TimerAccess for NoTimer {
    type Instant = u64;
    type Key = ();
    type Registration = Arc<crate::time::TimerRegistration<()>>;
    fn runtime_id(&self) -> RuntimeId { self.id }
    fn deadline_after(&self, _: Duration) -> pi_result::RawResult<Self::Instant, TimerError> { self.forbidden() }
    fn prepare_registration(&self, _: TimerRegistrationKind) -> pi_result::RawResult<Self::Registration, TimerError> { self.forbidden() }
    fn try_register(&self, _: Self::Instant, _: TimerEntry<Self::Key, Self::Registration>)
        -> pi_result::RawResult<Self::Key, TimerSubmitError<TimerEntry<Self::Key, Self::Registration>>> { self.forbidden() }
    fn try_register_local(&self, _: Self::Instant, _: TimerEntry<Self::Key, Self::Registration>)
        -> pi_result::RawResult<Self::Key, TimerSubmitError<TimerEntry<Self::Key, Self::Registration>>> { self.forbidden() }
    fn request_cancel(&self, _: &Self::Key) -> pi_result::RawResult<TimerCancelRequestOutcome, TimerQueueError> { self.forbidden() }
    fn defer_cancel(&self, _: Self::Registration, _: Option<TimerQueueError>,
        _: crate::time::access::Confirmation<'_, Self::Key>) { self.forbidden::<()>() }
}
impl RuntimeTime for NoTimer {
    type Timer = Self;
    fn timeout(&self, duration: Duration) -> Timeout<Self::Timer> { Timeout::new(self.clone(), duration) }
    fn timeout_on<F: Future>(&self, future: F, duration: Duration) -> TimeoutOn<Self::Timer, F> {
        TimeoutOn::new(self.clone(), future, duration)
    }
    fn sleep(&self, duration: Duration) -> Sleep<Self::Timer> { Sleep::new(self.clone(), duration) }
}
impl RuntimeWait for NoTimer {}

/// 构造探针；本组只走业务优先分支，来源调用计数必须保持零。
fn no_timer() -> (NoTimer, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    (NoTimer { id: RuntimeId::try_new().expect("运行时身份（runtime id）"), calls: calls.clone() }, calls)
}

/// 先交值后轮询：一次交值等待立即返回原值，且不触达计时来源。
#[test]
fn test_wait_one_timeout_returns_original_value_without_timer_source() {
    let (timer, calls) = no_timer();
    let (mut wait, sender) = timer.wait_one_timeout::<u32>(Duration::from_secs(2));
    sender.send(41).expect("首次交值必须被接受");
    let mut wait = std::pin::pin!(wait);
    let polled = wait.as_mut().poll(&mut Context::from_waker(Waker::noop()));
    assert!(matches!(polled, Poll::Ready(Ok(41))), "业务优先返回原值");
    assert_eq!(calls.load(Ordering::Relaxed), 0, "不得触达计时来源");
}

/// 发送者先释放：原等待错误按Wait保留，不改成整体失败或超时。
#[test]
fn test_wait_one_timeout_preserves_original_wait_error() {
    let (timer, calls) = no_timer();
    let (mut wait, sender) = timer.wait_one_timeout::<u32>(Duration::from_secs(2));
    drop(sender);
    let mut wait = std::pin::pin!(wait);
    let polled = wait.as_mut().poll(&mut Context::from_waker(Waker::noop()));
    assert!(matches!(polled, Poll::Ready(Err(WaitTimeoutError::Wait(WaitError::SenderDropped)))),
        "原等待错误必须保留为Wait");
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

/// 不要求T为Send／Unpin：本地借用计数直接观察，返回同一地址的原值。
#[test]
fn test_wait_one_timeout_keeps_borrowed_non_send_value() {
    let (timer, calls) = no_timer();
    let value = Rc::new(Cell::new(5));
    let address = Rc::as_ptr(&value);
    let (mut wait, sender) = timer.wait_one_timeout::<Rc<Cell<u32>>>(Duration::from_secs(2));
    sender.send(value).expect("本地原值必须被接受");
    let mut wait = std::pin::pin!(wait);
    match wait.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(received)) => {
            assert_eq!(Rc::as_ptr(&received), address, "必须是同一原值");
            assert_eq!(received.get(), 5);
        }
        other => panic!("应原样交出本地原值：{:?}", other.is_pending()),
    }
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

/// 并发竞争只交出一个胜者原值。
#[test]
fn test_wait_any_timeout_returns_exactly_one_winner() {
    let (timer, calls) = no_timer();
    let (mut wait, sender) = timer.wait_any_timeout::<u32>(Duration::from_secs(2));
    let second = sender.clone();
    sender.send(7).expect("首个并发交值必须被接受");
    let mut wait = std::pin::pin!(wait);
    let polled = wait.as_mut().poll(&mut Context::from_waker(Waker::noop()));
    assert!(matches!(polled, Poll::Ready(Ok(7))), "只交出一个胜者（winner）");
    assert!(second.send(9).is_err(), "未取得接受许可的交值保留原值并返还");
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

/// 全部交值按登记顺序完整返回；发送者释放只影响对应项。
#[test]
fn test_wait_all_timeout_returns_complete_ordered_results() {
    let (timer, calls) = no_timer();
    let (mut wait, first) = timer.wait_all_timeout::<u32>(Duration::from_secs(2));
    let second = first.clone();
    first.send(1).expect("登记顺序第一项必须被接受");
    second.send(2).expect("登记顺序第二项必须被接受");
    let mut wait = std::pin::pin!(wait);
    match wait.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(results)) => {
            assert_eq!(results.len(), 2, "必须交完整向量且不截断");
            assert_eq!(results[0].as_ref().copied().ok(), Some(1));
            assert_eq!(results[1].as_ref().copied().ok(), Some(2));
        }
        other => panic!("应返回完整逐项结果：{:?}", other.is_pending()),
    }
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}

/// 发送者释放计入对应项的原错误，不把整体改成失败。
#[test]
fn test_wait_all_timeout_keeps_per_item_sender_dropped_error() {
    let (timer, calls) = no_timer();
    let (mut wait, first) = timer.wait_all_timeout::<u32>(Duration::from_secs(2));
    let second = first.clone();
    first.send(3).expect("第一项必须被接受");
    drop(second);
    let mut wait = std::pin::pin!(wait);
    match wait.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Ok(results)) => {
            assert_eq!(results.len(), 2);
            assert!(matches!(results[0], Ok(3)));
            assert!(matches!(results[1], Err(WaitError::SenderDropped)), "释放只影响对应项");
        }
        other => panic!("应返回逐项结果而非整体失败：{:?}", other.is_pending()),
    }
    assert_eq!(calls.load(Ordering::Relaxed), 0);
}
