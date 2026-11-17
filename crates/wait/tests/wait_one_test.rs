//! 单次交值等待（one-shot wait）的公开责任与清理（cleanup）验证。
//!
//! 使用wait_one、交值句柄（sender handle）及标准轮询（poll）入口；
//! 不注入共享状态（shared state），不在测试辅助代码（test helper）中代补通知（notification）或原值清理（cleanup）。

use pi_async_wait::{wait_one, SendErrorKind, WaitError};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

/// T406／T409／T410／T411：两个合法释放顺序（drop orders）均须完成，空存储（empty storage）不得误读未初始化原值。
/// 必须先配齐构造（construction）与全部析构（drop）路径，随后才能运行真实持有（ownership）用例。
#[test]
fn test_unused_pair_can_be_dropped_in_either_order() {
    let (wait, sender) = wait_one::<String>();
    drop(wait);
    drop(sender);
    let (wait, sender) = wait_one::<String>();
    drop(sender);
    drop(wait);
}

/// T408／T409：接收关闭（receiver close）先于交值许可（send permission）时返还同一原值（original value）。
/// 先接受后放弃则由生产关闭（production close）清理恰好一次，不依赖等待端已轮询（poll）。
#[test]
fn test_send_returns_original_on_close_and_cleans_accepted_value() {
    let (wait, sender) = wait_one::<Box<usize>>();
    let value = Box::new(31);
    let address = std::ptr::from_ref(value.as_ref());
    drop(wait);
    let error = sender.send(value).unwrap_err();
    assert_eq!(error.kind(), SendErrorKind::ReceiverClosed);
    let (kind, returned) = error.into_parts();
    assert_eq!(kind, SendErrorKind::ReceiverClosed);
    assert_eq!(std::ptr::from_ref(returned.as_ref()), address);
    assert_eq!(*returned, 31);

    let drops = Arc::new(AtomicUsize::new(0));
    let (wait, sender) = wait_one();
    sender.send(DropProbe(Arc::clone(&drops))).unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(wait);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

/// T407：首次轮询（poll）前已交值或无值释放时立即得到准确结果，完成后再次轮询（poll）明确拒绝。
/// 返回原非Unpin值（value）不要求克隆（Clone）、同步共享（Sync）或静态生命周期（'static lifetime）。
#[test]
fn test_poll_returns_early_value_or_sender_dropped_once() {
    let text = String::from("borrowed");
    let (mut wait, sender) = wait_one();
    sender.send((std::rc::Rc::new(7usize), text.as_str(), std::marker::PhantomPinned)).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    match Pin::new(&mut wait).poll(&mut cx) {
        Poll::Ready(Ok((value, borrowed, _))) => {
            assert_eq!(*value, 7);
            assert_eq!(borrowed, "borrowed");
        }
        _ => panic!("早到原值（early value）必须立即返回"),
    }
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| Pin::new(&mut wait).poll(&mut cx))).is_err());
    let (mut wait, sender) = wait_one::<usize>();
    drop(sender);
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Ready(Err(WaitError::SenderDropped)));
}

/// T407／T408／T410：挂起（Pending）后只通知最新实际唤醒器（Waker），无值退出也须通知真实等待方。
/// 原生通知（native notification）允许合并，不要求严格一次；旧目标不应承担新登记后的通知（notification）。
#[test]
fn test_pending_wait_notifies_latest_waker_and_sender_drop() {
    let (mut wait, sender) = wait_one::<usize>();
    let first = Arc::new(CountWake(AtomicUsize::new(0)));
    let second = Arc::new(CountWake(AtomicUsize::new(0)));
    let first_waker = Waker::from(Arc::clone(&first));
    let second_waker = Waker::from(Arc::clone(&second));
    assert_eq!(Pin::new(&mut wait).poll(&mut Context::from_waker(&first_waker)), Poll::Pending);
    assert_eq!(Pin::new(&mut wait).poll(&mut Context::from_waker(&second_waker)), Poll::Pending);
    sender.send(41).unwrap();
    assert_eq!(first.0.load(Ordering::SeqCst), 0);
    assert!(second.0.load(Ordering::SeqCst) > 0);
    assert_eq!(Pin::new(&mut wait).poll(&mut Context::from_waker(&second_waker)), Poll::Ready(Ok(41)));

    let (mut wait, sender) = wait_one::<usize>();
    assert_eq!(Pin::new(&mut wait).poll(&mut Context::from_waker(&first_waker)), Poll::Pending);
    drop(sender);
    assert!(first.0.load(Ordering::SeqCst) > 0);
    assert_eq!(Pin::new(&mut wait).poll(&mut Context::from_waker(&first_waker)), Poll::Ready(Err(WaitError::SenderDropped)));
}

/// T408／T409／T410：通知回调（notification callback）恐慌（panic）保留原载荷（payload），放弃后原值（value）只清理一次。
/// 不要求异常之后继续轮询（poll）；生产包装（production wrapper）已移出责任，展开（unwinding）不能重复无值退出。
#[test]
fn test_notification_panic_preserves_cleanup_responsibility() {
    let (mut wait, sender) = wait_one();
    let drops = Arc::new(AtomicUsize::new(0));
    let waker = Waker::from(Arc::new(PanicWake));
    assert!(Pin::new(&mut wait).poll(&mut Context::from_waker(&waker)).is_pending());
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        sender.send(DropProbe(Arc::clone(&drops)))
    })).unwrap_err();
    assert_eq!(failure.downcast_ref::<usize>(), Some(&827));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(wait);
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    let (mut wait, sender) = wait_one::<usize>();
    assert!(Pin::new(&mut wait).poll(&mut Context::from_waker(&waker)).is_pending());
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(sender))).unwrap_err();
    assert_eq!(failure.downcast_ref::<usize>(), Some(&827));
    drop(wait);
}

/// T408／T409：两个真实调用线程（threads）竞争接收关闭（receiver close）与发送（send），无论胜出者是谁原值（value）恰好清理一次。
/// 一个发送线程（sender thread）、一次交值，不以大量重复运行替代指定交错（interleaving）证明。
#[test]
fn test_cross_thread_send_and_close_process_value_once() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (wait, sender) = wait_one();
    let payload = DropProbe(Arc::clone(&drops));
    let send = std::thread::spawn(move || sender.send(payload));
    drop(wait);
    if let Err(error) = send.join().unwrap() {
        let (kind, returned) = error.into_parts();
        assert_eq!(kind, SendErrorKind::ReceiverClosed);
        drop(returned);
    }
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

/// T407：替换通知目标（notification target）时，旧唤醒器（Waker）的合法析构（drop）同步交值。
/// 完成发生在首次检查（first check）与登记结束（registration end）之间，同一次轮询（poll）必须复查并得到原值（value）。
#[test]
fn test_value_arriving_during_waker_registration_is_not_lost() {
    let (mut wait, sender) = wait_one::<usize>();
    let old = Waker::from(Arc::new(SendOnWakerDrop { sender: Some(sender) }));
    assert_eq!(Pin::new(&mut wait).poll(&mut Context::from_waker(&old)), Poll::Pending);
    drop(old);
    let next = Waker::from(Arc::new(CountWake(AtomicUsize::new(0))));
    assert_eq!(Pin::new(&mut wait).poll(&mut Context::from_waker(&next)), Poll::Ready(Ok(61)));
}

/// 用户唤醒器（user Waker）持有真实交值句柄（sender handle），最后释放时经公开send交值，不改内部状态（internal state）。
struct SendOnWakerDrop {
    sender: Option<pi_async_wait::OneSender<usize>>,
}
impl Wake for SendOnWakerDrop {
    fn wake(self: Arc<Self>) {}
}
impl Drop for SendOnWakerDrop {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            sender.send(61).unwrap();
        }
    }
}

/// 只计实际通知（notification），不推进被测异步计算（Future）或补发唤醒（wake-up）。
struct CountWake(AtomicUsize);
impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// 原用户通知（user notification）主动失败，测试（test）只在公开调用外捕获（catch）。
struct PanicWake;
impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        std::panic::panic_any(827usize);
    }
}

/// 只观察业务原值（business value）的实际析构（drop），不触及等待记录（wait record）。
struct DropProbe(Arc<AtomicUsize>);
impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
