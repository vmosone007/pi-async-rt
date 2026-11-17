//! 任一交值等待（wait_any）的公开竞争与责任（responsibility）验证。
//! 测试（tests）只经公开构造、克隆（Clone）、交值（send）、轮询（poll）与析构（drop）；不注入内部计数（internal count）。

use pi_async_wait::{wait_any, SendErrorKind, WaitError};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

/// T398／T402／T403／T405：配齐构造（construction）及全部释放（drop）后，两种无值退出顺序（exit orders）都合法。
#[test]
fn test_unused_any_pair_releases_in_both_orders() {
    let (wait, sender) = wait_any::<String>();
    drop(wait);
    drop(sender);
    let (wait, sender) = wait_any::<String>();
    drop(sender);
    drop(wait);
}

/// T401／T404：开放时克隆（Clone）建立独立责任（responsibility），关闭后的克隆（Clone）固定拒绝且可继续克隆（Clone）。
/// 不以共享持有数（Arc count）推导有效发送数量（active sender count），只核公开原值返还（value return）。
#[test]
fn test_clone_is_independent_and_closed_clones_keep_rejection() {
    let (wait, sender) = wait_any::<Box<usize>>();
    let second = sender.clone();
    drop(sender);
    drop(wait);
    let rejected = second.clone();
    let rejected_again = rejected.clone();
    drop(second);
    for handle in [rejected, rejected_again] {
        let value = Box::new(19);
        let address = std::ptr::from_ref(value.as_ref());
        let error = handle.send(value).unwrap_err();
        let (kind, returned) = error.into_parts();
        assert_eq!(kind, SendErrorKind::ReceiverClosed);
        assert_eq!(std::ptr::from_ref(returned.as_ref()), address);
        assert_eq!(*returned, 19);
    }
}

/// T400：没有胜出者（winner）时，接收关闭（receiver close）后的原有效发送份（Active sender）须返原值（original value）并只结束本份责任（responsibility）。
/// 不先克隆（Clone）为拒绝包装（Rejected）；真实线程（thread）传递非Sync原值（value），覆盖生产Active分支（branch）。
#[test]
fn test_active_sender_after_close_returns_same_non_sync_value() {
    let (wait, sender) = wait_any::<Box<std::cell::Cell<usize>>>();
    let other = sender.clone();
    let value = Box::new(std::cell::Cell::new(29));
    let address = std::ptr::from_ref(value.as_ref()) as usize;
    drop(wait);
    let failure = std::thread::spawn(move || sender.send(value)).join().unwrap().unwrap_err();
    let (kind, returned) = failure.into_parts();
    assert_eq!(kind, SendErrorKind::ReceiverClosed);
    assert_eq!(std::ptr::from_ref(returned.as_ref()) as usize, address);
    assert_eq!(returned.get(), 29);
    let second = Box::new(std::cell::Cell::new(31));
    let address = std::ptr::from_ref(second.as_ref());
    let (kind, returned_second) = other.send(second).unwrap_err().into_parts();
    assert_eq!(kind, SendErrorKind::ReceiverClosed);
    assert_eq!(std::ptr::from_ref(returned_second.as_ref()), address);
    assert_eq!(returned_second.get(), 31);

    let drops = Arc::new(AtomicUsize::new(0));
    let (wait, sender) = wait_any();
    drop(wait);
    let failure = sender.send(DropProbe(Arc::clone(&drops))).unwrap_err();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let (kind, value) = failure.into_parts();
    assert_eq!(kind, SendErrorKind::ReceiverClosed);
    drop(value);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

/// T400：首个被接受原值（accepted value）胜出，后续失败返原值（original value）且原因不被关闭（close）改写。
/// 胜出后克隆（Clone）固定ValueAlreadyAccepted，剩余句柄（handles）不延迟接收方放弃时的原值清理（cleanup）。
#[test]
fn test_first_accepted_value_wins_and_later_senders_keep_originals() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (wait, sender) = wait_any();
    let loser = sender.clone();
    sender.send(DropProbe(Arc::clone(&drops))).unwrap();
    let rejected = loser.clone();
    let again = rejected.clone();
    drop(wait);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    for handle in [loser, rejected, again] {
        let error = handle.send(DropProbe(Arc::clone(&drops))).unwrap_err();
        assert_eq!(error.kind(), SendErrorKind::ValueAlreadyAccepted);
        let (_, value) = error.into_parts();
        drop(value);
    }
    assert_eq!(drops.load(Ordering::SeqCst), 4);
}

/// T399／T403：只有全部有效发送方（senders）无值结束才返回SenderDropped；胜出值（winning value）不等其它句柄（handles）释放。
/// 同一接收端（receiver）完成后再次轮询（poll）明确失败，不允许第二次取值。
#[test]
fn test_poll_waits_for_last_sender_but_not_for_losers_after_acceptance() {
    let (mut wait, sender) = wait_any::<usize>();
    let other = sender.clone();
    let counter = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&counter));
    let mut cx = Context::from_waker(&waker);
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Pending);
    drop(sender);
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Pending);
    drop(other);
    assert!(counter.0.load(Ordering::SeqCst) > 0);
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Ready(Err(WaitError::SenderDropped)));

    let (mut wait, sender) = wait_any::<usize>();
    let other = sender.clone();
    sender.send(73).unwrap();
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Ready(Ok(73)));
    let error = other.send(91).unwrap_err();
    assert_eq!(error.into_parts(), (SendErrorKind::ValueAlreadyAccepted, 91));
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| Pin::new(&mut wait).poll(&mut cx))).is_err());
}

/// T399／T400：两个真实线程（threads）各交一个原值（value），必须恰一成功且接收同一胜出值（winning value）。
/// 失败值（rejected value）完整返回；一次有界竞争（bounded race）不证明全部处理器交错（processor interleavings）。
#[test]
fn test_two_senders_accept_exactly_one_original_value() {
    let (mut wait, first) = wait_any::<Box<usize>>();
    let second = first.clone();
    let left = std::thread::spawn(move || first.send(Box::new(11)));
    let right = std::thread::spawn(move || second.send(Box::new(23)));
    let left = left.join().unwrap();
    let right = right.join().unwrap();
    let expected = match (left, right) {
        (Ok(()), Err(error)) => {
            let (kind, value) = error.into_parts();
            assert_eq!(kind, SendErrorKind::ValueAlreadyAccepted);
            assert_eq!(*value, 23);
            11
        }
        (Err(error), Ok(())) => {
            let (kind, value) = error.into_parts();
            assert_eq!(kind, SendErrorKind::ValueAlreadyAccepted);
            assert_eq!(*value, 11);
            23
        }
        _ => panic!("必须恰有一份成功交值（accepted value）"),
    };
    let mut cx = Context::from_waker(Waker::noop());
    match Pin::new(&mut wait).poll(&mut cx) {
        Poll::Ready(Ok(value)) => assert_eq!(*value, expected),
        _ => panic!("胜出原值（winning value）必须已保存"),
    }
}

/// T399／T400／T403：通知（notification）恐慌（panic）不重复结束有效责任（active responsibility），原值（value）仍可恰一次清理（cleanup）。
/// 不在异常后继续轮询（poll）；剩余句柄（handle）合法释放不把胜出事实（winning fact）改成无值结束。
#[test]
fn test_waker_panic_does_not_lose_value_or_double_retire_sender() {
    let (mut wait, sender) = wait_any();
    let other = sender.clone();
    let drops = Arc::new(AtomicUsize::new(0));
    let waker = Waker::from(Arc::new(PanicWake));
    assert!(Pin::new(&mut wait).poll(&mut Context::from_waker(&waker)).is_pending());
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sender.send(DropProbe(Arc::clone(&drops)))))
        .unwrap_err();
    assert_eq!(failure.downcast_ref::<usize>(), Some(&913));
    drop(other);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(wait);
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    let (mut wait, sender) = wait_any::<usize>();
    assert!(Pin::new(&mut wait).poll(&mut Context::from_waker(&waker)).is_pending());
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(sender))).unwrap_err();
    assert_eq!(failure.downcast_ref::<usize>(), Some(&913));
    drop(wait);
}

/// T399：本地原值（local value）允许非Send、非Sync、非Unpin及外部短借用（short borrow）。
#[test]
fn test_local_value_keeps_borrow_and_non_send_identity() {
    let text = String::from("borrowed");
    let local = std::rc::Rc::new(17);
    let (mut wait, sender) = wait_any();
    sender.send((std::rc::Rc::clone(&local), text.as_str(), std::marker::PhantomPinned)).unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    match Pin::new(&mut wait).poll(&mut cx) {
        Poll::Ready(Ok((returned, borrowed, _))) => {
            assert!(std::rc::Rc::ptr_eq(&returned, &local));
            assert_eq!(borrowed, "borrowed");
        }
        _ => panic!("必须领取原本地值（original local value）"),
    }
}

/// T399：替换真实唤醒器（Waker）时交值（send）必须被登记后的复查（recheck）看到。
/// 原旧唤醒器（Waker）最后释放时经公开send交值，且后续通知目标（notification target）必须是实际最新Context。
#[test]
fn test_waker_replacement_observes_value_sent_during_registration() {
    let (mut wait, sender) = wait_any::<usize>();
    let old = Waker::from(Arc::new(SendOnWakerDrop { sender: Some(sender) }));
    assert_eq!(Pin::new(&mut wait).poll(&mut Context::from_waker(&old)), Poll::Pending);
    drop(old);
    let next = Waker::from(Arc::new(CountWake(AtomicUsize::new(0))));
    assert_eq!(Pin::new(&mut wait).poll(&mut Context::from_waker(&next)), Poll::Ready(Ok(47)));

    let (mut wait, sender) = wait_any::<usize>();
    let first = Arc::new(CountWake(AtomicUsize::new(0)));
    let second = Arc::new(CountWake(AtomicUsize::new(0)));
    let first_waker = Waker::from(Arc::clone(&first));
    let second_waker = Waker::from(Arc::clone(&second));
    assert_eq!(Pin::new(&mut wait).poll(&mut Context::from_waker(&first_waker)), Poll::Pending);
    assert_eq!(Pin::new(&mut wait).poll(&mut Context::from_waker(&second_waker)), Poll::Pending);
    sender.send(59).unwrap();
    assert_eq!(first.0.load(Ordering::SeqCst), 0);
    assert!(second.0.load(Ordering::SeqCst) > 0);
    assert_eq!(Pin::new(&mut wait).poll(&mut Context::from_waker(&second_waker)), Poll::Ready(Ok(59)));
}

/// 真实用户唤醒器（user Waker）只在最后析构（drop）时消耗公开发送句柄（sender handle）。
struct SendOnWakerDrop {
    sender: Option<pi_async_wait::AnySender<usize>>,
}
impl Wake for SendOnWakerDrop {
    fn wake(self: Arc<Self>) {}
}
impl Drop for SendOnWakerDrop {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            sender.send(47).unwrap();
        }
    }
}

/// 原用户回调（user callback）主动恐慌（panic），只由公开调用外的测试边界（test boundary）捕获（catch）。
struct PanicWake;
impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        std::panic::panic_any(913usize);
    }
}

/// 只计真实通知（notification），不推进等待（wait）或补发原生通知（native notification）。
struct CountWake(AtomicUsize);
impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// 只观察原业务载荷（business payload）的析构（drop），不触及等待数量（wait count）。
struct DropProbe(Arc<AtomicUsize>);
impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
