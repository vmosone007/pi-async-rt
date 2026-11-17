//! 全部交值等待（wait_all）的公开登记顺序（registration order）与清理（cleanup）验证。
//! 只用公开构造、克隆（Clone）、交值（send）、轮询（poll）和析构（drop），不写内部状态（internal state）。

use pi_async_wait::{wait_all, SendErrorKind, WaitError};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Wake, Waker};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// T380／T384／T385／T388—T397：初始位置（initial slot）无值时，两种公开释放顺序（drop orders）均合法。
/// 真实对象（object）只能在结果槽（result slot）、列表持有（list ownership）和清理守卫（cleanup guard）配齐后运行。
#[test]
fn test_unused_all_pair_releases_in_both_orders() {
    let (wait, sender) = wait_all::<String>();
    drop(wait);
    drop(sender);
    let (wait, sender) = wait_all::<String>();
    drop(sender);
    drop(wait);
}

/// T382／T384／T393／T394：接收先关闭（receiver closes first）返同一原值（original value），先接受再放弃恰一次清理（cleanup）。
/// 首次轮询（poll）前也须清理（cleanup），不把接受成功（acceptance）当作接收完成（reception）。
#[test]
fn test_send_returns_original_on_close_and_cleans_accepted_slot() {
    let (wait, sender) = wait_all::<Box<usize>>();
    let value = Box::new(37);
    let address = std::ptr::from_ref(value.as_ref());
    drop(wait);
    let (kind, returned) = sender.send(value).unwrap_err().into_parts();
    assert_eq!(kind, SendErrorKind::ReceiverClosed);
    assert_eq!(*returned, 37);
    assert_eq!(std::ptr::from_ref(returned.as_ref()), address);

    let drops = Arc::new(AtomicUsize::new(0));
    let (wait, sender) = wait_all();
    sender.send(DropProbe(Arc::clone(&drops))).unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(wait);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

/// T383／T386／T387：每次有效克隆（Clone）拥有独立位置（slot），关闭后拒绝份（rejected sender）可继续克隆（Clone）。
/// 原份（original sender）释放不使其它份失效；所有拒绝（rejection）完整返还同一原值（original value）。
#[test]
fn test_cloned_senders_are_independent_and_closed_clones_stay_rejected() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (wait, sender) = wait_all();
    let second = sender.clone();
    let third = second.clone();
    drop(sender);
    second.send(DropProbe(Arc::clone(&drops))).unwrap();
    third.send(DropProbe(Arc::clone(&drops))).unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(wait);
    assert_eq!(drops.load(Ordering::SeqCst), 2);

    let (wait, sender) = wait_all::<Box<usize>>();
    drop(wait);
    let rejected = sender.clone();
    let again = rejected.clone();
    for sender in [sender, rejected, again] {
        let value = Box::new(43);
        let address = std::ptr::from_ref(value.as_ref());
        let (kind, returned) = sender.send(value).unwrap_err().into_parts();
        assert_eq!(kind, SendErrorKind::ReceiverClosed);
        assert_eq!(*returned, 43);
        assert_eq!(std::ptr::from_ref(returned.as_ref()), address);
    }
}

/// T381／T383／T385：收集（collect）须等全部有效份（active senders）结束，按登记顺序（registration order）保留逐项无值错误（per-slot error）。
/// 交值顺序（send order）刻意相反；完成后重复轮询（poll）须明确恐慌（panic）。
#[test]
fn test_poll_preserves_registration_order_and_each_missing_value() {
    let (mut wait, first) = wait_all::<usize>();
    let second = first.clone();
    let third = second.clone();
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Pending);
    third.send(79).unwrap();
    drop(second);
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Pending);
    first.send(53).unwrap();
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Ready(vec![Ok(53), Err(WaitError::SenderDropped), Ok(79)]));
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| Pin::new(&mut wait).poll(&mut cx))).is_err());

    let (mut wait, sender) = wait_all::<usize>();
    drop(sender);
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Ready(vec![Err(WaitError::SenderDropped)]));
    let (mut wait, sender) = wait_all::<usize>();
    sender.send(83).unwrap();
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Ready(vec![Ok(83)]));
}

/// T381／T390：只有最后有效份（last active sender）结束才通知（notify），且目标是最新真实唤醒器（Waker）。
/// 早先交值（send）和无值释放（drop）都不使等待（wait）提前完成。
#[test]
fn test_only_final_sender_notifies_latest_waker() {
    let (mut wait, first) = wait_all::<usize>();
    let second = first.clone();
    let third = first.clone();
    let old = Arc::new(CountWake(AtomicUsize::new(0)));
    let latest = Arc::new(CountWake(AtomicUsize::new(0)));
    let old_waker = Waker::from(Arc::clone(&old));
    let latest_waker = Waker::from(Arc::clone(&latest));
    assert_eq!(Pin::new(&mut wait).poll(&mut Context::from_waker(&old_waker)), Poll::Pending);
    assert_eq!(Pin::new(&mut wait).poll(&mut Context::from_waker(&latest_waker)), Poll::Pending);
    first.send(101).unwrap();
    drop(second);
    assert_eq!(old.0.load(Ordering::SeqCst), 0);
    assert_eq!(latest.0.load(Ordering::SeqCst), 0);
    drop(third);
    assert_eq!(old.0.load(Ordering::SeqCst), 0);
    assert!(latest.0.load(Ordering::SeqCst) > 0);
    assert_eq!(Pin::new(&mut wait).poll(&mut Context::from_waker(&latest_waker)),
        Poll::Ready(vec![Ok(101), Err(WaitError::SenderDropped), Err(WaitError::SenderDropped)]));
}

/// T381：替换唤醒器（Waker）释放旧用户对象（user object）时，最后交值（send）或无值退出（no-value exit）均不能丢失。
/// 只由公开发送句柄（sender handle）执行动作（action），复查（recheck）须在同次轮询（poll）返回真实结果（result）。
#[test]
fn test_registration_recheck_sees_final_send_or_drop() {
    for send_value in [true, false] {
        let (mut wait, first) = wait_all::<usize>();
        let last = first.clone();
        first.send(103).unwrap();
        let old = Waker::from(Arc::new(FinishOnWakerDrop { sender: Some(last), send_value }));
        assert_eq!(Pin::new(&mut wait).poll(&mut Context::from_waker(&old)), Poll::Pending);
        drop(old);
        let next = Waker::from(Arc::new(CountWake(AtomicUsize::new(0))));
        let last_result = if send_value { Ok(107) } else { Err(WaitError::SenderDropped) };
        assert_eq!(Pin::new(&mut wait).poll(&mut Context::from_waker(&next)), Poll::Ready(vec![Ok(103), last_result]));
    }
}

/// T382／T385／T390：真实通知回调（notification callback）恐慌（panic）不遗留虚假有效份（active sender）或重复退出。
/// 异常后只放弃等待（wait），不要求继续轮询（poll）；已接受原值（accepted value）恰一次清理（cleanup）。
#[test]
fn test_notification_panic_preserves_retirement_and_cleanup() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (mut wait, first) = wait_all();
    let last = first.clone();
    let waker = Waker::from(Arc::new(PanicWake));
    assert!(Pin::new(&mut wait).poll(&mut Context::from_waker(&waker)).is_pending());
    first.send(DropProbe(Arc::clone(&drops))).unwrap();
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| last.send(DropProbe(Arc::clone(&drops)))))
        .unwrap_err();
    assert_eq!(failure.downcast_ref::<usize>(), Some(&941));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(wait);
    assert_eq!(drops.load(Ordering::SeqCst), 2);

    let (mut wait, first) = wait_all::<usize>();
    let last = first.clone();
    assert!(Pin::new(&mut wait).poll(&mut Context::from_waker(&waker)).is_pending());
    drop(first);
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(last))).unwrap_err();
    assert_eq!(failure.downcast_ref::<usize>(), Some(&941));
    drop(wait);
}

/// T384／T393—T397：关闭（close）固定链（fixed list）的首项或中间项析构（drop）首次恐慌（panic）后，余项仍真实清理（cleanup）。
/// 各原值（values）独立计数；异常后保留的发送句柄（sender handle）仍可返还原值（original value），不需等待它退出。
#[test]
fn test_first_cleanup_panic_keeps_every_remaining_slot_owned() {
    for panic_position in [1usize, 2] {
        let drops = Arc::new([AtomicUsize::new(0), AtomicUsize::new(0), AtomicUsize::new(0)]);
        let (wait, first) = wait_all();
        let second = first.clone();
        let third = first.clone();
        let late = first.clone();
        for (position, sender) in [first, second, third].into_iter().enumerate() {
            sender.send(PanicDropProbe { drops: Arc::clone(&drops), position, panic_position }).unwrap();
        }
        let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(wait))).unwrap_err();
        assert_eq!(failure.downcast_ref::<usize>(), Some(&947));
        for count in drops.iter() {
            assert_eq!(count.load(Ordering::SeqCst), 1);
        }
        let (kind, returned) = late.send(PanicDropProbe { drops: Arc::clone(&drops), position: 0, panic_position: usize::MAX })
            .unwrap_err().into_parts();
        assert_eq!(kind, SendErrorKind::ReceiverClosed);
        assert_eq!(drops[0].load(Ordering::SeqCst), 1);
        drop(returned);
        assert_eq!(drops[0].load(Ordering::SeqCst), 2);
    }
}

/// T381：非Send／非Sync／非Unpin原值（value）可带外部短借用（short borrow）本地收集（collect），没有Debug或Clone约束（bounds）。
#[test]
fn test_local_results_keep_non_send_identity_and_short_borrows() {
    struct LocalValue<'a> {
        owner: std::rc::Rc<usize>,
        text: &'a str,
        _pinned: std::marker::PhantomPinned,
    }
    let text = String::from("borrowed-all");
    let local = std::rc::Rc::new(109usize);
    let (mut wait, first) = wait_all();
    let second = first.clone();
    first.send(LocalValue { owner: std::rc::Rc::clone(&local), text: &text, _pinned: std::marker::PhantomPinned }).unwrap();
    drop(second);
    match Pin::new(&mut wait).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(mut values) => {
            assert_eq!(values.len(), 2);
            assert!(matches!(values.pop(), Some(Err(WaitError::SenderDropped))));
            let value = values.pop().unwrap().unwrap();
            assert!(std::rc::Rc::ptr_eq(&value.owner, &local));
            assert_eq!(value.text, "borrowed-all");
        }
        _ => panic!("本地结果（local results）必须完整返回"),
    }
}

/// T381／T382／T383：两个真实线程（threads）各自登记一份新位置（slot）并交值（send），所有已发布位置（published slots）只收集一次。
/// 首两份（initial senders）已顺序登记，后两份并发登记（concurrent registration）的相对顺序（relative order）不由线程启动（thread start）推断。
#[test]
fn test_cross_thread_clones_publish_every_slot_and_keep_initial_order() {
    let (mut wait, first) = wait_all::<std::cell::Cell<usize>>();
    let second = first.clone();
    let left = std::thread::spawn(move || {
        let child = first.clone();
        child.send(std::cell::Cell::new(113)).unwrap();
        first.send(std::cell::Cell::new(127)).unwrap();
    });
    let right = std::thread::spawn(move || {
        let child = second.clone();
        second.send(std::cell::Cell::new(131)).unwrap();
        child.send(std::cell::Cell::new(137)).unwrap();
    });
    left.join().unwrap();
    right.join().unwrap();
    match Pin::new(&mut wait).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(values) => {
            assert_eq!(values.len(), 4);
            let values: Vec<usize> = values.into_iter().map(|result| result.unwrap().get()).collect();
            assert_eq!(&values[..2], &[127, 131]);
            let mut children = values[2..].to_vec();
            children.sort_unstable();
            assert_eq!(children, vec![113, 137]);
        }
        _ => panic!("已结束的登记位置（registered slots）必须全部返回"),
    }
}

/// T382／T383／T384：一个真实业务线程（business thread）登记并发送（send），主线程（main thread）同时放弃接收（close）。
/// 无论接纳（acceptance）或拒绝（rejection），每份原值（original value）恰一次析构（drop）；一次有界竞争（bounded race）不证明全部指定交错（interleavings）。
#[test]
fn test_close_racing_clone_and_send_cleans_every_original_once() {
    let drops = Arc::new(AtomicUsize::new(0));
    let (wait, sender) = wait_all();
    let thread_drops = Arc::clone(&drops);
    let worker = std::thread::spawn(move || {
        let other = sender.clone();
        for sender in [sender, other] {
            if let Err(error) = sender.send(DropProbe(Arc::clone(&thread_drops))) {
                let (kind, value) = error.into_parts();
                assert_eq!(kind, SendErrorKind::ReceiverClosed);
                drop(value);
            }
        }
    });
    drop(wait);
    worker.join().unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

/// T397：清理展开（cleanup unwinding）中再次析构恐慌（drop panic）遵循Rust终止规则（abort rule），仅在独立子进程（child process）验证。
/// 子进程（child process）只经公开wait_all构造和send发布两个真实原值（values）；父进程（parent process）核异常来源和终止诊断（abort diagnostic）。
#[test]
fn test_double_cleanup_panic_aborts_only_child_process() {
    const CHILD: &str = "PI_ASYNC_WAIT_ALL_DOUBLE_DROP_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let (wait, first) = wait_all();
        let second = first.clone();
        first.send(AlwaysPanicDrop(1)).unwrap();
        second.send(AlwaysPanicDrop(2)).unwrap();
        drop(wait);
        panic!("双重析构异常（double drop panic）必须终止");
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "test_double_cleanup_panic_aborts_only_child_process", "--test-threads=1", "--nocapture"])
        .env(CHILD, "1")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(stderr.contains("expected cleanup panic 2"), "{stderr}");
    assert!(stderr.contains("expected cleanup panic 1"), "{stderr}");
    // 当前Windows工具链（toolchain）经结构化异常处理（SEH）快速终止（fail-fast），不输出其它平台的终止文句（abort wording）。
    #[cfg(windows)]
    assert_eq!(output.status.code().map(|code| code as u32), Some(0xc000_0409));
    #[cfg(not(windows))]
    assert!(stderr.contains("panic in a destructor during cleanup"), "{stderr}");
    eprintln!("子进程（child process）终止状态（status）：{}\n{}", output.status, stderr);
}

/// 只在隔离子进程（isolated child process）中使用的真实业务析构（business drop）异常来源（panic source）。
struct AlwaysPanicDrop(usize);
impl Drop for AlwaysPanicDrop {
    fn drop(&mut self) {
        panic!("expected cleanup panic {}", self.0);
    }
}

/// 用户通知回调（user notification callback）只传播可识别原载荷（payload）。
struct PanicWake;
impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        std::panic::panic_any(941usize);
    }
}

/// 各业务原值（business values）独立记录析构（drop）；只有指定一项首次恐慌（panic），不代补余链清理（list cleanup）。
struct PanicDropProbe {
    drops: Arc<[AtomicUsize; 3]>,
    position: usize,
    panic_position: usize,
}
impl Drop for PanicDropProbe {
    fn drop(&mut self) {
        self.drops[self.position].fetch_add(1, Ordering::SeqCst);
        if self.position == self.panic_position {
            std::panic::panic_any(947usize);
        }
    }
}

/// 只观察真实通知（notification），不推进等待（wait）。
struct CountWake(AtomicUsize);
impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// 旧用户唤醒器（user Waker）最后析构（drop）时，经公开句柄（handle）结束最后一份责任（responsibility）。
struct FinishOnWakerDrop {
    sender: Option<pi_async_wait::AllSender<usize>>,
    send_value: bool,
}
impl Wake for FinishOnWakerDrop {
    fn wake(self: Arc<Self>) {}
}
impl Drop for FinishOnWakerDrop {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            if self.send_value {
                sender.send(107).unwrap();
            } else {
                drop(sender);
            }
        }
    }
}

/// 只观察业务原值（business value）的析构（drop），不推进等待（wait）或代补清理（cleanup）。
struct DropProbe(Arc<AtomicUsize>);
impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
