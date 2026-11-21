//! 根专用推进（root-only block_on）的通知（notification）、固定地址（pinning）及退出顺序（exit order）。
use pi_async_runtime_core::{block_on::block_on_root, context::record::DriveScope};
use std::{cell::{Cell, RefCell}, future::Future, marker::PhantomPinned, pin::Pin, rc::Rc,
    sync::{Arc, atomic::{AtomicUsize, Ordering}}, task::{Context, Poll, Waker}};

/// 使用借用（borrow）、非跨线程持有（Rc）和禁止移动标记（PhantomPinned）的真实根异步计算（root Future）。
/// 地址（address）只作相等比较，不解引用裸指针（raw pointer）；两次通知（notifications）合并为第二次轮询（poll）。
struct PinnedRoot<'a> {
    value: &'a str,
    polls: &'a Cell<usize>,
    drops: &'a Cell<usize>,
    address: Cell<*const Self>,
    retained: Rc<RefCell<Option<Waker>>>,
    _pinned: PhantomPinned,
}
impl<'a> Future for PinnedRoot<'a> {
    type Output = &'a str;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_ref().get_ref();
        assert!(DriveScope::is_active());
        assert!(pi_async_task::task::try_current_task_id().is_none());
        let address = std::ptr::from_ref(this);
        match this.polls.get() {
            0 => {
                assert!(this.address.get().is_null());
                this.address.set(address);
                this.polls.set(1);
                *this.retained.borrow_mut() = Some(cx.waker().clone());
                cx.waker().wake_by_ref();
                cx.waker().clone().wake();
                Poll::Pending
            }
            1 => {
                assert_eq!(this.address.get(), address);
                this.polls.set(2);
                Poll::Ready(this.value)
            }
            _ => panic!("完成后不能再次轮询（completed root must not be polled again）"),
        }
    }
}
impl Drop for PinnedRoot<'_> {
    fn drop(&mut self) {
        assert!(DriveScope::is_active());
        assert_eq!(self.address.get(), std::ptr::from_ref(self));
        self.drops.set(self.drops.get() + 1);
    }
}

/// 验证 block_on_root 接受有借用（borrowed）、!Send、!Unpin 的异步计算（Future），不建立任务身份（TaskId）。
/// 原值（original value）在活动范围（active scope）内恰一次原地析构（in-place drop）；保留唤醒器（retained Waker）不延长原值生命周期（lifetime）。
#[test]
fn test_root_block_on_preserves_pin_borrow_and_drop_before_scope_exit() {
    let value = String::from("borrowed root value");
    let polls = Cell::new(0);
    let drops = Cell::new(0);
    let retained = Rc::new(RefCell::new(None));
    assert!(!DriveScope::is_active());
    let result = block_on_root(PinnedRoot { value: &value, polls: &polls, drops: &drops,
        address: Cell::new(std::ptr::null()), retained: retained.clone(), _pinned: PhantomPinned });
    assert_eq!(result, "borrowed root value");
    assert_eq!(polls.get(), 2);
    assert_eq!(drops.get(), 1);
    assert!(!DriveScope::is_active());
    let waker = retained.borrow_mut().take().unwrap();
    waker.wake_by_ref();
    std::thread::spawn(move || waker.wake()).join().unwrap();
    assert_eq!(drops.get(), 1);
    assert!(!DriveScope::is_active());
}

/// 一个真实有限发布线程（finite publishing thread）只发布结果并通知根（notify root）；不代替生产轮询（production poll）或清理（cleanup）。
struct WakeLater<'a> {
    polls: &'a Cell<usize>,
    result: Arc<AtomicUsize>,
    worker: &'a RefCell<Option<std::thread::JoinHandle<()>>>,
}
impl Future for WakeLater<'_> {
    type Output = usize;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<usize> {
        let this = self.get_mut();
        assert!(DriveScope::is_active());
        assert!(pi_async_task::task::try_current_task_id().is_none());
        let count = this.polls.get();
        this.polls.set(count + 1);
        if count == 0 {
            let result = this.result.clone();
            let waker = cx.waker().clone();
            *this.worker.borrow_mut() = Some(std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(2));
                result.store(91, Ordering::Relaxed);
                waker.wake();
            }));
            Poll::Pending
        } else {
            assert_eq!(count, 1);
            let result = this.result.load(Ordering::Relaxed);
            assert_eq!(result, 91);
            Poll::Ready(result)
        }
    }
}

/// 验证跨线程根通知（cross-thread root notification）及其发布可见性（publication visibility），没有通知前不重复轮询（poll）。
/// 延迟（delay）只安排一次有限发布，不宣称精确命中检查与停放（park）之间的内部窗口（internal window）。
#[test]
fn test_root_block_on_observes_cross_thread_publication_and_wake() {
    let polls = Cell::new(0);
    let worker = RefCell::new(None);
    let result = Arc::new(AtomicUsize::new(0));
    assert_eq!(block_on_root(WakeLater { polls: &polls, result: result.clone(), worker: &worker }), 91);
    worker.borrow_mut().take().unwrap().join().unwrap();
    assert_eq!(polls.get(), 2);
    assert!(!DriveScope::is_active());
}

/// 观察异常退出（unwinding）和首次轮询前拒绝（rejection before first poll）的原值释放（original-value drop）。
struct PanicOrCount<'a> { polls: &'a Cell<usize>, drops: &'a Cell<usize>, panic: bool }
impl Future for PanicOrCount<'_> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        assert!(DriveScope::is_active());
        self.polls.set(self.polls.get() + 1);
        if self.panic { std::panic::panic_any(789_u32); }
        Poll::Ready(())
    }
}
impl Drop for PanicOrCount<'_> {
    fn drop(&mut self) { assert!(DriveScope::is_active()); self.drops.set(self.drops.get() + 1); }
}

/// 验证根恐慌载荷（root panic payload）保持、析构（drop）先于活动范围恢复（scope restoration），非法嵌套（nested drive）在首次轮询（poll）前拒绝。
#[test]
fn test_root_block_on_unwinds_and_rejects_nested_drive_before_poll() {
    let polls = Cell::new(0); let drops = Cell::new(0);
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        block_on_root(PanicOrCount { polls: &polls, drops: &drops, panic: true });
    }));
    assert_eq!(failure.unwrap_err().downcast_ref::<u32>(), Some(&789));
    assert_eq!(polls.get(), 1); assert_eq!(drops.get(), 1);
    assert!(!DriveScope::is_active());
    let scope = DriveScope::enter();
    let nested = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        block_on_root(PanicOrCount { polls: &polls, drops: &drops, panic: false });
    }));
    assert!(nested.is_err());
    assert_eq!(polls.get(), 1); assert_eq!(drops.get(), 2);
    assert!(DriveScope::is_active());
    drop(scope);
    assert!(!DriveScope::is_active());
}

/// 无服务工作（no service work）的合法驱动能力（driver capability）；只观察组合调用（composition calls）并委托原根停放（root park）。
/// 不创建服务（service）、推进任务（Task）或发布结果（result），根结果（root result）只由真实异步计算（Future）及有限发布线程（publisher thread）产生。
struct EmptyDriver { turns: usize, parks: usize, panic_on_turn: bool }
impl pi_async_runtime_core::block_on::BlockOnDriver for EmptyDriver {
    fn turn(&mut self) -> bool {
        assert!(DriveScope::is_active());
        assert!(pi_async_task::task::try_current_task_id().is_none());
        self.turns += 1;
        if self.panic_on_turn { std::panic::panic_any(321_u32); }
        false
    }
    fn park(&mut self, root: &pi_async_runtime_core::block_on::RootWakeState) {
        assert!(DriveScope::is_active());
        self.parks += 1;
        let was_notified = root.is_notified();
        if was_notified { assert!(root.is_notified()); }
        root.park();
        assert!(root.is_notified());
        assert!(root.is_notified());
    }
}

/// 验证 block_on_with_driver 在根就绪（root Ready）时立即返回，不轮转或停放服务（service turn/park）。
/// 跨线程通知（cross-thread notification）走原根停放（root park）；重复查询（query）不消费通知（notification），不重建活动范围（DriveScope）。
/// 此处只验组合协议（composition protocol），具体执行器（executor）及真实来源（sources）尚未装配。
#[test]
fn test_driver_block_on_uses_original_root_notification_and_stops_at_ready() {
    use pi_async_runtime_core::block_on::block_on_with_driver;
    let mut driver = EmptyDriver { turns: 0, parks: 0, panic_on_turn: false };
    assert_eq!(block_on_with_driver(async { 37 }, &mut driver), 37);
    assert_eq!(driver.turns, 0); assert_eq!(driver.parks, 0);
    let polls = Cell::new(0); let worker = RefCell::new(None);
    assert_eq!(block_on_with_driver(WakeLater { polls: &polls, result: Arc::new(AtomicUsize::new(0)), worker: &worker }, &mut driver), 91);
    worker.borrow_mut().take().unwrap().join().unwrap();
    assert_eq!(polls.get(), 2);
    assert_eq!(driver.turns, 1); assert_eq!(driver.parks, 1);
    assert!(!DriveScope::is_active());
    let value = String::from("driver pinned root");
    let polls = Cell::new(0); let drops = Cell::new(0);
    let retained = Rc::new(RefCell::new(None));
    assert_eq!(block_on_with_driver(PinnedRoot { value: &value, polls: &polls, drops: &drops,
        address: Cell::new(std::ptr::null()), retained: retained.clone(), _pinned: PhantomPinned }, &mut driver), "driver pinned root");
    assert_eq!(polls.get(), 2); assert_eq!(drops.get(), 1);
    assert_eq!(driver.turns, 2); assert_eq!(driver.parks, 2);
    retained.borrow_mut().take().unwrap().wake();
    assert!(!DriveScope::is_active());
}

/// 验证服务驱动（service driver）的原恐慌载荷（panic payload）保持、根值原地释放（in-place root drop）先于活动范围退出（scope exit）。
/// 根自身异常（root panic）与非法嵌套（nested drive）均不能触发后续服务轮转（service turn）。
#[test]
fn test_driver_block_on_preserves_panic_and_rejects_nested_entry() {
    use pi_async_runtime_core::block_on::block_on_with_driver;
    let mut driver = EmptyDriver { turns: 0, parks: 0, panic_on_turn: true };
    let polls = Cell::new(0); let drops = Cell::new(0);
    let retained = Rc::new(RefCell::new(None));
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        block_on_with_driver(PinnedRoot { value: "panic", polls: &polls, drops: &drops,
            address: Cell::new(std::ptr::null()), retained: retained.clone(), _pinned: PhantomPinned }, &mut driver);
    }));
    assert_eq!(failure.unwrap_err().downcast_ref::<u32>(), Some(&321));
    assert_eq!(polls.get(), 1); assert_eq!(drops.get(), 1);
    assert_eq!(driver.turns, 1); assert_eq!(driver.parks, 0);
    assert!(!DriveScope::is_active());
    retained.borrow_mut().take().unwrap().wake();
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        block_on_with_driver(PanicOrCount { polls: &polls, drops: &drops, panic: true }, &mut driver);
    }));
    assert_eq!(failure.unwrap_err().downcast_ref::<u32>(), Some(&789));
    assert_eq!(polls.get(), 2); assert_eq!(drops.get(), 2); assert_eq!(driver.turns, 1);
    assert!(!DriveScope::is_active());
    let scope = DriveScope::enter();
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        block_on_with_driver(PanicOrCount { polls: &polls, drops: &drops, panic: false }, &mut driver);
    }));
    assert!(failure.is_err());
    assert_eq!(polls.get(), 2); assert_eq!(drops.get(), 3); assert_eq!(driver.turns, 1);
    assert!(DriveScope::is_active());
    drop(scope); assert!(!DriveScope::is_active());
}

/// 只观察核心组合协议（composition protocol）的立即工作答复（immediate-work response）；
/// 不拥有任务（Task）或来源（source），不能用它认定具体执行器（executor）已经完成装配（integration）。
struct ImmediateWorkDriver { turns: usize }
impl pi_async_runtime_core::block_on::BlockOnDriver for ImmediateWorkDriver {
    fn turn(&mut self) -> bool {
        assert!(DriveScope::is_active());
        assert!(pi_async_task::task::try_current_task_id().is_none());
        self.turns += 1;
        assert_eq!(self.turns, 1);
        true
    }
    fn park(&mut self, _: &pi_async_runtime_core::block_on::RootWakeState) {
        panic!("已确认立即工作（immediate work）时不得停放（park）");
    }
}

/// 验证 block_on_with_driver 保持驱动能力（driver capability）的立即工作答复（immediate-work response）：
/// 根异步计算（root Future）第一次挂起（Pending）并自通知（self-notify），驱动（driver）答复true后不允许停放（park）；
/// 下一次根就绪（root Ready）立即返回，不再次调用驱动（driver）。此例补足T166的true分支（branch），
/// 原五例的异常（panic）、跨线程通知（cross-thread notification）及false分支（branch）继续独立保留。
#[test]
fn test_driver_block_on_skips_park_for_immediate_work_and_stops_at_root_ready() {
    use pi_async_runtime_core::block_on::block_on_with_driver;
    let value = String::from("immediate driver root");
    let polls = Cell::new(0);
    let drops = Cell::new(0);
    let retained = Rc::new(RefCell::new(None));
    let mut driver = ImmediateWorkDriver { turns: 0 };
    assert!(!DriveScope::is_active());
    assert_eq!(block_on_with_driver(PinnedRoot {
        value: &value, polls: &polls, drops: &drops, address: Cell::new(std::ptr::null()),
        retained: retained.clone(), _pinned: PhantomPinned,
    }, &mut driver), "immediate driver root");
    assert_eq!(driver.turns, 1);
    assert_eq!(polls.get(), 2);
    assert_eq!(drops.get(), 1);
    assert!(!DriveScope::is_active());
    retained.borrow_mut().take().unwrap().wake();
    assert_eq!(drops.get(), 1);
}
