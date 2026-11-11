//! 经创建（creation）、唯一执行持有（Runnable）和结果句柄（JoinHandle）验证清理（cleanup）。

use pi_async_task::{create, Runnable, Schedule, TaskOptions};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

// 取消等待（cancellation wait）使用进程级等待图（wait graph），独立用例（test case）不得竞争其单次try_lock。
// 仅隔离测试之间的图操作；用例内部的真实线程（thread）与清理竞争保持原样。
static CANCEL_WAIT_TEST_ACCESS: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

/// 从未绑定的初始放弃（abandonment）不允许调用调度（Schedule）。
struct NoSchedule;

impl Schedule for NoSchedule {
    fn schedule(&self, _task: Runnable) {
        panic!("初始放弃（abandonment）不得调度（schedule）");
    }
}

/// 原异步计算（Future）保存独立的轮询（poll）和析构（drop）计数。
struct PendingProbe {
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl Future for PendingProbe {
    type Output = usize;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        Poll::Pending
    }
}

impl Drop for PendingProbe {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

/// @test T67/T104/T66/T119/T111/T123/T94/T112/T60；@api create/Runnable::drop。
/// 原异步计算（Future）必须在初始持有放弃时清理一次，不等待控制句柄（ControlHandle）。
/// 元数据（metadata）仍由原存储（storage）持有，最后一个合法句柄（handle）退出才释放。
#[test]
fn test_initial_send_abandonment_drops_future_before_control_handles() {
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let metadata = Arc::new(());
    let parts = create(
        PendingProbe { polls: Arc::clone(&polls), drops: Arc::clone(&drops) },
        NoSchedule,
        Arc::clone(&metadata),
        TaskOptions::plain(),
    ).unwrap_or_else(|_| panic!("合法创建（creation）应成功"));
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(parts.runnable);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(Arc::strong_count(&metadata), 2);
    drop(parts.join);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(parts.control);
    assert_eq!(Arc::strong_count(&metadata), 1);
}

/// @test T59/T86/T88/T114；@api JoinHandle::poll。
/// 初始放弃（abandonment）只产生一次可领取错误（error），不伪装取消（cancellation）。
#[test]
fn test_initial_abandonment_publishes_one_abandoned_result() {
    let mut parts = create(std::future::pending::<usize>(), NoSchedule, (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("合法创建（creation）应成功"));
    drop(parts.runnable);
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(matches!(Pin::new(&mut parts.join).poll(&mut cx), Poll::Ready(Err(pi_async_task::TaskError::Abandoned))));
    assert!(matches!(Pin::new(&mut parts.join).poll(&mut cx), Poll::Ready(Err(pi_async_task::TaskError::AlreadyTaken))));
}

/// 只记录公开完成通知（wake-up），不推进任务（Task）。
struct WakeCount(AtomicUsize);
impl std::task::Wake for WakeCount {
    fn wake(self: Arc<Self>) { self.0.fetch_add(1, Ordering::SeqCst); }
}

/// @test T86/T111；@api JoinHandle::poll/Runnable::drop。
/// 已登记结果等待者（result waiter）在原值清理确认（cleanup confirmation）后必须收到通知（wake-up）。
#[test]
fn test_abandonment_notifies_registered_result_waiter() {
    let mut parts = create(std::future::pending::<usize>(), NoSchedule, (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("合法创建（creation）应成功"));
    let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let waker = std::task::Waker::from(Arc::clone(&wakes));
    let mut cx = Context::from_waker(&waker);
    assert!(Pin::new(&mut parts.join).poll(&mut cx).is_pending());
    drop(parts.runnable);
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    assert!(matches!(Pin::new(&mut parts.join).poll(&mut cx), Poll::Ready(Err(pi_async_task::TaskError::Abandoned))));
}

/// 调度接口（Schedule）保存原唯一持有（Runnable），宿主通过公开结果继续退出清理。
struct Delivery(std::sync::mpsc::Sender<Runnable>);
impl Schedule for Delivery {
    fn schedule(&self, task: Runnable) { self.0.send(task).unwrap_or_else(|_| panic!("交付（delivery）通道已关闭")); }
}

/// @test T63/T66/T84/T113/T119；@api create_local/Runnable::drop/Runnable::run。
/// 远端放弃（remote abandonment）只交付同一身份（ID）的清理持有，不隐式接纳（admission）。
#[test]
fn test_remote_local_abandonment_delivers_cleanup_to_owner() {
    let owner = pi_async_task::TaskThread::new();
    let (tx, rx) = std::sync::mpsc::channel();
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let mut parts = pi_async_task::create_local(&owner,
        PendingProbe { polls: Arc::clone(&polls), drops: Arc::clone(&drops) }, Delivery(tx), (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("本地创建（local creation）应成功"));
    let id = parts.control.id();
    std::thread::spawn(move || drop(parts.runnable)).join().unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let cleanup = rx.try_recv().unwrap();
    assert_eq!(cleanup.id(), id);
    assert!(!cleanup.snapshot().admitted);
    assert_eq!(cleanup.snapshot().termination, Some(pi_async_task::definitions::TerminationKind::Abandoned));
    assert!(matches!(cleanup.run(&owner), Ok(pi_async_task::definitions::RunOutcome::Finished(pi_async_task::definitions::TerminationKind::Abandoned))));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(owner.live_tasks(), 0);
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(matches!(Pin::new(&mut parts.join).poll(&mut cx), Poll::Ready(Err(pi_async_task::TaskError::Abandoned))));
}

/// @test T63/T84/T110；@api Runnable::run/JoinHandle::poll。
/// 单次轮询（poll）正常就绪（Ready）后保留唯一结果（output），原异步计算（Future）先清理。
#[test]
fn test_direct_run_returns_ready_output_once() {
    let owner = pi_async_task::TaskThread::new();
    let mut parts = create(std::future::ready(String::from("done")), NoSchedule, (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("合法创建（creation）应成功"));
    assert!(matches!(parts.runnable.run(&owner), Ok(pi_async_task::definitions::RunOutcome::Finished(pi_async_task::definitions::TerminationKind::Completed))));
    let mut cx = Context::from_waker(std::task::Waker::noop());
    match Pin::new(&mut parts.join).poll(&mut cx) {
        Poll::Ready(Ok(value)) => assert_eq!(value, "done"),
        other => panic!("正常结果（output）丢失：{other:?}"),
    }
    assert_eq!(owner.live_tasks(), 0);
}

/// 原异步计算（Future）在原址析构（drop），就绪（Ready）输出（output）与其清理分别计数。
#[cfg(feature = "observe")]
struct ReadyProbe {
    drops: Arc<AtomicUsize>,
}
#[cfg(feature = "observe")]
impl Future for ReadyProbe {
    type Output = usize;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<usize> { Poll::Ready(7) }
}
#[cfg(feature = "observe")]
impl Drop for ReadyProbe {
    fn drop(&mut self) { self.drops.fetch_add(1, Ordering::SeqCst); }
}

/// 观测接收者（TaskObserver）只在原值已清理（FutureDropped）事件（event）向外展开（unwind）。
#[cfg(feature = "observe")]
struct PanicAfterCleanup;
#[cfg(feature = "observe")]
impl pi_async_task::task::TaskObserver for PanicAfterCleanup {
    fn event(&self, _id: pi_async_task::definitions::TaskId, event: pi_async_task::task::TaskEvent) {
        if event == pi_async_task::task::TaskEvent::FutureDropped { panic!("cleanup-observer"); }
    }
}

/// @test T110/T111/T122；@api Runnable::run/JoinHandle::poll。
/// 清理确认（cleanup confirmation）后的独立观测恐慌（observer panic）不能丢失已就绪（Ready）结果。
#[cfg(feature = "observe")]
#[test]
fn test_cleanup_observer_panic_preserves_ready_output() {
    let owner = pi_async_task::TaskThread::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let mut options = TaskOptions::plain();
    options.observer = Some(Arc::new(PanicAfterCleanup));
    let mut parts = create(ReadyProbe { drops: Arc::clone(&drops) }, NoSchedule, (), options)
        .unwrap_or_else(|_| panic!("合法创建（creation）应成功"));
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| { let _ = parts.runnable.run(&owner); }));
    assert_eq!(failure.unwrap_err().downcast_ref::<&str>(), Some(&"cleanup-observer"));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(matches!(Pin::new(&mut parts.join).poll(&mut cx), Poll::Ready(Ok(7))));
    owner.close();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

/// 首次轮询（poll）合并两次任务唤醒（wake-up），再次轮询（poll）返回完成就绪（Ready）。
struct SelfWake {
    polls: usize,
    drops: Arc<AtomicUsize>,
}
impl Future for SelfWake {
    type Output = usize;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<usize> {
        self.polls += 1;
        if self.polls == 1 {
            cx.waker().wake_by_ref();
            cx.waker().clone().wake();
            Poll::Pending
        } else { Poll::Ready(self.polls) }
    }
}
impl Drop for SelfWake {
    fn drop(&mut self) { self.drops.fetch_add(1, Ordering::SeqCst); }
}

/// @test T63/T81/T82/T84/T106/T107/T108/T109/T110；@api Runnable::run。
/// 一次执行（run）至多轮询（poll）一次，进行中任务唤醒（wake-up）只产生一份后续持有（Runnable）。
#[test]
fn test_pending_self_wakes_issue_one_followup_runnable() {
    let owner = pi_async_task::TaskThread::new();
    let (tx, rx) = std::sync::mpsc::channel();
    let drops = Arc::new(AtomicUsize::new(0));
    let mut parts = create(SelfWake { polls: 0, drops: Arc::clone(&drops) }, Delivery(tx), (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("合法创建（creation）应成功"));
    let id = parts.runnable.id();
    assert!(matches!(parts.runnable.run(&owner), Ok(pi_async_task::definitions::RunOutcome::Pending)));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(owner.live_tasks(), 1);
    assert_eq!(parts.control.snapshot().execution, pi_async_task::definitions::ExecutionStatus::Idle);
    let next = rx.try_recv().unwrap();
    assert_eq!(next.id(), id);
    assert!(rx.try_recv().is_err());
    assert!(matches!(next.run(&owner), Ok(pi_async_task::definitions::RunOutcome::Finished(pi_async_task::definitions::TerminationKind::Completed))));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(owner.live_tasks(), 0);
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(matches!(Pin::new(&mut parts.join).poll(&mut cx), Poll::Ready(Ok(2))));
}

/// 原异步计算（Future）的析构（drop）失败计数独立于轮询（poll）。
struct PanicDrop(Arc<AtomicUsize>);
impl Future for PanicDrop {
    type Output = usize;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<usize> { Poll::Ready(9) }
}
impl Drop for PanicDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("future-drop");
    }
}

/// @test T63/T84/T110/T111/T122/T123；@api Runnable::run/TaskThread::close。
/// 原值析构（Future::drop）展开（unwinding）保留正常原因，禁止后续关闭（close）再次清理。
#[test]
fn test_future_drop_panic_keeps_reason_and_forbids_repeated_cleanup() {
    let owner = pi_async_task::TaskThread::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let mut parts = create(PanicDrop(Arc::clone(&drops)), NoSchedule, (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("合法创建（creation）应成功"));
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| { let _ = parts.runnable.run(&owner); }));
    assert_eq!(failure.unwrap_err().downcast_ref::<&str>(), Some(&"future-drop"));
    let snapshot = parts.control.snapshot();
    assert_eq!(snapshot.execution, pi_async_task::definitions::ExecutionStatus::CleanupUnconfirmed);
    assert_eq!(snapshot.future_drop, pi_async_task::definitions::FutureDropStatus::Unconfirmed);
    assert_eq!(snapshot.termination, Some(pi_async_task::definitions::TerminationKind::Completed));
    owner.close();
    owner.close();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(matches!(Pin::new(&mut parts.join).poll(&mut cx), Poll::Ready(Err(pi_async_task::TaskError::CleanupUnconfirmed))));
}

/// @test T62/T83/T77；@api Runnable::admit/TaskRegistration::drop。
/// 显式接纳（admission）保留初始持有（Runnable），重复或错误所属域拒绝不改变原目标。
#[test]
fn test_admission_is_unique_and_registration_drop_requests_exit() {
    use pi_async_task::definitions::{AdmissionError, CancelRequestStatus, TerminationKind};
    let owner = pi_async_task::TaskThread::new();
    let other = pi_async_task::TaskThread::new();
    let (tx, rx) = std::sync::mpsc::channel();
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let parts = create(PendingProbe { polls: Arc::clone(&polls), drops: Arc::clone(&drops) }, Delivery(tx), (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("合法创建（creation）应成功"));
    let registration = parts.runnable.admit(&owner).unwrap_or_else(|_| panic!("首次接纳（admission）应成功"));
    assert!(parts.control.snapshot().admitted);
    assert_eq!(parts.control.snapshot().owner, Some(owner.id()));
    assert!(matches!(parts.runnable.admit(&owner), Err(AdmissionError::AlreadyAdmitted)));
    assert!(matches!(parts.runnable.admit(&other), Err(AdmissionError::WrongOwner)));
    assert_eq!(owner.live_tasks(), 1);
    drop(registration);
    assert_eq!(parts.control.snapshot().cancellation, CancelRequestStatus::Recorded);
    assert!(rx.try_recv().is_err());
    assert!(matches!(parts.runnable.run(&owner), Ok(pi_async_task::definitions::RunOutcome::Finished(TerminationKind::Cancelled))));
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

/// @test T45/T80/T119；@api ControlHandle::cancel/Runnable::drop。
/// 未绑定初始持有（Runnable）的取消记录（cancellation record）不覆盖放弃原因（Abandoned）。
#[test]
fn test_cancel_record_does_not_replace_initial_abandonment() {
    let mut parts = create(std::future::pending::<usize>(), NoSchedule, (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("合法创建（creation）应成功"));
    parts.control.cancel();
    parts.control.cancel();
    assert_eq!(parts.control.snapshot().cancellation, pi_async_task::definitions::CancelRequestStatus::Recorded);
    assert_eq!(parts.control.snapshot().termination, None);
    drop(parts.runnable);
    assert_eq!(parts.control.snapshot().termination, Some(pi_async_task::definitions::TerminationKind::Abandoned));
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(matches!(Pin::new(&mut parts.join).poll(&mut cx), Poll::Ready(Err(pi_async_task::TaskError::Abandoned))));
}

/// @test T47/T81/T106/T109；@api ControlHandle::waker。
/// 晚到唤醒器（Waker）只保留控制存储（storage），原值（Future）不随之延迟清理。
#[test]
fn test_late_native_waker_keeps_metadata_not_future() {
    let metadata = Arc::new(());
    let drops = Arc::new(AtomicUsize::new(0));
    let parts = create(PendingProbe { polls: Arc::new(AtomicUsize::new(0)), drops: Arc::clone(&drops) }, NoSchedule,
        Arc::clone(&metadata), TaskOptions::plain()).unwrap_or_else(|_| panic!("合法创建（creation）应成功"));
    let waker = parts.control.waker();
    let cloned = waker.clone();
    drop(parts);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(Arc::strong_count(&metadata), 2);
    cloned.wake_by_ref();
    drop(cloned);
    waker.wake();
    assert_eq!(Arc::strong_count(&metadata), 1);
}

/// @test T46/T82；@api ControlHandle::wake/Runnable::run。
/// 原持有（Runnable）已消费且挂起（Pending）时，重复共享唤醒（wake-up）只交付一份同任务持有。
#[test]
fn test_control_wake_coalesces_suspended_task() {
    let owner = pi_async_task::TaskThread::new();
    let (tx, rx) = std::sync::mpsc::channel();
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let mut parts = create(PendingProbe { polls: Arc::clone(&polls), drops: Arc::clone(&drops) }, Delivery(tx), (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("合法创建（creation）应成功"));
    let id = parts.runnable.id();
    assert!(matches!(parts.runnable.run(&owner), Ok(pi_async_task::definitions::RunOutcome::Pending)));
    assert!(rx.try_recv().is_err());
    parts.control.wake();
    parts.control.wake();
    let next = rx.try_recv().unwrap();
    assert_eq!(next.id(), id);
    assert!(rx.try_recv().is_err());
    parts.control.cancel();
    assert!(matches!(next.run(&owner), Ok(pi_async_task::definitions::RunOutcome::Finished(pi_async_task::definitions::TerminationKind::Cancelled))));
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(matches!(Pin::new(&mut parts.join).poll(&mut cx), Poll::Ready(Err(pi_async_task::TaskError::Cancelled))));
}

/// 观测（observation）克隆（clone）事件（event）失败不得遗失尚未交付的强持有（strong reference）。
#[cfg(feature = "observe")]
struct PanicWakerClone;
#[cfg(feature = "observe")]
impl pi_async_task::task::TaskObserver for PanicWakerClone {
    fn event(&self, _id: pi_async_task::definitions::TaskId, event: pi_async_task::task::TaskEvent) {
        if event == pi_async_task::task::TaskEvent::WakerClone { panic!("waker-clone-observer"); }
    }
}

/// @test T106/T93/T94；@api ControlHandle::waker。
/// 唤醒器克隆（Waker clone）观测恐慌（observer panic）后，未交付引用必须正常归还。
#[cfg(feature = "observe")]
#[test]
fn test_native_waker_clone_observer_panic_releases_prepared_reference() {
    let metadata = Arc::new(());
    let mut options = TaskOptions::plain();
    options.observer = Some(Arc::new(PanicWakerClone));
    let parts = create(std::future::pending::<usize>(), NoSchedule, Arc::clone(&metadata), options)
        .unwrap_or_else(|_| panic!("合法创建（creation）应成功"));
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| parts.control.waker()));
    let payload = match failure { Err(payload) => payload, Ok(_) => panic!("应传播独立观测恐慌（observer panic）") };
    assert_eq!(payload.downcast_ref::<&str>(), Some(&"waker-clone-observer"));
    drop(parts);
    assert_eq!(Arc::strong_count(&metadata), 1);
}

/// @test T63/T84/T110/T111/T119/T120/T121/T122/T123；@api create_local/Runnable::run/TaskThread::close。
/// 清理（cleanup）中的重入关闭（reentrant close）只留下关闭事实，不重复访问原值（Future）。
#[test]
fn test_future_drop_reentrant_close_cleans_other_registered_task() {
    struct CloseOnDrop {
        owner: std::rc::Rc<pi_async_task::TaskThread>,
        drops: Arc<AtomicUsize>,
    }
    impl Future for CloseOnDrop {
        type Output = usize;
        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<usize> { Poll::Ready(3) }
    }
    impl Drop for CloseOnDrop {
        fn drop(&mut self) { self.drops.fetch_add(1, Ordering::SeqCst); self.owner.close(); }
    }
    let owner = std::rc::Rc::new(pi_async_task::TaskThread::new());
    let drops = Arc::new(AtomicUsize::new(0));
    let other_drops = Arc::new(AtomicUsize::new(0));
    let first = pi_async_task::create_local(&owner, CloseOnDrop { owner: std::rc::Rc::clone(&owner), drops: Arc::clone(&drops) }, NoSchedule, (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("本地创建（local creation）应成功"));
    let other = pi_async_task::create_local(&owner, PendingProbe { polls: Arc::new(AtomicUsize::new(0)), drops: Arc::clone(&other_drops) }, NoSchedule, (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("本地创建（local creation）应成功"));
    assert!(matches!(first.runnable.run(&owner), Ok(pi_async_task::definitions::RunOutcome::Finished(pi_async_task::definitions::TerminationKind::Completed))));
    assert!(owner.is_closed());
    assert_eq!(owner.live_tasks(), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(other_drops.load(Ordering::SeqCst), 1);
    drop(other);
    owner.close();
    assert_eq!(other_drops.load(Ordering::SeqCst), 1);
}

/// @test T119/T113；@api Runnable::drop/TaskThread::close。
/// 清理持有（Runnable）再次释放不递归调度（Schedule），原线程（thread）关闭仍能实际清理。
#[test]
fn test_repeated_cleanup_ticket_drop_does_not_reschedule() {
    let owner = pi_async_task::TaskThread::new();
    let (tx, rx) = std::sync::mpsc::channel();
    let drops = Arc::new(AtomicUsize::new(0));
    let parts = pi_async_task::create_local(&owner, PendingProbe { polls: Arc::new(AtomicUsize::new(0)), drops: Arc::clone(&drops) }, Delivery(tx), (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("本地创建（local creation）应成功"));
    drop(parts.runnable);
    drop(rx.try_recv().unwrap());
    assert!(rx.try_recv().is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    owner.close();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(owner.live_tasks(), 0);
}

/// @test T48/T52/T53；@api ControlHandle::cancel_and_wait/CancelWait::poll。
/// 惰性取消等待（cancellation wait）在释放和未知等待者拒绝时均不发送取消请求（cancellation request）。
#[test]
fn test_lazy_cancel_wait_rejects_unknown_waiter_without_cancelling() {
    let parts = create(std::future::pending::<usize>(), NoSchedule, (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("合法创建（creation）应成功"));
    drop(parts.control.cancel_and_wait());
    assert_eq!(parts.control.snapshot().cancellation, pi_async_task::definitions::CancelRequestStatus::NotRecorded);
    let mut wait = parts.control.cancel_and_wait();
    let mut cx = Context::from_waker(std::task::Waker::noop());
    let expected = pi_async_task::definitions::CancellationOutcome::Failed(pi_async_task::definitions::CancellationError::UnknownWaiter);
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Ready(expected));
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Ready(expected));
    assert_eq!(parts.control.snapshot().cancellation, pi_async_task::definitions::CancelRequestStatus::NotRecorded);
}

/// 可信外部等待者（ExternalWaiter）在本测试存续期始终承接退出责任。
struct Alive;
impl pi_async_task::ExternalWaitSource for Alive {
    fn is_alive(&self) -> bool { true }
}

/// @test T49/T51/T52/T54/T55/T89/T90/T91/T111；@api CancelWait::poll/Runnable::run。
/// 取消等待（cancellation wait）只有在原值（Future）合法清理后完成，并通知已登记等待者。
#[test]
fn test_external_cancel_wait_completes_after_future_cleanup() {
    let _test_access = CANCEL_WAIT_TEST_ACCESS.lock();
    let owner = pi_async_task::TaskThread::new();
    let (tx, rx) = std::sync::mpsc::channel();
    let drops = Arc::new(AtomicUsize::new(0));
    let parts = create(PendingProbe { polls: Arc::new(AtomicUsize::new(0)), drops: Arc::clone(&drops) }, Delivery(tx), (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("合法创建（creation）应成功"));
    let external = pi_async_task::ExternalWaiter::new(Arc::new(Alive));
    let mut wait = parts.control.cancel_and_wait_from(&external);
    let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let waker = std::task::Waker::from(Arc::clone(&wakes));
    let mut cx = Context::from_waker(&waker);
    assert!(Pin::new(&mut wait).poll(&mut cx).is_pending());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(rx.try_recv().is_err());
    assert!(matches!(parts.runnable.run(&owner), Ok(pi_async_task::definitions::RunOutcome::Finished(pi_async_task::definitions::TerminationKind::Cancelled))));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(wakes.0.load(Ordering::SeqCst), 1);
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Ready(pi_async_task::definitions::CancellationOutcome::Cancelled));
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Ready(pi_async_task::definitions::CancellationOutcome::Cancelled));
}

/// @test T52/T53/T55/T89/T90；@api CancelWait::poll/Drop。
/// 同目标（target）只允许一份持续等待，释放旧等待后新等待可接续原请求（request）。
#[test]
fn test_cancel_wait_drop_releases_target_for_next_waiter() {
    let _test_access = CANCEL_WAIT_TEST_ACCESS.lock();
    let owner = pi_async_task::TaskThread::new();
    let parts = create(std::future::pending::<usize>(), NoSchedule, (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("合法创建（creation）应成功"));
    let external = pi_async_task::ExternalWaiter::new(Arc::new(Alive));
    let mut first = parts.control.cancel_and_wait_from(&external);
    let mut second = parts.control.cancel_and_wait_from(&external);
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(Pin::new(&mut first).poll(&mut cx).is_pending());
    assert_eq!(Pin::new(&mut second).poll(&mut cx), Poll::Ready(pi_async_task::definitions::CancellationOutcome::Failed(pi_async_task::definitions::CancellationError::AlreadyWaiting)));
    drop(first);
    let mut third = parts.control.cancel_and_wait_from(&external);
    assert!(Pin::new(&mut third).poll(&mut cx).is_pending());
    assert!(matches!(parts.runnable.run(&owner), Ok(pi_async_task::definitions::RunOutcome::Finished(pi_async_task::definitions::TerminationKind::Cancelled))));
    assert_eq!(Pin::new(&mut third).poll(&mut cx), Poll::Ready(pi_async_task::definitions::CancellationOutcome::Cancelled));
}

/// @test T52/T91/T123；@api CancelWait::poll/Runnable::run。
/// 原值清理（Future cleanup）展开（unwind）后再次推进等待只能报告未确认，不能永久保留旧边。
#[test]
fn test_cancel_wait_repoll_observes_cleanup_failure() {
    let _test_access = CANCEL_WAIT_TEST_ACCESS.lock();
    let owner = pi_async_task::TaskThread::new();
    let parts = create(PanicDrop(Arc::new(AtomicUsize::new(0))), NoSchedule, (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("合法创建（creation）应成功"));
    let external = pi_async_task::ExternalWaiter::new(Arc::new(Alive));
    let mut wait = parts.control.cancel_and_wait_from(&external);
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(Pin::new(&mut wait).poll(&mut cx).is_pending());
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| { let _ = parts.runnable.run(&owner); })).is_err());
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Ready(pi_async_task::definitions::CancellationOutcome::Failed(pi_async_task::definitions::CancellationError::CleanupUnconfirmed)));
}

/// @test T52/T99/T100/T101；@api CancelWait::poll/Runnable::run。
/// 真实当前任务（Task）不能通过外部凭据（ExternalWaiter）绕过自等待（self wait）拒绝。
#[test]
fn test_current_task_self_wait_is_rejected_without_request() {
    let owner = pi_async_task::TaskThread::new();
    let control = std::rc::Rc::new(std::cell::RefCell::new(None::<pi_async_task::ControlHandle>));
    let slot = std::rc::Rc::clone(&control);
    let external = pi_async_task::ExternalWaiter::new(Arc::new(Alive));
    let future = std::future::poll_fn(move |cx| {
        let target = slot.borrow().as_ref().unwrap().clone();
        let mut wait = target.cancel_and_wait_from(&external);
        let outcome = Pin::new(&mut wait).poll(cx);
        assert_eq!(target.snapshot().cancellation, pi_async_task::definitions::CancelRequestStatus::NotRecorded);
        match outcome { Poll::Ready(outcome) => Poll::Ready(outcome), Poll::Pending => panic!("自等待（self wait）必须立即拒绝") }
    });
    let mut parts = pi_async_task::create_local(&owner, future, NoSchedule, (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("本地创建（local creation）应成功"));
    *control.borrow_mut() = Some(parts.control.clone());
    assert!(parts.runnable.run(&owner).is_ok());
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(matches!(Pin::new(&mut parts.join).poll(&mut cx), Poll::Ready(Ok(pi_async_task::definitions::CancellationOutcome::Failed(pi_async_task::definitions::CancellationError::SelfWait)))));
    let mut outside = parts.control.cancel_and_wait();
    assert_eq!(Pin::new(&mut outside).poll(&mut cx), Poll::Ready(pi_async_task::definitions::CancellationOutcome::Failed(pi_async_task::definitions::CancellationError::UnknownWaiter)));
}

/// @test T52/T54/T55/T99/T100/T101；@api Runnable::run/CancelWait::poll。
/// 嵌套同步轮询（nested poll）先建立B到A的等待，恢复A身份后拒绝闭合A到B的循环。
#[test]
fn test_cancel_wait_cycle_is_rejected_after_nested_run() {
    let _test_access = CANCEL_WAIT_TEST_ACCESS.lock();
    let owner = std::rc::Rc::new(pi_async_task::TaskThread::new());
    let a_control = std::rc::Rc::new(std::cell::RefCell::new(None::<pi_async_task::ControlHandle>));
    let b_control = std::rc::Rc::new(std::cell::RefCell::new(None::<pi_async_task::ControlHandle>));
    let b_runnable = std::rc::Rc::new(std::cell::RefCell::new(None::<Runnable>));
    let a_slot = std::rc::Rc::clone(&a_control);
    let mut b_wait = None;
    let (tx, rx) = std::sync::mpsc::channel();
    let b_schedule = Delivery(tx.clone());
    let b_future = std::future::poll_fn(move |cx| {
        let wait = b_wait.get_or_insert_with(|| a_slot.borrow().as_ref().unwrap().cancel_and_wait());
        Pin::new(wait).poll(cx)
    });
    let b = pi_async_task::create_local(&owner, b_future, b_schedule, (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("本地创建（local creation）应成功"));
    *b_control.borrow_mut() = Some(b.control.clone());
    *b_runnable.borrow_mut() = Some(b.runnable);
    let b_ticket = std::rc::Rc::clone(&b_runnable);
    let b_target = std::rc::Rc::clone(&b_control);
    let nested_owner = std::rc::Rc::clone(&owner);
    let a_future = std::future::poll_fn(move |cx| {
        let b = b_ticket.borrow_mut().take().unwrap();
        assert!(matches!(b.run(&nested_owner), Ok(pi_async_task::definitions::RunOutcome::Pending)));
        let target = b_target.borrow().as_ref().unwrap().clone();
        let mut wait = target.cancel_and_wait();
        let outcome = Pin::new(&mut wait).poll(cx);
        assert_eq!(target.snapshot().cancellation, pi_async_task::definitions::CancelRequestStatus::NotRecorded);
        outcome
    });
    let mut a = pi_async_task::create_local(&owner, a_future, Delivery(tx), (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("本地创建（local creation）应成功"));
    *a_control.borrow_mut() = Some(a.control.clone());
    assert!(a.runnable.run(&owner).is_ok());
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(matches!(Pin::new(&mut a.join).poll(&mut cx), Poll::Ready(Ok(pi_async_task::definitions::CancellationOutcome::Failed(pi_async_task::definitions::CancellationError::WouldCycle)))));
    owner.close();
    while let Ok(ticket) = rx.try_recv() { drop(ticket); }
}

/// @test T63/T84/T110/T119/T122；@api Runnable::run/TaskThread::close。
/// 后继持有（Runnable）已由调度接口（Schedule）接管后展开（unwind），原持有必须只归还一次。
#[test]
fn test_schedule_panic_after_acceptance_keeps_followup_cleanup() {
    struct StoreThenPanic(std::sync::mpsc::Sender<Runnable>);
    impl Schedule for StoreThenPanic {
        fn schedule(&self, task: Runnable) { self.0.send(task).unwrap_or_else(|_| panic!("通道（channel）关闭")); panic!("schedule-after-store"); }
    }
    let owner = pi_async_task::TaskThread::new();
    let (tx, rx) = std::sync::mpsc::channel();
    let drops = Arc::new(AtomicUsize::new(0));
    let metadata = Arc::new(());
    let parts = create(SelfWake { polls: 0, drops: Arc::clone(&drops) }, StoreThenPanic(tx), Arc::clone(&metadata), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("合法创建（creation）应成功"));
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| { let _ = parts.runnable.run(&owner); }));
    assert_eq!(failure.unwrap_err().downcast_ref::<&str>(), Some(&"schedule-after-store"));
    assert_eq!(parts.control.snapshot().termination, Some(pi_async_task::definitions::TerminationKind::Cancelled));
    let next = rx.try_recv().unwrap();
    assert!(matches!(next.run(&owner), Ok(pi_async_task::definitions::RunOutcome::Finished(pi_async_task::definitions::TerminationKind::Cancelled))));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(parts.join);
    drop(parts.control);
    assert_eq!(Arc::strong_count(&metadata), 1);
}

/// @test T59/T60/T87/T88/T110/T114；@api JoinHandle::drop/Runnable::run。
/// 结果兴趣（result interest）在正常就绪（Ready）前放弃，完成者仍必须合法清理非Send输出。
#[test]
fn test_abandoned_local_result_is_dropped_by_completion() {
    let owner = pi_async_task::TaskThread::new();
    let output = std::rc::Rc::new(());
    let future_output = std::rc::Rc::clone(&output);
    let parts = pi_async_task::create_local(&owner, async move { future_output }, NoSchedule, (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("本地创建（local creation）应成功"));
    drop(parts.join);
    assert!(parts.runnable.run(&owner).is_ok());
    assert_eq!(std::rc::Rc::strong_count(&output), 1);
}

/// @test T52/T89/T90；@api CancelWait::poll/Drop。
/// 等待（wait）与原线程关闭（close）交错时，旧通知不会清除后继等待者（waiter）。
#[test]
fn test_cancel_wait_registration_races_owner_cleanup() {
    let _test_access = CANCEL_WAIT_TEST_ACCESS.lock();
    let owner = pi_async_task::TaskThread::new();
    let (tx, rx) = std::sync::mpsc::channel();
    let parts = create(std::future::pending::<usize>(), Delivery(tx), (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("合法创建（creation）应成功"));
    assert!(matches!(parts.runnable.run(&owner), Ok(pi_async_task::definitions::RunOutcome::Pending)));
    let control = parts.control.clone();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let other = Arc::clone(&barrier);
    let waiting = std::thread::spawn(move || {
        let external = pi_async_task::ExternalWaiter::new(Arc::new(Alive));
        let mut wait = control.cancel_and_wait_from(&external);
        let mut cx = Context::from_waker(std::task::Waker::noop());
        other.wait();
        let _ = Pin::new(&mut wait).poll(&mut cx);
        (wait, external)
    });
    barrier.wait();
    owner.close();
    let (mut wait, external) = waiting.join().unwrap();
    let mut cx = Context::from_waker(std::task::Waker::noop());
    match Pin::new(&mut wait).poll(&mut cx) {
        Poll::Ready(pi_async_task::definitions::CancellationOutcome::Cancelled) => {},
        outcome => panic!("合法清理（cleanup）后等待结果（outcome）错误：{outcome:?}"),
    }
    drop(wait);
    let mut followup = parts.control.cancel_and_wait_from(&external);
    assert_eq!(Pin::new(&mut followup).poll(&mut cx), Poll::Ready(pi_async_task::definitions::CancellationOutcome::Cancelled));
    while let Ok(ticket) = rx.try_recv() { drop(ticket); }
}

/// @test T110；@api Runnable::run/JoinHandle::poll。
/// 只捕获直接轮询（poll）的恐慌（panic），保持原载荷（payload）供唯一结果领取。
#[test]
fn test_poll_panic_payload_is_taken_once() {
    let owner = pi_async_task::TaskThread::new();
    let future = std::future::poll_fn(|_| -> Poll<usize> { std::panic::panic_any(71_u32); });
    let mut parts = create(future, NoSchedule, (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("合法创建（creation）应成功"));
    assert!(matches!(parts.runnable.run(&owner), Ok(pi_async_task::definitions::RunOutcome::Finished(pi_async_task::definitions::TerminationKind::Panicked))));
    let mut cx = Context::from_waker(std::task::Waker::noop());
    match Pin::new(&mut parts.join).poll(&mut cx) {
        Poll::Ready(Err(pi_async_task::TaskError::Panicked(record))) => {
            assert_eq!(record.payload.downcast_ref::<u32>(), Some(&71));
            assert!(record.backtraces.is_empty());
        }
        outcome => panic!("恐慌载荷（panic payload）未保留：{outcome:?}"),
    }
}

/// @test T112；@api ControlHandle::drop。
/// 回收开始观测（ReclaimStarted observation）展开（unwind）仍归还元数据（metadata），不留下手工析构（ManuallyDrop）资源。
#[cfg(feature = "observe")]
#[test]
fn test_reclaim_observer_panic_still_releases_metadata() {
    struct PanicReclaim;
    impl pi_async_task::task::TaskObserver for PanicReclaim {
        fn event(&self, _: pi_async_task::definitions::TaskId, event: pi_async_task::task::TaskEvent) {
            if event == pi_async_task::task::TaskEvent::ReclaimStarted { panic!("reclaim-observer"); }
        }
    }
    let metadata = Arc::new(());
    let mut options = TaskOptions::plain();
    options.observer = Some(Arc::new(PanicReclaim));
    let parts = create(std::future::pending::<()>(), NoSchedule, metadata.clone(), options).unwrap_or_else(|_| panic!("创建（creation）失败"));
    drop(parts.runnable);
    drop(parts.join);
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(parts.control))).unwrap_err();
    assert_eq!(failure.downcast_ref::<&str>(), Some(&"reclaim-observer"));
    assert_eq!(Arc::strong_count(&metadata), 1);
}

/// @test T59/T60/T75/T76/T94/T110/T114；@api TaskThread::drop/JoinHandle::poll。
/// 已发布的本地输出（local output）由唯一结果句柄（JoinHandle）保留；所属域退出不让远程末引用（last reference）清理它。
#[test]
fn test_local_ready_output_survives_owner_until_join_releases_it() {
    let output = std::rc::Rc::new(());
    let owner = pi_async_task::TaskThread::new();
    let value = output.clone();
    let mut parts = pi_async_task::create_local(&owner, async move { value }, NoSchedule, (), TaskOptions::plain()).unwrap_or_else(|_| panic!("创建（creation）失败"));
    let waker = parts.control.waker();
    assert!(parts.runnable.run(&owner).is_ok());
    assert_eq!(std::rc::Rc::strong_count(&output), 2);
    drop(owner);
    std::thread::spawn(move || { waker.wake(); drop(parts.control); }).join().unwrap();
    assert_eq!(std::rc::Rc::strong_count(&output), 2);
    let mut cx = Context::from_waker(std::task::Waker::noop());
    let received = match Pin::new(&mut parts.join).poll(&mut cx) { Poll::Ready(Ok(value)) => value, _ => panic!("结果（output）丢失") };
    assert!(std::rc::Rc::ptr_eq(&received, &output));
    drop(received);
    drop(parts.join);
    assert_eq!(std::rc::Rc::strong_count(&output), 1);
}

/// @test T52/T53/T55；@api CancelWait::poll/Drop。
/// 持续等待（wait）开始后可信外部身份（external identity）失效，立即拒绝并释放本份目标登记（registration）。
#[test]
fn test_cancel_wait_identity_change_releases_registration() {
    let _test_access = CANCEL_WAIT_TEST_ACCESS.lock();
    struct Switch(Arc<std::sync::atomic::AtomicBool>);
    impl pi_async_task::ExternalWaitSource for Switch {
        fn is_alive(&self) -> bool { self.0.load(Ordering::SeqCst) }
    }
    use pi_async_task::definitions::{CancellationError, CancellationOutcome};
    let owner = pi_async_task::TaskThread::new();
    let parts = create(std::future::pending::<()>(), NoSchedule, (), TaskOptions::plain()).unwrap_or_else(|_| panic!("创建（creation）失败"));
    let alive = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let external = pi_async_task::ExternalWaiter::new(Arc::new(Switch(alive.clone())));
    let mut wait = parts.control.cancel_and_wait_from(&external);
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(Pin::new(&mut wait).poll(&mut cx).is_pending());
    alive.store(false, Ordering::SeqCst);
    assert_eq!(Pin::new(&mut wait).poll(&mut cx), Poll::Ready(CancellationOutcome::Failed(CancellationError::WaiterChanged)));
    let valid = pi_async_task::ExternalWaiter::new(Arc::new(Alive));
    let mut next = parts.control.cancel_and_wait_from(&valid);
    assert!(Pin::new(&mut next).poll(&mut cx).is_pending());
    assert!(parts.runnable.run(&owner).is_ok());
    assert_eq!(Pin::new(&mut next).poll(&mut cx), Poll::Ready(CancellationOutcome::Cancelled));
}

/// @test T84/T110；@api Runnable::run。
/// 已开始轮询（poll）的Ready胜过同次取消（cancellation），Pending才落实停止后继轮询（poll）。
#[test]
fn test_started_ready_wins_cancellation_but_pending_stops() {
    use pi_async_task::definitions::{RunOutcome, TerminationKind};
    for ready in [true, false] {
        let owner = pi_async_task::TaskThread::new();
        let control = std::rc::Rc::new(std::cell::RefCell::new(None::<pi_async_task::ControlHandle>));
        let current = control.clone();
        let future = std::future::poll_fn(move |_| {
            current.borrow().as_ref().unwrap().cancel();
            if ready { Poll::Ready(41_u32) } else { Poll::Pending }
        });
        let mut parts = pi_async_task::create_local(&owner, future, NoSchedule, (), TaskOptions::plain()).unwrap_or_else(|_| panic!("创建（creation）失败"));
        *control.borrow_mut() = Some(parts.control.clone());
        let reason = if ready { TerminationKind::Completed } else { TerminationKind::Cancelled };
        assert!(matches!(parts.runnable.run(&owner), Ok(RunOutcome::Finished(actual)) if actual == reason));
        let mut cx = Context::from_waker(std::task::Waker::noop());
        let result = Pin::new(&mut parts.join).poll(&mut cx);
        if ready { assert!(matches!(result, Poll::Ready(Ok(41)))); }
        else { assert!(matches!(result, Poll::Ready(Err(pi_async_task::TaskError::Cancelled)))); }
    }
}

/// @test T63/T84；@api Runnable::run。
/// 错误所属域（wrong owner）拒绝须返还原执行持有（Runnable），原线程（thread）仍可合法推进及清理。
#[test]
fn test_wrong_owner_run_returns_original_runnable_for_retry() {
    use pi_async_task::definitions::{RunErrorKind, RunOutcome};
    let owner = pi_async_task::TaskThread::new();
    let other = pi_async_task::TaskThread::new();
    let (tx, rx) = std::sync::mpsc::channel();
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let parts = pi_async_task::create_local(&owner,
        PendingProbe { polls: polls.clone(), drops: drops.clone() }, Delivery(tx), (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("创建（creation）失败"));
    let id = parts.runnable.id();
    let before = parts.control.snapshot();
    let error = match parts.runnable.run(&other) { Err(error) => error, Ok(_) => panic!("错误所属域（wrong owner）不得执行") };
    assert_eq!(error.cause, RunErrorKind::WrongOwner);
    assert_eq!(error.runnable.id(), id);
    assert_eq!(parts.control.snapshot(), before);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(owner.live_tasks(), 1);
    assert_eq!(other.live_tasks(), 0);
    assert!(matches!(error.runnable.run(&owner), Ok(RunOutcome::Pending)));
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    owner.close();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(owner.live_tasks(), 0);
    assert!(rx.try_recv().is_err());
}

/// @test T63/T84/T119；@api Runnable::run/Drop。
/// 关闭所属域（closed owner）拒绝尚未绑定的原持有（Runnable），后续初始放弃仍为Abandoned。
#[test]
fn test_closed_owner_run_returns_unbound_runnable_without_cancelling() {
    use pi_async_task::definitions::RunErrorKind;
    let owner = pi_async_task::TaskThread::new();
    owner.close();
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let mut parts = create(PendingProbe { polls: polls.clone(), drops: drops.clone() },
        NoSchedule, (), TaskOptions::plain()).unwrap_or_else(|_| panic!("创建（creation）失败"));
    let id = parts.runnable.id();
    let before = parts.control.snapshot();
    let error = match parts.runnable.run(&owner) { Err(error) => error, Ok(_) => panic!("关闭所属域（closed owner）不得接管") };
    assert_eq!(error.cause, RunErrorKind::OwnerClosed);
    assert_eq!(error.runnable.id(), id);
    assert_eq!(parts.control.snapshot(), before);
    assert_eq!(owner.live_tasks(), 0);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(error.runnable);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(matches!(Pin::new(&mut parts.join).poll(&mut cx), Poll::Ready(Err(pi_async_task::TaskError::Abandoned))));
}

/// @test T43/T44/T50/T56/T57/T58/T61/T64/T65/T78/T79/T85/T115。
/// @api create/Runnable/ControlHandle/JoinHandle；创建（creation）只建立同一固定身份（ID）。
#[test]
fn test_created_handles_share_identity_snapshot_and_metadata() {
    let parts = create(std::future::pending::<usize>(), NoSchedule, String::from("metadata"), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("合法创建（creation）应成功"));
    let id = parts.runnable.id();
    assert_eq!(parts.control.id(), id);
    assert_eq!(parts.join.id(), id);
    let control = parts.control.clone();
    assert_eq!(control.id(), id);
    let snapshot = parts.runnable.snapshot();
    assert_eq!(snapshot, control.snapshot());
    assert_eq!(snapshot, parts.join.snapshot());
    assert_eq!(snapshot.phase, pi_async_task::definitions::TaskPhase::Preparing);
    assert_eq!(snapshot.owner, None);
    assert!(!snapshot.admitted);
    assert_eq!(parts.runnable.metadata::<String>().map(String::as_str), Some("metadata"));
    assert!(parts.runnable.metadata::<usize>().is_none());
    assert_eq!(parts.join.metadata(), "metadata");
}

/// @test T68/T105/T75/T76/T120/T121；@api create_local/TaskThread::close。
/// 绑定但未接纳（admission）的本地原值（local Future）仍由原线程（thread）关闭清理。
#[test]
fn test_owner_close_cleans_unadmitted_local_future() {
    let owner = pi_async_task::TaskThread::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let polls = Arc::new(AtomicUsize::new(0));
    let local = std::rc::Rc::new(());
    let future = {
        let local = std::rc::Rc::clone(&local);
        let probe = PendingProbe { polls: Arc::clone(&polls), drops: Arc::clone(&drops) };
        async move { let _ = local; probe.await }
    };
    let parts = pi_async_task::create_local(&owner, future, NoSchedule, (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("本地创建（local creation）应成功"));
    assert_eq!(owner.live_tasks(), 1);
    assert!(!parts.control.snapshot().admitted);
    owner.close();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(owner.live_tasks(), 0);
    assert!(owner.is_closed());
    owner.close();
    drop(parts);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}
