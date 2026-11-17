//! 通过公开接口（public API）验证等待任务唤醒（wake-up）的首个合法场景。
//!
//! 创建（creation）、直接执行（run）、所属域关闭（owner close）及结果领取（result retrieval）
//! 均使用真实入口；默认选项（default options）不启用额外观测（observation）。

use pi_async_task::definitions::{
    ExecutionStatus, FutureDropStatus, RunOutcome, TaskPhase, TerminationKind,
};
use pi_async_task::{create, Runnable, Schedule, TaskError, TaskOptions, TaskThread};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

/// 本场景没有任务唤醒（wake-up）；意外调度（scheduling）必须显式失败。
struct NoSchedule;

impl Schedule for NoSchedule {
    fn schedule(&self, _task: Runnable) {
        panic!("没有任务唤醒（wake-up）时不得调度（schedule）");
    }
}

/// 原异步计算（Future）只返回挂起（Pending），分别记录轮询（poll）及析构（drop）。
/// 两个计数仅观察业务原值，不访问任务（Task）的私有状态（private state）。
struct PendingProbe {
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl Future for PendingProbe {
    type Output = ();

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

/// @test test_direct_pending_task_waits_for_wake_without_admission。
/// @api ControlHandle::is_waiting_for_wake；@impl ref_waiting_for_wake_inner。
/// @design-doc crates/task/src/task.rs 中同一查询的冻结壳（frozen shell）及原局部说明。
/// 未开始与稳定挂起（Pending）的公开执行描述（ExecutionStatus）均可为Idle，
/// 但只有后者等待任务唤醒（wake-up）；显式接纳（admission）不是这一判定的前提。
/// 单线程（single-threaded）顺序调用，无并发更新（concurrent update）、真实计时或外部服务。
/// 被动依赖（passive dependencies）为create、Runnable::run、TaskThread::close及JoinHandle::poll；
/// 它们仅建立合法场景并闭合释放（release），本用例不代表这些入口的完整验收（acceptance）。
#[test]
fn test_direct_pending_task_waits_for_wake_without_admission() {
    let owner = TaskThread::new();
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let metadata = Arc::new(());
    let mut parts = create(
        PendingProbe {
            polls: Arc::clone(&polls),
            drops: Arc::clone(&drops),
        },
        NoSchedule,
        Arc::clone(&metadata),
        TaskOptions::plain(),
    )
    .unwrap_or_else(|_| panic!("合法创建（creation）应成功"));

    let created = parts.control.snapshot();
    assert_eq!(created.phase, TaskPhase::Preparing);
    assert_eq!(created.execution, ExecutionStatus::Idle);
    assert_eq!(created.owner, None);
    assert!(!created.admitted);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(
        !parts.control.is_waiting_for_wake(),
        "尚未轮询（poll）的任务（Task）没有等待任务唤醒（wake-up）"
    );

    // 直接执行（run）合法绑定所属域，未调用接纳（admission）入口。
    assert!(matches!(parts.runnable.run(&owner), Ok(RunOutcome::Pending)));
    let suspended = parts.control.snapshot();
    assert_eq!(suspended.execution, ExecutionStatus::Idle);
    assert_eq!(suspended.owner, Some(owner.id()));
    assert!(!suspended.admitted);
    assert_eq!(owner.live_tasks(), 1);
    assert!(
        parts.control.is_waiting_for_wake(),
        "稳定挂起（Pending）且未接纳（admission）时仍须等待任务唤醒（wake-up）"
    );
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 0);

    // 真实关闭（close）清理原值，不由测试辅助代码（test helper）代补清理（cleanup）。
    owner.close();
    assert!(owner.is_closed());
    assert_eq!(owner.live_tasks(), 0);
    let closed = parts.control.snapshot();
    assert_eq!(closed.future_drop, FutureDropStatus::Confirmed);
    assert_eq!(closed.termination, Some(TerminationKind::Cancelled));
    assert!(!parts.control.is_waiting_for_wake());
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(
        Pin::new(&mut parts.join).poll(&mut cx),
        Poll::Ready(Err(TaskError::Cancelled))
    ));

    drop(parts.join);
    drop(parts.control);
    drop(owner);
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(Arc::strong_count(&metadata), 1);
}

/// 原查询（waiting-for-wake query）：轮询（poll）内、唤醒（wake-up）交付前、已交付及完成时为false。
/// 接纳登记（TaskRegistration）和共享控制句柄（ControlHandle）在受控静止点须分类一致；
/// 两次挂起（Pending）都由原生产交接（production handoff）完成，调度（Schedule）只保管真实原生节点（native node）。
#[test]
fn test_waiting_query_tracks_running_handoff_and_registered_delivery() {
    let owner = TaskThread::new();
    let delivery = Arc::new(cordyceps::MpscQueue::<pi_async_task::MpscQueueEntry>::new_with_stub(
        pi_async_task::MpscEntryHandle::try_new_empty().unwrap(),
    ));
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let deliveries = Arc::new(AtomicUsize::new(0));
    let mut created = None;
    let control = Arc::new_cyclic(|control| {
        let parts = create(
            WaitingQueryFuture { control: control.clone(), polls: Arc::clone(&polls), drops: Arc::clone(&drops) },
            WaitingQuerySchedule { control: control.clone(), delivery: Arc::clone(&delivery), deliveries: Arc::clone(&deliveries) },
            (),
            TaskOptions::intrusive_mpsc(),
        ).unwrap_or_else(|_| panic!("合法创建（creation）应成功"));
        created = Some((parts.runnable, parts.join));
        parts.control
    });
    let (runnable, mut join) = created.unwrap();
    let registration = runnable.admit(&owner).unwrap();
    assert!(!control.is_waiting_for_wake());
    assert!(!registration.is_waiting_for_wake());

    assert!(matches!(runnable.run(&owner), Ok(RunOutcome::Pending)));
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(deliveries.load(Ordering::SeqCst), 1);
    assert!(!control.is_waiting_for_wake());
    assert!(!registration.is_waiting_for_wake());
    let next = delivery.try_dequeue().unwrap().try_into_runnable()
        .unwrap_or_else(|_| panic!("须取回原执行持有（runnable handle）"));
    assert_eq!(next.id(), control.id());
    assert!(matches!(next.run(&owner), Ok(RunOutcome::Pending)));
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    assert!(control.is_waiting_for_wake());
    assert!(registration.is_waiting_for_wake());

    control.wake();
    assert_eq!(deliveries.load(Ordering::SeqCst), 2);
    assert!(!control.is_waiting_for_wake());
    assert!(!registration.is_waiting_for_wake());
    let next = delivery.try_dequeue().unwrap().try_into_runnable()
        .unwrap_or_else(|_| panic!("须取回原执行持有（runnable handle）"));
    assert!(matches!(next.run(&owner), Ok(RunOutcome::Finished(TerminationKind::Completed))));
    assert!(!control.is_waiting_for_wake());
    assert!(!registration.is_waiting_for_wake());
    assert_eq!(polls.load(Ordering::SeqCst), 3);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(owner.live_tasks(), 0);
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(Pin::new(&mut join).poll(&mut cx), Poll::Ready(Ok(()))));
    assert!(matches!(delivery.try_dequeue(), Err(cordyceps::mpsc_queue::TryDequeueError::Empty)));
}

/// 原查询（waiting-for-wake query）：取消请求（cancellation request）已记录而尚未清理时不得返回true。
/// 共享控制句柄（ControlHandle）与接纳登记（TaskRegistration）观察同一静止事实，
/// 原生产执行（production run）只清理、不再轮询（poll）业务异步计算（Future）。
#[test]
fn test_cancelled_pending_task_is_not_waiting_before_cleanup() {
    let owner = TaskThread::new();
    let delivery = Arc::new(cordyceps::MpscQueue::<pi_async_task::MpscQueueEntry>::new_with_stub(
        pi_async_task::MpscEntryHandle::try_new_empty().unwrap(),
    ));
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let deliveries = Arc::new(AtomicUsize::new(0));
    let mut created = None;
    let control = Arc::new_cyclic(|control| {
        let parts = create(
            PendingProbe { polls: Arc::clone(&polls), drops: Arc::clone(&drops) },
            WaitingQuerySchedule { control: control.clone(), delivery: Arc::clone(&delivery), deliveries: Arc::clone(&deliveries) },
            (),
            TaskOptions::intrusive_mpsc(),
        ).unwrap_or_else(|_| panic!("合法创建（creation）应成功"));
        created = Some((parts.runnable, parts.join));
        parts.control
    });
    let (runnable, mut join) = created.unwrap();
    let registration = runnable.admit(&owner).unwrap();
    assert!(matches!(runnable.run(&owner), Ok(RunOutcome::Pending)));
    assert!(control.is_waiting_for_wake());
    assert!(registration.is_waiting_for_wake());
    control.cancel();
    assert!(!control.is_waiting_for_wake());
    assert!(!registration.is_waiting_for_wake());
    assert_eq!(control.snapshot().cancellation, pi_async_task::definitions::CancelRequestStatus::Recorded);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(deliveries.load(Ordering::SeqCst), 1);
    let cleanup = delivery.try_dequeue().unwrap().try_into_runnable()
        .unwrap_or_else(|_| panic!("须取回原清理持有（cleanup handle）"));
    assert!(matches!(cleanup.run(&owner), Ok(RunOutcome::Finished(TerminationKind::Cancelled))));
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(owner.live_tasks(), 0);
    assert!(!control.is_waiting_for_wake());
    assert!(!registration.is_waiting_for_wake());
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(Pin::new(&mut join).poll(&mut cx), Poll::Ready(Err(TaskError::Cancelled))));
    assert!(matches!(delivery.try_dequeue(), Err(cordyceps::mpsc_queue::TryDequeueError::Empty)));
}

/// 原查询（waiting-for-wake query）：清理（cleanup）恐慌（panic）不能被误判为等待唤醒（waiting for wake-up）。
/// 通过原业务析构（Drop）产生合法异常（exception）；不重试原值清理（cleanup），不把未确认当成功。
#[test]
fn test_cleanup_unconfirmed_task_is_never_waiting_for_wake() {
    let owner = TaskThread::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let metadata = Arc::new(());
    let mut created = None;
    let control = Arc::new_cyclic(|control| {
        let parts = create(
            QueryDuringFailingDrop { control: control.clone(), drops: Arc::clone(&drops) },
            NoSchedule,
            Arc::clone(&metadata),
            TaskOptions::plain(),
        ).unwrap_or_else(|_| panic!("合法创建（creation）应成功"));
        created = Some((parts.runnable, parts.join));
        parts.control
    });
    let (runnable, mut join) = created.unwrap();
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| runnable.run(&owner)))
        .err().expect("必须传播原析构恐慌（drop panic）");
    assert_eq!(failure.downcast_ref::<usize>(), Some(&617));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(control.snapshot().execution, ExecutionStatus::CleanupUnconfirmed);
    assert_eq!(control.snapshot().future_drop, FutureDropStatus::Unconfirmed);
    assert!(!control.is_waiting_for_wake());
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(Pin::new(&mut join).poll(&mut cx), Poll::Ready(Err(TaskError::CleanupUnconfirmed))));
    owner.close();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(!control.is_waiting_for_wake());
    drop(join);
    drop(owner);
    drop(control);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(Arc::strong_count(&metadata), 1);
}

/// 原查询（waiting-for-wake query）：既有观测入口（observation API）精确观察挂起交接（Pending handoff）。
/// 仅observe配置（configuration）执行；回调（callback）只读公开状态（public state），不阻塞、不重入原值或代补交接（handoff）。
#[cfg(feature = "observe")]
#[test]
fn test_pending_handoff_is_not_yet_waiting_for_wake() {
    let owner = TaskThread::new();
    let observer = Arc::new(HandoffQueryObserver {
        control: std::sync::OnceLock::new(),
        handoffs: AtomicUsize::new(0),
    });
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let mut created = None;
    let control = Arc::new_cyclic(|control| {
        observer.control.set(control.clone()).unwrap();
        let mut options = TaskOptions::plain();
        options.observer = Some(observer.clone());
        let parts = create(
            PendingProbe { polls: Arc::clone(&polls), drops: Arc::clone(&drops) },
            NoSchedule,
            (),
            options,
        ).unwrap_or_else(|_| panic!("合法创建（creation）应成功"));
        created = Some((parts.runnable, parts.join));
        parts.control
    });
    let (runnable, mut join) = created.unwrap();
    assert!(matches!(runnable.run(&owner), Ok(RunOutcome::Pending)));
    assert_eq!(observer.handoffs.load(Ordering::SeqCst), 1);
    assert!(control.is_waiting_for_wake());
    owner.close();
    assert!(!control.is_waiting_for_wake());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(owner.live_tasks(), 0);
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(Pin::new(&mut join).poll(&mut cx), Poll::Ready(Err(TaskError::Cancelled))));
}

/// 只读取原生产观测点（production observation point）；弱持有（Weak）不延长任务存储（task storage）存续。
#[cfg(feature = "observe")]
struct HandoffQueryObserver {
    control: std::sync::OnceLock<std::sync::Weak<pi_async_task::ControlHandle>>,
    handoffs: AtomicUsize,
}
#[cfg(feature = "observe")]
impl pi_async_task::task::TaskObserver for HandoffQueryObserver {
    fn event(&self, task: pi_async_task::definitions::TaskId, event: pi_async_task::task::TaskEvent) {
        if event == pi_async_task::task::TaskEvent::PollPending {
            let control = self.control.get().unwrap().upgrade().unwrap();
            assert_eq!(control.id(), task);
            assert_eq!(control.snapshot().execution, ExecutionStatus::PendingHandoff);
            assert!(!control.is_waiting_for_wake());
            self.handoffs.fetch_add(1, Ordering::SeqCst);
        }
    }
}

/// 原业务异步计算（Future）就绪（Ready）后，在自身析构（drop）中观察公开查询并抛出原始载荷（payload）。
struct QueryDuringFailingDrop {
    control: std::sync::Weak<pi_async_task::ControlHandle>,
    drops: Arc<AtomicUsize>,
}
impl Future for QueryDuringFailingDrop {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        assert!(!self.control.upgrade().unwrap().is_waiting_for_wake());
        Poll::Ready(())
    }
}
impl Drop for QueryDuringFailingDrop {
    fn drop(&mut self) {
        assert!(!self.control.upgrade().unwrap().is_waiting_for_wake());
        self.drops.fetch_add(1, Ordering::SeqCst);
        std::panic::panic_any(617usize);
    }
}

/// 业务异步计算（Future）只经共享控制句柄（ControlHandle）观察自身；首次唤醒（wake-up）不代做交接（handoff）。
struct WaitingQueryFuture {
    control: std::sync::Weak<pi_async_task::ControlHandle>,
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}
impl Future for WaitingQueryFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let control = self.control.upgrade().unwrap();
        assert_eq!(control.snapshot().execution, ExecutionStatus::Running);
        assert!(!control.is_waiting_for_wake());
        match self.polls.fetch_add(1, Ordering::SeqCst) {
            0 => {
                cx.waker().wake_by_ref();
                assert!(!control.is_waiting_for_wake());
                Poll::Pending
            }
            1 => Poll::Pending,
            2 => Poll::Ready(()),
            _ => panic!("不得额外轮询（poll）"),
        }
    }
}
impl Drop for WaitingQueryFuture {
    fn drop(&mut self) {
        let control = self.control.upgrade().unwrap();
        assert_eq!(control.snapshot().execution, ExecutionStatus::Exiting);
        assert!(!control.is_waiting_for_wake());
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

/// 真实调度入口（Schedule）在入队（enqueue）前查询公开状态（public state），再把唯一持有交给原生MPSC队列（queue）。
/// 弱持有（Weak）避免测试夹具（test fixture）延长任务存储（task storage）的生命周期（lifecycle），不引入新锁（lock）。
struct WaitingQuerySchedule {
    control: std::sync::Weak<pi_async_task::ControlHandle>,
    delivery: Arc<cordyceps::MpscQueue<pi_async_task::MpscQueueEntry>>,
    deliveries: Arc<AtomicUsize>,
}
impl Schedule for WaitingQuerySchedule {
    fn schedule(&self, task: Runnable) {
        let control = self.control.upgrade().unwrap();
        assert_eq!(task.id(), control.id());
        assert!(!control.is_waiting_for_wake());
        self.deliveries.fetch_add(1, Ordering::SeqCst);
        let entry = pi_async_task::MpscEntryHandle::try_from_runnable(task)
            .unwrap_or_else(|_| panic!("真实MPSC链接（link）必须存在"));
        self.delivery.enqueue(entry);
    }
}
