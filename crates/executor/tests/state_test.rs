//! 执行器控制状态（executor control state）的公开初始化（initialization）与关闭请求（close request）。
use pi_async_executor::state::ExecutorState;

/// 验证 ExecutorState::new 可独立创建和释放（drop）控制对象（control object），无需队列（queue）、线程（thread）或时钟（clock）。
/// 各状态分类（state classifications）由后续对应公开查询（public queries）独立核验，不直接访问原子字段（atomic fields）。
#[test]
fn test_executor_state_constructs_without_runtime_resources() {
    let state = ExecutorState::new();
    drop(state);
}

/// 验证 ExecutorState::status 初始为开放（Open），不因查询（query）改变状态（state）。
#[test]
fn test_executor_state_starts_open() {
    let state = ExecutorState::new();
    assert_eq!(state.status(), pi_async_runtime_core::RuntimeStatus::Open);
    assert_eq!(state.status(), pi_async_runtime_core::RuntimeStatus::Open);
}

/// 验证 ExecutorState::close_state 初始无关闭请求（close request），查询（query）不伪造完成（completion）。
#[test]
fn test_executor_state_starts_without_close_request() {
    let state = ExecutorState::new();
    assert_eq!(state.close_state(), pi_async_runtime_core::CloseState::NotRequested);
    assert_eq!(state.close_state(), pi_async_runtime_core::CloseState::NotRequested);
}

/// 验证 ExecutorState::is_driving 初始无人持有推进权限（drive permission），只读查询（query）不取得权限。
#[test]
fn test_executor_state_starts_without_drive_permission() {
    let state = ExecutorState::new();
    assert!(!state.is_driving());
    assert!(!state.is_driving());
}

/// 验证 ExecutorState::last_idle_state 未调用策略（policy）前为未知（Unknown），查询（query）不代调策略（policy）。
#[test]
fn test_executor_state_starts_with_unknown_idle_state() {
    let state = ExecutorState::new();
    assert_eq!(state.last_idle_state(), pi_async_runtime_core::IdleState::Unknown);
    assert_eq!(state.last_idle_state(), pi_async_runtime_core::IdleState::Unknown);
}

/// 验证 ExecutorState::request_close 只记录请求（request）并通知（notify）原线程（owner thread）。
/// 首次和重复均安排检查（check），状态（state）为 Closing／Requested；无人推进（drive）不伪报 Completed。
#[test]
fn test_executor_state_close_request_notifies_without_claiming_completion() {
    use pi_async_executor::ThreadParker;
    use pi_async_runtime_core::{CloseRequestStatus, CloseState, IdleState, RuntimeStatus};
    let state = ExecutorState::new();
    let mut parker = ThreadParker::new();
    assert!(parker.take_notification());
    let notifier = parker.notifier();
    assert_eq!(state.request_close(&notifier), CloseRequestStatus::Requested);
    assert_eq!(state.status(), RuntimeStatus::Closing);
    assert_eq!(state.close_state(), CloseState::Requested);
    assert!(parker.take_notification());
    assert_eq!(state.request_close(&notifier), CloseRequestStatus::AlreadyRequested);
    assert!(parker.take_notification());
    assert!(!parker.take_notification());
    assert_eq!(state.status(), RuntimeStatus::Closing);
    assert_eq!(state.close_state(), CloseState::Requested);
    assert!(!state.is_driving());
    assert_eq!(state.last_idle_state(), IdleState::Unknown);
}

/// 验证两名真实关闭请求者（close requesters）对同一开放状态（open state）只产生一份首次请求（first request）。
/// 标准线程（threads）仅调用生产入口（production API），不修改私有状态（private state）或代发通知（notification）。
#[test]
fn test_executor_state_concurrent_close_requests_have_one_first_request() {
    use pi_async_executor::ThreadParker;
    use pi_async_runtime_core::{CloseRequestStatus, CloseState, RuntimeStatus};
    let state = ExecutorState::new();
    let mut parker = ThreadParker::new();
    assert!(parker.take_notification());
    let notifier = parker.notifier();
    let (first, second) = std::thread::scope(|scope| {
        let first = scope.spawn(|| state.request_close(&notifier));
        let second = scope.spawn(|| state.request_close(&notifier));
        (first.join().unwrap(), second.join().unwrap())
    });
    assert!(matches!((first, second),
        (CloseRequestStatus::Requested, CloseRequestStatus::AlreadyRequested)
        | (CloseRequestStatus::AlreadyRequested, CloseRequestStatus::Requested)));
    assert!(parker.take_notification());
    assert!(!parker.take_notification());
    assert_eq!(state.status(), RuntimeStatus::Closing);
    assert_eq!(state.close_state(), CloseState::Requested);
}

/// 真实原生任务（native task）的有限观察值（probe）；入队（enqueue）和拒绝（rejection）均不得轮询（poll）业务。
struct QueueFutureProbe { polls: std::sync::Arc<std::sync::atomic::AtomicUsize>, drops: std::sync::Arc<std::sync::atomic::AtomicUsize> }
impl std::future::Future for QueueFutureProbe {
    type Output = ();
    fn poll(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        self.polls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::task::Poll::Pending
    }
}
impl Drop for QueueFutureProbe {
    fn drop(&mut self) { self.drops.fetch_add(1, std::sync::atomic::Ordering::Relaxed); }
}

/// 仅禁止本切片（slice）意外进入调度（scheduling）；测试先用真实所属域（TaskThread）关闭（close），再归还队列持有（queue-held runnable）。
struct NoQueueSchedule;
impl pi_async_task::Schedule for NoQueueSchedule {
    fn schedule(&self, _: pi_async_task::Runnable) { panic!("入队切片不得调度（enqueue slice must not schedule）"); }
}

/// 验证 ExecutorState::push_front/back 只交原持有（Runnable）给真实 TaskQueue，不接纳（admit）、轮询（poll）或取消（cancel）。
/// 队首／队尾（front/back）保持原身份顺序；WrongOwner、LinkMissing、Closed 均返还原值（original value），所属域（TaskThread）仍唯一负责本地清理（local cleanup）。
/// 内部计数（internal count）的精确回滚（rollback）由本体消费者（executor consumer）后续补证，本用例不读取私有原子（private atomic）。
#[test]
fn test_state_local_push_preserves_order_original_values_and_rejection() {
    use pi_async_task::{create, create_local, TaskOptions, TaskThread};
    use pi_async_task::definitions::{CancelRequestStatus, QueueSupport};
    use pi_async_task_queue::{TaskQueue, local::QueuePushErrorKind};
    use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
    let state = ExecutorState::new();
    let owner = TaskThread::new();
    let mut queue = TaskQueue::new(&owner);
    let polls = Arc::new(AtomicUsize::new(0)); let drops = Arc::new(AtomicUsize::new(0));
    let first = create_local(&owner, QueueFutureProbe { polls: polls.clone(), drops: drops.clone() }, NoQueueSchedule, (), TaskOptions::intrusive())
        .unwrap_or_else(|_| panic!("合法创建应成功（valid creation must succeed）"));
    let second = create_local(&owner, QueueFutureProbe { polls: polls.clone(), drops: drops.clone() }, NoQueueSchedule, (), TaskOptions::intrusive())
        .unwrap_or_else(|_| panic!("合法创建应成功（valid creation must succeed）"));
    let first_id = first.runnable.id(); let second_id = second.runnable.id();
    state.push_back(&mut queue, first.runnable).unwrap_or_else(|_| panic!("队尾接收应成功（back push must succeed）"));
    state.push_front(&mut queue, second.runnable).unwrap_or_else(|_| panic!("队首接收应成功（front push must succeed）"));
    assert_eq!(queue.len(), 2);
    assert!(!first.control.snapshot().admitted); assert!(!second.control.snapshot().admitted);
    assert_eq!(first.control.snapshot().cancellation, CancelRequestStatus::NotRecorded);
    assert_eq!(polls.load(Ordering::Relaxed), 0); assert_eq!(drops.load(Ordering::Relaxed), 0);
    owner.close();
    assert_eq!(drops.load(Ordering::Relaxed), 2);
    let second_value = queue.pop_front().unwrap(); let first_value = queue.pop_front().unwrap();
    assert_eq!(second_value.id(), second_id); assert_eq!(first_value.id(), first_id);
    drop(second_value); drop(first_value); assert!(queue.is_empty());
    assert_eq!(drops.load(Ordering::Relaxed), 2);
    for front in [false, true] {
        let parts = create(QueueFutureProbe { polls: polls.clone(), drops: drops.clone() }, NoQueueSchedule, (), TaskOptions::plain())
            .unwrap_or_else(|_| panic!("合法创建应成功（valid creation must succeed）"));
        let id = parts.runnable.id();
        let rejected = if front { state.push_front(&mut queue, parts.runnable) } else { state.push_back(&mut queue, parts.runnable) };
        let error = rejected.err().unwrap();
        assert_eq!(error.cause, QueuePushErrorKind::WrongOwner); assert_eq!(error.task.id(), id);
        assert_eq!(error.task.snapshot().cancellation, CancelRequestStatus::NotRecorded);
        assert!(!error.task.snapshot().admitted); assert!(queue.is_empty());
        drop(error.task);
    }
    assert_eq!(drops.load(Ordering::Relaxed), 4);
    let other_owner = TaskThread::new(); let mut other_queue = TaskQueue::new(&other_owner);
    let parts = create_local(&other_owner, QueueFutureProbe { polls: polls.clone(), drops: drops.clone() }, NoQueueSchedule, (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("合法创建应成功（valid creation must succeed）"));
    let id = parts.runnable.id();
    let error = state.push_back(&mut other_queue, parts.runnable).err().unwrap();
    assert_eq!(error.cause, QueuePushErrorKind::LinkMissing); assert_eq!(error.task.id(), id);
    assert_eq!(error.task.snapshot().cancellation, CancelRequestStatus::NotRecorded); assert!(other_queue.is_empty());
    assert_eq!(drops.load(Ordering::Relaxed), 4);
    other_owner.close(); drop(error.task);
    assert_eq!(drops.load(Ordering::Relaxed), 5);
    other_queue.close().unwrap();
    for front in [false, true] {
        let mut options = TaskOptions::plain(); options.queue = QueueSupport::IntrusiveLocalAndMpsc;
        let parts = create(QueueFutureProbe { polls: polls.clone(), drops: drops.clone() }, NoQueueSchedule, (), options)
            .unwrap_or_else(|_| panic!("合法创建应成功（valid creation must succeed）"));
        let id = parts.runnable.id();
        let rejected = if front { state.push_front(&mut other_queue, parts.runnable) } else { state.push_back(&mut other_queue, parts.runnable) };
        let error = rejected.err().unwrap();
        assert_eq!(error.cause, QueuePushErrorKind::Closed); assert_eq!(error.task.id(), id);
        assert_eq!(error.task.snapshot().cancellation, CancelRequestStatus::NotRecorded); assert!(other_queue.is_empty());
        drop(error.task);
    }
    assert_eq!(drops.load(Ordering::Relaxed), 7); assert_eq!(polls.load(Ordering::Relaxed), 0);
}

/// 验证 ExecutorState::push_back 对未绑定原值（unbound runnable）的拒绝（WrongOwner）保持原身份（identity），且不轮询（poll）或取消（cancel）。
/// 无所属域的原值（unbound Future）由调用者最终释放（drop）；占位（stub）前置不依赖已绑定清理调度（bound cleanup scheduling）。
#[test]
fn test_state_local_push_rejects_unbound_task_without_consuming_original() {
    use pi_async_task::{create, TaskOptions, TaskThread};
    use pi_async_task::definitions::CancelRequestStatus;
    use pi_async_task_queue::{TaskQueue, local::QueuePushErrorKind};
    use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
    let state = ExecutorState::new(); let owner = TaskThread::new(); let mut queue = TaskQueue::new(&owner);
    let polls = Arc::new(AtomicUsize::new(0)); let drops = Arc::new(AtomicUsize::new(0));
    let parts = create(QueueFutureProbe { polls: polls.clone(), drops: drops.clone() }, NoQueueSchedule, (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("合法创建应成功（valid creation must succeed）"));
    let id = parts.runnable.id();
    let error = state.push_back(&mut queue, parts.runnable).err().unwrap();
    assert_eq!(error.cause, QueuePushErrorKind::WrongOwner); assert_eq!(error.task.id(), id);
    assert_eq!(error.task.snapshot().cancellation, CancelRequestStatus::NotRecorded);
    assert!(!error.task.snapshot().admitted); assert!(queue.is_empty());
    assert_eq!(polls.load(Ordering::Relaxed), 0); assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(error.task);
    assert_eq!(drops.load(Ordering::Relaxed), 1); assert_eq!(polls.load(Ordering::Relaxed), 0);
}
