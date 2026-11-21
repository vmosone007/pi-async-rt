#[allow(unused_variables)]
fn state_new_inner() -> ExecutorState {
    ExecutorState {
        flags: AtomicU8::new(0),
        idle: AtomicU8::new(0),
        internal: AtomicUsize::new(0),
        in_flight: AtomicUsize::new(0),
    }
}
#[allow(unused_variables)]
fn state_status_inner(this: &ExecutorState) -> RuntimeStatus {
    let flags = this.flags.load(std::sync::atomic::Ordering::Acquire);
    if flags & UNAVAILABLE != 0 {
        RuntimeStatus::Closed
    } else if flags & CLOSE_REQUESTED != 0 {
        RuntimeStatus::Closing
    } else {
        RuntimeStatus::Open
    }
}
#[allow(unused_variables)]
fn state_close_inner(this: &ExecutorState) -> CloseState {
    let flags = this.flags.load(std::sync::atomic::Ordering::Acquire);
    if flags & CLOSE_COMPLETE != 0 {
        CloseState::Completed
    } else if flags & (CLOSE_REQUESTED | UNAVAILABLE) != 0 {
        CloseState::Requested
    } else {
        CloseState::NotRequested
    }
}
#[allow(unused_variables)]
fn state_driving_inner(this: &ExecutorState) -> bool {
    this.flags.load(std::sync::atomic::Ordering::Acquire) & DRIVING != 0
}
#[allow(unused_variables)]
fn state_idle_inner(this: &ExecutorState) -> IdleState {
    match this.idle.load(std::sync::atomic::Ordering::Acquire) {
        1 => IdleState::Idle,
        2 => IdleState::NotIdle,
        _ => IdleState::Unknown,
    }
}
#[allow(unused_variables)]
fn state_request_inner(this: &ExecutorState, notifier: &ThreadNotifier) -> CloseRequestStatus {
    let previous = this.flags.fetch_or(CLOSE_REQUESTED, std::sync::atomic::Ordering::AcqRel);
    if previous & (UNAVAILABLE | CLOSE_COMPLETE) != 0 {
        return CloseRequestStatus::AlreadyClosed;
    }
    pi_async_task_queue::notify::QueueNotifier::notify(notifier);
    if previous & CLOSE_REQUESTED != 0 {
        CloseRequestStatus::AlreadyRequested
    } else {
        CloseRequestStatus::Requested
    }
}
#[allow(unused_variables)]
fn state_try_drive_inner(this: &ExecutorState) -> bool {
    // 只有看到推进位原本未设置的调用者取得推进权；服务是否继续接收由关闭事实独立决定。
    this.flags.fetch_or(DRIVING, std::sync::atomic::Ordering::AcqRel) & DRIVING == 0
}
#[allow(unused_variables)]
fn state_release_drive_inner(this: &ExecutorState) {
    // 原子读改写（RMW）只归还推进权；同一事实字里的关闭请求即使并发写入也不会被覆盖。
    this.flags.fetch_and(!DRIVING, std::sync::atomic::Ordering::Release);
}
#[allow(unused_variables)]
fn state_record_idle_inner(this: &ExecutorState, value: IdleState) {
    let stored = match value {
        IdleState::Unknown => 0,
        IdleState::Idle => 1,
        IdleState::NotIdle => 2,
    };
    // 调用方只在策略（policy）正常返回后发布；读者取最近记录，不重新调用策略。
    this.idle.store(stored, std::sync::atomic::Ordering::Release);
}
#[allow(unused_variables)]
fn state_finish_owner_inner(this: &ExecutorState, failed_in_flight: usize, cleanup_failed: bool) {
    // 原所属域（TaskThread）已退出且各来源责任已结束或移交。先发布已接纳但未确认的末值，再发布关闭结果。
    this.in_flight.store(failed_in_flight, std::sync::atomic::Ordering::Release);
    let _ = this.flags.try_update(
        std::sync::atomic::Ordering::AcqRel,
        std::sync::atomic::Ordering::Relaxed,
        |previous| {
            let closed = previous | UNAVAILABLE | CLOSE_REQUESTED;
            // 零项已接纳失败不排除未接纳原任务（Task）或来源清理（cleanup）失败。
            Some(if cleanup_failed || failed_in_flight != 0 || previous & CLEANUP_FAILED != 0 {
                (closed | CLEANUP_FAILED) & !CLOSE_COMPLETE
            } else {
                closed | CLOSE_COMPLETE
            })
        },
    );
}

#[allow(unused_variables)]
fn state_push_inner(
    this: &ExecutorState,
    queue: &mut TaskQueue,
    task: Runnable,
    front: bool,
) -> pi_result::RawResult<(), QueuePushError> {
    let previous = this.internal.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    if previous == usize::MAX {
        std::process::abort();
    }
    let mut guard = InternalCountGuard { state: this, armed: true };
    // 具体队列（TaskQueue）接受后不调用用户代码；原拒绝值（QueuePushError）保留到预留（reservation）归还之后。
    let result = if front { queue.push_front(task) } else { queue.push_back(task) };
    if result.is_ok() {
        guard.armed = false;
    }
    drop(guard);
    result
}
#[allow(unused_variables)]
fn state_count_guard_drop_inner(this: &mut InternalCountGuard<'_>) {
    if this.armed {
        // 先撤销守卫（guard），只归还未交接的计数预留（count reservation）。
        this.armed = false;
        if this.state.internal.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) == 0 {
            std::process::abort();
        }
    }
}

#[cfg(test)]
mod state_tests {
    use super::ExecutorState;
    use crate::ThreadParker;
    use pi_async_runtime_core::definitions::{CloseRequestStatus, CloseState, RuntimeStatus};

    /// 经 ExecutorState 的推进权接口验证排他取得、重复拒绝、释放后重取和关闭请求（close request）保留。
    /// 用真实 ThreadParker 区分关闭请求发出的通知（notification）与推进权操作；不访问内部原子字段（atomic fields）。
    #[test]
    fn test_drive_permission_is_exclusive_and_release_preserves_close_request() {
        let state = ExecutorState::new();
        let mut parker = ThreadParker::new();
        assert!(parker.take_notification());
        let notifier = parker.notifier();

        assert!(!state.is_driving());
        assert!(state.try_drive());
        assert!(state.is_driving());
        assert!(!state.try_drive());
        assert!(state.is_driving());
        assert!(!parker.take_notification());

        assert_eq!(state.request_close(&notifier), CloseRequestStatus::Requested);
        assert!(state.is_driving());
        assert!(parker.take_notification());
        state.release_drive();
        assert!(!state.is_driving());
        assert_eq!(state.status(), RuntimeStatus::Closing);
        assert_eq!(state.close_state(), CloseState::Requested);
        assert!(!parker.take_notification());

        assert!(state.try_drive());
        assert_eq!(state.status(), RuntimeStatus::Closing);
        state.release_drive();
        assert!(!state.is_driving());
        assert_eq!(state.close_state(), CloseState::Requested);
        assert!(!parker.take_notification());
    }

    /// 经 ExecutorState::record_idle 交入已返回的空闲判断（idle decision）；推进权（drive permission）和关闭请求（close request）独立保存。
    /// 只核状态发布，不代替事件循环（event loop）调用策略或处理策略恐慌（panic）。
    #[test]
    fn test_idle_decision_remains_independent_of_service_control() {
        use pi_async_runtime_core::definitions::IdleState;

        let state = ExecutorState::new();
        let parker = ThreadParker::new();
        let notifier = parker.notifier();
        assert_eq!(state.last_idle_state(), IdleState::Unknown);
        assert!(state.try_drive());

        state.record_idle(IdleState::Idle);
        assert_eq!(state.last_idle_state(), IdleState::Idle);
        assert_eq!(state.status(), RuntimeStatus::Open);
        assert!(state.is_driving());

        state.record_idle(IdleState::NotIdle);
        state.release_drive();
        assert_eq!(state.last_idle_state(), IdleState::NotIdle);
        assert_eq!(state.request_close(&notifier), CloseRequestStatus::Requested);
        assert_eq!(state.last_idle_state(), IdleState::NotIdle);
        assert_eq!(state.close_state(), CloseState::Requested);
        assert!(!state.is_driving());
    }

    /// 记录真实所属域（TaskThread）对原异步计算（Future）的清理次数，失败时传播原恐慌（panic）。
    struct OwnerExitProbe {
        drops: std::rc::Rc<std::cell::Cell<usize>>,
        fail: bool,
    }

    impl std::future::Future for OwnerExitProbe {
        type Output = ();

        fn poll(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<()> {
            panic!("退出验证不得轮询（poll）业务")
        }
    }

    impl Drop for OwnerExitProbe {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
            if self.fail {
                std::panic::panic_any("owner-cleanup-failure");
            }
        }
    }

    /// 未提交的原任务（Task）由唯一所属域退出清理，本用例不允许进入调度（scheduling）。
    struct NoExitSchedule;

    impl pi_async_task::Schedule for NoExitSchedule {
        fn schedule(&self, _: pi_async_task::Runnable) {
            panic!("退出验证不得安排调度（schedule）");
        }
    }

    /// 以真实 TaskThread::drop 产生两种清理（cleanup）结果，再通过 ExecutorState::finish_owner 发布。
    /// 两例都没有其它来源或已接纳项；只核最终状态发布及公开查询，不替代执行器（executor）汇总失败。
    #[test]
    fn test_owner_exit_distinguishes_unadmitted_cleanup_failure_from_completed_shutdown() {
        use pi_async_task::definitions::{ExecutionStatus, FutureDropStatus};
        use pi_async_task::{create_local, TaskOptions, TaskThread};
        use std::{cell::Cell, panic::{catch_unwind, AssertUnwindSafe}, rc::Rc};

        for fail in [false, true] {
            let state = ExecutorState::new();
            let mut parker = ThreadParker::new();
            assert!(parker.take_notification());
            let notifier = parker.notifier();
            let owner = TaskThread::new();
            let owner_id = owner.id();
            let drops = Rc::new(Cell::new(0));
            let task = create_local(
                &owner,
                OwnerExitProbe { drops: Rc::clone(&drops), fail },
                NoExitSchedule,
                (),
                TaskOptions::plain(),
            ).unwrap_or_else(|_| panic!("合法所属域（TaskThread）应能创建本地任务（Task）"));
            assert!(!task.control.snapshot().admitted);
            assert_eq!(drops.get(), 0);
            assert_eq!(state.request_close(&notifier), CloseRequestStatus::Requested);
            assert!(parker.take_notification());
            assert_eq!(state.status(), RuntimeStatus::Closing);
            assert_eq!(state.close_state(), CloseState::Requested);

            let exit = catch_unwind(AssertUnwindSafe(|| drop(owner)));
            let cleanup_failed = exit.is_err();
            assert_eq!(cleanup_failed, fail);
            let snapshot = task.control.snapshot();
            assert_eq!(snapshot.owner, Some(owner_id));
            assert!(!snapshot.admitted);
            if let Err(original) = exit {
                assert_eq!(original.downcast_ref::<&str>(), Some(&"owner-cleanup-failure"));
                assert_eq!(snapshot.execution, ExecutionStatus::CleanupUnconfirmed);
                assert_eq!(snapshot.future_drop, FutureDropStatus::Unconfirmed);
            } else {
                assert_eq!(snapshot.future_drop, FutureDropStatus::Confirmed);
            }
            assert_eq!(drops.get(), 1);
            assert_eq!(state.close_state(), CloseState::Requested);

            state.finish_owner(0, cleanup_failed);
            let expected = if fail { CloseState::Requested } else { CloseState::Completed };
            assert_eq!(state.status(), RuntimeStatus::Closed);
            assert_eq!(state.close_state(), expected);
            assert_eq!(state.request_close(&notifier), CloseRequestStatus::AlreadyClosed);
            assert!(!parker.take_notification());
            drop(task);
            assert_eq!(drops.get(), 1);
            assert_eq!(state.close_state(), expected);
        }
    }

    /// 原任务（Task）已接纳（admission）且清理失败时，ExecutorState::finish_owner 发布不可用，
    /// 但不归还已取得的独占推进权限（exclusive advancement permission）。通过状态查询和所属域析构（owner-domain drop）验证，
    /// 不读取内部计数，也不代替执行器（executor）汇总失败。
    #[test]
    fn test_admitted_cleanup_failure_keeps_drive_permission_until_released() {
        use pi_async_task::definitions::{ExecutionStatus, FutureDropStatus};
        use pi_async_task::{create_local, TaskOptions, TaskThread};
        use std::{cell::Cell, panic::{catch_unwind, AssertUnwindSafe}, rc::Rc};

        let state = ExecutorState::new();
        let mut parker = ThreadParker::new();
        assert!(parker.take_notification());
        let notifier = parker.notifier();
        let owner = TaskThread::new();
        let owner_id = owner.id();
        let drops = Rc::new(Cell::new(0));
        let task = create_local(
            &owner,
            OwnerExitProbe { drops: Rc::clone(&drops), fail: true },
            NoExitSchedule,
            (),
            TaskOptions::plain(),
        ).unwrap_or_else(|_| panic!("合法所属域（owner domain）应能创建本地任务（Task）"));
        let registration = task.runnable.admit(&owner).expect("应实际接纳（admission）原任务（Task）");
        assert!(task.control.snapshot().admitted);
        assert_eq!(drops.get(), 0);
        assert!(state.try_drive());
        assert_eq!(state.request_close(&notifier), CloseRequestStatus::Requested);
        assert!(parker.take_notification());

        let original = catch_unwind(AssertUnwindSafe(|| drop(owner)))
            .expect_err("所属域析构（owner-domain drop）应传播原恐慌（panic）");
        assert_eq!(original.downcast_ref::<&str>(), Some(&"owner-cleanup-failure"));
        let snapshot = task.control.snapshot();
        assert_eq!(snapshot.owner, Some(owner_id));
        assert!(snapshot.admitted);
        assert_eq!(snapshot.execution, ExecutionStatus::CleanupUnconfirmed);
        assert_eq!(snapshot.future_drop, FutureDropStatus::Unconfirmed);
        assert_eq!(drops.get(), 1);

        // 本用例只有这一项真实清理未确认（CleanupUnconfirmed），也没有其它来源。
        state.finish_owner(1, true);
        assert_eq!(state.status(), RuntimeStatus::Closed);
        assert_eq!(state.close_state(), CloseState::Requested);
        assert!(state.is_driving());
        assert!(!state.try_drive());
        assert_eq!(state.request_close(&notifier), CloseRequestStatus::AlreadyClosed);
        state.release_drive();
        assert!(!state.is_driving());
        assert_eq!(state.status(), RuntimeStatus::Closed);
        assert_eq!(state.close_state(), CloseState::Requested);
        assert!(!parker.take_notification());
        drop((registration, task));
        assert_eq!(drops.get(), 1);
    }
}
