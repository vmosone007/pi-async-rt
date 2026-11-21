#[allow(unused_variables)]
fn local_owner_inner<T: ExecutorTimer + 'static, R>(
    this: &ExecutorLocal<T>,
    action: impl FnOnce(&TaskThread) -> R,
) -> pi_result::Result<R> {
    // 先建立活动守卫：owner 借用或 action 展开都先归还借用，再由守卫在无借用时接续退出。
    let activity = LocalActivityGuard { local: this };
    let borrowed = match this.owner.try_borrow() {
        Ok(borrowed) => borrowed,
        Err(_) => {
            drop(activity);
            return Err(pi_result::Error::new(pi_result::ErrorKind::InvalidState)
                .attach("所属域（owner）正在被使用"));
        }
    };
    let Some(owner) = borrowed.as_ref() else {
        drop(borrowed);
        drop(activity);
        return Err(pi_result::Error::new(pi_result::ErrorKind::InvalidState)
            .attach("所属域（owner）已经退出"));
    };
    // 只持共享不可变 owner 借用执行同步 action；不在此借 ready／timer／registry 或 TLS。
    let result = action(owner);
    drop(borrowed);
    drop(activity);
    Ok(result)
}
fn local_push_inner<T: ExecutorTimer + 'static>(
    this: &ExecutorLocal<T>,
    task: Runnable,
    front: bool,
) -> pi_result::RawResult<(), QueuePushError> {
    // 原线程与有效本地权限已由上层 TLS 本地访问核验；这里只核当前服务是否仍开放。
    if this.state.status() != pi_async_runtime_core::definitions::RuntimeStatus::Open {
        return Err(QueuePushError { task, cause: pi_async_task_queue::local::QueuePushErrorKind::Closed });
    }
    // 只短借线程内容器；push 计账与 ReadyReleaseGuard 成对，分配/用户代码都不在借用内运行。
    let mut ready = match this.ready.try_borrow_mut() {
        Ok(ready) => ready,
        Err(_) => panic!("本地就绪队列（ready queue）不得在借用内重入"),
    };
    let Some(queue) = ready.as_mut() else {
        return Err(QueuePushError { task, cause: pi_async_task_queue::local::QueuePushErrorKind::Closed });
    };
    let result = if front {
        this.state.push_front(queue, task)
    } else {
        this.state.push_back(queue, task)
    };
    drop(ready);
    if result.is_ok() {
        // 借用结束后才通知；拒绝时原任务与原因原样返还，不接纳、不取消、不轮询。
        pi_async_task_queue::notify::QueueNotifier::notify(&this.notifier);
    }
    result
}
fn local_timer_push_inner<T: ExecutorTimer + 'static>(
    this: &ExecutorLocal<T>,
    deadline: T::Instant,
    item: T::Item,
) -> pi_result::RawResult<T::Key, TimerSubmitError<T::Item>> {
    use pi_async_task_queue::timer::TimerQueueError;
    // 同源所属线程与有效权限已由上层本地访问核验；本层只核服务仍开放。
    if this.state.status() != pi_async_runtime_core::definitions::RuntimeStatus::Open {
        return Err(TimerSubmitError { item, cause: TimerQueueError::Closed });
    }
    // 暂借冲突不伪造为来源拒绝，按既有分类返 Busy 并保留原值。
    let mut timer = match this.timer.try_borrow_mut() {
        Ok(timer) => timer,
        Err(_) => return Err(TimerSubmitError { item, cause: TimerQueueError::Busy }),
    };
    let Some(timer) = timer.as_mut() else {
        return Err(TimerSubmitError { item, cause: TimerQueueError::Closed });
    };
    // 借用只覆盖一次本地提交；结果独立返回，不在此再取时或调用钩子。
    pi_async_task_queue::timer::TimerTaskQueue::try_push_local(timer, deadline, item)
}
fn local_selected_inner<T: ExecutorTimer + 'static>(
    this: &ExecutorLocal<T>,
    task: Runnable,
) -> pi_result::Result<PreparedTask> {
    use pi_async_task::definitions::{CancelRequestStatus, ExecutionStatus, TaskPhase};
    use std::sync::atomic::Ordering::Release;

    let missing = || {
        pi_result::Error::new(pi_result::ErrorKind::InvalidState)
            .attach("本地任务选择（local selection）缺少有效登记")
    };
    let snapshot = task.snapshot();
    // 普通创建任务在首次接纳前合法处于未绑定（owner == None）；只拒绝已属于其它所属域的任务。
    if matches!(snapshot.owner, Some(owner) if owner != this.owner_id) {
        return Err(missing());
    }
    // 已结束或清理已未确认：本域迟到值归既有结算，不重试清理。
    if snapshot.phase == TaskPhase::Ended
        || snapshot.execution == ExecutionStatus::CleanupUnconfirmed
    {
        return Ok(PreparedTask::Settled(task));
    }
    // 只核同一来源登记的任务查询句柄（TaskQuery），在任何运行或析构前结束索引借用。
    let registered = this.registry.borrow_mut().as_mut()
        .is_some_and(|registry| registry.query(snapshot.id).is_some());
    if !registered {
        return Err(missing());
    }
    // 已记录取消或已进入退出：只清理，不为了取消隐式接纳或触达业务钩子。
    if snapshot.cancellation == CancelRequestStatus::Recorded || snapshot.phase == TaskPhase::Exiting {
        return Ok(PreparedTask::Cleanup(task));
    }
    // 已接纳：复用活动表中的登记；没有可复用登记者按清理处理。
    if snapshot.admitted {
        let existing = {
            let active = this.active.borrow();
            match active.get(&snapshot.id) {
                Some(ActiveRecord::Registered(registration)) => Some(std::rc::Rc::clone(registration)),
                _ => None,
            }
        };
        return match existing {
            Some(registration) => Ok(PreparedTask::Business(task, registration)),
            None => Ok(PreparedTask::Cleanup(task)),
        };
    }
    // 尚未接纳：先在活动表预留容量，再借原任务建立补账守卫并调用一次原生接纳。
    this.active.borrow_mut().reserve(1);
    this.active_order.borrow_mut().reserve(1);
    let id = snapshot.id;
    let outcome = {
        let borrowed = this.owner.borrow();
        let Some(owner) = borrowed.as_ref() else {
            return Err(missing());
        };
        let mut attempt = AdmissionAttemptGuard { local: this, task: &task, armed: true };
        let outcome = task.admit(owner);
        // 正常返回先撤守卫；展开时由守卫按实际接纳事实补账。
        attempt.armed = false;
        drop(attempt);
        outcome
    };
    match outcome {
        Ok(registration) => {
            let registration = std::rc::Rc::new(registration);
            this.active
                .borrow_mut()
                .insert(id, ActiveRecord::Registered(std::rc::Rc::clone(&registration)));
            this.active_order.borrow_mut().push_back(id);
            this.state.in_flight.fetch_add(1, Release);
            Ok(PreparedTask::Business(task, registration))
        }
        Err(pi_async_task::definitions::AdmissionError::AlreadyEnded) => {
            // 仅“已被结束”拒绝重取一次单调快照：已取消或已退出归清理，否则按原拒绝报告。
            let again = task.snapshot();
            if again.cancellation == CancelRequestStatus::Recorded
                || matches!(again.phase, TaskPhase::Exiting | TaskPhase::Ended)
            {
                Ok(PreparedTask::Cleanup(task))
            } else {
                Err(missing())
            }
        }
        Err(_) => Err(missing()),
    }
}
fn local_maintain_inner<T: ExecutorTimer + 'static>(this: &ExecutorLocal<T>, max_nodes: usize) {
    use pi_async_task::definitions::{FutureDropStatus, TaskPhase};
    use std::sync::atomic::Ordering::Release;

    // 按既有顺序轮转最多 max_nodes 个已登记身份；每项只短暂借用，读完立即放开。
    let rounds = max_nodes.min(this.active_order.borrow().len());
    for _ in 0..rounds {
        let Some(id) = this.active_order.borrow_mut().pop_front() else {
            break;
        };
        let Some(finished) = ({
            let active = this.active.borrow();
            active.get(&id).map(|record| {
                let snapshot = record.snapshot();
                snapshot.phase == TaskPhase::Ended
                    && snapshot.future_drop == FutureDropStatus::Confirmed
            })
        }) else {
            continue;
        };
        if finished {
            // 先移出再在借用外释放；失败清理未确认者不会走到这里，保留原计数与事实。
            let removed = this.active.borrow_mut().remove(&id);
            // 先结算已经结束的一项；附加数据（metadata）的析构可能发生恐慌（panic）。
            this.state.in_flight.fetch_sub(1, Release);
            drop(removed);
        } else {
            this.active_order.borrow_mut().push_back(id);
        }
    }
    // 维护可能归还最后一份任务附加数据（task metadata）。先移出管理者，结束借用，再做析构。
    // 即使附加数据的析构展开（unwind），也须归还同一管理者，避免提前封闭登记表。
    struct RestoreRegistry<'a> {
        slot: &'a RefCell<Option<TaskRegistry>>,
        registry: Option<TaskRegistry>,
    }
    impl Drop for RestoreRegistry<'_> {
        fn drop(&mut self) {
            *self.slot.borrow_mut() = self.registry.take();
        }
    }
    let registry = this.registry.borrow_mut().take();
    let mut restore = RestoreRegistry { slot: &this.registry, registry };
    if let Some(registry) = restore.registry.as_mut() {
        registry.maintain(max_nodes);
    }
}
fn local_finish_exit_inner<T: ExecutorTimer + 'static>(
    this: &ExecutorLocal<T>,
    source_failed: bool,
) -> bool {
    // 意图与来源失败单调记录，重复调用幂等。
    this.exit_requested.set(true);
    if source_failed {
        this.source_failed.set(true);
    }
    if this.exit_finished.get() {
        return true;
    }
    // 已有唯一执行者：只合并意图，不以 owner 缺失当作完成。
    if this.exit_running.get() {
        return false;
    }
    // owner 仍被同步活动借用：不穿透借用、不等待，保留设施等下一次安全作用域。
    if this.owner.try_borrow_mut().is_err() {
        return false;
    }
    this.exit_running.set(true);
    let mut guard = LocalExitGuard {
        local: this,
        phase: LocalExitPhase::Owner,
        current_failed: false,
        failed: false,
        domain_unconfirmed: 0,
        admitted_unconfirmed: 0,
    };
    // 正常路径显式排水；完成与首错余项由同一守卫在 Finish 阶段发布。
    local_exit_drain_inner(&mut guard);
    guard.phase = LocalExitPhase::Done;
    drop(guard);
    this.exit_finished.get()
}
fn local_drop_inner<T: ExecutorTimer + 'static>(this: &mut ExecutorLocal<T>) {
    // 最后一份独占持有：先走正常退出路径（幂等）；已完成则无需重复。
    if local_finish_exit_inner(this, false) {
        return;
    }
    // 未能完成时以独占访问继续：不依赖 Rc::get_mut（其它 Weak 会使其失败），
    // 直接按同一阶段顺序取出全部余项；已空字段只见空 Option，不会二次释放。
    let mut guard = LocalExitGuard {
        local: this,
        phase: LocalExitPhase::Owner,
        current_failed: false,
        failed: true,
        domain_unconfirmed: 0,
        admitted_unconfirmed: 0,
    };
    local_exit_drain_inner(&mut guard);
    guard.phase = LocalExitPhase::Done;
    drop(guard);
}
fn local_exit_drain_inner<T: ExecutorTimer + 'static>(guard: &mut LocalExitGuard<'_, T>) {
    let local = guard.local;
    // 1. 原 TaskThread 真正退出：先 take 结束借用，再在借用外释放。
    if matches!(guard.phase, LocalExitPhase::Owner) {
        guard.phase = LocalExitPhase::Ready;
        guard.current_failed = true;
        let owner = local.owner.borrow_mut().take();
        drop(owner);
        guard.current_failed = false;
    }
    // 2. 所属域退出后允许在原 TaskThread 的 Drop 展开路径采样未确认清理。
    if matches!(guard.phase, LocalExitPhase::Ready) {
        // 真实域退出后按原所属域核未确认数，供 Finish 决定最终状态。
        if let Ok(borrowed) = local.registry.try_borrow() {
            if let Some(registry) = borrowed.as_ref() {
                let (domain, admitted) = registry.unconfirmed_counts(local.owner_id);
                guard.domain_unconfirmed = domain;
                guard.admitted_unconfirmed = admitted;
            }
        }
        // 取出线程内容器后，以原释放守卫配对结清内部计数。
        guard.phase = LocalExitPhase::Timer;
        guard.current_failed = true;
        let ready = local.ready.borrow_mut().take();
        if let Some(queue) = ready {
            let release = ReadyReleaseGuard { state: &local.state, count: queue.len() };
            drop(queue);
            drop(release);
        }
        guard.current_failed = false;
    }
    // 3. 定时来源拥有者：直接释放，由其永久停收并接续清理或移交。
    if matches!(guard.phase, LocalExitPhase::Timer) {
        guard.phase = LocalExitPhase::Returns;
        guard.current_failed = true;
        let timer = local.timer.borrow_mut().take();
        drop(timer);
        guard.current_failed = false;
    }
    // 4. 归属回送：拥有者退出后先关闭再释放。
    if matches!(guard.phase, LocalExitPhase::Returns) {
        guard.phase = LocalExitPhase::Active;
        guard.current_failed = true;
        let returns = local.returns.borrow_mut().take();
        if let Some(mut returns) = returns {
            let _ = returns.close_after_owner();
            drop(returns);
        }
        guard.current_failed = false;
    }
    // 5. 活动记录：逐项移出、借用外释放；未清空前保持本阶段，首错不跳过余项。
    if matches!(guard.phase, LocalExitPhase::Active) {
        loop {
            let removed = {
                let mut active = local.active.borrow_mut();
                match active.keys().next().copied() {
                    Some(id) => active.remove(&id),
                    None => None,
                }
            };
            match removed {
                Some(record) => {
                    guard.current_failed = true;
                    drop(record);
                    guard.current_failed = false;
                }
                None => break,
            }
        }
        drop(std::mem::take(&mut *local.active.borrow_mut()));
        drop(std::mem::take(&mut *local.active_order.borrow_mut()));
        guard.phase = LocalExitPhase::Registry;
    }
    // 6. 登记索引最后释放，才允许封闭（seal）。
    if matches!(guard.phase, LocalExitPhase::Registry) {
        guard.phase = LocalExitPhase::Finish;
        guard.current_failed = true;
        let registry = local.registry.borrow_mut().take();
        drop(registry);
        guard.current_failed = false;
    }
    // 7. 发布最终状态：先置完成再撤执行资格。
    if matches!(guard.phase, LocalExitPhase::Finish) {
        guard.phase = LocalExitPhase::Done;
        let cleanup_failed = guard.failed || local.source_failed.get() || guard.domain_unconfirmed != 0;
        local.state.finish_owner(guard.admitted_unconfirmed, cleanup_failed);
        local.exit_finished.set(true);
        local.exit_running.set(false);
    }
}
fn local_exit_drop_inner<T: ExecutorTimer + 'static>(guard: &mut LocalExitGuard<'_, T>) {
    if matches!(guard.phase, LocalExitPhase::Done) {
        return;
    }
    // 上一真实阶段未正常返回：单调记为失败并清掉局部标记，再从已推进的阶段继续。
    if guard.current_failed {
        guard.current_failed = false;
        guard.failed = true;
    }
    // 已经移出原值的余项由这里继续；不重建同类守卫、不嵌套捕获，首次异常不被替换。
    local_exit_drain_inner(guard);
}
fn local_activity_drop_inner<T: ExecutorTimer + 'static>(guard: &mut LocalActivityGuard<'_, T>) {
    // 此处的 owner／队列借用已经结束；只有确实请求退出且无人执行时才接续真实清理。
    let local = guard.local;
    if !local.exit_requested.get() || local.exit_finished.get() || local.exit_running.get() {
        return;
    }
    // 正在真正结束原 owner 中的重入只归还本次活动：finish_exit 会以 running 标记拒绝第二次进入。
    let _ = local_finish_exit_inner(local, false);
}
fn ready_release_inner(guard: &mut ReadyReleaseGuard<'_>) {
    // 只结算已取走的内部任务队列（internal task queue）；并发的原归属返回（owner return）仍可增加数量。
    let count = std::mem::take(&mut guard.count);
    if count != 0 {
        guard.state.internal.fetch_sub(count, std::sync::atomic::Ordering::Release);
    }
}
#[allow(unused_variables)]
fn local_new_inner<T: ExecutorTimer + 'static>(
    runtime: RuntimeId,
    owner: TaskThread,
    ready: TaskQueue,
    timer: T,
    returns: ReturnQueue,
    registry: TaskRegistry,
    state: Arc<ExecutorState>,
    notifier: ThreadNotifier,
) -> ExecutorLocal<T> {
    // 只做不可失败的字段组装：移动原管理者、保存原所属域身份，空活动表不预先分配。
    let owner_id = owner.id();
    ExecutorLocal {
        runtime,
        owner_id,
        owner: std::cell::RefCell::new(Some(owner)),
        ready: std::cell::RefCell::new(Some(ready)),
        timer: std::cell::RefCell::new(Some(timer)),
        returns: std::cell::RefCell::new(Some(returns)),
        registry: std::cell::RefCell::new(Some(registry)),
        active: std::cell::RefCell::new(std::collections::HashMap::new()),
        active_order: std::cell::RefCell::new(std::collections::VecDeque::new()),
        state,
        notifier,
        exit_requested: std::cell::Cell::new(false),
        exit_running: std::cell::Cell::new(false),
        source_failed: std::cell::Cell::new(false),
        exit_finished: std::cell::Cell::new(false),
    }
}
fn admission_attempt_drop_inner<T: ExecutorTimer + 'static>(
    guard: &mut AdmissionAttemptGuard<'_, T>,
) {
    use pi_async_task::definitions::FutureDropStatus;
    use std::sync::atomic::Ordering::Release;

    // 正常成功已先撤销；只有原生接纳途中展开（原 Rune 尚未释放）才需补账。
    if !guard.armed {
        return;
    }
    guard.armed = false;
    let snapshot = guard.task.snapshot();
    let local = guard.local;
    if snapshot.owner != Some(local.owner_id)
        || !snapshot.admitted
        || snapshot.future_drop != FutureDropStatus::Unconfirmed
    {
        return;
    }
    let id = snapshot.id;
    if local.active.borrow().contains_key(&id) {
        return;
    }
    // 冷路径：先结束 registry 借用（克隆同一原查询），再改动活动表与顺序。
    let query = {
        let Ok(mut borrowed) = local.registry.try_borrow_mut() else {
            return;
        };
        let Some(registry) = borrowed.as_mut() else {
            return;
        };
        registry.query(id).cloned()
    };
    let Some(query) = query else {
        return;
    };
    {
        let mut active = local.active.borrow_mut();
        if active.contains_key(&id) {
            return;
        }
        active.insert(id, ActiveRecord::Interrupted(query));
    }
    local.active_order.borrow_mut().push_back(id);
    // 已接纳且原值析构未确认：与正常接纳一样只Release加一次在途计数。
    local.state.in_flight.fetch_add(1, Release);
}
