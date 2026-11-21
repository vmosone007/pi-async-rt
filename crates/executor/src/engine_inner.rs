#[allow(unused_variables)]
fn executor_new_inner<C, E, I, T, L>(
    clock: C,
    external: E,
    idle: I,
    options: ExecutorOptions,
    make_timer: impl FnOnce(RuntimeId, &C, ThreadNotifier) -> pi_result::Result<T>,
) -> pi_result::Result<DefaultExecutor<C, E, I, T, L>>
where
    C: ExecutorClock,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = C::Instant> + 'static,
    L: LocalReadySubmit,
{
    // 1. 先校验配置（无时钟采样、线程或副作用），失败直返原报告。
    options.validate()?;
    // 2. 原线程停放拥有者与共享通知句柄；一份状态计数。
    let parker = ThreadParker::new();
    let notifier = parker.notifier();
    let state = Arc::new(ExecutorState::new());
    // 3. 构造守卫先就位：任何一步失败，已建立资源按字段顺序释放。
    let mut guard: ExecutorBuildGuard<T> = ExecutorBuildGuard {
        owner: None,
        ready: None,
        timer: None,
        returns: None,
        registry: None,
    };
    let owner = guard.owner.insert(TaskThread::new());
    let owner_id = owner.id();
    guard.ready = Some(TaskQueue::new(owner));
    guard.returns = Some(ReturnQueue::new(
        owner_id, notifier.clone(), Arc::clone(&state), options.maintenance_batch,
    )?);
    guard.registry = Some(TaskRegistry::new());
    // 4. 本地回送路由与恢复：同一返回入口、同一状态下建立调度目标。
    let returns_handle = guard.returns.as_ref().expect("返回来源已就位").handle();
    let route: crate::routing::ReturnRoute<L> = crate::routing::ReturnRoute::new(returns_handle.clone());
    let route_for_id = route.clone();
    let recovery = crate::routing::ReturnRecovery::new(returns_handle, Arc::clone(&state), notifier.clone());
    let schedule = crate::routing::new_schedule(guard.owner.as_ref().expect("所属域已就位"), route, recovery);
    // 5. 工厂取得唯一运行时身份；同一身份发布给路由一次。
    let factory = pi_async_runtime_core::task::TaskFactory::try_new(
        schedule,
        notifier.clone(),
        pi_async_task::definitions::QueueSupport::IntrusiveLocalAndMpsc,
    )?;
    let runtime = pi_async_runtime_core::task::TaskFactory::runtime_id(&factory);
    route_for_id.set_runtime(runtime);
    // 6. 计时来源只构造一次，并核其访问接口与同一身份。
    let timer = guard.timer.insert(make_timer(runtime, &clock, notifier.clone())?);
    let timer_access = timer.timer_access();
    if pi_async_runtime_core::time::TimerAccess::runtime_id(&timer_access) != runtime {
        return Err(pi_result::Error::new(pi_result::ErrorKind::InvalidInput)
            .attach("计时访问（timer access）与原运行时身份不一致"));
    }
    // 7. 共享句柄：外部／空闲来源交各自句柄，计时交出访问与来源句柄。
    let handle = ExecutorHandle {
        factory,
        external: external.handle(),
        idle: idle.handle(),
        timer: timer_access,
        timer_query: timer.handle(),
        state: Arc::clone(&state),
        registry: guard.registry.as_ref().expect("登记已就位").handle(),
        notifier: notifier.clone(),
        owner: owner_id,
        types: PhantomData,
    };
    // 8. 本地设施只移动原 parts，不做额外克隆或新建身份。
    let local = ExecutorLocal::new(
        runtime,
        guard.owner.take().expect("所属域只移动一次"),
        guard.ready.take().expect("就绪队列只移动一次"),
        guard.timer.take().expect("定时来源只移动一次"),
        guard.returns.take().expect("返回来源只移动一次"),
        guard.registry.take().expect("登记只移动一次"),
        Arc::clone(&state),
        notifier,
    );
    drop(guard);
    Ok(DefaultExecutor {
        clock,
        external: Some(external),
        idle: Some(idle),
        local: Rc::new(local),
        handle,
        parker,
        options,
    })
}
#[allow(unused_variables)]
fn executor_build_drop_inner<T: ExecutorTimer + 'static>(guard: &mut ExecutorBuildGuard<T>) {
    // 所属域先退出。发生析构展开时，未取出的字段继续按声明顺序释放。
    drop(guard.owner.take());
    drop(guard.ready.take());
    drop(guard.timer.take());
    drop(guard.returns.take());
    drop(guard.registry.take());
}
#[allow(unused_variables)]
fn executor_begin_drive_inner<C, E, I, T, L>(this: &DefaultExecutor<C, E, I, T, L>) -> DriveLease
where
    C: ExecutorClock,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = C::Instant> + 'static,
    L: LocalReadySubmit,
{
        // 任何用户 poll 前先核本线程，并以一次 CAS 取得独占推进权；失败明确恐慌。
    let current = std::thread::current().id();
    assert_eq!(current, this.parker.owner_thread(),
        "只有原所属线程（owner thread）可以取得推进租用");
    assert!(this.handle.state.try_drive(),
        "同一服务不能被两处同时推进（drive lease already held）");
    DriveLease {
        state: Arc::clone(&this.handle.state),
        runtime: this.handle.factory.runtime_id(),
        thread: current,
        local: PhantomData,
    }
}
#[allow(unused_variables)]
fn drive_lease_drop_inner(lease: &mut DriveLease) {
    // 只归还同一状态的独占推进位一次；不动其它单调关闭事实、不通知、不处理任务。
    // 正常返回与展开都走到这里（租用本身不可克隆、不可跨线程）。
    lease.state.release_drive();
}
#[allow(unused_variables)]
fn executor_once_inner<C, E, I, T, L, A, P, H>(
    this: &mut DefaultExecutor<C, E, I, T, L>,
    admission: &A,
    policy: &mut P,
    hooks: &mut H,
) -> pi_result::Result<TurnResult>
where
    C: ExecutorClock,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = C::Instant> + 'static,
    L: LocalReadySubmit,
    A: AdmissionRule,
    P: IdlePolicy,
    H: ExecutorHooks,
{
        // 活动范围内不嵌套另一次 poll；先建 Scope 再取一次租用推进一轮。
    let _scope = pi_async_runtime_core::context::record::DriveScope::enter();
    let lease = this.begin_drive();
    // 单轮：不在此循环、不 park；用户 Future／同步 hook／Drop 可能阻塞本次调用。
    this.turn_with_lease(&lease, admission, policy, hooks)
}
#[allow(unused_variables)]
fn executor_run_inner<C, E, I, T, L, A, P, H>(
    this: &mut DefaultExecutor<C, E, I, T, L>,
    admission: &A,
    policy: &mut P,
    hooks: &mut H,
) -> pi_result::Result<()>
where
    C: ExecutorClock,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = C::Instant> + 'static,
    L: LocalReadySubmit,
    A: AdmissionRule,
    P: IdlePolicy,
    H: ExecutorHooks,
{
        // 同一活动范围与同一租用贯穿整段循环；不重复建 Scope/Lease。
    let _scope = pi_async_runtime_core::context::record::DriveScope::enter();
    let lease = this.begin_drive();
    loop {
        // 首错原报告直返；关闭以 Ok 结束，不把关闭当错误。
        let turn = this.turn_with_lease(&lease, admission, policy, hooks)?;
        if turn.service_closed
            || matches!(this.handle.state.status(), pi_async_runtime_core::definitions::RuntimeStatus::Closed)
        {
            return Ok(());
        }
        // 需要等待时交原停放组合（不含根）；主动通知与超时由原实现处理。
        this.park_with_lease(&lease, admission, &turn, None)?;
    }
}
fn executor_turn_inner<C, E, I, T, L, A, P, H>(
    this: &mut DefaultExecutor<C, E, I, T, L>,
    lease: &DriveLease,
    admission: &A,
    policy: &mut P,
    hooks: &mut H,
) -> pi_result::Result<TurnResult>
where
    C: ExecutorClock,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = C::Instant> + 'static,
    L: LocalReadySubmit,
    A: AdmissionRule,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    use crate::policy::WorkReadiness;
    use pi_async_runtime_core::definitions::IdleState;
    use pi_async_task::definitions::{CancelRequestStatus, TaskPhase};
    use pi_async_task_queue::{
        idle::{IdleQueueError, IdleTake, IdleTaskQueue}, ExternalTaskQueue, QueueCleanupProgress,
    };
    use std::{
        num::NonZeroUsize,
        sync::atomic::Ordering::{Acquire, Release},
    };

    assert!(Arc::ptr_eq(&lease.state, &this.handle.state), "推进权必须来自原执行器");
    assert_eq!(lease.thread, this.parker.owner_thread(), "推进权必须属于原线程");
    let mut runs = 0;
    // 回调（callback）及清理可能请求关闭。保留本轮已有的运行次数，再停止后续业务。
    macro_rules! finish_if_closed {
        () => {
            match this.handle.state.status() {
                RuntimeStatus::Open => {},
                status => {
                    if status == RuntimeStatus::Closing {
                        this.close_service()?;
                    }
                    return Ok(TurnResult {
                        runs,
                        immediate_work: false,
                        service_closed: true,
                        sleep_for: None,
                        readiness: WorkReadiness::default(),
                    });
                }
            }
        };
    }
    finish_if_closed!();
    this.parker.take_notification();

    ExecutorExitGuard::during_cleanup(this, |executor| {
        executor.local.maintain(executor.options.maintenance_batch.get());
    });
    finish_if_closed!();
    let (idle_cleanup, maintenance_ready) = ExecutorExitGuard::during_cleanup(this, |executor| {
        executor.handle.idle.process_cleanup_with_readiness()
    })?;
    let idle_cleanup_pending = matches!(idle_cleanup, QueueCleanupProgress::MoreWork);
    finish_if_closed!();
    // 回送中的原值、所属域借用和内部计数均先于整体退出结清。
    let returns = ExecutorExitGuard::during_cleanup(this, |executor| executor.import_returns())?;
    finish_if_closed!();
    let timers = this.service_timers(hooks)?;
    runs += timers.runs;
    finish_if_closed!();
    let external = this.import_external(admission)?;
    finish_if_closed!();

    // 队列借用（borrow）结束后才执行任务（Task）或调用钩子（hook）。
    let next = this.local.ready.borrow_mut().as_mut().and_then(TaskQueue::pop_front);
    if let Some(runnable) = next {
        this.handle.state.internal.fetch_sub(1, Release);
        if timers.deferred_task == Some(runnable.id()) {
            ExecutorExitGuard::during_cleanup(this, |executor| -> pi_result::Result<()> {
                let state = runnable.snapshot();
                if state.cancellation == CancelRequestStatus::Recorded
                    || matches!(state.phase, TaskPhase::Exiting | TaskPhase::Ended)
                {
                    let _ = executor.prepare_for_queue(runnable)?;
                } else {
                    executor.requeue_selected(runnable, false, "无法将原定时任务放回内部任务队列")?;
                }
                Ok(())
            })?;
        } else {
            let step = this.run_task(runnable, hooks)?;
            runs += usize::from(step.ran);
        }
        finish_if_closed!();
    }

    let state = AdmissionState {
        runtime: this.handle.factory.runtime_id(),
        status: this.handle.state.status(),
        internal_tasks: this.handle.state.internal.load(Acquire),
        in_flight_tasks: this.handle.state.in_flight.load(Acquire),
    };
    let can_admit = admission.admits_external(&state);
    finish_if_closed!();
    let readiness = WorkReadiness {
        external_deferred: external.deferred,
        returns_deferred: returns.deferred,
        maintenance_ready,
        external_count: this.handle.external.len(),
        internal_count: state.internal_tasks,
    };
    let snapshot = this.snapshot(can_admit, timers.pending, idle_cleanup_pending, readiness);
    let idle = policy.is_idle(&snapshot);
    this.handle.state.record_idle(if idle { IdleState::Idle } else { IdleState::NotIdle });
    finish_if_closed!();

    if idle {
        let taken = ExecutorExitGuard::during_cleanup(this, |executor| {
            executor.handle.idle.try_take_batch(NonZeroUsize::MIN)
        });
        match taken {
            Ok(IdleTake::Ready(tasks)) => {
                if this.handle.state.status() == RuntimeStatus::Open {
                    for task in tasks {
                        let step = this.run_task(task, hooks)?;
                        runs += usize::from(step.ran);
                        finish_if_closed!();
                    }
                } else {
                    ExecutorExitGuard::drop_detached(this, tasks);
                }
            }
            Ok(IdleTake::Empty | IdleTake::WouldBlock | IdleTake::Busy | IdleTake::Closed) => {}
            Err(IdleQueueError::Backend(error)) => return Err(error),
            Err(cause) => {
                use pi_result::ErrorKind;
                let kind = match cause {
                    IdleQueueError::Closed => ErrorKind::InvalidState,
                    IdleQueueError::Busy => ErrorKind::Busy,
                    _ => ErrorKind::ResourceExhausted,
                };
                return Err(pi_result::Error::new(kind).attach("空闲任务队列取出失败"));
            }
        }
    }
    finish_if_closed!();
    let snapshot = this.snapshot(can_admit, timers.pending, idle_cleanup_pending, readiness);
    let sleep_for = policy.sleep_duration(&snapshot);
    let immediate_work = runs != 0
        || snapshot.ready_tasks != 0
        || snapshot.timer_work_pending
        || snapshot.maintenance_ready
        || (snapshot.can_admit_external && snapshot.external_tasks != 0 && !snapshot.external_deferred)
        || this.parker.is_notified();
    finish_if_closed!();
    Ok(TurnResult {
        runs,
        immediate_work,
        service_closed: false,
        sleep_for,
        readiness,
    })
}
fn executor_snapshot_inner<C, E, I, T, L>(
    this: &DefaultExecutor<C, E, I, T, L>,
    admit: bool,
    timer_pending: bool,
    maintenance: bool,
    readiness: crate::policy::WorkReadiness,
) -> ExecutorSnapshot<'_>
where
    C: ExecutorClock,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = C::Instant> + 'static,
    L: LocalReadySubmit,
{
    use std::sync::atomic::Ordering::Acquire;

    // 只取已有持有样本：ready 长度一次短借、内部/在途计数各一次 Acquire、状态与空闲一次读取。
    let ready_tasks = this
        .local
        .ready
        .borrow()
        .as_ref()
        .map(|queue| queue.len())
        .unwrap_or(0);
    let timer_registrations = this
        .local
        .timer
        .borrow()
        .as_ref()
        .map(|timer| pi_async_task_queue::TimerTaskQueue::len(timer))
        .unwrap_or(0);
    ExecutorSnapshot {
        runtime: this.handle.factory.runtime_id(),
        status: this.handle.state.status(),
        external_tasks: readiness.external_count,
        internal_tasks: this.handle.state.internal.load(Acquire),
        ready_tasks,
        external_deferred: readiness.external_deferred,
        returns_deferred: readiness.returns_deferred,
        timer_registrations,
        idle_tasks: pi_async_task_queue::IdleTaskQueue::len(&this.handle.idle),
        in_flight_tasks: this.handle.state.in_flight.load(Acquire),
        registry: &this.handle.registry,
        can_admit_external: admit,
        timer_work_pending: timer_pending,
        maintenance_pending: maintenance,
        maintenance_ready: readiness.maintenance_ready,
        last_idle_state: this.handle.state.last_idle_state(),
    }
}
#[allow(unused_variables)]
fn executor_returns_inner<C, E, I, T, L>(
    this: &mut DefaultExecutor<C, E, I, T, L>,
) -> pi_result::Result<ImportResult>
where
    C: ExecutorClock,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = C::Instant> + 'static,
    L: LocalReadySubmit,
{
        use crate::{local::PreparedTask, returns::ReturnTake};

    let mut deferred = false;
    let mut retries = this.options.would_block_retries;
    for _ in 0..this.options.maintenance_batch.get() {
        let take = loop {
            if this.handle.state.status() != RuntimeStatus::Open {
                return Ok(ImportResult { deferred });
            }
            let take = {
                let mut returns = this.local.returns.borrow_mut();
                match returns.as_mut() {
                    Some(queue) => queue.try_take(),
                    None => ReturnTake::Closed,
                }
            };
            if matches!(take, ReturnTake::WouldBlock) && retries != 0 {
                retries -= 1;
                continue;
            }
            break take;
        };
        let task = match take {
            ReturnTake::Ready(task) => task,
            ReturnTake::WouldBlock | ReturnTake::Busy => {
                deferred = true;
                break;
            }
            ReturnTake::Empty | ReturnTake::Closed => break,
        };
        // 从返回来源独立取出后先保管一份内部计数，成功入本地队列才移交。
        let mut transfer = InternalTransferGuard {
            state: Arc::clone(&this.handle.state),
            count: 1,
        };
        if this.handle.state.status() != RuntimeStatus::Open {
            drop(task);
            break;
        }
        match this.local.selected(task) {
            Ok(PreparedTask::Business(runnable, registration)) => {
                drop(registration);
                if this.handle.state.status() != RuntimeStatus::Open {
                    drop(runnable);
                    break;
                }
                let pushed = {
                    let mut ready = this.local.ready.borrow_mut();
                    match ready.as_mut() {
                        Some(queue) => queue.push_back(runnable),
                        None => Err(pi_async_task_queue::QueuePushError {
                            task: runnable,
                            cause: pi_async_task_queue::QueuePushErrorKind::Closed,
                        }),
                    }
                };
                if pushed.is_ok() {
                    // 入队后由队列携带该份计数；守卫不再重复减。
                    transfer.count = 0;
                }
                drop(pushed);
            }
            Ok(PreparedTask::Cleanup(runnable)) => {
                // 退出分支：只在所属域内直接运行退出，不接纳、不计数。
                let mut pending = Some(runnable);
                let _ = this.local.with_owner(|owner| {
                    let runnable = pending.take().expect("原任务只取出一次");
                    runnable.run(owner)
                });
                drop(pending.take());
            }
            Ok(PreparedTask::Settled(runnable)) => drop(runnable),
            Err(error) => return Err(error),
        }
        drop(transfer);
    }
    // 停放／关闭时导入不构成忙等；本步只报告是否已有暂不可取样本。
    Ok(ImportResult { deferred })
}
fn executor_external_inner<C, E, I, T, L, A>(
    this: &mut DefaultExecutor<C, E, I, T, L>,
    admission: &A,
) -> pi_result::Result<ImportResult>
where
    C: ExecutorClock,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = C::Instant> + 'static,
    L: LocalReadySubmit,
    A: AdmissionRule,
{
    use pi_async_task_queue::{BatchAttempt, ExternalTaskQueue, QueuePushError, QueuePushErrorKind};
    use std::sync::atomic::Ordering::Acquire;

    // 只取当前样本交原接纳规则；不允许时本轮不动外部来源。
    let state = AdmissionState {
        runtime: this.handle.factory.runtime_id(),
        status: this.handle.state.status(),
        internal_tasks: this.handle.state.internal.load(Acquire),
        in_flight_tasks: this.handle.state.in_flight.load(Acquire),
    };
    if !admission.admits_external(&state) {
        return Ok(ImportResult { deferred: false });
    }
    // 取出及本批移交共用同一次退出守卫。展开时，本批余项先于外层守卫析构。
    ExecutorExitGuard::during_cleanup(this, |executor| {
        let retries = executor.options.would_block_retries;
        for attempt in 0..=retries {
            if executor.handle.state.status() != RuntimeStatus::Open {
                break;
            }
            let batch = match executor.handle.external.try_take_batch() {
                BatchAttempt::Ready(batch) => batch,
                BatchAttempt::WouldBlock if attempt < retries => continue,
                BatchAttempt::WouldBlock | BatchAttempt::Busy => return Ok(ImportResult { deferred: true }),
                BatchAttempt::Empty | BatchAttempt::Closed => break,
            };
            for task in batch {
                if executor.handle.state.status() != RuntimeStatus::Open {
                    break;
                }
                let Some(task) = executor.prepare_for_queue(task)? else {
                    continue;
                };
                if executor.handle.state.status() != RuntimeStatus::Open {
                    break;
                }
                let id = task.id();
                let pushed = {
                    let mut ready = executor.local.ready.borrow_mut();
                    match ready.as_mut() {
                        Some(queue) => executor.handle.state.push_back(queue, task),
                        None => Err(QueuePushError { task, cause: QueuePushErrorKind::Closed }),
                    }
                };
                if let Err(error) = pushed {
                    return Err(pi_result::Error::new(pi_result::ErrorKind::InvalidState)
                        .attach("无法交付已从外部任务队列取出的任务")
                        .attach_opaque((id, error.cause)));
                }
            }
            break;
        }
        Ok(ImportResult { deferred: false })
    })
}
fn executor_timers_inner<C, E, I, T, L, H>(
    this: &mut DefaultExecutor<C, E, I, T, L>,
    hooks: &mut H,
) -> pi_result::Result<TimerStep>
where
    C: ExecutorClock,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = C::Instant> + 'static,
    L: LocalReadySubmit,
    H: ExecutorHooks,
{
    use crate::drive::TimerDelivery;
    use pi_async_task_queue::{TimerQueueError, TimerTake};

    let idle = || TimerStep { runs: 0, pending: false, deferred_task: None };
    let has_timer = ExecutorExitGuard::during_cleanup(this, |executor| -> pi_result::Result<bool> {
        let mut timer = executor.local.timer.borrow_mut();
        let Some(timer) = timer.as_mut() else {
            return Ok(false);
        };
        timer.process_cancel_retries()?;
        if executor.handle.state.status() == RuntimeStatus::Open {
            timer.process_cleanup()?;
        }
        Ok(true)
    })?;
    if !has_timer || this.handle.state.status() != RuntimeStatus::Open {
        return Ok(idle());
    }
    let now = this.clock.now()?;
    if this.handle.state.status() != RuntimeStatus::Open {
        return Ok(idle());
    }
    // 到期序列（due batch）独立拥有原值。释放来源借用后才交付信号或运行任务。
    let Some((single, batch)) = ExecutorExitGuard::during_cleanup(this, |executor| -> pi_result::Result<_> {
        let mut source = executor.local.timer.borrow_mut();
        let Some(timer) = source.as_mut() else {
            return Ok(None);
        };
        timer.advance(&now).map_err(TimerQueueError::into_report)?;
        if executor.handle.state.status() != RuntimeStatus::Open {
            return Ok(None);
        }
        let taken = match executor.options.timer_delivery {
            TimerDelivery::One => {
                let item = match timer.try_take_due_one().map_err(TimerQueueError::into_report)? {
                    TimerTake::Ready(item) => Some(item),
                    _ => None,
                };
                (item, None)
            }
            TimerDelivery::All => {
                let entries = match timer.try_take_due_all().map_err(TimerQueueError::into_report)? {
                    TimerTake::Ready(items) => Some(items),
                    _ => None,
                };
                (None, entries)
            }
        };
        Ok(Some(taken))
    })? else {
        return Ok(idle());
    };
    if this.handle.state.status() != RuntimeStatus::Open {
        ExecutorExitGuard::drop_detached(this, (single, batch));
        return Ok(idle());
    }
    let mut step = idle();
    if let Some(entry) = single {
        let task = ExecutorExitGuard::during_cleanup(this, |_| entry.into_due());
        if let Some(task) = task {
            if this.handle.state.status() == RuntimeStatus::Open {
                let task_step = this.run_task(task, hooks)?;
                step.runs = usize::from(task_step.ran);
                step.deferred_task = task_step.deferred_task;
            } else {
                ExecutorExitGuard::drop_detached(this, task);
            }
        }
    }
    if let Some(entries) = batch {
        // 同一作用域保管到期原值和已准备任务，通知异常时先清理批次余项。
        ExecutorExitGuard::during_cleanup(this, |executor| -> pi_result::Result<()> {
            let mut prepared = Vec::new();
            let mut entries = entries.into_iter();
            while executor.handle.state.status() == RuntimeStatus::Open {
                let Some(entry) = entries.next() else {
                    break;
                };
                if executor.handle.state.status() != RuntimeStatus::Open {
                    return Ok(());
                }
                if let Some(task) = entry.into_due() {
                    if executor.handle.state.status() != RuntimeStatus::Open {
                        return Ok(());
                    }
                    if let Some(task) = executor.prepare_for_queue(task)? {
                        prepared.push(task);
                    }
                }
            }
            if executor.handle.state.status() == RuntimeStatus::Open {
                for task in prepared.into_iter().rev() {
                    if !executor.requeue_selected(task, true, "无法交付到期定时任务")? {
                        return Ok(());
                    }
                    step.pending = true;
                }
            }
            Ok(())
        })?;
    }
    // 到期原值已经交付。后续重查或计算剩余时长的错误不改变原值的结果。
    if this.handle.state.status() == RuntimeStatus::Open {
        let next = {
            let timer = this.local.timer.borrow();
            match timer.as_ref() {
                Some(timer) => timer.next_check_at(&now).map_err(TimerQueueError::into_report)?,
                None => None,
            }
        };
        if let Some(next) = next {
            if this.handle.state.status() == RuntimeStatus::Open {
                step.pending |= this.clock.remaining(&now, &next)?.is_zero();
            }
        }
    }
    Ok(step)
}
fn executor_task_inner<C, E, I, T, L, H>(
    this: &mut DefaultExecutor<C, E, I, T, L>,
    task: Runnable,
    hooks: &mut H,
) -> pi_result::Result<TaskStep>
where
    C: ExecutorClock,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = C::Instant> + 'static,
    L: LocalReadySubmit,
    H: ExecutorHooks,
{
    use crate::hooks::{ExecutorEvent, TaskAction, TaskInfo};
    use crate::local::PreparedTask;
    use pi_async_task::definitions::{CancelRequestStatus, TaskPhase};

    let run = |executor: &mut DefaultExecutor<C, E, I, T, L>, runnable: Runnable| {
        // 原生 run 也会析构已结束的计算和无人领取的输出。释放原所属域借用后再接续关闭。
        ExecutorExitGuard::during_cleanup(executor, |executor| {
            let id = runnable.id();
            match executor.local.with_owner(|owner| runnable.run(owner))? {
                Ok(outcome) => Ok(outcome),
                Err(error) => {
                    let cause = error.cause;
                    drop(error.runnable);
                    Err(pi_result::Error::new(pi_result::ErrorKind::InvalidState)
                        .attach("原任务未能在所属域内运行")
                        .attach_opaque((id, cause)))
                }
            }
        })
    };

    let selected = this.local.selected(task)?;
    match selected {
        // 已结算：借用外放弃原值，不重试清理。
        PreparedTask::Settled(runnable) => {
            ExecutorExitGuard::during_cleanup(this, |_| drop(runnable));
            Ok(TaskStep { ran: false, deferred_task: None })
        }
        // 退出分支：只在所属域内直接运行退出，不接纳、不触达业务钩子与业务计数。
        PreparedTask::Cleanup(runnable) => {
            run(this, runnable)?;
            Ok(TaskStep { ran: false, deferred_task: None })
        }
        PreparedTask::Business(runnable, registration) => {
            let info = TaskInfo::new(&registration);
            let skipped = TaskStep { ran: false, deferred_task: None };
            let needs_cleanup = || {
                let snapshot = registration.snapshot();
                snapshot.cancellation == CancelRequestStatus::Recorded
                    || matches!(snapshot.phase, TaskPhase::Exiting | TaskPhase::Ended)
            };
            if this.handle.state.status() != RuntimeStatus::Open {
                ExecutorExitGuard::drop_detached(this, (runnable, registration));
                return Ok(skipped);
            }
            hooks.on_event(ExecutorEvent::BeforePoll, &info);
            if this.handle.state.status() != RuntimeStatus::Open {
                ExecutorExitGuard::drop_detached(this, (runnable, registration));
                return Ok(skipped);
            }
            if needs_cleanup() {
                run(this, runnable)?;
                return Ok(skipped);
            }
            let action = hooks.before_poll(&info);
            if this.handle.state.status() != RuntimeStatus::Open {
                ExecutorExitGuard::drop_detached(this, (runnable, registration));
                return Ok(skipped);
            }
            if needs_cleanup() {
                run(this, runnable)?;
                return Ok(skipped);
            }
            match action {
                TaskAction::Defer => {
                    let id = runnable.id();
                    let queued = ExecutorExitGuard::during_cleanup(this, |executor| {
                        executor.requeue_selected(runnable, false, "无法将延后的原任务放回内部任务队列")
                    })?;
                    Ok(TaskStep { ran: false, deferred_task: queued.then_some(id) })
                }
                TaskAction::Cancel => {
                    ExecutorExitGuard::during_cleanup(this, |_| drop(runnable));
                    Ok(skipped)
                }
                TaskAction::Poll => {
                    let outcome = run(this, runnable)?;
                    if this.handle.state.status() == RuntimeStatus::Open {
                        hooks.on_event(ExecutorEvent::AfterRun(outcome), &info);
                    }
                    Ok(TaskStep { ran: true, deferred_task: None })
                }
            }
        }
    }
}
fn executor_requeue_selected_inner<C, E, I, T, L>(
    this: &DefaultExecutor<C, E, I, T, L>,
    task: Runnable,
    front: bool,
    description: &'static str,
) -> pi_result::Result<bool>
where
    C: ExecutorClock,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = C::Instant> + 'static,
    L: LocalReadySubmit,
{
    use pi_async_task_queue::QueuePushErrorKind;

    let id = task.id();
    let Err(error) = this.local.try_push(task, front) else {
        return Ok(true);
    };
    // 独立确认真实服务状态。内部队列无故关闭或其它拒绝仍沿原错误返回。
    if error.cause == QueuePushErrorKind::Closed && this.handle.state.status() != RuntimeStatus::Open {
        drop(error.task);
        Ok(false)
    } else {
        Err(pi_result::Error::new(pi_result::ErrorKind::InvalidState)
            .attach(description)
            .attach_opaque((id, error.cause)))
    }
}
fn executor_park_inner<C, E, I, T, L, A>(
    this: &mut DefaultExecutor<C, E, I, T, L>,
    lease: &DriveLease,
    admission: &A,
    last: &TurnResult,
    root: Option<&RootWakeState>,
) -> pi_result::Result<()>
where
    C: ExecutorClock,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = C::Instant> + 'static,
    L: LocalReadySubmit,
    A: AdmissionRule,
{
    use pi_async_task_queue::{ExternalTaskQueue, TimerQueueError};
    use std::sync::atomic::Ordering::Acquire;

    assert!(Arc::ptr_eq(&lease.state, &this.handle.state), "推进权必须来自原执行器");
    let Some(suggested) = last.sleep_for else {
        return Ok(());
    };
    if suggested.is_zero() || last.immediate_work || this.handle.state.status() != RuntimeStatus::Open {
        return Ok(());
    }
    let now = this.clock.now()?;
    let timer_next = {
        let timer = this.local.timer.borrow();
        match timer.as_ref() {
            Some(timer) => timer.next_check_at(&now).map_err(TimerQueueError::into_report)?,
            None => None,
        }
    };
    let mut limit = suggested.min(this.options.max_sleep);
    if let Some(next) = timer_next {
        limit = limit.min(this.clock.remaining(&now, &next)?);
    }
    if limit.is_zero() {
        return Ok(());
    }
    let state = AdmissionState {
        runtime: lease.runtime,
        status: this.handle.state.status(),
        internal_tasks: this.handle.state.internal.load(Acquire),
        in_flight_tasks: this.handle.state.in_flight.load(Acquire),
    };
    let can_admit = admission.admits_external(&state);
    let external = this.handle.external.len();
    let internal = this.handle.state.internal.load(Acquire);
    let ready = this.local.ready.borrow().as_ref().is_some_and(|queue| !queue.is_empty());
    if ready
        || (internal != 0 && (!last.readiness.returns_deferred || internal != last.readiness.internal_count))
        || (can_admit && external != 0 && (!last.readiness.external_deferred || external != last.readiness.external_count))
        || this.handle.state.status() != RuntimeStatus::Open
        || root.is_some_and(RootWakeState::is_notified)
        || this.parker.is_notified()
    {
        return Ok(());
    }
    // 最后检查后不再调用策略或用户代码。间隙内的通知由原线程许可接续。
    this.parker.park_timeout(limit);
    Ok(())
}
#[allow(unused_variables)]
fn executor_close_inner<C, E, I, T, L>(
    this: &mut DefaultExecutor<C, E, I, T, L>,
) -> pi_result::Result<()>
where
    C: ExecutorClock,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = C::Instant> + 'static,
    L: LocalReadySubmit,
{
    // 停收后各来源各推进一步。首个恐慌（panic）展开时，同一守卫接续尚未清理的来源。
    let _ = this.handle.state.request_close(&this.handle.notifier);
    let mut guard = ExecutorExitGuard {
        executor: this,
        phase: ExecutorExitPhase::Idle,
        current_failed: true,
        failed: false,
    };
    let mut first: Option<pi_result::Error> = None;
    if let Some(mut external) = guard.executor.external.take() {
        if let Err(error) = external.close() {
            first = Some(error);
        }
    }
    guard.phase = ExecutorExitPhase::Local;
    if let Some(mut idle) = guard.executor.idle.take() {
        if let Err(error) = idle.close() {
            if first.is_none() {
                first = Some(error);
            }
        }
    }
    let timer_close = {
        let mut timer = guard.executor.local.timer.borrow_mut();
        timer.as_mut().map(|source| source.close())
    };
    if let Some(Err(error)) = timer_close {
        if first.is_none() {
            first = Some(error);
        }
    }
    guard.current_failed = false;
    guard.phase = ExecutorExitPhase::Done;
    let finished = guard.executor.local.finish_exit(first.is_some());
    match first {
        Some(error) => Err(error),
        None if finished => Ok(()),
        // 未完成不伪报成功；退出意图已记录，由合法所属域后续接续。
        None => Err(pi_result::Error::new(pi_result::ErrorKind::InvalidState)
            .attach("本地域（local domain）尚未完成退出")),
    }
}
#[allow(unused_variables)]
fn executor_drop_inner<C, E, I, T, L>(this: &mut DefaultExecutor<C, E, I, T, L>)
where
    C: ExecutorClock,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = C::Instant> + 'static,
    L: LocalReadySubmit,
{
    // 先记录关闭请求（close request），再由各拥有者按原规则释放。
    let _ = this.handle.state.request_close(&this.handle.notifier);
    let mut guard = ExecutorExitGuard {
        executor: this,
        phase: ExecutorExitPhase::External,
        current_failed: false,
        failed: false,
    };
    guard.drain();
}
#[allow(unused_variables)]
fn executor_exit_drain_inner<C, E, I, T, L>(guard: &mut ExecutorExitGuard<'_, C, E, I, T, L>)
where
    C: ExecutorClock,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = C::Instant> + 'static,
    L: LocalReadySubmit,
{
    let executor = &mut *guard.executor;
    // 1. 普通外部来源管理者：交出唯一持有，由其自身停收与清理。
    if matches!(guard.phase, ExecutorExitPhase::External) {
        guard.phase = ExecutorExitPhase::Idle;
        guard.current_failed = true;
        let external = executor.external.take();
        drop(external);
        guard.current_failed = false;
    }
    // 2. 空闲来源管理者：同上。
    if matches!(guard.phase, ExecutorExitPhase::Idle) {
        guard.phase = ExecutorExitPhase::Local;
        guard.current_failed = true;
        let idle = executor.idle.take();
        drop(idle);
        guard.current_failed = false;
    }
    // 3. 本地设施：把已确定的失败事实交给真实域退出；活动借用仍存在时由活动守卫或最后 Rc 接续，
    //    不以 owner 缺失当作完成。
    if matches!(guard.phase, ExecutorExitPhase::Local) {
        guard.phase = ExecutorExitPhase::Done;
        let _ = executor.local.finish_exit(guard.failed);
    }
}
#[allow(unused_variables)]
fn executor_exit_drop_inner<C, E, I, T, L>(guard: &mut ExecutorExitGuard<'_, C, E, I, T, L>)
where
    C: ExecutorClock,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = C::Instant> + 'static,
    L: LocalReadySubmit,
{
    if matches!(guard.phase, ExecutorExitPhase::Done) {
        return;
    }
    if matches!(guard.phase, ExecutorExitPhase::IfClosing) {
        if guard.executor.handle.state.status() == RuntimeStatus::Open {
            return;
        }
        guard.phase = ExecutorExitPhase::External;
    }
    // 上一真实阶段异常未返回：单调记失败并清局部标记，再从已推进阶段继续。
    if guard.current_failed {
        guard.current_failed = false;
        guard.failed = true;
    }
    // 已经移走原值的余项由同一排水继续；不递归装第二个同类守卫、不等待。
    guard.drain();
}
#[allow(unused_variables)]
fn internal_transfer_drop_inner(guard: &mut InternalTransferGuard) {
    // 只归还本份保管的欠账：先取走数量，非零时以一次Release减原计数。
    // 成功本地入队会把count置0，因此这里不会重复减；不清零整个internal，也不循环。
    let count = std::mem::take(&mut guard.count);
    if count != 0 {
        guard
            .state
            .internal
            .fetch_sub(count, std::sync::atomic::Ordering::Release);
    }
}
#[allow(unused_variables)]
fn executor_prepare_inner<C, E, I, T, L>(
    this: &DefaultExecutor<C, E, I, T, L>,
    task: Runnable,
) -> pi_result::Result<Option<Runnable>>
where
    C: ExecutorClock,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = C::Instant> + 'static,
    L: LocalReadySubmit,
{
        // 来源借用已在调用者处结束；这里只经本地选择三分。
    let selected = this.local.selected(task);
    match selected {
        Ok(crate::local::PreparedTask::Business(runnable, registration)) => {
            // 登记主记录仍在活动表；此处只归还本份副持有，不重复加计数。
            drop(registration);
            Ok(Some(runnable))
        }
        Ok(crate::local::PreparedTask::Cleanup(runnable)) => {
            // 退出分支：只在所属域内直接运行退出，不接纳、不触达业务钩子与业务计数。
            let mut pending = Some(runnable);
            let ran = this.local.with_owner(|owner| {
                let runnable = pending.take().expect("原任务只取出一次");
                runnable.run(owner)
            });
            match ran {
                Ok(_) => Ok(None),
                Err(_) => {
                    // 无法进入所属域：在合法路径放弃同一原值并保留原分类。
                    drop(pending.take());
                    Ok(None)
                }
            }
        }
        Ok(crate::local::PreparedTask::Settled(runnable)) => {
            // 已结算：借用外放弃原值，不重试清理。
            drop(runnable);
            Ok(None)
        }
        Err(error) => Err(error),
    }
}
