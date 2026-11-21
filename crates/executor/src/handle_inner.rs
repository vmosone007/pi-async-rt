#[allow(unused_variables)]
fn handle_clone_inner<
    E: ExternalTaskQueue + Clone,
    I: IdleTaskQueue + Clone,
    T: ExecutorTimer + 'static,
    L: LocalReadySubmit,
>(
    this: &ExecutorHandle<E, I, T, L>,
) -> ExecutorHandle<E, I, T, L> {
    // 逐项克隆既有持有；不产生新RuntimeId／新来源，也不要求T或L可克隆。
    ExecutorHandle {
        factory: this.factory.clone(),
        external: this.external.clone(),
        idle: this.idle.clone(),
        timer: this.timer.clone(),
        timer_query: this.timer_query.clone(),
        state: Arc::clone(&this.state),
        registry: this.registry.clone(),
        notifier: this.notifier.clone(),
        owner: this.owner,
        types: PhantomData,
    }
}
fn handle_create_inner<
    E: ExternalTaskQueue + Clone,
    I: IdleTaskQueue + Clone,
    T: ExecutorTimer + 'static,
    L: LocalReadySubmit,
    F,
    M,
>(
    this: &ExecutorHandle<E, I, T, L>,
    future: F,
    config: TaskConfig<M>,
) -> pi_result::RawResult<
    (UnsubmittedTask, TaskHandle<ThreadNotifier, F::Output, M>),
    CreateError<F, M>,
>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    M: Send + Sync + 'static,
{
    // 先创建原任务，成功后才登记本份只读查询；不提交、不接纳、不轮询（poll）。
    let (task, handle) = this.factory.create(future, config)?;
    this.registry.register(handle.control_handle().query_handle());
    Ok((task, handle))
}
fn handle_submit_inner<
    E: ExternalTaskQueue + Clone,
    I: IdleTaskQueue + Clone,
    T: ExecutorTimer + 'static,
    L: LocalReadySubmit,
>(
    this: &ExecutorHandle<E, I, T, L>,
    task: UnsubmittedTask,
    idle: bool,
) -> pi_result::RawResult<(), SubmitError> {
    // 提交前先取一次状态样本：已停收／已关闭不进入来源，直接返同一原任务与该来源的关闭原因。
    let status = this.state.status();
    let closed = matches!(status, RuntimeStatus::Closing | RuntimeStatus::Closed);
    let external = this.external.clone();
    let idle_queue = this.idle.clone();
    let notifier = this.notifier.clone();
    this.factory.submit(
        task,
        if idle { SubmissionMode::Idle } else { SubmissionMode::Ordinary },
        |runnable| {
            if closed {
                let cause = if idle {
                    SubmitErrorKind::Idle(pi_async_task_queue::idle::IdleQueueError::Closed)
                } else {
                    SubmitErrorKind::External(pi_async_task_queue::external::ExternalPushErrorKind::Closed)
                };
                return Err((runnable, cause));
            }
            // 只在真正尝试来源时启用通知守卫；正常返回先撤销，展开时由守卫补一次检查。
            let mut guard = SourceNotifyGuard { notifier: &notifier, armed: true };
            let result = if idle {
                idle_queue.try_push(runnable).map_err(|error| {
                    (error.task, SubmitErrorKind::Idle(error.cause))
                })
            } else {
                external.try_push(runnable).map_err(|error| {
                    (error.task, SubmitErrorKind::External(error.cause))
                })
            };
            guard.armed = false;
            result
        },
    )
}
fn handle_create_local_inner<
    E: ExternalTaskQueue + Clone,
    I: IdleTaskQueue + Clone,
    T: ExecutorTimer + 'static,
    L: LocalReadySubmit,
    F,
    M,
>(
    this: &ExecutorHandle<E, I, T, L>,
    local: &ExecutorLocal<T>,
    future: F,
    config: TaskConfig<M>,
) -> pi_result::RawResult<
    (
        UnsubmittedTask,
        TaskHandle<ThreadNotifier, F::Output, M, LocalOutput>,
    ),
    CreateError<F, M>,
>
where
    F: Future + 'static,
    F::Output: 'static,
    M: Send + Sync + 'static,
{
    // 先核同一原服务；不同运行时不下移，原值与配置直返并标明本地上下文原因。
    if local.runtime_id() != this.factory.runtime_id() {
        return Err(CreateError {
            future,
            config,
            cause: CreateErrorKind::LocalContext(LocalRuntimeAccessError::TypeMismatch),
        });
    }
    // 进入前保原值：取不到同源所属域（或已关闭）时返还同一份输入。
    let mut pending = Some((future, config));
    let registry = this.registry.clone();
    let created = local.with_owner(|owner| {
        let (future, config) = pending.take().expect("原值与配置只在此处取出一次");
        this.factory.create_local(owner, future, config)
    });
    match created {
        Ok(result) => {
            let (task, handle) = result?;
            // 成功后立刻登记本份只读查询，再交用户；不提前提交或接纳。
            registry.register(handle.control_handle().query_handle());
            Ok((task, handle))
        }
        Err(_) => {
            let (future, config) = pending.take().expect("未取得所属域时原值仍在本处");
            Err(CreateError {
                future,
                config,
                cause: CreateErrorKind::LocalContext(LocalRuntimeAccessError::ContextUnavailable),
            })
        }
    }
}
fn handle_submit_local_inner<
    E: ExternalTaskQueue + Clone,
    I: IdleTaskQueue + Clone,
    T: ExecutorTimer + 'static,
    L: LocalReadySubmit,
>(
    this: &ExecutorHandle<E, I, T, L>,
    local: &ExecutorLocal<T>,
    task: UnsubmittedTask,
    front: bool,
) -> pi_result::RawResult<(), SubmitError> {
    // 先核同一原服务；不同运行时不下移，原任务原样返还。
    if local.runtime_id() != this.factory.runtime_id() {
        return Err(SubmitError {
            task,
            cause: SubmitErrorKind::LocalContext(LocalRuntimeAccessError::TypeMismatch),
        });
    }
    // 进入前保原待提交值；取不到所属域时返还同一份，不转共享来源、不主动取消。
    let mut pending = Some(task);
    let submitted = local.with_owner(|owner| {
        let task = pending.take().expect("原待提交值只在此处取出一次");
        this.factory.submit(task, SubmissionMode::Local(owner.id()), |runnable| {
            // 直接本地入队：默认尾插，队首优先只影响下一次选择，不抢断正在轮询的异步计算。
            local
                .try_push(runnable, front)
                .map_err(|error| (error.task, SubmitErrorKind::LocalQueue(error.cause)))
        })
    });
    match submitted {
        Ok(result) => result,
        Err(_) => Err(SubmitError {
            task: pending.take().expect("未取得所属域时原任务仍在本处"),
            cause: SubmitErrorKind::LocalContext(LocalRuntimeAccessError::ContextUnavailable),
        }),
    }
}
fn handle_timed_inner<
    E: ExternalTaskQueue + Clone,
    I: IdleTaskQueue + Clone,
    T: ExecutorTimer + 'static,
    L: LocalReadySubmit,
    F,
    M,
>(
    this: &ExecutorHandle<E, I, T, L>,
    future: F,
    delay: Duration,
    config: TaskConfig<M>,
) -> pi_result::RawResult<
    TaskHandle<TimedTaskNotifier<ThreadNotifier, T::Access>, F::Output, M>,
    SpawnError<F, TimedTaskNotifier<ThreadNotifier, T::Access>, M>,
>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    M: Send + Sync + 'static,
{
    // 只委托核心定时派发组合：同一工厂、同一计时访问、原值与配置，以及一次登记只读查询的闭包。
    // 回调在来源可见前完成，保持核心错误、原值、截止一次及键与取消责任，不复制算法。
    let registry = this.registry.clone();
    pi_async_runtime_core::time::task_support::spawn_task_with_query(
        &this.factory,
        this.timer.clone(),
        future,
        delay,
        config,
        move |query| registry.register(query),
    )
}
fn handle_timed_local_inner<
    E: ExternalTaskQueue + Clone,
    I: IdleTaskQueue + Clone,
    T: ExecutorTimer + 'static,
    L: LocalReadySubmit,
    F,
    M,
>(
    this: &ExecutorHandle<E, I, T, L>,
    local: &ExecutorLocal<T>,
    future: F,
    delay: Duration,
    config: TaskConfig<M>,
) -> pi_result::RawResult<
    TaskHandle<TimedTaskNotifier<ThreadNotifier, T::Access>, F::Output, M, LocalOutput>,
    SpawnError<F, TimedTaskNotifier<ThreadNotifier, T::Access>, M, LocalOutput>,
>
where
    F: Future + 'static,
    F::Output: 'static,
    M: Send + Sync + 'static,
{
    // 先保原值与配置：取不到同源所属域时返还同一份输入，不吞错、不重试。
    let mut pending = Some((future, config));
    let registry = this.registry.clone();
    let outcome = local.with_owner(|owner| {
        let (future, config) = pending.take().expect("原值与配置只在此处取出一次");
        pi_async_runtime_core::time::task_support::spawn_local_task_with_query(
            &this.factory,
            owner,
            this.timer.clone(),
            future,
            delay,
            config,
            move |query| registry.register(query),
        )
    });
    match outcome {
        Ok(result) => result,
        Err(_) => {
            let (future, config) = pending.take().expect("未取得所属域时原值仍在本处");
            Err(SpawnError::Create(CreateError {
                future,
                config,
                cause: CreateErrorKind::LocalContext(LocalRuntimeAccessError::ContextUnavailable),
            }))
        }
    }
}
fn handle_internal_count_inner<
    E: ExternalTaskQueue + Clone,
    I: IdleTaskQueue + Clone,
    T: ExecutorTimer + 'static,
    L: LocalReadySubmit,
>(
    this: &ExecutorHandle<E, I, T, L>,
) -> usize {
    this.state.internal.load(std::sync::atomic::Ordering::Acquire)
}
#[allow(unused_variables)]
fn handle_in_flight_inner<
    E: ExternalTaskQueue + Clone,
    I: IdleTaskQueue + Clone,
    T: ExecutorTimer + 'static,
    L: LocalReadySubmit,
>(
    this: &ExecutorHandle<E, I, T, L>,
) -> usize {
    this.state.in_flight.load(std::sync::atomic::Ordering::Acquire)
}
#[allow(unused_variables)]
fn source_notify_drop_inner(guard: &mut SourceNotifyGuard<'_>) {
    // 只有尚未结清的实际提交尝试才补一次线程检查：可能发生在接收前或接收后，通知不证明接收。
    if !guard.armed {
        return;
    }
    guard.armed = false;
    // 既有通知器负责合并，不分配、不调用用户代码、不重试提交或移交原值。
    pi_async_task_queue::notify::QueueNotifier::notify(guard.notifier);
}

#[cfg(test)]
#[path = "handle_inner_test.rs"]
mod handle_inner_test;
