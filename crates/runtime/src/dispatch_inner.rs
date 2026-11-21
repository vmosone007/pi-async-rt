#[allow(unused_variables)]
fn local_create_inner<C, E, I, T, F, V>(
    future: F,
    config: TaskConfig<V>,
) -> pi_result::RawResult<(UnsubmittedTask, LocalTaskHandle<F::Output, V>), CreateError<F, V>>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalTaskQueue,
    I: IdleTaskQueue,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
    F: Future + 'static,
    F::Output: 'static,
    V: Send + Sync + 'static,
{
        // 进入前保原值与配置：未能进入本地环境时返还同一份输入。
    let mut pending = Some((future, config));
    let outcome = access::with_local::<C, E, I, T, _>(|shared, local| {
        let (future, config) = pending.take().expect("原值与配置只取出一次");
        // 句柄自身的本地创建入口会核原服务与所属域；这里只传当前本地设施。
        shared.inner.create_local(local, future, config)
    });
    match outcome {
        Ok(inner) => inner,
        Err(_) => {
            let (future, config) = pending.take().expect("未进入本地环境时原值仍在本处");
            Err(CreateError {
                future,
                config,
                cause: pi_async_runtime_core::definitions::CreateErrorKind::LocalContext(
                    pi_async_runtime_core::definitions::LocalRuntimeAccessError::ContextUnavailable,
                ),
            })
        }
    }
}
#[allow(unused_variables)]
fn local_submit_inner<C, E, I, T>(
    task: UnsubmittedTask,
    front: bool,
) -> pi_result::RawResult<(), SubmitError>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalTaskQueue,
    I: IdleTaskQueue,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
{
        // 进入前保原待提交值：未进入本地环境时返还同一份输入。
    let mut pending = Some(task);
    let outcome = access::with_local::<C, E, I, T, _>(|shared, local| {
        let task = pending.take().expect("原待提交值只取出一次");
        // 头插只影响下一次选择，不抢断正在轮询的异步计算。
        if front {
            shared.inner.submit_local_front(local, task)
        } else {
            shared.inner.submit_local(local, task)
        }
    });
    match outcome {
        Ok(inner) => inner,
        Err(_) => Err(SubmitError {
            task: pending.take().expect("未进入本地环境时原任务仍在本处"),
            cause: pi_async_runtime_core::definitions::SubmitErrorKind::LocalContext(
                pi_async_runtime_core::definitions::LocalRuntimeAccessError::ContextUnavailable,
            ),
        }),
    }
}
#[allow(unused_variables)]
fn local_idle_inner<C, E, I, T, F, V>(
    future: F,
    config: TaskConfig<V>,
) -> pi_result::RawResult<TaskHandle<F::Output, V>, SpawnError<F, ThreadNotifier, V>>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalTaskQueue,
    I: IdleTaskQueue,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    V: Send + Sync + 'static,
{
        // 进入前保原值与配置；本地环境只为选择目标，不把任务提前绑当前线程。
    let mut pending = Some((future, config));
    let outcome = access::with_local::<C, E, I, T, _>(
        |shared, _local| -> pi_result::RawResult<TaskHandle<F::Output, V>, SpawnError<F, ThreadNotifier, V>> {
            let (future, config) = pending.take().expect("原值与配置只取出一次");
            // 先按普通任务创建，再交独立共享空闲来来源；不调用本地派发产生非 Send 绑定任务。
            let (task, handle) = shared.inner.create(future, config).map_err(SpawnError::Create)?;
            match shared.inner.submit_idle(task) {
                Ok(()) => Ok(handle),
                // 明确拒绝保原待提交值与结果句柄。
                Err(error) => Err(SpawnError::Submit { error, handle }),
            }
        },
    );
    match outcome {
        Ok(inner) => inner,
        Err(_) => {
            let (future, config) = pending.take().expect("未进入本地环境时原值仍在本处");
            Err(SpawnError::Create(CreateError {
                future,
                config,
                cause: pi_async_runtime_core::definitions::CreateErrorKind::LocalContext(
                    pi_async_runtime_core::definitions::LocalRuntimeAccessError::ContextUnavailable,
                ),
            }))
        }
    }
}
#[allow(unused_variables)]
fn local_timing_inner<C, E, I, T, F, V>(
    future: F,
    delay: Duration,
    config: TaskConfig<V>,
) -> pi_result::RawResult<
    pi_async_runtime_core::task::TaskHandle<CoreTimedTaskNotifier<ThreadNotifier, T::Access>, F::Output, V, LocalOutput>,
    SpawnError<F, CoreTimedTaskNotifier<ThreadNotifier, T::Access>, V, LocalOutput>,
>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalTaskQueue,
    I: IdleTaskQueue,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
    F: Future + 'static,
    F::Output: 'static,
    V: Send + Sync + 'static,
{
        // 进入前保原值与配置：未能进入本地环境时返还同一份输入。
    let mut pending = Some((future, config));
    let outcome = access::with_local::<C, E, I, T, _>(|shared, local| {
        let (future, config) = pending.take().expect("原值与配置只取出一次");
        // 同步非等待提交：不在此 await，登记与迟到键由既有支持完成。
        shared.inner.spawn_timing_local(local, future, delay, config)
    });
    match outcome {
        Ok(inner) => inner,
        Err(_) => {
            let (future, config) = pending.take().expect("未进入本地环境时原值仍在本处");
            Err(SpawnError::Create(CreateError {
                future,
                config,
                cause: pi_async_runtime_core::definitions::CreateErrorKind::LocalContext(
                    pi_async_runtime_core::definitions::LocalRuntimeAccessError::ContextUnavailable,
                ),
            }))
        }
    }
}
