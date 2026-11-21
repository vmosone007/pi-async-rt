#[allow(unused_variables)]
fn task_config_new_inner<M>(metadata: M) -> TaskConfig<M> {
    TaskConfig {
        metadata,
        capture_panic_backtrace: false,
        #[cfg(feature = "observe")]
        observer: None,
    }
}
#[allow(unused_variables)]
fn task_control_cancel_inner<N: TaskNotifier>(this: &TaskControl<N>) {
    let mut guard = CancelNotifyGuard { target: this, armed: true };
    this.control.cancel();
    guard.armed = false;
    this.notifier.notify_cancel(|| this.control.snapshot());
}
#[allow(unused_variables)]
fn task_control_wait_inner<N: TaskNotifier>(
    this: &TaskControl<N>,
    external: Option<&ExternalWaiter>,
) -> TaskCancelWait<N> {
    let wait = match external {
        Some(external) => this.control.cancel_and_wait_from(external),
        None => this.control.cancel_and_wait(),
    };
    TaskCancelWait { wait, probe: this.control.clone(), notifier: this.notifier.clone(), notified: false }
}
#[allow(unused_variables)]
fn task_control_clone_inner<N: TaskNotifier>(this: &TaskControl<N>) -> TaskControl<N> {
    TaskControl { control: this.control.clone(), notifier: this.notifier.clone() }
}
#[allow(unused_variables)]
fn task_handle_poll_inner<N: TaskNotifier, T, M: Send + Sync + 'static, K: OutputMode>(
    this: Pin<&mut TaskHandle<N, T, M, K>>,
    cx: &mut Context<'_>,
) -> Poll<pi_result::RawResult<T, pi_async_task::TaskError>> {
    Pin::new(&mut this.get_mut().join).poll(cx)
}
#[allow(unused_variables)]
fn task_cancel_wait_poll_inner<N: TaskNotifier>(
    wait: Pin<&mut CancelWait>,
    cx: &mut Context<'_>,
    guard: &mut CancelWaitNotifyGuard<'_, N>,
) -> Poll<CancellationOutcome> {
    // 仅当本等待尚未通知时，在调用原生轮询前立即激活守卫（异常展开时由它按同一任务的实际取消事实接续）。
    if !*guard.notified {
        guard.armed = true;
    }
    // 只轮询传入的原生等待一次并原样返回其结果；身份、登记、拒绝与释放均由它负责。
    wait.poll(cx)
}

fn task_factory_new_inner<S: Schedule, N: TaskNotifier>(
    schedule: S,
    notifier: N,
    queue_support: QueueSupport,
) -> pi_result::Result<TaskFactory<S, N>> {
    let runtime_id = RuntimeId::try_new()?;
    Ok(TaskFactory { runtime_id, schedule, notifier, queue_support })
}
#[allow(unused_variables)]
fn task_factory_submit_inner<S: Schedule, N: TaskNotifier>(
    this: &TaskFactory<S, N>,
    task: UnsubmittedTask,
    mode: SubmissionMode,
    push: impl FnOnce(Runnable) -> pi_result::RawResult<(), (Runnable, SubmitErrorKind)>,
) -> pi_result::RawResult<(), SubmitError> {
    // 先核同一原服务身份；跨服务提交不调用来源、不通知，原值原样返还。
    if task.runtime_id != this.runtime_id {
        return Err(SubmitError { task, cause: SubmitErrorKind::WrongRuntime });
    }
    // 提交方式与原任务所属域必须匹配：普通／空闲只接受非本地任务，本地须精确所属域。
    match mode {
        SubmissionMode::Ordinary | SubmissionMode::Idle => {
            if task.local_owner.is_some() {
                return Err(SubmitError { task, cause: SubmitErrorKind::LocalSubmissionRequired });
            }
        }
        SubmissionMode::Local(owner) => match task.local_owner {
            Some(actual) if actual == owner => {}
            Some(_) => return Err(SubmitError { task, cause: SubmitErrorKind::WrongOwner }),
            None => return Err(SubmitError { task, cause: SubmitErrorKind::OrdinarySubmissionRequired }),
        },
    }
    let UnsubmittedTask { runnable, runtime_id, local_owner } = task;
    match push(runnable) {
        Ok(()) => {
            // 只有正常接收才在来源借用与保护之外通知一次；通知异常不撤销已接收任务。
            this.notifier.notify();
            Ok(())
        }
        // 拒绝必须返还同一原任务与身份；不通知、不接纳、不轮询（poll）。
        Err((runnable, cause)) => Err(SubmitError {
            task: UnsubmittedTask { runnable, runtime_id, local_owner },
            cause,
        }),
    }
}

fn task_factory_create_inner<S: Schedule + Clone, N: TaskNotifier, F, M>(
    this: &TaskFactory<S, N>,
    future: F,
    config: TaskConfig<M>,
) -> pi_result::RawResult<(UnsubmittedTask, TaskHandle<N, F::Output, M>), CreateError<F, M>>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    M: Send + Sync + 'static,
{
    let TaskConfig {
        metadata,
        capture_panic_backtrace,
        #[cfg(feature = "observe")]
        observer,
    } = config;
    let options = pi_async_task::TaskOptions {
        queue: this.queue_support,
        capture_panic_backtrace,
        #[cfg(feature = "observe")]
        observer,
    };
    // 在原生创建（native creation）前克隆通知能力（notifier），避免成功后的用户克隆（user clone）异常放弃已发布权限（published capabilities）。
    let notifier = this.notifier.clone();
    match pi_async_task::create(future, this.schedule.clone(), metadata, options) {
        Ok(parts) => Ok((
            UnsubmittedTask { runnable: parts.runnable, runtime_id: this.runtime_id, local_owner: None },
            TaskHandle { join: parts.join, control: TaskControl { control: parts.control, notifier } },
        )),
        Err(error) => {
            let pi_async_task::BuildError { future, schedule, metadata, options, cause } = error;
            drop(schedule);
            Err(CreateError {
                future,
                config: TaskConfig {
                    metadata,
                    capture_panic_backtrace: options.capture_panic_backtrace,
                    #[cfg(feature = "observe")]
                    observer: options.observer,
                },
                cause: CreateErrorKind::Task(cause),
            })
        }
    }
}
#[allow(unused_variables)]
fn task_factory_create_local_inner<S: Schedule + Clone, N: TaskNotifier, F, M>(
    this: &TaskFactory<S, N>,
    owner: &TaskThread,
    future: F,
    config: TaskConfig<M>,
) -> pi_result::RawResult<
    (UnsubmittedTask, TaskHandle<N, F::Output, M, LocalOutput>),
    CreateError<F, M>,
>
where
    F: Future + 'static,
    F::Output: 'static,
    M: Send + Sync + 'static,
{
    let TaskConfig {
        metadata,
        capture_panic_backtrace,
        #[cfg(feature = "observe")]
        observer,
    } = config;
    let options = pi_async_task::TaskOptions {
        queue: this.queue_support,
        capture_panic_backtrace,
        #[cfg(feature = "observe")]
        observer,
    };
    let notifier = this.notifier.clone();
    match pi_async_task::create_local(owner, future, this.schedule.clone(), metadata, options) {
        Ok(parts) => Ok((
            UnsubmittedTask { runnable: parts.runnable, runtime_id: this.runtime_id, local_owner: Some(owner.id()) },
            TaskHandle { join: parts.join, control: TaskControl { control: parts.control, notifier } },
        )),
        Err(error) => {
            let pi_async_task::BuildError { future, schedule, metadata, options, cause } = error;
            drop(schedule);
            Err(CreateError {
                future,
                config: TaskConfig {
                    metadata,
                    capture_panic_backtrace: options.capture_panic_backtrace,
                    #[cfg(feature = "observe")]
                    observer: options.observer,
                },
                cause: CreateErrorKind::Task(cause),
            })
        }
    }
}
#[allow(unused_variables)]
fn task_factory_notifier_inner<S: Schedule + Clone, N: TaskNotifier, N2: TaskNotifier>(
    this: &TaskFactory<S, N>,
    notifier: N2,
) -> TaskFactory<S, N2> {
    TaskFactory {
        runtime_id: this.runtime_id,
        schedule: this.schedule.clone(),
        notifier,
        queue_support: this.queue_support,
    }
}
#[allow(unused_variables)]
fn task_factory_clone_inner<S: Schedule + Clone, N: TaskNotifier>(
    this: &TaskFactory<S, N>,
) -> TaskFactory<S, N> {
    TaskFactory {
        runtime_id: this.runtime_id,
        schedule: this.schedule.clone(),
        notifier: this.notifier.clone(),
        queue_support: this.queue_support,
    }
}
#[allow(unused_variables)]
fn cancel_wait_notify_drop_inner<N: TaskNotifier>(
    guard: &mut CancelWaitNotifyGuard<'_, N>,
) -> Option<TaskSnapshot> {
    // 先撤销本份重复触发资格；本等待已通知过就不再追加。
    guard.armed = false;
    if *guard.notified {
        return None;
    }
    // 只按同一原控制句柄的一次快照判断：实际已记录取消才接续原定时登记（registration）。
    let snapshot = guard.probe.snapshot();
    if snapshot.cancellation == pi_async_task::definitions::CancelRequestStatus::Recorded {
        *guard.notified = true;
        Some(snapshot)
    } else {
        None
    }
}
fn cancel_notify_drop_inner<N: TaskNotifier>(this: &mut CancelNotifyGuard<'_, N>) {
    if this.armed {
        this.armed = false;
        this.target.notifier.notify_cancel(|| this.target.control.snapshot());
    }
}
