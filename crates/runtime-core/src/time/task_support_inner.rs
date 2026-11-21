#[cfg(test)]
#[path = "task_support_inner_test.rs"]
mod task_support_inner_test;

fn spawn_query_inner<S, N, A, F, M, G>(
    factory: &TaskFactory<S, N>,
    access: A,
    future: F,
    delay: Duration,
    config: TaskConfig<M>,
    register: G,
) -> pi_result::RawResult<
    TaskHandle<TimedTaskNotifier<N, A>, F::Output, M>,
    SpawnError<F, TimedTaskNotifier<N, A>, M>,
>
where
    S: Schedule + Clone,
    N: TaskNotifier,
    A: TimerAccess,
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    M: Send + Sync + 'static,
    G: FnOnce(TaskQuery),
{
    use crate::definitions::TimerError;
    use crate::task::CreateError;
    use pi_async_task_queue::timer::{TimerQueueError, TimerSubmitError};

    // 1. 准备阶段与普通定时派发一致：同源 RuntimeId、合法时长、取时一次、准备任务登记一次。
    if factory.runtime_id() != access.runtime_id() {
        return Err(SpawnError::Create(CreateError {
            future,
            config,
            cause: CreateErrorKind::Timer(TimerError::Queue(TimerQueueError::WrongQueue)),
        }));
    }
    let min_positive = Duration::from_millis(1);
    if !delay.is_zero() && delay < min_positive {
        return Err(SpawnError::Create(CreateError {
            future,
            config,
            cause: CreateErrorKind::Timer(TimerError::InvalidDuration {
                value: delay,
                min_positive,
                max: None,
            }),
        }));
    }
    let deadline = match access.deadline_after(delay) {
        Ok(deadline) => deadline,
        Err(error) => {
            return Err(SpawnError::Create(CreateError {
                future,
                config,
                cause: CreateErrorKind::Timer(error),
            }));
        }
    };
    let registration = match access.prepare_registration(TimerRegistrationKind::Task) {
        Ok(registration) => registration,
        Err(error) => {
            return Err(SpawnError::Create(CreateError {
                future,
                config,
                cause: CreateErrorKind::Timer(error),
            }));
        }
    };
    // 2. 组合定时任务通知器并创建唯一原任务（Task）。
    let timed_notifier = TimedTaskNotifier::new(
        factory.notifier().clone(),
        access.clone(),
        registration.clone(),
    );
    let timed_factory = factory.with_notifier(timed_notifier);
    let (unsubmitted, handle) = timed_factory
        .create(future, config)
        .map_err(SpawnError::Create)?;

    // 3. 来源可见前同步移交只读查询句柄（TaskQuery）一次；若恐慌不捕获，未提交原值沿 Abandoned 路径清理。
    register(handle.query_handle());

    // 4. 外层建立查询登记守卫（QueryRegistrationGuard），经原工厂提交至定时来源并发布键。
    let mut guard = QueryRegistrationGuard {
        access: access.clone(),
        registration: registration.clone(),
        armed: false,
    };
    let submit_result = factory.submit(unsubmitted, SubmissionMode::Ordinary, |runnable| {
        if !registration.as_ref().begin() {
            registration.as_ref().finish(RegistrationEnd::DirectFailure);
            return Err((runnable, SubmitErrorKind::Timer(TimerQueueError::Closed)));
        }
        guard.armed = true;
        let entry = TimerEntry::task(runnable, registration.clone());
        match access.try_register(deadline, entry) {
            Ok(key) => {
                let already_recorded = registration.as_ref().publish_key(key);
                if already_recorded {
                    let _ = request_registration_cancel(
                        &access,
                        &registration,
                        CancelMode::KeyPublished,
                    );
                }
                Ok(())
            }
            Err(TimerSubmitError { item, cause }) => {
                let runnable = item
                    .recover()
                    .expect("刚构造的任务定时入口（TimerEntry::task）必须保有原Runnable");
                registration.as_ref().finish(RegistrationEnd::DirectFailure);
                guard.armed = false;
                Err((runnable, SubmitErrorKind::Timer(cause)))
            }
        }
    });
    match submit_result {
        Ok(()) => {
            guard.armed = false;
            Ok(handle)
        }
        Err(error) => {
            guard.armed = false;
            Err(SpawnError::Submit { error, handle })
        }
    }
}
fn spawn_local_query_inner<S, N, A, F, M, G>(
    factory: &TaskFactory<S, N>,
    owner: &TaskThread,
    access: A,
    future: F,
    delay: Duration,
    config: TaskConfig<M>,
    register: G,
) -> pi_result::RawResult<
    TaskHandle<TimedTaskNotifier<N, A>, F::Output, M, LocalOutput>,
    SpawnError<F, TimedTaskNotifier<N, A>, M, LocalOutput>,
>
where
    S: Schedule + Clone,
    N: TaskNotifier,
    A: TimerAccess,
    F: Future + 'static,
    F::Output: 'static,
    M: Send + Sync + 'static,
    G: FnOnce(TaskQuery),
{
    use crate::definitions::TimerError;
    use crate::task::CreateError;
    use pi_async_task_queue::timer::{TimerQueueError, TimerSubmitError};

    // 1. 同源与时长准备；不要求 F 或输出实现 Send/Sync。
    if factory.runtime_id() != access.runtime_id() {
        return Err(SpawnError::Create(CreateError {
            future,
            config,
            cause: CreateErrorKind::Timer(TimerError::Queue(TimerQueueError::WrongQueue)),
        }));
    }
    let min_positive = Duration::from_millis(1);
    if !delay.is_zero() && delay < min_positive {
        return Err(SpawnError::Create(CreateError {
            future,
            config,
            cause: CreateErrorKind::Timer(TimerError::InvalidDuration {
                value: delay,
                min_positive,
                max: None,
            }),
        }));
    }
    let deadline = match access.deadline_after(delay) {
        Ok(deadline) => deadline,
        Err(error) => {
            return Err(SpawnError::Create(CreateError {
                future,
                config,
                cause: CreateErrorKind::Timer(error),
            }));
        }
    };
    let registration = match access.prepare_registration(TimerRegistrationKind::Task) {
        Ok(registration) => registration,
        Err(error) => {
            return Err(SpawnError::Create(CreateError {
                future,
                config,
                cause: CreateErrorKind::Timer(error),
            }));
        }
    };
    // 2. 沿原所属线程（TaskThread）本地创建任务并同步登记只读查询（TaskQuery）。
    let timed_notifier = TimedTaskNotifier::new(
        factory.notifier().clone(),
        access.clone(),
        registration.clone(),
    );
    let timed_factory = factory.with_notifier(timed_notifier);
    let (unsubmitted, handle) = timed_factory
        .create_local(owner, future, config)
        .map_err(SpawnError::Create)?;

    // 若 register 恐慌，未提交的本地原任务仍在当前所属线程栈上按原 TaskThread 规则合法清理。
    register(handle.query_handle());

    // 3. 经本地提交模式与 try_register_local 交定时来源。
    let mut guard = QueryRegistrationGuard {
        access: access.clone(),
        registration: registration.clone(),
        armed: false,
    };
    let submit_result = factory.submit(unsubmitted, SubmissionMode::Local(owner.id()), |runnable| {
        if !registration.as_ref().begin() {
            registration.as_ref().finish(RegistrationEnd::DirectFailure);
            return Err((runnable, SubmitErrorKind::Timer(TimerQueueError::Closed)));
        }
        guard.armed = true;
        let entry = TimerEntry::task(runnable, registration.clone());
        match access.try_register_local(deadline, entry) {
            Ok(key) => {
                let already_recorded = registration.as_ref().publish_key(key);
                if already_recorded {
                    let _ = request_registration_cancel(
                        &access,
                        &registration,
                        CancelMode::KeyPublished,
                    );
                }
                Ok(())
            }
            Err(TimerSubmitError { item, cause }) => {
                let runnable = item
                    .recover()
                    .expect("刚构造的本地任务定时入口（TimerEntry::task）必须保有原Runnable");
                registration.as_ref().finish(RegistrationEnd::DirectFailure);
                guard.armed = false;
                Err((runnable, SubmitErrorKind::Timer(cause)))
            }
        }
    });
    match submit_result {
        Ok(()) => {
            guard.armed = false;
            Ok(handle)
        }
        Err(error) => {
            guard.armed = false;
            Err(SpawnError::Submit { error, handle })
        }
    }
}
#[allow(unused_variables)]
fn query_registration_drop_inner<A: TimerAccess>(this: &mut QueryRegistrationGuard<A>) {
    if !this.armed {
        return;
    }
    // 先撤销本份重复触发资格；已取得责任但未完整结束时，按外部取消（Detached）规则
    // 请求一次原登记取消。键未就绪交真实发布者，错误由原预备接续承担，不在此重试或等待。
    this.armed = false;
    let _ = request_registration_cancel(&this.access, &this.registration, CancelMode::Detached);
}
