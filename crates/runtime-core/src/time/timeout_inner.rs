#[allow(unused_variables)]
fn timeout_new_inner<A: TimerAccess>(access: A, duration: Duration) -> Timeout<A> {
    let min_positive = Duration::from_millis(1);
    // 零时长只让出一次，无时钟、无登记；正时长不足1ms属入口非法，错误保存到首次轮询（poll）。
    if duration.is_zero() {
        return Timeout {
            access,
            duration,
            registration: None,
            initial_error: None,
            phase: TimeoutPhase::Zero,
            zero: YieldOnce::new(),
        };
    }
    if duration < min_positive {
        return Timeout {
            access,
            duration,
            registration: None,
            initial_error: Some(TimerError::InvalidDuration {
                value: duration,
                min_positive,
                max: None,
            }),
            phase: TimeoutPhase::Prepared,
            zero: YieldOnce::new(),
        };
    }
    // 正时长只准备共享登记，不取时、不登记；准备失败保留原报告（Report），控制随之为无效。
    let (registration, initial_error) = match access.prepare_registration(TimerRegistrationKind::Wait) {
        Ok(registration) => (Some(registration), None),
        Err(error) => (None, Some(error)),
    };
    Timeout {
        access,
        duration,
        registration,
        initial_error,
        phase: TimeoutPhase::Prepared,
        zero: YieldOnce::new(),
    }
}
#[allow(unused_variables)]
fn timeout_control_inner<A: TimerAccess>(this: &Timeout<A>) -> TimeoutControl<A> {
    // 只有在等待仍可能登记时才交出同一登记；零时长、准备失败与结束状态都不提供有效取消。
    let usable = this.initial_error.is_none()
        && matches!(this.phase, TimeoutPhase::Prepared | TimeoutPhase::Registered);
    TimeoutControl {
        access: this.access.clone(),
        registration: if usable { this.registration.clone() } else { None },
    }
}
#[allow(unused_variables)]
fn timeout_poll_inner<A: TimerAccess>(
    this: Pin<&mut Timeout<A>>,
    cx: &mut Context<'_>,
) -> Poll<pi_result::RawResult<(), TimerError>> {
    let this = this.get_mut();
    assert!(!matches!(this.phase, TimeoutPhase::Done),
        "已完成的计时等待不能再次轮询（timeout polled after completion）");
    // 准备阶段保存的原错误只在首次轮询原样交出。
    if let Some(error) = this.initial_error.take() {
        this.phase = TimeoutPhase::Done;
        return Poll::Ready(Err(error));
    }
    match this.phase {
        TimeoutPhase::Zero => return match Pin::new(&mut this.zero).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(()) => {
                this.phase = TimeoutPhase::Done;
                Poll::Ready(Ok(()))
            }
        },
        TimeoutPhase::Prepared => {
            let registration = match this.registration.as_ref() {
                Some(registration) => registration,
                None => {
                    this.phase = TimeoutPhase::Done;
                    return Poll::Ready(Err(TimerError::ServiceClosed));
                }
            };
            // 在开始登记前准备好守卫及原入口持有。适配者克隆（Clone）可能发生恐慌（panic）。
            let mut guard = RegistrationGuard {
                access: this.access.clone(),
                registration: registration.clone(),
                armed: false,
            };
            let entry_registration = registration.clone();
            let target = registration.as_ref();
            // 先以原开始协调预先取消；取消先成立时本地结束，不接触来源。
            if !target.begin() {
                this.phase = TimeoutPhase::Done;
                target.close_interest();
                target.finish(RegistrationEnd::Cancelled);
                return Poll::Ready(Err(TimerError::Cancelled));
            }
            guard.armed = true;
            let deadline = match this.access.deadline_after(this.duration) {
                Ok(deadline) => deadline,
                Err(error) => {
                    this.phase = TimeoutPhase::Done;
                    guard.armed = false;
                    target.finish(RegistrationEnd::DirectFailure);
                    return Poll::Ready(Err(error));
                }
            };
            // 等待结果只经同一登记确认，不构造额外任务（Task）。
            let entry = TimerEntry::signal(entry_registration);
            match this.access.try_register(deadline, entry) {
                Ok(key) => {
                    // 发布键只报告已有取消；不能由发布本身制造取消。
                    let already_recorded = target.publish_key(key);
                    guard.armed = false;
                    this.phase = TimeoutPhase::Registered;
                    if already_recorded {
                        let _ = request_registration_cancel(&this.access, registration, CancelMode::KeyPublished);
                    }
                }
                Err(submit_error) => {
                    let pi_async_task_queue::timer::TimerSubmitError { item, cause } = submit_error;
                    // 明确拒绝：先恢复同一原值，再以直接失败结束登记，最后返原来源错误。
                    let _ = item.recover();
                    target.finish(RegistrationEnd::DirectFailure);
                    guard.armed = false;
                    this.phase = TimeoutPhase::Done;
                    return Poll::Ready(Err(TimerError::Queue(cause)));
                }
            }
        }
        TimeoutPhase::Registered => {}
        TimeoutPhase::Done => unreachable!(),
    }

    let Some(registration) = this.registration.as_ref() else {
        this.phase = TimeoutPhase::Done;
        return Poll::Ready(Err(TimerError::ServiceClosed));
    };
    // 首次成功登记也须使用原唤醒器（AtomicWaker）的读取、登记、复查协议。
    let Poll::Ready(end) = registration.as_ref().poll(cx) else {
        return Poll::Pending;
    };
    this.phase = TimeoutPhase::Done;
    Poll::Ready(match end {
        RegistrationEnd::Elapsed => Ok(()),
        RegistrationEnd::Cancelled => Err(TimerError::Cancelled),
        RegistrationEnd::Removed => Err(TimerError::RegistrationRemoved),
        RegistrationEnd::DirectFailure => Err(TimerError::ServiceClosed),
    })
}

fn timeout_drop_inner<A: TimerAccess>(this: &mut Timeout<A>) {
    // 同一登记的等待兴趣在放弃后结束，后续控制不得报告新的取消请求。
    if let Some(registration) = this.registration.as_ref() {
        if matches!(this.phase, TimeoutPhase::Prepared | TimeoutPhase::Registered) {
            registration.as_ref().close_interest();
        }
        if matches!(this.phase, TimeoutPhase::Registered) {
            let _ = request_registration_cancel(&this.access, registration, CancelMode::Detached);
        }
    }
    this.phase = TimeoutPhase::Done;
}
#[allow(unused_variables)]
fn timeout_cancel_inner<A: TimerAccess>(
    this: &TimeoutControl<A>,
) -> pi_result::RawResult<bool, TimerError> {
    // 没有有效登记（零时长、准备失败或已结束）时不可取消，也不触达来源。
    let Some(registration) = this.registration.as_ref() else {
        return Ok(false);
    };
    // 同一事实字内同时核兴趣并记录请求；键（key）未就绪时交等待值本地完成，本处不等待。
    match request_registration_cancel(&this.access, registration, CancelMode::WaitControl) {
        Ok(recorded) => Ok(recorded),
        // 原来源错误原样映射交调用者；重试责任已由转交协议接续，不在此复制报告（Report）。
        Err(error) => Err(TimerError::Queue(error)),
    }
}

fn timeout_control_clone_inner<A: TimerAccess>(this: &TimeoutControl<A>) -> TimeoutControl<A> {
    // 只克隆原访问与同一登记；零时长与失效控制保持失效，不创建登记、不重置键（key）。
    TimeoutControl {
        access: this.access.clone(),
        registration: this.registration.clone(),
    }
}
#[allow(unused_variables)]
fn timer_resume_inner<A: TimerAccess>(access: &A, registration: A::Registration) {
    // 只对真实待接续（Deferred）责任一次取得发布资格；已结束返回false，不在此重试或等待。
    let _ = request_registration_cancel(access, &registration, CancelMode::Resume);
}
#[allow(unused_variables)]
fn registration_guard_drop_inner<A: TimerAccess>(this: &mut RegistrationGuard<A>) {
    if !this.armed {
        return;
    }
    // 先撤销本份重复触发资格，再按外部取消（Detached）规则请求一次；键未就绪交真实发布者，
    // 明确未接受的直接失败由外层完成，本处不伪报拒绝也不复制原错误。
    this.armed = false;
    let _ = request_registration_cancel(&this.access, &this.registration, CancelMode::Detached);
}
#[allow(unused_variables)]
fn forward_guard_drop_inner<'a, A: TimerAccess>(
    this: &mut ForwardGuard<'a, A>,
) -> Option<(A::Registration, Confirmation<'a, A::Key>)> {
    // 一次取出本份原准备持有与对应确认；原错误由壳（shell）在调用计时访问前单独取出一次。
    // pending 已空（已交接或重复进入）时返回 None，不递归请求、不凭原错误重启转交。
    this.pending.take()
}
#[cfg(test)]
#[path = "timeout_inner_test.rs"]
mod timeout_inner_test;

#[cfg(test)]
#[path = "timeout_construct_inner_test.rs"]
mod timeout_construct_inner_test;

fn registration_cancel_forward_inner<A: TimerAccess>(
    access: &A,
    registration: &A::Registration,
    mode: CancelMode,
) -> pi_result::RawResult<bool, TimerQueueError> {
    // 适配者（adapter）代码先完成：克隆原持有并核其关联同一登记，之后才记录取消或取唯一转交资格。
    let prepared = registration.clone();
    let target = registration.as_ref();
    if !std::ptr::eq(prepared.as_ref(), target) {
        return Err(TimerQueueError::WrongQueue);
    }
    let access_for_guard = access.clone();
    // 确认（Confirmation）借用调用者的同一登记，守卫（guard）另行持有已准备的克隆持有。
    // WaitControl 在同一事实字内同时核等待兴趣；Detached 记录内部撤销；两种接续模式只接续既有请求。
    let recorded = match mode {
        CancelMode::WaitControl => target.request_wait_cancel(),
        CancelMode::Detached => target.request_cancel(),
        CancelMode::Resume | CancelMode::KeyPublished => true,
    };
    if !recorded {
        return Ok(false);
    }
    // 键（key）未就绪或已结束都没有可结清的责任：交真实发布者／关闭参与者，本入口不等待。
    // 只有真正从预备接续取出的 Resume 期望 Deferred；键发布前已有取消时 forwarding 仍为 Idle，
    // 因此 KeyPublished 须从 Idle 取得首次转交资格（且不制造新取消请求）。
    let deferred = matches!(mode, CancelMode::Resume);
    let Some(confirmation) = target.claim_forwarding(deferred) else {
        // 控制已记录或合并请求。不必等待登记键（key）或正在转交的参与者完成。
        return Ok(matches!(mode, CancelMode::WaitControl));
    };
    let mut guard = ForwardGuard {
        access: access_for_guard,
        pending: Some((prepared, confirmation)),
        error: None,
    };
    let attempt = {
        let key = target.key().expect("取得转交资格（forwarding）时键（key）已就绪");
        access.request_cancel(key)
    };
    match attempt {
        Ok(outcome) => {
            // 来源已返回真实结果。先结清确认，再释放可能执行用户代码的准备持有。
            let (prepared, confirmation) = match forward_guard_drop_inner(&mut guard) {
                Some(pair) => pair,
                None => return Ok(false),
            };
            confirmation.finish_request(outcome);
            drop(prepared);
            Ok(true)
        }
        Err(error) => {
            if matches!(mode, CancelMode::WaitControl) {
                // 控制路径把原错误交调用者；节点只保既有责任，原错误不复制、不提前发布Deferred。
                Err(error)
            } else {
                // 其它模式把同一原错误交守卫，沿正常返回；守卫结束（drop）时才转交。
                guard.error = Some(error);
                Ok(true)
            }
        }
    }
}

fn spawn_timed_task_inner<S, N, A, F, M>(
    factory: &crate::task::TaskFactory<S, N>,
    access: A,
    future: F,
    delay: Duration,
    config: crate::task::TaskConfig<M>,
) -> pi_result::RawResult<
    crate::task::TaskHandle<super::task_notify::TimedTaskNotifier<N, A>, F::Output, M>,
    crate::task::SpawnError<F, super::task_notify::TimedTaskNotifier<N, A>, M>,
>
where
    S: pi_async_task::Schedule + Clone,
    N: crate::task::TaskNotifier,
    A: TimerAccess,
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    M: Send + Sync + 'static,
{
    use crate::definitions::{CreateErrorKind, SubmitErrorKind};
    use crate::task::{CreateError, SpawnError, SubmissionMode};
    use pi_async_task_queue::timer::TimerSubmitError;

    // 1. 先核同一运行时身份（RuntimeId）与合法时长；0 时长仍真实登记，正时长至少 1ms 且无上限。
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
    // 2. 只取时一次、准备任务登记一次；失败均原样归还 F 与配置，不创建任务（Task）。
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
    // 3. 组合定时任务通知器（TimedTaskNotifier）并由同源工厂创建唯一原任务（Task）。
    let timed_notifier = super::task_notify::TimedTaskNotifier::new(
        factory.notifier().clone(),
        access.clone(),
        registration.clone(),
    );
    let timed_factory = factory.with_notifier(timed_notifier);
    let (unsubmitted, handle) = timed_factory
        .create(future, config)
        .map_err(SpawnError::Create)?;

    // 4. 在 factory.submit 外层建立登记守卫（RegistrationGuard），保护来源接受后的键发布与通知异常。
    let mut guard = RegistrationGuard {
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
                // 键发布只报告已有取消；已有取消经 KeyPublished 接续，原错误交预备接续不伪拒绝。
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

fn spawn_timed_local_task_inner<S, N, A, F, M>(
    factory: &crate::task::TaskFactory<S, N>,
    owner: &pi_async_task::TaskThread,
    access: A,
    future: F,
    delay: Duration,
    config: crate::task::TaskConfig<M>,
) -> pi_result::RawResult<
    crate::task::TaskHandle<
        super::task_notify::TimedTaskNotifier<N, A>,
        F::Output,
        M,
        pi_async_task::LocalOutput,
    >,
    crate::task::SpawnError<
        F,
        super::task_notify::TimedTaskNotifier<N, A>,
        M,
        pi_async_task::LocalOutput,
    >,
>
where
    S: pi_async_task::Schedule + Clone,
    N: crate::task::TaskNotifier,
    A: TimerAccess,
    F: Future + 'static,
    F::Output: 'static,
    M: Send + Sync + 'static,
{
    use crate::definitions::{CreateErrorKind, SubmitErrorKind};
    use crate::task::{CreateError, SpawnError, SubmissionMode};
    use pi_async_task_queue::timer::TimerSubmitError;

    // 1. 与普通定时派发保持一致的准备检查；不要求 F 或输出实现 Send/Sync。
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
    // 2. 沿原所属线程（TaskThread）创建本地任务（LocalOutput），保持 local_owner。
    let timed_notifier = super::task_notify::TimedTaskNotifier::new(
        factory.notifier().clone(),
        access.clone(),
        registration.clone(),
    );
    let timed_factory = factory.with_notifier(timed_notifier);
    let (unsubmitted, handle) = timed_factory
        .create_local(owner, future, config)
        .map_err(SpawnError::Create)?;

    // 3. 经本地提交模式与 try_register_local 交定时来源；拒绝原样返还本地持有与 LocalOutput 句柄。
    let mut guard = RegistrationGuard {
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
