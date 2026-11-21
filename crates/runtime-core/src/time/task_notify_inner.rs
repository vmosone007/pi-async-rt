#[allow(unused_variables)]
fn timed_notifier_new_inner<N: TaskNotifier, A: TimerAccess>(
    inner: N,
    access: A,
    registration: A::Registration,
) -> TimedTaskNotifier<N, A> {
    TimedTaskNotifier { inner, access, registration }
}
#[allow(unused_variables)]
fn timed_notifier_clone_inner<N: TaskNotifier, A: TimerAccess>(
    this: &TimedTaskNotifier<N, A>,
) -> TimedTaskNotifier<N, A> {
    TimedTaskNotifier {
        inner: this.inner.clone(),
        access: this.access.clone(),
        registration: this.registration.clone(),
    }
}
#[allow(unused_variables)]
fn timed_notifier_cancel_inner<N: TaskNotifier, A: TimerAccess>(
    this: &TimedTaskNotifier<N, A>,
    snapshot: impl FnOnce() -> TaskSnapshot,
) {
    // 快照至多求值一次：只有已记录的原生取消才合并定时取消，已有独立取消按同一任务事实合并。
    let recorded = snapshot().cancellation
        == pi_async_task::definitions::CancelRequestStatus::Recorded;
    if recorded {
        // 统一经既有转交入口有限尝试一次；键未就绪交真实发布者，错误由预备接续承担，不在此重试。
        let _ = crate::time::timeout::request_registration_cancel(
            &this.access,
            &this.registration,
            crate::time::timeout::CancelMode::Detached,
        );
    }
    // 原通知仍安排执行线程检查，不直接轮询（poll）任务。
    this.inner.notify();
}
