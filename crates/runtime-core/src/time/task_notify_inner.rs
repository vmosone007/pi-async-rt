#[allow(unused_variables)]
fn timed_notifier_new_inner<N: TaskNotifier, A: TimerAccess>(
    inner: N,
    access: A,
    registration: A::Registration,
) -> TimedTaskNotifier<N, A> {
    todo!("目标：移动原通知、受限access及同一registration，不保留额外ControlHandle，不创建任务或复制原Runnable。普通任务不构造此对象。")
}
#[allow(unused_variables)]
fn timed_notifier_clone_inner<N: TaskNotifier, A: TimerAccess>(
    this: &TimedTaskNotifier<N, A>,
) -> TimedTaskNotifier<N, A> {
    todo!("目标：只克隆原通知、access及同一登记持有；原键不Clone，不建立另一登记或改变原TaskId。")
}
#[allow(unused_variables)]
fn timed_notifier_cancel_inner<N: TaskNotifier, A: TimerAccess>(
    this: &TimedTaskNotifier<N, A>,
    snapshot: impl FnOnce() -> TaskSnapshot,
) {
    todo!("目标：惰性快照至多求值一次，只有snapshot.cancellation为CancelRequestStatus::Recorded才合并定时取消。无键交真实发布者，已结束不复活；统一经time::timeout::request_registration_cancel的CancelMode::Detached入口有限转交，失败原错误无返回口则交预备接续，不直接访问ForwardGuard私有字段。原inner.notify仍安排执行线程检查，不直接poll。通知前撤销重复触发，原panic不捕获，不改变原取消等待拒绝或成功分类。")
}
