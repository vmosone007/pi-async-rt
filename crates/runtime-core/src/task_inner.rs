#[allow(unused_variables)]
fn task_config_new_inner<M>(metadata: M) -> TaskConfig<M> {
    todo!("编号：T191，尚未实现， 目标：移动metadata，capture_panic_backtrace=false，observe下observer=None。只组织配置，不创建或提交任务；不得添加Schedule／QueueSupport覆盖能力。")
}
#[allow(unused_variables)]
fn task_control_cancel_inner<N: TaskNotifier>(this: &TaskControl<N>) {
    todo!("编号：T192，尚未实现， 目标：在原control.cancel前建立CancelNotifyGuard，原生调用之后撤销守卫再notify_cancel。惰性快照闭包只读取原control.snapshot；普通通知默认不求值。原生异常仍安排必要一次通知，notify自身异常不得再次通知。不改变既有取消分类、清理线程或原首次panic。")
}
#[allow(unused_variables)]
fn task_control_wait_inner<N: TaskNotifier>(
    this: &TaskControl<N>,
    external: Option<&ExternalWaiter>,
) -> TaskCancelWait<N> {
    todo!("编号：T193，尚未实现， 目标：按external选择原cancel_and_wait／cancel_and_wait_from，移动原等待并克隆一份只读控制探针及notifier，notified=false。构造不发送取消、不取时、不通知，不伪造外部等待者身份。")
}
#[allow(unused_variables)]
fn task_control_clone_inner<N: TaskNotifier>(this: &TaskControl<N>) -> TaskControl<N> {
    todo!("编号：T194，尚未实现， 目标：克隆原ControlHandle与notifier，仍指同一TaskId；不额外创建Task、取消等待或源登记，不取消。")
}
#[allow(unused_variables)]
fn task_handle_poll_inner<N: TaskNotifier, T, M: Send + Sync + 'static, K: OutputMode>(
    this: Pin<&mut TaskHandle<N, T, M, K>>,
    cx: &mut Context<'_>,
) -> Poll<pi_result::RawResult<T, pi_async_task::TaskError>> {
    todo!("编号：T195，尚未实现， 目标：仅轮询原JoinHandle并原样转交原输出／TaskError；不能因尚未提交而隐式提交，不能把业务T中的Err当成任务取消。join已Unpin；N不结构性固定，不对输入增加Unpin。原生重复领取语义原样保留。")
}
#[allow(unused_variables)]
fn task_cancel_wait_poll_inner<N: TaskNotifier>(
    this: Pin<&mut TaskCancelWait<N>>,
    cx: &mut Context<'_>,
) -> Poll<CancellationOutcome> {
    todo!("编号：T196，尚未实现， 目标：先建立普通异常通知保障再poll原CancelWait；首次Pending才表明原合法登记／请求已发生，此时设notified后经惰性探针notify_cancel一次。直接Ready的拒绝不产生本次定时取消；原有独立取消按原事实。后续Pending不重复通知。Drop等待只复用原退登记，不撤销原取消，原实际等待者身份保持。异常路径不新增catch_unwind。")
}
#[allow(unused_variables)]
fn task_factory_new_inner<S: Schedule, N: TaskNotifier>(
    schedule: S,
    notifier: N,
    queue_support: QueueSupport,
) -> pi_result::Result<TaskFactory<S, N>> {
    todo!("编号：T197，尚未实现， 目标：只分配一次RuntimeId并保存原Schedule、notifier及QueueSupport；失败不启动服务或创建Task，不接管外部已有RuntimeId。")
}
#[allow(unused_variables)]
fn task_factory_submit_inner<S: Schedule, N: TaskNotifier>(
    this: &TaskFactory<S, N>,
    task: UnsubmittedTask,
    mode: SubmissionMode,
    push: impl FnOnce(Runnable) -> pi_result::RawResult<(), (Runnable, SubmitErrorKind)>,
) -> pi_result::RawResult<(), SubmitError> {
    todo!("编号：T198，尚未实现， 目标：先核原runtime_id相同；Ordinary／Idle拒绝local_owner存在，Local核精确owner。失败返回同一UnsubmittedTask，不调用push。通过后将唯一原Runnable一次交push；Err必须是原值并重建同一身份和local_owner，保留原cause。只有来源真正接受才通知；在来源借用和保护之外notify，异常先有原责任守卫，通知前disarm。不得公开绕过原工厂身份的拆Rune入口，不接纳或poll业务。")
}
#[allow(unused_variables)]
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
    todo!("编号：T199，尚未实现， 目标：克隆原Schedule，用原QueueSupport及用户选项经pi_async_task::create一次构造；拆TaskParts后原Runnable到UnsubmittedTask，原join/control到TaskHandle，不另加TaskRef。native BuildError归还F、metadata及options重建原TaskConfig，保留BuildErrorKind；内部Schedule克隆自行释放，不扩散到外部错误。创建不取时、不提交、不接纳、不poll。")
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
    todo!(r#"编号：T200，尚未实现， 目标：复用原create_local(&TaskThread)，先保本地原值清理责任，UnsubmittedTask记录owner.id。输出始终LocalOutput，不能因为T偶然Send就跨线程。失败原F／配置返还；不得增加F／Output的Send、Sync或Unpin。无隐式接纳、提交或poll。"#)
}
#[allow(unused_variables)]
fn task_factory_notifier_inner<S: Schedule + Clone, N: TaskNotifier, N2: TaskNotifier>(
    this: &TaskFactory<S, N>,
    notifier: N2,
) -> TaskFactory<S, N2> {
    todo!("编号：T201，尚未实现， 目标：仅复制同一RuntimeId、原Schedule与QueueSupport，保存指定受限notifier；这是原工厂授权的同源组合，不分配新服务身份、不增加原任务引用。普通运行时不导出工厂。")
}
#[allow(unused_variables)]
fn task_factory_clone_inner<S: Schedule + Clone, N: TaskNotifier>(
    this: &TaskFactory<S, N>,
) -> TaskFactory<S, N> {
    todo!("编号：T202，尚未实现， 目标：同源复制固定RuntimeId、Schedule、notifier及QueueSupport；不新建服务或来源，不改变任何Task身份。")
}
#[allow(unused_variables)]
fn cancel_notify_drop_inner<N: TaskNotifier>(this: &mut CancelNotifyGuard<'_, N>) {
    todo!("编号：T203，尚未实现， 目标：只有armed时，先将armed=false，再调用notify_cancel一次；快照惰性从原control读取。普通展开仍保已记录取消，通知panic继续传播，不捕获或重复。不能把查询结果当取消成功或Future已drop。")
}
