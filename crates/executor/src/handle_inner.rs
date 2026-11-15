#[allow(unused_variables)]
fn handle_clone_inner<
    E: ExternalTaskQueue + Clone,
    I: IdleTaskQueue + Clone,
    T: ExecutorTimer + 'static,
    L: LocalReadySubmit,
>(
    this: &ExecutorHandle<E, I, T, L>,
) -> ExecutorHandle<E, I, T, L> {
    todo!("P1；逐项克隆原工厂、受限来源、计时访问/查询、state Arc、registry和notifier，复制原owner和类型标记；无新RuntimeId/新来源/本地Rc/管理权限，不要求T或L Clone/Send/Sync。")
}
#[allow(unused_variables)]
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
    todo!("P1；factory.create一次，成功后registry.register(handle.query_handle())先完成，再返同一元组；不提交/接纳/poll，原错误及输入直返。create在Closed仍可产生未绑定原值，提交将被拒绝；sealed registry会直接归还本份只读查询，不影响用户持有的未绑定Send原Future。注册分配panic走原Rune放弃路径，不捕获。")
}
#[allow(unused_variables)]
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
    todo!("P0；factory.submit按Ordinary或Idle核原身份/首次模式；回调先Acquire读Open，Closing/Closed以对应External::Closed或Idle::Closed返同一Rune，创建/关闭竞态的最终接收仍由原来源停收协议裁定。Open调用原external/idle.try_push一次，拒绝保原cause分类及同一原值。成功由通知守卫在发布后notify，通知panic不能改成拒绝。未admit/未poll，不能把local创建交普通共享源；源自身已接收后的异常继续由它负责。")
}
#[allow(unused_variables)]
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
    todo!("P1；先核local.runtime_id==factory.id及local owner==原owner，错误CreateError LocalContext原原因保原F/config。在local.with_owner内调用factory.create_local，只持共享owner Ref，结果原元组独立后结束借用，再登记TaskQuery后返用户。无owner/已关闭沿OwnerClosed/原本地错误；不能因取不到owner丢失F/config，先验证可借/有效后才移动F进入闭包，或以Option局部取回，不额外装箱F。F/Output不增Send/Sync/Unpin。")
}
#[allow(unused_variables)]
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
    todo!("P0；factory.submit Local原owner，先核local.runtime与owner；回调直接local.try_push同一Rune按front到头或尾，默认尾。TaskQueue允许原已绑定但未admit任务入队，因此拒绝前不调用admit；成功之后选出时才admit/加in_flight。QueuePushError保task/cause，错线程/模式/原服务均返原UnsubmittedTask，不转共享源、不主动取消。notify成功快路径合并即可，无第二份执行持有；队首优先不抢断正在poll的Future。")
}
#[allow(unused_variables)]
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
    todo!("P1；仅调用核心time::task_support::spawn_task_with_query，传原factory/timer、原F/delay/config和一次registry.register闭包；回调先于来源可见，不能在函数返回后才登记以致其它执行器提前取走。保持核心错误、原值、deadline一次、键和取消责任，不复制核心算法或包普通Task重新派发。")
}
#[allow(unused_variables)]
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
    todo!("P1；核同RuntimeId/原owner/有效本地权限后，经local.with_owner调用核心spawn_local_task_with_query并传登记闭包；owner共享Ref可跨本次同步构造，不持timer/registry/TLS借用。进入前用Option保存原F/config以便无法借owner时返原CreateError，禁止先移值后以无法恢复为由吞错。核心注册在来源接收前完成；无Send/Sync/Unpin附加约束，不向其它运行时回退。")
}
#[allow(unused_variables)]
fn handle_internal_count_inner<
    E: ExternalTaskQueue + Clone,
    I: IdleTaskQueue + Clone,
    T: ExecutorTimer + 'static,
    L: LocalReadySubmit,
>(
    this: &ExecutorHandle<E, I, T, L>,
) -> usize {
    todo!("P1；一次Acquire load state.internal，不扫队列/不推进，不包含纯Pending；回送在途和本地容器共享同一份数量。")
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
    todo!("P1；一次Acquire load state.in_flight；只在接纳登记后加、停止poll且Future确认清理后减。失败退出保admitted未确认数，不用external/internal相减，不许置零伪成功。")
}
#[allow(unused_variables)]
fn source_notify_drop_inner(guard: &mut SourceNotifyGuard<'_>) {
    todo!("P0；先撤armed，原生产尝试可能已经发布时经ThreadNotifier.notify一次；守卫在调用共享try_push之前建立并激活，明确Err时撤销，成功正常/异常均通知。panic前未接受也允许一次保守通知，不能为了绝对零伪通知丢失已经接受的工作。notify不调用用户代码、不分配，不重复发送任务。")
}
