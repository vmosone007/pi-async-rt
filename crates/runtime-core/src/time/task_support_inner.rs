#[allow(unused_variables)]
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
    todo!(r#"编号：T229，尚未实现， P1；保持原spawn_timed_task_inner语义，只增加创建后、提交前register一次。先核factory/access原RuntimeId、时长零或>=1ms、deadline_after一次、prepare_registration(Task)，准备失败CreateError保原F/config及原TimerError。TimedTaskNotifier组合原通知后factory.with_notifier创建同一原Task；创建成功立刻handle.query_handle移交register，在它完成前不得begin/入队或导出结果。随后原Factory.submit(Ordinary)以同一UnsubmittedTask移出Rune，registration.begin取得责任，最外QueryRegistrationGuard先建立，TimerEntry::task唯一包原Rune，try_register一次。明确拒绝recover原Rune，DirectFailure结束登记，SubmitError::Timer保原UnsubmittedTask及原TaskHandle；不取消/重新admit/poll。成功publish_key一次，已有取消经KeyPublished正常接续（原错误交预备节点不伪拒绝），解除守卫后返原handle。接受后panic守卫保取消责任；register panic时未接受，原值按已有Abandoned路径清理，不捕获。零时长仍真实定时登记，与timeout(0)不同。无额外F约束、不分配新Task/锁、不改旧函数体。"#)
}
#[allow(unused_variables)]
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
    todo!(r#"编号：T230，尚未实现， P1；与配对普通query入口相同阶段、错误、键和守卫协议；只改factory.create_local(owner,...)、提交模式Local(owner.id())和access.try_register_local。本地权限须由外层在原Current范围先验证；本函数沿TaskThread核原线程，F/输出不增Send/Sync。register仍在真实来源可见前且恰一次；本地拒绝保原Rune和LocalOutput handle，不把无法投递变为跨服务共享回退，不重取时/重建Task。原本地Task即使注册回调panic也仍由原TaskThread负责清理，不从当前线程外drop原Future。"#)
}
#[allow(unused_variables)]
fn query_registration_drop_inner<A: TimerAccess>(this: &mut QueryRegistrationGuard<A>) {
    todo!("编号：T231，尚未实现， P0；先撤armed，已开始交接而未完整结束时按原RegistrationGuard/Detached规则请求原登记取消并安排预备接续；无返回口原TimerQueueError移交节点，不能字符串化、吞掉或冒充来源未接受。键未就绪不等；原TimerEntry实际移除自行发布Released，原Rune合法清理。明确拒绝已DirectFailure并撤守卫时不再重发。首次panic沿原范围传播，不新catch/每次分配/后台Task。")
}
