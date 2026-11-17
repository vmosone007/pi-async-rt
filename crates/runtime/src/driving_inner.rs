#[allow(unused_variables)]
fn runtime_once_inner<M, C, P, H>(
    this: &mut ConfiguredRuntime<M, C, P, H>,
) -> pi_result::Result<TurnResult>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    todo!("编号：T257，尚未实现， P0；先核DriveScope活动/原当前Task禁止嵌套，随后ensure_binding：Open建立或核实自动父Frame，Closing不新授本地权限。再核心with_selected同RuntimeHandle包住全部原executor.run_once，覆盖Closing且从未绑定时的公共目标检查和用户退出回调；不同目标在任何业务/退出回调前返原typed cause的pi_result InvalidState。BindingExitGuard覆盖服务调用；短借Data后拆executor/mode/policy/hooks，调用原公共run_once，不再建第二Scope/Lease。本地回调经弱设施而非再借Data。with_selected按原规则恢复前Frame；新自动父Frame在选择前安装，普通单轮返回保留，外层enter恢复旧目标时下次ensure再核。源包装实际close/Drop先撤权限，原Result/Report保持。")
}
#[allow(unused_variables)]
fn runtime_run_inner<M, C, P, H>(this: &mut ConfiguredRuntime<M, C, P, H>) -> pi_result::Result<()>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    todo!("编号：T258，尚未实现， P1；同run_once核活动、ensure自动父Frame，再with_selected同目标覆盖整个原executor.run；Closing无Token仍需公共目标检查，不在错误服务环境执行退出回调。BindingExitGuard覆盖，原执行器一次Scope/Lease负责整段轮转及主动/被动park，不外层重复公共run_once。原pi_result Report返回，选择失败原typed cause入InvalidState，不假关成功；原panic传播。不创建线程/重开来源，原来源包装在内部实际关闭前撤Token。")
}
#[allow(unused_variables)]
fn handle_block_on_inner<C: ExecutorClock<Instant = GlobalManualInstant>, F: Future>(
    this: &RuntimeHandle<C>,
    future: F,
) -> F::Output {
    todo!(r#"编号：T259，尚未实现， P0；首次poll前核全局活动DriveScope/原Task，非法嵌套明确panic。以核心with_selected选择同一原Handle（冲突原CurrentRuntimeAccessError用panic_any保原值），在action中仅核心block_on_root(future)，不取Data/Lease/本地权限/服务线程。关闭原服务仍可同目标独立根，不重新开放来源；同线程没有其它服务推进时await原任务可能持续等待，用户应选拥有者block_on或另行驱动。F栈固定无Box/Unpin/Send/Sync/static要求，根无TaskId；根Ready不等其它Task全部结束。普通Handle释放不cancel/close。"#)
}
#[allow(unused_variables)]
fn runtime_block_on_inner<M, C, P, H, F>(
    this: &ConfiguredRuntime<M, C, P, H>,
    future: F,
) -> F::Output
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    P: IdlePolicy,
    H: ExecutorHooks,
    F: Future,
{
    todo!("编号：T260，尚未实现， P0；首次poll前核全局活动推进/原Task。非Closed先ensure_binding，以原bind_current准备自动父Frame或核真实当前本地设施，Closing无绑定不新授权。随后所有状态统一核心with_selected同RuntimeHandle覆盖根和服务；不同目标原CurrentRuntimeAccessError用panic_any，绝不在其它服务环境poll根。Closed只核心block_on_root；其它建立BindingExitGuard，data.try_borrow_mut排他，取executor.begin_drive Lease（不建Scope），构造Driver借Data/RuntimeBinding/Handle/last None，核心block_on_with_driver唯一建Scope并栈Pin F。Driver只内部turn。根Ready立即返且本轮不再turn/park，不等子Task；根原地Drop和必要局部先于Scope/Lease，原选择最后恢复。服务错误panic_any原Report保链，原panic不catch。run_once持久自动父Frame在临时选择之前准备，显式外层范围仍按核心恢复。")
}
#[allow(unused_variables)]
fn runtime_driver_turn_inner<M, C, P, H>(driver: &mut RuntimeDriver<'_, M, C, P, H>) -> bool
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    todo!("编号：T261，尚未实现， P0；Closed仅binding.revoke且last置闭服务轮次，false；否则同Lease/原mode/P/H调用原turn_with_lease，保真实last并返回immediate_work。原来源包装在内部close/Drop开始前撤Token，不能等本函数看到Closed才首次撤。根已Ready时核心不会再调用本方法。原Error以panic_any同Report保source/context/attachment，不转字符串或伪F输出，正常无新增分配。")
}
#[allow(unused_variables)]
fn runtime_driver_park_inner<M, C, P, H>(
    driver: &mut RuntimeDriver<'_, M, C, P, H>,
    root: &RootWakeState,
) where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    todo!(r#"编号：T262，尚未实现， P0；Closed只root.park，不忙转已退出服务。Open/Closing需真实last；无last立即turn，不以默认快照睡。调用原park_with_lease(mode,last,Some(root))，新鲜同源时间/根与服务通知最终核验；不重复用户策略。原Report panic_any不丢链，最终检查到park无其它回调/日志/park，不延长已定被动上限。"#)
}
