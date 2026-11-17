#[allow(unused_variables)]
fn runtime_with_local_inner<C: ExecutorClock<Instant = GlobalManualInstant>, R>(
    action: impl FnOnce(&RuntimeHandle<C>, &ExecutorLocal<Timer<C>>) -> R,
) -> pi_result::RawResult<R, LocalRuntimeAccessError> {
    todo!("编号：T248，尚未实现， P1；直接复用核心try_with_current_local::<RuntimeHandle<C>,ExecutorLocal<Timer<C>>,R>；它结束TLS RefCell借用后核原RuntimeId/owner/有效标记/Weak及类型，再调用action。无环境/错误类型/已撤销返回原LocalRuntimeAccessError，不默认创建服务、不跨服务回退、不clone Handle Arc以便同步借用快速；引用不跨await。Serial/Concurrent的Handle相同，静态本地方法定位实际当前服务，不改变其模式。")
}
#[allow(unused_variables)]
fn runtime_bind_inner<M, C, P, H>(runtime: &ConfiguredRuntime<M, C, P, H>) -> pi_result::Result<()>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    todo!("编号：T249，尚未实现， P0；先用核心try_with_local_for::<ExecutorLocal<Timer<C>>>核当前同RuntimeId真实owner/valid和原设施，而不是只凭binding.is_present。已有Token且该当前本地访问确实有效才快速复用。显式Handle.enter退出会恢复前Frame，Some Token可能已不在当前链；Open时经短Data借用取原local Rc，结束借用，再local.with_owner调用bind_current同Handle/owner/&local，新Token成功后binding.replace，RefMut外归还旧Token。新绑定失败保旧Token并将原CurrentRuntimeAccessError作为pi_result InvalidState source；不同目标不覆写。Closing/Closed不新建本地权限（已有可达的可保持至真正退出包装撤销），仍允许后续公共选择覆盖合法关闭推进。每次实际驱动在ensure后另用with_selected核原目标；本地权限和公共选择不能互相替代。构造不绑定，普通run_once返回仍有自动父Frame；若外层显式enter随后恢复旧目标，只承诺下次合法驱动重新核/建，不改核心SelectionGuard恢复。无TLS/容器借用跨回调，无锁/自旋。")
}
#[allow(unused_variables)]
fn binding_exit_inner<C: ExecutorClock<Instant = GlobalManualInstant>>(
    guard: &mut BindingExitGuard<'_, C>,
) {
    todo!("编号：T250，尚未实现， P0；Closed时只幂等调用guard.binding.revoke，作为原来源包装先撤权限之后的末尾校验；不可把它当唯一退出前撤销点。Open单轮返回不撤；Closing未开始退出仍以原真实接续为准。正常/错误/展开覆盖，revoke本身先结束RefMut再Drop Token，不新catch，不删除其它当前范围。")
}
