fn runtime_binding_new_inner() -> Rc<RuntimeBinding> {
    todo!("编号：T267，尚未实现， P0；一份Rc<RuntimeBinding>，current为空；原线程内部共用，不跨线程，不含业务Future或本地设施强根。仅每运行时一次准备成本，不是每Task/poll分配。")
}
#[allow(unused_variables)]
fn runtime_binding_replace_inner(this: &RuntimeBinding, binding: LocalBinding) {
    todo!("编号：T268，尚未实现， P0；调用者已通过核心bind_current成功建立同目标新Token。先借current用replace装新Token、取旧Token，结束RefMut后才Drop旧值。不先丢旧Token再尝试绑定；错误目标/绑定失败原Some保持。旧Token只撤自身，不删除新的当前记录，遵守核心已有范围恢复。不能持借用调用用户Drop。")
}
#[allow(unused_variables)]
fn runtime_binding_revoke_inner(this: &RuntimeBinding) {
    todo!("编号：T269，尚未实现， P0；从current take到局部，先结束RefMut再Drop；空幂等。核心Token先valid=false/frame inactive，再恢复合法当前记录并释放旧公共持有。撤销中的重入见None，不重复Drop；首次panic不捕获。该Rc无跨线程权限，外线程request_close不直接触此Cell，原线程进入实际退出时必须先执行本方法。")
}
#[allow(unused_variables)]
fn runtime_source_new_inner(
    queue: MpscTaskQueue,
    binding: Rc<RuntimeBinding>,
) -> RuntimeExternalSource {
    todo!("编号：T270，尚未实现， P0；只移动原queue及同一RuntimeBinding Rc进真实字段，不创建第二来源/数量/线程或克隆任务。与拥有者共用同一绑定槽，原共享Handle仍是MpscTaskQueueHandle，不携带Rc。本构造无用户回调和额外分配。")
}
#[allow(unused_variables)]
fn runtime_source_close_inner(
    this: &mut RuntimeExternalSource,
) -> pi_result::Result<QueueCleanupProgress> {
    todo!("编号：T271，尚未实现， P0；先binding.revoke，再原queue.close恰一次，原Progress/Report直返。默认执行器真实关闭固定先E.close，故即使用户poll/hook/Drop中途request_close，也先撤本地权限再调用任何原来源清理和实际TaskThread Drop。普通clear不得撤权限；close幂等不重建绑定。不得将Complete改称Future清理成功，不加catch/锁/等待。")
}
#[allow(unused_variables)]
fn runtime_source_drop_inner(this: &mut RuntimeExternalSource) {
    todo!("编号：T272，尚未实现， P0；只先binding.revoke；原queue由Rust字段随后真实Drop，不能手工重复Drop/forget。覆盖执行器直接Drop及异常退出链，先于E本体用户清理和后续Idle/Local退场。revoke首次panic时queue仍在字段，由自动清理及外层原ExitGuard保余项；不调用fallible close吞错。原Token已先撤valid，后续共享句柄存续不恢复本地权限。")
}
