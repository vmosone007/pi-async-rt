fn runtime_binding_new_inner() -> Rc<RuntimeBinding> {
    Rc::new(RuntimeBinding { current: RefCell::new(None) })
}
#[allow(unused_variables)]
fn runtime_binding_replace_inner(this: &RuntimeBinding, binding: LocalBinding) {
        // 先装新 Token 再取旧 Token，借用结束后才析构旧值；失败调用不进入本函数。
    let previous = this.current.replace(Some(binding));
    drop(previous);
}
#[allow(unused_variables)]
fn runtime_binding_revoke_inner(this: &RuntimeBinding) {
        // 先 take 到局部，结束借用后再 Drop；空幂等，撤销中的重入看到 None。
    let previous = this.current.take();
    drop(previous);
}
#[allow(unused_variables)]
fn runtime_source_new_inner<E: ExternalQueueManager>(
    queue: E,
    binding: Rc<RuntimeBinding>,
) -> RuntimeExternalSource<E> {
        // 只移动原 queue 与同一绑定 Rc；不新增来源、数量、线程或克隆任务。
    RuntimeExternalSource { queue, binding }
}
#[allow(unused_variables)]
fn runtime_source_close_inner<E: ExternalQueueManager>(
    this: &mut RuntimeExternalSource<E>,
) -> pi_result::Result<QueueCleanupProgress> {
        // 任何来源清理之前先撤本地权限；close 幂等且不重建绑定。
    this.binding.revoke();
    this.queue.close()
}
#[allow(unused_variables)]
fn runtime_source_drop_inner<E: ExternalQueueManager>(this: &mut RuntimeExternalSource<E>) {
        // 只先撤本地权限；原 queue 由 Rust 字段随后真实析构，不重复 Drop、不 forget。
    this.binding.revoke();
}
