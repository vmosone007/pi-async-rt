#[allow(unused_variables)]
fn hook_event_default_inner(event: ExecutorEvent, task: &TaskInfo<'_>) {
    todo!("P0；默认空操作，不采样metadata/格式化/分配/日志/计数，不控制调度；实际替换为空体即可。泛型静态调用，不要求UserHook Send/Sync。")
}
#[allow(unused_variables)]
fn hook_control_default_inner(task: &TaskInfo<'_>) -> TaskAction {
    todo!("P0；默认TaskAction::Poll，不额外查状态/metadata，不分配。真正执行前仍由外层及原任务核心核取消/关闭，不能用默认值绕过。")
}
