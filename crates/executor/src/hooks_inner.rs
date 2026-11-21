#[allow(unused_variables)]
fn hook_event_default_inner(event: ExecutorEvent, task: &TaskInfo<'_>) {

}
#[allow(unused_variables)]
fn hook_control_default_inner(task: &TaskInfo<'_>) -> TaskAction {
    TaskAction::Poll
}
