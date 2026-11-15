#[allow(unused_variables)]
fn state_new_inner() -> ExecutorState {
    todo!("P0；flags零表示Open且未推进，idle零Unknown，两数量零；没有队列/线程/锁分配。")
}
#[allow(unused_variables)]
fn state_status_inner(this: &ExecutorState) -> RuntimeStatus {
    todo!("P0；Acquire读取一次flags，UNAVAILABLE优先Closed，CLOSE_REQUESTED为Closing，否则Open；不把关闭失败或无人驱动报成功完成。")
}
#[allow(unused_variables)]
fn state_close_inner(this: &ExecutorState) -> CloseState {
    todo!("P0；同一Acquire样本，COMPLETE才Completed，否则REQUEST或UNAVAILABLE为Requested，其余NotRequested；永久不回退。")
}
#[allow(unused_variables)]
fn state_driving_inner(this: &ExecutorState) -> bool {
    todo!("P0；Acquire单样本DRIVING，包括用户同步hook及停放，不包括共享句柄的根专用block_on。")
}
#[allow(unused_variables)]
fn state_idle_inner(this: &ExecutorState) -> IdleState {
    todo!("P0；Acquire一次idle，0Unknown、1Idle、2NotIdle；不调用策略或取队列。")
}
#[allow(unused_variables)]
fn state_request_inner(this: &ExecutorState, notifier: &ThreadNotifier) -> CloseRequestStatus {
    todo!("P1；AcqRel fetch_or REQUEST原子记录，旧UNAVAILABLE或COMPLETE返回AlreadyClosed，旧REQUEST AlreadyRequested，否则Requested；有效新/重复请求均用同一notifier安排最小检查，无等待，不在此关闭源或drop Future。")
}
#[allow(unused_variables)]
fn state_try_drive_inner(this: &ExecutorState) -> bool {
    todo!("P1；AcqRel fetch_or DRIVING，旧位已置则false且不清别人的位；首次取得为true。服务Closed仍可由上层选择根专用推进，不能因此重开服务。")
}
#[allow(unused_variables)]
fn state_release_drive_inner(this: &ExecutorState) {
    todo!("P0；仅成功取得的同一驱动守卫Release清DRIVING一次，不改其它事实。")
}
#[allow(unused_variables)]
fn state_record_idle_inner(this: &ExecutorState, value: IdleState) {
    todo!("P0；成功调用外部策略返回后Release发布对应0/1/2，失败/恐慌不伪造新判断。")
}
#[allow(unused_variables)]
fn state_finish_owner_inner(this: &ExecutorState, failed_in_flight: usize, cleanup_failed: bool) {
    todo!("P2；仅实际TaskThread已退出且所有来源责任完成或合法移交后调用；in_flight Release置已接纳未确认failed_in_flight。cleanup_failed必须包含原所属域全部失败（含未submit/admit的本地原值），及已知来源清理/异常失败，不能由已接纳数是否零代替。flags AcqRel发布UNAVAILABLE/REQUEST；只有cleanup_failed=false且未曾FAILED、failed_in_flight零才COMPLETE，否则FAILED且不完成。失败事实不因再次查询或句柄释放变成功；不捕获原panic，不允许域仅close但未实际退出就调用。")
}

#[allow(unused_variables)]
fn state_push_inner(
    this: &ExecutorState,
    queue: &mut TaskQueue,
    task: Runnable,
    front: bool,
) -> pi_result::RawResult<(), QueuePushError> {
    todo!("P1；在真正本地TaskQueue独占借用中，先Relaxed internal加一并建立InternalCountGuard；防回绕超限abort。只调用当前已核具体TaskQueue::push_front/back，不接收任意回调，原生入队接受后到返回无用户代码/恐慌点。成功先撤guard保留一次增加；明确Err保存完整QueuePushError，guard归还预留后再返，外层退出队列借用才处理原值；接受前展开同样归还。LocalReadySubmit必须通过此窄出口，不能成功入队却漏记；失败转共享publish时预留已归还，共享路径另记一次。已计数回送转本地用原TaskQueue压入而不调用本出口。这里只记队列持有，不admit/poll/取消或计in_flight。")
}
#[allow(unused_variables)]
fn state_count_guard_drop_inner(this: &mut InternalCountGuard<'_>) {
    todo!("P0；armed先撤，Relaxed internal减一次，不能下溢；只有未实际接受的本地入队预留使用，不调用用户代码/通知/队列。")
}
