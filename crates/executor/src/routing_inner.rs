#[allow(unused_variables)]
fn route_new_inner<L: 'static>(queue: ReturnQueueHandle) -> ReturnRoute<L> {
    todo!("P0；一份Arc身份槽未初始化phase=0，保存同一queue，PhantomData仅fn()->L；无本地Rc/裸指针，无Task/线程。")
}
#[allow(unused_variables)]
fn route_clone_inner<L: 'static>(this: &ReturnRoute<L>) -> ReturnRoute<L> {
    todo!("P0；只克隆原queue和identity Arc，不要求L Clone/Send/Sync；不制造新身份。")
}
#[allow(unused_variables)]
fn route_set_id_inner<L: 'static>(this: &ReturnRoute<L>, runtime: RuntimeId) {
    todo!("P0；只在完整执行器构造、用户设施未暴露前调用，强CAS 0->1 AcqRel取得唯一写资格，失败明确panic不重复写；原始投影写MaybeUninit RuntimeId，Release置2。TaskFactory try_new决定原id，随后本方法发布；不从裸u64重建私有RuntimeId、不等待其它写者。")
}
#[allow(unused_variables)]
fn recovery_new_inner(
    queue: ReturnQueueHandle,
    state: Arc<ExecutorState>,
    notifier: ThreadNotifier,
) -> ReturnRecovery {
    todo!("P0；保存原返回入口及相同state/notifier，无新队列/线程/每Task分配。")
}
#[allow(unused_variables)]
fn route_push_inner<L: LocalReadySubmit>(
    this: &ReturnRoute<L>,
    task: Runnable,
) -> pi_result::RawResult<(), ExternalPushError> {
    todo!("P1；Acquire确认phase=2才读取不可变RuntimeId并L::try_push_local；本地实现必须核原服务/线程/有效性和短暂队列借用。成功就Thread通知；失败返回同一task后进入原queue.publish，不借本地队列到外线程。身份未就绪亦只共享返回，不等。默认原Task有两套链接且原域匹配，publish不会因CAS预算/队列容量拒绝；Native WakeTarget已经在本接口之前拒绝错误owner。此处若原域仍异常不符，保持原Rune并明确panic，按原Schedule异常边界传播，不伪造现有有限错误分类；缺MPSC链接则ExternalPushError(LinkMissing)返原Rune，不能伪成功。共享路径通知由publish负责。本适配不admit/poll/drop原Future，不用Native LocalQueueGuard。")
}
#[allow(unused_variables)]
fn recovery_retain_inner(this: &ReturnRecovery, task: Runnable, reason: DispatchFailure) {
    todo!("P2；只处理原WakeTarget正常Err后的原Runnable。先原queue.publish保既有归属责任；成功即返回。真正不符原域/缺链接的非默认输入不能无限重试或丢给Waker调用者：原state请求整体关闭并通知，随后在队列/TLS/Consumer保护外drop同一Rune，按原Task机制交回其真正所属域；同域首次Abandoned最多另产一份Exiting清理持有，后者再Drop不递归调度，原TaskThread仍保住原值并由本服务关闭清理。外域原Task的Schedule自行接回，不能在本线程poll它。默认工厂选择两链接，合法正常热路径不进入此失败分支；保原reason作为本地诊断分类，不以该路径声称任务成功清理。不catch、不建后台任务/新锁/无界等待。")
}
