#[allow(unused_variables)]
fn return_new_inner(
    owner: TaskOwnerId,
    notifier: ThreadNotifier,
    counts: Arc<ExecutorState>,
    batch: NonZeroUsize,
) -> pi_result::Result<ReturnQueue> {
    todo!("P0；准备不同的原生stub和clear终点各一MpscEntryHandle::try_new_empty，失败返原Report，配对释放已建者；new_with_stub接管stub，endpoint Available(另一原值)，state零，原owner/notifier/counts/batch不可换源。标准Arc分配策略保持。")
}
#[allow(unused_variables)]
fn return_handle_inner(this: &ReturnQueue) -> ReturnQueueHandle {
    todo!("P0；Arc克隆同一Shared，无新来源/所属域。")
}
#[allow(unused_variables)]
fn return_clone_inner(this: &ReturnQueueHandle) -> ReturnQueueHandle {
    todo!("P0；Arc克隆同一Shared，不复制Runnable或Consumer。")
}
#[allow(unused_variables)]
fn return_len_inner(this: &ReturnQueueHandle) -> usize {
    todo!("P0；Relaxed读取state低位单样本，包含尚未完成发布和权限外清理；不与internal总数混同，不证明原Future清理。")
}
#[allow(unused_variables)]
fn return_publish_inner(
    this: &ReturnQueueHandle,
    task: Runnable,
) -> pi_result::RawResult<(), Runnable> {
    todo!("P1；snapshot.owner必须同源且admitted或(Exiting且已有termination)，否则原样Err；MpscEntryHandle::try_from_runnable失败原样Err。最外Completion先建，再AcqRel state加一及Relaxed internal加一并arm；超出低位表示范围abort，禁止回绕/返回伪拒绝。唯一Handle enqueue一次，Thread通知，可靠退出确认。登记后无可返回失败/分配/用户代码；OWNER_GONE仍允许真实原归属交接并由本出口帮助，只清理不poll。正常不锁、不等其它参与者、不增TaskRef。")
}
#[allow(unused_variables)]
fn return_take_inner(this: &ReturnShared) -> ReturnTake {
    todo!("P1；先Completion后无分配CleanupGuard再ConsumerGuard，try_consume一次；Busy原分支。OWNER_GONE则归还权限、合法自动帮助并Closed。开放时try_dequeue一次，业务原Handle安全恢复Runnable并实际放独立结果后在Consumer内Release减本来源数；internal不减，外层转本地队列或选出时处理。独立结果和endpoint不在Consumer内Drop，真实Empty/WouldBlock保持；遇终点保存Reached并继续至多本步batch，不把空节点交用户。最末权限归还再可靠确认，正常队首选择不轮询Future。")
}
#[allow(unused_variables)]
fn return_batch_inner(this: &ReturnQueueHandle) -> BatchAttempt {
    todo!("P1；一次Acquire RMW固定本次count，先准备不超该上界的VecDeque结果，再尝试一次Consumer；不循环扩充范围或在Consumer内分配。按实际预留容量和当次上界安全取原Rune，放独立TaskBatch后一次减来源数，internal保持。遇缺口/Empty停止，已有非空前缀正常Ready；无前缀映射Empty/WouldBlock/Busy/Closed。与单项共享终点规则和Completion顺序。默认执行器使用单项有限回送预算，不依赖本批次方法。")
}
#[allow(unused_variables)]
fn return_clear_inner(this: &mut ReturnQueue) -> pi_result::Result<QueueCleanupProgress> {
    todo!("P2；本地唯一管理入口调用cleanup(false)一次，Complete对应Complete，其余MoreWork；仅本唯一管理者确认clear完成；正常共享消费者只能保存Reached，不提前复用。Consumer内首次把Available原终点移入队尾形成固定范围，不能代用stub/追后来新清理Rune。原值均在Consumer外合法Drop，复用原任务取消路径，不直接清Future。失败原责任不丢，不停普通归属接续。")
}
#[allow(unused_variables)]
fn return_close_inner(this: &mut ReturnQueue) -> QueueCleanupProgress {
    todo!("P2；调用者已结束实际TaskThread原值责任才允许，AcqRel标OWNER_GONE/REQUEST，立即无分配帮助；以最终joint proof决定Complete，否则MoreWork且交真实参与者。禁止先关该接续再遗留活非Send Future；这是维护原有任务的尾部交接，不重新接收新业务。")
}
#[allow(unused_variables)]
fn return_owner_drop_inner(this: &mut ReturnQueue) {
    todo!("P0；宿主字段Drop保证原TaskThread及本地/定时原值已先退出；AcqRel置OWNER_GONE/REQUEST并assist，不等最后共享句柄。原Queue默认Drop不能代替最后发布者接续，不新增catch。")
}
#[allow(unused_variables)]
fn return_request_inner(this: &ReturnShared) {
    todo!("P0；AcqRel OR REQUEST，仅独立帮助首次进入或确有消费/合法清理减数/终点进展后登记，纯空重查不登记。")
}
#[allow(unused_variables)]
fn return_confirm_inner(this: &ReturnShared) -> bool {
    todo!("P1；AcqRel零增量RMW可靠确认OWNER_GONE，真实发布/消费/权限外清理独立出口见退出便进入帮助，不要求旧REQUEST仍为1；不普通load后退出。")
}
#[allow(unused_variables)]
fn return_cleanup_inner(this: &ReturnShared, exiting: bool) -> ReturnStep {
    todo!("P2；普通clear最外Completion，退出帮助不递归建Completion；其次无分配CleanupGuard，Consumer最后。每步最多batch节点，Consumer内AcqRel清旧请求；clear建立/接续独立固定终点，退出则只接续已有终点且清全部尾部。每次安全取业务Rune放current，结束链接引用归还Consumer，再Drop原Rune，Release减来源数和internal各一次，撤debt；完成真实进展才重请。可逐项重新try Consumer，失败交实际持有者不等。开放clear只有本管理者能内部清理，归还Consumer后完成本步current/debt才再次取得Consumer确认Reached并恢复Available；共享普通取出不确认终点。不得以包含后缀的全局count作为固定范围完成条件。OwnerGone后则须共同证明count零才确认退出。最终完整Empty+可靠count零+终点Available优先请求；否则新REQUEST必须MoreWork重查但本重查不再请求；缺口/权限外欠账由真实参与者负责才Deferred。禁止只因count零或旧Empty报告完成。")
}
#[allow(unused_variables)]
fn return_consumer_drop_inner(this: &mut ReturnConsumerGuard<'_>) {
    todo!("P0；take归还Consumer，如有Transferring终点，先把guard.endpoint的同一持有恢复Available或完成原定交接，不得留下Transferring；再结束全部节点/endpoint引用。不Drop业务原值/通知/清理Future。")
}
#[allow(unused_variables)]
fn return_completion_drop_inner(this: &mut ReturnCompletionGuard<'_>) {
    todo!("P0；先撤armed，再在Consumer归还及当前原值处理/数量结算后confirm；退出帮助中不安装同类递归触发器。")
}
#[allow(unused_variables)]
fn return_cleanup_drop_inner(this: &mut ReturnCleanupGuard<'_>) {
    todo!("P0；当前Rune先take，在Consumer外Drop；debt先撤并保证该次合法清理/移交或首次异常后来源数和internal各Release归还一次，失败Rune不再Drop。退出时栈上单项无分配接续余项，第一次请求一次、每步重置真实进展，完整结束优先新请求，其余新请求重查不续请。已有参与者接续才返回，不catch、不等发布、不新建同类递归guard；二次panic沿原进程规则。")
}
#[allow(unused_variables)]
fn return_notify_inner(this: &ReturnQueueHandle) {
    todo!("P0；调用同一原ThreadNotifier的QueueNotifier::notify，Release记录并按需unpark；本地快速入队成功也用此入口通知，不增加队列计数，不poll，不访问原Future。")
}
