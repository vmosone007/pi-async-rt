#[allow(unused_variables)]
fn local_owner_inner<T: ExecutorTimer + 'static, R>(
    this: &ExecutorLocal<T>,
    action: impl FnOnce(&TaskThread) -> R,
) -> pi_result::Result<R> {
    todo!("P1；先建LocalActivityGuard后借owner不可变Ref、核Some及原线程，调用同步action后结束Ref；不同时借ready/timer/registry/TLS。无owner用pi_result InvalidState，不伪造默认R。核心TLS升级Rc已覆盖活动；若action调用本地创建可重入同一owner的不可变借用。action panic不捕获，Ref先自动退出；ActivityGuard最后在无借用时接续已请求退出。")
}
#[allow(unused_variables)]
fn local_push_inner<T: ExecutorTimer + 'static>(
    this: &ExecutorLocal<T>,
    task: Runnable,
    front: bool,
) -> pi_result::RawResult<(), QueuePushError> {
    todo!("P1；核原线程/owner、服务Open及设施有效。创建本地任务本已绑定本owner但未admit，允许先排队；首次拒绝绝不admit/cancel/poll。只短借ready，按front调用state.push_front/back，计数入口配对；结束借用后notify。失败保原Rune及现有QueuePushErrorKind（Closed/WrongOwner/LinkMissing等），不发明不存在的Busy。ready为私有，真实push/pop无用户代码，清理take整队列后才Drop，故合法调用没有借用重入窗口；若内部破坏该边界，明确panic，不伪造外部可恢复拒绝。原归属Wake在Closing仍需合法退出：路由此入口拒绝后回原共享返回，由OwnerGone机制清理，不改归属。无新增分配/锁/TaskRef。")
}
#[allow(unused_variables)]
fn local_timer_push_inner<T: ExecutorTimer + 'static>(
    this: &ExecutorLocal<T>,
    deadline: T::Instant,
    item: T::Item,
) -> pi_result::RawResult<T::Key, TimerSubmitError<T::Item>> {
    todo!("P1；先核同源/原线程/有效Open，try_borrow_mut timer仅覆盖try_push_local，返回原独立结果后结束借用；失败保原item，暂借冲突Busy，失效Closed，不走另一服务。来源自身内部清理回调按其原协议；外层不持此RefMut再调用IntoDue、hook或业务poll。deadline来自同一显式时钟；不二次取时、不添加Item Send/Sync。")
}
#[allow(unused_variables)]
fn local_selected_inner<T: ExecutorTimer + 'static>(
    this: &ExecutorLocal<T>,
    task: Runnable,
) -> pi_result::Result<PreparedTask> {
    todo!("P0；先取得原snapshot核owner，已确认Ended/FutureDrop或CleanupUnconfirmed的本域迟到Rune归Settled，不要求已退役查询、不重试清理。其它先按原TaskId刷新registry核原查询，缺记录InvalidState且原Rune在借用外合法放弃。取消Recorded或Exiting者归Cleanup，不论是否admitted，不能为了取消隐式admit或调用业务hook；外层直接native run原owner仅清理。正常已admitted复用active Registered Rc，Interrupted只可走退出分支。正常尚未admit先准备active HashMap/VecDeque容量，然后借原Rune建立AdmissionAttemptGuard再调用admit；原生observe回调可在ADMITTED后panic，守卫必须先于Rune Drop补账。成功RcRegistration、active Registered和order稳定后Release加一次in_flight并撤守卫；所有正常Err同样先撤armed再分类，补账只承接异常展开；固定TaskId插入不调用户Hash/分配。admit拒绝AlreadyEnded时只重取一次单调snapshot，若已取消/退出则归Cleanup/Settled，不空转重admit；其它错误原样报告。所有active/index借用在原生admit（用户observer）及返回前结束。")
}
#[allow(unused_variables)]
fn local_maintain_inner<T: ExecutorTimer + 'static>(this: &ExecutorLocal<T>, max_nodes: usize) {
    todo!("P1；按active_order轮转最多max_nodes个已登记Id，不每次从HashMap固定前缀重扫。Registered或Interrupted均只读原snapshot，短借map读完立即放开；仅Ended且FutureDrop Confirmed从active/order移出并Release减一次in_flight，移出的ActiveRecord在借用外drop。CleanupUnconfirmed留原计数和失败事实，不无限等待/重drop。未结束者按原次序压尾；Rc/冷路径Query最后归还和metadata回调必须在所有map/order借用外。registry.maintain同额度另步，不穿透原任务状态。HashMap/VecDeque只接纳时扩容，正常轮转不分配。")
}
#[allow(unused_variables)]
fn local_finish_exit_inner<T: ExecutorTimer + 'static>(
    this: &ExecutorLocal<T>,
    source_failed: bool,
) -> bool {
    todo!("P0；单调记录exit_requested及已有来源失败。已exit_finished幂等true；exit_running已true则仅合并意图返回false，绝不以owner None当真正Drop结束。owner尚被同步活动借用时try_borrow_mut失败返回false保设施，不等待。可取得时结束试借，先exit_running=true建立唯一LocalExitGuard再drain；原TaskThread Drop中重入with_owner失败后的ActivityGuard也只返回false，不能启动第二相位执行者。先take原owner结束RefMut再真正drop；实际Drop完成后才Returns OWNER_GONE和registry seal。完成/首错余项均由原唯一guard最后发布finished并撤running；仅本阶段未返回标记失败，不把入场已展开误记新错误。最终按原owner核全域及admitted未确认数、合并来源失败，state.finish_owner，不以数量0/合法移交替真正域退出。")
}
#[allow(unused_variables)]
fn local_drop_inner<T: ExecutorTimer + 'static>(this: &mut ExecutorLocal<T>) {
    todo!("P0；最后Rc已排除活动借用，沿finish_exit真实清理；已完成无重复。若未完成则以独占get_mut确认并取所有Option，不能因Weak仍存在依赖Rc::get_mut（有Weak时它会失败）。同一LocalExitGuard完成正常与展开余项，原TaskThread先真实退出，原首次异常传播，第二次按原Rust/任务规则。退出后字段自动Drop只见空Option/空active，不二次drop。")
}
#[allow(unused_variables)]
fn local_exit_drain_inner<T: ExecutorTimer + 'static>(guard: &mut LocalExitGuard<'_, T>) {
    todo!("P0；状态顺序Owner→Ready→Timer→Returns→Active→Registry→Finish→Done，唯一exit_running资格保持至Finish。单值Option步骤先推进phase/置current_failed，再take并结束RefCell访问后drop；正常清current_failed。Owner真Drop后核registry.unconfirmed_counts；Ready取出以len建ReadyReleaseGuard再drop，正常/展开各一次归还internal；Timer直接drop来源拥有者，由它永久停收/清理或移交；Returns此后close_after_owner再drop。Active步骤逐项remove出ActiveRecord，先放借用再drop，未清空前phase保持Active，首错后继续余项，不将整map阶段提前跳过；Registry最后take/drop才seal。显式close原Err由外层保存返回，source_failed单调合并，不等所有共享句柄。Finish按原域全失败及admitted未确认决定state，先置finished再撤running，失败Closed但Requested；首次panic由本guard继续已经移出原值之后的责任，不重drop。")
}
#[allow(unused_variables)]
fn local_exit_drop_inner<T: ExecutorTimer + 'static>(guard: &mut LocalExitGuard<'_, T>) {
    todo!("P0；若current_failed说明上一真正阶段未正常返回，单调置failed并清该局部标记；从已经推进的下个phase drain剩余责任。正常路径必须显式drain，Drop不代替常规收尾；Done无事。没有递归重新构造同类守卫/新catch/等待。首次异常不被替换；清理再次panic按语言规则abort，不宣称可恢复。")
}
#[allow(unused_variables)]
fn local_activity_drop_inner<T: ExecutorTimer + 'static>(guard: &mut LocalActivityGuard<'_, T>) {
    todo!("P0；所有随后建立的owner/队列借用已先结束；仅exit_requested、未finished且exit_running=false才接续finish_exit。正在真正Drop原owner中的重入只归还本次活动，不递归新建退出守卫，不以owner None确认完成。没有请求直接返回，不为活动自旋；首次异常照常传播，额外清理panic按语言/任务原规则。")
}
#[allow(unused_variables)]
fn ready_release_inner(guard: &mut ReadyReleaseGuard<'_>) {
    todo!("P0；先mem::take count，再对internal Release一次fetch_sub原固定len；零不操作。只结算已移走整份本地队列的持有，回送新发布另有计数，不把internal置零覆盖并发生产者。正常/展开均执行，无用户回调/分配/循环。")
}
#[allow(unused_variables)]
fn local_new_inner<T: ExecutorTimer + 'static>(
    runtime: RuntimeId,
    owner: TaskThread,
    ready: TaskQueue,
    timer: T,
    returns: ReturnQueue,
    registry: TaskRegistry,
    state: Arc<ExecutorState>,
    notifier: ThreadNotifier,
) -> ExecutorLocal<T> {
    todo!("P0；原参数同owner/RuntimeId，移动进各RefCell<Option>，保存owner.id，active及order用new空值不分配，exit_requested/running/finished/source_failed均false。这里只作不可失败字段组装，不调用户/容量准备/时钟，不克隆管理者/新建TaskThread。完成Self后原构造守卫各Option已空；容量在真实接纳前准备。")
}
#[allow(unused_variables)]
fn admission_attempt_drop_inner<T: ExecutorTimer + 'static>(
    guard: &mut AdmissionAttemptGuard<'_, T>,
) {
    todo!("P0；armed先撤，正常成功已disarm立即返回。异常时原Rune仍被借用尚未Drop，核原snapshot：仅owner匹配原域、admitted、FutureDrop未确认且active缺Id时补账。冷路径registry.query(id)可刷新新增incoming/准备索引，clone一份原TaskQuery（不构造假TaskRegistration）进ActiveRecord::Interrupted，利用入场预留容量同时登记order，Release加in_flight一次；无admitted或已确认结束不加。所有registry/map借用在任何冷路径Query释放前结束，原observer首次panic仍传播；不catch/重admit/访问F。正常路径无额外TaskRef/分配，冷路径索引工作及原查询克隆成本如实保留。")
}
