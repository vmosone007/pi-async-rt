#[allow(unused_variables)]
fn registry_new_inner() -> TaskRegistry {
    todo!("编号：T58，尚未实现， P0；一份Arc Shared，head/incoming空，participants零，final_retired空；本地index空、retired/cursor/previous空，Rc标记。Box节点只由register逐任务准备；没有Task/线程/锁。")
}
#[allow(unused_variables)]
fn registry_handle_inner(this: &TaskRegistry) -> TaskRegistryHandle {
    todo!("编号：T59，尚未实现， P0；Arc克隆同一Shared，无新记录或管理权限。")
}
#[allow(unused_variables)]
fn registry_clone_inner(this: &TaskRegistryHandle) -> TaskRegistryHandle {
    todo!("编号：T60，尚未实现， P0；只Arc克隆原Shared，不保本地管理者或Future。")
}
#[allow(unused_variables)]
fn registry_enter_inner(this: &RegistryShared) -> Option<RegistryReadGuard<'_>> {
    todo!("编号：T61，尚未实现， P1；load Relaxed，SEALED则None且绝不再增加低位；否则强/弱CAS AcqRel增一，失败Relaxed按新样本重试。达到低位上限按标准引用计数资源极限abort，不回绕。此为锁自由竞争，不宣称wait-free；无等待/锁/分配。成功后才读取head或incoming。")
}
#[allow(unused_variables)]
fn registry_register_inner(this: &TaskRegistryHandle, query: TaskQuery) {
    todo!("编号：T62，尚未实现， P1；先enter，已seal直接在保护外Drop原query。入场后Box准备不可变query、空next/incoming_next/retired_next、indexed=false、PhantomPinned。head CAS Release发布同一Box一次，失败Relaxed重试更新私有next；随后incoming CAS Release发布非拥有索引链，写incoming_next仅在节点未入incoming前。生产者Guard覆盖读取旧head和整个双发布，防ABA/回收。不能在活节点上创建&mut整体，不把其中query移出。Box标准OOM沿分配器；成功create后调用，不伪造取回F的创建错误。最后退出Guard；即使owner同时seal，原参与者完成原发布，再由真正最后者收集。没有每poll/wake分配或用户回调。")
}
#[allow(unused_variables)]
fn registry_index_inner(this: &mut TaskRegistry) {
    todo!("编号：T63，尚未实现， \"P1；仅开放的本地管理者，incoming.swap(null,AcqRel")取得当前有限链，不追后来发布；用原始字段投影沿incoming_next遍历，插入TaskId->同一NonNull索引，再写indexed=true。该链不拥有Box且不释放节点；indexed前普通维护不能退役。先建RegistryIndexGuard保当前及余链，index扩容/展开时由其Drop无分配挂回incoming；逐项索引成功后标indexed再前移guard.remaining，不能让链悬空。不得跨调用返回裸指针。")
}
#[allow(unused_variables)]
fn registry_query_inner(this: &mut TaskRegistry, id: TaskId) -> Option<&TaskQuery> {
    todo!("编号：T64，尚未实现， P1；先refresh_index当前已发布有限链，再借index原节点不可变query，引用不超过本次管理者借用；不clone、移出或访问原Future。submit在register双发布之后才对外可用，真实来源交接提供先行关系；找不到明确拒绝该来源原值，不用伪造查询或穿透任务头。")
}
#[allow(unused_variables)]
fn registry_maintain_inner(this: &mut TaskRegistry, max_nodes: usize) {
    todo!("编号：T65，尚未实现， \"P2；每步最多max_nodes检查，借cursor/previous继续原链，不从每个poll全表扫描。只退役已indexed且query.snapshot phase=Ended、FutureDrop Confirmed的节点；未接纳但已合法Abandoned完成亦可。CleanupUnconfirmed保留供失败核对。头节点用Release CAS摘链，失败本步不强行删除/等新head；内部next仅单写者Release更新。先结束查询借用、从index移出、再用retired_next挂入本地私链；不能修改读者仍在用的query或next链接含义。需要保留原next供既有读者通过，退役链使用另一字段。末尾participants.fetch_add(0,AcqRel")==0才把当时私有退役链移给ReclaimGuard释放，且不再使用被释放cursor/previous。新进入者取得摘链先行关系，旧者计数阻止释放。max为零不扫但可合法回收旧私链；成本和游标存活须专项验证。")
}
#[allow(unused_variables)]
fn registry_waiting_inner(this: &TaskRegistryHandle) -> usize {
    todo!("编号：T66，尚未实现， P1；enter None仅在真实所属域退出之后，返回0。成功后Acquire取head和next，逐项先以snapshot.admitted筛本服务已接纳者，再调用原TaskQuery::is_waiting_for_wake；原接口不要求admitted，合法直接run未接纳也可true，所以不得省略上层筛选。该原接口负责根Pending交接完成及无唤醒取消退出条件；不读私有头，不派生子Future，block_on裸根无记录。无每项clone/分配/用户策略。保存数量后退出Guard，异常同样退出；跨任务/跨查询独立样本。")
}
#[allow(unused_variables)]
fn registry_cancelling_inner(this: &TaskRegistryHandle) -> usize {
    todo!("编号：T67，尚未实现， P1；同一读者保护，逐项一次snapshot；只计admitted且cancellation=Recorded、future_drop=Unconfirmed、execution不是CleanupUnconfirmed。FutureDropped/Ended已有最终结果，失败清理也有确定失败，不计待取消；不把终止原因None或队列数差当依据。计数溢出按真实任务数量不可能回绕的地址空间约束核实。只读不触发用户策略。")
}
#[allow(unused_variables)]
fn registry_unconfirmed_inner(
    this: &TaskRegistry,
    owner: pi_async_task::definitions::TaskOwnerId,
) -> (usize, usize) {
    todo!("编号：T68，尚未实现， \"P2；真实TaskThread完成最终退出后、本管理者seal之前，扫描现存head一次。只对snapshot.owner==Some(原owner")且FutureDrop仍Unconfirmed的终止失败：第一项total全部原域失败，包含create_local成功但未submit/admit者；第二项admitted只计已接纳子集，供in_flight保留。尚未绑定、仍由用户持有原Rune的Send创建不算本域失败；已被合法维护移除的成功结束不计。关闭还须组合来源清理已知失败，不能用admitted==0判断整体成功。原失败不重复poll/drop；无其它本地写者，原任务最后原子事实只读。")
}
#[allow(unused_variables)]
fn registry_owner_drop_inner(this: &mut TaskRegistry) {
    todo!("编号：T69，尚未实现， \"P2；调用前实际TaskThread必须已经完成真正Drop退出，原生来源责任也已结束或合法移交；后者不能替代真正所属域退出；先结束全部本地index/游标/next写操作，移空index，cursor/previous置空。把本地retired私链唯一交Shared.final_retired（未seal前只有本管理者写），之后participants.fetch_or(SEALED,AcqRel")，不得再访问节点/写incoming/head。旧低位零则自己collect_final，否则真正最后旧参与者负责；不等待读者/生产者，不新开线程。seal后新访问不登记，原在场发布仍完成入场时承诺的双发布。先发布垃圾链再seal，不让最后者见未初始化责任。")
}
#[allow(unused_variables)]
fn registry_read_drop_inner(this: &mut RegistryReadGuard<'_>) {
    todo!("编号：T70，尚未实现， P0；仅成功入场的一份Guard，Release fetch_sub一次；旧样本恰为SEALED|1时Acquire fence后唯一collect_final。未seal或仍有旧参与者不收集；seal后新读/发布完全不加数，故不会出现第二个最后者。不递归创建读者Guard，不捕获用户metadata Drop；保护本身无Arc克隆，外层真实借用保证Shared存续。")
}
#[allow(unused_variables)]
fn registry_final_inner(this: &RegistryShared) {
    todo!("编号：T71，尚未实现， \"P0；只在已seal且所有旧参与者结束时唯一调用；head.swap(null,Relaxed")取得全部尚存Box，final_retired取走原私链，incoming置空仅丢非拥有索引。两条链各自独立，不重复Box；以ReclaimGuard在已无读者/写者条件下显式正常循环逐项配对Box::from_raw释放，守卫只在异常时接续尚余项，不通过直接Drop满链执行正常清理。该最后归还可能在远端线程，但TaskQuery只释放存储持有，原Future必须已由原域结束，绝不跨线程访问它。不得等待或重新开放Shared。")
}
#[allow(unused_variables)]
fn registry_reclaim_drop_inner(this: &mut RegistryReclaimGuard) {
    todo!("编号：T72，尚未实现， P0；正常回收在守卫存续的外部循环执行；本Drop仅兜底尚余链，每次先取原retired_next并更新remaining，再Box::from_raw Drop一次；从head收集的链须先在无参与者条件下整理为独立retired_next链。失败原Box不重试；首次panic原样传播，尚余Box仍由本守卫继续，一次再panic沿进程规则，不新增catch。释放前无index/节点引用；不访问已释放Node或伪造TaskRef。")
}

#[allow(unused_variables)]
fn registry_index_drop_inner(this: &mut RegistryIndexGuard<'_>) {
    todo!("编号：T73，尚未实现， P0；remaining为当前未完成索引及余链。仅原管理者开放阶段，沿不可变incoming_next找到尾，CAS Release把整条非拥有链挂回shared.incoming，失败Relaxed改尾部旧head后重试；不分配/释放Box，不改读者next，不持TaskQuery引用到写入后。成功先清remaining；正常索引逐项成功后先标indexed并前移remaining，已完成项不重挂。管理者单写者此时不回收节点，生产者在参与者保护内，无ABA释放；不允许在seal后调用。")
}
