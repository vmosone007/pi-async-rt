//! 只读任务（Task）统计与创建记录；不修改任务本体状态。
use pi_async_runtime_core::task::TaskQuery;
use pi_async_task::definitions::TaskId;
use std::{
    cell::UnsafeCell,
    collections::HashMap,
    marker::{PhantomData, PhantomPinned},
    ptr::NonNull,
    rc::Rc,
    sync::{
        atomic::{AtomicPtr, AtomicUsize},
        Arc,
    },
};

/// 同一服务的只读统计访问；克隆（Clone）只共享记录，不控制原任务（Task）。
pub struct TaskRegistryHandle {
    shared: Arc<RegistryShared>,
}
pub(crate) struct TaskRegistry {
    shared: Arc<RegistryShared>,
    index: HashMap<TaskId, NonNull<RegistryNode>>,
    // 仅本地管理者访问，指向已取得的索引链中首个未处理项。原记录仍由 head 持有。
    pending_index: *mut RegistryNode,
    // 初始优先更新索引。两类工作都可做时，实际处理一项后优先处理另一类。
    index_first: bool,
    retired: *mut RegistryNode,
    cursor: *mut RegistryNode,
    previous: *mut RegistryNode,
    local: PhantomData<Rc<()>>,
}
struct RegistryShared {
    head: AtomicPtr<RegistryNode>,
    incoming: AtomicPtr<RegistryNode>,
    participants: AtomicUsize,
    final_retired: UnsafeCell<*mut RegistryNode>,
}
struct RegistryNode {
    query: TaskQuery,
    next: AtomicPtr<RegistryNode>,
    incoming_next: UnsafeCell<*mut RegistryNode>,
    retired_next: UnsafeCell<*mut RegistryNode>,
    indexed: UnsafeCell<bool>,
    pin: PhantomPinned,
}
struct RegistryReadGuard<'a> {
    shared: &'a RegistryShared,
}
struct RegistryReclaimGuard {
    remaining: *mut RegistryNode,
}
struct RegistryIndexGuard<'a> {
    shared: &'a RegistryShared,
    remaining: *mut RegistryNode,
}
const REGISTRY_SEALED: usize = 1usize << (usize::BITS - 1);
const REGISTRY_READERS: usize = REGISTRY_SEALED - 1;
// head拥有每份Box一次；incoming仅非拥有索引，不能据它释放或再次取得Box。
// 正常阶段仅本线程管理者删除next、写indexed/retired_next；生产者只发布新head和incoming。
// 每个生产者和读者访问链前，CAS登记participants；成功AcqRel、失败Relaxed重试。
// 正常回收：先Release摘链并移出index，退役到管理者私链，再AcqRel零增量RMW读到零才释放。
// 新参与者取得该RMW发布的摘链事实；旧参与者未退出则不能回收。生产者也登记，防地址ABA。
// 封闭（seal）前清除索引和本地非拥有指针，停止本地链写入；发布final_retired后fetch_or SEALED AcqRel。
// test_registry_drops_with_unindexed_remainder 核对仍有本地余项时封闭，以及最后参与者的释放责任。
// seal后新访问不登记/不读链：统计为零，新创建查询持有直接归还；原已入场发布者完成原发布。
// 旧计数零由管理者收集，否则最后fetch_sub读到SEALED|1的真实参与者Acquire fence后唯一收集。
// seal之后永不重新增加计数；最终收集者接管head及final_retired，incoming只丢弃非拥有指针。
// 最后收集可发生在原查询线程，最多释放任务存储/metadata，不访问或释放原Future；
// 必须先真实结束TaskThread原值责任，才能seal。失败的Future不因此得到成功确认。
// 没有用户回调在链、index或TLS借用内运行；先从私链移走下项再Drop当前Box。
// Arc/Box仅创建时分配；查询O(n)且不clone每任务引用，poll/wake不更新另一份任务状态。
// 持续重叠读者可延迟退役存储回收，不延迟Future清理；需要后续压力/弱内存/成本验证。
// SAFETY: 所有可变槽按上述单写者/阶段协议访问；跨线程只访问原子与不可变TaskQuery。
unsafe impl Send for RegistryShared {}
unsafe impl Sync for RegistryShared {}

impl TaskRegistry {
    pub(crate) fn new() -> Self {
        registry_new_inner()
    }
    pub(crate) fn handle(&self) -> TaskRegistryHandle {
        registry_handle_inner(self)
    }
    // 先更新本地已有余项，再取得至多一段新的固定索引链。插入异常时保留未处理项，
    // 本地只保留非拥有位置，原拥有链（head）仍持有原记录；新链由 RegistryIndexGuard 回挂。
    pub(crate) fn refresh_index(&mut self) {
        registry_index_inner(self)
    }
    // 实际选中或接纳异常补账时先查已有索引；未命中可刷新两个固定范围，
    // 以原任务编号（TaskId）核对同一任务查询句柄（TaskQuery），不受普通维护额度限制。
    pub(crate) fn query(&mut self, id: TaskId) -> Option<&TaskQuery> {
        registry_query_inner(self, id)
    }
    // 更新一项索引或检查一项退役各占一份额度，两类工作均可做时交替推进。
    // 额度为零时不取得索引链或扫描；已有退役记录可在确认旧参与者均退出后按原方式回收。
    // 有本地余项时继续使用原链。无余项时，单次调用至多一次以 AcqRel 取得新链。
    // 读到共享入口为空时，随后发布留给下次。扫描至本次末端即止。
    // 用例 test_registry_maintenance_resumes_and_releases_only_indexed_finished_tasks 核对共用额度，
    // test_registry_lookup_keeps_pending_and_newly_published_records 核对限定取得和查找。
    pub(crate) fn maintain(&mut self, max_nodes: usize) {
        registry_maintain_inner(self, max_nodes)
    }
    /// 第一项为原域全部未确认数，第二项仅已接纳未确认数；未绑定原值不归本域。
    pub(crate) fn unconfirmed_counts(
        &self,
        owner: pi_async_task::definitions::TaskOwnerId,
    ) -> (usize, usize) {
        registry_unconfirmed_inner(self, owner)
    }
}
// 更新一项索引（index），插入成功后才标记记录，最后返还原索引链的下一项。
// 安全（safety）：调用者是本地原管理者，node 是已取得、未处理的索引记录，
// 且仍由 head 保有。当前管理者可独占更新 index 和 indexed。
unsafe fn registry_index_record(
    index: &mut HashMap<TaskId, NonNull<RegistryNode>>,
    node: NonNull<RegistryNode>,
) -> *mut RegistryNode {
    // SAFETY: 本入口的调用前提与配对实现一致。
    unsafe { registry_index_record_inner(index, node) }
}
impl Drop for TaskRegistry {
    fn drop(&mut self) {
        registry_owner_drop_inner(self)
    }
}
impl Clone for TaskRegistryHandle {
    fn clone(&self) -> Self {
        registry_clone_inner(self)
    }
}
impl TaskRegistryHandle {
    /// 创建成功后、提交或导出句柄前登记查询；不使任务（Task）接纳，不保留结果领取权。
    /// 一份Box分配和一份原控制持有；关闭交错时仍只归还这份查询，不改变原Runnable。
    pub(crate) fn register(&self, query: TaskQuery) {
        registry_register_inner(self, query)
    }
    /// 只统计本服务已接纳、正在等待任务唤醒（wake-up）的任务（Task）。
    /// 对每项独立取样，成本为 O(n)，n 为登记规模。
    ///
    /// 与所属域关闭并发时，最后退出的查询可能在当前线程释放任务附加数据（task metadata）。
    /// 启用 `observe` 且配置观测接收者（TaskObserver）时，可能同时报告 `TaskEvent::ReclaimStarted`。
    /// 析构（drop）或该回调的恐慌（panic）原样传播。已绑定任务的原异步计算（Future）仍由所属线程清理。
    pub fn waiting_for_wake_count(&self) -> usize {
        registry_waiting_inner(self)
    }
    /// 只统计本服务已接纳、已记录取消且最终结果未确定的任务（Task）。
    /// 已确认清理或清理未确认（CleanupUnconfirmed）的任务不再计入待取消。
    /// 取样、成本和与关闭并发时的析构边界同 [Self::waiting_for_wake_count]。
    pub fn pending_cancel_count(&self) -> usize {
        registry_cancelling_inner(self)
    }
}
impl RegistryShared {
    fn enter(&self) -> Option<RegistryReadGuard<'_>> {
        registry_enter_inner(self)
    }
    fn collect_final(&self) {
        registry_final_inner(self)
    }
}
impl Drop for RegistryReadGuard<'_> {
    fn drop(&mut self) {
        registry_read_drop_inner(self)
    }
}
impl Drop for RegistryReclaimGuard {
    fn drop(&mut self) {
        registry_reclaim_drop_inner(self)
    }
}
impl Drop for RegistryIndexGuard<'_> {
    fn drop(&mut self) {
        registry_index_drop_inner(self)
    }
}
include!("registry_inner.rs");
