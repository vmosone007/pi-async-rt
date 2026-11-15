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
// 拥有者退出前停止全部本地链写入；发布final_retired后fetch_or SEALED AcqRel。
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
    pub(crate) fn refresh_index(&mut self) {
        registry_index_inner(self)
    }
    pub(crate) fn query(&mut self, id: TaskId) -> Option<&TaskQuery> {
        registry_query_inner(self, id)
    }
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
    /// 精确按每任务原生查询口径独立取样；O(n)，不是全局同时刻快照（snapshot）。
    pub fn waiting_for_wake_count(&self) -> usize {
        registry_waiting_inner(self)
    }
    /// 原取消已记录、最终结果尚未确定者；已确认清理或CleanupUnconfirmed不再计待取消。
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
