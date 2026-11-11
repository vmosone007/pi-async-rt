//! 单线程本地侵入式双端队列（intrusive deque），不承担执行器（executor）职责。
use crate::external::TaskBatch;
use intrusive_collections::linked_list::LinkOps;
use intrusive_collections::{Adapter, DefaultPointerOps, LinkedList, LinkedListLink, UnsafeRef};
use pi_async_task::definitions::TaskOwnerId;
use pi_async_task::{QueueEntry, Runnable, TaskThread};
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::rc::Rc;

// TaskQueue只在其创建线程使用，&mut串行维护链接及usize计数，无额外互斥锁。
// QueueEntry内嵌于原任务，UnsafeRef仅承载结构指针；真正强持有在node.ticket，
// 入链前store唯一Runnable，安全摘链后take，队列Drop必须逐个归还，不能只drop UnsafeRef。
// 这是有界的结构往返：Adapter.get_link -> QueueEntry.link_from_node；
// Adapter.get_value -> QueueEntry.node_from_link；只处理本队列已保活节点，不恢复F/Header。
// 同一组链接同一时刻只属一个队列。成功出队且链接操作完整结束后才可复用。
// push_front是下一个选择优先，不切断当前poll，不承诺高优先级无限插入时仍公平。
// 外部批次只能append到尾部，保持其获取顺序；不将加权池称为全局FIFO。

/// 与所属域绑定的本地任务队列（task queue）；不可跨线程转移或共享。
// @trace 100
pub struct TaskQueue {
    owner: TaskOwnerId,
    entries: LinkedList<QueueAdapter>,
    count: usize,
    local: PhantomData<Rc<()>>,
}

/// 入队失败时返还原持有，队列不改变原任务（Task）的绑定。
// @trace 101
pub struct QueuePushError {
    /// 未接纳到本队列的原Runnable。
    pub task: Runnable,
    /// 明确拒绝原因。
    pub cause: QueuePushErrorKind,
}

/// 整批预检查失败，原批次完整返回，没有已插入前缀。
// @trace 102
pub struct BatchPushError {
    /// 原顺序保持，调用者继续承担交付或退出责任。
    pub batch: TaskBatch,
    /// 首个明确拒绝原因。
    pub cause: QueuePushErrorKind,
}

// @trace 103
struct QueueAdapter {
    links: LinkOps,
    pointers: DefaultPointerOps<UnsafeRef<QueueEntry>>,
}

// SAFETY: 两个换算严格互逆且仅用于保活QueueEntry；指针持有槽由TaskQueue维护。
unsafe impl Adapter for QueueAdapter {
    type LinkOps = LinkOps;
    type PointerOps = DefaultPointerOps<UnsafeRef<QueueEntry>>;
    unsafe fn get_value(&self, link: NonNull<LinkedListLink>) -> *const QueueEntry {
        // SAFETY: Adapter调用者保证这是本适配器先前返回的有效链接。
        unsafe { adapter_value_inner(self, link) }
    }
    unsafe fn get_link(&self, value: *const QueueEntry) -> NonNull<LinkedListLink> {
        // SAFETY: Adapter调用者保证value有效且仍由本队列保活。
        unsafe { adapter_link_inner(self, value) }
    }
    fn link_ops(&self) -> &Self::LinkOps {
        adapter_ops_inner(self)
    }
    fn link_ops_mut(&mut self) -> &mut Self::LinkOps {
        adapter_ops_mut_inner(self)
    }
    fn pointer_ops(&self) -> &Self::PointerOps {
        adapter_pointer_ops_inner(self)
    }
}

/// 本地队列的有限拒绝分类；拒绝不隐式取消原任务（Task）。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
// @trace 104
pub enum QueuePushErrorKind {
    /// 任务（Task）尚未绑定本队列所属域或已绑定其它域。
    WrongOwner,
    /// 创建时未选择IntrusiveLocal布局。
    LinkMissing,
    /// 对应链接仍被另一次成员关系占用。
    AlreadyLinked,
    /// 本地精确计数将溢出；不允许回绕。
    CountOverflow,
}

// @trace 1
impl TaskQueue {
    /// 建立与当前所属域绑定的空队列（queue）；不接纳或执行任务（Task）。
    pub fn new(owner: &TaskThread) -> Self {
        queue_new_inner(owner)
    }
}

// @trace 2
impl TaskQueue {
    /// 取得不可更换的所属域身份，O(1)。
    pub fn owner_id(&self) -> TaskOwnerId {
        queue_owner_inner(self)
    }
}

// @trace 3
impl TaskQueue {
    /// 查询本队列当前精确数量，O(1)，不含外部队列及执行中任务。
    pub fn len(&self) -> usize {
        queue_len_inner(self)
    }
}

// @trace 4
impl TaskQueue {
    /// 只判断本地是否无节点，不证明运行时（runtime）可以结束。
    pub fn is_empty(&self) -> bool {
        queue_is_empty_inner(self)
    }
}

// @trace 5
impl TaskQueue {
    /// 将本地任务放到队首，影响下一次选择，不打断当前轮询（poll）。
    /// 失败返还原值；不隐式接纳到执行器（executor）。
    pub fn push_front(&mut self, task: Runnable) -> Result<(), QueuePushError> {
        queue_push_front_inner(self, task)
    }
}

// @trace 6
impl TaskQueue {
    /// 从队首取出唯一可执行持有；空时None，不轮询（poll）。
    pub fn pop_front(&mut self) -> Option<Runnable> {
        queue_pop_front_inner(self)
    }
}

// @trace 7
impl TaskQueue {
    /// 将任务（Task）放到队尾；挂起（Pending）后重新唤醒默认走此入口。
    /// 失败返还原值；不隐式接纳到执行器（executor）。
    pub fn push_back(&mut self, task: Runnable) -> Result<(), QueuePushError> {
        queue_push_back_inner(self, task)
    }
}

// @trace 8
impl TaskQueue {
    /// 从队尾取出唯一可执行持有；空时None，不轮询（poll）。
    pub fn pop_back(&mut self) -> Option<Runnable> {
        queue_pop_back_inner(self)
    }
}

// @trace 9
impl TaskQueue {
    /// 把外部单次获取的有序批次完整追加到队尾，返回追加数量。
    /// 先完整预检查；失败返回原批次，不留下部分成功前缀。O(n)，不等待新增任务。
    pub fn append_batch(&mut self, batch: TaskBatch) -> Result<usize, BatchPushError> {
        queue_append_inner(self, batch)
    }
}

// @trace 10
impl TaskQueue {
    /// 按队首顺序取回全部当前持有，返回批次；不执行或自动取消它们。
    pub fn drain(&mut self) -> TaskBatch {
        queue_drain_inner(self)
    }
}

// @trace 11
impl Drop for TaskQueue {
    /// 归还所有队列持有并正常释放；不遗留节点内自持有环，不直接推进业务原值。
    fn drop(&mut self) {
        queue_drop_inner(self)
    }
}

include!("local_inner.rs");
