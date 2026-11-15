//! 已绑定任务（Task）的原归属返回；不接收尚未绑定的新业务。
use crate::{parking::ThreadNotifier, state::ExecutorState};
use cordyceps::mpsc_queue::Consumer;
use pi_async_task::definitions::TaskOwnerId;
use pi_async_task::{MpscEntryHandle, MpscQueueEntry, Runnable};
use pi_async_task_queue::{management::QueueCleanupProgress, BatchAttempt};
use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    num::NonZeroUsize,
    sync::{atomic::AtomicUsize, Arc},
};

pub(crate) struct ReturnQueue {
    shared: Arc<ReturnShared>,
}
/// 固定所属域（owner domain）的内部交付权限，仅供实现者组合原生唤醒器（Waker）。
pub struct ReturnQueueHandle {
    shared: Arc<ReturnShared>,
}
struct ReturnShared {
    queue: cordyceps::MpscQueue<MpscQueueEntry>,
    state: AtomicUsize,
    endpoint: UnsafeCell<ReturnEndpoint>,
    owner: TaskOwnerId,
    notifier: ThreadNotifier,
    counts: Arc<ExecutorState>,
    batch: NonZeroUsize,
}
enum ReturnEndpoint {
    Available(MpscEntryHandle),
    Transferring,
    Queued,
    Reached(MpscEntryHandle),
}
pub(crate) enum ReturnTake {
    Ready(Runnable),
    Empty,
    WouldBlock,
    Busy,
    Closed,
}
enum ReturnStep {
    Complete,
    MoreWork,
    Deferred,
    Busy,
}
struct ReturnConsumerGuard<'a> {
    shared: &'a ReturnShared,
    consumer: Option<Consumer<'a, MpscQueueEntry>>,
    endpoint: Option<MpscEntryHandle>,
}
struct ReturnCompletionGuard<'a> {
    shared: &'a ReturnShared,
    armed: bool,
}
struct ReturnCleanupGuard<'a> {
    shared: &'a ReturnShared,
    current: Option<Runnable>,
    debt: bool,
    resume_tail: bool,
}
const RETURN_OWNER_GONE: usize = 1usize << (usize::BITS - 1);
const RETURN_REQUEST: usize = 1usize << (usize::BITS - 2);
const RETURN_COUNT: usize = usize::MAX >> 2;
// 不是另一普通业务来源：只移动已经归属本域的唯一Runnable或原域清理持有。
// Native WakeTarget先校验owner；此处再验，原任务必须具有MPSC链接。
// 普通接收不因有限CAS预算失败：fetch_add登记一次，enqueue一次，Completion确认。
// count包含已登记发布/原队列/权限外清理；高两位OWNER_GONE、REQUEST用RMW。
// count上限依真实唯一Task存储及短暂重交接：每存储至少8字节且每任务至多两份欠账，
// 因此可表示地址空间无法耗尽低位；实现仍显式断言/abort防回绕，不制造默默拒绝。
// counts.internal是跨回送与本地队列的一份总数：提交加一次，正常移交只减本来源数，
// 外层真正选出或清理时减internal；退出内部清理则合法Drop后两数各减一次。
// endpoint只由唯一Consumer访问；clear固定物理终点，不吸收后来发布；管理者Drop
// 必须在TaskThread实际退出后才设置OWNER_GONE并自动帮助有限真实尾部。
// 已存在Waker的迟到交付仍可入本接续，不重开业务接收；发布/消费独立出口确认退出，
// 不以REQUEST仍在为前提；完整Empty+count零+终点可用优先迟到请求。
// Consumer内不Drop Runnable、不调度、不通知用户；原生安全取回后才能复用MPSC链接。
// ThreadNotifier只做线程许可；不从其它线程碰本地双端队列/非Send原Future。
// SAFETY: endpoint只在原生Consumer中改；state/counters原子，原生Linked保唯一固定节点。
unsafe impl Sync for ReturnShared {}

impl ReturnQueue {
    pub(crate) fn new(
        owner: TaskOwnerId,
        notifier: ThreadNotifier,
        counts: Arc<ExecutorState>,
        batch: NonZeroUsize,
    ) -> pi_result::Result<Self> {
        return_new_inner(owner, notifier, counts, batch)
    }
    pub(crate) fn handle(&self) -> ReturnQueueHandle {
        return_handle_inner(self)
    }
    pub(crate) fn try_take(&mut self) -> ReturnTake {
        self.shared.take()
    }
    pub(crate) fn clear(&mut self) -> pi_result::Result<QueueCleanupProgress> {
        return_clear_inner(self)
    }
    pub(crate) fn close_after_owner(&mut self) -> QueueCleanupProgress {
        return_close_inner(self)
    }
}
impl Drop for ReturnQueue {
    fn drop(&mut self) {
        return_owner_drop_inner(self)
    }
}
impl Clone for ReturnQueueHandle {
    fn clone(&self) -> Self {
        return_clone_inner(self)
    }
}
impl ReturnQueueHandle {
    pub(crate) fn notify(&self) {
        return_notify_inner(self)
    }
    /// 保留原值的同步交付；只拒绝不属于原域或缺链接的输入。正常默认任务不发生配额拒绝。
    pub(crate) fn publish(&self, task: Runnable) -> pi_result::RawResult<(), Runnable> {
        return_publish_inner(self, task)
    }
    pub(crate) fn try_take_batch(&self) -> BatchAttempt {
        return_batch_inner(self)
    }
    pub fn len(&self) -> usize {
        return_len_inner(self)
    }
}
impl ReturnShared {
    fn take(&self) -> ReturnTake {
        return_take_inner(self)
    }
    fn confirm(&self) {
        if return_confirm_inner(self) {
            self.assist();
        }
    }
    fn assist(&self) {
        return_request_inner(self);
        loop {
            match self.cleanup(true) {
                ReturnStep::MoreWork => continue,
                ReturnStep::Complete | ReturnStep::Deferred | ReturnStep::Busy => break,
            }
        }
    }
    fn cleanup(&self, exiting: bool) -> ReturnStep {
        return_cleanup_inner(self, exiting)
    }
}
impl Drop for ReturnConsumerGuard<'_> {
    fn drop(&mut self) {
        return_consumer_drop_inner(self)
    }
}
impl Drop for ReturnCompletionGuard<'_> {
    fn drop(&mut self) {
        return_completion_drop_inner(self)
    }
}
impl Drop for ReturnCleanupGuard<'_> {
    fn drop(&mut self) {
        return_cleanup_drop_inner(self)
    }
}
include!("returns_inner.rs");
