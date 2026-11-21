//! 默认空闲任务来源；队列（queue）不判断空闲，不轮询（poll）业务。
//! 普通获取由运行时（runtime）先判定空闲；取消清理和管理清理独立推进。
use crate::idle::{
    IdleQueueError, IdleQueueManager, IdleQueueStatus, IdleSubmitError, IdleTake, IdleTaskQueue,
};
use crate::management::QueueCleanupProgress;
use crate::mpsc::MpscQueueOptions;
use cordyceps::mpsc_queue::{Consumer, TryDequeueError};
use pi_async_task::{MpscEntryHandle, MpscQueueEntry, Runnable};
use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

// 同一实际模块（module）包含此文件与配对芯。queue保原持有，state保数量／四标志，
// ranges.book仅在原生唯一Consumer内访问，三个清理欠账原子量允许权限外结算，options构造后不变。
// 没有锁（lock）、编号索引或额外任务。
// 原Runnable与节点的转换只移动同一持有；同一任务不得同时占用本地及MPSC链接。
// state低位计数，最高四位停收／管理者退出／接续请求／已关闭。开放检查与加一同一有限CAS；
// 归还数量Release，停止与接续AcqRel，观察状态Acquire，len为Relaxed样本。
// CLOSED仅在停收、真实原生Empty、数量零、两个终点安全取回及当前清理持有结束后发布。
//
// 普通取出至多检查cleanup_batch_size个节点，并交付不多于max_tasks的原值；实现可少取。
// 已取消或退出的原值先转CleanupGuard并登记Normal／Scan／Clear对应欠账，
// 退出Consumer后合法释放／移交，先归还全局数量、再Release结算对应欠账；不交给业务poll。
// 活跃clear时不向普通调用者交出它范围内的原值；返回WouldBlock，已有管理清理仍由来源负责。
//
// process_cleanup不要求空闲：有clear则继续原范围，否则以scan终点固定一次取消检查范围。
// 未取消原值在Consumer内转换回原节点并尾插，不减数、不创建任务或额外强引用。
// 取消检查允许改变未接纳空闲项的顺序；普通获取按本实现当次原生顺序交付，外层保持。
// 新提交及已经检查后才发生的取消留下一次检查。每步有限，完成当前范围不保证以后没有取消。
// 上层事件循环仍须周期推进清理；待清理进度不能单独证明有普通业务可poll或必须忙等。
// 不新增按TaskId的取消索引／外部取消入口；取消事实仍由原任务控制句柄设置。
//
// scan与clear各有一份独立终点，另有原生stub；三者不互相冒用。clear可在scan未完时追加
// 自己的终点，固定调用时范围，不复用旧scan终点漏掉它之后的旧任务。clear一旦激活，
// 之前已旋转到尾部但仍在新clear终点之前的项也清理，不再旋转越过clear边界。
// clear活跃期间不开始新scan。MarkerOrder仅可能None／Scan／Clear／ScanThenClear，
// 在Consumer内按原生入队顺序更新，遇空节点按该顺序取回，不暴露节点地址或私有字段。
// Scan终点Reached且scan欠账Acquire为零才可确认；Clear还须normal／scan／clear三个欠账全零。
// Clear活跃后不再开始新的Scan或普通交付，因此这些旧欠账不会被后来提交无限延长。
// 开放时共享Scan只协助Clear到Reached及结清欠账，不确认／复用Clear终点；
// 原管理Clear调用负责ack并返回Complete，避免帮助者提前复用导致管理者重开下一范围。
// 关闭只接续已存在终点，不插入新的Scan／Clear终点；停收后不会不断制造管理节点。
// 发布缺口返回WouldBlock／Deferred，不将原生Empty或数量零单独作为范围结束。
// Transferring的原值归ConsumerGuard，释放权限前须恢复Available或已完整入队。
// 不在Consumer内分配、扩容、Drop业务、通知或调用用户代码。
//
// 壳（shell）建立完成守卫（CompletionGuard），取得Consumer或登记发布后由芯（inner）启动。
// 取出交付和清理先结束Consumer与CleanupGuard，再确认退出。结果仍保管已取原值。
// OwnerExit不递归安装完成守卫；退出确认经同一壳（shell）回返assist。
// 管理者退出先停收／退出／请求，再帮助；完成证明优先于迟到请求。OwnerExit只有真实处理并
// Release减数或取回终点后才续记请求；纯空重查不能制造请求。Busy交实际Consumer，
// Deferred交实际发布者／权限外清理者；它们退出的可靠AcqRel确认继续尾部责任，不等最后Arc。
// 独立参与者确认只要求OWNER_EXIT且非CLOSED，进入assist入口登记一次请求，不要求旧REQUEST仍在。
// 每步取得Consumer后先AcqRel确认并清旧请求；权限外处理和记账结束后再可靠核新请求。
// 异常时current只结算一次，不重试失败Drop；pending和管理者退出尾部仍有真实承担者。
// 用栈上单项备援，不新增catch_unwind、锁、线程或无界等待参与者；再次恐慌沿原进程规则。
// 本来源Complete不表示原Future已确认drop。所有用户原值均经原Runnable清理路径。

/// 默认空闲来源的唯一管理对象，Send＋Sync，不可克隆（Clone）。
///
/// 来源独立于普通外部任务队列（external task queue）；释放即停收并接续全部剩余责任，
/// 不等所有共享句柄释放。每步清理有限，用户析构（drop）本身不承诺固定耗时。
pub struct MpscIdleTaskQueue {
    shared: Arc<IdleShared>,
}
/// 同一来源的Clone＋Send＋Sync透明访问；不具有全量clear／close权限。
///
/// 取消检查不判断空闲，不保证立即检查到任意位置；已有原值与数量始终由来源负责。
pub struct MpscIdleTaskQueueHandle {
    shared: Arc<IdleShared>,
}

struct IdleShared {
    queue: cordyceps::MpscQueue<MpscQueueEntry>,
    state: AtomicUsize,
    ranges: IdleRanges,
    options: MpscQueueOptions,
}
struct IdleRanges {
    book: UnsafeCell<IdleRangeBook>,
    // 在Consumer内登记；权限外原值处理后Release减数，范围确认Acquire观察。
    pending_normal: AtomicUsize,
    pending_scan: AtomicUsize,
    pending_clear: AtomicUsize,
}
struct IdleRangeBook {
    scan: RangeState,
    clear: RangeState,
    order: MarkerOrder,
}
enum RangeState {
    Available(MpscEntryHandle),
    Transferring,
    Queued,
    Reached(MpscEntryHandle),
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum MarkerKind {
    Scan,
    Clear,
}
#[derive(Clone, Copy)]
enum MarkerOrder {
    None,
    Scan,
    Clear,
    ScanThenClear,
}
#[derive(Clone, Copy)]
enum CleanupMode {
    Scan,
    Clear,
    Close,
    OwnerExit,
}
enum CleanupStep {
    Complete,
    MoreWork,
    Deferred,
    Busy,
}
enum IdleNode {
    Task(Runnable),
    Endpoint(MarkerKind),
}
#[derive(Clone, Copy)]
enum CleanupClass {
    Normal,
    Scan,
    Clear,
}
struct PendingIdleTask {
    task: Runnable,
    class: CleanupClass,
}

struct IdleConsumerGuard<'q> {
    shared: &'q IdleShared,
    consumer: Option<Consumer<'q, MpscQueueEntry>>,
    handed_off: usize,
    endpoint: Option<(MarkerKind, MpscEntryHandle)>,
}
struct IdleCompletionGuard<'q> {
    shared: &'q IdleShared,
    armed: bool,
}
struct IdleCleanupGuard<'q> {
    shared: &'q IdleShared,
    pending: VecDeque<PendingIdleTask>,
    current: Option<CleanupClass>,
    resume_source: bool,
}
const IDLE_COUNT_MASK: usize = usize::MAX >> 4;
const IDLE_STOP: usize = 1usize << (usize::BITS - 1);
const IDLE_OWNER_EXIT: usize = 1usize << (usize::BITS - 2);
const IDLE_REQUEST: usize = 1usize << (usize::BITS - 3);
const IDLE_CLOSED: usize = 1usize << (usize::BITS - 4);

// SAFETY: 原生Consumer使ranges.book及终点顺序的每次访问排他；不在权限外留引用。
// ranges其余三个计数只按原子方式访问，不建立整个IdleRanges的可写引用。
// 其余共享成员为原子或只读值。原生Linked只转交Send节点，不把本地非Send Future搬到外线程。
// 用户代码与原Runnable析构发生在Consumer归还之后；不会据此增加节点本身的Sync保证。
unsafe impl Sync for IdleShared {}

impl MpscIdleTaskQueue {
    /// 创建独立来源，复用默认64／3选项；不创建业务任务或轮询异步计算（Future）。
    ///
    /// 一次准备原生stub、取消检查终点和全量清理终点，三者分别配对释放。
    /// 显式节点分配失败保原报告（Report）；标准共享分配遵守分配器失败策略。
    /// 逻辑数量上限usize::MAX >> 4；正常提交／旋转不增加每任务分配。
    pub fn new(options: MpscQueueOptions) -> pi_result::Result<Self> {
        idle_new_inner(options)
    }
}
impl IdleQueueManager for MpscIdleTaskQueue {
    type Handle = MpscIdleTaskQueueHandle;
    fn handle(&self) -> Self::Handle {
        idle_handle_inner(self)
    }
    /// 固定本次清理终点并有限推进；不停止并发新接收，重复调用继续原范围。
    fn clear(&mut self) -> pi_result::Result<QueueCleanupProgress> {
        idle_clear_inner(self)
    }
    /// 永久停收并接续全部旧责任；幂等，不以最后句柄释放或原Future析构确认为结束条件。
    fn close(&mut self) -> pi_result::Result<QueueCleanupProgress> {
        idle_close_inner(self)
    }
}
impl Drop for MpscIdleTaskQueue {
    fn drop(&mut self) {
        idle_owner_drop_inner(self)
    }
}
impl Clone for MpscIdleTaskQueueHandle {
    fn clone(&self) -> Self {
        idle_clone_inner(self)
    }
}
impl IdleTaskQueue for MpscIdleTaskQueueHandle {
    /// 明确拒绝返回同一原Runnable；缺MPSC链接为Backend中的InvalidInput，不提前drop原值。
    fn try_push(&self, task: Runnable) -> pi_result::RawResult<(), IdleSubmitError> {
        let mut completion = IdleCompletionGuard::new(&self.shared);
        idle_push_inner(self, task, &mut completion)
    }
    /// 一次有限获取；不因有内容而自行断言空闲。返回量非零且不超过本次上限。
    fn try_take_batch(
        &self,
        max_tasks: NonZeroUsize,
    ) -> pi_result::RawResult<IdleTake, IdleQueueError> {
        let mut completion = IdleCompletionGuard::new(&self.shared);
        let result = idle_take_inner(self, max_tasks, &mut completion);
        drop(completion);
        // 完成确认可能接续清理。仅在确认结束后核原生未取到内容的数量样本。
        if matches!(result, Ok(IdleTake::Empty)) && self.shared.has_pending_tasks() {
            Ok(IdleTake::WouldBlock)
        } else {
            result
        }
    }
    /// 有限检查取消或继续已开始的管理清理；未取消项可被旋转到队尾，数量不变。
    /// 当前范围未完为MoreWork，但不据此要求外层无休眠地空转。
    /// 开放Clear已完成内部清理时可返回Complete，保留终点供原管理调用最终确认。
    fn process_cleanup(&self) -> pi_result::Result<QueueCleanupProgress> {
        self.process_cleanup_with_readiness().map(|(progress, _)| progress)
    }
    fn process_cleanup_with_readiness(&self) -> pi_result::Result<(QueueCleanupProgress, bool)> {
        idle_process_inner(self)
    }
    fn status(&self) -> IdleQueueStatus {
        idle_status_inner(self)
    }
    fn len(&self) -> usize {
        idle_len_inner(self)
    }
    fn is_empty(&self) -> bool {
        idle_empty_inner(self)
    }
}

impl IdleShared {
    fn reserve(&self) -> pi_result::RawResult<(), IdleQueueError> {
        idle_reserve_inner(self)
    }
    fn snapshot(&self) -> usize {
        idle_state_inner(self)
    }
    fn has_pending_tasks(&self) -> bool {
        idle_pending_inner(self)
    }
    fn return_count(&self, n: usize) {
        idle_return_inner(self, n)
    }
    fn stop(&self, owner_exit: bool) {
        idle_stop_inner(self, owner_exit)
    }
    fn request(&self) {
        idle_request_inner(self)
    }
    fn confirm_exit(&self) {
        if idle_confirm_inner(self) {
            self.assist();
        }
    }
    fn assist(&self) {
        self.request();
        loop {
            match self.step(CleanupMode::OwnerExit) {
                CleanupStep::MoreWork => continue,
                CleanupStep::Complete | CleanupStep::Deferred | CleanupStep::Busy => break,
            }
        }
    }
    fn step(&self, mode: CleanupMode) -> CleanupStep {
        let mut completion = if matches!(mode, CleanupMode::OwnerExit) {
            None
        } else {
            Some(IdleCompletionGuard::new(self))
        };
        idle_step_inner(self, mode, completion.as_mut())
    }
}
impl<'q> IdleConsumerGuard<'q> {
    fn try_new(shared: &'q IdleShared) -> Option<Self> {
        idle_consumer_new_inner(shared)
    }
    fn take(&mut self) -> pi_result::RawResult<IdleNode, TryDequeueError> {
        idle_consumer_take_inner(self)
    }
    fn start_range(&mut self, kind: MarkerKind) {
        idle_start_range_inner(self, kind)
    }
    fn rotate(&mut self, task: Runnable) {
        idle_rotate_inner(self, task)
    }
    fn acknowledge(&mut self, kind: MarkerKind) -> bool {
        idle_ack_inner(self, kind)
    }
}
impl Drop for IdleConsumerGuard<'_> {
    fn drop(&mut self) {
        idle_consumer_drop_inner(self)
    }
}
impl<'q> IdleCompletionGuard<'q> {
    fn new(shared: &'q IdleShared) -> Self {
        idle_completion_new_inner(shared)
    }
    fn arm(&mut self) {
        idle_completion_arm_inner(self)
    }
}
impl Drop for IdleCompletionGuard<'_> {
    fn drop(&mut self) {
        if idle_completion_drop_inner(self) {
            self.shared.confirm_exit();
        }
    }
}
impl<'q> IdleCleanupGuard<'q> {
    fn new(shared: &'q IdleShared, resume_source: bool) -> Self {
        idle_cleanup_new_inner(shared, resume_source)
    }
    fn prepare(&mut self, n: usize) {
        idle_prepare_inner(self, n)
    }
    fn drain(&mut self) {
        idle_drain_inner(self)
    }
}
impl Drop for IdleCleanupGuard<'_> {
    fn drop(&mut self) {
        idle_cleanup_drop_inner(self)
    }
}
include!("mpsc_idle_inner.rs");
