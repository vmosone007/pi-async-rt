//! 所属线程（owner thread）内的原生定时拥有者与安全弱访问。
use super::{
    access::{
        new_retry_owner, CancelRetryNode, DefaultTimerAccess, DefaultTimerItem,
        DefaultTimerRegistration, TimerSourceShared,
    },
    protocol::{ExecutorTimer, LocalTimerSubmit},
    retry::RetryOwner,
};
use crate::{clock::ExecutorClock, parking::ThreadNotifier};
use pi_async_runtime_core::definitions::RuntimeId;
use pi_async_task_queue::{
    management::QueueCleanupProgress,
    timer::{
        TimerAdvanceProgress, TimerCancelRequestError, TimerQueueError, TimerQueueStatus,
        TimerSubmitError, TimerTake, TimerTaskQueue,
    },
    CancelTimerKey, CancelTimerOptions, CancelTimerQueue, CancelTimerQueueHandle,
};
use pi_clock_manual::global::GlobalManualInstant;
use std::{
    cell::RefCell,
    rc::{Rc, Weak},
    sync::Arc,
    thread::ThreadId,
};

/// 默认定时接入的唯一线程内驱动（driver），!Send＋!Sync，不克隆管理责任。
///
/// 原生来源使用pi_cancel_timer 0.7，由已冻结队列实现维护；本对象仅连接核心登记和取消重试。
/// 每次advance只消费显式同源时刻；时钟由执行器统一取得，不由队列刷新。
/// 普通业务运行时（runtime）不导出本对象、原生句柄或LocalTimerHandle。
/// 精度及范围沿CancelTimerQueue：1ms，推进向下取整、截止向上取整，不提前交付；
/// 截止为同一起点后0至18,446,744,073,709,000,000ns，参考时刻可达GlobalManualInstant::MAX。
/// 完整到期批次保持当前范围实际发现顺序，空间和处理成本随批次增大，不承诺固定开销。
/// 释放（drop）请求退场；正在执行的本地同步活动可延迟实际清理，不等待外部共享句柄释放才清理原值。
pub struct DefaultTimerDriver<C, L>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    core: Rc<TimerLocalCore<C, L>>,
}
/// 仅创建线程可用的弱访问；不保活拥有者或原生来源，不产生公共环境选择权限。
///
/// L的实现先通过核心当前本地权限核RuntimeId，再调用此对象；直接低层调用仍核对
/// 原服务、线程与退出意图。错误返还同一原值，不退到另一运行时（runtime）。
pub struct LocalTimerHandle<C, L>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    core: Weak<TimerLocalCore<C, L>>,
    runtime: RuntimeId,
    thread: ThreadId,
}
struct TimerLocalCore<C, L>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    // 只有Driver的长期Rc及一次同步活动的临时Rc；LocalTimerHandle始终Weak。
    queue: RefCell<Option<CancelTimerQueue<DefaultTimerItem<C, L>, ThreadNotifier>>>,
    // Drop顺序：先实际移走/drop原生拥有者，之后才退出取消接续者。
    retry: RefCell<RetryOwner<CancelRetryNode<C, L>>>,
    shared: Arc<TimerSourceShared<C, L>>,
    thread: ThreadId,
    // 仅无返回口的本地退场帮助保存第一份原报告，不在槽借用内Drop后来报告。
    close_error: RefCell<Option<pi_result::Error>>,
}
struct TimerLocalActivityGuard<C, L>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    core: Rc<TimerLocalCore<C, L>>,
    armed: bool,
}
// Driver退出先发布owner_exiting，临时活动Rc保住当前本地原值；不在仍借用时强行取&mut。
// 活动结束先放RefMut，再撤销guard，必要时推进已请求close；最后Rc归还。
// 若用户回调在一次本地调用内释放Driver，真正LocalCore Drop会延至该活动结束，
// 以独占get_mut取走queue并在借用外Drop，随后RetryOwner字段退出。没有悬空指针/等待/锁。
// close只停止本计时设施，不允许把它作为服务继续开放的稳态；整运行时负责组合关闭。
// 每次普通驱动有限；退出/停收后独立接续可帮助实际有限尾部，不以请求空转。

impl<C, L> DefaultTimerDriver<C, L>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    /// 在当前线程创建独立来源，真实取同一clock一次作为初始参考；不初始化pi_clock。
    /// 同时准备一份接续stub，默认重试工作额度复用options.work_batch_size。
    pub fn new(
        runtime: RuntimeId,
        clock: C,
        notifier: ThreadNotifier,
        options: CancelTimerOptions,
    ) -> pi_result::Result<Self> {
        driver_new_inner(runtime, clock, notifier, options)
    }
    /// 返回仅本线程可用的同源Weak访问；不增加长期原生拥有者。
    pub fn local_handle(&self) -> LocalTimerHandle<C, L> {
        driver_local_handle_inner(self)
    }
}
impl<C, L> Drop for DefaultTimerDriver<C, L>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    fn drop(&mut self) {
        driver_drop_inner(self)
    }
}
impl<C, L> Clone for LocalTimerHandle<C, L>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    fn clone(&self) -> Self {
        local_timer_clone_inner(self)
    }
}
impl<C, L> LocalTimerHandle<C, L>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    /// 同步本地登记；先核权限、后暂借原来源，无Send／Sync附加约束。
    /// 来源失效/关闭返回Closed，原目标或线程不符WrongOwner，暂借冲突Busy，均保原item。
    pub fn try_push(
        &self,
        runtime: RuntimeId,
        deadline: GlobalManualInstant,
        item: DefaultTimerItem<C, L>,
    ) -> pi_result::RawResult<CancelTimerKey, TimerSubmitError<DefaultTimerItem<C, L>>> {
        local_timer_push_inner(self, runtime, deadline, item)
    }
}
impl<C, L> TimerTaskQueue for DefaultTimerDriver<C, L>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    type Item = DefaultTimerItem<C, L>;
    type Instant = GlobalManualInstant;
    type Key = CancelTimerKey;
    type DueBatch = Vec<Self::Item>;
    type Handle = CancelTimerQueueHandle<Self::Item, ThreadNotifier>;
    fn handle(&self) -> Self::Handle {
        driver_native_handle_inner(self)
    }
    /// 在原所属线程（owner thread）提交同源原值，不附加Send／Sync；拒绝返还同一原值及原因。
    /// 成功只确认来源接收，不轮询（poll）或隐式接纳；已接收后的退场不倒退为提交拒绝。
    fn try_push_local(
        &mut self,
        deadline: Self::Instant,
        item: Self::Item,
    ) -> pi_result::RawResult<Self::Key, TimerSubmitError<Self::Item>> {
        driver_local_push_inner(self, deadline, item)
    }
    fn request_cancel_local(
        &mut self,
        key: &Self::Key,
    ) -> pi_result::RawResult<(), TimerCancelRequestError> {
        driver_local_cancel_inner(self, key)
    }
    /// 以显式同源时刻有限推进，不刷新或替换时间轴；时间倒退返回原InvalidTime。
    /// ReachedTarget只表示追时位置，不证明所有到期项均已发现。
    fn advance(
        &mut self,
        now: &Self::Instant,
    ) -> pi_result::RawResult<TimerAdvanceProgress, TimerQueueError> {
        driver_advance_inner(self, now)
    }
    fn try_take_due_one(&mut self) -> pi_result::RawResult<TimerTake<Self::Item>, TimerQueueError> {
        driver_due_one_inner(self)
    }
    /// 完整交付本次固定范围内全部可业务交付的到期原值，保留来源发现顺序，可能超过普通工作额度。
    /// 尚不能确认完整返回WouldBlock并保管原值；下次重试重新确定范围，不以部分向量冒充全部。
    fn try_take_due_all(
        &mut self,
    ) -> pi_result::RawResult<TimerTake<Self::DueBatch>, TimerQueueError> {
        driver_due_all_inner(self)
    }
    /// 有限处理来源清理及取消接续，不以是否到期或空闲决定是否清理。
    /// 移交此前无返回口保留的首份原错误；MoreWork或错误不解除剩余责任，纯查询不取走该错误。
    fn process_cleanup(&mut self) -> pi_result::Result<QueueCleanupProgress> {
        driver_cleanup_inner(self)
    }
    /// 只读返回最迟再次检查时刻，不采时或推进；可立即接续的取消工作返回当前参考时刻。
    /// 其余沿来源保守上限：有登记或管理未完不晚于now+1ms，开放空源不晚于now+10ms；
    /// 时刻夹取MAX，确认来源退出才None；不替代主动通知及停放前重查。
    fn next_check_at(
        &self,
        now: &Self::Instant,
    ) -> pi_result::RawResult<Option<Self::Instant>, TimerQueueError> {
        driver_next_inner(self, now)
    }
    fn status(&self) -> TimerQueueStatus {
        driver_status_inner(self)
    }
    fn len(&self) -> usize {
        driver_len_inner(self)
    }
    fn is_empty(&self) -> bool {
        driver_empty_inner(self)
    }
    fn clear(&mut self) -> pi_result::Result<QueueCleanupProgress> {
        driver_clear_inner(self)
    }
    /// 永久停收并推进来源与取消接续的退出；重复调用幂等（idempotent），用于整体服务关闭。
    /// Complete须两部分责任均结束且没有待报告错误；活动中的本地操作保有实际退出责任。
    /// 不表示业务原异步计算（Future）已全部成功析构，不支持关闭计时后继续正常服务的稳态。
    fn close(&mut self) -> pi_result::Result<QueueCleanupProgress> {
        driver_close_inner(self)
    }
}
impl<C, L> ExecutorTimer for DefaultTimerDriver<C, L>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    type Access = DefaultTimerAccess<C, L>;
    fn timer_access(&self) -> Self::Access {
        driver_access_inner(self)
    }
    fn process_cancel_retries(&mut self) -> pi_result::Result<QueueCleanupProgress> {
        driver_retries_inner(self)
    }
}
impl<C, L> TimerLocalCore<C, L>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    fn finish_close_step(&self) -> pi_result::Result<QueueCleanupProgress> {
        driver_finish_close_inner(self)
    }
    fn record_close_error(&self, error: pi_result::Error) {
        driver_record_close_error_inner(self, error)
    }
}
impl<C, L> Drop for TimerLocalCore<C, L>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    fn drop(&mut self) {
        driver_core_drop_inner(self)
    }
}
impl<C, L> Drop for TimerLocalActivityGuard<C, L>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    fn drop(&mut self) {
        driver_activity_drop_inner(self)
    }
}
include!("local_inner.rs");
