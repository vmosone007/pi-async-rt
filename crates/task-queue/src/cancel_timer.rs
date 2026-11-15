//! 默认可取消定时队列（cancelable timer queue）。
//!
//! 本地拥有者固定在创建线程（thread）；共享句柄（shared handle）只发布受限请求。
//! 外部以同源显式单调时间（monotonic time）推进，队列（queue）不读取或刷新时钟，
//! 不轮询（poll）业务。启用`cancel-timer`特性（feature）后提供本模块（module）。

mod definitions;
mod value;
mod shared;
mod local;

pub use definitions::{CancelTimerKey, CancelTimerOptions};
use crate::management::QueueCleanupProgress;
use crate::notify::QueueNotifier;
use crate::timer::{
    TimerAdvanceProgress, TimerCancelRequestError, TimerQueueError, TimerQueueHandle,
    TimerQueueStatus, TimerSubmitError, TimerTake, TimerTaskQueue,
};
use local::TimerLocalState;
use pi_clock_manual::global::GlobalManualInstant;
use shared::TimerShared;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

// 本地状态与共享状态分离借用；共享模块从不回访本地表。
// 只有new准备全部组成后导出拥有者；只有拥有者拥有推进/取出/清理/关闭权限。
// Drop先原子建立停收+退出，再进入原线程清理守卫，最后可靠接续实际Send共享余项。
// 共享句柄克隆只增加同一来源保活，不把本地非Send原值变为可跨线程移动。

/// 默认定时来源的唯一拥有者，实现[`TimerTaskQueue`]。
///
/// `Item`只要求'static，本地登记不额外要求Send或Sync。此对象固定创建线程（thread），
/// 不能跨线程转移或共享（!Send + !Sync）；限制来自零大小标记，不读取线程局部存储（TLS）。
/// 共享访问通过[`CancelTimerQueueHandle`]，不公开内部容器或可写状态（mutable state）。
///
/// 时间为同一`GlobalManualInstant`时间轴；1ms精度，推进向下取整、截止时间向上取整，
/// 不提前交付。截止时间合法范围为起点后0至18,446,744,073,709,000,000ns；
/// 构造、推进及检查参考时间可达到`GlobalManualInstant::MAX`。
///
/// `clear`固定本次旧范围且继续接收新内容；`close`永久停收并接续全部停收前责任。
/// 释放（drop）不等待最后共享句柄消失；本地原值在创建线程合法清理，共享实际Send余项
/// 由真实参与者接续。首次恐慌（panic）继续传播，失败原值不重复析构（drop）。
/// 来源清理完成不承诺原异步计算（Future）已完成所属线程析构，也不代表最终存储已回收。
// @trace 1
pub struct CancelTimerQueue<Item: 'static, N: QueueNotifier> {
    shared: Arc<TimerShared<Item, N>>,
    local: TimerLocalState<Item>,
    owner_thread: PhantomData<Rc<()>>,
}

/// 同一默认定时来源的受限共享句柄（shared handle）。
///
/// 支持Clone + Send + Sync，即使该来源也保存本地非Send原值。共享提交方法单独要求
/// `Item:Send+'static`，不要求Sync；取消（cancellation）只提交请求，不承诺目标存在或成功。
/// 克隆（Clone）不要求Item或通知实现可克隆，不复制队列（queue）或原值（original value）。
///
/// 无推进、取出、clear或close权限。释放此句柄（handle）不取消登记；停收后不会重新开放。
/// 查询是各自的原子快照（atomic snapshot），不构成多项全局同一瞬间的观测。
// @trace 2
pub struct CancelTimerQueueHandle<Item: 'static, N: QueueNotifier> {
    shared: Arc<TimerShared<Item, N>>,
}

impl<Item: 'static, N: QueueNotifier> CancelTimerQueue<Item, N> {
    /// 在当前线程（thread）创建一份独立来源；不初始化或刷新全局时钟。
    ///
    /// `now`为外部提供的同源参考时刻，`notifier`负责发布后的所属线程通知，
    /// `options`为三个独立工作额度。当前阶段无需执行器（executor）实现。
    ///
    /// 创建原生定时器（native timer）、两份用途不同的控制节点（control node）及共享保活。
    /// 显式控制节点分配失败返回资源不足报告（resource-exhausted report），已准备部分配对释放；
    /// 不导出半成品。不承诺标准容器、Arc及所有下层内存耗尽都可恢复。
    pub fn new(
        now: GlobalManualInstant,
        notifier: N,
        options: CancelTimerOptions,
    ) -> pi_result::Result<Self> {
        let (shared, marker) = TimerShared::new(notifier, options)?;
        let local = TimerLocalState::new(now, marker);
        Ok(cancel_timer_new_inner(shared, local))
    }
}

impl<Item: 'static, N: QueueNotifier> TimerTaskQueue for CancelTimerQueue<Item, N> {
    type Item = Item;
    type Instant = GlobalManualInstant;
    type Key = CancelTimerKey;
    type DueBatch = Vec<Item>;
    type Handle = CancelTimerQueueHandle<Item, N>;

    /// 取得同一来源的共享句柄（shared handle）；只增加一份Arc保活，不另建来源。
    fn handle(&self) -> Self::Handle {
        cancel_timer_handle_inner(Arc::clone(&self.shared))
    }

    /// 尝试本地登记同一原值（original value），明确拒绝返还原值及原因。
    /// 不要求Send/Sync；成功保留同一登记身份，不轮询（poll）或隐式接纳业务。
    /// 本地表准备失败在接收前报告；已接收后来源继续承担责任，不能倒退为拒绝。
    fn try_push_local(
        &mut self, deadline: Self::Instant, item: Item,
    ) -> pi_result::RawResult<Self::Key, TimerSubmitError<Item>> {
        self.local.try_push(&self.shared, deadline, item)
    }

    /// 记录本地取消请求（cancellation request）；成功不证明目标存在或已经清理。
    /// 不消费键（key）；错来源不得命中另一项，重复请求不重复交出或析构（drop）原值。
    fn request_cancel_local(
        &mut self, key: &Self::Key,
    ) -> pi_result::RawResult<(), TimerCancelRequestError> {
        self.local.request_cancel(&self.shared, key)
    }

    /// 以外部同源时刻有限推进；纳秒倒退在任何推进效果之前返回InvalidTime。
    /// ReachedTarget只证明追时位置，不证明该刻度所有到期项已发现。
    /// 非空原生轮受毫秒跨度和尝试额度限制；真实空轮可用同一绝对坐标重建追时。
    fn advance(
        &mut self, now: &Self::Instant,
    ) -> pi_result::RawResult<TimerAdvanceProgress, TimerQueueError> {
        self.local.advance(&self.shared, *now)
    }

    /// 最多交出一份当前组到期原值（original value），不等待全部发现或后来提交。
    /// 普通调用受工作额度限制；暂不能交付与已知为空分别表达，不改变原值身份。
    fn try_take_due_one(&mut self) -> pi_result::RawResult<TimerTake<Item>, TimerQueueError> {
        self.local.take_one(&self.shared)
    }

    /// 完整交出本次固定参考时间及起始范围内、可业务交付的全部到期原值（original value）。
    ///
    /// 成功向量（vector）按当前组实际发现顺序，非空才Ready；可超过普通64项额度。
    /// 尚不能证明完整返回WouldBlock并保留原值，每次重试重新固定范围；
    /// 持续新到达可能持续不能完成。准备结果空间失败保留尚未交出内容并返回真实错误。
    /// 已清理旧组及已落实取消项不业务交付，尚未采用的取消请求没有抢先成功承诺。
    fn try_take_due_all(&mut self) -> pi_result::RawResult<TimerTake<Vec<Item>>, TimerQueueError> {
        self.local.take_all(&self.shared)
    }

    /// 有限处理已待清理内容，不要求到期或空闲，不启动新的clear范围。
    /// 真正合法清理／移交之后才减数；MoreWork和错误均保留剩余责任。
    fn process_cleanup(&mut self) -> pi_result::Result<QueueCleanupProgress> {
        self.local.process_cleanup(&self.shared)
    }

    /// 只读、无扫描地返回最迟再次检查时刻；不读取或刷新时钟。
    ///
    /// 已知可推进工作返回now；有登记或管理未完不晚于now+1ms；
    /// 开放空源无管理工作不晚于now+10ms；夹取MAX，确认来源退出才None。
    /// 不替代主动通知（active notification）、休眠前重查和被动定时唤醒（passive timed wake-up）。
    fn next_check_at(
        &self, now: &Self::Instant,
    ) -> pi_result::RawResult<Option<Self::Instant>, TimerQueueError> {
        self.local.next_check_at(&self.shared, *now)
    }

    /// 本次单独Acquire样本：Accepting、Closing或LocalExited，不承诺原值已析构（drop）。
    fn status(&self) -> TimerQueueStatus {
        self.shared.status()
    }

    /// 本次Acquire数量样本；含发布、暂存、登记、到期及清理欠账，不含已实际交出的原值。
    fn len(&self) -> usize {
        self.shared.len()
    }

    /// 与一次数量样本对应，不保证下一瞬间仍为空或可以休眠。
    fn is_empty(&self) -> bool {
        self.shared.is_empty()
    }

    /// 固定旧接收组并有限清理；新组继续接收，重复未完调用延续同一范围。
    /// marker安全取回后仍须结清旧表、暂存和欠账，才返回Complete并允许复用。
    fn clear(&mut self) -> pi_result::Result<QueueCleanupProgress> {
        self.local.clear(&self.shared)
    }

    /// 永久停收后接续全部停收前责任；不等待未到期内容到期，不交还未到期原值。
    /// 重复调用幂等（idempotent），Complete只表示来源责任结束。
    fn close(&mut self) -> pi_result::Result<QueueCleanupProgress> {
        self.local.close(&self.shared)
    }
}

impl<Item: 'static, N: QueueNotifier> Clone for CancelTimerQueueHandle<Item, N> {
    fn clone(&self) -> Self {
        cancel_timer_handle_inner(Arc::clone(&self.shared))
    }
}

impl<Item: 'static, N: QueueNotifier> TimerQueueHandle for CancelTimerQueueHandle<Item, N> {
    type Item = Item;
    type Instant = GlobalManualInstant;
    type Key = CancelTimerKey;

    /// 一次共享请求分配一个内嵌原值（original value）的节点（node），无需Item:Sync。
    /// 成功指受检接收及完整发布，随后通知所属线程（owner thread）；不代表业务已接纳或到期。
    /// 明确拒绝保留同一item及原因；已发布后的通知恐慌（panic）不撤销接收。
    fn try_push(
        &self, deadline: Self::Instant, item: Item,
    ) -> pi_result::RawResult<Self::Key, TimerSubmitError<Item>>
    where
        Item: Send + 'static,
        Self::Instant: Send + 'static,
    {
        self.shared.try_push(deadline, item)
    }

    /// 同步发布取消请求（cancellation request），不等待落实；同一共享FIFO保持发布顺序。
    /// 错来源返回错误；不存在、已取消、已交出等情况均无取消成功保证。
    fn request_cancel(&self, key: &Self::Key) -> pi_result::RawResult<(), TimerCancelRequestError> {
        self.shared.request_cancel(key)
    }

    /// 本次独立Acquire状态样本，句柄（handle）存续不延迟来源责任结束。
    fn status(&self) -> TimerQueueStatus {
        self.shared.status()
    }

    /// 一次Acquire低位数量样本，不扫描任何队列（queue）或本地登记。
    fn len(&self) -> usize {
        self.shared.len()
    }

    /// 与本次数量样本一致；不作为来源Complete或休眠判断的唯一依据。
    fn is_empty(&self) -> bool {
        self.shared.is_empty()
    }
}

impl<Item: 'static, N: QueueNotifier> Drop for CancelTimerQueue<Item, N> {
    fn drop(&mut self) {
        self.shared.begin_owner_exit();
        self.local.drop_owner_local(&self.shared);
    }
}

include!("cancel_timer_inner.rs");
