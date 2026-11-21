//! 默认计时共享访问与原登记的一次预备节点。
use super::{
    protocol::LocalTimerSubmit,
    retry::{RetryHandle, RetryOwner, RetryShared, RetryWork},
};
use crate::{clock::ExecutorClock, parking::ThreadNotifier};
use cordyceps::{mpsc_queue::Links, Linked};
use pi_async_runtime_core::{
    definitions::{RuntimeId, TimerError},
    time::{
        access::{Confirmation, TimerCancelRequestOutcome},
        resume_timer_cancel, TimerAccess, TimerEntry, TimerRegistration, TimerRegistrationKind,
    },
};
use pi_async_task_queue::{
    timer::{TimerQueueError, TimerQueueHandle, TimerSubmitError},
    CancelTimerKey, CancelTimerQueueHandle,
};
use pi_clock_manual::global::GlobalManualInstant;
use std::{
    cell::UnsafeCell,
    marker::{PhantomData, PhantomPinned},
    ptr::NonNull,
    sync::{
        atomic::{AtomicBool, AtomicU8},
        Arc, Weak,
    },
    time::Duration,
};

/// 默认适配的登记载荷；只有运行时（runtime）构造和执行器（executor）交付，不向普通业务拆出。
pub type DefaultTimerItem<C, L> = TimerEntry<CancelTimerKey, DefaultTimerRegistration<C, L>>;

/// 同一计时来源的Clone＋Send＋Sync访问；不保本地容器或可写线程局部指针。
///
/// 正常业务通过运行时（runtime）的timeout／sleep等使用。C为同源时钟，L为静态本地接入；
/// 外部签名不要求业务Future的Unpin或额外Sync，最终运行时别名隐藏这两个实现参数。
pub struct DefaultTimerAccess<C: ExecutorClock<Instant = GlobalManualInstant>, L: 'static> {
    pub(super) shared: Arc<TimerSourceShared<C, L>>,
}
/// 核心登记与其一次准备节点的共享句柄；复制只增加同一Arc，不复制键或任务（Task）。
pub struct DefaultTimerRegistration<C: ExecutorClock<Instant = GlobalManualInstant>, L: 'static> {
    pub(super) node: Arc<CancelRetryNode<C, L>>,
}
pub(super) struct TimerSourceShared<C: ExecutorClock<Instant = GlobalManualInstant>, L: 'static> {
    pub(super) runtime: RuntimeId,
    pub(super) clock: C,
    pub(super) backend: CancelTimerQueueHandle<DefaultTimerItem<C, L>, ThreadNotifier>,
    pub(super) retries: Arc<RetryShared<CancelRetryNode<C, L>>>,
    pub(super) owner_exiting: AtomicBool,
    pub(super) local: PhantomData<fn() -> L>,
}
// Node不含原Future、ControlHandle或本地Rc；原业务持有只在TimerEntry。
// error仅在一次链接资格的Publishing期间写，保留第一份真实原错误直到节点最终释放；
// 不是把Report字符串化或变成用户控制错误的副本。重复送入不是第二取消责任。
// links地址用原始投影；Arc只保地址，不能对已发布节点try_unwrap/get_mut移动。
// 没有Access→Node→Access强环：origin与retry均为Weak。驱动退出先撤权限/停收意图，
// 实际本地活动者和本地核心负责原生来源Drop，之后才退出RetryOwner。
pub(super) struct CancelRetryNode<C: ExecutorClock<Instant = GlobalManualInstant>, L: 'static> {
    facts: TimerRegistration<CancelTimerKey>,
    links: Links<Self>,
    origin: Weak<TimerSourceShared<C, L>>,
    retry: Weak<RetryShared<Self>>,
    membership: AtomicU8,
    error: UnsafeCell<Option<TimerQueueError>>,
    stub: bool,
    pin: PhantomPinned,
}
const NODE_IDLE: u8 = 0;
const NODE_PUBLISHING: u8 = 1;
const NODE_QUEUED: u8 = 2;
// SAFETY: error只有IDLE->PUBLISHING成功者访问；成功CAS使用AcqRel，取得上次恢复IDLE的Release。
// 失败分支不访问error。下一次成功取得或最终独占Drop之前，不在其它资格外读写该槽。
// 同一节点最多一份RetryHandle入队；Links仅原生原始指针投影，facts已有独立发布协议。
// origin/retry为线程安全Weak，L仅类型标记，绝不存跨线程可访问的本地数据。
unsafe impl<C: ExecutorClock<Instant = GlobalManualInstant>, L: 'static> Sync
    for CancelRetryNode<C, L>
{
}

impl<C: ExecutorClock<Instant = GlobalManualInstant>, L: 'static> Clone
    for DefaultTimerAccess<C, L>
{
    fn clone(&self) -> Self {
        default_access_clone_inner(self)
    }
}
impl<C: ExecutorClock<Instant = GlobalManualInstant>, L: 'static> Clone
    for DefaultTimerRegistration<C, L>
{
    fn clone(&self) -> Self {
        default_registration_clone_inner(self)
    }
}
impl<C: ExecutorClock<Instant = GlobalManualInstant>, L: 'static>
    AsRef<TimerRegistration<CancelTimerKey>> for DefaultTimerRegistration<C, L>
{
    fn as_ref(&self) -> &TimerRegistration<CancelTimerKey> {
        &self.node.facts
    }
}
impl<C, L> TimerAccess for DefaultTimerAccess<C, L>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    type Instant = GlobalManualInstant;
    type Key = CancelTimerKey;
    type Registration = DefaultTimerRegistration<C, L>;
    fn runtime_id(&self) -> RuntimeId {
        self.shared.runtime
    }
    fn deadline_after(
        &self,
        duration: Duration,
    ) -> pi_result::RawResult<Self::Instant, TimerError> {
        default_deadline_inner(self, duration)
    }
    fn prepare_registration(
        &self,
        kind: TimerRegistrationKind,
    ) -> pi_result::RawResult<Self::Registration, TimerError> {
        default_prepare_inner(self, kind)
    }
    fn try_register(
        &self,
        deadline: Self::Instant,
        item: DefaultTimerItem<C, L>,
    ) -> pi_result::RawResult<Self::Key, TimerSubmitError<DefaultTimerItem<C, L>>> {
        default_register_inner(self, deadline, item)
    }
    fn try_register_local(
        &self,
        deadline: Self::Instant,
        item: DefaultTimerItem<C, L>,
    ) -> pi_result::RawResult<Self::Key, TimerSubmitError<DefaultTimerItem<C, L>>> {
        default_register_local_inner(self, deadline, item)
    }
    fn request_cancel(
        &self,
        key: &Self::Key,
    ) -> pi_result::RawResult<TimerCancelRequestOutcome, TimerQueueError> {
        default_cancel_inner(self, key)
    }
    fn defer_cancel(
        &self,
        registration: Self::Registration,
        error: Option<TimerQueueError>,
        confirmation: Confirmation<'_, Self::Key>,
    ) {
        assert!(confirmation.is_for(registration.as_ref()), "取消接续须使用原登记");
        default_defer_inner(self, registration, error, confirmation)
    }
}
impl<C, L> RetryWork for CancelRetryNode<C, L>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    fn resume(this: &Arc<Self>) {
        default_retry_resume_inner(this)
    }
}
// SAFETY: 唯一RetryHandle只在节点资格成功后构造；原生入队仅移动这份持有。
// Arc数据固定，Self含PhantomPinned；原生安全归还后才可复用Links；stub不进入核心resume。
// into_ptr抑制包装Drop，from_ptr仅按原生合法归还重建同一单位和内部结算函数。
unsafe impl<C: ExecutorClock<Instant = GlobalManualInstant>, L: 'static> Linked<Links<Self>>
    for CancelRetryNode<C, L>
{
    type Handle = RetryHandle<Self>;
    fn into_ptr(handle: Self::Handle) -> NonNull<Self> {
        default_retry_into_ptr_inner(handle)
    }
    unsafe fn from_ptr(ptr: NonNull<Self>) -> Self::Handle {
        default_retry_from_ptr_inner(ptr)
    }
    unsafe fn links(ptr: NonNull<Self>) -> NonNull<Links<Self>> {
        default_retry_links_inner(ptr)
    }
}
pub(super) fn release_retry_unit<C: ExecutorClock<Instant = GlobalManualInstant>, L: 'static>(
    node: &CancelRetryNode<C, L>,
) {
    default_release_unit_inner(node)
}
// 这里收到的原节点（node）已独占链接资格，保有本次原错误，且关联同一确认。
fn publish_retry<C, L>(
    source: &TimerSourceShared<C, L>,
    node: Arc<CancelRetryNode<C, L>>,
    confirmation: Confirmation<'_, CancelTimerKey>,
) where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    source.retries.publish(node, release_retry_unit::<C, L>, |retained| {
        assert!(
            confirmation.deferred(&retained.facts).is_ok(),
            "发布的原节点须与已核对的登记相同",
        );
    });
}
// 原节点表示留在本模块；本地拥有者只取得已配对的取消接续拥有者。
pub(super) fn new_retry_owner<C, L>(
    notifier: ThreadNotifier,
    batch: std::num::NonZeroUsize,
) -> RetryOwner<CancelRetryNode<C, L>>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    default_retry_owner_new_inner(notifier, batch)
}
include!("access_inner.rs");
