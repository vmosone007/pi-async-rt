//! 固定目标的唤醒（wake-up）交付适配，不取得异步计算（Future）访问权。
use crate::{parking::ThreadNotifier, returns::ReturnQueueHandle, state::ExecutorState};
use pi_async_runtime_core::definitions::RuntimeId;
use pi_async_task::{Runnable, TaskThread};
use pi_async_task_queue::{BatchAttempt, ExternalPushError, ExternalTaskQueue};
use pi_async_task_wake::{DispatchFailure, DispatchRecovery, WakeTarget};
use std::{
    cell::UnsafeCell,
    marker::PhantomData,
    mem::MaybeUninit,
    sync::{atomic::AtomicU8, Arc},
};

/// 当前本地权限中的快速尾插；失败只返回同一原值，调用者转原归属共享返回。
/// 实现必须使用同一ExecutorState::push_back计数窄出口，成功增加一次internal；
/// 拒绝／展开归还预留后才允许共享回退，不自行重复记账。
pub trait LocalReadySubmit: 'static {
    fn try_push_local(runtime: RuntimeId, task: Runnable) -> pi_result::RawResult<(), Runnable>;
}
/// 供WakeTarget装配的原归属接口；泛型（generic）仅静态标记，不持本地设施。
pub struct ReturnRoute<L: 'static> {
    queue: ReturnQueueHandle,
    identity: Arc<RouteIdentity>,
    local: PhantomData<fn() -> L>,
}
/// 无错误通道交付失败后的退出安排；不会把责任退给Waker调用者。
pub struct ReturnRecovery {
    queue: ReturnQueueHandle,
    state: Arc<ExecutorState>,
    notifier: ThreadNotifier,
}
struct RouteIdentity {
    value: UnsafeCell<MaybeUninit<RuntimeId>>,
    phase: AtomicU8,
}
// phase 0未写、1写者、2已发布；仅构造阶段一次CAS取得写资格，Release发布，读取Acquire。
// shared不保存L实例/Rc/本地指针；身份未就绪只回原共享来源，绝不等待。
// Native WakeTarget持续保留，但不使用其长期借用TaskThread的LocalQueueGuard；
// 同线程路径在本适配内经核心本地权限静态回返，实现同样原目标尾插，不延长TaskThread借用。
// 因此不需要自引用结构、transmute生命周期或公开可替换的static守卫。
// Schedule -> WakeTarget校验 -> 本Route -> 本地尾或原共享返回；拒绝 -> Recovery。
// 编译依赖均向task-wake/task-queue/task/core；L的静态回返不产生反向编译依赖。
// SAFETY: value仅首次成功写者写，Release 2后不可变；读取前Acquire确认，失败不读槽。
unsafe impl Send for RouteIdentity {}
unsafe impl Sync for RouteIdentity {}
pub type ExecutorSchedule<L> = WakeTarget<ReturnRoute<L>, ReturnRecovery>;
impl<L: 'static> Clone for ReturnRoute<L> {
    fn clone(&self) -> Self {
        route_clone_inner(self)
    }
}
impl<L: 'static> ReturnRoute<L> {
    pub(crate) fn new(queue: ReturnQueueHandle) -> Self {
        route_new_inner(queue)
    }
    pub(crate) fn set_runtime(&self, runtime: RuntimeId) {
        route_set_id_inner(self, runtime)
    }
}
impl ReturnRecovery {
    pub(crate) fn new(
        queue: ReturnQueueHandle,
        state: Arc<ExecutorState>,
        notifier: ThreadNotifier,
    ) -> Self {
        recovery_new_inner(queue, state, notifier)
    }
}
impl<L: LocalReadySubmit> ExternalTaskQueue for ReturnRoute<L> {
    fn try_push(&self, task: Runnable) -> pi_result::RawResult<(), ExternalPushError> {
        route_push_inner(self, task)
    }
    fn try_take_batch(&self) -> BatchAttempt {
        self.queue.try_take_batch()
    }
    fn len(&self) -> usize {
        self.queue.len()
    }
}
impl DispatchRecovery for ReturnRecovery {
    fn retain(&self, task: Runnable, reason: DispatchFailure) {
        recovery_retain_inner(self, task, reason)
    }
}
pub(crate) fn new_schedule<L: LocalReadySubmit>(
    owner: &TaskThread,
    route: ReturnRoute<L>,
    recovery: ReturnRecovery,
) -> ExecutorSchedule<L> {
    WakeTarget::new(owner, route, recovery)
}
include!("routing_inner.rs");
