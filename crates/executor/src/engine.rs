//! 通用默认执行器（executor）；业务接纳规则由外层运行时（runtime）传入。
use crate::{
    clock::ExecutorClock,
    drive::*,
    handle::ExecutorHandle,
    hooks::ExecutorHooks,
    local::ExecutorLocal,
    parking::{ThreadNotifier, ThreadParker},
    policy::{ExecutorSnapshot, IdlePolicy},
    registry::TaskRegistry,
    returns::ReturnQueue,
    routing::{ExecutorSchedule, LocalReadySubmit},
    state::ExecutorState,
    timer::ExecutorTimer,
};
use pi_async_runtime_core::{
    block_on::RootWakeState,
    definitions::{RuntimeId, RuntimeStatus},
};
use pi_async_task::{Runnable, TaskThread};
use pi_async_task_queue::{idle::IdleQueueManager, ExternalQueueManager, TaskQueue};
use std::{marker::PhantomData, rc::Rc, sync::Arc, thread::ThreadId};
struct TaskStep {
    ran: bool,
    deferred_task: Option<pi_async_task::definitions::TaskId>,
}
struct TimerStep {
    runs: usize,
    pending: bool,
    deferred_task: Option<pi_async_task::definitions::TaskId>,
}
#[derive(Clone, Copy, Default)]
struct ImportResult {
    progress: bool,
    deferred: bool,
}

/// 供运行时（runtime）实现者静态组合的完整机制；本对象!Send／!Sync，不创建线程。
/// 外部提供当前线程，创建后固定所属域。公开业务由更外层简洁类型包装，不暴露类型链。
pub struct DefaultExecutor<C, E, I, T, L>
where
    C: ExecutorClock,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = C::Instant> + 'static,
    L: LocalReadySubmit,
{
    pub(crate) clock: C,
    pub(crate) external: Option<E>,
    pub(crate) idle: Option<I>,
    pub(crate) local: Rc<ExecutorLocal<T>>,
    pub(crate) handle: ExecutorHandle<E::Handle, I::Handle, T, L>,
    pub(crate) parker: ThreadParker,
    pub(crate) options: ExecutorOptions,
}
/// 只持共享推进标记，!Send／!Sync；不能凭此访问队列或改变原线程。
pub struct DriveLease {
    state: Arc<ExecutorState>,
    runtime: RuntimeId,
    thread: ThreadId,
    local: PhantomData<Rc<()>>,
}
impl Drop for DriveLease {
    fn drop(&mut self) {
        drive_lease_drop_inner(self)
    }
}
struct ExecutorBuildGuard<T: ExecutorTimer + 'static> {
    // owner最先真实退出，余字段即使Drop中展开仍按此顺序自动归还。
    owner: Option<TaskThread>,
    ready: Option<TaskQueue>,
    timer: Option<T>,
    returns: Option<ReturnQueue>,
    registry: Option<TaskRegistry>,
}
impl<T: ExecutorTimer + 'static> Drop for ExecutorBuildGuard<T> {
    fn drop(&mut self) {
        executor_build_drop_inner(self)
    }
}
enum ExecutorExitPhase {
    External,
    Idle,
    Local,
    Done,
}
struct InternalTransferGuard {
    state: Arc<ExecutorState>,
    count: usize,
}
impl Drop for InternalTransferGuard {
    fn drop(&mut self) {
        internal_transfer_drop_inner(self)
    }
}
struct ExecutorExitGuard<'a, C, E, I, T, L>
where
    C: ExecutorClock,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = C::Instant> + 'static,
    L: LocalReadySubmit,
{
    executor: &'a mut DefaultExecutor<C, E, I, T, L>,
    phase: ExecutorExitPhase,
    current_failed: bool,
    failed: bool,
}
impl<C, E, I, T, L> DefaultExecutor<C, E, I, T, L>
where
    C: ExecutorClock,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = C::Instant> + 'static,
    L: LocalReadySubmit,
{
    /// 接管新建的私有普通／空闲管理对象，在本线程新建原所属域。
    /// make_timer取得最终RuntimeId、同一clock及原线程通知，必须返回同源新拥有者。
    /// 原来源应未公开业务访问；构造失败合法释放已接管资源，不初始化全局时钟。
    pub fn new(
        clock: C,
        external: E,
        idle: I,
        options: ExecutorOptions,
        make_timer: impl FnOnce(RuntimeId, &C, ThreadNotifier) -> pi_result::Result<T>,
    ) -> pi_result::Result<Self> {
        executor_new_inner(clock, external, idle, options, make_timer)
    }
    pub fn handle(&self) -> ExecutorHandle<E::Handle, I::Handle, T, L> {
        self.handle.clone()
    }
    /// 框架本地权限注册目标，只供运行时（runtime）装配；最终业务不得取得它。
    pub fn local(&self) -> &Rc<ExecutorLocal<T>> {
        &self.local
    }
    /// 核当前线程并独占标记；重复推进在用户Future首次poll前panic。
    /// 本函数不进入全局DriveScope，供block_on核心自身统一建立该范围。
    pub fn begin_drive(&self) -> DriveLease {
        executor_begin_drive_inner(self)
    }
    /// 一次有限轮转，不停放线程、不等新任务。用户Future、Drop或控制hook可同步阻塞。
    /// 最多One到期／内部／空闲各一次业务run机会；有限批次直接清理另算。
    /// All到期业务按序转内部，不逐项内联poll；已取消／退出者不隐式admit。
    pub fn run_once<A: AdmissionRule, P: IdlePolicy, H: ExecutorHooks>(
        &mut self,
        admission: &A,
        policy: &mut P,
        hooks: &mut H,
    ) -> pi_result::Result<TurnResult> {
        executor_once_inner(self, admission, policy, hooks)
    }
    /// 持续推进至服务关闭；外部提交、已有唤醒、取消／关闭及超时均可使停放返回。
    pub fn run<A: AdmissionRule, P: IdlePolicy, H: ExecutorHooks>(
        &mut self,
        admission: &A,
        policy: &mut P,
        hooks: &mut H,
    ) -> pi_result::Result<()> {
        executor_run_inner(self, admission, policy, hooks)
    }
    /// 已合法取得本次DriveLease的内部轮转；不再次进入DriveScope，不为block_on制造嵌套。
    pub fn turn_with_lease<A: AdmissionRule, P: IdlePolicy, H: ExecutorHooks>(
        &mut self,
        lease: &DriveLease,
        admission: &A,
        policy: &mut P,
        hooks: &mut H,
    ) -> pi_result::Result<TurnResult> {
        executor_turn_inner(self, lease, admission, policy, hooks)
    }
    /// 用上次策略建议重新核工作、通知与定时上界；root仅用于同步根的额外通知。
    /// 返回前若通知已消费必须实际重新轮询，不能以旧snapshot继续停放。
    pub fn park_with_lease<A: AdmissionRule>(
        &mut self,
        lease: &DriveLease,
        admission: &A,
        last: &TurnResult,
        root: Option<&RootWakeState>,
    ) -> pi_result::Result<()> {
        executor_park_inner(self, lease, admission, last, root)
    }
    pub(crate) fn snapshot(
        &self,
        admit: bool,
        timer_pending: bool,
        maintenance: bool,
        readiness: crate::policy::WorkReadiness,
    ) -> ExecutorSnapshot<'_> {
        executor_snapshot_inner(self, admit, timer_pending, maintenance, readiness)
    }
    pub(crate) fn import_returns(&mut self) -> pi_result::Result<ImportResult> {
        executor_returns_inner(self)
    }
    pub(crate) fn import_external<A: AdmissionRule>(
        &mut self,
        admission: &A,
    ) -> pi_result::Result<ImportResult> {
        executor_external_inner(self, admission)
    }
    pub(crate) fn service_timers<H: ExecutorHooks>(
        &mut self,
        hooks: &mut H,
    ) -> pi_result::Result<TimerStep> {
        executor_timers_inner(self, hooks)
    }
    pub(crate) fn run_task<H: ExecutorHooks>(
        &mut self,
        task: Runnable,
        hooks: &mut H,
    ) -> pi_result::Result<TaskStep> {
        executor_task_inner(self, task, hooks)
    }
    pub(crate) fn close_service(&mut self) -> pi_result::Result<()> {
        executor_close_inner(self)
    }
    pub(crate) fn prepare_for_queue(&self, task: Runnable) -> pi_result::Result<Option<Runnable>> {
        executor_prepare_inner(self, task)
    }
}
impl<C, E, I, T, L> Drop for DefaultExecutor<C, E, I, T, L>
where
    C: ExecutorClock,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = C::Instant> + 'static,
    L: LocalReadySubmit,
{
    fn drop(&mut self) {
        executor_drop_inner(self)
    }
}
impl<C, E, I, T, L> ExecutorExitGuard<'_, C, E, I, T, L>
where
    C: ExecutorClock,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = C::Instant> + 'static,
    L: LocalReadySubmit,
{
    fn drain(&mut self) {
        executor_exit_drain_inner(self)
    }
}
impl<C, E, I, T, L> Drop for ExecutorExitGuard<'_, C, E, I, T, L>
where
    C: ExecutorClock,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = C::Instant> + 'static,
    L: LocalReadySubmit,
{
    fn drop(&mut self) {
        executor_exit_drop_inner(self)
    }
}
include!("engine_inner.rs");
