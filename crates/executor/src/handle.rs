//! 执行器（executor）的框架共享入口；最终运行时（runtime）再封装为业务句柄。
use crate::{
    local::ExecutorLocal,
    parking::ThreadNotifier,
    registry::TaskRegistryHandle,
    routing::{ExecutorSchedule, LocalReadySubmit},
    state::ExecutorState,
    timer::ExecutorTimer,
};
use pi_async_runtime_core::{
    control::RuntimeControl,
    definitions::*,
    query::RuntimeQuery,
    task::*,
    time::{TimedTaskNotifier, TimerAccess},
};
use pi_async_task::{definitions::TaskOwnerId, LocalOutput};
use pi_async_task_queue::{idle::IdleTaskQueue, timer::TimerQueueHandle, ExternalTaskQueue};
use std::{future::Future, marker::PhantomData, sync::Arc, time::Duration};

/// 支持Clone＋Send＋Sync；T/L仅静态类型映射，不把本地拥有者装进共享存储。
/// 不公开工厂（factory）、队列（queue）句柄或管理权限；不能推进服务。
pub struct ExecutorHandle<
    E: ExternalTaskQueue + Clone,
    I: IdleTaskQueue + Clone,
    T: ExecutorTimer + 'static,
    L: LocalReadySubmit,
> {
    pub(crate) factory: TaskFactory<ExecutorSchedule<L>, ThreadNotifier>,
    pub(crate) external: E,
    pub(crate) idle: I,
    pub(crate) timer: T::Access,
    pub(crate) timer_query: T::Handle,
    pub(crate) state: Arc<ExecutorState>,
    pub(crate) registry: TaskRegistryHandle,
    pub(crate) notifier: ThreadNotifier,
    pub(crate) owner: TaskOwnerId,
    pub(crate) types: PhantomData<fn() -> T>,
}
struct SourceNotifyGuard<'a> {
    notifier: &'a ThreadNotifier,
    armed: bool,
}
impl Drop for SourceNotifyGuard<'_> {
    fn drop(&mut self) {
        source_notify_drop_inner(self)
    }
}
impl<
        E: ExternalTaskQueue + Clone,
        I: IdleTaskQueue + Clone,
        T: ExecutorTimer + 'static,
        L: LocalReadySubmit,
    > Clone for ExecutorHandle<E, I, T, L>
{
    fn clone(&self) -> Self {
        handle_clone_inner(self)
    }
}
impl<
        E: ExternalTaskQueue + Clone,
        I: IdleTaskQueue + Clone,
        T: ExecutorTimer + 'static,
        L: LocalReadySubmit,
    > ExecutorHandle<E, I, T, L>
{
    /// 取得原服务计时抽象，不暴露队列管理或强本地持有。
    pub fn timer_access(&self) -> T::Access {
        self.timer.clone()
    }
    /// 创建后先登记基本查询，再导出原值；创建本身不要求服务推进，也不隐式提交。
    /// 普通创建可在服务结束后成功，后续提交仍明确拒绝；不重新开放服务。
    pub fn create<F, M>(
        &self,
        future: F,
        config: TaskConfig<M>,
    ) -> pi_result::RawResult<
        (UnsubmittedTask, TaskHandle<ThreadNotifier, F::Output, M>),
        CreateError<F, M>,
    >
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
        M: Send + Sync + 'static,
    {
        handle_create_inner(self, future, config)
    }
    pub fn submit(&self, task: UnsubmittedTask) -> pi_result::RawResult<(), SubmitError> {
        handle_submit_inner(self, task, false)
    }
    pub fn submit_idle(&self, task: UnsubmittedTask) -> pi_result::RawResult<(), SubmitError> {
        handle_submit_inner(self, task, true)
    }
    /// 由上层已核同一当前权限的本地设施提供；本函数仍核RuntimeId，不替外线程借owner。
    pub fn create_local<F, M>(
        &self,
        local: &ExecutorLocal<T>,
        future: F,
        config: TaskConfig<M>,
    ) -> pi_result::RawResult<
        (
            UnsubmittedTask,
            TaskHandle<ThreadNotifier, F::Output, M, LocalOutput>,
        ),
        CreateError<F, M>,
    >
    where
        F: Future + 'static,
        F::Output: 'static,
        M: Send + Sync + 'static,
    {
        handle_create_local_inner(self, local, future, config)
    }
    pub fn submit_local(
        &self,
        local: &ExecutorLocal<T>,
        task: UnsubmittedTask,
    ) -> pi_result::RawResult<(), SubmitError> {
        handle_submit_local_inner(self, local, task, false)
    }
    /// 原线程首次提交到内部头；只改变下一次选取，不抢断当前Future或绕过接纳/取消规则。
    pub fn submit_local_front(
        &self,
        local: &ExecutorLocal<T>,
        task: UnsubmittedTask,
    ) -> pi_result::RawResult<(), SubmitError> {
        handle_submit_local_inner(self, local, task, true)
    }
    pub fn spawn_timing<F, M>(
        &self,
        future: F,
        delay: Duration,
        config: TaskConfig<M>,
    ) -> pi_result::RawResult<
        TaskHandle<TimedTaskNotifier<ThreadNotifier, T::Access>, F::Output, M>,
        SpawnError<F, TimedTaskNotifier<ThreadNotifier, T::Access>, M>,
    >
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
        M: Send + Sync + 'static,
    {
        handle_timed_inner(self, future, delay, config)
    }
    pub fn spawn_timing_local<F, M>(
        &self,
        local: &ExecutorLocal<T>,
        future: F,
        delay: Duration,
        config: TaskConfig<M>,
    ) -> pi_result::RawResult<
        TaskHandle<TimedTaskNotifier<ThreadNotifier, T::Access>, F::Output, M, LocalOutput>,
        SpawnError<F, TimedTaskNotifier<ThreadNotifier, T::Access>, M, LocalOutput>,
    >
    where
        F: Future + 'static,
        F::Output: 'static,
        M: Send + Sync + 'static,
    {
        handle_timed_local_inner(self, local, future, delay, config)
    }
}
impl<
        E: ExternalTaskQueue + Clone,
        I: IdleTaskQueue + Clone,
        T: ExecutorTimer + 'static,
        L: LocalReadySubmit,
    > RuntimeQuery for ExecutorHandle<E, I, T, L>
{
    fn id(&self) -> RuntimeId {
        self.factory.runtime_id()
    }
    fn status(&self) -> RuntimeStatus {
        self.state.status()
    }
    fn is_driving(&self) -> bool {
        self.state.is_driving()
    }
    fn last_idle_state(&self) -> IdleState {
        self.state.last_idle_state()
    }
    fn close_state(&self) -> CloseState {
        self.state.close_state()
    }
    fn external_task_count(&self) -> usize {
        self.external.len()
    }
    fn internal_task_count(&self) -> usize {
        handle_internal_count_inner(self)
    }
    fn timer_registration_count(&self) -> usize {
        self.timer_query.len()
    }
    fn idle_task_count(&self) -> usize {
        self.idle.len()
    }
    fn in_flight_task_count(&self) -> usize {
        handle_in_flight_inner(self)
    }
    fn waiting_for_wake_count(&self) -> usize {
        self.registry.waiting_for_wake_count()
    }
    fn pending_cancel_task_count(&self) -> usize {
        self.registry.pending_cancel_count()
    }
}
impl<
        E: ExternalTaskQueue + Clone,
        I: IdleTaskQueue + Clone,
        T: ExecutorTimer + 'static,
        L: LocalReadySubmit,
    > RuntimeControl for ExecutorHandle<E, I, T, L>
{
    fn request_close(&self) -> CloseRequestStatus {
        self.state.request_close(&self.notifier)
    }
}
include!("handle_inner.rs");
