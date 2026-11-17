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
/// waiting_for_wake_count及pending_cancel_task_count按需O(n)扫描已登记任务（Task），
/// n为当前可观察登记规模；两项独立取样，不承诺全局同一瞬间，默认事件循环（event loop）不主动调用。
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
    /// 将本服务普通创建的原值提交至普通外部来源，不要求当前正在推进服务。
    /// 错误保留同一UnsubmittedTask与原原因；本地创建值须走本地入口，关闭服务拒绝新提交。
    /// 成功只确认来源接收并安排通知，不保证已经接纳或轮询（poll）。
    pub fn submit(&self, task: UnsubmittedTask) -> pi_result::RawResult<(), SubmitError> {
        handle_submit_inner(self, task, false)
    }
    /// 将本服务普通创建的Send任务（Task）交给空闲来源，不保证空闲发生、首次接纳或执行。
    /// 关闭、错服务或错误创建方式等明确拒绝时返还同一原值及原因，不改变已有取消事实。
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
    /// 使用同一原服务、原所属域且有效的本地设施，将本地创建值放入内部队尾。
    /// 错服务、错所属域、失效或队列拒绝均返还同一原值及原因，不回退到外部来源。
    /// 提交不隐式接纳、轮询（poll）或取消；普通创建值须走普通入口。
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
    /// 同步创建并提交本服务定时任务（Task），delay从本次提交准备取时，零时长也真实登记。
    /// delay允许0或至少1ms，无sleep的1s固定上限，但仍须满足原时钟与截止时间的可表示范围。
    /// 准备或创建失败返还原异步计算（Future）与配置；明确提交拒绝保留待提交原值及结果句柄（handle）。
    /// 成功仅确认定时来源接收，截止后仍由原服务接纳／推进，不保证按墙钟时间立即执行。
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
    /// 在同服务、原所属域及有效本地权限中同步创建并提交定时任务（Task），不要求F或输出Send／Sync。
    /// delay沿spawn_timing的0或至少1ms规则，无1s固定上限，仍须满足截止时间可表示范围。
    /// 计时、零时长及分阶段错误返还沿普通定时派发约定；本地输出仍限原所属线程（owner thread）。
    /// 缺少合法本地能力时保留原输入和原因，不转投其它服务或跨线程处理原异步计算（Future）。
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
