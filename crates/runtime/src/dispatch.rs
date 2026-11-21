//! 一类派发入口（dispatch API）；只组合原工厂与受限来源，不再定义任务生命周期。
use pi_async_executor::ExecutorTimer;
use pi_async_task_queue::{ExternalQueueManager, ExternalTaskQueue, IdleQueueManager, IdleTaskQueue};
use crate::{access, instance::*, mode::RuntimeMode};
use pi_async_executor::{ExecutorClock, ExecutorHooks, IdlePolicy, ThreadNotifier};
use pi_async_runtime_core::task::{CreateError, SpawnError, SubmitError};
use pi_async_runtime_core::{
    IdleDispatch, LocalTaskDispatch, TaskConfig, TaskDispatch, TimedTaskDispatch, UnsubmittedTask,
};
use pi_async_runtime_core::time::TimedTaskNotifier as CoreTimedTaskNotifier;
use pi_async_task::LocalOutput;
use pi_clock_manual::global::GlobalManualInstant;
use std::{future::Future, time::Duration};

pub(crate) fn create_local<C, E, I, T, F, V>(
    future: F,
    config: TaskConfig<V>,
) -> pi_result::RawResult<(UnsubmittedTask, LocalTaskHandle<F::Output, V>), CreateError<F, V>>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalTaskQueue,
    I: IdleTaskQueue,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
    F: Future + 'static,
    F::Output: 'static,
    V: Send + Sync + 'static,
{
    local_create_inner::<C, E, I, T, F, V>(future, config)
}
pub(crate) fn submit_local<C, E, I, T>(
    task: UnsubmittedTask,
    front: bool,
) -> pi_result::RawResult<(), SubmitError>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalTaskQueue,
    I: IdleTaskQueue,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
{
    local_submit_inner::<C, E, I, T>(task, front)
}
pub(crate) fn idle_local<C, E, I, T, F, V>(
    future: F,
    config: TaskConfig<V>,
) -> pi_result::RawResult<TaskHandle<F::Output, V>, SpawnError<F, ThreadNotifier, V>>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalTaskQueue,
    I: IdleTaskQueue,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    V: Send + Sync + 'static,
{
    local_idle_inner::<C, E, I, T, F, V>(future, config)
}
pub(crate) fn timing_local<C, E, I, T, F, V>(
    future: F,
    delay: Duration,
    config: TaskConfig<V>,
) -> pi_result::RawResult<
    pi_async_runtime_core::task::TaskHandle<CoreTimedTaskNotifier<ThreadNotifier, T::Access>, F::Output, V, LocalOutput>,
    SpawnError<F, CoreTimedTaskNotifier<ThreadNotifier, T::Access>, V, LocalOutput>,
>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalTaskQueue,
    I: IdleTaskQueue,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
    F: Future + 'static,
    F::Output: 'static,
    V: Send + Sync + 'static,
{
    local_timing_inner::<C, E, I, T, F, V>(future, delay, config)
}
include!("dispatch_inner.rs");
impl<C, E, I, T> TaskDispatch for RuntimeHandle<C, E, I, T>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalTaskQueue,
    I: IdleTaskQueue,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
{
    type Notifier = ThreadNotifier;
    fn create_with_config<F, V>(
        &self,
        future: F,
        config: TaskConfig<V>,
    ) -> pi_result::RawResult<(UnsubmittedTask, TaskHandle<F::Output, V>), CreateError<F, V>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
        V: Send + Sync + 'static,
    {
        self.shared().inner.create(future, config)
    }
    fn submit(&self, task: UnsubmittedTask) -> pi_result::RawResult<(), SubmitError> {
        self.shared().inner.submit(task)
    }
}
impl<C, E, I, T> LocalTaskDispatch for RuntimeHandle<C, E, I, T>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalTaskQueue,
    I: IdleTaskQueue,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
{
    fn create_local_with_config<F, V>(
        future: F,
        config: TaskConfig<V>,
    ) -> pi_result::RawResult<(UnsubmittedTask, LocalTaskHandle<F::Output, V>), CreateError<F, V>>
    where
        F: Future + 'static,
        F::Output: 'static,
        V: Send + Sync + 'static,
    {
        create_local::<C, E, I, T, F, V>(future, config)
    }
    fn submit_local(task: UnsubmittedTask) -> pi_result::RawResult<(), SubmitError> {
        submit_local::<C, E, I, T>(task, false)
    }
}
impl<C, E, I, T> IdleDispatch for RuntimeHandle<C, E, I, T>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalTaskQueue,
    I: IdleTaskQueue,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
{
    fn submit_idle(&self, task: UnsubmittedTask) -> pi_result::RawResult<(), SubmitError> {
        self.shared().inner.submit_idle(task)
    }
    fn idle_local_with_config<F, V>(
        future: F,
        config: TaskConfig<V>,
    ) -> pi_result::RawResult<TaskHandle<F::Output, V>, SpawnError<F, ThreadNotifier, V>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
        V: Send + Sync + 'static,
    {
        idle_local::<C, E, I, T, F, V>(future, config)
    }
}
impl<C, E, I, T> TimedTaskDispatch for RuntimeHandle<C, E, I, T>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalTaskQueue,
    I: IdleTaskQueue,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
{
    fn spawn_timing_with_config<F, V>(
        &self,
        future: F,
        delay: Duration,
        config: TaskConfig<V>,
    ) -> pi_result::RawResult<
        pi_async_runtime_core::task::TaskHandle<CoreTimedTaskNotifier<ThreadNotifier, T::Access>, F::Output, V>,
        SpawnError<F, CoreTimedTaskNotifier<ThreadNotifier, T::Access>, V>,
    >
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
        V: Send + Sync + 'static,
    {
        self.shared().inner.spawn_timing(future, delay, config)
    }
    fn spawn_timing_local_with_config<F, V>(
        future: F,
        delay: Duration,
        config: TaskConfig<V>,
    ) -> pi_result::RawResult<
        pi_async_runtime_core::task::TaskHandle<CoreTimedTaskNotifier<ThreadNotifier, T::Access>, F::Output, V, LocalOutput>,
        SpawnError<F, CoreTimedTaskNotifier<ThreadNotifier, T::Access>, V, LocalOutput>,
    >
    where
        F: Future + 'static,
        F::Output: 'static,
        V: Send + Sync + 'static,
    {
        timing_local::<C, E, I, T, F, V>(future, delay, config)
    }
}
impl<C, E, I, T> RuntimeHandle<C, E, I, T>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalTaskQueue,
    I: IdleTaskQueue,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
{
    /// 由当前原线程把首次提交放内部头；无本地权限时返原值及原因。
    pub fn submit_local_front(task: UnsubmittedTask) -> pi_result::RawResult<(), SubmitError> {
        submit_local::<C, E, I, T>(task, true)
    }
    /// 本地队首便利派发；与spawn_local相同原值及权限，只改变下一次排队优先级。
    pub fn spawn_local_front_with_config<F, V>(
        future: F,
        config: TaskConfig<V>,
    ) -> pi_result::RawResult<
        LocalTaskHandle<F::Output, V>,
        SpawnError<F, ThreadNotifier, V, LocalOutput>,
    >
    where
        F: Future + 'static,
        F::Output: 'static,
        V: Send + Sync + 'static,
    {
        let (task, handle) = create_local::<C, E, I, T, F, V>(future, config).map_err(SpawnError::Create)?;
        match submit_local::<C, E, I, T>(task, true) {
            Ok(()) => Ok(handle),
            Err(error) => Err(SpawnError::Submit { error, handle }),
        }
    }
    /// 默认附加数据（metadata）的本地队首入口，不增加F的Send／Sync／Unpin。
    pub fn spawn_local_front<F>(
        future: F,
    ) -> pi_result::RawResult<
        LocalTaskHandle<F::Output>,
        SpawnError<F, ThreadNotifier, (), LocalOutput>,
    >
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        Self::spawn_local_front_with_config(future, TaskConfig::default())
    }
}
impl<M, C, P, H, E, I, T> TaskDispatch for ConfiguredRuntime<M, C, P, H, E, I, T>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    type Notifier = ThreadNotifier;
    fn create_with_config<F, V>(
        &self,
        future: F,
        config: TaskConfig<V>,
    ) -> pi_result::RawResult<(UnsubmittedTask, TaskHandle<F::Output, V>), CreateError<F, V>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
        V: Send + Sync + 'static,
    {
        self.shared().inner.create(future, config)
    }
    fn submit(&self, task: UnsubmittedTask) -> pi_result::RawResult<(), SubmitError> {
        self.shared().inner.submit(task)
    }
}
impl<M, C, P, H, E, I, T> LocalTaskDispatch for ConfiguredRuntime<M, C, P, H, E, I, T>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    fn create_local_with_config<F, V>(
        future: F,
        config: TaskConfig<V>,
    ) -> pi_result::RawResult<(UnsubmittedTask, LocalTaskHandle<F::Output, V>), CreateError<F, V>>
    where
        F: Future + 'static,
        F::Output: 'static,
        V: Send + Sync + 'static,
    {
        create_local::<C, E::Handle, I::Handle, T, F, V>(future, config)
    }
    fn submit_local(task: UnsubmittedTask) -> pi_result::RawResult<(), SubmitError> {
        submit_local::<C, E::Handle, I::Handle, T>(task, false)
    }
}
impl<M, C, P, H, E, I, T> IdleDispatch for ConfiguredRuntime<M, C, P, H, E, I, T>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    fn submit_idle(&self, task: UnsubmittedTask) -> pi_result::RawResult<(), SubmitError> {
        self.shared().inner.submit_idle(task)
    }
    fn idle_local_with_config<F, V>(
        future: F,
        config: TaskConfig<V>,
    ) -> pi_result::RawResult<TaskHandle<F::Output, V>, SpawnError<F, ThreadNotifier, V>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
        V: Send + Sync + 'static,
    {
        idle_local::<C, E::Handle, I::Handle, T, F, V>(future, config)
    }
}
impl<M, C, P, H, E, I, T> TimedTaskDispatch for ConfiguredRuntime<M, C, P, H, E, I, T>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    fn spawn_timing_with_config<F, V>(
        &self,
        future: F,
        delay: Duration,
        config: TaskConfig<V>,
    ) -> pi_result::RawResult<
        pi_async_runtime_core::task::TaskHandle<CoreTimedTaskNotifier<ThreadNotifier, T::Access>, F::Output, V>,
        SpawnError<F, CoreTimedTaskNotifier<ThreadNotifier, T::Access>, V>,
    >
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
        V: Send + Sync + 'static,
    {
        self.shared().inner.spawn_timing(future, delay, config)
    }
    fn spawn_timing_local_with_config<F, V>(
        future: F,
        delay: Duration,
        config: TaskConfig<V>,
    ) -> pi_result::RawResult<
        pi_async_runtime_core::task::TaskHandle<CoreTimedTaskNotifier<ThreadNotifier, T::Access>, F::Output, V, LocalOutput>,
        SpawnError<F, CoreTimedTaskNotifier<ThreadNotifier, T::Access>, V, LocalOutput>,
    >
    where
        F: Future + 'static,
        F::Output: 'static,
        V: Send + Sync + 'static,
    {
        timing_local::<C, E::Handle, I::Handle, T, F, V>(future, delay, config)
    }
}
impl<M, C, P, H, E, I, T> ConfiguredRuntime<M, C, P, H, E, I, T>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    /// 由当前原线程把首次提交放内部头；无本地权限时返原值及原因。
    pub fn submit_local_front(task: UnsubmittedTask) -> pi_result::RawResult<(), SubmitError> {
        submit_local::<C, E::Handle, I::Handle, T>(task, true)
    }
    /// 本地队首便利派发；与spawn_local相同原值及权限，只改变下一次排队优先级。
    pub fn spawn_local_front_with_config<F, V>(
        future: F,
        config: TaskConfig<V>,
    ) -> pi_result::RawResult<
        LocalTaskHandle<F::Output, V>,
        SpawnError<F, ThreadNotifier, V, LocalOutput>,
    >
    where
        F: Future + 'static,
        F::Output: 'static,
        V: Send + Sync + 'static,
    {
        let (task, handle) = create_local::<C, E::Handle, I::Handle, T, F, V>(future, config).map_err(SpawnError::Create)?;
        match submit_local::<C, E::Handle, I::Handle, T>(task, true) {
            Ok(()) => Ok(handle),
            Err(error) => Err(SpawnError::Submit { error, handle }),
        }
    }
    /// 默认附加数据（metadata）的本地队首入口，不增加F的Send／Sync／Unpin。
    pub fn spawn_local_front<F>(
        future: F,
    ) -> pi_result::RawResult<
        LocalTaskHandle<F::Output>,
        SpawnError<F, ThreadNotifier, (), LocalOutput>,
    >
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        Self::spawn_local_front_with_config(future, TaskConfig::default())
    }
}
