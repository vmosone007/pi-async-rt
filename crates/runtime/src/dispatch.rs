//! 一类派发入口（dispatch API）；只组合原工厂与受限来源，不再定义任务生命周期。
use crate::{access, instance::*, mode::RuntimeMode};
use pi_async_executor::{ExecutorClock, ExecutorHooks, IdlePolicy, ThreadNotifier};
use pi_async_runtime_core::task::{CreateError, SpawnError, SubmitError};
use pi_async_runtime_core::{
    IdleDispatch, LocalTaskDispatch, TaskConfig, TaskDispatch, TimedTaskDispatch, UnsubmittedTask,
};
use pi_async_task::LocalOutput;
use pi_clock_manual::global::GlobalManualInstant;
use std::{future::Future, time::Duration};

pub(crate) fn create_local<C, F, V>(
    future: F,
    config: TaskConfig<V>,
) -> pi_result::RawResult<(UnsubmittedTask, LocalTaskHandle<F::Output, V>), CreateError<F, V>>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    F: Future + 'static,
    F::Output: 'static,
    V: Send + Sync + 'static,
{
    local_create_inner::<C, F, V>(future, config)
}
pub(crate) fn submit_local<C: ExecutorClock<Instant = GlobalManualInstant>>(
    task: UnsubmittedTask,
    front: bool,
) -> pi_result::RawResult<(), SubmitError> {
    local_submit_inner::<C>(task, front)
}
pub(crate) fn idle_local<C, F, V>(
    future: F,
    config: TaskConfig<V>,
) -> pi_result::RawResult<TaskHandle<F::Output, V>, SpawnError<F, ThreadNotifier, V>>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    V: Send + Sync + 'static,
{
    local_idle_inner::<C, F, V>(future, config)
}
pub(crate) fn timing_local<C, F, V>(
    future: F,
    delay: Duration,
    config: TaskConfig<V>,
) -> pi_result::RawResult<
    pi_async_runtime_core::task::TaskHandle<TimedTaskNotifier<C>, F::Output, V, LocalOutput>,
    SpawnError<F, TimedTaskNotifier<C>, V, LocalOutput>,
>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    F: Future + 'static,
    F::Output: 'static,
    V: Send + Sync + 'static,
{
    local_timing_inner::<C, F, V>(future, delay, config)
}
include!("dispatch_inner.rs");
impl<C> TaskDispatch for RuntimeHandle<C>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
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
impl<C> LocalTaskDispatch for RuntimeHandle<C>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
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
        create_local::<C, F, V>(future, config)
    }
    fn submit_local(task: UnsubmittedTask) -> pi_result::RawResult<(), SubmitError> {
        submit_local::<C>(task, false)
    }
}
impl<C> IdleDispatch for RuntimeHandle<C>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
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
        idle_local::<C, F, V>(future, config)
    }
}
impl<C> TimedTaskDispatch for RuntimeHandle<C>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
{
    fn spawn_timing_with_config<F, V>(
        &self,
        future: F,
        delay: Duration,
        config: TaskConfig<V>,
    ) -> pi_result::RawResult<
        TimedTaskHandle<F::Output, V, C>,
        SpawnError<F, TimedTaskNotifier<C>, V>,
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
        pi_async_runtime_core::task::TaskHandle<TimedTaskNotifier<C>, F::Output, V, LocalOutput>,
        SpawnError<F, TimedTaskNotifier<C>, V, LocalOutput>,
    >
    where
        F: Future + 'static,
        F::Output: 'static,
        V: Send + Sync + 'static,
    {
        timing_local::<C, F, V>(future, delay, config)
    }
}
impl<C> RuntimeHandle<C>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
{
    /// 由当前原线程把首次提交放内部头；无本地权限时返原值及原因。
    pub fn submit_local_front(task: UnsubmittedTask) -> pi_result::RawResult<(), SubmitError> {
        submit_local::<C>(task, true)
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
        let (task, handle) = create_local::<C, F, V>(future, config).map_err(SpawnError::Create)?;
        match submit_local::<C>(task, true) {
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
impl<M, C, P, H> TaskDispatch for ConfiguredRuntime<M, C, P, H>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
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
impl<M, C, P, H> LocalTaskDispatch for ConfiguredRuntime<M, C, P, H>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
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
        create_local::<C, F, V>(future, config)
    }
    fn submit_local(task: UnsubmittedTask) -> pi_result::RawResult<(), SubmitError> {
        submit_local::<C>(task, false)
    }
}
impl<M, C, P, H> IdleDispatch for ConfiguredRuntime<M, C, P, H>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
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
        idle_local::<C, F, V>(future, config)
    }
}
impl<M, C, P, H> TimedTaskDispatch for ConfiguredRuntime<M, C, P, H>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    fn spawn_timing_with_config<F, V>(
        &self,
        future: F,
        delay: Duration,
        config: TaskConfig<V>,
    ) -> pi_result::RawResult<
        TimedTaskHandle<F::Output, V, C>,
        SpawnError<F, TimedTaskNotifier<C>, V>,
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
        pi_async_runtime_core::task::TaskHandle<TimedTaskNotifier<C>, F::Output, V, LocalOutput>,
        SpawnError<F, TimedTaskNotifier<C>, V, LocalOutput>,
    >
    where
        F: Future + 'static,
        F::Output: 'static,
        V: Send + Sync + 'static,
    {
        timing_local::<C, F, V>(future, delay, config)
    }
}
impl<M, C, P, H> ConfiguredRuntime<M, C, P, H>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    /// 由当前原线程把首次提交放内部头；无本地权限时返原值及原因。
    pub fn submit_local_front(task: UnsubmittedTask) -> pi_result::RawResult<(), SubmitError> {
        submit_local::<C>(task, true)
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
        let (task, handle) = create_local::<C, F, V>(future, config).map_err(SpawnError::Create)?;
        match submit_local::<C>(task, true) {
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
