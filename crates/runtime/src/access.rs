//! 当前本地访问及绑定存续；不实现高层聚合接口（trait）。
use pi_async_executor::ExecutorTimer;
use pi_async_task_queue::{ExternalQueueManager, ExternalTaskQueue, IdleQueueManager, IdleTaskQueue};
use crate::{instance::*, lifecycle::RuntimeBinding, mode::RuntimeMode};
use pi_async_executor::{ExecutorClock, ExecutorHooks, ExecutorLocal, IdlePolicy};
use pi_async_runtime_core::{definitions::LocalRuntimeAccessError, RuntimeQuery};
use pi_clock_manual::global::GlobalManualInstant;

pub(crate) fn with_local<C, E, I, T, R>(
    action: impl FnOnce(&RuntimeHandle<C, E, I, T>, &ExecutorLocal<T>) -> R,
) -> pi_result::RawResult<R, LocalRuntimeAccessError>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalTaskQueue,
    I: IdleTaskQueue,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
{
    runtime_with_local_inner(action)
}
pub(crate) fn ensure_binding<M, C, P, H, E, I, T>(
    runtime: &ConfiguredRuntime<M, C, P, H, E, I, T>,
) -> pi_result::Result<()>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    runtime_bind_inner(runtime)
}
pub(crate) struct BindingExitGuard<'a, C, E, I, T>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalTaskQueue,
    I: IdleTaskQueue,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
{
    pub(crate) binding: &'a RuntimeBinding,
    pub(crate) handle: &'a RuntimeHandle<C, E, I, T>,
}
impl<C, E, I, T> Drop for BindingExitGuard<'_, C, E, I, T>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalTaskQueue,
    I: IdleTaskQueue,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
{
    fn drop(&mut self) {
        binding_exit_inner(self)
    }
}
include!("access_inner.rs");
