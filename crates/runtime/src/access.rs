//! 当前本地访问及绑定存续；不实现高层聚合接口（trait）。
use crate::{instance::*, lifecycle::RuntimeBinding, mode::RuntimeMode};
use pi_async_executor::{ExecutorClock, ExecutorHooks, ExecutorLocal, IdlePolicy};
use pi_async_runtime_core::{definitions::LocalRuntimeAccessError, RuntimeQuery};
use pi_clock_manual::global::GlobalManualInstant;

pub(crate) fn with_local<C: ExecutorClock<Instant = GlobalManualInstant>, R>(
    action: impl FnOnce(&RuntimeHandle<C>, &ExecutorLocal<Timer<C>>) -> R,
) -> pi_result::RawResult<R, LocalRuntimeAccessError> {
    runtime_with_local_inner(action)
}
pub(crate) fn ensure_binding<M, C, P, H>(
    runtime: &ConfiguredRuntime<M, C, P, H>,
) -> pi_result::Result<()>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    runtime_bind_inner(runtime)
}
pub(crate) struct BindingExitGuard<'a, C: ExecutorClock<Instant = GlobalManualInstant>> {
    pub(crate) binding: &'a RuntimeBinding,
    pub(crate) handle: &'a RuntimeHandle<C>,
}
impl<C: ExecutorClock<Instant = GlobalManualInstant>> Drop for BindingExitGuard<'_, C> {
    fn drop(&mut self) {
        binding_exit_inner(self)
    }
}
include!("access_inner.rs");
