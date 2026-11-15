//! 当前公开访问接口（API）的聚合实现；本地设施操作另归access。
use crate::{instance::*, mode::RuntimeMode};
use pi_async_executor::{ExecutorClock, ExecutorHooks, IdlePolicy};
use pi_async_runtime_core::RuntimeContext;
use pi_clock_manual::global::GlobalManualInstant;
impl<C: ExecutorClock<Instant = GlobalManualInstant>> RuntimeContext for RuntimeHandle<C> {
    type Handle = Self;
    fn handle(&self) -> Self {
        self.clone()
    }
}
impl<M, C, P, H> RuntimeContext for ConfiguredRuntime<M, C, P, H>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    type Handle = RuntimeHandle<C>;
    fn handle(&self) -> RuntimeHandle<C> {
        self.shared.clone()
    }
}
