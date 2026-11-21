//! 当前公开访问接口（API）的聚合实现；本地设施操作另归access。
use pi_async_executor::ExecutorTimer;
use pi_async_task_queue::{ExternalQueueManager, ExternalTaskQueue, IdleQueueManager, IdleTaskQueue};
use crate::{instance::*, mode::RuntimeMode};
use pi_async_executor::{ExecutorClock, ExecutorHooks, IdlePolicy};
use pi_async_runtime_core::RuntimeContext;
use pi_clock_manual::global::GlobalManualInstant;
impl<C, E, I, T> RuntimeContext for RuntimeHandle<C, E, I, T>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalTaskQueue,
    I: IdleTaskQueue,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
{
    type Handle = Self;
    fn handle(&self) -> Self {
        self.clone()
    }
}
impl<M, C, P, H, E, I, T> RuntimeContext for ConfiguredRuntime<M, C, P, H, E, I, T>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    type Handle = RuntimeHandle<C, E::Handle, I::Handle, T>;
    fn handle(&self) -> RuntimeHandle<C, E::Handle, I::Handle, T> {
        self.shared.clone()
    }
}
