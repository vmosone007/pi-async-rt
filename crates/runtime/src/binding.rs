//! 无实例状态的静态本地回返；不把本地Rc或指针装进跨线程唤醒器（Waker）。
use pi_async_executor::{
    routing::LocalReadySubmit,
    timer::{DefaultTimerItem, ExecutorTimer, LocalTimerSubmit},
    ExecutorClock, ExecutorLocal,
};
use pi_async_runtime_core::definitions::RuntimeId;
use pi_async_task::Runnable;
use pi_async_task_queue::{timer::TimerSubmitError, CancelTimerKey};
use pi_clock_manual::global::GlobalManualInstant;
use std::marker::PhantomData;

/// 框架按选定计时来源关联本地设施（local facilities）。业务无需构造此类型（type）。
pub struct RuntimeTag<C: ExecutorClock<Instant = GlobalManualInstant>, T = ()>(PhantomData<fn() -> (C, T)>);
impl<C, T> LocalReadySubmit for RuntimeTag<C, T>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
{
    fn try_push_local(runtime: RuntimeId, task: Runnable) -> pi_result::RawResult<(), Runnable> {
        tag_ready_inner::<T>(runtime, task)
    }
}
impl<C: ExecutorClock<Instant = GlobalManualInstant>> LocalTimerSubmit<DefaultTimerItem<C, Self>>
    for RuntimeTag<C>
{
    fn try_push_local(
        runtime: RuntimeId,
        deadline: GlobalManualInstant,
        item: DefaultTimerItem<C, Self>,
    ) -> pi_result::RawResult<CancelTimerKey, TimerSubmitError<DefaultTimerItem<C, Self>>> {
        tag_timer_inner::<C>(runtime, deadline, item)
    }
}
include!("binding_inner.rs");
