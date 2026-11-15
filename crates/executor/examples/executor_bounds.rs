//! 仅编译静态组合及共享句柄（shared handle）约束，不执行占位。
use pi_async_executor::{
    routing::LocalReadySubmit,
    timer::{DefaultTimerDriver, DefaultTimerItem, LocalTimerSubmit},
    DefaultExecutor, ExecutorHandle, ExecutorOptions, ManualClock,
};
use pi_async_runtime_core::definitions::RuntimeId;
use pi_async_task::Runnable;
use pi_async_task_queue::{
    timer::{TimerQueueError, TimerSubmitError},
    CancelTimerKey, CancelTimerOptions, MpscIdleTaskQueue, MpscIdleTaskQueueHandle, MpscTaskQueue,
    MpscTaskQueueHandle,
};
use pi_clock_manual::global::GlobalManualInstant;
use std::{marker::PhantomData, rc::Rc};
struct LocalOnly(PhantomData<Rc<()>>);
impl LocalReadySubmit for LocalOnly {
    fn try_push_local(_: RuntimeId, task: Runnable) -> pi_result::RawResult<(), Runnable> {
        Err(task)
    }
}
impl LocalTimerSubmit<DefaultTimerItem<ManualClock, Self>> for LocalOnly {
    fn try_push_local(
        _: RuntimeId,
        _: GlobalManualInstant,
        item: DefaultTimerItem<ManualClock, Self>,
    ) -> pi_result::RawResult<CancelTimerKey, TimerSubmitError<DefaultTimerItem<ManualClock, Self>>>
    {
        Err(TimerSubmitError {
            item,
            cause: TimerQueueError::WrongOwner,
        })
    }
}
type Timer = DefaultTimerDriver<ManualClock, LocalOnly>;
type Service = DefaultExecutor<ManualClock, MpscTaskQueue, MpscIdleTaskQueue, Timer, LocalOnly>;
type Shared = ExecutorHandle<MpscTaskQueueHandle, MpscIdleTaskQueueHandle, Timer, LocalOnly>;
fn send_sync<T: Send + Sync>() {}
fn declarations(clock: ManualClock, external: MpscTaskQueue, idle: MpscIdleTaskQueue) {
    send_sync::<Shared>();
    let _: pi_result::Result<Service> = Service::new(
        clock,
        external,
        idle,
        ExecutorOptions::default(),
        |id, clock, notify| Timer::new(id, *clock, notify, CancelTimerOptions::default()),
    );
}
fn main() {}
