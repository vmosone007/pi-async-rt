//! 仅编译类型关系，不执行未实现的驱动或计时操作。
use pi_async_executor::timer::{
    DefaultTimerAccess, DefaultTimerDriver, DefaultTimerItem, DefaultTimerRegistration,
    LocalTimerSubmit,
};
use pi_async_executor::{ManualClock, ThreadNotifier};
use pi_async_runtime_core::definitions::RuntimeId;
use pi_async_task_queue::{
    timer::{TimerQueueError, TimerSubmitError},
    CancelTimerKey, CancelTimerOptions,
};
use pi_clock_manual::global::GlobalManualInstant;
use std::{marker::PhantomData, rc::Rc};

// 本地路由类型明确不可Send/Sync；共享访问的自动约束不能因此被连带加强。
struct Route(PhantomData<Rc<()>>);
impl LocalTimerSubmit<DefaultTimerItem<ManualClock, Self>> for Route {
    fn try_push_local(
        _: RuntimeId,
        _: GlobalManualInstant,
        item: DefaultTimerItem<ManualClock, Self>,
    ) -> pi_result::RawResult<CancelTimerKey, TimerSubmitError<DefaultTimerItem<ManualClock, Self>>>
    {
        // 只用于类型实例化；没有当前环境时保原值拒绝，不代补任何生产行为。
        Err(TimerSubmitError {
            item,
            cause: TimerQueueError::WrongOwner,
        })
    }
}
fn send_sync<T: Send + Sync>() {}
fn send<T: Send>() {}
fn declarations(id: RuntimeId, clock: ManualClock, notifier: ThreadNotifier) {
    send_sync::<DefaultTimerAccess<ManualClock, Route>>();
    send_sync::<DefaultTimerRegistration<ManualClock, Route>>();
    send::<DefaultTimerItem<ManualClock, Route>>();
    let _ = DefaultTimerDriver::<_, Route>::new(id, clock, notifier, CancelTimerOptions::default());
}
fn main() {}
