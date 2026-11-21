#![doc = include_str!("../README.md")]
#![doc(html_no_source)]
mod access;
#[doc(hidden)]
pub mod binding;
mod capabilities;
mod current;
mod dispatch;
mod driving;
pub mod instance;
mod lifecycle;
pub mod mode;
pub mod options;
mod queues;

pub use instance::{
    ConcurrentRuntime, ConfiguredRuntime, Handle, LocalTaskHandle, Runtime, RuntimeHandle,
    RuntimeTimerAccess, SerialRuntime, TaskHandle, TimedTaskHandle, TimedTaskNotifier,
};
pub use mode::{ConcurrentMode, SerialMode};
pub use options::RuntimeOptions;
pub use queues::RuntimeQueues;
pub use pi_async_executor::{
    DefaultIdlePolicy, ExecutorClock, ExecutorEvent, ExecutorHooks, ExecutorOptions, IdlePolicy,
    ManualClock, NoHooks, TaskAction, TaskInfo, TimerDelivery, TurnResult,
};
#[cfg(feature = "observe")]
pub use pi_async_runtime_core::{TaskEvent, TaskObserver};
pub use pi_async_runtime_core::{
    BuildErrorKind, CancelRequestStatus, CancellationError, CancellationOutcome, CreateError,
    CurrentTaskAccessError, ExecutionStatus, ExternalPushErrorKind, FutureDropStatus, IdleQueueError,
    QueuePushErrorKind, SendError, SendErrorKind, SpawnError, SubmitError, TaskCancelWait, TaskError,
    TaskOwnerId, TaskPhase, TaskQuery, TaskSnapshot, TerminationKind, TimerAccess,
    TimerCancelRequestOutcome, TimerQueueError, WaitError,
};
pub use pi_async_runtime_core::{
    wait_all, wait_any, wait_one, yield_now, AllSender, AnySender, CloseRequestStatus, CloseState,
    CreateErrorKind, CurrentRuntimeAccessError, IdleDispatch, IdleState, LocalRuntimeAccessError,
    LocalTaskDispatch, OneSender, Runtime as RuntimeCapabilities, RuntimeBlockOn, RuntimeContext,
    RuntimeControl, RuntimeId, RuntimeQuery, RuntimeStatus, RuntimeTime, RuntimeWait, Sleep,
    SubmitErrorKind, TaskConfig, TaskControl, TaskDispatch, TaskId, TimedTaskDispatch, Timeout,
    TimeoutControl, TimeoutError, TimeoutOn, TimerError, UnsubmittedTask, WaitAll, WaitAllTimeout,
    WaitAny, WaitAnyTimeout, WaitOne, WaitOneTimeout, WaitTimeoutError,
};
pub use pi_async_task::task::{try_current_task_id, try_with_current_metadata};

/// 常用类型（type）和共同能力接口（trait）；不导出内部来源或管理权限。
pub mod prelude {
    pub use crate::{
        try_current_task_id, try_with_current_metadata, wait_all, wait_any, wait_one, yield_now,
        ConcurrentRuntime, Handle, IdleDispatch, LocalTaskDispatch, RuntimeBlockOn, RuntimeContext,
        RuntimeCapabilities, RuntimeControl, RuntimeOptions, RuntimeQuery, RuntimeQueues, RuntimeTime, RuntimeWait, SerialRuntime,
        TaskConfig, TaskDispatch, TimedTaskDispatch,
    };
}
/// 单线程串行运行时（serial runtime）；外部并发提交不受本地接纳暂停限制。
pub mod serial_runtime {
    pub use crate::{Handle, RuntimeOptions, RuntimeQueues, SerialRuntime};
}
/// 单线程并发运行时（concurrent runtime）；已有任务Pending时仍尝试接纳下一普通批次。
pub mod concurrent_runtime {
    pub use crate::{ConcurrentRuntime, Handle, RuntimeOptions, RuntimeQueues};
}
