#![doc = include_str!("../README.md")]
#![doc(html_no_source)]
pub mod block_on;
pub mod context;
pub mod control;
pub mod definitions;
pub mod dispatch;
pub mod query;
pub mod task;
pub mod time;
pub mod wait;

pub use block_on::RuntimeBlockOn;
pub use context::{Runtime, RuntimeContext};
pub use control::RuntimeControl;
pub use definitions::*;
pub use dispatch::{IdleDispatch, LocalTaskDispatch, TaskDispatch, TimedTaskDispatch};
pub use pi_async_task::definitions::{
    BuildErrorKind, CancelRequestStatus, CancellationError, CancellationOutcome,
    CurrentTaskAccessError, ExecutionStatus, FutureDropStatus, TaskId, TaskOwnerId,
    TaskPhase, TaskSnapshot, TerminationKind,
};
#[cfg(feature = "observe")]
pub use pi_async_task::task::{TaskEvent, TaskObserver};
pub use pi_async_task::{LocalOutput, SendOutput, TaskError};
pub use pi_async_task_queue::{ExternalPushErrorKind, IdleQueueError, QueuePushErrorKind, TimerQueueError};
pub use pi_async_wait::{
    wait_all, wait_any, wait_one, AllSender, AnySender, OneSender, SendError, SendErrorKind, WaitAll, WaitAny, WaitError, WaitOne,
};
pub use query::RuntimeQuery;
pub use task::{
    CreateError, SpawnError, SubmitError, TaskCancelWait, TaskConfig, TaskControl, TaskFactory, TaskHandle, TaskNotifier, TaskQuery,
    UnsubmittedTask,
};
pub use time::{yield_now, RuntimeTime, Sleep, Timeout, TimeoutControl, TimeoutOn, TimerAccess, TimerCancelRequestOutcome};
pub use wait::{RuntimeWait, WaitAllTimeout, WaitAnyTimeout, WaitOneTimeout};
