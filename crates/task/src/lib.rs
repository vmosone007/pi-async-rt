#![doc = include_str!("../README.md")]
#![doc(html_no_source)]

pub mod definitions;

/// 共享控制与取消等待（cancellation wait）。
pub mod control;
/// 唯一结果等待与领取。
pub mod join;
/// 创建与唯一可执行句柄（runnable handle）。
pub mod runnable;
/// 单个任务（Task）的存储、访问、清理及原生唤醒器（Waker）基础。
pub mod task;

pub use control::{CancelWait, ControlHandle, ExternalWaitSource, ExternalWaiter};
pub use definitions::CurrentTaskAccessError;
pub use join::{JoinHandle, LocalOutput, SendOutput, TaskError};
pub use runnable::{create, create_local, BuildError, RunError, Runnable, TaskParts};
pub use task::{
    capture_current_panic_backtrace, try_current_task_id, try_with_current_metadata, MpscEntryHandle,
    MpscQueueEntry, QueueEntry, Schedule, TaskOptions, TaskRegistration,
    TaskThread,
};
