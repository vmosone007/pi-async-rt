//! 异步任务（Task）的基础类型（type）。
//!
//! 当前提供 [`definitions::TaskPhase`]、[`definitions::CancelRequestStatus`]
//! 和 [`definitions::FutureDropStatus`]，分别描述生命周期（lifecycle）阶段、
//! 取消请求（cancellation request）是否已记录，以及原异步计算（Future）的
//! 合法析构（drop）是否已确认完成。
//! 这些描述值不持有异步计算（Future），也不提供执行、取消（cancellation）或排队能力。
//! 单独构造描述值不能证明某个实际任务（Task）已经到达相应情况。

//!
//! 新增创建、执行持有、共享控制、结果等待及唤醒器（Waker）的声明。
//! 上述纯描述值可独立使用；实际任务（Task）操作由以下模块（module）提供。
//! 队列（queue）与具体唤醒投递分别位于独立库，不在此库实现执行器（executor）。
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
pub use join::{JoinHandle, LocalOutput, SendOutput, TaskError};
pub use runnable::{create, create_local, BuildError, RunError, Runnable, TaskParts};
pub use task::{
    capture_current_panic_backtrace, QueueEntry, Schedule, TaskOptions, TaskRegistration,
    TaskThread,
};
