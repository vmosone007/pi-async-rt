//! 运行时核心（runtime core）的标准能力及组合。
//!
//! 本库定义原服务的派发（dispatch）、计时、查询、控制、当前访问与根推进。
//! 不包含具体执行器（executor）、时钟（clock）、定时轮（timing wheel）或任务池（task pool）。
//! 本版本包含未实现占位（unimplemented stub），可编译不表示可执行。

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
pub use pi_async_task::definitions::{TaskId, TaskOwnerId};
pub use pi_async_task::{LocalOutput, SendOutput};
pub use pi_async_wait::{
    wait_all, wait_any, wait_one, AllSender, AnySender, OneSender, WaitAll, WaitAny, WaitOne,
};
pub use query::RuntimeQuery;
pub use task::{
    TaskCancelWait, TaskConfig, TaskControl, TaskFactory, TaskHandle, TaskNotifier, TaskQuery,
    UnsubmittedTask,
};
pub use time::{yield_now, RuntimeTime, Sleep, Timeout, TimeoutControl, TimeoutOn};
pub use wait::{RuntimeWait, WaitAllTimeout, WaitAnyTimeout, WaitOneTimeout};
