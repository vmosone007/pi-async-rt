//! 由外部当前线程推动的两种单线程运行时（runtime）。
//!
//! [`SerialRuntime`]等本地已接纳任务结束后才取下一普通外部批次；[`ConcurrentRuntime`]
//! 每轮都积极尝试普通来源。两者均允许外部持续并发提交，支持本地非Send任务及计时。
//! 并发（concurrency）指同一线程交替推进，不创建工作线程，不提供并行CPU或异步I/O驱动。
//! 串行任务需等待本地子任务时应使用spawn_local；把子任务交普通外部来源再等待，可能互相等待。
//! 共享Handle的block_on只推进根计算，服务由拥有者另行推进；完整运行时的block_on同时推进服务。
//! 本版本提供完整骨架（skeleton），业务算法仍有todo!，编译成功不表示可以执行。
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

pub use instance::{
    ConcurrentRuntime, ConfiguredRuntime, Handle, LocalTaskHandle, Runtime, RuntimeHandle,
    RuntimeTimerAccess, SerialRuntime, TaskHandle, TimedTaskHandle, TimedTaskNotifier,
};
pub use mode::{ConcurrentMode, SerialMode};
pub use options::RuntimeOptions;
pub use pi_async_executor::{
    DefaultIdlePolicy, ExecutorClock, ExecutorEvent, ExecutorHooks, ExecutorOptions, IdlePolicy,
    ManualClock, NoHooks, TaskAction, TaskInfo, TimerDelivery, TurnResult,
};
pub use pi_async_runtime_core::task::{CreateError, SpawnError, SubmitError};
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
        RuntimeControl, RuntimeOptions, RuntimeQuery, RuntimeTime, RuntimeWait, SerialRuntime,
        TaskConfig, TaskDispatch, TimedTaskDispatch,
    };
}
/// 单线程串行运行时（serial runtime）；外部并发提交不受本地接纳暂停限制。
pub mod serial_runtime {
    pub use crate::{Handle, RuntimeOptions, SerialRuntime};
}
/// 单线程并发运行时（concurrent runtime）；已有任务Pending时仍尝试接纳下一普通批次。
pub mod concurrent_runtime {
    pub use crate::{ConcurrentRuntime, Handle, RuntimeOptions};
}
