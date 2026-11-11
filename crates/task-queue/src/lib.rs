//! 本地侵入式双端队列（intrusive deque）及独立外部接入协议。
//! 提供本地队列（queue）与外部接入接口（API）；不包含执行器（executor）。
#![doc(html_no_source)]

/// 外部队列（queue）接口及一次获取的有序批次。
pub mod external;
/// 空闲任务队列（idle task queue）的共享访问及独立管理声明。
pub mod idle;
/// 默认本地TaskQueue及受限侵入式适配。
pub mod local;
/// 清理／关闭的有限进度与交付数据。
pub mod management;
/// 定时任务队列（timer task queue）的本地拥有者及共享入口声明。
pub mod timer;
/// 已有pi_task_pool的默认外部适配。
#[cfg(feature = "pool")]
pub mod pool;

pub use external::{
    BatchAttempt, ExternalPushError, ExternalPushErrorKind, ExternalQueueManager, ExternalTaskQueue,
    TaskBatch,
};
pub use idle::{
    IdleQueueError, IdleQueueManager, IdleQueueStatus, IdleSubmitError, IdleTake, IdleTaskQueue,
};
pub use local::{BatchPushError, QueuePushError, QueuePushErrorKind, TaskQueue};
pub use management::QueueCleanupProgress;
pub use timer::{
    TimerAdvanceProgress, TimerCancelRequestError, TimerCleanup, TimerCleanupReason, TimerQueueError,
    TimerQueueHandle, TimerQueueStatus, TimerSubmitError, TimerTake, TimerTaskQueue,
};
#[cfg(feature = "pool")]
pub use pool::TaskPoolQueue;
