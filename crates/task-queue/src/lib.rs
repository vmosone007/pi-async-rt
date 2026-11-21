#![doc = include_str!("../README.md")]
#![doc(html_no_source)]

/// 外部队列（queue）接口及一次获取的有序批次。
pub mod external;
/// 空闲任务队列（idle task queue）的共享访问及独立管理声明。
pub mod idle;
/// 默认本地TaskQueue及受限侵入式适配。
pub mod local;
/// 清理／关闭的有限无值进度。
pub mod management;
/// 默认侵入式多生产者单消费者队列（MPSC queue）与受限共享句柄（shared handle）。
pub mod mpsc;
/// 默认空闲来源的侵入式MPSC实现；取消清理不受空闲策略限制。
pub mod mpsc_idle;
/// 定时任务队列（timer task queue）的本地拥有者及共享入口声明。
pub mod timer;
pub mod notify;
#[cfg(feature = "cancel-timer")]
pub mod cancel_timer;

pub use external::{
    BatchAttempt, ExternalPushError, ExternalPushErrorKind, ExternalQueueManager, ExternalTaskQueue,
    TaskBatch,
};
pub use idle::{
    IdleQueueError, IdleQueueManager, IdleQueueStatus, IdleSubmitError, IdleTake, IdleTaskQueue,
};
pub use local::{BatchPushError, QueuePushError, QueuePushErrorKind, TaskQueue};
pub use management::QueueCleanupProgress;
pub use notify::QueueNotifier;
#[cfg(feature = "cancel-timer")]
pub use cancel_timer::{CancelTimerKey, CancelTimerOptions, CancelTimerQueue, CancelTimerQueueHandle};
pub use mpsc::{MpscQueueOptions, MpscTake, MpscTaskQueue, MpscTaskQueueHandle};
pub use mpsc_idle::{MpscIdleTaskQueue, MpscIdleTaskQueueHandle};
pub use timer::{
    TimerAdvanceProgress, TimerCancelRequestError, TimerQueueError,
    TimerQueueHandle, TimerQueueStatus, TimerSubmitError, TimerTake, TimerTaskQueue,
};
