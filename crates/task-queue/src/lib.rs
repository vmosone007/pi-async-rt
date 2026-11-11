//! 本地侵入式双端队列（intrusive deque）及独立外部接入协议。
//! 提供本地队列（queue）与外部接入接口（API）；不包含执行器（executor）。
#![doc(html_no_source)]

/// 外部队列（queue）接口及一次获取的有序批次。
pub mod external;
/// 默认本地TaskQueue及受限侵入式适配。
pub mod local;
/// 已有pi_task_pool的默认外部适配。
#[cfg(feature = "pool")]
pub mod pool;

pub use external::{
    BatchAttempt, ExternalPushError, ExternalPushErrorKind, ExternalTaskQueue, TaskBatch,
};
pub use local::{BatchPushError, QueuePushError, QueuePushErrorKind, TaskQueue};
#[cfg(feature = "pool")]
pub use pool::TaskPoolQueue;
