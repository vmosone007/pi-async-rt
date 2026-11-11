//! 固定目标的任务唤醒（wake-up）接入，不执行任务（Task）或事件循环（event loop）。
//!
//! WakeTarget把本线程唤醒交给默认TaskQueue的尾部，把跨线程唤醒交给外部队列接口。
//! 恢复接口必须接管未成功投递的原任务；本库不替外部选择重试或关闭策略。
//!
//! 提供任务唤醒（wake-up）的交付与恢复接口（API），实际推进由执行器（executor）负责。
#![doc(html_no_source)]

pub mod recovery;
pub mod target;
pub use recovery::{DeliveryError, DispatchFailure, DispatchRecovery};
pub use target::{BindError, ImportOutcome, LocalAccessError, LocalQueueGuard, WakeTarget};
