#![doc = include_str!("../README.md")]
#![doc(html_no_source)]

pub mod recovery;
pub mod target;
pub use recovery::{DeliveryError, DispatchFailure, DispatchRecovery};
pub use target::{BindError, ImportOutcome, LocalAccessError, LocalQueueGuard, WakeTarget};
