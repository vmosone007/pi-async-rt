//! 计时核心与具体来源的默认连接；不改变运行时（runtime）公开计时语义。
mod access;
mod local;
pub mod protocol;
mod retry;
pub use access::{DefaultTimerAccess, DefaultTimerItem, DefaultTimerRegistration};
pub use local::{DefaultTimerDriver, LocalTimerHandle};
pub use protocol::{ExecutorTimer, LocalTimerSubmit};
