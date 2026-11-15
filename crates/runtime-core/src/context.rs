//! 当前公共访问、实际本地权限及活动推进分别维护。
mod access;
pub mod record;
pub use access::{Runtime, RuntimeContext};
pub use record::{bind_current, try_with_current_local, LocalBinding};
