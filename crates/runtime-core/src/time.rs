//! 与具体定时器（timer）无关的计时登记与等待组合。
pub mod access;
pub mod composition;
pub mod entry;
pub mod registration;
pub mod task_notify;
pub mod timeout;
mod yielding;

pub use access::TimerAccess;
pub use composition::{Sleep, TimeoutOn};
pub use entry::TimerEntry;
pub use registration::{TimerRegistration, TimerRegistrationKind};
use std::{future::Future, time::Duration};
pub use task_notify::TimedTaskNotifier;
pub use timeout::{resume_timer_cancel, Timeout, TimeoutControl};
pub use yielding::yield_now;

/// 原服务拥有的计时能力；构造等待值不隐式推进服务。
pub trait RuntimeTime {
    type Timer: TimerAccess;
    /// 准备惰性计时等待；正时长首次poll才取时登记，至少1ms；零时长让出一次且不可取消。
    fn timeout(&self, duration: Duration) -> Timeout<Self::Timer>;
    /// 按业务优先组合Future与计时；不要求额外Unpin／Send／Sync／'static，完成时撤销另一等待兴趣。
    fn timeout_on<F: Future>(&self, future: F, duration: Duration) -> TimeoutOn<Self::Timer, F>;
    /// 准备短时等待；正时长范围1ms至1s，不返回独立控制句柄（handle），首次poll才开始。
    fn sleep(&self, duration: Duration) -> Sleep<Self::Timer>;
}
