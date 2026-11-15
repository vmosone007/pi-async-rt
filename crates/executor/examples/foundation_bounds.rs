//! 只编译声明约束，不执行尚未实现的停放或取时。
use pi_async_executor::{ExecutorClock, ManualClock, ThreadNotifier};
use pi_clock_manual::global::GlobalManualInstant;

fn main() {
    fn send_sync_clone<T: Send + Sync + Clone>() {}
    fn clock<C: ExecutorClock<Instant = GlobalManualInstant>>() {}
    send_sync_clone::<ThreadNotifier>();
    send_sync_clone::<ManualClock>();
    clock::<ManualClock>();
}
