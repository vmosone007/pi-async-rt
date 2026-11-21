//! 编译对照（compile control）：共享通知句柄（shared notification handle）支持 Clone、Send、Sync。
use pi_async_executor::ThreadNotifier;
fn main() {
    fn require_shared<T: Clone + Send + Sync>() {}
    require_shared::<ThreadNotifier>();
}
