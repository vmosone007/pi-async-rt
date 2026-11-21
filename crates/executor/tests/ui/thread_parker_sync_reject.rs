//! 编译拒绝（compile rejection）：线程停放拥有者（thread parker owner）不可跨线程共享（Sync）。
use pi_async_executor::ThreadParker;
fn main() {
    fn require_sync<T: Sync>() {}
    require_sync::<ThreadParker>();
}
