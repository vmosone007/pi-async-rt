//! 编译拒绝（compile rejection）：线程停放拥有者（thread parker owner）不可跨线程转移（Send）。
use pi_async_executor::ThreadParker;
fn main() {
    fn require_send<T: Send>() {}
    require_send::<ThreadParker>();
}
