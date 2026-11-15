//! 根异步计算（root Future）的同步推进；不将根包装成任务（Task）。
use crate::context::record::DriveScope;
use std::{
    future::Future,
    pin::Pin,
    sync::{atomic::AtomicBool, Arc},
    task::{Context, Poll, Wake, Waker},
    thread::Thread,
};

pub trait RuntimeBlockOn {
    /// 完整拥有者推进原服务；普通共享句柄只推进本根值。
    /// 非法嵌套／目标或推进权限冲突在首次poll前panic；根panic继续传播。
    fn block_on<F: Future>(&self, future: F) -> F::Output;
}

/// 每次同步根推进的独立通知状态；不保有根Future或完整服务。
pub struct RootWakeState {
    thread: Thread,
    notified: AtomicBool,
}
impl RootWakeState {
    pub fn is_notified(&self) -> bool {
        root_notified_inner(self)
    }
    /// 根专用停放；通知permit与检查防丢唤醒，不能改为忙等。
    pub fn park(&self) {
        root_park_inner(self)
    }
}
impl Wake for RootWakeState {
    fn wake(self: Arc<Self>) {
        root_wake_inner(&self)
    }
    fn wake_by_ref(self: &Arc<Self>) {
        root_wake_inner(self)
    }
}

/// 实现者提供已经独占取得的原服务推进；不转移已绑定任务（Task）线程。
pub trait BlockOnDriver {
    /// 推进一步服务，返回是否还有应立即处理的工作；只在本调用活动范围使用。
    fn turn(&mut self) -> bool;
    /// 停放前重查根通知与服务工作，必要截止时间限制由服务实现提供。
    fn park(&mut self, root: &RootWakeState);
}
/// 仅推进根值；调用者自行建立原公共环境，不隐式取得服务推进权。
pub fn block_on_root<F: Future>(future: F) -> F::Output {
    root_block_on_inner(future)
}
/// 用原服务的独占内部能力推进根值；无需给公开F添加任何额外约束。
pub fn block_on_with_driver<F: Future, D: BlockOnDriver>(future: F, driver: &mut D) -> F::Output {
    driver_block_on_inner(future, driver)
}
include!("block_on_inner.rs");
