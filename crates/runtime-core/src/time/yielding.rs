use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
/// 主动让出一次；须实际await才开始，不保证另一个任务（Task）一定先执行。
pub async fn yield_now() {
    YieldOnce::new().await
}
pub(crate) struct YieldOnce {
    yielded: bool,
}
impl YieldOnce {
    pub(crate) fn new() -> Self {
        Self { yielded: false }
    }
}
impl Future for YieldOnce {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        yield_once_poll_inner(self, cx)
    }
}
include!("yielding_inner.rs");
