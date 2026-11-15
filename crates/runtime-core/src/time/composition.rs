use super::{access::TimerAccess, timeout::Timeout};
use crate::definitions::{TimeoutError, TimerError};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

/// 短时主动等待；正时长1ms至1s，零时长可立即完成，不暴露取消句柄（handle）。
#[must_use = "须await或poll才开始等待"]
pub struct Sleep<A: TimerAccess> {
    timeout: Option<Timeout<A>>,
    error: Option<TimerError>,
    done: bool,
}
impl<A: TimerAccess> Sleep<A> {
    pub fn new(access: A, duration: Duration) -> Self {
        sleep_new_inner(access, duration)
    }
}
impl<A: TimerAccess> Unpin for Sleep<A> {}
impl<A: TimerAccess> Future for Sleep<A> {
    type Output = pi_result::RawResult<(), TimerError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        sleep_poll_inner(self, cx)
    }
}

/// 按业务优先组合；F不要求Unpin／Send／Sync／'static，不强迫额外装箱。
#[must_use = "须await或poll才推进业务及计时"]
pub struct TimeoutOn<A: TimerAccess, F: Future> {
    // F是结构性Pin字段；结束时只能原地析构，不得take移动已固定的F。
    future: Option<F>,
    access: Option<A>,
    duration: Duration,
    timeout: Option<Timeout<A>>,
    done: bool,
}
impl<A: TimerAccess, F: Future> TimeoutOn<A, F> {
    pub fn new(access: A, future: F, duration: Duration) -> Self {
        timeout_on_new_inner(access, future, duration)
    }
}
impl<A: TimerAccess, F: Future> Future for TimeoutOn<A, F> {
    type Output = pi_result::RawResult<F::Output, TimeoutError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        timeout_on_poll_inner(self, cx)
    }
}
include!("composition_inner.rs");
