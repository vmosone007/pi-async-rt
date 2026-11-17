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
    /// 构造短时等待，不取时或登记；零时长首次轮询（poll）可立即成功。
    /// 正时长须在1ms至1s，非法时长及准备错误在等待时返回；首次轮询才固定截止时间。
    /// 没有独立取消句柄（handle）；释放（drop）按内部计时等待规则安排撤销。
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
/// 每次轮询（poll）先推进业务，业务Ready优先返回结果，Pending后才准备并推进计时。
/// 正常到期返回TimeoutError::Elapsed，计时设施失败返回Timer；再次轮询不重置截止时间。
/// 业务完成时撤销计时兴趣；到期或计时失败返回前合法析构（drop）本包装持有的业务异步计算（Future）。
/// 若F只是后台任务（Task）的结果句柄，释放F沿原句柄约定，不等于取消后台任务。
pub struct TimeoutOn<A: TimerAccess, F: Future> {
    // F是结构性Pin字段；结束时只能原地析构，不得take移动已固定的F。
    future: Option<F>,
    access: Option<A>,
    duration: Duration,
    timeout: Option<Timeout<A>>,
    done: bool,
}
impl<A: TimerAccess, F: Future> TimeoutOn<A, F> {
    /// 只保存原业务异步计算（Future）、计时访问及时长，不取时、不准备或提交定时登记。
    /// 真正计时准备从首次业务轮询（poll）返回Pending后开始；业务先完成不产生计时登记。
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
