//! 普通等待与计时能力组合；不要求具体执行器（executor）。
use crate::{
    definitions::{TimeoutError, WaitTimeoutError},
    time::{RuntimeTime, TimeoutOn, TimerAccess},
};
use pi_async_wait::{AllSender, AnySender, OneSender, WaitAll, WaitAny, WaitError, WaitOne};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

pub struct WaitOneTimeout<A: TimerAccess, T> {
    inner: TimeoutOn<A, WaitOne<T>>,
}
pub struct WaitAnyTimeout<A: TimerAccess, T> {
    inner: TimeoutOn<A, WaitAny<T>>,
}
pub struct WaitAllTimeout<A: TimerAccess, T> {
    inner: TimeoutOn<A, WaitAll<T>>,
}
impl<A: TimerAccess, T> Future for WaitOneTimeout<A, T> {
    type Output = pi_result::RawResult<T, WaitTimeoutError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        wait_one_timeout_poll_inner(self, cx)
    }
}
impl<A: TimerAccess, T> Future for WaitAnyTimeout<A, T> {
    type Output = pi_result::RawResult<T, WaitTimeoutError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        wait_any_timeout_poll_inner(self, cx)
    }
}
impl<A: TimerAccess, T> Future for WaitAllTimeout<A, T> {
    type Output = pi_result::RawResult<Vec<pi_result::RawResult<T, WaitError>>, TimeoutError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        wait_all_timeout_poll_inner(self, cx)
    }
}
/// 只依赖原等待与RuntimeTime；不增加T的Send／Sync／'static／Unpin约束。
pub trait RuntimeWait: RuntimeTime {
    /// 返回一次交值等待与原单发送者；通过timeout_on组合，不新增T的Send／Sync／'static／Unpin约束。
    fn wait_one_timeout<T>(
        &self,
        duration: Duration,
    ) -> (WaitOneTimeout<Self::Timer, T>, OneSender<T>) {
        let (wait, sender) = pi_async_wait::wait_one();
        (
            WaitOneTimeout {
                inner: self.timeout_on(wait, duration),
            },
            sender,
        )
    }
    /// 返回并发竞争一次交值的等待与可克隆发送者；保留原胜者及关闭错误语义。
    fn wait_any_timeout<T>(
        &self,
        duration: Duration,
    ) -> (WaitAnyTimeout<Self::Timer, T>, AnySender<T>) {
        let (wait, sender) = pi_async_wait::wait_any();
        (
            WaitAnyTimeout {
                inner: self.timeout_on(wait, duration),
            },
            sender,
        )
    }
    /// 返回全部交值等待与原登记发送者；正常返回完整有序向量，超时不交部分结果。
    fn wait_all_timeout<T>(
        &self,
        duration: Duration,
    ) -> (WaitAllTimeout<Self::Timer, T>, AllSender<T>) {
        let (wait, sender) = pi_async_wait::wait_all();
        (
            WaitAllTimeout {
                inner: self.timeout_on(wait, duration),
            },
            sender,
        )
    }
}
include!("wait_inner.rs");
