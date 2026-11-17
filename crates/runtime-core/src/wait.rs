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

/// 一次交值的带超时等待，正常返回原值，原等待错误保留为WaitTimeoutError::Wait。
/// 超时返回Elapsed，计时设施失败返回Timer；关闭后未取得接受许可的交值按原关闭规则失败并返还输入值。
/// 超时或释放（drop）不回退此前已接受的并发交值；发送成功不承诺等待方已取得该值。
pub struct WaitOneTimeout<A: TimerAccess, T> {
    inner: TimeoutOn<A, WaitOne<T>>,
}
/// 并发竞争一次交值的带超时等待，只接收原规则选定的一个值。
/// 原等待失败、超时、计时失败分别为Wait／Elapsed／Timer；其它发送者（senders）沿原竞争及关闭规则处理。
/// 超时或释放（drop）不撤销此前已接受的交值；未取得接受许可的后续交值保留原值和实际拒绝原因。
pub struct WaitAnyTimeout<A: TimerAccess, T> {
    inner: TimeoutOn<A, WaitAny<T>>,
}
/// 全部交值的带超时等待，正常返回按登记顺序排列的完整逐项结果向量（vector）。
/// 发送者（sender）释放造成的原WaitError保留在对应项；超时或计时失败返回TimeoutError，不返回部分向量。
/// 超时或释放（drop）结束内部等待；外部句柄可安全释放，未取得接受许可的后续交值返还值和关闭原因。
/// 此前已接受的并发交值不因关闭回退，仍由原等待机制承担保存与必要清理责任。
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
