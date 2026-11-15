//! 拥有者服务推进与共享句柄（shared handle）的根专用推进。
use crate::{
    access::{self, BindingExitGuard},
    instance::*,
    lifecycle::RuntimeBinding,
    mode::RuntimeMode,
};
use pi_async_executor::{DriveLease, ExecutorClock, ExecutorHooks, IdlePolicy, TurnResult};
use pi_async_runtime_core::{
    block_on::{BlockOnDriver, RootWakeState},
    RuntimeBlockOn, RuntimeQuery, RuntimeStatus,
};
use pi_clock_manual::global::GlobalManualInstant;
use std::future::Future;

struct RuntimeDriver<'a, M, C, P, H>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    data: &'a mut RuntimeData<M, C, P, H>,
    lease: DriveLease,
    last: Option<TurnResult>,
    binding: &'a RuntimeBinding,
    handle: &'a RuntimeHandle<C>,
}
impl<M, C, P, H> ConfiguredRuntime<M, C, P, H>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    /// 原线程一次有限事件循环（event loop），返回后保留本地权限；不自行停放线程。
    /// 其它线程可继续通过Handle提交；同步控制hook和原F仍可能阻塞本次调用。
    pub fn run_once(&mut self) -> pi_result::Result<TurnResult> {
        runtime_once_inner(self)
    }
    /// 原线程持续推动同服务，直到关闭或首个推进错误；没有隐式工作线程。
    pub fn run(&mut self) -> pi_result::Result<()> {
        runtime_run_inner(self)
    }
}
impl<C: ExecutorClock<Instant = GlobalManualInstant>> RuntimeBlockOn for RuntimeHandle<C> {
    /// 只阻塞调用线程推进这份根Future；任务服务必须由拥有者另行推动。
    /// 无Unpin／Send／Sync／'static约束，不等根派发的其它任务全部结束。
    fn block_on<F: Future>(&self, future: F) -> F::Output {
        handle_block_on_inner(self, future)
    }
}
impl<M, C, P, H> RuntimeBlockOn for ConfiguredRuntime<M, C, P, H>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    /// 原线程独占服务并推进根，根完成即返回；没有&mut self或F额外约束。
    /// 非法嵌套／不同当前目标在首次poll前panic；关闭后只推进独立根，不重开服务。
    /// 服务推进失败以原pi_result::Error为panic载荷，保留报告链；根自身panic原样传播。
    fn block_on<F: Future>(&self, future: F) -> F::Output {
        runtime_block_on_inner(self, future)
    }
}
impl<M, C, P, H> BlockOnDriver for RuntimeDriver<'_, M, C, P, H>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    fn turn(&mut self) -> bool {
        runtime_driver_turn_inner(self)
    }
    fn park(&mut self, root: &RootWakeState) {
        runtime_driver_park_inner(self, root)
    }
}
include!("driving_inner.rs");
