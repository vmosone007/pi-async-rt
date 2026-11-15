//! 两种运行时（runtime）的共同拥有者及透明共享句柄（shared handle）。
use crate::{
    binding::RuntimeTag,
    lifecycle::{RuntimeBinding, RuntimeExternalSource},
    mode::{ConcurrentMode, RuntimeMode, SerialMode},
    options::RuntimeOptions,
};
use pi_async_executor::{
    timer::{DefaultTimerAccess, DefaultTimerDriver},
    DefaultExecutor, DefaultIdlePolicy, ExecutorClock, ExecutorHandle, ExecutorHooks,
    ExecutorLocal, IdlePolicy, ManualClock, NoHooks, ThreadNotifier,
};
use pi_async_task_queue::{
    MpscIdleTaskQueue, MpscIdleTaskQueueHandle, MpscTaskQueue, MpscTaskQueueHandle,
};
use pi_clock_manual::global::GlobalManualInstant;
use std::{cell::RefCell, rc::Rc, sync::Arc};

pub(crate) type Timer<C> = DefaultTimerDriver<C, RuntimeTag<C>>;
pub(crate) type Executor<C> =
    DefaultExecutor<C, RuntimeExternalSource, MpscIdleTaskQueue, Timer<C>, RuntimeTag<C>>;
pub(crate) type Shared<C> =
    ExecutorHandle<MpscTaskQueueHandle, MpscIdleTaskQueueHandle, Timer<C>, RuntimeTag<C>>;
/// 常规业务只通过RuntimeTime使用，无需拼写内部计时器（timer）参数。
pub type RuntimeTimerAccess<C = ManualClock> = DefaultTimerAccess<C, RuntimeTag<C>>;
pub type TaskHandle<T, V = ()> = pi_async_runtime_core::task::TaskHandle<ThreadNotifier, T, V>;
pub type LocalTaskHandle<T, V = ()> =
    pi_async_runtime_core::task::TaskHandle<ThreadNotifier, T, V, pi_async_task::LocalOutput>;
pub type TimedTaskHandle<T, V = (), C = ManualClock> = pi_async_runtime_core::task::TaskHandle<
    pi_async_runtime_core::time::TimedTaskNotifier<ThreadNotifier, RuntimeTimerAccess<C>>,
    T,
    V,
>;
pub type TimedTaskNotifier<C = ManualClock> =
    pi_async_runtime_core::time::TimedTaskNotifier<ThreadNotifier, RuntimeTimerAccess<C>>;

/// 一份固定线程服务，不可Send／Sync；不创建或接管外部线程的所有权。
/// 常规使用SerialRuntime／ConcurrentRuntime，无需填写内部参数；定制类型由with_components推导。
pub struct ConfiguredRuntime<M, C, P, H>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    pub(crate) shared: RuntimeHandle<C>,
    pub(crate) data: RefCell<Option<RuntimeData<M, C, P, H>>>,
    pub(crate) binding: Rc<RuntimeBinding>,
}
pub(crate) struct RuntimeData<M, C, P, H>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    pub(crate) executor: Executor<C>,
    pub(crate) mode: M,
    pub(crate) policy: P,
    pub(crate) hooks: H,
}
/// 两种方式共用的Clone＋Send＋Sync业务访问；一次Clone只复制同一Arc，不保本地管理者。
/// 可以从外线程提交／取消／查询；其block_on只推进调用者根，不代推原服务。
pub type Handle = RuntimeHandle<ManualClock>;
/// 默认并发服务，与ConcurrentRuntime为同一具体静态类型（type）。
pub type Runtime = ConfiguredRuntime<ConcurrentMode, ManualClock, DefaultIdlePolicy, NoHooks>;

/// 定制时钟的共享访问；通常由构造结果推导，默认用户使用不带参数的Handle。
pub struct RuntimeHandle<C>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
{
    pub(crate) inner: Arc<Shared<C>>,
}
/// 完整默认串行服务；静态本地入口无需填写内部参数。
pub type SerialRuntime = ConfiguredRuntime<SerialMode, ManualClock, DefaultIdlePolicy, NoHooks>;
/// 完整默认并发服务；与Runtime同一默认类型。
pub type ConcurrentRuntime =
    ConfiguredRuntime<ConcurrentMode, ManualClock, DefaultIdlePolicy, NoHooks>;

impl<M: RuntimeMode> ConfiguredRuntime<M, ManualClock, DefaultIdlePolicy, NoHooks> {
    /// 默认配置及策略构造；外部必须已安装pi_clock全局时间轴，失败保原报告（Report）。
    pub fn new() -> pi_result::Result<Self> {
        runtime_new_inner(RuntimeOptions::default())
    }
    /// 采用指定工作额度；不改变创建线程归属，不安装全局时钟或启动线程。
    pub fn with_options(options: RuntimeOptions) -> pi_result::Result<Self> {
        runtime_new_inner(options)
    }
    /// 由参数推导定制类型，不把内部队列泛型传播到调用点。
    pub fn with_components<C, P, H>(
        clock: C,
        policy: P,
        hooks: H,
        options: RuntimeOptions,
    ) -> pi_result::Result<ConfiguredRuntime<M, C, P, H>>
    where
        C: ExecutorClock<Instant = GlobalManualInstant>,
        P: IdlePolicy,
        H: ExecutorHooks,
    {
        runtime_components_inner(clock, policy, hooks, options)
    }
}
impl<M, C, P, H> ConfiguredRuntime<M, C, P, H>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    /// 静态替换同源时钟、空闲／休眠策略及钩子；无额外Send／Sync／Default约束强加给P/H。
    /// 模式M固定普通新业务接纳；策略不能把串行改成并发。
    pub fn from_components(
        clock: C,
        policy: P,
        hooks: H,
        options: RuntimeOptions,
    ) -> pi_result::Result<Self> {
        runtime_components_inner(clock, policy, hooks, options)
    }
    /// 取得同一服务的共享访问；不移交服务推进权，也不延长本地Future的清理。
    pub fn handle(&self) -> RuntimeHandle<C> {
        self.shared.clone()
    }
    pub(crate) fn shared(&self) -> &RuntimeHandle<C> {
        &self.shared
    }
}
impl<C: ExecutorClock<Instant = GlobalManualInstant>> RuntimeHandle<C> {
    pub(crate) fn shared(&self) -> &Self {
        self
    }
}
impl<C: ExecutorClock<Instant = GlobalManualInstant>> Clone for RuntimeHandle<C> {
    fn clone(&self) -> Self {
        runtime_handle_clone_inner(self)
    }
}
impl<M, C, P, H> Drop for ConfiguredRuntime<M, C, P, H>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    fn drop(&mut self) {
        runtime_drop_inner(self)
    }
}
include!("instance_inner.rs");
