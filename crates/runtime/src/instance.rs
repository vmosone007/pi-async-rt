//! 两种运行时（runtime）的共同拥有者及透明共享句柄（shared handle）。
use crate::{
    binding::RuntimeTag,
    lifecycle::{RuntimeBinding, RuntimeExternalSource},
    mode::{ConcurrentMode, RuntimeMode, SerialMode},
    options::RuntimeOptions,
    queues::RuntimeQueues,
};
use pi_async_executor::{
    timer::{DefaultTimerAccess, DefaultTimerDriver},
    DefaultExecutor, DefaultIdlePolicy, ExecutorClock, ExecutorHandle, ExecutorHooks,
    ExecutorLocal, ExecutorTimer, IdlePolicy, ManualClock, NoHooks, ThreadNotifier,
};
use pi_async_task_queue::{
    CancelTimerOptions, ExternalQueueManager, ExternalTaskQueue, IdleQueueManager, IdleTaskQueue,
    MpscIdleTaskQueue, MpscIdleTaskQueueHandle, MpscTaskQueue, MpscTaskQueueHandle,
};
use pi_async_runtime_core::RuntimeId;
use pi_clock_manual::global::GlobalManualInstant;
use std::{cell::RefCell, rc::Rc, sync::Arc};

pub(crate) type Timer<C> = DefaultTimerDriver<C, RuntimeTag<C>>;
pub(crate) type Executor<C, E, I, T> = DefaultExecutor<C, RuntimeExternalSource<E>, I, T, RuntimeTag<C, T>>;
pub(crate) type Shared<C, E, I, T> = ExecutorHandle<E, I, T, RuntimeTag<C, T>>;
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
/// 释放（drop）先撤本地权限，再按原执行器（executor）及任务域约定实际退出。
/// 正在进行的同步活动可延迟实际本地清理，不等待普通共享句柄（shared handle）全部释放。
/// 关闭请求、服务不可用与成功完成分别观察，清理失败不能报CloseState::Completed。
/// waiting_for_wake_count及pending_cancel_task_count沿默认执行器按需O(n)扫描登记，n为登记规模；
/// 封装不把这两项查询转为固定成本，默认推进不主动采集。
pub struct ConfiguredRuntime<M, C, P, H, E = MpscTaskQueue, I = MpscIdleTaskQueue, T = Timer<C>>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    pub(crate) shared: RuntimeHandle<C, E::Handle, I::Handle, T>,
    pub(crate) data: RefCell<Option<RuntimeData<M, C, P, H, E, I, T>>>,
    pub(crate) binding: Rc<RuntimeBinding>,
}
pub(crate) struct RuntimeData<M, C, P, H, E, I, T>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    pub(crate) executor: Executor<C, E, I, T>,
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
/// 两项等待唤醒／待取消查询与ConfiguredRuntime相同，按需O(n)扫描登记，其它操作沿对应能力约定。
/// 释放（drop）本句柄不关闭服务，不延迟本地原异步计算（Future）的合法清理。
pub struct RuntimeHandle<C, E = MpscTaskQueueHandle, I = MpscIdleTaskQueueHandle, T = Timer<C>>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalTaskQueue,
    I: IdleTaskQueue,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
{
    pub(crate) inner: Arc<Shared<C, E, I, T>>,
}
/// 完整默认串行服务；静态本地入口无需填写内部参数。
pub type SerialRuntime = ConfiguredRuntime<SerialMode, ManualClock, DefaultIdlePolicy, NoHooks>;
/// 完整默认并发服务；与Runtime同一默认类型。
pub type ConcurrentRuntime =
    ConfiguredRuntime<ConcurrentMode, ManualClock, DefaultIdlePolicy, NoHooks>;

impl<M: RuntimeMode> ConfiguredRuntime<M, ManualClock, DefaultIdlePolicy, NoHooks> {
    /// 默认配置及策略构造；外部必须已安装pi_clock全局时间轴，失败保原报告（Report）。
    pub fn new() -> pi_result::Result<Self> {
        runtime_new_inner(RuntimeOptions::default(), RuntimeQueues::default())
    }
    /// 采用指定工作额度；不改变创建线程归属，不安装全局时钟或启动线程。
    pub fn with_options(options: RuntimeOptions) -> pi_result::Result<Self> {
        runtime_new_inner(options, RuntimeQueues::default())
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
        runtime_components_inner(clock, policy, hooks, options, RuntimeQueues::default())
    }

    /// 当前线程构造，分别使用指定的队列（queue）和各缺项的默认来源。
    /// 时钟（clock）、空闲策略及钩子（hook）沿原默认，失败保留原报告（Report）。
    ///
    /// 按值接收选配。构造失败时，原线程释放已移交的队列管理者和未调用的回调（callback）；
    /// 返回的报告（Report）不含这些值，也不会交出尚未完成的运行时（runtime）。
    pub fn with_queues<E, I, F, T>(
        options: RuntimeOptions,
        queues: RuntimeQueues<ManualClock, E, I, F>,
    ) -> pi_result::Result<ConfiguredRuntime<M, ManualClock, DefaultIdlePolicy, NoHooks, E, I, T>>
    where
        E: ExternalQueueManager,
        I: IdleQueueManager,
        T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
        F: FnOnce(RuntimeId, &ManualClock, ThreadNotifier, CancelTimerOptions) -> pi_result::Result<T>,
    {
        runtime_new_inner(options, queues)
    }

    /// 同时选择同源时钟（clock）、策略、钩子（hook）及具象队列（queue）。
    /// 管理者随运行时（runtime）接管，计时构造回调使用本次的线程通知句柄（ThreadNotifier）。
    ///
    /// 按值接收选配。选项校验或任一来源准备失败时，原线程释放已接管的资源和
    /// 未调用的回调（callback）。直接返回原报告（Report），调用方不会取得半成品。
    pub fn with_components_and_queues<C, P, H, E, I, F, T>(
        clock: C,
        policy: P,
        hooks: H,
        options: RuntimeOptions,
        queues: RuntimeQueues<C, E, I, F>,
    ) -> pi_result::Result<ConfiguredRuntime<M, C, P, H, E, I, T>>
    where
        C: ExecutorClock<Instant = GlobalManualInstant>,
        P: IdlePolicy,
        H: ExecutorHooks,
        E: ExternalQueueManager,
        I: IdleQueueManager,
        T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
        F: FnOnce(RuntimeId, &C, ThreadNotifier, CancelTimerOptions) -> pi_result::Result<T>,
    {
        runtime_components_inner(clock, policy, hooks, options, queues)
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
        runtime_components_inner(clock, policy, hooks, options, RuntimeQueues::default())
    }
}
impl<M, C, P, H, E, I, T> ConfiguredRuntime<M, C, P, H, E, I, T>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    /// 取得同一服务的共享访问；不移交服务推进权，也不延长本地Future的清理。
    pub fn handle(&self) -> RuntimeHandle<C, E::Handle, I::Handle, T> {
        self.shared.clone()
    }
    pub(crate) fn shared(&self) -> &RuntimeHandle<C, E::Handle, I::Handle, T> {
        &self.shared
    }
}
impl<C, E, I, T> RuntimeHandle<C, E, I, T>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalTaskQueue,
    I: IdleTaskQueue,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
{
    pub(crate) fn shared(&self) -> &Self {
        self
    }
}
impl<C, E, I, T> Clone for RuntimeHandle<C, E, I, T>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalTaskQueue,
    I: IdleTaskQueue,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
{
    fn clone(&self) -> Self {
        runtime_handle_clone_inner(self)
    }
}
impl<M, C, P, H, E, I, T> Drop for ConfiguredRuntime<M, C, P, H, E, I, T>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    fn drop(&mut self) {
        runtime_drop_inner(self)
    }
}
include!("instance_inner.rs");
