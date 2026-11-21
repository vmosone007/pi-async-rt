//! 原生任务（Task）权限的受限组合，不重新实现生命周期（lifecycle）。
use crate::definitions::*;
use pi_async_task::control::{CancelWait, ExternalWaiter};
use pi_async_task::definitions::{
    CancellationOutcome, QueueSupport, TaskId, TaskOwnerId, TaskSnapshot,
};
use pi_async_task::join::{JoinHandle, LocalOutput, OutputMode, SendOutput};
use pi_async_task::{ControlHandle, Runnable, Schedule, TaskThread};
use pi_async_task_queue::notify::QueueNotifier;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

/// 原生取消事实之后安排必要检查；默认实现不求值快照闭包（closure）。
// @trace 1
pub trait TaskNotifier: QueueNotifier + Clone {
    /// 安排取消事实之后的必要检查；普通实现不求值snapshot，定时实现只依据原生Recorded事实。
    fn notify_cancel(&self, _snapshot: impl FnOnce() -> TaskSnapshot) {
        self.notify();
    }
}

/// 用户可选创建配置；调度（Schedule）和队列链接能力由原运行时决定。
// @trace 2
pub struct TaskConfig<M = ()> {
    /// 固定类型的附加数据；读写经其自身线程安全（thread-safe）接口。
    pub metadata: M,
    /// 是否允许捕获本任务恐慌（panic）起点信息，默认false。
    pub capture_panic_backtrace: bool,
    /// 编译启用observe时的可选接收者，默认None。
    #[cfg(feature = "observe")]
    pub observer: Option<std::sync::Arc<dyn pi_async_task::task::TaskObserver>>,
}
impl<M> TaskConfig<M> {
    pub fn new(metadata: M) -> Self {
        task_config_new_inner(metadata)
    }
}
impl<M: Default> Default for TaskConfig<M> {
    fn default() -> Self {
        Self::new(M::default())
    }
}

/// 原服务的一份唯一待提交持有；结果等待不会隐式提交。
/// 可跨线程转交，不支持Clone；本地首次提交仍限原所属线程（owner thread）。
#[must_use = "提交回原服务，或释放以按原任务机制放弃"]
// @trace 3
pub struct UnsubmittedTask {
    pub(crate) runnable: Runnable,
    pub(crate) runtime_id: RuntimeId,
    pub(crate) local_owner: Option<TaskOwnerId>,
}
impl UnsubmittedTask {
    /// 读取原服务或任务（Task）的固定身份；不创建新身份或权限。
    pub fn id(&self) -> TaskId {
        self.runnable.id()
    }
    /// 读取原任务（Task）的基本事实，多个原子量不保证全局同一瞬间；不代表物理队列位置。
    pub fn snapshot(&self) -> TaskSnapshot {
        self.runnable.snapshot()
    }
}

/// 查询与控制；普通释放不取消，不交出原Future或结果。
// @trace 4
pub struct TaskControl<N: TaskNotifier> {
    control: ControlHandle,
    notifier: N,
}
impl<N: TaskNotifier> TaskControl<N> {
    /// 取得只读查询持有；不授予取消、唤醒、结果领取或原值访问权限。
    /// 用于服务自身统计；保留此持有不延后原Future的合法清理。
    pub fn query_handle(&self) -> TaskQuery {
        TaskQuery {
            control: self.control.clone(),
        }
    }
    /// 读取原服务或任务（Task）的固定身份；不创建新身份或权限。
    pub fn id(&self) -> TaskId {
        self.control.id()
    }
    /// 读取原任务（Task）的基本事实，多个原子量不保证全局同一瞬间；不代表物理队列位置。
    pub fn snapshot(&self) -> TaskSnapshot {
        self.control.snapshot()
    }
    /// 提出原生取消并安排检查；不等待原Future清理。
    pub fn cancel(&self) {
        task_control_cancel_inner(self)
    }
    /// 通过原任务唤醒路径安排后续推进，不复活结束任务。
    pub fn wake(&self) {
        self.control.wake();
    }
    /// 取得同一原任务（Task）的标准唤醒器（Waker），可克隆并跨线程唤醒。
    /// 只安排原任务后续检查，不直接轮询（poll）或访问原异步计算（Future），不复活结束任务。
    pub fn waker(&self) -> Waker {
        self.control.waker()
    }
    /// 惰性等待；构造本身不提出取消请求。
    pub fn cancel_and_wait(&self) -> TaskCancelWait<N> {
        task_control_wait_inner(self, None)
    }
    /// 使用已有可信外部等待者（external waiter）身份构造惰性取消等待，构造本身不请求取消。
    /// 受管任务（Task）内仍以实际当前任务身份为准；首次挂起（Pending）后实际等待者身份不得改变。
    /// 首次轮询（poll）沿原生身份检查决定能否登记；拒绝不冒充已发送取消请求。
    /// 释放等待只放弃本份等待，不撤销此前已记录的取消。
    pub fn cancel_and_wait_from(&self, waiter: &ExternalWaiter) -> TaskCancelWait<N> {
        task_control_wait_inner(self, Some(waiter))
    }
}

/// 同一原任务（Task）的只读查询句柄（query handle），支持Clone＋Send＋Sync。
/// 不含通知或调度能力，不可据此更换任务归属或访问原异步计算（Future）。
pub struct TaskQuery {
    control: ControlHandle,
}
impl TaskQuery {
    /// O(1)读取原任务身份；编号从不复用。
    pub fn id(&self) -> TaskId {
        self.control.id()
    }
    /// O(1)读取同一原生状态字的快照（snapshot），不推进任务。
    pub fn snapshot(&self) -> TaskSnapshot {
        self.control.snapshot()
    }
    /// O(1)沿原生窄出口读取挂起交接完成且没有唤醒、取消或退出需求的事实。
    /// 读取后任务可立即变化，不据此承诺下一次执行次序。
    pub fn is_waiting_for_wake(&self) -> bool {
        self.control.is_waiting_for_wake()
    }
}
impl Clone for TaskQuery {
    fn clone(&self) -> Self {
        Self {
            control: self.control.clone(),
        }
    }
}
impl<N: TaskNotifier> Clone for TaskControl<N> {
    fn clone(&self) -> Self {
        task_control_clone_inner(self)
    }
}

/// 唯一可等待结果与共享控制组合；本地输出模式保持原线程限制。
#[must_use = "等待领取结果，或释放以放弃结果兴趣；释放不自动取消"]
// @trace 5
/// 作为异步计算（Future）等待时，只领取原输出或原TaskError；不会隐式提交尚未提交的任务。
/// 输出值自身的错误与任务（Task）级失败分别保留；领取后再次轮询（poll）返回TaskError::AlreadyTaken。
/// 释放（drop）只放弃结果兴趣，不自动取消后台任务；本地输出仍限原所属线程（owner thread）。
pub struct TaskHandle<N: TaskNotifier, T, M: Send + Sync + 'static = (), K: OutputMode = SendOutput>
{
    join: JoinHandle<T, M, K>,
    control: TaskControl<N>,
}
// join已经Unpin；notifier不作结构性Pin投影，不因N约束输入Future。
impl<N: TaskNotifier, T, M: Send + Sync + 'static, K: OutputMode> Unpin for TaskHandle<N, T, M, K> {}
impl<N: TaskNotifier, T, M: Send + Sync + 'static, K: OutputMode> TaskHandle<N, T, M, K> {
    /// 读取原服务或任务（Task）的固定身份；不创建新身份或权限。
    pub fn id(&self) -> TaskId {
        self.join.id()
    }
    /// 读取原任务（Task）的基本事实，多个原子量不保证全局同一瞬间；不代表物理队列位置。
    pub fn snapshot(&self) -> TaskSnapshot {
        self.join.snapshot()
    }
    /// 同步借用创建时同类型附加数据；引用不得超过原持有借用期，写入需使用数据自身的安全接口。
    pub fn metadata(&self) -> &M {
        self.join.metadata()
    }
    /// 按需克隆同一原目标的控制权限，不复制执行权限或改变取消事实。
    pub fn control_handle(&self) -> TaskControl<N> {
        self.control.clone()
    }
    /// 取得相同任务（Task）的只读查询，不取得另一份结果或控制权限。
    pub fn query_handle(&self) -> TaskQuery {
        self.control.query_handle()
    }
}
impl<N: TaskNotifier, T, M: Send + Sync + 'static, K: OutputMode> Future
    for TaskHandle<N, T, M, K>
{
    type Output = pi_result::RawResult<T, pi_async_task::TaskError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        task_handle_poll_inner(self, cx)
    }
}

/// 一份取消等待；额外探针只为通知核原生取消事实，不形成定时登记持有环。
// @trace 6
/// 首次轮询（poll）才推进原生取消等待，返回原CancellationOutcome，不把请求记录当作析构成功。
/// 受管任务（Task）内以实际当前任务身份为准；首次挂起（Pending）后不得更换实际等待者身份，这不禁止保持身份的跨线程移动。
/// 释放（drop）撤销本份等待登记，不撤销已记录的取消；构造和重复挂起不额外重复通知。
pub struct TaskCancelWait<N: TaskNotifier> {
    wait: CancelWait,
    probe: ControlHandle,
    notifier: N,
    notified: bool,
}
impl<N: TaskNotifier> Unpin for TaskCancelWait<N> {}
impl<N: TaskNotifier> Future for TaskCancelWait<N> {
    type Output = CancellationOutcome;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut guard = CancelWaitNotifyGuard {
            probe: &this.probe,
            notifier: &this.notifier,
            notified: &mut this.notified,
            armed: false,
        };
        let outcome = task_cancel_wait_poll_inner(Pin::new(&mut this.wait), cx, &mut guard);
        guard.armed = false;
        if outcome.is_pending() && !*guard.notified {
            *guard.notified = true;
            guard.notifier.notify_cancel(|| guard.probe.snapshot());
        }
        outcome
    }
}

/// 实现者持有的创建能力；普通运行时（runtime）不向用户返回本对象。
/// Clone保持原服务身份。仅新建构造分配RuntimeId，不接受外部指定原身份。
// @trace 7
pub struct TaskFactory<S: Schedule, N: TaskNotifier> {
    runtime_id: RuntimeId,
    schedule: S,
    notifier: N,
    queue_support: QueueSupport,
}
impl<S: Schedule, N: TaskNotifier> TaskFactory<S, N> {
    /// 准备同一服务的创建工厂（factory），分配不复用的RuntimeId并保存传入能力。
    /// 失败返回原报告（Report），不创建任务（Task）、提交业务、启动服务或线程（thread）。
    pub fn try_new(
        schedule: S,
        notifier: N,
        queue_support: QueueSupport,
    ) -> pi_result::Result<Self> {
        task_factory_new_inner(schedule, notifier, queue_support)
    }
    /// 读取同一原逻辑服务的身份；不赋予来源管理或执行权限。
    pub fn runtime_id(&self) -> RuntimeId {
        self.runtime_id
    }
    pub(crate) fn notifier(&self) -> &N {
        &self.notifier
    }
    /// 同源提交能力；只有持有原工厂才可从待提交值交出Runnable给来源实现。
    /// 回调（callback）明确拒绝时必须返还同一原值，不接纳、不poll、不取消。
    pub fn submit(
        &self,
        task: UnsubmittedTask,
        mode: SubmissionMode,
        push: impl FnOnce(Runnable) -> pi_result::RawResult<(), (Runnable, SubmitErrorKind)>,
    ) -> pi_result::RawResult<(), SubmitError> {
        task_factory_submit_inner(self, task, mode, push)
    }
}
impl<S: Schedule + Clone, N: TaskNotifier> TaskFactory<S, N> {
    /// 使用指定配置创建；返回唯一待提交值与结果句柄（handle），不隐式派发。
    pub fn create<F, M>(
        &self,
        future: F,
        config: TaskConfig<M>,
    ) -> pi_result::RawResult<(UnsubmittedTask, TaskHandle<N, F::Output, M>), CreateError<F, M>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
        M: Send + Sync + 'static,
    {
        task_factory_create_inner(self, future, config)
    }
    /// 使用指定配置和原TaskThread本地创建；不依赖当前公共环境，不隐式接纳或提交。
    pub fn create_local<F, M>(
        &self,
        owner: &TaskThread,
        future: F,
        config: TaskConfig<M>,
    ) -> pi_result::RawResult<
        (UnsubmittedTask, TaskHandle<N, F::Output, M, LocalOutput>),
        CreateError<F, M>,
    >
    where
        F: Future + 'static,
        F::Output: 'static,
        M: Send + Sync + 'static,
    {
        task_factory_create_local_inner(self, owner, future, config)
    }
    /// 保留原服务与Schedule，组合受限通知；不给普通用户获取原工厂的路径。
    pub fn with_notifier<N2: TaskNotifier>(&self, notifier: N2) -> TaskFactory<S, N2> {
        task_factory_notifier_inner(self, notifier)
    }
}
impl<S: Schedule + Clone, N: TaskNotifier> Clone for TaskFactory<S, N> {
    fn clone(&self) -> Self {
        task_factory_clone_inner(self)
    }
}

/// 实现者在来源边界验证的首次提交方式；不是任务（Task）运行状态。
#[derive(Clone, Copy)]
pub enum SubmissionMode {
    /// 普通创建的可跨线程任务（Task），经原服务普通来源首次提交。
    Ordinary,
    /// 本地创建值的首次提交，载荷为必须匹配的原TaskOwnerId。
    Local(TaskOwnerId),
    /// 普通创建的Send任务（Task）提交至原空闲来源，不保证被接纳或执行。
    Idle,
}

/// 创建失败，保留尚未成为任务（Task）的原输入。
pub struct CreateError<F, M> {
    pub future: F,
    pub config: TaskConfig<M>,
    pub cause: CreateErrorKind,
}
impl<F, M> CreateError<F, M> {
    /// 消耗错误并返还原值与原原因，不克隆或释放尚需调用者处理的持有。
    pub fn into_parts(self) -> (F, TaskConfig<M>, CreateErrorKind) {
        (self.future, self.config, self.cause)
    }
}
/// 提交失败，保留同一原任务（Task），不从原任务恢复已移入的Future。
pub struct SubmitError {
    pub task: UnsubmittedTask,
    pub cause: SubmitErrorKind,
}
impl SubmitError {
    /// 消耗错误并返还原值与原原因，不克隆或释放尚需调用者处理的持有。
    pub fn into_parts(self) -> (UnsubmittedTask, SubmitErrorKind) {
        (self.task, self.cause)
    }
}
/// 便利派发（dispatch）失败时明确区分尚未创建和已经创建。
pub enum SpawnError<
    F: Future,
    N: TaskNotifier,
    M: Send + Sync + 'static = (),
    K: OutputMode = SendOutput,
> {
    Create(CreateError<F, M>),
    Submit {
        error: SubmitError,
        handle: TaskHandle<N, F::Output, M, K>,
    },
}

impl<F, M> std::fmt::Debug for CreateError<F, M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CreateError")
            .field("cause", &self.cause)
            .finish_non_exhaustive()
    }
}
impl std::fmt::Debug for SubmitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubmitError")
            .field("task_id", &self.task.id())
            .field("cause", &self.cause)
            .finish()
    }
}
impl<F: Future, N: TaskNotifier, M: Send + Sync + 'static, K: OutputMode> std::fmt::Debug
    for SpawnError<F, N, M, K>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Create(error) => f.debug_tuple("Create").field(error).finish(),
            Self::Submit { error, .. } => f.debug_tuple("Submit").field(error).finish(),
        }
    }
}

// 本通知守卫先于原生取消操作建立；实际notify之前先disarm。
// 原生取消、通知和用户Drop发生异常时不重复发请求、不新增catch_unwind。
struct CancelNotifyGuard<'a, N: TaskNotifier> {
    target: &'a TaskControl<N>,
    armed: bool,
}
impl<N: TaskNotifier> Drop for CancelNotifyGuard<'_, N> {
    fn drop(&mut self) {
        cancel_notify_drop_inner(self)
    }
}
// 每次轮询（poll）只借用原控制句柄（ControlHandle）、原通知器（TaskNotifier）
// 及本等待的通知标记。原生轮询正常返回即撤销。展开（unwind）时，只以同一任务（Task）
// 已记录的取消请求（cancellation request）接续原定时登记（registration）。
struct CancelWaitNotifyGuard<'a, N: TaskNotifier> {
    probe: &'a ControlHandle,
    notifier: &'a N,
    notified: &'a mut bool,
    armed: bool,
}
impl<N: TaskNotifier> Drop for CancelWaitNotifyGuard<'_, N> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(snapshot) = cancel_wait_notify_drop_inner(self) {
            self.notifier.notify_cancel(move || snapshot);
        }
    }
}
include!("task_inner.rs");
