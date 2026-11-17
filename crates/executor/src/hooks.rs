//! 执行器钩子（executor hooks）的有限权限，不转交Runnable。
use pi_async_task::{
    definitions::{RunOutcome, TaskId, TaskSnapshot},
    TaskRegistration,
};

/// 本次同步调用中合法借用的任务信息（task information）；不授予执行／控制权限。
pub struct TaskInfo<'a> {
    registration: &'a TaskRegistration,
}
impl<'a> TaskInfo<'a> {
    pub(crate) fn new(registration: &'a TaskRegistration) -> Self {
        Self { registration }
    }
    pub fn id(&self) -> TaskId {
        self.registration.snapshot().id
    }
    pub fn snapshot(&self) -> TaskSnapshot {
        self.registration.snapshot()
    }
    /// 按创建类型借用附加数据，不能逃逸或跨await；M自身的安全内部可变性保持。
    pub fn metadata<M: Send + Sync + 'static>(&self) -> Option<&M> {
        self.registration.metadata::<M>()
    }
}
/// 轮询（poll）前同步控制结果；只改变下一步选择，不从中间切断正在运行的Future。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskAction {
    /// 再核取消/关闭后调用原Runnable::run一次。
    Poll,
    /// 本轮不poll，将原任务放回同一内部队尾；不承诺延迟时长或公平性。
    Defer,
    /// 放弃本份执行安排，按原任务规则取消并交原线程清理，不直接drop Future。
    Cancel,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// 正常业务选择期间的同步通知（notification），不授予控制或执行持有的所有权（ownership）。
/// 延后、取消、错误或恐慌（panic）可使BeforePoll没有配对AfterRun，不承诺所有路径都发送两次通知。
pub enum ExecutorEvent {
    /// 已结束容器借用、尚未调用控制钩子（hook）；回调后仍须重查取消与关闭。
    BeforePoll,
    /// 仅原Runnable::run真实返回RunOutcome后通知，不将RunError、延后或直接退出伪装为该结果。
    AfterRun(RunOutcome),
}
/// 同线程静态钩子（hook）；普通通知必须快速非阻塞，控制调用可同步阻塞当前执行器。
/// 两者均不在队列Consumer/TLS/注册表借用内调用；panic沿原范围传播，不新增捕获。
pub trait ExecutorHooks {
    /// 同步非阻塞通知，无返回控制结果；不能通过此签名承诺任意实现绝对零延迟。
    fn on_event(&mut self, event: ExecutorEvent, task: &TaskInfo<'_>) {
        hook_event_default_inner(event, task)
    }
    /// 同步控制钩子；返回前本执行器不越过此位置。不得等待自身必须推进的任务或嵌套block_on。
    /// 不得据旧快照覆盖期间发生的取消/关闭；Defer后仍服从原归属与下次选择。
    fn before_poll(&mut self, task: &TaskInfo<'_>) -> TaskAction {
        hook_control_default_inner(task)
    }
}
/// 默认无操作接收者；静态分发下由优化器消去空回调，不强制高级观测。
#[derive(Default, Clone, Copy, Debug)]
pub struct NoHooks;
impl ExecutorHooks for NoHooks {}
include!("hooks_inner.rs");
