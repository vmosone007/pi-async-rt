//! 所属线程（owner thread）的内部设施；普通业务不直接获得队列或原生所属域。
use crate::{
    parking::ThreadNotifier, registry::TaskRegistry, returns::ReturnQueue, state::ExecutorState,
    timer::ExecutorTimer,
};
use pi_async_runtime_core::definitions::RuntimeId;
use pi_async_task::{
    definitions::{TaskId, TaskOwnerId},
    Runnable, TaskRegistration, TaskThread,
};
use pi_async_task_queue::{timer::TimerSubmitError, QueuePushError, TaskQueue};
use std::{
    cell::{Cell, RefCell},
    collections::{HashMap, VecDeque},
    rc::Rc,
    sync::Arc,
};

/// 本地设施（local facilities）的弱访问目标；不实现Send／Sync，不能跨线程借入。
/// 公开给框架实现者用于静态接入，不向最终运行时（runtime）用户导出其实例。
/// 释放（drop）时在原所属线程（owner thread）完成实际退出，仍在进行的同步活动可延迟最终释放。
/// 原任务域与来源沿自身约定清理，首次恐慌（panic）继续传播；不可把关闭请求当成功确认。
pub struct ExecutorLocal<T: ExecutorTimer + 'static> {
    pub(crate) runtime: RuntimeId,
    pub(crate) owner_id: TaskOwnerId,
    pub(crate) owner: RefCell<Option<TaskThread>>,
    pub(crate) ready: RefCell<Option<TaskQueue>>,
    pub(crate) timer: RefCell<Option<T>>,
    pub(crate) returns: RefCell<Option<ReturnQueue>>,
    pub(crate) registry: RefCell<Option<TaskRegistry>>,
    pub(crate) active: RefCell<HashMap<TaskId, ActiveRecord>>,
    pub(crate) active_order: RefCell<VecDeque<TaskId>>,
    pub(crate) state: Arc<ExecutorState>,
    pub(crate) notifier: ThreadNotifier,
    exit_requested: Cell<bool>,
    exit_running: Cell<bool>,
    source_failed: Cell<bool>,
    exit_finished: Cell<bool>,
}
pub(crate) enum ActiveRecord {
    Registered(Rc<TaskRegistration>),
    Interrupted(pi_async_runtime_core::task::TaskQuery),
}
impl ActiveRecord {
    fn snapshot(&self) -> pi_async_task::definitions::TaskSnapshot {
        match self {
            Self::Registered(r) => r.snapshot(),
            Self::Interrupted(q) => q.snapshot(),
        }
    }
}
pub(crate) enum PreparedTask {
    Business(Runnable, Rc<TaskRegistration>),
    Cleanup(Runnable),
    Settled(Runnable),
}
struct AdmissionAttemptGuard<'a, T: ExecutorTimer + 'static> {
    local: &'a ExecutorLocal<T>,
    task: &'a Runnable,
    armed: bool,
}
impl<T: ExecutorTimer + 'static> Drop for AdmissionAttemptGuard<'_, T> {
    fn drop(&mut self) {
        admission_attempt_drop_inner(self)
    }
}
enum LocalExitPhase {
    Owner,
    Ready,
    Timer,
    Returns,
    Active,
    Registry,
    Finish,
    Done,
}
struct LocalExitGuard<'a, T: ExecutorTimer + 'static> {
    local: &'a ExecutorLocal<T>,
    phase: LocalExitPhase,
    current_failed: bool,
    failed: bool,
    domain_unconfirmed: usize,
    admitted_unconfirmed: usize,
}
struct LocalActivityGuard<'a, T: ExecutorTimer + 'static> {
    local: &'a ExecutorLocal<T>,
}
struct ReadyReleaseGuard<'a> {
    state: &'a ExecutorState,
    count: usize,
}
// 唯一TaskThread放Option中；只有共享不可变owner借用能跨一次poll/用户调用。
// ready/timer/returns/registry/index/TLS可写借用必须在调用用户逻辑前全部结束。
// 每次本地操作由TLS升级的临时Rc保活；退出先记意图，owner尚被借用则延后真实take。
// 下一安全作用域结束或最后Local Drop继续；不强行穿透RefCell、转型生命周期或新增锁。
// 真正TaskThread Drop -> ready/timer -> Returns OWNER_GONE -> active -> registry seal。
// 原域失败统计在TaskThread真正结束后、registry移走前查询，失败不伪造Completed。
impl<T: ExecutorTimer + 'static> ExecutorLocal<T> {
    pub(crate) fn new(
        runtime: RuntimeId,
        owner: TaskThread,
        ready: TaskQueue,
        timer: T,
        returns: ReturnQueue,
        registry: TaskRegistry,
        state: Arc<ExecutorState>,
        notifier: ThreadNotifier,
    ) -> Self {
        local_new_inner(
            runtime, owner, ready, timer, returns, registry, state, notifier,
        )
    }
    pub fn runtime_id(&self) -> RuntimeId {
        self.runtime
    }
    /// 同步框架访问；不允许返回owner借用。只读Ref可跨本次调用，不持其它队列借用。
    pub fn with_owner<R>(&self, action: impl FnOnce(&TaskThread) -> R) -> pi_result::Result<R> {
        local_owner_inner(self, action)
    }
    /// 原线程尾插／头插，尚未接纳的create_local可排队；不在此poll或提前admit。
    /// 正常新入队用同一ExecutorState记数，拒绝返还原值。
    pub fn try_push(
        &self,
        task: Runnable,
        front: bool,
    ) -> pi_result::RawResult<(), QueuePushError> {
        local_push_inner(self, task, front)
    }
    /// 同源定时本地提交；不为Item添加Send／Sync，不刷新时钟。
    pub fn try_push_timer(
        &self,
        deadline: T::Instant,
        item: T::Item,
    ) -> pi_result::RawResult<T::Key, TimerSubmitError<T::Item>> {
        local_timer_push_inner(self, deadline, item)
    }
    pub(crate) fn selected(&self, task: Runnable) -> pi_result::Result<PreparedTask> {
        local_selected_inner(self, task)
    }
    pub(crate) fn maintain(&self, max_nodes: usize) {
        local_maintain_inner(self, max_nodes)
    }
    pub(crate) fn finish_exit(&self, source_failed: bool) -> bool {
        local_finish_exit_inner(self, source_failed)
    }
}
impl<T: ExecutorTimer + 'static> Drop for ExecutorLocal<T> {
    fn drop(&mut self) {
        local_drop_inner(self)
    }
}
impl<T: ExecutorTimer + 'static> Drop for LocalActivityGuard<'_, T> {
    fn drop(&mut self) {
        local_activity_drop_inner(self)
    }
}
impl Drop for ReadyReleaseGuard<'_> {
    fn drop(&mut self) {
        ready_release_inner(self)
    }
}
impl<T: ExecutorTimer + 'static> LocalExitGuard<'_, T> {
    fn drain(&mut self) {
        local_exit_drain_inner(self)
    }
}
impl<T: ExecutorTimer + 'static> Drop for LocalExitGuard<'_, T> {
    fn drop(&mut self) {
        local_exit_drop_inner(self)
    }
}
include!("local_inner.rs");
