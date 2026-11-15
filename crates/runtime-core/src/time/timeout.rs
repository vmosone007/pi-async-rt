//! 惰性计时等待及独立取消权限。
use super::{
    access::TimerAccess,
    entry::TimerEntry,
    registration::{RegistrationEnd, TimerRegistrationKind},
    yielding::YieldOnce,
};
use crate::definitions::TimerError;
use pi_async_task_queue::timer::TimerQueueError;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

/// 正时长首次poll才固定截止时间并登记；零时长只让出一次。
#[must_use = "须await或poll才开始计时；Drop结束兴趣并安排撤销"]
// @trace 1
pub struct Timeout<A: TimerAccess> {
    access: A,
    duration: Duration,
    registration: Option<A::Registration>,
    initial_error: Option<TimerError>,
    phase: TimeoutPhase,
    zero: YieldOnce,
}
#[derive(Clone, Copy)]
enum TimeoutPhase {
    Prepared,
    Registered,
    Zero,
    Done,
}
impl<A: TimerAccess> Timeout<A> {
    /// 只检查时长并准备共享登记；不读取时钟，不提交来源。
    pub fn new(access: A, duration: Duration) -> Self {
        timeout_new_inner(access, duration)
    }
    /// 按需取得同一登记的共享控制；失败准备或零时长所得控制取消返回false。
    pub fn control_handle(&self) -> TimeoutControl<A> {
        timeout_control_inner(self)
    }
}
impl<A: TimerAccess> Unpin for Timeout<A> {}
impl<A: TimerAccess> Future for Timeout<A> {
    type Output = pi_result::RawResult<(), TimerError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        timeout_poll_inner(self, cx)
    }
}
impl<A: TimerAccess> Drop for Timeout<A> {
    fn drop(&mut self) {
        timeout_drop_inner(self)
    }
}

/// 同一计时登记的线程安全（thread-safe）控制，不拥有被等待的业务Future。
// @trace 2
pub struct TimeoutControl<A: TimerAccess> {
    access: A,
    registration: Option<A::Registration>,
}
impl<A: TimerAccess> TimeoutControl<A> {
    /// 记录／合并取消；true不证明来源移除，false表示本份等待已结束或不可取消。
    /// 错误不回滚已记录请求，后续责任仍由预备接续者承担。
    pub fn cancel(&self) -> pi_result::RawResult<bool, TimerError> {
        timeout_cancel_inner(self)
    }
}
impl<A: TimerAccess> Clone for TimeoutControl<A> {
    fn clone(&self) -> Self {
        timeout_control_clone_inner(self)
    }
}

/// 实现者只在取出一份真实预备接续后调用；每次有限工作，不新建任务（Task）。
pub fn resume_timer_cancel<A: TimerAccess>(access: &A, registration: A::Registration) {
    timer_resume_inner(access, registration)
}

/// 原创建工厂持有者的定时派发支持；普通运行时不导出原工厂。
/// 准备失败保原F／配置，来源明确拒绝保原待提交值及结果句柄（handle）。
pub fn spawn_task<S, N, A, F, M>(
    factory: &crate::task::TaskFactory<S, N>,
    access: A,
    future: F,
    delay: Duration,
    config: crate::task::TaskConfig<M>,
) -> pi_result::RawResult<
    crate::task::TaskHandle<super::task_notify::TimedTaskNotifier<N, A>, F::Output, M>,
    crate::task::SpawnError<F, super::task_notify::TimedTaskNotifier<N, A>, M>,
>
where
    S: pi_async_task::Schedule + Clone,
    N: crate::task::TaskNotifier,
    A: TimerAccess,
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    M: Send + Sync + 'static,
{
    spawn_timed_task_inner(factory, access, future, delay, config)
}

/// 当前合法所属线程的相同组合，不要求原F／输出Send或Sync。
pub fn spawn_local_task<S, N, A, F, M>(
    factory: &crate::task::TaskFactory<S, N>,
    owner: &pi_async_task::TaskThread,
    access: A,
    future: F,
    delay: Duration,
    config: crate::task::TaskConfig<M>,
) -> pi_result::RawResult<
    crate::task::TaskHandle<
        super::task_notify::TimedTaskNotifier<N, A>,
        F::Output,
        M,
        pi_async_task::LocalOutput,
    >,
    crate::task::SpawnError<
        F,
        super::task_notify::TimedTaskNotifier<N, A>,
        M,
        pi_async_task::LocalOutput,
    >,
>
where
    S: pi_async_task::Schedule + Clone,
    N: crate::task::TaskNotifier,
    A: TimerAccess,
    F: Future + 'static,
    F::Output: 'static,
    M: Send + Sync + 'static,
{
    spawn_timed_local_task_inner(factory, owner, access, future, delay, config)
}

// start与键发布之间的唯一登记责任；失败／展开不能伪装未接收。
struct RegistrationGuard<A: TimerAccess> {
    access: A,
    registration: A::Registration,
    armed: bool,
}
impl<A: TimerAccess> Drop for RegistrationGuard<A> {
    fn drop(&mut self) {
        registration_guard_drop_inner(self)
    }
}
// 正在转交取消请求的唯一责任；失败原错误归显式返回或预备节点，不能两份拥有。
struct ForwardGuard<A: TimerAccess> {
    access: A,
    registration: Option<A::Registration>,
    error: Option<TimerQueueError>,
}
impl<A: TimerAccess> Drop for ForwardGuard<A> {
    fn drop(&mut self) {
        forward_guard_drop_inner(self)
    }
}
pub(crate) enum CancelMode {
    WaitControl,
    Detached,
    Resume,
    KeyPublished,
}
pub(crate) fn request_registration_cancel<A: TimerAccess>(
    access: &A,
    registration: &A::Registration,
    mode: CancelMode,
) -> pi_result::RawResult<bool, TimerQueueError> {
    registration_cancel_forward_inner(access, registration, mode)
}
include!("timeout_inner.rs");
