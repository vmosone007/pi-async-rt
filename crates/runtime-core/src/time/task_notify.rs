//! 原生任务（Task）取消与定时登记取消的受限关联。
use super::access::TimerAccess;
use crate::task::TaskNotifier;
use pi_async_task::definitions::TaskSnapshot;
use pi_async_task_queue::notify::QueueNotifier;
/// 同一原任务（Task）的通知与定时登记关联，不持有另一份执行权限。
/// 普通notify只转发线程通知（thread notification）；取消通知依据已记录的原生取消安排定时取消。
/// 克隆（Clone）只保同一登记和原目标，不创建任务、登记或新截止时间。
pub struct TimedTaskNotifier<N: TaskNotifier, A: TimerAccess> {
    inner: N,
    access: A,
    registration: A::Registration,
}
impl<N: TaskNotifier, A: TimerAccess> TimedTaskNotifier<N, A> {
    /// 组合原通知、同服务计时访问及该任务（Task）的原登记；调用者须保持它们属于同一原目标。
    /// 构造只接管这些能力，不提交、取消或轮询（poll）任务。
    pub fn new(inner: N, access: A, registration: A::Registration) -> Self {
        timed_notifier_new_inner(inner, access, registration)
    }
}
impl<N: TaskNotifier, A: TimerAccess> Clone for TimedTaskNotifier<N, A> {
    fn clone(&self) -> Self {
        timed_notifier_clone_inner(self)
    }
}
impl<N: TaskNotifier, A: TimerAccess> QueueNotifier for TimedTaskNotifier<N, A> {
    fn notify(&self) {
        self.inner.notify();
    }
}
impl<N: TaskNotifier, A: TimerAccess> TaskNotifier for TimedTaskNotifier<N, A> {
    /// 安排取消事实之后的必要检查；普通实现不求值snapshot，定时实现只依据原生Recorded事实。
    fn notify_cancel(&self, snapshot: impl FnOnce() -> TaskSnapshot) {
        timed_notifier_cancel_inner(self, snapshot)
    }
}
include!("task_notify_inner.rs");
