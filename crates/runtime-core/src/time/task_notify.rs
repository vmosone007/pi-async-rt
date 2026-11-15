//! 原生任务（Task）取消与定时登记取消的受限关联。
use super::access::TimerAccess;
use crate::task::TaskNotifier;
use pi_async_task::definitions::TaskSnapshot;
use pi_async_task_queue::notify::QueueNotifier;
pub struct TimedTaskNotifier<N: TaskNotifier, A: TimerAccess> {
    inner: N,
    access: A,
    registration: A::Registration,
}
impl<N: TaskNotifier, A: TimerAccess> TimedTaskNotifier<N, A> {
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
