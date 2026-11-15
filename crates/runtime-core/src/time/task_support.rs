//! 定时派发（timed dispatch）在来源可见前登记只读查询的实现者支持。
use super::{
    access::TimerAccess,
    entry::TimerEntry,
    registration::{RegistrationEnd, TimerRegistrationKind},
    task_notify::TimedTaskNotifier,
    timeout::{request_registration_cancel, CancelMode},
};
use crate::{
    definitions::{CreateErrorKind, SubmitErrorKind},
    task::{
        SpawnError, SubmissionMode, TaskConfig, TaskFactory, TaskHandle, TaskNotifier, TaskQuery,
    },
};
use pi_async_task::{LocalOutput, Schedule, TaskThread};
use std::{future::Future, time::Duration};

/// 与普通定时派发相同，增加创建后、来源接收前的一次只读查询交接。
/// register同步消耗一份TaskQuery，不接管执行持有；失败准备不调用它。
/// 供实现者完成基本统计，普通业务仍使用TimedTaskDispatch，不接触此回调。
pub fn spawn_task_with_query<S, N, A, F, M, G>(
    factory: &TaskFactory<S, N>,
    access: A,
    future: F,
    delay: Duration,
    config: TaskConfig<M>,
    register: G,
) -> pi_result::RawResult<
    TaskHandle<TimedTaskNotifier<N, A>, F::Output, M>,
    SpawnError<F, TimedTaskNotifier<N, A>, M>,
>
where
    S: Schedule + Clone,
    N: TaskNotifier,
    A: TimerAccess,
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    M: Send + Sync + 'static,
    G: FnOnce(TaskQuery),
{
    spawn_query_inner(factory, access, future, delay, config, register)
}
/// 相同的原线程（owner thread）入口；不增加F／输出的Send／Sync或Unpin约束。
pub fn spawn_local_task_with_query<S, N, A, F, M, G>(
    factory: &TaskFactory<S, N>,
    owner: &TaskThread,
    access: A,
    future: F,
    delay: Duration,
    config: TaskConfig<M>,
    register: G,
) -> pi_result::RawResult<
    TaskHandle<TimedTaskNotifier<N, A>, F::Output, M, LocalOutput>,
    SpawnError<F, TimedTaskNotifier<N, A>, M, LocalOutput>,
>
where
    S: Schedule + Clone,
    N: TaskNotifier,
    A: TimerAccess,
    F: Future + 'static,
    F::Output: 'static,
    M: Send + Sync + 'static,
    G: FnOnce(TaskQuery),
{
    spawn_local_query_inner(factory, owner, access, future, delay, config, register)
}
struct QueryRegistrationGuard<A: TimerAccess> {
    access: A,
    registration: A::Registration,
    armed: bool,
}
impl<A: TimerAccess> Drop for QueryRegistrationGuard<A> {
    fn drop(&mut self) {
        query_registration_drop_inner(self)
    }
}
// 不改既有timeout::spawn_task/local或其原占位。构造顺序：时间/登记准备→原任务
// 创建→register一次原TaskQuery→原Factory.submit交原Rune→原TimerEntry→来源→键发布。
// register之前来源绝不可见原Task，防并发执行器取到尚无查询记录的原值。
// 若register恐慌，原任务仍未提交，局部原待提交值按原放弃路径合法清理；不伪正常拒绝。
// 直接错误/拒绝/接受后异常及迟到键沿原事实协议，不引入取消状态副本。
include!("task_support_inner.rs");
