//! 标准派发（dispatch）接口；来源接收与执行器接纳分别成立。
use crate::{
    task::*,
    time::{RuntimeTime, TimedTaskNotifier},
};
use pi_async_task::join::LocalOutput;
use pi_result::RawResult;
use std::{future::Future, time::Duration};

pub trait TaskDispatch {
    type Notifier: TaskNotifier;
    /// 创建原服务的可跨线程任务（Task）并保留配置；不提交、不接纳、不轮询（poll）。失败返还原Future和配置。
    fn create_with_config<F, M>(
        &self,
        future: F,
        config: TaskConfig<M>,
    ) -> RawResult<(UnsubmittedTask, TaskHandle<Self::Notifier, F::Output, M>), CreateError<F, M>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
        M: Send + Sync + 'static;
    /// 向创建它的原服务提交唯一原值；明确拒绝返还原持有及原因，不取消、不改线程归属。
    fn submit(&self, task: UnsubmittedTask) -> RawResult<(), SubmitError>;
    /// 使用默认配置创建；返回唯一待提交值与结果句柄（handle），不隐式派发。
    fn create<F>(
        &self,
        future: F,
    ) -> RawResult<(UnsubmittedTask, TaskHandle<Self::Notifier, F::Output>), CreateError<F, ()>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.create_with_config(future, TaskConfig::default())
    }
    /// 组合一次创建与提交；错误按创建／提交阶段保留对应原值，成功不表示已经执行。
    fn spawn_with_config<F, M>(
        &self,
        future: F,
        config: TaskConfig<M>,
    ) -> RawResult<TaskHandle<Self::Notifier, F::Output, M>, SpawnError<F, Self::Notifier, M>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
        M: Send + Sync + 'static,
    {
        let (task, handle) = self
            .create_with_config(future, config)
            .map_err(SpawnError::Create)?;
        match self.submit(task) {
            Ok(()) => Ok(handle),
            Err(error) => Err(SpawnError::Submit { error, handle }),
        }
    }
    /// 使用默认配置组合创建与提交；输入及输出只要求可跨线程转交（Send），不要求Sync或Unpin。
    fn spawn<F>(
        &self,
        future: F,
    ) -> RawResult<TaskHandle<Self::Notifier, F::Output>, SpawnError<F, Self::Notifier>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.spawn_with_config(future, TaskConfig::default())
    }
}

/// 静态本地入口；真实当前权限由实现核对，不要求Future／Output为Send或Sync。
pub trait LocalTaskDispatch: TaskDispatch {
    /// 通过当前有效本地权限创建；原Future与输出保持原线程清理，不要求Send／Sync／Unpin。失败返还原输入。
    fn create_local_with_config<F, M>(
        future: F,
        config: TaskConfig<M>,
    ) -> RawResult<
        (
            UnsubmittedTask,
            TaskHandle<Self::Notifier, F::Output, M, LocalOutput>,
        ),
        CreateError<F, M>,
    >
    where
        F: Future + 'static,
        F::Output: 'static,
        M: Send + Sync + 'static;
    /// 仅在原所属线程（owner thread）的有效本地范围首次提交；错误返还同一待提交值。
    fn submit_local(task: UnsubmittedTask) -> RawResult<(), SubmitError>;
    /// 使用默认配置进行本地创建；无合法当前环境时明确失败，不猜测目标。
    fn create_local<F>(
        future: F,
    ) -> RawResult<
        (
            UnsubmittedTask,
            TaskHandle<Self::Notifier, F::Output, (), LocalOutput>,
        ),
        CreateError<F, ()>,
    >
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        Self::create_local_with_config(future, TaskConfig::default())
    }
    /// 组合本地创建与本地提交；不把结果变为可跨线程，不将拒绝当成取消。
    fn spawn_local_with_config<F, M>(
        future: F,
        config: TaskConfig<M>,
    ) -> RawResult<
        TaskHandle<Self::Notifier, F::Output, M, LocalOutput>,
        SpawnError<F, Self::Notifier, M, LocalOutput>,
    >
    where
        F: Future + 'static,
        F::Output: 'static,
        M: Send + Sync + 'static,
    {
        let (task, handle) =
            Self::create_local_with_config(future, config).map_err(SpawnError::Create)?;
        match Self::submit_local(task) {
            Ok(()) => Ok(handle),
            Err(error) => Err(SpawnError::Submit { error, handle }),
        }
    }
    /// 默认配置的本地快速派发（local dispatch）；不创建新线程，不内联轮询（poll）。
    fn spawn_local<F>(
        future: F,
    ) -> RawResult<
        TaskHandle<Self::Notifier, F::Output, (), LocalOutput>,
        SpawnError<F, Self::Notifier, (), LocalOutput>,
    >
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        Self::spawn_local_with_config(future, TaskConfig::default())
    }
}

/// 空闲来源不判断是否空闲；提交成功不承诺一定被接纳或执行。
pub trait IdleDispatch: TaskDispatch {
    /// 向同一服务的空闲来源提交普通Send任务（Task）；不检查空闲、不保证未来接纳。
    fn submit_idle(&self, task: UnsubmittedTask) -> RawResult<(), SubmitError>;
    /// 创建并提交空闲时任务（idle task）；成功只证明来源接受，不保证执行。
    fn idle_with_config<F, M>(
        &self,
        future: F,
        config: TaskConfig<M>,
    ) -> RawResult<TaskHandle<Self::Notifier, F::Output, M>, SpawnError<F, Self::Notifier, M>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
        M: Send + Sync + 'static,
    {
        let (task, handle) = self
            .create_with_config(future, config)
            .map_err(SpawnError::Create)?;
        match self.submit_idle(task) {
            Ok(()) => Ok(handle),
            Err(error) => Err(SpawnError::Submit { error, handle }),
        }
    }
    /// 默认配置的空闲提交；任务（Task）未接纳前仍可按原控制能力取消。
    fn idle<F>(
        &self,
        future: F,
    ) -> RawResult<TaskHandle<Self::Notifier, F::Output>, SpawnError<F, Self::Notifier>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.idle_with_config(future, TaskConfig::default())
    }
    /// 从当前环境定位原服务后提交共享空闲来源；Future与输出要求Send，不要求Sync，不预先绑定执行线程。
    fn idle_local_with_config<F, M>(
        future: F,
        config: TaskConfig<M>,
    ) -> RawResult<TaskHandle<Self::Notifier, F::Output, M>, SpawnError<F, Self::Notifier, M>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
        M: Send + Sync + 'static;
    /// 默认配置的当前空闲提交；输入保持Send约束，不等同spawn_local的本地非Send能力。
    fn idle_local<F>(
        future: F,
    ) -> RawResult<TaskHandle<Self::Notifier, F::Output>, SpawnError<F, Self::Notifier>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        Self::idle_local_with_config(future, TaskConfig::default())
    }
}

/// 同步提交原业务任务（Task）到定时来源；不直接poll，不经普通来源绕行。
pub trait TimedTaskDispatch: TaskDispatch + RuntimeTime {
    /// 同步固定本次截止时间并提交定时来源；准备失败保原Future，明确拒绝保原任务（Task）和结果句柄（handle）。
    fn spawn_timing_with_config<F, M>(
        &self,
        future: F,
        delay: Duration,
        config: TaskConfig<M>,
    ) -> RawResult<
        TaskHandle<TimedTaskNotifier<Self::Notifier, Self::Timer>, F::Output, M>,
        SpawnError<F, TimedTaskNotifier<Self::Notifier, Self::Timer>, M>,
    >
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
        M: Send + Sync + 'static;
    /// 默认配置的同步定时派发（timed dispatch）；零时长仍入定时来源，不直接执行。
    fn spawn_timing<F>(
        &self,
        future: F,
        delay: Duration,
    ) -> RawResult<
        TaskHandle<TimedTaskNotifier<Self::Notifier, Self::Timer>, F::Output>,
        SpawnError<F, TimedTaskNotifier<Self::Notifier, Self::Timer>>,
    >
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.spawn_timing_with_config(future, delay, TaskConfig::default())
    }
    /// 当前原线程内同步定时派发（timed dispatch）；原Future／输出不要求Send／Sync，准备和拒绝均保留原值。
    fn spawn_timing_local_with_config<F, M>(
        future: F,
        delay: Duration,
        config: TaskConfig<M>,
    ) -> RawResult<
        TaskHandle<TimedTaskNotifier<Self::Notifier, Self::Timer>, F::Output, M, LocalOutput>,
        SpawnError<F, TimedTaskNotifier<Self::Notifier, Self::Timer>, M, LocalOutput>,
    >
    where
        F: Future + 'static,
        F::Output: 'static,
        M: Send + Sync + 'static;
    /// 默认配置的本地定时派发（timed dispatch）；不跳过原所属域或定时来源。
    fn spawn_timing_local<F>(
        future: F,
        delay: Duration,
    ) -> RawResult<
        TaskHandle<TimedTaskNotifier<Self::Notifier, Self::Timer>, F::Output, (), LocalOutput>,
        SpawnError<F, TimedTaskNotifier<Self::Notifier, Self::Timer>, (), LocalOutput>,
    >
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        Self::spawn_timing_local_with_config(future, delay, TaskConfig::default())
    }
}
