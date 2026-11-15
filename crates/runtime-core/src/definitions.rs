//! 公共身份、观察分类和不携带业务原值的错误原因。
use pi_async_task::definitions::BuildErrorKind;
use pi_async_task_queue::{
    external::ExternalPushErrorKind, idle::IdleQueueError, local::QueuePushErrorKind,
    timer::TimerQueueError,
};
use pi_result::prelude::*;
use std::num::NonZeroU64;
use std::time::Duration;

/// 同一进程内原逻辑服务的固定身份；不复用，不代表任务（Task）身份或权限。
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
// @trace 1
pub struct RuntimeId(NonZeroU64);

impl RuntimeId {
    /// 分配一份新的服务身份；耗尽明确报错，不回绕或复用。
    pub fn try_new() -> pi_result::Result<Self> {
        runtime_id_new_inner()
    }
    /// 读取固定非零值，无权限变化。
    pub fn get(self) -> u64 {
        self.0.get()
    }
}

/// 服务接收与退出的基本状态（state）；关闭后不重新开放。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeStatus {
    Open,
    Closing,
    Closed,
}
/// 最近一次策略判断；读取不重新调用策略。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdleState {
    Unknown,
    Idle,
    NotIdle,
}
/// 关闭责任进度；完成不表示最后共享存储已释放。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CloseState {
    NotRequested,
    Requested,
    Completed,
}
/// 同步关闭请求的即时结果，不等待实际清理。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CloseRequestStatus {
    Requested,
    AlreadyRequested,
    AlreadyClosed,
}

/// 当前公共环境访问失败；不据此隐式创建服务。
#[derive(Debug, ThisError)]
pub enum CurrentRuntimeAccessError {
    #[error("没有当前运行时（runtime）环境")]
    NoCurrentRuntime,
    #[error("当前运行时（runtime）的公开访问类型不匹配")]
    TypeMismatch,
    #[error("当前范围已选择另一运行时（runtime）")]
    DifferentRuntime,
    #[error("线程局部存储（TLS）不可访问")]
    ContextUnavailable,
}

/// 本地入口失效；不把缺少本地能力误判为允许外线程执行。
#[derive(Debug, ThisError)]
pub enum LocalRuntimeAccessError {
    #[error("没有当前有效的本地运行时（runtime）")]
    NoLocalRuntime,
    #[error("本地访问类型（type）不匹配")]
    TypeMismatch,
    #[error("本地权限已经撤销")]
    AccessRevoked,
    #[error("线程局部存储（TLS）不可访问")]
    ContextUnavailable,
}

/// 准备任务（Task）失败的有限原因；业务输入由外层错误单独返还。
pub enum CreateErrorKind {
    Task(BuildErrorKind),
    LocalContext(LocalRuntimeAccessError),
    Timer(TimerError),
}

/// 明确未接收的原因；原值、原来源及既有取消事实均保持。
pub enum SubmitErrorKind {
    WrongRuntime,
    LocalSubmissionRequired,
    OrdinarySubmissionRequired,
    WrongOwner,
    LocalContext(LocalRuntimeAccessError),
    External(ExternalPushErrorKind),
    LocalQueue(QueuePushErrorKind),
    Idle(IdleQueueError),
    Timer(TimerQueueError),
}

/// 计时等待错误；保存原报告（Report），不要求其可克隆（Clone）。
pub enum TimerError {
    /// 正时长不在入口允许范围；max为None时没有该入口的固定时长上限。
    InvalidDuration {
        value: Duration,
        min_positive: Duration,
        max: Option<Duration>,
    },
    Clock(pi_result::Error),
    DeadlineOutOfRange(pi_result::Error),
    RegistrationPreparation(pi_result::Error),
    Queue(TimerQueueError),
    Cancelled,
    RegistrationRemoved,
    ServiceClosed,
}

/// 组合中的超时与计时设施失败分别表达。
pub enum TimeoutError {
    Elapsed,
    Timer(TimerError),
}
/// 单值／任意值等待的原等待错误与超时分别表达。
pub enum WaitTimeoutError {
    Wait(pi_async_wait::WaitError),
    Elapsed,
    Timer(TimerError),
}

// 下层来源错误不必实现Debug／Error。外层自行保留有限分类、原报告和source，
// 不修改下层声明，不为格式化要求业务原值Debug，不将Report变为字符串。
macro_rules! error_format {
    ($ty:ty, $debug:ident, $display:ident) => {
        impl std::fmt::Debug for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                $debug(self, f)
            }
        }
        impl std::fmt::Display for $ty {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                $display(self, f)
            }
        }
    };
}
error_format!(
    CreateErrorKind,
    create_error_debug_inner,
    create_error_display_inner
);
error_format!(
    SubmitErrorKind,
    submit_error_debug_inner,
    submit_error_display_inner
);
error_format!(
    TimerError,
    timer_error_debug_inner,
    timer_error_display_inner
);
error_format!(
    TimeoutError,
    timeout_error_debug_inner,
    timeout_error_display_inner
);
error_format!(
    WaitTimeoutError,
    wait_timeout_error_debug_inner,
    wait_timeout_error_display_inner
);

impl std::error::Error for TimerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        timer_error_source_inner(self)
    }
}
impl TimerError {
    /// 消耗式转入统一报告链（report chain），原错误与附件必须保持可读取。
    pub fn into_report(self) -> pi_result::Error {
        timer_error_report_inner(self)
    }
}
include!("definitions_inner.rs");
