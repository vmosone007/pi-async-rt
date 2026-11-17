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
    /// 原服务开放；一次样本不保证后续提交或接纳成功。
    Open,
    /// 关闭请求已记录，停止新业务接纳；已有责任仍须合法推进或清理。
    Closing,
    /// 原服务已关闭不可用，不重新开放正常业务；不证明原异步计算（Future）均已成功清理。
    /// 成功完成须另查CloseState::Completed；清理失败可同时观察到Closed与CloseState::Requested。
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
    /// 当前保存的公共句柄（handle）与请求的具体类型（type）不同。
    #[error("当前运行时（runtime）的公开访问类型不匹配")]
    TypeMismatch,
    /// 当前同步范围已选择不同RuntimeId，不能覆盖该选择。
    #[error("当前范围已选择另一运行时（runtime）")]
    DifferentRuntime,
    /// 线程局部存储（TLS）处于不可访问状态，本次不能取得或修改当前环境。
    #[error("线程局部存储（TLS）不可访问")]
    ContextUnavailable,
}

/// 本地入口失效；不把缺少本地能力误判为允许外线程执行。
#[derive(Debug, ThisError)]
pub enum LocalRuntimeAccessError {
    #[error("没有当前有效的本地运行时（runtime）")]
    NoLocalRuntime,
    /// 当前公共或本地设施与请求的具体类型（type）不匹配。
    #[error("本地访问类型（type）不匹配")]
    TypeMismatch,
    #[error("本地权限已经撤销")]
    AccessRevoked,
    /// 线程局部存储（TLS）处于不可访问状态，本次不能取得或修改当前环境。
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
    /// 待提交值属于另一RuntimeId；原持有和原任务（Task）身份返还。
    WrongRuntime,
    /// 本地创建值须使用原所属线程（owner thread）的本地提交入口。
    LocalSubmissionRequired,
    /// 普通创建值不能作为本地创建值首次提交，须使用普通或空闲来源入口。
    OrdinarySubmissionRequired,
    /// 提交使用的所属域与原值保存的TaskOwnerId不同，不能转投另一所属域。
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
    /// 本次计时等待因已记录的取消结束，不是正常到期。
    Cancelled,
    /// 原登记因其它实际移除结束交付责任；不包含取消结束或直接准备失败，不等于原异步计算（Future）已析构（drop）。
    RegistrationRemoved,
    /// 准备或取时所需服务已不可用；已有登记退出仍按实际结束原因分类，不一律改为本错误。
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
