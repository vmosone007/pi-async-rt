//! 定时设施的受限访问；不暴露来源管理者或可变队列（queue）。
use super::{
    entry::TimerEntry,
    registration::{TimerRegistration, TimerRegistrationKind},
};
use crate::definitions::{RuntimeId, TimerError};
use pi_async_task_queue::timer::{TimerQueueError, TimerSubmitError};
use std::time::Duration;

/// 供运行时（runtime）实现者连接计时来源的七项操作，通常静态分发。
///
/// 每个Registration须始终关联同一原来源与基础记录，准备时配齐有限重试所需存储。
/// 成功接受后通知异常不能伪装成未接收；实现必须保留迟到键或真实关闭接续者。
// @trace 1
pub trait TimerAccess: Clone + Send + Sync + 'static {
    type Instant: Send + 'static;
    type Key: Send + Sync + 'static;
    type Registration: Clone + Send + Sync + 'static + AsRef<TimerRegistration<Self::Key>>;
    /// 读取同一原逻辑服务的身份；不赋予来源管理或执行权限。
    fn runtime_id(&self) -> RuntimeId;
    /// 真实取当前单调时间一次并检查相加；不以无新鲜度保证的旧样本导致提前到期。
    fn deadline_after(&self, duration: Duration)
        -> pi_result::RawResult<Self::Instant, TimerError>;
    /// 一次准备共享登记与必要重试存储；失败保原报告（Report），不取时、不入队、不创建Task。
    fn prepare_registration(
        &self,
        kind: TimerRegistrationKind,
    ) -> pi_result::RawResult<Self::Registration, TimerError>;
    /// 线程安全（thread-safe）尝试接收原登记；明确拒绝返还同一TimerEntry，接受后异常由真实参与者接续，不伪装拒绝。
    fn try_register(
        &self,
        deadline: Self::Instant,
        item: TimerEntry<Self::Key, Self::Registration>,
    ) -> pi_result::RawResult<Self::Key, TimerSubmitError<TimerEntry<Self::Key, Self::Registration>>>;
    /// 仅当前合法所属线程可用；错误必须返还原值，本地载荷不额外要求Send／Sync。
    fn try_register_local(
        &self,
        deadline: Self::Instant,
        item: TimerEntry<Self::Key, Self::Registration>,
    ) -> pi_result::RawResult<Self::Key, TimerSubmitError<TimerEntry<Self::Key, Self::Registration>>>;
    /// 以同源键（key）记录或合并取消请求；Ok不证明登记存在、已移除或任务（Task）已清理。
    fn request_cancel(&self, key: &Self::Key) -> pi_result::RawResult<(), TimerQueueError>;
    /// 接管同一取消责任和可选原错误，不失败、不等待、不新增Task或每次重试分配。
    /// 实际服务通过resume_timer_cancel有限处理，关闭／拥有者退出仍须保有真实接续者。
    fn defer_cancel(&self, registration: Self::Registration, error: Option<TimerQueueError>);
}
