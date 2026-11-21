//! 定时设施的受限访问；不暴露来源管理者或可变队列（queue）。
use super::{
    entry::TimerEntry,
    registration::{TimerRegistration, TimerRegistrationKind},
};
use crate::definitions::{RuntimeId, TimerError};
use pi_async_task_queue::timer::{TimerQueueError, TimerSubmitError};
use std::time::Duration;

pub use super::registration::Confirmation;

/// 本次请求在同一来源取得的结果。
///
/// 两种结果都不证明目标已移除、任务（Task）已取消或原异步计算（Future）已释放。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimerCancelRequestOutcome {
    /// 原来源已记录或合并请求。
    Recorded,
    /// 已确认同一来源合法接管关闭（close），由该来源继续现有清理责任。
    HandledByClose,
}

impl<K: Send + Sync + 'static> Confirmation<'_, K> {
    pub(crate) fn finish_request(self, outcome: TimerCancelRequestOutcome) {
        match outcome {
            TimerCancelRequestOutcome::Recorded => self.forwarded(),
            TimerCancelRequestOutcome::HandledByClose => self.source_finished(),
        }
    }
}

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
    /// 以同源键（key）记录或合并取消请求（cancellation request），保持原失败原因。
    /// 已确认合法关闭接续时，返回 `HandledByClose`，由同一来源继续清理。
    /// 由本次操作确定分类，后续独立采样不能替代发布或关闭接管的证据。
    fn request_cancel(
        &self,
        key: &Self::Key,
    ) -> pi_result::RawResult<TimerCancelRequestOutcome, TimerQueueError>;
    /// 接管同一登记的取消责任及可选原错误（error），同步且不等待。
    ///
    /// `confirmation` 只能对应原 `registration`。先让准备好的原接续节点（node）实际接管，
    /// 再确认待接续，随后才允许原节点被取出。已确认由同一来源合法关闭或完成移除时，
    /// 可直接结束转交责任。正常返回须完成上述一种交接。
    /// 通过 `resume_timer_cancel` 从原节点有限接续，不新增任务（Task）或每次重试分配。
    /// 实现须在已有恐慌（panic）展开时保有原责任，通知（notification）在交接完成后发生。
    fn defer_cancel(
        &self,
        registration: Self::Registration,
        error: Option<TimerQueueError>,
        confirmation: Confirmation<'_, Self::Key>,
    );
}
