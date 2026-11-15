//! 事件循环（event loop）的外部空闲和休眠策略；不决定任务（Task）所属域。
use crate::registry::TaskRegistryHandle;
use pi_async_runtime_core::definitions::{IdleState, RuntimeId, RuntimeStatus};
use std::time::Duration;
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct WorkReadiness {
    pub external_deferred: bool,
    pub returns_deferred: bool,
    pub external_count: usize,
    pub internal_count: usize,
}

/// 本轮独立取样的只读输入，不持队列（queue）、任务原值或借用守卫（guard）。
#[derive(Clone, Copy)]
pub struct ExecutorSnapshot<'a> {
    pub runtime: RuntimeId,
    pub status: RuntimeStatus,
    pub external_tasks: usize,
    pub internal_tasks: usize,
    /// 真正线程内容器的待取数量；与包含发布过程的internal_tasks不同。
    pub ready_tasks: usize,
    /// 本轮已作有限尝试但发布或消费暂不可完成；总数量不能推翻这一事实。
    pub external_deferred: bool,
    pub returns_deferred: bool,
    pub timer_registrations: usize,
    pub idle_tasks: usize,
    pub in_flight_tasks: usize,
    pub(crate) registry: &'a TaskRegistryHandle,
    /// 当前运行时（runtime）的固定接纳规则是否允许本轮取普通外部新业务。
    pub can_admit_external: bool,
    /// 原定时来源仍有可立即推进的到期发现／交付，不以登记总数非零替代。
    pub timer_work_pending: bool,
    /// 尚有管理责任；不自动等价于业务可执行或无限忙等。
    pub maintenance_pending: bool,
    pub last_idle_state: IdleState,
}
/// 同步策略（policy），在所属线程调用；允许实现者保存本地数据，不要求Send／Sync。
/// 实现不能持运行时推进权递归推进或等待必须由本执行器处理的工作。
pub trait IdlePolicy {
    /// 同步返回本次是否空闲；默认忽略纯Pending和未接纳空闲时任务。
    fn is_idle(&mut self, state: &ExecutorSnapshot<'_>) -> bool {
        default_idle_inner(state)
    }
    /// None继续检查，Some为本次建议休眠上限；执行器还会受通知、就绪工作和定时截止限制。
    /// 默认10ms；不能由本返回值提前关闭、跳过取消清理或强行接纳业务。
    fn sleep_duration(&mut self, state: &ExecutorSnapshot<'_>) -> Option<Duration> {
        default_sleep_inner(state)
    }
}
/// 默认策略（policy）；覆盖其中一个方法即可保留另一个默认行为。
#[derive(Default, Clone, Copy, Debug)]
pub struct DefaultIdlePolicy;
impl IdlePolicy for DefaultIdlePolicy {}
impl ExecutorSnapshot<'_> {
    /// 按需O(n)扫描；默认策略不调用，不把成本强加给每次事件循环（event loop）。
    pub fn waiting_for_wake_tasks(&self) -> usize {
        self.registry.waiting_for_wake_count()
    }
    /// 与其它数量独立取样；不保存任务（Task）原值或可写借用。
    pub fn pending_cancel_tasks(&self) -> usize {
        self.registry.pending_cancel_count()
    }
}
include!("policy_inner.rs");
