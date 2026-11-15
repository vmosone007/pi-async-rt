//! 事件循环（event loop）的固定结果与运行时（runtime）接纳规则。
use pi_async_runtime_core::definitions::{RuntimeId, RuntimeStatus};
use std::{num::NonZeroUsize, time::Duration};

/// 由运行时（runtime）提供的普通新业务接纳规则；不限制原任务唤醒回送。
pub trait AdmissionRule {
    /// 在实际获取外部批次前同步取样；不能重入本执行器或访问原队列。
    fn admits_external(&self, state: &AdmissionState) -> bool;
}
/// 供普通新业务接纳规则读取的独立样本；不包含本次判断的输出，避免循环定义。
#[derive(Clone, Copy, Debug)]
pub struct AdmissionState {
    pub runtime: RuntimeId,
    pub status: RuntimeStatus,
    /// 含本地及原归属回送的尚待处理持有，不含纯Pending。
    pub internal_tasks: usize,
    /// 已接纳且自身原值责任未结束；未确认失败仍保留。
    pub in_flight_tasks: usize,
}
/// 定时来源交付方式；One直接执行至多一份，All保持来源顺序放本地队首。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimerDelivery {
    One,
    All,
}
/// 默认执行器（executor）的固定工作配置；不改变运行时（runtime）接纳语义。
#[derive(Clone, Copy, Debug)]
pub struct ExecutorOptions {
    /// 回送转入本地、注册维护的每步节点上限，默认64，不限制外部当前有限批次。
    pub maintenance_batch: NonZeroUsize,
    /// 原生WouldBlock后的额外立即尝试次数，允许0至2，默认2；Busy不立即重试。
    pub would_block_retries: u8,
    pub timer_delivery: TimerDelivery,
    /// 线程被动重查上限，允许1ms至10ms，默认10ms；到期更近时可以更短。
    pub max_sleep: Duration,
}
impl Default for ExecutorOptions {
    fn default() -> Self {
        executor_options_default_inner()
    }
}
impl ExecutorOptions {
    /// 校验范围；无时钟采样、线程创建或副作用，非法配置返回原分类报告（Report）。
    pub fn validate(&self) -> pi_result::Result<()> {
        executor_options_validate_inner(self)
    }
}
/// 一次有限轮转结果；数量不是全局同一瞬间快照（snapshot）。
#[derive(Clone, Copy, Debug)]
pub struct TurnResult {
    /// 业务选择后调用原run次数，至多One计时／本地／空闲各一次；原安全检查可能转退出。
    /// 不计有限批次中直接退出清理，不能当作真实Future::poll次数统计。
    pub runs: usize,
    pub made_progress: bool,
    /// 有立即可推进工作时为true；管理欠账或纯Pending不单独造成永久忙等。
    pub immediate_work: bool,
    /// 仅实际服务关闭不可用，不等价于原Future全成功清理。
    pub service_closed: bool,
    /// 本次建议上限；实际停放前必须重新采样并核通知和定时约束。
    pub sleep_for: Option<Duration>,
    pub(crate) readiness: crate::policy::WorkReadiness,
}
include!("drive_inner.rs");
