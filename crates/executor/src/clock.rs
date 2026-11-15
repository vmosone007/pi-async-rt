//! 执行器（executor）统一取得同源时间，来源队列仅消费显式时间。
use pi_clock_manual::global::GlobalManualInstant;
use std::time::Duration;

/// 一份逻辑时间轴；克隆（Clone）不得改变起点或产生另一份独立时间。
///
/// 正常取时不推进任何任务（Task）或队列。实际系统取时和竞争成本由实现说明；
/// 不以旧缓存冒充新鲜时间，不用时钟值代替业务数据的同步。
pub trait ExecutorClock: Clone + Send + Sync + 'static {
    /// 与接入的定时来源相同的时间点（time point）；不要求Copy、Ord、Sync或Unpin。
    type Instant: Send + 'static;
    /// 一次真实采样并采用同源单调结果；原失败报告（Report）原样返回。
    fn now(&self) -> pi_result::Result<Self::Instant>;
    /// 对显式原时刻受检相加；不再次采样，不在越界时回绕或饱和成正常截止。
    fn checked_add(
        &self,
        now: &Self::Instant,
        duration: Duration,
    ) -> pi_result::Result<Self::Instant>;
    /// 计算到显式截止的剩余时长；已到或已过返回零，不再次取时。
    /// 输入必须来自同一时间轴；实现可报告不能验证或越界的输入。
    fn remaining(
        &self,
        now: &Self::Instant,
        deadline: &Self::Instant,
    ) -> pi_result::Result<Duration>;
}

/// 默认全局手推时钟（manual clock）的零大小访问能力，支持Copy＋Clone。
///
/// [`Self::new`]核对外部已经安装的同一进程全局实例，不安装或重置已有起点。每次[`ExecutorClock::now`]
/// 通过pi_clock_manual实际刷新，不把try_now的无新鲜度保证缓存用作新截止时间的起点。
/// 外部须先按pi_clock的公开初始化接口准备全局时间轴；本库不接管进程全局安装责任。
/// 正常访问不创建运行时（runtime）或线程，也不为接入增加同步保护。
#[derive(Clone, Copy, Debug)]
pub struct ManualClock;
impl ManualClock {
    /// 核对原全局时间轴已安装；未安装及其它原报告原样返回，不隐式初始化或等待。
    pub fn new() -> pi_result::Result<Self> {
        manual_clock_new_inner()
    }
}
impl ExecutorClock for ManualClock {
    type Instant = GlobalManualInstant;
    fn now(&self) -> pi_result::Result<Self::Instant> {
        manual_clock_now_inner(self)
    }
    fn checked_add(
        &self,
        now: &Self::Instant,
        duration: Duration,
    ) -> pi_result::Result<Self::Instant> {
        manual_clock_add_inner(self, now, duration)
    }
    fn remaining(
        &self,
        now: &Self::Instant,
        deadline: &Self::Instant,
    ) -> pi_result::Result<Duration> {
        manual_clock_remaining_inner(self, now, deadline)
    }
}
include!("clock_inner.rs");
