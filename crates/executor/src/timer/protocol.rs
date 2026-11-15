//! 执行器（executor）连接既有计时来源的窄协议。
use pi_async_runtime_core::{
    definitions::RuntimeId,
    time::{TimerAccess, TimerEntry},
};
use pi_async_task_queue::{
    management::QueueCleanupProgress,
    timer::{TimerSubmitError, TimerTaskQueue},
};
use pi_clock_manual::global::GlobalManualInstant;

/// 当前运行时（runtime）本地设施通过静态方法交付同一原值；实现不持有跨线程本地指针。
///
/// 具体实现先经当前本地权限核RuntimeId、原线程及存续，再短暂借用本地来源；
/// 失败返还原值。此接口不负责公共选择，不创建线程，不把借用保留到用户异步等待。
pub trait LocalTimerSubmit<Item>: 'static {
    fn try_push_local(
        runtime: RuntimeId,
        deadline: GlobalManualInstant,
        item: Item,
    ) -> pi_result::RawResult<pi_async_task_queue::CancelTimerKey, TimerSubmitError<Item>>;
}

/// 既有定时队列（timer queue）与核心计时访问的组合；静态分发，不暴露给普通业务使用者。
///
/// 原TimerTaskQueue仍管理时间、取出和清理；本接口仅增加同源核心访问和有限取消重试。
/// Item精确关联同一Access的登记类型，不把任意队列载荷当核心计时原值。
pub trait ExecutorTimer:
    TimerTaskQueue<
    Item = TimerEntry<
        <Self as TimerTaskQueue>::Key,
        <<Self as ExecutorTimer>::Access as TimerAccess>::Registration,
    >,
>
{
    type Access: TimerAccess<
        Instant = <Self as TimerTaskQueue>::Instant,
        Key = <Self as TimerTaskQueue>::Key,
    >;
    fn timer_access(&self) -> Self::Access;
    /// 在普通定时推进前有限处理预备取消节点；不分配每次重试，不等待键或其它发布者。
    fn process_cancel_retries(&mut self) -> pi_result::Result<QueueCleanupProgress>;
}
