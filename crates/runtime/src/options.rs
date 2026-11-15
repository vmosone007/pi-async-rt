//! 两种默认运行时（runtime）共用的构造选项；不暴露来源实例。
use pi_async_executor::ExecutorOptions;
use pi_async_task_queue::{CancelTimerOptions, MpscQueueOptions};
/// 相互独立的普通／空闲来源、定时工作及轮转选项；没有默认任务数量上限。
#[derive(Clone, Copy)]
pub struct RuntimeOptions {
    /// 普通来源的管理工作与接收CAS额度；与空闲来源相互独立。
    pub external_queue: MpscQueueOptions,
    /// 空闲来源的管理工作与接收CAS额度，不负责判断何时空闲。
    pub idle_queue: MpscQueueOptions,
    /// 原定时来源三个工作额度；保留1ms精度和显式同源时间规则。
    pub timer: CancelTimerOptions,
    /// 事件循环的有限维护、One／All交付、额外重试和被动重查上限。
    pub executor: ExecutorOptions,
}
impl Default for RuntimeOptions {
    fn default() -> Self {
        runtime_options_default_inner()
    }
}
include!("options_inner.rs");
