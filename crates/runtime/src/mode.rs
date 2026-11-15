//! 两种固定新业务接纳规则；不限制原任务唤醒、定时推进或本地子任务。
use pi_async_executor::drive::{AdmissionRule, AdmissionState};
mod sealed {
    pub trait Sealed {}
}
/// 封闭的运行方式（mode）集合；最终业务用两个别名，不靠覆盖空闲策略改变它。
pub trait RuntimeMode: sealed::Sealed + AdmissionRule + Default + 'static {}
#[derive(Clone, Copy, Debug, Default)]
/// 等本地在途任务和内部待执行任务清零后，才接纳下一普通外部批次。
pub struct SerialMode;
#[derive(Clone, Copy, Debug, Default)]
/// 每轮继续尝试普通来源，已有任务挂起不阻止接纳。
pub struct ConcurrentMode;
impl sealed::Sealed for SerialMode {}
impl sealed::Sealed for ConcurrentMode {}
impl RuntimeMode for SerialMode {}
impl RuntimeMode for ConcurrentMode {}
impl AdmissionRule for SerialMode {
    fn admits_external(&self, state: &AdmissionState) -> bool {
        serial_admit_inner(state)
    }
}
impl AdmissionRule for ConcurrentMode {
    fn admits_external(&self, state: &AdmissionState) -> bool {
        concurrent_admit_inner(state)
    }
}
include!("mode_inner.rs");
