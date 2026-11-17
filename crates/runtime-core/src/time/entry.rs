//! 定时来源唯一保存的原值；实际交付与普通移除明确分开。
use super::registration::{RegistrationEnd, TimerRegistration};
use pi_async_task::Runnable;
use std::marker::PhantomData;

/// 来源保存原任务（Task）或计时信号，不克隆、不创建新的Task。
// @trace 1
/// 普通释放（drop）结束本登记的交付责任，按已有取消事实区分结果，原任务（Task）沿自身规则退出。
/// 来源移除不表示原异步计算（Future）已完成合法析构，也不执行或重新接纳业务。
pub struct TimerEntry<K: Send + Sync + 'static, R: AsRef<TimerRegistration<K>>> {
    registration: R,
    value: EntryValue,
    settled: bool,
    key_type: PhantomData<fn() -> K>,
}
enum EntryValue {
    Task(Option<Runnable>),
    Signal,
}
impl<K: Send + Sync + 'static, R: AsRef<TimerRegistration<K>>> TimerEntry<K, R> {
    pub(crate) fn task(task: Runnable, registration: R) -> Self {
        entry_task_inner(task, registration)
    }
    pub(crate) fn signal(registration: R) -> Self {
        entry_signal_inner(registration)
    }
    /// 仅在来源确认真实到期并交付后使用；信号返回None，业务项交出同一Runnable。
    /// 不接纳、不直接poll、不承诺原Future已完成。
    pub fn into_due(self) -> Option<Runnable> {
        entry_due_inner(self)
    }
    pub(crate) fn recover(self) -> Option<Runnable> {
        entry_recover_inner(self)
    }
}
impl<K: Send + Sync + 'static, R: AsRef<TimerRegistration<K>>> Drop for TimerEntry<K, R> {
    fn drop(&mut self) {
        entry_drop_inner(self)
    }
}
include!("entry_inner.rs");
