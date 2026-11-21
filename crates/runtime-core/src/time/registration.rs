//! 单次登记的共享事实；取消转交资格与只增事实分开。
use atomic_waker::AtomicWaker;
use std::{cell::UnsafeCell, mem::MaybeUninit, sync::atomic::AtomicU8};

/// 仅控制登记存储是否需要等待结果信号，不授予任务（Task）执行权限。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimerRegistrationKind {
    Task,
    Wait,
}

/// 实现者共同嵌入的登记事实；键（key）只初始化一次，不要求Clone／Copy。
// @trace 1
pub struct TimerRegistration<K: Send + Sync + 'static> {
    // 单调事实：STARTED／KEY_WRITER／KEY_READY／CANCEL_REQUESTED／
    // CANCEL_FORWARDED／RELEASED／INTEREST_CLOSED；无SeqCst依赖。
    facts: AtomicU8,
    key: UnsafeCell<MaybeUninit<K>>,
    completion: TimerCompletion,
    // Idle／Publishing／Deferred／Finished；不混入只增事实字。
    forwarding: AtomicU8,
}
enum TimerCompletion {
    Task,
    Wait {
        outcome: AtomicU8,
        waker: AtomicWaker,
    },
}
// SAFETY: 键（key）通过独占写入资格初始化一次，Release发布后才允许Acquire读取；
// 其它共享成员均为原子或AtomicWaker，最终独占析构仅释放已初始化键。
unsafe impl<K: Send + Sync + 'static> Send for TimerRegistration<K> {}
// SAFETY: 不暴露可写键槽，借用K要求K:Sync，发布与读取协议同上。
unsafe impl<K: Send + Sync + 'static> Sync for TimerRegistration<K> {}
impl<K: Send + Sync + 'static> TimerRegistration<K> {
    const STARTED: u8 = 1;
    const KEY_WRITER: u8 = 2;
    const KEY_READY: u8 = 4;
    const CANCEL_REQUESTED: u8 = 8;
    const CANCEL_FORWARDED: u8 = 16;
    const RELEASED: u8 = 32;
    const INTEREST_CLOSED: u8 = 64;
    /// 准备未登记的基础事实；由实现者与预备重试节点共同持有，不读取时钟。
    pub fn new(kind: TimerRegistrationKind) -> Self {
        registration_new_inner(kind)
    }
    // 内部清理记录请求，不受等待兴趣结束阻挡，不确认实际移除。
    pub(crate) fn request_cancel(&self) -> bool {
        registration_cancel_inner(self)
    }
    // 用户控制必须在同一事实字CAS内同时核兴趣，不能先查询再无条件请求。
    pub(crate) fn request_wait_cancel(&self) -> bool {
        registration_wait_cancel_inner(self)
    }
    // 返回发布时已经存在的取消请求，不能用发布键本身制造取消。
    pub(crate) fn publish_key(&self, key: K) -> bool {
        registration_key_inner(self, key)
    }
    pub(crate) fn key(&self) -> Option<&K> {
        registration_key_ref_inner(self)
    }
    pub(crate) fn begin(&self) -> bool {
        registration_begin_inner(self)
    }
    pub(crate) fn finish(&self, reason: RegistrationEnd) {
        registration_finish_inner(self, reason)
    }
    pub(crate) fn close_interest(&self) {
        registration_interest_inner(self)
    }
    pub(crate) fn poll(&self, cx: &mut std::task::Context<'_>) -> std::task::Poll<RegistrationEnd> {
        registration_poll_inner(self, cx)
    }
    pub(crate) fn claim_forwarding(&self, deferred: bool) -> Option<Confirmation<'_, K>> {
        if registration_claim_inner(self, deferred) {
            Some(Confirmation { record: self })
        } else {
            None
        }
    }
    fn defer_forwarding(&self) {
        registration_defer_inner(self)
    }
    fn finish_forwarding(&self, forwarded: bool) {
        registration_forwarded_inner(self, forwarded)
    }
}
/// 一次性确认同一登记的取消转交，由核心计时登记事实（TimerRegistration）取得转交资格时交出。
///
/// 供计时访问接口（TimerAccess）的实现者在同步调用中消费。它借用原登记，只有一份，
/// 无法从登记自行构造。取得确认并不表示原取消请求已记录到来源或原值（original value）已移除。
#[must_use = "须确认原接续节点接管或已有合法结束责任"]
pub struct Confirmation<'a, K: Send + Sync + 'static> {
    record: &'a TimerRegistration<K>,
}
impl<'a, K: Send + Sync + 'static> Confirmation<'a, K> {
    /// 检查是否是同一登记对象。可在修改预备节点（node）之前使用。
    pub fn is_for(&self, registration: &TimerRegistration<K>) -> bool {
        std::ptr::eq(self.record, registration)
    }

    /// 在同一登记由实际接续者保管之后、原节点（node）可被取出之前，确认待接续。
    /// 若输入关联不同登记，返还同一个确认值，原登记与传入登记均保持原状态。
    pub fn deferred(self, retained: &TimerRegistration<K>) -> pi_result::RawResult<(), Self> {
        if !self.is_for(retained) {
            return Err(self);
        }
        self.record.defer_forwarding();
        Ok(())
    }

    /// 同一来源已有可核的合法移除或关闭接续时结清转交责任。
    /// 返回只确认责任已交接，不报告实际取消成功。
    pub fn source_finished(self) {
        self.record.finish_forwarding(false);
    }

    pub(crate) fn forwarded(self) {
        self.record.finish_forwarding(true);
    }
}
impl<K: Send + Sync + 'static> Drop for TimerRegistration<K> {
    fn drop(&mut self) {
        registration_drop_inner(self)
    }
}
#[derive(Clone, Copy)]
pub(crate) enum RegistrationEnd {
    Elapsed,
    Cancelled,
    Removed,
    DirectFailure,
}

#[repr(u8)]
enum ForwardState {
    Idle = 0,
    Publishing = 1,
    Deferred = 2,
    Finished = 3,
}
#[repr(u8)]
enum WaitOutcome {
    Pending = 0,
    Elapsed = 1,
    Cancelled = 2,
    Removed = 3,
    DirectFailure = 4,
}
include!("registration_inner.rs");
