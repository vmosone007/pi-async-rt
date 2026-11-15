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
    pub(crate) fn claim_forwarding(&self, deferred: bool) -> bool {
        registration_claim_inner(self, deferred)
    }
    pub(crate) fn defer_forwarding(&self) {
        registration_defer_inner(self)
    }
    pub(crate) fn finish_forwarding(&self, forwarded: bool) {
        registration_forwarded_inner(self, forwarded)
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
