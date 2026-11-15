//! 执行器（executor）的共享控制事实，不保存队列或本地原值。
use crate::parking::ThreadNotifier;
use pi_async_runtime_core::definitions::{
    CloseRequestStatus, CloseState, IdleState, RuntimeStatus,
};
use pi_async_task::Runnable;
use pi_async_task_queue::{QueuePushError, TaskQueue};
use std::sync::atomic::{AtomicU8, AtomicUsize};

/// 实现者共享的控制与必要数量；业务仅经运行时（runtime）只读接口访问。
pub struct ExecutorState {
    flags: AtomicU8,
    idle: AtomicU8,
    pub(crate) internal: AtomicUsize,
    pub(crate) in_flight: AtomicUsize,
}
const CLOSE_REQUESTED: u8 = 1;
const DRIVING: u8 = 2;
const UNAVAILABLE: u8 = 4;
const CLOSE_COMPLETE: u8 = 8;
const CLEANUP_FAILED: u8 = 16;
struct InternalCountGuard<'a> {
    state: &'a ExecutorState,
    armed: bool,
}
impl Drop for InternalCountGuard<'_> {
    fn drop(&mut self) {
        state_count_guard_drop_inner(self)
    }
}
// 单个flags样本解释状态和关闭进度；UNAVAILABLE不自动等于CLOSE_COMPLETE。
// 数量独立取样。internal包括原归属回送和本地可执行持有，跨容器搬运不重计。
// 取消已记录不减in_flight；原Future确认清理及本次责任完成后才减。
impl ExecutorState {
    /// 新的本地入队一次计入internal；拒绝／展开归还预留，原值由具体队列接口返还。
    /// 已经从共享返回交付的原值不能重复调用；其数量随原持有转移。
    pub fn push_back(
        &self,
        queue: &mut TaskQueue,
        task: Runnable,
    ) -> pi_result::RawResult<(), QueuePushError> {
        state_push_inner(self, queue, task, false)
    }
    /// 与push_back相同记账，只影响下一次队首选择，不抢断当前轮询（poll）。
    pub fn push_front(
        &self,
        queue: &mut TaskQueue,
        task: Runnable,
    ) -> pi_result::RawResult<(), QueuePushError> {
        state_push_inner(self, queue, task, true)
    }
    pub fn new() -> Self {
        state_new_inner()
    }
    pub fn status(&self) -> RuntimeStatus {
        state_status_inner(self)
    }
    pub fn close_state(&self) -> CloseState {
        state_close_inner(self)
    }
    pub fn is_driving(&self) -> bool {
        state_driving_inner(self)
    }
    pub fn last_idle_state(&self) -> IdleState {
        state_idle_inner(self)
    }
    pub fn request_close(&self, notifier: &ThreadNotifier) -> CloseRequestStatus {
        state_request_inner(self, notifier)
    }
    pub(crate) fn try_drive(&self) -> bool {
        state_try_drive_inner(self)
    }
    pub(crate) fn release_drive(&self) {
        state_release_drive_inner(self)
    }
    pub(crate) fn record_idle(&self, value: IdleState) {
        state_record_idle_inner(self, value)
    }
    pub(crate) fn finish_owner(&self, failed_in_flight: usize, cleanup_failed: bool) {
        state_finish_owner_inner(self, failed_in_flight, cleanup_failed)
    }
}
include!("state_inner.rs");
