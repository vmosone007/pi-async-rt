//! 所属线程（owner thread）内的定时登记、发现和清理。

use super::definitions::{
    CancelTimerKey, TaggedRegistrationId, TimerLinks, TimerListEnds, MAX_DEADLINE_NANOS,
};
use super::shared::{TimerRequest, TimerShared};
use crate::management::QueueCleanupProgress;
use crate::notify::QueueNotifier;
use crate::timer::{TimerAdvanceProgress, TimerQueueError, TimerSubmitError, TimerTake};
use indexmap::IndexMap;
use pi_clock_manual::global::GlobalManualInstant;
use std::collections::VecDeque;
use std::num::NonZeroU64;

// 原值只在本地表中拥有一次；原生定时器只存组+登记编号，不存Item或表位置。
// Native -> Due或PendingCleanup；Due -> PendingCleanup；任何移动不减来源数量。
// 两组各有到期FIFO，旧clear组只清理，新组才可业务交付；不能合并两组或扫描旧组找新组。
// 私有清理链覆盖两组PendingCleanup；稳定编号不受IndexMap::swap_remove位置变化影响。
// 所有用户Drop都在表/节点/链接借用结束之后，不把&mut整个拥有者转成裸指针绕过借用。

// @trace 1
pub(super) struct TimerLocalState<Item: 'static> {
    native: pi_cancel_timer::Timer<TaggedRegistrationId, 100, 60, 3, 1>,
    reference: GlobalManualInstant,
    groups: [TimerGroupStorage<Item>; 2],
    cleanup: TimerListEnds,
    staged: VecDeque<Box<TimerRequest<Item>>>,
    clear: TimerClearState<Item>,
}

// @trace 2
struct TimerGroupStorage<Item: 'static> {
    entries: IndexMap<NonZeroU64, TimerRegistration<Item>>,
    due: TimerListEnds,
    due_len: usize,
}

// @trace 3
struct TimerRegistration<Item: 'static> {
    item: Item,
    phase: TimerRegistrationPhase,
}

enum TimerRegistrationPhase {
    Native { key: pi_slot_wheel::TimerKey },
    Due { links: TimerLinks },
    PendingCleanup { links: TimerLinks },
}

// @trace 4
enum TimerClearState<Item: 'static> {
    Available { marker: Box<TimerRequest<Item>> },
    WaitingPublishers { old_group: bool, marker: Box<TimerRequest<Item>> },
    Queued { old_group: bool },
    Reached { old_group: bool, marker: Box<TimerRequest<Item>> },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TimerLocalCleanupMode {
    Ordinary,
    Clear,
    Close,
    OwnerExit,
}

// @trace 5
struct TimerLocalCleanupGuard<'a, Item: 'static, N: QueueNotifier> {
    shared: &'a TimerShared<Item, N>,
    local: &'a mut TimerLocalState<Item>,
    mode: TimerLocalCleanupMode,
    current_debt: bool,
    unsettled: usize,
    armed: bool,
}

impl<Item: 'static> TimerLocalState<Item> {
    pub(super) fn new(now: GlobalManualInstant, marker: Box<TimerRequest<Item>>) -> Self {
        timer_local_new_inner(now, marker)
    }

    // 纯校验必须在推进/接收效果之前，不能把纳秒倒退夹到当前毫秒。
    fn validate_now(&self, now: GlobalManualInstant) -> pi_result::RawResult<(), TimerQueueError> {
        timer_validate_now_inner(self, now)
    }

    fn validate_deadline(deadline: GlobalManualInstant) -> pi_result::RawResult<u64, TimerQueueError> {
        timer_validate_deadline_inner(deadline)
    }

    fn reserve_registration(&mut self, group: bool) -> pi_result::RawResult<(), TimerQueueError> {
        timer_reserve_registration_inner(self, group)
    }

    // 先采用已有staged前缀；空间失败不越过/销毁该前缀，不再取后来请求。
    fn adopt_front<N: QueueNotifier>(
        &mut self,
        shared: &TimerShared<Item, N>,
    ) -> pi_result::RawResult<bool, TimerQueueError> {
        timer_adopt_front_inner(self, shared)
    }

    fn discover_due<N: QueueNotifier>(
        &mut self,
        shared: &TimerShared<Item, N>,
        target: GlobalManualInstant,
    ) -> pi_result::RawResult<bool, TimerQueueError> {
        timer_discover_due_inner(self, shared, target)
    }

    // 只摘原生/到期关系并接清理链，不Drop、不减数；不存在/已取消为无额外效果。
    fn mark_cancel(&mut self, key: CancelTimerKey) {
        timer_mark_cancel_inner(self, key)
    }

    fn clear_step<N: QueueNotifier>(
        &mut self,
        shared: &TimerShared<Item, N>,
    ) -> pi_result::RawResult<QueueCleanupProgress, TimerQueueError> {
        timer_clear_step_inner(self, shared)
    }

    // 只检查本地实际责任；共享发布/安全取空由Shared的窄确认出口核实。
    fn ready_for_source_exit(&self) -> bool {
        timer_local_ready_for_exit_inner(self)
    }

    pub(super) fn try_push<N: QueueNotifier>(
        &mut self,
        shared: &TimerShared<Item, N>,
        deadline: GlobalManualInstant,
        item: Item,
    ) -> pi_result::RawResult<CancelTimerKey, TimerSubmitError<Item>> {
        let mut cleanup = TimerLocalCleanupGuard::new(shared, self, TimerLocalCleanupMode::Ordinary);
        let result = timer_local_push_inner(&mut cleanup, deadline, item);
        cleanup.finish();
        result
    }

    pub(super) fn request_cancel<N: QueueNotifier>(
        &mut self,
        shared: &TimerShared<Item, N>,
        key: &CancelTimerKey,
    ) -> pi_result::RawResult<(), TimerQueueError> {
        let mut cleanup = TimerLocalCleanupGuard::new(shared, self, TimerLocalCleanupMode::Ordinary);
        let result = timer_local_cancel_inner(&mut cleanup, key);
        cleanup.finish();
        result
    }

    pub(super) fn advance<N: QueueNotifier>(
        &mut self,
        shared: &TimerShared<Item, N>,
        now: GlobalManualInstant,
    ) -> pi_result::RawResult<TimerAdvanceProgress, TimerQueueError> {
        self.validate_now(now)?;
        let mut cleanup = TimerLocalCleanupGuard::new(shared, self, TimerLocalCleanupMode::Ordinary);
        let result = timer_local_advance_inner(&mut cleanup, now);
        cleanup.finish();
        result
    }

    pub(super) fn take_one<N: QueueNotifier>(
        &mut self,
        shared: &TimerShared<Item, N>,
    ) -> pi_result::RawResult<TimerTake<Item>, TimerQueueError> {
        let mut cleanup = TimerLocalCleanupGuard::new(shared, self, TimerLocalCleanupMode::Ordinary);
        let result = timer_local_take_one_inner(&mut cleanup);
        cleanup.finish();
        result
    }

    // None仅表示本次尚不能证明完整；Some是已预备容量但尚未持有业务原值的独立Vec。
    // 固定范围证据只属于当前调用；不存入Local或跨调用缓存。
    fn prepare_all<N: QueueNotifier>(
        &mut self,
        shared: &TimerShared<Item, N>,
    ) -> pi_result::RawResult<Option<Vec<Item>>, TimerQueueError> {
        timer_prepare_all_inner(self, shared)
    }

    pub(super) fn take_all<N: QueueNotifier>(
        &mut self,
        shared: &TimerShared<Item, N>,
    ) -> pi_result::RawResult<TimerTake<Vec<Item>>, TimerQueueError> {
        let mut cleanup = TimerLocalCleanupGuard::new(shared, self, TimerLocalCleanupMode::Ordinary);
        let result = match cleanup.local.prepare_all(shared) {
            Ok(Some(values)) => Ok(cleanup.commit_all(values)),
            Ok(None) => Ok(TimerTake::WouldBlock),
            Err(TimerQueueError::Busy) => Ok(TimerTake::Busy),
            Err(TimerQueueError::Closed) => Ok(TimerTake::Closed),
            Err(error) => Err(error),
        };
        cleanup.finish();
        result
    }

    pub(super) fn process_cleanup<N: QueueNotifier>(
        &mut self,
        shared: &TimerShared<Item, N>,
    ) -> pi_result::Result<QueueCleanupProgress> {
        let mut cleanup = TimerLocalCleanupGuard::new(shared, self, TimerLocalCleanupMode::Ordinary);
        let result = timer_local_process_cleanup_inner(&mut cleanup);
        cleanup.finish_progress(result)
    }

    pub(super) fn clear<N: QueueNotifier>(
        &mut self,
        shared: &TimerShared<Item, N>,
    ) -> pi_result::Result<QueueCleanupProgress> {
        let mut cleanup = TimerLocalCleanupGuard::new(shared, self, TimerLocalCleanupMode::Clear);
        let result = timer_local_clear_inner(&mut cleanup);
        cleanup.finish_progress(result)
    }

    pub(super) fn close<N: QueueNotifier>(
        &mut self,
        shared: &TimerShared<Item, N>,
    ) -> pi_result::Result<QueueCleanupProgress> {
        // 永久停收先于任何用户析构；继续先前clear并覆盖停收前接收范围。
        shared.stop_accepting();
        let mut cleanup = TimerLocalCleanupGuard::new(shared, self, TimerLocalCleanupMode::Close);
        let result = timer_local_close_inner(&mut cleanup);
        cleanup.finish_progress(result)
    }

    pub(super) fn next_check_at<N: QueueNotifier>(
        &self,
        shared: &TimerShared<Item, N>,
        now: GlobalManualInstant,
    ) -> pi_result::RawResult<Option<GlobalManualInstant>, TimerQueueError> {
        self.validate_now(now)?;
        timer_next_check_inner(self, shared, now)
    }

    // 调用前外层必须已经原子停收+发布拥有者退出；从此不导出任何本地持有。
    pub(super) fn drop_owner_local<N: QueueNotifier>(&mut self, shared: &TimerShared<Item, N>) {
        let mut cleanup = TimerLocalCleanupGuard::new(shared, self, TimerLocalCleanupMode::OwnerExit);
        timer_local_owner_exit_inner(&mut cleanup);
        cleanup.finish();
        // 正常/展开均由这份守卫的Drop在本地余责结束后确认共享退出。
    }
}

impl<'a, Item: 'static, N: QueueNotifier> TimerLocalCleanupGuard<'a, Item, N> {
    fn new(
        shared: &'a TimerShared<Item, N>,
        local: &'a mut TimerLocalState<Item>,
        mode: TimerLocalCleanupMode,
    ) -> Self {
        timer_local_guard_new_inner(shared, local, mode)
    }

    // 所有用户原值析构必须经过此窄顺序；take_cleanup已摘出原值并建立current_debt，
    // 返回后任何表/节点借用已结束。panic时旧Item被消耗，Drop只结算同一欠账，不重试Item。
    fn drop_one(&mut self) -> bool {
        match self.take_cleanup() {
            Some(item) => {
                drop(item);
                self.settle_current();
                true
            }
            None => false,
        }
    }

    fn take_cleanup(&mut self) -> Option<Item> {
        timer_take_cleanup_inner(self)
    }

    fn settle_current(&mut self) {
        timer_settle_current_inner(self)
    }

    fn commit_all(&mut self, values: Vec<Item>) -> TimerTake<Vec<Item>> {
        timer_commit_all_inner(self, values)
    }

    fn finish(&mut self) {
        timer_local_guard_finish_inner(self)
    }

    // 正常close、停收后的clear及既有清理接续共用最终确认；本地完成不冒充来源完成。
    fn finish_progress(
        &mut self,
        result: pi_result::Result<QueueCleanupProgress>,
    ) -> pi_result::Result<QueueCleanupProgress> {
        self.finish();
        match result {
            Ok(QueueCleanupProgress::Complete) if self.shared.snapshot().stopped => {
                if !self.local.ready_for_source_exit() {
                    return Ok(QueueCleanupProgress::MoreWork);
                }
                match self.shared.try_confirm_closed(&mut self.local.staged) {
                    Ok(true) => Ok(QueueCleanupProgress::Complete),
                    Ok(false) | Err(TimerQueueError::Busy) => Ok(QueueCleanupProgress::MoreWork),
                    Err(error) => Err(error.into_report()),
                }
            }
            other => other,
        }
    }
}

impl<Item: 'static, N: QueueNotifier> Drop for TimerLocalCleanupGuard<'_, Item, N> {
    fn drop(&mut self) {
        // 普通模式余项留在活着的拥有者；OwnerExit必须在原线程完成全部本地余项。
        // 芯只落实这份既有守卫，不新建同类守卫，不再Drop失败当前Item。
        timer_local_guard_drop_inner(self);
        if self.mode == TimerLocalCleanupMode::OwnerExit {
            self.shared.confirm_exit();
        }
    }
}

include!("local_inner.rs");
