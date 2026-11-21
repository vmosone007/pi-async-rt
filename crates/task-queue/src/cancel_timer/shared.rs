//! 共享请求、一次接收及拥有者退出后的有限清理。

use super::definitions::{
    CancelTimerKey, CancelTimerOptions, CLEANUP_REQUESTED,
    COUNT_MASK, CURRENT_GROUP, EXIT_CONFIRMED, MAX_DEADLINE_NANOS, NATIVE_CAPACITY, OWNER_EXITED,
    PUBLISHER_COUNT_MASK, STOPPED,
};
use super::value::SendValue;
use crate::notify::QueueNotifier;
use crate::timer::{TimerQueueError, TimerQueueStatus, TimerSubmitError};
use cordyceps::mpsc_queue::Links;
use cordyceps::Linked;
use pi_clock_manual::global::GlobalManualInstant;
use std::collections::VecDeque;
use std::num::{NonZeroU64, NonZeroUsize};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;

// 共享状态不包含任何本地登记表、定时器或本地Item借用。
// 接收前准备 -> 登记发布参与者 -> 整字受检接收 -> 完整发布 -> Release归还参与
// -> 通知 -> 最新AcqRel退出确认。未接收保留同一原值；接收后不再退回为拒绝。
// 每次真实归还Consumer也必须在权限之外执行最新确认；不得用入操作时的旧样本。
// 来源数量涵盖发布中、请求中、本地暂存及两组登记，实际交出或合法清理之后才减一次。
// 所有发布计数初始化后只用RMW。禁止SeqCst、锁、等待已有参与者或以请求位制造空循环。
// 双组布尔编号false/true对应0/1；身份不用于判断FIFO或固定clear范围。

// @trace 1
pub(super) struct TimerShared<Item: 'static, N: QueueNotifier> {
    queue: cordyceps::MpscQueue<TimerRequest<Item>>,
    state: AtomicUsize,
    // 各组低 63 位记录当前发布参与者（publisher）。实际完成或接收前回滚各归还一次。
    // 最高位由参与者在登记时设置，仅本地拥有者可在开放清理的完成检查前清除。
    // 固定清理（clear）及关闭（close）查询使用低位。逻辑登记数归 state。
    publishers: [AtomicU64; 2],
    queue_id: NonZeroU64,
    next_registration_id: AtomicU64,
    notifier: N,
    options: CancelTimerOptions,
}

// 私有只增身份源；Relaxed CAS只保证唯一编号，不承担业务数据发布。
static NEXT_QUEUE_ID: AtomicU64 = AtomicU64::new(1);

fn next_queue_id() -> pi_result::RawResult<NonZeroU64, TimerQueueError> {
    allocate_queue_id()
}

// @trace 2
pub(super) struct TimerRequest<Item: 'static> {
    links: Links<Self>,
    body: TimerRequestKind<Item>,
}

enum TimerRequestKind<Item: 'static> {
    Submit {
        key: CancelTimerKey,
        group: bool,
        deadline: GlobalManualInstant,
        value: SendValue<Item>,
    },
    Cancel { key: CancelTimerKey },
    EndOfClear { old_group: bool },
    Stub,
}

// 一次Acquire样本的派生值，不另存到Shared，不承诺不同调用同一瞬间。
#[derive(Clone, Copy)]
pub(super) struct TimerSharedSnapshot {
    pub(super) count: usize,
    pub(super) current_group: bool,
    pub(super) stopped: bool,
    pub(super) owner_exited: bool,
    pub(super) cleanup_requested: bool,
    pub(super) exit_confirmed: bool,
}

// 每步局部结果；请求位不等于Progress，真实完成优先于任何迟到请求。
enum TimerSharedCleanupStep {
    Progress,
    Complete,
    Deferred,
    Busy,
}

// @trace 3
pub(super) struct TimerPublishGuard<'a, Item: 'static, N: QueueNotifier> {
    shared: &'a TimerShared<Item, N>,
    group: bool,
    pin_active: bool,
    confirmation_needed: bool,
}

// @trace 4
struct TimerSharedCleanupGuard<'a, Item: 'static, N: QueueNotifier> {
    shared: &'a TimerShared<Item, N>,
    pending: Vec<Box<TimerRequest<Item>>>,
    fallback: Option<Box<TimerRequest<Item>>>,
    current_debt: bool,
    unsettled: usize,
    armed: bool,
}

impl<Item: 'static, N: QueueNotifier> TimerShared<Item, N> {
    // 两个控制节点都准备好才导出来源；失败时已有Box由真实持有者配对释放。
    pub(super) fn new(
        notifier: N,
        options: CancelTimerOptions,
    ) -> pi_result::Result<(Arc<Self>, Box<TimerRequest<Item>>)> {
        let queue_id = next_queue_id().map_err(TimerQueueError::into_report)?;
        let stub = TimerRequest::try_empty().map_err(TimerQueueError::into_report)?;
        let marker = TimerRequest::try_empty().map_err(TimerQueueError::into_report)?;
        Ok((timer_shared_new_inner(queue_id, notifier, options, stub), marker))
    }

    pub(super) fn options(&self) -> CancelTimerOptions {
        timer_shared_options_inner(self)
    }

    pub(super) fn uses_notifier(&self, expected: &N) -> bool
    where
        N: PartialEq,
    {
        timer_shared_uses_notifier_inner(self, expected)
    }

    pub(super) fn snapshot(&self) -> TimerSharedSnapshot {
        timer_shared_snapshot_inner(self)
    }

    pub(super) fn status(&self) -> TimerQueueStatus {
        timer_shared_status_inner(self)
    }

    pub(super) fn len(&self) -> usize {
        timer_shared_len_inner(self)
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // 本地取消的纯同源校验；不发布控制请求，不访问本地表，不暴露来源编号的可写入口。
    pub(super) fn check_key(&self, key: &CancelTimerKey) -> pi_result::RawResult<(), TimerQueueError> {
        timer_check_key_inner(self, key)
    }

    // 此函数只准备，不接收；节点和同一原值都尚可返还。
    fn prepare_submit(
        &self,
        deadline: GlobalManualInstant,
        item: Item,
    ) -> pi_result::RawResult<Box<TimerRequest<Item>>, TimerSubmitError<Item>>
    where
        Item: Send,
    {
        timer_prepare_submit_inner(self, deadline, item)
    }

    pub(super) fn try_push(
        &self,
        deadline: GlobalManualInstant,
        item: Item,
    ) -> pi_result::RawResult<CancelTimerKey, TimerSubmitError<Item>>
    where
        Item: Send,
    {
        let mut node = self.prepare_submit(deadline, item)?;
        let mut publication = match TimerPublishGuard::try_new(self) {
            Ok(guard) => guard,
            Err(cause) => return Err(node.reject_submit(cause)),
        };
        if let Err(cause) = self.admit_submit(publication.group()) {
            return Err(node.reject_submit(cause));
        }
        // 已接收的窄区间：只有原节点元信息更新及原生完整发布，不分配或运行用户代码。
        let key = node.set_submit_group(publication.group());
        self.publish(node);
        publication.published();
        Ok(key)
    }

    pub(super) fn request_cancel(
        &self,
        key: &CancelTimerKey,
    ) -> pi_result::RawResult<(), TimerQueueError> {
        let node = timer_prepare_cancel_inner(self, key)?;
        let mut publication = TimerPublishGuard::try_new(self)?;
        self.admit_cancel(publication.group())?;
        self.publish(node);
        publication.published();
        Ok(())
    }

    // 本地调用只取得接收身份和发布责任，不让非Send原值进入共享节点。
    // 本地表空间须在此前准备；调用者实际装入本地记录后才published。
    pub(super) fn begin_local_submit(
        &self,
    ) -> pi_result::RawResult<(CancelTimerKey, TimerPublishGuard<'_, Item, N>), TimerQueueError> {
        let key = self.next_key()?;
        let publication = TimerPublishGuard::try_new(self)?;
        self.admit_submit(publication.group())?;
        Ok((key, publication))
    }

    fn next_key(&self) -> pi_result::RawResult<CancelTimerKey, TimerQueueError> {
        timer_next_key_inner(self)
    }

    fn admit_submit(&self, group: bool) -> pi_result::RawResult<(), TimerQueueError> {
        timer_admit_submit_inner(self, group)
    }

    fn admit_cancel(&self, group: bool) -> pi_result::RawResult<(), TimerQueueError> {
        timer_admit_cancel_inner(self, group)
    }

    fn publish(&self, node: Box<TimerRequest<Item>>) {
        timer_publish_inner(self, node)
    }

    fn finish_publication(&self, group: bool) {
        timer_finish_publication_inner(self, group)
    }

    pub(super) fn release_originals(&self, count: usize) {
        timer_release_originals_inner(self, count)
    }

    pub(super) fn stop_accepting(&self) {
        timer_stop_accepting_inner(self)
    }

    // 本地已记录请求后也使用同一通知接口；调用前结束本地表/链接借用。
    pub(super) fn notify(&self) {
        self.notifier.notify();
    }

    pub(super) fn begin_owner_exit(&self) {
        timer_begin_owner_exit_inner(self)
    }

    // 只有本地唯一拥有者切组；另一组必须已经空且marker可用。
    pub(super) fn flip_group(&self) -> bool {
        timer_flip_group_inner(self)
    }

    pub(super) fn publishers_finished(&self, group: bool) -> bool {
        timer_publishers_finished_inner(self, group)
    }

    pub(super) fn try_stage_requests(
        &self,
        target: &mut VecDeque<Box<TimerRequest<Item>>>,
        limit: NonZeroUsize,
    ) -> pi_result::RawResult<usize, TimerQueueError> {
        let result = timer_stage_requests_inner(self, target, limit);
        // 叶操作已结束更窄的Consumer作用域；这里不再持有原生权限。
        self.confirm_exit();
        result
    }

    // 仅原所属线程（owner thread）在本轮为开放的普通清理、且已结算本地责任时使用。
    // 成功确认只涵盖本次已取样的接收范围。取到的原请求由 target 暂存到下轮。
    // 原生取出权限（Consumer）归还后，经原 confirm_exit 检查退出接续。
    pub(super) fn try_confirm_open_cleanup(
        &self,
        target: &mut VecDeque<Box<TimerRequest<Item>>>,
        local_count: usize,
    ) -> pi_result::RawResult<bool, TimerQueueError> {
        let result = timer_confirm_open_cleanup_inner(self, target, local_count);
        self.confirm_exit();
        result
    }

    pub(super) fn publish_clear_marker(
        &self,
        marker: Box<TimerRequest<Item>>,
        old_group: bool,
    ) {
        timer_publish_marker_inner(self, marker, old_group);
        self.confirm_exit();
    }

    // 调用前本地已证明两表/原生关联/清理/staged/当前欠账全结束，marker已独立取回。
    // 这里只完成共享侧证据：停收、接收发布结束、可靠零数量及Consumer内真实安全取空。
    // 如仍取到控制节点，直接交给target独立保管并返回false，不能丢弃或假称完成。
    // 不导出Consumer；空间准备在权限前，归还权限后再作最新退出确认。
    pub(super) fn try_confirm_closed(
        &self,
        target: &mut VecDeque<Box<TimerRequest<Item>>>,
    ) -> pi_result::RawResult<bool, TimerQueueError> {
        let result = timer_confirm_closed_inner(self, target);
        self.confirm_exit();
        result
    }

    // 回返及接续关系留在本模块壳中；不从芯隐藏安装递归守卫。
    pub(super) fn confirm_exit(&self) {
        if timer_confirm_exit_inner(self) {
            self.assist_cleanup();
        }
    }

    fn assist_cleanup(&self) {
        let mut cleanup = TimerSharedCleanupGuard::new(self);
        loop {
            match cleanup.step() {
                TimerSharedCleanupStep::Progress => continue,
                TimerSharedCleanupStep::Complete
                | TimerSharedCleanupStep::Deferred
                | TimerSharedCleanupStep::Busy => break,
            }
        }
        cleanup.finish();
    }
}

impl<Item: 'static> TimerRequest<Item> {
    fn try_empty() -> pi_result::RawResult<Box<Self>, TimerQueueError> {
        timer_request_empty_inner()
    }

    fn set_submit_group(&mut self, group: bool) -> CancelTimerKey {
        timer_request_group_inner(self, group)
    }

    fn reject_submit(self: Box<Self>, cause: TimerQueueError) -> TimerSubmitError<Item> {
        timer_request_reject_inner(self, cause)
    }

    // 以下四个出口仅对安全出队后独立持有的Box调用；不导出Item借用或SendValue。
    pub(super) fn submit_info(&self) -> Option<(CancelTimerKey, bool, GlobalManualInstant)> {
        timer_request_submit_info_inner(self)
    }

    pub(super) fn take_submit(&mut self) -> Option<(CancelTimerKey, bool, GlobalManualInstant, Item)> {
        timer_request_take_submit_inner(self)
    }

    pub(super) fn cancel_key(&self) -> Option<CancelTimerKey> {
        timer_request_cancel_key_inner(self)
    }

    pub(super) fn take_end_of_clear(&mut self) -> Option<bool> {
        timer_request_take_end_inner(self)
    }
}

// 安全（safety）：Box交出后旧持有停止析构；仅原生队列的合法归还可恢复一次。
// 请求节点内只可能存在受Send入口约束的SendValue，控制变体不持有Item。
// 链接定位必须以原始地址投影（raw-pointer projection），不临时建立整个节点的可写引用。
// 这是容器回访本项目适配的有界关系，不给予业务回调或本地状态反向访问权限。
unsafe impl<Item: 'static> Linked<Links<Self>> for TimerRequest<Item> {
    type Handle = Box<Self>;

    fn into_ptr(handle: Self::Handle) -> NonNull<Self> {
        timer_request_into_ptr_inner(handle)
    }

    unsafe fn from_ptr(ptr: NonNull<Self>) -> Self::Handle {
        // 安全（safety）：调用者履行原生Linked合法归还且仅归还一次的前提。
        unsafe { timer_request_from_ptr_inner(ptr) }
    }

    unsafe fn links(ptr: NonNull<Self>) -> NonNull<Links<Self>> {
        // 安全（safety）：调用者保证节点存活、固定地址及原生链接访问协议。
        unsafe { timer_request_links_inner(ptr) }
    }
}

impl<'a, Item: 'static, N: QueueNotifier> TimerPublishGuard<'a, Item, N> {
    // 先检查永久停收/退出，已关闭的新调用不得登记或安装守卫，防止用户清理重入助清理。
    // 有界受检登记成功之后立刻建立；正式接收前必须已有守卫，接收CAS仍再次检查状态。
    fn try_new(shared: &'a TimerShared<Item, N>) -> pi_result::RawResult<Self, TimerQueueError> {
        timer_publication_new_inner(shared)
    }

    pub(super) fn group(&self) -> bool {
        timer_publication_group_inner(self)
    }

    pub(super) fn published(&mut self) {
        // 只在完整原生发布或本地记录真正安装后；先释放登记，再通知。
        timer_publication_release_inner(self);
        self.shared.notifier.notify();
        // 若通知恐慌，Drop仍负责同一最新确认，不撤销接收。
    }
}

impl<Item: 'static, N: QueueNotifier> Drop for TimerPublishGuard<'_, Item, N> {
    fn drop(&mut self) {
        if self.pin_active {
            self.shared.finish_publication(self.group);
            self.pin_active = false;
        }
        if self.confirmation_needed {
            self.confirmation_needed = false;
            self.shared.confirm_exit();
        }
    }
}

impl<'a, Item: 'static, N: QueueNotifier> TimerSharedCleanupGuard<'a, Item, N> {
    fn new(shared: &'a TimerShared<Item, N>) -> Self {
        timer_shared_guard_new_inner(shared)
    }

    fn step(&mut self) -> TimerSharedCleanupStep {
        timer_shared_guard_step_inner(self)
    }

    fn finish(&mut self) {
        timer_shared_guard_finish_inner(self)
    }
}

impl<Item: 'static, N: QueueNotifier> Drop for TimerSharedCleanupGuard<'_, Item, N> {
    fn drop(&mut self) {
        // 不从此Drop调用assist_cleanup，不重新安装同类守卫；首次异常原样继续。
        timer_shared_guard_drop_inner(self)
    }
}

include!("shared_inner.rs");
