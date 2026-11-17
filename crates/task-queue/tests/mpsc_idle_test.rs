//! 经公开空闲来源接口（idle source API）验证原任务（Task）的保管、有限清理和退出责任。
//! 仅驱动真实生产出口，不判断业务空闲，不写入来源私有状态（private state）。
use pi_async_task_queue::{
    IdleQueueManager, IdleQueueStatus, IdleTake, IdleTaskQueue, MpscIdleTaskQueue,
    MpscQueueOptions, QueueCleanupProgress,
};
use std::num::{NonZeroU8, NonZeroUsize};
use pi_async_task::{create, Runnable, Schedule, TaskOptions};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// 队列（queue）不得替业务轮询（poll）或调度（schedule）未绑定原任务（Task）。
struct NoSchedule;
impl Schedule for NoSchedule {
    fn schedule(&self, _: Runnable) { panic!("未绑定任务（Task）不得被来源调度（schedule）"); }
}

/// 记录原异步计算（Future）实际析构（drop），首次异常（panic）只由指定原值产生。
struct DropProbe { drops: Arc<AtomicUsize>, panic: bool }
impl std::future::Future for DropProbe {
    type Output = ();
    fn poll(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        panic!("来源不得业务轮询（poll）");
    }
}
impl Drop for DropProbe {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if self.panic { std::panic::panic_any(971usize); }
    }
}

/// 使用真实创建出口（creation API），不登记、推进或清理来源内部状态（internal state）。
fn make_task(drops: &Arc<AtomicUsize>, panic: bool, options: TaskOptions) -> pi_async_task::TaskParts<()> {
    create(DropProbe { drops: Arc::clone(drops), panic }, NoSchedule, (), options)
        .unwrap_or_else(|_| panic!("真实任务创建（task creation）失败"))
}

/// 最小节点额度（node budget）使固定范围和复用边界可由公开结果确定性观察。
fn one_node_options() -> MpscQueueOptions {
    MpscQueueOptions { cleanup_batch_size: NonZeroUsize::new(1).unwrap(), push_attempts: NonZeroU8::new(3).unwrap() }
}

/// @api MpscIdleTaskQueue::new/handle/Clone/clear/close/Drop；空来源可重复关闭且句柄（handle）不重开。
#[test]
fn test_empty_idle_source_closes_while_shared_handles_survive() {
    let mut queue = MpscIdleTaskQueue::new(MpscQueueOptions::default()).unwrap();
    let handle = queue.handle();
    let clone = handle.clone();
    assert!(matches!(handle.status(), IdleQueueStatus::Accepting));
    assert_eq!(handle.len(), 0);
    assert!(clone.is_empty());
    assert!(matches!(handle.try_take_batch(NonZeroUsize::new(1).unwrap()), Ok(IdleTake::Empty)));
    assert!(matches!(queue.clear().unwrap(), QueueCleanupProgress::Complete));
    assert!(matches!(handle.process_cleanup().unwrap(), QueueCleanupProgress::Complete));
    assert!(matches!(queue.close().unwrap(), QueueCleanupProgress::Complete));
    assert!(matches!(clone.status(), IdleQueueStatus::Closed));
    assert!(matches!(queue.close().unwrap(), QueueCleanupProgress::Complete));
    drop(queue);
    assert!(matches!(handle.status(), IdleQueueStatus::Closed));
    assert!(matches!(handle.try_take_batch(NonZeroUsize::new(1).unwrap()), Ok(IdleTake::Closed)));
    assert_eq!(clone.len(), 0);
}

/// @api IdleTaskQueue::try_push；缺链接（link）和关闭拒绝均返原身份，调用方继续承担原值。
#[test]
fn test_rejected_idle_submission_returns_original_and_owner_drop_cleans_accepted_task() {
    let drops = Arc::new(AtomicUsize::new(0));
    let queue = MpscIdleTaskQueue::new(MpscQueueOptions::default()).unwrap();
    let handle = queue.handle();
    let plain = make_task(&drops, false, TaskOptions::plain());
    let id = plain.runnable.id();
    let error = handle.try_push(plain.runnable).err().expect("必须拒绝缺少链接（link）的原值");
    match error.cause {
        pi_async_task_queue::IdleQueueError::Backend(report) => {
            assert_eq!(report.current_context(), &pi_result::ErrorKind::InvalidInput);
        }
        _ => panic!("缺链接（link）须保留Backend分类"),
    }
    assert_eq!(error.task.id(), id);
    assert_eq!(handle.len(), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(error.task);
    let accepted = make_task(&drops, false, TaskOptions::intrusive_mpsc());
    handle.try_push(accepted.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    assert_eq!(handle.len(), 1);
    drop(queue);
    assert!(matches!(handle.status(), IdleQueueStatus::Closed));
    assert_eq!(handle.len(), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    let late = make_task(&drops, false, TaskOptions::intrusive_mpsc());
    let id = late.runnable.id();
    let error = handle.try_push(late.runnable).err().expect("关闭后必须拒绝");
    assert!(matches!(error.cause, pi_async_task_queue::IdleQueueError::Closed));
    assert_eq!(error.task.id(), id);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    drop(error.task);
    assert_eq!(drops.load(Ordering::SeqCst), 3);
}

/// @api IdleTaskQueue::try_take_batch/len；交付受节点额度（node budget）和调用上限约束，结果独立保活原值。
#[test]
fn test_idle_batch_is_bounded_and_keeps_original_order_and_ownership() {
    let drops = Arc::new(AtomicUsize::new(0));
    let queue = MpscIdleTaskQueue::new(one_node_options()).unwrap();
    let handle = queue.handle();
    let first = make_task(&drops, false, TaskOptions::intrusive_mpsc());
    let second = make_task(&drops, false, TaskOptions::intrusive_local_and_mpsc());
    let ids = [first.runnable.id(), second.runnable.id()];
    handle.try_push(first.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    handle.try_push(second.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    let first_batch = match handle.try_take_batch(NonZeroUsize::new(8).unwrap()) {
        Ok(IdleTake::Ready(batch)) => batch,
        _ => panic!("必须交付首个原值"),
    };
    assert_eq!(first_batch.len(), 1);
    assert_eq!(first_batch[0].id(), ids[0]);
    assert_eq!(handle.len(), 1);
    let second_batch = match handle.try_take_batch(NonZeroUsize::new(1).unwrap()) {
        Ok(IdleTake::Ready(batch)) => batch,
        _ => panic!("必须交付第二个原值"),
    };
    assert_eq!(second_batch.len(), 1);
    assert_eq!(second_batch[0].id(), ids[1]);
    assert!(handle.is_empty());
    drop(queue);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(first_batch);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(second_batch);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

/// @api IdleTaskQueue::process_cleanup；扫描（scan）旋转未取消原值且不减数，只清理真实取消目标。
#[test]
fn test_cancellation_scan_keeps_live_tasks_and_retires_cancelled_task() {
    let drops = Arc::new(AtomicUsize::new(0));
    let queue = MpscIdleTaskQueue::new(one_node_options()).unwrap();
    let handle = queue.handle();
    let first = make_task(&drops, false, TaskOptions::intrusive_mpsc());
    let cancelled = make_task(&drops, false, TaskOptions::intrusive_mpsc());
    let first_id = first.runnable.id();
    handle.try_push(first.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    handle.try_push(cancelled.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    cancelled.control.cancel();
    assert!(matches!(handle.process_cleanup().unwrap(), QueueCleanupProgress::MoreWork));
    assert_eq!(handle.len(), 2);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(matches!(handle.process_cleanup().unwrap(), QueueCleanupProgress::MoreWork));
    assert_eq!(handle.len(), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(matches!(handle.process_cleanup().unwrap(), QueueCleanupProgress::MoreWork));
    assert!(matches!(handle.process_cleanup().unwrap(), QueueCleanupProgress::Complete));
    assert_eq!(handle.len(), 1);
    let batch = match handle.try_take_batch(NonZeroUsize::new(1).unwrap()) {
        Ok(IdleTake::Ready(batch)) => batch,
        _ => panic!("未取消原任务（Task）仍须可取"),
    };
    assert_eq!(batch[0].id(), first_id);
    drop(batch);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

/// @api IdleQueueManager::clear及共享process_cleanup；已有扫描（scan）不能冒充新全清理范围，后来提交留到下一轮。
#[test]
fn test_clear_uses_its_own_endpoint_and_helpers_leave_acknowledgement_to_manager() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut queue = MpscIdleTaskQueue::new(one_node_options()).unwrap();
    let handle = queue.handle();
    let first = make_task(&drops, false, TaskOptions::intrusive_mpsc());
    let second = make_task(&drops, false, TaskOptions::intrusive_mpsc());
    handle.try_push(first.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    handle.try_push(second.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    assert!(matches!(handle.process_cleanup().unwrap(), QueueCleanupProgress::MoreWork));
    let third = make_task(&drops, false, TaskOptions::intrusive_mpsc());
    handle.try_push(third.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    assert!(matches!(queue.clear().unwrap(), QueueCleanupProgress::MoreWork));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let late = make_task(&drops, false, TaskOptions::intrusive_mpsc());
    let late_id = late.runnable.id();
    handle.try_push(late.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    assert!(matches!(handle.try_take_batch(NonZeroUsize::new(1).unwrap()), Ok(IdleTake::WouldBlock)));
    for _ in 0..4 {
        assert!(matches!(handle.process_cleanup().unwrap(), QueueCleanupProgress::MoreWork));
    }
    assert_eq!(drops.load(Ordering::SeqCst), 3);
    assert_eq!(handle.len(), 1);
    assert!(matches!(handle.process_cleanup().unwrap(), QueueCleanupProgress::Complete));
    assert!(matches!(handle.try_take_batch(NonZeroUsize::new(1).unwrap()), Ok(IdleTake::WouldBlock)));
    assert!(matches!(queue.clear().unwrap(), QueueCleanupProgress::Complete));
    let remaining = match handle.try_take_batch(NonZeroUsize::new(1).unwrap()) {
        Ok(IdleTake::Ready(batch)) => batch,
        _ => panic!("原固定范围后的任务（Task）必须保留"),
    };
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].id(), late_id);
    assert_eq!(drops.load(Ordering::SeqCst), 3);
    drop(remaining);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
}

/// @api IdleTaskQueue::try_take_batch；取消目标在普通取出时内部清理，不交给业务或提前归还数量。
#[test]
fn test_ordinary_take_discards_cancelled_task_without_polling_it() {
    let drops = Arc::new(AtomicUsize::new(0));
    let queue = MpscIdleTaskQueue::new(one_node_options()).unwrap();
    let handle = queue.handle();
    let cancelled = make_task(&drops, false, TaskOptions::intrusive_mpsc());
    let live = make_task(&drops, false, TaskOptions::intrusive_mpsc());
    let live_id = live.runnable.id();
    handle.try_push(cancelled.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    handle.try_push(live.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    cancelled.control.cancel();
    assert!(matches!(handle.try_take_batch(NonZeroUsize::new(1).unwrap()), Ok(IdleTake::WouldBlock)));
    assert_eq!(handle.len(), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let batch = match handle.try_take_batch(NonZeroUsize::new(1).unwrap()) {
        Ok(IdleTake::Ready(batch)) => batch,
        _ => panic!("须交付未取消原值"),
    };
    assert_eq!(batch[0].id(), live_id);
    assert_eq!(handle.len(), 0);
    drop(batch);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

/// @api IdleQueueManager::clear；首个或中间原值析构（drop）异常保持原载荷（payload），余项仍恰好处理一次。
#[test]
fn test_clear_panic_preserves_original_payload_and_settles_every_pending_task() {
    for panic_index in [0, 1] {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut queue = MpscIdleTaskQueue::new(MpscQueueOptions::default()).unwrap();
        let handle = queue.handle();
        for index in 0..3 {
            let task = make_task(&drops, index == panic_index, TaskOptions::intrusive_mpsc());
            handle.try_push(task.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
        }
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| queue.clear()))
            .err().expect("原析构（drop）异常必须传播");
        assert_eq!(panic.downcast_ref::<usize>(), Some(&971));
        assert_eq!(drops.load(Ordering::SeqCst), 3);
        assert_eq!(handle.len(), 0);
        assert!(matches!(handle.status(), IdleQueueStatus::Accepting));
        assert!(matches!(queue.clear().unwrap(), QueueCleanupProgress::Complete));
        assert!(matches!(queue.close().unwrap(), QueueCleanupProgress::Complete));
        drop(queue);
        assert_eq!(drops.load(Ordering::SeqCst), 3);
    }
}

/// @api MpscIdleTaskQueue::Drop；首个原值异常后无分配备援（allocation-free fallback）接续尚在来源中的尾部。
#[test]
fn test_owner_drop_panic_finishes_tail_without_waiting_for_shared_handles() {
    let drops = Arc::new(AtomicUsize::new(0));
    let queue = MpscIdleTaskQueue::new(one_node_options()).unwrap();
    let handle = queue.handle();
    for index in 0..3 {
        let task = make_task(&drops, index == 0, TaskOptions::intrusive_mpsc());
        handle.try_push(task.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    }
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(queue)))
        .err().expect("管理者析构（manager drop）须传播原异常");
    assert_eq!(panic.downcast_ref::<usize>(), Some(&971));
    assert_eq!(drops.load(Ordering::SeqCst), 3);
    assert_eq!(handle.len(), 0);
    assert!(matches!(handle.status(), IdleQueueStatus::Closed));
    assert!(matches!(handle.process_cleanup().unwrap(), QueueCleanupProgress::Complete));
}

/// 原值析构（drop）内只通过公开入口（public API）重入（reenter）来源，证明权限先于用户代码归还。
struct ReentrantDrop {
    handle: pi_async_task_queue::MpscIdleTaskQueueHandle,
    drops: Arc<AtomicUsize>,
    observed: Arc<AtomicUsize>,
}
impl std::future::Future for ReentrantDrop {
    type Output = ();
    fn poll(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        panic!("来源不得业务轮询（poll）");
    }
}
impl Drop for ReentrantDrop {
    fn drop(&mut self) {
        assert_eq!(self.handle.len(), 1, "原值清理结束前不得归还数量");
        assert!(matches!(self.handle.process_cleanup(), Ok(QueueCleanupProgress::MoreWork)),
            "权限外清理尚欠账（pending cleanup）时不能Busy或Complete");
        assert_eq!(self.handle.len(), 1);
        self.observed.fetch_add(1, Ordering::SeqCst);
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

/// @api clear/process_cleanup；分类欠账（classified pending cleanup）阻止共享帮助者过早确认范围。
#[test]
fn test_clear_reentrant_cleanup_waits_for_original_drop_to_finish() {
    let drops = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(AtomicUsize::new(0));
    let mut queue = MpscIdleTaskQueue::new(MpscQueueOptions::default()).unwrap();
    let handle = queue.handle();
    let task = create(ReentrantDrop { handle: handle.clone(), drops: Arc::clone(&drops), observed: Arc::clone(&observed) },
        NoSchedule, (), TaskOptions::intrusive_mpsc()).unwrap_or_else(|_| panic!("创建（creation）失败"));
    handle.try_push(task.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    assert!(matches!(queue.clear().unwrap(), QueueCleanupProgress::MoreWork));
    assert_eq!(observed.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(handle.len(), 0);
    assert!(matches!(queue.clear().unwrap(), QueueCleanupProgress::Complete));
}

/// @api try_push/try_take_batch；真实生产线程（producer thread）发布三项后立即join，原身份与次序保持。
#[test]
fn test_cross_thread_idle_submission_keeps_each_original_and_order() {
    let drops = Arc::new(AtomicUsize::new(0));
    let queue = MpscIdleTaskQueue::new(MpscQueueOptions::default()).unwrap();
    let handle = queue.handle();
    let producer = handle.clone();
    let producer_drops = Arc::clone(&drops);
    let ids = std::thread::spawn(move || {
        let mut ids = Vec::new();
        for _ in 0..3 {
            let task = make_task(&producer_drops, false, TaskOptions::intrusive_mpsc());
            ids.push(task.runnable.id());
            producer.try_push(task.runnable).unwrap_or_else(|_| panic!("跨线程提交（cross-thread submission）失败"));
        }
        ids
    }).join().expect("生产线程（producer thread）必须正常结束");
    assert_eq!(handle.len(), 3);
    let batch = match handle.try_take_batch(NonZeroUsize::new(2).unwrap()) {
        Ok(IdleTake::Ready(batch)) => batch,
        _ => panic!("跨线程原值须可取"),
    };
    assert_eq!(batch.iter().map(Runnable::id).collect::<Vec<_>>(), ids[..2]);
    assert_eq!(handle.len(), 1);
    drop(queue);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(matches!(handle.status(), IdleQueueStatus::Closed));
    drop(batch);
    assert_eq!(drops.load(Ordering::SeqCst), 3);
}
