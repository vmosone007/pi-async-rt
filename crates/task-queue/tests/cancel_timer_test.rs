//! 经默认可取消定时来源（cancelable timer source）的公开出口验证独立工作额度（work budgets）。
#![cfg(feature = "cancel-timer")]
use pi_async_task_queue::{
    CancelTimerOptions, CancelTimerQueue, CancelTimerQueueHandle, QueueCleanupProgress,
    QueueNotifier, TimerAdvanceProgress, TimerQueueError, TimerQueueHandle, TimerQueueStatus,
    TimerTake, TimerTaskQueue,
};
use pi_clock_manual::global::GlobalManualInstant;
use std::cell::Cell;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// 只用公开纯运算（pure arithmetic）构造同源时刻，不初始化/刷新全局时钟。
fn instant(nanos: u64) -> GlobalManualInstant {
    GlobalManualInstant::try_from_elapsed(Duration::from_nanos(nanos)).unwrap_or_else(|error| panic!("{error:?}"))
}

/// @api try_push_local/advance/try_take_due_one/next_check_at；非Send原值（original value）按1ms向上对齐。
/// 断言（assertion）来源为公开时域和取整约定；同一毫秒内纳秒倒退也必须在产生推进效果前拒绝。
#[test]
fn test_local_deadlines_never_arrive_early_and_raw_time_never_goes_back() {
    let notifications = Arc::new(AtomicUsize::new(0));
    let mut queue = CancelTimerQueue::new(GlobalManualInstant::ZERO,
        CheckNotifier(Arc::clone(&notifications)), CancelTimerOptions::default()).unwrap_or_else(|error| panic!("{error:?}"));
    let original = Rc::new(Cell::new(17));
    let address = Rc::as_ptr(&original);
    queue.try_push_local(instant(1_000_001), original).unwrap_or_else(|_| panic!("合法本地登记（local registration）应接收"));
    assert_eq!(notifications.load(Ordering::Relaxed), 1);
    assert_eq!(queue.len(), 1);
    assert!(matches!(queue.advance(&instant(1_999_999)), Ok(TimerAdvanceProgress::ReachedTarget)));
    assert!(matches!(queue.try_take_due_one(), Ok(TimerTake::Empty)));
    assert!(matches!(queue.advance(&instant(1_999_998)), Err(TimerQueueError::InvalidTime)));
    assert!(matches!(queue.next_check_at(&instant(1_999_998)), Err(TimerQueueError::InvalidTime)));
    assert_eq!(queue.len(), 1);
    assert!(matches!(queue.advance(&instant(2_000_000)), Ok(TimerAdvanceProgress::ReachedTarget)));
    let value = match queue.try_take_due_one().unwrap_or_else(|_| panic!("到期取出（due take）不应报错")) {
        TimerTake::Ready(value) => value, _ => panic!("2ms必须到期（due）"),
    };
    assert_eq!(Rc::as_ptr(&value), address);
    assert_eq!(value.get(), 17);
    assert_eq!(queue.len(), 0);
    assert!(matches!(queue.try_take_due_one(), Ok(TimerTake::Empty)));
    assert!(matches!(queue.close().unwrap_or_else(|error| panic!("{error:?}")), QueueCleanupProgress::Complete));
}

/// @api try_take_due_all；完整到期批次（due batch）不得被普通64项工作额度截断。
/// 全部原值（original values）用本地公开提交预备，断言（assertion）按已接受的70项原始顺序与地址。
#[test]
fn test_all_due_batch_exceeds_work_budget_without_losing_order_or_originals() {
    let mut queue = CancelTimerQueue::new(GlobalManualInstant::ZERO,
        CheckNotifier(Arc::new(AtomicUsize::new(0))), CancelTimerOptions::default()).unwrap_or_else(|error| panic!("{error:?}"));
    let mut addresses = Vec::new();
    for index in 0..70 {
        let value = Box::new(Cell::new(index));
        addresses.push((&*value) as *const Cell<usize>);
        queue.try_push_local(GlobalManualInstant::ZERO, value).unwrap_or_else(|_| panic!("本地提交（local submission）应接收"));
    }
    assert_eq!(queue.len(), 70);
    let values = match queue.try_take_due_all().unwrap_or_else(|_| panic!("批次（batch）不应报错")) {
        TimerTake::Ready(values) => values, _ => panic!("全部已到期（due）应完整交出"),
    };
    assert_eq!(values.len(), 70);
    for (index, value) in values.iter().enumerate() {
        assert_eq!((&**value) as *const Cell<usize>, addresses[index]);
        assert_eq!(value.get(), index);
    }
    assert_eq!(queue.len(), 0);
    assert!(matches!(queue.try_take_due_all(), Ok(TimerTake::Empty)));
    assert!(matches!(queue.close().unwrap_or_else(|error| panic!("{error:?}")), QueueCleanupProgress::Complete));
}

/// 析构观察（drop observation）只计本业务原值被消费，不代补来源数量或清理。
struct CountedValue { id: usize, drops: Arc<AtomicUsize> }
impl Drop for CountedValue {
    fn drop(&mut self) { self.drops.fetch_add(1, Ordering::Relaxed); }
}

/// @api clear/try_push_local/try_take_due_one；未完清理固定旧组，新组仍开放且不得被旧范围误清。
/// 额度1强制真实多次推进，三项原值（original values）只允许两项旧值在清理中析构（drop）。
#[test]
fn test_clear_finishes_only_old_range_and_keeps_new_group_open() {
    let old_drops = Arc::new(AtomicUsize::new(0));
    let new_drops = Arc::new(AtomicUsize::new(0));
    let mut options = CancelTimerOptions::default();
    options.work_batch_size = std::num::NonZeroUsize::new(1).unwrap();
    let mut queue = CancelTimerQueue::new(GlobalManualInstant::ZERO,
        CheckNotifier(Arc::new(AtomicUsize::new(0))), options).unwrap_or_else(|error| panic!("{error:?}"));
    for id in 0..2 {
        queue.try_push_local(instant(1_000_000_000), CountedValue { id, drops: Arc::clone(&old_drops) })
            .unwrap_or_else(|_| panic!("旧登记（old registration）应接收"));
    }
    assert!(matches!(queue.clear().unwrap_or_else(|error| panic!("{error:?}")), QueueCleanupProgress::MoreWork));
    assert_eq!(old_drops.load(Ordering::Relaxed), 1);
    assert_eq!(queue.len(), 1);
    assert!(matches!(queue.status(), TimerQueueStatus::Accepting));
    queue.try_push_local(GlobalManualInstant::ZERO, CountedValue { id: 7, drops: Arc::clone(&new_drops) })
        .unwrap_or_else(|_| panic!("新组（new group）仍应接收"));
    let mut complete = false;
    for _ in 0..4 {
        if matches!(queue.clear().unwrap_or_else(|error| panic!("{error:?}")), QueueCleanupProgress::Complete) {
            complete = true; break;
        }
    }
    assert!(complete);
    assert_eq!(old_drops.load(Ordering::Relaxed), 2);
    assert_eq!(new_drops.load(Ordering::Relaxed), 0);
    assert_eq!(queue.len(), 1);
    let value = match queue.try_take_due_one().unwrap_or_else(|_| panic!("新到期项（new due item）应可取出")) {
        TimerTake::Ready(value) => value, _ => panic!("新组原值（new-group original value）必须保留"),
    };
    assert_eq!(value.id, 7);
    assert_eq!(queue.len(), 0);
    drop(value);
    assert_eq!(new_drops.load(Ordering::Relaxed), 1);
    assert!(matches!(queue.close().unwrap_or_else(|error| panic!("{error:?}")), QueueCleanupProgress::Complete));
}

/// 真实跨线程原值（cross-thread original value）包含Cell，满足Send而不要求Sync。
struct TransferValue { id: Cell<usize>, drops: Arc<AtomicUsize> }
impl Drop for TransferValue {
    fn drop(&mut self) { self.drops.fetch_add(1, Ordering::Relaxed); }
}

/// @api handle.try_push/request_cancel、advance、try_take_due_all、process_cleanup；同一共享FIFO不丢取消顺序。
/// 业务线程（business thread）仅提交真实原值并立即join；所有推进和清理由生产拥有者执行。
#[test]
fn test_cross_thread_submit_and_cancel_preserve_originals_and_fifo() {
    let drops = Arc::new(AtomicUsize::new(0));
    let notifications = Arc::new(AtomicUsize::new(0));
    let mut queue = CancelTimerQueue::new(GlobalManualInstant::ZERO,
        CheckNotifier(Arc::clone(&notifications)), CancelTimerOptions::default()).unwrap_or_else(|error| panic!("{error:?}"));
    let handle = queue.handle();
    let thread_handle = handle.clone();
    let thread_drops = Arc::clone(&drops);
    let addresses = std::thread::spawn(move || {
        let mut addresses = Vec::new();
        let mut cancelled = None;
        for id in 0..3 {
            let value = Box::new(TransferValue { id: Cell::new(id), drops: Arc::clone(&thread_drops) });
            addresses.push((&*value) as *const TransferValue as usize);
            let key = thread_handle.try_push(GlobalManualInstant::ZERO, value)
                .unwrap_or_else(|_| panic!("真实共享提交（shared submission）应接收"));
            if id == 1 { cancelled = Some(key); }
        }
        thread_handle.request_cancel(&cancelled.unwrap()).unwrap_or_else(|_| panic!("同源取消请求（cancellation request）应发布"));
        addresses
    }).join().unwrap();
    assert_eq!(handle.len(), 3);
    assert_eq!(notifications.load(Ordering::Relaxed), 4);
    assert!(matches!(queue.advance(&GlobalManualInstant::ZERO), Ok(TimerAdvanceProgress::ReachedTarget)));
    let values = match queue.try_take_due_all().unwrap_or_else(|_| panic!("到期批次（due batch）不应报错")) {
        TimerTake::Ready(values) => values, _ => panic!("两项未取消原值（uncancelled original values）必须交出"),
    };
    assert_eq!(values.len(), 2);
    assert_eq!(values[0].id.get(), 0);
    assert_eq!(values[1].id.get(), 2);
    assert_eq!((&*values[0]) as *const TransferValue as usize, addresses[0]);
    assert_eq!((&*values[1]) as *const TransferValue as usize, addresses[2]);
    assert_eq!(queue.len(), 1);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    assert!(matches!(queue.process_cleanup().unwrap_or_else(|error| panic!("{error:?}")), QueueCleanupProgress::Complete));
    assert_eq!(queue.len(), 0);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    drop(values);
    assert_eq!(drops.load(Ordering::Relaxed), 3);
    assert!(matches!(queue.close().unwrap_or_else(|error| panic!("{error:?}")), QueueCleanupProgress::Complete));
}

/// @api process_cleanup；SPEC-04：已发布控制请求（control request）不计业务数量，数量齐全不能证明管理责任结束。
/// 额度1时第一次只能采用Submit，FIFO中的Cancel仍未处理，因此必须MoreWork；第二次才清理并Complete。
#[test]
fn test_cleanup_does_not_report_complete_with_published_cancel_still_waiting() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut options = CancelTimerOptions::default();
    options.work_batch_size = std::num::NonZeroUsize::new(1).unwrap();
    let mut queue = CancelTimerQueue::new(GlobalManualInstant::ZERO,
        CheckNotifier(Arc::new(AtomicUsize::new(0))), options).unwrap_or_else(|error| panic!("{error:?}"));
    let handle = queue.handle();
    let key = handle.try_push(instant(1_000_000_000), CountedValue { id: 23, drops: Arc::clone(&drops) })
        .unwrap_or_else(|_| panic!("共享原值（shared original value）应接收"));
    handle.request_cancel(&key).unwrap_or_else(|_| panic!("取消请求（cancellation request）应发布"));
    assert_eq!(queue.len(), 1);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    let first = queue.process_cleanup().unwrap_or_else(|error| panic!("{error:?}"));
    assert!(matches!(first, QueueCleanupProgress::MoreWork), "已发布Cancel尚未采用时不能确认管理完成（management complete）");
    assert_eq!(queue.len(), 1);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    assert!(matches!(queue.process_cleanup().unwrap_or_else(|error| panic!("{error:?}")), QueueCleanupProgress::Complete));
    assert_eq!(queue.len(), 0);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

/// @api request_cancel_local；SPEC-03：已返回的共享登记键（registration key）尚未采用，也不能丢弃本地取消意向。
/// 请求成功不是清理完成，但之后真实采用/清理必须保留已记录请求，不能仍交出该原值（original value）。
#[test]
fn test_local_cancel_keeps_intent_for_shared_submission_not_yet_adopted() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut queue = CancelTimerQueue::new(GlobalManualInstant::ZERO,
        CheckNotifier(Arc::new(AtomicUsize::new(0))), CancelTimerOptions::default()).unwrap_or_else(|error| panic!("{error:?}"));
    let handle = queue.handle();
    let key = handle.try_push(GlobalManualInstant::ZERO, CountedValue { id: 29, drops: Arc::clone(&drops) })
        .unwrap_or_else(|_| panic!("共享提交（shared submission）应接收"));
    assert!(queue.request_cancel_local(&key).is_ok());
    assert_eq!(queue.len(), 1);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    assert!(matches!(queue.advance(&GlobalManualInstant::ZERO), Ok(TimerAdvanceProgress::ReachedTarget)));
    assert!(matches!(queue.try_take_due_one(), Ok(TimerTake::Empty)),
        "已经记录的本地取消意向（local cancellation intent）不能在采用共享提交时丢失");
    assert!(matches!(queue.process_cleanup().unwrap_or_else(|error| panic!("{error:?}")), QueueCleanupProgress::Complete));
    assert_eq!(queue.len(), 0);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

/// 只观察真实公开通知（notification），不代做来源推进或清理。
struct CheckNotifier(Arc<AtomicUsize>);

impl QueueNotifier for CheckNotifier {
    fn notify(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// @api CancelTimerQueue::new/handle/close/Drop；非Send本地类型（local type）不妨碍共享句柄（shared handle）。
/// ZERO与MAX均为合法构造参考；保留句柄（handle）不得阻挡来源责任完成，也不得产生虚假通知（notification）。
#[test]
fn test_empty_cancel_timer_closes_with_non_send_item_and_live_handles() {
    fn require_send_sync<T: Send + Sync>() {}
    require_send_sync::<CancelTimerQueueHandle<Rc<Cell<usize>>, CheckNotifier>>();
    for now in [GlobalManualInstant::ZERO, GlobalManualInstant::MAX] {
        let notifications = Arc::new(AtomicUsize::new(0));
        let mut queue = CancelTimerQueue::<Rc<Cell<usize>>, _>::new(
            now, CheckNotifier(Arc::clone(&notifications)), CancelTimerOptions::default(),
        ).unwrap_or_else(|error| panic!("{error:?}"));
        let handle = queue.handle();
        let copy = handle.clone();
        assert!(matches!(queue.status(), TimerQueueStatus::Accepting));
        assert!(matches!(handle.status(), TimerQueueStatus::Accepting));
        assert_eq!(queue.len(), 0);
        assert!(copy.is_empty());
        assert!(matches!(queue.close().unwrap_or_else(|error| panic!("{error:?}")), QueueCleanupProgress::Complete));
        assert!(matches!(queue.close().unwrap_or_else(|error| panic!("{error:?}")), QueueCleanupProgress::Complete));
        assert!(matches!(copy.status(), TimerQueueStatus::LocalExited));
        drop(queue);
        assert!(matches!(handle.status(), TimerQueueStatus::LocalExited));
        assert_eq!(copy.len(), 0);
        assert_eq!(notifications.load(Ordering::Relaxed), 0);
    }
}

/// @api CancelTimerOptions::default；三类独立非零额度（nonzero budgets）按已定64/64/3构造。
#[test]
fn test_cancel_timer_default_budgets_remain_independent_after_copy() {
    let options = CancelTimerOptions::default();
    let copy = options;
    assert_eq!(options.work_batch_size.get(), 64);
    assert_eq!(options.advance_ticks.get(), 64);
    assert_eq!(copy.atomic_attempts.get(), 3);
}
