//! 默认计时驱动（default timer driver）的公开构造与所属线程退出（owner-thread exit）。
//! 使用真实原生来源（native source）及线程通知（thread notification）；时钟（clock）仅以公开协议提供同源固定参考。
use pi_async_executor::{
    timer::{DefaultTimerItem, LocalTimerSubmit},
    DefaultTimerAccess, DefaultTimerDriver, DefaultTimerRegistration, ExecutorClock, ExecutorTimer,
    ThreadParker,
};
use pi_async_runtime_core::{definitions::RuntimeId, time::TimerAccess};
use pi_async_task_queue::{
    timer::{TimerQueueError, TimerQueueHandle, TimerQueueStatus, TimerSubmitError, TimerTaskQueue},
    CancelTimerKey, CancelTimerOptions,
};
use pi_clock_manual::global::GlobalManualInstant;
use std::{
    marker::PhantomData,
    rc::Rc,
    sync::{atomic::{AtomicUsize, Ordering}, Arc},
    time::Duration,
};

/// 时钟边界探针（clock-boundary probe）：本例不推进时间；所有引用共用原时间轴（timeline）。
/// 额外取时（sampling）、克隆（clone）或算术（arithmetic）会在独立计数中显现。
struct ClockProbe {
    now: AtomicUsize,
    clones: AtomicUsize,
    arithmetic: AtomicUsize,
    drops: AtomicUsize,
}
struct FixedClock(Arc<ClockProbe>);
impl Clone for FixedClock {
    fn clone(&self) -> Self {
        self.0.clones.fetch_add(1, Ordering::Relaxed);
        Self(Arc::clone(&self.0))
    }
}
impl Drop for FixedClock {
    fn drop(&mut self) {
        self.0.drops.fetch_add(1, Ordering::Relaxed);
    }
}
impl ExecutorClock for FixedClock {
    type Instant = GlobalManualInstant;
    fn now(&self) -> pi_result::Result<Self::Instant> {
        self.0.now.fetch_add(1, Ordering::Relaxed);
        Ok(GlobalManualInstant::ZERO)
    }
    fn checked_add(&self, now: &Self::Instant, duration: Duration) -> pi_result::Result<Self::Instant> {
        self.0.arithmetic.fetch_add(1, Ordering::Relaxed);
        now.try_add(duration)
    }
    fn remaining(&self, now: &Self::Instant, deadline: &Self::Instant) -> pi_result::Result<Duration> {
        self.0.arithmetic.fetch_add(1, Ordering::Relaxed);
        if deadline <= now { Ok(Duration::ZERO) } else { deadline.try_duration_since(*now) }
    }
}

/// 无本地权限（local permission）的路由类型（route type），明确非Send／非Sync；拒绝始终返还同一原值（original value）。
/// 本例不调用该入口（entry），不以路由（route）代补接收、推进或取消（cancellation）。
struct UnboundRoute(PhantomData<Rc<()>>);
impl LocalTimerSubmit<DefaultTimerItem<FixedClock, Self>> for UnboundRoute {
    fn try_push_local(
        _: RuntimeId,
        _: GlobalManualInstant,
        item: DefaultTimerItem<FixedClock, Self>,
    ) -> pi_result::RawResult<CancelTimerKey, TimerSubmitError<DefaultTimerItem<FixedClock, Self>>> {
        Err(TimerSubmitError { item, cause: TimerQueueError::WrongOwner })
    }
}
fn assert_send_sync<T: Send + Sync>() {}

/// 公开接缝（public seam）：DefaultTimerDriver::new、TimerTaskQueue::handle、ExecutorTimer::timer_access 与正常释放（drop）。
/// 外部共享能力（shared capabilities）仍存续时，真实原生来源（native source）必须先退出；空节点（stub）不算业务登记（registration）。
/// 不声称覆盖非空重试（retry）、活动中退场、分配失败或异常尾部（exceptional tail）。
#[test]
fn test_empty_driver_exits_native_source_before_shared_access_drops() {
    assert_send_sync::<DefaultTimerAccess<FixedClock, UnboundRoute>>();
    assert_send_sync::<DefaultTimerRegistration<FixedClock, UnboundRoute>>();
    let probe = Arc::new(ClockProbe {
        now: AtomicUsize::new(0), clones: AtomicUsize::new(0),
        arithmetic: AtomicUsize::new(0), drops: AtomicUsize::new(0),
    });
    let runtime = RuntimeId::try_new().unwrap();
    let mut parker = ThreadParker::new();
    assert!(parker.take_notification());
    let driver = DefaultTimerDriver::<_, UnboundRoute>::new(
        runtime, FixedClock(Arc::clone(&probe)), parker.notifier(), CancelTimerOptions::default(),
    ).unwrap();
    let native = driver.handle();
    let access = driver.timer_access();
    let copied = access.clone();
    assert_eq!(access.runtime_id(), runtime);
    assert_eq!(copied.runtime_id(), runtime);
    assert!(matches!(native.status(), TimerQueueStatus::Accepting));
    assert_eq!(native.len(), 0);
    assert_eq!(probe.now.load(Ordering::Relaxed), 1);
    assert_eq!(probe.clones.load(Ordering::Relaxed), 0);
    assert_eq!(probe.arithmetic.load(Ordering::Relaxed), 0);
    assert_eq!(probe.drops.load(Ordering::Relaxed), 0);
    assert!(!parker.take_notification());

    drop(driver);
    assert!(matches!(native.status(), TimerQueueStatus::LocalExited));
    assert_eq!(native.len(), 0);
    assert_eq!(access.runtime_id(), runtime);
    assert_eq!(copied.runtime_id(), runtime);
    assert_eq!(probe.drops.load(Ordering::Relaxed), 0);
    drop(access);
    assert_eq!(probe.drops.load(Ordering::Relaxed), 0);
    drop(copied);
    assert_eq!(probe.drops.load(Ordering::Relaxed), 1);
    drop(native);
    assert_eq!(probe.now.load(Ordering::Relaxed), 1);
    assert_eq!(probe.clones.load(Ordering::Relaxed), 0);
    assert_eq!(probe.arithmetic.load(Ordering::Relaxed), 0);
}

/// 只有完成定时任务队列（timer task queue）清理后，才不再需要检查时刻。
#[test]
fn test_next_check_during_bounded_timer_close() {
    use pi_async_runtime_core::time::Timeout;
    use pi_async_task_queue::management::QueueCleanupProgress;
    use std::{future::Future, num::NonZeroUsize, pin::Pin, task::{Context, Waker}};

    let probe = Arc::new(ClockProbe {
        now: AtomicUsize::new(0), clones: AtomicUsize::new(0),
        arithmetic: AtomicUsize::new(0), drops: AtomicUsize::new(0),
    });
    let parker = ThreadParker::new();
    let mut options = CancelTimerOptions::default();
    options.work_batch_size = NonZeroUsize::MIN;
    let mut driver = DefaultTimerDriver::<_, UnboundRoute>::new(
        RuntimeId::try_new().unwrap(), FixedClock(Arc::clone(&probe)), parker.notifier(), options,
    ).unwrap();
    let source = driver.handle();
    let access = driver.timer_access();
    let mut cx = Context::from_waker(Waker::noop());
    let mut waits: Vec<_> = (0..4).map(|_| Timeout::new(access.clone(), Duration::from_millis(20))).collect();
    for wait in &mut waits {
        assert!(Pin::new(wait).poll(&mut cx).is_pending());
    }
    assert_eq!(source.len(), waits.len());

    let now = GlobalManualInstant::ZERO;
    let latest_check = now.try_add(Duration::from_millis(1)).unwrap();
    let samples = probe.now.load(Ordering::Relaxed);
    assert!(matches!(driver.close().unwrap(), QueueCleanupProgress::MoreWork));
    assert!(matches!(source.status(), TimerQueueStatus::Closing));
    assert!(matches!(driver.next_check_at(&now), Ok(Some(check)) if check >= now && check <= latest_check));
    for _ in 0..16 {
        if matches!(driver.close().unwrap(), QueueCleanupProgress::Complete) {
            break;
        }
    }
    assert!(matches!(source.status(), TimerQueueStatus::LocalExited));
    assert_eq!(source.len(), 0);
    assert!(matches!(driver.next_check_at(&now), Ok(None)));
    assert_eq!(probe.now.load(Ordering::Relaxed), samples);
}

use std::alloc::{GlobalAlloc, Layout, System};
use std::{cell::Cell, future::Future, pin::Pin, task::{Context, Waker}};
use pi_async_runtime_core::{definitions::TimerError, time::{Timeout, TimeoutControl}};
use pi_async_task_queue::{management::QueueCleanupProgress, TimerTake};

thread_local! {
    static PI_ASYNC_EXECUTOR_TEST_FAIL_NEXT_CANCEL_ALLOC: Cell<bool> = const { Cell::new(false) };
}

struct FailOneAllocation;

// 安全（safety）。仅在当前测试线程的一次合法分配调用返回空指针。
// 其它分配、释放及其布局（layout）原样交给 System，测试不读取未初始化存储。
unsafe impl GlobalAlloc for FailOneAllocation {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if PI_ASYNC_EXECUTOR_TEST_FAIL_NEXT_CANCEL_ALLOC.try_with(|flag| flag.replace(false)).unwrap_or(false) {
            return std::ptr::null_mut();
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, address: *mut u8, layout: Layout) {
        unsafe { System.dealloc(address, layout) }
    }
}

#[global_allocator]
static TEST_ALLOCATOR: FailOneAllocation = FailOneAllocation;

/// 令同一线程的下一次真实取消节点分配失败，核对公开取消入口返回资源耗尽（ResourceExhausted）。
fn defer_cancellation<A: TimerAccess>(control: &TimeoutControl<A>) {
    PI_ASYNC_EXECUTOR_TEST_FAIL_NEXT_CANCEL_ALLOC.with(|fail_next| fail_next.set(true));
    let result = control.cancel();
    assert!(!PI_ASYNC_EXECUTOR_TEST_FAIL_NEXT_CANCEL_ALLOC.with(|fail_next| fail_next.replace(false)));
    match result {
        Err(TimerError::Queue(TimerQueueError::Backend(report))) => {
            assert_eq!(*report.current_context(), pi_result::ErrorKind::ResourceExhausted);
        }
        other => panic!("预期原生定时取消节点分配失败，实际为 {other:?}"),
    }
}

/// 放弃等待时第一次原生取消失败，随后同一接续的下一次原生分配也失败。
/// 一次有限步骤后仍须保管原责任，第三次以同一原生来源完成取消。
#[test]
fn test_repeated_cancel_report_keeps_the_original_retry_live() {
    let clock = FixedClock(Arc::new(ClockProbe {
        now: AtomicUsize::new(0),
        clones: AtomicUsize::new(0),
        arithmetic: AtomicUsize::new(0),
        drops: AtomicUsize::new(0),
    }));
    let mut parker = ThreadParker::new();
    assert!(parker.take_notification());
    let options = CancelTimerOptions {
        work_batch_size: std::num::NonZeroUsize::MIN,
        ..CancelTimerOptions::default()
    };
    let mut driver = DefaultTimerDriver::<_, UnboundRoute>::new(
        RuntimeId::try_new().unwrap(), clock, parker.notifier(), options,
    ).unwrap();
    let source = driver.handle();
    let mut wait = Timeout::new(driver.timer_access(), Duration::from_secs(1));
    let mut context = Context::from_waker(Waker::noop());
    assert!(Pin::new(&mut wait).poll(&mut context).is_pending());
    assert_eq!(source.len(), 1);
    assert!(parker.take_notification());

    PI_ASYNC_EXECUTOR_TEST_FAIL_NEXT_CANCEL_ALLOC.with(|fail| fail.set(true));
    drop(wait);
    assert!(!PI_ASYNC_EXECUTOR_TEST_FAIL_NEXT_CANCEL_ALLOC.with(|fail| fail.replace(false)));
    assert!(parker.take_notification());
    assert_eq!(source.len(), 1);

    PI_ASYNC_EXECUTOR_TEST_FAIL_NEXT_CANCEL_ALLOC.with(|fail| fail.set(true));
    assert!(matches!(driver.process_cancel_retries().unwrap(), QueueCleanupProgress::MoreWork));
    assert!(!PI_ASYNC_EXECUTOR_TEST_FAIL_NEXT_CANCEL_ALLOC.with(|fail| fail.replace(false)));
    assert!(parker.take_notification());
    assert_eq!(source.len(), 1);

    // 步骤用尽一个节点额度，后一次观察到实际空才确认完整完成。
    assert!(matches!(driver.process_cancel_retries().unwrap(), QueueCleanupProgress::MoreWork));
    assert!(parker.take_notification());
    assert!(matches!(driver.process_cancel_retries().unwrap(), QueueCleanupProgress::Complete));
    for _ in 0..8 {
        if matches!(driver.process_cleanup().unwrap(), QueueCleanupProgress::Complete) {
            break;
        }
    }
    assert_eq!(source.len(), 0);
}

/// 通过原超时控制和真实原生定时任务队列观察待结清的取消接续。
/// 只使真实定时取消节点的一次分配失败，保留原错误和预备的接续节点。
#[test]
fn test_next_check_bounds_deferred_cancel_with_open_empty_native_queue() {
    let clock_probe = Arc::new(ClockProbe {
        now: AtomicUsize::new(0),
        clones: AtomicUsize::new(0),
        arithmetic: AtomicUsize::new(0),
        drops: AtomicUsize::new(0),
    });
    let mut parker = ThreadParker::new();
    assert!(parker.take_notification());
    let mut driver = DefaultTimerDriver::<_, UnboundRoute>::new(
        RuntimeId::try_new().unwrap(),
        FixedClock(Arc::clone(&clock_probe)),
        parker.notifier(),
        CancelTimerOptions::default(),
    ).unwrap();
    let native = driver.handle();
    let interval = Duration::from_millis(1);
    let now = GlobalManualInstant::ZERO.try_add(interval).unwrap();
    let mut wait = Timeout::new(driver.timer_access(), interval);
    let control = wait.control_handle();
    let mut context = Context::from_waker(Waker::noop());
    assert!(Pin::new(&mut wait).poll(&mut context).is_pending());
    assert_eq!(native.len(), 1);

    let mut expired = None;
    for _ in 0..4 {
        driver.advance(&now).map_err(TimerQueueError::into_report).unwrap();
        if let TimerTake::Ready(entry) = driver.try_take_due_one().map_err(TimerQueueError::into_report).unwrap() {
            expired = Some(entry);
            break;
        }
    }
    let expired = expired.expect("原生定时任务队列实际交付原登记");
    assert_eq!(native.len(), 0);
    assert!(matches!(native.status(), TimerQueueStatus::Accepting));
    assert_eq!(
        driver.next_check_at(&now).map_err(TimerQueueError::into_report).unwrap(),
        Some(now.try_add(Duration::from_millis(10)).unwrap()),
    );
    parker.take_notification();

    defer_cancellation(&control);
    assert!(parker.take_notification(), "既有取消接续必须主动通知");
    let samples = clock_probe.now.load(Ordering::Relaxed);
    let observed = driver.next_check_at(&now).map_err(TimerQueueError::into_report).unwrap();
    assert_eq!(clock_probe.now.load(Ordering::Relaxed), samples);
    assert!(!parker.take_notification(), "查询本身不发通知");
    assert!(matches!(driver.process_cancel_retries().unwrap(), QueueCleanupProgress::Complete));
    assert!(parker.take_notification(), "真正接续后的原请求交给同一来源");
    assert!(expired.into_due().is_none());
    assert_eq!(observed, Some(now.try_add(interval).unwrap()), "待结清取消限制最迟复查");
}

/// 经 process_cancel_retries 与 process_cleanup 推进已完成登记的取消接续。
/// 这些登记已结束，只有普通步骤处理满额才会通知。连续两步的未消费通知可合并。
#[test]
fn test_retry_notifies_only_after_processing_the_full_batch() {
    use std::num::NonZeroUsize;

    for (batch, take_first_notice) in [(1, true), (1, false), (3, false)] {
        let mut parker = ThreadParker::new();
        assert!(parker.take_notification());
        let mut options = CancelTimerOptions::default();
        options.work_batch_size = NonZeroUsize::new(batch).unwrap();
        let mut driver = DefaultTimerDriver::<_, UnboundRoute>::new(
            RuntimeId::try_new().unwrap(),
            FixedClock(Arc::new(ClockProbe {
                now: AtomicUsize::new(0),
                clones: AtomicUsize::new(0),
                arithmetic: AtomicUsize::new(0),
                drops: AtomicUsize::new(0),
            })),
            parker.notifier(),
            options,
        ).unwrap();
        let interval = Duration::from_millis(1);
        let now = GlobalManualInstant::ZERO.try_add(interval).unwrap();
        let mut context = Context::from_waker(Waker::noop());
        let mut waits = std::array::from_fn::<_, 2, _>(|_| {
            Timeout::new(driver.timer_access(), interval)
        });
        for wait in &mut waits {
            assert!(Pin::new(wait).poll(&mut context).is_pending());
        }
        assert_eq!(driver.handle().len(), 2);

        let mut expired = Vec::new();
        for _ in 0..8 {
            driver.advance(&now).map_err(TimerQueueError::into_report).unwrap();
            if let TimerTake::Ready(entry) = driver.try_take_due_one().map_err(TimerQueueError::into_report).unwrap() {
                expired.push(entry);
                if expired.len() == waits.len() {
                    break;
                }
            }
        }
        assert_eq!(expired.len(), waits.len(), "原生来源须交出两份原登记");
        assert_eq!(driver.handle().len(), 0);
        for wait in &waits {
            defer_cancellation(&wait.control_handle());
        }
        for entry in expired {
            assert!(entry.into_due().is_none());
        }

        // 原生来源确认已有到期值时，待结清取消不得推迟本次检查。
        let mut urgent = Timeout::new(driver.timer_access(), interval);
        assert!(Pin::new(&mut urgent).poll(&mut context).is_pending());
        driver.advance(&now).map_err(TimerQueueError::into_report).unwrap();
        assert_eq!(driver.next_check_at(&now).map_err(TimerQueueError::into_report).unwrap(), Some(now));
        let TimerTake::Ready(entry) = driver.try_take_due_one().map_err(TimerQueueError::into_report).unwrap() else {
            panic!("原生来源须立即交出到期登记");
        };
        assert!(entry.into_due().is_none());
        assert!(matches!(Pin::new(&mut urgent).poll(&mut context), std::task::Poll::Ready(Ok(()))));
        // 只观察最大可表示时刻的计算，不改变原生来源的参考时刻。
        let latest = GlobalManualInstant::MAX;
        assert_eq!(driver.next_check_at(&latest).map_err(TimerQueueError::into_report).unwrap(), Some(latest));
        assert!(parker.take_notification(), "先消费真正发布时的合并通知");
        assert!(!parker.take_notification());

        let idle_check = now.try_add(Duration::from_millis(10)).unwrap();
        let retry_check = now.try_add(interval).unwrap();
        if batch == 1 {
            assert!(matches!(driver.process_cancel_retries().unwrap(), QueueCleanupProgress::MoreWork));
            assert!(parker.is_notified(), "真实处理满一项必须安排下一次复查");
            if take_first_notice {
                assert!(parker.take_notification());
            }
            assert_eq!(driver.next_check_at(&now).map_err(TimerQueueError::into_report).unwrap(), Some(retry_check));
            assert!(matches!(driver.process_cleanup().unwrap(), QueueCleanupProgress::MoreWork));
            assert!(parker.take_notification(), "最后一项恰好耗尽额度也允许一次复查");
            assert!(!parker.take_notification(), "两个出口的未消费通知可以合并");
        }
        assert!(matches!(driver.process_cancel_retries().unwrap(), QueueCleanupProgress::Complete));
        assert!(!parker.take_notification(), "观察到 Empty 的步骤不再补发通知");
        assert_eq!(driver.next_check_at(&now).map_err(TimerQueueError::into_report).unwrap(), Some(idle_check));
    }
}
