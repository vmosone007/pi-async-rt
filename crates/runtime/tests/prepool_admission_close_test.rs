//! PREPOOL-ADMISSION-001 与 PREPOOL-CLOSE-001 公开行为回归测试。
//! 覆盖：
//! 1. 默认串行运行时（SerialRuntime）首次接纳未绑定普通任务（owner=None）并执行交付结果。
//! 2. 首次推进前请求关闭（Closing）后，run_once 合法完成实际收尾并报告 Closed / Completed。
//! 3. 首次开放推进后请求关闭，run_once 与 run 均真实完成收尾。

use pi_async_executor::timer::DefaultTimerDriver;
use pi_async_executor::{
    DefaultIdlePolicy, ExecutorClock, ExecutorEvent, ExecutorHooks, ExecutorSnapshot, IdlePolicy,
    ManualClock, NoHooks, TaskAction, TaskInfo, TimerDelivery,
};
use pi_async_runtime::{
    binding::RuntimeTag, mode::RuntimeMode, CancellationError, CancellationOutcome,
    CloseRequestStatus, CloseState, ConcurrentMode, ConcurrentRuntime, ConfiguredRuntime, FutureDropStatus, Handle,
    CurrentRuntimeAccessError, IdleDispatch, IdleState, LocalTaskDispatch, RuntimeBlockOn,
    RuntimeContext, RuntimeControl, RuntimeOptions, RuntimeQuery, RuntimeQueues, RuntimeStatus, RuntimeTime,
    SerialMode, SerialRuntime, TaskConfig, TaskDispatch, TerminationKind, TimedTaskDispatch, TimerError,
};
use pi_async_task::control::{ExternalWaitSource, ExternalWaiter};
use pi_async_task_queue::{
    CancelTimerOptions, CancelTimerQueue, ExternalQueueManager, ExternalTaskQueue,
    IdleQueueManager, IdleTaskQueue, MpscIdleTaskQueue, MpscQueueOptions, MpscTaskQueue,
    TimerQueueHandle, TimerQueueStatus, TimerTaskQueue,
};
use pi_clock_manual::global::GlobalManualInstant;
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::panic::{catch_unwind, panic_any, AssertUnwindSafe};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

fn ensure_clock() {
    let _ = pi_clock_manual::global::try_init();
}

/// 测试按明确的时间点推进，与执行机器经过的实际时间无关。
#[derive(Clone)]
struct TestClock(Arc<AtomicU64>);

impl ExecutorClock for TestClock {
    type Instant = GlobalManualInstant;

    fn now(&self) -> pi_result::Result<Self::Instant> {
        GlobalManualInstant::try_from_elapsed(Duration::from_nanos(self.0.load(Ordering::Relaxed)))
    }

    fn checked_add(
        &self,
        now: &Self::Instant,
        duration: Duration,
    ) -> pi_result::Result<Self::Instant> {
        now.try_add(duration)
    }

    fn remaining(
        &self,
        now: &Self::Instant,
        deadline: &Self::Instant,
    ) -> pi_result::Result<Duration> {
        if deadline <= now {
            Ok(Duration::ZERO)
        } else {
            deadline.try_duration_since(*now)
        }
    }
}

struct CountingFuture {
    polls: Arc<AtomicUsize>,
    value: usize,
}

impl Future for CountingFuture {
    type Output = usize;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<usize> {
        self.polls.fetch_add(1, Ordering::Relaxed);
        Poll::Ready(self.value)
    }
}

/// PREPOOL-ADMISSION-001：默认串行运行时接收的普通任务（初始 owner=None）在首次 run_once 中正常接纳并执行一次。
#[test]
fn test_prepool_admission_001_ordinary_first_admission_runs_task_and_delivers_output() {
    ensure_clock();
    let mut rt = SerialRuntime::new().expect("构造默认串行运行时");
    let polls = Arc::new(AtomicUsize::new(0));
    let mut handle = rt
        .spawn(CountingFuture {
            polls: polls.clone(),
            value: 1234,
        })
        .expect("普通提交必须成功");

    assert_eq!(handle.snapshot().owner, None);
    assert!(!handle.snapshot().admitted);

    let turn = rt.run_once().expect("首次推进必须合法接纳未绑定普通任务");
    assert_eq!(turn.runs, 1, "普通任务应被轮询一次");
    assert!(!turn.service_closed);
    assert_eq!(polls.load(Ordering::Relaxed), 1);

    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    match Pin::new(&mut handle).poll(&mut cx) {
        Poll::Ready(Ok(v)) => assert_eq!(v, 1234),
        other => panic!("期望交付结果 1234，实际: {:?}", other),
    }
}

/// PREPOOL-CLOSE-001（首次推进前）：未绑定的空运行时在 request_close 后，run_once 必须合法推进到 Closed / Completed。
#[test]
fn test_prepool_close_001_explicit_close_before_first_turn_completes_shutdown() {
    ensure_clock();
    let mut rt = SerialRuntime::new().expect("构造默认串行运行时");
    assert_eq!(rt.request_close(), CloseRequestStatus::Requested);
    assert_eq!(rt.status(), RuntimeStatus::Closing);
    assert_eq!(rt.close_state(), CloseState::Requested);

    let turn = rt.run_once().expect("首次推进前请求关闭后的 run_once 不得因绑定失败拒绝");
    assert!(turn.service_closed, "显式关闭后应报告 service_closed=true");
    assert_eq!(rt.status(), RuntimeStatus::Closed);
    assert_eq!(rt.close_state(), CloseState::Completed);
}

/// PREPOOL-CLOSE-001（首次推进后及 run 入口）：开放轮转一轮后请求关闭，run_once 与 run 均真实完成收尾。
#[test]
fn test_prepool_close_001_explicit_close_after_open_turn_and_via_run_completes_shutdown() {
    ensure_clock();
    let mut rt1 = SerialRuntime::new().expect("构造默认串行运行时 1");
    let first = rt1.run_once().expect("空开放推进");
    assert!(!first.service_closed);

    assert_eq!(rt1.request_close(), CloseRequestStatus::Requested);
    let closing_turn = rt1.run_once().expect("开放轮转后请求关闭应推进收尾");
    assert!(closing_turn.service_closed, "应真实完成收尾");
    assert_eq!(rt1.status(), RuntimeStatus::Closed);
    assert_eq!(rt1.close_state(), CloseState::Completed);

    // 经 run() 入口验证显式关闭正常返回 Ok(()) 且不永久停放
    let mut rt2 = SerialRuntime::new().expect("构造默认串行运行时 2");
    assert_eq!(rt2.request_close(), CloseRequestStatus::Requested);
    rt2.run().expect("显式关闭后 run() 必须完成收尾并正常返回");
    assert_eq!(rt2.status(), RuntimeStatus::Closed);
    assert_eq!(rt2.close_state(), CloseState::Completed);
}

/// 端到端链路：SerialRuntime 经真实 spawn_timing（覆盖 T88）提交 0ms 定时任务，经首次接纳执行交付结果后再显式关闭。
#[test]
fn test_serial_runtime_timed_spawn_and_explicit_close_end_to_end() {
    let elapsed_nanos = Arc::new(AtomicU64::new(2_100_000));
    let mut rt = SerialRuntime::with_components(
        TestClock(elapsed_nanos.clone()),
        DefaultIdlePolicy,
        NoHooks,
        RuntimeOptions::default(),
    )
    .expect("构造使用指定时钟和默认队列的串行运行时");
    let polls = Arc::new(AtomicUsize::new(0));
    let mut handle = rt
        .spawn_timing(
            CountingFuture {
                polls: polls.clone(),
                value: 2026,
            },
            Duration::ZERO,
        )
        .expect("0ms 定时派发必须真实登记成功");

    // 2.1 毫秒的登记归入第 3 毫秒，轮次数量不能代替计时推进。
    assert_eq!(polls.load(Ordering::Relaxed), 0);
    elapsed_nanos.store(2_999_999, Ordering::Relaxed);
    let before_deadline = rt.run_once().expect("推进到第 3 毫秒前");
    assert_eq!(before_deadline.runs, 0);
    assert_eq!(polls.load(Ordering::Relaxed), 0);
    elapsed_nanos.store(3_000_000, Ordering::Relaxed);

    // 到达真实到期边界后，仍经原来源领取和执行。
    let mut total_runs = 0;
    for _ in 0..4 {
        let turn = rt.run_once().expect("推进定时任务");
        total_runs += turn.runs;
        if polls.load(Ordering::Relaxed) == 1 {
            break;
        }
    }
    assert_eq!(polls.load(Ordering::Relaxed), 1, "定时任务必须真实运行一次");
    assert!(total_runs >= 1);

    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    match Pin::new(&mut handle).poll(&mut cx) {
        Poll::Ready(Ok(v)) => assert_eq!(v, 2026),
        other => panic!("期望定时任务交付结果 2026，实际: {:?}", other),
    }

    assert_eq!(rt.request_close(), CloseRequestStatus::Requested);
    let closed = rt.run_once().expect("显式关闭必须收尾");
    assert!(closed.service_closed);
    assert_eq!(rt.status(), RuntimeStatus::Closed);
    assert_eq!(rt.close_state(), CloseState::Completed);
}

/// 通过公开空闲派发和推进入口验证空闲时任务（idle task）的真实交付。
#[test]
fn test_serial_runtime_idle_task_runs_when_policy_reports_idle() {
    ensure_clock();
    let mut rt = SerialRuntime::new().expect("构造默认串行运行时");
    let polls = Arc::new(AtomicUsize::new(0));
    let mut handle = rt
        .idle(CountingFuture {
            polls: polls.clone(),
            value: 42,
        })
        .expect("空闲来源应接受任务");
    let second = rt
        .idle(CountingFuture {
            polls: polls.clone(),
            value: 43,
        })
        .expect("空闲来源应接受后续任务");
    assert_eq!(rt.last_idle_state(), IdleState::Unknown);
    assert_eq!(polls.load(Ordering::Relaxed), 0);

    let turn = rt.run_once().expect("空闲轮次应正常推进");
    assert_eq!(rt.last_idle_state(), IdleState::Idle);
    assert_eq!(turn.runs, 1);
    assert_eq!(polls.load(Ordering::Relaxed), 1);
    assert_eq!(rt.idle_task_count(), 1);

    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(Pin::new(&mut handle).poll(&mut cx), Poll::Ready(Ok(42))));
    assert_eq!(rt.run_once().expect("下一轮推进空闲任务").runs, 1);
    assert_eq!(polls.load(Ordering::Relaxed), 2);
    assert_eq!(rt.idle_task_count(), 0);
    drop(second);
}

struct KeepChecking;

impl IdlePolicy for KeepChecking {
    fn is_idle(&mut self, _state: &ExecutorSnapshot<'_>) -> bool {
        false
    }

    fn sleep_duration(&mut self, _state: &ExecutorSnapshot<'_>) -> Option<Duration> {
        None
    }
}

/// 策略（policy）可以拒绝空闲任务，并要求下一轮重查。
#[test]
fn test_runtime_observes_idle_and_sleep_policy() {
    let mut rt = SerialRuntime::with_components(
        TestClock(Arc::new(AtomicU64::new(0))),
        KeepChecking,
        NoHooks,
        RuntimeOptions::default(),
    )
    .expect("构造使用自定义空闲策略的串行运行时");
    let polls = Arc::new(AtomicUsize::new(0));
    let _handle = rt
        .idle(CountingFuture {
            polls: polls.clone(),
            value: 1,
        })
        .expect("空闲提交独立于策略判断");
    let turn = rt.run_once().expect("空闲策略允许轮转但拒绝空闲派发");
    assert_eq!(rt.last_idle_state(), IdleState::NotIdle);
    assert_eq!(turn.sleep_for, None);
    assert_eq!(turn.runs, 0);
    assert_eq!(rt.idle_task_count(), 1);
    assert_eq!(polls.load(Ordering::Relaxed), 0);
}

/// 单次报告由一个写者写入，接收线程在到达期限前等待结果。
struct TestReport<T> {
    value: OnceLock<T>,
    receiver: thread::Thread,
}

impl<T> TestReport<T> {
    fn new() -> Arc<Self> {
        Arc::new(Self { value: OnceLock::new(), receiver: thread::current() })
    }

    fn publish(&self, value: T) {
        assert!(self.value.set(value).is_ok(), "同一测试结果只报告一次");
        self.receiver.unpark();
    }

    fn receive(&self) -> &T {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(value) = self.value.get() {
                return value;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero(), "在期限内取得测试结果");
            thread::park_timeout(remaining);
        }
    }
}

/// 任务（Task）初次挂起时交出真实唤醒器（Waker），随后只允许原线程继续推进。
struct WakeFromAnotherThread {
    owner: thread::ThreadId,
    report: Option<Arc<TestReport<Waker>>>,
}

impl Future for WakeFromAnotherThread {
    type Output = thread::ThreadId;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert_eq!(thread::current().id(), self.owner);
        match self.report.take() {
            Some(report) => {
                report.publish(cx.waker().clone());
                Poll::Pending
            }
            None => Poll::Ready(thread::current().id()),
        }
    }
}

struct CountDrop(Arc<AtomicUsize>);

impl Drop for CountDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// 本测试保有外部等待者（external waiter）身份，直到领取取消结果。
struct TestWaiter;

impl ExternalWaitSource for TestWaiter {
    fn is_alive(&self) -> bool {
        true
    }
}

fn check_selected_queues<M: RuntimeMode>() {
    let elapsed_nanos = Arc::new(AtomicU64::new(0));
    let external = MpscTaskQueue::new(MpscQueueOptions::default()).unwrap();
    let external_handle = external.handle();
    let idle = MpscIdleTaskQueue::new(MpscQueueOptions::default()).unwrap();
    let idle_handle = idle.handle();
    let mut timer_handle = None;
    let queues = RuntimeQueues::default()
        .with_external(external)
        .with_idle(idle)
        .with_timer(|id, clock: &TestClock, notifier| {
            let queue = CancelTimerQueue::new(
                clock.now()?,
                notifier.clone(),
                CancelTimerOptions::default(),
            )?;
            timer_handle = Some(queue.handle());
            DefaultTimerDriver::<_, RuntimeTag<TestClock>>::from_queue(
                id, clock.clone(), notifier, queue,
            )
        });
    let mut rt = ConfiguredRuntime::<M, _, _, _>::with_components_and_queues(
        TestClock(elapsed_nanos.clone()),
        DefaultIdlePolicy,
        NoHooks,
        RuntimeOptions::default(),
        queues,
    )
    .unwrap();
    let timer_handle = timer_handle.expect("定时来源必须在所属线程完成绑定");
    let owner = thread::current().id();
    let recv_waker = TestReport::new();
    let send_waker = recv_waker.clone();
    let shared = rt.handle();
    let mut task = thread::spawn(move || {
        shared
            .spawn(WakeFromAnotherThread { owner, report: Some(send_waker) })
            .expect("共享句柄允许跨线程投递")
    })
    .join()
    .unwrap();
    assert_eq!(external_handle.len(), 1);
    assert_eq!(task.snapshot().owner, None);
    assert_eq!(rt.run_once().unwrap().runs, 1);
    assert_eq!(external_handle.len(), 0);
    let bound_owner = task.snapshot().owner.expect("任务应已绑定所属域");
    let waker = recv_waker.receive().clone();
    assert_eq!(rt.waiting_for_wake_count(), 1);
    let late_waker = waker.clone();
    thread::spawn(move || waker.wake()).join().unwrap();
    assert_eq!(rt.waiting_for_wake_count(), 0);
    assert_eq!(rt.run_once().unwrap().runs, 1);
    assert_eq!(rt.internal_task_count(), 0, "原归属回送只承担一份内部任务计数");
    assert_eq!(task.snapshot().owner, Some(bound_owner));
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(Ok(actual_owner)) = Pin::new(&mut task).poll(&mut cx) else {
        panic!("被唤醒的同一任务应交付结果");
    };
    assert_eq!(owner, actual_owner);

    let drops = Arc::new(AtomicUsize::new(0));
    let guard = CountDrop(drops.clone());
    let delayed = rt
        .spawn_timing(
            async move {
                drop(guard);
                panic!("已取消的较晚定时任务不能轮询");
            },
            Duration::from_secs(1),
        )
        .unwrap();
    let mut early = rt.spawn_timing(async { 7 }, Duration::from_millis(2)).unwrap();
    assert_eq!(timer_handle.len(), 2);
    let control = delayed.control_handle();
    let mut cancel_wait = thread::spawn(move || {
        let waiter = ExternalWaiter::new(Arc::new(TestWaiter));
        let mut wait = control.cancel_and_wait_from(&waiter);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(Pin::new(&mut wait).poll(&mut cx).is_pending());
        wait
    })
    .join()
    .unwrap();
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    assert_eq!(rt.run_once().unwrap().runs, 0);
    assert_eq!(timer_handle.len(), 1);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(delayed.snapshot().future_drop, FutureDropStatus::Confirmed);
    // T66。原任务从未被接纳，来源移除并释放时应保留放弃（Abandoned）分类。
    assert_eq!(delayed.snapshot().termination, Some(TerminationKind::Abandoned));
    assert_eq!(
        Pin::new(&mut cancel_wait).poll(&mut cx),
        Poll::Ready(CancellationOutcome::Failed(CancellationError::TargetAbandoned)),
    );

    elapsed_nanos.store(2_000_000, Ordering::Relaxed);
    assert_eq!(rt.run_once().unwrap().runs, 1);
    assert!(matches!(Pin::new(&mut early).poll(&mut cx), Poll::Ready(Ok(7))));
    assert_eq!(timer_handle.len(), 0);

    let mut idle_task = rt.idle(async { 13 }).unwrap();
    assert_eq!(idle_handle.len(), 1);
    assert_eq!(rt.run_once().unwrap().runs, 1);
    assert!(matches!(Pin::new(&mut idle_task).poll(&mut cx), Poll::Ready(Ok(13))));
    assert_eq!(idle_handle.len(), 0);

    let shared = rt.handle();
    thread::spawn(move || {
        assert_eq!(shared.request_close(), CloseRequestStatus::Requested);
    })
    .join()
    .unwrap();
    assert!(rt.run_once().unwrap().service_closed);
    assert_eq!(rt.close_state(), CloseState::Completed);
    assert!(matches!(timer_handle.status(), TimerQueueStatus::LocalExited));
    thread::spawn(move || late_waker.wake()).join().unwrap();
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

/// 实际串行运行时（serial runtime）装配三种选配队列，覆盖跨线程派发、唤醒、取消和关闭。
#[test]
fn test_serial_runtime_selected_queues() {
    check_selected_queues::<SerialMode>();
}

/// 实际单线程并发运行时（single-threaded concurrent runtime）使用同一组公开判据。
#[test]
fn test_concurrent_runtime_selected_queues() {
    check_selected_queues::<ConcurrentMode>();
}

struct CloseBeforePoll {
    handle: Rc<RefCell<Option<Handle>>>,
    controls: Rc<Cell<usize>>,
}

impl ExecutorHooks for CloseBeforePoll {
    fn on_event(&mut self, event: ExecutorEvent, _task: &TaskInfo<'_>) {
        if event == ExecutorEvent::BeforePoll {
            let shared = self.handle.borrow();
            assert_eq!(shared.as_ref().unwrap().request_close(), CloseRequestStatus::Requested);
        }
    }

    fn before_poll(&mut self, _task: &TaskInfo<'_>) -> TaskAction {
        self.controls.set(self.controls.get() + 1);
        TaskAction::Poll
    }
}

/// 通知回调请求关闭后，当前轮不再调用控制钩子（hook）或轮询（poll）原任务。
#[test]
fn test_before_poll_notification_can_close_before_control_or_poll() {
    ensure_clock();
    let handle = Rc::new(RefCell::new(None));
    let controls = Rc::new(Cell::new(0));
    let mut rt = SerialRuntime::with_components(
        ManualClock::new().unwrap(),
        DefaultIdlePolicy,
        CloseBeforePoll { handle: handle.clone(), controls: controls.clone() },
        RuntimeOptions::default(),
    )
    .unwrap();
    *handle.borrow_mut() = Some(rt.handle());
    let polls = Arc::new(AtomicUsize::new(0));
    let task = rt.spawn(CountingFuture { polls: polls.clone(), value: 1 }).unwrap();
    let turn = rt.run_once().unwrap();
    assert_eq!(controls.get(), 0);
    assert_eq!(polls.load(Ordering::Relaxed), 0);
    assert_eq!(turn.runs, 0);
    assert!(turn.service_closed);
    assert_eq!(rt.close_state(), CloseState::Completed);
    assert_eq!(task.snapshot().future_drop, FutureDropStatus::Confirmed);
}

struct DeferOnce(bool);

impl ExecutorHooks for DeferOnce {
    fn before_poll(&mut self, _task: &TaskInfo<'_>) -> TaskAction {
        if std::mem::replace(&mut self.0, false) {
            TaskAction::Defer
        } else {
            TaskAction::Poll
        }
    }
}

/// 单项定时交付被延后时，本轮即使从内部队首重新取到它，也只留到下一轮执行。
#[test]
fn test_one_timer_deferral_waits_until_next_turn() {
    let mut options = RuntimeOptions::default();
    options.executor.timer_delivery = TimerDelivery::One;
    let mut rt = SerialRuntime::with_components(
        TestClock(Arc::new(AtomicU64::new(0))),
        DefaultIdlePolicy,
        DeferOnce(true),
        options,
    )
    .unwrap();
    let polls = Arc::new(AtomicUsize::new(0));
    let mut task = rt
        .spawn_timing(CountingFuture { polls: polls.clone(), value: 4 }, Duration::ZERO)
        .unwrap();
    assert_eq!(rt.run_once().unwrap().runs, 0);
    assert_eq!(polls.load(Ordering::Relaxed), 0);
    assert_eq!(rt.internal_task_count(), 1);
    assert_eq!(rt.run_once().unwrap().runs, 1);
    assert_eq!(polls.load(Ordering::Relaxed), 1);
    assert_eq!(rt.internal_task_count(), 0);
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(Pin::new(&mut task).poll(&mut cx), Poll::Ready(Ok(4))));
}

#[derive(Clone)]
struct FailingRemainingClock(TestClock);

impl ExecutorClock for FailingRemainingClock {
    type Instant = GlobalManualInstant;

    fn now(&self) -> pi_result::Result<GlobalManualInstant> {
        self.0.now()
    }

    fn checked_add(&self, now: &GlobalManualInstant, duration: Duration) -> pi_result::Result<GlobalManualInstant> {
        self.0.checked_add(now, duration)
    }

    fn remaining(&self, _: &GlobalManualInstant, _: &GlobalManualInstant) -> pi_result::Result<Duration> {
        Err(pi_result::Error::new(pi_result::ErrorKind::Unavailable).attach("测试时钟的剩余时长不可用"))
    }
}

/// 先交付已经取出的到期值，再返回后续计算剩余时长的原错误。
#[test]
fn test_due_wait_survives_later_clock_error() {
    use pi_result::ErrorInspectExt;

    for delivery in [TimerDelivery::One, TimerDelivery::All] {
        let elapsed = Arc::new(AtomicU64::new(0));
        let mut options = RuntimeOptions::default();
        options.executor.timer_delivery = delivery;
        let mut rt = SerialRuntime::with_components(
            FailingRemainingClock(TestClock(elapsed.clone())), DefaultIdlePolicy, NoHooks, options,
        ).unwrap();
        let mut wait = rt.timeout(Duration::from_millis(1));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(Pin::new(&mut wait).poll(&mut cx).is_pending());
        elapsed.store(1_000_000, Ordering::Relaxed);

        let Err(error) = rt.run_once() else { panic!("必须保留计算剩余时长的原错误") };
        assert_eq!(error.current_kind(), &pi_result::ErrorKind::Unavailable);
        assert_eq!(error.attachment_ref::<&'static str>(), Some(&"测试时钟的剩余时长不可用"));
        assert!(matches!(Pin::new(&mut wait).poll(&mut cx), Poll::Ready(Ok(()))));
    }
}

/// 在默认全量交付中，第一项到期等待的通知请求关闭，后续原任务必须析构且保留其异常。
#[test]
fn test_close_from_due_notification_preserves_later_drop_failure() {
    assert_close_from_due_notification(false);
}

/// 唤醒器请求关闭后自身展开时，先结束同一批未移交的原定时任务，再确认退出中的失败。
#[test]
fn test_due_notification_panic_completes_the_remaining_close_steps() {
    assert_close_from_due_notification(true);
}

// 以同一组到期值核对两个故障位置。每次只让一处发生原恐慌。
fn assert_close_from_due_notification(panic_in_waker: bool) {
    struct RequestClose(
        std::sync::Weak<pi_async_runtime::RuntimeHandle<TestClock>>,
        Arc<AtomicUsize>,
        bool,
    );
    impl Wake for RequestClose {
        fn wake(self: Arc<Self>) {
            self.1.fetch_add(1, Ordering::Relaxed);
            assert_eq!(self.0.upgrade().unwrap().request_close(), CloseRequestStatus::Requested);
            if self.2 {
                panic_any(274u32);
            }
        }
    }
    struct PanicOnDrop(Arc<AtomicUsize>, bool);
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
            if self.1 {
                panic_any(274u32);
            }
        }
    }

    let elapsed = Arc::new(AtomicU64::new(0));
    let mut options = RuntimeOptions::default();
    options.executor.timer_delivery = TimerDelivery::All;
    let mut rt = SerialRuntime::with_components(
        TestClock(elapsed.clone()), DefaultIdlePolicy, NoHooks, options,
    ).unwrap();
    let access = Arc::new(rt.handle());
    let notification = Arc::new(AtomicUsize::new(0));
    let waker = Waker::from(Arc::new(RequestClose(
        Arc::downgrade(&access), notification.clone(), panic_in_waker,
    )));
    let mut wait = rt.timeout(Duration::from_millis(1));
    let mut cx = Context::from_waker(&waker);
    assert!(Pin::new(&mut wait).poll(&mut cx).is_pending());
    assert!(Pin::new(&mut wait).poll(&mut cx).is_pending());
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let original = PanicOnDrop(drops.clone(), !panic_in_waker);
    let _later = rt.spawn_timing({
        let polls = polls.clone();
        async move {
            polls.fetch_add(1, Ordering::Relaxed);
            drop(original);
        }
    }, Duration::from_millis(2)).unwrap();
    assert_eq!(access.timer_registration_count(), 2);

    elapsed.store(2_000_000, Ordering::Relaxed);
    let failed = catch_unwind(AssertUnwindSafe(|| { rt.run_once().unwrap(); }));
    assert_eq!(failed.expect_err("保留首个析构恐慌").downcast_ref::<u32>(), Some(&274));
    assert_eq!(notification.load(Ordering::Relaxed), 1);
    assert_eq!(polls.load(Ordering::Relaxed), 0);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(access.status(), RuntimeStatus::Closed);
    assert_eq!(access.close_state(), CloseState::Requested);
    assert_eq!(access.timer_registration_count(), 0);
    assert!(rt.run_once().unwrap().service_closed);
    assert!(matches!(Pin::new(&mut wait).poll(&mut cx), Poll::Ready(Ok(()))));
}

/// 释放拥有者时实际关闭四类任务队列，并在共享句柄仍存在时确认所有原值的释放。
#[test]
fn test_runtime_owner_drop_releases_four_queues_before_reporting_completion() {
    ensure_clock();
    let mut rt = SerialRuntime::new().unwrap();
    let shared = rt.handle();
    let drops = Arc::new(AtomicUsize::new(0));
    let active = CountDrop(drops.clone());
    let internal = CountDrop(drops.clone());
    let _running = rt.spawn(async move {
        drop(SerialRuntime::spawn_local(async move { drop(internal) }).unwrap());
        std::future::pending::<()>().await;
        drop(active);
    }).unwrap();
    assert_eq!(rt.run_once().unwrap().runs, 1);

    let external = CountDrop(drops.clone());
    let _external = shared.spawn(async move { drop(external) }).unwrap();
    let idle = CountDrop(drops.clone());
    let _idle = shared.idle(async move { drop(idle) }).unwrap();
    let timed = CountDrop(drops.clone());
    let _timed = shared.spawn_timing(async move { drop(timed) }, Duration::from_secs(60)).unwrap();

    assert_eq!(shared.external_task_count(), 1);
    assert_eq!(shared.internal_task_count(), 1);
    assert_eq!(shared.idle_task_count(), 1);
    assert_eq!(shared.timer_registration_count(), 1);
    assert_eq!(shared.in_flight_task_count(), 1);
    assert_eq!(drops.load(Ordering::Relaxed), 0);

    drop(rt);
    assert_eq!(drops.load(Ordering::Relaxed), 5);
    assert_eq!(shared.external_task_count(), 0);
    assert_eq!(shared.internal_task_count(), 0);
    assert_eq!(shared.idle_task_count(), 0);
    assert_eq!(shared.timer_registration_count(), 0);
    assert_eq!(shared.in_flight_task_count(), 0);
    assert_eq!(shared.status(), RuntimeStatus::Closed);
    assert_eq!(shared.close_state(), CloseState::Completed);
}

struct PanicOnDrop(Arc<AtomicUsize>);

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
        panic_any(274u32);
    }
}

/// 外部来源清理发生首个恐慌（panic）时，本次显式关闭仍继续清理空闲和定时来源。
#[test]
fn test_explicit_close_keeps_cleaning_sources_after_first_drop_panic() {
    ensure_clock();
    let mut rt = SerialRuntime::new().unwrap();
    let external_drops = Arc::new(AtomicUsize::new(0));
    let idle_drops = Arc::new(AtomicUsize::new(0));
    let timer_drops = Arc::new(AtomicUsize::new(0));
    let external = PanicOnDrop(external_drops.clone());
    let _external = rt.spawn(async move { drop(external) }).unwrap();
    let idle = CountDrop(idle_drops.clone());
    let _idle = rt.idle(async move { drop(idle) }).unwrap();
    let timer = CountDrop(timer_drops.clone());
    let _timer = rt.spawn_timing(async move { drop(timer) }, Duration::from_secs(60)).unwrap();
    assert_eq!(rt.request_close(), CloseRequestStatus::Requested);

    let payload = catch_unwind(AssertUnwindSafe(|| rt.run_once())).unwrap_err();
    assert_eq!(payload.downcast_ref::<u32>(), Some(&274));
    assert_eq!(external_drops.load(Ordering::Relaxed), 1);
    assert_eq!(idle_drops.load(Ordering::Relaxed), 1);
    assert_eq!(timer_drops.load(Ordering::Relaxed), 1);
    assert_eq!(rt.status(), RuntimeStatus::Closed);
    assert_eq!(rt.close_state(), CloseState::Requested);
}

/// `block_on` 推进独立根时仍选择原运行时，以同一公开查询入口观察编号及关闭事实。
#[test]
fn test_root_block_on_selects_shared_and_closed_runtime() {
    ensure_clock();
    let mut rt = SerialRuntime::new().unwrap();
    let handle = rt.handle();
    let id = handle.id();
    assert_eq!(handle.block_on(async { Handle::try_current().unwrap().id() }), id);
    assert!(matches!(Handle::try_current(), Err(CurrentRuntimeAccessError::NoCurrentRuntime)));

    assert_eq!(rt.request_close(), CloseRequestStatus::Requested);
    assert!(rt.run_once().unwrap().service_closed);
    let closed = rt.block_on(async {
        let current = Handle::try_current().unwrap();
        (current.id(), current.status(), current.close_state())
    });
    assert_eq!(closed, (id, RuntimeStatus::Closed, CloseState::Completed));
    assert!(matches!(Handle::try_current(), Err(CurrentRuntimeAccessError::NoCurrentRuntime)));
}

/// 已选择另一运行时的同步范围中，首次轮询（poll）前返还原冲突。
#[test]
fn test_shared_block_on_rejects_other_current_runtime_before_poll() {
    ensure_clock();
    let rt = SerialRuntime::new().unwrap();
    let other = SerialRuntime::new().unwrap();
    let polls = Arc::new(AtomicUsize::new(0));
    other.enter(|| {
        let rejected = catch_unwind(AssertUnwindSafe(|| {
            rt.handle().block_on(CountingFuture { polls: polls.clone(), value: 19 })
        }));
        let payload = rejected.expect_err("已选择另一运行时");
        assert!(matches!(
            payload.downcast_ref::<CurrentRuntimeAccessError>(),
            Some(CurrentRuntimeAccessError::DifferentRuntime)
        ));
        assert_eq!(Handle::try_current().unwrap().id(), other.id());
    }).unwrap();
    assert_eq!(polls.load(Ordering::Relaxed), 0);
}

#[derive(Default)]
struct CountWake(AtomicUsize);

impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// 正时长等待只轮询（poll）一次到挂起（Pending），到期后须主动唤醒原等待者。
#[test]
fn test_timeout_wakes_after_single_pending_poll() {
    let elapsed_nanos = Arc::new(AtomicU64::new(0));
    let mut rt = SerialRuntime::with_components(
        TestClock(elapsed_nanos.clone()), DefaultIdlePolicy, NoHooks, RuntimeOptions::default(),
    ).unwrap();
    let wake = Arc::new(CountWake::default());
    let waker = Waker::from(wake.clone());
    let mut cx = Context::from_waker(&waker);
    let mut timeout = rt.timeout(Duration::from_millis(2));
    assert!(Pin::new(&mut timeout).poll(&mut cx).is_pending());
    assert_eq!(wake.0.load(Ordering::Relaxed), 0);

    elapsed_nanos.store(2_000_000, Ordering::Relaxed);
    rt.run_once().unwrap();
    assert_eq!(wake.0.load(Ordering::Relaxed), 1);
    assert!(matches!(Pin::new(&mut timeout).poll(&mut cx), Poll::Ready(Ok(()))));
}

struct ScheduleLocalTimer(Arc<AtomicUsize>);

impl Wake for ScheduleLocalTimer {
    fn wake(self: Arc<Self>) {
        type TestRuntime = ConfiguredRuntime<SerialMode, TestClock, DefaultIdlePolicy, NoHooks>;
        let runs = self.0.clone();
        let task = TestRuntime::spawn_timing_local(async move {
            runs.fetch_add(1, Ordering::Relaxed);
        }, Duration::from_millis(1)).expect("等待返回时可通过原本地入口登记新定时任务");
        drop(task);
    }
}

/// 交付原等待的通知回调进入同一个本地定时入口，验证此时队列借用（borrow）已结束。
#[test]
fn test_timer_completion_releases_queue_before_waking_waiter() {
    let elapsed_nanos = Arc::new(AtomicU64::new(0));
    let mut rt = SerialRuntime::with_components(
        TestClock(elapsed_nanos.clone()), DefaultIdlePolicy, NoHooks, RuntimeOptions::default(),
    ).unwrap();
    let next_runs = Arc::new(AtomicUsize::new(0));
    let waker = Waker::from(Arc::new(ScheduleLocalTimer(next_runs.clone())));
    let mut cx = Context::from_waker(&waker);
    let mut timeout = rt.timeout(Duration::from_millis(2));
    assert!(Pin::new(&mut timeout).poll(&mut cx).is_pending());
    assert!(Pin::new(&mut timeout).poll(&mut cx).is_pending());

    elapsed_nanos.store(2_000_000, Ordering::Relaxed);
    rt.run_once().unwrap();
    assert_eq!(next_runs.load(Ordering::Relaxed), 0);
    assert!(matches!(Pin::new(&mut timeout).poll(&mut cx), Poll::Ready(Ok(()))));
    elapsed_nanos.store(3_000_000, Ordering::Relaxed);
    rt.run_once().unwrap();
    assert_eq!(next_runs.load(Ordering::Relaxed), 1);
}

/// 正时长等待可以在首次轮询前接受取消，释放或完成后持有的控制句柄也应失效。
#[test]
fn test_timeout_cancel_before_registration_ends_interest() {
    let rt = SerialRuntime::with_components(
        TestClock(Arc::new(AtomicU64::new(0))), DefaultIdlePolicy, NoHooks, RuntimeOptions::default(),
    ).unwrap();
    let mut timeout = rt.timeout(Duration::from_secs(30));
    let control = timeout.control_handle();
    assert!(control.cancel().unwrap());
    assert_eq!(rt.timer_registration_count(), 0);
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(Pin::new(&mut timeout).poll(&mut cx), Poll::Ready(Err(TimerError::Cancelled))));
    assert!(!control.cancel().unwrap());
    assert_eq!(rt.timer_registration_count(), 0);

    let abandoned = rt.timeout(Duration::from_secs(30));
    let late_control = abandoned.control_handle();
    drop(abandoned);
    assert!(!late_control.cancel().unwrap());
    assert_eq!(rt.timer_registration_count(), 0);
}

/// 正常任务结束后，运行时仍开放时就应释放已不用的任务附加数据（task metadata）。
#[test]
fn test_completed_task_metadata_retires_while_runtime_stays_open() {
    ensure_clock();
    let mut rt = SerialRuntime::new().unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let handle = rt.spawn_with_config(async {}, TaskConfig::new(CountDrop(drops.clone()))).unwrap();
    drop(handle);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    assert_eq!(rt.run_once().unwrap().runs, 1);
    assert_eq!(rt.run_once().unwrap().runs, 0);
    assert_eq!(rt.in_flight_task_count(), 0);
    assert_eq!(rt.status(), RuntimeStatus::Open);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

/// 从未提交的任务同样需要在开放服务中退役，单靠下一次新任务接纳会保留其数据。
#[test]
fn test_unsubmitted_task_metadata_retires_without_new_work() {
    ensure_clock();
    let mut rt = SerialRuntime::new().unwrap();
    let drops = Arc::new(AtomicUsize::new(0));
    let (task, handle) = rt.create_with_config(async {}, TaskConfig::new(CountDrop(drops.clone()))).unwrap();
    drop(handle);
    drop(task);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    for _ in 0..2 {
        assert_eq!(rt.run_once().unwrap().runs, 0);
    }
    assert_eq!(rt.status(), RuntimeStatus::Open);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

/// 查询记录先退役后，活动记录的附加数据（metadata）异常也只能结算一次在途数。
#[test]
fn test_maintenance_settles_in_flight_before_metadata_panic() {
    let mut options = RuntimeOptions::default();
    options.executor.maintenance_batch = std::num::NonZeroUsize::MIN;
    let mut rt = ConcurrentRuntime::with_components(
        TestClock(Arc::new(AtomicU64::new(0))), DefaultIdlePolicy, NoHooks, options,
    ).unwrap();
    let first = rt.spawn(std::future::pending::<()>()).unwrap();
    let done = Arc::new(AtomicBool::new(false));
    let finish = done.clone();
    let drops = Arc::new(AtomicUsize::new(0));
    let complete = rt.spawn_with_config(
        std::future::poll_fn(move |_| if finish.load(Ordering::Relaxed) { Poll::Ready(()) } else { Poll::Pending }),
        TaskConfig::new(PanicOnDrop(drops.clone())),
    ).unwrap();
    let last = rt.spawn(std::future::pending::<()>()).unwrap();
    for _ in 0..3 {
        assert_eq!(rt.run_once().unwrap().runs, 1);
    }
    done.store(true, Ordering::Relaxed);
    complete.control_handle().wake();
    drop(complete);
    assert_eq!(rt.run_once().unwrap().runs, 1);
    assert_eq!(rt.in_flight_task_count(), 3);
    assert_eq!(rt.run_once().unwrap().runs, 0);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    let payload = catch_unwind(AssertUnwindSafe(|| rt.run_once())).unwrap_err();
    assert_eq!(payload.downcast_ref::<u32>(), Some(&274));
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(rt.in_flight_task_count(), 2);
    assert_eq!(rt.run_once().unwrap().runs, 0);
    assert_eq!(rt.in_flight_task_count(), 2);
    drop((first, last));
}

/// 查询记录退役时先结清自身，再保留附加数据（task metadata）析构发起的关闭及原异常。
#[test]
fn test_close_during_maintenance_keeps_the_first_failure() {
    struct ClosingMetadata(std::sync::Weak<pi_async_runtime::RuntimeHandle<TestClock>>);
    impl Drop for ClosingMetadata {
        fn drop(&mut self) {
            assert_eq!(self.0.upgrade().unwrap().request_close(), CloseRequestStatus::Requested);
            panic_any(274u32);
        }
    }

    let mut rt = ConcurrentRuntime::with_components(
        TestClock(Arc::new(AtomicU64::new(0))), DefaultIdlePolicy, NoHooks, RuntimeOptions::default(),
    ).unwrap();
    let access = Arc::new(rt.handle());
    let task = rt.spawn_with_config(async {}, TaskConfig::new(ClosingMetadata(Arc::downgrade(&access)))).unwrap();
    drop(task);
    assert_eq!(rt.run_once().unwrap().runs, 1);
    let payload = catch_unwind(AssertUnwindSafe(|| rt.run_once())).unwrap_err();
    assert_eq!(payload.downcast_ref::<u32>(), Some(&274));
    assert!(rt.run_once().unwrap().service_closed);
    assert_eq!(access.in_flight_task_count(), 0);
    assert_eq!(access.status(), RuntimeStatus::Closed);
    assert_eq!(access.close_state(), CloseState::Requested);
}

struct CloseOnDrop(Handle);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        self.0.request_close();
    }
}

/// 原线程持续运行，外部发布、唤醒和请求关闭可与其实际等待或推进交错。
/// 用任务依次返回的真实线程身份、定时结果和最终关闭事实观察，不用时间上界断言调度延迟。
#[test]
fn test_active_runtime_cross_thread_wake_timer_and_close() {
    ensure_clock();
    let recv_handle = TestReport::new();
    let send_handle = recv_handle.clone();
    let worker = thread::spawn(move || {
        let mut owner = SerialRuntime::new().unwrap();
        send_handle.publish(owner.handle());
        owner.run().unwrap();
        assert_eq!(owner.close_state(), CloseState::Completed);
    });
    let handle = recv_handle.receive().clone();
    let close = CloseOnDrop(handle.clone());
    let owner = worker.thread().id();
    let recv_waker = TestReport::new();
    let send_waker = recv_waker.clone();
    let recv_finished = TestReport::new();
    let finished = recv_finished.clone();
    let _task = handle.spawn(async move {
        let result = WakeFromAnotherThread { owner, report: Some(send_waker) }.await;
        finished.publish(result);
    }).unwrap();
    recv_waker.receive().wake_by_ref();
    assert_eq!(*recv_finished.receive(), owner);

    let recv_due = TestReport::new();
    let send_due = recv_due.clone();
    let delay = Duration::from_millis(20);
    let entered = Instant::now();
    let _timer = handle.spawn_timing(async move {
        send_due.publish((thread::current().id(), entered.elapsed()));
    }, delay).unwrap();
    let (thread, elapsed) = *recv_due.receive();
    assert_eq!(thread, owner);
    assert!(elapsed >= delay);
    drop(close);
    worker.join().unwrap();
    assert_eq!(handle.status(), RuntimeStatus::Closed);
    assert_eq!(handle.close_state(), CloseState::Completed);
}
