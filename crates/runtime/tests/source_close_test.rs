//! 通过公开运行时（runtime）入口核对来源移交、原异步计算（Future）释放和关闭完成的顺序。
use pi_async_executor::{
    timer::DefaultTimerDriver, DefaultIdlePolicy, ExecutorClock, ExecutorSnapshot, ExecutorTimer,
    IdlePolicy, NoHooks, TimerDelivery,
};
use pi_async_runtime::{
    binding::RuntimeTag,
    CloseRequestStatus, CloseState, ConcurrentRuntime, IdleDispatch, RuntimeControl, RuntimeOptions,
    RuntimeQuery, RuntimeQueues, RuntimeStatus, TaskDispatch, TimedTaskDispatch,
};
use pi_async_task::Runnable;
use pi_async_task_queue::{
    BatchAttempt, ExternalPushError, ExternalQueueManager, ExternalTaskQueue,
    IdleQueueError, IdleQueueManager, IdleQueueStatus, IdleSubmitError, IdleTake, IdleTaskQueue,
    MpscIdleTaskQueue, MpscIdleTaskQueueHandle, MpscTaskQueue, MpscTaskQueueHandle, QueueCleanupProgress,
    TimerAdvanceProgress, TimerCancelRequestError, TimerQueueError, TimerQueueStatus, TimerSubmitError,
    TimerTake, TimerTaskQueue,
};
use pi_clock_manual::global::GlobalManualInstant;
use std::{future::Future, num::NonZeroUsize};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, OnceLock,
};
use std::time::Duration;

// 在原来源交出批次后触发一次关闭。原队列仍负责计数、关闭和未交出的原值。
type AfterTake = Arc<OnceLock<Box<dyn Fn() + Send + Sync>>>;

struct IdleSource {
    queue: MpscIdleTaskQueue,
    after_take: AfterTake,
}

#[derive(Clone)]
struct IdleHandle {
    handle: MpscIdleTaskQueueHandle,
    after_take: AfterTake,
}

impl IdleTaskQueue for IdleHandle {
    fn try_push(&self, task: Runnable) -> pi_result::RawResult<(), IdleSubmitError> {
        self.handle.try_push(task)
    }

    fn try_take_batch(&self, max: NonZeroUsize) -> pi_result::RawResult<IdleTake, IdleQueueError> {
        // 原来源已释放取出权限。关闭请求只更新服务并通知，不等待任何线程。
        let result = self.handle.try_take_batch(max)?;
        if matches!(result, IdleTake::Ready(_)) {
            if let Some(after_take) = self.after_take.get() {
                after_take();
            }
        }
        Ok(result)
    }

    fn process_cleanup(&self) -> pi_result::Result<QueueCleanupProgress> {
        self.handle.process_cleanup()
    }

    fn status(&self) -> IdleQueueStatus {
        self.handle.status()
    }

    fn len(&self) -> usize {
        self.handle.len()
    }

    fn is_empty(&self) -> bool {
        self.handle.is_empty()
    }
}

impl IdleQueueManager for IdleSource {
    type Handle = IdleHandle;

    fn handle(&self) -> Self::Handle {
        IdleHandle { handle: self.queue.handle(), after_take: self.after_take.clone() }
    }

    fn clear(&mut self) -> pi_result::Result<QueueCleanupProgress> {
        self.queue.clear()
    }

    fn close(&mut self) -> pi_result::Result<QueueCleanupProgress> {
        self.queue.close()
    }
}

// 保证同一原值的析构（drop）只处理一次。用户函数可查询服务或产生原恐慌（panic）。
struct DropProbe<F: FnOnce()>(Option<F>);

impl<F: FnOnce()> Drop for DropProbe<F> {
    fn drop(&mut self) {
        (self.0.take().expect("原异步计算只清理一次"))();
    }
}

/// 在原值真正析构时查询关闭状态，并通过取出和轮询次数验证两条路径的先后关系。
fn observe_status_when_dropping(trigger_after_take: bool) -> bool {
    pi_clock_manual::global::try_init().ok();
    let options = RuntimeOptions::default();
    let after_take = AfterTake::default();
    let queue = MpscIdleTaskQueue::new(options.idle_queue).unwrap();
    let mut runtime = ConcurrentRuntime::with_queues(
        options,
        RuntimeQueues::default().with_idle(IdleSource { queue, after_take: after_take.clone() }),
    ).unwrap();
    let access = Arc::new(runtime.handle());
    let drop_count = Arc::new(AtomicUsize::new(0));
    let saw_complete = Arc::new(AtomicBool::new(false));
    let observed = Arc::new(AtomicBool::new(false));
    let polls = Arc::new(AtomicUsize::new(0));

    // 弱引用只用于在原计算析构期间读取当前关闭状态。
    let weak_access = Arc::downgrade(&access);
    let drop_probe = DropProbe(Some({
        let drop_count = drop_count.clone();
        let saw_complete = saw_complete.clone();
        move || {
            drop_count.fetch_add(1, Ordering::Relaxed);
            let access = weak_access.upgrade().expect("外部查询仍存续");
            saw_complete.store(access.close_state() == CloseState::Completed, Ordering::Relaxed);
        }
    }));
    let _result = access.idle({
        let polls = polls.clone();
        async move {
            polls.fetch_add(1, Ordering::Relaxed);
            drop(drop_probe);
        }
    }).unwrap_or_else(|_| panic!("空闲任务应成功接收"));

    assert!(after_take.set(Box::new({
        let weak_access = Arc::downgrade(&access);
        let observed = observed.clone();
        move || {
            assert!(!observed.swap(true, Ordering::Relaxed), "只取出一次");
            assert!(trigger_after_take, "先关闭时不得取出空闲任务");
            let service = weak_access.upgrade().unwrap();
            assert_eq!(service.status(), RuntimeStatus::Open);
            assert_eq!(service.request_close(), CloseRequestStatus::Requested);
        }
    })).is_ok());
    if !trigger_after_take {
        assert_eq!(access.request_close(), CloseRequestStatus::Requested);
    }

    assert!(runtime.run_once().unwrap().service_closed);
    assert_eq!(observed.load(Ordering::Relaxed), trigger_after_take);
    assert_eq!(access.status(), RuntimeStatus::Closed);
    assert_eq!(drop_count.load(Ordering::Relaxed), 1);
    assert_eq!(polls.load(Ordering::Relaxed), 0);
    assert_eq!(access.close_state(), CloseState::Completed);
    saw_complete.load(Ordering::Relaxed)
}

/// 关闭先发生时绝不从空闲任务队列取出业务。
#[test]
fn close_requested_before_take_keeps_cleanup_order() {
    assert!(!observe_status_when_dropping(false));
}

/// 从空闲任务队列移交后，关闭完成须晚于原值析构。
#[test]
fn close_requested_after_take_keeps_cleanup_order() {
    assert!(
        !observe_status_when_dropping(true),
        "原空闲任务尚在析构时不得已可观察到关闭完成（Completed）",
    );
}

/// 空闲任务队列已交出一项，原异常传播，同一队列剩余项仍正常析构。
#[test]
fn close_during_idle_take_retains_the_panic_and_cleans_remaining_tasks() {
    pi_clock_manual::global::try_init().ok();
    let options = RuntimeOptions::default();
    let after_take = AfterTake::default();
    let queue = MpscIdleTaskQueue::new(options.idle_queue).unwrap();
    let mut runtime = ConcurrentRuntime::with_queues(
        options,
        RuntimeQueues::default().with_idle(IdleSource { queue, after_take: after_take.clone() }),
    ).unwrap();
    let access = Arc::new(runtime.handle());
    let weak_access = Arc::downgrade(&access);
    assert!(after_take.set(Box::new(move || {
        assert_eq!(weak_access.upgrade().unwrap().request_close(), CloseRequestStatus::Requested);
    })).is_ok());

    let probe = TaskProbe::default();
    let _result = access.idle(probe.future(|| std::panic::panic_any(274u32)))
        .unwrap_or_else(|_| panic!("空闲任务应成功接收"));
    let later_dropped = Arc::new(AtomicUsize::new(0));
    let later = DropProbe(Some({
        let later_dropped = later_dropped.clone();
        move || {
            later_dropped.fetch_add(1, Ordering::Relaxed);
        }
    }));
    let later_polls = Arc::new(AtomicUsize::new(0));
    let _later_result = access.idle({
        let later_polls = later_polls.clone();
        async move {
            later_polls.fetch_add(1, Ordering::Relaxed);
            drop(later);
        }
    }).unwrap_or_else(|_| panic!("另一项空闲任务应成功接收"));

    assert_original_panic(|| { runtime.run_once().unwrap(); });
    probe.assert_dropped_without_poll();
    assert_eq!(later_polls.load(Ordering::Relaxed), 0);
    assert_eq!(later_dropped.load(Ordering::Relaxed), 1);
    assert_eq!(access.status(), RuntimeStatus::Closed);
    assert_eq!(access.close_state(), CloseState::Requested);
}

/// 通过真实默认空闲任务队列的扫描析构请求关闭，检查原异常和最终关闭状态。
#[test]
fn close_during_idle_cleanup_preserves_the_failure() {
    pi_clock_manual::global::try_init().ok();
    let mut runtime = ConcurrentRuntime::new().unwrap();
    let access = Arc::new(runtime.handle());
    let weak_access = Arc::downgrade(&access);
    let probe = TaskProbe::default();
    let result = access.idle(probe.future(move || {
        assert_eq!(weak_access.upgrade().unwrap().request_close(), CloseRequestStatus::Requested);
        std::panic::panic_any(274u32);
    })).unwrap_or_else(|_| panic!("空闲任务应成功接收"));
    result.control_handle().cancel();
    assert_original_panic(|| { runtime.run_once().unwrap(); });
    probe.assert_dropped_without_poll();
    assert!(runtime.run_once().unwrap().service_closed);
    assert_eq!(access.status(), RuntimeStatus::Closed);
    assert_eq!(access.close_state(), CloseState::Requested);
    assert_eq!(access.idle_task_count(), 0);
}

/// 空闲来源的普通取消清理异常继续传播，开放的运行时仍可正常接纳和推进。
#[test]
fn idle_cleanup_failure_without_close_preserves_open_service() {
    pi_clock_manual::global::try_init().ok();
    let mut runtime = ConcurrentRuntime::new().unwrap();
    let probe = TaskProbe::default();
    let result = runtime.idle(probe.future(|| std::panic::panic_any(274u32))).unwrap();
    result.control_handle().cancel();

    assert_original_panic(|| { runtime.run_once().unwrap(); });
    probe.assert_dropped_without_poll();
    assert_eq!(runtime.status(), RuntimeStatus::Open);
    assert_eq!(runtime.close_state(), CloseState::NotRequested);
    let _next = runtime.spawn(async {}).unwrap();
    assert_eq!(runtime.run_once().unwrap().runs, 1);
}

/// 原空闲任务的清理已完成，紧接着可以确认同一扫描终点。自定义休眠建议不取消这一事实。
#[test]
fn idle_cleanup_with_ready_endpoint_reports_immediate_work() {
    struct PermitSleep;
    impl IdlePolicy for PermitSleep {
        fn is_idle(&mut self, _: &ExecutorSnapshot<'_>) -> bool {
            false
        }

        fn sleep_duration(&mut self, _: &ExecutorSnapshot<'_>) -> Option<Duration> {
            Some(Duration::from_millis(10))
        }
    }

    let clock = TestClock(Arc::new(std::sync::atomic::AtomicU64::new(0)));
    let mut runtime = ConcurrentRuntime::with_components(
        clock, PermitSleep, NoHooks, RuntimeOptions::default(),
    ).unwrap();
    let probe = TaskProbe::default();
    let result = runtime.idle(probe.future(|| {})).unwrap();
    result.control_handle().cancel();
    let step = runtime.run_once().unwrap();
    assert_eq!(step.runs, 0);
    assert_eq!(step.sleep_for, Some(Duration::from_millis(10)));
    probe.assert_dropped_without_poll();
    assert_eq!(runtime.idle_task_count(), 0);
    assert!(step.immediate_work);
    let confirmed = runtime.run_once().unwrap();
    assert_eq!(confirmed.runs, 0);
    assert!(!confirmed.immediate_work);
}

// 用真实默认外部任务队列执行取出，只在原批次交出后记录关闭请求。
struct ExternalSource {
    queue: MpscTaskQueue,
    after_take: AfterTake,
}

#[derive(Clone)]
struct ExternalHandle {
    handle: MpscTaskQueueHandle,
    after_take: AfterTake,
}

impl ExternalTaskQueue for ExternalHandle {
    fn try_push(&self, task: Runnable) -> pi_result::RawResult<(), ExternalPushError> {
        self.handle.try_push(task)
    }

    fn try_take_batch(&self) -> BatchAttempt {
        let batch = self.handle.try_take_batch();
        if matches!(batch, BatchAttempt::Ready(_)) {
            self.after_take.get().expect("预先保存关闭操作")();
        }
        batch
    }

    fn len(&self) -> usize {
        self.handle.len()
    }
}

impl ExternalQueueManager for ExternalSource {
    type Handle = ExternalHandle;

    fn handle(&self) -> Self::Handle {
        ExternalHandle { handle: self.queue.handle(), after_take: self.after_take.clone() }
    }

    fn clear(&mut self) -> pi_result::Result<QueueCleanupProgress> {
        self.queue.clear()
    }

    fn close(&mut self) -> pi_result::Result<QueueCleanupProgress> {
        self.queue.close()
    }
}

/// 普通外部批次已经交出时关闭。原队列无在途计数也不能掩盖原析构异常。
#[test]
fn close_during_external_take_preserves_the_failure() {
    observe_external_take_failure(false);
}

/// 自定义来源已经完成原批次的取出，回调请求关闭后异常展开。原批次和其他来源都要结清。
#[test]
fn close_during_external_source_callback_preserves_the_failure() {
    observe_external_take_failure(true);
}

// 同一原队列分别复现取出回调异常与交出后原值析构异常。只有一个位置产生恐慌。
fn observe_external_take_failure(panic_in_source: bool) {
    pi_clock_manual::global::try_init().ok();
    let options = RuntimeOptions::default();
    let after_take = AfterTake::default();
    let queue = MpscTaskQueue::new(options.external_queue).unwrap();
    let mut runtime = ConcurrentRuntime::with_queues(options,
        RuntimeQueues::default().with_external(ExternalSource { queue, after_take: after_take.clone() }),
    ).unwrap();
    let access = Arc::new(runtime.handle());
    let weak_access = Arc::downgrade(&access);
    assert!(after_take.set(Box::new(move || {
        let access = weak_access.upgrade().unwrap();
        assert_eq!(access.external_task_count(), 0, "原队列已交出并结清本批计数");
        assert_eq!(access.request_close(), CloseRequestStatus::Requested);
        if panic_in_source {
            std::panic::panic_any(274u32);
        }
    })).is_ok());
    let failing = TaskProbe::default();
    let other = TaskProbe::default();
    let idle = TaskProbe::default();
    let _first = access.spawn(failing.future(move || {
        if !panic_in_source {
            std::panic::panic_any(274u32);
        }
    })).unwrap();
    let _second = access.spawn(other.future(|| {})).unwrap();
    let _idle = access.idle(idle.future(|| {})).unwrap();
    assert_original_panic(|| { runtime.run_once().unwrap(); });
    failing.assert_dropped_without_poll();
    other.assert_dropped_without_poll();
    // 首次展开已经接续了关闭，不需要调用方捕获后再次推进才完成其他来源。
    assert_eq!(access.status(), RuntimeStatus::Closed);
    idle.assert_dropped_without_poll();
    assert!(runtime.run_once().unwrap().service_closed);
    assert_eq!(access.status(), RuntimeStatus::Closed);
    assert_eq!(access.close_state(), CloseState::Requested);
}

/// 真实外部任务已取出，其取消清理在接纳之前发起关闭并展开。验证同批余项同时结束。
#[test]
fn close_during_external_cancelled_task_cleanup_preserves_the_failure() {
    pi_clock_manual::global::try_init().ok();
    let mut runtime = ConcurrentRuntime::new().unwrap();
    let access = Arc::new(runtime.handle());
    let weak_access = Arc::downgrade(&access);
    let probe = TaskProbe::default();
    let result = access.spawn(probe.future(move || {
        assert_eq!(weak_access.upgrade().unwrap().request_close(), CloseRequestStatus::Requested);
        std::panic::panic_any(274u32);
    })).unwrap();
    let later = TaskProbe::default();
    let _later_result = access.spawn(later.future(|| {})).unwrap();
    result.control_handle().cancel();
    assert_original_panic(|| { runtime.run_once().unwrap(); });
    probe.assert_dropped_without_poll();
    later.assert_dropped_without_poll();
    assert_eq!(access.status(), RuntimeStatus::Closed);
    assert_eq!(access.close_state(), CloseState::Requested);
    assert_eq!(access.external_task_count(), 0);
}

/// 远端取消经原归属回送。所属线程必须先结清该项内部计数，再接续析构触发的关闭。
#[test]
fn close_during_return_cleanup_settles_the_count_and_preserves_the_failure() {
    pi_clock_manual::global::try_init().ok();
    let mut runtime = ConcurrentRuntime::new().unwrap();
    let access = Arc::new(runtime.handle());
    let weak_access = Arc::downgrade(&access);
    let drops = Arc::new(AtomicUsize::new(0));
    let original = DropProbe(Some({
        let drops = drops.clone();
        move || {
            drops.fetch_add(1, Ordering::Relaxed);
            assert_eq!(weak_access.upgrade().unwrap().request_close(), CloseRequestStatus::Requested);
            std::panic::panic_any(274u32);
        }
    }));
    let result = access.spawn(async move {
        std::future::pending::<()>().await;
        drop(original);
    }).unwrap();
    assert_eq!(runtime.run_once().unwrap().runs, 1);
    let control = result.control_handle();
    std::thread::spawn(move || control.cancel()).join().unwrap();
    assert_eq!(access.internal_task_count(), 1);
    assert_original_panic(|| { runtime.run_once().unwrap(); });
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(access.internal_task_count(), 0);
    assert_eq!(access.status(), RuntimeStatus::Closed);
    assert_eq!(access.close_state(), CloseState::Requested);
}

/// 真实空闲来源交出单项后记录取消。运行该项的取消清理时，原值请求关闭并保留异常。
#[test]
fn close_during_single_idle_task_cleanup_preserves_the_failure() {
    pi_clock_manual::global::try_init().ok();
    let options = RuntimeOptions::default();
    let after_take = AfterTake::default();
    let queue = MpscIdleTaskQueue::new(options.idle_queue).unwrap();
    let mut runtime = ConcurrentRuntime::with_queues(
        options,
        RuntimeQueues::default().with_idle(IdleSource { queue, after_take: after_take.clone() }),
    ).unwrap();
    let access = Arc::new(runtime.handle());
    let weak_access = Arc::downgrade(&access);
    let probe = TaskProbe::default();
    let result = access.idle(probe.future(move || {
        assert_eq!(weak_access.upgrade().unwrap().request_close(), CloseRequestStatus::Requested);
        std::panic::panic_any(274u32);
    })).unwrap();
    let control = result.control_handle();
    assert!(after_take.set(Box::new(move || control.cancel())).is_ok());

    assert_original_panic(|| { runtime.run_once().unwrap(); });
    probe.assert_dropped_without_poll();
    assert_eq!(access.idle_task_count(), 0);
    assert_eq!(access.status(), RuntimeStatus::Closed);
    assert_eq!(access.close_state(), CloseState::Requested);
}

/// 原异步计算在返回 Ready 后析构。此次轮询已请求关闭时，原析构异常要接续整体退出。
#[test]
fn close_after_poll_preserves_the_future_drop_failure() {
    pi_clock_manual::global::try_init().ok();
    let mut runtime = ConcurrentRuntime::new().unwrap();
    let access = Arc::new(runtime.handle());
    let weak_access = Arc::downgrade(&access);
    let drops = Arc::new(AtomicUsize::new(0));
    let original = DropProbe(Some({
        let drops = drops.clone();
        move || {
            drops.fetch_add(1, Ordering::Relaxed);
            std::panic::panic_any(274u32);
        }
    }));
    let _result = access.spawn(std::future::poll_fn(move |_| {
        let _original = &original;
        assert_eq!(weak_access.upgrade().unwrap().request_close(), CloseRequestStatus::Requested);
        std::task::Poll::Ready(())
    })).unwrap();
    assert_original_panic(|| { runtime.run_once().unwrap(); });
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(access.status(), RuntimeStatus::Closed);
    assert_eq!(access.close_state(), CloseState::Requested);
}

/// 结果句柄已释放，原输出由运行时析构。输出发起的关闭及首次失败都须真实保留。
#[test]
fn close_during_unclaimed_output_drop_preserves_the_failure() {
    pi_clock_manual::global::try_init().ok();
    let mut runtime = ConcurrentRuntime::new().unwrap();
    let access = Arc::new(runtime.handle());
    let weak_access = Arc::downgrade(&access);
    let drops = Arc::new(AtomicUsize::new(0));
    let output = DropProbe(Some({
        let drops = drops.clone();
        move || {
            drops.fetch_add(1, Ordering::Relaxed);
            assert_eq!(weak_access.upgrade().unwrap().request_close(), CloseRequestStatus::Requested);
            std::panic::panic_any(274u32);
        }
    }));
    drop(access.spawn(async move { output }).unwrap());
    assert_original_panic(|| { runtime.run_once().unwrap(); });
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(access.status(), RuntimeStatus::Closed);
    assert_eq!(access.close_state(), CloseState::Requested);
    assert!(runtime.run_once().unwrap().service_closed);
}

// 严格使用同一显式时间轴，推进测试不依赖系统时间及1ms取整相位。
#[derive(Clone)]
struct TestClock(Arc<std::sync::atomic::AtomicU64>);

impl ExecutorClock for TestClock {
    type Instant = GlobalManualInstant;

    fn now(&self) -> pi_result::Result<Self::Instant> {
        GlobalManualInstant::try_from_elapsed(Duration::from_millis(self.0.load(Ordering::Relaxed)))
    }

    fn checked_add(&self, now: &Self::Instant, duration: Duration) -> pi_result::Result<Self::Instant> {
        now.try_add(duration)
    }

    fn remaining(&self, now: &Self::Instant, deadline: &Self::Instant) -> pi_result::Result<Duration> {
        if deadline <= now { Ok(Duration::ZERO) } else { deadline.try_duration_since(*now) }
    }
}

type Timer = DefaultTimerDriver<TestClock, RuntimeTag<TestClock>>;

// 原定时任务队列实际推进并转交单项后，才允许测试固定关闭请求的位置。
struct TimerSource {
    timer: Timer,
    after_take: AfterTake,
}

impl TimerTaskQueue for TimerSource {
    type Item = <Timer as TimerTaskQueue>::Item;
    type Instant = <Timer as TimerTaskQueue>::Instant;
    type Key = <Timer as TimerTaskQueue>::Key;
    type DueBatch = <Timer as TimerTaskQueue>::DueBatch;
    type Handle = <Timer as TimerTaskQueue>::Handle;

    fn handle(&self) -> Self::Handle {
        self.timer.handle()
    }

    fn try_push_local(&mut self, deadline: Self::Instant, item: Self::Item)
        -> pi_result::RawResult<Self::Key, TimerSubmitError<Self::Item>>
    {
        self.timer.try_push_local(deadline, item)
    }

    fn request_cancel_local(&mut self, key: &Self::Key) -> pi_result::RawResult<(), TimerCancelRequestError> {
        self.timer.request_cancel_local(key)
    }

    fn advance(&mut self, now: &Self::Instant) -> pi_result::RawResult<TimerAdvanceProgress, TimerQueueError> {
        self.timer.advance(now)
    }

    fn try_take_due_one(&mut self) -> pi_result::RawResult<TimerTake<Self::Item>, TimerQueueError> {
        let result = self.timer.try_take_due_one()?;
        if matches!(result, TimerTake::Ready(_)) {
            self.after_take.get().expect("预先保存关闭操作")();
        }
        Ok(result)
    }

    fn try_take_due_all(&mut self) -> pi_result::RawResult<TimerTake<Self::DueBatch>, TimerQueueError> {
        self.timer.try_take_due_all()
    }

    fn process_cleanup(&mut self) -> pi_result::Result<QueueCleanupProgress> {
        self.timer.process_cleanup()
    }

    fn next_check_at(&self, now: &Self::Instant) -> pi_result::RawResult<Option<Self::Instant>, TimerQueueError> {
        self.timer.next_check_at(now)
    }

    fn status(&self) -> TimerQueueStatus {
        self.timer.status()
    }

    fn len(&self) -> usize {
        self.timer.len()
    }

    fn is_empty(&self) -> bool {
        self.timer.is_empty()
    }

    fn clear(&mut self) -> pi_result::Result<QueueCleanupProgress> {
        self.timer.clear()
    }

    fn close(&mut self) -> pi_result::Result<QueueCleanupProgress> {
        self.timer.close()
    }
}

impl ExecutorTimer for TimerSource {
    type Access = <Timer as ExecutorTimer>::Access;

    fn timer_access(&self) -> Self::Access {
        self.timer.timer_access()
    }

    fn process_cancel_retries(&mut self) -> pi_result::Result<QueueCleanupProgress> {
        self.timer.process_cancel_retries()
    }
}

/// 单项定时任务已脱离来源时关闭，原析构异常不能变成关闭完成。
#[test]
fn close_during_one_timer_take_preserves_the_failure() {
    let mut options = RuntimeOptions::default();
    options.executor.timer_delivery = TimerDelivery::One;
    let timer_options = options.timer;
    let after_take = AfterTake::default();
    let clock = TestClock(Arc::new(std::sync::atomic::AtomicU64::new(0)));
    let queues = RuntimeQueues::default().with_timer({
        let after_take = after_take.clone();
        move |id, clock: &TestClock, notifier| {
            let timer = Timer::new(id, clock.clone(), notifier, timer_options)?;
            Ok(TimerSource { timer, after_take })
        }
    });
    let mut runtime = ConcurrentRuntime::with_components_and_queues(
        clock.clone(), DefaultIdlePolicy, NoHooks, options, queues,
    ).unwrap();
    let access = Arc::new(runtime.handle());
    let weak_access = Arc::downgrade(&access);
    assert!(after_take.set(Box::new(move || {
        assert_eq!(weak_access.upgrade().unwrap().request_close(), CloseRequestStatus::Requested);
    })).is_ok());
    let probe = TaskProbe::default();
    let _result = access.spawn_timing(probe.future(|| {
        if !std::thread::panicking() {
            std::panic::panic_any(274u32);
        }
    }), Duration::from_millis(1)).unwrap();
    assert_eq!(access.timer_registration_count(), 1);
    clock.0.store(1, Ordering::Relaxed);
    assert_original_panic(|| { runtime.run_once().unwrap(); });
    probe.assert_dropped_without_poll();
    assert!(runtime.run_once().unwrap().service_closed);
    assert_eq!(access.status(), RuntimeStatus::Closed);
    assert_eq!(access.close_state(), CloseState::Requested);
    assert_eq!(access.timer_registration_count(), 0);
}

/// 默认定时任务队列取消未到期任务时原值请求关闭。检查定时来源借用已退还，异常与关闭状态相符。
#[test]
fn close_during_timer_cleanup_preserves_the_failure() {
    let clock = TestClock(Arc::new(std::sync::atomic::AtomicU64::new(0)));
    let mut runtime = ConcurrentRuntime::with_components(
        clock, DefaultIdlePolicy, NoHooks, RuntimeOptions::default(),
    ).unwrap();
    let access = Arc::new(runtime.handle());
    let weak_access = Arc::downgrade(&access);
    let probe = TaskProbe::default();
    let result = access.spawn_timing(probe.future(move || {
        assert_eq!(weak_access.upgrade().unwrap().request_close(), CloseRequestStatus::Requested);
        std::panic::panic_any(274u32);
    }), Duration::from_secs(60)).unwrap();
    assert_eq!(access.timer_registration_count(), 1);
    result.control_handle().cancel();
    assert_original_panic(|| { runtime.run_once().unwrap(); });
    probe.assert_dropped_without_poll();
    assert!(runtime.run_once().unwrap().service_closed);
    assert_eq!(access.status(), RuntimeStatus::Closed);
    assert_eq!(access.close_state(), CloseState::Requested);
    assert_eq!(access.timer_registration_count(), 0);
}

// 原异步计算的轮询次数和释放次数分别记录，免受任务本身恐慌处理的影响。
#[derive(Default)]
struct TaskProbe {
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl TaskProbe {
    fn future(&self, action: impl FnOnce() + Send + 'static) -> impl Future<Output = ()> + Send + 'static {
        let polls = self.polls.clone();
        let drops = self.drops.clone();
        let probe = DropProbe(Some(move || {
            drops.fetch_add(1, Ordering::Relaxed);
            action();
        }));
        async move {
            polls.fetch_add(1, Ordering::Relaxed);
            drop(probe);
        }
    }

    fn assert_dropped_without_poll(&self) {
        assert_eq!(self.polls.load(Ordering::Relaxed), 0);
        assert_eq!(self.drops.load(Ordering::Relaxed), 1);
    }
}

// 从公开推进出口观察原恐慌载荷。测试捕获后才能继续读取仍保存的运行时状态。
fn assert_original_panic(run: impl FnOnce()) {
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run));
    let original = failure.expect_err("应当传播首个析构恐慌");
    assert_eq!(original.downcast_ref::<u32>(), Some(&274));
}
