#[cfg(not(feature = "serial"))]
mod default_runtime {
    use futures::future::BoxFuture;
    use pi_async_rt::rt::{
        multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder, StealableTaskPool},
        AsyncRuntime, AsyncTask, AsyncTaskPool,
    };
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            atomic::{AtomicUsize, Ordering},
            mpsc, Arc, Barrier, Mutex, Weak,
        },
        task::{Context, Poll, Waker},
        thread,
        time::{Duration, Instant},
    };

    const WORKER_SIZE: usize = 4;
    const TASK_COUNT: usize = 4;
    const CONCURRENT_WAKERS: usize = 8;
    const WAKES_PER_THREAD: usize = 8;
    const DUPLICATE_ENTRIES: usize = 64;
    const START_TIMEOUT: Duration = Duration::from_secs(5);
    const RELEASE_TIMEOUT: Duration = Duration::from_secs(2);

    static ASYNC_TASK_SCHEDULING_TEST_LOCK: parking_lot::Mutex<()> =
        parking_lot::Mutex::new(());

    type RuntimeTask = AsyncTask<StealableTaskPool<()>, ()>;

    /// 在一次轮询内合法请求后续轮询并立即完成的一次性 Future。
    ///
    /// `Future::poll` 执行期间允许调用 `Waker::wake_by_ref`。执行器可以把该唤醒与
    /// 当前完成合并，但不得保留或重复入队已完成任务。本夹具只发出合法唤醒，不提供
    /// 入队、轮询、取消或清理行为。
    struct WakeThenReady {
        polls: Arc<AtomicUsize>,
    }

    impl Future for WakeThenReady {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.polls.fetch_add(1, Ordering::AcqRel);
            cx.waker().wake_by_ref();
            Poll::Ready(())
        }
    }

    /// 首次轮询发出多次自唤醒、总共被轮询两次的合法 Future。
    ///
    /// 本夹具自身不合并也不入队。精确轮询次数因此可以证明生产唤醒处理把同一运行
    /// 代次中的全部唤醒合并为一个后续义务。
    struct WakeManyThenPending {
        polls: Arc<AtomicUsize>,
        yielded: bool,
    }

    impl Future for WakeManyThenPending {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.polls.fetch_add(1, Ordering::AcqRel);
            if self.yielded {
                Poll::Ready(())
            } else {
                self.yielded = true;
                for _ in 0..DUPLICATE_ENTRIES {
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            }
        }
    }

    /// 捕获当前生产唤醒器，然后等待一次外部唤醒。
    struct CaptureIdleWaker {
        polls: Arc<AtomicUsize>,
        waker: Arc<Mutex<Option<Waker>>>,
        armed: Option<mpsc::Sender<()>>,
    }

    impl Future for CaptureIdleWaker {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let poll = self.polls.fetch_add(1, Ordering::AcqRel);
            if poll == 0 {
                *self.waker.lock().unwrap() = Some(cx.waker().clone());
                self.armed.take().unwrap().send(()).unwrap();
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        }
    }

    #[derive(Clone, Copy)]
    enum RunningResult {
        PendingThenReady,
        Ready,
    }

    /// 在有界窗口内保持首次生产轮询，以确定性构造唤醒交错。阻塞接收端只用于
    /// 测试，绝不提供队列、唤醒、重试、完成或状态机行为。
    struct BlockedRunningFuture {
        polls: Arc<AtomicUsize>,
        waker: Arc<Mutex<Option<Waker>>>,
        entered: Option<mpsc::Sender<()>>,
        release: Option<mpsc::Receiver<()>>,
        dropped: Option<mpsc::Sender<()>>,
        result: RunningResult,
        yielded: bool,
    }

    impl Future for BlockedRunningFuture {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.polls.fetch_add(1, Ordering::AcqRel);
            if self.yielded {
                return Poll::Ready(());
            }

            *self.waker.lock().unwrap() = Some(cx.waker().clone());
            self.entered.take().unwrap().send(()).unwrap();
            self.release
                .take()
                .unwrap()
                .recv_timeout(START_TIMEOUT)
                .unwrap();

            match self.result {
                RunningResult::PendingThenReady => {
                    self.yielded = true;
                    Poll::Pending
                },
                RunningResult::Ready => Poll::Ready(()),
            }
        }
    }

    impl Drop for BlockedRunningFuture {
        fn drop(&mut self) {
            if let Some(dropped) = self.dropped.take() {
                let _ = dropped.send(());
            }
        }
    }

    /// 保存一个在 Future 返回 `Ready` 后仍然存活的唤醒器。
    struct SaveWakerThenReady {
        polls: Arc<AtomicUsize>,
        waker: Arc<Mutex<Option<Waker>>>,
        dropped: Option<mpsc::Sender<()>>,
    }

    impl Future for SaveWakerThenReady {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.polls.fetch_add(1, Ordering::AcqRel);
            *self.waker.lock().unwrap() = Some(cx.waker().clone());
            Poll::Ready(())
        }
    }

    impl Drop for SaveWakerThenReady {
        fn drop(&mut self) {
            if let Some(dropped) = self.dropped.take() {
                let _ = dropped.send(());
            }
        }
    }

    /// 仅用于扩大重复物理队列项竞争窗口的慢速一次性 Future。
    struct SlowReady {
        polls: Arc<AtomicUsize>,
        active: Arc<AtomicUsize>,
        maximum_active: Arc<AtomicUsize>,
    }

    impl Future for SlowReady {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.polls.fetch_add(1, Ordering::AcqRel);
            let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
            self.maximum_active.fetch_max(active, Ordering::AcqRel);
            thread::sleep(Duration::from_millis(20));
            self.active.fetch_sub(1, Ordering::AcqRel);
            Poll::Ready(())
        }
    }

    /// 构造红线测试使用的真实生产多线程运行时。
    ///
    /// 运行时不包含定时器工作线程、通道适配器或业务夹具，从而隔离生产
    /// `AsyncTask::wake_by_ref -> StealableTaskPool -> multi_thread::run_task`
    /// 生命周期。时间和空间复杂度均为 O(工作线程数量)；辅助函数只分配真实运行时队列
    /// 和工作线程。
    fn new_runtime() -> MultiTaskRuntime<()> {
        let pool = StealableTaskPool::with(WORKER_SIZE, 1024, [1, 1], 10);
        MultiTaskRuntimeBuilder::new(pool)
            .thread_prefix("Async-Task-Scheduling-Redline")
            .thread_stack_size(2 * 1024 * 1024)
            .init_worker_size(WORKER_SIZE)
            .set_worker_limit(WORKER_SIZE, WORKER_SIZE)
            .set_timeout(10)
            .build()
    }

    fn submit(
        runtime: &MultiTaskRuntime<()>,
        future: BoxFuture<'static, ()>,
    ) -> Weak<RuntimeTask> {
        let task = Arc::new(AsyncTask::new(
            runtime.alloc::<()>(),
            runtime.shared_pool(),
            5,
            Some(future),
        ));
        let task_ref = Arc::downgrade(&task);
        runtime.shared_pool().push(task).unwrap();
        task_ref
    }

    fn wait_for_poll_count(polls: &AtomicUsize, expected: usize) -> bool {
        let deadline = Instant::now() + START_TIMEOUT;
        while Instant::now() < deadline {
            if polls.load(Ordering::Acquire) == expected {
                return true;
            }
            thread::sleep(Duration::from_millis(1));
        }
        false
    }

    fn retained_count(tasks: &[Weak<AsyncTask<StealableTaskPool<()>, ()>>]) -> usize {
        tasks.iter().filter(|task| task.upgrade().is_some()).count()
    }

    fn wait_for_release(
        tasks: &[Weak<AsyncTask<StealableTaskPool<()>, ()>>],
    ) -> usize {
        let deadline = Instant::now() + RELEASE_TIMEOUT;
        loop {
            let retained = retained_count(tasks);
            if retained == 0 || Instant::now() >= deadline {
                return retained;
            }
            thread::sleep(Duration::from_millis(1));
        }
    }

    fn wake_concurrently(waker: &Waker) {
        let barrier = Arc::new(Barrier::new(CONCURRENT_WAKERS + 1));
        let mut threads = Vec::with_capacity(CONCURRENT_WAKERS);

        for _ in 0..CONCURRENT_WAKERS {
            let barrier = barrier.clone();
            let waker = waker.clone();
            threads.push(thread::spawn(move || {
                barrier.wait();
                for _ in 0..WAKES_PER_THREAD {
                    waker.wake_by_ref();
                }
            }));
        }

        barrier.wait();
        for thread in threads {
            thread.join().unwrap();
        }
    }

    fn assert_released(task: Weak<RuntimeTask>, message: &str) {
        let retained = wait_for_release(&[task]);
        assert_eq!(retained, 0, "{}", message);
    }

    fn run_blocked_running_case(runtime: &MultiTaskRuntime<()>, result: RunningResult) {
        let polls = Arc::new(AtomicUsize::new(0));
        let waker = Arc::new(Mutex::new(None));
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let task = submit(
            runtime,
            Box::pin(BlockedRunningFuture {
                polls: polls.clone(),
                waker: waker.clone(),
                entered: Some(entered_tx),
                release: Some(release_rx),
                dropped: Some(dropped_tx),
                result,
                yielded: false,
            }),
        );

        entered_rx.recv_timeout(START_TIMEOUT).unwrap();
        let waker = waker.lock().unwrap().take().unwrap();
        wake_concurrently(&waker);
        release_tx.send(()).unwrap();
        dropped_rx.recv_timeout(START_TIMEOUT).unwrap();

        let expected = match result {
            RunningResult::PendingThenReady => 2,
            RunningResult::Ready => 1,
        };
        assert_eq!(
            polls.load(Ordering::Acquire),
            expected,
            "running wake produced an incorrect number of polls"
        );

        for _ in 0..DUPLICATE_ENTRIES {
            waker.wake_by_ref();
        }
        thread::sleep(Duration::from_millis(20));
        assert_eq!(
            polls.load(Ordering::Acquire),
            expected,
            "late waker re-polled a completed task"
        );
        drop(waker);
        assert_released(task, "running-wake task remained retained");
    }

    /// 已完成任务队列保留及其工作线程活锁的红线测试。
    ///
    /// 测试有意构造真实公开 `AsyncTask`，同时覆盖公开构造器和运行时驱动。每个 Future
    /// 必须准确轮询一次并完成，且全部任务强引用必须在硬截止前释放。若 `Weak` 仍可
    /// 升级，即可证明生产队列所有权仍保留已完成任务；判定不依赖 CPU 人工采样。
    ///
    /// 测试是确定性、有界的，不阻塞工作线程执行，并与本测试目标的其它测试串行。测试
    /// 不使用 `block_on`、模拟任务池、替代调度器、定时器或人工 RSS 观察。
    #[test]
    fn test_multi_thread_self_wake_ready_releases_task_without_requeue_livelock() {
        let _test_lock = ASYNC_TASK_SCHEDULING_TEST_LOCK.lock();
        let runtime = new_runtime();
        let polls = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::with_capacity(TASK_COUNT);

        for _ in 0..TASK_COUNT {
            let future: BoxFuture<'static, ()> = Box::pin(WakeThenReady {
                polls: polls.clone(),
            });
            let task = Arc::new(AsyncTask::new(
                runtime.alloc::<()>(),
                runtime.shared_pool(),
                5,
                Some(future),
            ));
            tasks.push(Arc::downgrade(&task));
            runtime.shared_pool().push(task).unwrap();
        }

        let all_started = wait_for_poll_count(&polls, TASK_COUNT);
        let retained = if all_started {
            wait_for_release(&tasks)
        } else {
            retained_count(&tasks)
        };
        let observed_polls = polls.load(Ordering::Acquire);
        let _ = runtime.close();

        assert!(
            all_started,
            "not all redline futures started before the deadline: polls={}, expected={}",
            observed_polls,
            TASK_COUNT
        );
        assert_eq!(
            observed_polls,
            TASK_COUNT,
            "a one-shot Ready future must be polled exactly once"
        );
        assert_eq!(
            retained,
            0,
            "completed self-woken tasks remained retained by runtime queues"
        );
    }

    /// 覆盖运行中自唤醒以及空闲/已入队状态下的并发外部唤醒。
    ///
    /// 每个场景都使用真实运行时、精确轮询计数和任务生命周期。测试不包含调度器
    /// 替代实现，全部操作系统线程均在下一场景开始前完成回收。
    #[test]
    fn test_multi_thread_managed_wake_coalescing_is_exact_and_bounded() {
        let _test_lock = ASYNC_TASK_SCHEDULING_TEST_LOCK.lock();
        let runtime = new_runtime();

        let self_polls = Arc::new(AtomicUsize::new(0));
        let self_task = submit(
            &runtime,
            Box::pin(WakeManyThenPending {
                polls: self_polls.clone(),
                yielded: false,
            }),
        );
        assert!(wait_for_poll_count(&self_polls, 2));
        assert_eq!(self_polls.load(Ordering::Acquire), 2);
        assert_released(self_task, "self-woken Pending task remained retained");

        let external_polls = Arc::new(AtomicUsize::new(0));
        let external_waker = Arc::new(Mutex::new(None));
        let (armed_tx, armed_rx) = mpsc::channel();
        let external_task = submit(
            &runtime,
            Box::pin(CaptureIdleWaker {
                polls: external_polls.clone(),
                waker: external_waker.clone(),
                armed: Some(armed_tx),
            }),
        );
        armed_rx.recv_timeout(START_TIMEOUT).unwrap();
        let external_waker = external_waker.lock().unwrap().take().unwrap();
        wake_concurrently(&external_waker);
        assert!(wait_for_poll_count(&external_polls, 2));
        assert_eq!(external_polls.load(Ordering::Acquire), 2);

        for _ in 0..DUPLICATE_ENTRIES {
            external_waker.wake_by_ref();
        }
        thread::sleep(Duration::from_millis(20));
        assert_eq!(
            external_polls.load(Ordering::Acquire),
            2,
            "late wake after idle/queued concurrency re-polled completion"
        );
        drop(external_waker);
        assert_released(external_task, "external-woken task remained retained");
        let _ = runtime.close();
    }

    /// 确定性覆盖 `Pending` 和 `Ready` 两种轮询期间唤醒。
    #[test]
    fn test_multi_thread_running_wake_pending_ready_and_late_wake_are_safe() {
        let _test_lock = ASYNC_TASK_SCHEDULING_TEST_LOCK.lock();
        let runtime = new_runtime();

        run_blocked_running_case(&runtime, RunningResult::PendingThenReady);
        run_blocked_running_case(&runtime, RunningResult::Ready);

        let polls = Arc::new(AtomicUsize::new(0));
        let waker = Arc::new(Mutex::new(None));
        let (dropped_tx, dropped_rx) = mpsc::channel();
        let task = submit(
            &runtime,
            Box::pin(SaveWakerThenReady {
                polls: polls.clone(),
                waker: waker.clone(),
                dropped: Some(dropped_tx),
            }),
        );
        dropped_rx.recv_timeout(START_TIMEOUT).unwrap();
        let waker = waker.lock().unwrap().take().unwrap();
        wake_concurrently(&waker);
        thread::sleep(Duration::from_millis(20));
        assert_eq!(polls.load(Ordering::Acquire), 1);
        drop(waker);
        assert_released(task, "completed task was retained by its stale waker");
        let _ = runtime.close();
    }

    /// 陈旧物理队列项和 Future 缺失场景的健壮性红线。
    ///
    /// 直接向任务池重复插入不属于高层运行时契约，但安全的低层接口不得因此并发轮询
    /// Future，也不得形成永久 pop/push 循环。
    #[test]
    fn test_multi_thread_stale_entries_and_empty_task_are_discarded() {
        let _test_lock = ASYNC_TASK_SCHEDULING_TEST_LOCK.lock();
        let runtime = new_runtime();
        let polls = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let maximum_active = Arc::new(AtomicUsize::new(0));
        let task = Arc::new(AsyncTask::new(
            runtime.alloc::<()>(),
            runtime.shared_pool(),
            5,
            Some(Box::pin(SlowReady {
                polls: polls.clone(),
                active: active.clone(),
                maximum_active: maximum_active.clone(),
            })),
        ));
        let task_ref = Arc::downgrade(&task);

        for _ in 0..DUPLICATE_ENTRIES {
            runtime.shared_pool().push(task.clone()).unwrap();
        }
        drop(task);

        assert!(wait_for_poll_count(&polls, 1));
        assert_released(task_ref, "stale duplicate entries retained a completed task");
        assert_eq!(polls.load(Ordering::Acquire), 1);
        assert_eq!(maximum_active.load(Ordering::Acquire), 1);
        assert_eq!(active.load(Ordering::Acquire), 0);

        let empty = Arc::new(AsyncTask::new(
            runtime.alloc::<()>(),
            runtime.shared_pool(),
            5,
            None,
        ));
        let empty_ref = Arc::downgrade(&empty);
        runtime.shared_pool().push(empty).unwrap();
        assert_released(empty_ref, "managed task without a future was requeued");
        let _ = runtime.close();
    }

    /// 固定已审查的 x86_64 布局及百万存活任务增量。
    ///
    /// 状态字节跨过既有 16 字节对齐边界。本门禁用于防止布局继续静默增长；恢复到
    /// 80 字节基线属于需要单独冻结的优化，不属于本次 P0 修复。
    #[test]
    #[cfg(target_arch = "x86_64")]
    fn test_async_task_layout_size_and_growth_are_documented() {
        let _test_lock = ASYNC_TASK_SCHEDULING_TEST_LOCK.lock();
        const BASELINE_SIZE: usize = 80;
        const REVIEWED_SIZE: usize = 96;
        const ONE_MILLION_TASKS: usize = 1_000_000;

        let actual_size = std::mem::size_of::<RuntimeTask>();
        assert_eq!(actual_size, REVIEWED_SIZE);
        assert_eq!(std::mem::align_of::<RuntimeTask>(), 16);
        assert_eq!(actual_size - BASELINE_SIZE, 16);
        assert_eq!(
            (actual_size - BASELINE_SIZE) * ONE_MILLION_TASKS,
            16_000_000,
        );
    }
}
