#[cfg(not(feature = "serial"))]
mod default_runtime {
    use crossbeam_queue::SegQueue;
    use futures::{future::poll_fn, task::waker_ref};
    use pi_async_rt::rt::{
        alloc_rt_uid,
        multi_thread::{
            ComputationalTaskPool, MultiTaskRuntime, MultiTaskRuntimeBuilder,
            StealableTaskPool,
        },
        single_thread::SingleTaskRunner,
        startup_global_time_loop,
        worker_thread::WorkerTaskRunner,
        AsyncRuntime, AsyncRuntimeExt, AsyncTask, AsyncTaskPool, AsyncTaskPoolExt,
    };
    use std::{
        collections::HashSet,
        future::Future,
        io::Result,
        panic::{catch_unwind, AssertUnwindSafe},
        pin::Pin,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            mpsc, Arc, Mutex, Weak,
        },
        task::{Context, Poll, Waker},
        thread,
        time::{Duration, Instant},
        vec::IntoIter,
    };

    const WAKE_COUNT: usize = 64;
    const TEST_TIMEOUT: Duration = Duration::from_secs(5);
    const RELEASE_TIMEOUT: Duration = Duration::from_secs(2);

    static ASYNC_TASK_RUNTIME_MATRIX_LOCK: parking_lot::Mutex<()> =
        parking_lot::Mutex::new(());

    /// 只用于观察生产执行器调用的最小自定义存储任务池。
    ///
    /// 它有意不执行唤醒合并、状态转换、轮询、重试、工作线程通知或完成处理。
    /// `SingleTaskRunner` 和 `AsyncTask::wake_by_ref` 仍是唯一生产调度机制。
    struct CountingPool {
        id: usize,
        queue: SegQueue<Arc<AsyncTask<CountingPool, ()>>>,
        initial_pushes: AtomicUsize,
        keep_pushes: AtomicUsize,
    }

    impl Default for CountingPool {
        fn default() -> Self {
            CountingPool {
                id: alloc_rt_uid() << 32,
                queue: SegQueue::new(),
                initial_pushes: AtomicUsize::new(0),
                keep_pushes: AtomicUsize::new(0),
            }
        }
    }

    impl AsyncTaskPool<()> for CountingPool {
        type Pool = CountingPool;

        fn get_thread_id(&self) -> usize {
            self.id
        }

        fn len(&self) -> usize {
            self.queue.len()
        }

        fn push(&self, task: Arc<AsyncTask<Self::Pool, ()>>) -> Result<()> {
            self.initial_pushes.fetch_add(1, Ordering::AcqRel);
            self.queue.push(task);
            Ok(())
        }

        fn push_local(&self, task: Arc<AsyncTask<Self::Pool, ()>>) -> Result<()> {
            self.push(task)
        }

        fn push_priority(
            &self,
            _priority: usize,
            task: Arc<AsyncTask<Self::Pool, ()>>,
        ) -> Result<()> {
            self.push(task)
        }

        fn push_keep(&self, task: Arc<AsyncTask<Self::Pool, ()>>) -> Result<()> {
            self.keep_pushes.fetch_add(1, Ordering::AcqRel);
            self.queue.push(task);
            Ok(())
        }

        fn try_pop(&self) -> Option<Arc<AsyncTask<Self::Pool, ()>>> {
            self.queue.pop()
        }

        fn try_pop_all(&self) -> IntoIter<Arc<AsyncTask<Self::Pool, ()>>> {
            let mut tasks = Vec::with_capacity(self.queue.len());
            while let Some(task) = self.queue.pop() {
                tasks.push(task);
            }
            tasks.into_iter()
        }

        fn get_thread_waker(
            &self,
        ) -> Option<&Arc<(AtomicBool, parking_lot::Mutex<()>, parking_lot::Condvar)>> {
            None
        }
    }

    impl AsyncTaskPoolExt<()> for CountingPool {}

    struct WakeMany {
        polls: Arc<AtomicUsize>,
        yielded: bool,
        ready_on_first_poll: bool,
    }

    impl Future for WakeMany {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.polls.fetch_add(1, Ordering::AcqRel);
            if self.ready_on_first_poll || self.yielded {
                if self.ready_on_first_poll {
                    for _ in 0..WAKE_COUNT {
                        cx.waker().wake_by_ref();
                    }
                }
                return Poll::Ready(());
            }

            self.yielded = true;
            for _ in 0..WAKE_COUNT {
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        }
    }

    struct CaptureWaker {
        polls: Arc<AtomicUsize>,
        waker: Arc<Mutex<Option<Waker>>>,
    }

    impl Future for CaptureWaker {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let poll = self.polls.fetch_add(1, Ordering::AcqRel);
            if poll == 0 {
                *self.waker.lock().unwrap() = Some(cx.waker().clone());
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        }
    }

    struct PanicAfterWaker {
        waker: Arc<Mutex<Option<Waker>>>,
    }

    impl Future for PanicAfterWaker {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            *self.waker.lock().unwrap() = Some(cx.waker().clone());
            panic!("intentional AsyncTask poll panic");
        }
    }

    fn wait_for_release<P>(
        task: &Weak<AsyncTask<P, ()>>,
    ) -> bool
    where
        P: AsyncTaskPoolExt<()> + AsyncTaskPool<(), Pool = P>,
    {
        let deadline = Instant::now() + RELEASE_TIMEOUT;
        while Instant::now() < deadline {
            if task.upgrade().is_none() {
                return true;
            }
            thread::sleep(Duration::from_millis(1));
        }
        task.upgrade().is_none()
    }

    fn drain_runner(runner: &SingleTaskRunner<(), CountingPool>) {
        for _ in 0..(WAKE_COUNT + 8) {
            if runner.run_once().unwrap() == 0 {
                return;
            }
        }
        panic!("custom pool did not drain within the bounded runner steps");
    }

    fn new_stealable_runtime(worker_size: usize, timer: bool) -> MultiTaskRuntime<()> {
        let pool = StealableTaskPool::with(worker_size, 4096, [1, 1], 10);
        let builder = MultiTaskRuntimeBuilder::new(pool)
            .thread_prefix("Async-Task-Runtime-Matrix")
            .thread_stack_size(2 * 1024 * 1024)
            .init_worker_size(worker_size)
            .set_worker_limit(worker_size, worker_size)
            .set_timeout(20);
        if timer {
            builder.set_timer_interval(1).build()
        } else {
            builder.build()
        }
    }

    async fn completion_future(
        runtime: MultiTaskRuntime<()>,
        id: usize,
        completed: mpsc::Sender<usize>,
    ) {
        runtime.yield_now().await;
        completed.send(id).unwrap();
    }

    fn receive_exact_ids(receiver: &mpsc::Receiver<usize>, expected: usize) {
        let deadline = Instant::now() + TEST_TIMEOUT;
        let mut ids = HashSet::with_capacity(expected);
        while ids.len() < expected {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                remaining > Duration::ZERO,
                "runtime API matrix timed out after receiving {} of {} ids",
                ids.len(),
                expected
            );
            let id = receiver.recv_timeout(remaining).unwrap_or_else(|error| {
                let mut received = ids.iter().copied().collect::<Vec<_>>();
                received.sort_unstable();
                panic!(
                    "runtime API matrix receive failed after receiving {:?} of {} ids: {:?}",
                    received,
                    expected,
                    error,
                );
            });
            assert!(ids.insert(id), "runtime API completed id {} twice", id);
        }
        assert!(receiver.try_recv().is_err());
    }

    /// 精确入队计数和手工驱动兼容矩阵。
    #[test]
    fn test_single_runner_managed_and_legacy_scheduling_contracts() {
        let _test_lock = ASYNC_TASK_RUNTIME_MATRIX_LOCK.lock();
        let runner = SingleTaskRunner::<(), CountingPool>::new(CountingPool::default());
        let runtime = runner.startup().unwrap();
        let pool = runtime.shared_pool();

        let ready_polls = Arc::new(AtomicUsize::new(0));
        let keep_before = pool.keep_pushes.load(Ordering::Acquire);
        runtime
            .spawn(WakeMany {
                polls: ready_polls.clone(),
                yielded: false,
                ready_on_first_poll: true,
            })
            .unwrap();
        drain_runner(&runner);
        assert_eq!(ready_polls.load(Ordering::Acquire), 1);
        assert_eq!(pool.keep_pushes.load(Ordering::Acquire), keep_before);

        let pending_polls = Arc::new(AtomicUsize::new(0));
        let keep_before = pool.keep_pushes.load(Ordering::Acquire);
        runtime
            .spawn(WakeMany {
                polls: pending_polls.clone(),
                yielded: false,
                ready_on_first_poll: false,
            })
            .unwrap();
        drain_runner(&runner);
        assert_eq!(pending_polls.load(Ordering::Acquire), 2);
        assert_eq!(
            pool.keep_pushes.load(Ordering::Acquire) - keep_before,
            1,
            "managed running wakes must produce one physical follow-up entry"
        );

        let queued_polls = Arc::new(AtomicUsize::new(0));
        let queued = Arc::new(AsyncTask::new(
            runtime.alloc::<()>(),
            pool.clone(),
            5,
            Some(Box::pin(WakeMany {
                polls: queued_polls.clone(),
                yielded: false,
                ready_on_first_poll: false,
            })),
        ));
        pool.push(queued.clone()).unwrap();
        let keep_before = pool.keep_pushes.load(Ordering::Acquire);
        {
            let queued_waker = waker_ref(&queued);
            for _ in 0..WAKE_COUNT {
                queued_waker.wake_by_ref();
            }
        }
        assert_eq!(
            pool.keep_pushes.load(Ordering::Acquire),
            keep_before,
            "wakes for an already queued managed task must not add queue entries"
        );
        drain_runner(&runner);
        assert_eq!(queued_polls.load(Ordering::Acquire), 2);
        assert_eq!(
            pool.keep_pushes.load(Ordering::Acquire) - keep_before,
            1,
            "the future's running wake must still schedule its one Pending follow-up"
        );
        drop(queued);

        let idle_polls = Arc::new(AtomicUsize::new(0));
        let idle_waker = Arc::new(Mutex::new(None));
        runtime
            .spawn(CaptureWaker {
                polls: idle_polls.clone(),
                waker: idle_waker.clone(),
            })
            .unwrap();
        assert_eq!(runner.run_once().unwrap(), 0);
        let waker = idle_waker.lock().unwrap().take().unwrap();
        let keep_before = pool.keep_pushes.load(Ordering::Acquire);
        for _ in 0..WAKE_COUNT {
            waker.wake_by_ref();
        }
        drain_runner(&runner);
        assert_eq!(idle_polls.load(Ordering::Acquire), 2);
        assert_eq!(
            pool.keep_pushes.load(Ordering::Acquire) - keep_before,
            1,
            "managed idle/queued wakes must produce one physical entry"
        );
        drop(waker);

        let legacy_polls = Arc::new(AtomicUsize::new(0));
        let legacy = Arc::new(AsyncTask::new(
            runtime.alloc::<()>(),
            pool.clone(),
            5,
            Some(Box::pin(WakeMany {
                polls: legacy_polls.clone(),
                yielded: false,
                ready_on_first_poll: false,
            })),
        ));
        let future = legacy.get_inner().unwrap();
        legacy.set_inner(Some(future));
        let keep_before = pool.keep_pushes.load(Ordering::Acquire);
        pool.push(legacy).unwrap();
        drain_runner(&runner);
        assert_eq!(legacy_polls.load(Ordering::Acquire), 2);
        assert_eq!(
            pool.keep_pushes.load(Ordering::Acquire) - keep_before,
            WAKE_COUNT,
            "public get/set manual driver must retain its legacy wake enqueue behavior"
        );

        let first_polls = Arc::new(AtomicUsize::new(0));
        let reusable = Arc::new(AsyncTask::new(
            runtime.alloc::<()>(),
            pool.clone(),
            5,
            Some(Box::pin(WakeMany {
                polls: first_polls.clone(),
                yielded: false,
                ready_on_first_poll: true,
            })),
        ));
        pool.push(reusable.clone()).unwrap();
        drain_runner(&runner);
        assert_eq!(first_polls.load(Ordering::Acquire), 1);

        let replacement_polls = Arc::new(AtomicUsize::new(0));
        reusable.set_inner(Some(Box::pin(WakeMany {
            polls: replacement_polls.clone(),
            yielded: false,
            ready_on_first_poll: true,
        })));
        pool.push(reusable).unwrap();
        drain_runner(&runner);
        assert_eq!(replacement_polls.load(Ordering::Acquire), 1);

        let runtime_context_polls = Arc::new(AtomicUsize::new(0));
        let runtime_context_task = Arc::new(AsyncTask::with_runtime_and_context(
            &runtime,
            5,
            Some(Box::pin(WakeMany {
                polls: runtime_context_polls.clone(),
                yielded: false,
                ready_on_first_poll: true,
            })),
            17usize,
        ));
        assert!(runtime_context_task.exist_context());
        let runtime_context_ref = Arc::downgrade(&runtime_context_task);
        pool.push(runtime_context_task).unwrap();
        drain_runner(&runner);
        assert_eq!(runtime_context_polls.load(Ordering::Acquire), 1);
        assert!(
            wait_for_release(&runtime_context_ref),
            "with_runtime_and_context task remained retained"
        );

        let panic_waker = Arc::new(Mutex::new(None));
        let panic_task = Arc::new(AsyncTask::new(
            runtime.alloc::<()>(),
            pool.clone(),
            5,
            Some(Box::pin(PanicAfterWaker {
                waker: panic_waker.clone(),
            })),
        ));
        let panic_ref = Arc::downgrade(&panic_task);
        pool.push(panic_task).unwrap();
        assert!(catch_unwind(AssertUnwindSafe(|| runner.run_once())).is_err());
        let panic_waker = panic_waker.lock().unwrap().take().unwrap();
        let keep_before = pool.keep_pushes.load(Ordering::Acquire);
        for _ in 0..WAKE_COUNT {
            panic_waker.wake_by_ref();
        }
        assert_eq!(runner.run_once().unwrap(), 0);
        assert_eq!(pool.keep_pushes.load(Ordering::Acquire), keep_before);
        drop(panic_waker);
        assert!(
            wait_for_release(&panic_ref),
            "panic-completed task remained retained after stale waker release"
        );
    }

    /// 覆盖受首次托管调度影响的全部公开多线程提交变体，包括定时器/context 和 TaskId
    /// 唤醒路径。
    #[test]
    fn test_multi_thread_spawn_timer_context_and_task_id_matrix() {
        let _test_lock = ASYNC_TASK_RUNTIME_MATRIX_LOCK.lock();
        let _time_loop = startup_global_time_loop(1);
        let runtime = new_stealable_runtime(4, true);
        let (completed_tx, completed_rx) = mpsc::channel();
        let mut next_id = 0;

        macro_rules! future {
            () => {{
                let id = next_id;
                next_id += 1;
                completion_future(runtime.clone(), id, completed_tx.clone())
            }};
        }

        runtime.spawn(future!()).unwrap();
        runtime.spawn_local(future!()).unwrap();
        runtime.spawn_priority(10, future!()).unwrap();
        runtime.spawn_yield(future!()).unwrap();
        runtime.spawn_timing(future!(), 1).unwrap();
        runtime
            .spawn_by_id(runtime.alloc::<()>(), future!())
            .unwrap();
        runtime
            .spawn_local_by_id(runtime.alloc::<()>(), future!())
            .unwrap();
        runtime
            .spawn_priority_by_id(runtime.alloc::<()>(), 10, future!())
            .unwrap();
        runtime
            .spawn_yield_by_id(runtime.alloc::<()>(), future!())
            .unwrap();
        runtime
            .spawn_timing_by_id(runtime.alloc::<()>(), future!(), 1)
            .unwrap();
        runtime
            .spawn_with_context(runtime.alloc::<()>(), future!(), 11usize)
            .unwrap();
        runtime
            .spawn_timing_with_context(runtime.alloc::<()>(), future!(), 12usize, 1)
            .unwrap();

        let task_id = runtime.alloc::<()>();
        let task_id_for_poll = task_id.clone();
        let runtime_for_poll = runtime.clone();
        let task_ready = Arc::new(AtomicBool::new(false));
        let task_ready_for_poll = task_ready.clone();
        let (armed_tx, armed_rx) = mpsc::channel();
        let mut armed_tx = Some(armed_tx);
        let completion_id = next_id;
        next_id += 1;
        let completed_for_task_id = completed_tx.clone();
        runtime
            .spawn_by_id(
                task_id.clone(),
                poll_fn(move |cx| {
                    if task_ready_for_poll.load(Ordering::Acquire) {
                        completed_for_task_id.send(completion_id).unwrap();
                        Poll::Ready(())
                    } else {
                        let pending =
                            runtime_for_poll.pending::<()>(&task_id_for_poll, cx.waker().clone());
                        // 必须先由生产 `runtime.pending` 注册唤醒器，再通知测试线程执行
                        // wakeup；反向顺序会由测试夹具自身制造“注册前唤醒”的非法竞态。
                        if let Some(armed_tx) = armed_tx.take() {
                            armed_tx.send(()).unwrap();
                        }
                        pending
                    }
                }),
            )
            .unwrap();
        armed_rx.recv_timeout(TEST_TIMEOUT).unwrap();
        task_ready.store(true, Ordering::Release);
        runtime.wakeup::<()>(&task_id);

        drop(completed_tx);
        receive_exact_ids(&completed_rx, next_id);
        let _ = runtime.close();
    }

    /// 覆盖第二种内置多线程任务池和由另一个运行时发出的唤醒。
    #[test]
    fn test_computational_pool_and_cross_runtime_wake_complete_exactly_once() {
        let _test_lock = ASYNC_TASK_RUNTIME_MATRIX_LOCK.lock();
        let pool = ComputationalTaskPool::new(4);
        let computational = MultiTaskRuntimeBuilder::new(pool)
            .thread_prefix("Async-Task-Computational-Matrix")
            .thread_stack_size(2 * 1024 * 1024)
            .init_worker_size(4)
            .set_worker_limit(4, 4)
            .set_timeout(20)
            .build();
        let computational_polls = Arc::new(AtomicUsize::new(0));
        let (computational_tx, computational_rx) = mpsc::channel();
        let polls_for_task = computational_polls.clone();
        computational
            .spawn(async move {
                WakeMany {
                    polls: polls_for_task,
                    yielded: false,
                    ready_on_first_poll: false,
                }
                .await;
                computational_tx.send(()).unwrap();
            })
            .unwrap();
        computational_rx.recv_timeout(TEST_TIMEOUT).unwrap();
        assert_eq!(computational_polls.load(Ordering::Acquire), 2);

        let source = new_stealable_runtime(2, false);
        let target = new_stealable_runtime(2, false);
        let target_polls = Arc::new(AtomicUsize::new(0));
        let target_waker = Arc::new(Mutex::new(None));
        let (armed_tx, armed_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let polls_for_target = target_polls.clone();
        let waker_for_target = target_waker.clone();
        target
            .spawn(poll_fn(move |cx| {
                let poll = polls_for_target.fetch_add(1, Ordering::AcqRel);
                if poll == 0 {
                    *waker_for_target.lock().unwrap() = Some(cx.waker().clone());
                    armed_tx.send(()).unwrap();
                    Poll::Pending
                } else {
                    done_tx.send(()).unwrap();
                    Poll::Ready(())
                }
            }))
            .unwrap();
        armed_rx.recv_timeout(TEST_TIMEOUT).unwrap();
        let target_waker = target_waker.lock().unwrap().take().unwrap();
        let (source_done_tx, source_done_rx) = mpsc::channel();
        source
            .spawn(async move {
                target_waker.wake_by_ref();
                source_done_tx.send(()).unwrap();
            })
            .unwrap();
        source_done_rx.recv_timeout(TEST_TIMEOUT).unwrap();
        done_rx.recv_timeout(TEST_TIMEOUT).unwrap();
        assert_eq!(target_polls.load(Ordering::Acquire), 2);

        let _ = computational.close();
        let _ = source.close();
        let _ = target.close();
    }

    /// 确认 `WorkerRuntime` 继承生产 `SingleTaskRunner` 状态路径。
    #[test]
    fn test_worker_runtime_self_wake_pending_completes_exactly_once() {
        let _test_lock = ASYNC_TASK_RUNTIME_MATRIX_LOCK.lock();
        let runner = WorkerTaskRunner::<()>::default();
        let runner_for_loop = runner.clone();
        let runtime = runner.startup(
            "Async-Task-Worker-Matrix",
            2 * 1024 * 1024,
            20,
            None,
            move || {
                let started = Instant::now();
                let len = runner_for_loop.run().unwrap();
                (len == 0, started.elapsed())
            },
            || 0,
        );
        let polls = Arc::new(AtomicUsize::new(0));
        let (done_tx, done_rx) = mpsc::channel();
        let polls_for_task = polls.clone();
        runtime
            .spawn(async move {
                WakeMany {
                    polls: polls_for_task,
                    yielded: false,
                    ready_on_first_poll: false,
                }
                .await;
                done_tx.send(()).unwrap();
            })
            .unwrap();
        done_rx.recv_timeout(TEST_TIMEOUT).unwrap();
        assert_eq!(polls.load(Ordering::Acquire), 2);
        let _ = runtime.close();
    }
}
