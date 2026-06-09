static WORKER_WAKEUP_TEST_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

#[cfg(not(feature = "serial"))]
mod default_runtime {
    use futures::channel::oneshot;
    use futures::future::poll_fn;
    use pi_async_rt::rt::{
        multi_thread::{MultiTaskRuntimeBuilder, StealableTaskPool},
        worker_thread::WorkerTaskRunner,
        startup_global_time_loop,
        AsyncRuntime, AsyncValue,
    };
    use std::{
        future::Future,
        pin::Pin,
        sync::mpsc,
        task::Poll,
        thread,
        time::{Duration, Instant},
    };

    const SLEEP_TIMEOUT_MS: u64 = 80;
    const IDLE_WAKE_DELAY_MS: u64 = 20;
    const WAKE_THRESHOLD_MS: u64 = 50;

    fn new_multi_runtime(worker_size: usize, sleep_timeout_ms: u64) -> pi_async_rt::rt::multi_thread::MultiTaskRuntime<()> {
        let pool = StealableTaskPool::with(worker_size, 4096, [1, 1], 10);
        MultiTaskRuntimeBuilder::new(pool)
            .thread_prefix("Worker-Wakeup-Test")
            .thread_stack_size(2 * 1024 * 1024)
            .init_worker_size(worker_size)
            .set_worker_limit(worker_size, worker_size)
            .set_timeout(sleep_timeout_ms)
            .build()
    }

    fn new_multi_runtime_with_timer(worker_size: usize, sleep_timeout_ms: u64) -> pi_async_rt::rt::multi_thread::MultiTaskRuntime<()> {
        let pool = StealableTaskPool::with(worker_size, 4096, [1, 1], 10);
        MultiTaskRuntimeBuilder::new(pool)
            .thread_prefix("Worker-Wakeup-Timer-Test")
            .thread_stack_size(2 * 1024 * 1024)
            .init_worker_size(worker_size)
            .set_worker_limit(worker_size, worker_size)
            .set_timeout(sleep_timeout_ms)
            .set_timer_interval(1)
            .build()
    }

    #[test]
    fn test_multi_thread_external_oneshot_resumes_without_sleep_timeout() {
        let _test_lock = super::WORKER_WAKEUP_TEST_LOCK.lock();
        let rt = new_multi_runtime_with_timer(1, SLEEP_TIMEOUT_MS);
        let (ready_tx, ready_rx) = oneshot::channel::<Instant>();
        let (armed_tx, armed_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();

        rt.spawn(async move {
            let mut ready_rx = ready_rx;
            let mut armed_tx = Some(armed_tx);
            let started = poll_fn(move |cx| {
                match Pin::new(&mut ready_rx).poll(cx) {
                    Poll::Ready(result) => {
                        Poll::Ready(result.unwrap())
                    },
                    Poll::Pending => {
                        if let Some(armed_tx) = armed_tx.take() {
                            armed_tx.send(()).unwrap();
                        }
                        Poll::Pending
                    },
                }
            }).await;
            done_tx.send(started.elapsed()).unwrap();
        })
        .unwrap();

        armed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        thread::sleep(Duration::from_millis(IDLE_WAKE_DELAY_MS));
        let started = Instant::now();
        ready_tx.send(started).unwrap();

        let elapsed = done_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(
            elapsed < Duration::from_millis(WAKE_THRESHOLD_MS),
            "external oneshot wake elapsed {:?}, expected below {:?}",
            elapsed,
            Duration::from_millis(WAKE_THRESHOLD_MS)
        );
    }

    #[test]
    fn test_multi_thread_external_spawn_local_wakes_sleeping_worker() {
        let _test_lock = super::WORKER_WAKEUP_TEST_LOCK.lock();
        let rt = new_multi_runtime(1, SLEEP_TIMEOUT_MS);
        let (done_tx, done_rx) = mpsc::channel();

        thread::sleep(Duration::from_millis(IDLE_WAKE_DELAY_MS));
        let started = Instant::now();
        rt.spawn_local(async move {
            done_tx.send(started.elapsed()).unwrap();
        })
        .unwrap();

        let elapsed = done_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(
            elapsed < Duration::from_millis(WAKE_THRESHOLD_MS),
            "external spawn_local elapsed {:?}, expected below {:?}",
            elapsed,
            Duration::from_millis(WAKE_THRESHOLD_MS)
        );
    }

    #[test]
    fn test_multi_thread_external_spawn_by_id_and_priority_wake_sleeping_worker() {
        let _test_lock = super::WORKER_WAKEUP_TEST_LOCK.lock();
        let rt = new_multi_runtime(1, SLEEP_TIMEOUT_MS);

        thread::sleep(Duration::from_millis(SLEEP_TIMEOUT_MS + IDLE_WAKE_DELAY_MS));
        let (by_id_tx, by_id_rx) = mpsc::channel();
        let by_id_started = Instant::now();
        rt.spawn_by_id(rt.alloc::<()>(), async move {
            by_id_tx.send(by_id_started.elapsed()).unwrap();
        })
        .unwrap();
        let by_id_elapsed = by_id_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(
            by_id_elapsed < Duration::from_millis(WAKE_THRESHOLD_MS),
            "external spawn_by_id elapsed {:?}, expected below {:?}",
            by_id_elapsed,
            Duration::from_millis(WAKE_THRESHOLD_MS)
        );

        thread::sleep(Duration::from_millis(SLEEP_TIMEOUT_MS + IDLE_WAKE_DELAY_MS));
        let (priority_tx, priority_rx) = mpsc::channel();
        let priority_started = Instant::now();
        rt.spawn_priority(10, async move {
            priority_tx.send(priority_started.elapsed()).unwrap();
        })
        .unwrap();
        let priority_elapsed = priority_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(
            priority_elapsed < Duration::from_millis(WAKE_THRESHOLD_MS),
            "external spawn_priority elapsed {:?}, expected below {:?}",
            priority_elapsed,
            Duration::from_millis(WAKE_THRESHOLD_MS)
        );
    }

    #[test]
    fn test_multi_thread_async_value_set_wakes_without_sleep_timeout() {
        let _test_lock = super::WORKER_WAKEUP_TEST_LOCK.lock();
        let rt = new_multi_runtime(1, SLEEP_TIMEOUT_MS);
        let value = AsyncValue::<Instant>::new();
        let value_for_task = value.clone();
        let (armed_tx, armed_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();

        rt.spawn(async move {
            let mut value_for_task = value_for_task;
            let mut armed_tx = Some(armed_tx);
            let started = poll_fn(move |cx| {
                match Pin::new(&mut value_for_task).poll(cx) {
                    Poll::Ready(value) => {
                        Poll::Ready(value)
                    },
                    Poll::Pending => {
                        if let Some(armed_tx) = armed_tx.take() {
                            armed_tx.send(()).unwrap();
                        }
                        Poll::Pending
                    },
                }
            }).await;
            done_tx.send(started.elapsed()).unwrap();
        })
        .unwrap();

        armed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        thread::sleep(Duration::from_millis(SLEEP_TIMEOUT_MS + IDLE_WAKE_DELAY_MS));
        value.set(Instant::now());

        let elapsed = done_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(
            elapsed < Duration::from_millis(WAKE_THRESHOLD_MS),
            "AsyncValue wake elapsed {:?}, expected below {:?}",
            elapsed,
            Duration::from_millis(WAKE_THRESHOLD_MS)
        );
    }

    #[test]
    fn test_multi_thread_timeout_wakes_without_sleep_timeout() {
        let _test_lock = super::WORKER_WAKEUP_TEST_LOCK.lock();
        let _time_loop = startup_global_time_loop(1);
        let rt = new_multi_runtime(1, SLEEP_TIMEOUT_MS);
        let rt_for_task = rt.clone();
        let (done_tx, done_rx) = mpsc::channel();

        rt.spawn(async move {
            let started = Instant::now();
            rt_for_task.timeout(1).await;
            done_tx.send(started.elapsed()).unwrap();
        })
        .unwrap();

        let elapsed = done_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(
            elapsed < Duration::from_millis(WAKE_THRESHOLD_MS),
            "runtime.timeout wake elapsed {:?}, expected below {:?}",
            elapsed,
            Duration::from_millis(WAKE_THRESHOLD_MS)
        );
    }

    #[test]
    fn test_default_multi_thread_worker_size_can_exceed_cpu_count() {
        let _test_lock = super::WORKER_WAKEUP_TEST_LOCK.lock();
        let requested = num_cpus::get() + 1;
        let rt = pi_async_rt::rt::AsyncRuntimeBuilder::<()>::default_multi_thread(
            Some("Worker-Size-Boundary-Test"),
            None,
            Some(requested),
            Some(SLEEP_TIMEOUT_MS),
        );

        assert_eq!(rt.worker_len(), requested);
    }

    #[test]
    fn test_default_multi_thread_worker_size_zero_preserves_builder_fallback() {
        let _test_lock = super::WORKER_WAKEUP_TEST_LOCK.lock();
        let reference_rt = MultiTaskRuntimeBuilder::<()>::default()
            .thread_prefix("Worker-Size-Zero-Fallback-Reference")
            .init_worker_size(0)
            .set_worker_limit(0, 0)
            .set_timeout(SLEEP_TIMEOUT_MS)
            .build();
        let rt = pi_async_rt::rt::AsyncRuntimeBuilder::<()>::default_multi_thread(
            Some("Worker-Size-Zero-Fallback-Test"),
            None,
            Some(0),
            Some(SLEEP_TIMEOUT_MS),
        );
        let worker_len = rt.worker_len();
        let reference_worker_len = reference_rt.worker_len();
        let _ = rt.close();
        let _ = reference_rt.close();

        assert!(worker_len > 0);
        assert_eq!(worker_len, reference_worker_len);
    }

    #[test]
    fn test_worker_thread_external_spawn_wakes_sleeping_worker() {
        let _test_lock = super::WORKER_WAKEUP_TEST_LOCK.lock();
        let runner = WorkerTaskRunner::<()>::default();
        let runner_for_loop = runner.clone();
        let rt = runner.startup(
            "Direct-Worker-Wakeup-Test",
            2 * 1024 * 1024,
            SLEEP_TIMEOUT_MS,
            None,
            move || {
                let started = Instant::now();
                let len = runner_for_loop.run().unwrap();
                (len == 0, started.elapsed())
            },
            || 0,
        );
        let (done_tx, done_rx) = mpsc::channel();

        thread::sleep(Duration::from_millis(SLEEP_TIMEOUT_MS + IDLE_WAKE_DELAY_MS));
        let started = Instant::now();
        rt.spawn(async move {
            done_tx.send(started.elapsed()).unwrap();
        })
        .unwrap();

        let elapsed = done_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let _ = rt.close();

        assert!(
            elapsed < Duration::from_millis(WAKE_THRESHOLD_MS),
            "worker thread external spawn elapsed {:?}, expected below {:?}",
            elapsed,
            Duration::from_millis(WAKE_THRESHOLD_MS)
        );
    }
}
