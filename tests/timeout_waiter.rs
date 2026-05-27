static TIME_LOOP_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(not(feature = "serial"))]
mod default_runtime {
    use async_lock::Mutex as AsyncMutex;
    use futures::channel::oneshot;
    use futures::future::poll_fn;
    use pi_async_rt::rt::{
        multi_thread::{MultiTaskRuntimeBuilder, StealableTaskPool},
        single_thread::SingleTaskRunner,
        startup_global_time_loop, AsyncRuntime,
    };
    use std::{
        env,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        task::Poll,
        thread,
        time::{Duration, Instant},
    };

    #[test]
    fn test_single_thread_timeout_zero_and_drop_after_poll() {
        let _test_lock = super::TIME_LOOP_TEST_LOCK.lock().unwrap();
        let _time_loop = startup_global_time_loop(1);
        let runner = SingleTaskRunner::<()>::default();
        let rt = runner.startup().unwrap();
        let counter = Arc::new(AtomicUsize::new(0));
        let rt_for_task = rt.clone();
        let counter_for_task = counter.clone();

        rt.spawn(async move {
            rt_for_task.timeout(0).await;
            counter_for_task.fetch_add(1, Ordering::SeqCst);

            let rt_for_drop = rt_for_task.clone();
            poll_fn(move |cx| {
                let mut timeout = rt_for_drop.timeout(2);
                assert!(matches!(timeout.as_mut().poll(cx), Poll::Pending));
                Poll::Ready(())
            })
            .await;
            counter_for_task.fetch_add(1, Ordering::SeqCst);

            rt_for_task.timeout(5).await;
            counter_for_task.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();

        let started = Instant::now();
        while counter.load(Ordering::SeqCst) < 3 {
            assert!(
                started.elapsed() < Duration::from_secs(3),
                "single-thread timeout task stalled at count {}",
                counter.load(Ordering::SeqCst)
            );
            while runner.run().unwrap() > 0 {}
            thread::sleep(Duration::from_millis(1));
        }

        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_multi_thread_timeout_churn_completes() {
        let _test_lock = super::TIME_LOOP_TEST_LOCK.lock().unwrap();
        let _time_loop = startup_global_time_loop(1);
        let pool = StealableTaskPool::with(2, 4096, [1, 1], 10);
        let rt = MultiTaskRuntimeBuilder::new(pool)
            .thread_prefix("Timeout-Waiter-Test")
            .thread_stack_size(2 * 1024 * 1024)
            .init_worker_size(2)
            .set_worker_limit(2, 2)
            .set_timeout(2)
            .set_timer_interval(1)
            .build();

        thread::sleep(Duration::from_millis(50));

        let expected = 1600;
        let counter = Arc::new(AtomicUsize::new(0));
        let started_counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..16 {
            let rt_for_task = rt.clone();
            let counter_for_task = counter.clone();
            let started_for_task = started_counter.clone();
            rt.spawn(async move {
                started_for_task.fetch_add(1, Ordering::SeqCst);
                for _ in 0..100 {
                    rt_for_task.timeout(1).await;
                    counter_for_task.fetch_add(1, Ordering::SeqCst);
                }
            })
            .unwrap();
        }

        let started = Instant::now();
        while counter.load(Ordering::SeqCst) < expected {
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "multi-thread timeout churn stalled at started {}, count {} of {}",
                started_counter.load(Ordering::SeqCst),
                counter.load(Ordering::SeqCst),
                expected
            );
            thread::sleep(Duration::from_millis(1));
        }

        assert_eq!(counter.load(Ordering::SeqCst), expected);
    }

    #[test]
    fn test_multi_thread_timeout_churn_does_not_block_mutex_waiters() {
        let _test_lock = super::TIME_LOOP_TEST_LOCK.lock().unwrap();
        let _time_loop = startup_global_time_loop(1);
        let pool = StealableTaskPool::with(4, 4096, [1, 1], 10);
        let rt = MultiTaskRuntimeBuilder::new(pool)
            .thread_prefix("Timeout-Mutex-Test")
            .thread_stack_size(2 * 1024 * 1024)
            .init_worker_size(4)
            .set_worker_limit(4, 4)
            .set_timeout(2)
            .set_timer_interval(1)
            .build();

        thread::sleep(Duration::from_millis(50));

        let mutex = Arc::new(AsyncMutex::new(()));
        let completed = Arc::new(AtomicUsize::new(0));
        let churn_done = Arc::new(AtomicUsize::new(0));
        let expected_waiters = 128;

        for index in 0..expected_waiters {
            let mutex_for_task = mutex.clone();
            let completed_for_task = completed.clone();
            let rt_for_task = rt.clone();
            rt.spawn(async move {
                let _guard = mutex_for_task.lock().await;
                if index % 3 == 0 {
                    rt_for_task.timeout(1).await;
                }
                completed_for_task.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();
        }

        for _ in 0..8 {
            let rt_for_task = rt.clone();
            let churn_done_for_task = churn_done.clone();
            rt.spawn(async move {
                for _ in 0..200 {
                    rt_for_task.timeout(1).await;
                }
                churn_done_for_task.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();
        }

        let started = Instant::now();
        while completed.load(Ordering::SeqCst) < expected_waiters
            || churn_done.load(Ordering::SeqCst) < 8
        {
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "mutex waiter test stalled, completed={}, churn_done={}",
                completed.load(Ordering::SeqCst),
                churn_done.load(Ordering::SeqCst)
            );
            thread::sleep(Duration::from_millis(1));
        }

        assert_eq!(completed.load(Ordering::SeqCst), expected_waiters);
        assert_eq!(churn_done.load(Ordering::SeqCst), 8);
    }

    #[test]
    fn test_multi_thread_timeout_churn_does_not_block_oneshot_append_shape() {
        let _test_lock = super::TIME_LOOP_TEST_LOCK.lock().unwrap();
        let _time_loop = startup_global_time_loop(1);
        let pool = StealableTaskPool::with(4, 4096, [1, 1], 10);
        let rt = MultiTaskRuntimeBuilder::new(pool)
            .thread_prefix("Timeout-Oneshot-Test")
            .thread_stack_size(2 * 1024 * 1024)
            .init_worker_size(4)
            .set_worker_limit(4, 4)
            .set_timeout(2)
            .set_timer_interval(1)
            .build();

        thread::sleep(Duration::from_millis(50));

        let expected_appends = 512;
        let completed = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));

        for index in 0..expected_appends {
            let rt_for_front = rt.clone();
            let rt_for_back = rt.clone();
            let rt_for_back_task = rt_for_back.clone();
            let completed_for_task = completed.clone();
            let dropped_for_task = dropped.clone();
            rt.spawn(async move {
                let (sender, receiver) = oneshot::channel::<usize>();
                rt_for_back
                    .spawn(async move {
                        if index % 2 == 0 {
                            rt_for_back_task.timeout(1).await;
                        } else {
                            rt_for_back_task.timeout(0).await;
                        }
                        let _ = sender.send(index);
                    })
                    .unwrap();

                if index % 17 == 0 {
                    drop(receiver);
                    dropped_for_task.fetch_add(1, Ordering::SeqCst);
                    return;
                }

                let value = receiver.await.expect("append reply sender dropped");
                assert_eq!(value, index);
                if index % 5 == 0 {
                    rt_for_front.timeout(1).await;
                }
                completed_for_task.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();
        }

        let expected_dropped = (0..expected_appends).filter(|index| index % 17 == 0).count();
        let expected_completed = expected_appends - expected_dropped;
        let started = Instant::now();
        while completed.load(Ordering::SeqCst) < expected_completed
            || dropped.load(Ordering::SeqCst) < expected_dropped
        {
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "oneshot append shape stalled, completed={} of {}, dropped={} of {}",
                completed.load(Ordering::SeqCst),
                expected_completed,
                dropped.load(Ordering::SeqCst),
                expected_dropped
            );
            thread::sleep(Duration::from_millis(1));
        }

        assert_eq!(completed.load(Ordering::SeqCst), expected_completed);
        assert_eq!(dropped.load(Ordering::SeqCst), expected_dropped);
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore]
    fn test_multi_thread_timeout_churn_rss_diagnostic() {
        let _test_lock = super::TIME_LOOP_TEST_LOCK.lock().unwrap();
        let _time_loop = startup_global_time_loop(1);
        let task_count = env_usize("PI_ASYNC_RT_TIMEOUT_CHURN_TASKS", 64);
        let loops = env_usize("PI_ASYNC_RT_TIMEOUT_CHURN_LOOPS", 12000);
        let timeout_ms = env_usize("PI_ASYNC_RT_TIMEOUT_CHURN_MS", 1);
        let cooldown_ms = env_usize("PI_ASYNC_RT_TIMEOUT_CHURN_COOLDOWN_MS", 1000);
        let max_growth_kb = env_usize("PI_ASYNC_RT_TIMEOUT_CHURN_MAX_GROWTH_KB", 65536);
        let expected = task_count * loops;
        let pool = StealableTaskPool::with(4, 8192, [1, 1], 10);
        let rt = MultiTaskRuntimeBuilder::new(pool)
            .thread_prefix("Timeout-Waiter-Rss")
            .thread_stack_size(2 * 1024 * 1024)
            .init_worker_size(4)
            .set_worker_limit(4, 4)
            .set_timeout(2)
            .set_timer_interval(1)
            .build();

        thread::sleep(Duration::from_millis(50));

        let rss_before_kb = linux_rss_kb().expect("failed to read rss before timeout churn");
        let counter = Arc::new(AtomicUsize::new(0));

        for _ in 0..task_count {
            let rt_for_task = rt.clone();
            let counter_for_task = counter.clone();
            rt.spawn(async move {
                for _ in 0..loops {
                    rt_for_task.timeout(timeout_ms).await;
                    counter_for_task.fetch_add(1, Ordering::SeqCst);
                }
            })
            .unwrap();
        }

        let started = Instant::now();
        while counter.load(Ordering::SeqCst) < expected {
            assert!(
                started.elapsed() < Duration::from_secs((loops as u64 / 1000).max(10) + 30),
                "timeout rss diagnostic stalled at count {} of {}",
                counter.load(Ordering::SeqCst),
                expected
            );
            thread::sleep(Duration::from_millis(5));
        }

        thread::sleep(Duration::from_millis(cooldown_ms as u64));
        let rss_after_kb = linux_rss_kb().expect("failed to read rss after timeout churn");
        let growth_kb = rss_after_kb.saturating_sub(rss_before_kb);
        eprintln!(
            "timeout churn rss diagnostic: calls={}, rss_before_kb={}, rss_after_kb={}, growth_kb={}, max_growth_kb={}",
            expected, rss_before_kb, rss_after_kb, growth_kb, max_growth_kb
        );

        assert!(
            growth_kb <= max_growth_kb,
            "timeout churn rss growth too high: calls={}, growth_kb={}, max_growth_kb={}",
            expected,
            growth_kb,
            max_growth_kb
        );
    }

    #[cfg(target_os = "linux")]
    fn env_usize(name: &str, default: usize) -> usize {
        env::var(name)
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .unwrap_or(default)
    }

    #[cfg(target_os = "linux")]
    fn linux_rss_kb() -> Option<usize> {
        std::fs::read_to_string("/proc/self/status")
            .ok()?
            .lines()
            .find(|line| line.starts_with("VmRSS:"))
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|value| value.parse::<usize>().ok())
    }
}

#[cfg(feature = "serial")]
mod serial_runtime {
    use futures::future::poll_fn;
    use pi_async_rt::rt::{
        serial::AsyncRuntime,
        serial_single_thread::SingleTaskRunner,
        startup_global_time_loop,
    };
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        task::Poll,
        thread,
        time::{Duration, Instant},
    };

    #[test]
    fn test_serial_single_thread_timeout_zero_and_drop_after_poll() {
        let _test_lock = super::TIME_LOOP_TEST_LOCK.lock().unwrap();
        let _time_loop = startup_global_time_loop(1);
        let runner = SingleTaskRunner::<()>::default();
        let rt = runner.startup().unwrap();
        let counter = Arc::new(AtomicUsize::new(0));
        let rt_for_task = rt.clone();
        let counter_for_task = counter.clone();

        rt.spawn(async move {
            rt_for_task.timeout(0).await;
            counter_for_task.fetch_add(1, Ordering::SeqCst);

            let rt_for_drop = rt_for_task.clone();
            poll_fn(move |cx| {
                let mut timeout = rt_for_drop.timeout(2);
                assert!(matches!(timeout.as_mut().poll(cx), Poll::Pending));
                Poll::Ready(())
            })
            .await;
            counter_for_task.fetch_add(1, Ordering::SeqCst);

            rt_for_task.timeout(5).await;
            counter_for_task.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();

        let started = Instant::now();
        while counter.load(Ordering::SeqCst) < 3 {
            assert!(
                started.elapsed() < Duration::from_secs(3),
                "serial single-thread timeout task stalled at count {}",
                counter.load(Ordering::SeqCst)
            );
            while runner.run().unwrap() > 0 {}
            thread::sleep(Duration::from_millis(1));
        }

        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }
}
