static TIME_LOOP_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(not(feature = "serial"))]
mod default_runtime {
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
