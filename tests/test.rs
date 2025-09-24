extern crate crossbeam_channel;
extern crate dashmap;
extern crate futures;
extern crate pi_async_rt;
extern crate tokio;
extern crate twox_hash;

#[allow(unused_imports)]
#[macro_use]
extern crate env_logger;

use std::thread;
use std::time::{Duration, Instant};
use std::sync::{Arc, Mutex,
                atomic::{AtomicUsize, Ordering}};

use futures::future::{FutureExt, LocalBoxFuture};
use pi_async_rt::{lock::spin_lock::SpinLock,
                  rt::{startup_global_time_loop, AsyncRuntime, multi_thread::{MultiTaskRuntimeBuilder, StealableTaskPool},
                       serial_local_thread::{LocalTaskRunner, LocalTaskRuntime},
                       single_thread::{SingleTaskRunner, SingleTaskPool}}};
use polling::Poller;

struct AtomicCounter(AtomicUsize, Instant);
impl Drop for AtomicCounter {
    fn drop(&mut self) {
        {
            println!(
                "!!!!!!drop counter, count: {:?}, time: {:?}",
                self.0.load(Ordering::Relaxed),
                Instant::now() - self.1
            );
        }
    }
}

#[test]
fn test_local_task_runtime_with_poll() {
    let poller = Poller::new().unwrap();
    let runner = LocalTaskRunner::with_poll(Arc::new(poller));
    let rt = runner.startup_with_poll("Test-Local-RT",
                             2 * 1024 * 1024,
                             3,
                             Some(Duration::from_millis(10)));

    for index in 0..10 {
        thread::sleep(Duration::from_millis(1000));
        rt.send(async move {
            println!("index: {:?}", index);
        });
    }

    thread::sleep(Duration::from_millis(10000));
}

#[test]
fn test_empty_local_task() {
    thread::sleep(Duration::from_millis(10000));

    let rt = LocalTaskRunner::new().into_local();
    let rt_copy = rt.clone();

    let start = Instant::now();
    let _ = rt.block_on(async move {
        let start = Instant::now();
        for _ in 0..10000000 {
            rt_copy.spawn(async move {});
        }
        println!("!!!!!!spawn local task ok, time: {:?}", Instant::now() - start);
    });
    println!("!!!!!!block on ok, time: {:?}", Instant::now() - start);

    thread::sleep(Duration::from_millis(10000));

    let counter = Arc::new(AtomicCounter(AtomicUsize::new(0), Instant::now()));
    let start = Instant::now();
    let _ = rt.block_on(loop_local_task(rt.clone(), counter, 0, start));

    thread::sleep(Duration::from_millis(10000));

    let runner = LocalTaskRunner::new();
    let rt = runner.get_runtime();

    thread::spawn(move || {
        let start = Instant::now();
        for _ in 0..10000000 {
            rt.spawn(async move {});
            runner.run_once();
        }
        println!("!!!!!!local task ok, time: {:?}", Instant::now() - start);
    });

    thread::sleep(Duration::from_millis(10000));

    let runner = LocalTaskRunner::new();
    let rt = runner.get_runtime();
    thread::spawn(move || loop {
        runner.run_once();
    });

    thread::spawn(move || {
        let counter = Arc::new(AtomicCounter(AtomicUsize::new(0), Instant::now()));
        let start = Instant::now();
        for _ in 0..10000000 {
            let counter_copy = counter.clone();
            rt.send(async move {
                counter_copy.0.fetch_add(1, Ordering::Relaxed);
            });
        }
        println!("!!!!!!spawn local task ok, time: {:?}", Instant::now() - start);
    });

    thread::sleep(Duration::from_millis(100000000));
}

fn loop_local_task(
    rt: LocalTaskRuntime<()>,
    counter: Arc<AtomicCounter>,
    count: usize,
    time: Instant,
) -> LocalBoxFuture<'static, ()> {
    if count >= 10000000 {
        println!("!!!!!!spawn local task ok, time: {:?}", Instant::now() - time);
        return async move {}.boxed_local();
    }

    let counter_copy = counter.clone();
    rt.spawn(async move {
        counter_copy.0.fetch_add(1, Ordering::Relaxed);
    });

    async move {
        rt.spawn(loop_local_task(rt.clone(), counter, count + 1, time));
    }
    .boxed_local()
}

#[test]
fn test_empty_single_task() {
    let _handle = startup_global_time_loop(100);

    thread::sleep(Duration::from_millis(10000));

    let pool = SingleTaskPool::new([254, 1]);
    let runner0 = SingleTaskRunner::new(pool);
    let rt0 = runner0.startup().unwrap();
    let pool = SingleTaskPool::new([254, 1]);
    let runner1 = SingleTaskRunner::new(pool);
    let rt1 = runner1.startup().unwrap();
    let pool = SingleTaskPool::new([254, 1]);
    let runner2 = SingleTaskRunner::new(pool);
    let rt2 = runner2.startup().unwrap();
    let pool = SingleTaskPool::new([254, 1]);
    let runner3 = SingleTaskRunner::new(pool);
    let rt3 = runner3.startup().unwrap();

    thread::spawn(move || loop {
        if let Err(e) = runner0.run() {
            println!("!!!!!!run failed, reason: {:?}", e);
            break;
        }
        thread::sleep(Duration::from_millis(10));
    });

    thread::spawn(move || loop {
        if let Err(e) = runner1.run() {
            println!("!!!!!!run failed, reason: {:?}", e);
            break;
        }
        thread::sleep(Duration::from_millis(10));
    });

    thread::spawn(move || loop {
        if let Err(e) = runner2.run() {
            println!("!!!!!!run failed, reason: {:?}", e);
            break;
        }
        thread::sleep(Duration::from_millis(10));
    });

    thread::spawn(move || loop {
        if let Err(e) = runner3.run() {
            println!("!!!!!!run failed, reason: {:?}", e);
            break;
        }
        thread::sleep(Duration::from_millis(10));
    });

    thread::spawn(move || {
        let counter = Arc::new(AtomicCounter(AtomicUsize::new(0), Instant::now()));
        let start = Instant::now();
        for _ in 0..10000000 {
            let counter_copy = counter.clone();
            if let Err(e) = rt0.spawn(async move {
                counter_copy.0.fetch_add(1, Ordering::Relaxed);
            }) {
                println!("!!!> spawn empty singale task failed, reason: {:?}", e);
            }
        }
        println!("!!!!!!spawn single timing task ok, time: {:?}", Instant::now() - start);
    });

    thread::spawn(move || {
        let counter = Arc::new(AtomicCounter(AtomicUsize::new(0), Instant::now()));
        let start = Instant::now();
        for _ in 0..10000000 {
            let counter_copy = counter.clone();
            if let Err(e) = rt1.spawn(async move {
                counter_copy.0.fetch_add(1, Ordering::Relaxed);
            }) {
                println!("!!!> spawn empty singale task failed, reason: {:?}", e);
            }
        }
        println!("!!!!!!spawn single timing task ok, time: {:?}", Instant::now() - start);
    });

    thread::spawn(move || {
        let counter = Arc::new(AtomicCounter(AtomicUsize::new(0), Instant::now()));
        let start = Instant::now();
        for _ in 0..10000000 {
            let counter_copy = counter.clone();
            if let Err(e) = rt2.spawn(async move {
                counter_copy.0.fetch_add(1, Ordering::Relaxed);
            }) {
                println!("!!!> spawn empty singale task failed, reason: {:?}", e);
            }
        }
        println!("!!!!!!spawn single timing task ok, time: {:?}", Instant::now() - start);
    });

    thread::spawn(move || {
        let counter = Arc::new(AtomicCounter(AtomicUsize::new(0), Instant::now()));
        let start = Instant::now();
        for _ in 0..10000000 {
            let counter_copy = counter.clone();
            if let Err(e) = rt3.spawn(async move {
                counter_copy.0.fetch_add(1, Ordering::Relaxed);
            }) {
                println!("!!!> spawn empty singale task failed, reason: {:?}", e);
            }
        }
        println!("!!!!!!spawn single timing task ok, time: {:?}", Instant::now() - start);
    });

    thread::sleep(Duration::from_millis(10000));

    let runner = SingleTaskRunner::default();
    let rt = runner.startup().unwrap();

    thread::spawn(move || {
        let counter = Arc::new(AtomicCounter(AtomicUsize::new(0), Instant::now()));
        let start = Instant::now();
        for _ in 0..10000000 {
            let counter_copy = counter.clone();
            if let Err(e) = rt.spawn(async move {
                counter_copy.0.fetch_add(1, Ordering::Relaxed);
            }) {
                println!("!!!> spawn empty singale task failed, reason: {:?}", e);
            }
        }
        let _ = runner.run();
        println!("!!!!!!spawn single timing task ok, time: {:?}", Instant::now() - start);
    });

    thread::sleep(Duration::from_millis(100000000));
}

#[test]
fn test_empty_single_task_by_internal() {
    let _handle = startup_global_time_loop(100);

    thread::sleep(Duration::from_millis(10000));

    let pool = SingleTaskPool::new([1, 254]);
    let runner = SingleTaskRunner::new(pool);
    let rt = runner.startup().unwrap();

    thread::spawn(move || loop {
        if let Err(e) = runner.run() {
            println!("!!!!!!run failed, reason: {:?}", e);
            break;
        }
        thread::sleep(Duration::from_millis(10));
    });

    //测试派发定时任务的性能
    let rt_copy = rt.clone();
    let start = Instant::now();
    let _ = rt.spawn(async move {
        let counter = Arc::new(AtomicCounter(AtomicUsize::new(0), Instant::now()));
        for _ in 0..10000000 {
            let counter_copy = counter.clone();
            let _ = rt_copy.spawn_local(async move {
                counter_copy.0.fetch_add(1, Ordering::Relaxed);
            });
        }
        println!("!!!!!!spawn single timing task ok, time: {:?}", Instant::now() - start);
    });

    thread::sleep(Duration::from_millis(10000));

    let runner = SingleTaskRunner::default();
    let rt = runner.startup().unwrap();

    thread::spawn(move || {
        let counter = Arc::new(AtomicCounter(AtomicUsize::new(0), Instant::now()));
        let start = Instant::now();
        for _ in 0..10000000 {
            let counter_copy = counter.clone();
            if let Err(e) = rt.spawn_local(async move {
                counter_copy.0.fetch_add(1, Ordering::Relaxed);
            }) {
                println!("!!!> spawn empty singale task failed, reason: {:?}", e);
            }
            let _ = runner.run_once();
        }
        println!("!!!!!!spawn single timing task ok, time: {:?}", Instant::now() - start);
    });

    thread::sleep(Duration::from_millis(100000000));
}

#[test]
fn test_empty_multi_task() {
    let _handle = startup_global_time_loop(100);

    thread::sleep(Duration::from_millis(10000));

    let pool = StealableTaskPool::with(4, 10000, [254, 1], 3000);
    let rt = MultiTaskRuntimeBuilder::new(pool)
        .thread_stack_size(2 * 1024 * 1024)
        .init_worker_size(4)
        .set_worker_limit(4, 4)
        .build();
    let rt0 = rt.clone();
    let rt1 = rt.clone();
    let rt2 = rt.clone();
    let rt3 = rt.clone();

    //测试派发定时任务的性能
    {
        let counter = Arc::new(AtomicCounter(AtomicUsize::new(0), Instant::now()));
        let counter0 = counter.clone();
        let counter1 = counter.clone();
        let counter2 = counter.clone();
        let counter3 = counter.clone();

        thread::spawn(move || {
            let start = Instant::now();
            for _ in 0..2500000 {
                let counter_copy = counter0.clone();
                if let Err(e) = rt0.spawn(async move {
                    counter_copy.0.fetch_add(1, Ordering::Relaxed);
                }) {
                    println!("!!!> spawn empty singale task failed, reason: {:?}", e);
                }
            }
            println!("!!!!!!spawn single timing task ok 0, time: {:?}", Instant::now() - start);
        });

        thread::spawn(move || {
            let start = Instant::now();
            for _ in 2500000..5000000 {
                let counter_copy = counter1.clone();
                if let Err(e) = rt1.spawn(async move {
                    counter_copy.0.fetch_add(1, Ordering::Relaxed);
                }) {
                    println!("!!!> spawn empty singale task failed, reason: {:?}", e);
                }
            }
            println!("!!!!!!spawn single timing task ok 1, time: {:?}", Instant::now() - start);
        });

        thread::spawn(move || {
            let start = Instant::now();
            for _ in 5000000..7500000 {
                let counter_copy = counter2.clone();
                if let Err(e) = rt2.spawn(async move {
                    counter_copy.0.fetch_add(1, Ordering::Relaxed);
                }) {
                    println!("!!!> spawn empty singale task failed, reason: {:?}", e);
                }
            }
            println!("!!!!!!spawn single timing task ok 2, time: {:?}", Instant::now() - start);
        });

        thread::spawn(move || {
            let start = Instant::now();
            for _ in 7500000..10000000 {
                let counter_copy = counter3.clone();
                if let Err(e) = rt3.spawn(async move {
                    counter_copy.0.fetch_add(1, Ordering::Relaxed);
                }) {
                    println!("!!!> spawn empty singale task failed, reason: {:?}", e);
                }
            }
            println!("!!!!!!spawn single timing task ok 3, time: {:?}", Instant::now() - start);
        });
    }

    thread::sleep(Duration::from_millis(10000));

    let pool = StealableTaskPool::with(4, 10000, [254, 1], 3000);
    let rt = MultiTaskRuntimeBuilder::new(pool)
        .thread_stack_size(2 * 1024 * 1024)
        .init_worker_size(4)
        .set_worker_limit(4, 4)
        .build();
    let rt0 = rt.clone();
    let rt1 = rt.clone();
    let rt2 = rt.clone();
    let rt3 = rt.clone();

    //测试派发定时任务的性能
    {
        let counter = Arc::new(AtomicCounter(AtomicUsize::new(0), Instant::now()));
        let counter0 = counter.clone();
        let counter1 = counter.clone();
        let counter2 = counter.clone();
        let counter3 = counter.clone();

        thread::spawn(move || {
            let start = Instant::now();
            for _ in 0..2500000 {
                let counter_copy = counter0.clone();
                if let Err(e) = rt0.spawn(async move {
                    counter_copy.0.fetch_add(1, Ordering::Relaxed);
                }) {
                    println!("!!!> spawn empty singale task failed, reason: {:?}", e);
                }
            }
            println!("!!!!!!spawn single timing task ok 0, time: {:?}", Instant::now() - start);
        });

        thread::spawn(move || {
            let start = Instant::now();
            for _ in 2500000..5000000 {
                let counter_copy = counter1.clone();
                if let Err(e) = rt1.spawn(async move {
                    counter_copy.0.fetch_add(1, Ordering::Relaxed);
                }) {
                    println!("!!!> spawn empty singale task failed, reason: {:?}", e);
                }
            }
            println!("!!!!!!spawn single timing task ok 1, time: {:?}", Instant::now() - start);
        });

        thread::spawn(move || {
            let start = Instant::now();
            for _ in 5000000..7500000 {
                let counter_copy = counter2.clone();
                if let Err(e) = rt2.spawn(async move {
                    counter_copy.0.fetch_add(1, Ordering::Relaxed);
                }) {
                    println!("!!!> spawn empty singale task failed, reason: {:?}", e);
                }
            }
            println!("!!!!!!spawn single timing task ok 2, time: {:?}", Instant::now() - start);
        });

        thread::spawn(move || {
            let start = Instant::now();
            for _ in 7500000..10000000 {
                let counter_copy = counter3.clone();
                if let Err(e) = rt3.spawn(async move {
                    counter_copy.0.fetch_add(1, Ordering::Relaxed);
                }) {
                    println!("!!!> spawn empty singale task failed, reason: {:?}", e);
                }
            }
            println!("!!!!!!spawn single timing task ok 3, time: {:?}", Instant::now() - start);
        });
    }

    thread::sleep(Duration::from_millis(100000000));
}

#[test]
fn test_empty_multi_task_by_internal() {
    let _handle = startup_global_time_loop(100);

    thread::sleep(Duration::from_millis(10000));

    let pool = StealableTaskPool::with(6, 10000000, [1, 254], 3000);
    let rt = MultiTaskRuntimeBuilder::new(pool)
        .thread_stack_size(2 * 1024 * 1024)
        .init_worker_size(6)
        .set_worker_limit(6, 6)
        .build();
    let rt0 = rt.clone();
    let rt1 = rt.clone();
    let rt2 = rt.clone();
    let rt3 = rt.clone();

    //测试派发定时任务的性能
    {
        let counter = Arc::new(AtomicCounter(AtomicUsize::new(0), Instant::now()));
        let counter0 = counter.clone();
        let counter1 = counter.clone();
        let counter2 = counter.clone();
        let counter3 = counter.clone();

        let _ = rt.spawn(async move {
            let start = Instant::now();
            for _ in 0..2500000 {
                let counter_copy = counter0.clone();
                if let Err(e) = rt0.spawn_local(async move {
                    counter_copy.0.fetch_add(1, Ordering::Relaxed);
                }) {
                    println!("!!!> spawn empty singale task failed, reason: {:?}", e);
                }
            }
            println!("!!!!!!spawn single timing task ok 0, time: {:?}", Instant::now() - start);
        });

        let _ = rt.spawn(async move {
            let start = Instant::now();
            for _ in 2500000..5000000 {
                let counter_copy = counter1.clone();
                if let Err(e) = rt1.spawn_local(async move {
                    counter_copy.0.fetch_add(1, Ordering::Relaxed);
                }) {
                    println!("!!!> spawn empty singale task failed, reason: {:?}", e);
                }
            }
            println!("!!!!!!spawn single timing task ok 1, time: {:?}", Instant::now() - start);
        });

        let _ = rt.spawn(async move {
            let start = Instant::now();
            for _ in 5000000..7500000 {
                let counter_copy = counter2.clone();
                if let Err(e) = rt2.spawn_local(async move {
                    counter_copy.0.fetch_add(1, Ordering::Relaxed);
                }) {
                    println!("!!!> spawn empty singale task failed, reason: {:?}", e);
                }
            }
            println!("!!!!!!spawn single timing task ok 2, time: {:?}", Instant::now() - start);
        });

        let _ = rt.spawn(async move {
            let start = Instant::now();
            for _ in 7500000..10000000 {
                let counter_copy = counter3.clone();
                if let Err(e) = rt3.spawn_local(async move {
                    counter_copy.0.fetch_add(1, Ordering::Relaxed);
                }) {
                    println!("!!!> spawn empty singale task failed, reason: {:?}", e);
                }
            }
            println!("!!!!!!spawn single timing task ok 3, time: {:?}", Instant::now() - start);
        });
    }

    thread::sleep(Duration::from_millis(10000));

    let pool = StealableTaskPool::with(7, 10000, [1, 254], 3000);
    let rt = MultiTaskRuntimeBuilder::new(pool)
        .thread_stack_size(2 * 1024 * 1024)
        .init_worker_size(7)
        .set_worker_limit(7, 7)
        .build();
    let rt0 = rt.clone();
    let rt1 = rt.clone();
    let rt2 = rt.clone();
    let rt3 = rt.clone();

    //测试派发定时任务的性能
    {
        let counter = Arc::new(AtomicCounter(AtomicUsize::new(0), Instant::now()));
        let counter0 = counter.clone();
        let counter1 = counter.clone();
        let counter2 = counter.clone();
        let counter3 = counter.clone();

        let _ = rt.spawn(async move {
            let start = Instant::now();
            for _ in 0..2500000 {
                let counter_copy = counter0.clone();
                if let Err(e) = rt0.spawn_local(async move {
                    counter_copy.0.fetch_add(1, Ordering::Relaxed);
                }) {
                    println!("!!!> spawn empty singale task failed, reason: {:?}", e);
                }
            }
            println!("!!!!!!spawn single timing task ok 0, time: {:?}", Instant::now() - start);
        });

        let _ = rt.spawn(async move {
            let start = Instant::now();
            for _ in 2500000..5000000 {
                let counter_copy = counter1.clone();
                if let Err(e) = rt1.spawn_local(async move {
                    counter_copy.0.fetch_add(1, Ordering::Relaxed);
                }) {
                    println!("!!!> spawn empty singale task failed, reason: {:?}", e);
                }
            }
            println!("!!!!!!spawn single timing task ok 1, time: {:?}", Instant::now() - start);
        });

        let _ = rt.spawn(async move {
            let start = Instant::now();
            for _ in 5000000..7500000 {
                let counter_copy = counter2.clone();
                if let Err(e) = rt2.spawn_local(async move {
                    counter_copy.0.fetch_add(1, Ordering::Relaxed);
                }) {
                    println!("!!!> spawn empty singale task failed, reason: {:?}", e);
                }
            }
            println!("!!!!!!spawn single timing task ok 2, time: {:?}", Instant::now() - start);
        });

        let _ = rt.spawn(async move {
            let start = Instant::now();
            for _ in 7500000..10000000 {
                let counter_copy = counter3.clone();
                if let Err(e) = rt3.spawn_local(async move {
                    counter_copy.0.fetch_add(1, Ordering::Relaxed);
                }) {
                    println!("!!!> spawn empty singale task failed, reason: {:?}", e);
                }
            }
            println!("!!!!!!spawn single timing task ok 3, time: {:?}", Instant::now() - start);
        });
    }

    thread::sleep(Duration::from_millis(100000000));
}

#[test]
fn test_timeout() {
    let _handle = startup_global_time_loop(10);
    let pool = StealableTaskPool::with(8, 1000000, [1, 1], 3000);
    let builder = MultiTaskRuntimeBuilder::new(pool)
        .thread_prefix("PI-SERV-FILE")
        .thread_stack_size(2 * 1024 * 1024)
        .init_worker_size(8)
        .set_worker_limit(8, 8)
        .set_timeout(10)
        .set_timer_interval(1);
    let rt = builder.build();

    thread::sleep(Duration::from_millis(1000));

    {
        let counter = Arc::new(AtomicCounter(AtomicUsize::new(0), Instant::now()));
        let rt_copy = rt.clone();
        rt.spawn(async move {
            for _ in 0..10000 {
                let rt_clone = rt_copy.clone();
                let counter_copy = counter.clone();
                rt_copy.spawn(async move {
                    rt_clone.timeout(1).await;
                    counter_copy
                        .0
                        .fetch_add(1, Ordering::Relaxed);
                });
            }
        });
    }

    thread::sleep(Duration::from_millis(1000000000));
}

#[test]
fn test_spin_lock() {
    let lock = SpinLock::new(0);
    let start = Instant::now();
    thread::scope(|s| {
        for _ in 0..1000 {
            s.spawn(|| {
                for _ in 0..10000 {
                    *lock.lock() += 1;
                }
            });
        }
    });
    assert_eq!(*lock.lock(), 10_000_000);
    println!("Test SpinLock time: {:?}", start.elapsed());

    let mut lock = spin_mutex::Mutex::<usize>::new(0);
    let start = Instant::now();
    thread::scope(|s| {
        for _ in 0..1000 {
            s.spawn(|| {
                for _ in 0..10000 {
                    *lock.lock() += 1;
                }
            });
        }
    });
    assert_eq!(*lock.lock(), 10_000_000);
    println!("Test spin::Mutex time: {:?}", start.elapsed());

    let lock = Mutex::new(0);
    let start = Instant::now();
    thread::scope(|s| {
        for _ in 0..1000 {
            s.spawn(|| {
                for _ in 0..10000 {
                    *lock.lock().unwrap() += 1;
                }
            });
        }
    });
    assert_eq!(*lock.lock().unwrap(), 10_000_000);
    println!("Test std::sync::Mutex time: {:?}", start.elapsed());

    let lock = parking_lot::Mutex::new(0);
    let start = Instant::now();
    thread::scope(|s| {
        for _ in 0..1000 {
            s.spawn(|| {
                for _ in 0..10000 {
                    *lock.lock() += 1;
                }
            });
        }
    });
    assert_eq!(*lock.lock(), 10_000_000);
    println!("Test parking_lot::Mutex time: {:?}", start.elapsed());
}
