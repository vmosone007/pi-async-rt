#![feature(test)]

extern crate test;

#[cfg(not(feature = "serial"))]
mod default_benches {
    use futures::{future::poll_fn, task::{waker_ref, ArcWake}};
    use pi_async_rt::rt::{
        multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder, StealableTaskPool},
        single_thread::SingleTaskRunner,
        AsyncRuntime, AsyncValue,
    };
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            atomic::{AtomicUsize, Ordering},
            mpsc::{self, Receiver, TryRecvError},
            Arc,
        },
        task::Poll,
        thread,
        time::{Duration, Instant},
    };
    use test::{black_box, Bencher};

    const WORKER_SIZE: usize = 4;
    const TASKS_PER_ITER: usize = 256;
    const SINGLE_TASKS_PER_ITER: usize = 128;
    const CROSS_TASKS_PER_ITER: usize = 64;
    const BENCH_TIMEOUT: Duration = Duration::from_secs(10);

    struct CountWake {
        count: AtomicUsize,
    }

    impl CountWake {
        fn new() -> Arc<Self> {
            Arc::new(CountWake {
                count: AtomicUsize::new(0),
            })
        }
    }

    impl ArcWake for CountWake {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.count.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn poll_with_wake<F: Future + Unpin>(future: &mut F, wake: &Arc<CountWake>) -> Poll<F::Output> {
        let waker = waker_ref(wake);
        let mut cx = std::task::Context::from_waker(&*waker);
        Pin::new(future).poll(&mut cx)
    }

    fn new_runtime() -> MultiTaskRuntime<()> {
        let pool = StealableTaskPool::with(WORKER_SIZE, 4096, [1, 1], 10);
        MultiTaskRuntimeBuilder::new(pool)
            .thread_prefix("AsyncValue-Bench")
            .thread_stack_size(2 * 1024 * 1024)
            .init_worker_size(WORKER_SIZE)
            .set_worker_limit(WORKER_SIZE, WORKER_SIZE)
            .set_timeout(80)
            .build()
    }

    fn drive_single_until_count<T>(runner: &SingleTaskRunner<()>,
                                   receiver: &Receiver<T>,
                                   expected: usize) {
        let started = Instant::now();
        let mut count = 0;
        while count < expected {
            match receiver.try_recv() {
                Ok(_) => {
                    count += 1;
                },
                Err(TryRecvError::Empty) => {},
                Err(TryRecvError::Disconnected) => {
                    panic!("single-thread bench receiver disconnected at {} of {}", count, expected);
                },
            }

            if count < expected {
                if started.elapsed() >= BENCH_TIMEOUT {
                    panic!("single-thread AsyncValue bench timed out at {} of {}", count, expected);
                }
                while runner.run().unwrap() > 0 {}
                thread::yield_now();
            }
        }
    }

    #[bench]
    fn bench_async_value_pending_poll(b: &mut Bencher) {
        let mut value = AsyncValue::<usize>::new();
        let wake = CountWake::new();

        b.iter(|| {
            match poll_with_wake(&mut value, &wake) {
                Poll::Pending => {
                    black_box(());
                },
                Poll::Ready(_) => {
                    panic!("never-set AsyncValue unexpectedly became ready");
                },
            }
        });
    }

    #[bench]
    fn bench_async_value_set_then_ready(b: &mut Bencher) {
        let wake = CountWake::new();

        b.iter(|| {
            let value = AsyncValue::<usize>::new();
            let mut receiver = value.clone();

            match poll_with_wake(&mut receiver, &wake) {
                Poll::Pending => {},
                Poll::Ready(_) => {
                    panic!("unset AsyncValue unexpectedly became ready");
                },
            }
            value.set(black_box(1));
            match poll_with_wake(&mut receiver, &wake) {
                Poll::Ready(value) => {
                    black_box(value);
                },
                Poll::Pending => {
                    panic!("set AsyncValue stayed pending");
                },
            }
        });
    }

    #[bench]
    fn bench_multi_thread_async_value_set_await_batch(b: &mut Bencher) {
        let rt = new_runtime();

        b.iter(|| {
            let (done_tx, done_rx) = mpsc::channel();

            for _ in 0..TASKS_PER_ITER {
                let value = AsyncValue::<usize>::new();
                let receiver = value.clone();
                let done_tx = done_tx.clone();

                rt.spawn(async move {
                    let result = receiver.await;
                    done_tx.send(result).unwrap();
                }).unwrap();

                value.set(black_box(1));
            }

            drop(done_tx);
            for _ in 0..TASKS_PER_ITER {
                let _ = done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            }
        });

        let _ = rt.close();
    }

    #[bench]
    fn bench_single_thread_async_value_internal_set_await_batch(b: &mut Bencher) {
        let runner = SingleTaskRunner::<()>::default();
        let rt = runner.startup().unwrap();

        b.iter(|| {
            let (done_tx, done_rx) = mpsc::channel();

            for _ in 0..SINGLE_TASKS_PER_ITER {
                let value = AsyncValue::<usize>::new();
                let receiver = value.clone();
                let done_tx = done_tx.clone();

                rt.spawn(async move {
                    let result = receiver.await;
                    done_tx.send(result).unwrap();
                }).unwrap();
                rt.spawn(async move {
                    value.set(black_box(1));
                }).unwrap();
            }

            drop(done_tx);
            drive_single_until_count(&runner, &done_rx, SINGLE_TASKS_PER_ITER);
        });
    }

    #[bench]
    fn bench_multi_thread_async_value_internal_set_await_batch(b: &mut Bencher) {
        let rt = new_runtime();

        b.iter(|| {
            let (done_tx, done_rx) = mpsc::channel();

            for _ in 0..TASKS_PER_ITER {
                let value = AsyncValue::<usize>::new();
                let receiver = value.clone();
                let done_tx = done_tx.clone();

                rt.spawn(async move {
                    let result = receiver.await;
                    done_tx.send(result).unwrap();
                }).unwrap();
                rt.spawn(async move {
                    value.set(black_box(1));
                }).unwrap();
            }

            drop(done_tx);
            for _ in 0..TASKS_PER_ITER {
                let _ = done_rx.recv_timeout(BENCH_TIMEOUT).unwrap();
            }
        });

        let _ = rt.close();
    }

    #[bench]
    fn bench_cross_runtime_single_wait_multi_set_batch(b: &mut Bencher) {
        let single_runner = SingleTaskRunner::<()>::default();
        let single_rt = single_runner.startup().unwrap();
        let multi_rt = new_runtime();

        b.iter(|| {
            let (armed_tx, armed_rx) = mpsc::channel();
            let (done_tx, done_rx) = mpsc::channel();
            let mut setters = Vec::with_capacity(CROSS_TASKS_PER_ITER);

            for _ in 0..CROSS_TASKS_PER_ITER {
                let value = AsyncValue::<usize>::new();
                let mut receiver = value.clone();
                let armed_tx = armed_tx.clone();
                let done_tx = done_tx.clone();
                setters.push(value);

                single_rt.spawn(async move {
                    let mut armed_tx = Some(armed_tx);
                    let result = poll_fn(move |cx| {
                        match Pin::new(&mut receiver).poll(cx) {
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
                    done_tx.send(result).unwrap();
                }).unwrap();
            }

            drop(armed_tx);
            drop(done_tx);
            drive_single_until_count(&single_runner, &armed_rx, CROSS_TASKS_PER_ITER);

            for value in setters {
                multi_rt.spawn(async move {
                    value.set(black_box(1));
                }).unwrap();
            }

            drive_single_until_count(&single_runner, &done_rx, CROSS_TASKS_PER_ITER);
        });

        let _ = multi_rt.close();
    }
}

#[cfg(feature = "serial")]
mod serial_benches {
    use futures::task::{waker_ref, ArcWake};
    use pi_async_rt::prelude::{AsyncRuntime, AsyncValue, SingleTaskRunner};
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            atomic::{AtomicUsize, Ordering},
            mpsc::{self, Receiver, TryRecvError},
            Arc,
        },
        task::Poll,
        thread,
        time::{Duration, Instant},
    };
    use test::{black_box, Bencher};

    const SERIAL_TASKS_PER_ITER: usize = 128;
    const BENCH_TIMEOUT: Duration = Duration::from_secs(10);

    struct CountWake {
        count: AtomicUsize,
    }

    impl CountWake {
        fn new() -> Arc<Self> {
            Arc::new(CountWake {
                count: AtomicUsize::new(0),
            })
        }
    }

    impl ArcWake for CountWake {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.count.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn poll_with_wake<F: Future + Unpin>(future: &mut F, wake: &Arc<CountWake>) -> Poll<F::Output> {
        let waker = waker_ref(wake);
        let mut cx = std::task::Context::from_waker(&*waker);
        Pin::new(future).poll(&mut cx)
    }

    fn drive_single_until_count<T>(runner: &SingleTaskRunner<()>,
                                   receiver: &Receiver<T>,
                                   expected: usize) {
        let started = Instant::now();
        let mut count = 0;
        while count < expected {
            match receiver.try_recv() {
                Ok(_) => {
                    count += 1;
                },
                Err(TryRecvError::Empty) => {},
                Err(TryRecvError::Disconnected) => {
                    panic!("serial single-thread bench receiver disconnected at {} of {}", count, expected);
                },
            }

            if count < expected {
                if started.elapsed() >= BENCH_TIMEOUT {
                    panic!("serial single-thread AsyncValue bench timed out at {} of {}", count, expected);
                }
                while runner.run().unwrap() > 0 {}
                thread::yield_now();
            }
        }
    }

    #[bench]
    fn bench_serial_async_value_pending_poll(b: &mut Bencher) {
        let mut value = AsyncValue::<usize>::new();
        let wake = CountWake::new();

        b.iter(|| {
            match poll_with_wake(&mut value, &wake) {
                Poll::Pending => {
                    black_box(());
                },
                Poll::Ready(_) => {
                    panic!("never-set serial AsyncValue unexpectedly became ready");
                },
            }
        });
    }

    #[bench]
    fn bench_serial_async_value_set_then_ready(b: &mut Bencher) {
        let wake = CountWake::new();

        b.iter(|| {
            let value = AsyncValue::<usize>::new();
            let mut receiver = value.clone();

            match poll_with_wake(&mut receiver, &wake) {
                Poll::Pending => {},
                Poll::Ready(_) => {
                    panic!("unset serial AsyncValue unexpectedly became ready");
                },
            }
            value.set(black_box(1));
            match poll_with_wake(&mut receiver, &wake) {
                Poll::Ready(value) => {
                    black_box(value);
                },
                Poll::Pending => {
                    panic!("set serial AsyncValue stayed pending");
                },
            }
        });
    }

    #[bench]
    fn bench_serial_single_thread_async_value_internal_set_await_batch(b: &mut Bencher) {
        let runner = SingleTaskRunner::<()>::default();
        let rt = runner.startup().unwrap();

        b.iter(|| {
            let (done_tx, done_rx) = mpsc::channel();

            for _ in 0..SERIAL_TASKS_PER_ITER {
                let value = AsyncValue::<usize>::new();
                let receiver = value.clone();
                let done_tx = done_tx.clone();

                rt.spawn(async move {
                    let result = receiver.await;
                    done_tx.send(result).unwrap();
                }).unwrap();
                rt.spawn(async move {
                    value.set(black_box(1));
                }).unwrap();
            }

            drop(done_tx);
            drive_single_until_count(&runner, &done_rx, SERIAL_TASKS_PER_ITER);
        });
    }
}
