use futures::task::{waker_ref, ArcWake};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::Poll,
};

struct CountWake {
    count: AtomicUsize,
}

impl CountWake {
    fn new() -> Arc<Self> {
        Arc::new(CountWake {
            count: AtomicUsize::new(0),
        })
    }

    fn count(&self) -> usize {
        self.count.load(Ordering::Acquire)
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

#[cfg(not(feature = "serial"))]
fn poll_pinned_with_wake<F: Future>(future: Pin<&mut F>, wake: &Arc<CountWake>) -> Poll<F::Output> {
    let waker = waker_ref(wake);
    let mut cx = std::task::Context::from_waker(&*waker);
    future.poll(&mut cx)
}

#[cfg(not(feature = "serial"))]
mod default_runtime {
    use super::{poll_pinned_with_wake, poll_with_wake, CountWake};
    use futures::future::{join_all, poll_fn};
    use pi_async_rt::rt::{
        multi_thread::{MultiTaskRuntimeBuilder, StealableTaskPool},
        single_thread::SingleTaskRunner,
        worker_thread::WorkerTaskRunner,
        AsyncRuntime, AsyncValue,
    };
    use std::{
        future::Future,
        pin::Pin,
        sync::mpsc::{self, Receiver, TryRecvError},
        task::Poll,
        thread,
        time::{Duration, Instant},
    };

    const WORKER_SIZE: usize = 2;
    const TEST_TIMEOUT: Duration = Duration::from_secs(2);

    fn new_runtime() -> pi_async_rt::rt::multi_thread::MultiTaskRuntime<()> {
        let pool = StealableTaskPool::with(WORKER_SIZE, 4096, [1, 1], 10);
        MultiTaskRuntimeBuilder::new(pool)
            .thread_prefix("AsyncValue-Test")
            .thread_stack_size(2 * 1024 * 1024)
            .init_worker_size(WORKER_SIZE)
            .set_worker_limit(WORKER_SIZE, WORKER_SIZE)
            .set_timeout(80)
            .build()
    }

    fn drive_single_until<T>(runner: &SingleTaskRunner<()>, receiver: &Receiver<T>) -> T {
        let started = Instant::now();
        loop {
            match receiver.try_recv() {
                Ok(value) => {
                    return value;
                },
                Err(TryRecvError::Empty) => {},
                Err(TryRecvError::Disconnected) => {
                    panic!("single-thread test receiver disconnected");
                },
            }

            assert!(
                started.elapsed() < TEST_TIMEOUT,
                "single-thread AsyncValue test timed out"
            );
            while runner.run().unwrap() > 0 {}
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn poll_pending_twice_does_not_panic_and_stays_pending() {
        let mut value = AsyncValue::<usize>::new();
        let wake = CountWake::new();

        assert!(!value.is_complete());
        assert!(matches!(poll_with_wake(&mut value, &wake), Poll::Pending));
        assert!(matches!(poll_with_wake(&mut value, &wake), Poll::Pending));
        assert_eq!(wake.count(), 0);
        assert!(!value.is_complete());
    }

    #[test]
    fn poll_pending_replaces_waker_and_set_wakes_latest() {
        let value = AsyncValue::<usize>::new();
        let mut receiver = value.clone();
        let first_wake = CountWake::new();
        let second_wake = CountWake::new();

        assert!(matches!(poll_with_wake(&mut receiver, &first_wake), Poll::Pending));
        assert!(matches!(poll_with_wake(&mut receiver, &second_wake), Poll::Pending));
        value.set(7);

        assert_eq!(first_wake.count(), 0);
        assert_eq!(second_wake.count(), 1);
        assert!(receiver.is_complete());
        assert!(matches!(poll_with_wake(&mut receiver, &second_wake), Poll::Ready(7)));
    }

    #[test]
    fn poll_pending_then_set_wakes_and_ready_once() {
        let value = AsyncValue::<usize>::new();
        let mut receiver = value.clone();
        let wake = CountWake::new();

        assert!(matches!(poll_with_wake(&mut receiver, &wake), Poll::Pending));
        value.set(11);

        assert_eq!(wake.count(), 1);
        assert!(receiver.is_complete());
        assert!(matches!(poll_with_wake(&mut receiver, &wake), Poll::Ready(11)));
        assert!(receiver.is_complete());
    }

    #[test]
    fn join_all_multiple_async_values_does_not_panic() {
        let values: Vec<_> = (0..8).map(|_| AsyncValue::<usize>::new()).collect();
        let senders = values.clone();
        let wake = CountWake::new();
        let mut joined = Box::pin(join_all(values));

        assert!(matches!(poll_pinned_with_wake(joined.as_mut(), &wake), Poll::Pending));
        assert!(matches!(poll_pinned_with_wake(joined.as_mut(), &wake), Poll::Pending));
        for (index, sender) in senders.into_iter().enumerate() {
            sender.set(index);
        }

        match poll_pinned_with_wake(joined.as_mut(), &wake) {
            Poll::Ready(values) => {
                assert_eq!(values, vec![0, 1, 2, 3, 4, 5, 6, 7]);
            },
            Poll::Pending => {
                panic!("join_all stayed pending after all AsyncValue senders were set");
            },
        }
    }

    #[test]
    fn set_twice_second_does_not_overwrite() {
        let value = AsyncValue::<usize>::new();
        let mut receiver = value.clone();
        let wake = CountWake::new();

        value.clone().set(23);
        value.set(42);

        assert!(matches!(poll_with_wake(&mut receiver, &wake), Poll::Ready(23)));
    }

    #[test]
    fn drop_pending_receiver_then_set_does_not_panic() {
        let value = AsyncValue::<usize>::new();
        let sender = value.clone();
        let wake = CountWake::new();

        {
            let mut receiver = value;
            assert!(matches!(poll_with_wake(&mut receiver, &wake), Poll::Pending));
        }

        sender.set(9);
    }

    #[test]
    fn never_set_stays_pending_without_panic() {
        let mut value = AsyncValue::<usize>::new();
        let wake = CountWake::new();

        for _ in 0..16 {
            assert!(matches!(poll_with_wake(&mut value, &wake), Poll::Pending));
        }
    }

    #[test]
    fn multi_thread_cross_thread_set_wakes_waiter() {
        let rt = new_runtime();
        let value = AsyncValue::<usize>::new();
        let receiver = value.clone();
        let (armed_tx, armed_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();

        rt.spawn(async move {
            let mut receiver = receiver;
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

        armed_rx.recv_timeout(TEST_TIMEOUT).unwrap();
        let setter = thread::spawn(move || {
            value.set(77);
        });
        setter.join().unwrap();

        assert_eq!(done_rx.recv_timeout(TEST_TIMEOUT).unwrap(), 77);
        let _ = rt.close();
    }

    #[test]
    fn single_thread_runtime_internal_set_wakes_waiter() {
        let runner = SingleTaskRunner::<()>::default();
        let rt = runner.startup().unwrap();
        let value = AsyncValue::<usize>::new();
        let receiver = value.clone();
        let (done_tx, done_rx) = mpsc::channel();

        rt.spawn(async move {
            done_tx.send(receiver.await).unwrap();
        }).unwrap();
        rt.spawn(async move {
            value.set(101);
        }).unwrap();

        assert_eq!(drive_single_until(&runner, &done_rx), 101);
    }

    #[test]
    fn multi_thread_runtime_internal_set_wakes_waiter() {
        let rt = new_runtime();
        let value = AsyncValue::<usize>::new();
        let receiver = value.clone();
        let (done_tx, done_rx) = mpsc::channel();

        rt.spawn(async move {
            done_tx.send(receiver.await).unwrap();
        }).unwrap();
        rt.spawn(async move {
            value.set(102);
        }).unwrap();

        assert_eq!(done_rx.recv_timeout(TEST_TIMEOUT).unwrap(), 102);
        let _ = rt.close();
    }

    #[test]
    fn worker_thread_runtime_internal_set_wakes_waiter() {
        let runner = WorkerTaskRunner::<()>::default();
        let runner_for_loop = runner.clone();
        let rt = runner.startup(
            "AsyncValue-Worker-Test",
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
        let value = AsyncValue::<usize>::new();
        let receiver = value.clone();
        let (done_tx, done_rx) = mpsc::channel();

        rt.spawn(async move {
            done_tx.send(receiver.await).unwrap();
        }).unwrap();
        rt.spawn(async move {
            value.set(103);
        }).unwrap();

        assert_eq!(done_rx.recv_timeout(TEST_TIMEOUT).unwrap(), 103);
        let _ = rt.close();
    }

    #[test]
    fn single_thread_wait_multi_thread_set_cross_runtime() {
        let single_runner = SingleTaskRunner::<()>::default();
        let single_rt = single_runner.startup().unwrap();
        let multi_rt = new_runtime();
        let value = AsyncValue::<usize>::new();
        let receiver = value.clone();
        let (armed_tx, armed_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();

        single_rt.spawn(async move {
            let mut receiver = receiver;
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

        drive_single_until(&single_runner, &armed_rx);
        multi_rt.spawn(async move {
            value.set(201);
        }).unwrap();

        assert_eq!(drive_single_until(&single_runner, &done_rx), 201);
        let _ = multi_rt.close();
    }

    #[test]
    fn multi_thread_wait_single_thread_set_cross_runtime() {
        let multi_rt = new_runtime();
        let single_runner = SingleTaskRunner::<()>::default();
        let single_rt = single_runner.startup().unwrap();
        let value = AsyncValue::<usize>::new();
        let receiver = value.clone();
        let (armed_tx, armed_rx) = mpsc::channel();
        let (setter_done_tx, setter_done_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();

        multi_rt.spawn(async move {
            let mut receiver = receiver;
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

        armed_rx.recv_timeout(TEST_TIMEOUT).unwrap();
        single_rt.spawn(async move {
            value.set(202);
            setter_done_tx.send(()).unwrap();
        }).unwrap();

        drive_single_until(&single_runner, &setter_done_rx);
        assert_eq!(done_rx.recv_timeout(TEST_TIMEOUT).unwrap(), 202);
        let _ = multi_rt.close();
    }
}

#[cfg(feature = "serial")]
mod serial_runtime {
    use super::{poll_with_wake, CountWake};
    use pi_async_rt::prelude::{AsyncRuntime, AsyncValue, SingleTaskRunner};
    use std::{
        rc::Rc,
        sync::mpsc::{self, Receiver, TryRecvError},
        task::Poll,
        thread,
        time::{Duration, Instant},
    };

    const TEST_TIMEOUT: Duration = Duration::from_secs(2);

    fn drive_serial_single_until<T>(runner: &SingleTaskRunner<()>, receiver: &Receiver<T>) -> T {
        let started = Instant::now();
        loop {
            match receiver.try_recv() {
                Ok(value) => {
                    return value;
                },
                Err(TryRecvError::Empty) => {},
                Err(TryRecvError::Disconnected) => {
                    panic!("serial single-thread receiver disconnected");
                },
            }

            assert!(
                started.elapsed() < TEST_TIMEOUT,
                "serial single-thread AsyncValue test timed out"
            );
            while runner.run().unwrap() > 0 {}
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn serial_poll_pending_twice_does_not_panic() {
        let mut value = AsyncValue::<usize>::new();
        let wake = CountWake::new();

        assert!(matches!(poll_with_wake(&mut value, &wake), Poll::Pending));
        assert!(matches!(poll_with_wake(&mut value, &wake), Poll::Pending));
        assert_eq!(wake.count(), 0);
    }

    #[test]
    fn serial_local_non_send_value_ready() {
        let value = AsyncValue::<Rc<usize>>::new();
        let mut receiver = value.clone();
        let wake = CountWake::new();

        assert!(matches!(poll_with_wake(&mut receiver, &wake), Poll::Pending));
        value.set(Rc::new(31));

        match poll_with_wake(&mut receiver, &wake) {
            Poll::Ready(value) => {
                assert_eq!(*value, 31);
            },
            Poll::Pending => {
                panic!("serial AsyncValue stayed pending after set");
            },
        }
    }

    #[test]
    fn serial_single_thread_runtime_internal_set_wakes_waiter() {
        let runner = SingleTaskRunner::<()>::default();
        let rt = runner.startup().unwrap();
        let value = AsyncValue::<Rc<usize>>::new();
        let receiver = value.clone();
        let (done_tx, done_rx) = mpsc::channel();

        rt.spawn(async move {
            let result = receiver.await;
            done_tx.send(*result).unwrap();
        }).unwrap();
        rt.spawn(async move {
            value.set(Rc::new(301));
        }).unwrap();

        assert_eq!(drive_serial_single_until(&runner, &done_rx), 301);
    }
}
