#![feature(test)]

extern crate test;

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;

use crossbeam_channel::bounded;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntimeBuilder, StealableTaskPool},
    startup_global_time_loop, AsyncRuntime,
};
use test::Bencher;

#[bench]
fn bench_multi_thread_timeout_churn_spawn(b: &mut Bencher) {
    let _time_loop = startup_global_time_loop(1);
    let pool = StealableTaskPool::with(2, 4096, [1, 1], 10);
    let rt = MultiTaskRuntimeBuilder::new(pool)
        .thread_prefix("Timeout-Waiter-Bench")
        .thread_stack_size(2 * 1024 * 1024)
        .init_worker_size(2)
        .set_worker_limit(2, 2)
        .set_timeout(2)
        .set_timer_interval(1)
        .build();

    thread::sleep(Duration::from_millis(50));

    b.iter(|| {
        let task_count = 128;
        let (sender, receiver) = bounded(1);
        let remaining = Arc::new(AtomicUsize::new(task_count));

        for _ in 0..task_count {
            let rt_for_task = rt.clone();
            let sender_for_task = sender.clone();
            let remaining_for_task = remaining.clone();
            rt.spawn(async move {
                rt_for_task.timeout(1).await;
                if remaining_for_task.fetch_sub(1, Ordering::SeqCst) == 1 {
                    let _ = sender_for_task.send(());
                }
            })
            .unwrap();
        }

        drop(sender);
        receiver.recv_timeout(Duration::from_secs(5)).unwrap();
    });
}
