#![feature(test)]

extern crate test;

use futures::future::poll_fn;
use pi_async_rt::rt::{
    multi_thread::{MultiTaskRuntime, MultiTaskRuntimeBuilder, StealableTaskPool},
    AsyncRuntime, AsyncValue,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc, Arc, Barrier, Condvar, Mutex,
    },
    task::Poll,
    thread,
    time::{Duration, Instant},
};
use test::{black_box, Bencher};

const WORKER_SIZE: usize = 8;
const SLEEP_TIMEOUT_MS: u64 = 80;
const IDLE_WAKE_DELAY_MS: u64 = 5;
const LATENCY_SAMPLES: usize = 128;
const BENCH_LATENCY_SAMPLES: usize = 1;
const THROUGHPUT_TASKS: usize = 1_000_000;
const BENCH_THROUGHPUT_TASKS: usize = 1_000;
const IN_FLIGHT_TASKS: usize = 65536;
const LATENCY_SAMPLE_COUNT: usize = 2048;
const SPAWN_THREADS: usize = 8;

struct TaskWorkloadState {
    completed: AtomicUsize,
    in_flight: AtomicUsize,
    done: Mutex<bool>,
    done_cv: Condvar,
}

impl TaskWorkloadState {
    fn new() -> Self {
        TaskWorkloadState {
            completed: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            done: Mutex::new(false),
            done_cv: Condvar::new(),
        }
    }
}

fn new_runtime() -> MultiTaskRuntime<()> {
    let pool = StealableTaskPool::with(WORKER_SIZE, 65535, [1, 1], 10);
    MultiTaskRuntimeBuilder::new(pool)
        .thread_prefix("Worker-Wakeup-Bench")
        .thread_stack_size(2 * 1024 * 1024)
        .init_worker_size(WORKER_SIZE)
        .set_worker_limit(WORKER_SIZE, WORKER_SIZE)
        .set_timeout(SLEEP_TIMEOUT_MS)
        .build()
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn print_latency_stats(name: &str, latencies: &mut [Duration]) {
    latencies.sort_unstable();
    let p50 = latencies[latencies.len() / 2];
    let p90 = latencies[latencies.len() * 90 / 100];
    let p99 = latencies[latencies.len() * 99 / 100];
    let max = *latencies.last().unwrap();

    println!(
        "{} latency: samples={}, p50={:?}, p90={:?}, p99={:?}, max={:?}",
        name,
        latencies.len(),
        p50,
        p90,
        p99,
        max
    );
}

fn run_async_value_wake_latency_batch(rt: &MultiTaskRuntime<()>, samples: usize) -> Vec<Duration> {
    let mut latencies = Vec::with_capacity(samples);

    for _ in 0..samples {
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
        }).unwrap();

        armed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        thread::sleep(Duration::from_millis(IDLE_WAKE_DELAY_MS));
        value.set(Instant::now());
        latencies.push(done_rx.recv_timeout(Duration::from_secs(2)).unwrap());
    }

    latencies
}

fn run_external_spawn_latency_batch(rt: &MultiTaskRuntime<()>, samples: usize) -> Vec<Duration> {
    let mut latencies = Vec::with_capacity(samples);

    for _ in 0..samples {
        let (done_tx, done_rx) = mpsc::channel();

        thread::sleep(Duration::from_millis(IDLE_WAKE_DELAY_MS));
        let started = Instant::now();
        rt.spawn(async move {
            done_tx.send(started.elapsed()).unwrap();
        }).unwrap();

        latencies.push(done_rx.recv_timeout(Duration::from_secs(2)).unwrap());
    }

    latencies
}

fn run_concurrent_task_workload(
    rt: &MultiTaskRuntime<()>,
    total: usize,
    spawn_threads: usize,
    max_in_flight: usize,
    sample_count: usize,
) -> (Duration, Vec<Duration>) {
    let spawn_threads = spawn_threads.max(1);
    let max_in_flight = max_in_flight.max(spawn_threads);
    let state = Arc::new(TaskWorkloadState::new());
    let start_barrier = Arc::new(Barrier::new(spawn_threads + 1));
    let (sample_tx, sample_rx) = mpsc::channel();
    let sample_stride = (total / sample_count.max(1)).max(1);
    let mut handles = Vec::with_capacity(spawn_threads);

    for thread_index in 0..spawn_threads {
        let rt = rt.clone();
        let state = state.clone();
        let start_barrier = start_barrier.clone();
        let sample_tx = sample_tx.clone();
        let begin = thread_index * total / spawn_threads;
        let end = (thread_index + 1) * total / spawn_threads;

        handles.push(thread::spawn(move || {
            start_barrier.wait();

            for task_index in begin..end {
                loop {
                    let current = state.in_flight.fetch_add(1, Ordering::AcqRel);
                    if current < max_in_flight {
                        break;
                    }
                    state.in_flight.fetch_sub(1, Ordering::AcqRel);
                    thread::yield_now();
                }

                let state = state.clone();
                let sample = if task_index % sample_stride == 0 {
                    Some((sample_tx.clone(), Instant::now()))
                } else {
                    None
                };

                rt.spawn(async move {
                    if let Some((sample_tx, sample_started)) = sample {
                        let _ = sample_tx.send(sample_started.elapsed());
                    }
                    state.in_flight.fetch_sub(1, Ordering::AcqRel);
                    if state.completed.fetch_add(1, Ordering::AcqRel) + 1 == total {
                        let mut done = state.done.lock().unwrap();
                        *done = true;
                        state.done_cv.notify_one();
                    }
                }).unwrap();
            }
        }));
    }

    drop(sample_tx);
    let start = Instant::now();
    start_barrier.wait();

    let mut done = state.done.lock().unwrap();
    while !*done {
        let (next_done, wait_result) = state
            .done_cv
            .wait_timeout(done, Duration::from_secs(120))
            .unwrap();
        done = next_done;
        if wait_result.timed_out() {
            panic!(
                "Concurrent empty task workload timed out, completed: {}, total: {}",
                state.completed.load(Ordering::Acquire),
                total
            );
        }
    }
    drop(done);

    for handle in handles {
        handle.join().unwrap();
    }

    let mut sample_latencies = Vec::with_capacity(sample_count.min(total));
    while let Ok(latency) = sample_rx.try_recv() {
        sample_latencies.push(latency);
    }

    (start.elapsed(), sample_latencies)
}

/// 在真实 multi-thread worker 内并发 `spawn_local` 空任务，测量 internal 调度热路径。
///
/// 被测生产入口是 `MultiTaskRuntime::spawn_local`、`StealableTaskPool::push_local/try_pop` 及
/// internal steal；测试侧只设置开始门和完成计数，不提供队列、owner 校验或唤醒实现。
/// `producer_tasks` 个根 future 通过 runtime 的 `yield_now` 协作等待，不阻塞 worker 线程。
///
/// 输入：
/// - `total`：需要 exact-once 完成的空子任务数，调用方必须大于 0；
/// - `producer_tasks`：runtime 内 producer future 数，至少按 1 处理；
/// - `max_in_flight`：未完成子任务上限，至少按 producer 数处理；
/// - `sample_count`：近似延迟样本上限。
///
/// 输出为总耗时和从 `spawn_local` 到子任务执行的采样延迟。时间复杂度 O(N)，测试状态空间
/// O(min(N, max_in_flight) + sample_count)。该 helper 非纯函数，会向真实 runtime 投递任务；
/// 不持有生产锁、不执行 I/O、不跨 await 持锁，所有同步等待只发生在基准主线程并有 120 秒
/// 截止。它不承诺幂等，每次调用都是独立负载。
fn run_internal_task_workload(
    rt: &MultiTaskRuntime<()>,
    total: usize,
    producer_tasks: usize,
    max_in_flight: usize,
    sample_count: usize,
) -> (Duration, Vec<Duration>) {
    let producer_tasks = producer_tasks.max(1);
    let max_in_flight = max_in_flight.max(producer_tasks);
    let state = Arc::new(TaskWorkloadState::new());
    let armed = Arc::new(AtomicUsize::new(0));
    let start_gate = Arc::new(AtomicBool::new(false));
    let (sample_tx, sample_rx) = mpsc::channel();
    let sample_stride = (total / sample_count.max(1)).max(1);

    for producer_index in 0..producer_tasks {
        let rt_for_producer = rt.clone();
        let state = state.clone();
        let armed = armed.clone();
        let start_gate = start_gate.clone();
        let sample_tx = sample_tx.clone();
        let begin = producer_index * total / producer_tasks;
        let end = (producer_index + 1) * total / producer_tasks;

        rt.spawn(async move {
            armed.fetch_add(1, Ordering::Release);
            while !start_gate.load(Ordering::Acquire) {
                rt_for_producer.yield_now().await;
            }

            for task_index in begin..end {
                loop {
                    let current = state.in_flight.fetch_add(1, Ordering::AcqRel);
                    if current < max_in_flight {
                        break;
                    }
                    state.in_flight.fetch_sub(1, Ordering::AcqRel);
                    rt_for_producer.yield_now().await;
                }

                let state = state.clone();
                let sample = if task_index % sample_stride == 0 {
                    Some((sample_tx.clone(), Instant::now()))
                } else {
                    None
                };
                rt_for_producer.spawn_local(async move {
                    if let Some((sample_tx, sample_started)) = sample {
                        let _ = sample_tx.send(sample_started.elapsed());
                    }
                    state.in_flight.fetch_sub(1, Ordering::AcqRel);
                    if state.completed.fetch_add(1, Ordering::AcqRel) + 1 == total {
                        let mut done = state.done.lock().unwrap();
                        *done = true;
                        state.done_cv.notify_one();
                    }
                }).unwrap();
            }
        }).unwrap();
    }

    drop(sample_tx);
    let armed_deadline = Instant::now() + Duration::from_secs(120);
    while armed.load(Ordering::Acquire) != producer_tasks {
        if Instant::now() >= armed_deadline {
            panic!(
                "Internal empty task producers did not arm, armed: {}, producers: {}",
                armed.load(Ordering::Acquire),
                producer_tasks
            );
        }
        thread::yield_now();
    }

    let start = Instant::now();
    start_gate.store(true, Ordering::Release);

    let mut done = state.done.lock().unwrap();
    while !*done {
        let (next_done, wait_result) = state
            .done_cv
            .wait_timeout(done, Duration::from_secs(120))
            .unwrap();
        done = next_done;
        if wait_result.timed_out() {
            panic!(
                "Internal empty task workload timed out, completed: {}, total: {}",
                state.completed.load(Ordering::Acquire),
                total
            );
        }
    }
    drop(done);

    let mut sample_latencies = Vec::with_capacity(sample_count.min(total));
    while let Ok(latency) = sample_rx.try_recv() {
        sample_latencies.push(latency);
    }

    (start.elapsed(), sample_latencies)
}

fn print_throughput_stats(
    name: &str,
    total: usize,
    spawn_threads: usize,
    elapsed: Duration,
    latencies: &mut [Duration],
) {
    let tasks_per_sec = total as f64 / elapsed.as_secs_f64();
    println!(
        "{} throughput: workers={}, spawn_threads={}, tasks={}, elapsed={:?}, tasks_per_sec={:.0}",
        name,
        WORKER_SIZE,
        spawn_threads,
        total,
        elapsed,
        tasks_per_sec
    );

    if !latencies.is_empty() {
        print_latency_stats(name, latencies);
    }
}

#[bench]
fn bench_multi_thread_async_value_external_wake_latency_8_workers(b: &mut Bencher) {
    let rt = new_runtime();

    b.iter(|| {
        let latencies = run_async_value_wake_latency_batch(&rt, BENCH_LATENCY_SAMPLES);
        black_box(latencies.len());
    });

    let mut latencies = run_async_value_wake_latency_batch(&rt, LATENCY_SAMPLES);
    print_latency_stats("pi_async_rt AsyncValue external wake", &mut latencies);
}

#[bench]
fn bench_multi_thread_external_spawn_latency_8_workers(b: &mut Bencher) {
    let rt = new_runtime();

    b.iter(|| {
        let latencies = run_external_spawn_latency_batch(&rt, BENCH_LATENCY_SAMPLES);
        black_box(latencies.len());
    });

    let mut latencies = run_external_spawn_latency_batch(&rt, LATENCY_SAMPLES);
    print_latency_stats("pi_async_rt external spawn", &mut latencies);
}

#[bench]
fn bench_multi_thread_empty_task_throughput_8_workers(b: &mut Bencher) {
    let rt = new_runtime();

    b.iter(|| {
        let (elapsed, latencies) = run_concurrent_task_workload(
            &rt,
            BENCH_THROUGHPUT_TASKS,
            SPAWN_THREADS,
            IN_FLIGHT_TASKS,
            LATENCY_SAMPLE_COUNT.min(BENCH_THROUGHPUT_TASKS),
        );
        black_box((elapsed, latencies.len()));
    });

    let total = env_usize("PI_ASYNC_RT_WAKE_BENCH_TASKS", THROUGHPUT_TASKS);
    let in_flight = env_usize("PI_ASYNC_RT_WAKE_BENCH_IN_FLIGHT", IN_FLIGHT_TASKS);
    let spawn_threads = env_usize("PI_ASYNC_RT_WAKE_BENCH_SPAWN_THREADS", SPAWN_THREADS);
    let (elapsed, mut latencies) = run_concurrent_task_workload(
        &rt,
        total,
        spawn_threads,
        in_flight,
        LATENCY_SAMPLE_COUNT,
    );
    print_throughput_stats("pi_async_rt concurrent empty task", total, spawn_threads, elapsed, &mut latencies);
}

/// 8 worker、8 个 runtime 内 producer 的 internal 空任务吞吐和排队延迟基准。
///
/// 该标准 bench 专门量化 multi-thread pool owner 校验在 `spawn_local`/`try_pop` 热路径上的
/// 真实成本。默认正式样本为 1,000,000 个任务，可用现有环境变量先降为 100,000 做资源探针。
/// 输出包括 tasks/s、elapsed、p50/p90/p99/max；资源指标由外层 `/usr/bin/time -v` 记录。
#[bench]
fn bench_multi_thread_internal_empty_task_throughput_8_workers(b: &mut Bencher) {
    let rt = new_runtime();

    b.iter(|| {
        let (elapsed, latencies) = run_internal_task_workload(
            &rt,
            BENCH_THROUGHPUT_TASKS,
            SPAWN_THREADS,
            IN_FLIGHT_TASKS,
            LATENCY_SAMPLE_COUNT.min(BENCH_THROUGHPUT_TASKS),
        );
        black_box((elapsed, latencies.len()));
    });

    let total = env_usize("PI_ASYNC_RT_WAKE_BENCH_TASKS", THROUGHPUT_TASKS);
    let in_flight = env_usize("PI_ASYNC_RT_WAKE_BENCH_IN_FLIGHT", IN_FLIGHT_TASKS);
    let producer_tasks = env_usize("PI_ASYNC_RT_WAKE_BENCH_SPAWN_THREADS", SPAWN_THREADS);
    let (elapsed, mut latencies) = run_internal_task_workload(
        &rt,
        total,
        producer_tasks,
        in_flight,
        LATENCY_SAMPLE_COUNT,
    );
    print_throughput_stats(
        "pi_async_rt internal empty task",
        total,
        producer_tasks,
        elapsed,
        &mut latencies,
    );
}
