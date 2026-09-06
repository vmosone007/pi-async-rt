//! 单线程池所有者校验的有界、独立基准，不创建多线程运行时。
//!
//! 对应 `DESIGN-SINGLE-TASK-POOL-OWNER-2026-09-06`。每场景五轮，单轮最多4096次，
//! 同时在途一个任务；结果以逐行 JSON 输出，统计吞吐、延迟、进程 CPU 和 RSS。
//! 真实 Runner 推进真实任务，不以测试实现替代调度。不作为硬实时承诺或全局压力测试。
//! 跨线程生产者先实际驱动自己的空池，保证旧版本也只走目标公共队列，避免危险基线。

use std::{fs, sync::{atomic::{AtomicUsize, Ordering}, mpsc, Arc}, thread, time::{Duration, Instant}};
use futures::channel::oneshot;
use pi_async_rt::prelude::{AsyncRuntime, SingleTaskRunner};

const SAMPLES: usize = 5;
const OPS: usize = 2048;

/// Unix 进程 CPU 时间；失败或非 Unix 返回 0，此时不能据此作有效 CPU 性能结论。
#[cfg(unix)]
fn cpu_ns() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // 安全：指针指向有效且足够大的输出存储，只在系统调用成功后读取。
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return 0;
    }
    let usage = unsafe { usage.assume_init() };
    let seconds = usage.ru_utime.tv_sec + usage.ru_stime.tv_sec;
    let micros = usage.ru_utime.tv_usec + usage.ru_stime.tv_usec;
    seconds as u64 * 1_000_000_000 + micros as u64 * 1000
}

#[cfg(not(unix))]
fn cpu_ns() -> u64 { 0 }

/// 非侵入读取 Linux RSS；不可用时输出 JSON null，而不是伪造零内存。
fn rss_kb() -> String {
    fs::read_to_string("/proc/self/status").ok()
        .and_then(|text| text.lines().find(|line| line.starts_with("VmRSS:"))
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|value| value.parse::<usize>().ok()))
        .map(|value| value.to_string()).unwrap_or_else(|| "null".to_string())
}

/// 所有样本已完成后排序，不把统计开销计入操作时间。
fn report(mode: &str, sample: usize, mut times: Vec<u64>, elapsed: u128, cpu: u64) {
    assert!(!times.is_empty());
    times.sort_unstable();
    let ops = times.len();
    let percentile = |p: usize| times[(ops * p).div_ceil(100) - 1];
    println!("{{\"mode\":\"{}\",\"sample\":{},\"ops\":{},\"elapsed_ns\":{},\"ns_per_op\":{:.3},\"ops_per_second\":{:.3},\"p50_ns\":{},\"p90_ns\":{},\"p99_ns\":{},\"max_ns\":{},\"cpu_ns\":{},\"rss_kb\":{},\"max_inflight\":1}}",
        mode, sample, ops, elapsed, elapsed as f64 / ops as f64,
        ops as f64 * 1_000_000_000.0 / elapsed as f64,
        percentile(50), percentile(90), percentile(99), times[ops - 1], cpu, rss_kb());
}

fn main() {
    let runner = SingleTaskRunner::<()>::default();
    let runtime = runner.startup().unwrap();
    runner.run_once().unwrap();
    let (send, recv) = mpsc::sync_channel::<oneshot::Sender<usize>>(1);
    let (ready_send, ready_recv) = mpsc::sync_channel(1);
    let producer = thread::spawn(move || {
        let foreign = SingleTaskRunner::<()>::default();
        let _foreign_runtime = foreign.startup().unwrap();
        foreign.run_once().unwrap();
        ready_send.send(()).unwrap();
        while let Ok(sender) = recv.recv() {
            sender.send(7).unwrap();
        }
    });
    ready_recv.recv_timeout(Duration::from_secs(5)).unwrap();

    for mode in ["empty_run_once", "public_ready", "local_ready", "local_yield", "external_oneshot"] {
        for sample in 0..SAMPLES {
            let ops = if mode == "empty_run_once" { 4096 } else { OPS };
            let done = Arc::new(AtomicUsize::new(0));
            let mut times = Vec::with_capacity(ops);
            let cpu = cpu_ns();
            let start = Instant::now();
            for index in 0..ops {
                let begin = Instant::now();
                if mode == "empty_run_once" {
                    assert_eq!(runner.run_once().unwrap(), 0);
                } else if mode == "external_oneshot" {
                    let (sender, receiver) = oneshot::channel();
                    let output = done.clone();
                    runtime.spawn(async move {
                        assert_eq!(receiver.await.unwrap(), 7);
                        output.fetch_add(1, Ordering::Release);
                    }).unwrap();
                    runner.run_once().unwrap();
                    assert_eq!(done.load(Ordering::Acquire), index);
                    send.send(sender).unwrap();
                    while done.load(Ordering::Acquire) != index + 1 {
                        runner.run_once().unwrap();
                        assert!(start.elapsed() < Duration::from_secs(10), "跨线程回填超过截止");
                        if runtime.len() == 0 { thread::yield_now(); }
                    }
                } else {
                    let output = done.clone();
                    let runtime_copy = runtime.clone();
                    let yielding = mode == "local_yield";
                    let task = async move {
                        if yielding { runtime_copy.yield_now().await; }
                        output.fetch_add(1, Ordering::Release);
                    };
                    if mode == "public_ready" {
                        runtime.spawn(task).unwrap();
                    } else {
                        runtime.spawn_local(task).unwrap();
                    }
                    runner.run_once().unwrap();
                    if yielding { runner.run_once().unwrap(); }
                    assert_eq!(done.load(Ordering::Acquire), index + 1);
                }
                times.push(begin.elapsed().as_nanos() as u64);
            }
            let elapsed = start.elapsed().as_nanos();
            let cpu = cpu_ns().saturating_sub(cpu);
            assert_eq!(runtime.len(), 0);
            assert!(elapsed < 10_000_000_000, "场景超过十秒截止");
            report(mode, sample, times, elapsed, cpu);
        }
    }
    drop(send);
    producer.join().unwrap();
}
