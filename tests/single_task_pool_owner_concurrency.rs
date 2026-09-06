//! 风险聚焦的真实并发专项/检测器入口：一个消费线程，四个外部提交/唤醒线程。
//! 只创建单线程/单工作者运行时，不创建 MultiTaskRuntime。每例小于 512 个任务；
//! 普通测试和 TSan 共用严格断言，不使用 mock、替代队列、人工轮询 Future 或 block_on。
//! 设计与验收索引：`docs/SINGLE_TASK_POOL_OWNER_DESIGN.md#owner-design`，OWN-F05/F10。

use std::{sync::{atomic::{AtomicBool, AtomicUsize, Ordering}, mpsc, Arc, Barrier, Mutex},
    thread, time::{Duration, Instant}};
use futures::channel::oneshot;
use pi_async_rt::prelude::{AsyncRuntime, AsyncValue, SingleTaskRunner, WorkerTaskRunner};

const DEADLINE: Duration = Duration::from_secs(10);
const PRODUCERS: usize = 4;
const PER_PRODUCER: usize = 48;
const EXTRA: usize = 24;

struct Released(Arc<AtomicUsize>);
impl Drop for Released {
    fn drop(&mut self) { self.0.fetch_add(1, Ordering::SeqCst); }
}

/// OWN-F10：生产者从干净线程进入；192个Pending任务与96次混合提交，精确完成/线程/析构断言。
#[test]
fn test_four_external_producers_wake_and_submit_to_one_real_consumer() {
    let runner = SingleTaskRunner::<()>::default();
    let rt = runner.startup().unwrap();
    let result = Arc::new(Mutex::new(Vec::new()));
    let dropped = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(AtomicUsize::new(0));
    let mut batches = Vec::new();
    for producer in 0..PRODUCERS {
        let mut batch = Vec::new();
        for offset in 0..PER_PRODUCER {
            let id = producer * PER_PRODUCER + offset;
            let (send, recv) = oneshot::channel();
            let value = AsyncValue::new();
            let setter = value.clone();
            let output = result.clone();
            let entered = started.clone();
            let witness = Released(dropped.clone());
            rt.spawn(async move {
                let _witness = witness;
                entered.fetch_add(1, Ordering::SeqCst);
                let (received, set) = futures::join!(recv, value);
                assert_eq!(set, id);
                if id.is_multiple_of(3) { assert!(received.is_err()); }
                else { assert_eq!(received.unwrap(), id); }
                output.lock().unwrap().push((id, thread::current().id()));
            }).unwrap();
            batch.push((id, send, setter));
        }
        batches.push(batch);
    }
    let (ready_send, ready_recv) = mpsc::sync_channel(1);
    let owner_rt = rt.clone();
    let observed = result.clone();
    let owner = thread::spawn(move || {
        let start = Instant::now();
        let mut sent = false;
        let total = PRODUCERS * (PER_PRODUCER + EXTRA);
        while observed.lock().unwrap().len() != total || owner_rt.len() != 0 {
            assert!(start.elapsed() < DEADLINE, "单消费者未在截止内完成");
            runner.run_once().unwrap();
            if !sent && started.load(Ordering::SeqCst) == PRODUCERS * PER_PRODUCER {
                // run_once 已返回，首批真实接收 future 均已完成首次 Pending。
                assert_eq!(owner_rt.len(), 0);
                ready_send.send(thread::current().id()).unwrap();
                sent = true;
            }
            thread::yield_now();
        }
        assert!(sent);
        assert_eq!(owner_rt.wait_len(), 0);
        thread::current().id()
    });
    let consumer_id = ready_recv.recv_timeout(DEADLINE).unwrap();
    let start = Arc::new(Barrier::new(PRODUCERS));
    let mut producers = Vec::new();
    for (index, batch) in batches.into_iter().enumerate() {
        let rt = rt.clone();
        let output = result.clone();
        let drops = dropped.clone();
        let start = start.clone();
        producers.push(thread::spawn(move || {
            start.wait();
            for (id, send, setter) in batch {
                setter.set(id);
                if id.is_multiple_of(3) { drop(send); }
                else { send.send(id).unwrap(); }
            }
            for offset in 0..EXTRA {
                let id = PRODUCERS * PER_PRODUCER + index * EXTRA + offset;
                let output = output.clone();
                let witness = Released(drops.clone());
                let task = async move {
                    let _witness = witness;
                    output.lock().unwrap().push((id, thread::current().id()));
                };
                match offset % 3 {
                    0 => { rt.spawn(task).unwrap(); },
                    1 => { rt.spawn_local(task).unwrap(); },
                    _ => { rt.spawn_priority(usize::MAX, task).unwrap(); },
                }
            }
        }));
    }
    for producer in producers { producer.join().unwrap(); }
    assert_eq!(owner.join().unwrap(), consumer_id);
    let total = PRODUCERS * (PER_PRODUCER + EXTRA);
    let mut records = result.lock().unwrap().clone();
    records.sort_by_key(|record| record.0);
    assert_eq!(records.len(), total);
    for (id, record) in records.iter().enumerate() { assert_eq!(*record, (id, consumer_id)); }
    assert_eq!(dropped.load(Ordering::SeqCst), total);
    assert_eq!(rt.len(), 0);
}

/// 析构通知只证明真实 worker 闭包已退出，不控制其运行时身份或睡眠协议。
struct Exited(Arc<AtomicBool>);
impl Drop for Exited {
    fn drop(&mut self) { self.0.store(true, Ordering::Release); }
}

/// OWN-F10-W：由公开startup创建两个真实worker，互相回填后必须关闭并释放各自loop闭包。
#[test]
fn test_two_real_worker_threads_cross_runtime_response_and_shutdown() {
    let mut runtimes = Vec::new();
    let mut exited = Vec::new();
    for name in ["Owner-Contract-A", "Owner-Contract-B"] {
        let runner = WorkerTaskRunner::<()>::default();
        let consumer = runner.clone();
        let stopped = Arc::new(AtomicBool::new(false));
        let witness = Exited(stopped.clone());
        let observed = runner.get_runtime();
        let rt = runner.startup(name, 1024 * 1024, 10, None, move || {
            let _keep = &witness;
            let started = Instant::now();
            let len = consumer.run_once().unwrap();
            (len == 0, started.elapsed())
        }, move || observed.len());
        runtimes.push(rt);
        exited.push(stopped);
    }
    let (done_send, done_recv) = mpsc::sync_channel(1);
    let (a, b) = (runtimes[0].clone(), runtimes[1].clone());
    a.clone().spawn(async move {
        let origin = thread::current().id();
        let wait = a.wait();
        wait.spawn(b.clone(), None, async move {
            let worker = thread::current().id();
            let value = AsyncValue::new();
            let setter = value.clone();
            a.spawn(async move { setter.set(thread::current().id()); }).unwrap();
            assert_eq!(value.await, origin);
            Ok(worker)
        }).unwrap();
        let remote = wait.wait_result().await.unwrap();
        assert_ne!(remote, origin);
        assert_eq!(thread::current().id(), origin);
        done_send.send(()).unwrap();
    }).unwrap();
    let completed = done_recv.recv_timeout(DEADLINE);
    for rt in &runtimes { rt.close(); }
    let start = Instant::now();
    while !exited.iter().all(|flag| flag.load(Ordering::Acquire)) {
        assert!(start.elapsed() < DEADLINE, "真实 worker 未退出");
        thread::sleep(Duration::from_millis(1));
    }
    completed.unwrap();
    for rt in &runtimes { assert_eq!(rt.len(), 0); }
}
