//! MPSC节点（node）的有界安全专项：真实原生交接、原线程清理及局部分配失败。
//! 仅通过公开接口（API）触达生产行为；不实现上层计数、清理或关闭协议。
//! 两个生产线程（producer threads）、一个消费线程（consumer thread），每种布局（layout）各8个任务（Task）。
//! 单独指定本测试目标（test target），并使用--test-threads=1；不是压力或性能基准（benchmark）。

use cordyceps::mpsc_queue::TryDequeueError;
use cordyceps::MpscQueue;
use pi_async_task::definitions::{FutureDropStatus, RunOutcome, TerminationKind};
use pi_async_task::{
    create, create_local, MpscEntryHandle, MpscQueueEntry, Runnable, Schedule, TaskOptions,
    TaskThread,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::future::Future;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

/// 仅在指定测试线程（test thread）主动启用时，拒绝一次准确布局（layout）的分配请求。
/// 其它请求原样委托System；不全局耗尽内存，也不改变生产分配接口（allocation API）。
struct FailOneAllocation;

#[global_allocator]
static ALLOCATOR: FailOneAllocation = FailOneAllocation;

thread_local! {
    static FAIL_EMPTY_NODE: Cell<bool> = const { Cell::new(false) };
    static FAILED_ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

// SAFETY: 除唯一明确注入的合法空指针（null pointer）返回外，所有请求和释放配对转交System。
// 线程局部常量（thread-local constant）访问不分配、不记录日志；不在此处制造递归分配。
unsafe impl GlobalAlloc for FailOneAllocation {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout == Layout::new::<MpscQueueEntry>()
            && FAIL_EMPTY_NODE.try_with(|armed| armed.replace(false)).unwrap_or(false)
        {
            let _ = FAILED_ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
            return std::ptr::null_mut();
        }
        // SAFETY: 上层提供合法布局（layout），由同一系统分配器（system allocator）实际分配。
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: 非空成功分配全部来自System；没有把注入失败当作已分配存储（storage）。
        unsafe { System.dealloc(pointer, layout) }
    }
}

/// 原生队列（native queue）装配；只交付原Runnable，不代替任务（Task）轮询或清理。
#[derive(Clone)]
struct NativeDelivery(Arc<MpscQueue<MpscQueueEntry>>);

impl Schedule for NativeDelivery {
    fn schedule(&self, runnable: Runnable) {
        self.0.enqueue(
            MpscEntryHandle::try_from_runnable(runnable)
                .unwrap_or_else(|_| panic!("缺少MPSC链接（link）")),
        );
    }
}

/// 建立真实cordyceps队列（queue），独立空节点（empty node）由生产工厂（factory）提供。
fn native_queue() -> Arc<MpscQueue<MpscQueueEntry>> {
    Arc::new(MpscQueue::new_with_stub(MpscEntryHandle::try_new_empty().unwrap()))
}

/// 只观测地址、身份、次数及线程（thread），不修正生产状态（state）。
#[derive(Default)]
struct Evidence {
    address: AtomicUsize,
    task: AtomicU64,
    polls: AtomicUsize,
    drops: AtomicUsize,
}

/// 首次轮询（poll）中真实唤醒（wake），返回挂起（Pending）；下次返回就绪（Ready）。
struct TwicePolled {
    evidence: Arc<Evidence>,
    owner_thread: std::thread::ThreadId,
    _pin: PhantomPinned,
}

impl Future for TwicePolled {
    type Output = u64;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<u64> {
        let this = self.as_ref().get_ref();
        assert_eq!(this.owner_thread, std::thread::current().id());
        let address = std::ptr::from_ref(this) as usize;
        let previous = this.evidence.address.swap(address, Ordering::Relaxed);
        assert!(previous == 0 || previous == address);
        let task = pi_async_task::try_current_task_id().unwrap().get();
        let previous = this.evidence.task.swap(task, Ordering::Relaxed);
        assert!(previous == 0 || previous == task);
        match this.evidence.polls.fetch_add(1, Ordering::Relaxed) {
            0 => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            1 => Poll::Ready(task),
            _ => panic!("重复轮询（poll）"),
        }
    }
}

impl Drop for TwicePolled {
    fn drop(&mut self) {
        assert_eq!(self.owner_thread, std::thread::current().id());
        assert_eq!(self.evidence.address.load(Ordering::Relaxed), std::ptr::from_ref(self) as usize);
        assert_eq!(self.evidence.drops.fetch_add(1, Ordering::Relaxed), 0);
    }
}

/// 两个生产者（producer）与单个消费者（consumer）同时使用真实队列（queue）。
/// 覆盖单／双链接（link）、发布、取回、轮询中唤醒、再次交接及最后释放；不使用标准库通道或锁。
/// 有限16个任务（Task）、32次业务轮询（poll）；20秒只作失败截止，不参与业务判定。
#[test]
fn test_concurrent_native_handoff_and_link_reuse() {
    let owner = TaskThread::new();
    let queue = native_queue();
    let start = Arc::new(AtomicBool::new(false));
    let owner_thread = std::thread::current().id();
    let mut producers = Vec::new();
    for _ in 0..2 {
        let queue = queue.clone();
        let start = start.clone();
        producers.push(std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(20);
            while !start.load(Ordering::Acquire) {
                assert!(Instant::now() < deadline, "生产者（producer）启动超时");
                std::thread::yield_now();
            }
            let mut retained = Vec::new();
            for dual in [false, true] {
                for _ in 0..4 {
                    let evidence = Arc::new(Evidence::default());
                    let future = TwicePolled { evidence: evidence.clone(), owner_thread, _pin: PhantomPinned };
                    let options = if dual { TaskOptions::intrusive_local_and_mpsc() } else { TaskOptions::intrusive_mpsc() };
                    let parts = create(future, NativeDelivery(queue.clone()), (), options)
                        .unwrap_or_else(|_| panic!("创建失败"));
                    let id = parts.runnable.id();
                    queue.enqueue(MpscEntryHandle::try_from_runnable(parts.runnable)
                        .unwrap_or_else(|_| panic!("缺少MPSC链接（link）")));
                    retained.push((id, parts.control, parts.join, evidence));
                }
            }
            retained
        }));
    }
    start.store(true, Ordering::Release);
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut completed = 0;
    let mut pending = 0;
    while completed < 16 {
        assert!(Instant::now() < deadline, "交接未完成");
        match queue.try_dequeue() {
            Ok(entry) => {
                let runnable = entry.try_into_runnable().unwrap_or_else(|_| panic!("业务节点（node）被拒绝"));
                match runnable.run(&owner).unwrap_or_else(|_| panic!("轮询（poll）被拒绝")) {
                    RunOutcome::Pending => pending += 1,
                    RunOutcome::Finished(TerminationKind::Completed) => completed += 1,
                    _ => panic!("非预期退出"),
                }
            }
            Err(TryDequeueError::Empty | TryDequeueError::Inconsistent) => std::thread::yield_now(),
            Err(TryDequeueError::Busy) => panic!("出现额外消费者（consumer）"),
        }
    }
    assert_eq!(pending, 16);
    let mut ids = Vec::new();
    for producer in producers {
        for (id, control, mut join, evidence) in producer.join().unwrap() {
            assert!(!ids.contains(&id));
            ids.push(id);
            assert_eq!(evidence.task.load(Ordering::Relaxed), id.get());
            assert_eq!(evidence.polls.load(Ordering::Relaxed), 2);
            assert_eq!(evidence.drops.load(Ordering::Relaxed), 1);
            assert_eq!(control.snapshot().future_drop, FutureDropStatus::Confirmed);
            let mut cx = Context::from_waker(std::task::Waker::noop());
            assert!(matches!(Pin::new(&mut join).poll(&mut cx), Poll::Ready(Ok(value)) if value == id.get()));
        }
    }
    assert_eq!(ids.len(), 16);
    assert_eq!(owner.live_tasks(), 0);
    assert!(matches!(queue.try_dequeue(), Err(TryDequeueError::Empty)));
}

/// 只允许所属线程（owner thread）清理的本地原值，轮询（poll）不是本例的合法路径。
struct LocalDrop {
    local: Rc<()>,
    drops: Arc<AtomicUsize>,
    owner: std::thread::ThreadId,
}

impl Future for LocalDrop {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        panic!("本例不得执行原异步计算（Future）");
    }
}

impl Drop for LocalDrop {
    fn drop(&mut self) {
        assert_eq!(self.owner, std::thread::current().id());
        assert_eq!(Rc::strong_count(&self.local), 2);
        assert_eq!(self.drops.fetch_add(1, Ordering::Relaxed), 0);
    }
}

/// 所属线程关闭（owner close）与远端释放（remote drop）同时可达；两者共享原生产清理路径。
/// 首个合法原因可能为Abandoned或Cancelled，后续不得覆盖；不允许外线程析构本地原值。
#[test]
fn test_owner_close_races_remote_node_release() {
    let owner = TaskThread::new();
    let queue = native_queue();
    let mut entries = Vec::new();
    let mut retained = Vec::new();
    for _ in 0..4 {
        let local = Rc::new(());
        let drops = Arc::new(AtomicUsize::new(0));
        let future = LocalDrop { local: local.clone(), drops: drops.clone(), owner: std::thread::current().id() };
        let parts = create_local(&owner, future, NativeDelivery(queue.clone()), (), TaskOptions::intrusive_local_and_mpsc())
            .unwrap_or_else(|_| panic!("创建失败"));
        entries.push(MpscEntryHandle::try_from_runnable(parts.runnable)
            .unwrap_or_else(|_| panic!("缺少MPSC链接（link）")));
        retained.push((parts.control, parts.join, drops, local));
    }
    let start = Arc::new(AtomicBool::new(false));
    let remote_start = start.clone();
    let remote = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(20);
        while !remote_start.load(Ordering::Acquire) {
            assert!(Instant::now() < deadline, "远端启动超时");
            std::thread::yield_now();
        }
        drop(entries);
    });
    start.store(true, Ordering::Release);
    owner.close();
    remote.join().unwrap();
    loop {
        match queue.try_dequeue() {
            Ok(entry) => drop(entry),
            Err(TryDequeueError::Empty) => break,
            Err(other) => panic!("生产者（producer）已结束仍未完成交接：{other:?}"),
        }
    }
    for (control, mut join, drops, local) in retained {
        let snapshot = control.snapshot();
        assert!(matches!(snapshot.termination, Some(TerminationKind::Abandoned | TerminationKind::Cancelled)));
        assert_eq!(snapshot.future_drop, FutureDropStatus::Confirmed);
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(Rc::strong_count(&local), 1);
        let mut cx = Context::from_waker(std::task::Waker::noop());
        let actual = Pin::new(&mut join).poll(&mut cx);
        match snapshot.termination.unwrap() {
            TerminationKind::Abandoned => assert!(matches!(actual, Poll::Ready(Err(pi_async_task::TaskError::Abandoned)))),
            TerminationKind::Cancelled => assert!(matches!(actual, Poll::Ready(Err(pi_async_task::TaskError::Cancelled)))),
            _ => unreachable!(),
        }
    }
    assert_eq!(owner.live_tasks(), 0);
    assert!(matches!(queue.try_dequeue(), Err(TryDequeueError::Empty)));
}

/// 对try_new_empty的一次真实分配（allocation）返回空指针（null pointer），检查准确错误分类。
/// 前后合法工厂调用必须成功；不修改生产分配器接口（allocator API），不影响其它线程（thread）。
#[test]
fn test_empty_node_allocation_failure_is_local_and_recoverable() {
    drop(MpscEntryHandle::try_new_empty().unwrap());
    FAILED_ALLOCATIONS.with(|count| count.set(0));
    FAIL_EMPTY_NODE.with(|armed| armed.set(true));
    let result = MpscEntryHandle::try_new_empty();
    let remained = FAIL_EMPTY_NODE.with(|armed| armed.replace(false));
    assert!(!remained, "未命中目标分配（allocation）");
    assert_eq!(FAILED_ALLOCATIONS.with(Cell::get), 1);
    let error = match result {
        Err(error) => error,
        Ok(entry) => {
            drop(entry);
            panic!("分配失败未返回错误");
        }
    };
    assert_eq!(*error.current_context(), pi_result::ErrorKind::ResourceExhausted);
    drop(MpscEntryHandle::try_new_empty().unwrap());
}

