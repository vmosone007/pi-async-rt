//! 经公开创建入口（creation API）及原生队列（native queue）验证MPSC节点（node）的唯一持有。
//! 分配观测（allocation observation）只记录当前测试线程（test thread）的请求，原样委托System。
//! 此处为有界基本测试（bounded basic tests），不替代内存安全专项（memory-safety checks）或基准（benchmark）。

use cordyceps::mpsc_queue::TryDequeueError;
use cordyceps::MpscQueue;
use pi_async_task::definitions::QueueSupport;
use pi_async_task::MpscQueueEntry;
use pi_async_task::{create, create_local, MpscEntryHandle, QueueEntry, Runnable, Schedule, TaskOptions, TaskThread};
use pi_async_task::definitions::{FutureDropStatus, RunOutcome, TaskId, TerminationKind};
use std::future::Future;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::AtomicBool;

/// 只观察分配器（allocator）边界；所有实际分配／释放（allocation/deallocation）原样委托System。
struct ObservedAllocator;
#[global_allocator]
static ALLOCATOR: ObservedAllocator = ObservedAllocator;
thread_local! {
    // 仅记录当前测试线程（test thread）的请求和释放，避免其它测试（test）复用已释放地址（address）干扰计数。
    static RECORD_ALLOCATIONS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static WATCH_DEALLOCATIONS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
static ALLOCATION_COUNT: AtomicUsize = AtomicUsize::new(0);
static ALLOCATIONS: [AllocationRecord; 16] = [const { AllocationRecord::new() }; 16];

/// 固定大小原子记录（atomic record）不在分配器（allocator）内部递归分配或加锁（lock）。
struct AllocationRecord {
    address: AtomicUsize,
    size: AtomicUsize,
    align: AtomicUsize,
    deallocations: AtomicUsize,
    wrong_layout: AtomicBool,
}
impl AllocationRecord {
    const fn new() -> Self {
        Self { address: AtomicUsize::new(0), size: AtomicUsize::new(0), align: AtomicUsize::new(0),
            deallocations: AtomicUsize::new(0), wrong_layout: AtomicBool::new(false) }
    }
}
// SAFETY: 所有请求（request）原样委托System，观测（observation）仅使用固定原子（atomic）存储，不改变失败或释放策略。
unsafe impl GlobalAlloc for ObservedAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: GlobalAlloc调用者提供合法布局（layout），原样转交同一系统分配器（system allocator）。
        let pointer = unsafe { System.alloc(layout) };
        if RECORD_ALLOCATIONS.try_with(std::cell::Cell::get).unwrap_or(false) && !pointer.is_null() {
            let index = ALLOCATION_COUNT.fetch_add(1, Ordering::SeqCst);
            if let Some(record) = ALLOCATIONS.get(index) {
                record.size.store(layout.size(), Ordering::SeqCst);
                record.align.store(layout.align(), Ordering::SeqCst);
                record.address.store(pointer as usize, Ordering::SeqCst);
            }
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        if WATCH_DEALLOCATIONS.try_with(std::cell::Cell::get).unwrap_or(false) {
            for record in &ALLOCATIONS {
                if record.address.load(Ordering::SeqCst) == pointer as usize {
                    record.deallocations.fetch_add(1, Ordering::SeqCst);
                    if record.size.load(Ordering::SeqCst) != layout.size() || record.align.load(Ordering::SeqCst) != layout.align() {
                        record.wrong_layout.store(true, Ordering::SeqCst);
                    }
                }
            }
        }
        // SAFETY: 观察（observation）没有接管资源（resource），原指针（pointer）及布局（layout）交回原System一次。
        unsafe { System.dealloc(pointer, layout); }
    }
}

/// 只在一个串行测试（serial test）的有界阶段观测；退出守卫（guard）在断言（assertion）失败时也停止记录。
struct AllocationObservation;
impl AllocationObservation {
    fn start() -> Self {
        for record in &ALLOCATIONS {
            record.address.store(0, Ordering::SeqCst);
            record.deallocations.store(0, Ordering::SeqCst);
            record.wrong_layout.store(false, Ordering::SeqCst);
        }
        ALLOCATION_COUNT.store(0, Ordering::SeqCst);
        WATCH_DEALLOCATIONS.with(|watch| watch.set(true));
        RECORD_ALLOCATIONS.with(|record| record.set(true));
        Self
    }
}
impl Drop for AllocationObservation {
    fn drop(&mut self) {
        RECORD_ALLOCATIONS.with(|record| record.set(false));
        WATCH_DEALLOCATIONS.with(|watch| watch.set(false));
    }
}

/// 根据真实返回地址（address）检查配对，不用分配总数推算目标对象（object）的清理。
fn allocation_record(address: usize) -> &'static AllocationRecord {
    assert!(ALLOCATION_COUNT.load(Ordering::SeqCst) <= ALLOCATIONS.len(), "observation-capacity-exceeded");
    ALLOCATIONS.iter().find(|record| record.address.load(Ordering::SeqCst) == address).expect("allocation-not-observed")
}

/// 只记录原异步计算（Future）的真实轮询（poll）、固定地址（Pin）和一次清理（cleanup）。
#[derive(Default)]
struct ProbeStats {
    polls: AtomicUsize,
    drops: AtomicUsize,
    poll_address: AtomicUsize,
    drop_address: AtomicUsize,
    task_id: AtomicU64,
    token_address: AtomicUsize,
    wrong_thread: AtomicUsize,
}

/// 非Unpin原异步计算（Future）以稳定内部标记（identity token）检验没有重建或移动。
struct Probe {
    stats: Arc<ProbeStats>,
    token: Arc<()>,
    thread: std::thread::ThreadId,
    ready_after: usize,
    _pin: PhantomPinned,
}
impl Future for Probe {
    type Output = usize;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<usize> {
        let this = self.as_ref().get_ref();
        let address = std::ptr::from_ref(this) as usize;
        let previous = this.stats.poll_address.swap(address, Ordering::SeqCst);
        assert!(previous == 0 || previous == address, "future-moved");
        this.stats.token_address.store(Arc::as_ptr(&this.token) as usize, Ordering::SeqCst);
        this.stats.task_id.store(pi_async_task::try_current_task_id().unwrap().get(), Ordering::SeqCst);
        this.stats.wrong_thread.fetch_add(usize::from(this.thread != std::thread::current().id()), Ordering::SeqCst);
        let polls = this.stats.polls.fetch_add(1, Ordering::SeqCst) + 1;
        if polls >= this.ready_after { Poll::Ready(73) } else { Poll::Pending }
    }
}
impl Drop for Probe {
    fn drop(&mut self) {
        self.stats.drop_address.store(std::ptr::from_ref(self) as usize, Ordering::SeqCst);
        self.stats.token_address.store(Arc::as_ptr(&self.token) as usize, Ordering::SeqCst);
        self.stats.wrong_thread.fetch_add(usize::from(self.thread != std::thread::current().id()), Ordering::SeqCst);
        self.stats.drops.fetch_add(1, Ordering::SeqCst);
    }
}

/// 建立观测夹具（observation fixture），不接管生产轮询（poll）或清理（cleanup）。
fn probe(ready_after: usize) -> (Probe, Arc<ProbeStats>, Arc<()>) {
    let stats = Arc::new(ProbeStats::default());
    let token = Arc::new(());
    (Probe { stats: stats.clone(), token: token.clone(), thread: std::thread::current().id(), ready_after, _pin: PhantomPinned }, stats, token)
}

/// 使用真实公开工厂（public factory），四种布局（layout）的预期能力由需求独立给定。
fn options_for(support: QueueSupport) -> TaskOptions {
    match support {
        QueueSupport::Plain => TaskOptions::plain(),
        QueueSupport::IntrusiveLocal => TaskOptions::intrusive(),
        QueueSupport::IntrusiveMpsc => TaskOptions::intrusive_mpsc(),
        QueueSupport::IntrusiveLocalAndMpsc => TaskOptions::intrusive_local_and_mpsc(),
    }
}

/// 只交出并合法恢复同一持有，取得公共Linked承诺的节点（node）和链接（link）地址作比较。
fn entry_addresses(entry: MpscEntryHandle) -> (MpscEntryHandle, usize, usize) {
    let pointer = <MpscQueueEntry as cordyceps::Linked<_>>::into_ptr(entry);
    // SAFETY: 本次唯一交出仍保活，尚未交给容器（container）且固定地址（Pin）保持；仅取链接（link）地址。
    let links = unsafe { <MpscQueueEntry as cordyceps::Linked<_>>::links(pointer) };
    // SAFETY: 同一次唯一恢复，没有原生容器（native container）仍访问该节点（node）。
    let entry = unsafe { <MpscQueueEntry as cordyceps::Linked<_>>::from_ptr(pointer) };
    (entry, pointer.as_ptr() as usize, links.as_ptr() as usize)
}

/// 依次使用两套真实公共节点操作（node operations），核失败原值返还及单份执行责任。
fn check_layout(mut runnable: Runnable, owner: &TaskThread, support: QueueSupport, stats: &ProbeStats) {
    let registration = runnable.admit(owner).unwrap();
    let before = runnable.snapshot();
    let mut local_address = None;
    for _ in 0..2 {
        // SAFETY: 当前为原线程（owner thread），独占尚未入链节点（node）；成功后立即按回退责任（rollback）取回。
        runnable = match unsafe { QueueEntry::store(runnable, owner.id()) } {
            Ok(node) => {
                assert!(matches!(support, QueueSupport::IntrusiveLocal | QueueSupport::IntrusiveLocalAndMpsc));
                let address = node.as_ptr() as usize;
                if let Some(previous) = local_address { assert_eq!(address, previous); }
                local_address = Some(address);
                // SAFETY: 未加入任何链（list），已完全脱离且由唯一ticket保活，原线程（owner thread）取回一次。
                unsafe { QueueEntry::take(node, owner.id()) }
            }
            Err(original) => {
                assert!(matches!(support, QueueSupport::Plain | QueueSupport::IntrusiveMpsc));
                original
            }
        };
        assert_eq!(runnable.snapshot(), before);
        runnable = match MpscEntryHandle::try_from_runnable(runnable) {
            Ok(entry) => {
                assert!(matches!(support, QueueSupport::IntrusiveMpsc | QueueSupport::IntrusiveLocalAndMpsc));
                let (entry, address, links) = entry_addresses(entry);
                if let Some(local) = local_address { assert_ne!(address, local); assert_ne!(links, local); }
                assert_eq!(registration.snapshot(), before);
                entry.try_into_runnable().unwrap_or_else(|_| panic!("business-node-rejected"))
            }
            Err(original) => {
                assert!(matches!(support, QueueSupport::Plain | QueueSupport::IntrusiveLocal));
                original
            }
        };
        assert_eq!(runnable.snapshot(), before);
        assert_eq!(stats.polls.load(Ordering::SeqCst), 0);
        assert_eq!(stats.drops.load(Ordering::SeqCst), 0);
    }
    assert!(matches!(runnable.run(owner), Ok(RunOutcome::Finished(TerminationKind::Completed))));
    assert_eq!(registration.snapshot().future_drop, FutureDropStatus::Confirmed);
    assert_eq!(owner.live_tasks(), 0);
}

/// 独立核原Future身份（identity）、原址析构（in-place drop）、任务编号（TaskId）及线程（thread）。
fn assert_completed(stats: &ProbeStats, token: &Arc<()>, id: TaskId, polls: usize) {
    assert_eq!(stats.polls.load(Ordering::SeqCst), polls);
    assert_eq!(stats.drops.load(Ordering::SeqCst), 1);
    assert_ne!(stats.poll_address.load(Ordering::SeqCst), 0);
    assert_eq!(stats.poll_address.load(Ordering::SeqCst), stats.drop_address.load(Ordering::SeqCst));
    assert_eq!(stats.token_address.load(Ordering::SeqCst), Arc::as_ptr(token) as usize);
    assert_eq!(stats.task_id.load(Ordering::SeqCst), id.get());
    assert_eq!(stats.wrong_thread.load(Ordering::SeqCst), 0);
    assert_eq!(Arc::strong_count(token), 1);
}

/// 外部调度（Schedule）只保管生产交付的原Runnable，不代轮询（poll）或推进清理（cleanup）。
/// 这三项场景（scenario）均启用MPSC链接（link）；原生队列（native queue）及独立空节点（node）在交付前建立。
/// 接收方仅在同步唤醒（wake-up）返回或远端线程（remote thread）结束后安全出队（dequeue），不等待或重试（retry）。
struct Delivery(Arc<MpscQueue<MpscQueueEntry>>);
impl Schedule for Delivery {
    fn schedule(&self, task: Runnable) {
        let entry = MpscEntryHandle::try_from_runnable(task)
            .unwrap_or_else(|_| panic!("delivery-mpsc-link-missing"));
        self.0.enqueue(entry);
    }
}

/// 原生队列（native queue）转交后只比较地址（address），不重建业务任务（Task）。
fn native_round_trip(runnable: Runnable, queue: &MpscQueue<MpscQueueEntry>) -> (Runnable, usize, usize) {
    let before = runnable.snapshot();
    let entry = MpscEntryHandle::try_from_runnable(runnable).unwrap_or_else(|_| panic!("mpsc-link-missing"));
    let (entry, address, links) = entry_addresses(entry);
    queue.enqueue(entry);
    let entry = queue.try_dequeue().unwrap();
    let (entry, returned, returned_links) = entry_addresses(entry);
    assert_eq!(returned, address);
    assert_eq!(returned_links, links);
    assert!(matches!(queue.try_dequeue(), Err(TryDequeueError::Empty)));
    let runnable = entry.try_into_runnable().unwrap_or_else(|_| panic!("business-node-rejected"));
    assert_eq!(runnable.snapshot(), before);
    (runnable, address, links)
}

/// Rc使原异步计算（Future）及输出（output）均不满足Send；清理（cleanup）只准在原线程（owner thread）。
struct LocalFuture {
    stats: Arc<ProbeStats>,
    local: std::rc::Rc<()>,
    thread: std::thread::ThreadId,
}
impl Future for LocalFuture {
    type Output = std::rc::Rc<()>;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.stats.polls.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(self.local.clone())
    }
}
impl Drop for LocalFuture {
    fn drop(&mut self) {
        self.stats.wrong_thread.fetch_add(usize::from(self.thread != std::thread::current().id()), Ordering::SeqCst);
        assert_eq!(self.thread, std::thread::current().id(), "local-future-dropped-remotely");
        self.stats.drops.fetch_add(1, Ordering::SeqCst);
    }
}

/// 本场景（scenario）没有合法调度（Schedule）动作，错误调用立即暴露。
struct NoSchedule;
impl Schedule for NoSchedule {
    fn schedule(&self, _task: Runnable) {
        panic!("unexpected-schedule");
    }
}

/// 核对TaskOptions::intrusive_mpsc的布局（layout）选择及默认观测（observation）策略。
#[test]
fn test_mpsc_options_select_only_mpsc_and_disable_observation() {
    let options = TaskOptions::intrusive_mpsc();
    assert_eq!(options.queue, QueueSupport::IntrusiveMpsc);
    assert!(!options.capture_panic_backtrace);
    #[cfg(feature = "observe")]
    assert!(options.observer.is_none());
}

/// 核对TaskOptions::intrusive_local_and_mpsc选择双链接（dual links），但不建立执行持有。
#[test]
fn test_dual_options_select_both_links_and_disable_observation() {
    let options = TaskOptions::intrusive_local_and_mpsc();
    assert_eq!(options.queue, QueueSupport::IntrusiveLocalAndMpsc);
    assert!(!options.capture_panic_backtrace);
    #[cfg(feature = "observe")]
    assert!(options.observer.is_none());
}

/// 经create及MpscEntryHandle双向转换（conversion）核原身份（identity）及原状态（state）。
/// 该完整持有链（ownership chain）的配对析构（drop）完成后，才允许执行成功分支。
#[test]
fn test_business_round_trip_preserves_original_runnable() {
    let parts = create(std::future::pending::<()>(), NoSchedule, (), TaskOptions::intrusive_mpsc())
        .unwrap_or_else(|_| panic!("creation-failed"));
    let before = parts.runnable.snapshot();
    let entry = MpscEntryHandle::try_from_runnable(parts.runnable)
        .unwrap_or_else(|_| panic!("mpsc-link-missing"));
    assert_eq!(parts.control.snapshot(), before);
    let runnable = entry.try_into_runnable().unwrap_or_else(|_| panic!("business-node-rejected"));
    assert_eq!(runnable.snapshot(), before);
    drop(runnable);
    assert_eq!(parts.control.snapshot().termination,
        Some(pi_async_task::definitions::TerminationKind::Abandoned));
}

/// 经独立空节点工厂（empty-node factory）及真实MpscQueue核建立、失败原值返还和复用。
/// 每次原生交接（native handoff）只恢复一次持有，不从任意地址构造句柄（handle）。
#[test]
fn test_empty_node_is_returned_unchanged_and_reused_by_native_queue() {
    let stub = MpscEntryHandle::try_new_empty().unwrap();
    let entry = MpscEntryHandle::try_new_empty().unwrap();
    let pointer = <MpscQueueEntry as cordyceps::Linked<_>>::into_ptr(entry);
    let stub_pointer = <MpscQueueEntry as cordyceps::Linked<_>>::into_ptr(stub);
    assert_ne!(pointer, stub_pointer);
    // SAFETY: 两份指针（pointer）各来自本次唯一交出，未交给容器（container），仍存活且未移动。
    let mut entry = unsafe { <MpscQueueEntry as cordyceps::Linked<_>>::from_ptr(pointer) };
    // SAFETY: stub_pointer持有独立、尚未恢复的同一次交接责任（handoff responsibility）。
    let stub = unsafe { <MpscQueueEntry as cordyceps::Linked<_>>::from_ptr(stub_pointer) };
    let queue = MpscQueue::<MpscQueueEntry>::new_with_stub(stub);
    assert!(matches!(queue.try_dequeue(), Err(TryDequeueError::Empty)));
    for _ in 0..2 {
        entry = entry.try_into_runnable().err().expect("empty-node-must-be-returned");
        let returned = <MpscQueueEntry as cordyceps::Linked<_>>::into_ptr(entry);
        assert_eq!(returned, pointer);
        // SAFETY: returned为本轮唯一交出，尚未入队（enqueue）或恢复，地址（address）保持。
        entry = unsafe { <MpscQueueEntry as cordyceps::Linked<_>>::from_ptr(returned) };
        queue.enqueue(entry);
        entry = queue.try_dequeue().unwrap();
        assert!(matches!(queue.try_dequeue(), Err(TryDequeueError::Empty)));
    }
    drop(entry);
    drop(queue);
}

/// create的四种布局（layout）必须保持旧本地能力，缺链接（link）时原样返还且不提前清理。
#[test]
fn test_send_creation_exposes_exact_four_layout_capabilities() {
    for support in [QueueSupport::Plain, QueueSupport::IntrusiveLocal, QueueSupport::IntrusiveMpsc, QueueSupport::IntrusiveLocalAndMpsc] {
        let owner = TaskThread::new();
        let (future, stats, token) = probe(1);
        let mut parts = create(future, NoSchedule, (), options_for(support)).unwrap_or_else(|_| panic!("creation-failed"));
        let id = parts.runnable.id();
        check_layout(parts.runnable, &owner, support, &stats);
        assert_completed(&stats, &token, id, 1);
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(matches!(Pin::new(&mut parts.join).poll(&mut cx), Poll::Ready(Ok(73))));
    }
}

/// create_local同样核四布局（layout），创建时所属域（owner）及两套初始空ticket保持。
#[test]
fn test_local_creation_exposes_exact_four_layout_capabilities() {
    for support in [QueueSupport::Plain, QueueSupport::IntrusiveLocal, QueueSupport::IntrusiveMpsc, QueueSupport::IntrusiveLocalAndMpsc] {
        let owner = TaskThread::new();
        let (future, stats, token) = probe(1);
        let mut parts = create_local(&owner, future, NoSchedule, (), options_for(support)).unwrap_or_else(|_| panic!("creation-failed"));
        let id = parts.runnable.id();
        assert_eq!(parts.runnable.snapshot().owner, Some(owner.id()));
        check_layout(parts.runnable, &owner, support, &stats);
        assert_completed(&stats, &token, id, 1);
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(matches!(Pin::new(&mut parts.join).poll(&mut cx), Poll::Ready(Ok(73))));
    }
}

/// 同一业务节点（business node）经真实原生队列（native queue）复用，前后轮询（poll）保持同一Future地址及身份（identity）。
#[test]
fn test_native_business_handoff_preserves_pinned_future_across_polls() {
    let owner = TaskThread::new();
    let queue = MpscQueue::<MpscQueueEntry>::new_with_stub(MpscEntryHandle::try_new_empty().unwrap());
    let delivery = Arc::new(MpscQueue::<MpscQueueEntry>::new_with_stub(MpscEntryHandle::try_new_empty().unwrap()));
    let (future, stats, token) = probe(2);
    let mut parts = create(future, Delivery(delivery.clone()), (), TaskOptions::intrusive_local_and_mpsc()).unwrap_or_else(|_| panic!("creation-failed"));
    let id = parts.runnable.id();
    let (runnable, address, links) = native_round_trip(parts.runnable, &queue);
    assert_eq!(stats.polls.load(Ordering::SeqCst), 0);
    assert!(matches!(runnable.run(&owner), Ok(RunOutcome::Pending)));
    let original_address = stats.poll_address.load(Ordering::SeqCst);
    assert_ne!(original_address, 0);
    parts.control.wake();
    let runnable = delivery.try_dequeue().unwrap().try_into_runnable()
        .unwrap_or_else(|_| panic!("business-node-rejected"));
    assert!(matches!(delivery.try_dequeue(), Err(TryDequeueError::Empty)));
    let (runnable, returned, returned_links) = native_round_trip(runnable, &queue);
    assert_eq!(returned, address);
    assert_eq!(returned_links, links);
    assert_eq!(stats.polls.load(Ordering::SeqCst), 1);
    assert_eq!(stats.drops.load(Ordering::SeqCst), 0);
    assert!(matches!(runnable.run(&owner), Ok(RunOutcome::Finished(TerminationKind::Completed))));
    assert_completed(&stats, &token, id, 2);
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(matches!(Pin::new(&mut parts.join).poll(&mut cx), Poll::Ready(Ok(73))));
    assert_eq!(owner.live_tasks(), 0);
}

/// 真实原生队列（native queue）析构（drop）放弃业务持有，不等控制句柄（control handle）才清理原Future。
#[test]
fn test_native_queue_drop_abandons_each_business_future_once() {
    let queue = MpscQueue::<MpscQueueEntry>::new_with_stub(MpscEntryHandle::try_new_empty().unwrap());
    let (future, stats, token) = probe(usize::MAX);
    let mut parts = create(future, NoSchedule, (), TaskOptions::intrusive_mpsc()).unwrap_or_else(|_| panic!("creation-failed"));
    queue.enqueue(MpscEntryHandle::try_from_runnable(parts.runnable).unwrap_or_else(|_| panic!("mpsc-link-missing")));
    assert_eq!(stats.drops.load(Ordering::SeqCst), 0);
    drop(queue);
    assert_eq!(stats.polls.load(Ordering::SeqCst), 0);
    assert_eq!(stats.drops.load(Ordering::SeqCst), 1);
    assert_eq!(Arc::strong_count(&token), 1);
    assert_eq!(parts.control.snapshot().termination, Some(TerminationKind::Abandoned));
    assert_eq!(parts.control.snapshot().future_drop, FutureDropStatus::Confirmed);
    let mut cx = Context::from_waker(std::task::Waker::noop());
    assert!(matches!(Pin::new(&mut parts.join).poll(&mut cx), Poll::Ready(Err(pi_async_task::TaskError::Abandoned))));
}

/// 非Send原Future远端释放（remote drop）只交回退出Runnable，再经原生队列（native queue）在原线程（owner thread）清理。
#[test]
fn test_remote_non_send_release_routes_original_cleanup_back_to_owner() {
    for support in [QueueSupport::IntrusiveMpsc, QueueSupport::IntrusiveLocalAndMpsc] {
        let owner = TaskThread::new();
        let delivery = Arc::new(MpscQueue::<MpscQueueEntry>::new_with_stub(MpscEntryHandle::try_new_empty().unwrap()));
        let stats = Arc::new(ProbeStats::default());
        let local = std::rc::Rc::new(());
        let future = LocalFuture { stats: stats.clone(), local: local.clone(), thread: std::thread::current().id() };
        let mut parts = create_local(&owner, future, Delivery(delivery.clone()), (), options_for(support)).unwrap_or_else(|_| panic!("creation-failed"));
        let id = parts.runnable.id();
        let entry = MpscEntryHandle::try_from_runnable(parts.runnable).unwrap_or_else(|_| panic!("mpsc-link-missing"));
        std::thread::spawn(move || drop(entry)).join().unwrap();
        assert_eq!(stats.drops.load(Ordering::SeqCst), 0);
        assert_eq!(std::rc::Rc::strong_count(&local), 2);
        let cleanup = delivery.try_dequeue().unwrap().try_into_runnable()
            .unwrap_or_else(|_| panic!("business-node-rejected"));
        assert_eq!(cleanup.id(), id);
        assert_eq!(cleanup.snapshot().owner, Some(owner.id()));
        assert_eq!(cleanup.snapshot().termination, Some(TerminationKind::Abandoned));
        assert!(!cleanup.snapshot().admitted);
        let queue = MpscQueue::<MpscQueueEntry>::new_with_stub(MpscEntryHandle::try_new_empty().unwrap());
        let (cleanup, _, _) = native_round_trip(cleanup, &queue);
        assert!(matches!(cleanup.run(&owner), Ok(RunOutcome::Finished(TerminationKind::Abandoned))));
        assert_eq!(stats.polls.load(Ordering::SeqCst), 0);
        assert_eq!(stats.drops.load(Ordering::SeqCst), 1);
        assert_eq!(stats.wrong_thread.load(Ordering::SeqCst), 0);
        assert_eq!(std::rc::Rc::strong_count(&local), 1);
        assert!(matches!(delivery.try_dequeue(), Err(TryDequeueError::Empty)));
        assert_eq!(owner.live_tasks(), 0);
        let mut cx = Context::from_waker(std::task::Waker::noop());
        assert!(matches!(Pin::new(&mut parts.join).poll(&mut cx), Poll::Ready(Err(pi_async_task::TaskError::Abandoned))));
    }
}

/// 原线程关闭（owner close）能清理仍被节点（node）持有的非Send原Future，迟到远端释放（remote drop）保留Cancelled原因。
#[test]
fn test_owner_close_cleans_local_future_before_late_remote_node_drop() {
    let owner = TaskThread::new();
    let delivery = Arc::new(MpscQueue::<MpscQueueEntry>::new_with_stub(MpscEntryHandle::try_new_empty().unwrap()));
    let stats = Arc::new(ProbeStats::default());
    let local = std::rc::Rc::new(());
    let future = LocalFuture { stats: stats.clone(), local: local.clone(), thread: std::thread::current().id() };
    let parts = create_local(&owner, future, Delivery(delivery.clone()), (), TaskOptions::intrusive_local_and_mpsc()).unwrap_or_else(|_| panic!("creation-failed"));
    let entry = MpscEntryHandle::try_from_runnable(parts.runnable).unwrap_or_else(|_| panic!("mpsc-link-missing"));
    owner.close();
    assert_eq!(stats.drops.load(Ordering::SeqCst), 1);
    assert_eq!(std::rc::Rc::strong_count(&local), 1);
    assert_eq!(parts.control.snapshot().termination, Some(TerminationKind::Cancelled));
    assert_eq!(parts.control.snapshot().future_drop, FutureDropStatus::Confirmed);
    std::thread::spawn(move || drop(entry)).join().unwrap();
    assert_eq!(stats.drops.load(Ordering::SeqCst), 1);
    assert_eq!(stats.wrong_thread.load(Ordering::SeqCst), 0);
    assert!(matches!(delivery.try_dequeue(), Err(TryDequeueError::Empty)));
    assert_eq!(parts.control.snapshot().termination, Some(TerminationKind::Cancelled));
    assert_eq!(owner.live_tasks(), 0);
}

/// 经try_new_empty、Linked及原生队列（native queue）核准确布局（layout）和一次释放（deallocation）；
/// 再经create保留节点（node）为最后持有者（last owner），核转换（conversion）不分配及整块存储（storage）回收。
#[test]
fn test_allocation_pairing_and_last_business_owner_release() {
    {
        let observation = AllocationObservation::start();
        let stub = MpscEntryHandle::try_new_empty().unwrap();
        let entry = MpscEntryHandle::try_new_empty().unwrap();
        let (stub, stub_address, _) = entry_addresses(stub);
        let (entry, address, _) = entry_addresses(entry);
        let stub_record = allocation_record(stub_address);
        let record = allocation_record(address);
        let layout = Layout::new::<MpscQueueEntry>();
        for record in [stub_record, record] {
            assert_eq!(record.size.load(Ordering::SeqCst), layout.size());
            assert_eq!(record.align.load(Ordering::SeqCst), layout.align());
            assert_eq!(record.deallocations.load(Ordering::SeqCst), 0);
        }
        assert_eq!(ALLOCATION_COUNT.load(Ordering::SeqCst), 2);
        let queue = MpscQueue::<MpscQueueEntry>::new_with_stub(stub);
        let entry = entry.try_into_runnable().err().expect("empty-node-must-be-returned");
        queue.enqueue(entry);
        let entry = queue.try_dequeue().unwrap();
        assert_eq!(ALLOCATION_COUNT.load(Ordering::SeqCst), 2);
        assert_eq!(record.deallocations.load(Ordering::SeqCst), 0);
        drop(entry);
        assert_eq!(record.deallocations.load(Ordering::SeqCst), 1);
        assert_eq!(stub_record.deallocations.load(Ordering::SeqCst), 0);
        drop(queue);
        drop(observation);
        for record in [stub_record, record] {
            assert_eq!(record.deallocations.load(Ordering::SeqCst), 1);
            assert!(!record.wrong_layout.load(Ordering::SeqCst));
        }
    }
    for support in [QueueSupport::IntrusiveMpsc, QueueSupport::IntrusiveLocalAndMpsc] {
        let (future, stats, token) = probe(usize::MAX);
        let observation = AllocationObservation::start();
        let parts = create(future, NoSchedule, (), options_for(support))
            .unwrap_or_else(|_| panic!("creation-failed"));
        let allocations_before = ALLOCATION_COUNT.load(Ordering::SeqCst);
        let entry = MpscEntryHandle::try_from_runnable(parts.runnable)
            .unwrap_or_else(|_| panic!("mpsc-link-missing"));
        let (entry, address, _) = entry_addresses(entry);
        assert!(allocations_before <= ALLOCATIONS.len(), "observation-capacity-exceeded");
        // 节点（node）是原分配（allocation）的内部地址；不将其当作独立分配（allocation）的首地址。
        let record = ALLOCATIONS.iter().find(|record| {
            let start = record.address.load(Ordering::SeqCst);
            start != 0 && address >= start && address - start < record.size.load(Ordering::SeqCst)
        }).expect("task-storage-allocation-not-observed");
        let runnable = entry.try_into_runnable().unwrap_or_else(|_| panic!("business-node-rejected"));
        let entry = MpscEntryHandle::try_from_runnable(runnable)
            .unwrap_or_else(|_| panic!("mpsc-link-missing"));
        assert_eq!(ALLOCATION_COUNT.load(Ordering::SeqCst), allocations_before);
        assert_eq!(Arc::strong_count(&stats), 2);
        assert_eq!(Arc::strong_count(&token), 2);
        drop(parts.control);
        drop(parts.join);
        assert_eq!(record.deallocations.load(Ordering::SeqCst), 0);
        assert_eq!(stats.drops.load(Ordering::SeqCst), 0);
        drop(entry);
        drop(observation);
        assert_eq!(record.deallocations.load(Ordering::SeqCst), 1);
        assert!(!record.wrong_layout.load(Ordering::SeqCst));
        assert_eq!(stats.polls.load(Ordering::SeqCst), 0);
        assert_eq!(stats.drops.load(Ordering::SeqCst), 1);
        assert_eq!(Arc::strong_count(&stats), 1);
        assert_eq!(Arc::strong_count(&token), 1);
    }
}
