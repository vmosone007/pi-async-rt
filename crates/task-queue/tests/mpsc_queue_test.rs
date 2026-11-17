//! 经公开MPSC接口（API）验证原持有交接、固定范围、永久停收和管理者退出责任。
use pi_async_task::{create, Runnable, Schedule, TaskOptions};
use pi_async_task_queue::{ExternalPushErrorKind, ExternalQueueManager, ExternalTaskQueue, MpscQueueOptions, MpscTaskQueue};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

/// 只在系统分配器（system allocator）边界注入指定空节点（empty node）失败，正常路径原样委托System。
struct ObservedAllocator;
#[global_allocator]
static ALLOCATOR: ObservedAllocator = ObservedAllocator;
thread_local! {
    static OBSERVE_ALLOC: Cell<bool> = const { Cell::new(false) };
    static FAIL_NODE_AT: Cell<usize> = const { Cell::new(0) };
    static NODE_REQUESTS: Cell<usize> = const { Cell::new(0) };
    static ALL_REQUESTS: Cell<usize> = const { Cell::new(0) };
    static NODE_POINTERS: Cell<[usize; 2]> = const { Cell::new([0; 2]) };
    static NODE_FREES: Cell<[usize; 2]> = const { Cell::new([0; 2]) };
}

// SAFETY: System负责准确布局（layout）的实际分配与释放；故障只返回空指针（null pointer），
// 观察（observation）使用线程局部（thread-local）固定空间，不展开（unwind）、加锁（lock）或递归分配。
unsafe impl GlobalAlloc for ObservedAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        coordinate_helper_entry(layout);
        let observing = OBSERVE_ALLOC.try_with(Cell::get).unwrap_or(false);
        let node_layout = Layout::new::<pi_async_task::MpscQueueEntry>();
        let node = observing && layout == node_layout
            && FAIL_NODE_AT.try_with(Cell::get).unwrap_or(usize::MAX) != usize::MAX;
        let mut index = 0;
        if observing {
            let _ = ALL_REQUESTS.try_with(|count| count.set(count.get().saturating_add(1)));
        }
        if node {
            index = NODE_REQUESTS.try_with(|count| {
                let next = count.get().saturating_add(1);
                count.set(next);
                next
            }).unwrap_or(0);
            if FAIL_NODE_AT.try_with(Cell::get).unwrap_or(0) == index {
                // 故障已经命中；错误报告（Report）可能采用同大小布局（layout），不将其误算为下一空节点（empty node）。
                let _ = FAIL_NODE_AT.try_with(|value| value.set(usize::MAX));
                return std::ptr::null_mut();
            }
        }
        // SAFETY: 合法GlobalAlloc布局（layout）原样转交System。
        let pointer = unsafe { System.alloc(layout) };
        if node && index > 0 && index <= 2 && !pointer.is_null() {
            let _ = NODE_POINTERS.try_with(|record| {
                let mut pointers = record.get();
                pointers[index - 1] = pointer as usize;
                record.set(pointers);
            });
        }
        pointer
    }
    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        if OBSERVE_ALLOC.try_with(Cell::get).unwrap_or(false) {
            let pointers = NODE_POINTERS.try_with(Cell::get).unwrap_or([0; 2]);
            let _ = NODE_FREES.try_with(|record| {
                let mut frees = record.get();
                for index in 0..2 {
                    if pointers[index] != 0 && pointers[index] == pointer as usize {
                        frees[index] = frees[index].saturating_add(1);
                    }
                }
                record.set(frees);
            });
        }
        // SAFETY: 指针（pointer）与布局（layout）仍属于System的原配对请求（request）。
        unsafe { System.dealloc(pointer, layout); }
    }
}

/// 串行测试（serial test）内启停观察（observation），包括断言（assertion）展开（unwinding）路径。
struct ObserveAlloc;
impl ObserveAlloc {
    fn start(fail_at: usize) -> Self {
        FAIL_NODE_AT.with(|value| value.set(fail_at));
        NODE_REQUESTS.with(|value| value.set(0));
        ALL_REQUESTS.with(|value| value.set(0));
        NODE_POINTERS.with(|value| value.set([0; 2]));
        NODE_FREES.with(|value| value.set([0; 2]));
        OBSERVE_ALLOC.with(|value| value.set(true));
        Self
    }
}
impl Drop for ObserveAlloc {
    fn drop(&mut self) { OBSERVE_ALLOC.with(|value| value.set(false)); }
}

/// 未绑定原异步计算（Future）的队列（queue）退出不应调度（Schedule）。
struct NoSchedule;
impl Schedule for NoSchedule {
    fn schedule(&self, _: Runnable) { panic!("未绑定原值不得调度（schedule）"); }
}

/// 禁止轮询（poll）并记录真实析构（drop）次数；不代补队列（queue）清理。
struct CountDrop(Arc<AtomicUsize>);
impl std::future::Future for CountDrop {
    type Output = ();
    fn poll(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        panic!("队列（queue）不得轮询（poll）");
    }
}
impl Drop for CountDrop {
    fn drop(&mut self) { self.0.fetch_add(1, Ordering::SeqCst); }
}

/// @api MpscQueueOptions::default；准确非零默认值（defaults）及按值复制不构造任务（Task）。
#[test]
fn test_options_default_values_survive_copy() {
    let options = MpscQueueOptions::default();
    let copied = options;
    assert_eq!(options.cleanup_batch_size.get(), 64);
    assert_eq!(copied.push_attempts.get(), 3);
}

/// @api MpscTaskQueue::new/handle/Drop；普通共享句柄（shared handle）不阻止空来源的管理者退出。
#[test]
fn test_empty_source_shared_handles_outlive_manager_without_work() {
    let queue = MpscTaskQueue::new(MpscQueueOptions::default()).unwrap();
    let handle = queue.handle();
    let clone = handle.clone();
    assert_eq!(handle.len(), 0);
    assert!(clone.is_empty());
    drop(queue);
    assert_eq!(clone.len(), 0);
    assert!(handle.is_empty());
}

/// @api ExternalTaskQueue::try_push/len及MpscTaskQueue::Drop；缺链接（link）返原值，管理者退出不等共享句柄（shared handle）。
#[test]
fn test_push_rejection_and_owner_exit_keep_original_responsibility() {
    let drops = Arc::new(AtomicUsize::new(0));
    let queue = MpscTaskQueue::new(MpscQueueOptions::default()).unwrap();
    let handle = queue.handle();
    let plain = create(CountDrop(drops.clone()), NoSchedule, (), TaskOptions::plain())
        .unwrap_or_else(|_| panic!("创建（creation）失败"));
    let id = plain.runnable.id();
    let error = handle.try_push(plain.runnable).err().expect("缺链接（link）必须拒绝");
    assert_eq!(error.cause, ExternalPushErrorKind::LinkMissing);
    assert_eq!(error.task.id(), id);
    assert_eq!(handle.len(), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(error.task);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let queued = create(CountDrop(drops.clone()), NoSchedule, (), TaskOptions::intrusive_mpsc())
        .unwrap_or_else(|_| panic!("创建（creation）失败"));
    handle.try_push(queued.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    assert_eq!(handle.len(), 1);
    drop(queue);
    assert_eq!(handle.len(), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    let late = create(CountDrop(drops.clone()), NoSchedule, (), TaskOptions::intrusive_mpsc())
        .unwrap_or_else(|_| panic!("创建（creation）失败"));
    let id = late.runnable.id();
    let error = handle.try_push(late.runnable).err().expect("退出后必须拒绝（reject）");
    assert_eq!(error.cause, ExternalPushErrorKind::Closed);
    assert_eq!(error.task.id(), id);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    drop(error.task);
    assert_eq!(drops.load(Ordering::SeqCst), 3);
}

/// @api MpscTaskQueueHandle::try_take_one；最终结果独立保活原任务（Task），计数先归还且原顺序不变。
#[test]
fn test_single_take_returns_original_independent_ownership() {
    let drops = Arc::new(AtomicUsize::new(0));
    let queue = MpscTaskQueue::new(MpscQueueOptions::default()).unwrap();
    let handle = queue.handle();
    assert!(matches!(handle.try_take_one(), pi_async_task_queue::MpscTake::Empty));
    let first = create(CountDrop(drops.clone()), NoSchedule, (), TaskOptions::intrusive_mpsc())
        .unwrap_or_else(|_| panic!("创建（creation）失败"));
    let id = first.runnable.id();
    handle.try_push(first.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    let returned = match handle.try_take_one() {
        pi_async_task_queue::MpscTake::Ready(task) => task,
        _ => panic!("单项获取（single take）必须交出原值"),
    };
    assert_eq!(returned.id(), id);
    assert_eq!(handle.len(), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(queue);
    assert!(matches!(handle.try_take_one(), pi_async_task_queue::MpscTake::Closed));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(returned);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}


/// @api ExternalTaskQueue::try_take_batch；固定数量内按原顺序移交，批次（TaskBatch）独立于管理者存续。
#[test]
fn test_batch_take_preserves_order_and_independent_lifetime() {
    let drops = Arc::new(AtomicUsize::new(0));
    let queue = MpscTaskQueue::new(MpscQueueOptions::default()).unwrap();
    let handle = queue.handle();
    assert!(matches!(handle.try_take_batch(), pi_async_task_queue::BatchAttempt::Empty));
    let mut ids = Vec::new();
    for _ in 0..4 {
        let parts = create(CountDrop(drops.clone()), NoSchedule, (), TaskOptions::intrusive_mpsc())
            .unwrap_or_else(|_| panic!("创建（creation）失败"));
        ids.push(parts.runnable.id());
        handle.try_push(parts.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    }
    let batch = match handle.try_take_batch() {
        pi_async_task_queue::BatchAttempt::Ready(batch) => batch,
        _ => panic!("批次获取（batch take）应有原值"),
    };
    assert_eq!(batch.len(), 4);
    assert_eq!(handle.len(), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(queue);
    assert!(matches!(handle.try_take_batch(), pi_async_task_queue::BatchAttempt::Closed));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(batch.into_iter().map(|task| task.id()).collect::<Vec<_>>(), ids);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
}

/// @api ExternalQueueManager::clear；额度包含终点（endpoint），后来提交不扩大本轮固定范围（fixed range）。
#[test]
fn test_clear_keeps_its_first_endpoint_and_later_tasks() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut options = MpscQueueOptions::default();
    options.cleanup_batch_size = std::num::NonZeroUsize::new(1).unwrap();
    let mut queue = MpscTaskQueue::new(options).unwrap();
    let handle = queue.handle();
    for _ in 0..2 {
        let parts = create(CountDrop(drops.clone()), NoSchedule, (), TaskOptions::intrusive_mpsc())
            .unwrap_or_else(|_| panic!("创建（creation）失败"));
        handle.try_push(parts.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    }
    assert!(matches!(queue.clear().unwrap(), pi_async_task_queue::QueueCleanupProgress::MoreWork));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let late = create(CountDrop(drops.clone()), NoSchedule, (), TaskOptions::intrusive_mpsc())
        .unwrap_or_else(|_| panic!("创建（creation）失败"));
    let id = late.runnable.id();
    handle.try_push(late.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    assert!(matches!(queue.clear().unwrap(), pi_async_task_queue::QueueCleanupProgress::MoreWork));
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    assert!(matches!(queue.clear().unwrap(), pi_async_task_queue::QueueCleanupProgress::Complete));
    assert_eq!(handle.len(), 1);
    let returned = match handle.try_take_one() {
        pi_async_task_queue::MpscTake::Ready(task) => task,
        _ => panic!("终点（endpoint）后的原值必须保留"),
    };
    assert_eq!(returned.id(), id);
    assert!(matches!(queue.clear().unwrap(), pi_async_task_queue::QueueCleanupProgress::Complete));
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    drop(returned);
    assert_eq!(drops.load(Ordering::SeqCst), 3);
}

/// @api ExternalQueueManager::close/clear；关闭合并未完成范围（range），立即永久停收且每次仅推进节点额度（node budget）。
#[test]
fn test_close_merges_clear_and_permanently_rejects_original() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut options = MpscQueueOptions::default();
    options.cleanup_batch_size = std::num::NonZeroUsize::new(1).unwrap();
    let mut queue = MpscTaskQueue::new(options).unwrap();
    let handle = queue.handle();
    for _ in 0..3 {
        let parts = create(CountDrop(drops.clone()), NoSchedule, (), TaskOptions::intrusive_mpsc())
            .unwrap_or_else(|_| panic!("创建（creation）失败"));
        handle.try_push(parts.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    }
    assert!(matches!(queue.clear().unwrap(), pi_async_task_queue::QueueCleanupProgress::MoreWork));
    assert!(matches!(queue.close().unwrap(), pi_async_task_queue::QueueCleanupProgress::MoreWork));
    assert!(matches!(handle.try_take_one(), pi_async_task_queue::MpscTake::Closed));
    assert!(matches!(handle.try_take_batch(), pi_async_task_queue::BatchAttempt::Closed));
    let late = create(CountDrop(drops.clone()), NoSchedule, (), TaskOptions::intrusive_mpsc())
        .unwrap_or_else(|_| panic!("创建（creation）失败"));
    let id = late.runnable.id();
    let error = handle.clone().try_push(late.runnable).err().expect("关闭后必须拒绝（reject）");
    assert_eq!(error.cause, ExternalPushErrorKind::Closed);
    assert_eq!(error.task.id(), id);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    drop(error.task);
    assert!(matches!(queue.close().unwrap(), pi_async_task_queue::QueueCleanupProgress::MoreWork));
    assert!(matches!(queue.close().unwrap(), pi_async_task_queue::QueueCleanupProgress::MoreWork));
    assert!(matches!(queue.clear().unwrap(), pi_async_task_queue::QueueCleanupProgress::Complete));
    assert!(matches!(queue.close().unwrap(), pi_async_task_queue::QueueCleanupProgress::Complete));
    assert!(handle.is_empty());
    assert_eq!(drops.load(Ordering::SeqCst), 4);
}

/// 原异步计算（Future）的首次析构（drop）恐慌（panic）保留原载荷，不再次尝试失败原值。
struct PanicDrop {
    drops: Arc<AtomicUsize>,
    panic: bool,
}
impl std::future::Future for PanicDrop {
    type Output = ();
    fn poll(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        panic!("不得轮询（poll）");
    }
}
impl Drop for PanicDrop {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if self.panic { panic!("first-future-drop"); }
    }
}

/// 展开（unwinding）时只核对已发生副作用，不移除队列（queue）原值、不代补清理。
struct VerifyUnwind {
    handle: pi_async_task_queue::MpscTaskQueueHandle,
    drops: Arc<AtomicUsize>,
    expected: usize,
}
impl Drop for VerifyUnwind {
    fn drop(&mut self) {
        assert_eq!(self.handle.len(), 0);
        assert_eq!(self.drops.load(Ordering::SeqCst), self.expected);
    }
}

/// @api MpscTaskQueue::Drop；首项恐慌（panic）后仍清理跨额度尾部（tail），共享句柄（shared handle）继续存活。
#[test]
#[should_panic(expected = "first-future-drop")]
fn test_owner_exit_panic_cleans_pending_and_uncollected_tail() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut options = MpscQueueOptions::default();
    options.cleanup_batch_size = std::num::NonZeroUsize::new(2).unwrap();
    let queue = MpscTaskQueue::new(options).unwrap();
    let handle = queue.handle();
    for index in 0..5 {
        let parts = create(PanicDrop { drops: drops.clone(), panic: index == 0 }, NoSchedule, (), TaskOptions::intrusive_mpsc())
            .unwrap_or_else(|_| panic!("创建（creation）失败"));
        handle.try_push(parts.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    }
    let _verify = VerifyUnwind { handle, drops, expected: 5 };
    drop(queue);
}

/// @api MpscTaskQueue::Drop；容量溢出（capacity overflow）发生在Consumer之前，无分配备援仍承担全部关闭责任。
#[test]
#[should_panic(expected = "capacity overflow")]
fn test_owner_exit_capacity_panic_uses_allocation_free_fallback() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut options = MpscQueueOptions::default();
    options.cleanup_batch_size = std::num::NonZeroUsize::new(usize::MAX).unwrap();
    let queue = MpscTaskQueue::new(options).unwrap();
    let handle = queue.handle();
    for _ in 0..3 {
        let parts = create(CountDrop(drops.clone()), NoSchedule, (), TaskOptions::intrusive_mpsc())
            .unwrap_or_else(|_| panic!("创建（creation）失败"));
        handle.try_push(parts.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    }
    let _verify = VerifyUnwind { handle, drops, expected: 3 };
    drop(queue);
}

/// @api MpscTaskQueue::new；首／第二空节点（empty node）失败保留原Report，先前资源（resource）准确释放一次。
#[test]
fn test_empty_node_allocation_failures_preserve_report_and_pairing() {
    for fail_at in [1, 2] {
        let options = MpscQueueOptions::default();
        let observation = ObserveAlloc::start(fail_at);
        let result = MpscTaskQueue::new(options);
        let requests = NODE_REQUESTS.with(Cell::get);
        let pointers = NODE_POINTERS.with(Cell::get);
        let frees = NODE_FREES.with(Cell::get);
        drop(observation);
        let error = match result { Err(error) => error, Ok(_) => panic!("节点（node）故障未被原样反馈") };
        assert_eq!(*error.current_context(), pi_result::ErrorKind::ResourceExhausted);
        assert!(format!("{error:?}").contains("MPSC独立空节点"));
        assert_eq!(requests, fail_at);
        if fail_at == 1 {
            assert_eq!(pointers, [0, 0]);
            assert_eq!(frees, [0, 0]);
        } else {
            assert_ne!(pointers[0], 0);
            assert_eq!(pointers[1], 0);
            assert_eq!(frees, [1, 0]);
        }
    }
    let observation = ObserveAlloc::start(0);
    let queue = MpscTaskQueue::new(MpscQueueOptions::default()).unwrap();
    let pointers = NODE_POINTERS.with(Cell::get);
    assert_ne!(pointers[0], pointers[1]);
    assert!(pointers.iter().all(|pointer| *pointer != 0));
    drop(queue);
    let frees = NODE_FREES.with(Cell::get);
    drop(observation);
    assert_eq!(frees, [1, 1]);
}

/// @api MpscTaskQueueHandle::try_take_one/try_push；合法单项往返（round trip）无批次空间或每任务分配（allocation）。
#[test]
fn test_single_take_and_push_do_not_allocate() {
    let queue = MpscTaskQueue::new(MpscQueueOptions::default()).unwrap();
    let handle = queue.handle();
    let parts = create(std::future::pending::<()>(), NoSchedule, (), TaskOptions::intrusive_mpsc())
        .unwrap_or_else(|_| panic!("创建（creation）失败"));
    let observation = ObserveAlloc::start(0);
    handle.try_push(parts.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    let task = match handle.try_take_one() {
        pi_async_task_queue::MpscTake::Ready(task) => task,
        _ => panic!("获取（take）失败"),
    };
    let allocations = ALL_REQUESTS.with(Cell::get);
    drop(observation);
    assert_eq!(allocations, 0);
    drop(task);
}

/// 真实析构（drop）回调（callback）重入同一来源，核实权限已归还且保护外清理仍计入数量（count）。
struct ReentrantDrop {
    handle: pi_async_task_queue::MpscTaskQueueHandle,
    late: Option<Runnable>,
    checks: Arc<AtomicUsize>,
}
impl std::future::Future for ReentrantDrop {
    type Output = ();
    fn poll(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        panic!("不得轮询（poll）");
    }
}
impl Drop for ReentrantDrop {
    fn drop(&mut self) {
        assert_eq!(self.handle.len(), 2);
        let late = self.late.take().unwrap();
        let id = late.id();
        self.handle.try_push(late).unwrap_or_else(|_| panic!("开放清理应允许提交（submission）"));
        let returned = match self.handle.try_take_one() {
            pi_async_task_queue::MpscTake::Ready(task) => task,
            _ => panic!("用户析构（drop）时Consumer必须已经归还"),
        };
        assert_eq!(returned.id(), id);
        drop(returned);
        assert_eq!(self.handle.len(), 2);
        assert!(matches!(self.handle.try_take_one(), pi_async_task_queue::MpscTake::WouldBlock));
        assert!(matches!(self.handle.try_take_batch(), pi_async_task_queue::BatchAttempt::WouldBlock));
        self.checks.fetch_add(1, Ordering::SeqCst);
    }
}

/// @api ExternalQueueManager::clear及普通获取（take）；重入（reentrancy）只消费后来提交，不重复当前清理原值或提前减数。
#[test]
fn test_cleanup_returns_consumer_before_reentrant_user_drop() {
    let drops = Arc::new(AtomicUsize::new(0));
    let checks = Arc::new(AtomicUsize::new(0));
    let mut queue = MpscTaskQueue::new(MpscQueueOptions::default()).unwrap();
    let handle = queue.handle();
    let late = create(CountDrop(drops.clone()), NoSchedule, (), TaskOptions::intrusive_mpsc())
        .unwrap_or_else(|_| panic!("创建（creation）失败"));
    let first = create(ReentrantDrop { handle: handle.clone(), late: Some(late.runnable), checks: checks.clone() },
        NoSchedule, (), TaskOptions::intrusive_mpsc()).unwrap_or_else(|_| panic!("创建（creation）失败"));
    let second = create(CountDrop(drops.clone()), NoSchedule, (), TaskOptions::intrusive_mpsc())
        .unwrap_or_else(|_| panic!("创建（creation）失败"));
    handle.try_push(first.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    handle.try_push(second.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    assert!(matches!(queue.clear().unwrap(), pi_async_task_queue::QueueCleanupProgress::MoreWork));
    assert_eq!(checks.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    assert_eq!(handle.len(), 0);
    assert!(matches!(queue.clear().unwrap(), pi_async_task_queue::QueueCleanupProgress::Complete));
}

/// @api MpscTaskQueueHandle::try_take_one/try_take_batch与clear；普通获取跨过内部终点（endpoint）但不开始新范围（range）。
#[test]
fn test_normal_take_saves_endpoint_without_exposing_it() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut options = MpscQueueOptions::default();
    options.cleanup_batch_size = std::num::NonZeroUsize::new(1).unwrap();
    let mut queue = MpscTaskQueue::new(options).unwrap();
    let handle = queue.handle();
    let initial = create(CountDrop(drops.clone()), NoSchedule, (), TaskOptions::intrusive_mpsc())
        .unwrap_or_else(|_| panic!("创建（creation）失败"));
    handle.try_push(initial.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    assert!(matches!(queue.clear().unwrap(), pi_async_task_queue::QueueCleanupProgress::MoreWork));
    let late = create(CountDrop(drops.clone()), NoSchedule, (), TaskOptions::intrusive_mpsc())
        .unwrap_or_else(|_| panic!("创建（creation）失败"));
    let id = late.runnable.id();
    handle.try_push(late.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    let returned = match handle.try_take_one() {
        pi_async_task_queue::MpscTake::Ready(task) => task,
        _ => panic!("获取（take）必须跳过内部终点（endpoint）"),
    };
    assert_eq!(returned.id(), id);
    assert_eq!(handle.len(), 0);
    assert!(matches!(queue.clear().unwrap(), pi_async_task_queue::QueueCleanupProgress::Complete));
    drop(returned);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

/// 调度器（Schedule）只把原任务（Task）机制交来的清理持有送回原线程（owner thread）的合法来源。
struct OwnerDelivery(pi_async_task_queue::MpscTaskQueueHandle);
impl Schedule for OwnerDelivery {
    fn schedule(&self, task: Runnable) {
        self.0.try_push(task).unwrap_or_else(|_| panic!("原线程清理投递（owner cleanup delivery）失败"));
    }
}

/// 非Send原异步计算（Future）保留Rc与所属线程（owner thread），远端来源清理不能直接访问它。
struct LocalOwnerFuture {
    drops: std::rc::Rc<Cell<usize>>,
    owner: std::thread::ThreadId,
}
impl std::future::Future for LocalOwnerFuture {
    type Output = ();
    fn poll(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        panic!("不得轮询（poll）");
    }
}
impl Drop for LocalOwnerFuture {
    fn drop(&mut self) {
        assert_eq!(std::thread::current().id(), self.owner);
        self.drops.set(self.drops.get() + 1);
    }
}

/// @api MpscTaskQueue::Drop与原Runnable::run；远端管理者退出只合法移交非Send原值，在所属线程（owner thread）准确析构（drop）。
#[test]
fn test_remote_manager_exit_hands_local_future_back_to_owner() {
    let owner = pi_async_task::TaskThread::new();
    let delivery = MpscTaskQueue::new(MpscQueueOptions::default()).unwrap();
    let receive = delivery.handle();
    let drops = std::rc::Rc::new(Cell::new(0));
    let parts = pi_async_task::create_local(&owner,
        LocalOwnerFuture { drops: drops.clone(), owner: std::thread::current().id() },
        OwnerDelivery(receive.clone()), (), TaskOptions::intrusive_mpsc())
        .unwrap_or_else(|_| panic!("创建（creation）失败"));
    let id = parts.runnable.id();
    let queue = MpscTaskQueue::new(MpscQueueOptions::default()).unwrap();
    let handle = queue.handle();
    handle.try_push(parts.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    std::thread::spawn(move || drop(queue)).join().unwrap();
    assert_eq!(handle.len(), 0);
    assert_eq!(drops.get(), 0);
    assert_ne!(parts.control.snapshot().future_drop, pi_async_task::definitions::FutureDropStatus::Confirmed);
    let cleanup = match receive.try_take_one() {
        pi_async_task_queue::MpscTake::Ready(task) => task,
        _ => panic!("来源责任（source responsibility）未合法移交"),
    };
    assert_eq!(cleanup.id(), id);
    assert_eq!(cleanup.snapshot().owner, Some(owner.id()));
    assert!(matches!(cleanup.run(&owner), Ok(pi_async_task::definitions::RunOutcome::Finished(
        pi_async_task::definitions::TerminationKind::Abandoned))));
    assert_eq!(drops.get(), 1);
    assert_eq!(parts.control.snapshot().future_drop, pi_async_task::definitions::FutureDropStatus::Confirmed);
    assert!(receive.is_empty());
}

/// 结果通知（result notification）真实唤醒器（Waker）首次通知恐慌（panic），不修改生产通知路径。
struct PanicWake;
impl std::task::Wake for PanicWake {
    fn wake(self: Arc<Self>) { panic!("first-result-notification"); }
}

/// @api ExternalQueueManager::clear与JoinHandle::poll；通知异常（notification panic）不丢失本次pending原持有。
#[test]
#[should_panic(expected = "first-result-notification")]
fn test_clear_notification_panic_preserves_remaining_cleanup() {
    use std::future::Future;
    let drops = Arc::new(AtomicUsize::new(0));
    let mut queue = MpscTaskQueue::new(MpscQueueOptions::default()).unwrap();
    let handle = queue.handle();
    let mut first = create(CountDrop(drops.clone()), NoSchedule, (), TaskOptions::intrusive_mpsc())
        .unwrap_or_else(|_| panic!("创建（creation）失败"));
    let waker = std::task::Waker::from(Arc::new(PanicWake));
    let mut context = std::task::Context::from_waker(&waker);
    assert!(std::pin::Pin::new(&mut first.join).poll(&mut context).is_pending());
    handle.try_push(first.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    for _ in 0..2 {
        let parts = create(CountDrop(drops.clone()), NoSchedule, (), TaskOptions::intrusive_mpsc())
            .unwrap_or_else(|_| panic!("创建（creation）失败"));
        handle.try_push(parts.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    }
    let _verify = VerifyUnwind { handle, drops, expected: 3 };
    queue.clear().unwrap();
}

/// @api ExternalTaskQueue::try_push/try_take_batch；两个真实生产者（producers）各提交两份，按每生产者原顺序交付且只计数一次。
#[test]
fn test_two_producers_preserve_their_original_subsequence() {
    let drops = Arc::new(AtomicUsize::new(0));
    let queue = MpscTaskQueue::new(MpscQueueOptions::default()).unwrap();
    let handle = queue.handle();
    let mut workers = Vec::new();
    for _ in 0..2 {
        let producer = handle.clone();
        let drops = drops.clone();
        workers.push(std::thread::spawn(move || {
            let mut ids = Vec::new();
            // 对方仅两次增加；三次CAS预算（CAS budget）足以容纳这份有界争用，不做概率重试（probabilistic retry）。
            for _ in 0..2 {
                let parts = create(CountDrop(drops.clone()), NoSchedule, (), TaskOptions::intrusive_mpsc())
                    .unwrap_or_else(|_| panic!("创建（creation）失败"));
                ids.push(parts.runnable.id());
                producer.try_push(parts.runnable).unwrap_or_else(|_| panic!("有界提交（bounded submission）失败"));
            }
            ids
        }));
    }
    let expected: Vec<_> = workers.into_iter().map(|worker| worker.join().unwrap()).collect();
    assert_eq!(handle.len(), 4);
    let batch = match handle.try_take_batch() {
        pi_async_task_queue::BatchAttempt::Ready(batch) => batch,
        _ => panic!("批次（batch）应持有四份原值"),
    };
    let actual: Vec<_> = batch.into_iter().map(|task| task.id()).collect();
    assert_eq!(actual.len(), 4);
    for producer_ids in expected {
        assert_eq!(actual.iter().copied().filter(|id| producer_ids.contains(id)).collect::<Vec<_>>(), producer_ids);
    }
    assert_eq!(handle.len(), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
}

/// @api MpscTaskQueueHandle及TaskQueue::clear；同一双链接（dual-link）原值先MPSC后本地队列（local queue），所属线程（owner thread）保持。
#[test]
fn test_dual_link_original_moves_to_local_queue_before_cleanup() {
    let owner = pi_async_task::TaskThread::new();
    let drops = std::rc::Rc::new(Cell::new(0));
    let delivery = MpscTaskQueue::new(MpscQueueOptions::default()).unwrap();
    let receive = delivery.handle();
    let parts = pi_async_task::create_local(&owner,
        LocalOwnerFuture { drops: drops.clone(), owner: std::thread::current().id() },
        OwnerDelivery(receive.clone()), (), TaskOptions::intrusive_local_and_mpsc())
        .unwrap_or_else(|_| panic!("创建（creation）失败"));
    let id = parts.runnable.id();
    let queue = MpscTaskQueue::new(MpscQueueOptions::default()).unwrap();
    let handle = queue.handle();
    handle.try_push(parts.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    let returned = match handle.try_take_one() {
        pi_async_task_queue::MpscTake::Ready(task) => task,
        _ => panic!("获取（take）失败"),
    };
    assert_eq!(returned.id(), id);
    let mut local = pi_async_task_queue::TaskQueue::new(&owner);
    local.push_back(returned).unwrap_or_else(|_| panic!("本地链接（local link）不可复用"));
    assert!(matches!(local.clear().unwrap(), pi_async_task_queue::QueueCleanupProgress::Complete));
    assert_eq!(drops.get(), 0);
    let cleanup = match receive.try_take_one() {
        pi_async_task_queue::MpscTake::Ready(task) => task,
        _ => panic!("原持有（ownership）未移交"),
    };
    assert_eq!(cleanup.id(), id);
    assert!(matches!(cleanup.run(&owner), Ok(pi_async_task::definitions::RunOutcome::Finished(
        pi_async_task::definitions::TerminationKind::Abandoned))));
    assert_eq!(drops.get(), 1);
}

// 分配器边界（allocator boundary）仅控制公开调用（public call）的进入时点，不触碰来源状态（source state）。
// 第一阶段暂停普通批次空间（batch allocation），第二阶段暂停管理者退出后的帮助空间（helper allocation）。
thread_local! {
    static HELPER_ENTRY_SLOT: Cell<usize> = const { Cell::new(usize::MAX) };
}
static HELPER_ENTRY_STAGE: [AtomicUsize; 2] = [AtomicUsize::new(0), AtomicUsize::new(0)];
static HELPER_ENTRY_RELEASE: [AtomicUsize; 2] = [AtomicUsize::new(0), AtomicUsize::new(0)];
static HELPER_ENTRY_ROUNDS: [AtomicUsize; 2] = [AtomicUsize::new(0), AtomicUsize::new(0)];
static HELPER_ENTRY_STOP: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static HELPER_ENTRY_TIMEOUT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// 全局分配器（global allocator）中的有界进入控制（bounded entry control），不恐慌（panic）或分配（allocate）。
fn coordinate_helper_entry(layout: Layout) {
    let slot = HELPER_ENTRY_SLOT.try_with(Cell::get).unwrap_or(usize::MAX);
    if slot >= 2 { return; }
    let stage = HELPER_ENTRY_STAGE[slot].load(Ordering::Acquire);
    let next = if stage == 0 && layout == Layout::new::<Runnable>() {
        1
    } else if layout.size() == 64 * std::mem::size_of::<Runnable>()
        && layout.align() == std::mem::align_of::<Runnable>() {
        HELPER_ENTRY_ROUNDS[slot].fetch_add(1, Ordering::Relaxed);
        if stage == 1 { 2 } else { return; }
    } else {
        return;
    };
    // 防止线程停放（thread parking）的平台初始化触发嵌套观察（nested observation）。
    let _ = HELPER_ENTRY_SLOT.try_with(|value| value.set(usize::MAX));
    HELPER_ENTRY_STAGE[slot].store(next, Ordering::Release);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while HELPER_ENTRY_RELEASE[slot].load(Ordering::Acquire) < next
        && !HELPER_ENTRY_STOP.load(Ordering::Acquire) {
        if std::time::Instant::now() >= deadline {
            HELPER_ENTRY_TIMEOUT.store(true, Ordering::Release);
            break;
        }
        std::thread::park_timeout(std::time::Duration::from_millis(1));
    }
    let _ = HELPER_ENTRY_SLOT.try_with(|value| value.set(slot));
}

/// 断言（assertion）失败时解除测试侧暂停（test-side pause），不取出或清理任何生产原值（production value）。
struct ReleaseHelperEntry;
impl Drop for ReleaseHelperEntry {
    fn drop(&mut self) { HELPER_ENTRY_STOP.store(true, Ordering::Release); }
}

/// 在同一固定截止（deadline）前观察进入阶段（entry stage），不循环调用生产接口（production API）。
fn helper_stage_before(slot: usize, expected: usize, deadline: std::time::Instant) -> bool {
    while HELPER_ENTRY_STAGE[slot].load(Ordering::Acquire) < expected {
        if std::time::Instant::now() >= deadline { return false; }
        std::thread::park_timeout(std::time::Duration::from_millis(1));
    }
    true
}

/// @api try_take_batch与MpscTaskQueue::Drop；两个已进入的普通调用（ordinary calls）在停收后成为真实帮助者（helpers）。
/// 仅一份原任务（Task）和两个线程（threads）；五秒截止（deadline）核公开调用（public calls）完成及一次真实析构（drop）。
/// 本例控制帮助入口（helper entry），不能固定Consumer归还与请求采样（request sampling）之间的纯原子交错（atomic interleaving）。
#[test]
fn test_two_helpers_finish_after_manager_exit_without_more_tasks() {
    HELPER_ENTRY_STOP.store(false, Ordering::Release);
    HELPER_ENTRY_TIMEOUT.store(false, Ordering::Release);
    for slot in 0..2 {
        HELPER_ENTRY_STAGE[slot].store(0, Ordering::Release);
        HELPER_ENTRY_RELEASE[slot].store(0, Ordering::Release);
        HELPER_ENTRY_ROUNDS[slot].store(0, Ordering::Release);
    }
    let _release = ReleaseHelperEntry;
    let drops = Arc::new(AtomicUsize::new(0));
    let queue = MpscTaskQueue::new(MpscQueueOptions::default()).unwrap();
    let handle = queue.handle();
    let parts = create(CountDrop(drops.clone()), NoSchedule, (), TaskOptions::intrusive_mpsc())
        .unwrap_or_else(|_| panic!("创建（creation）失败"));
    handle.try_push(parts.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut workers = Vec::new();
    for slot in 0..2 {
        let worker_handle = handle.clone();
        workers.push(std::thread::spawn(move || {
            // 先完成平台停放（platform parking）初始化，再启用本线程分配器边界（allocator boundary）。
            std::thread::park_timeout(std::time::Duration::ZERO);
            HELPER_ENTRY_SLOT.with(|value| value.set(slot));
            let result = worker_handle.try_take_batch();
            HELPER_ENTRY_SLOT.with(|value| value.set(usize::MAX));
            assert!(matches!(result, pi_async_task_queue::BatchAttempt::Empty));
        }));
    }
    assert!(helper_stage_before(0, 1, deadline), "第一公开调用（public call）未进入");
    assert!(helper_stage_before(1, 1, deadline), "第二公开调用（public call）未进入");
    drop(queue);
    assert_eq!(handle.len(), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    for slot in 0..2 {
        HELPER_ENTRY_RELEASE[slot].store(1, Ordering::Release);
        assert!(helper_stage_before(slot, 2, deadline), "真实帮助调用（help call）未进入");
    }
    HELPER_ENTRY_RELEASE[0].store(2, Ordering::Release);
    HELPER_ENTRY_RELEASE[1].store(2, Ordering::Release);
    while workers.iter().any(|worker| !worker.is_finished()) && std::time::Instant::now() < deadline {
        std::thread::park_timeout(std::time::Duration::from_millis(1));
    }
    assert!(!HELPER_ENTRY_TIMEOUT.load(Ordering::Acquire), "测试侧进入控制（entry control）超过截止（deadline）");
    assert!(workers.iter().all(|worker| worker.is_finished()), "真实帮助调用（help calls）未在截止（deadline）前结束");
    for worker in workers { worker.join().unwrap(); }
    assert_eq!(handle.len(), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(HELPER_ENTRY_ROUNDS.iter().all(|rounds| rounds.load(Ordering::Acquire) >= 1));
}

/// 仅Windows x64调试构建（debug build）的真实指令调度（instruction scheduling）；不修改生产状态（production state）。
#[cfg(all(windows, target_arch = "x86_64", debug_assertions))]
mod exact_helper_schedule {
    use super::*;
    use std::ffi::{c_char, c_void, CString};
    use std::sync::atomic::AtomicBool;

    /// 对应本机Windows SDK的SYMBOL_INFO，尾部名称（name）由系统提供有效长度（length）。
    #[repr(C)]
    struct SymbolInfo {
        size_of_struct: u32, type_index: u32, reserved: [u64; 2], index: u32, size: u32,
        module_base: u64, flags: u32, value: u64, address: u64, register: u32,
        scope: u32, tag: u32, name_len: u32, max_name_len: u32, name: [u8; 1],
    }
    /// 对应Windows SDK的IMAGEHLP_LINE64，地址（address）只用于硬件执行断点（hardware execution breakpoint）。
    #[repr(C)]
    struct SourceLine { size: u32, key: *mut c_void, number: u32, file: *mut c_char, address: u64 }
    /// 对应Windows x64 CONTEXT的完整1232字节布局（layout），仅改变调试寄存器（debug registers）和恢复标志（resume flag）。
    #[repr(C, align(16))]
    struct ThreadContext {
        homes: [u64; 6], flags: u32, mxcsr: u32, segments: [u16; 6], eflags: u32,
        debug: [u64; 6], integer: [u64; 16], rip: u64, remainder: [u8; 976],
    }
    #[repr(C)]
    struct ExceptionRecord {
        code: u32, flags: u32, nested: *mut ExceptionRecord, address: *mut c_void,
        parameter_count: u32, information: [usize; 15],
    }
    #[repr(C)]
    struct ExceptionPointers { record: *mut ExceptionRecord, context: *mut ThreadContext }

    #[link(name = "dbghelp")]
    extern "system" {
        fn SymSetOptions(options: u32) -> u32;
        fn SymInitialize(process: *mut c_void, path: *const c_char, invade: i32) -> i32;
        fn SymCleanup(process: *mut c_void) -> i32;
        fn SymEnumSymbols(process: *mut c_void, base: u64, mask: *const c_char,
            callback: unsafe extern "system" fn(*mut SymbolInfo, u32, *mut c_void) -> i32, context: *mut c_void) -> i32;
        fn SymGetLineFromName64(process: *mut c_void, module: *const c_char, file: *const c_char,
            line: u32, displacement: *mut i32, result: *mut SourceLine) -> i32;
    }
    #[link(name = "kernel32")]
    extern "system" {
        fn GetCurrentProcess() -> *mut c_void;
        fn GetCurrentThreadId() -> u32;
        fn GetTickCount64() -> u64;
        fn Sleep(milliseconds: u32);
        fn OpenThread(access: u32, inherit: i32, id: u32) -> *mut c_void;
        fn SuspendThread(thread: *mut c_void) -> u32;
        fn ResumeThread(thread: *mut c_void) -> u32;
        fn GetThreadContext(thread: *mut c_void, context: *mut ThreadContext) -> i32;
        fn SetThreadContext(thread: *mut c_void, context: *const ThreadContext) -> i32;
        fn CloseHandle(handle: *mut c_void) -> i32;
        fn AddVectoredExceptionHandler(first: u32,
            handler: unsafe extern "system" fn(*mut ExceptionPointers) -> i32) -> *mut c_void;
        fn RemoveVectoredExceptionHandler(handle: *mut c_void) -> u32;
    }

    thread_local! { static SLOT: Cell<usize> = const { Cell::new(usize::MAX) }; }
    static IDS: [AtomicUsize; 2] = [AtomicUsize::new(0), AtomicUsize::new(0)];
    static EVENTS: [AtomicUsize; 2] = [AtomicUsize::new(0), AtomicUsize::new(0)];
    static KINDS: [AtomicUsize; 2] = [AtomicUsize::new(0), AtomicUsize::new(0)];
    static PERMITS: [AtomicUsize; 2] = [AtomicUsize::new(0), AtomicUsize::new(0)];
    static RELEASED: [AtomicUsize; 2] = [AtomicUsize::new(0), AtomicUsize::new(0)];
    static START: AtomicBool = AtomicBool::new(false);
    static STOP: AtomicBool = AtomicBool::new(false);
    static TIMEOUT: AtomicBool = AtomicBool::new(false);
    static EXCEPTIONAL: AtomicBool = AtomicBool::new(false);
    static HELPING: [AtomicBool; 2] = [AtomicBool::new(false), AtomicBool::new(false)];
    static REQUESTS: [AtomicUsize; 2] = [AtomicUsize::new(0), AtomicUsize::new(0)];
    const REQUEST: usize = 1;
    const ACQUIRE: usize = 2;
    const AFTER_RELEASE: usize = 3;

    /// 回调（callback）只收集链接产物（linked binary）的真实函数地址（function addresses）。
    unsafe extern "system" fn collect_symbol(info: *mut SymbolInfo, _: u32, context: *mut c_void) -> i32 {
        // SAFETY: DbgHelp在回调（callback）期间提供完整SYMBOL_INFO；context由本线程（thread）的有效Vec借用传入。
        let (info, addresses) = unsafe { (&*info, &mut *context.cast::<Vec<(u64, u32)>>()) };
        // SAFETY: NameLen是DbgHelp提供的尾部有效字节数（valid trailing byte count），只读本次符号（symbol）。
        let name = unsafe { std::slice::from_raw_parts(info.name.as_ptr(), info.name_len as usize) };
        let name = std::str::from_utf8(name).unwrap_or("");
        if info.tag == 5 && !name.contains("closure") && !addresses.iter().any(|item| item.0 == info.address) {
            addresses.push((info.address, info.size));
        }
        1
    }

    /// 符号会话（symbol session）仅在测试控制线程（test controller thread）使用，不与业务并发调用DbgHelp。
    struct Symbols(*mut c_void);
    impl Symbols {
        fn new() -> Self {
            let executable = std::env::current_exe().unwrap();
            let path = CString::new(executable.parent().unwrap().to_string_lossy().as_bytes()).unwrap();
            // SAFETY: 当前进程（process）伪句柄（pseudo handle）与有效C字符串（C string）满足系统签名（signature）。
            let process = unsafe { GetCurrentProcess() };
            unsafe { SymSetOptions(0x0000_0016 | 0x0000_0200); }
            assert_ne!(unsafe { SymInitialize(process, path.as_ptr(), 1) }, 0, "debugger-setup: SymInitialize");
            Self(process)
        }
        fn function(&self, name: &str) -> (u64, u32) {
            let mask = CString::new(format!("*!*{name}*")).unwrap();
            let mut addresses = Vec::<(u64, u32)>::new();
            // SAFETY: 同步回调（synchronous callback）不保存context，Vec借用（borrow）仅覆盖本次调用。
            assert_ne!(unsafe { SymEnumSymbols(self.0, 0, mask.as_ptr(), collect_symbol,
                (&mut addresses as *mut Vec<(u64, u32)>).cast()) }, 0, "debugger-setup: SymEnumSymbols {name}");
            assert_eq!(addresses.len(), 1, "debugger-setup: unique function {name}: {addresses:?}");
            addresses[0]
        }
        fn fallback_after_release(&self) -> u64 {
            let source = include_str!("../src/mpsc_inner.rs");
            let line = source.lines().position(|line| line.trim() == "let state = this.shared.state.fetch_or(0, AcqRel);")
                .expect("debugger-setup: fallback sample line") + 1;
            let file = CString::new(concat!(env!("CARGO_MANIFEST_DIR"), "/src/mpsc_inner.rs").replace('/', "\\")).unwrap();
            let mut result = SourceLine { size: std::mem::size_of::<SourceLine>() as u32,
                key: std::ptr::null_mut(), number: 0, file: std::ptr::null_mut(), address: 0 };
            let mut displacement = 0;
            // SAFETY: 输出缓冲区（output buffers）及文件名（filename）在同步系统调用（system call）期间有效。
            assert_ne!(unsafe { SymGetLineFromName64(self.0, std::ptr::null(), file.as_ptr(), line as u32,
                &mut displacement, &mut result) }, 0, "debugger-setup: fallback source line {line}");
            let (start, size) = self.function("mpsc_cleanup_drop_inner");
            assert!(result.address >= start && result.address < start + u64::from(size), "debugger-setup: source range");
            result.address
        }
    }
    impl Drop for Symbols {
        fn drop(&mut self) { unsafe { SymCleanup(self.0); } }
    }

    /// 断点异常（breakpoint exception）仅暂停执行，不改变生产指令（production instructions）、数据或结果寄存器（result registers）。
    unsafe extern "system" fn on_breakpoint(pointers: *mut ExceptionPointers) -> i32 {
        // SAFETY: Windows仅在异常处理器（exception handler）调用期间借出有效记录（records）。
        let (record, context) = unsafe { (&*(*pointers).record, &mut *(*pointers).context) };
        let slot = SLOT.try_with(Cell::get).unwrap_or(usize::MAX);
        if record.code != 0x8000_0004 || slot >= 2 || context.debug[4] & 7 == 0 { return 0; }
        let kind = if context.debug[4] & 1 != 0 { REQUEST }
            else if context.debug[4] & 2 != 0 { ACQUIRE } else { AFTER_RELEASE };
        if kind == REQUEST { HELPING[slot].store(true, Ordering::Release); }
        if !STOP.load(Ordering::Acquire) && HELPING[slot].load(Ordering::Acquire)
            && !(slot == 0 && EXCEPTIONAL.load(Ordering::Acquire) && !std::thread::panicking()) {
            if kind == REQUEST { REQUESTS[slot].fetch_add(1, Ordering::Relaxed); }
            if kind == AFTER_RELEASE { RELEASED[slot].fetch_add(1, Ordering::Relaxed); }
            KINDS[slot].store(kind, Ordering::Relaxed);
            let sequence = EVENTS[slot].fetch_add(1, Ordering::Release) + 1;
            let deadline = unsafe { GetTickCount64() } + 10_000;
            while PERMITS[slot].load(Ordering::Acquire) < sequence && !STOP.load(Ordering::Acquire) {
                if unsafe { GetTickCount64() } >= deadline {
                    TIMEOUT.store(true, Ordering::Release);
                    STOP.store(true, Ordering::Release);
                    break;
                }
                unsafe { Sleep(1); }
            }
        }
        if STOP.load(Ordering::Acquire) { context.debug[5] = 0; }
        context.debug[4] = 0;
        context.eflags |= 0x0001_0000;
        -1
    }

    /// 本线程（thread）先等待控制者安装硬件执行断点（hardware execution breakpoints），不进入任何生产操作（production operation）。
    fn enter_worker(slot: usize) {
        SLOT.with(|value| value.set(slot));
        IDS[slot].store(unsafe { GetCurrentThreadId() } as usize, Ordering::Release);
        let deadline = unsafe { GetTickCount64() } + 10_000;
        while !START.load(Ordering::Acquire) && !STOP.load(Ordering::Acquire) {
            assert!(unsafe { GetTickCount64() } < deadline, "debugger-setup: worker start deadline");
            unsafe { Sleep(1); }
        }
    }

    /// 只设置测试工作线程（test worker thread）的Dr0–Dr3／Dr6–Dr7，保留全部业务执行上下文（execution context）。
    fn install(slot: usize, addresses: [u64; 3]) {
        let deadline = unsafe { GetTickCount64() } + 10_000;
        while IDS[slot].load(Ordering::Acquire) == 0 {
            assert!(unsafe { GetTickCount64() } < deadline, "debugger-setup: worker ID deadline");
            unsafe { Sleep(1); }
        }
        // SAFETY: 只操作本测试（test）创建且暂停在进入门（entry gate）的线程（thread），不挂起未知线程（thread）。
        unsafe {
            let thread = OpenThread(0x005a, 0, IDS[slot].load(Ordering::Acquire) as u32);
            assert!(!thread.is_null(), "debugger-setup: OpenThread");
            let suspended = SuspendThread(thread);
            if suspended == u32::MAX { CloseHandle(thread); panic!("debugger-setup: SuspendThread"); }
            let mut context: ThreadContext = std::mem::zeroed();
            context.flags = 0x0010_0010;
            let read = GetThreadContext(thread, &mut context);
            context.debug = [addresses[0], addresses[1], addresses[2], 0, 0, 0x15];
            let written = if read != 0 { SetThreadContext(thread, &context) } else { 0 };
            let resumed = ResumeThread(thread);
            CloseHandle(thread);
            assert_ne!(read, 0, "debugger-setup: GetThreadContext");
            assert_ne!(written, 0, "debugger-setup: SetThreadContext");
            assert_ne!(resumed, u32::MAX, "debugger-setup: ResumeThread");
        }
    }

    /// 任一路径（path）退出都解除调试暂停（debugger pause），由生产代码（production code）自行完成清理（cleanup）。
    struct DebugSession(*mut c_void);
    impl DebugSession {
        fn new(exceptional: bool) -> Self {
            STOP.store(false, Ordering::Release);
            START.store(false, Ordering::Release);
            TIMEOUT.store(false, Ordering::Release);
            EXCEPTIONAL.store(exceptional, Ordering::Release);
            for slot in 0..2 {
                IDS[slot].store(0, Ordering::Release);
                EVENTS[slot].store(0, Ordering::Release);
                PERMITS[slot].store(0, Ordering::Release);
                RELEASED[slot].store(0, Ordering::Release);
                HELPING[slot].store(false, Ordering::Release);
                REQUESTS[slot].store(0, Ordering::Release);
                HELPER_ENTRY_STAGE[slot].store(0, Ordering::Release);
                HELPER_ENTRY_RELEASE[slot].store(0, Ordering::Release);
                HELPER_ENTRY_ROUNDS[slot].store(0, Ordering::Release);
            }
            HELPER_ENTRY_STOP.store(false, Ordering::Release);
            HELPER_ENTRY_TIMEOUT.store(false, Ordering::Release);
            let handler = unsafe { AddVectoredExceptionHandler(1, on_breakpoint) };
            assert!(!handler.is_null(), "debugger-setup: AddVectoredExceptionHandler");
            Self(handler)
        }
    }
    impl Drop for DebugSession {
        fn drop(&mut self) {
            STOP.store(true, Ordering::Release);
            START.store(true, Ordering::Release);
            HELPER_ENTRY_STOP.store(true, Ordering::Release);
            // 正常路径（normal path）已join两个线程（threads）；失败时保留处理器（handler）供在途断点（breakpoint）安全退出。
            if IDS.iter().all(|id| id.load(Ordering::Acquire) == 0) {
                unsafe { RemoveVectoredExceptionHandler(self.0); }
            }
        }
    }

    /// 控制断言（controller assertion）异常时也停止断点暂停（breakpoint pause）并回收本测试线程（test threads）。
    struct DebugWorkers(Vec<std::thread::JoinHandle<()>>);
    impl std::ops::Deref for DebugWorkers {
        type Target = Vec<std::thread::JoinHandle<()>>;
        fn deref(&self) -> &Self::Target { &self.0 }
    }
    impl std::ops::DerefMut for DebugWorkers {
        fn deref_mut(&mut self) -> &mut Self::Target { &mut self.0 }
    }
    impl Drop for DebugWorkers {
        fn drop(&mut self) {
            STOP.store(true, Ordering::Release);
            START.store(true, Ordering::Release);
            HELPER_ENTRY_STOP.store(true, Ordering::Release);
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while self.0.iter().any(|worker| !worker.is_finished()) && std::time::Instant::now() < deadline {
                unsafe { Sleep(1); }
            }
            // 不允许失败实验（failed experiment）的在途线程（in-flight thread）污染后续串行测试（serial tests）。
            if self.0.iter().any(|worker| !worker.is_finished()) { std::process::abort(); }
            for worker in self.0.drain(..) { drop(worker.join()); }
            for id in &IDS { id.store(0, Ordering::Release); }
        }
    }

    /// 观察一个真实断点（breakpoint）或原公开调用（public call）结束，截止（deadline）不构成目标失败（target failure）的替代。
    fn event(slot: usize, sequence: usize, kind: usize, worker: &std::thread::JoinHandle<()>, deadline: std::time::Instant) -> bool {
        while EVENTS[slot].load(Ordering::Acquire) < sequence {
            if worker.is_finished() { return false; }
            assert!(std::time::Instant::now() < deadline, "debugger-control: event deadline");
            unsafe { Sleep(1); }
        }
        assert_eq!(EVENTS[slot].load(Ordering::Acquire), sequence, "debugger-control: event sequence");
        assert_eq!(KINDS[slot].load(Ordering::Acquire), kind, "debugger-control: event kind");
        true
    }
    fn release(slot: usize) { PERMITS[slot].store(EVENTS[slot].load(Ordering::Acquire), Ordering::Release); }

    /// 正常帮助（normal help）及真实异常展开备援（unwind fallback）共用严格交错（interleaving），每次仅两个参与者（participants）。
    fn exercise(exceptional: bool) {
        assert_eq!(std::mem::size_of::<ThreadContext>(), 1232);
        let symbols = Symbols::new();
        let request = symbols.function("mpsc_request_cleanup_inner").0;
        let acquire = symbols.function("mpsc_consumer_new_inner").0;
        let ordinary_release = symbols.function("mpsc_cleanup_drain_inner").0;
        let fallback_release = if exceptional { symbols.fallback_after_release() } else { ordinary_release };
        let session = DebugSession::new(exceptional);
        let drops = Arc::new(AtomicUsize::new(0));
        let queue = MpscTaskQueue::new(MpscQueueOptions::default()).unwrap();
        let handle = queue.handle();
        if exceptional {
            let parts = create(PanicDrop { drops: drops.clone(), panic: true }, NoSchedule, (), TaskOptions::intrusive_mpsc())
                .unwrap_or_else(|_| panic!("创建（creation）失败"));
            handle.try_push(parts.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
        } else {
            let parts = create(CountDrop(drops.clone()), NoSchedule, (), TaskOptions::intrusive_mpsc())
                .unwrap_or_else(|_| panic!("创建（creation）失败"));
            handle.try_push(parts.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
        }
        let mut workers = DebugWorkers(Vec::new());
        let mut manager = Some(queue);
        for slot in 0..2 {
            if exceptional && slot == 0 {
                let queue = manager.take().unwrap();
                workers.push(std::thread::spawn(move || {
                    enter_worker(slot);
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
                    while HELPER_ENTRY_RELEASE[0].load(Ordering::Acquire) < 2 && !STOP.load(Ordering::Acquire) {
                        assert!(std::time::Instant::now() < deadline, "debugger-setup: manager entry deadline");
                        unsafe { Sleep(1); }
                    }
                    drop(queue);
                }));
            } else {
                let worker_handle = handle.clone();
                workers.push(std::thread::spawn(move || {
                    enter_worker(slot);
                    HELPER_ENTRY_SLOT.with(|value| value.set(slot));
                    let result = worker_handle.try_take_batch();
                    HELPER_ENTRY_SLOT.with(|value| value.set(usize::MAX));
                    assert!(matches!(result, pi_async_task_queue::BatchAttempt::Empty));
                }));
            }
        }
        install(0, [request, acquire, fallback_release]);
        install(1, [request, acquire, ordinary_release]);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        START.store(true, Ordering::Release);
        let ordinary_slots: &[usize] = if exceptional { &[1] } else { &[0, 1] };
        for &slot in ordinary_slots {
            assert!(helper_stage_before(slot, 1, deadline), "debugger-setup: ordinary batch entry");
        }
        drop(manager);
        HELPER_ENTRY_RELEASE[0].store(2, Ordering::Release);
        assert!(event(0, 1, REQUEST, &workers[0], deadline), "debugger-control: first real request");
        // 第一帮助者（helper）停在请求（request）前且未占Consumer，第二普通调用（ordinary call）可确定取得权限。
        HELPER_ENTRY_RELEASE[1].store(2, Ordering::Release);
        assert!(event(1, 1, REQUEST, &workers[1], deadline), "debugger-control: second real request");
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(handle.len(), 0);
        let mut sequence = [1usize, 1usize];
        // 只放行真实生产操作（production operation），依次固定“归还Consumer → 对方登记请求 → 原帮助者采样”。
        release(0);
        sequence[0] += 1;
        assert!(event(0, sequence[0], ACQUIRE, &workers[0], deadline));
        let mut cycles = 0;
        for _ in 0..4 {
            release(0);
            sequence[0] += 1;
            assert!(event(0, sequence[0], AFTER_RELEASE, &workers[0], deadline));
            release(1);
            sequence[1] += 1;
            if !event(1, sequence[1], ACQUIRE, &workers[1], deadline) { break; }
            release(0);
            sequence[0] += 1;
            if !event(0, sequence[0], REQUEST, &workers[0], deadline) { break; }
            release(1);
            sequence[1] += 1;
            assert!(event(1, sequence[1], AFTER_RELEASE, &workers[1], deadline));
            release(0);
            sequence[0] += 1;
            assert!(event(0, sequence[0], ACQUIRE, &workers[0], deadline));
            release(1);
            sequence[1] += 1;
            if !event(1, sequence[1], REQUEST, &workers[1], deadline) { break; }
            cycles += 1;
        }
        let released = [RELEASED[0].load(Ordering::Acquire), RELEASED[1].load(Ordering::Acquire)];
        let requests = [REQUESTS[0].load(Ordering::Acquire), REQUESTS[1].load(Ordering::Acquire)];
        STOP.store(true, Ordering::Release);
        HELPER_ENTRY_STOP.store(true, Ordering::Release);
        while workers.iter().any(|worker| !worker.is_finished()) && std::time::Instant::now() < deadline { unsafe { Sleep(1); } }
        assert!(workers.iter().all(|worker| worker.is_finished()), "debugger-control: public completion deadline");
        for (slot, worker) in workers.0.drain(..).enumerate() {
            let result = worker.join();
            if exceptional && slot == 0 {
                let error = result.expect_err("原始析构恐慌（original drop panic）必须继续传播");
                assert_eq!(error.downcast_ref::<&str>().copied(), Some("first-future-drop"));
            } else { result.unwrap(); }
            IDS[slot].store(0, Ordering::Release);
        }
        assert!(!TIMEOUT.load(Ordering::Acquire), "debugger-control: handler deadline");
        assert_eq!(handle.len(), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        println!("exact-helper-schedule exceptional={exceptional} cycles={cycles} released={released:?} requests={requests:?}");
        drop(session);
        // 各自只需初次请求（request）；单方多试一次也违背完整完成事实（completion fact），不能等另一方也重试才失败。
        assert_eq!(requests, [1, 1], "request-only-cycle: 已满足完整结束条件仍因对方请求（request）重试");
        assert_eq!(cycles, 0, "request-only-cycle: 完成来源（completed source）发生重复帮助循环（help cycle）");
    }

    /// @api try_take_batch与管理者Drop；固定四轮可重复交错（repeatable interleaving），验证完成事实不被纯请求（request）推翻。
    #[test]
    fn test_normal_helpers_do_not_repeat_completed_source_for_requests() { exercise(false); }
    /// @api 管理者Drop与原Future::drop；真实首次恐慌（panic）进入无分配备援（allocation-free fallback），原恐慌（panic）仍传播。
    #[test]
    fn test_unwind_helper_does_not_repeat_completed_source_for_requests() { exercise(true); }
}

/// 经公开入口（public APIs）验证真实进展通知（progress notification）；只控制本测试线程（test threads）的执行时点。
/// Windows x64调试构建（debug build）需要本地PDB；每例最多三工作线程（worker threads）、每线程三硬件执行断点（hardware execution breakpoints）。
/// 调度（scheduling）共用25秒截止（deadline），失败回收另限2秒；主会话（host session）的30秒命令上限同时覆盖符号准备（symbol setup）。
/// 只读生产内存（production memory），不修改业务寄存器（business registers）或指令（instructions）；装配（setup）或控制失败不算业务RED。
#[cfg(all(windows, target_arch = "x86_64", debug_assertions))]
mod cleanup_progress_schedule {
    use super::*;
    use std::ffi::{c_char, c_void, CString};
    use std::sync::atomic::{AtomicBool, AtomicU64};

    // 既有调试模块（debug module）的私有声明不可复用；这些本地ABI声明仅供本批追加测试（appended tests）。
    /// 对应Windows SDK的SYMBOL_INFO；名称（name）的有效尾部长度由DbgHelp提供。
    #[repr(C)]
    struct SymbolInfo {
        size_of_struct: u32, type_index: u32, reserved: [u64; 2], index: u32, size: u32,
        module_base: u64, flags: u32, value: u64, address: u64, register: u32,
        scope: u32, tag: u32, name_len: u32, max_name_len: u32, name: [u8; 1],
    }
    /// 对应Windows SDK的IMAGEHLP_LINE64，只取得本次链接产物（linked binary）的精确源码地址（source address）。
    #[repr(C)]
    struct SourceLine { size: u32, key: *mut c_void, number: u32, file: *mut c_char, address: u64 }
    /// 完整Windows x64 CONTEXT布局（layout）；只写debug、Dr6／Dr7和eflags的恢复标志（resume flag）。
    #[repr(C, align(16))]
    struct ThreadContext {
        homes: [u64; 6], flags: u32, mxcsr: u32, segments: [u16; 6], eflags: u32,
        debug: [u64; 6], integer: [u64; 16], rip: u64, remainder: [u8; 976],
    }
    /// Windows异常记录（exception record）只借用于核对单步异常码（single-step exception code）。
    #[repr(C)]
    struct ExceptionRecord {
        code: u32, flags: u32, nested: *mut ExceptionRecord, address: *mut c_void,
        parameter_count: u32, information: [usize; 15],
    }
    /// Windows异常指针（exception pointers）的借用（borrow）只在本处理器（handler）调用期间有效。
    #[repr(C)]
    struct ExceptionPointers { record: *mut ExceptionRecord, context: *mut ThreadContext }

    #[link(name = "dbghelp")]
    extern "system" {
        fn SymSetOptions(options: u32) -> u32;
        fn SymInitialize(process: *mut c_void, path: *const c_char, invade: i32) -> i32;
        fn SymCleanup(process: *mut c_void) -> i32;
        fn SymGetLineFromName64(process: *mut c_void, module: *const c_char, file: *const c_char,
            line: u32, displacement: *mut i32, result: *mut SourceLine) -> i32;
    }
    #[link(name = "kernel32")]
    extern "system" {
        fn GetCurrentProcess() -> *mut c_void;
        fn GetCurrentThreadId() -> u32;
        fn GetTickCount64() -> u64;
        fn Sleep(milliseconds: u32);
        fn OpenThread(access: u32, inherit: i32, id: u32) -> *mut c_void;
        fn SuspendThread(thread: *mut c_void) -> u32;
        fn ResumeThread(thread: *mut c_void) -> u32;
        fn GetThreadContext(thread: *mut c_void, context: *mut ThreadContext) -> i32;
        fn SetThreadContext(thread: *mut c_void, context: *const ThreadContext) -> i32;
        fn CloseHandle(handle: *mut c_void) -> i32;
        fn RemoveVectoredExceptionHandler(handle: *mut c_void) -> u32;
    }
    // Windows x64导入地址表（import address table）的真实函数指针（function pointers）；所需库（libraries）由上方声明链接。
    // 这里声明导入指针本身，不再次标记为动态库导入（DLL import），也不写入导入表（import table）。
    extern "system" {
        #[link_name = "__imp_SymEnumSymbols"]
        static ENUM_SYMBOLS: unsafe extern "system" fn(*mut c_void, u64, *const c_char,
            unsafe extern "system" fn(*mut SymbolInfo, u32, *mut c_void) -> i32, *mut c_void) -> i32;
        #[link_name = "__imp_AddVectoredExceptionHandler"]
        static ADD_EXCEPTION_HANDLER: unsafe extern "system" fn(u32,
            unsafe extern "system" fn(*mut ExceptionPointers) -> i32) -> *mut c_void;
    }

    // 线程局部槽（thread-local slot）只标识本例创建的线程（threads）；零身份（ID）表示尚未就绪或已经回收。
    thread_local! { static SLOT: Cell<usize> = const { Cell::new(usize::MAX) }; }
    static IDS: [AtomicUsize; 3] = [const { AtomicUsize::new(0) }; 3];
    static GO: [AtomicBool; 3] = [const { AtomicBool::new(false) }; 3];
    static EVENTS: [AtomicUsize; 3] = [const { AtomicUsize::new(0) }; 3];
    static SITES: [AtomicUsize; 3] = [const { AtomicUsize::new(0) }; 3];
    static PERMITS: [AtomicUsize; 3] = [const { AtomicUsize::new(0) }; 3];
    static PAUSES: [AtomicUsize; 3] = [const { AtomicUsize::new(0) }; 3];
    static PANIC_ONLY: [AtomicBool; 3] = [const { AtomicBool::new(false) }; 3];
    static CONSUMER_AFTER_REQUEST: [AtomicBool; 3] = [const { AtomicBool::new(false) }; 3];
    static HELPING: [AtomicBool; 3] = [const { AtomicBool::new(false) }; 3];
    // HITS只记实际指令（instructions）命中；每例的install调用明确各槽的观察位置（observation site）。
    static HITS: [[AtomicUsize; 3]; 3] = [const { [const { AtomicUsize::new(0) }; 3] }; 3];
    static STOP: AtomicBool = AtomicBool::new(false);
    static TIMEOUT: AtomicBool = AtomicBool::new(false);
    static WRONG_ADDRESS: AtomicBool = AtomicBool::new(false);
    static DEADLINE: AtomicU64 = AtomicU64::new(0);
    static DROP_STARTED: AtomicBool = AtomicBool::new(false);
    static DROP_RELEASED: AtomicBool = AtomicBool::new(false);

    /// 单调系统时钟（monotonic system clock），用于全例截止（deadline），不访问生产状态（production state）。
    fn now() -> u64 {
        // SAFETY: GetTickCount64不接收指针（pointers），不改变线程（thread）上下文（context）。
        unsafe { GetTickCount64() }
    }
    /// 控制线程（controller thread）或异常处理器（exception handler）的有限让出，不运行生产回调（production callbacks）。
    fn pause_tick() {
        // SAFETY: Sleep(1)仅暂停当前测试线程（test thread）；所有外层等待均检查同一截止（deadline）。
        unsafe { Sleep(1); }
    }

    /// 同步枚举回调（enumeration callback）只收集实际函数（functions），排除同名前缀闭包（closures）。
    unsafe extern "system" fn collect_symbol(info: *mut SymbolInfo, _: u32, context: *mut c_void) -> i32 {
        // SAFETY: DbgHelp在回调（callback）期间提供有效记录；context借用当前控制者的Vec，不被保存。
        let (info, addresses) = unsafe { (&*info, &mut *context.cast::<Vec<(u64, u32)>>()) };
        // SAFETY: NameLen是当前记录（record）中名称尾部（name tail）的有效字节数（byte count）。
        let bytes = unsafe { std::slice::from_raw_parts(info.name.as_ptr(), info.name_len as usize) };
        let name = std::str::from_utf8(bytes).unwrap_or("");
        if info.tag == 5 && !name.contains("closure") && !addresses.iter().any(|item| item.0 == info.address) {
            addresses.push((info.address, info.size));
        }
        1
    }

    /// 本地符号会话（symbol session）仅由控制者串行访问；异常处理器（exception handler）不调用DbgHelp。
    struct Symbols(*mut c_void);
    impl Symbols {
        /// 从当前测试产物（test binary）的相邻符号库（symbol database）加载，只保存当前进程（process）的借用句柄（borrowed handle）。
        fn new() -> Self {
            let executable = std::env::current_exe().unwrap();
            let path = CString::new(executable.parent().unwrap().to_string_lossy().as_bytes()).unwrap();
            // SAFETY: 当前进程（process）伪句柄（pseudo handle）及有效C字符串（C string）满足SDK签名（signature）。
            let process = unsafe { GetCurrentProcess() };
            unsafe { SymSetOptions(0x0000_0016 | 0x0000_0200); }
            assert_ne!(unsafe { SymInitialize(process, path.as_ptr(), 1) }, 0, "progress-debugger-setup: SymInitialize");
            Self(process)
        }
        /// name必须是实际生产函数名（production function name）；要求唯一函数（function）并返回地址（address）与范围（range）。
        fn function(&self, name: &str) -> (u64, u32) {
            let mask = CString::new(format!("*!*{name}*")).unwrap();
            let mut addresses = Vec::<(u64, u32)>::new();
            // SAFETY: Windows加载器（loader）已初始化原系统入口的导入指针（import pointer），回调ABI及记录布局（record layout）与SDK一致。
            // 同步回调（synchronous callback）的Vec借用（borrow）覆盖本次调用，无逃逸指针（escaping pointer）。
            assert_ne!(unsafe { ENUM_SYMBOLS(self.0, 0, mask.as_ptr(), collect_symbol,
                (&mut addresses as *mut Vec<(u64, u32)>).cast()) }, 0, "progress-debugger-setup: enumerate {name}");
            assert_eq!(addresses.len(), 1, "progress-debugger-setup: unique {name}: {addresses:?}");
            addresses[0]
        }
        /// function与needle在当前芯文件（inner file）内必须唯一匹配；只接受零偏移（zero displacement）的实际源码地址（source address）。
        fn line(&self, function: &str, needle: &str) -> u64 {
            let source = include_str!("../src/mpsc_inner.rs");
            let lines: Vec<_> = source.lines().collect();
            let first = lines.iter().position(|line| line.starts_with(&format!("fn {function}(")))
                .expect("progress-debugger-setup: function source");
            let last = lines[first + 1..].iter().position(|line| *line == "}")
                .map(|offset| first + 1 + offset).expect("progress-debugger-setup: function end");
            let matches: Vec<_> = (first..last).filter(|index| lines[*index].trim() == needle).collect();
            assert_eq!(matches.len(), 1, "progress-debugger-setup: unique source line {function}: {needle}");
            let number = matches[0] + 1;
            let file = CString::new(concat!(env!("CARGO_MANIFEST_DIR"), "/src/mpsc_inner.rs").replace('/', "\\")).unwrap();
            let mut result = SourceLine { size: std::mem::size_of::<SourceLine>() as u32,
                key: std::ptr::null_mut(), number: 0, file: std::ptr::null_mut(), address: 0 };
            let mut displacement = 0;
            // SAFETY: 文件名（filename）及输出缓冲区（output buffers）在同步调用（synchronous call）期间有效。
            assert_ne!(unsafe { SymGetLineFromName64(self.0, std::ptr::null(), file.as_ptr(), number as u32,
                &mut displacement, &mut result) }, 0, "progress-debugger-setup: source line {number}");
            let (start, size) = self.function(function);
            assert!(result.address >= start && result.address < start + u64::from(size), "progress-debugger-setup: function range");
            assert_eq!(result.number, number as u32, "progress-debugger-setup: exact line");
            assert_eq!(displacement, 0, "progress-debugger-setup: line displacement");
            result.address
        }
    }
    impl Drop for Symbols {
        fn drop(&mut self) {
            // SAFETY: 本控制者拥有该符号会话（symbol session），全部地址（addresses）已在工作线程（worker threads）结束前使用。
            unsafe { SymCleanup(self.0); }
        }
    }

    /// 硬件异常（hardware exception）只观察命中并暂停；恢复原指令（original instruction），不改生产数据（production data）或业务寄存器（business registers）。
    unsafe extern "system" fn on_breakpoint(pointers: *mut ExceptionPointers) -> i32 {
        // SAFETY: Windows在处理器（handler）调用期间借出有效异常记录（exception record）及上下文（context）。
        let (record, context) = unsafe { (&*(*pointers).record, &mut *(*pointers).context) };
        let slot = SLOT.try_with(Cell::get).unwrap_or(usize::MAX);
        if record.code != 0x8000_0004 || slot >= 3 || context.debug[4] & 7 == 0 { return 0; }
        let site = (context.debug[4] & 7).trailing_zeros() as usize;
        if context.rip != context.debug[site] {
            WRONG_ADDRESS.store(true, Ordering::Release);
            STOP.store(true, Ordering::Release);
        }
        if site == 0 { HELPING[slot].store(true, Ordering::Release); }
        let eligible = !(PANIC_ONLY[slot].load(Ordering::Acquire) && !std::thread::panicking())
            && !(site == 1 && CONSUMER_AFTER_REQUEST[slot].load(Ordering::Acquire)
                && !HELPING[slot].load(Ordering::Acquire));
        if !STOP.load(Ordering::Acquire) && eligible {
            HITS[slot][site].fetch_add(1, Ordering::Relaxed);
            if PAUSES[slot].load(Ordering::Acquire) & (1 << site) != 0 {
                SITES[slot].store(site + 1, Ordering::Relaxed);
                let event = EVENTS[slot].fetch_add(1, Ordering::Release) + 1;
                while PERMITS[slot].load(Ordering::Acquire) < event && !STOP.load(Ordering::Acquire) {
                    if now() >= DEADLINE.load(Ordering::Acquire) {
                        TIMEOUT.store(true, Ordering::Release);
                        STOP.store(true, Ordering::Release);
                        break;
                    }
                    pause_tick();
                }
            }
        }
        if STOP.load(Ordering::Acquire) { context.debug[5] = 0; }
        context.debug[4] = 0;
        context.eflags |= 0x0001_0000;
        -1
    }

    /// 本测试（test）建立的至多三个线程（threads）先报告身份（identity），待安装断点（breakpoints）后独立放行。
    fn enter(slot: usize) {
        SLOT.with(|value| value.set(slot));
        // SAFETY: GetCurrentThreadId无参数，只读当前线程（thread）身份（identity）。
        IDS[slot].store(unsafe { GetCurrentThreadId() } as usize, Ordering::Release);
        while !GO[slot].load(Ordering::Acquire) && !STOP.load(Ordering::Acquire) {
            assert!(now() < DEADLINE.load(Ordering::Acquire), "progress-debugger-control: worker start deadline");
            pause_tick();
        }
    }

    /// 仅安装三处以内执行地址（execution addresses）；保留完整业务执行上下文（business execution context）。
    fn install(slot: usize, addresses: [u64; 3]) {
        while IDS[slot].load(Ordering::Acquire) == 0 {
            assert!(now() < DEADLINE.load(Ordering::Acquire), "progress-debugger-control: worker ID deadline");
            pause_tick();
        }
        // SAFETY: 仅挂起本测试（test）创建并停在进入门（entry gate）的线程（thread）；仅设置CONTEXT_DEBUG_REGISTERS。
        unsafe {
            let thread = OpenThread(0x005a, 0, IDS[slot].load(Ordering::Acquire) as u32);
            assert!(!thread.is_null(), "progress-debugger-setup: OpenThread");
            if SuspendThread(thread) == u32::MAX { CloseHandle(thread); panic!("progress-debugger-setup: SuspendThread"); }
            let mut context: ThreadContext = std::mem::zeroed();
            context.flags = 0x0010_0010;
            let read = GetThreadContext(thread, &mut context);
            let mut enabled = 0;
            for (index, address) in addresses.iter().enumerate() { if *address != 0 { enabled |= 1 << (2 * index); } }
            context.debug = [addresses[0], addresses[1], addresses[2], 0, 0, enabled];
            let written = if read != 0 { SetThreadContext(thread, &context) } else { 0 };
            let resumed = ResumeThread(thread);
            CloseHandle(thread);
            assert_ne!(read, 0, "progress-debugger-setup: GetThreadContext");
            assert_ne!(written, 0, "progress-debugger-setup: SetThreadContext");
            assert_ne!(resumed, u32::MAX, "progress-debugger-setup: ResumeThread");
        }
    }

    /// 串行测试（serial test）内的调试处理器（debug handler）与有界控制状态（bounded control state）。
    struct Session(*mut c_void);
    impl Session {
        /// 在创建工作线程（worker threads）前重置测试原子量（test atomics）并安装处理器（handler），不访问生产状态（production state）。
        fn new() -> Self {
            assert_eq!(std::mem::size_of::<ThreadContext>(), 1232);
            assert_eq!(std::mem::offset_of!(ThreadContext, debug), 72);
            assert_eq!(std::mem::offset_of!(ThreadContext, eflags), 68);
            assert_eq!(std::mem::offset_of!(ThreadContext, rip), 248);
            STOP.store(false, Ordering::Release);
            TIMEOUT.store(false, Ordering::Release);
            WRONG_ADDRESS.store(false, Ordering::Release);
            DEADLINE.store(now() + 25_000, Ordering::Release);
            DROP_STARTED.store(false, Ordering::Release);
            DROP_RELEASED.store(false, Ordering::Release);
            for slot in 0..3 {
                IDS[slot].store(0, Ordering::Release);
                GO[slot].store(false, Ordering::Release);
                EVENTS[slot].store(0, Ordering::Release);
                SITES[slot].store(0, Ordering::Release);
                PERMITS[slot].store(0, Ordering::Release);
                PAUSES[slot].store(0, Ordering::Release);
                PANIC_ONLY[slot].store(false, Ordering::Release);
                CONSUMER_AFTER_REQUEST[slot].store(false, Ordering::Release);
                HELPING[slot].store(false, Ordering::Release);
                for hit in &HITS[slot] { hit.store(0, Ordering::Release); }
            }
            HELPER_ENTRY_STOP.store(false, Ordering::Release);
            HELPER_ENTRY_TIMEOUT.store(false, Ordering::Release);
            for slot in 0..2 {
                HELPER_ENTRY_STAGE[slot].store(0, Ordering::Release);
                HELPER_ENTRY_RELEASE[slot].store(0, Ordering::Release);
                HELPER_ENTRY_ROUNDS[slot].store(0, Ordering::Release);
            }
            // SAFETY: Windows加载器（loader）已初始化原入口的导入指针（import pointer），ABI与SDK一致；
            // 处理器（handler）在所有本测试线程（test threads）回收后移除。
            let handler = unsafe { ADD_EXCEPTION_HANDLER(1, on_breakpoint) };
            assert!(!handler.is_null(), "progress-debugger-setup: exception handler");
            Self(handler)
        }
    }
    impl Drop for Session {
        fn drop(&mut self) {
            STOP.store(true, Ordering::Release);
            DROP_RELEASED.store(true, Ordering::Release);
            HELPER_ENTRY_STOP.store(true, Ordering::Release);
            for gate in &GO { gate.store(true, Ordering::Release); }
            // SAFETY: Workers先于Session析构（drop）并回收本例全部线程（threads），不留在途断点（in-flight breakpoint）。
            unsafe { RemoveVectoredExceptionHandler(self.0); }
        }
    }

    /// 失败时只解除测试暂停（test pauses）并join原线程（threads），不消费或清理生产原值（production values）。
    struct Workers(Vec<(usize, std::thread::JoinHandle<()>)>);
    impl Workers {
        /// 本控制者（controller）独占线程句柄（thread handles）；析构（drop）先于Session结束。
        fn new() -> Self { Self(Vec::new()) }
        /// slot限定为本例唯一的0至2；work持有真实公开操作（public operation）的原始输入（original input）。
        fn spawn(&mut self, slot: usize, work: impl FnOnce() + Send + 'static) {
            self.0.push((slot, std::thread::spawn(move || { enter(slot); work(); })));
        }
        /// slot必须仍由本控制者（controller）持有；只查询线程（thread）结束状态（status），不执行生产操作（production operation）。
        fn finished(&self, slot: usize) -> bool { self.0.iter().find(|item| item.0 == slot).unwrap().1.is_finished() }
        /// 只等待同一截止（deadline）内的原公开调用（public call）结束；不重复调用或接管清理（cleanup）。
        fn wait_finished(&self, slot: usize) {
            while !self.finished(slot) {
                assert!(now() < DEADLINE.load(Ordering::Acquire), "progress-debugger-control: public call deadline slot={slot}");
                pause_tick();
            }
        }
        /// 在wait_finished之后回收一次指定线程（thread），原恐慌载荷（panic payload）完整交回测试断言（test assertion）。
        fn join(&mut self, slot: usize) -> std::thread::Result<()> {
            let index = self.0.iter().position(|item| item.0 == slot).unwrap();
            let result = self.0.remove(index).1.join();
            IDS[slot].store(0, Ordering::Release);
            result
        }
    }
    impl Drop for Workers {
        fn drop(&mut self) {
            STOP.store(true, Ordering::Release);
            DROP_RELEASED.store(true, Ordering::Release);
            HELPER_ENTRY_STOP.store(true, Ordering::Release);
            for gate in &GO { gate.store(true, Ordering::Release); }
            let cleanup_deadline = now() + 2_000;
            while self.0.iter().any(|item| !item.1.is_finished()) && now() < cleanup_deadline { pause_tick(); }
            // 不让失败实验（failed experiment）的线程（threads）污染后续测试（tests），超过本例回收上限则终止本测试进程（test process）。
            if self.0.iter().any(|item| !item.1.is_finished()) { std::process::abort(); }
            for (slot, worker) in self.0.drain(..) { drop(worker.join()); IDS[slot].store(0, Ordering::Release); }
        }
    }

    /// 只观察真实断点（breakpoint）或公开调用（public call）退出；未命中归类为控制失败（control failure），不是业务RED。
    fn event(workers: &Workers, slot: usize, sequence: usize) -> Option<usize> {
        while EVENTS[slot].load(Ordering::Acquire) < sequence {
            if workers.finished(slot) { return None; }
            assert!(now() < DEADLINE.load(Ordering::Acquire), "progress-debugger-control: event deadline slot={slot}");
            pause_tick();
        }
        assert_eq!(EVENTS[slot].load(Ordering::Acquire), sequence, "progress-debugger-control: event sequence");
        Some(SITES[slot].load(Ordering::Acquire))
    }
    /// 放行已观察到的一次真实指令（real instruction），不推进任何生产状态（production state）。
    fn release(slot: usize) { PERMITS[slot].store(EVENTS[slot].load(Ordering::Acquire), Ordering::Release); }
    /// 停止本线程（thread）后续调试暂停（debug pauses），保持生产代码（production code）自行退出。
    fn run_free(slot: usize) { PAUSES[slot].store(0, Ordering::Release); release(slot); }
    /// 观察公开批次入口（public batch entry）的已存在分配暂停（allocation pause），不重复调用被测接口（API）。
    fn batch_entered() {
        while HELPER_ENTRY_STAGE[1].load(Ordering::Acquire) < 1 {
            assert!(now() < DEADLINE.load(Ordering::Acquire), "progress-debugger-control: ordinary batch entry deadline");
            pause_tick();
        }
        assert!(!HELPER_ENTRY_TIMEOUT.load(Ordering::Acquire), "progress-debugger-control: batch gate timeout");
    }
    /// 核对调试装配（debug setup）未超时或偏离地址（address），避免将装配错误当业务失败（target failure）。
    fn instrumentation_ok() {
        assert!(!TIMEOUT.load(Ordering::Acquire), "progress-debugger-control: breakpoint timeout");
        assert!(!WRONG_ADDRESS.load(Ordering::Acquire), "progress-debugger-control: breakpoint address mismatch");
        assert!(!HELPER_ENTRY_TIMEOUT.load(Ordering::Acquire), "progress-debugger-control: allocator timeout");
    }

    /// 真实Future::drop只等待测试释放（test release）；此时原Runnable仍由生产清理守卫（cleanup guard）负责记账。
    struct PausedDrop(Arc<AtomicUsize>);
    impl std::future::Future for PausedDrop {
        type Output = ();
        fn poll(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> std::task::Poll<()> {
            panic!("来源清理（source cleanup）不得轮询（poll）");
        }
    }
    impl Drop for PausedDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            DROP_STARTED.store(true, Ordering::Release);
            while !DROP_RELEASED.load(Ordering::Acquire) && !STOP.load(Ordering::Acquire) {
                if now() >= DEADLINE.load(Ordering::Acquire) {
                    TIMEOUT.store(true, Ordering::Release);
                    STOP.store(true, Ordering::Release);
                    break;
                }
                pause_tick();
            }
        }
    }

    /// @api clear／MpscTaskQueue::Drop／try_take_batch；Reached与权限外欠账（outside count debt）并存时，真实减数须通知当前Consumer。
    /// 两工作线程（worker threads）；旧实现会在所有原值已处理后仍无人确认Reached，断言（assertion）针对真实确认指令（confirmation instruction）。
    #[test]
    fn test_outside_cleanup_notifies_current_consumer_after_release() {
        let symbols = Symbols::new();
        let request = symbols.function("mpsc_request_cleanup_inner").0;
        let before_release = symbols.line("mpsc_cleanup_step_inner", "let processed = !cleanup.pending.is_empty();");
        let confirmed_endpoint = symbols.line("mpsc_consumer_acknowledge_inner", "clear.write(MpscClearState::Available(endpoint));");
        let _session = Session::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let mut options = MpscQueueOptions::default();
        options.cleanup_batch_size = std::num::NonZeroUsize::new(1).unwrap();
        let mut queue = MpscTaskQueue::new(options).unwrap();
        let handle = queue.handle();
        let first = create(CountDrop(drops.clone()), NoSchedule, (), TaskOptions::intrusive_mpsc())
            .unwrap_or_else(|_| panic!("创建（creation）失败"));
        handle.try_push(first.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
        assert!(matches!(queue.clear().unwrap(), pi_async_task_queue::QueueCleanupProgress::MoreWork));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(handle.len(), 0);
        // 公开clear留下原终点（endpoint）；后来业务原值（business value）排在它之后，OwnerExit取回后保留Reached。
        let later = create(PausedDrop(drops.clone()), NoSchedule, (), TaskOptions::intrusive_mpsc())
            .unwrap_or_else(|_| panic!("创建（creation）失败"));
        handle.try_push(later.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
        let mut workers = Workers::new();
        let ordinary = handle.clone();
        workers.spawn(1, move || {
            HELPER_ENTRY_SLOT.with(|value| value.set(1));
            let result = ordinary.try_take_batch();
            HELPER_ENTRY_SLOT.with(|value| value.set(usize::MAX));
            assert!(matches!(result, pi_async_task_queue::BatchAttempt::WouldBlock));
        });
        workers.spawn(0, move || drop(queue));
        PAUSES[1].store(2, Ordering::Release);
        install(0, [request, 0, 0]);
        install(1, [request, before_release, confirmed_endpoint]);
        GO[1].store(true, Ordering::Release);
        batch_entered();
        GO[0].store(true, Ordering::Release);
        while !DROP_STARTED.load(Ordering::Acquire) {
            assert!(now() < DEADLINE.load(Ordering::Acquire), "progress-debugger-control: outside drop entry");
            pause_tick();
        }
        assert_eq!(handle.len(), 1);
        // 普通获取（ordinary take）先看到权限外欠账（outside debt），其确认守卫（completion guard）随后进入OwnerExit。
        HELPER_ENTRY_RELEASE[1].store(2, Ordering::Release);
        assert_eq!(event(&workers, 1, 1), Some(2), "progress-debugger-control: helper holds Consumer after Empty sample");
        assert_eq!(HITS[1][2].load(Ordering::Acquire), 0);
        DROP_RELEASED.store(true, Ordering::Release);
        workers.wait_finished(0);
        workers.join(0).unwrap();
        assert_eq!(handle.len(), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        // 管理者（manager）已在真实Busy路径退出；当前Consumer必须接到减数后的进展通知（progress notification）。
        run_free(1);
        workers.wait_finished(1);
        workers.join(1).unwrap();
        instrumentation_ok();
        let confirmations = HITS[1][2].load(Ordering::Acquire);
        let manager_requests = HITS[0][0].load(Ordering::Acquire);
        println!("cleanup-progress outside confirmations={confirmations} manager_requests={manager_requests} len={} drops={}",
            handle.len(), drops.load(Ordering::SeqCst));
        assert_eq!(handle.len(), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        assert_eq!(confirmations, 1, "cleanup-progress-handoff: 权限外减数（outside decrement）未通知实际Consumer，Reached无人确认");
    }

    /// @api try_push／MpscTaskQueue::Drop／try_take_batch；真实发布者（publisher）在登记后暂停，首次原恐慌（panic）后的纯请求重查不得再造请求。
    /// 三工作线程（worker threads）各至多两个／三个断点（breakpoints）；先让帮助者（helpers）退出，再放行原发布者（publisher）完成余项。
    #[test]
    fn test_paused_publisher_does_not_keep_unwind_helper_running() {
        let symbols = Symbols::new();
        let request = symbols.function("mpsc_request_cleanup_inner").0;
        let acquire = symbols.function("mpsc_consumer_new_inner").0;
        let fallback_sample = symbols.line("mpsc_cleanup_drop_inner", "let state = this.shared.state.fetch_or(0, AcqRel);");
        let before_publish = symbols.line("mpsc_push_inner", "this.shared.queue.enqueue(entry);");
        let _session = Session::new();
        PANIC_ONLY[0].store(true, Ordering::Release);
        CONSUMER_AFTER_REQUEST[1].store(true, Ordering::Release);
        PAUSES[0].store(3, Ordering::Release);
        PAUSES[1].store(3, Ordering::Release);
        PAUSES[2].store(4, Ordering::Release);
        let panic_drops = Arc::new(AtomicUsize::new(0));
        let late_drops = Arc::new(AtomicUsize::new(0));
        let queue = MpscTaskQueue::new(MpscQueueOptions::default()).unwrap();
        let handle = queue.handle();
        let first = create(PanicDrop { drops: panic_drops.clone(), panic: true }, NoSchedule, (), TaskOptions::intrusive_mpsc())
            .unwrap_or_else(|_| panic!("创建（creation）失败"));
        handle.try_push(first.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
        let later = create(CountDrop(late_drops.clone()), NoSchedule, (), TaskOptions::intrusive_mpsc())
            .unwrap_or_else(|_| panic!("创建（creation）失败"));
        let mut workers = Workers::new();
        let ordinary = handle.clone();
        workers.spawn(1, move || {
            HELPER_ENTRY_SLOT.with(|value| value.set(1));
            let result = ordinary.try_take_batch();
            HELPER_ENTRY_SLOT.with(|value| value.set(usize::MAX));
            assert!(matches!(result, pi_async_task_queue::BatchAttempt::WouldBlock));
        });
        let publisher = handle.clone();
        workers.spawn(2, move || publisher.try_push(later.runnable).unwrap_or_else(|_| panic!("已登记原值（reserved original）不得拒绝")));
        workers.spawn(0, move || drop(queue));
        install(0, [request, fallback_sample, 0]);
        install(1, [request, acquire, 0]);
        install(2, [0, 0, before_publish]);
        GO[1].store(true, Ordering::Release);
        batch_entered();
        GO[2].store(true, Ordering::Release);
        assert_eq!(event(&workers, 2, 1), Some(3), "progress-debugger-control: reserved publisher before enqueue");
        assert_eq!(handle.len(), 2);
        GO[0].store(true, Ordering::Release);
        assert_eq!(event(&workers, 0, 1), Some(1), "progress-debugger-control: first unwind fallback request");
        assert_eq!(panic_drops.load(Ordering::SeqCst), 1);
        assert_eq!(handle.len(), 1);
        release(0);
        assert_eq!(event(&workers, 0, 2), Some(2), "progress-debugger-control: fallback Empty after Consumer release");
        HELPER_ENTRY_RELEASE[1].store(2, Ordering::Release);
        assert_eq!(event(&workers, 1, 1), Some(1), "progress-debugger-control: independent helper request");
        release(1);
        assert_eq!(event(&workers, 1, 2), Some(2), "progress-debugger-control: request registered before Consumer attempt");
        // 唯一新事实来自另一独立入口（independent entry）；原发布者（publisher）仍暂停，没有业务或终点推进（endpoint progress）。
        release(0);
        let recheck_site = event(&workers, 0, 3).expect("progress-debugger-control: pure-request recheck");
        assert!(recheck_site == 1 || recheck_site == 2, "progress-debugger-control: recheck site");
        run_free(0);
        workers.wait_finished(0);
        let error = workers.join(0).expect_err("首次原析构恐慌（original drop panic）必须传播");
        assert_eq!(error.downcast_ref::<&str>().copied(), Some("first-future-drop"));
        run_free(1);
        workers.wait_finished(1);
        workers.join(1).unwrap();
        assert!(!workers.finished(2), "progress-debugger-control: publisher must remain paused until helpers finish");
        assert_eq!(handle.len(), 1);
        assert_eq!(late_drops.load(Ordering::SeqCst), 0);
        let fallback_requests = HITS[0][0].load(Ordering::Acquire);
        // 只解除发布者（publisher）的执行暂停；其原try_push和生产退出确认（production exit confirmation）负责发布、尾部及减数。
        run_free(2);
        workers.wait_finished(2);
        workers.join(2).unwrap();
        instrumentation_ok();
        println!("cleanup-progress unwind fallback_requests={fallback_requests} recheck_site={recheck_site} len={} panic_drops={} late_drops={}",
            handle.len(), panic_drops.load(Ordering::SeqCst), late_drops.load(Ordering::SeqCst));
        assert_eq!(handle.len(), 0);
        assert_eq!(panic_drops.load(Ordering::SeqCst), 1);
        assert_eq!(late_drops.load(Ordering::SeqCst), 1);
        assert_eq!(fallback_requests, 1, "cleanup-progress-unwind: 无真实推进的重查（recheck without progress）再次制造请求（request）");
    }

    /// @api try_push／MpscTaskQueue::Drop／try_take_batch；真实发布者（publisher）暂停期间，两个正常帮助者（normal helpers）必须结束。
    /// 三工作线程（worker threads），每线程至多两个断点（breakpoints）；固定归还Consumer后的一次纯请求重查（request-only recheck）。
    /// 此窗口没有业务或终点进展（endpoint progress），所以各自只有独立入口（independent entry）的一次请求（request）；不泛化该次数。
    #[test]
    fn test_paused_publisher_does_not_keep_normal_helpers_running() {
        let symbols = Symbols::new();
        let request = symbols.function("mpsc_request_cleanup_inner").0;
        let acquire = symbols.function("mpsc_consumer_new_inner").0;
        let after_release = symbols.line("mpsc_cleanup_step_inner", "let state = this.state.fetch_or(0, AcqRel);");
        let before_publish = symbols.line("mpsc_push_inner", "this.shared.queue.enqueue(entry);");
        let _session = Session::new();
        CONSUMER_AFTER_REQUEST[1].store(true, Ordering::Release);
        PAUSES[0].store(3, Ordering::Release);
        PAUSES[1].store(3, Ordering::Release);
        PAUSES[2].store(4, Ordering::Release);
        let drops = Arc::new(AtomicUsize::new(0));
        let queue = MpscTaskQueue::new(MpscQueueOptions::default()).unwrap();
        let handle = queue.handle();
        let task = create(CountDrop(drops.clone()), NoSchedule, (), TaskOptions::intrusive_mpsc())
            .unwrap_or_else(|_| panic!("创建（creation）失败"));
        let mut workers = Workers::new();
        let publisher = handle.clone();
        workers.spawn(2, move || publisher.try_push(task.runnable)
            .unwrap_or_else(|_| panic!("已登记原值（reserved original）不得拒绝")));
        let ordinary = handle.clone();
        workers.spawn(1, move || {
            HELPER_ENTRY_SLOT.with(|value| value.set(1));
            let result = ordinary.try_take_batch();
            HELPER_ENTRY_SLOT.with(|value| value.set(usize::MAX));
            assert!(matches!(result, pi_async_task_queue::BatchAttempt::WouldBlock));
        });
        workers.spawn(0, move || drop(queue));
        install(0, [request, after_release, 0]);
        install(1, [request, acquire, 0]);
        install(2, [0, 0, before_publish]);
        // 先建立真实登记（reservation），普通批次入口（batch entry）才会准备空间；不注入私有数量（private count）。
        GO[2].store(true, Ordering::Release);
        assert_eq!(event(&workers, 2, 1), Some(3), "progress-debugger-control: normal publisher before enqueue");
        assert_eq!(handle.len(), 1);
        GO[1].store(true, Ordering::Release);
        batch_entered();
        GO[0].store(true, Ordering::Release);
        assert_eq!(event(&workers, 0, 1), Some(1), "progress-debugger-control: normal manager initial request");
        release(0);
        assert_eq!(event(&workers, 0, 2), Some(2), "progress-debugger-control: normal Empty after Consumer release");
        HELPER_ENTRY_RELEASE[1].store(2, Ordering::Release);
        assert_eq!(event(&workers, 1, 1), Some(1), "progress-debugger-control: independent normal helper request");
        release(1);
        assert_eq!(event(&workers, 1, 2), Some(2), "progress-debugger-control: normal helper before Consumer attempt");
        // 第二份请求（request）已真实登记，发布者（publisher）仍暂停；重查本身不能成为新通知（notification）。
        release(0);
        let recheck_site = event(&workers, 0, 3).expect("progress-debugger-control: normal pure-request recheck");
        assert_eq!(recheck_site, 2, "cleanup-progress-normal: 无真实推进的重查（recheck without progress）不应再次登记请求（request）");
        run_free(0);
        workers.wait_finished(0);
        workers.join(0).unwrap();
        run_free(1);
        workers.wait_finished(1);
        workers.join(1).unwrap();
        assert!(!workers.finished(2), "progress-debugger-control: normal helpers must finish before publisher release");
        assert_eq!(handle.len(), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        let requests = [HITS[0][0].load(Ordering::Acquire), HITS[1][0].load(Ordering::Acquire)];
        // 只恢复同一公开提交（public submission）；其生产退出确认（production exit confirmation）负责余项。
        run_free(2);
        workers.wait_finished(2);
        workers.join(2).unwrap();
        instrumentation_ok();
        println!("cleanup-progress normal requests={requests:?} recheck_site={recheck_site} len={} drops={}",
            handle.len(), drops.load(Ordering::SeqCst));
        assert_eq!(handle.len(), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(requests, [1, 1], "cleanup-progress-normal: 暂停发布（paused publication）且无真实推进时续造请求（request）");
    }


    /// @api clear／MpscTaskQueue::Drop／try_take_batch；首次原恐慌（original panic）后，T32栈上清理（stack cleanup）的Release减数须通知当前Consumer。
    /// 两工作线程（worker threads），各两个／三个断点（breakpoints）；额度（budget）1保证后项由备援（fallback）从原生队列（native queue）取得。
    /// 同时观察原恐慌载荷（panic payload）、一次记账（accounting）和Reached真实确认；来源完成不推广为任意Future的所属线程析构（owner-thread drop）。
    #[test]
    fn test_unwind_cleanup_notifies_current_consumer_after_release() {
        let symbols = Symbols::new();
        let request = symbols.function("mpsc_request_cleanup_inner").0;
        let acquire = symbols.function("mpsc_consumer_new_inner").0;
        let before_release = symbols.line("mpsc_cleanup_step_inner", "let processed = !cleanup.pending.is_empty();");
        let confirmed_endpoint = symbols.line("mpsc_consumer_acknowledge_inner", "clear.write(MpscClearState::Available(endpoint));");
        let _session = Session::new();
        PANIC_ONLY[0].store(true, Ordering::Release);
        PAUSES[1].store(2, Ordering::Release);
        let prefix_drops = Arc::new(AtomicUsize::new(0));
        let panic_drops = Arc::new(AtomicUsize::new(0));
        let late_drops = Arc::new(AtomicUsize::new(0));
        let mut options = MpscQueueOptions::default();
        options.cleanup_batch_size = std::num::NonZeroUsize::new(1).unwrap();
        let mut queue = MpscTaskQueue::new(options).unwrap();
        let handle = queue.handle();
        let prefix = create(CountDrop(prefix_drops.clone()), NoSchedule, (), TaskOptions::intrusive_mpsc())
            .unwrap_or_else(|_| panic!("创建（creation）失败"));
        handle.try_push(prefix.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
        assert!(matches!(queue.clear().unwrap(), pi_async_task_queue::QueueCleanupProgress::MoreWork));
        assert_eq!(prefix_drops.load(Ordering::SeqCst), 1);
        assert_eq!(handle.len(), 0);
        let first = create(PanicDrop { drops: panic_drops.clone(), panic: true }, NoSchedule, (), TaskOptions::intrusive_mpsc())
            .unwrap_or_else(|_| panic!("创建（creation）失败"));
        handle.try_push(first.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
        let later = create(PausedDrop(late_drops.clone()), NoSchedule, (), TaskOptions::intrusive_mpsc())
            .unwrap_or_else(|_| panic!("创建（creation）失败"));
        assert_eq!(handle.len(), 1);
        let mut workers = Workers::new();
        let ordinary = handle.clone();
        workers.spawn(1, move || {
            HELPER_ENTRY_SLOT.with(|value| value.set(1));
            let result = ordinary.try_take_batch();
            HELPER_ENTRY_SLOT.with(|value| value.set(usize::MAX));
            assert!(matches!(result, pi_async_task_queue::BatchAttempt::WouldBlock));
        });
        workers.spawn(0, move || drop(queue));
        install(0, [request, acquire, 0]);
        install(1, [request, before_release, confirmed_endpoint]);
        GO[1].store(true, Ordering::Release);
        batch_entered();
        // 既有分配门（allocation gate）只暂停一份Runnable的空间请求（allocation request）；先固定普通获取上界（take bound），再公开提交后项。
        handle.try_push(later.runnable).unwrap_or_else(|_| panic!("提交（submission）失败"));
        assert_eq!(handle.len(), 2);
        GO[0].store(true, Ordering::Release);
        while !DROP_STARTED.load(Ordering::Acquire) {
            assert!(now() < DEADLINE.load(Ordering::Acquire), "progress-debugger-control: unwind outside drop entry");
            pause_tick();
        }
        // 原恐慌（original panic）已经发生；其后首次取得Consumer，证明后项不是先前pending中的原值。
        assert_eq!(panic_drops.load(Ordering::SeqCst), 1);
        assert_eq!(late_drops.load(Ordering::SeqCst), 1);
        assert_eq!(handle.len(), 1);
        assert_eq!(HITS[0][0].load(Ordering::Acquire), 1, "progress-debugger-control: first fallback request before outside drop");
        assert_eq!(HITS[0][1].load(Ordering::Acquire), 1, "progress-debugger-control: fallback Consumer before outside drop");
        HELPER_ENTRY_RELEASE[1].store(2, Ordering::Release);
        assert_eq!(event(&workers, 1, 1), Some(2), "progress-debugger-control: current Consumer holds Empty and Reached during unwind cleanup");
        assert_eq!(HITS[1][2].load(Ordering::Acquire), 0);
        DROP_RELEASED.store(true, Ordering::Release);
        workers.wait_finished(0);
        let error = workers.join(0).expect_err("首次原析构恐慌（original drop panic）必须传播");
        assert_eq!(error.downcast_ref::<&str>().copied(), Some("first-future-drop"));
        assert_eq!(handle.len(), 0);
        let fallback_requests = HITS[0][0].load(Ordering::Acquire);
        let fallback_attempts = HITS[0][1].load(Ordering::Acquire);
        // 当前Consumer仍持权限；第二次真实尝试（real attempt）遇Busy，备援（fallback）已经在归还后交接。
        assert_eq!(fallback_attempts, 2, "cleanup-progress-unwind-handoff: 备援（fallback）须在真实推进后尝试交接（handoff）");
        assert_eq!(fallback_requests, 2, "cleanup-progress-unwind-handoff: 首次入口（first entry）及真实减数（real decrement）各通知一次");
        run_free(1);
        workers.wait_finished(1);
        workers.join(1).unwrap();
        instrumentation_ok();
        let confirmations = HITS[1][2].load(Ordering::Acquire);
        println!("cleanup-progress unwind-outside confirmations={confirmations} fallback_requests={fallback_requests} fallback_attempts={fallback_attempts} len={} prefix_drops={} panic_drops={} late_drops={}",
            handle.len(), prefix_drops.load(Ordering::SeqCst), panic_drops.load(Ordering::SeqCst), late_drops.load(Ordering::SeqCst));
        assert_eq!(handle.len(), 0);
        assert_eq!(prefix_drops.load(Ordering::SeqCst), 1);
        assert_eq!(panic_drops.load(Ordering::SeqCst), 1);
        assert_eq!(late_drops.load(Ordering::SeqCst), 1);
        assert_eq!(confirmations, 1, "cleanup-progress-unwind-handoff: 备援减数（fallback decrement）后Reached必须由当前Consumer自行确认");
    }


    /// @api try_push／MpscTaskQueue::Drop／try_take_batch；最后发布者（last publisher）完成发布后遇Busy，由当前Consumer接续。
    /// 三工作线程（worker threads），分别一个／两个／三个断点（breakpoints）；发布前Empty已被保存，随后在Consumer仍持有时完成原提交（original submission）。
    /// 只恢复执行（execution），不写私有状态（private state）；发布者（publisher）结束且当前Consumer尚未归还时原值仍由来源保管。
    #[test]
    fn test_last_publisher_hands_busy_to_current_consumer() {
        let symbols = Symbols::new();
        let request = symbols.function("mpsc_request_cleanup_inner").0;
        let acquire = symbols.function("mpsc_consumer_new_inner").0;
        let before_release = symbols.line("mpsc_cleanup_step_inner", "let processed = !cleanup.pending.is_empty();");
        let before_publish = symbols.line("mpsc_push_inner", "this.shared.queue.enqueue(entry);");
        let _session = Session::new();
        CONSUMER_AFTER_REQUEST[2].store(true, Ordering::Release);
        PAUSES[1].store(2, Ordering::Release);
        PAUSES[2].store(4, Ordering::Release);
        let drops = Arc::new(AtomicUsize::new(0));
        let queue = MpscTaskQueue::new(MpscQueueOptions::default()).unwrap();
        let handle = queue.handle();
        let task = create(CountDrop(drops.clone()), NoSchedule, (), TaskOptions::intrusive_mpsc())
            .unwrap_or_else(|_| panic!("创建（creation）失败"));
        let mut workers = Workers::new();
        let publisher = handle.clone();
        workers.spawn(2, move || publisher.try_push(task.runnable)
            .unwrap_or_else(|_| panic!("已登记原值（reserved original）不得拒绝")));
        let ordinary = handle.clone();
        workers.spawn(1, move || {
            HELPER_ENTRY_SLOT.with(|value| value.set(1));
            let result = ordinary.try_take_batch();
            HELPER_ENTRY_SLOT.with(|value| value.set(usize::MAX));
            assert!(matches!(result, pi_async_task_queue::BatchAttempt::WouldBlock));
        });
        workers.spawn(0, move || drop(queue));
        install(0, [request, 0, 0]);
        install(1, [request, before_release, 0]);
        install(2, [request, acquire, before_publish]);
        GO[2].store(true, Ordering::Release);
        assert_eq!(event(&workers, 2, 1), Some(3), "progress-debugger-control: last publisher before enqueue");
        assert_eq!(handle.len(), 1);
        GO[1].store(true, Ordering::Release);
        batch_entered();
        GO[0].store(true, Ordering::Release);
        workers.wait_finished(0);
        workers.join(0).unwrap();
        assert_eq!(handle.len(), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        HELPER_ENTRY_RELEASE[1].store(2, Ordering::Release);
        assert_eq!(event(&workers, 1, 1), Some(2), "progress-debugger-control: current Consumer saved Empty before publication");
        // 当前帮助者（helper）仍持Consumer；同一发布者（publisher）完成enqueue及公开调用（public call），只可能将Busy交给它。
        run_free(2);
        workers.wait_finished(2);
        workers.join(2).unwrap();
        assert!(!workers.finished(1), "progress-debugger-control: current Consumer remains paused");
        assert_eq!(handle.len(), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        let publisher_requests = HITS[2][0].load(Ordering::Acquire);
        let publisher_attempts = HITS[2][1].load(Ordering::Acquire);
        assert_eq!(publisher_requests, 1, "cleanup-progress-last-publisher: 最后发布完成（last publication）须登记一次请求（request）");
        assert_eq!(publisher_attempts, 1, "cleanup-progress-last-publisher: 须真实尝试Consumer并交接Busy");
        // 原Empty样本（sample）尚未包含刚发布原值；生产退出确认（production exit confirmation）必须促使当前帮助者（helper）重查。
        run_free(1);
        workers.wait_finished(1);
        workers.join(1).unwrap();
        instrumentation_ok();
        println!("cleanup-progress last-publisher publisher_requests={publisher_requests} publisher_attempts={publisher_attempts} len={} drops={}",
            handle.len(), drops.load(Ordering::SeqCst));
        assert_eq!(handle.len(), 0, "cleanup-progress-last-publisher: 当前Consumer不得沿旧Empty退出而遗留原值");
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

}
