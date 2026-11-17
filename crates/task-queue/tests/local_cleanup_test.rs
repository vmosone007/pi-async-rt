//! 经公开本地清理入口（local cleanup API）观察原持有的合法移交及所属线程（owner thread）清理。
use pi_async_task::{create_local, Runnable, Schedule, TaskOptions, TaskThread};
use pi_async_task_queue::{QueueCleanupProgress, QueuePushErrorKind, TaskBatch, TaskQueue};
use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

thread_local! {
    static DELIVERED: RefCell<Vec<Runnable>> = const { RefCell::new(Vec::new()) };
}

/// 保存原任务（Task）机制交来的清理持有；不代补被测队列（queue）的移除或释放责任。
struct Delivery;
impl Schedule for Delivery {
    fn schedule(&self, task: Runnable) {
        DELIVERED.with(|tasks| tasks.borrow_mut().push(task));
    }
}

/// 非Send原异步计算（Future）只在所属线程（owner thread）记录一次析构（drop），禁止轮询（poll）。
struct LocalFuture(Rc<std::cell::Cell<usize>>);
impl Future for LocalFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        panic!("清理入口（cleanup API）不得轮询（poll）");
    }
}
impl Drop for LocalFuture {
    fn drop(&mut self) { self.0.set(self.0.get() + 1); }
}

/// @api TaskQueue::clear；非空／重复／空清理保持接收，原身份及非Send原值的所属线程（owner thread）不变。
#[test]
fn test_clear_transfers_each_original_once_and_keeps_accepting() {
    let owner = TaskThread::new();
    let drops = Rc::new(std::cell::Cell::new(0));
    let mut queue = TaskQueue::new(&owner);
    let mut controls = Vec::new();
    let mut joins = Vec::new();
    let mut ids = Vec::new();
    for _ in 0..3 {
        let parts = create_local(&owner, LocalFuture(drops.clone()), Delivery, (), TaskOptions::intrusive())
            .unwrap_or_else(|_| panic!("创建（creation）失败"));
        ids.push(parts.runnable.id());
        queue.push_back(parts.runnable).unwrap_or_else(|_| panic!("入队（enqueue）失败"));
        controls.push(parts.control);
        joins.push(parts.join);
    }
    assert_eq!(queue.len(), 3);
    assert!(matches!(queue.clear().unwrap(), QueueCleanupProgress::Complete));
    assert_eq!(queue.len(), 0);
    assert_eq!(drops.get(), 0);
    let delivered = DELIVERED.with(|tasks| tasks.replace(Vec::new()));
    assert_eq!(delivered.iter().map(Runnable::id).collect::<Vec<_>>(), ids);
    assert!(matches!(queue.clear().unwrap(), QueueCleanupProgress::Complete));
    assert!(DELIVERED.with(|tasks| tasks.borrow().is_empty()));
    let next = create_local(&owner, LocalFuture(drops.clone()), Delivery, (), TaskOptions::intrusive())
        .unwrap_or_else(|_| panic!("创建（creation）失败"));
    queue.push_front(next.runnable).unwrap_or_else(|_| panic!("清理后应接收（accept）"));
    assert!(matches!(queue.clear().unwrap(), QueueCleanupProgress::Complete));
    let next_delivery = DELIVERED.with(|tasks| tasks.replace(Vec::new()));
    assert_eq!(next_delivery.len(), 1);
    owner.close();
    drop((delivered, next_delivery, controls, joins, next.control, next.join));
    assert_eq!(drops.get(), 4);
}

/// @api TaskQueue::close/clear/push_front/push_back/append_batch；关闭后拒绝保留原身份及批次（batch）顺序。
#[test]
fn test_close_rejects_every_insertion_and_remains_closed_after_clear() {
    let owner = TaskThread::new();
    let drops = Rc::new(std::cell::Cell::new(0));
    let mut queue = TaskQueue::new(&owner);
    let first = create_local(&owner, LocalFuture(drops.clone()), Delivery, (), TaskOptions::intrusive())
        .unwrap_or_else(|_| panic!("创建（creation）失败"));
    queue.push_back(first.runnable).unwrap_or_else(|_| panic!("入队（enqueue）失败"));
    assert!(matches!(queue.close().unwrap(), QueueCleanupProgress::Complete));
    assert_eq!(queue.len(), 0);
    assert!(matches!(queue.close().unwrap(), QueueCleanupProgress::Complete));
    assert!(matches!(queue.clear().unwrap(), QueueCleanupProgress::Complete));
    let next = create_local(&owner, LocalFuture(drops.clone()), Delivery, (), TaskOptions::intrusive())
        .unwrap_or_else(|_| panic!("创建（creation）失败"));
    let id = next.runnable.id();
    let error = queue.push_front(next.runnable).err().expect("关闭后必须拒绝（reject）");
    assert_eq!(error.cause, QueuePushErrorKind::Closed);
    assert_eq!(error.task.id(), id);
    let error = queue.push_back(error.task).err().expect("关闭后必须拒绝（reject）");
    assert_eq!(error.cause, QueuePushErrorKind::Closed);
    assert_eq!(error.task.id(), id);
    let error = queue.append_batch(TaskBatch::from_tasks(std::collections::VecDeque::from([error.task])))
        .err().expect("关闭后必须拒绝（reject）");
    assert_eq!(error.cause, QueuePushErrorKind::Closed);
    assert_eq!(error.batch.len(), 1);
    assert_eq!(error.batch.into_iter().next().unwrap().id(), id);
    assert!(queue.pop_front().is_none());
    assert!(queue.drain().is_empty());
    owner.close();
    let delivered = DELIVERED.with(|tasks| tasks.replace(Vec::new()));
    assert_eq!(delivered.len(), 2);
    drop((delivered, first.control, first.join, next.control, next.join));
    assert_eq!(drops.get(), 2);
}

/// 第一次调度（Schedule）恐慌（panic），其它原持有仍由任务（Task）机制逐一交出。
struct PanicFirst(std::sync::Arc<std::sync::atomic::AtomicUsize>);
impl Schedule for PanicFirst {
    fn schedule(&self, task: Runnable) {
        if self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            panic!("first-local-schedule");
        }
        DELIVERED.with(|tasks| tasks.borrow_mut().push(task));
    }
}

/// 展开（unwinding）观察器只断言生产出口（production API）已清空来源并移交余项；不清理来源内容。
struct CheckLocalUnwind<'a> {
    queue: &'a mut TaskQueue,
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    closed: bool,
    owner: &'a TaskThread,
}
impl Drop for CheckLocalUnwind<'_> {
    fn drop(&mut self) {
        assert_eq!(self.queue.len(), 0);
        assert_eq!(self.calls.load(std::sync::atomic::Ordering::SeqCst), 3);
        assert_eq!(DELIVERED.with(|tasks| tasks.borrow().len()), 2);
        if self.closed {
            let parts = create_local(self.owner, std::future::pending::<()>(), Delivery, (), TaskOptions::intrusive())
                .unwrap_or_else(|_| panic!("创建（creation）失败"));
            let error = self.queue.push_back(parts.runnable).err().expect("异常后必须仍停收（closed）");
            assert_eq!(error.cause, QueuePushErrorKind::Closed);
            drop(error.task);
        }
    }
}

/// @api TaskQueue::clear；首次外部调度（Schedule）恐慌（panic）仍传播，原批次（batch）承担余项。
#[test]
#[should_panic(expected = "first-local-schedule")]
fn test_clear_first_schedule_panic_still_transfers_remaining_values() {
    let owner = TaskThread::new();
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut queue = TaskQueue::new(&owner);
    for _ in 0..3 {
        let parts = create_local(&owner, std::future::pending::<()>(), PanicFirst(calls.clone()), (), TaskOptions::intrusive())
            .unwrap_or_else(|_| panic!("创建（creation）失败"));
        queue.push_back(parts.runnable).unwrap_or_else(|_| panic!("入队（enqueue）失败"));
    }
    let check = CheckLocalUnwind { queue: &mut queue, calls, closed: false, owner: &owner };
    check.queue.clear().unwrap();
}

/// @api TaskQueue::close；展开（unwinding）后停收不回滚，余项仍经真实原持有合法移交。
#[test]
#[should_panic(expected = "first-local-schedule")]
fn test_close_first_schedule_panic_keeps_closed_and_transfers_tail() {
    let owner = TaskThread::new();
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut queue = TaskQueue::new(&owner);
    for _ in 0..3 {
        let parts = create_local(&owner, std::future::pending::<()>(), PanicFirst(calls.clone()), (), TaskOptions::intrusive())
            .unwrap_or_else(|_| panic!("创建（creation）失败"));
        queue.push_back(parts.runnable).unwrap_or_else(|_| panic!("入队（enqueue）失败"));
    }
    let check = CheckLocalUnwind { queue: &mut queue, calls, closed: true, owner: &owner };
    check.queue.close().unwrap();
}
