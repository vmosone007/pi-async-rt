//! 经公开批次（TaskBatch）及本地队列（TaskQueue）验证唯一持有与保序交接。
use pi_async_task::{create, Runnable, Schedule, TaskOptions};
use pi_async_task_queue::TaskBatch;
use std::collections::VecDeque;

/// 从未绑定的批次（TaskBatch）放弃不需要调度（Schedule）。
struct NoSchedule;
impl Schedule for NoSchedule {
    fn schedule(&self, _task: Runnable) { panic!("批次（batch）不应调度（schedule）"); }
}

/// @test T5/T6/T7/T8/T15；@api TaskQueue::new/owner_id/len/is_empty。
/// 本地队列（TaskQueue）从固定所属域（owner domain）的空状态建立，释放不影响所属域。
#[test]
fn test_local_queue_starts_empty_with_fixed_owner() {
    let owner = pi_async_task::TaskThread::new();
    let queue = pi_async_task_queue::TaskQueue::new(&owner);
    assert_eq!(queue.owner_id(), owner.id());
    assert_eq!(queue.len(), 0);
    assert!(queue.is_empty());
    drop(queue);
    assert!(!owner.is_closed());
}

/// 清理持有（Runnable）交宿主后续原线程（thread）处理，本夹具只保存，不轮询（poll）。
struct Delivery(std::sync::mpsc::Sender<Runnable>);
impl Schedule for Delivery {
    fn schedule(&self, task: Runnable) { self.0.send(task).unwrap_or_else(|_| panic!("通道（channel）已关闭")); }
}

/// @test T9/T10/T11/T12/T15/T16/T17/T18/T19/T20/T92/T95/T96/T97/T98/T116/T117/T118。
/// @api TaskQueue::push_front/push_back/pop_front/pop_back；原址链接（link）摘除后可安全复用。
#[test]
fn test_local_deque_order_and_reusable_embedded_links() {
    let owner = pi_async_task::TaskThread::new();
    let (tx, rx) = std::sync::mpsc::channel();
    let a = pi_async_task::create_local(&owner, std::future::pending::<()>(), Delivery(tx.clone()), (), TaskOptions::intrusive()).unwrap_or_else(|_| panic!("创建（creation）失败"));
    let b = pi_async_task::create_local(&owner, std::future::pending::<()>(), Delivery(tx), (), TaskOptions::intrusive()).unwrap_or_else(|_| panic!("创建（creation）失败"));
    let ids = [a.runnable.id(), b.runnable.id()];
    let mut queue = pi_async_task_queue::TaskQueue::new(&owner);
    queue.push_back(a.runnable).unwrap_or_else(|_| panic!("尾插（push_back）失败"));
    queue.push_front(b.runnable).unwrap_or_else(|_| panic!("头插（push_front）失败"));
    assert_eq!(queue.len(), 2);
    let first = queue.pop_front().unwrap();
    assert_eq!(first.id(), ids[1]);
    queue.push_back(first).unwrap_or_else(|_| panic!("重用链接（link）失败"));
    assert_eq!(queue.pop_back().unwrap().id(), ids[1]);
    assert_eq!(queue.pop_front().unwrap().id(), ids[0]);
    assert!(queue.is_empty());
    owner.close();
    while let Ok(task) = rx.try_recv() { drop(task); }
}

/// @test T13/T14/T117；@api TaskQueue::append_batch/drain。
/// 整批拒绝（batch rejection）保留原顺序且不插入前缀；成功后按队首顺序取回所有唯一持有。
#[test]
fn test_batch_rejection_is_atomic_and_drain_preserves_order() {
    let owner = pi_async_task::TaskThread::new();
    let (tx, rx) = std::sync::mpsc::channel();
    let a = pi_async_task::create_local(&owner, std::future::pending::<()>(), Delivery(tx.clone()), (), TaskOptions::intrusive()).unwrap_or_else(|_| panic!("创建（creation）失败"));
    let b = pi_async_task::create_local(&owner, std::future::pending::<()>(), Delivery(tx), (), TaskOptions::plain()).unwrap_or_else(|_| panic!("创建（creation）失败"));
    let ids = [a.runnable.id(), b.runnable.id()];
    let mut queue = pi_async_task_queue::TaskQueue::new(&owner);
    let batch = TaskBatch::from_tasks(VecDeque::from([a.runnable, b.runnable]));
    let rejected = match queue.append_batch(batch) { Err(error) => error, Ok(_) => panic!("无链接（link）任务必须拒绝") };
    assert_eq!(rejected.cause, pi_async_task_queue::QueuePushErrorKind::LinkMissing);
    assert!(queue.is_empty());
    let returned: Vec<_> = rejected.batch.into_iter().collect();
    assert_eq!(returned.iter().map(Runnable::id).collect::<Vec<_>>(), ids);
    let mut returned = returned.into_iter();
    let valid = returned.next().unwrap();
    let plain = returned.next().unwrap();
    assert_eq!(queue.append_batch(TaskBatch::from_tasks(VecDeque::from([valid]))).unwrap_or_else(|_| panic!("有效批次（batch）失败")), 1);
    let drained = queue.drain();
    assert!(queue.is_empty());
    assert_eq!(drained.into_iter().next().unwrap().id(), ids[0]);
    drop(plain);
    owner.close();
    while let Ok(task) = rx.try_recv() { drop(task); }
}

/// @test T15；@api TaskQueue::drop。
/// 调度（Schedule）在队列析构（drop）中展开（unwind）时，剩余节点（node）仍须退出自持有。
#[test]
fn test_local_queue_drop_unwind_returns_remaining_tickets() {
    struct PanicFirst {
        calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        tx: std::sync::mpsc::Sender<Runnable>,
    }
    impl Schedule for PanicFirst {
        fn schedule(&self, task: Runnable) {
            if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 { panic!("first-schedule"); }
            self.tx.send(task).unwrap_or_else(|_| panic!("通道（channel）已关闭"));
        }
    }
    let owner = pi_async_task::TaskThread::new();
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let metadata = std::sync::Arc::new(());
    let (tx, rx) = std::sync::mpsc::channel();
    let a = pi_async_task::create_local(&owner, std::future::pending::<()>(), PanicFirst { calls: calls.clone(), tx: tx.clone() }, metadata.clone(), TaskOptions::intrusive()).unwrap_or_else(|_| panic!("创建（creation）失败"));
    let b = pi_async_task::create_local(&owner, std::future::pending::<()>(), PanicFirst { calls: calls.clone(), tx }, metadata.clone(), TaskOptions::intrusive()).unwrap_or_else(|_| panic!("创建（creation）失败"));
    let mut queue = pi_async_task_queue::TaskQueue::new(&owner);
    queue.push_back(a.runnable).unwrap_or_else(|_| panic!("入队（enqueue）失败"));
    queue.push_back(b.runnable).unwrap_or_else(|_| panic!("入队（enqueue）失败"));
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(queue))).unwrap_err();
    assert_eq!(panic.downcast_ref::<&str>(), Some(&"first-schedule"));
    owner.close();
    while let Ok(task) = rx.try_recv() { drop(task); }
    drop((a.control, a.join, b.control, b.join));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(std::sync::Arc::strong_count(&metadata), 1);
}

/// @test T1/T2/T3/T4；@api TaskBatch::from_tasks/len/is_empty/into_iter。
/// 批次（batch）接管原序列，不轮询（poll）、克隆（clone）或改变任务（Task）身份。
#[test]
fn test_batch_transfers_original_order_and_identity() {
    let first = create(std::future::pending::<()>(), NoSchedule, (), TaskOptions::plain()).unwrap_or_else(|_| panic!("创建（creation）失败"));
    let second = create(std::future::pending::<()>(), NoSchedule, (), TaskOptions::plain()).unwrap_or_else(|_| panic!("创建（creation）失败"));
    let ids = [first.runnable.id(), second.runnable.id()];
    let batch = TaskBatch::from_tasks(VecDeque::from([first.runnable, second.runnable]));
    assert_eq!(batch.len(), 2);
    assert!(!batch.is_empty());
    let actual: Vec<_> = batch.into_iter().map(|task| task.id()).collect();
    assert_eq!(actual, ids);
    let empty = TaskBatch::from_tasks(VecDeque::new());
    assert!(empty.is_empty());
}
