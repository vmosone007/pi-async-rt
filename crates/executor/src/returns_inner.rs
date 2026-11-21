#[allow(unused_variables)]
fn return_new_inner(
    owner: TaskOwnerId,
    notifier: ThreadNotifier,
    counts: Arc<ExecutorState>,
    batch: NonZeroUsize,
) -> pi_result::Result<ReturnQueue> {
    // 原生空节点（stub）与专用终点节点（node marking the end of a clear operation）独立分配；第二次失败时释放先前节点。
    let stub = MpscEntryHandle::try_new_empty()?;
    let endpoint = MpscEntryHandle::try_new_empty()?;
    Ok(ReturnQueue {
        shared: Arc::new(ReturnShared {
            queue: cordyceps::MpscQueue::new_with_stub(stub),
            state: AtomicUsize::new(0),
            endpoint: UnsafeCell::new(ReturnEndpoint::Available(endpoint)),
            owner,
            notifier,
            counts,
            batch,
        }),
    })
}
#[allow(unused_variables)]
fn return_handle_inner(this: &ReturnQueue) -> ReturnQueueHandle {
    ReturnQueueHandle { shared: Arc::clone(&this.shared) }
}
#[allow(unused_variables)]
fn return_clone_inner(this: &ReturnQueueHandle) -> ReturnQueueHandle {
    ReturnQueueHandle { shared: Arc::clone(&this.shared) }
}
#[allow(unused_variables)]
fn return_len_inner(this: &ReturnQueueHandle) -> usize {
    this.shared.state.load(std::sync::atomic::Ordering::Relaxed) & RETURN_COUNT
}
#[allow(unused_variables)]
fn return_publish_inner(
    this: &ReturnQueueHandle,
    task: Runnable,
    completion: &mut ReturnCompletionGuard<'_>,
) -> pi_result::RawResult<(), Runnable> {
    use pi_async_task::definitions::ExecutionStatus;
    use std::sync::atomic::Ordering::{AcqRel, Relaxed};

    let shared = this.shared.as_ref();
    let snapshot = task.snapshot();
    if snapshot.owner != Some(shared.owner)
        || !(snapshot.admitted || (snapshot.execution == ExecutionStatus::Exiting && snapshot.termination.is_some()))
    {
        return Err(task);
    }
    let entry = MpscEntryHandle::try_from_runnable(task)?;
    // 已登记但尚未发布的原值也保有责任；真正的内容发布由原生队列（native queue）完成。
    if shared.state.fetch_add(1, AcqRel) & RETURN_COUNT == RETURN_COUNT
        || shared.counts.internal.fetch_add(1, Relaxed) == usize::MAX
    {
        std::process::abort();
    }
    completion.arm();
    shared.queue.enqueue(entry);
    Ok(())
}
#[allow(unused_variables)]
fn return_take_inner(this: &ReturnShared, completion: &mut ReturnCompletionGuard<'_>) -> ReturnTake {
    use cordyceps::mpsc_queue::TryDequeueError;
    use std::sync::atomic::Ordering::{Acquire, Release};

    let mut result = ReturnTake::Empty;
    let cleanup = ReturnCleanupGuard { shared: this, current: None, debt: false, resume_tail: false };
    let Some(consumer) = this.queue.try_consume() else { return ReturnTake::Busy; };
    let consumer = ReturnConsumerGuard { shared: this, consumer: Some(consumer), endpoint: None };
    completion.arm();

    let state = this.state.fetch_or(0, Acquire);
    if state & RETURN_OWNER_GONE != 0 {
        result = ReturnTake::Closed;
    } else {
        let native = consumer.consumer.as_ref().unwrap_or_else(|| std::process::abort());
        // 首次尝试后，仅在取回专用终点节点（node marking the end of a clear operation）时继续。
        for _ in 0..=this.batch.get() {
            match native.try_dequeue() {
                Ok(entry) => match entry.try_into_runnable() {
                    Ok(task) => {
                        result = ReturnTake::Ready(task);
                        // 原值已由独立结果持有；归还本来源数量，内部任务队列（internal task queue）的数量随原值移交。
                        if this.state.fetch_sub(1, Release) & RETURN_COUNT == 0 {
                            std::process::abort();
                        }
                        break;
                    }
                    Err(endpoint) => {
                        // SAFETY: 原生队列（native queue）已归还节点（node），当前 Consumer 独占终点访问。
                        unsafe {
                            let state = this.endpoint.get();
                            if !matches!(&*state, ReturnEndpoint::Queued) {
                                std::process::abort();
                            }
                            state.write(ReturnEndpoint::Reached(endpoint));
                        }
                    }
                },
                // 原生空（Empty）可能出现在首项已登记且前序链接尚未发布时。
                // 原执行器在所属线程单项取出，计数仍包含尚未交出的已登记项，真实发布方完成后通知。
                Err(TryDequeueError::Empty) if state & RETURN_COUNT == 0 => break,
                Err(TryDequeueError::Empty | TryDequeueError::Inconsistent) => {
                    result = ReturnTake::WouldBlock;
                    break;
                }
                Err(TryDequeueError::Busy) => std::process::abort(),
            }
        }
    }
    drop(consumer);
    drop(cleanup);
    result
}
#[allow(unused_variables)]
fn return_batch_inner(this: &ReturnQueueHandle, completion: &mut ReturnCompletionGuard<'_>) -> BatchAttempt {
    use cordyceps::mpsc_queue::TryDequeueError;
    use pi_async_task_queue::TaskBatch;
    use std::sync::atomic::Ordering::{Acquire, Release};

    let shared = this.shared.as_ref();
    let initial = shared.state.fetch_or(0, Acquire);
    if initial & RETURN_OWNER_GONE != 0 {
        return BatchAttempt::Closed;
    }
    let count = initial & RETURN_COUNT;
    if count == 0 {
        return BatchAttempt::Empty;
    }
    let mut result = BatchAttempt::Empty;
    let cleanup = ReturnCleanupGuard { shared, current: None, debt: false, resume_tail: false };
    let mut tasks = VecDeque::with_capacity(count);
    let limit = count.min(tasks.capacity());
    let Some(consumer) = shared.queue.try_consume() else { return BatchAttempt::Busy; };
    let consumer = ReturnConsumerGuard { shared, consumer: Some(consumer), endpoint: None };
    completion.arm();

    if shared.state.load(Acquire) & RETURN_OWNER_GONE != 0 {
        result = BatchAttempt::Closed;
    } else {
        let native = consumer.consumer.as_ref().unwrap_or_else(|| std::process::abort());
        while tasks.len() < limit {
            match native.try_dequeue() {
                Ok(entry) => match entry.try_into_runnable() {
                    Ok(task) => tasks.push_back(task),
                    Err(endpoint) => {
                        // SAFETY: 原生队列（native queue）已归还节点（node），当前 Consumer 独占终点访问。
                        unsafe {
                            let state = shared.endpoint.get();
                            if !matches!(&*state, ReturnEndpoint::Queued) {
                                std::process::abort();
                            }
                            state.write(ReturnEndpoint::Reached(endpoint));
                        }
                    }
                },
                Err(TryDequeueError::Empty) => break,
                Err(TryDequeueError::Inconsistent) => {
                    result = BatchAttempt::WouldBlock;
                    break;
                }
                Err(TryDequeueError::Busy) => std::process::abort(),
            }
        }
        let taken = tasks.len();
        if taken != 0 {
            // 先把非空前缀交给独立批次（batch），再归还本来源数量；内部任务队列（internal task queue）的数量不变。
            result = BatchAttempt::Ready(TaskBatch::from_tasks(tasks));
            if shared.state.fetch_sub(taken, Release) & RETURN_COUNT < taken {
                std::process::abort();
            }
        }
    }
    drop(consumer);
    drop(cleanup);
    result
}
#[allow(unused_variables)]
fn return_clear_inner(this: &mut ReturnQueue) -> pi_result::Result<QueueCleanupProgress> {
    Ok(match this.shared.cleanup(false) {
        ReturnStep::Complete => QueueCleanupProgress::Complete,
        ReturnStep::MoreWork | ReturnStep::Deferred | ReturnStep::Busy => QueueCleanupProgress::MoreWork,
    })
}
#[allow(unused_variables)]
fn return_close_inner(this: &mut ReturnQueue) -> QueueCleanupProgress {
    // 调用者已完成所属域（owner domain）的退出；此后仍接续迟到的原任务（Task）。
    this.shared.state.fetch_or(
        RETURN_OWNER_GONE | RETURN_REQUEST,
        std::sync::atomic::Ordering::AcqRel,
    );
    loop {
        match this.shared.cleanup(true) {
            ReturnStep::Complete => return QueueCleanupProgress::Complete,
            ReturnStep::MoreWork => continue,
            ReturnStep::Deferred | ReturnStep::Busy => return QueueCleanupProgress::MoreWork,
        }
    }
}
#[allow(unused_variables)]
fn return_owner_drop_inner(this: &mut ReturnQueue) {
    // 合法持有者在所属域（TaskThread）及本地原值退出后才释放本队列（queue）。
    this.shared.state.fetch_or(RETURN_OWNER_GONE | RETURN_REQUEST, std::sync::atomic::Ordering::AcqRel);
    this.shared.assist();
}
#[allow(unused_variables)]
fn return_request_inner(this: &ReturnShared) {
    // 只在独立进入或实际清理推进后调用；仅因请求重查时不登记新的请求。
    this.state.fetch_or(RETURN_REQUEST, std::sync::atomic::Ordering::AcqRel);
}
#[allow(unused_variables)]
fn return_confirm_inner(this: &ReturnShared) -> bool {
    // 请求位即使已清除，也须以同一状态字的读改写（read-modify-write，RMW）确认所属域（owner domain）退出。
    this.state.fetch_or(0, std::sync::atomic::Ordering::AcqRel) & RETURN_OWNER_GONE != 0
}
#[allow(unused_variables)]
fn return_cleanup_inner(
    this: &ReturnShared,
    exiting: bool,
    mut completion: Option<&mut ReturnCompletionGuard<'_>>,
) -> ReturnStep {
    use cordyceps::mpsc_queue::TryDequeueError;
    use std::sync::atomic::Ordering::{AcqRel, Release};

    let mut cleanup = ReturnCleanupGuard { shared: this, current: None, debt: false, resume_tail: exiting };
    let mut result = ReturnStep::MoreWork;
    for _ in 0..this.batch.get() {
        let Some(consumer) = this.queue.try_consume() else {
            result = ReturnStep::Busy;
            break;
        };
        let mut consumer = ReturnConsumerGuard { shared: this, consumer: Some(consumer), endpoint: None };
        if let Some(completion) = &mut completion {
            completion.arm();
        }
        let previous = this.state.fetch_and(!RETURN_REQUEST, AcqRel);
        let closing = exiting || previous & RETURN_OWNER_GONE != 0;
        let mut progressed = false;

        // SAFETY: 当前 Consumer 是终点的唯一访问者，且上一原值已经在权限外完成清理和结算。
        unsafe {
            let endpoint = this.endpoint.get();
            if matches!(&*endpoint, ReturnEndpoint::Reached(_)) && (!closing || previous & RETURN_COUNT == 0) {
                let ReturnEndpoint::Reached(value) = endpoint.replace(ReturnEndpoint::Transferring) else {
                    std::process::abort();
                };
                endpoint.write(ReturnEndpoint::Available(value));
                if !closing {
                    // 开放清理只到此次终点；终点之后的原值不属于本次范围。
                    result = ReturnStep::Complete;
                    break;
                }
                progressed = true;
            }
            if !closing && matches!(&*endpoint, ReturnEndpoint::Available(_)) {
                let ReturnEndpoint::Available(value) = endpoint.replace(ReturnEndpoint::Transferring) else {
                    std::process::abort();
                };
                consumer.endpoint = Some(value);
            }
        }
        if let Some(endpoint) = consumer.endpoint.take() {
            this.queue.enqueue(endpoint);
            // SAFETY: 同一 Consumer 仍独占终点；原生队列（native queue）已接管该节点（node）。
            unsafe { this.endpoint.get().write(ReturnEndpoint::Queued); }
        }

        let (empty, blocked) = match consumer.consumer.as_ref().unwrap_or_else(|| std::process::abort()).try_dequeue() {
            Ok(entry) => {
                match entry.try_into_runnable() {
                    Ok(task) => {
                        cleanup.current = Some(task);
                        cleanup.debt = true;
                    }
                    Err(endpoint) => {
                        // SAFETY: 原生队列（native queue）已交回节点（node）；仍在独占 Consumer 内恢复其唯一持有。
                        unsafe {
                            let state = this.endpoint.get();
                            if !matches!(&*state, ReturnEndpoint::Queued) {
                                std::process::abort();
                            }
                            state.write(ReturnEndpoint::Reached(endpoint));
                        }
                        progressed = true;
                    }
                }
                (false, false)
            }
            Err(TryDequeueError::Empty) => (true, false),
            Err(TryDequeueError::Inconsistent) => (false, true),
            Err(TryDequeueError::Busy) => std::process::abort(),
        };
        // SAFETY: 空（Empty）和终点状态取自同一次 Consumer 权限，引用不跨越权限归还。
        let endpoint_available = empty && unsafe { matches!(&*this.endpoint.get(), ReturnEndpoint::Available(_)) };
        let empty_and_settled = closing && endpoint_available && this.state.fetch_or(0, AcqRel) & RETURN_COUNT == 0;
        drop(consumer);
        if let Some(task) = cleanup.current.take() {
            // 首次恐慌（panic）时守卫只归还这两项数量，不会再次析构（drop）已经移走的原值。
            drop(task);
            cleanup.debt = false;
            if this.counts.internal.fetch_sub(1, Release) == 0
                || this.state.fetch_sub(1, Release) & RETURN_COUNT == 0
            {
                std::process::abort();
            }
            progressed = true;
        }
        if closing && progressed {
            this.state.fetch_or(RETURN_REQUEST, AcqRel);
        }
        if empty || blocked {
            let current = this.state.fetch_or(0, AcqRel);
            result = if empty_and_settled && current & RETURN_COUNT == 0 {
                ReturnStep::Complete
            } else if closing && (progressed || current & RETURN_REQUEST != 0) {
                ReturnStep::MoreWork
            } else {
                // 原生节点连接尚未完成（Inconsistent）或数量尚未结算时，由真实参与者在结束后请求重查。
                ReturnStep::Deferred
            };
            break;
        }
    }
    cleanup.resume_tail = false;
    drop(cleanup);
    result
}
#[allow(unused_variables)]
fn return_consumer_drop_inner(this: &mut ReturnConsumerGuard<'_>) {
    if let Some(endpoint) = this.endpoint.take() {
        if this.consumer.is_none() {
            std::process::abort();
        }
        // SAFETY: 本守卫（guard）仍持有同一队列（queue）的唯一 Consumer。
        // Transferring 不持有节点（node）；只写回未发布的原节点，访问在归还 Consumer 前结束。
        unsafe {
            let state = this.shared.endpoint.get();
            if !matches!(&*state, ReturnEndpoint::Transferring) {
                std::process::abort();
            }
            state.write(ReturnEndpoint::Available(endpoint));
        }
    }
    drop(this.consumer.take());
}
fn return_completion_new_inner(shared: &ReturnShared) -> ReturnCompletionGuard<'_> {
    ReturnCompletionGuard { shared, armed: false }
}

fn return_completion_arm_inner(this: &mut ReturnCompletionGuard<'_>) {
    this.armed = true;
}

fn return_completion_drop_inner(this: &mut ReturnCompletionGuard<'_>) -> bool {
    // 在实际发布（publication）之后，或在本次归还 Consumer 并结清清理责任之后确认退出。
    std::mem::take(&mut this.armed)
}
#[allow(unused_variables)]
fn return_cleanup_drop_inner(this: &mut ReturnCleanupGuard<'_>) {
    use std::sync::atomic::Ordering::{AcqRel, Release};
    use cordyceps::mpsc_queue::TryDequeueError;

    let shared = this.shared;
    let release_count = || {
        // 先归还内部任务队列（internal task queue）的数量，再发布原归属返回（owner return）的数量。
        if shared.counts.internal.fetch_sub(1, Release) == 0
            || shared.state.fetch_sub(1, Release) & RETURN_COUNT == 0
        {
            std::process::abort();
        }
    };
    let debt = std::mem::take(&mut this.debt);
    // 正常路径在析构（drop）前已移走原句柄；首次恐慌（panic）后这里只归还它的数量。
    drop(this.current.take());
    if debt {
        release_count();
    }
    if !std::mem::take(&mut this.resume_tail) {
        return;
    }

    // 异常展开（unwinding）的首次接续只登记一次；后续仅在真正取回终点或归还数量后登记。
    shared.state.fetch_or(RETURN_REQUEST, AcqRel);
    loop {
        let Some(consumer) = shared.queue.try_consume() else {
            // 已登记请求；实际持有 Consumer 的调用者负责归还后重查。
            return;
        };
        let request = shared.state.fetch_and(!RETURN_REQUEST, AcqRel);
        let mut progressed = false;
        if request & RETURN_COUNT == 0 {
            // SAFETY: 当前 Consumer 独占终点且无尚未结算的原句柄；只归还已安全取回的原终点。
            unsafe {
                let endpoint = shared.endpoint.get();
                if matches!(&*endpoint, ReturnEndpoint::Reached(_)) {
                    let ReturnEndpoint::Reached(value) = endpoint.replace(ReturnEndpoint::Transferring) else {
                        std::process::abort();
                    };
                    endpoint.write(ReturnEndpoint::Available(value));
                    progressed = true;
                }
            }
        }

        let mut task = None;
        let empty = match consumer.try_dequeue() {
            Ok(entry) => {
                match entry.try_into_runnable() {
                    Ok(value) => task = Some(value),
                    Err(value) => {
                        // SAFETY: 原生队列（native queue）已归还节点（node），Consumer 仍独占终点。
                        unsafe {
                            let endpoint = shared.endpoint.get();
                            if !matches!(&*endpoint, ReturnEndpoint::Queued) {
                                std::process::abort();
                            }
                            endpoint.write(ReturnEndpoint::Reached(value));
                        }
                        progressed = true;
                    }
                }
                false
            }
            Err(TryDequeueError::Empty) => true,
            Err(TryDequeueError::Inconsistent) => false,
            Err(TryDequeueError::Busy) => std::process::abort(),
        };
        // SAFETY: 空（Empty）和终点状态取自同一次 Consumer 权限，引用不跨越权限归还。
        let endpoint_available = empty && unsafe { matches!(&*shared.endpoint.get(), ReturnEndpoint::Available(_)) };
        let empty_and_settled = endpoint_available && shared.state.fetch_or(0, AcqRel) & RETURN_COUNT == 0;
        drop(consumer);
        if let Some(task) = task {
            drop(task);
            release_count();
            progressed = true;
        }
        if progressed {
            shared.state.fetch_or(RETURN_REQUEST, AcqRel);
        }
        // 第一次未证明完成时不能用较早的空（Empty）与后来归零的数量拼成完成事实。
        let state = shared.state.fetch_or(0, AcqRel);
        if empty_and_settled && state & RETURN_COUNT == 0 {
            return;
        }
        if !progressed && state & RETURN_REQUEST == 0 {
            // 尚不能取出的原值仍有真实发布者或权限外的清理者，完成方会重新登记请求。
            return;
        }
    }
}
#[allow(unused_variables)]
fn return_notify_inner(this: &ReturnQueueHandle) {
    pi_async_task_queue::notify::QueueNotifier::notify(&this.shared.notifier);
}

#[cfg(test)]
mod returns_tests {
    use super::{ExecutorState, ReturnQueue};
    use crate::ThreadParker;
    use pi_async_task::definitions::{FutureDropStatus, RunOutcome};
    use pi_async_task::{create_local, MpscEntryHandle, MpscQueueEntry, Runnable, Schedule, TaskOptions, TaskThread};
    use std::cell::Cell;
    use std::future::Future;
    use std::num::NonZeroUsize;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Weak,
    };
    use std::task::{Context, Poll};

    /// 记录原线程上的轮询（poll）及析构（drop）；记录值不可跨线程转移（Send）。
    struct LocalFuture {
        polls: Rc<Cell<usize>>,
        drops: Rc<Cell<usize>>,
    }

    impl Future for LocalFuture {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            self.polls.set(self.polls.get() + 1);
            Poll::Pending
        }
    }

    impl Drop for LocalFuture {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    struct Captured {
        queue: cordyceps::MpscQueue<MpscQueueEntry>,
        pending: AtomicUsize,
        capacity: usize,
    }

    /// 只接住原任务唤醒（wake-up）已经交付的可执行句柄（runnable handle）。
    /// 调度者持弱引用（Weak），故接收端释放时不会和留下的原任务形成环。
    #[derive(Clone)]
    struct CaptureWake(Weak<Captured>);

    /// 按原用例的容量捕获调度出口，超出容量仍直接触发断言失败。
    fn capture_wake(capacity: usize) -> (CaptureWake, Arc<Captured>) {
        let stub = MpscEntryHandle::try_new_empty().unwrap();
        let destination = Arc::new(Captured {
            queue: cordyceps::MpscQueue::new_with_stub(stub),
            pending: AtomicUsize::new(0),
            capacity,
        });
        (CaptureWake(Arc::downgrade(&destination)), destination)
    }

    /// 交付已完成的同一原值，在原归属回送（owner return）操作前归还捕获端链接。
    fn take_captured(destination: &Captured) -> Option<Runnable> {
        use cordyceps::mpsc_queue::TryDequeueError;

        match destination.queue.try_dequeue() {
            Ok(entry) => {
                destination.pending.fetch_sub(1, Ordering::Release);
                Some(entry.try_into_runnable().unwrap_or_else(|_| panic!("唤醒出口只交付原可执行句柄")))
            }
            Err(TryDequeueError::Empty) => None,
            Err(other) => panic!("捕获端应已完成单次交付，实际为 {other:?}"),
        }
    }

    impl Schedule for CaptureWake {
        fn schedule(&self, task: Runnable) {
            let destination = self.0.upgrade().expect("用例仍保管接收端");
            let entry = MpscEntryHandle::try_from_runnable(task)
                .unwrap_or_else(|_| panic!("用例任务须保留原 MPSC 链接"));
            let previous = destination.pending.fetch_add(1, Ordering::AcqRel);
            assert!(previous < destination.capacity, "只应交付指定数量的可执行句柄");
            destination.queue.enqueue(entry);
        }
    }

    /// 在可执行句柄（runnable handle）成为末份持有后，从第一个任务附加数据（task metadata）产生原恐慌（panic）。
    struct MetadataDrop {
        drops: Arc<AtomicUsize>,
        panic_on_drop: Option<Arc<AtomicBool>>,
    }

    impl Drop for MetadataDrop {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
            if self.panic_on_drop.as_ref().is_some_and(|armed| armed.load(Ordering::Relaxed)) {
                std::panic::panic_any("return-metadata-drop");
            }
        }
    }

    /// 通过 ReturnQueue::new、ReturnQueueHandle::publish 和 ReturnQueue 的析构（drop）验证原归属返回（owner return）。
    /// 所属域（owner domain）先真实退出；单项额度下首份任务附加数据（task metadata）发生恐慌（panic）时仍结清余项。
    /// 另一个真实任务唤醒（wake-up）交出的原句柄在管理者退出后才发布；共享句柄（shared handle）仍在时即须结清。
    #[test]
    fn test_owner_return_releases_tail_after_panic_and_accepts_late_publication() {
        // 在异常展开（unwinding）时也先实际释放所属域（TaskThread），再释放 ReturnQueue。
        let returns;
        let owner = TaskThread::new();
        let mut parker = ThreadParker::new();
        let state = Arc::new(ExecutorState::new());
        returns = ReturnQueue::new(owner.id(), parker.notifier(), state, NonZeroUsize::MIN)
            .expect("准备原归属返回（owner return）");
        let handle = returns.handle();
        let (sender, received) = capture_wake(1);
        let polls: [Rc<Cell<usize>>; 3] = std::array::from_fn(|_| Rc::new(Cell::new(0)));
        let drops: [Rc<Cell<usize>>; 3] = std::array::from_fn(|_| Rc::new(Cell::new(0)));
        let metadata: [Arc<AtomicUsize>; 3] = std::array::from_fn(|_| Arc::new(AtomicUsize::new(0)));
        let panic_on_drop = Arc::new(AtomicBool::new(false));
        let create = |index: usize| {
            create_local(
                &owner,
                LocalFuture { polls: Rc::clone(&polls[index]), drops: Rc::clone(&drops[index]) },
                sender.clone(),
                MetadataDrop {
                    drops: Arc::clone(&metadata[index]),
                    panic_on_drop: (index == 0).then(|| Arc::clone(&panic_on_drop)),
                },
                TaskOptions::intrusive_local_and_mpsc(),
            ).unwrap_or_else(|error| panic!("创建原任务（Task）失败：{:?}", error.cause))
        };
        assert_eq!(handle.len(), 0);
        assert!(parker.take_notification());
        for index in 0..2 {
            let parts = create(index);
            let admission = parts.runnable.admit(&owner).expect("接纳（admission）原任务（Task）");
            assert!(handle.publish(parts.runnable).is_ok());
            drop((admission, parts.control, parts.join));
        }
        assert_eq!(handle.len(), 2);
        assert!(parker.take_notification());

        let parts = create(2);
        let admission = parts.runnable.admit(&owner).expect("接纳（admission）原任务（Task）");
        assert!(matches!(parts.runnable.run(&owner), Ok(RunOutcome::Pending)));
        parts.control.wake();
        let late = take_captured(&received).expect("取得实际任务唤醒（wake-up）交出的可执行句柄（runnable handle）");
        assert_eq!(late.id(), parts.control.id());
        drop(admission);
        assert!(take_captured(&received).is_none());
        drop(owner);
        assert_eq!(parts.control.snapshot().future_drop, FutureDropStatus::Confirmed);
        drop((parts.control, parts.join));
        assert_eq!(std::array::from_fn::<_, 3, _>(|index| polls[index].get()), [0, 0, 1]);
        assert_eq!(std::array::from_fn::<_, 3, _>(|index| drops[index].get()), [1, 1, 1]);
        assert_eq!(std::array::from_fn::<_, 3, _>(|index| metadata[index].load(Ordering::Relaxed)), [0, 0, 0]);

        panic_on_drop.store(true, Ordering::Relaxed);
        let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(returns)))
            .expect_err("首份任务附加数据（task metadata）的恐慌（panic）应原样传播");
        assert_eq!(failure.downcast_ref::<&'static str>(), Some(&"return-metadata-drop"));
        assert_eq!(handle.len(), 0);
        assert_eq!(std::array::from_fn::<_, 3, _>(|index| metadata[index].load(Ordering::Relaxed)), [1, 1, 0]);
        assert!(!parker.take_notification());

        std::thread::scope(|scope| {
            let handle = &handle;
            scope.spawn(move || assert!(handle.publish(late).is_ok())).join().unwrap();
        });
        assert!(parker.take_notification());
        assert_eq!(handle.len(), 0);
        assert_eq!(std::array::from_fn::<_, 3, _>(|index| metadata[index].load(Ordering::Relaxed)), [1, 1, 1]);
        assert_eq!(std::array::from_fn::<_, 3, _>(|index| polls[index].get()), [0, 0, 1]);
        assert_eq!(std::array::from_fn::<_, 3, _>(|index| drops[index].get()), [1, 1, 1]);
    }

    /// 通过 ReturnQueue::clear、try_take 和 close_after_owner 验证原归属返回（owner return）的固定清理范围。
    /// 单项额度先清理终点前的值；随后只取到专用终点节点（node marking the end of a clear operation），不得把它当作任务（Task）。
    /// 后来发布的原值不扩大本次范围；实际所属域（TaskThread）退出后，同一来源再清理余项。
    #[test]
    fn test_owner_return_clear_leaves_later_publication_for_close() {
        use super::{QueueCleanupProgress, ReturnTake};

        let mut returns;
        let owner = TaskThread::new();
        let parker = ThreadParker::new();
        returns = ReturnQueue::new(
            owner.id(), parker.notifier(), Arc::new(ExecutorState::new()), NonZeroUsize::MIN,
        ).expect("准备原归属返回（owner return）");
        let handle = returns.handle();
        let (sender, received) = capture_wake(2);
        let make_task = || {
            create_local(
                &owner, std::future::pending::<()>(), sender.clone(), (),
                TaskOptions::intrusive_local_and_mpsc(),
            ).unwrap_or_else(|error| panic!("创建原任务（Task）失败：{:?}", error.cause))
        };
        let prefix = make_task();
        let suffix = make_task();
        let prefix_admission = prefix.runnable.admit(&owner).expect("接纳（admission）原任务（Task）");
        let suffix_admission = suffix.runnable.admit(&owner).expect("接纳（admission）原任务（Task）");
        assert!(handle.publish(prefix.runnable).is_ok());
        assert_eq!(handle.len(), 1);
        assert!(matches!(returns.clear(), Ok(QueueCleanupProgress::MoreWork)));
        let cleanup = take_captured(&received).expect("接到首项原任务（Task）的清理持有");
        assert_eq!(cleanup.id(), prefix.control.id());
        assert_eq!(handle.len(), 0);
        assert!(matches!(returns.try_take(), ReturnTake::Empty));

        let retained = handle.clone();
        assert!(retained.publish(suffix.runnable).is_ok());
        assert!(matches!(returns.clear(), Ok(QueueCleanupProgress::Complete)));
        assert_eq!(handle.len(), 1);
        assert_eq!(retained.len(), 1);
        assert!(take_captured(&received).is_none());
        drop((prefix_admission, suffix_admission));
        drop(owner);
        assert_eq!(prefix.control.snapshot().future_drop, FutureDropStatus::Confirmed);
        assert_eq!(suffix.control.snapshot().future_drop, FutureDropStatus::Confirmed);
        drop(cleanup);
        assert!(matches!(returns.close_after_owner(), QueueCleanupProgress::Complete));
        assert_eq!(handle.len(), 0);
        assert!(matches!(returns.try_take(), ReturnTake::Closed));
        drop(returns);
        assert_eq!(retained.len(), 0);
    }

    /// 通过 ReturnQueue::try_take 和 ReturnQueueHandle::try_take_batch 取回专用终点节点之后的原值。
    /// 清理额度为一时，单项取出仍应返回已排在终点之后的原值；批次（batch）按原顺序交回两份。
    /// 普通取出只记录终点；两次固定范围均由同一管理者随后确认完成。
    #[test]
    fn test_owner_return_takes_original_values_past_clear_endpoints() {
        use super::{BatchAttempt, QueueCleanupProgress, ReturnTake};

        let mut returns;
        let owner = TaskThread::new();
        let parker = ThreadParker::new();
        returns = ReturnQueue::new(
            owner.id(), parker.notifier(), Arc::new(ExecutorState::new()), NonZeroUsize::MIN,
        ).expect("准备原归属返回（owner return）");
        let handle = returns.handle();
        let (sender, received) = capture_wake(2);
        let create = || {
            create_local(
                &owner, std::future::pending::<()>(), sender.clone(), (),
                TaskOptions::intrusive_local_and_mpsc(),
            ).unwrap_or_else(|error| panic!("创建原任务（Task）失败：{:?}", error.cause))
        };
        let mut cleanup_holders = Vec::with_capacity(2);
        let mut publish_prefix = |returns: &mut ReturnQueue| {
            let parts = create();
            let admission = parts.runnable.admit(&owner).expect("接纳（admission）原任务（Task）");
            assert!(handle.publish(parts.runnable).is_ok());
            assert!(matches!(returns.clear(), Ok(QueueCleanupProgress::MoreWork)));
            let cleanup = take_captured(&received).expect("接到终点之前的原任务（Task）的清理持有");
            assert_eq!(cleanup.id(), parts.control.id());
            assert_eq!(handle.len(), 0);
            cleanup_holders.push(cleanup);
            drop(admission);
        };
        publish_prefix(&mut returns);
        let one = create();
        let one_admission = one.runnable.admit(&owner).expect("接纳（admission）原任务（Task）");
        assert!(handle.publish(one.runnable).is_ok());
        assert_eq!(handle.len(), 1);
        let ReturnTake::Ready(single) = returns.try_take() else {
            panic!("单项取出应越过专用终点节点，取到已发布的原任务（Task）");
        };
        assert_eq!(single.id(), one.control.id());
        assert_eq!(handle.len(), 0);
        assert!(matches!(returns.clear(), Ok(QueueCleanupProgress::Complete)));

        publish_prefix(&mut returns);
        let first = create();
        let second = create();
        let first_admission = first.runnable.admit(&owner).expect("接纳（admission）原任务（Task）");
        let second_admission = second.runnable.admit(&owner).expect("接纳（admission）原任务（Task）");
        assert!(handle.publish(first.runnable).is_ok());
        assert!(handle.publish(second.runnable).is_ok());
        assert_eq!(handle.len(), 2);
        let BatchAttempt::Ready(batch) = handle.try_take_batch() else {
            panic!("批次（batch）取出应越过专用终点节点，按发布顺序交付两份原任务（Task）");
        };
        assert_eq!(batch.len(), 2);
        let mut original = batch.into_iter();
        let first_task = original.next().expect("取得第一份原值");
        let second_task = original.next().expect("取得第二份原值");
        assert!(original.next().is_none());
        assert_eq!(first_task.id(), first.control.id());
        assert_eq!(second_task.id(), second.control.id());
        assert_eq!(handle.len(), 0);
        assert!(matches!(returns.clear(), Ok(QueueCleanupProgress::Complete)));
        assert!(matches!(handle.try_take_batch(), BatchAttempt::Empty));
        assert!(take_captured(&received).is_none());

        drop((one_admission, first_admission, second_admission));
        drop(owner);
        assert_eq!(one.control.snapshot().future_drop, FutureDropStatus::Confirmed);
        assert_eq!(first.control.snapshot().future_drop, FutureDropStatus::Confirmed);
        assert_eq!(second.control.snapshot().future_drop, FutureDropStatus::Confirmed);
        drop((cleanup_holders, single, first_task, second_task));
        assert!(matches!(returns.close_after_owner(), QueueCleanupProgress::Complete));
        assert!(matches!(handle.try_take_batch(), BatchAttempt::Closed));
    }

    /// 通过 ReturnQueue::close_after_owner 查验：已经取空的原生队列（native queue）仍可能有未结清的清理责任。
    /// 迟到发布者在权限外实际析构（drop）任务附加数据（task metadata）；管理者在这段时间只返回 MoreWork。
    #[test]
    fn test_owner_return_close_reports_more_work_until_cleanup_settles() {
        use super::QueueCleanupProgress;
        use std::sync::OnceLock;
        use std::thread::{self, Thread};
        use std::time::{Duration, Instant};

        struct Pause {
            observer: Thread,
            waiting: OnceLock<Thread>,
            released: AtomicBool,
        }

        /// 在原值析构（drop）和关闭观察点同步两线程，超时直接判用例失败。
        fn wait_until(mut ready: impl FnMut() -> bool) {
            let deadline = Instant::now() + Duration::from_secs(5);
            while !ready() {
                let remaining = deadline.saturating_duration_since(Instant::now());
                assert!(!remaining.is_zero(), "未在限定时间内收到对方的通知");
                thread::park_timeout(remaining);
            }
        }

        /// 用例正常结束或断言失败都会通知对方继续原析构。
        struct ResumeOnDrop<'a>(&'a Pause);

        impl Drop for ResumeOnDrop<'_> {
            fn drop(&mut self) {
                self.0.released.store(true, Ordering::Release);
                if let Some(thread) = self.0.waiting.get() {
                    thread.unpark();
                }
            }
        }

        struct PausedMetadataDrop {
            armed: Arc<AtomicBool>,
            pause: Arc<Pause>,
        }

        impl Drop for PausedMetadataDrop {
            fn drop(&mut self) {
                if !self.armed.load(Ordering::Acquire) {
                    return;
                }
                assert!(self.pause.waiting.set(thread::current()).is_ok());
                self.pause.observer.unpark();
                wait_until(|| self.pause.released.load(Ordering::Acquire));
            }
        }

        let mut returns;
        let owner = TaskThread::new();
        let parker = ThreadParker::new();
        returns = ReturnQueue::new(
            owner.id(), parker.notifier(), Arc::new(ExecutorState::new()), NonZeroUsize::MIN,
        ).expect("准备原归属返回（owner return）");
        let handle = returns.handle();
        let (wake_sender, wake_receiver) = capture_wake(1);
        let pause = Arc::new(Pause {
            observer: thread::current(),
            waiting: OnceLock::new(),
            released: AtomicBool::new(false),
        });
        let armed = Arc::new(AtomicBool::new(false));
        let parts = create_local(
            &owner,
            std::future::pending::<()>(),
            wake_sender,
            PausedMetadataDrop { armed: Arc::clone(&armed), pause: Arc::clone(&pause) },
            TaskOptions::intrusive_local_and_mpsc(),
        ).unwrap_or_else(|error| panic!("创建原任务（Task）失败：{:?}", error.cause));
        let admission = parts.runnable.admit(&owner).expect("接纳（admission）原任务（Task）");
        assert!(matches!(parts.runnable.run(&owner), Ok(RunOutcome::Pending)));
        parts.control.wake();
        let late = take_captured(&wake_receiver).expect("取得实际任务唤醒（wake-up）交出的可执行句柄（runnable handle）");
        assert_eq!(late.id(), parts.control.id());
        drop(admission);
        drop(owner);
        assert_eq!(parts.control.snapshot().future_drop, FutureDropStatus::Confirmed);
        drop((parts.control, parts.join));
        assert!(matches!(returns.close_after_owner(), QueueCleanupProgress::Complete));

        armed.store(true, Ordering::Release);
        thread::scope(|scope| {
            let resume = ResumeOnDrop(&pause);
            let handle = &handle;
            let publisher = scope.spawn(move || assert!(handle.publish(late).is_ok()));
            wait_until(|| pause.waiting.get().is_some());
            assert_eq!(handle.len(), 1);
            assert!(matches!(returns.close_after_owner(), QueueCleanupProgress::MoreWork));
            assert_eq!(handle.len(), 1);
            drop(resume);
            publisher.join().unwrap();
        });
        assert_eq!(handle.len(), 0);
        assert!(matches!(returns.close_after_owner(), QueueCleanupProgress::Complete));
    }

    /// 通过 ReturnQueueHandle::publish 核对未接纳任务（Task）的交付边界。
    /// 仍可首次轮询（poll）的输入保持原值；原调度接口（Schedule）交出的退出清理值允许发布。
    /// 实际所属域析构（owner-domain drop）失败后，未接纳的清理未确认（CleanupUnconfirmed）不再接收。
    #[test]
    fn test_owner_return_accepts_only_active_cleanup_for_unadmitted_tasks() {
        use pi_async_task::definitions::{ExecutionStatus, TaskPhase, TerminationKind};

        struct FailingFuture;

        impl Future for FailingFuture {
            type Output = ();

            fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
                panic!("不得轮询（poll）未接纳的原异步计算（Future）");
            }
        }

        impl Drop for FailingFuture {
            fn drop(&mut self) {
                std::panic::panic_any("return-unadmitted-cleanup-failure");
            }
        }

        let mut returns;
        let owner = TaskThread::new();
        let parker = ThreadParker::new();
        returns = ReturnQueue::new(
            owner.id(), parker.notifier(), Arc::new(ExecutorState::new()), NonZeroUsize::MIN,
        ).expect("准备原归属返回（owner return）");
        let handle = returns.handle();
        let (sender, received) = capture_wake(1);
        let abandoned = create_local(
            &owner, std::future::pending::<()>(), sender.clone(), (),
            TaskOptions::intrusive_local_and_mpsc(),
        ).unwrap_or_else(|error| panic!("创建原任务（Task）失败：{:?}", error.cause));
        let failed = create_local(
            &owner, FailingFuture, sender, (), TaskOptions::intrusive_local_and_mpsc(),
        ).unwrap_or_else(|error| panic!("创建原任务（Task）失败：{:?}", error.cause));
        assert!(!abandoned.control.snapshot().admitted);
        assert_eq!(abandoned.control.snapshot().phase, TaskPhase::Preparing);
        let unsubmitted = handle.publish(abandoned.runnable).expect_err("拒绝尚可首次轮询（poll）的未接纳任务（Task）");
        assert_eq!(unsubmitted.id(), abandoned.control.id());
        assert_eq!(handle.len(), 0);
        drop(unsubmitted);
        let cleanup = take_captured(&received).expect("原调度接口（Schedule）交出清理持有");
        assert_eq!(cleanup.id(), abandoned.control.id());
        let snapshot = cleanup.snapshot();
        assert!(!snapshot.admitted);
        assert_eq!(snapshot.execution, ExecutionStatus::Exiting);
        assert_eq!(snapshot.termination, Some(TerminationKind::Abandoned));
        assert!(handle.publish(cleanup).is_ok());
        assert_eq!(handle.len(), 1);

        let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(owner)))
            .expect_err("所属域析构（owner-domain drop）应传递原恐慌（panic）");
        assert_eq!(failure.downcast_ref::<&str>(), Some(&"return-unadmitted-cleanup-failure"));
        assert_eq!(abandoned.control.snapshot().future_drop, FutureDropStatus::Confirmed);
        let snapshot = failed.runnable.snapshot();
        assert!(!snapshot.admitted);
        assert_eq!(snapshot.phase, TaskPhase::Exiting);
        assert_eq!(snapshot.execution, ExecutionStatus::CleanupUnconfirmed);
        assert!(snapshot.termination.is_some());
        let rejected = handle.publish(failed.runnable).expect_err("清理未确认（CleanupUnconfirmed）不是仍在退出的持有");
        assert_eq!(rejected.id(), failed.control.id());
        assert_eq!(handle.len(), 1);
        drop(rejected);
        assert!(matches!(returns.close_after_owner(), super::QueueCleanupProgress::Complete));
        assert_eq!(handle.len(), 0);
    }
}
