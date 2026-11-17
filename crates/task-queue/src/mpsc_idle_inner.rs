#[allow(unused_variables)]
fn idle_new_inner(options: MpscQueueOptions) -> pi_result::Result<MpscIdleTaskQueue> {
    let stub = MpscEntryHandle::try_new_empty()?;
    let scan = MpscEntryHandle::try_new_empty()?;
    let clear = MpscEntryHandle::try_new_empty()?;
    Ok(MpscIdleTaskQueue {
        shared: Arc::new(IdleShared {
            queue: cordyceps::MpscQueue::new_with_stub(stub),
            state: AtomicUsize::new(0),
            ranges: IdleRanges {
                book: UnsafeCell::new(IdleRangeBook {
                    scan: RangeState::Available(scan),
                    clear: RangeState::Available(clear),
                    order: MarkerOrder::None,
                }),
                pending_normal: AtomicUsize::new(0),
                pending_scan: AtomicUsize::new(0),
                pending_clear: AtomicUsize::new(0),
            },
            options,
        }),
    })
}

#[allow(unused_variables)]
fn idle_handle_inner(this: &MpscIdleTaskQueue) -> MpscIdleTaskQueueHandle {
    MpscIdleTaskQueueHandle { shared: Arc::clone(&this.shared) }
}

#[allow(unused_variables)]
fn idle_clear_inner(this: &mut MpscIdleTaskQueue) -> pi_result::Result<QueueCleanupProgress> {
    let mode = if this.shared.snapshot() & IDLE_STOP != 0 { CleanupMode::Close } else { CleanupMode::Clear };
    match this.shared.step(mode) {
        CleanupStep::Complete => Ok(QueueCleanupProgress::Complete),
        CleanupStep::MoreWork | CleanupStep::Deferred => Ok(QueueCleanupProgress::MoreWork),
        CleanupStep::Busy => Err(pi_result::Error::new(pi_result::ErrorKind::Busy)
            .attach("空闲来源清理（idle source clear）暂未取得唯一消费者（Consumer）")),
    }
}

#[allow(unused_variables)]
fn idle_close_inner(this: &mut MpscIdleTaskQueue) -> pi_result::Result<QueueCleanupProgress> {
    this.shared.stop(false);
    match this.shared.step(CleanupMode::Close) {
        CleanupStep::Complete => Ok(QueueCleanupProgress::Complete),
        CleanupStep::MoreWork | CleanupStep::Deferred => Ok(QueueCleanupProgress::MoreWork),
        CleanupStep::Busy => Err(pi_result::Error::new(pi_result::ErrorKind::Busy)
            .attach("空闲来源关闭（idle source close）已停收，暂未取得唯一消费者（Consumer）")),
    }
}

#[allow(unused_variables)]
fn idle_owner_drop_inner(this: &mut MpscIdleTaskQueue) -> () {
    this.shared.stop(true);
    this.shared.assist();
}

#[allow(unused_variables)]
fn idle_clone_inner(this: &MpscIdleTaskQueueHandle) -> MpscIdleTaskQueueHandle {
    MpscIdleTaskQueueHandle { shared: Arc::clone(&this.shared) }
}

#[allow(unused_variables)]
fn idle_push_inner(
    this: &MpscIdleTaskQueueHandle,
    task: Runnable,
) -> pi_result::RawResult<(), IdleSubmitError> {
    let mut completion = IdleCompletionGuard::new(&this.shared);
    let entry = match MpscEntryHandle::try_from_runnable(task) {
        Ok(entry) => entry,
        Err(task) => return Err(IdleSubmitError {
            task,
            cause: IdleQueueError::Backend(pi_result::Error::new(pi_result::ErrorKind::InvalidInput)
                .attach("原任务（Task）缺少空闲来源所需的MPSC链接（link）")),
        }),
    };
    if let Err(cause) = this.shared.reserve() {
        let task = entry.try_into_runnable().unwrap_or_else(|_| std::process::abort());
        return Err(IdleSubmitError { task, cause });
    }
    completion.arm();
    this.shared.queue.enqueue(entry);
    drop(completion);
    Ok(())
}

#[allow(unused_variables)]
fn idle_take_inner(
    this: &MpscIdleTaskQueueHandle,
    max_tasks: NonZeroUsize,
) -> pi_result::RawResult<IdleTake, IdleQueueError> {
    use std::sync::atomic::Ordering::{Acquire, Relaxed};
    let mut completion = IdleCompletionGuard::new(&this.shared);
    let state = this.shared.snapshot();
    if state & IDLE_STOP != 0 { return Ok(IdleTake::Closed); }
    let budget = this.shared.options.cleanup_batch_size.get();
    let limit = max_tasks.get().min(budget).min(state & IDLE_COUNT_MASK);
    let mut result = Vec::with_capacity(limit);
    let mut cleanup = IdleCleanupGuard::new(&this.shared, false);
    cleanup.prepare(budget);
    let Some(mut consumer) = IdleConsumerGuard::try_new(&this.shared) else { return Ok(IdleTake::Busy); };
    completion.arm();
    let state = this.shared.snapshot();
    if state & IDLE_STOP != 0 { return Ok(IdleTake::Closed); }
    // SAFETY: 本次真实Consumer独占book；Clear一旦活跃，普通出口不接管其原值。
    let clear_active = unsafe {
        !matches!((*this.shared.ranges.book.get()).clear, RangeState::Available(_))
    };
    if clear_active { return Ok(IdleTake::WouldBlock); }
    let mut blocked = false;
    for _ in 0..budget {
        // 使用实际预留上界（reserved bound），不能吸收并发新提交而在Consumer内扩容。
        if result.len() == limit { break; }
        match consumer.take() {
            Ok(IdleNode::Task(task)) => {
                let snapshot = task.snapshot();
                if snapshot.cancellation == pi_async_task::definitions::CancelRequestStatus::Recorded
                    || snapshot.termination.is_some()
                    || matches!(snapshot.execution, pi_async_task::definitions::ExecutionStatus::Exiting
                        | pi_async_task::definitions::ExecutionStatus::FutureDropped
                        | pi_async_task::definitions::ExecutionStatus::Ended
                        | pi_async_task::definitions::ExecutionStatus::CleanupUnconfirmed) {
                    this.shared.ranges.pending_normal.fetch_add(1, Relaxed);
                    cleanup.pending.push_back(PendingIdleTask { task, class: CleanupClass::Normal });
                } else {
                    result.push(task);
                    consumer.handed_off += 1;
                }
            }
            Ok(IdleNode::Endpoint(kind)) => {
                consumer.acknowledge(kind);
            }
            Err(TryDequeueError::Empty) => {
                blocked = this.shared.state.load(Acquire) & IDLE_COUNT_MASK != 0;
                break;
            }
            Err(TryDequeueError::Inconsistent) => { blocked = true; break; }
            Err(TryDequeueError::Busy) => std::process::abort(),
        }
    }
    drop(consumer);
    cleanup.drain();
    drop(cleanup);
    drop(completion);
    if !result.is_empty() { Ok(IdleTake::Ready(result)) }
    else if blocked || this.shared.state.load(Acquire) & IDLE_COUNT_MASK != 0 { Ok(IdleTake::WouldBlock) }
    else { Ok(IdleTake::Empty) }
}

#[allow(unused_variables)]
fn idle_process_inner(this: &MpscIdleTaskQueueHandle) -> pi_result::Result<QueueCleanupProgress> {
    match this.shared.step(CleanupMode::Scan) {
        CleanupStep::Complete => Ok(QueueCleanupProgress::Complete),
        CleanupStep::MoreWork | CleanupStep::Deferred => Ok(QueueCleanupProgress::MoreWork),
        CleanupStep::Busy => Err(pi_result::Error::new(pi_result::ErrorKind::Busy)
            .attach("空闲来源取消检查（idle source cancellation scan）暂未取得唯一消费者（Consumer）")),
    }
}

#[allow(unused_variables)]
fn idle_status_inner(this: &MpscIdleTaskQueueHandle) -> IdleQueueStatus {
    let state = this.shared.state.load(std::sync::atomic::Ordering::Acquire);
    if state & IDLE_CLOSED != 0 { IdleQueueStatus::Closed }
    else if state & IDLE_STOP != 0 { IdleQueueStatus::Closing }
    else { IdleQueueStatus::Accepting }
}

#[allow(unused_variables)]
fn idle_len_inner(this: &MpscIdleTaskQueueHandle) -> usize {
    this.shared.state.load(std::sync::atomic::Ordering::Relaxed) & IDLE_COUNT_MASK
}

#[allow(unused_variables)]
fn idle_empty_inner(this: &MpscIdleTaskQueueHandle) -> bool {
    this.shared.state.load(std::sync::atomic::Ordering::Relaxed) & IDLE_COUNT_MASK == 0
}

#[allow(unused_variables)]
fn idle_reserve_inner(this: &IdleShared) -> pi_result::RawResult<(), IdleQueueError> {
    use std::sync::atomic::Ordering::Relaxed;
    let mut state = this.state.load(Relaxed);
    for _ in 0..this.options.push_attempts.get() {
        if state & IDLE_STOP != 0 { return Err(IdleQueueError::Closed); }
        if state & IDLE_COUNT_MASK == IDLE_COUNT_MASK { return Err(IdleQueueError::CountOverflow); }
        // 开放检查与责任登记（reservation）同次提交，不能先读开放后单独增加数量。
        match this.state.compare_exchange(state, state + 1, Relaxed, Relaxed) {
            Ok(_) => return Ok(()),
            Err(actual) => state = actual,
        }
    }
    Err(IdleQueueError::Busy)
}

#[allow(unused_variables)]
fn idle_state_inner(this: &IdleShared) -> usize {
    this.state.fetch_add(0, std::sync::atomic::Ordering::Acquire)
}

#[allow(unused_variables)]
fn idle_return_inner(this: &IdleShared, n: usize) -> () {
    if n != 0 {
        this.state.fetch_sub(n, std::sync::atomic::Ordering::Release);
    }
}

#[allow(unused_variables)]
fn idle_stop_inner(this: &IdleShared, owner_exit: bool) -> () {
    let flags = if owner_exit { IDLE_STOP | IDLE_OWNER_EXIT | IDLE_REQUEST } else { IDLE_STOP };
    this.state.fetch_or(flags, std::sync::atomic::Ordering::AcqRel);
}

#[allow(unused_variables)]
fn idle_request_inner(this: &IdleShared) -> () {
    this.state.fetch_or(IDLE_REQUEST, std::sync::atomic::Ordering::AcqRel);
}

#[allow(unused_variables)]
fn idle_confirm_inner(this: &IdleShared) -> bool {
    let state = this.state.fetch_or(0, std::sync::atomic::Ordering::AcqRel);
    state & (IDLE_OWNER_EXIT | IDLE_CLOSED) == IDLE_OWNER_EXIT
}

#[allow(unused_variables)]
fn idle_step_inner(this: &IdleShared, mode: CleanupMode) -> CleanupStep {
    use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed};
    let owner_exit = matches!(mode, CleanupMode::OwnerExit);
    let mut completion = if owner_exit { None } else { Some(IdleCompletionGuard::new(this)) };
    let mut cleanup = IdleCleanupGuard::new(this, owner_exit);
    let budget = this.options.cleanup_batch_size.get();
    cleanup.prepare(budget);
    let Some(mut consumer) = IdleConsumerGuard::try_new(this) else {
        cleanup.resume_source = false;
        return CleanupStep::Busy;
    };
    if let Some(completion) = completion.as_mut() { completion.arm(); }
    let state = this.state.fetch_and(!IDLE_REQUEST, AcqRel);
    if state & IDLE_CLOSED != 0 {
        cleanup.resume_source = false;
        return CleanupStep::Complete;
    }
    let closing = state & IDLE_STOP != 0 || matches!(mode, CleanupMode::Close | CleanupMode::OwnerExit);
    let mut endpoint_progress = false;
    let target = if closing {
        endpoint_progress |= consumer.acknowledge(MarkerKind::Scan);
        endpoint_progress |= consumer.acknowledge(MarkerKind::Clear);
        None
    } else {
        // SAFETY: 真实Consumer独占book，局部样本（sample）不跨权限归还。
        let clear_active = unsafe {
            !matches!((*this.ranges.book.get()).clear, RangeState::Available(_))
        };
        if matches!(mode, CleanupMode::Clear) || clear_active {
            if !clear_active { consumer.start_range(MarkerKind::Clear); }
            Some(MarkerKind::Clear)
        } else {
            consumer.start_range(MarkerKind::Scan);
            Some(MarkerKind::Scan)
        }
    };
    let mut result = CleanupStep::MoreWork;
    for _ in 0..budget {
        if let Some(kind) = target {
            // SAFETY: 当前Consumer保持范围状态（range state）的唯一访问权。
            let reached = unsafe {
                let book = &*this.ranges.book.get();
                match kind {
                    MarkerKind::Scan => matches!(book.scan, RangeState::Reached(_)),
                    MarkerKind::Clear => matches!(book.clear, RangeState::Reached(_)),
                }
            };
            if reached {
                if kind == MarkerKind::Clear && matches!(mode, CleanupMode::Scan) {
                    // 共享扫描（scan）只协助，不能确认或复用管理者的固定清理终点（clear endpoint）。
                    if this.ranges.pending_normal.load(Acquire) == 0
                        && this.ranges.pending_scan.load(Acquire) == 0
                        && this.ranges.pending_clear.load(Acquire) == 0 {
                        result = CleanupStep::Complete;
                    } else {
                        result = CleanupStep::Deferred;
                    }
                } else if consumer.acknowledge(kind) {
                    endpoint_progress = true;
                    if kind == MarkerKind::Clear {
                        endpoint_progress |= consumer.acknowledge(MarkerKind::Scan);
                    }
                    result = CleanupStep::Complete;
                } else {
                    result = CleanupStep::Deferred;
                }
                break;
            }
        }
        match consumer.take() {
            Ok(IdleNode::Task(task)) => {
                let discard = if closing || target == Some(MarkerKind::Clear) {
                    true
                } else {
                    let snapshot = task.snapshot();
                    snapshot.cancellation == pi_async_task::definitions::CancelRequestStatus::Recorded
                        || snapshot.termination.is_some()
                        || matches!(snapshot.execution, pi_async_task::definitions::ExecutionStatus::Exiting
                            | pi_async_task::definitions::ExecutionStatus::FutureDropped
                            | pi_async_task::definitions::ExecutionStatus::Ended
                            | pi_async_task::definitions::ExecutionStatus::CleanupUnconfirmed)
                };
                if discard {
                    let class = if closing || target == Some(MarkerKind::Clear) {
                        CleanupClass::Clear
                    } else {
                        CleanupClass::Scan
                    };
                    let pending = match class {
                        CleanupClass::Normal => &this.ranges.pending_normal,
                        CleanupClass::Scan => &this.ranges.pending_scan,
                        CleanupClass::Clear => &this.ranges.pending_clear,
                    };
                    pending.fetch_add(1, Relaxed);
                    cleanup.pending.push_back(PendingIdleTask { task, class });
                } else {
                    consumer.rotate(task);
                }
            }
            Ok(IdleNode::Endpoint(kind)) => {
                endpoint_progress = true;
                if closing || target != Some(kind) {
                    endpoint_progress |= consumer.acknowledge(kind);
                }
            }
            Err(TryDequeueError::Empty) => {
                if closing {
                    endpoint_progress |= consumer.acknowledge(MarkerKind::Scan);
                    endpoint_progress |= consumer.acknowledge(MarkerKind::Clear);
                    // SAFETY: 联合结束证明及CLOSED发布均处于真实Consumer内；不复用旧权限样本。
                    let available = unsafe {
                        let book = &*this.ranges.book.get();
                        matches!(book.scan, RangeState::Available(_))
                            && matches!(book.clear, RangeState::Available(_))
                    };
                    let state = this.snapshot();
                    if available && state & IDLE_STOP != 0 && state & IDLE_COUNT_MASK == 0
                        && this.ranges.pending_normal.load(Acquire) == 0
                        && this.ranges.pending_scan.load(Acquire) == 0
                        && this.ranges.pending_clear.load(Acquire) == 0 {
                        this.state.fetch_or(IDLE_CLOSED, AcqRel);
                        result = CleanupStep::Complete;
                    } else {
                        result = CleanupStep::Deferred;
                    }
                } else {
                    result = CleanupStep::Deferred;
                }
                break;
            }
            Err(TryDequeueError::Inconsistent) => {
                result = CleanupStep::Deferred;
                break;
            }
            Err(TryDequeueError::Busy) => std::process::abort(),
        }
    }
    let processed = !cleanup.pending.is_empty();
    drop(consumer);
    cleanup.drain();
    let progressed = processed || endpoint_progress;
    if owner_exit && progressed { this.request(); }
    // 归还权限、原值处理和记账后再可靠确认；完整结束优先于后来登记的空请求（empty request）。
    let state = this.state.fetch_or(0, AcqRel);
    if state & IDLE_CLOSED != 0 {
        result = CleanupStep::Complete;
    } else if closing && !matches!(result, CleanupStep::Complete)
        && (progressed || state & IDLE_REQUEST != 0) {
        result = CleanupStep::MoreWork;
    }
    cleanup.resume_source = false;
    result
}

#[allow(unused_variables)]
fn idle_consumer_new_inner(shared: &IdleShared) -> Option<IdleConsumerGuard<'_>> {
    shared.queue.try_consume().map(|consumer| IdleConsumerGuard {
        shared,
        consumer: Some(consumer),
        handed_off: 0,
        endpoint: None,
    })
}

#[allow(unused_variables)]
fn idle_consumer_take_inner(
    this: &mut IdleConsumerGuard<'_>,
) -> pi_result::RawResult<IdleNode, TryDequeueError> {
    let entry = this.consumer.as_ref().unwrap_or_else(|| std::process::abort()).try_dequeue()?;
    match entry.try_into_runnable() {
        Ok(task) => Ok(IdleNode::Task(task)),
        Err(endpoint) => {
            // SAFETY: 原生Consumer独占book；安全取回的独立空节点（empty node）按发布顺序唯一对应终点。
            let kind = unsafe {
                let book = &mut *this.shared.ranges.book.get();
                let (kind, next) = match book.order {
                    MarkerOrder::Scan => (MarkerKind::Scan, MarkerOrder::None),
                    MarkerOrder::Clear => (MarkerKind::Clear, MarkerOrder::None),
                    MarkerOrder::ScanThenClear => (MarkerKind::Scan, MarkerOrder::Clear),
                    MarkerOrder::None => std::process::abort(),
                };
                let range = match kind {
                    MarkerKind::Scan => &mut book.scan,
                    MarkerKind::Clear => &mut book.clear,
                };
                if !matches!(range, RangeState::Queued) { std::process::abort(); }
                *range = RangeState::Reached(endpoint);
                book.order = next;
                kind
            };
            Ok(IdleNode::Endpoint(kind))
        }
    }
}

#[allow(unused_variables)]
fn idle_start_range_inner(this: &mut IdleConsumerGuard<'_>, kind: MarkerKind) -> () {
    // SAFETY: 本守卫（guard）仍持真实Consumer；只有book可变，原子欠账（atomic debt）不在此借用内。
    unsafe {
        let book = &mut *this.shared.ranges.book.get();
        let order = match kind {
            MarkerKind::Scan => {
                if !matches!(book.clear, RangeState::Available(_)) { return; }
                if !matches!(book.scan, RangeState::Available(_)) { return; }
                match book.order {
                    MarkerOrder::None => MarkerOrder::Scan,
                    _ => std::process::abort(),
                }
            }
            MarkerKind::Clear => {
                if !matches!(book.clear, RangeState::Available(_)) { return; }
                match book.order {
                    MarkerOrder::None => MarkerOrder::Clear,
                    MarkerOrder::Scan => MarkerOrder::ScanThenClear,
                    _ => std::process::abort(),
                }
            }
        };
        let range = match kind { MarkerKind::Scan => &mut book.scan, MarkerKind::Clear => &mut book.clear };
        let RangeState::Available(endpoint) = std::mem::replace(range, RangeState::Transferring) else {
            std::process::abort();
        };
        this.endpoint = Some((kind, endpoint));
        book.order = order;
    }
    let (_, endpoint) = this.endpoint.take().unwrap_or_else(|| std::process::abort());
    this.shared.queue.enqueue(endpoint);
    // SAFETY: 原生发布（publication）不分配或调用用户代码；终点（endpoint）由容器接管，权限未归还。
    unsafe {
        let book = &mut *this.shared.ranges.book.get();
        let range = match kind { MarkerKind::Scan => &mut book.scan, MarkerKind::Clear => &mut book.clear };
        if !matches!(range, RangeState::Transferring) { std::process::abort(); }
        *range = RangeState::Queued;
    }
}

#[allow(unused_variables)]
fn idle_rotate_inner(this: &mut IdleConsumerGuard<'_>, task: Runnable) -> () {
    let entry = match MpscEntryHandle::try_from_runnable(task) {
        Ok(entry) => entry,
        Err(task) => {
            // 内部不变量（invariant）失效时先归还Consumer，原值随后只在权限外展开清理。
            drop(this.consumer.take());
            let _task = task;
            panic!("原空闲节点（idle node）缺少已验证的MPSC链接（link）");
        }
    };
    this.shared.queue.enqueue(entry);
}

#[allow(unused_variables)]
fn idle_ack_inner(this: &mut IdleConsumerGuard<'_>, kind: MarkerKind) -> bool {
    use std::sync::atomic::Ordering::Acquire;
    let settled = match kind {
        MarkerKind::Scan => this.shared.ranges.pending_scan.load(Acquire) == 0,
        MarkerKind::Clear => this.shared.ranges.pending_normal.load(Acquire) == 0
            && this.shared.ranges.pending_scan.load(Acquire) == 0
            && this.shared.ranges.pending_clear.load(Acquire) == 0,
    };
    if !settled { return false; }
    // SAFETY: 实际Consumer保护book；欠账（debt）确认后只移动同一Reached终点（endpoint）。
    unsafe {
        let book = &mut *this.shared.ranges.book.get();
        let range = match kind { MarkerKind::Scan => &mut book.scan, MarkerKind::Clear => &mut book.clear };
        if !matches!(range, RangeState::Reached(_)) { return false; }
        let RangeState::Reached(endpoint) = std::mem::replace(range, RangeState::Transferring) else {
            std::process::abort();
        };
        *range = RangeState::Available(endpoint);
    }
    true
}

#[allow(unused_variables)]
fn idle_consumer_drop_inner(this: &mut IdleConsumerGuard<'_>) -> () {
    let handed_off = std::mem::replace(&mut this.handed_off, 0);
    this.shared.return_count(handed_off);
    if let Some((kind, endpoint)) = this.endpoint.take() {
        // SAFETY: 未发布终点（endpoint）仍由本守卫（guard）独占持有；原生Consumer尚未归还。
        // 仅借book，不为可在权限外结算的整个IdleRanges建立可写引用（mutable reference）。
        unsafe {
            let book = &mut *this.shared.ranges.book.get();
            match kind {
                MarkerKind::Scan => {
                    if !matches!(book.scan, RangeState::Transferring) { std::process::abort(); }
                    book.scan = RangeState::Available(endpoint);
                    book.order = match book.order {
                        MarkerOrder::Scan => MarkerOrder::None,
                        _ => std::process::abort(),
                    };
                }
                MarkerKind::Clear => {
                    if !matches!(book.clear, RangeState::Transferring) { std::process::abort(); }
                    book.clear = RangeState::Available(endpoint);
                    book.order = match book.order {
                        MarkerOrder::Clear => MarkerOrder::None,
                        MarkerOrder::ScanThenClear => MarkerOrder::Scan,
                        _ => std::process::abort(),
                    };
                }
            }
        }
    }
    drop(this.consumer.take());
}

#[allow(unused_variables)]
fn idle_completion_new_inner(shared: &IdleShared) -> IdleCompletionGuard<'_> {
    IdleCompletionGuard { shared, armed: false }
}

#[allow(unused_variables)]
fn idle_completion_arm_inner(this: &mut IdleCompletionGuard<'_>) -> () {
    this.armed = true;
}

#[allow(unused_variables)]
fn idle_completion_drop_inner(this: &mut IdleCompletionGuard<'_>) -> () {
    if std::mem::replace(&mut this.armed, false) {
        this.shared.confirm_exit();
    }
}

#[allow(unused_variables)]
fn idle_cleanup_new_inner(shared: &IdleShared, resume_source: bool) -> IdleCleanupGuard<'_> {
    IdleCleanupGuard { shared, pending: VecDeque::new(), current: None, resume_source }
}

#[allow(unused_variables)]
fn idle_prepare_inner(this: &mut IdleCleanupGuard<'_>, n: usize) -> () {
    this.pending.reserve(n);
}

#[allow(unused_variables)]
fn idle_drain_inner(this: &mut IdleCleanupGuard<'_>) -> () {
    while let Some(PendingIdleTask { task, class }) = this.pending.pop_front() {
        // 原值先移出并登记两份欠账（debt）；首次析构（drop）失败后只结算，不重试原值。
        this.current = Some(class);
        drop(task);
        this.shared.return_count(1);
        let pending = match class {
            CleanupClass::Normal => &this.shared.ranges.pending_normal,
            CleanupClass::Scan => &this.shared.ranges.pending_scan,
            CleanupClass::Clear => &this.shared.ranges.pending_clear,
        };
        pending.fetch_sub(1, std::sync::atomic::Ordering::Release);
        this.current = None;
    }
}

#[allow(unused_variables)]
fn idle_cleanup_drop_inner(this: &mut IdleCleanupGuard<'_>) -> () {
    use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release};
    if let Some(class) = this.current.take() {
        this.shared.return_count(1);
        let pending = match class {
            CleanupClass::Normal => &this.shared.ranges.pending_normal,
            CleanupClass::Scan => &this.shared.ranges.pending_scan,
            CleanupClass::Clear => &this.shared.ranges.pending_clear,
        };
        pending.fetch_sub(1, Release);
    }
    while let Some(PendingIdleTask { task, class }) = this.pending.pop_front() {
        this.current = Some(class);
        drop(task);
        this.shared.return_count(1);
        let pending = match class {
            CleanupClass::Normal => &this.shared.ranges.pending_normal,
            CleanupClass::Scan => &this.shared.ranges.pending_scan,
            CleanupClass::Clear => &this.shared.ranges.pending_clear,
        };
        pending.fetch_sub(1, Release);
        this.current = None;
    }
    if !std::mem::replace(&mut this.resume_source, false) { return; }
    // 异常备援（unwind fallback）仅首次登记接续请求；内部空重查不能自行制造新进展。
    this.shared.request();
    loop {
        let mut progressed = false;
        let mut task = None;
        let Some(mut consumer) = IdleConsumerGuard::try_new(this.shared) else { return; };
        let state = this.shared.state.fetch_and(!IDLE_REQUEST, AcqRel);
        if state & IDLE_CLOSED != 0 { return; }
        progressed |= consumer.acknowledge(MarkerKind::Scan);
        progressed |= consumer.acknowledge(MarkerKind::Clear);
        let mut empty = false;
        let mut deferred = false;
        match consumer.take() {
            Ok(IdleNode::Task(value)) => {
                this.shared.ranges.pending_clear.fetch_add(1, Relaxed);
                task = Some(value);
            }
            Ok(IdleNode::Endpoint(_)) => {
                progressed = true;
                progressed |= consumer.acknowledge(MarkerKind::Scan);
                progressed |= consumer.acknowledge(MarkerKind::Clear);
            }
            Err(TryDequeueError::Empty) => empty = true,
            Err(TryDequeueError::Inconsistent) => deferred = true,
            Err(TryDequeueError::Busy) => std::process::abort(),
        }
        // SAFETY: 本次真实Consumer独占book；只读终点（endpoint），不借整个IdleRanges。
        let available = unsafe {
            let book = &*this.shared.ranges.book.get();
            matches!(book.scan, RangeState::Available(_))
                && matches!(book.clear, RangeState::Available(_))
        };
        let state = this.shared.snapshot();
        let complete = empty && available && state & IDLE_STOP != 0
            && state & IDLE_COUNT_MASK == 0
            && this.shared.ranges.pending_normal.load(Acquire) == 0
            && this.shared.ranges.pending_scan.load(Acquire) == 0
            && this.shared.ranges.pending_clear.load(Acquire) == 0;
        if complete { this.shared.state.fetch_or(IDLE_CLOSED, AcqRel); }
        drop(consumer);
        if let Some(task) = task {
            this.current = Some(CleanupClass::Clear);
            drop(task);
            this.shared.return_count(1);
            this.shared.ranges.pending_clear.fetch_sub(1, Release);
            this.current = None;
            progressed = true;
        }
        if progressed { this.shared.request(); }
        let state = this.shared.state.fetch_or(0, AcqRel);
        if complete || state & IDLE_CLOSED != 0 { return; }
        if progressed || state & IDLE_REQUEST != 0 { continue; }
        if empty || deferred { return; }
    }
}
