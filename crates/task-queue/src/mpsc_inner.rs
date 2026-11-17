// 同一mpsc模块（module）中各个已声明操作的局部实现位置。

#[allow(unused_variables)]
fn mpsc_options_default_inner() -> MpscQueueOptions {
    MpscQueueOptions {
        cleanup_batch_size: const { NonZeroUsize::new(64).unwrap() },
        push_attempts: const { NonZeroU8::new(3).unwrap() },
    }
}

#[allow(unused_variables)]
fn mpsc_queue_new_inner(options: MpscQueueOptions) -> pi_result::Result<MpscTaskQueue> {
    let stub = MpscEntryHandle::try_new_empty()?;
    let endpoint = MpscEntryHandle::try_new_empty()?;
    Ok(MpscTaskQueue {
        shared: Arc::new(MpscQueueShared {
            queue: cordyceps::MpscQueue::new_with_stub(stub),
            state: AtomicUsize::new(0),
            clear: UnsafeCell::new(MpscClearState::Available(endpoint)),
            options,
        }),
    })
}

#[allow(unused_variables)]
fn mpsc_queue_handle_inner(this: &MpscTaskQueue) -> MpscTaskQueueHandle {
    MpscTaskQueueHandle { shared: Arc::clone(&this.shared) }
}

#[allow(unused_variables)]
fn mpsc_queue_clear_inner(this: &mut MpscTaskQueue) -> pi_result::Result<QueueCleanupProgress> {
    match this.shared.cleanup_step(MpscCleanupMode::Clear) {
        MpscCleanupStep::Complete => Ok(QueueCleanupProgress::Complete),
        MpscCleanupStep::MoreWork | MpscCleanupStep::Deferred => Ok(QueueCleanupProgress::MoreWork),
        MpscCleanupStep::Busy => Err(pi_result::Error::new(pi_result::ErrorKind::Busy)
            .attach("MPSC清理（clear）未取得唯一消费者（Consumer），已有持有者占用")),
    }
}

#[allow(unused_variables)]
fn mpsc_queue_close_inner(this: &mut MpscTaskQueue) -> pi_result::Result<QueueCleanupProgress> {
    this.shared.stop_accepting();
    match this.shared.cleanup_step(MpscCleanupMode::Close) {
        MpscCleanupStep::Complete => Ok(QueueCleanupProgress::Complete),
        MpscCleanupStep::MoreWork | MpscCleanupStep::Deferred => Ok(QueueCleanupProgress::MoreWork),
        MpscCleanupStep::Busy => Err(pi_result::Error::new(pi_result::ErrorKind::Busy)
            .attach("MPSC关闭（close）已永久停收，但未取得唯一消费者（Consumer），已有持有者占用")),
    }
}

#[allow(unused_variables)]
fn mpsc_handle_clone_inner(this: &MpscTaskQueueHandle) -> MpscTaskQueueHandle {
    MpscTaskQueueHandle { shared: Arc::clone(&this.shared) }
}

#[allow(unused_variables)]
fn mpsc_take_one_inner(this: &MpscTaskQueueHandle) -> MpscTake {
    let Some(count) = this.shared.start_take() else { return MpscTake::Closed; };
    if count == 0 { return MpscTake::Empty; }
    let mut completion = MpscCompletionGuard::new(&this.shared);
    let mut result = MpscTake::Empty;
    let Some(mut consumer) = MpscConsumerGuard::try_new(&this.shared) else { return MpscTake::Busy; };
    completion.arm();
    loop {
        match consumer.try_take() {
            Ok(Some(task)) => {
                result = MpscTake::Ready(task);
                consumer.handed_off += 1;
                break;
            }
            Ok(None) => continue,
            Err(TryDequeueError::Empty) => {
                if this.shared.state.load(std::sync::atomic::Ordering::Relaxed) & COUNT_MASK != 0 {
                    result = MpscTake::WouldBlock;
                }
                break;
            }
            Err(TryDequeueError::Inconsistent) => {
                result = MpscTake::WouldBlock;
                break;
            }
            Err(TryDequeueError::Busy) => std::process::abort(),
        }
    }
    drop(consumer);
    // 确认（confirmation）可能帮助关闭并执行用户代码；此时最终结果（result）仍独立保活原持有。
    drop(completion);
    result
}

#[allow(unused_variables)]
fn mpsc_is_empty_inner(this: &MpscTaskQueueHandle) -> bool {
    this.shared.state.load(std::sync::atomic::Ordering::Relaxed) & COUNT_MASK == 0
}

#[allow(unused_variables)]
fn mpsc_push_inner(
    this: &MpscTaskQueueHandle,
    task: Runnable,
) -> pi_result::RawResult<(), ExternalPushError> {
    let mut completion = MpscCompletionGuard::new(&this.shared);
    let entry = match MpscEntryHandle::try_from_runnable(task) {
        Ok(entry) => entry,
        Err(task) => return Err(ExternalPushError { task, cause: ExternalPushErrorKind::LinkMissing }),
    };
    if let Err(cause) = this.shared.try_reserve() {
        let task = entry.try_into_runnable().unwrap_or_else(|_| std::process::abort());
        return Err(ExternalPushError { task, cause });
    }
    this.shared.queue.enqueue(entry);
    completion.arm();
    Ok(())
}

#[allow(unused_variables)]
fn mpsc_take_batch_inner(this: &MpscTaskQueueHandle) -> BatchAttempt {
    let Some(count) = this.shared.start_take() else { return BatchAttempt::Closed; };
    if count == 0 { return BatchAttempt::Empty; }
    let mut completion = MpscCompletionGuard::new(&this.shared);
    let mut batch = crate::external::TaskBatch::from_tasks(VecDeque::with_capacity(count));
    let Some(mut consumer) = MpscConsumerGuard::try_new(&this.shared) else { return BatchAttempt::Busy; };
    completion.arm();
    let mut blocked = false;
    while consumer.handed_off < count {
        match consumer.try_take() {
            Ok(Some(task)) => {
                batch.tasks.push_back(task);
                consumer.handed_off += 1;
            }
            Ok(None) => continue,
            Err(TryDequeueError::Empty) => {
                blocked = this.shared.state.load(std::sync::atomic::Ordering::Relaxed) & COUNT_MASK != 0;
                break;
            }
            Err(TryDequeueError::Inconsistent) => {
                blocked = true;
                break;
            }
            Err(TryDequeueError::Busy) => std::process::abort(),
        }
    }
    drop(consumer);
    drop(completion);
    if !batch.tasks.is_empty() {
        BatchAttempt::Ready(batch)
    } else if blocked {
        BatchAttempt::WouldBlock
    } else {
        BatchAttempt::Empty
    }
}

#[allow(unused_variables)]
fn mpsc_len_inner(this: &MpscTaskQueueHandle) -> usize {
    this.shared.state.load(std::sync::atomic::Ordering::Relaxed) & COUNT_MASK
}

#[allow(unused_variables)]
fn mpsc_reserve_inner(this: &MpscQueueShared) -> pi_result::RawResult<(), ExternalPushErrorKind> {
    use std::sync::atomic::Ordering::Relaxed;
    let mut state = this.state.load(Relaxed);
    for _ in 0..this.options.push_attempts.get() {
        if state & STOP_ACCEPTING != 0 {
            return Err(ExternalPushErrorKind::Closed);
        }
        if state & COUNT_MASK == COUNT_MASK {
            return Err(ExternalPushErrorKind::CountOverflow);
        }
        // 接收登记（reservation）只保管责任；原生发布（publication）另行提供内容可见性。
        match this.state.compare_exchange(state, state + 1, Relaxed, Relaxed) {
            Ok(_) => return Ok(()),
            Err(actual) => state = actual,
        }
    }
    Err(ExternalPushErrorKind::Busy)
}

#[allow(unused_variables)]
fn mpsc_start_take_inner(this: &MpscQueueShared) -> Option<usize> {
    let state = this.state.fetch_or(0, std::sync::atomic::Ordering::Acquire);
    if state & STOP_ACCEPTING != 0 { None } else { Some(state & COUNT_MASK) }
}

#[allow(unused_variables)]
fn mpsc_return_count_inner(this: &MpscQueueShared, count: usize) {
    if count != 0 {
        this.state.fetch_sub(count, std::sync::atomic::Ordering::Release);
    }
}

#[allow(unused_variables)]
fn mpsc_stop_accepting_inner(this: &MpscQueueShared) {
    this.state.fetch_or(STOP_ACCEPTING, std::sync::atomic::Ordering::AcqRel);
}

#[allow(unused_variables)]
fn mpsc_manager_released_inner(this: &MpscQueueShared) {
    this.state.fetch_or(
        STOP_ACCEPTING | MANAGER_RELEASED | CLEANUP_REQUESTED,
        std::sync::atomic::Ordering::AcqRel,
    );
}

#[allow(unused_variables)]
fn mpsc_confirm_exit_inner(this: &MpscQueueShared) -> bool {
    this.state.fetch_or(0, std::sync::atomic::Ordering::AcqRel) & MANAGER_RELEASED != 0
}

#[allow(unused_variables)]
fn mpsc_request_cleanup_inner(this: &MpscQueueShared) {
    this.state.fetch_or(CLEANUP_REQUESTED, std::sync::atomic::Ordering::AcqRel);
}

#[allow(unused_variables)]
fn mpsc_cleanup_step_inner(this: &MpscQueueShared, mode: MpscCleanupMode) -> MpscCleanupStep {
    use std::sync::atomic::Ordering::AcqRel;
    let owner_exit = matches!(mode, MpscCleanupMode::OwnerExit);
    // 普通管理步（management step）的完成确认（completion confirmation）最后退出；OwnerExit不安装递归触发器。
    let mut completion = if owner_exit { None } else { Some(MpscCompletionGuard::new(this)) };
    let mut cleanup = MpscCleanupGuard::new(this, owner_exit);
    let budget = this.options.cleanup_batch_size.get();
    cleanup.prepare(budget);
    let Some(mut consumer) = MpscConsumerGuard::try_new(this) else {
        cleanup.disarm();
        return MpscCleanupStep::Busy;
    };
    if let Some(completion) = completion.as_mut() { completion.arm(); }
    let state = this.state.fetch_and(!CLEANUP_REQUESTED, AcqRel);
    let closing = !matches!(mode, MpscCleanupMode::Clear) || state & STOP_ACCEPTING != 0;
    // 终点进展（endpoint progress）只属于本步；确认false或重复状态（state）不产生通知。
    let mut endpoint_progress = false;
    if !closing {
        if consumer.acknowledge_clear() {
            drop(consumer);
            cleanup.disarm();
            return MpscCleanupStep::Complete;
        }
        consumer.begin_clear();
    } else if state & COUNT_MASK == 0 {
        // 逻辑数量（logical count）零意味着没有其它保护外清理欠账，才可确认旧终点（endpoint）。
        endpoint_progress |= consumer.acknowledge_clear();
    }
    let mut result = MpscCleanupStep::MoreWork;
    for _ in 0..budget {
        if !closing {
            // SAFETY: 当前Consumer独占clear；Reached只结束原固定范围，不读取后续业务值。
            if unsafe { matches!(&*this.clear.get(), MpscClearState::Reached(_)) } {
                if cleanup.pending.is_empty() {
                    endpoint_progress |= consumer.acknowledge_clear();
                    result = MpscCleanupStep::Complete;
                }
                break;
            }
        }
        match consumer.try_take() {
            Ok(Some(task)) => cleanup.pending.push_back(task),
            Ok(None) => {
                endpoint_progress = true;
                if !closing {
                    if cleanup.pending.is_empty() {
                        endpoint_progress |= consumer.acknowledge_clear();
                        result = MpscCleanupStep::Complete;
                    }
                    break;
                }
                if cleanup.pending.is_empty() && this.state.fetch_or(0, AcqRel) & COUNT_MASK == 0 {
                    endpoint_progress |= consumer.acknowledge_clear();
                }
            }
            Err(TryDequeueError::Empty) => {
                // 原生Empty、同一原子数量（atomic count）及终点（endpoint）回收共同证明完成；
                // pending尚未减数，不能在保护内提前宣布本步释放（drop）已经结束。
                // SAFETY: 终点（endpoint）采样仍在唯一Consumer内，引用在此结束。
                let available = unsafe { matches!(&*this.clear.get(), MpscClearState::Available(_)) };
                result = if closing && available && this.state.fetch_or(0, AcqRel) & COUNT_MASK == 0 {
                    MpscCleanupStep::Complete
                } else {
                    MpscCleanupStep::Deferred
                };
                break;
            }
            Err(TryDequeueError::Inconsistent) => {
                result = MpscCleanupStep::Deferred;
                break;
            }
            Err(TryDequeueError::Busy) => std::process::abort(),
        }
    }
    let processed = !cleanup.pending.is_empty();
    drop(consumer);
    cleanup.drain();
    // pending只有正常drain及Release减数后才算真实业务进展（business progress）；
    // 更早的首次异常（first panic）仍交给原CleanupGuard，不提前通知或撤销责任。
    // 此处至通知（notification）没有用户代码（user code）、分配（allocation）或可展开操作。
    let progressed = processed || endpoint_progress;
    if owner_exit && progressed { mpsc_request_cleanup_inner(this); }
    // 停收后的完整结束条件（termination condition）一旦在Consumer内成立，就不会因纯请求（request）失效；
    // 否则两个帮助者（helpers）可互相登记空请求（empty requests）而永不退出。
    // 尚未完成时，实际原值或新请求（request）仍要求接续；Deferred只交已有责任参与者。
    let state = this.state.fetch_or(0, AcqRel);
    if closing && !matches!(result, MpscCleanupStep::Complete)
        && (progressed || state & CLEANUP_REQUESTED != 0) {
        result = MpscCleanupStep::MoreWork;
    }
    cleanup.disarm();
    result
}

#[allow(unused_variables)]
fn mpsc_consumer_new_inner(shared: &MpscQueueShared) -> Option<MpscConsumerGuard<'_>> {
    shared.queue.try_consume().map(|consumer| MpscConsumerGuard {
        shared,
        consumer: Some(consumer),
        handed_off: 0,
        endpoint: None,
    })
}

#[allow(unused_variables)]
fn mpsc_consumer_take_inner(
    this: &mut MpscConsumerGuard<'_>,
) -> pi_result::RawResult<Option<Runnable>, TryDequeueError> {
    let entry = this.consumer.as_ref().unwrap_or_else(|| std::process::abort()).try_dequeue()?;
    match entry.try_into_runnable() {
        Ok(task) => Ok(Some(task)),
        Err(endpoint) => {
            // SAFETY: 原生Consumer独占clear；唯一独立空节点（empty node）只能是已发布的终点（endpoint）。
            // 原生安全取回后才恢复其唯一持有，不借用或恢复任何任务（Task）私有头部（header）。
            unsafe {
                let clear = this.shared.clear.get();
                if !matches!(&*clear, MpscClearState::Queued) { std::process::abort(); }
                clear.write(MpscClearState::Reached(endpoint));
            }
            Ok(None)
        }
    }
}

#[allow(unused_variables)]
fn mpsc_consumer_begin_clear_inner(this: &mut MpscConsumerGuard<'_>) {
    if this.shared.state.load(std::sync::atomic::Ordering::Acquire) & STOP_ACCEPTING != 0 {
        return;
    }
    // SAFETY: 唯一Consumer覆盖本次原始字段（field）访问；终点（endpoint）先进入守卫（guard），
    // 尚未发布的展开（unwinding）路径可恢复Available，不产生第二份持有。
    unsafe {
        let clear = this.shared.clear.get();
        if !matches!(&*clear, MpscClearState::Available(_)) { return; }
        let MpscClearState::Available(endpoint) = clear.replace(MpscClearState::Transferring) else {
            std::process::abort();
        };
        this.endpoint = Some(endpoint);
    }
    this.shared.queue.enqueue(this.endpoint.take().unwrap_or_else(|| std::process::abort()));
    // SAFETY: 原生发布（publication）已接管终点（endpoint），仍持Consumer且无clear引用存活。
    unsafe { this.shared.clear.get().write(MpscClearState::Queued); }
}

#[allow(unused_variables)]
fn mpsc_consumer_acknowledge_inner(this: &mut MpscConsumerGuard<'_>) -> bool {
    // SAFETY: 调用者已结束当前范围的保护外清理（cleanup），当前Consumer独占clear。
    // 仅移动同一终点（endpoint）；临时状态（state）在本无回调（callback）区间内消除。
    unsafe {
        let clear = this.shared.clear.get();
        if !matches!(&*clear, MpscClearState::Reached(_)) { return false; }
        let MpscClearState::Reached(endpoint) = clear.replace(MpscClearState::Transferring) else {
            std::process::abort();
        };
        clear.write(MpscClearState::Available(endpoint));
    }
    true
}

#[allow(unused_variables)]
fn mpsc_consumer_drop_inner(this: &mut MpscConsumerGuard<'_>) {
    if let Some(endpoint) = this.endpoint.take() {
        // SAFETY: 本守卫（guard）仍持有唯一Consumer；临时终点（endpoint）未交给原生容器（container）。
        // 只在原始位置恢复同一持有，不让清理状态（clear state）的借用跨越权限归还。
        unsafe {
            let clear = this.shared.clear.get();
            if !matches!(&*clear, MpscClearState::Transferring) { std::process::abort(); }
            clear.write(MpscClearState::Available(endpoint));
        }
    }
    let handed_off = std::mem::replace(&mut this.handed_off, 0);
    this.shared.return_count(handed_off);
    drop(this.consumer.take());
}

#[allow(unused_variables)]
fn mpsc_completion_new_inner(shared: &MpscQueueShared) -> MpscCompletionGuard<'_> {
    MpscCompletionGuard { shared, armed: false }
}

#[allow(unused_variables)]
fn mpsc_completion_arm_inner(this: &mut MpscCompletionGuard<'_>) {
    this.armed = true;
}

#[allow(unused_variables)]
fn mpsc_completion_drop_inner(this: &mut MpscCompletionGuard<'_>) {
    if this.armed {
        this.armed = false;
        this.shared.confirm_exit();
    }
}

#[allow(unused_variables)]
fn mpsc_cleanup_new_inner(
    shared: &MpscQueueShared,
    resume_source: bool,
) -> MpscCleanupGuard<'_> {
    MpscCleanupGuard {
        shared,
        pending: VecDeque::new(),
        current: false,
        resume_source,
    }
}

#[allow(unused_variables)]
fn mpsc_cleanup_prepare_inner(this: &mut MpscCleanupGuard<'_>, capacity: usize) {
    this.pending.reserve(capacity);
}

#[allow(unused_variables)]
fn mpsc_cleanup_drain_inner(this: &mut MpscCleanupGuard<'_>) {
    while let Some(task) = this.pending.pop_front() {
        // 原值先移出，保留一次欠账（count debt）；恐慌（panic）后的守卫（guard）不重试该原值。
        this.current = true;
        drop(task);
        this.shared.return_count(1);
        this.current = false;
    }
}

#[allow(unused_variables)]
fn mpsc_cleanup_disarm_inner(this: &mut MpscCleanupGuard<'_>) {
    this.resume_source = false;
}

#[allow(unused_variables)]
fn mpsc_cleanup_drop_inner(this: &mut MpscCleanupGuard<'_>) {
    use std::sync::atomic::Ordering::AcqRel;
    if this.current {
        this.current = false;
        this.shared.return_count(1);
    }
    while let Some(task) = this.pending.pop_front() {
        this.current = true;
        drop(task);
        this.shared.return_count(1);
        this.current = false;
    }
    // 仅管理者已退出的异常尾部（exceptional tail）由本份备援承担；正常步骤（step）显式交回外层循环。
    // 前段欠账（count debt）及pending处理完后仅首次登记；纯请求重查（request-only recheck）不再制造请求。
    if this.resume_source { mpsc_request_cleanup_inner(this.shared); }
    while this.resume_source {
        let mut progressed = false;
        let mut task = None;
        let Some(mut consumer) = MpscConsumerGuard::try_new(this.shared) else {
            // 请求先于争用；实际Consumer的归还后确认（exit confirmation）承担接续。
            this.resume_source = false;
            break;
        };
        let state = this.shared.state.fetch_and(!CLEANUP_REQUESTED, AcqRel);
        if state & COUNT_MASK == 0 { progressed |= consumer.acknowledge_clear(); }
        let mut empty = false;
        let mut deferred = false;
        match consumer.try_take() {
            Ok(Some(value)) => task = Some(value),
            Ok(None) => {
                progressed = true;
                if this.shared.state.fetch_or(0, AcqRel) & COUNT_MASK == 0 {
                    progressed |= consumer.acknowledge_clear();
                }
            }
            Err(TryDequeueError::Empty) => empty = true,
            Err(TryDequeueError::Inconsistent) => deferred = true,
            Err(TryDequeueError::Busy) => std::process::abort(),
        }
        // SAFETY: 仍持唯一Consumer，仅采样终点（endpoint）是否已安全回收；引用在归还权限前结束。
        let endpoint_available = unsafe {
            matches!(&*this.shared.clear.get(), MpscClearState::Available(_))
        };
        drop(consumer);
        if let Some(task) = task {
            this.current = true;
            drop(task);
            this.shared.return_count(1);
            this.current = false;
            progressed = true;
        }
        // 终点进展（endpoint progress）和原值处理后的Release减数只通知本步一次；
        // Consumer已归还，减数至通知（notification）之间没有用户代码（user code）或可展开操作。
        if progressed { mpsc_request_cleanup_inner(this.shared); }
        // 不以普通读取（load）代替退出确认（exit confirmation）；完整结束条件（termination condition）
        // 优先于纯请求（request），避免异常备援（unwind fallback）与另一帮助者（helper）互相空转。
        // 尚未完成时，新请求（request）仍要求再处理，不能把已退出的参与者误当仍在途（in-flight）。
        let state = this.shared.state.fetch_or(0, AcqRel);
        if empty && endpoint_available && state & COUNT_MASK == 0 {
            this.resume_source = false;
        } else if progressed || state & CLEANUP_REQUESTED != 0 {
            continue;
        } else if empty || deferred {
            // 原生发布缺口（publication gap）或保护外清理仍由登记过的参与者负责，其退出会重新登记请求。
            this.resume_source = false;
        }
    }
}
