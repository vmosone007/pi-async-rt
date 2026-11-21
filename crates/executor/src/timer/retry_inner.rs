#[allow(unused_variables)]
fn retry_handle_drop_inner<W>(this: &mut RetryHandle<W>) -> () {
    if let Some(on_drop) = this.on_drop.take() {
        on_drop(&this.value);
    }
}

#[allow(unused_variables)]
fn retry_begin_close_inner<W: RetryWork>(this: &RetryShared<W>) -> QueueCleanupProgress {
    this.state.fetch_or(RETRY_AUTO_HELP, std::sync::atomic::Ordering::AcqRel);
    if matches!(this.assist(), RetryStep::Complete) {
        QueueCleanupProgress::Complete
    } else {
        QueueCleanupProgress::MoreWork
    }
}

#[allow(unused_variables)]
fn retry_has_work_inner<W: RetryWork>(this: &RetryShared<W>) -> bool {
    this.units.load(std::sync::atomic::Ordering::Acquire) != 0
}

#[allow(unused_variables)]
fn retry_owner_new_inner<W: RetryWork>(
    stub: RetryHandle<W>,
    notifier: ThreadNotifier,
    batch: NonZeroUsize,
) -> RetryOwner<W> {
    RetryOwner {
        shared: Arc::new(RetryShared {
            queue: cordyceps::MpscQueue::new_with_stub(stub),
            state: AtomicU8::new(0),
            units: AtomicUsize::new(0),
            notifier,
            batch,
        }),
    }
}

#[allow(unused_variables)]
fn retry_owner_shared_inner<W: RetryWork>(this: &RetryOwner<W>) -> Arc<RetryShared<W>> {
    Arc::clone(&this.shared)
}

#[allow(unused_variables)]
fn retry_owner_process_inner(step: RetryStep) -> QueueCleanupProgress {
    match step {
        RetryStep::Complete => QueueCleanupProgress::Complete,
        RetryStep::MoreWork | RetryStep::BatchProcessed | RetryStep::Deferred | RetryStep::Busy => {
            QueueCleanupProgress::MoreWork
        }
    }
}

#[allow(unused_variables)]
fn retry_owner_drop_inner<W: RetryWork>(this: &mut RetryOwner<W>) -> () {
    this.shared.state.fetch_or(RETRY_AUTO_HELP | RETRY_REQUEST, std::sync::atomic::Ordering::AcqRel);
    this.shared.assist();
}

#[allow(unused_variables)]
fn retry_publish_inner<W: RetryWork>(
    this: &RetryShared<W>,
    completion: &mut RetryCompletionGuard<'_, W>,
) -> () {
    if this.units.fetch_add(1, std::sync::atomic::Ordering::AcqRel) == usize::MAX {
        std::process::abort();
    }
    completion.armed = true;
}

#[allow(unused_variables)]
fn retry_return_unit_inner<W>(this: &RetryShared<W>) -> ()
where
    W: Linked<Links<W>, Handle = RetryHandle<W>>,
{
    if this.units.fetch_sub(1, std::sync::atomic::Ordering::Release) == 0 {
        std::process::abort();
    }
}

#[allow(unused_variables)]
fn retry_confirm_inner<W: RetryWork>(this: &RetryShared<W>) -> bool {
    this.state.fetch_or(0, std::sync::atomic::Ordering::AcqRel) & RETRY_AUTO_HELP != 0
}

#[allow(unused_variables)]
fn retry_request_inner<W: RetryWork>(this: &RetryShared<W>) -> () {
    this.state.fetch_or(RETRY_REQUEST, std::sync::atomic::Ordering::AcqRel);
}

#[allow(unused_variables)]
fn retry_step_inner<W: RetryWork>(
    this: &RetryShared<W>,
    helping: bool,
    mut completion: Option<&mut RetryCompletionGuard<'_, W>>,
) -> RetryStep {
    use std::sync::atomic::Ordering::AcqRel;
    let mut cleanup = RetryCleanupGuard { shared: this, current: None, resume_tail: helping };
    let mut empty = false;
    let mut busy = false;
    let mut processed = 0;
    let batch = this.batch.get();
    for _ in 0..batch {
        let Some(consumer) = this.queue.try_consume() else {
            busy = true;
            break;
        };
        let consumer = RetryConsumerGuard { consumer: Some(consumer) };
        if let Some(completion) = completion.as_mut() {
            completion.armed = true;
        }
        this.state.fetch_and(!RETRY_REQUEST, AcqRel);
        match consumer.consumer.as_ref().unwrap_or_else(|| std::process::abort()).try_dequeue() {
            Ok(value) => cleanup.current = Some(value),
            Err(cordyceps::mpsc_queue::TryDequeueError::Empty) => empty = true,
            Err(cordyceps::mpsc_queue::TryDequeueError::Inconsistent) => {}
            Err(cordyceps::mpsc_queue::TryDequeueError::Busy) => std::process::abort(),
        }
        drop(consumer);
        let Some(current) = cleanup.current.as_ref() else { break; };
        // 原生链接引用（native link references）已结束；恢复（resume）可安全复用同一节点（node）。
        W::resume(&current.value);
        drop(cleanup.current.take());
        processed += 1;
        if helping {
            this.state.fetch_or(RETRY_REQUEST, AcqRel);
        }
    }
    // 先确认请求（request），再可靠核单位（unit）；完整Empty与零单位优先，避免纯请求互相续造工作。
    let state = this.state.fetch_or(0, AcqRel);
    let units = this.units.fetch_add(0, AcqRel);
    let result = if busy {
        // 已有真实消费者（Consumer）持权并承担归还确认；不能拿自己的请求（request）迫使等待它。
        RetryStep::Busy
    } else if empty && units == 0 {
        RetryStep::Complete
    } else if !helping && processed == batch {
        RetryStep::BatchProcessed
    } else if state & RETRY_REQUEST != 0 || processed != 0 {
        RetryStep::MoreWork
    } else {
        RetryStep::Deferred
    };
    cleanup.resume_tail = false;
    drop(cleanup);
    result
}

#[allow(unused_variables)]
fn retry_consumer_drop_inner<W: RetryWork>(this: &mut RetryConsumerGuard<'_, W>) -> () {
    drop(this.consumer.take());
}

#[allow(unused_variables)]
fn retry_completion_drop_inner<W: RetryWork>(this: &mut RetryCompletionGuard<'_, W>) -> bool {
    std::mem::take(&mut this.armed)
}

#[allow(unused_variables)]
fn retry_cleanup_drop_inner<W: RetryWork>(this: &mut RetryCleanupGuard<'_, W>) -> () {
    use std::sync::atomic::Ordering::AcqRel;
    // 原当前项（current item）可能已开始恢复（resume）；只结算原持有，绝不再次恢复同一项。
    drop(this.current.take());
    if !this.resume_tail {
        return;
    }
    this.resume_tail = false;
    this.shared.state.fetch_or(RETRY_REQUEST, AcqRel);
    loop {
        // 单项备援（single-item fallback）先于消费者守卫（consumer guard）存续；展开先归还原生访问权限。
        let mut current = None;
        let Some(consumer) = this.shared.queue.try_consume() else { return; };
        let consumer = RetryConsumerGuard { consumer: Some(consumer) };
        this.shared.state.fetch_and(!RETRY_REQUEST, AcqRel);
        let empty = match consumer.consumer.as_ref().unwrap_or_else(|| std::process::abort()).try_dequeue() {
            Ok(value) => { current = Some(value); false }
            Err(cordyceps::mpsc_queue::TryDequeueError::Empty) => true,
            Err(cordyceps::mpsc_queue::TryDequeueError::Inconsistent) => false,
            Err(cordyceps::mpsc_queue::TryDequeueError::Busy) => std::process::abort(),
        };
        drop(consumer);
        let progressed = if let Some(current) = current {
            W::resume(&current.value);
            drop(current);
            true
        } else {
            false
        };
        if progressed {
            this.shared.state.fetch_or(RETRY_REQUEST, AcqRel);
        }
        // 完整结束（completion）优先于纯请求（request）；两次可靠读改写（RMW）的顺序不能反转。
        let state = this.shared.state.fetch_or(0, AcqRel);
        let units = this.shared.units.fetch_add(0, AcqRel);
        if empty && units == 0 {
            return;
        }
        if !progressed && state & RETRY_REQUEST == 0 {
            return;
        }
    }
}
