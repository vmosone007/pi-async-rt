fn allocate_queue_id() -> pi_result::RawResult<NonZeroU64, TimerQueueError> {
    use std::sync::atomic::Ordering::Relaxed;
    let mut candidate = NEXT_QUEUE_ID.load(Relaxed);
    loop {
        if candidate == u64::MAX {
            return Err(TimerQueueError::KeyExhausted);
        }
        match NEXT_QUEUE_ID.compare_exchange(candidate, candidate + 1, Relaxed, Relaxed) {
            Ok(_) => return Ok(NonZeroU64::new(candidate).expect("来源身份（queue identity）从1开始且不复用")),
            Err(actual) => candidate = actual,
        }
    }
}

fn timer_shared_new_inner<Item: 'static, N: QueueNotifier>(
    queue_id: NonZeroU64, notifier: N, options: CancelTimerOptions,
    stub: Box<TimerRequest<Item>>,
) -> Arc<TimerShared<Item, N>> {
    Arc::new(TimerShared {
        queue: cordyceps::MpscQueue::new_with_stub(stub),
        state: AtomicUsize::new(0),
        publishers: [AtomicU64::new(0), AtomicU64::new(0)],
        queue_id,
        next_registration_id: AtomicU64::new(1),
        notifier,
        options,
    })
}

fn timer_shared_options_inner<Item: 'static, N: QueueNotifier>(shared: &TimerShared<Item, N>) -> CancelTimerOptions {
    shared.options
}

fn timer_shared_uses_notifier_inner<Item: 'static, N: QueueNotifier + PartialEq>(
    shared: &TimerShared<Item, N>,
    expected: &N,
) -> bool {
    // 采用通知类型（notifier type）自己的相等语义；只读比较，保留原通知状态，
    // 不克隆、不分配、不取得取出权限（Consumer）、不调用notify。
    shared.notifier.eq(expected)
}

fn timer_shared_snapshot_inner<Item: 'static, N: QueueNotifier>(shared: &TimerShared<Item, N>) -> TimerSharedSnapshot {
    let state = shared.state.load(std::sync::atomic::Ordering::Acquire);
    TimerSharedSnapshot {
        count: state & COUNT_MASK,
        current_group: state & CURRENT_GROUP != 0,
        stopped: state & STOPPED != 0,
        owner_exited: state & OWNER_EXITED != 0,
        cleanup_requested: state & CLEANUP_REQUESTED != 0,
        exit_confirmed: state & EXIT_CONFIRMED != 0,
    }
}

fn timer_shared_status_inner<Item: 'static, N: QueueNotifier>(shared: &TimerShared<Item, N>) -> TimerQueueStatus {
    let snapshot = shared.snapshot();
    if snapshot.exit_confirmed {
        TimerQueueStatus::LocalExited
    } else if snapshot.stopped {
        TimerQueueStatus::Closing
    } else {
        TimerQueueStatus::Accepting
    }
}

fn timer_shared_len_inner<Item: 'static, N: QueueNotifier>(shared: &TimerShared<Item, N>) -> usize {
    shared.state.load(std::sync::atomic::Ordering::Acquire) & COUNT_MASK
}

fn timer_check_key_inner<Item: 'static, N: QueueNotifier>(
    shared: &TimerShared<Item, N>, key: &CancelTimerKey,
) -> pi_result::RawResult<(), TimerQueueError> {
    if key.queue_id == shared.queue_id {
        Ok(())
    } else {
        Err(TimerQueueError::WrongQueue)
    }
}

fn timer_prepare_submit_inner<Item: Send + 'static, N: QueueNotifier>(
    shared: &TimerShared<Item, N>, deadline: GlobalManualInstant, item: Item,
) -> pi_result::RawResult<Box<TimerRequest<Item>>, TimerSubmitError<Item>> {
    let snapshot = shared.snapshot();
    if snapshot.stopped || snapshot.owner_exited || snapshot.exit_confirmed {
        return Err(TimerSubmitError { item, cause: TimerQueueError::Closed });
    }
    if deadline.duration_since_origin().as_nanos() > u128::from(MAX_DEADLINE_NANOS) {
        return Err(TimerSubmitError { item, cause: TimerQueueError::InvalidTime });
    }
    let key = match shared.next_key() {
        Ok(key) => key,
        Err(cause) => return Err(TimerSubmitError { item, cause }),
    };
    // 安全（safety）：只分配准确非零布局（layout）；检查空指针后才移动原值（original value）。
    let raw = unsafe { std::alloc::alloc(std::alloc::Layout::new::<TimerRequest<Item>>()) }.cast::<TimerRequest<Item>>();
    let Some(ptr) = NonNull::new(raw) else {
        return Err(TimerSubmitError { item, cause: TimerQueueError::Backend(
            pi_result::Error::new(pi_result::ErrorKind::ResourceExhausted)
                .attach("定时提交节点（timer submission node）分配失败")) });
    };
    // 安全（safety）：本地址独立拥有、对齐且未初始化；全部字段一次写入后恢复配对Box。
    unsafe {
        ptr.as_ptr().write(TimerRequest { links: Links::new(), body: TimerRequestKind::Submit {
            key, group: false, deadline, value: SendValue::new(item),
        } });
        Ok(Box::from_raw(ptr.as_ptr()))
    }
}

fn timer_prepare_cancel_inner<Item: 'static, N: QueueNotifier>(
    shared: &TimerShared<Item, N>, key: &CancelTimerKey,
) -> pi_result::RawResult<Box<TimerRequest<Item>>, TimerQueueError> {
    let snapshot = shared.snapshot();
    if snapshot.stopped || snapshot.owner_exited || snapshot.exit_confirmed { return Err(TimerQueueError::Closed); }
    shared.check_key(key)?;
    // 安全（safety）：准确非零布局（layout）经空指针检查后全量初始化，最终Box配对释放。
    let raw = unsafe { std::alloc::alloc(std::alloc::Layout::new::<TimerRequest<Item>>()) }.cast::<TimerRequest<Item>>();
    let Some(ptr) = NonNull::new(raw) else {
        return Err(TimerQueueError::Backend(pi_result::Error::new(pi_result::ErrorKind::ResourceExhausted)
            .attach("定时取消节点（timer cancellation node）分配失败")));
    };
    // 安全（safety）：节点独立拥有，不含Item，原始写入不读取未初始化字节。
    unsafe {
        ptr.as_ptr().write(TimerRequest { links: Links::new(), body: TimerRequestKind::Cancel { key: *key } });
        Ok(Box::from_raw(ptr.as_ptr()))
    }
}

fn timer_next_key_inner<Item: 'static, N: QueueNotifier>(shared: &TimerShared<Item, N>) -> pi_result::RawResult<CancelTimerKey, TimerQueueError> {
    use std::sync::atomic::Ordering::Relaxed;
    let mut candidate = shared.next_registration_id.load(Relaxed);
    loop {
        if candidate == u64::MAX {
            return Err(TimerQueueError::KeyExhausted);
        }
        match shared.next_registration_id.compare_exchange(candidate, candidate + 1, Relaxed, Relaxed) {
            Ok(_) => return Ok(CancelTimerKey {
                queue_id: shared.queue_id,
                registration_id: NonZeroU64::new(candidate).expect("登记身份（registration identity）从1开始且不复用"),
            }),
            Err(actual) => candidate = actual,
        }
    }
}

fn timer_admit_submit_inner<Item: 'static, N: QueueNotifier>(shared: &TimerShared<Item, N>, group: bool) -> pi_result::RawResult<(), TimerQueueError> {
    use std::sync::atomic::Ordering::{Acquire, AcqRel};
    let mut state = shared.state.load(Acquire);
    for _ in 0..shared.options.atomic_attempts.get() {
        if state & (STOPPED | OWNER_EXITED | EXIT_CONFIRMED) != 0 { return Err(TimerQueueError::Closed); }
        if (state & CURRENT_GROUP != 0) != group { return Err(TimerQueueError::Busy); }
        let count = state & COUNT_MASK;
        if count == COUNT_MASK { return Err(TimerQueueError::CountOverflow); }
        if count >= NATIVE_CAPACITY { return Err(TimerQueueError::Capacity); }
        match shared.state.compare_exchange(state, state + 1, AcqRel, Acquire) {
            Ok(_) => return Ok(()),
            Err(actual) => state = actual,
        }
    }
    Err(TimerQueueError::Busy)
}

fn timer_admit_cancel_inner<Item: 'static, N: QueueNotifier>(shared: &TimerShared<Item, N>, group: bool) -> pi_result::RawResult<(), TimerQueueError> {
    use std::sync::atomic::Ordering::{Acquire, AcqRel};
    let mut state = shared.state.load(Acquire);
    for _ in 0..shared.options.atomic_attempts.get() {
        if state & (STOPPED | OWNER_EXITED | EXIT_CONFIRMED) != 0 { return Err(TimerQueueError::Closed); }
        if (state & CURRENT_GROUP != 0) != group { return Err(TimerQueueError::Busy); }
        match shared.state.compare_exchange(state, state, AcqRel, Acquire) {
            Ok(_) => return Ok(()),
            Err(actual) => state = actual,
        }
    }
    Err(TimerQueueError::Busy)
}

fn timer_publish_inner<Item: 'static, N: QueueNotifier>(shared: &TimerShared<Item, N>, node: Box<TimerRequest<Item>>) {
    shared.queue.enqueue(node);
}

fn timer_finish_publication_inner<Item: 'static, N: QueueNotifier>(shared: &TimerShared<Item, N>, group: bool) {
    // 只归还守卫仍持有的一份发布参与计数（publisher count）；低位非零，减一保留最高位。
    shared.publishers[usize::from(group)].fetch_sub(1, std::sync::atomic::Ordering::Release);
}

fn timer_release_originals_inner<Item: 'static, N: QueueNotifier>(shared: &TimerShared<Item, N>, count: usize) {
    if count != 0 {
        shared.state.fetch_sub(count, std::sync::atomic::Ordering::Release);
    }
}

fn timer_stop_accepting_inner<Item: 'static, N: QueueNotifier>(shared: &TimerShared<Item, N>) {
    shared.state.fetch_or(STOPPED, std::sync::atomic::Ordering::AcqRel);
}

fn timer_begin_owner_exit_inner<Item: 'static, N: QueueNotifier>(shared: &TimerShared<Item, N>) {
    shared.state.fetch_or(STOPPED | OWNER_EXITED | CLEANUP_REQUESTED, std::sync::atomic::Ordering::AcqRel);
}

fn timer_flip_group_inner<Item: 'static, N: QueueNotifier>(shared: &TimerShared<Item, N>) -> bool {
    use std::sync::atomic::Ordering::{Acquire, AcqRel};
    let mut state = shared.state.load(Acquire);
    loop {
        match shared.state.compare_exchange(state, state ^ CURRENT_GROUP, AcqRel, Acquire) {
            Ok(_) => return state & CURRENT_GROUP != 0,
            Err(actual) => state = actual,
        }
    }
}

fn timer_publishers_finished_inner<Item: 'static, N: QueueNotifier>(shared: &TimerShared<Item, N>, group: bool) -> bool {
    shared.publishers[usize::from(group)].fetch_or(0, std::sync::atomic::Ordering::Acquire) & PUBLISHER_COUNT_MASK == 0
}

fn timer_stage_requests_inner<Item: 'static, N: QueueNotifier>(
    shared: &TimerShared<Item, N>, target: &mut VecDeque<Box<TimerRequest<Item>>>, limit: NonZeroUsize,
) -> pi_result::RawResult<usize, TimerQueueError> {
    target.try_reserve(limit.get()).map_err(|error| TimerQueueError::Backend(
        pi_result::error_stack::Report::new(error).change_context(pi_result::ErrorKind::ResourceExhausted)))?;
    let mut added = 0;
    {
        let Some(consumer) = shared.queue.try_consume() else { return Err(TimerQueueError::Busy); };
        for _ in 0..limit.get() {
            match consumer.try_dequeue() {
                Ok(node) => { target.push_back(node); added += 1; }
                Err(cordyceps::mpsc_queue::TryDequeueError::Empty
                    | cordyceps::mpsc_queue::TryDequeueError::Inconsistent) => break,
                Err(cordyceps::mpsc_queue::TryDequeueError::Busy) => unreachable!("唯一消费者（Consumer）已持有"),
            }
        }
    }
    Ok(added)
}

#[allow(unused_variables)]
fn timer_confirm_open_cleanup_inner<Item: 'static, N: QueueNotifier>(
    shared: &TimerShared<Item, N>,
    target: &mut VecDeque<Box<TimerRequest<Item>>>,
    local_count: usize,
) -> pi_result::RawResult<bool, TimerQueueError> {
    use super::definitions::PUBLISHER_REGISTERED;
    use cordyceps::mpsc_queue::TryDequeueError;
    use std::sync::atomic::Ordering::Acquire;

    // 接收前已登记发布责任。以原接收字的读改写（RMW）确定本次确认范围。
    let state = shared.state.fetch_or(0, Acquire);
    if state & STOPPED != 0 || state & COUNT_MASK != local_count {
        return Ok(false);
    }
    let group = state & CURRENT_GROUP != 0;
    if !shared.publishers_finished(!group) {
        return Ok(false);
    }
    let publishers = &shared.publishers[usize::from(group)];
    let publication = publishers.load(Acquire);
    if publication & PUBLISHER_COUNT_MASK != 0
        || publishers.compare_exchange(publication, 0, Acquire, Acquire).is_err()
    {
        return Ok(false);
    }

    // 原生消费者（Consumer）权限内只移交节点（node），所需空间在此前准备。
    target.try_reserve(1).map_err(|error| TimerQueueError::Backend(
        pi_result::error_stack::Report::new(error)
            .change_context(pi_result::ErrorKind::ResourceExhausted)))?;
    let Some(consumer) = shared.queue.try_consume() else {
        return Ok(false);
    };
    match consumer.try_dequeue() {
        Ok(node) => {
            target.push_back(node);
            Ok(false)
        }
        Err(TryDequeueError::Empty) => {
            // 晚到发布可让原生取出暂返 Empty，即使较早的请求仍在队列（queue）中。
            // 晚到方归还计数后仍保留登记位，因此必须复查该位。
            Ok(publishers.load(Acquire) & PUBLISHER_REGISTERED == 0)
        }
        Err(TryDequeueError::Inconsistent) => Ok(false),
        Err(TryDequeueError::Busy) => unreachable!("唯一消费者（Consumer）已持有"),
    }
}

fn timer_publish_marker_inner<Item: 'static, N: QueueNotifier>(
    shared: &TimerShared<Item, N>, mut marker: Box<TimerRequest<Item>>, old_group: bool,
) {
    assert!(matches!(&marker.body, TimerRequestKind::Stub), "清理终点（clear marker）必须独立且可复用");
    marker.body = TimerRequestKind::EndOfClear { old_group };
    shared.publish(marker);
}

fn timer_confirm_exit_inner<Item: 'static, N: QueueNotifier>(shared: &TimerShared<Item, N>) -> bool {
    use std::sync::atomic::Ordering::{Acquire, AcqRel};
    // 在尝试取得消费者权限（Consumer）前先以 AcqRel 读改写（RMW）登记退出清理请求，
    // 与持权者归还消费者权限（Consumer）后的最新 state 检查建立同步顺序（synchronizes-with）。
    let mut state = shared.state.fetch_or(0, AcqRel);
    loop {
        if state & OWNER_EXITED == 0 || state & EXIT_CONFIRMED != 0 {
            return false;
        }
        match shared.state.compare_exchange(state, state | CLEANUP_REQUESTED, AcqRel, Acquire) {
            Ok(_) => return true,
            Err(actual) => state = actual,
        }
    }
}

fn timer_confirm_closed_inner<Item: 'static, N: QueueNotifier>(
    shared: &TimerShared<Item, N>, target: &mut VecDeque<Box<TimerRequest<Item>>>,
) -> pi_result::RawResult<bool, TimerQueueError> {
    use std::sync::atomic::Ordering::{Acquire, AcqRel};
    target.try_reserve(1).map_err(|error| TimerQueueError::Backend(
        pi_result::error_stack::Report::new(error).change_context(pi_result::ErrorKind::ResourceExhausted)))?;
    let mut state = shared.state.fetch_or(0, Acquire);
    if state & STOPPED == 0 || state & COUNT_MASK != 0
        || !shared.publishers_finished(false) || !shared.publishers_finished(true) {
        return Ok(false);
    }
    let Some(consumer) = shared.queue.try_consume() else { return Err(TimerQueueError::Busy); };
    // 先确认发布者（publisher）结束，再安全取空（safe empty）；反序会漏掉刚完成的控制请求。
    match consumer.try_dequeue() {
        Ok(node) => { target.push_back(node); return Ok(false); }
        Err(cordyceps::mpsc_queue::TryDequeueError::Inconsistent) => return Ok(false),
        Err(cordyceps::mpsc_queue::TryDequeueError::Busy) => unreachable!("唯一消费者（Consumer）已持有"),
        Err(cordyceps::mpsc_queue::TryDequeueError::Empty) => {}
    }
    loop {
        if state & STOPPED == 0 || state & COUNT_MASK != 0 { return Ok(false); }
        match shared.state.compare_exchange(state, state | EXIT_CONFIRMED, AcqRel, Acquire) {
            Ok(_) => return Ok(true),
            Err(actual) => state = actual,
        }
    }
}

fn timer_request_empty_inner<Item: 'static>() -> pi_result::RawResult<Box<TimerRequest<Item>>, TimerQueueError> {
    let layout = std::alloc::Layout::new::<TimerRequest<Item>>();
    // 安全（safety）：布局（layout）非零且准确匹配最终Box；空指针先检查，再初始化全部字段。
    let raw = unsafe { std::alloc::alloc(layout) }.cast::<TimerRequest<Item>>();
    let Some(ptr) = NonNull::new(raw) else {
        return Err(TimerQueueError::Backend(pi_result::Error::new(pi_result::ErrorKind::ResourceExhausted)
            .attach("定时控制节点（timer control node）分配失败")));
    };
    // 安全（safety）：本分配独立拥有且对齐；write不读取旧字节，from_raw只恢复这一份Box。
    unsafe {
        ptr.as_ptr().write(TimerRequest { links: Links::new(), body: TimerRequestKind::Stub });
        Ok(Box::from_raw(ptr.as_ptr()))
    }
}

fn timer_request_group_inner<Item: 'static>(node: &mut TimerRequest<Item>, group: bool) -> CancelTimerKey {
    let TimerRequestKind::Submit { key, group: node_group, .. } = &mut node.body else {
        unreachable!("只有已准备的提交节点（Submit node）可以设置接收组");
    };
    *node_group = group;
    *key
}

fn timer_request_reject_inner<Item: 'static>(node: Box<TimerRequest<Item>>, cause: TimerQueueError) -> TimerSubmitError<Item> {
    let TimerRequest { body, .. } = *node;
    let TimerRequestKind::Submit { value, .. } = body else {
        unreachable!("只有未接收的提交节点（Submit node）可以返还原值");
    };
    TimerSubmitError { item: value.into_inner(), cause }
}

fn timer_request_submit_info_inner<Item: 'static>(node: &TimerRequest<Item>) -> Option<(CancelTimerKey, bool, GlobalManualInstant)> {
    match &node.body {
        TimerRequestKind::Submit { key, group, deadline, .. } => Some((*key, *group, *deadline)),
        _ => None,
    }
}

fn timer_request_take_submit_inner<Item: 'static>(node: &mut TimerRequest<Item>) -> Option<(CancelTimerKey, bool, GlobalManualInstant, Item)> {
    if !matches!(&node.body, TimerRequestKind::Submit { .. }) {
        return None;
    }
    let TimerRequestKind::Submit { key, group, deadline, value } =
        std::mem::replace(&mut node.body, TimerRequestKind::Stub) else { unreachable!() };
    Some((key, group, deadline, value.into_inner()))
}

fn timer_request_cancel_key_inner<Item: 'static>(node: &TimerRequest<Item>) -> Option<CancelTimerKey> {
    match &node.body {
        TimerRequestKind::Cancel { key } => Some(*key),
        _ => None,
    }
}

fn timer_request_take_end_inner<Item: 'static>(node: &mut TimerRequest<Item>) -> Option<bool> {
    let TimerRequestKind::EndOfClear { old_group } = &node.body else { return None; };
    let old_group = *old_group;
    node.body = TimerRequestKind::Stub;
    Some(old_group)
}

fn timer_request_into_ptr_inner<Item: 'static>(node: Box<TimerRequest<Item>>) -> NonNull<TimerRequest<Item>> {
    NonNull::new(Box::into_raw(node)).expect("Box节点（node）地址非空")
}

unsafe fn timer_request_from_ptr_inner<Item: 'static>(ptr: NonNull<TimerRequest<Item>>) -> Box<TimerRequest<Item>> {
    // 安全（safety）：原生Linked已完成安全出队（dequeue），地址来自配对Box::into_raw且仅归还一次。
    unsafe { Box::from_raw(ptr.as_ptr()) }
}

unsafe fn timer_request_links_inner<Item: 'static>(ptr: NonNull<TimerRequest<Item>>) -> NonNull<Links<TimerRequest<Item>>> {
    // 安全（safety）：Linked保证节点存活、固定地址；仅原始投影（raw projection）且不建立整个节点引用。
    unsafe { NonNull::new_unchecked(std::ptr::addr_of_mut!((*ptr.as_ptr()).links)) }
}

fn timer_publication_new_inner<'a, Item: 'static, N: QueueNotifier>(
    shared: &'a TimerShared<Item, N>,
) -> pi_result::RawResult<TimerPublishGuard<'a, Item, N>, TimerQueueError> {
    use super::definitions::PUBLISHER_REGISTERED;
    use std::sync::atomic::Ordering::{Acquire, AcqRel};
    for _ in 0..shared.options.atomic_attempts.get() {
        let state = shared.state.load(Acquire);
        if state & (STOPPED | OWNER_EXITED | EXIT_CONFIRMED) != 0 {
            return Err(TimerQueueError::Closed);
        }
        let group = state & CURRENT_GROUP != 0;
        let publishers = &shared.publishers[usize::from(group)];
        let publication = publishers.load(Acquire);
        if publication & PUBLISHER_COUNT_MASK == PUBLISHER_COUNT_MASK {
            return Err(TimerQueueError::CountOverflow);
        }
        // 低位未满，加一不会进位；同一次原子比较交换（CAS）记录新登记。
        let next = (publication + 1) | PUBLISHER_REGISTERED;
        if publishers.compare_exchange(publication, next, AcqRel, Acquire).is_ok() {
            return Ok(TimerPublishGuard { shared, group, pin_active: true, confirmation_needed: true });
        }
    }
    Err(TimerQueueError::Busy)
}

fn timer_publication_group_inner<Item: 'static, N: QueueNotifier>(guard: &TimerPublishGuard<'_, Item, N>) -> bool {
    guard.group
}

fn timer_publication_release_inner<Item: 'static, N: QueueNotifier>(guard: &mut TimerPublishGuard<'_, Item, N>) {
    if guard.pin_active {
        guard.shared.finish_publication(guard.group);
        guard.pin_active = false;
    }
}

fn timer_shared_guard_new_inner<'a, Item: 'static, N: QueueNotifier>(
    shared: &'a TimerShared<Item, N>,
) -> TimerSharedCleanupGuard<'a, Item, N> {
    TimerSharedCleanupGuard { shared, pending: Vec::new(), fallback: None,
        current_debt: false, unsettled: 0, armed: true }
}

fn timer_shared_guard_step_inner<Item: 'static, N: QueueNotifier>(guard: &mut TimerSharedCleanupGuard<'_, Item, N>) -> TimerSharedCleanupStep {
    use std::sync::atomic::Ordering::{Acquire, AcqRel};
    let limit = guard.shared.options().work_batch_size.get();
    let buffered = guard.pending.try_reserve(limit).is_ok();
    let state = guard.shared.state.fetch_or(0, AcqRel);
    if state & EXIT_CONFIRMED != 0 { return TimerSharedCleanupStep::Complete; }
    let mut added = 0;
    {
        let Some(consumer) = guard.shared.queue.try_consume() else { return TimerSharedCleanupStep::Busy; };
        // 进入消费者权限（Consumer）后先清除旧清理请求位；之后若再变 1，仅可能来自新的外部参与者进入 confirm_exit。
        if guard.shared.state.fetch_and(!CLEANUP_REQUESTED, AcqRel) & EXIT_CONFIRMED != 0 {
            return TimerSharedCleanupStep::Complete;
        }
        for _ in 0..if buffered { limit } else { 1 } {
            match consumer.try_dequeue() {
                Ok(node) => {
                    if buffered { guard.pending.push(node); } else { guard.fallback = Some(node); }
                    added += 1;
                }
                Err(cordyceps::mpsc_queue::TryDequeueError::Empty
                    | cordyceps::mpsc_queue::TryDequeueError::Inconsistent) => break,
                Err(cordyceps::mpsc_queue::TryDequeueError::Busy) => unreachable!("唯一消费者（Consumer）已持有"),
            }
        }
        // 未取到节点（含 Empty 与 Inconsistent）时，仍在消费者权限（Consumer）内复查最新停收与双组发布计数；
        // 先确认无发布者在途再作一次有界重取，安全取空且计数为零才确认 EXIT_CONFIRMED（41.10）。
        if added == 0 {
            let mut current = guard.shared.state.fetch_or(0, Acquire);
            if current & STOPPED != 0
                && guard.shared.publishers_finished(false)
                && guard.shared.publishers_finished(true)
            {
                match consumer.try_dequeue() {
                    Ok(node) => {
                        if buffered { guard.pending.push(node); } else { guard.fallback = Some(node); }
                        added += 1;
                    }
                    Err(cordyceps::mpsc_queue::TryDequeueError::Empty) => {
                        loop {
                            if current & COUNT_MASK != 0 { break; }
                            match guard.shared.state.compare_exchange(current, current | EXIT_CONFIRMED, AcqRel, Acquire) {
                                Ok(_) => return TimerSharedCleanupStep::Complete,
                                Err(actual) => current = actual,
                            }
                        }
                    }
                    Err(cordyceps::mpsc_queue::TryDequeueError::Inconsistent) => {}
                    Err(cordyceps::mpsc_queue::TryDequeueError::Busy) => unreachable!("唯一消费者（Consumer）已持有"),
                }
            }
        }
    }
    let mut released = 0usize;
    while let Some(mut node) = guard.fallback.take().or_else(|| guard.pending.pop()) {
        if let Some((_, _, _, item)) = node.take_submit() {
            guard.current_debt = true;
            // 原值析构（original value drop）始终在Consumer之外；异常交给本份守卫。
            drop(item);
            guard.current_debt = false;
            guard.shared.release_originals(1);
            released += 1;
        }
        drop(node);
    }
    if released != 0 {
        // 本轮在消费者权限（Consumer）外真实扣减了逻辑计数：若另一持权者刚因旧非零计数跳过确认，
        // 此处以 AcqRel CAS 登记接续（不触碰已 EXIT_CONFIRMED 状态），闭合双清理者末项减数与 Busy 竞争。
        let _ = timer_confirm_exit_inner(guard.shared);
    }
    // 归还消费者权限（Consumer）后必须执行 AcqRel 复查：若期间有外部发布者退出并置位 CLEANUP_REQUESTED，
    // 本参与者接续下一轮；否则若后续仍有外部发布者进入 confirm_exit，其 AcqRel 将看见已归还的消费者权限（Consumer）。
    let latest = guard.shared.state.fetch_or(0, AcqRel);
    if latest & EXIT_CONFIRMED != 0 { return TimerSharedCleanupStep::Complete; }
    if added != 0 || latest & CLEANUP_REQUESTED != 0 {
        TimerSharedCleanupStep::Progress
    } else {
        TimerSharedCleanupStep::Deferred
    }
}

fn timer_shared_guard_finish_inner<Item: 'static, N: QueueNotifier>(guard: &mut TimerSharedCleanupGuard<'_, Item, N>) {
    let debt = guard.unsettled + usize::from(guard.current_debt);
    guard.unsettled = 0;
    guard.current_debt = false;
    guard.shared.release_originals(debt);
    assert!(guard.pending.is_empty() && guard.fallback.is_none(), "退出清理（exit cleanup）不能遗弃独立持有");
    guard.armed = false;
}

fn timer_shared_guard_drop_inner<Item: 'static, N: QueueNotifier>(guard: &mut TimerSharedCleanupGuard<'_, Item, N>) {
    if !guard.armed { return; }
    guard.armed = false;
    let debt = guard.unsettled + usize::from(guard.current_debt);
    guard.unsettled = 0;
    guard.current_debt = false;
    guard.shared.release_originals(debt);
    if debt != 0 {
        let _ = timer_confirm_exit_inner(guard.shared);
    }
    loop {
        let node = guard.fallback.take().or_else(|| guard.pending.pop());
        let mut node = match node {
            Some(node) => node,
            None => {
                use std::sync::atomic::Ordering::{Acquire, AcqRel};
                if guard.shared.state.fetch_or(0, AcqRel) & EXIT_CONFIRMED != 0 {
                    return;
                }
                let mut confirmed = false;
                let mut dequeued: Option<Box<TimerRequest<Item>>> = None;
                {
                    let Some(consumer) = guard.shared.queue.try_consume() else { return; };
                    // 与正常路径一致：先清除旧清理请求位，再取出并在权限内复查最新停收与双组发布计数。
                    if guard.shared.state.fetch_and(!CLEANUP_REQUESTED, AcqRel) & EXIT_CONFIRMED != 0 {
                        return;
                    }
                    match consumer.try_dequeue() {
                        Ok(node) => { dequeued = Some(node); }
                        Err(cordyceps::mpsc_queue::TryDequeueError::Busy) => unreachable!("唯一消费者（Consumer）已持有"),
                        Err(cordyceps::mpsc_queue::TryDequeueError::Empty
                            | cordyceps::mpsc_queue::TryDequeueError::Inconsistent) => {
                            let mut current = guard.shared.state.fetch_or(0, Acquire);
                            if current & STOPPED != 0
                                && guard.shared.publishers_finished(false)
                                && guard.shared.publishers_finished(true)
                            {
                                match consumer.try_dequeue() {
                                    Ok(node) => { dequeued = Some(node); }
                                    Err(cordyceps::mpsc_queue::TryDequeueError::Empty) => {
                                        loop {
                                            if current & COUNT_MASK != 0 { break; }
                                            match guard.shared.state.compare_exchange(
                                                current,
                                                current | EXIT_CONFIRMED,
                                                AcqRel,
                                                Acquire,
                                            ) {
                                                Ok(_) => { confirmed = true; break; }
                                                Err(actual) => current = actual,
                                            }
                                        }
                                    }
                                    Err(cordyceps::mpsc_queue::TryDequeueError::Inconsistent) => {}
                                    Err(cordyceps::mpsc_queue::TryDequeueError::Busy) => unreachable!("唯一消费者（Consumer）已持有"),
                                }
                            }
                        }
                    }
                }
                if confirmed {
                    return;
                }
                match dequeued {
                    Some(node) => node,
                    None => {
                        // 归还消费者权限（Consumer）后复查最新 state，闭合异常展开期间与发布者的交接窗口。
                        let latest = guard.shared.state.fetch_or(0, AcqRel);
                        if latest & EXIT_CONFIRMED == 0 && latest & CLEANUP_REQUESTED != 0 {
                            continue;
                        }
                        return;
                    }
                }
            }
        };
        if let Some((_, _, _, item)) = node.take_submit() {
            // 当前原值（original value）只在局部拥有；再次恐慌（panic）保持语言进程边界。
            drop(item);
            guard.shared.release_originals(1);
            let _ = timer_confirm_exit_inner(guard.shared);
        }
        drop(node);
    }
}
