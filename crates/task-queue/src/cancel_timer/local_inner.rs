fn timer_local_new_inner<Item: 'static>(
    now: GlobalManualInstant, marker: Box<TimerRequest<Item>>,
) -> TimerLocalState<Item> {
    TimerLocalState {
        native: pi_cancel_timer::Timer::new((now.duration_since_origin().as_nanos() / 1_000_000) as u64),
        reference: now,
        groups: [
            TimerGroupStorage { entries: IndexMap::new(), due: TimerListEnds::default(), due_len: 0 },
            TimerGroupStorage { entries: IndexMap::new(), due: TimerListEnds::default(), due_len: 0 },
        ],
        cleanup: TimerListEnds::default(),
        staged: VecDeque::new(),
        clear: TimerClearState::Available { marker },
    }
}

fn timer_validate_now_inner<Item: 'static>(local: &TimerLocalState<Item>, now: GlobalManualInstant) -> pi_result::RawResult<(), TimerQueueError> {
    if now < local.reference {
        Err(TimerQueueError::InvalidTime)
    } else {
        Ok(())
    }
}

fn timer_validate_deadline_inner(deadline: GlobalManualInstant) -> pi_result::RawResult<u64, TimerQueueError> {
    let nanos = deadline.duration_since_origin().as_nanos();
    if nanos > u128::from(MAX_DEADLINE_NANOS) {
        return Err(TimerQueueError::InvalidTime);
    }
    // 已校验上界（upper bound），先除后加不会溢出，也不会提前交付。
    Ok((nanos / 1_000_000) as u64 + u64::from(nanos % 1_000_000 != 0))
}

fn timer_reserve_registration_inner<Item: 'static>(local: &mut TimerLocalState<Item>, group: bool) -> pi_result::RawResult<(), TimerQueueError> {
    local.groups[usize::from(group)].entries.try_reserve(1).map_err(|error| {
        TimerQueueError::Backend(pi_result::error_stack::Report::new(error)
            .change_context(pi_result::ErrorKind::ResourceExhausted))
    })
}

fn timer_adopt_front_inner<Item: 'static, N: QueueNotifier>(
    local: &mut TimerLocalState<Item>, shared: &TimerShared<Item, N>,
) -> pi_result::RawResult<bool, TimerQueueError> {
    let Some(node) = local.staged.front() else { return Ok(false); };
    if let Some((key, group, deadline)) = node.submit_info() {
        let deadline_ms = TimerLocalState::<Item>::validate_deadline(deadline)?;
        local.reserve_registration(group)?;
        let id = TaggedRegistrationId { group, registration_id: key.registration_id };
        let due = deadline_ms <= local.native.current_time_ms();
        // 原生准备只持稳定身份（stable identity）；原值（original value）仍在独立节点内。
        let phase = if due { TimerRegistrationPhase::Due { links: TimerLinks {
            prev: local.groups[usize::from(group)].due.tail, next: None,
        } } } else { TimerRegistrationPhase::Native { key: local.native.schedule_at(deadline_ms, id) } };
        let mut node = local.staged.pop_front().expect("已有请求前缀（request prefix）");
        let (_, _, _, item) = node.take_submit().expect("已确认提交节点（Submit node）");
        let storage = &mut local.groups[usize::from(group)];
        let previous = storage.due.tail;
        let replaced = storage.entries.insert(key.registration_id, TimerRegistration { item, phase });
        debug_assert!(replaced.is_none());
        if due {
            if let Some(prev) = previous {
                let TimerRegistrationPhase::Due { links } = &mut storage.entries.get_mut(&prev.registration_id)
                    .expect("到期前项（due predecessor）存在").phase else { unreachable!() };
                links.next = Some(id);
            } else { storage.due.head = Some(id); }
            storage.due.tail = Some(id);
            storage.due_len += 1;
        }
        let old_group = match &local.clear {
            TimerClearState::Available { .. } => None,
            TimerClearState::WaitingPublishers { old_group, .. }
            | TimerClearState::Queued { old_group }
            | TimerClearState::Reached { old_group, .. } => Some(*old_group),
        };
        if old_group == Some(group) || shared.snapshot().stopped { local.mark_cancel(key); }
        return Ok(true);
    }
    if let Some(key) = node.cancel_key() {
        shared.check_key(&key)?;
        local.mark_cancel(key);
        local.staged.pop_front();
        return Ok(true);
    }
    let mut node = local.staged.pop_front().expect("已有控制节点（control node）");
    if let Some(old_group) = node.take_end_of_clear() {
        assert!(matches!(&local.clear, TimerClearState::Queued { old_group: expected } if *expected == old_group),
            "清理终点（clear marker）必须对应当前范围");
        local.clear = TimerClearState::Reached { old_group, marker: node };
    }
    Ok(true)
}

fn timer_discover_due_inner<Item: 'static, N: QueueNotifier>(
    local: &mut TimerLocalState<Item>, shared: &TimerShared<Item, N>, target: GlobalManualInstant,
) -> pi_result::RawResult<bool, TimerQueueError> {
    let target_ms = (target.duration_since_origin().as_nanos() / 1_000_000) as u64;
    let options = shared.options();
    if local.native.count() == 0 {
        // 原生关联（native association）与Native阶段成对，没有尚未完成的转换（transition）。
        if local.native.current_time_ms() != target_ms { local.native = pi_cancel_timer::Timer::new(target_ms); }
        return Ok(true);
    }
    for _ in 0..options.work_batch_size.get() {
        let step = local.native.current_time_ms().saturating_add(u64::from(options.advance_ticks.get())).min(target_ms);
        match local.native.pop(step) {
            Some(id) => {
                let group = usize::from(id.group);
                let old = match &local.clear {
                    TimerClearState::Available { .. } => None,
                    TimerClearState::WaitingPublishers { old_group, .. }
                    | TimerClearState::Queued { old_group }
                    | TimerClearState::Reached { old_group, .. } => Some(*old_group),
                };
                let discard = old == Some(id.group) || shared.snapshot().stopped;
                let tail = if discard { local.cleanup.tail } else { local.groups[group].due.tail };
                let links = TimerLinks { prev: tail, next: None };
                let registration = local.groups[group].entries.get_mut(&id.registration_id)
                    .expect("原生登记（native registration）存在");
                assert!(matches!(&registration.phase, TimerRegistrationPhase::Native { .. }));
                registration.phase = if discard { TimerRegistrationPhase::PendingCleanup { links } }
                    else { TimerRegistrationPhase::Due { links } };
                if let Some(prev) = tail {
                    let previous = &mut local.groups[usize::from(prev.group)].entries.get_mut(&prev.registration_id)
                        .expect("链前项（list predecessor）存在").phase;
                    match previous {
                        TimerRegistrationPhase::PendingCleanup { links } if discard => links.next = Some(id),
                        TimerRegistrationPhase::Due { links } if !discard => links.next = Some(id),
                        _ => unreachable!(),
                    }
                } else if discard { local.cleanup.head = Some(id); }
                else { local.groups[group].due.head = Some(id); }
                if discard { local.cleanup.tail = Some(id); }
                else { local.groups[group].due.tail = Some(id); local.groups[group].due_len += 1; }
                if local.native.count() == 0 {
                    if local.native.current_time_ms() != target_ms { local.native = pi_cancel_timer::Timer::new(target_ms); }
                    return Ok(true);
                }
            }
            None if step == target_ms => return Ok(true),
            None => {}
        }
    }
    Ok(false)
}

fn timer_mark_cancel_inner<Item: 'static>(local: &mut TimerLocalState<Item>, key: CancelTimerKey) {
    let Some(group) = (0..2).find(|&group| local.groups[group].entries.contains_key(&key.registration_id)) else { return; };
    let id = TaggedRegistrationId { group: group != 0, registration_id: key.registration_id };
    match &local.groups[group].entries[&key.registration_id].phase {
        TimerRegistrationPhase::PendingCleanup { .. } => return,
        TimerRegistrationPhase::Native { key } => {
            let native_key = *key;
            let removed = local.native.cancel(native_key);
            debug_assert_eq!(removed, Some(id));
        }
        TimerRegistrationPhase::Due { links } => {
            let links = *links;
            if let Some(prev) = links.prev {
                let TimerRegistrationPhase::Due { links: previous } = &mut local.groups[group].entries
                    .get_mut(&prev.registration_id).expect("到期前项（due predecessor）存在").phase else { unreachable!() };
                previous.next = links.next;
            } else { local.groups[group].due.head = links.next; }
            if let Some(next) = links.next {
                let TimerRegistrationPhase::Due { links: following } = &mut local.groups[group].entries
                    .get_mut(&next.registration_id).expect("到期后项（due successor）存在").phase else { unreachable!() };
                following.prev = links.prev;
            } else { local.groups[group].due.tail = links.prev; }
            local.groups[group].due_len -= 1;
        }
    }
    let previous = local.cleanup.tail;
    local.groups[group].entries.get_mut(&key.registration_id).expect("登记（registration）存在").phase =
        TimerRegistrationPhase::PendingCleanup { links: TimerLinks { prev: previous, next: None } };
    if let Some(prev) = previous {
        let TimerRegistrationPhase::PendingCleanup { links } = &mut local.groups[usize::from(prev.group)].entries
            .get_mut(&prev.registration_id).expect("清理前项（cleanup predecessor）存在").phase else { unreachable!() };
        links.next = Some(id);
    } else { local.cleanup.head = Some(id); }
    local.cleanup.tail = Some(id);
}

fn timer_clear_step_inner<Item: 'static, N: QueueNotifier>(
    local: &mut TimerLocalState<Item>, shared: &TimerShared<Item, N>,
) -> pi_result::RawResult<QueueCleanupProgress, TimerQueueError> {
    if matches!(&local.clear, TimerClearState::Available { .. }) {
        let current = shared.snapshot().current_group;
        assert!(local.groups[usize::from(!current)].entries.is_empty(), "复用组（reused group）必须已清空");
        let old_group = shared.flip_group();
        // 仅无用户代码窄区间转移marker，任何可退出操作之前恢复准确归属（ownership）。
        let TimerClearState::Available { marker } = std::mem::replace(&mut local.clear,
            TimerClearState::Queued { old_group }) else { unreachable!() };
        local.clear = TimerClearState::WaitingPublishers { old_group, marker };
    }
    if let TimerClearState::WaitingPublishers { old_group, .. } = &local.clear {
        let old_group = *old_group;
        if !shared.publishers_finished(old_group) { return Ok(QueueCleanupProgress::MoreWork); }
        let TimerClearState::WaitingPublishers { marker, .. } = std::mem::replace(&mut local.clear,
            TimerClearState::Queued { old_group }) else { unreachable!() };
        shared.publish_clear_marker(marker, old_group);
    }
    let limit = shared.options().work_batch_size.get();
    if matches!(&local.clear, TimerClearState::Queued { .. }) {
        let mut attempts = 0;
        while attempts < limit {
            if local.staged.is_empty() {
                let remaining = std::num::NonZeroUsize::new(limit - attempts).expect("剩余请求额度（request budget）非零");
                if shared.try_stage_requests(&mut local.staged, remaining)? == 0 { break; }
            }
            attempts += 1;
            local.adopt_front(shared)?;
            if matches!(&local.clear, TimerClearState::Reached { .. }) { break; }
        }
    }
    let old_group = match &local.clear {
        TimerClearState::WaitingPublishers { old_group, .. }
        | TimerClearState::Queued { old_group }
        | TimerClearState::Reached { old_group, .. } => *old_group,
        TimerClearState::Available { .. } => unreachable!(),
    };
    let group = usize::from(old_group);
    for index in 0..limit.min(local.groups[group].entries.len()) {
        let (&registration_id, registration) = local.groups[group].entries.get_index(index).expect("登记位置（registration index）存在");
        if matches!(&registration.phase, TimerRegistrationPhase::PendingCleanup { .. }) { continue; }
        let id = TaggedRegistrationId { group: old_group, registration_id };
        match &registration.phase {
            TimerRegistrationPhase::Native { key } => {
                let key = *key;
                let removed = local.native.cancel(key);
                debug_assert_eq!(removed, Some(id));
            }
            TimerRegistrationPhase::Due { links } => {
                let links = *links;
                if let Some(prev) = links.prev {
                    let TimerRegistrationPhase::Due { links: previous } = &mut local.groups[group].entries.get_mut(&prev.registration_id)
                        .expect("到期前项（due predecessor）存在").phase else { unreachable!() };
                    previous.next = links.next;
                } else { local.groups[group].due.head = links.next; }
                if let Some(next) = links.next {
                    let TimerRegistrationPhase::Due { links: following } = &mut local.groups[group].entries.get_mut(&next.registration_id)
                        .expect("到期后项（due successor）存在").phase else { unreachable!() };
                    following.prev = links.prev;
                } else { local.groups[group].due.tail = links.prev; }
                local.groups[group].due_len -= 1;
            }
            TimerRegistrationPhase::PendingCleanup { .. } => unreachable!(),
        }
        let tail = local.cleanup.tail;
        local.groups[group].entries.get_mut(&registration_id).expect("登记（registration）存在").phase =
            TimerRegistrationPhase::PendingCleanup { links: TimerLinks { prev: tail, next: None } };
        if let Some(prev) = tail {
            let TimerRegistrationPhase::PendingCleanup { links } = &mut local.groups[usize::from(prev.group)].entries
                .get_mut(&prev.registration_id).expect("清理前项（cleanup predecessor）存在").phase else { unreachable!() };
            links.next = Some(id);
        } else { local.cleanup.head = Some(id); }
        local.cleanup.tail = Some(id);
    }
    if local.groups[group].entries.is_empty() && matches!(&local.clear, TimerClearState::Reached { .. }) {
        let TimerClearState::Reached { marker, .. } = std::mem::replace(&mut local.clear,
            TimerClearState::Queued { old_group }) else { unreachable!() };
        local.clear = TimerClearState::Available { marker };
        return Ok(QueueCleanupProgress::Complete);
    }
    Ok(QueueCleanupProgress::MoreWork)
}

fn timer_local_push_inner<Item: 'static, N: QueueNotifier>(
    cleanup: &mut TimerLocalCleanupGuard<'_, Item, N>, deadline: GlobalManualInstant, item: Item,
) -> pi_result::RawResult<CancelTimerKey, TimerSubmitError<Item>> {
    let deadline_ms = match TimerLocalState::<Item>::validate_deadline(deadline) {
        Ok(value) => value, Err(cause) => return Err(TimerSubmitError { item, cause }),
    };
    let prepared_group = cleanup.shared.snapshot().current_group;
    if let Err(cause) = cleanup.local.reserve_registration(prepared_group) { return Err(TimerSubmitError { item, cause }); }
    let (key, mut publication) = match cleanup.shared.begin_local_submit() {
        Ok(value) => value, Err(cause) => return Err(TimerSubmitError { item, cause }),
    };
    let group = publication.group();
    // 唯一拥有者（unique owner）不会在当前排他调用中切组；共享发布者（publisher）不能切组。
    debug_assert_eq!(group, prepared_group);
    cleanup.current_debt = true;
    let id = TaggedRegistrationId { group, registration_id: key.registration_id };
    let due = deadline_ms <= cleanup.local.native.current_time_ms();
    let tail = cleanup.local.groups[usize::from(group)].due.tail;
    let phase = if due { TimerRegistrationPhase::Due { links: TimerLinks { prev: tail, next: None } } }
        else { TimerRegistrationPhase::Native { key: cleanup.local.native.schedule_at(deadline_ms, id) } };
    let storage = &mut cleanup.local.groups[usize::from(group)];
    let previous = storage.entries.insert(key.registration_id, TimerRegistration { item, phase });
    debug_assert!(previous.is_none());
    if due {
        if let Some(prev) = tail {
            let TimerRegistrationPhase::Due { links } = &mut storage.entries.get_mut(&prev.registration_id)
                .expect("到期前项（due predecessor）存在").phase else { unreachable!() };
            links.next = Some(id);
        } else { storage.due.head = Some(id); }
        storage.due.tail = Some(id);
        storage.due_len += 1;
    }
    // 已安装原值（original value）留在真实表内，通知异常不得再按临时欠账减数。
    cleanup.current_debt = false;
    publication.published();
    Ok(key)
}

// 两组本地登记中找到同一键（key）时直接记录并通知。
// 两组均未找到时，沿同一 TimerShared::request_cancel 发布原请求并返回其结果。
fn timer_local_cancel_inner<Item: 'static, N: QueueNotifier>(
    cleanup: &mut TimerLocalCleanupGuard<'_, Item, N>, key: &CancelTimerKey,
) -> pi_result::RawResult<(), TimerQueueError> {
    cleanup.shared.check_key(key)?;
    let snapshot = cleanup.shared.snapshot();
    if snapshot.stopped || snapshot.owner_exited || snapshot.exit_confirmed { return Err(TimerQueueError::Closed); }
    if !cleanup.local.groups.iter().any(|group| group.entries.contains_key(&key.registration_id)) {
        return cleanup.shared.request_cancel(key);
    }
    cleanup.local.mark_cancel(*key);
    cleanup.shared.notify();
    Ok(())
}

fn timer_local_advance_inner<Item: 'static, N: QueueNotifier>(
    cleanup: &mut TimerLocalCleanupGuard<'_, Item, N>, now: GlobalManualInstant,
) -> pi_result::RawResult<TimerAdvanceProgress, TimerQueueError> {
    cleanup.local.reference = now;
    let target_ms = (now.duration_since_origin().as_nanos() / 1_000_000) as u64;
    if cleanup.local.native.count() == 0 && cleanup.local.native.current_time_ms() != target_ms {
        cleanup.local.native = pi_cancel_timer::Timer::new(target_ms);
    }
    let limit = cleanup.shared.options().work_batch_size.get();
    let mut attempts = 0;
    while attempts < limit {
        if cleanup.local.staged.is_empty() {
            let remaining = std::num::NonZeroUsize::new(limit - attempts).expect("剩余额度（remaining budget）非零");
            if cleanup.shared.try_stage_requests(&mut cleanup.local.staged, remaining)? == 0 { break; }
        }
        attempts += 1;
        cleanup.local.adopt_front(cleanup.shared)?;
    }
    cleanup.local.discover_due(cleanup.shared, now)?;
    if cleanup.local.native.current_time_ms() == target_ms {
        Ok(TimerAdvanceProgress::ReachedTarget)
    } else { Ok(TimerAdvanceProgress::MoreWork) }
}

fn timer_local_take_one_inner<Item: 'static, N: QueueNotifier>(
    cleanup: &mut TimerLocalCleanupGuard<'_, Item, N>,
) -> pi_result::RawResult<TimerTake<Item>, TimerQueueError> {
    if cleanup.shared.snapshot().stopped { return Ok(TimerTake::Closed); }
    let limit = cleanup.shared.options().work_batch_size.get();
    let mut attempts = 0;
    while attempts < limit {
        if cleanup.local.staged.is_empty() {
            let remaining = std::num::NonZeroUsize::new(limit - attempts).expect("剩余额度（remaining budget）非零");
            match cleanup.shared.try_stage_requests(&mut cleanup.local.staged, remaining) {
                Ok(0) => break, Ok(_) => {}, Err(TimerQueueError::Busy) => return Ok(TimerTake::Busy), Err(error) => return Err(error),
            }
        }
        attempts += 1;
        cleanup.local.adopt_front(cleanup.shared)?;
    }
    let complete = cleanup.local.discover_due(cleanup.shared, cleanup.local.reference)?;
    let snapshot = cleanup.shared.snapshot();
    let group = usize::from(snapshot.current_group);
    if let Some(id) = cleanup.local.groups[group].due.head {
        let TimerRegistrationPhase::Due { links } = &cleanup.local.groups[group].entries[&id.registration_id].phase else { unreachable!() };
        let next = links.next;
        cleanup.local.groups[group].due.head = next;
        if let Some(next) = next {
            let TimerRegistrationPhase::Due { links } = &mut cleanup.local.groups[group].entries.get_mut(&next.registration_id)
                .expect("到期后项（due successor）存在").phase else { unreachable!() };
            links.prev = None;
        } else { cleanup.local.groups[group].due.tail = None; }
        cleanup.local.groups[group].due_len -= 1;
        let registration = cleanup.local.groups[group].entries.swap_remove(&id.registration_id).expect("到期登记（due registration）存在");
        let item = registration.item;
        cleanup.shared.release_originals(1);
        return Ok(TimerTake::Ready(item));
    }
    let known = cleanup.local.groups[0].entries.len() + cleanup.local.groups[1].entries.len();
    if complete && cleanup.local.staged.is_empty() && known == snapshot.count {
        Ok(TimerTake::Empty)
    } else { Ok(TimerTake::WouldBlock) }
}

fn timer_prepare_all_inner<Item: 'static, N: QueueNotifier>(
    local: &mut TimerLocalState<Item>, shared: &TimerShared<Item, N>,
) -> pi_result::RawResult<Option<Vec<Item>>, TimerQueueError> {
    let initial = shared.snapshot();
    if initial.stopped { return Err(TimerQueueError::Closed); }
    let target = local.reference;
    let expected = initial.count;
    let initial_known = local.groups[0].entries.len() + local.groups[1].entries.len()
        + local.staged.iter().filter(|node| node.submit_info().is_some()).count();
    let absorb = initial_known < expected;
    if initial_known > expected { return Ok(None); }
    let limit = shared.options().work_batch_size.get();
    let mut attempts = 0;
    loop {
        if local.staged.is_empty() {
            let known = local.groups[0].entries.len() + local.groups[1].entries.len();
            if known == expected || !absorb || attempts == limit { break; }
            let remaining = std::num::NonZeroUsize::new(limit - attempts).expect("剩余额度（remaining budget）非零");
            if shared.try_stage_requests(&mut local.staged, remaining)? == 0 { break; }
        }
        if attempts == limit { break; }
        attempts += 1;
        local.adopt_front(shared)?;
    }
    let discovered = local.discover_due(shared, target)?;
    let adopted = local.groups[0].entries.len() + local.groups[1].entries.len();
    if adopted != expected || local.staged.iter().any(|node| node.submit_info().is_some())
        || !discovered || (absorb && shared.snapshot().count != expected) {
        return Ok(None);
    }
    let mut values = Vec::new();
    values.try_reserve(local.groups[usize::from(initial.current_group)].due_len).map_err(|error|
        TimerQueueError::Backend(pi_result::error_stack::Report::new(error)
            .change_context(pi_result::ErrorKind::ResourceExhausted)))?;
    Ok(Some(values))
}

fn timer_commit_all_inner<Item: 'static, N: QueueNotifier>(
    cleanup: &mut TimerLocalCleanupGuard<'_, Item, N>, mut values: Vec<Item>,
) -> TimerTake<Vec<Item>> {
    let group = usize::from(cleanup.shared.snapshot().current_group);
    while let Some(id) = cleanup.local.groups[group].due.head {
        let TimerRegistrationPhase::Due { links } = &cleanup.local.groups[group].entries[&id.registration_id].phase else { unreachable!() };
        let next = links.next;
        cleanup.local.groups[group].due.head = next;
        if let Some(next) = next {
            let TimerRegistrationPhase::Due { links } = &mut cleanup.local.groups[group].entries.get_mut(&next.registration_id)
                .expect("到期后项（due successor）存在").phase else { unreachable!() };
            links.prev = None;
        } else { cleanup.local.groups[group].due.tail = None; }
        cleanup.local.groups[group].due_len -= 1;
        let registration = cleanup.local.groups[group].entries.swap_remove(&id.registration_id).expect("到期登记（due registration）存在");
        values.push(registration.item);
        cleanup.unsettled += 1;
        cleanup.settle_current();
    }
    if values.is_empty() { TimerTake::Empty } else { TimerTake::Ready(values) }
}

// 普通开放清理先采用已有暂存的前缀，再以剩余额度最多做一批普通取出。先准备空间，
// 将每次普通取出尝试计入额度。归还消费者（Consumer）后采用请求、清理原值并结清计数。
// 暂存、待清理链及欠账结清后，经 try_confirm_open_cleanup 作最多一次限定非等待检查。
// 取到原请求只暂存并返回 MoreWork，下轮采用。原生 Empty 还须核实期间没有新发布参与。
// 只要来源仍开放，进入本轮时已有固定清理范围的调用即使结清该范围，也返回 MoreWork；
// 下一次普通开放调用才确认。已停收时沿原 finish_progress 及 try_confirm_closed。
fn timer_local_process_cleanup_inner<Item: 'static, N: QueueNotifier>(
    cleanup: &mut TimerLocalCleanupGuard<'_, Item, N>,
) -> pi_result::Result<QueueCleanupProgress> {
    let limit = cleanup.shared.options().work_batch_size.get();
    let continuing_clear = !matches!(&cleanup.local.clear, TimerClearState::Available { .. });
    if continuing_clear {
        cleanup.local.clear_step(cleanup.shared).map_err(TimerQueueError::into_report)?;
    } else {
        let staged = limit.min(cleanup.local.staged.len());
        for _ in 0..staged {
            cleanup.local.adopt_front(cleanup.shared).map_err(TimerQueueError::into_report)?;
        }
        if let Some(remaining) = std::num::NonZeroUsize::new(limit - staged) {
            let taken = cleanup.shared.try_stage_requests(&mut cleanup.local.staged, remaining)
                .map_err(TimerQueueError::into_report)?;
            for _ in 0..taken {
                cleanup.local.adopt_front(cleanup.shared).map_err(TimerQueueError::into_report)?;
            }
        }
    }
    for _ in 0..limit { if !cleanup.drop_one() { break; } }
    if matches!(&cleanup.local.clear, TimerClearState::Reached { old_group, .. }
        if cleanup.local.groups[usize::from(*old_group)].entries.is_empty()) {
        cleanup.local.clear_step(cleanup.shared).map_err(TimerQueueError::into_report)?;
    }
    if cleanup.local.cleanup.head.is_some() || !cleanup.local.staged.is_empty()
        || !matches!(&cleanup.local.clear, TimerClearState::Available { .. })
        || cleanup.current_debt || cleanup.unsettled != 0
    {
        return Ok(QueueCleanupProgress::MoreWork);
    }
    let snapshot = cleanup.shared.snapshot();
    let local_count = cleanup.local.groups[0].entries.len() + cleanup.local.groups[1].entries.len();
    let complete = if snapshot.stopped {
        local_count == snapshot.count && cleanup.local.ready_for_source_exit()
    } else if continuing_clear {
        false
    } else {
        cleanup.shared.try_confirm_open_cleanup(&mut cleanup.local.staged, local_count)
            .map_err(TimerQueueError::into_report)?
    };
    if complete {
        Ok(QueueCleanupProgress::Complete)
    } else {
        Ok(QueueCleanupProgress::MoreWork)
    }
}

fn timer_local_clear_inner<Item: 'static, N: QueueNotifier>(
    cleanup: &mut TimerLocalCleanupGuard<'_, Item, N>,
) -> pi_result::Result<QueueCleanupProgress> {
    let mut progress = cleanup.local.clear_step(cleanup.shared).map_err(TimerQueueError::into_report)?;
    for _ in 0..cleanup.shared.options().work_batch_size.get() { if !cleanup.drop_one() { break; } }
    if matches!(&cleanup.local.clear, TimerClearState::Reached { old_group, .. }
        if cleanup.local.groups[usize::from(*old_group)].entries.is_empty()) {
        progress = cleanup.local.clear_step(cleanup.shared).map_err(TimerQueueError::into_report)?;
    }
    if matches!(progress, QueueCleanupProgress::Complete) && cleanup.shared.snapshot().stopped
        && !cleanup.local.ready_for_source_exit() {
        return Ok(QueueCleanupProgress::MoreWork);
    }
    Ok(progress)
}

fn timer_local_close_inner<Item: 'static, N: QueueNotifier>(
    cleanup: &mut TimerLocalCleanupGuard<'_, Item, N>,
) -> pi_result::Result<QueueCleanupProgress> {
    if cleanup.shared.snapshot().exit_confirmed { return Ok(QueueCleanupProgress::Complete); }
    let mut progress = cleanup.local.clear_step(cleanup.shared).map_err(TimerQueueError::into_report)?;
    for _ in 0..cleanup.shared.options().work_batch_size.get() { if !cleanup.drop_one() { break; } }
    if matches!(&cleanup.local.clear, TimerClearState::Reached { old_group, .. }
        if cleanup.local.groups[usize::from(*old_group)].entries.is_empty()) {
        progress = cleanup.local.clear_step(cleanup.shared).map_err(TimerQueueError::into_report)?;
    }
    if matches!(progress, QueueCleanupProgress::Complete) && cleanup.local.ready_for_source_exit() {
        Ok(QueueCleanupProgress::Complete)
    } else { Ok(QueueCleanupProgress::MoreWork) }
}

fn timer_next_check_inner<Item: 'static, N: QueueNotifier>(
    local: &TimerLocalState<Item>, shared: &TimerShared<Item, N>, now: GlobalManualInstant,
) -> pi_result::RawResult<Option<GlobalManualInstant>, TimerQueueError> {
    let snapshot = shared.snapshot();
    if snapshot.exit_confirmed { return Ok(None); }
    let target_ms = (local.reference.duration_since_origin().as_nanos() / 1_000_000) as u64;
    let known_work = local.groups.iter().any(|group| group.due_len != 0)
        || local.cleanup.head.is_some() || !local.staged.is_empty()
        || local.native.current_time_ms() < target_ms
        || matches!(&local.clear, TimerClearState::Reached { .. });
    if known_work { return Ok(Some(now)); }
    let management = snapshot.stopped || snapshot.cleanup_requested
        || !matches!(&local.clear, TimerClearState::Available { .. });
    let nanos = if snapshot.count != 0 || management { 1_000_000 } else { 10_000_000 };
    Ok(Some(now.try_add(std::time::Duration::from_nanos(nanos)).unwrap_or(GlobalManualInstant::MAX)))
}

fn timer_local_owner_exit_inner<Item: 'static, N: QueueNotifier>(
    cleanup: &mut TimerLocalCleanupGuard<'_, Item, N>,
) {
    while cleanup.drop_one() {}
}

fn timer_local_ready_for_exit_inner<Item: 'static>(local: &TimerLocalState<Item>) -> bool {
    local.groups.iter().all(|group| {
        group.entries.is_empty() && group.due_len == 0
            && group.due.head.is_none() && group.due.tail.is_none()
    }) && local.cleanup.head.is_none() && local.cleanup.tail.is_none()
        && local.staged.is_empty() && local.native.count() == 0
        && matches!(&local.clear, TimerClearState::Available { .. })
}

fn timer_local_guard_new_inner<'a, Item: 'static, N: QueueNotifier>(
    shared: &'a TimerShared<Item, N>, local: &'a mut TimerLocalState<Item>, mode: TimerLocalCleanupMode,
) -> TimerLocalCleanupGuard<'a, Item, N> {
    TimerLocalCleanupGuard { shared, local, mode, current_debt: false, unsettled: 0, armed: true }
}

fn timer_take_cleanup_inner<Item: 'static, N: QueueNotifier>(
    cleanup: &mut TimerLocalCleanupGuard<'_, Item, N>,
) -> Option<Item> {
    if cleanup.local.cleanup.head.is_none() && cleanup.mode == TimerLocalCleanupMode::OwnerExit {
        let candidate = (0..2).find_map(|group| cleanup.local.groups[group].entries.first()
            .map(|(&registration_id, _)| TaggedRegistrationId { group: group != 0, registration_id }));
        if let Some(id) = candidate {
            let group = usize::from(id.group);
            match &cleanup.local.groups[group].entries[&id.registration_id].phase {
                TimerRegistrationPhase::Native { key } => {
                    let key = *key;
                    let removed = cleanup.local.native.cancel(key);
                    debug_assert_eq!(removed, Some(id));
                }
                TimerRegistrationPhase::Due { links } => {
                    let links = *links;
                    if let Some(prev) = links.prev {
                        let TimerRegistrationPhase::Due { links: previous } = &mut cleanup.local.groups[group].entries
                            .get_mut(&prev.registration_id).expect("到期前项（due predecessor）存在").phase else { unreachable!() };
                        previous.next = links.next;
                    } else { cleanup.local.groups[group].due.head = links.next; }
                    if let Some(next) = links.next {
                        let TimerRegistrationPhase::Due { links: following } = &mut cleanup.local.groups[group].entries
                            .get_mut(&next.registration_id).expect("到期后项（due successor）存在").phase else { unreachable!() };
                        following.prev = links.prev;
                    } else { cleanup.local.groups[group].due.tail = links.prev; }
                    cleanup.local.groups[group].due_len -= 1;
                }
                TimerRegistrationPhase::PendingCleanup { .. } => unreachable!("待清理链（cleanup list）保持完整"),
            }
            cleanup.local.groups[group].entries.get_mut(&id.registration_id).expect("登记（registration）存在").phase =
                TimerRegistrationPhase::PendingCleanup { links: TimerLinks::default() };
            cleanup.local.cleanup = TimerListEnds { head: Some(id), tail: Some(id) };
        }
    }
    if let Some(id) = cleanup.local.cleanup.head {
        let group = usize::from(id.group);
        let TimerRegistrationPhase::PendingCleanup { links } = &cleanup.local.groups[group].entries[&id.registration_id].phase
            else { unreachable!() };
        let next = links.next;
        cleanup.local.cleanup.head = next;
        if let Some(next) = next {
            let TimerRegistrationPhase::PendingCleanup { links } = &mut cleanup.local.groups[usize::from(next.group)].entries
                .get_mut(&next.registration_id).expect("清理后项（cleanup successor）存在").phase else { unreachable!() };
            links.prev = None;
        } else { cleanup.local.cleanup.tail = None; }
        let registration = cleanup.local.groups[group].entries.swap_remove(&id.registration_id)
            .expect("清理登记（cleanup registration）存在");
        cleanup.current_debt = true;
        return Some(registration.item);
    }
    if cleanup.mode == TimerLocalCleanupMode::OwnerExit {
        while let Some(mut node) = cleanup.local.staged.pop_front() {
            if let Some((_, _, _, item)) = node.take_submit() {
                cleanup.current_debt = true;
                return Some(item);
            }
            if let Some(old_group) = node.take_end_of_clear() {
                debug_assert!(matches!(cleanup.local.clear, TimerClearState::Queued { old_group: expected } if expected == old_group));
                cleanup.local.clear = TimerClearState::Available { marker: node };
            }
        }
    }
    None
}

fn timer_settle_current_inner<Item: 'static, N: QueueNotifier>(
    cleanup: &mut TimerLocalCleanupGuard<'_, Item, N>,
) {
    let debt = cleanup.unsettled + usize::from(cleanup.current_debt);
    cleanup.unsettled = 0;
    cleanup.current_debt = false;
    cleanup.shared.release_originals(debt);
}

fn timer_local_guard_finish_inner<Item: 'static, N: QueueNotifier>(
    cleanup: &mut TimerLocalCleanupGuard<'_, Item, N>,
) {
    cleanup.settle_current();
    if cleanup.mode == TimerLocalCleanupMode::OwnerExit {
        assert!(cleanup.local.groups.iter().all(|group| group.entries.is_empty())
            && cleanup.local.staged.is_empty() && cleanup.local.native.count() == 0,
            "拥有者退出（owner exit）不能遗弃本地原值（local original values）");
    }
    cleanup.armed = false;
}

fn timer_local_guard_drop_inner<Item: 'static, N: QueueNotifier>(
    cleanup: &mut TimerLocalCleanupGuard<'_, Item, N>,
) {
    if !cleanup.armed { return; }
    cleanup.settle_current();
    if cleanup.mode == TimerLocalCleanupMode::OwnerExit {
        while cleanup.drop_one() {}
    }
    cleanup.armed = false;
}
