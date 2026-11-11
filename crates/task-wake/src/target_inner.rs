// 当前文件落实配对声明中的局部操作。

#[allow(unused_variables)]
fn target_new_inner<E: ExternalTaskQueue, R: DispatchRecovery>(
    owner: &TaskThread,
    external: E,
    recovery: R,
) -> WakeTarget<E, R> {
    use std::sync::atomic::Ordering;
    let mut key = NEXT_TARGET_KEY.load(Ordering::Relaxed);
    loop {
        let next = key.checked_add(1).unwrap_or_else(|| std::process::abort());
        match NEXT_TARGET_KEY.compare_exchange_weak(key, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => key = actual,
        }
    }
    WakeTarget { shared: Arc::new(Shared {
        owner: owner.id(), thread: std::thread::current().id(), key, external, recovery,
        closed: AtomicBool::new(owner.is_closed()),
    }) }
}

#[allow(unused_variables)]
fn target_clone_inner<E: ExternalTaskQueue, R: DispatchRecovery>(
    target: &WakeTarget<E, R>,
) -> WakeTarget<E, R> {
    WakeTarget { shared: Arc::clone(&target.shared) }
}

#[allow(unused_variables)]
fn target_owner_inner<E: ExternalTaskQueue, R: DispatchRecovery>(
    target: &WakeTarget<E, R>,
) -> TaskOwnerId {
    target.shared.owner
}

#[allow(unused_variables)]
fn target_external_inner<E: ExternalTaskQueue, R: DispatchRecovery>(
    target: &WakeTarget<E, R>,
) -> &E {
    &target.shared.external
}

#[allow(unused_variables)]
fn target_close_inner<E: ExternalTaskQueue, R: DispatchRecovery>(target: &WakeTarget<E, R>) {
    target.shared.closed.store(true, std::sync::atomic::Ordering::Release);
}

#[allow(unused_variables)]
fn target_closed_inner<E: ExternalTaskQueue, R: DispatchRecovery>(
    target: &WakeTarget<E, R>,
) -> bool {
    target.shared.closed.load(std::sync::atomic::Ordering::Acquire)
}

#[allow(unused_variables)]
fn target_bind_inner<'owner, E: ExternalTaskQueue, R: DispatchRecovery>(
    target: &WakeTarget<E, R>,
    owner: &'owner TaskThread,
    queue: Rc<RefCell<TaskQueue>>,
) -> Result<LocalQueueGuard<'owner, E, R>, BindError> {
    if std::thread::current().id() != target.shared.thread { return Err(BindError::WrongThread); }
    if owner.id() != target.shared.owner { return Err(BindError::WrongOwner); }
    if target.is_closed() { return Err(BindError::Closed); }
    {
        let queue_ref = queue.try_borrow().map_err(|_| BindError::QueueBorrowed)?;
        if queue_ref.owner_id() != owner.id() { return Err(BindError::WrongOwner); }
    }
    // 本地队列（TaskQueue）借用已结束；注册表（registry）只保存弱引用（Weak）。
    LOCAL_QUEUES.try_with(|routes| {
        let mut routes = routes.try_borrow_mut().map_err(|_| BindError::QueueBorrowed)?;
        if routes.get(&owner.id()).is_some_and(|route| route.queue.strong_count() != 0) {
            return Err(BindError::AlreadyBound);
        }
        routes.insert(owner.id(), LocalRoute { key: target.shared.key, queue: Rc::downgrade(&queue) });
        Ok(())
    }).map_err(|_| BindError::Closed)??;
    Ok(LocalQueueGuard { target: target.clone(), queue, owner, local: PhantomData })
}

#[allow(unused_variables)]
fn target_submit_inner<E: ExternalTaskQueue, R: DispatchRecovery>(
    target: &WakeTarget<E, R>,
    task: Runnable,
) -> Result<(), DeliveryError> {
    if target.is_closed() { return Err(DeliveryError { task, cause: DispatchFailure::Closed }); }
    let snapshot = task.snapshot();
    if snapshot.owner != Some(target.shared.owner)
        || (!snapshot.admitted
            && !(snapshot.execution == pi_async_task::definitions::ExecutionStatus::Exiting
                && snapshot.termination.is_some())) {
        return Err(DeliveryError { task, cause: DispatchFailure::WrongOwner });
    }
    if std::thread::current().id() == target.shared.thread {
        let route = LOCAL_QUEUES.try_with(|routes| {
            let routes = routes.try_borrow().ok()?;
            let route = routes.get(&target.shared.owner)?;
            Some((route.key, route.queue.clone()))
        }).ok().flatten();
        // 注册表（registry）借用已结束，再校验装配身份（key）及升级弱引用（Weak）。
        if let Some((key, queue)) = route {
            if key == target.shared.key {
                if let Some(queue) = queue.upgrade() {
                    if let Ok(mut queue) = queue.try_borrow_mut() {
                        let result = queue.push_back(task);
                        drop(queue);
                        return result.map_err(|error| DeliveryError { task: error.task, cause: DispatchFailure::Local(error.cause) });
                    }
                }
            }
        }
    }
    target.shared.external.try_push(task)
        .map_err(|error| DeliveryError { task: error.task, cause: DispatchFailure::External(error.cause) })
}

#[allow(unused_variables)]
fn target_waker_inner<E: ExternalTaskQueue, R: DispatchRecovery>(
    target: &WakeTarget<E, R>,
    task: &ControlHandle,
) -> Result<Waker, BindError> {
    if target.is_closed() { return Err(BindError::Closed); }
    if task.snapshot().owner != Some(target.shared.owner) { return Err(BindError::WrongOwner); }
    Ok(task.waker())
}

#[allow(unused_variables)]
fn guard_target_inner<E: ExternalTaskQueue, R: DispatchRecovery>(
    guard: &LocalQueueGuard<'_, E, R>,
) -> WakeTarget<E, R> {
    guard.target.clone()
}

#[allow(unused_variables)]
fn guard_push_front_inner<E: ExternalTaskQueue, R: DispatchRecovery>(
    guard: &mut LocalQueueGuard<'_, E, R>,
    task: Runnable,
) -> Result<(), DeliveryError> {
    if guard.target.is_closed() { return Err(DeliveryError { task, cause: DispatchFailure::Closed }); }
    let snapshot = task.snapshot();
    if snapshot.owner != Some(guard.owner.id()) || !snapshot.admitted {
        return Err(DeliveryError { task, cause: DispatchFailure::WrongOwner });
    }
    let result = {
        let Ok(mut queue) = guard.queue.try_borrow_mut() else {
            return Err(DeliveryError { task, cause: DispatchFailure::LocalBorrowed });
        };
        queue.push_front(task)
    };
    result.map_err(|error| DeliveryError { task: error.task, cause: DispatchFailure::Local(error.cause) })
}

#[allow(unused_variables)]
fn guard_push_back_inner<E: ExternalTaskQueue, R: DispatchRecovery>(
    guard: &mut LocalQueueGuard<'_, E, R>,
    task: Runnable,
) -> Result<(), DeliveryError> {
    if guard.target.is_closed() { return Err(DeliveryError { task, cause: DispatchFailure::Closed }); }
    let snapshot = task.snapshot();
    if snapshot.owner != Some(guard.owner.id()) || !snapshot.admitted {
        return Err(DeliveryError { task, cause: DispatchFailure::WrongOwner });
    }
    let result = {
        let Ok(mut queue) = guard.queue.try_borrow_mut() else {
            return Err(DeliveryError { task, cause: DispatchFailure::LocalBorrowed });
        };
        queue.push_back(task)
    };
    result.map_err(|error| DeliveryError { task: error.task, cause: DispatchFailure::Local(error.cause) })
}

#[allow(unused_variables)]
fn guard_pop_inner<E: ExternalTaskQueue, R: DispatchRecovery>(
    guard: &mut LocalQueueGuard<'_, E, R>,
) -> Result<Option<Runnable>, LocalAccessError> {
    let mut queue = guard.queue.try_borrow_mut().map_err(|_| LocalAccessError::QueueBorrowed)?;
    Ok(queue.pop_front())
}

#[allow(unused_variables)]
fn guard_len_inner<E: ExternalTaskQueue, R: DispatchRecovery>(
    guard: &LocalQueueGuard<'_, E, R>,
) -> Result<usize, LocalAccessError> {
    let queue = guard.queue.try_borrow().map_err(|_| LocalAccessError::QueueBorrowed)?;
    Ok(queue.len())
}

#[allow(unused_variables)]
fn guard_import_inner<E: ExternalTaskQueue, R: DispatchRecovery>(
    guard: &mut LocalQueueGuard<'_, E, R>,
) -> ImportOutcome {
    if guard.target.is_closed() { return ImportOutcome::Closed; }
    match guard.queue.try_borrow_mut() {
        Ok(queue) => drop(queue),
        Err(_) => return ImportOutcome::Busy,
    }
    // 外部接口（external interface）调用时没有本地借用（borrow）；只取一个当前批次（batch）。
    match guard.target.shared.external.try_take_batch() {
        BatchAttempt::Ready(batch) => {
            let result = {
                let Ok(mut queue) = guard.queue.try_borrow_mut() else { return ImportOutcome::LocalBusy(batch); };
                queue.append_batch(batch)
            };
            match result {
                Ok(count) => ImportOutcome::Imported(count),
                Err(error) => ImportOutcome::Rejected(error),
            }
        }
        BatchAttempt::Empty => ImportOutcome::Empty,
        BatchAttempt::WouldBlock => ImportOutcome::WouldBlock,
        BatchAttempt::Busy => ImportOutcome::Busy,
        BatchAttempt::Closed => ImportOutcome::Closed,
    }
}

#[allow(unused_variables)]
fn guard_drop_inner<E: ExternalTaskQueue, R: DispatchRecovery>(
    guard: &mut LocalQueueGuard<'_, E, R>,
) {
    guard.target.close();
    let _ = LOCAL_QUEUES.try_with(|routes| {
        let mut routes = routes.borrow_mut();
        if routes.get(&guard.owner.id()).is_some_and(|route| route.key == guard.target.shared.key) {
            routes.remove(&guard.owner.id());
        }
    });
}
