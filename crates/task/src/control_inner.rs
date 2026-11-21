// 当前文件落实配对声明中的局部操作。

#[allow(unused_variables)]
fn control_id_inner(this: &ControlHandle) -> TaskId {
    this.task.id()
}

#[allow(unused_variables)]
fn control_snapshot_inner(this: &ControlHandle) -> TaskSnapshot {
    this.task.snapshot()
}

#[allow(unused_variables)]
fn control_cancel_inner(this: &ControlHandle) {
    this.task.request_cancel();
}

#[allow(unused_variables)]
fn control_wake_inner(this: &ControlHandle) {
    this.task.wake();
}

#[allow(unused_variables)]
fn control_waker_inner(this: &ControlHandle) -> Waker {
    this.task.waker()
}

#[allow(unused_variables)]
fn control_wait_inner(this: &ControlHandle) -> CancelWait {
    CancelWait { target: this.clone(), external: None, state: CancelWaitState::Created }
}

#[allow(unused_variables)]
fn control_external_wait_inner(this: &ControlHandle, waiter: &ExternalWaiter) -> CancelWait {
    CancelWait { target: this.clone(), external: Some(waiter.clone()), state: CancelWaitState::Created }
}

#[allow(unused_variables)]
fn control_clone_inner(this: &ControlHandle) -> ControlHandle {
    ControlHandle { task: this.task.clone() }
}

#[allow(unused_variables)]
fn external_waiter_new_inner(source: Arc<dyn ExternalWaitSource>) -> ExternalWaiter {
    let id = NEXT_EXTERNAL_WAITER.try_update(std::sync::atomic::Ordering::Relaxed, std::sync::atomic::Ordering::Relaxed,
        |id| id.checked_add(1)).unwrap_or_else(|_| std::process::abort());
    ExternalWaiter { inner: Arc::new(ExternalWaiterState { id: WaiterId::External(id), source }) }
}

#[allow(unused_variables)]
fn cancel_wait_poll_inner(
    this: Pin<&mut CancelWait>,
    cx: &mut Context<'_>,
) -> Poll<CancellationOutcome> {
    let this = this.get_mut();
    if let CancelWaitState::Done(outcome) = this.state { return Poll::Ready(outcome); }
    let actual = crate::task::current_task_id().map(WaiterId::Task).or_else(|| {
        this.external.as_ref().and_then(|external| external.inner.source.is_alive().then_some(external.inner.id))
    });
    let failure = match (&this.state, actual) {
        (CancelWaitState::Waiting { waiter, .. }, actual) if Some(*waiter) != actual => Some(CancellationError::WaiterChanged),
        (_, None) => Some(CancellationError::UnknownWaiter),
        (_, Some(WaiterId::Task(id))) if id == this.target.id() => Some(CancellationError::SelfWait),
        _ => None,
    };
    if let Some(error) = failure {
        if let CancelWaitState::Waiting { registration, .. } = &this.state {
            this.unregister(registration);
        }
        let outcome = CancellationOutcome::Failed(error);
        this.state = CancelWaitState::Done(outcome);
        return Poll::Ready(outcome);
    }
    let waiter = actual.unwrap_or_else(|| std::process::abort());
    if matches!(this.state, CancelWaitState::Created) {
        if let Some(outcome) = this.target.task.cancellation_outcome() {
            this.state = CancelWaitState::Done(outcome);
            return Poll::Ready(outcome);
        }
        let key = NEXT_WAIT_KEY.try_update(std::sync::atomic::Ordering::Relaxed, std::sync::atomic::Ordering::Relaxed,
            |key| key.checked_add(1)).unwrap_or_else(|_| std::process::abort());
        let registration = Arc::new(WaitRegistration {
            key,
            waiter,
            target: this.target.id(),
            result: Arc::new(AtomicU8::new(WAIT_RESULT_ACTIVE)),
            waker_state: AtomicU8::new(WAIT_WAKER_IDLE),
            waker: UnsafeCell::new(None),
        });
        if let Err(error) = WAIT_REGISTRY.insert(&registration) {
            let outcome = CancellationOutcome::Failed(error);
            this.state = CancelWaitState::Done(outcome);
            return Poll::Ready(outcome);
        }
        // 先把退边责任交给等待值（CancelWait），随后任何外部调用（call）展开（unwind）仍可撤销本份登记。
        this.state = CancelWaitState::Waiting { waiter, registration: Arc::clone(&registration) };
        let observer: Arc<dyn CancelObserver> = registration.clone();
        if let Err(error) = this.target.task.register_cancel(key, observer) {
            registration.abandon();
            let outcome = CancellationOutcome::Failed(error);
            this.state = CancelWaitState::Done(outcome);
            return Poll::Ready(outcome);
        }
        this.target.cancel();
    }
    let registration = match &this.state {
        CancelWaitState::Waiting { registration, .. } => registration,
        _ => std::process::abort(),
    };
    // 原值析构（Future::drop）展开（unwind）的守卫禁止回调；再次轮询（poll）负责重查并结束旧边。
    if let Some(outcome) = this.target.task.cancellation_outcome() {
        registration.complete(outcome);
        this.target.task.unregister_cancel(registration.key);
    }
    if let Some(outcome) = registration.register_waker(cx.waker()) {
        this.target.task.unregister_cancel(registration.key);
        this.state = CancelWaitState::Done(outcome);
        Poll::Ready(outcome)
    } else { Poll::Pending }
}

#[allow(unused_variables)]
fn cancel_wait_drop_inner(this: &mut CancelWait) {
    if let CancelWaitState::Waiting { registration, .. } = &this.state {
        this.unregister(registration);
    }
}

#[allow(unused_variables)]
fn wait_complete_inner(this: &WaitRegistration, outcome: CancellationOutcome) {
    // CancelObserver的完成方只交付TaskRef::cancellation_outcome的真实终结分类。
    let result = match outcome {
        CancellationOutcome::Cancelled => WAIT_RESULT_CANCELLED,
        CancellationOutcome::Completed => WAIT_RESULT_COMPLETED,
        CancellationOutcome::Failed(CancellationError::TargetAbandoned) => WAIT_RESULT_TARGET_ABANDONED,
        CancellationOutcome::Failed(CancellationError::TargetPanicked) => WAIT_RESULT_TARGET_PANICKED,
        CancellationOutcome::Failed(CancellationError::CleanupUnconfirmed) => WAIT_RESULT_CLEANUP_UNCONFIRMED,
        _ => std::process::abort(),
    };
    this.claim_result(result);
}

#[allow(unused_variables)]
fn wait_abandon_inner(this: &WaitRegistration) {
    this.claim_result(WAIT_RESULT_ABANDONED);
}

fn wait_finish_inner(this: &WaitRegistration) -> bool {
    let previous = this.result.fetch_or(WAIT_RESULT_PUBLISHED, Ordering::AcqRel);
    previous & !WAIT_RESULT_PUBLISHED != WAIT_RESULT_ABANDONED
}

fn wait_outcome_inner(this: &WaitRegistration) -> Option<CancellationOutcome> {
    let result = this.result.load(Ordering::Acquire);
    if result & WAIT_RESULT_PUBLISHED == 0 {
        return None;
    }
    let outcome = match result & !WAIT_RESULT_PUBLISHED {
        WAIT_RESULT_ABANDONED => return None,
        WAIT_RESULT_CANCELLED => CancellationOutcome::Cancelled,
        WAIT_RESULT_COMPLETED => CancellationOutcome::Completed,
        WAIT_RESULT_TARGET_ABANDONED => CancellationOutcome::Failed(CancellationError::TargetAbandoned),
        WAIT_RESULT_TARGET_PANICKED => CancellationOutcome::Failed(CancellationError::TargetPanicked),
        WAIT_RESULT_CLEANUP_UNCONFIRMED => CancellationOutcome::Failed(CancellationError::CleanupUnconfirmed),
        _ => std::process::abort(),
    };
    Some(outcome)
}

fn wait_register_waker_inner(
    this: &WaitRegistration,
    replacement: Waker,
) -> (Option<Waker>, Option<Waker>) {
    if this.waker_state.compare_exchange(
        WAIT_WAKER_IDLE, WAIT_WAKER_REGISTERING, Ordering::Acquire, Ordering::Acquire,
    ).is_err() {
        return (Some(replacement), None);
    }
    // SAFETY: WAIT_WAKER_REGISTERING授予本次轮询独占权。并发终态只可移交槽权限。
    let previous = unsafe { (*this.waker.get()).replace(replacement) };
    if this.waker_state.compare_exchange(
        WAIT_WAKER_REGISTERING, WAIT_WAKER_IDLE, Ordering::AcqRel, Ordering::Acquire,
    ).is_err() {
        // SAFETY: 终态方登记了FINISH_PENDING，仍由本次轮询独占，之后不再登记。
        let current = unsafe { (*this.waker.get()).take() };
        this.waker_state.store(WAIT_WAKER_CLOSED, Ordering::Release);
        return (previous, current);
    }
    (previous, None)
}

fn wait_close_waker_inner(this: &WaitRegistration) -> Option<Waker> {
    let mut state = this.waker_state.load(Ordering::Acquire);
    loop {
        let next = match state {
            WAIT_WAKER_IDLE => WAIT_WAKER_CLOSED,
            WAIT_WAKER_REGISTERING => WAIT_WAKER_FINISH_PENDING,
            WAIT_WAKER_FINISH_PENDING | WAIT_WAKER_CLOSED => return None,
            _ => std::process::abort(),
        };
        match this.waker_state.compare_exchange(state, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(WAIT_WAKER_IDLE) => {
                // SAFETY: 本次直接关闭空闲槽，取得唯一访问权。关闭后不会再登记。
                return unsafe { (*this.waker.get()).take() };
            }
            Ok(_) => return None,
            Err(actual) => state = actual,
        }
    }
}

fn wait_registry_insert_inner(
    this: &WaitRegistry,
    registration: &WaitRegistration,
) -> Result<(), CancellationError> {
    let current = this.snapshot();
    if let Some(snapshot) = &current {
        let relations = &snapshot.relations;
        if let Some(relation) = relations.targets.get(&registration.target) {
            let error = if relation.result.load(Ordering::SeqCst) == WAIT_RESULT_ACTIVE {
                CancellationError::AlreadyWaiting
            } else {
                CancellationError::RegistrationBusy
            };
            return Err(error);
        }
        let mut pending = vec![WaiterId::Task(registration.target)];
        let mut visited = std::collections::HashSet::new();
        while let Some(node) = pending.pop() {
            if node == registration.waiter {
                return Err(CancellationError::WouldCycle);
            }
            if visited.insert(node) {
                if let Some(edges) = relations.edges.get(&node) {
                    pending.extend(edges.iter().filter_map(|(key, target)| {
                        let relation = relations.targets.get(target)?;
                        (relation.key == *key
                            && relation.result.load(Ordering::SeqCst) == WAIT_RESULT_ACTIVE)
                            .then_some(WaiterId::Task(*target))
                    }));
                }
            }
        }
    }
    let mut relations = current.as_ref().map(|snapshot| snapshot.relations.clone()).unwrap_or_default();
    relations.edges.entry(registration.waiter).or_default()
        .insert(registration.key, registration.target);
    relations.targets.insert(registration.target, WaitRelation {
        key: registration.key,
        result: Arc::clone(&registration.result),
    });
    if this.replace(current.as_ref(), relations) {
        Ok(())
    } else {
        Err(CancellationError::RegistrationBusy)
    }
}

fn wait_registry_remove_inner(this: &WaitRegistry, registration: &WaitRegistration) {
    while let Some(current) = this.snapshot() {
        if current.relations.targets.get(&registration.target).map(|relation| relation.key) != Some(registration.key) {
            return;
        }
        let relations = if current.relations.targets.len() == 1 {
            // 保留独立空版本，释放原表的容量。
            WaitRelations::default()
        } else {
            let mut relations = current.relations.clone();
            let edges = relations.edges.get_mut(&registration.waiter).unwrap_or_else(|| std::process::abort());
            edges.remove(&registration.key);
            if edges.is_empty() {
                relations.edges.remove(&registration.waiter);
            }
            relations.targets.remove(&registration.target);
            relations
        };
        if this.replace(Some(&current), relations) {
            return;
        }
    }
}

// SAFETY: 调用方须保有本图的访问计数，直至取得独立强引用或确认空图。
unsafe fn wait_registry_snapshot_inner(this: &WaitRegistry) -> Option<Arc<WaitRegistrySnapshot>> {
    let raw = this.current.load(Ordering::SeqCst);
    if raw.is_null() {
        None
    } else {
        // SAFETY: 本次访问计数先于读取，旧全局Arc只有摘链后读到零访问才能释放。
        // 先增加独立引用再归还访问计数，因此返回的版本可独立保活。
        unsafe {
            Arc::increment_strong_count(raw);
            Some(Arc::from_raw(raw))
        }
    }
}

fn wait_registry_replace_inner(
    this: &WaitRegistry,
    current: Option<&Arc<WaitRegistrySnapshot>>,
    relations: WaitRelations,
) -> Result<*mut WaitRegistrySnapshot, ()> {
    let expected = current.map_or(std::ptr::null_mut(), |snapshot| Arc::as_ptr(snapshot).cast_mut());
    let proposed = Arc::into_raw(Arc::new(WaitRegistrySnapshot {
        relations,
        retired_next: AtomicPtr::new(std::ptr::null_mut()),
    })).cast_mut();
    match this.current.compare_exchange(expected, proposed, Ordering::SeqCst, Ordering::SeqCst) {
        Ok(previous) => Ok(previous),
        Err(_) => {
            // SAFETY: 新强引用未发布，只能由本次调用重新取得并释放。
            drop(unsafe { Arc::from_raw(proposed) });
            Err(())
        }
    }
}

unsafe fn wait_registry_push_retired_inner(
    this: &WaitRegistry,
    head: *mut WaitRegistrySnapshot,
    tail: *mut WaitRegistrySnapshot,
) -> bool {
    this.begin_access();
    let mut previous = this.retired.load(Ordering::SeqCst);
    loop {
        // SAFETY: 本次持有尚未发布的链。访问计数保护用来比较的旧链头地址免于释放复用。
        unsafe { (*tail).retired_next.store(previous, Ordering::Relaxed); }
        match this.retired.compare_exchange(previous, head, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return this.end_access(),
            Err(actual) => previous = actual,
        }
    }
}

fn wait_registry_reclaim_inner(this: &WaitRegistry) {
    loop {
        let head = this.retired.swap(std::ptr::null_mut(), Ordering::SeqCst);
        if !head.is_null() && this.accesses.load(Ordering::SeqCst) == 0 {
            let mut next = head;
            while !next.is_null() {
                // SAFETY: 摘链取得其各强引用。摘链后观察到零访问，后来的访问不能从
                // current或retired取得这条链的裸指针。已保活的快照继续用独立强引用。
                let snapshot = unsafe { Arc::from_raw(next) };
                next = snapshot.retired_next.load(Ordering::Relaxed);
            }
        } else if !head.is_null() {
            let mut tail = head;
            loop {
                // SAFETY: 摘链方独占后继字段。仍有访问者，只遍历而不释放任何强引用。
                let next = unsafe { (*tail).retired_next.load(Ordering::Relaxed) };
                if next.is_null() {
                    break;
                }
                tail = next;
            }
            // SAFETY: 此时仍独占整链。拼回者若是最后的访问者，继续复查。
            if unsafe { this.push_retired(head, tail) } {
                continue;
            }
        }
        if this.reclaim_state.compare_exchange(
            WAIT_REGISTRY_RECLAIMING, 0, Ordering::SeqCst, Ordering::SeqCst,
        ).is_ok() {
            return;
        }
        // 结束期间有真实参与者登记了复查。保持唯一回收资格，清掉已接手的请求后重查。
        this.reclaim_state.store(WAIT_REGISTRY_RECLAIMING, Ordering::SeqCst);
    }
}
