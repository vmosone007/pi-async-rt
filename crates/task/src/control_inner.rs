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
            this.target.task.unregister_cancel(registration.key);
            registration.abandon();
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
        let registry = WAIT_REGISTRY.get_or_init(|| Mutex::new(WaitRegistry { edges: HashMap::new(), targets: HashMap::new() }));
        let error = {
            let Some(mut graph) = registry.try_lock() else {
                let outcome = CancellationOutcome::Failed(CancellationError::RegistrationBusy);
                this.state = CancelWaitState::Done(outcome);
                return Poll::Ready(outcome);
            };
            if graph.targets.contains_key(&this.target.id()) { Some(CancellationError::AlreadyWaiting) }
            else {
                let mut pending = vec![WaiterId::Task(this.target.id())];
                let mut visited = std::collections::HashSet::new();
                let mut cycle = false;
                while let Some(node) = pending.pop() {
                    if node == waiter { cycle = true; break; }
                    if visited.insert(node) {
                        if let Some(edges) = graph.edges.get(&node) { pending.extend(edges.values().copied()); }
                    }
                }
                if cycle { Some(CancellationError::WouldCycle) }
                else {
                    graph.edges.entry(waiter).or_default().insert(key, WaiterId::Task(this.target.id()));
                    graph.targets.insert(this.target.id(), key);
                    None
                }
            }
        };
        if let Some(error) = error {
            let outcome = CancellationOutcome::Failed(error);
            this.state = CancelWaitState::Done(outcome);
            return Poll::Ready(outcome);
        }
        let registration = Arc::new(WaitRegistration {
            key, waiter, target: this.target.clone(), active: AtomicBool::new(true),
            result: Mutex::new(None), waker: Mutex::new(None),
        });
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
    // 克隆（clone）和旧唤醒器（Waker）析构（drop）均在锁（lock）外；结果与通知登记用同一锁序交接。
    let replacement = cx.waker().clone();
    let (outcome, previous) = {
        let result = registration.result.lock();
        if let Some(outcome) = *result { (Some(outcome), Some(replacement)) }
        else { (None, registration.waker.lock().replace(replacement)) }
    };
    drop(previous);
    if let Some(outcome) = outcome {
        this.target.task.unregister_cancel(registration.key);
        this.state = CancelWaitState::Done(outcome);
        Poll::Ready(outcome)
    } else { Poll::Pending }
}

#[allow(unused_variables)]
fn cancel_wait_drop_inner(this: &mut CancelWait) {
    if let CancelWaitState::Waiting { registration, .. } = &this.state {
        this.target.task.unregister_cancel(registration.key);
        registration.abandon();
    }
}

#[allow(unused_variables)]
fn wait_complete_inner(this: &WaitRegistration, outcome: CancellationOutcome) {
    if !this.active.swap(false, std::sync::atomic::Ordering::SeqCst) { return; }
    if let Some(registry) = WAIT_REGISTRY.get() {
        let mut graph = registry.lock();
        if let Some(edges) = graph.edges.get_mut(&this.waiter) {
            edges.remove(&this.key);
            if edges.is_empty() { graph.edges.remove(&this.waiter); }
        }
        if graph.targets.get(&this.target.id()) == Some(&this.key) { graph.targets.remove(&this.target.id()); }
    }
    let waker = {
        let mut result = this.result.lock();
        if result.is_some() { return; }
        *result = Some(outcome);
        this.waker.lock().take()
    };
    if let Some(waker) = waker { waker.wake(); }
}

#[allow(unused_variables)]
fn wait_abandon_inner(this: &WaitRegistration) {
    if !this.active.swap(false, std::sync::atomic::Ordering::SeqCst) { return; }
    if let Some(registry) = WAIT_REGISTRY.get() {
        let mut graph = registry.lock();
        if let Some(edges) = graph.edges.get_mut(&this.waiter) {
            edges.remove(&this.key);
            if edges.is_empty() { graph.edges.remove(&this.waiter); }
        }
        if graph.targets.get(&this.target.id()) == Some(&this.key) { graph.targets.remove(&this.target.id()); }
    }
    let old = this.waker.lock().take();
    drop(old);
}
