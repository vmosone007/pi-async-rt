// 当前文件只落实配对声明中的局部操作。

#[allow(unused_variables)]
fn options_plain_inner() -> TaskOptions {
    TaskOptions {
        queue: QueueSupport::Plain,
        capture_panic_backtrace: false,
        #[cfg(feature = "observe")]
        observer: None,
    }
}

#[allow(unused_variables)]
fn options_intrusive_inner() -> TaskOptions {
    TaskOptions {
        queue: QueueSupport::IntrusiveLocal,
        capture_panic_backtrace: false,
        #[cfg(feature = "observe")]
        observer: None,
    }
}

#[allow(unused_variables)]
fn thread_new_inner() -> TaskThread {
    let id = NEXT_OWNER_ID.try_update(
        std::sync::atomic::Ordering::Relaxed,
        std::sync::atomic::Ordering::Relaxed,
        |next| next.checked_add(1),
    ).unwrap_or_else(|_| std::process::abort());
    TaskThread {
        id: TaskOwnerId(std::num::NonZeroU64::new(id).unwrap_or_else(|| std::process::abort())),
        thread: std::thread::current().id(),
        closing: Cell::new(false),
        active: Cell::new(0),
        tasks: RefCell::new(HashMap::new()),
        local: PhantomData,
    }
}

#[allow(unused_variables)]
fn thread_id_inner(this: &TaskThread) -> TaskOwnerId {
    this.id
}

#[allow(unused_variables)]
fn thread_closed_inner(this: &TaskThread) -> bool {
    this.closing.get()
}

#[allow(unused_variables)]
fn thread_live_inner(this: &TaskThread) -> usize {
    this.tasks.borrow().len()
}

#[allow(unused_variables)]
fn thread_close_inner(this: &TaskThread) {
    assert_eq!(this.thread, std::thread::current().id());
    this.closing.set(true);
    if this.active.get() != 0 { return; }
    let activity = TaskActivityGuard::enter(this);
    let tasks: Vec<TaskRef> = this.tasks.borrow().values().cloned().collect();
    for task in tasks {
        // SAFETY: 所属登记（registration）及局部强持有（strong reference）覆盖本次关闭（close）。
        let header = unsafe { task.pointer.as_ref() };
        let mut state = header.state.load(std::sync::atomic::Ordering::SeqCst);
        loop {
            if matches!(state & ACCESS_MASK, ACCESS_DROPPED | ACCESS_ENDED | ACCESS_UNCONFIRMED) { break; }
            assert!(!matches!(state & ACCESS_MASK, ACCESS_RUNNING | ACCESS_HANDOFF));
            let mut next = (state & !(ACCESS_MASK | WAKE_PENDING)) | ACCESS_EXITING | CANCEL_RECORDED;
            if state & REASON_MASK == 0 { next |= REASON_CANCELLED << REASON_SHIFT; }
            match header.state.compare_exchange(state, next, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
                Ok(_) => {
                    // SAFETY: 关闭（close）的活动守卫（TaskActivityGuard）阻止重入取得原值。
                    unsafe { (header.vtable.drop_future)(task.pointer, Some(this)); }
                    break;
                }
                Err(actual) => state = actual,
            }
        }
        if matches!(header.state.load(std::sync::atomic::Ordering::SeqCst) & ACCESS_MASK, ACCESS_DROPPED | ACCESS_ENDED) {
            let root = this.tasks.borrow_mut().remove(&header.id);
            drop(root);
        }
    }
    drop(activity);
}

#[allow(unused_variables)]
fn thread_drop_inner(this: &mut TaskThread) {
    let outer_unwinding = std::thread::panicking();
    assert_eq!(this.thread, std::thread::current().id());
    assert_eq!(this.active.get(), 0);
    this.closing.set(true);
    let activity = TaskActivityGuard::enter(this);
    let mut first_panic = None;
    let mut record_error = |result: std::thread::Result<()>| {
        if let Err(payload) = result {
            if outer_unwinding || first_panic.is_some() { std::process::abort(); }
            first_panic = Some(payload);
        }
    };
    // 未处理登记（registrations）及快照（snapshot）均在捕获（catch）边界外保活；不重复扫描稀疏登记表（registry）。
    let tasks: Vec<TaskRef> = this.tasks.borrow().values().cloned().collect();
    for task in tasks {
        // SAFETY: 登记（registration）和局部强持有（strong reference）覆盖清理及失败通知（notification）。
        let header = unsafe { task.pointer.as_ref() };
        record_error(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut state = header.state.load(std::sync::atomic::Ordering::SeqCst);
            loop {
                if matches!(state & ACCESS_MASK, ACCESS_DROPPED | ACCESS_ENDED | ACCESS_UNCONFIRMED) { break; }
                assert!(!matches!(state & ACCESS_MASK, ACCESS_RUNNING | ACCESS_HANDOFF));
                let mut next = (state & !(ACCESS_MASK | WAKE_PENDING)) | ACCESS_EXITING | CANCEL_RECORDED;
                if state & REASON_MASK == 0 { next |= REASON_CANCELLED << REASON_SHIFT; }
                match header.state.compare_exchange(state, next, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
                    Ok(_) => {
                        // SAFETY: 原线程（owner thread）及活动守卫（TaskActivityGuard）维持排他清理；失败由FutureDropGuard隔离。
                        unsafe { (header.vtable.drop_future)(task.pointer, Some(this)); }
                        break;
                    }
                    Err(actual) => state = actual,
                }
            }
        })));
        if header.state.load(std::sync::atomic::Ordering::SeqCst) & ACCESS_MASK == ACCESS_UNCONFIRMED {
            // 捕获（catch）返回后只访问头部（Header），不再读取或析构（drop）失效原值（Future）。
            record_error(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut notification = header.result_waiter.state.load(std::sync::atomic::Ordering::SeqCst);
                let result_waker = loop {
                    match notification {
                        0 => match header.result_waiter.state.compare_exchange(0, 2, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
                            Ok(_) => {
                                // SAFETY: 状态2独占通知槽（NotifySlot）；移出旧唤醒器（Waker）后先归还权限（permission）。
                                let waker = unsafe { (*header.result_waiter.value.get()).take() };
                                header.result_waiter.state.store(0, std::sync::atomic::Ordering::SeqCst);
                                break waker;
                            }
                            Err(actual) => notification = actual,
                        },
                        1 => match header.result_waiter.state.compare_exchange(1, 3, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
                            Ok(_) => break None,
                            Err(actual) => notification = actual,
                        },
                        2 | 3 => break None,
                        _ => std::process::abort(),
                    }
                };
                let mut notification = header.cancel_waiter.state.load(std::sync::atomic::Ordering::SeqCst);
                let cancel_observer = loop {
                    match notification {
                        0 => match header.cancel_waiter.state.compare_exchange(0, 2, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
                            Ok(_) => {
                                // SAFETY: 状态2独占通知槽（NotifySlot），旧键（key）和登记（registration）先隔离，再执行外部通知（notification）。
                                let observer = unsafe { (*header.cancel_waiter.value.get()).take() };
                                header.cancel_wait_key.store(0, std::sync::atomic::Ordering::SeqCst);
                                header.state.fetch_and(!CANCEL_WAIT_INTEREST, std::sync::atomic::Ordering::SeqCst);
                                header.cancel_waiter.state.store(0, std::sync::atomic::Ordering::SeqCst);
                                break observer;
                            }
                            Err(actual) => notification = actual,
                        },
                        1 => match header.cancel_waiter.state.compare_exchange(1, 3, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
                            Ok(_) => break None,
                            Err(actual) => notification = actual,
                        },
                        2 | 3 => break None,
                        _ => std::process::abort(),
                    }
                };
                if let Some(observer) = cancel_observer {
                    observer.complete(task.cancellation_outcome().unwrap_or_else(|| std::process::abort()));
                }
                if let Some(waker) = result_waker { waker.wake(); }
            })));
        }
        let root = this.tasks.borrow_mut().remove(&header.id);
        // 先移出登记（registration），再在捕获（catch）内归还本域全部持有；末引用（reference）可触发用户析构（drop）。
        record_error(std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            drop(root);
            drop(task);
        })));
    }
    drop(activity);
    if let Some(payload) = first_panic { std::panic::resume_unwind(payload); }
}

#[allow(unused_variables)]
fn registration_drop_inner(this: &mut TaskRegistration) {
    // SAFETY: 本份登记（TaskRegistration）只在原线程（thread）释放，字段持有覆盖查询。
    assert_eq!(unsafe { this.task.pointer.as_ref() }.owner.load(std::sync::atomic::Ordering::SeqCst), this.owner.get());
    this.task.request_cancel();
}

#[allow(unused_variables)]
fn ref_id_inner(this: &TaskRef) -> TaskId {
    // SAFETY: TaskRef的强持有（strong reference）保证固定身份（ID）仍存活。
    unsafe { this.pointer.as_ref().id }
}

#[allow(unused_variables)]
fn ref_snapshot_inner(this: &TaskRef) -> TaskSnapshot {
    // SAFETY: 共享持有仅查询固定身份（ID）和原子头部（Header）。
    let header = unsafe { this.pointer.as_ref() };
    let state = header.state.load(std::sync::atomic::Ordering::SeqCst);
    let access = state & ACCESS_MASK;
    let execution = match access {
        ACCESS_IDLE => ExecutionStatus::Idle,
        ACCESS_RUNNING => ExecutionStatus::Running,
        ACCESS_HANDOFF => ExecutionStatus::PendingHandoff,
        ACCESS_EXITING => ExecutionStatus::Exiting,
        ACCESS_DROPPED => ExecutionStatus::FutureDropped,
        ACCESS_ENDED => ExecutionStatus::Ended,
        ACCESS_UNCONFIRMED => ExecutionStatus::CleanupUnconfirmed,
        _ => std::process::abort(),
    };
    let termination = match (state & REASON_MASK) >> REASON_SHIFT {
        REASON_NONE => None,
        REASON_COMPLETED => Some(TerminationKind::Completed),
        REASON_CANCELLED => Some(TerminationKind::Cancelled),
        REASON_PANICKED => Some(TerminationKind::Panicked),
        REASON_ABANDONED => Some(TerminationKind::Abandoned),
        REASON_CLEANUP_FAILED => Some(TerminationKind::CleanupFailed),
        _ => std::process::abort(),
    };
    let phase = match access {
        ACCESS_IDLE if state & STARTED == 0 => TaskPhase::Preparing,
        ACCESS_IDLE | ACCESS_RUNNING | ACCESS_HANDOFF => TaskPhase::Advancing,
        ACCESS_ENDED => TaskPhase::Ended,
        _ => TaskPhase::Exiting,
    };
    TaskSnapshot {
        id: header.id,
        owner: std::num::NonZeroU64::new(header.owner.load(std::sync::atomic::Ordering::SeqCst)).map(TaskOwnerId),
        phase, execution, termination,
        cancellation: if state & CANCEL_RECORDED != 0 { CancelRequestStatus::Recorded } else { CancelRequestStatus::NotRecorded },
        future_drop: if matches!(access, ACCESS_DROPPED | ACCESS_ENDED) { FutureDropStatus::Confirmed } else { FutureDropStatus::Unconfirmed },
        admitted: state & ADMITTED != 0,
    }
}

#[allow(unused_variables)]
fn ref_cancel_inner(this: &TaskRef) {
    // SAFETY: 共享强持有（strong reference）覆盖原子头部（Header），从不访问原值（Future）。
    let header = unsafe { this.pointer.as_ref() };
    let current = current_task_id() == Some(header.id);
    let mut prepared = None;
    let mut state = header.state.load(std::sync::atomic::Ordering::SeqCst);
    loop {
        if state & ACCESS_MASK >= ACCESS_EXITING || state & REASON_MASK != 0 { return; }
        let issue = !current && state & ACCESS_MASK == ACCESS_IDLE && state & RUNNABLE_ISSUED == 0
            && header.owner.load(std::sync::atomic::Ordering::SeqCst) != 0;
        if issue && prepared.is_none() {
            prepared = Some(this.clone());
            state = header.state.load(std::sync::atomic::Ordering::SeqCst);
            continue;
        }
        let mut next = state | CANCEL_RECORDED;
        if issue {
            next = (next & !(ACCESS_MASK | WAKE_PENDING)) | ACCESS_EXITING | RUNNABLE_ISSUED | (REASON_CANCELLED << REASON_SHIFT);
        }
        match header.state.compare_exchange(state, next, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
            Ok(_) => {
                let cleanup = if issue {
                    Some(crate::runnable::Runnable { task: prepared.take().unwrap_or_else(|| std::process::abort()) })
                } else { None };
                #[cfg(feature = "observe")]
                if state & CANCEL_RECORDED == 0 {
                    if let Some(observer) = &header.observer { observer.event(header.id, TaskEvent::CancelRecorded); }
                }
                if let Some(cleanup) = cleanup {
                    // SAFETY: 新持有（Runnable）及退出事实已完整成立，只交回原线程（thread）清理。
                    unsafe { (header.vtable.schedule)(this.pointer, cleanup); }
                }
                return;
            }
            Err(actual) => state = actual,
        }
    }
}

#[allow(unused_variables)]
fn ref_waker_inner(this: &TaskRef) -> Waker {
    // SAFETY: 本份TaskRef保活，借用包装（ManuallyDrop）不额外归还已有强持有。
    let raw = unsafe { native_clone(this.pointer.as_ptr().cast()) };
    unsafe { Waker::from_raw(raw) }
}

#[allow(unused_variables)]
fn ref_wake_inner(this: &TaskRef) {
    // SAFETY: 共享持有（TaskRef）只修改原子状态（atomic state）。
    let header = unsafe { this.pointer.as_ref() };
    let mut prepared = None;
    let mut state = header.state.load(std::sync::atomic::Ordering::SeqCst);
    loop {
        if state & ACCESS_MASK >= ACCESS_EXITING || state & REASON_MASK != 0 { return; }
        let issue = state & ACCESS_MASK == ACCESS_IDLE && state & RUNNABLE_ISSUED == 0;
        if issue && prepared.is_none() {
            prepared = Some(this.clone());
            state = header.state.load(std::sync::atomic::Ordering::SeqCst);
            continue;
        }
        let next = state | WAKE_PENDING | if issue { RUNNABLE_ISSUED } else { 0 };
        match header.state.compare_exchange(state, next, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
            Ok(_) => {
                if issue {
                    let task = prepared.take().unwrap_or_else(|| std::process::abort());
                    // SAFETY: 新持有（Runnable）先成立，再经原调度接口（Schedule）交付。
                    unsafe { (header.vtable.schedule)(this.pointer, crate::runnable::Runnable { task }); }
                }
                return;
            }
            Err(actual) => state = actual,
        }
    }
}

#[allow(unused_variables)]
fn ref_admit_inner(this: &TaskRef, owner: &TaskThread) -> Result<TaskRegistration, AdmissionError> {
    if owner.thread != std::thread::current().id() { return Err(AdmissionError::WrongThread); }
    if owner.closing.get() { return Err(AdmissionError::OwnerClosed); }
    // SAFETY: 本份借用（borrow）保住头部（Header），原持有（Runnable）不能同时释放或执行。
    let header = unsafe { this.pointer.as_ref() };
    let bound = header.owner.load(std::sync::atomic::Ordering::SeqCst);
    if bound != 0 && bound != owner.id.get() { return Err(AdmissionError::WrongOwner); }
    let mut state = header.state.load(std::sync::atomic::Ordering::SeqCst);
    if state & ACCESS_MASK >= ACCESS_EXITING { return Err(AdmissionError::AlreadyEnded); }
    if state & ADMITTED != 0 { return Err(AdmissionError::AlreadyAdmitted); }
    let registration = this.clone();
    if bound == 0 {
        owner.tasks.borrow_mut().insert(header.id, this.clone());
        if header.owner.compare_exchange(0, owner.id.get(), std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_err() {
            let root = owner.tasks.borrow_mut().remove(&header.id);
            drop(root);
            return Err(AdmissionError::WrongOwner);
        }
    }
    let admitted = loop {
        if header.owner.load(std::sync::atomic::Ordering::SeqCst) != owner.id.get() { break Err(AdmissionError::WrongOwner); }
        if state & ACCESS_MASK >= ACCESS_EXITING { break Err(AdmissionError::AlreadyEnded); }
        if state & ADMITTED != 0 { break Err(AdmissionError::AlreadyAdmitted); }
        match header.state.compare_exchange(state, state | ADMITTED, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
            Ok(_) => break Ok(()),
            Err(actual) => state = actual,
        }
    };
    if let Err(error) = admitted {
        if bound == 0 {
            // 拒绝（rejection）只归还准备好的普通强持有（strong reference），不触发登记退出请求。
            let root = owner.tasks.borrow_mut().remove(&header.id);
            let _ = header.owner.compare_exchange(owner.id.get(), 0, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst);
            drop(root);
        }
        return Err(error);
    }
    let registration = TaskRegistration { task: registration, owner: owner.id, local: PhantomData };
    #[cfg(feature = "observe")]
    if let Some(observer) = &header.observer { observer.event(header.id, TaskEvent::Admitted); }
    Ok(registration)
}

#[allow(unused_variables)]
fn ref_run_inner(this: &TaskRef, owner: &TaskThread) -> Result<RunOutcome, RunErrorKind> {
    if owner.thread != std::thread::current().id() { return Err(RunErrorKind::WrongThread); }
    // SAFETY: 本份执行强持有（strong reference）在整个调用期间存活。
    let header = unsafe { this.pointer.as_ref() };
    let bound = header.owner.load(std::sync::atomic::Ordering::SeqCst);
    if bound != 0 && bound != owner.id.get() { return Err(RunErrorKind::WrongOwner); }
    if owner.closing.get() && owner.active.get() != 0 { return Err(RunErrorKind::OwnerClosed); }
    if bound == 0 {
        if owner.closing.get() { return Err(RunErrorKind::OwnerClosed); }
        owner.tasks.borrow_mut().insert(header.id, this.clone());
        header.owner.store(owner.id.get(), std::sync::atomic::Ordering::SeqCst);
    }
    let activity = TaskActivityGuard::enter(owner);
    let mut state = header.state.load(std::sync::atomic::Ordering::SeqCst);
    let poll = loop {
        let access = state & ACCESS_MASK;
        if matches!(access, ACCESS_RUNNING | ACCESS_HANDOFF) { return Err(RunErrorKind::AlreadyRunning); }
        if state & RUNNABLE_ISSUED == 0 { std::process::abort(); }
        let should_poll = access == ACCESS_IDLE && state & CANCEL_RECORDED == 0 && !owner.closing.get();
        let mut next = state & !(RUNNABLE_ISSUED | WAKE_PENDING);
        if should_poll { next = (next & !ACCESS_MASK) | ACCESS_RUNNING | STARTED; }
        else if access == ACCESS_IDLE {
            next = (next & !ACCESS_MASK) | ACCESS_EXITING | CANCEL_RECORDED;
            if next & REASON_MASK == 0 { next |= REASON_CANCELLED << REASON_SHIFT; }
        }
        match header.state.compare_exchange(state, next, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
            Ok(_) => break should_poll,
            Err(actual) => state = actual,
        }
    };
    let mut guard = RunGuard { task: this, armed: true };
    let outcome = if poll {
        // SAFETY: 排他访问、活动守卫（TaskActivityGuard）及原线程（thread）检查均已成立。
        unsafe { (header.vtable.run)(this.pointer, owner) }
    } else {
        if header.state.load(std::sync::atomic::Ordering::SeqCst) & ACCESS_MASK == ACCESS_EXITING {
            unsafe { (header.vtable.drop_future)(this.pointer, Some(owner)); }
        }
        RunOutcome::Finished(this.snapshot().termination.unwrap_or_else(|| std::process::abort()))
    };
    let outcome = if outcome == RunOutcome::Pending {
        let prepared = this.clone();
        let mut state = header.state.load(std::sync::atomic::Ordering::SeqCst);
        loop {
            if state & CANCEL_RECORDED != 0 || owner.closing.get() {
                let next = (state & !(ACCESS_MASK | WAKE_PENDING)) | ACCESS_EXITING | (REASON_CANCELLED << REASON_SHIFT) | CANCEL_RECORDED;
                match header.state.compare_exchange(state, next, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
                    Ok(_) => {
                        drop(prepared);
                        unsafe { (header.vtable.drop_future)(this.pointer, Some(owner)); }
                        break RunOutcome::Finished(TerminationKind::Cancelled);
                    }
                    Err(actual) => { state = actual; continue; }
                }
            }
            let issue = state & WAKE_PENDING != 0;
            let next = (state & !(ACCESS_MASK | WAKE_PENDING)) | if issue { RUNNABLE_ISSUED } else { 0 };
            match header.state.compare_exchange(state, next, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
                Ok(_) => {
                    if issue { unsafe { (header.vtable.schedule)(this.pointer, crate::runnable::Runnable { task: prepared }); } }
                    else { drop(prepared); }
                    break RunOutcome::Pending;
                }
                Err(actual) => state = actual,
            }
        }
    } else { outcome };
    if matches!(header.state.load(std::sync::atomic::Ordering::SeqCst) & ACCESS_MASK, ACCESS_DROPPED | ACCESS_ENDED) {
        let root = owner.tasks.borrow_mut().remove(&header.id);
        drop(root);
    }
    guard.armed = false;
    drop(guard);
    drop(activity);
    if owner.closing.get() && owner.active.get() == 0 { owner.close(); }
    Ok(outcome)
}

#[allow(unused_variables)]
fn ref_metadata_inner<M: Send + Sync + 'static>(this: &TaskRef) -> Option<&M> {
    // SAFETY: 虚函数表（vtable）以TypeId核对原始M，存储（storage）由本份TaskRef保活。
    let pointer = unsafe { (this.pointer.as_ref().vtable.metadata)(this.pointer, TypeId::of::<M>()) }?;
    // SAFETY: M创建后不可变且满足Send+Sync，返回借用（borrow）不超过this。
    Some(unsafe { pointer.cast::<M>().as_ref() })
}

#[allow(unused_variables)]
fn ref_register_result_inner(this: &TaskRef, waker: &Waker) {
    // SAFETY: 当前TaskRef覆盖等待槽（NotifySlot）；唯一结果句柄（JoinHandle）串行登记（register）。
    let header = unsafe { this.pointer.as_ref() };
    let state = header.state.load(std::sync::atomic::Ordering::SeqCst);
    if state & OUTPUT_PRESENT != 0 || state & ACCESS_MASK == ACCESS_UNCONFIRMED {
        waker.wake_by_ref(); return;
    }
    let replacement = waker.clone();
    if header.result_waiter.state.compare_exchange(0, 1, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_err() {
        drop(replacement); waker.wake_by_ref(); return;
    }
    // SAFETY: 状态1赋予本次写权限；外部唤醒器（Waker）仅在槽权限外释放。
    let old = unsafe { (*header.result_waiter.value.get()).replace(replacement) };
    let notified = header.result_waiter.state.compare_exchange(1, 0, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_err();
    let wake = if notified {
        header.result_waiter.state.store(2, std::sync::atomic::Ordering::SeqCst);
        let wake = unsafe { (*header.result_waiter.value.get()).take() };
        header.result_waiter.state.store(0, std::sync::atomic::Ordering::SeqCst);
        wake
    } else { None };
    drop(old);
    if let Some(wake) = wake { wake.wake(); }
    let state = header.state.load(std::sync::atomic::Ordering::SeqCst);
    if state & OUTPUT_PRESENT != 0 || state & ACCESS_MASK == ACCESS_UNCONFIRMED { waker.wake_by_ref(); }
}

#[allow(unused_variables)]
fn ref_abandon_result_inner(this: &TaskRef) {
    // SAFETY: 本份TaskRef维持头部（Header）存活；唯一结果句柄（JoinHandle）负责已发布T的类型安全清理。
    let header = unsafe { this.pointer.as_ref() };
    header.state.fetch_and(!RESULT_INTEREST, std::sync::atomic::Ordering::SeqCst);
    if header.result_waiter.state.compare_exchange(0, 1, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_ok() {
        // SAFETY: 已取得等待槽（NotifySlot）独占权限，移出后先释放权限再析构（drop）外部唤醒器（Waker）。
        let old = unsafe { (*header.result_waiter.value.get()).take() };
        let previous = header.result_waiter.state.swap(0, std::sync::atomic::Ordering::SeqCst);
        assert!(previous == 1 || previous == 3);
        drop(old);
    }
}

#[allow(unused_variables)]
unsafe fn ref_take_inner<T>(this: &TaskRef, destination: NonNull<Result<T, TaskError>>) -> bool {
    // SAFETY: 本份持有覆盖头部（Header）；调用者维持唯一结果领取权及T匹配。
    let header = unsafe { this.pointer.as_ref() };
    let mut state = header.state.load(std::sync::atomic::Ordering::SeqCst);
    loop {
        if state & OUTPUT_PRESENT == 0 || state & OUTPUT_TAKEN != 0 { return false; }
        let next = (state & !OUTPUT_PRESENT) | OUTPUT_TAKEN;
        match header.state.compare_exchange(state, next, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
            Ok(_) => {
                // SAFETY: 本次原子认领（atomic claim）唯一成功，destination由调用者保证可写且对齐。
                unsafe { (header.vtable.take_result)(this.pointer, destination.cast()); }
                return true;
            }
            Err(actual) => state = actual,
        }
    }
}

#[allow(unused_variables)]
fn ref_register_cancel_inner(
    this: &TaskRef,
    key: u64,
    observer: Arc<dyn CancelObserver>,
) -> Result<(), CancellationError> {
    if key == 0 { return Err(CancellationError::RegistrationBusy); }
    if let Some(outcome) = this.cancellation_outcome() { observer.complete(outcome); return Ok(()); }
    // SAFETY: 当前强持有（strong reference）覆盖等待槽（NotifySlot）和登记身份。
    let header = unsafe { this.pointer.as_ref() };
    if header.cancel_wait_key.load(std::sync::atomic::Ordering::SeqCst) != 0 { return Err(CancellationError::AlreadyWaiting); }
    if header.cancel_waiter.state.compare_exchange(0, 1, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_err() {
        return Err(CancellationError::RegistrationBusy);
    }
    if header.cancel_wait_key.compare_exchange(0, key, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_err() {
        // 先前的零key快照（snapshot）可能已过期；当前槽（slot）属于抢先完成登记的等待者（waiter）。
        // 若完成者已交出状态3，须接续其通知，不能直接清零而遗失旧接收者（observer）。
        if header.cancel_waiter.state.compare_exchange(1, 0, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_err() {
            header.cancel_waiter.state.store(2, std::sync::atomic::Ordering::SeqCst);
            let previous = unsafe { (*header.cancel_waiter.value.get()).take() };
            header.cancel_wait_key.store(0, std::sync::atomic::Ordering::SeqCst);
            header.state.fetch_and(!CANCEL_WAIT_INTEREST, std::sync::atomic::Ordering::SeqCst);
            header.cancel_waiter.state.store(0, std::sync::atomic::Ordering::SeqCst);
            if let Some(previous) = previous {
                previous.complete(this.cancellation_outcome().unwrap_or_else(|| std::process::abort()));
            }
        }
        return Err(CancellationError::AlreadyWaiting);
    }
    // SAFETY: 状态1仅本次登记者可写，旧槽（slot）为空才允许新身份。
    let previous = unsafe { (*header.cancel_waiter.value.get()).replace(observer) };
    if previous.is_some() { std::process::abort(); }
    header.state.fetch_or(CANCEL_WAIT_INTEREST, std::sync::atomic::Ordering::SeqCst);
    let outcome = this.cancellation_outcome();
    let handoff = header.cancel_waiter.state.compare_exchange(1, 0, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_err();
    if handoff || outcome.is_some() {
        if !handoff && header.cancel_waiter.state.compare_exchange(0, 2, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_err() {
            return Ok(());
        }
        if handoff { header.cancel_waiter.state.store(2, std::sync::atomic::Ordering::SeqCst); }
        // SAFETY: 通知权限（notification permission）已独占，先取走并隔离旧身份，再调用接收者。
        let observer = unsafe { (*header.cancel_waiter.value.get()).take() };
        header.cancel_wait_key.store(0, std::sync::atomic::Ordering::SeqCst);
        header.state.fetch_and(!CANCEL_WAIT_INTEREST, std::sync::atomic::Ordering::SeqCst);
        header.cancel_waiter.state.store(0, std::sync::atomic::Ordering::SeqCst);
        if let (Some(observer), Some(outcome)) = (observer, outcome.or_else(|| this.cancellation_outcome())) { observer.complete(outcome); }
    }
    Ok(())
}

#[allow(unused_variables)]
fn ref_unregister_cancel_inner(this: &TaskRef, key: u64) {
    // SAFETY: 本份共享持有（TaskRef）覆盖通知槽（NotifySlot），旧身份不能清除新登记（registration）。
    let header = unsafe { this.pointer.as_ref() };
    if key == 0 || header.cancel_wait_key.load(std::sync::atomic::Ordering::SeqCst) != key { return; }
    if header.cancel_waiter.state.compare_exchange(0, 1, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_err() {
        // 通知中（notifying）的旧登记已由完成者接管，不接触其值；控制层另行可靠隔离图边。
        return;
    }
    let matches = header.cancel_wait_key.compare_exchange(key, 0, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_ok();
    if matches {
        header.state.fetch_and(!CANCEL_WAIT_INTEREST, std::sync::atomic::Ordering::SeqCst);
        // SAFETY: 独占登记权限（registration permission）下只移出匹配本key的旧接收者。
        let old = unsafe { (*header.cancel_waiter.value.get()).take() };
        let previous = header.cancel_waiter.state.swap(0, std::sync::atomic::Ordering::SeqCst);
        if previous != 1 && previous != 3 { std::process::abort(); }
        if let Some(observer) = old { observer.abandon(); }
    } else if header.cancel_waiter.state.compare_exchange(1, 0, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst).is_err() {
        // 旧key不能撤销新等待者（waiter）；若有状态3，仅接续新等待者自己的完成交接（completion handoff）。
        header.cancel_waiter.state.store(2, std::sync::atomic::Ordering::SeqCst);
        let observer = unsafe { (*header.cancel_waiter.value.get()).take() };
        header.cancel_wait_key.store(0, std::sync::atomic::Ordering::SeqCst);
        header.state.fetch_and(!CANCEL_WAIT_INTEREST, std::sync::atomic::Ordering::SeqCst);
        header.cancel_waiter.state.store(0, std::sync::atomic::Ordering::SeqCst);
        if let Some(observer) = observer {
            observer.complete(this.cancellation_outcome().unwrap_or_else(|| std::process::abort()));
        }
    }
}

#[allow(unused_variables)]
fn ref_cancel_outcome_inner(this: &TaskRef) -> Option<CancellationOutcome> {
    // SAFETY: 只读取本份持有所覆盖的原子状态字（atomic state word）。
    let state = unsafe { this.pointer.as_ref() }.state.load(std::sync::atomic::Ordering::SeqCst);
    if state & ACCESS_MASK == ACCESS_UNCONFIRMED { return Some(CancellationOutcome::Failed(CancellationError::CleanupUnconfirmed)); }
    if !matches!(state & ACCESS_MASK, ACCESS_DROPPED | ACCESS_ENDED) { return None; }
    match (state & REASON_MASK) >> REASON_SHIFT {
        REASON_COMPLETED => Some(CancellationOutcome::Completed),
        REASON_CANCELLED => Some(CancellationOutcome::Cancelled),
        REASON_ABANDONED => Some(CancellationOutcome::Failed(CancellationError::TargetAbandoned)),
        REASON_PANICKED => Some(CancellationOutcome::Failed(CancellationError::TargetPanicked)),
        REASON_CLEANUP_FAILED => Some(CancellationOutcome::Failed(CancellationError::CleanupUnconfirmed)),
        _ => None,
    }
}

#[allow(unused_variables)]
fn ref_queue_entry_inner(this: &TaskRef) -> Option<NonNull<QueueEntry>> {
    // SAFETY: 本份强持有（TaskRef）覆盖节点（node）投影，布局（layout）由创建时固定。
    unsafe { (this.pointer.as_ref().vtable.queue_entry)(this.pointer) }
}

#[allow(unused_variables)]
fn ref_clone_inner(this: &TaskRef) -> TaskRef {
    // SAFETY: 借用的TaskRef保住控制存储（storage）。
    let header = unsafe { this.pointer.as_ref() };
    if header.state.try_update(
        std::sync::atomic::Ordering::SeqCst,
        std::sync::atomic::Ordering::SeqCst,
        |state| {
            let count = state >> REF_SHIFT;
            if count == 0 || count == REF_MAX { None } else { Some(state + REF_ONE) }
        },
    ).is_err() {
        std::process::abort();
    }
    TaskRef { pointer: this.pointer }
}

#[allow(unused_variables)]
fn ref_drop_inner(this: &mut TaskRef) {
    // SAFETY: 本份强持有（strong reference）覆盖头部（Header）的原子操作（atomic operation）。
    let header = unsafe { this.pointer.as_ref() };
    let state = header.state.fetch_sub(REF_ONE, std::sync::atomic::Ordering::SeqCst);
    if state >> REF_SHIFT == 0 {
        std::process::abort();
    }
    if state >> REF_SHIFT == 1 {
        if !matches!(state & ACCESS_MASK, ACCESS_DROPPED | ACCESS_ENDED | ACCESS_UNCONFIRMED)
            || state & (RUNNABLE_ISSUED | OUTPUT_PRESENT) != 0 {
            std::process::abort();
        }
        // SAFETY: 最后强持有（strong reference）且原值、输出（output）与执行责任均已退出。
        unsafe { (header.vtable.destroy)(this.pointer); }
    }
}

#[allow(unused_variables)]
unsafe fn link_from_node_inner(node: NonNull<QueueEntry>) -> NonNull<LinkedListLink> {
    // SAFETY: repr(C)字段（field）投影不访问或移动节点（node）。
    unsafe { NonNull::new_unchecked(std::ptr::addr_of_mut!((*node.as_ptr()).link)) }
}

#[allow(unused_variables)]
unsafe fn node_from_link_inner(link: NonNull<LinkedListLink>) -> NonNull<QueueEntry> {
    // SAFETY: link必来自同一repr(C)节点（node）的已知字段（field），偏移换算严格互逆。
    unsafe { NonNull::new_unchecked(link.as_ptr().cast::<u8>().sub(std::mem::offset_of!(QueueEntry, link)).cast()) }
}

#[allow(unused_variables)]
unsafe fn queue_store_inner(
    task: crate::runnable::Runnable,
    owner: TaskOwnerId,
) -> Result<NonNull<QueueEntry>, crate::runnable::Runnable> {
    if task.snapshot().owner != Some(owner) { return Err(task); }
    let Some(node) = task.task.queue_entry() else { return Err(task); };
    // SAFETY: 调用者在原线程（thread）独占尚未入链节点（node），Runnable保住原分配（allocation）。
    let entry = unsafe { node.as_ref() };
    if entry.link.is_linked() || unsafe { (*entry.ticket.get()).is_some() } { return Err(task); }
    unsafe { entry.ticket.get().write(Some(task)); }
    Ok(node)
}

#[allow(unused_variables)]
unsafe fn queue_take_inner(
    node: NonNull<QueueEntry>,
    owner: TaskOwnerId,
) -> crate::runnable::Runnable {
    // SAFETY: 调用者保证原线程（thread）及完整摘链，节点（node）仍由ticket保活。
    let entry = unsafe { node.as_ref() };
    if entry.link.is_linked() { std::process::abort(); }
    let ticket = unsafe { (*entry.ticket.get()).take() }.unwrap_or_else(|| std::process::abort());
    if ticket.snapshot().owner != Some(owner) { std::process::abort(); }
    ticket
}

#[allow(unused_variables)]
unsafe fn current_enter_inner(pointer: NonNull<Header>) -> CurrentGuard {
    // SAFETY: 调用者独占本次原值访问，头部（Header）覆盖当前同步轮询（poll）。
    let id = unsafe { pointer.as_ref().id };
    let previous = CURRENT_TASK.with(|current| current.replace(Some(CurrentTask { pointer, id })));
    let previous_backtraces = CURRENT_PANIC_BACKTRACES.with(|records| records.replace(None));
    CurrentGuard { previous, previous_backtraces }
}

#[allow(unused_variables)]
fn current_guard_drop_inner(this: &mut CurrentGuard) {
    let _ = CURRENT_TASK.try_with(|current| current.set(this.previous));
    let previous = this.previous_backtraces.take();
    let _ = CURRENT_PANIC_BACKTRACES.try_with(|records| *records.borrow_mut() = previous);
}

fn current_task_id_inner() -> Option<TaskId> {
    CURRENT_TASK.try_with(|current| current.get().map(|task| task.id)).unwrap_or(None)
}
fn capture_current_backtrace_inner() -> bool {
    let Some(current) = CURRENT_TASK.try_with(|current| current.get()).ok().flatten() else { return false; };
    // SAFETY: 当前同步轮询（poll）保住原任务（Task）头部（Header）。
    if !unsafe { current.pointer.as_ref() }.capture_panic_backtrace { return false; }
    CURRENT_PANIC_BACKTRACES.try_with(|records| {
        let Ok(mut records) = records.try_borrow_mut() else { return false; };
        let record = records.get_or_insert_with(|| (current.id, Vec::new()));
        if record.0 != current.id { return false; }
        record.1.push(std::backtrace::Backtrace::force_capture());
        true
    }).unwrap_or(false)
}
#[allow(unused_variables)]
fn error_display_inner(this: &TaskError, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let message = match this {
        TaskError::Cancelled => "任务（Task）已取消（cancelled）",
        TaskError::Abandoned => "初始执行持有（runnable handle）已放弃",
        TaskError::Panicked(_) => "任务轮询（Future::poll）发生恐慌（panic）",
        TaskError::CleanupUnconfirmed => "原异步计算（Future）清理尚未确认",
        TaskError::OwnerClosed => "所属域已关闭",
        TaskError::WrongThread => "当前线程（thread）不满足结果领取限制",
        TaskError::AlreadyTaken => "结果（output）已领取",
    };
    f.write_str(message)
}

#[allow(unused_variables)]
fn allocate_send_inner<F, S, M>(
    future: F,
    schedule: S,
    metadata: M,
    options: TaskOptions,
) -> Result<CreatedRefs, (F, S, M, TaskOptions, BuildErrorKind)>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
    M: Send + Sync + 'static,
{
    macro_rules! allocate {
        ($links:ty, $value:expr) => {{
            let layout = std::alloc::Layout::new::<TaskStorage<F, S, M, $links>>();
            // SAFETY: 使用完整存储布局（layout）分配，失败不转移任何输入。
            let raw = unsafe { std::alloc::alloc(layout) }.cast::<TaskStorage<F, S, M, $links>>();
            let Some(pointer) = NonNull::new(raw) else {
                return Err((future, schedule, metadata, options, BuildErrorKind::AllocationFailed));
            };
            let id = NEXT_TASK_ID.try_update(std::sync::atomic::Ordering::Relaxed, std::sync::atomic::Ordering::Relaxed,
                |next| next.checked_add(1)).unwrap_or_else(|_| std::process::abort());
            let storage = TaskStorage {
                header: Header {
                    state: AtomicUsize::new(3 * REF_ONE | RUNNABLE_ISSUED | RESULT_INTEREST),
                    id: TaskId(std::num::NonZeroU64::new(id).unwrap_or_else(|| std::process::abort())),
                    owner: AtomicU64::new(0),
                    vtable: &TaskStorage::<F, S, M, $links>::VTABLE,
                    result_waiter: NotifySlot { state: AtomicU8::new(0), value: UnsafeCell::new(None) },
                    cancel_waiter: NotifySlot { state: AtomicU8::new(0), value: UnsafeCell::new(None) },
                    cancel_wait_key: AtomicU64::new(0),
                    capture_panic_backtrace: options.capture_panic_backtrace,
                    #[cfg(feature = "observe")]
                    observer: options.observer,
                },
                schedule,
                metadata: ManuallyDrop::new(metadata),
                links: $value,
                payload: UnsafeCell::new(ManuallyDrop::new(Payload::Future(future))),
            };
            // SAFETY: 分配（allocation）对齐且尚未初始化，一次发布完整原值与头部（Header）。
            unsafe { raw.write(storage); }
            let pointer = pointer.cast::<Header>();
            Ok(CreatedRefs { runnable: TaskRef { pointer }, control: TaskRef { pointer }, join: TaskRef { pointer } })
        }};
    }
    match options.queue {
        QueueSupport::Plain => allocate!((), ()),
        QueueSupport::IntrusiveLocal => allocate!(QueueEntry, QueueEntry { link: LinkedListLink::new(), ticket: UnsafeCell::new(None) }),
    }
}

#[allow(unused_variables)]
fn allocate_local_inner<F, S, M>(
    owner: &TaskThread,
    future: F,
    schedule: S,
    metadata: M,
    options: TaskOptions,
) -> Result<CreatedRefs, (F, S, M, TaskOptions, BuildErrorKind)>
where
    F: Future + 'static,
    F::Output: 'static,
    S: Schedule,
    M: Send + Sync + 'static,
{
    if owner.thread != std::thread::current().id() { return Err((future, schedule, metadata, options, BuildErrorKind::WrongThread)); }
    if owner.closing.get() { return Err((future, schedule, metadata, options, BuildErrorKind::OwnerClosed)); }
    macro_rules! allocate {
        ($links:ty, $value:expr) => {{
            let layout = std::alloc::Layout::new::<TaskStorage<F, S, M, $links>>();
            // SAFETY: 使用完整存储布局（layout）分配，失败不转移任何输入。
            let raw = unsafe { std::alloc::alloc(layout) }.cast::<TaskStorage<F, S, M, $links>>();
            let Some(pointer) = NonNull::new(raw) else {
                return Err((future, schedule, metadata, options, BuildErrorKind::AllocationFailed));
            };
            let id = NEXT_TASK_ID.try_update(std::sync::atomic::Ordering::Relaxed, std::sync::atomic::Ordering::Relaxed,
                |next| next.checked_add(1)).unwrap_or_else(|_| std::process::abort());
            let storage = TaskStorage {
                header: Header {
                    state: AtomicUsize::new(4 * REF_ONE | RUNNABLE_ISSUED | RESULT_INTEREST),
                    id: TaskId(std::num::NonZeroU64::new(id).unwrap_or_else(|| std::process::abort())),
                    owner: AtomicU64::new(owner.id.get()),
                    vtable: &TaskStorage::<F, S, M, $links>::VTABLE,
                    result_waiter: NotifySlot { state: AtomicU8::new(0), value: UnsafeCell::new(None) },
                    cancel_waiter: NotifySlot { state: AtomicU8::new(0), value: UnsafeCell::new(None) },
                    cancel_wait_key: AtomicU64::new(0),
                    capture_panic_backtrace: options.capture_panic_backtrace,
                    #[cfg(feature = "observe")]
                    observer: options.observer,
                },
                schedule,
                metadata: ManuallyDrop::new(metadata),
                links: $value,
                payload: UnsafeCell::new(ManuallyDrop::new(Payload::Future(future))),
            };
            // SAFETY: 分配（allocation）对齐且尚未初始化，一次发布完整原值与头部（Header）。
            unsafe { raw.write(storage); }
            let pointer = pointer.cast::<Header>();
            owner.tasks.borrow_mut().insert(unsafe { pointer.as_ref() }.id, TaskRef { pointer });
            Ok(CreatedRefs { runnable: TaskRef { pointer }, control: TaskRef { pointer }, join: TaskRef { pointer } })
        }};
    }
    match options.queue {
        QueueSupport::Plain => allocate!((), ()),
        QueueSupport::IntrusiveLocal => allocate!(QueueEntry, QueueEntry { link: LinkedListLink::new(), ticket: UnsafeCell::new(None) }),
    }
}

#[allow(unused_variables)]
unsafe fn native_clone_inner(pointer: *const ()) -> std::task::RawWaker {
    let task = ManuallyDrop::new(TaskRef { pointer: NonNull::new(pointer.cast_mut().cast::<Header>()).unwrap_or_else(|| std::process::abort()) });
    // 未交付的强持有（strong reference）保持普通局部所有权（ownership），覆盖观测展开（unwinding）。
    let cloned = TaskRef::clone(&task);
    #[cfg(feature = "observe")]
    if let Some(observer) = &unsafe { task.pointer.as_ref() }.observer { observer.event(task.id(), TaskEvent::WakerClone); }
    let cloned = ManuallyDrop::new(cloned);
    std::task::RawWaker::new(cloned.pointer.as_ptr().cast(), &NATIVE_WAKER_VTABLE)
}
#[allow(unused_variables)]
unsafe fn native_wake_inner(pointer: *const ()) -> () {
    let task = TaskRef { pointer: NonNull::new(pointer.cast_mut().cast::<Header>()).unwrap_or_else(|| std::process::abort()) };
    #[cfg(feature = "observe")]
    if let Some(observer) = &unsafe { task.pointer.as_ref() }.observer { observer.event(task.id(), TaskEvent::WakerWake); }
    task.wake();
}
#[allow(unused_variables)]
unsafe fn native_wake_by_ref_inner(pointer: *const ()) -> () {
    let task = ManuallyDrop::new(TaskRef { pointer: NonNull::new(pointer.cast_mut().cast::<Header>()).unwrap_or_else(|| std::process::abort()) });
    #[cfg(feature = "observe")]
    if let Some(observer) = &unsafe { task.pointer.as_ref() }.observer { observer.event(task.id(), TaskEvent::WakerWakeByRef); }
    task.wake();
}
#[allow(unused_variables)]
unsafe fn native_drop_inner(pointer: *const ()) -> () {
    let task = TaskRef { pointer: NonNull::new(pointer.cast_mut().cast::<Header>()).unwrap_or_else(|| std::process::abort()) };
    #[cfg(feature = "observe")]
    if let Some(observer) = &unsafe { task.pointer.as_ref() }.observer { observer.event(task.id(), TaskEvent::WakerDrop); }
    drop(task);
}
#[allow(unused_variables)]
unsafe fn storage_run_inner<F, S, M, L>(pointer: NonNull<Header>, owner: &TaskThread) -> RunOutcome
where
    F: Future + 'static,
    F::Output: 'static,
    S: Schedule,
    M: Send + Sync + 'static,
    L: StorageLinks + 'static,
{
    // SAFETY: 原布局（layout）与排他轮询（poll）权限由虚函数表（vtable）调用方保证。
    let storage = unsafe { pointer.cast::<TaskStorage<F, S, M, L>>().as_ref() };
    let header = &storage.header;
    let task = ManuallyDrop::new(TaskRef { pointer });
    let waker = task.waker();
    let mut context = std::task::Context::from_waker(&waker);
    #[cfg(feature = "observe")]
    if let Some(observer) = &header.observer { observer.event(header.id, TaskEvent::PollBegin); }
    // SAFETY: 当前原值（Future）已原址固定，守卫（CurrentGuard）仅覆盖一次同步轮询（poll）。
    let current = unsafe { CurrentGuard::enter(pointer) };
    let polled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // SAFETY: 不移动载荷（payload），仅投影仍存活的原异步计算（Future）。
        let payload = unsafe { &mut *storage.payload.get().cast::<Payload<F>>() };
        match payload {
            Payload::Future(future) => unsafe { std::pin::Pin::new_unchecked(future) }.poll(&mut context),
            _ => std::process::abort(),
        }
    }));
    let backtraces = if polled.is_err() {
        CURRENT_PANIC_BACKTRACES.with(|records| {
            match records.borrow_mut().take() {
                Some((id, records)) if id == header.id => records,
                _ => Vec::new(),
            }
        })
    } else { Vec::new() };
    drop(current);
    let (reason, result) = match polled {
        Ok(std::task::Poll::Pending) => {
            let _ = header.state.try_update(std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst,
                |state| Some((state & !ACCESS_MASK) | ACCESS_HANDOFF));
            #[cfg(feature = "observe")]
            if let Some(observer) = &header.observer { observer.event(header.id, TaskEvent::PollPending); }
            return RunOutcome::Pending;
        }
        Ok(std::task::Poll::Ready(output)) => (REASON_COMPLETED, Ok(output)),
        Err(payload) => (REASON_PANICKED, Err(TaskError::Panicked(PanicRecord { payload, backtraces }))),
    };
    // 本次就绪（Ready）胜过并发取消（cancellation）；先确立原因，原值清理（cleanup）仍由独立守卫保护。
    let _ = header.state.try_update(std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst,
        |state| Some((state & !(ACCESS_MASK | REASON_MASK | WAKE_PENDING)) | ACCESS_EXITING | (reason << REASON_SHIFT)));
    // SAFETY: 已停止轮询（poll），所属线程（thread）和活动计数覆盖同一原值清理（cleanup）。
    unsafe { storage_drop_future::<F, S, M, L>(pointer, Some(owner)); }
    // SAFETY: F已合法析构（drop），这一区域现在只保存尚未发布的唯一输出（output）。
    unsafe { storage.payload.get().write(ManuallyDrop::new(Payload::Output(result))); }
    let mut state = header.state.load(std::sync::atomic::Ordering::SeqCst);
    loop {
        if state & RESULT_INTEREST == 0 {
            // SAFETY: 领取权已放弃且输出（output）未发布，完成者在原线程（thread）独占清理。
            let abandoned = unsafe { storage.payload.get().cast::<Payload<F>>().read() };
            unsafe { storage.payload.get().write(ManuallyDrop::new(Payload::Empty)); }
            let _ = header.state.try_update(std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst,
                |current| Some((current & !ACCESS_MASK) | ACCESS_ENDED));
            drop(abandoned);
            break;
        }
        let next = (state & !ACCESS_MASK) | ACCESS_ENDED | OUTPUT_PRESENT;
        match header.state.compare_exchange(state, next, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
            Ok(_) => break,
            Err(actual) => state = actual,
        }
    }
    // 唯一完成者（completion producer）在发布后交接结果等待槽（NotifySlot）。
    let mut notification = header.result_waiter.state.load(std::sync::atomic::Ordering::SeqCst);
    let result_waker = loop {
        match notification {
            0 => match header.result_waiter.state.compare_exchange(0, 2, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
                Ok(_) => {
                    // SAFETY: 状态2独占槽（slot），先恢复权限再调用外部代码。
                    let waker = unsafe { (*header.result_waiter.value.get()).take() };
                    header.result_waiter.state.store(0, std::sync::atomic::Ordering::SeqCst);
                    break waker;
                }
                Err(actual) => notification = actual,
            },
            1 => match header.result_waiter.state.compare_exchange(1, 3, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
                Ok(_) => break None,
                Err(actual) => notification = actual,
            },
            2 | 3 => break None,
            _ => std::process::abort(),
        }
    };
    // 结果状态（result state）已恢复，再隔离取消等待（cancellation wait）旧身份并执行外部完成通知。
    let mut notification = header.cancel_waiter.state.load(std::sync::atomic::Ordering::SeqCst);
    let cancel_observer = loop {
        match notification {
            0 => match header.cancel_waiter.state.compare_exchange(0, 2, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
                Ok(_) => {
                    // SAFETY: 状态2持有通知权限（notification permission），旧值与旧key一并移出。
                    let observer = unsafe { (*header.cancel_waiter.value.get()).take() };
                    header.cancel_wait_key.store(0, std::sync::atomic::Ordering::SeqCst);
                    header.state.fetch_and(!CANCEL_WAIT_INTEREST, std::sync::atomic::Ordering::SeqCst);
                    header.cancel_waiter.state.store(0, std::sync::atomic::Ordering::SeqCst);
                    break observer;
                }
                Err(actual) => notification = actual,
            },
            1 => match header.cancel_waiter.state.compare_exchange(1, 3, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
                Ok(_) => break None,
                Err(actual) => notification = actual,
            },
            2 | 3 => break None,
            _ => std::process::abort(),
        }
    };
    if let Some(observer) = cancel_observer {
        let task = ManuallyDrop::new(TaskRef { pointer });
        observer.complete(task.cancellation_outcome().unwrap_or_else(|| std::process::abort()));
    }
    #[cfg(feature = "observe")]
    if let Some(observer) = &header.observer {
        observer.event(header.id, if reason == REASON_COMPLETED { TaskEvent::PollReady } else { TaskEvent::PollPanicked });
        observer.event(header.id, TaskEvent::PollingStopped);
        observer.event(header.id, TaskEvent::FutureDropped);
    }
    if let Some(waker) = result_waker { waker.wake(); }
    RunOutcome::Finished(if reason == REASON_COMPLETED { TerminationKind::Completed } else { TerminationKind::Panicked })
}
#[allow(unused_variables)]
unsafe fn storage_drop_future_inner<F, S, M, L>(
    pointer: NonNull<Header>,
    owner: Option<&TaskThread>,
) -> ()
where
    F: Future + 'static,
    F::Output: 'static,
    S: Schedule,
    M: Send + Sync + 'static,
    L: StorageLinks + 'static,
{
    // SAFETY: 虚函数表（vtable）保证布局（layout）匹配，本份持有覆盖原址。
    let storage = unsafe { pointer.cast::<TaskStorage<F, S, M, L>>().as_ref() };
    let header = &storage.header;
    let state = header.state.load(std::sync::atomic::Ordering::SeqCst);
    if matches!(state & ACCESS_MASK, ACCESS_DROPPED | ACCESS_ENDED | ACCESS_UNCONFIRMED) { return; }
    assert_eq!(state & ACCESS_MASK, ACCESS_EXITING);
    match owner {
        Some(owner) => {
            assert_eq!(owner.thread, std::thread::current().id());
            assert_eq!(header.owner.load(std::sync::atomic::Ordering::SeqCst), owner.id.get());
            assert!(owner.active.get() > 0);
        }
        None => assert_eq!(header.owner.load(std::sync::atomic::Ordering::SeqCst), 0),
    }
    let mut guard = FutureDropGuard { header, armed: true };
    // SAFETY: 原异步计算（Future）仍在原址且已永久停轮询（poll），仅析构（drop）内部F。
    let payload = unsafe { &mut *storage.payload.get().cast::<Payload<F>>() };
    match payload {
        Payload::Future(future) => unsafe { std::ptr::drop_in_place(future); },
        _ => std::process::abort(),
    }
    guard.armed = false;
    let reason = (state & REASON_MASK) >> REASON_SHIFT;
    let error = match reason {
        REASON_ABANDONED => Some(TaskError::Abandoned),
        REASON_CANCELLED => Some(TaskError::Cancelled),
        REASON_COMPLETED | REASON_PANICKED => None,
        _ => std::process::abort(),
    };
    // SAFETY: 原F已结束，只写新载荷（payload），绝不隐式再次析构（drop）旧变体。
    unsafe { std::ptr::write(storage.payload.get(), ManuallyDrop::new(Payload::Empty)); }
    let _ = header.state.try_update(std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst,
        |current| Some((current & !ACCESS_MASK) | ACCESS_DROPPED));
    let Some(error) = error else {
        // 正常输出（output）与恐慌载荷（panic payload）由本次轮询（poll）完成者保留；
        // 其发布责任结束前不能调用外部通知（notification）或观测接收者（TaskObserver）。
        return;
    };
    // SAFETY: 本入口仍独占载荷（payload），先写唯一错误（error），再与领取意愿原子仲裁。
    unsafe { storage.payload.get().write(ManuallyDrop::new(Payload::Output(Err(error)))); }
    let mut state = header.state.load(std::sync::atomic::Ordering::SeqCst);
    loop {
        if state & RESULT_INTEREST == 0 {
            // SAFETY: 未发布的错误（error）不含业务输出（output），仅本完成者可清理。
            let abandoned = unsafe { storage.payload.get().cast::<Payload<F>>().read() };
            unsafe { storage.payload.get().write(ManuallyDrop::new(Payload::Empty)); }
            let _ = header.state.try_update(std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst,
                |current| Some((current & !ACCESS_MASK) | ACCESS_ENDED));
            drop(abandoned);
            break;
        }
        let next = (state & !ACCESS_MASK) | ACCESS_ENDED | OUTPUT_PRESENT;
        match header.state.compare_exchange(state, next, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
            Ok(_) => break,
            Err(actual) => state = actual,
        }
    }
    let mut notification = header.result_waiter.state.load(std::sync::atomic::Ordering::SeqCst);
    let result_waker = loop {
        match notification {
            0 => match header.result_waiter.state.compare_exchange(0, 2, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
                Ok(_) => {
                    // SAFETY: 状态2独占通知槽（notification slot），先移出再恢复空闲权限。
                    let waker = unsafe { (*header.result_waiter.value.get()).take() };
                    header.result_waiter.state.store(0, std::sync::atomic::Ordering::SeqCst);
                    break waker;
                }
                Err(actual) => notification = actual,
            },
            1 => match header.result_waiter.state.compare_exchange(1, 3, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
                Ok(_) => break None,
                Err(actual) => notification = actual,
            },
            2 | 3 => break None,
            _ => std::process::abort(),
        }
    };
    // 结果状态（result state）已恢复，再隔离取消等待（cancellation wait）旧身份并执行外部完成通知。
    let mut notification = header.cancel_waiter.state.load(std::sync::atomic::Ordering::SeqCst);
    let cancel_observer = loop {
        match notification {
            0 => match header.cancel_waiter.state.compare_exchange(0, 2, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
                Ok(_) => {
                    // SAFETY: 状态2持有通知权限（notification permission），旧值与旧key一并移出。
                    let observer = unsafe { (*header.cancel_waiter.value.get()).take() };
                    header.cancel_wait_key.store(0, std::sync::atomic::Ordering::SeqCst);
                    header.state.fetch_and(!CANCEL_WAIT_INTEREST, std::sync::atomic::Ordering::SeqCst);
                    header.cancel_waiter.state.store(0, std::sync::atomic::Ordering::SeqCst);
                    break observer;
                }
                Err(actual) => notification = actual,
            },
            1 => match header.cancel_waiter.state.compare_exchange(1, 3, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
                Ok(_) => break None,
                Err(actual) => notification = actual,
            },
            2 | 3 => break None,
            _ => std::process::abort(),
        }
    };
    if let Some(observer) = cancel_observer {
        let task = ManuallyDrop::new(TaskRef { pointer });
        observer.complete(task.cancellation_outcome().unwrap_or_else(|| std::process::abort()));
    }
    if let Some(waker) = result_waker { waker.wake(); }
    #[cfg(feature = "observe")]
    if let Some(observer) = &header.observer {
        observer.event(header.id, TaskEvent::PollingStopped);
        observer.event(header.id, TaskEvent::FutureDropped);
    }
}
#[allow(unused_variables)]
unsafe fn storage_destroy_inner<F, S, M, L>(pointer: NonNull<Header>) -> ()
where
    F: Future + 'static,
    F::Output: 'static,
    S: Schedule,
    M: Send + Sync + 'static,
    L: StorageLinks + 'static,
{
    // SAFETY: 末份强持有（strong reference）已独占存储（storage），载荷（payload）不再访问原值。
    let mut storage = unsafe { Box::from_raw(pointer.cast::<TaskStorage<F, S, M, L>>().as_ptr()) };
    let state = storage.header.state.load(std::sync::atomic::Ordering::SeqCst);
    if state >> REF_SHIFT != 0 || state & (RUNNABLE_ISSUED | OUTPUT_PRESENT) != 0 {
        std::process::abort();
    }
    // SAFETY: 最后持有已归零，无其它元数据借用（metadata borrow）；一次移出后原ManuallyDrop不再析构。
    // 普通局部所有权（ownership）覆盖后续观测展开（observer unwind），防止手工字段（ManuallyDrop）泄漏。
    let metadata = unsafe { ManuallyDrop::take(&mut storage.metadata) };
    #[cfg(feature = "observe")]
    if let Some(observer) = &storage.header.observer {
        observer.event(storage.header.id, TaskEvent::ReclaimStarted);
    }
    drop(metadata);
    drop(storage);
}
#[allow(unused_variables)]
unsafe fn storage_schedule_inner<F, S, M, L>(
    pointer: NonNull<Header>,
    task: crate::runnable::Runnable,
) -> ()
where
    F: Future + 'static,
    F::Output: 'static,
    S: Schedule,
    M: Send + Sync + 'static,
    L: StorageLinks + 'static,
{
    let keep_alive = task.task.clone();
    // SAFETY: 独立强持有（strong reference）覆盖调度（Schedule）的正常返回及展开（unwinding）。
    let storage = unsafe { pointer.cast::<TaskStorage<F, S, M, L>>().as_ref() };
    storage.schedule.schedule(task);
    drop(keep_alive);
}
#[allow(unused_variables)]
unsafe fn storage_take_result_inner<F, S, M, L>(
    pointer: NonNull<Header>,
    destination: NonNull<()>,
) -> ()
where
    F: Future + 'static,
    F::Output: 'static,
    S: Schedule,
    M: Send + Sync + 'static,
    L: StorageLinks + 'static,
{
    // SAFETY: 布局（layout）及结果类型（output type）由原创建入口与唯一领取者保持。
    let storage = unsafe { pointer.cast::<TaskStorage<F, S, M, L>>().as_ref() };
    let payload = storage.payload.get().cast::<Payload<F>>();
    // SAFETY: 原F已清理，结果（output）已被唯一认领，只移动一次并留下空载荷（payload）。
    let result = match unsafe { payload.read() } {
        Payload::Output(result) => result,
        _ => std::process::abort(),
    };
    unsafe { payload.write(Payload::Empty); destination.cast::<Result<F::Output, TaskError>>().as_ptr().write(result); }
}
#[allow(unused_variables)]
unsafe fn storage_metadata_inner<F, S, M, L>(
    pointer: NonNull<Header>,
    requested: TypeId,
) -> Option<NonNull<()>>
where
    F: Future + 'static,
    F::Output: 'static,
    S: Schedule,
    M: Send + Sync + 'static,
    L: StorageLinks + 'static,
{
    if requested != TypeId::of::<M>() { return None; }
    // SAFETY: 原布局（layout）包含仍存活的ManuallyDrop<M>，仅投影元数据（metadata）字段。
    let raw = pointer.cast::<TaskStorage<F, S, M, L>>().as_ptr();
    Some(unsafe { NonNull::new_unchecked(std::ptr::addr_of_mut!((*raw).metadata).cast::<M>()).cast() })
}
#[allow(unused_variables)]
unsafe fn storage_queue_entry_inner<F, S, M, L>(
    pointer: NonNull<Header>,
) -> Option<NonNull<QueueEntry>>
where
    F: Future + 'static,
    F::Output: 'static,
    S: Schedule,
    M: Send + Sync + 'static,
    L: StorageLinks + 'static,
{
    let storage = pointer.cast::<TaskStorage<F, S, M, L>>().as_ptr();
    // SAFETY: 只投影已初始化的links字段（field），不借出原值（Future）。
    let links = unsafe { NonNull::new_unchecked(std::ptr::addr_of_mut!((*storage).links)) };
    unsafe { L::node(links) }
}

#[allow(unused_variables)]
unsafe fn no_link_node_inner(pointer: NonNull<()>) -> Option<NonNull<QueueEntry>> {
    None
}
#[allow(unused_variables)]
unsafe fn local_link_node_inner(pointer: NonNull<QueueEntry>) -> Option<NonNull<QueueEntry>> {
    Some(pointer)
}

#[allow(unused_variables)]
fn ref_abandon_runnable_inner(this: &TaskRef) {
    // SAFETY: 调用者持有唯一执行责任，字段（field）的TaskRef尚未释放。
    let header = unsafe { this.pointer.as_ref() };
    let bound = header.owner.load(std::sync::atomic::Ordering::SeqCst) != 0;
    let mut prepared = None;
    let mut state = header.state.load(std::sync::atomic::Ordering::SeqCst);
    loop {
        let access = state & ACCESS_MASK;
        if matches!(access, ACCESS_RUNNING | ACCESS_HANDOFF) || state & RUNNABLE_ISSUED == 0 {
            std::process::abort();
        }
        if access != ACCESS_IDLE {
            let next = state & !RUNNABLE_ISSUED;
            match header.state.compare_exchange(state, next, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
                Ok(_) => return,
                Err(actual) => { state = actual; continue; }
            }
        }
        if bound && prepared.is_none() {
            prepared = Some(this.clone());
            state = header.state.load(std::sync::atomic::Ordering::SeqCst);
            continue;
        }
        let mut next = (state & !(ACCESS_MASK | WAKE_PENDING)) | ACCESS_EXITING;
        if state & REASON_MASK == 0 {
            if state & (ADMITTED | STARTED) == 0 {
                next |= REASON_ABANDONED << REASON_SHIFT;
            } else {
                next |= (REASON_CANCELLED << REASON_SHIFT) | CANCEL_RECORDED;
            }
        }
        if !bound { next &= !RUNNABLE_ISSUED; }
        match header.state.compare_exchange(state, next, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst) {
            Ok(_) => {
                if let Some(task) = prepared.take() {
                    // SAFETY: 新强持有（strong reference）及唯一清理票（Runnable）已连续承接旧责任。
                    unsafe { (header.vtable.schedule)(this.pointer, crate::runnable::Runnable { task }); }
                } else {
                    // SAFETY: 未绑定创建（Send creation）的原值，放弃者已取得排他清理权。
                    unsafe { (header.vtable.drop_future)(this.pointer, None); }
                }
                return;
            }
            Err(actual) => state = actual,
        }
    }
}

#[allow(unused_variables)]
fn task_activity_enter_inner(owner: &TaskThread) -> TaskActivityGuard<'_> {
    owner.active.set(owner.active.get().checked_add(1).unwrap_or_else(|| std::process::abort()));
    TaskActivityGuard { owner }
}

#[allow(unused_variables)]
fn task_activity_drop_inner(this: &mut TaskActivityGuard<'_>) {
    this.owner.active.set(this.owner.active.get().checked_sub(1).unwrap_or_else(|| std::process::abort()));
}

#[allow(unused_variables)]
fn run_guard_drop_inner(this: &mut RunGuard<'_>) {
    if !this.armed { return; }
    // SAFETY: 执行守卫（RunGuard）借用仍存活的TaskRef，展开（unwinding）中只恢复原子事实。
    let header = unsafe { this.task.pointer.as_ref() };
    let _ = header.state.try_update(std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst, |state| {
        if matches!(state & ACCESS_MASK, ACCESS_DROPPED | ACCESS_ENDED | ACCESS_UNCONFIRMED) { return Some(state); }
        let mut next = (state & !(ACCESS_MASK | WAKE_PENDING)) | ACCESS_EXITING;
        if state & REASON_MASK == 0 { next |= (REASON_CANCELLED << REASON_SHIFT) | CANCEL_RECORDED; }
        Some(next)
    });
}

#[allow(unused_variables)]
fn future_drop_guard_drop_inner(this: &mut FutureDropGuard<'_>) {
    if this.armed {
        let _ = this.header.state.try_update(
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst,
            |state| Some((state & !ACCESS_MASK) | ACCESS_UNCONFIRMED),
        );
    }
}

#[allow(unused_variables)]
fn ref_waiting_for_wake_inner(this: &TaskRef) -> bool {
    todo!("{}", r#"目标与范围：实现TaskRef::is_waiting_for_wake的同步只读判定；
ControlHandle和TaskRegistration共用此结果。本操作不聚合运行时（runtime）统计，
不取得原异步计算（Future）的访问权，不安排执行或清理。

前提与依赖：this为仍存活的合法TaskRef，其强持有（strong reference）保住Header；
复用同一实际模块（module）的已有状态编码及首版SeqCst协议，不增加字段（field）。
先取得受this借用覆盖的Header，只对header.state执行一次SeqCst原子读取（atomic load）。
unsafe处说明this保活、共享读取及不访问原值的安全条件。

精确行为：对同一次样本，访问部分必须为ACCESS_IDLE，STARTED必须非零，
WAKE_PENDING、RUNNABLE_ISSUED、CANCEL_RECORDED及REASON_MASK必须全部为零才返回true；
否则返回false。引用计数（reference count）高位及其它无关位不参与判定。
这对应原根计算已挂起（Pending）并完成交接，当前无执行访问、待处理唤醒（wake-up）、
已发出的可执行持有、取消（cancellation）或退出。运行中和挂起交接（Pending handoff）
均为false；已经唤醒但尚未物理入队（enqueue）也必须为false。

状态与时序：一次取样不是未来保证。唤醒（wake-up）或取消（cancellation）在取样后
生效时可以返回此前事实，不循环等待稳定。直接run而没有admit的任务（Task）也可能
真正等唤醒，不能增加ADMITTED作为本判定前提；服务统计另以自己已接纳登记筛选。
不得用TaskSnapshot::execution的Idle或存活数量减队列（queue）数代替此判定。

成本与禁忌：O(1)时间及额外空间；不分配、不加锁、不自旋、不改状态、不增减强持有，
不调用用户代码、调度（scheduling）、Waker、轮询（poll）、取消（cancel）或析构（drop）。
保留所有已有状态布局、生命周期（lifecycle）操作及原测试断言。

验证判据：经真实创建／执行／唤醒（wake-up）／取消（cancellation）入口核未开始false、
poll中false、已返回Pending但交接未完false、稳定等待true、已唤醒未入队false、
已发出Runnable false、取消／正常结束／清理未确认false。有效登记与共享控制对同一次
受控事实分类一致；构造直接run未admit的合法等待，仍可true。同步竞争只要求样本语义，
不能把时间上独立的两次读取硬断言为同刻。优先完成此独立查询，再接入服务统计。"#)
}

#[allow(unused_variables)]
fn current_metadata_inner<M: Send + Sync + 'static, R>(
    read: impl FnOnce(&M) -> R,
) -> Result<R, CurrentTaskAccessError> {
    todo!("{}", r#"目标与范围：实现try_with_current_metadata的一次同步借用（borrowing）。
当前本库任务（Task）由已有CurrentGuard和CURRENT_TASK标识；本操作只定位同一任务（Task）
的创建时附加数据（task metadata），不访问原异步计算（Future）或结果（output）。
不引入执行器（executor）、运行时（runtime）默认实例或另一份任务（Task）。

前提与依赖：CurrentGuard只在原F::poll动态范围安装CurrentTask，当前执行的存活持有
覆盖Header和M。沿现有ref_metadata_inner使用的Header.vtable.metadata及TypeId核对
取回原M投影；该虚函数表（vtable）返回的指针仅在TypeId完全匹配时解释为M。
复用已有存储布局，不将Header指针直接强转为猜测的TaskStorage实例。

精确行为：用CURRENT_TASK.try_with取得当前身份的复制值，并在调用read前结束所有TLS
访问借用。TLS不可用或无当前身份，返回Err(CurrentTaskAccessError::NoCurrentTask)，
不调用read。存在当前任务（Task）但既有metadata投影返回None，返回
Err(CurrentTaskAccessError::MetadataTypeMismatch)，不调用read、不生成默认M。
匹配时只借用&M，同步调用read恰好一次，正常得到R后返回Ok(R)。不得为错误分支
调用read来取得默认返回值；不克隆（clone）M，不转交M所有权（ownership）。

借用与保活：M保持Send + Sync + 'static；R不新增Send、Sync或'static约束。
FnOnce(&M) -> R必须阻止所给&M逃逸到R或跨异步等待（await），但允许R本身持有
调用者原先合法拥有的非Send数据或外部短借用。每处unsafe均说明当前轮询（poll）
持有覆盖投影、TypeId已匹配、共享引用（shared reference）有效且只在本次调用中使用。
CURRENT_TASK.pointer只是借来的地址，绝不能据此构造会正常Drop的TaskRef并凭空归还
一份引用计数（reference count）。可直接沿已有虚函数表（vtable）投影，不需要增加
强持有（strong reference）；若使用借用包装，必须保证绝不执行其所有权析构（drop）。

嵌套、取消与异常：此入口不安装或清除当前身份、不新增TLS或恢复栈。
合法同步嵌套沿原CurrentGuard恢复，当前任务（Task）内提出取消（cancellation）
也不切断本次合法借用；不在这里重设运行状态（state）或处理取消请求。
read可以经M自己的安全接口（API）更新内部值，也可以执行其它合法业务操作；
因此整个调用不保证纯函数（pure function）或零副作用。read恐慌（panic）沿原调用范围
传播，不新增catch_unwind；原F::poll已有捕获及CurrentGuard恢复继续负责。

成本与禁忌：身份定位、TypeId及投影为O(1)时间和额外空间，不额外分配、不加锁、
不增减强持有、不新增原子写或每次poll维护；read自己的时间和同步成本单独承担。
不授予ControlHandle或Waker的M访问权，不将运行时（runtime）TLS当Task身份，
不改CurrentGuard、Header、原F清理、T66放弃执行持有或所属域析构（owner-domain drop）行为。

验证判据：经真实create／create_local及Runnable::run核正确M得到R，错误M及无当前任务
分别返回精确错误且read调用数为零；裸block_on或poll外回调（callback）不能伪造当前身份。
核M的安全内部更新，R可为非Send数据及合法外部短借用；借出所给&M的用法必须编译失败。
核合法嵌套访问结束恢复外层M，read发生panic后原错误传播／当前身份恢复及下一任务访问
均沿原行为；取消请求不使当前借用悬空。不得通过测试侧另装CURRENT_TASK代补生产能力。"#)
}
