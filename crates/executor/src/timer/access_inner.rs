#[allow(unused_variables)]
fn default_retry_owner_new_inner<C, L>(
    notifier: ThreadNotifier,
    batch: std::num::NonZeroUsize,
) -> RetryOwner<CancelRetryNode<C, L>>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    let value = Arc::new(CancelRetryNode {
        facts: TimerRegistration::new(TimerRegistrationKind::Task),
        links: Links::new(),
        origin: Weak::new(),
        retry: Weak::new(),
        membership: AtomicU8::new(NODE_IDLE),
        error: UnsafeCell::new(None),
        stub: true,
        pin: PhantomPinned,
    });
    RetryOwner::new(RetryHandle { value, on_drop: None }, notifier, batch)
}

#[allow(unused_variables)]
fn default_access_clone_inner<C: ExecutorClock<Instant = GlobalManualInstant>, L: 'static>(
    this: &DefaultTimerAccess<C, L>,
) -> DefaultTimerAccess<C, L> {
    DefaultTimerAccess { shared: Arc::clone(&this.shared) }
}

#[allow(unused_variables)]
fn default_registration_clone_inner<C: ExecutorClock<Instant = GlobalManualInstant>, L: 'static>(
    this: &DefaultTimerRegistration<C, L>,
) -> DefaultTimerRegistration<C, L> {
    DefaultTimerRegistration { node: Arc::clone(&this.node) }
}

#[allow(unused_variables)]
fn default_deadline_inner<C, L>(
    this: &DefaultTimerAccess<C, L>,
    duration: Duration,
) -> pi_result::RawResult<GlobalManualInstant, TimerError>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    // 本服务一旦开始退出即不可再取时，也不初始化或另建时间轴。
    if this.shared.owner_exiting.load(std::sync::atomic::Ordering::Acquire) {
        return Err(TimerError::ServiceClosed);
    }
    // 只真实取时一次；相加只用该原时刻，失败保留原报告（Report）。
    let now = this.shared.clock.now().map_err(TimerError::Clock)?;
    this.shared
        .clock
        .checked_add(&now, duration)
        .map_err(TimerError::DeadlineOutOfRange)
}
fn default_prepare_inner<C, L>(
    this: &DefaultTimerAccess<C, L>,
    kind: TimerRegistrationKind,
) -> pi_result::RawResult<DefaultTimerRegistration<C, L>, TimerError>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    // Owner 开始退出后不再准备新的登记；不取时、不入队、不创建任务。
    if this.shared.owner_exiting.load(std::sync::atomic::Ordering::Acquire) {
        return Err(TimerError::ServiceClosed);
    }
    // 一份节点承载该登记的全部后续重试；只持原来源与重试环的 Weak，不形成强环。
    let node = Arc::new(CancelRetryNode {
        facts: TimerRegistration::new(kind),
        links: Links::new(),
        origin: Arc::downgrade(&this.shared),
        retry: Arc::downgrade(&this.shared.retries),
        membership: AtomicU8::new(NODE_IDLE),
        error: UnsafeCell::new(None),
        stub: false,
        pin: PhantomPinned,
    });
    Ok(DefaultTimerRegistration { node })
}
#[allow(unused_variables)]
fn default_register_inner<C, L>(
    this: &DefaultTimerAccess<C, L>,
    deadline: GlobalManualInstant,
    item: DefaultTimerItem<C, L>,
) -> pi_result::RawResult<CancelTimerKey, TimerSubmitError<DefaultTimerItem<C, L>>>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    // 开始退出后不再接收：返同一原值与关闭原因，不交给来源。
    if this.shared.owner_exiting.load(std::sync::atomic::Ordering::Acquire) {
        return Err(TimerSubmitError { item, cause: TimerQueueError::Closed });
    }
    // 同一共享来源一次提交；接受与拒绝原样保留（已接受后的通知异常不伪装为拒绝）。
    pi_async_task_queue::TimerQueueHandle::try_push(&this.shared.backend, deadline, item)
}
fn default_register_local_inner<C, L>(
    this: &DefaultTimerAccess<C, L>,
    deadline: GlobalManualInstant,
    item: DefaultTimerItem<C, L>,
) -> pi_result::RawResult<CancelTimerKey, TimerSubmitError<DefaultTimerItem<C, L>>>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    // 同一流程先核退出意图；随后经原本地接入核服务与线程，缺权限／错目标返 WrongOwner。
    if this.shared.owner_exiting.load(std::sync::atomic::Ordering::Acquire) {
        return Err(TimerSubmitError { item, cause: TimerQueueError::Closed });
    }
    L::try_push_local(this.shared.runtime, deadline, item)
}
fn default_cancel_inner<C, L>(
    this: &DefaultTimerAccess<C, L>,
    key: &CancelTimerKey,
) -> pi_result::RawResult<TimerCancelRequestOutcome, TimerQueueError>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    use pi_async_task_queue::TimerQueueStatus;

    // 已确认退出意图由本来源关闭者接管，不再投原取消。
    if this.shared.owner_exiting.load(std::sync::atomic::Ordering::Acquire) {
        return Ok(TimerCancelRequestOutcome::HandledByClose);
    }
    match pi_async_task_queue::TimerQueueHandle::request_cancel(&this.shared.backend, key) {
        // 原 Ok 只映射为已记录；即便随后发生关闭也不覆盖已存在的记录或合并事实。
        Ok(()) => Ok(TimerCancelRequestOutcome::Recorded),
        Err(TimerQueueError::Closed) => {
            // 只有同一来源确实已停收／退出时才算关闭接管，否则保留原 Closed。
            match pi_async_task_queue::TimerQueueHandle::status(&this.shared.backend) {
                TimerQueueStatus::Closing | TimerQueueStatus::LocalExited => {
                    Ok(TimerCancelRequestOutcome::HandledByClose)
                }
                TimerQueueStatus::Accepting => Err(TimerQueueError::Closed),
            }
        }
        // 其余错误（WrongQueue／Busy／Backend 等）保留原值与报告链。
        Err(other) => Err(other),
    }
}
fn default_defer_inner<C, L>(
    this: &DefaultTimerAccess<C, L>,
    registration: DefaultTimerRegistration<C, L>,
    error: Option<TimerQueueError>,
    confirmation: Confirmation<'_, CancelTimerKey>,
) -> ()
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
        use std::sync::atomic::Ordering::{AcqRel, Relaxed, Release};

    let node = registration.node;
    // 原来源实际已结束：直接结清转交，沿原错误归属结束。
    let Some(source) = node.origin.upgrade() else {
        confirmation.source_finished();
        drop(error);
        return;
    };
    // 一次强 CAS 取得错误槽写资格；已有参与者不读写槽、不重复发布。
    if node
        .membership
        .compare_exchange(NODE_IDLE, NODE_PUBLISHING, AcqRel, Relaxed)
        .is_err()
    {
        drop(error);
        return;
    }
    // 独占资格：空槽保第一份原错误，后来的报告只在槽外释放。
    // 安全（safety）：只有取得 PUBLISHING 的持有者访问错误槽，且仅在入队前。
    let slot = unsafe { &mut *node.error.get() };
    let later_error = if slot.is_none() {
        *slot = error;
        None
    } else {
        error
    };
    // 仍须接续：以 Release 标记 QUEUED 后立即交原发布助手，包含同一确认。
    node.membership.store(NODE_QUEUED, Release);
    publish_retry(&source, node, confirmation);
    // 原节点（node）已由来源保管，释放后来报告时不再悬置转交。
    drop(later_error);
}

#[allow(unused_variables)]
fn default_retry_resume_inner<C, L>(this: &Arc<CancelRetryNode<C, L>>) -> ()
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    // 仅真实原生安全出队、且消费者／链接引用已结束后调用：先由 QUEUED 恢复 IDLE。
    this.membership
        .store(NODE_IDLE, std::sync::atomic::Ordering::Release);
    // 原来源仍存活才构造同源访问与同节点登记；失效则不重建源（已交退出责任）。
    let Some(shared) = this.origin.upgrade() else {
        return;
    };
    let access = DefaultTimerAccess { shared };
    let registration = DefaultTimerRegistration { node: Arc::clone(this) };
    // 核心接续一次：已结束或无待交接时不制造新工作；仍暂不可交付时由核心再次 defer。
    pi_async_runtime_core::time::resume_timer_cancel(&access, registration);
}
fn default_retry_into_ptr_inner<C: ExecutorClock<Instant = GlobalManualInstant>, L: 'static>(
    handle: RetryHandle<CancelRetryNode<C, L>>,
) -> NonNull<CancelRetryNode<C, L>> {
    let handle = std::mem::ManuallyDrop::new(handle);
    // SAFETY: 唯一持有（unique handle）已抑制析构（drop）；只移出原Arc，不调用单位结算（unit accounting）。
    let value = unsafe { std::ptr::read(std::ptr::addr_of!(handle.value)) };
    let raw = Arc::into_raw(value).cast_mut();
    // SAFETY: Arc原始指针（raw pointer）非空，原强持有（strong reference）已交给原生队列（native queue）。
    unsafe { NonNull::new_unchecked(raw) }
}

#[allow(unused_variables)]
fn default_retry_from_ptr_inner<C: ExecutorClock<Instant = GlobalManualInstant>, L: 'static>(
    ptr: NonNull<CancelRetryNode<C, L>>,
) -> RetryHandle<CancelRetryNode<C, L>> {
    // SAFETY: 原生队列（native queue）只归还一次此前交入的原Arc指针（raw Arc pointer）；此时节点已可安全取回。
    let value = unsafe { Arc::from_raw(ptr.as_ptr()) };
    let on_drop = if value.stub {
        None
    } else {
        Some(release_retry_unit::<C, L> as fn(&CancelRetryNode<C, L>))
    };
    RetryHandle { value, on_drop }
}

#[allow(unused_variables)]
fn default_retry_links_inner<C: ExecutorClock<Instant = GlobalManualInstant>, L: 'static>(
    ptr: NonNull<CancelRetryNode<C, L>>,
) -> NonNull<Links<CancelRetryNode<C, L>>> {
    // SAFETY: Linked调用方（caller）保证原节点地址（node address）有效且非空；原始字段投影（raw field projection）不创建整个节点的可写引用。
    unsafe { NonNull::new_unchecked(std::ptr::addr_of_mut!((*ptr.as_ptr()).links)) }
}

#[allow(unused_variables)]
fn default_release_unit_inner<C: ExecutorClock<Instant = GlobalManualInstant>, L: 'static>(
    node: &CancelRetryNode<C, L>,
) -> () {
    if let Some(retry) = node.retry.upgrade() {
        retry.return_unit();
    }
}
