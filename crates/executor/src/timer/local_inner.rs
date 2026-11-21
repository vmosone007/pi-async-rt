#[allow(unused_variables)]
fn driver_new_inner<C, L>(
    runtime: RuntimeId,
    clock: C,
    notifier: ThreadNotifier,
    options: CancelTimerOptions,
) -> pi_result::Result<DefaultTimerDriver<C, L>>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
        // 先真实取同源初始参考；失败保留原报告（Report）。
    let now = clock.now()?;
    // 原生来源与一份独立取消接续 stub（由既有助手构造，不对外）。
    let queue = CancelTimerQueue::new(now, notifier.clone(), options)?;
    let retries = new_retry_owner(notifier, options.work_batch_size);
    let retry_shared = retries.shared();
    let shared = Arc::new(TimerSourceShared {
        runtime,
        clock,
        backend: queue.handle(),
        retries: retry_shared,
        owner_exiting: std::sync::atomic::AtomicBool::new(false),
        local: std::marker::PhantomData,
    });
    let core = Rc::new(TimerLocalCore {
        queue: RefCell::new(Some(queue)),
        retry: RefCell::new(retries),
        shared,
        thread: std::thread::current().id(),
        close_error: RefCell::new(None),
    });
    Ok(DefaultTimerDriver { core })
}

#[allow(unused_variables)]
fn driver_from_queue_inner<C, L>(
    runtime: RuntimeId,
    clock: C,
    notifier: ThreadNotifier,
    queue: CancelTimerQueue<DefaultTimerItem<C, L>, ThreadNotifier>,
) -> pi_result::Result<DefaultTimerDriver<C, L>>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
        // 接管前核原来源仍开放、通知状态同一且初始时间合法；失败不导出半成品。
    let now = clock.now()?;
    if !matches!(pi_async_task_queue::TimerTaskQueue::status(&queue), TimerQueueStatus::Accepting) {
        return Err(pi_result::Error::new(pi_result::ErrorKind::InvalidState)
            .attach("已准备的定时队列（prepared timer queue）不再开放"));
    }
    if !queue.uses_notifier(&notifier) {
        return Err(pi_result::Error::new(pi_result::ErrorKind::InvalidInput)
            .attach("通知状态（notification state）与原线程不同源"));
    }
    pi_async_task_queue::TimerTaskQueue::next_check_at(&queue, &now).map_err(|error| error.into_report())?;
    // 沿用传入实例的工作额度与同一受限共享句柄，不重复取时或重建队列。
    let options = queue.options();
    let retries = new_retry_owner(notifier, options.work_batch_size);
    let retry_shared = retries.shared();
    let shared = Arc::new(TimerSourceShared {
        runtime,
        clock,
        backend: queue.handle(),
        retries: retry_shared,
        owner_exiting: std::sync::atomic::AtomicBool::new(false),
        local: std::marker::PhantomData,
    });
    let core = Rc::new(TimerLocalCore {
        queue: RefCell::new(Some(queue)),
        retry: RefCell::new(retries),
        shared,
        thread: std::thread::current().id(),
        close_error: RefCell::new(None),
    });
    Ok(DefaultTimerDriver { core })
}

#[allow(unused_variables)]
fn driver_local_handle_inner<C, L>(this: &DefaultTimerDriver<C, L>) -> LocalTimerHandle<C, L>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    LocalTimerHandle {
        core: Rc::downgrade(&this.core),
        runtime: this.core.shared.runtime,
        thread: this.core.thread,
    }
}

#[allow(unused_variables)]
fn driver_drop_inner<C, L>(this: &mut DefaultTimerDriver<C, L>) -> ()
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    // 驱动退出：先发布退出意图，再由本份持有者帮助一次有限关闭；错误保首份报告。
    this.core
        .shared
        .owner_exiting
        .store(true, std::sync::atomic::Ordering::Release);
    let core = Rc::clone(&this.core);
    if let Err(error) = core.finish_close_step() {
        core.record_close_error(error);
    }}

#[allow(unused_variables)]
fn local_timer_clone_inner<C, L>(this: &LocalTimerHandle<C, L>) -> LocalTimerHandle<C, L>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    LocalTimerHandle {
        core: Weak::clone(&this.core),
        runtime: this.runtime,
        thread: this.thread,
    }
}

#[allow(unused_variables)]
fn local_timer_push_inner<C, L>(
    this: &LocalTimerHandle<C, L>,
    runtime: RuntimeId,
    deadline: GlobalManualInstant,
    item: DefaultTimerItem<C, L>,
) -> pi_result::RawResult<CancelTimerKey, TimerSubmitError<DefaultTimerItem<C, L>>>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
        use pi_async_task_queue::timer::TimerQueueError;

    // 先核原服务、本线程与存续；失败返同一原值与真实分类，不跨源回退。
    let Some(core) = this.core.upgrade() else {
        return Err(TimerSubmitError { item, cause: TimerQueueError::WrongOwner });
    };
    if this.runtime != core.shared.runtime || std::thread::current().id() != this.thread {
        return Err(TimerSubmitError { item, cause: TimerQueueError::WrongOwner });
    }
    if core.shared.owner_exiting.load(std::sync::atomic::Ordering::Acquire) {
        return Err(TimerSubmitError { item, cause: TimerQueueError::Closed });
    }
    // 一次同步活动临时 Rc：借用冲突归 Busy，缺失来源归 Closed。
    let activity = TimerLocalActivityGuard { core: Rc::clone(&core), armed: true };
    let result = {
        let mut queue = match core.queue.try_borrow_mut() {
            Ok(queue) => queue,
            Err(_) => {
                drop(activity);
                return Err(TimerSubmitError { item, cause: TimerQueueError::Busy });
            }
        };
        match queue.as_mut() {
            Some(queue) => pi_async_task_queue::TimerTaskQueue::try_push_local(queue, deadline, item),
            None => Err(TimerSubmitError { item, cause: TimerQueueError::Closed }),
        }
    };
    // 先结束 RefMut，再退出活动守卫；拒绝时原值与原因原样返还。
    drop(activity);
    result
}

#[allow(unused_variables)]
fn driver_native_handle_inner<C, L>(
    this: &DefaultTimerDriver<C, L>,
) -> CancelTimerQueueHandle<DefaultTimerItem<C, L>, ThreadNotifier>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    this.core.shared.backend.clone()
}

#[allow(unused_variables)]
fn driver_local_push_inner<C, L>(
    this: &mut DefaultTimerDriver<C, L>,
    deadline: GlobalManualInstant,
    item: DefaultTimerItem<C, L>,
) -> pi_result::RawResult<CancelTimerKey, TimerSubmitError<DefaultTimerItem<C, L>>>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
        // 经同一驱动的弱本地句柄走同一活动守卫路径，不另建源、不改变本地语义。
    let handle = driver_local_handle_inner(this);
    let runtime = handle.runtime;
    local_timer_push_inner(&handle, runtime, deadline, item)
}

#[allow(unused_variables)]
fn driver_local_cancel_inner<C, L>(
    this: &mut DefaultTimerDriver<C, L>,
    key: &CancelTimerKey,
) -> pi_result::RawResult<(), TimerCancelRequestError>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
        let core = std::rc::Rc::clone(&this.core);
    let mut queue = match core.queue.try_borrow_mut() {
        Ok(queue) => queue,
        Err(_) => return Err(pi_async_task_queue::timer::TimerQueueError::Busy),
    };
    let Some(queue) = queue.as_mut() else {
        return Err(pi_async_task_queue::timer::TimerQueueError::Closed);
    };
    pi_async_task_queue::TimerTaskQueue::request_cancel_local(queue, key)
}

#[allow(unused_variables)]
fn driver_advance_inner<C, L>(
    this: &mut DefaultTimerDriver<C, L>,
    now: &GlobalManualInstant,
) -> pi_result::RawResult<TimerAdvanceProgress, TimerQueueError>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
        // 只消费显式同源时刻一次推进；不刷新时钟、不另建时间轴。
    let core = std::rc::Rc::clone(&this.core);
    let mut queue = match core.queue.try_borrow_mut() {
        Ok(queue) => queue,
        Err(_) => return Err(pi_async_task_queue::timer::TimerQueueError::Busy),
    };
    let Some(queue) = queue.as_mut() else {
        return Err(pi_async_task_queue::timer::TimerQueueError::Closed);
    };
    pi_async_task_queue::TimerTaskQueue::advance(queue, now)
}

#[allow(unused_variables)]
fn driver_due_one_inner<C, L>(
    this: &mut DefaultTimerDriver<C, L>,
) -> pi_result::RawResult<TimerTake<DefaultTimerItem<C, L>>, TimerQueueError>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
        // 同一活动借阅下一次到期取出；原条目原样交出，不在此执行或接纳。
    let core = std::rc::Rc::clone(&this.core);
    let mut queue = match core.queue.try_borrow_mut() {
        Ok(queue) => queue,
        Err(_) => return Err(pi_async_task_queue::timer::TimerQueueError::Busy),
    };
    let Some(queue) = queue.as_mut() else {
        return Err(pi_async_task_queue::timer::TimerQueueError::Closed);
    };
    pi_async_task_queue::TimerTaskQueue::try_take_due_one(queue)
}

#[allow(unused_variables)]
fn driver_due_all_inner<C, L>(
    this: &mut DefaultTimerDriver<C, L>,
) -> pi_result::RawResult<TimerTake<Vec<DefaultTimerItem<C, L>>>, TimerQueueError>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
        // 完整到期批次一次取出；不加数量上限、不拆成多次发现。
    let core = std::rc::Rc::clone(&this.core);
    let mut queue = match core.queue.try_borrow_mut() {
        Ok(queue) => queue,
        Err(_) => return Err(pi_async_task_queue::timer::TimerQueueError::Busy),
    };
    let Some(queue) = queue.as_mut() else {
        return Err(pi_async_task_queue::timer::TimerQueueError::Closed);
    };
    pi_async_task_queue::TimerTaskQueue::try_take_due_all(queue)
}

#[allow(unused_variables)]
fn driver_cleanup_inner<C, L>(
    this: &mut DefaultTimerDriver<C, L>,
) -> pi_result::Result<QueueCleanupProgress>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
        use pi_async_task_queue::management::QueueCleanupProgress;

    let core = std::rc::Rc::clone(&this.core);
    let native = {
        let mut queue = match core.queue.try_borrow_mut() {
            Ok(queue) => queue,
            Err(_) => return Ok(QueueCleanupProgress::MoreWork),
        };
        match queue.as_mut() {
            Some(queue) => pi_async_task_queue::TimerTaskQueue::process_cleanup(queue),
            None => Ok(QueueCleanupProgress::Complete),
        }
    };
    let retry = {
        let mut retries = match core.retry.try_borrow_mut() {
            Ok(retries) => retries,
            Err(_) => return Ok(QueueCleanupProgress::MoreWork),
        };
        retries.process()
    };
    let previous_error = core.close_error.borrow_mut().take();
    if let Some(error) = previous_error {
        return Err(error);
    }
    Ok(if matches!(native?, QueueCleanupProgress::Complete)
        && matches!(retry, QueueCleanupProgress::Complete)
    {
        QueueCleanupProgress::Complete
    } else {
        QueueCleanupProgress::MoreWork
    })
}

#[allow(unused_variables)]
fn driver_next_inner<C, L>(
    this: &DefaultTimerDriver<C, L>,
    now: &GlobalManualInstant,
) -> pi_result::RawResult<Option<GlobalManualInstant>, TimerQueueError>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    let queue = match this.core.queue.try_borrow() {
        Ok(queue) => queue,
        Err(_) => return Err(pi_async_task_queue::timer::TimerQueueError::Busy),
    };
    let Some(queue) = queue.as_ref() else {
        return Err(pi_async_task_queue::timer::TimerQueueError::Closed);
    };
    let native = pi_async_task_queue::TimerTaskQueue::next_check_at(queue, now)?
        .map(|next| next.max(*now));
    if !this.core.shared.retries.has_work() {
        return Ok(native);
    }
    // 待结清单位也包括在途发布。缩短检查间隔，实际能否取出仍由来源决定。
    let retry_check = now.try_add(std::time::Duration::from_millis(1))
        .unwrap_or(GlobalManualInstant::MAX);
    Ok(Some(native.map_or(retry_check, |next| next.min(retry_check))))
}

#[allow(unused_variables)]
fn driver_status_inner<C, L>(this: &DefaultTimerDriver<C, L>) -> TimerQueueStatus
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
        use pi_async_task_queue::TimerQueueStatus;

    let reported = match this.core.queue.try_borrow() {
        Ok(queue) => match queue.as_ref() {
            Some(queue) => pi_async_task_queue::TimerTaskQueue::status(queue),
            None => TimerQueueStatus::LocalExited,
        },
        Err(_) => TimerQueueStatus::Closing,
    };
    if matches!(reported, TimerQueueStatus::LocalExited) {
        return reported;
    }
    if this.core.shared.owner_exiting.load(std::sync::atomic::Ordering::Acquire) {
        return TimerQueueStatus::Closing;
    }
    reported
}

#[allow(unused_variables)]
fn driver_len_inner<C, L>(this: &DefaultTimerDriver<C, L>) -> usize
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    pi_async_task_queue::timer::TimerQueueHandle::len(&this.core.shared.backend)
}

#[allow(unused_variables)]
fn driver_empty_inner<C, L>(this: &DefaultTimerDriver<C, L>) -> bool
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    pi_async_task_queue::timer::TimerQueueHandle::len(&this.core.shared.backend) == 0
}

#[allow(unused_variables)]
fn driver_clear_inner<C, L>(
    this: &mut DefaultTimerDriver<C, L>,
) -> pi_result::Result<QueueCleanupProgress>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
        let core = std::rc::Rc::clone(&this.core);
    let mut queue = match core.queue.try_borrow_mut() {
        Ok(queue) => queue,
        Err(_) => return Err(pi_result::Error::new(pi_result::ErrorKind::Busy)),
    };
    let Some(queue) = queue.as_mut() else {
        return Err(pi_result::Error::new(pi_result::ErrorKind::InvalidState));
    };
    pi_async_task_queue::TimerTaskQueue::clear(queue)
}

#[allow(unused_variables)]
fn driver_close_inner<C, L>(
    this: &mut DefaultTimerDriver<C, L>,
) -> pi_result::Result<QueueCleanupProgress>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
        this.core
        .shared
        .owner_exiting
        .store(true, std::sync::atomic::Ordering::Release);
    let core = std::rc::Rc::clone(&this.core);
    core.finish_close_step()
}

#[allow(unused_variables)]
fn driver_access_inner<C, L>(this: &DefaultTimerDriver<C, L>) -> DefaultTimerAccess<C, L>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    DefaultTimerAccess { shared: Arc::clone(&this.core.shared) }
}

#[allow(unused_variables)]
fn driver_retries_inner<C, L>(
    this: &mut DefaultTimerDriver<C, L>,
) -> pi_result::Result<QueueCleanupProgress>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
        let mut retries = match this.core.retry.try_borrow_mut() {
        Ok(retries) => retries,
        Err(_) => return Err(pi_result::Error::new(pi_result::ErrorKind::Busy)),
    };
    let progress = retries.process();
    drop(retries);
    Ok(progress)
}

#[allow(unused_variables)]
fn driver_finish_close_inner<C, L>(
    this: &TimerLocalCore<C, L>,
) -> pi_result::Result<QueueCleanupProgress>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    use pi_async_task_queue::management::QueueCleanupProgress;

    let native = {
        let Ok(mut queue) = this.queue.try_borrow_mut() else {
            // 实际本地操作保留退出责任，仍有借用时由它接续。
            return Ok(QueueCleanupProgress::MoreWork);
        };
        match queue.as_mut() {
            Some(queue) => pi_async_task_queue::TimerTaskQueue::close(queue),
            None => Ok(QueueCleanupProgress::Complete),
        }
    };
    let retry = this.shared.retries.begin_close();
    let previous_error = this.close_error.borrow_mut().take();
    if let Some(error) = previous_error {
        return Err(error);
    }
    if matches!(native?, QueueCleanupProgress::Complete)
        && matches!(retry, QueueCleanupProgress::Complete)
    {
        Ok(QueueCleanupProgress::Complete)
    } else {
        Ok(QueueCleanupProgress::MoreWork)
    }
}

#[allow(unused_variables)]
fn driver_record_close_error_inner<C, L>(this: &TimerLocalCore<C, L>, error: pi_result::Error) -> ()
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
        let mut pending = Some(error);
    if let Ok(mut slot) = this.close_error.try_borrow_mut() {
        if slot.is_none() {
            *slot = pending.take();
        }
    }
    drop(pending);
}

#[allow(unused_variables)]
fn driver_core_drop_inner<C, L>(this: &mut TimerLocalCore<C, L>) -> ()
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
        this.shared
        .owner_exiting
        .store(true, std::sync::atomic::Ordering::Release);
    if let Some(queue) = this.queue.get_mut().take() {
        drop(queue);
    }
}

#[allow(unused_variables)]
fn driver_activity_drop_inner<C, L>(this: &mut TimerLocalActivityGuard<C, L>) -> ()
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
        if !this.armed {
        return;
    }
    this.armed = false;
    let core = std::rc::Rc::clone(&this.core);
    if !core.shared.owner_exiting.load(std::sync::atomic::Ordering::Acquire) {
        return;
    }
    match core.finish_close_step() {
        Ok(_) => {}
        Err(error) => core.record_close_error(error),
    }
}
