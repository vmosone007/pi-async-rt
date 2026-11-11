// 当前文件落实配对声明中的局部操作。

#[allow(unused_variables)]
fn pool_new_inner(random: fn() -> u64) -> TaskPoolQueue {
    TaskPoolQueue {
        pool: TaskPool::new(NoFastPath),
        access: Mutex::new(()),
        queued: AtomicUsize::new(0),
        closed: AtomicBool::new(false),
        random,
    }
}

#[allow(unused_variables)]
fn pool_close_inner(this: &TaskPoolQueue) -> BatchAttempt {
    this.closed.store(true, std::sync::atomic::Ordering::SeqCst);
    pool_take_inner(this)
}

#[allow(unused_variables)]
fn pool_push_inner(this: &TaskPoolQueue, task: Runnable) -> Result<(), ExternalPushError> {
    use std::sync::atomic::Ordering;
    if this.closed.load(Ordering::SeqCst) {
        return Err(ExternalPushError { task, cause: ExternalPushErrorKind::Closed });
    }
    let Some(access) = this.access.try_lock() else {
        return Err(ExternalPushError { task, cause: ExternalPushErrorKind::Busy });
    };
    if this.closed.load(Ordering::SeqCst) {
        return Err(ExternalPushError { task, cause: ExternalPushErrorKind::Closed });
    }
    let Some(count) = this.queued.load(Ordering::SeqCst).checked_add(1) else {
        return Err(ExternalPushError { task, cause: ExternalPushErrorKind::CountOverflow });
    };
    this.pool.push_parallel(Some(task), TaskWeight::Middle);
    // 原生发布（publication）正常完成后，在接入独占权（access）内提交精确数量（count）。
    this.queued.store(count, Ordering::SeqCst);
    drop(access);
    Ok(())
}

#[allow(unused_variables)]
fn pool_take_inner(this: &TaskPoolQueue) -> BatchAttempt {
    use std::sync::atomic::Ordering;
    // 批次容器（batch container）先于接入守卫（access guard）建立，随机源（random source）展开时先解锁再释放已取持有。
    let mut tasks = std::collections::VecDeque::new();
    let Some(access) = this.access.try_lock() else { return BatchAttempt::Busy; };
    let count = this.queued.load(Ordering::SeqCst);
    tasks.reserve(count);
    for taken in 0..count {
        let Some(mut handle) = this.pool.pop((this.random)()) else { break; };
        let task = handle.take().unwrap_or_else(|| std::process::abort());
        drop(handle);
        this.queued.store(count - taken - 1, Ordering::SeqCst);
        tasks.push_back(task);
    }
    let remaining = this.queued.load(Ordering::SeqCst);
    let closed = this.closed.load(Ordering::SeqCst);
    drop(access);
    if !tasks.is_empty() {
        BatchAttempt::Ready(TaskBatch::from_tasks(tasks))
    } else if remaining != 0 {
        BatchAttempt::WouldBlock
    } else if closed {
        BatchAttempt::Closed
    } else {
        BatchAttempt::Empty
    }
}

#[allow(unused_variables)]
fn pool_len_inner(this: &TaskPoolQueue) -> usize {
    this.queued.load(std::sync::atomic::Ordering::SeqCst)
}

#[allow(unused_variables)]
fn pool_drop_inner(this: &mut TaskPoolQueue) {
    use std::sync::atomic::Ordering;
    this.closed.store(true, Ordering::SeqCst);
    let count = *this.queued.get_mut();
    let mut tasks = std::collections::VecDeque::with_capacity(count);
    for _ in 0..count {
        // 所有项只在Middle档，独占析构（drop）无发布者；固定输入不增加随机源（random source）展开风险。
        let mut handle = this.pool.pop(0).unwrap_or_else(|| std::process::abort());
        let task = handle.take().unwrap_or_else(|| std::process::abort());
        drop(handle);
        *this.queued.get_mut() -= 1;
        tasks.push_back(task);
    }
    // 先退完原生句柄（TaskHandle），再释放可重入的执行持有（Runnable）；原值清理由其合法出口承担。
    drop(tasks);
}
