// 当前文件落实配对声明中的局部操作。

#[allow(unused_variables)]
fn join_id_inner<T, M: Send + Sync + 'static, K: OutputMode>(this: &JoinHandle<T, M, K>) -> TaskId {
    this.task.id()
}

#[allow(unused_variables)]
fn join_snapshot_inner<T, M: Send + Sync + 'static, K: OutputMode>(
    this: &JoinHandle<T, M, K>,
) -> TaskSnapshot {
    this.task.snapshot()
}

#[allow(unused_variables)]
fn join_metadata_inner<T, M: Send + Sync + 'static, K: OutputMode>(
    this: &JoinHandle<T, M, K>,
) -> &M {
    this.task.metadata::<M>().unwrap_or_else(|| std::process::abort())
}

#[allow(unused_variables)]
fn join_poll_inner<T, M: Send + Sync + 'static, K: OutputMode>(
    this: Pin<&mut JoinHandle<T, M, K>>,
    cx: &mut Context<'_>,
) -> Poll<Result<T, TaskError>> {
    let this = this.get_mut();
    if this.consumed { return Poll::Ready(Err(TaskError::AlreadyTaken)); }
    let mut result = std::mem::MaybeUninit::<Result<T, TaskError>>::uninit();
    // SAFETY: 唯一结果句柄（JoinHandle）固定原输出类型（output type）；本地模式不能跨线程（thread）。
    if unsafe { this.task.take_result::<T>(std::ptr::NonNull::from(&mut result).cast()) } {
        let result = unsafe { result.assume_init() };
        this.consumed = true;
        this.task.abandon_result();
        return Poll::Ready(result);
    }
    if this.task.snapshot().execution == ExecutionStatus::CleanupUnconfirmed {
        this.consumed = true;
        this.task.abandon_result();
        return Poll::Ready(Err(TaskError::CleanupUnconfirmed));
    }
    this.task.register_result(cx.waker());
    // SAFETY: 复核（recheck）使用相同唯一领取权，避免登记（registration）与发布（publication）交错丢通知。
    if unsafe { this.task.take_result::<T>(std::ptr::NonNull::from(&mut result).cast()) } {
        let result = unsafe { result.assume_init() };
        this.consumed = true;
        this.task.abandon_result();
        Poll::Ready(result)
    } else if this.task.snapshot().execution == ExecutionStatus::CleanupUnconfirmed {
        this.consumed = true;
        this.task.abandon_result();
        Poll::Ready(Err(TaskError::CleanupUnconfirmed))
    } else {
        Poll::Pending
    }
}

#[allow(unused_variables)]
fn join_drop_inner<T, M: Send + Sync + 'static, K: OutputMode>(this: &mut JoinHandle<T, M, K>) {
    if !this.consumed {
        this.task.abandon_result();
        let mut result = std::mem::MaybeUninit::<Result<T, TaskError>>::uninit();
        // SAFETY: 创建入口固定T且本值独占领取权，本地模式（LocalOutput）不能转移到其它线程。
        if unsafe { this.task.take_result::<T>(std::ptr::NonNull::from(&mut result).cast()) } {
            this.consumed = true;
            // SAFETY: 成功领取已经初始化结果（output），只在本句柄（handle）所在合法线程析构（drop）。
            drop(unsafe { result.assume_init() });
        }
    }
}
