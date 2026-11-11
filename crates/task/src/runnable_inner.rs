// 当前文件落实配对声明中的局部操作。

#[allow(unused_variables)]
fn runnable_id_inner(this: &Runnable) -> TaskId {
    this.task.id()
}

#[allow(unused_variables)]
fn runnable_admit_inner(
    this: &Runnable,
    owner: &TaskThread,
) -> Result<TaskRegistration, AdmissionError> {
    this.task.admit(owner)
}

#[allow(unused_variables)]
fn runnable_run_inner(this: Runnable, owner: &TaskThread) -> Result<RunOutcome, RunError> {
    let runnable = std::mem::ManuallyDrop::new(this);
    // SAFETY: 原包装（Runnable）不再析构（drop）或访问，本份TaskRef仅移动一次。
    let task = unsafe { std::ptr::read(&runnable.task) };
    match task.run_once(owner) {
        Ok(outcome) => Ok(outcome),
        Err(cause) => Err(RunError { runnable: Runnable { task }, cause }),
    }
}

#[allow(unused_variables)]
fn runnable_snapshot_inner(this: &Runnable) -> TaskSnapshot {
    this.task.snapshot()
}

#[allow(unused_variables)]
fn runnable_metadata_inner<M: Send + Sync + 'static>(this: &Runnable) -> Option<&M> {
    this.task.metadata::<M>()
}

#[allow(unused_variables)]
fn runnable_drop_inner(this: &mut Runnable) {
    this.task.abandon_runnable();
}

#[allow(unused_variables)]
fn create_inner<F, S, M>(
    future: F,
    schedule: S,
    metadata: M,
    options: TaskOptions,
) -> Result<TaskParts<F::Output, M, SendOutput>, BuildError<F, S, M>>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
    M: Send + Sync + 'static,
{
    #[cfg(feature = "observe")]
    let observer = options.observer.clone();
    match crate::task::allocate_send(future, schedule, metadata, options) {
        Ok(refs) => {
            let parts = TaskParts {
                runnable: Runnable { task: refs.runnable },
                control: ControlHandle { task: refs.control },
                join: JoinHandle { task: refs.join, consumed: false, marker: std::marker::PhantomData },
            };
            #[cfg(feature = "observe")]
            if let Some(observer) = observer { observer.event(parts.runnable.task.id(), crate::task::TaskEvent::Created); }
            Ok(parts)
        }
        Err((future, schedule, metadata, options, cause)) => Err(BuildError { future, schedule, metadata, options, cause }),
    }
}

#[allow(unused_variables)]
fn create_local_inner<F, S, M>(
    owner: &TaskThread,
    future: F,
    schedule: S,
    metadata: M,
    options: TaskOptions,
) -> Result<TaskParts<F::Output, M, LocalOutput>, BuildError<F, S, M>>
where
    F: Future + 'static,
    F::Output: 'static,
    S: Schedule,
    M: Send + Sync + 'static,
{
    #[cfg(feature = "observe")]
    let observer = options.observer.clone();
    match crate::task::allocate_local(owner, future, schedule, metadata, options) {
        Ok(refs) => {
            let parts = TaskParts {
                runnable: Runnable { task: refs.runnable },
                control: ControlHandle { task: refs.control },
                join: JoinHandle { task: refs.join, consumed: false, marker: std::marker::PhantomData },
            };
            #[cfg(feature = "observe")]
            if let Some(observer) = observer { observer.event(parts.runnable.task.id(), crate::task::TaskEvent::Created); }
            Ok(parts)
        }
        Err((future, schedule, metadata, options, cause)) => Err(BuildError { future, schedule, metadata, options, cause }),
    }
}
