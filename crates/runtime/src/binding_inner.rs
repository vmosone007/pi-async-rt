#[allow(unused_variables)]
fn tag_ready_inner<T: ExecutorTimer<Instant = GlobalManualInstant> + 'static>(
    runtime: RuntimeId,
    task: Runnable,
) -> pi_result::RawResult<(), Runnable> {
    // 先持住唯一原任务（Runnable）；闭包真正进入才取出，未进入时原样返还给共享回送。
    let mut task = Some(task);
    let result = pi_async_runtime_core::context::record::try_with_local_for::<ExecutorLocal<T>, _>(
        runtime,
        |local| {
            let held = task.take().expect("原任务（Runnable）只在此处取出一次");
            local.try_push(held, false)
        },
    );
    match result {
        Ok(Ok(())) => Ok(()),
        // 拒绝只取回原任务；原状态已回退本次预留。
        Ok(Err(error)) => Err(error.task),
        Err(_) => Err(task.take().expect("未进入本地环境时原任务仍在本处")),
    }
}

fn tag_timer_inner<C: ExecutorClock<Instant = GlobalManualInstant>>(
    runtime: RuntimeId,
    deadline: GlobalManualInstant,
    item: DefaultTimerItem<C, RuntimeTag<C>>,
) -> pi_result::RawResult<CancelTimerKey, TimerSubmitError<DefaultTimerItem<C, RuntimeTag<C>>>> {
    // 同一原权限核验后短暂调用本地定时提交；不持 TLS 借用执行用户代码。
    let mut pending = Some((deadline, item));
    let result = pi_async_runtime_core::context::record::try_with_local_for::<ExecutorLocal<pi_async_executor::timer::DefaultTimerDriver<C, RuntimeTag<C>>>, _>(
        runtime,
        |local| {
            let (deadline, item) = pending.take().expect("原截止与原值只在此处取出一次");
            local.try_push_timer(deadline, item)
        },
    );
    match result {
        Ok(outcome) => outcome,
        Err(_) => {
            let (_deadline, item) = pending.take().expect("未进入本地环境时原值仍在本处");
            // 未取得本地权限时按真实失效分类返还同一原值，不把无本地环境当作允许共享投递。
            Err(TimerSubmitError { item, cause: pi_async_task_queue::timer::TimerQueueError::WrongOwner })
        }
    }
}
