fn runtime_assert_drive_available() {
    use pi_async_runtime_core::context::record::DriveScope;

    assert!(
        !DriveScope::is_active() && pi_async_task::try_current_task_id().is_none(),
        "活动推进范围或当前任务中不能再次推进运行时"
    );
}

fn runtime_once_inner<M, C, P, H, E, I, T>(
    this: &mut ConfiguredRuntime<M, C, P, H, E, I, T>,
) -> pi_result::Result<TurnResult>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    runtime_assert_drive_available();
    access::ensure_binding(this)?;
    let _binding_exit = BindingExitGuard { binding: &this.binding, handle: &this.shared };
    // 以同目标公共选择包住整次原 run_once；不同目标在任何回调前失败。
    let handle = this.shared.clone();
    let selected = pi_async_runtime_core::context::record::with_selected(handle, || {
        let mut data = this.data.borrow_mut();
        match data.as_mut() {
            Some(data) => {
                let RuntimeData { executor, mode, policy, hooks } = data;
                executor.run_once(mode, policy, hooks)
            }
            None => Err(pi_result::Error::new(pi_result::ErrorKind::InvalidState)
                .attach("运行时数据（runtime data）已经退出")),
        }
    });
    match selected {
        Ok(result) => result,
        // 选择失败把原 typed cause 作为来源附到 InvalidState，不伪造成功。
        Err(error) => Err(pi_result::Error::new(pi_result::ErrorKind::InvalidState).attach(error)),
    }
}
fn runtime_run_inner<M, C, P, H, E, I, T>(this: &mut ConfiguredRuntime<M, C, P, H, E, I, T>) -> pi_result::Result<()>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    runtime_assert_drive_available();
    access::ensure_binding(this)?;
    let _binding_exit = BindingExitGuard { binding: &this.binding, handle: &this.shared };
    let handle = this.shared.clone();
    let selected = pi_async_runtime_core::context::record::with_selected(handle, || {
        let mut data = this.data.borrow_mut();
        match data.as_mut() {
            Some(data) => {
                // 原执行器一次运行负责整段轮转与主动／被动停放；外层不重复 run_once。
                let RuntimeData { executor, mode, policy, hooks } = data;
                executor.run(mode, policy, hooks)
            }
            None => Err(pi_result::Error::new(pi_result::ErrorKind::InvalidState)
                .attach("运行时数据（runtime data）已经退出")),
        }
    });
    match selected {
        Ok(result) => result,
        Err(error) => Err(pi_result::Error::new(pi_result::ErrorKind::InvalidState).attach(error)),
    }
}
fn handle_block_on_inner<C, E, I, T, F: Future>(
    this: &RuntimeHandle<C, E, I, T>,
    future: F,
) -> F::Output
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalTaskQueue,
    I: IdleTaskQueue,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
{
    runtime_assert_drive_available();
    // 保留原公共环境与错误载荷（error payload），只在该环境推进根异步计算（Future）。
    match pi_async_runtime_core::context::record::with_selected(this.clone(), || {
        pi_async_runtime_core::block_on::block_on_root(future)
    }) {
        Ok(output) => output,
        Err(error) => std::panic::panic_any(error),
    }
}
fn runtime_block_on_inner<M, C, P, H, E, I, T, F>(
    this: &ConfiguredRuntime<M, C, P, H, E, I, T>,
    future: F,
) -> F::Output
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
    P: IdlePolicy,
    H: ExecutorHooks,
    F: Future,
{
    runtime_assert_drive_available();
    // 已关闭时仍选择同一公开句柄（shared handle），只推进独立根。
    if this.shared.status() == RuntimeStatus::Closed {
        return this.shared.block_on(future);
    }
    // 非关闭：先确保自动父 Frame；失败以原报告恐慌，不在错误环境推进根。
    if let Err(error) = access::ensure_binding(this) {
        std::panic::panic_any(error);
    }
    let _binding_exit = BindingExitGuard { binding: &this.binding, handle: &this.shared };
    let handle = this.shared.clone();
    let binding = &*this.binding;
    let owner_handle = &this.shared;
    let selected = pi_async_runtime_core::context::record::with_selected(handle, || {
        let mut data = this.data.borrow_mut();
        let Some(data) = data.as_mut() else {
            std::panic::panic_any(pi_result::Error::new(pi_result::ErrorKind::InvalidState)
                .attach("运行时数据（runtime data）已经退出"));
        };
        // 一次租用覆盖整次根推进；本函数不另建 Scope。
        let lease = data.executor.begin_drive();
        let mut driver = RuntimeDriver {
            data,
            lease,
            last: None,
            binding,
            handle: owner_handle,
        };
        pi_async_runtime_core::block_on::block_on_with_driver(future, &mut driver)
    });
    match selected {
        Ok(output) => output,
        // 选择失败把原 typed cause 作为 panic 载荷，绝不在其它服务环境 poll 根。
        Err(error) => std::panic::panic_any(error),
    }
}
#[allow(unused_variables)]
fn runtime_driver_turn_inner<M, C, P, H, E, I, T>(driver: &mut RuntimeDriver<'_, M, C, P, H, E, I, T>) -> bool
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
    P: IdlePolicy,
    H: ExecutorHooks,
{
        // 已关闭：只幂等撤本地权限并记录闭服务轮次，不重开来源。
    if driver.handle.status() == RuntimeStatus::Closed {
        driver.binding.revoke();
        // 关闭轮次没有可构造的推进样本：清空上一样本，避免把旧结果当本轮进展。
        driver.last = None;
        return false;
    }
    // 正常：同一租用、原模式作为接纳规则、原策略与钩子推进一次有限轮次。
    let result = driver.data.executor.turn_with_lease(
        &driver.lease,
        &driver.data.mode,
        &mut driver.data.policy,
        &mut driver.data.hooks,
    );
    match result {
        Ok(turn) => {
            let report = turn.immediate_work;
            driver.last = Some(turn);
            report
        }
        // 服务错误以原报告为恐慌载荷，不转字符串、不伪 F 输出。
        Err(error) => std::panic::panic_any(error),
    }
}
#[allow(unused_variables)]
fn runtime_driver_park_inner<M, C, P, H, E, I, T>(
    driver: &mut RuntimeDriver<'_, M, C, P, H, E, I, T>,
    root: &RootWakeState,
) where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
    P: IdlePolicy,
    H: ExecutorHooks,
{
        // 已关闭：只等调用线程自身的根许可，不把关闭当忙转。
    if driver.handle.status() == RuntimeStatus::Closed {
        root.park();
        return;
    }
    // 开放／停收中：只以上一轮真实样本取得等待依据。
    let Some(last) = driver.last.take() else {
        return;
    };
    match driver.data.executor.park_with_lease(
        &driver.lease,
        &driver.data.mode,
        &last,
        Some(root),
    ) {
        Ok(()) => {
            driver.last = Some(last);
        }
        // 服务错误以原报告为恐慌载荷，保留链；停放未被当作成功。
        Err(error) => std::panic::panic_any(error),
    }
}
