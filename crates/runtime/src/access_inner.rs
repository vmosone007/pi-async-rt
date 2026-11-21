#[allow(unused_variables)]
fn runtime_with_local_inner<C, E, I, T, R>(
    action: impl FnOnce(&RuntimeHandle<C, E, I, T>, &ExecutorLocal<T>) -> R,
) -> pi_result::RawResult<R, LocalRuntimeAccessError>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalTaskQueue,
    I: IdleTaskQueue,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
{
    // 直接复用核心同步本地访问：它先结束TLS借用，再核原RuntimeId、所属域、有效标记、
    // Weak与具体类型，最后调用action；不默认创建服务、不跨服务回退、不clone Handle。
    pi_async_runtime_core::context::try_with_current_local::<
        RuntimeHandle<C, E, I, T>,
        ExecutorLocal<T>,
        R,
    >(action)
}

fn runtime_bind_inner<M, C, P, H, E, I, T>(runtime: &ConfiguredRuntime<M, C, P, H, E, I, T>) -> pi_result::Result<()>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
    P: IdlePolicy,
    H: ExecutorHooks,
{
        use pi_async_executor::ExecutorLocal;

    // 停收中或已关闭时不再新授本地权限，保留后续公共选择以合法推进关闭收尾。
    if runtime.shared.status() != pi_async_runtime_core::definitions::RuntimeStatus::Open {
        return Ok(());
    }
    let runtime_id = runtime.shared.id();
    // 已有 Token 且当前本地访问确实有效时快速复用；只凭 is_present 不足以证明。
    if runtime.binding.is_present()
        && pi_async_runtime_core::context::record::try_with_local_for::<ExecutorLocal<T>, _>(
            runtime_id,
            |_| (),
        )
        .is_ok()
    {
        return Ok(());
    }
    // 短借数据取原本地设施 Rc，借用结束再进入所属域建立绑定。
    let local: Option<std::rc::Rc<ExecutorLocal<T>>> = match runtime.data.try_borrow() {
        Ok(data) => data.as_ref().map(|data| std::rc::Rc::clone(data.executor.local())),
        Err(_) => None,
    };
    let Some(local) = local else {
        // 已退出：不新建本地权限；后续公共选择仍可服务合法关闭推进。
        return Ok(());
    };
    let handle = runtime.shared.clone();
    let binding = std::rc::Rc::clone(&runtime.binding);
    let bound = local.with_owner(|owner| {
        pi_async_runtime_core::context::bind_current(handle.clone(), owner, &local)
    });
    match bound {
        Ok(Ok(new_binding)) => {
            // 新 Token 成功后才替换；旧 Token 在借用结束后析构。
            binding.replace(new_binding);
            Ok(())
        }
        // 绑定失败：旧 Token 保持，原 typed cause 作为 InvalidState 来源。
        Ok(Err(error)) => Err(pi_result::Error::new(pi_result::ErrorKind::InvalidState).attach(error)),
        Err(error) => Err(pi_result::Error::new(pi_result::ErrorKind::InvalidState).attach(error)),
    }
}
#[allow(unused_variables)]
fn binding_exit_inner<C, E, I, T>(
    guard: &mut BindingExitGuard<'_, C, E, I, T>,
)
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalTaskQueue,
    I: IdleTaskQueue,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
{
    // 只在服务真正关闭时幂等撤销本地权限；这是来源包装先撤权限之后的末尾校验，
    // 不是退出前唯一的撤销点。开放单轮返回与尚未开始的退出都不在此撤销。
    if guard.handle.status() == pi_async_runtime_core::definitions::RuntimeStatus::Closed {
        guard.binding.revoke();
    }
}
