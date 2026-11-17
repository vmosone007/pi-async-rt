#[allow(unused_variables)]
fn runtime_new_inner<M: RuntimeMode>(
    options: RuntimeOptions,
) -> pi_result::Result<ConfiguredRuntime<M, ManualClock, DefaultIdlePolicy, NoHooks>> {
    todo!("编号：T263，尚未实现， P1；ManualClock::new只核外部已安装，不初始化/重置全局时钟。以DefaultIdlePolicy/NoHooks和原options调用同M的ConfiguredRuntime::from_components，不把Serial误构造成默认Concurrent类型。不给创建服务默认派发任务/启动线程/注册TLS。时钟原Report直返。")
}
#[allow(unused_variables)]
fn runtime_components_inner<M, C, P, H>(
    clock: C,
    policy: P,
    hooks: H,
    options: RuntimeOptions,
) -> pi_result::Result<ConfiguredRuntime<M, C, P, H>>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    todo!("编号：T264，尚未实现， P0；先RuntimeBinding::new准备一份原线程Rc槽，再validate executor options。分别new两份独立MpscTaskQueue/MpscIdleTaskQueue，普通管理者用RuntimeExternalSource::new包原queue和同binding Rc；共享Handle仍原MpscTaskQueueHandle，wrapper不复制数量。交DefaultExecutor::<C,RuntimeExternalSource,...>::new，make_timer用最终id/clock.clone/原notifier及timer options构造DefaultTimerDriver，统一pi_cancel_timer 0.7和同源时间。成功把executor.handle装单一Arc Shared，Data存原executor/M::default/P/H，拥有者保存同Rc binding（无Token），不enter/创建线程。原Report/失败Drop配对保留，来源wrapper保证首个退出操作先撤本地权限。额外Rc仅每runtime一次，绝非每Task/poll分配或动态分发。")
}
#[allow(unused_variables)]
fn runtime_handle_clone_inner<C: ExecutorClock<Instant = GlobalManualInstant>>(
    this: &RuntimeHandle<C>,
) -> RuntimeHandle<C> {
    todo!("编号：T265，尚未实现， P0；Arc::clone同一inner一次，不逐项clone原工厂/来源/registry、不新RuntimeId/线程/本地Rc。普通Handle Drop只归共享存储，不请求close，不推迟已绑定Future合法清理。")
}
#[allow(unused_variables)]
fn runtime_drop_inner<M, C, P, H>(this: &mut ConfiguredRuntime<M, C, P, H>)
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    P: IdlePolicy,
    H: ExecutorHooks,
{
    todo!("编号：T266，尚未实现， \"P0；先this.binding.revoke撤原本地Token，再data.get_mut(").take后Drop原Data/Executor。包装来源退出也幂等revoke，保证即使退出从执行器内部发起也先撤权限。若先撤Token时附属Drop异常，尚未take的Data仍在字段，由Rust自动清理和下层原ExitGuard实际退出；不forget/重复Drop。不等共享Handle/Control/Waker全部释放，不加catch或fallible close吞错；Closed仍不等于Completed。")
}
