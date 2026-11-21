#[allow(unused_variables)]
fn runtime_new_inner<M, E, I, F, T>(
    options: RuntimeOptions,
    queues: RuntimeQueues<ManualClock, E, I, F>,
) -> pi_result::Result<ConfiguredRuntime<M, ManualClock, DefaultIdlePolicy, NoHooks, E, I, T>>
where
    M: RuntimeMode,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
    F: FnOnce(RuntimeId, &ManualClock, ThreadNotifier, CancelTimerOptions) -> pi_result::Result<T>,
{
        // 只核外部已安装的全局时间轴；不初始化、不重置，缺装时原报告直返。
    let clock = pi_async_executor::ManualClock::new()?;
    // 以默认策略与钩子连同原 options/queues 交同一模式的原装配入口。
    runtime_components_inner::<M, pi_async_executor::ManualClock, pi_async_executor::DefaultIdlePolicy, pi_async_executor::NoHooks, E, I, F, T>(
        clock,
        pi_async_executor::DefaultIdlePolicy,
        pi_async_executor::NoHooks,
        options,
        queues,
    )
}
#[allow(unused_variables)]
fn runtime_components_inner<M, C, P, H, E, I, F, T>(
    clock: C,
    policy: P,
    hooks: H,
    options: RuntimeOptions,
    queues: RuntimeQueues<C, E, I, F>,
) -> pi_result::Result<ConfiguredRuntime<M, C, P, H, E, I, T>>
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
    P: IdlePolicy,
    H: ExecutorHooks,
    F: FnOnce(RuntimeId, &C, ThreadNotifier, CancelTimerOptions) -> pi_result::Result<T>,
{
        use crate::{
        binding::RuntimeTag,
        lifecycle::{RuntimeBinding, RuntimeExternalSource},
        queues::QueueSelection,
    };

    // 1. 解析原绑定槽和各组选项。执行器在构造自身及定时来源之前校验其选项。
    let binding = RuntimeBinding::new();
    let executor_options = options.executor;
    let timer_options = options.timer;
    // 2. 分别兑现外部与空闲管理者：指定项移动实例，缺项才用对应选项建默认来源。
    let external = match queues.external {
        QueueSelection::Specified(value) => value,
        QueueSelection::Default(make) => make(options.external_queue)?,
    };
    let idle = match queues.idle {
        QueueSelection::Specified(value) => value,
        QueueSelection::Default(make) => make(options.idle_queue)?,
    };
    // 3. 普通管理者用来源包装共享同一绑定槽；公开访问仍是 E::Handle。
    let source = RuntimeExternalSource::new(external, Rc::clone(&binding));
    // 4. 计时步骤只执行一次，用最终 RuntimeId、同源 clock 与原线程通知句柄。
    let mut make_timer = Some(queues.timer);
    let executor = DefaultExecutor::<
        C,
        RuntimeExternalSource<E>,
        I,
        T,
        RuntimeTag<C, T>,
    >::new(
        clock,
        source,
        idle,
        executor_options,
        move |runtime, clock, notifier| {
            let make = make_timer.take().expect("定时构造回调只执行一次");
            make(runtime, clock, notifier, timer_options)
        },
    )?;
    // 5. 成功：执行器句柄装单一 Arc，数据存原执行器与默认模式／策略／钩子。
    let handle = executor.handle();
    let shared = RuntimeHandle { inner: Arc::new(handle) };
    let data = RuntimeData {
        executor,
        mode: M::default(),
        policy,
        hooks,
    };
    Ok(ConfiguredRuntime {
        shared,
        data: RefCell::new(Some(data)),
        binding,
    })
}
#[allow(unused_variables)]
fn runtime_handle_clone_inner<C, E, I, T>(this: &RuntimeHandle<C, E, I, T>) -> RuntimeHandle<C, E, I, T>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalTaskQueue,
    I: IdleTaskQueue,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
{
        // 只克隆同一共享存储一次；不逐项克隆工厂/来源/登记，不产生新身份或线程。
    RuntimeHandle { inner: std::sync::Arc::clone(&this.inner) }
}
#[allow(unused_variables)]
fn runtime_drop_inner<M, C, P, H, E, I, T>(this: &mut ConfiguredRuntime<M, C, P, H, E, I, T>)
where
    M: RuntimeMode,
    C: ExecutorClock<Instant = GlobalManualInstant>,
    E: ExternalQueueManager,
    I: IdleQueueManager,
    T: ExecutorTimer<Instant = GlobalManualInstant> + 'static,
    P: IdlePolicy,
    H: ExecutorHooks,
{
        // 先撤原本地权限，再取走原数据与执行器；顺序固定，展开时未取走的数据仍由字段自动清理。
    this.binding.revoke();
    let data = this.data.get_mut().take();
    drop(data);
}
