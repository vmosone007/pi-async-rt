#[allow(unused_variables)]
fn local_create_inner<C, F, V>(
    future: F,
    config: TaskConfig<V>,
) -> pi_result::RawResult<(UnsubmittedTask, LocalTaskHandle<F::Output, V>), CreateError<F, V>>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    F: Future + 'static,
    F::Output: 'static,
    V: Send + Sync + 'static,
{
    todo!("编号：T253，尚未实现， \"P1；Option持原(F,config")，access::with_local::<C,_>真正进入后才take并调用handle.inner.create_local(local,...)。进入前失败把Option原值返CreateError LocalContext原分类，成功原结果直返；不F装箱/不SendSync/Unpin附加要求。核心结束TLS借用再action，原native创建前不持队列或registry可写借用；结果可Clone控制但本地输出不变Send。")
}
#[allow(unused_variables)]
fn local_submit_inner<C: ExecutorClock<Instant = GlobalManualInstant>>(
    task: UnsubmittedTask,
    front: bool,
) -> pi_result::RawResult<(), SubmitError> {
    todo!("编号：T254，尚未实现， P0；Option保原待提交值，with_local真正进入才take；先核当前原RuntimeId/owner/valid，再调用原inner.submit_local或submit_local_front。进入失败返同一UnsubmittedTask及LocalContext原错；不同创建服务仍由Factory拒绝WrongRuntime。不直接取Rune/预admit/取消/跨线程投内部队列；头插仅影响下一选择不抢断。常规spawn_local走尾，Front独立显式入口。")
}
#[allow(unused_variables)]
fn local_idle_inner<C, F, V>(
    future: F,
    config: TaskConfig<V>,
) -> pi_result::RawResult<TaskHandle<F::Output, V>, SpawnError<F, ThreadNotifier, V>>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    V: Send + Sync + 'static,
{
    todo!("编号：T255，尚未实现， P1；从当前有效本地环境借原公共Handle，只为选择目标，不把任务提前绑当前线程。Option保F/config防进入失败，错误SpawnError::Create LocalContext保原输入；成功用同一普通TaskDispatch.create_with_config再IdleDispatch.submit_idle语义组合，创建后的拒绝保原待提交值和结果handle。仍入独立共享空闲源，Send不Sync，接纳/是否空闲/未来执行无承诺；不要调用spawn_local产生非Send绑定任务再塞共享空闲源。")
}
#[allow(unused_variables)]
fn local_timing_inner<C, F, V>(
    future: F,
    delay: Duration,
    config: TaskConfig<V>,
) -> pi_result::RawResult<
    pi_async_runtime_core::task::TaskHandle<TimedTaskNotifier<C>, F::Output, V, LocalOutput>,
    SpawnError<F, TimedTaskNotifier<C>, V, LocalOutput>,
>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    F: Future + 'static,
    F::Output: 'static,
    V: Send + Sync + 'static,
{
    todo!("编号：T256，尚未实现， \"P1；Option保原F/config，with_local合法进入后调用原handle.inner.spawn_timing_local(local,F,delay,config")；未进入返Create LocalContext保原值。同步非等待提交，不在此await，来源前查询登记/迟到键/取消/出错均由既有支持完成。不额外取时、重建Task或把本地F标Send；接受后错误不假拒绝，0延迟仍真实入定时源，与timeout(0)只yield不同。")
}
