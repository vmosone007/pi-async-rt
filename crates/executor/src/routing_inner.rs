#[allow(unused_variables)]
fn route_new_inner<L: 'static>(queue: ReturnQueueHandle) -> ReturnRoute<L> {
    // 一份未初始化身份槽（phase=0）与同一返回入口；不持本地实例、裸指针或任务。
    ReturnRoute {
        queue,
        identity: Arc::new(RouteIdentity {
            value: UnsafeCell::new(MaybeUninit::uninit()),
            phase: AtomicU8::new(0),
        }),
        local: PhantomData,
    }
}
fn route_clone_inner<L: 'static>(this: &ReturnRoute<L>) -> ReturnRoute<L> {
    // 只克隆原返回入口与同一身份 Arc；不要求 L 可克隆，也不制造新身份。
    ReturnRoute { queue: this.queue.clone(), identity: Arc::clone(&this.identity), local: PhantomData }
}
#[allow(unused_variables)]
fn route_set_id_inner<L: 'static>(this: &ReturnRoute<L>, runtime: RuntimeId) {
    use std::sync::atomic::Ordering::{AcqRel, Acquire, Release};
    // 只在完整构造、用户设施未暴露前调用：强CAS取得唯一写资格，失败明确恐慌而不重复写。
    assert_eq!(
        this.identity.phase.compare_exchange(0, 1, AcqRel, Acquire),
        Ok(0),
        "路由身份（route identity）只能发布一次",
    );
    // 安全（safety）：唯一写者持有未初始化槽；原始投影写入后以Release发布就绪。
    unsafe { (*this.identity.value.get()).write(runtime); }
    this.identity.phase.store(2, Release);
}
fn recovery_new_inner(
    queue: ReturnQueueHandle,
    state: Arc<ExecutorState>,
    notifier: ThreadNotifier,
) -> ReturnRecovery {
    // 保存原返回入口及相同状态与通知器；不新建队列、线程或每任务分配。
    ReturnRecovery { queue, state, notifier }
}
fn route_push_inner<L: LocalReadySubmit>(
    this: &ReturnRoute<L>,
    task: Runnable,
) -> pi_result::RawResult<(), ExternalPushError> {
        use std::sync::atomic::Ordering::Acquire;

    let missing = |task: Runnable| pi_async_task_queue::external::ExternalPushError {
        task,
        cause: pi_async_task_queue::external::ExternalPushErrorKind::LinkMissing,
    };
    // 身份未发布：只走共享返回，不等待其它写者。
    if this.identity.phase.load(Acquire) != 2 {
        return this.queue.publish(task).map_err(missing);
    }
    // 安全（safety）：phase=2 已由 Release 发布，槽内值此后不再变化。
    let runtime = unsafe { (*this.identity.value.get()).assume_init() };
    match L::try_push_local(runtime, task) {
        Ok(()) => {
            // 本地尾插成功只通知本线程；不接纳、不轮询、不动原值。
            this.queue.notify();
            Ok(())
        }
        // 本地拒绝：同一原值走共享返回；缺链接如实报错，不伪成功。
        Err(task) => this.queue.publish(task).map_err(missing),
    }
}
#[allow(unused_variables)]
fn recovery_retain_inner(this: &ReturnRecovery, task: Runnable, reason: DispatchFailure) {
        // 先按既有归属责任发布；成功即结束，正常热路径不进入下面分支。
    let task = match this.queue.publish(task) {
        Ok(()) => return,
        Err(task) => task,
    };
    // 确实无法投递：请求整体关闭并通知，让原来源进入停收与清理。
    let _ = this.state.request_close(&this.notifier);
    pi_async_task_queue::notify::QueueNotifier::notify(&this.notifier);
    // 在队列／TLS／消费者保护之外放弃同一原值，由原任务机制交回其真正所属域；
    // 不重试、不吞原原因、不新建等待。
    drop(task);
}
