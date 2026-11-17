#[allow(unused_variables)]
fn tag_ready_inner<C: ExecutorClock<Instant = GlobalManualInstant>>(
    runtime: RuntimeId,
    task: Runnable,
) -> pi_result::RawResult<(), Runnable> {
    todo!("编号：T251，尚未实现， \"P0；通过核心try_with_local_for::<ExecutorLocal<DefaultTimerDriver<C,RuntimeTag<C>>>>核原RuntimeId/真实owner/valid后同步local.try_push(task,false")。先Option保原Rune，闭包真正进入才take；无环境/错目标/借用失效返同一原Rune供ReturnRoute原共享回送，不能直接访问另一线程双端队列。成功/失败均结束TLS内部借用后执行action，拒绝QueuePushError只取原task，原State已回退预留。不保本地Rc/裸指针，不clone任务或分配，不因公共Handle存在授本地权限。")
}
#[allow(unused_variables)]
fn tag_timer_inner<C: ExecutorClock<Instant = GlobalManualInstant>>(
    runtime: RuntimeId,
    deadline: GlobalManualInstant,
    item: DefaultTimerItem<C, RuntimeTag<C>>,
) -> pi_result::RawResult<CancelTimerKey, TimerSubmitError<DefaultTimerItem<C, RuntimeTag<C>>>> {
    todo!("编号：T252，尚未实现， \"P0；同一try_with_local_for核原权限，短暂调用local.try_push_timer(deadline,item")。Option保原deadline/item，闭包未执行时原值返TimerSubmitError::WrongOwner/Closed按真实失效分类，不将NoLocal当允许共享非Send投递。F/Item无新增Send/Sync，源真实Busy原样返；不刷新时钟/换RuntimeId/创建代理Task，不能持TLS RefCell调用源。")
}
