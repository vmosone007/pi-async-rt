#[allow(unused_variables)]
fn default_retry_owner_new_inner<C, L>(
    notifier: ThreadNotifier,
    batch: std::num::NonZeroUsize,
) -> RetryOwner<CancelRetryNode<C, L>>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P0；一次Arc准备独立stub节点，facts=TimerRegistration::new(Task)、Links::new、origin/retry均Weak::new、membership=IDLE、error=None、stub=true、PhantomPinned。构造RetryHandle时on_drop=None，移交RetryOwner::new；原生new_with_stub自己设置stub标记，不使用Links::new_stub，不对stub调用核心resume，不计业务或重试单位。标准Arc分配遵守分配器失败策略，不创建Task、线程或新时间轴；失败/展开对已构造部分配对释放。")
}

#[allow(unused_variables)]
fn default_access_clone_inner<C: ExecutorClock<Instant = GlobalManualInstant>, L: 'static>(
    this: &DefaultTimerAccess<C, L>,
) -> DefaultTimerAccess<C, L> {
    todo!("P0；只Arc::clone原shared；无新源、身份、时钟起点、节点或本地权限。")
}

#[allow(unused_variables)]
fn default_registration_clone_inner<C: ExecutorClock<Instant = GlobalManualInstant>, L: 'static>(
    this: &DefaultTimerRegistration<C, L>,
) -> DefaultTimerRegistration<C, L> {
    todo!("P0；只克隆同一节点Arc，不改变membership/facts，不克隆Report或键，不增执行权限。")
}

#[allow(unused_variables)]
fn default_deadline_inner<C, L>(
    this: &DefaultTimerAccess<C, L>,
    duration: Duration,
) -> pi_result::RawResult<GlobalManualInstant, TimerError>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P1；先本服务存续检查，退出返回ServiceClosed；clock.now一次真实取时，失败Clock(原Report)；clock.checked_add只用原时刻，失败DeadlineOutOfRange(原Report)。不读缓存另建时间轴，不重置截止、不初始化pi_clock，业务时长规则归上层入口。")
}

#[allow(unused_variables)]
fn default_prepare_inner<C, L>(
    this: &DefaultTimerAccess<C, L>,
    kind: TimerRegistrationKind,
) -> pi_result::RawResult<DefaultTimerRegistration<C, L>, TimerError>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P1；开放时一次Arc节点内准备TimerRegistration::new(kind)、Links::new、原Source/retry的Weak、IDLE、error=None、stub=false和PhantomPinned；同一节点全部后续重试复用。Owner退出为ServiceClosed，不取时／入队／创建Task。Arc标准分配失败沿分配器策略，不冒称其必返回可恢复报告；保持正时长等待准备错误的原报告语义。")
}

#[allow(unused_variables)]
fn default_register_inner<C, L>(
    this: &DefaultTimerAccess<C, L>,
    deadline: GlobalManualInstant,
    item: DefaultTimerItem<C, L>,
) -> pi_result::RawResult<CancelTimerKey, TimerSubmitError<DefaultTimerItem<C, L>>>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P1；owner_exiting Acquire为真返同一item及Closed，否则经同一backend.try_push(deadline,item)，原接受/拒绝原样保留。已开始交付与退出交错可被原来源接收并由真实关闭者清理；不得把已接受通知异常伪装拒绝。默认ThreadNotifier无用户回调；原后续退出帮助若触发用户恐慌，原TimerEntry负责先发布Released事实及合法移交，不补建另一键或直接poll。")
}

#[allow(unused_variables)]
fn default_register_local_inner<C, L>(
    this: &DefaultTimerAccess<C, L>,
    deadline: GlobalManualInstant,
    item: DefaultTimerItem<C, L>,
) -> pi_result::RawResult<CancelTimerKey, TimerSubmitError<DefaultTimerItem<C, L>>>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P1；先同样Acquire核owner_exiting，再L::try_push_local原runtime/deadline/item。L必须通过当前本地权限核服务和线程并调用原LocalTimerHandle；缺权限/错误目标返同一item及WrongOwner，短暂本地借用冲突Busy，不回退跨服务或增加Future Send/Sync，不直接操作来源私有表。")
}

#[allow(unused_variables)]
fn default_cancel_inner<C, L>(
    this: &DefaultTimerAccess<C, L>,
    key: &CancelTimerKey,
) -> pi_result::RawResult<(), TimerQueueError>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P1；owner_exiting已发布则Ok表示真实本地退出／关闭者已经接管该原服务所有登记的移除责任，不表示目标存在/成功或Future已drop。否则调用原backend.request_cancel。只有原Closed且本地退出意图或backend状态非Accepting时可视为同一关闭责任已接管并Ok；其余WrongQueue/Busy/Backend原错误保持。不能因关闭把键交别的源，也不能在已关闭来源无限重排相同取消；先前接收和取消分类不回滚。")
}

#[allow(unused_variables)]
fn default_defer_inner<C, L>(
    this: &DefaultTimerAccess<C, L>,
    registration: DefaultTimerRegistration<C, L>,
    error: Option<TimerQueueError>,
) -> ()
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P1；以registration节点的origin为准升级原Source，不把错误的self重定向到别的服务；合法核心调用在真实Access或本地核心保活期间。对同一membership一次强CAS IDLE->PUBLISHING取得链接资格，成功AcqRel以取得上次恢复IDLE的Release，失败Relaxed且绝不访问error槽；失败合并已有接续，不另入队。成功者独占error槽，若空保存第一份原错误，已有则保第一份、后来的原报告在发布完成且退出槽访问之后正常释放，不转字符串或复制；不得在错误Drop前丢掉接续责任。设置QUEUED Release，构造唯一RetryHandle(value同Arc,on_drop内部release_retry_unit)，经原RetryShared.publish交接并主动Thread通知。重复/关闭/并发不分配新节点、不等待键、不创建Task。原Source不存在只可能无活访问/本地拥有者后，此时不再链接节点；登记原值移除责任已由真实退出处理，不伪造Elapsed。")
}

#[allow(unused_variables)]
fn default_retry_resume_inner<C, L>(this: &Arc<CancelRetryNode<C, L>>) -> ()
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P1；仅在真实原生安全出队且Consumer/links引用已结束后调用，stub不能被当业务节点；membership由QUEUED恢复IDLE Release，再以origin升级原Source构造同源DefaultTimerAccess和同节点Registration，调用核心resume_timer_cancel一次。核心已结束或无待交接不制造新工作。仍暂不能交付时核心会复用同一节点再defer；原Handle在外层稍后Drop结算旧单位。origin失效时无活Access/本地核心/外部等待者，合法移除已交退出责任，不重新创建源。保第一份原错误直到节点最后释放，不在该槽恢复IDLE后读写error。")
}

#[allow(unused_variables)]
fn default_retry_into_ptr_inner<C: ExecutorClock<Instant = GlobalManualInstant>, L: 'static>(
    handle: RetryHandle<CancelRetryNode<C, L>>,
) -> NonNull<CancelRetryNode<C, L>> {
    todo!("P0；ManuallyDrop抑制整个Handle的单位结算，移动唯一Arc到Arc::into_raw并返回原节点地址。不clone、不分配、不减units，不把业务登记Arc包装误当另一单位；不要借整个节点可写引用。")
}

#[allow(unused_variables)]
fn default_retry_from_ptr_inner<C: ExecutorClock<Instant = GlobalManualInstant>, L: 'static>(
    ptr: NonNull<CancelRetryNode<C, L>>,
) -> RetryHandle<CancelRetryNode<C, L>> {
    todo!("P0；仅在原生允许的合法唯一归还时Arc::from_raw一次，恢复相同Arc持有。根据固定stub恢复on_drop=None或内部release_retry_unit函数，不新增单位/Arc强引用，不重复恢复同指针。原生Stub标记由原生自己维护。")
}

#[allow(unused_variables)]
fn default_retry_links_inner<C: ExecutorClock<Instant = GlobalManualInstant>, L: 'static>(
    ptr: NonNull<CancelRetryNode<C, L>>,
) -> NonNull<Links<CancelRetryNode<C, L>>> {
    todo!("P0；原始addr_of_mut!投影links，借用外部合法性与非空由Linked调用前提保证；不临造&mut整个节点、不读error/键或本地状态，不改变links和资格。")
}

#[allow(unused_variables)]
fn default_release_unit_inner<C: ExecutorClock<Instant = GlobalManualInstant>, L: 'static>(
    node: &CancelRetryNode<C, L>,
) -> () {
    todo!("P0；从节点固定retry Weak升级原队列，存在则调用其Release return_unit一次。它不直接assist或通知用户，真正独立Completion最后确认；升级失败仅可能原Shared最终独占释放阶段，已无外部可观察单位总数。不能以这个函数访问原Future/核心私有facts。")
}
