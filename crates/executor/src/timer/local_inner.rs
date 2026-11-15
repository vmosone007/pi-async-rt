#[allow(unused_variables)]
fn driver_new_inner<C, L>(
    runtime: RuntimeId,
    clock: C,
    notifier: ThreadNotifier,
    options: CancelTimerOptions,
) -> pi_result::Result<DefaultTimerDriver<C, L>>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P0；先clock.now一次取同源初始参考，原Report原样返回；构造CancelTimerQueue::new(now,notifier.clone,options)及一份独立取消接续Stub（facts用途Task但stub=true，不对外），RetryOwner::new额度options.work_batch_size。取其原Shared，Arc<Source>保存原runtime/clock/backendHandle/retryShared/owner_exiting=false；用Rc<TimerLocalCore>保存Some(nativeQ)、RetryOwner、同一Source、当前ThreadId和close_error=None。所有已建资源配对释放，失败不导出半成品，不增加Task/线程、锁或新时间轴。")
}

#[allow(unused_variables)]
fn driver_local_handle_inner<C, L>(this: &DefaultTimerDriver<C, L>) -> LocalTimerHandle<C, L>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P0；Rc::downgrade原core，保存原runtime/ThreadId；不制造长期Rc，不把本地core放Arc或Send包装。")
}

#[allow(unused_variables)]
fn driver_drop_inner<C, L>(this: &mut DefaultTimerDriver<C, L>) -> ()
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P0；先Release发布同一Source.owner_exiting=true，撤销新访问意图，随后让本字段Rc正常归还。不能在现有本地活动仍借用时强行borrow_mut/take或声明已清理；活动临时Rc保住原core，在它结束后真实Core Drop依次释放原生来源与RetryOwner。没有长期本地句柄Rc，最后本地拥有者不会依赖外部Send句柄消失。")
}

#[allow(unused_variables)]
fn local_timer_clone_inner<C, L>(this: &LocalTimerHandle<C, L>) -> LocalTimerHandle<C, L>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P0；只克隆同一Weak及复制固定身份；不延后原生来源Drop，不建立本地权限。")
}

#[allow(unused_variables)]
fn local_timer_push_inner<C, L>(
    this: &LocalTimerHandle<C, L>,
    runtime: RuntimeId,
    deadline: GlobalManualInstant,
    item: DefaultTimerItem<C, L>,
) -> pi_result::RawResult<CancelTimerKey, TimerSubmitError<DefaultTimerItem<C, L>>>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P1；先核原runtime/线程、Weak升级与Acquire退出意图；错目标/线程WrongOwner，失效/退出Closed，均返回同一item。开放时先建立ActivityGuard保该次Rc，之后try_borrow_mut queue；冲突Busy，None为Closed。调用原native.try_push_local一次，先保存独立返回值，再结束RefMut，最后退出ActivityGuard；不在槽借用内Drop拒绝原值或用户报告。不额外要求Send/Sync，不把本地请求变共享节点。若回调使Driver在本次活动中退出，临时Rc保住Core，守卫先结束借用再继续退场；实际成功后来源关闭也不改成拒绝。")
}

#[allow(unused_variables)]
fn driver_native_handle_inner<C, L>(
    this: &DefaultTimerDriver<C, L>,
) -> CancelTimerQueueHandle<DefaultTimerItem<C, L>, ThreadNotifier>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P0；克隆Source.backend同一原生受限句柄，不导出本地容器，最终运行时不将此低层句柄交普通业务。")
}

#[allow(unused_variables)]
fn driver_local_push_inner<C, L>(
    this: &mut DefaultTimerDriver<C, L>,
    deadline: GlobalManualInstant,
    item: DefaultTimerItem<C, L>,
) -> pi_result::RawResult<CancelTimerKey, TimerSubmitError<DefaultTimerItem<C, L>>>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P1；通过本驱动同源LocalTimerHandle按同样活动守卫路径本地提交，运行时核心通过L入口在此之前核TLS权限；作为低层原拥有者调用不要求额外TLS。拒绝原值和接收后退场事实不改变。")
}

#[allow(unused_variables)]
fn driver_local_cancel_inner<C, L>(
    this: &mut DefaultTimerDriver<C, L>,
    key: &CancelTimerKey,
) -> pi_result::RawResult<(), TimerCancelRequestError>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P1；先ActivityGuard、后短暂try_borrow_mut原Q，缺Q Closed/借用Busy，原native.request_cancel_local借同一key记录；保存结果后结束借用再退出ActivityGuard。不等待实际移除、不读私有登记表，不额外读时钟。")
}

#[allow(unused_variables)]
fn driver_advance_inner<C, L>(
    this: &mut DefaultTimerDriver<C, L>,
    now: &GlobalManualInstant,
) -> pi_result::RawResult<TimerAdvanceProgress, TimerQueueError>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P1；同一活动守卫和短暂排他本地借用，只调用native.advance(now)一次，原错误保持。显式now不能刷新／换算另一时间轴，ReachedTarget不等于全部到期已发现。关闭意图时不因此跳过实际清理/退出，不能用borrow冲突自旋。")
}

#[allow(unused_variables)]
fn driver_due_one_inner<C, L>(
    this: &mut DefaultTimerDriver<C, L>,
) -> pi_result::RawResult<TimerTake<DefaultTimerItem<C, L>>, TimerQueueError>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P1；活动守卫下native.try_take_due_one一次，独立TimerEntry原样返回，先结束来源借用再退出守卫和让外层into_due。不得在借用内调用into_due/Waker/业务poll，不改变原生Ready/Empty/WouldBlock/Busy/Closed分类。")
}

#[allow(unused_variables)]
fn driver_due_all_inner<C, L>(
    this: &mut DefaultTimerDriver<C, L>,
) -> pi_result::RawResult<TimerTake<Vec<DefaultTimerItem<C, L>>>, TimerQueueError>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P1；活动守卫下native.try_take_due_all一次，不加数量上限、不把发现前缀当全部，不重新排列。独立非空原TimerEntry向量先结束本地借用才返回，不在借用内逐项into_due或Drop用户原值。")
}

#[allow(unused_variables)]
fn driver_cleanup_inner<C, L>(
    this: &mut DefaultTimerDriver<C, L>,
) -> pi_result::Result<QueueCleanupProgress>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P1；有限处理原native.process_cleanup，再在来源本地借用退出后处理一次retryOwner.process；前者原Report不能被后续结果覆盖。保留无返回口首次close_error并在后续显式process_cleanup或close出口移交一次，纯只读查询不取走报告；MoreWork不冒充全部清完。使用同一ActivityGuard，不因未到期或非空闲跳过清理，不直接poll。")
}

#[allow(unused_variables)]
fn driver_next_inner<C, L>(
    this: &DefaultTimerDriver<C, L>,
    now: &GlobalManualInstant,
) -> pi_result::RawResult<Option<GlobalManualInstant>, TimerQueueError>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P1；只短暂不可变try_borrow原Q并native.next_check_at(now)，Busy原分类，不取时、不推进、不遍历。已有取消重试可立即推进时返回Some(*now)保守上限，不把未知退场责任当None；不能暴露RefCell借用到外部。")
}

#[allow(unused_variables)]
fn driver_status_inner<C, L>(this: &DefaultTimerDriver<C, L>) -> TimerQueueStatus
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P1；原backend状态LocalExited保持，尚未退出且owner_exiting已Acquire为真则Closing，其余原状态。只有独立快照，不借本地Q或因count零报LocalExited。")
}

#[allow(unused_variables)]
fn driver_len_inner<C, L>(this: &DefaultTimerDriver<C, L>) -> usize
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P1；直接原backend.len单次原子样本，不把取消接续单位加进业务定时登记数，不扫描/推进。")
}

#[allow(unused_variables)]
fn driver_empty_inner<C, L>(this: &DefaultTimerDriver<C, L>) -> bool
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P1；同一原来源数量口径单样本等于零，不代表服务关闭或重试无责任。")
}

#[allow(unused_variables)]
fn driver_clear_inner<C, L>(
    this: &mut DefaultTimerDriver<C, L>,
) -> pi_result::Result<QueueCleanupProgress>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P2；同一活动守卫下短暂本地借用调用native.clear一次，保留其固定范围与原Report，不停新接收，不把重试节点直接丢弃或假造登记Elapsed。来源确认只针对该原生固定范围；核心登记随原TimerEntry合法移除结束，节点持有可稍后正常结清，不影响Future合法清理。")
}

#[allow(unused_variables)]
fn driver_close_inner<C, L>(
    this: &mut DefaultTimerDriver<C, L>,
) -> pi_result::Result<QueueCleanupProgress>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P2；Release永久owner_exiting=true，然后core.finish_close_step，不恢复新服务。即使本地借用暂Busy，当前真实ActivityGuard在借用结束后继续关闭；无活动时本驱动自身拥有关闭责任。实际NativeClose已经开始或由该真实活动者接续后，允许retry来源自动帮助取消元数据尾部；这部分可处理实际有限已接受尾部，不保证固定64项总耗时，不等待其它参与者。只有nativeClose Complete且当次重试责任Complete、无待报告原错误才Complete；MoreWork/Err保真实责任，重复幂等。")
}

#[allow(unused_variables)]
fn driver_access_inner<C, L>(this: &DefaultTimerDriver<C, L>) -> DefaultTimerAccess<C, L>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P0；Arc克隆同一Source，保持runtime/clock/backend/retries；不保活本地Rc、复制原生来源或取得执行器推进权限。")
}

#[allow(unused_variables)]
fn driver_retries_inner<C, L>(
    this: &mut DefaultTimerDriver<C, L>,
) -> pi_result::Result<QueueCleanupProgress>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P1；只对本地retryOwner短暂try_borrow_mut，调用一次process，借用结束后返回；冲突用pi_result Busy，保存第一份close_error需要由管理路径准确取回，不静默丢弃。不刷新时钟，不进入native.advance，不把重试单位当timer len。")
}

#[allow(unused_variables)]
fn driver_finish_close_inner<C, L>(
    this: &TimerLocalCore<C, L>,
) -> pi_result::Result<QueueCleanupProgress>
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P2；只作内部一次非递归关闭步，不再创建ActivityGuard。先try_borrow_mut原queue，Busy保持意图且真实外层活动者会继续；None视原生已移走。native.close建立停收，保存结果并结束借用；随后RetryShared启用自动接续并帮助真实有限元数据尾部，原源closing使Core重试有限结束而不因Closed再入队。不能在原生借用/Consumer内调用核心/错误Drop。已有close_error在借用外取走原Report返回一次，不因此跳过剩余责任。Native和重试均Complete才Complete；所有首次原错误不复制不替换，native抛panic时其自身守卫保责、外层Core最后Drop仍清理。")
}

#[allow(unused_variables)]
fn driver_record_close_error_inner<C, L>(this: &TimerLocalCore<C, L>, error: pi_result::Error) -> ()
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P2；在来源及Consumer借用外保存第一份原Report到close_error，已有首份时保首份、退出槽借用后再Drop后来报告；不转字符串/clone，不覆盖静默丢失。该错误只供后续显式清理/关闭出口移交或Core最后按普通Drop清理，不宣称已向业务交付。")
}

#[allow(unused_variables)]
fn driver_core_drop_inner<C, L>(this: &mut TimerLocalCore<C, L>) -> ()
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P0；最后本地Rc保证无仍存原生借用，先Release owner_exiting；用RefCell::get_mut取走Option原Q并结束槽访问，再Drop原生拥有者，先建立其停收/退出与全部原值合法责任。RetryOwner字段之后才自动Drop，shared最后仍保活供元数据帮助，原字段close_error正常释放。已有用户原panic不catch，失败原值不重复；再次panic沿原进程边界。不能等待外部Send句柄，不用try_borrow失败后跳过实际Q Drop，不移动存活的非Send原Future到别的线程。")
}

#[allow(unused_variables)]
fn driver_activity_drop_inner<C, L>(this: &mut TimerLocalActivityGuard<C, L>) -> ()
where
    C: ExecutorClock<Instant = GlobalManualInstant>,
    L: LocalTimerSubmit<DefaultTimerItem<C, L>>,
{
    todo!("P0；先撤armed，只在本次所有原Q借用已退出且owner_exiting已发布时调用一次finish_close_step；MoreWork保持原Core关闭责任，Err原Report经record_close_error保存，不能假Complete、静默吞错或递归构造本ActivityGuard。当前活动Rc随后归还，若Driver早已在本次回调里释放，真正Core Drop此时在原线程触发，保证NativeQ在RetryOwner之前实际退出。用户不返回会延迟退出不作时限承诺；原展开中再次用户panic按进程边界，不新catch。")
}
