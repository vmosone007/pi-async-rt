#[allow(unused_variables)]
fn entry_task_inner<K: Send + Sync + 'static, R: AsRef<TimerRegistration<K>>>(
    task: Runnable,
    registration: R,
) -> TimerEntry<K, R> {
    todo!("目标：唯一移动原Runnable及原registration，settled=false；不得增加Task引用、创建新Task、接纳、取时或poll。登记用途应为Task；错误提供者不能导致UB。")
}
#[allow(unused_variables)]
fn entry_signal_inner<K: Send + Sync + 'static, R: AsRef<TimerRegistration<K>>>(
    registration: R,
) -> TimerEntry<K, R> {
    todo!("目标：构造Signal原值及原registration，settled=false，不创建任务。仅此登记等待结果信号；不在构造时完成或通知。")
}
#[allow(unused_variables)]
fn entry_due_inner<K: Send + Sync + 'static, R: AsRef<TimerRegistration<K>>>(
    mut this: TimerEntry<K, R>,
) -> Option<Runnable> {
    todo!("目标：先独占确定真实Elapsed并使普通Drop不再重报Removed，再取出唯一原Runnable。结束登记借用后才允许返回或释放原值。Signal只完成原等待，不构造额外Task。不能以调用本方法代替来源实际到期判定。")
}
#[allow(unused_variables)]
fn entry_recover_inner<K: Send + Sync + 'static, R: AsRef<TimerRegistration<K>>>(
    mut this: TimerEntry<K, R>,
) -> Option<Runnable> {
    todo!("目标：明确未接收的原值恢复，先disarm普通Drop结束路径；原Runnable原样取回。由外层保存DirectFailure及原错误，不误发RegistrationRemoved或取消，不能drop后再次访问原节点。")
}
#[allow(unused_variables)]
fn entry_drop_inner<K: Send + Sync + 'static, R: AsRef<TimerRegistration<K>>>(
    this: &mut TimerEntry<K, R>,
) {
    todo!("目标：尚未settled时先定Released及按此前取消事实分类的结果，撤销重复清理；再把原Runnable移到局部，结束全部字段／登记借用后drop它。原Task负责合法所属线程清理，来源移除不冒充FutureDrop确认。不catch_unwind、不重复析构。")
}
