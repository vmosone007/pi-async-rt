#[allow(unused_variables)]
fn registration_new_inner<K: Send + Sync + 'static>(
    kind: TimerRegistrationKind,
) -> TimerRegistration<K> {
    todo!("目标：facts=0，未初始化键槽，forwarding=Idle；Task不需要等待结果，Wait建立Pending outcome及AtomicWaker。无取时／入队／额外Task，完整共享包装与重试节点由实现者一次准备。")
}
#[allow(unused_variables)]
fn registration_cancel_inner<K: Send + Sync + 'static>(this: &TimerRegistration<K>) -> bool {
    todo!("目标：有界强CAS记录CANCEL_REQUESTED，重复请求合并。若登记交付责任RELEASED先结束则false；INTEREST_CLOSED仅关闭用户等待，不能阻止Drop内部提出撤销。公开控制另在入口检查兴趣已结束。成功表示请求已记，不表示实际移除。只增字至多七项变化，不用weak CAS制造伪失败自旋，不等待键或另一转交者；先取消再begin的情况无需入队。")
}
#[allow(unused_variables)]
fn registration_wait_cancel_inner<K: Send + Sync + 'static>(this: &TimerRegistration<K>) -> bool {
    todo!("目标：用户控制专用取消记录。用同一只增事实字的有界强CAS同时核INTEREST_CLOSED／RELEASED并记录CANCEL_REQUESTED；结束先成立返回false，开放重复请求可合并为true。不能先独立load兴趣后调用不检查兴趣的内部取消。内部Drop仍用request_cancel，兴趣关闭不阻挡真正撤销。")
}
#[allow(unused_variables)]
fn registration_key_inner<K: Send + Sync + 'static>(this: &TimerRegistration<K>, key: K) -> bool {
    todo!("目标：在本记录自身取得一次KEY_WRITER资格后raw pointer write初始化K，再Release发布KEY_READY。失去资格不得重复写槽，应按提供者协议错误安全释放新输入而不UB。不能仅信安全AsRef在每次调用返回同一记录；发布操作取得此前CANCEL_REQUESTED事实并返回bool，由调用方仅对已存在请求安排接续，不能无条件取消新登记。已结束登记不得重新激活。")
}
#[allow(unused_variables)]
fn registration_key_ref_inner<K: Send + Sync + 'static>(this: &TimerRegistration<K>) -> Option<&K> {
    todo!("目标：Acquire读KEY_READY，未就绪立即None，已就绪仅借已初始化K，生命期限本记录借用。不clone、不move、不等待或建立整个记录的&mut引用。")
}
#[allow(unused_variables)]
fn registration_begin_inner<K: Send + Sync + 'static>(this: &TimerRegistration<K>) -> bool {
    todo!("目标：只增事实字内一次开始交接；已取消且未开始或已结束时不开始，交调用者发布对应结果；开始一旦成功由RegistrationGuard承接后续时钟／拒绝／异常责任。Acquire／Release按真实载荷发布关系选择，不用SeqCst。")
}
#[allow(unused_variables)]
fn registration_finish_inner<K: Send + Sync + 'static>(
    this: &TimerRegistration<K>,
    reason: RegistrationEnd,
) {
    todo!("目标：原子终结登记交付责任一次，先保存RELEASED事实再发布Wait结果及wake；Task只保存事实。到期交付Elapsed；实际移除且此前已有取消请求为Cancelled，其它Removed。DirectFailure不被普通Drop覆盖，原错误由等待值持有。迟到取消不得重写结果；释放原Runnable必须在字段访问和保护结束之后。")
}
#[allow(unused_variables)]
fn registration_interest_inner<K: Send + Sync + 'static>(this: &TimerRegistration<K>) {
    todo!("目标：记录INTEREST_CLOSED并防止后续向已放弃等待交结果；不假装原来源已完成清理。由调用者接续取消请求。销毁等待Waker等外部值不能发生在共享状态保护或裸槽引用仍有效时。")
}
#[allow(unused_variables)]
fn registration_poll_inner<K: Send + Sync + 'static>(
    this: &TimerRegistration<K>,
    cx: &mut std::task::Context<'_>,
) -> std::task::Poll<RegistrationEnd> {
    todo!("目标：先Acquire读取结果，未定则AtomicWaker注册当前Waker后再读，避免丢唤醒；只用于Wait。返回已确定原因，不把取消标记当成实际移除。Task误用不得UB；明确panic或已定内部错误，不返回伪Ready。")
}
#[allow(unused_variables)]
fn registration_claim_inner<K: Send + Sync + 'static>(
    this: &TimerRegistration<K>,
    deferred: bool,
) -> bool {
    todo!("目标：正常调用只尝试Idle->Publishing，真实接续处理只尝试Deferred->Publishing；一次强CAS，竞争者合并。须有已请求、键就绪且尚未结束事实；无条件不等待、不空转。取得后必须由ForwardGuard承担异常归还或合法转交。")
}
#[allow(unused_variables)]
fn registration_defer_inner<K: Send + Sync + 'static>(this: &TimerRegistration<K>) {
    todo!("目标：唯一Publishing责任交给已准备真实接续节点，Release标为Deferred；先保证节点接管，不能出现状态已改却无人持有的空窗。不能多次排入同一节点、每次重试分配或仅记录请求后遗忘责任。")
}
#[allow(unused_variables)]
fn registration_forwarded_inner<K: Send + Sync + 'static>(
    this: &TimerRegistration<K>,
    forwarded: bool,
) {
    todo!("目标：原请求实际交出时记录CANCEL_FORWARDED；来源已结束则仅结束转交。最后Release发布Finished，不报告实际取消成功，不重置只增事实，不产生无限重查请求。")
}
#[allow(unused_variables)]
fn registration_drop_inner<K: Send + Sync + 'static>(this: &mut TimerRegistration<K>) {
    todo!("目标：最终独占释放时，仅KEY_READY成立才drop_in_place原K一次；未初始化槽不读取不drop，任何原值引用结束之后再释放。普通字段完成其各自析构，不执行源取消／业务poll，不catch_unwind，不把元数据回收等同原Future已析构。")
}
