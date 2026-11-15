#[allow(unused_variables)]
fn default_idle_inner(state: &ExecutorSnapshot) -> bool {
    todo!("P1；ready_tasks==0，且(internal_tasks==0或returns_deferred)，且没有真正立即定时工作，且(!can_admit_external或external_tasks==0或external_deferred)，才Idle。总逻辑数量非零不能推翻已尝试WouldBlock/Busy的暂不可用事实，允许依通知和被动上限重查，不等待暂停发布者。纯Pending/in_flight/未到期timer/尚未选择idle默认不否决；串行外部积压不迫忙等。只读快照，不查O(n)状态或调源。")
}
#[allow(unused_variables)]
fn default_sleep_inner(state: &ExecutorSnapshot) -> Option<Duration> {
    todo!("P1；按配对默认Idle的实际ready/本轮deferred判定无可立即工作时建议Some(10ms)，其它None。维护欠账、发布中count非零和未选择idle不单独强制忙等，仍每轮有限推进。空闲项处理后才调用；最终等待min(建议,配置上限,真实定时最迟重查剩余)。原定时next_check已经区分只等另一发布者，不把它误当立即推进；不改源接口保证。发布后主动notify及最终重查防漏唤醒。")
}
