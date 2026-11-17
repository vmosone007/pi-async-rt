#[allow(unused_variables)]
fn sleep_new_inner<A: TimerAccess>(access: A, duration: Duration) -> Sleep<A> {
    todo!("编号：T204，尚未实现， 目标：零时长保存可立即完成形态，无登记；正时长在[1ms,1s]内组合Timeout，其它时长保存InvalidDuration。构造不取时、入队或暴露控制句柄。原时钟／准备错误到await返回，不panic掩盖错误。")
}
#[allow(unused_variables)]
fn sleep_poll_inner<A: TimerAccess>(
    this: Pin<&mut Sleep<A>>,
    cx: &mut Context<'_>,
) -> Poll<pi_result::RawResult<(), TimerError>> {
    todo!("编号：T205，尚未实现， 目标：首次归还保存错误或零时长Ok，否则轮询原Timeout。原错误保留不克隆；Ready后再poll明确panic，不默认成功。无单独取消句柄；Drop内部Timeout按原责任撤销。")
}
#[allow(unused_variables)]
fn timeout_on_new_inner<A: TimerAccess, F: Future>(
    access: A,
    future: F,
    duration: Duration,
) -> TimeoutOn<A, F> {
    todo!("编号：T206，尚未实现， 目标：只拥有原F、access及时长，不取时或准备定时源；timeout=None、done=false。真正首次poll先业务，再在业务Pending后构造计时等待。不能要求F额外Unpin／Send／Sync／static或强制Box::pin。")
}
#[allow(unused_variables)]
fn timeout_on_poll_inner<A: TimerAccess, F: Future>(
    this: Pin<&mut TimeoutOn<A, F>>,
    cx: &mut Context<'_>,
) -> Poll<pi_result::RawResult<F::Output, TimeoutError>> {
    todo!("编号：T207，尚未实现， 目标：合法结构性Pin投影Option<F>并优先poll业务；Ready先保输出、原地清空已固定F并结束计时兴趣，返回Ok。业务Pending后首次才构造并poll Timeout，后续不重置时钟。计时Elapsed返回Elapsed，其它原错误Timer；返回前原地析构业务F，不等待外部保留本包装才清理。不得把Pin字段take移动；普通panic沿Rust展开，已有清理保持，不catch或重复drop。双方Pending注册各自Waker后Pending；Drop字段自然原地处理。")
}
