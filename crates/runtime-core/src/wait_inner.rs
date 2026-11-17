#[allow(unused_variables)]
fn wait_one_timeout_poll_inner<A: TimerAccess, T>(
    this: Pin<&mut WaitOneTimeout<A, T>>,
    cx: &mut Context<'_>,
) -> Poll<pi_result::RawResult<T, WaitTimeoutError>> {
    todo!(r#"编号：T245，尚未实现， 目标：合法投影原TimeoutOn，不移动已固定字段；将Ok(Ok(T))交原T、Ok(Err(WaitError))为Wait、超时为Elapsed、计时失败保原TimerError。业务优先，发送已接受事实不回滚，不另建通道／TimerTask。验证先发送、后发送、接收先Drop、超时与send竞争及!Unpin／借用T。"#)
}
#[allow(unused_variables)]
fn wait_any_timeout_poll_inner<A: TimerAccess, T>(
    this: Pin<&mut WaitAnyTimeout<A, T>>,
    cx: &mut Context<'_>,
) -> Poll<pi_result::RawResult<T, WaitTimeoutError>> {
    todo!("编号：T246，尚未实现， 目标：复用原WaitAny和TimeoutOn一次胜者语义，结果映射同one；超时关闭原接收兴趣，原已接受发送不改写为ReceiverClosed，已有ValueAlreadyAccepted仍保持。禁止重做并发发送协议、丢原值或额外T约束。")
}
#[allow(unused_variables)]
fn wait_all_timeout_poll_inner<A: TimerAccess, T>(
    this: Pin<&mut WaitAllTimeout<A, T>>,
    cx: &mut Context<'_>,
) -> Poll<pi_result::RawResult<Vec<pi_result::RawResult<T, WaitError>>, TimeoutError>> {
    todo!("编号：T247，尚未实现， 目标：正常结果按原登记顺序完整交原向量；超时不交部分向量，原接收Drop按其协议清理已经接收的值，外部Sender后续返回原值与关闭原因。原sender释放计入逐项错误，不将其改成整体失败。计时组合及Pin投影沿TimeoutOn，无全局扫描或新锁。")
}
