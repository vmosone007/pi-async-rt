#[allow(unused_variables)]
fn yield_once_poll_inner(this: Pin<&mut YieldOnce>, cx: &mut Context<'_>) -> Poll<()> {
    todo!("目标：首次poll必须先设yielded=true，再wake_by_ref当前Waker，返回Pending；后续Ready(())。不创建其它Task、定时登记、通道或分配。原任务Waker在Pending交接后回原队列尾；裸block_on根使用其真实根Waker，不制造TaskId。子Future Pending不保证外层根也Pending，不保证他任务优先。")
}
