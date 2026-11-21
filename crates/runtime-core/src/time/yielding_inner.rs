#[allow(unused_variables)]
fn yield_once_poll_inner(this: Pin<&mut YieldOnce>, cx: &mut Context<'_>) -> Poll<()> {
    let this = this.get_mut();
    if this.yielded {
        Poll::Ready(())
    } else {
        // 唤醒（wake-up）前记录已让出（yielded），避免唤醒器（Waker）恐慌（panic）后重复首次通知（notification）。
        this.yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
