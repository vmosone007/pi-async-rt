fn cancel_timer_new_inner<Item: 'static, N: QueueNotifier>(
    shared: Arc<TimerShared<Item, N>>, local: TimerLocalState<Item>,
) -> CancelTimerQueue<Item, N> {
    todo!(r#"目标：把已完整准备的同一shared和local按值装入唯一拥有者，owner_thread=PhantomData。
边界：不再分配/取时/接收/通知，不调用清理，不克隆或移动Item；不增加Send/Sync实现。
判据：创建线程限制仅由PhantomData<Rc<()>>静态表达，不增加Rc分配或逐调用TLS读取。"#)
}

fn cancel_timer_handle_inner<Item: 'static, N: QueueNotifier>(
    shared: Arc<TimerShared<Item, N>>,
) -> CancelTimerQueueHandle<Item, N> {
    todo!(r#"目标：按值装入已经由外层克隆的一份同源Arc，不再克隆，不增加Item/N的Clone约束。
边界：不创建来源、不读取本地、不改变身份/接收/数量/取消状态，不增加其它字段或分配。
判据：句柄自然具有Clone+Send+Sync；不能为其补unsafe实现来掩盖共享请求封装错误。"#)
}
