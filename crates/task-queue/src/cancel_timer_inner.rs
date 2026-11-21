fn cancel_timer_new_inner<Item: 'static, N: QueueNotifier>(
    shared: Arc<TimerShared<Item, N>>, local: TimerLocalState<Item>,
) -> CancelTimerQueue<Item, N> {
    CancelTimerQueue { shared, local, owner_thread: PhantomData }
}

fn cancel_timer_handle_inner<Item: 'static, N: QueueNotifier>(
    shared: Arc<TimerShared<Item, N>>,
) -> CancelTimerQueueHandle<Item, N> {
    CancelTimerQueueHandle { shared }
}
