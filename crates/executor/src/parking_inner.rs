fn thread_parker_new_inner() -> ThreadParker {
    let thread = std::thread::current();
    let owner = thread.id();
    ThreadParker {
        notifier: ThreadNotifier {
            shared: Arc::new(ThreadNotification {
                thread,
                notified: AtomicBool::new(true),
            }),
        },
        owner,
        local: PhantomData,
    }
}
fn thread_notifier_eq_inner(this: &ThreadNotifier, other: &ThreadNotifier) -> bool {
    // 只比较通知状态（notification state）身份；线程（thread）编号相同也不代表同一状态。
    // 不读取或消费通知标记，不唤醒、不克隆、不分配。
    Arc::ptr_eq(&this.shared, &other.shared)
}

#[allow(unused_variables)]
fn thread_notify_inner(shared: &ThreadNotification) {
    if !shared.notified.swap(true, std::sync::atomic::Ordering::Release) {
        shared.thread.unpark();
    }
}
#[allow(unused_variables)]
fn thread_is_notified_inner(shared: &ThreadNotification) -> bool {
    shared.notified.load(std::sync::atomic::Ordering::Acquire)
}
#[allow(unused_variables)]
fn thread_take_notification_inner(this: &mut ThreadParker) -> bool {
    assert_eq!(
        std::thread::current().id(),
        this.owner,
        "只能由创建线程取得通知（only the creating thread may take notifications）",
    );
    this.notifier.shared.notified.swap(false, std::sync::atomic::Ordering::Acquire)
}
#[allow(unused_variables)]
fn thread_park_inner(this: &mut ThreadParker, duration: Duration) -> ThreadParkResult {
    if this.take_notification() {
        return ThreadParkResult::Notified;
    }
    if duration.is_zero() {
        return ThreadParkResult::Recheck;
    }
    std::thread::park_timeout(duration);
    if this.take_notification() {
        ThreadParkResult::Notified
    } else {
        ThreadParkResult::Recheck
    }
}
