#[allow(unused_variables)]
fn root_notified_inner(this: &RootWakeState) -> bool {
    this.notified.load(std::sync::atomic::Ordering::Acquire)
}
#[allow(unused_variables)]
fn root_wake_inner(this: &RootWakeState) {
    if !this.notified.swap(true, std::sync::atomic::Ordering::Release) {
        this.thread.unpark();
    }
}
#[allow(unused_variables)]
fn root_park_inner(this: &RootWakeState) {
    // 通知（notification）由推进循环（drive loop）消费；裸线程许可（thread permit）或伪唤醒（spurious wake-up）不能触发根轮询（root poll）。
    while !this.is_notified() {
        std::thread::park();
    }
}
#[allow(unused_variables)]
fn root_block_on_inner<F: Future>(future: F) -> F::Output {
    let _scope = DriveScope::enter();
    let root = Arc::new(RootWakeState {
        thread: std::thread::current(),
        notified: AtomicBool::new(true),
    });
    let waker = Waker::from(root.clone());
    let mut context = Context::from_waker(&waker);
    // 固定根值（pinned root value）后声明，正常返回或展开（unwinding）时先原地析构（in-place drop），最后恢复活动范围（active scope）。
    let mut future = std::pin::pin!(future);
    loop {
        if root.notified.swap(false, std::sync::atomic::Ordering::Acquire) {
            if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
                return output;
            }
        }
        root.park();
    }
}
#[allow(unused_variables)]
fn driver_block_on_inner<F: Future, D: BlockOnDriver>(future: F, driver: &mut D) -> F::Output {
    let _scope = DriveScope::enter();
    let root = Arc::new(RootWakeState {
        thread: std::thread::current(),
        notified: AtomicBool::new(true),
    });
    let waker = Waker::from(root.clone());
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        if root.notified.swap(false, std::sync::atomic::Ordering::Acquire) {
            if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
                return output;
            }
        }
        // 每轮只推进一次原服务（service turn）；根就绪（root Ready）已直接返回，不再轮转或停放（park）。
        if !driver.turn() {
            driver.park(&root);
        }
    }
}
