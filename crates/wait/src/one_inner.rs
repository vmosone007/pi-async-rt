// 唯一等待的局部实现入口；真实类型、状态和必要关系由父模块声明。

use super::*;

pub(super) fn one_create_inner<T>() -> (WaitOne<T>, OneSender<T>) {
    let shared = Arc::new(OneShared {
        state: AtomicUsize::new(0),
        value: UnsafeCell::new(MaybeUninit::uninit()),
        notify: AtomicWaker::new(),
    });
    (
        WaitOne { shared: Some(Arc::clone(&shared)) },
        OneSender { shared: Some(shared) },
    )
}

pub(super) fn one_poll_inner<T>(wait: &mut WaitOne<T>, context: &mut Context<'_>) -> Poll<RawResult<T, WaitError>> {
    let shared = wait.shared.as_ref().expect("等待（wait）已完成，不能重复轮询（poll）");
    let check = || {
        let mut state = shared.state.load(std::sync::atomic::Ordering::Acquire);
        loop {
            if state & OneShared::<T>::VALUE_READY != 0 {
                assert_eq!(state & OneShared::<T>::VALUE_TAKEN, 0, "原值（value）已取走");
                match shared.state.compare_exchange(
                    state,
                    state | OneShared::<T>::VALUE_TAKEN,
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Acquire,
                ) {
                    Ok(_) => {
                        // SAFETY: 获取发布（acquire publication）确认初始化，本次唯一取走许可（VALUE_TAKEN）已成立；
                        // 接收端共享持有（Arc）仍存活，不移动未初始化原值（value）。
                        return Some(Ok(unsafe { (*shared.value.get()).assume_init_read() }));
                    }
                    Err(actual) => state = actual,
                }
            } else if state & OneShared::<T>::NO_VALUE != 0 {
                return Some(Err(WaitError::SenderDropped));
            } else {
                return None;
            }
        }
    };
    let result = match check() {
        Some(result) => Some(result),
        None => {
            // 完成检查（completion check）与实际通知登记（notification registration）间可交值，登记后必须复查。
            shared.notify.register(context.waker());
            check()
        }
    };
    match result {
        Some(result) => {
            let shared = wait.shared.take().expect("接收责任（receiver responsibility）仍有效");
            // 原值（value）已唯一移出，关闭（close）仅结束接收责任，不能再次析构（drop）它。
            shared.state.fetch_or(OneShared::<T>::RECEIVER_CLOSED, std::sync::atomic::Ordering::AcqRel);
            Poll::Ready(result)
        }
        None => Poll::Pending,
    }
}

pub(super) fn one_send_inner<T>(shared: &OneShared<T>, value: T) -> RawResult<(), SendError<T>> {
    let mut state = shared.state.load(std::sync::atomic::Ordering::Acquire);
    loop {
        if state & OneShared::<T>::RECEIVER_CLOSED != 0 {
            return Err(SendError::new(crate::definitions::SendErrorKind::ReceiverClosed, value));
        }
        assert_eq!(state & OneShared::<T>::VALUE_CLAIMED, 0, "唯一交值许可（send permission）已消耗");
        match shared.state.compare_exchange(
            state,
            state | OneShared::<T>::VALUE_CLAIMED,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        ) {
            Ok(_) => break,
            Err(actual) => state = actual,
        }
    }
    // SAFETY: 唯一发送者（sender）已获写入许可，接收方未见VALUE_READY不能读；局部Arc覆盖写入。
    unsafe { (*shared.value.get()).write(value); }
    // 同字发布（publication）的前态决定与关闭（close）的先后，不能用后续load替代。
    let previous = shared.state.fetch_or(OneShared::<T>::VALUE_READY, std::sync::atomic::Ordering::AcqRel);
    let cleanup = if previous & OneShared::<T>::RECEIVER_CLOSED != 0
        && shared.state.fetch_or(OneShared::<T>::VALUE_TAKEN, std::sync::atomic::Ordering::AcqRel)
            & OneShared::<T>::VALUE_TAKEN == 0
    {
        // SAFETY: 本次写入已完成且本方取得唯一取走许可（VALUE_TAKEN）；关闭方不能重复读取。
        Some(unsafe { (*shared.value.get()).assume_init_read() })
    } else {
        None
    };
    // 原值（T）已移到普通局部所有权（local ownership），通知（notification）展开也会归还本方清理责任。
    shared.notify.wake();
    drop(cleanup);
    Ok(())
}

pub(super) fn one_close_inner<T>(shared: &OneShared<T>) {
    let state = shared.state.fetch_or(
        OneShared::<T>::RECEIVER_CLOSED,
        std::sync::atomic::Ordering::AcqRel,
    );
    // 与保存完成（VALUE_READY）的同字发布（publication）确定清理方；未就绪时由写者接续，不等待。
    if state & (OneShared::<T>::VALUE_READY | OneShared::<T>::VALUE_TAKEN)
        == OneShared::<T>::VALUE_READY
    {
        let previous = shared.state.fetch_or(
            OneShared::<T>::VALUE_TAKEN,
            std::sync::atomic::Ordering::AcqRel,
        );
        if previous & OneShared::<T>::VALUE_TAKEN == 0 {
            // SAFETY: 已获取保存完成（VALUE_READY）且唯一取走许可（VALUE_TAKEN）；
            // 局部共享持有（Arc）保活，先移出原值（T）再执行用户析构（drop）。
            let value = unsafe { (*shared.value.get()).assume_init_read() };
            drop(value);
        }
    }
}

pub(super) fn one_sender_drop_inner<T>(shared: &OneShared<T>) {
    let previous = shared.state.fetch_or(
        OneShared::<T>::NO_VALUE,
        std::sync::atomic::Ordering::AcqRel,
    );
    // 外壳（shell）先移出唯一发送责任（sender responsibility）；结束事实先发布，通知（notification）异常不触发第二次退出。
    if previous & OneShared::<T>::RECEIVER_CLOSED == 0 {
        shared.notify.wake();
    }
}

pub(super) fn one_shared_drop_inner<T>(shared: &mut OneShared<T>) {
    let state = shared.state.get_mut();
    if *state & (OneShared::<T>::VALUE_READY | OneShared::<T>::VALUE_TAKEN)
        == OneShared::<T>::VALUE_READY
    {
        *state |= OneShared::<T>::VALUE_TAKEN;
        // SAFETY: 最后共享持有（Arc）已独占记录；保存完成（VALUE_READY）确认初始化，
        // 取走标记（VALUE_TAKEN）先成立，原值（T）仅移出一次，不留字段借用（field borrow）。
        let value = unsafe { (*shared.value.get()).assume_init_read() };
        drop(value);
    }
}
