// 任一等待的局部实现入口；有效计数和原值许可属于同一个状态字。

use super::*;

pub(super) fn any_create_inner<T>() -> (WaitAny<T>, AnySender<T>) {
    let shared = Arc::new(AnyShared {
        state: AtomicUsize::new(AnyShared::<T>::COUNT_ONE),
        value: UnsafeCell::new(MaybeUninit::uninit()),
        notify: AtomicWaker::new(),
    });
    (
        WaitAny { shared: Some(Arc::clone(&shared)) },
        AnySender { state: AnySenderState::Active(shared) },
    )
}

pub(super) fn any_poll_inner<T>(wait: &mut WaitAny<T>, context: &mut Context<'_>) -> Poll<RawResult<T, WaitError>> {
    let shared = wait.shared.as_ref().expect("等待（wait）已完成，不能重复轮询（poll）");
    let check = || {
        let mut state = shared.state.load(std::sync::atomic::Ordering::Acquire);
        loop {
            if state & AnyShared::<T>::VALUE_READY != 0 {
                assert_eq!(state & AnyShared::<T>::VALUE_TAKEN, 0, "原值（value）已取走");
                match shared.state.compare_exchange(
                    state, state | AnyShared::<T>::VALUE_TAKEN,
                    std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire,
                ) {
                    Ok(_) => {
                        // SAFETY: READY获取发布（acquire publication）确认初始化，本方唯一取得TAKEN，接收Arc保活。
                        return Some(Ok(unsafe { (*shared.value.get()).assume_init_read() }));
                    }
                    Err(actual) => state = actual,
                }
            } else if state & (AnyShared::<T>::NO_VALUE | AnyShared::<T>::VALUE_CLAIMED)
                == AnyShared::<T>::NO_VALUE
            {
                return Some(Err(WaitError::SenderDropped));
            } else {
                return None;
            }
        }
    };
    let result = match check() {
        Some(result) => Some(result),
        None => {
            // 保存完成（value ready）可以在检查与登记（registration）间发生，登记真实Context后复查同一条件。
            shared.notify.register(context.waker());
            check()
        }
    };
    match result {
        Some(result) => {
            let shared = wait.shared.take().expect("接收责任（receiver responsibility）仍有效");
            shared.state.fetch_or(AnyShared::<T>::RECEIVER_CLOSED, std::sync::atomic::Ordering::AcqRel);
            Poll::Ready(result)
        }
        None => Poll::Pending,
    }
}

pub(super) fn any_send_inner<T>(shared: &AnyShared<T>, value: T) -> RawResult<(), SendError<T>> {
    let mut state = shared.state.load(std::sync::atomic::Ordering::Acquire);
    loop {
        let rejected = if state & AnyShared::<T>::VALUE_CLAIMED != 0 {
            Some(SendErrorKind::ValueAlreadyAccepted)
        } else if state & AnyShared::<T>::RECEIVER_CLOSED != 0 {
            Some(SendErrorKind::ReceiverClosed)
        } else {
            None
        };
        if let Some(kind) = rejected {
            // 本份Active责任（active responsibility）保证高位至少一份；减数不借位到低五位，也不发布无值事实。
            let previous = shared.state.fetch_sub(AnyShared::<T>::COUNT_ONE, std::sync::atomic::Ordering::AcqRel);
            assert_ne!(previous >> 5, 0, "有效发送责任（sender responsibility）不得下溢");
            return Err(SendError::new(kind, value));
        }
        match shared.state.compare_exchange(
            state, state | AnyShared::<T>::VALUE_CLAIMED,
            std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire,
        ) {
            Ok(_) => break,
            Err(actual) => state = actual,
        }
    }
    // SAFETY: 同字条件更新（CAS）授予唯一写入许可；READY发布前其它参与者不能读取，局部Arc仍保活。
    unsafe { (*shared.value.get()).write(value); }
    let previous = shared.state.fetch_or(AnyShared::<T>::VALUE_READY, std::sync::atomic::Ordering::AcqRel);
    let cleanup = if previous & AnyShared::<T>::RECEIVER_CLOSED != 0
        && shared.state.fetch_or(AnyShared::<T>::VALUE_TAKEN, std::sync::atomic::Ordering::AcqRel)
            & AnyShared::<T>::VALUE_TAKEN == 0
    {
        // SAFETY: 保存完成（VALUE_READY），本方唯一取得取走位（VALUE_TAKEN）；关闭者不能重复读。
        Some(unsafe { (*shared.value.get()).assume_init_read() })
    } else {
        None
    };
    // 原值（value）已保存或移入本方局部，先结束有效责任（active responsibility）再调用用户通知（notification）或析构（drop）。
    let previous = shared.state.fetch_sub(AnyShared::<T>::COUNT_ONE, std::sync::atomic::Ordering::AcqRel);
    assert_ne!(previous >> 5, 0, "有效发送责任（sender responsibility）不得下溢");
    shared.notify.wake();
    drop(cleanup);
    Ok(())
}

pub(super) fn any_clone_inner<T>(sender: &AnySender<T>) -> AnySender<T> {
    let shared = match &sender.state {
        AnySenderState::Rejected(kind) => return AnySender { state: AnySenderState::Rejected(*kind) },
        AnySenderState::Consumed => panic!("交值句柄（sender handle）已消耗"),
        AnySenderState::Active(shared) => shared,
    };
    let mut state = shared.state.load(std::sync::atomic::Ordering::Acquire);
    let mut prepared = None;
    loop {
        if state & AnyShared::<T>::VALUE_CLAIMED != 0 {
            return AnySender { state: AnySenderState::Rejected(SendErrorKind::ValueAlreadyAccepted) };
        }
        if state & AnyShared::<T>::RECEIVER_CLOSED != 0 {
            return AnySender { state: AnySenderState::Rejected(SendErrorKind::ReceiverClosed) };
        }
        assert_ne!(state >> 5, 0, "不能恢复零份有效责任（sender responsibility）");
        let next = AnyShared::<T>::checked_increment(state)
            .expect("有效交值数量（active sender count）越界");
        // 实际共享持有（Arc）先准备，逻辑计数（logical count）成功后直接交给Active，期间不执行用户代码。
        if prepared.is_none() { prepared = Some(Arc::clone(shared)); }
        match shared.state.compare_exchange(
            state, next, std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire,
        ) {
            Ok(_) => return AnySender { state: AnySenderState::Active(prepared.take().unwrap()) },
            Err(actual) => state = actual,
        }
    }
}

pub(super) fn any_close_inner<T>(shared: &AnyShared<T>) {
    let state = shared.state.fetch_or(AnyShared::<T>::RECEIVER_CLOSED, std::sync::atomic::Ordering::AcqRel);
    if state & (AnyShared::<T>::VALUE_READY | AnyShared::<T>::VALUE_TAKEN)
        == AnyShared::<T>::VALUE_READY
    {
        let previous = shared.state.fetch_or(AnyShared::<T>::VALUE_TAKEN, std::sync::atomic::Ordering::AcqRel);
        if previous & AnyShared::<T>::VALUE_TAKEN == 0 {
            // SAFETY: READY的获取（acquire）确认初始化，本方唯一取得TAKEN；原值（T）移到局部后才析构（drop）。
            let value = unsafe { (*shared.value.get()).assume_init_read() };
            drop(value);
        }
    }
}

pub(super) fn any_sender_drop_inner<T>(shared: &AnyShared<T>) {
    let mut state = shared.state.load(std::sync::atomic::Ordering::Acquire);
    let notify = loop {
        assert_ne!(state >> 5, 0, "有效发送责任（sender responsibility）不得下溢");
        let last_without_value = state >> 5 == 1 && state & AnyShared::<T>::VALUE_CLAIMED == 0;
        let next = (state - AnyShared::<T>::COUNT_ONE)
            | if last_without_value { AnyShared::<T>::NO_VALUE } else { 0 };
        match shared.state.compare_exchange(
            state, next, std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire,
        ) {
            Ok(_) => break last_without_value && state & AnyShared::<T>::RECEIVER_CLOSED == 0,
            Err(actual) => state = actual,
        }
    };
    // 最后无值事实（NO_VALUE）和减数在同一条件更新（CAS）成立，通知（notification）恐慌不重复退场。
    if notify { shared.notify.wake(); }
}

pub(super) fn any_checked_increment_inner(word: usize) -> Option<usize> {
    if word >> 5 == AnyShared::<()>::MAX_SENDERS {
        None
    } else {
        Some(word + AnyShared::<()>::COUNT_ONE)
    }
}

pub(super) fn any_shared_drop_inner<T>(shared: &mut AnyShared<T>) {
    let state = shared.state.get_mut();
    if *state & (AnyShared::<T>::VALUE_READY | AnyShared::<T>::VALUE_TAKEN)
        == AnyShared::<T>::VALUE_READY
    {
        *state |= AnyShared::<T>::VALUE_TAKEN;
        // SAFETY: 最后共享持有（Arc）独占记录，READY确认初始化；先TAKEN再移出原值（T），只清理一次。
        let value = unsafe { (*shared.value.get()).assume_init_read() };
        drop(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// R1。发送计数增加时逐一保留低五位，且最大计数时明确拒绝。
    #[test]
    fn test_wait_any_count_increment_preserves_every_low_bit_pattern() {
        let count_step = 1usize << 5;
        let count_limit = usize::MAX >> 5;

        for flags in 0..count_step {
            for count in [0, 1, count_limit - 1] {
                let original = (count << 5) | flags;
                let expected = ((count + 1) << 5) | flags;
                assert_eq!(any_checked_increment_inner(original), Some(expected));
            }
            assert_eq!(any_checked_increment_inner((count_limit << 5) | flags), None);
        }
    }
}
