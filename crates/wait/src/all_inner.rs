// 全部等待的局部实现入口；组、结果槽和余链守卫的真实定义位于父模块。

use super::*;

pub(super) fn all_create_inner<T>() -> (WaitAll<T>, AllSender<T>) {
    let slot = Arc::new(AllSlot::new());
    let group = Arc::new(AllShared {
        state: AtomicUsize::new(1),
        registered: AtomicUsize::new(1),
        head: AtomicPtr::new(std::ptr::null_mut()),
        notify: AtomicWaker::new(),
        owns_slots: PhantomData,
    });
    // 两次共享分配（Arc allocations）先齐备；交出列表强引用（list strong reference）后立即由组（group）承担。
    let head = Arc::into_raw(Arc::clone(&slot)).cast_mut();
    group.head.store(head, std::sync::atomic::Ordering::Release);
    (WaitAll { shared: Some(Arc::clone(&group)) }, AllSender { state: AllSenderState::Active { group, slot } })
}

pub(super) fn all_poll_inner<T>(wait: &mut WaitAll<T>, context: &mut Context<'_>) -> Poll<Vec<RawResult<T, WaitError>>> {
    let shared = wait.shared.as_ref().expect("等待（wait）已完成，不能重复轮询（poll）");
    let mut state = shared.state.load(std::sync::atomic::Ordering::Acquire);
    if state != 0 {
        assert_eq!(state & AllShared::<T>::RECEIVER_CLOSED, 0, "等待（wait）仍持有开放接收责任（open receiver responsibility）");
        shared.notify.register(context.waker());
        // 完成（completion）可与登记（registration）交错，登记真实Context后必须复查（recheck）。
        state = shared.state.load(std::sync::atomic::Ordering::Acquire);
        if state != 0 {
            assert_eq!(state & AllShared::<T>::RECEIVER_CLOSED, 0, "接收责任（receiver responsibility）仍开放");
            return Poll::Pending;
        }
    }
    let shared = wait.shared.take().expect("接收责任（receiver responsibility）仍有效");
    let mut guard = shared.detach();
    let registered = shared.registered.load(std::sync::atomic::Ordering::Acquire);
    let mut results = Vec::with_capacity(registered);
    while let Some(slot) = guard.take_next() {
        results.push(slot.take_ready().expect("全部有效份（active senders）结束后每个登记位置（registered slot）均须有结果（result）"));
    }
    assert_eq!(results.len(), registered, "结果数量（result count）必须匹配有效登记（registration）");
    results.reverse();
    Poll::Ready(results)
}

pub(super) fn all_send_inner<T>(group: &AllShared<T>, slot: &AllSlot<T>, value: T) -> RawResult<(), SendError<T>> {
    assert!(slot.state.compare_exchange(
        0, AllSlot::<T>::WRITING,
        std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire,
    ).is_ok(), "本份槽（slot）只能交值（send）一次");
    let mut state = group.state.load(std::sync::atomic::Ordering::Acquire);
    loop {
        if state & AllShared::<T>::RECEIVER_CLOSED != 0 {
            group.retire();
            return Err(SendError::new(SendErrorKind::ReceiverClosed, value));
        }
        // 同字条件更新（CAS）确认开放许可（open permission），旧读取（load）不能单独授权保存原值（value）。
        match group.state.compare_exchange(
            state, state,
            std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire,
        ) {
            Ok(_) => break,
            Err(actual) => state = actual,
        }
    }
    // SAFETY: 本方独占写入许可（write permission），局部Arc保活；就绪发布（ready publication）前无人读取原值（value）。
    unsafe { (*slot.value.get()).write(value); }
    slot.state.fetch_or(AllSlot::<T>::VALUE_READY, std::sync::atomic::Ordering::Release);
    // 同字读改写（RMW）与关闭（close）双向排序；必要取走（take）必须先于本份减数（decrement）。
    let previous = group.state.fetch_or(0, std::sync::atomic::Ordering::AcqRel);
    let cleanup = if previous & AllShared::<T>::RECEIVER_CLOSED != 0 {
        slot.take_ready()
    } else {
        None
    };
    group.retire();
    drop(cleanup);
    Ok(())
}

pub(super) fn all_clone_inner<T>(sender: &AllSender<T>) -> AllSender<T> {
    let group = match &sender.state {
        AllSenderState::Rejected(kind) => return AllSender { state: AllSenderState::Rejected(*kind) },
        AllSenderState::Consumed => panic!("交值句柄（sender handle）已经消耗"),
        AllSenderState::Active { group, .. } => group,
    };
    let mut state = group.state.load(std::sync::atomic::Ordering::Acquire);
    if state & AllShared::<T>::RECEIVER_CLOSED != 0 {
        return AllSender { state: AllSenderState::Rejected(SendErrorKind::ReceiverClosed) };
    }
    let slot = Arc::new(AllSlot::new());
    let new_group = Arc::clone(group);
    loop {
        if state & AllShared::<T>::RECEIVER_CLOSED != 0 {
            return AllSender { state: AllSenderState::Rejected(SendErrorKind::ReceiverClosed) };
        }
        let active = state & AllShared::<T>::MAX_ACTIVE;
        assert_ne!(active, 0, "借用的有效份（active sender）尚未结束");
        assert!(active < AllShared::<T>::MAX_ACTIVE, "有效交值数量（active sender count）越界");
        match group.state.compare_exchange(
            state, state + 1,
            std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire,
        ) {
            Ok(_) => break,
            Err(actual) => state = actual,
        }
    }
    let mut registered = group.registered.load(std::sync::atomic::Ordering::Acquire);
    loop {
        let Some(next) = AllShared::<T>::checked_registered_increment(registered) else {
            // 新位置（new slot）尚未发布；只撤销本次有效份预留（active reservation），原份（original sender）仍有效。
            group.retire();
            panic!("累计结果容量（result capacity）越界");
        };
        match group.registered.compare_exchange(
            registered, next,
            std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire,
        ) {
            Ok(_) => break,
            Err(actual) => registered = actual,
        }
    }
    let raw = Arc::into_raw(Arc::clone(&slot)).cast_mut();
    let marker = AllShared::<T>::closed_marker();
    let mut head = group.head.load(std::sync::atomic::Ordering::Acquire);
    loop {
        if head == marker {
            // SAFETY: 条件发布（CAS publication）从未成功，raw仍是本次独占的未交出列表强引用（list strong reference）。
            drop(unsafe { Arc::from_raw(raw) });
            group.registered.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            group.retire();
            return AllSender { state: AllSenderState::Rejected(SendErrorKind::ReceiverClosed) };
        }
        // SAFETY: 本槽（slot）尚未发布，next只由本调用（call）写入；失败时无人通过列表（list）读取它。
        unsafe { *slot.next.get() = head; }
        match group.head.compare_exchange(
            head, raw,
            std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire,
        ) {
            Ok(_) => return AllSender { state: AllSenderState::Active { group: new_group, slot } },
            Err(actual) => head = actual,
        }
    }
}

pub(super) fn all_close_inner<T>(group: &AllShared<T>) {
    let mut guard = group.detach();
    guard.drain();
}

pub(super) fn all_sender_drop_inner<T>(group: &AllShared<T>, slot: &AllSlot<T>) {
    // 本份包装（wrapper）已移出责任（responsibility）；先发布无值结果（no-value result），再结束计数（count）。
    slot.state.fetch_or(AllSlot::<T>::NO_VALUE, std::sync::atomic::Ordering::Release);
    group.retire();
}

pub(super) fn all_max_results_inner<T>() -> usize {
    AllShared::<T>::MAX_ACTIVE.min(
        isize::MAX as usize / mem::size_of::<RawResult<T, WaitError>>().max(1),
    )
}

pub(super) fn all_checked_registered_increment_inner<T>(registered: usize) -> Option<usize> {
    if registered >= AllShared::<T>::max_results() {
        None
    } else {
        Some(registered + 1)
    }
}

pub(super) fn all_closed_marker_inner<T>() -> *mut AllSlot<T> {
    std::ptr::from_ref(&CLOSED_LIST_MARKER).cast_mut().cast::<AllSlot<T>>()
}

pub(super) fn all_detach_inner<T>(group: &AllShared<T>) -> AllDrainGuard<T> {
    group.state.fetch_or(AllShared::<T>::RECEIVER_CLOSED, std::sync::atomic::Ordering::AcqRel);
    let marker = AllShared::<T>::closed_marker();
    let head = group.head.swap(marker, std::sync::atomic::Ordering::AcqRel);
    AllDrainGuard { next: if head == marker { std::ptr::null_mut() } else { head } }
}

pub(super) fn all_retire_inner<T>(group: &AllShared<T>) {
    let mut state = group.state.load(std::sync::atomic::Ordering::Acquire);
    loop {
        assert_ne!(state & AllShared::<T>::MAX_ACTIVE, 0, "有效责任（active responsibility）不得重复结束");
        match group.state.compare_exchange(
            state, state - 1,
            std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire,
        ) {
            Ok(_) => {
                // 减数（decrement）先完成；只有开放时最后一份结束才通知（notify），回调（callback）异常不得重做退出。
                if state == 1 {
                    group.notify.wake();
                }
                return;
            }
            Err(actual) => state = actual,
        }
    }
}

pub(super) fn all_shared_drop_inner<T>(group: &mut AllShared<T>) {
    let mut guard = group.detach();
    guard.drain();
}

pub(super) fn all_slot_new_inner<T>() -> AllSlot<T> {
    AllSlot {
        next: UnsafeCell::new(std::ptr::null_mut()),
        state: AtomicUsize::new(0),
        value: UnsafeCell::new(MaybeUninit::uninit()),
    }
}

pub(super) fn all_slot_take_ready_inner<T>(slot: &AllSlot<T>) -> Option<RawResult<T, WaitError>> {
    let mut state = slot.state.load(std::sync::atomic::Ordering::Acquire);
    loop {
        if state & AllSlot::<T>::VALUE_TAKEN != 0
            || state & (AllSlot::<T>::VALUE_READY | AllSlot::<T>::NO_VALUE) == 0
        {
            return None;
        }
        match slot.state.compare_exchange(
            state, state | AllSlot::<T>::VALUE_TAKEN,
            std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire,
        ) {
            Ok(_) => {
                if state & AllSlot::<T>::VALUE_READY != 0 {
                    // SAFETY: 获取读取（acquire read）看到原值（value）发布，且本方唯一取得消费许可（consume permission）。
                    return Some(Ok(unsafe { (*slot.value.get()).assume_init_read() }));
                }
                return Some(Err(WaitError::SenderDropped));
            }
            Err(actual) => state = actual,
        }
    }
}

pub(super) fn all_slot_drop_inner<T>(slot: &mut AllSlot<T>) {
    let state = slot.state.get_mut();
    if *state & AllSlot::<T>::VALUE_READY != 0 && *state & AllSlot::<T>::VALUE_TAKEN == 0 {
        *state |= AllSlot::<T>::VALUE_TAKEN;
        // SAFETY: 最后共享持有（Arc ownership）提供独占访问（exclusive access）；原值（value）已初始化且先标记取走。
        let value = unsafe { slot.value.get_mut().assume_init_read() };
        drop(value);
    }
}

pub(super) fn all_drain_take_next_inner<T>(guard: &mut AllDrainGuard<T>) -> Option<Arc<AllSlot<T>>> {
    if guard.next.is_null() {
        return None;
    }
    // SAFETY: 当前真实指针（pointer）代表本守卫（guard）独占的一份列表强引用（list strong reference），仅恢复一次。
    let slot = unsafe { Arc::from_raw(guard.next) };
    // SAFETY: 列表发布（list publication）后连接（link）不再修改；当前Arc保持槽（slot）及字段（field）可读。
    guard.next = unsafe { *slot.next.get() };
    Some(slot)
}

pub(super) fn all_drain_inner<T>(guard: &mut AllDrainGuard<T>) {
    while let Some(slot) = guard.take_next() {
        if let Some(Ok(value)) = slot.take_ready() {
            // 原值（value）已唯一取走、余链（remaining list）已推进；用户析构（drop）异常由外层仍存活的守卫（guard）接续。
            drop(value);
        }
    }
}

pub(super) fn all_drain_drop_inner<T>(guard: &mut AllDrainGuard<T>) {
    if guard.next.is_null() {
        return;
    }
    // 当前析构（drop）不会重入自身；先交出余链（remaining list），后继守卫（successor guard）在首次异常展开（unwinding）时仍存活。
    let mut successor = AllDrainGuard {
        next: mem::replace(&mut guard.next, std::ptr::null_mut()),
    };
    successor.drain();
}
