#[allow(unused_variables)]
fn registration_new_inner<K: Send + Sync + 'static>(
    kind: TimerRegistrationKind,
) -> TimerRegistration<K> {
    TimerRegistration {
        facts: AtomicU8::new(0),
        key: UnsafeCell::new(MaybeUninit::uninit()),
        completion: match kind {
            TimerRegistrationKind::Task => TimerCompletion::Task,
            TimerRegistrationKind::Wait => TimerCompletion::Wait {
                outcome: AtomicU8::new(WaitOutcome::Pending as u8),
                waker: AtomicWaker::new(),
            },
        },
        forwarding: AtomicU8::new(ForwardState::Idle as u8),
    }
}
#[allow(unused_variables)]
fn registration_cancel_inner<K: Send + Sync + 'static>(this: &TimerRegistration<K>) -> bool {
    use std::sync::atomic::Ordering::{AcqRel, Acquire};
    let mut previous = this.facts.load(Acquire);
    loop {
        // 等待兴趣结束后，内部析构（drop）仍负责撤销来源中的登记。
        if previous & TimerRegistration::<K>::RELEASED != 0 {
            return false;
        }
        if previous & TimerRegistration::<K>::CANCEL_REQUESTED != 0 {
            return true;
        }
        match this.facts.compare_exchange(
            previous,
            previous | TimerRegistration::<K>::CANCEL_REQUESTED,
            AcqRel,
            Acquire,
        ) {
            Ok(_) => return true,
            // 强比较交换（strong CAS）失败意味着至少新增了一项事实。
            Err(actual) => previous = actual,
        }
    }
}
#[allow(unused_variables)]
fn registration_wait_cancel_inner<K: Send + Sync + 'static>(this: &TimerRegistration<K>) -> bool {
    use std::sync::atomic::Ordering::{AcqRel, Acquire};
    let mut previous = this.facts.load(Acquire);
    loop {
        // 即使已有旧请求，用户控制也不能在兴趣或交付责任结束后报告仍可取消。
        if previous & (TimerRegistration::<K>::INTEREST_CLOSED | TimerRegistration::<K>::RELEASED) != 0 {
            return false;
        }
        if previous & TimerRegistration::<K>::CANCEL_REQUESTED != 0 {
            return true;
        }
        match this.facts.compare_exchange(
            previous,
            previous | TimerRegistration::<K>::CANCEL_REQUESTED,
            AcqRel,
            Acquire,
        ) {
            Ok(_) => return true,
            Err(actual) => previous = actual,
        }
    }
}
#[allow(unused_variables)]
fn registration_key_inner<K: Send + Sync + 'static>(this: &TimerRegistration<K>, key: K) -> bool {
    use std::sync::atomic::Ordering::{AcqRel, Release};
    let previous = this.facts.fetch_or(TimerRegistration::<K>::KEY_WRITER, AcqRel);
    assert!(previous & TimerRegistration::<K>::KEY_WRITER == 0,
        "同一计时登记（TimerRegistration）不能重复发布键（key）");
    // SAFETY: 唯一写入者（KEY_WRITER）拥有仍未初始化的槽；读者在就绪位发布前不借用槽。
    unsafe { this.key.get().write(MaybeUninit::new(key)); }
    // 真实交付可以早于键（key）发布。这里只报告已有取消请求（cancellation request）；
    // 是否仍能转交由转交入口判断，发布键（key）不撤销登记结束事实。
    let at_publication = this.facts.fetch_or(TimerRegistration::<K>::KEY_READY, Release);
    at_publication & TimerRegistration::<K>::CANCEL_REQUESTED != 0
}
#[allow(unused_variables)]
fn registration_key_ref_inner<K: Send + Sync + 'static>(this: &TimerRegistration<K>) -> Option<&K> {
    if this.facts.load(std::sync::atomic::Ordering::Acquire) & TimerRegistration::<K>::KEY_READY == 0 {
        return None;
    }
    // SAFETY: 获取（Acquire）读到就绪位，原键（key）已完整初始化；不再写入，登记借用防止释放。
    Some(unsafe { (*this.key.get()).assume_init_ref() })
}
#[allow(unused_variables)]
fn registration_begin_inner<K: Send + Sync + 'static>(this: &TimerRegistration<K>) -> bool {
    use std::sync::atomic::Ordering::{AcqRel, Acquire};
    let prevents_start = TimerRegistration::<K>::STARTED
        | TimerRegistration::<K>::CANCEL_REQUESTED
        | TimerRegistration::<K>::RELEASED;
    let mut previous = this.facts.load(Acquire);
    loop {
        // 先到的取消请求与开始交接竞争同一原子字；取消先成立就不接触来源。
        if previous & prevents_start != 0 {
            return false;
        }
        match this.facts.compare_exchange(
            previous,
            previous | TimerRegistration::<K>::STARTED,
            AcqRel,
            Acquire,
        ) {
            Ok(_) => return true,
            Err(actual) => previous = actual,
        }
    }
}
#[allow(unused_variables)]
fn registration_finish_inner<K: Send + Sync + 'static>(
    this: &TimerRegistration<K>,
    reason: RegistrationEnd,
) {
    use std::sync::atomic::Ordering::{AcqRel, Acquire, Release};
    let mut previous = this.facts.load(Acquire);
    loop {
        if previous & TimerRegistration::<K>::RELEASED != 0 {
            return;
        }
        match this.facts.compare_exchange(
            previous,
            previous | TimerRegistration::<K>::RELEASED,
            AcqRel,
            Acquire,
        ) {
            Ok(_) => break,
            Err(actual) => previous = actual,
        }
    }

    if let TimerCompletion::Wait { outcome, waker } = &this.completion {
        // 只有真实移除（removal）按结束时的取消事实分类；晚到请求不会改写结束原因。
        let result = match reason {
            RegistrationEnd::Elapsed => WaitOutcome::Elapsed,
            RegistrationEnd::Cancelled => WaitOutcome::Cancelled,
            RegistrationEnd::Removed if previous & TimerRegistration::<K>::CANCEL_REQUESTED != 0 => {
                WaitOutcome::Cancelled
            }
            RegistrationEnd::Removed => WaitOutcome::Removed,
            RegistrationEnd::DirectFailure => WaitOutcome::DirectFailure,
        };
        outcome.store(result as u8, Release);
        if let Some(registered) = waker.take() {
            // 先取出唤醒器（Waker），再确认兴趣；唤醒或释放均在槽保护结束后执行。
            if this.facts.load(Acquire) & TimerRegistration::<K>::INTEREST_CLOSED == 0 {
                registered.wake();
            }
        }
    }
}
#[allow(unused_variables)]
fn registration_interest_inner<K: Send + Sync + 'static>(this: &TimerRegistration<K>) {
    let interest = TimerRegistration::<K>::INTEREST_CLOSED;
    if this.facts.fetch_or(interest, std::sync::atomic::Ordering::AcqRel) & interest == 0 {
        if let TimerCompletion::Wait { waker, .. } = &this.completion {
            // 与结束方争取同一唤醒器（Waker）；取出后在原槽保护之外释放，不结束来源责任。
            drop(waker.take());
        }
    }
}
#[allow(unused_variables)]
fn registration_poll_inner<K: Send + Sync + 'static>(
    this: &TimerRegistration<K>,
    cx: &mut std::task::Context<'_>,
) -> std::task::Poll<RegistrationEnd> {
    use std::sync::atomic::Ordering::Acquire;
    use std::task::Poll;
    let TimerCompletion::Wait { outcome, waker } = &this.completion else {
        panic!("只允许轮询（poll）计时等待登记（TimerRegistrationKind::Wait）");
    };
    let mut stored = outcome.load(Acquire);
    if stored == WaitOutcome::Pending as u8 {
        waker.register(cx.waker());
        // 在首次读取和登记之间已经结束时，重查直接返回结果，不依赖额外唤醒（wake-up）。
        stored = outcome.load(Acquire);
        if stored != WaitOutcome::Pending as u8 {
            drop(waker.take());
        }
    }
    match stored {
        value if value == WaitOutcome::Pending as u8 => Poll::Pending,
        value if value == WaitOutcome::Elapsed as u8 => Poll::Ready(RegistrationEnd::Elapsed),
        value if value == WaitOutcome::Cancelled as u8 => Poll::Ready(RegistrationEnd::Cancelled),
        value if value == WaitOutcome::Removed as u8 => Poll::Ready(RegistrationEnd::Removed),
        value if value == WaitOutcome::DirectFailure as u8 => Poll::Ready(RegistrationEnd::DirectFailure),
        _ => unreachable!("未知的计时等待结果（WaitOutcome）"),
    }
}
fn registration_claim_inner<K: Send + Sync + 'static>(
    this: &TimerRegistration<K>,
    deferred: bool,
) -> bool {
    use std::sync::atomic::Ordering::{AcqRel, Acquire};
    // 只有已记录取消请求、键（key）已经就绪且尚未结束的登记才存在可转交的责任。
    let required = TimerRegistration::<K>::CANCEL_REQUESTED | TimerRegistration::<K>::KEY_READY;
    let facts = this.facts.load(Acquire);
    if facts & required != required || facts & TimerRegistration::<K>::RELEASED != 0 {
        return false;
    }
    // 正常调用只认Idle，真实接续只认Deferred；一次强比较交换（strong CAS）合并竞争者。
    let expected = if deferred {
        ForwardState::Deferred as u8
    } else {
        ForwardState::Idle as u8
    };
    this.forwarding
        .compare_exchange(expected, ForwardState::Publishing as u8, AcqRel, Acquire)
        .is_ok()
}
fn registration_defer_inner<K: Send + Sync + 'static>(this: &TimerRegistration<K>) {
    use std::sync::atomic::Ordering::Release;
    // 只消费一次性确认（Confirmation）代表的唯一Publishing责任：Release发布待接续（Deferred），
    // 不改键、不改只增事实。已结清时不做任何迁移，不能把Finished复活成可接续。
    let _ = this.forwarding.compare_exchange(
        ForwardState::Publishing as u8,
        ForwardState::Deferred as u8,
        Release,
        std::sync::atomic::Ordering::Acquire,
    );
}
fn registration_forwarded_inner<K: Send + Sync + 'static>(
    this: &TimerRegistration<K>,
    forwarded: bool,
) {
    use std::sync::atomic::Ordering::{AcqRel, Release};
    // 原请求实际交出才记录转交事实；来源已结束或合法关闭接管时只结清本份转交责任。
    if forwarded {
        this.facts
            .fetch_or(TimerRegistration::<K>::CANCEL_FORWARDED, AcqRel);
    }
    // 只从本份Publishing责任结清；重复结清不做额外迁移。
    let _ = this.forwarding.compare_exchange(
        ForwardState::Publishing as u8,
        ForwardState::Finished as u8,
        Release,
        std::sync::atomic::Ordering::Acquire,
    );
}
#[allow(unused_variables)]
fn registration_drop_inner<K: Send + Sync + 'static>(this: &mut TimerRegistration<K>) {
    if *this.facts.get_mut() & TimerRegistration::<K>::KEY_READY != 0 {
        // SAFETY: 最终独占释放（exclusive final drop）已结束全部键借用（key borrows）；就绪位（KEY_READY）只在原键（key）完整初始化后发布。
        unsafe { this.key.get_mut().assume_init_drop(); }
    }
}

#[cfg(test)]
mod registration_tests {
    use super::{RegistrationEnd, TimerRegistration, TimerRegistrationKind};
    use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};
    use std::task::{Context, Poll, Wake, Waker};

    /// 记录实际唤醒（wake-up）；本测试只使用等待登记（TimerRegistration），不创建计时来源。
    #[derive(Default)]
    struct WaitWake(AtomicUsize);

    impl Wake for WaitWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 经登记接口（API）核对开始前取消（cancellation）：请求可合并，先不结束等待，
    /// 登记未开始时在本地结束并唤醒一次；结束后两类取消均返回 false。
    #[test]
    fn test_registration_cancel_before_begin_finishes_locally() {
        let registration = TimerRegistration::<()>::new(TimerRegistrationKind::Wait);
        let wake = Arc::new(WaitWake::default());
        let waker = Waker::from(Arc::clone(&wake));
        let mut cx = Context::from_waker(&waker);

        assert!(registration.request_wait_cancel());
        assert!(registration.request_wait_cancel());
        assert!(!registration.begin());
        assert!(registration.poll(&mut cx).is_pending());
        assert_eq!(wake.0.load(Ordering::Relaxed), 0);

        registration.finish(RegistrationEnd::Cancelled);
        assert_eq!(wake.0.load(Ordering::Relaxed), 1);
        assert!(matches!(registration.poll(&mut cx), Poll::Ready(RegistrationEnd::Cancelled)));
        assert!(!registration.request_wait_cancel());
        assert!(!registration.request_cancel());
        registration.finish(RegistrationEnd::Cancelled);
        assert_eq!(wake.0.load(Ordering::Relaxed), 1);
    }

    /// 通过登记接口（API）核对只允许开始一次；一次直接失败（DirectFailure）结束责任，后来的取消（cancellation）不能改写它。
    #[test]
    fn test_registration_begin_is_unique_and_direct_failure_is_final() {
        let registration = TimerRegistration::<()>::new(TimerRegistrationKind::Wait);
        let wake = Arc::new(WaitWake::default());
        let waker = Waker::from(Arc::clone(&wake));
        let mut cx = Context::from_waker(&waker);

        assert!(registration.begin());
        assert!(!registration.begin());
        registration.finish(RegistrationEnd::DirectFailure);
        assert!(!registration.request_wait_cancel());
        assert!(!registration.request_cancel());
        assert!(!registration.begin());
        assert!(matches!(registration.poll(&mut cx), Poll::Ready(RegistrationEnd::DirectFailure)));
        assert_eq!(wake.0.load(Ordering::Relaxed), 0);
    }

    /// 经登记接口（API）核对移除原因：仅真实移除（removal）结合此前请求才得 Cancelled；
    /// 即使取消请求（cancellation request）已记录，真实到期（elapsed）也保持原结果。
    #[test]
    fn test_registration_preserves_terminal_reason_after_start() {
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);

        let removed = TimerRegistration::<()>::new(TimerRegistrationKind::Wait);
        assert!(removed.begin());
        removed.finish(RegistrationEnd::Removed);
        assert!(!removed.request_cancel());
        assert!(matches!(removed.poll(&mut cx), Poll::Ready(RegistrationEnd::Removed)));

        let cancelled = TimerRegistration::<()>::new(TimerRegistrationKind::Wait);
        assert!(cancelled.begin());
        assert!(cancelled.request_wait_cancel());
        assert!(cancelled.poll(&mut cx).is_pending());
        cancelled.finish(RegistrationEnd::Removed);
        assert!(matches!(cancelled.poll(&mut cx), Poll::Ready(RegistrationEnd::Cancelled)));

        let elapsed = TimerRegistration::<()>::new(TimerRegistrationKind::Wait);
        assert!(elapsed.begin());
        assert!(elapsed.request_wait_cancel());
        elapsed.finish(RegistrationEnd::Elapsed);
        assert!(matches!(elapsed.poll(&mut cx), Poll::Ready(RegistrationEnd::Elapsed)));
    }

    /// 只读键借用（key borrow）须等发布（publication）；迟到的键报告已有取消，原键和拒绝键各释放一次。
    #[test]
    fn test_registration_publishes_one_key_and_reports_prior_cancel() {
        /// 不可克隆（non-Clone）的原键（key），用不同计数区分接受与未接受的原值。
        struct Key(Arc<AtomicUsize>);

        impl Drop for Key {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let registration = Arc::new(TimerRegistration::new(TimerRegistrationKind::Wait));
        let accepted = Arc::new(AtomicUsize::new(0));
        let duplicate = Arc::new(AtomicUsize::new(0));
        assert!(registration.begin());
        assert!(registration.key().is_none());
        assert!(registration.request_wait_cancel());
        assert!(registration.key().is_none());
        assert!(registration.publish_key(Key(Arc::clone(&accepted))));
        assert!(Arc::ptr_eq(&registration.key().unwrap().0, &accepted));

        let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            registration.publish_key(Key(Arc::clone(&duplicate)))
        }));
        assert!(rejected.is_err());
        assert_eq!(duplicate.load(Ordering::Relaxed), 1);
        assert!(Arc::ptr_eq(&registration.key().unwrap().0, &accepted));
        assert_eq!(accepted.load(Ordering::Relaxed), 0);

        registration.finish(RegistrationEnd::Removed);
        let reader = Arc::clone(&registration);
        drop(registration);
        std::thread::spawn(move || {
            assert_eq!(reader.key().unwrap().0.load(Ordering::Relaxed), 0);
            drop(reader);
        }).join().unwrap();
        assert_eq!(accepted.load(Ordering::Relaxed), 1);
    }

    /// 键发布（key publication）只报告已有取消（cancellation）；即便来源先交付，迟到键也不会恢复登记。
    #[test]
    fn test_registration_late_key_preserves_completed_delivery() {
        let uncancelled = TimerRegistration::<usize>::new(TimerRegistrationKind::Wait);
        assert!(uncancelled.begin());
        assert!(!uncancelled.publish_key(7));
        uncancelled.finish(RegistrationEnd::Removed);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(uncancelled.poll(&mut cx), Poll::Ready(RegistrationEnd::Removed)));

        let delivered = TimerRegistration::<usize>::new(TimerRegistrationKind::Wait);
        assert!(delivered.begin());
        assert!(delivered.request_wait_cancel());
        delivered.finish(RegistrationEnd::Elapsed);
        assert!(delivered.publish_key(11));
        assert_eq!(delivered.key(), Some(&11));
        assert!(!delivered.request_cancel());
        assert!(!delivered.begin());
        assert!(matches!(delivered.poll(&mut cx), Poll::Ready(RegistrationEnd::Elapsed)));
    }

    /// 核对等待兴趣关闭（close_interest）归还唤醒器（Waker）：用户控制随后失效，
    /// 内部撤销仍能合并，且不产生源接收、计时或唤醒（wake-up）。
    #[test]
    fn test_registration_close_interest_keeps_internal_cancel_available() {
        let registration = TimerRegistration::<()>::new(TimerRegistrationKind::Wait);
        let wake = Arc::new(WaitWake::default());
        let lifetime = Arc::downgrade(&wake);
        {
            let waker = Waker::from(Arc::clone(&wake));
            assert!(registration.poll(&mut Context::from_waker(&waker)).is_pending());
        }
        assert!(Arc::strong_count(&wake) > 1);

        registration.close_interest();
        assert!(!registration.request_wait_cancel());
        assert!(registration.request_cancel());
        assert!(registration.request_cancel());
        assert!(!registration.begin());
        registration.finish(RegistrationEnd::Cancelled);
        assert_eq!(wake.0.load(Ordering::Relaxed), 0);
        drop(wake);
        assert!(lifetime.upgrade().is_none());
    }

    /// 经登记接口（registration API）核对取消转交（cancellation forwarding）资格：
    /// 只有已请求取消、键（key）已就绪且未结束的登记能取得一次Publishing；待接续（Deferred）
    /// 只对真实接续者开放一次，结清后不再产生新资格，也不改变只增事实。
    #[test]
    fn test_registration_forwarding_claims_once_and_requires_requested_ready_key() {
        let registration = TimerRegistration::<usize>::new(TimerRegistrationKind::Wait);
        assert!(registration.begin());
        assert!(!registration.publish_key(5), "发布键（key）时尚未请求取消");
        assert!(registration.claim_forwarding(false).is_none(),
            "未请求取消时不得取得转交资格");
        assert!(registration.request_wait_cancel());
        let confirmation = match registration.claim_forwarding(false) {
            Some(value) => value,
            None => panic!("首次正常调用必须取得一次性确认"),
        };
        assert!(confirmation.is_for(&registration));
        assert!(registration.claim_forwarding(false).is_none(),
            "Publishing期间竞争者只能合并");
        assert!(registration.claim_forwarding(true).is_none(),
            "未发布待接续时接续调用不得取得资格");
        assert!(confirmation.deferred(&registration).is_ok());
        assert!(registration.claim_forwarding(false).is_none(),
            "待接续状态不再接受正常调用");
        let resumed = match registration.claim_forwarding(true) {
            Some(value) => value,
            None => panic!("待接续只对真实接续者开放一次"),
        };
        assert!(registration.claim_forwarding(true).is_none(),
            "同一待接续不得被两个人同时取得");
        resumed.forwarded();
        assert!(registration.claim_forwarding(false).is_none());
        assert!(registration.claim_forwarding(true).is_none());
    }

    /// 不同登记不得消费待接续：确认原样返还，原登记仍保持待接续可被真实接续者取得。
    #[test]
    fn test_registration_deferred_confirmation_rejects_other_registration() {
        let registration = TimerRegistration::<usize>::new(TimerRegistrationKind::Wait);
        let other = TimerRegistration::<usize>::new(TimerRegistrationKind::Wait);
        assert!(registration.begin());
        assert!(!registration.publish_key(9));
        assert!(registration.request_wait_cancel());
        let confirmation = match registration.claim_forwarding(false) {
            Some(value) => value,
            None => panic!("正常调用必须取得一次性确认"),
        };
        let returned = match confirmation.deferred(&other) {
            Ok(()) => panic!("不同登记不能确认待接续"),
            Err(same) => same,
        };
        assert!(registration.claim_forwarding(true).is_none(),
            "错误登记不能消费原登记的待接续");
        assert!(returned.deferred(&registration).is_ok());
        assert!(registration.claim_forwarding(true).is_some());
    }

    /// 已结束（released）登记不得恢复转交资格，结束原因由原登记事实负责。
    #[test]
    fn test_registration_forwarding_rejects_finished_source() {
        let registration = TimerRegistration::<usize>::new(TimerRegistrationKind::Wait);
        assert!(registration.begin());
        assert!(!registration.publish_key(3));
        assert!(registration.request_wait_cancel());
        registration.finish(RegistrationEnd::Removed);
        assert!(registration.claim_forwarding(false).is_none());
        assert!(registration.claim_forwarding(true).is_none());
    }

    /// 原来源返回后，释放自定义登记持有时的首个恐慌（panic）不能打断成功确认。
    #[test]
    fn test_cancel_confirmation_precedes_registration_drop() {
        use super::ForwardState;
        use crate::definitions::{RuntimeId, TimerError};
        use crate::time::access::{Confirmation, TimerAccess, TimerCancelRequestOutcome};
        use crate::time::entry::TimerEntry;
        use pi_async_task_queue::timer::{TimerQueueError, TimerSubmitError};
        use std::time::Duration;

        struct Registration(Arc<TimerRegistration<usize>>, bool);

        impl AsRef<TimerRegistration<usize>> for Registration {
            fn as_ref(&self) -> &TimerRegistration<usize> {
                &self.0
            }
        }

        impl Clone for Registration {
            fn clone(&self) -> Self {
                Self(self.0.clone(), true)
            }
        }

        impl Drop for Registration {
            fn drop(&mut self) {
                if self.1 {
                    std::panic::panic_any(114u32);
                }
            }
        }

        #[derive(Clone)]
        struct Access(RuntimeId, TimerCancelRequestOutcome);

        type Entry = TimerEntry<usize, Registration>;
        type Submit = pi_result::RawResult<usize, TimerSubmitError<Entry>>;

        impl TimerAccess for Access {
            type Instant = u64;
            type Key = usize;
            type Registration = Registration;

            fn runtime_id(&self) -> RuntimeId {
                self.0
            }
            fn deadline_after(&self, _: Duration) -> pi_result::RawResult<u64, TimerError> {
                unreachable!("原登记已经开始")
            }
            fn prepare_registration(
                &self, _: TimerRegistrationKind,
            ) -> pi_result::RawResult<Registration, TimerError> {
                unreachable!("原登记已经准备")
            }
            fn try_register(&self, _: u64, _: Entry) -> Submit {
                unreachable!("原登记已经接收")
            }
            fn try_register_local(&self, _: u64, _: Entry) -> Submit {
                unreachable!("原登记已经接收")
            }
            fn request_cancel(
                &self, key: &usize,
            ) -> pi_result::RawResult<TimerCancelRequestOutcome, TimerQueueError> {
                assert_eq!(*key, 7);
                Ok(self.1)
            }
            fn defer_cancel(
                &self, _: Registration, _: Option<TimerQueueError>, _: Confirmation<'_, usize>,
            ) {
                unreachable!("真实成功无需重新发布")
            }
        }

        for outcome in [TimerCancelRequestOutcome::Recorded, TimerCancelRequestOutcome::HandledByClose] {
            let record = Arc::new(TimerRegistration::new(TimerRegistrationKind::Wait));
            assert!(record.begin());
            assert!(!record.publish_key(7));
            assert!(record.request_wait_cancel());
            let confirmation = record.claim_forwarding(false).expect("须有首个转交者");
            assert!(confirmation.deferred(&record).is_ok());
            let access = Access(RuntimeId::try_new().unwrap(), outcome);
            let registration = Registration(record.clone(), false);
            let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                crate::time::timeout::resume_timer_cancel(&access, registration);
            })).expect_err("必须传播原恐慌");
            assert_eq!(failure.downcast_ref::<u32>(), Some(&114));
            assert_eq!(record.forwarding.load(Ordering::Acquire), ForwardState::Finished as u8);
            assert_eq!(
                record.facts.load(Ordering::Acquire) & TimerRegistration::<usize>::CANCEL_FORWARDED != 0,
                outcome == TimerCancelRequestOutcome::Recorded,
            );
        }
    }
}
