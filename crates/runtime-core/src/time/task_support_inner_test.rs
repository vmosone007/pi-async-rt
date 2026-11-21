//! 定时派发的查询登记守卫（query registration guard），只经来源调用计数观察。
use super::QueryRegistrationGuard;
use crate::definitions::{RuntimeId, TimerError};
use crate::time::access::{Confirmation, TimerAccess, TimerCancelRequestOutcome};
use crate::time::entry::TimerEntry;
use crate::time::registration::{TimerRegistration, TimerRegistrationKind};
use pi_async_task_queue::timer::{TimerQueueError, TimerSubmitError};
use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};
use std::time::Duration;

/// 原键（key）只作身份关联，不要求克隆（Clone）。
struct GuardKey(usize);

/// 记录来源调用的计时访问（TimerAccess）；不登记、不取时。
#[derive(Clone)]
struct GuardAccess {
    id: RuntimeId,
    requested: Arc<AtomicUsize>,
    deferred: Arc<AtomicUsize>,
    answer: bool,
    calls: Arc<AtomicUsize>,
}
impl GuardAccess {
    fn count(&self) { self.calls.fetch_add(1, Ordering::Relaxed); }
}
impl TimerAccess for GuardAccess {
    type Instant = u64;
    type Key = GuardKey;
    type Registration = Arc<TimerRegistration<GuardKey>>;
    fn runtime_id(&self) -> RuntimeId { self.id }
    fn deadline_after(&self, _: Duration) -> pi_result::RawResult<Self::Instant, TimerError> {
        self.count();
        panic!("本组不得取时（must not read clock）")
    }
    fn prepare_registration(&self, _: TimerRegistrationKind) -> pi_result::RawResult<Self::Registration, TimerError> {
        self.count();
        panic!("本组不得准备登记（must not prepare registration）")
    }
    fn try_register(&self, _: Self::Instant, _: TimerEntry<Self::Key, Self::Registration>)
        -> pi_result::RawResult<Self::Key, TimerSubmitError<TimerEntry<Self::Key, Self::Registration>>> {
        self.count();
        panic!("本组不得登记（must not register）")
    }
    fn try_register_local(&self, _: Self::Instant, _: TimerEntry<Self::Key, Self::Registration>)
        -> pi_result::RawResult<Self::Key, TimerSubmitError<TimerEntry<Self::Key, Self::Registration>>> {
        self.count();
        panic!("本组不得本地登记（must not register locally）")
    }
    fn request_cancel(&self, key: &Self::Key) -> pi_result::RawResult<TimerCancelRequestOutcome, TimerQueueError> {
        assert_eq!(key.0, 4);
        self.requested.fetch_add(1, Ordering::Relaxed);
        if self.answer {
            Ok(TimerCancelRequestOutcome::Recorded)
        } else {
            Err(TimerQueueError::Busy)
        }
    }
    fn defer_cancel(&self, registration: Self::Registration, error: Option<TimerQueueError>, confirmation: Confirmation<'_, Self::Key>) {
        assert!(matches!(error, Some(TimerQueueError::Busy)));
        self.deferred.fetch_add(1, Ordering::Relaxed);
        let _ = confirmation.deferred(registration.as_ref());
    }
}

/// 构造已开始并已发布键（key）的等待登记（wait registration）。
fn prepared(accept: bool) -> (Arc<TimerRegistration<GuardKey>>, GuardAccess, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let registration = Arc::new(TimerRegistration::<GuardKey>::new(TimerRegistrationKind::Wait));
    assert!(registration.begin());
    assert!(!registration.publish_key(GuardKey(4)));
    let requested = Arc::new(AtomicUsize::new(0));
    let deferred = Arc::new(AtomicUsize::new(0));
    let access = GuardAccess {
        id: RuntimeId::try_new().expect("运行时身份（runtime id）"),
        requested: requested.clone(),
        deferred: deferred.clone(),
        answer: accept,
        calls: Arc::new(AtomicUsize::new(0)),
    };
    (registration, access, requested, deferred)
}

/// 未完整结束的查询登记守卫在结束时请求一次原登记取消；已撤不重复。
#[test]
fn test_query_registration_guard_drop_requests_cancel_once_when_armed() {
    let (registration, access, requested, deferred) = prepared(true);
    let guard = QueryRegistrationGuard { access: access.clone(), registration: registration.clone(), armed: true };
    drop(guard);
    assert_eq!(requested.load(Ordering::Relaxed), 1, "结束时请求一次原登记取消");
    assert_eq!(deferred.load(Ordering::Relaxed), 0);
    let disarmed = QueryRegistrationGuard { access: access.clone(), registration: registration.clone(), armed: false };
    drop(disarmed);
    assert_eq!(requested.load(Ordering::Relaxed), 1, "已撤销资格不得重复请求");
}

/// 来源拒绝时把原错误交预备接续，不由守卫冒充未接受。
#[test]
fn test_query_registration_guard_drop_hands_error_to_successor() {
    let (registration, access, requested, deferred) = prepared(false);
    let guard = QueryRegistrationGuard { access: access.clone(), registration: registration.clone(), armed: true };
    drop(guard);
    assert_eq!(requested.load(Ordering::Relaxed), 1);
    assert_eq!(deferred.load(Ordering::Relaxed), 1, "同一原错误交预备接续");
    assert!(registration.claim_forwarding(true).is_some(), "节点仍保真实待接续责任");
}
