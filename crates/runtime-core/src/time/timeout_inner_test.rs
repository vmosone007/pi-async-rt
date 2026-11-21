//! 取消转交（cancellation forwarding）的四模式入口，只经登记事实（registration facts）与
//! 适配者（adapter）调用计数观察，不登记来源、不取时、不创建任务（Task）。
use super::{request_registration_cancel, CancelMode};
use crate::definitions::{RuntimeId, TimerError};
use crate::time::access::TimerCancelRequestOutcome;
use crate::time::entry::TimerEntry;
use crate::time::registration::{RegistrationEnd, TimerRegistration, TimerRegistrationKind};
use pi_async_task_queue::timer::{TimerQueueError, TimerSubmitError};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, OnceLock,
};
use std::time::Duration;

/// 原键（key）；不要求Clone／Copy，只要求Send＋Sync＋'static。
struct TestKey(usize);

/// 适配者本次应答；失败统一用 Busy 作为原错误样例。
#[derive(Clone, Copy)]
enum Answer {
    Recorded,
    Close,
    Fail,
}

type RequestLog = Arc<OnceLock<usize>>;
type DeferLog = Arc<OnceLock<Option<&'static str>>>;

/// 记录调用次数与顺序的计时访问（TimerAccess）；不提供来源接收或取时能力。
#[derive(Clone)]
struct ProbeAccess {
    id: RuntimeId,
    requested: RequestLog,
    deferred: DeferLog,
    answer: Answer,
    started: Arc<AtomicUsize>,
}
impl ProbeAccess {
    fn new(answer: Answer) -> (Self, RequestLog, DeferLog) {
        let requested: RequestLog = Arc::new(OnceLock::new());
        let deferred: DeferLog = Arc::new(OnceLock::new());
        let access = Self {
            id: RuntimeId::try_new().expect("运行时身份（runtime id）分配"),
            requested: requested.clone(),
            deferred: deferred.clone(),
            answer,
            started: Arc::new(AtomicUsize::new(0)),
        };
        (access, requested, deferred)
    }
}
impl crate::time::access::TimerAccess for ProbeAccess {
    type Instant = u64;
    type Key = TestKey;
    type Registration = Arc<TimerRegistration<TestKey>>;

    fn runtime_id(&self) -> RuntimeId {
        self.id
    }
    fn deadline_after(&self, _: Duration) -> pi_result::RawResult<Self::Instant, TimerError> {
        self.started.fetch_add(1, Ordering::Relaxed);
        panic!("本组不得取时（must not read clock）")
    }
    fn prepare_registration(
        &self,
        _: TimerRegistrationKind,
    ) -> pi_result::RawResult<Self::Registration, TimerError> {
        self.started.fetch_add(1, Ordering::Relaxed);
        panic!("本组不得准备登记（must not prepare registration）")
    }
    fn try_register(
        &self,
        _: Self::Instant,
        _: TimerEntry<Self::Key, Self::Registration>,
    ) -> pi_result::RawResult<Self::Key, TimerSubmitError<TimerEntry<Self::Key, Self::Registration>>> {
        panic!("本组不得登记（must not register）")
    }
    fn try_register_local(
        &self,
        _: Self::Instant,
        _: TimerEntry<Self::Key, Self::Registration>,
    ) -> pi_result::RawResult<Self::Key, TimerSubmitError<TimerEntry<Self::Key, Self::Registration>>> {
        panic!("本组不得本地登记（must not register locally）")
    }
    fn request_cancel(
        &self,
        key: &Self::Key,
    ) -> pi_result::RawResult<TimerCancelRequestOutcome, TimerQueueError> {
        assert!(self.requested.set(key.0).is_ok(), "本次原取消只尝试一次");
        match self.answer {
            Answer::Recorded => Ok(TimerCancelRequestOutcome::Recorded),
            Answer::Close => Ok(TimerCancelRequestOutcome::HandledByClose),
            Answer::Fail => Err(TimerQueueError::Busy),
        }
    }
    fn defer_cancel(
        &self,
        registration: Self::Registration,
        error: Option<TimerQueueError>,
        confirmation: crate::time::access::Confirmation<'_, Self::Key>,
    ) {
        let label = match error {
            None => None,
            Some(TimerQueueError::Busy) => Some("busy"),
            Some(_) => Some("other"),
        };
        assert!(self.deferred.set(label).is_ok(), "本次原责任只转交一次");
        let _ = confirmation.deferred(registration.as_ref());
    }
}

/// 构造已开始并已发布键（key）的等待登记（wait registration）。
fn prepared(answer: Answer) -> (Arc<TimerRegistration<TestKey>>, ProbeAccess, RequestLog, DeferLog) {
    let registration = Arc::new(TimerRegistration::<TestKey>::new(TimerRegistrationKind::Wait));
    assert!(registration.begin());
    assert!(!registration.publish_key(TestKey(7)));
    let (access, requested, deferred) = ProbeAccess::new(answer);
    (registration, access, requested, deferred)
}

/// 接续模式（Resume）在尚无待接续责任时不得自行制造请求或触达来源。
#[test]
fn test_forwarding_resume_without_deferred_responsibility_does_not_touch_source() {
    let (registration, access, requested, deferred) = prepared(Answer::Recorded);
    let result = request_registration_cancel(&access, &registration, CancelMode::Resume);
    assert!(matches!(result, Ok(false)));
    assert!(requested.get().is_none(), "无待接续责任不得调用来源取消（cancel）");
    assert!(deferred.get().is_none());
    assert!(registration.claim_forwarding(false).is_none(), "登记事实保持不变");
}

/// Recorded与HandledByClose都保持原Ok(true)，且不再交预备接续。
#[test]
fn test_forwarding_keeps_original_ok_true_and_never_defers_on_success() {
    for answer in [Answer::Recorded, Answer::Close] {
        let (registration, access, requested, deferred) = prepared(answer);
        assert!(registration.request_wait_cancel());
        let result = request_registration_cancel(&access, &registration, CancelMode::WaitControl);
        assert!(matches!(result, Ok(true)), "合法请求的原Ok(true)必须保持");
        assert_eq!(requested.get().copied(), Some(7), "只尝试一次");
        assert!(deferred.get().is_none(), "成功不得转交预备接续");
        assert!(registration.claim_forwarding(true).is_none(), "转交已结清");
    }
}

/// WaitControl失败把原错误交调用者，节点只保责任（发布Deferred、错误不复制）。
#[test]
fn test_forwarding_wait_control_failure_returns_error_and_keeps_responsibility() {
    let (registration, access, requested, deferred) = prepared(Answer::Fail);
    assert!(registration.request_wait_cancel());
    let result = request_registration_cancel(&access, &registration, CancelMode::WaitControl);
    assert!(matches!(result, Err(TimerQueueError::Busy)), "原错误必须原样返回");
    assert_eq!(requested.get().copied(), Some(7));
    assert_eq!(deferred.get(), Some(&None), "只结清责任，不复制原错误");
    assert!(registration.claim_forwarding(true).is_some(), "节点仍保真实待接续责任");
}

/// Detached失败沿正常返回，并把同一原错误交守卫在结束（drop）时转交。
#[test]
fn test_forwarding_detached_failure_hands_same_error_to_successor() {
    let (registration, access, requested, deferred) = prepared(Answer::Fail);
    assert!(registration.request_cancel());
    let result = request_registration_cancel(&access, &registration, CancelMode::Detached);
    assert!(matches!(result, Ok(true)), "内部撤销沿正常返回");
    assert_eq!(requested.get().copied(), Some(7));
    assert_eq!(deferred.get(), Some(&Some("busy")), "同一原错误交预备接续");
    assert!(registration.claim_forwarding(true).is_some());
}

/// 键（key）未就绪与登记已结束都返回false，且都不调用来源。
#[test]
fn test_cancel_records_without_forwarding_until_key_and_rejects_after_finish() {
    let unfinished = TimerRegistration::<TestKey>::new(TimerRegistrationKind::Wait);
    assert!(unfinished.begin());
    assert!(unfinished.request_wait_cancel());
    let (access, requested, _deferred) = ProbeAccess::new(Answer::Recorded);
    let registration = Arc::new(unfinished);
    assert!(matches!(request_registration_cancel(&access, &registration, CancelMode::WaitControl), Ok(true)), "取消请求已合并，键未就绪由真实发布者接续");
    assert!(requested.get().is_none());

    let (finished, access, requested, deferred) = prepared(Answer::Recorded);
    assert!(finished.request_wait_cancel());
    finished.finish(RegistrationEnd::Removed);
    assert!(matches!(request_registration_cancel(&access, &finished, CancelMode::WaitControl), Ok(false)), "已结束登记不再取得资格");
    assert!(requested.get().is_none());
    assert!(deferred.get().is_none());
}

/// 公共接续入口（resume_timer_cancel）只对真实待接续责任调用一次来源取消。
#[test]
fn test_resume_timer_cancel_forwards_only_deferred_responsibility() {
    let (registration, access, requested, deferred) = prepared(Answer::Recorded);
    crate::time::timeout::resume_timer_cancel(&access, registration.clone());
    assert!(requested.get().is_none(), "无待接续责任不得调用来源取消（cancel）");
    assert!(registration.request_wait_cancel());
    let confirmation = registration.claim_forwarding(false).expect("首次正常调用取得确认");
    assert!(confirmation.deferred(&registration).is_ok());
    crate::time::timeout::resume_timer_cancel(&access, registration.clone());
    assert_eq!(requested.get().copied(), Some(7), "接续只尝试一次");
    assert!(deferred.get().is_none(), "成功不得再转交预备接续");
}

/// 登记守卫（RegistrationGuard）在未完整结束时撤销资格并请求一次取消；已撤则不重复。
#[test]
fn test_registration_guard_drop_requests_cancel_once_when_armed() {
    let (registration, access, requested, deferred) = prepared(Answer::Recorded);
    let guard = super::RegistrationGuard {
        access: access.clone(),
        registration: registration.clone(),
        armed: true,
    };
    drop(guard);
    assert_eq!(requested.get().copied(), Some(7), "结束时请求一次原登记取消");
    assert!(deferred.get().is_none());
    let disarmed = super::RegistrationGuard {
        access: access.clone(),
        registration: registration.clone(),
        armed: false,
    };
    drop(disarmed);
    assert_eq!(requested.get().copied(), Some(7), "已撤销资格不得重复请求");
}

/// 键发布前已记录取消时，forwarding 仍为 Idle；键发布后的 KeyPublished 必须接续转发一次，
/// 且来源失败时把原错误交预备接续（Deferred）。
#[test]
fn test_forwarding_key_published_forwards_prior_cancel_from_idle() {
    let registration = Arc::new(TimerRegistration::<TestKey>::new(TimerRegistrationKind::Task));
    let (access, requested, deferred) = ProbeAccess::new(Answer::Recorded);
    assert!(registration.begin());
    assert!(registration.request_cancel());
    // 键尚未就绪：此时 Detached 只能记录取消，不能取得转交资格，forwarding 仍为 Idle。
    assert!(matches!(
        request_registration_cancel(&access, &registration, CancelMode::Detached),
        Ok(false)
    ));
    assert!(requested.get().is_none());

    // 键发布返回已有取消，随后 KeyPublished 必须把键 9 转发给来源一次。
    assert!(registration.publish_key(TestKey(9)));
    assert!(matches!(
        request_registration_cancel(&access, &registration, CancelMode::KeyPublished),
        Ok(true)
    ));
    assert_eq!(requested.get().copied(), Some(9), "KeyPublished 必须接续转发已记录的取消");
    assert!(deferred.get().is_none());

    // 失败场景：KeyPublished 遇来源 Busy 必须把同一原错误交预备接续且沿 Ok(true) 返回。
    let reg_fail = Arc::new(TimerRegistration::<TestKey>::new(TimerRegistrationKind::Task));
    let (access_fail, req_fail, def_fail) = ProbeAccess::new(Answer::Fail);
    assert!(reg_fail.begin());
    assert!(reg_fail.request_cancel());
    assert!(reg_fail.publish_key(TestKey(13)));
    assert!(matches!(
        request_registration_cancel(&access_fail, &reg_fail, CancelMode::KeyPublished),
        Ok(true)
    ));
    assert_eq!(req_fail.get().copied(), Some(13));
    assert_eq!(def_fail.get(), Some(&Some("busy")));
    assert!(reg_fail.claim_forwarding(true).is_some(), "失败后须进入 Deferred 供后续接续");
}
