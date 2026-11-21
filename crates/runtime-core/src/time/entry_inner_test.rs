//! 定时入口（TimerEntry）的交付、恢复与结束分类，只经登记结果观察。
//! 任务（Task）型入口需要可构造的原Runnable，本组不制造它，避免伪证；信号型覆盖同一状态机。
use super::TimerEntry;
use crate::time::registration::{RegistrationEnd, TimerRegistration, TimerRegistrationKind};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

/// 已开始并已发布键（key）的等待登记（wait registration）。
fn prepared() -> Arc<TimerRegistration<()>> {
    let registration = Arc::new(TimerRegistration::<()>::new(TimerRegistrationKind::Wait));
    assert!(registration.begin());
    assert!(!registration.publish_key(()));
    registration
}

fn poll_now(registration: &TimerRegistration<()>) -> Poll<RegistrationEnd> {
    registration.poll(&mut Context::from_waker(Waker::noop()))
}

/// 到期交付：信号不交出业务原值，但把原等待完成为到期（Elapsed）。
#[test]
fn test_signal_entry_delivery_finishes_wait_as_elapsed() {
    let registration = prepared();
    let entry = TimerEntry::signal(registration.clone());
    assert!(entry.into_due().is_none(), "信号不交出业务原值");
    assert!(matches!(poll_now(&registration), Poll::Ready(RegistrationEnd::Elapsed)));
    registration.finish(RegistrationEnd::Elapsed);
    assert!(matches!(poll_now(&registration), Poll::Ready(RegistrationEnd::Elapsed)), "重复结束不改原结果");
}

/// 未交付即结束：按真实移除定结果一次，后续重复结束不改写。
#[test]
fn test_undelivered_entry_reports_removed_once() {
    let registration = prepared();
    drop(TimerEntry::signal(registration.clone()));
    assert!(matches!(poll_now(&registration), Poll::Ready(RegistrationEnd::Removed)));
    registration.finish(RegistrationEnd::Removed);
    assert!(matches!(poll_now(&registration), Poll::Ready(RegistrationEnd::Removed)));
}

/// 此前已有取消请求时，移除按原取消事实分类为Cancelled，而不是到期。
#[test]
fn test_undelivered_entry_with_recorded_cancel_reports_cancelled() {
    let registration = prepared();
    assert!(registration.request_wait_cancel());
    drop(TimerEntry::signal(registration.clone()));
    assert!(matches!(poll_now(&registration), Poll::Ready(RegistrationEnd::Cancelled)));
}

/// 恢复路径只撤销本份结束责任，不结束登记、不误发移除或取消。
#[test]
fn test_recovered_entry_keeps_registration_unsettled() {
    let registration = prepared();
    assert!(TimerEntry::signal(registration.clone()).recover().is_none(), "信号不恢复业务原值");
    assert!(poll_now(&registration).is_pending(), "恢复不结束登记");
    assert!(registration.claim_forwarding(false).is_none(), "未请求取消时不存在转交资格");
}
