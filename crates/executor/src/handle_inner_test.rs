//! 来源通知守卫（source notification guard）结束时的线程检查补偿。
use super::SourceNotifyGuard;
use crate::parking::ThreadParker;

/// 未结清的守卫在结束时经原通知器补一次线程检查；已结清则不再通知。
#[test]
fn test_source_notify_guard_drop_notifies_only_when_armed() {
    let mut parker = ThreadParker::new();
    assert!(parker.take_notification(), "清掉创建时的初始通知");
    let notifier = parker.notifier();
    let armed = SourceNotifyGuard { notifier: &notifier, armed: true };
    drop(armed);
    assert!(parker.is_notified(), "未结清守卫须补一次线程检查");
    assert!(parker.take_notification());
    let disarmed = SourceNotifyGuard { notifier: &notifier, armed: false };
    drop(disarmed);
    assert!(!parker.is_notified(), "已结清不得重复通知");
}
