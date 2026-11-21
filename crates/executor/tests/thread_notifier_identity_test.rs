//! 线程通知句柄（thread notification handle）的相等（equality）语义。
//! 只经公开 PartialEq 观察通知状态（notification state）身份，不读取、消费或重发通知（notification）。

use pi_async_executor::ThreadParker;

/// @api ThreadNotifier::eq；克隆（clone）共享同一份通知状态时相等，同一线程（thread）上的
/// 两个独立状态不等。比较本身不得读取、消费通知标记，也不得唤醒原线程（owner thread）。
#[test]
fn test_thread_notifier_equality_tracks_shared_state_identity_only() {
    let mut parker = ThreadParker::new();
    assert!(parker.take_notification(), "取得初始通知（initial notification）以便观察比较副作用");
    assert!(!parker.is_notified());
    let notifier = parker.notifier();
    let clone = notifier.clone();
    assert!(notifier.eq(&notifier), "同一句柄（handle）必须自反相等");
    assert!(notifier == clone, "同源克隆（shared clone）共享通知状态，必须相等");
    assert!(clone == notifier, "相等必须对称");
    assert!(!parker.is_notified(), "相等性比较（equality comparison）不得读取或消费通知标记");

    let other = ThreadParker::new();
    assert_eq!(other.owner_thread(), parker.owner_thread(), "两个拥有者（owner）位于同一线程（thread）");
    let other_notifier = other.notifier();
    assert!(notifier != other_notifier, "同一线程的不同通知状态不能相等");
    assert!(other_notifier != notifier, "同一线程的不同通知状态不能相等（反向）");
    assert!(other.is_notified(), "比较不得消费对方通知标记");
    assert!(!parker.is_notified(), "比较不得消费本方通知标记");
}
