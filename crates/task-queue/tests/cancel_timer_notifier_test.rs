//! 默认来源（default source）与原线程通知（owner-thread notification）的配对比较。
//! 只经公开 uses_notifier 观察构造时保存的通知器（notifier）身份，不通知、不取出、不推进。

#![cfg(feature = "cancel-timer")]

use pi_async_task_queue::notify::QueueNotifier;
use pi_async_task_queue::{CancelTimerOptions, CancelTimerQueue};
use pi_clock_manual::global::GlobalManualInstant;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// 以共享状态身份判断相等，模拟 ThreadNotifier 的语义：编号只用于诊断，通知计数不参与相等。
struct IdentityNotifier {
    label: usize,
    notifications: Arc<AtomicUsize>,
}

impl Clone for IdentityNotifier {
    fn clone(&self) -> Self {
        Self { label: self.label, notifications: Arc::clone(&self.notifications) }
    }
}

impl PartialEq for IdentityNotifier {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.notifications, &other.notifications)
    }
}

impl Eq for IdentityNotifier {}

impl QueueNotifier for IdentityNotifier {
    fn notify(&self) {
        self.notifications.fetch_add(1, Ordering::Relaxed);
    }
}

/// @api CancelTimerQueue::uses_notifier；只按构造时保存的通知器身份比较。
/// 同源克隆相等；同一编号（label）或相同通知计数（count）但不同状态不等；比较不通知。
#[test]
fn test_uses_notifier_compares_constructing_notifier_state_identity() {
    let notifications = Arc::new(AtomicUsize::new(0));
    let notifier = IdentityNotifier { label: 9, notifications: Arc::clone(&notifications) };
    let queue = CancelTimerQueue::<u32, _>::new(
        GlobalManualInstant::ZERO,
        notifier.clone(),
        CancelTimerOptions::default(),
    )
    .unwrap_or_else(|error| panic!("创建定时来源失败: {error:?}"));
    assert!(queue.uses_notifier(&notifier), "构造时保存的同一句柄（handle）必须相等");
    assert!(queue.uses_notifier(&notifier.clone()), "同源克隆（shared clone）必须相等");
    assert_eq!(notifications.load(Ordering::Relaxed), 0, "配对比较（pairing comparison）不得通知");

    let same_label = IdentityNotifier { label: notifier.label, notifications: Arc::new(AtomicUsize::new(0)) };
    assert!(!queue.uses_notifier(&same_label), "同一编号 label={} 不代表同一通知状态", same_label.label);
    assert_eq!(same_label.notifications.load(Ordering::Relaxed), 0);

    let same_count = IdentityNotifier { label: 3, notifications: Arc::new(AtomicUsize::new(0)) };
    assert!(!queue.uses_notifier(&same_count), "相同通知计数不代表同一通知状态");
    assert_eq!(same_count.notifications.load(Ordering::Relaxed), 0);
    assert_eq!(notifications.load(Ordering::Relaxed), 0, "全部分支都不得通知");
}
