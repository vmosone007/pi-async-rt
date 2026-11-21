//! 计时登记（timer registration）的公开构造（construction）与最终释放（final drop），不代补来源登记（source registration）或取消转交（cancellation forwarding）。
use pi_async_runtime_core::time::{TimerRegistration, TimerRegistrationKind};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};

/// 无克隆（Clone）能力的键（key）；未发布键（unpublished key）时，构造（construction）和释放（drop）都不得产生或析构（drop）此值。
struct UnpublishedKey { drops: Arc<AtomicUsize>, _pinned: std::marker::PhantomPinned }
impl Drop for UnpublishedKey {
    fn drop(&mut self) { self.drops.fetch_add(1, Ordering::Relaxed); }
}

/// 验证 TimerRegistration::new 对任务（Task）和等待（Wait）两种用途均可构造（construct）、共享（share）并跨线程最终释放（drop）。
/// 未初始化键槽（uninitialized key slot）不得被读取或析构（drop）；本测试不声明已验证键发布（key publication）或完成通知（completion notification）。
#[test]
fn test_registration_construction_and_unpublished_drop_need_no_key_value() {
    fn assert_shared<T: Send + Sync>() {}
    assert_shared::<TimerRegistration<UnpublishedKey>>();
    let drops = Arc::new(AtomicUsize::new(0));
    let unused = UnpublishedKey { drops: drops.clone(), _pinned: std::marker::PhantomPinned };
    for kind in [TimerRegistrationKind::Task, TimerRegistrationKind::Wait] {
        let registration = Arc::new(TimerRegistration::<UnpublishedKey>::new(kind));
        let second = registration.clone();
        drop(registration);
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        std::thread::spawn(move || drop(second)).join().unwrap();
        assert_eq!(drops.load(Ordering::Relaxed), 0);
    }
    drop(unused);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}
