//! 当前公共环境（current public context）与活动推进（active drive）的合法查询（queries）。
use pi_async_runtime_core::{context::record::{self, DriveScope}, CurrentRuntimeAccessError};
use std::cell::Cell;

/// 验证 DriveScope::is_active 初始为否且各线程（threads）独立，查询（query）不创建服务（service）或取得推进权（drive permission）。
#[test]
fn test_drive_scope_starts_inactive_without_creating_service() {
    assert!(!DriveScope::is_active());
    std::thread::spawn(|| { assert!(!DriveScope::is_active()); assert!(!DriveScope::is_active()); }).join().unwrap();
    assert!(!DriveScope::is_active());
}

/// 验证 try_with_current 在无目标（no selection）时返回原分类（classification），不调用用户闭包（closure）或安装目标（target）。
#[test]
fn test_current_borrow_without_selection_does_not_call_action() {
    let calls = Cell::new(0);
    let result = record::try_with_current::<usize, _>(|_| { calls.set(calls.get() + 1); });
    assert!(matches!(result, Err(CurrentRuntimeAccessError::NoCurrentRuntime)));
    assert_eq!(calls.get(), 0);
    assert!(matches!(record::try_with_current::<String, _>(|_| ()), Err(CurrentRuntimeAccessError::NoCurrentRuntime)));
}

/// 观察用户值析构（user-value drop）期间活动标记（active marker）仍受保护，不推进任何服务（service）。
struct ActiveDrop<'a>(&'a Cell<usize>);
impl Drop for ActiveDrop<'_> {
    fn drop(&mut self) { assert!(DriveScope::is_active()); self.0.set(self.0.get() + 1); }
}

/// 验证 DriveScope::enter 与释放（drop）必须配对：首次进入（enter）、同线程嵌套拒绝（nested rejection）、不同线程隔离（thread isolation）。
/// 正常／异常退出（normal exit/unwinding）先析构用户值（drop user value）再恢复活动标记（active marker），原恐慌载荷（panic payload）保持。
#[test]
fn test_drive_scope_pairs_entry_exit_and_unwind_without_task_identity() {
    assert!(!DriveScope::is_active());
    let dropped = Cell::new(0);
    {
        let scope = DriveScope::enter();
        assert!(DriveScope::is_active());
        assert!(pi_async_task::task::try_current_task_id().is_none());
        let nested = std::panic::catch_unwind(DriveScope::enter);
        assert!(nested.is_err());
        assert!(DriveScope::is_active());
        std::thread::spawn(|| { assert!(!DriveScope::is_active()); }).join().unwrap();
        drop(ActiveDrop(&dropped));
        drop(scope);
    }
    assert_eq!(dropped.get(), 1);
    assert!(!DriveScope::is_active());
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _scope = DriveScope::enter();
        let _value = ActiveDrop(&dropped);
        std::panic::panic_any(123_u32);
    }));
    let payload = failure.unwrap_err();
    assert_eq!(payload.downcast_ref::<u32>(), Some(&123));
    assert_eq!(dropped.get(), 2);
    assert!(!DriveScope::is_active());
    let scope = DriveScope::enter();
    assert!(DriveScope::is_active());
    drop(scope);
    assert!(!DriveScope::is_active());
}

/// 验证 try_current 在无目标（no selection）时不合成句柄（handle），保持 NoCurrentRuntime。
#[test]
fn test_current_owned_access_without_selection_preserves_no_target() {
    assert!(matches!(record::try_current::<String>(), Err(CurrentRuntimeAccessError::NoCurrentRuntime)));
    assert!(matches!(record::try_current::<usize>(), Err(CurrentRuntimeAccessError::NoCurrentRuntime)));
    assert!(matches!(record::try_with_current::<String, _>(|_| ()), Err(CurrentRuntimeAccessError::NoCurrentRuntime)));
}

/// 同步公共选择（synchronous public selection）的真实查询能力（query capability）；只提供固定身份（identity）及关闭事实（closed status）。
/// 其余查询（queries）拒绝被调用，避免测试设施（fixture）代补任务推进（task progress）或服务生命周期（service lifecycle）。
struct QueryHandle<const TAG: usize> {
    id: pi_async_runtime_core::RuntimeId,
    label: usize,
    clones: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    drops: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}
impl<const TAG: usize> Clone for QueryHandle<TAG> {
    fn clone(&self) -> Self {
        self.clones.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self { id: self.id, label: self.label, clones: self.clones.clone(), drops: self.drops.clone() }
    }
}
impl<const TAG: usize> Drop for QueryHandle<TAG> {
    fn drop(&mut self) {
        // 最终释放（final drop）允许重入查询（reentrant query），不能仍持有线程局部存储（TLS）的可变借用（mutable borrow）。
        assert!(matches!(record::try_with_current::<usize, _>(|_| ()),
            Err(CurrentRuntimeAccessError::NoCurrentRuntime | CurrentRuntimeAccessError::TypeMismatch)));
        self.drops.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}
impl<const TAG: usize> pi_async_runtime_core::RuntimeQuery for QueryHandle<TAG> {
    fn id(&self) -> pi_async_runtime_core::RuntimeId { self.id }
    fn status(&self) -> pi_async_runtime_core::RuntimeStatus { pi_async_runtime_core::RuntimeStatus::Closed }
    fn is_driving(&self) -> bool { panic!("公共选择不查询推进权（selection must not query drive ownership）") }
    fn last_idle_state(&self) -> pi_async_runtime_core::IdleState { panic!("不查询空闲策略（idle policy must not be queried）") }
    fn close_state(&self) -> pi_async_runtime_core::CloseState { panic!("不推进关闭（close progress must not be queried）") }
    fn external_task_count(&self) -> usize { panic!("不查询来源（sources must not be queried）") }
    fn internal_task_count(&self) -> usize { panic!("不查询来源（sources must not be queried）") }
    fn timer_registration_count(&self) -> usize { panic!("不查询计时登记（timer registrations must not be queried）") }
    fn idle_task_count(&self) -> usize { panic!("不查询来源（sources must not be queried）") }
    fn in_flight_task_count(&self) -> usize { panic!("不查询任务（tasks must not be queried）") }
    fn waiting_for_wake_count(&self) -> usize { panic!("不查询任务（tasks must not be queried）") }
    fn pending_cancel_task_count(&self) -> usize { panic!("不查询取消（cancellation must not be queried）") }
}

/// 验证 with_selected 及其配对恢复（paired restoration）：关闭公共目标（closed public target）仍可选，同身份不同类型（same identity/different type）可嵌套。
/// 覆盖借用不克隆（borrow without clone）、拥有型取得恰一次克隆（owned access clones once）、不同身份拒绝（different-identity rejection）、异常载荷（panic payload）及恢复。
/// 返回的异步计算（Future）在选择范围（selection scope）外轮询（poll），不延长当前环境（current context）；最终句柄析构（handle drop）可重入查询（query）。
#[test]
fn test_selected_context_restores_identity_types_and_unwind_without_permissions() {
    use pi_async_runtime_core::{RuntimeId, RuntimeQuery, RuntimeStatus};
    use std::{future::Future, sync::{Arc, atomic::{AtomicUsize, Ordering}}, task::{Context, Poll, Waker}};
    let id = RuntimeId::try_new().unwrap();
    let different = RuntimeId::try_new().unwrap();
    let clones = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let rejected_calls = Cell::new(0);
    let outer = QueryHandle::<0> { id, label: 10, clones: clones.clone(), drops: drops.clone() };
    let other = QueryHandle::<0> { id: different, label: 99, clones: clones.clone(), drops: drops.clone() };
    let future = record::with_selected(outer, || {
        assert!(!DriveScope::is_active());
        assert!(pi_async_task::task::try_current_task_id().is_none());
        record::try_with_current::<QueryHandle<0>, _>(|handle| {
            assert_eq!(handle.label, 10);
            assert_eq!(handle.status(), RuntimeStatus::Closed);
            assert_eq!(record::try_with_current::<QueryHandle<0>, _>(|same| same.id()).unwrap(), id);
        }).unwrap();
        assert_eq!(clones.load(Ordering::Relaxed), 0);
        let owned = record::try_current::<QueryHandle<0>>().unwrap();
        assert_eq!(owned.id(), id);
        assert_eq!(clones.load(Ordering::Relaxed), 1);
        record::try_with_current::<QueryHandle<0>, _>(|handle| {
            drop(owned);
            assert_eq!(handle.label, 10);
        }).unwrap();
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert!(matches!(record::with_selected(other, || rejected_calls.set(1)), Err(CurrentRuntimeAccessError::DifferentRuntime)));
        assert_eq!(rejected_calls.get(), 0);
        let nested = QueryHandle::<1> { id, label: 20, clones: clones.clone(), drops: drops.clone() };
        record::with_selected(nested, || {
            assert!(matches!(record::try_current::<QueryHandle<0>>(), Err(CurrentRuntimeAccessError::TypeMismatch)));
            assert_eq!(record::try_with_current::<QueryHandle<1>, _>(|handle| handle.label).unwrap(), 20);
        }).unwrap();
        assert_eq!(record::try_with_current::<QueryHandle<0>, _>(|handle| handle.label).unwrap(), 10);
        let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let nested = QueryHandle::<0> { id, label: 30, clones: clones.clone(), drops: drops.clone() };
            let _: Result<(), _> = record::with_selected(nested, || {
                assert_eq!(record::try_with_current::<QueryHandle<0>, _>(|handle| handle.label).unwrap(), 30);
                std::panic::panic_any(456_u32);
            });
        }));
        assert_eq!(failure.unwrap_err().downcast_ref::<u32>(), Some(&456));
        assert_eq!(record::try_with_current::<QueryHandle<0>, _>(|handle| handle.label).unwrap(), 10);
        assert_eq!(clones.load(Ordering::Relaxed), 1);
        assert_eq!(drops.load(Ordering::Relaxed), 4);
        async { matches!(record::try_current::<QueryHandle<0>>(), Err(CurrentRuntimeAccessError::NoCurrentRuntime)) }
    }).unwrap();
    assert_eq!(drops.load(Ordering::Relaxed), 5);
    assert!(matches!(record::try_current::<QueryHandle<0>>(), Err(CurrentRuntimeAccessError::NoCurrentRuntime)));
    let mut future = std::pin::pin!(future);
    assert_eq!(future.as_mut().poll(&mut Context::from_waker(Waker::noop())), Poll::Ready(true));
    assert!(!DriveScope::is_active());
}
