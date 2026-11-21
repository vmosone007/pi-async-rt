//! 核心（runtime core）本地绑定（local binding）与公共选择（public selection）的公开行为。
//! 使用真实 TaskThread 和仅供身份、状态查询的实现者句柄；不代替任务接纳、推进或退出。

use pi_async_runtime_core::{
    context::record::{bind_current, try_with_current, try_with_current_local, try_with_local_for, with_selected},
    definitions::LocalRuntimeAccessError,
    CloseState, CurrentRuntimeAccessError, IdleState, RuntimeId, RuntimeQuery, RuntimeStatus,
};
use pi_async_task::TaskThread;
use std::{cell::Cell, rc::Rc};

/// 公共查询（public query）夹具。本测试只使用身份与服务状态，不提供业务操作。
#[derive(Clone)]
struct BindingQuery {
    id: RuntimeId,
    status: RuntimeStatus,
    label: usize,
}

impl RuntimeQuery for BindingQuery {
    fn id(&self) -> RuntimeId { self.id }
    fn status(&self) -> RuntimeStatus { self.status }
    fn is_driving(&self) -> bool { panic!("不应查询推进权（drive permission）") }
    fn last_idle_state(&self) -> IdleState { panic!("不应查询空闲策略（idle policy）") }
    fn close_state(&self) -> CloseState { panic!("不应查询关闭进度（close progress）") }
    fn external_task_count(&self) -> usize { panic!("不应查询来源（source）") }
    fn internal_task_count(&self) -> usize { panic!("不应查询来源（source）") }
    fn timer_registration_count(&self) -> usize { panic!("不应查询计时登记（timer registration）") }
    fn idle_task_count(&self) -> usize { panic!("不应查询来源（source）") }
    fn in_flight_task_count(&self) -> usize { panic!("不应查询任务（Task）") }
    fn waiting_for_wake_count(&self) -> usize { panic!("不应查询任务（Task）") }
    fn pending_cancel_task_count(&self) -> usize { panic!("不应查询取消（cancellation）") }
}

/// 通过 bind_current、with_selected 与 try_with_current 核对撤销（revocation）顺序：
/// 释放内层令牌恢复外层；在同服务显式选择（selection）中释放外层令牌，不清除活动选择，退出时也不复活外层。
/// 使用本地非 Send 设施，离开范围后该设施仍只由测试持有并正常释放（drop）。
#[test]
fn test_binding_drop_restores_parent_and_preserves_active_selection() {
    let owner = TaskThread::new();
    let local = Rc::new(Cell::new(3));
    let outer = BindingQuery { id: RuntimeId::try_new().unwrap(), status: RuntimeStatus::Open, label: 10 };
    let binding = bind_current(outer.clone(), &owner, &local).unwrap();
    let inner = BindingQuery { label: 20, ..outer.clone() };
    let inner_binding = bind_current(inner, &owner, &local).unwrap();
    assert_eq!(try_with_current::<BindingQuery, _>(|handle| handle.label).unwrap(), 20);
    drop(inner_binding);
    assert_eq!(try_with_current::<BindingQuery, _>(|handle| handle.label).unwrap(), 10);
    let selected = BindingQuery { label: 30, ..outer };
    with_selected(selected, || {
        drop(binding);
        assert_eq!(try_with_current::<BindingQuery, _>(|handle| handle.label).unwrap(), 30);
    }).unwrap();
    assert!(matches!(try_with_current::<BindingQuery, _>(|_| ()), Err(CurrentRuntimeAccessError::NoCurrentRuntime)));
    assert_eq!(local.get(), 3);
    assert_eq!(Rc::strong_count(&local), 1);
    owner.close();
}

/// 验证 bind_current 不为关闭的服务或已关闭的所属域（owner domain）安装绑定；
/// 不同服务返回 DifferentRuntime，任何拒绝（rejection）均保留此前选定的句柄身份。
#[test]
fn test_binding_rejects_closed_owner_and_conflicting_runtime() {
    let owner = TaskThread::new();
    let closed_owner = TaskThread::new();
    closed_owner.close();
    let local = Rc::new(Cell::new(0));
    let selected = BindingQuery { id: RuntimeId::try_new().unwrap(), status: RuntimeStatus::Open, label: 10 };
    let other = BindingQuery { id: RuntimeId::try_new().unwrap(), status: RuntimeStatus::Open, label: 20 };
    with_selected(selected.clone(), || {
        assert!(matches!(bind_current(other, &owner, &local), Err(CurrentRuntimeAccessError::DifferentRuntime)));
        assert_eq!(try_with_current::<BindingQuery, _>(|handle| handle.id()).unwrap(), selected.id);
        assert!(bind_current(selected.clone(), &closed_owner, &local).is_err());
        let closed_service = BindingQuery { status: RuntimeStatus::Closed, ..selected };
        assert!(bind_current(closed_service, &owner, &local).is_err());
        assert_eq!(try_with_current::<BindingQuery, _>(|handle| handle.label).unwrap(), 10);
    }).unwrap();
    assert!(matches!(try_with_current::<BindingQuery, _>(|_| ()), Err(CurrentRuntimeAccessError::NoCurrentRuntime)));
    assert_eq!(Rc::strong_count(&local), 1);
    owner.close();
}

/// 验证 try_with_current_local 仅继承同目标的有效本地设施，使用当前公共句柄；
/// 类型错误、权限撤销与弱引用失效均拒绝执行；合法闭包可重入公共查询（public query）。
#[test]
fn test_current_local_uses_selected_handle_and_does_not_restore_revoked_binding() {
    let owner = TaskThread::new();
    let local = Rc::new(Cell::new(3_usize));
    let root = BindingQuery { id: RuntimeId::try_new().unwrap(), status: RuntimeStatus::Open, label: 10 };
    let unexpected = |_: &BindingQuery, _: &Cell<usize>| panic!("不应取得本地权限（local permission）");
    assert!(matches!(try_with_current_local(unexpected), Err(LocalRuntimeAccessError::NoLocalRuntime)));

    let binding = bind_current(root.clone(), &owner, &local).unwrap();
    let selected = BindingQuery { label: 30, ..root.clone() };
    with_selected(selected, || {
        assert_eq!(try_with_current_local::<BindingQuery, Cell<usize>, _>(|handle, value| {
            assert_eq!(try_with_current::<BindingQuery, _>(|active| active.id()).unwrap(), root.id);
            (handle.label, value.get())
        }).unwrap(), (30, 3));
        assert!(matches!(try_with_current_local::<u8, Cell<usize>, _>(|_, _| panic!("错误句柄类型（handle type）")), Err(LocalRuntimeAccessError::TypeMismatch)));
        assert!(matches!(try_with_current_local::<BindingQuery, u8, _>(|_, _| panic!("错误设施类型（facility type）")), Err(LocalRuntimeAccessError::TypeMismatch)));
        drop(binding);
        assert!(matches!(try_with_current_local(unexpected), Err(LocalRuntimeAccessError::AccessRevoked)));
    }).unwrap();
    assert!(matches!(try_with_current_local(unexpected), Err(LocalRuntimeAccessError::NoLocalRuntime)));
    assert_eq!(Rc::strong_count(&local), 1);
    let binding = bind_current(root, &owner, &local).unwrap();
    drop(local);
    assert!(matches!(try_with_current_local(unexpected), Err(LocalRuntimeAccessError::AccessRevoked)));
    drop(binding);
    owner.close();
}

/// 验证 try_with_local_for 按显式服务身份选择原本地设施，不创建公共选择或授权。
/// 其他目标、错误类型、无绑定的公共选择及绑定撤销均不得调用用户闭包（closure）。
#[test]
fn test_explicit_local_access_checks_target_and_binding_before_calling() {
    let owner = TaskThread::new();
    let local = Rc::new(Cell::new(7_usize));
    let handle = BindingQuery { id: RuntimeId::try_new().unwrap(), status: RuntimeStatus::Open, label: 4 };
    let actual = handle.id;
    let other = RuntimeId::try_new().unwrap();
    let unexpected = |_: &Cell<usize>| panic!("缺失或不符的本地权限（local permission）");
    assert!(matches!(try_with_local_for(actual, unexpected), Err(LocalRuntimeAccessError::NoLocalRuntime)));
    with_selected(handle.clone(), || {
        assert!(matches!(try_with_local_for(actual, unexpected), Err(LocalRuntimeAccessError::NoLocalRuntime)));
    }).unwrap();
    let binding = bind_current(handle.clone(), &owner, &local).unwrap();
    with_selected(handle, || {
        assert!(matches!(try_with_local_for(other, unexpected), Err(LocalRuntimeAccessError::NoLocalRuntime)));
        assert!(matches!(try_with_local_for::<u8, _>(actual, |_| panic!("错误设施类型（facility type）")), Err(LocalRuntimeAccessError::TypeMismatch)));
        assert_eq!(try_with_local_for::<Cell<usize>, _>(actual, |value| {
            assert_eq!(try_with_current::<BindingQuery, _>(|selected| selected.id()).unwrap(), actual);
            value.get()
        }).unwrap(), 7);
        drop(binding);
        assert!(matches!(try_with_local_for(actual, unexpected), Err(LocalRuntimeAccessError::AccessRevoked)));
    }).unwrap();
    assert!(matches!(try_with_local_for(actual, unexpected), Err(LocalRuntimeAccessError::NoLocalRuntime)));
    assert_eq!(Rc::strong_count(&local), 1);
    owner.close();
}
