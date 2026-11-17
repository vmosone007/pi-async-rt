use crate::{definitions::*, query::RuntimeQuery};
use pi_async_task::definitions::TaskOwnerId;
use pi_async_task::TaskThread;
use std::{
    any::{Any, TypeId},
    cell::{Cell, RefCell},
    rc::{Rc, Weak},
};

// Rc保公共入口，Weak只指向同线程本地设施。不得把本地设施强根放入公共Frame。
struct ContextFrame {
    runtime_id: RuntimeId,
    handle_type: TypeId,
    handle: Rc<dyn Any>,
    local: Option<LocalRecord>,
    previous: Option<Rc<ContextFrame>>,
    active: Cell<bool>,
}
struct LocalRecord {
    owner: TaskOwnerId,
    value: Weak<dyn Any>,
    valid: Rc<Cell<bool>>,
}
thread_local! {
    static CURRENT_RUNTIME:RefCell<Option<Rc<ContextFrame>>>=const { RefCell::new(None) };
    static ACTIVE_DRIVE:Cell<bool>=const { Cell::new(false) };
}

/// 实现者持有的线程绑定范围；释放先撤销本地权限，不因run_once返回而自动消失。
/// 不可跨线程转交。实际任务清理由拥有者经原生TaskThread负责。
/// 释放（drop）先撤销本次本地权限；本次仍为当前选择时才恢复此前有效记录。
/// 内层同步选择仍活动时保留内层选择，之后恢复不会复活本次已撤销绑定。
/// 线程局部存储（TLS）已不可访问时仍撤销本次权限；本操作不代表任务（Task）已清理。
pub struct LocalBinding {
    frame: Rc<ContextFrame>,
    valid: Rc<Cell<bool>>,
}
impl Drop for LocalBinding {
    fn drop(&mut self) {
        local_binding_drop_inner(self)
    }
}

/// 绑定已存在的原所属域与设施；创建运行时本身不调用本函数。
pub fn bind_current<H: RuntimeQuery + Clone + Send + Sync + 'static, L: 'static>(
    handle: H,
    owner: &TaskThread,
    local: &Rc<L>,
) -> pi_result::RawResult<LocalBinding, CurrentRuntimeAccessError> {
    context_bind_inner(handle, owner, local)
}
/// 克隆（Clone）当前公共句柄（handle）；H须与选择时保存的具体类型（type）一致。
/// 无目标、类型不符或线程局部存储（TLS）不可访问时返回对应错误，不取得本地权限。
pub fn try_current<H: Clone + 'static>() -> pi_result::RawResult<H, CurrentRuntimeAccessError> {
    context_current_inner()
}
/// 在同步闭包（closure）内借用当前公共句柄（handle），不克隆H。引用不能逃逸或跨await。
/// 无目标、类型（type）不符或线程局部存储（TLS）不可访问时返回错误。
/// 有效公共选择在服务关闭后仍可查询原事实，不因此恢复本地权限。
pub fn try_with_current<H: 'static, R>(
    action: impl FnOnce(&H) -> R,
) -> pi_result::RawResult<R, CurrentRuntimeAccessError> {
    context_borrow_inner(action)
}
/// 在同步闭包（closure）期间选择原服务；只允许无目标或同一RuntimeId，不同目标返回DifferentRuntime。
/// 线程局部存储（TLS）不可访问返回ContextUnavailable；正常返回或恐慌（panic）展开均恢复此前仍有效的选择。
/// 已撤销的本地权限不会复活；允许选择已关闭服务，但不产生本地或推进权限。返回的异步计算（Future）不延长范围。
pub fn with_selected<H: RuntimeQuery + Clone + Send + Sync + 'static, R>(
    handle: H,
    action: impl FnOnce() -> R,
) -> pi_result::RawResult<R, CurrentRuntimeAccessError> {
    context_select_inner(handle, action)
}
/// 实现者使用的同步本地访问；先校验同一公开目标与本地有效性，再短暂升级Weak。
/// 闭包（closure）不跨await，TLS内部借用不得覆盖其执行。
pub fn try_with_current_local<H: 'static, L: 'static, R>(
    action: impl FnOnce(&H, &L) -> R,
) -> pi_result::RawResult<R, LocalRuntimeAccessError> {
    context_local_inner(action)
}

/// 实现者按原服务身份取得同线程设施，不依赖上层公共句柄（handle）的具体类型。
/// 仍核当前公共目标、原所属域和有效标记；不创建、转投或延长本地权限。
pub fn try_with_local_for<L: 'static, R>(
    runtime_id: RuntimeId,
    action: impl FnOnce(&L) -> R,
) -> pi_result::RawResult<R, LocalRuntimeAccessError> {
    context_local_for_inner(runtime_id, action)
}

// 活动推进范围不等于公共环境；覆盖用户hook、轮询和退出收尾。
/// 本线程的一次活动推进范围，包含轮询（poll）、同步钩子（hook）和必要退出收尾。
/// 不同于公共服务选择和原服务的全局推进状态，不授予队列或任务（Task）访问权限。
/// 释放（drop）恢复原活动标记；正常返回和恐慌（panic）展开均配对处理。
pub struct DriveScope {
    previous: bool,
    local: std::marker::PhantomData<Rc<()>>,
}
impl DriveScope {
    /// 已有活动推进或原生当前Task时，在首次新根poll前panic。
    pub fn enter() -> Self {
        drive_scope_enter_inner()
    }
    /// 只读当前线程活动标记，不取得推进权，也不查询某服务的全局is_driving。
    /// 线程局部存储（TLS）不可访问时保守返回禁止新推进的结果。
    pub fn is_active() -> bool {
        drive_scope_active_inner()
    }
}
impl Drop for DriveScope {
    fn drop(&mut self) {
        drive_scope_drop_inner(self)
    }
}
struct SelectionGuard {
    frame: Rc<ContextFrame>,
}
impl Drop for SelectionGuard {
    fn drop(&mut self) {
        selection_drop_inner(self)
    }
}
include!("record_inner.rs");
