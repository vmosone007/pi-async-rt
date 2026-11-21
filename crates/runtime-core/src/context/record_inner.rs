#[allow(unused_variables)]
fn context_bind_inner<H: RuntimeQuery + Clone + Send + Sync + 'static, L: 'static>(
    handle: H,
    owner: &TaskThread,
    local: &Rc<L>,
) -> pi_result::RawResult<LocalBinding, CurrentRuntimeAccessError> {
    // 查询能力（query capability）可能重入；先取样，再读取当前线程的选择（selection）。
    let runtime_id = handle.id();
    let open = handle.status() == RuntimeStatus::Open && !owner.is_closed();
    let current = CURRENT_RUNTIME.try_with(|current| current.borrow().clone())
        .map_err(|_| CurrentRuntimeAccessError::ContextUnavailable)?;
    let mut previous = current.as_ref();
    while let Some(frame) = previous {
        if frame.active.get() {
            break;
        }
        previous = frame.previous.as_ref();
    }
    if previous.is_some_and(|frame| frame.runtime_id != runtime_id) {
        return Err(CurrentRuntimeAccessError::DifferentRuntime);
    }
    if !open {
        // 不可绑定的目标不提供当前本地环境；已有公共选择（public selection）保留。
        return Err(CurrentRuntimeAccessError::NoCurrentRuntime);
    }

    // TaskThread 及 Rc 只可在原线程借用；记录只保留设施的弱引用（Weak）。
    let value: Weak<dyn Any> = Rc::<L>::downgrade(local);
    let valid = Rc::new(Cell::new(true));
    let frame = Rc::new(ContextFrame {
        runtime_id,
        handle_type: TypeId::of::<H>(),
        handle: Rc::new(handle),
        local: Some(LocalRecord { owner: owner.id(), value, valid: Rc::clone(&valid) }),
        previous: previous.cloned(),
        active: Cell::new(true),
    });
    let removed = CURRENT_RUNTIME.try_with(|current| current.replace(Some(Rc::clone(&frame))))
        .map_err(|_| CurrentRuntimeAccessError::ContextUnavailable)?;
    // 先建立撤销令牌（revocation token），再释放旧记录；旧公共句柄析构展开时也能恢复范围。
    let binding = LocalBinding { frame, valid };
    drop(removed);
    drop(current);
    Ok(binding)
}
#[allow(unused_variables)]
fn context_current_inner<H: Clone + 'static>() -> pi_result::RawResult<H, CurrentRuntimeAccessError>
{
    try_with_current::<H, H>(Clone::clone)
}
#[allow(unused_variables)]
fn context_borrow_inner<H: 'static, R>(
    action: impl FnOnce(&H) -> R,
) -> pi_result::RawResult<R, CurrentRuntimeAccessError> {
    let mut frame = CURRENT_RUNTIME.try_with(|current| current.borrow().clone())
        .map_err(|_| CurrentRuntimeAccessError::ContextUnavailable)?;
    // 撤销记录（inactive frame）只沿原前序（previous frame）查找；原强持有（Rc）保住回调（callback）借用。
    while let Some(current) = frame {
        if current.active.get() {
            let handle = current.handle.downcast_ref::<H>()
                .ok_or(CurrentRuntimeAccessError::TypeMismatch)?;
            return Ok(action(handle));
        }
        frame = current.previous.clone();
    }
    Err(CurrentRuntimeAccessError::NoCurrentRuntime)
}
#[allow(unused_variables)]
fn context_select_inner<H: RuntimeQuery + Clone + Send + Sync + 'static, R>(
    handle: H,
    action: impl FnOnce() -> R,
) -> pi_result::RawResult<R, CurrentRuntimeAccessError> {
    let runtime_id = handle.id();
    let mut previous = CURRENT_RUNTIME.try_with(|current| current.borrow().clone())
        .map_err(|_| CurrentRuntimeAccessError::ContextUnavailable)?;
    while let Some(frame) = previous.as_ref() {
        if frame.active.get() {
            break;
        }
        previous = frame.previous.clone();
    }
    if previous.as_ref().is_some_and(|frame| frame.runtime_id != runtime_id) {
        return Err(CurrentRuntimeAccessError::DifferentRuntime);
    }
    let frame = Rc::new(ContextFrame {
        runtime_id,
        handle_type: TypeId::of::<H>(),
        handle: Rc::new(handle),
        local: None,
        previous,
        active: Cell::new(true),
    });
    let removed = CURRENT_RUNTIME.try_with(|current| current.replace(Some(frame.clone())))
        .map_err(|_| CurrentRuntimeAccessError::ContextUnavailable)?;
    // 先建立恢复守卫（restoration guard），旧记录（old frame）的用户析构（drop）即使展开也不会遗留本次选择（selection）。
    let _guard = SelectionGuard { frame };
    drop(removed);
    Ok(action())
}
#[allow(unused_variables)]
fn context_local_inner<H: 'static, L: 'static, R>(
    action: impl FnOnce(&H, &L) -> R,
) -> pi_result::RawResult<R, LocalRuntimeAccessError> {
    let current = CURRENT_RUNTIME.try_with(|current| current.borrow().clone())
        .map_err(|_| LocalRuntimeAccessError::ContextUnavailable)?;
    let mut selected = current.as_deref();
    while let Some(frame) = selected {
        if frame.active.get() {
            break;
        }
        selected = frame.previous.as_deref();
    }
    let selected = selected.ok_or(LocalRuntimeAccessError::NoLocalRuntime)?;
    if selected.handle_type != TypeId::of::<H>() {
        return Err(LocalRuntimeAccessError::TypeMismatch);
    }
    let handle = selected.handle.downcast_ref::<H>().ok_or(LocalRuntimeAccessError::TypeMismatch)?;
    // 公开句柄（public handle）来自本次选择，本地权限沿同一目标的窄出口核验。
    try_with_local_for::<L, R>(selected.runtime_id, |local| action(handle, local))
}
#[allow(unused_variables)]
fn local_binding_drop_inner(this: &mut LocalBinding) {
    this.valid.set(false);
    this.frame.active.set(false);

    let mut previous = this.frame.previous.as_ref();
    while let Some(frame) = previous {
        if frame.active.get() {
            break;
        }
        previous = frame.previous.as_ref();
    }

    // 只恢复仍指向本次绑定的选择（selection）；子范围退出时会跳过此已撤销记录。
    let removed = CURRENT_RUNTIME.try_with(|current| {
        let mut current = current.borrow_mut();
        if current.as_ref().is_some_and(|frame| Rc::ptr_eq(frame, &this.frame)) {
            std::mem::replace(&mut *current, previous.cloned())
        } else {
            None
        }
    });
    // 公共句柄析构（handle drop）可以重入查询，须在 TLS 借用归还之后。
    drop(removed);
}
#[allow(unused_variables)]
fn selection_drop_inner(this: &mut SelectionGuard) {
    this.frame.active.set(false);
    let mut previous = this.frame.previous.clone();
    while let Some(frame) = previous.as_ref() {
        if frame.active.get() {
            break;
        }
        previous = frame.previous.clone();
    }
    // 先恢复选择（selection），再于线程局部存储（TLS）借用外释放旧记录（old frame）；用户析构（drop）可重入查询（query）。
    let removed = CURRENT_RUNTIME.try_with(|current| current.replace(previous));
    drop(removed);
}
fn drive_scope_enter_inner() -> DriveScope {
    assert!(pi_async_task::task::try_current_task_id().is_none(),
        "任务轮询中不能嵌套推进（nested drive during task polling is forbidden）");
    let previous = ACTIVE_DRIVE.try_with(|active| {
        assert!(!active.get(), "已有活动推进范围（a drive scope is already active）");
        active.replace(true)
    }).expect("活动推进的线程局部存储不可访问（drive-scope TLS unavailable）");
    DriveScope { previous, local: std::marker::PhantomData }
}
fn drive_scope_active_inner() -> bool {
    ACTIVE_DRIVE.try_with(Cell::get).unwrap_or(true)
}

#[allow(unused_variables)]
fn context_local_for_inner<L: 'static, R>(
    runtime_id: RuntimeId,
    action: impl FnOnce(&L) -> R,
) -> pi_result::RawResult<R, LocalRuntimeAccessError> {
    let current = CURRENT_RUNTIME.try_with(|current| current.borrow().clone())
        .map_err(|_| LocalRuntimeAccessError::ContextUnavailable)?;
    let mut selected = current.as_deref();
    while let Some(frame) = selected {
        if frame.active.get() {
            break;
        }
        selected = frame.previous.as_deref();
    }
    let selected = selected.ok_or(LocalRuntimeAccessError::NoLocalRuntime)?;
    if selected.runtime_id != runtime_id {
        return Err(LocalRuntimeAccessError::NoLocalRuntime);
    }

    // 公共选择（public selection）只能沿同目标的原线程绑定继承设施；不能越过已撤销的绑定。
    let mut candidate = Some(selected);
    while let Some(frame) = candidate {
        if frame.runtime_id != runtime_id {
            break;
        }
        if let Some(local) = frame.local.as_ref() {
            // 该记录只由持有原 TaskThread 借用的线程创建，TLS 与 Rc 均不允许跨线程使用。
            if !frame.active.get() || !local.valid.get() {
                return Err(LocalRuntimeAccessError::AccessRevoked);
            }
            let value = local.value.upgrade().ok_or(LocalRuntimeAccessError::AccessRevoked)?;
            let value = value.downcast::<L>().map_err(|_| LocalRuntimeAccessError::TypeMismatch)?;
            if !local.valid.get() {
                return Err(LocalRuntimeAccessError::AccessRevoked);
            }
            return Ok(action(&value));
        }
        candidate = frame.previous.as_deref();
    }
    Err(LocalRuntimeAccessError::NoLocalRuntime)
}
#[allow(unused_variables)]
fn drive_scope_drop_inner(this: &mut DriveScope) {
    let _ = ACTIVE_DRIVE.try_with(|active| active.set(this.previous));
}
