//! 运行时（runtime）本地权限与默认普通来源的退出接线。
use pi_async_runtime_core::context::LocalBinding;
use pi_async_task_queue::{
    management::QueueCleanupProgress, ExternalQueueManager, MpscTaskQueue, MpscTaskQueueHandle,
};
use std::{cell::RefCell, rc::Rc};

// 只在原线程共享；公共Handle和ExecutorLocal均不持此Rc，不形成管理者强环。
pub(crate) struct RuntimeBinding {
    current: RefCell<Option<LocalBinding>>,
}
impl RuntimeBinding {
    pub(crate) fn new() -> Rc<Self> {
        runtime_binding_new_inner()
    }
    pub(crate) fn is_present(&self) -> bool {
        self.current.borrow().is_some()
    }
    pub(crate) fn replace(&self, binding: LocalBinding) {
        runtime_binding_replace_inner(self, binding)
    }
    pub(crate) fn revoke(&self) {
        runtime_binding_revoke_inner(self)
    }
}

// 不修改原来源协议；只在运行时选择的首个管理来源退出前撤本地权限。
// queue字段在本Drop之后自动退出，revoke即使展开也不能遗失原queue。
pub(crate) struct RuntimeExternalSource {
    queue: MpscTaskQueue,
    binding: Rc<RuntimeBinding>,
}
impl RuntimeExternalSource {
    pub(crate) fn new(queue: MpscTaskQueue, binding: Rc<RuntimeBinding>) -> Self {
        runtime_source_new_inner(queue, binding)
    }
}
impl ExternalQueueManager for RuntimeExternalSource {
    type Handle = MpscTaskQueueHandle;
    fn handle(&self) -> Self::Handle {
        self.queue.handle()
    }
    fn clear(&mut self) -> pi_result::Result<QueueCleanupProgress> {
        self.queue.clear()
    }
    fn close(&mut self) -> pi_result::Result<QueueCleanupProgress> {
        runtime_source_close_inner(self)
    }
}
impl Drop for RuntimeExternalSource {
    fn drop(&mut self) {
        runtime_source_drop_inner(self)
    }
}
include!("lifecycle_inner.rs");
