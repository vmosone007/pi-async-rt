//! 任务配置（TaskConfig）保留原附加数据（metadata），不创建任务（Task）。
use pi_async_runtime_core::TaskConfig;
use std::{cell::Cell, rc::Rc};

/// 仅观察原值（original value）是否被移动（move）和析构（drop）；故意不实现调试（Debug）、克隆（Clone）或跨线程转移（Send）。
struct MetadataProbe { dropped: Rc<Cell<usize>> }
impl Drop for MetadataProbe {
    fn drop(&mut self) { self.dropped.set(self.dropped.get() + 1); }
}

/// 验证 TaskConfig::new 可组织借用（borrowed）及非Send附加数据（metadata），默认不捕获堆栈（backtrace）或安装观察者（observer）。
/// 释放配置（configuration）恰好析构（drop）一次原值（original value）；创建任务（Task）时的约束（bounds）不提前外推。
#[test]
fn test_task_config_preserves_metadata_and_opt_in_defaults() {
    let dropped = Rc::new(Cell::new(0));
    let config = TaskConfig::new(MetadataProbe { dropped: dropped.clone() });
    assert!(!config.capture_panic_backtrace);
    #[cfg(feature = "observe")]
    assert!(config.observer.is_none());
    assert_eq!(dropped.get(), 0);
    assert!(Rc::ptr_eq(&config.metadata.dropped, &dropped));
    drop(config);
    assert_eq!(dropped.get(), 1);
    let owned = String::from("借用附加数据（borrowed metadata）");
    let borrowed = TaskConfig::new(owned.as_str());
    assert_eq!(borrowed.metadata, owned.as_str());
    assert!(!borrowed.capture_panic_backtrace);
    let default = TaskConfig::<()>::default();
    assert!(!default.capture_panic_backtrace);
    #[cfg(feature = "observe")]
    assert!(default.observer.is_none());
}
