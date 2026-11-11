//! 验证创建选项（task options）和错误（error）的公开值行为。

use pi_async_task::definitions::QueueSupport;
use pi_async_task::join::PanicRecord;
use pi_async_task::{TaskError, TaskOptions};
use std::fmt::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// 默认选项（task options）不携带链接（link），也不启用可选采集或观测（observation）。
#[test]
fn test_plain_options_disable_optional_task_facilities() {
    let options = TaskOptions::plain();
    assert_eq!(options.queue, QueueSupport::Plain);
    assert!(!options.capture_panic_backtrace);
    #[cfg(feature = "observe")]
    assert!(options.observer.is_none());
}

/// 侵入式选项（intrusive options）只选择链接（link）能力，不自动开启观测（observation）。
#[test]
fn test_intrusive_options_disable_optional_task_facilities() {
    let options = TaskOptions::intrusive();
    assert_eq!(options.queue, QueueSupport::IntrusiveLocal);
    assert!(!options.capture_panic_backtrace);
    #[cfg(feature = "observe")]
    assert!(options.observer.is_none());
}

/// 记录任意恐慌载荷（panic payload）的析构（drop）次数，不提供格式化（formatting）能力。
struct PayloadDrop(Arc<AtomicUsize>);

impl Drop for PayloadDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// 通过错误显示（Display）核对完整分类，并确认恐慌载荷（panic payload）保持原持有。
#[test]
fn test_error_display_preserves_classification_and_panic_payload() {
    let drops = Arc::new(AtomicUsize::new(0));
    let cases = [
        (TaskError::Cancelled, "任务（Task）已取消（cancelled）"),
        (TaskError::Abandoned, "初始执行持有（runnable handle）已放弃"),
        (
            TaskError::Panicked(PanicRecord {
                payload: Box::new(PayloadDrop(Arc::clone(&drops))),
                backtraces: Vec::new(),
            }),
            "任务轮询（Future::poll）发生恐慌（panic）",
        ),
        (TaskError::CleanupUnconfirmed, "原异步计算（Future）清理尚未确认"),
        (TaskError::OwnerClosed, "所属域已关闭"),
        (TaskError::WrongThread, "当前线程（thread）不满足结果领取限制"),
        (TaskError::AlreadyTaken, "结果（output）已领取"),
    ];
    for (error, expected) in &cases {
        assert_eq!(error.to_string(), *expected);
        assert_eq!(error.to_string(), *expected);
    }
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(cases);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

/// 模拟格式化（formatting）目标拒绝写入，验证错误（error）被直接传回。
struct RejectWrite;

impl std::fmt::Write for RejectWrite {
    fn write_str(&mut self, _value: &str) -> std::fmt::Result {
        Err(std::fmt::Error)
    }
}

/// 格式化（formatting）失败不被显示入口（Display）吞掉。
#[test]
fn test_error_display_propagates_destination_failure() {
    let mut destination = RejectWrite;
    assert!(write!(&mut destination, "{}", TaskError::Cancelled).is_err());
}
