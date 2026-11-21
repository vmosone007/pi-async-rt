//! 提交错误（submission errors）的公开原因格式化（cause formatting），不触及原任务（Task）。
use pi_async_runtime_core::{LocalRuntimeAccessError, SubmitErrorKind};
use pi_async_task_queue::{external::ExternalPushErrorKind, idle::IdleQueueError, local::QueuePushErrorKind, TimerQueueError};
use pi_result::ErrorKind;
use std::fmt::{self, Write};

/// 首次写入即拒绝的标准写入器（writer），仅观察格式化错误（formatting error）传播。
struct RejectOutput;
impl Write for RejectOutput {
    fn write_str(&mut self, _: &str) -> fmt::Result { Err(fmt::Error) }
}

/// 验证 SubmitErrorKind::Debug 保留四种直接拒绝（direct rejections）、四种本地访问原因（local access causes）和四类来源（sources）。
/// 所有公开有限原因（finite causes）均覆盖，两个后端报告（backend reports）均保留原附件（attachments）。
#[test]
fn test_submit_error_debug_keeps_all_rejection_causes() {
    for (error, name) in [
        (SubmitErrorKind::WrongRuntime, "WrongRuntime"), (SubmitErrorKind::LocalSubmissionRequired, "LocalSubmissionRequired"),
        (SubmitErrorKind::OrdinarySubmissionRequired, "OrdinarySubmissionRequired"), (SubmitErrorKind::WrongOwner, "WrongOwner"),
        (SubmitErrorKind::LocalContext(LocalRuntimeAccessError::NoLocalRuntime), "NoLocalRuntime"),
        (SubmitErrorKind::LocalContext(LocalRuntimeAccessError::TypeMismatch), "TypeMismatch"),
        (SubmitErrorKind::LocalContext(LocalRuntimeAccessError::AccessRevoked), "AccessRevoked"),
        (SubmitErrorKind::LocalContext(LocalRuntimeAccessError::ContextUnavailable), "ContextUnavailable"),
        (SubmitErrorKind::External(ExternalPushErrorKind::Busy), "Busy"), (SubmitErrorKind::External(ExternalPushErrorKind::Closed), "Closed"),
        (SubmitErrorKind::External(ExternalPushErrorKind::Capacity), "Capacity"), (SubmitErrorKind::External(ExternalPushErrorKind::CountOverflow), "CountOverflow"),
        (SubmitErrorKind::External(ExternalPushErrorKind::LinkMissing), "LinkMissing"),
        (SubmitErrorKind::LocalQueue(QueuePushErrorKind::Closed), "Closed"), (SubmitErrorKind::LocalQueue(QueuePushErrorKind::WrongOwner), "WrongOwner"),
        (SubmitErrorKind::LocalQueue(QueuePushErrorKind::LinkMissing), "LinkMissing"), (SubmitErrorKind::LocalQueue(QueuePushErrorKind::AlreadyLinked), "AlreadyLinked"),
        (SubmitErrorKind::LocalQueue(QueuePushErrorKind::CountOverflow), "CountOverflow"),
        (SubmitErrorKind::Idle(IdleQueueError::Closed), "Closed"), (SubmitErrorKind::Idle(IdleQueueError::Busy), "Busy"),
        (SubmitErrorKind::Idle(IdleQueueError::Capacity), "Capacity"), (SubmitErrorKind::Idle(IdleQueueError::CountOverflow), "CountOverflow"),
        (SubmitErrorKind::Idle(IdleQueueError::Backend(pi_result::Error::new(ErrorKind::Dependency).attach("空闲原附件（original idle attachment）"))), "Backend"),
        (SubmitErrorKind::Timer(TimerQueueError::Closed), "Closed"), (SubmitErrorKind::Timer(TimerQueueError::Busy), "Busy"),
        (SubmitErrorKind::Timer(TimerQueueError::WrongQueue), "WrongQueue"), (SubmitErrorKind::Timer(TimerQueueError::WrongOwner), "WrongOwner"),
        (SubmitErrorKind::Timer(TimerQueueError::InvalidTime), "InvalidTime"), (SubmitErrorKind::Timer(TimerQueueError::Capacity), "Capacity"),
        (SubmitErrorKind::Timer(TimerQueueError::CountOverflow), "CountOverflow"), (SubmitErrorKind::Timer(TimerQueueError::KeyExhausted), "KeyExhausted"),
        (SubmitErrorKind::Timer(TimerQueueError::Backend(pi_result::Error::new(ErrorKind::Dependency).attach("计时原附件（original timer attachment）"))), "Backend"),
    ] {
        let text = format!("{error:?}");
        assert!(text.contains(name), "{text}");
        let prefix = match &error {
            SubmitErrorKind::External(_) => Some("External"), SubmitErrorKind::LocalQueue(_) => Some("LocalQueue"),
            SubmitErrorKind::LocalContext(_) => Some("LocalContext"), SubmitErrorKind::Idle(_) => Some("Idle"),
            SubmitErrorKind::Timer(_) => Some("Timer"), _ => None,
        };
        if let Some(prefix) = prefix { assert!(text.contains(prefix), "{text}"); }
        match &error {
            SubmitErrorKind::Idle(IdleQueueError::Backend(report)) => {
                assert!(text.contains("空闲原附件"));
                assert_eq!(report.downcast_ref::<&'static str>(), Some(&"空闲原附件（original idle attachment）"));
            }
            SubmitErrorKind::Timer(TimerQueueError::Backend(report)) => {
                assert!(text.contains("计时原附件"));
                assert_eq!(report.downcast_ref::<&'static str>(), Some(&"计时原附件（original timer attachment）"));
            }
            _ => {}
        }
        assert!(write!(&mut RejectOutput, "{error:?}").is_err());
    }
}

/// 验证 SubmitErrorKind::Display 对全部拒绝原因（rejection causes）说明未接收事实（not accepted），不把繁忙（busy）解释为取消（cancellation）。
/// 本地队列（local queue）的所属域（owner domain）与计时来源（timer source）的所属线程（owner thread）／上下文（context）分别核对。
#[test]
fn test_submit_error_display_explains_every_rejection_without_cancellation() {
    for (error, part) in [
        (SubmitErrorKind::WrongRuntime, "另一运行时"), (SubmitErrorKind::LocalSubmissionRequired, "本地提交入口"),
        (SubmitErrorKind::OrdinarySubmissionRequired, "普通提交入口"), (SubmitErrorKind::WrongOwner, "所属域"),
        (SubmitErrorKind::LocalContext(LocalRuntimeAccessError::NoLocalRuntime), "没有当前"),
        (SubmitErrorKind::LocalContext(LocalRuntimeAccessError::TypeMismatch), "不匹配"),
        (SubmitErrorKind::LocalContext(LocalRuntimeAccessError::AccessRevoked), "撤销"),
        (SubmitErrorKind::LocalContext(LocalRuntimeAccessError::ContextUnavailable), "不可访问"),
        (SubmitErrorKind::External(ExternalPushErrorKind::Busy), "繁忙"), (SubmitErrorKind::External(ExternalPushErrorKind::Closed), "关闭"),
        (SubmitErrorKind::External(ExternalPushErrorKind::Capacity), "容量"), (SubmitErrorKind::External(ExternalPushErrorKind::CountOverflow), "计数"),
        (SubmitErrorKind::External(ExternalPushErrorKind::LinkMissing), "链接"),
        (SubmitErrorKind::LocalQueue(QueuePushErrorKind::Closed), "关闭"), (SubmitErrorKind::LocalQueue(QueuePushErrorKind::WrongOwner), "所属域"),
        (SubmitErrorKind::LocalQueue(QueuePushErrorKind::LinkMissing), "链接"), (SubmitErrorKind::LocalQueue(QueuePushErrorKind::AlreadyLinked), "链接已被占用"),
        (SubmitErrorKind::LocalQueue(QueuePushErrorKind::CountOverflow), "计数"),
        (SubmitErrorKind::Idle(IdleQueueError::Closed), "关闭"), (SubmitErrorKind::Idle(IdleQueueError::Busy), "繁忙"),
        (SubmitErrorKind::Idle(IdleQueueError::Capacity), "容量"), (SubmitErrorKind::Idle(IdleQueueError::CountOverflow), "计数"),
        (SubmitErrorKind::Idle(IdleQueueError::Backend(pi_result::Error::new(ErrorKind::Dependency).attach("空闲诊断（idle diagnostic）"))), "来源失败"),
        (SubmitErrorKind::Timer(TimerQueueError::Closed), "关闭"), (SubmitErrorKind::Timer(TimerQueueError::Busy), "繁忙"),
        (SubmitErrorKind::Timer(TimerQueueError::WrongQueue), "队列不匹配"), (SubmitErrorKind::Timer(TimerQueueError::WrongOwner), "所属线程"),
        (SubmitErrorKind::Timer(TimerQueueError::InvalidTime), "时间"), (SubmitErrorKind::Timer(TimerQueueError::Capacity), "容量"),
        (SubmitErrorKind::Timer(TimerQueueError::CountOverflow), "计数"), (SubmitErrorKind::Timer(TimerQueueError::KeyExhausted), "键"),
        (SubmitErrorKind::Timer(TimerQueueError::Backend(pi_result::Error::new(ErrorKind::Dependency).attach("计时诊断（timer diagnostic）"))), "来源失败"),
    ] {
        let text = error.to_string();
        assert!(text.contains("未接收") && text.contains(part), "{text}");
        assert!(!text.contains("取消"), "{text}");
        if matches!(&error, SubmitErrorKind::Timer(TimerQueueError::WrongOwner)) {
            assert!(text.contains("上下文")); assert!(!text.contains("所属域"));
        }
        match &error {
            SubmitErrorKind::Idle(IdleQueueError::Backend(report)) => assert_eq!(report.current_context(), &ErrorKind::Dependency),
            SubmitErrorKind::Timer(TimerQueueError::Backend(report)) => assert_eq!(report.current_context(), &ErrorKind::Dependency),
            _ => {}
        }
        assert!(write!(&mut RejectOutput, "{error}").is_err());
    }
}
