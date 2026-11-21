//! 计时错误（timer error）的原报告借用链（borrowed report chain）。
use pi_async_runtime_core::TimerError;
use pi_async_task_queue::TimerQueueError;
use pi_result::{ErrorKind, error_stack::Report};
use std::error::Error;
use std::time::Duration;

/// 验证 TimerError::source 仅借用四类真实报告（reports），保持同一错误对象（error object）与附件（attachment）。
/// 所有限定原因（finite causes）返回 None；不访问定时来源（timer source）或执行等待（wait）。
#[test]
fn test_timer_error_source_borrows_only_original_reports() {
    for wrap in [
        TimerError::Clock as fn(pi_result::Error) -> TimerError,
        TimerError::DeadlineOutOfRange,
        TimerError::RegistrationPreparation,
        |report| TimerError::Queue(TimerQueueError::Backend(report)),
    ] {
        let report = Report::new(std::io::Error::from_raw_os_error(123))
            .change_context(ErrorKind::Dependency)
            .attach(String::from("原计时来源附件（original timer-source attachment）"));
        let error = wrap(report);
        let report = match &error {
            TimerError::Clock(report) | TimerError::DeadlineOutOfRange(report)
                | TimerError::RegistrationPreparation(report)
                | TimerError::Queue(TimerQueueError::Backend(report)) => report,
            _ => unreachable!(),
        };
        let expected: &(dyn Error + 'static) = report.as_error();
        let source = error.source().unwrap();
        // 数据地址（data address）标识原借用对象（borrowed object）；虚函数表（vtable）不保证跨编译单元（codegen units）唯一。
        assert!(std::ptr::addr_eq(source, expected));
        assert_eq!(report.current_context(), &ErrorKind::Dependency);
        assert_eq!(report.downcast_ref::<std::io::Error>().unwrap().raw_os_error(), Some(123));
        assert_eq!(report.downcast_ref::<String>().unwrap(), "原计时来源附件（original timer-source attachment）");
    }
    for error in [
        TimerError::InvalidDuration { value: Duration::ZERO, min_positive: Duration::from_millis(1), max: None },
        TimerError::Cancelled, TimerError::RegistrationRemoved, TimerError::ServiceClosed,
        TimerError::Queue(TimerQueueError::Closed), TimerError::Queue(TimerQueueError::Busy),
        TimerError::Queue(TimerQueueError::WrongQueue), TimerError::Queue(TimerQueueError::WrongOwner),
        TimerError::Queue(TimerQueueError::InvalidTime), TimerError::Queue(TimerQueueError::Capacity),
        TimerError::Queue(TimerQueueError::CountOverflow), TimerError::Queue(TimerQueueError::KeyExhausted),
    ] {
        assert!(error.source().is_none());
    }
}
