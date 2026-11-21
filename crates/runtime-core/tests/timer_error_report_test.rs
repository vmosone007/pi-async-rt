//! 计时错误（timer error）的消耗式报告转换（consuming report conversion）。
use pi_async_runtime_core::TimerError;
use pi_async_task_queue::TimerQueueError;
use pi_result::{error_stack::Report, ErrorKind};
use std::time::Duration;

/// 验证 TimerError::into_report 对四种原报告（original reports）保持原帧（frames）、原错误（original error）与附件（attachments）。
/// 有限分类（finite classifications）映射稳定公共原因（public causes），取消（cancellation）与其它移除（removal）不混淆。
#[test]
fn test_timer_error_into_report_preserves_originals_and_finite_causes() {
    for wrap in [TimerError::Clock as fn(pi_result::Error) -> TimerError, TimerError::DeadlineOutOfRange,
        TimerError::RegistrationPreparation, |report| TimerError::Queue(TimerQueueError::Backend(report))] {
        let report = Report::new(std::io::Error::from_raw_os_error(123))
            .change_context(ErrorKind::Dependency).attach(String::from("原报告附件（original report attachment）"));
        let error_address = report.downcast_ref::<std::io::Error>().unwrap() as *const std::io::Error;
        let attachment_address = report.downcast_ref::<String>().unwrap() as *const String;
        let frame_count = report.frames().count();
        let converted = wrap(report).into_report();
        assert_eq!(converted.current_context(), &ErrorKind::Dependency);
        assert_eq!(converted.frames().count(), frame_count);
        let original = converted.downcast_ref::<std::io::Error>().unwrap();
        assert_eq!(original.raw_os_error(), Some(123));
        assert_eq!(original as *const std::io::Error, error_address);
        let attachment = converted.downcast_ref::<String>().unwrap();
        assert_eq!(attachment, "原报告附件（original report attachment）");
        assert_eq!(attachment as *const String, attachment_address);
    }
    for (error, kind, reason) in [
        (TimerError::InvalidDuration { value: Duration::from_nanos(7), min_positive: Duration::from_millis(1), max: Some(Duration::from_secs(1)) }, ErrorKind::InvalidInput, "InvalidDuration"),
        (TimerError::Cancelled, ErrorKind::Cancelled, "Cancelled"),
        (TimerError::RegistrationRemoved, ErrorKind::InvalidState, "RegistrationRemoved"),
        (TimerError::ServiceClosed, ErrorKind::InvalidState, "ServiceClosed"),
        (TimerError::Queue(TimerQueueError::Closed), ErrorKind::InvalidState, "Closed"),
        (TimerError::Queue(TimerQueueError::Busy), ErrorKind::Busy, "Busy"),
        (TimerError::Queue(TimerQueueError::WrongQueue), ErrorKind::InvalidInput, "WrongQueue"),
        (TimerError::Queue(TimerQueueError::WrongOwner), ErrorKind::InvalidState, "WrongOwner"),
        (TimerError::Queue(TimerQueueError::InvalidTime), ErrorKind::InvalidInput, "InvalidTime"),
        (TimerError::Queue(TimerQueueError::Capacity), ErrorKind::ResourceExhausted, "Capacity"),
        (TimerError::Queue(TimerQueueError::CountOverflow), ErrorKind::ResourceExhausted, "CountOverflow"),
        (TimerError::Queue(TimerQueueError::KeyExhausted), ErrorKind::ResourceExhausted, "KeyExhausted"),
    ] {
        let converted = error.into_report();
        assert_eq!(converted.current_context(), &kind);
        let original = converted.downcast_ref::<TimerError>().expect("保留有限错误原值（preserved finite error value）");
        let detail = format!("{original:?}");
        assert!(detail.contains(reason));
        assert!(converted.downcast_ref::<std::io::Error>().is_none());
        if reason == "InvalidDuration" {
            for part in ["7ns", "1ms", "1s"] { assert!(detail.contains(part)); }
        }
    }
}
