//! 经公开错误转换（error conversion）验证分类、具体诊断及底层报告（Report）的原样交接。
//! 不构造定时来源（timer source），不把本目标通过扩大为队列（queue）功能通过。
use pi_async_task_queue::TimerQueueError;
use pi_result::{Error, ErrorInspectExt, ErrorKind};

/// @api TimerQueueError::into_report；八种非后端原因须保留各自可读说明，不能仅返回公共分类。
#[test]
fn test_timer_errors_keep_classification_and_distinct_reasons() {
    let cases = [
        (TimerQueueError::Closed, ErrorKind::InvalidState, "Closed"),
        (TimerQueueError::Busy, ErrorKind::Busy, "Busy"),
        (TimerQueueError::WrongQueue, ErrorKind::InvalidInput, "WrongQueue"),
        (TimerQueueError::WrongOwner, ErrorKind::InvalidState, "WrongOwner"),
        (TimerQueueError::InvalidTime, ErrorKind::InvalidInput, "InvalidTime"),
        (TimerQueueError::Capacity, ErrorKind::ResourceExhausted, "Capacity"),
        (TimerQueueError::CountOverflow, ErrorKind::ResourceExhausted, "CountOverflow"),
        (TimerQueueError::KeyExhausted, ErrorKind::ResourceExhausted, "KeyExhausted"),
    ];
    let mut reasons = Vec::new();
    for (cause, expected, name) in cases {
        let report = cause.into_report();
        assert_eq!(report.current_kind(), &expected, "{name}");
        assert_eq!(report.frames().filter(|frame| frame.is::<ErrorKind>()).count(), 1);
        let reason = report.downcast_ref::<&'static str>().copied()
            .expect("具体原因须作为可读附件（attachment）保留");
        assert!(reason.contains(name), "具体诊断（diagnostic）必须标明原分类：{name}");
        assert!(reason.chars().any(|character| ('一'..='鿿').contains(&character)));
        assert!(!reasons.contains(&reason), "不同原因不能使用同一说明");
        assert!(!report.contains::<std::io::Error>());
        assert!(format!("{report:?}").contains(reason));
        reasons.push(reason);
    }
}

/// @api TimerQueueError::Backend/into_report；原来源、附件（attachment）、帧（frame）身份和层数完整保留。
#[test]
fn test_backend_report_moves_without_rewrapping_or_losing_attachments() {
    let original: Error = pi_result::error_stack::Report::new(std::io::Error::from_raw_os_error(123))
        .change_context(ErrorKind::Dependency)
        .attach(String::from("原后端诊断（backend diagnostic）"))
        .attach(41u64);
    let source = original.downcast_ref::<std::io::Error>().unwrap() as *const _;
    let attachment = original.downcast_ref::<String>().unwrap() as *const _;
    let frames: Vec<_> = original.frames().map(|frame| frame as *const _).collect();
    let returned = TimerQueueError::Backend(original).into_report();
    assert_eq!(returned.current_kind(), &ErrorKind::Dependency);
    assert_eq!(returned.downcast_ref::<std::io::Error>().unwrap().raw_os_error(), Some(123));
    assert_eq!(returned.downcast_ref::<std::io::Error>().unwrap() as *const _, source);
    assert_eq!(returned.downcast_ref::<String>().unwrap() as *const _, attachment);
    assert_eq!(returned.downcast_ref::<u64>(), Some(&41));
    assert_eq!(returned.frames().map(|frame| frame as *const _).collect::<Vec<_>>(), frames);
}
