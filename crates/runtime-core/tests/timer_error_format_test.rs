//! 计时错误（timer errors）格式化（formatting）保留真实分类（classification）与原报告（Report）。
use pi_async_runtime_core::TimerError;
use pi_async_task_queue::TimerQueueError;
use pi_result::ErrorKind;
use std::fmt::{self, Write};
use std::time::Duration;

/// 拒绝所有输出（output）的标准写入器（writer），用于验证格式化失败（formatting failure）原样返回。
struct RejectOutput;
impl Write for RejectOutput {
    fn write_str(&mut self, _: &str) -> fmt::Result { Err(fmt::Error) }
}

/// 验证 TimerError::Debug 穷举八种分类（classifications），保留时长（duration）、队列原因（queue cause）与原附件（attachment）。
/// 不把报告（Report）转成替代存储（replacement storage），格式化（formatting）后原报告（Report）仍可读取。
#[test]
fn test_timer_error_debug_keeps_variants_values_and_reports() {
    let duration = TimerError::InvalidDuration {
        value: Duration::from_nanos(7), min_positive: Duration::from_millis(1), max: Some(Duration::from_secs(1)),
    };
    let text = format!("{duration:?}");
    for part in ["InvalidDuration", "value", "7ns", "min_positive", "1ms", "max", "1s"] {
        assert!(text.contains(part), "{text}");
    }
    for (error, name) in [
        (TimerError::Clock(pi_result::Error::new(ErrorKind::Dependency).attach("原计时诊断（original timer diagnostic）")), "Clock"),
        (TimerError::DeadlineOutOfRange(pi_result::Error::new(ErrorKind::InvalidInput)), "DeadlineOutOfRange"),
        (TimerError::RegistrationPreparation(pi_result::Error::new(ErrorKind::ResourceExhausted)), "RegistrationPreparation"),
        (TimerError::Cancelled, "Cancelled"), (TimerError::RegistrationRemoved, "RegistrationRemoved"),
        (TimerError::ServiceClosed, "ServiceClosed"),
    ] {
        let text = format!("{error:?}");
        assert!(text.contains(name));
        if let TimerError::Clock(report) = &error {
            assert!(text.contains("原计时诊断"));
            assert_eq!(report.current_context(), &ErrorKind::Dependency);
            assert_eq!(report.downcast_ref::<&'static str>(), Some(&"原计时诊断（original timer diagnostic）"));
        }
        assert!(write!(&mut RejectOutput, "{error:?}").is_err());
    }
    for (cause, name) in [
        (TimerQueueError::Closed, "Closed"), (TimerQueueError::Busy, "Busy"),
        (TimerQueueError::WrongQueue, "WrongQueue"), (TimerQueueError::WrongOwner, "WrongOwner"),
        (TimerQueueError::InvalidTime, "InvalidTime"), (TimerQueueError::Capacity, "Capacity"),
        (TimerQueueError::CountOverflow, "CountOverflow"), (TimerQueueError::KeyExhausted, "KeyExhausted"),
        (TimerQueueError::Backend(pi_result::Error::new(ErrorKind::Dependency).attach("队列原附件（original queue attachment）")), "Backend"),
    ] {
        let error = TimerError::Queue(cause);
        let text = format!("{error:?}");
        assert!(text.contains("Queue") && text.contains(name), "{text}");
        if let TimerError::Queue(TimerQueueError::Backend(report)) = &error {
            assert!(text.contains("队列原附件"));
            assert_eq!(report.current_context(), &ErrorKind::Dependency);
        }
        assert!(write!(&mut RejectOutput, "{error:?}").is_err());
    }
}

/// 验证 TimerError::Display 区分时长（duration）、时钟（clock）、准备（preparation）及全部来源拒绝（source rejections）。
/// 仅检查公开显示（Display）中的原因（cause）与必要载荷（payload），显示（Display）不消费原报告（Report）。
#[test]
fn test_timer_error_display_explains_causes_without_losing_reports() {
    let bounded = TimerError::InvalidDuration {
        value: Duration::from_nanos(7), min_positive: Duration::from_millis(1), max: Some(Duration::from_secs(1)),
    };
    let text = bounded.to_string();
    for part in ["时长", "7ns", "1ms", "1s"] { assert!(text.contains(part), "{text}"); }
    assert!(write!(&mut RejectOutput, "{bounded}").is_err());
    let unbounded = TimerError::InvalidDuration {
        value: Duration::ZERO, min_positive: Duration::from_millis(1), max: None,
    };
    assert!(unbounded.to_string().contains("无固定上限"));
    for (error, part) in [
        (TimerError::Clock(pi_result::Error::new(ErrorKind::Dependency)), "时钟"),
        (TimerError::DeadlineOutOfRange(pi_result::Error::new(ErrorKind::InvalidInput)), "截止时间"),
        (TimerError::RegistrationPreparation(pi_result::Error::new(ErrorKind::ResourceExhausted)), "准备"),
        (TimerError::Cancelled, "取消"), (TimerError::RegistrationRemoved, "移除"),
        (TimerError::ServiceClosed, "关闭"),
    ] {
        assert!(error.to_string().contains(part));
        assert!(write!(&mut RejectOutput, "{error}").is_err());
    }
    for (cause, part) in [
        (TimerQueueError::Closed, "关闭"), (TimerQueueError::Busy, "繁忙"),
        (TimerQueueError::WrongQueue, "队列不匹配"), (TimerQueueError::WrongOwner, "所属线程"),
        (TimerQueueError::InvalidTime, "时间无效"), (TimerQueueError::Capacity, "容量"),
        (TimerQueueError::CountOverflow, "计数"), (TimerQueueError::KeyExhausted, "键"),
        (TimerQueueError::Backend(pi_result::Error::new(ErrorKind::Dependency).attach("原来源附件（original source attachment）")), "来源失败"),
    ] {
        let error = TimerError::Queue(cause);
        let text = error.to_string();
        assert!(text.contains(part), "{text}");
        if matches!(&error, TimerError::Queue(TimerQueueError::Busy)) { assert!(!text.contains("取消")); }
        if matches!(&error, TimerError::Queue(TimerQueueError::WrongOwner)) {
            assert!(text.contains("上下文"));
            assert!(!text.contains("所属域"));
        }
        if let TimerError::Queue(TimerQueueError::Backend(report)) = &error {
            assert_eq!(report.current_context(), &ErrorKind::Dependency);
            assert_eq!(report.downcast_ref::<&'static str>(), Some(&"原来源附件（original source attachment）"));
        }
        assert!(write!(&mut RejectOutput, "{error}").is_err());
    }
}

/// 验证 CreateErrorKind::Debug 保留四种任务创建原因（task build causes）、四种本地访问原因（local access causes）及嵌套计时错误（timer error）。
/// 经公开调试格式化（Debug formatting）观察，不创建业务异步计算（Future）或访问队列（queue）。
#[test]
fn test_create_error_debug_keeps_all_sources() {
    use pi_async_runtime_core::{CreateErrorKind, LocalRuntimeAccessError};
    use pi_async_task::definitions::BuildErrorKind;
    for (cause, name) in [
        (BuildErrorKind::AllocationFailed, "AllocationFailed"), (BuildErrorKind::LayoutOverflow, "LayoutOverflow"),
        (BuildErrorKind::OwnerClosed, "OwnerClosed"), (BuildErrorKind::WrongThread, "WrongThread"),
    ] {
        let error = CreateErrorKind::Task(cause);
        let text = format!("{error:?}");
        assert!(text.contains("Task") && text.contains(name), "{text}");
        assert!(write!(&mut RejectOutput, "{error:?}").is_err());
    }
    for (cause, name) in [
        (LocalRuntimeAccessError::NoLocalRuntime, "NoLocalRuntime"), (LocalRuntimeAccessError::TypeMismatch, "TypeMismatch"),
        (LocalRuntimeAccessError::AccessRevoked, "AccessRevoked"), (LocalRuntimeAccessError::ContextUnavailable, "ContextUnavailable"),
    ] {
        let error = CreateErrorKind::LocalContext(cause);
        let text = format!("{error:?}");
        assert!(text.contains("LocalContext") && text.contains(name), "{text}");
        assert!(write!(&mut RejectOutput, "{error:?}").is_err());
    }
    let error = CreateErrorKind::Timer(TimerError::Clock(pi_result::Error::new(ErrorKind::Dependency).attach("创建原附件（original creation attachment）")));
    let text = format!("{error:?}");
    assert!(text.contains("Timer") && text.contains("Clock") && text.contains("创建原附件"));
    if let CreateErrorKind::Timer(TimerError::Clock(report)) = &error {
        assert_eq!(report.downcast_ref::<&'static str>(), Some(&"创建原附件（original creation attachment）"));
    } else { panic!("计时分类丢失（timer classification lost）"); }
    assert!(write!(&mut RejectOutput, "{error:?}").is_err());
}

/// 验证 CreateErrorKind::Display 分别说明分配（allocation）、布局（layout）、所属域（owner domain）、线程（thread）及本地访问（local access）失败。
/// 嵌套计时错误（timer error）复用原显示（Display），所有路径传播拒写失败（write failure）。
#[test]
fn test_create_error_display_explains_build_and_access_failures() {
    use pi_async_runtime_core::{CreateErrorKind, LocalRuntimeAccessError};
    use pi_async_task::definitions::BuildErrorKind;
    for (cause, part) in [
        (BuildErrorKind::AllocationFailed, "分配"), (BuildErrorKind::LayoutOverflow, "布局"),
        (BuildErrorKind::OwnerClosed, "所属域"), (BuildErrorKind::WrongThread, "所属线程"),
    ] {
        let error = CreateErrorKind::Task(cause);
        let text = error.to_string();
        assert!(text.contains("创建") && text.contains(part), "{text}");
        assert!(write!(&mut RejectOutput, "{error}").is_err());
    }
    for cause in [LocalRuntimeAccessError::NoLocalRuntime, LocalRuntimeAccessError::TypeMismatch,
        LocalRuntimeAccessError::AccessRevoked, LocalRuntimeAccessError::ContextUnavailable] {
        let expected = cause.to_string();
        let error = CreateErrorKind::LocalContext(cause);
        assert!(error.to_string().contains(&expected));
        assert!(write!(&mut RejectOutput, "{error}").is_err());
    }
    let error = CreateErrorKind::Timer(TimerError::RegistrationRemoved);
    assert!(error.to_string().contains("移除"));
    assert!(write!(&mut RejectOutput, "{error}").is_err());
}

/// 验证 TimeoutError::Debug 将正常到期（elapsed）与计时设施失败（timer failure）分开，保留原报告（Report）。
#[test]
fn test_timeout_error_debug_distinguishes_elapsed_and_timer() {
    use pi_async_runtime_core::TimeoutError;
    assert_eq!(format!("{:?}", TimeoutError::Elapsed), "Elapsed");
    assert!(write!(&mut RejectOutput, "{:?}", TimeoutError::Elapsed).is_err());
    let error = TimeoutError::Timer(TimerError::Clock(pi_result::Error::new(ErrorKind::Dependency).attach("超时原附件（original timeout attachment）")));
    let text = format!("{error:?}");
    assert!(text.contains("Timer") && text.contains("Clock") && text.contains("超时原附件"));
    assert!(!text.contains("Elapsed"));
    if let TimeoutError::Timer(TimerError::Clock(report)) = &error { assert_eq!(report.current_context(), &ErrorKind::Dependency); }
    else { panic!("计时分类丢失（timer classification lost）"); }
    assert!(write!(&mut RejectOutput, "{error:?}").is_err());
}

/// 验证 TimeoutError::Display 不将计时设施失败（timer failure）误称到期（elapsed），嵌套原显示（Display）保持。
#[test]
fn test_timeout_error_display_distinguishes_elapsed_and_timer() {
    use pi_async_runtime_core::TimeoutError;
    let elapsed = TimeoutError::Elapsed;
    assert!(elapsed.to_string().contains("到期"));
    assert!(write!(&mut RejectOutput, "{elapsed}").is_err());
    let timer = TimeoutError::Timer(TimerError::RegistrationRemoved);
    assert!(timer.to_string().contains("移除"));
    assert!(!timer.to_string().contains("到期"));
    assert!(write!(&mut RejectOutput, "{timer}").is_err());
}

/// 验证 WaitTimeoutError::Debug 区分原发送者释放（sender dropped）、到期（elapsed）与计时错误（timer error），无业务值（business value）约束。
#[test]
fn test_wait_timeout_error_debug_keeps_original_wait_failure() {
    use pi_async_runtime_core::WaitTimeoutError;
    let wait = WaitTimeoutError::Wait(pi_async_wait::WaitError::SenderDropped);
    assert_eq!(format!("{wait:?}"), "Wait(SenderDropped)");
    assert!(write!(&mut RejectOutput, "{wait:?}").is_err());
    assert_eq!(format!("{:?}", WaitTimeoutError::Elapsed), "Elapsed");
    assert!(write!(&mut RejectOutput, "{:?}", WaitTimeoutError::Elapsed).is_err());
    let timer = WaitTimeoutError::Timer(TimerError::ServiceClosed);
    assert_eq!(format!("{timer:?}"), "Timer(ServiceClosed)");
    assert!(write!(&mut RejectOutput, "{timer:?}").is_err());
}

/// 验证 WaitTimeoutError::Display 保留原交值失败说明（wait failure description），到期（elapsed）与计时关闭（timer closure）不混淆。
#[test]
fn test_wait_timeout_error_display_preserves_distinct_causes() {
    use pi_async_runtime_core::WaitTimeoutError;
    let cause = pi_async_wait::WaitError::SenderDropped;
    let expected = cause.to_string();
    let wait = WaitTimeoutError::Wait(cause);
    assert!(wait.to_string().contains(&expected));
    assert!(!wait.to_string().contains("到期"));
    let elapsed = WaitTimeoutError::Elapsed;
    assert!(elapsed.to_string().contains("到期"));
    let timer = WaitTimeoutError::Timer(TimerError::ServiceClosed);
    assert!(timer.to_string().contains("关闭"));
    assert!(!timer.to_string().contains("到期"));
    for error in [wait, elapsed, timer] { assert!(write!(&mut RejectOutput, "{error}").is_err()); }
}
