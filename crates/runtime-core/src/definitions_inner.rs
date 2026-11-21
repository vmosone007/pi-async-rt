#[allow(unused_variables)]
fn runtime_id_new_inner() -> pi_result::Result<RuntimeId> {
    static LAST_RUNTIME_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    // 零仅表示尚未分配（unallocated）；受检更新（checked update）成功后才产生非零身份（nonzero identity）。
    // 最大值（maximum value）不再写回零，失败（failure）也不回收已发身份（issued identity）。
    let previous = LAST_RUNTIME_ID.try_update(
        std::sync::atomic::Ordering::Relaxed,
        std::sync::atomic::Ordering::Relaxed,
        |last| last.checked_add(1),
    ).map_err(|_| {
        pi_result::Error::new(pi_result::ErrorKind::ResourceExhausted)
            .attach("运行时身份已耗尽（runtime identifiers exhausted）")
    })?;
    Ok(RuntimeId(NonZeroU64::new(previous + 1)
        .expect("成功受检增加必定非零（a successful checked increment is nonzero）")))
}
#[allow(unused_variables)]
fn create_error_debug_inner(
    value: &CreateErrorKind,
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    match value {
        CreateErrorKind::Task(cause) => f.debug_tuple("Task").field(cause).finish(),
        CreateErrorKind::LocalContext(cause) => f.debug_tuple("LocalContext").field(cause).finish(),
        CreateErrorKind::Timer(cause) => f.debug_tuple("Timer").field(cause).finish(),
    }
}
#[allow(unused_variables)]
fn create_error_display_inner(
    value: &CreateErrorKind,
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    match value {
        CreateErrorKind::Task(cause) => {
            f.write_str("任务创建失败（task creation failed）：")?;
            match cause {
                BuildErrorKind::AllocationFailed => f.write_str("基础存储分配失败（base storage allocation failed）"),
                BuildErrorKind::LayoutOverflow => f.write_str("布局或引用容量计算溢出（layout or reference capacity overflow）"),
                BuildErrorKind::OwnerClosed => f.write_str("指定所属域已关闭（target owner domain closed）"),
                BuildErrorKind::WrongThread => f.write_str("本地创建不在所属线程（local creation outside owner thread）"),
            }
        }
        CreateErrorKind::LocalContext(cause) => write!(f, "本地创建失败（local creation failed）：{cause}"),
        CreateErrorKind::Timer(cause) => write!(f, "计时创建失败（timer creation failed）：{cause}"),
    }
}
#[allow(unused_variables)]
fn submit_error_debug_inner(
    value: &SubmitErrorKind,
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    match value {
        SubmitErrorKind::WrongRuntime => f.write_str("WrongRuntime"),
        SubmitErrorKind::LocalSubmissionRequired => f.write_str("LocalSubmissionRequired"),
        SubmitErrorKind::OrdinarySubmissionRequired => f.write_str("OrdinarySubmissionRequired"),
        SubmitErrorKind::WrongOwner => f.write_str("WrongOwner"),
        SubmitErrorKind::LocalContext(cause) => f.debug_tuple("LocalContext").field(cause).finish(),
        SubmitErrorKind::External(cause) => f.debug_tuple("External").field(cause).finish(),
        SubmitErrorKind::LocalQueue(cause) => f.debug_tuple("LocalQueue").field(cause).finish(),
        SubmitErrorKind::Idle(cause) => match cause {
            IdleQueueError::Closed => f.write_str("Idle(Closed)"),
            IdleQueueError::Busy => f.write_str("Idle(Busy)"),
            IdleQueueError::Capacity => f.write_str("Idle(Capacity)"),
            IdleQueueError::CountOverflow => f.write_str("Idle(CountOverflow)"),
            IdleQueueError::Backend(report) => f.debug_tuple("Idle::Backend").field(report).finish(),
        },
        SubmitErrorKind::Timer(cause) => match cause {
            TimerQueueError::Closed => f.write_str("Timer(Closed)"),
            TimerQueueError::Busy => f.write_str("Timer(Busy)"),
            TimerQueueError::WrongQueue => f.write_str("Timer(WrongQueue)"),
            TimerQueueError::WrongOwner => f.write_str("Timer(WrongOwner)"),
            TimerQueueError::InvalidTime => f.write_str("Timer(InvalidTime)"),
            TimerQueueError::Capacity => f.write_str("Timer(Capacity)"),
            TimerQueueError::CountOverflow => f.write_str("Timer(CountOverflow)"),
            TimerQueueError::KeyExhausted => f.write_str("Timer(KeyExhausted)"),
            TimerQueueError::Backend(report) => f.debug_tuple("Timer::Backend").field(report).finish(),
        },
    }
}
#[allow(unused_variables)]
fn submit_error_display_inner(
    value: &SubmitErrorKind,
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    f.write_str("任务未接收（task not accepted）：")?;
    match value {
        SubmitErrorKind::WrongRuntime => f.write_str("原任务属于另一运行时（task belongs to another runtime）"),
        SubmitErrorKind::LocalSubmissionRequired => f.write_str("须使用原所属线程的本地提交入口（owner-thread local submission required）"),
        SubmitErrorKind::OrdinarySubmissionRequired => f.write_str("须使用普通提交入口或空闲来源（ordinary submission or idle source required）"),
        SubmitErrorKind::WrongOwner => f.write_str("原任务所属域不匹配（task owner domain mismatch）"),
        SubmitErrorKind::LocalContext(cause) => write!(f, "本地访问失败（local access failed）：{cause}"),
        SubmitErrorKind::External(cause) => {
            f.write_str("外部来源拒绝（external source rejection）：")?;
            match cause {
                ExternalPushErrorKind::Busy => f.write_str("来源繁忙（source busy）"),
                ExternalPushErrorKind::Closed => f.write_str("来源已关闭（source closed）"),
                ExternalPushErrorKind::Capacity => f.write_str("容量不足（capacity exhausted）"),
                ExternalPushErrorKind::CountOverflow => f.write_str("计数将溢出（count would overflow）"),
                ExternalPushErrorKind::LinkMissing => f.write_str("缺少所需链接（required link missing）"),
            }
        }
        SubmitErrorKind::LocalQueue(cause) => {
            f.write_str("本地队列拒绝（local queue rejection）：")?;
            match cause {
                QueuePushErrorKind::Closed => f.write_str("队列已关闭（queue closed）"),
                QueuePushErrorKind::WrongOwner => f.write_str("任务未绑定本队列所属域或已绑定其它域（task unbound or bound to another owner domain）"),
                QueuePushErrorKind::LinkMissing => f.write_str("缺少所需链接（required link missing）"),
                QueuePushErrorKind::AlreadyLinked => f.write_str("链接已被占用（link already in use）"),
                QueuePushErrorKind::CountOverflow => f.write_str("计数将溢出（count would overflow）"),
            }
        }
        SubmitErrorKind::Idle(cause) => {
            f.write_str("空闲来源拒绝（idle source rejection）：")?;
            match cause {
                IdleQueueError::Closed => f.write_str("来源已关闭（source closed）"),
                IdleQueueError::Busy => f.write_str("来源繁忙（source busy）"),
                IdleQueueError::Capacity => f.write_str("容量不足（capacity exhausted）"),
                IdleQueueError::CountOverflow => f.write_str("计数将溢出（count would overflow）"),
                IdleQueueError::Backend(report) => write!(f, "来源失败（backend failure）：{report}"),
            }
        }
        SubmitErrorKind::Timer(cause) => {
            f.write_str("计时来源拒绝（timer source rejection）：")?;
            match cause {
                TimerQueueError::Closed => f.write_str("来源已关闭（source closed）"),
                TimerQueueError::Busy => f.write_str("来源繁忙（source busy）"),
                TimerQueueError::WrongQueue => f.write_str("队列不匹配（wrong queue）"),
                TimerQueueError::WrongOwner => f.write_str("不在合法所属线程（owner thread）或上下文（context）执行"),
                TimerQueueError::InvalidTime => f.write_str("时间域、范围或单调推进条件不满足（invalid time domain, range or monotonic progress）"),
                TimerQueueError::Capacity => f.write_str("容量不足（capacity exhausted）"),
                TimerQueueError::CountOverflow => f.write_str("计数将溢出（count would overflow）"),
                TimerQueueError::KeyExhausted => f.write_str("登记键耗尽（registration keys exhausted）"),
                TimerQueueError::Backend(report) => write!(f, "来源失败（backend failure）：{report}"),
            }
        }
    }
}
#[allow(unused_variables)]
fn timer_error_debug_inner(
    value: &TimerError,
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    match value {
        TimerError::InvalidDuration { value, min_positive, max } => f.debug_struct("InvalidDuration")
            .field("value", value).field("min_positive", min_positive).field("max", max).finish(),
        TimerError::Clock(report) => f.debug_tuple("Clock").field(report).finish(),
        TimerError::DeadlineOutOfRange(report) => f.debug_tuple("DeadlineOutOfRange").field(report).finish(),
        TimerError::RegistrationPreparation(report) => f.debug_tuple("RegistrationPreparation").field(report).finish(),
        TimerError::Queue(cause) => match cause {
            TimerQueueError::Closed => f.write_str("Queue(Closed)"),
            TimerQueueError::Busy => f.write_str("Queue(Busy)"),
            TimerQueueError::WrongQueue => f.write_str("Queue(WrongQueue)"),
            TimerQueueError::WrongOwner => f.write_str("Queue(WrongOwner)"),
            TimerQueueError::InvalidTime => f.write_str("Queue(InvalidTime)"),
            TimerQueueError::Capacity => f.write_str("Queue(Capacity)"),
            TimerQueueError::CountOverflow => f.write_str("Queue(CountOverflow)"),
            TimerQueueError::KeyExhausted => f.write_str("Queue(KeyExhausted)"),
            TimerQueueError::Backend(report) => f.debug_tuple("Queue::Backend").field(report).finish(),
        },
        TimerError::Cancelled => f.write_str("Cancelled"),
        TimerError::RegistrationRemoved => f.write_str("RegistrationRemoved"),
        TimerError::ServiceClosed => f.write_str("ServiceClosed"),
    }
}
#[allow(unused_variables)]
fn timer_error_display_inner(
    value: &TimerError,
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    match value {
        TimerError::InvalidDuration { value, min_positive, max } => {
            write!(f, "计时时长无效（invalid timer duration）：{value:?}，最小正时长（minimum positive duration）{min_positive:?}，")?;
            match max {
                Some(max) => write!(f, "最大时长（maximum duration）{max:?}"),
                None => f.write_str("无固定上限（no fixed maximum）"),
            }
        }
        TimerError::Clock(report) => write!(f, "时钟失败（clock failure）：{report}"),
        TimerError::DeadlineOutOfRange(report) => write!(f, "截止时间越界（deadline out of range）：{report}"),
        TimerError::RegistrationPreparation(report) => write!(f, "计时登记准备失败（timer registration preparation failed）：{report}"),
        TimerError::Queue(cause) => {
            f.write_str("计时来源拒绝（timer source rejection）：")?;
            match cause {
                TimerQueueError::Closed => f.write_str("来源已关闭（source closed）"),
                TimerQueueError::Busy => f.write_str("来源繁忙（source busy）"),
                TimerQueueError::WrongQueue => f.write_str("队列不匹配（wrong queue）"),
                TimerQueueError::WrongOwner => f.write_str("不在合法所属线程（owner thread）或上下文（context）执行"),
                TimerQueueError::InvalidTime => f.write_str("时间无效（invalid time）"),
                TimerQueueError::Capacity => f.write_str("容量不足（capacity exhausted）"),
                TimerQueueError::CountOverflow => f.write_str("计数越界（count overflow）"),
                TimerQueueError::KeyExhausted => f.write_str("登记键耗尽（registration keys exhausted）"),
                TimerQueueError::Backend(report) => write!(f, "来源失败（backend failure）：{report}"),
            }
        }
        TimerError::Cancelled => f.write_str("计时等待已取消（timer wait cancelled）"),
        TimerError::RegistrationRemoved => f.write_str("计时登记已移除（timer registration removed）"),
        TimerError::ServiceClosed => f.write_str("运行时服务已关闭（runtime service closed）"),
    }
}
#[allow(unused_variables)]
fn timeout_error_debug_inner(
    value: &TimeoutError,
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    match value {
        TimeoutError::Elapsed => f.write_str("Elapsed"),
        TimeoutError::Timer(cause) => f.debug_tuple("Timer").field(cause).finish(),
    }
}
#[allow(unused_variables)]
fn timeout_error_display_inner(
    value: &TimeoutError,
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    match value {
        TimeoutError::Elapsed => f.write_str("等待已到期（wait elapsed）"),
        TimeoutError::Timer(cause) => write!(f, "计时设施失败（timer facility failure）：{cause}"),
    }
}
#[allow(unused_variables)]
fn wait_timeout_error_debug_inner(
    value: &WaitTimeoutError,
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    match value {
        WaitTimeoutError::Wait(cause) => f.debug_tuple("Wait").field(cause).finish(),
        WaitTimeoutError::Elapsed => f.write_str("Elapsed"),
        WaitTimeoutError::Timer(cause) => f.debug_tuple("Timer").field(cause).finish(),
    }
}
#[allow(unused_variables)]
fn wait_timeout_error_display_inner(
    value: &WaitTimeoutError,
    f: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    match value {
        WaitTimeoutError::Wait(cause) => write!(f, "交值等待失败（value wait failed）：{cause}"),
        WaitTimeoutError::Elapsed => f.write_str("等待已到期（wait elapsed）"),
        WaitTimeoutError::Timer(cause) => write!(f, "计时设施失败（timer facility failure）：{cause}"),
    }
}
#[allow(unused_variables)]
fn timer_error_source_inner(value: &TimerError) -> Option<&(dyn std::error::Error + 'static)> {
    match value {
        TimerError::Clock(report)
        | TimerError::DeadlineOutOfRange(report)
        | TimerError::RegistrationPreparation(report)
        | TimerError::Queue(TimerQueueError::Backend(report)) => Some(report.as_error()),
        _ => None,
    }
}
#[allow(unused_variables)]
fn timer_error_report_inner(value: TimerError) -> pi_result::Error {
    let kind = match value {
        TimerError::Clock(report)
        | TimerError::DeadlineOutOfRange(report)
        | TimerError::RegistrationPreparation(report)
        | TimerError::Queue(TimerQueueError::Backend(report)) => return report,
        TimerError::InvalidDuration { .. }
        | TimerError::Queue(TimerQueueError::WrongQueue | TimerQueueError::InvalidTime) => pi_result::ErrorKind::InvalidInput,
        TimerError::Cancelled => pi_result::ErrorKind::Cancelled,
        TimerError::RegistrationRemoved | TimerError::ServiceClosed
        | TimerError::Queue(TimerQueueError::Closed | TimerQueueError::WrongOwner) => pi_result::ErrorKind::InvalidState,
        TimerError::Queue(TimerQueueError::Busy) => pi_result::ErrorKind::Busy,
        TimerError::Queue(TimerQueueError::Capacity | TimerQueueError::CountOverflow | TimerQueueError::KeyExhausted) => pi_result::ErrorKind::ResourceExhausted,
    };
    // 原报告（original report）已直接归还；有限原因（finite cause）保留类型化原值（typed original value），不以文本（text）取代。
    pi_result::Error::new(kind).attach(value)
}
