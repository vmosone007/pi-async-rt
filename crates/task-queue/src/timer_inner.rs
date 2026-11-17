// 当前文件仅落实配对声明中的错误报告（Report）转换，不接管定时来源或原值。

#[allow(unused_variables)]
fn timer_error_report_inner(cause: TimerQueueError) -> pi_result::Error {
    let (kind, reason) = match cause {
        TimerQueueError::Closed => (pi_result::ErrorKind::InvalidState,
            "定时来源（timer source）已停止接收或操作已关闭：Closed"),
        TimerQueueError::Busy => (pi_result::ErrorKind::Busy,
            "定时来源（timer source）的本次访问权暂不可取得：Busy"),
        TimerQueueError::WrongQueue => (pi_result::ErrorKind::InvalidInput,
            "登记键（registration key）属于其它定时来源（timer source）：WrongQueue"),
        TimerQueueError::WrongOwner => (pi_result::ErrorKind::InvalidState,
            "本地操作不在合法所属线程（owner thread）或上下文（context）：WrongOwner"),
        TimerQueueError::InvalidTime => (pi_result::ErrorKind::InvalidInput,
            "时间域（time domain）、可表示范围或单调推进条件不满足：InvalidTime"),
        TimerQueueError::Capacity => (pi_result::ErrorKind::ResourceExhausted,
            "定时来源（timer source）已达到声明的容量（capacity）限制：Capacity"),
        TimerQueueError::CountOverflow => (pi_result::ErrorKind::ResourceExhausted,
            "登记数量（registration count）继续增加将溢出：CountOverflow"),
        TimerQueueError::KeyExhausted => (pi_result::ErrorKind::ResourceExhausted,
            "无法取得可靠的独立登记键（registration key）：KeyExhausted"),
        TimerQueueError::Backend(report) => return report,
    };
    // 具体原因作为附件（attachment）保留，不虚构第三方来源或增加错误上下文（error context）层。
    pi_result::Error::new(kind).attach(reason)
}
