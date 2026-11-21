//! 执行器时钟（executor clock）对显式时间点（explicit time points）的纯运算（pure arithmetic）。
//! 本文件（file）不安装全局时钟（global clock）；各用例（test cases）不共享可变状态（mutable state）。
use pi_async_executor::{ExecutorClock, ManualClock};
use pi_clock_manual::global::{self, GlobalManualInstant};
use pi_result::ErrorKind;
use std::time::Duration;

/// 验证 ExecutorClock::checked_add 的零时长（zero duration）、纳秒精度（nanosecond precision）、
/// 最大可表示值（maximum representable value）及越界错误（overflow error）。
/// 一天时长（one-day duration）不受运行时（runtime）入口的业务限制；运算（arithmetic）前后仍未安装。
#[test]
fn test_manual_clock_checked_add_uses_explicit_time_and_rejects_overflow() {
    let clock = ManualClock;
    assert_eq!(global::try_now().unwrap_err().current_context(), &ErrorKind::InvalidState);
    let start = GlobalManualInstant::try_from_elapsed(Duration::from_nanos(7)).unwrap();
    assert_eq!(clock.checked_add(&start, Duration::ZERO).unwrap(), start);
    assert_eq!(
        clock.checked_add(&start, Duration::from_nanos(13)).unwrap().duration_since_origin(),
        Duration::from_nanos(20),
    );
    assert_eq!(
        clock.checked_add(&GlobalManualInstant::ZERO, Duration::from_secs(86_400)).unwrap().duration_since_origin(),
        Duration::from_secs(86_400),
    );
    assert_eq!(clock.checked_add(&GlobalManualInstant::MAX, Duration::ZERO).unwrap(), GlobalManualInstant::MAX);
    let before_max = GlobalManualInstant::try_from_elapsed(Duration::from_nanos(u64::MAX - 1)).unwrap();
    assert_eq!(clock.checked_add(&before_max, Duration::from_nanos(1)).unwrap(), GlobalManualInstant::MAX);
    for (start, duration) in [
        (GlobalManualInstant::MAX, Duration::from_nanos(1)),
        (GlobalManualInstant::ZERO, Duration::MAX),
    ] {
        let error = clock.checked_add(&start, duration).unwrap_err();
        assert_eq!(error.current_context(), &ErrorKind::InvalidInput);
        assert!(format!("{error:?}").contains("时间点加法超界"));
    }
    assert_eq!(global::try_now().unwrap_err().current_context(), &ErrorKind::InvalidState);
}

/// 验证 ExecutorClock::remaining 只处理显式同源时间（explicit same-timeline time）。
/// 相等或过去截止（elapsed deadline）返回零；未来截止（future deadline）保持纳秒精度（nanosecond precision）。
/// 最大跨度（maximum span）不饱和（saturate），全部路径（paths）都不安装或刷新（refresh）时钟（clock）。
#[test]
fn test_manual_clock_remaining_handles_elapsed_and_maximum_deadlines() {
    let clock = ManualClock;
    assert_eq!(global::try_now().unwrap_err().current_context(), &ErrorKind::InvalidState);
    let now = GlobalManualInstant::try_from_elapsed(Duration::from_nanos(20)).unwrap();
    let before = GlobalManualInstant::try_from_elapsed(Duration::from_nanos(7)).unwrap();
    let later = GlobalManualInstant::try_from_elapsed(Duration::from_nanos(33)).unwrap();
    assert_eq!(clock.remaining(&now, &now).unwrap(), Duration::ZERO);
    assert_eq!(clock.remaining(&now, &before).unwrap(), Duration::ZERO);
    assert_eq!(clock.remaining(&now, &later).unwrap(), Duration::from_nanos(13));
    assert_eq!(clock.remaining(&GlobalManualInstant::ZERO, &GlobalManualInstant::MAX).unwrap(), Duration::from_nanos(u64::MAX));
    assert_eq!(clock.remaining(&GlobalManualInstant::MAX, &GlobalManualInstant::ZERO).unwrap(), Duration::ZERO);
    assert_eq!(global::try_now().unwrap_err().current_context(), &ErrorKind::InvalidState);
}
