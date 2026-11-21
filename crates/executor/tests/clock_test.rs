//! 全局时钟（global clock）的公开访问约定（public access contract）。
//! 每个用例（test case）由独立进程（process）精确执行，避免安装状态（installation state）相互影响。
use pi_async_executor::ManualClock;
use pi_clock_manual::global::{self, GlobalManualInstant};
use pi_result::ErrorKind;

/// 未安装时不得隐式安装（implicit initialization）；外部安装后重复构造（construction）
/// 只核同一时间轴（timeline），不刷新缓存（cache）或改变起点（origin）。
#[test]
fn test_manual_clock_requires_external_installation() {
    assert_eq!(global::try_now().unwrap_err().current_context(), &ErrorKind::InvalidState);
    let error = ManualClock::new().unwrap_err();
    assert_eq!(error.current_context(), &ErrorKind::InvalidState);
    assert!(format!("{error:?}").contains("手推时钟尚未安装"));
    assert_eq!(global::try_now().unwrap_err().current_context(), &ErrorKind::InvalidState);
    global::try_init().unwrap();
    let before = global::try_now().unwrap();
    assert_eq!(before, GlobalManualInstant::ZERO);
    let _clock = ManualClock::new().unwrap();
    let _same_clock = ManualClock::new().unwrap();
    assert_eq!(global::try_now().unwrap(), before);
}
