//! 手推时钟（manual clock）从未安装到真实刷新（refresh）的公开行为。
//! 本文件（file）仅有一个用例（test case），独立进程（process）隔离全局安装状态（global installation state）。
use pi_async_executor::{ExecutorClock, ManualClock};
use pi_clock_manual::global::{self, GlobalManualInstant};
use pi_result::ErrorKind;
use std::time::Duration;

/// 验证 ExecutorClock::now 不隐式安装（implicit initialization），安装后返回已发布时间（published time）。
/// 等待两毫秒只让真实时间（real time）经过，不推进生产缓存（production cache）或伪造来源（source）；
/// 仅断言（assert）严格前进及同源非递减（same-timeline monotonicity），不要求精确延迟（latency）。
#[test]
fn test_manual_clock_now_refreshes_published_time() {
    let clock = ManualClock;
    let error = clock.now().unwrap_err();
    assert_eq!(error.current_context(), &ErrorKind::InvalidState);
    assert!(format!("{error:?}").contains("手推时钟尚未安装"));
    assert_eq!(global::try_now().unwrap_err().current_context(), &ErrorKind::InvalidState);

    global::try_init().unwrap();
    assert_eq!(global::try_now().unwrap(), GlobalManualInstant::ZERO);
    std::thread::sleep(Duration::from_millis(2));
    assert_eq!(global::try_now().unwrap(), GlobalManualInstant::ZERO);
    let first = clock.now().unwrap();
    assert!(first > GlobalManualInstant::ZERO);
    assert_eq!(global::try_now().unwrap(), first);
    let copied = clock;
    let second = copied.now().unwrap();
    assert!(second >= first);
    assert_eq!(global::try_now().unwrap(), second);
}
