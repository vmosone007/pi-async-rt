//! 固定接纳规则（admission rules）的公开独立样本（independent samples），不代替服务推进（service drive）。
use pi_async_executor::drive::{AdmissionRule, AdmissionState};
use pi_async_runtime::{ConcurrentMode, RuntimeId, RuntimeStatus, SerialMode};

/// 验证 SerialMode 仅开放（Open）且两项数量（counts）均零时接纳下一批（admit next batch）。
/// 27种组合含最大计数（maximum count），不以加法（addition）合并计数，避免溢出（overflow）改变判断。
#[test]
fn test_serial_mode_waits_for_internal_and_in_flight_completion() {
    let runtime = RuntimeId::try_new().unwrap();
    for status in [RuntimeStatus::Open, RuntimeStatus::Closing, RuntimeStatus::Closed] {
        for (internal_tasks, in_flight_tasks, open_allowed) in [
            (0, 0, true), (0, 1, false), (0, usize::MAX, false),
            (1, 0, false), (1, 1, false), (1, usize::MAX, false),
            (usize::MAX, 0, false), (usize::MAX, 1, false), (usize::MAX, usize::MAX, false),
        ] {
            let state = AdmissionState { runtime, status, internal_tasks, in_flight_tasks };
            let expected = if status == RuntimeStatus::Open { open_allowed } else { false };
            assert_eq!(SerialMode.admits_external(&state), expected, "{state:?}");
        }
    }
}

/// 验证 ConcurrentMode 仅依据开放状态（Open status），纯挂起（Pending）或在途（in-flight）不阻止普通获取（ordinary take）。
/// 27种状态与数量组合（state/count combinations）不改变输入（input），关闭（closing/closed）均拒绝。
#[test]
fn test_concurrent_mode_keeps_admitting_open_service_with_pending_tasks() {
    let runtime = RuntimeId::try_new().unwrap();
    for (status, expected) in [(RuntimeStatus::Open, true), (RuntimeStatus::Closing, false), (RuntimeStatus::Closed, false)] {
        for internal_tasks in [0, 1, usize::MAX] {
            for in_flight_tasks in [0, 1, usize::MAX] {
                let state = AdmissionState { runtime, status, internal_tasks, in_flight_tasks };
                assert_eq!(ConcurrentMode.admits_external(&state), expected, "{state:?}");
            }
        }
    }
}
