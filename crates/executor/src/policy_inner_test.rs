//! 默认空闲策略（idle policy）：只读快照判定与休眠建议。
use super::{default_idle_inner, default_sleep_inner, ExecutorSnapshot};
use crate::registry::TaskRegistry;
use pi_async_runtime_core::definitions::{IdleState, RuntimeId, RuntimeStatus};
use std::time::Duration;

/// 构造一份未选定空闲、无计数的工作快照；各用例只改相关字段。
fn snapshot<'a>(registry: &'a crate::registry::TaskRegistryHandle) -> ExecutorSnapshot<'a> {
    ExecutorSnapshot {
        runtime: RuntimeId::try_new().expect("运行时身份（runtime id）"),
        status: RuntimeStatus::Open,
        external_tasks: 0,
        internal_tasks: 0,
        ready_tasks: 0,
        external_deferred: false,
        returns_deferred: false,
        timer_registrations: 0,
        idle_tasks: 0,
        in_flight_tasks: 0,
        registry,
        can_admit_external: true,
        timer_work_pending: false,
        maintenance_pending: false,
        maintenance_ready: false,
        last_idle_state: IdleState::Unknown,
    }
}

/// 完全没有可立即工作时空闲，并按配对建议10ms休眠。
#[test]
fn test_default_policy_idles_and_suggests_ten_millis_when_nothing_is_ready() {
    let registry = TaskRegistry::new();
    let handle = registry.handle();
    let state = snapshot(&handle);
    assert!(default_idle_inner(&state), "无工作时空闲");
    assert_eq!(default_sleep_inner(&state), Some(Duration::from_millis(10)));
}

/// 线程内容器非空、或可接纳且外部有积压、或仍有立即定时／管理责任时不空闲。
#[test]
fn test_default_policy_stays_busy_for_ready_or_admissible_work() {
    let registry = TaskRegistry::new();
    let handle = registry.handle();
    let mut state = snapshot(&handle);
    state.ready_tasks = 1;
    assert!(!default_idle_inner(&state));
    assert_eq!(default_sleep_inner(&state), None);

    let mut state = snapshot(&handle);
    state.external_tasks = 3;
    assert!(!default_idle_inner(&state), "可接纳且有积压不得空闲");

    let mut state = snapshot(&handle);
    state.timer_work_pending = true;
    assert!(!default_idle_inner(&state), "存在立即定时工作不得空闲");

    let mut state = snapshot(&handle);
    state.maintenance_pending = true;
    state.maintenance_ready = true;
    assert!(!default_idle_inner(&state), "已确认可立即推进的管理责任不得空闲");
}

/// 已尝试且暂不可取的事实可以推翻总数量：延迟的内部／外部样本不迫忙等。
#[test]
fn test_default_policy_accepts_deferred_samples_as_idle() {
    let registry = TaskRegistry::new();
    let handle = registry.handle();
    let mut state = snapshot(&handle);
    state.internal_tasks = 2;
    assert!(!default_idle_inner(&state), "未标记暂不可取的内部欠账不得空闲");
    state.returns_deferred = true;
    assert!(default_idle_inner(&state), "本轮已尝试且暂不可取可空闲");

    let mut state = snapshot(&handle);
    state.external_tasks = 4;
    state.external_deferred = true;
    assert!(default_idle_inner(&state), "外部暂不可取可空闲");

    let mut state = snapshot(&handle);
    state.can_admit_external = false;
    state.external_tasks = 5;
    assert!(default_idle_inner(&state), "不可接纳时外部积压不迫忙等");

    let mut state = snapshot(&handle);
    state.maintenance_pending = true;
    assert!(default_idle_inner(&state), "等待原参与者接续不迫忙等");
    assert_eq!(default_sleep_inner(&state), Some(Duration::from_millis(10)));
}
