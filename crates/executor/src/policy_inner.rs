#[cfg(test)]
#[path = "policy_inner_test.rs"]
mod policy_inner_test;

fn default_idle_inner(state: &ExecutorSnapshot) -> bool {
    // 只有本轮确实没有可立即推进的工作才空闲：线程内容器须为空，内部欠账须已尝试且暂不可取，
    // 定时与管理责任不待推进，来源可按本轮事实判为暂不可用。
    state.ready_tasks == 0
        && (state.internal_tasks == 0 || state.returns_deferred)
        && !state.timer_work_pending
        && !state.maintenance_ready
        && (!state.can_admit_external || state.external_tasks == 0 || state.external_deferred)
}

fn default_sleep_inner(state: &ExecutorSnapshot) -> Option<Duration> {
    // 与默认空闲判定配对：无可立即工作时建议10ms上限，其它情况继续检查而不休眠。
    if default_idle_inner(state) {
        Some(Duration::from_millis(10))
    } else {
        None
    }
}
