#![cfg(all(feature = "trace", not(target_arch = "wasm32")))]

//! MET-TEST-LIFECYCLE-001／MET-L-PARENT-001：既有冻结合同的独立生命周期／重入专项。
//!
//! 父入口严格顺序运行8个隔离子场景，子场景各只初始化一次原时钟。
//! 仅消费公开指标及原runtime API，不补生产采样、通知、截止、许可或清理。
//! SDK探针只用于故障注入／实际调用观测，不是SDK聚合或OTLP运输证据。
//! 合同／三图／边界：docs/RUNTIME_METRICS_LIFECYCLE_TEST_PLAN.md。

#[path = "support/runtime_metrics_lifecycle/common.rs"]
mod common;
#[path = "support/runtime_metrics_lifecycle/state.rs"]
mod state;
#[path = "support/runtime_metrics_lifecycle/local.rs"]
mod local;
#[path = "support/runtime_metrics_lifecycle/provider.rs"]
mod provider;

use std::{process::{Child, Command, Stdio}, thread, time::{Duration, Instant}};

const CASE_ENV: &str = "PI_ASYNC_RT_METRICS_LIFECYCLE_CASE";

/// 只拥有本入口启动的Child；不凭裸PID回收其它工作，失败清理不延长成功截止。
struct OwnedChild(Child);
impl OwnedChild {
    fn wait(&mut self, deadline: Instant) {
        loop {
            assert!(Instant::now() < deadline, "生命周期场景超过成功截止");
            if let Some(status) = self.0.try_wait().unwrap() {
                assert!(Instant::now() < deadline, "不能接受过时退出观察");
                assert!(status.success(), "生命周期场景失败：{}", status);
                return;
            }
            thread::sleep(Duration::from_millis(5));
        }
    }
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(Some(_))) { return; }
        if let Err(error) = self.0.kill() { eprintln!("终止生命周期子进程失败：{}", error); }
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match self.0.try_wait() {
                Ok(Some(_)) => return,
                Err(error) => { eprintln!("回收生命周期子进程失败：{}", error); return; }
                Ok(None) => (),
            }
            if Instant::now() >= deadline { eprintln!("回收生命周期子进程超过5秒"); return; }
            thread::sleep(Duration::from_millis(5));
        }
    }
}

/// trace／serial+trace原生平台唯一入口；8场景串行、每例15秒、全目标180秒。
/// 不调用block_on、基准、旧无界目标或外部服务；父不构建runtime／初始化时钟。
#[test]
fn test_runtime_metrics_lifecycle_matrix() {
    if let Ok(case) = std::env::var(CASE_ENV) {
        let _clock = pi_async_rt::rt::startup_global_time_loop(1)
            .expect("每个隔离场景只初始化一次原时钟");
        eprintln!("指标生命周期场景：{}", case);
        match case.as_str() {
            "registry" => state::registry(),
            "snapshot" => state::snapshot(),
            "poller" => local::poller(),
            "worker" => state::worker(),
            "generic" => state::generic(),
            "initialization" => provider::initialization(),
            "initialization_panic" => provider::initialization_panic(),
            "record_panic" => provider::record_panic(),
            _ => panic!("未知生命周期场景"),
        }
        eprintln!("指标生命周期场景通过：{}", case);
        return;
    }
    let target_deadline = Instant::now() + Duration::from_secs(180);
    for case in ["registry", "snapshot", "poller", "worker", "generic", "initialization",
        "initialization_panic", "record_panic"]
    {
        let deadline = (Instant::now() + Duration::from_secs(15)).min(target_deadline);
        assert!(Instant::now() < deadline);
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "test_runtime_metrics_lifecycle_matrix", "--nocapture", "--test-threads=1"])
            .env(CASE_ENV, case).stdin(Stdio::null()).stdout(Stdio::inherit()).stderr(Stdio::inherit())
            .spawn().unwrap();
        OwnedChild(child).wait(deadline);
    }
}
