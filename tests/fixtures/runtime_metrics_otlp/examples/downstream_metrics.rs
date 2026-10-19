//! 下游完整接入示例的编译载体，不由普通验收测试自动运行。
//!
//! 独立运行会显式创建宿主运输／SDK设施和两个业务runtime，并连接指定OTLP服务；
//! 生产宿主已经存在这些设施时应复用，不照搬重复创建。本库指标入口不创建它们。
//! 主线程不推动这些Multi的worker，因此可以同步采集；不使用block_on。

use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
use pi_async_rt::{runtime_metrics::*, rt::{AsyncRuntime, multi_thread::{MultiTaskRuntimeBuilder, StealableTaskPool}}};
use std::{error::Error, thread, time::Duration};

/// 在宿主已经进入且持续运行的Tokio上下文中构建指标管道；生产中优先复用现有Provider。
/// SDK的导出线程和网络是宿主显式选择，不是pi-async-rt注册或采集的隐式副作用。
fn provider(endpoint: &str) -> Result<SdkMeterProvider, Box<dyn Error>> {
    let exporter = MetricExporter::builder().with_tonic().with_endpoint(endpoint)
        .with_timeout(Duration::from_secs(3)).with_temporality(Temporality::Cumulative).build()?;
    let reader = PeriodicReader::builder(exporter).with_interval(Duration::from_secs(3)).build();
    Ok(SdkMeterProvider::builder().with_reader(reader).build())
}

fn main() -> Result<(), Box<dyn Error>> {
    // 宿主标记须区分机器／灰度／角色／启动实例，最多256字节；本库统一追加系统PID。
    let process_instance = std::env::var("PI_RUNTIME_METRICS_PROCESS_INSTANCE")?;
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")?;
    let transport = tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all().build()?;
    let sdk = { let _entered = transport.enter(); provider(&endpoint)? };
    opentelemetry::global::set_meter_provider(sdk.clone());
    init_runtime_metrics_reporting(&process_instance)?;

    // 本独立进程只启动一次原业务时钟；已有宿主已启动时不可重复初始化。
    let _clock = pi_async_rt::rt::startup_global_time_loop(1).expect("独立示例唯一业务时钟");
    let network = MultiTaskRuntimeBuilder::new(StealableTaskPool::<()>::with(2, 4096, [1, 1], 3000))
        .init_worker_size(2).set_worker_limit(2, 2).set_timer_interval(1).build();
    let jobs = MultiTaskRuntimeBuilder::new(StealableTaskPool::<()>::with(2, 4096, [1, 1], 3000))
        .init_worker_size(2).set_worker_limit(2, 2).set_timer_interval(1).build();
    let network_id = register_runtime_metrics(&network, "gateway-network")?;
    let jobs_id = register_runtime_metrics(&jobs, "gateway-jobs")?;
    assert_eq!(register_runtime_metrics(&network.clone(), "gateway-network")?, network_id);
    // 同核第二名称仅扩展观测属性；上报仍每指标一个点，不产生第二份任务读数。
    assert_eq!(register_runtime_metrics(&network, "gateway-rpc")?, network_id);
    network.spawn(async {})?;
    jobs.spawn(async {})?;

    let options = RuntimeMetricCollectionOptions::new(Duration::from_millis(100), Duration::from_secs(3))?;
    // 这里只演示三轮。长驻宿主应在已有、可等待且不承担runtime推进的线程按周期调用。
    for _ in 0..3 {
        let report = collect_and_report_runtime_metrics(options)?;
        for instance in report.instances() {
            println!("运行时={} 首名={} 全部原名={:?} 类别={:?} 状态={} 原读数={:?}", instance.id().as_u64(),
                instance.name(), instance.names().collect::<Vec<_>>(), instance.kind(), instance.status().code(), instance.counts());
        }
        thread::sleep(Duration::from_secs(1));
    }

    // 已停止所有新轮／重登记，本例同步调用已返回；没有登记本地runtime或其它采样调用者。
    assert!(unregister_runtime_metrics(network_id));
    assert!(unregister_runtime_metrics(jobs_id));
    // 可选的退休状态记录仍走同一入口；不会把SDK中旧数量点清成零。
    let retired = collect_and_report_runtime_metrics(options)?;
    assert_eq!((retired.active_count(), retired.retired_count()), (0, 2));
    // 仅宿主退出时显式刷新／关闭，不每轮flush；运输上下文要保持到SDK完成关闭。
    sdk.force_flush()?;
    sdk.shutdown()?;
    transport.shutdown_timeout(Duration::from_secs(2));
    // Multi的原close不提供worker停止；独立示例退出进程，不伪称注销等于关闭worker。
    Ok(())
}
