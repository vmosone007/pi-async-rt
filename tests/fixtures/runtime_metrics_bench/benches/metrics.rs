//! MET-SAFETY-PERF-001：真实公开指标、官方SDK聚合／编码及8worker空业务轻任务基准。
//!
//! 只在明确授权后运行；全部子场景串行且有资源／时间门禁。采样始终由已有外部
//! 线程调用，不补Source、通知、截止、许可或调度；观测成本及结论见正式补证合同。

mod probe;

use opentelemetry_sdk::{error::OTelSdkResult, metrics::{SdkMeterProvider, PeriodicReader,
    Temporality, data::ResourceMetrics, exporter::PushMetricExporter}};
use opentelemetry_proto::tonic::{collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::any_value, metrics::v1::{metric, number_data_point}};
use pi_async_rt::{runtime_metrics::*, rt::{AsyncRuntime, single_thread::SingleTaskRunner,
    serial_local_thread::LocalTaskRunner, multi_thread::{MultiTaskRuntimeBuilder, StealableTaskPool}}};
use prost::Message;
use std::{collections::{BTreeMap, BTreeSet}, sync::{Arc, Mutex, Barrier, mpsc,
    atomic::{AtomicBool, Ordering}}, thread, time::{Duration, Instant}};

#[global_allocator]
static ALLOCATOR: probe::Allocator = probe::Allocator;
const PROCESS: &str = "metrics-benchmark-process";

/// 合法导出末端只保存官方转换结果，实际聚合由SDK完成；不伪造网络或远端确认。
struct Capture(Arc<Mutex<Option<ExportMetricsServiceRequest>>>);
impl PushMetricExporter for Capture {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        *self.0.lock().unwrap() = Some(metrics.into());
        Ok(())
    }
    fn force_flush(&self) -> OTelSdkResult { Ok(()) }
    fn shutdown_with_timeout(&self, _: Duration) -> OTelSdkResult { Ok(()) }
    fn temporality(&self) -> Temporality { Temporality::Cumulative }
}

type Host = (SdkMeterProvider, Arc<Mutex<Option<ExportMetricsServiceRequest>>>);
/// 显式装配宿主SDK或保留默认no-op对照；只在子场景初始化期调用，不计入采集时间。
/// 周期设为一小时以避免短测中自动导出争用，计时外由export主动刷新并核验真实产物。
fn sdk(enabled: bool) -> Option<Host> {
    if !enabled { return None; }
    let captured = Arc::new(Mutex::new(None));
    let reader = PeriodicReader::builder(Capture(captured.clone())).with_interval(Duration::from_secs(3600)).build();
    let provider = SdkMeterProvider::builder().with_reader(reader).build();
    opentelemetry::global::set_meter_provider(provider.clone());
    Some((provider, captured))
}

/// 只读检查真实SDK产物全集，编码长度来自官方prost，不是字符串估计或网络总字节。
fn export(host: Option<Host>, case: &str, expected: &[(RuntimeMetricId, String)], local: bool) {
    if let Some((provider, captured)) = host {
        provider.force_flush().unwrap();
        let request = captured.lock().unwrap().take().expect("实际SDK必须产生数据");
        let mut actual = BTreeSet::new();
        let mut points = 0;
        for resource in &request.resource_metrics {
            for scope in &resource.scope_metrics {
                assert_eq!(scope.scope.as_ref().unwrap().name, "pi_async_rt");
                for metric in &scope.metrics {
                    assert!(["pi_async_rt.runtime.len", "pi_async_rt.runtime.wait_len", "pi_async_rt.runtime.sample_state"].contains(&metric.name.as_str()));
                    let gauge = match metric.data.as_ref().unwrap() { metric::Data::Gauge(gauge) => gauge, _ => panic!("实际聚合必须是Gauge") };
                    for point in &gauge.data_points {
                        let attributes: BTreeMap<_, _> = point.attributes.iter().map(|attribute| (attribute.key.as_str(),
                            attribute.value.as_ref().unwrap().value.as_ref().unwrap())).collect();
                        assert_eq!((attributes.len(), point.attributes.len()), (3, 3));
                        assert_eq!(attributes["rt.process"], &any_value::Value::StringValue(PROCESS.to_owned()));
                        let id = match attributes["rt.id"] { any_value::Value::IntValue(id) => *id as u64, _ => panic!("ID必须是整数") };
                        let wanted = expected.iter().find(|(wanted, _)| wanted.as_u64() == id).unwrap();
                        assert_eq!(attributes["rt.name"], &any_value::Value::StringValue(wanted.1.clone()));
                        let value = match point.value { Some(number_data_point::Value::AsInt(value)) => value, _ => panic!("指标必须是整数") };
                        if metric.name.ends_with("sample_state") { assert!([1, 2].contains(&value)); }
                        else { assert_eq!(value, 0); }
                        assert!(actual.insert((metric.name.as_str(), id)));
                        points += 1;
                    }
                }
            }
        }
        assert_eq!(points, expected.len() * if local { 2 } else { 3 });
        for (id, _) in expected {
            for name in ["pi_async_rt.runtime.len", "pi_async_rt.runtime.sample_state"] { assert!(actual.contains(&(name, id.as_u64()))); }
            assert_eq!(actual.contains(&("pi_async_rt.runtime.wait_len", id.as_u64())), !local);
        }
        let encoded = request.encode_to_vec();
        assert_eq!(encoded.len(), request.encoded_len());
        assert_eq!(ExportMetricsServiceRequest::decode(encoded.as_slice()).unwrap(), request);
        println!("{{\"type\":\"protobuf\",\"case\":\"{}\",\"points\":{},\"encoded_message_bytes\":{}}}", case, points, encoded.len());
        provider.shutdown().unwrap();
    }
}

/// 独立期望是初始空队列及固定身份，报告范围／失败不由测试代填；本地允许旧请求在途。
fn rounds(case: &str, phase: &str, count: usize, expected: &[(RuntimeMetricId, String)], local: bool, hz: u64) {
    let mut times = Vec::with_capacity(count);
    let mut successes = 0;
    let mut in_flight = 0;
    let cpu = probe::cpu_ticks();
    let before = probe::allocations();
    for _ in 0..count {
        let start = Instant::now();
        let report = collect_and_report_runtime_metrics(RuntimeMetricCollectionOptions::default()).unwrap();
        times.push(start.elapsed().as_nanos());
        assert_eq!((report.active_count(), report.retired_count()), (expected.len(), 0));
        assert_eq!(report.instances().len(), expected.len());
        for (item, (id, name)) in report.instances().iter().zip(expected) {
            assert_eq!((item.id(), item.name()), (*id, name.as_str()));
            match item.status() {
                RuntimeMetricInstanceStatus::RecordInvoked => {
                    assert_eq!(item.counts(), Some((0, if local { None } else { Some(0) })));
                    successes += 1;
                }
                RuntimeMetricInstanceStatus::RequestInFlight if local => { assert!(item.counts().is_none()); in_flight += 1; }
                _ => panic!("健康基准出现异常状态：{:?}", item),
            }
        }
        assert_eq!(report.sampled_count(), report.count(RuntimeMetricInstanceStatus::RecordInvoked));
    }
    let after = probe::allocations();
    let used = probe::cpu_ticks() - cpu;
    probe::row(case, phase, times, before, after, used, hz);
    assert_eq!(successes + in_flight, count * expected.len());
    println!("{{\"type\":\"samples\",\"case\":\"{}\",\"phase\":\"{}\",\"successful_instances\":{},\"in_flight_instances\":{}}}", case, phase, successes, in_flight);
}

/// 每子进程只登记一种真实来源；合法输入由父清单限定，实例／名称均不跨子场景共享。
/// 冷／暖采集后计时外核验官方编码；Local只在原后台线程取数，外部不读取其队列。
fn sampling(case: &str, parts: &[&str], hz: u64) {
    let local = parts[0] == "local";
    let host = sdk(parts[1] == "sdk");
    init_runtime_metrics_reporting(PROCESS).unwrap();
    let count: usize = parts[2].parse().unwrap();
    let bytes: usize = parts[3].parse().unwrap();
    let mut expected = Vec::new();
    let mut singles = Vec::new();
    let mut locals = Vec::new();
    for _ in 0..count {
        let name = "n".repeat(bytes);
        let id = if local {
            let poller = Arc::new(polling::Poller::new().unwrap());
            let runtime = LocalTaskRunner::<()>::with_poll(poller).startup_with_poll(
                "Metrics-Bench-Local", 2 * 1024 * 1024, 0, Some(Duration::from_millis(20)));
            let id = register_runtime_metrics(&runtime, &name).unwrap();
            locals.push(runtime);
            id
        } else {
            let runner = SingleTaskRunner::<()>::default();
            let runtime = runner.startup().unwrap();
            let id = register_runtime_metrics(&runtime, &name).unwrap();
            singles.push((runner, runtime));
            id
        };
        expected.push((id, name));
    }
    rounds(case, "cold", 1, &expected, local, hz);
    rounds(case, "warm", 1000, &expected, local, hz);
    export(host, case, &expected, local);
    for (id, _) in expected { assert!(unregister_runtime_metrics(id)); }
    for runtime in locals { assert!(runtime.close()); }
}

/// 固定64个真实核心，测同名幂等及退休恢复，不用无限创建核心耗尽历史系列预算。
/// 每次严格核验原ID；时间包括实际控制API，阶段分配／CPU还含结果断言。
fn registration(case: &str, hz: u64) {
    let mut handles = Vec::new();
    for _ in 0..64 {
        let runner = SingleTaskRunner::<()>::default();
        let runtime = runner.startup().unwrap();
        let id = register_runtime_metrics(&runtime, "registered").unwrap();
        handles.push((runner, runtime, id));
    }
    for phase in ["duplicate", "retire_restore"] {
        let mut times = Vec::with_capacity(1000);
        let cpu = probe::cpu_ticks();
        let before = probe::allocations();
        for index in 0..1000 {
            let (_, runtime, id) = &handles[index % handles.len()];
            let start = Instant::now();
            if phase == "retire_restore" { assert!(unregister_runtime_metrics(*id)); }
            let actual = register_runtime_metrics(runtime, "registered").unwrap();
            times.push(start.elapsed().as_nanos());
            assert_eq!(actual, *id);
        }
        let after = probe::allocations();
        probe::row(case, phase, times, before, after, probe::cpu_ticks() - cpu, hz);
    }
    for (_, _, id) in handles { assert!(unregister_runtime_metrics(id)); }
}

/// 四生产者原spawn，最多1920个未回填任务；只用观测通道确认唯一编号／首次执行延迟。
fn tasks<R: AsyncRuntime<()>>(runtime: &R, total: usize) -> (Duration, Vec<u128>) {
    let start_gate = Arc::new(Barrier::new(5));
    let (done, results) = mpsc::sync_channel(4);
    let mut threads = Vec::new();
    for _ in 0..4 {
        let (runtime, gate, done) = (runtime.clone(), start_gate.clone(), done.clone());
        threads.push(thread::spawn(move || {
            let limit = total / 4;
            let (sender, receiver) = mpsc::sync_channel(480);
            let mut seen = vec![false; limit];
            let mut times = Vec::with_capacity(limit);
            let mut pending = 0;
            let mut receive = || {
                let (id, latency): (usize, u128) = receiver.recv_timeout(Duration::from_secs(5)).unwrap();
                assert!(id < limit && !seen[id]);
                seen[id] = true;
                times.push(latency);
            };
            gate.wait();
            for index in 0..limit {
                if pending == 480 { receive(); pending -= 1; }
                let sender = sender.clone();
                let started = Instant::now();
                runtime.spawn(async move { sender.try_send((index, started.elapsed().as_nanos())).unwrap(); }).unwrap();
                pending += 1;
            }
            for _ in 0..pending { receive(); }
            assert!(seen.into_iter().all(|seen| seen));
            done.try_send(times).unwrap();
        }));
    }
    drop(done);
    let start = Instant::now();
    start_gate.wait();
    let mut times = Vec::with_capacity(total);
    for _ in 0..4 { times.extend(results.recv_timeout(Duration::from_secs(5)).unwrap()); }
    for task in threads { task.join().unwrap(); }
    let elapsed = start.elapsed();
    assert_eq!(times.len(), total);
    (elapsed, times)
}

/// 同一8worker生产装配对照未登记、只登记及已有外部线程每1ms采集三模式。
/// 每模式预热1000加3轮各10000空业务任务；观测器成本统一，不作旧版本性能结论。
fn scheduling(case: &str, mode: &str, hz: u64) {
    let pool = StealableTaskPool::<()>::with(8, 8192, [1, 1], 3000);
    let runtime = MultiTaskRuntimeBuilder::new(pool).init_worker_size(8).set_worker_limit(8, 8)
        .set_timer_interval(1).set_timeout(10).build();
    assert_eq!(runtime.worker_len(), 8);
    let _host = sdk(true);
    init_runtime_metrics_reporting(PROCESS).unwrap();
    let id = if mode == "unregistered" { None } else { Some(register_runtime_metrics(&runtime, "task-bench").unwrap()) };
    let stop = Arc::new(AtomicBool::new(false));
    let (done, samples) = mpsc::sync_channel(1);
    let sampler = if mode == "sampled" {
        let stop = stop.clone();
        Some(thread::spawn(move || {
            let mut rounds = 0;
            let mut recorded = 0;
            let mut out_of_range = 0;
            while !stop.load(Ordering::Acquire) {
                let report = collect_and_report_runtime_metrics(RuntimeMetricCollectionOptions::default()).unwrap();
                assert_eq!((report.active_count(), report.sampled_count()), (1, 1));
                recorded += report.count(RuntimeMetricInstanceStatus::RecordInvoked);
                out_of_range += report.count(RuntimeMetricInstanceStatus::MetricValueOutOfRange);
                rounds += 1;
                thread::sleep(Duration::from_millis(1));
            }
            done.try_send((rounds, recorded, out_of_range)).unwrap();
        }))
    } else { None };
    let _ = tasks(&runtime, 1000);
    for index in 0..3 {
        let cpu = probe::cpu_ticks();
        let before = probe::allocations();
        let (elapsed, times) = tasks(&runtime, 10000);
        let after = probe::allocations();
        let used = probe::cpu_ticks() - cpu;
        println!("{{\"type\":\"tasks\",\"case\":\"{}\",\"repeat\":{},\"workers\":8,\"producers\":4,\"tasks\":10000,\"elapsed_ns\":{},\"tasks_per_second\":{},\"cpu_ticks\":{},\"clock_ticks_per_second\":{}}}",
            case, index, elapsed.as_nanos(), 10000.0 / elapsed.as_secs_f64(), used, hz);
        probe::row(case, "submit_to_first_poll", times, before, after, used, hz);
    }
    stop.store(true, Ordering::Release);
    if let Some(sampler) = sampler {
        let (count, recorded, out_of_range) = samples.recv_timeout(Duration::from_secs(5)).unwrap();
        sampler.join().unwrap();
        assert!(count > 0);
        assert_eq!(count, recorded + out_of_range);
        println!("{{\"type\":\"concurrent_sampling\",\"case\":\"{}\",\"rounds\":{},\"recorded\":{},\"out_of_range\":{}}}", case, count, recorded, out_of_range);
    }
    if let Some(id) = id { assert!(unregister_runtime_metrics(id)); }
}

fn main() {
    if let Ok(case) = std::env::var("PI_RT_METRICS_BENCH_CASE") {
        let _clock = pi_async_rt::rt::startup_global_time_loop(1).expect("每实际子进程唯一时钟");
        let hz = probe::clock_ticks();
        let parts: Vec<_> = case.split(':').collect();
        match parts[0] {
            "direct" | "local" => sampling(&case, &parts, hz),
            "registration" => registration(&case, hz),
            "tasks" => scheduling(&case, parts[1], hz),
            _ => panic!("未知基准场景"),
        }
        return;
    }
    let mut cases = Vec::new();
    for mode in ["noop", "sdk"] {
        for count in [1, 8, 64, 256] {
            for bytes in [8, 256] { cases.push(format!("direct:{}:{}:{}", mode, count, bytes)); }
        }
        for count in [1, 8] { cases.push(format!("local:{}:{}:8", mode, count)); }
    }
    cases.push("registration".to_owned());
    for mode in ["unregistered", "registered", "sampled"] { cases.push(format!("tasks:{}", mode)); }
    probe::parent(&cases);
}
