#![cfg(all(feature = "trace", not(target_arch = "wasm32")))]

//! MN-D04／MC-T01～03：真实公开采集链的合并标签及单次请求标准测试。
//! 官方接口观测器只保存实际Gauge调用，不生成标签、读数、通知或生产状态。
//! 单个独立目标独占全局Provider／注册表／时钟；编译300秒、实际进程15秒。
//! 不使用block_on，不代替最终官方SDK／OTLP验收，不属于压力或性能测试。

use opentelemetry::{InstrumentationScope, KeyValue, Value,
    metrics::{Gauge, InstrumentBuilder, InstrumentProvider, Meter, MeterProvider, SyncInstrument}};
use pi_async_rt::{runtime_metrics::*, rt::{AsyncRuntime,
    single_thread::SingleTaskRunner, serial_local_thread::{LocalTaskRunner, LocalTaskRuntime}}};
use std::{collections::BTreeMap, rc::Rc, sync::{Arc, Mutex,
    atomic::{AtomicUsize, Ordering}}, thread, time::{Duration, Instant}};

const HOST: &str = "多名称验收/宿主/启动实例";
const LEN: &str = "pi_async_rt.runtime.len";
const WAIT: &str = "pi_async_rt.runtime.wait_len";
const STATE: &str = "pi_async_rt.runtime.sample_state";
const LIMIT: Duration = Duration::from_secs(3);
type Events = Arc<Mutex<Vec<(String, u64, Vec<KeyValue>)>>>;

/// 只观测原Meter创建及原Gauge调用；不聚合、不补值、不调用采集或runtime。
#[derive(Clone, Debug)]
struct Probe {
    built: Arc<Mutex<Vec<String>>>,
    events: Events,
}
impl MeterProvider for Probe {
    fn meter_with_scope(&self, scope: InstrumentationScope) -> Meter {
        assert_eq!(scope.name(), "pi_async_rt");
        assert_eq!(scope.version(), Some(env!("CARGO_PKG_VERSION")));
        Meter::new(Arc::new(self.clone()))
    }
}
impl InstrumentProvider for Probe {
    fn u64_gauge(&self, builder: InstrumentBuilder<'_, Gauge<u64>>) -> Gauge<u64> {
        let name = builder.name.into_owned();
        self.built.lock().unwrap().push(name.clone());
        Gauge::new(Arc::new(CallProbe(name, self.events.clone())))
    }
}
#[derive(Debug)]
struct CallProbe(String, Events);
impl SyncInstrument<u64> for CallProbe {
    fn measure(&self, value: u64, attributes: &[KeyValue]) {
        self.1.lock().unwrap().push((self.0.clone(), value, attributes.to_vec()));
    }
}

/// 外部观测只接收公开入口结果，绝对截止前后检查；原生产通道／截止完全不替代。
fn start_collection() -> (thread::JoinHandle<()>, crossbeam_channel::Receiver<RuntimeMetricCollectionReport>) {
    let (sender, receiver) = crossbeam_channel::bounded(1);
    let handle = thread::spawn(move || {
        let options = RuntimeMetricCollectionOptions::new(Duration::from_secs(2), LIMIT).unwrap();
        sender.try_send(collect_and_report_runtime_metrics(options).unwrap()).unwrap();
    });
    (handle, receiver)
}
fn finish_collection(handle: thread::JoinHandle<()>, receiver: crossbeam_channel::Receiver<RuntimeMetricCollectionReport>)
    -> RuntimeMetricCollectionReport
{
    let deadline = Instant::now() + LIMIT;
    let report = receiver.recv_deadline(deadline).unwrap();
    assert!(Instant::now() < deadline);
    handle.join().unwrap();
    report
}

/// 精确核验调用全集和次数，忽略无契约意义的属性顺序；期望来自场景字面值而非Report。
fn assert_events(events: &Events, expected: &[(&str, RuntimeMetricId, &str, u64)]) {
    let calls = std::mem::take(&mut *events.lock().unwrap());
    let mut actual = BTreeMap::new();
    for (instrument, value, attributes) in calls {
        assert_eq!(attributes.len(), 3);
        let get = |key: &str| {
            let mut found = attributes.iter().filter(|attribute| attribute.key.as_str() == key);
            let value = &found.next().expect("必需标签缺失").value;
            assert!(found.next().is_none(), "属性键不得重复");
            value
        };
        let id = match get("rt.id") { Value::I64(id) if *id > 0 => *id, _ => panic!("ID必须为正i64") };
        let name = match get("rt.name") { Value::String(name) => name.to_string(), _ => panic!("名称类型错误") };
        let process = match get("rt.process") { Value::String(value) => value.to_string(), _ => panic!("进程类型错误") };
        assert_eq!(process, format!("{}-{}", HOST, std::process::id()));
        actual.entry((instrument, id, name)).or_insert_with(Vec::new).push(value);
    }
    let expected: BTreeMap<_, _> = expected.iter().map(|(instrument, id, name, value)|
        ((instrument.to_string(), id.as_u64() as i64, name.to_string()), vec![*value])).collect();
    assert_eq!(actual, expected, "每核心每适用Gauge必须恰好一次，且使用完整合并名");
}

fn assert_item(report: &RuntimeMetricCollectionReport, id: RuntimeMetricId, names: &[&str],
    status: RuntimeMetricInstanceStatus, counts: Option<(usize, Option<usize>)>)
{
    let items: Vec<_> = report.instances().iter().filter(|item| item.id() == id).collect();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].name(), names[0]);
    assert_eq!(items[0].names().collect::<Vec<_>>(), names);
    assert_eq!((items[0].status(), items[0].counts()), (status, counts));
}

static DEFAULTS: AtomicUsize = AtomicUsize::new(0);
static DROPS: AtomicUsize = AtomicUsize::new(0);
/// !Send输出仅观测真实采样任务在owner构造／析构，不参与回填或许可操作。
struct OwnerOutput(Rc<thread::ThreadId>);
impl Default for OwnerOutput {
    fn default() -> Self {
        DEFAULTS.fetch_add(1, Ordering::Relaxed);
        Self(Rc::new(thread::current().id()))
    }
}
impl Drop for OwnerOutput {
    fn drop(&mut self) {
        assert_eq!(*self.0, thread::current().id());
        DROPS.fetch_add(1, Ordering::Relaxed);
    }
}
fn wait_request(runtime: &LocalTaskRuntime<OwnerOutput>) {
    let deadline = Instant::now() + LIMIT;
    while runtime.len() == 0 {
        assert!(Instant::now() < deadline, "真实采样send未到达");
        thread::yield_now();
    }
    assert_eq!(runtime.len(), 1, "多名称不能形成多个本地采样任务");
}

/// 完整MC-T01～03：合并展示名相同但ID不同的两核心、Local旧快照追加、一次请求、
/// 整体注销／恢复及报告独立性；三Gauge仅初始化一次，真实业务任务由原Runner消费。
#[test]
fn test_multi_name_collection_uses_one_source_and_combined_attributes() {
    let _clock = pi_async_rt::rt::startup_global_time_loop(1).expect("本进程仅初始化一次时钟");
    let events: Events = Arc::new(Mutex::new(Vec::new()));
    let built = Arc::new(Mutex::new(Vec::new()));
    opentelemetry::global::set_meter_provider(Probe { built: built.clone(), events: events.clone() });
    init_runtime_metrics_reporting(HOST).unwrap();
    init_runtime_metrics_reporting(HOST).unwrap();
    assert_eq!(*built.lock().unwrap(), [LEN, WAIT, STATE]);
    let first_runner = SingleTaskRunner::<()>::default();
    let first = first_runner.startup().unwrap();
    let second_runner = SingleTaskRunner::<()>::default();
    let second = second_runner.startup().unwrap();
    let a = register_runtime_metrics(&first, "a-b").unwrap();
    assert_eq!(register_runtime_metrics(&first.clone(), "c"), Ok(a));
    let b = register_runtime_metrics(&second, "a").unwrap();
    assert_eq!(register_runtime_metrics(&second, "b-c"), Ok(b));
    assert_ne!(a, b);
    for _ in 0..2 { first.spawn(async {}).unwrap(); }
    second.spawn(async {}).unwrap();
    let (handle, receiver) = start_collection();
    let report = finish_collection(handle, receiver);
    assert_eq!((report.instances().len(), report.active_count(), report.sampled_count()), (2, 2, 2));
    assert_item(&report, a, &["a-b", "c"], RuntimeMetricInstanceStatus::RecordInvoked, Some((2, Some(0))));
    assert_item(&report, b, &["a", "b-c"], RuntimeMetricInstanceStatus::RecordInvoked, Some((1, Some(0))));
    assert_events(&events, &[(LEN, a, "a-b-c", 2), (WAIT, a, "a-b-c", 0), (STATE, a, "a-b-c", 1),
        (LEN, b, "a-b-c", 1), (WAIT, b, "a-b-c", 0), (STATE, b, "a-b-c", 1)]);
    for _ in 0..2 { first_runner.run_once().unwrap(); }
    second_runner.run_once().unwrap();
    assert!(unregister_runtime_metrics(a));
    assert!(unregister_runtime_metrics(b));

    let local_runner = LocalTaskRunner::<OwnerOutput>::new();
    let local = local_runner.get_runtime();
    let id = register_runtime_metrics(&local, "local").unwrap();
    assert_eq!(register_runtime_metrics(&local, "收发"), Ok(id));
    let (handle, receiver) = start_collection();
    wait_request(&local);
    assert_eq!(DEFAULTS.load(Ordering::Relaxed), 0);
    assert_eq!(register_runtime_metrics(&local, "extra"), Ok(id));
    assert_eq!(local.len(), 1);
    local_runner.poll();
    local_runner.run_once();
    let old = finish_collection(handle, receiver);
    assert_item(&old, id, &["local", "收发"], RuntimeMetricInstanceStatus::RecordInvoked, Some((0, None)));
    assert_eq!((old.instances().len(), old.active_count(), old.retired_count(), old.sampled_count()), (3, 1, 2, 1));
    assert_events(&events, &[(STATE, a, "a-b-c", 0), (STATE, b, "a-b-c", 0),
        (LEN, id, "local-收发", 0), (STATE, id, "local-收发", 1)]);
    assert_eq!((DEFAULTS.load(Ordering::Relaxed), DROPS.load(Ordering::Relaxed), local.len()), (1, 1, 0));

    for expected in [2, 3] {
        if expected == 3 {
            assert!(unregister_runtime_metrics(id));
            assert_eq!(register_runtime_metrics(&local, "收发"), Ok(id));
        }
        let (handle, receiver) = start_collection();
        wait_request(&local);
        local_runner.poll();
        local_runner.run_once();
        let current = finish_collection(handle, receiver);
        assert_item(&current, id, &["local", "收发", "extra"], RuntimeMetricInstanceStatus::RecordInvoked, Some((0, None)));
        assert_events(&events, &[(STATE, a, "a-b-c", 0), (STATE, b, "a-b-c", 0),
            (LEN, id, "local-收发-extra", 0), (STATE, id, "local-收发-extra", 1)]);
        assert_eq!((DEFAULTS.load(Ordering::Relaxed), DROPS.load(Ordering::Relaxed), local.len()), (expected, expected, 0));
    }
    assert!(unregister_runtime_metrics(id));
    let (handle, receiver) = start_collection();
    let retired = finish_collection(handle, receiver);
    assert_eq!((retired.active_count(), retired.retired_count(), retired.sampled_count()), (0, 3, 0));
    assert_item(&retired, id, &["local", "收发", "extra"], RuntimeMetricInstanceStatus::Unregistered, None);
    assert_events(&events, &[(STATE, a, "a-b-c", 0), (STATE, b, "a-b-c", 0), (STATE, id, "local-收发-extra", 0)]);
    assert_eq!((DEFAULTS.load(Ordering::Relaxed), DROPS.load(Ordering::Relaxed), local.len()), (3, 3, 0));
    assert_eq!(*built.lock().unwrap(), [LEN, WAIT, STATE]);
    assert_eq!(old.instances().iter().find(|item| item.id() == id).unwrap().names().collect::<Vec<_>>(), ["local", "收发"]);
}
