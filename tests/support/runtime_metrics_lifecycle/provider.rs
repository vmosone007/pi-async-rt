//! MET-L-PROVIDER-001：仅通过官方接口注入外部重入／指定panic，验证真实生产保护。
//!
//! 不实现来源或采样、不碰生产私有状态、不造OTLP，不作为SDK聚合／网络证据。
//! 观测事件锁不跨重入或panic；故意等待仅发生于外部初始化线程且有截止。

use super::common::{assert_injected_panic, assert_sample, assert_summary, collect_external, options, receive, LIMIT};
use crossbeam_channel::{bounded, Receiver, Sender};
use opentelemetry::{InstrumentationScope, KeyValue, metrics::{Gauge, InstrumentBuilder,
    InstrumentProvider, Meter, MeterProvider, SyncInstrument}};
use pi_async_rt::{runtime_metrics::*, rt::{AsyncRuntime, single_thread::{SingleTaskRunner, SingleTaskRuntime}}};
use std::{sync::{Arc, Mutex, atomic::{AtomicBool, AtomicUsize, Ordering}}, thread, time::Instant};

const LEN: &str = "pi_async_rt.runtime.len";
const WAIT: &str = "pi_async_rt.runtime.wait_len";
const STATE: &str = "pi_async_rt.runtime.sample_state";
const METER_PANIC: &str = "故障注入：MeterProvider";
const RECORD_PANIC: &str = "故障注入：Gauge.measure";
const NAME: &str = "记录重入真实源";
const PROCESS: &str = "生命周期-记录重入";

enum InitAction {
    Normal,
    Pause { entered: Sender<()>, resume: Receiver<()> },
    PanicOnce,
}

/// 外部Provider状态不是生产初始化状态，不允许由探针清本库OnceLock或CAS保护。
struct Hooks {
    action: InitAction,
    meters: AtomicUsize,
    gauges: AtomicUsize,
    measure: Option<Arc<MeasureProbe>>,
}

impl Hooks {
    fn new(action: InitAction, measure: Option<Arc<MeasureProbe>>) -> Arc<Self> {
        Arc::new(Self { action, meters: AtomicUsize::new(0), gauges: AtomicUsize::new(0), measure })
    }
}

#[derive(Clone)]
struct Provider(Arc<Hooks>);

impl MeterProvider for Provider {
    fn meter_with_scope(&self, scope: InstrumentationScope) -> Meter {
        assert_eq!(scope.name(), "pi_async_rt");
        assert_eq!(scope.version(), Some(env!("CARGO_PKG_VERSION")));
        let call = self.0.meters.fetch_add(1, Ordering::AcqRel);
        match &self.0.action {
            InitAction::Normal => (),
            InitAction::PanicOnce if call == 0 => std::panic::panic_any(METER_PANIC),
            InitAction::PanicOnce => (),
            InitAction::Pause { entered, resume } => {
                assert_eq!(call, 0, "未发布期间不应重复创建Meter");
                assert_eq!(init_runtime_metrics_reporting("重入标识"),
                    Err(RuntimeMetricReportingInitError::InitializationInProgress));
                assert!(matches!(collect_and_report_runtime_metrics(options()),
                    Err(RuntimeMetricCollectionError::ReportingNotInitialized)));
                entered.try_send(()).unwrap();
                receive(resume, Instant::now() + LIMIT);
            }
        }
        Meter::new(Arc::new(Factory(self.0.clone())))
    }
}

struct Factory(Arc<Hooks>);
impl InstrumentProvider for Factory {
    fn u64_gauge(&self, builder: InstrumentBuilder<'_, Gauge<u64>>) -> Gauge<u64> {
        let (name, unit, description) = match builder.name.as_ref() {
            LEN => (LEN, "{task}", "已登记运行时原任务池待轮询数量"),
            WAIT => (WAIT, "{task}", "已登记运行时原等待读数"),
            STATE => (STATE, "1", "运行时指标采样及记录状态码"),
            _ => panic!("构建了未冻结仪器"),
        };
        assert_eq!(builder.unit.as_deref(), Some(unit));
        assert_eq!(builder.description.as_deref(), Some(description));
        self.0.gauges.fetch_add(1, Ordering::Relaxed);
        Gauge::new(Arc::new(Instrument { name, probe: self.0.measure.clone() }))
    }
}

type Event = (&'static str, u64, Vec<KeyValue>);

/// 仅持真实外部句柄和观测事件，重入均经公开函数，不直接释放或修改登记表。
/// 全局Provider有意保留本探针至子进程结束，本场景不据此声称runtime已析构。
struct MeasureProbe {
    runtime: SingleTaskRuntime<()>,
    id: RuntimeMetricId,
    armed: AtomicBool,
    reentries: AtomicUsize,
    events: Mutex<Vec<Event>>,
}

struct Instrument {
    name: &'static str,
    probe: Option<Arc<MeasureProbe>>,
}

impl SyncInstrument<u64> for Instrument {
    fn measure(&self, value: u64, attributes: &[KeyValue]) {
        if let Some(probe) = &self.probe {
            {
                let mut events = probe.events.lock().unwrap();
                assert!(events.len() < 16, "本例事件必须有界");
                events.push((self.name, value, attributes.to_vec()));
            }
            if self.name == LEN && probe.armed.swap(false, Ordering::AcqRel) {
                // 事件锁已释放；重入必须由生产保护拒绝，不靠探针串行化或代释放许可。
                assert!(matches!(collect_and_report_runtime_metrics(options()),
                    Err(RuntimeMetricCollectionError::CollectionInProgress)));
                assert!(unregister_runtime_metrics(probe.id));
                assert_eq!(register_runtime_metrics(&probe.runtime, NAME), Ok(probe.id));
                probe.reentries.fetch_add(1, Ordering::Release);
                std::panic::panic_any(RECORD_PANIC);
            }
        }
    }
}

/// MET-L06：外部Provider尚未返回时重入及另一线程均不能等待初始化保护。
/// 故障门真正进入后才尝试并发；释放后只发布一次，同参数幂等、异参数明确拒绝。
pub(super) fn initialization() {
    let (entered_tx, entered_rx) = bounded(1);
    let (resume_tx, resume_rx) = bounded(1);
    let hooks = Hooks::new(InitAction::Pause { entered: entered_tx, resume: resume_rx }, None);
    opentelemetry::global::set_meter_provider(Provider(hooks.clone()));
    let (done_tx, done_rx) = bounded(1);
    let initializer = thread::spawn(move || {
        done_tx.try_send(init_runtime_metrics_reporting("初始化唯一标识")).unwrap();
    });
    receive(&entered_rx, Instant::now() + LIMIT);
    assert_eq!(hooks.meters.load(Ordering::Acquire), 1);
    assert_eq!(hooks.gauges.load(Ordering::Relaxed), 0);
    assert_eq!(init_runtime_metrics_reporting("并发标识"),
        Err(RuntimeMetricReportingInitError::InitializationInProgress));
    assert!(matches!(collect_and_report_runtime_metrics(options()),
        Err(RuntimeMetricCollectionError::ReportingNotInitialized)));
    resume_tx.try_send(()).unwrap();
    assert_eq!(receive(&done_rx, Instant::now() + LIMIT), Ok(()));
    initializer.join().unwrap();
    assert_eq!(init_runtime_metrics_reporting("初始化唯一标识"), Ok(()));
    assert_eq!(init_runtime_metrics_reporting("其它标识"), Err(RuntimeMetricReportingInitError::AlreadyInitialized));
    assert_eq!((hooks.meters.load(Ordering::Relaxed), hooks.gauges.load(Ordering::Relaxed)), (1, 3));
    let empty = collect_external();
    assert_summary(&empty, 0, 0);
}

/// MET-L07：仅准确指定的Provider panic才有效；保护展开后不同标识可成功发布。
/// 第一轮在Meter返回前故障，必须没有仪器；恢复后重复初始化不得再建三仪器。
pub(super) fn initialization_panic() {
    let hooks = Hooks::new(InitAction::PanicOnce, None);
    opentelemetry::global::set_meter_provider(Provider(hooks.clone()));
    let failed = std::panic::catch_unwind(|| init_runtime_metrics_reporting("失败标识"));
    assert_injected_panic(failed, METER_PANIC);
    assert_eq!((hooks.meters.load(Ordering::Relaxed), hooks.gauges.load(Ordering::Relaxed)), (1, 0));
    assert!(matches!(collect_and_report_runtime_metrics(options()),
        Err(RuntimeMetricCollectionError::ReportingNotInitialized)));
    assert_eq!(init_runtime_metrics_reporting("恢复标识"), Ok(()));
    assert_eq!(init_runtime_metrics_reporting("恢复标识"), Ok(()));
    assert_eq!(init_runtime_metrics_reporting("失败标识"), Err(RuntimeMetricReportingInitError::AlreadyInitialized));
    assert_eq!((hooks.meters.load(Ordering::Relaxed), hooks.gauges.load(Ordering::Relaxed)), (2, 3));
    assert_summary(&collect_external(), 0, 0);
}

/// MET-L08：原record触发真实重入／登记操作及指定panic，不把任意panic算成成功。
/// 已记录事件不撤销；恢复轮重新读取真实队列，原3任务仍只执行一次，标签保持。
pub(super) fn record_panic() {
    let runner = SingleTaskRunner::<()>::default();
    let runtime = runner.startup().unwrap();
    let id = register_runtime_metrics(&runtime, NAME).unwrap();
    let probe = Arc::new(MeasureProbe { runtime: runtime.clone(), id, armed: AtomicBool::new(true),
        reentries: AtomicUsize::new(0), events: Mutex::new(Vec::new()) });
    let hooks = Hooks::new(InitAction::Normal, Some(probe.clone()));
    opentelemetry::global::set_meter_provider(Provider(hooks.clone()));
    init_runtime_metrics_reporting(PROCESS).unwrap();
    let completed = Arc::new(AtomicUsize::new(0));
    for _ in 0..3 {
        let completed = completed.clone();
        runtime.spawn(async move { completed.fetch_add(1, Ordering::Relaxed); }).unwrap();
    }
    let (failed_tx, failed_rx) = bounded(1);
    let collector = thread::spawn(move || {
        failed_tx.try_send(std::panic::catch_unwind(|| collect_and_report_runtime_metrics(options()))).unwrap();
    });
    let failed = receive(&failed_rx, Instant::now() + LIMIT);
    collector.join().unwrap();
    assert_injected_panic(failed, RECORD_PANIC);
    assert_eq!(probe.reentries.load(Ordering::Acquire), 1);
    assert_eq!(runtime.len(), 3);
    let report = collect_external();
    assert_summary(&report, 1, 0);
    assert_sample(&report, id, NAME, RuntimeMetricKind::Single,
        RuntimeMetricInstanceStatus::RecordInvoked, Some((3, Some(0))));
    assert_eq!(completed.load(Ordering::Relaxed), 0);
    for _ in 0..3 { runner.run_once().unwrap(); }
    assert_eq!(completed.load(Ordering::Relaxed), 3);
    let empty = collect_external();
    assert_summary(&empty, 1, 0);
    assert_sample(&empty, id, NAME, RuntimeMetricKind::Single,
        RuntimeMetricInstanceStatus::RecordInvoked, Some((0, Some(0))));
    let labels = vec![KeyValue::new("rt.id", id.as_u64() as i64), KeyValue::new("rt.name", NAME),
        KeyValue::new("rt.process", PROCESS)];
    let expected: Vec<_> = [(LEN, 3), (LEN, 3), (WAIT, 0), (STATE, 1), (LEN, 0), (WAIT, 0), (STATE, 1)]
        .iter().map(|&(name, value)| (name, value, labels.clone())).collect();
    assert_eq!(*probe.events.lock().unwrap(), expected);
    assert_eq!((hooks.meters.load(Ordering::Relaxed), hooks.gauges.load(Ordering::Relaxed)), (1, 3));
    assert!(unregister_runtime_metrics(id));
}
