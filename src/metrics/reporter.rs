//! MET-REPORTER：复用宿主Provider的三共享Gauge，只消费纯样本／标签。
//!
//! 不读runtime／登记，不新建导出线程、网络、重试队列或flush。record同步可能有
//! SDK内部锁／分配，不是远端确认；所有SDK调用均在本库控制锁外。合同：MET-IMPLEMENTATION-001。

use super::model::RuntimeMetricReportingInitError;
#[cfg(feature = "trace")]
use std::sync::{Arc, OnceLock, atomic::{AtomicBool, Ordering}};
#[cfg(feature = "trace")]
use super::{counts::RuntimeMetricCounts, model::{validate_label, LabelValidationError, RuntimeMetricMetadata,
    RuntimeMetricInstanceStatus, RuntimeMetricCollectionError}};

#[cfg(feature = "trace")]
pub(super) struct Reporter {
    process: Arc<str>,
    len: opentelemetry::metrics::Gauge<u64>,
    wait_len: opentelemetry::metrics::Gauge<u64>,
    state: opentelemetry::metrics::Gauge<u64>,
}

#[cfg(feature = "trace")]
static REPORTER: OnceLock<Reporter> = OnceLock::new();
#[cfg(feature = "trace")]
static INITIALIZING: AtomicBool = AtomicBool::new(false);

/// 初始化许可不跨Mutex，SDK可重入但第二次初始化立即失败，避免OnceLock闭包自等。
#[cfg(feature = "trace")]
struct InitializationGuard;
#[cfg(feature = "trace")]
impl Drop for InitializationGuard {
    fn drop(&mut self) { INITIALIZING.store(false, Ordering::Release); }
}

#[cfg(feature = "trace")]
/// 入口初始化合同的唯一实现：先校验／认领，再锁外创建SDK仪器并一次发布。
/// process是外部宿主语义标记，原1～256字节门禁只校验输入；完整标记-PID最大267字节。
/// 拼接／PID查询只在本初始化冷路径发生，同参重复也可构造临时比较值，但不重建
/// 已发布字符串或仪器；采集仍借用Arc及标签缓存。专项：本模块MN-P01真实初始化单元。
/// 用户Provider可能阻塞或panic，保护对象只保证正常／展开释放许可，不吞异常。
pub(super) fn initialize(process: &str) -> Result<(), RuntimeMetricReportingInitError> {
    validate_label(process).map_err(|error| match error {
        LabelValidationError::TooLong => RuntimeMetricReportingInitError::ProcessInstanceTooLong,
        LabelValidationError::Invalid => RuntimeMetricReportingInitError::InvalidProcessInstance,
    })?;
    // 只保存完整组合，不截断原标记；按完整组合比较，避免解析含连字符的宿主名称。
    let process = format!("{}-{}", process, std::process::id());
    let check = |reporter: &Reporter| if reporter.process.as_ref() == process.as_str() { Ok(()) }
        else { Err(RuntimeMetricReportingInitError::AlreadyInitialized) };
    if let Some(reporter) = REPORTER.get() { return check(reporter); }
    INITIALIZING.compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
        .map_err(|_| RuntimeMetricReportingInitError::InitializationInProgress)?;
    let _guard = InitializationGuard;
    if let Some(reporter) = REPORTER.get() { return check(reporter); }
    let scope = opentelemetry::InstrumentationScope::builder("pi_async_rt")
        .with_version(env!("CARGO_PKG_VERSION")).build();
    let meter = opentelemetry::global::meter_with_scope(scope);
    let reporter = Reporter {
        process: Arc::from(process),
        len: meter.u64_gauge("pi_async_rt.runtime.len").with_unit("{task}")
            .with_description("已登记运行时原任务池待轮询数量").build(),
        wait_len: meter.u64_gauge("pi_async_rt.runtime.wait_len").with_unit("{task}")
            .with_description("已登记运行时原等待读数").build(),
        state: meter.u64_gauge("pi_async_rt.runtime.sample_state").with_unit("1")
            .with_description("运行时指标采样及记录状态码").build(),
    };
    // 唯一初始化者发布，SDK构建和可能的资源析构从未持本库Mutex。
    match REPORTER.set(reporter) {
        Ok(()) => Ok(()),
        Err(_) => unreachable!("唯一指标初始化许可不允许重复发布"),
    }
}

#[cfg(not(feature = "trace"))]
/// trace关闭的O(1)失败出口，不校验／存储参数、不分配或调用SDK。
pub(super) fn initialize(_process: &str) -> Result<(), RuntimeMetricReportingInitError> {
    Err(RuntimeMetricReportingInitError::ReportingDisabled)
}

#[cfg(feature = "trace")]
/// O(1)只读已发布记录器，未初始化明确失败；不等待初始化、不调用SDK或建仪器。
pub(super) fn initialized() -> Result<&'static Reporter, RuntimeMetricCollectionError> {
    REPORTER.get().ok_or(RuntimeMetricCollectionError::ReportingNotInitialized)
}

#[cfg(feature = "trace")]
impl Reporter {
    /// O(1)本库固定操作加SDK成本。整份数字先校验再record，失败只写状态不补假零。
    /// 原Counts不修改，None无wait点，借用缓存三标签，不重建Vec或逐轮拼字符串。
    /// 全局调用方传该核心快照的reported描述符，每核心只调用一次；不自行查表或
    /// 遍历名称。MN-D04标签及调用次数验证：tests/runtime_metrics_multi_name.rs。
    pub(super) fn record(&self, metadata: &RuntimeMetricMetadata,
        counts: Option<RuntimeMetricCounts>, mut status: RuntimeMetricInstanceStatus)
        -> RuntimeMetricInstanceStatus
    {
        if let Some(counts) = counts {
            if counts.len() as u128 > i64::MAX as u128
                || counts.wait_len().map_or(false, |value| value as u128 > i64::MAX as u128)
            {
                status = RuntimeMetricInstanceStatus::MetricValueOutOfRange;
            }
        }
        let labels = metadata.labels(&self.process);
        if status == RuntimeMetricInstanceStatus::RecordInvoked {
            if let Some(counts) = counts {
                self.len.record(counts.len() as u64, labels);
                if let Some(wait_len) = counts.wait_len() { self.wait_len.record(wait_len as u64, labels); }
            }
        }
        self.state.record(status.code(), labels);
        status
    }
}

#[cfg(all(test, feature = "trace"))]
mod tests {
    use super::*;
    use super::super::{identity::RuntimeMetricIdentitySlot, model::RuntimeMetricKind};
    use opentelemetry::{KeyValue, metrics::{Gauge, SyncInstrument}};
    use std::sync::Mutex;

    type Events = Arc<Mutex<Vec<(&'static str, u64, Vec<KeyValue>)>>>;
    struct CallProbe(&'static str, Events);
    impl SyncInstrument<u64> for CallProbe {
        fn measure(&self, value: u64, attributes: &[KeyValue]) {
            self.1.lock().unwrap().push((self.0, value, attributes.to_vec()));
        }
    }

    /// 仅局部record调用探针：使用官方Gauge／SyncInstrument出口观测参数，不代替
    /// 真实SDK聚合或OTLP运输，不能以本测试宣称远端完整验收。Source／运行时不mock。
    fn fixture() -> (Reporter, Events, RuntimeMetricMetadata) {
        let events: Events = Arc::new(Mutex::new(Vec::new()));
        let gauge = |name| Gauge::new(Arc::new(CallProbe(name, events.clone())));
        let reporter = Reporter { process: Arc::from("process-unique"), len: gauge("len"),
            wait_len: gauge("wait_len"), state: gauge("sample_state") };
        let id = RuntimeMetricIdentitySlot::new().get_or_allocate().unwrap().id();
        (reporter, events, RuntimeMetricMetadata::new(id, "网关", RuntimeMetricKind::Single))
    }

    /// MET-REPORTER：固定三标签、None／Some(0)、数量先于状态、缓存稳定及全失败类别。
    #[test]
    fn test_record_calls_labels_and_failure_without_fake_counts() {
        let (reporter, events, metadata) = fixture();
        assert_eq!(reporter.record(&metadata, Some(RuntimeMetricCounts::new(7, Some(0))),
            RuntimeMetricInstanceStatus::RecordInvoked), RuntimeMetricInstanceStatus::RecordInvoked);
        let labels = vec![KeyValue::new("rt.id", metadata.id().as_u64() as i64),
            KeyValue::new("rt.name", "网关"), KeyValue::new("rt.process", "process-unique")];
        assert_eq!(*events.lock().unwrap(), vec![("len", 7, labels.clone()),
            ("wait_len", 0, labels.clone()), ("sample_state", 1, labels.clone())]);
        let cached = metadata.labels(&reporter.process).as_ptr();
        events.lock().unwrap().clear();
        reporter.record(&metadata, Some(RuntimeMetricCounts::new(0, None)), RuntimeMetricInstanceStatus::RecordInvoked);
        assert_eq!(*events.lock().unwrap(), vec![("len", 0, labels.clone()), ("sample_state", 1, labels.clone())]);
        assert_eq!(cached, metadata.labels(&reporter.process).as_ptr());
        for status in [RuntimeMetricInstanceStatus::Unregistered, RuntimeMetricInstanceStatus::RequestInFlight,
            RuntimeMetricInstanceStatus::DeadlineExceeded, RuntimeMetricInstanceStatus::ReplyClosed,
            RuntimeMetricInstanceStatus::RoundDeadlineSkipped]
        {
            events.lock().unwrap().clear();
            assert_eq!(reporter.record(&metadata, None, status), status);
            assert_eq!(*events.lock().unwrap(), vec![("sample_state", status.code(), labels.clone())]);
        }
    }

    /// MET-REPORTER：整份范围校验避免部分记录；原Counts保存完整usize，末端合法值不失真。
    #[test]
    fn test_whole_sample_integer_range_preserves_raw_counts() {
        let (reporter, events, metadata) = fixture();
        let max_valid = (i64::MAX as u128).min(usize::MAX as u128) as usize;
        reporter.record(&metadata, Some(RuntimeMetricCounts::new(max_valid, Some(max_valid))),
            RuntimeMetricInstanceStatus::RecordInvoked);
        assert_eq!(events.lock().unwrap().iter().map(|(name, value, _)| (*name, *value)).collect::<Vec<_>>(),
            vec![("len", max_valid as u64), ("wait_len", max_valid as u64), ("sample_state", 1)]);
        if usize::BITS >= 64 {
            let over = (i64::MAX as u128 + 1) as usize;
            for (len, wait_len) in [(over, None), (0, Some(over)), (usize::MAX, Some(usize::MAX))] {
                events.lock().unwrap().clear();
                let counts = RuntimeMetricCounts::new(len, wait_len);
                assert_eq!(reporter.record(&metadata, Some(counts), RuntimeMetricInstanceStatus::RecordInvoked),
                    RuntimeMetricInstanceStatus::MetricValueOutOfRange);
                assert_eq!((counts.len(), counts.wait_len()), (len, wait_len));
                let records = events.lock().unwrap();
                assert_eq!(records.len(), 1);
                assert_eq!((records[0].0, records[0].1), ("sample_state", 6));
            }
        }
    }

    /// MET-F-B04：真实record结果进入真实报告，验证越界保留原数／失败不补零／精确汇总。
    /// 这是局部防御合同，不声称真实队列拥有usize::MAX项，也不是SDK聚合或网络证据。
    #[test]
    fn test_record_result_and_report_summary_preserve_range_failure() {
        use super::super::model::{RuntimeMetricCollectionReport, RuntimeMetricInstanceReport, RuntimeMetricNames};
        let (reporter, events, metadata) = fixture();
        let names = RuntimeMetricNames::new(Arc::new(metadata));
        let metadata = names.primary();
        let max = (i64::MAX as u128).min(usize::MAX as u128) as usize;
        let mut items = Vec::new();
        for counts in [RuntimeMetricCounts::new(max, Some(0)), RuntimeMetricCounts::new(0, None)] {
            let status = reporter.record(metadata, Some(counts), RuntimeMetricInstanceStatus::RecordInvoked);
            assert_eq!(status, RuntimeMetricInstanceStatus::RecordInvoked);
            items.push(RuntimeMetricInstanceReport::new(&names, status, Some((counts.len(), counts.wait_len()))));
        }
        let mut expected = vec![("len", max as u64), ("wait_len", 0), ("sample_state", 1), ("len", 0), ("sample_state", 1)];
        let mut out_of_range = 0;
        if usize::BITS >= 64 {
            let counts = RuntimeMetricCounts::new(0, Some(usize::MAX));
            let status = reporter.record(metadata, Some(counts), RuntimeMetricInstanceStatus::RecordInvoked);
            assert_eq!(status, RuntimeMetricInstanceStatus::MetricValueOutOfRange);
            items.push(RuntimeMetricInstanceReport::new(&names, status, Some((0, Some(usize::MAX)))));
            expected.push(("sample_state", 6));
            out_of_range = 1;
        }
        for status in [RuntimeMetricInstanceStatus::Unregistered, RuntimeMetricInstanceStatus::RequestInFlight,
            RuntimeMetricInstanceStatus::DeadlineExceeded, RuntimeMetricInstanceStatus::ReplyClosed,
            RuntimeMetricInstanceStatus::RoundDeadlineSkipped]
        {
            assert_eq!(reporter.record(metadata, None, status), status);
            items.push(RuntimeMetricInstanceReport::new(&names, status, None));
            expected.push(("sample_state", status.code()));
        }
        assert_eq!(events.lock().unwrap().iter().map(|(name, value, _)| (*name, *value)).collect::<Vec<_>>(), expected);
        let report = RuntimeMetricCollectionReport::new(items);
        assert_eq!((report.active_count(), report.retired_count(), report.sampled_count()),
            (6 + out_of_range, 1, 2 + out_of_range));
        assert_eq!(report.count(RuntimeMetricInstanceStatus::RecordInvoked), 2);
        assert_eq!(report.count(RuntimeMetricInstanceStatus::MetricValueOutOfRange), out_of_range);
        if out_of_range != 0 { assert_eq!(report.instances()[2].counts(), Some((0, Some(usize::MAX)))); }
        for status in [RuntimeMetricInstanceStatus::Unregistered, RuntimeMetricInstanceStatus::RequestInFlight,
            RuntimeMetricInstanceStatus::DeadlineExceeded, RuntimeMetricInstanceStatus::ReplyClosed,
            RuntimeMetricInstanceStatus::RoundDeadlineSkipped]
        { assert_eq!(report.count(status), 1); }
    }

    /// MN-P01：直接调用真实initialize，不通过夹具补拼PID或重置全局记录器。
    /// 本测试独占本进程唯一初始化；其它Reporter单元只使用局部仪器，不触碰全局表。
    /// 覆盖校验失败不发布、256字节Unicode标记完整保留、PID精确十进制后缀、同参
    /// 幂等／异参拒绝及属性缓存不重建。不创建运行时／时钟，不代证SDK运输或远端接收。
    #[test]
    fn test_process_marker_pid_initialization_and_cached_attributes() {
        assert!(matches!(initialized(), Err(RuntimeMetricCollectionError::ReportingNotInitialized)));
        for invalid in ["", " ", "\t", "\n", "宿主\0", "宿主\u{7f}"] {
            assert_eq!(initialize(invalid), Err(RuntimeMetricReportingInitError::InvalidProcessInstance));
            assert!(matches!(initialized(), Err(RuntimeMetricCollectionError::ReportingNotInitialized)));
        }
        for overlong in ["x".repeat(257), "界".repeat(86)] {
            assert_eq!(initialize(&overlong), Err(RuntimeMetricReportingInitError::ProcessInstanceTooLong));
            assert!(matches!(initialized(), Err(RuntimeMetricCollectionError::ReportingNotInitialized)));
        }
        let marker = format!("{}x", "界".repeat(85));
        assert_eq!(marker.len(), 256);
        let expected = format!("{}-{}", marker, std::process::id());
        assert_eq!(initialize(&marker), Ok(()));
        let reporter = initialized().unwrap();
        assert_eq!(reporter.process.as_ref(), expected);
        assert!(reporter.process.len() > 256 && reporter.process.len() <= 267);
        let process = reporter.process.clone();
        let id = RuntimeMetricIdentitySlot::new().get_or_allocate().unwrap().id();
        let metadata = RuntimeMetricMetadata::new(id, "宿主标记测试", RuntimeMetricKind::Single);
        let attributes = metadata.labels(&reporter.process);
        let expected_attributes = [KeyValue::new("rt.id", id.as_u64() as i64),
            KeyValue::new("rt.name", "宿主标记测试"), KeyValue::new("rt.process", expected)];
        assert_eq!(attributes, &expected_attributes);
        for _ in 0..2 {
            assert_eq!(initialize(&marker), Ok(()));
            assert!(Arc::ptr_eq(&process, &initialized().unwrap().process));
            assert!(std::ptr::eq(attributes, metadata.labels(&initialized().unwrap().process)));
        }
        assert_eq!(initialize("其它宿主"), Err(RuntimeMetricReportingInitError::AlreadyInitialized));
        assert_eq!(initialize(""), Err(RuntimeMetricReportingInitError::InvalidProcessInstance));
        assert_eq!(initialize(&"x".repeat(257)), Err(RuntimeMetricReportingInitError::ProcessInstanceTooLong));
        assert!(Arc::ptr_eq(&process, &initialized().unwrap().process));
        assert_eq!(attributes, &expected_attributes);
    }
}
