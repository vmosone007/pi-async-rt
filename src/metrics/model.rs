//! MET-MODEL：公开纯类型和内部不可变描述符，不读取／调度runtime。
//!
//! 标签缓存仅构造纯KeyValue，不调用SDK仪器；表和记录层单向消费本层。
//! 合同：docs/RUNTIME_METRICS_IMPLEMENTATION_DESIGN.md；验证：本模块及runtime_metrics专项。

use std::{fmt, sync::Arc, time::Duration};
#[cfg(feature = "trace")]
use std::sync::OnceLock;
use super::identity::RuntimeMetricId;

/// 首次成功登记的实现类别；不是指标种类，也不作为额外远端标签。
/// Copy／比较为O(1)纯值操作，线程安全，无资源、等待或运行时依赖。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeMetricKind {
    /// 普通单线程核心。
    Single,
    /// 普通多线程核心。
    Multi,
    /// 工作者包装；与其内部Single共享身份，首次登记类别保持不变。
    Worker,
    /// 原生本地核心，采样只在原执行线程读取。
    Local,
}

/// 每实例采样／记录状态；code是稳定上报码，不承诺枚举ABI。
///
/// 状态是纯值，不取消任务、不释放许可。失败／注销不得补数量0；RecordInvoked也
/// 不证明远端接收。O(1)、无副作用或阻塞，自动Send／Sync，不含用户资源。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeMetricInstanceStatus {
    /// 历史已注销实例，本次未采样。
    Unregistered = 0,
    /// 合法数量已调用共享Gauge.record，不是网络回执。
    RecordInvoked = 1,
    /// 先前真实本地请求仍存活，本次没有投递。
    RequestInFlight = 2,
    /// 本次观察已达截止，不接受迟到读数。
    DeadlineExceeded = 3,
    /// 截止前观察回复通道无值关闭。
    ReplyClosed = 4,
    /// 整轮预算耗尽，本实例未尝试。
    RoundDeadlineSkipped = 5,
    /// 样本成功但有数字超过i64范围，没有记录任何数量。
    MetricValueOutOfRange = 6,
}

impl RuntimeMetricInstanceStatus {
    /// 返回0～6稳定数字码；纯函数、幂等、O(1)，不执行任何状态转换。
    pub const fn code(self) -> u64 { self as u64 }
}

macro_rules! metric_error {
    ($(#[$doc:meta])* $name:ident { $($(#[$variant_doc:meta])* $variant:ident => $message:literal),+ $(,)? }) => {
        $(#[$doc])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum $name { $($(#[$variant_doc])* $variant),+ }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(match self { $(Self::$variant => $message),+ })
            }
        }
        impl std::error::Error for $name {}
    };
}

metric_error! {
    /// 显式登记错误纯值；不持runtime、不提供隐式恢复，格式化仅写固定中文类别。
    /// Copy／比较O(1)无分配／等待；Display的输出成本由调用方格式化目标决定。
    RuntimeMetricRegistrationError {
        /// 空、纯空白或包含控制字符。
        InvalidName => "运行时指标名称无效",
        /// 名称超过256个UTF-8字节。
        NameTooLong => "运行时指标名称过长",
        /// 同一真实核心已固定为其它名称。
        NameConflict => "同一运行时核心的指标名称冲突",
        /// 累计1024个不同已登记核心的预算已耗尽。
        CapacityExceeded => "运行时指标累计系列容量已满",
        /// 正i64身份编号已耗尽，不能回绕。
        IdentityExhausted => "运行时指标身份已耗尽",
        /// 当前serial类型的线程受限资源未提供全局释放桥接。
        ThreadBoundRuntimeRequiresOwnerBridge => "该线程受限运行时暂不支持全局指标登记"
    }
}

metric_error! {
    /// 一次记录器初始化错误；纯类别不夹带SDK或用户资源，成本同其它指标错误。
    RuntimeMetricReportingInitError {
        /// 未启用trace，不创建仪器。
        ReportingDisabled => "未启用trace指标上报",
        /// 进程实例标识为空、纯空白或含控制字符。
        InvalidProcessInstance => "进程实例标识无效",
        /// 进程实例标识超过256个UTF-8字节。
        ProcessInstanceTooLong => "进程实例标识过长",
        /// 已成功初始化为其它进程标识。
        AlreadyInitialized => "指标上报已使用其它进程标识初始化",
        /// 并发或重入初始化立即拒绝，不等待自定义Provider回调。
        InitializationInProgress => "指标上报初始化正在执行"
    }
}

metric_error! {
    /// 整轮启动错误；不代替单实例失败，也不放宽getter／SDK／关闭线程合同。
    RuntimeMetricCollectionError {
        /// trace关闭，不投递采样任务。
        ReportingDisabled => "未启用trace指标上报",
        /// 尚未显式初始化记录器。
        ReportingNotInitialized => "指标记录器尚未初始化",
        /// 其它采集轮或本轮重入正在执行，立即拒绝、不排队。
        CollectionInProgress => "已有运行时指标采集正在执行",
        /// 实例或整轮预算为零。
        InvalidTimeout => "指标采集时间预算必须非零",
        /// 单调时钟无法表示指定截止，不自动退化为无限等待。
        DeadlineOverflow => "指标采集截止超出单调时钟范围"
    }
}

/// 外部同步采集的纯值预算；不是timer／runtime／线程句柄。
///
/// 默认每实例100ms、整轮3s，实例预算裁剪到整轮余量。new拒绝零，实际Instant溢出
/// 在采集前检查。构造／读取／复制O(1)，无分配、时钟读取、阻塞或副作用；预算不
/// 能抢占getter／SDK／Default／Drop或OS调度，不能作为整函数硬实时保证。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeMetricCollectionOptions {
    instance_timeout: Duration,
    round_timeout: Duration,
}

impl RuntimeMetricCollectionOptions {
    /// 原样保存两个非零预算；任一零返回InvalidTimeout，无runtime或全局副作用。
    pub fn new(instance_timeout: Duration, round_timeout: Duration)
        -> Result<Self, RuntimeMetricCollectionError>
    {
        if instance_timeout.is_zero() || round_timeout.is_zero() {
            return Err(RuntimeMetricCollectionError::InvalidTimeout);
        }
        Ok(Self { instance_timeout, round_timeout })
    }

    /// 只读实例预算，O(1)纯函数；实际截止还受整轮余量约束。
    pub const fn instance_timeout(&self) -> Duration { self.instance_timeout }

    /// 只读整轮预算，O(1)纯函数，不重新计时或重置进行中的轮次。
    pub const fn round_timeout(&self) -> Duration { self.round_timeout }
}

impl Default for RuntimeMetricCollectionOptions {
    fn default() -> Self {
        Self { instance_timeout: Duration::from_millis(100), round_timeout: Duration::from_secs(3) }
    }
}

/// 独立拥有的实例报告，不持Source／runtime／锁或可写控制状态。
///
/// getter均O(1)只读，无调度／分配／阻塞；名称借用受self生命期约束，其余返回纯值。
/// counts仅样本成功有值，包含记录越界；不是全局同瞬间快照或全部Pending数量。
/// 自动Send／Sync，析构仅释放数字和共享名称，不能取消仍存活的采样任务。
#[derive(Debug)]
pub struct RuntimeMetricInstanceReport {
    id: RuntimeMetricId,
    name: Arc<str>,
    kind: RuntimeMetricKind,
    status: RuntimeMetricInstanceStatus,
    counts: Option<(usize, Option<usize>)>,
}

impl RuntimeMetricInstanceReport {
    /// 返回本实例不复用的指标ID，不进行查表。
    pub const fn id(&self) -> RuntimeMetricId { self.id }
    /// 返回登记时的完整名称，只读、不复制字符串。
    pub fn name(&self) -> &str { &self.name }
    /// 返回首次登记类别，包装／重登记不重写。
    pub const fn kind(&self) -> RuntimeMetricKind { self.kind }
    /// 返回本轮观测／记录类别，不是远端交付确认。
    pub const fn status(&self) -> RuntimeMetricInstanceStatus { self.status }
    /// 返回原样本；外层None表示未得样本，内层None只表示没有wait_len接口。
    pub const fn counts(&self) -> Option<(usize, Option<usize>)> { self.counts }

    /// O(1)保存本次纯结果并Clone共享名称，不保留描述符／Source或触发新采样。
    /// 调用方保证counts与status对应；记录越界仍必须保留原成功样本。
    pub(super) fn new(metadata: &RuntimeMetricMetadata, status: RuntimeMetricInstanceStatus,
        counts: Option<(usize, Option<usize>)>) -> Self
    {
        Self { id: metadata.id, name: metadata.name.clone(), kind: metadata.kind, status, counts }
    }
}

/// 一轮不可变报告，含当前活动及历史退休实例，按稳定ID排序。
///
/// 形成O(C)时间／空间，C≤1024；只读接口O(1)，无runtime访问／阻塞。报告不保活
/// 来源，自动Send／Sync，析构释放自己的Vec／名称引用；成功返回不等于所有实例成功。
#[derive(Debug)]
pub struct RuntimeMetricCollectionReport {
    instances: Vec<RuntimeMetricInstanceReport>,
    statuses: [usize; 7],
}

impl RuntimeMetricCollectionReport {
    /// 只读整个稳定顺序报告，不开放修改、runtime或可写统计状态。
    pub fn instances(&self) -> &[RuntimeMetricInstanceReport] { &self.instances }
    /// 返回指定状态精确数量，O(1)纯查询，不触发新采样。
    pub fn count(&self, status: RuntimeMetricInstanceStatus) -> usize { self.statuses[status.code() as usize] }
    /// 返回本轮快照中的活动实例数，包含失败及未尝试，不含退休。
    pub fn active_count(&self) -> usize { self.instances.len() - self.retired_count() }
    /// 返回本轮快照中的退休实例数，不推断底层资源已释放。
    pub fn retired_count(&self) -> usize { self.count(RuntimeMetricInstanceStatus::Unregistered) }
    /// 返回成功读取样本的实例数，包含随后记录范围失败。
    pub fn sampled_count(&self) -> usize {
        self.count(RuntimeMetricInstanceStatus::RecordInvoked) + self.count(RuntimeMetricInstanceStatus::MetricValueOutOfRange)
    }
    /// 消费现有报告Vec，以O(C)遍历建立精确七分类汇总，不再复制实例或访问来源。
    pub(super) fn new(instances: Vec<RuntimeMetricInstanceReport>) -> Self {
        let mut statuses = [0; 7];
        for instance in &instances { statuses[instance.status.code() as usize] += 1; }
        Self { instances, statuses }
    }
}

pub(super) const SERIES_CAPACITY: usize = 1024;

/// 仅归属本层的不可变元数据；标签惰性缓存只初始化纯值，绝不调用SDK仪器／上层。
pub(super) struct RuntimeMetricMetadata {
    id: RuntimeMetricId,
    name: Arc<str>,
    kind: RuntimeMetricKind,
    #[cfg(feature = "trace")]
    labels: OnceLock<[opentelemetry::KeyValue; 3]>,
}

impl RuntimeMetricMetadata {
    /// O(n)复制已验证名称到共享只读存储；ID／类别为纯值，不保活runtime或执行回调。
    pub(super) fn new(id: RuntimeMetricId, name: &str, kind: RuntimeMetricKind) -> Self {
        Self { id, name: Arc::from(name), kind, #[cfg(feature = "trace")] labels: OnceLock::new() }
    }
    /// O(1)纯值身份查询，不提供反查／修改核心能力。
    pub(super) fn id(&self) -> RuntimeMetricId { self.id }
    /// O(1)只读名称借用，不分配、重命名或推进任何状态。
    pub(super) fn name(&self) -> &str { &self.name }

    /// 首次固定三字段初始化并克隆共享字符串Arc，随后O(1)借用，不复制字符串字节。
    /// process只来自成功唯一初始化的Reporter，不允许用同一描述符切换进程身份。
    /// 初始化闭包只构造纯值，无SDK／用户回调，不可能通过本方法重入OnceLock。
    #[cfg(feature = "trace")]
    pub(super) fn labels(&self, process: &Arc<str>) -> &[opentelemetry::KeyValue; 3] {
        self.labels.get_or_init(|| [
            opentelemetry::KeyValue::new("rt.id", self.id.as_u64() as i64),
            opentelemetry::KeyValue::new("rt.name", self.name.clone()),
            opentelemetry::KeyValue::new("rt.process", process.clone()),
        ])
    }
}

/// 私有纯校验事实，不用bool混淆超长与非法内容；映射到公开错误由各入口负责。
pub(super) enum LabelValidationError { TooLong, Invalid }

/// 名称与进程标识的纯校验；先检查字节上限，再查空白／控制，O(n)、无分配／副作用。
pub(super) fn validate_label(value: &str) -> Result<(), LabelValidationError> {
    if value.len() > 256 { return Err(LabelValidationError::TooLong); }
    if value.trim().is_empty() || value.chars().any(char::is_control) { return Err(LabelValidationError::Invalid); }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::identity::RuntimeMetricIdentitySlot;

    /// MET-MODEL：独立字符／字节矩阵，保留Unicode原文，明确边界而不是字符数限制。
    #[test]
    fn test_label_and_options_boundaries() {
        for invalid in ["", " ", "\t", "\n", "\u{2003}", "a\0b", "a\u{7f}", "a\r"] {
            assert!(matches!(validate_label(invalid), Err(LabelValidationError::Invalid)));
        }
        for valid in ["a", " gateway ", "网关", "a-b_1"] { assert!(validate_label(valid).is_ok()); }
        assert!(validate_label(&"x".repeat(256)).is_ok());
        assert!(validate_label(&"界".repeat(85)).is_ok());
        for invalid in ["x".repeat(257), "界".repeat(86)] {
            assert!(matches!(validate_label(&invalid), Err(LabelValidationError::TooLong)));
        }
        for a in [Duration::ZERO, Duration::from_nanos(1), Duration::MAX] {
            for b in [Duration::ZERO, Duration::from_nanos(1), Duration::MAX] {
                let options = RuntimeMetricCollectionOptions::new(a, b);
                if a.is_zero() || b.is_zero() {
                    assert_eq!(options, Err(RuntimeMetricCollectionError::InvalidTimeout));
                } else {
                    let options = options.unwrap();
                    assert_eq!((options.instance_timeout(), options.round_timeout()), (a, b));
                }
            }
        }
        let default = RuntimeMetricCollectionOptions::default();
        assert_eq!((default.instance_timeout(), default.round_timeout()),
            (Duration::from_millis(100), Duration::from_secs(3)));
    }

    /// MET-MODEL：七种状态稳定码、样本与记录越界分离、精确汇总及独立报告生命期。
    #[test]
    fn test_report_statuses_and_counts_do_not_retain_source() {
        let statuses = [RuntimeMetricInstanceStatus::Unregistered, RuntimeMetricInstanceStatus::RecordInvoked,
            RuntimeMetricInstanceStatus::RequestInFlight, RuntimeMetricInstanceStatus::DeadlineExceeded,
            RuntimeMetricInstanceStatus::ReplyClosed, RuntimeMetricInstanceStatus::RoundDeadlineSkipped,
            RuntimeMetricInstanceStatus::MetricValueOutOfRange];
        let id = RuntimeMetricIdentitySlot::new().get_or_allocate().unwrap().id();
        let report = {
            let metadata = RuntimeMetricMetadata::new(id, "网关", RuntimeMetricKind::Local);
            let items = statuses.iter().enumerate().map(|(index, &status)| {
                assert_eq!(status.code(), index as u64);
                let counts = if matches!(status, RuntimeMetricInstanceStatus::RecordInvoked
                    | RuntimeMetricInstanceStatus::MetricValueOutOfRange) { Some((usize::MAX, None)) } else { None };
                RuntimeMetricInstanceReport::new(&metadata, status, counts)
            }).collect();
            RuntimeMetricCollectionReport::new(items)
        };
        assert_eq!((report.active_count(), report.retired_count(), report.sampled_count()), (6, 1, 2));
        for (&status, item) in statuses.iter().zip(report.instances()) {
            assert_eq!(report.count(status), 1);
            assert_eq!((item.id(), item.name(), item.kind(), item.status()), (id, "网关", RuntimeMetricKind::Local, status));
        }
        assert_eq!(report.instances()[6].counts(), Some((usize::MAX, None)));
        let empty = RuntimeMetricCollectionReport::new(Vec::new());
        assert_eq!((empty.active_count(), empty.retired_count(), empty.sampled_count()), (0, 0, 0));
    }
}
