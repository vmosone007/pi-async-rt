//! MET-MODEL：公开纯类型和内部不可变描述符，不读取／调度runtime。
//!
//! 标签缓存仅构造纯KeyValue，不调用SDK仪器；表和记录层单向消费本层。
//! 合同：docs/RUNTIME_METRICS_IMPLEMENTATION_DESIGN.md；验证：本模块及runtime_metrics专项。
//! 多名称纯值边界MN-M01／MN-M02见docs/RUNTIME_METRICS_MULTI_NAME_MODEL_DESIGN.md：
//! v2将原始名称与合并上报描述符分开；报告共享不可变纯元数据，不保活来源或反查表。

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
        /// 原始名称或按首次登记顺序用-连接的完整名称超过256个UTF-8字节。
        NameTooLong => "运行时指标名称过长",
        /// 保留旧错误变体以兼容；合法同核心异名现在追加，不再返回本错误。
        NameConflict => "同一运行时核心的指标名称冲突",
        /// 累计1024份不同(核心ID,原始名称)预算已耗尽；同名重复免费，注销不返还。
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
/// getter及构造名称迭代器均O(1)只读，无调度／分配／阻塞；遍历A个名称为O(A)。
/// 名称借用受self生命期约束，其余返回纯值；后续登记不改变本报告的名称集合。
/// counts仅样本成功有值，包含记录越界；不是全局同瞬间快照或全部Pending数量。
/// 自动Send／Sync，析构仅释放数字和共享纯描述符，不能取消仍存活的采样任务。
/// 最后集合引用的析构可释放A项及名称／标签存储；分配器内部时延沿用标准库边界。
/// MN-M02：一核心一报告，多个名称只是完整读数的不同视图，不得按名称重复相加。
#[derive(Debug)]
pub struct RuntimeMetricInstanceReport {
    id: RuntimeMetricId,
    names: RuntimeMetricNames,
    kind: RuntimeMetricKind,
    status: RuntimeMetricInstanceStatus,
    counts: Option<(usize, Option<usize>)>,
}

impl RuntimeMetricInstanceReport {
    /// 返回本实例不复用的指标ID，不进行查表。
    pub const fn id(&self) -> RuntimeMetricId { self.id }
    /// 返回首次成功登记的完整名称；追加名称不重排它，旧单名称读取方式保持不变。
    /// O(1)纯查询、无分配／引用计数／阻塞；完整观测名称请使用names，不查当前注册表。
    pub fn name(&self) -> &str { self.names.primary().name() }
    /// 按本报告快照中的首次加入顺序借用全部观测名称，至少一项，不复制字符串。
    ///
    /// MN-M02：返回精确长度、可双向遍历的只读迭代器；创建及单步O(1)，全遍历O(A)，
    /// A为本核名称数。纯函数、重复读取幂等，无分配／引用计数／锁／调度／等待，
    /// 不要求trace、时钟或运行时；借用不能跨越self析构，追加／注销不修改旧报告。
    /// 可在任意合法线程及Future内读取已有报告，但不因此允许在Future内同步采集。
    /// 不暴露内部Arc、缓存或控制状态；各名称共享原counts，不表示独占任务贡献。
    ///
    /// ```
    /// use pi_async_rt::runtime_metrics::RuntimeMetricInstanceReport;
    /// fn has_name(report: &RuntimeMetricInstanceReport, expected: &str) -> bool {
    ///     report.names().any(|name| name == expected)
    /// }
    /// ```
    pub fn names(&self) -> impl ExactSizeIterator<Item = &str> + DoubleEndedIterator + '_ {
        self.names.iter().map(RuntimeMetricMetadata::name)
    }
    /// 返回首次登记类别，包装／重登记不重写。
    pub const fn kind(&self) -> RuntimeMetricKind { self.kind }
    /// 返回本轮观测／记录类别，不是远端交付确认。
    pub const fn status(&self) -> RuntimeMetricInstanceStatus { self.status }
    /// 返回原样本；外层None表示未得样本，内层None只表示没有wait_len接口。
    pub const fn counts(&self) -> Option<(usize, Option<usize>)> { self.counts }

    /// O(1)保存本次纯结果并Clone一次名称集合Arc，不重建名称清单或触发新采样。
    /// 只持纯描述符，不持Source／runtime／许可；ID／类别继承集合首项，保持const getter。
    /// 调用方保证counts与status对应；记录越界仍必须保留原成功样本。
    pub(super) fn new(names: &RuntimeMetricNames, status: RuntimeMetricInstanceStatus,
        counts: Option<(usize, Option<usize>)>) -> Self
    {
        let primary = names.primary();
        Self { id: primary.id, names: names.clone(), kind: primary.kind, status, counts }
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

/// MN-M01：同一核心的非空、去重、按首次加入顺序排列的不可变观测名称集合。
///
/// 私有字段只保存纯描述符Arc，不持Source／runtime／许可或回调；读取和最终析构
/// 不推进运行时。new产生单项，with_name只能沿用首项ID／类别并追加，故不需要
/// 暴露可写Vec、空集合构造器或跨模块修补不变量的接口。旧快照不会随追加改变。
/// v2的reported单独借用合并展示名；首名与原始列表不受拼接影响，单名复用描述符。
/// 合并值最长256 UTF-8字节，含分隔符；不能通过拆分它恢复原始名称或代替rt.id。
///
/// 自动Send／Sync、无unsafe及控制锁；原描述符OnceLock仍只缓存纯标签，不调用SDK。
/// getter为幂等纯查询；new／with_name／Clone有冷分配或引用计数副作用，不是严格纯函数。
/// 分配失败沿用标准库行为，不捕获panic；本层不负责登记容量、发布或原用户资源释放。
/// 分配／最终释放不保证标准分配器内部无锁或严格时延；本类型不增加控制锁或后台推进者。
#[derive(Clone)]
pub(super) struct RuntimeMetricNames(Arc<RuntimeMetricNameSet>);

/// 本模块唯一构造的纯组；字段不向注册表／记录器开放，避免跨边界改写原始名称。
/// 下一组只复用entries中的原始描述符，不持上一组reported，旧合并缓存由旧快照释放。
struct RuntimeMetricNameSet {
    entries: Box<[Arc<RuntimeMetricMetadata>]>,
    reported: Arc<RuntimeMetricMetadata>,
}

impl RuntimeMetricNames {
    /// 消费一份已验证描述符的Arc，O(1)构造非空集合，不复制名称字节或初始化标签。
    /// 冷分配组与切片；原始首项和reported共享同一描述符，不读全局状态／runtime。
    /// 名称合法性及不超过256字节由原注册入口validate_label保证。
    pub(super) fn new(metadata: Arc<RuntimeMetricMetadata>) -> Self {
        Self(Arc::new(RuntimeMetricNameSet {
            entries: vec![metadata.clone()].into_boxed_slice(),
            reported: metadata,
        }))
    }

    /// 借用首份纯描述符；O(1)、无分配／锁／副作用，不提供可写状态或运行时能力。
    /// 非空由本类型两个构造路径保证，后续追加／整体恢复均不重排首项。
    pub(super) fn primary(&self) -> &RuntimeMetricMetadata { &self.0.entries[0] }

    /// 返回本集合名称数量，始终非零；O(1)幂等纯查询，不等于运行时核心数量。
    pub(super) fn len(&self) -> usize { self.0.entries.len() }

    /// 借用本组唯一上报描述符，O(1)纯查询，不Clone、分配、加锁或调用SDK／运行时。
    /// 生命周期受self约束，不返回写权限；记录层每核心每指标只使用这一份，而非逐名记录。
    pub(super) fn reported(&self) -> &RuntimeMetricMetadata { &self.0.reported }

    /// 原文去重后返回集合：同名仅Clone原集合；异名追加到新集合，复用旧描述符／缓存。
    ///
    /// 输入须已通过原validate_label；不裁剪、归一或转义。只继承primary的ID／类别，
    /// 不能混入另一核心。新合并长度含一个半角-，超过256返回NameTooLong，不分配
    /// 新组或改变旧值；同名优先成功，即使合并长度已满。每项合法输入及旧组合至多
    /// 256字节，长度相加至多513，不存在usize溢出。专项：MN-V2-T01～03。
    /// 最多A次比较、最坏O(A*n)；异名另需O(A+L+n)时间／新空间，L为旧合并字节数，
    /// n为新名称字节数。旧版本仍被持有时，其各自切片和合并缓存继续保留。
    /// 有冷分配／Arc成本，但无控制锁、等待、用户代码或SDK；不表示已成功全局登记，
    /// 登记预算及原子发布仍由注册表负责。旧集合在正常返回或展开时均不被原地修改。
    pub(super) fn with_name(&self, name: &str) -> Result<Self, RuntimeMetricRegistrationError> {
        if self.iter().any(|metadata| metadata.name() == name) { return Ok(self.clone()); }
        let combined_len = self.reported().name().len() + 1 + name.len();
        if combined_len > 256 { return Err(RuntimeMetricRegistrationError::NameTooLong); }
        let primary = self.primary();
        let mut next = Vec::with_capacity(self.len() + 1);
        next.extend(self.0.entries.iter().cloned());
        next.push(Arc::new(RuntimeMetricMetadata::new(primary.id, name, primary.kind)));
        let mut combined = String::with_capacity(combined_len);
        combined.push_str(self.reported().name());
        combined.push('-');
        combined.push_str(name);
        Ok(Self(Arc::new(RuntimeMetricNameSet {
            entries: next.into_boxed_slice(),
            reported: Arc::new(RuntimeMetricMetadata::new(primary.id, &combined, primary.kind)),
        })))
    }

    /// 仅借用纯描述符，不克隆Arc或调用标签缓存；创建／单步O(1)、全遍历O(A)。
    /// 精确长度、可双向遍历；返回值不超出self生命期，不暴露集合的写入能力。
    pub(super) fn iter(&self)
        -> impl ExactSizeIterator<Item = &RuntimeMetricMetadata> + DoubleEndedIterator + '_
    {
        self.0.entries.iter().map(|metadata| metadata.as_ref())
    }
}

impl fmt::Debug for RuntimeMetricNames {
    /// 只格式化完整名称，不展示标签缓存；O(A及输出字节数)，输出目标可自行阻塞或失败。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter().map(RuntimeMetricMetadata::name)).finish()
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
            let metadata = RuntimeMetricNames::new(Arc::new(RuntimeMetricMetadata::new(id, "网关", RuntimeMetricKind::Local)));
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

    /// MN-D02夹具只构造合法纯输入，使用原身份分配和描述符；不模拟注册或采样。
    fn name_metadata(name: &str, kind: RuntimeMetricKind) -> Arc<RuntimeMetricMetadata> {
        assert!(validate_label(name).is_ok());
        let id = RuntimeMetricIdentitySlot::new().get_or_allocate().unwrap().id();
        Arc::new(RuntimeMetricMetadata::new(id, name, kind))
    }

    /// MN-T01：合法字节边界及全部类别的单项集合非空，复用原描述符和完整名称。
    #[test]
    fn test_names_singleton_preserves_validated_input() {
        let cases = ["r".to_owned(), " rpc ".to_owned(), "网关".to_owned(),
            "x".repeat(255), "x".repeat(256), "界".repeat(85)];
        for kind in [RuntimeMetricKind::Single, RuntimeMetricKind::Multi,
            RuntimeMetricKind::Worker, RuntimeMetricKind::Local]
        {
            for name in &cases {
                let metadata = name_metadata(name, kind);
                let names = RuntimeMetricNames::new(metadata.clone());
                assert_eq!(names.len(), 1);
                assert!(std::ptr::eq(names.primary(), metadata.as_ref()));
                assert_eq!((names.primary().id(), names.primary().name(), names.primary().kind),
                    (metadata.id(), name.as_str(), kind));
                assert_eq!(names.iter().map(RuntimeMetricMetadata::name).collect::<Vec<_>>(), vec![name.as_str()]);
                assert_eq!(names.iter().size_hint(), (1, Some(1)));
            }
        }
    }

    /// MN-T02：同名复用原集合；异名追加共享旧描述符，旧快照不能被追加动作修改。
    #[test]
    fn test_names_deduplicate_and_share_immutable_snapshots() {
        let metadata = name_metadata("rpc", RuntimeMetricKind::Single);
        let original = RuntimeMetricNames::new(metadata.clone());
        let duplicate = original.with_name("rpc").unwrap();
        assert!(Arc::ptr_eq(&original.0, &duplicate.0));
        assert_eq!(Arc::strong_count(&metadata), 3);
        let expanded = original.with_name("storage").unwrap();
        assert!(!Arc::ptr_eq(&original.0, &expanded.0));
        assert!(std::ptr::eq(original.primary(), expanded.primary()));
        assert_eq!(Arc::strong_count(&metadata), 4);
        assert_eq!(original.iter().map(RuntimeMetricMetadata::name).collect::<Vec<_>>(), vec!["rpc"]);
        assert_eq!(expanded.iter().map(RuntimeMetricMetadata::name).collect::<Vec<_>>(), vec!["rpc", "storage"]);
        for name in ["rpc", "storage"] {
            let duplicate = expanded.with_name(name).unwrap();
            assert!(Arc::ptr_eq(&expanded.0, &duplicate.0));
            assert_eq!(duplicate.len(), 2);
        }
    }

    /// MN-T03：不归一名称；全部类别保持身份、首名及首次加入顺序，重复不重排。
    #[test]
    fn test_names_order_exact_strings_and_core_identity() {
        let expected = ["rpc", "RPC", " rpc", "rpc ", "网关", "网关\u{301}"];
        for kind in [RuntimeMetricKind::Single, RuntimeMetricKind::Multi,
            RuntimeMetricKind::Worker, RuntimeMetricKind::Local]
        {
            let metadata = name_metadata(expected[0], kind);
            let id = metadata.id();
            let mut names = RuntimeMetricNames::new(metadata);
            for name in &expected[1..] { names = names.with_name(name).unwrap(); }
            for name in expected.iter().rev() { names = names.with_name(name).unwrap(); }
            assert_eq!(names.len(), expected.len());
            assert_eq!(names.primary().name(), expected[0]);
            assert_eq!(names.iter().map(RuntimeMetricMetadata::name).collect::<Vec<_>>(), expected);
            for item in names.iter() { assert_eq!((item.id(), item.kind), (id, kind)); }
        }
    }

    /// MN-T04：只测真实纯标签缓存；不安装Provider，不把本用例当SDK或OTLP证据。
    #[cfg(feature = "trace")]
    #[test]
    fn test_names_append_preserves_cached_labels_and_exact_attributes() {
        use opentelemetry::KeyValue;
        let metadata = name_metadata("rpc", RuntimeMetricKind::Local);
        let process: Arc<str> = Arc::from("进程启动实例");
        let cached = metadata.labels(&process).as_ptr();
        let original = RuntimeMetricNames::new(metadata.clone());
        let expanded = original.with_name("storage").unwrap().with_name("网关").unwrap();
        assert_eq!(expanded.primary().labels(&process).as_ptr(), cached);
        assert!(std::ptr::eq(original.primary(), expanded.primary()));
        for item in expanded.iter() {
            let expected = [KeyValue::new("rt.id", metadata.id().as_u64() as i64),
                KeyValue::new("rt.name", item.name().to_owned()),
                KeyValue::new("rt.process", "进程启动实例")];
            assert_eq!(item.labels(&process), &expected);
            assert_eq!(item.labels(&process).as_ptr(), item.labels(&process).as_ptr());
        }
        let duplicate = expanded.with_name("storage").unwrap();
        assert!(Arc::ptr_eq(&expanded.0, &duplicate.0));
        for (left, right) in expanded.iter().zip(duplicate.iter()) {
            assert!(std::ptr::eq(left, right));
            assert_eq!(left.labels(&process).as_ptr(), right.labels(&process).as_ptr());
        }
        assert_eq!(metadata.labels(&process).as_ptr(), cached);
    }

    /// MN-T05：报告只共享纯集合；追加不改旧报告，构造变量释放后可读，最终不遗留引用。
    #[test]
    fn test_names_report_primary_compatibility_snapshot_and_lifetime() {
        let cases = [(RuntimeMetricInstanceStatus::RecordInvoked, Some((0, Some(0)))),
            (RuntimeMetricInstanceStatus::RecordInvoked, Some((7, None))),
            (RuntimeMetricInstanceStatus::MetricValueOutOfRange, Some((usize::MAX, Some(usize::MAX)))),
            (RuntimeMetricInstanceStatus::DeadlineExceeded, None)];
        for kind in [RuntimeMetricKind::Single, RuntimeMetricKind::Multi,
            RuntimeMetricKind::Worker, RuntimeMetricKind::Local]
        {
            for (status, counts) in cases {
                let (old, current, weak, id) = {
                    let metadata = name_metadata("rpc", kind);
                    let (id, weak) = (metadata.id(), Arc::downgrade(&metadata));
                    let names = RuntimeMetricNames::new(metadata);
                    let old = RuntimeMetricInstanceReport::new(&names, status, counts);
                    let expanded = names.with_name("storage").unwrap().with_name("业务").unwrap();
                    let current = RuntimeMetricInstanceReport::new(&expanded, status, counts);
                    assert!(Arc::ptr_eq(&old.names.0, &names.0));
                    assert!(Arc::ptr_eq(&current.names.0, &expanded.0));
                    (old, current, weak, id)
                };
                assert_eq!(old.names().collect::<Vec<_>>(), vec!["rpc"]);
                assert_eq!(current.names().collect::<Vec<_>>(), vec!["rpc", "storage", "业务"]);
                for report in [&old, &current] {
                    assert_eq!((report.id(), report.name(), report.kind(), report.status(), report.counts()),
                        (id, "rpc", kind, status, counts));
                    let metadata = weak.upgrade().unwrap();
                    assert_eq!(report.name().as_ptr(), metadata.name().as_ptr());
                }
                drop(old);
                assert!(weak.upgrade().is_some());
                drop(current);
                assert!(weak.upgrade().is_none());
            }
        }
    }

    /// MN-T06：名称不是核心；空／单核心／七码混合均按核心精确计数，保留原读数类别。
    #[test]
    fn test_names_reports_keep_core_count_and_raw_sample_semantics() {
        let statuses = [RuntimeMetricInstanceStatus::Unregistered, RuntimeMetricInstanceStatus::RecordInvoked,
            RuntimeMetricInstanceStatus::RequestInFlight, RuntimeMetricInstanceStatus::DeadlineExceeded,
            RuntimeMetricInstanceStatus::ReplyClosed, RuntimeMetricInstanceStatus::RoundDeadlineSkipped,
            RuntimeMetricInstanceStatus::MetricValueOutOfRange];
        let mut items = Vec::new();
        for (index, &status) in statuses.iter().enumerate() {
            let mut names = RuntimeMetricNames::new(name_metadata("首名", RuntimeMetricKind::Single));
            for alias in 0..index % 3 { names = names.with_name(&format!("别名{}", alias)).unwrap(); }
            let counts = match status {
                RuntimeMetricInstanceStatus::RecordInvoked => Some((0, Some(0))),
                RuntimeMetricInstanceStatus::MetricValueOutOfRange => Some((usize::MAX, None)),
                _ => None,
            };
            items.push(RuntimeMetricInstanceReport::new(&names, status, counts));
        }
        let report = RuntimeMetricCollectionReport::new(items);
        assert_eq!(report.instances().len(), 7);
        assert_eq!(report.instances().iter().map(|item| item.names().len()).sum::<usize>(), 13);
        assert_eq!((report.active_count(), report.retired_count(), report.sampled_count()), (6, 1, 2));
        for (&status, item) in statuses.iter().zip(report.instances()) {
            assert_eq!(report.count(status), 1);
            assert_eq!(item.status(), status);
            let expected = match status {
                RuntimeMetricInstanceStatus::RecordInvoked => Some((0, Some(0))),
                RuntimeMetricInstanceStatus::MetricValueOutOfRange => Some((usize::MAX, None)),
                _ => None,
            };
            assert_eq!(item.counts(), expected);
        }
        let names = RuntimeMetricNames::new(name_metadata("一个核心", RuntimeMetricKind::Multi)).with_name("另一个视图").unwrap();
        let single = RuntimeMetricCollectionReport::new(vec![RuntimeMetricInstanceReport::new(&names,
            RuntimeMetricInstanceStatus::RecordInvoked, Some((3, Some(2))))]);
        assert_eq!((single.instances().len(), single.active_count(), single.sampled_count()), (1, 1, 1));
        let empty = RuntimeMetricCollectionReport::new(Vec::new());
        assert!(empty.instances().is_empty());
        assert_eq!((empty.active_count(), empty.retired_count(), empty.sampled_count()), (0, 0, 0));
        for status in statuses { assert_eq!(empty.count(status), 0); }
    }

    /// MN-T07：公开迭代器精确长度、前后向组合、极大跳过及借用重读；不代建名称列表。
    #[test]
    fn test_names_iterator_boundaries_and_standard_composition() {
        fn borrowed_names<'a>(report: &'a RuntimeMetricInstanceReport)
            -> impl ExactSizeIterator<Item = &'a str> + DoubleEndedIterator + 'a
        { report.names() }
        let names = RuntimeMetricNames::new(name_metadata("rpc", RuntimeMetricKind::Worker))
            .with_name("storage").unwrap().with_name("metrics").unwrap().with_name("observer").unwrap();
        let report = RuntimeMetricInstanceReport::new(&names, RuntimeMetricInstanceStatus::RequestInFlight, None);
        let mut values = borrowed_names(&report);
        assert_eq!((values.len(), values.size_hint()), (4, (4, Some(4))));
        assert_eq!(values.next(), Some("rpc"));
        assert_eq!(values.len(), 3);
        assert_eq!(values.next_back(), Some("observer"));
        assert_eq!((values.len(), values.size_hint()), (2, (2, Some(2))));
        assert_eq!(values.nth(0), Some("storage"));
        assert_eq!(values.nth_back(0), Some("metrics"));
        assert_eq!((values.len(), values.size_hint()), (0, (0, Some(0))));
        assert_eq!(values.next(), None);
        assert_eq!(values.next_back(), None);
        let mut skipped = report.names();
        assert_eq!(skipped.nth(usize::MAX), None);
        assert_eq!(skipped.len(), 0);
        assert_eq!(skipped.next_back(), None);
        assert_eq!(report.names().rev().collect::<Vec<_>>(), vec!["observer", "metrics", "storage", "rpc"]);
        assert_eq!(report.names().zip(0..).collect::<Vec<_>>(),
            vec![("rpc", 0), ("storage", 1), ("metrics", 2), ("observer", 3)]);
        assert_eq!(report.name(), "rpc");
        assert_eq!(report.names().len(), 4);
    }

    /// MN-T08：验证实际纯类型自动Send／Sync及两个线程只读，不赋予本地runtime跨线程权限。
    #[test]
    fn test_names_pure_types_are_send_sync_and_share_read_only() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RuntimeMetricMetadata>();
        assert_send_sync::<RuntimeMetricNames>();
        assert_send_sync::<RuntimeMetricInstanceReport>();
        assert_send_sync::<RuntimeMetricCollectionReport>();
        let names = RuntimeMetricNames::new(name_metadata("rpc", RuntimeMetricKind::Local)).with_name("storage").unwrap();
        let report = RuntimeMetricInstanceReport::new(&names, RuntimeMetricInstanceStatus::RecordInvoked, Some((2, None)));
        std::thread::scope(|scope| {
            let first = scope.spawn(|| {
                assert_eq!(names.iter().map(RuntimeMetricMetadata::name).collect::<Vec<_>>(), vec!["rpc", "storage"]);
                assert_eq!(report.names().collect::<Vec<_>>(), vec!["rpc", "storage"]);
            });
            let second = scope.spawn(|| {
                assert_eq!((report.name(), report.kind(), report.counts()), ("rpc", RuntimeMetricKind::Local, Some((2, None))));
                assert_eq!(report.names().rev().collect::<Vec<_>>(), vec!["storage", "rpc"]);
            });
            first.join().unwrap();
            second.join().unwrap();
        });
    }

    /// MN-V2-T01：只驱动真实纯值构造，独立核验字节长度而非字符数。
    /// 覆盖255／256成功与257／258拒绝、满长重复、Unicode、旧组及旧报告不被失败改变。
    #[test]
    fn test_combined_name_byte_boundaries_preserve_old_snapshots() {
        for first in ["x".repeat(253), "x".repeat(254), "x".repeat(255), "x".repeat(256),
            format!("{}a", "界".repeat(84)), "界".repeat(85)]
        {
            let names = RuntimeMetricNames::new(name_metadata(&first, RuntimeMetricKind::Local));
            let old = RuntimeMetricInstanceReport::new(&names, RuntimeMetricInstanceStatus::RecordInvoked,
                Some((7, None)));
            let duplicate = names.with_name(&first).unwrap();
            assert!(Arc::ptr_eq(&names.0, &duplicate.0));
            let next = names.with_name("y");
            if first.len() + 2 <= 256 {
                let next = next.unwrap();
                assert_eq!(next.reported().name(), format!("{}-y", first));
                assert_eq!(next.reported().name().len(), first.len() + 2);
                assert_eq!(next.iter().map(RuntimeMetricMetadata::name).collect::<Vec<_>>(), vec![first.as_str(), "y"]);
                assert_eq!((next.reported().id(), next.reported().kind), (names.primary().id(), RuntimeMetricKind::Local));
            } else {
                assert!(matches!(next, Err(RuntimeMetricRegistrationError::NameTooLong)));
            }
            assert_eq!(names.reported().name(), first);
            assert!(std::ptr::eq(names.primary(), names.reported()));
            assert_eq!(old.names().collect::<Vec<_>>(), vec![first.as_str()]);
            assert_eq!((old.name(), old.counts()), (first.as_str(), Some((7, None))));
        }
    }

    /// MN-V2-T02：合并名只展示、不反解或替代核心身份；原始名称含连字符仍合法。
    /// 两组可显示相同字符串，但原始列表、ID、首名保持各自权威语义。
    #[test]
    fn test_combined_name_keeps_raw_aliases_and_distinct_core_identity() {
        let first = RuntimeMetricNames::new(name_metadata("a-b", RuntimeMetricKind::Multi));
        assert!(std::ptr::eq(first.primary(), first.reported()));
        let first = first.with_name("c").unwrap();
        let second = RuntimeMetricNames::new(name_metadata("a", RuntimeMetricKind::Multi))
            .with_name("b-c").unwrap();
        assert_eq!((first.reported().name(), second.reported().name()), ("a-b-c", "a-b-c"));
        assert_ne!(first.reported().id(), second.reported().id());
        assert_eq!(first.iter().map(RuntimeMetricMetadata::name).collect::<Vec<_>>(), vec!["a-b", "c"]);
        assert_eq!(second.iter().map(RuntimeMetricMetadata::name).collect::<Vec<_>>(), vec!["a", "b-c"]);
        let duplicate = first.with_name("c").unwrap();
        assert!(Arc::ptr_eq(&first.0, &duplicate.0));
        assert!(std::ptr::eq(first.reported(), duplicate.reported()));
        let report = RuntimeMetricInstanceReport::new(&first, RuntimeMetricInstanceStatus::RecordInvoked,
            Some((3, Some(1))));
        assert_eq!((report.name(), report.names().collect::<Vec<_>>(), report.counts()),
            ("a-b", vec!["a-b", "c"], Some((3, Some(1)))));
    }

    /// MN-V2-T03：上报只读出口提供精确合并标签；旧快照、重复组和预热缓存保持稳定。
    /// 仅真实纯标签，不造SDK、采样来源或网络；共享进程串来自本局部唯一输入。
    #[cfg(feature = "trace")]
    #[test]
    fn test_combined_reported_labels_are_cached_without_changing_old_groups() {
        use opentelemetry::KeyValue;
        let process: Arc<str> = Arc::from("宿主标记-12345");
        let original = RuntimeMetricNames::new(name_metadata("rpc", RuntimeMetricKind::Single));
        let old = original.reported().labels(&process);
        let expanded = original.with_name("storage").unwrap().with_name("网关").unwrap();
        let reported = expanded.reported();
        let expected = [KeyValue::new("rt.id", original.primary().id().as_u64() as i64),
            KeyValue::new("rt.name", "rpc-storage-网关"), KeyValue::new("rt.process", "宿主标记-12345")];
        assert_eq!(reported.labels(&process), &expected);
        assert_eq!(reported.kind, RuntimeMetricKind::Single);
        assert!(!std::ptr::eq(expanded.primary(), reported));
        let cached = reported.labels(&process);
        let duplicate = expanded.with_name("storage").unwrap();
        assert!(std::ptr::eq(cached, duplicate.reported().labels(&process)));
        assert!(std::ptr::eq(old, original.reported().labels(&process)));
        assert_eq!(old[1], KeyValue::new("rt.name", "rpc"));
        assert_eq!(expanded.iter().map(RuntimeMetricMetadata::name).collect::<Vec<_>>(), vec!["rpc", "storage", "网关"]);
        let next = expanded.with_name("other").unwrap();
        assert_eq!(next.reported().name(), "rpc-storage-网关-other");
        assert_eq!(reported.labels(&process), &expected);
        assert_eq!(original.reported().name(), "rpc");
    }
}
