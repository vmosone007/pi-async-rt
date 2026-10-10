//! 显式登记和由外部周期推动的同步运行时指标入口，边界MET-ENTRY。
//!
//! # 使用与线程边界
//!
//! 构建／Clone不会登记；名称必传，成功登记强保活，须手动注销。原close仍可调用，
//! 但不自动注销／排空。普通Single／Multi／Worker直接读原len／wait_len；原生Local
//! 经原send在原执行线程读len，没有wait_len接口，不补Pending数量或伪造零。
//!
//! 采集只能从宿主已有、允许等待且不负责推进runtime／依赖工作的外部线程调用。
//! 主线程可以，但若主线程兼任本地Runner则禁止；worker／Future内禁止同步调用，
//! 不得持采样、关闭或SDK所需锁。库不自动识别所有线程职责，不新建线程／runtime／
//! timer，不初始化全局时钟。bounded(1)回复只占外部接收线程，原执行线程只回填
//! 一次，不等待取值；recv_deadline有保底，但不能硬抢占getter／SDK／Default／Drop。
//!
//! # 接入示例
//!
//! ```
//! use pi_async_rt::{rt::single_thread::SingleTaskRunner, runtime_metrics::*};
//! let runner = SingleTaskRunner::<()>::default();
//! let runtime = runner.startup().unwrap();
//! let id = register_runtime_metrics(&runtime, "gateway-io").unwrap();
//! assert_eq!(register_runtime_metrics(&runtime.clone(), "gateway-io").unwrap(), id);
//! assert!(unregister_runtime_metrics(id));
//! assert!(!unregister_runtime_metrics(id));
//! ```
//!
//! 宿主先安装有效MeterProvider，再init_runtime_metrics_reporting传入跨进程／重启
//! 唯一标识；已有外部线程例如每1～3秒调用collect_and_report_runtime_metrics。
//! 同入口同步record复用宿主OTLP，不每轮flush、不等待网络；RecordInvoked不是
//! 远端回执，SDK／远端必须保留rt.id／rt.name／rt.process并提供≥1024系列容量。
//!
//! # 关闭、失败和资源
//!
//! 全局仅一轮活动采集，并发／重入立即失败。每本地核心仅一份存活读取任务，超时
//! 只结束接收，不清许可或取消已send任务；迟到回填正常失败。停推核心不会逐轮
//! 积压新请求，但任务／核心可能持续存活。先停新轮及重登记、注销、等当前采集
//! 返回并继续推动采样实际结束，最后按原API关闭。注销不能撤销旧快照后续投递／
//! record，len==0不证明所有在途工作结束。软推进约定不放宽Send／Sync、UnsafeCell
//! 或V8析构域；本模块不新增unsafe，不暴露核心可写状态或可恢复裸指针。
//!
//! 表更新O(C)，每轮O(C)加原getter／调度／SDK成本，报告O(C)，C≤累计1024核心；
//! 本库控制锁短小且不跨用户调用／提交／接收／record／实际资源Drop。Crossbeam
//! 通知和SDK含内部同步，不声称严格无锁、零成本或硬实时；业务热路径不增加统计。
//! wasm32暂不提供本模块，serial Single／Worker只明确拒绝新登记，旧业务API不变。
//! 正式合同：docs/RUNTIME_METRICS_IMPLEMENTATION_DESIGN.md；专项：tests/runtime_metrics.rs。

use std::sync::Arc;
#[cfg(feature = "trace")]
use std::{sync::atomic::{AtomicBool, Ordering}, time::Instant};
use crate::rt::{AsyncTaskPool, AsyncTaskPoolExt, single_thread::SingleTaskRuntime,
    multi_thread::MultiTaskRuntime, worker_thread::WorkerRuntime,
    serial_local_thread::LocalTaskRuntime};
use super::{registry, reporter, source::{RuntimeSource, LocalRuntimeSource}, model::{validate_label, LabelValidationError}};
#[cfg(feature = "trace")]
use super::sample_error::RuntimeMetricSampleError;

pub use super::identity::RuntimeMetricId;
pub use super::model::{RuntimeMetricKind, RuntimeMetricRegistrationError,
    RuntimeMetricReportingInitError, RuntimeMetricCollectionOptions, RuntimeMetricCollectionError,
    RuntimeMetricInstanceStatus, RuntimeMetricInstanceReport, RuntimeMetricCollectionReport};

mod private { pub trait Sealed {} }

/// 本库具体类型的封闭登记能力，只提供指标接入，不改变或约束原AsyncRuntime。
///
/// 外部不能实现，不返回私有Source或调度句柄。name约定、强保活、错误与O(C+n)
/// 控制成本继承register_runtime_metrics；接入不读取getter、poll用户或启动线程。
pub trait RuntimeMetricRegistrable: private::Sealed {
    /// 显式命名登记；同核心同名幂等、不同名失败；原运行时构造不会自动调用。
    fn register_metrics(&self, name: &str)
        -> Result<RuntimeMetricId, RuntimeMetricRegistrationError>;
}

fn check_name(name: &str) -> Result<(), RuntimeMetricRegistrationError> {
    validate_label(name).map_err(|error| match error {
        LabelValidationError::TooLong => RuntimeMetricRegistrationError::NameTooLong,
        LabelValidationError::Invalid => RuntimeMetricRegistrationError::InvalidName,
    })
}

macro_rules! direct_registration {
    ($runtime:ident, $kind:ident) => {
        impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>>
            private::Sealed for $runtime<O, P> {}
        impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>>
            RuntimeMetricRegistrable for $runtime<O, P>
        {
            fn register_metrics(&self, name: &str) -> Result<RuntimeMetricId, RuntimeMetricRegistrationError> {
                check_name(name)?;
                let identity = self.runtime_metric_slot().get_or_allocate()
                    .ok_or(RuntimeMetricRegistrationError::IdentityExhausted)?;
                registry::global_registry().register(identity.id(), name, RuntimeMetricKind::$kind,
                    Arc::new(RuntimeSource::<_, O>::new(self.clone())))
            }
        }
    };
}

direct_registration!(SingleTaskRuntime, Single);
direct_registration!(MultiTaskRuntime, Multi);

impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>>
    private::Sealed for WorkerRuntime<O, P> {}
impl<O: Default + 'static, P: AsyncTaskPoolExt<O> + AsyncTaskPool<O, Pool = P>>
    RuntimeMetricRegistrable for WorkerRuntime<O, P>
{
    fn register_metrics(&self, name: &str) -> Result<RuntimeMetricId, RuntimeMetricRegistrationError> {
        check_name(name)?;
        let identity = self.get_worker_runtime().runtime_metric_slot().get_or_allocate()
            .ok_or(RuntimeMetricRegistrationError::IdentityExhausted)?;
        registry::global_registry().register(identity.id(), name, RuntimeMetricKind::Worker,
            Arc::new(RuntimeSource::<_, O>::new(self.clone())))
    }
}

impl<O: Default + 'static> private::Sealed for LocalTaskRuntime<O> {}
impl<O: Default + 'static> RuntimeMetricRegistrable for LocalTaskRuntime<O> {
    fn register_metrics(&self, name: &str) -> Result<RuntimeMetricId, RuntimeMetricRegistrationError> {
        check_name(name)?;
        let identity = self.runtime_metric_slot().get_or_allocate()
            .ok_or(RuntimeMetricRegistrationError::IdentityExhausted)?;
        registry::global_registry().register(identity.id(), name, RuntimeMetricKind::Local,
            Arc::new(LocalRuntimeSource::new(self.clone(), identity)))
    }
}

macro_rules! reject_serial_registration {
    ($module:ident, $runtime:ident) => {
        impl<O: Default + 'static, P: crate::rt::serial::AsyncTaskPoolExt<O>
            + crate::rt::serial::AsyncTaskPool<O, Pool = P>>
            private::Sealed for crate::rt::$module::$runtime<O, P> {}
        impl<O: Default + 'static, P: crate::rt::serial::AsyncTaskPoolExt<O>
            + crate::rt::serial::AsyncTaskPool<O, Pool = P>>
            RuntimeMetricRegistrable for crate::rt::$module::$runtime<O, P>
        {
            fn register_metrics(&self, _name: &str) -> Result<RuntimeMetricId, RuntimeMetricRegistrationError> {
                Err(RuntimeMetricRegistrationError::ThreadBoundRuntimeRequiresOwnerBridge)
            }
        }
    };
}

reject_serial_registration!(serial_single_thread, SingleTaskRuntime);
reject_serial_registration!(serial_worker_thread, WorkerRuntime);

/// 显式登记并强保活真实核心，返回新的指标身份；不自动开启trace／采样／线程。
///
/// 名称必传：1～256 UTF-8字节、非纯空白、无控制字符。不同核心可同名；同核心
/// 名称首次成功后固定，同名重复／注销后恢复复用身份与历史系列，异名失败。
/// 累计不同已登记核心最多1024，注销不返还系列预算。具体错误见返回类型。
///
/// O(C+n)、非纯；相同核心同名幂等，有冷分配／短锁／Arc成本，不访问原getter或
/// 用户任务。线程安全但原runtime合法生命周期与自定义池合同仍由调用方负责。
/// 注册强引用只阻止自动析构，不禁止旧close；注销不是取消／排空／关闭。
pub fn register_runtime_metrics<R: RuntimeMetricRegistrable + ?Sized>(runtime: &R, name: &str)
    -> Result<RuntimeMetricId, RuntimeMetricRegistrationError>
{
    runtime.register_metrics(name)
}

/// 撤销活动强登记；首次删除true，未知／重复false，不关闭runtime或取消旧快照。
///
/// O(C)非纯、幂等效果，可能短锁；实际来源及用户资源仅锁外析构，旧快照／任务
/// 可延迟释放。宿主须防并发重登记，按模块关闭顺序结束采样工作；不保证即刻静默。
pub fn unregister_runtime_metrics(id: RuntimeMetricId) -> bool {
    registry::global_registry().unregister(id)
}

/// 初始化共享三Gauge；宿主须先装有效Provider，process_instance须跨进程／重启唯一。
///
/// 标识采用与名称相同的256字节校验；成功后同参数幂等、不同参数AlreadyInitialized，
/// 并发／重入初始化立即InitializationInProgress。trace关闭ReportingDisabled优先。
/// 非纯，有一次SDK／分配成本，SDK可能同步阻塞／panic；无本库锁跨SDK、不新建导出
/// 器／线程／网络，初始化成功不证明Provider非no-op或远端可达，后续换Provider不重绑。
/// 本库O(n)时间／空间，n为进程标识字节数；SDK成本另计，不是业务热路径。
pub fn init_runtime_metrics_reporting(process_instance: &str) -> Result<(), RuntimeMetricReportingInitError> {
    reporter::initialize(process_instance)
}

#[cfg(feature = "trace")]
static COLLECTING: AtomicBool = AtomicBool::new(false);

#[cfg(feature = "trace")]
struct CollectionGuard;
#[cfg(feature = "trace")]
impl Drop for CollectionGuard {
    fn drop(&mut self) { COLLECTING.store(false, Ordering::Release); }
}

/// 同一同步调用内完成快照采样和SDK记录，返回逐实例只读报告，不是网络确认。
///
/// 只能由宿主已有、可等待且不推动runtime的外部线程调用；主线程兼Runner、worker／
/// Future内或持采样依赖锁均禁止。库不自动检测全部调用域，不创建线程或timer。
/// 本地任务经原send，接收bounded(1)只占调用线程，实例截止不超过整轮截止。
/// 无值关闭／超时／旧在途明确失败，整轮到期剩余不投递；失败不补数量零／旧值。
///
/// 并发／重入立即CollectionInProgress，参数／配置错误在投递前返回；trace关闭
/// ReportingDisabled优先。超时不取消任务／提前清许可，注销不撤销旧快照；迟到回复
/// 正常失败。同步入口无Future取消接口，已record不撤销，用户／SDK panic自然传播。
///
/// O(C)控制时间／报告空间，加原getter、调度及SDK成本；非纯、非幂等，含短锁、
/// 本地通道分配及有限接收等待。截止不能抢占getter／Default／Drop／SDK／OS调度，
/// 不保证硬实时、严格无锁或原子的跨实例快照。原业务热路径不新增统计。
/// 对照：MET-IMPLEMENTATION-001；专项：runtime_metrics的截止／重入／生命周期矩阵。
pub fn collect_and_report_runtime_metrics(options: RuntimeMetricCollectionOptions)
    -> Result<RuntimeMetricCollectionReport, RuntimeMetricCollectionError>
{
    #[cfg(not(feature = "trace"))]
    {
        let _ = options;
        Err(RuntimeMetricCollectionError::ReportingDisabled)
    }
    #[cfg(feature = "trace")]
    {
        let reporter = reporter::initialized()?;
        let now = Instant::now();
        // Options只有拒绝零值的公开构造器与非零Default，不重复校验不可达的零状态。
        let round_deadline = now.checked_add(options.round_timeout())
            .ok_or(RuntimeMetricCollectionError::DeadlineOverflow)?;
        now.checked_add(options.instance_timeout()).ok_or(RuntimeMetricCollectionError::DeadlineOverflow)?;
        COLLECTING.compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .map_err(|_| RuntimeMetricCollectionError::CollectionInProgress)?;
        let _guard = CollectionGuard;
        let snapshot = registry::global_registry().snapshot();
        let mut instances = Vec::with_capacity(snapshot.len());
        for entry in snapshot.iter() {
            let (counts, status) = if let Some(source) = &entry.source {
                let now = Instant::now();
                if now >= round_deadline {
                    (None, RuntimeMetricInstanceStatus::RoundDeadlineSkipped)
                } else {
                    let budget = options.instance_timeout().min(round_deadline.duration_since(now));
                    let deadline = now.checked_add(budget).ok_or(RuntimeMetricCollectionError::DeadlineOverflow)?;
                    match source.sample(deadline) {
                        Ok(counts) => (Some(counts), RuntimeMetricInstanceStatus::RecordInvoked),
                        Err(RuntimeMetricSampleError::RequestInFlight) => (None, RuntimeMetricInstanceStatus::RequestInFlight),
                        Err(RuntimeMetricSampleError::DeadlineExceeded) => (None, RuntimeMetricInstanceStatus::DeadlineExceeded),
                        Err(RuntimeMetricSampleError::ReplyClosed) => (None, RuntimeMetricInstanceStatus::ReplyClosed),
                    }
                }
            } else { (None, RuntimeMetricInstanceStatus::Unregistered) };
            let status = reporter.record(&entry.metadata, counts, status);
            instances.push(RuntimeMetricInstanceReport::new(&entry.metadata, status,
                counts.map(|counts| (counts.len(), counts.wait_len()))));
        }
        Ok(RuntimeMetricCollectionReport::new(instances))
    }
}
