//! MET-REGISTRY：一级表的权威登记与不可变快照，不采样、不记录SDK。
//!
//! MN-D03：一核心一Source及多个名称；累计1024历史(核心ID,原始名称)对，注销不返还。
//! 控制写含O(C)快照复制及本核名称处理，快照借取O(1)，C≤1024；不是无锁／硬实时。
//! 实际来源／用户资源Drop均在锁外；旧快照不受注销撤销，不增加业务热路径动作。
//! 合同／三图：docs/RUNTIME_METRICS_MULTI_NAME_REGISTRY_DESIGN.md；验证：本模块RG-T01～06。

use std::sync::{Arc, OnceLock};
use parking_lot::Mutex;
use super::{identity::RuntimeMetricId, model::{RuntimeMetricMetadata, RuntimeMetricNames,
    RuntimeMetricKind, RuntimeMetricRegistrationError, SERIES_CAPACITY}, source::RuntimeMetricsSource};

/// 不可变条目，共享只读名称集合；None保留历史纯元数据及系列预算，但不持实际runtime。
#[derive(Clone)]
pub(super) struct Entry {
    pub(super) metadata: RuntimeMetricNames,
    pub(super) source: Option<Arc<dyn RuntimeMetricsSource>>,
}

/// 唯一权威快照，按ID排序。原始锁只在本模块可见，不能跨业务操作。
pub(super) struct Registry(Mutex<Arc<[Entry]>>);

impl Registry {
    /// 创建独立空表，O(1)冷分配／控制初始化；不访问全局状态、runtime或SDK。
    /// 生产只由唯一OnceLock使用，单元独立表用于隔离真实登记状态而非替代采样。
    pub(super) fn new() -> Self { Self(Mutex::new(Arc::from(Vec::<Entry>::new()))) }

    /// O(1)只克隆一次快照Arc，不克隆各runtime，不在锁内采样或调用用户。
    pub(super) fn snapshot(&self) -> Arc<[Entry]> { self.0.lock().clone() }

    /// MN-R01：显式登记真实核心及已通过原校验的完整名称，首次Kind固定。
    /// 同名活动幂等；新名追加并验证合并256字节，再检查1024历史名称预算，故
    /// NameTooLong优先于CapacityExceeded。失败不发布、不追加或恢复退休Source。
    /// 活动Source不替换；退休仅在整次成功后恢复一个Source及全部历史名称。
    /// 非纯、线程安全但可能短锁／冷分配；不采样、不投递、不等待或调用SDK／用户。
    /// O(C+A*n+L+n)时间和O(C+A+L+n)新空间；A为本核名称数，L+n+1≤256。
    /// 参数source及新候选保留在锁外变量，失败／幂等／复制展开也不在锁内最终
    /// 析构实际来源；旧快照保持自己的纯名称及Source，发布不是排空或取消。
    /// 保护入口：本模块test_multi_name_*及公开runtime_metrics登记／支持场景。
    pub(super) fn register(&self, id: RuntimeMetricId, name: &str, kind: RuntimeMetricKind,
        source: Arc<dyn RuntimeMetricsSource>) -> Result<RuntimeMetricId, RuntimeMetricRegistrationError>
    {
        let candidate = Entry {
            metadata: RuntimeMetricNames::new(Arc::new(RuntimeMetricMetadata::new(id, name, kind))),
            source: Some(source),
        };
        let old = {
            let mut snapshot = self.0.lock();
            let position = snapshot.binary_search_by_key(&id, |entry| entry.metadata.primary().id());
            let (metadata, adds_name) = match position {
                Ok(index) => {
                    let entry = &snapshot[index];
                    let metadata = entry.metadata.with_name(name)?;
                    let adds_name = metadata.len() != entry.metadata.len();
                    if !adds_name && entry.source.is_some() { return Ok(id); }
                    (metadata, adds_name)
                }
                Err(_) => (candidate.metadata.clone(), true),
            };
            // 唯一权威快照中的历史名称就是预算；只在冷写路径求和，不维护镜像计数。
            if adds_name && snapshot.iter().map(|entry| entry.metadata.len()).sum::<usize>() >= SERIES_CAPACITY {
                return Err(RuntimeMetricRegistrationError::CapacityExceeded);
            }
            let mut next = snapshot.to_vec();
            match position {
                Ok(index) => {
                    next[index].metadata = metadata;
                    // 活动核心保留原Source；退休时才恢复，且与名称一起一次发布。
                    if next[index].source.is_none() { next[index].source = candidate.source.clone(); }
                }
                Err(index) => {
                    next.insert(index, Entry { metadata, source: candidate.source.clone() });
                }
            }
            std::mem::replace(&mut *snapshot, Arc::from(next))
        };
        drop(old);
        Ok(id)
    }

    /// 只撤销当前强登记，重复false；旧快照可继续采样和record，历史预算不归还。
    pub(super) fn unregister(&self, id: RuntimeMetricId) -> bool {
        let old = {
            let mut snapshot = self.0.lock();
            let index = match snapshot.binary_search_by_key(&id, |entry| entry.metadata.primary().id()) {
                Ok(index) if snapshot[index].source.is_some() => index,
                _ => return false,
            };
            let mut next = snapshot.to_vec();
            next[index].source = None;
            std::mem::replace(&mut *snapshot, Arc::from(next))
        };
        // old可能是Source的最后引用，绝不能在表锁内释放。
        drop(old);
        true
    }
}

static REGISTRY: OnceLock<Registry> = OnceLock::new();

/// 初始化只有空表纯控制数据，无SDK、runtime或用户回调。
pub(super) fn global_registry() -> &'static Registry { REGISTRY.get_or_init(Registry::new) }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rt::{AsyncRuntime, single_thread::SingleTaskRunner, serial_local_thread::LocalTaskRunner};
    use super::super::{source::{LocalRuntimeSource, RuntimeSource}, model::{RuntimeMetricInstanceReport, RuntimeMetricInstanceStatus}};

    /// 每项均构造真实本地runtime／Source，不用假来源替换注册或生命周期能力。
    fn local_source() -> (RuntimeMetricId, Arc<dyn RuntimeMetricsSource>) {
        let runtime = LocalTaskRunner::<()>::new().into_local();
        let identity = runtime.runtime_metric_slot().get_or_allocate().unwrap();
        (identity.id(), Arc::new(LocalRuntimeSource::new(runtime, identity)))
    }

    /// MET-REGISTRY：同名幂等、异名追加、真实来源强保活、旧快照、退休恢复与首次类别。
    /// Weak观测真实Source及原pool所有权，不通过临时清理或测试替代品完成注销。
    #[test]
    fn test_snapshot_retirement_and_shared_source_lifetime() {
        let registry = Registry::new();
        let runner = SingleTaskRunner::<()>::default();
        let runtime = runner.startup().unwrap();
        let pool = Arc::downgrade(&runtime.shared_pool());
        let identity = runtime.runtime_metric_slot().get_or_allocate().unwrap();
        let id = identity.id();
        let source: Arc<dyn RuntimeMetricsSource> = Arc::new(RuntimeSource::<_, ()>::new(runtime));
        let weak_source = Arc::downgrade(&source);
        registry.register(id, "shared", RuntimeMetricKind::Single, source.clone()).unwrap();
        assert_eq!(registry.register(id, "shared", RuntimeMetricKind::Worker, source.clone()), Ok(id));
        assert_eq!(registry.register(id, "different", RuntimeMetricKind::Single, source.clone()),
            Ok(id));
        let old = registry.snapshot();
        drop(runner);
        drop(source);
        assert!(pool.upgrade().is_some());
        assert!(registry.unregister(id));
        assert!(!registry.unregister(id));
        assert!(registry.snapshot()[0].source.is_none());
        assert!(old[0].source.is_some());
        assert!(weak_source.upgrade().is_some());
        registry.register(id, "shared", RuntimeMetricKind::Worker, old[0].source.as_ref().unwrap().clone()).unwrap();
        let current = registry.snapshot();
        assert!(std::ptr::eq(old[0].metadata.primary(), current[0].metadata.primary()));
        let item = RuntimeMetricInstanceReport::new(&current[0].metadata, RuntimeMetricInstanceStatus::Unregistered, None);
        assert_eq!(item.kind(), RuntimeMetricKind::Single);
        assert_eq!(item.names().collect::<Vec<_>>(), ["shared", "different"]);
        assert!(registry.unregister(id));
        drop(current);
        drop(old);
        assert!(weak_source.upgrade().is_none());
        assert!(pool.upgrade().is_none());
    }

    /// MET-REGISTRY：真实1024项边界，不启动执行线程；注销不返还累计系列预算。
    /// 乱序提交仍按ID排列，同名不同核心不合并，满表可恢复原核心但拒绝新核心。
    #[test]
    fn test_capacity_sorted_identity_and_retired_budget() {
        let registry = Registry::new();
        let mut inputs: Vec<_> = (0..SERIES_CAPACITY).map(|_| local_source()).collect();
        inputs.reverse();
        for (id, source) in &inputs {
            registry.register(*id, "same-name", RuntimeMetricKind::Local, source.clone()).unwrap();
        }
        let snapshot = registry.snapshot();
        assert_eq!(snapshot.len(), SERIES_CAPACITY);
        assert!(snapshot.windows(2).all(|pair| pair[0].metadata.primary().id() < pair[1].metadata.primary().id()));
        for (id, _) in &inputs { assert!(registry.unregister(*id)); }
        assert!(registry.snapshot().iter().all(|entry| entry.source.is_none()));
        let (extra_id, extra_source) = local_source();
        assert!(!registry.unregister(extra_id));
        assert_eq!(registry.register(extra_id, "same-name", RuntimeMetricKind::Local, extra_source),
            Err(RuntimeMetricRegistrationError::CapacityExceeded));
        let (id, source) = &inputs[0];
        assert_eq!(registry.register(*id, "same-name", RuntimeMetricKind::Local, source.clone()), Ok(*id));
        assert_eq!(registry.snapshot().len(), SERIES_CAPACITY);
        assert_eq!(registry.snapshot().iter().filter(|entry| entry.source.is_some()).count(), 1);
    }

    /// RG-T01／02／03：真实Single核心新增别名不替换活动Source或首次Kind。
    /// 观测旧快照、非首名重复、整体退休／恢复／追加及真实来源和pool的最终释放；不采样。
    #[test]
    fn test_multi_name_snapshot_source_and_whole_core_lifetime() {
        let registry = Registry::new();
        assert!(registry.snapshot().is_empty());
        let runner = SingleTaskRunner::<()>::default();
        let runtime = runner.startup().unwrap();
        let pool = Arc::downgrade(&runtime.shared_pool());
        let id = runtime.runtime_metric_slot().get_or_allocate().unwrap().id();
        let source: Arc<dyn RuntimeMetricsSource> = Arc::new(RuntimeSource::<_, ()>::new(runtime.clone()));
        let weak = Arc::downgrade(&source);
        registry.register(id, "rpc", RuntimeMetricKind::Single, source.clone()).unwrap();
        let original = registry.snapshot();
        let candidate: Arc<dyn RuntimeMetricsSource> = Arc::new(RuntimeSource::<_, ()>::new(runtime.clone()));
        let unused = Arc::downgrade(&candidate);
        assert_eq!(registry.register(id, "storage", RuntimeMetricKind::Worker, candidate), Ok(id));
        assert!(unused.upgrade().is_none(), "别名候选不能替换原活动Source或额外保活");
        let expanded = registry.snapshot();
        assert_eq!(expanded.len(), 1);
        assert!(Arc::ptr_eq(expanded[0].source.as_ref().unwrap(), &source));
        assert_eq!(original[0].metadata.iter().map(RuntimeMetricMetadata::name).collect::<Vec<_>>(), ["rpc"]);
        assert_eq!(expanded[0].metadata.iter().map(RuntimeMetricMetadata::name).collect::<Vec<_>>(), ["rpc", "storage"]);
        assert_eq!(expanded[0].metadata.reported().name(), "rpc-storage");
        assert!(std::ptr::eq(original[0].metadata.primary(), expanded[0].metadata.primary()));
        assert_eq!(registry.register(id, "storage", RuntimeMetricKind::Worker, source.clone()), Ok(id));
        assert!(Arc::ptr_eq(&expanded, &registry.snapshot()), "活动同名重复不发布新快照");
        let report = RuntimeMetricInstanceReport::new(&expanded[0].metadata,
            RuntimeMetricInstanceStatus::RecordInvoked, Some((7, Some(2))));
        assert_eq!((report.id(), report.name(), report.kind(), report.counts()),
            (id, "rpc", RuntimeMetricKind::Single, Some((7, Some(2)))));
        assert!(registry.unregister(id));
        assert!(!registry.unregister(id));
        assert!(registry.snapshot()[0].source.is_none());
        assert!(original[0].source.is_some());
        assert_eq!(registry.register(id, "storage", RuntimeMetricKind::Worker, source.clone()), Ok(id));
        assert_eq!(registry.snapshot()[0].metadata.reported().name(), "rpc-storage");
        assert!(registry.unregister(id));
        assert_eq!(registry.register(id, "业务", RuntimeMetricKind::Worker, source.clone()), Ok(id));
        let current = registry.snapshot();
        assert_eq!(current[0].metadata.reported().name(), "rpc-storage-业务");
        assert_eq!(current[0].metadata.iter().map(RuntimeMetricMetadata::name).collect::<Vec<_>>(),
            ["rpc", "storage", "业务"]);
        assert_eq!(report.names().collect::<Vec<_>>(), ["rpc", "storage"]);
        assert!(registry.unregister(id));
        drop(runtime);
        drop(runner);
        drop(source);
        assert!(weak.upgrade().is_some());
        drop(original);
        drop(expanded);
        assert!(weak.upgrade().is_some());
        drop(current);
        assert!(weak.upgrade().is_none());
        assert!(pool.upgrade().is_none(), "纯报告和退休名称不能保活真实pool");
        assert_eq!(report.names().collect::<Vec<_>>(), ["rpc", "storage"]);
    }

    /// RG-T04：以真实Local来源保护255／256合并成功、257／258失败及Unicode字节边界。
    /// 同名满长仍幂等；失败不发布快照、不恢复退休Source；未推动任何本地队列。
    #[test]
    fn test_multi_name_combined_length_and_retired_failure_are_atomic() {
        for name in ["x".repeat(253), "x".repeat(254), "x".repeat(255), "x".repeat(256),
            format!("{}a", "界".repeat(84)), "界".repeat(85)]
        {
            let registry = Registry::new();
            let (id, source) = local_source();
            registry.register(id, &name, RuntimeMetricKind::Local, source.clone()).unwrap();
            let before = registry.snapshot();
            assert_eq!(registry.register(id, &name, RuntimeMetricKind::Local, source.clone()), Ok(id));
            assert!(Arc::ptr_eq(&before, &registry.snapshot()));
            let result = registry.register(id, "y", RuntimeMetricKind::Local, source.clone());
            if name.len() + 2 <= 256 {
                assert_eq!(result, Ok(id));
                assert_eq!(registry.snapshot()[0].metadata.reported().name(), format!("{}-y", name));
            } else {
                assert_eq!(result, Err(RuntimeMetricRegistrationError::NameTooLong));
                assert!(Arc::ptr_eq(&before, &registry.snapshot()));
                assert!(registry.unregister(id));
                let retired = registry.snapshot();
                assert_eq!(registry.register(id, "y", RuntimeMetricKind::Local, source.clone()),
                    Err(RuntimeMetricRegistrationError::NameTooLong));
                assert!(Arc::ptr_eq(&retired, &registry.snapshot()));
                assert!(registry.snapshot()[0].source.is_none());
                assert_eq!(registry.register(id, &name, RuntimeMetricKind::Local, source.clone()), Ok(id));
            }
            assert_eq!(before[0].metadata.reported().name(), name);
            assert_eq!(before[0].metadata.len(), 1);
        }
    }

    /// RG-T04：512个真实核心各两名精确耗尽1024历史名称预算，而非只限制核心数。
    /// 满额重复／旧别名恢复免费；新名／新核失败不发布，合并超长优先于预算耗尽。
    #[test]
    fn test_multi_name_history_budget_and_error_priority() {
        let registry = Registry::new();
        let inputs: Vec<_> = (0..SERIES_CAPACITY / 2).map(|_| local_source()).collect();
        for (index, (id, source)) in inputs.iter().enumerate() {
            registry.register(*id, "a", RuntimeMetricKind::Local, source.clone()).unwrap();
            if index == 0 || index == SERIES_CAPACITY / 2 - 1 {
                assert_eq!(registry.snapshot().iter().map(|entry| entry.metadata.len()).sum::<usize>(), index * 2 + 1);
            }
            registry.register(*id, "b", RuntimeMetricKind::Local, source.clone()).unwrap();
        }
        let full = registry.snapshot();
        assert_eq!(full.len(), SERIES_CAPACITY / 2);
        assert_eq!(full.iter().map(|entry| entry.metadata.len()).sum::<usize>(), SERIES_CAPACITY);
        assert!(full.windows(2).all(|pair| pair[0].metadata.primary().id() < pair[1].metadata.primary().id()));
        for entry in full.iter() {
            assert_eq!(entry.metadata.iter().map(RuntimeMetricMetadata::name).collect::<Vec<_>>(), ["a", "b"]);
            assert_eq!(entry.metadata.reported().name(), "a-b");
        }
        let (id, source) = &inputs[0];
        assert_eq!(registry.register(*id, "b", RuntimeMetricKind::Local, source.clone()), Ok(*id));
        assert!(Arc::ptr_eq(&full, &registry.snapshot()));
        for retired in [false, true] {
            if retired { assert!(registry.unregister(*id)); }
            let before = registry.snapshot();
            assert_eq!(registry.register(*id, "c", RuntimeMetricKind::Local, source.clone()),
                Err(RuntimeMetricRegistrationError::CapacityExceeded));
            assert!(Arc::ptr_eq(&before, &registry.snapshot()));
            assert_eq!(registry.register(*id, &"x".repeat(255), RuntimeMetricKind::Local, source.clone()),
                Err(RuntimeMetricRegistrationError::NameTooLong));
            assert!(Arc::ptr_eq(&before, &registry.snapshot()));
            assert_eq!(registry.snapshot()[0].source.is_none(), retired);
        }
        let (extra_id, extra_source) = local_source();
        let extra_weak = Arc::downgrade(&extra_source);
        let before = registry.snapshot();
        assert_eq!(registry.register(extra_id, "a", RuntimeMetricKind::Local, extra_source),
            Err(RuntimeMetricRegistrationError::CapacityExceeded));
        assert!(extra_weak.upgrade().is_none());
        assert!(Arc::ptr_eq(&before, &registry.snapshot()));
        assert!(!registry.unregister(extra_id));
        assert_eq!(registry.register(*id, "b", RuntimeMetricKind::Local, source.clone()), Ok(*id));
        let restored = registry.snapshot();
        assert_eq!(restored[0].metadata.reported().name(), "a-b");
        assert!(Arc::ptr_eq(restored[0].source.as_ref().unwrap(), source));
        assert_eq!(restored.iter().map(|entry| entry.metadata.len()).sum::<usize>(), SERIES_CAPACITY);
    }

    /// RG-T05：四线程对同一真实Single核心并发提交两次alpha及beta／gamma。
    /// 容量1启动信号和3秒截止仅协调测试，不替代注册；首名固定，竞争别名不假定固定顺序。
    #[test]
    fn test_multi_name_concurrent_registration_deduplicates_one_core() {
        use std::{thread, time::{Duration, Instant}};
        let registry = Arc::new(Registry::new());
        let runner = SingleTaskRunner::<()>::default();
        let runtime = runner.startup().unwrap();
        let id = runtime.runtime_metric_slot().get_or_allocate().unwrap().id();
        let source: Arc<dyn RuntimeMetricsSource> = Arc::new(RuntimeSource::<_, ()>::new(runtime.clone()));
        registry.register(id, "root", RuntimeMetricKind::Single, source.clone()).unwrap();
        let (done, results) = crossbeam_channel::bounded(4);
        let mut starts = Vec::new();
        let mut handles = Vec::new();
        let mut weak = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(3);
        for name in ["alpha", "beta", "alpha", "gamma"] {
            let (start, go) = crossbeam_channel::bounded(1);
            let registry = registry.clone();
            let done = done.clone();
            let candidate: Arc<dyn RuntimeMetricsSource> = Arc::new(RuntimeSource::<_, ()>::new(runtime.clone()));
            weak.push(Arc::downgrade(&candidate));
            starts.push(start);
            handles.push(thread::spawn(move || {
                go.recv_deadline(deadline).unwrap();
                let result = registry.register(id, name, RuntimeMetricKind::Worker, candidate);
                done.try_send(result).unwrap();
            }));
        }
        for start in starts { start.try_send(()).unwrap(); }
        for _ in 0..4 { assert_eq!(results.recv_deadline(deadline).unwrap(), Ok(id)); }
        for handle in handles { handle.join().unwrap(); }
        assert!(Instant::now() < deadline);
        assert!(weak.iter().all(|source| source.upgrade().is_none()));
        let snapshot = registry.snapshot();
        assert_eq!(snapshot.len(), 1);
        let entry = &snapshot[0];
        assert_eq!((entry.metadata.primary().id(), entry.metadata.primary().name()), (id, "root"));
        assert!(Arc::ptr_eq(entry.source.as_ref().unwrap(), &source));
        let mut names: Vec<_> = entry.metadata.iter().map(RuntimeMetricMetadata::name).collect();
        assert_eq!(names.len(), 4);
        let combined = names.join("-");
        assert_eq!(entry.metadata.reported().name(), combined);
        names.sort_unstable();
        assert_eq!(names, ["alpha", "beta", "gamma", "root"]);
        let report = RuntimeMetricInstanceReport::new(&entry.metadata, RuntimeMetricInstanceStatus::Unregistered, None);
        assert_eq!(report.kind(), RuntimeMetricKind::Single);
    }
}
