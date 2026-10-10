//! MET-REGISTRY：一级表的权威登记与不可变快照，不采样、不记录SDK。
//!
//! 控制写操作O(C)，快照借取O(1)，C≤1024；短Mutex可能争用，不是无锁／硬实时。
//! 实际来源／用户资源Drop均在锁外；旧快照不受注销撤销。验证：本模块／runtime_metrics。

use std::sync::{Arc, OnceLock};
use parking_lot::Mutex;
use super::{identity::RuntimeMetricId, model::{RuntimeMetricMetadata,
    RuntimeMetricKind, RuntimeMetricRegistrationError, SERIES_CAPACITY}, source::RuntimeMetricsSource};

/// 不可变条目；None保留历史纯元数据及系列预算，但不持实际runtime。
#[derive(Clone)]
pub(super) struct Entry {
    pub(super) metadata: Arc<RuntimeMetricMetadata>,
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

    /// 显式注册。参数source已经锁外构建，失败／幂等也在锁外释放参数资源。
    /// 新候选保留在锁外变量，保证复制快照中途展开不会在锁内最后析构候选来源。
    pub(super) fn register(&self, id: RuntimeMetricId, name: &str, kind: RuntimeMetricKind,
        source: Arc<dyn RuntimeMetricsSource>) -> Result<RuntimeMetricId, RuntimeMetricRegistrationError>
    {
        let candidate = Entry {
            metadata: Arc::new(RuntimeMetricMetadata::new(id, name, kind)),
            source: Some(source),
        };
        let old = {
            let mut snapshot = self.0.lock();
            let position = snapshot.binary_search_by_key(&id, |entry| entry.metadata.id());
            if let Ok(index) = position {
                if snapshot[index].metadata.name() != name {
                    return Err(RuntimeMetricRegistrationError::NameConflict);
                }
                if snapshot[index].source.is_some() { return Ok(id); }
            } else if snapshot.len() == SERIES_CAPACITY {
                return Err(RuntimeMetricRegistrationError::CapacityExceeded);
            }
            let mut next = snapshot.to_vec();
            match position {
                Ok(index) => {
                    // 保留首次名称／类别／缓存，重登记只恢复来源，不另造系列。
                    next[index].source = candidate.source.clone();
                }
                Err(index) => {
                    next.insert(index, candidate.clone());
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
            let index = match snapshot.binary_search_by_key(&id, |entry| entry.metadata.id()) {
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

    /// MET-REGISTRY：同名幂等、异名冲突、真实来源强保活、旧快照、退休恢复与首次类别。
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
            Err(RuntimeMetricRegistrationError::NameConflict));
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
        assert!(Arc::ptr_eq(&old[0].metadata, &current[0].metadata));
        let item = RuntimeMetricInstanceReport::new(&current[0].metadata, RuntimeMetricInstanceStatus::Unregistered, None);
        assert_eq!(item.kind(), RuntimeMetricKind::Single);
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
        assert!(snapshot.windows(2).all(|pair| pair[0].metadata.id() < pair[1].metadata.id()));
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
}
