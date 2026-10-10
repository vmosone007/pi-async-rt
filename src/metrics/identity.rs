//! MET-ID：真实核心的冷身份与单在途许可；不依赖运行时或指标上层。
//!
//! 槽不自动登记、不保存名称或runtime，业务poll／wake从不访问它。
//! 合同：docs/RUNTIME_METRICS_IMPLEMENTATION_DESIGN.md；验证：本模块单元专项。

#[cfg(not(target_arch = "wasm32"))]
use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}};
#[cfg(not(target_arch = "wasm32"))]
use parking_lot::Mutex;

/// 登记域内不复用的实例标识；不是旧runtime编号，也不能还原核心。
///
/// 只经显式登记得到，范围1..=i64::MAX，失败登记可以留下编号空洞。
/// Copy／比较／散列／读取均为O(1)纯值操作，无阻塞、分配、线程或运行时要求。
/// 不保证内存布局／ABI；多个链接版本或动态副本不共享编号域，远端还须结合进程标识。
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RuntimeMetricId(u64);

#[cfg(not(target_arch = "wasm32"))]
impl RuntimeMetricId {
    /// 原样返回正整数编号；幂等、纯函数、O(1)，不查表或保活任何runtime。
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

#[cfg(not(target_arch = "wasm32"))]
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// 仅修改传入的身份计数器；标准CAS重试负责唯一性，耗尽保持原值、不回绕。
/// 独立计数器单元测试能验证真实分配算法的上界，不重置生产全局状态。
#[cfg(not(target_arch = "wasm32"))]
fn allocate_id(counter: &AtomicU64) -> Option<RuntimeMetricId> {
    counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed,
        |next| if next != 0 && next <= i64::MAX as u64 { Some(next + 1) } else { None })
        .ok().map(RuntimeMetricId)
}

/// 惰性冷槽：只在指标注册时访问，Clone／Worker包装共享真实核心中的同一份槽。
///
/// new为O(1)无堆分配初始化；get_or_allocate有短锁、首次Arc分配及原子ID分配，
/// 不调用用户代码、不跨其它锁，不保证争用等待时长。槽不持runtime，不形成引用环。
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct RuntimeMetricIdentitySlot(Mutex<Option<Arc<RuntimeMetricIdentity>>>);

#[cfg(not(target_arch = "wasm32"))]
impl RuntimeMetricIdentitySlot {
    /// O(1)纯初始化空槽，不分配身份、不访问全局表／计数器，不登记或启动线程。
    pub(crate) const fn new() -> Self {
        Self(Mutex::new(None))
    }

    /// 返回共享身份；编号耗尽返回None，不回绕。副作用仅为首次冷初始化。
    /// 标准fetch_update负责争用重试，不用单次weak CAS决定正确性。
    pub(crate) fn get_or_allocate(&self) -> Option<Arc<RuntimeMetricIdentity>> {
        let mut slot = self.0.lock();
        if slot.is_none() {
            let id = allocate_id(&NEXT_ID)?;
            *slot = Some(Arc::new(RuntimeMetricIdentity {
                id,
                in_flight: AtomicBool::new(false),
            }));
        }
        slot.as_ref().cloned()
    }
}

/// wasm只保留零大小核心占位，不提供身份分配、登记或采集能力。
#[cfg(target_arch = "wasm32")]
pub(crate) struct RuntimeMetricIdentitySlot;

#[cfg(target_arch = "wasm32")]
impl RuntimeMetricIdentitySlot {
    /// wasm零大小纯初始化，不提供注册或身份能力。
    pub(crate) const fn new() -> Self { Self }
}

/// 请求状态唯一归属；只含ID和许可原子，不持runtime／Source／回调。
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct RuntimeMetricIdentity {
    id: RuntimeMetricId,
    in_flight: AtomicBool,
}

#[cfg(not(target_arch = "wasm32"))]
impl RuntimeMetricIdentity {
    /// O(1)纯只读，返回身份值而非控制状态。
    pub(crate) fn id(&self) -> RuntimeMetricId { self.id }

    /// 强CAS认领，失败立即None，不自旋／等待／排队；成功仅Clone一次纯身份Arc。
    /// acquire与上次许可release配对；许可必须随真实任务结束，不能随接收超时释放。
    pub(crate) fn try_claim(self: &Arc<Self>) -> Option<RuntimeMetricRequestPermit> {
        self.in_flight.compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .ok().map(|_| RuntimeMetricRequestPermit(self.clone()))
    }
}

/// 不可复制的单次许可；正常／展开Drop均以O(1) release释放，不调用任何上层。
///
/// 仅由真实读取任务持有，不能通过注销、重登记或接收超时提前清除状态。
/// 自动Send／Sync，只持纯身份，无用户资源或线程受限析构，无锁／阻塞／异步等待。
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct RuntimeMetricRequestPermit(Arc<RuntimeMetricIdentity>);

#[cfg(not(target_arch = "wasm32"))]
impl Drop for RuntimeMetricRequestPermit {
    fn drop(&mut self) {
        self.0.in_flight.store(false, Ordering::Release);
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use std::{collections::BTreeSet, sync::Barrier, thread};

    /// MET-ID：真实分配器的正i64末端、耗尽保持、不复用，以及同槽并发唯一性。
    /// 独立Atomic只隔离算法输入，不更改生产NEXT_ID或另写分配算法。
    #[test]
    fn test_identity_allocation_boundaries_and_shared_slot() {
        let counter = AtomicU64::new(i64::MAX as u64 - 1);
        assert_eq!(allocate_id(&counter).unwrap().as_u64(), i64::MAX as u64 - 1);
        assert_eq!(allocate_id(&counter).unwrap().as_u64(), i64::MAX as u64);
        for _ in 0..3 { assert!(allocate_id(&counter).is_none()); }
        assert_eq!(counter.load(Ordering::Relaxed), i64::MAX as u64 + 1);
        for invalid in [0, u64::MAX] {
            let counter = AtomicU64::new(invalid);
            assert!(allocate_id(&counter).is_none());
            assert_eq!(counter.load(Ordering::Relaxed), invalid);
        }
        let slot = Arc::new(RuntimeMetricIdentitySlot::new());
        let threads: Vec<_> = (0..8).map(|_| {
            let slot = slot.clone();
            thread::spawn(move || slot.get_or_allocate().unwrap().id())
        }).collect();
        let ids: BTreeSet<_> = threads.into_iter().map(|thread| thread.join().unwrap()).collect();
        assert_eq!(ids.len(), 1);
        let first = slot.get_or_allocate().unwrap();
        assert!(Arc::ptr_eq(&first, &slot.get_or_allocate().unwrap()));
        let second = RuntimeMetricIdentitySlot::new().get_or_allocate().unwrap();
        assert_ne!(first.id(), second.id());
    }

    /// MET-ID：8个并发认领者共享真实许可，保持成功许可至所有尝试结束，严格只能一个。
    /// 正常／panic展开后都可再次认领；不以瞬时完成导致的多次先后成功冒充竞态。
    #[test]
    fn test_request_permit_exactly_one_and_unwind_release() {
        let identity = RuntimeMetricIdentitySlot::new().get_or_allocate().unwrap();
        let start = Arc::new(Barrier::new(8));
        let attempted = Arc::new(Barrier::new(8));
        let threads: Vec<_> = (0..8).map(|_| {
            let (identity, start, attempted) = (identity.clone(), start.clone(), attempted.clone());
            thread::spawn(move || {
                start.wait();
                let permit = identity.try_claim();
                attempted.wait();
                permit.is_some()
            })
        }).collect();
        assert_eq!(threads.into_iter().map(|thread| usize::from(thread.join().unwrap())).sum::<usize>(), 1);
        let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _permit = identity.try_claim().unwrap();
            assert!(identity.try_claim().is_none());
            panic!("仅验证许可展开释放");
        }));
        assert!(failed.is_err());
        assert!(identity.try_claim().is_some());
    }
}
