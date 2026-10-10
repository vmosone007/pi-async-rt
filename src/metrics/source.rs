//! MET-SOURCE：对象安全的私有只读来源，不继承AsyncRuntime或暴露泛型／核心。
//!
//! 普通来源直读原getter；本地来源只经原send在执行线程取值，外部线程同步接收。
//! 不调用登记／上报／入口，不更新原计数。合同与验证见MET-IMPLEMENTATION-001。

use std::{marker::PhantomData, sync::Arc, time::Instant};
use crossbeam_channel::{bounded, RecvTimeoutError};
use parking_lot::Mutex;
use crate::rt::{AsyncRuntime, serial_local_thread::LocalTaskRuntime};
use super::{counts::RuntimeMetricCounts, sample_error::RuntimeMetricSampleError,
    identity::RuntimeMetricIdentity};

/// 同步采样出口；deadline是已裁剪的单调截止，不是timer／驱动对象。
///
/// 非纯、非幂等：读取可变状态，本地还会分配／send并占住外部调用线程。
/// 只许外部允许等待且不推动runtime的线程调用，不持依赖锁。成本为原getter成本；
/// 本地另有一次任务／通道／通知和有限截止等待，不保证OS硬实时或SDK之外的进度。
/// Counts保留原数字，None只表示无wait接口；三Error不携带源或控制权限。
pub(super) trait RuntimeMetricsSource: Send + Sync {
    fn sample(&self, deadline: Instant) -> Result<RuntimeMetricCounts, RuntimeMetricSampleError>;
}

/// 普通来源拥有合法的原runtime Clone；泛型只存在内部，PhantomData不拥有O。
pub(super) struct RuntimeSource<R, O> {
    runtime: R,
    output: PhantomData<fn() -> O>,
}

impl<R, O> RuntimeSource<R, O> {
    /// 消费已有合法句柄，O(1)移动保存，不再Clone、分配、采样或调用用户。
    /// 来源析构沿原R的生命周期合同释放；只有满足AsyncRuntime的R才能装配成来源。
    pub(super) fn new(runtime: R) -> Self { Self { runtime, output: PhantomData } }
}

impl<R: AsyncRuntime<O>, O: Default + 'static> RuntimeMetricsSource for RuntimeSource<R, O> {
    fn sample(&self, deadline: Instant) -> Result<RuntimeMetricCounts, RuntimeMetricSampleError> {
        if Instant::now() >= deadline { return Err(RuntimeMetricSampleError::DeadlineExceeded); }
        let counts = RuntimeMetricCounts::new(self.runtime.len(), Some(self.runtime.wait_len()));
        // getter不可硬抢占；完成后观察已到期时不能把迟观察当成按时成功。
        if Instant::now() >= deadline { return Err(RuntimeMetricSampleError::DeadlineExceeded); }
        Ok(counts)
    }
}

/// Mutex仅允许合法克隆!Sync句柄，不能使UnsafeCell内部队列异线程可读。
/// 源强持runtime；身份不持runtime，避免新增引用环。所有队列读取由rt helper负责。
pub(super) struct LocalRuntimeSource<O: Default + 'static> {
    runtime: Mutex<LocalTaskRuntime<O>>,
    identity: Arc<RuntimeMetricIdentity>,
}

impl<O: Default + 'static> LocalRuntimeSource<O> {
    /// 消费本核心句柄／身份，O(1)初始化保护锁，不投递任务、不构造O或读取队列。
    /// 身份必须来自runtime自己的冷槽；不能借Mutex将原len的owner边界扩大到其它线程。
    pub(super) fn new(runtime: LocalTaskRuntime<O>, identity: Arc<RuntimeMetricIdentity>) -> Self {
        Self { runtime: Mutex::new(runtime), identity }
    }
}

impl<O: Default + 'static> RuntimeMetricsSource for LocalRuntimeSource<O> {
    fn sample(&self, deadline: Instant) -> Result<RuntimeMetricCounts, RuntimeMetricSampleError> {
        if Instant::now() >= deadline { return Err(RuntimeMetricSampleError::DeadlineExceeded); }
        let permit = self.identity.try_claim().ok_or(RuntimeMetricSampleError::RequestInFlight)?;
        let (sender, receiver) = bounded(1);
        let runtime = self.runtime.lock().clone();
        // 来源锁已释放；截止前不再能投递时，许可尚未交给任务，可由本栈正常释放。
        if Instant::now() >= deadline { return Err(RuntimeMetricSampleError::DeadlineExceeded); }
        runtime.send_metric_sample(sender, permit);
        let reply = receiver.recv_deadline(deadline);
        // Crossbeam先尝试取已有值；必须保留本库接收后的到期优先检查。
        if Instant::now() >= deadline { return Err(RuntimeMetricSampleError::DeadlineExceeded); }
        match reply {
            Ok(len) => Ok(RuntimeMetricCounts::new(len, None)),
            Err(RecvTimeoutError::Timeout) => Err(RuntimeMetricSampleError::DeadlineExceeded),
            Err(RecvTimeoutError::Disconnected) => Err(RuntimeMetricSampleError::ReplyClosed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rt::{single_thread::SingleTaskRunner, serial_local_thread::LocalTaskRunner};
    use std::{rc::Rc, sync::{OnceLock, atomic::{AtomicUsize, Ordering}}, thread, time::Duration};

    /// 本测试进程真实时钟只初始化一次并保留守卫；生产指标模块不调用本夹具。
    fn time_loop() {
        static CLOCK: OnceLock<crate::rt::GlobalTimeLoopHandle> = OnceLock::new();
        CLOCK.get_or_init(|| crate::rt::startup_global_time_loop(1).expect("专项应独占全局时钟"));
    }

    /// 只观测原公共队列收到请求，等待有界；不造任务、回填、通知或生产截止。
    fn wait_queued<O: Default + 'static>(runtime: &LocalTaskRuntime<O>) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while runtime.len() == 0 {
            assert!(Instant::now() < deadline, "真实send未投递采样任务");
            thread::yield_now();
        }
    }

    /// 私有错误按冻结合同没有Debug，判定器只读取成功纯数字，不吞错误或代填结果。
    fn values(result: Result<RuntimeMetricCounts, RuntimeMetricSampleError>) -> (usize, Option<usize>) {
        match result {
            Ok(counts) => (counts.len(), counts.wait_len()),
            Err(_) => panic!("真实采样必须成功"),
        }
    }

    /// MET-SOURCE：真实Single的队列对照、零值／Some(0)、已到期不推进及原spawn／run链。
    #[test]
    fn test_direct_source_preserves_original_getters() {
        time_loop();
        let runner = SingleTaskRunner::<()>::default();
        let runtime = runner.startup().unwrap();
        let source = Arc::new(RuntimeSource::<_, ()>::new(runtime.clone()));
        for _ in 0..3 { runtime.spawn(async {}).unwrap(); }
        let copy = source.clone();
        assert_eq!(thread::spawn(move || values(copy.sample(Instant::now() + Duration::from_secs(2)))).join().unwrap(),
            (runtime.len(), Some(runtime.wait_len())));
        let copy = source.clone();
        assert!(matches!(thread::spawn(move || copy.sample(Instant::now())).join().unwrap(),
            Err(RuntimeMetricSampleError::DeadlineExceeded)));
        assert_eq!(runtime.len(), 3);
        for _ in 0..3 { runner.run_once().unwrap(); }
        assert_eq!(thread::spawn(move || values(source.sample(Instant::now() + Duration::from_secs(2)))).join().unwrap(), (0, Some(0)));
    }

    static LOCAL_DROPS: AtomicUsize = AtomicUsize::new(0);
    struct ThreadBound(Rc<thread::ThreadId>);
    impl Default for ThreadBound {
        fn default() -> Self { Self(Rc::new(thread::current().id())) }
    }
    impl Drop for ThreadBound {
        fn drop(&mut self) {
            assert_eq!(*self.0, thread::current().id(), "!Send输出必须在构造线程析构");
            LOCAL_DROPS.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// MET-SOURCE：真实本地停推→生产截止→拒绝重复→恢复驱动／迟到→新请求成功。
    /// !Send输出在真实owner构造／析构；接收始终在不同的外部线程，不占本地执行线程。
    #[test]
    fn test_native_deadline_in_flight_late_reply_and_thread_bound_output() {
        let runner = LocalTaskRunner::<ThreadBound>::new();
        let runtime = runner.get_runtime();
        let identity = runtime.runtime_metric_slot().get_or_allocate().unwrap();
        let source = Arc::new(LocalRuntimeSource::new(runtime.clone(), identity.clone()));
        let before = LOCAL_DROPS.load(Ordering::Relaxed);
        let copy = source.clone();
        let expired = thread::spawn(move || copy.sample(Instant::now())).join().unwrap();
        assert!(matches!(expired, Err(RuntimeMetricSampleError::DeadlineExceeded)));
        assert_eq!(runtime.len(), 0);
        let copy = source.clone();
        let result = thread::spawn(move || copy.sample(Instant::now() + Duration::from_millis(20))).join().unwrap();
        assert!(matches!(result, Err(RuntimeMetricSampleError::DeadlineExceeded)));
        assert_eq!(runtime.len(), 1);
        assert!(identity.try_claim().is_none());
        for _ in 0..3 {
            let copy = source.clone();
            let result = thread::spawn(move || copy.sample(Instant::now() + Duration::from_secs(2))).join().unwrap();
            assert!(matches!(result, Err(RuntimeMetricSampleError::RequestInFlight)));
            assert_eq!(runtime.len(), 1);
        }
        runner.poll();
        runner.run_once();
        assert_eq!(runtime.len(), 0);
        assert_eq!(LOCAL_DROPS.load(Ordering::Relaxed), before + 1);
        assert!(identity.try_claim().is_some());
        let copy = source.clone();
        let receiver = thread::spawn(move || copy.sample(Instant::now() + Duration::from_secs(2)));
        wait_queued(&runtime);
        runner.poll();
        runner.run_once();
        assert_eq!(values(receiver.join().unwrap()), (0, None));
        assert_eq!(LOCAL_DROPS.load(Ordering::Relaxed), before + 2);
        assert_eq!(runtime.len(), 0);
    }

    /// MET-SOURCE：合法用户Default panic在真实owner展开；发送端无值退出必须ReplyClosed。
    /// 不在测试侧取消／drop回复Sender，验证生产helper的实际展开与许可释放。
    #[test]
    fn test_native_default_panic_closes_reply_and_releases_permit() {
        struct Panics;
        impl Default for Panics { fn default() -> Self { panic!("真实用户Default故障"); } }
        let runner = LocalTaskRunner::<Panics>::new();
        let runtime = runner.get_runtime();
        let identity = runtime.runtime_metric_slot().get_or_allocate().unwrap();
        let source = Arc::new(LocalRuntimeSource::new(runtime.clone(), identity.clone()));
        let receiver = thread::spawn(move || source.sample(Instant::now() + Duration::from_secs(2)));
        wait_queued(&runtime);
        runner.poll();
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| runner.run_once())).is_err());
        assert!(matches!(receiver.join().unwrap(), Err(RuntimeMetricSampleError::ReplyClosed)));
        assert!(identity.try_claim().is_some());
        assert_eq!(runtime.len(), 0);
    }
}
