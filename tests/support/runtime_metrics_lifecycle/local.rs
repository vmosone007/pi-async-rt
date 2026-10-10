//! MET-L-LOCAL-001：原生本地Poller专项，只调用原send／Runner及关闭接口。
//!
//! 不调用Poller.notify，不跨线程读后台本地len，不补本地Waker的旧通知行为。
//! !Send输出只由生产采样helper在owner创建并由原执行器在owner析构。
//! Poller允许伪唤醒；及时返回不是某次内核通知因果关系或所有竞态的动态证明。

use super::common::{assert_sample, assert_summary, collect_external, collect_native,
    wait_drops, wait_released, wait_running};
use pi_async_rt::{runtime_metrics::*, rt::serial_local_thread::LocalTaskRunner};
use polling::Poller;
use std::{rc::Rc, sync::{Arc, atomic::{AtomicUsize, Ordering}}, thread, time::{Duration, Instant}};

static OUTPUT_DROPS: AtomicUsize = AtomicUsize::new(0);

struct ThreadBoundOutput(Rc<thread::ThreadId>);
impl Default for ThreadBoundOutput {
    fn default() -> Self { Self(Rc::new(thread::current().id())) }
}
impl Drop for ThreadBoundOutput {
    fn drop(&mut self) {
        assert_eq!(*self.0, thread::current().id(), "采样输出必须在原owner析构");
        OUTPUT_DROPS.fetch_add(1, Ordering::Release);
    }
}

/// MET-L03：同一原Poller连续两次send采样并经原try_sleep重新布防，随后后台采样。
/// 5秒原wait不能靠等满超时通过2秒观察门；允许伪唤醒，不断言内核返回的唯一原因。
/// 不手动notify，也不是微秒延迟、所有漏唤醒时序或吞吐基准。
pub(super) fn poller() {
    init_runtime_metrics_reporting("生命周期-本地Poller").unwrap();
    let poller = Arc::new(Poller::new().unwrap());
    let weak = Arc::downgrade(&poller);
    let runner = LocalTaskRunner::<ThreadBoundOutput>::with_poll(poller);
    assert!(runner.is_with_polling());
    let runtime = runner.get_runtime();
    let id = register_runtime_metrics(&runtime, "手动Poller").unwrap();
    for expected in 1..=2 {
        let report = collect_native(&runner);
        assert_summary(&report, 1, 0);
        assert_sample(&report, id, "手动Poller", RuntimeMetricKind::Local,
            RuntimeMetricInstanceStatus::RecordInvoked, Some((0, None)));
        assert_eq!(runtime.len(), 0);
        assert_eq!(OUTPUT_DROPS.load(Ordering::Acquire), expected);
        // 不加入其它任务／测试通知，经原wait重新布防；官方仍允许无事件伪唤醒。
        let deadline = Instant::now() + Duration::from_secs(2);
        assert_eq!(runner.try_sleep(0, Some(Duration::from_secs(5))).unwrap(), None);
        assert!(Instant::now() < deadline, "send通知不能退化为5秒兜底超时");
    }
    assert!(unregister_runtime_metrics(id));
    drop(runtime);
    drop(runner);
    assert!(weak.upgrade().is_none());

    let poller = Arc::new(Poller::new().unwrap());
    let weak = Arc::downgrade(&poller);
    let runtime = LocalTaskRunner::<ThreadBoundOutput>::with_poll(poller)
        .startup_with_poll("Metrics-Lifecycle-Local", 2 * 1024 * 1024, 0, Some(Duration::from_millis(20)));
    wait_running(&runtime);
    let background_id = register_runtime_metrics(&runtime, "后台Poller").unwrap();
    for expected in 3..=4 {
        let report = collect_external();
        assert_summary(&report, 1, 1);
        assert_sample(&report, id, "手动Poller", RuntimeMetricKind::Local,
            RuntimeMetricInstanceStatus::Unregistered, None);
        assert_sample(&report, background_id, "后台Poller", RuntimeMetricKind::Local,
            RuntimeMetricInstanceStatus::RecordInvoked, Some((0, None)));
        // 回复可能早于Future／输出实际退出；等待真实Drop后才能要求下一次不在途。
        wait_drops(&OUTPUT_DROPS, expected);
    }
    assert!(unregister_runtime_metrics(background_id));
    assert!(runtime.clone().close());
    assert!(!runtime.clone().close());
    drop(runtime);
    // 原close不主动notify；有限原wait负责退出，不由夹具补关闭通知或清空队列。
    wait_released(&weak);
    assert_eq!(OUTPUT_DROPS.load(Ordering::Acquire), 4);
}
