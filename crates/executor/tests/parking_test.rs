//! 线程停放（thread parking）的公开拥有权（ownership）与通知（notification）行为。
//! 各用例（test cases）自行拥有线程绑定（thread binding），不共享全局可变状态（global mutable state）。
use pi_async_executor::{ThreadNotifier, ThreadParker};

/// 验证 ThreadParker::new 绑定创建线程（creating thread），每份拥有者（owner）可取得同源共享句柄（shared handles）。
/// 静态约束（static bounds）核对 ThreadNotifier 支持 Clone、Send、Sync；不转移受限拥有者（restricted owner）。
#[test]
fn test_thread_parker_binds_current_thread_and_shares_notification_handle() {
    fn assert_shared<T: Clone + Send + Sync>() {}
    assert_shared::<ThreadNotifier>();
    let parker = ThreadParker::new();
    assert_eq!(parker.owner_thread(), std::thread::current().id());
    let notifier = parker.notifier();
    let same_notifier = notifier.clone();
    drop(notifier);
    drop(same_notifier);
    assert_eq!(parker.owner_thread(), std::thread::current().id());
}

/// 验证 ThreadParker::is_notified 观察首次检查通知（initial check notification）而不消费（consume）。
/// 连续读取（reads）均保持真，查询（query）不取得其它线程（thread）的推进权限（drive permission）。
#[test]
fn test_thread_parker_query_preserves_initial_notification() {
    let parker = ThreadParker::new();
    assert!(parker.is_notified());
    assert!(parker.is_notified());
}

/// 验证 ThreadParker::take_notification 在创建线程（creating thread）只消费一次初始通知（initial notification）。
/// 消费后连续查询（queries）及再次取得（take）均为假，不能把旧通知（notification）重复交付（deliver）。
#[test]
fn test_thread_parker_consumes_each_initial_notification_once() {
    let mut parker = ThreadParker::new();
    assert!(parker.is_notified());
    assert!(parker.take_notification());
    assert!(!parker.is_notified());
    assert!(!parker.is_notified());
    assert!(!parker.take_notification());
}

/// 验证 QueueNotifier::notify 通过原共享句柄（shared handle）重新发布（publish）通知（notification）。
/// 连续通知（notifications）合并为一次取得（take）；两名真实发布线程（publisher threads）只发通知（notification），
/// 不代替生产消费（production consumption），主线程（main thread）在会合（join）后核实际记录（record）。
#[test]
fn test_thread_notifier_merges_repeated_and_cross_thread_notifications() {
    use pi_async_task_queue::notify::QueueNotifier;
    let mut parker = ThreadParker::new();
    assert!(parker.take_notification());
    assert!(!parker.is_notified());
    let notifier = parker.notifier();
    notifier.notify();
    notifier.notify();
    assert!(parker.is_notified());
    assert!(parker.take_notification());
    assert!(!parker.take_notification());
    std::thread::scope(|scope| {
        let first = notifier.clone();
        let second = notifier.clone();
        let one = scope.spawn(move || first.notify());
        let two = scope.spawn(move || second.notify());
        one.join().unwrap();
        two.join().unwrap();
    });
    assert!(parker.is_notified());
    assert!(parker.take_notification());
    assert!(!parker.take_notification());
    drop(parker);
    notifier.notify();
}

/// 验证 ThreadParker::park_timeout 优先消费通知（notification），零时长（zero duration）返回重查（recheck）。
/// 直接标准许可（standard permit）不含生产通知事实（production notification fact），不得误报为 Notified；
/// 限时等待（timed wait）允许伪唤醒（spurious wake-up），不以耗时（elapsed time）证明准确到期（deadline）。
#[test]
fn test_thread_parker_classifies_notifications_zero_timeout_and_bare_permit() {
    use pi_async_executor::ThreadParkResult;
    use pi_async_task_queue::notify::QueueNotifier;
    use std::time::Duration;
    let mut parker = ThreadParker::new();
    assert_eq!(parker.park_timeout(Duration::ZERO), ThreadParkResult::Notified);
    assert_eq!(parker.park_timeout(Duration::ZERO), ThreadParkResult::Recheck);
    parker.notifier().notify();
    assert_eq!(parker.park_timeout(Duration::ZERO), ThreadParkResult::Notified);
    assert_eq!(parker.park_timeout(Duration::ZERO), ThreadParkResult::Recheck);
    std::thread::current().unpark();
    assert_eq!(parker.park_timeout(Duration::from_millis(1)), ThreadParkResult::Recheck);
    assert_eq!(parker.park_timeout(Duration::from_millis(1)), ThreadParkResult::Recheck);
    assert!(!parker.is_notified());
}

/// 验证真实跨线程通知（cross-thread notification）可在限时停放（timed parking）期间被取得（take）。
/// 两秒截止（deadline）只限制脚手架（test harness）；允许伪唤醒（spurious wake-up）后重查（recheck）。
/// 不声称精确命中内部检查与停放间的窗口（window），不依赖等待耗时（elapsed time）作断言（assertion）。
#[test]
fn test_thread_parker_observes_concurrent_notification_during_timed_wait() {
    use pi_async_executor::ThreadParkResult;
    use pi_async_task_queue::notify::QueueNotifier;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};
    let mut parker = ThreadParker::new();
    assert!(parker.take_notification());
    let entering_wait = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let notifier = parker.notifier();
        let entering_wait = &entering_wait;
        let publisher = scope.spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(2);
            while !entering_wait.load(Ordering::Acquire) {
                assert!(Instant::now() < deadline, "等待测试线程进入（waiting for test thread）");
                std::thread::yield_now();
            }
            std::thread::sleep(Duration::from_millis(2));
            notifier.notify();
        });
        entering_wait.store(true, Ordering::Release);
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if parker.park_timeout(Duration::from_millis(20)) == ThreadParkResult::Notified {
                break;
            }
            assert!(Instant::now() < deadline, "未取得原通知（original notification not observed）");
        }
        assert!(!parker.is_notified());
        publisher.join().unwrap();
    });
}
