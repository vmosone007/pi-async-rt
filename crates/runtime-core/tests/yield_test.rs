//! 公开主动让出（public cooperative yield）的惰性（laziness）、通知（notification）和完成（completion）。
use pi_async_runtime_core::yield_now;
use std::future::Future;
use std::sync::{atomic::{AtomicUsize, Ordering}, Arc};
use std::task::{Context, Poll, Wake, Waker};

/// 只观察真实唤醒（wake-up），不轮询（poll）或代补被测异步计算（Future）的状态推进（state advancement）。
#[derive(Default)]
struct WakeCount(AtomicUsize);
impl Wake for WakeCount {
    fn wake(self: Arc<Self>) { self.0.fetch_add(1, Ordering::Relaxed); }
    fn wake_by_ref(self: &Arc<Self>) { self.0.fetch_add(1, Ordering::Relaxed); }
}

/// 验证 yield_now 构造／未轮询释放（unpolled drop）无通知（notification），首次轮询（first poll）
/// 返回 Pending 且安排一次自身唤醒（self wake-up），下一次 Ready 且不再通知（notify）。
/// 不断言（assert）其它任务（Task）必定先运行，无定时器（timer）或运行时（runtime）脚手架（harness）。
#[test]
fn test_yield_now_is_lazy_and_yields_exactly_once() {
    let count = Arc::new(WakeCount::default());
    let waker = Waker::from(count.clone());
    let mut context = Context::from_waker(&waker);
    let abandoned = yield_now();
    assert_eq!(count.0.load(Ordering::Relaxed), 0);
    drop(abandoned);
    assert_eq!(count.0.load(Ordering::Relaxed), 0);
    let mut future = std::pin::pin!(yield_now());
    assert_eq!(count.0.load(Ordering::Relaxed), 0);
    assert_eq!(future.as_mut().poll(&mut context), Poll::Pending);
    assert_eq!(count.0.load(Ordering::Relaxed), 1);
    assert_eq!(future.as_mut().poll(&mut context), Poll::Ready(()));
    assert_eq!(count.0.load(Ordering::Relaxed), 1);
}
