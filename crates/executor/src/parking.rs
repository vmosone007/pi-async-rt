//! 绑定线程（thread）的主动通知与被动限时返回，不是任务唤醒器（Waker）。
use pi_async_runtime_core::task::TaskNotifier;
use pi_async_task_queue::notify::QueueNotifier;
use std::{
    marker::PhantomData,
    rc::Rc,
    sync::{atomic::AtomicBool, Arc},
    thread::{Thread, ThreadId},
    time::Duration,
};

/// 原线程的透明通知句柄（notification handle），支持Clone＋Send＋Sync。
///
/// 只保住线程通知状态，不保住原Future或完整服务。并发通知合并，不为一次通知创建任务。
/// 只保证安排线程重新检查，不承诺已经轮询任务或完成取消／关闭。
pub struct ThreadNotifier {
    shared: Arc<ThreadNotification>,
}
struct ThreadNotification {
    thread: Thread,
    notified: AtomicBool,
}
impl Clone for ThreadNotifier {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}
impl PartialEq for ThreadNotifier {
    /// 句柄（handle）指向同一份通知状态时相等。同一个线程也可拥有不同的通知状态。
    fn eq(&self, other: &Self) -> bool {
        thread_notifier_eq_inner(self, other)
    }
}
impl Eq for ThreadNotifier {}
impl QueueNotifier for ThreadNotifier {
    /// 先Release记录通知，再按需unpark同一线程；已存在通知时合并。
    fn notify(&self) {
        thread_notify_inner(&self.shared)
    }
}
impl TaskNotifier for ThreadNotifier {}

/// 只有创建线程可以停放和消费通知；不可跨线程转移或共享。
///
/// 提交、任务唤醒、取消和关闭的实际发布者通过[`ThreadNotifier`]通知。
/// 策略、来源重查和最迟再次检查时刻由执行器负责，本对象不判断空闲，不访问队列。
pub struct ThreadParker {
    notifier: ThreadNotifier,
    owner: ThreadId,
    local: PhantomData<Rc<()>>,
}
/// 标准线程停放允许伪唤醒；不能把一次返回伪报成已经到达截止时间。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ThreadParkResult {
    /// 已取得一次实际通知；对外工作仍须重新检查。
    Notified,
    /// 超时、伪唤醒或零时长返回，须重查，不保证发生过主动通知。
    Recheck,
}
impl ThreadParker {
    /// 绑定当前线程，准备一份共享通知状态，初始notified=true以安排首次检查。
    pub fn new() -> Self {
        thread_parker_new_inner()
    }
    /// 克隆同一原线程的受限通知，不产生另一个停放拥有者。
    pub fn notifier(&self) -> ThreadNotifier {
        self.notifier.clone()
    }
    /// 读取固定原线程身份，不取得其它线程推进权。
    pub fn owner_thread(&self) -> ThreadId {
        self.owner
    }
    /// Acquire取样，不消费通知，不证明下一瞬间可以休眠。
    pub fn is_notified(&self) -> bool {
        thread_is_notified_inner(&self.notifier.shared)
    }
    /// 原线程一次Acquire交换，取走现有通知；取走后必须实际重新检查工作。
    pub fn take_notification(&mut self) -> bool {
        thread_take_notification_inner(self)
    }
    /// 通知存在则直接返回，否则限时停放，返回后再次取得通知并重新分类。
    ///
    /// `duration`是调用方选定的本次上限，零不等待。休眠策略与定时来源最迟检查时刻须
    /// 在调用前组合；本层不擅自延长上限。最后通知检查到实际park之间不得执行用户回调、
    /// 日志或另一个park，以免其它代码消费同一线程permit。任何返回都需要上层重查来源。
    /// 系统调度可能延迟线程恢复，本函数不承诺墙钟硬上界。
    pub fn park_timeout(&mut self, duration: Duration) -> ThreadParkResult {
        thread_park_inner(self, duration)
    }
}
impl Default for ThreadParker {
    fn default() -> Self {
        Self::new()
    }
}
include!("parking_inner.rs");
