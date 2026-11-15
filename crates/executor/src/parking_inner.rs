fn thread_parker_new_inner() -> ThreadParker {
    todo!("目标：取得std::thread::current一次，保存其ThreadId，Arc内放同一Thread与AtomicBool(true)，PhantomData<Rc<()>>阻止拥有者Send／Sync。只分配一份通知状态，不创建工作线程，不安装TLS，不初始化时钟。后续句柄仅Clone同一Arc；不得存Future、任务句柄或整个运行时。")
}
#[allow(unused_variables)]
fn thread_notify_inner(shared: &ThreadNotification) {
    todo!("目标：notified.swap(true,Release)，仅旧值false时调用原Thread::unpark。先发布事实再发线程permit；并发及连续通知合并，不能先unpark再写位。正常路径O(1)，无锁、分配、用户hook、任务poll或取消；保留晚到句柄只通知原线程，不复活服务。不可拿任务Waker代替此线程通知。")
}
#[allow(unused_variables)]
fn thread_is_notified_inner(shared: &ThreadNotification) -> bool {
    todo!("目标：Acquire加载notified，不消耗permit、不修改通知，不以本次false推导未来不会通知或可以关闭；不访问来源、任务或时钟。")
}
#[allow(unused_variables)]
fn thread_take_notification_inner(this: &mut ThreadParker) -> bool {
    todo!("目标：核当前ThreadId为owner后，notified.swap(false,Acquire)取得现有通知；线程不符在任何停放前明确panic。原子交换将该读前通知与下一次通知分开；调用方取走后必须实际检查工作。不能因为是同线程而使用普通非原子bool，也不消费其它线程的记录。")
}
#[allow(unused_variables)]
fn thread_park_inner(this: &mut ThreadParker, duration: Duration) -> ThreadParkResult {
    todo!("目标：核原线程，先take_notification，若true直接Notified；零时长无等待返回Recheck。否则立即std::thread::park_timeout(duration)，返回后再次take_notification，有事实为Notified否则Recheck，不声称准确超时。最后检查与park间不执行用户代码、日志、分配或另一个park；检查后通知由原Thread permit保留。处理检查前／间／后通知与伪唤醒，不自旋等待，不重试休眠凑满时长，不用sleep替代unpark，不增加SeqCst或新锁。来源和根通知在外层最后重查，原根Waker使用同一线程permit但独立事实，不能在这里清除其状态。")
}
