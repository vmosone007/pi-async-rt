//! 队列（queue）发布后的所属线程通知（owner-thread notification）。

/// 安排所属线程（owner thread）继续检查队列（queue）的受限接口（interface）。
///
/// 实现由组合该队列（queue）的运行时（runtime）提供。正常返回表示已经安排检查；
/// 本方法同步、非等待，不直接轮询（poll）业务，不等待所属线程（owner thread）响应。
/// 通知（notification）发生在完整发布之后及原生消费者权限（Consumer）之外。
///
/// 多次调用允许由实现合并，但不能遗漏仍待处理的发布或关闭责任。通知（notification）
/// 不代替休眠前重查与被动定时唤醒（passive timed wake-up）。
/// 恐慌（panic）继续传播，已经接收、发布的事实不回滚；来源仍负责退出确认。
/// 本特征（trait）没有默认空实现，以免把未安排检查误报成成功。
// @trace 1
pub trait QueueNotifier: Send + Sync + 'static {
    /// 同步安排一次后续检查；不得等待业务、队列（queue）内容或其它线程（thread）。
    fn notify(&self);
}
