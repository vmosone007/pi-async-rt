//! 未成功交付的原任务（Task）及继续持有责任。
use pi_async_task::Runnable;
use pi_async_task_queue::{ExternalPushErrorKind, QueuePushErrorKind};

// Schedule::schedule和Waker::wake均没有错误返回通道，因此恢复对象必须接管原持有。
// retain正常返回以后，原任务及未处理唤醒需求仍由该实现负责重试或合法退出；
// 不能丢弃原值、在调用者线程轮询它，或把责任转给已经返回的Waker调用者。
// 该接口不替实现提供默认存储；具体运行时必须装配一个满足成本和退出边界的实现。

// @trace 1
/// 本次投递明确失败的分类，不等同目标任务（Task）已经结束。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DispatchFailure {
    /// 原目标已停止新投递，原任务仍须合法清理。
    Closed,
    /// 原任务尚未绑定或属于其它所属域；本次不能重新绑定。
    WrongOwner,
    /// 显式本地提交暂时无法借用队列，本次不等待。
    LocalBorrowed,
    /// 本地入队明确拒绝，保留其原始分类。
    Local(QueuePushErrorKind),
    /// 外部队列明确未接纳，保留其原始分类。
    External(ExternalPushErrorKind),
}

/// 没有被接纳到目标队列（queue）的原值，调用者继续承担交付或退出责任。
// @trace 2
pub struct DeliveryError {
    /// 未入队的唯一可执行持有，身份和待处理需求不变。
    pub task: Runnable,
    /// 本次明确拒绝原因。
    pub cause: DispatchFailure,
}

/// 接管无法交付的任务（Task）；实现必须线程安全（thread-safe）。
///
/// 正常返回代表已接管原值及重试／合法退出责任；不得静默丢失唤醒需求。
/// 不等待容量、其它线程或异步结果，不在本调用中轮询（poll）原异步计算（Future）。
/// 可以请求合法取消（cancellation），不能在错误线程析构（drop）原值。
/// 实现自身恐慌（panic）不由本库另行捕获，不能将其冒充正常拒绝。
// @trace 3
pub trait DispatchRecovery: Send + Sync + 'static {
    /// 接管task；reason只说明本次交付失败，重复请求仍由任务自身合并。
    fn retain(&self, task: Runnable, reason: DispatchFailure);
}
