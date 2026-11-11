//! 空闲任务队列（idle task queue）的共享访问及独立管理接口（API）。
//!
//! 队列（queue）保存原可执行句柄（Runnable），不自行判断是否空闲。
//! 运行时（runtime）先依据自己的策略判断，再调用普通取出；取消（cancellation）清理
//! 不受空闲判定限制。此处只提供抽象声明，不提供默认存储或执行器（executor）。
use crate::management::QueueCleanupProgress;
use pi_async_task::Runnable;
use std::num::NonZeroUsize;

// 共享访问和唯一管理分别表达；关联类型（associated type）相同不证明实例相同。
// handle()与其克隆（Clone）必须指向原管理对象负责的同一来源，不另建存储或重置计数。
// 公开给运行时（runtime）业务调用者的包装只暴露普通权限，不导出管理对象或裸存储。
// 已绑定任务（Task）不能借此改变所属线程（owner thread）；idle_local只选择提交目标，
// 不以提交线程（thread）绑定将来的执行。上层idle／idle_local要求输入和输出均Send，
// 不要求Sync；这里直接接管原Runnable，不额外创建任务（Task）或专用键（key）。
// 具体取消发现、固定清理范围及并发发布的实现尚未在此选定；声明不冒充这些机制已完成。

/// 可被多个线程（thread）共享的空闲来源；每份原可执行句柄（Runnable）只交付一次。
///
/// 所有方法同步返回，不等待未来任务（Task）、容量或其它线程（thread）完成工作，
/// 不以无界重试取得访问权。允许已说明的短时同步成本；具体复杂度和分配由实现说明。
/// 返回的向量（Vec）独立拥有内容，不携带队列（queue）的借用或锁守卫（lock guard）；
/// 迭代不再访问来源，也不吸收后来提交的内容。调用用户代码、唤醒器（Waker）或
/// 析构（drop）原内容之前须退出对应同步保护。
// @trace 1
pub trait IdleTaskQueue: Send + Sync + 'static {
    /// 尝试接收原`task`；成功后来源承担保管和后续交付责任。
    ///
    /// 不判断空闲，不进行业务轮询（poll），不转入普通外部或本地来源。
    /// 明确拒绝时返回完整原值；已经接收后发生的问题仍由来源负责，不能事后假装拒绝。
    /// 每次调用提交一份原值，不是可重复执行的幂等（idempotent）提交。
    fn try_push(&self, task: Runnable) -> pi_result::RawResult<(), IdleSubmitError>;

    /// 在调用者已经判定空闲后，尝试取得本次有限数量的普通任务（Task）。
    ///
    /// `max_tasks`为本次非零数量上限；首版调用者传1。`Ready`内向量（Vec）必须非空且
    /// 不超过上限；实现决定顺序，调用者须保持顺序。本次可少取，不等待未来提交。
    /// `Empty`不证明运行时（runtime）空闲或可关闭；`WouldBlock`／`Busy`不应无限重试。
    /// 正常出口关闭后返回正常分类`Closed`，不因此停止管理或取消（cancellation）清理。
    fn try_take_batch(
        &self,
        max_tasks: NonZeroUsize,
    ) -> pi_result::RawResult<IdleTake, IdleQueueError>;

    /// 独立尝试取得有限待清理内容，不要求空闲，也不受普通取出上限1限制。
    ///
    /// `Ready`交出非空原值序列及后续处理责任，不证明取消（cancellation）成功或原
    /// 异步计算（Future）已析构（drop）。并发调用时同一原值只有一个领取者。
    /// `Err`没有交出向量（Vec），来源保留未完成责任，但此前状态变化不因此回滚。
    fn try_take_cleanup(&self) -> pi_result::RawResult<IdleTake, IdleQueueError>;

    /// 查询本来源单向变化的状态快照（snapshot），不推进业务或清理。
    /// 不承诺与另一次数量读取对应同一瞬间。
    fn status(&self) -> IdleQueueStatus;

    /// 安全查询已接收但尚未交付或实际清理的任务（Task）数量。
    ///
    /// 包括中转、正常待取及待清理内容；内部搬运或取消标记不减数，实际交付／清理
    /// 只减一次。不含已交给执行器（executor）的在途任务（in-flight task）。
    /// 单次样本不保证下一次取出结果，不以并发近似值冒充此口径。
    fn len(&self) -> usize;

    /// 按`len`相同口径查询当次是否为零，不等待稳定，不证明来源关闭完成。
    fn is_empty(&self) -> bool;
}

/// 空闲来源实际清理／关闭的唯一管理责任；普通共享访问不具有这些权限。
///
/// 默认构造须保持唯一管理者，不提供克隆（Clone）管理者的能力。
/// 此特征（trait）本身无法禁止第三方实现克隆（Clone），构造与装配须维护唯一责任。
/// 整体运行时（runtime）管理共享来源；单个工作成员退出不应清空其它成员的来源。
/// `&mut self`串行化实际管理，不要求外部关闭请求取得`&mut Runtime`。
// @trace 2
pub trait IdleQueueManager {
    /// 同一来源的普通访问对象；克隆（Clone）不产生新来源或管理权限。
    type Handle: IdleTaskQueue + Clone;

    /// 取得同一原来源的受限共享访问，不重新开放已关闭来源。
    fn handle(&self) -> Self::Handle;

    /// 有限推进本次固定范围的清理，不关闭接收，不轮询（poll）业务。
    ///
    /// 首次调用固定范围，未完成的重复调用继续该范围；后来提交不无限扩展本次工作。
    /// 范围完成后下一次调用可清理新内容；无新增内容时不重复交付或释放原值。
    /// 返回向量（Vec）可以为空，不能据此推导完成；`Err`时仍保有未交出的原值和责任。
    /// 不等待其它线程（thread）发布完成，不以无界循环等候取得保护。
    fn clear(&mut self) -> pi_result::Result<QueueCleanupProgress<Vec<Runnable>>>;

    /// 先停止接收，再复用清理过程；错误或尚有工作均不得重新开放。
    ///
    /// 包含旧未完成清理和停收前已接收内容的剩余责任，不能遗漏尚未完成的合法发布。
    /// 只有该来源责任完成／合法移交后才返回`Complete`；不等待所有共享句柄（shared handle）
    /// 释放。重复调用幂等（idempotent），不重复交出内容；取消（cancellation）清理不受
    /// 空闲条件或普通取出上限限制。每次仅做有限工作，不新增恐慌（panic）捕获规则。
    fn close(&mut self) -> pi_result::Result<QueueCleanupProgress<Vec<Runnable>>>;
}

/// 来源服务状态（state）；普通取出出口可以在整体仍为`Closing`时先结束。
/// 数量、任务（Task）退出以及原异步计算（Future）析构（drop）分别查询。
// @trace 3
pub enum IdleQueueStatus {
    /// 接收开放；不保证每次提交均成功。
    Accepting,
    /// 已停收，自身清理或发布责任尚未完成。
    Closing,
    /// 自身资源及责任已完成或合法移交；不表示已交出任务（Task）全部结束。
    Closed,
}

/// 一次普通／清理取出的结果；正常分支与异常错误分别表达。
// @trace 4
pub enum IdleTake {
    /// 非空原值序列；交接责任一次，不能继续保留会重复交付的副本。
    Ready(Vec<Runnable>),
    /// 本次无可交付内容，不是全局结束证明。
    Empty,
    /// 发布或本次必要准备未完成，不等同于空。
    WouldBlock,
    /// 本次同步访问权被占用，不无限等待。
    Busy,
    /// 该取出出口已结束，不等于来源全部管理责任已结束。
    Closed,
}

/// 明确未接收的原值与原因；原值置于错误报告（Report）之外。
// @trace 5
pub struct IdleSubmitError {
    /// 调用者继续持有的原可执行句柄（Runnable）。
    pub task: Runnable,
    /// 本次明确拒绝原因，不等于原任务（Task）取消成功。
    pub cause: IdleQueueError,
}

/// 空闲来源的有界异常分类；不强迫原任务（Task）成为错误上下文（error context）。
/// 正常取出中的`Busy`／`Closed`用`IdleTake`对应分支，不重复套同名错误。
// @trace 6
pub enum IdleQueueError {
    /// 来源已停止接收或所需操作已不再开放。
    Closed,
    /// 请求／管理所需的一次访问暂不可取得。
    Busy,
    /// 实现声明的容量限制拒绝。
    Capacity,
    /// 逻辑数量将溢出，不允许回绕。
    CountOverflow,
    /// 原底层错误报告（Report）；保留其分类、来源和附件，不要求可克隆（Clone）。
    Backend(pi_result::Error),
}
