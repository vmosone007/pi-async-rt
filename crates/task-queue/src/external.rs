//! 外部任务队列（external task queue）的统一接口及有序批次。
use pi_async_task::Runnable;
use pi_result::RawResult as Result;
use crate::management::QueueCleanupProgress;
use std::collections::VecDeque;
use std::sync::Arc;

// 外部实现可为MPSC、共享任务池或其它结构，不强制侵入式组织。
// try_take_batch只作一次接入同步尝试，以当次可取得的有限集合为界，无默认数量上限；
// 不等新任务，不通过反复获取扩充本批。成功顺序由该实现定义，接入方必须原样保序。
// Empty只说明本次无可取得任务，不证明无在途工作/无Pending任务/可以关闭执行器。
// WouldBlock表示暂未完成发布；执行器可额外立即重试1到2次，仍不可取则继续原事件循环。
// Busy是本次接入同步权已占用，不能改名当Empty或无界自旋；这里不实现执行器循环。
// len报告已接纳尚未交给消费者的逻辑任务数，单次查询不保证下一次获取结果。

/// 外部队列（queue）接口；具体实现负责并发安全和所有权交接。
///
/// 接入不可等待新任务或队列容量；允许实现已说明的短时同步成本，不允许不可预期长等待。
/// 实现者须保证所有生产/消费均纳入同一计数协议，不能将粗略估计伪装成精确数量。
// @trace 100
pub trait ExternalTaskQueue: Send + Sync + 'static {
    /// 尝试接管一份Runnable；拒绝时返还原值，不能执行后再拒绝。
    fn try_push(&self, task: Runnable) -> Result<(), ExternalPushError>;
    /// 一次获取当前有序批次；不能循环等待未来提交来填满批次。
    fn try_take_batch(&self) -> BatchAttempt;
    /// 线程安全（thread-safe）地读取已接纳尚未移交的数量，不含执行中/挂起任务。
    fn len(&self) -> usize;
}

// 只转交同一外部实现；不新建队列或另设计数，因此多个目标可共享同一池。
impl<Q: ExternalTaskQueue + ?Sized> ExternalTaskQueue for Arc<Q> {
    fn try_push(&self, task: Runnable) -> Result<(), ExternalPushError> {
        (**self).try_push(task)
    }
    fn try_take_batch(&self) -> BatchAttempt {
        (**self).try_take_batch()
    }
    fn len(&self) -> usize {
        (**self).len()
    }
}

/// 外部投递拒绝；始终包含未接纳的原Runnable。
// @trace 101
pub struct ExternalPushError {
    /// 由调用者或恢复实现继续持有，不能静默丢失。
    pub task: Runnable,
    /// 本次明确拒绝原因。
    pub cause: ExternalPushErrorKind,
}

/// 明确未接纳的外部投递失败；不表示任务（Task）已经取消。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
// @trace 102
pub enum ExternalPushErrorKind {
    /// 本次非等待式同步获取失败，可保留后重试。
    Busy,
    /// 接入已关闭，不再接纳。
    Closed,
    /// 实现声明的容量限制拒绝。
    Capacity,
    /// 精确计数无法再增加，不能回绕。
    CountOverflow,
    /// 原任务（Task）没有本次来源需要的链接（link）；原Runnable保持不变。
    LinkMissing,
}

/// 一次获取结果，区分空、暂不可完成、同步权占用及已关闭。
// @trace 103
pub enum BatchAttempt {
    /// 有限的当前批次；内部顺序不可擅改。
    Ready(TaskBatch),
    /// 本次没有可取得任务，不是全局结束证明。
    Empty,
    /// 任务发布在途，本次无法完成获取。
    WouldBlock,
    /// 获取接入权被占用；不可无限自旋等待。
    Busy,
    /// 明确关闭；是否还有应清理的任务须按实现契约处理。
    Closed,
}

/// 持有按获取顺序排列的一批唯一Runnable；不复制任务（Task）。
// @trace 104
pub struct TaskBatch {
    pub(crate) tasks: VecDeque<Runnable>,
}

// @trace 1
impl TaskBatch {
    /// 接管已有序列，不排序、不轮询（poll），不新增每任务节点分配。
    pub fn from_tasks(tasks: VecDeque<Runnable>) -> Self {
        batch_from_inner(tasks)
    }
}

// @trace 2
impl TaskBatch {
    /// 取得本批当前任务数，O(1)，不访问外部队列。
    pub fn len(&self) -> usize {
        batch_len_inner(self)
    }
}

// @trace 3
impl TaskBatch {
    /// 判断本批是否为空，不代表运行时（runtime）无任务。
    pub fn is_empty(&self) -> bool {
        batch_empty_inner(self)
    }
}

// @trace 4
impl IntoIterator for TaskBatch {
    type Item = Runnable;
    type IntoIter = std::collections::vec_deque::IntoIter<Runnable>;
    /// 按原顺序转交每份Runnable，未消费部分随迭代器释放并承担正常退出。
    fn into_iter(self) -> Self::IntoIter {
        batch_into_iter_inner(self)
    }
}

/// 普通外部来源的唯一实际管理权限，与共享提交／取出接口（API）分开。
///
/// 默认管理对象不提供克隆（Clone）；特征（trait）不能禁止第三方另行克隆（Clone），
/// 构造与装配负责保持唯一管理责任。整体运行时（runtime）管理共享来源，单个工作成员
/// 退出不能清空其它成员仍在使用的来源。普通运行时（runtime）句柄（handle）不应通过
/// Deref／AsRef或裸存储访问间接取得管理权限。
// @trace 105
pub trait ExternalQueueManager {
    /// 同一原来源的普通共享访问；不是另一队列（queue）或另一份计数。
    type Handle: ExternalTaskQueue + Clone;

    /// 取得同一来源的受限共享句柄（shared handle），不重置已关闭状态（state）。
    fn handle(&self) -> Self::Handle;

    /// 有限清理本次固定范围，接收仍保持原状，不轮询（poll）业务。
    ///
    /// 首次调用固定范围；未完成时重复调用继续原范围，后来提交不无限扩展本次工作。
    /// 来源内部移除并合法清理原值，不向调用者交付待清理批次（batch）。
    /// 完成后新调用可处理新内容；`MoreWork`保留原范围及其剩余责任。
    /// `Err`不回滚已发生的合法变化，未完成原值和责任继续由来源保管。
    /// 不等待新任务（Task）或其它线程（thread）发布完成，不用无界重试取得保护。
    fn clear(&mut self) -> pi_result::Result<QueueCleanupProgress>;

    /// 先停收再复用清理，重复调用幂等（idempotent），失败不重开接收。
    ///
    /// 包含未完成的旧清理及停收前已接收内容，不能遗漏合法发布尚未完成的原值。
    /// 每次有限推进，只有该来源的责任完成／合法移交后才返回`Complete`；
    /// 不等所有共享句柄（shared handle）释放，不将来源结束当作所有任务（Task）已退出。
    /// 实现说明具体成本和资源释放边界，不新增恐慌（panic）捕获或吞错规则。
    fn close(&mut self) -> pi_result::Result<QueueCleanupProgress>;
}

include!("external_inner.rs");
