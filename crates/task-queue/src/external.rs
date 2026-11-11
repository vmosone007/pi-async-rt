//! 外部任务队列（external task queue）的统一接口及有序批次。
use pi_async_task::Runnable;
use pi_result::RawResult as Result;
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

include!("external_inner.rs");
