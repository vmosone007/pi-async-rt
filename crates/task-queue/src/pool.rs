//! 已有pi_task_pool的外部队列适配；沿用其原生权重选择与返回顺序。
use crate::external::*;
use parking_lot::Mutex;
use pi_async_task::Runnable;
use pi_result::RawResult as Result;
use pi_task_pool::{TaskExecutor, TaskHandle, TaskPool, TaskWeight};
use std::sync::atomic::{AtomicBool, AtomicUsize};

// 独占适配器创建的私有原生池，禁止旁路push/pop；只用并行Middle档，禁快速直接执行。
// 原生项是一次Runnable交付（Option包装用于安全take），不是重新创建的业务任务。
// pop取得TaskHandle后把Option取出；该交付完成才释放原生句柄，不使用串行占用或等待业务结果。
// native pool为静态生命周期，不提供池回收；此适配不伪造可销毁保证，close须先退任务持有。
// 精确count由所有适配入口共同维护，不使用native粗略queue_length_estimates代替。
// 生产/消费都先try_lock，同一次锁内完成原生修改与计数发布；Busy不等待、不丢原任务。
// 批取一次try_lock，记录当前count作为有限上界，至多pop该数量；不等新生产者。
// 当前锁内无另一适配发布者，减少遇到原生未完成发布的机会；不能据编译声称完整时限已验证。
// 原生pop提前None且逻辑count仍正数时返回WouldBlock或已有前缀，不虚报全局空或补全缺失任务。

/// 默认外部共享任务池适配；只通过本对象提交和获取才能保持计数契约。
///
/// 若队列中任务（Task）的交付实现也持有本队列，会形成强持有环；宿主必须显式关闭并
/// 取回／移交全部队列持有，单纯释放外部句柄不保证触发Drop。原生池本体仍保留静态分配。
// @trace 100
pub struct TaskPoolQueue {
    pool: &'static TaskPool<Option<Runnable>, NoFastPath>,
    access: Mutex<()>,
    queued: AtomicUsize,
    closed: AtomicBool,
    random: fn() -> u64,
}

// @trace 101
struct NoFastPath;

// 这三项是真实接线策略：禁快速执行，只让原生池负责排队，不是业务未实现的默认成功值。
impl TaskExecutor<Option<Runnable>> for NoFastPath {
    fn fast_path_threshold(&self) -> usize {
        0
    }
    fn is_idle(&self) -> bool {
        false
    }
    fn try_execute(
        &self,
        task: TaskHandle<Option<Runnable>, Self>,
    ) -> Result<(), TaskHandle<Option<Runnable>, Self>> {
        Err(task)
    }
}

// @trace 1
impl TaskPoolQueue {
    /// 创建私有适配池；random须快速、线程安全（thread-safe）、不阻塞，返回任意u64。
    /// 原生池构造保留静态分配，不承诺Drop回收池本体；应复用实例。
    pub fn new(random: fn() -> u64) -> Self {
        pool_new_inner(random)
    }
}

// @trace 2
impl TaskPoolQueue {
    /// 停止新接纳并尝试取回当前任务（Task）；Busy/WouldBlock可继续调用，不等待消费者。
    /// 关闭后的剩余任务仍须由合法所属域清理，不因此消失。
    pub fn close(&self) -> BatchAttempt {
        pool_close_inner(self)
    }
}

impl ExternalTaskQueue for TaskPoolQueue {
    // @trace 3
    /// 单次尝试外部提交（submission），拒绝包含原Runnable；不执行原异步计算（Future）。
    fn try_push(&self, task: Runnable) -> Result<(), ExternalPushError> {
        pool_push_inner(self, task)
    }

    // @trace 4
    /// 单次同步获取当前可取批次；保持原生pop选出顺序，不保证全局FIFO。
    fn try_take_batch(&self) -> BatchAttempt {
        pool_take_inner(self)
    }

    // @trace 5
    /// 单次原子读取适配队列精确逻辑数量；不把粗略权重提示相加。
    fn len(&self) -> usize {
        pool_len_inner(self)
    }
}

// @trace 6
impl Drop for TaskPoolQueue {
    /// 关闭并归还队列仍持有的任务（Task）；不宣称回收原生静态池本体。
    fn drop(&mut self) {
        pool_drop_inner(self)
    }
}

include!("pool_inner.rs");
