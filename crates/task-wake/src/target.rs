//! 固定所属线程与双队列目标；只负责交付，不包含执行器（executor）。
use crate::recovery::{DeliveryError, DispatchFailure, DispatchRecovery};
use pi_async_task::definitions::TaskOwnerId;
use pi_async_task::{ControlHandle, Runnable, Schedule, TaskThread};
use pi_async_task_queue::{BatchAttempt, BatchPushError, ExternalTaskQueue, TaskBatch, TaskQueue};
use pi_result::RawResult as Result;
use std::cell::RefCell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::rc::{Rc, Weak};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::task::Waker;
use std::thread::ThreadId;

// 编译依赖：target -> recovery、任务库、队列库；任务库不反向引用本库。
// 运行访问：任务核心的Schedule回调 -> 本目标 -> 本地/外部队列；恢复只消费透明Runnable。
// 原生RawWaker仍在任务库，其clone只增加原任务存储计数，本库不以Arc<Waker>再套一层。
// Shared只含Send+Sync的数据，绝不保存Rc、队列可变借用或本地Future指针。
// 本地入口只存在创建线程TLS内；查找须同时核owner、target_key、固定ThreadId、closed。
// 精确ThreadId判断同线程；TLS缺失或暂借用失败不等于换了线程，可回退原目标外部接口。
// 先退出TLS的RefCell借用，再尝试队列借用；退出队列借用后才调用恢复或其它用户回调。
// 同一所属域一次只装配一个有效本地入口；guard销毁只停入口，不宣称清理所有任务。
// 关键交付链：try_submit -> target_submit_inner -> (本地尾插 | 原目标外部try_push)。
// Schedule -> try_submit；Err -> recovery.retain，正常返回后责任不能悬空。
// 所有路径都不轮询Future、不更换原目标；本地显式头插只影响下一次选择。
// 清理交付仍走try_submit：owner必须匹配；除已admitted外，仅允许execution=Exiting、
// termination已确定的未接纳Runnable。绑定未接纳任务的首次放弃需要此通路返回原线程。
// 此例外不设置ADMITTED/STARTED，不放宽guard.push_front/back的业务接纳条件。
// Closed或其它拒绝由现有Schedule连接交R.retain；R须保留实际重试或原线程退出通路。
// 只drop退出中的Runnable不能冒充F已清理；target.close与TaskThread.close是不同操作。

/// 同一固定目标的线程安全（thread-safe）透明句柄，可低成本克隆（clone）。
///
/// 克隆只共享原目标，不复制任务（Task）或队列（queue）；业务投递前须已被指定域接纳。
/// 已绑定同一域但未接纳（admission）的退出清理，也可经try_submit交付，不执行业务。
/// 绑定不负责建立执行器（executor）；原异步计算（Future）的访问和清理由任务库管理。
/// 排队任务可能保有本目标；关闭不清空外部队列，宿主须按外部接口取回并处理剩余持有，
/// 不能依赖最后一个用户句柄释放来自动解除队列与交付实现间的强持有环。
// @trace 100
pub struct WakeTarget<E: ExternalTaskQueue, R: DispatchRecovery> {
    shared: Arc<Shared<E, R>>,
}

// @trace 101
struct Shared<E: ExternalTaskQueue, R: DispatchRecovery> {
    // owner/thread在构造后永不改变；key区别同域先后装配，禁止旧入口命中新装配。
    owner: TaskOwnerId,
    thread: ThreadId,
    key: u64,
    external: E,
    recovery: R,
    closed: AtomicBool,
}

/// 当前线程的受控本地入口；不可跨线程移动（move）或共享。
///
/// guard存续借用owner，防止所属域先于本地入口退出；queue仍由本线程共享持有。
/// 释放guard永久停止该目标的新交付并移除本地入口，不隐式清空队列或关闭共享外部池。
/// 已排队及挂起（Pending）任务的取消（cancellation）与清理由所属域负责。
#[must_use = "本地入口仅在本守卫存续期间有效"]
// @trace 102
pub struct LocalQueueGuard<'owner, E: ExternalTaskQueue, R: DispatchRecovery> {
    target: WakeTarget<E, R>,
    queue: Rc<RefCell<TaskQueue>>,
    owner: &'owner TaskThread,
    local: PhantomData<Rc<()>>,
}

// @trace 103
struct LocalRoute {
    key: u64,
    queue: Weak<RefCell<TaskQueue>>,
}

// 各线程各自管理，不是跨线程共享Rc。弱引用不延长队列存续。
thread_local! {
    static LOCAL_QUEUES: RefCell<HashMap<TaskOwnerId, LocalRoute>> = RefCell::new(HashMap::new());
}
// 只在创建目标时取号；不得回绕复用，不进入逐次唤醒路径。
static NEXT_TARGET_KEY: AtomicU64 = AtomicU64::new(1);

/// 本地入口建立失败，不改变已有入口或原任务（Task）绑定。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
// @trace 104
pub enum BindError {
    /// owner或queue不属于原目标。
    WrongOwner,
    /// 不在原目标的固定线程。
    WrongThread,
    /// 该所属域已有有效本地入口。
    AlreadyBound,
    /// 队列（queue）正在被借用，不等待或制造别名。
    QueueBorrowed,
    /// 原目标已停止新交付。
    Closed,
}

/// 本次本地读取或取出失败，没有取得额外任务持有。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
// @trace 105
pub enum LocalAccessError {
    /// 队列（queue）正在被借用，本次不等待。
    QueueBorrowed,
    /// 该本地入口已关闭。
    Closed,
}

/// 一次外部获取及尾部交接结果；不据此判断整个运行时（runtime）结束。
// @trace 106
pub enum ImportOutcome {
    /// 按外部交付顺序追加的数量；零不代表没有存活任务。
    Imported(usize),
    /// 本次没有可取得任务。
    Empty,
    /// 原发布尚未完成；本方法不额外重试。
    WouldBlock,
    /// 外部同步权或本地访问暂被占用；没有无界等待。
    Busy,
    /// 入口或外部实现明确已关闭。
    Closed,
    /// 已取得的整个原批次未被本地接纳，调用者继续负责，不丢任务。
    Rejected(BatchPushError),
    /// 已取得批次后，本地访问被重入占用；原批次由调用者继续持有。
    LocalBusy(TaskBatch),
}

// @trace 1
impl<E: ExternalTaskQueue, R: DispatchRecovery> WakeTarget<E, R> {
    /// 建立固定目标，接管external和recovery；不接纳任务（Task），不建立本地入口。
    /// 一次共享分配；同一目标的后续克隆不重复分配，不阻塞等待。
    pub fn new(owner: &TaskThread, external: E, recovery: R) -> Self {
        target_new_inner(owner, external, recovery)
    }
}

// @trace 2
impl<E: ExternalTaskQueue, R: DispatchRecovery> Clone for WakeTarget<E, R> {
    /// 克隆（clone）同一目标的透明句柄，O(1)；不要求E或R本身可克隆。
    fn clone(&self) -> Self {
        target_clone_inner(self)
    }
}

// @trace 3
impl<E: ExternalTaskQueue, R: DispatchRecovery> WakeTarget<E, R> {
    /// 取得不可更换的所属域身份，O(1)，不授予原值访问权限。
    pub fn owner_id(&self) -> TaskOwnerId {
        target_owner_inner(self)
    }
}

// @trace 4
impl<E: ExternalTaskQueue, R: DispatchRecovery> WakeTarget<E, R> {
    /// 借用装配的外部接口（interface），存续不超过本句柄；不暴露本地队列。
    pub fn external(&self) -> &E {
        target_external_inner(self)
    }
}

// @trace 5
impl<E: ExternalTaskQueue, R: DispatchRecovery> WakeTarget<E, R> {
    /// 同步停止该目标的新交付，幂等、线程安全（thread-safe）、不等待。
    /// 与关闭交错的已开始交付可以完成；不关闭共享外部池，不等目标任务（Task）退出。
    pub fn close(&self) {
        target_close_inner(self)
    }
}

// @trace 6
impl<E: ExternalTaskQueue, R: DispatchRecovery> WakeTarget<E, R> {
    /// 取得单次原子关闭快照（snapshot），O(1)；旧的已保存值不会自动更新。
    pub fn is_closed(&self) -> bool {
        target_closed_inner(self)
    }
}

// @trace 7
impl<E: ExternalTaskQueue, R: DispatchRecovery> WakeTarget<E, R> {
    /// 在原线程装配默认本地队列，失败不替换已有入口。
    /// queue必须属于owner；临时借用失败立即返回，成功守卫（guard）不能跨线程转交。
    pub fn bind_local<'owner>(
        &self,
        owner: &'owner TaskThread,
        queue: Rc<RefCell<TaskQueue>>,
    ) -> Result<LocalQueueGuard<'owner, E, R>, BindError> {
        target_bind_inner(self, owner, queue)
    }
}

// @trace 8
impl<E: ExternalTaskQueue, R: DispatchRecovery> WakeTarget<E, R> {
    /// 向原目标交付已接纳（admission）的任务（Task），或同域已进入退出处理的清理持有。
    /// 后者必须已有明确终止原因；不隐式接纳（admission）或开始业务轮询（poll）。
    /// 尚未接纳且仍可开始执行的任务（Task）继续拒绝；失败返还原值，不隐式取消。
    /// 本线程默认尾插，跨线程或本地入口暂不可用时走原外部接口；不执行轮询（poll）。
    pub fn try_submit(&self, task: Runnable) -> Result<(), DeliveryError> {
        target_submit_inner(self, task)
    }
}

// Schedule没有错误通道；这段连接明确把正常拒绝交给恢复对象，绝不丢给Waker调用者。
// 清理持有也适用；恢复实现正常返回后须有真实后续通路，不能只释放它并声称已清理。
// @trace 17
impl<E: ExternalTaskQueue, R: DispatchRecovery> Schedule for WakeTarget<E, R> {
    fn schedule(&self, task: Runnable) {
        if let Err(error) = self.try_submit(task) {
            self.shared.recovery.retain(error.task, error.cause);
        }
    }
}

// @trace 9
impl<E: ExternalTaskQueue, R: DispatchRecovery> WakeTarget<E, R> {
    /// 检查目标所属域后取得原任务（Task）的原生唤醒器（Waker）；不重新绑定。
    /// 关闭返回Closed，所属域不符返回WrongOwner；返回值可克隆并跨线程使用。
    pub fn waker_for(&self, task: &ControlHandle) -> Result<Waker, BindError> {
        target_waker_inner(self, task)
    }
}

// @trace 10
impl<'owner, E: ExternalTaskQueue, R: DispatchRecovery> LocalQueueGuard<'owner, E, R> {
    /// 取得同一固定目标的共享句柄；可用于创建任务（Task）时的Schedule。
    pub fn target(&self) -> WakeTarget<E, R> {
        guard_target_inner(self)
    }
}

// @trace 11
impl<'owner, E: ExternalTaskQueue, R: DispatchRecovery> LocalQueueGuard<'owner, E, R> {
    /// 显式本地提交至队首，影响下一次选择；任务须已被同一所属域接纳。
    /// 失败返还原值，不跨线程回退、不抢断当前轮询（poll）。
    pub fn push_front(&mut self, task: Runnable) -> Result<(), DeliveryError> {
        guard_push_front_inner(self, task)
    }
}

// @trace 12
impl<'owner, E: ExternalTaskQueue, R: DispatchRecovery> LocalQueueGuard<'owner, E, R> {
    /// 显式本地提交至队尾，保持默认先后顺序；任务须已被同一所属域接纳。
    /// 失败返还原值，不跨线程回退、不抢断当前轮询（poll）。
    pub fn push_back(&mut self, task: Runnable) -> Result<(), DeliveryError> {
        guard_push_back_inner(self, task)
    }
}

// @trace 13
impl<'owner, E: ExternalTaskQueue, R: DispatchRecovery> LocalQueueGuard<'owner, E, R> {
    /// 从本地队首取回下一份可执行持有；不轮询（poll），不实现事件循环（event loop）。
    /// 临时借用失败返回QueueBorrowed；关闭后仍允许取出既有项供合法退出。
    pub fn pop_front(&mut self) -> Result<Option<Runnable>, LocalAccessError> {
        guard_pop_inner(self)
    }
}

// @trace 14
impl<'owner, E: ExternalTaskQueue, R: DispatchRecovery> LocalQueueGuard<'owner, E, R> {
    /// 查询默认本地队列精确数量；临时借用失败立即返回，不计外部或执行中任务。
    pub fn len(&self) -> Result<usize, LocalAccessError> {
        guard_len_inner(self)
    }
}

// @trace 15
impl<'owner, E: ExternalTaskQueue, R: DispatchRecovery> LocalQueueGuard<'owner, E, R> {
    /// 只获取一次外部当前批次，并保序追加本地队尾；不等待、不内置重试或节拍。
    /// 先取得本地访问条件，失败不取外部批次；后续拒绝完整返还原批次。
    pub fn import_external_once(&mut self) -> ImportOutcome {
        guard_import_inner(self)
    }
}

// @trace 16
impl<E: ExternalTaskQueue, R: DispatchRecovery> Drop for LocalQueueGuard<'_, E, R> {
    /// 关闭本目标并移除属于自己的本地入口；不取消全部任务或清空共享外部队列。
    fn drop(&mut self) {
        guard_drop_inner(self)
    }
}

include!("target_inner.rs");
