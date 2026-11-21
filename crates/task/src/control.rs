//! 共享控制与单份取消等待（cancellation wait），业务输出独立领取。
use crate::definitions::*;
use crate::task::{CancelObserver, TaskRef};
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

// 控制句柄可克隆/跨线程，普通释放不取消目标，不借出F/结果/元数据。
// cancel只原子提出请求；自行请求使用当前任务身份快速路径。
// CancelWait创建是惰性的，不发请求；首次poll确定实际等待者，非创建者。
// 支持任务根或组合子内的实际等待，同一主体可有多条边；不能把单目标接口误作单出边。
// 有序步骤：身份和目标事实 -> 同一图版本检查成环及登记 -> 唯一目标槽 -> 发请求。
// 比较交换发现版本不匹配时返回Failed(RegistrationBusy)。初始拒绝及此后的Drop均不发新请求。
// 已有合法请求不回滚；发现成环只拒绝闭环新操作，不改旧等待方结果。
// 活跃边仅覆盖真正尚无结果且准备Pending的期间；完成/失败/放弃即移除或可靠隔离，
// 不等结果被领取或Wait值最终释放。target原值drop确认后才发布成功，业务输出不取走。
// 结束先退出本份有效关系，再发布结果并交接唤醒。图仅持身份、登记编号和原子结果分类。
// 同一Wait首次Pending后绑定actual waiter，不支持转交后的身份变更；变更立即失败退边。
// 已完成重复poll返回同一控制分类，不再次发请求；Drop等待只退登记，不撤销既有取消。
// 普通poll/wake/同步cancel不读图。

const WAIT_WAKER_IDLE: u8 = 0;
const WAIT_WAKER_REGISTERING: u8 = 1;
const WAIT_WAKER_FINISH_PENDING: u8 = 2;
const WAIT_WAKER_CLOSED: u8 = 3;

const WAIT_RESULT_ACTIVE: u8 = 0;
const WAIT_RESULT_ABANDONED: u8 = 1;
const WAIT_RESULT_CANCELLED: u8 = 2;
const WAIT_RESULT_COMPLETED: u8 = 3;
const WAIT_RESULT_TARGET_ABANDONED: u8 = 4;
const WAIT_RESULT_TARGET_PANICKED: u8 = 5;
const WAIT_RESULT_CLEANUP_UNCONFIRMED: u8 = 6;
const WAIT_RESULT_PUBLISHED: u8 = 1 << 7;

const WAIT_REGISTRY_RECLAIMING: u8 = 1;
const WAIT_REGISTRY_RECHECK: u8 = 1 << 1;

/// 共享且线程安全（thread-safe）的控制句柄；不授予轮询（poll）或结果领取权。
// @trace 100
pub struct ControlHandle {
    pub(crate) task: TaskRef,
}

/// 一份惰性的取消等待异步计算（cancellation wait future），本身不是任务（Task）。
/// 首次实际推进前可以移动（move）；开始等待后实际等待者不能改变。
#[must_use = "创建不发送取消请求；须轮询等待或显式使用cancel"]
// @trace 101
pub struct CancelWait {
    target: ControlHandle,
    external: Option<ExternalWaiter>,
    state: CancelWaitState,
}

/// 已可靠识别的外部等待者凭据；不冒充受管任务（Task）。
/// 只能由外部宿主在可信边界创建，宿主必须如实申报会参与的等待关系。
#[derive(Clone)]
// @trace 102
pub struct ExternalWaiter {
    inner: Arc<ExternalWaiterState>,
}

/// 外部宿主提供的等待关系身份接口；不允许把目标自身伪装为外部等待者。
/// 这是逻辑协议，不授予原值访问或指针恢复能力；违反协议不获得循环检测保证。
// @trace 103
pub trait ExternalWaitSource: Send + Sync + 'static {
    /// 宿主是否仍承接这一身份的退出/放弃责任；必须线程安全且不阻塞。
    fn is_alive(&self) -> bool;
}

// @trace 104
struct ExternalWaiterState {
    id: WaiterId,
    source: Arc<dyn ExternalWaitSource>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
// @trace 105
enum WaiterId {
    Task(TaskId),
    External(u64),
}

// @trace 106
enum CancelWaitState {
    Created,
    Waiting {
        waiter: WaiterId,
        registration: Arc<WaitRegistration>,
    },
    Done(CancellationOutcome),
}

// @trace 107
struct WaitRegistration {
    key: u64,
    waiter: WaiterId,
    target: TaskId,
    // 首次终态比较交换固定原结果。任一终态参与者可按原编号退边，退边后才增加PUBLISHED。
    result: Arc<AtomicU8>,
    waker_state: AtomicU8,
    waker: UnsafeCell<Option<Waker>>,
}

// SAFETY: Waker仅由取得REGISTERING的唯一登记者或从IDLE直接关闭的终态方访问。
// 若登记中收到终态，只有登记者移出原唤醒器。关闭后不会再次登记，也不返回槽内借用。
unsafe impl Sync for WaitRegistration {}

// @trace 108
struct WaitRegistry {
    // current持一份已发布版本的Arc。空指针只用于首次登记前；后续空图也有独立版本。
    // 接手旧引用的一方将它移入retired。
    current: AtomicPtr<WaitRegistrySnapshot>,
    retired: AtomicPtr<WaitRegistrySnapshot>,
    // 增加版本强引用和拼接退役链时计数。计数和两个全局指针均以SeqCst排序。
    accesses: AtomicUsize,
    // 仅回收方需要取得资格。其它操作可登记复查并继续，不依赖回收方推进。
    reclaim_state: AtomicU8,
}

#[derive(Clone, Default)]
struct WaitRelations {
    // 同一实际等待者可有多条边。targets和edges使用同一版本替换。
    edges: HashMap<WaiterId, HashMap<u64, TaskId>>,
    targets: HashMap<TaskId, WaitRelation>,
}

#[derive(Clone)]
struct WaitRelation {
    key: u64,
    // 不持有唤醒器或任务。旧快照可判断这条边已经退出。
    result: Arc<AtomicU8>,
}

struct WaitRegistrySnapshot {
    relations: WaitRelations,
    // 只有处置本版本的全局强引用时才会写入。读取关系的一方不读取此字段。
    retired_next: AtomicPtr<WaitRegistrySnapshot>,
}

static WAIT_REGISTRY: WaitRegistry = WaitRegistry {
    current: AtomicPtr::new(std::ptr::null_mut()),
    retired: AtomicPtr::new(std::ptr::null_mut()),
    accesses: AtomicUsize::new(0),
    reclaim_state: AtomicU8::new(0),
};
static NEXT_WAIT_KEY: AtomicU64 = AtomicU64::new(1);
static NEXT_EXTERNAL_WAITER: AtomicU64 = AtomicU64::new(1);

// @trace 1
impl ControlHandle {
    /// 取得原任务（Task）固定身份。
    pub fn id(&self) -> TaskId {
        control_id_inner(self)
    }
}

// @trace 2
impl ControlHandle {
    /// 无阻塞（nonblocking）地查询基本状态快照（snapshot），不保证保存值自动更新。
    pub fn snapshot(&self) -> TaskSnapshot {
        control_snapshot_inner(self)
    }
}

// @trace 3
impl ControlHandle {
    /// 同步非阻塞（synchronous nonblocking）、线程安全（thread-safe）、幂等地提出取消请求（cancellation request），无返回值。
    /// 不承诺送达/受理或成功；已开始本次poll返回Ready仍保留正常结果。
    pub fn cancel(&self) {
        control_cancel_inner(self)
    }
}

// @trace 4
impl ControlHandle {
    /// 提出原目标后续推进需求，重复任务唤醒（wake-up）可合并；不复活终结任务（Task）。
    pub fn wake(&self) {
        control_wake_inner(self)
    }
}

// @trace 5
impl ControlHandle {
    /// 取得原目标标准唤醒器（Waker），不授予元数据或输出访问。
    pub fn waker(&self) -> Waker {
        control_waker_inner(self)
    }
}

// @trace 6
impl ControlHandle {
    /// 创建惰性取消等待（cancellation wait）；首次poll才检查并发送，结果仅为控制分类。
    /// 目标已有同步请求时允许首份合法等待接续；同目标仅一份持续等待。
    pub fn cancel_and_wait(&self) -> CancelWait {
        control_wait_inner(self)
    }
}

// @trace 7
impl ControlHandle {
    /// 使用可信外部等待者凭据创建同类等待；受管任务（Task）内仍以实际当前身份为准。
    pub fn cancel_and_wait_from(&self, waiter: &ExternalWaiter) -> CancelWait {
        control_external_wait_inner(self, waiter)
    }
}

// @trace 8
impl Clone for ControlHandle {
    /// 增加共享控制持有，不复制任务（Task）或取消等待权限。
    fn clone(&self) -> Self {
        control_clone_inner(self)
    }
}

// @trace 9
impl ExternalWaiter {
    /// 为可信外部宿主建立不复用的等待者身份；不伪造目标任务（Task）身份。
    pub fn new(source: Arc<dyn ExternalWaitSource>) -> Self {
        external_waiter_new_inner(source)
    }
}

// @trace 10
impl Future for CancelWait {
    type Output = CancellationOutcome;
    /// 推进取消等待（cancellation wait）；失败直接Ready，未定才Pending。
    /// 自等待、未知身份、重复等待或成环拒绝不得发本份新请求；成功须原值合法drop完成。
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        cancel_wait_poll_inner(self, cx)
    }
}

// @trace 11
impl Drop for CancelWait {
    /// 放弃等待时退出或隔离本份登记；不撤销已发合法请求，未开始则不补发。
    fn drop(&mut self) {
        cancel_wait_drop_inner(self)
    }
}

// @trace 110
impl CancelWait {
    /// 先使旧边退出循环检测。原目标等待槽撤销或由完成方接手后，才释放登记。
    fn unregister(&self, registration: &WaitRegistration) {
        registration.claim_result(WAIT_RESULT_ABANDONED);
        self.target.task.unregister_cancel(registration.key);
        registration.finish();
    }
}

impl CancelObserver for WaitRegistration {
    // @trace 12
    /// 完成本份登记，先退边再发布和唤醒，不修改已发布事实。
    fn complete(&self, outcome: CancellationOutcome) {
        wait_complete_inner(self, outcome);
        self.finish();
    }

    // @trace 13
    /// 安全放弃本份等待登记，不给旧合法请求做回滚。
    fn abandon(&self) {
        wait_abandon_inner(self);
        self.finish();
    }
}

// @trace 112
impl WaitRegistration {
    /// 读取已发布结果。尚在处理的终态不会作为完成返回。
    fn outcome(&self) -> Option<CancellationOutcome> {
        wait_outcome_inner(self)
    }

    /// 原子比较交换只接受本份登记的第一项终态。
    fn claim_result(&self, result: u8) {
        let _ = self.result.compare_exchange(
            WAIT_RESULT_ACTIVE, result, Ordering::SeqCst, Ordering::SeqCst,
        );
    }

    /// 退出本份图边后发布先前确认的终态，再通过同一唤醒槽接续。
    fn finish(&self) {
        WAIT_REGISTRY.remove(self);
        let should_notify = wait_finish_inner(self);
        if let Some(waker) = self.close_waker() {
            if should_notify {
                waker.wake();
            }
        }
    }

    /// 登记当前唤醒器并复核结果；若接过并发终态，先关闭槽再执行通知。
    fn register_waker(&self, waker: &Waker) -> Option<CancellationOutcome> {
        if let Some(outcome) = self.outcome() {
            return Some(outcome);
        }
        let (previous, current) = wait_register_waker_inner(self, waker.clone());
        if let Some(current) = current {
            if self.outcome().is_some() {
                current.wake();
            }
        }
        // 旧唤醒器的析构可重入。归还权限并执行析构后，再复查本次结果。
        drop(previous);
        self.outcome()
    }

    /// 处理终态唤醒器。并发登记时将移出责任交给实际登记者。
    fn close_waker(&self) -> Option<Waker> {
        wait_close_waker_inner(self)
    }
}

// @trace 111
impl WaitRegistry {
    /// 按同一版本检查唯一目标和循环，只在该版本仍为当前时登记。
    fn insert(&self, registration: &WaitRegistration) -> Result<(), CancellationError> {
        wait_registry_insert_inner(self, registration)
    }

    /// 仅移除匹配该登记编号的关系。新版本复制剩余关系，遇到竞争则重试。
    fn remove(&self, registration: &WaitRegistration) {
        wait_registry_remove_inner(self, registration)
    }

    /// 只在取得强持有的窄区间保留访问计数。之后仅延长本版本寿命。
    fn snapshot(&self) -> Option<Arc<WaitRegistrySnapshot>> {
        self.begin_access();
        // SAFETY: 整个读取和增加强引用的区间已登记访问。
        let snapshot = unsafe { wait_registry_snapshot_inner(self) };
        if self.end_access() {
            self.reclaim();
        }
        snapshot
    }

    /// 以保活的原版本作为比较对象，并转交被替换的强持有。
    fn replace(&self, current: Option<&Arc<WaitRegistrySnapshot>>, relations: WaitRelations) -> bool {
        let Ok(previous) = wait_registry_replace_inner(self, current, relations) else {
            return false;
        };
        if !previous.is_null() {
            // SAFETY: 成功的比较交换唯一接手原全局强引用，且链尚无其它成员。
            if unsafe { self.push_retired(previous, previous) } {
                self.reclaim();
            }
        }
        true
    }

    /// 登记短时访问。达到不可实现的访问容量时保持计数不回绕。
    fn begin_access(&self) {
        if self.accesses.fetch_add(1, Ordering::SeqCst) >= isize::MAX as usize {
            std::process::abort();
        }
    }

    /// 归还访问计数，返回是否由本次负责复查可回收对象。
    fn end_access(&self) -> bool {
        self.accesses.fetch_sub(1, Ordering::SeqCst) == 1
    }

    /// 将独占摘下的整条链或刚替换的单个版本交给等待回收的链。
    ///
    /// SAFETY: head到tail由调用者独占，均代表尚未释放的全局强持有。
    unsafe fn push_retired(&self, head: *mut WaitRegistrySnapshot, tail: *mut WaitRegistrySnapshot) -> bool {
        // SAFETY: 调用方移交同一份强持有和链尾写权限。
        unsafe { wait_registry_push_retired_inner(self, head, tail) }
    }

    /// 摘下退役版本，待没有短时访问即可释放。期间新图版本仍可正常替换。
    fn reclaim(&self) {
        if self.retired.load(Ordering::SeqCst).is_null() {
            return;
        }
        self.reclaim_state.fetch_or(WAIT_REGISTRY_RECHECK, Ordering::SeqCst);
        if self.reclaim_state.compare_exchange(
            WAIT_REGISTRY_RECHECK, WAIT_REGISTRY_RECLAIMING, Ordering::SeqCst, Ordering::SeqCst,
        ).is_ok() {
            wait_registry_reclaim_inner(self);
        }
    }
}

// 等唤醒判定统一归TaskRef；共享控制只借用已有持有，不复制任务（Task）或执行权限。
// 本入口不以TaskSnapshot::execution的Idle单项代替判定，也不维护第二份状态（state）。
// @trace 109
impl ControlHandle {
    /// 查询原任务（Task）是否仍在等待一次新的任务唤醒（wake-up）。
    ///
    /// 返回true表示原根异步计算（root Future）已经返回挂起（Pending）、完成交接，
    /// 且本次取样时没有执行访问、待处理唤醒（wake-up）、已发出的可执行持有、取消（cancellation）或退出。
    /// 正在轮询（poll）、正在交接、已唤醒但尚未入队（enqueue）及已经结束均返回false；
    /// false本身不能证明任务（Task）已结束或已经入队（enqueue）。
    ///
    /// 线程安全（thread-safe）、同步非阻塞（synchronous nonblocking）。只读取一次现有状态，
    /// 时间与额外空间均为O(1)，不分配、不推进或唤醒（wake-up）任务（Task）。
    /// 返回的是取样结果，之后可能立即失效；与其它独立查询不构成同一时刻快照（snapshot）。
    /// 不判断目标属于哪个运行时（runtime）或是否已被接纳（admission），不据此授予执行权限。
    pub fn is_waiting_for_wake(&self) -> bool {
        self.task.is_waiting_for_wake()
    }
}

include!("control_inner.rs");
