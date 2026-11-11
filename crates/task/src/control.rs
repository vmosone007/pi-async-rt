//! 共享控制与单份取消等待（cancellation wait），业务输出独立领取。
use crate::definitions::*;
use crate::task::{CancelObserver, TaskRef};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Waker};

// 控制句柄可克隆/跨线程，普通释放不取消目标，不借出F/结果/元数据。
// cancel只原子提出请求，不能进入全局等待图锁；自行请求使用当前任务身份快速路径。
// CancelWait创建是惰性的，不发请求；首次poll确定实际等待者，非创建者。
// 支持任务根或组合子内的实际等待，同一主体可有多条边；不能把单目标接口误作单出边。
// 有序步骤：身份/自等待/目标事实 -> try_lock图登记与成环检查 -> 唯一目标槽 -> 发请求。
// 初始拒绝不发本份请求，包括Drop也不能补发；图锁忙立即Failed(RegistrationBusy)。
// 已有合法请求不回滚；发现成环只拒绝闭环新操作，不改旧等待方结果。
// 活跃边仅覆盖真正尚无结果且准备Pending的期间；完成/失败/放弃即移除或可靠隔离，
// 不等结果被领取或Wait值最终释放。target原值drop确认后才发布成功，业务输出不取走。
// 不在持图锁时调用目标接口、Waker或用户drop；结束回调先退边，再原子发布和唤醒。
// 同一Wait首次Pending后绑定actual waiter，不支持转交后的身份变更；变更立即失败退边。
// 已完成重复poll返回同一控制分类，不再次发请求；Drop等待只退登记，不撤销既有取消。
// 注册图为可选取消等待路径独有成本；普通poll/wake/同步cancel不扫描图或持图锁。

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
    target: ControlHandle,
    active: AtomicBool,
    // 此两锁只用于取消等待冷路径；普通poll/wake/sync cancel不得访问。
    // 先结束图边和目标准入，再发布结果；不持锁调用Waker或释放外部载荷。
    result: Mutex<Option<CancellationOutcome>>,
    waker: Mutex<Option<Waker>>,
}

// @trace 108
struct WaitRegistry {
    // 同一个实际任务可经组合子等待多个目标，因此按登记key保存所有活动出边。
    // 登记与可达性检查在同一图锁内线性化；未发请求的预登记失败也必须撤回本份。
    edges: HashMap<WaiterId, HashMap<u64, WaiterId>>,
    targets: HashMap<TaskId, u64>,
}

static WAIT_REGISTRY: OnceLock<Mutex<WaitRegistry>> = OnceLock::new();
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

impl CancelObserver for WaitRegistration {
    // @trace 12
    /// 完成本份登记，先退边再发布和唤醒，不修改已发布事实。
    fn complete(&self, outcome: CancellationOutcome) {
        wait_complete_inner(self, outcome)
    }

    // @trace 13
    /// 安全放弃本份等待登记，不给旧合法请求做回滚。
    fn abandon(&self) {
        wait_abandon_inner(self)
    }
}

include!("control_inner.rs");
