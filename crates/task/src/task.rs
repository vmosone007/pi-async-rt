//! 单个任务（Task）的持有、访问、唤醒协调与合法清理。
//!
//! 本模块不选择下一任务（Task），不实现执行器（executor）或业务事件循环。
//! 本模块通过以下接口（API）管理实际任务（Task）的访问与生命周期（lifecycle）。
use crate::definitions::*;
use pi_result::RawResult as Result;
use intrusive_collections::LinkedListLink;
use std::any::TypeId;
use std::cell::{Cell, RefCell, UnsafeCell};
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize};
use std::sync::Arc;
use std::task::{RawWakerVTable, Waker};
use std::thread::ThreadId;

// 存储方案：TaskStorage<F,S,M,L>一次分配，原值/结果复用Payload区域。
// L=()使普通任务不承担链接成本；L=QueueEntry时内嵌本地链接和唯一队列持有槽。
// 原值固定后只能原址访问和析构；先合法drop F，再原址写输出，不移动存活的F。
// 与Header/S/M处于同一块分配不等于共享相同生命周期；F须按时清理，不等最后引用。
// 最后引用只在原值/非Send结果均已合法处理、链接脱离且队列持有归还后回收存储。
// 大F的空间可能被晚到Waker保留；这是同块分配的存储代价，不宣称实测更快。
//
// 状态字低15位：访问情况[0..=2]、首次poll已开始[3]、待处理wake[4]、
// 唯一可执行持有已发出[5]、取消已记录[6]、接纳[7]、结束原因[8..=10]、
// 输出存在[11]、输出已取走[12]、结果领取意愿[13]、取消等待意愿[14]；高位为强引用。
// 引用溢出必须中止，不能回绕；状态描述查询不公开引用计数。首版事实发布用SeqCst。
// 状态0 Idle由started位区分准备/已挂起；1 Running和2 PendingHandoff均须started。
// 3 Exiting、4 FutureDropped、5 Ended、6 CleanupUnconfirmed不可回到可轮询状态。
// 访问值7、结束原因6/7无效；正常推进原因必须None，退出必须具有明确原因。
// FutureDropped/Ended才给FutureDropStatus::Confirmed；CleanupUnconfirmed永不给成功确认。
// Ready与取消竞争：已经开始的本次poll返回Ready仍正常完成；Pending才落实后续停止。
// 有输出与已取走不能同时成立；未合法drop F前不能把同一区域报告为已存输出。
// Running时不发第二份可执行持有，新的wake只记需求；Handoff完成后唯一交出Runnable。
// 独立等待槽状态为0空闲/1登记中/2通知中/3登记中且有通知，只有持有相应权限才访问value。
// 所有其余组合须由上述条件及唯一持有约束拒绝，不作为预留合法状态。
//
// 关键调用链：Runnable::run -> TaskRef::run_once -> VTABLE.run -> storage_run -> storage_run_inner。
// 原生Waker四入口 -> TaskRef计数/请求 -> VTABLE.schedule -> S::schedule(Runnable)。
// poll Pending -> 保存原位/结束访问 -> 承接新需求 -> 外部Schedule；不递归再poll。
// F的轮询（poll）捕获可展开恐慌（unwinding panic）；独立入队（enqueue）、
// 钩子（hook）、析构（drop）等默认不另捕获。
// 唯一额外边界是所属域析构（owner-domain drop）结束剩余责任，详见下方局部设计；
// 显式close和其它独立入口不因此增加捕获，退出守卫（guard）不得虚报清理成功。
// 任何外部操作前恢复状态/持有，不持等待槽写权限或registry可变借用调用Waker/用户drop。
//
// 内部设计，不进入API docs：放弃执行持有与普通强引用（strong reference）释放分开。
// Runnable::drop -> TaskRef::abandon_runnable；只有持本份唯一执行责任者能调用。
// 首次放弃以状态字的原子转换（atomic transition）确立退出：尚未接纳且从未开始
// poll为Abandoned，否则无既定原因时为Cancelled并记取消；既定原因始终保留。
// 已记录取消但尚未确立原因，不抢占初始放弃的Abandoned；不撤回该取消记录。
// 合法admit/run与同一Runnable的Drop由借用/移动互斥，不制造额外绑定中间状态。
// 其它TaskRef的wake/cancel可并发，必须复核退出事实，不得再授予业务poll。
//
// 从未绑定的Send创建可用VTABLE.drop_future(None)；已绑定者即使F:Send也只交原线程。
// 已绑定首次放弃：先保住清理Runnable的新强持有，在同一次状态决定中置Exiting、
// 撤业务wake并把原issued责任交给这份新值，再调用VTABLE.schedule；不是新增任务、
// 不重新分配F、不设置ADMITTED/STARTED。原Runnable字段随后自动归还旧TaskRef一次。
// Exiting的清理Runnable再次Drop只归还issued，不再次schedule；原线程登记继续保活。
// 调度实现正常返回必须有实际接管/重试/原线程退出通路，只有保活不等于完成清理。
// owner.close清理登记是兜底，不把WakeTarget关闭等同整个owner关闭；不要求新公开出口。
//
// 关键保护顺序：先成立所属登记和强持有 -> TaskActivityGuard -> 取得单任务访问
// -> RunGuard -> 访问原值；合法drop F另外由FutureDropGuard防重复，后者展开时先标
// CleanupUnconfirmed，RunGuard不能覆盖。RunGuard先恢复任务事实，活动计数再归还。
// 只在普通run正常最外层收尾且closing时接续一次close；任何守卫Drop都不调用close、
// Schedule、observer或用户drop，避免异常展开（unwinding）和关闭重入再次执行用户代码。
// 非poll独立panic仍向外传播：已接管执行且原值完整、无原因时转Exiting/Cancelled，
// 记录取消并停后续poll；保留已有原因及新Runnable的真实持有，不伪报清理确认。
//
// 所属域析构（owner-domain drop）的局部设计：
// TaskThread::drop -> thread_drop_inner：先核原线程（owner thread）/active=0，
// 记录进入时是否已展开（unwinding），发布closing，以活动守卫（TaskActivityGuard）覆盖处理。
// 不能直接依赖首错就返回的close遍历。登记（registration）集合及其它未处理TaskRef须在
// 逐项捕获（catch_unwind）边界之外保活；不先丢全部登记（registrations）。
// 集合字段（field）自动析构（Drop）不能代替尚活本地异步计算（Future）的原线程清理；
// 外部调用（call）前退出RefCell借用（borrow）。每项沿既有排他访问（exclusive access）
// 规则停止后续轮询（poll）；已有原因不改，FutureDropped/Ended/CleanupUnconfirmed不再析构（drop）F。
// 原址析构（drop）F仍由FutureDropGuard保护；守卫（guard）只记CleanupUnconfirmed，
// 不通知（notify）、不重试（retry）。捕获（catch）返回后由普通控制流（control flow）
// 读取仍保活的头部（Header）事实，处理失败等待的交接（handoff），再释放本项登记（registration）
// 及局部持有；这些释放（drop）若触发用户代码，同样属于本次所属域析构（owner-domain drop）边界。
// 原值清理失败不写失效Payload，取消等待（cancellation wait）返回Failed(CleanupUnconfirmed)，
// 结果等待返回既有TaskError::CleanupUnconfirmed。先隔离本库旧登记（registration），
// 恢复槽写权限（slot write permission），再调用接收者或唤醒器（Waker）。
// NotifySlot由1原子切换（atomic transition）为3时，由登记者继续，不等待对端，
// 也不再访问F或消失的TaskThread。其它线程接手后发生恐慌（panic），按其原入口传播，
// 不计入当前所属域析构（owner-domain drop）同步调用（synchronous call）范围内的首错或二错。
// 外部通知（notification）发生恐慌（panic）不代表已经送达，
// 不重试（retry）该回调（callback）；真实Confirmed/输出（output）不回退。
// 进入时无外层展开（outer unwinding）且无暂存错误，独占保存首次原恐慌载荷（panic payload），
// 正常控制流（control flow）继续其余项。已暂存首错后再错，或进入时已有外层展开（outer unwinding）
// 后发生新错误，显式中止（abort）；这是异常策略，不是说thread::panicking()==true时都不能捕获（catch）。
// 无新错误则处理全部可处理责任，确认无尚活且未处理的登记（registration），归还活动计数（active count）；
// 无首错返回，有首错用resume_unwind转交原恐慌载荷（panic payload）一次，
// 不克隆（clone）/格式化（format）/丢弃（drop），不用panic!触发第二次恐慌钩子（panic hook）。
// panic=abort、钩子（hook）自身恐慌（panic）、原值字段（field）清理中再次恐慌（panic）
// 跨越编译器（compiler）清理路径（cleanup path）等仍可能直接中止（abort）；
// 不承诺其后清理或通知（notification）。用户代码不返回仍可阻止退出，无法保证何时处理完。
// 最后引用（reference）释放（drop）时的检查不放宽；
// CleanupUnconfirmed保持失败，不进入要求Confirmed的TaskPhase::Ended。
// 不新增头部（Header）字段（field）/公开接口（API）/正常轮询（poll）或唤醒（wake-up）检查，
// 不把处理其余项放入三个守卫（guards）的析构（Drop）。

/// 接收唯一可执行持有的外部安排接口（interface）。
///
/// 正常返回后实现者负责已接纳、保留重试或合法退出；不能丢弃可恢复拒绝的责任。
/// 不得在此接口直接轮询（poll）；必须保持原目标，支持并发调用，不等待队列容量。
/// 该非阻塞约束适用于实现者及其调用链；不满足的外部实现不能接入同步取消路径。
/// 也接收只需退出清理的Runnable，包括已绑定但尚未接纳（admission）的任务（Task）。
/// 此类交付不能隐式接纳或轮询（poll）；正常返回须保留实际重试或原线程清理通路。
/// 不在本次调用内执行原异步计算（Future）的清理；清理由原线程的后续执行或关闭负责。
// @trace 100
pub trait Schedule: Send + Sync + 'static {
    /// 接管本份Runnable；不创建第二任务（Task），不重复交付同一持有。
    fn schedule(&self, task: crate::runnable::Runnable);
}

/// 完成或放弃取消等待（cancellation wait）登记的外部窄接口。
///
/// 实现者先移除实际等待关系，再发布结果并通知；不得回返访问原异步计算（Future）。
// @trace 101
pub trait CancelObserver: Send + Sync + 'static {
    /// 结果只发布一次；失败不得冒充原值清理已成功。
    fn complete(&self, outcome: CancellationOutcome);
    /// 移除本份登记和通知责任；不撤销此前合法取消请求（cancellation request）。
    fn abandon(&self);
}

/// 可选观测（observation）接收者；由observe特性启用，默认关闭。
/// 回调不得破坏状态、重入原值或阻塞；panic沿所在调用边界传播，不默认捕获。
#[cfg(feature = "observe")]
// @trace 102
pub trait TaskObserver: Send + Sync + 'static {
    /// 使用原任务（Task）不复用的身份报告事实，不泄露原值或业务数据。
    fn event(&self, task: TaskId, event: TaskEvent);
}

/// 启用观测（observation）后的基本事实；开始回收不等于已经释放。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
// @trace 103
pub enum TaskEvent {
    /// 创建成功。
    Created,
    /// 执行器（executor）已接纳。
    Admitted,
    /// 一次合法轮询（poll）开始。
    PollBegin,
    /// 本次返回挂起（Pending）。
    PollPending,
    /// 本次正常完成就绪（Ready）。
    PollReady,
    /// 捕获本次轮询（poll）恐慌（panic）。
    PollPanicked,
    /// 目标已记录取消请求（cancellation request）。
    CancelRecorded,
    /// 当前无轮询（poll）且永久禁止后续轮询（poll）。
    PollingStopped,
    /// 原异步计算（Future）合法析构（drop）完成。
    FutureDropped,
    /// 剩余任务存储开始回收。
    ReclaimStarted,
    /// 增加一份唤醒器（Waker）持有。
    WakerClone,
    /// 消费本份唤醒器（Waker）并提出需求。
    WakerWake,
    /// 保留本份唤醒器（Waker）并提出需求。
    WakerWakeByRef,
    /// 释放本份唤醒器（Waker）。
    WakerDrop,
}

/// 创建选项；不代表已经提交（submission）或接纳（admission）。
// @trace 104
pub struct TaskOptions {
    /// Plain不分配链接空间；IntrusiveLocal可进入默认TaskQueue。
    pub queue: QueueSupport,
    /// 允许宿主钩子在恐慌（panic）起点采集本次轮询记录；本库不安装全局钩子。
    pub capture_panic_backtrace: bool,
    /// 可选事件接收者；编译时关闭observe则没有此字段和热路径事件调用。
    #[cfg(feature = "observe")]
    pub observer: Option<Arc<dyn TaskObserver>>,
}

/// 线程所属域；不是执行器（executor），不包含任务（Task）选择或轮询循环。
///
/// 只能在创建线程使用。活跃登记保住本地原值，不以外部Waker存在作为关停清理依据。
/// 关闭先禁后续poll，再在合法线程清理；不能从正在进行的poll中间切断。
// @trace 105
pub struct TaskThread {
    id: TaskOwnerId,
    thread: ThreadId,
    closing: Cell<bool>,
    // 覆盖原值访问、清理及必要收尾，不仅覆盖F.poll；只由本线程成对维护。
    // closing && active>0时关闭请求不重入遍历；不能据active归零自动递归close。
    active: Cell<usize>,
    // 本地强根只保住仍需本域负责的原值/清理，不用Waker计数代替关停枚举。
    // HashMap可能在创建/登记时扩容；普通已登记任务poll不应重新插入或分配。
    tasks: RefCell<HashMap<TaskId, TaskRef>>,
    local: PhantomData<Rc<()>>,
}

/// 唯一接纳登记；由执行组织保留，释放时提出取消而不重复析构（drop）。
/// 当前域的活跃登记另保证线程退出前完成本地原值清理；本值不可跨线程转移。
// @trace 106
pub struct TaskRegistration {
    task: TaskRef,
    owner: TaskOwnerId,
    local: PhantomData<Rc<()>>,
}

/// 嵌入任务存储（task allocation）的本地链节点；不单独堆分配。
///
/// 队列持有的Runnable暂存于ticket；这是一份有明确进出责任的自引用持有，
/// 出队或队列析构（drop）必须取回它。普通控制句柄不能访问此槽或链接。
#[repr(C)]
// @trace 107
pub struct QueueEntry {
    link: LinkedListLink,
    ticket: UnsafeCell<Option<crate::runnable::Runnable>>,
}

// @trace 108
pub(crate) struct NotifySlot<T> {
    // 四种交接状态避免登记与完成各自误认为另一方会通知；不是供外部自旋的锁。
    state: AtomicU8,
    // 只有取得相应登记/通知权限的路径可取放；克隆/释放外部Waker必须在权限外执行。
    value: UnsafeCell<Option<T>>,
}

#[repr(C)]
// @trace 109
pub(crate) struct Header {
    state: AtomicUsize,
    id: TaskId,
    owner: AtomicU64,
    vtable: &'static TaskVTable,
    // 唯一JoinHandle可以更换自己的Waker，完成方负责与其交接。
    result_waiter: NotifySlot<Waker>,
    // 终止或撤销时先取走并隔离旧登记，再回调；不能遗留observer->target自持有环。
    cancel_waiter: NotifySlot<Arc<dyn CancelObserver>>,
    // 不复用的登记号阻止旧撤销清除新等待者；0表示没有当前登记。
    cancel_wait_key: AtomicU64,
    // 只在外层panic hook调用本库采集入口时读取，原值访问期间保持不变。
    capture_panic_backtrace: bool,
    // 关闭observe时字段完全不存在；原生唤醒器可经头部记录原身份而不延长F存续。
    #[cfg(feature = "observe")]
    observer: Option<Arc<dyn TaskObserver>>,
}

// @trace 110
pub(crate) struct TaskVTable {
    run: unsafe fn(NonNull<Header>, &TaskThread) -> RunOutcome,
    // None仅用于从未绑定且由Send创建入口建立的原值；绑定后必须Some合法所属域。
    drop_future: unsafe fn(NonNull<Header>, Option<&TaskThread>),
    destroy: unsafe fn(NonNull<Header>),
    schedule: unsafe fn(NonNull<Header>, crate::runnable::Runnable),
    take_result: unsafe fn(NonNull<Header>, NonNull<()>),
    metadata: unsafe fn(NonNull<Header>, TypeId) -> Option<NonNull<()>>,
    queue_entry: unsafe fn(NonNull<Header>) -> Option<NonNull<QueueEntry>>,
}

// @trace 111
pub(crate) enum Payload<F: Future> {
    Future(F),
    Output(Result<F::Output, TaskError>),
    Empty,
}

#[repr(C)]
// @trace 112
pub(crate) struct TaskStorage<F: Future, S, M, L> {
    header: Header,
    schedule: S,
    // M:Send+Sync且创建后不替换；合法共享读取结束后才随控制存储清理。
    metadata: ManuallyDrop<M>,
    links: L,
    // enum区分活F/结果/空；ManuallyDrop阻止panic或远端末次持有隐式重复drop。
    // F存活时不得移动enum来切换变体；必须原址drop F后才能写下一种载荷。
    payload: UnsafeCell<ManuallyDrop<Payload<F>>>,
}

// 仅在本库内部使用的强持有；不向外导出Header或指针恢复能力。
// @trace 113
pub(crate) struct TaskRef {
    pointer: NonNull<Header>,
}

// SAFETY: 共享持有只访问原子头部；本地F和非Send输出始终由TaskThread或合法结果持有清理。
// 增减计数不搬移载荷，最后释放必须检查载荷已清理和链接已脱离。
unsafe impl Send for TaskRef {}
// SAFETY: 所有可变访问经状态/等待槽独占协议，不经共享引用直接访问F。
unsafe impl Sync for TaskRef {}

#[derive(Clone, Copy)]
// @trace 114
struct CurrentTask {
    pointer: NonNull<Header>,
    id: TaskId,
}

thread_local! {
    // 只保存当前同步访问期间借用的身份；不建立额外强引用，不保存队列或执行器。
    static CURRENT_TASK: Cell<Option<CurrentTask>> = const { Cell::new(None) };
}

// @trace 115
struct CurrentGuard {
    previous: Option<CurrentTask>,
    previous_backtraces: Option<(TaskId, Vec<std::backtrace::Backtrace>)>,
}

// 内部设计，不进入API docs：三个守卫（guard）职责分开，不增加Header字段或堆分配。
// 活动守卫只管理owner的嵌套活动计数；借用保证owner先于本次访问析构不了。
// 进入前调用者已检查原线程/关闭准入；作用域覆盖原值访问及交接，析构不启动新清理。
// @trace 121
struct TaskActivityGuard<'owner> {
    owner: &'owner TaskThread,
}

// 取得执行责任后立即建立，task借用保住原强持有；armed在全部后继责任成立后撤销。
// 未撤销而退出只恢复状态并停后续poll，不吞掉panic、不调用外部代码。
// issued若已对应新Runnable，不得清除或代减其引用；新值尚未成立时不许发布issued。
// @trace 122
struct RunGuard<'task> {
    task: &'task TaskRef,
    armed: bool,
}

// 只在原址drop F前建立；header借用必须由调用者的存活强持有覆盖。
// armed为true时退出表示原值清理未确认，不能重读/重drop可能已部分析构的F。
// 正常drop F后、任何通知/回调前撤销；只有正常完成才能发布Confirmed。
// @trace 123
struct FutureDropGuard<'header> {
    header: &'header Header,
    armed: bool,
}

thread_local! {
    // 只存本次poll的起点记录；进入/退出恢复父层，不把旧记录交给下一次poll。
    static CURRENT_PANIC_BACKTRACES: RefCell<Option<(TaskId, Vec<std::backtrace::Backtrace>)>> = const { RefCell::new(None) };
}

// 内部身份只在创建时分配，不在轮询/唤醒路径取号；0无效，耗尽必须停止而不回绕。
static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_OWNER_ID: AtomicU64 = AtomicU64::new(1);

// 状态字编码是具体访问协议，不改变公开枚举的布局或判别值。
const ACCESS_MASK: usize = 0b111;
const ACCESS_IDLE: usize = 0;
const ACCESS_RUNNING: usize = 1;
const ACCESS_HANDOFF: usize = 2;
const ACCESS_EXITING: usize = 3;
const ACCESS_DROPPED: usize = 4;
const ACCESS_ENDED: usize = 5;
const ACCESS_UNCONFIRMED: usize = 6;
const STARTED: usize = 1 << 3;
const WAKE_PENDING: usize = 1 << 4;
const RUNNABLE_ISSUED: usize = 1 << 5;
const CANCEL_RECORDED: usize = 1 << 6;
const ADMITTED: usize = 1 << 7;
const REASON_SHIFT: usize = 8;
const REASON_MASK: usize = 0b111 << REASON_SHIFT;
const REASON_NONE: usize = 0;
const REASON_COMPLETED: usize = 1;
const REASON_CANCELLED: usize = 2;
const REASON_PANICKED: usize = 3;
const REASON_ABANDONED: usize = 4;
const REASON_CLEANUP_FAILED: usize = 5;
const OUTPUT_PRESENT: usize = 1 << 11;
const OUTPUT_TAKEN: usize = 1 << 12;
const RESULT_INTEREST: usize = 1 << 13;
const CANCEL_WAIT_INTEREST: usize = 1 << 14;
const REF_SHIFT: usize = 15;
const REF_ONE: usize = 1 << REF_SHIFT;
const REF_MAX: usize = usize::MAX >> REF_SHIFT;

// @trace 1
impl TaskOptions {
    /// 构造无链接、无观测的默认选项；不创建任务（Task）。
    pub fn plain() -> Self {
        options_plain_inner()
    }
}

// @trace 2
impl TaskOptions {
    /// 构造可放入默认TaskQueue的选项，默认关闭可选观测（observation）与堆栈采集（backtrace capture）。
    pub fn intrusive() -> Self {
        options_intrusive_inner()
    }
}

// @trace 3
impl TaskThread {
    /// 建立当前线程的所属域；身份不复用，耗尽时中止。域本身不启动线程或轮询（poll）。
    pub fn new() -> Self {
        thread_new_inner()
    }
}

// @trace 4
impl TaskThread {
    /// 取得固定所属域身份；O(1)，不改变登记或执行状态。
    pub fn id(&self) -> TaskOwnerId {
        thread_id_inner(self)
    }
}

// @trace 5
impl TaskThread {
    /// 在本线程查询是否停止接纳新任务（Task），O(1)；不代表原值均已析构（drop）。
    pub fn is_closed(&self) -> bool {
        thread_closed_inner(self)
    }
}

// @trace 6
impl TaskThread {
    /// 查询本域仍承担原值清理责任的任务数，不等于队列（queue）长度。
    pub fn live_tasks(&self) -> usize {
        thread_live_inner(self)
    }
}

// @trace 7
impl TaskThread {
    /// 停止本域接纳并请求全部登记任务（Task）退出；幂等，不中断已开始轮询（poll）。
    /// 当前无访问时在本线程清理，存在访问则由退出交接继续负责；不承诺完成时限。
    /// 独立用户析构（drop）或通知恐慌（panic）立即向调用者传播，剩余登记仍由本域持有。
    /// 本方法不采用所属域析构（owner-domain drop）的暂存首错规则。
    pub fn close(&self) {
        thread_close_inner(self)
    }
}

// @trace 8
impl Drop for TaskThread {
    /// 结束线程所属责任；不等待其它线程释放观察句柄。
    /// 进入前无异常展开（unwinding）时，首次可捕获的清理或通知恐慌（panic）暂存；
    /// 在原线程处理其它原值及本库等待责任后，继续传播原恐慌载荷（panic payload）。
    /// 失败原异步计算（Future）不重试（retry）、不报告成功清理；已确认事实不因通知（notification）失败回退。
    /// 已暂存首错后再次恐慌（panic），或进入前已展开而又发生新错误时，中止进程（abort）。
    /// 恐慌中止模式（panic=abort）及无法越过清理边界的再次恐慌（panic）不保证捕获。
    fn drop(&mut self) {
        thread_drop_inner(self)
    }
}

// @trace 9
impl Drop for TaskRegistration {
    /// 放弃本份接纳登记时请求退出；不撤销其它请求或重复释放原值。
    fn drop(&mut self) {
        registration_drop_inner(self)
    }
}

// @trace 10
impl TaskRef {
    // 取得原任务（Task）固定身份。
    pub(crate) fn id(&self) -> TaskId {
        ref_id_inner(self)
    }
}

// @trace 11
impl TaskRef {
    // 线程安全（thread-safe）地取得基本描述快照（snapshot）；不报告物理队列长度。
    pub(crate) fn snapshot(&self) -> TaskSnapshot {
        ref_snapshot_inner(self)
    }
}

// @trace 12
impl TaskRef {
    // 同步非阻塞（synchronous nonblocking）地提出取消请求（cancellation request），无返回值；不承诺送达或成功。
    pub(crate) fn request_cancel(&self) {
        ref_cancel_inner(self)
    }
}

// @trace 13
impl TaskRef {
    // 取得标准唤醒器（Waker），共享同一任务（Task）控制存储；不另分配包装。
    pub(crate) fn waker(&self) -> Waker {
        ref_waker_inner(self)
    }
}

// @trace 14
impl TaskRef {
    // 提出同一目标的后续推进需求；重复唤醒（wake-up）可合并。
    pub(crate) fn wake(&self) {
        ref_wake_inner(self)
    }
}

// @trace 15
impl TaskRef {
    // 显式建立所属域接纳（admission），不自动入队（enqueue）或轮询（poll）；失败保留原持有。
    pub(crate) fn admit(&self, owner: &TaskThread) -> Result<TaskRegistration, AdmissionError> {
        ref_admit_inner(self, owner)
    }
}

// @trace 16
impl TaskRef {
    // 在合法所属线程取得一次访问权，至多轮询（poll）原异步计算（Future）一次；允许合法直接执行。
    pub(crate) fn run_once(&self, owner: &TaskThread) -> Result<RunOutcome, RunErrorKind> {
        ref_run_inner(self, owner)
    }
}

// @trace 120
impl TaskRef {
    // 仅Runnable::drop调用，归还本份唯一执行责任；不消费或手动drop当前TaskRef。
    // 允许任意线程调用，但已绑定原值绝不在此处析构；普通TaskRef::drop不调用本方法。
    // 初始放弃/既定原因/退出重复分别处理；不访问队列链接、不调用业务poll。
    // 未绑定Send原值可能在本次调用析构，用户drop的时间和panic不能承诺为无阻塞。
    pub(crate) fn abandon_runnable(&self) {
        ref_abandon_runnable_inner(self)
    }
}

// @trace 124
impl<'owner> TaskActivityGuard<'owner> {
    // 已核原线程及当前准入；增加活动计数，溢出中止，不分配或执行外部代码。
    fn enter(owner: &'owner TaskThread) -> Self {
        task_activity_enter_inner(owner)
    }
}

// @trace 125
impl Drop for TaskActivityGuard<'_> {
    // 恰归还一次本层活动计数，不遍历登记，不在展开或close自身收尾中再调用close。
    fn drop(&mut self) {
        task_activity_drop_inner(self)
    }
}

// @trace 126
impl Drop for RunGuard<'_> {
    // 仅armed时恢复本次未完成执行；不接管新Runnable，不覆盖清理未确认事实。
    fn drop(&mut self) {
        run_guard_drop_inner(self)
    }
}

// @trace 127
impl Drop for FutureDropGuard<'_> {
    // 仅armed时记录清理未确认；不重做析构、不通知、不改变既定原因。
    fn drop(&mut self) {
        future_drop_guard_drop_inner(self)
    }
}

// @trace 17
impl TaskRef {
    // 按原始类型（type）借用附加数据；类型不匹配返回None，不读取输出（output）或原异步计算（Future）。
    pub(crate) fn metadata<M: Send + Sync + 'static>(&self) -> Option<&M> {
        ref_metadata_inner(self)
    }
}

// @trace 18
impl TaskRef {
    // 登记唯一结果等待者的唤醒器（Waker），并与完成发布可靠交接。
    // 完成复核包括OUTPUT_PRESENT或ACCESS_UNCONFIRMED；登记前后及调用方最后返回
    // 挂起（Pending）前都不能遗漏失败事实。失败不伪造OUTPUT_PRESENT，不读取失效Payload。
    // 复用槽0/1/2/3：完成者0->2取通知（notification），1->3由登记者接续；先恢复权限再调用外部唤醒器（Waker）。
    pub(crate) fn register_result(&self, waker: &Waker) {
        ref_register_result_inner(self, waker)
    }
}

// @trace 19
impl TaskRef {
    // 放弃结果领取权，不自动取消任务（Task）。
    pub(crate) fn abandon_result(&self) {
        ref_abandon_result_inner(self)
    }
}

// @trace 20
impl TaskRef {
    // 尝试唯一领取已就绪结果。
    // # Safety
    // T必须是原F::Output，destination须可写且对齐，调用者持唯一领取权并满足结果线程限制。
    pub(crate) unsafe fn take_result<T>(&self, destination: NonNull<Result<T, TaskError>>) -> bool {
        // SAFETY: 本入口的公开前提原样传给局部实现，不扩大访问权限。
        unsafe { ref_take_inner(self, destination) }
    }
}

// @trace 21
impl TaskRef {
    // 登记至多一份持续取消等待（cancellation wait），拒绝时不发送本份新请求。
    pub(crate) fn register_cancel(
        &self,
        key: u64,
        observer: Arc<dyn CancelObserver>,
    ) -> Result<(), CancellationError> {
        ref_register_cancel_inner(self, key, observer)
    }
}

// @trace 22
impl TaskRef {
    // 退出本份等待登记；旧key不能清除新等待者。
    pub(crate) fn unregister_cancel(&self, key: u64) {
        ref_unregister_cancel_inner(self, key)
    }
}

// @trace 23
impl TaskRef {
    // 查询已确定控制结果；未达到原值合法析构（drop）确认不返回成功。
    pub(crate) fn cancellation_outcome(&self) -> Option<CancellationOutcome> {
        ref_cancel_outcome_inner(self)
    }
}

// @trace 24
impl TaskRef {
    // 取得本任务（Task）可选的内嵌链节点；只供受限队列适配使用。
    pub(crate) fn queue_entry(&self) -> Option<NonNull<QueueEntry>> {
        ref_queue_entry_inner(self)
    }
}

// @trace 25
impl Clone for TaskRef {
    // 增加一份控制存储强持有。
    fn clone(&self) -> Self {
        ref_clone_inner(self)
    }
}

// @trace 26
impl Drop for TaskRef {
    // 释放本份强持有；不把普通释放解释成取消。
    fn drop(&mut self) {
        ref_drop_inner(self)
    }
}

// @trace 27
impl QueueEntry {
    /// 取得内嵌链接地址；不导出任务（Task）状态。
    /// # Safety
    /// node须来自仍存活且未移动的QueueEntry，并持有对应队列访问权及保活责任。
    pub unsafe fn link_from_node(node: NonNull<Self>) -> NonNull<LinkedListLink> {
        // SAFETY: 本入口的公开前提原样传给局部实现，不扩大访问权限。
        unsafe { link_from_node_inner(node) }
    }
}

// @trace 28
impl QueueEntry {
    /// 仅用于对应适配器（adapter）的逆向地址换算。
    /// # Safety
    /// link必须确由link_from_node取得，节点仍保活且属于当前队列；不能传任意指针。
    pub unsafe fn node_from_link(link: NonNull<LinkedListLink>) -> NonNull<Self> {
        // SAFETY: 本入口的公开前提原样传给局部实现，不扩大访问权限。
        unsafe { node_from_link_inner(link) }
    }
}

// @trace 29
impl QueueEntry {
    /// 把唯一可执行持有移入自身节点；失败原样返回。
    /// # Safety
    /// 调用者须在owner线程，独占准备插入的节点，随后承担入链成功或回退取回责任。
    pub unsafe fn store(
        task: crate::runnable::Runnable,
        owner: TaskOwnerId,
    ) -> Result<NonNull<Self>, crate::runnable::Runnable> {
        // SAFETY: 本入口的公开前提原样传给局部实现，不扩大访问权限。
        unsafe { queue_store_inner(task, owner) }
    }
}

// @trace 30
impl QueueEntry {
    /// 已安全摘链后取回唯一可执行持有，使链接可复用。
    /// # Safety
    /// 须为本队列已完全摘除的保活节点，当前owner线程且只取一次。
    pub unsafe fn take(node: NonNull<Self>, owner: TaskOwnerId) -> crate::runnable::Runnable {
        // SAFETY: 本入口的公开前提原样传给局部实现，不扩大访问权限。
        unsafe { queue_take_inner(node, owner) }
    }
}

// @trace 31
impl CurrentGuard {
    /// 安装当前单次轮询（poll）身份；pointer必须由本线程独占访问且覆盖守卫存续。
    unsafe fn enter(pointer: NonNull<Header>) -> Self {
        // SAFETY: 本入口的公开前提原样传给局部实现，不扩大访问权限。
        unsafe { current_enter_inner(pointer) }
    }
}

// @trace 32
impl Drop for CurrentGuard {
    // 同步访问退出时恢复上层当前任务（Task）身份，覆盖正常和展开路径。
    fn drop(&mut self) {
        current_guard_drop_inner(self)
    }
}

// @trace 34
pub(crate) fn current_task_id() -> Option<TaskId> {
    current_task_id_inner()
}

// @trace 35
/// 由宿主统一安装的恐慌钩子（panic hook）在展开前调用，按当前任务选项采集起点堆栈（backtrace）。
///
/// 本库不安装或替换全局钩子；未被宿主接入时不会伪造起点资料。
/// 不在本库轮询（poll）范围或未启用采集时返回false；成功保存返回true。
/// 记录按本次轮询内发生顺序保存；用户可自行捕获或重新抛出恐慌，记录不保证对应最终载荷。
/// 只保存当前物理调用栈，不代表完整异步历史；采集可能分配并有冷路径成本。
pub fn capture_current_panic_backtrace() -> bool {
    capture_current_backtrace_inner()
}

/// 捕获的恐慌（panic）资料；原载荷只可由结果持有者领取一次。
#[derive(Debug)]
// @trace 116
pub struct PanicRecord {
    /// 标准catch_unwind取得的原始载荷，不保证是字符串。
    pub payload: Box<dyn std::any::Any + Send + 'static>,
    /// 本次轮询（poll）中由宿主钩子取得的起点记录，按发生顺序排列；未接入或关闭时为空。
    /// 记录可能来自已被用户内部捕获的恐慌，不保证某条属于payload；不代表完整异步历史。
    pub backtraces: Vec<std::backtrace::Backtrace>,
}

/// 任务（Task）执行或结果领取失败，不解释业务输出的成功/失败。
#[derive(Debug)]
// @trace 117
pub enum TaskError {
    /// 取消（cancellation）已落实且没有本次正常Ready结果。
    Cancelled,
    /// 未接纳的初始执行持有已被放弃，原异步计算（Future）未再推进。
    Abandoned,
    /// 直接轮询（poll）范围中捕获的恐慌（panic）。
    Panicked(PanicRecord),
    /// 原值清理失败，不能提供成功确认。
    CleanupUnconfirmed,
    /// 所属域已关闭且没有可领取正常输出。
    OwnerClosed,
    /// 调用不满足输出的所属线程限制。
    WrongThread,
    /// 唯一领取已经结束，不能重复领取。
    AlreadyTaken,
}

// @trace 118
pub(crate) struct CreatedRefs {
    pub(crate) runnable: TaskRef,
    pub(crate) control: TaskRef,
    pub(crate) join: TaskRef,
}

// @trace 36
pub(crate) fn allocate_send<F, S, M>(
    future: F,
    schedule: S,
    metadata: M,
    options: TaskOptions,
) -> Result<CreatedRefs, (F, S, M, TaskOptions, BuildErrorKind)>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
    M: Send + Sync + 'static,
{
    allocate_send_inner(future, schedule, metadata, options)
}

// @trace 37
pub(crate) fn allocate_local<F, S, M>(
    owner: &TaskThread,
    future: F,
    schedule: S,
    metadata: M,
    options: TaskOptions,
) -> Result<CreatedRefs, (F, S, M, TaskOptions, BuildErrorKind)>
where
    F: Future + 'static,
    F::Output: 'static,
    S: Schedule,
    M: Send + Sync + 'static,
{
    allocate_local_inner(owner, future, schedule, metadata, options)
}

// @trace 33
impl std::fmt::Display for TaskError {
    /// 给出稳定含义的失败说明，不消耗或格式化任意panic载荷。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        error_display_inner(self, f)
    }
}

impl std::error::Error for TaskError {}

// 所有底层入口的签名与连接在此列出；实现只能落实对应局部操作。
// @trace 38
static NATIVE_WAKER_VTABLE: RawWakerVTable =
    RawWakerVTable::new(native_clone, native_wake, native_wake_by_ref, native_drop);

// @trace 119
pub(crate) trait StorageLinks {
    unsafe fn node(pointer: NonNull<Self>) -> Option<NonNull<QueueEntry>>;
}

impl StorageLinks for () {
    unsafe fn node(pointer: NonNull<Self>) -> Option<NonNull<QueueEntry>> {
        // SAFETY: 本实现仅对零大小无链接载荷操作，原前提传入局部实现。
        unsafe { no_link_node_inner(pointer) }
    }
}

impl StorageLinks for QueueEntry {
    unsafe fn node(pointer: NonNull<Self>) -> Option<NonNull<QueueEntry>> {
        // SAFETY: 只转交存活QueueEntry的同一指针，不转换为其它类型。
        unsafe { local_link_node_inner(pointer) }
    }
}

// @trace 39
impl<F, S, M, L> TaskStorage<F, S, M, L>
where
    F: Future + 'static,
    F::Output: 'static,
    S: Schedule,
    M: Send + Sync + 'static,
    L: StorageLinks + 'static,
{
    const VTABLE: TaskVTable = TaskVTable {
        run: storage_run::<F, S, M, L>,
        drop_future: storage_drop_future::<F, S, M, L>,
        destroy: storage_destroy::<F, S, M, L>,
        schedule: storage_schedule::<F, S, M, L>,
        take_result: storage_take_result::<F, S, M, L>,
        metadata: storage_metadata::<F, S, M, L>,
        queue_entry: storage_queue_entry::<F, S, M, L>,
    };
}

unsafe fn native_clone(pointer: *const ()) -> std::task::RawWaker {
    // SAFETY: RawWaker协议保证pointer持有相应存活引用；四入口不借出F。
    unsafe { native_clone_inner(pointer) }
}

unsafe fn native_wake(pointer: *const ()) -> () {
    // SAFETY: RawWaker协议保证pointer持有相应存活引用；四入口不借出F。
    unsafe { native_wake_inner(pointer) }
}

unsafe fn native_wake_by_ref(pointer: *const ()) -> () {
    // SAFETY: RawWaker协议保证pointer持有相应存活引用；四入口不借出F。
    unsafe { native_wake_by_ref_inner(pointer) }
}

unsafe fn native_drop(pointer: *const ()) -> () {
    // SAFETY: RawWaker协议保证pointer持有相应存活引用；四入口不借出F。
    unsafe { native_drop_inner(pointer) }
}

unsafe fn storage_run<F, S, M, L>(pointer: NonNull<Header>, owner: &TaskThread) -> RunOutcome
where
    F: Future + 'static,
    F::Output: 'static,
    S: Schedule,
    M: Send + Sync + 'static,
    L: StorageLinks + 'static,
{
    // SAFETY: vtable创建与调用保持同一F/S/M/L布局及此入口独占/线程前提。
    unsafe { storage_run_inner::<F, S, M, L>(pointer, owner) }
}

unsafe fn storage_drop_future<F, S, M, L>(
    pointer: NonNull<Header>,
    owner: Option<&TaskThread>,
) -> ()
where
    F: Future + 'static,
    F::Output: 'static,
    S: Schedule,
    M: Send + Sync + 'static,
    L: StorageLinks + 'static,
{
    // SAFETY: vtable创建与调用保持同一F/S/M/L布局及此入口独占/线程前提。
    unsafe { storage_drop_future_inner::<F, S, M, L>(pointer, owner) }
}

unsafe fn storage_destroy<F, S, M, L>(pointer: NonNull<Header>) -> ()
where
    F: Future + 'static,
    F::Output: 'static,
    S: Schedule,
    M: Send + Sync + 'static,
    L: StorageLinks + 'static,
{
    // SAFETY: vtable创建与调用保持同一F/S/M/L布局及此入口独占/线程前提。
    unsafe { storage_destroy_inner::<F, S, M, L>(pointer) }
}

unsafe fn storage_schedule<F, S, M, L>(
    pointer: NonNull<Header>,
    task: crate::runnable::Runnable,
) -> ()
where
    F: Future + 'static,
    F::Output: 'static,
    S: Schedule,
    M: Send + Sync + 'static,
    L: StorageLinks + 'static,
{
    // SAFETY: vtable创建与调用保持同一F/S/M/L布局及此入口独占/线程前提。
    unsafe { storage_schedule_inner::<F, S, M, L>(pointer, task) }
}

unsafe fn storage_take_result<F, S, M, L>(pointer: NonNull<Header>, destination: NonNull<()>) -> ()
where
    F: Future + 'static,
    F::Output: 'static,
    S: Schedule,
    M: Send + Sync + 'static,
    L: StorageLinks + 'static,
{
    // SAFETY: vtable创建与调用保持同一F/S/M/L布局及此入口独占/线程前提。
    unsafe { storage_take_result_inner::<F, S, M, L>(pointer, destination) }
}

unsafe fn storage_metadata<F, S, M, L>(
    pointer: NonNull<Header>,
    requested: TypeId,
) -> Option<NonNull<()>>
where
    F: Future + 'static,
    F::Output: 'static,
    S: Schedule,
    M: Send + Sync + 'static,
    L: StorageLinks + 'static,
{
    // SAFETY: vtable创建与调用保持同一F/S/M/L布局及此入口独占/线程前提。
    unsafe { storage_metadata_inner::<F, S, M, L>(pointer, requested) }
}

unsafe fn storage_queue_entry<F, S, M, L>(pointer: NonNull<Header>) -> Option<NonNull<QueueEntry>>
where
    F: Future + 'static,
    F::Output: 'static,
    S: Schedule,
    M: Send + Sync + 'static,
    L: StorageLinks + 'static,
{
    // SAFETY: vtable创建与调用保持同一F/S/M/L布局及此入口独占/线程前提。
    unsafe { storage_queue_entry_inner::<F, S, M, L>(pointer) }
}

// 等唤醒查询只消费已有状态字，不另加状态或统计维护。
// TaskRef的存活强持有（strong reference）覆盖Header的一次SeqCst读取；
// ACCESS_IDLE且STARTED成立，并且WAKE_PENDING、RUNNABLE_ISSUED、CANCEL_RECORDED、
// REASON_MASK均为零时才返回true。其它访问阶段一律false，原因或引用计数不被改写。
// 直接run而未admit也可能形成稳定挂起（Pending）；服务聚合须另用自己的接纳登记限定范围。
// 取样后允许状态变化，不通过重读、自旋或取得执行权延长这一观察事实。
// @trace 128
impl TaskRef {
    // 私有窄查询；控制句柄（control handle）及接纳登记共享同一个判定出口。
    pub(crate) fn is_waiting_for_wake(&self) -> bool {
        ref_waiting_for_wake_inner(self)
    }
}

// 登记已有的TaskRef覆盖以下读取；不新增成员、引用计数（reference count）或执行持有。
// 登记的原线程属性保持，不能把这些方法当作跨线程访问TaskRegistration的许可。
// @trace 129
impl TaskRegistration {
    /// 查询本份接纳登记对应任务（Task）的基本快照（snapshot）。
    ///
    /// 可在交出可执行句柄（runnable handle）后使用，不取得新的执行或结果领取权限。
    /// 字段（field）的一致性与失效边界沿用[`TaskSnapshot`]；返回后不自动更新。
    /// 本方法同步只读，时间与额外空间均为O(1)，不分配、不等待任务（Task）推进。
    /// 本登记仍只能在所属线程（thread）内使用，不能跨线程转移或共享。
    pub fn snapshot(&self) -> TaskSnapshot {
        self.task.snapshot()
    }
}

// @trace 130
impl TaskRegistration {
    /// 查询原任务（Task）是否在交接完成后等待新的任务唤醒（wake-up）。
    ///
    /// 判定与[`crate::control::ControlHandle::is_waiting_for_wake`]相同：挂起（Pending）
    /// 已稳定、没有执行访问、待处理唤醒（wake-up）、已发出执行持有、取消（cancellation）或退出才为true。
    /// false不能证明已结束或已入队（enqueue）；取样后任务（Task）可立即发生变化。
    ///
    /// 只在所属线程（thread）通过本登记同步查询。时间与额外空间均为O(1)，
    /// 不分配、不等待、不改变原任务（Task）。统计时还须筛选本服务已接纳的登记；
    /// 多项查询不承诺全局同一时刻快照（snapshot）。
    pub fn is_waiting_for_wake(&self) -> bool {
        self.task.is_waiting_for_wake()
    }
}

// M独立于原F的清理存续。借用已有TaskRef不延迟F退出，不为读取再克隆（clone）持有。
// 共享借用（shared borrowing）不等于M不可经自身安全的内部可变性（interior mutability）更新。
// @trace 131
impl TaskRegistration {
    /// 按创建时关联的类型（type），共享借用（shared borrow）本任务（Task）的附加数据（task metadata）。
    ///
    /// M与原始类型（type）完全相符时返回Some(&M)，否则返回None；不会复制或转交M。
    /// M须满足Send、Sync及'static；只借出共享引用（shared reference），不提供任意&mut M。
    /// M可通过其自身安全接口（API）管理内部更新，相关同步及业务成本由M的实现决定。
    ///
    /// 返回引用（reference）不超过对本登记的借用期。本登记保持有效时，原异步计算（Future）
    /// 已完成合法析构（drop）后仍可读取M；读取不要求延迟原异步计算（Future）的清理。
    /// 本方法在所属线程（thread）同步完成，时间与额外空间均为O(1)，不分配、不等待，
    /// 不推进原任务（Task），也不承诺M内部多个值构成一致快照（snapshot）。
    pub fn metadata<M: Send + Sync + 'static>(&self) -> Option<&M> {
        self.task.metadata::<M>()
    }
}

// 当前身份仅由既有CurrentGuard安装于storage_run中的原F::poll动态范围。
// 本函数 -> current_metadata_inner -> 既有Header.vtable.metadata -> storage_metadata；
// TypeId匹配后只借M，绝不投影原F或结果。当前轮询（poll）的存活持有覆盖存储，
// 不额外生成可执行句柄（runnable handle）、强持有（strong reference）或另一份TLS绑定。
// CURRENT_TASK内的指针只是借用；不能将其包装成会正常Drop的TaskRef而凭空释放一份强引用。
// 闭包（closure）在全部TLS访问借用结束后调用；合法嵌套沿原CurrentGuard保存及恢复。
// 无身份及类型不符不调用read；read可经M自身安全接口更新M，不承诺整个调用无副作用。
// read的恐慌（panic）在当前调用范围传播，原F::poll的已有捕获边界及CurrentGuard恢复保持。
// @trace 132
/// 在当前任务（Task）中同步借用（borrow）附加数据（task metadata），并返回读取闭包（closure）的结果。
///
/// 只有本库正在轮询（poll）原异步计算（Future）的同步范围内才存在当前任务（Task）。
/// 仅有运行时（runtime）环境、在外部调用block_on或在轮询（poll）之外执行回调（callback），
/// 都不能替代这一前提。同步嵌套时访问实际当前任务（Task），结束后沿原作用域恢复。
///
/// read接收创建时关联M的共享引用（shared reference），核实身份和类型（type）后恰好调用一次。
/// M须满足Send、Sync及'static；R没有额外Send或'static约束。
/// 所借&M不能进入返回值、逃逸或跨异步等待（await）。可经M自身安全接口（API）更新其内部值，
/// 但不提供任意&mut M；read自身的其它合法业务行为、同步和成本由调用者负责。
///
/// # 错误
/// 没有当前任务（Task）时返回[`CurrentTaskAccessError::NoCurrentTask`]；
/// M类型（type）不符时返回[`CurrentTaskAccessError::MetadataTypeMismatch`]。
/// 两种失败都不调用read，不创建默认任务（Task）或默认M。
///
/// # 恐慌（panic）
/// read的恐慌（panic）沿当前调用范围传播，本函数不另行捕获。
///
/// # 成本与线程（thread）
/// 取得身份和核对类型（type）的时间及额外空间均为O(1)，本入口不分配、不阻塞或改变任务（Task）生命周期（lifecycle）。
/// read同步执行，其时间、分配及其它合法副作用不包含在上述成本内；引用（reference）仅在本次调用内有效。
pub fn try_with_current_metadata<M: Send + Sync + 'static, R>(
    read: impl FnOnce(&M) -> R,
) -> Result<R, CurrentTaskAccessError> {
    current_metadata_inner::<M, R>(read)
}

include!("task_inner.rs");
