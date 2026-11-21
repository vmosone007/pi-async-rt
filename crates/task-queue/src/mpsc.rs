//! 默认侵入式多生产者单消费者队列（MPSC queue）。
//!
//! [`MpscTaskQueue`]唯一管理清理和关闭；[`MpscTaskQueueHandle`]提供同一来源的共享访问。
//! 队列（queue）只保管及交付原Runnable，不接纳到执行器（executor）、绑定线程（thread）
//! 或轮询（poll）业务。清理只经原Runnable的合法退出路径，不能据此确认原Future已析构（drop）。
use crate::external::{
    BatchAttempt, ExternalPushError, ExternalPushErrorKind, ExternalQueueManager, ExternalTaskQueue,
};
use crate::management::QueueCleanupProgress;
use cordyceps::mpsc_queue::{Consumer, TryDequeueError};
use pi_async_task::{MpscEntryHandle, MpscQueueEntry, Runnable};
use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::num::{NonZeroU8, NonZeroUsize};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

// 内部设计，不进入API docs。
// 本模块（module）与include!接入的mpsc_inner.rs同属一个实际模块（module）。
// 容器（container）与原生Consumer归本模块；任务（Task）身份、固定地址和原线程清理归
// pi_async_task。唯一持有链为Runnable -> MpscEntryHandle -> 原生队列（native queue）
// -> 安全归还的MpscEntryHandle -> 原Runnable；只移动原持有，不增加每任务强引用或分配。
// 原生容器（container）通过任务库（task crate）的Linked适配回返窄操作；不读私有任务头。
// 本地链表与MPSC链接（link）可先后复用，但同一原Runnable不能同时占用两种结构。
//
// queue、state、clear、options是仅有的共享成员。state低位为逻辑数量，最高三位分别
// 是停收、管理者退出、清理请求；初始化后只用RMW修改，不设置独立完成位或独立计数。
// try_reserve以有限Relaxed CAS将开放检查与加一合为一次；该登记成功后只发布一次原生enqueue。
// len／is_empty是Relaxed样本；普通获取start_take用Acquire RMW固定数量上界，
// 不追逐后来提交。停止／退出／请求及确认旧请求采用AcqRel RMW，归还数量采用Release。
// 发布完成和归还Consumer之后，confirm_exit以AcqRel RMW可靠确认，不能改成普通load。
//
// clear固定范围用另一份独立空节点（node）标终点，不能与原生stub共用。
// Available -> Transferring -> Queued -> Reached -> Available；仅在Consumer内访问clear。
// 终点不计业务数量，永不交给执行器（executor）；Transferring归还权限前必须消除。
// 普通取出遇终点也只内部保存，不能开始新清理范围；先前独立结果不再属于来源责任。
// 清理先准备空间，再取得Consumer；保护内不扩容、不析构业务原值、不调用用户代码。
// 普通交付先装入最终独立结果，ConsumerGuard.handed_off记数并在保护内一次归还数量，
// 再归还权限、确认、返回；异常时最终结果也只在权限外析构（drop）。
// 内部清理则先把原值放CleanupGuard.pending，归还Consumer后逐项释放／合法移交再减数。
// Reached后若本次还在保护外清理，保留该状态并返回MoreWork，下次管理调用确认后才复用终点。
//
// 普通提交／取出的CompletionGuard在其最外层；普通Clear／Close在cleanup_step内部最外层建立它，
// OwnerExit帮助步不建立同类触发器。确认守卫先声明，结果／CleanupGuard其后，ConsumerGuard最后；
// 逆序退出使Consumer先归还、原值其后处理、最后作可靠确认。拒绝接收不激活CompletionGuard。
// 管理者退出先原子登记停收／退出／请求，再协助处理，不等最后Arc、不留无主尾部。
// assist_cleanup每次独立进入先登记一次请求，再明确循环调用cleanup_step(OwnerExit)。
// MoreWork只继续检查，不在循环入口重新登记；Complete结束，Busy交实际Consumer，
// Deferred交尚未完成发布或保护外清理的真实参与者，不能只凭请求位猜测承担者。
// cleanup_step在取得权限后、检查队列（queue）前确认旧请求；归还后可靠确认新请求。
// OwnerExit只有真实处理原Runnable并Release减数，或真实取回／确认终点后，才在权限外
// 再登记一次；只因他人请求重查、重复看见旧状态或取得又归还空Consumer，均不制造请求。
// 进展记录属于本处理步的局部量，每步重置；完成合法清理／减数至登记之间不插入用户代码。
// 完整完成事实优先于请求：停收、真实原生取空、终点安全回收、数量为零及本份责任结束
// 须共同成立；迟到请求不能推翻该证明。来源Complete仍不证明原Future已经析构（drop）。
// 帮助过程不再次建立会递归触发同一帮助过程的CompletionGuard，不自旋等待生产者。
//
// 原值析构（drop）可能调度（Schedule）或触发其它用户代码，只能在Consumer外进行。
// CleanupGuard先无分配建立责任，再prepare空间；首次异常保留原恐慌（panic），
// current仅归还一次欠账，失败原值不重试；pending及未合法移交的关闭尾部仍须继续处理。
// 批次分配失败时以栈上单项备援，不新增catch_unwind、锁（lock）、线程（thread）或后台任务。
// 真正双恐慌（double panic）和panic=abort不承诺恢复。来源减数不证明FutureDropStatus::Confirmed。
// 原生默认Drop逐项释放，不能用它代替本来源管理者退出后的完整接续。
//
// 核对入口：公开创建／提交／单项及批次获取／clear／close／Drop；验证相同TaskId、
// 原值返还、一次记账、终点范围、发布缺口、Consumer占用、管理者退出交错及原线程清理。
// 类型（type）声明和占位编译不证明这些行为或热路径（hot path）性能已经实现。

/// 默认MPSC来源的唯一管理对象，不支持Clone或Default。
///
/// 通过[`ExternalQueueManager`]取得普通句柄（handle）、清理固定范围或永久关闭接收。
/// 对象可跨线程转移和共享（Send + Sync），管理方法仍要求独占可变借用（&mut self）。
/// 释放（drop）先停收并安排剩余来源责任，不以所有共享句柄（shared handle）消失为前提。
/// 原任务（Task）的清理可能执行用户代码；整个关闭／析构（drop）不保证固定耗时。
// @trace 1
pub struct MpscTaskQueue {
    // 唯一管理责任与普通访问共用同一分配，禁止导出Shared或原生Consumer。
    shared: Arc<MpscQueueShared>,
}

/// 同一默认MPSC来源的受限共享句柄（shared handle），支持Clone + Send + Sync。
///
/// 提交及获取通过[`ExternalTaskQueue`]，另提供单项获取和空数量查询。
/// 克隆（Clone）不创建新来源、不复制任务（Task）、不取得clear／close管理权限。
/// 管理对象释放后，本句柄（handle）仍可合法查询或拒绝迟到提交，不会重新开放来源。
// @trace 2
pub struct MpscTaskQueueHandle {
    // 仅复制此Arc；不另存逻辑数量或缓存原Runnable。
    shared: Arc<MpscQueueShared>,
}

/// 默认MPSC来源的构造选项；构造后不改变。
#[derive(Clone, Copy)]
// @trace 3
pub struct MpscQueueOptions {
    /// 每步管理操作最多检查的节点（node）数，包含清理终点；默认64。
    /// 不限制普通取出批次，不承诺每次恰好处理满额度。
    pub cleanup_batch_size: NonZeroUsize,
    /// 接收登记最多进行的原子比较交换（CAS）次数；默认3。
    /// 耗尽而未接收时返回Busy；成功后不重复发布原值。
    pub push_attempts: NonZeroU8,
}

/// 一次单项非等待获取的结果；任何失败分类都不携带另一份任务（Task）。
// @trace 4
pub enum MpscTake {
    /// 安全交出原Runnable；任务（Task）身份、地址和线程归属保持。
    Ready(Runnable),
    /// 本次没有业务原值可取，不证明运行时（runtime）空闲或关闭完成。
    Empty,
    /// 本次未取到任务，且发布尚未完成，或来源计数仍包含未结清的责任，例如清理。
    WouldBlock,
    /// 本次没有取得唯一Consumer；不等待其它消费者（consumer）。
    Busy,
    /// 普通获取入口已停用；剩余来源责任仍按管理路径完成。
    Closed,
}

// 四类成员分别负责原生保管、同一原子登记、受保护清理状态、只读配置。
// @trace 5
struct MpscQueueShared {
    queue: cordyceps::MpscQueue<MpscQueueEntry>,
    state: AtomicUsize,
    clear: UnsafeCell<MpscClearState>,
    options: MpscQueueOptions,
}

// 只由Consumer保护；构造和最终独占释放不属于共享访问。
// @trace 6
enum MpscClearState {
    // 来源持有可复用独立终点，不在原生队列（native queue）内。
    Available(MpscEntryHandle),
    // 当前ConsumerGuard.endpoint唯一保管，不能在归还Consumer后残留。
    Transferring,
    // 终点已转入原生队列（native queue），本地不另留第二份持有。
    Queued,
    // 已安全取回终点，等待本轮保护外清理结束及管理确认。
    Reached(MpscEntryHandle),
}

// 不是新的任务（Task）退出原因；只决定当前来源处理范围。
#[derive(Clone, Copy)]
// @trace 7
enum MpscCleanupMode {
    Clear,
    Close,
    OwnerExit,
}

// 不交出原值；Deferred专门阻止帮助者自旋等待已有参与者。
// @trace 8
enum MpscCleanupStep {
    Complete,
    MoreWork,
    Deferred,
    Busy,
}

// 仅借用Shared，不增加Arc；临时持有唯一原生取出权限。
// @trace 9
struct MpscConsumerGuard<'q> {
    shared: &'q MpscQueueShared,
    consumer: Option<Consumer<'q, MpscQueueEntry>>,
    // 只记已装入独立最终结果、待一次归还的普通交付数量。
    handed_off: usize,
    // 只在Transferring时临时保管终点，归还权限前转移或恢复。
    endpoint: Option<MpscEntryHandle>,
}

// 最外层声明、最后退出；自身先撤销再确认，不在帮助循环内递归安装。
// @trace 10
struct MpscCompletionGuard<'q> {
    shared: &'q MpscQueueShared,
    armed: bool,
}

// 来源仍负责的原值在此暂存；current跟踪已经移出但尚欠一次记账的当前项。
// @trace 11
struct MpscCleanupGuard<'q> {
    shared: &'q MpscQueueShared,
    pending: VecDeque<Runnable>,
    current: bool,
    // 仅OwnerExit启用；本批空了不等于整个已停收尾部已完成／合法交接。
    resume_source: bool,
}

// 数量使用低位，增一前必须排除上限，不能让进位覆盖三个标志。
// @trace 12
const COUNT_MASK: usize = usize::MAX >> 3;
// @trace 13
const STOP_ACCEPTING: usize = 1usize << (usize::BITS - 1);
// @trace 14
const MANAGER_RELEASED: usize = 1usize << (usize::BITS - 2);
// @trace 15
const CLEANUP_REQUESTED: usize = 1usize << (usize::BITS - 3);

// SAFETY: queue按原生Linked和MpscEntryHandle的Send保证共享发布及唯一消费；state只作原子访问。
// clear的共享读写均在唯一Consumer内进行，引用（reference）不跨保护；options初始化后不变。
// 原Runnable不在Consumer内析构（drop），所属线程（owner thread）清理仍由原任务（Task）机制承担。
// Shared的Send由字段（field）自然满足；这里不为节点（node）或节点句柄（handle）追加Sync。
// @trace 16
unsafe impl Sync for MpscQueueShared {}

// @trace 17
impl Default for MpscQueueOptions {
    fn default() -> Self {
        mpsc_options_default_inner()
    }
}

// @trace 18
impl MpscTaskQueue {
    /// 创建独立空来源，保管两份不同用途的空节点（node），不创建业务任务（Task）。
    ///
    /// 显式节点（node）分配失败返回pi_result错误报告（error report）；已建资源正常归还。
    /// 标准共享分配的失败策略仍由分配器（allocator）决定，不承诺所有分配失败均可恢复。
    /// 数量上限为`usize::MAX >> 3`。构造成本含常数份分配；普通提交不新增每任务分配。
    pub fn new(options: MpscQueueOptions) -> pi_result::Result<Self> {
        mpsc_queue_new_inner(options)
    }
}

// @trace 19
impl ExternalQueueManager for MpscTaskQueue {
    type Handle = MpscTaskQueueHandle;

    /// 取得同一来源的共享访问；仅增加同一Arc持有，O(1)。
    fn handle(&self) -> Self::Handle {
        mpsc_queue_handle_inner(self)
    }

    /// 内部处理首次调用固定的范围；后来的提交留待下一范围，保持当前接收状态。
    ///
    /// 每次至多检查cleanup_batch_size个节点（node），可能返回MoreWork并由随后调用继续。
    /// 本轮刚清理完终点前原值后，允许下一次调用作完成确认；不返回待清理批次。
    /// 未取得Consumer按pi_result报告Busy；错误不撤销已发生变化，余项仍由来源保管。
    /// 若接收已停止，按关闭责任接续，不重新开放。实际原值清理成本不保证固定上界。
    fn clear(&mut self) -> pi_result::Result<QueueCleanupProgress> {
        mpsc_queue_clear_inner(self)
    }

    /// 永久停收并内部接续所有已接收责任；重复调用幂等（idempotent）。
    ///
    /// 包含先前清理及尚未完成发布的内容；MoreWork不是丢弃余项或等待发布的许可。
    /// Complete要求来源的数量、终点及剩余责任满足结束条件，不等待所有句柄（handle）释放。
    /// Busy按pi_result报告，首次清理恐慌（panic）继续传播，不重新开放、不重试失败原值。
    fn close(&mut self) -> pi_result::Result<QueueCleanupProgress> {
        mpsc_queue_close_inner(self)
    }
}

// @trace 20
impl Drop for MpscTaskQueue {
    fn drop(&mut self) {
        self.shared.manager_released();
        self.shared.assist_cleanup();
    }
}

// @trace 21
impl Clone for MpscTaskQueueHandle {
    fn clone(&self) -> Self {
        mpsc_handle_clone_inner(self)
    }
}

// @trace 22
impl MpscTaskQueueHandle {
    /// 一次非等待获取原Runnable，不为单项建立批次容器（batch container）。
    ///
    /// `Ready`保留原身份和线程归属。`Empty`只说明本次未取到任务。
    /// `WouldBlock`表示本次未取到任务且仍有来源责任，例如发布未完成或清理未结清。
    /// 消费者（Consumer）占用为`Busy`，停收后为`Closed`。
    /// 不轮询（poll）、绑定或改变原任务（Task）语义。
    pub fn try_take_one(&self) -> MpscTake {
        let Some(count) = self.shared.start_take() else { return MpscTake::Closed; };
        if count == 0 { return MpscTake::Empty; }
        let mut completion = MpscCompletionGuard::new(&self.shared);
        let result = mpsc_take_one_inner(self, &mut completion);
        drop(completion);
        result
    }

    /// O(1)查询本次逻辑数量是否为零；与len口径相同，不推进业务或清理。
    /// 不与其它读取组成全局同一瞬间，也不证明来源关闭完成。
    pub fn is_empty(&self) -> bool {
        mpsc_is_empty_inner(self)
    }
}

// @trace 23
impl ExternalTaskQueue for MpscTaskQueueHandle {
    /// 尝试接管同一原Runnable；拒绝返回原值，不取消（cancel）或隐式接纳任务（Task）。
    ///
    /// 缺链接（link）为LinkMissing；停收为Closed；数量上限为CountOverflow；
    /// 接收登记竞争预算耗尽为Busy。只报告本次实际检查出的原因，不承诺并发错误优先级。
    /// 不新增每任务分配、锁（lock）或强引用（strong reference），登记成功后只发布一次。
    fn try_push(&self, task: Runnable) -> pi_result::RawResult<(), ExternalPushError> {
        let mut completion = MpscCompletionGuard::new(&self.shared);
        mpsc_push_inner(self, task, &mut completion)
    }

    /// 一次取得开始时数量上界内的有限业务批次，保持原生获取顺序。
    ///
    /// 一次Consumer，不等发布或后来提交；非空前缀保持Ready，未取到才分别判断其它分类。
    /// 结果在返回前已独立拥有原值，不携带来源借用。批次空间先于取出准备，成本与上界相关。
    fn try_take_batch(&self) -> BatchAttempt {
        let Some(count) = self.shared.start_take() else { return BatchAttempt::Closed; };
        if count == 0 { return BatchAttempt::Empty; }
        let mut completion = MpscCompletionGuard::new(&self.shared);
        let result = mpsc_take_batch_inner(self, count, &mut completion);
        drop(completion);
        result
    }

    /// O(1)取得本来源已接收尚未交付或合法清理的数量样本。
    /// 包括已登记未发布和内部待清理原值，不含stub、终点或已经交出的独立结果。
    fn len(&self) -> usize {
        mpsc_len_inner(self)
    }
}

// 原子窄操作及其组合出口；这些方法不向模块（module）外开放内部状态。
// @trace 24
impl MpscQueueShared {
    fn try_reserve(&self) -> pi_result::RawResult<(), ExternalPushErrorKind> {
        mpsc_reserve_inner(self)
    }

    fn start_take(&self) -> Option<usize> {
        mpsc_start_take_inner(self)
    }

    fn return_count(&self, count: usize) {
        mpsc_return_count_inner(self, count)
    }

    fn stop_accepting(&self) {
        mpsc_stop_accepting_inner(self)
    }

    fn manager_released(&self) {
        mpsc_manager_released_inner(self)
    }

    // 原子确认与帮助分开，真实回返关系在此可见；帮助过程不再安装同一触发器。
    fn confirm_exit(&self) {
        if mpsc_confirm_exit_inner(self) {
            self.assist_cleanup();
        }
    }

    fn assist_cleanup(&self) {
        mpsc_request_cleanup_inner(self);
        loop {
            match self.cleanup_step(MpscCleanupMode::OwnerExit) {
                MpscCleanupStep::MoreWork => continue,
                MpscCleanupStep::Complete | MpscCleanupStep::Deferred | MpscCleanupStep::Busy => break,
            }
        }
    }

    // 内部设计，不进入API docs。对应mpsc_cleanup_step_inner的局部约定：
    // 壳（shell）持有完成守卫（completion guard），芯（inner）准备清理守卫和有限空间。
    // 普通Clear／Close取得一次Consumer后立即启用完成守卫，OwnerExit不递归安装。
    // 归还权限及结清清理后再确认退出。取得权限后以AcqRel确认／清除旧请求。
    // 本步单独记录终点实际推进：try_take返回Ok(None)为真实取回，acknowledge_clear返回true
    // 为真实确认；重复观察Reached／Available或确认false都不是新进展，记录不得跨步保留。
    // 业务原值先由pending保管，归还Consumer、drain正常完成并Release减数后，才能视为本步
    // 已处理；此前异常由既有CleanupGuard的首次备援承担，不撤销责任或另装递归守卫（guard）。
    // 关闭的Complete需要Consumer内真实Err(Empty)且clear为Available、可靠零数量、本份pending／current
    // 均结束及停收成立。证明一旦成立优先返回Complete，不能被新请求改成MoreWork。
    // 若未完成，只有OwnerExit在实际业务清理／减数或真实终点推进之后登记一次请求；
    // 先结束全部节点（node）访问并归还Consumer，再完成清理／记账，再无用户代码地登记，
    // 最后以AcqRel RMW确认新请求。纯请求引起的空重查不再次登记，但仍须检查别人新来的请求。
    // 有实际推进、额度用尽或新请求待重核时为MoreWork；不能进一步取得且已有实际发布者
    // 或保护外清理者负责时为Deferred；取得权限失败为Busy并交给实际持有者。
    // 开放Clear只确认固定终点前范围；停收Clear按Close接续。管理者在世时不得登记要求
    // MANAGER_RELEASED的请求，仍由原管理调用及其CompletionGuard承担后续责任。
    // 不改变四类共享成员、原子顺序（memory ordering）、原值持有、每步额度或异常传播。
    // 核对：完成空来源、发布者暂停、最后发布者遇Busy、保护外减数与Reached确认及首次异常。
    fn cleanup_step(&self, mode: MpscCleanupMode) -> MpscCleanupStep {
        let mut completion = if matches!(mode, MpscCleanupMode::OwnerExit) {
            None
        } else {
            Some(MpscCompletionGuard::new(self))
        };
        mpsc_cleanup_step_inner(self, mode, completion.as_mut())
    }
}

// @trace 25
impl<'q> MpscConsumerGuard<'q> {
    fn try_new(shared: &'q MpscQueueShared) -> Option<Self> {
        mpsc_consumer_new_inner(shared)
    }

    fn try_take(&mut self) -> pi_result::RawResult<Option<Runnable>, TryDequeueError> {
        mpsc_consumer_take_inner(self)
    }

    fn begin_clear(&mut self) {
        mpsc_consumer_begin_clear_inner(self)
    }

    fn acknowledge_clear(&mut self) -> bool {
        mpsc_consumer_acknowledge_inner(self)
    }
}

// @trace 26
impl Drop for MpscConsumerGuard<'_> {
    fn drop(&mut self) {
        mpsc_consumer_drop_inner(self)
    }
}

// @trace 27
impl<'q> MpscCompletionGuard<'q> {
    fn new(shared: &'q MpscQueueShared) -> Self {
        mpsc_completion_new_inner(shared)
    }

    fn arm(&mut self) {
        mpsc_completion_arm_inner(self)
    }
}

// @trace 28
impl Drop for MpscCompletionGuard<'_> {
    fn drop(&mut self) {
        if mpsc_completion_drop_inner(self) {
            self.shared.confirm_exit();
        }
    }
}

// @trace 29
impl<'q> MpscCleanupGuard<'q> {
    fn new(shared: &'q MpscQueueShared, resume_source: bool) -> Self {
        mpsc_cleanup_new_inner(shared, resume_source)
    }

    fn prepare(&mut self, capacity: usize) {
        mpsc_cleanup_prepare_inner(self, capacity)
    }

    fn drain(&mut self) {
        mpsc_cleanup_drain_inner(self)
    }

    fn disarm(&mut self) {
        mpsc_cleanup_disarm_inner(self)
    }
}

// @trace 30
impl Drop for MpscCleanupGuard<'_> {
    // 内部设计，不进入API docs。对应mpsc_cleanup_drop_inner的局部约定：
    // 先归还current的一次欠账，失败原值不再析构（drop）；再在Consumer外逐项处理pending，
    // 原恐慌（panic）继续传播。仅resume_source=true时承担管理者已退出的尾部备援。
    // 前段欠账及pending结束后，首次进入栈上单项备援只登记一次请求；内部重查不在入口续造。
    // 每个单项步重新建立局部进展记录；真实业务处理并Release减数、真实取回终点或
    // acknowledge_clear返回true才构成推进。归还Consumer且必要原值处理结束后再通知一次，
    // 不在减数与通知之间插入用户代码、分配（allocation）或其它可展开工作。
    // Consumer内保存真实Empty和Available事实，归还后以AcqRel RMW确认数量和请求；
    // 联合完成证明优先于请求。尚未完成且有真实推进或新请求才重查；Deferred／Busy必须交实际承担者。
    // 只因他人请求重查、重复状态（state）样本及没有推进的权限归还不能再次通知。
    // 不再reserve，不调用assist_cleanup，不创建递归CompletionGuard／CleanupGuard，
    // 不新增catch_unwind、锁（lock）或线程（thread）；已有展开再恐慌及panic=abort保持原边界。
    // 核对与正常帮助相同的交错（interleaving），并验证原恐慌、原值一次处理及剩余责任。
    fn drop(&mut self) {
        mpsc_cleanup_drop_inner(self)
    }
}

include!("mpsc_inner.rs");
