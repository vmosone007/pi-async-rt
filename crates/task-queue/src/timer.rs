//! 定时任务队列（timer task queue）的本地拥有者、共享入口与交付数据。
//!
//! 外部显式传入同一时钟的单调时间（monotonic time），本地拥有者推进并交出内容。
//! 本模块（module）不读取隐藏时钟，不固定时刻类型（time-point type）或定时轮（timing wheel），
//! 不轮询（poll）业务，也不把取消请求（cancellation request）等同于任务（Task）退出确认。
use crate::management::QueueCleanupProgress;

// 本地拥有者统一保管原Item、键（key）关联、计数和清理责任；共享入口只提供受限访问。
// 具体存储不在这些抽象声明中决定；内部移动不复制原异步计算（Future），不换登记身份。
// 共享中转／原生存储／到期缓冲／清理缓冲使用同一逻辑计数，只有实际交出或清理减一次。
// 实现必须将接收与停收的交错纳入同一责任范围；发布尚未完成时不能先报关闭完成。
// 成功发布后通知所属线程（owner thread），通知失败不能撤销已经接收的事实；
// 休眠前仍须重新检查，计时被动唤醒（passive wake-up）不取代发布后的主动通知。
// 默认时钟转换、稳定键（key）、中转及有限推进算法尚未在此选定，不由此生成适配实现。

/// 原定时来源的本地拥有者；排他方法不自动证明调用线程（thread）符合所属域要求。
///
/// `Item`、`Instant`不施加全局可跨线程转移（Send）或共享（Sync）约束。
/// 共享入口提交时才要求相应值Send + 'static；本地入口可接收非Send原异步计算（Future）。
/// 所属线程（owner thread）的轮询（poll）／析构（drop）责任不能通过共享句柄（shared handle）绕过。
///
/// 所有操作同步完成本次有限工作，不等待未来内容或其它线程（thread）完成发布，不无限自旋。
/// 允许实现已说明的短时同步成本；有限工作不保证固定墙钟耗时。各方法的具体复杂度、
/// 分配及工作额度由实现说明，不强制每次调用者传预算，也不要求默认堆分配（heap allocation）。
// @trace 1
pub trait TimerTaskQueue {
    /// 原登记内容，可为可执行句柄（Runnable）或计时登记；不要求额外封装新任务（Task）。
    type Item;
    /// 同一时钟实例、起点和单位下的绝对时刻；构造和实现负责其关联及比较。
    /// 不要求复制或全序比较，不指定为整数或标准库（standard library）时刻。
    type Instant;
    /// 原登记的不透明键（opaque key），不强制可复制（Copy）或可克隆（Clone）。
    /// 正常释放不取消；内部移动保持身份，过期或错误来源不能命中另一登记。
    type Key: Send + Sync + 'static;
    /// 独立拥有的有限到期序列；迭代不回访来源，不吸收新内容，不携带队列（queue）保护。
    type DueBatch: IntoIterator<Item = Self::Item>;
    /// 独立拥有的有限清理序列，保留每份原值与已确定缘由，不强制特定容器。
    type CleanupBatch: IntoIterator<Item = TimerCleanup<Self::Item>>;
    /// 同一原来源的受限共享入口；类型（type）匹配不单独证明实例匹配。
    type Handle: TimerQueueHandle<
        Item = Self::Item,
        Instant = Self::Instant,
        Key = Self::Key,
    >;

    /// 取得同一原来源的共享句柄（shared handle），不新建队列（queue）或重置状态（state）。
    /// 不强制使用Arc或进行分配；取得／释放句柄（handle）本身不取消登记。
    fn handle(&self) -> Self::Handle;

    /// 本地尝试接收`item`及同源绝对`deadline`，成功立即返回稳定的原登记键（key）。
    ///
    /// 明确拒绝须返还原`item`，不能执行后再拒绝；接收后才发现的问题由来源继续负责。
    /// 截止时间（deadline）不从其它时钟重新推导；具体可表示范围由实现说明，非法值报
    /// `InvalidTime`。无全局Send约束；可变访问不允许在错误线程（thread）触及本地原值。
    fn try_push_local(
        &mut self,
        deadline: Self::Instant,
        item: Self::Item,
    ) -> pi_result::RawResult<Self::Key, TimerSubmitError<Self::Item>>;

    /// 本地同步记录或合并`key`的取消请求（cancellation request），不消费键（key）。
    ///
    /// `Ok(())`只证明请求被记录／合并，不证明登记存在、请求已落实、任务（Task）已取消
    /// 或原异步计算（Future）已析构（drop）。已知无法发布才返回错误；重复请求不重复
    /// 交付原值。错来源和过期身份不能影响另一登记，不等待取消完成。
    fn request_cancel_local(
        &mut self,
        key: &Self::Key,
    ) -> pi_result::RawResult<(), TimerCancelRequestError>;

    /// 以外部给定的绝对`now`有限推进内部计时；不交出内容，也不自行获取时间。
    ///
    /// `now`与提交时刻来自同一时钟；相同时间重复合法，不重复交付。
    /// 早于已接受推进时间时，在产生推进效果之前返回`InvalidTime`，不回退或夹到当前值。
    /// `ReachedTarget`与`MoreWork`区分本次目标是否追完；`MoreWork`不携带原值。
    /// 不能因持续追赶时间而阻挡取消（cancellation）清理或关闭。
    fn advance(
        &mut self,
        now: &Self::Instant,
    ) -> pi_result::RawResult<TimerAdvanceProgress, TimerQueueError>;

    /// 最多交出一份可交付到期原值，不等待其它到期项发现或后来提交。
    /// 不读取另一时钟，使用已有显式推进依据。
    /// `Ready`取得原值及后续责任；不保证本次已经发现全部到期内容。
    fn try_take_due_one(
        &mut self,
    ) -> pi_result::RawResult<TimerTake<Self::Item>, TimerQueueError>;

    /// 完整交出本次固定范围内当前全部到期原值，不接受每次数量上限。
    ///
    /// 开始时固定参考时间与已接收内容范围，后来到达内容留到下次，不无限扩批。
    /// 尚不能证明完整时返回`WouldBlock`并保留原值，不以已发现前缀冒充全部；
    /// 已知无内容才返回`Empty`。`Ready`序列非空，顺序由实现说明且调用者保持。
    /// 序列拥有原值，返回前结束相关借用和同步保护，后续迭代不回访来源。
    fn try_take_due_all(
        &mut self,
    ) -> pi_result::RawResult<TimerTake<Self::DueBatch>, TimerQueueError>;

    /// 独立交出有限待清理内容，不要求到期、空闲或先追完时间。
    ///
    /// 返回每份原值及已有清理缘由；已有缘由不因后来关闭而改写。
    /// `Ready`序列非空，不表示任务（Task）取消或原异步计算（Future）清理已成功。
    /// 若本来源按合法上下文自行完成清理，也须结束相应等待责任，不能悄悄遗留计时等待。
    fn try_take_cleanup(
        &mut self,
    ) -> pi_result::RawResult<TimerTake<Self::CleanupBatch>, TimerQueueError>;

    /// 只读取得最迟必须再次检查本来源的绝对时刻，可保守提前。
    ///
    /// `now`必须与其它时间输入同源；已知立即有工作返回本次`now`对应时刻。
    /// 只有确认没有再次检查的约束时才返回`None`，未知情况不能伪装成`None`。
    /// 不推进计时、不要求全扫描或精确最小期限。调用者用结果限制休眠（sleep），
    /// 仍须保留发布通知及休眠前检查，不以本次样本承诺未来无工作。
    fn next_check_at(
        &self,
        now: &Self::Instant,
    ) -> pi_result::RawResult<Option<Self::Instant>, TimerQueueError>;

    /// 查询来源状态快照（snapshot），不推进计时或业务；不与另一次数量读取组成全局瞬间。
    fn status(&self) -> TimerQueueStatus;

    /// 查询已接收、尚未交出／实际清理的全部登记数。
    /// 包含共享中转、原生存储、到期及待清理缓冲；内部移动或取消标记不减数。
    /// 实际交付／清理恰减一次，不包含已经交给执行器（executor）的在途任务（in-flight task）。
    fn len(&self) -> usize;

    /// 按`len`相同口径取得是否为零的样本，不证明关闭完成或下一次取出结果。
    fn is_empty(&self) -> bool;

    /// 有限清理本次固定范围，不关闭接收，不要求到期，不业务轮询（poll）。
    ///
    /// 未完成时再次调用继续原范围；后来提交留给下一轮范围，不能无限扩展本次工作。
    /// 可在合法上下文自行清理，也可交还原值及缘由；结果序列可以为空，进度另行判断。
    /// `Err`不返回序列，但未交出的原值及责任仍由来源保管，已有合法变化不回滚。
    fn clear(&mut self) -> pi_result::Result<QueueCleanupProgress<Self::CleanupBatch>>;

    /// 先停收，再复用清理；重复调用幂等（idempotent），错误不重新开放。
    ///
    /// 包含未完成的旧清理及停收前所有已接收内容，包括有保留责任的在途发布。
    /// 不强制先取回全部未到期项，不等其它线程（thread）或所有句柄（handle）释放。
    /// 有限工作未结束报`MoreWork`；只有本来源全部责任已完成／合法移交才报`Complete`。
    /// 原值析构（drop）、异常展开（unwinding）与通知失败依既有任务（Task）责任处理，
    /// 本接口（API）不新增恐慌（panic）捕获层，也不报告任务（Task）层已全部退出。
    fn close(&mut self) -> pi_result::Result<QueueCleanupProgress<Self::CleanupBatch>>;
}

/// 同一原定时来源的线程安全共享入口；不提供推进、取出或实际管理权限。
///
/// 克隆（Clone）共享入口不复制来源，不强制Arc布局。共享入口存续不能阻止原
/// 异步计算（Future）的合法析构（drop），也不能重新开放已关闭服务。
/// 调用同步、非等待；允许已说明的短时同步成本，不无限自旋或等待未来任务（Task）。
// @trace 2
pub trait TimerQueueHandle: Clone + Send + Sync + 'static {
    /// 与本地拥有者关联的原登记内容；共享提交方法另加Send约束。
    type Item;
    /// 与原来源相同时间域的绝对时刻，不独立选择另一时钟。
    type Instant;
    /// 与原来源关联的不透明键（opaque key）；正常释放不取消。
    type Key: Send + Sync + 'static;

    /// 尝试接收同源绝对`deadline`及原`item`；成功立即返回原登记身份。
    ///
    /// 即使内容尚在共享中转区，返回键（key）也必须已经有效关联；接收后发布失败不能
    /// 伪装未接收。明确拒绝返还原`item`。仅此方法要求内容和时刻Send + 'static，
    /// 不要求Sync；共享入口可跨线程（thread）访问不授权跨线程触及本地原异步计算（Future）。
    /// 成功发布后安排所属线程（owner thread）继续检查；必要通知失败不撤销已接收事实。
    fn try_push(
        &self,
        deadline: Self::Instant,
        item: Self::Item,
    ) -> pi_result::RawResult<Self::Key, TimerSubmitError<Self::Item>>
    where
        Self::Item: Send + 'static,
        Self::Instant: Send + 'static;

    /// 借用`key`记录／合并取消请求（cancellation request），不等待目标清理。
    ///
    /// `Ok(())`不证明目标存在、请求落实或取消成功；只有已知不能发布才返回错误。
    /// 请求发布后安排所属线程（owner thread）继续检查，通知失败不撤销已经记录的事实。
    /// 共享调用不返回`WrongOwner`，错来源及过期身份绝不能命中另一登记。
    fn request_cancel(
        &self,
        key: &Self::Key,
    ) -> pi_result::RawResult<(), TimerCancelRequestError>;

    /// 安全读取原来源状态快照（snapshot）；不推进计时或清理，不重开已关闭服务。
    fn status(&self) -> TimerQueueStatus;

    /// 按拥有者`len`同一口径安全取样，不承诺与其它查询处于同一瞬间。
    fn len(&self) -> usize;

    /// 按同一计数口径查询当次是否为零，不承诺下一次取出或整体退出条件。
    fn is_empty(&self) -> bool;
}

/// 来源自身状态（state），按接收开放、关闭处理中、自身退出完成单向变化。
// @trace 3
pub enum TimerQueueStatus {
    /// 接收开放，不保证每次提交成功。
    Accepting,
    /// 已停收，自身原值／发布／资源责任尚未结束。
    Closing,
    /// 本来源自身责任完成或合法移交；不表示全部任务（Task）退出或存储最终释放。
    LocalExited,
}

/// 本次有限时间推进结果，不携带登记原值。
// @trace 4
pub enum TimerAdvanceProgress {
    /// 已追到本次输入目标，不表示已将全部到期内容交给调用者。
    ReachedTarget,
    /// 仍有推进工作；调用者随后继续，不能假称已经追到目标。
    MoreWork,
}

/// 一次到期或清理取出的正常分类；异常使用外层错误。
// @trace 5
pub enum TimerTake<T> {
    /// 已交出原值及责任；用于到期批次（DueBatch）或清理批次（CleanupBatch）时必须非空。
    /// 单项原值（Item）不受此限制，即使其自身是空向量（Vec）等容器也可以交付。
    Ready(T),
    /// 本次无可交付内容，不证明时间追赶完成或允许关闭。
    Empty,
    /// 发布／必要准备未完成；全部到期取出尚不能证明完整时也使用此分类。
    WouldBlock,
    /// 本次访问权被占用，不无限自旋（spin）。
    Busy,
    /// 此取出出口已结束，整体来源仍可能在关闭处理中。
    Closed,
}

/// 一份待清理原内容及来源交还它的缘由，不是任务（Task）的最终退出结果。
// @trace 6
pub struct TimerCleanup<Item> {
    /// 转交给调用者、需要在合法上下文继续处理的原值。
    pub item: Item,
    /// 已确定的交还缘由；后来关闭不能改写先前已确定的取消（cancellation）缘由。
    pub reason: TimerCleanupReason,
}

/// 来源移除原登记的缘由；不证明取消成功或原异步计算（Future）析构（drop）完成。
// @trace 7
pub enum TimerCleanupReason {
    /// 因已记录的取消请求（cancellation request）交还。
    CancelRequested,
    /// 关闭停止接收后，新纳入关闭范围的内容。
    QueueClosing,
    /// 接收开放时纳入清理范围的内容。
    QueueClearing,
}

/// 明确未接收时返还原值及原因；原值不放入错误报告（Report）。
/// 不为报告强迫`Item`满足Debug、Sync或错误上下文（error context）约束。
// @trace 8
pub struct TimerSubmitError<Item> {
    /// 未被来源接管的原内容，调用者继续负责。
    pub item: Item,
    /// 本次明确拒绝原因；不表示原任务（Task）已退出。
    pub cause: TimerQueueError,
}

/// 取消请求（cancellation request）的已知发布错误；不表达取消完成分类。
// @trace 9
pub type TimerCancelRequestError = TimerQueueError;

/// 定时来源的有限异常分类；每项操作只使用有意义的子集。
///
/// 无键（key）的操作不报`WrongQueue`；共享方法不报`WrongOwner`；`KeyExhausted`仅用于
/// 新登记。普通取出中的`Busy`／`Closed`用`TimerTake`正常分支，不再套同名错误。
/// 外层错误不交出批次（batch），也不保证此前没有状态变化；未交出内容仍由来源负责。
// @trace 10
pub enum TimerQueueError {
    /// 来源已停止接收或所需操作已不再开放。
    Closed,
    /// 本次请求／管理访问权暂不可取得。
    Busy,
    /// 键（key）明确属于其它来源。
    WrongQueue,
    /// 本地方法不在合法所属线程（owner thread）／上下文执行。
    WrongOwner,
    /// 时间域、可表示范围或单调推进条件不满足。
    InvalidTime,
    /// 实现声明的容量限制拒绝。
    Capacity,
    /// 逻辑计数将溢出，不允许回绕。
    CountOverflow,
    /// 新登记不能再取得可靠的独立键（key），不复用身份误命中旧登记。
    KeyExhausted,
    /// 原底层错误报告（Report），保留其分类、来源和附件。
    Backend(pi_result::Error),
}

// @trace 11
impl TimerQueueError {
    /// 消耗本错误，转换为pi_result统一错误报告（Report）。
    ///
    /// `Closed`／`WrongOwner`映射InvalidState，`Busy`映射Busy，
    /// `WrongQueue`／`InvalidTime`映射InvalidInput，其余资源分类映射ResourceExhausted。
    /// `Backend`直接归还原报告，不改上下文或另包一层，不要求报告可克隆（Clone）。
    /// 不访问原队列（queue）、内容或时钟；失败诊断可能分配并产生堆栈（backtrace）成本。
    /// 此转换的非后端分支仍是未实现占位（unimplemented stub），调用会触发恐慌（panic）。
    pub fn into_report(self) -> pi_result::Error {
        match self {
            Self::Backend(report) => report,
            cause => timer_error_report_inner(cause),
        }
    }
}

include!("timer_inner.rs");
