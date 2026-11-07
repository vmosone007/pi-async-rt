//! 任务（Task）的基础定义。
//!
//! [`TaskPhase`] 给出生命周期（lifecycle）的最高层阶段分类。
//! 一个阶段可以包含更细的状态（state）；例如，计算推进阶段既可正在轮询（poll），
//! 也可处于挂起（Pending）。阶段值不表示是否入队（enqueue），也不授予执行权限。
//!
//! [`CancelRequestStatus`] 只说明目标是否已经记录取消请求（cancellation request）；
//! [`FutureDropStatus`] 只说明原异步计算（Future）的合法析构（drop）是否已确认完成。
//! 两项分别成立：已记录请求仍可能正常返回完成就绪（Ready），正常完成也需要清理原值。
//! 单独构造或复制（copy）这些值，不会查询、控制或清理任何实际任务（Task）。

// 内部设计，不进入公开接口文档（API docs）。
// 本模块（module）只定义可以共享使用的描述值，不保存某个任务（Task）的权威可变状态（state）。
// 创建、执行、状态（state）维护和清理由实际持有任务（Task）的机制负责。
// TaskPhase 不持有原异步计算（Future）、结果（output）、队列（queue）链接或执行权限；
// 复制（copy）阶段值不会改变这些对象的持有关系。
//
// 首次合法轮询（poll）开始才从准备进入推进；提交（submission）、接纳（admission）或
// 入队（enqueue）不构成这个阶段边界。推进阶段中的挂起（Pending）与交接不回到准备。
// 进入退出处理须同时满足当前无轮询（poll）和永久停止后续轮询（poll）；
// 取消请求（cancellation request）已记录不单独满足这个条件。
// 退出处理内可存在原异步计算（Future）析构（drop）前后的不同情况，不能由 Exiting
// 推断已经完成析构（drop）。Ended 则要求原异步计算（Future）已合法析构（drop），
// 且自身责任全部完成或合法移交；不要求外部已经取得的结果（output）也被销毁。
//
// 本类型（type）不选择内部编码、原子存储或查询发布机制；不能把枚举（enum）的判别值
// 直接解释为实际任务（Task）的内部状态字。声明本类型（type）也不保证外部持有者能
// 观察到每一个阶段：具体观察入口及其存续范围须由提供该入口的对象自行说明。
// 纯声明没有配对实现或运行期转换操作；本位置无需要填充的算法（algorithm）。

// @trace 1
/// 描述单个任务（Task）的生命周期（lifecycle）所处的最高层阶段。
///
/// 阶段值不持有任务（Task）或异步计算（Future），不授予执行或控制权限。
/// 复制（copy）本值只产生另一份相同的描述值，不创建、提交或推进任务（Task）。
/// 本类型（type）本身不提供实时查询，也不保证某个外部观察入口能够取得全部阶段。
///
/// # 使用示例（Usage）
///
/// ```
/// use pi_async_task::definitions::TaskPhase;
///
/// let phase = TaskPhase::Preparing;
/// let another_copy = phase;
/// ```
///
/// # 成本与安全（Cost and safety）
///
/// 普通构造、复制（copy）及相等比较均为 O(1)，无需堆分配、锁（lock）、
/// 异步等待（await）或用户回调（callback），不修改任何实际任务（Task）。
/// 本值可在线程（thread）之间转移或通过共享引用（shared reference）读取。
/// 调试（debug）输出的成本和行为取决于调用方提供的格式化（formatting）目标。
///
/// 不保证内存布局（memory layout）或判别值（discriminant）的具体数值；
/// 不应将它们用作实际任务（Task）的内部编码或外部稳定数据格式。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskPhase {
    /// 准备阶段（Preparation phase）：已成功建立，尚未开始首次合法轮询（poll）。
    ///
    /// 尚未提交（submission）和已接纳（admission）但尚未首次轮询（poll）的任务（Task），
    /// 都属于此阶段。取消（cancellation）落实可使其直接进入退出处理。
    Preparing,

    /// 计算推进阶段（Computation advancement phase）：首次合法轮询（poll）已经开始，
    /// 但尚未满足退出处理的条件。
    ///
    /// 该阶段涵盖正在轮询（poll）、挂起（Pending）及必要交接，
    /// 不表示此刻一定有执行者正在调用轮询（poll）。
    Advancing,

    /// 退出处理阶段（Exit-handling phase）：当前没有轮询（poll），
    /// 并且已经永久停止后续轮询（poll），仍在处理自身退出责任。
    ///
    /// 原异步计算（Future）的合法析构（drop）可能尚未完成；
    /// 单纯记录取消请求（cancellation request）不等于已经进入此阶段。
    Exiting,

    /// 本任务全部自身责任已结束（Task responsibilities ended）：
    /// 原异步计算（Future）已合法析构（drop），自身责任全部完成或合法移交。
    ///
    /// 不表示分配器（allocator）已经归还存储，也不要求已经合法交给外部的值全部销毁。
    Ended,
}

// 内部设计，不进入公开接口文档（API docs）。
// 三项描述值的职责独立：TaskPhase回答所处阶段，CancelRequestStatus回答目标是否
// 记录过取消要求，FutureDropStatus回答原异步计算（Future）的合法清理是否已确认。
// 一个名称不复用为另外两项含义，也不以这些值授予轮询（poll）或析构（drop）权限。
//
// 选择三个无载荷枚举（enum），使调用者能按名称区分有限含义，避免多个bool被误换位置，
// 也避免字符串或裸整数额外引入拼写、值域和解释分歧。当前不需要联合结构体（struct）：
// 三个值本身不承担联合快照（snapshot）的采集或合法组合校验；实际任务（Task）的
// 唯一状态维护和查询出口以后负责这些保证。不为描述值增加物理字段（field）、
// 原子操作（atomic operation）、链接、共享持有或回调（callback）。
//
// 同一任务（Task）、同一逻辑时刻的允许组合：
//   TaskPhase    CancelRequestStatus        FutureDropStatus
//   Preparing    NotRecorded 或 Recorded   Unconfirmed
//   Advancing    NotRecorded 或 Recorded   Unconfirmed
//   Exiting      NotRecorded 或 Recorded   Unconfirmed 或 Confirmed
//   Ended        NotRecorded 或 Recorded   Confirmed
// 其余六种组合不允许；此表覆盖当前4×2×2种描述组合，不穷尽实际任务（Task）的所有状态。
// 独立查询得到的不同时刻历史值，不自动成为同一时刻组合。公开构造几个描述值本身
// 不产生不安全操作；约束所针对的是把它们发布为实际任务（Task）同一时刻的真实情况。
//
// 五项派生宏（derive macro）的选择与作用：
// - Clone：显式复制同一描述值；不克隆任务（Task）、原值或共享控制资源。
// - Copy：允许隐式复制无资源的值；不发放执行权、控制权或结果领取权。
// - Debug：提供调试（debug）展示；具体格式不是稳定的数据格式，格式化（formatting）
//   目标可能执行外部代码，其成本和失败不属于普通构造、复制或相等比较的固定成本。
// - PartialEq：只比较相同枚举（enum）的变体（variant），不查询实际任务（Task）。
// - Eq：明确这些无载荷值满足自反的相等关系，不增加状态（state）更新或资源访问。
// 不生成Default、排序、序列化（serialization）或转换方法；不承诺布局（layout）或
// 数值编码。没有资源字段（field），跨线程转移（Send）与共享引用的跨线程安全性（Sync）
// 由编译器（compiler）自动推导；这不改变实际原异步计算（Future）或输出（output）的限制。
// 不设Default，避免把默认构造当成已经观察到的事实；没有当前使用需求支持排序、
// 哈希（hash）或外部编码。以后需要这些能力时应另明确其意义，不能从变体排列推定。
// 不需要不安全代码（unsafe）、手写方法、配对实现文件或未实现占位。

// @trace 2
/// 描述目标任务（Task）是否已经记录取消请求（cancellation request）。
///
/// 本值描述目标已经发生的事实，不描述调用者是否尝试发送请求，
/// 也不说明任务（Task）最终是否因取消（cancellation）退出。
/// 任务（Task）可能在记录请求后，仍从已开始的轮询（poll）返回完成就绪（Ready）。
/// 同一目标已记录的事实不会被后续状态（state）撤销；本值只是历史描述，不会自行更新。
///
/// # 使用示例（Usage）
///
/// ```
/// use pi_async_task::definitions::CancelRequestStatus;
///
/// let description = CancelRequestStatus::Recorded;
/// assert_eq!(description, CancelRequestStatus::Recorded);
/// ```
///
/// 构造本值不会向任何任务（Task）发送请求，也不能证明实际目标已记录请求。
/// 本类型（type）不持有目标、不提供实时查询，也不授予执行或控制权限。
///
/// # 成本与安全（Cost and safety）
///
/// 普通构造、复制（copy）及相等比较均为 O(1)，无需堆分配、锁（lock）、
/// 异步等待（await）或用户回调（callback），不改变实际任务（Task）。
/// 本值可跨线程转移（Send）或通过共享引用在线程（thread）间读取（Sync）。
/// 调试（debug）输出取决于格式化（formatting）目标；不保证其固定成本或无失败。
/// 不保证内存布局（memory layout）、判别值（discriminant）数值或稳定外部编码。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancelRequestStatus {
    /// 目标尚未记录取消请求（cancellation request）。
    ///
    /// 不代表外部没有尝试发送，也不代表目标此后一定会接收。
    NotRecorded,

    /// 目标确已记录取消请求（cancellation request）。
    ///
    /// 不保证当前轮询（poll）已经退出、后续取消（cancellation）必定成功，
    /// 或原异步计算（Future）已经完成析构（drop）。
    Recorded,
}

// 内部设计，不进入公开接口文档（API docs）。
// FutureDropStatus仅表示原值合法析构（drop）完成的正面确认，保留二值即可表达当前需求。
// Unconfirmed有意包含尚未开始、正在进行、完成后尚未发布确认，以及失败后不能给出
// 成功确认的情况；这些情况不被当成可重复析构（drop）的授权。需要更细内部过程时，
// 应由实际拥有清理责任的机制明确定义，不能从这个描述值猜测或补发析构（drop）。
// Confirmed只在原值合法析构（drop）正常完成后发布，之后不回退；失败不得虚报成功。
// 原值清理、输出（output）领取、附加数据及最终存储回收各有边界，本值不将它们合并。
// 在正常完成、取消（cancellation）及其它适用退出路径中使用相同含义；无须为每条路径
// 复制另一份枚举（enum）。本类型（type）和CancelRequestStatus不相互调用或嵌套持有。

// @trace 3
/// 描述原异步计算（Future）的合法析构（drop）是否已经确认完成。
///
/// 本值只描述原异步计算（Future）的清理确认，不说明取消（cancellation）是否成功，
/// 也不说明执行结果（output）、其它共享资源或任务（Task）存储是否已经释放。
/// 正常完成路径和取消（cancellation）路径使用相同含义。
/// 同一目标一旦给出完成确认，就不会回退；本值是历史描述，不会自行更新。
///
/// # 使用示例（Usage）
///
/// ```
/// use pi_async_task::definitions::FutureDropStatus;
///
/// let description = FutureDropStatus::Unconfirmed;
/// assert_ne!(description, FutureDropStatus::Confirmed);
/// ```
///
/// 构造本值不会执行析构（drop），也不能证明实际原值已完成清理。
/// 本类型（type）不持有原值、不提供实时查询，也不授予资源访问或清理权限。
///
/// # 成本与安全（Cost and safety）
///
/// 普通构造、复制（copy）及相等比较均为 O(1)，无需堆分配、锁（lock）、
/// 异步等待（await）或用户回调（callback），不改变实际任务（Task）。
/// 本值可跨线程转移（Send）或通过共享引用在线程（thread）间读取（Sync）。
/// 调试（debug）输出取决于格式化（formatting）目标；不保证其固定成本或无失败。
/// 不保证内存布局（memory layout）、判别值（discriminant）数值或稳定外部编码。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FutureDropStatus {
    /// 尚未确认原异步计算（Future）已经完成合法析构（drop）。
    ///
    /// 包括尚未开始、正在进行、尚未发布完成确认或发生失败的情况；
    /// 不表示原值一定仍然存在，也不允许据此重复析构（drop）。
    Unconfirmed,

    /// 原异步计算（Future）已经完成合法析构（drop），并已发布完成确认。
    ///
    /// 不表示全部任务（Task）责任、外部输入输出（I/O）或剩余存储已经结束。
    Confirmed,
}
