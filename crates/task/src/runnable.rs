//! 创建与唯一可执行句柄（runnable handle），不实现调度策略。
use crate::control::ControlHandle;
use crate::definitions::*;
use pi_result::RawResult as Result;
use crate::join::{JoinHandle, LocalOutput, OutputMode, SendOutput};
use crate::task::{Schedule, TaskOptions, TaskRef, TaskRegistration, TaskThread};
use std::future::Future;

// Runnable只有一份执行安排责任，不实现Clone/Copy；跨线程转交只移动透明持有，
// 原F地址不变。原值访问必须另外验证TaskThread归属，不因Runnable为Send而放宽。
// 创建阶段F:Send且输出Send可先无所属域；本地创建先建立owner清理登记才发布句柄。
// 正常Drop经TaskRef::abandon_runnable归还执行责任，字段随后只自动归还一次强引用。
// 未接纳且从未poll的初始持有为Abandoned；已接纳或已直接执行的放弃为Cancelled；
// 已定原因不覆盖。未绑定Send原值可当前线程清理，绑定后只交原线程清理。
// 已绑定的首次放弃经Schedule交出同一任务的清理Runnable，不隐式接纳/业务poll；
// 退出中的清理Runnable再Drop不得重新schedule，原线程登记保住剩余责任。
// run一次性移出TaskRef为普通局部值；成功/展开归还它，明确拒绝移回原Runnable，
// 不调用Runnable::drop，不把正常执行误记为Abandoned或再次归还issued。
// 不能等普通ControlHandle全部释放才drop F，不能由外线程最后引用drop本地F。
// core与本模块仅因Schedule/QueueEntry须承接Runnable存在固定的同层类型回返；
// 不增加独立生命周期事实。分配、计数和状态修改均经TaskRef窄出口。

/// 唯一待执行持有；移动（move）不复制任务（Task），不支持Clone。
/// 调用run前后均不能另借出F；进入队列（queue）后由队列保有本份值。
#[must_use = "须执行、移交或按明确退出责任释放可执行句柄"]
// @trace 100
pub struct Runnable {
    pub(crate) task: TaskRef,
}

/// 创建成功的三类权限；创建本身不提交（submission）、接纳（admission）或轮询（poll）。
#[must_use = "创建与提交/执行分离；须处理本份可执行持有"]
// @trace 101
pub struct TaskParts<T, M: Send + Sync + 'static = (), K: OutputMode = SendOutput> {
    /// 唯一可执行持有；可交给合法执行组织。
    pub runnable: Runnable,
    /// 共享查询与控制，不能领取输出（output）或访问原值。
    pub control: ControlHandle,
    /// 唯一结果领取权；释放它不自动取消任务（Task）。
    pub join: JoinHandle<T, M, K>,
}

/// 创建失败时返还全部原输入；未创建可观察的半成品任务（Task）。
// @trace 102
pub struct BuildError<F, S, M> {
    /// 未被接管的原异步计算（Future）。
    pub future: F,
    /// 未被接管的原交付实现。
    pub schedule: S,
    /// 未被接管的原附加数据。
    pub metadata: M,
    /// 原创建选项，不静默丢失观测接收者。
    pub options: TaskOptions,
    /// 创建失败的有限分类。
    pub cause: BuildErrorKind,
}

/// 执行被拒绝时返还原Runnable，不能把拒绝当作已经完成。
// @trace 103
pub struct RunError {
    /// 保持原任务（Task）身份和未处理执行责任。
    pub runnable: Runnable,
    /// 本次拒绝原因。
    pub cause: RunErrorKind,
}

// @trace 1
impl Runnable {
    /// 取得原任务（Task）固定身份，O(1)，不增加持有。
    pub fn id(&self) -> TaskId {
        runnable_id_inner(self)
    }
}

// @trace 2
impl Runnable {
    /// 将任务（Task）交给指定所属域管理；接纳（admission）与实际入队（enqueue）分离。
    /// 明确拒绝不消费本句柄、不自动取消，调用方可重试或另作合法安排。
    pub fn admit(&self, owner: &TaskThread) -> Result<TaskRegistration, AdmissionError> {
        runnable_admit_inner(self, owner)
    }
}

// @trace 3
impl Runnable {
    /// 消费本份执行持有，至多直接轮询（poll）一次，或只做合法退出清理。
    /// 返回Pending前完成必要交接；外部Schedule负责真正排队。拒绝须返还原持有。
    pub fn run(self, owner: &TaskThread) -> Result<RunOutcome, RunError> {
        runnable_run_inner(self, owner)
    }
}

// @trace 4
impl Runnable {
    /// 取得基本状态快照（snapshot），不报告物理队列位置。
    pub fn snapshot(&self) -> TaskSnapshot {
        runnable_snapshot_inner(self)
    }
}

// @trace 5
impl Runnable {
    /// 按创建时类型（type）借用附加数据，类型不匹配返回None；不转移所有权。
    pub fn metadata<M: Send + Sync + 'static>(&self) -> Option<&M> {
        runnable_metadata_inner(self)
    }
}

// @trace 6
impl Drop for Runnable {
    /// 放弃本份唯一执行持有；按任务（Task）当前归属合法退出，不直接轮询（poll）。
    /// 未接纳（admission）且从未轮询（poll）的初始放弃保留Abandoned分类。
    /// 已绑定者交回原线程清理，不等待其它控制句柄释放；本次返回不代表清理已经完成。
    /// 未绑定且满足Send的原异步计算（Future）可在本次调用析构（drop）。
    /// 用户析构（drop）或调度接口（Schedule）的恐慌（panic）继续向外传播。
    fn drop(&mut self) {
        runnable_drop_inner(self)
    }
}

// @trace 7
/// 创建可跨线程提交的任务（Task）；输入及输出必须满足Send。
/// 分配失败返还原输入；没有同步poll、隐式提交或运行时依赖。
pub fn create<F, S, M>(
    future: F,
    schedule: S,
    metadata: M,
    options: TaskOptions,
) -> Result<TaskParts<F::Output, M, SendOutput>, BuildError<F, S, M>>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
    S: Schedule,
    M: Send + Sync + 'static,
{
    create_inner(future, schedule, metadata, options)
}

// @trace 8
/// 在指定所属线程创建本地任务（Task），允许原值和输出不满足Send。
/// 先建立合法清理责任；绑定线程不等于已被执行器（executor）接纳。
pub fn create_local<F, S, M>(
    owner: &TaskThread,
    future: F,
    schedule: S,
    metadata: M,
    options: TaskOptions,
) -> Result<TaskParts<F::Output, M, LocalOutput>, BuildError<F, S, M>>
where
    F: Future + 'static,
    F::Output: 'static,
    S: Schedule,
    M: Send + Sync + 'static,
{
    create_local_inner(owner, future, schedule, metadata, options)
}

include!("runnable_inner.rs");
