//! 异步任务（Task）的基础类型（type）。
//!
//! 当前提供 [`definitions::TaskPhase`]、[`definitions::CancelRequestStatus`]
//! 和 [`definitions::FutureDropStatus`]，分别描述生命周期（lifecycle）阶段、
//! 取消请求（cancellation request）是否已记录，以及原异步计算（Future）的
//! 合法析构（drop）是否已确认完成。
//! 这些描述值不持有异步计算（Future），也不提供执行、取消（cancellation）或排队能力。
//! 单独构造描述值不能证明某个实际任务（Task）已经到达相应情况。

#![doc(html_no_source)]

pub mod definitions;
