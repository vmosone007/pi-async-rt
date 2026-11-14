//! 独立于运行时（runtime）的单次交值等待。
//!
//! [`wait_one`] 建立唯一交值关系；[`wait_any`] 接受多个交值句柄（sender handle）
//! 中最先被接受的值；[`wait_all`] 等待全部有效交值句柄分别结束。
//! 构造函数同步返回等待值与交值句柄，等待值实现标准异步协议（Future）。
//!
//! 交值方法 `send` 消耗句柄（handle），失败时返还原值和原因。
//! 放弃等待会停止接收，尚存的交值句柄可安全释放；本库不创建任务（Task）、
//! 不选择执行器（executor），也不提供定时器（timer）。
//!
//! # 当前可用范围
//!
//! 本版本已声明公开接口（public API），等待行为仍由未实现占位（todo!）承载。
//! 完整编译（build）不表示等待、通知或并发清理行为已实现。

#![doc(html_no_source)]

pub mod all;
pub mod any;
pub mod definitions;
pub mod one;

pub use all::{wait_all, AllSender, WaitAll};
pub use any::{wait_any, AnySender, WaitAny};
pub use definitions::{SendError, SendErrorKind, WaitError};
pub use one::{wait_one, OneSender, WaitOne};
