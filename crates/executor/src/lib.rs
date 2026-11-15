//! 默认执行器（executor）的基础能力。
//!
//! 当前批次提供统一时钟（clock）、线程通知和限时停放（thread parking）的完整壳（shell）。
//! 执行器本体、来源选择、任务接纳、策略和运行时装配仍须接续；本库当前不能作为可运行服务。
//! 详细实现由配对文件的todo!给出，本版本可以编译，不表示占位行为已实现。
pub mod clock;
pub mod parking;

pub use clock::{ExecutorClock, ManualClock};
pub use parking::{ThreadNotifier, ThreadParkResult, ThreadParker};
