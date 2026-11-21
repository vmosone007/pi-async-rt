#![doc = include_str!("../README.md")]
#![doc(html_no_source)]
pub mod clock;
pub mod parking;
/// 核心计时能力与默认可取消定时来源的静态接入。
pub mod timer;
pub mod state;
pub mod registry;
mod returns;
pub mod routing;
pub mod drive;
pub mod policy;
pub mod hooks;
pub mod local;
pub mod handle;
pub mod engine;

pub use clock::{ExecutorClock, ManualClock};
pub use parking::{ThreadNotifier, ThreadParkResult, ThreadParker};
pub use timer::{DefaultTimerAccess, DefaultTimerDriver, DefaultTimerRegistration, ExecutorTimer, LocalTimerHandle, LocalTimerSubmit};
pub use drive::{AdmissionRule,AdmissionState,ExecutorOptions,TimerDelivery,TurnResult};
pub use policy::{DefaultIdlePolicy,ExecutorSnapshot,IdlePolicy};
pub use hooks::{ExecutorHooks,ExecutorEvent,TaskAction,TaskInfo,NoHooks};
pub use local::ExecutorLocal;
pub use handle::ExecutorHandle;
pub use engine::{DefaultExecutor,DriveLease};
