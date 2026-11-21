#![doc = include_str!("../README.md")]
#![doc(html_no_source)]

pub mod all;
pub mod any;
pub mod definitions;
pub mod one;

pub use all::{wait_all, AllSender, WaitAll};
pub use any::{wait_any, AnySender, WaitAny};
pub use definitions::{SendError, SendErrorKind, WaitError};
pub use one::{wait_one, OneSender, WaitOne};
