//! 指标基础能力的私有命名空间。
//!
//! Counts／Error是已验收纯值叶；身份叶只供核心冷存储，业务调度不调用上层。
//! 其它子模块按MET-IMPLEMENTATION-001单向协作；只有api被重导出为公开命名空间。
//! 默认构建不登记，外部同步采集才读取／投递；不通过结果获得核心可写权限。

mod counts;
mod sample_error;
pub(crate) mod identity;
#[cfg(not(target_arch = "wasm32"))]
mod model;
#[cfg(not(target_arch = "wasm32"))]
mod source;
#[cfg(not(target_arch = "wasm32"))]
mod registry;
#[cfg(not(target_arch = "wasm32"))]
mod reporter;
#[cfg(not(target_arch = "wasm32"))]
pub mod api;
