// SPDX-License-Identifier: Apache-2.0
//! AgentLink 开源核心：发现、Mesh 传输、可靠投递、文件、
//! 本地 API 与星图控制台。依赖方向铁律：本 crate 永不引用闭源发行版代码
//! （类型、trait 实现、路由、配置字段）；闭源能力经 `api::bridge::MeshBridge`
//! 与 `core_hook::CoreHook` 两个投影 trait 反向挂载。

pub mod api;
pub mod cli;
pub mod config;
pub mod conn;
pub mod core_hook;
pub mod discovery;
pub mod extensions;
pub mod file;
pub mod logging;
#[cfg(feature = "mdns")]
pub mod mdns;
pub mod peer;
pub mod proto;
pub mod redact;
pub mod router;
pub mod store;
pub mod trace;
pub mod wizard;
