// SPDX-License-Identifier: Apache-2.0
//! router 事件钩子。
//!
//! 开源核心 `router` 不再直接引用 `ha`/`chaos`/`export` 等闭源模块，只持有
//! 可选的 [`CoreHook`]（闭源发行版启动时挂载一次，由其运行态实现本 trait）。
//! 钩子方法只做「通知/投影」，不持有任何 Router 锁；Raft 专用时序约定
//! （ha-ticker 线程、inner→applied 锁序等）全部封装在闭源实现内部。
//! 缺省（`hook = None`）= 免费层：不写审计、不注入延迟、不转发导出，
//! 行为与历史版本逐字节一致。

use serde_json::Value;

/// router 侧闭源能力钩子。
pub trait CoreHook: Send + Sync + 'static {
    /// v0.7 §4.4：追加不可变审计记录（actor=角色/peer；detail 不得含密钥）。
    /// 返回分配的 seq；`None` = 未挂载/关闭/链损坏/写入失败（调用方不因此
    /// 改变业务语义）。v0.8 M4：HA 启用时写 `audit` 命名空间（leader 串行、
    /// 多数派提交）；follower 把记录转给 leader——细节全在闭源实现内。
    fn audit_append(&self, event: &str, actor: &str, detail: Value) -> Option<u64>;

    /// v0.7 chaos §6.4：`send` 出口注入的延迟毫秒数；0 = 不注入。
    fn chaos_latency_ms(&self) -> u64;

    /// 任务帧导出旁路是否启用（关闭时跳过 entry 序列化开销）。
    fn export_enabled(&self) -> bool;

    /// 一条已脱敏的任务协议历史条目（JSON）喂给外部导出。
    fn export_frame(&self, entry: Value);
}
