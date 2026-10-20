# spine/relay 第二波验证（2026-10-20，v0.7）

第一波已交付「单跳中继最小可用集」（`docs/spine-relay-验证-2026-10-19.md`）。
本波补齐三件上线前必须有的东西：

1. **多级 spine 级联**：leaf → spine1 → spine2 → leaf（跨网段/跨机房链式）；
2. **节点目录同步**：叶子 `/v1/peers` 能看到非直连节点（`relayed=true` + `via`），
   `/v1/fleet/agents` 的 `node_online` 由目录新鲜度判定；
3. **控制面帧验签**：`agents_update` / `node_dir` 带签名即验，开启
   `security.control_auth` 后无签名一律拒绝（伪造 agent 表/目录不再可行）。

## 1. 拓扑与配置

```
A(leaf) ── R1(spine) ── R2(spine) ── B(leaf)
```

```jsonc
// R1：中继 + 上游级联（相邻 spine 互为 upstream，双向可达）
"relay": { "enabled": true, "upstream": "b00000000002", "max_hops": 3 }
// R2
"relay": { "enabled": true, "upstream": "b00000000001", "max_hops": 3 }
// A / B：叶子只配 spine
"discovery": { "static_peers": ["127.0.0.1:18925"] },
"relay": { "enabled": false, "spine": "b00000000001", "max_hops": 3 }
// 控制面强制验签（缺省 false，滚动升级友好）
"security": { "mode": "strict", "control_auth": true }
```

缺省 `upstream=""` + `control_auth=false` = 第一波语义**逐字节不变**。

## 2. 语义

| 场景 | 行为 |
|---|---|
| spine 找不到目标直连 | 向 `relay.upstream` 兜底转发（**不回弹给入站连接**，防两跳乒乓）；同样 `hops+1` + 本跳 pending |
| 叶子/普通节点出站 | 候选上游依次为 `spine` → `upstream`（去重、排除最终目标）；第一个直连可用者转发 |
| 节点目录帧 | `ctrl/node_dir`：本机直连 peer（`hops=0`）+ 已学到的目录（`hops+1`，超 `max_hops` 不再扩散）；中继每 5s 推送 + 新 peer 注册即推 |
| 目录条目 | 进 `/v1/peers` 时带 `relayed=true` / `via`；直连条目优先，同 node_id 不重复；`online` 由绝对过期时间判定 |
| **绝对过期时间** | 条目携带源头设定的 `expires`（unix 秒）并**原样转发**，只由源头续期。多级环内若各跳按本地接收时间续期，已离线叶子会被环路互相刷新、永不掉线（本次踩到并修复） |
| fleet 在线 | `node_online = 本机 ∪ 直连在线 ∪ 目录新鲜`；节点名/地址也可从目录补齐 |
| 控制面验签 | strict 下：带 `sig` **总是**校验（篡改即拒）；无 `sig` 且 `control_auth=false` 放行（老节点兼容）；`control_auth=true` 时无签名拒绝，并写 `audit_alert`（`control_frame_rejected`） |
| 升级顺序 | `control_auth=true` 先推接收方，再让发送方开启/升级；最坏只影响 agent 表/目录刷新，不影响业务消息 |

## 3. 改动面

- `src/config.rs`：`RelayConfig.upstream`；`SecurityConfig.control_auth`（均 serde default）。
- `src/peer.rs`：`PeerView.relayed` / `PeerView.via`（`skip_serializing_if`，直连条目字节不变）。
- `src/router.rs`：
  - `upstream_candidates` + `send` 级联兜底；`handle_transit` 未知目标向 upstream 转发（排除入站）；
  - `node_dir_envelope` / `sync_push_dir` / `push_dir_to` / `on_node_dir` / `dir_merge` /
    `dir_prune` / `dir_online` / `dir_snapshot`（绝对 `expires`）；
  - `sign_control` / `verify_control`；`agents_update_envelope` 在 strict 下签名；
  - maintenance 每 5s 清理目录，中继节点周期推送。
- `src/conn.rs`：`agents_update` 先验签再入表（拒绝写审计）；新增 `node_dir` 分支
  （验签 → 合并 → 中继扩散）；新 peer 注册后立即推目录。
- `src/api.rs`：`/v1/peers` 合并直连 + 目录视图；`/v1/fleet/agents` 的
  `node_online`/`node_name`/`node_addr` 使用目录兜底。

## 4. 验证

### 单测（`src/router.rs::relay_tests`，11 条；cargo test 137/137）

级联：`upstream` 出站、未知目标兜底、入站即 upstream 时不回弹；控制面：
缺省容忍未签名但带签名必验（篡改拒绝）、`control_auth` 强制签名；目录：
本机/直连不入目录、`relayed/via` 正确、**过期条目不得被视为在线**。

### 冒烟（10/10，`tests/relay_cascade_smoke.py`，端口 18924-18927）

四节点 A–R1–R2–B，全部 `control_auth=true`：

| 场景 | 结论 |
|---|---|
| C0 两级级联组网（每节点只见直连邻居） | PASS |
| C1 A→B 两级转发：B 收原帧 `from=A`、`hops=2` | PASS |
| C2 B→A 回向两级转发 | PASS |
| C3 节点目录：A 的 `/v1/peers` 出现 B/R2（`relayed=true` + `via`） | PASS |
| C4 跨级 fleet：A 看到 B 的 agent `online=true` + `node_name` | PASS |
| C5 未签名 `node_dir` 被拒（不落目录 + 审计日志） | PASS |
| C6 正确签名的 `node_dir` 被接受 | PASS |
| C7 篡改签名被拒 | PASS |
| C8 kill B 后目录 TTL 过期：A 端 B 条目消失、agent 转 offline | PASS |
| C9 剩余节点无崩溃 | PASS |

### 零回归

`cargo fmt --check` 全绿 · `cargo clippy -- -D warnings`（default + mdns）零 ·
`cargo test 137/137`；relay 7/7（第一波）、streaming 5/5、acceptance 5/5
（200/200、P99 356ms）、engine 27/27、tasks 17/17、events 20/20、jsonlog 21/21、
cli 13/13、circuit 3/3、warmpool 6/6、gitops 10/10、webhook 7/7、lease 8/8、
adapter 10/10、persistence 14/14、mtls-net 6/6、mtls-require 13/13、
mtls-ca-dist 12/12、admission 9/9、web-timeline 12/12。

## 5. 边界与残留（第三波候选）

1. **目录分发仍是非持久 gossip**：节点重启后靠 5s 周期重新学习；没有版本号/
   增量同步，中间节点掉线时目录会在 TTL 内保留（30s 收敛，符合预期）。
2. **环内重复转发**：目录帧在 R1↔R2 环内会以递增 `hops` 重复出现到上限
   （有界、不延长 TTL），流量可接受；若要极致可改为「只向未携带该条目的
   邻居发送」的 per-peer 过滤。
3. **控制面验签范围**：目前覆盖 `agents_update` 与 `node_dir`；`heartbeat` /
   `lease` / `ack` 只影响活性观察，不参与调度决策，未纳入（避免给高频帧
   加 HMAC 开销）。`hello` 仍走既有准入 HMAC。
4. **多级级联要求相邻 spine 互为 upstream**（显式配置，避免未知目标在 mesh
   中泛洪）；自动上游发现（按拓扑学习）留待后续。
5. 真 20 机 LAN 与外部用户「5 分钟装完」验收仍待实机。
