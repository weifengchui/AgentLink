# journal group commit 验证（2026-10-19，v0.7 性能项）

验收门仿真暴露的持久化瓶颈：`delivery.persist=true` 时 journal **每条记录**
`flush + sync_all`（v0.5 起的「掉电零丢」承诺）。200 帧突发派发 = 200 次
fsync，单节点串行落盘把 P99 调度延迟推到秒级（实测 ~2.0-2.6s）。

本包提供**可调批量刷盘**（group commit），缺省行为不变。

## 1. 语义与配置

```jsonc
"delivery": {
  "persist": true,
  "journal_sync_ms": 0     // 0=逐条 fsync（缺省，掉电零丢，既有行为）
}
```

| `journal_sync_ms` | append 行为 | fsync 时机 | 掉电语义 |
|---|---|---|---|
| `0`（缺省） | write + flush | **每条**立即 `sync_all` | 零丢（行为与 v0.5-v0.6 完全一致） |
| `>0`（如 50） | write + flush 到 OS 页缓存 | 后台 `journal-sync` 线程每窗口至多一次；`Drop` 收尾强刷 | **最多丢最后一个窗口**（如 ≤50ms 的记录） |

- 进程崩溃（非掉电）不丢：flush 已进 OS 页缓存，重启可读；
- 掉电/内核崩溃：窗口内未 fsync 的记录可能丢（OS 页缓存未落盘）；
- 窗口越小越安全、吞吐越接近逐条模式；建议生产 20~100ms（`agentlink.json`
  显式声明，缺省保持零丢——**拿性能必须显式换取**）。

## 2. 改动面

- `src/config.rs`：`DeliveryConfig.journal_sync_ms`（serde default 0，老配置零破坏）。
- `src/store.rs`：`Journal` 改为 `Arc<Mutex<BufWriter<File>>>` + `dirty`/`alive`
  原子标志；`open_with_sync(dir, enabled, sync_ms)`；批量模式后台线程按窗口
  fsync；`Drop` 收尾 flush+fsync；`record()` 零窗口路径与旧实现逐字节同义。
- `src/router.rs`：`Journal::open_with_sync(..., cfg.delivery.journal_sync_ms)`。
- 单测 +3（store）：批量模式 50 行全可见 + Drop 后完整、零窗口逐条可读、
  persist=false 不建 journal → `cargo test 126/126`。

## 3. 验证（10 节点 / 200 任务，`tests/acceptance_sim.py`）

| 配置 | P50 | P99 | 结论 |
|---|---|---|---|
| `persist=false`（内存队列，原验收门口径） | 176ms | **234ms** | PASS（<500ms） |
| `persist=true, journal_sync_ms=0`（逐条 fsync） | ~1.1-1.4s | **~2.0-2.6s**（多次运行） | 超门（fsync 代价，已知边界） |
| `persist=true, journal_sync_ms=50`（group commit） | 287ms | **395ms** | **PASS（<500ms）且 200/200 零丢失** |

复现：

```bash
# 内存队列口径（缺省验收）
python3 tests/acceptance_sim.py
# 持久化 + 批量刷盘口径
ACCEPT_PERSIST=1 ACCEPT_SYNC_MS=50 python3 tests/acceptance_sim.py
```

持久化回归：`tests/persistence_smoke.py` 14 检查 0 失败（含重启后
journal 续写 12→15 行）。

## 4. 边界与残留

- 掉电窗口语义只在真机断电/内核崩溃时体现，仿真无法注入；当前以「Drop 收尾
  必刷 + 单测验证文件完整性」+ 语义文档覆盖。
- 未做「按条数封顶」批（如 max 64 条立即刷）——窗口模式下突发 200 帧会在
  一个窗口内全部 flush 到页缓存，fsync 次数=窗口数（≤4 次/200ms），已满足
  验收门。如未来需更短丢数据窗口，可加 `journal_sync_max_records`。
