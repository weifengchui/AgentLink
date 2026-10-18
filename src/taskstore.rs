//! AgentMesh 任务持久存储（v0.6 M1 / 设计 §2.1）。
//!
//! 落在 `data_dir/tasks/` 子目录，两件套（完全复用 `store.rs` 的持久化模式，不引新存储）：
//! - `tasks/journal.jsonl`：append-only 事件日志，行形状对齐 `store::Rec`——
//!   `{op:"up"|"del", task_id, rev, rec?}`，`rec` 为整行 TaskRecord JSON。
//!   写入走 `Journal::record` 同款逐条 fsync；`rebuild()` 容忍断线尾行。
//! - `tasks/state.json`：`write_json_atomic` 全量快照（task_id → TaskRecord 表）。
//!
//! 重启恢复 = 快照 + journal 尾部增量重放；`snapshot_save()` 成功后清扫已落入
//! 快照的 journal 段（等同 `Journal::compact` 的原子重写，此处重写为空文件）。
//!
//! 本模块只存 `serde_json::Value`（任务原文 spec 未脱敏，字段透传），不依赖
//! `taskengine.rs` 的 TaskRecord 领域类型——避免跨文件类型耦合；rev/幂等/终态
//! 清扫策略由引擎侧决定，存储层按 journal 顺序"后写覆盖"落表。
#![allow(dead_code)] // M1 休眠骨架：P1-2/P1-5（taskengine/api 接线）合入后可移除

use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::store::{now_wall, write_json_atomic};

/// journal 单行事件。`op` 为 `"up"`（整行覆盖）或 `"del"`（清扫移除）；
/// `rev` 为引擎维护的任务修订号（存储层只透传，不做比较）；
/// `rec` 仅 `up` 携带：整行 TaskRecord 的 JSON。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEvent {
    pub op: String,
    pub task_id: String,
    #[serde(default)]
    pub rev: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rec: Option<Value>,
    /// 事件落盘时刻（unix 秒），仅诊断用，重放不依赖。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wall: Option<i64>,
}

impl TaskEvent {
    /// 写入/覆盖一行任务记录。
    pub fn up(task_id: &str, rev: u64, rec: Value) -> Self {
        TaskEvent {
            op: "up".to_string(),
            task_id: task_id.to_string(),
            rev,
            rec: Some(rec),
            wall: Some(now_wall()),
        }
    }
    /// 清扫一行任务（终态任务按留存策略删除时由引擎发此事件）。
    pub fn del(task_id: &str, rev: u64) -> Self {
        TaskEvent {
            op: "del".to_string(),
            task_id: task_id.to_string(),
            rev,
            rec: None,
            wall: None,
        }
    }
}

/// 把一条事件应用到内存表：up 整行覆盖、del 移除、未知 op 容忍跳过（前向兼容）。
fn apply_event(records: &mut HashMap<String, Value>, ev: &TaskEvent) {
    match ev.op.as_str() {
        "up" => {
            if let Some(rec) = &ev.rec {
                records.insert(ev.task_id.clone(), rec.clone());
            }
        }
        "del" => {
            records.remove(&ev.task_id);
        }
        other => {
            tracing::warn!(op = other, task_id = %ev.task_id, "taskstore: 忽略未知 journal op");
        }
    }
}

/// 读取 JSON 快照；任何损坏返回 `None`（对齐 `store::read_json_opt`：
/// 空表启动好过崩溃循环）。taskstore 内部用，故不导出。
fn read_json_opt<T: DeserializeOwned>(path: &Path) -> Option<T> {
    let raw = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&raw).ok()
}

/// 内存态 + 追加句柄，统一收在一把锁下：
/// `append` 与 `snapshot_save` 在此锁上互斥，天然获得并发 append 串行化，
/// 且快照捕获与 journal 清扫之间不会插入新事件（否则清扫会丢尾部）。
struct Inner {
    writer: Option<BufWriter<File>>,
    records: HashMap<String, Value>,
}

/// 任务表持久存储。生命周期：`open` →（启动时）`rebuild` → 运行期
/// `append` 落事件 → 定期/退出前 `snapshot_save`（顺带 compact journal）。
pub struct TaskStore {
    dir: PathBuf,
    journal_path: PathBuf,
    state_path: PathBuf,
    inner: Mutex<Inner>,
}

impl TaskStore {
    /// 在 `data_dir/tasks/` 下建目录并打开存储；不自动重放（启动流程由调用方
    /// 决定何时 `rebuild`，便于引擎先接线观测再灌表）。
    pub fn open(data_dir: &Path) -> std::io::Result<Self> {
        let dir = data_dir.join("tasks");
        std::fs::create_dir_all(&dir)?;
        Ok(TaskStore {
            journal_path: dir.join("journal.jsonl"),
            state_path: dir.join("state.json"),
            dir,
            inner: Mutex::new(Inner {
                writer: None,
                records: HashMap::new(),
            }),
        })
    }

    /// 存储子目录（诊断/测试用）。
    #[allow(dead_code)] // 预留给 P1-5 API 层与诊断接线
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// 追加一条事件：序列化 → 写行 → flush → fsync（对齐 `Journal::record`，
    /// 掉电不丢）；成功后同步应用到内存表。写失败时丢句柄下次重开并返回 Err，
    /// 内存表保持不动（引擎可据此拒绝后续状态推进）。
    pub fn append(&self, ev: &TaskEvent) -> std::io::Result<()> {
        let line = serde_json::to_string(ev)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let mut guard = self.inner.lock().unwrap();
        if guard.writer.is_none() {
            guard.writer = Self::open_writer(&self.journal_path);
        }
        let Some(w) = guard.writer.as_mut() else {
            tracing::error!(path = %self.journal_path.display(), "taskstore: journal 无法打开追加");
            return Err(std::io::Error::other("cannot open task journal"));
        };
        let res = w
            .write_all(line.as_bytes())
            .and_then(|_| w.write_all(b"\n"))
            .and_then(|_| w.flush())
            .and_then(|_| w.get_mut().sync_all());
        match res {
            Ok(()) => {
                apply_event(&mut guard.records, ev);
                Ok(())
            }
            Err(e) => {
                tracing::error!(error = %e, path = %self.journal_path.display(), "taskstore: journal 写入失败");
                guard.writer = None; // 下次 append 重开句柄
                Err(e)
            }
        }
    }

    /// 全量快照落 `state.json`（原子写），成功后把 journal 原子重写为空文件——
    /// 已落入快照的事件段就此清扫，journal 永远只保留"快照之后"的增量。
    /// 终态任务的留存清扫是引擎语义：先 `append(del)` 再调用本函数即可。
    pub fn snapshot_save(&self) -> std::io::Result<()> {
        let mut guard = self.inner.lock().unwrap();
        // 先落快照：state.json 完整覆盖当前表，此后 journal 可安全清空。
        let table: BTreeMap<&str, &Value> =
            guard.records.iter().map(|(k, v)| (k.as_str(), v)).collect();
        write_json_atomic(&self.state_path, &table)?;
        // 再清 journal：临时空文件 + fsync + 原子 rename（同 `Journal::compact` 手法）。
        let tmp = self.journal_path.with_extension("jsonl.tmp");
        let reset = (|| -> std::io::Result<()> {
            let f = File::create(&tmp)?;
            f.sync_all()?;
            std::fs::rename(&tmp, &self.journal_path)?;
            Ok(())
        })();
        if let Err(e) = reset {
            tracing::error!(error = %e, path = %self.journal_path.display(), "taskstore: journal 清扫失败");
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
        guard.writer = None; // 对着新空文件重开追加句柄
        Ok(())
    }

    /// 重启重建：读 `state.json` 快照（损坏视为空表），再按序重放 journal
    /// 尾部增量；不可解析的行（硬杀留下的断线尾行）跳过不炸。
    /// 返回重建后的全量表，同时装入内存供 `records()` 使用。
    pub fn rebuild(&self) -> std::io::Result<HashMap<String, Value>> {
        let mut records: HashMap<String, Value> =
            read_json_opt(&self.state_path).unwrap_or_default();
        let lines = match std::fs::read_to_string(&self.journal_path) {
            Ok(raw) => raw,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(e) => return Err(e),
        };
        for line in lines.lines().filter(|l| !l.trim().is_empty()) {
            match serde_json::from_str::<TaskEvent>(line) {
                Ok(ev) => apply_event(&mut records, &ev),
                Err(_) => {
                    // 对齐 `Journal::replay`：断线尾行/坏行容忍跳过。
                    tracing::warn!(path = %self.journal_path.display(), "taskstore: 跳过不可解析的 journal 行");
                }
            }
        }
        let mut guard = self.inner.lock().unwrap();
        guard.writer = None; // 重放后句柄作废，下次 append 重开
        guard.records = records.clone();
        Ok(records)
    }

    /// 当前全量 TaskRecord 表（task_id → 记录 JSON）。
    pub fn records(&self) -> HashMap<String, Value> {
        self.inner.lock().unwrap().records.clone()
    }

    /// 单行记录查询（预留给 P1-5 `GET /v1/tasks/{task_id}`）。
    #[allow(dead_code)]
    pub fn record(&self, task_id: &str) -> Option<Value> {
        self.inner.lock().unwrap().records.get(task_id).cloned()
    }

    /// journal 现存事件数（诊断/测试用）。
    #[allow(dead_code)]
    pub fn journal_len(&self) -> usize {
        std::fs::read_to_string(&self.journal_path)
            .map(|raw| raw.lines().filter(|l| !l.trim().is_empty()).count())
            .unwrap_or(0)
    }

    fn open_writer(path: &Path) -> Option<BufWriter<File>> {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .ok()
            .map(BufWriter::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 独立临时目录（对齐 `config.rs` 测试风格，无 tempfile 依赖）。
    fn temp_data_dir(tag: &str) -> PathBuf {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "agentlink-taskstore-{}-{}-{}",
            tag,
            std::process::id(),
            n
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    fn rec(task_id: &str, state: &str) -> Value {
        serde_json::json!({"task_id": task_id, "state": state, "spec": {"raw": task_id}})
    }

    /// roundtrip：up/up/del 事件序列落盘后，新实例 rebuild 得到最终表。
    #[test]
    fn task_journal_roundtrip() {
        let data = temp_data_dir("roundtrip");
        {
            let store = TaskStore::open(&data).unwrap();
            assert!(store
                .append(&TaskEvent::up("t1", 1, rec("t1", "pending")))
                .is_ok());
            assert!(store
                .append(&TaskEvent::up("t1", 2, rec("t1", "running")))
                .is_ok());
            assert!(store
                .append(&TaskEvent::up("t2", 1, rec("t2", "pending")))
                .is_ok());
            assert!(store.append(&TaskEvent::del("t2", 2)).is_ok());
            let mem = store.records();
            assert_eq!(mem.len(), 1);
            assert_eq!(mem["t1"]["state"], "running"); // 后写覆盖
        }
        let store2 = TaskStore::open(&data).unwrap();
        let rebuilt = store2.rebuild().unwrap();
        assert_eq!(rebuilt.len(), 1);
        assert_eq!(rebuilt["t1"]["state"], "running");
        assert!(!rebuilt.contains_key("t2"));
        assert_eq!(store2.journal_len(), 4);
        let _ = std::fs::remove_dir_all(&data);
    }

    /// 断线尾行容忍：journal 末行是被硬杀截断的半截 JSON，重建不炸、丢尾行。
    #[test]
    fn torn_tail_replay() {
        let data = temp_data_dir("torn");
        std::fs::create_dir_all(data.join("tasks")).unwrap();
        let journal = data.join("tasks").join("journal.jsonl");
        let mut body = String::new();
        body.push_str(
            &serde_json::to_string(&TaskEvent::up("t1", 1, rec("t1", "pending"))).unwrap(),
        );
        body.push('\n');
        body.push_str(
            &serde_json::to_string(&TaskEvent::up("t2", 1, rec("t2", "running"))).unwrap(),
        );
        body.push('\n');
        // 模拟掉电时写了一半的行：合法前缀，无换行、JSON 不完整
        body.push_str(
            "{\"op\":\"up\",\"task_id\":\"t3\",\"rev\":1,\"rec\":{\"task_id\":\"t3\",\"stat",
        );
        std::fs::write(&journal, body).unwrap();
        let store = TaskStore::open(&data).unwrap();
        let rebuilt = store.rebuild().unwrap();
        assert_eq!(rebuilt.len(), 2);
        assert_eq!(rebuilt["t2"]["state"], "running");
        assert!(!rebuilt.contains_key("t3"));
        // 断线尾行不能挡后续写入
        assert!(store
            .append(&TaskEvent::up("t4", 1, rec("t4", "pending")))
            .is_ok());
        let _ = std::fs::remove_dir_all(&data);
    }

    /// 原子快照：state.json 与内存表等值；rename 后不留 .tmp；重启读回一致。
    #[test]
    fn snapshot_atomic() {
        let data = temp_data_dir("snap");
        {
            let store = TaskStore::open(&data).unwrap();
            store
                .append(&TaskEvent::up("a", 1, rec("a", "pending")))
                .unwrap();
            store
                .append(&TaskEvent::up("b", 1, rec("b", "running")))
                .unwrap();
            store.snapshot_save().unwrap();
            let state_path = data.join("tasks").join("state.json");
            let on_disk: HashMap<String, Value> = read_json_opt(&state_path).unwrap();
            assert_eq!(on_disk, store.records());
            assert!(!state_path.with_extension("json.tmp").exists());
            assert!(!data.join("tasks").join("journal.jsonl.tmp").exists());
        }
        let store2 = TaskStore::open(&data).unwrap();
        assert_eq!(store2.rebuild().unwrap().len(), 2);
        let _ = std::fs::remove_dir_all(&data);
    }

    /// compact 清扫：snapshot_save 后 journal 只剩"快照之后"的增量；
    /// 终态 del 落入快照后不再出现在 journal。
    #[test]
    fn compact_drops_snapshot_segments() {
        let data = temp_data_dir("compact");
        let store = TaskStore::open(&data).unwrap();
        for i in 0..5 {
            store
                .append(&TaskEvent::up(
                    &format!("t{i}"),
                    1,
                    rec(&format!("t{i}"), "terminal_succeeded"),
                ))
                .unwrap();
        }
        store.append(&TaskEvent::del("t0", 2)).unwrap(); // 终态清扫
        assert_eq!(store.journal_len(), 6);
        store.snapshot_save().unwrap();
        assert_eq!(store.journal_len(), 0); // 已落快照的段全部清掉
        assert_eq!(store.records().len(), 4);
        // 快照之后的新事件照常进 journal（增量重放语义）
        store
            .append(&TaskEvent::up("t9", 1, rec("t9", "pending")))
            .unwrap();
        assert_eq!(store.journal_len(), 1);
        let store2 = TaskStore::open(&data).unwrap();
        let rebuilt = store2.rebuild().unwrap();
        assert_eq!(rebuilt.len(), 5); // 快照 4 行 + 尾部增量 1 行
        assert_eq!(rebuilt["t9"]["state"], "pending");
        assert!(!rebuilt.contains_key("t0"));
        let _ = std::fs::remove_dir_all(&data);
    }

    /// 并发 append 串行化：多线程同时灌事件，journal 行数不多不少、
    /// 无交错坏行，内存表与日志一致。
    #[test]
    fn concurrent_append_serialized() {
        let data = temp_data_dir("concurrent");
        let store = std::sync::Arc::new(TaskStore::open(&data).unwrap());
        const THREADS: usize = 4;
        const PER_THREAD: usize = 10;
        let mut handles = Vec::new();
        for t in 0..THREADS {
            let store = store.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..PER_THREAD {
                    let id = format!("w{t}-{i}");
                    store
                        .append(&TaskEvent::up(&id, 1, rec(&id, "pending")))
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(store.records().len(), THREADS * PER_THREAD);
        assert_eq!(store.journal_len(), THREADS * PER_THREAD);
        // 每行都可解析（写未被交错撕开）
        let raw = std::fs::read_to_string(data.join("tasks").join("journal.jsonl")).unwrap();
        let parsed: Vec<TaskEvent> = raw
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str::<TaskEvent>(l).expect("line intact"))
            .collect();
        assert_eq!(parsed.len(), THREADS * PER_THREAD);
        let store2 = TaskStore::open(&data).unwrap();
        assert_eq!(store2.rebuild().unwrap().len(), THREADS * PER_THREAD);
        let _ = std::fs::remove_dir_all(&data);
    }

    /// 未知 op 与缺 rec 的 up 行：容忍跳过，不污染表。
    #[test]
    fn unknown_op_and_bad_up_ignored() {
        let data = temp_data_dir("junk");
        std::fs::create_dir_all(data.join("tasks")).unwrap();
        std::fs::write(
            data.join("tasks").join("journal.jsonl"),
            "{\"op\":\"weird\",\"task_id\":\"t1\",\"rev\":1,\"rec\":{\"x\":1}}\n\
             {\"op\":\"up\",\"task_id\":\"t2\",\"rev\":1}\n\
             {\"op\":\"up\",\"task_id\":\"t3\",\"rev\":1,\"rec\":{\"state\":\"running\"}}\n",
        )
        .unwrap();
        let store = TaskStore::open(&data).unwrap();
        let rebuilt = store.rebuild().unwrap();
        assert_eq!(rebuilt.len(), 1);
        assert!(rebuilt.contains_key("t3"));
        let _ = std::fs::remove_dir_all(&data);
    }
}
