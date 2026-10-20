//! v0.7 §4.4 不可变审计日志（hash 链 append-only）+ 导出。
//!
//! 每条记录 = `{seq, ts, event, actor, node_id, detail, prev, hash}`；
//! `hash = sha256(prev ‖ "\n" ‖ canonical(body))`，`body` 为除 `prev/hash`
//! 外的字段。链头（head）随每次追加前移，任何一行被改/删/插都会使其后
//! 所有 `prev/hash` 对不上——`GET /v1/audit/verify` 重新读盘校验。
//!
//! 缺省 `security.audit.enabled=false`：不建目录、不落盘、端点返回
//! `enabled=false`，既有行为零变化。
//!
//! 失败策略是 **fail-closed**：启动时链损坏（含崩溃留下的半行）→ 拒绝继续
//! 追加（日志 error），但服务本身照常运行；修复文件后重启恢复。
//! 每次追加 `sync_data`，不做批量攒批（审计量低，换取崩溃后仍可界定尾部）。

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::store::now_wall;

/// 单条审计记录（落盘行）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditRecord {
    pub seq: u64,
    pub ts: i64,
    pub event: String,
    pub actor: String,
    pub node_id: String,
    #[serde(default)]
    pub detail: Value,
    /// 上一条的 hash（genesis 为空串）。
    pub prev: String,
    /// `sha256(prev ‖ "\n" ‖ body)` 的 hex。
    pub hash: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AuditVerify {
    pub enabled: bool,
    pub valid: bool,
    pub count: u64,
    pub head: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

struct Inner {
    file: Option<std::fs::File>,
    head: String,
    count: u64,
    /// 非空 = 链已损坏/文件不可写：拒绝继续追加，直到进程重启修复。
    broken: Option<String>,
}

pub struct AuditLog {
    enabled: bool,
    path: PathBuf,
    node_id: String,
    inner: Mutex<Inner>,
}

impl AuditLog {
    pub fn new(path: PathBuf, enabled: bool, node_id: &str) -> Self {
        let mut inner = Inner {
            file: None,
            head: String::new(),
            count: 0,
            broken: None,
        };
        if enabled {
            if let Some(dir) = path.parent() {
                let _ = std::fs::create_dir_all(dir);
            }
            let v = verify_path(&path, true);
            if v.valid {
                inner.count = v.count;
                inner.head = v.head;
            } else {
                let reason = v.error.unwrap_or_else(|| "unknown".to_string());
                tracing::error!(
                    path = %path.display(),
                    error = %reason,
                    "audit: 启动校验失败，拒绝继续追加（修复文件后重启）"
                );
                inner.broken = Some(reason);
            }
            match OpenOptions::new().create(true).append(true).open(&path) {
                Ok(f) => inner.file = Some(f),
                Err(e) => {
                    tracing::error!(path = %path.display(), error = %e, "audit: 打开审计文件失败");
                    inner.broken = Some(format!("open: {e}"));
                }
            }
        }
        Self {
            enabled,
            path,
            node_id: node_id.to_string(),
            inner: Mutex::new(inner),
        }
    }

    /// 追加一条审计记录（同步 fsync）。返回 seq；关闭/链损坏/写入失败 → None。
    pub fn append(&self, event: &str, actor: &str, detail: Value) -> Option<u64> {
        if !self.enabled {
            return None;
        }
        let mut inner = self.inner.lock().unwrap();
        if let Some(reason) = inner.broken.clone() {
            tracing::error!(event, reason = %reason, "audit: 拒绝追加（链已损坏）");
            return None;
        }
        let seq = inner.count + 1;
        let mut rec = AuditRecord {
            seq,
            ts: now_wall(),
            event: event.to_string(),
            actor: actor.to_string(),
            node_id: self.node_id.clone(),
            detail,
            prev: inner.head.clone(),
            hash: String::new(),
        };
        rec.hash = hash_for(&inner.head, &body_bytes(&rec));
        let line = match serde_json::to_string(&rec) {
            Ok(l) => l,
            Err(e) => {
                tracing::error!(error = %e, "audit: 记录序列化失败");
                return None;
            }
        };
        let Some(file) = inner.file.as_mut() else {
            inner.broken = Some("file not open".to_string());
            return None;
        };
        let wrote = (|| -> std::io::Result<()> {
            writeln!(file, "{line}")?;
            file.sync_data()
        })();
        if let Err(e) = wrote {
            tracing::error!(error = %e, "audit: 写入失败（后续追加将被拒绝）");
            inner.broken = Some(format!("write: {e}"));
            return None;
        }
        inner.head = rec.hash;
        inner.count = seq;
        Some(seq)
    }

    /// 重新读盘校验整条链（外部篡改可被发现）。
    pub fn verify(&self) -> AuditVerify {
        verify_path(&self.path, self.enabled)
    }

    /// 导出最近 `limit` 条记录 + 清单（count/head/valid）。
    pub fn export(&self, limit: usize) -> Value {
        let v = self.verify();
        if !self.enabled {
            return json!({
                "enabled": false,
                "valid": true,
                "count": 0,
                "head": "",
                "records": [],
            });
        }
        let limit = limit.clamp(1, 5000);
        let lines: Vec<String> = std::fs::read_to_string(&self.path)
            .unwrap_or_default()
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(str::to_string)
            .collect();
        let records: Vec<Value> = lines
            .iter()
            .rev()
            .take(limit)
            .rev()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .collect();
        json!({
            "enabled": true,
            "valid": v.valid,
            "count": v.count,
            "head": v.head,
            "error": v.error,
            "records": records,
            "limit": limit,
        })
    }
}

fn body_bytes(rec: &AuditRecord) -> Vec<u8> {
    // serde_json::Value 的 Map 默认 BTreeMap（无 preserve_order feature），
    // json! 宏输出键序确定；同一记录重放时逐字节一致。
    serde_json::to_vec(&json!({
        "seq": rec.seq,
        "ts": rec.ts,
        "event": rec.event,
        "actor": rec.actor,
        "node_id": rec.node_id,
        "detail": rec.detail,
    }))
    .unwrap_or_default()
}

fn hash_for(prev: &str, body: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(prev.as_bytes());
    h.update(b"\n");
    h.update(body);
    hex::encode(h.finalize())
}

/// 读盘校验（不依赖内存状态）。文件不存在 = 空链有效。
pub fn verify_path(path: &Path, enabled: bool) -> AuditVerify {
    if !enabled {
        return AuditVerify {
            enabled: false,
            valid: true,
            count: 0,
            head: String::new(),
            error: None,
        };
    }
    let text = match std::fs::read_to_string(path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return AuditVerify {
                enabled: true,
                valid: true,
                count: 0,
                head: String::new(),
                error: None,
            }
        }
        Err(e) => {
            return AuditVerify {
                enabled: true,
                valid: false,
                count: 0,
                head: String::new(),
                error: Some(format!("read: {e}")),
            }
        }
    };
    let mut count = 0u64;
    let mut head = String::new();
    for (idx, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let rec: AuditRecord = match serde_json::from_str(line) {
            Ok(r) => r,
            Err(e) => {
                return AuditVerify {
                    enabled: true,
                    valid: false,
                    count,
                    head,
                    error: Some(format!("line {}: 解析失败（{e}）", idx + 1)),
                }
            }
        };
        let expect = hash_for(&head, &body_bytes(&rec));
        if rec.seq != count + 1 || rec.prev != head || rec.hash != expect {
            return AuditVerify {
                enabled: true,
                valid: false,
                count,
                head,
                error: Some(format!("line {}: hash 链不匹配（篡改或截断）", idx + 1)),
            };
        }
        count = rec.seq;
        head = rec.hash;
    }
    AuditVerify {
        enabled: true,
        valid: true,
        count,
        head,
        error: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "al-audit-{}-{}-{name}",
            std::process::id(),
            now_wall()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("audit").join("audit.jsonl")
    }

    #[test]
    fn append_chain_verifies_and_export_limits() {
        let path = tmp("chain");
        let log = AuditLog::new(path.clone(), true, "n1");
        assert_eq!(
            log.append("task.submit", "admin", json!({"task_id": "t1"})),
            Some(1)
        );
        assert_eq!(
            log.append("task.cancel", "operator", json!({"task_id": "t1"})),
            Some(2)
        );
        let v = log.verify();
        assert!(v.valid, "{v:?}");
        assert_eq!(v.count, 2);
        assert_eq!(v.head.len(), 64);
        let ex = log.export(1);
        assert_eq!(ex["count"], 2);
        assert_eq!(ex["records"].as_array().unwrap().len(), 1);
        assert_eq!(ex["records"][0]["event"], "task.cancel");
        let _ = std::fs::remove_dir_all(path.parent().unwrap().parent().unwrap());
    }

    #[test]
    fn tamper_is_detected_and_append_refused() {
        let path = tmp("tamper");
        let log = AuditLog::new(path.clone(), true, "n1");
        log.append("task.submit", "admin", json!({"task_id": "t1"}));
        log.append("task.cancel", "admin", json!({"task_id": "t1"}));
        let text = std::fs::read_to_string(&path).unwrap();
        let tampered = text.replace("\"task.cancel\"", "\"task.delete\"");
        assert_ne!(text, tampered);
        std::fs::write(&path, tampered).unwrap();
        let v = log.verify();
        assert!(!v.valid, "篡改必须被发现: {v:?}");
        assert_eq!(v.count, 1, "报告最后一个有效前缀");
        // 重启后进入 fail-closed：拒绝继续追加。
        let again = AuditLog::new(path.clone(), true, "n1");
        assert_eq!(again.append("task.submit", "admin", json!({})), None);
        assert!(!again.verify().valid);
        let _ = std::fs::remove_dir_all(path.parent().unwrap().parent().unwrap());
    }

    #[test]
    fn restart_continues_chain() {
        let path = tmp("restart");
        {
            let log = AuditLog::new(path.clone(), true, "n1");
            log.append("a", "admin", json!({}));
        }
        let log = AuditLog::new(path.clone(), true, "n1");
        assert_eq!(log.append("b", "admin", json!({})), Some(2));
        let v = log.verify();
        assert!(v.valid);
        assert_eq!(v.count, 2);
        let _ = std::fs::remove_dir_all(path.parent().unwrap().parent().unwrap());
    }

    #[test]
    fn disabled_creates_nothing() {
        let path = tmp("disabled");
        let log = AuditLog::new(path.clone(), false, "n1");
        assert_eq!(log.append("x", "admin", json!({})), None);
        assert!(!path.exists());
        let v = log.verify();
        assert!(!v.enabled && v.valid && v.count == 0);
        let _ = std::fs::remove_dir_all(path.parent().unwrap().parent().unwrap());
    }
}
