// SPDX-License-Identifier: Apache-2.0
//! On-disk persistence helpers that back the router's reliability guarantees:
//! an append-only journal for message queues, atomic JSON snapshots for the
//! agent registry, and the rotating conversation-history file. All live under
//! `data_dir` so a node restart never loses in-flight messages.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::proto::Envelope;

/// Unix seconds, used for every persisted timestamp (Instant dies with
/// the process; wall clock survives restarts).
pub fn now_wall() -> i64 {
    OffsetDateTime::now_utc().unix_timestamp()
}

/// One journal line. `op` is `add` or `del`; `q` names the queue:
/// `inbox` | `offline` | `pending` | `local` | `seen`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rec {
    pub op: String,
    pub q: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env: Option<Envelope>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub msg_id: String,
    /// Absolute expiry in unix seconds (offline queue).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exp: Option<i64>,
    /// Retry counter carried across restarts (pending queue).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tries: Option<u32>,
    /// Wall time the record was created, unix seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wall: Option<i64>,
}

impl Rec {
    pub fn add_env(q: &str, env: Envelope) -> Self {
        Rec {
            op: "add".to_string(),
            q: q.to_string(),
            msg_id: env.msg_id.clone(),
            env: Some(env),
            exp: None,
            tries: None,
            wall: Some(now_wall()),
        }
    }
    pub fn add_offline(env: Envelope, exp: i64) -> Self {
        let mut r = Rec::add_env("offline", env);
        r.exp = Some(exp);
        r
    }
    pub fn add_pending(env: Envelope, tries: u32) -> Self {
        let mut r = Rec::add_env("pending", env);
        r.tries = Some(tries);
        r
    }
    pub fn add_id(q: &str, msg_id: &str) -> Self {
        Rec {
            op: "add".to_string(),
            q: q.to_string(),
            msg_id: msg_id.to_string(),
            env: None,
            exp: None,
            tries: None,
            wall: Some(now_wall()),
        }
    }
    pub fn del(q: &str, msg_id: &str) -> Self {
        Rec {
            op: "del".to_string(),
            q: q.to_string(),
            msg_id: msg_id.to_string(),
            env: None,
            exp: None,
            tries: None,
            wall: None,
        }
    }
}

/// Append-only journal with fsync-on-record, plus periodic compaction
/// (rewrite from live in-memory state via temp file + atomic rename).
///
/// v0.7 性能项：`sync_ms > 0` 时切到**批量 fsync**（group commit）——
/// append 只 flush 到 OS 页缓存，后台线程每窗口至多 `sync_all` 一次；
/// 掉电最多丢最后一个窗口，但突发派发不再逐条 fsync（P99 秒级 → 200ms 级）。
/// `sync_ms == 0` 保持既有逐条 fsync 语义（掉电零丢）。
/// `max_records > 0`（仅 `sync_ms > 0` 时有意义）：待刷记录达到封顶条数时
/// append 内联立即 fsync，不等窗口到期（v0.8，缩短突发掉电窗口）。
pub struct Journal {
    path: PathBuf,
    enabled: bool,
    writer: Arc<Mutex<Option<BufWriter<File>>>>,
    /// 0 = 每条 fsync；>0 = 批量窗口毫秒。
    sync_ms: u64,
    /// 单批条数封顶（0 = 关闭）。仅 sync_ms > 0 时生效。
    max_records: u64,
    /// 批处理模式下有「已 flush 未 fsync」的记录待刷。
    dirty: Arc<AtomicBool>,
    /// 已 flush 未 fsync 的记录条数（按条数封顶触发用）。
    pending: Arc<AtomicU64>,
    /// 批处理模式下实际执行的 fsync 次数（单测观测用）。
    syncs: Arc<AtomicU64>,
    /// 后台 sync 线程存活标志（Journal Drop 时退出）。
    alive: Arc<AtomicBool>,
}

impl Journal {
    /// 零窗口简写（=逐条 fsync）；运行期统一走 [`Journal::open_with_sync`]。
    #[allow(dead_code)]
    pub fn open(data_dir: &Path, enabled: bool) -> Self {
        Self::open_with_sync(data_dir, enabled, 0, 0)
    }

    /// `sync_ms == 0` 与 [`Journal::open`] 等价（逐条 fsync）。
    /// `max_records > 0`（仅 `sync_ms > 0` 时有意义）：待刷记录达到封顶条数
    /// 时 append 内联立即 fsync，不等窗口到期。
    pub fn open_with_sync(data_dir: &Path, enabled: bool, sync_ms: u64, max_records: u64) -> Self {
        let writer: Arc<Mutex<Option<BufWriter<File>>>> = Arc::new(Mutex::new(None));
        let dirty = Arc::new(AtomicBool::new(false));
        let alive = Arc::new(AtomicBool::new(true));
        let pending = Arc::new(AtomicU64::new(0));
        let syncs = Arc::new(AtomicU64::new(0));
        if enabled && sync_ms > 0 {
            let w = Arc::clone(&writer);
            let d = Arc::clone(&dirty);
            let a = Arc::clone(&alive);
            let p = Arc::clone(&pending);
            let s = Arc::clone(&syncs);
            let window = std::time::Duration::from_millis(sync_ms.max(1));
            let _ = std::thread::Builder::new()
                .name("journal-sync".into())
                .spawn(move || {
                    while a.load(Ordering::Relaxed) {
                        std::thread::sleep(window);
                        if d.swap(false, Ordering::Relaxed) {
                            p.store(0, Ordering::Relaxed);
                            if let Ok(mut g) = w.lock() {
                                if let Some(wr) = g.as_mut() {
                                    let _ = wr.flush();
                                    if wr.get_mut().sync_all().is_ok() {
                                        s.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                            }
                        }
                    }
                });
        }
        Journal {
            path: data_dir.join("journal.jsonl"),
            enabled,
            writer,
            sync_ms,
            max_records,
            dirty,
            pending,
            syncs,
            alive,
        }
    }

    /// Append one record. `sync_ms == 0`：flush + fsync，掉电零丢（既有语义）。
    /// `sync_ms > 0`：flush 到 OS 页缓存后标记 dirty，由后台线程按窗口 fsync。
    /// On write error we drop the handle and log — the bus stays up.
    pub fn record(&self, r: &Rec) {
        if !self.enabled {
            return;
        }
        let line = match serde_json::to_string(r) {
            Ok(l) => l,
            Err(_) => return,
        };
        let mut guard = self.writer.lock().unwrap();
        if guard.is_none() {
            *guard = Self::open_writer(&self.path);
        }
        let Some(w) = guard.as_mut() else {
            tracing::error!(path = %self.path.display(), "journal: cannot open for append");
            return;
        };
        let res = w
            .write_all(line.as_bytes())
            .and_then(|_| w.write_all(b"\n"))
            .and_then(|_| w.flush());
        if let Err(e) = res {
            tracing::error!(error = %e, path = %self.path.display(), "journal: write failed");
            *guard = None; // reopen on next record
            return;
        }
        if self.sync_ms == 0 {
            if let Err(e) = w.get_mut().sync_all() {
                tracing::error!(error = %e, path = %self.path.display(), "journal: fsync failed");
            }
        } else {
            self.dirty.store(true, Ordering::Relaxed);
            if self.max_records > 0 {
                let n = self.pending.fetch_add(1, Ordering::Relaxed) + 1;
                if n >= self.max_records {
                    self.pending.store(0, Ordering::Relaxed);
                    self.dirty.store(false, Ordering::Relaxed);
                    let _ = w.flush();
                    if let Err(e) = w.get_mut().sync_all() {
                        tracing::error!(error = %e, path = %self.path.display(), "journal: fsync failed");
                    }
                    self.syncs.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}

impl Drop for Journal {
    fn drop(&mut self) {
        self.alive.store(false, Ordering::Relaxed);
        // 收尾：把批处理模式下未 fsync 的记录落盘（进程退出不丢）。
        if let Ok(mut g) = self.writer.lock() {
            if let Some(w) = g.as_mut() {
                let _ = w.flush();
                let _ = w.get_mut().sync_all();
            }
        }
    }
}

impl Journal {
    fn open_writer(path: &Path) -> Option<BufWriter<File>> {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .ok()
            .map(BufWriter::new)
    }

    /// Rewrite the journal from live state (temp file + rename), then reset
    /// the append handle. Called at startup and periodically under churn.
    pub fn compact(&self, recs: &[Rec]) {
        if !self.enabled {
            return;
        }
        let tmp = self.path.with_extension("jsonl.tmp");
        let written = (|| -> std::io::Result<()> {
            let mut f = BufWriter::new(File::create(&tmp)?);
            for r in recs {
                let line = serde_json::to_string(r)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                f.write_all(line.as_bytes())?;
                f.write_all(b"\n")?;
            }
            f.flush()?;
            f.get_mut().sync_all()?;
            Ok(())
        })();
        if written.is_ok() {
            if let Err(e) = std::fs::rename(&tmp, &self.path) {
                tracing::error!(error = %e, "journal: compact rename failed");
            }
        } else {
            tracing::error!(path = %self.path.display(), "journal: compact write failed");
            let _ = std::fs::remove_file(&tmp);
        }
        *self.writer.lock().unwrap() = None; // reopen against the fresh file
    }

    /// Replay every record in order; unreadable lines are skipped (a torn
    /// last line from a hard kill must not brick the node).
    pub fn replay(&self) -> Vec<Rec> {
        if !self.enabled {
            return Vec::new();
        }
        Self::read_lines_impl(&self.path)
            .iter()
            .filter_map(|l| serde_json::from_str::<Rec>(l).ok())
            .collect()
    }

    fn read_lines_impl(path: &Path) -> Vec<String> {
        let Ok(raw) = std::fs::read_to_string(path) else {
            return Vec::new();
        };
        raw.lines()
            .filter(|l| !l.trim().is_empty())
            .map(String::from)
            .collect()
    }
}

/// Write `value` as pretty JSON atomically (temp file + rename + fsync).
///
/// Temp 名带进程号+序号：配置/快照可能被并发保存（如改名与 /v1/config
/// 同时落盘），固定 `.tmp` 会让两个写者互相截断。
pub fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> std::io::Result<()> {
    use std::sync::atomic::AtomicU64;
    static TMP_SEQ: AtomicU64 = AtomicU64::new(0);
    let file_name = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "state.json".to_string());
    let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let tmp = path.with_file_name(format!("{file_name}.tmp.{}.{seq}", std::process::id()));
    let raw = serde_json::to_string_pretty(value)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let mut f = File::create(&tmp)?;
    f.write_all(raw.as_bytes())?;
    f.write_all(b"\n")?;
    f.flush()?;
    f.sync_all()?;
    std::fs::rename(&tmp, path)
}

/// Read a JSON snapshot, returning `None` on any damage (the node must
/// still boot — an empty state is better than a crash loop).
pub fn read_json_opt<T: DeserializeOwned>(path: &Path) -> Option<T> {
    let raw = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&raw).ok()
}

// ---- conversation history ---------------------------------------------------

/// Sub-directory of `data_dir` that holds rolled-over conversation history.
pub const HISTORY_ARCHIVE_DIR: &str = "history_archive";

/// Append one history entry (a JSON line) to `data_dir/<file>`.
///
/// The active file is *rolled over*, never truncated: once it exceeds
/// `max_mb`, or is older than `archive_days` days, it is moved into
/// `history_archive/` with a timestamped name and a fresh active file is
/// started. Conversation records are therefore never deleted by the node.
pub fn append_history(data_dir: &Path, file: &str, line: &str, max_mb: u64, archive_days: u64) {
    let path = data_dir.join(file);
    if let Ok(meta) = path.metadata() {
        let cap = max_mb.max(1) * 1024 * 1024;
        let by_age = archive_days > 0
            && meta
                .modified()
                .ok()
                .map(|m| {
                    let secs = m
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs() as i64)
                        .unwrap_or(0);
                    now_wall().saturating_sub(secs) >= (archive_days * 86_400) as i64
                })
                .unwrap_or(false);
        if meta.len() >= cap || by_age {
            let _ = rotate_history(data_dir, file);
        }
    }
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&path) {
        let _ = f.write_all(line.as_bytes());
        let _ = f.write_all(b"\n");
    }
}

/// Move the active history file into `history_archive/` as
/// `history-YYYYMMDD-HHMMSS.jsonl`. Returns the archive file name.
///
/// A rename failure leaves the active file untouched (we prefer an oversized
/// log over losing records); the next append retries.
pub fn rotate_history(data_dir: &Path, file: &str) -> Option<String> {
    let src = data_dir.join(file);
    if !src.is_file() || src.metadata().map(|m| m.len() == 0).unwrap_or(true) {
        return None;
    }
    let adir = data_dir.join(HISTORY_ARCHIVE_DIR);
    std::fs::create_dir_all(&adir).ok()?;
    // Legacy single-archive scheme (`<file>.1`): fold it into the archive dir.
    std::fs::rename(
        data_dir.join(format!("{file}.1")),
        adir.join("history-legacy.jsonl"),
    )
    .ok();
    let now = OffsetDateTime::from(
        std::fs::metadata(&src)
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::now()),
    );
    let stem = format!(
        "history-{:04}{:02}{:02}-{:02}{:02}{:02}.jsonl",
        now.year(),
        now.month() as u8,
        now.day(),
        now.hour(),
        now.minute(),
        now.second()
    );
    let mut name = stem.clone();
    let mut i = 1;
    while adir.join(&name).exists() {
        name = stem.replace(".jsonl", &format!("-{i}.jsonl"));
        i += 1;
    }
    let dst = adir.join(&name);
    std::fs::rename(&src, &dst).ok()?;
    let _ = std::fs::File::create(src); // start the new active file
    Some(name)
}

/// Prune archives down to `keep` newest files (keep = 0 means keep everything —
/// the node never throws conversation records away unless the operator opts in).
pub fn prune_archives(data_dir: &Path, keep: usize) -> usize {
    if keep == 0 {
        return 0;
    }
    let mut files = list_archives(data_dir);
    files.retain(|f| {
        f.get("name")
            .and_then(|v| v.as_str())
            .map(|n| !n.starts_with("export-"))
            .unwrap_or(true)
    });
    let excess = files.len().saturating_sub(keep);
    for doomed in files.into_iter().take(excess) {
        if let Some(n) = doomed.get("name").and_then(|v| v.as_str()) {
            let _ = std::fs::remove_file(data_dir.join(HISTORY_ARCHIVE_DIR).join(n));
        }
    }
    excess
}

/// Archive files (oldest first) as `{name, bytes, mtime}` objects.
pub fn list_archives(data_dir: &Path) -> Vec<serde_json::Value> {
    let adir = data_dir.join(HISTORY_ARCHIVE_DIR);
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(&adir) else {
        return out;
    };
    for ent in rd.flatten() {
        let name = ent.file_name().to_string_lossy().to_string();
        if !name.ends_with(".jsonl") {
            continue;
        }
        let Ok(m) = ent.metadata() else { continue };
        let mt = m
            .modified()
            .ok()
            .map(|t| {
                let d = OffsetDateTime::from(t);
                format!(
                    "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                    d.year(),
                    d.month() as u8,
                    d.day(),
                    d.hour(),
                    d.minute(),
                    d.second()
                )
            })
            .unwrap_or_default();
        out.push(serde_json::json!({ "name": name, "bytes": m.len(), "mtime": mt }));
    }
    out.sort_by(|a, b| a["mtime"].as_str().cmp(&b["mtime"].as_str()));
    out
}

/// Filters for `history_search` (all optional; empty string = match anything).
#[derive(Debug, Clone, Default)]
pub struct HistSearch {
    /// sender: node id / name / IP or agent id (substring, case-insensitive)
    pub from: String,
    /// receiver, same semantics as `from`
    pub to: String,
    /// exact sender node id (console endpoint dropdowns)
    pub from_node: String,
    /// exact sender agent id (combined with `from_node`)
    pub from_agent: String,
    /// exact receiver node id
    pub to_node: String,
    /// exact receiver agent id (combined with `to_node`)
    pub to_agent: String,
    /// matches either side
    pub who: String,
    pub direction: String,
    pub kind: String,
    /// substring of the JSON payload
    pub q: String,
    /// UTC ISO-8601 bounds, compared on the first 19 characters
    pub since: String,
    pub until: String,
    pub limit: usize,
    /// total bytes to scan before giving up (newest-first, so recent hits stay cheap)
    pub max_bytes: u64,
    /// also scan archive files (newest archive first)
    pub scope_all: bool,
}

fn contains_ci(hay: &str, needle: &str) -> bool {
    hay.to_lowercase().contains(needle)
}

/// Stream-scan the history JSONL newest-first with filters. Files are read
/// backwards in 512 KiB chunks, so a filtered query against a multi-GB log
/// answers as soon as `limit` matches are found near the tail. Read-only —
/// never touches the files. `stopped` says why scanning ended:
/// `"eof" | "limit" | "budget" | "older"` (walked past `since`).
pub fn history_search(data_dir: &Path, file: &str, f: &HistSearch) -> serde_json::Value {
    let norm19 = |s: &str| -> String { s.chars().take(19).collect() };
    let since = norm19(&f.since);
    let until = norm19(&f.until);
    let (q_from, q_to) = (f.from.to_lowercase(), f.to.to_lowercase());
    let (q_who, q_dir) = (f.who.to_lowercase(), f.direction.to_lowercase());
    let (q_kind, q_q) = (f.kind.to_lowercase(), f.q.to_lowercase());
    let mut out: Vec<serde_json::Value> = Vec::new();
    let mut scanned: u64 = 0;
    let mut stopped = "eof";

    let mut paths: Vec<PathBuf> = vec![data_dir.join(file)];
    if f.scope_all {
        for a in list_archives(data_dir).into_iter().rev() {
            if let Some(n) = a["name"].as_str() {
                paths.push(data_dir.join(HISTORY_ARCHIVE_DIR).join(n));
            }
        }
    }

    let (q_fn, q_fag, q_tn, q_tag) = (
        f.from_node.to_lowercase(),
        f.from_agent.to_lowercase(),
        f.to_node.to_lowercase(),
        f.to_agent.to_lowercase(),
    );
    let mfrom_pair = |fr: &str, fa: &str| -> bool {
        (q_fn.is_empty() || fr.to_lowercase() == q_fn)
            && (q_fag.is_empty() || fa.to_lowercase() == q_fag)
    };
    let mto_pair = |tto: &str, ta: &str| -> bool {
        (q_tn.is_empty() || tto.to_lowercase() == q_tn)
            && (q_tag.is_empty() || ta.to_lowercase() == q_tag)
    };
    let mut handle = |line: &str| -> bool {
        // Returns false when scanning must stop (limit reached / older than since).
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
            return true; // not JSON (legacy raw line): ignore for structured search
        };
        let ts = norm19(v["ts"].as_str().unwrap_or(""));
        if !until.is_empty() && ts.as_str() > until.as_str() {
            return true; // newer than the window, keep walking backwards
        }
        if !since.is_empty() && !ts.is_empty() && ts.as_str() < since.as_str() {
            stopped = "older";
            return false; // everything further back is older still
        }
        let fr = v["from"].as_str().unwrap_or("");
        let fa = v["from_agent"].as_str().unwrap_or("");
        let tto = v["to"].as_str().unwrap_or("");
        let ta = v["to_agent"].as_str().unwrap_or("");
        let mfrom = (q_from.is_empty() || contains_ci(fr, &q_from) || contains_ci(fa, &q_from))
            && mfrom_pair(fr, fa);
        let mto = (q_to.is_empty() || contains_ci(tto, &q_to) || contains_ci(ta, &q_to))
            && mto_pair(tto, ta);
        let mwho = q_who.is_empty()
            || contains_ci(fr, &q_who)
            || contains_ci(fa, &q_who)
            || contains_ci(tto, &q_who)
            || contains_ci(ta, &q_who);
        let mdir = q_dir.is_empty() || v["direction"].as_str().unwrap_or("") == q_dir;
        let mkind = q_kind.is_empty() || v["kind"].as_str().unwrap_or("") == q_kind;
        let mq = q_q.is_empty() || contains_ci(&v["payload"].to_string(), &q_q);
        if mfrom && mto && mwho && mdir && mkind && mq {
            out.push(v);
            if out.len() >= f.limit {
                stopped = "limit";
                return false;
            }
        }
        true
    };

    'files: for path in &paths {
        let Ok(meta) = std::fs::metadata(path) else {
            continue;
        };
        let mut pos = meta.len();
        if pos == 0 {
            continue;
        }
        // `carry` = bytes of a line whose head lies further LEFT (older); it is
        // joined with the previous chunk's tail on the next iteration.
        let mut carry: Vec<u8> = Vec::new();
        loop {
            if pos == 0 {
                if !carry.is_empty() {
                    let s = String::from_utf8_lossy(&carry).trim().to_string();
                    if !s.is_empty() && !handle(&s) {
                        break 'files;
                    }
                }
                break;
            }
            let n = pos.min(1 << 19);
            pos -= n;
            let mut buf = vec![0u8; n as usize];
            let mut fh = match File::open(path) {
                Ok(x) => x,
                Err(_) => break,
            };
            if fh.seek(SeekFrom::Start(pos)).is_err() || fh.read_exact(&mut buf).is_err() {
                break;
            }
            scanned += n;
            if scanned > f.max_bytes {
                stopped = "budget";
                break 'files;
            }
            if !carry.is_empty() {
                buf.extend_from_slice(&carry);
            }
            carry.clear();
            let parts: Vec<&[u8]> = buf.split(|b| *b == b'\n').collect();
            if parts.len() == 1 {
                carry = buf; // window sits inside one enormous line
                continue;
            }
            // last piece is terminated (by the newline that opened `carry`, or EOF)
            let last = parts[parts.len() - 1];
            if !last.is_empty() {
                let s = String::from_utf8_lossy(last).trim().to_string();
                if !s.is_empty() && !handle(&s) {
                    break 'files;
                }
            }
            let hi = parts.len() - 2;
            let lo = if pos > 0 { 1 } else { 0 };
            for i in (lo..=hi).rev() {
                if parts[i].is_empty() {
                    continue;
                }
                let s = String::from_utf8_lossy(parts[i]).trim().to_string();
                if !s.is_empty() && !handle(&s) {
                    break 'files;
                }
            }
            if pos > 0 {
                carry = parts[0].to_vec();
            }
        }
    }

    serde_json::json!({
        "ok": true,
        "count": out.len(),
        "matches": out,
        "scanned_bytes": scanned,
        "stopped": stopped,
        "limit": f.limit,
    })
}

/// Identity colors for the web console:
/// `{ "node:<id>": "#rrggbb", "agent:<node>|<agent>": "#rrggbb" }`.
/// Missing keys fall back to a stable hash color computed in the browser, so
/// the console is colorful with zero configuration; overrides persist on the
/// node and are shared by every viewer.
pub const COLORS_FILE: &str = "colors.json";

pub fn load_colors(data_dir: &Path) -> serde_json::Map<String, serde_json::Value> {
    std::fs::read(data_dir.join(COLORS_FILE))
        .ok()
        .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default()
}

pub fn save_colors(
    data_dir: &Path,
    map: &serde_json::Map<String, serde_json::Value>,
) -> std::io::Result<()> {
    write_json_atomic(
        &data_dir.join(COLORS_FILE),
        &serde_json::Value::Object(map.clone()),
    )
}

/// Size/footprint summary for the web console (metadata only — cheap).
pub fn history_stats(
    data_dir: &Path,
    file: &str,
    max_mb: u64,
    archive_days: u64,
    archive_keep: usize,
) -> serde_json::Value {
    let active = data_dir.join(file);
    let active_bytes = std::fs::metadata(&active).map(|m| m.len()).unwrap_or(0);
    let archives = list_archives(data_dir);
    let archive_bytes: u64 = archives.iter().filter_map(|a| a["bytes"].as_u64()).sum();
    serde_json::json!({
        "file": file,
        "active_bytes": active_bytes,
        "rotate_mb": max_mb,
        "archive_days": archive_days,
        "archive_keep": archive_keep,
        "archive_dir": data_dir.join(HISTORY_ARCHIVE_DIR).to_string_lossy(),
        "archives": archives,
        "archive_count": archives.len(),
        "archive_bytes": archive_bytes,
        "total_bytes": active_bytes + archive_bytes,
    })
}

/// Files to export, oldest first: every archive, then the active file.
pub fn history_export_files(data_dir: &Path, file: &str, scope: &str) -> Vec<PathBuf> {
    let mut out = Vec::new();
    match scope {
        "active" => out.push(data_dir.join(file)),
        other if other.starts_with("archive:") => {
            if let Some(name) = crate::file::sanitize_name(&other[8..]) {
                let p = data_dir.join(HISTORY_ARCHIVE_DIR).join(name);
                if p.is_file() {
                    out.push(p);
                }
            }
        }
        _ => {
            for a in list_archives(data_dir) {
                if let Some(n) = a["name"].as_str() {
                    let p = data_dir.join(HISTORY_ARCHIVE_DIR).join(n);
                    if p.is_file() {
                        out.push(p);
                    }
                }
            }
            out.push(data_dir.join(file));
        }
    }
    out.retain(|p| p.is_file());
    out
}

/// Render one history JSONL line as a readable text line. Falls back to the
/// raw line when it cannot be parsed (an export must never drop records).
pub fn history_line_to_text(line: &str) -> String {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
        return line.to_string();
    };
    let s = |k: &str| v[k].as_str().unwrap_or("").to_string();
    let ts = s("ts");
    let when: String = ts
        .strip_suffix('Z')
        .map(|t| t.replace('T', " ").chars().take(19).collect())
        .unwrap_or(ts.clone());
    let dirv = s("direction");
    let dir = match dirv.as_str() {
        "in" => "收",
        "out" => "发",
        "local" => "本",
        other => other,
    };
    let mut who = format!("{} → {}", short_node(&s("from")), short_node(&s("to")));
    let fa = s("from_agent");
    let ta = s("to_agent");
    if !fa.is_empty() || !ta.is_empty() {
        who.push_str(&format!(
            " [{} → {}]",
            if fa.is_empty() { "-" } else { &fa },
            if ta.is_empty() { "-" } else { &ta }
        ));
    }
    let body = match s("kind").as_str() {
        "file" => {
            let name = v["payload"]["file_name"].as_str().unwrap_or("?");
            let size = v["payload"]["size"].as_u64().unwrap_or(0);
            format!("📎 {name} ({size} B)")
        }
        _ => {
            let c = v["payload"]["content"].as_str();
            match c {
                Some(t) => t.replace('\n', " ⏎ "),
                None => v["payload"].to_string(),
            }
        }
    };
    let id = s("msg_id");
    format!(
        "{when} [{dir}] {who} | {body} | #{}",
        &id[..id.len().min(8)]
    )
}

fn short_node(raw: &str) -> String {
    if raw.is_empty() {
        return "*".into();
    }
    if raw.len() <= 12 {
        raw.to_string()
    } else {
        raw.chars().take(12).collect()
    }
}

/// Load up to `needed` history lines (jsonl strings), newest last.
/// Reads the tail window of the main file first, then the archive.
pub fn tail_history(data_dir: &Path, file: &str, needed: usize) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for name in [file.to_string(), format!("{file}.1")] {
        let path = data_dir.join(&name);
        if out.len() >= needed {
            break;
        }
        let lines = tail_lines(&path, needed);
        let take = needed - out.len();
        let mut part: Vec<String> = lines.into_iter().rev().collect();
        part.truncate(take);
        part.reverse();
        out.splice(0..0, part);
    }
    out
}

/// Last `n` complete lines of a file, read from a bounded tail window so a
/// big archive does not slow the boot.
fn tail_lines(path: &Path, n: usize) -> Vec<String> {
    let Ok(mut f) = File::open(path) else {
        return Vec::new();
    };
    let Ok(len) = f.metadata().map(|m| m.len()) else {
        return Vec::new();
    };
    if len == 0 {
        return Vec::new();
    }
    // Generous window: history lines carry payloads, 8 MB keeps ~thousands.
    let window = len.min(8 * 1024 * 1024);
    let start = len - window;
    if f.seek(SeekFrom::Start(start)).is_err() {
        return Vec::new();
    }
    let mut buf = vec![0u8; window as usize];
    let got = match f.read(&mut buf) {
        Ok(g) => g,
        Err(_) => return Vec::new(),
    };
    buf.truncate(got);
    let mut text = match String::from_utf8(buf) {
        Ok(t) => t,
        Err(e) => String::from_utf8_lossy(e.as_bytes()).to_string(),
    };
    if start > 0 {
        // Drop the first (possibly torn) line of the window.
        if let Some(nl) = text.find('\n') {
            text = text[nl + 1..].to_string();
        }
    }
    let mut lines: Vec<String> = text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(String::from)
        .collect();
    if lines.len() > n {
        lines.drain(0..lines.len() - n);
    }
    lines
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(tag: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("agentlink-store-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        dir
    }

    #[test]
    fn journal_batch_mode_writes_all_records_and_flushes_on_drop() {
        let dir = scratch("batch");
        {
            let j = Journal::open_with_sync(&dir, true, 20, 0);
            for i in 0..50 {
                j.record(&Rec::add_id("inbox", &format!("m-{i:03}")));
            }
            // 批处理模式 append 即 flush 到 OS 页缓存：进程存活期间可读全量
            // （fsync 只是掉电语义，不影响文件可见性）。
            let alive = std::fs::read_to_string(dir.join("journal.jsonl")).unwrap();
            assert_eq!(alive.lines().count(), 50, "append 后 50 行应立即可见");
        }
        // Drop 后文件完整（后台线程 + 收尾 flush 不丢行）。
        let text = std::fs::read_to_string(dir.join("journal.jsonl")).unwrap();
        assert_eq!(text.lines().count(), 50);
        assert!(text.contains("m-000") && text.contains("m-049"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn journal_zero_window_keeps_per_record_sync_semantics() {
        let dir = scratch("sync");
        let j = Journal::open_with_sync(&dir, true, 0, 0);
        for i in 0..5 {
            j.record(&Rec::add_id("offline", &format!("s-{i}")));
        }
        let text = std::fs::read_to_string(dir.join("journal.jsonl")).unwrap();
        assert_eq!(text.lines().count(), 5, "零窗口=逐条落盘，随时可读");
        drop(j);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn journal_disabled_writes_nothing() {
        let dir = scratch("off");
        let j = Journal::open_with_sync(&dir, false, 20, 0);
        j.record(&Rec::add_id("inbox", "x"));
        drop(j);
        assert!(
            !dir.join("journal.jsonl").exists(),
            "persist=false 不创建 journal"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn journal_max_records_caps_batch_sync() {
        let dir = scratch("maxbatch");
        {
            // 巨大窗口（100s）+ 3 条封顶：达到条数即内联 fsync，不等窗口。
            let j = Journal::open_with_sync(&dir, true, 100_000, 3);
            for i in 0..3 {
                j.record(&Rec::add_id("inbox", &format!("c-{i}")));
            }
            assert_eq!(
                j.syncs.load(Ordering::Relaxed),
                1,
                "第 3 条应触发立即 fsync"
            );
            assert_eq!(j.pending.load(Ordering::Relaxed), 0, "封顶触发后计数清零");
            for i in 0..2 {
                j.record(&Rec::add_id("inbox", &format!("d-{i}")));
            }
            assert_eq!(j.syncs.load(Ordering::Relaxed), 1, "不足封顶条数不应额外刷");
            assert_eq!(j.pending.load(Ordering::Relaxed), 2);
            let text = std::fs::read_to_string(dir.join("journal.jsonl")).unwrap();
            assert_eq!(
                text.lines().count(),
                5,
                "全部记录立即可读（flush 到页缓存）"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn journal_max_records_zero_keeps_window_only_semantics() {
        let dir = scratch("maxbatch-off");
        {
            // 缺省 max_records=0：巨大窗口内不做任何 fsync（v0.7 语义不变）。
            let j = Journal::open_with_sync(&dir, true, 100_000, 0);
            for i in 0..10 {
                j.record(&Rec::add_id("inbox", &format!("w-{i}")));
            }
            assert_eq!(j.syncs.load(Ordering::Relaxed), 0, "0 = 关闭，只按窗口刷");
            let text = std::fs::read_to_string(dir.join("journal.jsonl")).unwrap();
            assert_eq!(text.lines().count(), 10);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
