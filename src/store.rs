//! On-disk persistence helpers that back the router's reliability guarantees:
//! an append-only journal for message queues, atomic JSON snapshots for the
//! agent registry, and the rotating conversation-history file. All live under
//! `data_dir` so a node restart never loses in-flight messages.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

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
pub struct Journal {
    path: PathBuf,
    enabled: bool,
    writer: Mutex<Option<BufWriter<File>>>,
}

impl Journal {
    pub fn open(data_dir: &Path, enabled: bool) -> Self {
        Journal {
            path: data_dir.join("journal.jsonl"),
            enabled,
            writer: Mutex::new(None),
        }
    }

    /// Append one record; flush + fsync so a power cut cannot lose it.
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
            .and_then(|_| w.flush())
            .and_then(|_| w.get_mut().sync_all());
        if let Err(e) = res {
            tracing::error!(error = %e, path = %self.path.display(), "journal: write failed");
            *guard = None; // reopen on next record
        }
    }

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
pub fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> std::io::Result<()> {
    let file_name = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "state.json".to_string());
    let tmp = path.with_file_name(format!("{file_name}.tmp"));
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

/// Append one history entry (a JSON line) to `data_dir/<file>`, rotating the
/// file to `<file>.1` (keeping exactly one archive) once it exceeds the cap.
pub fn append_history(data_dir: &Path, file: &str, line: &str, max_mb: u64) {
    let path = data_dir.join(file);
    let cap = max_mb.max(1) * 1024 * 1024;
    if let Ok(meta) = path.metadata() {
        if meta.len() >= cap {
            let archive = format!("{file}.1");
            let apath = data_dir.join(&archive);
            let _ = std::fs::remove_file(&apath);
            if std::fs::rename(&path, &apath).is_err() {
                // Rotate failed: truncate instead of growing without bound.
                let _ = std::fs::write(&path, "");
            }
        }
    }
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&path) {
        let _ = f.write_all(line.as_bytes());
        let _ = f.write_all(b"\n");
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
