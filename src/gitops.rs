//! GitOps 目录驱动（v0.6 P1，设计案 §8.3：watch 仓库 tasks/ 目录自动 apply）。
//!
//! 工作方式（单二进制形态下不引入 git/notify 依赖，纯轮询）：
//! - `gitops.enabled=true` + `gitops.dir=<仓库内 tasks/ 目录>` 时，每个
//!   `interval_secs`（默认 15s）扫描目录（含一层子目录）里的 `*.json`；
//! - 文件**内容 sha256 变化 / 新文件** → 经 `reconcile::apply_task_spec` 入引擎
//!   表（与 `POST /v1/tasks` 同一内核口径：必填门/非终态 409/journal/快照）；
//! - 解析失败 → 告警一次并**记**观察（同一坏内容不蝉联告警；作者改好内容后
//!   指纹变化自然重报）；journal 失败 → 告警并记观察，下一拍重试；
//! - 文件删除 → 清除观察项（**不删除任务**：任务的生命周期由 DAG/取消管，
//!   不因文件消失而回撤——删任务是对 API/CLI 的显式操作）。
//!
//! 兼容性：缺省 `enabled=false`（段可整体缺失）= 完全惰性；观察状态纯内存，
//! 重启后全量重扫靠 apply 幂等收敛（未清扫的重投=409，终态=粘性），无副作用。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;

use sha2::{Digest, Sha256};
use tokio::sync::watch;

use crate::api::SharedState;
use crate::reconcile::{apply_task_spec, ApplyFail};

/// 观察状态：文件路径 → 上次成功读取的内容指纹。
pub struct ScanState {
    seen: Mutex<HashMap<String, u128>>,
}

impl Default for ScanState {
    fn default() -> Self {
        Self {
            seen: Mutex::new(HashMap::new()),
        }
    }
}

impl ScanState {
    pub fn new() -> Self {
        Self::default()
    }
}

fn file_hash(bytes: &[u8]) -> u128 {
    let digest = Sha256::digest(bytes);
    u128::from_le_bytes(digest.as_slice()[..16].try_into().unwrap())
}

/// 一轮扫描捡到的条目。
enum Picked {
    /// 新/变内容且 JSON 可解析 → 待 apply。
    Spec(PathBuf, serde_json::Value),
    /// JSON 解析失败 → 告警（不记观察，改好后下一拍重试）。
    Unparseable(PathBuf, String),
}

/// 扫描一轮（纯函数，供 run 循环与单测）：根层 + 一层子目录的 `*.json`；
/// 与 `seen` 里指纹相同的内容跳过；被删除的文件条目顺带清除。
/// 目录不可读 → 当作空轮（告警在外层做）。
fn scan_dir(dir: &Path, seen: &mut HashMap<String, u128>) -> Vec<Picked> {
    let mut jsons: Vec<PathBuf> = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for entry in rd.flatten() {
            let p = entry.path();
            if p.is_dir() {
                if let Ok(sub) = std::fs::read_dir(&p) {
                    for e2 in sub.flatten() {
                        let p2 = e2.path();
                        if p2.extension().and_then(|x| x.to_str()) == Some("json") {
                            jsons.push(p2);
                        }
                    }
                }
            } else if p.extension().and_then(|x| x.to_str()) == Some("json") {
                jsons.push(p);
            }
        }
    }
    jsons.sort();

    let mut out: Vec<Picked> = Vec::new();
    for path in &jsons {
        let Ok(bytes) = std::fs::read(path) else {
            // 读取竞态（作者正在保存）：跳过，下一拍重来。
            continue;
        };
        let h = file_hash(&bytes);
        let key = path.to_string_lossy().to_string();
        if seen.get(&key) == Some(&h) {
            continue;
        }
        match serde_json::from_slice::<serde_json::Value>(&bytes) {
            Ok(spec) => {
                seen.insert(key, h);
                out.push(Picked::Spec(path.clone(), spec));
            }
            Err(e) => {
                // 记指纹：同一坏内容只告警一次；作者改内容（指纹变）后自然重报。
                seen.insert(key, h);
                out.push(Picked::Unparseable(path.clone(), e.to_string()));
            }
        }
    }
    // 顺带清除已删除文件的观察项（删除语义 = 不再由本声明源管辖）。
    let live: std::collections::HashSet<String> = jsons
        .iter()
        .map(|p| p.to_string_lossy().to_string())
        .collect();
    seen.retain(|k, _| live.contains(k));
    out
}

/// GitOps 出站循环。
pub fn spawn(state: SharedState, shutdown: watch::Receiver<bool>) {
    tokio::spawn(async move {
        let cfg = state.config.gitops.clone();
        if !cfg.enabled {
            return;
        }
        let Some(mesh) = state.mesh.clone() else {
            tracing::error!("gitops: mesh 未启用（license 无效或 mesh.enabled=false）——目录驱动不可用，未 spawn");
            return;
        };
        let Some(dir) = cfg.dir.clone() else {
            tracing::error!("gitops: enabled 但未配置 gitops.dir——未 spawn");
            return;
        };
        tracing::info!(
            dir = %dir.display(),
            interval_secs = cfg.interval_secs,
            "gitops: started (watch repo tasks/ dir, apply on change)"
        );
        let seen = ScanState::new();
        let mut ticker = tokio::time::interval(Duration::from_secs(cfg.interval_secs.max(1)));
        let mut shutdown = shutdown;
        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = ticker.tick() => {
                    // v0.8 M4：只有 leader 执行 apply（避免多节点重复落任务）。
                    if state.ha.as_ref().is_some_and(|ha| !ha.is_leader()) {
                        continue;
                    }
                    let mut observed = seen.seen.lock().unwrap();
                    for picked in scan_dir(&dir, &mut observed) {
                        match picked {
                            Picked::Spec(path, spec) => {
                                let tid = spec
                                    .get("task_id")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("?")
                                    .to_string();
                                match apply_task_spec(&mesh, &spec) {
                                    Ok(st) => tracing::info!(
                                        file = %path.display(),
                                        task_id = %tid,
                                        state = %st,
                                        "gitops: applied task spec"
                                    ),
                                    Err(ApplyFail::Conflicting(cur)) => tracing::debug!(
                                        file = %path.display(),
                                        task_id = %tid,
                                        state = %cur,
                                        "gitops: task already present (non-terminal), skipped"
                                    ),
                                    Err(ApplyFail::DuplicateSeq) => tracing::trace!(
                                        file = %path.display(), task_id = %tid,
                                        "gitops: terminal row re-apply (idempotent sticky/409)"
                                    ),
                                    Err(e) => tracing::warn!(
                                        file = %path.display(),
                                        task_id = %tid,
                                        error = ?e,
                                        "gitops: apply rejected (file kept, retry next scan)"
                                    ),
                                }
                            }
                            Picked::Unparseable(path, why) => tracing::warn!(
                                file = %path.display(),
                                error = %why,
                                "gitops: task file is not valid JSON; kept unapplied (fix the file to retry)"
                            ),
                        }
                    }
                }
            }
        }
        tracing::info!("gitops: stopped");
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::GitopsConfig;

    /// 搭一个迷你 tasks 目录。
    struct Repo {
        root: PathBuf,
    }
    impl Repo {
        fn new(tag: &str) -> Self {
            let root = std::env::temp_dir().join(format!("agentlink-gitops-test-{tag}"));
            let _ = std::fs::remove_dir_all(&root);
            std::fs::create_dir_all(root.join("sub")).unwrap();
            Repo { root }
        }
        fn write(&self, rel: &str, body: &str) {
            let p = self.root.join(rel);
            std::fs::write(&p, body).unwrap();
        }
    }
    impl Drop for Repo {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    fn spec(id: &str) -> String {
        format!(r#"{{"task_id":"{id}","type":"text"}}"#)
    }

    #[test]
    fn scan_pick_change_and_stability() {
        let repo = Repo::new("scan1");
        repo.write("a.json", &spec("g1"));
        repo.write("sub/b.json", &spec("g2"));

        let mut seen = HashMap::new();
        let first = scan_dir(&repo.root, &mut seen);
        // 首轮：两个文件都捡（含一层子目录）。
        assert_eq!(first.len(), 2);
        assert!(first.iter().all(|p| matches!(p, Picked::Spec(_, _))));

        // 不变 → 静默；变 a.json → 只捡 a.json；新增 c.json → 只捡它。
        assert!(scan_dir(&repo.root, &mut seen).is_empty());
        repo.write("a.json", &spec("g1-changed")); // 内容变（仍是合法 spec）
        repo.write("c.json", r#"{"task_id":"g3","type":"text"}"#);
        let third = scan_dir(&repo.root, &mut seen);
        assert_eq!(third.len(), 2);
        assert!(
            scan_dir(&repo.root, &mut seen).is_empty(),
            "同内容重扫不再捡"
        );
    }

    #[test]
    fn scan_unparseable_and_removal() {
        let repo = Repo::new("scan2");
        repo.write("bad.json", "not json at all");
        let mut seen = HashMap::new();
        let first = scan_dir(&repo.root, &mut seen);
        // 坏文件：告警一次（Unparseable），同内容不重报。
        assert!(matches!(first[0], Picked::Unparseable(..)));
        assert!(
            scan_dir(&repo.root, &mut seen).is_empty(),
            "坏内容不再蝉联告警"
        );
        // 修好 → 重新捡取。
        repo.write("bad.json", &spec("g4"));
        assert_eq!(scan_dir(&repo.root, &mut seen).len(), 1);

        // 删除文件 → 观察项被清；重建同名文件再捡来当新内容。
        std::fs::remove_file(repo.root.join("bad.json")).unwrap();
        scan_dir(&repo.root, &mut seen);
        repo.write("bad.json", &spec("g4-v2"));
        assert_eq!(scan_dir(&repo.root, &mut seen).len(), 1);
    }

    #[test]
    fn scan_missing_dir_is_quiet() {
        let mut seen = HashMap::new();
        let quiet = scan_dir(&Path::new("/nonexistent-gitops-dir-xyz"), &mut seen);
        assert!(quiet.is_empty(), "目录不可读时静默（告警在外层按需做）");
    }

    #[test]
    fn hash_is_stable_and_sensitive() {
        let h = file_hash(b"{}");
        assert_eq!(h, file_hash(b"{}"));
        assert_ne!(h, file_hash(b"{ }"));
    }

    #[test]
    fn config_defaults_and_partial_fields() {
        let absent: crate::config::Config =
            serde_json::from_str(r#"{"node_id":"n","name":"h"}"#).unwrap();
        assert!(!absent.gitops.enabled);
        assert!(absent.gitops.dir.is_none());
        assert_eq!(absent.gitops.interval_secs, 15);

        let partial: GitopsConfig =
            serde_json::from_str(r#"{"enabled":true,"dir":"/repo/tasks","interval_secs":5}"#)
                .unwrap();
        assert!(partial.enabled);
        assert_eq!(partial.dir, Some(PathBuf::from("/repo/tasks")));
        assert_eq!(partial.interval_secs, 5);
    }
}
