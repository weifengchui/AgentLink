// SPDX-License-Identifier: Apache-2.0
//! File transfer engine (`docs/protocol.md` §5).
//!
//! Transfers run on a dedicated WebSocket per file (`/peer/file/ws`) so they
//! never congest the message channel. Chunks travel as base64 inside JSON text
//! frames for simplicity; a whole-file SHA-256 checksum is verified before the
//! temporary `.part` file is renamed into place.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::extract::ws::WebSocket;
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::api::SharedState;
use crate::conn::{self, Frame};
use crate::proto::{Envelope, Hello, PROTOCOL_VERSION};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const HASH_BUF_SIZE: usize = 64 * 1024;

// ---- payload types ----------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
struct FileMetaPayload {
    file_id: String,
    file_name: String,
    file_size: u64,
    chunk_size: u64,
    total_chunks: u64,
    checksum: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct FileAcceptPayload {
    file_id: String,
    resume_from: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct FileChunkPayload {
    file_id: String,
    seq: u64,
    /// base64 encoded chunk bytes.
    data: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct FileDonePayload {
    file_id: String,
    saved_path: String,
}

// ---- manager ----------------------------------------------------------------

/// Tracks the number of in-flight transfers (for `/v1/status`).
#[derive(Default)]
pub struct FileManager {
    active: AtomicUsize,
}

impl FileManager {
    pub fn new() -> Self {
        FileManager::default()
    }

    pub fn active(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }

    fn begin(&self) {
        self.active.fetch_add(1, Ordering::Relaxed);
    }

    fn end(&self) {
        self.active.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Kick off an outbound transfer of a local file to an online peer.
/// recv_dir 下已有文件的总字节数（含 `.part` 残留；目录不存在记 0）。
async fn recv_dir_total_bytes(base: &Path) -> u64 {
    let mut total = 0u64;
    let Ok(mut rd) = tokio::fs::read_dir(base).await else {
        return 0;
    };
    while let Ok(Some(ent)) = rd.next_entry().await {
        if let Ok(md) = ent.metadata().await {
            if md.is_file() {
                total = total.saturating_add(md.len());
            }
        }
    }
    total
}

/// Returns the new `file_id`.
pub async fn send_file(
    state: &SharedState,
    to: &str,
    path: &Path,
    ttl_seconds: Option<u64>,
) -> anyhow::Result<String> {
    let addr = state
        .router
        .peer_addr(to)
        .ok_or_else(|| anyhow::anyhow!("peer '{to}' is not online"))?;
    if !path.is_file() {
        return Err(anyhow::anyhow!("file not found: {}", path.display()));
    }

    let file_id = crate::proto::new_id();
    let spawned_id = file_id.clone();
    state.files.begin();

    let state = state.clone();
    let path = path.to_path_buf();
    let to = to.to_string();
    let addr = addr.clone();
    tokio::spawn(async move {
        let result = run_sender(&state, &addr, &to, &path, &spawned_id, ttl_seconds).await;
        state.files.end();
        match result {
            Ok(saved) => {
                tracing::info!(file_id = %spawned_id, peer = %to, saved = %saved, "file: transfer complete")
            }
            Err(e) => {
                tracing::warn!(file_id = %spawned_id, peer = %to, error = %e, "file: transfer failed")
            }
        }
    });

    Ok(file_id)
}

/// Send a file and await completion (used by the web upload endpoint so the
/// temporary upload can be removed once the transfer finished).
pub async fn send_transfer(
    state: &SharedState,
    to: &str,
    path: &Path,
    ttl_seconds: Option<u64>,
    from_agent: Option<&str>,
) -> anyhow::Result<String> {
    let addr = state
        .router
        .peer_addr(to)
        .ok_or_else(|| anyhow::anyhow!("peer '{to}' is not online"))?;
    if !path.is_file() {
        return Err(anyhow::anyhow!("file not found: {}", path.display()));
    }
    let file_id = crate::proto::new_id();
    state.files.begin();
    let result = run_sender(state, &addr, to, path, &file_id, ttl_seconds).await;
    state.files.end();
    result.map(|saved| {
        tracing::info!(file_id = %file_id, peer = %to, saved = %saved, "file: transfer complete");
        if let Ok(md) = std::fs::metadata(path) {
            let name = path
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| "file".to_string());
            state.router.record_file_event(
                "out",
                &state.config.node_id,
                to,
                &name,
                md.len(),
                &saved,
                from_agent,
            );
        }
        file_id
    })
}

/// Inbound entry point for `/peer/file/ws`.
pub async fn handle_inbound(socket: WebSocket, state: SharedState, remote: std::net::SocketAddr) {
    let (out_tx, in_rx) = conn::spawn_axum_bridge(socket);
    state.files.begin();
    let result = run_receiver(&state, &out_tx, in_rx).await;
    state.files.end();
    match result {
        Ok(Some(path)) => tracing::info!(saved = %path.display(), "file: received complete"),
        Ok(None) => {}
        Err(e) => tracing::warn!(error = %e, "file: receive failed"),
    }
    let _ = remote;
}

// ---- handshake --------------------------------------------------------------

async fn exchange_hello(
    state: &SharedState,
    sink: &mpsc::Sender<Frame>,
    recv: &mut mpsc::Receiver<Frame>,
) -> anyhow::Result<String> {
    let mut hello = Hello::new(
        &state.config.node_id,
        &state.config.name,
        &state.config.security.cluster_token,
    );
    if let Some(b) = state.bridge() {
        b.seal_hello(&mut hello);
    }
    send_value(sink, &hello).await?;

    let strict = state.config.security.mode == "strict";
    let deadline = tokio::time::sleep(HANDSHAKE_TIMEOUT);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => return Err(anyhow::anyhow!("handshake timeout")),
            frame = recv.recv() => {
                let Some(Frame::Text(text)) = frame else {
                    if frame.is_none() { return Err(anyhow::anyhow!("connection closed during handshake")); }
                    continue;
                };
                if let Ok(value) = serde_json::from_str::<Value>(&text) {
                    let action = value.get("action").and_then(|v| v.as_str()).unwrap_or("");
                    if action == "hello" {
                        if let Ok(their) = serde_json::from_value::<Hello>(value) {
                            if their.v > PROTOCOL_VERSION {
                                return Err(anyhow::anyhow!("unsupported peer protocol version"));
                            }
                            if strict && !their.verify(&state.config.security.cluster_token) {
                                return Err(anyhow::anyhow!("peer rejected cluster token"));
                            }
                            // v0.6 P1 新节点准入：文件通道与对等通道共用同一闸门。
                            let gate = match state.bridge() {
                                Some(b) => b.admission_gate(&their),
                                None => Ok(()),
                            };
                            if let Err(reason) = gate {
                                return Err(anyhow::anyhow!("peer rejected by admission: {reason}"));
                            }
                            return Ok(their.node_id);
                        }
                    }
                }
            }
        }
    }
}

// ---- sender -----------------------------------------------------------------

async fn run_sender(
    state: &SharedState,
    addr: &str,
    peer_id: &str,
    path: &Path,
    file_id: &str,
    ttl_seconds: Option<u64>,
) -> anyhow::Result<String> {
    let (sink, mut recv) = conn::open_file_ws(state, addr, state.peers.mtls_addr(peer_id)).await?;
    let _their_id = exchange_hello(state, &sink, &mut recv).await?;

    tracing::info!(file_id = %file_id, peer = %peer_id, path = %path.display(), "file: sender started");

    // Pre-compute size + whole-file checksum (one streaming pass).
    let (size, checksum) = hash_file(path).await?;
    let chunk_size = state.config.files.chunk_size.max(1) as u64;
    let total_chunks = if chunk_size > 0 && size > 0 {
        size.div_ceil(chunk_size)
    } else {
        0
    };
    let file_name = sanitize_name(&path.to_string_lossy()).unwrap_or_else(|| "unnamed".to_string());

    let meta = json!({
        "file_id": file_id,
        "file_name": file_name,
        "file_size": size,
        "chunk_size": chunk_size,
        "total_chunks": total_chunks,
        "checksum": checksum,
    });
    send_file_frame(state, &sink, peer_id, "file_meta", meta, ttl_seconds).await?;

    // Wait for `file_accept` (or `file_error`).
    let resume_from = loop {
        match recv.recv().await {
            None => return Err(anyhow::anyhow!("peer closed during handshake")),
            Some(Frame::Text(text)) => {
                let Ok(value) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
                match value.get("action").and_then(|v| v.as_str()).unwrap_or("") {
                    "file_accept" => {
                        let ok: FileAcceptPayload = serde_json::from_value(
                            value.get("payload").cloned().unwrap_or(Value::Null),
                        )
                        .map_err(|_| anyhow::anyhow!("bad file_accept"))?;
                        break ok.resume_from;
                    }
                    "file_error" => {
                        let payload = parse_error(&value);
                        return Err(anyhow::anyhow!("peer rejected transfer: {payload}"));
                    }
                    _ => {}
                }
            }
            Some(_) => {}
        }
    };

    // Stream chunks, resuming from the requested chunk index.
    let mut file = tokio::fs::File::open(path).await?;
    file.seek(std::io::SeekFrom::Start(resume_from * chunk_size))
        .await?;
    let mut buf = vec![0u8; chunk_size as usize];
    let mut seq = resume_from;
    while seq < total_chunks && !buf.is_empty() {
        let mut n = 0usize;
        while n < buf.len() {
            let read = file.read(&mut buf[n..]).await?;
            if read == 0 {
                break;
            }
            n += read;
        }
        if n == 0 {
            break;
        }
        let data = base64::engine::general_purpose::STANDARD.encode(&buf[..n]);
        let chunk = json!({ "file_id": file_id, "seq": seq, "data": data });
        send_file_frame(state, &sink, peer_id, "file_chunk", chunk, None).await?;
        seq += 1;
    }

    send_file_frame(
        state,
        &sink,
        peer_id,
        "file_complete",
        json!({ "file_id": file_id, "checksum": checksum }),
        None,
    )
    .await?;

    // Wait for `file_done`.
    loop {
        match recv.recv().await {
            None => return Err(anyhow::anyhow!("connection closed before file_done")),
            Some(Frame::Text(text)) => {
                let Ok(value) = serde_json::from_str::<Value>(&text) else {
                    continue;
                };
                match value.get("action").and_then(|v| v.as_str()).unwrap_or("") {
                    "file_done" => {
                        let ok: FileDonePayload = serde_json::from_value(
                            value.get("payload").cloned().unwrap_or(Value::Null),
                        )
                        .map_err(|_| anyhow::anyhow!("bad file_done"))?;
                        return Ok(ok.saved_path);
                    }
                    "file_error" => {
                        let payload = parse_error(&value);
                        return Err(anyhow::anyhow!("peer failed transfer: {payload}"));
                    }
                    _ => {}
                }
            }
            Some(_) => {}
        }
    }
}

// ---- receiver ---------------------------------------------------------------

/// Run the receiver side of a file connection. Returns the saved path on success.
async fn run_receiver(
    state: &SharedState,
    sink: &mpsc::Sender<Frame>,
    mut recv: mpsc::Receiver<Frame>,
) -> anyhow::Result<Option<PathBuf>> {
    let peer_id = exchange_hello(state, sink, &mut recv).await?;
    tracing::info!(peer = %peer_id, "file: receiver ready");

    let mut recv_state: Option<Receiving> = None;

    while let Some(frame) = recv.recv().await {
        let Frame::Text(text) = frame else { continue };
        let Ok(value) = serde_json::from_str::<Value>(&text) else {
            continue;
        };
        let action = value.get("action").and_then(|v| v.as_str()).unwrap_or("");
        match action {
            "file_meta" => {
                let meta: FileMetaPayload = match serde_json::from_value(
                    value.get("payload").cloned().unwrap_or(Value::Null),
                ) {
                    Ok(m) => m,
                    Err(_) => {
                        send_file_error(
                            state,
                            sink,
                            &peer_id,
                            "",
                            "bad_meta",
                            "malformed file_meta",
                        )
                        .await;
                        continue;
                    }
                };
                if recv_state.is_some() {
                    send_file_error(
                        state,
                        sink,
                        &peer_id,
                        &meta.file_id,
                        "busy",
                        "active transfer on this connection",
                    )
                    .await;
                    continue;
                }
                let name = match sanitize_name(&meta.file_name) {
                    Some(n) => n,
                    None => {
                        send_file_error(
                            state,
                            sink,
                            &peer_id,
                            &meta.file_id,
                            "bad_name",
                            "unsafe file name",
                        )
                        .await;
                        continue;
                    }
                };
                if meta.file_size > state.config.files.max_file_size {
                    send_file_error(
                        state,
                        sink,
                        &peer_id,
                        &meta.file_id,
                        "too_large",
                        &format!(
                            "size {} exceeds limit {}",
                            meta.file_size, state.config.files.max_file_size
                        ),
                    )
                    .await;
                    continue;
                }
                let base = state.config.files.recv_dir_resolved().to_path_buf();
                if let Err(e) = tokio::fs::create_dir_all(&base).await {
                    send_file_error(state, sink, &peer_id, &meta.file_id, "io", &e.to_string())
                        .await;
                    continue;
                }
                // v0.8：接收侧磁盘预算（files.max_recv_disk_bytes，缺省 0=不限制，
                // 行为与 v0.7 一致）。>0 时 recv_dir 已有总量 + 本次文件超限即拒绝。
                let recv_limit = state.config.files.max_recv_disk_bytes;
                if recv_limit > 0 {
                    let used = recv_dir_total_bytes(&base).await;
                    if used.saturating_add(meta.file_size) > recv_limit {
                        tracing::warn!(
                            peer = %peer_id, file = %name,
                            incoming = meta.file_size, used, limit = recv_limit,
                            "file: recv disk budget exceeded"
                        );
                        state.router.audit_event(
                            "quota.reject",
                            "peer",
                            json!({
                                "reason": "recv_disk_quota",
                                "peer": peer_id,
                                "limit": recv_limit,
                                "current": used,
                                "incoming": meta.file_size,
                                "file_name": name,
                            }),
                        );
                        send_file_error(
                            state,
                            sink,
                            &peer_id,
                            &meta.file_id,
                            "recv_disk_quota",
                            &format!(
                                "recv disk budget: {used} + {} > {recv_limit}",
                                meta.file_size
                            ),
                        )
                        .await;
                        continue;
                    }
                }
                let part_path = base.join(format!("{}.part", meta.file_id));
                let final_path = base.join(&name);
                let file = match tokio::fs::File::create(&part_path).await {
                    Ok(f) => f,
                    Err(e) => {
                        send_file_error(state, sink, &peer_id, &meta.file_id, "io", &e.to_string())
                            .await;
                        continue;
                    }
                };
                recv_state = Some(Receiving {
                    file_id: meta.file_id.clone(),
                    size: meta.file_size,
                    checksum: meta.checksum.clone(),
                    part_path,
                    final_path,
                    file,
                    next_seq: 0,
                    hasher: Sha256::new(),
                    received_bytes: 0,
                });
                send_file_frame(
                    state,
                    sink,
                    &peer_id,
                    "file_accept",
                    json!({ "file_id": meta.file_id, "resume_from": 0 }),
                    None,
                )
                .await?;
            }

            "file_chunk" => {
                let Some(r) = recv_state.as_mut() else {
                    continue;
                };
                let chunk: FileChunkPayload = match serde_json::from_value(
                    value.get("payload").cloned().unwrap_or(Value::Null),
                ) {
                    Ok(c) => c,
                    Err(_) => {
                        send_file_error(
                            state,
                            sink,
                            &peer_id,
                            &r.file_id,
                            "bad_chunk",
                            "malformed file_chunk",
                        )
                        .await;
                        continue;
                    }
                };
                if chunk.seq < r.next_seq {
                    continue; // duplicate
                }
                if chunk.seq != r.next_seq {
                    send_file_error(
                        state,
                        sink,
                        &peer_id,
                        &r.file_id,
                        "out_of_order",
                        &format!("expected seq {} got {}", r.next_seq, chunk.seq),
                    )
                    .await;
                    continue;
                }
                let bytes = match base64::engine::general_purpose::STANDARD.decode(&chunk.data) {
                    Ok(b) => b,
                    Err(_) => {
                        send_file_error(
                            state,
                            sink,
                            &peer_id,
                            &r.file_id,
                            "bad_chunk",
                            "invalid base64",
                        )
                        .await;
                        continue;
                    }
                };
                if let Err(e) = r.file.write_all(&bytes).await {
                    send_file_error(state, sink, &peer_id, &r.file_id, "io", &e.to_string()).await;
                    continue;
                }
                r.hasher.update(&bytes);
                r.received_bytes += bytes.len() as u64;
                r.next_seq += 1;
            }

            "file_complete" => {
                let Some(r) = recv_state.take() else { continue };
                let file_id = r.file_id.clone();
                let fname = r
                    .final_path
                    .file_name()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_default();
                let fsize = r.size;
                let result = finish_receive(r).await;
                match result {
                    Ok(final_path) => {
                        state.router.record_file_event(
                            "in",
                            &peer_id,
                            &state.config.node_id,
                            &fname,
                            fsize,
                            &final_path.to_string_lossy(),
                            // 文件通道是整机级，入站无法得知对端 agent 身份
                            None,
                        );
                        let saved = final_path.to_string_lossy().to_string();
                        send_file_frame(
                            state,
                            sink,
                            &peer_id,
                            "file_done",
                            json!({ "file_id": file_id, "saved_path": saved }),
                            None,
                        )
                        .await?;
                        return Ok(Some(final_path));
                    }
                    Err(e) => {
                        send_file_error(
                            state,
                            sink,
                            &peer_id,
                            &file_id,
                            "checksum_failed",
                            &e.to_string(),
                        )
                        .await;
                    }
                }
            }

            "file_error" => {
                let payload = parse_error(&value);
                tracing::warn!(peer = %peer_id, error = %payload, "file: peer aborted transfer");
                if let Some(r) = recv_state.take() {
                    let _ = tokio::fs::remove_file(&r.part_path).await;
                }
                return Ok(None);
            }

            _ => {}
        }
    }

    if let Some(r) = recv_state {
        let _ = tokio::fs::remove_file(&r.part_path).await;
    }
    Ok(None)
}

/// Verify the written bytes, then rename the `.part` into its final location.
async fn finish_receive(mut r: Receiving) -> anyhow::Result<PathBuf> {
    r.file.flush().await?;
    if r.received_bytes != r.size {
        anyhow::bail!(
            "size mismatch: expected {} got {}",
            r.size,
            r.received_bytes
        );
    }
    let actual = hex::encode(r.hasher.finalize());
    if actual != r.checksum {
        anyhow::bail!("checksum mismatch: expected {} got {}", r.checksum, actual);
    }
    tokio::fs::rename(&r.part_path, &r.final_path).await?;
    Ok(r.final_path)
}

struct Receiving {
    file_id: String,
    size: u64,
    checksum: String,
    part_path: PathBuf,
    final_path: PathBuf,
    file: tokio::fs::File,
    next_seq: u64,
    hasher: Sha256,
    received_bytes: u64,
}

// ---- helpers ----------------------------------------------------------------

async fn send_file_frame(
    state: &SharedState,
    sink: &mpsc::Sender<Frame>,
    to: &str,
    action: &str,
    payload: Value,
    ttl: Option<u64>,
) -> anyhow::Result<()> {
    let env = Envelope::business(
        &state.config.node_id,
        to,
        "file",
        action,
        payload,
        ttl,
        None,
        None,
        None,
        None,
    );
    send_value(sink, &env).await
}

async fn send_value(sink: &mpsc::Sender<Frame>, value: &impl Serialize) -> anyhow::Result<()> {
    let text = serde_json::to_string(value)?;
    sink.send(Frame::Text(text))
        .await
        .map_err(|_| anyhow::anyhow!("connection closed"))
}

async fn send_file_error(
    state: &SharedState,
    sink: &mpsc::Sender<Frame>,
    to: &str,
    file_id: &str,
    code: &str,
    message: &str,
) {
    let payload = json!({ "file_id": file_id, "code": code, "message": message });
    let _ = send_file_frame(state, sink, to, "file_error", payload, None).await;
}

fn parse_error(value: &Value) -> String {
    value
        .get("payload")
        .and_then(|p| p.get("message"))
        .and_then(|m| m.as_str())
        .unwrap_or("unknown error")
        .to_string()
}

/// Streaming SHA-256 + size of a file.
async fn hash_file(path: &Path) -> anyhow::Result<(u64, String)> {
    let mut f = tokio::fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; HASH_BUF_SIZE];
    let mut size = 0u64;
    loop {
        let n = f.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        size += n as u64;
    }
    Ok((size, hex::encode(hasher.finalize())))
}

/// Take a file name from any path-ish input, strip separators and drop
/// directory traversal / absolute paths.
pub fn sanitize_name(raw: &str) -> Option<String> {
    let normalized = raw.replace('\\', "/");
    let base = Path::new(&normalized).file_name()?.to_str()?;
    let cleaned: String = base
        .chars()
        .map(|c| if "\\/:*?\"<>|".contains(c) { '_' } else { c })
        .collect();
    let cleaned = cleaned.trim();
    if cleaned.is_empty() || cleaned == "." || cleaned == ".." {
        return None;
    }
    Some(cleaned.to_string())
}
