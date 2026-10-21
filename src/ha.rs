//! v0.8 HA（M1）：**传输无关**的 Raft 内核 + KV 状态机。
//!
//! 本模块只实现共识算法本身（任期/投票/日志/提交/应用/快照），不碰网络：
//! - 传输由调用方通过 [`RaftMessage`] 收发（内核只产出「发给谁、什么消息」）；
//! - 时间由调用方以毫秒传入（内核不读墙钟，便于确定性测试）；
//! - 持久化由调用方通过 [`RaftNode::persistent`] / [`RaftNode::restore`] 负责。
//!
//! 设计基线见 `docs/v0.8-HA-Raft设计-2026-10-21.md`（M1 阶段）。
//! 语义要点：
//! - 每 term 每节点最多投一票，且**持久化** `voted_for`；
//! - 投票要求候选者日志**至少同样新**（term 优先，其次 index）；
//! - **pre-vote** 先探票，避免孤立节点回归时打断在任 leader；
//! - 追加日志用 `prev_log_index/prev_log_term` 校验，冲突回退重试；
//! - 提交规则：多数派复制 + **仅当该条 term == current_term** 才可推进
//!   `commit_index`（Raft §5.4.2），避免旧 term 日志被误提交；
//! - apply 严格按序，状态机命令幂等（`Put`/`Delete` 天然幂等）。
//!
//! 依赖保持纯 Rust，无 C/外部进程。
//!
//! M1 阶段说明：内核已实现并有单测，但**尚未接入运行时**（传输在 M2）。
//! 因此 `ha.enabled=true` 在 M1 会被启动校验直接拒绝（避免现场误以为
//! 已经具备 HA 保护）。本模块的公开类型在 M2 接线前属于休眠骨架。

#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

/// 复制日志里的命令（状态机写入）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    /// 写一个键（命名空间 + 键）。
    Put {
        ns: String,
        key: String,
        value: Vec<u8>,
    },
    /// 删一个键。
    Delete { ns: String, key: String },
}

/// 复制日志条目。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub command: Command,
}

/// 节点角色。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Follower => "follower",
            Role::Candidate => "candidate",
            Role::Leader => "leader",
        }
    }
}

/// 快照载荷：全量 KV（`键` -> `值`）。
pub type SnapshotData = Vec<(String, Vec<u8>)>;

/// 拉票请求参数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteRequest {
    pub term: u64,
    pub candidate_id: String,
    pub last_log_index: u64,
    pub last_log_term: u64,
    pub pre_vote: bool,
}

/// 追加日志请求参数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendRequest {
    pub term: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

/// 节点间消息。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftMessage {
    /// 拉票（`pre_vote=true` 表示试探票，不改变自身 term）。
    RequestVote {
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
        pre_vote: bool,
    },
    /// 投票结果（`pre_vote=true` 表示这是对试探票的回应）。
    RequestVoteResp {
        term: u64,
        voter_id: String,
        granted: bool,
        pre_vote: bool,
    },
    /// leader 追加日志（entries 可空 = 心跳）。
    AppendEntries {
        term: u64,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    /// 追加结果；失败时带 `conflict_index` 提示回退位置。
    AppendEntriesResp {
        term: u64,
        from_id: String,
        success: bool,
        match_index: u64,
        conflict_index: u64,
    },
    /// 安装快照（节点落后太多时用）。
    InstallSnapshot {
        term: u64,
        leader_id: String,
        last_included_index: u64,
        last_included_term: u64,
        data: SnapshotData,
    },
    /// 安装快照结果。
    InstallSnapshotResp {
        term: u64,
        from_id: String,
        success: bool,
    },
}

/// 内核参数。
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// 选举超时基准（毫秒）；实际超时在其 1.0~1.5 倍间随机。
    pub election_timeout_ms: u64,
    /// 心跳间隔（毫秒）。
    pub heartbeat_interval_ms: u64,
    /// 超过该未复制条数时改发快照。
    pub snapshot_threshold_entries: u64,
}

impl Default for RaftConfig {
    fn default() -> Self {
        RaftConfig {
            election_timeout_ms: 1000,
            heartbeat_interval_ms: 200,
            snapshot_threshold_entries: 10_000,
        }
    }
}

/// 复制状态机：按命名空间分区的 KV。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KvState {
    data: BTreeMap<String, Vec<u8>>,
}

impl KvState {
    pub fn put(&mut self, ns: &str, key: &str, value: Vec<u8>) {
        self.data.insert(format!("{ns}/{key}"), value);
    }

    pub fn delete(&mut self, ns: &str, key: &str) {
        self.data.remove(&format!("{ns}/{key}"));
    }

    pub fn get(&self, ns: &str, key: &str) -> Option<&Vec<u8>> {
        self.data.get(&format!("{ns}/{key}"))
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// 全量导出（快照用）。
    pub fn dump(&self) -> SnapshotData {
        self.data
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// 从快照恢复（全量覆盖）。
    pub fn load(&mut self, data: SnapshotData) {
        self.data = data.into_iter().collect();
    }
}

/// 需要持久化的部分（term / 投票 / 日志）。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PersistentState {
    pub current_term: u64,
    pub voted_for: Option<String>,
    /// 日志（index=1 起；index 0 为占位哨兵）。
    pub log: Vec<LogEntry>,
}

/// 一个 Raft 节点（内核）。
#[derive(Debug, Clone)]
pub struct RaftNode {
    id: String,
    peers: Vec<String>,
    cfg: RaftConfig,
    role: Role,
    current_term: u64,
    voted_for: Option<String>,
    /// `log[0]` 是 index=0/term=0 的哨兵；`log[i]` 对应 index=i。
    log: Vec<LogEntry>,
    commit_index: u64,
    last_applied: u64,
    state: KvState,
    next_index: HashMap<String, u64>,
    match_index: HashMap<String, u64>,
    votes: HashSet<String>,
    /// 预选票（试探票）收集。
    pre_votes: HashSet<String>,
    leader_id: Option<String>,
    /// 下次选举时间（ms）。
    election_deadline: u64,
    /// leader 租约到期（ms）：期间不发起选举，也不给 pre-vote 盖章。
    leader_lease_until: u64,
    /// 下次心跳时间（ms）。
    heartbeat_at: u64,
    /// 预选票阶段（还没正式竞选）。
    pre_vote: bool,
    /// 确定性伪随机（选举超时抖动）。
    rng: u64,
}

impl RaftNode {
    /// 新建节点。`peers` 不含自己。
    pub fn new(id: impl Into<String>, peers: Vec<String>, cfg: RaftConfig) -> Self {
        let id = id.into();
        let mut rng = 0x9E37_79B9_7F4A_7C15u64;
        for b in id.as_bytes() {
            rng = rng.rotate_left(5) ^ (*b as u64).wrapping_mul(0x100_0000_01B3);
        }
        RaftNode {
            id,
            peers,
            cfg,
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: vec![LogEntry {
                index: 0,
                term: 0,
                command: Command::Delete {
                    ns: String::new(),
                    key: String::new(),
                },
            }],
            commit_index: 0,
            last_applied: 0,
            state: KvState::default(),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            votes: HashSet::new(),
            pre_votes: HashSet::new(),
            leader_id: None,
            election_deadline: 0,
            leader_lease_until: 0,
            heartbeat_at: 0,
            pre_vote: false,
            rng,
        }
    }

    // ---- 只读观测 ---------------------------------------------------------

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn term(&self) -> u64 {
        self.current_term
    }

    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    pub fn last_applied(&self) -> u64 {
        self.last_applied
    }

    pub fn leader_id(&self) -> Option<&str> {
        self.leader_id.as_deref()
    }

    pub fn state(&self) -> &KvState {
        &self.state
    }

    pub fn log_len(&self) -> usize {
        self.log.len().saturating_sub(1)
    }

    /// 集群规模（含自己）。
    pub fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }

    /// 多数派票数。
    pub fn quorum(&self) -> usize {
        self.cluster_size() / 2 + 1
    }

    /// 是否已连上多数派（有 leader 或已提交过日志）。
    pub fn has_quorum(&self) -> bool {
        self.role == Role::Leader || self.commit_index > 0
    }

    /// 需要持久化的状态。
    pub fn persistent(&self) -> PersistentState {
        PersistentState {
            current_term: self.current_term,
            voted_for: self.voted_for.clone(),
            log: self.log.clone(),
        }
    }

    /// 恢复持久化状态（重启后调用）。
    pub fn restore(&mut self, p: PersistentState) {
        self.current_term = p.current_term;
        self.voted_for = p.voted_for;
        if !p.log.is_empty() {
            let mut log = p.log;
            if log[0].index != 0 {
                log.insert(
                    0,
                    LogEntry {
                        index: 0,
                        term: 0,
                        command: Command::Delete {
                            ns: String::new(),
                            key: String::new(),
                        },
                    },
                );
            }
            self.log = log;
        }
    }

    /// 状态机快照内容（`None` 表示还没到阈值）。
    pub fn snapshot(&self) -> Option<(u64, u64, SnapshotData)> {
        if self.last_applied == 0 || self.last_applied < self.cfg.snapshot_threshold_entries {
            return None;
        }
        Some((
            self.last_applied,
            self.log_term_at(self.last_applied),
            self.state.dump(),
        ))
    }

    // ---- 内部小工具 -------------------------------------------------------

    fn next_rand(&mut self) -> u64 {
        // xorshift64*
        let mut x = self.rng;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn last_log_index(&self) -> u64 {
        self.log.last().map(|e| e.index).unwrap_or(0)
    }

    fn last_log_term(&self) -> u64 {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    fn log_term_at(&self, index: u64) -> u64 {
        self.log.get(index as usize).map(|e| e.term).unwrap_or(0)
    }

    fn entry_at(&self, index: u64) -> Option<&LogEntry> {
        if index == 0 {
            return self.log.first();
        }
        self.log.get(index as usize)
    }

    /// 选举超时抖动（1.0~1.5 倍基准）。
    fn randomized_timeout(&mut self) -> u64 {
        let base = self.cfg.election_timeout_ms.max(1);
        let jitter = self.next_rand() % (base / 2 + 1);
        base + jitter
    }

    fn reset_election_timer(&mut self, now: u64) {
        self.election_deadline = now + self.randomized_timeout();
    }

    fn become_follower(&mut self, term: u64) {
        self.role = Role::Follower;
        self.current_term = term;
        self.voted_for = None;
        self.leader_id = None;
        self.votes.clear();
        self.pre_votes.clear();
        self.pre_vote = false;
    }

    /// 候选者日志是否至少同样新。
    fn log_up_to_date(&self, last_index: u64, last_term: u64) -> bool {
        let my_term = self.last_log_term();
        let my_index = self.last_log_index();
        last_term > my_term || (last_term == my_term && last_index >= my_index)
    }

    // ---- 驱动 -------------------------------------------------------------

    /// 推进定时器；返回需要发出的消息（`目标节点, 消息`）。
    pub fn tick(&mut self, now: u64) -> Vec<(String, RaftMessage)> {
        let mut out = Vec::new();
        match self.role {
            Role::Leader => {
                if now >= self.heartbeat_at {
                    self.heartbeat_at = now + self.cfg.heartbeat_interval_ms.max(1);
                    out.extend(self.broadcast_append(now));
                }
            }
            _ => {
                if now >= self.election_deadline && now >= self.leader_lease_until {
                    out.extend(self.start_election(now));
                }
            }
        }
        out
    }

    /// 处理一条消息；返回需要发出的消息。
    pub fn handle(&mut self, from: &str, msg: RaftMessage, now: u64) -> Vec<(String, RaftMessage)> {
        let mut out = Vec::new();
        match msg {
            RaftMessage::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
                pre_vote,
            } => {
                let req = VoteRequest {
                    term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                    pre_vote,
                };
                out.extend(self.on_request_vote(from, &req, now));
            }
            RaftMessage::RequestVoteResp {
                term,
                voter_id,
                granted,
                pre_vote,
            } => {
                out.extend(self.on_vote_resp(term, voter_id, granted, pre_vote, now));
            }
            RaftMessage::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                let a = AppendRequest {
                    term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                };
                out.extend(self.on_append_entries(leader_id, &a, now));
            }
            RaftMessage::AppendEntriesResp {
                term,
                from_id,
                success,
                match_index,
                conflict_index,
            } => {
                out.extend(self.on_append_resp(
                    from_id,
                    term,
                    success,
                    match_index,
                    conflict_index,
                    now,
                ));
            }
            RaftMessage::InstallSnapshot {
                term,
                leader_id,
                last_included_index,
                last_included_term,
                data,
            } => {
                out.extend(self.on_install_snapshot(
                    leader_id,
                    term,
                    last_included_index,
                    last_included_term,
                    data,
                    now,
                ));
            }
            RaftMessage::InstallSnapshotResp {
                term,
                from_id,
                success,
            } => {
                if term > self.current_term {
                    self.become_follower(term);
                }
                if self.role == Role::Leader && success {
                    self.match_index
                        .insert(from_id.clone(), self.last_log_index());
                    self.next_index
                        .insert(from_id.clone(), self.last_log_index() + 1);
                }
            }
        }
        out
    }

    /// leader 提交一条命令（仅 leader 可用）。
    pub fn propose(&mut self, command: Command) -> Option<u64> {
        if self.role != Role::Leader {
            return None;
        }
        let index = self.last_log_index() + 1;
        self.log.push(LogEntry {
            index,
            term: self.current_term,
            command,
        });
        self.match_index.insert(self.id.clone(), index);
        Some(index)
    }

    // ---- 选举 -------------------------------------------------------------

    fn start_election(&mut self, now: u64) -> Vec<(String, RaftMessage)> {
        let mut out = Vec::new();
        if self.pre_vote {
            // 预选票拿到多数 -> 正式竞选（term+1，投自己）
            self.pre_vote = false;
            self.pre_votes.clear();
            self.role = Role::Candidate;
            self.current_term += 1;
            self.voted_for = Some(self.id.clone());
            self.votes.clear();
            self.votes.insert(self.id.clone());
            self.leader_id = None;
            self.reset_election_timer(now);
        } else {
            // 先探票：term 用 current_term+1（不落盘、不改自身 term）
            self.pre_vote = true;
            self.pre_votes.clear();
            self.pre_votes.insert(self.id.clone());
            self.reset_election_timer(now);
        }
        let term = if self.pre_vote {
            self.current_term + 1
        } else {
            self.current_term
        };
        let req = RaftMessage::RequestVote {
            term,
            candidate_id: self.id.clone(),
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
            pre_vote: self.pre_vote,
        };
        for p in self.peers.clone() {
            out.push((p, req.clone()));
        }
        out
    }

    fn on_request_vote(
        &mut self,
        from: &str,
        req: &VoteRequest,
        now: u64,
    ) -> Vec<(String, RaftMessage)> {
        let term = req.term;
        let candidate_id = req.candidate_id.clone();
        let last_log_index = req.last_log_index;
        let last_log_term = req.last_log_term;
        let pre_vote = req.pre_vote;
        let mut out = Vec::new();
        if pre_vote {
            // 预选票：不改 term、不落票；leader 租约有效期内一律拒绝
            let granted = term >= self.current_term
                && self.log_up_to_date(last_log_index, last_log_term)
                && now >= self.leader_lease_until;
            out.push((
                from.to_string(),
                RaftMessage::RequestVoteResp {
                    term: self.current_term,
                    voter_id: self.id.clone(),
                    granted,
                    pre_vote: true,
                },
            ));
            return out;
        }
        if term < self.current_term {
            out.push((
                from.to_string(),
                RaftMessage::RequestVoteResp {
                    term: self.current_term,
                    voter_id: self.id.clone(),
                    granted: false,
                    pre_vote: false,
                },
            ));
            return out;
        }
        if term > self.current_term {
            self.become_follower(term);
        }
        let can_vote = self.voted_for.is_none() || self.voted_for.as_deref() == Some(&candidate_id);
        let granted = can_vote && self.log_up_to_date(last_log_index, last_log_term);
        if granted {
            self.voted_for = Some(candidate_id);
            self.reset_election_timer(now);
        }
        out.push((
            from.to_string(),
            RaftMessage::RequestVoteResp {
                term: self.current_term,
                voter_id: self.id.clone(),
                granted,
                pre_vote: false,
            },
        ));
        out
    }

    fn on_vote_resp(
        &mut self,
        term: u64,
        voter_id: String,
        granted: bool,
        pre_vote: bool,
        now: u64,
    ) -> Vec<(String, RaftMessage)> {
        let mut out = Vec::new();
        if term > self.current_term {
            self.become_follower(term);
            self.reset_election_timer(now);
            return out;
        }
        if term < self.current_term {
            return out;
        }
        if pre_vote {
            if !self.pre_vote {
                return out; // 迟到的预选票回应
            }
            if granted {
                self.pre_votes.insert(voter_id);
                if self.pre_votes.len() >= self.quorum() {
                    // 预选票通过 -> 转正式竞选
                    out.extend(self.start_election(now));
                }
            } else {
                // 被拒：放弃本轮，等下次选举超时再试
                self.pre_vote = false;
                self.pre_votes.clear();
            }
            return out;
        }
        if self.role != Role::Candidate || self.pre_vote {
            return out;
        }
        if granted {
            self.votes.insert(voter_id);
            if self.votes.len() >= self.quorum() {
                self.become_leader(now);
                out.extend(self.broadcast_append(now));
            }
        }
        out
    }

    fn become_leader(&mut self, now: u64) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id.clone());
        let last = self.last_log_index();
        for p in &self.peers {
            self.next_index.insert(p.clone(), last + 1);
            self.match_index.insert(p.clone(), 0);
        }
        self.heartbeat_at = now + self.cfg.heartbeat_interval_ms.max(1);
        // leader 自己的条目立刻算已复制
        self.match_index.insert(self.id.clone(), last);
    }

    // ---- 日志 -------------------------------------------------------------

    fn broadcast_append(&mut self, now: u64) -> Vec<(String, RaftMessage)> {
        let _ = now;
        let mut out = Vec::new();
        for p in self.peers.clone() {
            let msg = self.append_msg_for(&p);
            out.push((p, msg));
        }
        out
    }

    fn append_msg_for(&self, peer: &str) -> RaftMessage {
        let next = self.next_index.get(peer).copied().unwrap_or(1).max(1);
        let prev_index = next - 1;
        let prev_term = self.log_term_at(prev_index);
        let entries: Vec<LogEntry> = self
            .log
            .iter()
            .skip(next as usize)
            .take(64)
            .cloned()
            .collect();
        RaftMessage::AppendEntries {
            term: self.current_term,
            leader_id: self.id.clone(),
            prev_log_index: prev_index,
            prev_log_term: prev_term,
            entries,
            leader_commit: self.commit_index,
        }
    }

    fn on_append_entries(
        &mut self,
        leader_id: String,
        a: &AppendRequest,
        now: u64,
    ) -> Vec<(String, RaftMessage)> {
        let term = a.term;
        let prev_log_index = a.prev_log_index;
        let prev_log_term = a.prev_log_term;
        let entries = a.entries.clone();
        let leader_commit = a.leader_commit;
        let mut out = Vec::new();
        if term < self.current_term {
            out.push((
                leader_id.clone(),
                RaftMessage::AppendEntriesResp {
                    term: self.current_term,
                    from_id: self.id.clone(),
                    success: false,
                    match_index: 0,
                    conflict_index: 0,
                },
            ));
            return out;
        }
        if term > self.current_term || self.role != Role::Follower {
            self.become_follower(term);
        } else {
            self.current_term = term;
        }
        self.leader_id = Some(leader_id.clone());
        self.reset_election_timer(now);
        self.leader_lease_until = now + self.cfg.election_timeout_ms;

        // prev 日志校验
        if prev_log_index > 0 {
            match self.entry_at(prev_log_index) {
                Some(e) if e.term == prev_log_term => {}
                Some(e) => {
                    // 冲突：回退到该 term 的第一个 index
                    let mut idx = prev_log_index;
                    while idx > 1 && self.log_term_at(idx - 1) == e.term {
                        idx -= 1;
                    }
                    out.push((
                        leader_id.clone(),
                        RaftMessage::AppendEntriesResp {
                            term: self.current_term,
                            from_id: self.id.clone(),
                            success: false,
                            match_index: 0,
                            conflict_index: idx,
                        },
                    ));
                    return out;
                }
                None => {
                    out.push((
                        leader_id.clone(),
                        RaftMessage::AppendEntriesResp {
                            term: self.current_term,
                            from_id: self.id.clone(),
                            success: false,
                            match_index: 0,
                            conflict_index: prev_log_index,
                        },
                    ));
                    return out;
                }
            }
        }

        // 追加 / 覆盖冲突后缀
        let entries_len = entries.len() as u64;
        for entry in entries {
            if entry.index <= self.last_log_index() {
                let existing = self.log_term_at(entry.index);
                if existing == entry.term {
                    continue; // 已有相同条目
                }
                // 冲突 → 截断后重写
                self.log.truncate(entry.index as usize);
            }
            self.log.push(entry);
        }

        if leader_commit > self.commit_index {
            self.commit_index = leader_commit.min(self.last_log_index());
        }
        self.apply_committed();
        out.push((
            leader_id.clone(),
            RaftMessage::AppendEntriesResp {
                term: self.current_term,
                from_id: self.id.clone(),
                success: true,
                match_index: prev_log_index + entries_len,
                conflict_index: 0,
            },
        ));
        out
    }

    fn on_append_resp(
        &mut self,
        from_id: String,
        term: u64,
        success: bool,
        match_index: u64,
        conflict_index: u64,
        now: u64,
    ) -> Vec<(String, RaftMessage)> {
        let mut out = Vec::new();
        if term > self.current_term {
            self.become_follower(term);
            self.reset_election_timer(now);
            return out;
        }
        if self.role != Role::Leader || term < self.current_term {
            return out;
        }
        if success {
            let m = match_index.max(self.match_index.get(&from_id).copied().unwrap_or(0));
            self.match_index.insert(from_id.clone(), m);
            self.next_index.insert(from_id.clone(), m + 1);
            self.try_advance_commit();
            // 继续发下一批（流水线）
            let msg = self.append_msg_for(&from_id);
            out.push((from_id.clone(), msg));
        } else {
            // 失败：回退 next_index，**不立即重发**（避免与 follower 互刷死循环）；
            // 下一次心跳会带新的 prev_log_index 重试。
            let cur = self.next_index.get(&from_id).copied().unwrap_or(1);
            let hint = if conflict_index > 0 {
                conflict_index
            } else {
                cur.saturating_sub(1)
            };
            self.next_index.insert(from_id.clone(), hint.max(1));
        }
        out
    }

    /// 推进 commit_index（多数派 + 当前 term 限制）。
    fn try_advance_commit(&mut self) {
        let mut indexes: Vec<u64> = self
            .peers
            .iter()
            .map(|p| self.match_index.get(p).copied().unwrap_or(0))
            .collect();
        indexes.push(self.last_log_index());
        indexes.sort_unstable_by(|a, b| b.cmp(a));
        let quorum = self.quorum();
        if indexes.len() < quorum {
            return;
        }
        let candidate = indexes[quorum - 1];
        if candidate > self.commit_index && self.log_term_at(candidate) == self.current_term {
            self.commit_index = candidate;
            self.apply_committed();
        }
    }

    /// 按序 apply 已提交的日志到状态机。
    fn apply_committed(&mut self) {
        while self.last_applied < self.commit_index {
            let next = self.last_applied + 1;
            let Some(entry) = self.entry_at(next).cloned() else {
                break;
            };
            match &entry.command {
                Command::Put { ns, key, value } => {
                    self.state.put(ns, key, value.clone());
                }
                Command::Delete { ns, key } => {
                    self.state.delete(ns, key);
                }
            }
            self.last_applied = next;
        }
    }

    // ---- 快照 -------------------------------------------------------------

    fn on_install_snapshot(
        &mut self,
        leader_id: String,
        term: u64,
        last_included_index: u64,
        last_included_term: u64,
        data: SnapshotData,
        now: u64,
    ) -> Vec<(String, RaftMessage)> {
        let mut out = Vec::new();
        if term < self.current_term {
            out.push((
                leader_id.clone(),
                RaftMessage::InstallSnapshotResp {
                    term: self.current_term,
                    from_id: self.id.clone(),
                    success: false,
                },
            ));
            return out;
        }
        if term > self.current_term || self.role != Role::Follower {
            self.become_follower(term);
        } else {
            self.current_term = term;
        }
        self.leader_id = Some(leader_id.clone());
        self.reset_election_timer(now);
        self.leader_lease_until = now + self.cfg.election_timeout_ms;

        // 丢弃已包含在快照里的日志前缀
        if last_included_index as usize <= self.log.len() {
            let keep_from = last_included_index as usize + 1;
            let suffix: Vec<LogEntry> = self.log[keep_from.min(self.log.len())..].to_vec();
            self.log = vec![LogEntry {
                index: 0,
                term: 0,
                command: Command::Delete {
                    ns: String::new(),
                    key: String::new(),
                },
            }];
            self.log.extend(suffix);
        }
        let _ = last_included_term;
        self.state.load(data);
        self.commit_index = self.commit_index.max(last_included_index);
        self.last_applied = self.last_applied.max(last_included_index);
        out.push((
            leader_id.clone(),
            RaftMessage::InstallSnapshotResp {
                term: self.current_term,
                from_id: self.id.clone(),
                success: true,
            },
        ));
        out
    }
}

/// 投票成员上限（奇数，避免平票）。
pub const MAX_VOTING_MEMBERS: usize = 9;

/// HA 配置校验（`Config::validate` 调用）。
///
/// `enabled=false` 时全部跳过——老配置缺该段 = 行为与 v0.7 逐字相同。
pub fn validate(cfg: &crate::config::HaConfig, node_id: &str) -> anyhow::Result<()> {
    if !cfg.enabled {
        return Ok(());
    }
    if cfg.cluster.is_empty() {
        anyhow::bail!("ha.cluster must not be empty when ha.enabled");
    }
    let mut cluster_ids = Vec::new();
    for entry in &cfg.cluster {
        let (id, addr) = entry
            .split_once('@')
            .ok_or_else(|| anyhow::anyhow!("ha.cluster entry must be `node_id@host:port`"))?;
        if id.trim().is_empty() || addr.trim().is_empty() {
            anyhow::bail!("ha.cluster entry must be `node_id@host:port`");
        }
        if !addr.contains(':') {
            anyhow::bail!("ha.cluster entry {entry:?} must carry host:port");
        }
        cluster_ids.push(id.to_string());
    }
    if !cluster_ids.iter().any(|id| id == node_id) {
        anyhow::bail!("ha.cluster must contain this node's node_id ({node_id})");
    }
    let voting: Vec<&String> = if cfg.voting_members.is_empty() {
        cluster_ids.iter().collect()
    } else {
        cfg.voting_members.iter().collect()
    };
    if voting.is_empty() {
        anyhow::bail!("ha.voting_members must not be empty");
    }
    if voting.len().is_multiple_of(2) {
        anyhow::bail!("ha.voting_members must be odd (avoid split votes)");
    }
    if voting.len() > MAX_VOTING_MEMBERS {
        anyhow::bail!("ha.voting_members must be <= {MAX_VOTING_MEMBERS}");
    }
    for id in &voting {
        if !cluster_ids.iter().any(|c| c.as_str() == id.as_str()) {
            anyhow::bail!("ha.voting_members contains {id:?} which is not in ha.cluster");
        }
    }
    for id in &cfg.learners {
        if !cluster_ids.iter().any(|c| c.as_str() == id.as_str()) {
            anyhow::bail!("ha.learners contains {id:?} which is not in ha.cluster");
        }
        if voting.iter().any(|v| v.as_str() == id.as_str()) {
            anyhow::bail!("ha node {id:?} cannot be both voting member and learner");
        }
    }
    if !(300..=5000).contains(&cfg.election_timeout_ms) {
        anyhow::bail!("ha.election_timeout_ms must be 300-5000");
    }
    if !(50..=1000).contains(&cfg.heartbeat_interval_ms) {
        anyhow::bail!("ha.heartbeat_interval_ms must be 50-1000");
    }
    if cfg.election_timeout_ms <= cfg.heartbeat_interval_ms * 3 {
        anyhow::bail!("ha.election_timeout_ms must be > heartbeat_interval_ms * 3");
    }
    if !(1000..=10_000_000).contains(&cfg.snapshot_threshold_entries) {
        anyhow::bail!("ha.snapshot_threshold_entries must be 1000-10000000");
    }
    if cfg.read_consistency != "leader" && cfg.read_consistency != "local" {
        anyhow::bail!("ha.read_consistency must be 'leader' or 'local'");
    }
    if cfg.fsync != "batch" && cfg.fsync != "always" {
        anyhow::bail!("ha.fsync must be 'batch' or 'always'");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 内存网络：把消息投递到目标节点，返回新产生的消息。
    struct Net {
        nodes: HashMap<String, RaftNode>,
        queue: Vec<(String, String, RaftMessage)>,
        now: u64,
        dropped: HashSet<String>,
    }

    impl Net {
        fn new(ids: &[&str]) -> Self {
            let mut nodes = HashMap::new();
            for (i, id) in ids.iter().enumerate() {
                let peers: Vec<String> = ids
                    .iter()
                    .enumerate()
                    .filter(|(j, _)| j != &i)
                    .map(|(_, o)| o.to_string())
                    .collect();
                nodes.insert(
                    id.to_string(),
                    RaftNode::new(id.to_string(), peers, RaftConfig::default()),
                );
            }
            Net {
                nodes,
                queue: Vec::new(),
                now: 0,
                dropped: HashSet::new(),
            }
        }

        fn tick(&mut self, dt: u64) {
            self.now += dt;
            let mut out = Vec::new();
            for (id, node) in self.nodes.iter_mut() {
                if self.dropped.contains(id) {
                    continue;
                }
                for (to, msg) in node.tick(self.now) {
                    out.push((id.clone(), to, msg));
                }
            }
            self.queue.extend(out);
            self.drain();
        }

        fn drain(&mut self) {
            let mut guard = 0;
            while !self.queue.is_empty() && guard < 10_000 {
                guard += 1;
                let batch: Vec<_> = self.queue.drain(..).collect();
                let mut out = Vec::new();
                for (from, to, msg) in batch {
                    if self.dropped.contains(&from) || self.dropped.contains(&to) {
                        continue;
                    }
                    if let Some(node) = self.nodes.get_mut(&to) {
                        for (t, m) in node.handle(&from, msg, self.now) {
                            out.push((to.clone(), t, m));
                        }
                    }
                }
                self.queue.extend(out);
            }
        }

        fn send(&mut self, from: &str, to: &str, msg: RaftMessage) {
            self.queue.push((from.to_string(), to.to_string(), msg));
            self.drain();
        }

        /// 只统计未被隔离的节点里的 leader（隔离节点可能仍自认 leader）。
        fn active_leaders(&self) -> Vec<String> {
            let mut v: Vec<String> = self
                .nodes
                .iter()
                .filter(|(id, n)| !self.dropped.contains(*id) && n.role() == Role::Leader)
                .map(|(id, _)| id.clone())
                .collect();
            v.sort();
            v
        }

        fn leaders(&self) -> Vec<String> {
            let mut v: Vec<String> = self
                .nodes
                .iter()
                .filter(|(_, n)| n.role() == Role::Leader)
                .map(|(id, _)| id.clone())
                .collect();
            v.sort();
            v
        }
    }

    /// 推进直到出现 leader 或超时。
    fn elect(net: &mut Net, max_ticks: u64) -> Option<String> {
        for _ in 0..max_ticks {
            net.tick(50);
            let ls = net.leaders();
            if ls.len() == 1 {
                // 再跑几拍让日志/心跳稳定
                for _ in 0..6 {
                    net.tick(50);
                }
                return Some(ls[0].clone());
            }
        }
        None
    }

    fn put(ns: &str, key: &str, val: &str) -> Command {
        Command::Put {
            ns: ns.to_string(),
            key: key.to_string(),
            value: val.as_bytes().to_vec(),
        }
    }

    #[test]
    fn elects_exactly_one_leader() {
        let mut net = Net::new(&["n1", "n2", "n3"]);
        let leader = elect(&mut net, 200).expect("应选出 leader");
        assert_eq!(net.leaders(), vec![leader.clone()]);
        // 所有节点 term 一致，且只有 leader 是 Leader 角色
        let term = net.nodes[&leader].term();
        for (id, n) in &net.nodes {
            assert_eq!(n.term(), term, "{id} term 不一致");
            if *id != leader {
                assert_eq!(n.role(), Role::Follower);
            }
        }
    }

    #[test]
    fn vote_not_granted_twice_in_same_term() {
        let mut n1 = RaftNode::new("n1", vec!["n2".into()], RaftConfig::default());
        let req = RaftMessage::RequestVote {
            term: 1,
            candidate_id: "n2".into(),
            last_log_index: 0,
            last_log_term: 0,
            pre_vote: false,
        };
        let out = n1.handle("n2", req.clone(), 0);
        let granted = matches!(
            out.first().map(|(_, m)| m),
            Some(RaftMessage::RequestVoteResp { granted: true, .. })
        );
        assert!(granted, "首个候选应拿到票");
        // 同 term 第二个候选 -> 拒绝
        let req2 = RaftMessage::RequestVote {
            term: 1,
            candidate_id: "n3".into(),
            last_log_index: 0,
            last_log_term: 0,
            pre_vote: false,
        };
        let out2 = n1.handle("n3", req2, 0);
        let granted2 = matches!(
            out2.first().map(|(_, m)| m),
            Some(RaftMessage::RequestVoteResp { granted: true, .. })
        );
        assert!(!granted2, "同 term 只能投一票");
    }

    #[test]
    fn candidate_with_stale_log_is_rejected() {
        // n1 有一条已提交日志，n2 空日志 -> n2 拿不到票
        let mut net = Net::new(&["n1", "n2", "n3"]);
        elect(&mut net, 200).expect("选主");
        let leader = net.leaders()[0].clone();
        net.nodes
            .get_mut(&leader)
            .unwrap()
            .propose(put("task", "t1", "v1"));
        net.tick(50);
        net.tick(50);
        // 让 n1 有日志、n2 没有
        let stale = RaftMessage::RequestVote {
            term: net.nodes[&leader].term() + 1,
            candidate_id: "n2".into(),
            last_log_index: 0,
            last_log_term: 0,
            pre_vote: false,
        };
        let out = net
            .nodes
            .get_mut("n1")
            .unwrap()
            .handle("n2", stale, net.now);
        let granted = matches!(
            out.first().map(|(_, m)| m),
            Some(RaftMessage::RequestVoteResp { granted: true, .. })
        );
        assert!(!granted, "日志落后的候选不应拿到票");
    }

    #[test]
    fn replicates_and_applies_to_all_nodes() {
        let mut net = Net::new(&["n1", "n2", "n3"]);
        let leader = elect(&mut net, 200).expect("选主");
        net.nodes
            .get_mut(&leader)
            .unwrap()
            .propose(put("task", "t1", "hello"));
        net.tick(50);
        net.tick(50);
        net.tick(50);
        for (id, n) in &net.nodes {
            assert_eq!(
                n.state().get("task", "t1").map(|v| v.as_slice()),
                Some(b"hello".as_slice()),
                "{id} 未应用"
            );
        }
        // 删除也复制
        net.nodes
            .get_mut(&leader)
            .unwrap()
            .propose(Command::Delete {
                ns: "task".into(),
                key: "t1".into(),
            });
        net.tick(50);
        net.tick(50);
        for (id, n) in &net.nodes {
            assert!(n.state().get("task", "t1").is_none(), "{id} 删除未生效");
        }
    }

    #[test]
    fn new_leader_keeps_committed_entries() {
        let mut net = Net::new(&["n1", "n2", "n3"]);
        let leader = elect(&mut net, 200).expect("选主");
        net.nodes
            .get_mut(&leader)
            .unwrap()
            .propose(put("task", "committed", "v"));
        net.tick(50);
        net.tick(50);
        net.tick(50);
        // 隔离 leader
        net.dropped.insert(leader.clone());
        // 其余节点重新选主并保留已提交日志
        let mut new_leader = None;
        for _ in 0..200 {
            net.tick(50);
            let ls = net.active_leaders();
            if ls.len() == 1 && ls[0] != leader {
                new_leader = Some(ls[0].clone());
                break;
            }
        }
        let new_leader = new_leader.expect("应选出新 leader");
        assert!(net.nodes[&new_leader]
            .state()
            .get("task", "committed")
            .is_some());
        // 恢复后旧 leader 应被纠正
        net.dropped.remove(&leader);
        net.tick(50);
        net.tick(50);
        net.tick(50);
        assert_eq!(
            net.nodes[&leader]
                .state()
                .get("task", "committed")
                .map(|v| v.as_slice()),
            Some(b"v".as_slice()),
            "旧 leader 恢复后应追平"
        );
    }

    #[test]
    fn minority_partition_cannot_commit() {
        let mut net = Net::new(&["n1", "n2", "n3"]);
        let leader = elect(&mut net, 200).expect("选主");
        // 隔离两个节点，只留 leader 单机 -> 无 quorum，不能提交
        net.dropped.insert("n2".into());
        net.dropped.insert("n3".into());
        let idx = net
            .nodes
            .get_mut(&leader)
            .unwrap()
            .propose(put("task", "x", "1"));
        assert!(idx.is_some());
        net.tick(50);
        net.tick(50);
        assert_eq!(net.nodes[&leader].commit_index(), 0, "无多数派时不得提交");
        assert!(net.nodes[&leader].state().get("task", "x").is_none());
    }

    #[test]
    fn conflicting_suffix_is_overwritten() {
        // 直接构造冲突：n2 有一条 term=1 的日志，n1 发起 term=2 的复制
        let mut n1 = RaftNode::new("n1", vec!["n2".into()], RaftConfig::default());
        n1.current_term = 2;
        n1.log.push(LogEntry {
            index: 1,
            term: 2,
            command: put("task", "k", "leader-value"),
        });
        let msg = RaftMessage::AppendEntries {
            term: 2,
            leader_id: "n1".into(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![LogEntry {
                index: 1,
                term: 2,
                command: put("task", "k", "leader-value"),
            }],
            leader_commit: 1,
        };
        let mut n2 = RaftNode::new("n2", vec!["n1".into()], RaftConfig::default());
        n2.current_term = 1;
        n2.log.push(LogEntry {
            index: 1,
            term: 1,
            command: put("task", "k", "stale-value"),
        });
        let out = n2.handle("n1", msg, 0);
        assert!(matches!(
            out.first().map(|(_, m)| m),
            Some(RaftMessage::AppendEntriesResp { success: true, .. })
        ));
        assert_eq!(
            n2.state().get("task", "k").map(|v| v.as_slice()),
            Some(b"leader-value".as_slice())
        );
        assert_eq!(n2.log_len(), 1, "冲突后缀应被截断");
    }

    #[test]
    fn snapshot_install_restores_state() {
        let mut net = Net::new(&["n1", "n2", "n3"]);
        let leader = elect(&mut net, 200).expect("选主");
        net.nodes
            .get_mut(&leader)
            .unwrap()
            .propose(put("kv", "a", "1"));
        net.tick(50);
        net.tick(50);
        net.tick(50);
        // 手工构造快照并安装到 n3
        let data = net.nodes[&leader].state().dump();
        let term = net.nodes[&leader].term();
        let idx = net.nodes[&leader].last_applied();
        let msg = RaftMessage::InstallSnapshot {
            term,
            leader_id: leader.clone(),
            last_included_index: idx,
            last_included_term: term,
            data: data.clone(),
        };
        net.nodes
            .get_mut("n3")
            .unwrap()
            .handle(&leader, msg, net.now);
        assert_eq!(
            net.nodes["n3"].state().get("kv", "a").map(|v| v.as_slice()),
            Some(b"1".as_slice()),
            "快照应恢复状态机"
        );
        assert!(net.nodes["n3"].last_applied() >= idx);
    }

    #[test]
    fn pre_vote_does_not_inflate_term_of_isolated_node() {
        let mut net = Net::new(&["n1", "n2", "n3"]);
        let leader = elect(&mut net, 200).expect("选主");
        let term_before = net.nodes[&leader].term();
        // 隔离一个节点，让它反复超时选举
        net.dropped.insert("n2".into());
        let term_n2_before = net.nodes["n2"].term();
        for _ in 0..50 {
            net.tick(100);
        }
        // 在任 leader 的 term 不应被孤立节点抬高
        assert_eq!(
            net.nodes[&leader].term(),
            term_before,
            "pre-vote 不应抬高 leader 的 term"
        );
        // 孤立节点自己的 term 也不该增长（pre-vote 不落 term）
        assert_eq!(
            net.nodes["n2"].term(),
            term_n2_before,
            "pre-vote 不应抬高孤立节点的 term"
        );
    }

    #[test]
    fn persistent_state_survives_restart() {
        let mut net = Net::new(&["n1", "n2", "n3"]);
        let leader = elect(&mut net, 200).expect("选主");
        net.nodes
            .get_mut(&leader)
            .unwrap()
            .propose(put("task", "t", "v"));
        net.tick(50);
        net.tick(50);
        let saved = net.nodes[&leader].persistent();
        let mut restarted = RaftNode::new(
            leader.clone(),
            vec!["n2".into(), "n3".into()],
            RaftConfig::default(),
        );
        restarted.restore(saved);
        assert_eq!(restarted.term(), net.nodes[&leader].term());
        assert_eq!(restarted.log_len(), net.nodes[&leader].log_len());
    }

    #[test]
    fn kv_state_machine_basics() {
        let mut s = KvState::default();
        s.put("ns", "k", b"v".to_vec());
        assert_eq!(
            s.get("ns", "k").map(|v| v.as_slice()),
            Some(b"v".as_slice())
        );
        assert_eq!(s.len(), 1);
        let dump = s.dump();
        let mut s2 = KvState::default();
        s2.load(dump);
        assert_eq!(
            s2.get("ns", "k").map(|v| v.as_slice()),
            Some(b"v".as_slice())
        );
        s2.delete("ns", "k");
        assert!(s2.get("ns", "k").is_none());
        assert!(s2.is_empty());
    }

    // ---- 配置校验 ---------------------------------------------------------

    fn ha_cfg() -> crate::config::HaConfig {
        crate::config::HaConfig {
            enabled: true,
            cluster: vec!["n1@10.0.0.1:8766".into(), "n2@10.0.0.2:8766".into()],
            voting_members: vec!["n1".into()],
            learners: vec!["n2".into()],
            ..Default::default()
        }
    }

    #[test]
    fn validate_ha_config_rules() {
        // 关闭态一律放行
        let mut c = ha_cfg();
        c.enabled = false;
        assert!(validate(&c, "n1").is_ok());

        let c = ha_cfg();
        assert!(validate(&c, "n1").is_ok(), "合法配置应通过");
        // 自身不在 cluster -> 拒绝（避免孤立节点自任 leader）
        assert!(validate(&c, "n9").is_err());
        // cluster 格式错误
        let mut c2 = c.clone();
        c2.cluster = vec!["n1".into()];
        assert!(validate(&c2, "n1").is_err());
        // 投票成员必须是奇数
        let mut c3 = c.clone();
        c3.voting_members = vec!["n1".into(), "n2".into()];
        c3.learners = vec![];
        assert!(validate(&c3, "n1").is_err(), "偶数投票成员应拒绝");
        // 投票成员不在 cluster
        let mut c4 = c.clone();
        c4.voting_members = vec!["nX".into()];
        assert!(validate(&c4, "n1").is_err());
        // 同一节点不能既是投票成员又是 learner
        let mut c5 = c.clone();
        c5.learners = vec!["n1".into()];
        assert!(validate(&c5, "n1").is_err());
        // 超时参数
        let mut c6 = c.clone();
        c6.election_timeout_ms = 100;
        assert!(validate(&c6, "n1").is_err());
        let mut c7 = c.clone();
        c7.heartbeat_interval_ms = 500;
        assert!(validate(&c7, "n1").is_err(), "选举超时应 > 心跳*3");
        // 读一致性/落盘策略
        let mut c8 = c.clone();
        c8.read_consistency = "any".into();
        assert!(validate(&c8, "n1").is_err());
        let mut c9 = c.clone();
        c9.fsync = "never".into();
        assert!(validate(&c9, "n1").is_err());
    }
}
