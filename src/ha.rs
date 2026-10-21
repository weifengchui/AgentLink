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
use std::sync::{Condvar, Mutex as StdMutex};
use std::time::{Duration, Instant};

/// v0.8 M4：业务命名空间（与设计 §3.3 一一对应）。
pub const NS_TASK: &str = "task";
pub const NS_QUOTA: &str = "quota";
pub const NS_AUDIT: &str = "audit";
pub const NS_OIDC: &str = "oidc";
pub const NS_CFG: &str = "cfg";

/// 业务写入等待 apply 的默认上限。超过即视为多数派不可用（503）。
pub const APPLY_TIMEOUT: Duration = Duration::from_secs(3);

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
    /// 成员变更（joint consensus）：提交后 `voters` 成为生效配置。
    SetMembership {
        voters: Vec<String>,
        #[serde(default)]
        learners: Vec<String>,
    },
}

/// 规范化成员列表（去空白、去重、排序）。
pub fn normalize_members(list: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut v: Vec<String> = list
        .into_iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    v.sort();
    v.dedup();
    v
}

/// 校验成员配置（动态变更用；静态配置校验见 [`validate`]）。
pub fn validate_membership(voters: &[String], learners: &[String]) -> Result<(), String> {
    if voters.is_empty() {
        return Err("voters must not be empty".into());
    }
    if voters.len() > MAX_VOTING_MEMBERS {
        return Err(format!("voters must be <= {MAX_VOTING_MEMBERS}"));
    }
    if voters.len() != normalize_members(voters.iter().cloned()).len() {
        return Err("voters must not contain duplicates or blanks".into());
    }
    if learners.len() != normalize_members(learners.iter().cloned()).len() {
        return Err("learners must not contain duplicates or blanks".into());
    }
    if let Some(dup) = learners.iter().find(|l| voters.contains(l)) {
        return Err(format!("{dup:?} cannot be both voter and learner"));
    }
    Ok(())
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

/// 压缩基点的占位日志条目（永不 apply，只承载 `index/term`）。
fn sentinel(index: u64, term: u64) -> LogEntry {
    LogEntry {
        index,
        term,
        command: Command::Delete {
            ns: String::new(),
            key: String::new(),
        },
    }
}

/// 拉票请求参数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoteRequest {
    pub term: u64,
    pub candidate_id: String,
    pub last_log_index: u64,
    pub last_log_term: u64,
    pub pre_vote: bool,
}

/// 追加结果参数（内核内部传递，避免参数过多）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendResponse {
    pub term: u64,
    pub from_id: String,
    pub success: bool,
    pub match_index: u64,
    pub conflict_index: u64,
    /// 对端处理完本次追加后的 commit_index。
    pub commit_index: u64,
    /// 回显的 ReadIndex 序号。
    pub read_seq: u64,
}

/// 追加日志请求参数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendRequest {
    pub term: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
    /// ReadIndex 序号（原样回显）。
    pub read_seq: u64,
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
        /// ReadIndex 序号（原样回显：leader 据此确认「这条 ack 发生在读开始之后」）。
        #[serde(default)]
        read_seq: u64,
    },
    /// 追加结果；失败时带 `conflict_index` 提示回退位置。
    AppendEntriesResp {
        term: u64,
        from_id: String,
        success: bool,
        match_index: u64,
        conflict_index: u64,
        /// 对端处理完本次追加后的 commit_index（leader 判断 drain 是否完成）。
        #[serde(default)]
        commit_index: u64,
        /// 回显 AppendEntries 里的 read_seq（ReadIndex 确认用）。
        #[serde(default)]
        read_seq: u64,
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
        /// 快照点（成功时 leader 据此推进 `match/next_index`）。
        #[serde(default)]
        last_included_index: u64,
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

    /// 按命名空间前缀扫描（key 去掉 `<ns>/` 前缀）。返回顺序为 key 字典序。
    pub fn scan(&self, ns: &str) -> Vec<(String, Vec<u8>)> {
        let prefix = format!("{ns}/");
        self.data
            .iter()
            .filter_map(|(k, v)| {
                k.strip_prefix(&prefix)
                    .map(|rest| (rest.to_string(), v.clone()))
            })
            .collect()
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
    /// 日志（只含真实条目，不含压缩基点哨兵；`base` 记录基点）。
    pub log: Vec<LogEntry>,
    /// 已压缩到的基点 `(index, term)`；`(0, 0)` 表示未压缩。
    pub base: (u64, u64),
    /// 已应用位点：重启后据此把日志重放进状态机（0 = 从头重放）。
    pub applied_index: u64,
    /// 已提交的投票成员（缺省空 = 由调用方配置决定）。
    pub voters: Vec<String>,
    /// 已提交的 learners。
    pub learners: Vec<String>,
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
    /// `log[0]` 是压缩基点哨兵（`index=base/term=base_term`，初始 `0/0`）；
    /// `log[i]` 对应 `index = log[0].index + i`。压缩只丢弃前缀，不改后缀索引。
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
    /// 日志是否需要整文件重写（压缩 / 冲突截断后为真）。
    log_rewrite: bool,
    /// 生效投票成员（joint 阶段 = 目标新配置）。
    voters: Vec<String>,
    /// 已提交（已应用）的投票成员：joint 结束后 = `voters`。
    applied_voters: Vec<String>,
    /// 生效 learners（只复制、不投票）。
    learners: Vec<String>,
    applied_learners: Vec<String>,
    /// joint 阶段仍需同时满足多数派的旧配置。
    joint_old: Option<Vec<String>>,
    /// 已被移出配置、但还没追平日志的节点（leader 继续喂日志，避免其
    /// 停留在旧配置里反复发起无效选举）。
    draining: HashSet<String>,
    /// ReadIndex：leader 侧单调递增的读序号。
    read_seq: u64,
    /// 各对端已确认到的 ReadIndex 序号（`AppendEntriesResp.read_seq` 回显）。
    read_ack: HashMap<String, u64>,
    /// 尚未完成的读：`(seq, read_index, 起始时刻)`。
    pending_reads: Vec<(u64, u64, u64)>,
}

impl RaftNode {
    /// 新建节点。`peers` 不含自己；投票成员缺省 = `peers + 自己`（与 M1/M2 一致）。
    pub fn new(id: impl Into<String>, peers: Vec<String>, cfg: RaftConfig) -> Self {
        let id = id.into();
        let mut voters = peers.clone();
        voters.push(id.clone());
        Self::with_membership(id, peers, voters, Vec::new(), cfg)
    }

    /// 按显式成员配置新建节点（`voters` 必须含自己，否则只能当 learner）。
    pub fn with_membership(
        id: impl Into<String>,
        peers: Vec<String>,
        voters: Vec<String>,
        learners: Vec<String>,
        cfg: RaftConfig,
    ) -> Self {
        let id = id.into();
        let mut rng = 0x9E37_79B9_7F4A_7C15u64;
        for b in id.as_bytes() {
            rng = rng.rotate_left(5) ^ (*b as u64).wrapping_mul(0x100_0000_01B3);
        }
        let mut voters = normalize_members(voters);
        if voters.is_empty() {
            voters.push(id.clone());
        }
        let learners = normalize_members(learners);
        RaftNode {
            id,
            peers,
            cfg,
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: vec![sentinel(0, 0)],
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
            log_rewrite: false,
            applied_voters: voters.clone(),
            voters,
            applied_learners: learners.clone(),
            learners,
            joint_old: None,
            draining: HashSet::new(),
            read_seq: 0,
            read_ack: HashMap::new(),
            pending_reads: Vec::new(),
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

    /// 本 term 投给谁（持久化边界）。
    pub fn voted_for(&self) -> Option<&str> {
        self.voted_for.as_deref()
    }

    /// 取 `index >= from` 的日志条目（持久化增量用；不含哨兵之外的过滤）。
    pub fn log_entries_from(&self, from: u64) -> &[LogEntry] {
        let start = self.offset(from).unwrap_or(0).min(self.log.len());
        &self.log[start..]
    }

    /// 取 `index > after` 的真实日志条目（不含哨兵）。
    pub fn log_entries_after(&self, after: u64) -> Vec<LogEntry> {
        let from = after.max(self.base_index()) + 1;
        self.log_entries_from(from).to_vec()
    }

    /// 日志已压缩到的位点（= 快照点）；0 表示未压缩。
    pub fn snapshot_index(&self) -> u64 {
        self.base_index()
    }

    /// 集群规模（投票成员数）。
    pub fn cluster_size(&self) -> usize {
        self.voters.len()
    }

    /// 多数派票数（joint 阶段仍需同时满足旧配置多数派）。
    pub fn quorum(&self) -> usize {
        self.cluster_size() / 2 + 1
    }

    /// 是否已连上多数派（有 leader 或已提交过日志）。
    pub fn has_quorum(&self) -> bool {
        self.role == Role::Leader || self.commit_index > 0
    }

    /// 生效投票成员。
    pub fn voters(&self) -> &[String] {
        &self.voters
    }

    /// 只复制不投票的成员。
    pub fn learners(&self) -> &[String] {
        &self.learners
    }

    /// joint 阶段仍在生效的旧配置（`None` = 非 joint）。
    pub fn joint(&self) -> Option<&[String]> {
        self.joint_old.as_deref()
    }

    /// 已提交的投票成员（对外展示「稳定配置」）。
    pub fn committed_voters(&self) -> &[String] {
        &self.applied_voters
    }

    pub fn committed_learners(&self) -> &[String] {
        &self.applied_learners
    }

    /// 本节点是否拥有投票权（joint 阶段旧配置成员也算）。
    fn self_is_voter(&self) -> bool {
        self.voters.contains(&self.id)
            || self
                .joint_old
                .as_ref()
                .is_some_and(|old| old.contains(&self.id))
    }

    /// 某个集合是否达到多数派。
    fn majority_of(set: &[String], acked: &HashSet<String>) -> bool {
        if set.is_empty() {
            return false;
        }
        let have = set.iter().filter(|v| acked.contains(*v)).count();
        have > set.len() / 2
    }

    /// 是否达到多数派（joint 阶段需新旧双多数派）。
    fn quorum_reached(&self, acked: &HashSet<String>) -> bool {
        if !Self::majority_of(&self.voters, acked) {
            return false;
        }
        match &self.joint_old {
            Some(old) => Self::majority_of(old, acked),
            None => true,
        }
    }

    /// 需要收拉票的对端（投票成员 + joint 旧配置，排除自己）。
    fn vote_targets(&self) -> Vec<String> {
        let mut t: Vec<String> = self.voters.clone();
        if let Some(old) = &self.joint_old {
            t.extend(old.iter().cloned());
        }
        t.sort();
        t.dedup();
        t.retain(|id| id != &self.id);
        t
    }

    /// 需要复制日志的对端（投票成员 + joint 旧配置 + learners，排除自己）。
    fn replication_targets(&self) -> Vec<String> {
        let mut t: Vec<String> = self.voters.clone();
        if let Some(old) = &self.joint_old {
            t.extend(old.iter().cloned());
        }
        t.extend(self.learners.iter().cloned());
        t.extend(self.draining.iter().cloned());
        t.sort();
        t.dedup();
        t.retain(|id| id != &self.id);
        t
    }

    /// 维护 `next_index/match_index`：补齐新成员、清掉已移出的成员。
    fn sync_trackers(&mut self) {
        let targets = self.replication_targets();
        self.next_index.retain(|k, _| targets.contains(k));
        self.match_index
            .retain(|k, _| targets.contains(k) || k == &self.id);
        let last = self.last_log_index();
        for t in targets {
            self.next_index.entry(t.clone()).or_insert(last + 1);
            self.match_index.entry(t).or_insert(0);
        }
    }

    /// 需要持久化的状态。
    pub fn persistent(&self) -> PersistentState {
        PersistentState {
            current_term: self.current_term,
            voted_for: self.voted_for.clone(),
            log: self.log_entries_after(self.base_index()),
            base: (self.base_index(), self.base_term()),
            applied_index: self.last_applied,
            voters: self.applied_voters.clone(),
            learners: self.applied_learners.clone(),
        }
    }

    /// 恢复持久化状态（重启后调用）。
    pub fn restore(&mut self, p: PersistentState) {
        self.current_term = p.current_term;
        self.voted_for = p.voted_for;
        // 成员配置：持久化过（= 状态机里已提交）就以它为准。
        if !p.voters.is_empty() {
            self.voters = normalize_members(p.voters.iter().cloned());
            self.applied_voters = self.voters.clone();
            self.learners = normalize_members(p.learners.iter().cloned());
            self.applied_learners = self.learners.clone();
            self.joint_old = None;
            self.sync_trackers();
        }
        let (base_index, base_term) = p.base;
        self.log = Vec::with_capacity(p.log.len() + 1);
        self.log.push(sentinel(base_index, base_term));
        self.log
            .extend(p.log.into_iter().filter(|e| e.index > base_index));
        // 已应用位点必为已提交：重启后据此重放，避免「全员重启后状态机为空」。
        let base = self.base_index();
        let applied = p.applied_index.max(base).min(self.last_log_index());
        self.commit_index = applied;
        self.last_applied = base;
        self.apply_committed();
    }

    /// 直接把快照内容灌入状态机（启动恢复用；restore 负责位点与日志）。
    pub fn load_snapshot_state(&mut self, data: SnapshotData) {
        self.state.load(data);
    }

    /// 状态机快照内容（`None` 表示还没到压缩阈值）。**不压缩**，便于先落盘再压缩。
    pub fn pending_snapshot(&self) -> Option<(u64, u64, SnapshotData)> {
        if !self.should_compact() {
            return None;
        }
        Some((
            self.last_applied,
            self.log_term_at(self.last_applied),
            self.state.dump(),
        ))
    }

    /// 累计已应用但未压缩的条目是否达到阈值。
    pub fn should_compact(&self) -> bool {
        self.last_applied > 0
            && self.last_applied.saturating_sub(self.base_index())
                >= self.cfg.snapshot_threshold_entries
    }

    /// 在 `last_applied` 处压缩日志前缀（调用方负责先把快照落盘）。
    pub fn compact_now(&mut self) -> Option<(u64, u64)> {
        if !self.should_compact() {
            return None;
        }
        let idx = self.last_applied;
        let term = self.log_term_at(idx);
        self.compact_to(idx, term);
        self.log_rewrite = true;
        Some((idx, term))
    }

    /// 导出快照并立即压缩（内存网络 / 单测用）。
    pub fn maybe_compact(&mut self) -> Option<(u64, u64, SnapshotData)> {
        let snap = self.pending_snapshot()?;
        self.compact_to(snap.0, snap.1);
        self.log_rewrite = true;
        Some(snap)
    }

    /// 状态机快照内容（旧接口保留：等价于 `pending_snapshot`）。
    pub fn snapshot(&self) -> Option<(u64, u64, SnapshotData)> {
        self.pending_snapshot()
    }

    /// 压缩到 `(idx, term)`：只保留 `index > idx` 的条目，`log[0]` 换成新哨兵。
    fn compact_to(&mut self, idx: u64, term: u64) {
        if idx <= self.base_index() {
            return;
        }
        let keep: Vec<LogEntry> = self.log.iter().filter(|e| e.index > idx).cloned().collect();
        self.log = Vec::with_capacity(keep.len() + 1);
        self.log.push(sentinel(idx, term));
        self.log.extend(keep);
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

    /// 压缩基点（= 快照点）。
    fn base_index(&self) -> u64 {
        self.log.first().map(|e| e.index).unwrap_or(0)
    }

    fn base_term(&self) -> u64 {
        self.log.first().map(|e| e.term).unwrap_or(0)
    }

    /// index → log 下标（低于基点 = 已被压缩，返回 None）。
    fn offset(&self, index: u64) -> Option<usize> {
        let base = self.base_index();
        if index < base {
            None
        } else {
            Some((index - base) as usize)
        }
    }

    fn log_term_at(&self, index: u64) -> u64 {
        self.entry_at(index).map(|e| e.term).unwrap_or(0)
    }

    fn entry_at(&self, index: u64) -> Option<&LogEntry> {
        let o = self.offset(index)?;
        self.log.get(o)
    }

    /// 日志是否需要整文件重写（压缩/截断后）。
    pub fn log_rewrite_needed(&self) -> bool {
        self.log_rewrite
    }

    pub fn clear_log_rewrite(&mut self) {
        self.log_rewrite = false;
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
        // 不再是 leader：未完成的强一致读全部作废（等新 leader 再发起）。
        self.pending_reads.clear();
        self.read_ack.clear();
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
                read_seq,
            } => {
                let a = AppendRequest {
                    term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                    read_seq,
                };
                out.extend(self.on_append_entries(leader_id, &a, now));
            }
            RaftMessage::AppendEntriesResp {
                term,
                from_id,
                success,
                match_index,
                conflict_index,
                commit_index,
                read_seq,
            } => {
                let r = AppendResponse {
                    term,
                    from_id,
                    success,
                    match_index,
                    conflict_index,
                    commit_index,
                    read_seq,
                };
                out.extend(self.on_append_resp(&r, now));
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
                last_included_index,
            } => {
                if term > self.current_term {
                    self.become_follower(term);
                }
                if self.role == Role::Leader && success {
                    let m = last_included_index
                        .min(self.last_log_index())
                        .max(self.match_index.get(&from_id).copied().unwrap_or(0));
                    self.match_index.insert(from_id.clone(), m);
                    self.next_index.insert(from_id.clone(), m + 1);
                    // 快照点已提交；若后端还有条目，继续流水线补发。
                    if self.next_index.get(&from_id).copied().unwrap_or(1) <= self.last_log_index()
                    {
                        let msg = self.append_msg_for(&from_id);
                        out.push((from_id.clone(), msg));
                    }
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
        tracing::debug!(index, log_len = self.log.len(), "ha: propose");
        self.log.push(LogEntry {
            index,
            term: self.current_term,
            command,
        });
        self.match_index.insert(self.id.clone(), index);
        Some(index)
    }

    /// leader 发起成员变更（joint consensus）。成功返回日志 index。
    ///
    /// 语义：追加 `SetMembership` 后**立即**切到 joint（新旧配置双多数派），
    /// 该条提交后 `apply_committed` 撤销 joint、新配置正式生效。
    pub fn propose_membership(
        &mut self,
        voters: Vec<String>,
        learners: Vec<String>,
    ) -> Result<u64, String> {
        if self.role != Role::Leader {
            return Err("not the leader".into());
        }
        if self.joint_old.is_some() {
            return Err("membership change already in progress".into());
        }
        let voters = normalize_members(voters);
        let learners = normalize_members(learners);
        validate_membership(&voters, &learners)?;
        if voters == self.voters && learners == self.learners {
            return Err("membership unchanged".into());
        }
        // Raft §6：一次只增删一个投票成员（多成员变更会破坏 joint 的重叠多数派）。
        let changed = voters
            .iter()
            .filter(|v| !self.applied_voters.contains(v))
            .count()
            + self
                .applied_voters
                .iter()
                .filter(|v| !voters.contains(v))
                .count();
        if changed > 1 {
            return Err("only one voting member may be added or removed per change".into());
        }
        let index = self.last_log_index() + 1;
        self.log.push(LogEntry {
            index,
            term: self.current_term,
            command: Command::SetMembership {
                voters: voters.clone(),
                learners: learners.clone(),
            },
        });
        self.match_index.insert(self.id.clone(), index);
        // 立即进入 joint：旧配置 = 当前已提交配置。
        self.joint_old = Some(self.applied_voters.clone());
        self.voters = voters;
        self.learners = learners;
        self.sync_trackers();
        tracing::info!(
            index,
            voters = ?self.voters,
            joint = ?self.joint_old,
            "ha: membership change proposed (joint consensus)"
        );
        Ok(index)
    }

    // ---- ReadIndex（强一致读）--------------------------------------------

    /// 发起一次线性一致读（仅 leader）。返回 `(seq, read_index)`。
    ///
    /// 语义：读完 `commit_index >= read_index` 且本轮心跳拿到多数派确认
    /// （证明「读开始之后仍然只有我是 leader」）之后才可读状态机。
    pub fn begin_read(&mut self, now: u64) -> Option<(u64, u64)> {
        if self.role != Role::Leader {
            return None;
        }
        self.read_seq += 1;
        let seq = self.read_seq;
        let read_index = self.commit_index;
        // 顺带清理过期读（调用方超时放弃的），避免无限增长。
        self.pending_reads
            .retain(|(_, _, at)| now.saturating_sub(*at) < 10_000);
        if self.pending_reads.len() >= 64 {
            self.pending_reads.remove(0);
        }
        self.pending_reads.push((seq, read_index, now));
        // 让心跳立刻发生（不等下一个 tick），缩短确认时延。
        self.heartbeat_at = now + self.cfg.heartbeat_interval_ms.max(1);
        Some((seq, read_index))
    }

    /// 立刻广播一轮心跳（ReadIndex 确认 / 新 leader 快速建立权威）。
    pub fn broadcast_heartbeat(&mut self, now: u64) -> Vec<(String, RaftMessage)> {
        if self.role != Role::Leader {
            return Vec::new();
        }
        self.heartbeat_at = now + self.cfg.heartbeat_interval_ms.max(1);
        self.broadcast_append(now)
    }

    /// 读是否已经安全：多数派已确认本轮的读序号，且状态机已应用到 read_index。
    /// 返回 `read_index` 表示可以读了。
    pub fn read_ready(&self, seq: u64) -> Option<u64> {
        if self.role != Role::Leader {
            return None;
        }
        let read_index = self
            .pending_reads
            .iter()
            .find(|(s, _, _)| *s == seq)
            .map(|(_, idx, _)| *idx)?;
        let mut acked: HashSet<String> = self
            .read_ack
            .iter()
            .filter(|(_, a)| **a >= seq)
            .map(|(id, _)| id.clone())
            .collect();
        // leader 自身算一票（它在本任期当选，且这条读由它发起）。
        acked.insert(self.id.clone());
        if !self.quorum_reached(&acked) {
            return None;
        }
        if self.last_applied < read_index {
            return None;
        }
        Some(read_index)
    }

    /// 读完/超时后释放这条读。
    pub fn finish_read(&mut self, seq: u64) {
        self.pending_reads.retain(|(s, _, _)| *s != seq);
    }

    // ---- 选举 -------------------------------------------------------------

    fn start_election(&mut self, now: u64) -> Vec<(String, RaftMessage)> {
        let mut out = Vec::new();
        // learner / 已被移出配置：不参选（重置计时器避免空转）。
        if !self.self_is_voter() {
            self.pre_vote = false;
            self.pre_votes.clear();
            self.reset_election_timer(now);
            return out;
        }
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
        for p in self.vote_targets() {
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
        // learner / 已移出的节点不投票（只复制日志）；非成员候选者也不投票
        // （防「已移除节点靠旧配置重新当选」把已提交的成员变更顶掉）。
        let candidate_is_member = self.voters.contains(&candidate_id)
            || self
                .joint_old
                .as_ref()
                .is_some_and(|old| old.contains(&candidate_id));
        if !self.self_is_voter() || !candidate_is_member {
            out.push((
                from.to_string(),
                RaftMessage::RequestVoteResp {
                    term: self.current_term,
                    voter_id: self.id.clone(),
                    granted: false,
                    pre_vote,
                },
            ));
            return out;
        }
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
                if self.quorum_reached(&self.pre_votes) {
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
            if self.quorum_reached(&self.votes) {
                self.become_leader(now);
                out.extend(self.broadcast_append(now));
            }
        }
        out
    }

    fn become_leader(&mut self, now: u64) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id.clone());
        self.votes.clear();
        self.pre_votes.clear();
        self.pre_vote = false;
        // 新任期：旧的读确认全部作废（必须重新确认 leadership）。
        self.read_ack.clear();
        self.pending_reads.clear();
        // 从日志重建生效配置：已应用配置 + 未提交的成员变更 -> joint。
        self.rebuild_config_from_log();
        self.sync_trackers();
        let last = self.last_log_index();
        for p in self.replication_targets() {
            self.next_index.insert(p.clone(), last + 1);
            self.match_index.insert(p, 0);
        }
        self.heartbeat_at = now + self.cfg.heartbeat_interval_ms.max(1);
        // leader 自己的条目立刻算已复制
        self.match_index.insert(self.id.clone(), last);
    }

    /// 以已提交配置为基线，重放日志里未提交的成员变更（决定是否处于 joint）。
    fn rebuild_config_from_log(&mut self) {
        let mut cur_voters = self.applied_voters.clone();
        let mut cur_learners = self.applied_learners.clone();
        let mut joint: Option<Vec<String>> = None;
        for e in self.log.iter().filter(|e| e.index > self.last_applied) {
            if let Command::SetMembership { voters, learners } = &e.command {
                let voters = normalize_members(voters.iter().cloned());
                if voters != cur_voters && joint.is_none() {
                    joint = Some(cur_voters.clone());
                }
                cur_voters = voters;
                cur_learners = normalize_members(learners.iter().cloned());
            }
        }
        if self.role == Role::Leader {
            tracing::info!(
                voters = ?cur_voters,
                joint = ?joint,
                "ha: leader rebuilt membership from log"
            );
        }
        self.voters = cur_voters;
        self.learners = cur_learners;
        self.joint_old = joint;
    }

    // ---- 日志 -------------------------------------------------------------

    fn broadcast_append(&mut self, now: u64) -> Vec<(String, RaftMessage)> {
        let _ = now;
        let mut out = Vec::new();
        for p in self.replication_targets() {
            let msg = self.append_msg_for(&p);
            out.push((p, msg));
        }
        out
    }

    fn append_msg_for(&self, peer: &str) -> RaftMessage {
        let next = self
            .next_index
            .get(peer)
            .copied()
            .unwrap_or(1)
            .max(1)
            .min(self.last_log_index() + 1);
        // 对端需要的条目已被压缩：改发快照（以 leader 当前已应用点为准）。
        if next <= self.base_index() {
            let idx = self.last_applied.max(self.base_index());
            return RaftMessage::InstallSnapshot {
                term: self.current_term,
                leader_id: self.id.clone(),
                last_included_index: idx,
                last_included_term: self.log_term_at(idx),
                data: self.state.dump(),
            };
        }
        let prev_index = next - 1;
        let prev_term = self.log_term_at(prev_index);
        let entries: Vec<LogEntry> = self
            .log
            .iter()
            .skip((next - self.base_index()) as usize)
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
            read_seq: self.read_seq,
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
        let read_seq = a.read_seq;
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
                    commit_index: self.commit_index,
                    read_seq,
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

        // prev 日志校验（prev 已落在本节点压缩基之内时跳过：基点以内必为已提交）。
        let base = self.base_index();
        let mut entries = entries;
        if prev_log_index > 0 && prev_log_index < base {
            entries.retain(|e| e.index > base);
        } else if prev_log_index > 0 {
            match self.entry_at(prev_log_index) {
                Some(e) if e.term == prev_log_term => {}
                Some(e) => {
                    // 冲突：回退到该 term 的第一个 index
                    let mut idx = prev_log_index;
                    while idx > base + 1 && self.log_term_at(idx - 1) == e.term {
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
                            commit_index: self.commit_index,
                            read_seq,
                        },
                    ));
                    return out;
                }
                None => {
                    // 对端日志比 leader 短：提示「我有到哪」，让 leader 直接跳到下一条
                    // （不能回 prev_log_index，否则 next 原地打转、永远追不上）。
                    out.push((
                        leader_id.clone(),
                        RaftMessage::AppendEntriesResp {
                            term: self.current_term,
                            from_id: self.id.clone(),
                            success: false,
                            match_index: 0,
                            conflict_index: self.last_log_index() + 1,
                            commit_index: self.commit_index,
                            read_seq,
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
                // 冲突 → 截断后重写（落盘需要整文件重写）
                if let Some(o) = self.offset(entry.index) {
                    self.log.truncate(o);
                    self.log_rewrite = true;
                }
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
                // prev 落在压缩基以内时按基点确认（基点以内必为已提交）。
                match_index: (prev_log_index + entries_len).max(base),
                conflict_index: 0,
                commit_index: self.commit_index,
                read_seq,
            },
        ));
        out
    }

    fn on_append_resp(&mut self, r: &AppendResponse, now: u64) -> Vec<(String, RaftMessage)> {
        let mut out = Vec::new();
        let (from_id, term, success, match_index, conflict_index) = (
            r.from_id.clone(),
            r.term,
            r.success,
            r.match_index,
            r.conflict_index,
        );
        let peer_commit = r.commit_index;
        if term > self.current_term {
            self.become_follower(term);
            self.reset_election_timer(now);
            return out;
        }
        if self.role != Role::Leader || term < self.current_term {
            return out;
        }
        // CheckQuorum：投票成员在当前任期的有效回执即为「leader 租约」。
        // 在任 leader 有回执时拒绝 pre-vote；否则 follower 偶发丢一拍心跳
        // 就能把健康 leader 顶下台（term 反复抬升，换主窗口内写入全部超时）。
        if self.voters.contains(&from_id)
            || self
                .joint_old
                .as_ref()
                .is_some_and(|old| old.contains(&from_id))
        {
            self.leader_lease_until = now + self.cfg.election_timeout_ms;
        }
        tracing::debug!(from = %from_id, success, match_index, "ha: append resp");
        if success {
            // ReadIndex：记录该对端最近确认到的读序号（用于判断读是否安全）。
            let acked = self.read_ack.get(&from_id).copied().unwrap_or(0);
            self.read_ack.insert(from_id.clone(), acked.max(r.read_seq));
            let m = match_index.max(self.match_index.get(&from_id).copied().unwrap_or(0));
            self.match_index.insert(from_id.clone(), m);
            self.next_index.insert(from_id.clone(), m + 1);
            self.try_advance_commit();
            // 已移除节点**已应用**到当前提交位点后才停止喂日志。
            // 只看 match_index 不够：commit 可能在该次追加之后才前移，
            // 对端此时还没学到新的 leader_commit（曾导致被移出节点停在旧配置）。
            if self.draining.contains(&from_id) && peer_commit >= self.commit_index {
                self.draining.remove(&from_id);
                self.next_index.remove(&from_id);
                self.match_index.remove(&from_id);
            }
            // 流水线：**仅当对端还没追平**（还有条目要发）才立刻补发下一批。
            // 没有新条目时**不**回发——follower 对每条 AppendEntries 都会回一条
            // resp，若 leader 无条件回发就会形成无限 ping-pong（打满 peer 通道、
            // 把真消息挤掉）。无事可发时等下一次心跳。
            let next = self.next_index.get(&from_id).copied().unwrap_or(1);
            if next <= self.last_log_index() {
                let msg = self.append_msg_for(&from_id);
                out.push((from_id.clone(), msg));
            }
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

    /// 推进 commit_index（多数派 + 当前 term 限制；joint 阶段需新旧双多数派）。
    fn try_advance_commit(&mut self) {
        let Some(candidate) = self.quorum_commit_candidate(&self.voters) else {
            return;
        };
        let candidate = match &self.joint_old {
            Some(old) => match self.quorum_commit_candidate(old) {
                Some(old_idx) => candidate.min(old_idx),
                None => return,
            },
            None => candidate,
        };
        if candidate > self.commit_index && self.log_term_at(candidate) == self.current_term {
            self.commit_index = candidate;
            self.apply_committed();
        }
    }

    /// 给定成员集合：已被多数派复制的最大 index。
    fn quorum_commit_candidate(&self, set: &[String]) -> Option<u64> {
        if set.is_empty() {
            return None;
        }
        let mut indexes: Vec<u64> = set
            .iter()
            .map(|id| {
                if id == &self.id {
                    self.last_log_index()
                } else {
                    self.match_index.get(id).copied().unwrap_or(0)
                }
            })
            .collect();
        indexes.sort_unstable_by(|a, b| b.cmp(a));
        let quorum = set.len() / 2 + 1;
        indexes.get(quorum - 1).copied()
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
                Command::SetMembership { voters, learners } => {
                    // 成员变更已提交：joint 结束，新配置正式生效。
                    let voters = normalize_members(voters.iter().cloned());
                    let learners = normalize_members(learners.iter().cloned());
                    let self_removed = self.role == Role::Leader && !voters.contains(&self.id);
                    if self.role == Role::Leader {
                        // 被移出的成员继续喂日志，直到它确认到当前提交位点；
                        // 否则它会一直停在旧配置里（Raft §6 的「已移除节点」问题）。
                        let old: HashSet<String> = self
                            .applied_voters
                            .iter()
                            .chain(self.applied_learners.iter())
                            .cloned()
                            .collect();
                        for id in old {
                            if !voters.contains(&id) && !learners.contains(&id) {
                                self.draining.insert(id);
                            }
                        }
                    }
                    self.applied_voters = voters.clone();
                    self.applied_learners = learners.clone();
                    self.voters = voters;
                    self.learners = learners;
                    self.joint_old = None;
                    self.sync_trackers();
                    tracing::info!(
                        voters = ?self.voters,
                        learners = ?self.learners,
                        "ha: membership committed"
                    );
                    if self_removed {
                        // 自己被移出投票成员：立刻让位，等新 leader 接管。
                        tracing::warn!(
                            node_id = %self.id,
                            "ha: leader removed from voters, stepping down"
                        );
                        self.become_follower(self.current_term);
                    }
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
                    last_included_index,
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

        // 本节点已有更新的压缩基点：忽略旧快照，直接确认。
        if last_included_index > self.base_index() {
            // 快照点必为已提交：状态机整体覆盖，位点前移。
            self.state.load(data);
            // 日志：仅当本节点在快照点上的 term 一致才保留后缀，否则整段丢弃。
            let keep_suffix = self.log_term_at(last_included_index) == last_included_term;
            let suffix: Vec<LogEntry> = if keep_suffix {
                self.log
                    .iter()
                    .filter(|e| e.index > last_included_index)
                    .cloned()
                    .collect()
            } else {
                Vec::new()
            };
            self.log = Vec::with_capacity(suffix.len() + 1);
            self.log
                .push(sentinel(last_included_index, last_included_term));
            self.log.extend(suffix);
            self.log_rewrite = true;
            self.commit_index = self
                .commit_index
                .max(last_included_index)
                .min(self.last_log_index());
            self.last_applied = last_included_index;
            // 快照已含前缀，后缀若还有已提交条目则补 apply。
            self.apply_committed();
        }
        out.push((
            leader_id.clone(),
            RaftMessage::InstallSnapshotResp {
                term: self.current_term,
                from_id: self.id.clone(),
                success: true,
                last_included_index,
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

// ---- 持久化（M2）--------------------------------------------------------

/// 落盘的状态文件。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct StateFile {
    current_term: u64,
    #[serde(default)]
    voted_for: Option<String>,
    /// 已应用位点（重启后据此重放日志，避免全员重启后状态机为空）。
    #[serde(default)]
    applied_index: u64,
    /// 已提交的投票成员（动态成员变更后以它为准；缺省空 = 用配置文件）。
    #[serde(default)]
    voters: Vec<String>,
    /// 已提交的 learners。
    #[serde(default)]
    learners: Vec<String>,
}

/// 落盘的快照文件。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SnapshotFile {
    last_included_index: u64,
    last_included_term: u64,
    #[serde(default)]
    data: SnapshotData,
}

/// 启动时从磁盘恢复出来的内容。
#[derive(Debug, Clone, Default)]
pub struct HaPersisted {
    pub current_term: u64,
    pub voted_for: Option<String>,
    /// 已应用位点（重启后重放用）。
    pub applied_index: u64,
    /// 已提交的投票成员（空 = 配置文件为准）。
    pub voters: Vec<String>,
    /// 已提交的 learners。
    pub learners: Vec<String>,
    pub log: Vec<LogEntry>,
    pub snapshot: Option<(u64, u64, SnapshotData)>,
}

/// HA 落盘存储：`<data_dir>/ha/` 下 `state.json` / `log.jsonl` / `snapshot.json`。
pub struct HaStore {
    dir: std::path::PathBuf,
    fsync: bool,
}

impl HaStore {
    pub fn new(dir: impl Into<std::path::PathBuf>, fsync: bool) -> Self {
        HaStore {
            dir: dir.into(),
            fsync,
        }
    }

    fn state_path(&self) -> std::path::PathBuf {
        self.dir.join("state.json")
    }

    fn log_path(&self) -> std::path::PathBuf {
        self.dir.join("log.jsonl")
    }

    fn snapshot_path(&self) -> std::path::PathBuf {
        self.dir.join("snapshot.json")
    }

    /// 启动恢复：状态文件 + 日志重放 + 快照。
    pub fn load(&self) -> HaPersisted {
        let mut out = HaPersisted::default();
        if let Ok(text) = std::fs::read_to_string(self.state_path()) {
            if let Ok(sf) = serde_json::from_str::<StateFile>(&text) {
                out.current_term = sf.current_term;
                out.voted_for = sf.voted_for;
                out.applied_index = sf.applied_index;
                out.voters = sf.voters;
                out.learners = sf.learners;
            }
        }
        if let Ok(text) = std::fs::read_to_string(self.log_path()) {
            for line in text.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                match serde_json::from_str::<LogEntry>(line) {
                    Ok(e) => out.log.push(e),
                    // 半行/坏行：截断语义（宁可少日志，不用坏日志）
                    Err(_) => break,
                }
            }
        }
        if let Ok(text) = std::fs::read_to_string(self.snapshot_path()) {
            if let Ok(sf) = serde_json::from_str::<SnapshotFile>(&text) {
                out.snapshot = Some((sf.last_included_index, sf.last_included_term, sf.data));
            }
        }
        out
    }

    /// 持久化 term / 投票 / 已应用位点（原子写：temp + rename）。
    pub fn save_state(
        &self,
        current_term: u64,
        voted_for: Option<&str>,
        applied_index: u64,
        voters: &[String],
        learners: &[String],
    ) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.dir)?;
        let sf = StateFile {
            current_term,
            voted_for: voted_for.map(str::to_string),
            applied_index,
            voters: voters.to_vec(),
            learners: learners.to_vec(),
        };
        let text = serde_json::to_string(&sf).map_err(std::io::Error::other)?;
        let tmp = self.state_path().with_extension("json.tmp");
        std::fs::write(&tmp, text.as_bytes())?;
        if self.fsync {
            if let Ok(f) = std::fs::File::open(&tmp) {
                let _ = f.sync_all();
            }
        }
        std::fs::rename(&tmp, self.state_path())
    }

    /// 整文件重写日志（压缩 / 冲突截断后）。原子写，避免半份日志。
    pub fn rewrite_log(&self, entries: &[LogEntry]) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.dir)?;
        let mut buf = String::new();
        for e in entries {
            let line = serde_json::to_string(e).map_err(std::io::Error::other)?;
            buf.push_str(&line);
            buf.push('\n');
        }
        let tmp = self.log_path().with_extension("jsonl.tmp");
        std::fs::write(&tmp, buf.as_bytes())?;
        if self.fsync {
            if let Ok(f) = std::fs::File::open(&tmp) {
                let _ = f.sync_all();
            }
        }
        std::fs::rename(&tmp, self.log_path())
    }

    /// 追加日志条目（一行一条 JSON）。
    pub fn append_log(&self, entries: &[LogEntry]) -> std::io::Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        std::fs::create_dir_all(&self.dir)?;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.log_path())?;
        let mut buf = String::new();
        for e in entries {
            let line = serde_json::to_string(e).map_err(std::io::Error::other)?;
            buf.push_str(&line);
            buf.push('\n');
        }
        std::io::Write::write_all(&mut f, buf.as_bytes())?;
        if self.fsync {
            let _ = f.sync_all();
        }
        Ok(())
    }

    /// 保存快照（原子写）。
    pub fn save_snapshot(
        &self,
        last_included_index: u64,
        last_included_term: u64,
        data: &SnapshotData,
    ) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.dir)?;
        let sf = SnapshotFile {
            last_included_index,
            last_included_term,
            data: data.clone(),
        };
        let text = serde_json::to_string(&sf).map_err(std::io::Error::other)?;
        let tmp = self.snapshot_path().with_extension("json.tmp");
        std::fs::write(&tmp, text.as_bytes())?;
        if self.fsync {
            if let Ok(f) = std::fs::File::open(&tmp) {
                let _ = f.sync_all();
            }
        }
        std::fs::rename(&tmp, self.snapshot_path())
    }
}

/// 对外可观测的 HA 状态。
#[derive(Debug, Clone, Serialize)]
pub struct HaStatus {
    pub enabled: bool,
    pub role: String,
    pub term: u64,
    pub commit_index: u64,
    pub applied_index: u64,
    pub leader_id: Option<String>,
    pub cluster_size: usize,
    pub quorum: usize,
    pub has_quorum: bool,
    pub log_len: usize,
    pub state_keys: usize,
    /// 日志已压缩到的位点（= 快照点）。
    pub snapshot_index: u64,
    /// 已提交的投票成员（稳定配置）。
    pub voters: Vec<String>,
    /// 已提交的 learners。
    pub learners: Vec<String>,
    /// joint 阶段的目标配置（= 生效中的新配置）。
    pub pending_voters: Vec<String>,
    /// joint 阶段仍在生效的旧配置；`null` = 非 joint。
    pub joint: Option<Vec<String>>,
}

/// M4 业务写入失败分类（API 层据此映射 409/503）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HaError {
    /// 本节点不是 leader（`leader` 为已知的当前 leader）。
    NotLeader { leader: Option<String> },
    /// 已 propose，但在超时内未被多数派提交（多数派不可用/正在切主）。
    Timeout { index: u64 },
    /// 运行时未启用或其它不可用原因。
    Unavailable(String),
}

impl std::fmt::Display for HaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HaError::NotLeader { leader: Some(id) } => write!(f, "not the leader (leader={id})"),
            HaError::NotLeader { leader: None } => write!(f, "not the leader"),
            HaError::Timeout { index } => {
                write!(f, "apply timeout (index={index})")
            }
            HaError::Unavailable(reason) => write!(f, "ha unavailable: {reason}"),
        }
    }
}

impl std::error::Error for HaError {}

/// HA 运行时：内核 + 落盘。
pub struct HaRuntime {
    node: RaftNode,
    store: HaStore,
    persisted_term: u64,
    persisted_applied: u64,
    /// 已落盘的最后一条日志索引（压缩后重写并前移）。
    persisted_last_index: u64,
    /// 最近一次落盘的快照位点。
    persisted_snapshot: u64,
    /// 最近一次落盘的成员配置。
    persisted_membership: (Vec<String>, Vec<String>),
}

impl HaRuntime {
    /// 新建运行时并从磁盘恢复。
    pub fn new(
        cfg: &crate::config::HaConfig,
        node_id: &str,
        peers: Vec<String>,
        voters: Vec<String>,
        learners: Vec<String>,
        dir: impl Into<std::path::PathBuf>,
    ) -> anyhow::Result<HaRuntime> {
        let raft_cfg = RaftConfig {
            election_timeout_ms: cfg.election_timeout_ms,
            heartbeat_interval_ms: cfg.heartbeat_interval_ms,
            snapshot_threshold_entries: cfg.snapshot_threshold_entries,
        };
        let mut node =
            RaftNode::with_membership(node_id.to_string(), peers, voters, learners, raft_cfg);
        let store = HaStore::new(dir, cfg.fsync == "always");
        let persisted = store.load();
        // 状态机里的成员配置（动态变更）优先于配置文件。
        let voters = if persisted.voters.is_empty() {
            node.voters().to_vec()
        } else {
            persisted.voters.clone()
        };
        let learners = if persisted.voters.is_empty() {
            node.learners().to_vec()
        } else {
            persisted.learners.clone()
        };
        let (base, snap_applied) = match &persisted.snapshot {
            Some((i, t, data)) => {
                node.load_snapshot_state(data.clone());
                ((*i, *t), *i)
            }
            None => ((0, 0), 0),
        };
        node.restore(PersistentState {
            current_term: persisted.current_term,
            voted_for: persisted.voted_for.clone(),
            log: persisted.log.clone(),
            base,
            applied_index: persisted.applied_index.max(snap_applied),
            voters,
            learners,
        });
        let persisted_log_len = node.last_log_index();
        let persisted_term = node.term();
        let persisted_applied = node.last_applied();
        let persisted_membership = (
            node.committed_voters().to_vec(),
            node.committed_learners().to_vec(),
        );
        Ok(HaRuntime {
            node,
            store,
            persisted_term,
            persisted_applied,
            persisted_last_index: persisted_log_len,
            persisted_snapshot: base.0,
            persisted_membership,
        })
    }

    /// 把未落盘的状态补齐（term / 投票 / 已应用位点 / 新增日志条目）。
    fn persist(&mut self) {
        let term = self.node.term();
        let applied = self.node.last_applied();
        let membership = (
            self.node.committed_voters().to_vec(),
            self.node.committed_learners().to_vec(),
        );
        if term != self.persisted_term
            || applied != self.persisted_applied
            || membership != self.persisted_membership
        {
            let t0 = std::time::Instant::now();
            if let Err(e) = self.store.save_state(
                term,
                self.node.voted_for(),
                applied,
                &membership.0,
                &membership.1,
            ) {
                tracing::warn!(error = %e, "ha: failed to persist term/vote/applied");
            } else {
                self.persisted_term = term;
                self.persisted_applied = applied;
                self.persisted_membership = membership;
            }
            let elapsed = t0.elapsed();
            if elapsed > std::time::Duration::from_millis(300) {
                tracing::warn!(
                    elapsed_ms = elapsed.as_millis() as u64,
                    "ha: save_state 落盘慢（可能拖慢心跳）"
                );
            }
        }
        let last = self.node.last_log_index();
        if self.node.log_rewrite_needed() {
            // 压缩 / 冲突截断：整文件重写（前缀已压缩掉，不能靠增量 append）。
            let entries = self.node.log_entries_after(self.node.snapshot_index());
            let t0 = std::time::Instant::now();
            match self.store.rewrite_log(&entries) {
                Ok(()) => {
                    self.node.clear_log_rewrite();
                    self.persisted_last_index = last;
                }
                Err(e) => tracing::warn!(error = %e, "ha: failed to rewrite log"),
            }
            let elapsed = t0.elapsed();
            if elapsed > std::time::Duration::from_millis(300) {
                tracing::warn!(
                    elapsed_ms = elapsed.as_millis() as u64,
                    "ha: rewrite_log 落盘慢（可能拖慢心跳）"
                );
            }
        } else if last > self.persisted_last_index {
            let new_entries = self.node.log_entries_after(self.persisted_last_index);
            let t0 = std::time::Instant::now();
            if let Err(e) = self.store.append_log(&new_entries) {
                tracing::warn!(error = %e, "ha: failed to persist log entries");
            } else {
                self.persisted_last_index = last;
            }
            let elapsed = t0.elapsed();
            if elapsed > std::time::Duration::from_millis(300) {
                tracing::warn!(
                    elapsed_ms = elapsed.as_millis() as u64,
                    "ha: append_log 落盘慢（可能拖慢心跳）"
                );
            }
        }
    }

    /// 每次推进后调用：先落盘快照，再压缩日志，最后补持久化。
    fn maintain(&mut self) {
        if let Some((idx, term, data)) = self.node.pending_snapshot() {
            match self.store.save_snapshot(idx, term, &data) {
                Ok(()) => {
                    self.persisted_snapshot = idx;
                    self.node.compact_now();
                    tracing::info!(
                        index = idx,
                        term,
                        retain = self.node.log_len(),
                        "ha: snapshot saved, log prefix compacted"
                    );
                }
                // 落盘失败：先不压缩（下个 tick 重试），避免快照与日志不一致。
                Err(e) => tracing::warn!(error = %e, "ha: failed to persist snapshot"),
            }
        }
        self.persist();
    }

    /// 达到阈值时导出快照并压缩日志（调用方无需再做别的）。
    pub fn maybe_snapshot(&mut self) -> Option<(u64, u64, SnapshotData)> {
        let snap = self.node.pending_snapshot()?;
        if let Err(e) = self.store.save_snapshot(snap.0, snap.1, &snap.2) {
            tracing::warn!(error = %e, "ha: failed to persist snapshot");
            return None;
        }
        self.persisted_snapshot = snap.0;
        self.node.compact_now();
        self.persist();
        Some(snap)
    }
}

/// 在 tokio 多线程 runtime 的 worker 上执行阻塞等待。`block_in_place` 会把
/// worker core 移交给阻塞池线程继续驱动，避免一次 apply 等待把 worker 的
/// 定时器/I-O driver 一起占住。阻塞池/非 runtime 线程直接执行。
fn block_wait<R>(f: impl FnOnce() -> R) -> R {
    match tokio::runtime::Handle::try_current() {
        Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

/// 共享句柄：可在多个任务里用（tokio 任务 + 连接处理）。
#[derive(Clone)]
pub struct HaHandle {
    inner: std::sync::Arc<std::sync::Mutex<HaRuntime>>,
    /// M4：已应用到状态机的位点镜像 + 等待通知。锁序恒为 inner → applied
    /// （发布路径先拿 inner 再拿 applied；等待者只拿 applied，不回锁 inner，
    /// 无环）。
    applied: std::sync::Arc<(StdMutex<u64>, Condvar)>,
    started_at: std::time::Instant,
}

impl HaHandle {
    pub fn new(
        cfg: &crate::config::HaConfig,
        node_id: &str,
        peers: Vec<String>,
        voters: Vec<String>,
        learners: Vec<String>,
        dir: impl Into<std::path::PathBuf>,
    ) -> anyhow::Result<HaHandle> {
        let runtime = HaRuntime::new(cfg, node_id, peers, voters, learners, dir)?;
        let applied =
            std::sync::Arc::new((StdMutex::new(runtime.node.last_applied()), Condvar::new()));
        Ok(HaHandle {
            inner: std::sync::Arc::new(std::sync::Mutex::new(runtime)),
            applied,
            started_at: std::time::Instant::now(),
        })
    }

    /// 把当前 applied 位点发布给等待者（调用方持有 inner 锁）。
    fn publish_applied(&self, g: &HaRuntime) {
        let applied = g.node.last_applied();
        let (lock, cv) = &*self.applied;
        let mut cur = lock.lock().unwrap();
        if applied > *cur {
            *cur = applied;
            cv.notify_all();
        }
    }

    /// 当前已应用位点（本地视图）。
    pub fn applied_index(&self) -> u64 {
        let g = self.inner.lock().unwrap();
        g.node.last_applied()
    }

    /// 等待状态机应用到 `index`（本地已 apply）。超时返回 [`HaError::Timeout`]。
    pub fn wait_applied(&self, index: u64, timeout: Duration) -> Result<(), HaError> {
        block_wait(|| self.wait_applied_blocking(index, timeout))
    }

    fn wait_applied_blocking(&self, index: u64, timeout: Duration) -> Result<(), HaError> {
        let deadline = Instant::now() + timeout;
        let (lock, cv) = &*self.applied;
        let mut cur = lock.lock().unwrap();
        loop {
            if *cur >= index {
                return Ok(());
            }
            let now = Instant::now();
            if now >= deadline {
                drop(cur);
                return Err(self.timeout_error(index));
            }
            let (guard, _) = cv.wait_timeout(cur, deadline - now).unwrap();
            cur = guard;
            if Instant::now() >= deadline {
                // 先放 applied 再取 inner：锁序恒为 inner → applied，反向持有
                // 会与 publish_applied（inner → applied）形成死锁。
                drop(cur);
                return Err(self.timeout_error_logged(index));
            }
        }
    }

    /// leader 就绪屏障：等待本地状态机追上**已提交**日志。新 leader 在任期
    /// 切换瞬间可能还没 apply 前任已提交的条目；读改写（配额窗口、审计链头、
    /// 任务表冲突判定）必须先越过这个窗口，否则会拿旧视图覆盖已提交状态。
    pub fn await_caught_up(&self, timeout: Duration) -> Result<(), HaError> {
        let deadline = Instant::now() + timeout;
        loop {
            let (commit, applied) = {
                let g = self.inner.lock().unwrap();
                (g.node.commit_index(), g.node.last_applied())
            };
            if applied >= commit {
                return Ok(());
            }
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(self.timeout_error(commit));
            }
            let _ = self.wait_applied(commit, left);
        }
    }

    fn timeout_error(&self, index: u64) -> HaError {
        let g = self.inner.lock().unwrap();
        if g.node.role() != Role::Leader {
            HaError::NotLeader {
                leader: g.node.leader_id().map(str::to_string),
            }
        } else {
            HaError::Timeout { index }
        }
    }

    /// 超时诊断：一次取锁同时判定错误类型并记录位点/角色。
    fn timeout_error_logged(&self, index: u64) -> HaError {
        let (commit, applied, role, is_leader, leader) = {
            let g = self.inner.lock().unwrap();
            (
                g.node.commit_index(),
                g.node.last_applied(),
                format!("{:?}", g.node.role()),
                g.node.role() == Role::Leader,
                g.node.leader_id().map(str::to_string),
            )
        };
        let err = if !is_leader {
            HaError::NotLeader {
                leader: leader.clone(),
            }
        } else {
            HaError::Timeout { index }
        };
        tracing::warn!(
            index,
            commit,
            applied,
            role,
            leader = ?leader,
            "ha: wait_applied 超时（诊断：提交位点/角色）"
        );
        err
    }

    /// propose 并等待 apply（leader 专用；业务写入的线性一致入口）。
    pub fn propose_and_wait(&self, cmd: Command, timeout: Duration) -> Result<u64, HaError> {
        let index = {
            let mut g = self.inner.lock().unwrap();
            let Some(idx) = g.node.propose(cmd) else {
                return Err(HaError::NotLeader {
                    leader: g.node.leader_id().map(str::to_string),
                });
            };
            g.maintain();
            self.publish_applied(&g);
            idx
        };
        self.wait_applied(index, timeout)?;
        Ok(index)
    }

    /// leader 原子「读旧值 + 删除」：同一把 runtime 锁内完成读与 propose，
    /// 返回删除前的值。用于 OIDC 一次性 state 的跨节点语义。
    pub fn take(&self, ns: &str, key: &str, timeout: Duration) -> Result<Option<Vec<u8>>, HaError> {
        let (old, index) = {
            let mut g = self.inner.lock().unwrap();
            if g.node.role() != Role::Leader {
                return Err(HaError::NotLeader {
                    leader: g.node.leader_id().map(str::to_string),
                });
            }
            let old = g.node.state().get(ns, key).cloned();
            let Some(idx) = g.node.propose(Command::Delete {
                ns: ns.to_string(),
                key: key.to_string(),
            }) else {
                return Err(HaError::NotLeader {
                    leader: g.node.leader_id().map(str::to_string),
                });
            };
            g.maintain();
            (old, idx)
        };
        self.wait_applied(index, timeout)?;
        Ok(old)
    }

    /// 按命名空间扫描本地状态机，并返回扫描时刻的 applied 位点。
    pub fn scan_at(&self, ns: &str) -> (u64, Vec<(String, Vec<u8>)>) {
        let g = self.inner.lock().unwrap();
        (g.node.last_applied(), g.node.state().scan(ns))
    }

    /// 按命名空间扫描本地状态机。
    pub fn scan(&self, ns: &str) -> Vec<(String, Vec<u8>)> {
        self.inner.lock().unwrap().node.state().scan(ns)
    }

    /// 当前 leader（未知返回 None）。
    pub fn leader_id(&self) -> Option<String> {
        self.inner
            .lock()
            .unwrap()
            .node
            .leader_id()
            .map(str::to_string)
    }

    /// 单调时钟（毫秒）——内核不读墙钟，测试可确定复现。
    pub fn now_ms(&self) -> u64 {
        self.started_at.elapsed().as_millis() as u64
    }

    /// 推进内核（定时器驱动），返回要发出的消息。
    pub fn tick(&self, now_ms: u64) -> Vec<(String, RaftMessage)> {
        let t0 = std::time::Instant::now();
        let mut g = self.inner.lock().unwrap();
        let wait = t0.elapsed();
        let out = g.node.tick(now_ms);
        g.maintain();
        self.publish_applied(&g);
        drop(g);
        let total = t0.elapsed();
        if total > std::time::Duration::from_millis(200) {
            tracing::warn!(
                wait_ms = wait.as_millis() as u64,
                total_ms = total.as_millis() as u64,
                "ha: tick 慢（等锁或内核推进耗时）"
            );
        }
        out
    }

    /// 处理一条对端消息，返回要发出的消息。
    pub fn on_message(
        &self,
        from: &str,
        msg: RaftMessage,
        now_ms: u64,
    ) -> Vec<(String, RaftMessage)> {
        let t0 = std::time::Instant::now();
        let mut g = self.inner.lock().unwrap();
        let wait = t0.elapsed();
        let out = g.node.handle(from, msg, now_ms);
        g.maintain();
        self.publish_applied(&g);
        drop(g);
        let total = t0.elapsed();
        if total > std::time::Duration::from_millis(200) {
            tracing::warn!(
                wait_ms = wait.as_millis() as u64,
                total_ms = total.as_millis() as u64,
                "ha: on_message 慢（等锁或内核推进耗时）"
            );
        }
        out
    }

    /// leader 提交一条命令；非 leader 返回 `None`。
    pub fn propose(&self, cmd: Command) -> Option<u64> {
        let mut g = self.inner.lock().unwrap();
        let idx = g.node.propose(cmd);
        g.maintain();
        self.publish_applied(&g);
        idx
    }

    /// leader 发起成员变更（joint consensus）。返回新条目 index。
    pub fn propose_membership(
        &self,
        voters: Vec<String>,
        learners: Vec<String>,
    ) -> Result<u64, String> {
        let mut g = self.inner.lock().unwrap();
        let idx = g.node.propose_membership(voters, learners)?;
        g.maintain();
        self.publish_applied(&g);
        Ok(idx)
    }

    /// 发起线性一致读；非 leader 返回 `None`。
    pub fn begin_read(&self, now_ms: u64) -> Option<(u64, u64)> {
        let mut g = self.inner.lock().unwrap();
        g.node.begin_read(now_ms)
    }

    /// 立刻广播一轮心跳（ReadIndex 确认），返回要发出的消息。
    pub fn read_heartbeat(&self, now_ms: u64) -> Vec<(String, RaftMessage)> {
        let mut g = self.inner.lock().unwrap();
        let out = g.node.broadcast_heartbeat(now_ms);
        g.maintain();
        out
    }

    /// 读是否已可安全返回；`Some(read_index)` 表示可以读状态机。
    pub fn read_ready(&self, seq: u64) -> Option<u64> {
        let g = self.inner.lock().unwrap();
        g.node.read_ready(seq)
    }

    /// 释放读（读完 / 超时）。
    pub fn finish_read(&self, seq: u64) {
        let mut g = self.inner.lock().unwrap();
        g.node.finish_read(seq);
    }

    /// 已提交的成员配置（voters, learners）。
    pub fn membership(&self) -> (Vec<String>, Vec<String>) {
        let g = self.inner.lock().unwrap();
        (
            g.node.committed_voters().to_vec(),
            g.node.committed_learners().to_vec(),
        )
    }

    /// 读状态机（本地视图；leader 强一致，follower 可能滞后）。
    pub fn get(&self, ns: &str, key: &str) -> Option<Vec<u8>> {
        let g = self.inner.lock().unwrap();
        g.node.state().get(ns, key).cloned()
    }

    /// 状态快照。
    pub fn status(&self) -> HaStatus {
        let g = self.inner.lock().unwrap();
        HaStatus {
            enabled: true,
            role: g.node.role().as_str().to_string(),
            term: g.node.term(),
            commit_index: g.node.commit_index(),
            applied_index: g.node.last_applied(),
            leader_id: g.node.leader_id().map(str::to_string),
            cluster_size: g.node.cluster_size(),
            quorum: g.node.quorum(),
            has_quorum: g.node.has_quorum(),
            log_len: g.node.log_len(),
            state_keys: g.node.state().len(),
            snapshot_index: g.node.snapshot_index(),
            voters: g.node.committed_voters().to_vec(),
            learners: g.node.committed_learners().to_vec(),
            pending_voters: g.node.voters().to_vec(),
            joint: g.node.joint().map(|v| v.to_vec()),
        }
    }

    /// 当前是否已是 leader。
    pub fn is_leader(&self) -> bool {
        self.inner.lock().unwrap().node.role() == Role::Leader
    }

    /// 需要发给落后节点的快照（leader 侧）。
    pub fn maybe_snapshot(&self) -> Option<(u64, u64, SnapshotData)> {
        let mut g = self.inner.lock().unwrap();
        g.maybe_snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `block_wait` 必须把 worker core（含定时器驱动）让出去：否则一次
    /// 3s 的 apply 等待会冻住整个进程的定时器（心跳断流 → 误选主）。
    #[test]
    fn block_wait_yields_worker_driver() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let ticker = tokio::spawn(async {
                let mut t = tokio::time::interval(Duration::from_millis(50));
                let mut last = Instant::now();
                let mut max_gap = 0u128;
                for _ in 0..30 {
                    t.tick().await;
                    max_gap = max_gap.max(last.elapsed().as_millis());
                    last = Instant::now();
                }
                max_gap
            });
            tokio::time::sleep(Duration::from_millis(100)).await;
            let waiter = tokio::spawn(async {
                block_wait(|| std::thread::sleep(Duration::from_millis(1200)));
            });
            waiter.await.unwrap();
            let max_gap = ticker.await.unwrap();
            assert!(max_gap < 400, "ticker 被冻结 {max_gap}ms");
        });
    }

    /// 内存网络：把消息投递到目标节点，返回新产生的消息。
    struct Net {
        nodes: HashMap<String, RaftNode>,
        queue: Vec<(String, String, RaftMessage)>,
        now: u64,
        dropped: HashSet<String>,
    }

    impl Net {
        fn new(ids: &[&str]) -> Self {
            Self::with_cfg(ids, RaftConfig::default())
        }

        /// 全员用同一份显式成员配置（可含 learners）。
        fn with_roles(ids: &[&str], voters: &[&str], learners: &[&str], cfg: RaftConfig) -> Self {
            let voters: Vec<String> = voters.iter().map(|s| s.to_string()).collect();
            let learners: Vec<String> = learners.iter().map(|s| s.to_string()).collect();
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
                    RaftNode::with_membership(
                        id.to_string(),
                        peers,
                        voters.clone(),
                        learners.clone(),
                        cfg.clone(),
                    ),
                );
            }
            Net {
                nodes,
                queue: Vec::new(),
                now: 0,
                dropped: HashSet::new(),
            }
        }

        fn with_cfg(ids: &[&str], cfg: RaftConfig) -> Self {
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
                    RaftNode::new(id.to_string(), peers, cfg.clone()),
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
        // 心跳 200ms 一拍：给足时间让 leader 把条目发出去并收到 ack
        for _ in 0..8 {
            net.tick(50);
        }
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
        for _ in 0..8 {
            net.tick(50);
        }
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
        for _ in 0..8 {
            net.tick(50);
        }
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
            read_seq: 0,
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
        for _ in 0..8 {
            net.tick(50);
        }
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
    fn compaction_trims_prefix_and_lagging_node_gets_snapshot() {
        let cfg = RaftConfig {
            snapshot_threshold_entries: 3,
            ..RaftConfig::default()
        };
        let mut net = Net::with_cfg(&["n1", "n2", "n3"], cfg);
        let leader = elect(&mut net, 200).expect("选主");
        let lagging = ["n1", "n2", "n3"]
            .iter()
            .find(|id| **id != leader)
            .unwrap()
            .to_string();
        net.dropped.insert(lagging.clone());

        for i in 0..10 {
            let cmd = put("kv", &format!("k{i}"), &format!("v{i}"));
            net.nodes.get_mut(&leader).unwrap().propose(cmd);
        }
        // 两节点仍有多数派：提交并 apply，然后压缩 leader 日志前缀。
        for _ in 0..12 {
            net.tick(50);
        }
        let (idx, term, _) = net
            .nodes
            .get_mut(&leader)
            .unwrap()
            .maybe_compact()
            .expect("达到阈值应触发压缩");
        assert!(idx > 0 && term > 0);
        let leader_node = &net.nodes[&leader];
        assert_eq!(leader_node.snapshot_index(), idx, "压缩基点应前移");
        assert_eq!(leader_node.log_len(), 0, "全部已应用条目应被压缩");

        // 落后节点恢复：leader 的 next_index 已落在压缩基之前 -> 改发 InstallSnapshot。
        net.dropped.remove(&lagging);
        for _ in 0..20 {
            net.tick(50);
        }
        let caught = &net.nodes[&lagging];
        assert_eq!(
            caught.state().get("kv", "k9").map(|v| v.as_slice()),
            Some(b"v9".as_slice()),
            "落后节点应通过快照追平状态机"
        );
        assert!(
            caught.snapshot_index() >= idx,
            "落后节点应装上快照（base={}, idx={idx}）",
            caught.snapshot_index()
        );

        // 迟到的旧快照不得回退状态机。
        let stale = RaftMessage::InstallSnapshot {
            term: net.nodes[&leader].term(),
            leader_id: leader.clone(),
            last_included_index: idx / 2,
            last_included_term: 1,
            data: vec![("kv/stale".to_string(), b"x".to_vec())],
        };
        net.nodes
            .get_mut(&lagging)
            .unwrap()
            .handle(&leader, stale, net.now);
        assert_eq!(
            net.nodes[&lagging]
                .state()
                .get("kv", "k9")
                .map(|v| v.as_slice()),
            Some(b"v9".as_slice()),
            "旧快照不应覆盖更新的状态机"
        );
    }

    #[test]
    fn restart_from_snapshot_restores_state_and_continues_log() {
        let cfg = RaftConfig {
            snapshot_threshold_entries: 2,
            ..RaftConfig::default()
        };
        let mut net = Net::with_cfg(&["n1", "n2", "n3"], cfg.clone());
        let leader = elect(&mut net, 200).expect("选主");
        for i in 0..5 {
            let cmd = put("kv", &format!("r{i}"), &format!("w{i}"));
            net.nodes.get_mut(&leader).unwrap().propose(cmd);
        }
        for _ in 0..10 {
            net.tick(50);
        }
        let node = net.nodes.get_mut(&leader).unwrap();
        let (idx, _term, data) = node.maybe_compact().expect("压缩");
        // 压缩后再写几条：重启时需要「快照 + 后缀日志重放」两者都用。
        for i in 5..8 {
            let cmd = put("kv", &format!("r{i}"), &format!("w{i}"));
            net.nodes.get_mut(&leader).unwrap().propose(cmd);
        }
        for _ in 0..8 {
            net.tick(50);
        }
        let node = net.nodes.get_mut(&leader).unwrap();
        let saved = node.persistent();
        let applied_before = saved.applied_index;
        assert_eq!(saved.base.0, idx);
        assert!(saved.base.1 > 0, "压缩基点应带 term");
        assert_eq!(saved.applied_index, node.last_applied());
        assert!(saved.applied_index > idx, "后缀日志应多于压缩基点");

        let peers: Vec<String> = ["n1", "n2", "n3"]
            .iter()
            .filter(|id| **id != leader)
            .map(|id| id.to_string())
            .collect();
        let mut restarted = RaftNode::new(leader.clone(), peers, cfg);
        restarted.load_snapshot_state(data);
        restarted.restore(saved);
        assert_eq!(restarted.last_applied(), applied_before);
        assert_eq!(restarted.commit_index(), applied_before);
        assert_eq!(restarted.snapshot_index(), idx);
        assert_eq!(
            restarted.state().get("kv", "r4").map(|v| v.as_slice()),
            Some(b"w4".as_slice()),
            "重启后状态机应从快照恢复"
        );
        assert_eq!(
            restarted.state().get("kv", "r7").map(|v| v.as_slice()),
            Some(b"w7".as_slice()),
            "快照之后的后缀日志应被重放"
        );
        // 新条目追加后仍保持偏移式索引不变式。
        assert_eq!(restarted.last_log_index(), applied_before);
        assert_eq!(
            restarted.log_len(),
            (applied_before - idx) as usize,
            "后缀条目数 = applied - base"
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
        for _ in 0..8 {
            net.tick(50);
        }
        let saved = net.nodes[&leader].persistent();
        let mut restarted = RaftNode::new(
            leader.clone(),
            vec!["n2".into(), "n3".into()],
            RaftConfig::default(),
        );
        restarted.restore(saved);
        assert_eq!(restarted.term(), net.nodes[&leader].term());
        assert_eq!(restarted.log_len(), net.nodes[&leader].log_len());
        // 重启后必须从日志重放状态机（否则「全员重启 = 数据清空」）。
        assert_eq!(
            restarted.state().get("task", "t").map(|v| v.as_slice()),
            Some(b"v".as_slice()),
            "重启后应从日志重放出已应用状态"
        );
        assert_eq!(restarted.last_applied(), restarted.commit_index());
    }

    #[test]
    fn joint_consensus_removes_a_member() {
        let mut net = Net::new(&["n1", "n2", "n3"]);
        let leader = elect(&mut net, 200).expect("选主");
        let target = vec!["n1".to_string(), "n2".to_string()];
        let idx = net
            .nodes
            .get_mut(&leader)
            .unwrap()
            .propose_membership(target.clone(), vec![])
            .expect("leader 应能发起成员变更");
        assert!(idx > 0);
        // 追加后立刻进入 joint：旧配置仍然要多数派。
        assert_eq!(
            net.nodes[&leader].joint(),
            Some(["n1".to_string(), "n2".to_string(), "n3".to_string()].as_slice())
        );
        for _ in 0..30 {
            net.tick(50);
        }
        for id in ["n1", "n2", "n3"] {
            assert_eq!(
                net.nodes[id].committed_voters(),
                target.as_slice(),
                "{id} 未切到新配置"
            );
            assert!(net.nodes[id].joint().is_none(), "{id} joint 未结束");
            assert_eq!(net.nodes[id].cluster_size(), 2);
            assert_eq!(net.nodes[id].quorum(), 2);
        }
        // 被移出的 n3 不再收到新写入。
        net.nodes
            .get_mut(&leader)
            .unwrap()
            .propose(put("kv", "after", "1"));
        for _ in 0..12 {
            net.tick(50);
        }
        assert!(
            net.nodes["n3"].state().get("kv", "after").is_none(),
            "已移出成员不应再复制新日志"
        );
        // 剩余两名投票成员都拿到。
        for id in ["n1", "n2"] {
            assert_eq!(
                net.nodes[id]
                    .state()
                    .get("kv", "after")
                    .map(|v| v.as_slice()),
                Some(b"1".as_slice())
            );
        }
    }

    #[test]
    fn leader_steps_down_when_removed_from_voters() {
        let mut net = Net::new(&["n1", "n2", "n3"]);
        let leader = elect(&mut net, 200).expect("选主");
        let others: Vec<String> = ["n1", "n2", "n3"]
            .iter()
            .filter(|id| **id != leader)
            .map(|id| id.to_string())
            .collect();
        net.nodes
            .get_mut(&leader)
            .unwrap()
            .propose_membership(others.clone(), vec![])
            .expect("发起成员变更");
        for _ in 0..40 {
            net.tick(50);
        }
        assert_ne!(
            net.nodes[&leader].role(),
            Role::Leader,
            "被移出投票成员的 leader 应立即让位"
        );
        let ls = net.leaders();
        assert_eq!(ls.len(), 1, "剩余成员应选出唯一新 leader：{ls:?}");
        assert!(others.contains(&ls[0]));
        // 新配置仍能提交写入（2 投票成员 = 2 票多数派）。
        net.nodes
            .get_mut(&ls[0])
            .unwrap()
            .propose(put("kv", "k", "v"));
        for _ in 0..12 {
            net.tick(50);
        }
        assert_eq!(
            net.nodes[&ls[0]]
                .state()
                .get("kv", "k")
                .map(|v| v.as_slice()),
            Some(b"v".as_slice()),
            "成员变更后仍应能提交"
        );
    }

    #[test]
    fn learners_replicate_but_never_vote() {
        let mut net = Net::with_roles(
            &["n1", "n2", "n3", "n4"],
            &["n1", "n2", "n3"],
            &["n4"],
            RaftConfig::default(),
        );
        let leader = elect(&mut net, 200).expect("选主");
        net.nodes
            .get_mut(&leader)
            .unwrap()
            .propose(put("kv", "a", "1"));
        for _ in 0..10 {
            net.tick(50);
        }
        assert_eq!(
            net.nodes["n4"].state().get("kv", "a").map(|v| v.as_slice()),
            Some(b"1".as_slice()),
            "learner 应复制并应用日志"
        );
        assert_eq!(net.nodes["n4"].role(), Role::Follower);
        // 只剩 learner：它不参选、不投票。
        for id in ["n1", "n2", "n3"] {
            net.dropped.insert(id.to_string());
        }
        for _ in 0..60 {
            net.tick(50);
        }
        assert_ne!(net.nodes["n4"].role(), Role::Candidate);
        assert_ne!(net.nodes["n4"].role(), Role::Leader);
        let req = RaftMessage::RequestVote {
            term: 99,
            candidate_id: "n9".into(),
            last_log_index: 99,
            last_log_term: 99,
            pre_vote: false,
        };
        let out = net.nodes.get_mut("n4").unwrap().handle("n9", req, net.now);
        assert!(
            matches!(
                out.first().map(|(_, m)| m),
                Some(RaftMessage::RequestVoteResp { granted: false, .. })
            ),
            "learner 不得投票"
        );
    }

    #[test]
    fn membership_survives_restart() {
        let mut net = Net::new(&["n1", "n2", "n3"]);
        let leader = elect(&mut net, 200).expect("选主");
        net.nodes
            .get_mut(&leader)
            .unwrap()
            .propose_membership(vec!["n1".into(), "n2".into()], vec!["n3".into()])
            .expect("发起成员变更");
        for _ in 0..30 {
            net.tick(50);
        }
        let saved = net.nodes[&leader].persistent();
        assert_eq!(
            saved.voters,
            vec!["n1".to_string(), "n2".to_string()],
            "持久化应带已提交配置"
        );
        assert_eq!(saved.learners, vec!["n3".to_string()]);
        // 重启：配置文件仍是 3 投票成员，但状态机里的配置优先。
        let mut restarted = RaftNode::new(
            leader.clone(),
            vec!["n1".into(), "n2".into(), "n3".into()],
            RaftConfig::default(),
        );
        restarted.restore(saved);
        assert_eq!(restarted.voters(), ["n1", "n2"]);
        assert_eq!(restarted.learners(), ["n3"]);
        assert_eq!(restarted.cluster_size(), 2);
    }

    #[test]
    fn new_member_catches_up_and_votes() {
        let mut net = Net::with_roles(
            &["n1", "n2", "n3", "n4"],
            &["n1", "n2", "n3"],
            &[],
            RaftConfig::default(),
        );
        let leader = elect(&mut net, 200).expect("选主");
        net.nodes
            .get_mut(&leader)
            .unwrap()
            .propose(put("kv", "before", "1"));
        for _ in 0..8 {
            net.tick(50);
        }
        // n4 之前不是成员：加进来后必须从日志起点追平。
        net.nodes
            .get_mut(&leader)
            .unwrap()
            .propose_membership(
                vec!["n1".into(), "n2".into(), "n3".into(), "n4".into()],
                vec![],
            )
            .expect("扩容");
        for _ in 0..40 {
            net.tick(50);
        }
        let want = ["n1", "n2", "n3", "n4"];
        for id in want {
            assert_eq!(
                net.nodes[id].committed_voters().len(),
                4,
                "{id} 未切到 4 成员配置"
            );
        }
        assert_eq!(
            net.nodes["n4"]
                .state()
                .get("kv", "before")
                .map(|v| v.as_slice()),
            Some(b"1".as_slice()),
            "新成员应从日志起点追平历史数据"
        );
        // 4 成员多数派 = 3：继续提交并复制到新成员。
        net.nodes
            .get_mut(&leader)
            .unwrap()
            .propose(put("kv", "after", "2"));
        for _ in 0..12 {
            net.tick(50);
        }
        for id in want {
            assert_eq!(
                net.nodes[id]
                    .state()
                    .get("kv", "after")
                    .map(|v| v.as_slice()),
                Some(b"2".as_slice()),
                "{id} 未拿到新增后的写入"
            );
        }
    }

    #[test]
    fn read_index_needs_quorum_confirmation() {
        let mut net = Net::new(&["n1", "n2", "n3"]);
        let leader = elect(&mut net, 200).expect("选主");
        let now = net.now;
        let (seq, read_index) = net
            .nodes
            .get_mut(&leader)
            .unwrap()
            .begin_read(now)
            .expect("leader 应能发起读");
        // 还没拿回心跳 ack：不能读。
        assert!(
            net.nodes[&leader].read_ready(seq).is_none(),
            "未确认 leadership 前不得返回读"
        );
        let msgs = net.nodes.get_mut(&leader).unwrap().broadcast_heartbeat(now);
        assert!(!msgs.is_empty(), "应广播心跳");
        for (to, m) in msgs {
            net.queue.push((leader.clone(), to, m));
        }
        net.drain();
        assert_eq!(
            net.nodes[&leader].read_ready(seq),
            Some(read_index),
            "多数派确认后应可读"
        );
        net.nodes.get_mut(&leader).unwrap().finish_read(seq);
        assert!(net.nodes[&leader].read_ready(seq).is_none());
    }

    #[test]
    fn read_index_with_one_follower_ack_is_enough_for_three_nodes() {
        let mut net = Net::new(&["n1", "n2", "n3"]);
        let leader = elect(&mut net, 200).expect("选主");
        let followers: Vec<String> = ["n1", "n2", "n3"]
            .iter()
            .filter(|id| **id != leader)
            .map(|id| id.to_string())
            .collect();
        // 只剩一个 follower：3 节点多数派 = 2（leader + 它）。
        net.dropped.insert(followers[0].clone());
        let now = net.now;
        let (seq, read_index) = net
            .nodes
            .get_mut(&leader)
            .unwrap()
            .begin_read(now)
            .expect("发起读");
        let msgs = net.nodes.get_mut(&leader).unwrap().broadcast_heartbeat(now);
        for (to, m) in msgs {
            net.queue.push((leader.clone(), to, m));
        }
        net.drain();
        assert_eq!(
            net.nodes[&leader].read_ready(seq),
            Some(read_index),
            "leader + 1 个 follower 已达 3 节点多数派"
        );
    }

    #[test]
    fn read_index_blocked_without_quorum() {
        let mut net = Net::new(&["n1", "n2", "n3"]);
        let leader = elect(&mut net, 200).expect("选主");
        // 隔离另外两个节点：leader 拿不到多数派确认。
        for id in ["n1", "n2", "n3"] {
            if id != leader {
                net.dropped.insert(id.to_string());
            }
        }
        let now = net.now;
        let (seq, _ri) = net
            .nodes
            .get_mut(&leader)
            .unwrap()
            .begin_read(now)
            .expect("发起读");
        let msgs = net.nodes.get_mut(&leader).unwrap().broadcast_heartbeat(now);
        for (to, m) in msgs {
            net.queue.push((leader.clone(), to, m));
        }
        net.drain();
        assert!(
            net.nodes[&leader].read_ready(seq).is_none(),
            "失去多数派时强一致读必须失败而非返回旧值"
        );
    }

    #[test]
    fn read_index_only_on_leader_and_sees_committed_writes() {
        let mut net = Net::new(&["n1", "n2", "n3"]);
        let leader = elect(&mut net, 200).expect("选主");
        let follower = ["n1", "n2", "n3"]
            .iter()
            .find(|id| **id != leader)
            .unwrap()
            .to_string();
        assert!(
            net.nodes
                .get_mut(&follower)
                .unwrap()
                .begin_read(net.now)
                .is_none(),
            "follower 不能发起 ReadIndex"
        );

        let idx = net
            .nodes
            .get_mut(&leader)
            .unwrap()
            .propose(put("kv", "strong", "v"))
            .expect("propose");
        for _ in 0..8 {
            net.tick(50);
        }
        let now = net.now;
        let (seq, read_index) = net
            .nodes
            .get_mut(&leader)
            .unwrap()
            .begin_read(now)
            .expect("发起读");
        assert!(
            read_index >= idx,
            "read_index 应覆盖已提交的写入（{read_index} < {idx}）"
        );
        let msgs = net.nodes.get_mut(&leader).unwrap().broadcast_heartbeat(now);
        for (to, m) in msgs {
            net.queue.push((leader.clone(), to, m));
        }
        net.drain();
        assert_eq!(net.nodes[&leader].read_ready(seq), Some(read_index));
        assert_eq!(
            net.nodes[&leader]
                .state()
                .get("kv", "strong")
                .map(|v| v.as_slice()),
            Some(b"v".as_slice()),
            "强一致读应能看到已提交的写入"
        );
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
