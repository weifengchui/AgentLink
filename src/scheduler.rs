//! 任务调度器（M3 P3-1，bin-packing 调度）。
//!
//! 纯函数调度：给定 pending 任务 + fleet agents，为每个任务选出最佳
//! candidate 节点，返回 (task_id, node_id, to_agent) 供 reconcile loop 派发。
//!
//! 筛选规则（设计 §2.5）：
//!   spec.requires ⊆ agent.capabilities（cap_matches 逐条满足）
//!   agent.online == true
//!   所在节点 tags 不命中 spec.constraints.exclude_tags
//!   节点并发负载 < max_concurrent_per_node
//!
//! 打分（first-fit-decreasing，确定性）：
//!   pending 按 (priority 升序, created_ts 升序) 排序（越小越优先）
//!   逐个装入：剩余并发额度最大、其次负载最小的节点；同分按 node_id 字典序

use serde_json::Value;
use std::collections::HashMap;

use crate::router::AgentRecord;

/// 调度器。
#[derive(Debug, Clone)]
pub struct Scheduler {
    /// 每节点最大并发任务数（EngineConfig.max_concurrent_per_node，默认 2）。
    pub max_concurrent_per_node: usize,
    /// 全局最大 in-flight 任务数（0 = 不限制，协议 §5.5）。
    pub max_inflight: usize,
    /// v0.7 §4.2 ABAC 策略（缺省 enabled=false，零影响）。
    pub abac: crate::abac::AbacConfig,
}

impl Scheduler {
    pub fn new(max_concurrent_per_node: usize, max_inflight: usize) -> Self {
        Self {
            max_concurrent_per_node,
            max_inflight,
            abac: crate::abac::AbacConfig::default(),
        }
    }

    /// 从 agents 中筛选满足 spec 的 candidate（带节点 tag 过滤）。
    /// 任务 namespace（`spec.namespace`，缺省/空 = "default"）。
    fn task_namespace(spec: &Value) -> String {
        crate::router::task_namespace(Some(spec)).to_string()
    }

    /// agent 是否服务该 namespace（"*" = 共享池）。
    fn agent_serves(agent: &AgentRecord, namespace: &str) -> bool {
        let ns = agent
            .namespace
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or("default");
        ns == "*" || ns == namespace
    }

    pub fn candidates<'a>(
        spec: &Value,
        agents: &'a [AgentRecord],
        node_tags: &HashMap<String, Vec<String>>,
    ) -> Vec<&'a AgentRecord> {
        let namespace = Self::task_namespace(spec);
        let requires: Vec<String> = spec
            .get("requires")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let exclude_tags: Vec<String> = spec
            .get("constraints")
            .and_then(|v| v.get("exclude_tags"))
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        agents
            .iter()
            .filter(|a| a.online)
            // v0.7 §4.2 多租户：任务只能落到同 namespace 或共享池 agent。
            .filter(|a| Self::agent_serves(a, &namespace))
            .filter(|a| {
                // requires ⊆ capabilities（cap_matches 逐条满足）
                requires.iter().all(|r| {
                    a.capabilities
                        .iter()
                        .any(|c| crate::router::cap_matches(c, r))
                })
            })
            .filter(|a| {
                // 所在节点 tags 不命中 exclude_tags
                let tags = node_tags.get(&a.node_id).cloned().unwrap_or_default();
                !exclude_tags.iter().any(|et| tags.iter().any(|t| t == et))
            })
            .collect()
    }

    /// v0.7 §4.2：在 requires/exclude_tags 之上再按 ABAC 规则过滤。
    fn candidates_abac<'a>(
        spec: &Value,
        agents: &'a [AgentRecord],
        node_tags: &HashMap<String, Vec<String>>,
        abac: &crate::abac::AbacConfig,
    ) -> Vec<&'a AgentRecord> {
        if !abac.enabled || abac.rules.is_empty() {
            return Self::candidates(spec, agents, node_tags);
        }
        Self::candidates(spec, agents, node_tags)
            .into_iter()
            .filter(|a| {
                let tags = node_tags.get(&a.node_id).cloned().unwrap_or_default();
                crate::abac::evaluate(abac, spec, &tags).allowed
            })
            .collect()
    }

    /// 从 candidates 中选出最佳节点（`select_agent` 的节点级薄包装；
    /// 运行期派发走 select_agent，本函数保留给调度单测的节点级断言）。
    #[allow(dead_code)]
    pub fn select_node(
        candidates: &[&AgentRecord],
        loads: &HashMap<String, usize>,
        max_concurrent_per_node: usize,
    ) -> Option<String> {
        Self::select_agent(candidates, loads, max_concurrent_per_node).map(|a| a.node_id.clone())
    }

    /// v0.6 验收修复：返回选中 agent（含 node_id 与 agent_id）。
    pub fn select_agent<'a>(
        candidates: &[&'a AgentRecord],
        loads: &HashMap<String, usize>,
        max_concurrent_per_node: usize,
    ) -> Option<&'a AgentRecord> {
        candidates
            .iter()
            .filter(|a| loads.get(&a.node_id).copied().unwrap_or(0) < max_concurrent_per_node)
            .min_by_key(|a| {
                let load = loads.get(&a.node_id).copied().unwrap_or(0);
                let remaining = max_concurrent_per_node.saturating_sub(load);
                // 剩余额度越大越优先（Reverse 负载最小），同分按
                // (node_id, agent_id) 字典序（确定性）。
                (
                    std::cmp::Reverse(remaining),
                    a.node_id.clone(),
                    a.agent_id.clone(),
                )
            })
            .copied()
    }

    /// 为一批 pending 任务做 bin-packing 调度。
    ///
    /// 返回 (task_id, node_id, to_agent) 列表，按优先级顺序。
    /// `loads` 会被原地更新（被选中的节点负载 +1）。
    pub fn schedule_batch(
        &self,
        tasks: &[(String, Value)],
        agents: &[AgentRecord],
        node_tags: &HashMap<String, Vec<String>>,
        loads: &mut HashMap<String, usize>,
        already_inflight: usize,
    ) -> Vec<(String, String, String)> {
        let mut out = Vec::new();

        // 全局 in-flight 上限（0 = 不限制）
        let remaining_global = if self.max_inflight == 0 {
            usize::MAX
        } else {
            self.max_inflight.saturating_sub(already_inflight)
        };
        if remaining_global == 0 {
            return out;
        }

        // 按 (priority 升序, created_ts 升序) 排序（确定性，first-fit-decreasing）
        let mut ordered: Vec<(String, Value)> = tasks.to_vec();
        ordered.sort_by(|a, b| {
            let pa = a.1.get("priority").and_then(|v| v.as_u64()).unwrap_or(100);
            let pb = b.1.get("priority").and_then(|v| v.as_u64()).unwrap_or(100);
            pa.cmp(&pb).then_with(|| {
                let ta = a.1.get("created_ts").and_then(|v| v.as_str()).unwrap_or("");
                let tb = b.1.get("created_ts").and_then(|v| v.as_str()).unwrap_or("");
                ta.cmp(tb)
            })
        });

        let mut dispatched = 0usize;
        for (task_id, spec) in ordered.into_iter() {
            if dispatched >= remaining_global {
                break;
            }

            let explicit = spec
                .get("to_agent")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            let cands = Self::candidates_abac(&spec, agents, node_tags, &self.abac);
            if let Some(chosen) = Self::select_agent(&cands, loads, self.max_concurrent_per_node) {
                loads.entry(chosen.node_id.clone()).or_insert(0);
                *loads.get_mut(&chosen.node_id).unwrap() += 1;
                // v0.6 验收修复：显式 to_agent 优先（原样透传），否则落到
                // 选中的具体 agent——派发必须有 agent 身份才能定向投帧。
                let to_agent = if explicit.is_empty() {
                    chosen.agent_id.clone()
                } else {
                    explicit
                };
                out.push((task_id, chosen.node_id.clone(), to_agent));
                dispatched += 1;
            }
            // 无可用 candidate：任务保持 pending，下一拍重试
        }

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn agent(node_id: &str, agent_id: &str, caps: &[&str], online: bool) -> AgentRecord {
        AgentRecord {
            agent_id: agent_id.to_string(),
            name: None,
            capabilities: caps.iter().map(|s| s.to_string()).collect(),
            online,
            node_id: node_id.to_string(),
            last_seen: 0,
            namespace: None,
        }
    }

    fn spec(requires: &[&str], priority: u64, ts: &str) -> Value {
        let mut s = json!({
            "topic": "task_spec",
            "task_id": "t",
            "type": "deploy",
            "priority": priority,
            "created_ts": ts,
        });
        if !requires.is_empty() {
            s.as_object_mut()
                .unwrap()
                .insert("requires".to_string(), json!(requires));
        }
        s
    }

    #[test]
    fn requires_excludes_node() {
        let sched = Scheduler::new(2, 0);
        let agents = vec![
            agent("n1", "a1", &["cap:file"], true),
            agent("n2", "a2", &["cap:shell:safe"], true),
        ];
        let node_tags: HashMap<String, Vec<String>> = HashMap::new();
        let spec = spec(&["cap:node:admin"], 100, "2026-01-01T00:00:00Z");
        let cands = Scheduler::candidates(&spec, &agents, &node_tags);
        assert!(cands.is_empty(), "没有 agent 满足 cap:node:admin");
    }

    #[test]
    fn capability_match_wildcard() {
        let sched = Scheduler::new(2, 0);
        let agents = vec![agent("n1", "a1", &["cap:node:*"], true)];
        let node_tags: HashMap<String, Vec<String>> = HashMap::new();
        let spec = spec(&["cap:node:reboot"], 100, "2026-01-01T00:00:00Z");
        let cands = Scheduler::candidates(&spec, &agents, &node_tags);
        assert_eq!(cands.len(), 1, "cap:node:* 应匹配 cap:node:reboot");
    }

    #[test]
    fn offline_agent_excluded() {
        let sched = Scheduler::new(2, 0);
        let agents = vec![agent("n1", "a1", &["cap:file"], false)];
        let node_tags: HashMap<String, Vec<String>> = HashMap::new();
        let spec = spec(&["cap:file"], 100, "2026-01-01T00:00:00Z");
        let cands = Scheduler::candidates(&spec, &agents, &node_tags);
        assert!(cands.is_empty(), "离线 agent 应被排除");
    }

    #[test]
    fn exclude_tags_filter() {
        let sched = Scheduler::new(2, 0);
        let agents = vec![agent("n1", "a1", &["cap:file"], true)];
        let mut node_tags: HashMap<String, Vec<String>> = HashMap::new();
        node_tags.insert("n1".to_string(), vec!["gpu".to_string()]);
        let spec = json!({
            "topic": "task_spec",
            "task_id": "t",
            "type": "deploy",
            "priority": 100,
            "created_ts": "2026-01-01T00:00:00Z",
            "constraints": { "exclude_tags": ["gpu"] },
        });
        let cands = Scheduler::candidates(&spec, &agents, &node_tags);
        assert!(cands.is_empty(), "节点命中 exclude_tags 应被排除");
    }

    #[test]
    fn select_node_least_loaded() {
        let sched = Scheduler::new(2, 0);
        let agents = vec![
            agent("n1", "a1", &["cap:file"], true),
            agent("n2", "a2", &["cap:file"], true),
        ];
        let node_tags: HashMap<String, Vec<String>> = HashMap::new();
        let mut loads: HashMap<String, usize> = HashMap::new();
        loads.insert("n1".to_string(), 1);
        loads.insert("n2".to_string(), 0);

        let spec = spec(&["cap:file"], 100, "2026-01-01T00:00:00Z");
        let cands = Scheduler::candidates(&spec, &agents, &node_tags);
        let selected = Scheduler::select_node(&cands, &loads, sched.max_concurrent_per_node);
        assert_eq!(selected, Some("n2".to_string()), "负载较小的 n2 应被选中");
    }

    #[test]
    fn select_node_concurrency_cap() {
        let sched = Scheduler::new(1, 0);
        let agents = vec![agent("n1", "a1", &["cap:file"], true)];
        let node_tags: HashMap<String, Vec<String>> = HashMap::new();
        let mut loads: HashMap<String, usize> = HashMap::new();
        loads.insert("n1".to_string(), 1); // 已达上限

        let spec = spec(&["cap:file"], 100, "2026-01-01T00:00:00Z");
        let cands = Scheduler::candidates(&spec, &agents, &node_tags);
        let selected = Scheduler::select_node(&cands, &loads, sched.max_concurrent_per_node);
        assert_eq!(selected, None, "并发额度已满应返回 None");
    }

    #[test]
    fn schedule_batch_picks_concrete_agent_for_agentless_spec() {
        // v0.6 验收修复回归：agentless spec（无 to_agent）必须落到具体 agent，
        // 否则 mark_assigned 写空串 → send_engine_spec 必然「缺 to_agent」。
        let sched = Scheduler::new(2, 0);
        let agents = vec![
            agent("n1", "a1", &["cap:file"], true),
            agent("n2", "a2", &["cap:file"], true),
        ];
        let node_tags: HashMap<String, Vec<String>> = HashMap::new();
        let mut loads: HashMap<String, usize> = HashMap::new();
        let out = sched.schedule_batch(
            &[(
                "t".to_string(),
                spec(&["cap:file"], 100, "2026-01-01T00:00:00Z"),
            )],
            &agents,
            &node_tags,
            &mut loads,
            0,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1, "n1", "负载同分按 node_id 字典序");
        assert_eq!(out[0].2, "a1", "agentless 任务必须落到具体 agent");
    }

    #[test]
    fn schedule_batch_passes_explicit_to_agent() {
        let sched = Scheduler::new(2, 0);
        let agents = vec![agent("n1", "a1", &["cap:file"], true)];
        let node_tags: HashMap<String, Vec<String>> = HashMap::new();
        let mut loads: HashMap<String, usize> = HashMap::new();
        let mut s = spec(&["cap:file"], 100, "2026-01-01T00:00:00Z");
        s.as_object_mut()
            .unwrap()
            .insert("to_agent".to_string(), json!("pinned-agent"));
        let out = sched.schedule_batch(&[("t".to_string(), s)], &agents, &node_tags, &mut loads, 0);
        assert_eq!(out[0].2, "pinned-agent", "显式 to_agent 原样透传");
    }

    #[test]
    fn schedule_batch_priority_order() {
        let sched = Scheduler::new(10, 0);
        let agents = vec![agent("n1", "a1", &["cap:file"], true)];
        let node_tags: HashMap<String, Vec<String>> = HashMap::new();
        let mut loads: HashMap<String, usize> = HashMap::new();

        let tasks = vec![
            (
                "t-low".to_string(),
                spec(&["cap:file"], 200, "2026-01-01T00:00:00Z"),
            ),
            (
                "t-high".to_string(),
                spec(&["cap:file"], 50, "2026-01-01T00:00:00Z"),
            ),
        ];
        let result = sched.schedule_batch(&tasks, &agents, &node_tags, &mut loads, 0);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, "t-high", "优先级高的应先被调度");
        assert_eq!(result[1].0, "t-low");
    }

    #[test]
    fn max_inflight_cap() {
        let sched = Scheduler::new(10, 1);
        let agents = vec![agent("n1", "a1", &["cap:file"], true)];
        let node_tags: HashMap<String, Vec<String>> = HashMap::new();
        let mut loads: HashMap<String, usize> = HashMap::new();

        let tasks = vec![
            (
                "t1".to_string(),
                spec(&["cap:file"], 100, "2026-01-01T00:00:00Z"),
            ),
            (
                "t2".to_string(),
                spec(&["cap:file"], 100, "2026-01-01T00:00:01Z"),
            ),
        ];
        let result = sched.schedule_batch(&tasks, &agents, &node_tags, &mut loads, 1);
        assert_eq!(result.len(), 0, "已达全局 in-flight 上限应不调度");
    }

    #[test]
    fn namespace_isolation_and_shared_pool() {
        let agents = vec![
            agent("n1", "a1", &["cap:file"], true),
            agent("n2", "a2", &["cap:file"], true),
            agent("n3", "a3", &["cap:file"], true),
        ];
        let mut agents = agents;
        agents[0].namespace = Some("team-a".to_string());
        agents[1].namespace = Some("team-b".to_string());
        agents[2].namespace = Some("*".to_string());
        let node_tags: HashMap<String, Vec<String>> = HashMap::new();

        let mut ta = spec(&["cap:file"], 100, "2026-01-01T00:00:00Z");
        ta.as_object_mut()
            .unwrap()
            .insert("namespace".to_string(), json!("team-a"));
        let ids: Vec<&str> = Scheduler::candidates(&ta, &agents, &node_tags)
            .iter()
            .map(|a| a.agent_id.as_str())
            .collect();
        assert_eq!(ids, vec!["a1", "a3"], "team-a + 共享池");

        // 未声明 namespace = default：只有共享池 agent。
        let plain = spec(&["cap:file"], 100, "2026-01-01T00:00:00Z");
        let ids: Vec<&str> = Scheduler::candidates(&plain, &agents, &node_tags)
            .iter()
            .map(|a| a.agent_id.as_str())
            .collect();
        assert_eq!(ids, vec!["a3"], "default 只匹配共享池");
    }

    #[test]
    fn abac_filters_candidates_by_task_labels_and_node_tags() {
        use crate::abac::{AbacConfig, AbacRule};
        use std::collections::BTreeMap;

        let mut sched = Scheduler::new(2, 0);
        sched.abac = AbacConfig {
            enabled: true,
            rules: vec![AbacRule {
                name: "secret-high-only".to_string(),
                effect: "allow".to_string(),
                task_labels: BTreeMap::from([("class".to_string(), "secret".to_string())]),
                node_tags: vec!["zone:high".to_string()],
            }],
        };
        let agents = vec![
            agent("n-low", "a1", &["cap:file"], true),
            agent("n-high", "a2", &["cap:file"], true),
        ];
        let mut node_tags: HashMap<String, Vec<String>> = HashMap::new();
        node_tags.insert("n-low".to_string(), vec!["zone:low".to_string()]);
        node_tags.insert("n-high".to_string(), vec!["zone:high".to_string()]);
        let mut loads: HashMap<String, usize> = HashMap::new();

        let mut secret = spec(&["cap:file"], 100, "2026-01-01T00:00:00Z");
        secret
            .as_object_mut()
            .unwrap()
            .insert("labels".to_string(), json!({"class": "secret"}));
        let out = sched.schedule_batch(
            &[("t-secret".to_string(), secret)],
            &agents,
            &node_tags,
            &mut loads,
            0,
        );
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1, "n-high", "secret 只允许 zone:high");

        // 未命中 allow 规则 task_labels 的任务不受约束。
        let public = spec(&["cap:file"], 100, "2026-01-01T00:00:00Z");
        loads.clear();
        let out = sched.schedule_batch(
            &[("t-public".to_string(), public)],
            &agents,
            &node_tags,
            &mut loads,
            0,
        );
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn tie_break_deterministic() {
        let sched = Scheduler::new(2, 0);
        let agents = vec![
            agent("n2", "a2", &["cap:file"], true),
            agent("n1", "a1", &["cap:file"], true),
        ];
        let node_tags: HashMap<String, Vec<String>> = HashMap::new();
        let mut loads: HashMap<String, usize> = HashMap::new();

        let spec = spec(&["cap:file"], 100, "2026-01-01T00:00:00Z");
        let cands = Scheduler::candidates(&spec, &agents, &node_tags);
        let selected = Scheduler::select_node(&cands, &loads, sched.max_concurrent_per_node);
        assert_eq!(
            selected,
            Some("n1".to_string()),
            "同分时按 node_id 字典序，n1 < n2"
        );
    }
}
