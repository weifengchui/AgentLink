//! ABAC 策略引擎（v0.7 §4.2 第一波）。
//!
//! 纯函数、无 I/O：调度器在生成 candidate 时对每个「任务 spec × 节点 tags」
//! 求值。缺省 `security.abac.enabled=false`，所有节点放行（行为零变化）。
//!
//! 规则语义（最小可用模型）：
//! - 规则先按 `task_labels` 匹配任务：**全部** k=v 命中才算 applicable；
//! - `effect="deny"`：applicable 且节点具备 `node_tags` 全部标签 → 拒绝；
//! - `effect="allow"`：applicable 时节点必须命中至少一条 allow 规则的全部
//!   标签，否则拒绝（任务级 allowlist，只约束命中其 task_labels 的任务）；
//! - deny 优先于 allow；`node_tags` 为空 = 该规则对所有节点生效；
//! - 标签只认字符串值（非字符串忽略），节点标签缺失视为「不具备」。
//!
//! 决策只影响**派发候选**：被策略挡住的任务保持 pending，不会失败；节点打上
//! 正确标签后会自然被调度。远程节点 tags 经 `agents_update` 随 agent 表同步。

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// 策略配置（`security.abac`）。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AbacConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub rules: Vec<AbacRule>,
}

/// 单条规则。`effect` 缺省 `deny`（安全默认）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AbacRule {
    pub name: String,
    #[serde(default = "default_effect")]
    pub effect: String,
    #[serde(default)]
    pub task_labels: BTreeMap<String, String>,
    #[serde(default)]
    pub node_tags: Vec<String>,
}

fn default_effect() -> String {
    "deny".to_string()
}

/// 求值结果。`rule` 仅对 deny 命中给出规则名，便于排查/审计。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decision {
    pub allowed: bool,
    pub rule: Option<String>,
    pub reason: &'static str,
}

impl Decision {
    fn allow(reason: &'static str) -> Self {
        Decision {
            allowed: true,
            rule: None,
            reason,
        }
    }

    fn deny(rule: Option<String>, reason: &'static str) -> Self {
        Decision {
            allowed: false,
            rule,
            reason,
        }
    }
}

/// 从任务 spec 提取字符串标签（`spec.labels`）。非对象/非字符串值忽略。
pub fn task_labels(spec: &Value) -> BTreeMap<String, String> {
    spec.get("labels")
        .and_then(|v| v.as_object())
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

/// 对「任务 × 节点标签」求值。禁用/无规则 → 放行。
pub fn evaluate(cfg: &AbacConfig, spec: &Value, node_tags: &[String]) -> Decision {
    if !cfg.enabled || cfg.rules.is_empty() {
        return Decision::allow("disabled");
    }
    let labels = task_labels(spec);
    let mut allow_applicable = false;
    let mut allow_ok = false;
    for rule in &cfg.rules {
        let labels_match = rule
            .task_labels
            .iter()
            .all(|(k, v)| labels.get(k).is_some_and(|x| x == v));
        if !labels_match {
            continue;
        }
        let tags_match = rule
            .node_tags
            .iter()
            .all(|t| node_tags.iter().any(|x| x == t));
        if rule.effect == "allow" {
            allow_applicable = true;
            if tags_match {
                allow_ok = true;
            }
        } else if tags_match {
            // deny 优先：命中即拒绝，不再看其它规则。
            return Decision::deny(Some(rule.name.clone()), "deny");
        }
    }
    if allow_applicable && !allow_ok {
        return Decision::deny(None, "no_allow_match");
    }
    Decision::allow("ok")
}

/// 配置校验（`Config::validate` 调用）。只校验启用时的结构性错误。
pub fn validate(cfg: &AbacConfig) -> anyhow::Result<()> {
    if !cfg.enabled {
        return Ok(());
    }
    for (i, r) in cfg.rules.iter().enumerate() {
        if r.name.trim().is_empty() {
            anyhow::bail!("security.abac.rules[{i}].name must not be empty");
        }
        if r.effect != "allow" && r.effect != "deny" {
            anyhow::bail!(
                "security.abac.rules[{i}].effect must be 'allow' or 'deny' (got '{}')",
                r.effect
            );
        }
        if r.task_labels
            .iter()
            .any(|(k, v)| k.is_empty() || v.is_empty())
        {
            anyhow::bail!("security.abac.rules[{i}].task_labels keys/values must be non-empty");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn tags(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    fn cfg(rules: Vec<AbacRule>) -> AbacConfig {
        AbacConfig {
            enabled: true,
            rules,
        }
    }

    fn rule(name: &str, effect: &str, labels: &[(&str, &str)], tags: &[&str]) -> AbacRule {
        AbacRule {
            name: name.to_string(),
            effect: effect.to_string(),
            task_labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            node_tags: tags.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn disabled_allows_everything() {
        let c = AbacConfig {
            enabled: false,
            rules: vec![rule("deny-all", "deny", &[], &[])],
        };
        assert!(evaluate(&c, &json!({"labels": {"data": "finance"}}), &[]).allowed);
    }

    #[test]
    fn deny_matches_labels_and_tags() {
        let c = cfg(vec![rule(
            "finance-not-low",
            "deny",
            &[("data", "finance")],
            &["zone:low"],
        )]);
        let d = evaluate(
            &c,
            &json!({"labels": {"data": "finance"}}),
            &tags(&["zone:low"]),
        );
        assert!(!d.allowed);
        assert_eq!(d.rule.as_deref(), Some("finance-not-low"));
        // 标签不匹配 / 节点不是 low → 放行
        assert!(
            evaluate(
                &c,
                &json!({"labels": {"data": "public"}}),
                &tags(&["zone:low"])
            )
            .allowed
        );
        assert!(
            evaluate(
                &c,
                &json!({"labels": {"data": "finance"}}),
                &tags(&["zone:high"])
            )
            .allowed
        );
        // 缺标签同样不命中 deny
        assert!(evaluate(&c, &json!({}), &tags(&["zone:low"])).allowed);
    }

    #[test]
    fn allow_requires_all_tags_and_only_for_scoped_labels() {
        let c = cfg(vec![rule(
            "secret-high-only",
            "allow",
            &[("class", "secret")],
            &["zone:high"],
        )]);
        assert!(
            evaluate(
                &c,
                &json!({"labels": {"class": "secret"}}),
                &tags(&["zone:high"])
            )
            .allowed
        );
        let d = evaluate(
            &c,
            &json!({"labels": {"class": "secret"}}),
            &tags(&["zone:low"]),
        );
        assert!(!d.allowed);
        assert_eq!(d.reason, "no_allow_match");
        // 不命中 allow 规则 task_labels 的任务不受影响
        assert!(
            evaluate(
                &c,
                &json!({"labels": {"class": "public"}}),
                &tags(&["zone:low"])
            )
            .allowed
        );
        // 节点缺标签按「不具备」处理
        assert!(!evaluate(&c, &json!({"labels": {"class": "secret"}}), &[]).allowed);
    }

    #[test]
    fn deny_beats_allow() {
        let c = cfg(vec![
            rule(
                "allow-high",
                "allow",
                &[("data", "finance")],
                &["zone:high"],
            ),
            rule("deny-low", "deny", &[("data", "finance")], &["zone:low"]),
        ]);
        assert!(
            evaluate(
                &c,
                &json!({"labels": {"data": "finance"}}),
                &tags(&["zone:high"])
            )
            .allowed
        );
        assert!(
            !evaluate(
                &c,
                &json!({"labels": {"data": "finance"}}),
                &tags(&["zone:low"])
            )
            .allowed
        );
    }

    #[test]
    fn validate_rejects_bad_effect() {
        let mut c = cfg(vec![rule("x", "maybe", &[], &[])]);
        assert!(validate(&c).is_err());
        c.rules[0].effect = "deny".to_string();
        assert!(validate(&c).is_ok());
    }

    #[test]
    fn only_string_labels_are_used() {
        let l = task_labels(&json!({"labels": {"a": "x", "b": 1, "c": true}}));
        assert_eq!(l.get("a").map(String::as_str), Some("x"));
        assert!(!l.contains_key("b"));
        assert!(!l.contains_key("c"));
    }
}
