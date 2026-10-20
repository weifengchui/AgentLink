//! OIDC SSO 第一波（v0.7 §4.2 企业门槛）：**token introspection**（RFC 7662）
//! + claim → RBAC 角色映射 + 可选 namespace 绑定。
//!
//! 为什么先做 introspection 而不是 JWT 本地验签：
//! - 单二进制、无 C 依赖：现有 reqwest(rustls) 已具备，无需引入 RSA/ring；
//! - 兼容 opaque access token 与 JWT（提供商侧自省），不绑定算法；
//! - 失败按 401 fail-closed，未知 token 绝不落到 `fallback_role`。
//!
//! 网络调用在 `api::oidc_middleware`；本模块只做配置、claim 解析、缓存与
//! 校验，保持纯函数可测。缺省 `auth.oidc.enabled=false`，静态 token 行为
//! 逐字节不变。

use crate::config::Role;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

/// `auth.oidc` 配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OidcConfig {
    #[serde(default)]
    pub enabled: bool,
    /// RFC 7662 introspection endpoint。
    #[serde(default)]
    pub introspection_url: String,
    #[serde(default)]
    pub client_id: String,
    #[serde(default)]
    pub client_secret: String,
    /// 角色 claim 名（缺省 `role`）；值可为字符串或字符串数组。
    #[serde(default = "default_role_claim")]
    pub role_claim: String,
    /// claim 值 → 角色名。非空时必须命中；空则直接把 claim 值当角色名。
    #[serde(default)]
    pub role_map: BTreeMap<String, String>,
    /// claim 缺失/未命中时的兜底角色（缺省拒绝，不落 Admin）。
    #[serde(default)]
    pub default_role: Option<String>,
    /// 可选：从该 claim 读取 namespace，绑定 token 租户范围。
    /// 有 namespace 的 token **不能**映射为 Admin。
    #[serde(default)]
    pub namespace_claim: Option<String>,
    /// introspection 结果缓存秒数（1-3600，缺省 30）。
    #[serde(default = "default_cache_seconds")]
    pub cache_seconds: u64,
    /// introspection HTTP 超时秒数（1-30，缺省 3）。
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
}

fn default_role_claim() -> String {
    "role".to_string()
}

fn default_cache_seconds() -> u64 {
    30
}

fn default_timeout_seconds() -> u64 {
    3
}

impl Default for OidcConfig {
    fn default() -> Self {
        OidcConfig {
            enabled: false,
            introspection_url: String::new(),
            client_id: String::new(),
            client_secret: String::new(),
            role_claim: default_role_claim(),
            role_map: BTreeMap::new(),
            default_role: None,
            namespace_claim: None,
            cache_seconds: default_cache_seconds(),
            timeout_seconds: default_timeout_seconds(),
        }
    }
}

/// introspection 成功后的解析结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolved {
    pub role: Role,
    pub namespace: Option<String>,
}

/// 解析角色名（大小写不敏感）。
pub fn parse_role(s: &str) -> Option<Role> {
    match s.trim().to_ascii_lowercase().as_str() {
        "admin" => Some(Role::Admin),
        "operator" => Some(Role::Operator),
        "viewer" => Some(Role::Viewer),
        "executor" => Some(Role::Executor),
        _ => None,
    }
}

/// 租户名格式（与 API/config 的 namespace 口径一致）。
pub fn valid_namespace(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s != "*"
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
}

fn claim_strings(v: &Value) -> Vec<&str> {
    match v {
        Value::String(s) => vec![s.as_str()],
        Value::Array(items) => items.iter().filter_map(|x| x.as_str()).collect(),
        _ => Vec::new(),
    }
}

/// 把 introspection 响应转成角色 + 可选 namespace。
///
/// 规则：
/// - 必须有 `active=true`（调用方已检查，这里再防御一次）；
/// - 按 `role_claim` 取一个或多个值，依次走 `role_map`（非空时）或直接角色名；
/// - 全部未命中才用 `default_role`，仍无则拒绝；
/// - `namespace_claim` 存在时必须有合法值，且角色不得为 Admin。
pub fn resolve(cfg: &OidcConfig, claims: &Value) -> Result<Resolved, String> {
    if claims.get("active").and_then(|v| v.as_bool()) != Some(true) {
        return Err("token inactive".to_string());
    }
    let raw_roles = claims
        .get(&cfg.role_claim)
        .map(claim_strings)
        .unwrap_or_default();
    let mut role = None;
    for raw in raw_roles {
        let mapped = if cfg.role_map.is_empty() {
            Some(raw)
        } else {
            cfg.role_map.get(raw).map(String::as_str)
        };
        if let Some(r) = mapped.and_then(parse_role) {
            role = Some(r);
            break;
        }
    }
    let role = role
        .or_else(|| cfg.default_role.as_deref().and_then(parse_role))
        .ok_or_else(|| "no recognized role claim".to_string())?;

    let namespace = match cfg.namespace_claim.as_deref() {
        None => None,
        Some(claim) => {
            let raw = claims
                .get(claim)
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty());
            match raw {
                Some(ns) => {
                    if !valid_namespace(ns) {
                        return Err(format!("invalid namespace claim value {ns:?}"));
                    }
                    if role == Role::Admin {
                        return Err("namespace-bound OIDC token cannot be Admin".to_string());
                    }
                    Some(ns.to_string())
                }
                // 多租户模式：非 Admin 必须带 namespace，避免 IdP 漏配 claim
                // 时静默获得全局 operator 权限；Admin 允许不带（全局治理）。
                None if role == Role::Admin => None,
                None => {
                    return Err(format!(
                        "namespace claim {claim:?} missing for non-admin token"
                    ))
                }
            }
        }
    };
    Ok(Resolved { role, namespace })
}

/// introspection 成功结果缓存（key = token 的 sha256，不存原始 token）。
#[derive(Debug, Default)]
pub struct Cache {
    entries: HashMap<String, (Resolved, Instant)>,
}

impl Cache {
    pub fn get(&mut self, key: &str) -> Option<Resolved> {
        let now = Instant::now();
        match self.entries.get(key) {
            Some((v, exp)) if *exp > now => Some(v.clone()),
            Some(_) => {
                self.entries.remove(key);
                None
            }
            None => None,
        }
    }

    pub fn put(&mut self, key: String, value: Resolved, ttl: Duration) {
        self.entries.insert(key, (value, Instant::now() + ttl));
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }
}

/// 配置校验（`Config::validate` 调用）。
pub fn validate(cfg: &OidcConfig) -> anyhow::Result<()> {
    if !cfg.enabled {
        return Ok(());
    }
    if cfg.introspection_url.starts_with("http://") {
        // 明文 HTTP 会把 client_secret 与 bearer token 暴露在链路上；仅允许
        // 回环地址（本地 mock/开发）。生产必须 https。
        let rest = &cfg.introspection_url["http://".len()..];
        let hostport = rest.split('/').next().unwrap_or("");
        let host = if let Some(v6) = hostport.strip_prefix('[') {
            v6.split(']').next().unwrap_or("")
        } else {
            hostport.split(':').next().unwrap_or("")
        };
        if !matches!(host, "127.0.0.1" | "localhost" | "::1") {
            anyhow::bail!(
                "auth.oidc.introspection_url: plain http is only allowed for loopback; use https"
            );
        }
    } else if !cfg.introspection_url.starts_with("https://") {
        anyhow::bail!("auth.oidc.introspection_url must start with https:// (or loopback http)");
    }
    if cfg.client_id.trim().is_empty() {
        anyhow::bail!("auth.oidc.client_id must not be empty");
    }
    if cfg.client_secret.is_empty() {
        anyhow::bail!("auth.oidc.client_secret must not be empty");
    }
    if cfg.role_claim.trim().is_empty() {
        anyhow::bail!("auth.oidc.role_claim must not be empty");
    }
    for (k, v) in &cfg.role_map {
        if k.trim().is_empty() {
            anyhow::bail!("auth.oidc.role_map key must not be empty");
        }
        if parse_role(v).is_none() {
            anyhow::bail!("auth.oidc.role_map[{k:?}] must be admin/operator/viewer/executor");
        }
    }
    if let Some(r) = &cfg.default_role {
        match parse_role(r) {
            None => anyhow::bail!(
                "auth.oidc.default_role must be operator/viewer/executor (Admin not allowed)"
            ),
            Some(Role::Admin) => anyhow::bail!(
                "auth.oidc.default_role must not be Admin (fail-closed; map an explicit claim to admin)"
            ),
            Some(_) => {}
        }
    }
    if let Some(c) = &cfg.namespace_claim {
        if c.trim().is_empty() {
            anyhow::bail!("auth.oidc.namespace_claim must not be empty when set");
        }
    }
    if !(1..=3600).contains(&cfg.cache_seconds) {
        anyhow::bail!("auth.oidc.cache_seconds must be 1-3600");
    }
    if !(1..=30).contains(&cfg.timeout_seconds) {
        anyhow::bail!("auth.oidc.timeout_seconds must be 1-30");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cfg() -> OidcConfig {
        let mut role_map = BTreeMap::new();
        role_map.insert("ops".to_string(), "operator".to_string());
        role_map.insert("aud".to_string(), "viewer".to_string());
        OidcConfig {
            enabled: true,
            role_map,
            ..Default::default()
        }
    }

    #[test]
    fn maps_role_claim_and_array_first_match() {
        let c = cfg();
        let r = resolve(&c, &json!({"active": true, "role": "ops"})).unwrap();
        assert_eq!(r.role, Role::Operator);
        assert_eq!(r.namespace, None);

        let r = resolve(&c, &json!({"active": true, "role": ["unknown", "aud"]})).unwrap();
        assert_eq!(r.role, Role::Viewer);

        // role_map 非空时不接受裸角色名，除非 default_role 兜底。
        assert!(resolve(&c, &json!({"active": true, "role": "operator"})).is_err());
        let mut c2 = c.clone();
        c2.default_role = Some("viewer".to_string());
        assert_eq!(
            resolve(&c2, &json!({"active": true, "role": "operator"}))
                .unwrap()
                .role,
            Role::Viewer
        );
    }

    #[test]
    fn rejects_inactive_missing_and_unknown_role() {
        let c = cfg();
        assert!(resolve(&c, &json!({"active": false})).is_err());
        assert!(resolve(&c, &json!({"role": "ops"})).is_err());
        assert!(resolve(&c, &json!({"active": true, "role": "nope"})).is_err());
    }

    #[test]
    fn namespace_claim_binds_non_admin() {
        let mut c = cfg();
        c.namespace_claim = Some("namespace".to_string());
        let r = resolve(
            &c,
            &json!({"active": true, "role": "ops", "namespace": "team-a"}),
        )
        .unwrap();
        assert_eq!(r.role, Role::Operator);
        assert_eq!(r.namespace.as_deref(), Some("team-a"));
        assert!(resolve(
            &c,
            &json!({"active": true, "role": "ops", "namespace": "bad name"})
        )
        .is_err());
        assert!(resolve(
            &c,
            &json!({"active": true, "role": "ops", "namespace": "*"})
        )
        .is_err());
        // 多租户模式：非 Admin 缺 namespace 拒绝；Admin 可全局，但不可绑定。
        assert!(resolve(&c, &json!({"active": true, "role": "ops"})).is_err());
        let mut c_admin = cfg();
        c_admin.namespace_claim = Some("namespace".to_string());
        c_admin
            .role_map
            .insert("root".to_string(), "admin".to_string());
        assert_eq!(
            resolve(&c_admin, &json!({"active": true, "role": "root"}))
                .unwrap()
                .namespace,
            None
        );
        assert!(resolve(
            &c_admin,
            &json!({"active": true, "role": "root", "namespace": "team-a"})
        )
        .is_err());
    }

    #[test]
    fn cache_expires() {
        let mut cache = Cache::default();
        let v = Resolved {
            role: Role::Viewer,
            namespace: None,
        };
        cache.put("k".to_string(), v.clone(), Duration::from_millis(40));
        assert_eq!(cache.get("k"), Some(v));
        assert_eq!(cache.len(), 1);
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(cache.get("k"), None);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn validate_requires_url_and_credentials() {
        let mut c = cfg();
        assert!(validate(&c).is_err(), "enabled 时必填 URL/凭据");
        c.introspection_url = "http://127.0.0.1:1/introspect".to_string();
        c.client_id = "id".to_string();
        c.client_secret = "secret".to_string();
        assert!(validate(&c).is_ok());
        c.role_map.insert("x".to_string(), "root".to_string());
        assert!(validate(&c).is_err());
        c.role_map.remove("x");
        c.cache_seconds = 0;
        assert!(validate(&c).is_err());
        c.cache_seconds = 30;
        c.default_role = Some("admin".to_string());
        assert!(validate(&c).is_err(), "default_role 不允许 Admin");
        c.default_role = Some("viewer".to_string());
        c.introspection_url = "http://idp.example.com/introspect".to_string();
        assert!(validate(&c).is_err(), "非回环 http 拒绝");
        c.introspection_url = "https://idp.example.com/introspect".to_string();
        assert!(validate(&c).is_ok());
        c.enabled = false;
        assert!(validate(&c).is_ok(), "关闭态不校验");
    }
}
