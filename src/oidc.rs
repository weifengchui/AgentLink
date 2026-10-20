//! OIDC SSO（v0.7 §4.2）：token introspection（第一波）+ JWT/JWKS 本地验签 /
//! Authorization Code + PKCE 浏览器登录（第二波）。
//!
//! 两种 API token 验证路径：
//! - `introspection`（缺省）：RFC 7662，兼容 opaque/JWT access token；
//! - `jwt`：RS256 JWT 本地验签（JWKS 缓存），IdP 短时故障不影响已缓存公钥；
//! - `hybrid`：JWT 优先，非 JWT/验签失败再走 introspection（fail-closed）。
//!
//! 浏览器登录只走 Authorization Code + PKCE（S256），ID token 必须 RS256
//! 本地验签（iss/aud/exp/nbf/nonce），成功后签发**本节点短期 opaque session
//! token**，不把 IdP access/ID token 交给 Web UI。
//!
//! 依赖保持纯 Rust：`rsa`/`sha2`/`base64`/`rand`，无 C/openssl。缺省
//! `auth.oidc.enabled=false`、静态 token/旧配置行为不变。

use crate::config::Role;
use base64::Engine;
use rand::RngCore;
use rsa::{BigUint, Pkcs1v15Sign, RsaPublicKey};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

const B64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::URL_SAFE_NO_PAD;
const B64_PAD: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::URL_SAFE;
const LOGIN_TTL: Duration = Duration::from_secs(300);
const MAX_LOGINS: usize = 1024;
const MAX_SESSIONS: usize = 4096;

/// API token 验证模式。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OidcMode {
    /// RFC 7662 introspection（缺省，兼容 opaque token）。
    #[default]
    Introspection,
    /// 仅 RS256 JWT 本地验签（JWKS）。
    Jwt,
    /// JWT 本地验签优先，非 JWT 或失败再走 introspection。
    Hybrid,
}

/// `auth.oidc` 配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OidcConfig {
    #[serde(default)]
    pub enabled: bool,
    /// API token 验证模式；缺省 introspection（第一波行为不变）。
    #[serde(default)]
    pub mode: OidcMode,
    /// RFC 7662 introspection endpoint（mode=introspection/hybrid 必填）。
    #[serde(default)]
    pub introspection_url: String,
    #[serde(default)]
    pub client_id: String,
    #[serde(default)]
    pub client_secret: String,
    /// JWT `aud` 期望值；缺省用 `client_id`。IdP 的 access token audience
    /// 与 client_id 不同时设置（ID token 通常仍为 client_id）。
    #[serde(default)]
    pub audience: Option<String>,
    /// JWT 的 `iss` 必须精确等于该值（mode=jwt/hybrid 或浏览器登录必填）。
    #[serde(default)]
    pub issuer: Option<String>,
    /// JWKS 地址（mode=jwt/hybrid 或浏览器登录必填）。
    #[serde(default)]
    pub jwks_uri: Option<String>,
    /// JWKS 缓存秒数（1-86400，缺省 3600）。
    #[serde(default = "default_jwks_cache_seconds")]
    pub jwks_cache_seconds: u64,
    /// 时钟偏移容忍秒数（0-300，缺省 60）。
    #[serde(default = "default_leeway_seconds")]
    pub leeway_seconds: u64,
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
    /// 有 namespace 的 token **不能**映射为 Admin；多租户模式下非 Admin 必须带。
    #[serde(default)]
    pub namespace_claim: Option<String>,
    /// introspection/JWT 结果缓存秒数（1-3600，缺省 30）；JWT 不会超过 exp。
    #[serde(default = "default_cache_seconds")]
    pub cache_seconds: u64,
    /// OIDC HTTP 超时秒数（1-30，缺省 3）。
    #[serde(default = "default_timeout_seconds")]
    pub timeout_seconds: u64,
    /// 开启浏览器 Authorization Code + PKCE 登录。
    #[serde(default)]
    pub browser_login: bool,
    /// IdP authorization endpoint（browser_login 必填）。
    #[serde(default)]
    pub authorization_endpoint: Option<String>,
    /// IdP token endpoint（browser_login 必填）。
    #[serde(default)]
    pub token_endpoint: Option<String>,
    /// 回调地址，必须与 IdP 注册值逐字符一致（browser_login 必填）。
    #[serde(default)]
    pub redirect_uri: Option<String>,
    /// 登录成功后的站内跳转路径（缺省 `/web`），不得是 `//host`。
    #[serde(default = "default_post_login_path")]
    pub post_login_path: String,
    /// 授权 scope（缺省 `openid profile email`；必须包含 `openid`）。
    #[serde(default = "default_scopes")]
    pub scopes: String,
    /// 浏览器会话 token 有效期秒数（1-86400，缺省 8h）；不会超过 ID token exp。
    #[serde(default = "default_session_seconds")]
    pub session_seconds: u64,
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

fn default_jwks_cache_seconds() -> u64 {
    3600
}

fn default_leeway_seconds() -> u64 {
    60
}

fn default_post_login_path() -> String {
    "/web".to_string()
}

fn default_scopes() -> String {
    "openid profile email".to_string()
}

fn default_session_seconds() -> u64 {
    8 * 3600
}

impl Default for OidcConfig {
    fn default() -> Self {
        OidcConfig {
            enabled: false,
            mode: OidcMode::default(),
            introspection_url: String::new(),
            client_id: String::new(),
            client_secret: String::new(),
            audience: None,
            issuer: None,
            jwks_uri: None,
            jwks_cache_seconds: default_jwks_cache_seconds(),
            leeway_seconds: default_leeway_seconds(),
            role_claim: default_role_claim(),
            role_map: BTreeMap::new(),
            default_role: None,
            namespace_claim: None,
            cache_seconds: default_cache_seconds(),
            timeout_seconds: default_timeout_seconds(),
            browser_login: false,
            authorization_endpoint: None,
            token_endpoint: None,
            redirect_uri: None,
            post_login_path: default_post_login_path(),
            scopes: default_scopes(),
            session_seconds: default_session_seconds(),
        }
    }
}

/// introspection/JWT 成功后的解析结果。
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

/// introspection 响应 → 角色 + 可选 namespace（必须 `active=true`）。
pub fn resolve(cfg: &OidcConfig, claims: &Value) -> Result<Resolved, String> {
    if claims.get("active").and_then(|v| v.as_bool()) != Some(true) {
        return Err("token inactive".to_string());
    }
    resolve_claims(cfg, claims)
}

/// JWT ID token / access token 的 claim → 角色 + 可选 namespace。
/// 不要求 `active`；若响应显式带 `active=false` 仍拒绝。
pub fn resolve_claims(cfg: &OidcConfig, claims: &Value) -> Result<Resolved, String> {
    if claims.get("active").and_then(|v| v.as_bool()) == Some(false) {
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

/// JWT 三段解析结果（未验签）。
#[derive(Debug, Clone)]
pub struct JwtParts {
    pub kid: Option<String>,
    pub claims: Value,
    pub signing_input: String,
    pub signature: Vec<u8>,
}

fn b64_decode(s: &str) -> Result<Vec<u8>, String> {
    B64.decode(s)
        .or_else(|_| B64_PAD.decode(s))
        .map_err(|e| format!("base64url: {e}"))
}

/// 拆分/解码 JWT；只接受 `alg=RS256`（拒绝 none/HS*/PS*/ES*）。
pub fn split_jwt(token: &str) -> Result<JwtParts, String> {
    let mut parts = token.split('.');
    let h = parts.next().ok_or("jwt: missing header")?;
    let p = parts.next().ok_or("jwt: missing payload")?;
    let s = parts.next().ok_or("jwt: missing signature")?;
    if parts.next().is_some() {
        return Err("jwt: too many segments".to_string());
    }
    let header: Value =
        serde_json::from_slice(&b64_decode(h)?).map_err(|e| format!("jwt header: {e}"))?;
    let alg = header
        .get("alg")
        .and_then(|v| v.as_str())
        .ok_or("jwt: header.alg missing")?
        .to_string();
    if alg != "RS256" {
        return Err(format!("jwt: unsupported alg {alg:?} (only RS256)"));
    }
    if header.get("crit").is_some() {
        return Err("jwt: critical header extensions are not supported".to_string());
    }
    let kid = match header.get("kid") {
        None | Some(Value::Null) => None,
        Some(Value::String(k)) if !k.is_empty() => Some(k.clone()),
        Some(_) => return Err("jwt: header.kid must be a non-empty string".to_string()),
    };
    let claims: Value =
        serde_json::from_slice(&b64_decode(p)?).map_err(|e| format!("jwt payload: {e}"))?;
    if !claims.is_object() {
        return Err("jwt: payload must be an object".to_string());
    }
    let signature = b64_decode(s)?;
    if signature.is_empty() {
        return Err("jwt: empty signature".to_string());
    }
    Ok(JwtParts {
        kid,
        claims,
        signing_input: format!("{h}.{p}"),
        signature,
    })
}

fn claim_i64(v: &Value) -> Option<i64> {
    v.as_i64()
        .or_else(|| v.as_f64().map(|f| f as i64))
        .or_else(|| v.as_u64().and_then(|u| i64::try_from(u).ok()))
}

fn audience_contains(v: &Value, client_id: &str) -> bool {
    match v {
        Value::String(s) => s == client_id,
        Value::Array(a) => a.iter().any(|x| x.as_str() == Some(client_id)),
        _ => false,
    }
}

/// 校验 JWT 的 iss/aud/azp/exp/nbf/iat/nonce，再映射角色/namespace。
/// 返回 `(Resolved, exp)`，调用方据此限制缓存/会话 TTL。
pub fn validate_jwt_claims(
    cfg: &OidcConfig,
    claims: &Value,
    expected_nonce: Option<&str>,
    now: i64,
) -> Result<(Resolved, i64), String> {
    let issuer = cfg
        .issuer
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or("jwt: auth.oidc.issuer not configured")?;
    let iss = claims
        .get("iss")
        .and_then(|v| v.as_str())
        .ok_or("jwt: iss missing")?;
    if iss != issuer {
        return Err(format!("jwt: issuer mismatch ({iss:?})"));
    }
    if cfg.client_id.is_empty() {
        return Err("jwt: client_id not configured".to_string());
    }
    let expected_aud = cfg.audience.as_deref().unwrap_or(&cfg.client_id);
    let aud = claims.get("aud").ok_or("jwt: aud missing")?;
    if !audience_contains(aud, expected_aud) {
        return Err("jwt: audience mismatch".to_string());
    }
    if let Some(azp) = claims.get("azp") {
        let azp = azp
            .as_str()
            .ok_or("jwt: azp must be a string".to_string())?;
        if azp != cfg.client_id {
            return Err("jwt: azp mismatch".to_string());
        }
    }
    let exp = claims
        .get("exp")
        .and_then(claim_i64)
        .ok_or("jwt: exp missing")?;
    let leeway = cfg.leeway_seconds.min(300) as i64;
    if now >= exp.saturating_add(leeway) {
        return Err("jwt: token expired".to_string());
    }
    if let Some(v) = claims.get("nbf") {
        let nbf = claim_i64(v).ok_or("jwt: nbf must be a number")?;
        if nbf > now.saturating_add(leeway) {
            return Err("jwt: token not yet valid (nbf)".to_string());
        }
    }
    if let Some(v) = claims.get("iat") {
        let iat = claim_i64(v).ok_or("jwt: iat must be a number")?;
        if iat > now.saturating_add(leeway) {
            return Err("jwt: iat in the future".to_string());
        }
    }
    if let Some(expected) = expected_nonce {
        let got = claims
            .get("nonce")
            .and_then(|v| v.as_str())
            .ok_or("jwt: nonce missing")?;
        if got != expected {
            return Err("jwt: nonce mismatch".to_string());
        }
    }
    Ok((resolve_claims(cfg, claims)?, exp))
}

/// JWKS 中的 RSA 公钥（未构建 RsaPublicKey）。
#[derive(Debug, Clone)]
pub struct RsaJwk {
    pub kid: Option<String>,
    pub n: Vec<u8>,
    pub e: Vec<u8>,
}

impl RsaJwk {
    pub fn to_public_key(&self) -> Result<RsaPublicKey, String> {
        // 现代 IdP 至少 2048-bit；拒绝弱密钥（< 256 字节模长）。
        if self.n.len() < 256 {
            return Err("jwk: RSA modulus shorter than 2048 bits".to_string());
        }
        RsaPublicKey::new(
            BigUint::from_bytes_be(&self.n),
            BigUint::from_bytes_be(&self.e),
        )
        .map_err(|e| format!("jwk: invalid RSA key: {e}"))
    }
}

/// 解析 JWKS：只保留 `kty=RSA`、`use=sig`（或未声明）、`alg=RS256`
/// （或未声明）且 n/e 合法的 key。
pub fn parse_jwks(v: &Value) -> Result<Vec<RsaJwk>, String> {
    let keys = v
        .get("keys")
        .and_then(|k| k.as_array())
        .ok_or("jwks: keys array missing")?;
    let mut out = Vec::new();
    for k in keys {
        if k.get("kty").and_then(|x| x.as_str()) != Some("RSA") {
            continue;
        }
        if let Some(use_) = k.get("use").and_then(|x| x.as_str()) {
            if use_ != "sig" {
                continue;
            }
        }
        if let Some(alg) = k.get("alg").and_then(|x| x.as_str()) {
            if alg != "RS256" {
                continue;
            }
        }
        let n = match k.get("n").and_then(|x| x.as_str()) {
            Some(s) => b64_decode(s)?,
            None => continue,
        };
        let e = match k.get("e").and_then(|x| x.as_str()) {
            Some(s) => b64_decode(s)?,
            None => continue,
        };
        let kid = k
            .get("kid")
            .and_then(|x| x.as_str())
            .filter(|s| !s.is_empty())
            .map(str::to_string);
        out.push(RsaJwk { kid, n, e });
    }
    if out.is_empty() {
        return Err("jwks: no usable RS256 signing keys".to_string());
    }
    Ok(out)
}

/// JWKS 内存缓存（不保存私钥/原始 JWT）。
#[derive(Debug, Default)]
pub struct JwksCache {
    keys: Vec<(Option<String>, RsaPublicKey)>,
    fetched: Option<Instant>,
}

impl JwksCache {
    pub fn fresh(&self, ttl_seconds: u64) -> bool {
        self.fetched
            .is_some_and(|t| t.elapsed() < Duration::from_secs(ttl_seconds.clamp(1, 86_400)))
    }

    pub fn get(&self, kid: Option<&str>) -> Option<RsaPublicKey> {
        match kid {
            Some(k) => self
                .keys
                .iter()
                .find(|(kid, _)| kid.as_deref() == Some(k))
                .map(|(_, key)| key.clone()),
            // 无 kid 时只有唯一 key 才可安全选择。
            None if self.keys.len() == 1 => Some(self.keys[0].1.clone()),
            None => None,
        }
    }

    pub fn set(&mut self, jwks: &[RsaJwk]) -> Result<(), String> {
        let mut keys = Vec::new();
        for jwk in jwks {
            if let Ok(key) = jwk.to_public_key() {
                keys.push((jwk.kid.clone(), key));
            }
        }
        if keys.is_empty() {
            return Err("jwks: no usable RSA public keys".to_string());
        }
        self.keys = keys;
        self.fetched = Some(Instant::now());
        Ok(())
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.keys.len()
    }
}

/// 验签 RS256 签名（signing_input 原文，签名已 base64url 解码）。
pub fn verify_rs256(
    key: &RsaPublicKey,
    signing_input: &str,
    signature: &[u8],
) -> Result<(), String> {
    let hashed = Sha256::digest(signing_input.as_bytes());
    key.verify(Pkcs1v15Sign::new::<Sha256>(), hashed.as_slice(), signature)
        .map_err(|_| "jwt: signature verification failed".to_string())
}

/// PKCE S256 challenge = base64url(sha256(verifier))，无 padding。
pub fn pkce_challenge(verifier: &str) -> String {
    B64.encode(Sha256::digest(verifier.as_bytes()))
}

/// 加密随机 bytes → base64url（无 padding）。
pub fn random_b64(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rand::rngs::OsRng.fill_bytes(&mut buf);
    B64.encode(buf)
}

/// 浏览器登录待回调状态（state 一次性、5 分钟过期）。
#[derive(Debug, Clone)]
pub struct PendingLogin {
    pub verifier: String,
    pub nonce: String,
    pub redirect_uri: String,
    pub created: Instant,
}

#[derive(Debug, Default)]
pub struct LoginStore {
    entries: HashMap<String, PendingLogin>,
}

impl LoginStore {
    pub fn put(&mut self, state: String, verifier: String, nonce: String, redirect_uri: String) {
        self.purge();
        if self.entries.len() >= MAX_LOGINS {
            if let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, p)| p.created)
                .map(|(k, _)| k.clone())
            {
                self.entries.remove(&oldest);
            }
        }
        self.entries.insert(
            state,
            PendingLogin {
                verifier,
                nonce,
                redirect_uri,
                created: Instant::now(),
            },
        );
    }

    /// 取出并删除（防 state 重放）；过期返回 None。
    pub fn take(&mut self, state: &str) -> Option<PendingLogin> {
        self.purge();
        let p = self.entries.remove(state)?;
        (p.created.elapsed() <= LOGIN_TTL).then_some(p)
    }

    fn purge(&mut self) {
        let now = Instant::now();
        self.entries
            .retain(|_, p| now.duration_since(p.created) <= LOGIN_TTL);
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }
}

/// 浏览器登录成功后签发的本节点短期会话（不持久化）。
#[derive(Debug, Clone)]
pub struct Session {
    pub role: Role,
    pub namespace: Option<String>,
    pub sub: String,
    pub expires: Instant,
}

#[derive(Debug, Default)]
pub struct SessionStore {
    entries: HashMap<String, Session>,
}

impl SessionStore {
    pub fn issue(
        &mut self,
        role: Role,
        namespace: Option<String>,
        ttl: Duration,
        sub: String,
    ) -> String {
        self.purge();
        if self.entries.len() >= MAX_SESSIONS {
            if let Some(k) = self.entries.keys().next().cloned() {
                self.entries.remove(&k);
            }
        }
        let token = format!("al_sess_{}", random_b64(32));
        self.entries.insert(
            token.clone(),
            Session {
                role,
                namespace,
                sub,
                expires: Instant::now() + ttl,
            },
        );
        token
    }

    pub fn get(&mut self, token: &str) -> Option<Session> {
        let now = Instant::now();
        match self.entries.get(token) {
            Some(s) if s.expires > now => Some(s.clone()),
            Some(_) => {
                self.entries.remove(token);
                None
            }
            None => None,
        }
    }

    pub fn remove(&mut self, token: &str) -> Option<Session> {
        self.entries.remove(token)
    }

    fn purge(&mut self) {
        let now = Instant::now();
        self.entries.retain(|_, s| s.expires > now);
    }

    #[cfg(test)]
    fn len(&mut self) -> usize {
        self.purge();
        self.entries.len()
    }
}

/// introspection 结果缓存（key = token 的 sha256，不存原始 token）。
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

fn check_http_url(name: &str, url: &str) -> anyhow::Result<()> {
    if url.starts_with("https://") {
        return Ok(());
    }
    if let Some(rest) = url.strip_prefix("http://") {
        let hostport = rest.split('/').next().unwrap_or("");
        let host = if let Some(v6) = hostport.strip_prefix('[') {
            v6.split(']').next().unwrap_or("")
        } else {
            hostport.split(':').next().unwrap_or("")
        };
        if matches!(host, "127.0.0.1" | "localhost" | "::1") {
            return Ok(());
        }
        anyhow::bail!("{name}: plain http is only allowed for loopback; use https");
    }
    anyhow::bail!("{name} must start with https:// (or loopback http)");
}

fn require_url(name: &str, v: &Option<String>) -> anyhow::Result<()> {
    let s = v.as_deref().unwrap_or("").trim();
    if s.is_empty() {
        anyhow::bail!("{name} must not be empty");
    }
    check_http_url(name, s)
}

/// 配置校验（`Config::validate` 调用）。
pub fn validate(cfg: &OidcConfig) -> anyhow::Result<()> {
    if !cfg.enabled {
        return Ok(());
    }
    if cfg.client_id.trim().is_empty() {
        anyhow::bail!("auth.oidc.client_id must not be empty");
    }
    if cfg.audience.as_deref().is_some_and(|s| s.trim().is_empty()) {
        anyhow::bail!("auth.oidc.audience must not be empty when set");
    }
    let needs_introspection = matches!(cfg.mode, OidcMode::Introspection | OidcMode::Hybrid);
    let needs_jwt = matches!(cfg.mode, OidcMode::Jwt | OidcMode::Hybrid) || cfg.browser_login;
    if needs_introspection {
        check_http_url("auth.oidc.introspection_url", &cfg.introspection_url)?;
        if cfg.client_secret.is_empty() {
            anyhow::bail!("auth.oidc.client_secret must not be empty for introspection");
        }
    }
    if needs_jwt {
        require_url("auth.oidc.issuer", &cfg.issuer)?;
        require_url("auth.oidc.jwks_uri", &cfg.jwks_uri)?;
    }
    if cfg.browser_login {
        require_url(
            "auth.oidc.authorization_endpoint",
            &cfg.authorization_endpoint,
        )?;
        require_url("auth.oidc.token_endpoint", &cfg.token_endpoint)?;
        require_url("auth.oidc.redirect_uri", &cfg.redirect_uri)?;
        if cfg.redirect_uri.as_deref().is_some_and(|u| u.contains('#')) {
            anyhow::bail!("auth.oidc.redirect_uri must not contain a fragment");
        }
        if !cfg.scopes.split_whitespace().any(|s| s == "openid") {
            anyhow::bail!("auth.oidc.scopes must include `openid` for browser login");
        }
        let path = cfg.post_login_path.trim();
        if !path.starts_with('/')
            || path.starts_with("//")
            || path.contains('#')
            || path.chars().any(|c| c.is_control() || c.is_whitespace())
        {
            anyhow::bail!("auth.oidc.post_login_path must be a site-relative path");
        }
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
    if !(1..=86_400).contains(&cfg.jwks_cache_seconds) {
        anyhow::bail!("auth.oidc.jwks_cache_seconds must be 1-86400");
    }
    if cfg.leeway_seconds > 300 {
        anyhow::bail!("auth.oidc.leeway_seconds must be 0-300");
    }
    if !(1..=86_400).contains(&cfg.session_seconds) {
        anyhow::bail!("auth.oidc.session_seconds must be 1-86400");
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
    fn pkce_rfc_vector() {
        assert_eq!(
            pkce_challenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }

    #[test]
    fn jwt_split_rejects_none_and_accepts_rs256() {
        let h = B64.encode(br#"{"alg":"none","typ":"JWT"}"#);
        let p = B64.encode(br#"{"sub":"u"}"#);
        assert!(split_jwt(&format!("{h}.{p}.")).is_err());
        let h = B64.encode(br#"{"alg":"RS256","kid":"k1"}"#);
        let p = B64.encode(br#"{"sub":"u","iss":"https://idp"}"#);
        let parts = split_jwt(&format!("{h}.{p}.c2ln")).unwrap();
        assert_eq!(parts.kid.as_deref(), Some("k1"));
        assert_eq!(parts.signature, b"sig");
    }

    #[test]
    fn jwt_claims_validate_iss_aud_exp_nonce() {
        let mut c = cfg();
        c.issuer = Some("https://idp".to_string());
        c.client_id = "client".to_string();
        c.namespace_claim = Some("namespace".to_string());
        c.audience = Some("agentlink-api".to_string());
        let now = 1_700_000_000;
        let ok = json!({
            "iss": "https://idp", "aud": "agentlink-api", "exp": now + 60,
            "sub": "u", "role": "ops", "namespace": "team-a", "nonce": "n"
        });
        let (r, exp) = validate_jwt_claims(&c, &ok, Some("n"), now).unwrap();
        assert_eq!(r.role, Role::Operator);
        assert_eq!(r.namespace.as_deref(), Some("team-a"));
        assert_eq!(exp, now + 60);
        assert!(validate_jwt_claims(&c, &ok, Some("other"), now).is_err());
        let mut bad = ok.clone();
        bad["exp"] = json!(now - 120);
        assert!(validate_jwt_claims(&c, &bad, Some("n"), now).is_err());
        bad = ok.clone();
        bad["aud"] = json!("other");
        assert!(validate_jwt_claims(&c, &bad, Some("n"), now).is_err());
        bad = ok.clone();
        bad["iss"] = json!("https://evil");
        assert!(validate_jwt_claims(&c, &bad, Some("n"), now).is_err());
        bad = ok.clone();
        bad["nbf"] = json!("soon");
        assert!(validate_jwt_claims(&c, &bad, Some("n"), now).is_err());
        bad = ok;
        bad["azp"] = json!("other");
        assert!(validate_jwt_claims(&c, &bad, Some("n"), now).is_err());
    }

    #[test]
    fn jwks_parses_rsa_and_skips_ec() {
        // n 用 256 字节占位（只测解析过滤，不构建公钥）。
        let n = B64.encode(vec![1u8; 256]);
        let e = B64.encode([1u8, 0, 1]);
        let v = json!({"keys":[
            {"kty":"EC","crv":"P-256","kid":"ec"},
            {"kty":"RSA","kid":"rsa","use":"sig","alg":"RS256","n":n,"e":e},
            {"kty":"RSA","kid":"enc","use":"enc","n":n,"e":e}
        ]});
        let keys = parse_jwks(&v).unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].kid.as_deref(), Some("rsa"));
        let mut cache = JwksCache::default();
        cache.set(&keys).unwrap();
        assert_eq!(cache.len(), 1);
        assert!(cache.fresh(60));
    }

    #[test]
    fn login_store_one_shot_and_expiry() {
        let mut s = LoginStore::default();
        s.put(
            "st".to_string(),
            "v".to_string(),
            "n".to_string(),
            "r".to_string(),
        );
        assert_eq!(s.len(), 1);
        let p = s.take("st").unwrap();
        assert_eq!(p.verifier, "v");
        assert!(s.take("st").is_none(), "state 只能消费一次");
        s.put(
            "st2".to_string(),
            "v".to_string(),
            "n".to_string(),
            "r".to_string(),
        );
        std::thread::sleep(Duration::from_millis(20));
        assert!(s.take("st2").is_some());
    }

    #[test]
    fn sessions_expire_and_revoke() {
        let mut s = SessionStore::default();
        let t = s.issue(
            Role::Operator,
            Some("team-a".to_string()),
            Duration::from_millis(50),
            "sub".to_string(),
        );
        assert!(t.starts_with("al_sess_"));
        assert_eq!(s.get(&t).unwrap().role, Role::Operator);
        assert!(s.remove(&t).is_some());
        assert!(s.get(&t).is_none());
        let t = s.issue(
            Role::Viewer,
            None,
            Duration::from_millis(30),
            "sub".to_string(),
        );
        std::thread::sleep(Duration::from_millis(50));
        assert!(s.get(&t).is_none());
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn validate_requires_url_and_credentials() {
        let mut c = cfg();
        assert!(validate(&c).is_err(), "enabled 时必填 URL/凭据");
        c.introspection_url = "http://127.0.0.1:1/introspect".to_string();
        c.client_id = "id".to_string();
        c.client_secret = "secret".to_string();
        assert!(validate(&c).is_ok());
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

    #[test]
    fn validate_jwt_and_browser_modes() {
        let mut c = cfg();
        c.mode = OidcMode::Jwt;
        c.client_id = "id".to_string();
        c.issuer = Some("https://idp.example.com".to_string());
        c.jwks_uri = Some("https://idp.example.com/jwks".to_string());
        assert!(validate(&c).is_ok());
        c.browser_login = true;
        assert!(validate(&c).is_err(), "浏览器登录缺端点");
        c.authorization_endpoint = Some("https://idp.example.com/authorize".to_string());
        c.token_endpoint = Some("https://idp.example.com/token".to_string());
        c.redirect_uri = Some("http://127.0.0.1:8765/v1/auth/oidc/callback".to_string());
        assert!(validate(&c).is_ok());
        c.scopes = "profile email".to_string();
        assert!(validate(&c).is_err(), "scope 必须含 openid");
        c.scopes = "openid".to_string();
        c.post_login_path = "//evil".to_string();
        assert!(validate(&c).is_err());
        c.post_login_path = "/web".to_string();
        c.mode = OidcMode::Hybrid;
        assert!(validate(&c).is_err(), "hybrid 需要 introspection/secret");
        c.introspection_url = "https://idp.example.com/introspect".to_string();
        c.client_secret = "secret".to_string();
        assert!(validate(&c).is_ok());
    }
}
