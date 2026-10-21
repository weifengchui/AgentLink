//! OIDC SSO（v0.7 §4.2）：token introspection（第一波）+ JWT/JWKS 本地验签 /
//! Authorization Code + PKCE 浏览器登录（第二波）+ IdP 兼容硬化（第三波）。
//!
//! 两种 API token 验证路径：
//! - `introspection`（缺省）：RFC 7662，兼容 opaque/JWT access token；
//! - `jwt`：JWT 本地验签（JWKS 缓存），IdP 短时故障不影响已缓存公钥；
//! - `hybrid`：JWT 优先，非 JWT/验签失败再走 introspection（fail-closed）。
//!
//! 浏览器登录只走 Authorization Code + PKCE（S256），ID token 必须本地验签
//! （iss/aud/exp/nbf/nonce），成功后签发**本节点短期 opaque session
//! token**，不把 IdP access/ID token 交给 Web UI。
//!
//! 第三波（IdP 兼容硬化）：
//! - 签名算法白名单 `auth.oidc.allowed_algs`（缺省 `["RS256"]`）：支持
//!   RS256 / PS256 / ES256(ECDSA P-256) / EdDSA(Ed25519)，**算法与 JWK 类型
//!   必须匹配**（杜绝 alg confusion），`none`/`HS*` 永不支持；
//! - `at_hash` / `c_hash` 校验（OIDC Core §3.3.2.11 / §3.2.2.9）；
//! - 回调 `response_mode=form_post`（默认仍是 query）；
//! - RP-initiated logout（`end_session_endpoint` + `id_token_hint`）；
//! - refresh token 续期（refresh token 只留在服务端内存，不进浏览器）。
//!
//! 依赖保持纯 Rust：`rsa`/`p256`/`ed25519-compact`/`sha2`/`base64`/`rand`，
//! 无 C/openssl。缺省 `auth.oidc.enabled=false`、静态 token/旧配置行为不变。

use crate::config::Role;
use base64::Engine;
use ed25519_compact::PublicKey as Ed25519PublicKey;
use p256::ecdsa::signature::Verifier;
use p256::ecdsa::{Signature as EcdsaSignature, VerifyingKey as EcdsaVerifyingKey};
use rand::RngCore;
use rsa::{BigUint, Pkcs1v15Sign, Pss, RsaPublicKey};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256, Sha512};
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

const B64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::URL_SAFE_NO_PAD;
const B64_PAD: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::URL_SAFE;
const LOGIN_TTL: Duration = Duration::from_secs(300);
const MAX_LOGINS: usize = 1024;
const MAX_SESSIONS: usize = 4096;
/// session 过期后的**续期宽限窗**：期间仍可用 refresh token 换新 session
/// （浏览器 401 -> 静默续期 -> 重试）。宽限窗外才真正丢弃。
const REFRESH_GRACE: Duration = Duration::from_secs(900);

/// v0.8 M4：复制登录 state 的存活时间（毫秒，跨节点 clock 用绝对墙钟）。
pub(crate) const LOGIN_TTL_MS: i64 = LOGIN_TTL.as_millis() as i64;
/// v0.8 M4：复制 session 的续期宽限窗（毫秒）。
pub(crate) const REFRESH_GRACE_MS: i64 = REFRESH_GRACE.as_millis() as i64;

/// API token 验证模式。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OidcMode {
    /// RFC 7662 introspection（缺省，兼容 opaque token）。
    #[default]
    Introspection,
    /// 仅 JWT 本地验签（JWKS）。
    Jwt,
    /// JWT 本地验签优先，非 JWT 或失败再走 introspection。
    Hybrid,
}

/// 浏览器登录回调的响应模式。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseMode {
    /// 授权码走 query string（缺省，兼容所有 IdP）。
    #[default]
    Query,
    /// 授权码走 `application/x-www-form-urlencoded` 表单回传（`form_post`）。
    FormPost,
}

/// 支持的签名算法（`auth.oidc.allowed_algs` 白名单取值）。
pub const SUPPORTED_ALGS: [&str; 4] = ["RS256", "PS256", "ES256", "EdDSA"];

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
    /// 允许的 JWT 签名算法白名单（缺省 `["RS256"]`，第二波行为不变）。
    /// 可选：`RS256` / `PS256` / `ES256` / `EdDSA`；`none`/`HS*` 永不支持。
    /// 算法必须与 JWKS 中 JWK 的类型匹配（RSA→RS256/PS256、EC P-256→ES256、
    /// OKP Ed25519→EdDSA），杜绝 alg confusion。
    #[serde(default = "default_allowed_algs")]
    pub allowed_algs: Vec<String>,
    /// 浏览器登录回调响应模式：query（缺省）/ form_post。
    #[serde(default)]
    pub response_mode: ResponseMode,
    /// IdP 登出端点（RP-initiated logout）。配置后「退出 SSO」会 302 到该
    /// 端点并带 `id_token_hint`，让 IdP 侧会话一并失效。
    #[serde(default)]
    pub end_session_endpoint: Option<String>,
    /// 登出后回跳地址：https URL 或站内绝对路径（缺省不回跳）。
    #[serde(default)]
    pub post_logout_redirect_uri: Option<String>,
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

fn default_allowed_algs() -> Vec<String> {
    vec!["RS256".to_string()]
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
            allowed_algs: default_allowed_algs(),
            response_mode: ResponseMode::default(),
            end_session_endpoint: None,
            post_logout_redirect_uri: None,
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
    /// JWT header 的 `alg`（未校验白名单，由调用方 `check_alg` 判定）。
    pub alg: String,
    pub claims: Value,
    pub signing_input: String,
    pub signature: Vec<u8>,
}

fn b64_decode(s: &str) -> Result<Vec<u8>, String> {
    B64.decode(s)
        .or_else(|_| B64_PAD.decode(s))
        .map_err(|e| format!("base64url: {e}"))
}

/// 拆分/解码 JWT。`alg` 只做基本排除（`none`/空），**是否允许由调用方
/// 按 `auth.oidc.allowed_algs` 判定**（`none`/`HS*` 永不支持）。
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
    if alg.is_empty() || alg.eq_ignore_ascii_case("none") {
        return Err("jwt: alg none is not allowed".to_string());
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
        alg,
        claims,
        signing_input: format!("{h}.{p}"),
        signature,
    })
}

/// 判断 JWT 的 `alg` 是否在配置白名单内（fail-closed）。
pub fn check_alg(cfg: &OidcConfig, alg: &str) -> Result<(), String> {
    if cfg.allowed_algs.iter().any(|a| a == alg) {
        return Ok(());
    }
    Err(format!(
        "jwt: alg {alg:?} is not in auth.oidc.allowed_algs ({})",
        cfg.allowed_algs.join(",")
    ))
}

/// **不验签**地读取 JWT payload 里的字符串 claim（只用于 `at_hash` 这类
/// 预校验；最终结论仍以验签后的 claim 为准）。
pub fn peek_claim(token: &str, claim: &str) -> Option<String> {
    let payload = token.split('.').nth(1)?;
    let bytes = b64_decode(payload).ok()?;
    let value: Value = serde_json::from_slice(&bytes).ok()?;
    value.get(claim)?.as_str().map(str::to_string)
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

/// 签名公钥（按 JWK 类型）。
#[derive(Debug, Clone)]
pub enum SigningKey {
    /// RSA（RS256 / PS256）。
    Rsa(RsaPublicKey),
    /// ECDSA P-256（ES256）。
    EcdsaP256(EcdsaVerifyingKey),
    /// Ed25519（EdDSA）。
    Ed25519(Ed25519PublicKey),
}

/// JWK 密钥类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyType {
    Rsa,
    EcdsaP256,
    Ed25519,
}

/// JWKS 中的一把验签公钥。
#[derive(Debug, Clone)]
pub struct Jwk {
    pub kid: Option<String>,
    pub kty: KeyType,
    /// JWK 自声明的 `alg`（缺省则按 kty 推断）。
    pub alg: Option<String>,
    pub key: SigningKey,
}

impl Jwk {
    /// 该 key 是否可用于给定算法——**算法与密钥类型必须匹配**（防 alg confusion）。
    pub fn accepts_alg(&self, alg: &str) -> bool {
        match self.alg.as_deref() {
            Some(a) => a == alg,
            None => match self.kty {
                KeyType::Rsa => alg == "RS256" || alg == "PS256",
                KeyType::EcdsaP256 => alg == "ES256",
                KeyType::Ed25519 => alg == "EdDSA",
            },
        }
    }
}

/// 从 JWKS JSON 解析出一把可验签公钥。
///
/// 材料非法（弱 RSA、坐标不在曲线上、长度不对）的 key **跳过**（不拖垮整份
/// JWKS）；整份 JWKS 一把可用 key 都没有时由 `parse_jwks` 报错。
fn build_jwk(k: &Value) -> Result<Option<Jwk>, String> {
    let kty = k.get("kty").and_then(|x| x.as_str()).unwrap_or("");
    let kid = k
        .get("kid")
        .and_then(|x| x.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    let alg = k
        .get("alg")
        .and_then(|x| x.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    match kty {
        "RSA" => {
            let n = match k.get("n").and_then(|x| x.as_str()) {
                Some(s) => b64_decode(s)?,
                None => return Ok(None),
            };
            let e = match k.get("e").and_then(|x| x.as_str()) {
                Some(s) => b64_decode(s)?,
                None => return Ok(None),
            };
            // 现代 IdP 至少 2048-bit；拒绝弱密钥（< 256 字节模长）。
            if n.len() < 256 {
                return Ok(None);
            }
            let key =
                match RsaPublicKey::new(BigUint::from_bytes_be(&n), BigUint::from_bytes_be(&e)) {
                    Ok(k) => k,
                    Err(_) => return Ok(None),
                };
            Ok(Some(Jwk {
                kid,
                kty: KeyType::Rsa,
                alg,
                key: SigningKey::Rsa(key),
            }))
        }
        "EC" => {
            let crv = k.get("crv").and_then(|x| x.as_str()).unwrap_or("");
            if crv != "P-256" {
                return Ok(None);
            }
            let x = match k.get("x").and_then(|x| x.as_str()) {
                Some(s) => b64_decode(s)?,
                None => return Ok(None),
            };
            let y = match k.get("y").and_then(|x| x.as_str()) {
                Some(s) => b64_decode(s)?,
                None => return Ok(None),
            };
            if x.len() != 32 || y.len() != 32 {
                return Ok(None);
            }
            let mut sec1 = Vec::with_capacity(65);
            sec1.push(0x04u8);
            sec1.extend_from_slice(&x);
            sec1.extend_from_slice(&y);
            let key = match EcdsaVerifyingKey::from_sec1_bytes(&sec1) {
                Ok(k) => k,
                Err(_) => return Ok(None),
            };
            Ok(Some(Jwk {
                kid,
                kty: KeyType::EcdsaP256,
                alg,
                key: SigningKey::EcdsaP256(key),
            }))
        }
        "OKP" => {
            let crv = k.get("crv").and_then(|x| x.as_str()).unwrap_or("");
            if crv != "Ed25519" {
                return Ok(None);
            }
            let x = match k.get("x").and_then(|x| x.as_str()) {
                Some(s) => b64_decode(s)?,
                None => return Ok(None),
            };
            if x.len() != 32 {
                return Ok(None);
            }
            let key = match Ed25519PublicKey::from_slice(&x) {
                Ok(k) => k,
                Err(_) => return Ok(None),
            };
            Ok(Some(Jwk {
                kid,
                kty: KeyType::Ed25519,
                alg,
                key: SigningKey::Ed25519(key),
            }))
        }
        _ => Ok(None),
    }
}

/// 解析 JWKS：只保留 `use=sig`（或未声明）且密钥材料合法的 key。
/// `alg` 缺省时按 kty 推断；显式声明的 `alg` 只允许受支持的算法。
pub fn parse_jwks(v: &Value) -> Result<Vec<Jwk>, String> {
    let keys = v
        .get("keys")
        .and_then(|k| k.as_array())
        .ok_or("jwks: keys array missing")?;
    let mut out = Vec::new();
    for k in keys {
        if let Some(use_) = k.get("use").and_then(|x| x.as_str()) {
            if use_ != "sig" {
                continue;
            }
        }
        if let Some(alg) = k.get("alg").and_then(|x| x.as_str()) {
            if !SUPPORTED_ALGS.contains(&alg) {
                continue;
            }
        }
        if let Some(jwk) = build_jwk(k)? {
            out.push(jwk);
        }
    }
    if out.is_empty() {
        return Err("jwks: no usable signing keys".to_string());
    }
    Ok(out)
}

/// JWKS 内存缓存（不保存私钥/原始 JWT）。
#[derive(Debug, Default)]
pub struct JwksCache {
    keys: Vec<Jwk>,
    fetched: Option<Instant>,
}

impl JwksCache {
    pub fn fresh(&self, ttl_seconds: u64) -> bool {
        self.fetched
            .is_some_and(|t| t.elapsed() < Duration::from_secs(ttl_seconds.clamp(1, 86_400)))
    }

    /// 按 kid（可空）+ 算法取 key。
    /// - 有 kid：必须存在**且类型匹配**该算法；
    /// - 无 kid：只有唯一一把匹配该算法的 key 才可安全选择。
    pub fn get(&self, kid: Option<&str>, alg: &str) -> Option<Jwk> {
        let matched = |j: &Jwk| j.accepts_alg(alg);
        match kid {
            Some(k) => self
                .keys
                .iter()
                .find(|j| j.kid.as_deref() == Some(k) && matched(j))
                .cloned(),
            None => {
                let mut it = self.keys.iter().filter(|j| matched(j));
                let first = it.next()?;
                if it.next().is_some() {
                    None
                } else {
                    Some(first.clone())
                }
            }
        }
    }

    pub fn set(&mut self, jwks: &[Jwk]) -> Result<(), String> {
        if jwks.is_empty() {
            return Err("jwks: no usable public keys".to_string());
        }
        self.keys = jwks.to_vec();
        self.fetched = Some(Instant::now());
        Ok(())
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.keys.len()
    }
}

/// 按算法验签（signing_input 原文，签名已 base64url 解码）。
/// 算法与密钥类型不匹配一律失败（fail-closed）。
pub fn verify_signature(
    alg: &str,
    key: &SigningKey,
    signing_input: &str,
    signature: &[u8],
) -> Result<(), String> {
    let msg = signing_input.as_bytes();
    match (alg, key) {
        ("RS256", SigningKey::Rsa(k)) => {
            let hashed = Sha256::digest(msg);
            k.verify(Pkcs1v15Sign::new::<Sha256>(), hashed.as_slice(), signature)
                .map_err(|_| "jwt: signature verification failed".to_string())
        }
        ("PS256", SigningKey::Rsa(k)) => {
            let hashed = Sha256::digest(msg);
            k.verify(Pss::new::<Sha256>(), hashed.as_slice(), signature)
                .map_err(|_| "jwt: signature verification failed".to_string())
        }
        ("ES256", SigningKey::EcdsaP256(k)) => {
            let sig = EcdsaSignature::from_slice(signature)
                .map_err(|_| "jwt: invalid ES256 signature encoding".to_string())?;
            k.verify(msg, &sig)
                .map_err(|_| "jwt: signature verification failed".to_string())
        }
        ("EdDSA", SigningKey::Ed25519(k)) => {
            let sig = ed25519_compact::Signature::from_slice(signature)
                .map_err(|_| "jwt: invalid EdDSA signature encoding".to_string())?;
            k.verify(msg, &sig)
                .map_err(|_| "jwt: signature verification failed".to_string())
        }
        _ => Err("jwt: alg/key type mismatch".to_string()),
    }
}

/// 签名算法对应的摘要算法（OIDC Core §3.3.2.11：RS/ES/PS→SHA-256，
/// EdDSA→SHA-512）。
fn digest_for_alg(alg: &str) -> Option<&'static str> {
    match alg {
        "RS256" | "PS256" | "ES256" => Some("sha-256"),
        "EdDSA" => Some("sha-512"),
        _ => None,
    }
}

/// `at_hash` / `c_hash` = 摘要左半部分（base64url，无 padding）。
pub fn token_hash(alg: &str, token: &str) -> Result<String, String> {
    match digest_for_alg(alg) {
        Some("sha-256") => {
            let d = Sha256::digest(token.as_bytes());
            Ok(B64.encode(&d[..d.len() / 2]))
        }
        Some("sha-512") => {
            let d = Sha512::digest(token.as_bytes());
            Ok(B64.encode(&d[..d.len() / 2]))
        }
        _ => Err(format!("jwt: unsupported alg {alg:?} for token hash")),
    }
}

/// 校验 `at_hash` / `c_hash`（claim 存在才校验；不匹配即拒绝）。
pub fn verify_token_hash(alg: &str, token: &str, expected: &str) -> Result<(), String> {
    let got = token_hash(alg, token)?;
    if got == expected {
        Ok(())
    } else {
        Err("jwt: token hash mismatch".to_string())
    }
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
///
/// `id_token` / `refresh_token` 只留在**服务端内存**（供 RP-initiated logout
/// 与 refresh 续期），永不下发给浏览器。
#[derive(Debug, Clone)]
pub struct Session {
    pub role: Role,
    pub namespace: Option<String>,
    pub sub: String,
    pub expires: Instant,
    /// IdP ID token（仅服务端；用于 `id_token_hint` 登出）。
    pub id_token: Option<String>,
    /// IdP refresh token（仅服务端；用于静默续期）。
    pub refresh_token: Option<String>,
}

/// 签发会话所需的参数。
#[derive(Debug, Clone)]
pub struct SessionIssue {
    pub role: Role,
    pub namespace: Option<String>,
    pub sub: String,
    pub ttl: Duration,
    pub id_token: Option<String>,
    pub refresh_token: Option<String>,
}

/// v0.8 M4：复制状态机里的登录 state（绝对毫秒过期，跨节点共享）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HaLogin {
    pub verifier: String,
    pub nonce: String,
    pub redirect_uri: String,
    pub created_ms: i64,
}

/// v0.8 M4：复制状态机里的浏览器 session（绝对毫秒过期）。
/// IdP token 只随状态机在集群内部复制，永不下发浏览器。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HaSession {
    pub role: Role,
    #[serde(default)]
    pub namespace: Option<String>,
    pub sub: String,
    pub expires_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
}

/// 复制 session 是否可用（未过期；`grace` 时允许续期宽限窗内过期）。
pub fn ha_session_live(s: &HaSession, now_ms: i64, grace: bool) -> bool {
    if grace {
        s.expires_ms + REFRESH_GRACE_MS > now_ms
    } else {
        s.expires_ms > now_ms
    }
}

#[derive(Debug, Default)]
pub struct SessionStore {
    entries: HashMap<String, Session>,
}

impl SessionStore {
    pub fn issue(&mut self, issue: SessionIssue) -> String {
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
                role: issue.role,
                namespace: issue.namespace,
                sub: issue.sub,
                expires: Instant::now() + issue.ttl,
                id_token: issue.id_token,
                refresh_token: issue.refresh_token,
            },
        );
        token
    }

    /// 取**未过期**的会话。过期会话不立即丢弃（留给 refresh 续期宽限窗），
    /// 由 `purge` 统一回收。
    pub fn get(&mut self, token: &str) -> Option<Session> {
        let now = Instant::now();
        self.entries.get(token).filter(|s| s.expires > now).cloned()
    }

    /// 取会话用于 **refresh 续期**：已过期但在宽限窗内也返回（续期成功后
    /// 原地延长）。超出宽限窗的会话直接丢弃。
    pub fn get_for_refresh(&mut self, token: &str) -> Option<Session> {
        let now = Instant::now();
        match self.entries.get(token) {
            Some(s) if s.expires + REFRESH_GRACE > now => Some(s.clone()),
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

    /// refresh 续期：原地延长会话有效期并更新 IdP 侧 token（浏览器 token 不变）。
    pub fn extend(
        &mut self,
        token: &str,
        ttl: Duration,
        id_token: Option<String>,
        refresh_token: Option<String>,
    ) -> bool {
        match self.entries.get_mut(token) {
            Some(s) => {
                s.expires = Instant::now() + ttl;
                if id_token.is_some() {
                    s.id_token = id_token;
                }
                if refresh_token.is_some() {
                    s.refresh_token = refresh_token;
                }
                true
            }
            None => false,
        }
    }

    fn purge(&mut self) {
        let now = Instant::now();
        self.entries.retain(|_, s| s.expires + REFRESH_GRACE > now);
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
    if cfg.allowed_algs.is_empty() {
        anyhow::bail!("auth.oidc.allowed_algs must not be empty");
    }
    for alg in &cfg.allowed_algs {
        if !SUPPORTED_ALGS.contains(&alg.as_str()) {
            anyhow::bail!(
                "auth.oidc.allowed_algs: {alg:?} is not supported (RS256/PS256/ES256/EdDSA)"
            );
        }
    }
    if cfg.response_mode == ResponseMode::FormPost && !cfg.browser_login {
        anyhow::bail!("auth.oidc.response_mode=form_post requires browser_login");
    }
    if let Some(ep) = cfg.end_session_endpoint.as_deref() {
        if ep.trim().is_empty() {
            anyhow::bail!("auth.oidc.end_session_endpoint must not be empty when set");
        }
        check_http_url("auth.oidc.end_session_endpoint", ep)?;
    }
    if let Some(u) = cfg.post_logout_redirect_uri.as_deref() {
        if u.trim().is_empty() {
            anyhow::bail!("auth.oidc.post_logout_redirect_uri must not be empty when set");
        }
        if u.starts_with('/') {
            if u.starts_with("//") || u.contains('#') {
                anyhow::bail!(
                    "auth.oidc.post_logout_redirect_uri must be a site-relative path or https URL"
                );
            }
        } else {
            check_http_url("auth.oidc.post_logout_redirect_uri", u)?;
        }
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
    fn jwt_split_rejects_none_and_reports_alg() {
        let h = B64.encode(br#"{"alg":"none","typ":"JWT"}"#);
        let p = B64.encode(br#"{"sub":"u"}"#);
        assert!(split_jwt(&format!("{h}.{p}.")).is_err());
        let h = B64.encode(br#"{"alg":"HS256","kid":"k1"}"#);
        let p = B64.encode(br#"{"sub":"u","iss":"https://idp"}"#);
        assert!(
            split_jwt(&format!("{h}.{p}.c2ln")).is_ok(),
            "拆分不拦 HS*，由白名单拦"
        );
        let h = B64.encode(br#"{"alg":"RS256","kid":"k1"}"#);
        let parts = split_jwt(&format!("{h}.{p}.c2ln")).unwrap();
        assert_eq!(parts.kid.as_deref(), Some("k1"));
        assert_eq!(parts.alg, "RS256");
        assert_eq!(parts.signature, b"sig");
        // crit 头一律拒绝
        let h = B64.encode(br#"{"alg":"RS256","crit":["x"]}"#);
        assert!(split_jwt(&format!("{h}.{p}.c2ln")).is_err());
    }

    #[test]
    fn allowed_algs_gate_keeps_rs256_default() {
        let c = cfg();
        assert_eq!(c.allowed_algs, vec!["RS256".to_string()]);
        assert!(check_alg(&c, "RS256").is_ok());
        assert!(check_alg(&c, "ES256").is_err());
        assert!(check_alg(&c, "none").is_err());
        assert!(check_alg(&c, "HS256").is_err());
        let mut c2 = c.clone();
        c2.allowed_algs = vec!["RS256".to_string(), "ES256".to_string()];
        assert!(check_alg(&c2, "ES256").is_ok());
    }

    #[test]
    fn jwk_alg_must_match_key_type() {
        // RSA key 不接受 ES256；EC key 不接受 RS256（防 alg confusion）。
        let n = B64.encode(vec![1u8; 256]);
        let e = B64.encode([1u8, 0, 1]);
        let v = json!({"keys":[
            {"kty":"RSA","kid":"rsa","use":"sig","alg":"RS256","n":n,"e":e}
        ]});
        let keys = parse_jwks(&v).unwrap();
        assert_eq!(keys.len(), 1);
        assert!(keys[0].accepts_alg("RS256"));
        assert!(
            !keys[0].accepts_alg("PS256"),
            "JWK 显式声明 RS256 时不接受 PS256"
        );
        assert!(!keys[0].accepts_alg("ES256"));
    }

    #[test]
    fn jwks_parses_rsa_ec_and_okp_keys() {
        // RSA（占位模长，只测解析/过滤）
        let n = B64.encode(vec![1u8; 256]);
        let e = B64.encode([1u8, 0, 1]);
        // EC P-256：真实坐标（secp256r1 上的有效点）
        let (x, y) = {
            use p256::ecdsa::SigningKey;
            use p256::elliptic_curve::sec1::ToEncodedPoint;
            let sk = SigningKey::from_slice(&[7u8; 32]).unwrap();
            let pt = sk.verifying_key().to_encoded_point(false);
            let x = pt.x().unwrap().to_vec();
            let y = pt.y().unwrap().to_vec();
            (x, y)
        };
        let ec = json!({
            "kty":"EC","crv":"P-256","kid":"ec1","use":"sig","alg":"ES256",
            "x":B64.encode(x),"y":B64.encode(y)
        });
        let okp = json!({
            "kty":"OKP","crv":"Ed25519","kid":"ed1","use":"sig",
            "x":B64.encode([3u8; 32])
        });
        let v = json!({"keys":[
            {"kty":"EC","crv":"P-384","kid":"p384"},
            {"kty":"RSA","kid":"enc","use":"enc","n":n,"e":e},
            ec, okp,
            {"kty":"RSA","kid":"rsa","use":"sig","alg":"RS256","n":n,"e":e}
        ]});
        let keys = parse_jwks(&v).unwrap();
        let kids: Vec<&str> = keys.iter().filter_map(|k| k.kid.as_deref()).collect();
        assert!(kids.contains(&"ec1"));
        assert!(kids.contains(&"ed1"));
        assert!(kids.contains(&"rsa"));
        assert!(!kids.contains(&"p384"), "P-384 不支持");
        assert!(!kids.contains(&"enc"), "use=enc 跳过");
        let mut cache = JwksCache::default();
        cache.set(&keys).unwrap();
        assert_eq!(cache.len(), 3);
        assert!(cache.fresh(60));
        // kid 命中且算法匹配
        assert!(cache.get(Some("ec1"), "ES256").is_some());
        assert!(cache.get(Some("ec1"), "RS256").is_none());
        assert!(cache.get(Some("ed1"), "EdDSA").is_some());
        // 未知 kid -> None（触发刷新）
        assert!(cache.get(Some("nope"), "RS256").is_none());
    }

    #[test]
    fn verifies_rs256_ps256_es256_eddsa() {
        let msg = "header.payload";
        // RS256
        let rsa = rsa::RsaPrivateKey::new(&mut rand::rngs::OsRng, 2048).unwrap();
        let rsa_pub = rsa::RsaPublicKey::from(&rsa);
        let hashed = Sha256::digest(msg.as_bytes());
        let sig = rsa.sign(Pkcs1v15Sign::new::<Sha256>(), &hashed).unwrap();
        let key = SigningKey::Rsa(rsa_pub.clone());
        assert!(verify_signature("RS256", &key, msg, &sig).is_ok());
        // 用错算法（PS256 对同一签名）必须失败
        assert!(verify_signature("PS256", &key, msg, &sig).is_err());
        // PS256
        let pss_sk = rsa::pss::SigningKey::<Sha256>::new(rsa.clone());
        let pss = rsa::signature::RandomizedSigner::sign_with_rng(
            &pss_sk,
            &mut rand::rngs::OsRng,
            msg.as_bytes(),
        );
        let pss_bytes = rsa::signature::SignatureEncoding::to_bytes(&pss);
        assert!(verify_signature("PS256", &key, msg, &pss_bytes).is_ok());
        // ES256
        let sk = p256::ecdsa::SigningKey::from_slice(&[9u8; 32]).unwrap();
        let es: p256::ecdsa::Signature = p256::ecdsa::signature::Signer::sign(&sk, msg.as_bytes());
        let es_key = SigningKey::EcdsaP256(*sk.verifying_key());
        assert!(verify_signature("ES256", &es_key, msg, &es.to_bytes()).is_ok());
        // EdDSA
        let kp = ed25519_compact::KeyPair::generate();
        let ed = kp.sk.sign(msg.as_bytes(), None);
        let ed_key = SigningKey::Ed25519(kp.pk);
        assert!(verify_signature("EdDSA", &ed_key, msg, ed.as_ref()).is_ok());
        // 算法与 key 类型不匹配 -> 失败
        assert!(verify_signature("ES256", &key, msg, &es.to_bytes()).is_err());
        assert!(verify_signature("EdDSA", &es_key, msg, ed.as_ref()).is_err());
        // 篡改签名 -> 失败
        let mut bad = es.to_bytes();
        bad[0] ^= 0xff;
        assert!(verify_signature("ES256", &es_key, msg, &bad).is_err());
    }

    #[test]
    fn token_hash_matches_oidc_spec() {
        // OIDC Core §3.3.2.11 示例：RS256 -> base64url(左半 SHA-256)
        assert_eq!(
            token_hash("RS256", "jHkWEdUXMU1BwAsC4vtUsZwnNvTIxEl0z9K3vx5KF0Y").unwrap(),
            "77QmUPtjPfzWtF2AnpK9RQ"
        );
        // EdDSA -> 左半 SHA-512
        let d = Sha512::digest(b"abc");
        let expect = B64.encode(&d[..d.len() / 2]);
        assert_eq!(token_hash("EdDSA", "abc").unwrap(), expect);
        // 不匹配的 hash 必须报错
        assert!(verify_token_hash("RS256", "abc", "wrong").is_err());
        assert!(verify_token_hash("RS256", "abc", &token_hash("RS256", "abc").unwrap()).is_ok());
        // 不支持的算法
        assert!(token_hash("HS256", "abc").is_err());
    }

    #[test]
    fn peek_claim_reads_payload_without_verification() {
        let h = B64.encode(br#"{"alg":"RS256"}"#);
        let p = B64.encode(br#"{"sub":"u1","at_hash":"abc"}"#);
        let tok = format!("{h}.{p}.c2ln");
        assert_eq!(peek_claim(&tok, "at_hash").as_deref(), Some("abc"));
        assert_eq!(peek_claim(&tok, "sub").as_deref(), Some("u1"));
        assert!(peek_claim(&tok, "nope").is_none());
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
        let t = s.issue(SessionIssue {
            role: Role::Operator,
            namespace: Some("team-a".to_string()),
            sub: "sub".to_string(),
            ttl: Duration::from_millis(50),
            id_token: Some("id-token".to_string()),
            refresh_token: Some("refresh-token".to_string()),
        });
        assert!(t.starts_with("al_sess_"));
        let got = s.get(&t).unwrap();
        assert_eq!(got.role, Role::Operator);
        assert_eq!(got.id_token.as_deref(), Some("id-token"));
        assert_eq!(got.refresh_token.as_deref(), Some("refresh-token"));
        assert!(s.remove(&t).is_some());
        assert!(s.get(&t).is_none());
        let t = s.issue(SessionIssue {
            role: Role::Viewer,
            namespace: None,
            sub: "sub".to_string(),
            ttl: Duration::from_millis(30),
            id_token: None,
            refresh_token: None,
        });
        std::thread::sleep(Duration::from_millis(50));
        assert!(s.get(&t).is_none(), "过期会话不可用于鉴权");
        assert!(s.get_for_refresh(&t).is_some(), "宽限窗内仍可 refresh 续期");
        assert_eq!(s.len(), 1, "过期会话在宽限窗内暂不丢弃");
    }

    #[test]
    fn session_extend_keeps_token_and_updates_idp_tokens() {
        let mut s = SessionStore::default();
        let t = s.issue(SessionIssue {
            role: Role::Operator,
            namespace: Some("team-a".to_string()),
            sub: "sub".to_string(),
            ttl: Duration::from_millis(30),
            id_token: Some("old-id".to_string()),
            refresh_token: Some("old-refresh".to_string()),
        });
        std::thread::sleep(Duration::from_millis(50));
        assert!(s.get(&t).is_none(), "过期会话不可续期");
        let t2 = s.issue(SessionIssue {
            role: Role::Operator,
            namespace: Some("team-a".to_string()),
            sub: "sub".to_string(),
            ttl: Duration::from_millis(30),
            id_token: Some("old-id".to_string()),
            refresh_token: Some("old-refresh".to_string()),
        });
        assert!(s.extend(
            &t2,
            Duration::from_secs(600),
            Some("new-id".to_string()),
            Some("new-refresh".to_string())
        ));
        let got = s.get(&t2).unwrap();
        assert_eq!(got.id_token.as_deref(), Some("new-id"));
        assert_eq!(got.refresh_token.as_deref(), Some("new-refresh"));
        assert!(!s.extend("al_sess_unknown", Duration::from_secs(1), None, None));
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

    #[test]
    fn validate_third_wave_fields() {
        let mut c = cfg();
        c.mode = OidcMode::Jwt;
        c.client_id = "id".to_string();
        c.issuer = Some("https://idp.example.com".to_string());
        c.jwks_uri = Some("https://idp.example.com/jwks".to_string());
        // 算法白名单
        c.allowed_algs = vec!["RS256".to_string(), "ES256".to_string()];
        assert!(validate(&c).is_ok());
        c.allowed_algs = vec!["none".to_string()];
        assert!(validate(&c).is_err(), "none 不允许");
        c.allowed_algs = vec!["HS256".to_string()];
        assert!(validate(&c).is_err(), "HS* 不允许");
        c.allowed_algs = vec![];
        assert!(validate(&c).is_err(), "白名单不能为空");
        c.allowed_algs = vec!["RS256".to_string()];
        // form_post 需要浏览器登录
        c.response_mode = ResponseMode::FormPost;
        assert!(validate(&c).is_err());
        c.browser_login = true;
        c.authorization_endpoint = Some("https://idp.example.com/authorize".to_string());
        c.token_endpoint = Some("https://idp.example.com/token".to_string());
        c.redirect_uri = Some("https://al.example.com/v1/auth/oidc/callback".to_string());
        assert!(validate(&c).is_ok());
        // 登出端点
        c.end_session_endpoint = Some("https://idp.example.com/logout".to_string());
        c.post_logout_redirect_uri = Some("/web".to_string());
        assert!(validate(&c).is_ok());
        c.post_logout_redirect_uri = Some("//evil.example.com".to_string());
        assert!(validate(&c).is_err(), "回跳地址不能是 //host");
        c.post_logout_redirect_uri = Some("https://al.example.com/web".to_string());
        assert!(validate(&c).is_ok());
        c.end_session_endpoint = Some("http://idp.example.com/logout".to_string());
        assert!(validate(&c).is_err(), "非回环 http 拒绝");
        c.end_session_endpoint = Some("  ".to_string());
        assert!(validate(&c).is_err());
    }
}
