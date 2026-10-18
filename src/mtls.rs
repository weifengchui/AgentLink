//! mTLS 证书管理（v0.6 P1）。
//!
//! 内置 CA 一键签发本机节点证书，支持按 TTL 提前轮转。LAN 默认关闭，
//! `agentlink mtls --enable` 一键开启后，节点间连接使用双向 TLS 认证。
//!
//! 依赖：`rcgen`（证书生成）。纯 Rust、无 C 依赖，保持单二进制零运行时依赖。
//!
//! 存储（默认落在 `<data_dir>/mtls/`）：
//! - `ca.pem` / `ca.key`     —— 集群根 CA（自签）；CA 不变则全网信任不变
//! - `node.pem` / `node.key` —— 本机节点证书（由 CA 签发）+ 私钥
//! - `node.expiry`           —— 节点证书到期时间（Unix 秒），用于轮转判定
//!
//! 设计取舍：不引入 X.509 解析依赖。CA 仅作为「签发者」使用（issuer DN +
//! 私钥 + 签名算法），这三者都是本模块自持的常量与密钥，因此重签发节点证书
//! 无需回读解析 CA 证书；节点证书到期时间单独落 `node.expiry`。

#![allow(dead_code)] // P1 第一波：PEM 访问器/轮转判定为节点间 TLS 传输接线（第二波）预留

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DistinguishedName, DnType,
    ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose, PKCS_ECDSA_P256_SHA256,
};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;

use crate::config::MtlsConfig;

/// CA 证书的 Common Name（签发者 DN 与集群信任锚，改动即等价于换 CA）。
const CA_COMMON_NAME: &str = "AgentLink CA";
/// 节点证书的 Common Name 前缀。
const NODE_COMMON_NAME: &str = "AgentLink Node";
/// CA 有效期（天）：足够长，轮转只针对节点证书。
const CA_TTL_DAYS: i64 = 3650;

/// 证书文件路径集合（解析自 [`MtlsConfig`] 与 `data_dir`）。
#[derive(Debug, Clone)]
pub struct MtlsPaths {
    pub ca_cert: PathBuf,
    pub ca_key: PathBuf,
    pub node_cert: PathBuf,
    pub node_key: PathBuf,
    pub node_expiry: PathBuf,
}

impl MtlsPaths {
    /// 按配置解析路径：CA 用 `mtls.ca_cert`/`mtls.ca_key`（若给出），
    /// 否则与节点证书一样落在 `<data_dir>/mtls/`。
    pub fn resolve(config: &MtlsConfig, data_dir: &Path) -> Self {
        let dir = data_dir.join("mtls");
        Self {
            ca_cert: config.ca_cert.clone().unwrap_or_else(|| dir.join("ca.pem")),
            ca_key: config.ca_key.clone().unwrap_or_else(|| dir.join("ca.key")),
            node_cert: dir.join("node.pem"),
            node_key: dir.join("node.key"),
            node_expiry: dir.join("node.expiry"),
        }
    }
}

/// mTLS 证书管理器：持有 CA 与节点证书的 PEM 与到期信息。
pub struct MtlsManager {
    paths: MtlsPaths,
    ca_cert_pem: String,
    ca_key_pem: String,
    node_cert_pem: String,
    node_key_pem: String,
    /// 节点证书到期时间（Unix 秒）。
    node_expiry_unix: i64,
    /// 节点证书有效期（天）。
    cert_ttl_days: u32,
    /// 提前轮转窗口（秒）：距到期不足此值即重签。
    rotate_interval_seconds: u64,
}

impl MtlsManager {
    /// 加载或创建证书：四件套齐全则直接加载；节点证书缺失/临近到期则用
    /// 现有 CA 重签；CA 缺失则新生成整条链。
    pub fn load_or_create(config: &MtlsConfig, data_dir: &Path) -> Result<Self> {
        let paths = MtlsPaths::resolve(config, data_dir);
        if let Some(parent) = paths.node_cert.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create mtls dir {}", parent.display()))?;
        }

        let ttl = config.cert_ttl_days.max(1);
        let rotate = config.cert_rotate_interval_seconds;
        let now = OffsetDateTime::now_utc().unix_timestamp();

        // 1) CA：能读就用，读不到（缺失/损坏）才新生成。
        let (ca_cert_pem, ca_key_pem) = match read_ca(&paths) {
            Some(pair) => pair,
            None => {
                let (cert, key) = generate_ca()?;
                write_secret(&paths.ca_cert, cert.as_bytes())?;
                write_secret(&paths.ca_key, key.as_bytes())?;
                (cert, key)
            }
        };

        // 2) 节点证书：齐全且未到轮转窗口则复用，否则用 CA 重签。
        let ca_signer = ca_signer_from_pems(&ca_cert_pem, &ca_key_pem)?;
        let loaded = read_node(&paths);
        let reuse = match &loaded {
            Some((_, _, expiry)) => !needs_rotation_at(*expiry, rotate, now),
            None => false,
        };
        let (node_cert_pem, node_key_pem, node_expiry_unix) = if reuse {
            loaded.unwrap()
        } else {
            let (cert, key, expiry) = issue_node(&ca_signer, ttl)?;
            write_secret(&paths.node_cert, cert.as_bytes())?;
            write_secret(&paths.node_key, key.as_bytes())?;
            fs::write(&paths.node_expiry, expiry.to_string())
                .with_context(|| format!("failed to write {}", paths.node_expiry.display()))?;
            (cert, key, expiry)
        };

        Ok(Self {
            paths,
            ca_cert_pem,
            ca_key_pem,
            node_cert_pem,
            node_key_pem,
            node_expiry_unix,
            cert_ttl_days: ttl,
            rotate_interval_seconds: rotate,
        })
    }

    /// 证书文件路径集合。
    pub fn paths(&self) -> &MtlsPaths {
        &self.paths
    }

    /// CA 证书 PEM。
    pub fn ca_cert_pem(&self) -> &str {
        &self.ca_cert_pem
    }

    /// 节点证书 PEM（已由 CA 签发）。
    pub fn node_cert_pem(&self) -> &str {
        &self.node_cert_pem
    }

    /// 节点私钥 PEM（PKCS#8）。
    pub fn node_key_pem(&self) -> &str {
        &self.node_key_pem
    }

    /// 节点证书到期时间（Unix 秒）。
    pub fn node_expiry_unix(&self) -> i64 {
        self.node_expiry_unix
    }

    /// CA 证书 DER（用于计算指纹）。
    pub fn ca_cert_der(&self) -> Result<Vec<u8>> {
        pem_to_der(&self.ca_cert_pem)
    }

    /// 节点证书 DER（用于计算指纹）。
    pub fn node_cert_der(&self) -> Result<Vec<u8>> {
        pem_to_der(&self.node_cert_pem)
    }

    /// CA 证书 SHA-256 指纹（小写十六进制）。
    pub fn ca_fingerprint(&self) -> Result<String> {
        Ok(fingerprint(&self.ca_cert_der()?))
    }

    /// 节点证书 SHA-256 指纹（小写十六进制）。
    pub fn node_fingerprint(&self) -> Result<String> {
        Ok(fingerprint(&self.node_cert_der()?))
    }

    /// 节点证书是否已进入轮转窗口（距到期不足 `cert_rotate_interval_seconds`）。
    pub fn needs_rotation(&self) -> bool {
        needs_rotation_at(
            self.node_expiry_unix,
            self.rotate_interval_seconds,
            OffsetDateTime::now_utc().unix_timestamp(),
        )
    }

    /// 立即轮转节点证书：用现有 CA 重签，CA 与全网信任不变。
    pub fn rotate_node_cert(&mut self) -> Result<()> {
        let ca_signer = ca_signer_from_pems(&self.ca_cert_pem, &self.ca_key_pem)?;
        let (cert, key, expiry) = issue_node(&ca_signer, self.cert_ttl_days)?;
        write_secret(&self.paths.node_cert, cert.as_bytes())?;
        write_secret(&self.paths.node_key, key.as_bytes())?;
        fs::write(&self.paths.node_expiry, expiry.to_string())
            .with_context(|| format!("failed to write {}", self.paths.node_expiry.display()))?;
        self.node_cert_pem = cert;
        self.node_key_pem = key;
        self.node_expiry_unix = expiry;
        Ok(())
    }
}

/// 轮转判定：距到期不足 `rotate_interval_seconds`（或已过期）即需重签。
fn needs_rotation_at(expiry_unix: i64, rotate_interval_seconds: u64, now_unix: i64) -> bool {
    expiry_unix - now_unix <= rotate_interval_seconds as i64
}

/// 生成自签 CA，返回 `(ca_pem, ca_key_pem)`。
fn generate_ca() -> Result<(String, String)> {
    let key =
        KeyPair::generate(&PKCS_ECDSA_P256_SHA256).context("failed to generate CA key pair")?;
    let mut params = CertificateParams::default();
    params.alg = &PKCS_ECDSA_P256_SHA256;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.distinguished_name = distinguished_name(CA_COMMON_NAME);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let now = OffsetDateTime::now_utc();
    params.not_before = now - time::Duration::minutes(5);
    params.not_after = now + time::Duration::days(CA_TTL_DAYS);
    params.key_pair = Some(key);
    let cert = Certificate::from_params(params).context("failed to build CA certificate")?;
    let cert_pem = cert
        .serialize_pem()
        .context("failed to serialize CA cert")?;
    Ok((cert_pem, cert.serialize_private_key_pem()))
}

/// 用 CA 签发节点证书（ServerAuth + ClientAuth），返回
/// `(node_pem, node_key_pem, expiry_unix)`。
fn issue_node(ca: &Certificate, ttl_days: u32) -> Result<(String, String, i64)> {
    let key =
        KeyPair::generate(&PKCS_ECDSA_P256_SHA256).context("failed to generate node key pair")?;
    let mut params = CertificateParams::default();
    params.alg = &PKCS_ECDSA_P256_SHA256;
    params.is_ca = IsCa::NoCa;
    params.distinguished_name = distinguished_name(NODE_COMMON_NAME);
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let now = OffsetDateTime::now_utc();
    params.not_before = now - time::Duration::minutes(5);
    params.not_after = now + time::Duration::days(ttl_days as i64);
    let expiry_unix = params.not_after.unix_timestamp();
    params.key_pair = Some(key);
    let cert = Certificate::from_params(params).context("failed to build node certificate")?;
    let cert_pem = cert
        .serialize_pem_with_signer(ca)
        .context("failed to sign node certificate with CA")?;
    Ok((cert_pem, cert.serialize_private_key_pem(), expiry_unix))
}

/// 由 CA 的 PEM（issuer/算法来源）与私钥重建一个「签发者」证书对象。
///
/// 只用于 `serialize_pem_with_signer`：它只读取 CA 的 issuer DN、签名算法与
/// 私钥，因此无需反序列化原始 CA 证书（不引入 X.509 解析依赖）。
fn ca_signer_from_pems(_ca_cert_pem: &str, ca_key_pem: &str) -> Result<Certificate> {
    let key = KeyPair::from_pem(ca_key_pem).context("failed to parse CA private key")?;
    let mut params = CertificateParams::default();
    params.alg = &PKCS_ECDSA_P256_SHA256;
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.distinguished_name = distinguished_name(CA_COMMON_NAME);
    params.key_pair = Some(key);
    Certificate::from_params(params).context("failed to rebuild CA signer")
}

fn distinguished_name(cn: &str) -> DistinguishedName {
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, cn);
    dn
}

/// 读取 CA 证书与私钥；任一缺失或空则返回 `None`（触发重新生成）。
fn read_ca(paths: &MtlsPaths) -> Option<(String, String)> {
    let cert = fs::read_to_string(&paths.ca_cert).ok()?;
    let key = fs::read_to_string(&paths.ca_key).ok()?;
    if cert.trim().is_empty() || key.trim().is_empty() {
        return None;
    }
    Some((cert, key))
}

/// 读取节点证书、私钥与到期时间；任一缺失则返回 `None`。
fn read_node(paths: &MtlsPaths) -> Option<(String, String, i64)> {
    let cert = fs::read_to_string(&paths.node_cert).ok()?;
    let key = fs::read_to_string(&paths.node_key).ok()?;
    let expiry: i64 = fs::read_to_string(&paths.node_expiry)
        .ok()?
        .trim()
        .parse()
        .ok()?;
    if cert.trim().is_empty() || key.trim().is_empty() {
        return None;
    }
    Some((cert, key, expiry))
}

/// 提取 PEM 中的 DER 字节（首个证书块）。
fn pem_to_der(pem: &str) -> Result<Vec<u8>> {
    let body: String = pem
        .lines()
        .filter(|l| !l.starts_with("-----"))
        .collect::<Vec<_>>()
        .join("");
    let der = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, body.trim())
        .context("failed to base64-decode certificate PEM")?;
    Ok(der)
}

/// 计算 SHA-256 指纹（小写十六进制）。
fn fingerprint(der: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(der);
    hex::encode(hasher.finalize())
}

/// 写文件；Unix 上对私钥尽力收紧为 0600。
fn write_secret(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create dir {}", parent.display()))?;
    }
    fs::write(path, bytes).with_context(|| format!("failed to write {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 每个测试用独立临时目录，避免进程内并行相互踩踏。
    fn tmp_dir(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("agentlink-mtls-{tag}-{nanos}"));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn mtls_generates_then_reloads_same_ca() {
        let dir = tmp_dir("reload");
        let cfg = MtlsConfig::default();

        let m1 = MtlsManager::load_or_create(&cfg, &dir).unwrap();
        let ca_fp = m1.ca_fingerprint().unwrap();
        let node_fp = m1.node_fingerprint().unwrap();
        assert!(!m1.ca_cert_pem().is_empty());
        assert!(!m1.node_cert_pem().is_empty());
        assert!(!m1.node_key_pem().is_empty());
        // 新鲜证书不应触发轮转。
        assert!(!m1.needs_rotation());

        // 二次加载：CA 与节点证书都应复用（指纹不变）。
        let m2 = MtlsManager::load_or_create(&cfg, &dir).unwrap();
        assert_eq!(m2.ca_fingerprint().unwrap(), ca_fp);
        assert_eq!(m2.node_fingerprint().unwrap(), node_fp);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn mtls_rotation_keeps_ca_but_changes_node() {
        let dir = tmp_dir("rotate");
        let cfg = MtlsConfig::default();
        let mut m = MtlsManager::load_or_create(&cfg, &dir).unwrap();
        let ca_fp = m.ca_fingerprint().unwrap();
        let node_fp_before = m.node_fingerprint().unwrap();

        m.rotate_node_cert().unwrap();

        assert_eq!(m.ca_fingerprint().unwrap(), ca_fp, "CA 必须保持不变");
        assert_ne!(
            m.node_fingerprint().unwrap(),
            node_fp_before,
            "轮转后节点证书应更新"
        );

        // 轮转结果应落盘：重新加载得到新节点证书。
        let m2 = MtlsManager::load_or_create(&cfg, &dir).unwrap();
        assert_eq!(
            m2.node_fingerprint().unwrap(),
            m.node_fingerprint().unwrap()
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn mtls_needs_rotation_inside_window() {
        // 到期时间距现在 100s，轮转窗口 3600s => 需要轮转。
        assert!(needs_rotation_at(1_000, 3_600, 900));
        // 距离到期还有一天，窗口 6h => 不需要。
        assert!(!needs_rotation_at(86_400, 21_600, 0));
        // 已过期 => 需要。
        assert!(needs_rotation_at(500, 10, 1_000));
    }

    #[test]
    fn mtls_rotate_interval_triggers_reissue_on_load() {
        let dir = tmp_dir("window");
        // 轮转窗口设得比 TTL 还大：任何新鲜证书都落在窗口内 => 每次加载都重签。
        let cfg = MtlsConfig {
            cert_ttl_days: 1,
            cert_rotate_interval_seconds: 10 * 24 * 3600,
            ..MtlsConfig::default()
        };
        let m1 = MtlsManager::load_or_create(&cfg, &dir).unwrap();
        let node_fp = m1.node_fingerprint().unwrap();
        let m2 = MtlsManager::load_or_create(&cfg, &dir).unwrap();
        assert_ne!(
            m2.node_fingerprint().unwrap(),
            node_fp,
            "落在大窗口内的证书应在加载时重签"
        );
        assert_eq!(
            m2.ca_fingerprint().unwrap(),
            m1.ca_fingerprint().unwrap(),
            "重签仍复用同一 CA"
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn mtls_honors_custom_ca_paths() {
        let dir = tmp_dir("custom");
        let ca_dir = dir.join("elsewhere");
        fs::create_dir_all(&ca_dir).unwrap();
        let cfg = MtlsConfig {
            ca_cert: Some(ca_dir.join("root.pem")),
            ca_key: Some(ca_dir.join("root.key")),
            ..MtlsConfig::default()
        };
        let m = MtlsManager::load_or_create(&cfg, &dir).unwrap();
        assert!(ca_dir.join("root.pem").exists());
        assert!(ca_dir.join("root.key").exists());
        assert_eq!(m.paths().ca_cert, ca_dir.join("root.pem"));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn mtls_recovers_from_missing_node_cert() {
        let dir = tmp_dir("recover");
        let cfg = MtlsConfig::default();
        let m = MtlsManager::load_or_create(&cfg, &dir).unwrap();
        let ca_fp = m.ca_fingerprint().unwrap();
        // 删掉节点证书，模拟损坏/丢失：应沿用 CA 重签，而不是换 CA。
        fs::remove_file(dir.join("mtls").join("node.pem")).unwrap();
        let m2 = MtlsManager::load_or_create(&cfg, &dir).unwrap();
        assert_eq!(m2.ca_fingerprint().unwrap(), ca_fp);
        assert!(!m2.node_cert_pem().is_empty());
        fs::remove_dir_all(&dir).ok();
    }
}
