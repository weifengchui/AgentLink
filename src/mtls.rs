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
use std::sync::Arc;

use anyhow::{Context, Result};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DistinguishedName, DnType,
    ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose, PKCS_ECDSA_P256_SHA256,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use tokio::sync::watch;

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
    /// 现有 CA 重签；CA 缺失则新生成整条链。等价于
    /// [`Self::load_or_create_opt`] 且 `create_ca=true`。
    pub fn load_or_create(config: &MtlsConfig, data_dir: &Path) -> Result<Self> {
        Self::load_or_create_opt(config, data_dir, true)?
            .context("mtls manager unavailable despite create_ca=true")
    }

    /// 加载；**CA 缺失且 `create_ca=false` 时返回 `Ok(None)`**（成员节点：不建
    /// CA，等待经已认证的对等链路领取集群 CA，见 [`MtlsRuntime`]）。
    pub fn load_or_create_opt(
        config: &MtlsConfig,
        data_dir: &Path,
        create_ca: bool,
    ) -> Result<Option<Self>> {
        let paths = MtlsPaths::resolve(config, data_dir);
        if let Some(parent) = paths.node_cert.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create mtls dir {}", parent.display()))?;
        }

        let ttl = config.cert_ttl_days.max(1);
        let rotate = config.cert_rotate_interval_seconds;
        let now = OffsetDateTime::now_utc().unix_timestamp();

        // 1) CA：能读就用；读不到（缺失/损坏）时——权威节点新生成，成员节点
        //    返回 None（等待领取）。
        let (ca_cert_pem, ca_key_pem) = match read_ca(&paths) {
            Some(pair) => pair,
            None => {
                if !create_ca {
                    return Ok(None);
                }
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

        Ok(Some(Self {
            paths,
            ca_cert_pem,
            ca_key_pem,
            node_cert_pem,
            node_key_pem,
            node_expiry_unix,
            cert_ttl_days: ttl,
            rotate_interval_seconds: rotate,
        }))
    }

    /// 采用外部 CA（CA 自动分发）：先校验 PEM 可解析（能重建签发者）再落盘，
    /// 并用该 CA 重新签发本机节点证书。返回采用后的管理器，其 CA 指纹即新 CA。
    pub fn adopt_ca(
        config: &MtlsConfig,
        data_dir: &Path,
        ca_cert_pem: &str,
        ca_key_pem: &str,
    ) -> Result<Self> {
        // 校验：私钥可解析、算法受支持，能重建签发者，才允许落盘。
        let ca_signer = ca_signer_from_pems(ca_cert_pem, ca_key_pem)
            .context("rejected CA: unparseable cert/key")?;
        let paths = MtlsPaths::resolve(config, data_dir);
        if let Some(parent) = paths.node_cert.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create mtls dir {}", parent.display()))?;
        }
        write_secret(&paths.ca_cert, ca_cert_pem.as_bytes())?;
        write_secret(&paths.ca_key, ca_key_pem.as_bytes())?;

        let ttl = config.cert_ttl_days.max(1);
        let (cert, key, expiry) = issue_node(&ca_signer, ttl)?;
        write_secret(&paths.node_cert, cert.as_bytes())?;
        write_secret(&paths.node_key, key.as_bytes())?;
        fs::write(&paths.node_expiry, expiry.to_string())
            .with_context(|| format!("failed to write {}", paths.node_expiry.display()))?;

        Ok(Self {
            paths,
            ca_cert_pem: ca_cert_pem.to_string(),
            ca_key_pem: ca_key_pem.to_string(),
            node_cert_pem: cert,
            node_key_pem: key,
            node_expiry_unix: expiry,
            cert_ttl_days: ttl,
            rotate_interval_seconds: config.cert_rotate_interval_seconds,
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

    /// CA 私钥 PEM（PKCS#8）。CA 自动分发时用于推送给同信任域的对端。
    pub fn ca_key_pem(&self) -> &str {
        &self.ca_key_pem
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

    // ---- rustls 双向 TLS 配置（第二波：节点间传输） ----------------------------

    /// 信任锚：仅本集群 CA（CA pinning，不用系统根）。
    fn root_store(&self) -> Result<RootCertStore> {
        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from(self.ca_cert_der()?))
            .context("failed to add cluster CA to root store")?;
        Ok(roots)
    }

    /// 本机节点证书链（单证书，验签链由对端内置 CA 补全）。
    fn cert_chain(&self) -> Result<Vec<CertificateDer<'static>>> {
        Ok(vec![CertificateDer::from(self.node_cert_der()?)])
    }

    /// 本机节点私钥（PKCS#8 DER）。
    fn private_key(&self) -> Result<PrivateKeyDer<'static>> {
        let der = pem_to_der(&self.node_key_pem)?;
        Ok(PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(der)))
    }

    /// 服务端 TLS 配置：**要求并校验客户端证书**（双向 TLS），客户端证书必须
    /// 由本集群 CA 签发。
    pub fn server_config(&self) -> Result<Arc<ServerConfig>> {
        let verifier = WebPkiClientVerifier::builder(Arc::new(self.root_store()?))
            .build()
            .context("failed to build mTLS client verifier")?;
        let cfg = ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(self.cert_chain()?, self.private_key()?)
            .context("failed to build server TLS config")?;
        Ok(Arc::new(cfg))
    }

    /// 客户端 TLS 配置：只信任本集群 CA，并出示本机节点证书。
    pub fn client_config(&self) -> Result<Arc<ClientConfig>> {
        let cfg = ClientConfig::builder()
            .with_root_certificates(self.root_store()?)
            .with_client_auth_cert(self.cert_chain()?, self.private_key()?)
            .context("failed to build client TLS config")?;
        Ok(Arc::new(cfg))
    }
}

// ---- 运行期运行态（CA 自动分发） -------------------------------------------

/// 运行期 mTLS 运行态（v0.6 P1 CA 自动分发）。
///
/// 持有当前 CA/节点证书（可能为空——成员节点入网前尚无 CA），并把「服务端 TLS
/// 配置」经 watch 通道喂给独立 TLS 监听任务：成员节点领取到 CA 后即可**热启动**
/// 监听，无需重启进程。客户端配置供拨号时按需读取。
pub struct MtlsRuntime {
    config: MtlsConfig,
    data_dir: PathBuf,
    paths: MtlsPaths,
    inner: std::sync::RwLock<MtlsInner>,
    server_tx: watch::Sender<Option<Arc<ServerConfig>>>,
}

struct MtlsInner {
    manager: Option<MtlsManager>,
    client: Option<Arc<ClientConfig>>,
}

/// CA 领取结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdoptOutcome {
    /// 领取成功（此前无 CA）。
    Adopted,
    /// 与当前 CA 指纹一致，无需变更。
    Same,
    /// 已有不同的 CA：为防信任域分裂而拒绝覆盖。
    Conflict { local: String, remote: String },
}

impl MtlsRuntime {
    /// 由配置与当前管理器（可为 None = 成员节点待领取）构建运行态。
    pub fn new(
        config: MtlsConfig,
        data_dir: PathBuf,
        manager: Option<MtlsManager>,
    ) -> Result<Arc<Self>> {
        let paths = MtlsPaths::resolve(&config, &data_dir);
        let (client, server) = match &manager {
            Some(m) => (Some(m.client_config()?), Some(m.server_config()?)),
            None => (None, None),
        };
        let (server_tx, _rx) = watch::channel(server);
        Ok(Arc::new(Self {
            config,
            data_dir,
            paths,
            inner: std::sync::RwLock::new(MtlsInner { manager, client }),
            server_tx,
        }))
    }

    /// 证书文件路径集合。
    pub fn paths(&self) -> &MtlsPaths {
        &self.paths
    }

    /// 服务端 TLS 配置的 watch 订阅（TLS 监听任务据此绑定）。
    pub fn server_watch(&self) -> watch::Receiver<Option<Arc<ServerConfig>>> {
        self.server_tx.subscribe()
    }

    /// 是否已就绪（持有 CA，可用双向 TLS）。
    pub fn is_ready(&self) -> bool {
        self.inner.read().unwrap().manager.is_some()
    }

    /// 当前集群 CA 指纹（未领取时为 None）。
    pub fn ca_fingerprint(&self) -> Option<String> {
        self.inner
            .read()
            .unwrap()
            .manager
            .as_ref()
            .and_then(|m| m.ca_fingerprint().ok())
    }

    /// 当前 CA（证书 PEM + 私钥 PEM），用于推送给对端。
    pub fn ca_pems(&self) -> Option<(String, String)> {
        self.inner
            .read()
            .unwrap()
            .manager
            .as_ref()
            .map(|m| (m.ca_cert_pem().to_string(), m.ca_key_pem().to_string()))
    }

    /// 当前客户端 TLS 配置（未就绪时为 None，拨号回落明文）。
    pub fn client_config(&self) -> Option<Arc<ClientConfig>> {
        self.inner.read().unwrap().client.clone()
    }

    /// 领取集群 CA（CA 自动分发）：校验 → 落盘 → 用该 CA 重签本机节点证书 →
    /// 热更新 TLS 配置并通知监听任务。
    ///
    /// 冲突保护：本地已有**不同** CA 时返回 [`AdoptOutcome::Conflict`] 且不改动
    /// 任何文件（避免信任域被静默替换/分裂）。
    pub fn adopt(&self, ca_cert_pem: &str, ca_key_pem: &str) -> Result<AdoptOutcome> {
        let remote_fp = fingerprint_of_pem(ca_cert_pem)?;
        let mut g = self.inner.write().unwrap();
        if let Some(m) = &g.manager {
            let local_fp = m.ca_fingerprint().unwrap_or_default();
            if local_fp == remote_fp {
                return Ok(AdoptOutcome::Same);
            }
            return Ok(AdoptOutcome::Conflict {
                local: local_fp,
                remote: remote_fp,
            });
        }
        let manager = MtlsManager::adopt_ca(&self.config, &self.data_dir, ca_cert_pem, ca_key_pem)?;
        let client = manager.client_config()?;
        let server = manager.server_config()?;
        *g = MtlsInner {
            manager: Some(manager),
            client: Some(client),
        };
        let _ = self.server_tx.send(Some(server));
        Ok(AdoptOutcome::Adopted)
    }
}

/// 由 CA 证书 PEM 计算 SHA-256 指纹（用于对端声明的指纹去伪存真）。
pub fn fingerprint_of_pem(pem: &str) -> Result<String> {
    Ok(fingerprint(&pem_to_der(pem)?))
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
    let mut params = CertificateParams::new(resolve_sans());
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

/// 节点证书的 Subject Alternative Names：`localhost`/回环 + 主机名 +
/// 本机全部网卡 IP。对端按 IP 拨号时，标准 rustls 校验器即可通过名称校验，
/// 从而无需自写「跳过域名」的危险校验器。IP 变化由 24h 轮转自然刷新。
fn resolve_sans() -> Vec<String> {
    let mut sans = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "::1".to_string(),
    ];
    if let Some(h) = hostname() {
        sans.push(h);
    }
    if let Ok(ifs) = local_ip_address::list_afinet_netifas() {
        for (_name, ip) in ifs {
            if !ip.is_unspecified() {
                sans.push(ip.to_string());
            }
        }
    }
    sans.sort();
    sans.dedup();
    sans
}

/// 尽力获取主机名（Windows: COMPUTERNAME；类 Unix: HOSTNAME）。
fn hostname() -> Option<String> {
    std::env::var("COMPUTERNAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .ok()
        .filter(|s| !s.trim().is_empty())
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

    #[test]
    fn mtls_member_does_not_create_ca() {
        let dir = tmp_dir("member-noca");
        let cfg = MtlsConfig::default();
        // 成员节点（create_ca=false）且无 CA：不建 CA，返回 None。
        assert!(MtlsManager::load_or_create_opt(&cfg, &dir, false)
            .unwrap()
            .is_none());
        assert!(!dir.join("mtls").join("ca.pem").exists());
        fs::remove_dir_all(&dir).ok();
    }

    /// CA 自动分发：成员节点从零领取权威 CA → 落盘 → 与权威节点双向 TLS 握手。
    #[tokio::test]
    async fn mtls_member_adopts_ca_then_handshakes_with_authority() {
        let auth_dir = tmp_dir("adopt-auth");
        let member_dir = tmp_dir("adopt-member");
        let cfg = MtlsConfig::default();
        let authority = MtlsManager::load_or_create(&cfg, &auth_dir).unwrap();
        let authority_fp = authority.ca_fingerprint().unwrap();

        // 成员初始未就绪（无 CA）。
        let member = MtlsRuntime::new(cfg.clone(), member_dir.clone(), None).unwrap();
        assert!(!member.is_ready());
        assert!(member.client_config().is_none());
        assert!(member.ca_fingerprint().is_none());

        // 领取权威 CA。
        assert_eq!(
            member
                .adopt(authority.ca_cert_pem(), authority.ca_key_pem())
                .unwrap(),
            AdoptOutcome::Adopted
        );
        assert!(member.is_ready());
        assert_eq!(member.ca_fingerprint().unwrap(), authority_fp);

        // 落盘：成员以 create_ca=false 重新加载仍得到同一 CA。
        let reloaded = MtlsManager::load_or_create_opt(&cfg, &member_dir, false)
            .unwrap()
            .expect("成员领取后应能加载");
        assert_eq!(reloaded.ca_fingerprint().unwrap(), authority_fp);

        // 领取后：成员客户端 ↔ 权威服务器，双向 TLS 握手成功。
        let (addr, server) = spawn_tls_echo(authority.server_config().unwrap()).await;
        client_roundtrip(member.client_config().unwrap(), addr)
            .await
            .expect("领取 CA 后应能与权威节点双向 TLS 握手");
        let _ = server.await;

        // 重复领取同一 CA：Same（幂等）。
        assert_eq!(
            member
                .adopt(authority.ca_cert_pem(), authority.ca_key_pem())
                .unwrap(),
            AdoptOutcome::Same
        );

        fs::remove_dir_all(&auth_dir).ok();
        fs::remove_dir_all(&member_dir).ok();
    }

    /// 冲突保护：本地已有不同 CA 时拒绝覆盖（防信任域分裂）。
    #[test]
    fn mtls_runtime_adopt_conflict_keeps_local_ca() {
        let dir = tmp_dir("adopt-conflict");
        let cfg = MtlsConfig::default();
        let local = MtlsManager::load_or_create(&cfg, &dir).unwrap();
        let local_fp = local.ca_fingerprint().unwrap();
        let rt = MtlsRuntime::new(cfg.clone(), dir.clone(), Some(local)).unwrap();

        let other_dir = tmp_dir("adopt-other");
        let other = MtlsManager::load_or_create(&cfg, &other_dir).unwrap();
        let other_fp = other.ca_fingerprint().unwrap();
        assert_ne!(local_fp, other_fp);

        match rt.adopt(other.ca_cert_pem(), other.ca_key_pem()).unwrap() {
            AdoptOutcome::Conflict { local, remote } => {
                assert_eq!(local, local_fp);
                assert_eq!(remote, other_fp);
            }
            got => panic!("expected conflict, got {got:?}"),
        }
        // 本地 CA 未被覆盖。
        assert_eq!(rt.ca_fingerprint().unwrap(), local_fp);
        assert_ne!(rt.ca_fingerprint().unwrap(), other_fp);

        fs::remove_dir_all(&dir).ok();
        fs::remove_dir_all(&other_dir).ok();
    }

    /// 起一个最小 TLS echo 服务器，返回其地址；用于握手自证。
    async fn spawn_tls_echo(
        server: Arc<ServerConfig>,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (tcp, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => return,
            };
            let acceptor = tokio_rustls::TlsAcceptor::from(server);
            let mut tls = match acceptor.accept(tcp).await {
                Ok(s) => s,
                Err(_) => return,
            };
            let mut buf = [0u8; 4];
            if tls.read_exact(&mut buf).await.is_ok() {
                let _ = tls.write_all(b"pong").await;
                let _ = tls.flush().await;
            }
        });
        (addr, handle)
    }

    async fn client_roundtrip(
        client: Arc<ClientConfig>,
        addr: std::net::SocketAddr,
    ) -> anyhow::Result<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let tcp = tokio::net::TcpStream::connect(addr).await?;
        let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let mut tls = tokio_rustls::TlsConnector::from(client)
            .connect(name, tcp)
            .await?;
        tls.write_all(b"ping").await?;
        tls.flush().await?;
        let mut buf = [0u8; 4];
        tls.read_exact(&mut buf).await?;
        anyhow::ensure!(&buf == b"pong", "unexpected echo payload");
        Ok(())
    }

    /// 双向 TLS 正例：同一 CA 的客户端 → 服务器，握手成功且数据往返。
    #[tokio::test]
    async fn mtls_two_way_handshake_succeeds() {
        let dir = tmp_dir("tls-ok");
        let cfg = MtlsConfig::default();
        let m = MtlsManager::load_or_create(&cfg, &dir).unwrap();
        let (addr, server) = spawn_tls_echo(m.server_config().unwrap()).await;

        client_roundtrip(m.client_config().unwrap(), addr)
            .await
            .expect("同 CA 双向 TLS 握手应成功");

        let _ = server.await;
        fs::remove_dir_all(&dir).ok();
    }

    /// 客户端无证书（普通 TLS）→ 服务器要求客户端证书，必须拒绝。
    #[tokio::test]
    async fn mtls_server_rejects_client_without_cert() {
        let dir = tmp_dir("tls-noclient");
        let cfg = MtlsConfig::default();
        let m = MtlsManager::load_or_create(&cfg, &dir).unwrap();
        let (addr, server) = spawn_tls_echo(m.server_config().unwrap()).await;

        // 只信任 CA，但不出示客户端证书。
        let no_client = ClientConfig::builder()
            .with_root_certificates(m.root_store().unwrap())
            .with_no_client_auth();
        let res = client_roundtrip(Arc::new(no_client), addr).await;
        assert!(res.is_err(), "无客户端证书的连接必须失败");

        let _ = server.await;
        fs::remove_dir_all(&dir).ok();
    }

    /// 客户端来自另一套 CA（未受信）→ 服务器侧客户端证书校验失败。
    #[tokio::test]
    async fn mtls_server_rejects_client_from_other_ca() {
        let dir_a = tmp_dir("tls-ca-a");
        let dir_b = tmp_dir("tls-ca-b");
        let cfg = MtlsConfig::default();
        let server_m = MtlsManager::load_or_create(&cfg, &dir_a).unwrap();
        let client_m = MtlsManager::load_or_create(&cfg, &dir_b).unwrap();

        let (addr, server) = spawn_tls_echo(server_m.server_config().unwrap()).await;
        let res = client_roundtrip(client_m.client_config().unwrap(), addr).await;
        assert!(res.is_err(), "异 CA 客户端必须被拒绝");

        let _ = server.await;
        fs::remove_dir_all(&dir_a).ok();
        fs::remove_dir_all(&dir_b).ok();
    }
}
