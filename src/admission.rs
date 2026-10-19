//! 新节点准入（v0.6 P1，roadmap §4.1）：bootstrap 令牌 + 硬件指纹双验证。
//!
//! 目标：开启准入后，**即使拿到 cluster_token 的陌生机器也入不了网**——必须同时
//! 出示运维发放的准入令牌（`security.admission.bootstrap_token`），且本机硬件
//! 指纹在集群白名单（`security.admission.allowed_hardware`）里。
//!
//! 机制：握手帧（`Hello`）新增加性字段 `hw_fp`（本机指纹）与 `boot`（对 nonce 的
//! HMAC），接收端在同一 nonce 上复验。校验失败立即拒绝连接（对等通道与文件通道
//! 都走同一闸门）。
//!
//! 硬件指纹：优先用操作系统级机器标识（无第三方依赖、纯 Rust 读文件/命令行）——
//! Linux `/etc/machine-id`（或 `/var/lib/dbus/machine-id`）、Windows 机器 GUID
//! （MachineGuid）、macOS `IOPlatformUUID`；都没有时退回主机名。SHA-256 取前
//! 16 个 hex 字符，进程内缓存（同一台机器永不变化）。
//!
//! 安全说明：准入密钥与 `cluster_token` 同为集群信任域密钥（对称分发）。端口校验
//! 采取 fail-closed：开启却什么都没配（无令牌也无白名单）= 拒绝一切入网，并在
//! 启动时打告警，避免「看起来开了防护实际裸奔」。

use std::sync::OnceLock;

#[cfg(any(target_os = "macos", target_os = "windows"))]
use std::process::Command;

use sha2::{Digest, Sha256};

use crate::config::AdmissionConfig;
use crate::proto::{self, Hello};

/// 本机硬件指纹（进程内缓存）。
pub fn hardware_fingerprint() -> String {
    static FP: OnceLock<String> = OnceLock::new();
    FP.get_or_init(compute_fingerprint).clone()
}

fn compute_fingerprint() -> String {
    let raw = match machine_unique_id() {
        Some(id) => format!("agentlink|machine|{id}"),
        None => format!("agentlink|hostname|{}", hostname()),
    };
    let digest = Sha256::digest(raw.as_bytes());
    hex_short(&digest, 16)
}

/// 操作系统级机器标识（无则 None）。
fn machine_unique_id() -> Option<String> {
    if let Some(id) = read_trimmed_file("/etc/machine-id") {
        return Some(id);
    }
    if let Some(id) = read_trimmed_file("/var/lib/dbus/machine-id") {
        return Some(id);
    }
    #[cfg(target_os = "macos")]
    {
        if let Ok(out) = Command::new("ioreg")
            .args(["-rd1", "-c", "IOPlatformExpertDevice"])
            .output()
        {
            for line in String::from_utf8_lossy(&out.stdout).lines() {
                if let Some(rest) = line.trim().strip_prefix("\"IOPlatformUUID\"") {
                    if let Some(eq) = rest.find('=') {
                        let val = rest[eq + 1..].trim();
                        let val = val.strip_prefix('"').unwrap_or(val);
                        if let Some(val) = val.strip_suffix(';').or(Some(val)) {
                            let val = val.trim().trim_matches('"');
                            if !val.is_empty() {
                                return Some(val.to_string());
                            }
                        }
                    }
                }
            }
        }
    }
    #[cfg(target_os = "windows")]
    {
        if let Ok(out) = Command::new("reg")
            .args([
                "query",
                r"HKLM\SOFTWARE\Microsoft\Cryptography",
                "/v",
                "MachineGuid",
            ])
            .output()
        {
            for line in String::from_utf8_lossy(&out.stdout).lines() {
                // 形如：    MachineGuid    REG_SZ    8f2b-...
                if line.trim().starts_with("MachineGuid") {
                    if let Some(val) = line.split("REG_SZ").nth(1) {
                        let val = val.trim();
                        if !val.is_empty() {
                            return Some(val.to_string());
                        }
                    }
                }
            }
        }
    }
    None
}

/// 主机名（指纹兜底输入，不参与有机器标识时的哈希）。
fn hostname() -> String {
    #[cfg(target_os = "windows")]
    {
        if let Ok(h) = std::env::var("COMPUTERNAME") {
            if !h.is_empty() {
                return h;
            }
        }
    }
    if let Some(h) = read_trimmed_file("/etc/hostname") {
        return h;
    }
    if let Ok(h) = std::env::var("HOSTNAME") {
        if !h.is_empty() {
            return h;
        }
    }
    "unknown-host".to_string()
}

fn read_trimmed_file(path: &str) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// SHA-256 摘要前 `n` 个 hex 字符（小写）。
fn hex_short(digest: &[u8], n: usize) -> String {
    let full = hex::encode(digest);
    full.chars().take(n).collect()
}

// ---- 握手装配 / 校验 --------------------------------------------------------

/// 出站：把硬件指纹 + 准入令牌 HMAC 填进握手帧（幂等，任何模式都安全）。
pub fn seal_hello(hello: &mut Hello, adm: &AdmissionConfig) {
    hello.hw_fp = Some(hardware_fingerprint());
    if adm.boot_token_configured() {
        hello.boot = Some(proto::sign(
            &adm.bootstrap_token,
            &hello.node_id,
            &hello.nonce,
            hello.v,
        ));
    }
}

/// 入站：准入闸门。返回 `Err(原因)` 表示必须拒绝连接。
///
/// 顺序：必须在 cluster_token 校验**之后**、节点登记/CA 分发**之前**执行——
/// 未通过闸门的机器拿不到集群 CA（见 `conn.rs` 握手分支）。
pub fn gate(adm: &AdmissionConfig, hello: &Hello) -> Result<(), String> {
    if !adm.enabled {
        return Ok(());
    }
    let token_cfg = adm.boot_token_configured();
    if !token_cfg && adm.allowed_hardware.is_empty() {
        return Err("准入已开启但既没有配置准入令牌也没有指纹白名单（fail-closed）".to_string());
    }

    // 白名单检查（配置了名单才算）。
    if !adm.allowed_hardware.is_empty() {
        let Some(fp) = hello.hw_fp.as_deref() else {
            return Err("缺少硬件指纹（老版本节点或不支持准入）".to_string());
        };
        if !adm.hardware_allowed(fp) {
            return Err(format!("硬件指纹未在准入白名单 ({fp})"));
        }
    }

    // 准入令牌检查（配置了令牌才算）。
    if token_cfg {
        let Some(boot) = hello.boot.as_deref() else {
            return Err("缺少准入令牌证明（bootstrap token）".to_string());
        };
        let expected = proto::sign(&adm.bootstrap_token, &hello.node_id, &hello.nonce, hello.v);
        if !constant_time_eq(boot.as_bytes(), expected.as_bytes()) {
            return Err("准入令牌校验失败（bootstrap token）".to_string());
        }
    }
    Ok(())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AdmissionConfig;
    use crate::proto::Hello;

    fn default_adm() -> AdmissionConfig {
        AdmissionConfig::default()
    }

    /// 关闭准入：任何值都放行（与 v0.5 行为一致，零迁移）。
    #[test]
    fn gate_disabled_allows_everything() {
        assert!(gate(&default_adm(), &Hello::new("a", "n", "tok")).is_ok());
    }

    /// 双因素全配：bootstrap 令牌 + 白名单都对才放行。
    #[test]
    fn gate_dual_factor_accepts_only_full_match() {
        let adm = AdmissionConfig {
            enabled: true,
            bootstrap_token: "boot-tok".to_string(),
            allowed_hardware: vec!["aa11".to_string()],
        };
        // 正确装配的 hello 放行（seal 用真实 token，随后把指纹钉到白名单项）。
        let mut ok = Hello::new("n0", "m", "tok");
        seal_hello(&mut ok, &adm);
        ok.hw_fp = Some("aa11".to_string());
        assert!(gate(&adm, &ok).is_ok());

        // 缺 boot。
        let mut no_boot = Hello::new("n0", "m", "tok");
        no_boot.hw_fp = Some("aa11".to_string());
        assert!(gate(&adm, &no_boot).is_err());

        // boot 错（签名不对）。
        let mut bad_boot = Hello::new("n0", "m", "tok");
        bad_boot.hw_fp = Some("aa11".to_string());
        bad_boot.boot = Some("deadbeef".to_string());
        assert!(gate(&adm, &bad_boot).is_err());

        // 指纹不在白名单。
        let mut other_fp = Hello::new("n0", "m", "tok");
        seal_hello(&mut other_fp, &adm);
        other_fp.hw_fp = Some("bb22".to_string());
        assert!(gate(&adm, &other_fp).is_err());

        // 缺指纹（老版本节点）。
        let mut no_fp = Hello::new("n0", "m", "tok");
        seal_hello(&mut no_fp, &adm);
        no_fp.hw_fp = None;
        assert!(gate(&adm, &no_fp).is_err());
    }

    /// 只配令牌（白名单空）：单因素令牌模式。
    #[test]
    fn gate_token_only_mode() {
        let adm = AdmissionConfig {
            enabled: true,
            bootstrap_token: "boot-tok".to_string(),
            allowed_hardware: Vec::new(),
        };
        let mut h = Hello::new("n0", "m", "tok");
        seal_hello(&mut h, &adm);
        assert!(gate(&adm, &h).is_ok());
        h.boot = None;
        assert!(gate(&adm, &h).is_err());
    }

    /// 只配白名单：单因素指纹模式（令牌为占位符 = 未配置）。
    #[test]
    fn gate_fingerprint_only_mode() {
        let adm = AdmissionConfig {
            enabled: true,
            bootstrap_token: "change-me".to_string(),
            allowed_hardware: vec!["aa11".to_string()],
        };
        let mut h = Hello::new("n0", "m", "tok");
        h.hw_fp = Some("aa11".to_string());
        assert!(gate(&adm, &h).is_ok());
        h.hw_fp = Some("cc33".to_string());
        assert!(gate(&adm, &h).is_err());
        h.hw_fp = None;
        assert!(gate(&adm, &h).is_err());
    }

    /// fail-closed：开了准入但令牌和白名单都没配 → 拒绝一切（防止假防护）。
    #[test]
    fn gate_fail_closed_when_nothing_configured() {
        let mut adm = default_adm();
        adm.enabled = true;
        // 占位符 = 未配置。
        let h = Hello::new("n0", "m", "tok");
        assert!(gate(&adm, &h).is_err());
    }

    /// 硬件指纹：确定性 + 格式（16 位小写 hex）+ 两次不同指纹可区分。
    #[test]
    fn fingerprint_deterministic_and_hex() {
        let fp = hardware_fingerprint();
        let fp2 = hardware_fingerprint();
        assert_eq!(fp, fp2, "同一台机器指纹必定一致");
        assert_eq!(fp.len(), 16);
        assert!(fp
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase()));
        // 同一进程内不同 raw 前缀产生不同指纹（拿摘要空间保证域分离）。
        let digest = Sha256::digest(b"agentlink|other");
        assert_ne!(hex_short(&digest, 16), fp);
    }
}
