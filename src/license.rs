//! AgentMesh license：license.toml 的 Ed25519 离线验签 + tier/node_quota 校验（v0.6 M1 / 设计 §3）。
//!
//! # license.toml 格式规范
//!
//! 逐行 `key = "value"` / `key = 数字` 形式（`#` 注释与空行忽略）。字段固定：
//!
//! | 字段 | 类型 | 说明 |
//! |---|---|---|
//! | `tier` | 字符串 | `"starter"`（node_quota ≤ 15）或 `"pro"`（node_quota ≤ 50），上限硬校验 |
//! | `node_quota` | 无符号整数 | 被 Mesh 调度的不重复 node_id 上限 |
//! | `issued` | 日期时间 | RFC 3339（如 `2026-10-01T00:00:00Z`）或 `YYYY-MM-DD` |
//! | `expires` | 日期时间 | 同上；`YYYY-MM-DD` 按当日 23:59:59 UTC 理解 |
//! | `node_family_prefix` | 字符串（可选） | 节点族前缀限制，缺省=不限 |
//! | `signature` | base64 | 对下述“规范化签名体”的 Ed25519 签名（64 字节标准签） |
//!
//! # 规范化签名体（canonical bytes）—— 签发方与本验签方必须逐字节一致
//!
//! 取参与签名的明文字段（`tier`/`node_quota`/`issued`/`expires`，以及出现时的
//! `node_family_prefix`；`signature` 字段本身不参与），**按字段名字典序**排序，
//! 每行拼为 `key=value`（value 用明文形态：字符串去引号、整数直接十进制、日期
//! 保持原字符串），行间用 LF(`\n`) 连接，**无尾换行**，整段按 UTF-8 编码。
//!
//! 之所以手写极简解析而不引入 `toml` crate：license 字段集封闭、全部是顶层
//! `key = value` 标量（无表、无数组、无嵌套），逐行解析即可覆盖；查过 Cargo.toml，
//! 现依赖表本无 toml——避免为此新增依赖，对齐“单二进制零膨胀”取向。

#![allow(dead_code)] // M1 休眠骨架：verify_license_file 等接口为 P1-3/P1-5（config/api 接线）预留

use base64::Engine;
use ed25519_compact::{PublicKey, Signature};
use time::format_description::well_known::Rfc3339;
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};

/// 内置验签公钥（Ed25519，32 字节的标准 base64 编码）。
///
/// ⚠️ **开发 fixture 公钥**（对应测试私钥 seed=0x00..0x1F，仅用于打通流程）。
/// **发布前必须替换为厂方正式公钥**——私钥由厂方离线持有，绝不明文入库。
/// 替换方式：把正式公钥的 base64 粘贴到这里即可，其余代码零改动。
pub const DEV_PUBLIC_KEY_B64: &str = "A6EHv/POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg=";

/// Starter 档节点配额硬上限（设计 §1 拍板记录：Starter≤15）。
pub const STARTER_MAX_QUOTA: u32 = 15;
/// Pro 档节点配额硬上限（设计 §1 拍板记录：Pro≤50）。
pub const PRO_MAX_QUOTA: u32 = 50;

/// license 档位。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LicenseTier {
    Starter,
    Pro,
}

impl LicenseTier {
    /// 档位对应的 node_quota 硬上限。
    pub fn max_node_quota(self) -> u32 {
        match self {
            LicenseTier::Starter => STARTER_MAX_QUOTA,
            LicenseTier::Pro => PRO_MAX_QUOTA,
        }
    }

    /// 档位名的 wire 形态（与 license.toml 中 `tier` 取值一致）。
    pub fn as_str(self) -> &'static str {
        match self {
            LicenseTier::Starter => "starter",
            LicenseTier::Pro => "pro",
        }
    }
}

/// 验签通过后的 license 观测量（供后续调度/quota 接线消费）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LicenseInfo {
    /// 档位（starter/pro）。
    pub tier: LicenseTier,
    /// 节点配额上限（已经过 tier 硬校验）。
    pub node_quota: u32,
    /// 到期时刻（UTC）。
    pub expires: OffsetDateTime,
}

/// license 校验错误枚举。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LicenseError {
    /// 结构损坏：文件读取失败、行格式非法、缺必填字段、重复键、日期/数字/base64
    /// 解析失败、签名长度不对、未知 tier 等——内容根本不是一枚合法 license。
    Malformed(String),
    /// 篡改或错公钥：Ed25519 验签失败。两种成因在密码学上不可区分，合并报错，
    /// 避免给攻击者“格式对但钥错”的探测预言机。
    BadSignature,
    /// 已过期：`expires` 早于当前 UTC 时刻。
    Expired {
        /// 具体的到期时刻，便于日志展示。
        expires: OffsetDateTime,
    },
    /// tier 与 node_quota 组合越界：如 starter 签发 node_quota=16。
    TierQuotaExceeded {
        /// license 声明的档位。
        tier: LicenseTier,
        /// license 声明的配额值。
        node_quota: u32,
        /// 该档位允许的上限。
        max: u32,
    },
}

impl core::fmt::Display for LicenseError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            LicenseError::Malformed(m) => write!(f, "license 结构损坏: {m}"),
            LicenseError::BadSignature => write!(f, "license 验签失败（篡改或公钥不匹配）"),
            LicenseError::Expired { expires } => write!(f, "license 已过期: {expires}"),
            LicenseError::TierQuotaExceeded {
                tier,
                node_quota,
                max,
            } => write!(
                f,
                "license 档位越界: tier={} 上限 {max}，声明 {node_quota}",
                tier.as_str()
            ),
        }
    }
}

impl std::error::Error for LicenseError {}

/// 参与签名的已知明文字段（`signature` 除外）。**必须保持字典序**——
/// [`canonical_signing_body`] 按此顺序输出即满足规范化要求。
const SIGNED_FIELDS: [&str; 5] = [
    "expires",
    "issued",
    "node_family_prefix", // 可选：字段出现才参与
    "node_quota",
    "tier",
];

/// 解析 license.toml 文本 → 字段表（值已 trim/去引号）。
/// 只认顶层 `key = value` 标量行；`#` 注释与空行跳过；重复键视为结构损坏。
/// 未知字段忽略不报错（签发方将来加字段不破坏旧验签路径；读取方只取白名单键）。
fn parse_fields(text: &str) -> Result<Vec<(String, String)>, LicenseError> {
    let mut fields: Vec<(String, String)> = Vec::new();
    for (lineno, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            return Err(LicenseError::Malformed(format!(
                "第 {} 行不是 key = value 形式",
                lineno + 1
            )));
        };
        let key = key.trim().to_string();
        if key.is_empty() {
            return Err(LicenseError::Malformed(format!(
                "第 {} 行键为空",
                lineno + 1
            )));
        }
        let value = {
            let v = value.trim();
            // 极简字符串形态：仅剥离成对双引号，不支持转义（license 字段值均为
            // 固定格式标量，无引号内等号/转义需求，刻意不实现完整 TOML）。
            v.strip_prefix('"')
                .and_then(|s| s.strip_suffix('"'))
                .unwrap_or(v)
                .to_string()
        };
        if fields.iter().any(|(k, _)| *k == key) {
            return Err(LicenseError::Malformed(format!(
                "第 {} 行重复键 `{key}`",
                lineno + 1
            )));
        }
        fields.push((key, value));
    }
    Ok(fields)
}

/// 按模块注释中的规范拼出签名字节的规范化字节序（UTF-8）：
/// 白名单字段按字段名字典序重拼 `key=value` 行，LF 分隔，无尾换行。
/// 缺失的可选字段跳过；必填缺失由 [`get_required`] 在验签前统一报结构损坏。
fn canonical_signing_body(fields: &[(String, String)]) -> Vec<u8> {
    let mut body = String::new();
    let mut first = true;
    for name in SIGNED_FIELDS {
        if let Some((_, value)) = fields.iter().find(|(k, _)| k == name) {
            if !first {
                body.push('\n');
            }
            body.push_str(name);
            body.push('=');
            body.push_str(value);
            first = false;
        }
    }
    body.into_bytes()
}

/// 取必填字段；缺失即结构损坏。
fn get_required<'a>(fields: &'a [(String, String)], name: &str) -> Result<&'a str, LicenseError> {
    fields
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_str())
        .ok_or_else(|| LicenseError::Malformed(format!("缺少必填字段 `{name}`")))
}

/// 解析日期时间：优先 RFC 3339（`2026-10-01T00:00:00Z`），
/// 兼容纯日期 `YYYY-MM-DD`（按当日 23:59:59 UTC 理解，语义="该日结束前有效"）。
fn parse_datetime(s: &str) -> Result<OffsetDateTime, LicenseError> {
    if let Ok(dt) = OffsetDateTime::parse(s, &Rfc3339) {
        return Ok(dt);
    }
    // 手写 YYYY-MM-DD 拆解（不经 format_description，避开其已弃用的解析入口）
    if s.len() == 10
        && s.as_bytes()[4] == b'-'
        && s.as_bytes()[7] == b'-'
        && s.bytes().enumerate().all(|(i, c)| {
            if i == 4 || i == 7 {
                c == b'-'
            } else {
                c.is_ascii_digit()
            }
        })
    {
        let year: i32 = s[0..4].parse().expect("已验证全数字");
        let month: u8 = s[5..7].parse().expect("已验证全数字");
        let day: u8 = s[8..10].parse().expect("已验证全数字");
        if let Ok(month) = time::Month::try_from(month) {
            if let Ok(date) = Date::from_calendar_date(year, month, day) {
                let end_of_day =
                    PrimitiveDateTime::new(date, Time::from_hms(23, 59, 59).expect("常量时间合法"));
                return Ok(end_of_day.assume_utc());
            }
        }
    }
    Err(LicenseError::Malformed(format!(
        "无法解析日期时间 `{s}`（需要 RFC 3339 或 YYYY-MM-DD）"
    )))
}

/// 从内置常量解码验签公钥。
pub fn default_public_key() -> Result<PublicKey, LicenseError> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(DEV_PUBLIC_KEY_B64)
        .map_err(|e| LicenseError::Malformed(format!("内置公钥 base64 非法: {e}")))?;
    PublicKey::from_slice(&bytes)
        .map_err(|e| LicenseError::Malformed(format!("内置公钥长度/编码非法: {e}")))
}

/// 用内置（开发 fixture）公钥验签 license.toml 文本。
pub fn verify_license(text: &str) -> Result<LicenseInfo, LicenseError> {
    verify_license_with_key(text, &default_public_key()?)
}

/// 读取 license.toml 文件并用内置公钥验签（IO 失败归入结构损坏类）。
pub fn verify_license_file(path: &std::path::Path) -> Result<LicenseInfo, LicenseError> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| LicenseError::Malformed(format!("读取 {} 失败: {e}", path.display())))?;
    verify_license(&text)
}

/// 指定公钥验签（测试注入 fixture 钥用；生产路径走 [`verify_license`]）。
/// 校验链：解析 toml → 提取签名体（规范化字节序）→ Ed25519 验签 →
/// tier/quota 硬校验 → 有效期。先证明“没被篡改”再解释内容，
/// 防止用非法结构构造签名探测预言机。
pub fn verify_license_with_key(
    text: &str,
    public_key: &PublicKey,
) -> Result<LicenseInfo, LicenseError> {
    let fields = parse_fields(text)?;

    // 1) 必填字段存在性（先于验签做廉价结构检查）
    let sig_b64 = get_required(&fields, "signature")?;
    get_required(&fields, "tier")?;
    get_required(&fields, "node_quota")?;
    get_required(&fields, "issued")?;
    get_required(&fields, "expires")?;

    // 2) 提取签名与规范化签名体，做 Ed25519 验签
    let sig_bytes = base64::engine::general_purpose::STANDARD
        .decode(sig_b64)
        .map_err(|_| LicenseError::Malformed("signature 不是合法 base64".into()))?;
    let signature = Signature::from_slice(&sig_bytes).map_err(|_| {
        LicenseError::Malformed(format!(
            "signature 解码后 {} 字节，应为 {}",
            sig_bytes.len(),
            Signature::BYTES
        ))
    })?;
    let body = canonical_signing_body(&fields);
    public_key
        .verify(&body, &signature)
        .map_err(|_| LicenseError::BadSignature)?;

    // 3) 业务规则：tier 合法性与 node_quota 硬上限
    let tier = match get_required(&fields, "tier")? {
        "starter" => LicenseTier::Starter,
        "pro" => LicenseTier::Pro,
        other => return Err(LicenseError::Malformed(format!("未知 tier `{other}`"))),
    };
    let node_quota: u32 = get_required(&fields, "node_quota")?
        .parse()
        .map_err(|_| LicenseError::Malformed("node_quota 不是无符号整数".into()))?;
    if node_quota > tier.max_node_quota() {
        return Err(LicenseError::TierQuotaExceeded {
            tier,
            node_quota,
            max: tier.max_node_quota(),
        });
    }

    // 4) 有效期：issued ≤ expires 属结构一致性；expires 与当前时刻比较判过期。
    //    不校验 issued 是否晚于“现在”——签发方时钟超前不应让现网拒用合法 license。
    let issued = parse_datetime(get_required(&fields, "issued")?)?;
    let expires = parse_datetime(get_required(&fields, "expires")?)?;
    if issued > expires {
        return Err(LicenseError::Malformed(
            "issued 晚于 expires，时间区间非法".into(),
        ));
    }
    if expires < OffsetDateTime::now_utc() {
        return Err(LicenseError::Expired { expires });
    }

    Ok(LicenseInfo {
        tier,
        node_quota,
        expires,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_compact::KeyPair;

    /// 测试内现生成的 fixture 密钥对（与内置开发公钥常量无关，
    /// 注入 [`verify_license_with_key`] 使用）。
    fn fixture_keypair() -> KeyPair {
        KeyPair::generate()
    }

    /// 测试构造器：按规范化规范拼签名体并签发一枚 license.toml 文本。
    /// 字段按入参顺序书写（书写顺序 ≠ 签名顺序，用来验证规范化重拼）。
    fn sign_license(kp: &KeyPair, entries: &[(&str, &str)]) -> String {
        let fields: Vec<(String, String)> = entries
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let body = canonical_signing_body(&fields);
        let sig = kp.sk.sign(&body, None);
        let sig_b64 = base64::engine::general_purpose::STANDARD.encode(&sig[..]);
        let mut out = String::from("# test license\n");
        for (k, v) in entries {
            if *k == "node_quota" {
                // 整数按 TOML 裸数字风格写出，顺带验证引号剥离与裸值等价
                out.push_str(&format!("{k} = {v}\n"));
            } else {
                out.push_str(&format!("{k} = \"{v}\"\n"));
            }
        }
        out.push_str(&format!("signature = \"{sig_b64}\"\n"));
        out
    }

    fn valid_entries(expires: &str) -> Vec<(&str, &str)> {
        vec![
            ("tier", "starter"),
            ("node_quota", "10"),
            ("issued", "2026-01-01T00:00:00Z"),
            ("expires", expires),
        ]
    }

    fn future_expires() -> String {
        // 相对“现在 + 约一年”，避免单测随日历过期
        let d = OffsetDateTime::now_utc() + time::Duration::days(366);
        format!(
            "{:04}-{:02}-{:02}T00:00:00Z",
            d.year(),
            d.month() as u8,
            d.day()
        )
    }

    use time::Month;

    // ---------- 合法签 ----------
    #[test]
    fn valid_sig_ok() {
        let kp = fixture_keypair();
        let exp = future_expires();
        let mut entries = valid_entries(&exp);
        entries.push(("node_family_prefix", "lan"));
        let text = sign_license(&kp, &entries);
        let info = verify_license_with_key(&text, &kp.pk).expect("合法 license 应通过");
        assert_eq!(info.tier, LicenseTier::Starter);
        assert_eq!(info.node_quota, 10);
        assert!(info.expires > OffsetDateTime::now_utc());
    }

    #[test]
    fn field_order_permutation_still_valid() {
        // 规范化按字典序重拼，文件内书写顺序无关；pro 上限恰为 50，边界合法
        let kp = fixture_keypair();
        let text = sign_license(
            &kp,
            &[
                ("expires", &future_expires()),
                ("issued", "2026-01-01T00:00:00Z"),
                ("tier", "pro"),
                ("node_quota", "50"),
            ],
        );
        let info = verify_license_with_key(&text, &kp.pk).expect("乱序书写的合法 license 应通过");
        assert_eq!(info.tier, LicenseTier::Pro);
        assert_eq!(info.node_quota, 50);
    }

    // ---------- 篡改签 ----------
    #[test]
    fn tampered_body_rejected() {
        let kp = fixture_keypair();
        let text = sign_license(&kp, &valid_entries(&future_expires()));
        // 签后篡改：quota 从 10 抬到 50
        let tampered = text.replace("node_quota = 10", "node_quota = 50");
        assert_ne!(tampered, text, "篡改构造器必须真的改动了文本");
        assert_eq!(
            verify_license_with_key(&tampered, &kp.pk).unwrap_err(),
            LicenseError::BadSignature
        );
        // 篡改日期同理
        let tampered2 = text.replace(
            "issued = \"2026-01-01T00:00:00Z\"",
            "issued = \"2020-01-01T00:00:00Z\"",
        );
        assert_eq!(
            verify_license_with_key(&tampered2, &kp.pk).unwrap_err(),
            LicenseError::BadSignature
        );
    }

    // ---------- 过期签 ----------
    #[test]
    fn expired_rejected() {
        let kp = fixture_keypair();
        // 合法签发但 expires 在过去（issued < expires，避免先撞上区间非法检查）
        let text = sign_license(
            &kp,
            &[
                ("tier", "starter"),
                ("node_quota", "10"),
                ("issued", "2019-01-01T00:00:00Z"),
                ("expires", "2020-01-01T00:00:00Z"),
            ],
        );
        match verify_license_with_key(&text, &kp.pk) {
            Err(LicenseError::Expired { expires }) => {
                assert!(expires < OffsetDateTime::now_utc());
            }
            other => panic!("过期 license 应报 Expired，实际: {other:?}"),
        }
    }

    #[test]
    fn issued_after_expires_malformed() {
        let kp = fixture_keypair();
        // 真签但时间区间非法（issued > expires）
        let text = sign_license(
            &kp,
            &[
                ("tier", "starter"),
                ("node_quota", "5"),
                ("issued", "2030-01-01T00:00:00Z"),
                ("expires", "2020-01-01T00:00:00Z"),
            ],
        );
        assert!(matches!(
            verify_license_with_key(&text, &kp.pk),
            Err(LicenseError::Malformed(_))
        ));
    }

    // ---------- 错公钥 ----------
    #[test]
    fn wrong_pubkey_rejected() {
        let issuer = fixture_keypair();
        let victim = fixture_keypair();
        let text = sign_license(&issuer, &valid_entries(&future_expires()));
        // 用另一枚公钥验同一枚合法签：与篡改合并为 BadSignature
        assert_eq!(
            verify_license_with_key(&text, &victim.pk).unwrap_err(),
            LicenseError::BadSignature
        );
    }

    // ---------- tier 与 quota 超限 ----------
    #[test]
    fn tier_quota_over_limit_rejected() {
        let kp = fixture_keypair();
        // starter 声明 quota=16：真签（证明硬校验独立于签名、越界只在业务规则处拒绝）
        let over_starter = sign_license(
            &kp,
            &[
                ("tier", "starter"),
                ("node_quota", "16"),
                ("issued", "2026-01-01T00:00:00Z"),
                ("expires", &future_expires()),
            ],
        );
        assert_eq!(
            verify_license_with_key(&over_starter, &kp.pk).unwrap_err(),
            LicenseError::TierQuotaExceeded {
                tier: LicenseTier::Starter,
                node_quota: 16,
                max: STARTER_MAX_QUOTA
            }
        );
        // pro 声明 quota=51
        let over_pro = sign_license(
            &kp,
            &[
                ("tier", "pro"),
                ("node_quota", "51"),
                ("issued", "2026-01-01T00:00:00Z"),
                ("expires", &future_expires()),
            ],
        );
        assert_eq!(
            verify_license_with_key(&over_pro, &kp.pk).unwrap_err(),
            LicenseError::TierQuotaExceeded {
                tier: LicenseTier::Pro,
                node_quota: 51,
                max: PRO_MAX_QUOTA
            }
        );
        // 未知 tier（签名合法）→ 结构损坏类
        let weird_tier = sign_license(
            &kp,
            &[
                ("tier", "ultra"),
                ("node_quota", "1"),
                ("issued", "2026-01-01T00:00:00Z"),
                ("expires", &future_expires()),
            ],
        );
        assert!(matches!(
            verify_license_with_key(&weird_tier, &kp.pk),
            Err(LicenseError::Malformed(_))
        ));
    }

    // ---------- 结构损坏 ----------
    #[test]
    fn malformed_variants() {
        let kp = fixture_keypair();
        let good = sign_license(&kp, &valid_entries(&future_expires()));
        // 缺 signature
        let no_sig: String = good
            .lines()
            .filter(|l| !l.starts_with("signature"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(matches!(
            verify_license_with_key(&no_sig, &kp.pk),
            Err(LicenseError::Malformed(_))
        ));
        // signature 非 base64
        let bad_b64 = good.replace("signature = \"", "signature = \"not_base64_!!!_");
        assert!(matches!(
            verify_license_with_key(&bad_b64, &kp.pk).unwrap_err(),
            LicenseError::Malformed(_)
        ));
        // signature 长度不足（base64 合法但解码后 < 64 字节）
        let short_sig = good.replace("signature = \"", "signature = \"AAAA");
        assert!(matches!(
            verify_license_with_key(&short_sig, &kp.pk).unwrap_err(),
            LicenseError::Malformed(_)
        ));
        // 重复键
        let dup = good.clone() + "tier = \"pro\"\n";
        assert!(matches!(
            verify_license_with_key(&dup, &kp.pk),
            Err(LicenseError::Malformed(_))
        ));
        // 非 key=value 行
        let junk = good.clone() + "this is not toml\n";
        assert!(matches!(
            verify_license_with_key(&junk, &kp.pk),
            Err(LicenseError::Malformed(_))
        ));
        // node_quota 非数字：真签（quota 参与签名体）→ 验签过后业务解析报 Malformed
        let abc = sign_license(
            &kp,
            &[
                ("tier", "starter"),
                ("node_quota", "abc"),
                ("issued", "2026-01-01T00:00:00Z"),
                ("expires", &future_expires()),
            ],
        );
        assert!(matches!(
            verify_license_with_key(&abc, &kp.pk),
            Err(LicenseError::Malformed(_))
        ));
    }

    // ---------- 内置开发公钥端到端 ----------
    #[test]
    fn builtin_dev_fixture_end_to_end() {
        // 这枚 license 由 ed25519-compact（同一权威实现、自校验通过）以 fixture
        // 私钥 seed=0x00..0x1F 离线签发。该测试交叉验证：内置公钥常量 + 本模块
        // 规范化拼法与签发方逐字节一致（若拼法有出入，验签必失败）。
        let text = r#"# AgentMesh license fixture
tier = "starter"
node_quota = 15
issued = "2026-10-01T00:00:00Z"
expires = "2036-12-31T23:59:59Z"
node_family_prefix = "lan"
signature = "01qx+L3gOoXwdYa2o05/3qv8677f67r0uqwkOssx6HU+iL7Bgr6WOxWYw437KJQBlRJpyvBQu+MSVzQirG+BDw=="
"#;
        let info = verify_license(text).expect("内置 fixture 公钥应验过 fixture license");
        assert_eq!(info.tier, LicenseTier::Starter);
        assert_eq!(info.node_quota, 15);
    }

    #[test]
    fn canonical_bytes_are_lexicographic_lf_joined() {
        // 钉死规范化规范：字典序 + `key=value` + LF 分隔、无尾换行；signature 不参与
        let fields = vec![
            ("tier".to_string(), "starter".to_string()),
            ("node_quota".to_string(), "15".to_string()),
            ("issued".to_string(), "2026-10-01T00:00:00Z".to_string()),
            ("expires".to_string(), "2036-12-31T23:59:59Z".to_string()),
            ("node_family_prefix".to_string(), "lan".to_string()),
            ("signature".to_string(), "xxxx".to_string()),
        ];
        let body = canonical_signing_body(&fields);
        assert_eq!(
            String::from_utf8(body).unwrap(),
            "expires=2036-12-31T23:59:59Z\nissued=2026-10-01T00:00:00Z\nnode_family_prefix=lan\nnode_quota=15\ntier=starter"
        );
    }

    #[test]
    fn bare_date_expires_at_end_of_day() {
        // YYYY-MM-DD 兼容形态：expires 取裸日期 → 当日 23:59:59 UTC
        let kp = fixture_keypair();
        let text = sign_license(
            &kp,
            &[
                ("tier", "starter"),
                ("node_quota", "3"),
                ("issued", "2026-01-01"),
                ("expires", "2099-12-31"),
            ],
        );
        let info = verify_license_with_key(&text, &kp.pk).expect("裸日期 license 应通过");
        assert_eq!(
            info.expires,
            PrimitiveDateTime::new(
                Date::from_calendar_date(2099, Month::December, 31).unwrap(),
                Time::from_hms(23, 59, 59).unwrap()
            )
            .assume_utc()
        );
    }
}
