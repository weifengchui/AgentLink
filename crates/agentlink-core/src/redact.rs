// SPDX-License-Identifier: Apache-2.0
//! Observer-side payload redaction pipeline (v0.5).
//!
//! Scope contract: this module sanitizes the *human/audit view* of traffic —
//! the history ring and everything derived from it (`/v1/history`,
//! `/v1/history/search`, `/v1/history/export`, `/v1/events`, `/web`).
//! Frames forwarded or delivered to the target agent bypass it entirely:
//! the recipient always gets the original payload, observers get the
//! sanitized copy. This is the pre-condition for commercial audit trails.
//!
//! Built-in rule set (ordered; first match wins per rule):
//!   pem      — whole PRIVATE KEY block
//!   token    — `<key containing token|secret|api[-_]key> := value`
//!   password — `<key containing password|passwd|pwd> := value`
//!   hex      — standalone 32+ char hex blobs (hashes, key material)
//! Custom `redact.patterns` regexes run last and collapse to
//! `<REDACTED:custom>`.

use regex::Regex;
use serde_json::{Map, Value};

use crate::config::RedactConfig;

struct Rule {
    tag: &'static str,
    re: Regex,
    /// When true, the replacement keeps the leading named groups `$k$sep`
    /// (the key name and separator) and hides only the value.
    keep_key: bool,
}

pub struct Redactor {
    rules: Vec<Rule>,
}

impl Redactor {
    /// Compile the rule set once at boot. Invalid custom patterns are logged
    /// and skipped — a bad regex must never take the node down.
    pub fn from_config(cfg: &RedactConfig) -> Self {
        let mut rules: Vec<Rule> = Vec::new();
        if cfg.enabled {
            let builtins = [
                (
                    "pem",
                    r"(?s)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?-----END [A-Z0-9 ]*PRIVATE KEY-----",
                    false,
                ),
                (
                    "token",
                    r#"(?i)(?<k>[A-Za-z0-9_-]*(?:token|secret|api[-_]?key)[A-Za-z0-9_-]*)(?<sep>["' ]*[:=]["' ]*)"?[^\s"',;]\S*"#,
                    true,
                ),
                (
                    "password",
                    r#"(?i)(?<k>[A-Za-z0-9_-]*(?:password|passwd|pwd)[A-Za-z0-9_-]*)(?<sep>["' ]*[:=]["' ]*)"?[^\s"',;]\S*"#,
                    true,
                ),
                ("hex", r"\b[0-9a-fA-F]{32,}\b", false),
            ];
            for (tag, pat, keep_key) in builtins {
                match Regex::new(pat) {
                    Ok(re) => rules.push(Rule { tag, re, keep_key }),
                    Err(e) => {
                        tracing::error!(error = %e, rule = tag, "redact: builtin rule failed to compile")
                    }
                }
            }
        }
        if cfg.enabled {
            for pat in &cfg.patterns {
                match Regex::new(pat) {
                    Ok(re) => rules.push(Rule {
                        tag: "custom",
                        re,
                        keep_key: false,
                    }),
                    Err(e) => tracing::warn!(
                        error = %e, pattern = %pat,
                        "redact: custom pattern invalid, skipped"
                    ),
                }
            }
        }
        Redactor { rules }
    }

    /// Redaction effectively off (no rules): views pass payloads through.
    #[allow(dead_code)]
    pub fn disabled(&self) -> bool {
        self.rules.is_empty()
    }

    /// Redact one free-text string through every rule in order.
    pub fn redact_str(&self, s: &str) -> String {
        if self.rules.is_empty() || s.is_empty() {
            return s.to_string();
        }
        let mut cur = s.to_string();
        for r in &self.rules {
            let placeholder = format!("<REDACTED:{}>", r.tag);
            let replacement = if r.keep_key {
                format!("${{k}}${{sep}}{}", placeholder)
            } else {
                placeholder
            };
            if r.re.is_match(&cur) {
                cur = r.re.replace_all(&cur, replacement.as_str()).into_owned();
            }
        }
        cur
    }

    /// Recursively redact every string *value* in a JSON payload. Object keys
    /// are protocol field names and pass through untouched; nested objects and
    /// arrays are descended into. Non-string leaves are returned as-is.
    pub fn redact_value(&self, v: &Value) -> Value {
        if self.rules.is_empty() {
            return v.clone();
        }
        self.walk(v)
    }

    fn walk(&self, v: &Value) -> Value {
        match v {
            Value::String(s) => Value::String(self.redact_str(s)),
            Value::Array(items) => Value::Array(items.iter().map(|i| self.walk(i)).collect()),
            Value::Object(map) => {
                let mut out = Map::with_capacity(map.len());
                for (k, val) in map {
                    out.insert(k.clone(), self.walk(val));
                }
                Value::Object(out)
            }
            other => other.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn red() -> Redactor {
        Redactor::from_config(&RedactConfig::default())
    }

    #[test]
    fn token_value_hidden_key_visible() {
        let r = red();
        let out = r.redact_str("use X-Auth-Token: *** please");
        assert!(out.contains("<REDACTED:token>"), "{out}");
        assert!(!out.contains("abc123def456"), "{out}");
        assert!(out.contains("X-Auth-Token"), "{out}");
    }

    #[test]
    fn json_key_value_and_equals_forms() {
        let r = red();
        let v = json!({"content": "{\"api_key\": \"***\"}", "n": 3});
        let out = r.redact_value(&v);
        let s = out.get("content").unwrap().as_str().unwrap();
        assert!(s.contains("<REDACTED:token>"), "{s}");
        assert!(!s.contains("zzz"), "{s}");
        assert_eq!(out.get("n"), Some(&json!(3)));
        let eq = r.redact_str("secret=abcdef123456");
        assert!(eq.contains("<REDACTED:token>"), "{eq}");
    }

    #[test]
    fn pem_block_and_hex() {
        let r = red();
        let pem = "head\n-----BEGIN RSA PRIVATE KEY-----\nMIIBOgIBAAJ\n-----END RSA PRIVATE KEY-----\ntail";
        let out = r.redact_str(pem);
        assert!(out.contains("<REDACTED:pem>"), "{out}");
        assert!(!out.contains("MIIBOgIBAAJ"), "{out}");
        let hex64 = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef";
        assert_eq!(r.redact_str(hex64), "<REDACTED:hex>");
        // short hashes (uuid segments etc.) must survive
        let uuid = "a1b2c3d4-5e6f-4a7b-8c9d-0e1f2a3b4c5d";
        assert_eq!(r.redact_str(uuid), uuid);
    }

    #[test]
    fn custom_patterns_and_disable() {
        let r = Redactor::from_config(&RedactConfig {
            enabled: true,
            patterns: vec!["192\\.168\\.[0-9]+\\.[0-9]+".to_string(), "([".to_string()],
        });
        let out = r.redact_str("peer at 192.168.1.50 ok");
        assert!(out.contains("<REDACTED:custom>"), "{out}");
        let off = Redactor::from_config(&RedactConfig {
            enabled: false,
            patterns: vec!["Token".to_string()],
        });
        assert!(off.disabled());
        assert_eq!(off.redact_str("X-Auth-Token: ***"), "X-Auth-Token: ***");
    }
}
