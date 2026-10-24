// SPDX-License-Identifier: Apache-2.0
//! 首次运行 wizard。
//!
//! 设计目标：**别人第一次装就能跑通**。入口有两个——
//! * `agentlink setup`：显式向导；
//! * 裸 `agentlink`（默认 run）发现配置文件不存在且 stdin 是交互终端：
//!   自动进入向导（Windows 双击 exe 即走此路）。
//!
//! 非 TTY（autostart/systemd/管道）绝不进入交互，退回 init/join 既有路径，
//! 自动化流程零改变。向导只产出决策（名称/端口/令牌/模式/是否启动），
//! 配置的生成与落盘复用 main.rs 的 `generate_preserving_identity`——
//! 机器身份（node_id/data_dir/local token）永远保留（2026-10-13 事故教训）。

use std::io::{IsTerminal, Write};
use std::path::Path;

use anyhow::{bail, Result};

/// 向导收集的用户决策；字段为 None = 沿用默认/既有值。
pub struct WizardPlan {
    pub name: Option<String>,
    pub listen_port: Option<u16>,
    pub cluster_token: Option<String>,
    /// `Some("open")` = 信任局域网；None = strict（默认，生成集群令牌）。
    pub mode: Option<String>,
    pub start_now: bool,
}

pub fn is_interactive() -> bool {
    std::io::stdin().is_terminal()
}

fn ask(prompt: &str, default: &str) -> String {
    print!("{prompt}");
    let _ = std::io::stdout().flush();
    let mut line = String::new();
    let n = std::io::stdin().read_line(&mut line).unwrap_or(0);
    let t = line.trim();
    if n == 0 || t.is_empty() {
        default.to_string()
    } else {
        t.to_string()
    }
}

fn confirm(prompt: &str, default_yes: bool) -> bool {
    let hint = if default_yes { "[Y/n]" } else { "[y/N]" };
    let answer = ask(
        &format!("{prompt} {hint}: "),
        if default_yes { "y" } else { "n" },
    );
    matches!(answer.to_lowercase().as_str(), "y" | "yes" | "是")
}

/// 运行交互向导；返回 None = 用户在确认前放弃。
pub fn run_wizard(config_path: &Path, version: &str) -> Result<Option<WizardPlan>> {
    println!("欢迎使用 AgentLink {version} —— 局域网 AI Agent 消息总线。");
    println!("本向导把这台机器初始化成一个节点；所有选项之后都可以在配置文件里手改。");
    println!();

    // 已有配置：如实展示身份，说明将保留。
    let existing = config_path
        .to_path_buf()
        .exists()
        .then(|| crate::config::Config::load(config_path).ok())
        .flatten();
    let mut defaults = crate::config::Config::generate(None, None, None);
    if let Some(cfg) = &existing {
        println!(
            "检测到已有配置 {}：node_id={} name={}（机器身份会保留）",
            config_path.display(),
            cfg.node_id,
            cfg.name
        );
        defaults = cfg.clone();
        println!();
    }

    let role = ask(
        "选择角色： [1] 新建集群（本机为主节点）  [2] 加入现有集群（需主节点的集群令牌）  默认 1 : ",
        "1",
    );
    if role != "1" && role != "2" {
        bail!("无效选择: {role}（输入 1 或 2）");
    }

    let name = ask(&format!("节点名称 [{}]: ", defaults.name), &defaults.name);
    if name.trim().is_empty() {
        bail!("节点名称不能为空");
    }

    let port_str = ask(
        &format!("监听端口 [{}]: ", defaults.listen_port),
        &defaults.listen_port.to_string(),
    );
    let listen_port: u16 = match port_str.parse() {
        Ok(p) => p,
        Err(_) => {
            println!("(端口无效，沿用 {})", defaults.listen_port);
            defaults.listen_port
        }
    };

    let (cluster_token, mode) = match role.as_str() {
        "2" => {
            println!("在主节点机器上运行 `agentlink token` 可取得集群令牌。");
            let mut token = ask("请粘贴集群令牌: ", "").trim().to_string();
            if token.is_empty() {
                token = ask("令牌为空，再粘贴一次（直接回车放弃）: ", "")
                    .trim()
                    .to_string();
            }
            if token.is_empty() {
                bail!("未提供集群令牌，无法加入集群（可稍后手动运行 agentlink join <TOKEN>）");
            }
            (Some(token), None)
        }
        _ => {
            let sec = ask(
                "安全模式： [1] 令牌鉴权（推荐，跨机需集群令牌）  [2] open（信任整个局域网）  默认 1 : ",
                "1",
            );
            match sec.as_str() {
                "2" => (None, Some("open".to_string())),
                "1" => (None, None),
                other => bail!("无效选择: {other}（输入 1 或 2）"),
            }
        }
    };

    // --config 指定的路径若不在当前目录，提示一下（默认名 agentlink.json）。
    if let Some(parent) = config_path.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            println!("(将自动创建目录 {})", parent.display());
        }
    }
    println!();
    if !confirm(
        &format!(
            "即将写入配置： name={name} port={listen_port} {}",
            match (&cluster_token, &mode) {
                (Some(_), _) => "join(令牌鉴权)",
                (None, Some(m)) => m.as_str(),
                (None, None) => "新建集群(令牌鉴权)",
            }
        ),
        true,
    ) {
        println!("已取消，未写入任何文件。");
        return Ok(None);
    }

    let start_now = confirm("现在启动这个节点吗？", true);

    Ok(Some(WizardPlan {
        name: Some(name),
        listen_port: Some(listen_port),
        cluster_token,
        mode,
        start_now,
    }))
}
