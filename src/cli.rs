//! CLI 薄壳（v0.5 路线 §3.3）：`agentlink get/logs/apply` 是纯 REST 包装。
//!
//! 设计约束：这些子命令**不启动节点、不读写本地总线存储**，只访问本机
//! local API（`GET /v1/*`、`POST /v1/send`，鉴权头 `X-Auth-Token`）。
//! 默认寻址 `http://127.0.0.1:<--config 里的 listen_port>`，token 默认取
//! 同一配置的 `local_api.token`；`--url` / `--token` 可分别覆盖，两者同时
//! 给出时完全不读配置文件。输出为定宽表格/逐行文本，供人和脚本消费。

use std::path::Path;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

// ---- clap 形状（由 main.rs 的 Command 枚举引用） ----------------------------

/// `get` 子命令族：只读视图查询。
#[derive(Debug, clap::Subcommand)]
pub enum GetTarget {
    /// 节点清单（/v1/health 本机 + /v1/peers 对端汇总）
    Nodes,
    /// 舰队 agent 清单（/v1/fleet/agents；--requires 服务端原生过滤）
    Agents {
        /// 能力需求，逗号分隔（如 `cap:file,cap:shell:safe`）
        #[arg(long)]
        requires: Option<String>,
        /// 只显示在线 agent（客户端过滤）
        #[arg(long)]
        online: bool,
    },
    /// 任务视图（/v1/tasks，history 环派生投影）
    Tasks {
        /// 按 task_spec.correlation 精确过滤
        #[arg(long)]
        correlation: Option<String>,
        /// 状态过滤，逗号分隔（如 `running,succeeded`）
        #[arg(long)]
        state: Option<String>,
    },
}

/// `logs` 参数：读 `/v1/events`（脱敏后的观察者视图，v0.5 §1/§2）。
#[derive(Debug, clap::Args)]
pub struct LogsArgs {
    /// 只看涉及该 agent 的事件（from_agent 或 to_agent）
    #[arg(long)]
    agent: Option<String>,
    /// 帧类型过滤，逗号分隔（如 `text,file`；ctrl/heartbeat 不在视图内）
    #[arg(long)]
    types: Option<String>,
    /// 单次最多返回条数（默认服务端 500）
    #[arg(long)]
    limit: Option<usize>,
    /// 从该 seq 起增量读取（配合 --follow 断点续传）
    #[arg(long)]
    since: Option<u64>,
    /// 持续跟随：按 next_seq 增量轮询，Ctrl-C 干净退出
    #[arg(long)]
    follow: bool,
}

/// `apply` 参数：把一个 task_spec 经 `POST /v1/send` 投入总线。
#[derive(Debug, clap::Args)]
pub struct ApplyArgs {
    /// task_spec JSON 文件（docs/task-protocol.md §2 形状）
    #[arg(short = 'f', long)]
    file: std::path::PathBuf,
    /// 目标节点（node_id 或名称；默认本机节点）
    #[arg(long)]
    to: Option<String>,
    /// 目标 agent id（必填；执行方按协议回报状态）
    #[arg(long)]
    to_agent: String,
    /// 发起方 agent id（默认 `cli`；执行方回报会发给它）
    #[arg(long, default_value = "cli")]
    from_agent: String,
    /// 消息组线程键（v0.5.8 R2）：同 thread 的多回合消息在星图归为一个话题组
    #[arg(long)]
    thread: Option<String>,
    /// 只打印将要 POST 的 /v1/send 请求体，不实际发送
    #[arg(long)]
    dry_run: bool,
}

// ---- HTTP 小客户端 ----------------------------------------------------------

struct Api {
    base: String,
    token: String,
    http: reqwest::Client,
}

impl Api {
    /// 解析 --url/--token 覆盖与 --config 默认值；两者都覆盖时不读配置。
    async fn connect(url: Option<&str>, token: Option<&str>, config_path: &Path) -> Result<Api> {
        let (base, tok) = match (url, token) {
            (Some(u), Some(t)) => (u.trim_end_matches('/').to_string(), t.to_string()),
            _ => {
                // load_config 对缺失文件报「run `agentlink init`」友好错误。
                let cfg = crate::load_config(config_path, false)?;
                let base = url
                    .map(|u| u.trim_end_matches('/').to_string())
                    .unwrap_or_else(|| format!("http://127.0.0.1:{}", cfg.listen_port));
                let tok = token
                    .map(|t| t.to_string())
                    .unwrap_or_else(|| cfg.local_api.token.clone());
                (base, tok)
            }
        };
        Ok(Api {
            base,
            token: tok,
            http: reqwest::Client::new(),
        })
    }

    async fn get(&self, path: &str, query: &[(&str, String)]) -> Result<Value> {
        let mut req = self
            .http
            .get(format!("{}{}", self.base, path))
            .header("x-auth-token", self.token.clone());
        if !query.is_empty() {
            req = req.query(query);
        }
        let resp = req.send().await.map_err(connect_err(&self.base))?;
        self.done(resp, path).await
    }

    /// 同 [`Api::get`]，但把状态码原样带回（logs --follow 要识别 409 轮换）。
    async fn get_status(&self, path: &str, query: &[(&str, String)]) -> Result<(u16, Value)> {
        let mut req = self
            .http
            .get(format!("{}{}", self.base, path))
            .header("x-auth-token", self.token.clone());
        if !query.is_empty() {
            req = req.query(query);
        }
        let resp = req.send().await.map_err(connect_err(&self.base))?;
        let status = resp.status().as_u16();
        let body = decode(resp, path).await?;
        Ok((status, body))
    }

    async fn post(&self, path: &str, body: &Value) -> Result<Value> {
        let resp = self
            .http
            .post(format!("{}{}", self.base, path))
            .header("x-auth-token", self.token.clone())
            .json(body)
            .send()
            .await
            .map_err(connect_err(&self.base))?;
        self.done(resp, path).await
    }

    /// 非 2xx 转成带提示的错误；401 单独提醒 token 来源。
    async fn done(&self, resp: reqwest::Response, path: &str) -> Result<Value> {
        let status = resp.status().as_u16();
        let body = decode(resp, path).await?;
        if (200..400).contains(&status) {
            return Ok(body);
        }
        if status == 401 {
            bail!(
                "{path} → HTTP 401 未授权：检查 --token，或确认 {config} 里 local_api.token 与运行节点一致",
                config = "当前 --config"
            );
        }
        bail!("{} → HTTP {}: {}", path, status, compact(&body, 300));
    }
}

fn connect_err(base: &str) -> impl Fn(reqwest::Error) -> anyhow::Error + '_ {
    move |e| {
        anyhow::anyhow!(
            "无法连接 {}（{}）：节点是否在运行？（agentlink run；或用 --url 指定地址）",
            base,
            e
        )
    }
}

async fn decode(resp: reqwest::Response, path: &str) -> Result<Value> {
    let text = resp
        .text()
        .await
        .with_context(|| format!("读取 {path} 响应失败"))?;
    Ok(serde_json::from_str::<Value>(&text)
        .unwrap_or_else(|_| json!({ "_raw": truncate_chars(&text, 400) })))
}

// ---- 命令实现 ----------------------------------------------------------------

/// `agentlink get nodes|agents|tasks`
pub async fn get(
    target: GetTarget,
    url: Option<&str>,
    token: Option<&str>,
    config: &Path,
) -> Result<()> {
    let api = Api::connect(url, token, config).await?;
    match target {
        GetTarget::Nodes => {
            let health = api.get("/v1/health", &[]).await?;
            let peers = api.get("/v1/peers", &[]).await?;
            let addr = api.base.split("://").nth(1).unwrap_or("").to_string();
            let mut rows = vec![vec![
                s(&health, "name"),
                s(&health, "node_id"),
                addr,
                "在线".to_string(),
                s(&health, "version"),
            ]];
            for p in peers
                .get("peers")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
            {
                rows.push(vec![
                    s(&p, "name"),
                    s(&p, "node_id"),
                    s(&p, "addr"),
                    if p.get("online").and_then(Value::as_bool) == Some(true) {
                        "在线".to_string()
                    } else {
                        "离线".to_string()
                    },
                    // /v1/peers 帧里没有对端版本；薄壳不猜。
                    "-".to_string(),
                ]);
            }
            render_table(&["name", "node_id", "address", "在线", "版本"], &rows);
        }
        GetTarget::Agents { requires, online } => {
            let mut query: Vec<(&str, String)> = Vec::new();
            if let Some(r) = requires.as_deref().filter(|r| !r.trim().is_empty()) {
                query.push(("requires", r.to_string()));
            }
            let resp = api.get("/v1/fleet/agents", &query).await?;
            let mut rows: Vec<Vec<String>> = Vec::new();
            let mut online_n = 0usize;
            for a in resp
                .get("agents")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
            {
                let is_on = a.get("online").and_then(Value::as_bool) == Some(true);
                if is_on {
                    online_n += 1;
                }
                if online && !is_on {
                    continue;
                }
                rows.push(vec![
                    s(&a, "agent_id"),
                    s_or(&a, "name", "-"),
                    a.get("capabilities")
                        .and_then(Value::as_array)
                        .map(|v| {
                            v.iter()
                                .filter_map(Value::as_str)
                                .collect::<Vec<_>>()
                                .join(",")
                        })
                        .unwrap_or_default(),
                    if is_on {
                        "在线".to_string()
                    } else {
                        "离线".to_string()
                    },
                    s_or(&a, "node_name", &s(&a, "node_id")),
                ]);
            }
            render_table(&["agent_id", "name", "capabilities", "在线", "node"], &rows);
            println!("{} 个 agent（在线 {}）", rows.len(), online_n);
        }
        GetTarget::Tasks { correlation, state } => {
            let mut query: Vec<(&str, String)> = Vec::new();
            if let Some(c) = correlation.as_deref().filter(|c| !c.trim().is_empty()) {
                query.push(("correlation", c.to_string()));
            }
            if let Some(st) = state.as_deref().filter(|s| !s.trim().is_empty()) {
                query.push(("state", st.to_string()));
            }
            let resp = api.get("/v1/tasks", &query).await?;
            // TaskView 内核投影没有独立节点列：target=to_agent、from=from_agent，
            // 批次以 corr(correlation) 列呈现。
            let rows: Vec<Vec<String>> = resp
                .get("tasks")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default()
                .iter()
                .map(|t| {
                    vec![
                        s(t, "task_id"),
                        s_or(t, "type", "-"),
                        s(t, "state"),
                        s_or(t, "to_agent", "-"),
                        s_or(t, "from_agent", "-"),
                        s_or(t, "correlation", "-"),
                        s(t, "last_ts"),
                    ]
                })
                .collect();
            render_table(
                &[
                    "task_id", "type", "state", "target", "from", "corr", "last_ts",
                ],
                &rows,
            );
            println!(
                "显示 {} / 共 {} 个任务",
                rows.len(),
                resp.get("total").and_then(Value::as_u64).unwrap_or(0)
            );
        }
    }
    Ok(())
}

/// `agentlink logs [--follow]`
pub async fn logs(
    opts: LogsArgs,
    url: Option<&str>,
    token: Option<&str>,
    config: &Path,
) -> Result<()> {
    let api = Api::connect(url, token, config).await?;
    if opts.follow {
        follow(&api, &opts).await
    } else {
        let events = fetch_events(&api, &opts, opts.since.unwrap_or(0)).await?;
        for ev in &events {
            println!("{}", format_event(ev));
        }
        println!("(共 {} 条；--follow 持续跟随)", events.len());
        Ok(())
    }
}

fn logs_query(opts: &LogsArgs, since: u64) -> Vec<(&'static str, String)> {
    let mut q = vec![("since", since.to_string())];
    if let Some(a) = opts.agent.as_deref().filter(|a| !a.trim().is_empty()) {
        q.push(("agent", a.to_string()));
    }
    if let Some(t) = opts.types.as_deref().filter(|t| !t.trim().is_empty()) {
        q.push(("types", t.to_string()));
    }
    if let Some(l) = opts.limit {
        q.push(("limit", l.to_string()));
    }
    q
}

async fn fetch_events(api: &Api, opts: &LogsArgs, since: u64) -> Result<Vec<Value>> {
    let body = api.get("/v1/events", &logs_query(opts, since)).await?;
    Ok(body
        .get("events")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default())
}

async fn follow(api: &Api, opts: &LogsArgs) -> Result<()> {
    // 基线：先做一次全量（受 limit 约束）拉取并打印，水位记在 next_seq。
    let (status, first) = api
        .get_status("/v1/events", &logs_query(opts, opts.since.unwrap_or(0)))
        .await?;
    let mut since = handle_gap(status, &first, opts.since.unwrap_or(0));
    for ev in first
        .get("events")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
    {
        println!("{}", format_event(&ev));
    }
    if status == 200 {
        since = first
            .get("next_seq")
            .and_then(Value::as_u64)
            .unwrap_or(since);
    }
    println!("[logs] 跟随中，seq>={}（Ctrl-C 退出）", since + 1);
    let mut gap_handled = status != 409;
    loop {
        // 命中即提速到 200ms，空轮回到 500ms 基线。
        let mut wait_ms = 500u64;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\n[logs] 已退出跟随");
                return Ok(());
            }
            _ = tokio::time::sleep(Duration::from_millis(wait_ms)) => {}
        }
        loop {
            let (status, body) = api
                .get_status("/v1/events", &logs_query(opts, since))
                .await?;
            if status == 409 {
                // 环已轮换、since 掉队：薄壳不重建全史，从最旧可用 seq 续读。
                let oldest = body.get("oldest_seq").and_then(Value::as_u64).unwrap_or(0);
                if !gap_handled {
                    println!("[logs] 事件环已轮换（since={since} 掉出窗口），从 oldest_seq={oldest} 续读");
                    gap_handled = true;
                }
                since = oldest;
                continue;
            }
            gap_handled = false;
            let events = body
                .get("events")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            for ev in &events {
                println!("{}", format_event(ev));
            }
            since = body
                .get("next_seq")
                .and_then(Value::as_u64)
                .unwrap_or(since);
            if events.is_empty() {
                break;
            }
            wait_ms = 200;
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    println!("\n[logs] 已退出跟随");
                    return Ok(());
                }
                _ = tokio::time::sleep(Duration::from_millis(wait_ms)) => {}
            }
        }
    }
}

/// 409（since 掉出环）时返回重建起点；其余原样返回。
fn handle_gap(status: u16, body: &Value, since: u64) -> u64 {
    if status == 409 {
        body.get("oldest_seq").and_then(Value::as_u64).unwrap_or(0)
    } else {
        since
    }
}

/// `/v1/events` 的单条 `{seq,ts,frame}` → 一行文本。payload 是脱敏件，如实打印。
fn format_event(ev: &Value) -> String {
    let f = ev.get("frame").cloned().unwrap_or(json!({}));
    let seq = f.get("seq").and_then(Value::as_u64).unwrap_or(0);
    let who = |side: &str| -> String {
        let node = f
            .get(side)
            .and_then(Value::as_str)
            .unwrap_or("?")
            .to_string();
        match f.get(format!("{side}_agent")).and_then(Value::as_str) {
            Some(a) if !a.is_empty() => format!("{node}/{a}"),
            _ => node,
        }
    };
    let kind = f.get("kind").and_then(Value::as_str).unwrap_or("?");
    let dir = f.get("direction").and_then(Value::as_str).unwrap_or("?");
    let ts = f.get("ts").and_then(Value::as_str).unwrap_or("");
    format!(
        "#{seq:<6} {ts:<33} [{dir:<5}] {} -> {}  {kind}  {}",
        who("from"),
        who("to"),
        payload_summary(&f.get("payload").cloned().unwrap_or(json!({})))
    )
}

fn payload_summary(p: &Value) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(t) = p.get("topic").and_then(Value::as_str) {
        parts.push(format!("topic={t}"));
    }
    if let Some(id) = p.get("task_id").and_then(Value::as_str) {
        parts.push(format!("task_id={}", short_id(id)));
    }
    for k in ["stage", "state", "reason", "note", "seq"] {
        if let Some(v) = p.get(k) {
            if !v.is_null() {
                if k == "seq" {
                    continue; // 帧级 seq 已在行首
                }
                parts.push(format!(
                    "{k}={}",
                    v.as_str()
                        .map(str::to_string)
                        .unwrap_or_else(|| compact(v, 40))
                ));
            }
        }
    }
    if let Some(c) = p.get("content").and_then(Value::as_str) {
        parts.push(truncate_chars(c, 80));
    }
    if parts.is_empty() {
        return compact(p, 100);
    }
    parts.join(" ")
}

// ---- apply ------------------------------------------------------------------

/// `agentlink apply -f task.json [--dry-run]`
pub async fn apply(
    opts: ApplyArgs,
    url: Option<&str>,
    token: Option<&str>,
    config: &Path,
) -> Result<()> {
    let raw = std::fs::read_to_string(&opts.file)
        .with_context(|| format!("读取 task_spec 文件失败: {}", opts.file.display()))?;
    let mut spec: Value = serde_json::from_str(&raw)
        .with_context(|| format!("{} 不是合法 JSON", opts.file.display()))?;
    let obj = spec
        .as_object_mut()
        .context("task_spec 必须是 JSON 对象（协议 §2 形状）")?;
    obj.entry("topic").or_insert_with(|| json!("task_spec"));
    obj.entry("task_id")
        .or_insert_with(|| json!(uuid::Uuid::new_v4().to_string()));
    obj.entry("seq").or_insert_with(|| json!(1));
    let task_id = obj
        .get("task_id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();

    let api = Api::connect(url, token, config).await?;
    let health = api.get("/v1/health", &[]).await?;
    let node_id = health
        .get("node_id")
        .and_then(Value::as_str)
        .context("/v1/health 缺少 node_id")?
        .to_string();
    let body = json!({
        "from": node_id,
        "to": opts.to.as_deref().unwrap_or(&node_id),
        "from_agent": opts.from_agent,
        "to_agent": opts.to_agent,
        "type": "text",
        "action": "send",
        "thread": opts.thread.as_deref(),
        "payload": spec,
    });

    if opts.dry_run {
        println!("[dry-run] POST {}/v1/send", api.base);
        println!("{}", serde_json::to_string_pretty(&body)?);
        return Ok(());
    }
    let resp = api.post("/v1/send", &body).await?;
    println!(
        "已提交 task_spec task_id={task_id} → {}@{}",
        opts.to_agent,
        body.get("to").and_then(Value::as_str).unwrap_or("?")
    );
    println!(
        "msg_id: {}",
        resp.get("msg_id").and_then(Value::as_str).unwrap_or("")
    );
    println!("ack: {}", compact(&resp, 200));
    Ok(())
}

// ---- hello ------------------------------------------------------------------

/// `agentlink hello` 参数（v0.5 收官：首次安装自检）。
#[derive(Debug, clap::Args)]
pub struct HelloArgs {
    /// 只探测指定节点（node_id 或名称，可重复）；默认=本机自身+全部已知对端
    #[arg(long = "node")]
    node: Vec<String>,
    /// 等待应答的总时长（秒），默认 10
    #[arg(long, default_value_t = 10)]
    timeout: u64,
    /// 输出 JSON（机器可读：{ok,total,pass,nodes:[…]}）
    #[arg(long)]
    json: bool,
    /// 不探测本机自身
    #[arg(long)]
    no_self: bool,
}

struct HelloProbe {
    node_id: String,
    name: String,
    addr: String,
    online: bool,
    is_self: bool,
    /// 已发出探针的 msg_id；发送失败为 None。
    msg_id: Option<String>,
    sent: Option<std::time::Instant>,
    /// 收到的 hello_reply 帧 payload；超时/失败为 None。
    reply: Option<Value>,
    rtt_ms: Option<u128>,
    error: Option<String>,
}

/// `agentlink hello`：向总线里每个已知节点发一枚 `__node__` hello 探针，
/// 聚合应答（版本/RTT/在线态），一张表判断"这套安装是否真的通了"。
/// 纯 local API 消费者（同 get/logs/apply），内核零启动依赖——但要求本机
/// 节点在跑，否则连接失败即报错。
pub async fn hello(
    opts: HelloArgs,
    url: Option<&str>,
    token: Option<&str>,
    config: &Path,
) -> Result<()> {
    let api = Api::connect(url, token, config).await?;
    let health = api.get("/v1/health", &[]).await?;
    let self_id = s(&health, "node_id");

    // 目标清单：自身 + /v1/peers；--node 过滤（node_id 前缀或名称精确）。
    let peers = api.get("/v1/peers", &[]).await?;
    let mut probes: Vec<HelloProbe> = Vec::new();
    if !opts.no_self && target_selected(&opts.node, &self_id, &s(&health, "name")) {
        probes.push(HelloProbe {
            node_id: self_id.clone(),
            name: s(&health, "name"),
            addr: api.base.clone(),
            online: true,
            is_self: true,
            msg_id: None,
            sent: None,
            reply: None,
            rtt_ms: None,
            error: None,
        });
    }
    for p in peers
        .get("peers")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
    {
        let (id, name) = (s(&p, "node_id"), s(&p, "name"));
        if !target_selected(&opts.node, &id, &name) {
            continue;
        }
        probes.push(HelloProbe {
            node_id: id,
            name,
            addr: s_or(&p, "addr", "-"),
            online: p.get("online").and_then(Value::as_bool) == Some(true),
            is_self: false,
            msg_id: None,
            sent: None,
            reply: None,
            rtt_ms: None,
            error: None,
        });
    }
    if probes.is_empty() {
        println!("(没有可探测的节点：总线里只有自己，先 `agentlink join` 或在另一台机器起节点)");
        return Ok(());
    }

    // 熔断提示：hello_reply 是业务帧，lockdown 期间会被出口拦截。
    let lockdown_on = api
        .get("/v1/lockdown", &[])
        .await
        .map(|b| b.get("on").and_then(Value::as_bool) == Some(true))
        .unwrap_or(false);

    // 事件水位基线：只看发出探针之后到达的 hello_reply。
    let ev0 = api.get("/v1/events", &[("limit", "1".into())]).await?;
    let mut since = ev0.get("next_seq").and_then(Value::as_u64).unwrap_or(0);

    // 发探针（离线对端照常发：入离线队列，多半超时，表里如实呈现）。
    for pr in probes.iter_mut() {
        let body = json!({
            "to": pr.node_id,
            "type": "text",
            "action": "hello",
            "from_agent": "cli",
            "to_agent": crate::router::HELLO_AGENT,
            "payload": { "probe": pr.node_id },
        });
        let sent_at = std::time::Instant::now();
        match api.post("/v1/send", &body).await {
            Ok(resp) => {
                pr.msg_id = Some(s(&resp, "msg_id"));
                pr.sent = Some(sent_at);
                if pr.msg_id.is_none() {
                    pr.error = Some("应答缺少 msg_id".to_string());
                }
            }
            Err(e) => {
                let msg = e.to_string();
                pr.error = Some(if msg.contains("lockdown") {
                    "熔断中：业务帧被本机出口拦截".to_string()
                } else {
                    truncate_chars(&msg, 60)
                });
            }
        }
    }

    // 轮询事件环直到全部命中或超时。
    let deadline = std::time::Instant::now() + Duration::from_secs(opts.timeout.max(1));
    loop {
        if probes
            .iter()
            .all(|p| p.reply.is_some() || p.msg_id.is_none())
        {
            break;
        }
        if std::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        let body = api
            .get(
                "/v1/events",
                &[
                    ("since", since.to_string()),
                    ("types", "text".into()),
                    ("limit", "1000".into()),
                ],
            )
            .await?;
        if let Some(arr) = body.get("events").and_then(Value::as_array) {
            for ev in arr {
                let f = ev.get("frame").cloned().unwrap_or(json!({}));
                if f.get("action").and_then(Value::as_str) != Some("hello_reply") {
                    continue;
                }
                let rt = f.get("reply_to").and_then(Value::as_str);
                let Some(probe_id) = f
                    .get("payload")
                    .and_then(|p| p.get("probe"))
                    .and_then(Value::as_str)
                else {
                    continue;
                };
                for pr in probes.iter_mut() {
                    let hit = rt.map(|r| Some(r) == pr.msg_id.as_deref()).unwrap_or(false)
                        || probe_id == pr.node_id;
                    if pr.reply.is_none() && pr.msg_id.is_some() && hit {
                        pr.reply = f.get("payload").cloned();
                        pr.rtt_ms = pr.sent.map(|t| t.elapsed().as_millis());
                        break;
                    }
                }
            }
        }
        since = body
            .get("next_seq")
            .and_then(Value::as_u64)
            .unwrap_or(since);
    }

    let pass = probes.iter().filter(|p| p.reply.is_some()).count();
    let total = probes.len();

    if opts.json {
        let nodes: Vec<Value> = probes
            .iter()
            .map(|p| {
                json!({
                    "node_id": p.node_id,
                    "name": p.name,
                    "addr": p.addr,
                    "self": p.is_self,
                    "online": p.online,
                    "status": if p.reply.is_some() { "ok" } else { "fail" },
                    "rtt_ms": p.rtt_ms,
                    "version": p.reply.as_ref().and_then(|r| r.get("version")).and_then(Value::as_str),
                    "protocol": p.reply.as_ref().and_then(|r| r.get("protocol")),
                    "uptime_seconds": p.reply.as_ref().and_then(|r| r.get("uptime_seconds")),
                    "error": p.error,
                })
            })
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "ok": pass == total, "pass": pass, "total": total,
                "lockdown": lockdown_on, "nodes": nodes,
            }))?
        );
    } else {
        if lockdown_on {
            println!("(提示：本机熔断开启中，hello_reply 属业务帧会被拦截，超时属预期)");
        }
        let rows: Vec<Vec<String>> = probes
            .iter()
            .map(|p| {
                let (status, rtt) = match (&p.reply, p.rtt_ms) {
                    (Some(_), ms) => ("OK".to_string(), format!("{}ms", ms.unwrap_or(0))),
                    (None, _) => (
                        "超时".to_string(),
                        if p.msg_id.is_some() && !p.online && !p.is_self {
                            "(离线，已入离线队列)".to_string()
                        } else {
                            String::new()
                        },
                    ),
                };
                vec![
                    status,
                    if p.is_self {
                        format!("{}(本机)", p.name)
                    } else {
                        p.name.clone()
                    },
                    short_id(&p.node_id).to_string(),
                    rtt,
                    p.reply
                        .as_ref()
                        .and_then(|r| r.get("version"))
                        .and_then(Value::as_str)
                        .unwrap_or("-")
                        .to_string(),
                    p.error.clone().unwrap_or_default(),
                ]
            })
            .collect();
        render_table(&["状态", "名称", "node_id", "RTT", "版本", "备注"], &rows);
    }

    // --json 模式下 stdout 必须保持纯 JSON（供脚本消费），摘要走 stderr；退出码表达成败。
    if opts.json {
        if pass != total {
            eprintln!("hello 自检未过：{}/{} 节点应答", pass, total);
            std::process::exit(1);
        }
        return Ok(());
    }
    if pass == total {
        println!("hello 自检通过：{}/{} 节点应答", pass, total);
        Ok(())
    } else {
        println!("hello 自检未过：{}/{} 节点应答", pass, total);
        std::process::exit(1);
    }
}

/// --node 过滤：空=全选；node_id 全匹配或前缀≥4 位、名称精确匹配。
fn target_selected(filter: &[String], node_id: &str, name: &str) -> bool {
    if filter.is_empty() {
        return true;
    }
    filter
        .iter()
        .any(|f| node_id == f || (f.len() >= 4 && node_id.starts_with(f.as_str())) || name == f)
}

// ---- 表格渲染 ---------------------------------------------------------------

fn s(v: &Value, key: &str) -> String {
    v.get(key).and_then(Value::as_str).unwrap_or("").to_string()
}

fn s_or(v: &Value, key: &str, fallback: &str) -> String {
    let got = v.get(key).and_then(Value::as_str).unwrap_or("");
    if got.is_empty() {
        fallback.to_string()
    } else {
        got.to_string()
    }
}

fn short_id(id: &str) -> &str {
    let mut end = id.len().min(8);
    while end < id.len() && !id.is_char_boundary(end) {
        end -= 1;
    }
    &id[..end]
}

/// 终端显示宽度近似：CJK/全角按 2 列算。
fn term_width(s: &str) -> usize {
    s.chars()
        .map(|c| if (c as u32) > 0x2e80 { 2 } else { 1 })
        .sum()
}

fn truncate_chars(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut end = 0usize;
    for (i, _) in s.char_indices().take(max.saturating_sub(1)) {
        end = i + s[i..].chars().next().unwrap().len_utf8();
    }
    format!("{}…", &s[..end])
}

fn compact(v: &Value, max: usize) -> String {
    truncate_chars(&v.to_string(), max)
}

fn render_table(headers: &[&str], rows: &[Vec<String>]) {
    if rows.is_empty() {
        println!("(无匹配项)");
        return;
    }
    let ncol = headers.len();
    let mut widths: Vec<usize> = headers.iter().map(|h| term_width(h)).collect();
    for r in rows {
        for (i, cell) in r.iter().enumerate().take(ncol) {
            widths[i] = widths[i].max(term_width(cell));
        }
    }
    // 最后一列不补齐（避免行尾空白）。
    let pad = |s: &str, w: usize| format!("{}{}", s, " ".repeat(w.saturating_sub(term_width(s))));
    let line = |cells: &[String]| -> String {
        let mut out = String::new();
        for (i, cell) in cells.iter().enumerate().take(ncol) {
            if i + 1 == ncol {
                out.push_str(cell);
            } else {
                out.push_str(&pad(cell, widths[i]));
                out.push_str("  ");
            }
        }
        out
    };
    println!(
        "{}",
        line(&headers.iter().map(|h| h.to_string()).collect::<Vec<_>>())
    );
    for r in rows {
        println!("{}", line(r));
    }
}
