//! 本机 CPU 利用率采样（v0.6 P1 Lease 差异上报的数据源）。
//!
//! 形态约束：单二进制、纯 Rust、不引入 C 依赖——Linux 用 `/proc/stat`
//! 原生采样；Windows/macOS 退化为探测命令（一条子进程 per 采样，
//! 仅在 `lease.enabled=true` 时被调用）。任何失败返回 `None`，
//! 上报层自动退化为「无负载字段的裸 lease 心跳」，链路活性不受影响。

use std::time::Duration;

/// 采样本机 CPU 利用率（0..=100 的整数四舍五入）。失败/平台不支持 → `None`。
pub fn cpu_percent() -> Option<u32> {
    #[cfg(target_os = "linux")]
    {
        proc_snapshot_percent(Duration::from_millis(200))
    }
    #[cfg(target_os = "windows")]
    {
        windows_probe_percent()
    }
    #[cfg(target_os = "macos")]
    {
        macos_probe_percent()
    }
    #[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
    {
        None
    }
}

// ---- Linux：/proc/stat 两读取差分 ------------------------------------------

/// 读一次 `/proc/stat` 首行 `cpu ...`，返回 `(total, idle)`（jiffies）。
fn proc_stat_sample() -> Option<(u64, u64)> {
    let text = std::fs::read_to_string("/proc/stat").ok()?;
    parse_proc_stat_line(text.lines().next()?)
}

/// 解析 `/proc/stat` 首行：`cpu user nice system idle iowait irq softirq steal …`。
/// 返回 `(total, idle)`，其中 idle = idle + iowait（Linux 惯例）。
fn parse_proc_stat_line(line: &str) -> Option<(u64, u64)> {
    let rest = line.strip_prefix("cpu ")?;
    let vals: Vec<u64> = rest
        .split_whitespace()
        .filter_map(|s| s.parse().ok())
        .collect();
    if vals.len() < 4 {
        return None;
    }
    let idle = vals[3].saturating_add(vals.get(4).copied().unwrap_or(0));
    let total: u64 = vals.iter().sum();
    Some((total, idle))
}

/// 两次读取差分得出利用率。`wait` 是两次读取的间隔（约 200ms）。
fn proc_snapshot_percent(wait: Duration) -> Option<u32> {
    let (t0, i0) = proc_stat_sample()?;
    std::thread::sleep(wait);
    let (t1, i1) = proc_stat_sample()?;
    Some(percent_from_delta(t0, i0, t1, i1))
}

/// 差分计算（纯函数，供单测）：`(1 - Δidle/Δtotal) × 100`，四舍五入。
fn percent_from_delta(t0: u64, i0: u64, t1: u64, i1: u64) -> u32 {
    let d_total = t1.saturating_sub(t0) as f64;
    let d_idle = i1.saturating_sub(i0) as f64;
    if d_total <= f64::EPSILON {
        return 0;
    }
    let busy_ratio = 1.0 - (d_idle / d_total);
    (busy_ratio.clamp(0.0, 1.0) * 100.0).round() as u32
}

// ---- Windows：wmic / PowerShell 探测 ----------------------------------------

#[cfg(target_os = "windows")]
fn windows_probe_percent() -> Option<u32> {
    // wmic 已被逐步弃置但仍广泛可用；失败再退 PowerShell（慢但稳）。
    if let Ok(pct) = probe_shell(&["wmic", "cpu", "get", "loadpercentage"], filter_int) {
        return Some(pct);
    }
    probe_shell(
        &[
            "powershell",
            "-NoProfile",
            "-Command",
            "(Get-CimInstance CIM_Processor).LoadPercentage",
        ],
        filter_int,
    )
}

// ---- macOS：top 单帧解析 ----------------------------------------------------

#[cfg(target_os = "macos")]
fn macos_probe_percent() -> Option<u32> {
    // `top -l 1 -n 0` 首屏 "CPU usage: 12.34% user, 5.67% sys, ..."。
    probe_shell(&["top", "-l", "1", "-n", "0"], |out| {
        let usage = out.split("CPU usage:").nth(1)?;
        let mut sum = 0.0f64;
        for part in usage.split(',') {
            let p = part.trim();
            if p.ends_with("% user") || p.ends_with("% sys") {
                let v: f64 = p.split('%').next()?.trim().parse().ok()?;
                sum += v;
            }
        }
        Some(sum.round() as u32)
    })
}

/// 跑一条探测命令，把 stdout 交给解析器。失败 → None，绝不 panic。
#[cfg(any(target_os = "windows", target_os = "macos"))]
fn probe_shell(cmd: &[&str], parse: fn(&str) -> Option<u32>) -> Option<u32> {
    use std::process::Command;
    let out = Command::new(cmd[0]).args(&cmd[1..]).output().ok()?;
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    parse(&text)
}

/// 从命令输出里捞第一个整数（wmic 表格带表头，须跳过）。
#[cfg(target_os = "windows")]
fn filter_int(out: &str) -> Option<u32> {
    let mut lines = out.lines().filter_map(|l| {
        let t = l.trim();
        if t.is_empty() || !t.starts_with(|c: char| c.is_ascii_digit()) {
            return None;
        }
        t.parse::<u32>().ok()
    });
    lines.next()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_proc_stat_head() {
        // 经典 k format：idle + iowait 计入 idle。
        let (t, i) = parse_proc_stat_line("cpu  100 0 100 200 50 0 0 0 0 0").expect("should parse");
        assert_eq!(t, 450);
        assert_eq!(i, 250);
    }

    #[test]
    fn rejects_short_or_foreign_lines() {
        assert!(parse_proc_stat_line("cpu  100 0 100").is_none()); // 缺 idle
        assert!(parse_proc_stat_line("cpu0 1 2 3 4").is_none()); // 非 cpu 聚合行
        assert!(parse_proc_stat_line("intr 12345").is_none());
        assert!(parse_proc_stat_line("").is_none());
    }

    #[test]
    fn percent_delta_math() {
        // 全忙：Δidle = 0 → 100%。
        assert_eq!(percent_from_delta(100, 50, 200, 50), 100);
        // 全闲：Δtotal == Δidle → 0%。
        assert_eq!(percent_from_delta(100, 50, 200, 150), 0);
        // 一般：Δtotal=100, Δidle=40 → 60 busy → 60%。
        assert_eq!(percent_from_delta(100, 40, 200, 80), 60);
        // 计数器回绕/重置：饱和为 0。
        assert_eq!(percent_from_delta(500, 100, 10, 5), 0);
    }
}
