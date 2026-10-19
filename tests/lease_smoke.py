#!/usr/bin/env python3
"""Lease 心跳 + 阈值差异上报冒烟（v0.6 P1）：隔离端口，绝不碰生产 8765。

场景：
  S0 缺省（lease 关）：双节点互通，/v1/peers **无** load key（加性字段缺省
     不存在，默认态 JSON 与 0.5.9 逐字节一致），无 lease 日志（任务未 spawn）。
  S1 启用：双节点 lease.interval_secs=2 → 数拍内对端负载可见
     （/v1/peers load.cpu ∈ 0..=100）；/v1/metrics 出现
     agentlink_peer_cpu_percent 系列（Admin）。
  S2 健康与活性不受影响：/v1/health 仍通、对端持续 online（裸心跳/会话心跳维持）。

退出码 0 = 全过。用法: python3 tests/lease_smoke.py <agentlink-binary>
"""
import json, os, shutil, signal, socket, subprocess, sys, time, urllib.request

BIN = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser(
    "~/agentlink-target/release/agentlink")
assert os.path.exists(BIN), f"binary not found: {BIN}"

SCRATCH = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch")
ROOT = os.path.join(SCRATCH, "al-lease")
PORT_A, PORT_B = 18931, 18941
TOKEN = "lease-token"
NODE_A, NODE_B = "aaaaaaaaaaaa", "bbbbbbbbbbbb"
CLUSTER = "lease-cluster"

passed = failed = 0
def check(name, ok, detail=""):
    global passed, failed
    if ok:
        passed += 1
        print(f"PASS {name}")
    else:
        failed += 1
        print(f"FAIL {name}  {detail[:240]}")


def api(base, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data,
        headers={"X-Auth-Token": TOKEN, "Content-Type": "application/json"},
        method="POST" if body is not None else "GET")
    with urllib.request.urlopen(req, timeout=8) as r:
        return r.status, json.loads(r.read().decode())


def wait_health(base, tries=60):
    for _ in range(tries):
        try:
            with urllib.request.urlopen(base + "/v1/health", timeout=2) as r:
                if r.status == 200:
                    return True
        except Exception:
            time.sleep(0.25)
    return False


def port_free(port):
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) != 0


def write_cfg(key, peers, lease):
    port = PORT_A if key == "a" else PORT_B
    cfg = {
        "v": 1, "node_id": NODE_A if key == "a" else NODE_B,
        "name": f"lease-{key}", "listen_port": port,
        "data_dir": os.path.join(ROOT, key),
        "history": {"enabled": True, "len": 200, "file": "history.jsonl"},
        "discovery": {"enabled": False, "static_peers": list(peers)},
        "security": {"mode": "strict", "cluster_token": CLUSTER},
        "local_api": {"host": "0.0.0.0", "token": TOKEN},
        "web": {"auto_open": False}, "log": {"level": "info", "format": "text"},
    }
    if lease:
        cfg["lease"] = {"enabled": True, "interval_secs": 2,
                        "cpu_threshold_pct": 3, "bare_beat_secs": 10}
    json.dump(cfg, open(os.path.join(ROOT, f"{key}.json"), "w"))


def peer_of(key, node_id):
    base = f"http://127.0.0.1:{PORT_A if key == 'a' else PORT_B}"
    try:
        _, d = api(base, "/v1/peers")
        for pr in d.get("peers", []):
            if pr.get("node_id") == node_id:
                return pr
    except Exception:
        pass
    return {}


procs = []
def start(key):
    log = open(os.path.join(ROOT, f"{key}.log"), "w")
    p = subprocess.Popen([BIN, "--config", os.path.join(ROOT, f"{key}.json"),
                          "run", "--no-open"],
                         stdout=log, stderr=subprocess.STDOUT)
    procs.append((p, log))
    return p


def stop_all():
    for p, log in procs:
        try:
            p.send_signal(signal.SIGINT)
        except Exception:
            pass
    for p, log in procs:
        try:
            p.wait(timeout=6)
        except Exception:
            p.kill()
        try:
            log.close()
        except Exception:
            pass
    procs.clear()


def log_text(key):
    try:
        return open(os.path.join(ROOT, f"{key}.log")).read()
    except Exception:
        return ""


def wait_for(pred, tries=60):
    for _ in range(tries):
        try:
            if pred():
                return True
        except Exception:
            pass
        time.sleep(0.25)
    return False


def main():
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(ROOT, exist_ok=True)
    for p in (PORT_A, PORT_B):
        assert port_free(p), f"port {p} busy"

    # ---- S0: 缺省（lease 关）——零加性行为 --------------------------------
    write_cfg("a", [f"127.0.0.1:{PORT_B}"], lease=False)
    write_cfg("b", [], lease=False)
    start("a"); start("b")
    base_a, base_b = f"http://127.0.0.1:{PORT_A}", f"http://127.0.0.1:{PORT_B}"
    ok = wait_health(base_a) and wait_health(base_b)
    online = wait_for(lambda: peer_of("a", NODE_B).get("online") is True)
    check("S0 双节点互通（lease 默认关）", ok and online)
    pr = peer_of("a", NODE_B)
    check("S0 /v1/peers 无 load key（加性字段缺省不存在）",
          bool(pr) and "load" not in pr, json.dumps(pr, ensure_ascii=False))
    both_quiet = True
    for _ in range(30):
        time.sleep(0.5)
        logs = log_text("a") + log_text("b")
        if "lease: full status advertised" in logs or "lease: started" in logs:
            both_quiet = False
            break
    check("S0 无 lease 日志（任务未 spawn）", both_quiet)
    stop_all()

    # ---- S1: 启用 lease → 差异 Full Status 可见 ---------------------------
    shutil.rmtree(os.path.join(ROOT, "a"), ignore_errors=True)
    shutil.rmtree(os.path.join(ROOT, "b"), ignore_errors=True)
    write_cfg("a", [f"127.0.0.1:{PORT_B}"], lease=True)
    write_cfg("b", [], lease=True)
    start("a"); start("b")
    ok = wait_health(base_a) and wait_health(base_b)
    online = wait_for(lambda: peer_of("a", NODE_B).get("online") is True)
    check("S1 双节点启动并互通", ok and online)

    # 制造一次必然的 CPU 阈值跨越（3 个 hog → LOAD 显著上升 → Full 重发）。
    # 空闲机上首拍 Full 常发生在对端注册之前，阈值内静默期不会重发——
    # 新对端注册时的补推快照负责兜底，本动作用变化强制可见。
    hogs = [subprocess.Popen(["md5sum", "/dev/zero"], stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL) for _ in range(3)]
    try:
        def b_has_load():
            pr = peer_of("b", NODE_A)
            load = pr.get("load") or {}
            return isinstance(load.get("cpu"), int) and 0 <= load["cpu"] <= 100
        seen = wait_for(b_has_load, tries=60)  # ~15s；hog 存活期间必然有 Full
    finally:
        for h in hogs:
            h.kill()
            h.wait(timeout=5)
    pr = peer_of("b", NODE_A)
    check("S1 对端负载上报可见（load.cpu 0..=100）", seen, json.dumps(pr, ensure_ascii=False)[:200])

    # /v1/peers 的 load.ts 应为对端 ts（登记不遗漏）。
    ts_ok = seen and isinstance(pr.get("load", {}).get("ts"), str) and pr["load"]["ts"]
    check("S1 负载快照带时间戳", bool(ts_ok))

    # /v1/metrics 出现 node CPU gauge（Admin；text/plain —— 不能走 api() 的
    # json.loads，否则每次都抛异常被吞）。A 承载 hog、其 Full Status 被 B
    # 登记为 load —— 在 **B 侧**查（A 自己的台账里未必有 B 的样本）。
    def metrics_has_cpu():
        req = urllib.request.Request(f"{base_b}/v1/metrics",
                                     headers={"X-Auth-Token": TOKEN})
        with urllib.request.urlopen(req, timeout=8) as r:
            return "agentlink_peer_cpu_percent" in r.read().decode()
    check("S1 /v1/metrics 出现 agentlink_peer_cpu_percent",
          wait_for(metrics_has_cpu))

    # ---- S2: 活性与健康不受影响 ------------------------------------------
    alive = wait_health(base_a) and wait_health(base_b)
    still_online = peer_of("b", NODE_A).get("online") is True
    check("S2 /v1/health 正常 + 对端 online 持续（裸心跳/会话心跳维持）",
          alive and still_online)
    stop_all()


try:
    main()
finally:
    stop_all()

print(f"SMOKE_RESULT {passed}/{passed + failed}")
sys.exit(0 if failed == 0 else 1)
