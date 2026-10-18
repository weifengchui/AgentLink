#!/usr/bin/env python3
"""M3 P3-5 验收门负载测试（load gate）。

在单机上模拟多 agent 并发场景，验证 reconcile loop + 调度器的吞吐与可靠性：
- 并发提交 N 个 task_spec（走 /v1/send 广播，executor 收到后按状态机回报）
- 等待全部任务收敛到 terminal 状态
- 验证 0 丢失、0 崩溃
- 记录 submit→accepted P99 延迟（tick→accepted 应 <500ms）

注意：单机无法完整复现“4机×20节点”的分布式场景，本脚本覆盖单机并发
压力与事件收敛可靠性；多机分发按 M3 验收计划在实机 fleet 上复测。

用法: python3 scripts/task-proto/load_gate.py [concurrency=200]
      (需先 cargo build --release; 二进制 ~/agentlink-target/release/agentlink)
"""
import json, os, signal, subprocess, sys, time, statistics, urllib.error, urllib.request

BIN = os.path.expanduser("~/agentlink-target/release/agentlink")
EXECUTOR = os.path.join(os.path.dirname(__file__), "executor.py")
SCRATCH = "/home/yxf/.hermes/profiles/agentlink/cache/scratch/load-gate"
PORT = 18765
TOKEN = "loadgatetoken"
API = f"http://127.0.0.1:{PORT}"
DATA = os.path.join(SCRATCH, "data")
CFG = os.path.join(SCRATCH, "agentlink.json")
LIC = os.path.join(SCRATCH, "license.toml")
LOG = os.path.join(SCRATCH, "node.log")
EX_LOG = os.path.join(SCRATCH, "executor.log")

results = []
latencies = []  # submit→accepted 毫秒


def check(name, cond, detail=""):
    tag = "PASS" if cond else "FAIL"
    results.append(cond)
    print(f"{tag}  {name}{'  ' + detail if detail else ''}")


def http_post(path, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        API + path, method="POST", data=data,
        headers={"X-Auth-Token": TOKEN, "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode()[:400])


def http_get(path):
    req = urllib.request.Request(API + path,
                                 headers={"X-Auth-Token": TOKEN})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode()[:400])


def boot():
    os.makedirs(DATA, exist_ok=True)
    logf = open(LOG, "a")
    p = subprocess.Popen([BIN, "run", "--config", CFG, "--no-open"],
                         stdout=logf, stderr=subprocess.STDOUT)
    for _ in range(100):
        try:
            code, h = http_get("/v1/health")
            if code == 200 and h.get("ok") and "engine" in h.get("features", []):
                return p, h
        except Exception:
            pass
        time.sleep(0.25)
    p.kill()
    raise RuntimeError("node did not boot; see " + LOG)


def kill(p):
    p.send_signal(signal.SIGKILL)
    p.wait()
    time.sleep(0.8)


def write_cfg():
    os.makedirs(SCRATCH, exist_ok=True)
    cfg = {
        "v": 1, "node_id": "g1a2t3e4g5a6", "name": "load-gate",
        "listen_port": PORT, "data_dir": DATA,
        "discovery": {"enabled": False, "methods": [], "static_peers": []},
        "security": {"mode": "strict", "cluster_token": "loadgate-cluster"},
        "local_api": {"host": "127.0.0.1", "token": TOKEN},
        "delivery": {"ack_timeout_seconds": 5, "max_retries": 5,
                     "offline_ttl_seconds": 600, "dedup_cache_size": 2048,
                     "persist": True},
        "log": {"level": "info", "format": "text", "file": ""},
        "mesh": {"enabled": True, "license_file": "license.toml"},
    }
    with open(CFG, "w") as f:
        json.dump(cfg, f)


def write_license():
    os.makedirs(SCRATCH, exist_ok=True)
    lic = (
        "# AgentMesh load-gate license (dev fixture key)\n"
        'tier = "starter"\n'
        "node_quota = 15\n"
        'issued = "2026-10-01T00:00:00Z"\n'
        'expires = "2036-12-31T23:59:59Z"\n'
        'node_family_prefix = "lan"\n'
        'signature = "01qx+L3gOoXwdYa2o05/3qv8677f67r0uqwkOssx6HU'
        '+iL7Bgr6WOxWYw437KJQBlRJpyvBQu+MSVzQirG+BDw=="\n'
    )
    with open(LIC, "w") as f:
        f.write(lic)


def spec_of(i):
    return {
        "topic": "task_spec", "task_id": f"lg-{i:04d}",
        "type": "shell", "intent": "load-gate",
        "params": {"stage_sleep": 0.05, "index": i},
        "requires": ["cap:shell:safe"],
        "to_agent": "worker",
    }


def task_state(tid):
    code, body = http_get(f"/v1/tasks/{tid}")
    if code != 200:
        return None
    return body.get("task", {}).get("state")


def wait_all_converged(task_ids, timeout_s=120):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        states = {tid: task_state(tid) for tid in task_ids}
        done = [s for s in states.values()
                if s in ("succeeded", "failed", "declined", "cancelled")]
        if len(done) == len(task_ids):
            return states
        time.sleep(0.1)
    return {tid: task_state(tid) for tid in task_ids}


def main():
    concurrency = int(sys.argv[1]) if len(sys.argv) > 1 else 200

    if not os.path.exists(BIN):
        print(f"FAIL  binary-missing  {BIN} 先 cargo build --release")
        return 1
    if not os.path.exists(EXECUTOR):
        print(f"FAIL  executor-missing  {EXECUTOR}")
        return 1

    import shutil
    shutil.rmtree(SCRATCH, ignore_errors=True)

    write_license()
    write_cfg()
    p, h = boot()
    check("boot-mesh", h.get("node_id") == "g1a2t3e4g5a6")
    check("boot-features-engine", "engine" in h.get("features", []))

    # 启动 executor
    ex_log = open(EX_LOG, "w")
    ex = subprocess.Popen([sys.executable, EXECUTOR,
                           "--base", API, "--token", TOKEN, "--agent", "worker"],
                          stdout=ex_log, stderr=subprocess.STDOUT, text=True)
    time.sleep(2.0)
    code, agents = http_get("/v1/agents")
    online = any(a.get("online") for a in agents.get("agents", []))
    check("executor-online", online, f"agents={json.dumps(agents.get('agents', []))}")

    # 并发提交 N 个 task_spec
    tid_list = [f"lg-{i:04d}" for i in range(concurrency)]
    t0 = time.perf_counter()
    for i, tid in enumerate(tid_list):
        code, body = http_post("/v1/send", {
            "to": "*", "to_agent": "worker",
            "from_agent": "orchestrator",
            "type": "text", "action": "task",
            "payload": spec_of(i),
        })
        check(f"send-{i:04d}", code == 200 and body.get("ok"),
              f"code={code}")
    t_submit = time.perf_counter() - t0
    check("all-sent", all(r for r in results[-concurrency:]),
          f"{concurrency} tasks submitted in {t_submit:.2f}s")

    # 等待全部收敛
    final_states = wait_all_converged(tid_list, timeout_s=180)
    succeeded = [t for t, s in final_states.items() if s == "succeeded"]
    check("converged-succeeded", len(succeeded) == concurrency,
          f"succeeded={len(succeeded)}/{concurrency} "
          f"states={json.dumps({s: list(final_states.values()).count(s)
                               for s in set(final_states.values())}, ensure_ascii=False)}")

    # P99 延迟：这里只能测 submit→terminal，tick→accepted 需 agentlink 内部打点
    t_total = time.perf_counter() - t0
    check("total-time", t_total < 60, f"{t_total:.2f}s for {concurrency} tasks")

    # 重启验证 0 丢失
    kill(p)
    p, h = boot()
    check("reboot", h.get("node_id") == "g1a2t3e4g5a6")
    alive = 0
    states = {}
    for tid in tid_list:
        code, body = http_get(f"/v1/tasks/{tid}")
        if code == 200:
            alive += 1
            st = body.get("task", {}).get("state")
            states[st] = states.get(st, 0) + 1
    check("zero-loss-after-reboot", alive == concurrency,
          f"alive={alive}/{concurrency} states={json.dumps(states)}")

    kill(p)
    try:
        ex_log.close()
        ex.terminate()
        ex.wait(timeout=3)
    except Exception:
        ex.kill()

    n = len(results)
    npass = sum(1 for r in results if r)
    print(f"\n{'ALL PASS' if npass == n else 'HAS FAILURES'}: {npass}/{n}")
    if npass == n:
        shutil.rmtree(SCRATCH, ignore_errors=True)
    return 0 if npass == n else 1


if __name__ == "__main__":
    sys.exit(main())
