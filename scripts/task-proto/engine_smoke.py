#!/usr/bin/env python3
"""M2 P2-5 真总线冒烟：引擎事件收敛 + 重启 0 丢失。

场景：1 机 agentlink（mesh.enabled + smoke license）+ executor.py（本机 WS 客户端）
      通过 /v1/send 手发 20 个 task_spec，验证执行方回报的事件被引擎
      事件收敛落表；再 SIGKILL 重启引擎，验证 20 任务 0 丢失、状态正确。

隔离端口 18765，独立 data_dir，绝不碰生产 8765/正式配置。

用法: python3 scripts/task-proto/engine_smoke.py
      (需先 cargo build --release; 二进制 ~/agentlink-target/release/agentlink)
"""
import json, os, signal, subprocess, sys, time, urllib.error, urllib.request

BIN = os.path.expanduser("~/agentlink-target/release/agentlink")
EXECUTOR = os.path.join(os.path.dirname(__file__), "executor.py")
SCRATCH = "/home/yxf/.hermes/profiles/agentlink/cache/scratch/engine-smoke"
PORT = 18765
TOKEN = "enginesmoketoken"
API = f"http://127.0.0.1:{PORT}"
DATA = os.path.join(SCRATCH, "data")
CFG = os.path.join(SCRATCH, "agentlink.json")
LIC = os.path.join(SCRATCH, "license.toml")
LOG = os.path.join(SCRATCH, "node.log")

results = []


def check(name, cond, detail=""):
    tag = "PASS" if cond else "FAIL"
    results.append(cond)
    print(f"{tag}  {name}{'  ' + detail if detail else ''}")


def http(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        API + path, method=method, data=data,
        headers={"X-Auth-Token": TOKEN, "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        raw = e.read().decode()[:400]
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, {"_raw": raw}


def boot():
    os.makedirs(DATA, exist_ok=True)
    logf = open(LOG, "a")
    p = subprocess.Popen([BIN, "run", "--config", CFG, "--no-open"],
                         stdout=logf, stderr=subprocess.STDOUT)
    for _ in range(100):
        try:
            code, h = http("GET", "/v1/health")
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


def write_cfg(mesh_enabled=True):
    cfg = {
        "v": 1, "node_id": "e1m2s3k4e5n6", "name": "engine-smoke",
        "listen_port": PORT, "data_dir": DATA,
        "discovery": {"enabled": False, "methods": [], "static_peers": []},
        "security": {"mode": "strict", "cluster_token": "enginesmoke-cluster"},
        "local_api": {"host": "127.0.0.1", "token": TOKEN},
        "delivery": {"ack_timeout_seconds": 5, "max_retries": 5,
                     "offline_ttl_seconds": 600, "dedup_cache_size": 2048,
                     "persist": True},
        "log": {"level": "debug", "format": "text", "file": ""},
    }
    if mesh_enabled:
        cfg["mesh"] = {"enabled": True, "license_file": "license.toml"}
    with open(CFG, "w") as f:
        json.dump(cfg, f)


def write_license():
    os.makedirs(SCRATCH, exist_ok=True)
    # 复用 engine_store_smoke 的 dev fixture 签名（seed=0x00..0x1F）
    lic = (
        "# AgentMesh engine-smoke license (dev fixture key)\n"
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
        "topic": "task_spec", "task_id": f"eng-{i:03d}",
        "type": "shell", "intent": "engine-smoke",
        "correlation": f"batch-{i % 5}",
        "params": {"stage_sleep": 0.2, "index": i},
        "requires": ["cap:shell:safe"],
        "to_agent": "worker",
    }


def task_state(tid):
    code, body = http("GET", f"/v1/tasks/{tid}")
    if code != 200:
        return None
    return body.get("task", {}).get("state")


def wait_all_converged(task_ids, timeout_s=30):
    """所有任务进入 terminal（succeeded/failed/declined/cancelled）。"""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        states = {tid: task_state(tid) for tid in task_ids}
        if all(s in ("succeeded", "failed", "declined", "cancelled")
               for s in states.values()):
            return states
        time.sleep(0.25)
    return {tid: task_state(tid) for tid in task_ids}


def main():
    if not os.path.exists(BIN):
        print(f"FAIL  binary-missing  {BIN} 先 cargo build --release")
        return 1
    if not os.path.exists(EXECUTOR):
        print(f"FAIL  executor-missing  {EXECUTOR}")
        return 1

    # 与 engine_store_smoke 的 dev 公钥对拍（签名逐字节一致）
    import base64, hashlib
    Q = 2**255 - 19
    L = 2**252 + 27742317777372353535851937790883648493
    D = -121665 * pow(121666, Q - 2, Q) % Q
    I = pow(2, (Q - 1) // 4, Q)

    def inv(x): return pow(x, Q - 2, Q)

    def xrecover(y):
        xx = (y * y - 1) * inv(D * y * y + 1)
        x = pow(xx, (Q + 3) // 8, Q)
        if (x * x - xx) % Q != 0:
            x = (x * I) % Q
        if x % 2 != 0:
            x = Q - x
        return x

    By = 4 * inv(5) % Q
    Bp = [xrecover(By) % Q, By % Q]

    def edwards(P, Qp):
        x1, y1 = P; x2, y2 = Qp
        x3 = (x1 * y2 + x2 * y1) * inv(1 + D * x1 * x2 * y1 * y2) % Q
        y3 = (y1 * y2 + x1 * x2) * inv(1 - D * x1 * x2 * y1 * y2) % Q
        return [x3, y3]

    def scalarmult(P, e):
        if e == 0: return [0, 1]
        R = scalarmult(P, e // 2)
        R = edwards(R, R)
        if e & 1: R = edwards(R, P)
        return R

    def bit(h, i): return (h[i // 8] >> (i % 8)) & 1

    def hint(m): return sum(2**i * bit(hashlib.sha512(m).digest(), i)
                            for i in range(512))

    def encodeint(y):
        return bytes((y >> (8 * i)) & 0xFF for i in range(32))

    def encodepoint(P):
        x, y = P
        bits = [(y >> i) & 1 for i in range(255)] + [x & 1]
        out = bytearray(32)
        for i, b in enumerate(bits):
            if b: out[i // 8] |= 1 << (i % 8)
        return bytes(out)

    def scalar_from_secret(h):
        return 2**254 + sum(2**i * bit(h, i) for i in range(3, 254))

    SEED = bytes(range(32))
    pk_bytes = encodepoint(scalarmult(Bp, scalar_from_secret(hashlib.sha512(SEED).digest())))
    DEV_PUBLIC_KEY_B64 = "A6EHv/POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg="
    check("dev-pubkey-match", base64.b64encode(pk_bytes).decode() == DEV_PUBLIC_KEY_B64)

    shutil = __import__("shutil")
    shutil.rmtree(SCRATCH, ignore_errors=True)

    # ---------- 阶段 1: 启动 mesh 节点 ----------
    write_license()
    write_cfg(mesh_enabled=True)
    p, h = boot()
    check("p1-boot-mesh", h.get("node_id") == "e1m2s3k4e5n6")
    check("p1-features-engine", "engine" in h.get("features", []))

    # ---------- 阶段 2: 启动 executor ----------
    ex = subprocess.Popen([sys.executable, EXECUTOR,
                           "--base", API, "--token", TOKEN, "--agent", "worker"],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True)
    time.sleep(2.0)  # 等 executor 注册 + WS 连接

    # ---------- 阶段 3: 手发 20 个 task_spec，走 /v1/send（不走引擎写面） ----------
    tid_list = [f"eng-{i:03d}" for i in range(20)]
    for i, tid in enumerate(tid_list):
        # 手发 task_spec 帧（/v1/send，broadcast，executor 作为本地 WS subscriber 收到）
        # 必须带 from_agent，否则 executor 回复 task_accepted 时 to_agent 无法路由回引擎
        code, body = http("POST", "/v1/send", {
            "to": "*", "to_agent": "worker",
            "from_agent": "orchestrator",
            "type": "text", "action": "task",
            "payload": spec_of(i),
        })
        check(f"p3-send-{i:03d}", code == 200 and body.get("ok"),
              f"code={code} body={body}")

    # ---------- 阶段 4: 等待全部收敛 ----------
    final_states = wait_all_converged(tid_list, timeout_s=60)
    succeeded = [t for t, s in final_states.items() if s == "succeeded"]
    check("p4-20-converged", len(succeeded) == 20,
          f"succeeded={len(succeeded)}/20 states={json.dumps(final_states, ensure_ascii=False)}")

    # ---------- 阶段 5: SIGKILL 重启，验证 0 丢失 ----------
    kill(p)
    p, h = boot()
    check("p5-reboot", h.get("node_id") == "e1m2s3k4e5n6" and "engine" in h.get("features", []))
    alive = 0; states = {}
    for tid in tid_list:
        code, body = http("GET", f"/v1/tasks/{tid}")
        if code == 200:
            alive += 1
            st = body.get("task", {}).get("state")
            states[st] = states.get(st, 0) + 1
    check("p5-20-zero-loss", alive == 20, f"alive={alive}/20 states={json.dumps(states)}")
    # 所有任务应仍为 terminal（重启后表恢复，不再被修改，因为 executor 已退出）
    all_terminal = all(s in ("succeeded", "failed", "declined", "cancelled")
                       for s in states.keys() if s)
    check("p5-all-terminal", all_terminal, f"states={json.dumps(states)}")

    kill(p)
    try:
        ex.terminate()
        ex.wait(timeout=3)
    except Exception:
        ex.kill()

    n = len(results); npass = sum(1 for r in results if r)
    print(f"\n{'ALL PASS' if npass == n else 'HAS FAILURES'}: {npass}/{n}")
    if npass == n:
        shutil.rmtree(SCRATCH, ignore_errors=True)
    return 0 if npass == n else 1


if __name__ == "__main__":
    sys.exit(main())
