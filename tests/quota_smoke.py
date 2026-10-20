# -*- coding: utf-8 -*-
"""quota_smoke — v0.7 §5.4 per-namespace 配额（准入 + 派发截流）。

拓扑：S 引擎（18963）+ W1（18964，namespace=team-a 的持任务执行器）。
配额（S 的 mesh.quota）：
  team-a    : max_open=2, max_inflight=1, max_submit_per_minute=10
  team-rate : max_open=10, max_inflight=10, max_submit_per_minute=1
  default   : 全 0（不限制）

场景：
  S0 /v1/mesh/quota 配置视图；未绑定 Operator 403；绑定 operator 只看本租户
  S1 open 上限：A running + B pending 后 C → 429 open_quota 且不落表
  S2 in-flight 截流：A 在跑时 B 不派发；release A 后 B 才上线
  S3 速率：team-rate 第 2 次提交 429 rate_limited；default 不受影响
  S4 用量面：open/inflight/submit_last_minute 准确
  S5 进程存活、日志无 panic

端口 18963/18964（隔离，绝不指向生产 8765）。
"""
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.request

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "tests"))
import engine_store_smoke as es  # noqa: E402
import websocket                 # noqa: E402

BIN = os.environ.get("AL_BIN",
                     os.path.expanduser("~/agentlink-target/release/agentlink"))
ROOT = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch/quota-sim")
CLUSTER = "q-cluster"
ADMIN = "q-admin"
OP = "q-op"
TEAM_OP = "q-team"
PORT_S, PORT_W1 = 18963, 18964
NODE_S, NODE_W1 = "580000000001", "580000000002"
EVIDENCE = []


def record(name, ok, detail=""):
    EVIDENCE.append({"scenario": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + " | "
          + json.dumps(detail, ensure_ascii=False)[:380], flush=True)


def api(port, path, body=None, method=None, token=ADMIN, timeout=10):
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    m = method or ("POST" if body is not None else "GET")
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", method=m, data=data,
        headers={"X-Auth-Token": token, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:
            return e.code, {}
    except Exception as e:
        return 0, {"error": str(e)[:150]}


def probe(port):
    import socket
    with socket.socket() as s:
        s.settimeout(0.4)
        return s.connect_ex(("127.0.0.1", port)) == 0


def kill_stale():
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open(f"/proc/{pid}/cmdline", encoding="utf-8", errors="replace") as f:
                cmd = f.read().replace("\0", " ")
        except Exception:
            continue
        if "quota-sim" in cmd and "agentlink" in cmd:
            try:
                os.kill(int(pid), signal.SIGKILL)
            except Exception:
                pass


def write_cfg(name, port, node_id, peers, *, mesh=False, quota=None, tokens=None,
              token_namespaces=None):
    cfg = {
        "v": 1, "node_id": node_id, "name": name, "listen_port": port,
        "data_dir": os.path.join(ROOT, f"data{port}"),
        "history": {"enabled": True, "len": 500, "file": "history.jsonl"},
        "discovery": {"enabled": False,
                      "static_peers": [f"127.0.0.1:{p}" for p in peers]},
        "security": {"mode": "strict", "cluster_token": CLUSTER},
        "local_api": {"host": "127.0.0.1", "token": ADMIN if mesh else "w1-admin"},
        "delivery": {"ack_timeout_seconds": 5, "max_retries": 2,
                     "offline_ttl_seconds": 300, "dedup_cache_size": 2048,
                     "persist": False},
        "log": {"level": "info", "format": "text", "file": ""},
        "mesh": {"enabled": mesh, "license_file": "license.toml",
                 "engine": {"max_retries": 0, "backoff_secs": 1, "tick_ms": 200,
                            "max_concurrent_per_node": 4}},
    }
    if tokens:
        cfg["auth"] = {"tokens": tokens}
    if token_namespaces:
        cfg.setdefault("auth", {})["token_namespaces"] = token_namespaces
    if quota is not None:
        cfg["mesh"]["quota"] = quota
    path = os.path.join(ROOT, f"{name}.json")
    json.dump(cfg, open(path, "w"))
    return path


def start(cfg, log):
    return subprocess.Popen(
        [BIN, "run", "--config", cfg, "--no-open"],
        stdout=open(os.path.join(ROOT, log), "a"),
        stderr=subprocess.STDOUT)


def wait_for(fn, timeout_s=30, step=0.2):
    end = time.monotonic() + timeout_s
    while time.monotonic() < end:
        v = fn()
        if v:
            return v
        time.sleep(step)
    return fn()


def task_state(tid):
    _, t = api(PORT_S, f"/v1/tasks/{tid}")
    return (t.get("task") or {}).get("state")


def fleet_online(agent):
    _, d = api(PORT_S, "/v1/fleet/agents")
    for a in d.get("agents") or []:
        if a.get("agent_id") == agent:
            return bool(a.get("online"))
    return False


def submit(tid, namespace):
    return api(PORT_S, "/v1/tasks", {
        "task_id": tid, "type": "quota.echo", "priority": 100,
        "namespace": namespace, "requires": ["cap:qa"], "params": {}})


def quota_rows():
    _, d = api(PORT_S, "/v1/mesh/quota")
    return {r.get("namespace"): r for r in d.get("namespaces") or []}, d


class HeldExecutor(threading.Thread):
    """接任务后发 accepted 并持有，等 release() 才成功。"""

    def __init__(self, port, node_id, agent_id, token, namespace):
        super().__init__(daemon=True)
        self.port, self.node_id, self.agent = port, node_id, agent_id
        self.token, self.namespace = token, namespace
        self.running = True
        self.ws = None
        self.lock = threading.Lock()
        self.seen = set()
        self.seqs = {}
        self.specs = []

    def register(self):
        return api(self.port, "/v1/agents", {
            "agent_id": self.agent, "name": self.agent,
            "namespace": self.namespace,
            "capabilities": ["text", "cap:qa"]}, token=self.token)

    def send(self, payload):
        return api(self.port, "/v1/send", {
            "from": self.node_id, "to": NODE_S,
            "from_agent": self.agent, "to_agent": "engine",
            "type": "text", "action": "send", "payload": payload}, token=self.token)

    def run(self):
        self.register()
        while self.running:
            try:
                self.ws = websocket.create_connection(
                    f"ws://127.0.0.1:{self.port}/v1/ws?token={self.token}&client={self.agent}",
                    ping_interval=25, ping_timeout=10)
                self.register()
                while self.running:
                    raw = self.ws.recv()
                    if not raw:
                        continue
                    p = (json.loads(raw).get("payload") or {})
                    if p.get("topic") != "task_spec":
                        continue
                    tid, seq = p.get("task_id"), int(p.get("seq", 1))
                    if (tid, seq) in self.seen:
                        continue
                    self.seen.add((tid, seq))
                    with self.lock:
                        self.seqs[tid] = seq
                        self.specs.append(tid)
                    base = seq * 10
                    self.send({"topic": "task_accepted", "task_id": tid,
                               "seq": base + 1, "state": "running"})
            except Exception:
                if not self.running:
                    break
                time.sleep(0.5)

    def release(self, tid):
        with self.lock:
            seq = self.seqs.get(tid)
        if seq is None:
            return False
        base = seq * 10
        self.send({"topic": "task_progress", "task_id": tid, "seq": base + 2,
                   "stage": "done"})
        self.send({"topic": "task_succeeded", "task_id": tid, "seq": base + 3,
                   "result": {"ok": True}})
        return True

    def got(self, tid):
        with self.lock:
            return tid in self.specs

    def stop(self):
        self.running = False
        try:
            self.ws and self.ws.close()
        except Exception:
            pass


def main():
    kill_stale()
    time.sleep(0.5)
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(ROOT, exist_ok=True)
    lic = es.sign_license_text({
        "tier": "starter", "node_quota": 15,
        "issued": "2026-10-01T00:00:00Z",
        "expires": "2036-12-31T23:59:59Z"}, "quota sim")
    open(os.path.join(ROOT, "license.toml"), "w").write(lic)
    for p in (PORT_S, PORT_W1):
        assert not probe(p), f"port {p} busy（生产隔离铁律）"

    quota = {
        "enabled": True,
        "default_max_open": 0, "default_max_inflight": 0,
        "default_max_submit_per_minute": 0,
        "namespaces": {
            "team-a": {"max_open": 2, "max_inflight": 1,
                       "max_submit_per_minute": 10},
            "team-rate": {"max_open": 10, "max_inflight": 10,
                          "max_submit_per_minute": 1},
        },
    }
    procs = {}
    ex = None
    try:
        cfg_s = write_cfg("s", PORT_S, NODE_S, [PORT_W1], mesh=True,
                          quota=quota, tokens={OP: "operator", TEAM_OP: "operator"},
                          token_namespaces={TEAM_OP: "team-a"})
        cfg_w = write_cfg("w1", PORT_W1, NODE_W1, [PORT_S])
        procs["s"] = start(cfg_s, "s.log")
        assert wait_for(lambda: probe(PORT_S), 25), "S 未启动"
        procs["w1"] = start(cfg_w, "w1.log")
        assert wait_for(lambda: probe(PORT_W1), 25), "W1 未启动"

        ex = HeldExecutor(PORT_W1, NODE_W1, "q-a", "w1-admin", "team-a")
        ex.start()
        assert wait_for(lambda: fleet_online("q-a"), 30), "执行器未上线"

        # ---- S0 配置视图 + RBAC --------------------------------------------
        rows, view = quota_rows()
        st_op, _ = api(PORT_S, "/v1/mesh/quota", token=OP)
        st_team, team_view = api(PORT_S, "/v1/mesh/quota", token=TEAM_OP)
        team_rows = [r.get("namespace") for r in team_view.get("namespaces") or []]
        ta = rows.get("team-a") or {}
        record("S0 quota 视图正确；未绑定 Operator 403；绑定 operator 只看本租户",
               view.get("enabled") is True
               and ta.get("max_open") == 2 and ta.get("max_inflight") == 1
               and ta.get("max_submit_per_minute") == 10 and st_op == 403
               and st_team == 200 and team_rows == ["team-a"]
               and team_view.get("scope") == "team-a",
               {"team-a": ta, "operator": st_op, "team": st_team,
                "team_rows": team_rows})

        # ---- S1 open 上限 ---------------------------------------------------
        st_a, _ = submit("qta1", "team-a")
        ok_run = wait_for(lambda: task_state("qta1") == "running", 20)
        st_b, _ = submit("qta2", "team-a")
        time.sleep(1.5)
        st_c, b_c = submit("qta3", "team-a")
        st_c_get, _ = api(PORT_S, "/v1/tasks/qta3")
        rows1, _ = quota_rows()
        r1 = rows1.get("team-a") or {}
        record("S1 open=2：A running + B pending 后 C 429 且不落表",
               st_a == 202 and bool(ok_run) and st_b == 202
               and st_c == 429 and b_c.get("reason") == "open_quota"
               and st_c_get == 404 and r1.get("open") == 2
               and r1.get("inflight") == 1,
               {"a": st_a, "b": st_b, "c": st_c, "c_get": st_c_get,
                "team-a_usage": r1, "reason": b_c.get("reason")})

        # ---- S2 in-flight=1：B 不派发，release A 后 B 才上线 ----------------
        b_pending_before = task_state("qta2") == "pending"
        b_not_dispatched = not ex.got("qta2")
        ex.release("qta1")
        ok_a = wait_for(lambda: task_state("qta1") == "succeeded", 20)
        ok_b = wait_for(lambda: task_state("qta2") == "running", 25)
        b_dispatched = ex.got("qta2")
        record("S2 max_inflight=1：A 在跑时 B 保持 pending，release A 后 B 上线",
               b_pending_before and b_not_dispatched and bool(ok_a)
               and bool(ok_b) and b_dispatched,
               {"b_pending_before": b_pending_before,
                "b_not_dispatched": b_not_dispatched,
                "a_done": ok_a, "b_running": ok_b, "b_dispatched": b_dispatched})

        # ---- S3 提交速率（独立 namespace）+ default 不受影响 ----------------
        st_r1, _ = submit("qrate1", "team-rate")
        st_r2, b_r2 = submit("qrate2", "team-rate")
        st_r2_get, _ = api(PORT_S, "/v1/tasks/qrate2")
        st_d1, _ = submit("qdef1", "default")
        record("S3 team-rate 第 2 次 429 rate_limited；default 不受影响",
               st_r1 == 202 and st_r2 == 429
               and b_r2.get("reason") == "rate_limited" and st_r2_get == 404
               and st_d1 == 202,
               {"r1": st_r1, "r2": st_r2, "r2_get": st_r2_get,
                "reason": b_r2.get("reason"), "default": st_d1})

        # ---- S4 用量面 ------------------------------------------------------
        rows4, _ = quota_rows()
        ta4 = rows4.get("team-a") or {}
        tr4 = rows4.get("team-rate") or {}
        df4 = rows4.get("default") or {}
        st_team4, team4 = api(PORT_S, "/v1/mesh/quota", token=TEAM_OP)
        team4_rows = team4.get("namespaces") or []
        record("S4 用量面：team-a open/inflight、team-rate rate、default 用量；绑定 token 只看本行",
               ta4.get("open") == 1 and ta4.get("inflight") == 1
               and ta4.get("submit_last_minute") == 3
               and tr4.get("open") == 1 and tr4.get("submit_last_minute") == 1
               and df4.get("open") == 1 and df4.get("max_open") == 0
               and st_team4 == 200 and len(team4_rows) == 1
               and team4_rows[0].get("namespace") == "team-a"
               and team4_rows[0].get("open") == 1,
               {"team-a": ta4, "team-rate": tr4, "default": df4,
                "team_rows": team4_rows})

        # ---- S5 收尾：release B --------------------------------------------
        ex.release("qta2")
        ok_b_done = wait_for(lambda: task_state("qta2") == "succeeded", 20)
        rows5, _ = quota_rows()
        ta5 = rows5.get("team-a") or {}
        record("S5 release B 后终态、team-a 用量归零",
               bool(ok_b_done) and ta5.get("open") == 0 and ta5.get("inflight") == 0,
               {"b_done": ok_b_done, "team-a": ta5})

        # ---- S6 存活/panic ---------------------------------------------------
        alive = all(p.poll() is None for p in procs.values())
        panics = []
        for name in ("s", "w1"):
            log = os.path.join(ROOT, f"{name}.log")
            if os.path.exists(log) and "panic" in open(log).read().lower():
                panics.append(name)
        record("S6 两进程存活、日志无 panic", alive and not panics,
               {"alive": sum(1 for p in procs.values() if p.poll() is None),
                "panics": panics})

    finally:
        if ex:
            ex.stop()
        for p in procs.values():
            try:
                p.send_signal(signal.SIGINT)
            except Exception:
                pass
        for p in procs.values():
            try:
                p.wait(timeout=6)
            except Exception:
                p.kill()
        with open(os.path.join(REPO, "tests", "quota_smoke.jsonl"), "w") as f:
            for e in EVIDENCE:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        npass = sum(1 for e in EVIDENCE if e["pass"])
        print(f"quota_smoke: {npass}/{len(EVIDENCE)} "
              f"→ {'OK' if npass == len(EVIDENCE) else 'FAILED'}", flush=True)
    sys.exit(0 if all(e["pass"] for e in EVIDENCE) else 1)


if __name__ == "__main__":
    main()
