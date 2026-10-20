# -*- coding: utf-8 -*-
"""abac_smoke — v0.7 §4.2 ABAC 策略引擎（任务 label × 节点 tags）。

拓扑：S 引擎（18953，tags=[zone:low]）+ W1（18954，zone:low）+ W2（18955，
zone:high），W1/W2 各跑一个 cap:ab 执行器。

S 上策略：
  deny  finance-not-low   task_labels{data=finance} × node_tags[zone:low]
  allow secret-high-only  task_labels{class=secret} × node_tags[zone:high]

场景：
  S0 策略视图（enabled + 规则名）
  S1 finance 任务只落 W2（W1=low 被拒）
  S2 无 label 任务不受策略影响（正常完成）
  S3 deny：W2 离线时 finance2 保持 pending，W1 收不到；W2 回来即完成
  S4 allow：W2 离线时 secret1 保持 pending，W1 收不到；W2 回来即完成
  S5 进程存活、日志无 panic

端口 18953-18955（隔离，绝不指向生产 8765）。
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
ROOT = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch/al-abac")
TOKEN = "abac-sim-token"
CLUSTER = "abac-cluster"
PORT_S, PORT_W1, PORT_W2 = 18953, 18954, 18955
NODE_S, NODE_W1, NODE_W2 = "550000000001", "550000000002", "550000000003"
EVIDENCE = []


def record(name, ok, detail=""):
    EVIDENCE.append({"scenario": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + " | "
          + json.dumps(detail, ensure_ascii=False)[:350], flush=True)


def api(port, path, body=None, method=None, timeout=10):
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    m = method or ("POST" if body is not None else "GET")
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", method=m, data=data,
        headers={"X-Auth-Token": TOKEN, "Content-Type": "application/json"})
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
        if "al-abac" in cmd and "agentlink" in cmd:
            try:
                os.kill(int(pid), signal.SIGKILL)
            except Exception:
                pass


def write_cfg(name, port, node_id, peers, tags, *, mesh=False, abac=None):
    cfg = {
        "v": 1, "node_id": node_id, "name": name, "listen_port": port,
        "tags": tags,
        "data_dir": os.path.join(ROOT, f"data{port}"),
        "history": {"enabled": True, "len": 400, "file": "history.jsonl"},
        "discovery": {"enabled": False,
                      "static_peers": [f"127.0.0.1:{p}" for p in peers]},
        "security": {"mode": "strict", "cluster_token": CLUSTER},
        "local_api": {"host": "127.0.0.1", "token": TOKEN},
        "delivery": {"ack_timeout_seconds": 5, "max_retries": 5,
                     "offline_ttl_seconds": 600, "dedup_cache_size": 4096,
                     "persist": False},
        "log": {"level": "info", "format": "text", "file": ""},
        "mesh": {"enabled": mesh, "license_file": "license.toml",
                 "engine": {"max_retries": 0, "backoff_secs": 1, "tick_ms": 200,
                            "max_concurrent_per_node": 2}},
    }
    if abac is not None:
        cfg["security"]["abac"] = abac
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
    tv = t.get("task") or t
    return tv.get("state")


def fleet_online(agent):
    _, d = api(PORT_S, "/v1/fleet/agents")
    for a in d.get("agents") or []:
        if a.get("agent_id") == agent:
            return bool(a.get("online"))
    return False


class Executor(threading.Thread):
    def __init__(self, port, node_id, agent_id):
        super().__init__(daemon=True)
        self.port, self.node_id, self.agent = port, node_id, agent_id
        self.running = True
        self.ws = None
        self.seen = set()
        self.specs = []
        self.lock = threading.Lock()

    def register(self):
        api(self.port, "/v1/agents", {"agent_id": self.agent, "name": self.agent,
                                      "capabilities": ["text", "cap:ab"]})

    def unregister(self):
        api(self.port, "/v1/agents", {"agent_id": self.agent}, method="DELETE")

    def send(self, payload):
        api(self.port, "/v1/send", {
            "from": self.node_id, "to": NODE_S,
            "from_agent": self.agent, "to_agent": "engine",
            "type": "text", "action": "send", "payload": payload})

    def run(self):
        self.register()
        while self.running:
            try:
                self.ws = websocket.create_connection(
                    f"ws://127.0.0.1:{self.port}/v1/ws?token={TOKEN}&client={self.agent}",
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
                        self.specs.append((tid, p.get("labels")))
                    threading.Thread(target=self._finish, args=(tid, seq),
                                     daemon=True).start()
            except Exception:
                if not self.running:
                    break
                time.sleep(0.5)

    def _finish(self, tid, seq):
        time.sleep(0.05)
        base = seq * 10
        self.send({"topic": "task_progress", "task_id": tid, "seq": base + 1,
                   "stage": "done"})
        self.send({"topic": "task_succeeded", "task_id": tid, "seq": base + 2,
                   "result": {"ok": True, "node": self.node_id}})

    def got(self, tid):
        with self.lock:
            return any(t == tid for t, _ in self.specs)

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
        "expires": "2036-12-31T23:59:59Z"}, "abac sim")
    open(os.path.join(ROOT, "license.toml"), "w").write(lic)
    for p in (PORT_S, PORT_W1, PORT_W2):
        assert not probe(p), f"port {p} busy（生产隔离铁律）"

    abac = {
        "enabled": True,
        "rules": [
            {"name": "finance-not-low", "effect": "deny",
             "task_labels": {"data": "finance"}, "node_tags": ["zone:low"]},
            {"name": "secret-high-only", "effect": "allow",
             "task_labels": {"class": "secret"}, "node_tags": ["zone:high"]},
        ],
    }
    procs = {}
    ex1 = ex2 = None
    try:
        cfg_s = write_cfg("s", PORT_S, NODE_S, [PORT_W1, PORT_W2],
                          ["zone:low"], mesh=True, abac=abac)
        cfg_1 = write_cfg("w1", PORT_W1, NODE_W1, [PORT_S], ["zone:low"])
        cfg_2 = write_cfg("w2", PORT_W2, NODE_W2, [PORT_S], ["zone:high"])
        procs["s"] = start(cfg_s, "s.log")
        assert wait_for(lambda: probe(PORT_S), 25), "S 未启动"
        procs["w1"] = start(cfg_1, "w1.log")
        assert wait_for(lambda: probe(PORT_W1), 25), "W1 未启动"
        procs["w2"] = start(cfg_2, "w2.log")
        assert wait_for(lambda: probe(PORT_W2), 25), "W2 未启动"
        ex1 = Executor(PORT_W1, NODE_W1, "ab-w1")
        ex1.start()
        ex2 = Executor(PORT_W2, NODE_W2, "ab-w2")
        ex2.start()
        # dummy：注销最后一个 agent 时 agents_update 为空不会广播（既有实现）。
        api(PORT_W2, "/v1/agents", {"agent_id": "ab-w2-idle", "name": "idle",
                                    "capabilities": ["text"]})
        assert wait_for(lambda: fleet_online("ab-w1") and fleet_online("ab-w2"), 30), \
            "执行器未全部上线"

        # ---- S0 策略视图 ----------------------------------------------------
        _, ms = api(PORT_S, "/v1/mesh/status")
        eng = ms.get("engine") or {}
        ab = eng.get("abac") or {}
        record("S0 mesh 视图暴露 ABAC 启用与规则名",
               ab.get("enabled") is True
               and set(ab.get("rules") or []) == {"finance-not-low", "secret-high-only"},
               {"enabled": ab.get("enabled"), "rules": ab.get("rules")})

        # ---- S1 deny：finance 只落 high -------------------------------------
        api(PORT_S, "/v1/tasks", {
            "task_id": "ab-fin-1", "type": "ab.echo", "priority": 100,
            "requires": ["cap:ab"], "labels": {"data": "finance"}, "params": {}})
        ok1 = wait_for(lambda: task_state("ab-fin-1") == "succeeded", 25)
        record("S1 finance 任务只落 W2（zone:high），W1 收不到",
               bool(ok1) and ex2.got("ab-fin-1") and not ex1.got("ab-fin-1"),
               {"state": task_state("ab-fin-1"),
                "w1": ex1.got("ab-fin-1"), "w2": ex2.got("ab-fin-1")})

        # ---- S2 无 label 不受影响 -------------------------------------------
        api(PORT_S, "/v1/tasks", {
            "task_id": "ab-plain", "type": "ab.echo", "priority": 100,
            "requires": ["cap:ab"], "params": {}})
        ok2 = wait_for(lambda: task_state("ab-plain") == "succeeded", 25)
        record("S2 无 label 任务不受策略影响（正常完成）", bool(ok2),
               {"state": task_state("ab-plain"),
                "w1": ex1.got("ab-plain"), "w2": ex2.got("ab-plain")})

        # ---- S3 deny 门：W2 离线 → finance2 pending ------------------------
        ex2.unregister()
        assert wait_for(lambda: not fleet_online("ab-w2"), 15), "W2 未注销"
        api(PORT_S, "/v1/tasks", {
            "task_id": "ab-fin-2", "type": "ab.echo", "priority": 100,
            "requires": ["cap:ab"], "labels": {"data": "finance"}, "params": {}})
        time.sleep(3.0)
        pending3 = task_state("ab-fin-2") == "pending"
        w1_no3 = not ex1.got("ab-fin-2")
        ex2.register()
        assert wait_for(lambda: fleet_online("ab-w2"), 15), "W2 未重新上线"
        ok3 = wait_for(lambda: task_state("ab-fin-2") == "succeeded", 25)
        record("S3 deny：W2 离线时 finance2 保持 pending 且 W1 收不到；W2 回来即完成",
               pending3 and w1_no3 and bool(ok3) and ex2.got("ab-fin-2"),
               {"pending": pending3, "w1_got": not w1_no3,
                "state": task_state("ab-fin-2"), "w2": ex2.got("ab-fin-2")})

        # ---- S4 allow 门：secret 只允许 high --------------------------------
        ex2.unregister()
        assert wait_for(lambda: not fleet_online("ab-w2"), 15), "W2 未注销"
        api(PORT_S, "/v1/tasks", {
            "task_id": "ab-sec-1", "type": "ab.echo", "priority": 100,
            "requires": ["cap:ab"], "labels": {"class": "secret"}, "params": {}})
        time.sleep(3.0)
        pending4 = task_state("ab-sec-1") == "pending"
        w1_no4 = not ex1.got("ab-sec-1")
        ex2.register()
        assert wait_for(lambda: fleet_online("ab-w2"), 15), "W2 未重新上线"
        ok4 = wait_for(lambda: task_state("ab-sec-1") == "succeeded", 25)
        record("S4 allow：W2 离线时 secret1 保持 pending 且 W1 收不到；W2 回来即完成",
               pending4 and w1_no4 and bool(ok4) and ex2.got("ab-sec-1"),
               {"pending": pending4, "w1_got": not w1_no4,
                "state": task_state("ab-sec-1"), "w2": ex2.got("ab-sec-1")})

        # ---- S5 存活/panic ---------------------------------------------------
        alive = all(p.poll() is None for p in procs.values())
        panics = []
        for name in ("s", "w1", "w2"):
            log = os.path.join(ROOT, f"{name}.log")
            if os.path.exists(log) and "panic" in open(log).read().lower():
                panics.append(name)
        record("S5 三进程存活、日志无 panic", alive and not panics,
               {"alive": sum(1 for p in procs.values() if p.poll() is None),
                "panics": panics})

    finally:
        for ex in (ex1, ex2):
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
        with open(os.path.join(REPO, "tests", "abac_smoke.jsonl"), "w") as f:
            for e in EVIDENCE:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        npass = sum(1 for e in EVIDENCE if e["pass"])
        print(f"abac_smoke: {npass}/{len(EVIDENCE)} "
              f"→ {'OK' if npass == len(EVIDENCE) else 'FAILED'}", flush=True)
    sys.exit(0 if all(e["pass"] for e in EVIDENCE) else 1)


if __name__ == "__main__":
    main()
