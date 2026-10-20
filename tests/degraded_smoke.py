# -*- coding: utf-8 -*-
"""degraded_smoke — v0.7 §6.3 降级模式（read-only / standalone）。

拓扑：A（18947，standalone_after_seconds=2，audit enabled）+ B（18948）
互为 static_peer；A 上常驻执行器。

场景：
  S0 有对端时 mode=normal
  S1 运行时进只读：mode=read_only、任务声明 503、读面照常、standalone 手工设置 400
  S2 退出只读后任务可提交并成功（写面恢复）
  S3 停 B：2s 静默后 mode=standalone，本地队列自治（任务仍成功），
     审计出现 mode.change → standalone
  S4 重启 B：对端恢复 → mode=normal，审计出现 mode.change → normal
  S5 进程存活、日志无 panic

端口 18947-18948（隔离，绝不指向生产 8765）。
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
ROOT = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch/al-degraded")
TOKEN = "degraded-sim-token"
CLUSTER = "degraded-cluster"
PORT_A = 18947
PORT_B = 18948
NODE_A = "530000000001"
NODE_B = "530000000002"
EVIDENCE = []


def record(name, ok, detail=""):
    EVIDENCE.append({"scenario": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + " | "
          + json.dumps(detail, ensure_ascii=False)[:300], flush=True)


def api(port, path, body=None, timeout=10):
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", method="POST" if body is not None else "GET",
        data=data, headers={"X-Auth-Token": TOKEN, "Content-Type": "application/json"})
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
        if "al-degraded" in cmd and "agentlink" in cmd:
            try:
                os.kill(int(pid), signal.SIGKILL)
            except Exception:
                pass


def write_cfg(name, port, node_id, peer_port, *, standalone_after=0):
    cfg = {
        "v": 1, "node_id": node_id, "name": name, "listen_port": port,
        "data_dir": os.path.join(ROOT, f"data{port}"),
        "history": {"enabled": True, "len": 400, "file": "history.jsonl"},
        "discovery": {"enabled": False,
                      "static_peers": [f"127.0.0.1:{peer_port}"]},
        "security": {"mode": "strict", "cluster_token": CLUSTER,
                     "audit": {"enabled": True}},
        "local_api": {"host": "127.0.0.1", "token": TOKEN},
        "delivery": {"ack_timeout_seconds": 5, "max_retries": 5,
                     "offline_ttl_seconds": 600, "dedup_cache_size": 4096,
                     "persist": False},
        "log": {"level": "info", "format": "text", "file": ""},
        "mesh": {"enabled": True, "license_file": "license.toml",
                 "degraded": {"read_only": False,
                              "standalone_after_seconds": standalone_after},
                 "engine": {"max_retries": 0, "backoff_secs": 30, "tick_ms": 200,
                            "max_concurrent_per_node": 2}},
    }
    path = os.path.join(ROOT, f"{name}.json")
    json.dump(cfg, open(path, "w"))
    return path


def start(cfg, log):
    return subprocess.Popen(
        [BIN, "run", "--config", cfg, "--no-open"],
        stdout=open(os.path.join(ROOT, log), "a"),
        stderr=subprocess.STDOUT)


class LocalExecutor(threading.Thread):
    """A 节点常驻执行器：收 spec → 30ms 后 succeeded。"""

    def __init__(self, port, node_id):
        super().__init__(daemon=True)
        self.port, self.node_id = port, node_id
        self.agent = "deg-w"
        self.running = True
        self.ws = None
        self.seen = set()

    def send(self, payload):
        api(self.port, "/v1/send", {
            "from": self.node_id, "to": NODE_A,
            "from_agent": self.agent, "to_agent": "engine",
            "type": "text", "action": "send", "payload": payload})

    def run(self):
        api(self.port, "/v1/agents", {"agent_id": self.agent, "name": self.agent,
                                      "capabilities": ["text", "cap:deg"]})
        while self.running:
            try:
                self.ws = websocket.create_connection(
                    f"ws://127.0.0.1:{self.port}/v1/ws?token={TOKEN}&client={self.agent}",
                    ping_interval=25, ping_timeout=10)
                api(self.port, "/v1/agents", {"agent_id": self.agent, "name": self.agent,
                                              "capabilities": ["text", "cap:deg"]})
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
                    threading.Thread(target=self._run_one, args=(tid, seq),
                                     daemon=True).start()
            except Exception:
                if not self.running:
                    break
                time.sleep(0.5)

    def _run_one(self, tid, seq):
        time.sleep(0.03)
        base = seq * 10
        self.send({"topic": "task_succeeded", "task_id": tid, "seq": base + 2,
                   "result": {"ok": True}})

    def stop(self):
        self.running = False
        try:
            self.ws and self.ws.close()
        except Exception:
            pass


def mode_view():
    _, d = api(PORT_A, "/v1/mesh/mode")
    return d


def wait_for(fn, timeout_s=20, step=0.25):
    end = time.monotonic() + timeout_s
    while time.monotonic() < end:
        v = fn()
        if v:
            return v
        time.sleep(step)
    return fn()


def mode_events():
    _, ex = api(PORT_A, "/v1/audit/export?limit=100")
    out = []
    for r in ex.get("records") or []:
        if r.get("event") == "mode.change":
            out.append((r.get("detail") or {}).get("to"))
    return out


def main():
    kill_stale()
    time.sleep(0.5)
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(ROOT, exist_ok=True)
    lic = es.sign_license_text({
        "tier": "starter", "node_quota": 15,
        "issued": "2026-10-01T00:00:00Z",
        "expires": "2036-12-31T23:59:59Z"}, "degraded sim")
    open(os.path.join(ROOT, "license.toml"), "w").write(lic)
    for p in (PORT_A, PORT_B):
        assert not probe(p), f"port {p} busy（生产隔离铁律）"

    procs = {}
    ex = None
    try:
        cfg_a = write_cfg("a", PORT_A, NODE_A, PORT_B, standalone_after=2)
        cfg_b = write_cfg("b", PORT_B, NODE_B, PORT_A)
        procs["a"] = start(cfg_a, "a.log")
        assert wait_for(lambda: probe(PORT_A), 25), "A 未启动"
        procs["b"] = start(cfg_b, "b.log")
        assert wait_for(lambda: probe(PORT_B), 25), "B 未启动"
        ex = LocalExecutor(PORT_A, NODE_A)
        ex.start()

        # ---- S0 有对端 normal ---------------------------------------------
        ok0 = wait_for(lambda: (lambda m: m if m.get("mode") == "normal"
                                and m.get("peers_online", 0) >= 1 else None)(mode_view()), 25)
        record("S0 有对端时 mode=normal", bool(ok0),
               {"mode": (ok0 or {}).get("mode"),
                "peers_online": (ok0 or {}).get("peers_online")})

        # ---- S1 只读 --------------------------------------------------------
        st, m1 = api(PORT_A, "/v1/mesh/mode", {"mode": "read_only"})
        st_task, t1 = api(PORT_A, "/v1/tasks", {
            "task_id": "deg-blocked", "type": "deg.echo", "priority": 100,
            "requires": ["cap:deg"], "params": {}})
        st_status, s1 = api(PORT_A, "/v1/status")
        st_bad, bad = api(PORT_A, "/v1/mesh/mode", {"mode": "standalone"})
        record("S1 只读：写面 503、读面/状态照常、standalone 手工设置 400",
               st == 200 and m1.get("mode") == "read_only"
               and st_task == 503 and t1.get("error") == "read_only"
               and st_status == 200 and s1.get("mode") == "read_only"
               and st_bad == 400,
               {"set": m1.get("mode"), "task_status": st_task,
                "task_error": t1.get("error"), "status_mode": s1.get("mode"),
                "bad_mode_status": st_bad})

        # ---- S2 退出只读，任务恢复 -----------------------------------------
        st, m2 = api(PORT_A, "/v1/mesh/mode", {"mode": "normal"})
        assert st == 200 and m2.get("mode") == "normal", m2
        st, _ = api(PORT_A, "/v1/tasks", {
            "task_id": "deg-ok-1", "type": "deg.echo", "priority": 100,
            "requires": ["cap:deg"], "params": {}})

        def task_state(tid):
            _, t = api(PORT_A, f"/v1/tasks/{tid}")
            return (t.get("task") or t).get("state")

        ok2 = (st == 202
               and wait_for(lambda: task_state("deg-ok-1") == "succeeded", 20))
        record("S2 退出只读后任务可提交并成功", ok2,
               {"submit": st, "state": task_state("deg-ok-1")})

        # ---- S3 停 B → standalone 自治 --------------------------------------
        procs["b"].send_signal(signal.SIGINT)
        try:
            procs["b"].wait(timeout=6)
        except Exception:
            procs["b"].kill()
        st_auto = wait_for(
            lambda: (lambda m: m if m.get("mode") == "standalone" else None)(mode_view()), 20)
        st, _ = api(PORT_A, "/v1/tasks", {
            "task_id": "deg-ok-2", "type": "deg.echo", "priority": 100,
            "requires": ["cap:deg"], "params": {}})
        ok3 = bool(st_auto) and st == 202 and wait_for(
            lambda: task_state("deg-ok-2") == "succeeded", 20)
        ev3 = wait_for(lambda: "standalone" if "standalone" in mode_events() else None, 10)
        record("S3 停对端后自动 standalone，本地队列仍完成任务并审计",
               ok3 and ev3 == "standalone",
               {"mode": (st_auto or {}).get("mode"),
                "peer_silence": (st_auto or {}).get("peer_silence_seconds"),
                "task_state": task_state("deg-ok-2"),
                "mode_events": mode_events()})

        # ---- S4 重启 B → normal ---------------------------------------------
        procs["b"] = start(cfg_b, "b2.log")
        assert wait_for(lambda: probe(PORT_B), 25), "B 未重启"
        ok4 = wait_for(
            lambda: (lambda m: m if m.get("mode") == "normal"
                     and m.get("peers_online", 0) >= 1 else None)(mode_view()), 30)
        ev4 = wait_for(lambda: "normal" if mode_events().count("normal") >= 1 else None, 15)
        record("S4 对端恢复后回到 normal，审计出现 mode.change → normal",
               bool(ok4) and ev4 == "normal",
               {"mode": (ok4 or {}).get("mode"),
                "peers_online": (ok4 or {}).get("peers_online"),
                "mode_events": mode_events()})

        # ---- S5 存活/panic ---------------------------------------------------
        alive = all(p.poll() is None for p in procs.values())
        panics = []
        for name in ("a", "b", "b2"):
            log = os.path.join(ROOT, f"{name}.log")
            if os.path.exists(log) and "panic" in open(log).read().lower():
                panics.append(name)
        record("S5 两进程存活、日志无 panic", alive and not panics,
               {"alive": sum(1 for p in procs.values() if p.poll() is None),
                "panics": panics})

    finally:
        ex and ex.stop()
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
        with open(os.path.join(REPO, "tests", "degraded_smoke.jsonl"), "w") as f:
            for e in EVIDENCE:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        npass = sum(1 for e in EVIDENCE if e["pass"])
        print(f"degraded_smoke: {npass}/{len(EVIDENCE)} "
              f"→ {'OK' if npass == len(EVIDENCE) else 'FAILED'}", flush=True)
    sys.exit(0 if all(e["pass"] for e in EVIDENCE) else 1)


if __name__ == "__main__":
    main()
