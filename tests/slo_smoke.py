# -*- coding: utf-8 -*-
"""slo_smoke — v0.7 §7.2 SLO 告警 + 历史基线异常检测。

拓扑：单节点引擎（S，mesh enabled）+ 1 个常驻执行器；另起一个关闭 SLO 的
节点验证缺省视图。webhook 用进程内 HTTP 接收器捕获 `slo_alert`。

场景：
  S0 SLO 视图可读且初始为空（enabled=true, samples=0）
  S1 4 成功 + 1 失败任务后样本/错误率/P99 正确
  S2 阈值与基线漂移同时命中（p99 / error_rate / baseline_drift / queue_depth）
  S3 webhook 收到 4 类 slo_alert
  S4 同类告警冷却：3s 内不重复触发
  S5 未启用节点返回 enabled=false / samples=0
  S6 进程存活、无 panic

端口 18942-18944（隔离，绝不指向生产 8765）。
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
from http.server import BaseHTTPRequestHandler, HTTPServer

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "tests"))
import engine_store_smoke as es  # noqa: E402
import websocket                 # noqa: E402

BIN = os.environ.get("AL_BIN",
                     os.path.expanduser("~/agentlink-target/release/agentlink"))
ROOT = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch/al-slo")
TOKEN = "slo-sim-token"
CLUSTER = "slo-cluster"
PORT = 18942
PORT_OFF = 18943
HOOK_PORT = 18944
NODE_ID = "510000000001"
NODE_OFF = "510000000002"
EVIDENCE = []

HOOKS = []
HOOK_LOCK = threading.Lock()


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
        if "al-slo" in cmd and "agentlink" in cmd:
            try:
                os.kill(int(pid), signal.SIGKILL)
            except Exception:
                pass


class Hook(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(n).decode("utf-8", "replace")
        with HOOK_LOCK:
            HOOKS.append(body)
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *a):
        pass


def write_cfg(name, port, node_id, *, slo=None, engine_extra=None):
    engine = {"max_retries": 0, "backoff_secs": 30, "tick_ms": 200,
              "max_concurrent_per_node": 2}
    if engine_extra:
        engine.update(engine_extra)
    if slo is not None:
        engine["slo"] = slo
    cfg = {
        "v": 1, "node_id": node_id, "name": name, "listen_port": port,
        "data_dir": os.path.join(ROOT, f"data{port}"),
        "history": {"enabled": True, "len": 400, "file": "history.jsonl"},
        "discovery": {"enabled": False},
        "security": {"mode": "strict", "cluster_token": CLUSTER},
        "local_api": {"host": "127.0.0.1", "token": TOKEN},
        "delivery": {"ack_timeout_seconds": 5, "max_retries": 5,
                     "offline_ttl_seconds": 600, "dedup_cache_size": 4096,
                     "persist": False},
        "log": {"level": "info", "format": "text", "file": ""},
        "mesh": {"enabled": True, "license_file": "license.toml", "engine": engine},
        "webhook": {"url": f"http://127.0.0.1:{HOOK_PORT}/hook",
                    "events": "slo_alert", "timeout_seconds": 2, "max_retries": 0},
    }
    path = os.path.join(ROOT, f"{name}.json")
    json.dump(cfg, open(path, "w"))
    return path


class Executor(threading.Thread):
    """收 task_spec：普通任务 50ms 后成功；`fail-*` 任务 20ms 后失败。"""

    def __init__(self, port, node_id):
        super().__init__(daemon=True)
        self.port, self.node_id = port, node_id
        self.agent = "slo-w"
        self.running = True
        self.ws = None
        self.seen = set()

    def send(self, payload):
        api(self.port, "/v1/send", {
            "from": self.node_id, "to": NODE_ID,
            "from_agent": self.agent, "to_agent": "engine",
            "type": "text", "action": "send", "payload": payload})

    def run(self):
        api(self.port, "/v1/agents", {"agent_id": self.agent, "name": self.agent,
                                      "capabilities": ["text", "cap:shell:safe"]})
        while self.running:
            try:
                self.ws = websocket.create_connection(
                    f"ws://127.0.0.1:{self.port}/v1/ws?token={TOKEN}&client={self.agent}",
                    ping_interval=25, ping_timeout=10)
                api(self.port, "/v1/agents", {"agent_id": self.agent, "name": self.agent,
                                              "capabilities": ["text", "cap:shell:safe"]})
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
        base = seq * 10
        time.sleep(0.02 if tid.startswith("fail-") else 0.05)
        if tid.startswith("fail-"):
            self.send({"topic": "task_failed", "task_id": tid, "seq": base + 2,
                       "error": "intentional", "retryable": False})
        else:
            self.send({"topic": "task_progress", "task_id": tid, "seq": base + 1,
                       "stage": "apply"})
            self.send({"topic": "task_succeeded", "task_id": tid, "seq": base + 2,
                       "result": {"ok": True}})

    def stop(self):
        self.running = False
        try:
            self.ws and self.ws.close()
        except Exception:
            pass


def slo_view():
    _, d = api(PORT, "/v1/mesh/slo")
    return d


def wait_for(fn, timeout_s=20, step=0.25):
    end = time.monotonic() + timeout_s
    while time.monotonic() < end:
        v = fn()
        if v:
            return v
        time.sleep(step)
    return fn()


def main():
    kill_stale()
    time.sleep(0.5)
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(ROOT, exist_ok=True)
    lic = es.sign_license_text({
        "tier": "starter", "node_quota": 15,
        "issued": "2026-10-01T00:00:00Z",
        "expires": "2036-12-31T23:59:59Z"}, "slo sim")
    open(os.path.join(ROOT, "license.toml"), "w").write(lic)
    for p in (PORT, PORT_OFF, HOOK_PORT):
        assert not probe(p), f"port {p} busy（生产隔离铁律）"

    # 预置历史基线：5ms（后续任务 50ms → 漂移 >1.3 必触发）。
    os.makedirs(os.path.join(ROOT, f"data{PORT}"), exist_ok=True)
    json.dump({"baseline_ms": 5.0, "samples": 100},
              open(os.path.join(ROOT, f"data{PORT}", "slo_baseline.json"), "w"))

    server = HTTPServer(("127.0.0.1", HOOK_PORT), Hook)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    slo_cfg = {"enabled": True, "window_seconds": 60, "min_samples": 3,
               "p99_ms": 1, "error_rate": 0.0, "queue_depth": 0,
               "baseline_ratio": 1.3, "cooldown_seconds": 60}
    procs, logs, ex = [], [], None
    try:
        cfg = write_cfg("main", PORT, NODE_ID, slo=slo_cfg)
        procs.append(subprocess.Popen(
            [BIN, "run", "--config", cfg, "--no-open"],
            stdout=open(os.path.join(ROOT, "main.log"), "a"),
            stderr=subprocess.STDOUT))
        assert wait_for(lambda: probe(PORT), 25), "主节点未启动"
        cfg_off = write_cfg("off", PORT_OFF, NODE_OFF)
        procs.append(subprocess.Popen(
            [BIN, "run", "--config", cfg_off, "--no-open"],
            stdout=open(os.path.join(ROOT, "off.log"), "a"),
            stderr=subprocess.STDOUT))
        assert wait_for(lambda: probe(PORT_OFF), 25), "对照节点未启动"
        time.sleep(2.0)

        # ---- S0 初始视图 ------------------------------------------------
        v0 = slo_view()
        record("S0 SLO 视图可读（enabled=true, 初始 samples=0）",
               v0.get("enabled") is True and v0.get("samples") == 0,
               {"enabled": v0.get("enabled"), "samples": v0.get("samples")})

        # ---- 派发 4 成功 + 1 失败 + 1 不可调度（队列深度）---------------
        ex = Executor(PORT, NODE_ID)
        ex.start()
        time.sleep(1.5)
        for i in range(4):
            api(PORT, "/v1/tasks", {
                "task_id": f"slo-{i}", "type": "slo.echo", "priority": 100,
                "requires": ["cap:shell:safe"], "params": {"i": i}})
        api(PORT, "/v1/tasks", {
            "task_id": "fail-0", "type": "slo.echo", "priority": 100,
            "requires": ["cap:shell:safe"], "params": {}})
        api(PORT, "/v1/tasks", {
            "task_id": "stuck-0", "type": "slo.echo", "priority": 100,
            "requires": ["cap:never-exists"], "params": {}})

        ready = wait_for(
            lambda: (lambda v: v if v.get("samples", 0) >= 5 else None)(slo_view()), 25)
        ok1 = bool(ready) and ready.get("errors") == 1
        record("S1 样本/错误率正确（5 样本、1 失败、P99 含执行等待）",
               bool(ready) and ready.get("samples", 0) >= 5 and ok1
               and ready.get("p99_ms", 0) >= 40,
               {"samples": ready.get("samples") if ready else None,
                "errors": ready.get("errors") if ready else None,
                "error_rate": ready.get("error_rate") if ready else None,
                "p99_ms": ready.get("p99_ms") if ready else None})

        # ---- S2 阈值 + 基线漂移 ------------------------------------------
        active = set((ready or {}).get("active") or [])
        want = {"p99", "error_rate", "baseline_drift", "queue_depth"}
        record("S2 四类阈值/漂移全部命中（p99/error_rate/drift/queue）",
               want.issubset(active),
               {"active": sorted(active), "drift_ratio": (ready or {}).get("drift_ratio"),
                "baseline_ms": (ready or {}).get("baseline_ms")})

        # ---- S3 webhook 收到 slo_alert -----------------------------------
        def hook_kinds():
            kinds = set()
            with HOOK_LOCK:
                for body in HOOKS:
                    try:
                        d = json.loads(body)
                        payload = d.get("payload") or d
                        if payload.get("kind"):
                            kinds.add(payload["kind"])
                    except Exception:
                        pass
            return kinds
        kinds = wait_for(lambda: hook_kinds() if want.issubset(hook_kinds()) else None, 20) or hook_kinds()
        record("S3 webhook 收到 4 类 slo_alert", want.issubset(kinds),
               {"kinds": sorted(kinds), "deliveries": len(HOOKS)})

        # ---- S4 冷却：3s 内同类不重复 ------------------------------------
        before = len(slo_view().get("alerts") or [])
        time.sleep(3.2)
        after = len(slo_view().get("alerts") or [])
        record("S4 同类告警冷却（3s 内告警数不增）", after == before,
               {"alerts_before": before, "alerts_after": after})

        # ---- S5 未启用节点 ------------------------------------------------
        _, off = api(PORT_OFF, "/v1/mesh/slo")
        record("S5 未启用 SLO 的节点返回 enabled=false / samples=0",
               off.get("enabled") is False and off.get("samples") == 0,
               {"enabled": off.get("enabled"), "samples": off.get("samples")})

        # ---- S6 存活/崩溃 --------------------------------------------------
        alive = all(p.poll() is None for p in procs)
        panics = []
        for name in ("main", "off"):
            txt = open(os.path.join(ROOT, f"{name}.log")).read()
            if "panic" in txt.lower():
                panics.append(name)
        record("S6 进程存活、日志无 panic", alive and not panics,
               {"alive": sum(1 for p in procs if p.poll() is None), "panics": panics})

    finally:
        ex and ex.stop()
        server.shutdown()
        for p in procs:
            try:
                p.send_signal(signal.SIGINT)
            except Exception:
                pass
        for p in procs:
            try:
                p.wait(timeout=6)
            except Exception:
                p.kill()
        with open(os.path.join(REPO, "tests", "slo_smoke.jsonl"), "w") as f:
            for e in EVIDENCE:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        npass = sum(1 for e in EVIDENCE if e["pass"])
        print(f"slo_smoke: {npass}/{len(EVIDENCE)} "
              f"→ {'OK' if npass == len(EVIDENCE) else 'FAILED'}", flush=True)
    sys.exit(0 if all(e["pass"] for e in EVIDENCE) else 1)


if __name__ == "__main__":
    main()
