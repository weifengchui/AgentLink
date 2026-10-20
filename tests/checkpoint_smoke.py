# -*- coding: utf-8 -*-
"""checkpoint_smoke — v0.7 §6.2 任务迁移 + checkpoint。

拓扑：S 引擎（18949）+ W1（18950）+ W2（18952），W1/W2 只跑执行器；
S 的 reclaim 宽限期 10s（用于验证 drain 快速通道）。

场景：
  S0 三节点连通、W1 执行器在线
  S1 W1 上报 checkpoint{offset:50} 后硬杀 W1 → 宽限期后换机 W2，
     W2 收到的重投 spec 携带 checkpoint，完成后 result.resumed_from=50
  S2 W1 重启；仅 W1 在线时提交 ck-b，W1 上报 checkpoint{offset:50} 后
     优雅 SIGTERM W1（广播 node_drain）→ S 跳过宽限期立即换机 W2，
     W2 从 30 续跑成功且耗时 < 10s 宽限期
  S3 进程存活、日志无 panic

端口 18949/18950/18952（隔离，绝不指向生产 8765）。
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
sys.path.insert(0, os.path.join(REPO, "sdk"))
import engine_store_smoke as es  # noqa: E402
import websocket                 # noqa: E402
from adapter import RunResult, TaskAdapter  # noqa: E402

BIN = os.environ.get("AL_BIN",
                     os.path.expanduser("~/agentlink-target/release/agentlink"))
ROOT = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch/al-checkpoint")
TOKEN = "checkpoint-sim-token"
CLUSTER = "checkpoint-cluster"
PORT_S, PORT_W1, PORT_W2 = 18949, 18950, 18952
NODE_S, NODE_W1, NODE_W2 = "540000000001", "540000000002", "540000000003"
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
        if "al-checkpoint" in cmd and "agentlink" in cmd:
            try:
                os.kill(int(pid), signal.SIGKILL)
            except Exception:
                pass


def write_cfg(name, port, node_id, peers, *, mesh=False, reclaim=False):
    cfg = {
        "v": 1, "node_id": node_id, "name": name, "listen_port": port,
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
                            "max_concurrent_per_node": 1,
                            "reclaim": {"enabled": reclaim,
                                        "grace_seconds": 10,
                                        "max_attempts": 3}}},
    }
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


def task_view(tid):
    _, t = api(PORT_S, f"/v1/tasks/{tid}")
    return t.get("task") or t


def task_state(tid):
    return task_view(tid).get("state")


class _FakeBus:
    """SDK 钩子单测用：只记录 send，不连网络。"""

    agent = "fake-worker"

    def __init__(self):
        self.sent = []

    def register(self, capabilities=None):
        return {"ok": True}

    def send(self, to, payload, reply_to=None, to_node=None):
        self.sent.append(payload)
        return {"ok": True}


class _CheckpointRunner:
    def __init__(self):
        self.release = threading.Event()

    def preflight(self, spec):
        return None

    def run(self, spec, report, cancelled):
        report(stage="start")
        self.release.wait(5)
        return RunResult(ok=True, result={"done": True})

    def on_cancel(self, spec):
        pass

    def checkpoint(self, spec):
        return {"offset": 42, "artifact": "file:///tmp/sdk.ckpt"}


class Executor(threading.Thread):
    """收 spec：无 checkpoint → 上报首断点后挂起；有 checkpoint → 立即续跑成功。"""

    def __init__(self, port, node_id, agent_id, first_offset):
        super().__init__(daemon=True)
        self.port, self.node_id = port, node_id
        self.agent, self.first_offset = agent_id, first_offset
        self.running = True
        self.ws = None
        self.seen = set()
        self.specs = {}          # tid -> 收到的 checkpoint（dict/None）
        self.sent_cp = {}        # tid -> 上报过的 offset
        self.lock = threading.Lock()

    def register(self):
        api(self.port, "/v1/agents", {"agent_id": self.agent, "name": self.agent,
                                      "capabilities": ["text", "cap:ck"]})

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
                    threading.Thread(target=self._run_one, args=(tid, seq, p),
                                     daemon=True).start()
            except Exception:
                if not self.running:
                    break
                time.sleep(0.5)

    def _run_one(self, tid, seq, spec):
        cp = spec.get("checkpoint")
        with self.lock:
            # 同一任务换机重投会带新 seq：保留最近一次收到的 checkpoint。
            self.specs.setdefault(tid, []).append(cp)
        base = seq * 10
        if cp:
            self.send({"topic": "task_progress", "task_id": tid, "seq": base + 1,
                       "stage": "resume", "checkpoint": cp})
            time.sleep(0.1)
            total = (spec.get("params") or {}).get("total", 100)
            self.send({"topic": "task_succeeded", "task_id": tid, "seq": base + 2,
                       "result": {"resumed_from": cp.get("offset"),
                                  "total": total, "node": self.node_id}})
            return
        # 首次执行：存断点后挂起（不完成任务），等待被 kill/切走。
        cp_out = {"offset": self.first_offset, "node": self.node_id,
                  "artifact": f"file:///tmp/{tid}.ckpt"}
        with self.lock:
            self.sent_cp[tid] = self.first_offset
        self.send({"topic": "task_progress", "task_id": tid, "seq": base + 1,
                   "stage": "half", "checkpoint": cp_out})
        for i in range(600):
            if not self.running:
                return
            time.sleep(1.0)
            self.send({"topic": "task_progress", "task_id": tid,
                       "seq": base + 100 + i, "stage": "heartbeat"})

    def stop(self):
        self.running = False
        try:
            self.ws and self.ws.close()
        except Exception:
            pass


def fleet_online(agent):
    _, d = api(PORT_S, "/v1/fleet/agents")
    for a in d.get("agents") or []:
        if a.get("agent_id") == agent:
            return bool(a.get("online"))
    return False


def peers_online():
    _, d = api(PORT_S, "/v1/status")
    return d.get("peers_online", 0)


def w1_offline():
    _, d = api(PORT_S, "/v1/peers")
    for p in d.get("peers") or []:
        if p.get("node_id") == NODE_W1:
            return p.get("online") is False
    return False


def main():
    kill_stale()
    time.sleep(0.5)
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(ROOT, exist_ok=True)
    lic = es.sign_license_text({
        "tier": "starter", "node_quota": 15,
        "issued": "2026-10-01T00:00:00Z",
        "expires": "2036-12-31T23:59:59Z"}, "checkpoint sim")
    open(os.path.join(ROOT, "license.toml"), "w").write(lic)
    for p in (PORT_S, PORT_W1, PORT_W2):
        assert not probe(p), f"port {p} busy（生产隔离铁律）"

    procs = {}
    ex1 = ex2 = None
    try:
        cfg_s = write_cfg("s", PORT_S, NODE_S, [PORT_W1, PORT_W2],
                          mesh=True, reclaim=True)
        cfg_1 = write_cfg("w1", PORT_W1, NODE_W1, [PORT_S])
        cfg_2 = write_cfg("w2", PORT_W2, NODE_W2, [PORT_S])
        procs["s"] = start(cfg_s, "s.log")
        assert wait_for(lambda: probe(PORT_S), 25), "S 未启动"
        procs["w1"] = start(cfg_1, "w1.log")
        assert wait_for(lambda: probe(PORT_W1), 25), "W1 未启动"
        ex1 = Executor(PORT_W1, NODE_W1, "ck-w1", 50)
        ex1.start()
        assert wait_for(lambda: fleet_online("ck-w1"), 25), "W1 执行器未上线"
        assert wait_for(lambda: peers_online() >= 1, 25), "S 未连上 W1"
        record("S0 拓扑就绪（W1 执行器在线；W2 待 S1 启动）", True,
               {"peers_online": peers_online()})

        # ---- S1 checkpoint + 硬杀 + 宽限换机 --------------------------------
        api(PORT_S, "/v1/tasks", {
            "task_id": "ck-a", "type": "ck.stream", "priority": 100,
            "requires": ["cap:ck"], "params": {"total": 100}})
        assert wait_for(lambda: ex1.sent_cp.get("ck-a") == 50, 20), "W1 未上报断点"
        procs["w2"] = start(cfg_2, "w2.log")
        assert wait_for(lambda: probe(PORT_W2), 25), "W2 未启动"
        ex2 = Executor(PORT_W2, NODE_W2, "ck-w2", 10)
        ex2.start()
        # 常驻 dummy agent：只注销最后一个 agent 时 agents_update 为空，
        # 既有实现不会广播空表；留一个不匹配 cap:ck 的 agent 保证变更能同步。
        api(PORT_W2, "/v1/agents", {"agent_id": "ck-w2-idle", "name": "idle",
                                    "capabilities": ["text"]})
        assert wait_for(lambda: fleet_online("ck-w2"), 25), "W2 执行器未上线"
        procs["w1"].kill()  # 硬杀：只能等 reclaim 宽限期
        procs["w1"].wait(timeout=5)
        ok1 = wait_for(lambda: task_state("ck-a") == "succeeded", 40)
        tv = task_view("ck-a")
        cps = ex2.specs.get("ck-a") or []
        recv_cp = cps[-1] if cps else None
        record("S1 硬杀 W1：宽限后重派 W2，重投 spec 带 checkpoint 并续跑成功",
               bool(ok1) and isinstance(recv_cp, dict)
               and recv_cp.get("offset") == 50
               and tv.get("attempts", 0) >= 1,
               {"state": tv.get("state"), "attempts": tv.get("attempts"),
                "w2_checkpoint": recv_cp})

        # ---- S2 优雅 SIGTERM：drain 跳过宽限期 ------------------------------
        # 重启 W1（同一配置/数据目录），执行器线程会自动重连并重新注册。
        procs["w1"] = start(cfg_1, "w1b.log")
        assert wait_for(lambda: probe(PORT_W1), 25), "W1 未重启"
        assert wait_for(lambda: fleet_online("ck-w1"), 30), "W1 执行器未重新上线"
        # 只留 W1 一个候选：注销 W2 执行器。
        ex2.unregister()
        assert wait_for(lambda: not fleet_online("ck-w2"), 15), "W2 未注销"
        api(PORT_S, "/v1/tasks", {
            "task_id": "ck-b", "type": "ck.stream", "priority": 100,
            "requires": ["cap:ck"], "params": {"total": 100}})
        assert wait_for(lambda: ex1.sent_cp.get("ck-b") == 50, 20), "W1 未上报 ck-b 断点"
        t0 = time.monotonic()
        procs["w1"].send_signal(signal.SIGTERM)
        # 立刻让 W2 重新具备接收能力（drain 分支必须不依赖 10s 宽限）。
        ex2.register()
        ok2 = wait_for(lambda: task_state("ck-b") == "succeeded", 25)
        elapsed = time.monotonic() - t0
        tv2 = task_view("ck-b")
        cps2 = ex2.specs.get("ck-b") or []
        recv2 = cps2[-1] if cps2 else None
        record("S2 优雅 SIGTERM + node_drain：<10s 换机并带断点续跑",
               bool(ok2) and elapsed < 10
               and isinstance(recv2, dict) and recv2.get("offset") == 50
               and wait_for(w1_offline, 10),
               {"elapsed_s": round(elapsed, 2), "state": tv2.get("state"),
                "attempts": tv2.get("attempts"),
                "w2_checkpoint": recv2,
                "w1_offline": w1_offline()})

        # ---- S3 存活/panic ---------------------------------------------------
        alive = all(p.poll() is None for p in procs.values() if p is not procs.get("w1"))
        panics = []
        for name in ("s", "w1", "w1b", "w2"):
            log = os.path.join(ROOT, f"{name}.log")
            if os.path.exists(log) and "panic" in open(log).read().lower():
                panics.append(name)
        record("S3 S/W2 存活、日志无 panic", alive and not panics,
               {"s_alive": procs["s"].poll() is None,
                "w2_alive": procs["w2"].poll() is None, "panics": panics})

        # ---- S4 Adapter SDK：SIGTERM 钩子把 checkpoint 上报 -----------------
        bus = _FakeBus()
        runner = _CheckpointRunner()
        ad = TaskAdapter(bus, runner, capabilities=["cap:ck"])
        ad.handle_frame({
            "type": "text", "from_agent": "orch", "reply_to": "r1",
            "payload": {"topic": "task_spec", "task_id": "sdk-ck", "seq": 1,
                        "requires": ["cap:ck"], "params": {"total": 100}}})
        wait_for(lambda: any(p.get("stage") == "start" for p in bus.sent), 5)
        sent = ad.checkpoint_all()
        ok4 = wait_for(
            lambda: any(p.get("topic") == "task_progress"
                        and (p.get("checkpoint") or {}).get("offset") == 42
                        for p in bus.sent), 5)
        runner.release.set()
        ad.stop()
        record("S4 Adapter SDK checkpoint 钩子：在途任务上报 offset=42",
               bool(ok4) and sent == 1,
               {"sent": sent,
                "checkpoints": [p.get("checkpoint") for p in bus.sent
                                if p.get("topic") == "task_progress"
                                and p.get("checkpoint")]})

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
        with open(os.path.join(REPO, "tests", "checkpoint_smoke.jsonl"), "w") as f:
            for e in EVIDENCE:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        npass = sum(1 for e in EVIDENCE if e["pass"])
        print(f"checkpoint_smoke: {npass}/{len(EVIDENCE)} "
              f"→ {'OK' if npass == len(EVIDENCE) else 'FAILED'}", flush=True)
    sys.exit(0 if all(e["pass"] for e in EVIDENCE) else 1)


if __name__ == "__main__":
    main()
