# -*- coding: utf-8 -*-
"""chaos_smoke — v0.7 chaos 第一波：kill-node ×3 自动回收任务。

拓扑（6 进程、直接星型）：种子 S（引擎，mesh enabled） + 5 个执行节点 W1..W5。

场景：
  K0 组网：S 看到 5 个在线 peer，agent 全部在线
  K1 派发：6 个慢任务（执行 12s）分散到 5 个 worker（每节点并发 1）
  K2 依次 kill W1/W2/W3（每个都持有在飞任务）
  K3 自动回收：引擎判定节点失联（宽限 3s）→ 任务置 failed 立即到期 →
     既有 backoff 重投路径换机重派（max_retries=8）
  K4 全部收敛：6/6 succeeded，无 pending/running 残留
  K5 回收证据：被 kill 节点上的任务确实在幸存节点重跑（attempts≥1），
     引擎日志有 chaos reclaim 记录
  K6 幸存节点无崩溃、无 panic；被 kill 的进程确已退出

端口 18934-18939（隔离，绝不指向生产 8765）。
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
ROOT = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch/al-chaos")
TOKEN = "chaos-sim-token"
CLUSTER = "chaos-cluster"
SEED_PORT = 18934
SEED_ID = "5e0000000001"
N_WORKERS = 5
N_TASKS = 6
SLOW_SECONDS = 12.0
BASE_PORT = 18935
EVIDENCE = []


def record(name, ok, detail=""):
    EVIDENCE.append({"scenario": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + " | "
          + json.dumps(detail, ensure_ascii=False)[:260], flush=True)


def api(port, path, body=None, timeout=15):
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
        return 0, {"error": str(e)[:120]}


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
        if "al-chaos" in cmd and "agentlink" in cmd:
            try:
                os.kill(int(pid), signal.SIGKILL)
            except Exception:
                pass


def worker_id(i):
    return f"a0000000000{i + 1}"


def write_cfg(name, port, node_id, *, engine=None, static_peers=()):
    cfg = {
        "v": 1, "node_id": node_id, "name": name, "listen_port": port,
        "data_dir": os.path.join(ROOT, f"data{port}"),
        "history": {"enabled": True, "len": 400, "file": "history.jsonl"},
        "discovery": {"enabled": False, "static_peers": list(static_peers),
                      "heartbeat_interval_seconds": 2, "offline_timeout_seconds": 6},
        "security": {"mode": "strict", "cluster_token": CLUSTER},
        "local_api": {"host": "127.0.0.1", "token": TOKEN},
        "delivery": {"ack_timeout_seconds": 5, "max_retries": 5,
                     "offline_ttl_seconds": 600, "dedup_cache_size": 4096,
                     "persist": False},
        "log": {"level": "info", "format": "text", "file": ""},
    }
    if engine is not None:
        cfg["mesh"] = {"enabled": True, "license_file": "license.toml", "engine": engine}
    path = os.path.join(ROOT, f"{name}.json")
    json.dump(cfg, open(path, "w"))
    return path


class SlowExecutor(threading.Thread):
    """收 task_spec → 睡 SLOW_SECONDS 再回报（kill 窗口内必然在飞）。"""

    def __init__(self, index, port, node_id):
        super().__init__(daemon=True)
        self.index, self.port, self.node_id = index, port, node_id
        self.agent = f"chaos-w{index}"
        self.running = True
        self.ws = None
        self.seen = set()
        self.started = set()     # 本节点收到过的 task_id（含重派）
        self.inflight = set()    # 尚未回报终态的 task_id
        self.lock = threading.Lock()
        self.reply = []          # (task_id, delay_until)

    def send(self, payload):
        api(self.port, "/v1/send", {
            "from": self.node_id, "to": SEED_ID,
            "from_agent": self.agent, "to_agent": "engine",
            "type": "text", "action": "send", "payload": payload})

    def run(self):
        api(self.port, "/v1/agents", {"agent_id": self.agent, "name": self.agent,
                                      "capabilities": ["text", "cap:shell:safe"]})
        threading.Thread(target=self._reply_loop, daemon=True).start()
        while self.running:
            try:
                self.ws = websocket.create_connection(
                    f"ws://127.0.0.1:{self.port}/v1/ws?token={TOKEN}&client={self.agent}",
                    ping_interval=25, ping_timeout=10)
                api(self.port, "/v1/agents", {
                    "agent_id": self.agent, "name": self.agent,
                    "capabilities": ["text", "cap:shell:safe"]})
                while self.running:
                    raw = self.ws.recv()
                    if not raw:
                        continue
                    env = json.loads(raw)
                    p = env.get("payload") or {}
                    if p.get("topic") != "task_spec":
                        continue
                    tid, seq = p.get("task_id"), p.get("seq", 1)
                    key = (tid, seq)
                    with self.lock:
                        if key in self.seen:
                            continue
                        self.seen.add(key)
                        self.started.add(tid)
                        self.inflight.add(tid)
                    self.reply.append((tid, time.monotonic() + SLOW_SECONDS))
            except Exception:
                if not self.running:
                    break
                time.sleep(0.5)

    def _reply_loop(self):
        while self.running:
            due = None
            now = time.monotonic()
            with self.lock:
                for item in list(self.reply):
                    if item[1] <= now:
                        due = item
                        self.reply.remove(item)
                        break
            if not due:
                time.sleep(0.05)
                continue
            tid = due[0]
            # seq 固定 10/11：首轮节点在回报前被 kill，不会与重试撞键；
            # 重复重试时 progress 可能撞键被引擎忽略，终态 11 仍可收敛。
            self.send({"topic": "task_progress", "task_id": tid, "seq": 10,
                       "stage": "apply"})
            self.send({"topic": "task_succeeded", "task_id": tid, "seq": 11,
                       "result": {"node": self.index}})
            with self.lock:
                self.inflight.discard(tid)

    def stop(self):
        self.running = False
        try:
            self.ws and self.ws.close()
        except Exception:
            pass


def main():
    kill_stale()
    time.sleep(0.6)
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(ROOT, exist_ok=True)
    lic = es.sign_license_text({
        "tier": "starter", "node_quota": 15,
        "issued": "2026-10-01T00:00:00Z",
        "expires": "2036-12-31T23:59:59Z"}, "chaos sim")
    open(os.path.join(ROOT, "license.toml"), "w").write(lic)
    for p in [SEED_PORT] + [BASE_PORT + i for i in range(N_WORKERS)]:
        assert not probe(p), f"port {p} busy（生产隔离铁律）"

    procs = {}
    execs = {}
    try:
        engine = {
            "max_retries": 8, "backoff_secs": 0, "tick_ms": 200,
            "max_concurrent_per_node": 1,
            "reclaim": {"enabled": True, "grace_seconds": 3, "max_attempts": 3},
        }
        seed_cfg = write_cfg("seed", SEED_PORT, SEED_ID, engine=engine)
        procs["seed"] = subprocess.Popen(
            [BIN, "run", "--config", seed_cfg, "--no-open"],
            stdout=open(os.path.join(ROOT, "seed.log"), "a"),
            stderr=subprocess.STDOUT)
        assert wait_for(lambda: probe(SEED_PORT), 25), "seed 未启动"
        for i in range(N_WORKERS):
            port = BASE_PORT + i
            cfg = write_cfg(f"w{i + 1}", port, worker_id(i),
                            static_peers=[f"127.0.0.1:{SEED_PORT}"])
            procs[f"w{i + 1}"] = subprocess.Popen(
                [BIN, "run", "--config", cfg, "--no-open"],
                stdout=open(os.path.join(ROOT, f"w{i + 1}.log"), "a"),
                stderr=subprocess.STDOUT)
            assert wait_for(lambda p=port: probe(p), 25), f"w{i + 1} 未启动"
            time.sleep(0.1)
        for i in range(N_WORKERS):
            e = SlowExecutor(i + 1, BASE_PORT + i, worker_id(i))
            e.start()
            execs[f"w{i + 1}"] = e
        time.sleep(3)

        # ---- K0 组网 ------------------------------------------------------
        def online_peers():
            return [p for p in api(SEED_PORT, "/v1/peers")[1].get("peers", [])
                    if p.get("online")]

        def agents_online():
            ags = api(SEED_PORT, "/v1/fleet/agents")[1].get("agents", [])
            mine = [a for a in ags if a.get("agent_id", "").startswith("chaos-w")]
            return len(mine) == N_WORKERS and all(a.get("online") for a in mine)

        ok0 = wait_for(lambda: len(online_peers()) == N_WORKERS, 25)
        agents = wait_for(agents_online, 20)
        record("K0 组网（S 见 5 个在线 peer + 5 个在线 agent）", ok0 and bool(agents),
               {"peers": len(online_peers()),
                "agents": len(api(SEED_PORT, "/v1/fleet/agents")[1].get("agents", []))})

        # ---- K1 派发 6 个慢任务 ------------------------------------------
        submit = {}
        accepted = 0
        for k in range(N_TASKS):
            tid = f"chaos-{k:02d}"
            submit[tid] = time.monotonic()
            st, _ = api(SEED_PORT, "/v1/tasks", {
                "task_id": tid, "type": "chaos.slow",
                "intent": f"chaos kill-node task {k}", "priority": 100,
                "requires": ["cap:shell:safe"], "params": {"k": k}})
            if st == 202:
                accepted += 1
        distributed = wait_for(
            lambda: sum(1 for e in execs.values() if e.started) >= 5, 30)
        record("K1 6 个慢任务已派发（≥5 个 worker 在飞）", accepted == N_TASKS and bool(distributed),
               {"accepted": accepted,
                "started_per_worker": {k: sorted(v.started) for k, v in execs.items()}})

        # ---- K2 依次 kill 3 个持有在飞任务的 worker ----------------------
        victims, killed_tasks = [], {}
        for idx, (name, delay) in enumerate([("w1", 2.0), ("w2", 4.0), ("w3", 4.0)]):
            time.sleep(delay)
            e = execs[name]
            with e.lock:
                inflight = sorted(e.inflight)
            killed_tasks[name] = inflight
            procs[name].send_signal(signal.SIGINT)
            try:
                procs[name].wait(timeout=6)
            except Exception:
                procs[name].kill()
            victims.append(name)
            if not inflight:
                time.sleep(1.0)
        ok2 = all(killed_tasks[n] for n in victims)
        record("K2 依次 kill W1/W2/W3（每个都持有在飞任务）", ok2,
               {"killed": killed_tasks})

        # ---- K3/K4 收敛：6/6 succeeded ----------------------------------
        final = None
        end = time.monotonic() + 150
        while time.monotonic() < end:
            _, d = api(SEED_PORT, "/v1/mesh/status")
            e = d.get("engine") or {}
            by = dict(e.get("by_state") or [])
            if (e.get("tasks_total") == N_TASKS and e.get("tasks_pending") == 0
                    and e.get("tasks_running") == 0):
                final = (e, by)
                break
            time.sleep(0.3)
        eng, by = final or ({}, {})
        ok4 = bool(final) and by.get("succeeded", 0) == N_TASKS
        record("K3/K4 kill-node×3 后全部收敛（6/6 succeeded、无残留）", ok4,
               {"by_state": by, "tasks_total": eng.get("tasks_total"),
                "pending": eng.get("tasks_pending"), "running": eng.get("tasks_running")})

        # ---- K5 回收证据 --------------------------------------------------
        moved = {}
        attempts = {}
        for tid in [t for lst in killed_tasks.values() for t in lst]:
            reruns = [k for k, e in execs.items() if k not in victims and tid in e.started]
            _, d = api(SEED_PORT, f"/v1/tasks/{tid}")
            att = (d.get("task") or {}).get("attempts")
            attempts[tid] = att
            if reruns and att and att >= 1:
                moved[tid] = reruns
        seed_log = open(os.path.join(ROOT, "seed.log")).read()
        reclaim_lines = seed_log.count("chaos reclaim")
        ok5 = bool(killed_tasks) and len(moved) >= 1 and reclaim_lines >= 3
        record("K5 自动回收证据（重跑 attempts≥1 + 引擎 reclaim 日志 ≥3）", ok5,
               {"moved": moved, "attempts": attempts, "reclaim_log_lines": reclaim_lines})

        # ---- K6 存活与崩溃 ------------------------------------------------
        alive = [p.poll() is None for k, p in procs.items() if k not in victims]
        panics = []
        for name in ("seed", "w4", "w5"):
            try:
                txt = open(os.path.join(ROOT, f"{name}.log")).read()
                if "panic" in txt.lower():
                    panics.append(name)
            except Exception:
                pass
        record("K6 幸存者无崩溃 / 被 kill 进程已退出",
               all(alive) and not panics
               and all(procs[n].poll() is not None for n in victims),
               {"alive": sum(alive), "panics": panics})

    finally:
        for e in execs.values():
            e.stop()
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
        with open(os.path.join(REPO, "tests", "chaos_smoke.jsonl"), "w") as f:
            for e in EVIDENCE:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        npass = sum(1 for e in EVIDENCE if e["pass"])
        print(f"chaos_smoke: {npass}/{len(EVIDENCE)} "
              f"→ {'OK' if npass == len(EVIDENCE) else 'FAILED'}", flush=True)
    sys.exit(0 if all(e["pass"] for e in EVIDENCE) else 1)


def wait_for(fn, timeout_s=20, step=0.2):
    end = time.monotonic() + timeout_s
    while time.monotonic() < end:
        v = fn()
        if v:
            return v
        time.sleep(step)
    return fn()


if __name__ == "__main__":
    main()
