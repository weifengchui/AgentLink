# -*- coding: utf-8 -*-
"""acceptance_sim — v0.6 验收门单机仿真。

验收门（路线图）：20 机并发 200 任务 0 丢失 0 崩溃；P99 调度延迟 <500ms；
外部用户 5 分钟装完跑通多机任务。

本机无 20 台真机，做**多进程多节点仿真**：10 个节点进程（星型静态 peer 组网）
+ 10 个常驻执行器（各自接本节点 WS，SDK 口径直读帧），一次并发提交 200 个
agentless 任务（引擎 bin-pack 跨节点派发），量测：
  A1 零丢失：200/200 终态 succeeded，表内无 pending/running 残留
  A2 零崩溃：10 个节点进程全程存活、日志无 panic
  A3 P99 调度延迟 <500ms（任务提交 →执行器收到 task_spec 的墙钟差，进程内
     单调钟同基准，无轮询噪声）
  A4 负载确实跨节点：quota_used（引擎派发过的不重复节点数）≥ 8
  A5 外部接入路径：节点均用「解压即跑」的最小配置（无历史/无外加依赖）

端口 18901-18910（隔离，绝不指向生产 8765）。
"""
import json
import os
import shutil
import signal
import statistics
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

BIN = os.environ.get("AL_BIN",
                     os.path.expanduser("~/agentlink-target/release/agentlink"))
ROOT = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch/al-accept")
TOKEN = "accept-sim-token"
N_NODES = int(os.environ.get("ACCEPT_NODES", "10"))
N_TASKS = int(os.environ.get("ACCEPT_TASKS", "200"))
MAX_CONC = int(os.environ.get("ACCEPT_CONC", "20"))  # 10×20=200 槽位：200 任务一波内派发
BASE_PORT = 18901
SEED_PORT = BASE_PORT  # node1 为星型种子
EVIDENCE = []


def record(name, ok, detail=""):
    EVIDENCE.append({"scenario": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + " | "
          + json.dumps(detail, ensure_ascii=False)[:220], flush=True)


def api(port, path, body=None, method=None, timeout=15):
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", method=method
        or ("POST" if body is not None else "GET"), data=data,
        headers={"X-Auth-Token": TOKEN, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode())
    except Exception as e:
        return 0, {"error": str(e)[:160]}


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
        if "al-accept" in cmd and "agentlink" in cmd:
            try:
                os.kill(int(pid), signal.SIGKILL)
            except Exception:
                pass


def node_cfg(i):
    """第 i 个节点（0-based）：node0 为种子（无 static_peers），其余指向种子。"""
    port = BASE_PORT + i
    nid = f"acc{i:02d}0000000"
    cfg = {
        "v": 1, "node_id": nid, "name": f"accept-{i}",
        "listen_port": port, "data_dir": os.path.join(ROOT, f"data{port}"),
        "discovery": {"enabled": False,
                      "static_peers": [] if i == 0
                      else [f"127.0.0.1:{SEED_PORT}"]},
        "security": {"mode": "strict", "cluster_token": "accept-cluster"},
        "local_api": {"host": "127.0.0.1", "token": TOKEN},
        "delivery": {"ack_timeout_seconds": 5, "max_retries": 5,
                     "offline_ttl_seconds": 600, "dedup_cache_size": 4096,
                     "persist": os.environ.get("ACCEPT_PERSIST", "0") == "1"},
        "log": {"level": "warn", "format": "text", "file": ""},
        "mesh": {"enabled": True, "license_file": "license.toml",
                 "engine": {"max_retries": 0, "tick_ms": 200,
                            "max_concurrent_per_node": MAX_CONC}},
    }
    path = os.path.join(ROOT, f"node{port}.json")
    json.dump(cfg, open(path, "w"))
    return port, nid, path


class Executor(threading.Thread):
    """常驻执行器：接本节点 WS，收 task_spec → 记录到达时刻 → 秒回状态。"""

    def __init__(self, node_index, port, node_id):
        super().__init__(daemon=True)
        self.agent = f"acc-worker{node_index}"
        self.port, self.node_id = port, node_id
        self.node_index = node_index
        self.recv_at = {}       # task_id -> monotonic
        self.running = True
        self.ws = None
        self._q = []            # 回执队列（收帧线程只入队，回执独立线程发）
        self._qlock = threading.Lock()

    def send(self, payload, reply_to=None):
        body = {"from": self.node_id, "to": "acc000000000",
                "from_agent": self.agent, "to_agent": "engine",
                "type": "text", "action": "send", "payload": payload}
        if reply_to:
            body["reply_to"] = reply_to
        api(self.port, "/v1/send", body)

    def run(self):
        # 注册 agent（含执行能力），随后接 WS。
        api(self.port, "/v1/agents", {"agent_id": self.agent, "name": self.agent,
                                      "capabilities": ["cap:shell:safe", "text"]})
        while self.running:
            try:
                self.ws = websocket.create_connection(
                    f"ws://127.0.0.1:{self.port}/v1/ws?token={TOKEN}&client={self.agent}",
                    timeout=None, ping_interval=25, ping_timeout=10)
                api(self.port, "/v1/agents", {
                    "agent_id": self.agent, "name": self.agent,
                    "capabilities": ["cap:shell:safe", "text"]})
                while self.running:
                    raw = self.ws.recv()
                    if not raw:
                        continue
                    env = json.loads(raw)
                    p = env.get("payload") or {}
                    if p.get("topic") != "task_spec":
                        continue
                    tid = p.get("task_id")
                    # 收帧立即记时（量的是引擎派发到达，不含执行器回执开销）
                    self.recv_at[tid] = time.monotonic()
                    with self._qlock:
                        self._q.append((tid, env.get("msg_id")))
            except Exception:
                if not self.running:
                    break
                time.sleep(0.5)

    def _reply_loop(self):
        while self.running:
            item = None
            with self._qlock:
                if self._q:
                    item = self._q.pop(0)
            if not item:
                time.sleep(0.005)
                continue
            tid, mid = item
            for topic, seq in (("task_accepted", 1), ("task_progress", 2),
                               ("task_succeeded", 3)):
                pl = {"topic": topic, "task_id": tid, "seq": seq}
                if seq == 2:
                    pl["stage"] = "apply"
                if seq == 3:
                    pl["result"] = {"node": self.node_index}
                self.send(pl, reply_to=mid)

    def start(self):
        threading.Thread(target=self._reply_loop, daemon=True).start()
        super().start()

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
        "tier": "starter", "node_quota": max(15, N_NODES + 2),
        "issued": "2026-10-01T00:00:00Z",
        "expires": "2036-12-31T23:59:59Z"}, "acceptance sim")
    open(os.path.join(ROOT, "license.toml"), "w").write(lic)
    for i in range(N_NODES):
        assert not probe(BASE_PORT + i), f"port {BASE_PORT + i} busy 隔离铁律"

    procs, logs, nodes, execs = [], [], [], []
    try:
        # ---- 起 N 个节点 + N 个常驻执行器 -------------------------------
        for i in range(N_NODES):
            port, nid, path = node_cfg(i)
            if i > 0:
                time.sleep(0.15)   # 种子先起，其余随后拨入
            lf = open(os.path.join(ROOT, f"node{port}.log"), "a")
            p = subprocess.Popen([BIN, "run", "--config", path, "--no-open"],
                                 stdout=lf, stderr=subprocess.STDOUT)
            procs.append(p)
            logs.append(lf)
            nodes.append((port, nid))
            end = time.monotonic() + 25
            while time.monotonic() < end and not probe(port):
                time.sleep(0.2)
            assert probe(port), f"节点 {i} 未启动"
        for (port, nid) in nodes:
            e = Executor(nodes.index((port, nid)), port, nid)
            e.start()
            execs.append(e)

        # 等 mesh 全联通：种子节点看到 N-1 个在线 peer。
        def all_peers():
            _, d = api(SEED_PORT, "/v1/peers")
            return len([p for p in d.get("peers", []) if p.get("online")]) >= N_NODES - 1
        mesh_ready = False
        end = time.monotonic() + 45
        while time.monotonic() < end:
            if all_peers():
                mesh_ready = True
                break
            time.sleep(0.5)
        record("A5 组网就绪（种子看到 N-1 在线 peer）", mesh_ready,
               {"peers_expected": N_NODES - 1})

        # 等所有执行器 agent 在各自节点在线（agent 列表同步有延迟）。
        time.sleep(3)

        # ---- 一次并发提交 N_TASKS 个 agentless 任务 ---------------------
        t_submit = {}
        accepted = 0
        submit_errors = 0
        t0 = time.monotonic()
        for k in range(N_TASKS):
            tid = f"acc-{k:04d}"
            t_submit[tid] = time.monotonic()
            st, _ = api(SEED_PORT, "/v1/tasks", {
                "task_id": tid, "type": "accept.echo",
                "intent": f"acceptance sim task {k}",
                "priority": 100, "requires": ["cap:shell:safe"],
                "params": {"k": k}})
            if st == 202:
                accepted += 1
            else:
                submit_errors += 1
        submit_span = time.monotonic() - t0

        # ---- 等全部收敛（引擎表视图：/v1/mesh/status by_state）------------
        final = None
        end = time.monotonic() + 120
        while time.monotonic() < end:
            _, d = api(SEED_PORT, "/v1/mesh/status")
            e = d.get("engine") or {}
            by = {k: v for k, v in (e.get("by_state") or [])}
            ok = (e.get("tasks_total") == N_TASKS
                  and e.get("tasks_pending") == 0
                  and e.get("tasks_running") == 0)
            if ok:
                final = (e, by)
                break
            time.sleep(0.3)
        wall = time.monotonic() - t0

        # ---- A1 零丢失 -------------------------------------------------
        eng, by = final or ({}, {})
        succ = by.get("succeeded", 0)
        record("A1 零丢失（200/200 终态 succeeded）",
               accepted == N_TASKS and succ == N_TASKS
               and eng.get("tasks_total") == N_TASKS
               and eng.get("tasks_pending", 1) == 0
               and eng.get("tasks_running", 1) == 0,
               {"accepted": accepted, "submit_errors": submit_errors,
                "succeeded": succ, "by_state": by,
                "tasks_total": eng.get("tasks_total")})

        # ---- A2 零崩溃 -------------------------------------------------
        alive = [p.poll() is None for p in procs]
        panics = []
        for (port, _) in nodes:
            try:
                txt = open(os.path.join(ROOT, f"node{port}.log")).read()
                if "panic" in txt.lower():
                    panics.append(port)
            except Exception:
                pass
        record("A2 零崩溃（10 进程存活、日志无 panic）",
               all(alive) and not panics,
               {"alive": sum(alive), "panics": panics})

        # ---- A3 P99 调度延迟 -------------------------------------------
        lat = []
        missing = 0
        for tid in t_submit:
            for e in execs:
                if tid in e.recv_at:
                    lat.append((e.recv_at[tid] - t_submit[tid]) * 1000.0)
                    break
            else:
                missing += 1
        lat.sort()
        p99 = lat[int(len(lat) * 0.99) - 1] if lat else None
        p50 = statistics.median(lat) if lat else None
        record("A3 P99 调度延迟 <500ms", bool(lat) and p99 is not None and p99 < 500,
               {"p50_ms": round(p50, 1) if p50 is not None else None,
                "p99_ms": round(p99, 1) if p99 is not None else None,
                "max_ms": round(max(lat), 1) if lat else None,
                "samples": len(lat), "missing": missing,
                "submit_span_s": round(submit_span, 2),
                "wall_s": round(wall, 2)})

        # ---- A4 跨节点负载 ---------------------------------------------
        _, ms = api(SEED_PORT, "/v1/mesh/status")
        quota_used = (ms.get("license") or {}).get("quota_used", 0)
        distinct_exec = len({e.node_index for e in execs
                             if any(t in e.recv_at for t in t_submit)})
        record("A4 调度跨节点（quota_used ≥ 8 且执行器分布 ≥8）",
               quota_used >= 8 and distinct_exec >= 8,
               {"quota_used": quota_used, "executors_used": distinct_exec})

    finally:
        for e in execs:
            e.stop()
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
        for lf in logs:
            lf.close()
        with open(os.path.join(REPO, "tests", "acceptance_sim.jsonl"), "w") as f:
            for e in EVIDENCE:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        npass = sum(1 for e in EVIDENCE if e["pass"])
        print(f"acceptance_sim: {npass}/{len(EVIDENCE)} "
              f"→ {'OK' if npass == len(EVIDENCE) else 'FAILED'}", flush=True)
    sys.exit(0 if all(e["pass"] for e in EVIDENCE) else 1)


if __name__ == "__main__":
    main()
