# -*- coding: utf-8 -*-
"""namespace_smoke — v0.7 §4.2 多租户 namespace 隔离（第一波）。

拓扑：S 引擎（18956）+ W1（18957，team-a）+ W2（18958，team-b）。
所有任务 requires cap:ns。策略：
  - 任务只能落到同 namespace（或 namespace="*" 共享池）的 agent；
  - token ↔ namespace 绑定：绑定 token 只能提交/查看/操作本租户任务与 agent；
    未显式 namespace 时自动注入绑定值；
  - 跨租户 task_id/agent 的 get/cancel/delete/注册/退租/发信一律拒绝。

场景：
  S0 fleet 视图带 namespace，绑定 token 只看到本租户 agent
  S1 team-a / team-b 任务分别只落各自执行器
  S2 绑定 token 显式跨租户提交 403；省略 namespace 自动归位并完成
  S3 绑定 token 的任务列表/单查/撤销均被 namespace 过滤（跨租户 404）
  S4 无匹配 agent 时保持 pending；namespace="*" 共享池 agent 上线后接管
  S5 agent 注册/退租 scope：跨租户注册 403、自动归位、跨租户删除 403
  S6 /v1/send 不能借 from_agent 冒充其它租户
  S7 进程存活、日志无 panic

端口 18956-18958（隔离，绝不指向生产 8765）。
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
ROOT = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch/al-ns")
CLUSTER = "ns-cluster"
ADMIN = "ns-admin"
TOK_A, TOK_B = "ns-tok-a", "ns-tok-b"
OP_A, OP_B = "ns-op-a", "ns-op-b"
PORT_S, PORT_W1, PORT_W2 = 18956, 18957, 18958
NODE_S, NODE_W1, NODE_W2 = "560000000001", "560000000002", "560000000003"
EVIDENCE = []


def record(name, ok, detail=""):
    EVIDENCE.append({"scenario": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + " | "
          + json.dumps(detail, ensure_ascii=False)[:350], flush=True)


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
        if "al-ns" in cmd and "agentlink" in cmd:
            try:
                os.kill(int(pid), signal.SIGKILL)
            except Exception:
                pass


def write_cfg(name, port, node_id, peers, *, mesh=False, tokens=None, namespaces=None):
    cfg = {
        "v": 1, "node_id": node_id, "name": name, "listen_port": port,
        "data_dir": os.path.join(ROOT, f"data{port}"),
        "history": {"enabled": True, "len": 500, "file": "history.jsonl"},
        "discovery": {"enabled": False,
                      "static_peers": [f"127.0.0.1:{p}" for p in peers]},
        "security": {"mode": "strict", "cluster_token": CLUSTER},
        "local_api": {"host": "127.0.0.1", "token": ADMIN if mesh else name + "-admin"},
        "delivery": {"ack_timeout_seconds": 5, "max_retries": 5,
                     "offline_ttl_seconds": 600, "dedup_cache_size": 4096,
                     "persist": False},
        "log": {"level": "info", "format": "text", "file": ""},
        "mesh": {"enabled": mesh, "license_file": "license.toml",
                 "engine": {"max_retries": 0, "backoff_secs": 1, "tick_ms": 200,
                            "max_concurrent_per_node": 2}},
    }
    if tokens:
        cfg["auth"] = {"tokens": tokens}
        if namespaces:
            cfg["auth"]["token_namespaces"] = namespaces
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


def task_spec(tid):
    _, t = api(PORT_S, f"/v1/tasks/{tid}")
    return (t.get("task") or {}).get("spec") or {}


def fleet_online(agent):
    _, d = api(PORT_S, "/v1/fleet/agents")
    for a in d.get("agents") or []:
        if a.get("agent_id") == agent:
            return bool(a.get("online"))
    return False


def submit(tid, namespace=None, token=ADMIN):
    body = {"task_id": tid, "type": "ns.echo", "priority": 100,
            "requires": ["cap:ns"], "params": {}}
    if namespace is not None:
        body["namespace"] = namespace
    return api(PORT_S, "/v1/tasks", body, token=token)


class Executor(threading.Thread):
    def __init__(self, port, node_id, agent_id, token, namespace=None):
        super().__init__(daemon=True)
        self.port, self.node_id, self.agent = port, node_id, agent_id
        self.token, self.namespace = token, namespace
        self.running = True
        self.ws = None
        self.seen = set()
        self.specs = []
        self.lock = threading.Lock()

    def register(self):
        body = {"agent_id": self.agent, "name": self.agent,
                "capabilities": ["text", "cap:ns"]}
        if self.namespace is not None:
            body["namespace"] = self.namespace
        return api(self.port, "/v1/agents", body, token=self.token)

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
                        self.specs.append(tid)
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
        "expires": "2036-12-31T23:59:59Z"}, "ns sim")
    open(os.path.join(ROOT, "license.toml"), "w").write(lic)
    for p in (PORT_S, PORT_W1, PORT_W2):
        assert not probe(p), f"port {p} busy（生产隔离铁律）"

    procs = {}
    ex1 = ex2 = shared = None
    try:
        cfg_s = write_cfg("s", PORT_S, NODE_S, [PORT_W1, PORT_W2], mesh=True,
                          tokens={TOK_A: "operator", TOK_B: "operator"},
                          namespaces={TOK_A: "team-a", TOK_B: "team-b"})
        cfg_1 = write_cfg("w1", PORT_W1, NODE_W1, [PORT_S],
                          tokens={OP_A: "operator"},
                          namespaces={OP_A: "team-a"})
        cfg_2 = write_cfg("w2", PORT_W2, NODE_W2, [PORT_S],
                          tokens={OP_B: "operator"},
                          namespaces={OP_B: "team-b"})
        procs["s"] = start(cfg_s, "s.log")
        assert wait_for(lambda: probe(PORT_S), 25), "S 未启动"
        procs["w1"] = start(cfg_1, "w1.log")
        assert wait_for(lambda: probe(PORT_W1), 25), "W1 未启动"
        procs["w2"] = start(cfg_2, "w2.log")
        assert wait_for(lambda: probe(PORT_W2), 25), "W2 未启动"
        # WS 只认 local_api.token（RBAC token 不能订阅），执行器用本机
        # admin token 建连/注册，namespace 显式声明。
        ex1 = Executor(PORT_W1, NODE_W1, "ns-w1", "w1-admin", namespace="team-a")
        ex1.start()
        ex2 = Executor(PORT_W2, NODE_W2, "ns-w2", "w2-admin", namespace="team-b")
        ex2.start()
        assert wait_for(lambda: fleet_online("ns-w1") and fleet_online("ns-w2"), 30), \
            "执行器未全部上线"

        # ---- S0 fleet namespace + token 过滤 --------------------------------
        _, d = api(PORT_S, "/v1/fleet/agents")
        ns = {a.get("agent_id"): a.get("namespace") for a in d.get("agents") or []}
        _, da = api(PORT_S, "/v1/fleet/agents", token=TOK_A)
        names_a = {a.get("agent_id") for a in da.get("agents") or []}
        record("S0 fleet 带 namespace；绑定 token 只看到本租户 agent",
               ns.get("ns-w1") == "team-a" and ns.get("ns-w2") == "team-b"
               and names_a == {"ns-w1"},
               {"namespaces": ns, "team_a_view": sorted(names_a)})

        # ---- S1 任务按 namespace 落各自执行器 -------------------------------
        submit("ns-ta-1", "team-a")
        submit("ns-tb-1", "team-b")
        ok1 = (wait_for(lambda: task_state("ns-ta-1") == "succeeded", 25)
               and wait_for(lambda: task_state("ns-tb-1") == "succeeded", 25))
        record("S1 team-a / team-b 任务分别只落各自执行器",
               bool(ok1) and ex1.got("ns-ta-1") and not ex2.got("ns-ta-1")
               and ex2.got("ns-tb-1") and not ex1.got("ns-tb-1"),
               {"ta": [task_state("ns-ta-1"), ex1.got("ns-ta-1"), ex2.got("ns-ta-1")],
                "tb": [task_state("ns-tb-1"), ex2.got("ns-tb-1"), ex1.got("ns-tb-1")]})

        # ---- S2 绑定 token 的 namespace 强制 --------------------------------
        st_bad, _ = submit("ns-ta-bad", "team-b", token=TOK_A)
        st_ok, _ = submit("ns-ta-2", None, token=TOK_A)
        ok2 = wait_for(lambda: task_state("ns-ta-2") == "succeeded", 25)
        spec2 = task_spec("ns-ta-2")
        record("S2 绑定 token 跨租户提交 403；省略 namespace 自动归位 team-a",
               st_bad == 403 and st_ok == 202 and bool(ok2)
               and spec2.get("namespace") == "team-a" and ex1.got("ns-ta-2"),
               {"bad": st_bad, "ok": st_ok, "state": task_state("ns-ta-2"),
                "namespace": spec2.get("namespace")})

        # ---- S3 读/写面 namespace 过滤 -------------------------------------
        _, lst = api(PORT_S, "/v1/tasks", token=TOK_A)
        ids = {t.get("task_id") for t in lst.get("tasks") or []}
        st_get, _ = api(PORT_S, "/v1/tasks/ns-tb-1", token=TOK_A)
        st_cancel, _ = api(PORT_S, "/v1/tasks/ns-tb-1/cancel",
                           body={}, token=TOK_A)
        record("S3 绑定 token 只看到本租户任务；跨租户单查/撤销 404",
               "ns-ta-1" in ids and "ns-ta-2" in ids and "ns-tb-1" not in ids
               and st_get == 404 and st_cancel == 404,
               {"tasks": sorted(ids), "get": st_get, "cancel": st_cancel})

        # ---- S4 无匹配 → pending；共享池 agent 接管 -------------------------
        submit("ns-tc-1", "team-c")
        time.sleep(2.5)
        pending = task_state("ns-tc-1") == "pending"
        nobody = not ex1.got("ns-tc-1") and not ex2.got("ns-tc-1")
        shared = Executor(PORT_W2, NODE_W2, "ns-shared", "w2-admin", namespace="*")
        shared.start()
        assert wait_for(lambda: fleet_online("ns-shared"), 25), "共享池 agent 未上线"
        ok4 = wait_for(lambda: task_state("ns-tc-1") == "succeeded", 25)
        record("S4 team-c 无匹配时 pending；namespace=* 共享池上线后接管",
               pending and nobody and bool(ok4) and shared.got("ns-tc-1"),
               {"pending": pending, "nobody": nobody,
                "state": task_state("ns-tc-1"), "shared": shared.got("ns-tc-1")})

        # ---- S5 agent 注册/退租 scope --------------------------------------
        st_reg_bad, _ = api(PORT_W1, "/v1/agents",
                            {"agent_id": "ns-x", "namespace": "team-b",
                             "capabilities": ["text"]}, token=OP_A)
        st_reg_ok, reg = api(PORT_W1, "/v1/agents",
                             {"agent_id": "ns-a2", "capabilities": ["text"]},
                             token=OP_A)
        _, alist = api(PORT_W1, "/v1/agents", token=OP_A)
        reg_ids = {a.get("agent_id") for a in alist.get("agents") or []}
        # W1 上用未绑定 admin 建一个 team-b 的本地 agent，验证 OP_A 无权删除。
        api(PORT_W1, "/v1/agents",
            {"agent_id": "ns-b-local", "namespace": "team-b",
             "capabilities": ["text"]}, token="w1-admin")
        st_del_bad, _ = api(PORT_W1, "/v1/agents",
                            {"agent_id": "ns-b-local"}, method="DELETE", token=OP_A)
        api(PORT_W1, "/v1/agents",
            {"agent_id": "ns-b-local"}, method="DELETE", token="w1-admin")
        st_del_ok, _ = api(PORT_W1, "/v1/agents",
                           {"agent_id": "ns-a2"}, method="DELETE", token=OP_A)
        # 同名 agent 不能跨 namespace 静默接管。
        st_coll, _ = api(PORT_W1, "/v1/agents",
                         {"agent_id": "ns-w1", "namespace": "team-b",
                          "capabilities": ["text"]}, token="w1-admin")
        record("S5 agent 注册自动归位 team-a；跨租户注册/删除 403；同名接管 409",
               st_reg_bad == 403 and st_reg_ok == 200
               and (reg.get("agent") or {}).get("namespace") == "team-a"
               and "ns-a2" in reg_ids and st_del_bad == 403 and st_del_ok == 200
               and st_coll == 409,
               {"reg_bad": st_reg_bad, "reg_ok": st_reg_ok,
                "auto_ns": (reg.get("agent") or {}).get("namespace"),
                "del_bad": st_del_bad, "del_ok": st_del_ok, "collision": st_coll})

        # ---- S6 /v1/send 不能跨租户冒充 ------------------------------------
        st_spoof, _ = api(PORT_S, "/v1/send", {
            "to": NODE_S, "type": "text", "action": "send",
            "from_agent": "ns-w2", "payload": {"topic": "chat", "text": "hi"}},
            token=TOK_A)
        st_own, _ = api(PORT_S, "/v1/send", {
            "to": NODE_S, "type": "text", "action": "send",
            "from_agent": "ns-w1", "payload": {"topic": "chat", "text": "hi"}},
            token=TOK_A)
        record("S6 绑定 token 用其它租户 from_agent 发信 403，本租户 200",
               st_spoof == 403 and st_own == 200,
               {"spoof": st_spoof, "own": st_own})

        # ---- S7 全局可观测面对绑定 token 关闭 -------------------------------
        st_hist, _ = api(PORT_S, "/v1/history", token=TOK_A)
        st_events, _ = api(PORT_S, "/v1/events", token=TOK_A)
        st_files, _ = api(PORT_S, "/v1/files/history.jsonl", token=TOK_A)
        st_tasks_ok, _ = api(PORT_S, "/v1/tasks", token=TOK_A)
        record("S7 绑定 token 读全局 history/events/files 403，任务面仍可用",
               st_hist == 403 and st_events == 403 and st_files == 403
               and st_tasks_ok == 200,
               {"history": st_hist, "events": st_events,
                "files": st_files, "tasks": st_tasks_ok})

        # ---- S8 存活/panic ---------------------------------------------------
        alive = all(p.poll() is None for p in procs.values())
        panics = []
        for name in ("s", "w1", "w2"):
            log = os.path.join(ROOT, f"{name}.log")
            if os.path.exists(log) and "panic" in open(log).read().lower():
                panics.append(name)
        record("S8 三进程存活、日志无 panic", alive and not panics,
               {"alive": sum(1 for p in procs.values() if p.poll() is None),
                "panics": panics})

    finally:
        for ex in (ex1, ex2, shared):
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
        with open(os.path.join(REPO, "tests", "namespace_smoke.jsonl"), "w") as f:
            for e in EVIDENCE:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        npass = sum(1 for e in EVIDENCE if e["pass"])
        print(f"namespace_smoke: {npass}/{len(EVIDENCE)} "
              f"→ {'OK' if npass == len(EVIDENCE) else 'FAILED'}", flush=True)
    sys.exit(0 if all(e["pass"] for e in EVIDENCE) else 1)


if __name__ == "__main__":
    main()
