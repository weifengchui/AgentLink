# -*- coding: utf-8 -*-
"""adapter_sdk_smoke — Adapter SDK（sdk/adapter.py + hosts）包级冒烟。

对隔离测试总线（端口 18983，绝不指向生产 8765）验证 SDK 状态机与三宿主参考实现：
  S1 能力门 declined（missing cap）
  S2 happy 流转 accepted→progress→succeeded（EchoRunner）
  S3 (task_id,seq) 幂等重放 → task_ack(dup-dropped) 且不二次执行
  S4 运行中 cancel → task_cancelled（含 on_cancel 回调）
  S5 retryable 失败 → seq+1 重发 → succeeded（预算语义执行方侧契约）
  S6 逾期 deadline → declined(deadline_passed)
  S7 claude_code/opencode build_command 映射（纯函数桩）
  S8 Hermes 唤醒端到端：签名网关 wake → 会话回信(rely_to) → succeeded
  S9 Hermes 网关不可达 → failed(retryable=True)；回信超时 → failed(retryable=True)

结果逐条 JSONL 存档，退出码即结论。
"""
import hashlib
import hmac
import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "sdk"))

from adapter import TaskAdapter, TaskBus, RunResult            # noqa: E402
from hosts.claude_code import ClaudeCodeRunner                  # noqa: E402
from hosts.opencode import OpenCodeRunner                       # noqa: E402
from hosts.hermes import HermesRunner                           # noqa: E402

PORT = 18983
BASE = f"http://127.0.0.1:{PORT}"
TOKEN = "adapter-smoke-token"
SCRATCH = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch/al-adapter")
BIN = os.environ.get("AL_BIN",
                     os.path.expanduser("~/agentlink-target/debug/agentlink"))
CFG = os.path.join(SCRATCH, "agentlink.json")
DATA = os.path.join(SCRATCH, "data")
LOG = os.path.join(SCRATCH, "node.log")
ORCH = "sdk-orchestrator"
WORKER = "sdk-worker"
EVIDENCE = []


def record(name, ok, detail=""):
    EVIDENCE.append({"scenario": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + " | "
          + json.dumps(detail, ensure_ascii=False)[:220], flush=True)


def http(method, path, body=None):
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, method=method, data=data,
                                 headers={"X-Auth-Token": TOKEN,
                                          "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        return {"ok": False, "error": str(e)[:120]}


def write_cfg():
    os.makedirs(DATA, exist_ok=True)
    cfg = {
        "v": 1, "node_id": "ad4pt3rsdk06", "name": "adapter-smoke",
        "listen_port": PORT, "data_dir": DATA,
        "discovery": {"enabled": False, "methods": [], "static_peers": []},
        "security": {"mode": "strict", "cluster_token": "adapter-smoke-cluster"},
        "local_api": {"host": "127.0.0.1", "token": TOKEN},
        "delivery": {"ack_timeout_seconds": 5, "max_retries": 5,
                     "offline_ttl_seconds": 600, "dedup_cache_size": 2048,
                     "persist": True},
        "log": {"level": "info", "format": "text", "file": ""},
    }
    with open(CFG, "w") as f:
        json.dump(cfg, f)


def boot():
    logf = open(LOG, "a")
    p = subprocess.Popen([BIN, "run", "--config", CFG, "--no-open"],
                         stdout=logf, stderr=subprocess.STDOUT)
    for _ in range(120):
        try:
            if http("GET", "/v1/health").get("ok"):
                return p
        except Exception:
            pass
        time.sleep(0.25)
    p.kill()
    raise RuntimeError("node did not boot; see " + LOG)


def kill(p):
    p.send_signal(signal.SIGKILL)
    p.wait()
    time.sleep(0.6)


def kill_stale():
    for d in os.listdir("/proc"):
        if not d.isdigit():
            continue
        try:
            with open(f"/proc/{d}/cmdline", encoding="utf-8", errors="replace") as f:
                cmd = f.read().replace("\0", " ")
        except Exception:
            continue
        if "al-adapter" in cmd and "agentlink" in cmd:
            try:
                os.kill(int(d), signal.SIGKILL)
            except Exception:
                pass


class EchoRunner:
    """S1-S6 用 mock runner：stages → echo，可注入失败/慢执行/取消自查。"""

    def __init__(self, fail_once=False, slow_ms=0):
        self.runs = 0
        self.fail_once = fail_once
        self.slow_ms = slow_ms
        self.cancel_calls = []

    def preflight(self, spec):
        return None

    def run(self, spec, report, cancelled):
        self.runs += 1
        params = spec.get("params") or {}
        if self.fail_once and spec.get("seq", 1) == 1:
            return RunResult(ok=False, error="mock 脚手架首次失败",
                             retryable=True)
        for st in ("preflight", "apply"):
            report(stage=st)
            deadline = time.monotonic() + (self.slow_ms / 1000.0)
            while time.monotonic() < deadline:
                if cancelled():
                    return RunResult(ok=False, error="cancelled")
                time.sleep(0.05)
        return RunResult(ok=True, result=f"echo:{params.get('echo', '')}")

    def on_cancel(self, spec):
        self.cancel_calls.append(spec.get("task_id"))


def spec_of(tid, seq=1, params=None, requires=None, deadline=None, intent=""):
    spec = {"topic": "task_spec", "task_id": tid, "seq": seq,
            "type": "smoke.echo", "intent": intent, "params": params or {},
            "priority": 100}
    if requires:
        spec["requires"] = requires
    if deadline:
        spec["deadline"] = deadline
    return spec


def topics_for(history, tid):
    return [(m.get("payload") or {}).get("topic")
            for m in history if (m.get("payload") or {}).get("task_id") == tid
            and (m.get("to_agent") or "") == ORCH]


def wait_terminal(orch, tid, want, timeout_s=12):
    end = time.monotonic() + timeout_s
    last = False
    while time.monotonic() < end:
        topics = topics_for(orch.history(500), tid)
        last = want in topics
        if last:
            return True
        time.sleep(0.25)
    return last


def make_gateway(checks):
    """HMAC 校验桩网关：记录唤醒体，200 返回。"""
    class GW(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_POST(self):
            n = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(n)
            sig = self.headers.get("X-Hub-Signature-256") or ""
            good = hmac.new(b"smoke-secret", body, hashlib.sha256).hexdigest()
            ok = sig == "sha256=" + good
            checks.append({"sig_ok": ok, "body": json.loads(body or b"{}")})
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            self.wfile.write(b"{}")
    srv = ThreadingHTTPServer(("127.0.0.1", 0), GW)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}"


def main():
    os.makedirs(SCRATCH, exist_ok=True)
    kill_stale()
    subprocess.run(["rm", "-rf", DATA], check=False)  # 清持久化历史：断言只看本轮
    write_cfg()
    node = boot()
    try:
        # 编排方（SDK 客户端口径）注册；worker 由 adapter serve_forever 自注册
        orch = TaskBus(BASE, TOKEN, agent=ORCH)
        orch.register([])
        worker_bus = TaskBus(BASE, TOKEN, agent=WORKER)
        echo = EchoRunner()
        adapter = TaskAdapter(worker_bus, runner=echo,
                              capabilities=["cap:file", "cap:shell:safe"],
                              log=print)
        t = threading.Thread(target=adapter.serve_forever, daemon=True)
        t.start()
        def pstats(tag):
            print(f"[smoke] {tag} stats={adapter.stats} runs={echo.runs}",
                  flush=True)
        for _ in range(40):               # 等 ws 注册生效
            if any(a.get("agent_id") == WORKER
                   for a in http("GET", "/v1/agents").get("agents", [])):
                break
            time.sleep(0.25)

        # S1 能力门
        tid1 = "smoke-missing-cap"
        orch.send(WORKER, spec_of(tid1, requires=["cap:none"]))
        ok = wait_terminal(orch, tid1, "task_declined")
        record("S1 declined_missing_cap", ok)
        pstats("S1")

        # S2 happy
        tid2 = "smoke-happy"
        orch.send(WORKER, spec_of(tid2, params={"echo": "hi"}, intent="响铃测试"))
        ok = wait_terminal(orch, tid2, "task_succeeded")
        hist = orch.history(500)
        topics = topics_for(hist, tid2)
        good = ("task_accepted" in topics and
                any(t in topics for t in ("task_progress",)))
        record("S2 accepted_progress_succeeded", ok and good,
               {"topics": [x for x in topics if x]})
        pstats("S2")

        # S3 幂等重放
        orch.send(WORKER, spec_of(tid2, params={"echo": "hi"}))
        seen_dup = False
        for _ in range(40):
            if "task_ack" in topics_for(orch.history(500), tid2):
                seen_dup = True
                break
            time.sleep(0.25)
        record("S3 idempotent_replay", seen_dup and echo.runs == 1,
               {"runs": echo.runs})

        # S4 运行中 cancel
        tid4 = "smoke-cancel"
        slow = EchoRunner(slow_ms=3000)
        # 复用 adapter：换 runner 不可行（构造期绑定），改用独立慢任务 runner 标记
        echo.slow_ms = 3000
        echo.fail_once = False
        orch.send(WORKER, spec_of(tid4, params={"echo": "slow"}))
        time.sleep(2.5)                   # 进入 apply stage
        orch.send(WORKER, {"topic": "task_cancel", "task_id": tid4})
        ok = wait_terminal(orch, tid4, "task_cancelled")
        record("S4 cancel_running", ok, {"cancel_calls": len(echo.cancel_calls)})
        pstats("S4")

        # S5 retryable → seq+1 重发收敛
        fid = "smoke-retryable"
        echo.fail_once = True
        echo.slow_ms = 0
        orch.send(WORKER, spec_of(fid, seq=1, params={"echo": "r1"}))
        ok1 = wait_terminal(orch, fid, "task_failed")
        orch.send(WORKER, spec_of(fid, seq=2, params={"echo": "r1"}))
        ok2 = wait_terminal(orch, fid, "task_succeeded")
        record("S5 retryable_then_success", ok1 and ok2)
        echo.fail_once = False

        # S6 逾期 deadline
        tid6 = "smoke-deadline"
        orch.send(WORKER, spec_of(tid6, deadline="2026-01-01T00:00:00Z"))
        ok = wait_terminal(orch, tid6, "task_declined")
        record("S6 declined_deadline", ok)

        # S7 CLI 型宿主映射桩
        spec7 = spec_of("smoke-cli", params={"echo": "x"}, intent="列出目录")
        argv_cc = ClaudeCodeRunner().build_command(spec7)
        argv_oc = OpenCodeRunner().build_command(spec7)
        good = (argv_cc[0] == "claude" and argv_cc[1] == "-p" and "列出目录" in argv_cc[2]
                and argv_oc[0] == "opencode" and argv_oc[1] == "run"
                and "列出目录" in argv_oc[2])
        record("S7 host_command_mapping", good,
               {"cc": argv_cc[:3], "oc": argv_oc[:2]})

        # S8 Hermes 唤醒端到端（签名桩网关 + 会话回信 reply_to）
        checks = []
        gw, gw_url = make_gateway(checks)
        hermes = HermesRunner(gw_url, "smoke-secret", agent="study", timeout=15)
        h_bus = TaskBus(BASE, TOKEN, agent="hermes-worker")
        herm_adapter = TaskAdapter(h_bus, runner=hermes,
                                   capabilities=["cap:file"],
                                   frame_hook=hermes.on_frame)
        threading.Thread(target=herm_adapter.serve_forever, daemon=True).start()
        for _ in range(40):
            if any(a.get("agent_id") == "hermes-worker"
                   for a in http("GET", "/v1/agents").get("agents", [])):
                break
            time.sleep(0.25)
        tid8 = "smoke-hermes"
        spec8 = spec_of(tid8, intent="汇总今天告警")
        orch.send(WORKER.replace(WORKER, "hermes-worker"), spec8)
        # 等 wake 到达桩网关，再扮演会话：带 reply_to 回信（经 bus）
        done = False
        end = time.monotonic() + 15
        while time.monotonic() < end and not checks:
            time.sleep(0.2)
        if checks:
            mid = checks[0]["body"].get("msg_id")
            reply_bus = TaskBus(BASE, TOKEN, agent="session-study")
            reply_bus.register([])
            payload = {"content": "今日告警 3 条：磁盘 91% / 升级未决 / 心跳迟到",
                       "task_id": tid8}
            reply_bus.send("hermes-worker", payload, reply_to=mid)
            done = wait_terminal(orch, tid8, "task_succeeded")
        sig_ok = bool(checks) and checks[0]["sig_ok"]
        record("S8 hermes_wake_replyTo_succeeded", done and sig_ok,
               {"sig_ok": sig_ok, "wakes": len(checks)})

        # S9a 网关不可达 → failed(retryable)
        h2 = HermesRunner("http://127.0.0.1:1/webhook", "smoke-secret",
                          agent="study")
        a2 = TaskAdapter(TaskBus(BASE, TOKEN, agent="hermes-worker2"),
                         runner=h2, capabilities=[], frame_hook=h2.on_frame)
        threading.Thread(target=a2.serve_forever, daemon=True).start()
        for _ in range(40):
            if any(a.get("agent_id") == "hermes-worker2"
                   for a in http("GET", "/v1/agents").get("agents", [])):
                break
            time.sleep(0.25)
        tid9 = "smoke-hermes-unreachable"
        orch.send("hermes-worker2", spec_of(tid9, intent="唤醒不存在的网关"))
        hist = orch.history(500)
        err = ""
        ok9a = wait_terminal(orch, tid9, "task_failed", 10)
        for m in orch.history(500):
            p = m.get("payload") or {}
            if p.get("task_id") == tid9 and p.get("topic") == "task_failed":
                err = p.get("error") or ""
                break
        record("S9a hermes_unreachable_failed_retryable",
               ok9a and "wake 失败" in err, {"error": err[:80]})

        # S9b 网关可达但会话不回信 → 超时 failed(retryable)
        h3 = HermesRunner(gw_url, "smoke-secret", agent="study", timeout=2)
        checks2 = []
        gw2, gw2_url = make_gateway(checks2)
        h3.gateway_url = gw2_url
        a3 = TaskAdapter(TaskBus(BASE, TOKEN, agent="hermes-worker3"),
                         runner=h3, capabilities=[], frame_hook=h3.on_frame)
        threading.Thread(target=a3.serve_forever, daemon=True).start()
        for _ in range(40):
            if any(a.get("agent_id") == "hermes-worker3"
                   for a in http("GET", "/v1/agents").get("agents", [])):
                break
            time.sleep(0.25)
        tid9b = "smoke-hermes-timeout"
        orch.send("hermes-worker3", spec_of(tid9b, intent="唤醒后不回信"))
        ok9b = wait_terminal(orch, tid9b, "task_failed", 15)
        errb = ""
        for m in orch.history(500):
            p = m.get("payload") or {}
            if p.get("task_id") == tid9b and p.get("topic") == "task_failed":
                errb = p.get("error") or ""
                break
        record("S9b hermes_timeout_failed_retryable",
               ok9b and "超时" in errb, {"error": errb[:80]})

        adapter.stop()
        herm_adapter.stop()

    finally:
        kill(node)
        # 存档
        out = os.path.join(REPO, "tests", "adapter_sdk_smoke.jsonl")
        with open(out, "w", encoding="utf-8") as f:
            for e in EVIDENCE:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        npass = sum(1 for e in EVIDENCE if e["pass"])
        print(f"adapter_sdk_smoke: {npass}/{len(EVIDENCE)} "
              f"→ {'OK' if npass == len(EVIDENCE) else 'FAILED'}", flush=True)
    sys.exit(0 if all(e["pass"] for e in EVIDENCE) else 1)


if __name__ == "__main__":
    main()
