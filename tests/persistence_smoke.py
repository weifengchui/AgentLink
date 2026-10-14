#!/usr/bin/env python3
"""持久化重启冒烟测试（v0.4.12 引入）：journal/agents.json/history/inbox
在 SIGKILL 后完整恢复。隔离端口 18765 测试实例，防串入生产 mesh。
用法: python3 tests/persistence_smoke.py  (需先 cargo build --release，
或改 BIN 指向构建产物；全部 14 项 PASS 退出码为 0。"""
import json, os, signal, subprocess, time, urllib.error, urllib.request

SCRATCH = "/home/yxf/.hermes/profiles/agentlink/cache/scratch/persist-smoke"
BIN = os.path.expanduser("~/agentlink-target/release/agentlink")
PORT = 18765
TOKEN = "smoketes...OKEN"
NODE = "a1b2c3d4e5f6"
DATA = os.path.join(SCRATCH, "data")
CFG = os.path.join(SCRATCH, "agentlink.json")
LOG = os.path.join(SCRATCH, "node.log")
API = f"http://127.0.0.1:{PORT}"

cfg = {
    "v": 1, "node_id": NODE, "name": "persist-smoke", "listen_port": PORT,
    "data_dir": DATA,
    "discovery": {"enabled": False, "methods": [], "static_peers": []},
    "security": {"mode": "strict", "cluster_token": "smoketest-cluster"},
    "local_api": {"host": "127.0.0.1", "token": TOKEN},
    "delivery": {"ack_timeout_seconds": 5, "max_retries": 5,
                 "offline_ttl_seconds": 600, "dedup_cache_size": 2048,
                 "persist": True},
    "log": {"level": "info", "format": "text", "file": ""},
}

def http(method, path, body=None):
    req = urllib.request.Request(API + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"X-Auth-Token": TOKEN, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return {"_http_error": e.code, "body": e.read().decode()[:300]}

def boot():
    logf = open(LOG, "a")
    p = subprocess.Popen([BIN, "run", "--config", CFG, "--no-open"],
                         stdout=logf, stderr=subprocess.STDOUT)
    for _ in range(60):
        try:
            h = http("GET", "/v1/health")
            if h.get("ok"):
                return p, h
        except Exception:
            pass
        time.sleep(0.25)
    raise RuntimeError("node did not boot; see " + LOG)

results = []
def check(name, cond, detail=""):
    results.append(f"{'PASS' if cond else 'FAIL'}  {name}  {detail}".rstrip())

# fresh data dir
subprocess.run(["rm", "-rf", DATA], check=True)
os.makedirs(DATA, exist_ok=True)
with open(CFG, "w") as f:
    json.dump(cfg, f)

proc, h = boot()
check("boot", h.get("node_id") == NODE, f"uptime={h.get('uptime_seconds')}")

# 1. register an agent (tests agents.json snapshot persistence)
r = http("POST", "/v1/agents", {"agent_id": "alice", "name": "Alice",
                                "capabilities": ["test"]})
check("agent-register", r.get("ok", False), json.dumps(r)[:120])

# 2. msgs to unregistered local agent -> offline queue (ttl 600s)
sent_ids = []
for i in range(3):
    r = http("POST", "/v1/send", {"to": NODE, "type": "text", "action": "send",
        "from_agent": "orchestrator", "to_agent": "ghost",
        "payload": {"content": f"offline-probe-{i}"}})
    if r.get("ok"):
        sent_ids.append(r.get("msg_id"))
check("send-offline-queued", len(sent_ids) == 3, f"got={len(sent_ids)}")

# 3. msg to registered-but-not-connected agent -> inbox path
r = http("POST", "/v1/send", {"to": NODE, "type": "text", "action": "send",
    "from_agent": "orchestrator", "to_agent": "alice",
    "payload": {"content": "inbox-probe"}})
if r.get("ok"):
    sent_ids.append(r.get("msg_id"))
check("send-to-alice", bool(r.get("ok")), json.dumps(r)[:120])

time.sleep(1.0)
st1 = http("GET", "/v1/status")
h1 = http("GET", "/v1/history?limit=200")
msgs1 = [m.get("msg_id") for m in h1.get("history", [])]
check("status-durable-flag", st1.get("durable") is True, json.dumps(st1)[:200])
jpath = os.path.join(DATA, "journal.jsonl")
jlines1 = sum(1 for _ in open(jpath)) if os.path.exists(jpath) else 0
check("journal-file-exists", jlines1 > 0, f"lines={jlines1}")
apath = os.path.join(DATA, "agents.json")
check("agents-snapshot-exists", os.path.exists(apath))

# ---- hard kill ----
os.kill(proc.pid, signal.SIGKILL)
proc.wait()
time.sleep(0.5)

proc2, h2 = boot()
check("reboot", h2.get("node_id") == NODE, f"uptime={h2.get('uptime_seconds')}")
st2 = http("GET", "/v1/status")
h2r = http("GET", "/v1/history?limit=200")
msgs2 = [m.get("msg_id") for m in h2r.get("history", [])]
ag2 = http("GET", "/v1/agents")

check("offline-queue-recovered",
      st2.get("offline_queue", -1) == st1.get("offline_queue", -2),
      f"before={st1.get('offline_queue')} after={st2.get('offline_queue')}")
check("inbox-recovered", st2.get("inbox", -1) == st1.get("inbox", -2),
      f"before={st1.get('inbox')} after={st2.get('inbox')}")
check("history-recovered", all(m in msgs2 for m in msgs1),
      f"n1={len(msgs1)} n2={len(msgs2)}")
agents_after = json.dumps(ag2)
check("agent-registry-recovered", "alice" in agents_after, agents_after[:150])

# new message after reboot still works (journal append not broken)
r = http("POST", "/v1/send", {"to": NODE, "type": "text", "action": "send",
    "from_agent": "orchestrator", "to_agent": "ghost",
    "payload": {"content": "post-reboot-probe"}})
time.sleep(0.6)
st3 = http("GET", "/v1/status")
check("send-after-reboot", r.get("ok") and st3.get("inbox", 0) >=
      (st2.get("inbox", 0) + 1),
      f"inbox {st2.get('inbox')} -> {st3.get('inbox')}")

# ---- cleanup ----
proc2.terminate()
proc2.wait()
jlines2 = sum(1 for _ in open(jpath)) if os.path.exists(jpath) else 0
check("journal-append-after-reboot", jlines2 > jlines1, f"{jlines1} -> {jlines2}")

print("\n".join(results))
n_fail = sum(1 for x in results if x.startswith("FAIL"))
print(f"\n== {len(results)} checks, {n_fail} failed ==")
raise SystemExit(1 if n_fail else 0)
