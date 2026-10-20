#!/usr/bin/env python3
"""AgentLink bridge daemon for Hermes.

Subscribes to the local AgentLink node WS, dedupes by msg_id, stores
business messages into an inbox (max 1000), and exposes a tiny localhost
HTTP control API for Hermes sessions to poll / reply / register agents.

Control API (127.0.0.1:8766):
  GET  /status                      -> daemon+node status json
  GET  /inbox                       -> pending (unacked) envelopes json
  POST /ack {"msg_id": "..."}       -> mark consumed
  POST /send {to,type,action,payload,to_agent,from_agent,reply_to,ttl_seconds}
  POST /register {"agent_id","name","capabilities":[..]}  -> also persisted in registry.json (re-registered on reconnect)

Config: node url + token read from E:\\Tools\\AgentLink-v0.2.0\\agentlink.json (local_api.token, listen_port).
"""
import json, os, sys, time, threading, subprocess, urllib.request, urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from collections import deque

try:
    import websocket  # websocket-client
except ImportError:
    print("need websocket-client in this python env"); sys.exit(1)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path: sys.path.insert(0, BASE_DIR)
try:
    import wake_hook  # 可选唤醒钩子：wake.json 存在即生效，缺失自动静默
except Exception:
    wake_hook = None

def resolve_config():
    """找本机节点配置。

    优先顺序（与 agentlink 二进制一致）：
      1. AGENTLINK_CONFIG 环境变量；
      2. 稳定位置（升级不覆盖）：Windows %LOCALAPPDATA%\\AgentLink\\agentlink.json
         （WSL 侧映射到 /mnt/c/Users/*/AppData/Local/AgentLink/），或
         Linux ~/.config/agentlink/agentlink.json；
      3. 发行目录里的旧配置（已被二进制自动迁移到稳定位置，这里兜底读旧的）。
    """
    import glob
    env = os.environ.get("AGENTLINK_CONFIG")
    if env and os.path.exists(env):
        return env
    # 2) 稳定位置：Windows LOCALAPPDATA（WSL 可达）
    for pat in ("/mnt/c/Users/*/AppData/Local/AgentLink/agentlink.json",
                os.path.expanduser("~/.config/agentlink/agentlink.json"),
                os.path.expanduser("~/.agentlink/agentlink.json")):
        for p in sorted(glob.glob(pat)):
            if os.path.isfile(p):
                return p
    # 3) 发行目录里的旧配置（按版本号取最新）
    cands = sorted(glob.glob("/mnt/e/Tools/AgentLink-v*/agentlink.json"),
                   key=lambda p: [int(x) for x in __import__("re").findall(r"\d+", p)])
    if cands:
        return cands[-1]  # newest version dir
    return "/mnt/e/Tools/AgentLink-v0.4.4/agentlink.json"

NODE_CONFIG = resolve_config()
REGISTRY = os.path.join(BASE_DIR, "registry.json")
STATE = os.path.join(BASE_DIR, "state.json")
CTL_HOST, CTL_PORT = "127.0.0.1", 8766
SELF_AGENT_DEFAULT = "liteguard"

lock = threading.Lock()
inbox = deque(maxlen=1000)          # envelopes
seen = deque(maxlen=2000)          # msg_ids
seen_set = set()
events = deque(maxlen=100)         # join/leave/status events
node_info = {"node_id": None, "name": None, "ws_connected": False, "last_msg_ts": None,
             "started_ts": time.time(), "self_agent": SELF_AGENT_DEFAULT, "peers": []}

def load_conf():
    with open(NODE_CONFIG, "r", encoding="utf-8") as f:
        c = json.load(f)
    port = c.get("listen_port", 8765)
    token = c["local_api"]["token"]
    return f"http://127.0.0.1:{port}", token

def load_state():
    global seen_set
    try:
        with open(STATE) as f:
            s = json.load(f)
        for mid in s.get("seen", [])[-2000:]:
            if mid not in seen_set:
                seen_set.add(mid); seen.append(mid)
        node_info["self_agent"] = s.get("self_agent", SELF_AGENT_DEFAULT)
    except Exception:
        pass

def save_state():
    tmp = STATE + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"seen": list(seen)[-2000:], "self_agent": node_info["self_agent"]}, f)
    os.replace(tmp, STATE)

def load_registry():
    try:
        with open(REGISTRY) as f:
            return json.load(f)
    except Exception:
        return []

def save_registry(agents):
    tmp = REGISTRY + ".tmp"
    with open(tmp, "w") as f:
        json.dump(agents, f, ensure_ascii=False, indent=1)
    os.replace(tmp, REGISTRY)

CONF = None
def api(path, body=None, method=None, tries=3):
    """Call AgentLink node HTTP API."""
    global CONF
    base, token = load_conf(); CONF = (base, token)
    for i in range(tries):
        try:
            data = json.dumps(body).encode() if body is not None else None
            req = urllib.request.Request(base + path, data=data,
                headers={"X-Auth-Token": token, "Content-Type": "application/json"},
                method=method or ("POST" if data else "GET"))
            with urllib.request.urlopen(req, timeout=10) as r:
                return json.loads(r.read().decode())
        except Exception as e:
            if i == tries - 1:
                return {"ok": False, "error": str(e)}
            time.sleep(1)

def mark_seen(mid):
    with lock:
        if mid in seen_set:
            return False
        seen_set.add(mid); seen.append(mid)
        if len(seen_set) > 2000:
            old = seen.popleft(); seen_set.discard(old)
        return True

def register_all():
    """Register default + registry agents on the node."""
    agents = [
        {"agent_id": "liteguard", "name": "liteguard", "capabilities": ["综合助手", "系统操作", "代码", "文件", "消息处理"]},
    ] + [x for x in load_registry() if x["agent_id"] != "liteguard"]
    for a in agents:
        r = api("/v1/agents", {"agent_id": a["agent_id"], "name": a.get("name", a["agent_id"]),
                               "capabilities": a.get("capabilities", [])}, "POST")
        ev = {"t": time.strftime("%H:%M:%S"), "event": "register", "agent_id": a["agent_id"], "resp_ok": r.get("ok")}
        with lock: events.append(ev)

UPGRADE = {"running": False}

def upgrade_report(env, topic, extra=None):
    """向 notice/go 的发起方定点回报（点对点 at-least-once）。"""
    body = {"to": env.get("from"), "from_agent": node_info["self_agent"],
            "type": "text", "action": "send",
            "payload": dict({"topic": topic, "agent": node_info["self_agent"]}, **(extra or {}))}
    if env.get("from_agent"):
        body["reply_to"] = env.get("msg_id")
        body["to_agent"] = env["from_agent"]
    api("/v1/send", body, "POST")

def _find_upgrade_zip(file_name):
    node_dir = os.path.dirname(NODE_CONFIG)
    try:
        with open(NODE_CONFIG, encoding="utf-8") as f:
            cfg = json.load(f)
        recv = cfg.get("files", {}).get("recv_dir", "./data/files")
        cands = [os.path.join(node_dir, recv, file_name),
                 os.path.join(cfg.get("data_dir", node_dir + "/data"), "files", file_name)]
    except Exception:
        cands = []
    for c in cands:
        if c and os.path.exists(c):
            return c
    return None

def run_upgrade(env):
    p = env.get("payload") or {}
    try:
        if UPGRADE["running"]:
            upgrade_report(env, "upgrade_failed", {"reason": "already running"}); return
        zp = _find_upgrade_zip(p.get("file_name") or "")
        if not zp:
            upgrade_report(env, "upgrade_failed", {"reason": f"zip not found: {p.get('file_name')}"}); return
        UPGRADE["running"] = True
        with lock:
            events.append({"t": time.strftime("%H:%M:%S"), "event": "upgrade-start", "new": p.get("new")})
        cmd = [sys.executable, os.path.join(BASE_DIR, "upgrade_runner.py"),
               "--config", NODE_CONFIG, "--zip", zp,
               "--expect-version", str(p.get("new")),
               "--trigger", f"msg:{env.get('msg_id','')}"]
        if p.get("sha256"):
            cmd += ["--sha256", p["sha256"]]
        if p.get("git_ledger"):
            cmd += ["--git-ledger", p["git_ledger"]]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        final = {0: "upgrade_done", 3: "upgrade_rolled_back"}.get(r.returncode, "upgrade_failed")
        detail = {}
        try:
            with open(os.path.join(os.path.dirname(NODE_CONFIG), "upgrade-ledger.jsonl"), encoding="utf-8") as f:
                last = f.readlines()[-1].strip()
            detail = json.loads(last) if last else {}
        except Exception:
            pass
        detail.update({"rc": r.returncode, "runner_tail": (r.stdout or "")[-300:]})
        upgrade_report(env, final, detail)
        with lock:
            events.append({"t": time.strftime("%H:%M:%S"), "event": final, "new": p.get("new")})
    except Exception as e:
        try: upgrade_report(env, "upgrade_failed", {"reason": str(e)[:200]})
        except Exception: pass
    finally:
        UPGRADE["running"] = False

def handle_upgrade_msg(env):
    p = env.get("payload") or {}
    topic = p.get("topic")
    if topic == "upgrade_notice":
        with lock:
            events.append({"t": time.strftime("%H:%M:%S"), "event": "upgrade-notice",
                           "new": p.get("new"), "eta": p.get("eta")})
        upgrade_report(env, "upgrade_ready", {"drained": True})
    elif topic == "upgrade_go":
        import threading as _th
        _th.Thread(target=run_upgrade, args=(env,), daemon=True).start()

def ws_loop():
    backoff = 1
    while True:
        base, token = load_conf()
        url = base.replace("http://", "ws://") + f"/v1/ws?token={token}&client={node_info['self_agent']}"
        try:
            ws = websocket.create_connection(url, timeout=None, ping_interval=25, ping_timeout=10)
            node_info["ws_connected"] = True; backoff = 1
            with lock: events.append({"t": time.strftime("%H:%M:%S"), "event": "ws-connected"})
            register_all()
            while True:
                raw = ws.recv()
                if not raw: continue
                try:
                    env = json.loads(raw)
                except Exception:
                    continue
                typ = env.get("type")
                if typ == "ctrl":
                    continue
                if typ == "event" and env.get("action") in ("join", "leave"):
                    with lock:
                        events.append({"t": time.strftime("%H:%M:%S"), "event": env["action"],
                                       "node": env.get("payload", {})})
                    continue
                if typ in ("text", "file"):
                    mid = env.get("msg_id")
                    if mid and mark_seen(mid):
                        with lock:
                            inbox.append(env)
                            node_info["last_msg_ts"] = time.strftime("%H:%M:%S")
                        save_state()
                        if wake_hook: wake_hook.wake(env, BASE_DIR)
                        if typ == "text":
                            handle_upgrade_msg(env)
        except Exception as e:
            with lock:
                events.append({"t": time.strftime("%H:%M:%S"), "event": "ws-error", "err": str(e)[:160]})
        finally:
            node_info["ws_connected"] = False
            try: ws.close()
            except Exception: pass
        time.sleep(backoff); backoff = min(backoff * 2, 30)

def peers_refresh():
    while True:
        r = api("/v1/peers")
        if r.get("ok"):
            node_info["peers"] = [{"node_id": p.get("node_id"), "name": p.get("name"),
                                   "online": p.get("online"), "addr": p.get("addr")} for p in r.get("peers", [])]
            h = api("/v1/health")
            node_info["node_id"] = h.get("node_id"); node_info["name"] = h.get("name")
        time.sleep(20)

class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def _send(self, code, obj):
        b = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n)) if n else {}
    def do_GET(self):
        if self.path.startswith("/status"):
            with lock:
                self._send(200, {"daemon_uptime": int(time.time() - node_info["started_ts"]),
                                 **node_info, "inbox_pending": len(inbox),
                                 "events": list(events)[-20:]})
        elif self.path.startswith("/inbox"):
            with lock:
                self._send(200, {"count": len(inbox), "messages": list(inbox)})
        elif self.path.startswith("/registry"):
            self._send(200, {"agents": load_registry()})
        else:
            self._send(404, {"error": "not found"})
    def do_POST(self):
        b = self._body()
        if self.path.startswith("/ack"):
            mid = b.get("msg_id")
            with lock:
                kept = [e for e in inbox if e.get("msg_id") != mid]
                removed = len(inbox) - len(kept)
                inbox.clear(); inbox.extend(kept)
            self._send(200, {"ok": True, "removed": removed})
        elif self.path.startswith("/send"):
            body = {k: b[k] for k in ("to", "type", "action", "payload", "to_agent",
                                      "from_agent", "reply_to", "ttl_seconds") if k in b}
            body.setdefault("type", "text"); body.setdefault("action", "send")
            body.setdefault("from_agent", node_info["self_agent"])
            self._send(200, api("/v1/send", body, "POST"))
        elif self.path.startswith("/register"):
            a = {"agent_id": b["agent_id"], "name": b.get("name", b["agent_id"]),
                 "capabilities": b.get("capabilities", [])}
            if not a["agent_id"]:
                self._send(400, {"ok": False, "error": "agent_id required"}); return
            ags = [x for x in load_registry() if x["agent_id"] != a["agent_id"]] + [a]
            save_registry(ags)
            r = api("/v1/agents", a, "POST")
            self._send(200, {"ok": r.get("ok", False), "persisted": True})
        elif self.path.startswith("/drop-registry"):
            aid = b.get("agent_id")
            ags = [x for x in load_registry() if x["agent_id"] != aid]
            save_registry(ags); api("/v1/agents", {"agent_id": aid}, "DELETE")
            self._send(200, {"ok": True})
        else:
            self._send(404, {"error": "not found"})

def main():
    load_state()
    threading.Thread(target=ws_loop, daemon=True).start()
    threading.Thread(target=peers_refresh, daemon=True).start()
    print(f"agentlink bridge daemon: control api on http://{CTL_HOST}:{CTL_PORT}")
    ThreadingHTTPServer((CTL_HOST, CTL_PORT), H).serve_forever()

if __name__ == "__main__":
    main()
