#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AgentLink bridge daemon for Hermes「study」(B1 机) — 自 liteguard daemon.py 派生。
WS 订阅 client=study（to_agent=study 定向投递的唯一通道），管理 API 在 127.0.0.1:8767。
铁律内建：自动重连(指数退避1→30s)+重连重注册+msg_id去重+环形收件箱1000+join/leave事件。
配置发现：AGENTLINK_CONFIG 或 E:/Tools/AgentLink-v*/agentlink.json 取版本号最高（勿写死版本目录）。"""
import json, os, sys, time, glob, threading, collections
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

AGENT_ID   = os.environ.get("BRIDGE_AGENT", "study")
CAPS       = ["综合助手", "代码", "舰队运维", "消息处理"]
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path: sys.path.insert(0, BASE_DIR)
try:
    import wake_hook  # 可选唤醒钩子：wake.json 存在即生效，缺失自动静默
except Exception:
    wake_hook = None
STATE      = os.path.join(BASE_DIR, "state.json")
REGISTRY   = os.path.join(BASE_DIR, "registry.json")
CTL_HOST, CTL_PORT = "127.0.0.1", int(os.environ.get("BRIDGE_PORT", "8767"))
NODE_API   = "http://127.0.0.1:8765"

def resolve_config():
    """找本机节点配置：AGENTLINK_CONFIG > 稳定位置 > 发行目录旧配置。

    稳定位置与 agentlink 二进制一致（Windows %LOCALAPPDATA%\\AgentLink\\，
    Linux ~/.config/agentlink/）——升级换发行目录不会动它。
    """
    if os.environ.get("AGENTLINK_CONFIG"):
        return os.environ["AGENTLINK_CONFIG"]
    real_home = os.path.expanduser("~")
    # 稳定位置优先（升级不覆盖）
    for pat in ("/mnt/c/Users/*/AppData/Local/AgentLink/agentlink.json",
                os.path.join(real_home, ".config", "agentlink", "agentlink.json"),
                os.path.join(real_home, ".agentlink", "agentlink.json")):
        for p in sorted(glob.glob(pat)):
            if os.path.isfile(p):
                return p
    cands = glob.glob("/mnt/e/Tools/AgentLink-v*/agentlink.json")
    for extra in (os.path.join(real_home, "agentlink.json"),):
        if os.path.isfile(extra):
            cands.append(extra)
    if not cands:
        return None  # 无本地配置文件——load_conf 会回退到回环 API 取 token
    def ver(p):
        try: return tuple(int(x) for x in p.split("AgentLink-v")[1].split("/")[0].split("."))
        except Exception: return (0,)
    return max(cands, key=ver)

CONF = resolve_config()
lock = threading.RLock()
inbox  = collections.deque(maxlen=1000)
events = collections.deque(maxlen=100)
seen   = collections.deque(maxlen=2000); seen_set = set()
node_info = {"ws_connected": False, "started_ts": time.time(),
             "self_agent": AGENT_ID, "peers": [], "last_msg_ts": None}

def load_conf():
    if CONF:
        with open(CONF, encoding="utf-8") as f: d = json.load(f)
        return d.get("local_api", {}).get("host_url") or NODE_API, d["local_api"]["token"]
    # 兜底：无本地配置文件时，只经本机回环 API 取 token（严禁走远端节点）
    import urllib.request
    with urllib.request.urlopen(NODE_API + "/web/setup", timeout=5) as r:
        d = json.loads(r.read().decode())
    if not d.get("ok") or not d.get("token"):
        raise SystemExit("既无本地配置也拿不到回环 token")
    return NODE_API, d["token"]

def load_state():
    try:
        s = json.load(open(STATE))
        for m in s.get("seen", []):
            if m not in seen_set: seen.append(m); seen_set.add(m)
    except Exception: pass

def save_state():
    with lock:
        json.dump({"seen": list(seen), "self_agent": AGENT_ID}, open(STATE, "w"))

def load_registry():
    try: return json.load(open(REGISTRY))
    except Exception: return []

def save_registry(agents):
    json.dump(agents, open(REGISTRY, "w"), ensure_ascii=False, indent=1)

def api(path, body=None, method=None, tries=3):
    import urllib.request, urllib.error
    base, token = load_conf()
    for i in range(tries):
        try:
            req = urllib.request.Request(base + path,
                data=(json.dumps(body, ensure_ascii=False).encode() if body is not None else None),
                headers={"X-Auth-Token": token, "Content-Type": "application/json"},
                method=method or ("POST" if body is not None else "GET"))
            return json.loads(urllib.request.urlopen(req, timeout=10).read().decode())
        except urllib.error.HTTPError as ex:
            return {"ok": False, "error": f"{ex.code} {ex.read()[:120]}"}
        except Exception:
            time.sleep(1 + i)
    return {"ok": False, "error": "unreachable"}

def mark_seen(mid):
    with lock:
        if mid in seen_set: return False
        if len(seen_set) >= 2000: seen_set.discard(seen.popleft())
        seen.append(mid); seen_set.add(mid)
        return True

def ev(e):
    with lock: events.append({"t": time.strftime("%H:%M:%S"), **e})

def register_all():
    agents = [{"agent_id": AGENT_ID, "name": AGENT_ID, "capabilities": CAPS}] + \
             [x for x in load_registry() if x.get("agent_id") != AGENT_ID]
    for a in agents:
        r = api("/v1/agents", a, "POST")
        ev({"event": "register", "agent_id": a["agent_id"], "resp_ok": r.get("ok")})

def ws_loop():
    import websocket
    backoff = 1
    while True:
        base, token = load_conf()
        url = base.replace("http://", "ws://") + f"/v1/ws?token={token}&client={AGENT_ID}"
        ws = None
        try:
            # 铁律：timeout=None，靠 ping/pong 保活（有限 recv timeout 会每~30s 假断连）
            ws = websocket.create_connection(url, timeout=None, ping_interval=25, ping_timeout=10)
            node_info["ws_connected"] = True; backoff = 1
            ev({"event": "ws-connected"})
            register_all()
            while True:
                raw = ws.recv()
                if not raw: continue
                try: env = json.loads(raw)
                except Exception: continue
                typ = env.get("type")
                if typ == "ctrl": continue
                if typ == "event" and env.get("action") in ("join", "leave"):
                    ev({"event": env["action"], "node": env.get("payload", {})}); continue
                if typ in ("text", "file"):
                    mid = env.get("msg_id")
                    if mid and mark_seen(mid):
                        with lock:
                            inbox.append(env)
                            node_info["last_msg_ts"] = time.strftime("%H:%M:%S")
                        save_state()
                        if wake_hook: wake_hook.wake(env, BASE_DIR)
        except Exception as e:
            ev({"event": "ws-error", "err": str(e)[:160]})
        finally:
            node_info["ws_connected"] = False
            try:
                if ws: ws.close()
            except Exception: pass
        time.sleep(backoff); backoff = min(backoff * 2, 30)

def peers_refresh():
    while True:
        r = api("/v1/peers")
        if r.get("ok"):
            node_info["peers"] = [{"node_id": p.get("node_id"), "name": p.get("name"),
                                   "online": p.get("online"), "addr": p.get("addr")}
                                  for p in r.get("peers", [])]
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
                                 **node_info, "inbox_pending": len(inbox), "events": list(events)[-20:]})
        elif self.path.startswith("/inbox"):
            with lock: self._send(200, {"count": len(inbox), "messages": list(inbox)})
        elif self.path.startswith("/registry"):
            self._send(200, {"agents": load_registry()})
        elif self.path.startswith("/peers"):
            self._send(200, {"peers": node_info["peers"]})
        else: self._send(404, {"error": "not found"})
    def do_POST(self):
        b = self._body()
        if self.path.startswith("/ack"):
            mid = b.get("msg_id")
            with lock:
                kept = [e for e in inbox if e.get("msg_id") != mid]
                removed = len(inbox) - len(kept)
                inbox.clear(); inbox.extend(kept)
            api("/v1/ack-inbox", {"msg_id": mid}, "POST")
            self._send(200, {"ok": True, "removed": removed})
        elif self.path.startswith("/send"):
            body = {k: b[k] for k in ("to", "type", "action", "payload", "to_agent",
                                      "from_agent", "reply_to", "ttl_seconds") if k in b}
            body.setdefault("type", "text"); body.setdefault("action", "send")
            body.setdefault("from_agent", AGENT_ID)
            self._send(200, api("/v1/send", body, "POST"))
        elif self.path.startswith("/register"):
            a = {"agent_id": b["agent_id"], "name": b.get("name", b["agent_id"]),
                 "capabilities": b.get("capabilities", [])}
            if not a["agent_id"]: self._send(400, {"ok": False}); return
            ags = [x for x in load_registry() if x["agent_id"] != a["agent_id"]] + [a]
            save_registry(ags)
            r = api("/v1/agents", a, "POST")
            self._send(200, {"ok": r.get("ok", False), "persisted": True})
        elif self.path.startswith("/drop-registry"):
            aid = b.get("agent_id")
            ags = [x for x in load_registry() if x["agent_id"] != aid]
            save_registry(ags); api("/v1/agents", {"agent_id": aid}, "DELETE")
            self._send(200, {"ok": True})
        elif self.path.startswith("/send-file"):
            r = api("/v1/send-file", {"to": b.get("to"), "file_path": b.get("file_path"),
                                      "to_agent": b.get("to_agent")}, "POST")
            self._send(200, r)
        else: self._send(404, {"error": "not found"})

def main():
    load_state()
    threading.Thread(target=ws_loop, daemon=True).start()
    threading.Thread(target=peers_refresh, daemon=True).start()
    print(f"agentlink bridge [{AGENT_ID}] control api http://{CTL_HOST}:{CTL_PORT}")
    ThreadingHTTPServer((CTL_HOST, CTL_PORT), H).serve_forever()

if __name__ == "__main__":
    main()
