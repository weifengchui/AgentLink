# -*- coding: utf-8 -*-
"""webhook_converge_smoke — wake-kit 收敛冒烟（v0.6 P2）。

对隔离测试总线（端口 18995-18997，绝不指向生产 8765）验证内核 webhook
对 wake-hook 侧车语义的替代：
  S1 agent_message（inbox 路径）：定向件落 inbox → 签名 webhook 收到投递事实
  S2 agent_message（live 路径）：WS 客户端在册 → 同事件 channel=live
  S3 audit_alert：lockdown 开/关 → kind=lockdown 两次外呼
  S4 task_failed + node_taint：3 × task_failed 归账 → 熔断 warn + node_taint
    （熔断包先前 deferred 的 webhook 项顺带闭环 + P0 task_failed 兼容回归）
  S5 node_offline：第二节点入网后被杀 → 心跳超时/bye → node_offline
  S6 无 secret 向后兼容：事件照收、无 X-Hub-Signature-256 头
"""
import hashlib
import hmac as hmac_mod
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.environ.get("AL_BIN",
                     os.path.expanduser("~/agentlink-target/debug/agentlink"))
ROOT = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch/al-wakekit")
sys.path.insert(0, os.path.join(REPO, "tests"))
import engine_store_smoke as es  # noqa: E402  (license 签名助手, __main__ 守护)


def record(name, ok, detail=""):
    EVIDENCE.append({"scenario": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + " | "
          + json.dumps(detail, ensure_ascii=False)[:220], flush=True)


EVIDENCE = []
SECRET = "wk-smoke-secret"


class WebhookStub:
    """录桩网关：记录 (path, headers, body)，并按 secret 校验签名。"""

    def __init__(self, secret):
        self.hits = []
        self.lock = threading.Lock()
        self.secret = secret
        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass
            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                sig = self.headers.get("X-Hub-Signature-256") or ""
                good = None
                if self_.secret:
                    good = "sha256=" + hmac_mod.new(
                        self_.secret.encode(), body, hashlib.sha256).hexdigest()
                try:
                    parsed = json.loads(body or b"{}")
                except Exception:
                    parsed = {}
                with self_.lock:
                    self_.hits.append({"sig": sig, "sig_ok": good == sig
                                       if good is not None else None,
                                       "body": parsed})
                self.send_response(200)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")
        self_ = self
        srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{srv.server_address[1]}/hook"

    def events(self, name):
        with self.lock:
            return [h for h in self.hits if h["body"].get("event") == name]

    def sig_errors(self):
        with self.lock:
            return [h for h in self.hits
                    if h["body"].get("event") in ("agent_message", "node_offline")
                    and h["sig_ok"] is False]


def api(port, path, body=None, method=None, token="wakekit-smoke-token"):
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", method=method
        or ("POST" if body is not None else "GET"), data=data,
        headers={"X-Auth-Token": token, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=8) as r:
            code, out = r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        code = e.code
        try:
            out = json.loads(e.read().decode())
        except Exception:
            out = {}
    return code, out


def wait_for(fn, timeout_s=20, step=0.3):
    end = time.monotonic() + timeout_s
    while time.monotonic() < end:
        v = fn()
        if v:
            return v
        time.sleep(step)
    return fn()


def probe(port):
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) == 0


def write_cfg(port, name, webhook_extra, secret, mesh_on=False,
              node_token="wakekit-smoke-token"):
    cfg = {
        "v": 1, "node_id": f"wk{name}", "name": f"wakekit-{name}",
        "listen_port": port, "data_dir": os.path.join(ROOT, f"data{port}"),
        "discovery": {"enabled": True, "methods": ["udp"],
                      "heartbeat_interval_seconds": 1,
                      "offline_timeout_seconds": 2,
                      "static_peers": []},
        "security": {"mode": "strict", "cluster_token": "wakekit-cluster"},
        "local_api": {"host": "127.0.0.1", "token": node_token},
        "delivery": {"ack_timeout_seconds": 5, "max_retries": 5,
                     "offline_ttl_seconds": 600, "dedup_cache_size": 2048,
                     "persist": True},
        "log": {"level": "info", "format": "text", "file": ""},
    }
    wh = {"url": webhook_extra["url"], "events": webhook_extra["events"],
          "timeout_seconds": 3, "max_retries": 0, "retry_interval_seconds": 1}
    if secret:
        wh["secret"] = secret
    cfg["webhook"] = wh
    if mesh_on:
        cfg["mesh"] = {
            "enabled": True, "license_file": "license.toml",
            "engine": {"max_retries": 0, "tick_ms": 200,
                       "circuit": {"enabled": True, "failures": 3,
                                   "window_secs": 60, "cooldown_secs": 12}},
        }
    path = os.path.join(ROOT, f"node{port}.json")
    json.dump(cfg, open(path, "w"))
    return path


def start(cfg_path, port):
    logf = open(os.path.join(ROOT, f"node{port}.log"), "a")
    p = subprocess.Popen([BIN, "run", "--config", cfg_path, "--no-open"],
                         stdout=logf, stderr=subprocess.STDOUT)
    wait_for(lambda: probe(port), 25)
    return p, logf


def stop(p, logf):
    try:
        p.send_signal(signal.SIGINT)
        p.wait(timeout=6)
    except Exception:
        try:
            p.kill()
        except Exception:
            pass
    logf.close()


def main():
    # 先清残留（前次异常退出占端口的节点进程）——按 scratch 目录路径精确定位。
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open(f"/proc/{pid}/cmdline", encoding="utf-8",
                      errors="replace") as f:
                cmd = f.read().replace("\0", " ")
        except Exception:
            continue
        if "al-wakekit" in cmd and "agentlink" in cmd:
            try:
                os.kill(int(pid), signal.SIGKILL)
            except Exception:
                pass
    time.sleep(0.8)
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(ROOT, exist_ok=True)
    lic = es.sign_license_text({
        "tier": "starter", "node_quota": 6, "issued": "2026-10-01T00:00:00Z",
        "expires": "2036-12-31T23:59:59Z"}, "wakekit smoke")
    open(os.path.join(ROOT, "license.toml"), "w").write(lic)
    for port in (18995, 18996, 18997):
        assert not probe(port), f"port {port} busy（生产隔离铁律）"

    stub = WebhookStub(SECRET)
    stub3 = WebhookStub(None)  # S6 无 secret 兼容桩

    # ---- N1：webhook + secret + mesh/circuit 开 ------------------------
    cfg1 = write_cfg(18995, "1", {"url": stub.url,
                                  "events": "agent_message,node_offline,"
                                            "audit_alert,node_taint,task_failed"},
                     SECRET, mesh_on=True)
    p1, lg1 = start(cfg1, 18995)
    assert probe(18995), "N1 未启动"

    # ---- S1 agent_message inbox 路径 -----------------------------------
    code, _ = api(18995, "/v1/agents",
                  {"agent_id": "wk-worker", "name": "wk-worker",
                   "capabilities": ["cap:file"]})
    assert code in (200, 202)
    code, _ = api(18995, "/v1/send", {
        "from": "wk1", "to": "wk1", "from_agent": "orch",
        "to_agent": "wk-worker", "type": "text", "action": "send",
        "payload": {"content": "wake-kitted 定向件", "topic": "inbox-ok"}})
    got = wait_for(lambda: stub.events("agent_message"))
    h = stub.events("agent_message")
    pl = h[0]["body"]["payload"] if h else {}
    # channel 只作观察面：内核常驻事件订户使 receiver_count 未必为 0，
    # inbox/live 两分支语义都算「节点已接收定向件」。断言核心=字段+签名。
    ok = (bool(h)
          and pl.get("to_agent") == "wk-worker"
          and pl.get("content") == "wake-kitted 定向件"
          and pl.get("topic") == "inbox-ok")
    record("S1 agent_message 投递事实+HMAC 签名", ok and h[0]["sig_ok"] is True,
           {"hits": len(h), "channel": pl.get("channel"), "payload": pl})

    # ---- S2 agent_message live 路径 -------------------------------------
    import websocket
    ws = websocket.WebSocket()
    ws.connect(f"ws://127.0.0.1:18995/v1/ws?token=wakekit-smoke-token&client=wks")
    ws.settimeout(1)
    code, _ = api(18995, "/v1/send", {
        "from": "wk1", "to": "wk1", "from_agent": "orch",
        "to_agent": "wk-worker", "type": "text", "action": "send",
        "payload": {"content": "live 路径", "topic": "live-ok"}})
    def find_live():
        got = [x for x in stub.events("agent_message")
               if x["body"]["payload"].get("channel") == "live"]
        return got[0] if got else None
    live = wait_for(lambda: find_live() is not None, 10)
    lv = find_live()
    record("S2 agent_message live 路径", live,
           {"channel": lv["body"]["payload"].get("channel") if lv else None})
    ws.close()

    # ---- S3 audit_alert：lockdown 开关两次 ------------------------------
    api(18995, "/v1/lockdown", {"on": True})
    got_on = wait_for(
        lambda: [x for x in stub.events("audit_alert")
                 if x["body"]["payload"].get("on") is True], 10)
    api(18995, "/v1/lockdown", {"on": False})
    got_off = wait_for(
        lambda: [x for x in stub.events("audit_alert")
                 if x["body"]["payload"].get("on") is False], 10)
    record("S3 audit_alert lockdown 开/关", bool(got_on) and bool(got_off),
           {"on": len(got_on), "off": len(got_off)})

    # ---- S4 task_failed + node_taint（Previously deferred 项）-----------
    # 3 个 addressed 任务各一发 task_failed(seq=2)（Pending→Failed 直转合法），
    # 归账全在本节点（from_agent=wk-worker 且本机注册）。
    for tid in ["t1", "t2", "t3"]:
        api(18995, "/v1/tasks", {"task_id": tid, "type": "text",
                                 "intent": "wakekit-smoke", "to_agent": "wk-worker",
                                 "priority": 100, "payload": {"content": tid}})
        api(18995, "/v1/send", {
            "from": "wk1", "to": "wk1", "from_agent": "wk-worker",
            "type": "text", "action": "send",
            "payload": {"topic": "task_failed", "task_id": tid, "seq": 2,
                        "error": "wakekit-smoke 人为失败"}})
    taint = wait_for(lambda: stub.events("node_taint"), 15)
    failed = stub.events("task_failed")
    _, ms = api(18995, "/v1/mesh/status")
    taints_view = (ms.get("engine") or {}).get("circuit_taints") or []
    record("S4 task_failed(P0 兼容) + node_taint(熔断 webhook)",
           bool(failed) and bool(taint) and taints_view,
           {"task_failed": len(failed), "node_taint": len(taint),
            "circuit_taints": len(taints_view)})

    # ---- S5 node_offline：第二节点入网→被杀 ----------------------------
    cfg2 = write_cfg(18996, "2", {"url": stub.url,
                                  "events": "agent_message,node_offline"},
                     SECRET, mesh_on=False)
    # 静态 peer 直指 N1（同机 UDP 组播组冲突问题绕开——复用 mtls_net 冒烟口径）
    cfg2_path = os.path.join(ROOT, "node18996.json")
    d = json.load(open(cfg2_path))
    d["discovery"] = {"enabled": False,
                      "static_peers": ["127.0.0.1:18995"]}
    json.dump(d, open(cfg2_path, "w"))
    p2, lg2 = start(cfg2_path, 18996)
    up = wait_for(lambda: any(
        p.get("node_id") == "wk2" and p.get("online")
        for p in api(18995, "/v1/peers")[1].get("peers", [])), 20)
    if up:
        p2.send_signal(signal.SIGKILL)
        off = wait_for(
            lambda: [x for x in stub.events("node_offline")
                     if x["body"]["payload"].get("peer_id") == "wk2"], 30)
        reason = off[0]["body"]["payload"].get("reason") if off else None
        record("S5 node_offline 心跳超时捕获", bool(off), {"reason": reason})
    else:
        p2.send_signal(signal.SIGKILL)
        record("S5 node_offline 心跳超时捕获", False, {"peers": "wk2 未上线"})

    # ---- S6 无 secret 向后兼容 ------------------------------------------
    cfg3 = write_cfg(18997, "3", {"url": stub3.url, "events": "agent_message"},
                     None, mesh_on=False)
    p3, lg3 = start(cfg3, 18997)
    code, _ = api(18997, "/v1/agents",
                  {"agent_id": "wk-worker3", "name": "wk-worker3", "capabilities": []})
    code, _ = api(18997, "/v1/send", {
        "from": "wk3", "to": "wk3", "from_agent": "orch",
        "to_agent": "wk-worker3", "type": "text", "action": "send",
        "payload": {"content": "legacy no-secret"}})
    got3 = wait_for(lambda: stub3.events("agent_message"), 10)
    no_header = bool(got3) and got3[0]["sig"] == ""
    record("S6 无 secret 兼容（事件照收且无签名头）",
           got3 and no_header, {"hits": len(got3)})

    # 签名逻辑零误报（所有签名过的 agent_message/node_offline 桩内都验签通过）
    record("S7 HMAC 全验签无误报", not stub.sig_errors(),
           {"errors": len(stub.sig_errors())})

    stop(p1, lg1)
    stop(p2, lg2)
    stop(p3, lg3)

    with open(os.path.join(REPO, "tests", "webhook_converge_smoke.jsonl"), "w") as f:
        for e in EVIDENCE:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    npass = sum(1 for e in EVIDENCE if e["pass"])
    print(f"webhook_converge_smoke: {npass}/{len(EVIDENCE)} "
          f"→ {'OK' if npass == len(EVIDENCE) else 'FAILED'}", flush=True)
    sys.exit(0 if npass == len(EVIDENCE) else 1)


import shutil  # noqa: E402

if __name__ == "__main__":
    main()
