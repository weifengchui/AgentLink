# -*- coding: utf-8 -*-
"""ws_namespace_smoke — v0.7 §4.2 第三波：namespace 绑定 token 的 WS 订阅隔离。

单节点 S（18968，mesh 关闭，只测消息/agent 面）：
  auth.tokens         team-a-token=operator, team-b-token=operator,
                      unbound-token=operator, admin-static=admin
  auth.token_namespaces team-a-token=team-a, team-b-token=team-b
  agents              ws-a(team-a), ws-b(team-b), ws-global(*)

场景：
  W0 鉴权矩阵：local token 全局；绑定 token 仅本租户；未绑定/静态 Admin 401；
     绑定 token + 跨租户 client / 共享池 client -> 403
  W1 观察者（无 client）：A 只收 from/to 属于 team-a 的帧；B 同理；互不串租
  W2 agent client：A token + client=ws-a 只收 to_agent=ws-a（不看本租户出站）
  W3 local token 观察者仍收全量（含全局 agent 消息）
  W4 进程存活、日志无 panic
"""
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

import websocket  # noqa: E402

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.environ.get(
    "AL_BIN", os.path.expanduser("~/agentlink-target/release/agentlink"))
ROOT = os.path.expanduser(
    "~/.hermes/profiles/agentlink/cache/scratch/wsns-sim")
PORT = 18968
NODE = "5e0000000001"
ADMIN = "wsns-admin"
TOK_A = "team-a-token"
TOK_B = "team-b-token"
UNBOUND = "unbound-token"
ADMIN_STATIC = "admin-static"
EVIDENCE = []


def record(name, ok, detail=""):
    EVIDENCE.append({"scenario": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + " | "
          + json.dumps(detail, ensure_ascii=False)[:420], flush=True)


def api(path, body=None, method=None, token=ADMIN, timeout=10):
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    m = method or ("POST" if body is not None else "GET")
    req = urllib.request.Request(
        f"http://127.0.0.1:{PORT}{path}", method=m, data=data,
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
                cmd = f.read().replace("\\0", " ")
        except Exception:
            continue
        if "wsns-sim" in cmd and "agentlink" in cmd:
            try:
                os.kill(int(pid), signal.SIGKILL)
            except Exception:
                pass


def write_cfg():
    cfg = {
        "v": 1, "node_id": NODE, "name": "wsns-s", "listen_port": PORT,
        "data_dir": os.path.join(ROOT, f"data{PORT}"),
        "history": {"enabled": True, "len": 200, "file": "history.jsonl"},
        "discovery": {"enabled": False, "static_peers": []},
        "security": {"mode": "strict", "cluster_token": "wsns-cluster"},
        "local_api": {"host": "127.0.0.1", "token": ADMIN},
        "auth": {
            "tokens": {TOK_A: "operator", TOK_B: "operator",
                       UNBOUND: "operator", ADMIN_STATIC: "admin"},
            "token_namespaces": {TOK_A: "team-a", TOK_B: "team-b"},
        },
        "delivery": {"ack_timeout_seconds": 5, "max_retries": 2,
                     "offline_ttl_seconds": 300, "dedup_cache_size": 2048,
                     "persist": False},
        "log": {"level": "info", "format": "text", "file": ""},
        "mesh": {"enabled": False},
    }
    path = os.path.join(ROOT, "s.json")
    json.dump(cfg, open(path, "w"))
    return path


def wait_for(fn, timeout_s=25, step=0.1):
    end = time.monotonic() + timeout_s
    while time.monotonic() < end:
        v = fn()
        if v:
            return v
        time.sleep(step)
    return fn()


def ws_connect(token, client=None):
    url = f"ws://127.0.0.1:{PORT}/v1/ws?token={token}"
    if client:
        url += f"&client={client}"
    return websocket.create_connection(url, timeout=3, ping_interval=25,
                                       ping_timeout=10)


def ws_status(token, client=None):
    try:
        ws = ws_connect(token, client)
        ws.close()
        return 101
    except websocket.WebSocketBadStatusException as e:
        return e.status_code
    except Exception:
        return -1


def send(from_agent, to_agent, text):
    return api("/v1/send", {
        "from": NODE, "to": NODE,
        "from_agent": from_agent, "to_agent": to_agent,
        "type": "text", "action": "send",
        "payload": {"topic": "ws.scope", "text": text}})


def drain(ws, seconds=1.2):
    texts = []
    end = time.monotonic() + seconds
    ws.settimeout(0.2)
    while time.monotonic() < end:
        try:
            raw = ws.recv()
        except websocket.WebSocketTimeoutException:
            continue
        except Exception:
            break
        if not raw:
            continue
        try:
            env = json.loads(raw)
        except Exception:
            continue
        txt = (env.get("payload") or {}).get("text")
        if txt:
            texts.append(txt)
    return texts


def main():
    kill_stale()
    time.sleep(0.4)
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(ROOT, exist_ok=True)
    assert not probe(PORT), f"port {PORT} busy（生产隔离铁律）"
    proc = None
    try:
        proc = subprocess.Popen(
            [BIN, "run", "--config", write_cfg(), "--no-open"],
            stdout=open(os.path.join(ROOT, "s.log"), "a"),
            stderr=subprocess.STDOUT)
        assert wait_for(lambda: probe(PORT), 25), "S 未启动"
        for aid, ns in (("ws-a", "team-a"), ("ws-b", "team-b"),
                        ("ws-global", "*")):
            st, _ = api("/v1/agents", {
                "agent_id": aid, "name": aid, "namespace": ns,
                "capabilities": ["text"]})
            assert st == 200, f"register {aid}: {st}"

        # ---- W0 鉴权矩阵 ----------------------------------------------------
        st_local = ws_status(ADMIN)
        st_a = ws_status(TOK_A)
        st_unbound = ws_status(UNBOUND)
        st_admin_static = ws_status(ADMIN_STATIC)
        st_cross = ws_status(TOK_A, "ws-b")
        st_shared = ws_status(TOK_A, "ws-global")
        record("W0 local 可连；绑定 token 可连；未绑定/ Admin 静态 401；跨租户/共享池 client 403",
               st_local == 101 and st_a == 101 and st_unbound == 401
               and st_admin_static == 401 and st_cross == 403
               and st_shared == 403,
               {"local": st_local, "a": st_a, "unbound": st_unbound,
                "admin_static": st_admin_static, "cross_client": st_cross,
                "shared_client": st_shared})

        # ---- W1 观察者隔离 --------------------------------------------------
        ws_a = ws_connect(TOK_A)
        ws_b = ws_connect(TOK_B)
        ws_admin = ws_connect(ADMIN)
        time.sleep(0.2)
        msgs = [
            ("ws-a", "ws-a", "m1"),
            ("ws-a", "ws-b", "m2"),
            ("ws-b", "ws-b", "m3"),
            ("ws-b", "ws-a", "m4"),
            ("ws-global", "ws-global", "m5"),
        ]
        for f, t, text in msgs:
            st, _ = send(f, t, text)
            assert st == 200, f"send {text}: {st}"
        time.sleep(0.4)
        got_a, got_b, got_all = drain(ws_a), drain(ws_b), drain(ws_admin)
        set_a, set_b, set_all = set(got_a), set(got_b), set(got_all)
        record("W1 观察者 A/B 只收本租户 from/to 帧（跨租户仅双方可见），不串租",
               set_a == {"m1", "m2", "m4"} and set_b == {"m2", "m3", "m4"}
               and "m3" not in set_a and "m1" not in set_b
               and "m5" not in set_a and "m5" not in set_b,
               {"A": sorted(set_a), "B": sorted(set_b)})

        # ---- W2 client 过滤 -------------------------------------------------
        ws_ac = ws_connect(TOK_A, "ws-a")
        time.sleep(0.2)
        for f, t, text in msgs:
            send(f, t, text)
        time.sleep(0.4)
        got_ac = drain(ws_ac)
        record("W2 A token + client=ws-a 只收 to_agent=ws-a（本租户出站 m2 不投）",
               set(got_ac) == {"m1", "m4"}, {"client_a": sorted(set(got_ac))})

        # ---- W3 全局观察者 --------------------------------------------------
        record("W3 local token 观察者仍收全量（含全局 agent m5）",
               set_all == {"m1", "m2", "m3", "m4", "m5"},
               {"admin": sorted(set_all)})

        for ws in (ws_a, ws_b, ws_admin, ws_ac):
            try:
                ws.close()
            except Exception:
                pass

        # ---- W4 存活/panic --------------------------------------------------
        alive = proc.poll() is None
        log = open(os.path.join(ROOT, "s.log")).read().lower()
        record("W4 进程存活、日志无 panic", alive and "panic" not in log,
               {"alive": alive, "panic": "panic" in log})

    finally:
        if proc:
            try:
                proc.send_signal(signal.SIGINT)
                proc.wait(timeout=6)
            except Exception:
                proc.kill()
        with open(os.path.join(REPO, "tests", "ws_namespace_smoke.jsonl"), "w") as f:
            for e in EVIDENCE:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        npass = sum(1 for e in EVIDENCE if e["pass"])
        print(f"ws_namespace_smoke: {npass}/{len(EVIDENCE)} "
              f"→ {'OK' if npass == len(EVIDENCE) else 'FAILED'}", flush=True)
    sys.exit(0 if all(e["pass"] for e in EVIDENCE) else 1)


if __name__ == "__main__":
    main()
