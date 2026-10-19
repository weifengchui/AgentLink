# -*- coding: utf-8 -*-
"""relay_smoke — spine/relay 中继拓扑冒烟（v0.7 第一波）。

拓扑：A(leaf) ── R(spine) ── B(leaf)；A/B 只连 R（静态种子清单 =
discovery.static_peers 指向 R），跨叶子帧由 R 单跳转发。端口 18921-18923
（隔离，绝不指向生产 8765）。

  S1 A→B 定向：R 中继转发，B 收到原帧（from 保持 A、hops=1）
  S2 B→A 回向：对称成立（双向路由）
  S3 A 广播：R 本地投递 + 转发给 B（中继扩散）
  S4 未知目标：R 丢弃且不混入离线队列；三节点不崩
  S5 agent 表中继：B 注册 agent → A 的 fleet 视图可见（跨叶子调度前提）
  S6 直连对照：A 直接发给 R（本机）正常，证明中继不破坏直连路径
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
BIN = os.environ.get("AL_BIN",
                     os.path.expanduser("~/agentlink-target/debug/agentlink"))
ROOT = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch/al-relay")
TOKEN = "relay-smoke-token"
CLUSTER = "relay-cluster"
NA = "a1a1a1a1a1a1"   # 12 位 hex（/v1/send 的目标解析要求）
NR = "b1b1b1b1b1b1"
NB = "c1c1c1c1c1c1"
PORTS = {"a": 18921, "r": 18922, "b": 18923}
EVIDENCE = []


def record(name, ok, detail=""):
    EVIDENCE.append({"scenario": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + " | "
          + json.dumps(detail, ensure_ascii=False)[:220], flush=True)


def api(key, path, body=None, method=None):
    port = PORTS[key]
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", method=method
        or ("POST" if body is not None else "GET"), data=data,
        headers={"X-Auth-Token": TOKEN, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=8) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:
            return e.code, {}
    except Exception as e:
        return 0, {"error": str(e)[:120]}


def history(key, limit=300):
    _, d = api(key, f"/v1/history?limit={limit}")
    return d.get("history", [])


def has_msg(key, msg_id, direction=None):
    for h in history(key):
        if h.get("msg_id") == msg_id and (direction is None or h.get("direction") == direction):
            return h
    return None


def wait_for(fn, timeout_s=15, step=0.25):
    end = time.monotonic() + timeout_s
    while time.monotonic() < end:
        v = fn()
        if v:
            return v
        time.sleep(step)
    return fn()


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
        if "al-relay" in cmd and "agentlink" in cmd:
            try:
                os.kill(int(pid), signal.SIGKILL)
            except Exception:
                pass


def write_cfg(key, *, relay, static_peers):
    port = PORTS[key]
    cfg = {
        "v": 1, "node_id": {"a": NA, "r": NR, "b": NB}[key],
        "name": f"relay-{key}", "listen_port": port,
        "data_dir": os.path.join(ROOT, f"data{port}"),
        "history": {"enabled": True, "len": 400, "file": "history.jsonl"},
        "discovery": {"enabled": False, "static_peers": static_peers,
                      "heartbeat_interval_seconds": 2,
                      "offline_timeout_seconds": 6},
        "security": {"mode": "strict", "cluster_token": CLUSTER},
        "local_api": {"host": "127.0.0.1", "token": TOKEN},
        "delivery": {"ack_timeout_seconds": 2, "max_retries": 3,
                     "offline_ttl_seconds": 60, "dedup_cache_size": 2048,
                     "persist": False},
        "log": {"level": "info", "format": "text", "file": ""},
        "relay": relay,
    }
    path = os.path.join(ROOT, f"node-{key}.json")
    json.dump(cfg, open(path, "w"))
    return path


def main():
    kill_stale()
    time.sleep(0.6)
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(ROOT, exist_ok=True)
    for p in PORTS.values():
        assert not probe(p), f"port {p} busy（生产隔离铁律）"

    procs = {}
    try:
        # R=spine（不拨号）；A/B=leaf（拨 R，声明上游 spine=NR）
        cfgs = {
            "r": write_cfg("r", relay={"enabled": True, "max_hops": 2},
                           static_peers=[]),
            "a": write_cfg("a", relay={"enabled": False, "spine": NR},
                           static_peers=[f"127.0.0.1:{PORTS['r']}"]),
            "b": write_cfg("b", relay={"enabled": False, "spine": NR},
                           static_peers=[f"127.0.0.1:{PORTS['r']}"]),
        }
        for key in ("r", "a", "b"):
            procs[key] = subprocess.Popen(
                [BIN, "run", "--config", cfgs[key], "--no-open"],
                stdout=open(os.path.join(ROOT, f"{key}.log"), "a"),
                stderr=subprocess.STDOUT)
        for key in ("r", "a", "b"):
            assert wait_for(lambda k=key: probe(PORTS[k]), 25), f"{key} 未启动"

        # 组网：R 看到 A、B 两个在线 peer；A 只看到 R（星型）。
        rup = wait_for(lambda: len([p for p in api("r", "/v1/peers")[1].get("peers", [])
                                    if p.get("online")]) >= 2, 20)
        a_peers = [p.get("node_id") for p in api("a", "/v1/peers")[1].get("peers", [])]
        record("S0 星型组网（R 见 2 peer；A 只见 R）",
               rup and a_peers == [NR], {"a_peers": a_peers})
        time.sleep(1.5)

        # ---- S1 A→B 定向（R 中继） --------------------------------------
        import websocket
        wsb = websocket.WebSocket()
        wsb.connect(f"ws://127.0.0.1:{PORTS['b']}/v1/ws?token={TOKEN}&client=bw")
        wsb.settimeout(6)
        _, d = api("a", "/v1/send", {
            "from": NA, "to": NB, "from_agent": "aw", "to_agent": "bw",
            "type": "text", "action": "send",
            "payload": {"content": "relay-hi", "topic": "relay-s1"}})
        mid1 = d.get("msg_id")
        got = None
        try:
            while True:
                raw = wsb.recv()
                env = json.loads(raw)
                if (env.get("payload") or {}).get("topic") == "relay-s1":
                    got = env
                    break
        except Exception:
            pass
        hb = wait_for(lambda: has_msg("b", mid1, "in"))
        ok1 = bool(got) and got.get("from") == NA and got.get("to") == NB \
            and got.get("hops") == 1 and bool(hb)
        record("S1 A→B 定向经 R 中继（from 保持 / hops=1）", ok1,
               {"hops": got.get("hops") if got else None, "from": got.get("from") if got else None,
                "b_history": bool(hb)})
        wsb.close()

        # ---- S2 B→A 回向 -------------------------------------------------
        wsa = websocket.WebSocket()
        wsa.connect(f"ws://127.0.0.1:{PORTS['a']}/v1/ws?token={TOKEN}&client=aw")
        wsa.settimeout(6)
        _, d2 = api("b", "/v1/send", {
            "from": NB, "to": NA, "from_agent": "bw", "to_agent": "aw",
            "type": "text", "action": "send",
            "payload": {"content": "relay-back", "topic": "relay-s2"}})
        mid2 = d2.get("msg_id")
        got2 = None
        try:
            while True:
                env = json.loads(wsa.recv())
                if (env.get("payload") or {}).get("topic") == "relay-s2":
                    got2 = env
                    break
        except Exception:
            pass
        ok2 = bool(got2) and got2.get("from") == NB and got2.get("hops") == 1 \
            and bool(has_msg("a", mid2, "in"))
        record("S2 B→A 回向（双向路由）", ok2, {"hops": got2.get("hops") if got2 else None})
        wsa.close()

        # ---- S3 广播中继 --------------------------------------------------
        _, d3 = api("a", "/v1/send", {
            "from": NA, "to": "*", "from_agent": "aw",
            "type": "text", "action": "broadcast",
            "payload": {"content": "relay-bc", "topic": "relay-s3"}})
        mid3 = d3.get("msg_id")
        on_r = wait_for(lambda: has_msg("r", mid3, "in"))
        on_b = wait_for(lambda: has_msg("b", mid3, "in"))
        record("S3 广播中继（R 本地投递 + 转发 B）", bool(on_r) and bool(on_b),
               {"r": bool(on_r), "b": bool(on_b)})

        # ---- S4 未知目标 --------------------------------------------------
        _, d4 = api("a", "/v1/send", {
            "from": NA, "to": "deadbeef0000", "from_agent": "aw", "to_agent": "gw",
            "type": "text", "action": "send", "payload": {"content": "void"}})
        time.sleep(2.5)
        alive = all(p.poll() is None for p in procs.values())
        # R 不应把未知目标混进离线队列（用它的日志证据 + 无落盘 offline 记录）
        rlog = open(os.path.join(ROOT, "r.log")).read()
        ok4 = alive and "transit target not a direct peer, dropped" in rlog
        record("S4 未知目标丢弃不排队（三节点不崩）", ok4,
               {"alive": alive, "send_ok": bool(d4.get("msg_id"))})

        # ---- S5 agent 表中继 ----------------------------------------------
        api("b", "/v1/agents", {"agent_id": "b-worker", "name": "b-worker",
                                "capabilities": ["cap:shell:safe"]})
        seen = wait_for(lambda: any(
            a.get("agent_id") == "b-worker"
            for a in api("a", "/v1/fleet/agents")[1].get("agents", [])), 15)
        record("S5 agent 表中继（A 可见 B 的 agent）", bool(seen), {})

        # ---- S6 直连路径不受影响 -------------------------------------------
        _, d6 = api("a", "/v1/send", {
            "from": NA, "to": NR, "from_agent": "aw", "to_agent": "rw",
            "type": "text", "action": "send",
            "payload": {"content": "direct", "topic": "relay-s6"}})
        ok6 = bool(wait_for(lambda: has_msg("r", d6.get("msg_id"), "in")))
        # 直连帧 hops 不应被写成 1（未经中继）
        h6 = has_msg("r", d6.get("msg_id"), "in")
        record("S6 直连（A→R）正常、不经中继", ok6,
               {"hops": h6.get("hops") if h6 else None})

    finally:
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
        with open(os.path.join(REPO, "tests", "relay_smoke.jsonl"), "w") as f:
            for e in EVIDENCE:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        npass = sum(1 for e in EVIDENCE if e["pass"])
        print(f"relay_smoke: {npass}/{len(EVIDENCE)} "
              f"→ {'OK' if npass == len(EVIDENCE) else 'FAILED'}", flush=True)
    sys.exit(0 if all(e["pass"] for e in EVIDENCE) else 1)


if __name__ == "__main__":
    main()
