# -*- coding: utf-8 -*-
"""relay_cascade_smoke — spine/relay 第二波冒烟（v0.7）。

拓扑（两级 spine 级联，全部 `security.control_auth=true`）：

    A(leaf) ── R1(spine, upstream=R2) ── R2(spine) ── B(leaf)

  C0 组网：每节点只看到自己的直连邻居（星型/链型）
  C1 A→B 端到端：R1→R2 两级转发，B 收到 hops=2、from=A
  C2 B→A 回向：对称
  C3 节点目录：A 的 /v1/peers 出现 B/R2（relayed=true + via），无需直连
  C4 fleet 在线：A 看到 B 的 agent（node_online 由目录新鲜度判定）
  C5 控制面验签：伪造未签名 node_dir 被拒（不落目录 + 审计日志），
     正确签名被接受
  C6 篡改签名被拒
  C7 目录过期：kill B 后 A 端 B 目录条目在 TTL(30s)+余量内消失，
     fleet 里 B 的 agent 转为 offline

端口 18924-18927（隔离，绝不指向生产 8765）。
"""
import hashlib
import hmac
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import urllib.request
import uuid

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BIN = os.environ.get("AL_BIN",
                     os.path.expanduser("~/agentlink-target/debug/agentlink"))
ROOT = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch/al-cascade")
TOKEN = "cascade-smoke-token"
CLUSTER = "cascade-cluster"
NA, NR1, NR2, NB = "a00000000001", "b00000000001", "b00000000002", "c00000000001"
PORTS = {"a": 18924, "r1": 18925, "r2": 18926, "b": 18927}
IDS = {"a": NA, "r1": NR1, "r2": NR2, "b": NB}
EVIDENCE = []


def record(name, ok, detail=""):
    EVIDENCE.append({"scenario": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + " | "
          + json.dumps(detail, ensure_ascii=False)[:240], flush=True)


def api(key, path, body=None):
    port = PORTS[key]
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", method="POST" if body is not None else "GET",
        data=data, headers={"X-Auth-Token": TOKEN, "Content-Type": "application/json"})
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


def peers(key):
    return api(key, "/v1/peers")[1].get("peers", [])


def peer_by_id(key, nid):
    for p in peers(key):
        if p.get("node_id") == nid:
            return p
    return None


def history(key, limit=300):
    return api(key, f"/v1/history?limit={limit}")[1].get("history", [])


def has_msg(key, msg_id, direction=None):
    for h in history(key):
        if h.get("msg_id") == msg_id and (direction is None or h.get("direction") == direction):
            return h
    return None


def fleet(key):
    return api(key, "/v1/fleet/agents")[1].get("agents", [])


def wait_for(fn, timeout_s=20, step=0.3):
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
        if "al-cascade" in cmd and "agentlink" in cmd:
            try:
                os.kill(int(pid), signal.SIGKILL)
            except Exception:
                pass


def sign(from_id, msg_id, ts):
    return hmac.new(CLUSTER.encode(), f"{from_id}|{msg_id}|{ts}".encode(),
                    hashlib.sha256).hexdigest()


def write_cfg(key, *, relay, static_peers):
    port = PORTS[key]
    cfg = {
        "v": 1, "node_id": IDS[key], "name": f"cascade-{key}", "listen_port": port,
        "data_dir": os.path.join(ROOT, f"data{port}"),
        "history": {"enabled": True, "len": 400, "file": "history.jsonl"},
        "discovery": {"enabled": False, "static_peers": static_peers,
                      "heartbeat_interval_seconds": 2, "offline_timeout_seconds": 6},
        "security": {"mode": "strict", "cluster_token": CLUSTER, "control_auth": True},
        "local_api": {"host": "127.0.0.1", "token": TOKEN},
        "delivery": {"ack_timeout_seconds": 2, "max_retries": 3,
                     "offline_ttl_seconds": 60, "dedup_cache_size": 2048,
                     "persist": False},
        "log": {"level": os.environ.get("CASCADE_LOG", "info"), "format": "text", "file": ""},
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
        cfgs = {
            # 链式级联：相邻 spine 互为 upstream（双向可达）。
            "r2": write_cfg("r2", relay={"enabled": True, "upstream": NR1, "max_hops": 3},
                            static_peers=[]),
            "r1": write_cfg("r1", relay={"enabled": True, "upstream": NR2, "max_hops": 3},
                            static_peers=[f"127.0.0.1:{PORTS['r2']}"]),
            "a": write_cfg("a", relay={"enabled": False, "spine": NR1, "max_hops": 3},
                           static_peers=[f"127.0.0.1:{PORTS['r1']}"]),
            "b": write_cfg("b", relay={"enabled": False, "spine": NR2, "max_hops": 3},
                           static_peers=[f"127.0.0.1:{PORTS['r2']}"]),
        }
        for key in ("r2", "r1", "a", "b"):
            procs[key] = subprocess.Popen(
                [BIN, "run", "--config", cfgs[key], "--no-open"],
                stdout=open(os.path.join(ROOT, f"{key}.log"), "a"),
                stderr=subprocess.STDOUT)
        for key in ("r2", "r1", "a", "b"):
            assert wait_for(lambda k=key: probe(PORTS[k]), 25), f"{key} 未启动"

        def online_ids(key):
            # 只看直连（目录条目带 relayed=true，online 恒为 true，不能混入）
            return sorted(p["node_id"] for p in peers(key)
                          if p.get("online") and not p.get("relayed"))

        # ---- C0 组网 ------------------------------------------------------
        ok0 = wait_for(
            lambda: online_ids("r1") == sorted([NA, NR2])
            and online_ids("r2") == sorted([NR1, NB])
            and online_ids("a") == [NR1]
            and online_ids("b") == [NR2], 25)
        record("C0 两级级联组网（每节点只见直连邻居）", ok0,
               {"r1": online_ids("r1"), "r2": online_ids("r2"),
                "a": online_ids("a"), "b": online_ids("b")})
        time.sleep(1.0)

        # ---- C1 A→B 两级转发 ---------------------------------------------
        import websocket
        wsb = websocket.WebSocket()
        wsb.connect(f"ws://127.0.0.1:{PORTS['b']}/v1/ws?token={TOKEN}&client=cw")
        wsb.settimeout(8)
        _, d1 = api("a", "/v1/send", {
            "from": NA, "to": NB, "from_agent": "aw", "to_agent": "cw",
            "type": "text", "action": "send",
            "payload": {"content": "cascade", "topic": "cascade-c1"}})
        mid1 = d1.get("msg_id")
        got = None
        try:
            while True:
                env = json.loads(wsb.recv())
                if (env.get("payload") or {}).get("topic") == "cascade-c1":
                    got = env
                    break
        except Exception:
            pass
        wsb.close()
        ok1 = bool(got) and got.get("from") == NA and got.get("hops") == 2 \
            and bool(wait_for(lambda: has_msg("b", mid1, "in")))
        record("C1 A→B 两级转发（hops=2、from 保持）", ok1,
               {"hops": got.get("hops") if got else None,
                "from": got.get("from") if got else None})

        # ---- C2 B→A 回向 --------------------------------------------------
        import websocket as _ws
        wsa = _ws.WebSocket()
        wsa.connect(f"ws://127.0.0.1:{PORTS['a']}/v1/ws?token={TOKEN}&client=aw")
        wsa.settimeout(8)
        _, d2 = api("b", "/v1/send", {
            "from": NB, "to": NA, "from_agent": "cw", "to_agent": "aw",
            "type": "text", "action": "send",
            "payload": {"content": "back", "topic": "cascade-c2"}})
        mid2 = d2.get("msg_id")
        got2 = None
        try:
            while True:
                env = json.loads(wsa.recv())
                if (env.get("payload") or {}).get("topic") == "cascade-c2":
                    got2 = env
                    break
        except Exception:
            pass
        wsa.close()
        ok2 = bool(got2) and got2.get("hops") == 2 \
            and bool(wait_for(lambda: has_msg("a", mid2, "in")))
        record("C2 B→A 回向两级转发", ok2,
               {"hops": got2.get("hops") if got2 else None})

        # ---- C3 节点目录 --------------------------------------------------
        pb = wait_for(lambda: peer_by_id("a", NB), 20)
        pr2 = wait_for(lambda: peer_by_id("a", NR2), 20)
        ok3 = bool(pb) and bool(pr2) and pb.get("relayed") is True \
            and pb.get("via") == NR2 and pr2.get("relayed") is True
        record("C3 节点目录（A 看到 B/R2：relayed=true + via）", ok3,
               {"b": pb, "r2": pr2})

        # ---- C4 fleet 在线判定 -------------------------------------------
        # agent 的 online 语义 = 有 client==agent_id 的 WS 订阅在线；先连订阅
        # 再注册，注册记录即 online=true 并推给全网（生产上由 SDK 适配器完成）。
        wagent = websocket.WebSocket()
        wagent.connect(f"ws://127.0.0.1:{PORTS['b']}/v1/ws?token={TOKEN}&client=casc-worker")
        wagent.settimeout(1)
        api("b", "/v1/agents", {"agent_id": "casc-worker", "name": "casc-worker",
                                "capabilities": ["cap:shell:safe"]})
        fa = wait_for(lambda: next((x for x in fleet("a")
                                    if x.get("agent_id") == "casc-worker"), None), 20)
        ok4 = bool(fa) and fa.get("online") is True and fa.get("node_name") == "cascade-b"
        record("C4 跨级 fleet（A 看到 B 的 agent online + node_name）", ok4,
               {"agent": fa})

        # ---- C5/C6 控制面验签 --------------------------------------------
        fake = "d00000000001"
        ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        mid_unsigned = uuid.uuid4().hex
        def envelope(from_id, msg_id, nodes, *, sig=None):
            env = {
                "v": 1, "msg_id": msg_id, "from": from_id, "to": "*",
                "from_agent": None, "to_agent": None,
                "type": "ctrl", "action": "node_dir", "ts": ts,
                "reply_to": None, "thread": None, "trace_id": None,
                "ttl_seconds": None, "hops": None,
                "payload": {"nodes": nodes},
            }
            if sig:
                env["sig"] = sig
            return env

        forged = envelope(fake, mid_unsigned, [
            {"node_id": fake, "name": "forged", "addr": "10.9.9.9:8765",
             "via": fake, "hops": 0}])
        raw = websocket.WebSocket()
        raw.connect(f"ws://127.0.0.1:{PORTS['a']}/peer/ws")
        raw.settimeout(2)
        # 走真实节点握手（控制面帧只会在已认证链路上到达），但随后冒充
        # **另一个** node_id 发目录帧——验签按帧内 from 计算，签名缺失/不符即拒。
        raw.recv()  # server hello
        nonce = uuid.uuid4().hex
        raw.send(json.dumps({
            "type": "ctrl", "action": "hello", "v": 1,
            "node_id": "e00000000001", "name": "attacker",
            "nonce": nonce,
            "auth": hmac.new(CLUSTER.encode(), f"e00000000001|{nonce}|1".encode(),
                             hashlib.sha256).hexdigest(),
        }))
        time.sleep(0.3)
        raw.send(json.dumps(forged))
        time.sleep(1.2)
        rejected = peer_by_id("a", fake) is None
        alog = open(os.path.join(ROOT, "a.log")).read()
        logged = "node_dir rejected (control frame signature)" in alog
        record("C5 未签名 node_dir 被拒（不落目录 + 审计日志）",
               rejected and logged, {"absent": rejected, "logged": logged})

        fake2 = "d00000000002"
        mid2s = uuid.uuid4().hex
        good = envelope(fake2, mid2s, [
            {"node_id": fake2, "name": "signed-node", "addr": "10.9.9.8:8765",
             "via": fake2, "hops": 0,
             "expires": int(time.time()) + 60}], sig=sign(fake2, mid2s, ts))
        raw.send(json.dumps(good))
        accepted = wait_for(lambda: peer_by_id("a", fake2), 8)
        record("C6 正确签名的 node_dir 被接受", bool(accepted),
               {"relayed": accepted.get("relayed") if accepted else None,
                "via": accepted.get("via") if accepted else None})

        # 篡改签名：msg_id 改了但 sig 不变
        good_tamper = envelope("d00000000003", uuid.uuid4().hex, [
            {"node_id": "d00000000003", "name": "tampered", "addr": "10.9.9.7:8765",
             "via": "d00000000003", "hops": 0}], sig=sign(fake2, mid2s, ts))
        raw.send(json.dumps(good_tamper))
        time.sleep(1.2)
        record("C7 篡改签名被拒", peer_by_id("a", "d00000000003") is None, {})
        raw.close()

        # ---- C8 目录过期（kill B） ---------------------------------------
        procs["b"].send_signal(signal.SIGINT)
        try:
            procs["b"].wait(timeout=6)
        except Exception:
            procs["b"].kill()
        gone = wait_for(lambda: peer_by_id("a", NB) is None, 50, step=1.0)
        fa2 = next((x for x in fleet("a") if x.get("agent_id") == "casc-worker"), None)
        ok8 = gone and (fa2 is None or fa2.get("online") is False)
        detail8 = {"gone": gone, "agent_online": None if fa2 is None else fa2.get("online"),
                   "a_b": peer_by_id("a", NB),
                   "r2_direct": online_ids("r2"),
                   "r1_direct": online_ids("r1")}
        print("C8 detail:", json.dumps(detail8, ensure_ascii=False), flush=True)
        record("C8 目录 TTL 过期（kill B → A 端目录消失、agent 转 offline）", ok8, detail8)

        alive = all(p.poll() is None for k, p in procs.items() if k != "b")
        record("C9 剩余节点无崩溃", alive, {})

    finally:
        try:
            wagent.close()
        except Exception:
            pass
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
        with open(os.path.join(REPO, "tests", "relay_cascade_smoke.jsonl"), "w") as f:
            for e in EVIDENCE:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        npass = sum(1 for e in EVIDENCE if e["pass"])
        print(f"relay_cascade_smoke: {npass}/{len(EVIDENCE)} "
              f"→ {'OK' if npass == len(EVIDENCE) else 'FAILED'}", flush=True)
    sys.exit(0 if all(e["pass"] for e in EVIDENCE) else 1)


if __name__ == "__main__":
    main()
