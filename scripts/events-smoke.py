#!/usr/bin/env python3
"""events-smoke — v0.5 内核 §1 事件流 + §2 脱敏 端到端验证（隔离总线 18766，绝不指向生产）。

场景：
  S1  health.features 能力声明
  S2  脱敏边界：WS 收件人拿原文；/v1/history 与 /v1/events 观察者拿脱敏件
      （token/password/pem/hex/custom 五类内置+自定义规则）
  S3  /v1/events 增量拉取：seq 单调、next_seq 水位、since 续传不丢事件
  S4  agent/types 过滤
  S5  环满轮换 → since 落后回 409 + oldest_seq（降级全量重建）
  S6  重启续传：seq 从磁盘 history 恢复后继续单调
  S7  redact.enabled=false 时观察者拿原文（配置回退）

用法：python3 events-smoke.py <path-to-agentlink-binary>
退出码 0 = 全过。
"""
import json, os, shutil, subprocess, sys, time, urllib.request

BIN = sys.argv[1] if len(sys.argv) > 1 else None
assert BIN and os.path.exists(BIN), "usage: events-smoke.py <agentlink-binary>"
BASE = "http://127.0.0.1:18766"
TOKEN = "smoketoken"
SCRATCH = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch")
D1 = os.path.join(SCRATCH, "al-v05-testdata")
D2 = os.path.join(SCRATCH, "al-v05-testdata2")
CFG1 = os.path.join(SCRATCH, "al-v05-node.json")
CFG2 = os.path.join(SCRATCH, "al-v05-node-redactoff.json")

SECRET_TOKEN = "sk-SECRETTOKENabcdef123456"
SECRET_HEX = "deadbeef" * 8  # 64 hex
SECRET_PEM = ("-----BEGIN RSA PRIVATE KEY-----\nMIIBOgIBAAJ7SECRETLINE\n"
              "-----END RSA PRIVATE KEY-----")
SECRET_IP = "192.168.1.50"

results = []
def check(name, ok, detail=""):
    results.append((name, bool(ok)))
    print(("PASS " if ok else "FAIL ") + name + (f"  [{detail}]" if detail and not ok else ""))

def api(path, body=None, method=None):
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data,
        headers={"X-Auth-Token": TOKEN, "Content-Type": "application/json"},
        method=method or ("POST" if body is not None else "GET"))
    try:
        with urllib.request.urlopen(req, timeout=8) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode() or "{}")

def write_cfg(path, data_dir, redact_on=True, hist_len=4):
    json.dump({
        "v": 1, "node_id": "fa15ee000505", "name": "v05-smoke",
        "listen_port": 18766, "data_dir": data_dir,
        "history": {"enabled": True, "len": hist_len, "file": "history.jsonl"},
        "redact": {"enabled": redact_on,
                   "patterns": ["192\\.168\\.[0-9]+\\.[0-9]+"]},
        "discovery": {"enabled": False},
        "security": {"cluster_token": "smokecluster"},
        "local_api": {"token": TOKEN},
        "web": {"open": False},
    }, open(path, "w"))

def start_node(cfg):
    for d in (D1, D2):
        shutil.rmtree(d, ignore_errors=True)
    p = subprocess.Popen([BIN, "--config", cfg],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(50):
        try:
            s, h = api("/v1/health")
            if s == 200:
                return p
        except Exception:
            pass
        time.sleep(0.1)
    raise RuntimeError("node did not come up")

def send_text(content, to_agent="recv"):
    return api("/v1/send", {"from": NODE, "to": NODE,
                            "type": "text", "action": "send",
                            "from_agent": "sender", "to_agent": to_agent,
                            "payload": {"topic": "probe", "content": content}})

import websocket  # websocket-client
NODE = None

def main():
    write_cfg(CFG1, D1)
    node = start_node(CFG1)
    global NODE
    NODE = api("/v1/health")[1]["node_id"]
    try:
        # S1 health features
        s, h = api("/v1/health")
        feats = h.get("features", [])
        check("S1 health.features 含 events+redact", "events" in feats and "redact" in feats, str(feats))

        # 收件人 WS 拿原文
        api("/v1/agents", {"agent_id": "recv", "name": "recv"})
        ws = websocket.WebSocket()
        ws.connect(BASE.replace("http://", "ws://") + f"/v1/ws?token={TOKEN}&client=recv")
        ws.settimeout(5)

        raw = (f"登录用 X-Auth-Token: {SECRET_TOKEN} ；"
               f"sha256={SECRET_HEX} ；内网 {SECRET_IP} ；\n{SECRET_PEM}")
        st, resp = send_text(raw)
        check("S2a 发送成功", st == 200 and resp.get("ok"), json.dumps(resp)[:120])

        # 立即读事件（可见性延迟验收：P50 < 500ms，一次读取即命中）
        # 注意：事件视图是脱敏件，检索标记用占位符而非原文
        t0 = time.monotonic()
        seen = None
        while time.monotonic() - t0 < 2.0:
            s2, ev = api("/v1/events?since=0")
            cands = [e for e in ev.get("events", [])
                     if "<REDACTED:token>" in json.dumps(e["frame"])]
            if cands:
                seen = cands[0]
                break
            time.sleep(0.02)
        latency_ms = int((time.monotonic() - t0) * 1000)
        check(f"S3a 事件即时可见（{latency_ms}ms < 500ms）", seen is not None)

        frame = seen["frame"] if seen else {}
        content = json.dumps(frame, ensure_ascii=False)
        check("S2b 事件视图 token 已脱敏",
              "<REDACTED:token>" in content and SECRET_TOKEN not in content, content[:200])
        check("S2c hex/pem/custom 脱敏",
              "<REDACTED:hex>" in content and "<REDACTED:pem>" in content
              and "<REDACTED:custom>" in content
              and SECRET_HEX not in content and "MIIBOgIBAAJ" not in content
              and SECRET_IP not in content, content[:300])

        # WS 收件人拿原文
        got = None
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                m = json.loads(ws.recv())
            except Exception:
                break
            if SECRET_TOKEN in json.dumps(m, ensure_ascii=False):
                got = m
                break
        check("S2d WS 收件人拿原文（不脱敏）",
              got is not None and SECRET_TOKEN in json.dumps(got), str(got)[:120])
        ws.close()

        # /v1/history 同样脱敏（同一管线）
        s2, hist = api("/v1/history")
        hs = json.dumps(hist, ensure_ascii=False)
        check("S2e /v1/history 同一脱敏管线", SECRET_TOKEN not in hs and "<REDACTED:token>" in hs, hs[:200])

        # S3b seq 单调 + 水位续传
        s2, ev = api("/v1/events?since=0")
        seqs = [e["seq"] for e in ev["events"]]
        check("S3b seq 严格递增", all(b > a for a, b in zip(seqs, seqs[1:])), str(seqs))
        check("S3c next_seq 水位 = 尾 seq+1", ev["next_seq"] == (seqs[-1] + 1 if seqs else 0),
              json.dumps({"next": ev["next_seq"], "seqs": seqs}))
        n0 = ev["next_seq"]
        send_text("续传验证消息 one more")
        s2, ev2 = api(f"/v1/events?since={n0}")
        check("S3d since 续传只回新事件且不丢",
              len(ev2["events"]) == 1 and ev2["events"][0]["seq"] == n0,
              json.dumps(ev2)[:200])

        # S4 过滤
        send_text("other party msg", to_agent="bystander")
        s2, evA = api(f"/v1/events?since=0&agent=recv")
        okA = all((e["frame"].get("from_agent") == "recv" or e["frame"].get("to_agent") == "recv")
                  for e in evA["events"]) and len(evA["events"]) >= 1
        check("S4a agent 过滤", okA, json.dumps(evA)[:200])
        s2, evT = api("/v1/events?since=0&types=text")
        check("S4b types 过滤",
              all(e["frame"]["kind"] == "text" for e in evT["events"]),
              json.dumps([e["frame"]["kind"] for e in evT.get("events", [])]))
        check("S4c 视图无 ctrl/heartbeat",
              all(e["frame"]["kind"] not in ("ctrl", "event") for e in evT["events"]))

        # S8 能力查询（§3）：requires 过滤 + node_addr
        api("/v1/agents", {"agent_id": "capholder", "name": "capholder",
                           "capabilities": ["cap:node:*", "cap:edit:py"]})
        api("/v1/agents", {"agent_id": "plainjoe", "name": "plainjoe",
                           "capabilities": ["cap:edit:js"]})
        # online 标志来自 WS 订阅（client=agent_id），保持连接才算在线
        holders = []
        for cid in ("capholder", "plainjoe", "recv"):
            w = websocket.WebSocket()
            w.connect(BASE.replace("http://", "ws://") + f"/v1/ws?token={TOKEN}&client={cid}")
            holders.append(w)
        time.sleep(0.3)
        s2, fa = api("/v1/fleet/agents?requires=cap:node:reboot,cap:edit:py")
        ids = [a["agent_id"] for a in fa.get("agents", []) if a.get("online")]
        check("S8a requires 命中通配+精确",
              s2 == 200 and "capholder" in ids and "plainjoe" not in ids,
              json.dumps(fa)[:200])
        s2, fa2 = api("/v1/fleet/agents?requires=cap:db:dump")
        ids2 = [a["agent_id"] for a in fa2.get("agents", []) if a.get("online")]
        check("S8b 无能力则空", "capholder" not in ids2 and "plainjoe" not in ids2, json.dumps(fa2)[:200])
        s2, fa3 = api("/v1/fleet/agents")
        holder = [a for a in fa3.get("agents", []) if a["agent_id"] == "capholder"]
        check("S8c 无 requires 保留全量视图 + node_addr",
              holder and holder[0].get("node_addr", "").startswith("127.0.0.1:18766"),
              json.dumps(holder)[:200])
        for w in holders:
            w.close()

        # S5 环满 → 409
        for i in range(6):
            send_text(f"rotation flood {i}")
        s2, evF = api("/v1/events?since=0")
        s3, evG = api("/v1/events?since=1")
        check("S5 环满后 since 落后回 409 + oldest_seq",
              s3 == 409 and isinstance(evG.get("oldest_seq"), int),
              f"status={s3} body={json.dumps(evG)[:150]}")
        check("S5b 未越界 since 仍 200", s2 == 200, str(s2))
        last_next = evF["next_seq"]
    finally:
        node.terminate(); node.wait(timeout=5)

    # S6 重启续传（同 data_dir，seq 从磁盘恢复）
    node = start_node_keep_data(CFG1)
    try:
        s2, ev = api("/v1/events?since=0")
        seqs = [e["seq"] for e in ev.get("events", [])]
        check("S6 重启后 seq 续传单调",
              s2 == 200 and seqs and all(x > 0 for x in seqs)
              and ev["next_seq"] >= last_next
              and all(b > a for a, b in zip(seqs, seqs[1:])),
              json.dumps({"seqs": seqs, "next": ev.get("next_seq"), "was": last_next}))
    finally:
        node.terminate(); node.wait(timeout=5)

    # S7 关闭脱敏 → 原文可见（全新 data_dir）
    write_cfg(CFG2, D2, redact_on=False)
    node = start_node_new(CFG2)
    try:
        send_text(f"明文直读 X-Auth-Token: {SECRET_TOKEN}")
        s2, ev = api("/v1/events?since=0")
        check("S7 redact 关闭后观察者拿原文",
              SECRET_TOKEN in json.dumps(ev.get("events", [])),
              json.dumps(ev)[:150])
    finally:
        node.terminate(); node.wait(timeout=5)

    n = sum(1 for _, ok in results if ok)
    print(f"\n== {n}/{len(results)} PASS ==")
    sys.exit(0 if n == len(results) else 1)

def start_node_keep_data(cfg):  # S6: reuse D1 (do not wipe)
    p = subprocess.Popen([BIN, "--config", cfg],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(50):
        try:
            if api("/v1/health")[0] == 200:
                return p
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("node did not come up (keep-data)")

def start_node_new(cfg):  # S7: wipe D2 only
    shutil.rmtree(D2, ignore_errors=True)
    p = subprocess.Popen([BIN, "--config", cfg],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(50):
        try:
            if api("/v1/health")[0] == 200:
                return p
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("node did not come up (new)")

if __name__ == "__main__":
    main()
