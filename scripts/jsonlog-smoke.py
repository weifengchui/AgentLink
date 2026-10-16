#!/usr/bin/env python3
"""jsonlog-smoke — v0.5 §7.1 结构化 JSON 日志 + trace_id 端到端验证（隔离总线 18773/18774，绝不指向生产）。

场景：
  S1  /v1/health features 声明 json-log
  S2  注册变更 / peer up 的结构化事件（9 字段齐备）
  S3  task 生命周期收→转→投：内核生成 trace_id（msg_id 前 8 位），
      注入帧 metadata 后跨节点续链（strict HMAC 与注入并存，无 reject）
  S4  下游 agent 收到带 trace_id 的帧（注入策略验收）
  S5  执行方回报不显式带 trace_id：reply_to→trace 映射表续链
  S6  执行方回报显式 echo trace_id：沿用不重生成
  S7  history 落盘事件带全字段（correlation/topic）
  S8  同一 trace_id 串起 ≥3 类生命周期事件
  S9  默认 text 格式（不带任何 log 配置）输出零 al.* 事件——舰队向后兼容

用法：python3 jsonlog-smoke.py <path-to-agentlink-binary>
退出码 0 = 全过。
"""
import json, os, shutil, subprocess, sys, time, urllib.request

BIN = sys.argv[1] if len(sys.argv) > 1 else None
assert BIN and os.path.exists(BIN), "usage: jsonlog-smoke.py <agentlink-binary>"
TOKEN = "***"
CLUSTER = "smokecluster"
P1, P2, P3 = 18773, 18774, 18775
B1 = f"http://127.0.0.1:{P1}"
B2 = f"http://127.0.0.1:{P2}"
B3 = f"http://127.0.0.1:{P3}"
SCRATCH = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch")
D = {n: os.path.join(SCRATCH, f"al-v05-jsonlog-{n}") for n in ("n1", "n2", "n3")}
L = {n: os.path.join(SCRATCH, f"al-v05-jsonlog-{n}.log") for n in ("n1", "n2", "n3")}
C = {n: os.path.join(SCRATCH, f"al-v05-jsonlog-{n}.json") for n in ("n1", "n2", "n3")}

results = []
def check(name, ok, detail=""):
    results.append((name, bool(ok)))
    print(("PASS " if ok else "FAIL ") + name + (f"  [{detail}]" if detail and not ok else ""))

def api(base, path, body=None, method=None):
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data,
        headers={"X-Auth-Token": TOKEN, "Content-Type": "application/json"},
        method=method or ("POST" if body is not None else "GET"))
    try:
        with urllib.request.urlopen(req, timeout=8) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode() or "{}")

def write_cfg(name, port, node_id, peers, log_section):
    cfg = {
        "v": 1, "node_id": node_id, "name": f"jsonlog-{name}",
        "listen_port": port, "data_dir": D[name],
        "history": {"enabled": True, "len": 200, "file": "history.jsonl"},
        "discovery": {"enabled": False, "static_peers": peers},
        "security": {"cluster_token": CLUSTER},
        "local_api": {"token": TOKEN},
        "web": {"open": False},
    }
    if log_section:
        cfg["log"] = log_section
    json.dump(cfg, open(C[name], "w"))

procs = {}
def start(name):
    p = subprocess.Popen([BIN, "--config", C[name], "run", "--no-open"],
                         stdout=open(L[name], "w"), stderr=subprocess.DEVNULL)
    procs[name] = p
    base = {"n1": B1, "n2": B2, "n3": B3}[name]
    for _ in range(50):
        try:
            s, h = api(base, "/v1/health")
            if s == 200:
                return h
        except Exception:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"node {name} did not come up")

def json_events(name):
    """All al.* structured lines parsed from a node log."""
    out = []
    try:
        lines = open(L[name], encoding="utf-8", errors="replace").read().splitlines()
    except FileNotFoundError:
        return out
    for ln in lines:
        ln = ln.strip()
        if not ln.startswith("{"):
            continue
        try:
            v = json.loads(ln)
        except Exception:
            continue
        f = v.get("fields", {})
        if str(f.get("message", "")).startswith("al."):
            out.append(f)
    return out

def wait_events(name, pred, timeout=8.0):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        hit = [f for f in json_events(name) if pred(f)]
        if hit:
            return hit
        time.sleep(0.2)
    return []

REQ_KEYS = ["trace_id", "correlation_id", "msg_id", "from", "to",
            "from_agent", "to_agent", "node_id", "topic"]

import websocket
NODE1 = NODE2 = None

def main():
    global NODE1, NODE2
    jsonlog = {"level": "debug", "format": "json"}
    write_cfg("n1", P1, "fa15ee000a01", [f"127.0.0.1:{P2}"], jsonlog)
    write_cfg("n2", P2, "fa15ee000b02", [f"127.0.0.1:{P1}"], jsonlog)
    write_cfg("n3", P3, "fa15ee000c03", [], None)  # 默认 text 输出（向后兼容哨兵）
    h1 = start("n1"); NODE1 = h1["node_id"]
    h2 = start("n2"); NODE2 = h2["node_id"]

    # S1 health features
    check("S1a n1 features 含 json-log", "json-log" in h1.get("features", []), str(h1.get("features")))
    check("S1b n2 features 含 json-log", "json-log" in h2.get("features", []), str(h2.get("features")))

    # agents + WS
    api(B1, "/v1/agents", {"agent_id": "orch", "name": "orch"})
    api(B2, "/v1/agents", {"agent_id": "exec", "name": "exec"})
    wsx = websocket.WebSocket()
    wsx.connect(B2.replace("http://", "ws://") + f"/v1/ws?token={TOKEN}&client=exec")
    wsx.settimeout(5)
    wso = websocket.WebSocket()
    wso.connect(B1.replace("http://", "ws://") + f"/v1/ws?token={TOKEN}&client=orch")
    wso.settimeout(5)

    # S2 registry + peer events
    reg = wait_events("n2", lambda f: f.get("message") == "al.agent.register" and f.get("to_agent") == "exec")
    check("S2a n2 al.agent.register 结构化事件", bool(reg), "无注册事件")
    up = wait_events("n1", lambda f: f.get("message") == "al.peer.up" and f.get("from") == NODE2)
    check("S2b n1 al.peer.up 含 9 字段", bool(up) and all(k in up[0] for k in REQ_KEYS), str(up[:1]))
    # 等 n2 也连上（回报方向需要 n2 的 sink）
    up2 = wait_events("n2", lambda f: f.get("message") == "al.peer.up" and f.get("from") == NODE1)
    check("S2c n2 al.peer.up（双向静态链路）", bool(up2), "n2 未见 peer up")

    # S3 task_spec 生命周期：收→转→投
    spec = {"topic": "task_spec", "task_id": "jlog-1", "seq": 1, "correlation": "cbatch-9",
            "type": "probe.echo", "intent": "smoke", "params": {"n": 1}}
    st, r = api(B1, "/v1/send", {"from": NODE1, "to": NODE2, "type": "text", "action": "send",
                                 "from_agent": "orch", "to_agent": "exec", "payload": spec})
    check("S3a task_spec 发送 200", st == 200 and r.get("ok"), json.dumps(r)[:120])
    mid = r["msg_id"]
    texp = "t-" + mid[:8]  # 内核生成规则：msg_id 前 8 位

    s_events = wait_events("n1", lambda f: f.get("message") == "al.message.send" and f.get("msg_id") == mid)
    check("S3b n1 al.message.send trace=内核生成(msg_id前8位)",
          bool(s_events) and s_events[0]["trace_id"] == texp, str(s_events[:1]))
    f_events = wait_events("n1", lambda f: f.get("message") == "al.message.forward" and f.get("msg_id") == mid)
    check("S3c n1 al.message.forward 同 trace（转发）",
          bool(f_events) and f_events[0]["trace_id"] == texp, str(f_events[:1]))
    i_events = wait_events("n2", lambda f: f.get("message") == "al.message.incoming" and f.get("msg_id") == mid)
    check("S3d n2 al.message.incoming 同 trace（跨节点续链）",
          bool(i_events) and i_events[0]["trace_id"] == texp, str(i_events[:1]))
    d_events = wait_events("n2", lambda f: f.get("message") == "al.message.deliver" and f.get("msg_id") == mid)
    check("S3e n2 al.message.deliver 同 trace（投递）",
          bool(d_events) and d_events[0]["trace_id"] == texp, str(d_events[:1]))
    rej = [f for f in json_events("n2") + json_events("n1") if f.get("message") == "al.message.reject"]
    raw = open(L["n1"], encoding="utf-8", errors="replace").read() + open(L["n2"], encoding="utf-8", errors="replace").read()
    check("S3f strict HMAC 与 trace 注入并存（无 reject）",
          not rej and "incoming rejected" not in raw, "出现 reject")

    # S4 下游 agent 收到注入的 trace_id（注入 metadata 策略验收）
    got = None
    t0 = time.monotonic()
    while time.monotonic() - t0 < 5:
        try:
            fr = json.loads(wsx.recv())
        except Exception:
            break
        if fr.get("msg_id") == mid:
            got = fr
            break
    check("S4 exec 的 WS 帧带注入的 trace_id（metadata 注入策略）",
          bool(got) and got.get("trace_id") == texp, json.dumps(got)[:160] if got else "未收到 spec 帧")

    # S5 回报不带 trace_id：reply_to→trace 映射表续链
    st, r5 = api(B2, "/v1/send", {"from": NODE2, "to": NODE1, "type": "text", "action": "send",
                                  "from_agent": "exec", "to_agent": "orch", "reply_to": mid,
                                  "payload": {"topic": "task_accepted", "task_id": "jlog-1",
                                              "correlation": "cbatch-9"}})
    check("S5a task_accepted 回报 200", st == 200 and r5.get("ok"), json.dumps(r5)[:120])
    m5 = r5["msg_id"]
    e5 = wait_events("n2", lambda f: f.get("message") == "al.message.send" and f.get("msg_id") == m5)
    check("S5b 回报未 echo trace：reply_to 映射表续链",
          bool(e5) and e5[0]["trace_id"] == texp, str(e5[:1]))

    # S6 回报显式 echo trace_id：沿用
    st, r6 = api(B2, "/v1/send", {"from": NODE2, "to": NODE1, "type": "text", "action": "send",
                                  "from_agent": "exec", "to_agent": "orch", "reply_to": mid,
                                  "trace_id": texp,
                                  "payload": {"topic": "task_succeeded", "task_id": "jlog-1",
                                              "correlation": "cbatch-9", "seq": 2}})
    check("S6a task_succeeded（echo trace）200", st == 200 and r6.get("ok"), json.dumps(r6)[:120])
    m6 = r6["msg_id"]
    e6 = wait_events("n1", lambda f: f.get("message") == "al.message.incoming" and f.get("msg_id") == m6)
    check("S6b n1 收到回报 trace_id 与 spec 同链",
          bool(e6) and e6[0]["trace_id"] == texp, str(e6[:1]))
    got2 = None
    t0 = time.monotonic()
    while time.monotonic() - t0 < 6:
        try:
            fr = json.loads(wso.recv())
        except Exception:
            break
        if fr.get("msg_id") == m6:
            got2 = fr
            break
    check("S6c orch 的 WS 帧沿用 echo 的 trace_id",
          bool(got2) and got2.get("trace_id") == texp, json.dumps(got2)[:160] if got2 else "未收到回报帧")

    # S7 history 落盘事件
    h7 = wait_events("n2", lambda f: f.get("message") == "al.history.persist" and f.get("msg_id") == mid
                     and f.get("direction") == "in")
    check("S7 n2 al.history.persist 带 trace/correlation/topic",
          bool(h7) and h7[0]["trace_id"] == texp and h7[0]["correlation_id"] == "cbatch-9"
          and h7[0]["topic"] == "task_spec", str(h7[:1]))

    # S8 同一 trace 串起收→转→投 ≥3 类事件
    allf = json_events("n1") + json_events("n2")
    kinds = {f["message"] for f in allf if f.get("trace_id") == texp}
    need = {"al.message.send", "al.message.forward", "al.message.incoming", "al.message.deliver"}
    check("S8 同一 trace_id 覆盖 收→转→投 全部四类事件", need.issubset(kinds), str(sorted(kinds)))
    cnt = sum(1 for f in allf if f.get("trace_id") == texp)
    check("S8b 该 trace 事件数 ≥8", cnt >= 8, f"count={cnt}")

    # S9 默认 text 输出零变化（不带 log 配置的节点）
    h3 = start("n3")
    api(B3, "/v1/agents", {"agent_id": "solo", "name": "solo"})
    st, _ = api(B3, "/v1/send", {"from": h3["node_id"], "to": h3["node_id"], "type": "text",
                                 "action": "send", "from_agent": "solo", "to_agent": "solo",
                                 "payload": {"topic": "probe", "content": "text-default"}})
    time.sleep(1.0)
    txt = open(L["n3"], encoding="utf-8", errors="replace").read()
    import re
    plain = re.sub(r"\x1b\[[0-9;]*m", "", txt)
    check("S9 默认 text 节点：无 al.* 结构化事件、无 JSON 行（舰队输出格式不变）",
          st == 200 and '"fields":' not in plain and not any(
              ln.startswith('{"timestamp":') for ln in plain.splitlines())
          and " INFO agentlink" in plain)

    wsx.close(); wso.close()
    evd = {"trace_id": texp, "spec_msg_id": mid, "reply_map_msg_id": m5,
           "reply_echo_msg_id": m6, "trace_event_count": cnt, "trace_kinds": sorted(kinds)}
    json.dump(evd, open(os.path.join(SCRATCH, "al-v05-jsonlog-evidence.json"), "w"), indent=2)
    print("EVIDENCE " + json.dumps(evd, ensure_ascii=False))

if __name__ == "__main__":
    try:
        main()
    finally:
        for p in procs.values():
            p.kill()
    total = len(results); ok = sum(1 for _, v in results if v)
    print(f"SMOKE_RESULT {ok}/{total}")
    sys.exit(0 if ok == total else 1)
