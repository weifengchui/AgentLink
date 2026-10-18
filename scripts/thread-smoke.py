#!/usr/bin/env python3
"""thread-smoke — 第十五轮 R2 消息组 thread 线程键端到端冒烟（隔离双节点 18772/18773，绝不指向生产 8765/18765/18770/18771）。

场景：
  S1 同 thread 4 条跨双节点（预告/指令/go/done，done 带 reply_to=go 的 msg_id，A→B 与 B→A 双向），
     两端 /v1/history/search 返回 thread 全落账、方向与 reply_to 正确
  S2 无 thread 回归：thread=None 落账、reply_to 链字段不变（星图旧归组行为保持）
  S3 旧格式 history.jsonl（无 thread 字段的 v0.4.16 行）：启动恢复 + 磁盘检索均可加载不炸
  S4 混版本转发：新节点 → 旧二进制节点发带 thread 帧，旧节点照常收账不炸；
     旧节点落账里没有 thread 字段（未知字段丢失属预期 → 编排方 B 先升级即可）；
     无旧二进制时退化为本机自测：对未知新增字段的反序列化容忍（future_field 不炸）
  S5 CLI --thread：`agentlink apply --thread` 发出后落账 thread 正确

用法：python3 thread-smoke.py <path-to-new-agentlink-binary> [path-to-old-binary]
（old 默认探测 ~/agentlink-target/debug/agentlink；不存在则 S4 走退化路径）
退出码 0 = 全过。
"""
import json, os, shutil, socket, subprocess, sys, time, urllib.request

BIN = sys.argv[1] if len(sys.argv) > 1 else None
assert BIN and os.path.exists(BIN), "usage: thread-smoke.py <agentlink-binary> [old-binary]"
OLD = sys.argv[2] if len(sys.argv) > 2 else os.path.expanduser("~/agentlink-target/debug/agentlink")
PORT_A, PORT_B = 18772, 18773
BASE_A = f"http://127.0.0.1:{PORT_A}"
BASE_B = f"http://127.0.0.1:{PORT_B}"
TOKEN = "thr-smoke-token"
CLUSTER = "thr-smoke-cluster"
NODE_A, NODE_B = "a1b2c3d4e5f1", "a1b2c3d4e5f2"
SCRATCH = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch")
DA = os.path.join(SCRATCH, "al-r2-thread-a")
DB = os.path.join(SCRATCH, "al-r2-thread-b")
DO = os.path.join(SCRATCH, "al-r2-thread-old")
CFA = os.path.join(SCRATCH, "al-r2-thread-a.json")
CFB = os.path.join(SCRATCH, "al-r2-thread-b.json")
CFO = os.path.join(SCRATCH, "al-r2-thread-old.json")

results = []
def check(name, ok, detail=""):
    results.append((name, bool(ok)))
    print(("PASS " if ok else "FAIL ") + name + (f"  [{detail[:160]}]" if detail and not ok else ""))

def api(base, path, body=None, method=None):
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data,
        headers={"X-Auth-Token": TOKEN, "Content-Type": "application/json"},
        method=method or ("POST" if body is not None else "GET"))
    with urllib.request.urlopen(req, timeout=8) as r:
        return r.status, json.loads(r.read().decode())

def port_free(port):
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) != 0

def write_cfg(path, node_id, name, port, peers, data_dir):
    json.dump({
        "v": 1, "node_id": node_id, "name": name,
        "listen_port": port, "data_dir": data_dir,
        "history": {"enabled": True, "len": 500, "file": "history.jsonl"},
        "discovery": {"enabled": False, "static_peers": peers},
        "security": {"cluster_token": CLUSTER},
        "local_api": {"token": TOKEN},
        "web": {"open": False},
    }, open(path, "w"))

PROCS = []
def start(cfg):
    p = subprocess.Popen([BIN, "--config", cfg, "run", "--no-open"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    PROCS.append(p)
    return p

def start_old(cfg):
    p = subprocess.Popen([OLD, "--config", cfg, "run", "--no-open"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    PROCS.append(p)
    return p

def wait_health(base, timeout=12):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        try:
            if api(base, "/v1/health")[0] == 200:
                return True
        except Exception:
            pass
        time.sleep(0.15)
    return False

def stop_all():
    for p in PROCS:
        p.terminate()
    for p in PROCS:
        try:
            p.wait(timeout=5)
        except Exception:
            p.kill()
    PROCS.clear()
    for port in (PORT_A, PORT_B):
        t0 = time.monotonic()
        while not port_free(port) and time.monotonic() - t0 < 10:
            time.sleep(0.1)

def send(base, to, thread=None, reply_to=None, content="hello", from_agent="orch", to_agent="recv"):
    body = {"to": to, "type": "text", "action": "send",
            "from_agent": from_agent, "to_agent": to_agent,
            "payload": {"topic": "probe", "content": content}}
    if thread:
        body["thread"] = thread
    if reply_to:
        body["reply_to"] = reply_to
    return api(base, "/v1/send", body)

def wait_sent(base, to, thread=None, reply_to=None, content="hello", tries=100):
    """重试直到对端进入 peers 表、/v1/send 接受（首连握手需要时间）。"""
    last = None
    for _ in range(tries):
        try:
            s, r = send(base, to, thread=thread, reply_to=reply_to, content=content)
            if s == 200 and r.get("ok"):
                return r
            last = r
        except Exception as e:
            last = {"error": str(e)}
        time.sleep(0.2)
    raise RuntimeError(f"send never accepted: {last}")

def search(base, limit=400):
    s, d = api(base, f"/v1/history/search?limit={limit}")
    return d.get("matches", []) if s == 200 else []

def wait_search(base, pred, timeout=15):
    """history flush 是异步的：轮询直到 pred(matches) 为真，返回最后一次 matches。"""
    ms = []
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        ms = search(base)
        if pred(ms):
            return ms
        time.sleep(0.3)
    return ms

def find(matches, msg_id):
    for m in matches:
        if m.get("msg_id") == msg_id:
            return m
    return None

def main():
    for port in (PORT_A, PORT_B):
        assert port_free(port), f"port {port} occupied — 隔离端口被占，拒绝复用（勿碰并行任务端口）"

    # ---- 双新节点起服 ---------------------------------------------------------
    for d in (DA, DB, DO):
        shutil.rmtree(d, ignore_errors=True)
    write_cfg(CFA, NODE_A, "thread-a", PORT_A, [f"127.0.0.1:{PORT_B}"], DA)
    write_cfg(CFB, NODE_B, "thread-b", PORT_B, [f"127.0.0.1:{PORT_A}"], DB)
    na, nb = start(CFA), start(CFB)
    assert wait_health(BASE_A) and wait_health(BASE_B), "new nodes did not come up"

    # S1 同 thread 4 条（预告/指令/go/done），双向跨节点
    THR = "upgrade-v0.5.8-smoke"
    m1 = wait_sent(BASE_A, NODE_B, thread=THR, content="升级预告：今晚 21:00 滚动升级")["msg_id"]
    m2 = wait_sent(BASE_B, NODE_A, thread=THR, content="升级指令：各机停写准备")["msg_id"]
    m3 = wait_sent(BASE_A, NODE_B, thread=THR, content="upgrade_go 放行")["msg_id"]
    m4 = wait_sent(BASE_B, NODE_A, thread=THR, reply_to=m3, content="upgrade_done 全机完成")["msg_id"]
    for base, tag in ((BASE_A, "A"), (BASE_B, "B")):
        ms = wait_search(base, lambda x: len([m for m in x if m.get("thread") == THR]) == 4)
        thr = [m for m in ms if m.get("thread") == THR]
        check(f"S1{tag} 同 thread 4 条全部落账", len(thr) == 4, f"got {len(thr)}")
    ma = {m["msg_id"]: m for m in wait_search(
        BASE_A, lambda x: all(k in {m["msg_id"] for m in x} for k in (m1, m2, m3, m4)))}
    check("S1 A 端方向正确（A 发出=out×2、收到=in×2）",
          ma.get(m1, {}).get("direction") == "out" and ma.get(m3, {}).get("direction") == "out"
          and ma.get(m2, {}).get("direction") == "in" and ma.get(m4, {}).get("direction") == "in")
    check("S1 done 带 reply_to=go 且同 thread",
          ma[m4].get("reply_to") == m3 and ma[m4].get("thread") == THR, json.dumps(ma.get(m4, {})))

    # S2 无 thread 回归：链式两条 reply_to，thread 应为空
    r1 = wait_sent(BASE_A, NODE_B, content="旧链路根消息")["msg_id"]
    r2 = wait_sent(BASE_B, NODE_A, reply_to=r1, content="旧链回复")["msg_id"]
    ms = {m["msg_id"]: m for m in wait_search(
        BASE_B, lambda x: all(k in {m["msg_id"] for m in x} for k in (r1, r2)))}
    check("S2 无 thread 落账 thread=null（旧行为不变）",
          ms[r1].get("thread") in (None,) and ms[r2].get("reply_to") == r1,
          json.dumps([ms.get(r1, {}), ms.get(r2, {})]))

    stop_all()

    # ---- S3 旧格式 history.jsonl 可加载 --------------------------------------
    shutil.rmtree(DA, ignore_errors=True)
    os.makedirs(DA, exist_ok=True)
    OLD_ID = "oldformatline000000000000000000001"
    old_line = {"ts": "2026-10-10T10:00:00+08:00", "msg_id": OLD_ID, "node": "ffffffff0000",
                "direction": "in", "from": "ffffffff0000", "from_agent": "legacy",
                "to": NODE_A, "to_agent": "recv", "kind": "text", "action": "send",
                "payload": {"content": "v0.4.16 时代旧行（无 thread/seq/trace_id 字段）"}}
    with open(os.path.join(DA, "history.jsonl"), "w") as f:
        f.write(json.dumps(old_line, ensure_ascii=False) + "\n")
    start(CFA)
    ok_h = wait_health(BASE_A)
    ring = ok_h and any(e.get("msg_id") == OLD_ID
                        for e in api(BASE_A, "/v1/history")[1].get("history", []))
    ms3 = {m["msg_id"]: m for m in wait_search(BASE_A, lambda x: OLD_ID in {m["msg_id"] for m in x})}
    check("S3 旧格式行启动恢复进内存环不炸", ok_h and ring)
    check("S3 磁盘检索旧行 thread 缺省兼容",
          OLD_ID in ms3 and ms3[OLD_ID].get("thread") is None)

    stop_all()

    # ---- S4 混版本：新 A ← 旧二进制 O ----------------------------------------
    used_old = os.path.exists(OLD)
    if used_old:
        shutil.rmtree(DA, ignore_errors=True)
        shutil.rmtree(DO, ignore_errors=True)
        write_cfg(CFA, NODE_A, "thread-a", PORT_A, [f"127.0.0.1:{PORT_B}"], DA)
        write_cfg(CFO, "a1b2c3d4e5f3", "thread-old", PORT_B, [f"127.0.0.1:{PORT_A}"], DO)
        na = start(CFA)
        no = start_old(CFO)
        if not (wait_health(BASE_A) and wait_health(BASE_B)):
            used_old = False
        else:
            thr4 = "upgrade-v0.5.8-mixed"
            try:
                s4h, d4h = send(BASE_A, "a1b2c3d4e5f3", thread=thr4, content="发给旧节点的带 thread 帧")
                mid = d4h["msg_id"] if (s4h == 200 and d4h.get("ok")) else None
            except Exception:
                mid = None
            if mid is None:
                mid = wait_sent(BASE_A, "a1b2c3d4e5f3", thread=thr4,
                                content="发给旧节点的带 thread 帧")["msg_id"]
            ma4 = {m["msg_id"]: m for m in wait_search(
                BASE_A, lambda x: mid in {m["msg_id"] for m in x})}
            check("S4 新节点侧带 thread 帧发出并落账",
                  ma4.get(mid, {}).get("thread") == thr4, json.dumps(ma4.get(mid, {})))
            try:
                mo4 = wait_search(BASE_B, lambda x: find(x, mid) is not None)
                o_hit = find(mo4, mid)
                o_alive = api(BASE_B, "/v1/health")[0] == 200
            except Exception:
                o_hit, o_alive = None, False
            check("S4 旧节点收带未知 thread 字段帧不炸、照常落账", o_alive and o_hit is not None,
                  f"alive={o_alive} hit={bool(o_hit)}")
            check("S4 旧版本落账无 thread 字段（转发丢字段→编排方先升级）",
                  o_hit is not None and "thread" not in o_hit)
            stop_all()
    if not used_old:
        # 退化路径（单机自测）：新节点对未知新增字段容忍
        shutil.rmtree(DA, ignore_errors=True)
        write_cfg(CFA, NODE_A, "thread-a", PORT_A, [], DA)
        start(CFA)
        assert wait_health(BASE_A), "node A did not come up"
        body = {"to": NODE_A, "type": "text", "action": "send", "thread": "unknown-field-demo",
                "unknown_future_field": 42, "payload": {"content": "含未来字段的帧"}}
        s4, d4 = api(BASE_A, "/v1/send", body)
        ms4 = {m["msg_id"]: m for m in wait_search(
            BASE_A, lambda x: d4.get("msg_id") in {m["msg_id"] for m in x})}
        check("S4(退化) 未知新增字段反序列化容忍、thread 照常落账",
              s4 == 200 and d4.get("ok") and ms4.get(d4.get("msg_id"), {}).get("thread") == "unknown-field-demo",
              json.dumps(d4)[:160])
        stop_all()

    # ---- S5 CLI --thread ------------------------------------------------------
    shutil.rmtree(DA, ignore_errors=True)
    write_cfg(CFA, NODE_A, "thread-a", PORT_A, [], DA)
    start(CFA)
    assert wait_health(BASE_A), "node A did not come up"
    spec = os.path.join(SCRATCH, "al-r2-thread-spec.json")
    json.dump({"intent": "消息组冒烟任务", "correlation": "r2-cli", "requires": ["cap:noop"]},
              open(spec, "w"))
    cp = subprocess.run([BIN, "--config", CFA, "--url", BASE_A, "--token", TOKEN,
                         "apply", "-f", spec, "--to-agent", "worker",
                         "--thread", "cli-thread-r2"],
                        capture_output=True, text=True, timeout=30)
    mid5 = None
    for line in cp.stdout.splitlines():
        if line.startswith("msg_id:"):
            mid5 = line.split(":", 1)[1].strip()
    ms5 = {m["msg_id"]: m for m in wait_search(
        BASE_A, lambda x: mid5 in {m["msg_id"] for m in x})}
    check("S5 CLI --thread 发出后落账",
          cp.returncode == 0 and mid5 in ms5 and ms5[mid5].get("thread") == "cli-thread-r2",
          f"rc={cp.returncode} out={cp.stdout[-120:]} err={cp.stderr[-120:]}")

    stop_all()
    n = sum(1 for _, ok in results if ok)
    print(f"\n== {n}/{len(results)} PASS ==")
    sys.exit(0 if n == len(results) else 1)

if __name__ == "__main__":
    try:
        main()
    finally:
        stop_all()
