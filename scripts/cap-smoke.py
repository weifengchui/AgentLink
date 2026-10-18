#!/usr/bin/env python3
"""cap-smoke — 第十六轮 R1/R2 浏览器冒烟的环境编排（隔离端口 18776/18777，绝不指向生产）。

起双节点、灌双向同 thread 往来 4 条 + 旁路话题若干，把种子数据与节点状态写
scratch/r16-env.json 供 browser-use 步骤读取；--stop 收尾杀进程。
用法：python3 cap-smoke.py start <binary> | python3 cap-smoke.py stop
"""
import json, os, socket, subprocess, sys, time, urllib.request

SCRATCH = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch")
ENVF = os.path.join(SCRATCH, "r16-env.json")
BIN = os.path.expanduser("~/agentlink-target/release/agentlink")
PORT_A, PORT_B = 18776, 18777
BASE_A = f"http://127.0.0.1:{PORT_A}"
BASE_B = f"http://127.0.0.1:{PORT_B}"
TOKEN = "r16-cap-token"
CLUSTER = "r16-cap-cluster"
NODE_A, NODE_B = "f1a1b2c3d4e5", "f2a1b2c3d4e5"
DA = os.path.join(SCRATCH, "al-r16-cap-a")
DB = os.path.join(SCRATCH, "al-r16-cap-b")
CFA = os.path.join(SCRATCH, "al-r16-cap-a.json")
CFB = os.path.join(SCRATCH, "al-r16-cap-b.json")


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
    }, open(path, "w"), ensure_ascii=False)


def wait_health(base, timeout=15):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        try:
            if api(base, "/v1/health")[0] == 200:
                return True
        except Exception:
            pass
        time.sleep(0.15)
    return False


def send(base, to, thread, content, frm, to_agt, reply_to=None):
    body = {"to": to, "type": "text", "action": "send",
            "from_agent": frm, "to_agent": to_agt,
            "payload": {"topic": thread.split("|")[-1] if "|" in thread else thread, "content": content}}
    if thread:
        body["thread"] = thread
    if reply_to:
        body["reply_to"] = reply_to
    for _ in range(80):
        try:
            s, r = api(base, "/v1/send", body)
            if s == 200 and r.get("ok"):
                return r
        except Exception:
            pass
        time.sleep(0.2)
    raise RuntimeError(f"send failed {content!r}")


def start():
    os.makedirs(DA, exist_ok=True); os.makedirs(DB, exist_ok=True)
    for p in (DA, DB):
        h = os.path.join(p, "history.jsonl")
        if os.path.exists(h):
            os.remove(h)
    write_cfg(CFA, NODE_A, "CapA", PORT_A, [f"127.0.0.1:{PORT_B}"], DA)
    write_cfg(CFB, NODE_B, "CapB", PORT_B, [f"127.0.0.1:{PORT_A}"], DB)
    for cfg in (CFA, CFB):
        subprocess.Popen([BIN, "--config", cfg, "run", "--no-open"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not (wait_health(BASE_A) and wait_health(BASE_B)):
        print("FAIL: health timeout"); sys.exit(1)
    thr = "r16-thread-demo"
    ids = []
    ids.append(send(BASE_A, NODE_B, thr, "R16-01 A发B：星图演示 kickoff", "alice", "bob")["msg_id"])
    time.sleep(0.4)
    ids.append(send(BASE_B, NODE_A, thr, "R16-02 B回A：收到，这就排演示脚本", "bob", "alice", reply_to=ids[0])["msg_id"])
    time.sleep(0.4)
    ids.append(send(BASE_A, NODE_B, thr, "R16-03 A发B：go——开始录屏", "alice", "bob", reply_to=ids[1])["msg_id"])
    time.sleep(0.4)
    ids.append(send(BASE_B, NODE_A, thr, "R16-04 B回A：done——演示段落已归档", "bob", "alice", reply_to=ids[2])["msg_id"])
    # 旁路话题（验证分组隔离）
    send(BASE_A, NODE_B, "r16-side", "R16-S1 旁路话题首条", "alice", "bob")
    time.sleep(0.3)
    send(BASE_B, NODE_A, "r16-side", "R16-S2 旁路话题回复", "bob", "alice")
    time.sleep(0.6)
    s, hb = api(BASE_A, "/v1/health")
    st, setup = api(BASE_A, "/web/setup")
    json.dump({"base": BASE_A, "token": TOKEN, "setup_token": setup.get("token"),
               "node_a": NODE_A, "node_b": NODE_B, "ids": ids, "ts": time.time()},
              open(ENVF, "w"), ensure_ascii=False, indent=1)
    print("ENV-OK", ENVF)
    print("seeded:", ids)


def stop():
    # 按端口持有者 pid 杀（pkill -f '--config …' 会把 '--config' 当自身选项解析，静默失效）
    import re, subprocess as sp
    out = sp.run(["ss", "-tlnp"], capture_output=True, text=True).stdout
    pids = set()
    for line in out.splitlines():
        if f":{PORT_A} " in line or f":{PORT_B} " in line:
            pids.update(int(m) for m in re.findall(r"pid=(\d+)", line))
    for p in pids:
        try:
            os.kill(p, 15)
        except ProcessLookupError:
            pass
    t0 = time.monotonic()
    while time.monotonic() - t0 < 10:
        if port_free(PORT_A) and port_free(PORT_B):
            print("STOPPED")
            return
        time.sleep(0.2)
    print("STOP-FAIL: ports still busy")
    sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "stop":
        stop()
    else:
        if len(sys.argv) > 2:
            BIN = os.path.abspath(sys.argv[2])
        start()
