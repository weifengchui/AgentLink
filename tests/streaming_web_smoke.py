# -*- coding: utf-8 -*-
"""streaming_web_smoke — 流式结果前端逐字渲染冒烟（v0.7）。

/web（web/starmap.html）新增「任务流式视图」面板：复用页面既有无 client 观察者
WS（可见本机全部事件含定向帧），把任务帧按 seq 收敛成纯文本流，终态结果**逐字**
打上屏。本冒烟验证三件事：
  A1 页面交付面：/web 含面板标记 + 纯逻辑函数 + 展示态入口
  A2 页面脚本语法：node --check 全页 <script> 通过（无语法错）
  A3 纯逻辑单测（node 跑**页面里抽出的真实函数**）：seq 收敛/乱序丢弃/幂等重放/
     终态判定/结果文本化/逐字步进单调不越界
  A4 端到端数据面：真节点 + SDK 适配器（3 进度 + succeeded）；页面同款「无 client
     观察者 WS」收帧，喂进 A3 的函数，断言终态文本完整且逐字推进到最后一位

端口 18987（隔离，绝不指向生产 8765）。
"""
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.request

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "sdk"))
from adapter import TaskAdapter, TaskBus, RunResult  # noqa: E402

BIN = os.environ.get("AL_BIN",
                     os.path.expanduser("~/agentlink-target/debug/agentlink"))
ROOT = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch/al-stream")
TOKEN = "stream-smoke-token"
PORT = 18987
HTML = os.path.join(REPO, "web", "starmap.html")
EVIDENCE = []

RESULT_TEXT = "逐字渲染端到端：把终态结果一字一字打上屏，这正是 v0.7 的流式前端用例。"


def record(name, ok, detail=""):
    EVIDENCE.append({"scenario": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + " | "
          + json.dumps(detail, ensure_ascii=False)[:220], flush=True)


def extract_stream_js():
    html = open(HTML, encoding="utf-8").read()
    m = re.search(r"/\* AL-STREAM-BEGIN.*?AL-STREAM-END \*/", html, re.S)
    assert m, "页面缺 AL-STREAM 纯逻辑块"
    return m.group(0)


def node_run(script):
    p = subprocess.run(["node", "-e", script], capture_output=True, text=True,
                       timeout=30)
    if p.returncode != 0:
        raise RuntimeError("node 失败: " + p.stderr[-400:])
    return p.stdout.strip()


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
        if "al-stream" in cmd and "agentlink" in cmd:
            try:
                os.kill(int(pid), signal.SIGKILL)
            except Exception:
                pass


def api(path, body=None):
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    req = urllib.request.Request(
        f"http://127.0.0.1:{PORT}{path}", method="POST" if body is not None else "GET",
        data=data, headers={"X-Auth-Token": TOKEN, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=8) as r:
            return r.status, json.loads(r.read().decode())
    except Exception as e:
        return 0, {"error": str(e)[:160]}


class StreamRunner:
    def preflight(self, spec):
        return None

    def run(self, spec, report, cancelled):
        for st in ("preflight", "apply", "verify"):
            report(stage=st)
            time.sleep(0.15)
        return RunResult(ok=True, result=RESULT_TEXT)


def main():
    kill_stale()
    time.sleep(0.5)
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(ROOT, exist_ok=True)
    assert not probe(PORT), f"port {PORT} busy（生产隔离铁律）"

    html = open(HTML, encoding="utf-8").read()
    stream_js = extract_stream_js()

    procs = []
    try:
        # ---- A1 页面交付面 ------------------------------------------------
        checks = {
            "panel": 'data-al-stream="panel"' in html,
            "out": 'data-al-stream="out"' in html,
            "fn_reduce": "function alStreamReduce" in html,
            "fn_step": "function alStreamStep" in html,
            "entry": "window.__alStream=" in html,
            "wired": "STREAM=alStreamInit(stTid.value.trim())" in html,
            "observer_feed": "alStreamReduce(STREAM,env)" in html,
        }
        record("A1 /web 面板标记 + 纯逻辑 + 接线齐备", all(checks.values()), checks)

        # ---- A2 页面脚本语法 ----------------------------------------------
        m = re.search(r"<script>(.*)</script>", html, re.S)
        page_js = m.group(1)
        tmp = "/tmp/opencode/al-page-check.js"
        os.makedirs("/tmp/opencode", exist_ok=True)
        open(tmp, "w", encoding="utf-8").write(page_js)
        p = subprocess.run(["node", "--check", tmp], capture_output=True, text=True)
        record("A2 全页 <script> node --check 通过", p.returncode == 0,
               {"stderr": p.stderr[-120:]})

        # ---- A3 纯逻辑单测（node 跑页面抽出函数） --------------------------
        unit = stream_js + r"""
const R=[];
function chk(n,c){R.push([n,!!c]);}
let s=alStreamInit("t1");
chk("init", s.tid==="t1"&&s.seq===null&&!s.done);
function feed(st,o){const r=alStreamReduce(st,o);return r.changed?r.state:st;}
s=feed(s,{payload:{topic:"task_accepted",task_id:"t1",seq:1}});
s=feed(s,{payload:{topic:"task_progress",task_id:"t1",seq:2,stage:"apply"}});
s=feed(s,{payload:{topic:"task_progress",task_id:"t1",seq:3,stage:"verify"}});
chk("progress_lines", s.lines.length===3 && s.seq===3);
const dup=feed(s,{payload:{topic:"task_progress",task_id:"t1",seq:3,stage:"verify"}});
chk("dup_dropped", dup===s);
const late=alStreamReduce(s,{payload:{topic:"task_progress",task_id:"t1",seq:2,stage:"apply"}});
chk("out_of_order_dropped", late.changed===false);
const other=alStreamReduce(s,{payload:{topic:"task_progress",task_id:"t9",seq:99}});
chk("other_task_ignored", other.changed===false);
s=feed(s,{payload:{topic:"task_succeeded",task_id:"t1",seq:4,result:{a:1,b:"中"}}});
chk("terminal_ok", s.done&&!s.failed&&s.buf==='{"a":1,"b":"中"}');
// 终态后更大的 seq 仍可覆盖（如重投递终态帧）——但内容收敛即可，不崩
const after=alStreamReduce(s,{payload:{topic:"task_succeeded",task_id:"t1",seq:5,result:"done"}});
chk("after_terminal_safe", after.state.buf==="done");
let f=alStreamInit("t2");
f=feed(f,{payload:{topic:"task_progress",task_id:"t2",seq:1,stage:"x"}});
f=feed(f,{payload:{topic:"task_failed",task_id:"t2",seq:2,error:"boom"}});
chk("terminal_failed", f.done&&f.failed&&f.lines.join("|").includes("boom"));
let d=alStreamInit("t3");
d=feed(d,{payload:{topic:"task_declined",task_id:"t3",seq:1,reason:"missing cap"}});
chk("declined", d.done&&d.failed&&d.lines.join("|").includes("missing cap"));
// 逐字步进：单调、不越界、chars<=0 兜底 1
let shown="";const target="abcdef";
const seqLens=[];
for(let i=0;i<10;i++){shown=alStreamStep(shown,target,2);seqLens.push(shown.length);}
chk("typewriter_monotonic", seqLens.every((v,i)=>i===0||v>=seqLens[i-1]));
chk("typewriter_exact_end", shown===target);
chk("typewriter_no_overshoot", alStreamStep("abc","abcdef",999)==="abcdef");
chk("typewriter_chars_floor", alStreamStep("","abcd",0)==="a");
chk("text_of_null", alStreamResultText(null)==="");
const bad=R.filter(x=>!x[1]);
console.log(JSON.stringify({total:R.length,bad:bad.map(x=>x[0])}));
"""
        out = json.loads(node_run(unit))
        record("A3 纯逻辑单测（页面真实函数）", not out["bad"],
               {"total": out["total"], "bad": out["bad"]})

        # ---- A4 端到端数据面（页面同款观察者 WS） ---------------------------
        cfg = {
            "v": 1, "node_id": "57ea00000001", "name": "stream-smoke",
            "listen_port": PORT, "data_dir": os.path.join(ROOT, "data"),
            "discovery": {"enabled": False, "methods": [], "static_peers": []},
            "security": {"mode": "strict", "cluster_token": "stream-cluster"},
            "local_api": {"host": "127.0.0.1", "token": TOKEN},
            "delivery": {"ack_timeout_seconds": 5, "max_retries": 5,
                         "offline_ttl_seconds": 600, "dedup_cache_size": 2048,
                         "persist": False},
            "log": {"level": "info", "format": "text", "file": ""},
        }
        cfg_path = os.path.join(ROOT, "node.json")
        json.dump(cfg, open(cfg_path, "w"))
        p1 = subprocess.Popen([BIN, "run", "--config", cfg_path, "--no-open"],
                              stdout=open(os.path.join(ROOT, "node.log"), "a"),
                              stderr=subprocess.STDOUT)
        procs.append(p1)
        end = time.monotonic() + 25
        while time.monotonic() < end and not probe(PORT):
            time.sleep(0.2)
        assert probe(PORT), "节点未启动"

        # SDK 适配器（watched agent "sw"）
        wbus = TaskBus(f"http://127.0.0.1:{PORT}", TOKEN, agent="sw")
        ad = TaskAdapter(wbus, runner=StreamRunner(), capabilities=["cap:file"], log=print)
        threading.Thread(target=ad.serve_forever, daemon=True).start()
        end = time.monotonic() + 15
        while time.monotonic() < end:
            _, d = api("/v1/agents")
            if any(a.get("agent_id") == "sw" for a in d.get("agents", [])):
                break
            time.sleep(0.25)

        # 页面同款：无 client 观察者 WS
        import websocket
        ws = websocket.WebSocket()
        ws.connect(f"ws://127.0.0.1:{PORT}/v1/ws?token={TOKEN}")
        ws.settimeout(6)
        orch = TaskBus(f"http://127.0.0.1:{PORT}", TOKEN, agent="orch")
        orch.register([])
        tid = "stream-t1"
        orch.send("sw", {"topic": "task_spec", "task_id": tid, "seq": 1,
                         "type": "stream.demo", "intent": "流式渲染",
                         "params": {}})
        frames = []
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                raw = ws.recv()
            except Exception:
                break
            env = json.loads(raw)
            p = (env.get("payload") or {})
            if p.get("task_id") == tid:
                frames.append(env)
            if p.get("topic") == "task_succeeded":
                break
        ws.close()
        topics = [(f.get("payload") or {}).get("topic") for f in frames]
        seqs = [(f.get("payload") or {}).get("seq") for f in frames]
        ordered = all(seqs[i] < seqs[i + 1] for i in range(len(seqs) - 1) if
                      seqs[i] is not None and seqs[i + 1] is not None)
        record("A4a 观察者 WS 收到任务全帧且 seq 递增",
               "task_succeeded" in topics and ordered and len(frames) >= 5,
               {"topics": topics, "seqs": seqs})

        # 把真帧喂进页面真实函数（node），模拟逐字打屏到结束
        driver = stream_js + f"""
const frames={json.dumps(frames, ensure_ascii=False)};
// 只保留对"最关心的 task"有意义的帧（页面按订阅的 task_id 过滤）
let s=alStreamInit({json.dumps(tid)});
for(const env of frames){{const r=alStreamReduce(s,env);if(r.changed)s=r.state;}}
// 逐字推进直到打满
let guard=0;
while(s.shown.length<s.buf.length&&guard++<100000){{s.shown=alStreamStep(s.shown,s.buf,3);}}
console.log(JSON.stringify({{done:s.done,failed:s.failed,seq:s.seq,lines:s.lines.length,
 typed:s.shown,expected:s.buf,text:s.buf.length}}));
"""
        res = json.loads(node_run(driver))
        record("A4b 页面函数收敛终态并逐字打满",
               res["done"] and not res["failed"] and res["typed"] == res["expected"]
               and res["typed"] == RESULT_TEXT,
               {"seq": res["seq"], "lines": res["lines"], "chars": res["text"]})

    finally:
        for p in procs:
            try:
                p.send_signal(signal.SIGINT)
            except Exception:
                pass
        for p in procs:
            try:
                p.wait(timeout=6)
            except Exception:
                p.kill()
        with open(os.path.join(REPO, "tests", "streaming_web_smoke.jsonl"), "w") as f:
            for e in EVIDENCE:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        npass = sum(1 for e in EVIDENCE if e["pass"])
        print(f"streaming_web_smoke: {npass}/{len(EVIDENCE)} "
              f"→ {'OK' if npass == len(EVIDENCE) else 'FAILED'}", flush=True)
    sys.exit(0 if all(e["pass"] for e in EVIDENCE) else 1)


if __name__ == "__main__":
    main()
