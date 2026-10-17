#!/usr/bin/env python3
"""web-timeline-lockdown-smoke — v0.5 §3.2 /web 任务 Timeline + §4.5 一键熔断 lockdown 端到端验证。

隔离总线端口 18772、独立数据目录，绝不指向生产（127.0.0.1:8765），不与在线 agent 通信。

场景：
  A1  health.features 含 lockdown
  A2  /v1/lockdown GET/POST 未鉴权 401
  A3  GET /v1/lockdown 初态 on=false, blocked_count=0, replay_on_resume=false
  A4  正常态任务帧轨迹投影 running
  A5  POST on=true 开启熔断
  A6  熔断中业务帧被拦（/v1/send 400 lockdown），控制面帧畅通，history 查询畅通
  A7  blocked_count/blocked_out 计数与审计标记（history blocked=true）
  A8  被拦任务帧只进 trail 不推进状态机（state 仍 running，blocked_count=1）
  A9  POST on=false 恢复
  A10 恢复后新帧放行；被拦帧不补投（blocked 记录数不变）
  A11 /web HTML 关键元素齐全，且与 /v1/tasks 的 topic 词表一致

用法：python3 web-timeline-lockdown-smoke.py <path-to-agentlink-binary>
退出码 0 = 全过。
"""
import json, os, shutil, subprocess, sys, time, urllib.request

BIN = sys.argv[1] if len(sys.argv) > 1 else None
assert BIN and os.path.exists(BIN), "usage: web-timeline-lockdown-smoke.py <agentlink-binary>"
BASE = "http://127.0.0.1:18772"
TOKEN = "smoketoken2"
SCRATCH = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch")
D1 = os.path.join(SCRATCH, "al-v05-lkd-testdata")
CFG1 = os.path.join(SCRATCH, "al-v05-lkd-node.json")

results = []
def check(name, ok, detail=""):
    results.append((name, bool(ok)))
    print(("PASS " if ok else "FAIL ") + name + (f"  [{detail}]" if detail and not ok else ""))

def api(path, body=None, method=None, token=True):
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Auth-Token"] = TOKEN
    req = urllib.request.Request(BASE + path, data=data, headers=headers,
        method=method or ("POST" if body is not None else "GET"))
    try:
        with urllib.request.urlopen(req, timeout=8) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode() or "{}")
        except Exception:
            return e.code, {}

def raw(path):
    req = urllib.request.Request(BASE + path, headers={"X-Auth-Token": TOKEN})
    with urllib.request.urlopen(req, timeout=8) as r:
        return r.status, r.read().decode()

def write_cfg(path, data_dir):
    json.dump({
        "v": 1, "node_id": "fa15ee000772", "name": "v05-lkd-smoke",
        "listen_port": 18772, "data_dir": data_dir,
        "history": {"enabled": True, "len": 400, "file": "history.jsonl"},
        "discovery": {"enabled": False},
        "security": {"cluster_token": "smokecluster"},
        "local_api": {"token": TOKEN},
        "web": {"open": False},
    }, open(path, "w"))

def start_node(cfg, wipe_dirs=()):
    for d in wipe_dirs:
        shutil.rmtree(d, ignore_errors=True)
    p = subprocess.Popen([BIN, "--config", cfg],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(50):
        try:
            if api("/v1/health", token=False)[0] == 200:
                return p
        except Exception:
            pass
        time.sleep(0.1)
    raise RuntimeError("node did not come up")

NODE = None

def send_msg(payload, from_agent="orch", to_agent="worker", kind="msg"):
    return api("/v1/send", {"to": NODE, "type": kind, "action": "send",
                           "from_agent": from_agent, "to_agent": to_agent,
                           "payload": payload})

def task(topic, task_id, **extra):
    p = {"topic": topic, "task_id": task_id}
    p.update(extra)
    if topic in ("task_accepted", "task_progress", "task_succeeded"):
        return send_msg(p, from_agent="worker", to_agent="orch")
    return send_msg(p)

def tasks(query=""):
    return api("/v1/tasks" + (f"?{query}" if query else ""))

def find(lst, tid):
    return next((t for t in lst if t["task_id"] == tid), None)

def hist_msgs():
    s, d = api("/v1/history?limit=400")
    return d.get("history") or d.get("messages") or d.get("entries") or []

def main():
    global NODE
    write_cfg(CFG1, D1)
    node = start_node(CFG1, wipe_dirs=(D1,))
    NODE = api("/v1/health", token=False)[1]["node_id"]
    try:
        # A1 health features 含 lockdown
        s, h = api("/v1/health", token=False)
        feats = h.get("features", [])
        check("A1 health.features 含 lockdown（含 events/redact/tasks）",
              "lockdown" in feats and "tasks" in feats, str(feats))

        # A2 管理端点未鉴权 401
        req = urllib.request.Request(BASE + "/v1/lockdown")
        try:
            c1 = urllib.request.urlopen(req, timeout=5).status
        except urllib.error.HTTPError as e:
            c1 = e.code
        req2 = urllib.request.Request(BASE + "/v1/lockdown",
            data=b'{"on":true}', headers={"Content-Type": "application/json"}, method="POST")
        try:
            c2 = urllib.request.urlopen(req2, timeout=5).status
        except urllib.error.HTTPError as e:
            c2 = e.code
        check("A2 GET/POST /v1/lockdown 无 token 均 401", c1 == 401 and c2 == 401, f"{c1}/{c2}")

        # A3 初态
        s, lk = api("/v1/lockdown")
        check("A3 初态 on=false blocked_count=0 replay_on_resume=false",
              s == 200 and lk.get("on") is False and lk.get("blocked_count") == 0
              and lk.get("replay_on_resume") is False, json.dumps(lk)[:160])

        # A4 正常态任务轨迹
        task("task_spec", "t-live", type="deploy.demo", intent="熔断冒烟", correlation="smokeCorr")
        task("task_accepted", "t-live")
        s, tv = tasks("task_id=t-live")
        t = find(tv.get("tasks", []), "t-live")
        check("A4 正常态 spec→accepted 投影 running，trail 2 帧无 blocked",
              t and t["state"] == "running" and t["blocked_count"] == 0
              and len(t["trail"]) == 2 and not any(x["blocked"] for x in t["trail"]),
              json.dumps(t)[:220])

        # A5 开启熔断
        s, lk = api("/v1/lockdown", {"on": True})
        check("A5 POST on=true 开启熔断", s == 200 and lk.get("ok") and lk.get("on") is True,
              json.dumps(lk)[:160])

        # A6 熔断中：业务帧被拦 / 控制面帧畅通 / 查询面畅通
        s6, r6 = send_msg({"content": "熔断期间消息"}, kind="msg")
        blocked_msg_id = None
        s6c, r6c = send_msg({"ok": True}, kind="ctrl", to_agent=None)
        s6h = api("/v1/history?limit=5")[0]
        s6t = api("/v1/tasks")[0]
        check("A6 熔断中业务帧 400(lockdown)、ctrl 帧 200、history/tasks 查询畅通",
              s6 == 400 and "lockdown" in str(r6.get("error", "")).lower()
              and s6c == 200 and r6c.get("ok") and s6h == 200 and s6t == 200,
              f"{s6}:{json.dumps(r6)[:100]} ctrl:{s6c}")

        # A7 计数与审计标记
        s, lk = api("/v1/lockdown")
        hm = hist_msgs()
        marked = [e for e in hm if e.get("blocked") is True]
        check("A7 blocked_count=1 blocked_out=1 且 history 有 blocked=true 审计记录",
              lk.get("blocked_count") == 1 and lk.get("blocked_out") == 1
              and len(marked) >= 1 and marked[0].get("action") == "send",
              json.dumps(lk) + f" marked={len(marked)}")

        # A8 被拦任务帧不推进状态机
        s8, r8 = task("task_progress", "t-live", percent=50)
        s, tv = tasks("task_id=t-live")
        t = find(tv.get("tasks", []), "t-live")
        tl = t["trail"][-1] if t and t["trail"] else {}
        check("A8 被拦任务帧只进 trail：state 仍 running，blocked_count=1，trail 末帧 blocked=true",
              s8 == 400 and t and t["state"] == "running" and t["blocked_count"] == 1
              and len(t["trail"]) == 3 and tl.get("blocked") is True
              and tl.get("topic") == "task_progress" and t["progress_count"] == 0,
              json.dumps(t)[:260])

        # A9 恢复
        s, lk = api("/v1/lockdown", {"on": False})
        check("A9 POST on=false 恢复，blocked_count 保留为累计 2",
              s == 200 and lk.get("on") is False and lk.get("blocked_count") == 2,
              json.dumps(lk)[:160])

        # A10 恢复后放行 + 不补投
        s10, r10 = send_msg({"content": "恢复后消息"})
        blocked_ids = {e.get("msg_id") for e in hist_msgs() if e.get("blocked") is True}
        cnt_blocked = len([e for e in hist_msgs() if e.get("blocked") is True])
        s10b, r10b = task("task_progress", "t-live", percent=60)
        s, tv = tasks("task_id=t-live")
        t = find(tv.get("tasks", []), "t-live")
        check("A10 恢复后新帧放行；被拦帧不补投（blocked 记录仍 2 条）；重发 progress 正常推进 trail",
              s10 == 200 and r10.get("ok") and cnt_blocked == 2
              and s10b == 200 and t and t["progress_count"] == 1
              and t["blocked_count"] == 1 and not t["trail"][-1]["blocked"],
              f"send:{s10} cnt:{cnt_blocked} prog:{s10b} " + json.dumps(t)[:200])
        _ = blocked_ids

        # A11 /web HTML 关键元素 + 与 /v1/tasks 词表一致性
        s, html = raw("/web")
        elems = ['id="p-tasks"', 'data-p="tasks"', 'id="lkd-btn"', 'toggleLockdown()',
                 'id="tk-corr"', 'id="tk-state"', 'id="tk-list"', '熔断', '/v1/lockdown',
                 '/v1/tasks', 'blocked_count']
        miss = [x for x in elems if x not in html]
        topics = {x["topic"] for t0 in tasks()[1].get("tasks", []) for x in t0.get("trail", [])}
        miss_t = [tp for tp in topics if tp not in html]
        check("A11 /web HTML 200 且任务页签/熔断按钮/关键 JS 元素齐全",
              s == 200 and not miss, f"missing={miss}")
        check("A12 HTML 内嵌 topic 词表覆盖 /v1/tasks 实际出现的全部 topic",
              not miss_t and topics == {"task_spec", "task_accepted", "task_progress"},
              f"topics={topics} missing={miss_t}")
    finally:
        node.terminate(); node.wait(timeout=5)

    n = sum(1 for _, ok in results if ok)
    print(f"\n== {n}/{len(results)} PASS ==")
    sys.exit(0 if n == len(results) else 1)

if __name__ == "__main__":
    main()
