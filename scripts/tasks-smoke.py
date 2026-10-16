#!/usr/bin/env python3
"""tasks-smoke — v0.5 内核 §4 /v1/tasks 任务资源视图 端到端验证（隔离总线 18767，绝不指向生产）。

场景：
  S1  health.features 含 "tasks"
  S2  全生命周期 task_spec→accepted→progress×2→succeeded（correlation=batchX）投影 succeeded 终态
  S3  spec→declined 投影 declined 终态
  S4  spec→accepted→cancel（未收 cancelled）投影 cancelling 中间态；另有无 spec 的 accepted-only 任务
  S5  过滤：?correlation=batchX / ?state=succeeded,failed 多值 / ?task_id 精确 / ?agent / 倒序
  S6  非 task 消息（topic=probe 普通 text）不进入视图
  S7  空环（全新 data_dir 只发普通消息）tasks 为空数组且 ok=true

用法：python3 tasks-smoke.py <path-to-agentlink-binary>
退出码 0 = 全过。
"""
import json, os, shutil, subprocess, sys, time, urllib.request

BIN = sys.argv[1] if len(sys.argv) > 1 else None
assert BIN and os.path.exists(BIN), "usage: tasks-smoke.py <agentlink-binary>"
BASE = "http://127.0.0.1:18767"
TOKEN = "smoketoken"
SCRATCH = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch")
D1 = os.path.join(SCRATCH, "al-v05-tasks-testdata")
D2 = os.path.join(SCRATCH, "al-v05-tasks-testdata2")
CFG1 = os.path.join(SCRATCH, "al-v05-tasks-node.json")
CFG2 = os.path.join(SCRATCH, "al-v05-tasks-node-clean.json")

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

def write_cfg(path, data_dir):
    json.dump({
        "v": 1, "node_id": "fa15ee000707", "name": "v05-tasks-smoke",
        "listen_port": 18767, "data_dir": data_dir,
        "history": {"enabled": True, "len": 200, "file": "history.jsonl"},
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
            if api("/v1/health")[0] == 200:
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
    # 回报方向：执行方 worker → 发起方 orch
    if topic in ("task_accepted", "task_declined", "task_progress",
                 "task_succeeded", "task_failed", "task_cancelled", "task_rolled_back"):
        return send_msg(p, from_agent="worker", to_agent="orch")
    return send_msg(p)

def tasks(query=""):
    return api("/v1/tasks" + (f"?{query}" if query else ""))

def find(tasks_list, tid):
    for t in tasks_list:
        if t["task_id"] == tid:
            return t
    return None

def main():
    global NODE
    write_cfg(CFG1, D1)
    node = start_node(CFG1, wipe_dirs=(D1, D2))
    NODE = api("/v1/health")[1]["node_id"]
    try:
        # S1 health features
        s, h = api("/v1/health")
        feats = h.get("features", [])
        check("S1 health.features 含 tasks", "tasks" in feats and "events" in feats, str(feats))

        # 未鉴权必须 401
        req = urllib.request.Request(BASE + "/v1/tasks")
        try:
            code = urllib.request.urlopen(req, timeout=5).status
        except urllib.error.HTTPError as e:
            code = e.code
        check("S1b /v1/tasks 无 token 拒绝", code == 401, str(code))

        # S2 成功生命周期
        st, r = task("task_spec", "t-ok", type="deploy.demo", intent="冒烟成功链路",
                     correlation="batchX", deadline="2026-10-18T07:00:00+08:00",
                     priority=10, budget={"max_retries": 3})
        check("S2a task_spec 发送", st == 200 and r.get("ok"), json.dumps(r)[:120])
        task("task_accepted", "t-ok")
        task("task_progress", "t-ok", percent=50)
        task("task_progress", "t-ok", percent=90)
        task("task_succeeded", "t-ok", result={"elapsed_ms": 12})
        s2, tv = tasks()
        t = find(tv.get("tasks", []), "t-ok")
        check("S2b 投影 succeeded 终态",
              t and t["state"] == "succeeded" and t["terminal"] is True
              and t["correlation"] == "batchX" and t["type"] == "deploy.demo"
              and t["intent"] == "冒烟成功链路"
              and t["progress_count"] == 2 and t["message_count"] == 5
              and t["spec"] and t["spec"]["topic"] == "task_spec"
              and t["created_ts"] <= t["last_ts"], json.dumps(t)[:220])

        # S3 拒绝终态
        task("task_spec", "t-no", type="deploy.demo", intent="拒绝", correlation="batchX")
        task("task_declined", "t-no", reason="cap 缺失")
        s2, tv = tasks("task_id=t-no")
        t = find(tv.get("tasks", []), "t-no")
        check("S3 投影 declined 终态",
              t and t["state"] == "declined" and t["terminal"] and tv.get("total") == 1,
              json.dumps(t)[:160])

        # S4 cancelling 中间态 + 无 spec 任务
        task("task_spec", "t-cancel", type="deploy.demo", intent="撤销", correlation="batchX")
        task("task_accepted", "t-cancel")
        task("task_cancel", "t-cancel")  # 发起方撤销指令，未收 cancelled 确认
        task("task_accepted", "t-ghost")  # 环内没有它的 spec
        s2, tv = tasks()
        tc = find(tv.get("tasks", []), "t-cancel")
        tg = find(tv.get("tasks", []), "t-ghost")
        check("S4a 收 cancel 未收 cancelled → cancelling 非终态",
              tc and tc["state"] == "cancelling" and tc["terminal"] is False,
              json.dumps(tc)[:160])
        check("S4b 无 spec 任务也出现（state 按轨迹推，type/correlation 为 null）",
              tg and tg["state"] == "running" and tg["spec"] is None
              and tg["correlation"] is None and tg["type"] is None,
              json.dumps(tg)[:160])

        # S5 过滤与倒序（batchY 的 failed 任务）
        task("task_spec", "t-fail", type="deploy.demo", intent="失败", correlation="batchY")
        task("task_accepted", "t-fail")
        task("task_failed", "t-fail", error="boom")
        s2, tv = tasks()
        tf = find(tv.get("tasks", []), "t-fail")
        check("S5a failed 无后续 accepted → failed 终态",
              tf and tf["state"] == "failed" and tf["terminal"] is True,
              json.dumps(tf)[:160])
        seqs = [t["seq"] for t in tv["tasks"]]
        check("S5b 按 seq 倒序", seqs == sorted(seqs, reverse=True), str(seqs))
        s2, tvx = tasks("correlation=batchX")
        idx = sorted(t["task_id"] for t in tvx.get("tasks", []))
        check("S5c ?correlation=batchX 只命中 batchX（验收）",
              idx == ["t-cancel", "t-no", "t-ok"] and tvx.get("total") == 3,
              json.dumps({"ids": idx, "total": tvx.get("total")}))
        s2, tvs = tasks("state=succeeded,failed")
        idxs = sorted(t["task_id"] for t in tvs.get("tasks", []))
        check("S5d ?state=succeeded,failed 多值过滤", idxs == ["t-fail", "t-ok"], str(idxs))
        s2, tv1 = tasks("task_id=t-ok")
        check("S5e ?task_id 精确", s2 == 200 and tv1.get("total") == 1
              and tv1["tasks"][0]["state"] == "succeeded", json.dumps(tv1)[:160])
        s2, tva = tasks("agent=orch")
        check("S5f ?agent= 命中参与方（t-ghost 的 orch 也在，共 5）",
              sorted(t["task_id"] for t in tva.get("tasks", [])) ==
              ["t-cancel", "t-fail", "t-ghost", "t-no", "t-ok"],
              json.dumps([t["task_id"] for t in tva.get("tasks", [])]))
        s2, tvl = tasks("limit=2")
        check("S5g limit 页化 + total 仍为全匹配数",
              len(tvl["tasks"]) == 2 and tvl["total"] == 5,
              json.dumps({"n": len(tvl["tasks"]), "total": tvl["total"]}))

        # S6 非 task 消息不进视图
        s2, tv_b = tasks()
        st, r = send_msg({"topic": "probe", "content": "普通聊天消息"}, kind="text")
        check("S6a 普通 text 发送成功", st == 200 and r.get("ok"), json.dumps(r)[:120])
        s2, tv_c = tasks()
        check("S6b 普通消息不进任务视图", tv_b["total"] == tv_c["total"] == 5,
              json.dumps({"before": tv_b["total"], "after": tv_c["total"]}))
    finally:
        node.terminate(); node.wait(timeout=5)

    # S7 全新 data_dir：无 task 消息 → 空数组
    write_cfg(CFG2, D2)
    node = start_node(CFG2, wipe_dirs=(D2,))
    try:
        send_msg({"topic": "probe", "content": "只有普通消息"}, kind="text")
        s2, tv = tasks()
        check("S7 无 task 消息时 tasks 为空数组且 ok=true",
              s2 == 200 and tv.get("ok") is True and tv.get("tasks") == []
              and tv.get("total") == 0, json.dumps(tv)[:160])
    finally:
        node.terminate(); node.wait(timeout=5)

    n = sum(1 for _, ok in results if ok)
    print(f"\n== {n}/{len(results)} PASS ==")
    sys.exit(0 if n == len(results) else 1)

if __name__ == "__main__":
    main()
