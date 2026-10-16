"""taskctl — 任务协议 v1 编排参考实现：提交 task_spec 并按回报 reconcile。

编排方义务（docs/task-protocol.md §5 护栏）的参考落地：
- 终态等待超时 → expired
- maintain 心跳缺勤 > drift_after → drift 告警（不伪造状态）
- 金丝雀闸口：canary(correlation) 在首任务 succeeded 前阻断后续提交
用法：python taskctl.py submit --type T --params JSON [--wait S]；cancel --task-id
"""
import json, time, argparse, sys
from tasklib import Bus, make_spec, msg, task_events

TERMINAL_OK = ("task_succeeded", "task_rolled_back", "task_cancelled")
TERMINAL_BAD = ("task_failed", "task_declined")
CANARY_BLOCK = {}  # correlation -> True 表示金丝雀未过关

def register_orchestrator(base, token):
    b = Bus(base, token, "orchestrator")
    b.register("orchestrator", [])
    return b

def wait(bus, task_id, timeout=15.0, reconcile=None):
    """轮询 history 收敛任务状态。返回 (state, trail)。"""
    interval = float((reconcile or {}).get("interval_s", 1.0))
    maintain = (reconcile or {}).get("mode") == "maintain"
    drift_after = 3 * interval if maintain else None
    deadline = time.time() + timeout
    last_progress = None      # 最后一次"看到新心跳"的墙钟时刻
    seen_progress = 0
    ev = []
    while time.time() < deadline:
        ev = task_events(bus.history(400), task_id, to_agent="orchestrator")
        state = "pending"; trail = []
        n_prog = sum(1 for m in ev if (m.get("payload") or {}).get("topic") == "task_progress")
        if n_prog > seen_progress:
            seen_progress = n_prog
            last_progress = time.time()
        for m in ev:
            t = (m.get("payload") or {}).get("topic")
            if t in ("task_accepted", "upgrade_ready"): state = "running"
            elif t == "task_progress": state = "running"
            elif t in TERMINAL_OK: state = t
            elif t in TERMINAL_BAD: state = t
        trail = [((m.get("payload") or {}).get("topic"), str(m.get("ts", ""))[11:19]) for m in ev]
        if state in TERMINAL_OK or state in TERMINAL_BAD:
            return state, trail
        if drift_after and last_progress and time.time() - last_progress > drift_after:
            return "drift", trail
        time.sleep(0.3)
    return "expired", []

def submit(bus, task_type, params, wait_s=15.0, requires=None, correlation=None,
           reconcile=None, intent=""):
    if correlation and CANARY_BLOCK.get(correlation):
        return "blocked-canary", {}
    spec = make_spec(task_type, params, intent=intent, requires=requires,
                     correlation=correlation, reconcile=reconcile)
    r = bus.send("worker", spec)
    assert r.get("ok") is not False, r
    state, trail = wait(bus, spec["task_id"], wait_s, reconcile)
    if correlation:
        CANARY_BLOCK[correlation] = state not in TERMINAL_OK
    return state, {"task_id": spec["task_id"], "trail": trail}

def cancel(bus, task_id):
    return bus.send("worker", msg("task_cancel", task_id))

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["submit", "cancel"])
    ap.add_argument("--base", default="http://127.0.0.1:18765")
    ap.add_argument("--token", default="testtoken123")
    ap.add_argument("--type", default="rollout.node_version")
    ap.add_argument("--params", default="{}")
    ap.add_argument("--wait", type=float, default=15.0)
    ap.add_argument("--task-id", default=None)
    a = ap.parse_args()
    b = register_orchestrator(a.base, a.token)
    if a.cmd == "submit":
        st, info = submit(b, a.type, json.loads(a.params), a.wait)
        print(json.dumps({"state": st, **info}, ensure_ascii=False))
    else:
        print(cancel(b, a.task_id))
