"""executor — 任务协议 v1 参考执行者。注册为 WS 客户端 agent，收 task_spec 按状态机回报。

模拟语义：
- 能力门 requires → 缺能力 task_declined
- 预检 sha：params.expect_sha != params.actual_sha → task_declined(sha_mismatch)
- 正常：task_accepted → task_progress×stages → task_succeeded
- reconcile.mode=maintain：按 interval_s 周期心跳，共 maintain_beats 拍（模拟失联=调小拍数）
- task_cancel：运行中置 stop 标志 → task_cancelled
- 兼容层：旧 upgrade_notice 帧视同 rollout.node_version spec，回 upgrade_ready+task_accepted
用法：python executor.py [--base URL] [--token T] [--agent worker]
"""
import json, time, threading, sys
from tasklib import Bus, msg

AGENT_CAPS = ["cap:file", "cap:shell:safe"]
STOP = {}            # task_id -> True 表示取消
SEEN = set()         # (task_id, seq) 幂等
bus = None

def reply(to_agent, m, reply_to=None):
    bus.send(to_agent, m, reply_to=reply_to)

def run_task(env):
    p = env.get("payload") or {}
    tid = p["task_id"]; origin = env.get("from_agent") or "orchestrator"
    params = dict(p.get("params") or {})
    rec = (p.get("reconcile") or {})
    mode = rec.get("mode", "one_shot")
    # ---- 预检 ----
    for cap in p.get("requires") or []:
        if cap not in AGENT_CAPS:
            reply(origin, msg("task_declined", tid, reason=f"missing cap:{cap}")); return
    if params.get("expect_sha") and params.get("actual_sha") and \
       params["expect_sha"] != params["actual_sha"]:
        reply(origin, msg("task_declined", tid, reason="sha_mismatch")); return
    reply(origin, msg("task_accepted", tid))
    # ---- 执行 ----
    stage_sleep = float(params.get("stage_sleep", 0.4))
    if mode == "maintain":
        interval = float(rec.get("interval_s", 1.0))
        beats = int(params.get("maintain_beats", 2))
        for i in range(beats):
            if STOP.get(tid): break
            reply(origin, msg("task_progress", tid, seq=i+1,
                              state="in_desired_state", uptime_s=int((i+1)*interval)))
            time.sleep(interval)
        if not STOP.get(tid):
            reply(origin, msg("task_progress", tid, seq=beats+1, state="maintainer_gone"))
        return
    stages = ["preflight", "apply", "verify"]
    for i, s in enumerate(stages):
        if STOP.get(tid):
            reply(origin, msg("task_cancelled", tid, stage=s)); return
        time.sleep(stage_sleep)
        reply(origin, msg("task_progress", tid, seq=i+1, stage=s))
    reply(origin, msg("task_succeeded", tid,
                      result={"version": params.get("target_version", "n/a"),
                              "backup": "mock-backup", "elapsed_ms": int(3*stage_sleep*1000)}))

def legacy_upgrade_notice(env):
    """upgrade_notice → rollout spec 兼容转换；回 upgrade_ready（并双发 task_accepted）。"""
    p = env.get("payload") or {}; origin = env.get("from_agent") or "orchestrator"
    tid = p.get("task_id") or ("legacy-" + (env.get("msg_id") or "")[:8])
    if p.get("sha256") and p.get("actual_sha") and p["sha256"] != p["actual_sha"]:
        reply(origin, msg("upgrade_declined", tid, reason="sha_mismatch")); return
    fake = dict(p); fake["task_id"] = tid; fake["topic"] = "task_accepted"
    reply(origin, msg("upgrade_ready", tid))
    reply(origin, fake)

def main():
    global bus
    args = sys.argv[1:]
    def flag(name, default):
        return args[args.index(name)+1] if name in args else default
    bus = Bus(flag("--base", "http://127.0.0.1:18765"),
              flag("--token", "testtoken123"), flag("--agent", "worker"))
    bus.register(bus.agent, AGENT_CAPS)
    import websocket
    ws = websocket.create_connection(bus.ws_url(), timeout=1.0)
    print(f"[executor] {bus.agent}@{bus.node} caps={AGENT_CAPS}", flush=True)
    while True:
        try: raw = ws.recv()
        except Exception as e:
            if "timed out" in str(e).lower() or "timeout" in type(e).__name__.lower(): continue
            raise
        if not raw: continue
        try: env = json.loads(raw)
        except Exception: continue
        if env.get("type") not in ("text", "file"): continue
        p = env.get("payload") or {}
        topic = p.get("topic") or ""
        if topic in ("task_cancel", "task_rollback"):
            STOP[p.get("task_id")] = True
            continue
        key = (p.get("task_id"), p.get("seq", 1))
        if topic == "task_spec":
            if key in SEEN:  # 业务级幂等（总线帧级去重之外的双保险）
                reply(env.get("from_agent") or "orchestrator",
                      msg("task_ack", key[0], note="dup-dropped")); continue
            SEEN.add(key)
            threading.Thread(target=run_task, args=(env,), daemon=True).start()
        elif topic == "upgrade_notice":
            legacy_upgrade_notice(env)

if __name__ == "__main__":
    main()
