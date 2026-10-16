"""smoke — 任务协议 v1 端到端场景验证（对隔离测试总线 18765 运行，绝不指向生产）。

场景：happy / capability-decline / sha-decline / 业务级幂等 / cancel / maintain-drift /
金丝雀闸口 / 旧 upgrade_notice 兼容。结果逐条 JSONL 存档。
用法：venv python smoke.py（先起 executor.py）
"""
import json, time, threading, sys
from tasklib import Bus, make_spec, msg, task_events
from taskctl import register_orchestrator, wait, submit, cancel, CANARY_BLOCK

BASE = "http://127.0.0.1:18765"; TOKEN = "testtoken123"
EVIDENCE = []

def record(name, ok, detail):
    EVIDENCE.append({"scenario": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + " | " + json.dumps(detail, ensure_ascii=False)[:200], flush=True)

def topics_for(bus, tid):
    return [(m.get("payload") or {}).get("topic") for m in task_events(bus.history(400), tid, to_agent="orchestrator")]

def main():
    b = register_orchestrator(BASE, TOKEN)

    # 1 happy
    st, info = submit(b, "rollout.node_version",
                      {"target_version": "0.5.0", "stage_sleep": 0.2}, wait_s=15)
    record("happy_path", st == "task_succeeded", {"state": st, "task_id": info.get("task_id")})

    # 2 capability gate
    st, info = submit(b, "rollout.node_version", {"target_version": "0.5.0"},
                      wait_s=10, requires=["cap:node:admin"])
    record("capability_decline", st == "task_declined", {"state": st, "task_id": info.get("task_id")})

    # 3 sha gate（升级协议的 SHA 硬条件泛化）
    st, info = submit(b, "rollout.node_version",
                      {"expect_sha": "aa", "actual_sha": "bb"}, wait_s=10)
    record("sha_decline", st == "task_declined", {"state": st})

    # 4 业务级幂等：同 task_id+seq 双发
    spec = make_spec("rollout.node_version", {"target_version": "9.9.9", "stage_sleep": 0.2},
                     task_id=str(spec_id := __import__("uuid").uuid4()))
    b.send("worker", spec); b.send("worker", spec)
    st, info = wait(b, spec["task_id"], 15)
    tp = topics_for(b, spec["task_id"])
    record("idempotent_double_send", st == "task_succeeded" and tp.count("task_accepted") == 1,
           {"state": st, "accepted_count": tp.count("task_accepted"), "acks": tp.count("task_ack")})

    # 5 cancel
    spec = make_spec("rollout.node_version", {"stage_sleep": 1.2})
    b.send("worker", spec)
    time.sleep(1.4); cancel(b, spec["task_id"])
    st, info = wait(b, spec["task_id"], 15)
    record("cancel", st == "task_cancelled", {"state": st})

    # 6 maintain drift（执行者心跳缺勤，编排方告警而非伪造状态）
    st, info = submit(b, "service.keepalive", {"maintain_beats": 2},
                      wait_s=12, reconcile={"mode": "maintain", "interval_s": 0.5})
    record("maintain_drift", st == "drift", {"state": st})

    # 7 金丝雀闸口：批次首任务被拒 → 后续阻断
    CANARY_BLOCK.clear()
    st1, _ = submit(b, "rollout.node_version", {}, wait_s=8,
                    requires=["cap:node:admin"], correlation="batchSmoke")
    st2, _ = submit(b, "rollout.node_version", {"stage_sleep": 0.1}, wait_s=8,
                    correlation="batchSmoke")
    record("canary_gate", st1 == "task_declined" and st2 == "blocked-canary",
           {"first": st1, "second": st2})
    CANARY_BLOCK.clear()
    st3, _ = submit(b, "rollout.node_version", {"stage_sleep": 0.1}, wait_s=8,
                    correlation="batchSmoke")
    record("canary_release_after_success", st3 == "task_succeeded", {"third": st3})

    # 8 旧 upgrade_notice 兼容层
    legacy = {"topic": "upgrade_notice", "old": "0.4.19", "new": "0.5.0",
              "sha256": "cc", "actual_sha": "cc", "ready_deadline_s": 120}
    b.send("worker", legacy)
    time.sleep(2.5)
    tid = None
    for m in b.history(200):
        p = m.get("payload") or {}
        if p.get("topic") == "upgrade_ready" and m.get("to_agent") == "orchestrator":
            tid = p.get("task_id")
    tp = topics_for(b, tid) if tid else []
    record("legacy_upgrade_compat", tid is not None and "upgrade_ready" in tp and "task_accepted" in tp,
           {"task_id": tid, "trail": tp})

    n_pass = sum(1 for e in EVIDENCE if e["pass"])
    print(f"\nSMOKE_RESULT {n_pass}/{len(EVIDENCE)} passed")
    return n_pass == len(EVIDENCE)

if __name__ == "__main__":
    ok = main()
    json.dump(EVIDENCE, open(sys.argv[-1] if sys.argv[-1].endswith(".json") else "smoke-evidence.json", "w"),
              ensure_ascii=False, indent=1)
    sys.exit(0 if ok else 1)
