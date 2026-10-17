"""smoke — 任务协议 v1 端到端场景验证（对隔离测试总线运行，绝不指向生产）。

场景：happy / capability-decline / sha-decline / 业务级幂等 / cancel / maintain-drift /
金丝雀闸口 / 旧 upgrade_notice 兼容 / /v1/tasks 面板 / 脱敏面结构键匹配 /
事件路径 P50 计时 / 无 events 特性回退 history / events 409 环旋转降级。
后两个兼容场景用本地 stub 总线（stdlib http.server），不依赖真节点。
结果逐条 JSONL 存档。
用法：venv python smoke.py（先起 executor.py；地址经 env TASKPROTO_BASE/TOKEN 覆盖）
"""
import json, time, threading, sys, os, uuid, statistics
from http.server import BaseHTTPRequestHandler, HTTPServer
from tasklib import Bus, make_spec, msg, task_events, blob
from taskctl import register_orchestrator, wait, submit, cancel, panel, CANARY_BLOCK

BASE = os.environ.get("TASKPROTO_BASE", "http://127.0.0.1:18765")
TOKEN = os.environ.get("TASKPROTO_TOKEN", "testtoken123")
EVIDENCE = []

def record(name, ok, detail):
    EVIDENCE.append({"scenario": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + " | " + json.dumps(detail, ensure_ascii=False)[:200], flush=True)

def topics_for(bus, tid):
    return [(m.get("payload") or {}).get("topic") for m in task_events(bus.history(400), tid, to_agent="orchestrator")]

def make_stub(responder):
    """本地 stub 总线（127.0.0.1 临时端口）：responder(method, path, body) -> (code, dict)。
    用于验证旧节点回退与 409 降级——真节点难以按需触发环旋转。"""
    hits = {"events": 0, "history": 0, "health": 0}
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a): pass
        def _go(self, body):
            import urllib.parse as up
            u = up.urlparse(self.path)
            code, out = responder(u.path, up.parse_qs(u.query), body, hits)
            raw = json.dumps(out).encode()
            self.send_response(code); self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw))); self.end_headers(); self.wfile.write(raw)
        def do_GET(self):  self._go(None)
        def do_POST(self): self._go(json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0)))))
    srv = HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}", hits

def stub_task_frames(tid, topics=("task_accepted", "task_succeeded")):
    return [{"ts": "2026-10-17T00:00:00Z", "msg_id": uuid.uuid4().hex, "node": "stub",
             "direction": "in", "from": "stub", "from_agent": "worker", "to": "stub",
             "to_agent": "orchestrator", "kind": "text", "action": "send", "seq": 0,
             "payload": {"topic": t, "task_id": tid}} for t in topics]

def stub_health(features):
    return {"ok": True, "node_id": "aabbccddeeff", "features": features}

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

    # 9 任务面板：/v1/tasks?correlation= 投影（批次 batchSmoke 含 declined+succeeded，均终态）
    tasks = panel(b, correlation="batchSmoke")
    states = sorted(t.get("state") for t in tasks)
    record("tasks_panel", len(tasks) >= 2 and all(t.get("terminal") for t in tasks)
           and "succeeded" in states and "declined" in states,
           {"correlation": "batchSmoke", "n": len(tasks), "states": states})

    # 10 脱敏面结构键匹配：spec 带 64 位 hex sha（redact 的 hex 规则命中值文本），
    #    events 视图必须已脱敏为 <REDACTED:hex>，但仍能按 payload.task_id
    #    结构键收敛（uuid 段短 hex 不命中规则，键名与 task_id 原文存活）
    sha64 = "0123456789abcdef" * 4
    st, info = submit(b, "rollout.node_version",
                      {"stage_sleep": 0.0, "sha256": sha64, "actual_sha": sha64}, wait_s=12)
    tid = info.get("task_id")
    ev_resp = b.events(since=0, agent="orchestrator", types=["text"], limit=2000)
    spec_frames = [e["frame"] for e in ev_resp.get("events", [])
                   if (e["frame"].get("payload") or {}).get("task_id") == tid
                   and (e["frame"].get("payload") or {}).get("topic") == "task_spec"]
    term_frames = [e["frame"] for e in ev_resp.get("events", [])
                   if (e["frame"].get("payload") or {}).get("task_id") == tid
                   and (e["frame"].get("payload") or {}).get("topic") == "task_succeeded"]
    spec_blob = blob(spec_frames[0]) if spec_frames else ""
    record("redaction_struct_key_match",
           st == "task_succeeded" and len(term_frames) == 1 and "<REDACTED" in spec_blob
           and sha64 not in spec_blob,
           {"state": st, "spec_frames": len(spec_frames), "term_frames": len(term_frames),
            "redacted": "<REDACTED" in spec_blob, "secret_leaked": sha64 in spec_blob})

    # 11 事件驱动计时证明：wait() 走 /v1/events，发令→返回 P50 < 500ms
    lat = []; transports = []
    for _ in range(9):
        spec = make_spec("rollout.node_version", {"stage_sleep": 0.0},
                         task_id=str(uuid.uuid4()))
        t0 = time.perf_counter()
        b.send("worker", spec)
        st, _trail = wait(b, spec["task_id"], 10)
        lat.append((time.perf_counter() - t0) * 1000.0)
        transports.append(getattr(b, "last_wait_transport", "?"))
        if st != "task_succeeded": break
    p50 = statistics.median(lat)
    all_events = st == "task_succeeded" and set(transports) <= {"events"}
    record("events_path_p50", all_events and p50 < 500 and len(lat) == 9,
           {"transport": sorted(set(transports)), "n": len(lat),
            "p50_ms": round(p50, 1), "min_ms": round(min(lat), 1), "max_ms": round(max(lat), 1),
            "evidence": "wait() 传输=events（next_seq 续传，未触发 history 轮询）" if all_events
                        else f"transport={transports}"})

    # 12 兼容回退：features 无 events 的旧节点 → wait() 静默走 /v1/history 轮询
    def legacy_resp(path, q, body, hits):
        if path == "/v1/health": return 200, stub_health(["redact"])   # 0.4.x：无 features 中的 events
        if path.startswith("/v1/events"): hits["events"] += 1; return 404, {"ok": False, "error": "not found"}
        if path.startswith("/v1/history"): hits["history"] += 1; return 200, {"ok": True, "history": stub_task_frames("compat-1")}
        return 200, {"ok": True}
    srv2, url2, hits2 = make_stub(legacy_resp)
    ob = Bus(url2, "x", "orchestrator")
    st, trail = wait(ob, "compat-1", timeout=5)
    record("fallback_history_polling",
           st == "task_succeeded" and hits2["events"] == 0
           and hits2["history"] >= 1 and ob.last_wait_transport == "history",
           {"state": st, "transport": ob.last_wait_transport, "events_hits": hits2["events"],
            "history_hits": hits2["history"], "trail_len": len(trail)})
    srv2.shutdown()

    # 13 环旋转降级：/v1/events 回 409（since_before_history）→ history 全量重建一次后续传
    state2 = {"calls": 0}
    def rotate_resp(path, q, body, hits):
        if path == "/v1/health": return 200, stub_health(["events", "tasks", "redact"])
        if path.startswith("/v1/events"):
            hits["events"] += 1; state2["calls"] += 1
            if state2["calls"] == 1:
                return 200, {"ok": True, "events": [], "next_seq": 50, "oldest_seq": 50}
            if state2["calls"] == 2:
                return 409, {"ok": False, "error": "since_before_history",
                             "oldest_seq": 60, "next_seq": 70}
            return 200, {"ok": True, "events": [], "next_seq": 70, "oldest_seq": 60}
        if path.startswith("/v1/history"):
            hits["history"] += 1; return 200, {"ok": True, "history": stub_task_frames("rot-1")}
        return 200, {"ok": True}
    srv3, url3, hits3 = make_stub(rotate_resp)
    ob2 = Bus(url3, "x", "orchestrator")
    st, trail = wait(ob2, "rot-1", timeout=5)
    record("events_409_history_rebuild",
           st == "task_succeeded" and hits3["history"] == 1  # 恰一次全量重建
           and ob2.last_wait_transport == "events+rebuild",
           {"state": st, "transport": ob2.last_wait_transport,
            "events_hits": hits3["events"], "rebuild_hits": hits3["history"]})
    srv3.shutdown()

    n_pass = sum(1 for e in EVIDENCE if e["pass"])
    print(f"\nSMOKE_RESULT {n_pass}/{len(EVIDENCE)} passed")
    return n_pass == len(EVIDENCE)

if __name__ == "__main__":
    ok = main()
    json.dump(EVIDENCE, open(sys.argv[-1] if sys.argv[-1].endswith(".json") else "smoke-evidence.json", "w"),
              ensure_ascii=False, indent=1)
    sys.exit(0 if ok else 1)
