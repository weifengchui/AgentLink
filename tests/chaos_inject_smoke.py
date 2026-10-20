# -*- coding: utf-8 -*-
"""chaos_inject_smoke — v0.7 §6.4 第二波：latency / disk_full 故障注入。

单节点 S（18967，mesh + license）：
  - mesh.chaos.enabled=true, max_duration=10s, max_latency=2000ms
  - security.audit=true
  - auth.tokens: chaos-op=operator（验证治理面 403）

场景：
  C0 初始 GET：enabled、active 为空
  C1 latency 400ms/3s：/v1/send 实测被延迟，GET 显示剩余时间
  C2 到期自动失效：等待后再发恢复 <250ms
  C3 disk_full 3s：提交任务 500 且不落表；到期后提交 202
  C4 参数/权限：非法 kind/duration/latency 400；Operator 403
  C5 DELETE 提前解除：1s 延迟立即停止
  C6 审计：chaos.start / chaos.stop 入不可变链
  C7 进程存活、日志无 panic
"""
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "tests"))
import engine_store_smoke as es  # noqa: E402

BIN = os.environ.get(
    "AL_BIN", os.path.expanduser("~/agentlink-target/release/agentlink"))
ROOT = os.path.expanduser(
    "~/.hermes/profiles/agentlink/cache/scratch/chaos-sim")
PORT = 18967
NODE = "5f0000000001"
ADMIN = "chaos-admin"
OP = "chaos-op"
EVIDENCE = []


def record(name, ok, detail=""):
    EVIDENCE.append({"scenario": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + " | "
          + json.dumps(detail, ensure_ascii=False)[:420], flush=True)


def api(path, body=None, method=None, token=ADMIN, timeout=15):
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    m = method or ("POST" if body is not None else "GET")
    req = urllib.request.Request(
        f"http://127.0.0.1:{PORT}{path}", method=m, data=data,
        headers={"X-Auth-Token": token, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:
            return e.code, {}
    except Exception as e:
        return 0, {"error": str(e)[:160]}


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
        if "chaos-sim" in cmd and "agentlink" in cmd:
            try:
                os.kill(int(pid), signal.SIGKILL)
            except Exception:
                pass


def write_cfg():
    cfg = {
        "v": 1, "node_id": NODE, "name": "chaos-s", "listen_port": PORT,
        "data_dir": os.path.join(ROOT, f"data{PORT}"),
        "history": {"enabled": True, "len": 500, "file": "history.jsonl"},
        "discovery": {"enabled": False, "static_peers": []},
        "security": {"mode": "strict", "cluster_token": "chaos-cluster",
                     "audit": {"enabled": True}},
        "local_api": {"host": "127.0.0.1", "token": ADMIN},
        "auth": {"tokens": {OP: "operator"}},
        "delivery": {"ack_timeout_seconds": 5, "max_retries": 2,
                     "offline_ttl_seconds": 300, "dedup_cache_size": 2048,
                     "persist": False},
        "log": {"level": "info", "format": "text", "file": ""},
        "mesh": {
            "enabled": True, "license_file": "license.toml",
            "chaos": {"enabled": True, "max_duration_seconds": 10,
                      "max_latency_ms": 2000},
            "engine": {"max_retries": 0, "backoff_secs": 1, "tick_ms": 200,
                       "max_concurrent_per_node": 4},
        },
    }
    path = os.path.join(ROOT, "s.json")
    json.dump(cfg, open(path, "w"))
    return path


def wait_for(fn, timeout_s=25, step=0.1):
    end = time.monotonic() + timeout_s
    while time.monotonic() < end:
        v = fn()
        if v:
            return v
        time.sleep(step)
    return fn()


def send_once():
    t0 = time.monotonic()
    st, _ = api("/v1/send", {
        "from": NODE, "to": NODE,
        "from_agent": "chaos-a", "to_agent": "chaos-a",
        "type": "text", "action": "send",
        "payload": {"topic": "chaos.ping", "text": "x"}}, timeout=10)
    return st, (time.monotonic() - t0) * 1000.0


def active_kinds():
    _, d = api("/v1/mesh/chaos")
    return {a.get("kind"): a for a in d.get("active") or []}, d


def submit(tid):
    return api("/v1/tasks", {
        "task_id": tid, "type": "chaos.echo", "priority": 100,
        "requires": ["cap:never"], "params": {}})


def task_state(tid):
    st, d = api(f"/v1/tasks/{tid}")
    return st, (d.get("task") or {}).get("state")


def main():
    kill_stale()
    time.sleep(0.5)
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(ROOT, exist_ok=True)
    lic = es.sign_license_text({
        "tier": "starter", "node_quota": 15,
        "issued": "2026-10-01T00:00:00Z",
        "expires": "2036-12-31T23:59:59Z"}, "chaos inject sim")
    open(os.path.join(ROOT, "license.toml"), "w").write(lic)
    assert not probe(PORT), f"port {PORT} busy（生产隔离铁律）"
    proc = None
    try:
        proc = subprocess.Popen(
            [BIN, "run", "--config", write_cfg(), "--no-open"],
            stdout=open(os.path.join(ROOT, "s.log"), "a"),
            stderr=subprocess.STDOUT)
        assert wait_for(lambda: probe(PORT), 25), "S 未启动"
        st, _ = api("/v1/agents", {
            "agent_id": "chaos-a", "name": "chaos-a",
            "capabilities": ["text"]})
        assert st == 200, f"agent register {st}"

        # ---- C0 初始态 ------------------------------------------------------
        active0, view0 = active_kinds()
        record("C0 初始：chaos enabled、上限生效、active 为空",
               view0.get("enabled") is True
               and view0.get("max_duration_seconds") == 10
               and view0.get("max_latency_ms") == 2000 and not active0,
               {"view": {k: view0.get(k) for k in
                         ("enabled", "max_duration_seconds", "max_latency_ms")},
                "active": active0})

        # ---- C1 latency ------------------------------------------------------
        st, d = api("/v1/mesh/chaos", {
            "kind": "latency", "duration_seconds": 3, "latency_ms": 400})
        active1, _ = active_kinds()
        s1, ms1 = send_once()
        s2, ms2 = send_once()
        record("C1 latency 400ms：注入成功、/v1/send 实测被延迟",
               st == 200 and "latency" in active1
               and active1["latency"].get("latency_ms") == 400
               and s1 == 200 and s2 == 200
               and ms1 >= 300 and ms2 >= 300,
               {"start": st, "ms": [round(ms1), round(ms2)],
                "active": active1.get("latency")})

        # ---- C2 自动过期 -----------------------------------------------------
        time.sleep(2.6)
        active2, _ = active_kinds()
        s3, ms3 = send_once()
        record("C2 TTL 到期自动失效：active 清空、发送恢复",
               not active2 and s3 == 200 and ms3 < 250,
               {"active": active2, "ms": round(ms3)})

        # ---- C3 disk_full ----------------------------------------------------
        st, d = api("/v1/mesh/chaos", {
            "kind": "disk_full", "duration_seconds": 3})
        active3, _ = active_kinds()
        st_fail, body_fail = submit("chaos-disk-1")
        st_get, _ = task_state("chaos-disk-1")
        time.sleep(3.3)
        st_ok, _ = submit("chaos-disk-2")
        st_ok_get, state_ok = task_state("chaos-disk-2")
        record("C3 disk_full：提交 500 且不落表；到期后 202 并可查",
               st == 200 and "disk_full" in active3
               and st_fail == 500 and st_get == 404
               and st_ok == 202 and st_ok_get == 200 and state_ok == "pending",
               {"start": st, "active": active3.get("disk_full"),
                "during": st_fail, "body": body_fail.get("error"),
                "get_during": st_get, "after": st_ok,
                "after_get": st_ok_get, "state": state_ok})

        # ---- C4 参数/权限 ----------------------------------------------------
        st_kind, _ = api("/v1/mesh/chaos", {"kind": "explode",
                                           "duration_seconds": 1})
        st_dur, _ = api("/v1/mesh/chaos", {"kind": "latency",
                                          "duration_seconds": 999,
                                          "latency_ms": 100})
        st_lat, _ = api("/v1/mesh/chaos", {"kind": "latency",
                                          "duration_seconds": 1,
                                          "latency_ms": 99999})
        st_op_get, _ = api("/v1/mesh/chaos", token=OP)
        st_op_post, _ = api("/v1/mesh/chaos", {"kind": "latency",
                                              "duration_seconds": 1,
                                              "latency_ms": 10}, token=OP)
        record("C4 非法 kind/duration/latency 400；Operator 403",
               st_kind == 400 and st_dur == 400 and st_lat == 400
               and st_op_get == 403 and st_op_post == 403,
               {"kind": st_kind, "duration": st_dur, "latency": st_lat,
                "op_get": st_op_get, "op_post": st_op_post})

        # ---- C5 DELETE 提前解除 ----------------------------------------------
        st, _ = api("/v1/mesh/chaos", {
            "kind": "latency", "duration_seconds": 8, "latency_ms": 1000})
        active5a, _ = active_kinds()
        st_del, _ = api("/v1/mesh/chaos", method="DELETE")
        active5b, _ = active_kinds()
        s5, ms5 = send_once()
        record("C5 DELETE 提前解除：active 清空、发送立即恢复",
               st == 200 and "latency" in active5a and st_del == 200
               and not active5b and s5 == 200 and ms5 < 250,
               {"start": st, "del": st_del, "active_before": list(active5a),
                "active_after": active5b, "ms": round(ms5)})

        # ---- C6 审计 ---------------------------------------------------------
        _, ex = api("/v1/audit/export?limit=100")
        events = [r.get("event") for r in (ex.get("records") or [])]
        _, ver = api("/v1/audit/verify")
        record("C6 chaos.start / chaos.stop 入审计链且链有效",
               events.count("chaos.start") == 3
               and events.count("chaos.stop") == 1
               and ver.get("valid") is True,
               {"start": events.count("chaos.start"),
                "stop": events.count("chaos.stop"),
                "valid": ver.get("valid"), "count": ver.get("count")})

        # ---- C7 存活/panic ---------------------------------------------------
        alive = proc.poll() is None
        log = open(os.path.join(ROOT, "s.log")).read().lower()
        record("C7 进程存活、日志无 panic", alive and "panic" not in log,
               {"alive": alive, "panic": "panic" in log})

    finally:
        if proc:
            try:
                proc.send_signal(signal.SIGINT)
                proc.wait(timeout=6)
            except Exception:
                proc.kill()
        with open(os.path.join(REPO, "tests", "chaos_inject_smoke.jsonl"), "w") as f:
            for e in EVIDENCE:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        npass = sum(1 for e in EVIDENCE if e["pass"])
        print(f"chaos_inject_smoke: {npass}/{len(EVIDENCE)} "
              f"→ {'OK' if npass == len(EVIDENCE) else 'FAILED'}", flush=True)
    sys.exit(0 if all(e["pass"] for e in EVIDENCE) else 1)


if __name__ == "__main__":
    main()
