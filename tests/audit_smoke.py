# -*- coding: utf-8 -*-
"""audit_smoke — v0.7 §4.4 不可变审计日志（hash 链）+ 导出。

拓扑：单节点（S，mesh+audit enabled）+ 单节点（对照，audit 缺省关闭）。

场景：
  S0 初始空链可校验（enabled=true, valid=true, count=0）
  S1 写面操作全部入链：task.submit / task.cancel / task.delete /
     lockdown.set ×2 / config.update
  S2 控制面拒绝入链：/peer/ws 发高版本 hello → handshake_rejected
  S3 /v1/audit/verify 链有效、count 与 head 自洽
  S4 /v1/audit/export 限长导出、顺序与事件匹配
  S5 停机篡改一行 → 重启后 verify 报 invalid 且追加 fail-closed
  S6 未启用节点返回 enabled=false（零文件/零记录）
  S7 进程存活、无 panic

端口 18945-18946（隔离，绝不指向生产 8765）。
"""
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import urllib.request

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "tests"))
import engine_store_smoke as es  # noqa: E402
import websocket                 # noqa: E402

BIN = os.environ.get("AL_BIN",
                     os.path.expanduser("~/agentlink-target/release/agentlink"))
ROOT = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch/al-audit")
TOKEN = "audit-sim-token"
CLUSTER = "audit-cluster"
PORT = 18945
PORT_OFF = 18946
NODE_ID = "520000000001"
NODE_OFF = "520000000002"
EVIDENCE = []


def record(name, ok, detail=""):
    EVIDENCE.append({"scenario": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + " | "
          + json.dumps(detail, ensure_ascii=False)[:300], flush=True)


def api(port, path, body=None, timeout=10):
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", method="POST" if body is not None else "GET",
        data=data, headers={"X-Auth-Token": TOKEN, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:
            return e.code, {}
    except Exception as e:
        return 0, {"error": str(e)[:150]}


def put(port, path, body, timeout=10):
    data = json.dumps(body, ensure_ascii=False).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", method="PUT", data=data,
        headers={"X-Auth-Token": TOKEN, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:
            return e.code, {}
    except Exception as e:
        return 0, {"error": str(e)[:150]}


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
        if "al-audit" in cmd and "agentlink" in cmd:
            try:
                os.kill(int(pid), signal.SIGKILL)
            except Exception:
                pass


def write_cfg(name, port, node_id, *, audit=False):
    cfg = {
        "v": 1, "node_id": node_id, "name": name, "listen_port": port,
        "data_dir": os.path.join(ROOT, f"data{port}"),
        "history": {"enabled": True, "len": 400, "file": "history.jsonl"},
        "discovery": {"enabled": False},
        "security": {"mode": "strict", "cluster_token": CLUSTER,
                     "audit": {"enabled": audit}},
        "local_api": {"host": "127.0.0.1", "token": TOKEN},
        "delivery": {"ack_timeout_seconds": 5, "max_retries": 5,
                     "offline_ttl_seconds": 600, "dedup_cache_size": 4096,
                     "persist": False},
        "log": {"level": "info", "format": "text", "file": ""},
        "mesh": {"enabled": True, "license_file": "license.toml",
                 "engine": {"max_retries": 0, "backoff_secs": 30, "tick_ms": 200,
                            "max_concurrent_per_node": 2}},
    }
    path = os.path.join(ROOT, f"{name}.json")
    json.dump(cfg, open(path, "w"))
    return path


def wait_for(fn, timeout_s=20, step=0.25):
    end = time.monotonic() + timeout_s
    while time.monotonic() < end:
        v = fn()
        if v:
            return v
        time.sleep(step)
    return fn()


def bad_hello(port):
    """发一帧 v=99 的畸形（高版本）hello，触发 handshake_rejected 审计。"""
    try:
        ws = websocket.create_connection(
            f"ws://127.0.0.1:{port}/peer/ws", timeout=5)
        ws.send(json.dumps({
            "type": "hello", "action": "hello", "v": 99,
            "node_id": "deadbeefcafe", "name": "intruder", "nonce": "x",
            "auth": "bogus"}))
        time.sleep(0.3)
        ws.close()
        return True
    except Exception as e:
        print("bad_hello error:", e, flush=True)
        return False


def main():
    kill_stale()
    time.sleep(0.5)
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(ROOT, exist_ok=True)
    lic = es.sign_license_text({
        "tier": "starter", "node_quota": 15,
        "issued": "2026-10-01T00:00:00Z",
        "expires": "2036-12-31T23:59:59Z"}, "audit sim")
    open(os.path.join(ROOT, "license.toml"), "w").write(lic)
    for p in (PORT, PORT_OFF):
        assert not probe(p), f"port {p} busy（生产隔离铁律）"

    procs = []
    try:
        cfg = write_cfg("main", PORT, NODE_ID, audit=True)
        procs.append(subprocess.Popen(
            [BIN, "run", "--config", cfg, "--no-open"],
            stdout=open(os.path.join(ROOT, "main.log"), "a"),
            stderr=subprocess.STDOUT))
        assert wait_for(lambda: probe(PORT), 25), "主节点未启动"
        cfg_off = write_cfg("off", PORT_OFF, NODE_OFF)
        procs.append(subprocess.Popen(
            [BIN, "run", "--config", cfg_off, "--no-open"],
            stdout=open(os.path.join(ROOT, "off.log"), "a"),
            stderr=subprocess.STDOUT))
        assert wait_for(lambda: probe(PORT_OFF), 25), "对照节点未启动"
        time.sleep(1.5)

        # ---- S0 初始空链 --------------------------------------------------
        _, v0 = api(PORT, "/v1/audit/verify")
        record("S0 初始空链有效（enabled=true/valid=true/count=0）",
               v0.get("enabled") is True and v0.get("valid") is True
               and v0.get("count") == 0,
               {k: v0.get(k) for k in ("enabled", "valid", "count", "head")})

        # ---- S1 写面操作入链 ----------------------------------------------
        st, _ = api(PORT, "/v1/tasks", {
            "task_id": "aud-1", "type": "audit.echo", "priority": 100,
            "requires": ["cap:never"], "params": {}})
        assert st == 202, f"submit: {st}"
        st, _ = api(PORT, "/v1/tasks/aud-1/cancel", {})
        assert st == 200, f"cancel: {st}"
        # delete 仅终态可删：另起一条 deadline 已过的任务，等引擎 expire。
        api(PORT, "/v1/tasks", {
            "task_id": "aud-del", "type": "audit.echo", "priority": 100,
            "requires": ["cap:never"], "deadline": "2000-01-01T00:00:00Z",
            "params": {}})

        def expired():
            _, t = api(PORT, "/v1/tasks/aud-del")
            st = (t.get("task") or t).get("state")
            return st if st == "expired" else None
        assert wait_for(expired, 15), "任务未按 deadline 过期"
        req = urllib.request.Request(
            f"http://127.0.0.1:{PORT}/v1/tasks/aud-del", method="DELETE",
            headers={"X-Auth-Token": TOKEN})
        with urllib.request.urlopen(req, timeout=10) as r:
            assert r.status == 200, f"delete: {r.status}"
        api(PORT, "/v1/lockdown", {"on": True})
        api(PORT, "/v1/lockdown", {"on": False})
        st, _ = put(PORT, "/v1/config", {"name": "audit-node"})
        assert st == 200, f"config: {st}"

        # ---- S2 控制面拒绝入链 --------------------------------------------
        hello_ok = bad_hello(PORT)
        time.sleep(0.5)
        _, v1 = api(PORT, "/v1/audit/verify")
        record("S1/S2 写面 + 控制面拒绝全部入链（≥8 条）",
               hello_ok and v1.get("valid") is True and v1.get("count", 0) >= 8,
               {"count": v1.get("count"), "valid": v1.get("valid"),
                "head": (v1.get("head") or "")[:16]})

        # ---- S3 链自洽 ------------------------------------------------------
        _, ex = api(PORT, "/v1/audit/export?limit=20")
        events = [r.get("event") for r in (ex.get("records") or [])]
        want = ["task.submit", "task.cancel", "task.delete", "lockdown.set",
                "config.update", "handshake_rejected"]
        ok3 = (v1.get("valid") is True and len(v1.get("head") or "") == 64
               and all(e in events for e in want))
        record("S3 verify 有效且导出含全部关键事件", ok3,
               {"count": v1.get("count"), "events": events})

        # ---- S4 导出限长 / 顺序 --------------------------------------------
        _, ex5 = api(PORT, "/v1/audit/export?limit=5")
        recs = ex5.get("records") or []
        ok4 = (len(recs) == 5 and ex5.get("count") == v1.get("count")
               and recs[-1].get("event") == "handshake_rejected")
        record("S4 export?limit=5 只回最近 5 条且末条为 handshake_rejected",
               ok4, {"returned": len(recs),
                     "last": (recs[-1] if recs else {}).get("event"),
                     "count": ex5.get("count")})

        # ---- S5 篡改 → 重启 fail-closed ------------------------------------
        for p in procs:
            p.send_signal(signal.SIGINT)
        for p in procs:
            try:
                p.wait(timeout=6)
            except Exception:
                p.kill()
        procs = []
        audit_path = os.path.join(ROOT, f"data{PORT}", "audit", "audit.jsonl")
        before = open(audit_path).read()
        tampered = before.replace('"actor":"admin"', '"actor":"root"', 1)
        assert tampered != before, "未找到可篡改字段"
        open(audit_path, "w").write(tampered)
        cfg = write_cfg("main", PORT, NODE_ID, audit=True)
        procs.append(subprocess.Popen(
            [BIN, "run", "--config", cfg, "--no-open"],
            stdout=open(os.path.join(ROOT, "main2.log"), "a"),
            stderr=subprocess.STDOUT))
        assert wait_for(lambda: probe(PORT), 25), "重启后节点未起"
        procs.append(subprocess.Popen(
            [BIN, "run", "--config", cfg_off, "--no-open"],
            stdout=open(os.path.join(ROOT, "off2.log"), "a"),
            stderr=subprocess.STDOUT))
        assert wait_for(lambda: probe(PORT_OFF), 25), "对照节点未重启"
        time.sleep(1.0)
        _, vt = api(PORT, "/v1/audit/verify")
        # 追加 fail-closed：新任务仍然可提交（可用性），审计计数不增长。
        st, _ = api(PORT, "/v1/tasks", {
            "task_id": "aud-2", "type": "audit.echo", "priority": 100,
            "requires": ["cap:never"], "params": {}})
        _, vt2 = api(PORT, "/v1/audit/verify")
        record("S5 篡改被 verify 发现，重启后拒绝追加（计数不增、服务可用）",
               vt.get("valid") is False and st == 202
               and vt2.get("count") == vt.get("count"),
               {"valid": vt.get("valid"), "error": vt.get("error"),
                "count": vt.get("count"), "submit_status": st})

        # ---- S6 未启用对照 --------------------------------------------------
        _, offv = api(PORT_OFF, "/v1/audit/verify")
        _, offe = api(PORT_OFF, "/v1/audit/export")
        off_file = os.path.join(ROOT, f"data{PORT_OFF}", "audit", "audit.jsonl")
        record("S6 未启用节点 enabled=false、无审计文件",
               offv.get("enabled") is False and offv.get("count") == 0
               and offe.get("enabled") is False and not os.path.exists(off_file),
               {"verify": offv, "export_enabled": offe.get("enabled"),
                "file_exists": os.path.exists(off_file)})

        # ---- S7 存活/panic ---------------------------------------------------
        alive = all(p.poll() is None for p in procs)
        panics = []
        for name in ("main", "main2", "off", "off2"):
            log = os.path.join(ROOT, f"{name}.log")
            if os.path.exists(log) and "panic" in open(log).read().lower():
                panics.append(name)
        record("S7 进程存活、日志无 panic", alive and not panics,
               {"alive": sum(1 for p in procs if p.poll() is None),
                "panics": panics})

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
        with open(os.path.join(REPO, "tests", "audit_smoke.jsonl"), "w") as f:
            for e in EVIDENCE:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        npass = sum(1 for e in EVIDENCE if e["pass"])
        print(f"audit_smoke: {npass}/{len(EVIDENCE)} "
              f"→ {'OK' if npass == len(EVIDENCE) else 'FAILED'}", flush=True)
    sys.exit(0 if all(e["pass"] for e in EVIDENCE) else 1)


if __name__ == "__main__":
    main()
