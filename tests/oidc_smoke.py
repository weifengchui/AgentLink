# -*- coding: utf-8 -*-
"""oidc_smoke — v0.7 OIDC SSO 第一波（token introspection + 角色/namespace）。

拓扑：Python mock introspection（18965）+ 单节点 S（18966，mesh 开启）。
mock token：
  oidc-ops      role=ops, namespace=team-a -> role_map ops=operator
  oidc-admin    role=admin（全局）
  oidc-team     role=ops, namespace=team-a
  oidc-cache    role=ops, namespace=team-a（缓存计数）
  oidc-inactive active=false
  oidc-admin-ns role=admin, namespace=team-a（必须拒绝）
  oidc-slow     服务端 sleep 4s（客户端 timeout=2s）

场景：
  S0 静态 local/operator token 行为不变
  S1 OIDC operator 可读任务、治理面 403
  S2 OIDC admin 可读治理面
  S3 inactive/未知/伪造内部头 -> 401；有效 OIDC + 伪造 admin 仍是 operator
  S4 introspection 成功结果缓存 5s（同一 token 两次只打一次上游）
  S5 namespace claim：提交自动注入、跨租户 404、越界 403
  S6 Admin + namespace -> 401
  S7 introspection 超时 fail-closed（<4s 返回 401）
  S8 进程存活、日志无 panic
"""
import base64
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "tests"))
import engine_store_smoke as es  # noqa: E402

BIN = os.environ.get(
    "AL_BIN", os.path.expanduser("~/agentlink-target/release/agentlink"))
ROOT = os.path.expanduser(
    "~/.hermes/profiles/agentlink/cache/scratch/oidc-sim")
PORT_MOCK, PORT_S = 18965, 18966
NODE_S = "590000000001"
LOCAL_ADMIN = "oidc-local-admin"
BASIC = "Basic " + base64.b64encode(b"oidc-client:oidc-secret").decode()
EVIDENCE = []
COUNTS = {}
COUNTS_LOCK = threading.Lock()


def record(name, ok, detail=""):
    EVIDENCE.append({"scenario": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + " | "
          + json.dumps(detail, ensure_ascii=False)[:400], flush=True)


def api(port, path, body=None, method=None, token=None, bearer=False,
        extra_headers=None, timeout=10):
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    m = method or ("POST" if body is not None else "GET")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization" if bearer else "X-Auth-Token"] = (
            f"Bearer {token}" if bearer else token)
    headers.update(extra_headers or {})
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", method=m, data=data, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:
            return e.code, {}
    except Exception as e:
        return 0, {"error": str(e)[:200]}


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
        if "oidc-sim" in cmd and "agentlink" in cmd:
            try:
                os.kill(int(pid), signal.SIGKILL)
            except Exception:
                pass


def count_of(token):
    with COUNTS_LOCK:
        return COUNTS.get(token, 0)


class MockOidc(BaseHTTPRequestHandler):
    token_map = {
        "oidc-ops": {"active": True, "role": "ops", "namespace": "team-a"},
        "oidc-admin": {"active": True, "role": "admin"},
        "oidc-team": {"active": True, "role": "ops", "namespace": "team-a"},
        "oidc-cache": {"active": True, "role": "ops", "namespace": "team-a"},
        "oidc-admin-ns": {"active": True, "role": "admin", "namespace": "team-a"},
        "oidc-slow": {"active": True, "role": "ops"},
    }

    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if self.path != "/introspect":
            return self._json(404, {"error": "not found"})
        n = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(n).decode()
        form = urllib.parse.parse_qs(raw)
        token = (form.get("token") or [""])[0]
        with COUNTS_LOCK:
            COUNTS[token] = COUNTS.get(token, 0) + 1
        if self.headers.get("Authorization") != BASIC:
            return self._json(401, {"error": "bad client"})
        if token == "oidc-slow":
            time.sleep(4)
        return self._json(200, self.token_map.get(token, {"active": False}))

    def log_message(self, *args):
        pass


class MockThread(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.httpd = ThreadingHTTPServer(("127.0.0.1", PORT_MOCK), MockOidc)

    def run(self):
        self.httpd.serve_forever(poll_interval=0.1)

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()


def write_cfg():
    cfg = {
        "v": 1, "node_id": NODE_S, "name": "oidc-s", "listen_port": PORT_S,
        "data_dir": os.path.join(ROOT, f"data{PORT_S}"),
        "history": {"enabled": True, "len": 500, "file": "history.jsonl"},
        "discovery": {"enabled": False, "static_peers": []},
        "security": {"mode": "strict", "cluster_token": "oidc-cluster"},
        "local_api": {"host": "127.0.0.1", "token": LOCAL_ADMIN},
        "delivery": {"ack_timeout_seconds": 5, "max_retries": 2,
                     "offline_ttl_seconds": 300, "dedup_cache_size": 2048,
                     "persist": False},
        "log": {"level": "info", "format": "text", "file": ""},
        "auth": {
            "tokens": {"static-op": "operator"},
            "oidc": {
                "enabled": True,
                "introspection_url": f"http://127.0.0.1:{PORT_MOCK}/introspect",
                "client_id": "oidc-client",
                "client_secret": "oidc-secret",
                "role_claim": "role",
                "role_map": {"admin": "admin", "ops": "operator", "aud": "viewer"},
                "cache_seconds": 5,
                "timeout_seconds": 2,
                "namespace_claim": "namespace",
            },
        },
        "mesh": {"enabled": True, "license_file": "license.toml",
                 "engine": {"max_retries": 0, "backoff_secs": 1, "tick_ms": 200,
                            "max_concurrent_per_node": 4}},
    }
    path = os.path.join(ROOT, "s.json")
    json.dump(cfg, open(path, "w"))
    return path


def wait_for(fn, timeout_s=25, step=0.2):
    end = time.monotonic() + timeout_s
    while time.monotonic() < end:
        v = fn()
        if v:
            return v
        time.sleep(step)
    return fn()


def submit(tid, namespace=None):
    body = {"task_id": tid, "type": "oidc.echo", "priority": 100,
            "requires": ["cap:none"], "params": {}}
    if namespace is not None:
        body["namespace"] = namespace
    return api(PORT_S, "/v1/tasks", body, token=LOCAL_ADMIN)


def task_of(tid, token=None, bearer=True):
    st, d = api(PORT_S, f"/v1/tasks/{tid}", token=token or LOCAL_ADMIN,
                bearer=bearer)
    return st, (d.get("task") or {})


def main():
    kill_stale()
    time.sleep(0.5)
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(ROOT, exist_ok=True)
    lic = es.sign_license_text({
        "tier": "starter", "node_quota": 15,
        "issued": "2026-10-01T00:00:00Z",
        "expires": "2036-12-31T23:59:59Z"}, "oidc sim")
    open(os.path.join(ROOT, "license.toml"), "w").write(lic)
    for p in (PORT_MOCK, PORT_S):
        assert not probe(p), f"port {p} busy（生产隔离铁律）"

    mock = MockThread()
    mock.start()
    assert wait_for(lambda: probe(PORT_MOCK), 10), "mock OIDC 未启动"
    proc = None
    try:
        cfg = write_cfg()
        proc = subprocess.Popen(
            [BIN, "run", "--config", cfg, "--no-open"],
            stdout=open(os.path.join(ROOT, "s.log"), "a"),
            stderr=subprocess.STDOUT)
        assert wait_for(lambda: probe(PORT_S), 25), "S 未启动"

        # ---- S0 静态 token 零回归 ------------------------------------------
        st_local, _ = api(PORT_S, "/v1/status", token=LOCAL_ADMIN)
        st_op_tasks, _ = api(PORT_S, "/v1/tasks", token="static-op")
        st_op_status, _ = api(PORT_S, "/v1/mesh/status", token="static-op")
        st_none, _ = api(PORT_S, "/v1/tasks")
        record("S0 静态 local/operator token 行为不变；无 token 401",
               st_local == 200 and st_op_tasks == 200
               and st_op_status == 403 and st_none == 401,
               {"local": st_local, "op_tasks": st_op_tasks,
                "op_status": st_op_status, "none": st_none})

        # ---- S1 OIDC operator ----------------------------------------------
        st_tasks, _ = api(PORT_S, "/v1/tasks", token="oidc-ops", bearer=True)
        st_status, _ = api(PORT_S, "/v1/mesh/status", token="oidc-ops", bearer=True)
        st_quota, _ = api(PORT_S, "/v1/mesh/quota", token="oidc-ops", bearer=True)
        st_x, _ = api(PORT_S, "/v1/tasks", token="oidc-ops")  # X-Auth-Token 也支持
        record("S1 OIDC operator（role_map ops）可读任务、治理面 403",
               st_tasks == 200 and st_x == 200 and st_status == 403
               and st_quota == 403,
               {"tasks": st_tasks, "x_auth": st_x, "status": st_status,
                "quota": st_quota})

        # ---- S2 OIDC admin --------------------------------------------------
        st_admin, _ = api(PORT_S, "/v1/mesh/status", token="oidc-admin", bearer=True)
        record("S2 OIDC admin 可读治理面", st_admin == 200, {"status": st_admin})

        # ---- S3 失败/伪造头 --------------------------------------------------
        st_inactive, _ = api(PORT_S, "/v1/tasks", token="oidc-inactive", bearer=True)
        st_unknown, _ = api(PORT_S, "/v1/tasks", token="oidc-unknown", bearer=True)
        st_spoof_unknown, _ = api(
            PORT_S, "/v1/tasks", token="oidc-unknown", bearer=True,
            extra_headers={"X-Agentlink-Oidc-Role": "admin"})
        st_spoof_valid, _ = api(
            PORT_S, "/v1/mesh/status", token="oidc-ops", bearer=True,
            extra_headers={"X-Agentlink-Oidc-Role": "admin"})
        st_spoof_none, _ = api(
            PORT_S, "/v1/tasks",
            extra_headers={"X-Agentlink-Oidc-Role": "admin"})
        record("S3 inactive/未知/伪造内部头 401；有效 OIDC + 伪造 admin 仍 operator",
               st_inactive == 401 and st_unknown == 401
               and st_spoof_unknown == 401 and st_spoof_valid == 403
               and st_spoof_none == 401,
               {"inactive": st_inactive, "unknown": st_unknown,
                "spoof_unknown": st_spoof_unknown,
                "spoof_valid_status": st_spoof_valid,
                "spoof_none": st_spoof_none})

        # ---- S4 introspection 缓存 ------------------------------------------
        before = count_of("oidc-cache")
        st_c1, _ = api(PORT_S, "/v1/tasks", token="oidc-cache", bearer=True)
        st_c2, _ = api(PORT_S, "/v1/tasks", token="oidc-cache", bearer=True)
        after = count_of("oidc-cache")
        record("S4 5s 缓存：同 token 两次请求只打一次 introspection",
               st_c1 == 200 and st_c2 == 200 and after - before == 1,
               {"before": before, "after": after, "calls": after - before})

        # ---- S5 namespace claim ----------------------------------------------
        st_ns, _ = api(PORT_S, "/v1/tasks", {
            "task_id": "oidc-ns-1", "type": "oidc.echo", "priority": 100,
            "requires": ["cap:none"], "params": {}}, token="oidc-team", bearer=True)
        st_get_ns, t_ns = task_of("oidc-ns-1", "oidc-team")
        st_def, _ = submit("oidc-def-1", "default")
        st_cross, _ = task_of("oidc-def-1", "oidc-team")
        st_cross_submit, _ = api(PORT_S, "/v1/tasks", {
            "task_id": "oidc-ns-bad", "type": "oidc.echo",
            "namespace": "team-b", "requires": ["cap:none"]},
            token="oidc-team", bearer=True)
        st_bound_cross, _ = task_of("oidc-def-1", "oidc-ops")
        st_op_cross, _ = task_of("oidc-def-1", "oidc-admin")
        record("S5 namespace claim：自动注入 team-a、跨租户 404、越界 403、Admin 全局可见",
               st_ns == 202 and st_get_ns == 200
               and ((t_ns.get("spec") or {}).get("namespace") == "team-a")
               and st_def == 202 and st_cross == 404
               and st_cross_submit == 403 and st_bound_cross == 404
               and st_op_cross == 200,
               {"submit": st_ns, "get": st_get_ns,
                "task_namespace": (t_ns.get("spec") or {}).get("namespace"),
                "default_submit": st_def, "cross_get": st_cross,
                "cross_submit": st_cross_submit,
                "bound_ops_cross": st_bound_cross,
                "admin_global": st_op_cross})

        # ---- S6 Admin + namespace 拒绝 ---------------------------------------
        st_admin_ns, _ = api(PORT_S, "/v1/mesh/status",
                             token="oidc-admin-ns", bearer=True)
        record("S6 Admin + namespace claim 拒绝（防止租户 token 拥有全局权）",
               st_admin_ns == 401, {"status": st_admin_ns})

        # ---- S7 超时 fail-closed ---------------------------------------------
        t0 = time.monotonic()
        st_slow, _ = api(PORT_S, "/v1/tasks", token="oidc-slow", bearer=True,
                         timeout=8)
        elapsed = time.monotonic() - t0
        record("S7 introspection 超时 fail-closed（2s 超时，<4s 返回 401）",
               st_slow == 401 and 1.5 <= elapsed < 4.0,
               {"status": st_slow, "elapsed_s": round(elapsed, 2)})

        # ---- S8 存活/panic ---------------------------------------------------
        alive = proc.poll() is None
        log = open(os.path.join(ROOT, "s.log")).read().lower()
        record("S8 进程存活、日志无 panic", alive and "panic" not in log,
               {"alive": alive, "panic": "panic" in log})

    finally:
        if proc:
            try:
                proc.send_signal(signal.SIGINT)
                proc.wait(timeout=6)
            except Exception:
                proc.kill()
        mock.stop()
        with open(os.path.join(REPO, "tests", "oidc_smoke.jsonl"), "w") as f:
            for e in EVIDENCE:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        npass = sum(1 for e in EVIDENCE if e["pass"])
        print(f"oidc_smoke: {npass}/{len(EVIDENCE)} "
              f"→ {'OK' if npass == len(EVIDENCE) else 'FAILED'}", flush=True)
    sys.exit(0 if all(e["pass"] for e in EVIDENCE) else 1)


if __name__ == "__main__":
    main()
