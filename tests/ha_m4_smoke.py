# -*- coding: utf-8 -*-
"""ha_m4_smoke — v0.8 HA M4：业务状态迁移（taskstore/quota/audit/oidc）真实多进程验证。

拓扑：3 个 AgentLink 进程（19011/19012/19013，peer WS 与本地 API 同端口）
      + mesh license（engine-store 冒烟同一把自签 fixture 私钥）
      + mesh.quota（qns 速率 6/min；ons 活跃 2）+ audit 开启。

覆盖设计 §8 M4 出口标准：
  B0 三节点选主 + mesh engine 生效
  B1 任务声明走 leader -> 复制到全部节点；follower 写 -> 409
  B2 OIDC 会话写复制状态机：任一副本可用；follower 登出 -> 转发 leader 删除
  B3 配额全局一致：速率窗口跨 leader 连续（换主后仍按同一窗口截流）；
     活跃上限按复制任务表计数（新 leader 同样拒绝）
  B4 滚动重启（follower->follower->leader）：全程有 leader 可写（202），
     重启后任务行与会话仍在（登录不掉线）
  B5 审计链跨节点连续：换主后 seq 不重置、prev 接上前链、记录来自不同节点
  B6 进程存活、日志无 panic
"""
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "tests"))
import engine_store_smoke as es  # noqa: E402

BIN = os.environ.get("AL_BIN", os.path.expanduser("~/agentlink-target/release/agentlink"))
ROOT = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch/ha-m4-smoke")
PORTS = [19011, 19012, 19013]
NODES = ["m4-n1", "m4-n2", "m4-n3"]
CLUSTER_TOKEN = "ha-m4-cluster-token"
LOCAL_TOKEN = "ha-m4-local-admin"
QUOTA_RATE = 6

EVIDENCE = []


def record(name, ok, detail=""):
    EVIDENCE.append({"scenario": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + " | "
          + json.dumps(detail, ensure_ascii=False)[:700], flush=True)


def api(port, path, body=None, method=None, token=LOCAL_TOKEN, bearer=False, timeout=8):
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    m = method or ("POST" if body is not None else "GET")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization" if bearer else "X-Auth-Token"] = (
            f"Bearer {token}" if bearer else token)
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", method=m, data=data, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode()
            try:
                return r.status, json.loads(raw)
            except Exception:
                return r.status, {"raw": raw}
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
        s.settimeout(0.3)
        return s.connect_ex(("127.0.0.1", port)) == 0


def wait_for(fn, timeout=30, step=0.3):
    end = time.time() + timeout
    while time.time() < end:
        v = fn()
        if v:
            return v
        time.sleep(step)
    return fn()


def ha_status(port):
    st, body = api(port, "/v1/ha/status")
    return body if st == 200 else {}


def leaders():
    out = []
    for p in PORTS:
        if not probe(p):
            continue
        s = ha_status(p)
        if s.get("role") == "leader":
            out.append(p)
    return out


def current_leader(exclude=None):
    alive = [p for p in PORTS if p != exclude]
    got = [p for p in alive if ha_status(p).get("role") == "leader"]
    return got[0] if len(got) == 1 else None


def wait_leader(exclude=None, timeout=30):
    return wait_for(lambda: current_leader(exclude), timeout)


def task_state(port, task_id):
    st, body = api(port, f"/v1/tasks/{task_id}")
    if st == 200:
        return body.get("task", {}).get("state")
    return None


def submit(port, task_id, namespace=None, extra=None):
    spec = {"task_id": task_id, "type": "shell", "payload": {"cmd": f"echo {task_id}"}}
    if namespace:
        spec["namespace"] = namespace
    if extra:
        spec.update(extra)
    return api(port, "/v1/tasks", spec)


def quota_row(port, namespace):
    st, body = api(port, "/v1/mesh/quota")
    if st != 200:
        return {}
    for row in body.get("namespaces", []):
        if row.get("namespace") == namespace:
            return row
    return {}


def audit_verify(port):
    st, body = api(port, "/v1/audit/verify")
    return body if st == 200 else {}


def audit_export(port, limit=5000):
    st, body = api(port, f"/v1/audit/export?limit={limit}")
    return body if st == 200 else {}


def write_license():
    lic = es.sign_license_text(
        {"tier": "starter", "node_quota": 15,
         "issued": "2026-10-01T00:00:00Z", "expires": "2036-12-31T23:59:59Z",
         "node_family_prefix": "lan"},
        "AgentLink HA M4 smoke license (dev fixture key)")
    with open(os.path.join(ROOT, "license.toml"), "w", encoding="utf-8") as f:
        f.write(lic)


def cfg_of(port, node_id):
    peers = [f"{n}@{h}:{p}" for n, h, p in zip(NODES, ["127.0.0.1"] * 3, PORTS)]
    static = [f"127.0.0.1:{p}" for p in PORTS if p != port]
    return {
        "v": 1, "node_id": node_id, "name": f"HA M4 {node_id}",
        "listen_port": port,
        "data_dir": os.path.join(ROOT, f"data{port}"),
        "history": {"enabled": True, "len": 200, "file": "history.jsonl"},
        "discovery": {"enabled": False, "static_peers": static},
        "security": {"mode": "strict", "cluster_token": CLUSTER_TOKEN,
                     "audit": {"enabled": True}},
        "local_api": {"host": "127.0.0.1", "token": LOCAL_TOKEN},
        # 复制会话（sess:*）只有在 browser_login 开启时才作为 bearer 生效；
        # 端点仅做配置校验，session token 不会走 introspection。
        "auth": {"oidc": {
            "enabled": True,
            "mode": "introspection",
            "introspection_url": "http://127.0.0.1:9/introspect",
            "client_id": "ha-m4-smoke",
            "client_secret": "ha-m4-smoke-secret",
            "issuer": "http://127.0.0.1:9/",
            "jwks_uri": "http://127.0.0.1:9/jwks",
            "authorization_endpoint": "http://127.0.0.1:9/auth",
            "token_endpoint": "http://127.0.0.1:9/token",
            "redirect_uri": f"http://127.0.0.1:{port}/v1/auth/oidc/callback",
            "browser_login": True,
        }},
        "delivery": {"ack_timeout_seconds": 5, "max_retries": 2,
                     "offline_ttl_seconds": 300, "dedup_cache_size": 2048,
                     "persist": False},
        "log": {"level": "info", "format": "text", "file": ""},
        "ha": {
            "enabled": True,
            "cluster": peers,
            "election_timeout_ms": 1000,
            "heartbeat_interval_ms": 200,
            "snapshot_threshold_entries": 1000,
            "read_consistency": "leader",
            "fsync": "batch",
        },
        "mesh": {
            "enabled": True,
            "license_file": "license.toml",
            "engine": {"tick_ms": 200, "max_retries": 0},
            "quota": {
                "enabled": True,
                "namespaces": {
                    "qns": {"max_submit_per_minute": QUOTA_RATE},
                    "ons": {"max_open": 2},
                },
            },
        },
    }


def write_cfg(port, node_id):
    path = os.path.join(ROOT, f"{node_id}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg_of(port, node_id), f, ensure_ascii=False, indent=2)
    return path


def start_node(port, node_id):
    cfg = write_cfg(port, node_id)
    log = open(os.path.join(ROOT, f"{node_id}.log"), "a")
    return subprocess.Popen([BIN, "run", "--config", cfg, "--no-open"],
                            stdout=log, stderr=subprocess.STDOUT)


def stop_node(port, procs):
    p = procs.get(port)
    if p is None:
        return
    p.terminate()
    try:
        p.wait(timeout=6)
    except Exception:
        p.kill()
    time.sleep(0.5)


def session_value(ttl_seconds=3600):
    return {
        "role": "admin",
        "sub": "m4-smoke-sub",
        "expires_ms": int(time.time() * 1000) + ttl_seconds * 1000,
    }


def put_session(leader, token, rec):
    return api(leader, "/v1/ha/propose", {
        "ns": "oidc", "key": f"sess:{token}",
        "value": json.dumps(rec, separators=(",", ":")),
    })


def main():
    if not os.path.exists(BIN):
        print(f"FAIL  binary-missing  {BIN} 先 cargo build --release")
        return 1
    ok, why = es.signer_selfcheck()
    if not ok:
        print(f"ABORT: license fixture self-check failed: {why}")
        return 1

    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(ROOT, exist_ok=True)
    write_license()
    for p in PORTS:
        assert not probe(p), f"端口 {p} 被占用（生产隔离铁律）"

    procs = {}
    try:
        for port, node_id in zip(PORTS, NODES):
            procs[port] = start_node(port, node_id)
        assert wait_for(lambda: all(probe(p) for p in PORTS), 30), "节点未全部启动"

        # ---- B0 选主 + mesh engine ---------------------------------------
        leader = wait_leader()
        health = {p: api(p, "/v1/health")[1] for p in PORTS}
        engine_ok = all("engine" in h.get("features", []) for h in health.values())
        record("B0 三节点选出唯一 leader，mesh engine 全节点生效",
               bool(leader) and engine_ok
               and all(ha_status(p).get("cluster_size") == 3 for p in PORTS),
               {"leader": leader,
                "features": {p: h.get("features") for p, h in health.items()}})

        # ---- B1 任务复制 + follower 写拒绝 --------------------------------
        st_t1, body_t1 = submit(leader, "m4-t1")
        replic = {}
        for p in PORTS:
            replic[p] = wait_for(lambda q=p: task_state(q, "m4-t1"), 25)
        follower = [p for p in PORTS if p != leader][0]
        st_f, body_f = submit(follower, "m4-follower-write")
        leader_id = NODES[PORTS.index(leader)] if leader else ""
        record("B1 leader 声明任务复制到全部节点；follower 写 409",
               st_t1 == 202 and all(v == "pending" for v in replic.values())
               and st_f == 409 and body_f.get("leader_id") == leader_id,
               {"submit": st_t1, "states": replic,
                "follower_write": {"status": st_f, "body": body_f}})

        # ---- B2 OIDC 会话复制 + follower 登出转发 -------------------------
        sess = "al_sess_m4smoke_session"
        st_s, body_s = put_session(leader, sess, session_value())
        read_st = {}
        for p in PORTS:
            read_st[p] = wait_for(
                lambda q=p: api(q, "/v1/tasks", token=sess, bearer=True)[0] == 200, 25)
        # follower 登出：ha_kv(Delete) 必须转发给 leader 并等提交。
        st_o, body_o = api(follower, "/v1/auth/logout", method="POST",
                           token=sess, bearer=True)
        gone = {}
        for p in PORTS:
            gone[p] = wait_for(
                lambda q=p: api(q, "/v1/tasks", token=sess, bearer=True)[0] == 401, 25)
        record("B2 会话写复制状态机（全节点可认证）；follower 登出转发 leader 生效",
               st_s == 200 and all(read_st.values()) and st_o == 200
               and all(gone.values()),
               {"session_put": st_s, "read_ok": read_st,
                "follower_logout": {"status": st_o, "body": body_o},
                "after_logout_401": gone})

        # ---- B3 配额全局一致 ---------------------------------------------
        qcodes = [submit(leader, f"q{i}", namespace="qns")[0] for i in (1, 2, 3)]
        ocodes = [submit(leader, f"o{i}", namespace="ons")[0] for i in (1, 2)]
        # 记审计基线（换主前链头）。
        base = audit_verify(leader)
        base_count, base_head = base.get("count", 0), base.get("head", "")
        # 换主：新 leader 必须沿用复制窗口与复制任务表。
        stop_node(leader, procs)
        new_leader = wait_leader(exclude=leader)
        assert new_leader, "B3 换主失败"
        qcodes += [submit(new_leader, f"q{i}", namespace="qns")[0] for i in (4, 5, 6)]
        st_q7, body_q7 = submit(new_leader, "q7", namespace="qns")
        st_o3, body_o3 = submit(new_leader, "o3", namespace="ons")
        rate_rows = {}
        for p in [x for x in PORTS if x != leader]:
            rate_rows[p] = wait_for(
                lambda q=p: (quota_row(q, "qns") or {}).get("submit_last_minute") == QUOTA_RATE
                and quota_row(q, "qns"), 25)
        record("B3 配额全局一致：速率窗口跨 leader 连续；活跃上限按复制任务表计数",
               qcodes == [202] * 6 and ocodes == [202, 202]
               and st_q7 == 429 and body_q7.get("reason") == "rate_limited"
               and st_o3 == 429 and body_o3.get("reason") == "open_quota"
               and all(bool(v) for v in rate_rows.values()),
               {"qns_submits": qcodes, "ons_submits": ocodes,
                "after_failover": {"rate": [st_q7, body_q7.get("reason")],
                                   "open": [st_o3, body_o3.get("reason")]},
                "quota_views": rate_rows})

        # 旧 leader 回归（滚动前恢复三副本）。
        procs[leader] = start_node(leader, NODES[PORTS.index(leader)])
        assert wait_for(lambda: probe(leader) and ha_status(leader).get("enabled"), 30), \
            "旧 leader 未重启"
        caught = wait_for(lambda: task_state(leader, "q7") is None
                          and task_state(leader, "m4-t1") == "pending", 30)
        assert caught, "回归节点未追平任务表"

        # ---- B4 滚动重启不掉线 -------------------------------------------
        roll_sess = "al_sess_m4_roll"
        leader = wait_leader()
        st_rs, _ = put_session(leader, roll_sess, session_value())
        roll_codes = []
        order = [p for p in PORTS if p != leader] + [leader]
        for p in order:
            cur = wait_leader()
            if cur is None:
                break
            st_r, _b = submit(cur, f"roll-{p}")
            roll_codes.append(st_r)
            stop_node(p, procs)
            procs[p] = start_node(p, NODES[PORTS.index(p)])
            up = wait_for(lambda q=p: probe(q) and ha_status(q).get("enabled"), 30)
            if not up:
                break
            cur2 = wait_leader()
            if cur2 is None:
                break
            st_r2, _b2 = submit(cur2, f"roll2-{p}")
            roll_codes.append(st_r2)
        final_leader = wait_leader()
        all_up = all(probe(p) for p in PORTS)
        survives = {}
        for p in PORTS:
            survives[p] = wait_for(lambda q=p: task_state(q, "m4-t1") == "pending"
                                   and task_state(q, "roll2-" + str(order[-1])), 35)
        sess_after = {}
        for p in PORTS:
            sess_after[p] = wait_for(
                lambda q=p: api(q, "/v1/tasks", token=roll_sess, bearer=True)[0] == 200, 30)
        record("B4 滚动重启（follower->follower->leader）全程可写、任务/会话不掉线",
               st_rs == 200 and roll_codes == [202] * 6 and all_up
               and bool(final_leader) and all(survives.values())
               and all(sess_after.values()),
               {"rolling_writes": roll_codes, "all_up": all_up,
                "leader_after": final_leader, "tasks_present": survives,
                "session_after_roll": sess_after})

        # ---- B5 审计链跨节点连续 -----------------------------------------
        def audit_consistent():
            obs = {p: audit_verify(p) for p in PORTS}
            vals = list(obs.values())
            if (all(v.get("valid") and v.get("count", 0) >= base_count for v in vals)
                    and len({v.get("count") for v in vals}) == 1
                    and len({v.get("head") for v in vals}) == 1):
                return obs
            return None

        obs = wait_for(audit_consistent, 30) or {}
        counts = {p: (obs.get(p) or {}).get("count") for p in PORTS}
        heads = {p: (obs.get(p) or {}).get("head") for p in PORTS}
        verified = (obs.get(final_leader) or {}) if final_leader else {}
        export = audit_export(final_leader) if final_leader else {}
        recs = export.get("records", [])
        cont = next((r for r in recs if r.get("seq") == base_count + 1), None)
        after = [r for r in recs if r.get("seq", 0) > base_count]
        nodes_after = sorted({r.get("node_id") for r in after})
        chains_ok = len(set(counts.values())) == 1 and len(set(heads.values())) == 1
        new_leader_id = NODES[PORTS.index(new_leader)] if new_leader else ""
        record("B5 审计链跨节点连续（换主后 seq 不重置、prev 接前链、多节点追加）",
               chains_ok and verified.get("valid") is True
               and bool(cont) and cont.get("prev") == base_head
               and new_leader_id in nodes_after and len(nodes_after) >= 2,
               {"counts": counts, "heads_equal": len(set(heads.values())) == 1,
                "base": {"count": base_count, "head": base_head},
                "continues": {"seq": (cont or {}).get("seq"),
                              "prev": (cont or {}).get("prev")},
                "nodes_after": nodes_after,
                "verify": {k: verified.get(k) for k in ("valid", "count", "head")}})

        # 清理滚动测试留下的会话。
        api(final_leader, "/v1/auth/logout", method="POST", token=roll_sess, bearer=True)

        # ---- B6 进程存活、无 panic --------------------------------------
        logs = ""
        for node_id in NODES:
            path = os.path.join(ROOT, f"{node_id}.log")
            if os.path.exists(path):
                logs += open(path, encoding="utf-8", errors="replace").read()
        alive = [p for p in PORTS if probe(p)]
        record("B6 进程存活、日志无 panic",
               len(alive) == 3 and "panicked" not in logs,
               {"alive": alive, "panic": "panicked" in logs})
    finally:
        for p in list(procs.values()):
            try:
                p.terminate()
                p.wait(timeout=5)
            except Exception:
                try:
                    p.kill()
                except Exception:
                    pass

    passed = sum(1 for e in EVIDENCE if e["pass"])
    print(f"\n{passed}/{len(EVIDENCE)} 场景通过")
    with open(os.path.join(REPO, "tests", "ha_m4_smoke.jsonl"), "w",
              encoding="utf-8") as f:
        for e in EVIDENCE:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    print("证据: tests/ha_m4_smoke.jsonl")
    return 0 if passed == len(EVIDENCE) else 1


if __name__ == "__main__":
    sys.exit(main())
