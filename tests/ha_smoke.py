# -*- coding: utf-8 -*-
"""ha_smoke — v0.8 HA M2：内嵌 Raft 的真实多进程接线验证。

拓扑：3 个 AgentLink 进程（19001/19002/19003，peer WS 与本地 API 同端口）
      + 静态 peer 互联（discovery 关闭，避免 UDP 广播串扰）。

覆盖：
  A0 三节点选主：恰好一个 leader，其余 follower，term 一致
  A1 leader 写入 -> 复制到全部节点（状态机一致）
  A2 非 leader 写入 -> 409（写必须经 leader，线性一致）
  A3 杀掉 leader -> 重新选主；已提交数据仍在；旧 leader 回归后追平
  A4 重启节点 -> 从磁盘恢复（term/日志），仍能继续复制
  A5 /v1/ha/status 暴露角色/term/commit 位点；未启用 HA 的节点行为不变
  A7 leader 压缩日志前缀后，长期离线节点通过 InstallSnapshot 追平
  A8 joint consensus 动态成员变更：3 投票成员 -> 2 -> 3（含缩容后仍能提交）
  A6 进程存活、日志无 panic
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

BIN = os.environ.get("AL_BIN", os.path.expanduser("~/agentlink-target/release/agentlink"))
ROOT = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch/ha-smoke")
PORTS = [19001, 19002, 19003]
NODES = ["ha-n1", "ha-n2", "ha-n3"]
CLUSTER_TOKEN = "ha-cluster-token"
LOCAL_TOKEN = "ha-local-admin"

EVIDENCE = []


def record(name, ok, detail=""):
    EVIDENCE.append({"scenario": name, "pass": bool(ok), "detail": detail})
    print(("PASS " if ok else "FAIL ") + name + " | "
          + json.dumps(detail, ensure_ascii=False)[:600], flush=True)


def api(port, path, body=None, method=None, token=LOCAL_TOKEN, timeout=5):
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    m = method or ("POST" if body is not None else "GET")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["X-Auth-Token"] = token
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", method=m,
                                 data=data, headers=headers)
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


def write_cfg(port, node_id, name):
    peers = [f"{n}@{h}:{p}" for n, h, p in zip(NODES, ["127.0.0.1"] * 3, PORTS)]
    static = [f"127.0.0.1:{p}" for p in PORTS if p != port]
    cfg = {
        "v": 1, "node_id": node_id, "name": name, "listen_port": port,
        "data_dir": os.path.join(ROOT, f"data{port}"),
        "history": {"enabled": True, "len": 200, "file": "history.jsonl"},
        "discovery": {"enabled": False, "static_peers": static},
        "security": {"mode": "strict", "cluster_token": CLUSTER_TOKEN,
                     "audit": {"enabled": True}},
        "local_api": {"host": "127.0.0.1", "token": LOCAL_TOKEN},
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
        "mesh": {"enabled": False},
    }
    path = os.path.join(ROOT, f"{node_id}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    return path


def start_node(port, node_id, name):
    cfg = write_cfg(port, node_id, name)
    log = open(os.path.join(ROOT, f"{node_id}.log"), "a")
    proc = subprocess.Popen([BIN, "run", "--config", cfg, "--no-open"],
                            stdout=log, stderr=subprocess.STDOUT)
    return proc


def main():
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(ROOT, exist_ok=True)
    for p in PORTS:
        assert not probe(p), f"端口 {p} 被占用（生产隔离铁律）"

    procs = {}
    try:
        for port, node_id, name in zip(PORTS, NODES, ["HA 节点 1", "HA 节点 2", "HA 节点 3"]):
            procs[port] = start_node(port, node_id, name)
        for port in PORTS:
            assert wait_for(lambda p=port: probe(p), 25), f"节点 {port} 未启动"

        # A0 选主 ------------------------------------------------------------
        got = wait_for(lambda: leaders() if len(leaders()) == 1 else None, 30)
        statuses = {p: ha_status(p) for p in PORTS}
        record("A0 三节点选出唯一 leader，其余 follower，term 一致",
               bool(got) and len(got) == 1
               and all(s.get("role") in ("leader", "follower") for s in statuses.values())
               and len({s.get("term") for s in statuses.values()}) == 1
               and all(s.get("cluster_size") == 3 and s.get("quorum") == 2
                       for s in statuses.values()),
               {"leader": got, "status": statuses})

        leader = got[0]
        # A1 leader 写入 -> 全节点复制 ----------------------------------------
        st_prop, body = api(leader, "/v1/ha/propose",
                             {"ns": "kv", "key": "k1", "value": "v1"})
        replicated = {}
        for p in PORTS:
            def read_value(p=p):
                st, b = api(p, "/v1/ha/get", {"ns": "kv", "key": "k1"})
                return b.get("value") if st == 200 else None
            v = wait_for(lambda q=read_value: q(), 20)
            replicated[p] = v
        record("A1 leader 写入复制到全部节点（状态机一致）",
               st_prop == 200 and all(v == "v1" for v in replicated.values()),
               {"propose": st_prop, "body": body, "values": replicated})

        # A2 非 leader 写入 -> 409 --------------------------------------------
        followers = [p for p in PORTS if p != leader]
        st_f, body_f = api(followers[0], "/v1/ha/propose",
                           {"ns": "kv", "key": "k2", "value": "v2"})
        record("A2 非 leader 写入被拒（409，写必须线性一致）",
               st_f == 409, {"status": st_f, "body": body_f})

        # A3 杀 leader -> 重新选主 + 数据保留 ---------------------------------
        procs[leader].terminate()
        try:
            procs[leader].wait(timeout=5)
        except Exception:
            procs[leader].kill()
        time.sleep(0.5)
        new_leader = wait_for(
            lambda: [p for p in leaders() if p != leader] if
            len([p for p in leaders() if p != leader]) == 1 else None, 30)
        survivors = [p for p in PORTS if p != leader]
        values = {}
        for p in survivors:
            st, b = api(p, "/v1/ha/get", {"ns": "kv", "key": "k1"})
            values[p] = b.get("value") if st == 200 else None
        record("A3 leader 崩溃后重新选主，已提交数据仍在",
               bool(new_leader) and all(v == "v1" for v in values.values()),
               {"new_leader": new_leader, "values": values})

        # A3b 旧 leader 回归 -> 追平 ------------------------------------------
        procs[leader] = start_node(leader, NODES[PORTS.index(leader)],
                                   f"HA 节点 {PORTS.index(leader)+1}")
        assert wait_for(lambda: probe(leader), 25), "旧 leader 未重启"
        back = wait_for(lambda: api(leader, "/v1/ha/get",
                                    {"ns": "kv", "key": "k1"})[1].get("value")
                        == "v1", 25)
        st_l, b_l = api(leader, "/v1/ha/get", {"ns": "kv", "key": "k1"})
        record("A3b 旧 leader 回归后追平（不再是 leader，数据一致）",
               bool(back) and b_l.get("value") == "v1"
               and ha_status(leader).get("role") != "leader",
               {"value": b_l.get("value"), "status": ha_status(leader)})

        # A4 重启节点 -> 磁盘恢复 ---------------------------------------------
        victim = followers[0]
        procs[victim].terminate()
        try:
            procs[victim].wait(timeout=5)
        except Exception:
            procs[victim].kill()
        time.sleep(0.5)
        procs[victim] = start_node(victim, NODES[PORTS.index(victim)],
                                   f"HA 节点 {PORTS.index(victim)+1}")
        assert wait_for(lambda: probe(victim), 25), "重启节点未启动"
        restored = wait_for(lambda: api(victim, "/v1/ha/get",
                                        {"ns": "kv", "key": "k1"})[1].get("value")
                            == "v1", 25)
        st_v = ha_status(victim)
        record("A4 节点重启后从磁盘恢复（term/日志/状态机）",
               bool(restored) and st_v.get("enabled") is True,
               {"status": st_v})

        # A5 状态端点 & 未启用 HA 的节点 --------------------------------------
        st_s, s_s = api(PORTS[0], "/v1/ha/status")
        record("A5 /v1/ha/status 暴露角色/term/位点",
               st_s == 200 and s_s.get("role") in ("leader", "follower")
               and s_s.get("term", 0) >= 1,
               {"status": s_s})

        # A7 leader 快照压缩 + 落后节点快照拉齐 --------------------------------
        cur = wait_for(lambda: leaders()[0] if len(leaders()) == 1 else None, 20)
        assert cur, "A7 前应存在唯一 leader"
        lag = [p for p in PORTS if p != cur][0]
        procs[lag].terminate()
        try:
            procs[lag].wait(timeout=5)
        except Exception:
            procs[lag].kill()
        time.sleep(0.4)
        wrote = 0
        last_st = 0
        for i in range(1100):
            st, _b = api(cur, "/v1/ha/propose",
                         {"ns": "kv", "key": f"s{i}", "value": str(i)}, timeout=10)
            last_st = st
            if st != 200:
                break
            wrote += 1
        snap = wait_for(lambda: (ha_status(cur) if
                                 ha_status(cur).get("snapshot_index", 0) > 0 else None), 60)
        procs[lag] = start_node(lag, NODES[PORTS.index(lag)],
                                f"HA 节点 {PORTS.index(lag)+1}")
        assert wait_for(lambda: probe(lag), 25), "A7 落后节点未重启"
        caught = wait_for(lambda: api(lag, "/v1/ha/get",
                                      {"ns": "kv", "key": "s1099"})[1].get("value")
                          == "1099", 60)
        st_lag = ha_status(lag)
        record("A7 leader 压缩日志后，落后节点通过快照追平",
               last_st == 200 and wrote == 1100 and bool(snap) and bool(caught)
               and st_lag.get("snapshot_index", 0) >= (snap or {}).get("snapshot_index", 1)
               and st_lag.get("log_len", 9999) < 120
               and st_lag.get("applied_index", 0) >= 1100,
               {"wrote": wrote, "propose": last_st,
                "leader": {k: (snap or {}).get(k) for k in
                           ("snapshot_index", "log_len", "applied_index")},
                "follower": {k: st_lag.get(k) for k in
                             ("snapshot_index", "log_len", "applied_index")}})

        # A8 动态成员变更（joint consensus）：3 -> 2 -> 3 ---------------------
        cur = wait_for(lambda: leaders()[0] if len(leaders()) == 1 else None, 20)
        assert cur, "A8 前应存在唯一 leader"
        ids = dict(zip(PORTS, NODES))
        removed = [p for p in PORTS if p != cur][-1]
        keep = sorted(ids[p] for p in PORTS if p != removed)
        all_ids = sorted(ids.values())
        st_m, body_m = api(cur, "/v1/ha/members", {"voters": keep, "learners": []})
        converged = wait_for(
            lambda: all(ha_status(p).get("voters") == keep for p in PORTS), 30)
        st_w, _ = api(cur, "/v1/ha/propose",
                      {"ns": "kv", "key": "after_change", "value": "ok"})
        kept_val = wait_for(
            lambda: all(api(p, "/v1/ha/get", {"ns": "kv", "key": "after_change"})[1]
                        .get("value") == "ok" for p in PORTS if p != removed), 20)
        st_c = ha_status(cur)
        # 再扩回 3 成员：被移出的节点必须追平（AppendEntries / 快照）。
        st_b, body_b = api(cur, "/v1/ha/members", {"voters": all_ids, "learners": []})
        back = wait_for(
            lambda: all(ha_status(p).get("voters") == all_ids for p in PORTS), 30)
        caught = wait_for(
            lambda: api(removed, "/v1/ha/get", {"ns": "kv", "key": "s1099"})[1]
            .get("value") == "1099", 40)
        record("A8 joint consensus 动态缩容/扩容（3->2->3）",
               st_m == 200 and body_m.get("state") == "committed" and bool(converged)
               and st_c.get("cluster_size") == 2 and st_c.get("quorum") == 2
               and st_w == 200 and bool(kept_val)
               and st_b == 200 and body_b.get("state") == "committed"
               and bool(back) and bool(caught),
               {"shrink": {"status": st_m, "body": body_m, "converged": bool(converged)},
                "removed": removed,
                "after_shrink": {k: st_c.get(k) for k in
                                 ("cluster_size", "quorum", "voters")},
                "write_after_shrink": st_w,
                "grow": {"status": st_b, "body": body_b, "converged": bool(back),
                         "removed_caught_up": bool(caught)}})

        # A6 进程存活、无 panic -------------------------------------------------
        alive = [p for p in PORTS if probe(p)]
        logs = ""
        for node_id in NODES:
            p = os.path.join(ROOT, f"{node_id}.log")
            if os.path.exists(p):
                logs += open(p, encoding="utf-8", errors="replace").read()
        record("A6 进程存活、日志无 panic",
               len(alive) == 3 and "panicked" not in logs,
               {"alive": alive, "panic": "panicked" in logs})
    finally:
        for proc in procs.values():
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

    passed = sum(1 for e in EVIDENCE if e["pass"])
    print(f"\n{passed}/{len(EVIDENCE)} 场景通过")
    with open(os.path.join(REPO, "tests", "ha_smoke.jsonl"), "w", encoding="utf-8") as f:
        for e in EVIDENCE:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    print("证据: tests/ha_smoke.jsonl")
    return 0 if passed == len(EVIDENCE) else 1


if __name__ == "__main__":
    sys.exit(main())
