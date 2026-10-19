#!/usr/bin/env python3
"""熔断隔离冒烟（v0.6 P1）：隔离端口，绝不碰生产 8765。

场景（单节点闭环：本机 agent 的 failed 帧归账到本节点）：
  观察口径（关键）：行 to_agent 在建行时即由 taskengine 从 spec.to_agent 回填，
  不代表引擎派发；「引擎选了哪个节点」的决定性证据 = scheduled_nodes
  （/v1/mesh/status 的 license.quota_used），只有 agentless 任务（spec 无
  to_agent）才会进入 dispatch_pending 选点。
  S0 缺省（circuit 关）：agentless t0 正常 dispatch（quota_used→1），
     t1..t3 的 failed 帧归账但不熔断（taints 恒空）。
  S1-S2 熔断触发：circuit 启用（failures=3/window=60s/cooldown=12s）+
     本地 agent w-a 在线（REST register + WS client）；t1/t2/t3 依次
     accepted→failed（from_agent=w-a）→ /v1/mesh/status 的
     engine.circuit_taints 出现本节点（reason 含「熔断」）+ warn 日志在案。
  S3 隔离生效：agentless t4 建行 → 引擎 dispatch 跳过熔断节点 →
     quota_used 不增长、t4 行 to_agent 保持 None（3s 多拍验证）。
  S4 自动恢复：cooldown 过后 taints 清空 + 挂起的 t4 被派发
     （quota_used→2）。

退出码 0 = 全过。用法: python3 tests/circuit_smoke.py <agentlink-binary>
"""
import json, os, shutil, signal, socket, subprocess, sys, time, urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import engine_store_smoke as es  # license 签名助手（底部有 __main__ 守护）  # noqa: E402
import websocket  # noqa: E402  websocket-client（与 events-smoke 同款）

BIN = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser(
    "~/agentlink-target/release/agentlink")
assert os.path.exists(BIN), f"binary not found: {BIN}"

SCRATCH = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch")
ROOT = os.path.join(SCRATCH, "al-circuit")
PORT = 18971
TOKEN = "circuit-token"
AG = "w-a"

passed = failed = 0
def check(name, ok, detail=""):
    global passed, failed
    if ok:
        passed += 1
        print(f"PASS {name}")
    else:
        failed += 1
        print(f"FAIL {name}  {detail[:2000]}")


def api(path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"http://127.0.0.1:{PORT}" + path, data=data,
        headers={"X-Auth-Token": TOKEN, "Content-Type": "application/json"},
        method="POST" if body is not None else "GET")
    try:
        with urllib.request.urlopen(req, timeout=8) as r:
            return r.status, json.loads(r.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, {"_raw": raw}


def wait_health(tries=60):
    for _ in range(tries):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{PORT}/v1/health",
                                        timeout=2) as r:
                if r.status == 200:
                    return r.status, json.loads(r.read().decode())
        except Exception:
            time.sleep(0.25)
    return False, {}


def wait_for(pred, tries=60):
    for _ in range(tries):
        try:
            if pred():
                return True
        except Exception:
            pass
        time.sleep(0.25)
    return False


def start():
    log = open(os.path.join(ROOT, "node.log"), "a")
    p = subprocess.Popen([BIN, "--config", os.path.join(ROOT, "node.json"),
                          "run", "--no-open"], stdout=log, stderr=subprocess.STDOUT)
    return p, log


def stop(p, log):
    try:
        p.send_signal(signal.SIGINT)
    except Exception:
        pass
    try:
        p.wait(timeout=6)
    except Exception:
        p.kill()
    try:
        log.close()
    except Exception:
        pass


def engine_stats():
    """engine 段里的熔断视图（mesh status 的 "engine" 键）。"""
    _, d = api("/v1/mesh/status")
    return (d.get("engine") or {}).get("circuit_taints")


def row(task_id):
    """GET 单任务：响应是 {"ok":..., "task":{...}}，剥出 task 行。"""
    code, r = api(f"/v1/tasks/{task_id}")
    return (code == 200), (r.get("task") or r)


def to_agent_of(response):
    return response.get("to_agent")


def state_of(response):
    return response.get("state")


def write_cfg(circuit):
    cfg = {
        "v": 1, "node_id": "aaaaaaaaaaaa", "name": "circuit-a",
        "listen_port": PORT, "data_dir": os.path.join(ROOT, "data"),
        "history": {"enabled": True, "len": 400, "file": "history.jsonl"},
        "discovery": {"enabled": False, "methods": [], "static_peers": []},
        "security": {"mode": "strict", "cluster_token": "circuit-cluster"},
        "local_api": {"host": "127.0.0.1", "token": TOKEN},
        "web": {"auto_open": False}, "log": {"level": "info", "format": "text"},
        "mesh": {"enabled": True,
                 "license_file": os.path.join(ROOT, "license.toml"),
                 "engine": {"max_retries": 0, "tick_ms": 200,
                            "circuit": circuit}},
    }
    json.dump(cfg, open(os.path.join(ROOT, "node.json"), "w"))


def make_task(tid):
    """POST /v1/tasks：不带 to_agent 的字段置空即可；state=pending。"""
    st, r = api("/v1/tasks", {
        "task_id": tid, "type": "text", "intent": "circuit-smoke",
        "to_agent": AG, "priority": 100,
        "payload": {"content": f"task {tid}"},
    })
    return st, r


def make_agentless(tid):
    """POST /v1/tasks（无 to_agent）：引擎 dispatch 才会接手的路由形态。"""
    st, r = api("/v1/tasks", {
        "task_id": tid, "type": "text", "intent": "circuit-smoke",
        "priority": 100, "payload": {"content": f"task {tid}"},
    })
    return st, r


def quota_used():
    """/v1/mesh/status 的 license.quota_used = 引擎派发过的不重复节点数
    （dispatch_pending 在定向发送前就把 node_id 记入 scheduled_nodes，
    是「引擎选了哪个节点」的决定性证据）。"""
    _, d = api("/v1/mesh/status")
    return (d.get("license") or {}).get("quota_used", 0)


def status_frame(task_id, seq, state):
    """以执行方 w-a 名义发任务协议帧（经 /v1/send 入总线，topic 名称对齐协议 §3）。"""
    body = {"from": "aaaaaaaaaaaa", "to": "aaaaaaaaaaaa",
            "from_agent": AG, "type": "text", "action": "send",
            "payload": {"topic": "task_accepted" if state == "accepted" else "task_failed",
                        "task_id": task_id, "seq": seq,
                        "error": "circuit-smoke 人为失败（非 accepted 帧）"}}
    if state == "accepted":
        body["payload"].pop("error")
    return api("/v1/send", body)


def run_case(circuit, expect_taint):
    """走一遍 failed×3 → 隔离 → 恢复 的完整闭环，返回 (all_ok, details)。"""
    cooldown = int(circuit.get("cooldown_secs", 5))
    ok = True
    details = []

    # 基线：agentless t0 走引擎 dispatch（无熔断 → scheduled_nodes 收入本节点，
    # 行 to_agent 被标 ""——注意与建行回填 spec.to_agent 的 addressed 行区分）。
    st, _ = make_agentless("t0")
    ok &= st in (202, 200)
    dispatched = wait_for(lambda: quota_used() >= 1)
    ok &= dispatched
    details.append(f"t0.dispatch(quota_used={quota_used()})={dispatched}")

    # t1..t3（addressed，行 to_agent 来自 spec 回填；执行方失败归账用）：
    # 各一发 task_failed(seq2)（Pending→Failed 直转合法，_accepted 帧时序抖）
    # ——3 个失败事件归账到本节点。
    for tid in ["t1", "t2", "t3"]:
        make_task(tid)
        status_frame(tid, 2, True)
        wait_for(lambda t=tid: state_of(row(t)[1]) == "failed")

    # 熔断预期。
    def taints_ready():
        t = engine_stats()
        return t is not None and (len(t) > 0) == expect_taint
    taints = wait_for(taints_ready)
    details.append(f"taints={json.dumps(engine_stats(), ensure_ascii=False)[:160]}")
    ok &= taints
    if expect_taint:
        e1 = engine_stats()[0].get("node") == "aaaaaaaaaaaa"
        e2 = "熔断" in engine_stats()[0].get("reason", "")
        ok &= e1 and e2
        details.append(f"S1-S2 taint_check node_good={e1} reason_good={e2}")
    else:
        ok &= taints and not engine_stats()
        # 缺省验证不搞隔离/恢复段，直接走完。
        return ok, details

    # 隔离生效：agentless t4 建行后引擎 dispatch 跳过熔断节点——
    # quota_used 不再增长（scheduled_nodes 底数=1 来自 t0）且行 to_agent 保持 None
    # （dispatch 才会写行 to_agent，建行回填对无 to_agent spec 不生效）。
    make_agentless("t4")
    wait_for(lambda: row("t4")[0])
    time.sleep(3)
    c4, r4 = row("t4")
    d1 = c4 and state_of(r4) == "pending" and to_agent_of(r4) is None
    d2 = quota_used() == 1
    d3 = bool(engine_stats())
    ok &= d1 and d2 and d3
    details.append(f"S3 row_ok={d1} quota1={d2} taint_on={d3} t4={json.dumps(r4, ensure_ascii=False)[:140]} quota_used={quota_used()}")

    # 自动恢复：cooldown 过后 taints 清空 + 挂起的 t4 被派发（quota_used 增至 2）。
    # 自动恢复：cooldown 过后 taints 清空（必须显式 == []，HTTP 瞬断返回
    # None 不得误判为「已恢复」）+ 挂起的 t4 被派发（dispatch 的
    # mark_assigned 把行 to_agent 从 None 写成 ""——scheduled_nodes 是
    # 去重集合、同节点不重复计数，quota_used 不再变化，故用行标记作证据）。
    recovered = wait_for(lambda: engine_stats() == [], tries=cooldown * 4 + 30)
    assigned = wait_for(lambda: to_agent_of(row("t4")[1]) == "", tries=40)
    r4b = quota_used()
    ok &= recovered
    ok &= assigned
    details.append(f"S4 recovered={recovered} assigned={assigned} final_quota={r4b}")
    return ok, details


def main():
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(ROOT + "/data", exist_ok=True)
    es.signer_selfcheck()
    lic = es.sign_license_text({
        "tier": "starter", "node_quota": 4, "node_family_prefix": "lan",
        "issued": "2026-10-01T00:00:00Z", "expires": "2036-12-31T23:59:59Z",
    }, "circuit smoke")
    open(os.path.join(ROOT, "license.toml"), "w").write(lic)
    with socket.socket() as s:
        s.settimeout(0.5)
        assert s.connect_ex(("127.0.0.1", PORT)) != 0, f"port {PORT} busy"

    # ---- S0: 缺省（circuit 关）—— failed 帧归账但不熔断 ------------------
    write_cfg({"enabled": False})
    p, log = start()
    wait_health()
    api("/v1/agents", {"agent_id": AG, "name": AG, "capabilities": ["text"]})
    ws = websocket.WebSocket()
    ws.connect(f"ws://127.0.0.1:{PORT}/v1/ws?token={TOKEN}&client={AG}")
    ws.settimeout(5)
    ok, details = run_case({"enabled": False}, expect_taint=False)
    check("S0 circuit 关：failed 不熔断、dispatch 照常", ok, "; ".join(map(str, details)))
    p.send_signal(signal.SIGINT); p.wait(timeout=6); log.close()

    # ---- S1-S4: 启用 circuit 的完整闭环 -----------------------------------
    shutil.rmtree(os.path.join(ROOT, "data"), ignore_errors=True)
    open(os.path.join(ROOT, "node.log"), "w").close()  # 日志重开
    write_cfg({"enabled": True, "failures": 3, "window_secs": 60,
               "cooldown_secs": 12})
    p, log = start()
    wait_health()
    api("/v1/agents", {"agent_id": AG, "name": AG, "capabilities": ["text"]})
    ws.close()
    ws2 = websocket.WebSocket()
    ws2.connect(f"ws://127.0.0.1:{PORT}/v1/ws?token={TOKEN}&client={AG}")
    ws2.settimeout(5)
    ok, details = run_case(
        {"enabled": True, "failures": 3, "window_secs": 60, "cooldown_secs": 12},
        expect_taint=True)
    check("S1-S4 熔断触发/隔离派发/自动恢复闭环", ok, "; ".join(map(str, details)))
    # 熔断 warn 日志在案。
    logtxt = open(os.path.join(ROOT, "node.log")).read()
    check("S2 日志含「节点熔断」warn（unschedulable 已置）",
          "节点熔断" in logtxt and "unschedulable" in logtxt)
    stop(p, log)
    try:
        ws2.close()
    except Exception:
        pass


try:
    main()
except Exception as e:
    import traceback; traceback.print_exc()
finally:
    pass

print(f"SMOKE_RESULT {passed}/{passed + failed}")
sys.exit(0 if failed == 0 else 1)
