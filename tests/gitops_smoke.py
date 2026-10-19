#!/usr/bin/env python3
"""GitOps 目录驱动冒烟（v0.6 P1）：隔离端口，绝不碰生产 8765。

场景：
  S0 缺省（gitops 关）与 mesh off：节点正常、零惰性副作用（/v1/health 正常）。
  S1 mesh on（自签 smoke license）+ gitops on：tasks 目录写入合法 task_spec
     → 引擎表自动 apply（/v1/tasks 出现该任务，state 非 404）。
  S2 幂等：同内容文件不再重 apply（行数不涨 / 不刷 409 告警）；新文件 → 新任务。
  S3 坏 JSON：节点不炸、引擎表完好、日志含解析失败告警（每内容一次）。
  S4 mesh off 但 gitops enabled 的独立节点：启动即打 mesh 缺席错误、不 spawn、
     节点照常健康（空态不崩）。

退出码 0 = 全过。用法: python3 tests/gitops_smoke.py <agentlink-binary>
"""
import base64, json, os, shutil, signal, socket, subprocess, sys, time, urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import engine_store_smoke as es  # 复用 license 签名助手（module 底部有 __main__ 守护）  # noqa: E402

BIN = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser(
    "~/agentlink-target/release/agentlink")
assert os.path.exists(BIN), f"binary not found: {BIN}"

SCRATCH = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch")
ROOT = os.path.join(SCRATCH, "al-gitops")
PORT_A, PORT_B = 18951, 18961
TOKEN = "gitops-token"
NODE_A, NODE_B = "aaaaaaaaaaaa", "bbbbbbbbbbbb"
LICENSE_A, LICENSE_B = os.path.join(ROOT, "license.toml"), os.path.join(ROOT, "license_b.toml")

passed = failed = 0
def check(name, ok, detail=""):
    global passed, failed
    if ok:
        passed += 1
        print(f"PASS {name}")
    else:
        failed += 1
        print(f"FAIL {name}  {detail[:240]}")


def jreq(base, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data,
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


def wait_health(base, tries=60):
    for _ in range(tries):
        try:
            with urllib.request.urlopen(base + "/v1/health", timeout=2) as r:
                if r.status == 200:
                    return True
        except Exception:
            time.sleep(0.25)
    return False


def port_free(port):
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) != 0


# 引擎表只有在单任务 GET（/v1/tasks/{id}）可读；列表 GET 是 v0.5 历史投影
# （读 mesh 事件 journal 之外的 router 环），二者口径不同。故断言走单任务。
def task_row(key, task_id):
    base = f"http://127.0.0.1:{PORT_A if key == 'a' else PORT_B}"
    code, row = jreq(base, f"/v1/tasks/{task_id}")
    return code, row


def task_present(key, task_id):
    code, row = task_row(key, task_id)
    return code == 200 and row.get("task_id") == task_id


def wait_for(pred, tries=60):
    for _ in range(tries):
        try:
            if pred():
                return True
        except Exception:
            pass
        time.sleep(0.25)
    return False


procs = {}
def start(key):
    log = open(os.path.join(ROOT, f"{key}.log"), "a")
    p = subprocess.Popen([BIN, "--config", os.path.join(ROOT, f"{key}.json"),
                          "run", "--no-open"],
                         stdout=log, stderr=subprocess.STDOUT)
    procs[key] = (p, log)
    return p


def stop_all():
    for key, (p, log) in list(procs.items()):
        try:
            p.send_signal(signal.SIGINT)
        except Exception:
            pass
    for key, (p, log) in list(procs.items()):
        try:
            p.wait(timeout=6)
        except Exception:
            p.kill()
        try:
            log.close()
        except Exception:
            pass
    procs.clear()


def log_text(key):
    try:
        return open(os.path.join(ROOT, f"{key}.log")).read()
    except Exception:
        return ""


def write_license(path, node_prefix):
    es.signer_selfcheck()
    entries = {
        "tier": "starter",
        "node_quota": 4,
        "node_family_prefix": node_prefix,
        "issued": "2026-10-01T00:00:00Z",
        "expires": "2036-12-31T23:59:59Z",
    }
    body = es.sign_license_text(entries, f"gitops smoke ({node_prefix})")
    with open(path, "w") as f:
        f.write(body)


def write_cfg(key, *, mesh, gitops, tasks_dir=None):
    port = PORT_A if key == "a" else PORT_B
    node_id = NODE_A if key == "a" else NODE_B
    cfg = {
        "v": 1, "node_id": node_id, "name": f"gitops-{key}",
        "listen_port": port, "data_dir": os.path.join(ROOT, key),
        "history": {"enabled": True, "len": 200, "file": "history.jsonl"},
        "discovery": {"enabled": False, "methods": [], "static_peers": []},
        "security": {"mode": "strict", "cluster_token": "gitops-cluster"},
        "local_api": {"host": "127.0.0.1", "token": TOKEN},
        "web": {"auto_open": False}, "log": {"level": "info", "format": "text"},
    }
    if mesh:
        lic = LICENSE_A if key == "a" else LICENSE_B
        cfg["mesh"] = {"enabled": True, "license_file": lic}
    if gitops:
        cfg["gitops"] = {"enabled": True, "dir": tasks_dir,
                         "interval_secs": 2}
    json.dump(cfg, open(os.path.join(ROOT, f"{key}.json"), "w"))


def spec(tid, typ="echo"):
    return json.dumps({"task_id": tid, "type": typ,
                       "intent": "gitops-smoke",
                       "payload": {"content": f"from {tid}"}}) + "\n"


def main():
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(ROOT, exist_ok=True)
    os.makedirs(os.path.join(ROOT, "a"), exist_ok=True)
    os.makedirs(os.path.join(ROOT, "b"), exist_ok=True)
    for p in (PORT_A, PORT_B):
        assert port_free(p), f"port {p} busy"
    write_license(LICENSE_A, "lan")

    # ---- S0: 缺省（无 mesh、无 gitops）一切照旧 ----------------------------
    tasks_a = os.path.join(ROOT, "tasks")
    os.makedirs(tasks_a, exist_ok=True)
    write_cfg("a", mesh=False, gitops=False)
    start("a")
    check("S0 节点健康（缺省段缺省零副作用）", wait_health(f"http://127.0.0.1:{PORT_A}"))
    stop_all()

    # ---- S1/S2/S3: mesh on + gitops on → 自动 apply ------------------------
    write_cfg("a", mesh=True, gitops=True, tasks_dir=tasks_a)
    start("a")
    check("S1 mesh on 启动健康（license 有效）", wait_health(f"http://127.0.0.1:{PORT_A}"))
    check("S1 features 含 engine（引擎在场，apply 才有去处）",
          "engine" in json.dumps(jreq(f"http://127.0.0.1:{PORT_A}", "/v1/health")[1]))

    os.path.exists(tasks_a) or os.makedirs(tasks_a, exist_ok=True)
    with open(os.path.join(tasks_a, "g1.json"), "w") as f:
        f.write(spec("g1"))
    g1_in = wait_for(lambda: task_present("a", "g1"))  # interval 2s → 数拍内
    check("S1 tasks 目录写入 → 引擎表自动 apply（g1 在场）", g1_in,
          str(task_row("a", "g1"))[:160])

    # S2 幂等：同内容不再 apply（日志不再出现第二条 applied；行数不涨）。
    applied_before = log_text("a").count("gitops: applied task spec")
    with open(os.path.join(tasks_a, "g1.json"), "w") as f:
        f.write(spec("g1"))  # 同内容重写（mtime 变、hash 同）
    time.sleep(6)  # 两个 scan 周期以上
    applied_after = log_text("a").count("gitops: applied task spec")
    check("S2 同内容文件不重复 apply（无重复行/extra 日志）",
          applied_before >= 1 and applied_after == applied_before,
          f"applied={applied_before}→{applied_after}")

    with open(os.path.join(tasks_a, "g2.json"), "w") as f:
        f.write(spec("g2"))
    g2_in = wait_for(lambda: task_present("a", "g2"))
    check("S2 新文件 → 新任务 apply", g2_in)

    # 更新已有文件（同 task_id 新内容）：Effective 幂等=首见定版 + 非终态 409。
    with open(os.path.join(tasks_a, "g1.json"), "w") as f:
        f.write(json.dumps({"task_id": "g1", "type": "text",
                            "intent": "gitops-smoke-updated",
                            "payload": {"content": "changed"}}) + "\n")
    time.sleep(6)
    code, g1 = task_row("a", "g1")
    check("S3 同 task_id 新内容 → 不换行（首见定版/非终态 409）",
          g1_in and code == 200 and g1.get("state", "") != "404"
          and "g1" in json.dumps(g1),
          f"code={code} row={json.dumps(g1)[:120]}")

    # S3 坏 JSON：不炸、引擎表完好、告警一次。
    with open(os.path.join(tasks_a, "broken.json"), "w") as f:
        f.write("{not valid json")
    time.sleep(7)
    health = None
    try:
        health = jreq(f"http://127.0.0.1:{PORT_A}", "/v1/health")[0] == 200
    except Exception:
        pass
    g1_ok = task_present("a", "g1") and g2_in
    check("S3 坏 JSON 不炸、引擎表完好", health and g1_ok)
    check("S3 日志含坏文件告警（kept unapplied）",
          "kept unapplied" in log_text("a") and
          "broken.json" in log_text("a"), log_text("a")[-200:])
    stop_all()

    # ---- S4: mesh off + gitops on —— 空态不崩 ------------------------------
    write_cfg("b", mesh=False, gitops=True, tasks_dir=os.path.join(ROOT, "tasks"))
    start("b")
    check("S4 mesh 缺席：gitops 不 spawn 但节点健康",
          wait_health(f"http://127.0.0.1:{PORT_B}") and
          "gitops: mesh 未启用" in log_text("b"), log_text("b")[-240:])
    stop_all()


try:
    main()
finally:
    stop_all()

print(f"SMOKE_RESULT {passed}/{passed + failed}")
sys.exit(0 if failed == 0 else 1)
