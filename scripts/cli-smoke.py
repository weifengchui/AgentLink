#!/usr/bin/env python3
"""cli-smoke — v0.5 CLI 薄壳（get/logs/apply）端到端验证（隔离总线 18769，绝不指向生产）。

场景：
  S1 get nodes 表格（本机行 + 默认 --config 寻址 listen_port/token）
  S2 get agents：在线 worker、--requires（服务端过滤）、--online、无匹配不报错
  S3 开源构建不暴露 /v1/tasks（404，任务引擎属商业版）
  S4 apply --dry-run：打印请求体、不发流量
  S5 apply 实发：msg_id 输出；回报帧经总线投递，history 出现 task_succeeded
  S6 logs 单次拉取：逐行 seq/方向/payload 摘要
  S7 logs --follow：增量轮询捕获新事件，SIGINT 干净退出
  S8 无配置文件：友好错误提示 `agentlink init`
  S9 --url/--token 全覆盖：不读配置文件也能查
  S10 completions bash/zsh 补全脚本产物（v0.6 P1 CLI 完全体）
  S11 top 快照：本机行+CPU 列+熔断列，无 tty 不染 ANSI
  S12 top --watch 秒级刷新（≥2 帧）

用法：python3 cli-smoke.py <path-to-agentlink-binary>
退出码 0 = 全过。
"""
import json, os, shutil, signal, subprocess, sys, tempfile, threading, time, urllib.request

BIN = sys.argv[1] if len(sys.argv) > 1 else None
assert BIN and os.path.exists(BIN), "usage: cli-smoke.py <agentlink-binary>"
BASE = "http://127.0.0.1:18769"
TOKEN = "cli-smokeya9"
NODE_ID = "fa15ee000909"  # 12 位小写 hex：/v1/send resolve_target 的硬要求
CORR = "cli-smoke-batch-A"
VENV_PY = os.environ.get("AGENTLINK_SMOKE_PY", sys.executable)
HERE = os.path.dirname(os.path.abspath(__file__))
SCRATCH = tempfile.mkdtemp(prefix="agentlink-cli-smoke-")
D = os.path.join(SCRATCH, "al-cli-smoke")
CFG = os.path.join(SCRATCH, "al-cli-node.json")
TASK = os.path.join(SCRATCH, "al-cli-task.json")
EMPTY_DIR = os.path.join(SCRATCH, "al-cli-empty")

results = []
def check(name, ok, detail=""):
    results.append((name, bool(ok)))
    print(("PASS " if ok else "FAIL ") + name + (f"  [{detail}]" if detail and not ok else ""))

def api(path, body=None, method=None, token=TOKEN):
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data,
        headers={"X-Auth-Token": token, "Content-Type": "application/json"},
        method=method or ("POST" if body is not None else "GET"))
    with urllib.request.urlopen(req, timeout=8) as r:
        return json.loads(r.read().decode())

def run_cli(*args, cwd=None, cfg=CFG):
    parts = [BIN]
    if cfg:
        parts += ["--config", cfg]
    parts += list(args)
    p = subprocess.run(parts, cwd=cwd, capture_output=True, text=True, timeout=60)
    return p.returncode, p.stdout, p.stderr

def start_node():
    shutil.rmtree(D, ignore_errors=True)
    os.makedirs(D, exist_ok=True)
    json.dump({
        "v": 1, "node_id": NODE_ID, "name": "v05-cli-smoke",
        "listen_port": 18769, "data_dir": D,
        "history": {"enabled": True, "len": 500, "file": "history.jsonl"},
        "discovery": {"enabled": False},
        "security": {"cluster_token": "smokecluster"},
        "local_api": {"host": "127.0.0.1", "token": TOKEN},
        "web": {"open": False},
    }, open(CFG, "w"))
    p = subprocess.Popen([BIN, "--config", CFG, "run", "--no-open"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(80):
        try:
            if api("/v1/health", token="***").get("ok"):
                return p
        except Exception:
            pass
        time.sleep(0.1)
    raise RuntimeError("node did not come up")

def start_executor():
    p = subprocess.Popen([VENV_PY, os.path.join(HERE, "task-proto", "executor.py"),
                          "--base", BASE, "--token", TOKEN, "--agent", "worker"],
                         cwd=os.path.join(HERE, "task-proto"),
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(50):
        try:
            ag = api("/v1/fleet/agents").get("agents", [])
            if any(a.get("agent_id") == "worker" and a.get("online") for a in ag):
                return p
        except Exception:
            pass
        time.sleep(0.1)
    raise RuntimeError("executor agent did not come online")

def history_count():
    return len(api("/v1/history").get("history", []))

def history_has(topic, task_id):
    for e in api("/v1/history").get("history", []):
        pl = e.get("payload") or {}
        if pl.get("topic") == topic and pl.get("task_id") == task_id:
            return True
    return False

def main():
    rc = subprocess.run([VENV_PY, "-c", "import websocket"], capture_output=True).returncode
    assert rc == 0, f"需要 websocket-client 的 python: {VENV_PY}（设 AGENTLINK_SMOKE_PY 指向可用解释器）"
    node = start_node()
    executor = None
    follow = None
    try:
        executor = start_executor()
        # 注册发起方 agent（执行方的回报帧要能入站路由）
        api("/v1/agents", {"agent_id": "cli", "name": "cli", "capabilities": []})

        # S1 get nodes（默认 --config 寻址 + 鉴权）
        rc, out, err = run_cli("get", "nodes")
        check("S1 get nodes 表格含本机行",
              rc == 0 and "v05-cli-smoke" in out and NODE_ID in out and "在线" in out,
              f"rc={rc} {out[:150]}{err[:150]}")

        # S2 get agents
        rc, out, _ = run_cli("get", "agents")
        ok_all = rc == 0 and "worker" in out and "cap:file" in out and "在线" in out
        rc2, out2, _ = run_cli("get", "agents", "--requires", "cap:shell:safe")
        ok_req = rc2 == 0 and "worker" in out2
        rc3, out3, _ = run_cli("get", "agents", "--requires", "cap:nonexistent", "--online")
        ok_empty = rc3 == 0 and "无匹配" in out3
        rc4, out4, _ = run_cli("get", "agents", "--online")
        ok_online = rc4 == 0 and "worker" in out4 and "离线" not in out4
        check("S2 get agents（在线视图/--requires 服务端过滤/空结果）",
              ok_all and ok_req and ok_empty and ok_online,
              f"{out[:100]} | {out2[:100]} | {out3[:60]} | {out4[:100]}")

        # S3 开源构建不暴露 /v1/tasks（任务引擎属商业版）
        try:
            api("/v1/tasks")
            s3 = False
        except urllib.error.HTTPError as e:
            s3 = e.code == 404
        check("S3 开源构建 /v1/tasks 不暴露（404）", s3)

        # S4 apply --dry-run：打印请求体且不发流量
        spec = {"topic": "task_spec", "task_id": "11111111-2222-3333-4444-555566667777",
                "seq": 1, "type": "demo.echo", "intent": "cli-smoke 冒烟任务",
                "params": {"hello": "world", "stage_sleep": 0.2},
                "reconcile": {"mode": "one_shot", "interval_s": 5},
                "priority": 100, "correlation": CORR}
        before = history_count()
        with open(TASK, "w") as f:
            json.dump(spec, f)
        rc, out, err = run_cli("apply", "-f", TASK, "--to-agent", "worker", "--dry-run")
        check("S4 apply --dry-run 打印请求体且不发送",
              rc == 0 and "[dry-run]" in out and "task_spec" in out
              and '"to": "' + NODE_ID + '"' in out and history_count() == before,
              f"rc={rc} {out[:200]}{err[:150]}")

        # S5 apply 实发 + /v1/tasks 出现该 correlation 并到达终态
        tid2 = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        spec2 = dict(spec); spec2["task_id"] = tid2
        with open(TASK, "w") as f:
            json.dump(spec2, f)
        rc, out, err = run_cli("apply", "-f", TASK, "--to-agent", "worker")
        got_done = False
        deadline = time.time() + 20
        while time.time() < deadline and not got_done:
            got_done = history_has("task_succeeded", tid2)
            if not got_done:
                time.sleep(0.3)
        check("S5 apply 实发：msg_id 输出且执行器回报 task_succeeded 入 history",
              rc == 0 and "msg_id:" in out and tid2 in out and got_done,
              f"rc={rc} got_done={got_done} {out[:200]}{err[:150]}")

        # S6 logs 单次：逐行 seq/方向/摘要
        rc, out, err = run_cli("logs", "--limit", "100")
        has_seq = any(line.startswith("#") and "->" in line for line in out.splitlines())
        check("S6 logs 单次拉取逐行输出", rc == 0 and has_seq and "topic=task_spec" in out,
              f"rc={rc} {out[:200]}{err[:150]}")

        # S7 logs --follow：捕获新事件 + SIGINT 干净退出
        lines = []
        follow_cmd = [BIN, "--config", CFG, "logs", "--follow", "--limit", "5"]
        follow = subprocess.Popen(follow_cmd,
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        assert follow.stdout is not None
        threading.Thread(target=lambda: [lines.append(l) for l in follow.stdout],
                         daemon=True).start()
        time.sleep(1.5)
        api("/v1/send", {"from": NODE_ID, "to": NODE_ID, "type": "text", "action": "send",
                         "from_agent": "cli", "to_agent": "worker",
                         "payload": {"topic": "probe", "content": "follow-marker-77"}})
        got = False
        deadline = time.time() + 10
        while time.time() < deadline and not got:
            got = any("follow-marker-77" in l for l in lines)
            time.sleep(0.2)
        follow.send_signal(signal.SIGINT)
        try:
            frc = follow.wait(timeout=5)
        except subprocess.TimeoutExpired:
            follow.kill(); frc = -1
        check("S7 logs --follow 增量捕获新事件且 SIGINT 干净退出",
              got and frc == 0 and any("已退出跟随" in l for l in lines),
              f"got={got} rc={frc} lines={len(lines)}")
        follow = None

        # S8 无配置文件：友好错误提示 agentlink init
        os.makedirs(EMPTY_DIR, exist_ok=True)
        rc, out, err = run_cli("get", "nodes", cfg=None, cwd=EMPTY_DIR)
        check("S8 缺配置友好报错提示 agentlink init",
              rc != 0 and "agentlink init" in (err + out),
              f"rc={rc} err={err[:200]}")

        # S9 --url/--token 全覆盖：不依赖任何配置文件
        rc, out, err = run_cli("get", "nodes", "--url", BASE, "--token", TOKEN,
                               cfg=None, cwd=EMPTY_DIR)
        check("S9 --url/--token 覆盖免配置", rc == 0 and NODE_ID in out,
              f"rc={rc} {err[:200]}")

        # S10 completions：bash/zsh 脚本可产（stdout 非空 + 特征行）
        rc, out, err = run_cli("completions", "bash", cfg=None, cwd=EMPTY_DIR)
        check("S10a completions bash 脚本产物",
              rc == 0 and "_agentlink()" in out, f"rc={rc} err={err[:120]}")
        rc, out, err = run_cli("completions", "zsh", cfg=None, cwd=EMPTY_DIR)
        check("S10b completions zsh 脚本产物",
              rc == 0 and "#compdef agentlink" in out, f"rc={rc} err={err[:120]}")

        # S11 top 快照：聚合 health/peers（本机行 + 色标；无 tty 时无 ANSI 无干扰）
        rc, out, err = run_cli("top", "--url", BASE, "--token", TOKEN,
                               cfg=None, cwd=EMPTY_DIR)
        check("S11 top 快照（本机行+CPU 列+熔断列）",
              rc == 0 and "agentlink top" in out and NODE_ID in out
              and "CPU%" in out and "熔断" in out,
              f"rc={rc} {err[:200]}")
        # S12 top --watch 快速刷新：~2.2s 内产出 ≥2 帧快照（帧头 agentlink top 计数）
        try:
            p = subprocess.Popen([BIN, "top", "--watch", "1", "--url", BASE,
                                  "--token", TOKEN],
                                 cwd=EMPTY_DIR, stdout=subprocess.PIPE,
                                 stderr=subprocess.DEVNULL, text=True)
            time.sleep(2.2)
            p.send_signal(signal.SIGINT)
            out = p.communicate(timeout=3)[0]
            frames = out.count("agentlink top")
            check("S12 top --watch 秒级刷新", frames >= 2, f"frames={frames}")
        except Exception as e:
            check("S12 top --watch 秒级刷新", False, str(e)[:120])
    finally:
        for p in (executor, follow, node):
            if p is None:
                continue
            try:
                p.terminate(); p.wait(timeout=5)
            except Exception:
                try: p.kill()
                except Exception: pass
        shutil.rmtree(D, ignore_errors=True)
        shutil.rmtree(SCRATCH, ignore_errors=True)

    n = sum(1 for _, ok in results if ok)
    total = len(results)
    print(f"\n== {n}/{total} PASS ==")
    print(f"SMOKE_RESULT {n}/{total}")
    sys.exit(0 if n == total else 1)

if __name__ == "__main__":
    main()
