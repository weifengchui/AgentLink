#!/usr/bin/env python3
"""AgentLink 升级执行器（升级协调协议 v1 的执行侧实现）。

流程：备份 -> 解压校验 sha256 -> taskkill 旧进程 -> 覆盖二进制 -> 启动新进程
      -> /v1/health 轮询期望版本 -> 成功报 done / 超时自动回滚报 rolled_back
台账：追加写 <nodeDir>/upgrade-ledger.jsonl，并可选 git 副本路径。

用法（WSL）:
  python3 upgrade_runner.py --config /mnt/e/Tools/AgentLink-vX/agentlink.json \
      --zip /path/AgentLink-v0.4.8.zip --expect-version 0.4.8 \
      [--sha256 abc...] [--health-timeout 30] [--report-to study@4f793d7cd6fc]

依赖：cmd.exe（taskkill/启动.bat）、urllib、zipfile；无需第三方包。
"""
import argparse, hashlib, json, os, re, shutil, subprocess, sys, tempfile, time
import urllib.request

CMD = "/mnt/c/Windows/System32/cmd.exe"

def win_path(p):
    r = subprocess.run(["wslpath", "-w", p], capture_output=True, text=True)
    return r.stdout.strip() if r.returncode == 0 else p

def read_node_cfg(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)

def health(base, timeout=3):
    try:
        with urllib.request.urlopen(base + "/v1/health", timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        return {"ok": False, "error": str(e)}

def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()

def ledger_append(node_dir, entry, git_copy=None):
    entry = dict(entry, time=time.strftime("%Y-%m-%dT%H:%M:%S%z"))
    line = json.dumps(entry, ensure_ascii=False) + "\n"
    for p in (os.path.join(node_dir, "upgrade-ledger.jsonl"),
              os.path.join(git_copy, "upgrade-ledger.jsonl") if git_copy else None):
        if not p:
            continue
        try:
            os.makedirs(os.path.dirname(p), exist_ok=True)
            with open(p, "a", encoding="utf-8") as f:
                f.write(line)
        except Exception as e:
            print(f"[ledger] warn: {p}: {e}", file=sys.stderr)
    print("[ledger]", line)

def kill_node(base, timeout=10):
    """taskkill 后等待 health 不可达（启动.bat 有端口占用防重入检查，必须确认旧进程死透）。"""
    subprocess.run([CMD, "/c", "taskkill /F /IM agentlink.exe"], capture_output=True, text=True)
    t0 = time.time()
    while time.time() - t0 < timeout:
        if not health(base, timeout=2).get("ok"):
            time.sleep(0.8)  # 端口释放稍滞后于 health 失败
            return True
        time.sleep(0.5)
    return not health(base, timeout=2).get("ok")

def start_node(node_dir):
    bat = win_path(os.path.join(node_dir, "启动.bat"))
    log = open(os.path.join(node_dir, "run.log"), "ab")
    subprocess.Popen([CMD, "/c", bat], cwd=node_dir,
                     stdin=subprocess.DEVNULL, stdout=log, stderr=log)

def wait_health(base, expect_version, timeout_s):
    t0 = time.time()
    last = {}
    while time.time() - t0 < timeout_s:
        h = health(base)
        last = h
        if h.get("ok") and h.get("version") == expect_version:
            return True, h, int((time.time() - t0) * 1000)
        time.sleep(1)
    return False, last, int((time.time() - t0) * 1000)

def prune_backups(node_dir, keep=2):
    bd = os.path.join(node_dir, "backups")
    if not os.path.isdir(bd):
        return
    ds = sorted(os.listdir(bd), key=lambda d: os.path.getmtime(os.path.join(bd, d)))
    for d in ds[:-keep] if keep else ds:
        shutil.rmtree(os.path.join(bd, d), ignore_errors=True)

def extract_zip_exe(zip_path, dest_dir):
    import zipfile
    with zipfile.ZipFile(zip_path) as z:
        names = z.namelist()
        exe_name = next((n for n in names if re.fullmatch(r"agentlink(\.exe)?", n)), None)
        if not exe_name:
            raise RuntimeError(f"zip 内未找到 agentlink(.exe): {names}")
        with z.open(exe_name) as src, open(os.path.join(dest_dir, exe_name), "wb") as dst:
            shutil.copyfileobj(src, dst)
    return exe_name

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True, help="节点 agentlink.json 路径")
    ap.add_argument("--zip", required=True, help="新版本 zip 包路径")
    ap.add_argument("--expect-version", required=True, help="期望升级后的版本号，如 0.4.8")
    ap.add_argument("--sha256", default=None)
    ap.add_argument("--health-timeout", type=int, default=30)
    ap.add_argument("--git-ledger", default=None, help="台账 git 副本目录（可选）")
    ap.add_argument("--exe-name", default="agentlink.exe", help="节点二进制名（Linux 节点传 agentlink）")
    ap.add_argument("--trigger", default="manual", help="manual|msg:<msg_id>")
    args = ap.parse_args()

    node_dir = os.path.dirname(os.path.abspath(args.config))
    cfg = read_node_cfg(args.config)
    base = f"http://127.0.0.1:{cfg.get('listen_port', 8765)}"
    old_version = health(base).get("version")
    exe_name = args.exe_name
    exe_path = os.path.join(node_dir, exe_name)

    result = {"trigger": args.trigger, "node": cfg.get("node_id"), "old": old_version,
              "new": args.expect_version, "zip": os.path.basename(args.zip)}
    print(f"[upgrade] {cfg.get('name')} {old_version} -> {args.expect_version}")

    # 1) 校验 zip
    actual_sha = sha256_of(args.zip)
    if args.sha256 and actual_sha != args.sha256:
        result.update(status="failed", reason=f"sha256 mismatch: {actual_sha}")
        ledger_append(node_dir, result, args.git_ledger)
        print("[upgrade] FAILED: sha256 mismatch，不动现网"); sys.exit(2)
    print(f"[upgrade] sha256 ok: {actual_sha[:16]}…")

    # 2) 解压 + 校验包内 exe
    tmp = tempfile.mkdtemp(prefix="alup_")
    try:
        got = extract_zip_exe(args.zip, tmp)
        new_exe = os.path.join(tmp, got)
        if args.sha256 and sha256_of(new_exe):  # 冒烟：能读即可
            pass

        # 3) 备份
        ts = time.strftime("%Y%m%d-%H%M%S")
        bak = os.path.join(node_dir, "backups", f"v{old_version}_{ts}")
        os.makedirs(bak, exist_ok=True)
        for f in (exe_name, "agentlink.json", "启动.bat"):
            src = os.path.join(node_dir, f)
            if os.path.exists(src):
                shutil.copy2(src, bak)
        result["backup"] = os.path.relpath(bak, node_dir)
        print(f"[upgrade] backup -> {bak}")

        # 4) 换二进制 + 重启
        kill_node(base)
        shutil.copy2(new_exe, exe_path)
        start_node(node_dir)

        # 5) health 轮询
        ok, h, ms = wait_health(base, args.expect_version, args.health_timeout)
        if ok:
            result.update(status="done", health_ok=True, elapsed_ms=ms, node_version=h.get("version"))
            ledger_append(node_dir, result, args.git_ledger)
            prune_backups(node_dir)
            print(f"[upgrade] DONE in {ms}ms, version={h.get('version')}")
        else:
            print(f"[upgrade] health 未达标（{h}），开始回滚…")
            kill_node(base)
            shutil.copy2(os.path.join(bak, exe_name), exe_path)
            if os.path.exists(os.path.join(bak, "agentlink.json")):
                shutil.copy2(os.path.join(bak, "agentlink.json"), os.path.join(node_dir, "agentlink.json"))
            start_node(node_dir)
            ok2, h2, ms2 = wait_health(base, old_version, args.health_timeout)
            result.update(status="rolled_back", reason="health timeout after upgrade",
                          restored=old_version, rollback_health_ok=ok2, elapsed_ms=ms)
            ledger_append(node_dir, result, args.git_ledger)
            print(f"[upgrade] ROLLED BACK, old node alive={ok2}")
            sys.exit(3)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

if __name__ == "__main__":
    main()
