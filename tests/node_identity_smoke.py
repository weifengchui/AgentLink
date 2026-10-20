#!/usr/bin/env python3
"""机器身份（node_id）稳定性冒烟测试（2026-10-20 引入）。

守住的语义：**同一台机器重启后 node_id 永远不变**。
  1. 配置文件被改写/重建（node_id 换成了随机新值），同 data_dir 重启 →
     以 data_dir/node-identity.json 的身份锚点为准，并把配置修回；
  2. 换一个发行目录（新的 agentlink.json），同一台机器重新 init →
     仍然沿用首次生成的身份，不会变成网格里的“新节点”。

隔离端口 18960/18961 + 独立 data_dir，绝不碰生产节点(8765)。
用法: python3 tests/node_identity_smoke.py  (需先 cargo build --release)
可用 AGENTLINK_BIN 指定二进制路径。
"""
import json, os, shutil, signal, subprocess, tempfile, time, urllib.request

SCRATCH = os.environ.get("AL_SCRATCH") or os.path.join(
    tempfile.gettempdir(), "agentlink-node-identity-smoke")
CANDIDATES = [
    os.environ.get("AGENTLINK_BIN"),
    os.path.expanduser("~/agentlink-target/release/agentlink"),
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                 "target", "release", "agentlink"),
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                 "target", "debug", "agentlink"),
]
BIN = next((p for p in CANDIDATES if p and os.path.exists(p)), None)
if not BIN:
    raise SystemExit("找不到 agentlink 二进制：先 cargo build --release，或设 AGENTLINK_BIN")

D = os.path.join(SCRATCH, "drift")
DATA = os.path.join(D, "data")
CFG = os.path.join(D, "agentlink.json")
PORT = 18960
TOKEN = "identity-smoke-token-0123456789"
API = f"http://127.0.0.1:{PORT}"

results = []


def check(name, cond, detail=""):
    results.append(f"{'PASS' if cond else 'FAIL'}  {name}  {detail}".rstrip())


OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def health():
    try:
        with OPENER.open(API + "/v1/health", timeout=3) as r:
            return json.loads(r.read())
    except Exception:
        return {}


def write_cfg(node_id):
    cfg = {
        "v": 1, "node_id": node_id, "name": "identity-smoke", "listen_port": PORT,
        "data_dir": DATA,
        "discovery": {"enabled": False, "methods": [], "static_peers": []},
        "security": {"mode": "open", "cluster_token": "identity-smoke-cluster"},
        "local_api": {"host": "127.0.0.1", "token": TOKEN},
        "delivery": {"persist": True},
        "log": {"level": "warn", "format": "text", "file": ""},
    }
    os.makedirs(D, exist_ok=True)
    with open(CFG, "w", encoding="utf-8") as f:
        json.dump(cfg, f)


def boot(env=None):
    logf = open(os.path.join(SCRATCH, "node.log"), "ab")
    p = subprocess.Popen([BIN, "run", "--config", CFG, "--no-open"],
                         stdout=logf, stderr=subprocess.STDOUT, env=env)
    for _ in range(80):
        h = health()
        if h.get("ok"):
            return p, h
        time.sleep(0.25)
    raise RuntimeError("node did not boot; see node.log")


def stop(proc):
    if proc and proc.poll() is None:
        proc.send_signal(signal.SIGKILL)
        proc.wait()


def run_cli(args, env=None):
    r = subprocess.run([BIN] + args, capture_output=True, text=True, env=env)
    if r.returncode != 0:
        raise RuntimeError(f"{args} failed: {r.stdout}\n{r.stderr}")
    return r.stdout


def shown_id(out):
    for line in out.splitlines():
        if "node_id:" in line:
            return line.split("node_id:", 1)[1].strip()
    return ""


shutil.rmtree(SCRATCH, ignore_errors=True)
os.makedirs(SCRATCH, exist_ok=True)
proc = None
try:
    # ---- 1) config drift: config says a new id, anchor must win ----------
    original = "a1b2c3d4e5f6"
    write_cfg(original)
    proc, h = boot()
    check("首次启动 node_id 正确", h.get("node_id") == original, str(h.get("node_id")))
    anchor = os.path.join(DATA, "node-identity.json")
    check("身份锚点已落盘", os.path.exists(anchor), anchor)
    stop(proc)
    proc = None

    # Simulate a rebuilt/overwritten config with a fresh random identity.
    write_cfg("ffeeddccbbaa")
    proc, h = boot()
    check("配置漂移后重启仍沿用原身份", h.get("node_id") == original, str(h.get("node_id")))
    on_disk = json.load(open(CFG, encoding="utf-8")).get("node_id")
    check("漂移已被就地修回并落盘", on_disk == original, str(on_disk))
    stop(proc)
    proc = None

    # ---- 补救入口：配置 + 锚点都已经是错误身份时改回旧身份 ----------
    def write_anchor(node_id):
        os.makedirs(DATA, exist_ok=True)
        with open(os.path.join(DATA, "node-identity.json"), "w", encoding="utf-8") as f:
            json.dump({"v": 1, "node_id": node_id}, f)

    wrong = "111111111111"
    bdir = os.path.join(D, "backups", "v0.5.9_test")
    os.makedirs(bdir, exist_ok=True)
    backup_cfg = json.load(open(CFG, encoding="utf-8"))
    backup_cfg["node_id"] = original
    with open(os.path.join(bdir, "agentlink.json"), "w", encoding="utf-8") as f:
        json.dump(backup_cfg, f)

    # 模拟“已经用错误身份跑过一次”：配置和锚点都指向新 id。
    write_cfg(wrong)
    write_anchor(wrong)
    out = run_cli(["--config", CFG, "identity", "--set", original])
    check("identity --set 输出恢复结果", original in out and wrong in out, out.strip()[:160])
    proc, h = boot()
    check("--set 后重启身份正确", h.get("node_id") == original, str(h.get("node_id")))
    check("--set 同时修正锚点",
          json.load(open(os.path.join(DATA, "node-identity.json")))["node_id"] == original)
    stop(proc)
    proc = None

    # 再次模拟漂移，这次让 --recover 从 backups/ 自动找旧身份。
    write_cfg(wrong)
    write_anchor(wrong)
    out = run_cli(["--config", CFG, "identity", "--recover"])
    check("identity --recover 采用备份里的旧身份", original in out, out.strip()[:200])
    proc, h = boot()
    check("--recover 后重启身份正确", h.get("node_id") == original, str(h.get("node_id")))
    stop(proc)
    proc = None

    # ---- 2) new release folder / re-init keeps the machine identity ------
    xdg = os.path.join(SCRATCH, "xdg")
    env = dict(os.environ, XDG_DATA_HOME=xdg)
    first = os.path.join(SCRATCH, "v1", "agentlink.json")
    second = os.path.join(SCRATCH, "v2", "agentlink.json")
    id1 = shown_id(run_cli(["--config", first, "init", "--force", "--open",
                            "--name", "identity-init-test"], env=env))
    id2 = shown_id(run_cli(["--config", second, "init", "--force", "--open",
                            "--name", "identity-init-test"], env=env))
    check("首次 init 生成身份", bool(id1), id1)
    check("换目录重新 init 不换身份", id1 == id2, f"{id1} -> {id2}")
    shared_anchor = os.path.join(xdg, "agentlink", "node-identity.json")
    pinned = ""
    if os.path.exists(shared_anchor):
        pinned = json.load(open(shared_anchor, encoding="utf-8")).get("node_id", "")
    check("共享锚点记录同一身份", pinned == id1, f"{pinned} vs {id1}")
finally:
    stop(proc)

bad = [r for r in results if r.startswith("FAIL")]
print("\n".join(results))
print(f"\n冒烟 {len(results) - len(bad)}/{len(results)} PASS")
raise SystemExit(1 if bad else 0)
