#!/usr/bin/env python3
"""配置位置隔离冒烟测试（2026-10-20 引入）。

守住的语义：**升级不覆盖配置**——配置住在用户配置目录，不在发行目录里。

  1. 发行目录里的旧配置会被自动迁移到用户配置目录（node_id 不变）；
  2. 迁移后发行目录里换一份新配置/新二进制，节点仍读用户配置目录那份；
  3. 相对 data_dir 迁移后锚回旧目录（数据不搬家）；
  4. 显式 --config 时完全不介入（不迁移、不改路径）；
  5. `config --path` 输出的就是生效路径。

隔离端口 18962 + 独立 HOME/XDG，绝不碰生产节点(8765)。
用法: python3 tests/config_location_smoke.py  (需先 cargo build --release)
可用 AGENTLINK_BIN 指定二进制路径。
"""
import json, os, shutil, signal, subprocess, sys, tempfile, time, urllib.request

SCRATCH = os.environ.get("AL_SCRATCH") or os.path.join(
    tempfile.gettempdir(), "agentlink-config-location-smoke")
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

RELEASE = os.path.join(SCRATCH, "release")   # 模拟发行目录
STABLE_DIR = os.path.join(SCRATCH, "xdg", "agentlink")  # 模拟用户配置目录
STABLE_CFG = os.path.join(STABLE_DIR, "agentlink.json")
PORT = 18962
TOKEN = "cfg-location-smoke-token-0123456789"
NODE = "beadfeed0001"
API = f"http://127.0.0.1:{PORT}"

results = []


def check(name, cond, detail=""):
    results.append(f"{'PASS' if cond else 'FAIL'}  {name}  {detail}".rstrip())


OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def env():
    """模拟用户环境：XDG_CONFIG_HOME 指向稳定配置目录。"""
    e = dict(os.environ)
    e["XDG_CONFIG_HOME"] = os.path.join(SCRATCH, "xdg")
    e.pop("AGENTLINK_CONFIG", None)
    return e


def health():
    try:
        with OPENER.open(API + "/v1/health", timeout=3) as r:
            return json.loads(r.read())
    except Exception:
        return {}


def write_cfg(path, node_id, data_dir):
    cfg = {
        "v": 1, "node_id": node_id, "name": "cfg-location-smoke",
        "listen_port": PORT, "data_dir": data_dir,
        "discovery": {"enabled": False, "methods": [], "static_peers": []},
        "security": {"mode": "open", "cluster_token": "cfg-location-cluster"},
        "local_api": {"host": "127.0.0.1", "token": TOKEN},
        "delivery": {"persist": True},
        "log": {"level": "warn", "format": "text", "file": ""},
    }
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f)


def boot(cfg_path, cwd=None, extra=None):
    logf = open(os.path.join(SCRATCH, "node.log"), "ab")
    args = [BIN, "--config", cfg_path, "run", "--no-open"]
    if extra:
        args = [BIN] + extra
    p = subprocess.Popen(args, stdout=logf, stderr=subprocess.STDOUT,
                         env=env(), cwd=cwd)
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


def run_cli(args, cwd=None):
    r = subprocess.run([BIN] + args, capture_output=True, text=True, env=env(), cwd=cwd)
    return r


shutil.rmtree(SCRATCH, ignore_errors=True)
os.makedirs(RELEASE, exist_ok=True)
os.makedirs(STABLE_DIR, exist_ok=True)
proc = None
try:
    # ---- 1) 发行目录里的旧配置 → 自动迁移到稳定位置 --------------------
    legacy_cfg = os.path.join(RELEASE, "agentlink.json")
    data_dir = os.path.join(RELEASE, "data")
    write_cfg(legacy_cfg, NODE, data_dir)

    # 无 --config，从发行目录启动 → 应自动迁移
    p = subprocess.run([BIN, "config", "--path"], capture_output=True, text=True,
                       env=env(), cwd=RELEASE)
    check("config --path 输出稳定位置",
          STABLE_CFG in p.stdout, p.stdout.strip()[:160])
    check("旧配置已迁移到稳定位置", os.path.exists(STABLE_CFG))
    if os.path.exists(STABLE_CFG):
        moved = json.load(open(STABLE_CFG, encoding="utf-8"))
        check("迁移保住 node_id", moved.get("node_id") == NODE, str(moved.get("node_id")))
        check("相对 data_dir 锚回旧目录（数据不搬家）",
              moved.get("data_dir") == data_dir.replace("/", os.sep),
              str(moved.get("data_dir")))

    # 迁移后：发行目录里换一份"新版本"配置（模拟升级覆盖），稳定位置不动
    write_cfg(legacy_cfg, "ffffffffffff", os.path.join(RELEASE, "data2"))
    proc, h = boot(STABLE_CFG)
    check("稳定位置的配置优先，node_id 不受发行目录改动影响",
          h.get("node_id") == NODE, str(h.get("node_id")))
    stop(proc)
    proc = None

    # ---- 2) 显式 --config 时不迁移、不改路径 ---------------------------
    explicit = os.path.join(SCRATCH, "explicit.json")
    write_cfg(explicit, "cccccccccccc", os.path.join(SCRATCH, "data3"))
    p = run_cli(["--config", explicit, "config", "--path"])
    check("显式 --config 原样生效",
          explicit in p.stdout and STABLE_CFG not in p.stdout,
          p.stdout.strip()[:160])
    # 显式路径的配置文件不该被迁移走
    check("显式 --config 不触发迁移", os.path.exists(explicit))
    check("显式 --config 不影响稳定位置",
          json.load(open(STABLE_CFG, encoding="utf-8")).get("node_id") == NODE)

    # ---- 3) 稳定位置已存在时，不去碰发行目录的旧文件 -------------------
    before = os.path.getmtime(legacy_cfg)
    run_cli(["config", "--path"], cwd=RELEASE)
    check("稳定位置存在时不迁移发行目录旧文件",
          os.path.getmtime(legacy_cfg) == before)

    # ---- 4) config --migrate 幂等 ---------------------------------------
    p = run_cli(["config", "--migrate"], cwd=RELEASE)
    check("稳定位置已就绪时 --migrate 报无需迁移",
          "无需迁移" in p.stdout, p.stdout.strip()[:160])
finally:
    stop(proc)

bad = [r for r in results if r.startswith("FAIL")]
print("\n".join(results))
print(f"\n冒烟 {len(results) - len(bad)}/{len(results)} PASS")
raise SystemExit(1 if bad else 0)
