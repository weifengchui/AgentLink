#!/usr/bin/env python3
"""新节点准入冒烟（v0.6 P1）：bootstrap token + 硬件指纹双验证。隔离端口，绝不碰生产 8765。

场景：
  S0 CLI：`agentlink admission --status` 打印 16 位 hex 本机硬件指纹。
  S1 基线：准入关闭（默认）时，仅有集群令牌的节点即可互通（零行为变化）。
  S2 双验证全开：B 携带准入令牌 + 指纹已入白名单 → 互通；
  S3 陌生节点 C（有集群令牌，无准入令牌）→ 被拒（日志含 handshake rejected (admission)）。
    注意：单机冒烟里所有节点指纹相同（同一台机器的 machine-id 一致），因此
    「指纹不在白名单」用白名单**摘除**来复现同一代码路径：B 令牌合法但白名单
    不含其指纹 → 拒；恢复白名单 → 放行（运维修复闭环）。

  S4 指纹轴：白名单不含对方指纹（令牌合法）→ 拒；恢复收录 → 通。
  S5 fail-closed：开启准入但令牌/白名单都没配 → 连合法节点也被全拒。

退出码 0 = 全过。用法: python3 tests/admission_smoke.py <agentlink-binary>
"""
import json, os, shutil, signal, subprocess, sys, time, urllib.request

BIN = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser(
    "~/agentlink-target/release/agentlink")
assert os.path.exists(BIN), f"binary not found: {BIN}"

SCRATCH = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch")
ROOT = os.path.join(SCRATCH, "al-admission")
CLUSTER = "adm-cluster-token"
BOOT = "adm-bootstrap-token"

PORTS = {"a": 18911, "b": 18921, "c": 18931}
NODES = {"a": "aaaaaaaaaaaa", "b": "bbbbbbbbbbbb", "c": "cccccccccccc"}

passed = failed = 0
def check(name, ok, detail=""):
    global passed, failed
    if ok:
        passed += 1
        print(f"PASS {name}")
    else:
        failed += 1
        print(f"FAIL {name}  {detail[:240]}")

def api(base, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(base + path, data=data,
        headers={"X-Auth-Token": "adm-local"}, method="POST" if body is not None else "GET")
    with urllib.request.urlopen(req, timeout=8) as r:
        return r.status, json.loads(r.read().decode())

def wait_health(base, tries=80):
    for _ in range(tries):
        try:
            with urllib.request.urlopen(base + "/v1/health", timeout=2) as r:
                if r.status == 200:
                    return True
        except Exception:
            time.sleep(0.25)
    return False

def wait_peer_online(base, peer_id, tries=60):
    for _ in range(tries):
        try:
            _, d = api(base, "/v1/peers")
            for pr in d.get("peers", []):
                if pr.get("node_id") == peer_id and pr.get("online"):
                    return True
        except Exception:
            pass
        time.sleep(0.25)
    return False

def wait_log(key, needle, tries=60):
    f = os.path.join(ROOT, f"{key}.log")
    for _ in range(tries):
        try:
            if needle in open(f).read():
                return True
        except Exception:
            pass
        time.sleep(0.25)
    return False

def port_free(port):
    import socket
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) != 0

def cfg_path(key):
    return os.path.join(ROOT, f"{key}.json")

def data_dir(key):
    return os.path.join(ROOT, key)

def write_cfg(key, *, peers, admission=None, mode="strict"):
    """admission: None=不写段（默认关闭）；'fa-closed'=空开关；或 dict 透传。"""
    listen = PORTS[key]
    cfg = {
        "v": 1, "node_id": NODES[key], "name": f"adm-{key}",
        "listen_port": listen, "data_dir": data_dir(key),
        "history": {"enabled": True, "len": 200, "file": "history.jsonl"},
        "discovery": {"enabled": False, "static_peers": list(peers)},
        "security": {"mode": mode, "cluster_token": CLUSTER},
        "local_api": {"host": "0.0.0.0", "token": "adm-local"},
        "web": {"auto_open": False}, "log": {"level": "info", "format": "text"},
    }
    if admission == "fail-closed":
        cfg["security"]["admission"] = {"enabled": True}
    elif isinstance(admission, dict):
        cfg["security"]["admission"] = admission
    json.dump(cfg, open(cfg_path(key), "w"))

def set_admission(key, patch):
    cfg = json.load(open(cfg_path(key)))
    cfg["security"]["admission"] = patch
    json.dump(cfg, open(cfg_path(key), "w"))

def cli(args, cwd=None):
    return subprocess.run([BIN] + args, capture_output=True, timeout=60)

procs = []
def start(key):
    log = open(os.path.join(ROOT, f"{key}.log"), "w")
    p = subprocess.Popen([BIN, "--config", cfg_path(key), "run", "--no-open"],
                         stdout=log, stderr=subprocess.STDOUT)
    procs.append((p, log))
    return p

def stop_all():
    for p, log in procs:
        try:
            p.send_signal(signal.SIGINT)
        except Exception:
            pass
    for p, log in procs:
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

def self_fingerprint(key):
    """跑一次 CLI 拿本机指纹（16 位 hex）。"""
    r = cli(["--config", cfg_path(key), "admission", "--status"])
    for line in r.stdout.decode(errors="replace").splitlines():
        s = line.strip()
        if s.startswith("self hw fp:"):
            return s.split(":", 1)[1].strip()
    return ""

def main():
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(ROOT, exist_ok=True)
    for key, port in PORTS.items():
        assert port_free(port), f"port busy for {key}"

    # S0：CLI 自检——准入默认关闭，且本机指纹为 16 位小写 hex。
    write_cfg("a", peers=[])
    r = cli(["--config", cfg_path("a"), "admission", "--status"])
    out = r.stdout.decode(errors="replace")
    check("S0 admission CLI 可用（默认 disabled）", r.returncode == 0 and "admission: disabled" in out,
          out[-200:])
    fp_b = None

    # S1 基线：准入关闭 → B 直接互通（默认行为不变）。
    write_cfg("b", peers=[f"127.0.0.1:{PORTS['a']}"])
    start("a"); start("b")
    base_a = f"http://127.0.0.1:{PORTS['a']}"
    base_b = f"http://127.0.0.1:{PORTS['b']}"
    ok = wait_health(base_a) and wait_health(base_b)
    tol = wait_peer_online(base_a, NODES["b"])
    tol = tol and wait_peer_online(base_b, NODES["a"])
    check("S1 准入关闭时互通（零行为变化）", ok and tol)
    stop_all()
    time.sleep(1)

    # S2：运维旅程——取 B 的指纹入 A 的白名单；B 记准入令牌；A 开启准入（双因素）。
    fp_b = self_fingerprint("b")
    check("S0 self hw fp 为 16 位小写 hex",
          len(fp_b) == 16 and all(c in "0123456789abcdef" for c in fp_b), fp_b or "(empty)")
    ok_a = set_admission("a", {"enabled": True, "bootstrap_token": BOOT, "allowed_hardware": [fp_b]})
    ok_b = set_admission("b", {"enabled": True, "bootstrap_token": BOOT, "allowed_hardware": [fp_b]})
    start("a"); start("b")
    tol = wait_health(base_a) and wait_health(base_b)
    tol = tol and wait_peer_online(base_a, NODES["b"])
    tol = tol and wait_peer_online(base_b, NODES["a"])
    check("S2 双验证通过：准入全开且合法节点互通", tol)
    stop_all()
    time.sleep(1)

    # S3：陌生节点 C（有集群令牌、无准入令牌）→ 被 A 拒。
    write_cfg("b", peers=[f"127.0.0.1:{PORTS['a']}"], admission={"bootstrap_token": BOOT})
    write_cfg("c", peers=[f"127.0.0.1:{PORTS['a']}"])
    start("a"); start("b"); start("c")
    for base in (base_a, base_b, f"http://127.0.0.1:{PORTS['c']}"):
        wait_health(base)
    time.sleep(3)
    check("S3 C 无准入令牌被拒（A 日志 + 成员表没有 C）",
          wait_log("a", "handshake rejected (admission)")
          and not wait_peer_online(base_a, NODES["c"], tries=4),
          log_text("a")[-200:])
    # B（合法）仍在线。
    check("S3 合法节点 B 仍互通", wait_peer_online(base_a, NODES["b"], tries=4))
    stop_all()
    time.sleep(1)

    # S4：指纹轴 —— 白名单换成不存在的指纹 → B（令牌合法）也被拒；恢复后互通。
    set_admission("a", {"enabled": True, "bootstrap_token": BOOT, "allowed_hardware": ["0123456789abcdef0"]})
    start("a"); start("b")
    wait_health(base_a); wait_health(base_b)
    time.sleep(3)
    check("S4 指纹不在白名单（虽令牌合法）仍被拒",
          wait_log("a", "硬件指纹未在准入白名单")
          and not wait_peer_online(base_a, NODES["b"], tries=4),
          log_text("a")[-200:])
    stop_all()
    time.sleep(1)
    # 运维修复闭环：把指纹重新收录 → 放行。
    set_admission("a", {"enabled": True, "bootstrap_token": BOOT, "allowed_hardware": [fp_b]})
    start("a"); start("b")
    wait_health(base_a); wait_health(base_b)
    time.sleep(2)
    check("S4 指纹恢复收录后放行", wait_peer_online(base_a, NODES["b"], tries=40))
    stop_all()
    time.sleep(1)

    # S5：fail-closed —— 开启准入但令牌/白名单都没配（手工编辑配置模拟）→ 全拒。
    set_admission("a", {"enabled": True})
    write_cfg("b", peers=[f"127.0.0.1:{PORTS['a']}"], admission={"bootstrap_token": BOOT})
    start("a"); start("b")
    wait_health(base_a); wait_health(base_b)
    time.sleep(3)
    fc = not wait_peer_online(base_a, NODES["b"], tries=2)
    check("S5 fail-closed：无令牌无白名单时全拒", fc, log_text("a")[-160:])

if __name__ == "__main__":
    try:
        main()
    finally:
        stop_all()
    print(f"SMOKE_RESULT {passed}/{passed + failed}")
    sys.exit(0 if failed == 0 else 1)
