#!/usr/bin/env python3
"""强制 mTLS `require` 模式冒烟（v0.6 P1）：隔离端口，绝不碰生产 8765。

场景：
  S0 预生成集群共享 CA（boot 节点）；CLI `mtls --enable --require` 落盘验证。
  S1 A 开 require（含明文静态条目）：出站 A 本地拒拨（conn: dial refused）、
     入站明文拨入被 A 拒（peer link refused）；B（未开 require）明文拨号被拒回；
     本地 API 不受影响（`/v1/health` 仍通 —— require 只管对等通道）。
  S2 双方静态条目改 wss:// → require 模式下正常建连 + 消息经 wss 送达。
  S3 require 关闭（机会式）+ 对端无 mTLS → 明文互通（灰度共存零回归）。

退出码 0 = 全过。用法: python3 tests/mtls_require_smoke.py <agentlink-binary>
"""
import json, os, shutil, signal, socket, subprocess, sys, time, urllib.request

BIN = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser(
    "~/agentlink-target/release/agentlink")
assert os.path.exists(BIN), f"binary not found: {BIN}"

SCRATCH = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch")
ROOT = os.path.join(SCRATCH, "al-mtls-require")
PORT_A, MTLS_A = 18911, 18912
PORT_B, MTLS_B = 18921, 18922
TOKEN = "mtls-require-token"
NODE_A, NODE_B = "aaaaaaaaaaaa", "bbbbbbbbbbbb"
CLUSTER = "mtls-require-cluster"

passed = 0
failed = 0
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
        headers={"X-Auth-Token": TOKEN, "Content-Type": "application/json"},
        method="POST" if body is not None else "GET")
    with urllib.request.urlopen(req, timeout=8) as r:
        return r.status, json.loads(r.read().decode())


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


def port_open(port):
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) == 0


def write_cfg(key, *, peers, require, mtls=True, log_level="info"):
    port, mtls_port = (PORT_A, MTLS_A) if key == "a" else (PORT_B, MTLS_B)
    cfg = {
        "v": 1, "node_id": NODE_A if key == "a" else NODE_B,
        "name": f"require-{key}", "listen_port": port,
        "data_dir": os.path.join(ROOT, key),
        "history": {"enabled": True, "len": 500, "file": "history.jsonl"},
        "discovery": {"enabled": False, "static_peers": list(peers)},
        "security": {"mode": "strict", "cluster_token": CLUSTER},
        "local_api": {"host": "0.0.0.0", "token": TOKEN},
        "web": {"auto_open": False},
        "log": {"level": log_level, "format": "text"},
    }
    if mtls:
        cfg["mtls"] = {"enabled": True, "listen_port": mtls_port,
                       "ca_cert": SHARED_CA, "ca_key": SHARED_KEY,
                       "require": require}
    json.dump(cfg, open(os.path.join(ROOT, f"{key}.json"), "w"))


procs = []
def start(key):
    log = open(os.path.join(ROOT, f"{key}.log"), "w")
    p = subprocess.Popen([BIN, "--config", os.path.join(ROOT, f"{key}.json"),
                          "run", "--no-open"],
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


def any_peer_online(key, peer_id, tries=40):
    base = f"http://127.0.0.1:{PORT_A if key == 'a' else PORT_B}"
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


SHARED_DIR = os.path.join(ROOT, "shared")
SHARED_CA = os.path.join(SHARED_DIR, "ca.pem")
SHARED_KEY = os.path.join(SHARED_DIR, "ca.key")
BASE_A = f"http://127.0.0.1:{PORT_A}"
BASE_B = f"http://127.0.0.1:{PORT_B}"


def main():
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(SHARED_DIR, exist_ok=True)
    for p in (PORT_A, MTLS_A, PORT_B, MTLS_B):
        assert port_free(p), f"port {p} busy"

    # ---- S0: 预生成集群共享 CA（boot 节点，不参与运行） --------------------
    boot_cfg = os.path.join(ROOT, "boot.json")
    json.dump({"v": 1, "node_id": "cccccccccccc", "name": "ca-boot",
               "listen_port": 18999, "data_dir": os.path.join(ROOT, "boot"),
               "mtls": {"enabled": False, "ca_cert": SHARED_CA, "ca_key": SHARED_KEY}},
              open(boot_cfg, "w"))
    subprocess.run([BIN, "--config", boot_cfg, "mtls", "--rotate"],
                   capture_output=True, timeout=20, check=True)
    check("S0 集群共享 CA 就绪", os.path.exists(SHARED_CA) and os.path.exists(SHARED_KEY))

    # ---- S0b: CLI `mtls --enable --require` 将强制模式写入配置 ------------
    cli = subprocess.run([BIN, "--config", boot_cfg, "mtls", "--enable", "--require"],
                         capture_output=True, timeout=20)
    cli_cfg = json.load(open(boot_cfg))
    check("S0b mtls --enable --require 写入配置",
          cli.returncode == 0 and cli_cfg.get("mtls", {}).get("require") is True
          and cli_cfg.get("mtls", {}).get("enabled") is True,
          (cli.stdout + cli.stderr).decode(errors="replace")[-200:])
    miss = subprocess.run([BIN, "--config", boot_cfg, "mtls", "--require"],
                          capture_output=True, timeout=20)
    check("S0b `--require` 单独使用被拒绝（需先 --enable）", miss.returncode != 0)

    # ---- S1: A 开 require；A 出站只认 wss（本地拒拨）、入站拒明文 --------
    # A 也持有指向 B 的明文静态条目：验证 require 下出站拒拨（dial refused）。
    write_cfg("a", peers=[f"127.0.0.1:{PORT_B}"], require=True, log_level="debug")
    write_cfg("b", peers=[f"127.0.0.1:{PORT_A}"], require=False, log_level="debug")
    start("a"); start("b")
    ok = wait_health(BASE_A) and wait_health(BASE_B)
    check("S1 双节点启动健康（本地 API 不受 require 影响）", ok)
    if not ok:
        stop_all(); return
    time.sleep(5)  # 给双向多轮明文尝试 → 拒绝的往返留时间
    check("S1 A 日志含出站拒拨（conn: dial refused）",
          "conn: dial refused" in log_text("a"), log_text("a")[-300:])
    check("S1 A 日志含入站明文拒收（peer link refused）",
          "peer link refused" in log_text("a"), log_text("a")[-300:])
    check("S1 B 的明文拨号被 A 拒回（conn: dial failed）",
          "conn: dial failed" in log_text("b"), log_text("b")[-300:])
    # 本地域（/v1/health、/v1/peers）与对等域分离：require 不挡管理面。
    try:
        st, _ = api(BASE_A, "/v1/peers")
        local_ok = st == 200
    except Exception as e:
        local_ok = False
    check("S1 A 管理面 /v1/peers 正常响应", local_ok)
    check("S1 双端对等链路未建立（offline）",
          not any_peer_online("a", NODE_B) and not any_peer_online("b", NODE_A))
    stop_all()

    # ---- S2: require 模式 + wss:// 静态条目 → 正常建连且消息可达 ----------
    write_cfg("a", peers=[f"wss://127.0.0.1:{MTLS_B}"], require=True)
    write_cfg("b", peers=[f"wss://127.0.0.1:{MTLS_A}"], require=False)
    start("a"); start("b")
    ok = wait_health(BASE_A) and wait_health(BASE_B)
    check("S2 require 模式（wss:// 静态条目）双节点健康", ok)
    if not ok:
        stop_all(); return
    online = any_peer_online("a", NODE_B) or any_peer_online("b", NODE_A)
    check("S2 require 模式下 wss:// 建连对端 online", online)
    sent = False
    if online:
        try:
            api(BASE_A, "/v1/send", {"to": NODE_B, "type": "text",
                                     "action": "send",
                                     "payload": {"content": "require-mode-ok"}})
        except Exception as e:
            print("send error", e)
        for _ in range(40):
            try:
                _, h = api(BASE_B, "/v1/history")
                if any("require-mode-ok" in json.dumps(e, ensure_ascii=False)
                       for e in h.get("history", [])):
                    sent = True
                    break
            except Exception:
                pass
            time.sleep(0.25)
    check("S2 消息经 wss:// 送达对端 history", sent)
    stop_all()

    # ---- S3: require 关闭（机会式）+ 对端无 mTLS → 明文互通零回归 --------
    shutil.rmtree(os.path.join(ROOT, "a"), ignore_errors=True)
    shutil.rmtree(os.path.join(ROOT, "b"), ignore_errors=True)
    write_cfg("a", peers=[f"127.0.0.1:{PORT_B}"], require=False)
    write_cfg("b", peers=[], require=False, mtls=False)
    start("a"); start("b")
    ok = wait_health(BASE_A) and wait_health(BASE_B)
    pr = any_peer_online("a", NODE_B) if ok else False
    check("S3 机会式（require=false）+ 对端无 mTLS 明文互通（零回归）", pr)
    stop_all()


try:
    main()
finally:
    stop_all()

print(f"SMOKE_RESULT {passed}/{passed + failed}")
sys.exit(0 if failed == 0 else 1)
