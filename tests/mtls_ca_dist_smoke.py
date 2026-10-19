#!/usr/bin/env python3
"""CA 自动分发冒烟（v0.6 P1）：隔离端口，绝不碰生产 8765。

场景：
  S0 权威节点 A（create_ca=true）自建 CA；成员节点 B（create_ca=false）**无** CA。
  S1 双节点启动健康（B 无 CA 时以明文接入）。
  S2 B 经已认证的对等链路自动领取集群 CA（ca.pem 落盘，指纹 == A）。
  S3 B 的节点证书由**领取到的 CA**签发（openssl verify -CAfile A/ca.pem B/node.pem）。
  S4 B 领取后热启动 TLS 监听（mtls 端口可连）/ 日志含 “adopted cluster CA”。
  S5 冲突保护：自带另一套 CA 的 C 连上 A 后**不被覆盖**（指纹不变）。
  S6 成员节点无权威可达时：不建 CA、节点仍正常运行（空态不崩）。

退出码 0 = 全过。用法: python3 tests/mtls_ca_dist_smoke.py <agentlink-binary>
"""
import json, os, shutil, signal, socket, subprocess, sys, time, urllib.request

BIN = sys.argv[1] if len(sys.argv) > 1 else os.path.expanduser(
    "~/agentlink-target/release/agentlink")
assert os.path.exists(BIN), f"binary not found: {BIN}"

SCRATCH = os.path.expanduser("~/.hermes/profiles/agentlink/cache/scratch")
ROOT = os.path.join(SCRATCH, "al-mtls-ca-dist")
TOKEN = "ca-dist-token"
CLUSTER = "ca-dist-cluster"

# 隔离端口（绝不碰 8765）。
PORTS = {
    "a": (18871, 18872),
    "b": (18881, 18882),
    "c": (18891, 18892),
    "d": (18901, 18902),
}
NODES = {"a": "aaaaaaaaaaaa", "b": "bbbbbbbbbbbb",
         "c": "cccccccccccc", "d": "dddddddddddd"}

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
        headers={"X-Auth-Token": TOKEN, "Content-Type": "application/json"},
        method="POST" if body is not None else "GET")
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

def port_free(port):
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) != 0

def port_open(port):
    with socket.socket() as s:
        s.settimeout(0.5)
        return s.connect_ex(("127.0.0.1", port)) == 0

def fingerprint(path):
    r = subprocess.run(["openssl", "x509", "-in", path, "-noout", "-fingerprint", "-sha256"],
                       capture_output=True, timeout=10)
    return r.stdout.decode().strip() if r.returncode == 0 else ""

def verify(chain_ca, cert):
    r = subprocess.run(["openssl", "verify", "-CAfile", chain_ca, cert],
                       capture_output=True, timeout=10)
    return r.returncode == 0, (r.stdout + r.stderr).decode(errors="replace").strip()

def cfg_path(key):
    return os.path.join(ROOT, f"{key}.json")

def data_dir(key):
    return os.path.join(ROOT, key)

def write_cfg(key, *, create_ca, peers, enabled=True):
    listen, mtls_port = PORTS[key]
    cfg = {
        "v": 1, "node_id": NODES[key], "name": f"ca-{key}",
        "listen_port": listen, "data_dir": data_dir(key),
        "history": {"enabled": True, "len": 200, "file": "history.jsonl"},
        "discovery": {"enabled": False, "static_peers": list(peers)},
        "security": {"mode": "strict", "cluster_token": CLUSTER},
        "local_api": {"host": "0.0.0.0", "token": TOKEN},
        "web": {"auto_open": False}, "log": {"level": "info", "format": "text"},
        "mtls": {"enabled": enabled, "listen_port": mtls_port,
                 "create_ca": create_ca, "ca_sync": True},
    }
    json.dump(cfg, open(cfg_path(key), "w"))

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

def wait_file(path, tries=100):
    for _ in range(tries):
        if os.path.exists(path) and os.path.getsize(path) > 0:
            return True
        time.sleep(0.2)
    return False

def main():
    shutil.rmtree(ROOT, ignore_errors=True)
    os.makedirs(ROOT, exist_ok=True)
    for key, (listen, mtls_port) in PORTS.items():
        assert port_free(listen) and port_free(mtls_port), f"port busy for {key}"

    # A 权威：自建 CA；B 成员：无 CA，静态指向 A。
    write_cfg("a", create_ca=True, peers=[])
    write_cfg("b", create_ca=False, peers=[f"127.0.0.1:{PORTS['a'][0]}"])
    # C：自带另一套 CA（冲突保护验证），也静态指向 A。
    write_cfg("c", create_ca=True, peers=[f"127.0.0.1:{PORTS['a'][0]}"])

    a_ca = os.path.join(data_dir("a"), "mtls", "ca.pem")
    b_ca = os.path.join(data_dir("b"), "mtls", "ca.pem")
    b_node = os.path.join(data_dir("b"), "mtls", "node.pem")
    c_ca = os.path.join(data_dir("c"), "mtls", "ca.pem")

    start("a")
    base_a = f"http://127.0.0.1:{PORTS['a'][0]}"
    ok_a = wait_health(base_a)
    check("S0 权威节点 A 自建 CA", ok_a and wait_file(a_ca))
    check("S0 成员节点 B 启动前无 CA", not os.path.exists(b_ca))

    start("b")
    base_b = f"http://127.0.0.1:{PORTS['b'][0]}"
    ok_b = wait_health(base_b)
    check("S1 双节点启动健康", ok_a and ok_b)

    # S2：B 自动领取 CA（落盘 + 指纹一致）。
    got = wait_file(b_ca)
    check("S2 B 自动领取集群 CA（落盘）", got)
    fp_a, fp_b = "", ""
    if got:
        fp_a = fingerprint(a_ca)
        fp_b = fingerprint(b_ca)
    check("S2 领取的 CA 指纹与权威一致", fp_a != "" and fp_a == fp_b,
          f"A={fp_a} B={fp_b}")

    # S3：B 的节点证书由领取到的 CA 签发。
    if os.path.exists(b_node):
        ok, out = verify(a_ca, b_node)
        check("S3 B 节点证书由领取的 CA 签发（openssl verify）", ok, out)
    else:
        check("S3 B 节点证书由领取的 CA 签发（openssl verify）", False, "no B node.pem")

    # S4：B 领取后热启动 TLS 监听 + 日志证据。
    hot = False
    for _ in range(60):
        if port_open(PORTS["b"][1]):
            hot = True
            break
        time.sleep(0.25)
    check("S4 B 领取后热启动 TLS 监听（端口可连）", hot)
    check("S4 B 日志含 adopted cluster CA", "adopted cluster CA" in log_text("b"),
          log_text("b")[-200:])

    # S5：冲突保护 —— C 自带 CA，连上 A 后不被覆盖。
    start("c")
    base_c = f"http://127.0.0.1:{PORTS['c'][0]}"
    ok_c = wait_health(base_c)
    fp_c_before = fingerprint(c_ca) if os.path.exists(c_ca) else ""
    time.sleep(6)  # 给双方握手 + 可能的（被拒绝的）覆盖留时间
    fp_c_after = fingerprint(c_ca) if os.path.exists(c_ca) else ""
    check("S5 冲突方 C 的 CA 未被覆盖（指纹不变）",
          ok_c and fp_c_before != "" and fp_c_before == fp_c_after,
          f"before={fp_c_before} after={fp_c_after}")
    check("S5 C 日志含 CA conflict", "CA conflict" in log_text("c"), log_text("c")[-200:])
    check("S5 权威与冲突方 CA 互不相同", fp_c_after != "" and fp_c_after != fp_a)

    stop_all()

    # S6：成员无可达权威 —— 不建 CA、节点仍运行。
    shutil.rmtree(data_dir("d"), ignore_errors=True)
    write_cfg("d", create_ca=False, peers=[f"127.0.0.1:{PORTS['c'][1]}"])  # 指向已停的 C
    d_ca = os.path.join(data_dir("d"), "mtls", "ca.pem")
    start("d")
    base_d = f"http://127.0.0.1:{PORTS['d'][0]}"
    ok_d = wait_health(base_d)
    time.sleep(2)
    check("S6 成员无权威可达：节点运行且不自建 CA",
          ok_d and not os.path.exists(d_ca))
    stop_all()

try:
    main()
finally:
    stop_all()

print(f"SMOKE_RESULT {passed}/{passed + failed}")
sys.exit(0 if failed == 0 else 1)
