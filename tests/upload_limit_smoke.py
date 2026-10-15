#!/usr/bin/env python3
"""上传体上限冒烟测试（v0.4.13 引入）：/v1/send-file-upload 的请求体上限
必须跟随 files.max_file_size，而不是 axum 默认的 2 MiB。

历史坑：路由未设 DefaultBodyLimit，>2 MiB 的上传在 extractor 阶段就被
axum 用纯文本 413 "Failed to buffer the request body: length limit
exceeded" 拒掉，handler 里基于 max_file_size 的 JSON 413 永远走不到，
界面上表现为“发送失败:+ 乱码文本”，用户以为文件链路坏了。

隔离端口 18765 测试实例，防串入生产 mesh。目标用一个不存在的 12-hex
node_id：请求会走到 handler 并在投递阶段报错，这足以证明 body 已通过
extractor。全部 PASS 退出码为 0。
用法: python3 tests/upload_limit_smoke.py  (需先 cargo build --release)
"""
import json, os, subprocess, time, urllib.error, urllib.request

SCRATCH = "/home/yxf/.hermes/profiles/agentlink/cache/scratch/upload-smoke"
BIN = os.path.expanduser("~/agentlink-target/release/agentlink")
PORT = 18765
TOKEN = "smoketes...OKEN"
NODE = "a1b2c3d4e5f6"
MAX = 4 * 1024 * 1024  # 4 MiB：故意大于 axum 默认 2 MiB，才能测出修复
DATA = os.path.join(SCRATCH, "data")
CFG = os.path.join(SCRATCH, "agentlink.json")
LOG = os.path.join(SCRATCH, "node.log")
API = f"http://127.0.0.1:{PORT}"
GHOST = "000000000000"  # 合法 node_id 形态，但对端不存在

cfg = {
    "v": 1, "node_id": NODE, "name": "upload-smoke", "listen_port": PORT,
    "data_dir": DATA,
    "discovery": {"enabled": False, "methods": [], "static_peers": []},
    "security": {"mode": "strict", "cluster_token": "smoketest-cluster"},
    "local_api": {"host": "127.0.0.1", "token": TOKEN},
    "files": {"max_file_size": MAX, "chunk_size": 65536, "max_concurrent": 1},
    "delivery": {"ack_timeout_seconds": 5, "max_retries": 5,
                 "offline_ttl_seconds": 600, "dedup_cache_size": 2048,
                 "persist": True},
    "log": {"level": "info", "format": "text", "file": ""},
}


def upload(size):
    """POST 裸字节，返回 (http_code 或 None, body 文本)。"""
    req = urllib.request.Request(
        f"{API}/v1/send-file-upload?to={GHOST}", method="POST",
        data=b"x" * size,
        headers={"X-Auth-Token": TOKEN, "X-File-Name": "probe.bin"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def boot():
    with open(LOG, "a") as logf:
        p = subprocess.Popen([BIN, "run", "--config", CFG, "--no-open"],
                             stdout=logf, stderr=subprocess.STDOUT)
    for _ in range(60):
        try:
            req = urllib.request.Request(f"{API}/v1/health", method="GET")
            with urllib.request.urlopen(req, timeout=5) as r:
                if json.loads(r.read()).get("ok"):
                    return p
        except Exception:
            pass
        time.sleep(0.25)
    raise RuntimeError("node did not boot; see " + LOG)


results = []


def check(name, cond, detail=""):
    results.append(f"{'PASS' if cond else 'FAIL'}  {name}  {detail}".rstrip())


def is_handler_json(body):
    """响应由 handler 产出（而不是 axum extractor 的纯文本拒绝）。"""
    try:
        json.loads(body)
        return True
    except Exception:
        return False


subprocess.run(["rm", "-rf", SCRATCH], check=True)
os.makedirs(DATA, exist_ok=True)
with open(CFG, "w", encoding="utf-8") as f:
    json.dump(cfg, f)

proc = boot()
try:
    # 1) 旧 bug 断点：2 MiB + 1 字节必须能进 handler
    code, body = upload(2 * 1024 * 1024 + 1)
    check("2MiB+1 不再被 extractor 拒绝",
          is_handler_json(body) and "Failed to buffer" not in body,
          f"HTTP {code} {body[:120]}")

    # 2) 恰好等于 max_file_size：handler 用 `>` 判定，应放行到投递阶段
    code, body = upload(MAX)
    check("max_file_size 整值放行到投递阶段",
          code != 413 and is_handler_json(body),
          f"HTTP {code} {body[:120]}")

    # 3) 超过 max_file_size：必须是干净的 JSON 413，不是纯文本
    code, body = upload(MAX + 1)
    ok413 = code == 413 and is_handler_json(body) and "file too large" in body
    check("超限返回 JSON 413 file too large", ok413, f"HTTP {code} {body[:120]}")

    # 4) 小文件仍然正常（回归）
    code, body = upload(1024)
    check("小文件仍走 handler", is_handler_json(body), f"HTTP {code} {body[:120]}")

    # 5) 投递阶段确实执行过：对端不存在应给出投递错误而非体上限错误
    code, body = upload(4096)
    check("失败原因是投递而非体上限",
          "length limit exceeded" not in body and "too large" not in body,
          f"HTTP {code} {body[:120]}")

    # 6) 其它路由的默认上限没有被抬高（JsonAny 把体读取失败一律映射成 400）
    req = urllib.request.Request(f"{API}/v1/send", method="POST",
                                data=b"x" * (3 * 1024 * 1024),
                                headers={"X-Auth-Token": TOKEN,
                                         "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            code, body = r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        code, body = e.code, e.read().decode()
    check("/v1/send 3MiB 仍被拒（默认上限未被抬高）",
          code != 200 and "ok\":true" not in body,
          f"HTTP {code} {body[:120]}")
finally:
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()

print("\n".join(results))
bad = [r for r in results if r.startswith("FAIL")]
print(f"\n{len(results) - len(bad)}/{len(results)} PASS")
raise SystemExit(1 if bad else 0)
