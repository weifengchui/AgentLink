#!/usr/bin/env python3
"""AgentMesh 任务存储端到端冒烟（v0.6 M1 P1-6）。

五个阶段（隔离端口 18765 + 独立 data_dir，绝不碰生产 8765/正式配置）：
  1. 默认零回归：无 mesh 段 → /v1/health features 逐字符 = 0.5.9 六项；
     POST/GET /v1/tasks/{id} 写面 403（"mesh license required"）。
  2. Mesh 生效：mesh.enabled + 自签 smoke license（内置 fixture 私钥
     seed=0x00..0x1F，与 license.rs DEV_PUBLIC_KEY_B64 配对；签名前先与
     license.rs 内置 fixture 签名逐字节对拍自校验）→ features 追加
     "engine"；POST 50 个带唯一长文本 payload 的任务全 202；GET 抽查
     spec 原文逐字符未脱敏；重复 POST 同 task_id → 409。
  3. 重启不丢：SIGKILL → 同 data_dir 重启 → 50 任务计数核对（==50），
     状态正确（含 cancelling 行），payload 原文仍在。
  4. 生命周期门：cancel → state=cancelling；DELETE 非终态 → 409。
  5. 坏 license 降级：篡改签名一字节 → 重启后 features 无 engine、
     写面 403、进程存活不炸。

用法: python3 tests/engine_store_smoke.py   (需先 cargo build --release;
全部 PASS 退出码 0。--keep 保留临时目录便于排查; pytest 可直接收集。)
"""
import base64
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

SCRATCH = "/home/yxf/.hermes/profiles/agentlink/cache/scratch/engine-store-smoke"
BIN = os.path.expanduser("~/agentlink-target/release/agentlink")
PORT = 18765
TOKEN = "smoketes...OKEN"
NODE = "e1g2i3n4e5s6"
API = f"http://127.0.0.1:{PORT}"
DATA = os.path.join(SCRATCH, "data")
CFG = os.path.join(SCRATCH, "agentlink.json")
LICENSE = os.path.join(SCRATCH, "license.toml")
LICENSE_BAD = os.path.join(SCRATCH, "license_bad.toml")
LOG = os.path.join(SCRATCH, "node.log")

# license.rs builtin_dev_fixture_end_to_end 钉死的 fixture（开发私钥
# seed=0x00..0x1F 离线签发, 规范化体见 canonical_signing_body）。
FIXTURE_BODY = (
    "expires=2036-12-31T23:59:59Z\n"
    "issued=2026-10-01T00:00:00Z\n"
    "node_family_prefix=lan\n"
    "node_quota=15\n"
    "tier=starter"
).encode()
FIXTURE_SIG_B64 = ("01qx+L3gOoXwdYa2o05/3qv8677f67r0uqwkOssx6HU"
                   "+iL7Bgr6WOxWYw437KJQBlRJpyvBQu+MSVzQirG+BDw==")
DEV_PUBLIC_KEY_B64 = "A6EHv/POEL4dcN0Y50vAmWfk1jCbpQ1fHdyGZBJVMbg="
SEED = bytes(range(32))
DEFAULT_FEATURES = ["events", "redact", "tasks", "json-log", "lockdown", "hello"]

# ---------------------------------------------------------------------------
# 纯 stdlib Ed25519（RFC 8032 §5/§6 参考实现）。license 自签用, 慢但够用。
# ---------------------------------------------------------------------------
_B = 256
_Q = 2**255 - 19
_L = 2**252 + 27742317777372353535851937790883648493
_D = -121665 * pow(121666, _Q - 2, _Q) % _Q
_I = pow(2, (_Q - 1) // 4, _Q)


def _inv(x):
    return pow(x, _Q - 2, _Q)


def _xrecover(y):
    xx = (y * y - 1) * _inv(_D * y * y + 1)
    x = pow(xx, (_Q + 3) // 8, _Q)
    if (x * x - xx) % _Q != 0:
        x = (x * _I) % _Q
    if x % 2 != 0:
        x = _Q - x
    return x


_By = 4 * _inv(5) % _Q
_Bp = [_xrecover(_By) % _Q, _By % _Q]


def _edwards(P, Q):
    x1, y1 = P
    x2, y2 = Q
    x3 = (x1 * y2 + x2 * y1) * _inv(1 + _D * x1 * x2 * y1 * y2) % _Q
    y3 = (y1 * y2 + x1 * x2) * _inv(1 - _D * x1 * x2 * y1 * y2) % _Q
    return [x3, y3]


def _scalarmult(P, e):
    if e == 0:
        return [0, 1]
    Q = _scalarmult(P, e // 2)
    Q = _edwards(Q, Q)
    if e & 1:
        Q = _edwards(Q, P)
    return Q


def _bit(h, i):
    return (h[i // 8] >> (i % 8)) & 1


def _hint(m):
    return sum(2**i * _bit(hashlib.sha512(m).digest(), i) for i in range(2 * _B))


def _encodeint(y):
    return bytes((y >> (8 * i)) & 0xFF for i in range(_B // 8))


def _encodepoint(P):
    x, y = P
    bits = [(y >> i) & 1 for i in range(_B - 1)] + [x & 1]
    out = bytearray(_B // 8)
    for i, bit in enumerate(bits):
        if bit:
            out[i // 8] |= 1 << (i % 8)
    return bytes(out)


def _scalar_from_secret(h):
    return 2**254 + sum(2**i * _bit(h, i) for i in range(3, 254))


def publickey(sk):
    return _encodepoint(_scalarmult(_Bp, _scalar_from_secret(hashlib.sha512(sk).digest())))


def signature(m, sk, pk):
    h = hashlib.sha512(sk).digest()
    a = _scalar_from_secret(h)
    r = _hint(h[32:] + m)
    R = _scalarmult(_Bp, r)
    s = (r + _hint(_encodepoint(R) + pk + m) * a) % _L
    return _encodepoint(R) + _encodeint(s)


def signer_selfcheck():
    """双重对拍：seed 推出的公钥 == DEV_PUBLIC_KEY_B64；对 fixture 规范化体
    重签 == license.rs 测试钉死的 fixture 签名（Ed25519 确定性, 逐字节）。
    通过则本脚本签出的 smoke license 必然可被二进制验签。"""
    if base64.b64encode(publickey(SEED)).decode() != DEV_PUBLIC_KEY_B64:
        return False, "pubkey 与内置开发公钥不符"
    sig = base64.b64encode(signature(FIXTURE_BODY, SEED, publickey(SEED))).decode()
    if sig != FIXTURE_SIG_B64:
        return False, "fixture 签名逐字节对拍失败"
    return True, ""


def canonical_signing_body(entries):
    signed = {k: str(v) for k, v in entries.items() if k != "signature"}
    return "\n".join(f"{k}={signed[k]}" for k in sorted(signed)).encode()


def sign_license_text(entries, comment):
    body = canonical_signing_body(entries)
    pk = publickey(SEED)
    sig = base64.b64encode(signature(body, SEED, pk)).decode()
    lines = [f"# {comment}"]
    for k, v in entries.items():
        lines.append(f"{k} = {v}" if k == "node_quota" else f'{k} = "{v}"')
    lines.append(f'signature = "{sig}"')
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# 冒烟驱动
# ---------------------------------------------------------------------------
results = []


def check(name, cond, detail=""):
    tag = "PASS" if cond else "FAIL"
    results.append(cond)
    print(f"{tag}  {name}{'  ' + detail if detail else ''}")


def http(method, path, body=None):
    req = urllib.request.Request(
        API + path,
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"X-Auth-Token": TOKEN, "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            out = json.loads(r.read())
            return r.status, out if isinstance(out, dict) else {"_val": out}
    except urllib.error.HTTPError as e:
        raw = e.read().decode()[:400]
        try:
            out = json.loads(raw)
            return e.code, out if isinstance(out, dict) else {"_val": out}
        except Exception:
            return e.code, {"_raw": raw}


def base_cfg():
    return {
        "v": 1, "node_id": NODE, "name": "engine-store-smoke", "listen_port": PORT,
        "data_dir": DATA,
        "discovery": {"enabled": False, "methods": [], "static_peers": []},
        "security": {"mode": "strict", "cluster_token": "smoketest-cluster"},
        "local_api": {"host": "127.0.0.1", "token": TOKEN},
        "delivery": {"ack_timeout_seconds": 5, "max_retries": 5,
                     "offline_ttl_seconds": 600, "dedup_cache_size": 2048,
                     "persist": True},
        "log": {"level": "info", "format": "text", "file": ""},
    }


def write_cfg(extra=None):
    cfg = base_cfg()
    if extra:
        cfg.update(extra)
    with open(CFG, "w") as f:
        json.dump(cfg, f)


def boot():
    logf = open(LOG, "a")
    p = subprocess.Popen([BIN, "run", "--config", CFG, "--no-open"],
                         stdout=logf, stderr=subprocess.STDOUT)
    for _ in range(80):
        try:
            code, h = http("GET", "/v1/health")
            if code == 200 and h.get("ok"):
                return p, h
        except Exception:
            pass
        time.sleep(0.25)
    p.kill()
    raise RuntimeError("node did not boot; see " + LOG)


def kill(p):
    os.kill(p.pid, signal.SIGKILL)
    p.wait()
    time.sleep(0.6)


def marker(i):
    # 长文本 + 形似敏感串(手机号/邮箱/token)——若任何路径脱敏,逐字符比对必炸
    return (f"smoke-{i:03d}·" + "任务存储原文校验αβΓΔ" * 12 +
            f"·token=sk-SECRET{i:04d}abcd·user{i}@example.com·+861380000{i:04d}")


def spec_of(i):
    return {
        "task_id": f"smoke-{i:03d}", "type": "shell", "topic": "task_spec",
        "intent": "engine-store-smoke", "correlation": f"corr-{i % 7}",
        "payload": {"note": marker(i), "index": i},
    }


def main():
    if not os.path.exists(BIN):
        print(f"FAIL  binary-missing  {BIN} 先 cargo build --release")
        return 1
    ok, why = signer_selfcheck()
    check("signer-selfcheck", ok, why or "公钥+fixture签名逐字节对拍通过")
    if not ok:
        print(f"ABORT: {why}")
        return 1

    shutil.rmtree(SCRATCH, ignore_errors=True)
    os.makedirs(DATA, exist_ok=True)

    # ---------- 阶段 1: 默认零回归（无 mesh 段） ----------
    write_cfg()
    p, h = boot()
    check("p1-boot", h.get("node_id") == NODE)
    feats = h.get("features")
    check("p1-features-exact", feats == DEFAULT_FEATURES,
          f"got={json.dumps(feats, ensure_ascii=False)}")
    code, body = http("POST", "/v1/tasks", spec_of(0))
    check("p1-post-403", code == 403 and body.get("error") == "mesh license required",
          f"code={code} body={json.dumps(body, ensure_ascii=False)[:160]}")
    code, _ = http("GET", "/v1/tasks/smoke-000")
    check("p1-getone-403", code == 403, f"code={code}")
    kill(p)

    # ---------- 阶段 2: mesh + license 生效 ----------
    lic = sign_license_text(
        {"tier": "starter", "node_quota": 15,
         "issued": "2026-10-01T00:00:00Z", "expires": "2036-12-31T23:59:59Z",
         "node_family_prefix": "lan"},
        "AgentMesh engine-store smoke license (dev fixture key)")
    with open(LICENSE, "w") as f:
        f.write(lic)
    write_cfg({"mesh": {"enabled": True, "license_file": "license.toml"}})
    p, h = boot()
    feats = h.get("features")
    check("p2-features-engine",
          feats == DEFAULT_FEATURES + ["engine"],
          f"got={json.dumps(feats)}")
    codes = []
    for i in range(50):
        c, _b = http("POST", "/v1/tasks", spec_of(i))
        codes.append(c)
    check("p2-50-tasks-202", all(c == 202 for c in codes),
          f"codes={json.dumps(sorted(set(codes)))}")
    # GET 抽查 spec 原文逐字符
    spot = []
    for i in (0, 17, 42, 49):
        c, body = http("GET", f"/v1/tasks/smoke-{i:03d}")
        rec = body.get("task", {})
        spot.append(c == 200
                    and rec.get("state") == "pending"
                    and rec.get("spec") == spec_of(i)
                    and rec.get("spec", {}).get("payload", {}).get("note") == marker(i))
    check("p2-spec-verbatim", all(spot), f"spotchecks_ok={sum(spot)}/4")
    c, body = http("POST", "/v1/tasks", spec_of(7))
    check("p2-dup-409", c == 409 and body.get("state") == "pending",
          f"code={c}")

    # ---------- 阶段 4（提前做, 使重启核对含 cancelling 行） ----------
    c, body = http("POST", "/v1/tasks/smoke-042/cancel", {})
    check("p4-cancel-cancelling", c == 200 and body.get("state") == "cancelling",
          f"code={c} state={body.get('state')}")
    c, body = http("DELETE", "/v1/tasks/smoke-042")
    check("p4-delete-nonterminal-409", c == 409, f"code={c}")

    # ---------- 阶段 3: 重启不丢 ----------
    kill(p)
    p, h = boot()
    check("p3-reboot", h.get("node_id") == NODE and "engine" in h.get("features", []))
    alive, states = 0, {}
    for i in range(50):
        c, body = http("GET", f"/v1/tasks/smoke-{i:03d}")
        if c == 200:
            alive += 1
            states[body["task"]["state"]] = states.get(body["task"]["state"], 0) + 1
    check("p3-50-count", alive == 50, f"alive={alive}/50")
    check("p3-states", states == {"pending": 49, "cancelling": 1},
          f"states={json.dumps(states)}")
    c, body = http("GET", "/v1/tasks/smoke-042")
    check("p3-cancelling-kept",
          c == 200 and body.get("task", {}).get("state") == "cancelling")
    ok_txt = True
    for i in (3, 42):
        c, body = http("GET", f"/v1/tasks/smoke-{i:03d}")
        if body.get("task", {}).get("spec", {}).get("payload", {}).get("note") != marker(i):
            ok_txt = False
    check("p3-spec-text-after-reboot", ok_txt)
    # 重启后写面仍工作 + 重复仍 409
    c, _b = http("POST", "/v1/tasks", spec_of(3))
    check("p3-dup-409-after-reboot", c == 409, f"code={c}")
    kill(p)

    # ---------- 阶段 5: 坏 license 降级 ----------
    # 篡改签名 base64 首字符（= license.toml 文本恰好一字节变化, b64 解码后
    # 签名字节亦变 → Ed25519 验签必败）
    sig_prefix = 'signature = "'
    idx = lic.index(sig_prefix) + len(sig_prefix)
    rep = "B" if lic[idx] != "B" else "C"
    tampered = lic[:idx] + rep + lic[idx + 1:]
    with open(LICENSE_BAD, "w") as f:
        f.write(tampered)
    write_cfg({"mesh": {"enabled": True, "license_file": "license_bad.toml"}})
    p, h = boot()
    feats = h.get("features")
    check("p5-no-engine", feats == DEFAULT_FEATURES, f"got={json.dumps(feats)}")
    c, body = http("POST", "/v1/tasks", spec_of(50))
    check("p5-write-403",
          c == 403 and body.get("error") == "mesh license required", f"code={c}")
    c, _b = http("GET", "/v1/tasks/smoke-000")
    check("p5-getone-403", c == 403, f"code={c}")
    c, h2 = http("GET", "/v1/health")
    check("p5-alive", c == 200 and h2.get("ok"), "进程未炸")
    kill(p)

    n = len(results)
    npass = sum(1 for r in results if r)
    print(f"\n{'ALL PASS' if npass == n else 'HAS FAILURES'}: {npass}/{n}")
    if npass == n and "--keep" not in sys.argv:
        kill_leftovers()
        shutil.rmtree(SCRATCH, ignore_errors=True)
    return 0 if npass == n else 1


def kill_leftovers():
    # 兜底: 确保冒烟实例无残留进程（按本脚本专属 name 匹配, 不碰生产）
    try:
        out = subprocess.run(["pgrep", "-f", CFG], capture_output=True, text=True).stdout
        for pid in out.split():
            os.kill(int(pid), signal.SIGKILL)
    except Exception:
        pass


def test_engine_store_smoke():
    assert main() == 0


if __name__ == "__main__":
    sys.exit(main())
