#!/usr/bin/env python3
"""对话记录归档/导出冒烟测试（v0.4.15 引入）。

要守住的语义（用户 10-15 提的需求）：
  1. 记录只追加、只滚动，节点永远不删除对话记录；
  2. 活动文件超过 max_file_mb 或老于 archive_days 天，整份搬进
     data_dir/history_archive/，另起新文件（旧的 truncate+删 .1 归档
     的写法会在 16MB 时静默丢掉上一份归档）；
  3. 重启后 /v1/history 能从盘上恢复记录（不丢）；
  4. /v1/history/stats 报体积，/v1/history/export 只读导出
     json（原始 JSONL）与 text（可读文本），scope=all 含全部归档。

隔离端口 18770，独立 data_dir，绝不碰生产节点(8765)。
用法: python3 tests/history_archive_smoke.py  (需先 cargo build --release)
"""
import json, os, shutil, subprocess, time, urllib.error, urllib.request

SCRATCH = "/home/yxf/.hermes/profiles/agentlink/cache/scratch/hist-smoke"
BIN = os.path.expanduser("~/agentlink-target/release/agentlink")
PORT = 18770
TOKEN = "hist-smoke-local-token-0123456789"
NODE = "a1b2c3d4e5f7"
DATA = os.path.join(SCRATCH, "data")
CFG = os.path.join(SCRATCH, "agentlink.json")
LOG = os.path.join(SCRATCH, "node.log")
API = f"http://127.0.0.1:{PORT}"
GHOST = "000000000000"
HF = os.path.join(DATA, "history.jsonl")
ADIR = os.path.join(DATA, "history_archive")

cfg = {
    "v": 1, "node_id": NODE, "name": "hist-smoke", "listen_port": PORT,
    "data_dir": DATA,
    "discovery": {"enabled": False, "methods": [], "static_peers": []},
    "security": {"mode": "strict", "cluster_token": "hist-smoke-cluster"},
    "local_api": {"host": "127.0.0.1", "token": TOKEN},
    "files": {"max_file_size": 1048576, "chunk_size": 65536, "max_concurrent": 1},
    "delivery": {"ack_timeout_seconds": 5, "max_retries": 3,
                 "offline_ttl_seconds": 600, "dedup_cache_size": 2048,
                 "persist": True},
    "history": {"enabled": True, "len": 200, "file": "history.jsonl",
                "max_file_mb": 1024, "archive_days": 1, "archive_keep": 0},
    "log": {"level": "info", "format": "text", "file": ""},
}

results = []


def check(name, cond, detail=""):
    results.append(f"{'PASS' if cond else 'FAIL'}  {name}  {detail}".rstrip())


# 127.0.0.1 必须绕过系统代理（:1080 之类的残留会让本地 API 全部超时）
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


# 127.0.0.1 必须绕过系统代理（:1080 之类的残留会让本地 API 全部超时）
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def api(path, method="GET", body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(API + path, method=method, data=data,
                                 headers={"X-Auth-Token": TOKEN,
                                          "Content-Type": "application/json"})
    with OPENER.open(req, timeout=20) as r:
        return r.status, r.read()


def send(text):
    """发一条消息，确保进入本地对话记录。先发给自己节点（自投递），
    如果没落记录再退而发给一个不存在的 node_id（out 方向同样会记）。"""
    before = len(lines(HF))
    for target in (NODE, GHOST):
        try:
            api("/v1/send", "POST", {"type": "chat", "action": "msg",
                                     "from_agent": "smoke", "to": target,
                                     "payload": {"content": text}})
        except urllib.error.HTTPError:
            pass
        time.sleep(0.35)
        if len(lines(HF)) > before:
            return
    time.sleep(0.4)


def lines(path):
    if not os.path.isfile(path):
        return []
    with open(path, encoding="utf-8") as f:
        return [l for l in f.read().splitlines() if l.strip()]


def raw_get(path):
    """带 header 的原始 GET（导出用：要看 Content-Disposition）。"""
    req = urllib.request.Request(API + path, headers={"X-Auth-Token": TOKEN})
    with OPENER.open(req, timeout=30) as r:
        return r.headers, r.read()


def boot():
    with open(LOG, "a") as logf:
        p = subprocess.Popen([BIN, "run", "--config", CFG, "--no-open"],
                             stdout=logf, stderr=subprocess.STDOUT)
    for _ in range(60):
        try:
            if json.loads(api("/v1/health")[1]).get("ok"):
                return p
        except Exception:
            pass
        time.sleep(0.25)
    raise RuntimeError("node did not boot; see " + LOG)


def stop(p):
    p.terminate()
    try:
        p.wait(timeout=10)
    except Exception:
        p.kill()


shutil.rmtree(SCRATCH, ignore_errors=True)
os.makedirs(DATA, exist_ok=True)
with open(CFG, "w", encoding="utf-8") as f:
    json.dump(cfg, f)

proc = boot()
def dump():
    print("\n".join(results))


try:
    # 1) 记录落盘
    for i in range(3):
        send(f"alpha-{i}")
    base = lines(HF)
    check("消息写入 history.jsonl", len(base) >= 3, f"{len(base)} 行")

    # 2) 活动文件变老（>archive_days）→ 下一条消息触发整份归档，而不是删除
    open(os.path.join(DATA, "history.jsonl.1"), "w", encoding="utf-8").write(
        '{"ts":"2026-01-01T00:00:00Z","direction":"out","msg_id":"legacy0001","kind":"message","payload":{"content":"legacy"}}\n')
    old = time.time() - 3 * 86400
    if not os.path.isfile(HF):
        check("消息写入 history.jsonl", False, "文件不存在：/v1/send 未落记录，见 node.log")
        raise SystemExit(dump())
    os.utime(HF, (old, old))
    send("after-rotate")
    time.sleep(0.6)
    archives = sorted(os.listdir(ADIR)) if os.path.isdir(ADIR) else []
    timed = [a for a in archives if a.startswith("history-2")]
    check("按天滚动生成时间戳归档", len(timed) == 1, str(archives))
    check("旧版 .1 归档被收进归档目录（不再被删）",
          "history-legacy.jsonl" in archives, str(archives))
    archived_lines = lines(os.path.join(ADIR, timed[0])) if timed else []
    check("归档内容与滚动前一致（零丢失）",
          len(archived_lines) == len(base),
          f"归档 {len(archived_lines)} 行 / 滚动前 {len(base)} 行")
    act = open(HF, encoding="utf-8").read()
    check("活动文件另起且不含旧记录",
          len(lines(HF)) >= 1 and "alpha-" not in act and "after-rotate" in act,
          f"{len(lines(HF))} 行")

    # 3) stats
    st = json.loads(api("/v1/history/stats")[1])
    ab = os.path.getsize(HF)
    arb = sum(os.path.getsize(os.path.join(ADIR, a)) for a in archives)
    want = {"active_bytes": ab, "archive_bytes": arb, "total_bytes": ab + arb,
            "rotate_mb": 1024, "archive_days": 1}
    got = {k: st.get(k) for k in want}
    check("stats 体积正确",
          st.get("ok") and got == want,
          f"got={json.dumps(got)} want={json.dumps(want)} archives={archives} "
          f"file_sizes={[(a, os.path.getsize(os.path.join(ADIR, a))) for a in archives]}")

    # 4) 导出：json = 原始 JSONL（归档在前 + 活动），只读不删
    total_src = sum(len(lines(os.path.join(ADIR, a))) for a in archives) + len(lines(HF))
    hdr, body = raw_get("/v1/history/export?format=json&scope=all")
    exp = [l for l in body.decode("utf-8").splitlines() if l.strip()]
    cd = hdr.get("Content-Disposition", "")
    check("导出 JSONL 行数=全部记录（含归档）", len(exp) == total_src,
          f"导出 {len(exp)} / 源 {total_src}")
    check("导出带附件下载名", "agentlink-history-all-" in cd and ".jsonl" in cd, cd)
    check("导出后源文件未被改动",
          sum(len(lines(os.path.join(ADIR, a))) for a in archives) + len(lines(HF)) == total_src)

    # 5) 导出：text = 可读行，条数一致
    hdr2, body2 = raw_get("/v1/history/export?format=text&scope=all")
    txt = [l for l in body2.decode("utf-8").splitlines() if l.strip()]
    check("导出文本条数一致且可读", len(txt) == total_src and "→" in body2.decode("utf-8")
          and "alpha-0" in body2.decode("utf-8"),
          f"{len(txt)} 行 / {hdr2.get('Content-Type')}")
    hdr3, body3 = raw_get("/v1/history/export?format=text&scope=active")
    check("scope=active 只导当前文件",
          len([l for l in body3.decode().splitlines() if l.strip()]) == len(lines(HF)))

    # 6) 重启后记录仍在（用户最关心的问题）
    seen = [json.loads(l).get("msg_id") for l in lines(HF)]
    stop(proc)
    proc = boot()
    hist = json.loads(api("/v1/history?limit=500")[1])["history"]
    contents = json.dumps(hist, ensure_ascii=False)
    check("重启后 /v1/history 仍有本次会话记录",
          "after-rotate" in contents and len(hist) >= 1, f"{len(hist)} 条")
    check("重启后 stats 仍识别归档",
          json.loads(api("/v1/history/stats")[1]).get("archive_count") == len(archives),
          f"{len(archives)} 个归档")
finally:
    stop(proc)

dump()
bad = [r for r in results if r.startswith("FAIL")]
print(f"\n冒烟 {len(results) - len(bad)}/{len(results)} PASS")
raise SystemExit(1 if bad else 0)
