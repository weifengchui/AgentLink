#!/usr/bin/env python3
"""v0.4.15 新功能冒烟：双隔离节点 mesh，验证 history/search、colors、多文件上传。"""
import json, os, subprocess, tempfile, time, urllib.request, urllib.error

NO_PROXY = urllib.request.build_opener(urllib.request.ProxyHandler({}))
BASE = os.path.join(tempfile.gettempdir(), "al15feat-smoke")
os.makedirs(BASE, exist_ok=True)
BIN = os.environ.get("AGENTLINK_BIN", "/home/yxf/agentlink-target/release/agentlink")
PORTS = (18771, 18772)
IDS = ("aa15fe000001", "bb15fe000002")
TOKENS = ("tokA15feat", "tokB15feat")

fails = []
def ck(name, ok, info=""):
    print(("PASS " if ok else "FAIL ") + name + ("  " + str(info) if info else ""))
    if not ok: fails.append(name)

def api(port, tok, path, method="GET", body=None, headers=None):
    h = {"X-Auth-Token": tok}
    data = body.encode() if isinstance(body, str) else body
    if isinstance(body, str):
        h["Content-Type"] = "application/json"
    elif data is not None:
        h["Content-Type"] = "application/octet-stream"
    if headers: h.update(headers)
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", method=method, data=data, headers=h)
    try:
        with NO_PROXY.open(req, timeout=30) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try: return e.code, json.loads(e.read().decode())
        except Exception: return e.code, {}
    except Exception as e:
        return None, {"error": str(e)[:120]}

def main():
    procs = []
    for i, (port, nid, tok) in enumerate(zip(PORTS, IDS, TOKENS)):
        d = os.path.join(BASE, f"n{i}")
        os.makedirs(os.path.join(d, "data"), exist_ok=True)
        for f in os.listdir(os.path.join(d, "data")):
            p = os.path.join(d, "data", f)
            if os.path.isfile(p): os.remove(p)
        cfg = {
            "v": 1, "node_id": nid, "name": f"feat{i}", "listen_port": port,
            "data_dir": os.path.join(d, "data"),
            "discovery": {"enabled": True, "methods": [], "static_peers": [f"127.0.0.1:{PORTS[1-i]}"]},
            "security": {"mode": "strict", "cluster_token": "feat15cluster"},
            "local_api": {"host": "127.0.0.1", "token": tok},
            "files": {"max_file_size": 8388608, "chunk_size": 65536, "max_concurrent": 2},
            "history": {"enabled": True, "len": 200, "file": "history.jsonl", "max_file_mb": 1024, "archive_days": 7, "archive_keep": 0},
            "log": {"level": "warn", "format": "text", "file": ""},
        }
        cp = os.path.join(d, "agentlink.json")
        open(cp, "w").write(json.dumps(cfg))
        procs.append(subprocess.Popen([BIN, "run", "--config", cp], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    try:
        # wait for mesh
        online = False
        for _ in range(20):
            c, d = api(PORTS[0], TOKENS[0], "/v1/peers")
            if any(p.get("node_id") == IDS[1] and p.get("online") for p in d.get("peers", [])):
                online = True; break
            time.sleep(1)
        ck("双节点 mesh 在线", online)

        # chat both directions
        api(PORTS[0], TOKENS[0], "/v1/send", "POST", json.dumps({"to": IDS[1], "type": "chat", "action": "msg", "payload": {"content": "hello-15feat"}}))
        api(PORTS[1], TOKENS[1], "/v1/send", "POST", json.dumps({"to": IDS[0], "type": "chat", "action": "msg", "payload": {"content": "reply-15feat"}}))
        # multi-file: two raw uploads from node1 -> node0 (what UI loop does per file)
        for nm in ("f1.txt", "f2.txt"):
            s, d = api(PORTS[1], TOKENS[1], f"/v1/send-file-upload?to={IDS[0]}", "POST", b"content-of-" + nm.encode(), {"X-File-Name": nm})
            ck(f"上传 {nm} 到真实对端", s == 200 and d.get("ok") is not False, f"HTTP {s} {str(d)[:90]}")
        time.sleep(1)

        # ---- search ----
        c, d = api(PORTS[0], TOKENS[0], "/v1/history/search?limit=50")
        ck("search 基础", c == 200 and d.get("ok") and d.get("count", 0) >= 2, f"count={d.get('count')} stopped={d.get('stopped')}")
        c, d = api(PORTS[0], TOKENS[0], "/v1/history/search?kind=file")
        file_hits = d.get("matches", [])
        ck("search kind=file（接收侧 in）", c == 200 and any(m["direction"] == "in" for m in file_hits), f"count={d.get('count')}")
        c, d = api(PORTS[0], TOKENS[0], f"/v1/history/search?to={IDS[0]}&kind=file")
        ck("search to=节点0 kind=file", d.get("count", 0) >= 2, f"count={d.get('count')}")
        c, d = api(PORTS[1], TOKENS[1], "/v1/history/search?direction=out&kind=file")
        ck("search direction=out", d.get("count", 0) >= 2, f"count={d.get('count')}")
        c, d = api(PORTS[0], TOKENS[0], "/v1/history/search?q=hello-15feat")
        ck("search q 关键词", d.get("count", 0) >= 1, f"count={d.get('count')}")
        c, d = api(PORTS[0], TOKENS[0], "/v1/history/search?limit=1")
        ck("search limit=1", d.get("count") == 1 and d.get("stopped") in ("limit", "older"), f"count={d.get('count')} stopped={d.get('stopped')}")
        c, d = api(PORTS[0], TOKENS[0], "/v1/history/search?since=2035-01-01T00:00:00")
        ck("search since 未来=0 条", d.get("count") == 0, f"stopped={d.get('stopped')}")
        c, d = api(PORTS[0], TOKENS[0], "/v1/history/search?until=2001-01-01T00:00:00")
        ck("search until 过去=0 条", d.get("count") == 0, f"count={d.get('count')}")
        c, d = api(PORTS[0], TOKENS[0], f"/v1/history/search?who={IDS[1]}")
        ck("search who 双向", d.get("count", 0) >= 2, f"count={d.get('count')}")
        c, d = api(PORTS[0], TOKENS[0], "/v1/history/search?from=zzz-nothing")
        ck("search 无匹配", d.get("count") == 0, f"count={d.get('count')}")

        # ---- colors ----
        payload = json.dumps({"colors": {f"node:{IDS[1]}": "#ff6600", f"agent:{IDS[1]}|alice": "#00cc88"}})
        c, d = api(PORTS[0], TOKENS[0], "/v1/colors", "PUT", payload)
        ck("colors PUT 合法", c == 200 and d.get("ok"), f"HTTP {c}")
        c, d = api(PORTS[0], TOKENS[0], "/v1/colors")
        ck("colors GET 回读", d.get("colors", {}).get(f"node:{IDS[1]}") == "#ff6600", str(d.get("colors"))[:80])
        c, d = api(PORTS[0], TOKENS[0], "/v1/colors", "PUT", json.dumps({"colors": {"bad:key": "notahex"}}))
        ck("colors 非法值被拒", c == 400, f"HTTP {c}")
        c, d = api(PORTS[0], TOKENS[0], "/v1/colors", "PUT", json.dumps({"colors": {}}))
        ck("colors 清空 ok", c == 200, f"HTTP {c}")
        c, d = api(PORTS[0], TOKENS[0], "/v1/colors", "PUT", json.dumps({"colors": {f"node:{IDS[1]}": "#ff6600"}}))
        ck("colors 重设为重启做准备", c == 200, f"HTTP {c}")

        # restart node0 -> colors + history persist
        procs[0].terminate(); procs[0].wait(timeout=10)
        d0 = os.path.join(BASE, "n0")
        procs[0] = subprocess.Popen([BIN, "run", "--config", os.path.join(d0, "agentlink.json")], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(15):
            c, d = api(PORTS[0], TOKENS[0], "/v1/history/search?kind=file")
            if d.get("count", 0) >= 2: break
            time.sleep(1)
        ck("重启后 search 仍见全部记录（不丢）", d.get("count", 0) >= 2, f"count={d.get('count')}")
        c, d = api(PORTS[0], TOKENS[0], "/v1/colors")
        ck("重启后 colors 保留", d.get("colors", {}).get(f"node:{IDS[1]}") == "#ff6600", str(d.get("colors"))[:80])

        # archive scope
        c, d = api(PORTS[0], TOKENS[0], "/v1/history/search?scope=all&kind=file")
        ck("scope=all 扫描含归档目录", c == 200 and d.get("count", 0) >= 2, f"count={d.get('count')} scanned={d.get('scanned_bytes')}")

        print()
        if fails:
            print(f"RESULT: {len(fails)} FAIL -> " + "; ".join(fails)); raise SystemExit(1)
        print("RESULT: ALL PASS")
    finally:
        for p in procs:
            try: p.terminate()
            except Exception: pass

if __name__ == "__main__":
    main()
