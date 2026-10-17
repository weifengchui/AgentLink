#!/usr/bin/env python3
"""AgentLink 及时响应 · 看门狗巡检（cron 前置脚本，通用版 v0.5.0）。

设计=事件驱动唤醒链（桥→webhook，见 agentlink-message-wake 技能）的兜底：
gateway 死了、桥断了、钩子漏了，消息仍能被 5 分钟一轮的 cron 巡检查获并拉起会话处置。

判定为"急件"的三类（宁可多报不可漏报）：
  1. payload.topic 以 upgrade_ / task_ 开头（协调类，任意来源）
  2. 定向件：to_agent == 本机 agent（--agent 指定，或读 wake.json 的 agent 键）
  3. watch 名单里节点发来的任何消息（AL_WATCH.json 的 watch_from 数组）

输出哨兵（cron 会话按哨兵行事）：
  AGENTLINK-NO_NEW        无新增 → 只回一行心跳
  AGENTLINK-URGENT        后跟每条消息一行 JSON → 逐条处置后 --mark
  AGENTLINK-DOWN          本机节点 API 不可达 → 报机主，勿自动动作
  AGENTLINK-NODE-DRIFT    本机 runtime node_id 与身份账本不符 → 报机主

身份锚定：首次运行把 /v1/status 的 node_id 记入状态文件（pin）；此后不符即 DRIFT。
（防"init --force 抹身份"类事故——见 agentlink-node-identity-drift 技能。）

token/配置查找顺序（绝不递归扫盘）：
  $AGENTLINK_CONFIG → ~/.agentlink/agentlink.json → ~/.config/agentlink/agentlink.json
  → 脚本同级 agentlink.json → 回环 GET /web/setup（免手工配置的最后兜底）

用法：
  python3 al_watch.py                  # 巡检
  python3 al_watch.py --mark <msg_id>  # 处置完一条后标记（脚本绝不自动标记）
  python3 al_watch.py --agent <id> --expect <node_id>
可选环境变量：AL_BASE（默认 http://127.0.0.1:8765）
"""
import json, os, sys, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
BASE = os.environ.get("AL_BASE", "http://127.0.0.1:8765")
STATE = os.path.join(HERE, "al_watch_state.json")
URGENT_TOPIC_PREFIXES = ("upgrade_", "task_")


def load_state():
    try:
        with open(STATE) as f:
            st = json.load(f)
    except Exception:
        st = {}
    st.setdefault("processed", [])
    return st


def save_state(st):
    st["processed"] = st["processed"][-500:]
    tmp = STATE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(st, f)
    os.replace(tmp, STATE)


def resolve_token():
    cands = [os.environ.get("AGENTLINK_CONFIG"),
             os.path.expanduser("~/.agentlink/agentlink.json"),
             os.path.expanduser("~/.config/agentlink/agentlink.json"),
             os.path.join(HERE, "agentlink.json")]
    for p in [c for c in cands if c]:
        try:
            with open(p) as f:
                return json.load(f)["local_api"]["token"]
        except Exception:
            continue
    # 最后兜底：本机回环 /web/setup（远端一律 403，安全边界在节点侧硬编码）
    try:
        with urllib.request.urlopen(BASE + "/web/setup", timeout=5) as r:
            return json.load(r)["token"]
    except Exception:
        return None


def api(path, tok):
    req = urllib.request.Request(BASE + path, headers={"X-Auth-Token": tok})
    with urllib.request.urlopen(req, timeout=8) as r:
        return json.loads(r.read())


def resolve_self_agent(argv):
    if "--agent" in argv:
        return argv[argv.index("--agent") + 1]
    for p in (os.path.join(HERE, "wake.json"), os.environ.get("AL_WAKE_JSON", "")):
        try:
            with open(p) as f:
                return json.load(f).get("agent")
        except Exception:
            continue
    return None


def watch_nodes():
    try:
        with open(os.path.join(HERE, "AL_WATCH.json")) as f:
            return set(json.load(f).get("watch_from", []))
    except Exception:
        return set()


def main():
    argv = sys.argv[1:]
    st = load_state()
    if len(argv) >= 2 and argv[0] == "--mark":
        if argv[1] not in st["processed"]:
            st["processed"].append(argv[1])
        save_state(st)
        print(f"marked {argv[1]} processed")
        return

    tok = resolve_token()
    if not tok:
        print("AGENTLINK-DOWN 无法取得 local_api.token（检查 agentlink.json 或 /web/setup）")
        return
    try:
        status = api("/v1/status", tok)
    except Exception as e:
        print(f"AGENTLINK-DOWN 本机节点 API 不可达: {e}")
        return

    node = status.get("node_id", "")
    pinned = st.get("node_id")
    expect = argv[argv.index("--expect") + 1] if "--expect" in argv else pinned
    if expect and node != expect:
        print(f"AGENTLINK-NODE-DRIFT 当前node={node} 预期={expect}")
        return
    if not pinned:
        st["node_id"] = node
        save_state(st)
        print(f"[init] 本机身份已锚定 node_id={node}")

    hist = api("/v1/history?limit=80", tok)
    if isinstance(hist, dict):
        hist = hist.get("history") or hist.get("messages") or []
    done = set(st["processed"])
    me = resolve_self_agent(argv)
    watch = watch_nodes()
    urgent = []
    for m in hist:
        if m.get("msg_id") in done:
            continue
        p = m.get("payload") or {}
        topic = p.get("topic") or ""
        if topic.startswith(URGENT_TOPIC_PREFIXES) \
           or (me and m.get("to_agent") == me) \
           or m.get("from") in watch:
            urgent.append(m)

    if not urgent:
        print("AGENTLINK-NO_NEW")
        return
    print(f"AGENTLINK-URGENT 共{len(urgent)}条待处理：")
    for m in urgent:
        p = m.get("payload") or {}
        print(json.dumps({
            "msg_id": m.get("msg_id"), "ts": m.get("ts", ""),
            "from": m.get("from"), "from_agent": m.get("from_agent"),
            "to_agent": m.get("to_agent"), "topic": p.get("topic", ""),
            "content": (p.get("content") or "")[:2000],
        }, ensure_ascii=False))


if __name__ == "__main__":
    main()
