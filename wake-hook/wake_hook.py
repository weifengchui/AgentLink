# -*- coding: utf-8 -*-
"""wake_hook — AgentLink 桥接的会话唤醒钩子（hermes webhook）。

桥收到业务消息并入 inbox 后调用 wake(env)：POST 到本机 hermes gateway 的
webhook 路由（HMAC-SHA256 签名），触发对应 profile 的 agent 会话即时处理，
不再依赖 agent 自己想起来拉收件箱。

配置：桥目录下放 wake.json（缺失=钩子禁用，行为与旧版完全一致）：
  {"url": "http://127.0.0.1:8644/webhooks/agentlink-msg",
   "secret": "<hermes webhook list 里的 secret>",
   "agent": "study",                     // 可选：本桥代表的 agent 名（自回声过滤）
   "skip_topics": ["upgrade_ready"]}     // 可选：不唤醒的 topic
铁律：唤醒失败绝不抛出、绝不阻塞消息接收（尽力原则）。
"""
import hashlib
import hmac
import json
import os
import urllib.request

_cache = {"mtime": None, "conf": None}


def _conf(base_dir):
    path = os.path.join(base_dir, "wake.json")
    try:
        m = os.path.getmtime(path)
    except OSError:
        return None
    if _cache["mtime"] != m:
        try:
            with open(path, encoding="utf-8") as f:
                _cache["conf"] = json.load(f)
            _cache["mtime"] = m
        except Exception:
            return None
    return _cache["conf"]


def wake(env, base_dir):
    """env = 节点投递的消息信封。任何异常都吞掉——唤醒只是锦上添花。"""
    try:
        c = _conf(base_dir)
        if not c or not c.get("url"):
            return
        payload = env.get("payload") or {}
        topic = payload.get("topic") or env.get("type") or ""
        if topic in (c.get("skip_topics") or []):
            return
        me = c.get("agent")
        if me and env.get("from_agent") == me and env.get("to") in (None, "", me):
            return  # 自回声：自己桥发的回执不必再叫醒自己
        body = json.dumps(
            {
                "from": env.get("from") or env.get("node_id") or "",
                "from_agent": env.get("from_agent") or "",
                "topic": topic,
                "content": str(
                    payload.get("content") or json.dumps(payload, ensure_ascii=False)
                )[:2000],
                "msg_id": env.get("msg_id") or "",
                "to": me or "",
            },
            ensure_ascii=False,
        ).encode()
        sig = "sha256=" + hmac.new(str(c["secret"]).encode(), body, hashlib.sha256).hexdigest()
        req = urllib.request.Request(
            c["url"],
            data=body,
            headers={"Content-Type": "application/json", "X-Hub-Signature-256": sig},
        )
        urllib.request.urlopen(req, timeout=5).read()
    except Exception:
        pass
