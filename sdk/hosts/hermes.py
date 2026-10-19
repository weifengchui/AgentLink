# -*- coding: utf-8 -*-
"""hosts/hermes.py — Adapter SDK 的第一参考宿主：Hermes（现役 hermes_bridge 语义承接）。

现役形态（wake-hook/hermes_bridge.py）：WS 订阅入 inbox → wake_hook.py 以
HMAC-SHA256 签名 POST 唤醒本机 hermes 会话 → 会话经桥回信。本 runner 把同一条
数据流正式化到任务协议：task_spec → 唤醒会话（带 reply_to）→ 会话回信
（bus 帧带同一 reply_to）即任务结果。

会话侧契约（写给 prompt，由运营侧 prompt 规则承接）：
  会话处理完任务后，把最终答复经桥发回编排 agent，桥帧 outer.reply_to =
  唤醒时给它的 msg_id，payload 即被视为 task result（内容原文透传）。

返回语义：按时收到回信 → succeeded（result=回信 payload content）；
超时 → failed(retryable=True)；wait 期间被 cancel → failed(cancelled)。
"""
import hashlib
import hmac
import json
import threading
import time
import urllib.request
import uuid

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from adapter import BaseRunner, RunResult  # noqa: E402


class HermesRunner(BaseRunner):
    """会话唤醒式 runner（非 CLI 型，数据流与现役桥一致，非子进程）。"""

    def __init__(self, gateway_url, secret, agent="",
                 timeout=120, log=None):
        self.gateway_url = gateway_url.rstrip("/")
        self.secret = secret
        self.agent = agent
        self.timeout = timeout
        self.log = log
        self._pending = {}   # msg_id -> {"event": Event, "out": dict}

    # ---- frame_hook：承接会话回信（TaskAdapter(frame_hook=…) 传入） ----

    def on_frame(self, env):
        """bus 帧带 reply_to 且命中 pending msg_id → 唤醒等待。"""
        rid = env.get("reply_to") or ""
        pend = self._pending.get(rid)
        if not pend:
            return
        pend["out"] = env.get("payload") or {}
        pend["event"].set()

    # ---- 契约实现 ----

    def preflight(self, spec):
        if not self.gateway_url:
            return "gateway_url 未配置（hermes 会话唤醒式 runner 必须配网关）"
        if not self.secret:
            return "gateway secret 未配置（HMAC 唤醒缺签名即拒）"
        return None

    def build_prompt(self, spec):
        s = spec.get("intent") or spec.get("type") or ""
        params = spec.get("params") or {}
        if params:
            s += "\n\n任务参数：\n" + json.dumps(params, ensure_ascii=False, indent=2)
        return s

    def run(self, spec, report, cancelled):
        msg_id = uuid.uuid4().hex
        start = time.monotonic()
        ev = threading.Event()
        pend = {"event": ev, "out": None}
        self._pending[msg_id] = pend
        report(stage="wake", msg_id=msg_id)
        try:
            body = json.dumps({
                "from": self.agent or "adapter-hermes",
                "from_agent": self.agent or "adapter-hermes",
                "topic": "adapter_task_prompt",
                "content": self.build_prompt(spec)[:2000],
                "msg_id": msg_id,
                "task_id": spec.get("task_id"),
                "to": self.agent or "",
            }, ensure_ascii=False).encode()
            sig = "sha256=" + hmac.new(self.secret.encode(), body,
                                       hashlib.sha256).hexdigest()
            req = urllib.request.Request(
                self.gateway_url, data=body,
                headers={"Content-Type": "application/json",
                         "X-Hub-Signature-256": sig})
            urllib.request.urlopen(req, timeout=5).read()
        except Exception as e:
            self._pending.pop(msg_id, None)
            return RunResult(ok=False, error=f"wake 失败: {e}", retryable=True)
        while not ev.wait(0.5):
            if cancelled():
                self._pending.pop(msg_id, None)
                return RunResult(ok=False, error="cancelled")
            report(stage="waiting", msg_id=msg_id)
            if time.monotonic() > start + self.timeout:
                self._pending.pop(msg_id, None)
                return RunResult(ok=False, error=f"session 回信超时({self.timeout}s)",
                                 retryable=True)
        self._pending.pop(msg_id, None)
        out = pend["out"] or {}
        content = out.get("content")
        return RunResult(ok=True,
                         result=content if isinstance(content, str)
                         else json.dumps(out, ensure_ascii=False)[:4000])
