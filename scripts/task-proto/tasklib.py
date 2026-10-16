"""tasklib — AgentLink 任务协议 v1 共享库（参考实现）。

对应 docs/task-protocol.md。仅 stdlib + websocket-client。
用途：隔离测试总线上跑通任务生命周期；生产接入请按 README 调整。
"""
import json, time, uuid, urllib.request

class Bus:
    def __init__(self, base="http://127.0.0.1:18765", token="", agent=""):
        self.base, self.token, self.agent = base, token, agent
        self.node = self.health()["node_id"]

    def _api(self, path, body=None):
        data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data,
            headers={"X-Auth-Token": self.token, "Content-Type": "application/json"},
            method="POST" if body is not None else "GET")
        with urllib.request.urlopen(req, timeout=8) as r:
            return json.loads(r.read().decode())

    def health(self):        return self._api("/v1/health")
    def history(self, limit=200): return self._api(f"/v1/history?limit={limit}").get("history", [])
    def register(self, agent_id, capabilities=None):
        return self._api("/v1/agents", {"agent_id": agent_id, "name": agent_id,
                                        "capabilities": capabilities or []})
    def send(self, to_agent, payload, reply_to=None, to_node=None):
        body = {"from": self.node, "to": to_node or self.node, "to_agent": to_agent,
                "from_agent": self.agent, "type": "text", "action": "send",
                "payload": payload}
        if reply_to: body["reply_to"] = reply_to
        return self._api("/v1/send", body)

    def ws_url(self):
        return self.base.replace("http://", "ws://") + f"/v1/ws?token={self.token}&client={self.agent}"


def make_spec(task_type, params, intent="", requires=None, deadline=None,
              reconcile=None, priority=100, correlation=None, task_id=None, seq=1):
    spec = {"topic": "task_spec", "task_id": task_id or str(uuid.uuid4()), "seq": seq,
            "type": task_type, "intent": intent, "params": params, "priority": priority}
    if requires:   spec["requires"]   = requires
    if deadline:   spec["deadline"]   = deadline
    if reconcile:  spec["reconcile"]  = reconcile
    if correlation:spec["correlation"]= correlation
    return spec


def msg(topic, task_id, **kw):
    p = {"topic": topic, "task_id": task_id}
    p.update({k: v for k, v in kw.items() if v is not None})
    return p


def blob(m):
    """回报哨兵：payload 全文 blob 匹配（吸取 rolling-upgrade 教训，勿只查顶层 topic）。"""
    return json.dumps(m.get("payload") or {}, ensure_ascii=False)


def task_events(history, task_id, to_agent=None):
    out = []
    for m in history:
        p = m.get("payload") or {}
        if p.get("task_id") != task_id: continue
        if to_agent and m.get("to_agent") != to_agent: continue
        out.append(m)
    return out
