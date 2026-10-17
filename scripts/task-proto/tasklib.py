"""tasklib — AgentLink 任务协议 v1 共享库（参考实现）。

对应 docs/task-protocol.md。仅 stdlib + websocket-client。
用途：隔离测试总线上跑通任务生命周期；生产接入请按 README 调整。
"""
import json, time, uuid, urllib.request, urllib.parse, urllib.error

class Bus:
    def __init__(self, base="http://127.0.0.1:18765", token="", agent=""):
        self.base, self.token, self.agent = base, token, agent
        self.node = self.health()["node_id"]
        self._features = None                # /v1/health features 探测缓存
        self.stats = {"events": 0, "history": 0, "tasks": 0}  # 观测路径调用计数
        self.last_wait_transport = "?"       # wait() 实际传输：events/events+rebuild/history

    def _api(self, path, body=None):
        data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
        req = urllib.request.Request(self.base + path, data=data,
            headers={"X-Auth-Token": self.token, "Content-Type": "application/json"},
            method="POST" if body is not None else "GET")
        try:
            with urllib.request.urlopen(req, timeout=8) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            # 非 2xx 也回读 JSON 体（/v1/events 环旋转以 409 返回），带 _http_status 标记
            try:
                out = json.loads(e.read().decode())
            except Exception:
                out = {"ok": False, "error": str(e)}
            if not isinstance(out, dict):
                out = {"ok": False, "error": out}
            out["_http_status"] = e.code
            return out

    def health(self):        return self._api("/v1/health")

    def features(self):
        """特性探测：/v1/health 的 features 数组（v0.5+ 含 events/tasks/redact）。
        旧节点缺字段/请求失败 → 空集（调用方据此静默回退兼容路径）。"""
        if self._features is None:
            try:
                self._features = frozenset(self.health().get("features") or [])
            except Exception:
                self._features = frozenset()
        return self._features

    def supports(self, name): return name in self.features()

    def history(self, limit=200):
        self.stats["history"] += 1
        return self._api(f"/v1/history?limit={limit}").get("history", [])

    def events(self, since=0, agent=None, types=None, limit=None):
        """GET /v1/events 增量事件流：{events:[{seq,ts,frame}], next_seq, oldest_seq}。
        since 传上一轮 next_seq 续传；环旋转丢件时返回 409
        （{_http_status:409, error:'since_before_history', oldest_seq, next_seq}），
        调用方应以 /v1/history 全量重建一次后从 next_seq 继续增量。
        注意：这是脱敏观测面（payload 敏感字段为 <REDACTED:*>），匹配只用
        结构键（payload.task_id / topic、msg_id），原文 blob 匹配仅限目标
        agent 的 WS 收件侧（见 blob() 说明）。"""
        q = {"since": int(since)}
        if agent: q["agent"] = agent
        if types: q["types"] = ",".join(types) if isinstance(types, (list, tuple)) else str(types)
        if limit: q["limit"] = int(limit)
        self.stats["events"] += 1
        return self._api("/v1/events?" + urllib.parse.urlencode(q))

    def tasks(self, correlation=None, task_id=None, agent=None, state=None, limit=None):
        """GET /v1/tasks 任务面板投影：{tasks:[{task_id,state,terminal,correlation,...}], total}。
        state 为投影终态（succeeded/failed/declined/cancelled/...），同为脱敏观测面。"""
        q = {}
        if correlation: q["correlation"] = correlation
        if task_id:     q["task_id"] = task_id
        if agent:       q["agent"] = agent
        if state:       q["state"] = ",".join(state) if isinstance(state, (list, tuple)) else str(state)
        if limit:       q["limit"] = int(limit)
        self.stats["tasks"] += 1
        return self._api("/v1/tasks?" + urllib.parse.urlencode(q))
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
    """payload 全文 blob（回报哨兵用）。

    已知坑（v0.5 脱敏面）：/v1/history、/v1/events、/v1/tasks 返回的都是
    观测方脱敏件——payload 字符串值命中规则（内含 key:value 形态的
    token/secret/password、32+ 位 hex 摘要、PEM 块）会变为 <REDACTED:*> 占位。
    原文 blob 匹配只在【目标 agent 的 WS 收件侧】有效（executor.py 收到的帧是
    原文）；编排方在 events/history 面上匹配必须改用不在脱敏面内的结构键
    （payload.task_id——uuid 短 hex 段不命中规则、topic、msg_id），
    勿把旧 history 原文匹配逻辑直接平移过来。"""
    return json.dumps(m.get("payload") or {}, ensure_ascii=False)


def task_events(history, task_id, to_agent=None):
    out = []
    for m in history:
        p = m.get("payload") or {}
        if p.get("task_id") != task_id: continue
        if to_agent and m.get("to_agent") != to_agent: continue
        out.append(m)
    return out
