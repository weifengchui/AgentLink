# -*- coding: utf-8 -*-
"""adapter — AgentLink Adapter SDK 核心（参考实现正本，v0.6）。

把宿主 agent（Hermes / Claude Code / OpenCode / …）接入标准 Task Stream 协议
（docs/task-protocol.md）：收 task_spec → 预检 → 回 task_accepted/progress/
succeeded/failed/declined/cancelled，语义与 scripts/task-proto/executor.py
参考执行者完全同构，铁律一致：

- 执行者自治：requires 能力门 / runner.preflight 预检不过 → task_declined
  （终态，带原因），编排方不得绕过；adapter 内核不替宿主做任何业务决定；
- 幂等：(task_id, seq) 业务级去重，总线帧级去重之外的双保险，重放回 task_ack
  （note=dup-dropped），绝不二次执行；
- 可追溯：所有回报带 reply_to（指向收到的 spec 帧 msg_id），history 可重建轨迹；
- 预算：runner 失败标 retryable=True 的 task_failed 不算终态（编排方在
  spec.budget.max_retries 内重发 seq+1 的 spec），标 False 即终态；
- 取消：task_cancel / task_rollback 置停标志回调宿主 runner.on_cancel →
  收敛后回 task_cancelled / task_rolled_back 终态。

依赖：仅 stdlib + websocket-client（与 task-proto 参考实现同口径）。
零内核改动：全部语义是总线 kind=msg 帧 payload 层约定。
"""
import datetime
import json
import threading
import time
import urllib.error
import urllib.request

# --------------------------------------------------------------------------
# 总线客户端（tasklib.Bus 口径的独立拷贝，SDK 自包含、不依赖 scripts/ 目录）
# --------------------------------------------------------------------------


class TaskBus:
    """节点 HTTP/WS 客户端。base=本机回环 API 地址，绝不指远端节点。"""

    def __init__(self, base="http://127.0.0.1:8765", token="", agent=""):
        self.base, self.token, self.agent = base.rstrip("/"), token, agent
        self.node = (self.health() or {}).get("node_id", "")

    def _api(self, path, body=None, timeout=10):
        data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
        req = urllib.request.Request(
            self.base + path, data=data,
            headers={"X-Auth-Token": self.token, "Content-Type": "application/json"},
            method="POST" if body is not None else "GET")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            try:
                return json.loads(e.read().decode())
            except Exception:
                return {"ok": False, "error": f"http {e.code}"}
        except Exception:
            return {"ok": False, "error": "unreachable"}

    def health(self):
        return self._api("/v1/health")

    def history(self, limit=200):
        """GET /v1/history：脱敏观测面，编排方按结构键（task_id/topic）匹配。"""
        r = self._api(f"/v1/history?limit={int(limit)}")
        return r.get("history", [])

    def register(self, capabilities=None):
        return self._api("/v1/agents",
                         {"agent_id": self.agent, "name": self.agent,
                          "capabilities": list(capabilities or [])})

    def send(self, to_agent, payload, reply_to=None, to_node=None):
        """直发 payload（to_node=None 即本节点内置投递）。"""
        body = {"from": self.node or "", "to": to_node or self.node or "",
                "to_agent": to_agent, "from_agent": self.agent,
                "type": "text", "action": "send", "payload": payload}
        if reply_to:
            body["reply_to"] = reply_to
        return self._api("/v1/send", body)

    def ws_url(self):
        return self.base.replace("http://", "ws://") + \
            f"/v1/ws?token={self.token}&client={self.agent}"


# --------------------------------------------------------------------------
# Runner 契约（宿主适配器实现此接口；sdk/hosts/ 提供三个参考实现）
# --------------------------------------------------------------------------


class RunResult(dict):
    """runner.run() 返回。dict 兼容裸 dict；缺省 ok=True（报败必须显式）。"""

    def __init__(self, ok=True, result=None, error=None, retryable=False):
        super().__init__({"ok": ok, "result": result, "error": error,
                          "retryable": retryable})


class BaseRunner:
    """宿主 runner 契约。宿主适配器子类实现 preflight / run / on_cancel。"""

    def preflight(self, spec):
        """返回 None=放行；返回 str=拒绝原因（→ task_declined 终态）。

        通用预检（requires 能力门、deadline 过期）由 adapter 内核先做；
        这里只放宿主特有判断（host CLI 是否在场、params 结构符不符等）。"""
        return None

    def run(self, spec, report, cancelled):
        """执行 spec（task_spec payload）。返回 RunResult。

        report(stage=…, **kw)：进度上报（自动编 task_progress 递增 seq）
        cancelled()：轮询置停标志；runner 应尽快收敛(run() 后内核不再发包)
        """
        raise NotImplementedError

    def on_cancel(self, spec):
        """task_cancel / task_rollback 到达时回调（可选：杀子进程等）。"""
        pass


# --------------------------------------------------------------------------
# CommandRunner —— CLI 型宿主（claude_code / opencode）参考实现的公共底座
# --------------------------------------------------------------------------


class CommandRunner(BaseRunner):
    """把 task_spec 映射为宿主 CLI 非交互会话的通用 runner。

    契约：exit 0 == 成功（task_succeeded，result=stdout 尾部 4KB）；
    非 0 == task_failed（retryable=False；信号致死/超时 retryable=True）；
    超时先 kill 再回败。子类只需给 build_command()。"""

    timeout = 300

    def effective_timeout(self, spec):
        """单任务生效超时（子类可按 spec.params 覆盖；线程安全勿改 self）。"""
        return self.timeout

    def build_command(self, spec):
        """task_spec → 宿主 CLI argv。子类唯一必做。"""
        raise NotImplementedError

    @staticmethod
    def prompt(spec):
        s = spec.get("intent") or spec.get("type") or ""
        params = spec.get("params") or {}
        if params:
            s += "\n\n任务参数：\n" + json.dumps(params, ensure_ascii=False, indent=2)
        return s

    def preflight(self, spec):
        import shutil
        argv = self.build_command(spec)
        if not argv or shutil.which(argv[0]) is None:
            return f"host CLI 不在场: {argv[0] if argv else '?'}"
        return None

    def run(self, spec, report, cancelled):
        import subprocess
        argv = self.build_command(spec)
        report(stage="spawn", argv=argv)
        p = subprocess.Popen(argv, stdin=subprocess.DEVNULL,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             text=True)
        self._child = p
        t_out = self.effective_timeout(spec)
        try:
            out, _ = p.communicate(timeout=t_out)
        except subprocess.TimeoutExpired:
            p.kill()
            out, _ = p.communicate()
            return RunResult(ok=False, error=f"timeout({t_out}s)", retryable=True)
        report(stage="done", exit=p.returncode)
        if cancelled():
            return RunResult(ok=False, error="cancelled")
        out = (out or "")[-4000:]
        if p.returncode == 0:
            return RunResult(ok=True, result=(out or "").strip())
        return RunResult(ok=False,
                         error=f"exit={p.returncode}: {(out or '').strip()[:400]}",
                         retryable=p.returncode < 0)

    def on_cancel(self, spec):
        child = getattr(self, "_child", None)
        if child:
            try:
                child.kill()
            except Exception:
                pass


# --------------------------------------------------------------------------
# TaskAdapter —— task_spec 状态机内核
# --------------------------------------------------------------------------


class TaskAdapter:
    """执行者侧适配器内核。

    用法：
        bus = TaskBus("http://127.0.0.1:8765", token, agent="worker")
        adapter = TaskAdapter(bus, runner=MyRunner(), capabilities=["cap:file"])
        adapter.serve_forever()   # 自动重连 + 重注册；测试可 handle_frame 直调
    """

    def __init__(self, bus, runner, capabilities=None, log=None,
                 frame_hook=None):
        self.bus, self.runner, self.capabilities = bus, runner, list(capabilities or [])
        self.log = log
        self.frame_hook = frame_hook            # 可选：逐帧透传给宿主 runner
        self._stop = threading.Event()
        self._ws = None                         # 当前 WS 连接（stop() 硬关）
        self._canceled = {}                     # task_id -> Event（运行中置停）
        self._rollback = {}                     # task_id -> True（经 task_rollback）
        self.seen = {}                          # (task_id, seq) -> True 幂等
        self.in_flight = {}                     # task_id -> spec（供 on_cancel）
        self.stats = {"spec": 0, "dup": 0, "declined": 0, "succeeded": 0,
                      "failed": 0, "canceled": 0, "progress": 0}

    # ---- 对外 ----

    def serve_forever(self):
        """WS 订阅主循环（铁律同桥：ping 保活、指数退避 1→30s、重连即重注册）。"""
        if self.log:
            self.log(f"[adapter {self.bus.agent}] 启动 @ {self.bus.base} "
                     f"≢caps={self.capabilities}")
        import websocket
        backoff = 1
        ws = None
        while not self._stop.is_set():
            try:
                self.bus.register(self.capabilities)
                ws = websocket.create_connection(self.bus.ws_url(), timeout=None,
                                                 ping_interval=25, ping_timeout=10)
                self._ws = ws
                if self.log:
                    self.log(f"[adapter {self.bus.agent}] ws 已连")
                backoff = 1
                while not self._stop.is_set():
                    raw = ws.recv()
                    if not raw:
                        continue
                    try:
                        env = json.loads(raw)
                    except Exception:
                        continue
                    try:
                        self.handle_frame(env)
                    except Exception as e:
                        if self.log:
                            self.log(f"[adapter {self.bus.agent}] 帧处理异常: {e}")
            except Exception as e:
                if self.log:
                    self.log(f"[adapter {self.bus.agent}] ws 断开: "
                             f"{str(e)[:120] or type(e).__name__}，{backoff}s 后重连")
            finally:
                try:
                    if ws:
                        ws.close()
                except Exception:
                    pass
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

    def stop(self):
        self._stop.set()
        ws = self._ws
        if ws:
            try:
                ws.close()          # 硬关连接：节点即判定 agent 下线（client_offline）
            except Exception:
                pass
        for ev in self._canceled.values():
            ev.set()

    def handle_frame(self, env):
        """单帧处理（serve_forever 与冒烟共用）。未知 topic 忽略（向前兼容）。"""
        if env.get("type") not in ("text", "file"):
            return None
        p = env.get("payload") or {}
        topic = p.get("topic") or ""
        tid = p.get("task_id")
        origin = env.get("from_agent") or "orchestrator"
        reply_to = env.get("reply_to") or None

        if topic in ("task_cancel", "task_rollback"):
            if topic == "task_rollback":
                self._rollback[tid] = True
            ev = self._canceled.get(tid)
            if ev:
                ev.set()
            spec = self.in_flight.get(tid)
            if spec:
                threading.Thread(target=self.runner.on_cancel, args=(spec,),
                                 daemon=True).start()
            return topic, tid
        if topic == "task_spec":
            key = (tid, p.get("seq", 1))
            if key in self.seen:
                self.stats["dup"] += 1
                self._reply(origin, "task_ack", tid, reply_to=reply_to,
                            note="dup-dropped")
                return "dup", tid
            self.seen[key] = True
            self.stats["spec"] += 1
            threading.Thread(target=self._run_task,
                             args=(p, origin, reply_to), daemon=True).start()
            return "spec", tid
        if self.frame_hook is not None and env.get("reply_to"):
            try:
                self.frame_hook(env)
            except Exception:
                pass
        return topic, tid

    # ---- 内部 ----

    def _reply(self, origin, topic, tid, reply_to=None, **kw):
        p = {"topic": topic, "task_id": tid}
        p.update(kw)
        return self.bus.send(origin, p, reply_to=reply_to)

    def _run_task(self, spec, origin, reply_to):
        tid = spec["task_id"]
        pen = threading.Event()
        self._canceled[tid] = pen
        self.in_flight[tid] = spec
        # 通用预检 1：能力门（缺 → declined 终态 + reason）
        for cap in spec.get("requires") or []:
            if cap not in self.capabilities:
                self._decline(origin, tid, reply_to, f"missing cap:{cap}")
                return self._finish(tid)
        # 通用预检 2：deadline 已过
        if spec.get("deadline") and _past_deadline(spec["deadline"]):
            self._decline(origin, tid, reply_to, "deadline_passed")
            return self._finish(tid)
        # 宿主预检
        try:
            reason = self.runner.preflight(spec)
        except Exception as e:
            reason = f"preflight_exc: {e}"
        if reason:
            self._decline(origin, tid, reply_to, reason)
            return self._finish(tid)
        # 承接
        self._reply(origin, "task_accepted", tid, reply_to=reply_to)
        seq = [0]

        def report(**kw):
            seq[0] += 1
            self.stats["progress"] += 1
            p = {"topic": "task_progress", "task_id": tid, "seq": seq[0]}
            p.update({k: v for k, v in kw.items() if v is not None})
            self.bus.send(origin, p, reply_to=reply_to)
        # 执行
        try:
            out = self.runner.run(spec, report, pen.is_set)
        except Exception as e:
            out = RunResult(ok=False, error=f"runner_exc: {e}", retryable=True)
        out = RunResult(**out) if isinstance(out, dict) else out
        if pen.is_set():
            # 置停标志收敛 → cancelled / rolled_back 终态（runner 结果作废）
            self.stats["canceled"] += 1
            topic = "task_rolled_back" if self._rollback.pop(tid, False) \
                else "task_cancelled"
            self._reply(origin, topic, tid, reply_to=reply_to)
        elif out.get("ok"):
            self.stats["succeeded"] += 1
            self._reply(origin, "task_succeeded", tid, reply_to=reply_to,
                        result=out.get("result"))
        else:
            self.stats["failed"] += 1
            self._reply(origin, "task_failed", tid, reply_to=reply_to,
                        error=str(out.get("error") or "runner 报败")[:400],
                        retryable=bool(out.get("retryable")))
        self._finish(tid)

    def _decline(self, origin, tid, reply_to, reason):
        self.stats["declined"] += 1
        self._reply(origin, "task_declined", tid, reply_to=reply_to,
                    reason=str(reason)[:200])

    def _finish(self, tid):
        self._canceled.pop(tid, None)
        self.in_flight.pop(tid, None)


def _past_deadline(dl):
    """deadline 过期判定（RFC3339 宽松解析；解析不了 = 不拦）。"""
    try:
        return datetime.datetime.fromisoformat(str(dl).replace("Z", "+00:00")) \
            < datetime.datetime.now(datetime.timezone.utc)
    except Exception:
        return False
