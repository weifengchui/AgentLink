# -*- coding: utf-8 -*-
"""hosts/opencode.py — Adapter SDK 宿主参考实现：OpenCode。

把 task_spec 映射为 `opencode run <prompt>` 非交互会话：
  task_spec.intent + params → prompt → stdout == task result
执行契约见 sdk/adapter.py CommandRunner（并读入 params.timeout_seconds）。

用法：
    from adapter import TaskAdapter, TaskBus
    from hosts.opencode import OpenCodeRunner
    bus = TaskBus(token=…, agent="oc-worker")
    TaskAdapter(bus, runner=OpenCodeRunner(), capabilities=["cap:shell:safe"])
        .serve_forever()
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from adapter import CommandRunner, RunResult  # noqa: E402


class OpenCodeRunner(CommandRunner):
    """opencode CLI 非交互会话执行 task_spec。"""

    timeout = 300
    bin_name = "opencode"

    def build_command(self, spec):
        prompt = self.prompt(spec)
        return [self.bin_name, "run", prompt]

    def effective_timeout(self, spec):
        # params.timeout_seconds 覆盖默认（每个 spec 自带截止，尊重声明方）
        ts = (spec.get("params") or {}).get("timeout_seconds")
        if isinstance(ts, (int, float)) and 0 < ts <= 3600:
            return int(ts)
        return self.timeout
