# -*- coding: utf-8 -*-
"""hosts/claude_code.py — Adapter SDK 宿主参考实现：Claude Code。

把 task_spec 映射为 `claude -p <prompt>` 非交互会话：
  task_spec.intent + params → prompt → stdout == task result
执行契约见 sdk/adapter.py CommandRunner（exit 0=成功；非 0=失败；
timeout 先杀再回 retryable 失败）。

用法：
    from adapter import TaskAdapter, TaskBus
    from hosts.claude_code import ClaudeCodeRunner
    bus = TaskBus(token=…, agent="claude-worker")
    TaskAdapter(bus, runner=ClaudeCodeRunner(timeout=300),
                capabilities=["cap:shell:safe"]).serve_forever()
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from adapter import CommandRunner  # noqa: E402


class ClaudeCodeRunner(CommandRunner):
    """claude CLI 非交互会话执行 task_spec。"""

    timeout = 300
    bin_name = "claude"

    def build_command(self, spec):
        prompt = self.prompt(spec)
        return [self.bin_name, "-p", prompt, "--output-format", "text"]
