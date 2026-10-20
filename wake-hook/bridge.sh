#!/bin/bash
# ⚠ 本脚本是 50 号机(liteguard)专属实现：路径 E:\Tools\AgentLink-v*、HOME=/home/yxf、
#   daemon.py 布局都只在该机成立。**其他机器请直接用同目录 install-all.sh 一键安装，
#   不要跑这个脚本。** 保留仅作架构参考。
# Start / check AgentLink bridge daemon (detached, survives session)
D=/mnt/d/HermesSpace/LiteGuard/agentlink
PIDF="$D/daemon.pid"
LOGF="$D/logs/daemon.out"
mkdir -p "$D/logs"

is_up() { [ -f "$PIDF" ] && kill -0 "$(cat "$PIDF")" 2>/dev/null; }

ensure_node() {
  curl -s -m2 --noproxy '*' http://127.0.0.1:8765/v1/health >/dev/null 2>&1 && { echo "node already up"; return 0; }
  local dir win i
  dir=$(ls -d /mnt/e/Tools/AgentLink-v* 2>/dev/null | sort -V | tail -1)
  [ -n "$dir" ] || { echo "WARN: node dir not found"; return 1; }
  win=$(wslpath -w "$dir" 2>/dev/null)
  [ -n "$win" ] || { echo "WARN: wslpath failed for $dir"; return 1; }
  # 必须 Windows exe 形态拉起：WSL 内跑 linux 版入站会被 Windows 防火墙挡，
  # 远端节点连不进来（2026-10-13 实测致 7/50 互相离线事故）
  /mnt/c/Windows/System32/cmd.exe /c "cd /d $win && start agentlink.exe run --no-open" >/dev/null 2>&1
  for i in 1 2 3 4 5 6 7 8; do
    sleep 1
    curl -s -m2 --noproxy '*' http://127.0.0.1:8765/v1/health >/dev/null 2>&1 && { echo "node up ($win via agentlink.exe)"; return 0; }
  done
  echo "WARN: node failed to start"; return 1
}

case "${1:-start}" in
  start)
    ensure_node
    if is_up; then echo "already running (pid $(cat "$PIDF"))"; exit 0; fi
    cd "$D"
    HOME=/home/yxf AGENTLINK_CONFIG="${AGENTLINK_CONFIG:-/mnt/c/Users/yxf/AppData/Local/AgentLink/agentlink.json}" setsid nohup "$D/venv/bin/python" "$D/daemon.py" >> "$LOGF" 2>&1 &
    echo $! > "$PIDF"
    sleep 2
    if is_up; then echo "started pid $(cat "$PIDF")"; else echo "FAILED, see $LOGF"; tail -5 "$LOGF"; exit 1; fi
    ;;
  status)
    if is_up; then echo "running pid $(cat "$PIDF")"; curl -s --noproxy '*' -m 5 http://127.0.0.1:8766/status; echo; else echo "not running"; fi
    ;;
  stop)
    if is_up; then kill "$(cat "$PIDF")" && rm -f "$PIDF" && echo stopped; else echo "not running"; fi
    ;;
esac
