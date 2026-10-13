#!/usr/bin/env bash
# =============================================================================
# AgentLink 唤醒钩子 · 一键安装（傻瓜版）
# 用法（新机器 WSL 里，解包后在 wake-kit/ 目录执行）:
#     bash install-all.sh                 # 默认唤醒 hermes profile "study"
#     bash install-all.sh fanbot          # 指定 profile
#     bash install-all.sh study liteguard # 第2参数=桥订阅 client 名（默认同 profile）
#     PORT=8645 bash install-all.sh x     # webhook 端口冲突时换端口
# 幂等：重复执行安全（解释器/路由已存在则复用）。
# =============================================================================
set -e
P="${1:-study}"
AGENT="${2:-$P}"
PORT="${PORT:-8644}"
BRIDGE_DIR="${BRIDGE_DIR:-/mnt/d/HermesSpace/.fleet-B1/agentlink}"
SRC="$(cd "$(dirname "$0")" && pwd)"

say(){ printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }
fail(){ printf '\n\033[1;31mXX %s\033[0m\n' "$*"; exit 1; }

say "0/6 预检"
command -v hermes  >/dev/null || fail "没找到 hermes CLI"
command -v openssl >/dev/null || fail "没找到 openssl"
curl -s -m2 --noproxy '*' "http://127.0.0.1:8765/v1/health" >/dev/null \
  || echo "   提示：本机节点 8765 没活——先双击 启动.bat（或 agentlink.exe run）再继续"

say "1/6 桥目录与 python 环境"
mkdir -p "$BRIDGE_DIR/logs"

find_python() {
  local c
  for c in "$BRIDGE_DIR/venv/bin/python" \
           /mnt/d/HermesSpace/LiteGuard/agentlink/venv/bin/python \
           "$HOME/venv/bin/python" python3; do
    if command -v "$c" >/dev/null 2>&1 && "$c" -c "import websocket" >/dev/null 2>&1; then
      echo "$c"; return 0
    fi
  done
  return 1
}

PYEXE=$(find_python) || PYEXE=""
if [ -z "$PYEXE" ]; then
  if python3 -m pip --version >/dev/null 2>&1; then
    python3 -m pip -q install --user websocket-client 2>/dev/null || true
    PYEXE=$(find_python) || true
  fi
fi
if [ -z "$PYEXE" ]; then
  rm -rf "$BRIDGE_DIR/venv"
  python3 -m venv "$BRIDGE_DIR/venv" 2>/dev/null \
    && "$BRIDGE_DIR/venv/bin/pip" -q install websocket-client 2>/dev/null || true
  PYEXE=$(find_python) || fail "找不到带 websocket-client 的 python。解法任一：sudo apt install python3-websocket（+python3-venv）；或用有网机器 pip download websocket-client 后本地安装"
fi
# 清掉半坏的 venv 残骸，避免下次误判
[ -e "$BRIDGE_DIR/venv/bin/python" ] && ! "$BRIDGE_DIR/venv/bin/python" -c "import websocket" 2>/dev/null && rm -rf "$BRIDGE_DIR/venv" || true
cp -f "$SRC/hermes_bridge.py" "$SRC/wake_hook.py" "$BRIDGE_DIR/"
echo "   ok: 解释器=$PYEXE"

say "2/6 webhook 平台 (profile=$P port=$PORT)"
hermes -p "$P" config set platforms.webhook.enabled true           >/dev/null
hermes -p "$P" config set platforms.webhook.extra.host "127.0.0.1" >/dev/null
hermes -p "$P" config set platforms.webhook.extra.port "$PORT"     >/dev/null
CFGYAML="$HOME/.hermes/profiles/$P/config.yaml"
CUR_SECRET=$(sed -n '/^platforms:/,/^[a-z_]*:/p' "$CFGYAML" 2>/dev/null | grep -m1 'secret:' | awk '{print $2}')
[ -n "$CUR_SECRET" ] || hermes -p "$P" config set platforms.webhook.extra.secret "$(openssl rand -hex 24)" >/dev/null
hermes -p "$P" gateway restart >/dev/null 2>&1 || true
for i in $(seq 1 8); do
  curl -s -m2 --noproxy '*' "http://127.0.0.1:$PORT/health" 2>/dev/null | grep -q '"ok"' && break
  sleep 2
done
curl -s -m2 --noproxy '*' "http://127.0.0.1:$PORT/health" 2>/dev/null | grep -q '"ok"' \
  || fail "webhook :$PORT 没起来（可能被别的 profile 占用——用 PORT=8645 重跑）"
echo "   ok: :$PORT 监听中"

say "3/6 路由 agentlink-msg"
SUBS="$HOME/.hermes/profiles/$P/webhook_subscriptions.json"
if [ ! -f "$SUBS" ] || ! grep -q agentlink-msg "$SUBS"; then
  hermes -p "$P" webhook subscribe agentlink-msg \
    --prompt "【AgentLink 总线新消息】来自 {from}（agent {from_agent}）：主题={topic}
内容：{content}
msg_id={msg_id}
如果这条消息需要你行动（指令/升级协调/问询），现在处理并按需回复；纯通告类记录即可。" >/dev/null
  echo "   已创建路由"
else
  echo "   路由已存在，复用"
fi
SECRET=$("$PYEXE" - "$SUBS" <<'PY'
import sys,json
print(json.load(open(sys.argv[1]))['agentlink-msg']['secret'])
PY
)
[ -n "$SECRET" ] || fail "读不到路由 secret"

say "4/6 wake.json"
cat > "$BRIDGE_DIR/wake.json" <<EOF
{"url": "http://127.0.0.1:$PORT/webhooks/agentlink-msg", "secret": "$SECRET", "agent": "$AGENT", "skip_topics": []}
EOF
echo "   ok"

say "5/6 启动桥 (client=$AGENT)"
PIDF="$BRIDGE_DIR/bridge.pid"
[ -f "$PIDF" ] && kill -0 "$(cat "$PIDF")" 2>/dev/null && { kill "$(cat "$PIDF")" 2>/dev/null || true; sleep 1; }
cd "$BRIDGE_DIR"
BRIDGE_AGENT="$AGENT" BRIDGE_PORT=8767 setsid nohup "$PYEXE" hermes_bridge.py >> logs/bridge.out 2>&1 &
echo $! > "$PIDF"
sleep 3
curl -s -m3 --noproxy '*' "http://127.0.0.1:8767/status" 2>/dev/null | grep -q '"ws_connected": *true' \
  && echo "   ok: 桥已连上节点" \
  || echo "   警告：桥 ws 未确认，看 $BRIDGE_DIR/logs/bridge.out"

say "6/6 端到端自检（模拟总线消息唤醒）"
BODY="{\"from\":\"selftest\",\"from_agent\":\"selftest\",\"topic\":\"wake_probe\",\"content\":\"钩子安装自检，无需行动\",\"msg_id\":\"probe\",\"to\":\"$AGENT\"}"
SIG="sha256=$(printf '%s' "$BODY" | "$PYEXE" -c 'import sys,hmac,hashlib;print(hmac.new(sys.argv[1].encode(),sys.stdin.buffer.read(),hashlib.sha256).hexdigest())' "$SECRET")"
RESP=$(curl -s -m15 --noproxy '*' -X POST "http://127.0.0.1:$PORT/webhooks/agentlink-msg" \
  -H "Content-Type: application/json" -H "X-Hub-Signature-256: $SIG" -d "$BODY" || true)
echo "$RESP" | grep -q accepted && echo "   ok: webhook 已接受触发（对应 profile 会话将被唤醒）" \
  || echo "   注意: 自检响应=${RESP:-无}（若 401 稍后重跑本脚本即可；不影响桥收信进 inbox）"

printf '\n\033[1;32m完成 ✅\033[0m\n'
cat <<DONE
 桥目录 : $BRIDGE_DIR （状态口: curl http://127.0.0.1:8767/status）
 唤醒去 : hermes profile '$P'
 下一步 : 该机 agent 上线时照 SKILL.md 注册身份（client/agent_id 用 '$AGENT'）
 验证   : 从别的机器向本机 to_agent=$AGENT 发条消息，
          几秒内 ~/.hermes/profiles/$P/sessions/ 应冒出处理它的新会话
DONE
