#!/usr/bin/env bash
# Install AgentLink as a systemd service on Linux.
# Usage: sudo ./install-systemd.sh [path-to-agentlink-binary] [path-to-config]
set -euo pipefail

BIN="${1:-$(dirname "$0")/../target/release/agentlink}"
CONF="${2:-/etc/agentlink/agentlink.json}"

echo "==> installing binary: $BIN"
install -m 0755 "$BIN" /usr/local/bin/agentlink

echo "==> creating runtime user and directories"
id -u agentlink &>/dev/null || useradd --system --home /var/lib/agentlink --shell /usr/sbin/nologin agentlink
install -d -o agentlink -g agentlink /var/lib/agentlink /etc/agentlink

echo "==> installing config (edit before first start if it does not exist)"
if [ ! -f "$CONF" ]; then
  # generate a fresh identity, then point data dirs at /var/lib/agentlink
  (cd /tmp && /usr/local/bin/agentlink --config "$CONF" init --name "$(hostname)")
  echo "    generated $CONF — review it, then start the service"
else
  echo "    config already present: $CONF"
fi

echo "==> installing unit"
install -m 0644 "$(dirname "$0")/agentlink.service" /etc/systemd/system/agentlink.service
systemctl daemon-reload

echo "==> firewall: allow UDP 8766 (discovery) and TCP 8765 (peer WS)"
command -v ufw >/dev/null && {
  ufw allow 8765/tcp
  ufw allow 8766/udp
} || true

echo
echo "Done. Next steps:"
echo "  systemctl enable --now agentlink     # start now + on boot"
echo "  systemctl status agentlink          # check status"
echo "  journalctl -u agentlink -f          # follow logs"