#!/bin/bash
set -euo pipefail

# Directories and files
MOUNT_DIR=/etc/agent
DEFAULT=/opt/agent/default_config.river
TARGET=${MOUNT_DIR}/config.river
WAL_DIR=/tmp/agent/wal
AGENT_USER=grafana

# Prepare WAL directory
mkdir -p "$WAL_DIR"
chmod 0775 "$WAL_DIR"

# Ensure mounted config exists; copy default if missing
if [ ! -s "$TARGET" ]; then
    echo "[entrypoint] config missing at $TARGET — copying default"
    mkdir -p "$MOUNT_DIR"
    cp "$DEFAULT" "$TARGET"
    chmod 0644 "$TARGET"
fi

# Fix ownership if running as root
chown -R ${AGENT_USER}:${AGENT_USER} "$MOUNT_DIR" "$WAL_DIR" || true

# Log config checksum
echo "[entrypoint] Using config: $(sha256sum "$TARGET")"

# Run Grafana Agent as non-root
exec su-exec ${AGENT_USER} /usr/bin/grafana-agent --config.file "$TARGET" --positions.directory "$WAL_DIR"
