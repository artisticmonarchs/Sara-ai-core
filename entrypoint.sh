#!/bin/bash
set -euo pipefail

MOUNT_DIR=/etc/agent
DEFAULT=/opt/agent/default_config.river
TARGET=${MOUNT_DIR}/config.river
WAL_DIR=/tmp/agent/wal
AGENT_USER=grafana

echo "[entrypoint] started"

# Ensure WAL dir exists
mkdir -p "$WAL_DIR"
chmod 0775 "$WAL_DIR"

# Ensure mount dir exists
mkdir -p "$MOUNT_DIR"

# Copy default config if mount is empty
if [ ! -s "$TARGET" ]; then
    echo "[entrypoint] config missing at $TARGET — copying default"
    cp "$DEFAULT" "$TARGET"
    chmod 0644 "$TARGET"
fi

# Fix ownership for non-root agent user
chown -R ${AGENT_USER}:${AGENT_USER} "$MOUNT_DIR" "$WAL_DIR" || true

# Log config checksum
echo "[entrypoint] Using config: $(sha256sum "$TARGET")"

# Run agent as PID 1
exec su-exec ${AGENT_USER} /usr/bin/grafana-agent \
    --config.file "$TARGET" \
    --positions.directory "$WAL_DIR"
