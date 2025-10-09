#!/bin/bash
# Entrypoint script for Grafana Agent — ensures config exists and WAL dir is ready

set -e

# Create WAL directory
mkdir -p /tmp/agent/wal
chmod -R 0755 /tmp/agent/wal

# Check if /etc/agent/config.river exists and is non-empty
if [ ! -s /etc/agent/config.river ]; then
  echo "Config not found or empty, copying default..."
  mkdir -p /etc/agent
  cp /opt/agent/default_config.river /etc/agent/config.river
  chmod 0644 /etc/agent/config.river
fi

# Execute Grafana Agent
exec /usr/bin/grafana-agent run --config.file=/etc/agent/config.river
