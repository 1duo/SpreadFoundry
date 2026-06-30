#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

case "${1:-status}" in
  start)
    if [[ "${SPREAD_LIVE_ENGINE_ENABLED:-0}" == "1" ]]; then
      "$repo_root/scripts/signal-refresh-service.sh" stop
      SPREAD_LIVE_SIGNAL_ARTIFACT="${SPREAD_LIVE_ENGINE_SOURCE_ARTIFACT:-var/live_signal_refresh_source.json}" \
        "$repo_root/scripts/signal-refresh-service.sh" start
      "$repo_root/scripts/live-market-engine-service.sh" start
    else
      "$repo_root/scripts/live-market-engine-service.sh" stop
      "$repo_root/scripts/signal-refresh-service.sh" start
    fi
    "$repo_root/scripts/execution-service.sh" start
    "$repo_root/scripts/menubar-service.sh" start
    ;;
  stop)
    "$repo_root/scripts/execution-service.sh" stop
    "$repo_root/scripts/live-market-engine-service.sh" stop
    "$repo_root/scripts/signal-refresh-service.sh" stop
    "$repo_root/scripts/menubar-service.sh" stop
    ;;
  shutdown-from-menubar)
    "$repo_root/scripts/execution-service.sh" stop
    "$repo_root/scripts/live-market-engine-service.sh" stop
    "$repo_root/scripts/signal-refresh-service.sh" stop
    "$repo_root/scripts/menubar-service.sh" prepare-quit
    ;;
  restart)
    "$repo_root/scripts/spreadfoundry-service.sh" stop
    "$repo_root/scripts/spreadfoundry-service.sh" start
    ;;
  status)
    "$repo_root/scripts/live-market-engine-service.sh" status
    "$repo_root/scripts/signal-refresh-service.sh" status
    "$repo_root/scripts/execution-service.sh" status
    "$repo_root/scripts/menubar-service.sh" status
    ;;
  migrate-legacy)
    "$repo_root/scripts/signal-refresh-service.sh" migrate-legacy
    "$repo_root/scripts/execution-service.sh" migrate-legacy
    ;;
  *)
    echo "usage: $0 {start|stop|restart|shutdown-from-menubar|status|migrate-legacy}" >&2
    exit 2
    ;;
esac
