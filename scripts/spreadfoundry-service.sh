#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

live_engine_enabled() {
  if [[ -n "${SPREAD_LIVE_ENGINE_ENABLED:-}" ]]; then
    [[ "${SPREAD_LIVE_ENGINE_ENABLED:-0}" == "1" ]]
    return
  fi
  [[ -f "$repo_root/var/live_market_engine.env" ]]
}

load_saved_live_engine_env() {
  [[ -f "$repo_root/var/live_market_engine.env" ]] || return 0
  # shellcheck disable=SC1091
  source "$repo_root/var/live_market_engine.env"
}

case "${1:-status}" in
  start)
    if live_engine_enabled; then
      load_saved_live_engine_env
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
