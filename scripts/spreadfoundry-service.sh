#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

case "${1:-status}" in
  start)
    "$repo_root/scripts/canary-service.sh" start
    "$repo_root/scripts/menubar-service.sh" start
    ;;
  stop)
    "$repo_root/scripts/menubar-service.sh" stop
    "$repo_root/scripts/canary-service.sh" stop
    ;;
  restart)
    "$repo_root/scripts/spreadfoundry-service.sh" stop
    "$repo_root/scripts/spreadfoundry-service.sh" start
    ;;
  status)
    "$repo_root/scripts/canary-service.sh" status
    ;;
  *)
    echo "usage: $0 {start|stop|restart|status}" >&2
    exit 2
    ;;
esac
