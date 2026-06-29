#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

pid_file="${SPREAD_AUTO_RESEARCH_PID_FILE:-var/auto_research.pid}"
state_file="${SPREAD_AUTO_RESEARCH_STATE_FILE:-var/auto_research_last.json}"
log_file="${SPREAD_AUTO_RESEARCH_LOG_FILE:-var/auto_research.log}"

is_running() {
  local pid="$1"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

read_pid() {
  if [[ -f "$pid_file" ]]; then
    tr -d '[:space:]' < "$pid_file"
  fi
}

start_research() {
  if [[ -z "${SPREAD_AUTO_RESEARCH_COMMAND:-}" ]]; then
    echo "SPREAD_AUTO_RESEARCH_COMMAND is required" >&2
    exit 2
  fi
  mkdir -p "$(dirname "$pid_file")" "$(dirname "$state_file")" "$(dirname "$log_file")"
  local pid
  pid="$(read_pid || true)"
  if is_running "$pid"; then
    echo "auto research already running pid=$pid"
    return 0
  fi
  rm -f "$pid_file"
  nohup "$repo_root/scripts/auto-research-loop.sh" loop >> "$log_file" 2>&1 &
  pid="$!"
  echo "$pid" > "$pid_file"
  echo "started auto research pid=$pid state=$state_file log=$log_file"
}

stop_research() {
  local pid
  pid="$(read_pid || true)"
  if ! is_running "$pid"; then
    rm -f "$pid_file"
    echo "auto research stopped"
    return 0
  fi
  kill "$pid" 2>/dev/null || true
  for _ in {1..20}; do
    if ! is_running "$pid"; then
      rm -f "$pid_file"
      echo "stopped auto research pid=$pid"
      return 0
    fi
    sleep 0.5
  done
  kill -9 "$pid" 2>/dev/null || true
  rm -f "$pid_file"
  echo "force-stopped auto research pid=$pid"
}

status_research() {
  local pid
  pid="$(read_pid || true)"
  if is_running "$pid"; then
    echo "auto research running pid=$pid"
  else
    echo "auto research not running"
  fi
  if [[ -f "$state_file" ]]; then
    cat "$state_file"
  else
    echo "no state file at $state_file"
  fi
}

case "${1:-status}" in
  start)
    start_research
    ;;
  stop)
    stop_research
    ;;
  restart)
    stop_research
    start_research
    ;;
  status)
    status_research
    ;;
  once)
    "$repo_root/scripts/auto-research-loop.sh" once
    ;;
  log)
    mkdir -p "$(dirname "$log_file")"
    touch "$log_file"
    tail -n "${SPREAD_AUTO_RESEARCH_LOG_LINES:-80}" "$log_file"
    ;;
  *)
    echo "usage: $0 {start|stop|restart|status|once|log}" >&2
    exit 2
    ;;
esac
