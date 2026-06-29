#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

pid_file="${SPREAD_CANARY_PID_FILE:-var/canary_worker.pid}"
health_output="${SPREAD_CANARY_HEALTH_OUTPUT:-var/canary_worker_health.json}"
log_file="${SPREAD_CANARY_LOG_FILE:-var/canary_worker.log}"
spreadfoundry_bin="${SPREAD_BINARY:-target/release/spreadfoundry}"

is_running() {
  local pid="$1"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

read_pid() {
  if [[ -f "$pid_file" ]]; then
    tr -d '[:space:]' < "$pid_file"
  fi
}

start_worker() {
  mkdir -p "$(dirname "$pid_file")" "$(dirname "$health_output")" "$(dirname "$log_file")"
  local pid
  pid="$(read_pid || true)"
  if is_running "$pid"; then
    echo "canary worker already running pid=$pid"
    return 0
  fi
  rm -f "$pid_file"
  (
    export SPREAD_CANARY_ONCE=0
    export SPREAD_CANARY_HEALTH_OUTPUT="$health_output"
    exec "$repo_root/scripts/run_canary_worker.sh"
  ) >> "$log_file" 2>&1 &
  pid="$!"
  echo "$pid" > "$pid_file"
  echo "started canary worker pid=$pid health=$health_output log=$log_file"
}

stop_worker() {
  local pid
  pid="$(read_pid || true)"
  if ! is_running "$pid"; then
    rm -f "$pid_file"
    echo "canary worker stopped"
    return 0
  fi
  kill "$pid" 2>/dev/null || true
  for _ in {1..20}; do
    if ! is_running "$pid"; then
      rm -f "$pid_file"
      echo "stopped canary worker pid=$pid"
      return 0
    fi
    sleep 0.5
  done
  kill -9 "$pid" 2>/dev/null || true
  rm -f "$pid_file"
  echo "force-stopped canary worker pid=$pid"
}

snapshot_worker() {
  if [[ -x "$spreadfoundry_bin" ]]; then
    "$spreadfoundry_bin" canary-worker-snapshot \
      --health-output "$health_output" \
      --pid-file "$pid_file" \
      --json
  else
    cargo run --quiet --release -- canary-worker-snapshot \
      --health-output "$health_output" \
      --pid-file "$pid_file" \
      --json
  fi
}

case "${1:-status}" in
  start)
    start_worker
    ;;
  stop)
    stop_worker
    ;;
  restart)
    stop_worker
    start_worker
    ;;
  status|snapshot)
    snapshot_worker
    ;;
  log)
    mkdir -p "$(dirname "$log_file")"
    touch "$log_file"
    tail -n "${SPREAD_CANARY_LOG_LINES:-80}" "$log_file"
    ;;
  *)
    echo "usage: $0 {start|stop|restart|status|snapshot|log}" >&2
    exit 2
    ;;
esac
