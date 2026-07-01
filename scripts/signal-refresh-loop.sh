#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

refresh_command="scripts/refresh-live-signal.sh"
interval_seconds="${SPREAD_SIGNAL_REFRESH_INTERVAL_SECONDS:-300}"
timeout_seconds="${SPREAD_SIGNAL_REFRESH_TIMEOUT_SECONDS:-900}"
state_file="${SPREAD_SIGNAL_REFRESH_STATE_FILE:-var/signal_refresh_last.json}"
detail_state_file="${SPREAD_SIGNAL_REFRESH_DETAIL_STATE_FILE:-var/live_signal_refresh_last.json}"
log_file="${SPREAD_SIGNAL_REFRESH_LOG_FILE:-var/signal_refresh.log}"
mode="${1:-loop}"

json_escape() {
  sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'
}

write_state() {
  local started_at="$1"
  local finished_at="$2"
  local exit_code="$3"
  local escaped_command
  escaped_command="$(printf '%s' "$refresh_command" | json_escape)"
  mkdir -p "$(dirname "$state_file")"
  printf '{\n  "started_at": "%s",\n  "finished_at": "%s",\n  "exit_code": %s,\n  "command": "%s"\n}\n' \
	    "$started_at" "$finished_at" "$exit_code" "$escaped_command" > "$state_file"
}

write_detail_timeout_state() {
  local started_at="$1"
  local finished_at="$2"
  local run_to approved_strategy artifact reason
  run_to="${SPREAD_SIGNAL_REFRESH_TO:-$(date -u '+%Y-%m-%d')}"
  approved_strategy="${SPREAD_APPROVED_STRATEGY:-configs/approved_strategy.json}"
  artifact="${SPREAD_LIVE_SIGNAL_ARTIFACT:-var/live_signal_refresh_source.json}"
  reason="approved strategy signal refresh exceeded ${timeout_seconds}s Rust timeout plus watchdog grace"
  mkdir -p "$(dirname "$detail_state_file")"
  printf '{\n  "started_at": "%s",\n  "finished_at": "%s",\n  "status": "selector_timeout",\n  "exit_code": 124,\n  "run_to": "%s",\n  "run_dir": "",\n  "approved_strategy": "%s",\n  "live_signal_artifact": "%s",\n  "reason": "%s"\n}\n' \
    "$started_at" "$finished_at" "$run_to" \
    "$(printf '%s' "$approved_strategy" | json_escape)" \
    "$(printf '%s' "$artifact" | json_escape)" \
    "$(printf '%s' "$reason" | json_escape)" > "$detail_state_file"
}

terminate_process_tree() {
  local root_pid="$1"
  local signal="${2:-TERM}"
  local child_pid
  while IFS= read -r child_pid; do
    [[ -n "$child_pid" ]] && terminate_process_tree "$child_pid" "$signal"
  done < <(pgrep -P "$root_pid" 2>/dev/null || true)
  kill "-$signal" "$root_pid" 2>/dev/null || true
}

run_once() {
  mkdir -p "$(dirname "$log_file")"
  local started_at finished_at exit_code refresh_pid watchdog_pid timeout_file
  started_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  timeout_file="${state_file}.timeout.$$"
  rm -f "$timeout_file"
  set +e
  bash -lc "$refresh_command" >> "$log_file" 2>&1 &
  refresh_pid="$!"
  (
    # The Rust refresh command has its own timeout and writes the detailed
    # terminal state. Keep the shell watchdog later so Rust can report timeout
    # and release its refresh lock before the process tree is terminated.
    sleep "$((timeout_seconds + 30))"
    if kill -0 "$refresh_pid" 2>/dev/null; then
      touch "$timeout_file"
      terminate_process_tree "$refresh_pid" TERM
      sleep 2
      if kill -0 "$refresh_pid" 2>/dev/null; then
        terminate_process_tree "$refresh_pid" KILL
      fi
    fi
  ) &
  watchdog_pid="$!"
  wait "$refresh_pid"
  exit_code="$?"
  kill "$watchdog_pid" 2>/dev/null || true
  wait "$watchdog_pid" 2>/dev/null || true
  finished_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  if [[ -f "$timeout_file" ]]; then
    exit_code=124
    rm -f "$timeout_file"
    write_detail_timeout_state "$started_at" "$finished_at"
  fi
  set -e
  write_state "$started_at" "$finished_at" "$exit_code"
  return "$exit_code"
}

case "$mode" in
  once)
    run_once
    ;;
  loop)
    while true; do
      run_once || true
      sleep "$interval_seconds"
    done
    ;;
  *)
    echo "usage: $0 {once|loop}" >&2
    exit 2
    ;;
esac
