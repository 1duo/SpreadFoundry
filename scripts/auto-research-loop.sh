#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

research_command="${SPREAD_AUTO_RESEARCH_COMMAND:-}"
interval_seconds="${SPREAD_AUTO_RESEARCH_INTERVAL_SECONDS:-21600}"
state_file="${SPREAD_AUTO_RESEARCH_STATE_FILE:-var/auto_research_last.json}"
log_file="${SPREAD_AUTO_RESEARCH_LOG_FILE:-var/auto_research.log}"
mode="${1:-loop}"

json_escape() {
  sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'
}

write_state() {
  local started_at="$1"
  local finished_at="$2"
  local exit_code="$3"
  local escaped_command
  escaped_command="$(printf '%s' "$research_command" | json_escape)"
  mkdir -p "$(dirname "$state_file")"
  printf '{\n  "started_at": "%s",\n  "finished_at": "%s",\n  "exit_code": %s,\n  "command": "%s"\n}\n' \
    "$started_at" "$finished_at" "$exit_code" "$escaped_command" > "$state_file"
}

run_once() {
  if [[ -z "$research_command" ]]; then
    echo "SPREAD_AUTO_RESEARCH_COMMAND is required" >&2
    return 2
  fi
  mkdir -p "$(dirname "$log_file")"
  local started_at finished_at exit_code
  started_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  set +e
  bash -lc "$research_command" >> "$log_file" 2>&1
  exit_code="$?"
  set -e
  finished_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  write_state "$started_at" "$finished_at" "$exit_code"
  return "$exit_code"
}

case "$mode" in
  once)
    run_once
    ;;
  loop)
    if [[ -z "$research_command" ]]; then
      echo "SPREAD_AUTO_RESEARCH_COMMAND is required" >&2
      exit 2
    fi
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
