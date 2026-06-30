#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

pid_file="${SPREAD_SIGNAL_REFRESH_PID_FILE:-var/signal_refresh.pid}"
state_file="${SPREAD_SIGNAL_REFRESH_STATE_FILE:-var/signal_refresh_last.json}"
log_file="${SPREAD_SIGNAL_REFRESH_LOG_FILE:-var/signal_refresh.log}"
env_file="${SPREAD_SIGNAL_REFRESH_ENV_FILE:-var/signal_refresh.env}"
launch_label="com.spreadfoundry.signal-refresh"
launch_domain="gui/$(id -u)"
launch_script="var/signal_refresh_launch.sh"
launch_plist="var/${launch_label}.plist"

is_running() {
  local pid="$1"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
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

read_pid() {
  if [[ -f "$pid_file" ]]; then
    tr -d '[:space:]' < "$pid_file"
  fi
}

signal_refresh_env_names() {
	printf '%s\n' \
	    SPREAD_BINARY \
	    SPREAD_SIGNAL_REFRESH_INTERVAL_SECONDS \
	    SPREAD_SIGNAL_REFRESH_STATE_FILE \
	    SPREAD_SIGNAL_REFRESH_LOG_FILE \
	    SPREAD_SIGNAL_REFRESH_MARKET_WINDOW_ONLY \
	    SPREAD_SIGNAL_REFRESH_TIMEOUT_SECONDS \
	    SPREAD_SIGNAL_REFRESH_TO \
	    SPREAD_APPROVED_STRATEGY \
	    SPREAD_LIVE_SIGNAL_ARTIFACT \
	    SPREAD_SELECTOR_FROM \
	    SPREAD_SELECTOR_FETCH_CONCURRENCY \
	    SPREAD_SELECTOR_SYMBOL_CONCURRENCY \
	    SPREAD_SELECTOR_MAX_EXPIRATIONS \
	    SPREAD_SELECTOR_CACHE_ONLY \
	    SPREAD_SELECTOR_FORCE_REFRESH \
	    SPREAD_TRADIER_ACCOUNT_ID \
	    SPREAD_TRADIER_TOKEN \
	    SPREAD_TRADIER_BASE_URL
}

load_saved_signal_refresh_env() {
  [[ -f "$env_file" ]] || return 0
  local line assignment name
  while IFS= read -r line; do
    [[ "$line" == export\ SPREAD_* ]] || continue
    assignment="${line#export }"
    name="${assignment%%=*}"
    if [[ "$name" =~ ^SPREAD_[A-Z0-9_]+$ && -z "${!name+x}" ]]; then
      eval "$line"
    fi
  done < "$env_file"
}

load_saved_execution_tradier_env() {
  local execution_env="${SPREAD_EXECUTION_ENV_FILE:-var/execution_worker.env}"
  [[ -f "$execution_env" ]] || return 0
  local line assignment name
  while IFS= read -r line; do
    [[ "$line" == export\ SPREAD_TRADIER_* ]] || continue
    assignment="${line#export }"
    name="${assignment%%=*}"
    if [[ "$name" =~ ^SPREAD_TRADIER_[A-Z0-9_]+$ && -z "${!name+x}" ]]; then
      eval "$line"
    fi
  done < "$execution_env"
}

persist_signal_refresh_env() {
  mkdir -p "$(dirname "$env_file")"
  local tmp_file="${env_file}.tmp"
  (umask 077 && : > "$tmp_file")
  local env_name
  while IFS= read -r env_name; do
    if [[ -n "${!env_name+x}" ]]; then
      printf 'export %s=%q\n' "$env_name" "${!env_name}" >> "$tmp_file"
    fi
  done < <(signal_refresh_env_names)
  mv "$tmp_file" "$env_file"
  chmod 600 "$env_file"
}

configure_refresh() {
  load_saved_signal_refresh_env
  load_saved_execution_tradier_env
  export SPREAD_SIGNAL_REFRESH_INTERVAL_SECONDS="${SPREAD_SIGNAL_REFRESH_INTERVAL_SECONDS:-300}"
  export SPREAD_SIGNAL_REFRESH_MARKET_WINDOW_ONLY="${SPREAD_SIGNAL_REFRESH_MARKET_WINDOW_ONLY:-1}"
  export SPREAD_SIGNAL_REFRESH_TIMEOUT_SECONDS="${SPREAD_SIGNAL_REFRESH_TIMEOUT_SECONDS:-900}"
  export SPREAD_APPROVED_STRATEGY="${SPREAD_APPROVED_STRATEGY:-configs/approved_strategy.json}"
  export SPREAD_LIVE_SIGNAL_ARTIFACT="${SPREAD_LIVE_SIGNAL_ARTIFACT:-var/live_signal.json}"
  export SPREAD_SELECTOR_FROM="${SPREAD_SELECTOR_FROM:-2016-01-01}"
  export SPREAD_SELECTOR_FETCH_CONCURRENCY="${SPREAD_SELECTOR_FETCH_CONCURRENCY:-4}"
  export SPREAD_SELECTOR_SYMBOL_CONCURRENCY="${SPREAD_SELECTOR_SYMBOL_CONCURRENCY:-2}"
  persist_signal_refresh_env
  echo "configured signal refresh interval=${SPREAD_SIGNAL_REFRESH_INTERVAL_SECONDS}s"
}

start_refresh() {
  load_saved_signal_refresh_env
  load_saved_execution_tradier_env
  if [[ ! -f "$env_file" ]]; then
    configure_refresh
    load_saved_signal_refresh_env
    load_saved_execution_tradier_env
  fi
  persist_signal_refresh_env
  mkdir -p "$(dirname "$pid_file")" "$(dirname "$state_file")" "$(dirname "$log_file")"
  local pid
  pid="$(read_pid || true)"
  if is_running "$pid"; then
    echo "signal refresh already running pid=$pid"
    return 0
  fi
  rm -f "$pid_file"
  if command -v launchctl >/dev/null 2>&1; then
    write_launch_files
    launchctl bootout "$launch_domain/$launch_label" >/dev/null 2>&1 || true
    launchctl bootstrap "$launch_domain" "$repo_root/$launch_plist"
    sleep 1
    pid="$(read_pid || true)"
    echo "started signal refresh launchd=$launch_label pid=${pid:-unknown} state=$state_file log=$log_file"
    return 0
  fi
  nohup "$repo_root/scripts/signal-refresh-loop.sh" loop >> "$log_file" 2>&1 &
  pid="$!"
  echo "$pid" > "$pid_file"
  echo "started signal refresh pid=$pid state=$state_file log=$log_file"
}

stop_refresh() {
  local pid
  pid="$(read_pid || true)"
  if is_running "$pid"; then
    terminate_process_tree "$pid" TERM
  fi
  if command -v launchctl >/dev/null 2>&1; then
    launchctl bootout "$launch_domain/$launch_label" >/dev/null 2>&1 || true
  fi
  pid="$(read_pid || true)"
  if ! is_running "$pid"; then
    rm -f "$pid_file"
    echo "signal refresh stopped"
    return 0
  fi
  terminate_process_tree "$pid" TERM
  for _ in {1..20}; do
    if ! is_running "$pid"; then
      rm -f "$pid_file"
      echo "stopped signal refresh pid=$pid"
      return 0
    fi
    sleep 0.5
  done
  terminate_process_tree "$pid" KILL
  rm -f "$pid_file"
  echo "force-stopped signal refresh pid=$pid"
}

write_launch_files() {
  mkdir -p "$(dirname "$pid_file")" "$(dirname "$state_file")" "$(dirname "$log_file")"
  cat > "$launch_script" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cd "$repo_root"
echo "\$\$" > "$pid_file"
if [[ -f "$env_file" ]]; then
  # shellcheck disable=SC1090
  source "$env_file"
fi
exec "$repo_root/scripts/signal-refresh-loop.sh" loop
EOF
  chmod +x "$launch_script"
  cat > "$launch_plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$launch_label</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>$repo_root/$launch_script</string>
  </array>
  <key>WorkingDirectory</key>
  <string>$repo_root</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>$repo_root/$log_file</string>
  <key>StandardErrorPath</key>
  <string>$repo_root/$log_file</string>
</dict>
</plist>
EOF
}

status_refresh() {
  load_saved_signal_refresh_env
  load_saved_execution_tradier_env
  local pid
  pid="$(read_pid || true)"
  if is_running "$pid"; then
    echo "signal refresh running pid=$pid"
  else
    echo "signal refresh not running"
  fi
  if [[ -f "$state_file" ]]; then
    cat "$state_file"
  else
    echo "no state file at $state_file"
  fi
  echo "interval_seconds=${SPREAD_SIGNAL_REFRESH_INTERVAL_SECONDS:-300}"
}

stop_legacy_auto_research() {
  local legacy_domain="gui/$(id -u)"
  if command -v launchctl >/dev/null 2>&1; then
    launchctl bootout "$legacy_domain/com.spreadfoundry.auto-research" >/dev/null 2>&1 || true
  fi
  if [[ -f var/auto_research.pid ]]; then # legacy migration
    local pid
    pid="$(tr -d '[:space:]' < var/auto_research.pid)" # legacy migration
    if is_running "$pid"; then
      terminate_process_tree "$pid" TERM
    fi
    rm -f var/auto_research.pid # legacy migration
  fi
}

import_legacy_refresh_env() {
  local legacy_env="var/auto_research.env"
  [[ -f "$legacy_env" ]] || return 0
  # shellcheck disable=SC1090
  source "$legacy_env"
  export SPREAD_SIGNAL_REFRESH_INTERVAL_SECONDS="${SPREAD_SIGNAL_REFRESH_INTERVAL_SECONDS:-${SPREAD_AUTO_RESEARCH_INTERVAL_SECONDS:-300}}" # legacy migration
  export SPREAD_SIGNAL_REFRESH_MARKET_WINDOW_ONLY="${SPREAD_SIGNAL_REFRESH_MARKET_WINDOW_ONLY:-${SPREAD_CANARY_REFRESH_MARKET_WINDOW_ONLY:-1}}" # legacy migration
  export SPREAD_SIGNAL_REFRESH_TIMEOUT_SECONDS="${SPREAD_SIGNAL_REFRESH_TIMEOUT_SECONDS:-${SPREAD_CANARY_REFRESH_TIMEOUT_SECONDS:-900}}" # legacy migration
  export SPREAD_LIVE_SIGNAL_ARTIFACT="${SPREAD_LIVE_SIGNAL_ARTIFACT:-${SPREAD_CANARY_CANDIDATE:-var/live_signal.json}}" # legacy migration
}

migrate_legacy() {
  stop_legacy_auto_research
  load_saved_signal_refresh_env
  load_saved_execution_tradier_env
  import_legacy_refresh_env
  persist_signal_refresh_env
  echo "legacy refresh service stopped; signal refresh env persisted at $env_file"
}

case "${1:-status}" in
  configure)
    configure_refresh
    ;;
  start)
    start_refresh
    ;;
  stop)
    stop_refresh
    ;;
  restart)
    stop_refresh
    start_refresh
    ;;
  status)
    status_refresh
    ;;
  migrate-legacy)
    migrate_legacy
    ;;
	  once)
	    load_saved_signal_refresh_env
	    "$repo_root/scripts/signal-refresh-loop.sh" once
    ;;
  log)
    mkdir -p "$(dirname "$log_file")"
    touch "$log_file"
    tail -n "${SPREAD_SIGNAL_REFRESH_LOG_LINES:-80}" "$log_file"
    ;;
  *)
    echo "usage: $0 {configure|start|stop|restart|status|migrate-legacy|once|log}" >&2
    exit 2
    ;;
esac
