#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

pid_file="${SPREAD_AUTO_RESEARCH_PID_FILE:-var/auto_research.pid}"
state_file="${SPREAD_AUTO_RESEARCH_STATE_FILE:-var/auto_research_last.json}"
log_file="${SPREAD_AUTO_RESEARCH_LOG_FILE:-var/auto_research.log}"
env_file="${SPREAD_AUTO_RESEARCH_ENV_FILE:-var/auto_research.env}"
launch_label="com.spreadfoundry.auto-research"
launch_domain="gui/$(id -u)"
launch_script="var/auto_research_launch.sh"
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

auto_research_env_names() {
  printf '%s\n' \
    SPREAD_BINARY \
    SPREAD_AUTO_RESEARCH_COMMAND \
    SPREAD_AUTO_RESEARCH_INTERVAL_SECONDS \
    SPREAD_AUTO_RESEARCH_STATE_FILE \
    SPREAD_AUTO_RESEARCH_LOG_FILE \
    SPREAD_CANARY_REFRESH_STATE_FILE \
    SPREAD_CANARY_REFRESH_MARKET_WINDOW_ONLY \
    SPREAD_CANARY_REFRESH_TIMEOUT_SECONDS \
    SPREAD_CANARY_REFRESH_START_MINUTE_ET \
    SPREAD_CANARY_REFRESH_END_MINUTE_ET \
    SPREAD_CANARY_CANDIDATE \
    SPREAD_CANARY_CANDIDATE_ID \
    SPREAD_CANARY_EXPORT_RISK_CONTROLLED_LIVE \
    SPREAD_SELECTOR_FROM \
    SPREAD_SELECTOR_SYMBOLS \
    SPREAD_SELECTOR_FETCH_CONCURRENCY \
    SPREAD_SELECTOR_SYMBOL_CONCURRENCY \
    SPREAD_SELECTOR_CAPITAL_BUDGET \
    SPREAD_SELECTOR_MAX_SYMBOL_ALLOCATION_PCT \
    SPREAD_SELECTOR_MAX_OPEN_POSITIONS \
    SPREAD_SELECTOR_MAX_POSITIONS_PER_SYMBOL \
    SPREAD_SELECTOR_MAX_EXPIRATIONS \
    SPREAD_SELECTOR_CACHE_ONLY \
    SPREAD_SELECTOR_FORCE_REFRESH \
    SPREAD_SELECTOR_MAX_TOTAL_TRADES_PER_SYMBOL \
    SPREAD_SELECTOR_PORTFOLIO_DRAWDOWN_COOLDOWN_TRIGGER_PCT \
    SPREAD_SELECTOR_PORTFOLIO_DRAWDOWN_COOLDOWN_DAYS \
    SPREAD_SELECTOR_SYMBOL_DRAWDOWN_COOLDOWN_TRIGGER_PCT \
    SPREAD_SELECTOR_SYMBOL_DRAWDOWN_COOLDOWN_DAYS
}

load_saved_auto_research_env() {
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

persist_auto_research_env() {
  mkdir -p "$(dirname "$env_file")"
  local tmp_file="${env_file}.tmp"
  : > "$tmp_file"
  local env_name
  while IFS= read -r env_name; do
    if [[ -n "${!env_name+x}" ]]; then
      printf 'export %s=%q\n' "$env_name" "${!env_name}" >> "$tmp_file"
    fi
  done < <(auto_research_env_names)
  mv "$tmp_file" "$env_file"
}

configure_canary_refresh() {
  load_saved_auto_research_env
  export SPREAD_AUTO_RESEARCH_COMMAND="${SPREAD_AUTO_RESEARCH_COMMAND:-$repo_root/scripts/refresh-canary-artifact.sh}"
  export SPREAD_AUTO_RESEARCH_INTERVAL_SECONDS="${SPREAD_AUTO_RESEARCH_INTERVAL_SECONDS:-300}"
  export SPREAD_CANARY_REFRESH_MARKET_WINDOW_ONLY="${SPREAD_CANARY_REFRESH_MARKET_WINDOW_ONLY:-1}"
  export SPREAD_CANARY_REFRESH_TIMEOUT_SECONDS="${SPREAD_CANARY_REFRESH_TIMEOUT_SECONDS:-900}"
  export SPREAD_SELECTOR_FROM="${SPREAD_SELECTOR_FROM:-2016-01-01}"
  export SPREAD_SELECTOR_SYMBOLS="${SPREAD_SELECTOR_SYMBOLS:-IREN,PLTR,ORCL,TSLA,CRWV,AMD}"
  export SPREAD_SELECTOR_FETCH_CONCURRENCY="${SPREAD_SELECTOR_FETCH_CONCURRENCY:-4}"
  export SPREAD_SELECTOR_SYMBOL_CONCURRENCY="${SPREAD_SELECTOR_SYMBOL_CONCURRENCY:-2}"
  export SPREAD_SELECTOR_MAX_SYMBOL_ALLOCATION_PCT="${SPREAD_SELECTOR_MAX_SYMBOL_ALLOCATION_PCT:-0.20}"
  export SPREAD_SELECTOR_MAX_OPEN_POSITIONS="${SPREAD_SELECTOR_MAX_OPEN_POSITIONS:-4}"
  export SPREAD_SELECTOR_MAX_POSITIONS_PER_SYMBOL="${SPREAD_SELECTOR_MAX_POSITIONS_PER_SYMBOL:-2}"
  persist_auto_research_env
  echo "configured auto research command=$SPREAD_AUTO_RESEARCH_COMMAND interval=${SPREAD_AUTO_RESEARCH_INTERVAL_SECONDS}s"
}

start_research() {
  load_saved_auto_research_env
  if [[ -z "${SPREAD_AUTO_RESEARCH_COMMAND:-}" ]]; then
    echo "SPREAD_AUTO_RESEARCH_COMMAND is required" >&2
    exit 2
  fi
  persist_auto_research_env
  mkdir -p "$(dirname "$pid_file")" "$(dirname "$state_file")" "$(dirname "$log_file")"
  local pid
  pid="$(read_pid || true)"
  if is_running "$pid"; then
    echo "auto research already running pid=$pid"
    return 0
  fi
  rm -f "$pid_file"
  if command -v launchctl >/dev/null 2>&1; then
    write_launch_files
    launchctl bootout "$launch_domain/$launch_label" >/dev/null 2>&1 || true
    launchctl bootstrap "$launch_domain" "$repo_root/$launch_plist"
    sleep 1
    pid="$(read_pid || true)"
    echo "started auto research launchd=$launch_label pid=${pid:-unknown} state=$state_file log=$log_file"
    return 0
  fi
  nohup "$repo_root/scripts/auto-research-loop.sh" loop >> "$log_file" 2>&1 &
  pid="$!"
  echo "$pid" > "$pid_file"
  echo "started auto research pid=$pid state=$state_file log=$log_file"
}

stop_research() {
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
    echo "auto research stopped"
    return 0
  fi
  terminate_process_tree "$pid" TERM
  for _ in {1..20}; do
    if ! is_running "$pid"; then
      rm -f "$pid_file"
      echo "stopped auto research pid=$pid"
      return 0
    fi
    sleep 0.5
  done
  terminate_process_tree "$pid" KILL
  rm -f "$pid_file"
  echo "force-stopped auto research pid=$pid"
}

write_launch_files() {
  mkdir -p "$(dirname "$pid_file")" "$(dirname "$state_file")" "$(dirname "$log_file")"
  cat > "$launch_script" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cd "$repo_root"
echo "\$\$" > "$pid_file"
EOF
  while IFS= read -r env_name; do
    if [[ -n "${!env_name+x}" ]]; then
      printf 'export %s=%q\n' "$env_name" "${!env_name}" >> "$launch_script"
    fi
  done < <(auto_research_env_names)
  cat >> "$launch_script" <<EOF
exec "$repo_root/scripts/auto-research-loop.sh" loop
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

status_research() {
  load_saved_auto_research_env
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
  if [[ -n "${SPREAD_AUTO_RESEARCH_COMMAND:-}" ]]; then
    echo "command=$SPREAD_AUTO_RESEARCH_COMMAND"
    echo "interval_seconds=${SPREAD_AUTO_RESEARCH_INTERVAL_SECONDS:-21600}"
  fi
  local refresh_state_file="${SPREAD_CANARY_REFRESH_STATE_FILE:-var/canary_refresh_last.json}"
  if [[ -f "$refresh_state_file" ]]; then
    echo "refresh_state=$refresh_state_file"
    cat "$refresh_state_file"
  else
    echo "no refresh state file at $refresh_state_file"
  fi
}

case "${1:-status}" in
  configure-canary)
    configure_canary_refresh
    ;;
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
    load_saved_auto_research_env
    "$repo_root/scripts/auto-research-loop.sh" once
    ;;
  log)
    mkdir -p "$(dirname "$log_file")"
    touch "$log_file"
    tail -n "${SPREAD_AUTO_RESEARCH_LOG_LINES:-80}" "$log_file"
    ;;
  *)
    echo "usage: $0 {configure-canary|start|stop|restart|status|once|log}" >&2
    exit 2
    ;;
esac
