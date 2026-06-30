#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

pid_file="${SPREAD_LIVE_ENGINE_PID_FILE:-var/live_market_engine.pid}"
state_file="${SPREAD_LIVE_ENGINE_STATE_FILE:-var/live_market_engine_health.json}"
log_file="${SPREAD_LIVE_ENGINE_LOG_FILE:-var/live_market_engine.log}"
env_file="${SPREAD_LIVE_ENGINE_ENV_FILE:-var/live_market_engine.env}"
spreadfoundry_bin="${SPREAD_BINARY:-target/release/spreadfoundry}"
launch_label="com.spreadfoundry.live-market-engine"
launch_domain="gui/$(id -u)"
launch_script="var/live_market_engine_launch.sh"
launch_plist="var/${launch_label}.plist"

is_running() {
  local pid="$1"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

read_pid() {
  if [[ -f "$pid_file" ]]; then
    tr -d '[:space:]' < "$pid_file"
  fi
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

live_engine_env_names() {
  printf '%s\n' \
    SPREAD_BINARY \
    SPREAD_APPROVED_STRATEGY \
    SPREAD_LIVE_ENGINE_SOURCE_ARTIFACT \
    SPREAD_LIVE_SIGNAL_ARTIFACT \
    SPREAD_LIVE_ENGINE_STATE_FILE \
    SPREAD_LIVE_ENGINE_STORE \
    SPREAD_LIVE_ENGINE_INTERVAL_SECONDS \
    SPREAD_LIVE_ENGINE_MAX_SOURCE_AGE_SECONDS \
    SPREAD_LIVE_ENGINE_MARKET_WINDOW_ONLY \
    SPREAD_TRADIER_ACCOUNT_ID \
    SPREAD_TRADIER_TOKEN \
    SPREAD_TRADIER_BASE_URL
}

load_saved_live_engine_env() {
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

persist_live_engine_env() {
  mkdir -p "$(dirname "$env_file")"
  local tmp_file="${env_file}.tmp"
  (umask 077 && : > "$tmp_file")
  local env_name
  while IFS= read -r env_name; do
    if [[ -n "${!env_name+x}" ]]; then
      printf 'export %s=%q\n' "$env_name" "${!env_name}" >> "$tmp_file"
    fi
  done < <(live_engine_env_names)
  chmod 600 "$tmp_file"
  mv "$tmp_file" "$env_file"
  chmod 600 "$env_file"
}

bool_arg() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON)
      printf 'true'
      ;;
    0|false|FALSE|no|NO|off|OFF)
      printf 'false'
      ;;
    *)
      printf '%s' "$1"
      ;;
  esac
}

configure_live_engine() {
  load_saved_live_engine_env
  load_saved_execution_tradier_env
  export SPREAD_APPROVED_STRATEGY="${SPREAD_APPROVED_STRATEGY:-configs/approved_strategy.json}"
  export SPREAD_LIVE_ENGINE_SOURCE_ARTIFACT="${SPREAD_LIVE_ENGINE_SOURCE_ARTIFACT:-var/live_signal_refresh_source.json}"
  export SPREAD_LIVE_SIGNAL_ARTIFACT="${SPREAD_LIVE_SIGNAL_ARTIFACT:-var/live_signal.json}"
  export SPREAD_LIVE_ENGINE_STATE_FILE="${SPREAD_LIVE_ENGINE_STATE_FILE:-$state_file}"
  export SPREAD_LIVE_ENGINE_STORE="${SPREAD_LIVE_ENGINE_STORE:-data/spreadfoundry.duckdb}"
  export SPREAD_LIVE_ENGINE_INTERVAL_SECONDS="${SPREAD_LIVE_ENGINE_INTERVAL_SECONDS:-30}"
  export SPREAD_LIVE_ENGINE_MAX_SOURCE_AGE_SECONDS="${SPREAD_LIVE_ENGINE_MAX_SOURCE_AGE_SECONDS:-45}"
  export SPREAD_LIVE_ENGINE_MARKET_WINDOW_ONLY="${SPREAD_LIVE_ENGINE_MARKET_WINDOW_ONLY:-1}"
  persist_live_engine_env
  echo "configured live market engine interval=${SPREAD_LIVE_ENGINE_INTERVAL_SECONDS}s source=$SPREAD_LIVE_ENGINE_SOURCE_ARTIFACT output=$SPREAD_LIVE_SIGNAL_ARTIFACT"
}

live_engine_cli_args() {
  local once="${1:-0}"
  local market_window_only
  market_window_only="$(bool_arg "${SPREAD_LIVE_ENGINE_MARKET_WINDOW_ONLY:-1}")"
  cli_args=(
    live-market-engine
    --approved-strategy "${SPREAD_APPROVED_STRATEGY:-configs/approved_strategy.json}"
    --source-live-signal "${SPREAD_LIVE_ENGINE_SOURCE_ARTIFACT:-var/live_signal_refresh_source.json}"
    --output "${SPREAD_LIVE_SIGNAL_ARTIFACT:-var/live_signal.json}"
    --state-file "${SPREAD_LIVE_ENGINE_STATE_FILE:-$state_file}"
    --store "${SPREAD_LIVE_ENGINE_STORE:-data/spreadfoundry.duckdb}"
    --interval-seconds "${SPREAD_LIVE_ENGINE_INTERVAL_SECONDS:-30}"
    --max-source-age-seconds "${SPREAD_LIVE_ENGINE_MAX_SOURCE_AGE_SECONDS:-45}"
    --market-window-only "$market_window_only"
    --json
  )
  if [[ "$once" == "1" ]]; then
    cli_args+=(--once)
  fi
}

start_engine() {
  mkdir -p "$(dirname "$pid_file")" "$(dirname "$state_file")" "$(dirname "$log_file")"
  load_saved_live_engine_env
  load_saved_execution_tradier_env
  if [[ ! -f "$env_file" ]]; then
    configure_live_engine
    load_saved_live_engine_env
    load_saved_execution_tradier_env
  fi
  persist_live_engine_env
  spreadfoundry_bin="${SPREAD_BINARY:-$spreadfoundry_bin}"
  local pid
  pid="$(read_pid || true)"
  if is_running "$pid"; then
    echo "live market engine already running pid=$pid"
    return 0
  fi
  rm -f "$pid_file"
  if command -v launchctl >/dev/null 2>&1; then
    write_launch_files
    launchctl bootout "$launch_domain/$launch_label" >/dev/null 2>&1 || true
    launchctl bootstrap "$launch_domain" "$repo_root/$launch_plist"
    sleep 1
    pid="$(read_pid || true)"
    echo "started live market engine launchd=$launch_label pid=${pid:-unknown} state=$state_file log=$log_file"
    return 0
  fi
  live_engine_cli_args 0
  nohup "$spreadfoundry_bin" "${cli_args[@]}" >> "$log_file" 2>&1 &
  pid="$!"
  echo "$pid" > "$pid_file"
  echo "started live market engine pid=$pid state=$state_file log=$log_file"
}

stop_engine() {
  if command -v launchctl >/dev/null 2>&1; then
    launchctl bootout "$launch_domain/$launch_label" >/dev/null 2>&1 || true
  fi
  local pid
  pid="$(read_pid || true)"
  if ! is_running "$pid"; then
    rm -f "$pid_file"
    echo "live market engine stopped"
    return 0
  fi
  terminate_process_tree "$pid" TERM
  for _ in {1..20}; do
    if ! is_running "$pid"; then
      rm -f "$pid_file"
      echo "stopped live market engine pid=$pid"
      return 0
    fi
    sleep 0.5
  done
  terminate_process_tree "$pid" KILL
  rm -f "$pid_file"
  echo "force-stopped live market engine pid=$pid"
}

write_launch_files() {
  mkdir -p "$(dirname "$pid_file")" "$(dirname "$state_file")" "$(dirname "$log_file")"
  local market_window_only
  market_window_only="$(bool_arg "${SPREAD_LIVE_ENGINE_MARKET_WINDOW_ONLY:-1}")"
  (umask 077 && cat > "$launch_script" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cd "$repo_root"
echo "\$\$" > "$pid_file"
if [[ -f "$env_file" ]]; then
  # shellcheck disable=SC1090
  source "$env_file"
fi
exec "${SPREAD_BINARY:-$spreadfoundry_bin}" live-market-engine \\
  --approved-strategy "\${SPREAD_APPROVED_STRATEGY:-configs/approved_strategy.json}" \\
  --source-live-signal "\${SPREAD_LIVE_ENGINE_SOURCE_ARTIFACT:-var/live_signal_refresh_source.json}" \\
  --output "\${SPREAD_LIVE_SIGNAL_ARTIFACT:-var/live_signal.json}" \\
  --state-file "\${SPREAD_LIVE_ENGINE_STATE_FILE:-$state_file}" \\
  --store "\${SPREAD_LIVE_ENGINE_STORE:-data/spreadfoundry.duckdb}" \\
  --interval-seconds "\${SPREAD_LIVE_ENGINE_INTERVAL_SECONDS:-30}" \\
  --max-source-age-seconds "\${SPREAD_LIVE_ENGINE_MAX_SOURCE_AGE_SECONDS:-45}" \\
  --market-window-only "$market_window_only" \\
  --json
EOF
  )
  chmod 700 "$launch_script"
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

status_engine() {
  load_saved_live_engine_env
  local pid
  pid="$(read_pid || true)"
  if is_running "$pid"; then
    echo "live market engine running pid=$pid"
  else
    echo "live market engine not running"
  fi
  if [[ -f "$state_file" ]]; then
    cat "$state_file"
  else
    echo "no state file at $state_file"
  fi
  echo "interval_seconds=${SPREAD_LIVE_ENGINE_INTERVAL_SECONDS:-30}"
}

run_once() {
  load_saved_live_engine_env
  load_saved_execution_tradier_env
  spreadfoundry_bin="${SPREAD_BINARY:-$spreadfoundry_bin}"
  live_engine_cli_args 1
  if [[ -x "$spreadfoundry_bin" ]]; then
    "$spreadfoundry_bin" "${cli_args[@]}"
  else
    cargo run --quiet --release -- "${cli_args[@]}"
  fi
}

case "${1:-status}" in
  configure)
    configure_live_engine
    ;;
  start)
    start_engine
    ;;
  stop)
    stop_engine
    ;;
  restart)
    stop_engine
    start_engine
    ;;
  status)
    status_engine
    ;;
  once)
    run_once
    ;;
  log)
    mkdir -p "$(dirname "$log_file")"
    touch "$log_file"
    tail -n "${SPREAD_LIVE_ENGINE_LOG_LINES:-80}" "$log_file"
    ;;
  *)
    echo "usage: $0 {configure|start|stop|restart|status|once|log}" >&2
    exit 2
    ;;
esac
