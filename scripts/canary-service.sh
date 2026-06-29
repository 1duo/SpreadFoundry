#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

pid_file="${SPREAD_CANARY_PID_FILE:-var/canary_worker.pid}"
health_output="${SPREAD_CANARY_HEALTH_OUTPUT:-var/canary_worker_health.json}"
log_file="${SPREAD_CANARY_LOG_FILE:-var/canary_worker.log}"
env_file="${SPREAD_CANARY_ENV_FILE:-var/canary_worker.env}"
spreadfoundry_bin="${SPREAD_BINARY:-target/release/spreadfoundry}"
launch_label="com.spreadfoundry.canary-worker"
launch_domain="gui/$(id -u)"
launch_script="var/canary_worker_launch.sh"
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

canary_env_names() {
  printf '%s\n' \
    SPREAD_BINARY \
    SPREAD_CANARY_CANDIDATE \
    SPREAD_CANARY_ORDER_LEDGER \
    SPREAD_CANARY_MAX_ORDER_AGE_SECONDS \
    SPREAD_CANARY_POLL_SECONDS \
    SPREAD_CANARY_MODE \
    SPREAD_CANARY_ACCOUNT_CASH \
    SPREAD_CANARY_DEBIT_MAX_LOSS \
    SPREAD_CANARY_WHEEL_RESERVE_CAP \
    SPREAD_CANARY_FREE_CASH_BUFFER \
    SPREAD_CANARY_MAX_WHEEL_POSITIONS_PER_SYMBOL \
    SPREAD_CANARY_BROKER_MULTI_LEG_OPTIONS \
    SPREAD_CANARY_BROKER_CASH_SECURED_PUTS \
    SPREAD_CANARY_BROKER_COVERED_CALLS \
    SPREAD_ROBINHOOD_MCP_COMMAND
}

load_saved_canary_env() {
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

persist_canary_env() {
  mkdir -p "$(dirname "$env_file")"
  local tmp_file="${env_file}.tmp"
  : > "$tmp_file"
  local env_name
  while IFS= read -r env_name; do
    if [[ -n "${!env_name+x}" ]]; then
      printf 'export %s=%q\n' "$env_name" "${!env_name}" >> "$tmp_file"
    fi
  done < <(canary_env_names)
  mv "$tmp_file" "$env_file"
}

set_worker_mode() {
  local mode="${1:-}"
  case "$mode" in
    monitor|review|live)
      ;;
    *)
      echo "usage: $0 set-mode {monitor|review|live}" >&2
      exit 2
      ;;
  esac
  load_saved_canary_env
  export SPREAD_CANARY_MODE="$mode"
  persist_canary_env
  stop_worker
  start_worker
  echo "canary worker mode=$mode"
}

start_worker() {
  mkdir -p "$(dirname "$pid_file")" "$(dirname "$health_output")" "$(dirname "$log_file")"
  load_saved_canary_env
  spreadfoundry_bin="${SPREAD_BINARY:-$spreadfoundry_bin}"
  persist_canary_env
  local pid
  pid="$(read_pid || true)"
  if is_running "$pid"; then
    echo "canary worker already running pid=$pid"
    return 0
  fi
  rm -f "$pid_file"
  if command -v launchctl >/dev/null 2>&1; then
    write_launch_files
    launchctl bootout "$launch_domain/$launch_label" >/dev/null 2>&1 || true
    launchctl bootstrap "$launch_domain" "$repo_root/$launch_plist"
    sleep 1
    pid="$(read_pid || true)"
    echo "started canary worker launchd=$launch_label pid=${pid:-unknown} health=$health_output log=$log_file"
    return 0
  fi
  SPREAD_CANARY_ONCE=0 SPREAD_CANARY_HEALTH_OUTPUT="$health_output" \
    nohup "$repo_root/scripts/run_canary_worker.sh" >> "$log_file" 2>&1 &
  pid="$!"
  echo "$pid" > "$pid_file"
  echo "started canary worker pid=$pid health=$health_output log=$log_file"
}

stop_worker() {
  if command -v launchctl >/dev/null 2>&1; then
    launchctl bootout "$launch_domain/$launch_label" >/dev/null 2>&1 || true
  fi
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

write_launch_files() {
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
  done < <(canary_env_names)
  cat >> "$launch_script" <<EOF
export SPREAD_BINARY="$spreadfoundry_bin"
export SPREAD_CANARY_ONCE=0
export SPREAD_CANARY_HEALTH_OUTPUT="$health_output"
exec "$repo_root/scripts/run_canary_worker.sh"
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

readiness_report() {
  load_saved_canary_env
  spreadfoundry_bin="${SPREAD_BINARY:-$spreadfoundry_bin}"
  local candidate="${SPREAD_CANARY_CANDIDATE:-candidates/weekly_selector_canary.json}"
  local account_cash="${SPREAD_CANARY_ACCOUNT_CASH:-45000}"
  local debit_max_loss="${SPREAD_CANARY_DEBIT_MAX_LOSS:-1000}"
  local wheel_reserve_cap="${SPREAD_CANARY_WHEEL_RESERVE_CAP:-35000}"
  local free_cash_buffer="${SPREAD_CANARY_FREE_CASH_BUFFER:-11250}"
  local max_wheel_positions_per_symbol="${SPREAD_CANARY_MAX_WHEEL_POSITIONS_PER_SYMBOL:-1}"
  local max_order_age_seconds="${SPREAD_CANARY_MAX_ORDER_AGE_SECONDS:-1800}"
  local cli_args=(
    canary-live-readiness
    --candidate "$candidate"
    --account-cash "$account_cash"
    --debit-max-loss "$debit_max_loss"
    --wheel-reserve-cap "$wheel_reserve_cap"
    --free-cash-buffer "$free_cash_buffer"
    --max-wheel-positions-per-symbol "$max_wheel_positions_per_symbol"
    --max-order-age-seconds "$max_order_age_seconds"
    --json
  )

  if [[ "${SPREAD_CANARY_BROKER_MULTI_LEG_OPTIONS:-0}" == "1" ]]; then
    cli_args+=(--broker-multi-leg-options)
  fi
  if [[ "${SPREAD_CANARY_BROKER_CASH_SECURED_PUTS:-0}" == "1" ]]; then
    cli_args+=(--broker-cash-secured-puts)
  fi
  if [[ "${SPREAD_CANARY_BROKER_COVERED_CALLS:-0}" == "1" ]]; then
    cli_args+=(--broker-covered-calls)
  fi
  if [[ -n "${SPREAD_ROBINHOOD_MCP_COMMAND:-}" ]]; then
    cli_args+=(--robinhood-mcp-command "$SPREAD_ROBINHOOD_MCP_COMMAND")
  fi

  if [[ -x "$spreadfoundry_bin" ]]; then
    "$spreadfoundry_bin" "${cli_args[@]}"
  else
    cargo run --quiet --release -- "${cli_args[@]}"
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
  set-mode)
    set_worker_mode "${2:-}"
    ;;
  status|snapshot)
    snapshot_worker
    ;;
  readiness)
    readiness_report
    ;;
  log)
    mkdir -p "$(dirname "$log_file")"
    touch "$log_file"
    tail -n "${SPREAD_CANARY_LOG_LINES:-80}" "$log_file"
    ;;
  *)
    echo "usage: $0 {start|stop|restart|set-mode|status|snapshot|readiness|log}" >&2
    exit 2
    ;;
esac
