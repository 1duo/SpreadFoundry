#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

pid_file="${SPREAD_EXECUTION_PID_FILE:-var/execution_worker.pid}"
health_output="${SPREAD_EXECUTION_HEALTH_OUTPUT:-var/execution_worker_health.json}"
log_file="${SPREAD_EXECUTION_LOG_FILE:-var/execution_worker.log}"
env_file="${SPREAD_EXECUTION_ENV_FILE:-var/execution_worker.env}"
spreadfoundry_bin="${SPREAD_BINARY:-target/release/spreadfoundry}"
launch_label="com.spreadfoundry.execution-worker"
launch_domain="gui/$(id -u)"
launch_script="var/execution_worker_launch.sh"
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

execution_env_names() {
  printf '%s\n' \
    SPREAD_BINARY \
    SPREAD_LIVE_SIGNAL_ARTIFACT \
    SPREAD_EXECUTION_ORDER_LEDGER \
    SPREAD_EXECUTION_NOTIFY_COMMAND \
    SPREAD_EXECUTION_NOTIFY_LEDGER \
    SPREAD_EXECUTION_MAX_ORDER_AGE_SECONDS \
    SPREAD_EXECUTION_POLL_SECONDS \
    SPREAD_EXECUTION_MODE \
    SPREAD_EXECUTION_BROKER \
    SPREAD_EXECUTION_ACCOUNT_CASH \
    SPREAD_CANARY_RISK_DEBIT_MAX_LOSS \
    SPREAD_CANARY_RISK_WHEEL_RESERVE_CAP \
    SPREAD_CANARY_RISK_FREE_CASH_BUFFER \
    SPREAD_CANARY_RISK_MAX_WHEEL_POSITIONS_PER_SYMBOL \
    SPREAD_EXECUTION_BROKER_MULTI_LEG_OPTIONS \
    SPREAD_EXECUTION_BROKER_CASH_SECURED_PUTS \
    SPREAD_EXECUTION_BROKER_COVERED_CALLS \
    SPREAD_NTFY_URL \
    SPREAD_NTFY_TOPIC \
    SPREAD_NTFY_TOKEN \
    SPREAD_NTFY_PRIORITY \
    SPREAD_NTFY_TIMEOUT_SECONDS \
    SPREAD_ROBINHOOD_MCP_COMMAND \
    SPREAD_TRADIER_ACCOUNT_ID \
    SPREAD_TRADIER_TOKEN \
    SPREAD_TRADIER_BASE_URL
}

load_saved_execution_env() {
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

persist_execution_env() {
  mkdir -p "$(dirname "$env_file")"
  local tmp_file="${env_file}.tmp"
  (umask 077 && : > "$tmp_file")
  local env_name
  while IFS= read -r env_name; do
    if [[ -n "${!env_name+x}" ]]; then
      printf 'export %s=%q\n' "$env_name" "${!env_name}" >> "$tmp_file"
    fi
  done < <(execution_env_names)
  chmod 600 "$tmp_file"
  mv "$tmp_file" "$env_file"
  chmod 600 "$env_file"
}

set_execution_mode() {
  local mode="${1:-}"
  case "$mode" in
    monitor|review|live)
      ;;
    *)
      echo "usage: $0 set-mode {monitor|review|live}" >&2
      exit 2
      ;;
  esac
  load_saved_execution_env
  export SPREAD_EXECUTION_MODE="$mode"
  persist_execution_env
  stop_worker
  start_worker
  echo "execution mode=$mode"
}

configure_ntfy() {
  local topic="${1:-${SPREAD_NTFY_TOPIC:-}}"
  if [[ -z "$topic" ]]; then
    echo "usage: $0 configure-ntfy <topic>" >&2
    exit 2
  fi
  load_saved_execution_env
  export SPREAD_EXECUTION_NOTIFY_COMMAND="${SPREAD_EXECUTION_NOTIFY_COMMAND:-$repo_root/scripts/notify-ntfy.sh}"
  export SPREAD_EXECUTION_NOTIFY_LEDGER="${SPREAD_EXECUTION_NOTIFY_LEDGER:-var/execution_notify_ledger.json}"
  export SPREAD_NTFY_URL="${SPREAD_NTFY_URL:-https://ntfy.sh}"
  export SPREAD_NTFY_TOPIC="$topic"
  export SPREAD_NTFY_PRIORITY="${SPREAD_NTFY_PRIORITY:-high}"
  export SPREAD_NTFY_TIMEOUT_SECONDS="${SPREAD_NTFY_TIMEOUT_SECONDS:-10}"
  persist_execution_env
  echo "configured ntfy topic=$SPREAD_NTFY_TOPIC command=$SPREAD_EXECUTION_NOTIFY_COMMAND"
}

configure_tradier() {
  local environment="${1:-}"
  local base_url
  case "$environment" in
    sandbox)
      base_url="https://sandbox.tradier.com/v1"
      ;;
    production)
      base_url="https://api.tradier.com/v1"
      ;;
    *)
      echo "usage: $0 configure-tradier {sandbox|production}" >&2
      exit 2
      ;;
  esac
  load_saved_execution_env
  if [[ -z "${SPREAD_TRADIER_ACCOUNT_ID:-}" || -z "${SPREAD_TRADIER_TOKEN:-}" ]]; then
    echo "SPREAD_TRADIER_ACCOUNT_ID and SPREAD_TRADIER_TOKEN must be exported before configure-tradier" >&2
    exit 2
  fi
  export SPREAD_EXECUTION_BROKER=tradier
  export SPREAD_TRADIER_BASE_URL="$base_url"
  persist_execution_env
  echo "configured tradier environment=$environment broker=$SPREAD_EXECUTION_BROKER base_url=$SPREAD_TRADIER_BASE_URL account_id=$SPREAD_TRADIER_ACCOUNT_ID"
}

start_worker() {
  mkdir -p "$(dirname "$pid_file")" "$(dirname "$health_output")" "$(dirname "$log_file")"
  load_saved_execution_env
  spreadfoundry_bin="${SPREAD_BINARY:-$spreadfoundry_bin}"
  persist_execution_env
  local pid
  pid="$(read_pid || true)"
  if is_running "$pid"; then
    echo "execution worker already running pid=$pid"
    return 0
  fi
  rm -f "$pid_file"
  if command -v launchctl >/dev/null 2>&1; then
    write_launch_files
    launchctl bootout "$launch_domain/$launch_label" >/dev/null 2>&1 || true
    launchctl bootstrap "$launch_domain" "$repo_root/$launch_plist"
    sleep 1
    pid="$(read_pid || true)"
    echo "started execution worker launchd=$launch_label pid=${pid:-unknown} health=$health_output log=$log_file"
    return 0
  fi
  SPREAD_EXECUTION_ONCE=0 SPREAD_EXECUTION_HEALTH_OUTPUT="$health_output" \
    nohup "$spreadfoundry_bin" execution-worker-env >> "$log_file" 2>&1 &
  pid="$!"
  echo "$pid" > "$pid_file"
  echo "started execution worker pid=$pid health=$health_output log=$log_file"
}

stop_worker() {
  if command -v launchctl >/dev/null 2>&1; then
    launchctl bootout "$launch_domain/$launch_label" >/dev/null 2>&1 || true
  fi
  local pid
  pid="$(read_pid || true)"
  if ! is_running "$pid"; then
    rm -f "$pid_file"
    echo "execution worker stopped"
    return 0
  fi
  kill "$pid" 2>/dev/null || true
  for _ in {1..20}; do
    if ! is_running "$pid"; then
      rm -f "$pid_file"
      echo "stopped execution worker pid=$pid"
      return 0
    fi
    sleep 0.5
  done
  kill -9 "$pid" 2>/dev/null || true
  rm -f "$pid_file"
  echo "force-stopped execution worker pid=$pid"
}

write_launch_files() {
  (umask 077 && cat > "$launch_script" <<EOF
#!/usr/bin/env bash
set -euo pipefail
cd "$repo_root"
echo "\$\$" > "$pid_file"
if [[ -f "$env_file" ]]; then
  # shellcheck disable=SC1090
  source "$env_file"
fi
EOF
  cat >> "$launch_script" <<EOF
export SPREAD_BINARY="$spreadfoundry_bin"
export SPREAD_EXECUTION_ONCE=0
export SPREAD_EXECUTION_HEALTH_OUTPUT="$health_output"
exec "$spreadfoundry_bin" execution-worker-env
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

snapshot_worker() {
  load_saved_execution_env
  spreadfoundry_bin="${SPREAD_BINARY:-$spreadfoundry_bin}"
  if [[ -x "$spreadfoundry_bin" ]]; then
    "$spreadfoundry_bin" execution-worker-snapshot \
      --health-output "$health_output" \
      --pid-file "$pid_file" \
      --json
  else
    cargo run --quiet --release -- execution-worker-snapshot \
      --health-output "$health_output" \
      --pid-file "$pid_file" \
      --json
  fi
}

readiness_report() {
  load_saved_execution_env
  spreadfoundry_bin="${SPREAD_BINARY:-$spreadfoundry_bin}"
  local live_signal="${SPREAD_LIVE_SIGNAL_ARTIFACT:-var/live_signal.json}"
  local account_cash="${SPREAD_EXECUTION_ACCOUNT_CASH:-45000}"
  local debit_max_loss="${SPREAD_CANARY_RISK_DEBIT_MAX_LOSS:-1000}"
  local wheel_reserve_cap="${SPREAD_CANARY_RISK_WHEEL_RESERVE_CAP:-35000}"
  local free_cash_buffer="${SPREAD_CANARY_RISK_FREE_CASH_BUFFER:-11250}"
  local max_wheel_positions_per_symbol="${SPREAD_CANARY_RISK_MAX_WHEEL_POSITIONS_PER_SYMBOL:-1}"
  local max_order_age_seconds="${SPREAD_EXECUTION_MAX_ORDER_AGE_SECONDS:-1800}"
  local broker="${SPREAD_EXECUTION_BROKER:-tradier}"
  local cli_args=(
    execution-readiness
    --live-signal "$live_signal"
    --account-cash "$account_cash"
    --debit-max-loss "$debit_max_loss"
    --wheel-reserve-cap "$wheel_reserve_cap"
    --free-cash-buffer "$free_cash_buffer"
    --max-wheel-positions-per-symbol "$max_wheel_positions_per_symbol"
    --broker "$broker"
    --max-order-age-seconds "$max_order_age_seconds"
    --json
  )

  if [[ "${SPREAD_EXECUTION_BROKER_MULTI_LEG_OPTIONS:-0}" == "1" ]]; then
    cli_args+=(--broker-multi-leg-options)
  fi
  if [[ "${SPREAD_EXECUTION_BROKER_CASH_SECURED_PUTS:-0}" == "1" ]]; then
    cli_args+=(--broker-cash-secured-puts)
  fi
  if [[ "${SPREAD_EXECUTION_BROKER_COVERED_CALLS:-0}" == "1" ]]; then
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

stop_legacy_services() {
  local legacy_domain="gui/$(id -u)"
  if command -v launchctl >/dev/null 2>&1; then
    launchctl bootout "$legacy_domain/com.spreadfoundry.canary-worker" >/dev/null 2>&1 || true
    launchctl bootout "$legacy_domain/com.spreadfoundry.auto-research" >/dev/null 2>&1 || true
  fi
  for legacy_pid_file in var/canary_worker.pid var/auto_research.pid; do
    if [[ -f "$legacy_pid_file" ]]; then
      local pid
      pid="$(tr -d '[:space:]' < "$legacy_pid_file")"
      if is_running "$pid"; then
        kill "$pid" 2>/dev/null || true
      fi
      rm -f "$legacy_pid_file"
    fi
  done
}

import_legacy_execution_env() {
  local legacy_env="var/canary_worker.env"
  [[ -f "$legacy_env" ]] || return 0
  # shellcheck disable=SC1090
  source "$legacy_env"
  export SPREAD_LIVE_SIGNAL_ARTIFACT="${SPREAD_LIVE_SIGNAL_ARTIFACT:-${SPREAD_CANARY_CANDIDATE:-var/live_signal.json}}" # legacy migration
  export SPREAD_EXECUTION_ORDER_LEDGER="${SPREAD_EXECUTION_ORDER_LEDGER:-${SPREAD_CANARY_ORDER_LEDGER:-var/execution_order_ledger.json}}"
  export SPREAD_EXECUTION_NOTIFY_COMMAND="${SPREAD_EXECUTION_NOTIFY_COMMAND:-${SPREAD_CANARY_NOTIFY_COMMAND:-}}"
  export SPREAD_EXECUTION_NOTIFY_LEDGER="${SPREAD_EXECUTION_NOTIFY_LEDGER:-${SPREAD_CANARY_NOTIFY_LEDGER:-var/execution_notify_ledger.json}}"
  export SPREAD_EXECUTION_MAX_ORDER_AGE_SECONDS="${SPREAD_EXECUTION_MAX_ORDER_AGE_SECONDS:-${SPREAD_CANARY_MAX_ORDER_AGE_SECONDS:-1800}}"
  export SPREAD_EXECUTION_POLL_SECONDS="${SPREAD_EXECUTION_POLL_SECONDS:-${SPREAD_CANARY_POLL_SECONDS:-60}}"
  export SPREAD_EXECUTION_MODE="${SPREAD_EXECUTION_MODE:-${SPREAD_CANARY_MODE:-monitor}}"
  export SPREAD_EXECUTION_BROKER="${SPREAD_EXECUTION_BROKER:-${SPREAD_CANARY_BROKER:-tradier}}"
  export SPREAD_EXECUTION_ACCOUNT_CASH="${SPREAD_EXECUTION_ACCOUNT_CASH:-${SPREAD_CANARY_ACCOUNT_CASH:-45000}}"
  export SPREAD_CANARY_RISK_DEBIT_MAX_LOSS="${SPREAD_CANARY_RISK_DEBIT_MAX_LOSS:-${SPREAD_CANARY_DEBIT_MAX_LOSS:-1000}}"
  export SPREAD_CANARY_RISK_WHEEL_RESERVE_CAP="${SPREAD_CANARY_RISK_WHEEL_RESERVE_CAP:-${SPREAD_CANARY_WHEEL_RESERVE_CAP:-35000}}"
  export SPREAD_CANARY_RISK_FREE_CASH_BUFFER="${SPREAD_CANARY_RISK_FREE_CASH_BUFFER:-${SPREAD_CANARY_FREE_CASH_BUFFER:-11250}}"
  export SPREAD_CANARY_RISK_MAX_WHEEL_POSITIONS_PER_SYMBOL="${SPREAD_CANARY_RISK_MAX_WHEEL_POSITIONS_PER_SYMBOL:-${SPREAD_CANARY_MAX_WHEEL_POSITIONS_PER_SYMBOL:-1}}"
}

migrate_legacy() {
  stop_legacy_services
  load_saved_execution_env
  import_legacy_execution_env
  persist_execution_env
  echo "legacy services stopped; execution env persisted at $env_file"
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
    set_execution_mode "${2:-}"
    ;;
  configure-ntfy)
    configure_ntfy "${2:-}"
    ;;
  configure-tradier)
    configure_tradier "${2:-}"
    ;;
  migrate-legacy)
    migrate_legacy
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
    tail -n "${SPREAD_EXECUTION_LOG_LINES:-80}" "$log_file"
    ;;
  *)
    echo "usage: $0 {start|stop|restart|set-mode|configure-ntfy|configure-tradier|migrate-legacy|status|snapshot|readiness|log}" >&2
    exit 2
    ;;
esac
