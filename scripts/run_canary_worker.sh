#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

candidate="${SPREAD_CANARY_CANDIDATE:-candidates/weekly_selector_canary.json}"
health_output="${SPREAD_CANARY_HEALTH_OUTPUT:-target/canary_worker_health.json}"
order_ledger="${SPREAD_CANARY_ORDER_LEDGER:-var/canary_order_ledger.json}"
max_order_age_seconds="${SPREAD_CANARY_MAX_ORDER_AGE_SECONDS:-1800}"
poll_seconds="${SPREAD_CANARY_POLL_SECONDS:-60}"
account_cash="${SPREAD_CANARY_ACCOUNT_CASH:-45000}"
debit_max_loss="${SPREAD_CANARY_DEBIT_MAX_LOSS:-1000}"
wheel_reserve_cap="${SPREAD_CANARY_WHEEL_RESERVE_CAP:-35000}"
free_cash_buffer="${SPREAD_CANARY_FREE_CASH_BUFFER:-11250}"
max_wheel_positions_per_symbol="${SPREAD_CANARY_MAX_WHEEL_POSITIONS_PER_SYMBOL:-1}"
spreadfoundry_bin="${SPREAD_BINARY:-target/release/spreadfoundry}"

cli_args=(
  canary-worker
  --candidate "$candidate"
  --account-cash "$account_cash"
  --debit-max-loss "$debit_max_loss"
  --wheel-reserve-cap "$wheel_reserve_cap"
  --free-cash-buffer "$free_cash_buffer"
  --max-wheel-positions-per-symbol "$max_wheel_positions_per_symbol"
  --poll-seconds "$poll_seconds"
  --health-output "$health_output"
  --order-ledger "$order_ledger"
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
if [[ "${SPREAD_CANARY_BROKER_REVIEW_OK:-0}" == "1" ]]; then
  cli_args+=(--broker-review-ok)
fi
if [[ "${SPREAD_CANARY_LIVE_ORDERS_ENABLED:-0}" == "1" ]]; then
  cli_args+=(--live-orders-enabled)
fi
if [[ -n "${SPREAD_ROBINHOOD_MCP_COMMAND:-}" ]]; then
  cli_args+=(--robinhood-mcp-command "$SPREAD_ROBINHOOD_MCP_COMMAND")
fi
if [[ "${SPREAD_CANARY_PLACE_LIVE_ORDER:-0}" == "1" ]]; then
  cli_args+=(--place-live-order)
fi
if [[ "${SPREAD_CANARY_ONCE:-0}" == "1" ]]; then
  cli_args+=(--once)
fi

if [[ -x "$spreadfoundry_bin" ]]; then
  exec "$spreadfoundry_bin" "${cli_args[@]}"
fi

exec cargo run --quiet --release -- "${cli_args[@]}"
