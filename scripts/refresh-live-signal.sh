#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

spreadfoundry_bin="${SPREAD_BINARY:-target/release/spreadfoundry}"

args=(
  refresh-live-signal
  --approved-strategy "${SPREAD_APPROVED_STRATEGY:-configs/approved_strategy.json}"
  --output "${SPREAD_LIVE_SIGNAL_ARTIFACT:-var/live_signal.json}"
  --state-file "${SPREAD_SIGNAL_REFRESH_DETAIL_STATE_FILE:-var/live_signal_refresh_last.json}"
  --from "${SPREAD_SELECTOR_FROM:-2016-01-01}"
  --to "${SPREAD_SIGNAL_REFRESH_TO:-$(date -u '+%Y-%m-%d')}"
  --fetch-concurrency "${SPREAD_SELECTOR_FETCH_CONCURRENCY:-4}"
  --symbol-concurrency "${SPREAD_SELECTOR_SYMBOL_CONCURRENCY:-2}"
  --timeout-seconds "${SPREAD_SIGNAL_REFRESH_TIMEOUT_SECONDS:-900}"
)

if [[ "${SPREAD_SIGNAL_REFRESH_MARKET_WINDOW_ONLY:-1}" != "1" ]]; then
  args+=(--market-window-only false)
fi
if [[ -n "${SPREAD_SELECTOR_MAX_EXPIRATIONS:-}" ]]; then
  args+=(--max-expirations "$SPREAD_SELECTOR_MAX_EXPIRATIONS")
fi
if [[ "${SPREAD_SELECTOR_CACHE_ONLY:-0}" == "1" ]]; then
  args+=(--cache-only)
fi
if [[ "${SPREAD_SELECTOR_FORCE_REFRESH:-0}" == "1" ]]; then
  args+=(--force-refresh)
fi

if [[ -x "$spreadfoundry_bin" ]]; then
  exec "$spreadfoundry_bin" "${args[@]}"
fi

exec cargo run --quiet --release -- "${args[@]}"
