#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

spreadfoundry_bin="${SPREAD_BINARY:-target/release/spreadfoundry}"
state_file="${SPREAD_CANARY_REFRESH_STATE_FILE:-var/canary_refresh_last.json}"
candidate_output="${SPREAD_CANARY_CANDIDATE:-candidates/weekly_selector_canary.json}"
market_window_only="${SPREAD_CANARY_REFRESH_MARKET_WINDOW_ONLY:-1}"
refresh_timeout_seconds="${SPREAD_CANARY_REFRESH_TIMEOUT_SECONDS:-900}"
run_to="${SPREAD_CANARY_REFRESH_TO:-$(date -u '+%Y-%m-%d')}"
selector_from="${SPREAD_SELECTOR_FROM:-2016-01-01}"
selector_symbols="${SPREAD_SELECTOR_SYMBOLS:-IREN,PLTR,ORCL,TSLA,CRWV,AMD}"
candidate_id="${SPREAD_CANARY_CANDIDATE_ID:-weekly_selector_canary_${run_to//-/}}"

json_escape() {
  sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'
}

json_value() {
  printf '%s' "$1" | json_escape
}

write_state() {
  local status="$1"
  local exit_code="$2"
  local started_at="$3"
  local finished_at="$4"
  local run_dir="${5:-}"
  local reason="${6:-}"
  mkdir -p "$(dirname "$state_file")"
  cat > "$state_file" <<EOF
{
  "started_at": "$(json_value "$started_at")",
  "finished_at": "$(json_value "$finished_at")",
  "status": "$(json_value "$status")",
  "exit_code": $exit_code,
  "run_to": "$(json_value "$run_to")",
  "run_dir": "$(json_value "$run_dir")",
  "candidate_output": "$(json_value "$candidate_output")",
  "candidate_id": "$(json_value "$candidate_id")",
  "reason": "$(json_value "$reason")"
}
EOF
}

within_market_window() {
  local day hour minute current start end
  day="$(TZ=America/New_York date '+%u')"
  hour="$(TZ=America/New_York date '+%H')"
  minute="$(TZ=America/New_York date '+%M')"
  current=$((10#$hour * 60 + 10#$minute))
  start="${SPREAD_CANARY_REFRESH_START_MINUTE_ET:-565}"
  end="${SPREAD_CANARY_REFRESH_END_MINUTE_ET:-965}"
  [[ "$day" -ge 1 && "$day" -le 5 && "$current" -ge "$start" && "$current" -le "$end" ]]
}

run_spreadfoundry() {
  if [[ -x "$spreadfoundry_bin" ]]; then
    "$spreadfoundry_bin" "$@"
  else
    cargo run --quiet --release -- "$@"
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

run_research_with_timeout() {
  local output_file="$1"
  shift
  local timed_out_file="${output_file}.timed_out"
  local child_pid watchdog_pid exit_code
  rm -f "$timed_out_file"

  run_spreadfoundry "$@" > "$output_file" 2>&1 &
  child_pid="$!"
  (
    sleep "$refresh_timeout_seconds"
    if kill -0 "$child_pid" 2>/dev/null; then
      : > "$timed_out_file"
      terminate_process_tree "$child_pid" TERM
      sleep 5
      if kill -0 "$child_pid" 2>/dev/null; then
        terminate_process_tree "$child_pid" KILL
      fi
    fi
  ) &
  watchdog_pid="$!"

  wait "$child_pid" 2>/dev/null
  exit_code="$?"
  kill "$watchdog_pid" 2>/dev/null || true
  wait "$watchdog_pid" 2>/dev/null || true
  cat "$output_file"

  if [[ -f "$timed_out_file" ]]; then
    rm -f "$timed_out_file"
    return 124
  fi
  return "$exit_code"
}

started_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

if ! [[ "$refresh_timeout_seconds" =~ ^[0-9]+$ ]] || [[ "$refresh_timeout_seconds" -le 0 ]]; then
  finished_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  write_state "configuration_error" 2 "$started_at" "$finished_at" "" "SPREAD_CANARY_REFRESH_TIMEOUT_SECONDS must be a positive integer"
  echo "SPREAD_CANARY_REFRESH_TIMEOUT_SECONDS must be a positive integer" >&2
  exit 2
fi

if [[ "$market_window_only" == "1" ]] && ! within_market_window; then
  finished_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  write_state "skipped_market_closed" 0 "$started_at" "$finished_at" "" "outside configured regular-market refresh window"
  echo "canary refresh skipped: outside configured regular-market refresh window"
  exit 0
fi

write_state "running" 0 "$started_at" "" "" "portfolio selector research in progress"

research_args=(
  research-portfolio-selector
  --symbols "$selector_symbols"
  --from "$selector_from"
  --to "$run_to"
  --fetch-concurrency "${SPREAD_SELECTOR_FETCH_CONCURRENCY:-4}"
  --symbol-concurrency "${SPREAD_SELECTOR_SYMBOL_CONCURRENCY:-2}"
  --capital-budget "${SPREAD_SELECTOR_CAPITAL_BUDGET:-100000}"
  --max-symbol-allocation-pct "${SPREAD_SELECTOR_MAX_SYMBOL_ALLOCATION_PCT:-0.20}"
  --max-open-positions "${SPREAD_SELECTOR_MAX_OPEN_POSITIONS:-4}"
  --max-positions-per-symbol "${SPREAD_SELECTOR_MAX_POSITIONS_PER_SYMBOL:-2}"
)

if [[ -n "${SPREAD_SELECTOR_MAX_EXPIRATIONS:-}" ]]; then
  research_args+=(--max-expirations "$SPREAD_SELECTOR_MAX_EXPIRATIONS")
fi
if [[ "${SPREAD_SELECTOR_CACHE_ONLY:-0}" == "1" ]]; then
  research_args+=(--cache-only)
fi
if [[ "${SPREAD_SELECTOR_FORCE_REFRESH:-0}" == "1" ]]; then
  research_args+=(--force-refresh)
fi
if [[ -n "${SPREAD_SELECTOR_MAX_TOTAL_TRADES_PER_SYMBOL:-}" ]]; then
  research_args+=(--max-total-trades-per-symbol "$SPREAD_SELECTOR_MAX_TOTAL_TRADES_PER_SYMBOL")
fi
if [[ -n "${SPREAD_SELECTOR_PORTFOLIO_DRAWDOWN_COOLDOWN_TRIGGER_PCT:-}" ]]; then
  research_args+=(--portfolio-drawdown-cooldown-trigger-pct "$SPREAD_SELECTOR_PORTFOLIO_DRAWDOWN_COOLDOWN_TRIGGER_PCT")
fi
if [[ -n "${SPREAD_SELECTOR_PORTFOLIO_DRAWDOWN_COOLDOWN_DAYS:-}" ]]; then
  research_args+=(--portfolio-drawdown-cooldown-days "$SPREAD_SELECTOR_PORTFOLIO_DRAWDOWN_COOLDOWN_DAYS")
fi
if [[ -n "${SPREAD_SELECTOR_SYMBOL_DRAWDOWN_COOLDOWN_TRIGGER_PCT:-}" ]]; then
  research_args+=(--symbol-drawdown-cooldown-trigger-pct "$SPREAD_SELECTOR_SYMBOL_DRAWDOWN_COOLDOWN_TRIGGER_PCT")
fi
if [[ -n "${SPREAD_SELECTOR_SYMBOL_DRAWDOWN_COOLDOWN_DAYS:-}" ]]; then
  research_args+=(--symbol-drawdown-cooldown-days "$SPREAD_SELECTOR_SYMBOL_DRAWDOWN_COOLDOWN_DAYS")
fi

tmp_output="$(mktemp "${TMPDIR:-/tmp}/spreadfoundry-refresh.XXXXXX")"
set +e
run_research_with_timeout "$tmp_output" "${research_args[@]}"
research_code="$?"
set -e
run_dir=""
if [[ "$research_code" -eq 0 ]]; then
  run_dir="$(tail -n 1 "$tmp_output" | tr -d '[:space:]')"
fi
rm -f "$tmp_output"

if [[ "$research_code" -eq 124 ]]; then
  finished_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  write_state "research_timeout" 124 "$started_at" "$finished_at" "$run_dir" "portfolio selector research exceeded ${refresh_timeout_seconds}s timeout"
  echo "canary refresh timed out after ${refresh_timeout_seconds}s"
  exit 0
fi

if [[ "$research_code" -ne 0 ]]; then
  finished_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  write_state "research_failed" "$research_code" "$started_at" "$finished_at" "$run_dir" "portfolio selector research failed"
  exit "$research_code"
fi

set +e
export_args=(
  export-portfolio-canary
  --run "$run_dir" \
  --output "$candidate_output" \
  --candidate-id "$candidate_id" \
  --frozen-on "$run_to"
)
if [[ "${SPREAD_CANARY_EXPORT_RISK_CONTROLLED_LIVE:-0}" == "1" ]]; then
  export_args+=(--allow-risk-controlled-live)
fi
run_spreadfoundry "${export_args[@]}"
export_code="$?"
set -e
finished_at="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

if [[ "$export_code" -ne 0 ]]; then
  write_state "no_canary_ready_profile" 0 "$started_at" "$finished_at" "$run_dir" "research completed but no canary-ready profile was exportable"
  echo "canary refresh completed without export: no canary-ready profile in $run_dir"
  exit 0
fi

write_state "exported" 0 "$started_at" "$finished_at" "$run_dir" "exported fresh canary artifact"
echo "canary refresh exported $candidate_output from $run_dir"
