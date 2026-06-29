#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

forbidden='auto_research|auto-research|canary artifact|candidate artifact|entry_candidate|open_candidate|monitor_artifact_blocked|export-portfolio-canary|portfolio-canary-status|canary-service\.sh|run_canary_worker\.sh|refresh-canary-artifact\.sh|SPREAD_AUTO_RESEARCH|SPREAD_CANARY_REFRESH|SPREAD_CANARY_CANDIDATE|candidate_readable|artifact_parse_ok'
failed=0

check_files() {
  local label="$1"
  shift
  local matches
  matches="$(rg -n -i "$forbidden" "$@" 2>/dev/null | rg -v 'legacy|Historical research note only' || true)"
  if [[ -n "$matches" ]]; then
    printf 'terminology check failed in %s:\n%s\n' "$label" "$matches" >&2
    failed=1
  fi
}

check_stream() {
  local label="$1"
  local matches
  matches="$(rg -n -i "$forbidden" - 2>/dev/null | rg -v 'legacy|Historical research note only' || true)"
  if [[ -n "$matches" ]]; then
    printf 'terminology check failed in %s:\n%s\n' "$label" "$matches" >&2
    failed=1
  fi
}

check_files "current docs" \
  docs/production_architecture.md \
  docs/tradier_live.md \
  docs/robinhood_mcp_live.md

script_files=()
for script in scripts/*.sh; do
  if [[ "$script" != "scripts/check-terminology.sh" ]]; then
    script_files+=("$script")
  fi
done
check_files "scripts" "${script_files[@]}"
check_files "menubar" apps/SpreadFoundryMenubar/Sources/SpreadFoundryMenubar
check_files "live signal module" src/live_signal.rs configs/approved_strategy.json

awk '/#\[cfg\(test\)\]/ {exit} {print}' src/main.rs | check_stream "src/main.rs non-test live path"

if [[ "$failed" -ne 0 ]]; then
  exit 1
fi

echo "terminology check passed"
