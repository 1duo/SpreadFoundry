#!/usr/bin/env bash
set -euo pipefail

ntfy_url="${SPREAD_NTFY_URL:-https://ntfy.sh}"
topic="${SPREAD_NTFY_TOPIC:-}"
token="${SPREAD_NTFY_TOKEN:-}"
priority="${SPREAD_NTFY_PRIORITY:-high}"

if [[ -z "$topic" ]]; then
  echo "SPREAD_NTFY_TOPIC is required" >&2
  exit 2
fi

payload_file="$(mktemp "${TMPDIR:-/tmp}/spreadfoundry-ntfy-payload.XXXXXX")"
title_file="$(mktemp "${TMPDIR:-/tmp}/spreadfoundry-ntfy-title.XXXXXX")"
body_file="$(mktemp "${TMPDIR:-/tmp}/spreadfoundry-ntfy-body.XXXXXX")"
trap 'rm -f "$payload_file" "$title_file" "$body_file"' EXIT

cat > "$payload_file"

python3 - "$payload_file" "$title_file" "$body_file" <<'PY'
import json
import sys

payload_path, title_path, body_path = sys.argv[1:4]
with open(payload_path, "r", encoding="utf-8") as handle:
    payload = json.load(handle)

action = payload.get("action") or {}
symbol = action.get("symbol") or "unknown"
strategy = action.get("strategy") or "unknown"
status = payload.get("status") or "unknown"
mode = payload.get("mode") or "unknown"

title = f"SpreadFoundry {symbol} {strategy} {status}"
lines = [
    f"{symbol} {strategy}",
    f"status={status} mode={mode}",
]
for key in ("expiration", "entry_date", "max_loss", "reserve", "short_strike", "long_strike"):
    value = action.get(key)
    if value is not None:
        if isinstance(value, float):
            value = f"{value:.2f}"
        lines.append(f"{key}={value}")
reason = payload.get("reason")
if reason:
    lines.append(f"reason={reason}")

with open(title_path, "w", encoding="utf-8") as handle:
    handle.write(title[:120])
with open(body_path, "w", encoding="utf-8") as handle:
    handle.write("\n".join(lines))
PY

curl_args=(
  -fsS
  -m "${SPREAD_NTFY_TIMEOUT_SECONDS:-10}"
  -H "Title: $(cat "$title_file")"
  -H "Priority: $priority"
  --data-binary "@$body_file"
  "${ntfy_url%/}/$topic"
)

if [[ -n "$token" ]]; then
  curl_args=(-H "Authorization: Bearer $token" "${curl_args[@]}")
fi

curl "${curl_args[@]}" >/dev/null
