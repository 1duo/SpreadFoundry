# Robinhood MCP Canary Execution

SpreadFoundry does not let strategy code place Robinhood orders directly. The
canary runner emits deterministic `review_option_order` and `place_option_order`
requests, then calls an operator-provided MCP bridge command.

Official references:

- Robinhood Agentic MCP URL: `https://agent.robinhood.com/mcp/trading`
- Robinhood tools documented for options: `review_option_order` and
  `place_option_order`
- Robinhood notes that agentic options trading is rolling out and may not be
  available to every account.
- Robinhood Options Strategy Builder documents app-side multi-leg support, but
  production still requires proving the Agentic MCP surface supports the exact
  order shape.

## Local MCP Status

Current Codex config has the Robinhood MCP server registered:

```sh
codex mcp get robinhood-trading
```

Expected server:

```text
url: https://agent.robinhood.com/mcp/trading
transport: streamable_http
auth: OAuth
```

## Bridge Contract

Set `SPREAD_ROBINHOOD_MCP_COMMAND` to a command that:

1. Reads one JSON object from stdin.
2. Calls the requested Robinhood MCP tool.
3. Writes one JSON object to stdout.
4. For `review_option_order`, validates the Robinhood preview and echoes the
   supplied order intent as `raw.order_key`.

Input shape:

```json
{
  "server": "robinhood-trading",
  "tool": "review_option_order",
  "arguments": {
    "symbol": "CRWV",
    "strategy": "wheel",
    "quantity": 1,
    "order_effect": "credit",
    "order_type": "limit",
    "limit_price": 1.12,
    "time_in_force": "day",
    "legs": []
  }
}
```

Output shape:

```json
{
  "ok": true,
  "tool": "review_option_order",
  "raw": {
    "order_key": "{\"server\":\"robinhood-trading\",\"arguments\":{...}}"
  }
}
```

If `ok` is false, the runner records `review_failed` or
`live_order_rejected` and stops.

The runner refuses autonomous placement unless the review response echoes the
same deterministic `order_key` that would be used for placement. This binds
review and place to the same local order intent. The bridge should only echo the
key after checking Robinhood's previewed legs, side, strike, expiration,
quantity, limit price, and buying-power/max-loss terms against the request.

The local ledger defaults to `var/canary_order_ledger.json`. The runner writes
the ledger before calling `place_option_order`, so a restart after a broker call
cannot blindly repeat the same order intent. If a pre-submit ledger write is
followed by a bridge failure, manual reconciliation is required before deleting
that ledger entry.

## Rollout Flags

Shadow monitor:

```sh
scripts/run_canary_worker.sh
```

Review through MCP bridge, no placement:

```sh
SPREAD_ROBINHOOD_MCP_COMMAND=/path/to/bridge \
SPREAD_CANARY_BROKER_CASH_SECURED_PUTS=1 \
SPREAD_CANARY_BROKER_COVERED_CALLS=1 \
scripts/run_canary_worker.sh
```

Debit spreads additionally require:

```sh
SPREAD_CANARY_BROKER_MULTI_LEG_OPTIONS=1
```

Live placement requires every prior gate plus both:

```sh
SPREAD_CANARY_LIVE_ORDERS_ENABLED=1
SPREAD_CANARY_PLACE_LIVE_ORDER=1
```

Autonomous placement also requires a recently exported artifact. New exports
include `exported_at`; placement is blocked when that timestamp is older than
`SPREAD_CANARY_MAX_ORDER_AGE_SECONDS`, default `1800`.

Autonomous placement also requires the runner `--as-of` date to match today's
UTC date. Historical reruns can still review and shadow-monitor, but they cannot
submit live orders.

Only same-day `entry_candidate` actions are orderable. `open_candidate` is
monitor-only because it represents a position that would already be open in the
backtest.

Wheel actions are review-only for now. Autonomous wheel placement remains
blocked until the worker reconciles broker buying power, pending orders,
assignment state, and covered-call inventory directly from Robinhood.
