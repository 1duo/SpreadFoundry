# Robinhood MCP Execution Adapter

Robinhood remains supported as a broker adapter, but Tradier is the default
execution broker. Robinhood live placement must stay fail-closed unless the
configured MCP command proves the exact order shape, including atomic multi-leg
option spreads.

Official references:

- [Trading with your agent](https://robinhood.com/us/en/support/articles/trading-with-your-agent/)
- [Agentic Trading overview](https://robinhood.com/us/en/support/articles/agentic-trading-overview/)
- [Options Strategy Builder](https://robinhood.com/us/en/support/articles/about-the-options-strategy-builder/)

## Boundary

The execution worker reads a typed `LiveSignalArtifact`, applies risk policy,
and then calls the selected broker adapter. Robinhood does not receive research
reports and the worker never assembles unmanaged multi-leg orders from separate
single-leg executions.

## Configuration

```bash
export SPREAD_ROBINHOOD_MCP_COMMAND='...'
export SPREAD_EXECUTION_BROKER=robinhood
scripts/execution-service.sh set-mode review
scripts/execution-service.sh readiness
```

`review` mode may call Robinhood review/preview tooling. `live` mode only
submits when the adapter reports broker support and live orders are enabled.

## Required Gates

- fresh `var/live_signal.json`
- `selected_signal.status = new_entry`
- canary risk policy pass
- broker capability pass
- review/preview pass
- ledger idempotency pass
- live market-window pass for `live`

If Robinhood cannot prove atomic multi-leg support for the selected signal,
the decision remains `blocked` or review-only. The system must not synthesize
a debit spread by placing two unmanaged single-leg option orders.
