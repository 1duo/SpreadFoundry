# Tradier Execution

Tradier is the default broker for SpreadFoundry execution. Robinhood remains in
the broker adapter, but Tradier is the first broker path with direct REST
preview/place support for atomic multi-leg option orders.

Official references:

- [Tradier Trading API](https://docs.tradier.com/docs/trading)
- [Tradier Orders](https://docs.tradier.com/docs/orders)
- [Tradier Clock](https://docs.tradier.com/docs/clock)
- [Tradier Quotes](https://docs.tradier.com/docs/quotes)

## Scope

Current live scope:

- `call_debit_spread`
- `put_debit_spread`
- `call_credit_spread` and `put_credit_spread` when a future approved strategy
  explicitly enables them
- one spread contract per order
- `class=multileg`
- debit-spread entry orders use `type=debit`; debit-spread close orders use
  `type=credit`
- credit-spread entry orders use `type=credit`; credit-spread close orders use
  `type=debit`
- `duration=day`
- Tradier buying-power validation before preview
- current Tradier quote validation before preview
- broker position and active-order reconciliation before vertical-spread entries
  and closes
- assigned short-leg vertical-spread states are detected from broker stock and
  option positions and block with a manual recovery reason
- exported execution rules drive live vertical-spread take-profit, stop-loss,
  max-hold, and force-close decisions
- debit-spread stop/max-hold/force-close rules still evaluate when the
  conservative close credit is zero or negative; if a close is due but the
  broker-safe credit payload is not representable, the worker blocks with an
  explicit manual close/expiry/assignment-risk reason
- credit-spread exits use the simulator-parity close debit
  `short_ask - long_bid`, clamped to spread width. If a close is due but the
  conservative close debit is not positive, the worker blocks with the same
  manual close/expiry/assignment-risk semantics.
- preview before any placement

Autonomous placement is enabled in the current approved production config only
for `call_debit_spread` and `put_debit_spread`. Credit-spread Tradier lifecycle
support is implemented, but it remains dormant until research approval and
`allowed_live_strategies` explicitly include a credit-spread strategy. Early
assignment recovery is not autonomous yet: the worker identifies assigned
short-call/short-put vertical-spread states, stops new lifecycle automation for
that signal, and requires operator broker recovery before continuing. Wheel
broker management has guarded code paths for cash-secured-put entry, short-put
take-profit closes, assigned-stock covered-call opens, existing covered-call
recognition from broker positions, and assigned-stock liquidation at the current
equity bid when the exported wheel lifecycle reaches its stock-mark/max-hold
exit. When wheel is approved, recent closed wheel rows are also treated as
broker-reconciliation probes, so residual assigned stock or covered-call
inventory is inspected even after the simulator marks the row closed. If the
simulator marked the wheel terminal as `covered_call_assigned` / called away but
the broker still reports residual stock, the worker blocks for manual
reconciliation instead of opening another covered call or liquidating stock
autonomously. Covered call opens require the live artifact to export explicit
`wheel_covered_call_expiration` and `wheel_covered_call_strike` fields. If those
target fields are both absent while assigned stock remains, the worker holds
intentionally until a future refresh exports an eligible covered call or the
stock liquidation rule becomes due. Wheel is not currently approved in
`configs/approved_strategy.json`; the guarded wheel bridge is also limited to
one contract per live action. OCO orders, account streaming, Robinhood live
placement, and unsupported broker/strategy combinations remain fail-closed
before order placement.

## Required Inputs

The execution worker reads only:

```text
var/live_signal.json
```

That artifact must come from the live market engine or the explicit
signal-refresh fallback and must validate as `LiveSignalArtifact`. With
`SPREAD_LIVE_ENGINE_ENABLED=1`, signal refresh writes the engine source artifact
at `var/live_signal_refresh_source.json` and the live engine writes the
execution artifact at `var/live_signal.json`. Research reports are not execution
inputs, and DuckDB live audit rows are not an order queue.
If the artifact contains a selected live entry, its embedded approved strategy
must carry production approval: either a canary-approved source or an explicit
operator risk override. Override approvals are still bounded by
`max_order_max_loss` and do not bypass broker, freshness, market-clock, ledger,
or position-lifecycle gates.

The order ledger defaults to:

```text
var/execution_order_ledger.json
```

The notification ledger defaults to:

```text
var/execution_notify_ledger.json
```

## Configuration

Sandbox:

```bash
export SPREAD_TRADIER_ACCOUNT_ID=...
export SPREAD_TRADIER_TOKEN=...
scripts/execution-service.sh configure-tradier sandbox
```

Production:

```bash
export SPREAD_TRADIER_ACCOUNT_ID=...
export SPREAD_TRADIER_TOKEN=...
scripts/execution-service.sh configure-tradier production
```

Execution mode is independent of broker configuration:

```bash
scripts/execution-service.sh set-mode monitor
scripts/execution-service.sh set-mode review
scripts/execution-service.sh set-mode live
```

## Mode Behavior

`monitor`:

- validates the live signal and local risk policy
- does not require Tradier credentials
- does not send preview or place requests
- sends a notification if a signal is actionable

`review`:

- requires Tradier credentials
- checks current Tradier buying power before entry previews
- validates current option quotes for supported vertical-spread entries and
  exits
- reconciles current positions and active orders for supported vertical-spread
  lifecycle work
- submits preview only
- records reviewed/rejected order keys to avoid repeated preview loops
- reports `reviewed` on preview success
- never places an order

`live`:

- requires Tradier credentials
- requires a fresh same-day live signal
- requires Tradier market clock to report open
- checks current Tradier buying power before entry preview/place
- validates current option quotes and broker state
- for supported vertical spreads, live quote validation requires positive
  bid/ask size on positive-priced executable sides and quote timestamps no older
  than
  `SPREAD_EXECUTION_MAX_QUOTE_AGE_SECONDS` seconds; default `30`
- rechecks broker positions and active same-underlying orders after preview and
  before local ledger reservation/place
- places only after preview, local ledger reservation, and post-submit broker
  order confirmation

## Failure Semantics

Execution is fail-closed.

- Local validation failures become `blocked`.
- Tradier quote validation failures become `blocked`.
- Tradier preview rejections become `rejected` and are recorded in the ledger.
- Placement transport failures after ledger reservation become `submit_unknown`.
  Later cycles reconcile `pending_unknown` entries by broker order id or the
  deterministic SpreadFoundry order tag when Tradier returns conclusive order
  state; ambiguous or unmatched state remains `submit_unknown`.
- Duplicate submitted ledger keys become `already_submitted`.
- Duplicate rejected ledger keys stay `rejected` until an operator clears or
  reconciles the ledger.
- Close-order preview rejections are recorded as retryable
  `preview_rejected` observations; they do not block a later close retry when
  broker state and quotes improve. Submitted and `pending_unknown` close orders
  still block duplicates unless broker reconciliation shows a prior DAY close is
  terminal-unfilled (`canceled`, `cancelled`, `rejected`, `expired`, or `error`)
  while exposure remains and no active matching order is present. A prior close
  reported `filled` while positions still show exposure blocks for manual
  reconciliation rather than submitting a second close.
- No selected entry or open-position management signal becomes `no_signal`.

Notifications are best-effort. Notification failure is logged but does not
change the execution decision.

## Acceptance Commands

```bash
cargo fmt -- --check
cargo test
cargo clippy --all-targets -- -D warnings
cargo build --release
bash -n scripts/*.sh
scripts/live-market-engine-service.sh once
scripts/live-market-engine-service.sh status
scripts/signal-refresh-service.sh once
cargo run --quiet -- live-signal-status --live-signal var/live_signal.json
scripts/execution-service.sh readiness
cargo run --quiet -- execution-worker --mode monitor --once
```
