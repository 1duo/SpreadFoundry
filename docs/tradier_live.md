# Tradier Execution

Tradier is the default broker for SpreadFoundry execution. Robinhood remains in
the broker adapter, but Tradier is the first broker path with direct REST
preview/place support for atomic multi-leg option orders.

Official references:

- [Tradier Trading API](https://docs.tradier.com/docs/trading)
- [Tradier Orders](https://docs.tradier.com/docs/orders)

## Scope

Current live scope:

- `call_debit_spread`
- `put_debit_spread`
- one spread contract per order
- `class=multileg`
- `type=debit`
- `duration=day`
- current Tradier quote validation before preview
- preview before any placement

Autonomous placement is currently blocked by the position-lifecycle safety gate.
Wheel entries, assignment handling, covered-call lifecycle, exits, OCO orders,
and account streaming are later phases. Until those exist, `live` mode remains
fail-closed before order placement.

## Required Inputs

The execution worker reads only:

```text
var/live_signal.json
```

That artifact must come from signal refresh and must validate as
`LiveSignalArtifact`. Research reports are not execution inputs.

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
- validates current option quotes for debit spreads
- submits preview only
- records reviewed/rejected order keys to avoid repeated preview loops
- reports `reviewed` on preview success
- never places an order

`live`:

- requires Tradier credentials
- requires a fresh same-day live signal
- validates current option quotes and broker state
- currently blocks before placement until broker position reconciliation and
  exit lifecycle are implemented

## Failure Semantics

Execution is fail-closed.

- Local validation failures become `blocked`.
- Tradier quote validation failures become `blocked`.
- Tradier preview rejections become `rejected` and are recorded in the ledger.
- Placement transport failures after ledger reservation become
  `submit_unknown`.
- Duplicate submitted ledger keys become `already_submitted`.
- Duplicate rejected ledger keys stay `rejected` until an operator clears or
  reconciles the ledger.
- No selected signal becomes `no_signal`.

Notifications are best-effort. Notification failure is logged but does not
change the execution decision.

## Acceptance Commands

```bash
cargo fmt -- --check
cargo test
cargo clippy --all-targets -- -D warnings
cargo build --release
bash -n scripts/*.sh
scripts/signal-refresh-service.sh once
cargo run --quiet -- live-signal-status --live-signal var/live_signal.json
scripts/execution-service.sh readiness
cargo run --quiet -- execution-worker --mode monitor --once
```
