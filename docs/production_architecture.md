# SpreadFoundry Production Architecture

SpreadFoundry separates offline research from live signal detection and broker
execution. The execution worker never consumes research reports directly and
never recomputes signals.

```mermaid
flowchart LR
  R["Strategy Research\noffline discovery"] --> A["Approved Strategy\nfrozen config"]
  A --> S["Signal Refresh\nmarket-open detector"]
  S --> J["Live Signal Artifact"]
  J --> W["Execution Worker"]
  W --> K["Canary Risk Policy"]
  W --> B["Broker Adapter"]
  B --> T["Tradier"]
  W --> H["Health + Menubar"]
```

## Canonical Layers

- `strategy_research`: offline backtests, ranking, diagnostics, and reports.
  It never produces broker-ready orders.
- `approved_strategy`: frozen detector/profile config in
  `configs/approved_strategy.json`.
- `signal_refresh`: market-open job that applies one approved strategy to fresh
  data and writes a typed live signal.
- `live_signal_artifact`: JSON contract at `var/live_signal.json`; this is the
  only input the execution worker may trade from.
- `execution_worker`: always-on service that validates mode, risk, broker state,
  preview/place results, ledger idempotency, notifications, and health.
- `execution_decision`: worker output. Mode is separate from status.
- `canary`: risk tier only, currently represented by `CanaryRiskPolicy`.

## Contracts

`ApprovedStrategy` contains the strategy id, profile name, symbols, portfolio
constraints, allowed live strategies, and canary risk policy id.

`LiveSignalArtifact` contains:

- `schema_version`
- `strategy_id`
- `as_of`
- `generated_at`
- `market_data_through`
- `signals`
- `selected_signal`

Signal status is typed as `new_entry`, `already_open`, or `recent_closed`.
Only `new_entry` can become an order attempt. A valid no-trade refresh writes a
fresh artifact with `selected_signal = null`; the worker reports `no_signal`.

Execution decision statuses are mode-independent:

- `no_signal`
- `blocked`
- `ready`
- `reviewed`
- `submitted`
- `already_submitted`
- `rejected`
- `submit_unknown`

Execution modes remain only `monitor`, `review`, and `live`.

## Services

```mermaid
flowchart LR
  C["configs/approved_strategy.json"] --> R["spreadfoundry refresh-live-signal"]
  R --> L["var/live_signal.json"]
  L --> E["spreadfoundry execution-worker"]
  E --> H["var/execution_worker_health.json"]
  E --> O["var/execution_order_ledger.json"]
  H --> M["SpreadFoundryMenubar"]
  SR["scripts/signal-refresh-service.sh"] -. "supervises" .-> R
  ES["scripts/execution-service.sh"] -. "supervises" .-> E
```

`scripts/signal-refresh-service.sh` starts, stops, and checks the signal
refresh loop. It writes `var/signal_refresh_last.json` for loop health and
`var/live_signal_refresh_last.json` for the latest refresh attempt.
The shell layer is intentionally only service orchestration: launchd/env
loading, logs, state files, timeouts, and scheduling. Market-session checks and
live signal contract validation are Rust code paths.
Refresh runs through the Rust `refresh-live-signal` command, so market-session
checks, approved profile selection, live-signal export, and refresh state are one
typed code path. The approved profile, symbol list, and portfolio constraints
come from `configs/approved_strategy.json` rather than service environment
overrides.

`scripts/execution-service.sh` starts, stops, configures, and checks the
execution worker. It writes `var/execution_worker_health.json`, logs to
`var/execution_worker.log`, and stores settings in `var/execution_worker.env`.

`scripts/spreadfoundry-service.sh` coordinates signal refresh, execution, and
the macOS menubar.

One-time migration:

```bash
scripts/spreadfoundry-service.sh migrate-legacy
```

This stops old launchd labels and imports saved configuration into the new env
files. The old service scripts are intentionally not kept as aliases.

## Broker Execution

Tradier is the default broker. Robinhood remains available behind the broker
adapter, but live spread execution still requires proven atomic multi-leg
support.

Tradier order flow:

1. Local live signal contract validation.
2. Canary risk policy validation.
3. Broker account, position, and active-order checks.
4. Current Tradier quote validation for debit spreads.
5. Tradier preview.
6. Ledger idempotency and rejection suppression.
7. Autonomous placement stays blocked until broker position reconciliation and
   exit lifecycle are implemented.

Notifications are best-effort and never block monitoring or order handling.

## Menubar

The menubar reads the Rust execution snapshot and exposes:

- `Signal Refresh`
- `Execution`
- `Mode`
- `Broker`
- `Signal`
- `Decision`
- `Account`
- `Buying Power`

Mode changes call `scripts/execution-service.sh set-mode`. The menu never
changes broker configuration or risk policy.

## Operational Commands

```bash
cargo build --release
scripts/signal-refresh-service.sh configure
scripts/signal-refresh-service.sh start
scripts/execution-service.sh start
scripts/menubar-service.sh start
scripts/spreadfoundry-service.sh status
```

Force one refresh:

```bash
scripts/signal-refresh-service.sh once
cargo run --quiet -- live-signal-status --live-signal var/live_signal.json
```

Check whether the current time is an actual configured market session:

```bash
cargo run --quiet -- market-session-status
```

Check execution readiness:

```bash
scripts/execution-service.sh readiness
```

Switch mode:

```bash
scripts/execution-service.sh set-mode monitor
scripts/execution-service.sh set-mode review
scripts/execution-service.sh set-mode live
```

Configure Tradier:

```bash
export SPREAD_TRADIER_ACCOUNT_ID=...
export SPREAD_TRADIER_TOKEN=...
scripts/execution-service.sh configure-tradier production
```
