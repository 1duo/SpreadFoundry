# SpreadFoundry Production Architecture

SpreadFoundry should have one execution model that research, simulation, and live
canary routing all share. The simulator is not a side utility; it is the broker
behavior model used to judge whether a strategy deserves live exposure.

## First Principles

1. Strategy code decides what it wants to do.
2. Execution code decides whether that order is valid and how it fills.
3. Broker adapters translate a validated order intent to a broker surface.
4. Live services can only place what the simulator can represent.
5. UI and service scripts report state; they do not contain trading logic.

```mermaid
flowchart LR
  D["Decision Engine"] --> I["Execution Core"]
  I --> S["Simulator"]
  I --> B["Broker Adapter"]
  S --> R["Research Reports"]
  B --> RH["Robinhood MCP Bridge"]
  B --> TR["Tradier REST Bridge"]
  W["Worker"] --> D
  W --> B
  W --> H["Health Snapshot"]
  H --> M["Menubar"]
  L["Launch Scripts"] --> W
  L --> M
```

## Module Boundaries

- `src/execution.rs`: shared order intents, option legs, conservative fill math,
  and broker-like execution assumptions.
- `src/sim.rs`: scenario simulator that calls `execution` for fills and PnL.
- `src/research.rs`: strategy generation, backtests, ranking, and promotion
  gates. It should move toward calling `execution` for every fill assumption.
- `src/broker.rs`: broker capability checks, Robinhood MCP command execution,
  and minimal Tradier REST order preview/place calls.
- `src/main.rs`: CLI orchestration and temporary adapter glue. Long-term
  strategy and execution logic should move out of this file.
- `scripts/`: service launch, teardown, and health-check entry points.
- `apps/SpreadFoundryMenubar`: native menubar app that consumes the worker JSON
  snapshot and delegates lifecycle actions to scripts.

## Phase Plan

### Phase 1: Execution Core

Status: implemented.

- Add a Rust execution core for option order intents and conservative fills.
- Route existing put-spread simulator fill math through the execution core.
- Route Robinhood MCP canary payload construction through the same order intent.
- Keep live behavior fail-closed and avoid broad research refactors.

Success criteria:

- Existing simulation tests still pass.
- MCP canary tests still pass.
- Shared execution tests prove bid/ask fill direction, expiration clamping, and
  atomic debit-spread leg shape.

### Phase 2: Simulator-First Research Refactor

Status: first slice implemented.

- Weekly research candidate entry prices and exit fills now call `execution`
  primitives for credit spreads, debit spreads, and cash-secured puts.
- Replay/live parity tests now cover one put debit spread and one cash-secured
  put candidate by mapping the research candidate to an `OptionOrderIntent`.
- Remaining work: make every strategy report its fill model, max loss,
  buying-power reserve, and broker feasibility through shared exported types.

Success criteria:

- No duplicate spread PnL math in research paths touched by canary candidates.
- Research and live canary order previews agree on legs, price effect, and
  limit price for the tested debit and cash-secured put signals.

### Phase 3: Service Runtime

Status: implemented.

- Add start, stop, restart, status, and health scripts for the canary worker.
- Write a stable JSON health snapshot under `var/`.
- Keep teardown explicit and idempotent.

Success criteria:

- `start` creates one worker, `stop` removes it, `status` reports stale/missing
  health clearly, and `restart` is safe to repeat.

Commands:

```bash
cargo build --release
scripts/spreadfoundry-service.sh start
scripts/spreadfoundry-service.sh status
scripts/spreadfoundry-service.sh restart
scripts/spreadfoundry-service.sh stop
```

`spreadfoundry-service.sh` starts the canary worker and menubar. The canary
worker is managed as a macOS LaunchAgent so it survives the launching shell.
`status` calls `spreadfoundry canary-worker-snapshot`, which reads
`var/canary_worker_health.json`, checks `var/canary_worker.pid`, and emits one
JSON object for both CLI operations and the menubar. The script never computes a
trade decision.

`scripts/canary-service.sh readiness` runs a read-only live-readiness audit from
the same persisted service environment. It reports current blockers such as
missing fresh signal, stale export timestamp, disabled broker capability flags,
or missing Robinhood MCP command without previewing or placing an order. The
script exits nonzero unless the live worker is ready to attempt broker review
for a fresh selected action. The raw `spreadfoundry canary-live-readiness`
command has the same fail-closed default; use `--allow-blocked` only for
exploratory reporting where a zero exit code is required despite blockers.

Signal notifications are optional and run after the worker computes its local
decision. `scripts/canary-service.sh configure-ntfy <topic>` persists
`SPREAD_CANARY_NOTIFY_COMMAND=scripts/notify-ntfy.sh` and the ntfy topic in
`var/canary_worker.env`. The worker sends compact JSON to the command on stdin,
dedupes notifications in `var/canary_notify_ledger.json`, and never lets
notification delivery failure block monitoring, review, or placement gates.

Broker selection is explicit. Tradier is the default path for atomic
debit-spread preview/place. Run
`scripts/canary-service.sh configure-tradier sandbox|production` to persist
Tradier account settings, or set `SPREAD_CANARY_BROKER=robinhood` to use the
Robinhood MCP bridge. Tradier V1 only executes `call_debit_spread` and
`put_debit_spread`; wheel lifecycle remains monitor-only. See
`docs/tradier_live.md`.

### Phase 4: Menubar

Status: implemented.

- Add a small macOS menubar app modeled after AxiomTrade's snapshot-consumer
  pattern.
- Display only useful operational facts: worker state, current action, broker
  mode, canary side-effect mode, last check age, and kill-switch state.
- Keep controls minimal: refresh, open docs/log, stop worker.

Success criteria:

- Menubar reads the same health JSON as CLI status.
- No trading decisions or broker calls are implemented in Swift.

Commands:

```bash
cd apps/SpreadFoundryMenubar
swift build -c release
cargo build --release
SPREAD_ROOT=/Users/1duo/Projects/SpreadFoundry swift run -c release SpreadFoundryMenubar
```

The app renders the Rust snapshot and exposes only the canary mode switch,
`Refresh`, `Start`, `Stop`, `Restart`, `Log`, `Docs`, and `Quit`.

Essential menubar functions:

- `Status`: show worker liveness, health freshness, current decision, broker
  capability mode, canary side-effect mode, and selected action.
- `Mode`: switch the worker between `monitor`, `review`, and `live` through
  `scripts/canary-service.sh set-mode`, which persists `SPREAD_CANARY_MODE` and
  restarts the worker.
- `Refresh`: force a new read of the Rust canary snapshot.
- `Start`: start the canary worker service without changing trading gates.
- `Restart`: restart the worker after a binary/config update.
- `Stop`: stop the worker fail-closed; no order routing continues.
- `Log`: open `var/canary_worker.log` for operational triage.
- `Docs`: open this architecture/runbook.
- `Quit`: close the menubar only; it does not stop the worker.

Icon design: the menubar uses an original green mark inspired by finance/trading
visual language, not by copying any broker logo. The mark combines an upward
wing/arc with two spread bars to signal gated directional option exposure.

### Phase 5: Continuous Auto-Research

Status: implemented as an opt-in canary refresh service.

- Schedule research refreshes through service scripts.
- Store candidate artifacts with provenance and simulator version.
- Require promotion gates before a candidate can reach broker review.
- Refresh only exports a canary artifact when the rerun has a canary-ready
  profile; otherwise it records `no_canary_ready_profile` and leaves the prior
  artifact untouched.
- The default refresh loop is regular-market-window gated and reruns every
  `300` seconds after `scripts/auto-research-service.sh configure-canary`.
- Each selector refresh has a process-level watchdog, default
  `SPREAD_CANARY_REFRESH_TIMEOUT_SECONDS=900`. A timeout records
  `research_timeout`, kills the refresh process tree, leaves the prior artifact
  untouched, and lets the loop retry on the next interval.

Success criteria:

- A new research result can be traced from data window to simulator version to
  canary artifact to broker review decision.

Commands:

```bash
scripts/auto-research-service.sh configure-canary
scripts/auto-research-service.sh start
scripts/auto-research-service.sh status
scripts/auto-research-service.sh stop
```

`configure-canary` persists a real refresh command in `var/auto_research.env`.
The command runs `scripts/refresh-canary-artifact.sh`, which reruns the
portfolio selector through the current UTC date during the configured market
window and attempts `export-portfolio-canary`. Each refresh writes
`var/canary_refresh_last.json`; the loop writes `var/auto_research_last.json`
with start time, finish time, exit code, and command, plus append-only logs in
`var/auto_research.log`. Candidate export still must pass the existing canary
artifact gates before the worker can review an order.
