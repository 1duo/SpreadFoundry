# Tradier Canary Execution

Tradier is the default broker for SpreadFoundry canary execution. Robinhood
remains available by setting `SPREAD_CANARY_BROKER=robinhood` or
`--broker robinhood`.

Official references:

- Trading API: `https://docs.tradier.com/docs/trading`
- Orders API: `https://docs.tradier.com/docs/orders`

## Scope

V1 supports only one-contract defined-risk debit spreads:

- `call_debit_spread`
- `put_debit_spread`

Wheel, cash-secured puts, assignment handling, stock inventory, covered calls,
exits, and OCO lifecycle are intentionally not implemented for Tradier V1.

## Modes

- `monitor`: local artifact/risk/broker-shape validation only. Tradier
  credentials are not required and no HTTP request is made.
- `review`: requires `SPREAD_TRADIER_ACCOUNT_ID` and `SPREAD_TRADIER_TOKEN`.
  Sends one Tradier preview request with `preview=true`; never places. The
  preview must include a Tradier order envelope with `status=ok` and
  `result=true`.
- `live`: requires credentials. Sends preview first, then places with
  `preview=false` only if preview succeeds and the broker-specific order key is
  absent from `var/canary_order_ledger.json`.

Preview network failures and Tradier rejections become `review_failed`. Clean
place rejections become `live_rejected`. A transport failure after local live
ledger reservation becomes `live_submit_unknown`; check Tradier before any
manual retry. None of these paths crash the worker.

## Configuration

Sandbox:

```bash
export SPREAD_TRADIER_ACCOUNT_ID=...
export SPREAD_TRADIER_TOKEN=...
scripts/canary-service.sh configure-tradier sandbox
```

Production:

```bash
export SPREAD_TRADIER_ACCOUNT_ID=...
export SPREAD_TRADIER_TOKEN=...
scripts/canary-service.sh configure-tradier production
```

`configure-tradier` persists:

- `SPREAD_CANARY_BROKER=tradier`
- `SPREAD_TRADIER_ACCOUNT_ID`
- `SPREAD_TRADIER_TOKEN`
- `SPREAD_TRADIER_BASE_URL`

When no Tradier base URL is configured, the runner defaults to
`https://sandbox.tradier.com/v1`. Production uses `https://api.tradier.com/v1`
and must be explicitly selected.

## Order Shape

The runner posts form-encoded orders to:

```text
POST /accounts/{account_id}/orders
```

The form uses Tradier's atomic multi-leg shape:

```text
class=multileg
symbol=ORCL
type=debit
duration=day
price=4.50
option_symbol[0]=ORCL260702C00220000
side[0]=buy_to_open
quantity[0]=1
option_symbol[1]=ORCL260702C00225000
side[1]=sell_to_open
quantity[1]=1
preview=true|false
```

The live ledger stores structured order state. `pending_unknown` and
`submitted` entries block duplicates; clean `rejected` entries do not. Order keys
include the broker, account, base URL, action entry date, action status, and
payload so a future same-spread signal on a different day is not suppressed by
an older canary attempt.

The canary runner derives OCC option symbols from the shared
`OptionOrderIntent`, so Robinhood payload construction, Tradier payload
construction, and simulator/research order intent validation share the same
underlying order model.

## Acceptance

```bash
cargo fmt
cargo test
cargo build --release
bash -n scripts/canary-service.sh scripts/run_canary_worker.sh
SPREAD_CANARY_MODE=monitor scripts/run_canary_worker.sh
```

Run a sandbox `review` with real Tradier sandbox credentials before any
production `live` configuration.
