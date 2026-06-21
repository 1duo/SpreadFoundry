# SpreadFoundry

Rust-only options research system for put credit spread simulation, deterministic optimization, and later gated broker execution.

## Current v1

- Fixture-backed NVDA put-spread simulation.
- Conservative bid/ask fill model.
- Deterministic grid optimization scaffold.
- ThetaData ingest scaffolding.
- Robinhood broker capability gate that keeps credit-spread live execution disabled until atomic multi-leg support is available.

## Commands

```sh
cargo test
cargo run -- simulate --strategy put-spread --config configs/nvda_put_spread.toml
cargo run -- optimize --strategy put-spread --config configs/nvda_opt.toml --method grid
cargo run -- train-ranker --config configs/nvda_ranker.toml
cargo run -- shadow-live --symbol NVDA --strategy put-spread
cargo run -- research-nvda --from 2024-01-01 --to 2026-06-18 --max-expirations 48 --fetch-concurrency 4
cargo run -- research-symbol --symbol AAPL --from 2024-01-01 --to 2026-06-18 --max-expirations 48 --fetch-concurrency 4
cargo run -- research-universe --plateau-run runs/<nvda-run>/research.json --from 2024-01-01 --to 2026-06-18 --max-expirations 48 --fetch-concurrency 4
```

`research-universe` is plateau-gated when `--plateau-run` is supplied. Its default expansion seed is five liquid non-NVDA single stocks for put credit spread research:

```text
TSLA,AMD,META,AMZN,AAPL
```

Each universe artifact includes the seed rationale plus separate detector and execution strategy summaries per symbol.

ThetaData universe ingest requires Theta Terminal running locally:

```sh
cargo run -- ingest-theta --symbol NVDA --from 2018-01-01 --to 2026-06-19 --interval 1m
```
