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
```

ThetaData universe ingest requires Theta Terminal running locally:

```sh
cargo run -- ingest-theta --symbol NVDA --from 2018-01-01 --to 2026-06-19 --interval 1m
```
