# Weekly Selector Canary Workflow

Historical research note only. Current production operations use
`ApprovedStrategy`, `LiveSignalArtifact`, signal refresh, and the execution
worker documented in `docs/production_architecture.md`.

This note freezes the current professional decision for the frequent weekly-options research lane.

## Current Decision

There is no current canary-ready portfolio after the AMD-loaded audit.

- Superseding source run: `runs/portfolio-weekly-selector-research-20260628T105245.740136000Z`
- AMD cache evidence: `82` expirations loaded, `64433` rows loaded, first usable date `2016-03-04`, with `606` remaining cache failures
- Canary readiness: all `22` profiles blocked
- Export decision: do not regenerate `candidates/weekly_selector_canary.json` from this run because no profile is canary-ready

Current-code original-basket rerun `runs/portfolio-weekly-selector-research-20260628T124154.801278000Z` added explicit debit-only selector candidates and still found no canary-ready profile. The best ranked profile is now `selector_crash_put_and_call_debits_only`: `1577` trades, `58435` PnL, `19010` $25-cost PnL, `1.42` profit factor, and no wheel inventory risk. It is blocked because annual stability failed: `2019` lost `-2687` and `2020` lost `-6356`. This is a better research direction than short-credit filler, but it is not deployable.

The previous frozen artifact remains useful as a historical research artifact only. It should not be treated as a live action candidate unless a fresh rerun produces a canary-ready profile and an actionable same-day `entry_candidate` or still-open `open_candidate`.

The strongest AMD-loaded profiles still fail professional deployment logic:

- `selector_trend_credit_put_spread_plus_crash_put_and_call_debits`: `1791` trades, `69134` PnL, `24359` $25-cost PnL, but blocked because the active `put_credit_spread` sleeve lost `-13684`.
- `selector_call_credit_weak_plus_crash_put_and_call_debits`: `2768` trades, `74884` PnL, `5684` $25-cost PnL, but blocked because the active `call_credit_spread` sleeve lost `-24611`.
- `selector_economic_wheel_plus_crash_put_and_call_debits`: `1361` trades, `90463` PnL, `56438` $25-cost PnL, but blocked by annual stability with worst year `2018` at `-1442`.
- `selector_inventory_wheel_plus_put_and_call_debits`: `1483` trades, `83817` PnL, `46742` $25-cost PnL, but blocked because inventory risk is high: `28.1%` assignment rate, `44.1%` marked-stock loss/PnL, and `-36970` marked-stock PnL.

## Previous Frozen Artifact

- Candidate artifact: `candidates/weekly_selector_canary.json`
- Source run: `runs/portfolio-weekly-selector-research-20260628T103134.311416000Z`
- Profile: `selector_economic_wheel_plus_crash_put_and_call_debits`
- Artifact status: `canary_only`
- Recommended capital fraction: `5%`
- Full promotion: blocked

The pass is a portfolio selector across economic wheel, crash-regime put debit, and trend call debit sleeves. The source run requested the expanded symbol list, but cache-only data was usable only for `IREN`, `PLTR`, `ORCL`, `TSLA`, and `CRWV`, so the evidence is effectively from that basket.

This artifact is superseded by the AMD-loaded audit above.

## Evidence Snapshot

- Research gate: `research_pass`
- Portfolio constraints: `20%` max symbol allocation, `8` max open positions, `2` max positions per symbol
- Trades: `1253` vs `1091` required
- PnL: `90821`
- Profit factor: `1.87`
- Max drawdown: `0.42%`
- $10/trade stress PnL: `78291`
- $25/trade stress PnL: `59496`
- PnL/DD capital: `217.88`
- $25-stressed PnL/DD capital: `142.73`

The candidate remains canary-only because TSLA contributes `65.4%` of PnL, only two symbol ablations pass, and only one strategy-sleeve ablation passes.

## Ablation Method

Ablations now re-run allocation after removing a symbol or strategy, instead of merely filtering already accepted trades. This is the correct professional robustness test because freed capital and position slots can be filled by alternative opportunities.

The corrected method improved the strategy-sleeve evidence: removing the wheel sleeve now passes after reallocation. The candidate still fails full promotion because removing TSLA, ORCL, or PLTR leaves too few trades for the weekly cadence gate, and only two symbol ablations pass.

## Wheel Risk Review

The current evidence does not support replacing the selector with a pure wheel. In the best selector, the wheel sleeve has `282` trades, `21013` PnL, and a worst trade of `-3916`.

The wheel assignment path remains the key risk: `53` assigned cycles, `22` marked-stock cycles, and `-19056` marked-stock PnL. Unlike the prior guarded profile, this economic wheel sleeve is profitable and has `inventory_risk_contained`, but it still depends on the debit sleeves and must not be treated as a standalone wheel system.

The direct pure-wheel portfolio comparison also blocks. Current-code run `runs/portfolio-weekly-wheel-research-20260628T123337.193877000Z` found best profile `weekly_wheel_inventory_exit_dte3_10_callfloor95_credit01_hold21` with `492` trades versus `883` required, `25041` PnL, `12741` $25-cost-stressed PnL, and `inventory_risk_high`. The assignment rate was `29.7%`, marked-stock PnL was `-30624`, and the worst marked-stock loss was `-5191`. Prior run `runs/portfolio-weekly-wheel-research-20260628T093634.766879000Z` reached the same conclusion with `497` trades, `24761` PnL, and `inventory_risk_high`.

Use wheel-like inventory only as a guarded sleeve while assignment losses, marked-stock losses, and symbol concentration are explicitly monitored.

Current-code reruns of the pure-wheel basket were initially interrupted because the wheel loader spent several minutes walking TSLA call-side cache misses for covered-call simulation. Do not treat those interrupted runs as evidence; the empty run directories were removed. The cache-only loader now pre-checks complete call-side OI and Greeks coverage before trying to load calls in mixed put/call mode, so missing call windows skip directly to put-only evidence instead of walking the slower miss/error path.

A narrow TSLA wheel loader probe, `runs/portfolio-weekly-wheel-research-20260628T114943.501659000Z`, completed with `19` put expirations, `0` call expirations, and `53808` rows; it produced only `4` trades, so it is a loader sanity check rather than deployment evidence. A bounded current-code basket rerun, `runs/portfolio-weekly-wheel-research-20260628T115158.731972000Z`, completed with `--max-expirations 80`; it loaded incomplete call-side coverage across the basket and found best profile `weekly_wheel_dte3_10_delta10_25_credit01_hold21` with only `23` trades, `2847` PnL, `2272` $25-cost-stressed PnL, `17.4%` assignment rate, and gate status `blocked`. This confirms the loader fix, not a deployable wheel edge. Before another full wheel rerun, warm call-side cache or narrow to symbol/date windows with known call coverage.

The repo now has an `audit-option-cache-coverage` command for this exact issue. On the original basket, the bounded `--max-expirations 80` audit showed why the bounded wheel sample was weak: IREN had only `12.5%` both-side coverage, PLTR and ORCL had `0.0%`, TSLA had `0.0%`, and CRWV had `0.0%` because bounded samples request extra regime-lookback cache windows. The uncapped full-history audit is much healthier: IREN `100.0%` both-side coverage, PLTR `100.0%`, ORCL `99.1%`, TSLA `67.7%`, and CRWV `100.0%`. This means call-side alternatives are data-feasible on full-history runs, but bounded samples can be misleading unless the lookback cache is warmed too. The cache fallback now indexes available windows, reuses parsed cache JSON, suppresses per-chunk/per-expiration fallback logs, borrows opportunity lists during allocation/ablation, evaluates profiles in parallel, and indexes covered-call rows by quote date. With those fixes, the uncapped wheel basket completed in `137s` and loaded IREN `152/150` put/call expirations, PLTR `300/300`, ORCL `441/441`, TSLA `469/328`, and CRWV `66/66`.

Wheel risk-control sweep after the first-principles review: four full-history cache-only runs retested the proposed "sell puts until assigned, then sell calls" idea on the original basket with tighter capital and inventory controls:

```sh
cargo run --quiet -- research-portfolio-wheel --symbols IREN,PLTR,ORCL,TSLA,CRWV --from 2010-01-01 --to 2026-06-28 --cache-only --fetch-concurrency 2 --symbol-concurrency 2 --capital-budget 100000 --max-symbol-allocation-pct 0.20 --max-open-positions 4 --max-positions-per-symbol 1 --symbol-drawdown-cooldown-trigger-pct 0.10 --symbol-drawdown-cooldown-days 20
cargo run --quiet -- research-portfolio-wheel --symbols IREN,PLTR,ORCL,TSLA,CRWV --from 2010-01-01 --to 2026-06-28 --cache-only --fetch-concurrency 2 --symbol-concurrency 2 --capital-budget 100000 --max-symbol-allocation-pct 0.20 --max-open-positions 3 --max-positions-per-symbol 1 --symbol-drawdown-cooldown-trigger-pct 0.10 --symbol-drawdown-cooldown-days 20
cargo run --quiet -- research-portfolio-wheel --symbols IREN,PLTR,ORCL,TSLA,CRWV --from 2010-01-01 --to 2026-06-28 --cache-only --fetch-concurrency 2 --symbol-concurrency 2 --capital-budget 100000 --max-symbol-allocation-pct 0.20 --max-open-positions 5 --max-positions-per-symbol 2 --symbol-drawdown-cooldown-trigger-pct 0.10 --symbol-drawdown-cooldown-days 20
cargo run --quiet -- research-portfolio-wheel --symbols IREN,PLTR,ORCL,TSLA,CRWV --from 2010-01-01 --to 2026-06-28 --cache-only --fetch-concurrency 2 --symbol-concurrency 2 --capital-budget 100000 --max-symbol-allocation-pct 0.20 --max-open-positions 5 --max-positions-per-symbol 2
```

All four variants stayed blocked:

- `runs/portfolio-weekly-wheel-research-20260628T222607.711760000Z` (`20%` cap, `4` open, `1` per symbol, symbol cooldown): best profile had `277` trades versus `1091` required, `12164` PnL, `5239` $25-cost PnL, `5.62%` capital DD, `29.6%` assignment rate, `-24372` marked-stock PnL, `-5299` worst marked-stock loss, and `inventory_risk_high`.
- `runs/portfolio-weekly-wheel-research-20260628T222607.699252000Z` (`20%` cap, `3` open, `1` per symbol, symbol cooldown): best profile had `270` trades, `10366` PnL, `3616` $25-cost PnL, `6.98%` capital DD, `30.7%` assignment rate, `-25153` marked-stock PnL, and `inventory_risk_high`.
- `runs/portfolio-weekly-wheel-research-20260628T223042.657327000Z` (`20%` cap, `5` open, `2` per symbol, symbol cooldown): best profile had `411` trades, `23523` PnL, `13248` $25-cost PnL, `6.81%` capital DD, `28.2%` assignment rate, `-28801` marked-stock PnL, `-5246` worst marked-stock loss, and `inventory_risk_high`.
- `runs/portfolio-weekly-wheel-research-20260628T223042.670800000Z` (`20%` cap, `5` open, `2` per symbol, no cooldown): best profile had `490` trades, `17173` PnL, `4923` $25-cost PnL, `8.18%` capital DD, `29.8%` assignment rate, `-33846` marked-stock PnL, `-5191` worst marked-stock loss, and `inventory_risk_high`.

Professional conclusion after the wheel risk-control sweep: stricter wheel controls do not solve the core problem. One-position-per-symbol controls reduce trades too far while leaving assignment risk near `30%`. Allowing two positions per symbol improves PnL, and symbol cooldown helps versus no cooldown, but the best controlled row still has only `411` trades versus `1091` required and carries large marked-stock losses. Do not pivot the current basket to a pure wheel. Wheel exposure can remain only as a guarded sleeve inside a broader selector, and only if inventory-risk gates stay active.

## PnL/DD Challenger

The strongest remaining PnL/DD challenger is `selector_inventory_wheel_plus_put_and_call_debits`. It has higher PnL/DD capital (`254.53` vs `217.88`) and higher $25-stressed PnL/DD capital (`146.44` vs `142.73`) under the same `20%` symbol cap.

It is not the frozen canary because inventory risk is much higher: assignment rate is `30.1%`, marked-stock loss/PnL is `42.8%`, and the profile is flagged `inventory_risk_high`. Treat it as a research challenger, not as a current replacement, until inventory-risk controls improve.

## Diversification Review

Strict one-position-per-symbol constraints blocked every selector because cadence fell below the weekly gate. Softer capital caps worked better: `20%` max symbol allocation with `2` positions per symbol improved PnL, PnL/DD, $25-stressed PnL/DD, worst trade loss, and marked-stock loss/PnL versus the prior `25%` cap.

The `15%` cap also passed, but it reduced PnL and stressed PnL without solving concentration. Keep the `20%` cap as the frozen canary setting.

Total trade caps per symbol were also tested and rejected as a canary replacement. A `400` cap reduced TSLA PnL share to `63.5%`, but cut PnL to `74374`, $25-cost PnL to `45249`, PnL/DD capital to `159.48`, and symbol ablation passes to `1`. A `350` cap reduced TSLA PnL share to `56.7%`, but cut PnL to `62500`, $25-cost PnL to `34525`, PnL/DD capital to `131.35`, and symbol ablation passes to `0`. The cap solves less concentration than it costs in robustness and economics.

## Fallback Sleeve Review

Adding a second put-debit fallback sleeve did not improve promotion readiness.

- `selector_economic_wheel_plus_crash_and_pullback_puts_and_call_debits` increased trades and PnL, but failed annual stability after symbol ablations and did not pass any strategy ablation.
- `selector_economic_wheel_plus_crash_and_costaware_puts_and_call_debits` increased raw PnL to `126055` and reduced TSLA concentration to `51.8%`, but failed chronological robustness and annual stability. It is not deployable despite attractive headline metrics.

Keep the simpler economic wheel + crash put debit + call debit selector as the frozen canary until fallback sleeves pass robustness, not just raw PnL.

## Credit Spread Sleeve Review

Adding defined-risk weekly put-credit-spread sleeves improved raw cadence but did not add deployable edge.

- `selector_trend_credit_put_spread_plus_crash_put_and_call_debits` ranked first by score with `1643` trades and `68714` PnL, but it is blocked because the active `put_credit_spread` sleeve lost `-13261`, max drawdown rose to `1.27%`, and TSLA still contributed `70.9%` of PnL.
- `selector_economic_wheel_plus_credit_put_spread_and_debits` improved PnL/DD capital (`245.93`) and strategy-ablation count, but the credit sleeve lost `-8626`; removing that sleeve reverts to the current frozen selector with higher PnL and higher $25-cost PnL.

The canary export path now selects the first canary-ready profile instead of blindly exporting the top-ranked profile. Negative-PnL active sleeves block canary readiness, so credit spreads can remain research challengers without contaminating the frozen canary.

Weekly call-credit-spread sleeves were also tested as a bearish/overbought counterpart. They are rejected for the same reason: the active sleeve loses money even when the aggregate portfolio can look better.

- `selector_call_credit_weak_plus_crash_put_and_call_debits` had `2577` trades and `73877` PnL, but the `call_credit_spread` sleeve lost `-24519` and $25-cost PnL fell to `9452`.
- `selector_economic_wheel_plus_call_credit_and_debits` had `2050` trades and `105097` PnL, but the `call_credit_spread` sleeve lost `-18937`, so it is blocked despite better headline PnL.
- Call-credit-only profiles were directly negative: `selector_call_credit_weak_only` lost `-24270`; `selector_call_credit_overbought_only` lost `-10080`.

Do not use short-call credit spreads as cadence filler. They are valid research challengers only if a future regime filter makes the call-credit sleeve independently positive after friction.

## Debit-Only Selector Review

Current-code run `runs/portfolio-weekly-selector-research-20260628T124154.801278000Z` added two explicit debit-only challengers:

- `selector_crash_put_and_call_debits_only`: `1577` trades, `58435` PnL, `19010` $25-cost PnL, and `no_wheel_inventory`. Both active sleeves are independently positive: put debit `35413` PnL and call debit `23022` PnL. It is still blocked because annual stability failed in `2019` and `2020`, max drawdown is `3.36%`, and TSLA contributes `69.8%` of PnL.
- `selector_call_debit_trend_only`: `829` trades, `9825` PnL, and `-10900` $25-cost PnL. It fails the weekly cadence gate and is not a standalone candidate.

Follow-up run `runs/portfolio-weekly-selector-research-20260628T124833.916803000Z` added drawdown-required put-debit variants to avoid buying puts in shallow dips or positive trend tape. `selector_drawdown_put_and_call_debits_only` improved the original `2019`/`2020` annual problem (`2019` only `-469`, `2020` only `-661`), but it was not a better candidate: `1180` trades, `31376` PnL, only `1876` $25-cost PnL, `3.74%` max drawdown, and a new negative `2021` at `-3921`. `selector_drawdown_put_debit_only` had stronger raw put-debit economics (`23179` PnL, `13979` $25-cost PnL) but only `368` trades, so it fails the weekly cadence objective.

Follow-up run `runs/portfolio-weekly-selector-research-20260628T125514.862370000Z` tested cost-aware call-debit sleeves. This is the strongest debit-only improvement so far:

- `selector_crash_put_and_costaware_call_debits_only`: `1657` trades, `87334` PnL, `45909` $25-cost PnL, `1.69` profit factor, and both sleeves independently positive after $25/trade stress: put debit `20829`, call debit `25080`. It remains blocked because `2019` lost `-2570`, `2020` lost `-6797`, max drawdown is `3.79%`, and TSLA still contributes `66.1%` of PnL.
- `selector_drawdown_put_and_costaware_call_debits_only`: `1369` trades, `50081` PnL, `15856` $25-cost PnL, and the drawdown put gate nearly fixes annual stability: `2019` lost only `-338`, `2020` lost `-1172`, and all later years are positive. It is still blocked because `2020` is just beyond the material negative-year threshold and cost-stressed PnL is much weaker than the crash-put/cost-aware-call variant.

Follow-up run `runs/portfolio-weekly-selector-research-20260628T130653.661332000Z` tested narrower disciplined debit risk:

- `selector_disciplined_drawdown_put_and_costaware_call_debits_only`: `1360` trades, `42994` PnL, `8994` $25-cost PnL, and the full research gate passes. It is still not canary-ready because max drawdown is `3.40%`, above the `1%` canary threshold.
- `selector_disciplined_debits_only`: `1363` trades, `32931` PnL, but `-1144` $25-cost PnL. Narrowing both put and call debit sleeves reduced max drawdown to `2.83%`, but destroyed friction-adjusted edge.

Follow-up allocation sweeps tested whether portfolio heat controls could solve the drawdown problem without changing entries:

- `runs/portfolio-weekly-selector-research-20260628T131405.633416000Z` (`6` max open positions) kept the disciplined drawdown put + cost-aware call selector research-passing with `1338` trades, `48971` PnL, `15521` $25-cost PnL, and `3.43%` max drawdown.
- `runs/portfolio-weekly-selector-research-20260628T131652.794261000Z` (`4` max open positions) was the best heat-control setting. `selector_disciplined_drawdown_put_and_costaware_call_debits_only` had `1180` trades, `58776` PnL, `29276` $25-cost PnL, and `2.78%` max drawdown; `selector_disciplined_debits_only` had `1183` trades, `46216` PnL, `16641` $25-cost PnL, and `2.54%` max drawdown.
- `runs/portfolio-weekly-selector-research-20260628T131939.182153000Z` (`3` max open positions) failed cadence for the disciplined debit candidates, with roughly `1000` trades versus `1091` required.
- `runs/portfolio-weekly-selector-research-20260628T132226.597405000Z` (`1` max position per symbol) also failed cadence for the debit-only candidates and pushed top-ranked profiles back toward losing credit sleeves.
- `runs/portfolio-weekly-selector-research-20260628T132513.144267000Z` (`10%` symbol cap) did not materially improve the drawdown problem; the disciplined drawdown put + cost-aware call selector still had `3.33%` max drawdown.

Follow-up run `runs/portfolio-weekly-selector-research-20260628T133137.805310000Z` added a balanced cost-aware call-debit sleeve between the uncapped cost-aware call and the narrow disciplined call:

- `selector_drawdown_put_and_balanced_call_debits_only` became the top open-4 profile with `1186` trades, `65858` PnL, `36208` $25-cost PnL, `1.80` profit factor, and `2.43%` max drawdown. It is still blocked because `2020` lost `-1172`, just beyond the material negative-year threshold.
- `selector_disciplined_drawdown_put_and_balanced_call_debits_only` is the cleanest current research-pass debit-only variant: `1181` trades, `57507` PnL, `27982` $25-cost PnL, `1.75` profit factor, `2.63%` max drawdown, and worst trade `-391`. It is still not canary-ready because max drawdown is above `1%`, TSLA remains the largest PnL contributor at `48.8%`, and no strategy ablation passes.

Follow-up portfolio drawdown-cooldown sweeps added a default-off research throttle and tested it at the same open-4 setting:

- `runs/portfolio-weekly-selector-research-20260628T134058.765712000Z` (`1%` realized portfolio drawdown trigger, `10` cooldown days) reduced some inventory-style profile drawdown, but broke debit-only cadence. `selector_disciplined_drawdown_put_and_balanced_call_debits_only` fell to `539` trades versus `1091` required, `17651` PnL, `4176` $25-cost PnL, and `6.64%` max drawdown.
- `runs/portfolio-weekly-selector-research-20260628T134345.754785000Z` (`1%`, `20` days) was worse for the balanced debit selector: `398` trades, `11882` PnL, `1932` $25-cost PnL, and `7.01%` max drawdown.
- `runs/portfolio-weekly-selector-research-20260628T134632.468943000Z` (`1.5%`, `10` days) still failed cadence for the balanced debit selector: `665` trades, `39134` PnL, `22509` $25-cost PnL, and `5.70%` max drawdown.

The portfolio throttle is useful as a research control, but it is not the current canary fix. It removes too many later winners and causes the weekly cadence gate to fail before drawdown reaches the `1%` canary threshold.

Follow-up run `runs/portfolio-weekly-selector-research-20260628T135709.662811000Z` added explicit outer-short-leg delta guards for debit spreads. This tests the observed weakness that many large losers came from debit spreads whose short leg was too close to the money, especially around TSLA.

- `selector_legguard15_debits_only`: `1126` trades, `56152` PnL, `28002` $25-cost PnL, `1.80` profit factor, `3.06%` max drawdown, worst trade `-358`, and TSLA PnL share `46.3%`. It passes the research gate but remains above the `1%` canary drawdown threshold.
- `selector_disciplined_put_and_call_legguard15_debits_only`: `1161` trades, `58135` PnL, `29110` $25-cost PnL, `1.78` profit factor, `2.48%` max drawdown, worst trade `-391`, and TSLA PnL share `49.5%`. This is the best current research-pass near-miss on economics, friction, and concentration, but it still is not canary-ready because max drawdown remains too high and no strategy ablation passes.
- The unguarded `selector_drawdown_put_and_balanced_call_debits_only` still ranks first by score with `65858` PnL and `2.43%` max drawdown, but it is blocked by annual stability: `2020` lost `-1172`.

Drawdown attribution shows why the signal remains fragile even though the headline PnL is positive. For `selector_disciplined_put_and_call_legguard15_debits_only`, the maximum drawdown window ran from `2019-08-15` to `2021-06-21`, lost `-4372`, and was dominated by TSLA: `-4184` from TSLA, `-829` from ORCL, and `+641` from PLTR. The loss was also call-debit heavy: `-2964` from call debit spreads and `-1408` from put debit spreads.

Follow-up symbol drawdown-cooldown sweeps added a default-off allocator throttle. Unlike the portfolio-wide throttle, this only pauses the symbol that has realized a drawdown from its own high-water mark, using the symbol allocation cap as the denominator.

- `runs/portfolio-weekly-selector-research-20260628T141926.746468000Z` (`5%` of symbol cap, `5` days) was too tight. It produced no research-pass profiles. The best disciplined legguard candidate fell to `970` trades, `35770` PnL, `11520` $25-cost PnL, and `3.41%` max drawdown.
- `runs/portfolio-weekly-selector-research-20260628T141653.950272000Z` (`10%`, `5` days) preserved cadence and produced three research-pass debit profiles. `selector_legguard15_debits_only` became the top pass: `1095` trades, `57299` PnL, `29924` $25-cost PnL, `1.88` profit factor, and `2.31%` max drawdown.
- `runs/portfolio-weekly-selector-research-20260628T142200.019863000Z` (`15%`, `5` days) was the best professional adjustment for the disciplined profile. `selector_disciplined_put_and_call_legguard15_debits_only` stayed research-pass with `1143` trades, `58861` PnL, `30286` $25-cost PnL, `1.83` profit factor, and `2.26%` max drawdown. Versus the no-cooldown baseline for the same selector, it improved PnL, $25-cost PnL, profit factor, and drawdown while sacrificing only `18` trades.

The 15% symbol throttle reduced the same TSLA-heavy maximum drawdown window from `-4372` to `-3860`; TSLA's loss in that window improved from `-4184` to `-3677`. This is useful, but it is not a full solution. TSLA remains the dominant contributor, strategy-sleeve ablations still do not pass, and max drawdown is still far above the `1%` canary target.

Follow-up accounting hardening added explicit portfolio-capital closed-equity drawdown. The older `metrics.max_drawdown` remains useful as a risk-normalized scoring statistic, but it is not the right canary drawdown denominator because it divides by cumulative trade max-loss. Canary readiness now uses closed-equity drawdown divided by the `100000` capital budget.

Corrected run `runs/portfolio-weekly-selector-research-20260628T143948.510542000Z` regenerated the `15%` symbol-cooldown selector under that stricter accounting:

- `selector_disciplined_put_and_call_legguard15_debits_only`: `1143` trades, `58861` PnL, `30286` $25-cost PnL, `1.83` profit factor, `2.26%` risk-normalized drawdown, and `3.86%` capital drawdown. It remains research-pass but canary-blocked.
- `selector_legguard15_debits_only`: `1107` trades, `56237` PnL, `28562` $25-cost PnL, `1.83` profit factor, `2.53%` risk-normalized drawdown, and `3.96%` capital drawdown. It remains research-pass but canary-blocked.
- The $25-cost capital drawdowns are materially worse, roughly `7.21%` for the disciplined legguard selector and `8.74%` for the legguard-only selector.

This correction makes the previous professional conclusion stricter: symbol-local cooling improves the debit branch, but the best original-symbol weekly candidates are still not within a `1%` capital drawdown canary envelope.

Follow-up allocation-only sweeps tested whether the corrected capital-drawdown problem could be solved without changing entries or adding symbols:

- `runs/portfolio-weekly-selector-research-20260628T144432.128930000Z` (`max_open_positions=2`, `max_positions_per_symbol=1`, `15%` symbol cooldown for `5` days) produced no research-pass profiles. The lowest-capital-drawdown candidate still had `2.58%` capital drawdown and only `662` trades versus `1091` required.
- `runs/portfolio-weekly-selector-research-20260628T144720.362952000Z` (`max_open_positions=3`, `max_positions_per_symbol=1`, `15%`/`5` days) also produced no research-pass profiles. The lowest-capital-drawdown candidates remained around `2.79%` to `3.58%` and failed cadence.
- `runs/portfolio-weekly-selector-research-20260628T145007.857949000Z` (`10%` symbol cooldown for `20` days, baseline open-position limits) is the best allocation-only tradeoff so far. `selector_disciplined_put_and_call_legguard15_debits_only` remained research-pass with `1106` trades, `58395` PnL, `30745` $25-cost PnL, `2.78%` capital drawdown, and `5.33%` $25-cost capital drawdown.
- `runs/portfolio-weekly-selector-research-20260628T145254.835093000Z` (`5%` symbol cooldown for `20` days) was too strict. It produced no research-pass profiles; the best low-drawdown candidate had `833` trades and `3.41%` capital drawdown.

The allocation-only conclusion is now fairly clear: throttles can improve the original five-symbol debit branch, but they cannot get it near a `1%` capital drawdown budget while preserving weekly cadence. A true canary candidate likely needs lower-dollar-risk symbols/contracts, better regime filters, or a different entry family rather than more portfolio-level throttling.

TSLA ablation and substitution tests confirm the concentration tradeoff:

- `runs/portfolio-weekly-selector-research-20260628T145625.708371000Z` removed TSLA and kept `IREN`, `PLTR`, `ORCL`, and `CRWV` with the `10%`/`20` day symbol cooldown. No profile passed. Drawdown improved, but cadence and friction-adjusted edge broke: the closest cadence candidate had `951` trades versus `990` required, `23585` PnL, `-190` $25-cost PnL, and `1.49%` capital drawdown. TSLA is a major drawdown source, but simply removing it leaves too little robust, friction-positive signal.
- A cache audit for lower-dollar/high-cadence expansion candidates found no local cache for `SOFI`, `HOOD`, `RKLB`, `ASTS`, `COIN`, or `SMCI`. `AMD` has partial usable evidence, `META` has shallow evidence, and `NVDA` is still put-side-heavy with no cached call coverage.
- `runs/portfolio-weekly-selector-research-20260628T145803.976694000Z` replaced TSLA with AMD: `IREN`, `PLTR`, `ORCL`, `CRWV`, and `AMD`, using the same `10%`/`20` day symbol cooldown. AMD loaded `343` put expirations and `82` call expirations. The run produced two research-pass profiles, but no canary candidate. The best pass, `selector_trend_credit_put_spread_plus_crash_put_and_call_debits`, had `1387` trades, `41344` PnL, only `6669` $25-cost PnL, `2.48%` capital drawdown, and `16.16%` $25-cost capital drawdown. AMD helps cadence but does not solve capital drawdown or friction.

Professional implication: TSLA should be treated as an unstable edge carrier rather than a removable nuisance. Replacing it requires fresh lower-dollar symbols with real call and put coverage, not just dropping TSLA from the basket. The next research round should prioritize warming and validating lower-dollar/liquid weekly names before more profile tweaks on the original five.

Lower-dollar symbol warmup started with `SOFI`, `HOOD`, `RKLB`, and `ASTS`. A bounded live-backed selector run was launched with `--max-expirations 80`, then stopped after it had clearly warmed SOFI/HOOD but before RKLB/ASTS began. This left the following cache state:

- `SOFI`: `2633` cache files; audit over `80` sampled expirations found `60` complete put expirations, `59` complete call expirations, and `59` complete put+call expirations. Complete call coverage spans `2021-06-04` to `2025-02-28`.
- `HOOD`: `2059` cache files; audit over `80` sampled expirations found `48` complete put expirations, `44` complete call expirations, and `44` complete put+call expirations. Complete call coverage spans `2021-08-13` to `2024-04-12`.
- `RKLB` and `ASTS`: initially had no local expiration cache after this partial run.

SOFI/HOOD standalone selector run `runs/portfolio-weekly-selector-research-20260628T152601.564810000Z` is not usable as a candidate yet. It loaded `60` SOFI put expirations, `59` SOFI call expirations, `48` HOOD put expirations, and `44` HOOD call expirations, but generated only `192` trades in the top profile versus `531` required. The top profile had only `473` PnL and `-4327` $25-cost PnL. This is low-dollar risk, but far too sparse and friction-negative.

Combined original-plus-SOFI/HOOD run `runs/portfolio-weekly-selector-research-20260628T152634.350606000Z` also did not solve the canary problem. It produced four research-pass profiles, but no canary candidate. The best pass by capital drawdown, `selector_disciplined_put_and_call_legguard15_debits_only`, had `1130` trades, `57821` PnL, `29571` $25-cost PnL, `2.78%` capital drawdown, and `5.39%` $25-cost capital drawdown. TSLA still contributed the largest PnL share at `47.7%`. SOFI/HOOD therefore add a little data, but not enough signal or diversification yet.

A later bounded RKLB/ASTS warmup with `--max-expirations 20` produced enough cache for a small probe:

- RKLB audit: `188` expirations discovered, `20` audited, `20` complete put expirations, `16` complete call expirations, and `16` complete put+call expirations. Complete call coverage spans `2021-09-17` to `2025-10-17`.
- ASTS audit: `230` expirations discovered, `20` audited, `20` complete put expirations, `20` complete call expirations, and `20` complete put+call expirations. Complete call coverage spans `2021-04-16` to `2026-07-10`.
- Standalone RKLB/ASTS selector run `runs/portfolio-weekly-selector-research-20260628T154317.981589000Z` is not a candidate. The top profile generated only `63` trades versus `545` required, with `1642` PnL, `1012` $10-cost PnL, `1.12%` capital drawdown, and high inventory risk. RKLB was negative (`-161` PnL); ASTS carried more than all of the profit.
- Combined original-plus-SOFI/HOOD/RKLB/ASTS run `runs/portfolio-weekly-selector-research-20260628T154336.975892000Z` improved gross PnL but still produced no canary candidate. The top profile had `1150` trades, `69177` PnL, and `3.00%` capital drawdown, but was blocked by annual stability. The best research-pass low-drawdown profile, `selector_disciplined_put_and_call_legguard15_debits_only`, had `1134` trades, `57651` PnL, `29301` $25-cost PnL, `2.78%` capital drawdown, and `5.39%` $25-cost capital drawdown. That is effectively unchanged from the SOFI/HOOD-only expansion and still above the canary risk budget.

Lower-dollar expansion remains directionally sensible, but the current warmed evidence says these additions are marginal, not a fix. The bottleneck is still robust edge density and drawdown concentration, not just contract notional.

A next lower-dollar/high-beta cohort was started with `RIVN`, `MARA`, `RIOT`, and `U`. Current price/volume made these reasonable research candidates, but there was no local cache before the run. A bounded live-backed warmup was stopped once it had produced enough files for a first cache audit, not enough for promotion evidence:

- `RIVN`: `257` expirations discovered; `20` audited; only `2` complete put+call expirations.
- `MARA`: `366` expirations discovered; `20` audited; only `3` complete put+call expirations.
- `RIOT`: `455` expirations discovered; `20` audited; initially `4` complete put+call expirations.
- `U`: `263` expirations discovered; `20` audited; only `2` complete put+call expirations.

Because the full cohort was too sparse, the next-symbol probe narrowed to RIOT. A focused RIOT warmup lifted the audit to `12` complete put expirations, `10` complete call expirations, and `10` complete put+call expirations out of the `20` sampled windows. The standalone RIOT selector run `runs/portfolio-weekly-selector-research-20260628T155609.689802000Z` still was not useful as a strategy candidate: it loaded `12` expirations, `16017` rows, and produced only `7` trades versus `892` required. The top profile had `157` PnL, but only `-18` $25-cost PnL and failed cadence by orders of magnitude.

Combined original-plus-RIOT run `runs/portfolio-weekly-selector-research-20260628T155646.967534000Z` confirms RIOT is not yet contributing to the current best selector. The best profile was again `selector_disciplined_put_and_call_legguard15_debits_only` with `1106` trades, `58395` PnL, `30745` $25-cost PnL, `2.78%` capital drawdown, and `5.33%` $25-cost capital drawdown. RIOT loaded `12` expirations and `4614` rows but contributed no accepted trades to the best profile. Treat RIOT as data-readiness only until more complete recent weekly coverage exists and it can show marginal accepted trades.

The next cache-wide expansion audit ranked all currently cached names by complete put+call coverage. Over an `80` sampled-expiration audit, only `SOFI` and `HOOD` had substantial complete coverage among lower-dollar additions: `SOFI` had `59` complete put+call expirations, `HOOD` had `44`, while `META` had `14`, `AMD` had `12`, `RKLB` had `9`, `RIOT` had `3`, `ASTS` had `2`, and `MARA`/`RIVN` had `0`.

Independent sleeve screens then tested whether the lower-dollar names have a standalone weekly debit edge, separated from the portfolio selector:

- Put-debit universe run `runs/universe-research-20260628T160220.233561000Z`: all seven symbols were blocked. SOFI had the best activity with `108` trades and `533` PnL across `60` loaded expirations, but had `0` walk-forward trades and `0` holdout trades. HOOD had `101` trades and `-850` PnL. AMD had `45` trades and `615` PnL but only `15` loaded expirations. RIOT and ASTS generated no accepted put-debit trades.
- Call-debit universe run `runs/universe-research-20260628T160220.223144000Z`: all seven symbols were also blocked. SOFI again ranked first with `84` trades and `591` PnL across `59` loaded expirations, but had `0` walk-forward and `0` holdout trades. HOOD had `63` trades and `148` PnL. AMD was negative at `-396` PnL, and RIOT/ASTS again generated no accepted call-debit trades.

This is an important professional check. The problem is not that the selector is ignoring a strong lower-dollar sleeve; the lower-dollar sleeves themselves do not yet show robust out-of-sample evidence. Continue warming SOFI/HOOD only if the goal is data completion, but do not allocate more research effort to RIOT/RKLB/ASTS/MARA/RIVN until they have enough complete recent weekly coverage to produce real walk-forward samples.

Another liquid lower-notional cohort was started with `GME`, `F`, `BAC`, and `CCL`. These names were chosen as established, liquid single-name option candidates rather than as validated strategy symbols. There was no local cache for any of them before the run. A bounded live-backed warmup with `--max-expirations 10` was stopped after it produced enough files for a first audit:

- `GME`: `719` expirations discovered; `10` audited; only `1` complete put+call expiration.
- `F`: `749` expirations discovered; `10` audited; `3` complete put+call expirations.
- `BAC`: `757` expirations discovered; `10` audited; `3` complete put+call expirations.
- `CCL`: `560` expirations discovered; `10` audited; `2` complete put+call expirations.

The cache-only diagnostic selector run `runs/portfolio-weekly-selector-research-20260628T160810.906950000Z` confirmed this cohort is not research-usable yet. It loaded only `1` GME, `5` F, `3` BAC, and `3` CCL expirations. The top profile generated only `15` trades versus `974` required and had `-179` PnL, `-329` $10-cost PnL, and negative PnL in every active sleeve: call-credit `-38`, put-debit `-26`, and call-debit `-115`. F was positive in the tiny sample, but GME lost `-326`; the sample is far too small to infer symbol quality.

Treat `GME/F/BAC/CCL` as another data-readiness branch, not an edge branch. If we continue warming this cohort, prioritize `F` and `BAC` because they had the best first-pass complete coverage. Do not spend strategy-optimization effort on the cohort until the audit shows enough complete recent weekly windows to support walk-forward and holdout tests.

A focused follow-up warmup did prioritize `F` and `BAC` with `--max-expirations 20`, then stopped after the cache had grown to `354` F files and `261` BAC files. The broader 20-expiration audit still showed too little complete coverage:

- `F`: `749` expirations discovered; `20` audited; `4` complete put expirations, `3` complete call expirations, and `3` complete put+call expirations.
- `BAC`: `757` expirations discovered; `20` audited; `3` complete put expirations, `2` complete call expirations, and `2` complete put+call expirations.

The follow-up cache-only diagnostic `runs/portfolio-weekly-selector-research-20260628T161237.248346000Z` loaded only `3` F expirations and `3` BAC expirations. The best profile generated `5` trades versus `980` required, with `-10` PnL and `-60` $10-cost PnL. The only active sleeve was call-credit, and it was negative. This confirms `F/BAC` are not just under-warmed; at current coverage they provide no useful evidence. Stop this branch until a much larger cache-completion pass can be run intentionally.

Pure wheel research was then isolated in `runs/portfolio-weekly-wheel-research-20260628T143445.465160000Z` to test the proposed "sell puts until assigned, then sell calls until called away" branch without debit/credit sleeve mixing. The result does not fit the current objective:

- The top pure wheel profile, `weekly_wheel_inventory_exit_dte3_10_callfloor95_credit01_hold21`, had only `460` trades versus `1091` required, so it fails the weekly cadence gate.
- It had `24843` PnL and `13343` $25-cost PnL, but `7.36%` capital drawdown and `9.46%` $25-cost capital drawdown.
- Assignment/inventory risk is high: `30.2%` assignment rate, `-25148` marked-stock PnL, worst marked-stock loss `-5191`, and marked-stock loss equal to `101.2%` of total PnL.

Do not pivot wholesale into the wheel branch for these symbols. The wheel can be kept as a research comparator, but it is too sparse for the target cadence and too inventory-risk-heavy for the canary objective. The current professional direction is to keep improving the debit-only branch, use symbol-level realized-loss throttles, and search for better symbols or entry filters that reduce TSLA-like clustered drawdowns without relying on stock assignment recovery.

## SOFI/HOOD 120-Expiration Follow-Up

A bounded live-backed SOFI/HOOD warmup was repeated with `--max-expirations 120` after the initial 80-expiration lower-dollar checks. The added cache improved file counts, but it did not repair the out-of-sample evidence problem.

The 120-expiration audit improved only modestly:

- `SOFI`: complete put+call expirations improved from `25` to `29` out of `120` audited windows.
- `HOOD`: complete put+call expirations improved from `21` to `23` out of `120` audited windows.

The 80-expiration audit remained materially stronger because it samples a different bounded window:

- `SOFI`: `59` complete put+call expirations, with complete call coverage from `2021-06-04` to `2025-02-28`.
- `HOOD`: `44` complete put+call expirations, with complete call coverage from `2021-08-13` to `2024-04-12`.

Independent sleeve reruns at `--max-expirations 120` still blocked both symbols:

- Put-debit run `runs/universe-research-20260628T161738.857559000Z`: `SOFI` produced `50` trades and `72` PnL; `HOOD` produced `42` trades and `-359` PnL. Both had `0` walk-forward trades and `0` holdout trades.
- Call-debit run `runs/universe-research-20260628T161738.868160000Z`: `SOFI` produced `34` trades and `180` PnL; `HOOD` produced `29` trades and `190` PnL. Both had `0` walk-forward trades and `0` holdout trades.

Professional conclusion: the added SOFI/HOOD cache is data-readiness progress, not strategy evidence. These names remain potentially useful lower-dollar candidates, but the current cache does not support tuning, canary deployment, or promotion. A larger intentional recent-cache completion pass is required before judging whether SOFI/HOOD can add robust weekly edge.

A targeted cache-completion command now exists for this branch:

```sh
cargo run --quiet -- warm-option-cache-coverage --symbols SOFI --from 2024-01-01 --to 2026-06-28 --max-expirations 80 --max-windows-per-symbol 2 --fetch-concurrency 1 --json
```

The first bounded SOFI batch completed both attempted windows, `2024-01-05` and `2024-01-12`, turning both put and call coverage complete for each. SOFI recent-window audit then improved from `15` to `17` complete put+call expirations out of `80`, with complete call coverage now starting at `2024-01-05` and still ending at `2025-01-31`.

Follow-up cache-only SOFI sleeve reruns still blocked:

- Put-debit run `runs/universe-research-20260628T163123.260608000Z`: `31` trades, `46` PnL, `19` expirations loaded, `9645` rows loaded, and `0` walk-forward / `0` holdout trades.
- Call-debit run `runs/universe-research-20260628T163123.251426000Z`: `20` trades, `56` PnL, `17` expirations loaded, `11938` rows loaded, and `0` walk-forward / `0` holdout trades.

Professional conclusion after the targeted warm proof: the new cache-completion path works, but two completed windows are not enough to change the strategy decision. Continue SOFI only as a controlled data-completion target; do not tune, canary, or promote until enough recent windows exist to produce actual walk-forward and holdout evidence.

Two additional bounded SOFI batches then completed another eight audited windows:

- Batch 2 completed `2024-02-02`, `2024-03-01`, `2024-03-22`, and `2024-04-12`.
- Batch 3 completed `2024-05-10`, `2024-05-31`, `2024-06-21`, and `2024-07-19`.

SOFI recent-window coverage improved to `25` complete put+call expirations out of `80`, with `27` complete put windows and `25` complete call windows. The next missing call windows start at `2024-08-09`, `2024-08-30`, `2024-09-27`, `2024-10-04`, and `2024-10-18`.

Follow-up cache-only SOFI sleeve reruns after these eight extra completions still blocked:

- Put-debit run `runs/universe-research-20260628T163748.226508000Z`: `38` trades, `173` PnL, `27` expirations loaded, `13453` rows loaded, and `0` walk-forward / `0` holdout trades.
- Call-debit run `runs/universe-research-20260628T163748.236883000Z`: `28` trades, `72` PnL, `25` expirations loaded, `17733` rows loaded, and `0` walk-forward / `0` holdout trades.

Professional conclusion after twelve completed SOFI windows: the controlled cache-completion path is effective, and SOFI's raw sample is improving, but the evidence is still not strategy-grade. Continue cache completion before any profile changes. Do not promote, canary, or tune entries until SOFI produces nonzero walk-forward and holdout samples.

Full-history reruns after the SOFI cache-completion batches clarify the failure mode. The short 2024-2026 checks are useful for recent coverage, but they are not sufficient OOS proof because the research engine requires a multi-year training window. The relevant full-history runs are:

- Put-debit run `runs/universe-research-20260628T163942.849412000Z`: `108` trades, `533` PnL, `61` expirations loaded, `30983` rows loaded, `0` walk-forward / `0` holdout trades, and `0` fixed-profile OOS passes.
- Call-debit run `runs/universe-research-20260628T163942.839022000Z`: `84` trades, `591` PnL, `59` expirations loaded, `42907` rows loaded, `0` walk-forward / `0` holdout trades, and `0` fixed-profile OOS passes.

The reason is not merely missing recent rows. The full-history reports require roughly `531` trades for the effective SOFI window, but the best put-debit profile has only `108` trades and the best call-debit profile has only `84`. That is roughly `16` to `21` trades per year, far below the weekly cadence objective. Both also fail friction stress: the put-debit best profile falls to `-547` PnL at $10/trade extra cost and `-2167` at $25/trade; the call-debit best profile falls to `-249` and `-1509`.

Professional conclusion: SOFI is not just under-warmed; under the current debit-spread families it is structurally too sparse and too friction-sensitive for the weekly objective. More cache completion may still be useful to finish the data-readiness audit, but the next strategy work should not tune SOFI entries unless a different, higher-cadence entry family or portfolio-level role is being tested explicitly.

Short-premium follow-ups tested whether selling weekly put credit spreads fixes the SOFI/HOOD sparse-signal problem. It does not.

- Weekly put-credit run `runs/universe-research-20260628T164208.768746000Z`: `SOFI` produced `39` trades, `-64` PnL, `61` loaded expirations, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes. `HOOD` produced `53` trades, `-20` PnL, `48` loaded expirations, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes.
- Far-OTM weekly put-credit run `runs/universe-research-20260628T164208.778981000Z`: `SOFI` produced only `7` trades and `23` PnL; `HOOD` produced only `7` trades and `-15` PnL. Both remained OOS-blocked with `0` walk-forward and `0` holdout trades.

Professional conclusion: selling premium is not a free solution here. The core weekly put-credit profile is negative before costs, and the far-OTM profile avoids losses mostly by barely trading. The small positive SOFI far-OTM sample is not meaningful because it has only `7` trades and no OOS support. For SOFI/HOOD, short-premium variants are currently worse than the debit sleeves on both cadence and robustness. Do not pivot these names into put-credit or far-OTM credit canaries; only test a new short-premium family if the hypothesis explicitly changes to higher-cadence, lower-filter, inventory-aware research with fresh OOS gates.

## Weekly Signal Gate Diagnostics

A new `audit-weekly-signal-gates` command now separates data availability, pre-execution gate attrition, final spread candidates, and simulated trades. This is a diagnostic only; it does not create promotion evidence.

The first diagnostic pass explains why the sparse-signal problem is not one single issue:

- `SOFI` weekly put-credit, bounded to `80` sampled expirations: `61` expirations loaded, `44470` rows, `7813` DTE-eligible rows over `347` entry days, only `575` primary legs passed delta/OI/quote/IV gates, `331` passed regime gates, and the best high-cadence profile had only `150` spread candidates over `40` candidate days. It simulated `39` trades and lost `-64`.
- `SOFI` weekly put-debit, same bounded sample: `61` expirations loaded, `30983` rows, `10272` DTE-eligible rows over `461` entry days, `715` primary legs passed, `419` passed regime gates, and the best high-cadence profile had `2062` candidates over `222` candidate days. It simulated `108` trades and made `533`, still far below the roughly `531`-trade standalone weekly gate and still friction-negative under the prior cost-stress report.
- `PLTR` weekly put-debit, uncapped cache-only full history: `298` expirations loaded, `127850` rows, and high candidate volume. The top high-cadence rows reached roughly `443` to `456` trades. Wide variants can be profitable (`weekly_put_debit_dte3_10_w15_delta25_55_take25` showed `5321` PnL), but the most active narrow variants were negative or weak. `PLTR` is therefore not sparse like `SOFI`; it needs side/width/regime discipline and OOS checks.
- `PLTR` weekly call-debit, uncapped cache-only full history: `298` expirations loaded, `125972` rows, and roughly `336` to `347` trades among top high-cadence rows. Several variants were positive, including `weekly_call_debit_dte3_10_w25_delta30_60_take25` at `2689` PnL. This supports keeping PLTR in the original-symbol debit selector, but not standalone promotion.
- `TSLA` weekly put-debit, uncapped cache-only full history: `541` expirations loaded, `613357` rows, `285187` DTE-eligible rows over `2402` entry days, and many high-cadence profiles around `826` to `849` trades. The top high-cadence rows were strongly negative, from roughly `-18321` to `-29180` PnL. `TSLA` is not sparse; it is adverse-selection and drawdown dominated on put-debit entries.
- `ORCL` weekly call-debit, uncapped cache-only full history: `491` expirations loaded, `188795` rows, and high-cadence rows around `371` to `384` trades. Several wider call-debit variants were positive, including `weekly_call_debit_dte1_7_w25_delta30_60_take25` at `8983` PnL and `weekly_call_debit_dte3_10_w15_delta30_60_take25` at `7731` PnL.
- `ORCL` weekly put-debit, uncapped cache-only full history: `491` expirations loaded, `175206` rows, and many high-cadence rows around `837` to `863` trades, but the top high-cadence rows were negative. The least-bad wide variants were still negative, around `-1373` to `-1805` PnL.

Professional conclusion: the next round should be symbol- and side-selective. Do not loosen generic gates just to increase trade count, and do not add short-premium filler. `SOFI` needs either more data completion or a genuinely different higher-cadence hypothesis. `TSLA` put-debit needs stricter avoidance or regime-specific sizing because the problem is negative expectancy, not sparse entries. `ORCL` looks materially better on call-debit than put-debit. `PLTR` has enough flow to stay in the selector but needs width/cost/regime discipline before standalone action.

Follow-up side-selective selector profiles were added to test this directly. They allow sleeve-level symbol filters so a put-debit sleeve can apply only to symbols where the audit supports it, while a call-debit sleeve can exclude symbols with adverse evidence. Current-code run `runs/portfolio-weekly-selector-research-20260628T170737.699056000Z` used the original basket and the same `20%` symbol cap, `4` max open positions, `2` positions per symbol, and `10%`/`20` day symbol drawdown cooldown.

- `selector_side_selective_pltr_put_plus_non_tsla_call_debits_only` became the top scored profile, with `1143` trades, `42516` PnL, `2.04` profit factor, `2.16%` capital drawdown, and no wheel inventory. It uses PLTR-only put debits plus IREN/PLTR/ORCL/CRWV call debits, excluding TSLA entirely.
- The profile is still blocked: annual stability failed by one material negative year, `2020` at `-1099`, just beyond the `-1000` threshold. It also has weaker friction stress than the current research-pass baseline: `$25` cost-stressed PnL was only `13941` and `$25` capital drawdown rose to `9.64%`.
- The current research-pass baseline, `selector_disciplined_put_and_call_legguard15_debits_only`, remains economically stronger after costs: `1106` trades, `58395` PnL, `30745` $25-cost PnL, `2.78%` capital drawdown, and `5.33%` $25-cost capital drawdown. It is still not canary-ready because canary drawdown is above `1%`, TSLA is the largest PnL contributor, and ablations do not pass.
- Narrower side-selective profiles did not pass. PLTR/ORCL-only call debit had only `811` trades and `-5752` $25-cost PnL. PLTR put plus PLTR/ORCL call had `1030` trades versus `1091` required and only `7145` $25-cost PnL.

Professional conclusion after the side-selective experiment: the hypothesis is directionally useful, but not sufficient. It reduces TSLA dependence and improves raw capital drawdown, but the edge is too friction-sensitive and still depends heavily on PLTR/ORCL. Keep the side-selective profile as a research challenger, not as a canary. The next productive refinement is not short-premium or wheel; it is cost-aware side-selective call-debit design that preserves cadence while fixing the `2020` loss and $25-cost stress.

Follow-up run `runs/portfolio-weekly-selector-research-20260628T171257.873878000Z` tested the same side-selective profile with wider take-profit exits. `take33` and `take50` did not solve the blocker: both remained blocked on the same `2020` annual-stability issue, with lower PnL and weaker $25-cost stress than `take25`. The `2020` attribution was concentrated in ORCL call debits, not PLTR put debits or the exit target. Do not use a calendar cutoff to remove `2020`; that would overfit a known bad year.

Follow-up runs `runs/portfolio-weekly-selector-research-20260628T172153.471622000Z` and `runs/portfolio-weekly-selector-research-20260628T172705.700541000Z` added ORCL-specific cost-aware call-debit fallbacks while keeping PLTR-only put debits and wide calls for the other allowed call symbols. This fixed the annual-stability problem but gave up too much cadence:

- `selector_side_selective_pltr_put_plus_orcl_costaware_minw1_non_tsla_call_debits_only`: `1069` trades versus `1091` required, `41684` PnL, `2.17` profit factor, `2.16%` capital drawdown, `$14959` $25-cost PnL, and worst year `2020` improved to `-686`.
- `selector_side_selective_pltr_put_plus_orcl_costaware_minw3_non_tsla_call_debits_only`: `1050` trades versus `1091` required, `41676` PnL, `2.18` profit factor, `2.16%` capital drawdown, `$15426` $25-cost PnL, and worst year `2020` improved to `-569`.

Adding TSLA wide call-debit participation restored cadence but failed the professional robustness test. `selector_side_selective_pltr_put_plus_orcl_costaware_minw1_plus_tsla_call_debits_only` reached `1141` trades, but PnL fell to `33699`, capital drawdown rose to `4.46%`, $25-cost PnL fell to `5174`, $25-cost capital drawdown rose to `18.12%`, first-half PnL was `-1545`, and TSLA contributed `-7861`. The `minw3` TSLA variant was similar: `1122` trades, `33647` PnL, `4.40%` capital drawdown, `$5597` $25-cost PnL, and `17.70%` $25-cost capital drawdown.

Professional conclusion after ORCL fallback and TSLA-filler tests: ORCL cost-aware calls are a better risk fix than exit-target tuning, but they need a small independent cadence source. TSLA should not be used as the filler under the current wide-call profile because it restores trade count by adding negative expectancy and first-half fragility. The current research-pass baseline remains `selector_disciplined_put_and_call_legguard15_debits_only`; the closest cleaner challenger is the ORCL-cost-aware non-TSLA side-selective profile, but it is still cadence-blocked.

Follow-up run `runs/portfolio-weekly-selector-research-20260628T174015.699841000Z` tested a round-number ORCL-only `mindebit35_minw1` interpolation between the clean-but-cadence-short `mindebit40` and the cadence-passing-but-fragile `mindebit30`. This produced the best current non-TSLA side-selective research-pass profile:

- `selector_side_selective_pltr_put_plus_orcl_costaware_mindebit35_minw1_non_tsla_call_debits_only`: `1112` trades versus `1091` required, `40274` PnL, `2.12` profit factor, `2.16%` capital drawdown, `$12474` $25-cost PnL, `10.85%` $25-cost capital drawdown, and worst year `2020` at `-799`.
- The profile uses no wheel inventory and no TSLA trades. Symbol attribution was PLTR `580` trades / `25069` PnL, ORCL `387` / `11271`, IREN `89` / `2146`, and CRWV `56` / `1788`.
- It is not canary-ready: canary readiness remains blocked, PLTR is `62.2%` of PnL, $25-cost drawdown is high, and no symbol or strategy ablation passes.
- The adjacent profiles confirm the threshold tradeoff: `mindebit40_minw1` stayed cleaner but missed cadence by `2` trades; `mindebit30_minw1` cleared cadence but failed chronological robustness with first-half PnL `-170`.

Professional conclusion after the `mindebit35` interpolation: keep this as the current top non-TSLA research-pass challenger, but do not promote it. The next research step should improve diversification and friction robustness, not loosen ORCL further. A credible next test is adding a small independent non-TSLA cadence source with positive first-half behavior or reducing PLTR concentration while preserving the `mindebit35` ORCL risk fix.

Follow-up concentration-control run `runs/portfolio-weekly-selector-research-20260628T174502.205554000Z` tested the existing allocator `--max-total-trades-per-symbol 500` cap. This did not improve the side-selective challenger. The `mindebit35` profile fell to `1053` trades versus `1091` required, PnL dropped to `27209`, $25-cost PnL fell to only `884`, and capital drawdown rose to `3.62%`. A blunt symbol-count cap removes profitable PLTR entries faster than the allocator can replace them with independent edge.

Follow-up run `runs/portfolio-weekly-selector-research-20260628T174855.969352000Z` tested replacing the non-ORCL wide-call filler with the existing legguard call profile while preserving ORCL `mindebit35`:

- `selector_side_selective_pltr_put_plus_orcl_costaware_mindebit35_legguard_non_tsla_call_debits_only`: reduced concentration below the canary concentration threshold (`46.6%` max symbol PnL share from ORCL), but failed cadence with `1040` trades versus `1091`, had only `23012` PnL, and became $25-cost negative at `-2988`.
- `selector_side_selective_pltr_put_plus_orcl_costaware_mindebit35_legguard_plus_tsla_call_debits_only`: restored cadence with `1166` trades, but failed chronological robustness; first-half PnL was `-2285`, $25-cost capital drawdown was `17.21%`, and TSLA added only `696` PnL across `149` trades.

Professional conclusion after the concentration controls: neither a blunt symbol-count cap nor legguard call replacement is a viable diversification fix. The wide-call non-TSLA `mindebit35` challenger remains the best non-TSLA research-pass profile, but its PLTR concentration and friction sensitivity are structural. The next diversification work should look outside the current five-symbol basket or require a genuinely independent sleeve with positive first-half and cost-stressed evidence.

Follow-up expansion run `runs/portfolio-weekly-selector-research-20260628T175556.347945000Z` added SOFI after a fresh cache and signal audit showed it was the only locally cached lower-dollar addition with meaningful current put+call coverage and positive debit-flow evidence. The 80-expiration cache audit showed SOFI at `59/80` complete put+call expirations, versus HOOD at `44/80` with weaker signal. Current gate audits showed SOFI put-debit evidence was materially better than HOOD: the exact PLTR-wide put profile had `87` SOFI trades / `1135` PnL, while HOOD had `81` trades / `-136` PnL. SOFI call-debit evidence was smaller but positive.

The SOFI-enabled side-selective selector became the best current non-TSLA research-pass challenger:

- `selector_side_selective_pltr_sofi_put_plus_orcl_costaware_mindebit35_non_tsla_sofi_call_debits_only`: `1178` trades, `43839` PnL, `2.26` profit factor, `2.16%` capital drawdown, `$14389` $25-cost PnL, `9.97%` $25-cost capital drawdown, and worst year `2020` at `-799`.
- SOFI contributed `111` trades and `2059` PnL: `58` put-debit trades and `53` call-debit trades. Removing SOFI reverts to the prior `mindebit35` profile, which still passes, so SOFI is helpful but not structurally required.
- Symbol attribution improved but remains concentrated: PLTR `568` trades / `25054` PnL, ORCL `359` / `12670`, SOFI `111` / `2059`, IREN `84` / `2268`, CRWV `56` / `1788`. PLTR is still `57.2%` of PnL.
- Robustness improved versus the prior non-SOFI side-selective profile: symbol ablation passes improved from `0` to `3`, but strategy ablations still fail and canary readiness remains blocked by `2.16%` capital drawdown and PLTR concentration.

Professional conclusion after SOFI expansion: SOFI is a valid marginal diversifier and should stay in the research basket. It does not solve canary readiness. HOOD should not be added on current evidence because its put-debit profile is negative and call-debit evidence is too small. The next expansion should either warm/test another lower-dollar name until it has SOFI-like coverage, or investigate whether a smaller-contract/lower-risk proxy can reduce capital drawdown without reintroducing TSLA dependence.

## SOFI Allocation Control Recheck

Follow-up allocation controls retested whether SOFI's extra cadence made stricter heat limits viable for the current best non-TSLA side-selective profile.

- `runs/portfolio-weekly-selector-research-20260628T180708.061725000Z` lowered max open positions from `4` to `3`. The target profile improved risk quality but failed cadence: `988` trades versus `1091` required, `46382` PnL, `2.78` profit factor, `2.05%` capital drawdown, `$21682` $25-cost PnL, and `5.50%` $25-cost capital drawdown. This is not usable for the weekly objective.
- `runs/portfolio-weekly-selector-research-20260628T181003.875449000Z` kept `4` max open positions but lowered max symbol allocation from `20%` to `10%`. This is the best current allocator setting for the SOFI-expanded side-selective branch: `1146` trades, `43713` PnL, `2.35` profit factor, `1.23%` capital drawdown, `$15063` $25-cost PnL, and `9.09%` $25-cost capital drawdown. Annual stability remained acceptable, with `2020` at `-735`.
- The `10%` cap improved the canary blocker from `2.16%` to `1.23%` capital drawdown while preserving cadence, but it still missed the `1%` canary drawdown line and PLTR remained concentrated at `58.6%` of PnL.
- `runs/portfolio-weekly-selector-research-20260628T181401.132769000Z` tested an adjacent `8%` symbol cap. The target profile failed cadence by only four trades, `1087` versus `1091`, and did not improve capital drawdown below `1.23%`. It is therefore a worse control point than `10%`.
- Under the `10%` cap, removing `CRWV` or `IREN` still passed, while removing `ORCL`, `PLTR`, or `SOFI` failed cadence. Removing either debit sleeve also failed cadence and cost stress, so strategy ablations remain the hard robustness gap.

Professional conclusion after the SOFI allocation recheck: use the `10%` symbol cap as the current best research setting for this side-selective branch. Do not keep tightening allocator knobs as the primary path; the remaining gap needs another independent positive sleeve or symbol, because allocation controls now trade off cadence faster than they reduce canary drawdown.

## SOFI Cache Completion Recheck

SOFI is the only lower-dollar addition that has remained marginally useful inside the best side-selective portfolio branch, so it was retested after a fresh recent-window cache completion. A bounded cache-completion batch filled all `8` attempted put+call windows with no failures: `2025-03-21`, `2025-04-17`, `2025-05-09`, `2025-05-30`, `2025-06-27`, `2025-07-18`, `2025-08-08`, and `2025-09-05`.

The follow-up `80`-expiration audit improved SOFI complete put+call coverage from `59/80` to `67/80`. Put and call coverage are now both `67/80`, with complete call coverage from `2021-06-04` through `2025-09-05`.

Standalone sleeve retests were still not strategy-grade:

- Put-debit run `runs/universe-research-20260628T190603.625445000Z`: `131` trades, `161` PnL, `67` loaded expirations, `34626` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes. The prior broad lower-dollar put-debit screen had `108` trades and `533` PnL, so adding recent windows diluted the standalone put-debit edge.
- Call-debit run `runs/universe-research-20260628T190603.637538000Z`: `102` trades, `627` PnL, `67` loaded expirations, `52328` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes. This is the only SOFI sleeve that improved, but it remains far below standalone weekly cadence requirements and has no OOS support.
- Standard weekly put-credit run `runs/universe-research-20260628T190603.638146000Z`: `53` trades, `-76` PnL, `67` loaded expirations, `49947` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes.
- Far-OTM weekly put-credit run `runs/universe-research-20260628T190603.637575000Z`: `10` trades, `28` PnL, `67` loaded expirations, `34626` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes. The positive result is too small to matter.

The updated six-symbol portfolio rerun, `runs/portfolio-weekly-selector-research-20260628T190705.613684000Z`, kept the same `10%` symbol cap and confirmed that SOFI remains useful only as a marginal portfolio sleeve:

- Best profile `selector_side_selective_pltr_sofi_put_plus_orcl_costaware_mindebit35_non_tsla_sofi_call_debits_only` still research-passed with `1149` trades, `43481` PnL, `2.37` profit factor, `1.23%` capital drawdown, and `14756` $25-cost PnL.
- SOFI contribution improved slightly to `126` trades and `2237` PnL, split across `64` put-debit trades and `62` call-debit trades.
- Canary readiness stayed blocked: `$25` cost capital drawdown remained `9.09%`, capital drawdown remained `1.23%`, PLTR still supplied the largest PnL share at `57.9%`, and strategy ablation passes remained `0`.

Professional conclusion after SOFI cache completion: keep SOFI in the current research basket for portfolio cadence, but do not promote SOFI as a standalone symbol and do not pivot it to short premium or wheel. The current best branch remains a research-pass, canary-blocked side-selective debit portfolio; the next improvement needs a genuinely independent positive sleeve or a lower-drawdown structure, not more SOFI tuning.

## Side-Selective Risk-Control Recheck

After the SOFI-expanded side-selective branch remained blocked by `1.23%` capital drawdown, high $25-cost drawdown, PLTR concentration, and zero strategy-ablation passes, a bounded risk-control sweep tested whether the remaining gap was mainly position overlap or clustered losses rather than missing symbol diversification.

Three cooldown controls reused the current six-symbol basket, `10%` symbol cap, `4` max open positions, and `2` max positions per symbol:

- Portfolio drawdown cooldown only, `0.8%` for `20` days, run `runs/portfolio-weekly-selector-research-20260628T201256.247558000Z`: the target side-selective profile fell to `803` trades versus `1091` required, `33134` PnL, `1.33%` capital drawdown, and `13059` $25-cost PnL.
- Stronger symbol drawdown cooldown plus portfolio cooldown, `5%` symbol-cap trigger and `0.8%` portfolio trigger for `20` days, run `runs/portfolio-weekly-selector-research-20260628T201256.261379000Z`: the target profile had `874` trades, `30337` PnL, `1.28%` capital drawdown, and `8487` $25-cost PnL.
- Stronger symbol drawdown cooldown only, `5%` of symbol cap for `20` days, run `runs/portfolio-weekly-selector-research-20260628T201256.261398000Z`: the target profile had `1012` trades, `33868` PnL, `1.35%` capital drawdown, and `8568` $25-cost PnL.

A separate heat-control run lowered max open positions from `4` to `3`, `runs/portfolio-weekly-selector-research-20260628T201713.923941000Z`. The target side-selective profile improved $25-cost capital drawdown from `9.09%` to `4.90%`, but failed the weekly cadence gate with only `971` trades and worsened raw capital drawdown to `1.67%`. The top-ranked profile in that run was also blocked because it depended on a negative put-credit sleeve.

Professional conclusion after the risk-control recheck: do not keep tightening portfolio cooldowns or open-position caps as the primary route. They reduce activity faster than they reduce canary-relevant capital drawdown, and they do not create strategy-ablation robustness. The current blocker is still lack of an independent positive sleeve or symbol that can replace PLTR/ORCL cadence without adding negative short-premium or wheel inventory risk.

## CRWV Seasoning Recheck

The next bounded regime/loss-shape check tested whether the newest and shortest-history symbol, `CRWV`, should be excluded under the "sufficient long history" principle instead of treated as a permanent weekly basket member. The direct no-CRWV selector run `runs/portfolio-weekly-selector-research-20260628T211548.898965000Z` used `IREN,PLTR,ORCL,SOFI` with the same `20%` symbol cap, `4` max open positions, `2` positions per symbol, and `10%`/`20` day symbol drawdown cooldown.

This improved the research profile materially:

- Best profile `selector_side_selective_pltr_sofi_put_plus_orcl_costaware_mindebit35_non_tsla_sofi_call_debits_only` stayed `research_pass`.
- Trades: `1148` versus `990` required, so weekly cadence still clears.
- PnL: `38759`, profit factor `2.26`, and `$25` cost PnL `10059`.
- Capital drawdown dropped to `1.29%`, from `2.16%` in the comparable six-symbol risk-control run.
- The best lower-drawdown ORCL-strict variant reached `1.21%` capital drawdown with `1016` trades, but remained above the `1%` canary budget.
- PLTR still carried `63.3%` of PnL, strategy ablations remained `0`, and `$25` cost capital drawdown remained high at `9.97%`.

Professional conclusion after the CRWV seasoning recheck: exclude CRWV from the current top research challenger. CRWV adds short-history drawdown without solving concentration or ablation robustness. The no-CRWV branch is the cleaner research baseline, but it is still not canary-ready: the remaining blocker is PLTR dependence plus friction-stressed drawdown, not CRWV alone.

## No-CRWV Heat Frontier

Two bounded heat-control runs tested whether the no-CRWV branch could cross the canary drawdown budget by reducing overlap rather than changing symbols or strategy families:

- Max-open `3`, run `runs/portfolio-weekly-selector-research-20260628T211847.489213000Z`: the target side-selective profile improved to `49439` PnL, `3.20` profit factor, `$25` cost PnL `25339`, and `$25` cost capital drawdown `5.50%`, but failed cadence with `964` trades versus `990` required. Raw capital drawdown stayed `1.29%`, so it did not solve the canary budget either.
- Max `1` position per symbol, run `runs/portfolio-weekly-selector-research-20260628T211847.476423000Z`: the target profile crossed the raw drawdown threshold at `0.85%`, but cadence collapsed to `691` trades versus `990` required and `$25` cost capital drawdown stayed high at `5.71%`.

Professional conclusion after the no-CRWV heat frontier: overlap is part of the friction-stressed drawdown problem, but simply tightening heat controls is not sufficient. Max-open `3` is the most interesting frontier because it preserves most of the edge and materially improves $25 stress, but it misses the weekly cadence gate. Max-one-position-per-symbol proves drawdown can be reduced, but only by deleting too much activity. The next improvement needs a small independent, long-history sleeve that fills the max-open-3 cadence gap without reintroducing CRWV/TSLA-style drawdown.

## Alternative Strategy Decision

The same-symbol wheel question is now closed for the current research cycle. The direct pure-wheel reruns and the short-premium sleeve reruns answer the professional question differently than the headline gross PnL might suggest:

- Pure wheel can produce gross PnL, but it does so by warehousing stock inventory. The best strict SOFI-expanded wheel profile had only `351` trades versus `1091` required, `1.87%` capital drawdown, `6448` $25-cost PnL, `28.5%` assignment rate, and `inventory_risk_high`.
- Standard weekly put-credit, far-OTM put-credit, and call-credit rechecks were independently negative or too sparse on the better-covered expansion symbols. The 120-expiration AMD/HOOD follow-up closed the remaining "sell weekly premium" branch for the current cache.
- The no-CRWV max-open `3` debit frontier is a better risk frontier than wheel: the target profile had `964` trades versus `990` required, `49439` PnL, `3.20` profit factor, `25339` $25-cost PnL, and `5.50%` $25-cost capital drawdown. It is still blocked by cadence and raw capital drawdown, but it improves the right risk surface without taking assignment risk.

Professional interpretation: the negative-PnL problem is not primarily caused by choosing long premium instead of short premium. It is caused by marginal weekly trades whose expected edge does not survive conservative fills, clustered adverse regimes, and cost stress. Selling puts until assignment and then selling calls changes the accounting path, not the underlying edge. It can manufacture more actions, but the risk moves into stock inventory, gap exposure, capital lockup, and covered-call upside truncation.

Do not pivot the active basket wholesale into a plain wheel. Keep wheel-like inventory only as a comparator or a tightly capped sleeve. The next alternative structure should be a defined-risk same-expiration family before cross-expiration structures:

- First new-structure candidate: weekly iron condor / double credit spread, using the already loaded same-expiration put and call rows, 1-14 DTE, short legs around `10-30` delta, `$5-$25` wings where liquidity allows, conservative bid/ask entry, one-third credit exit, and explicit stop/force-close rules.
- Why this comes before calendars/diagonals: iron condors can reuse current put+call cache and same-expiration simulation machinery. Calendars and diagonals require cross-expiration pairing, different exit pricing, and broader loader/report changes.
- Gate expectation: an iron-condor branch should be rejected unless it is independently positive after $25/trade stress, clears weekly cadence, does not rely on one symbol for most PnL, and improves the no-CRWV max-open `3` drawdown/cadence frontier. It must not be used as cadence filler if either side is negative.

Current next research target: implement or prototype the same-expiration iron-condor family, then test it on the no-CRWV baseline symbols `IREN,PLTR,ORCL,SOFI` and only afterwards consider adding back TSLA/CRWV as stress comparators.

Prototype result: a cache-only same-expiration iron-condor probe was run outside the promotion engine on `IREN,PLTR,ORCL,SOFI`. The prototype used conservative bid/ask entry and close, 1-14 DTE, one open trade per symbol, one-third credit take-profit, 2x credit stop, 7-day max hold, and force close at 1 DTE. It is diagnostic only because it is not yet a first-class Rust strategy family, but the result was too weak to justify implementation as the next edge branch:

- Baseline `10-30` delta, `8%` minimum credit/width: `871` trades, `-20617` gross PnL, `-42392` $25-cost PnL, `0.26` profit factor, and roughly `20.6%` closed-equity drawdown on a `$100000` reference capital budget.
- Small grid over `5-15`, `5-20`, `10-20`, `10-30`, and `20-35` delta bands with `3%`, `5%`, `8%`, and `12%` minimum credit/width was negative in every row. The least-bad gross row was still `-8961` PnL before costs, and after $25/trade stress it was `-19636`.
- ORCL was consistently the largest loss source in the iron-condor probe, despite being one of the better call-debit symbols. This reinforces that combining a weak short-call/short-put volatility sale with a good directional debit sleeve can destroy edge rather than diversify it.

Updated decision: do not spend Rust implementation time on generic weekly iron condors for this basket. A future iron-condor branch would need a materially different regime hypothesis, such as event-volatility crush, realized-vs-implied-vol dislocation, or index-like underlyings, and must be screened as an independent positive sleeve before being allowed into a selector.

Long-volatility follow-up: a second cache-only prototype tested same-expiration weekly long strangle/straddle-style entries on the same no-CRWV basket. It bought one put and one call, used conservative ask entry and bid exit, 1-14 DTE, one open trade per symbol, 33-100% profit targets, 33-50% stop-loss variants, and a 7-day max hold. This branch also failed as a generic next structure:

- Every tested delta/exit combination was gross-negative before costs.
- The least-bad gross result was the high-delta `40-65` band with a 33% take-profit and 50% stop: `785` trades, `-7061` gross PnL, and `-26686` $25-cost PnL.
- ORCL was positive in the high-delta long-vol variants, but IREN, PLTR, and SOFI were negative enough that the basket failed. This is a possible symbol-specific research hint, not a portfolio candidate.

Updated structure ranking for this basket: generic wheel, short-credit spreads, generic iron condors, and generic long strangles are all rejected as immediate pivots. The active research baseline remains the no-CRWV side-selective debit selector plus max-open `3` frontier. The next credible work is either a symbol-specific ORCL long-vol/event-vol hypothesis, a truly new well-covered symbol, or a first-class regime feature that changes entry selection materially rather than repackaging the same weekly exposure.

## ORCL Long-Vol Regime Probe

The one useful hint from the failed basket-level long-vol prototype was that ORCL high-delta long-vol rows were gross-positive while the rest of the basket was negative. A focused ORCL-only cache prototype retested that hint with simple, pre-declared regime filters: 20/60-day return, 20-day drawdown, and 20-day realized volatility. It used the same conservative ask-entry/bid-exit convention, 1-14 DTE, one open trade at a time, and 7-day max hold.

The best row was positive after cost, but not robust enough to implement as a first-class strategy family:

- Best row: `45-70` delta long strangle, 50% take-profit, 50% stop, high-realized-vol filter (`20d RV >= 30%`), `87` trades, `6256` gross PnL, `4081` $25-cost PnL, `1.45` profit factor, and `3467` closed-equity drawdown.
- Chronological robustness failed: first half lost `-382`, second half made `6638`.
- Annual stability failed: `5` negative years, with yearly PnL `{2018: -12, 2020: -430, 2021: -163, 2022: -315, 2023: 178, 2024: -819, 2025: 6402, 2026: 1415}`.
- Removing `2025` removes essentially all of the edge: total gross PnL falls from `6256` to roughly `-146`, before cost stress. That is outlier-year dependence, not a reliable signal generator.

Professional conclusion: do not implement ORCL generic high-RV long-vol as the next production strategy. It is a research hint only. A future ORCL volatility branch would need a stronger economic hypothesis, such as known event windows or an explicit implied-vs-realized dislocation feature, and it must pass chronological and annual-stability gates before being mixed into the selector.

IV/RV follow-up: an explicit implied-vs-realized volatility filter improved the ORCL long-vol hint but did not make it strategy-grade. The best diagnostic row was `45-70` delta long strangle, 33% take-profit, 50% stop, `20d RV >= 30%`, and average entry IV at least `1.25x` 20-day realized volatility:

- `34` trades, `7590` gross PnL, `6740` $25-cost PnL, and `2.19` profit factor.
- Year-level PnL after $25/trade: `2018 -78`, `2020 124`, `2022 -402`, `2023 559`, `2024 -754`, `2025 6205`, `2026 1086`.
- Excluding `2025`, the row still has `22` trades, `1085` gross PnL, and `535` $25-cost PnL, so it is not purely a single-year artifact.
- It is still too sparse for the weekly objective and too unstable for promotion: only `34` trades across the loaded window and `3` negative active years.

Updated decision: do not implement this as a standalone weekly strategy yet. It is a better-defined research hint than generic high-RV long-vol, and it might be useful later as a tiny ORCL-specific volatility sleeve, but only after a first-class implementation can prove incremental portfolio contribution, non-overlap with ORCL call-debit exposure, and annual stability. It is not the missing frequent weekly engine.

Incremental portfolio probe: the same ORCL IV/RV long-vol row was merged into the no-CRWV max-open `3` selector frontier as a diagnostic overlay. Existing selector trades kept priority, and ORCL IV/RV trades were accepted only when they did not breach `3` total open positions or `2` open ORCL positions.

- Baseline max-open `3` selector: `964` trades, `49439` gross PnL, `25339` $25-cost PnL, `1.29%` raw capital drawdown, and `5.50%` $25-cost capital drawdown.
- Standalone ORCL IV/RV diagnostic row: `34` trades, `7590` gross PnL, and `6740` $25-cost PnL.
- Merged diagnostic overlay accepted `27` of the `34` ORCL IV/RV trades. The accepted overlay added `8621` gross PnL and `7946` $25-cost PnL; the `7` rejected overlap trades would have lost `-1031` gross and `-1206` after costs.
- Merged profile: `991` trades, `58060` gross PnL, `33285` $25-cost PnL, `1.46%` raw capital drawdown, and `5.81%` $25-cost capital drawdown.

Professional interpretation: this is the first fair alternative-structure probe that fills the no-CRWV max-open `3` cadence gap (`991` versus `990` required) and improves PnL after costs. It still is not canary-ready because both raw and $25-cost capital drawdown worsen, active negative cost-stressed years remain `7`, and ORCL concentration rises materially. Treat ORCL IV/RV long-vol as a research challenger worth first-class implementation only if it can add a non-anticipating risk gate; do not promote it as a standalone edge.

The diagnostic is now reproducible:

```sh
python3 scripts/ivrv_overlay.py --symbol ORCL
```

Use `--json` when downstream analysis needs exact accepted/rejected overlay trades.

Drawdown-aware overlay caveat: `scripts/ivrv_overlay.py` supports explicit overlay acceptance policies, but the `no-worse-*` policies use completed candidate PnL to decide whether a trade would have been accepted. They are diagnostic upper bounds, not deployable live allocation rules. The strict no-worse-drawdown policy improved risk but failed cadence:

```sh
python3 scripts/ivrv_overlay.py --symbol ORCL --policy no-worse-any-dd
```

- Result: `987` trades, `59716` gross PnL, `35041` $25-cost PnL, `1.22%` raw capital drawdown, and `5.47%` $25-cost capital drawdown.
- This is economically attractive and improves both drawdown measures versus the slot-only overlay, but it still misses the `990`-trade no-CRWV cadence gate.

The smallest tested allowance that restored cadence was `0.15%` of capital for both raw and cost-stressed drawdown:

```sh
python3 scripts/ivrv_overlay.py --symbol ORCL --policy no-worse-any-dd --raw-dd-allowance-pct 0.0015 --cost-dd-allowance-pct 0.0015
```

- Result: `990` trades, `58301` gross PnL, `33551` $25-cost PnL, `1.41%` raw capital drawdown, and `5.55%` $25-cost capital drawdown.
- Accepted overlay contribution: `26` ORCL IV/RV trades, `8862` gross PnL, and `8212` $25-cost PnL.
- Versus the baseline max-open `3` selector, this clears cadence and adds `8212` $25-cost PnL, but raw capital drawdown still worsens from `1.29%` to `1.41%` and $25-cost capital drawdown slightly worsens from `5.50%` to `5.55%`.

Updated challenger setting: the fair non-anticipating ORCL evidence is the slot-only overlay, not the drawdown-aware allowance overlay. The drawdown-aware rows are useful only as an upper bound showing that some rejected ORCL trades are avoidable in hindsight. A real implementation must replace that oracle with a pre-entry rule based only on information available at entry time.

A first non-anticipating replacement was tested with `pre-entry-no-worse-any-dd`, which only looks at already closed trades before the candidate entry:

```sh
python3 scripts/ivrv_overlay.py --symbol ORCL --policy pre-entry-no-worse-any-dd --raw-dd-allowance-pct 0.0015 --cost-dd-allowance-pct 0.0015
python3 scripts/ivrv_overlay.py --symbol TSLA --policy pre-entry-no-worse-any-dd --raw-dd-allowance-pct 0.0015 --cost-dd-allowance-pct 0.0015
```

- ORCL accepted only `3` overlay trades, added `46` $25-cost PnL, and reached only `967` merged trades, so it did not solve cadence.
- TSLA accepted only `1` overlay trade, lost `-620` after costs, worsened merged $25-cost drawdown to `6.12%`, and added an extra negative active year.

Professional conclusion from the pre-entry gate: realized portfolio drawdown throttling is not the missing rule. It either becomes an oracle when it uses future candidate PnL, or it is too blunt when restricted to information available at entry. The next ORCL work needs a trade-level pre-entry feature, such as event timing, IV/RV term structure, gap-follow-through, or quote/liquidity quality, not a portfolio-level hindsight filter.

Cross-symbol locked-rule recheck: the same IV/RV long-vol rule was then run unchanged on the other starting symbols. This is a cleaner "move to next symbol" test because no per-symbol retuning was allowed:

```sh
python3 scripts/ivrv_overlay.py --symbol PLTR --policy slot-only
python3 scripts/ivrv_overlay.py --symbol IREN --policy slot-only
python3 scripts/ivrv_overlay.py --symbol TSLA --policy slot-only
python3 scripts/ivrv_overlay.py --symbol CRWV --policy slot-only
```

- PLTR standalone IV/RV overlay: `39` trades, `-963` gross PnL, `-1938` $25-cost PnL, and `6` negative cost-stressed active years. Slot-only merge accepted `24` trades and added only `755` $25-cost PnL while increasing $25-cost drawdown to `5.89%`; it still missed cadence at `988` merged trades.
- IREN standalone IV/RV overlay: `14` trades, `-1272` gross PnL, `-1622` $25-cost PnL. Slot-only merge accepted `10` losing trades and stayed cadence-blocked at `974` merged trades.
- TSLA standalone IV/RV overlay: `117` trades, `-5002` gross PnL, `-7927` $25-cost PnL, and `12.26%` standalone $25-cost drawdown. Slot-only merge accepted `91` trades, lost `-6774` after costs, and worsened merged $25-cost drawdown to `12.90%`.
- CRWV standalone IV/RV overlay: `24` trades, `-4834` gross PnL, `-5434` $25-cost PnL. Slot-only merge accepted `19` losing trades and worsened raw capital drawdown to `1.77%`.

Professional conclusion after the cross-symbol IV/RV recheck: ORCL remains the only symbol where this exact long-vol IV/RV sleeve is worth further hypothesis work. PLTR, IREN, TSLA, and CRWV should not be the next implementation target for this rule. The TSLA oracle-looking result under `no-worse-any-dd` is explicitly rejected because the fair slot-only sleeve is negative; using the oracle policy would be another form of overfit selection.

Entry-feature sweep follow-up: `scripts/ivrv_overlay.py` now stores entry-time features for IV/RV trades and can run a small pre-declared filter sweep:

```sh
python3 scripts/ivrv_overlay.py --symbol ORCL --feature-sweep
python3 scripts/ivrv_overlay.py --symbol PLTR --feature-sweep
python3 scripts/ivrv_overlay.py --symbol IREN --feature-sweep
python3 scripts/ivrv_overlay.py --symbol TSLA --feature-sweep
python3 scripts/ivrv_overlay.py --symbol CRWV --feature-sweep
```

The sweep intentionally tests simple one-feature cuts and a few small combinations only: IV/RV threshold, 20-day realized vol cap, DTE cap, debit/underlying cap, 20/60-day return sign, 20-day drawdown, open interest, and quote tightness.

Best useful rows:

- ORCL `return60_ge_0`: `13` standalone overlay trades, `7830` $25-cost PnL, `1.11%` standalone $25-cost drawdown, and `1509` $25-cost PnL after removing the best year. Slot-only merge accepted only `8` trades, reached `972` merged trades, and added `8600` $25-cost PnL, so quality improved but cadence remained blocked.
- ORCL `all_slot_only`: still the only fair row that clears the no-CRWV cadence gap, at `991` merged trades and `33285` merged $25-cost PnL, but with worse `1.46%` raw and `5.81%` $25-cost capital drawdown.
- TSLA `return20_lt_0`: `54` standalone overlay trades, `10052` $25-cost PnL, and `3087` $25-cost PnL after removing the best year. Slot-only merge accepted `49` trades and lifted merged $25-cost PnL to `34427` over `1013` trades, but drawdown was unacceptable: `5.69%` raw and `6.56%` $25-cost capital drawdown.
- PLTR, IREN, and CRWV had no feature-sweep row that both improved quality and solved cadence/risk. PLTR's best row added only `1326` accepted $25-cost PnL and stayed below cadence. IREN and CRWV remained mostly negative or too sparse.

Professional conclusion after the entry-feature sweep: ORCL has a real but sparse "long vol while medium-term trend is positive" hint. TSLA has a separate "long vol after short-term decline" hint, but the drawdown profile is much too large for the current canary objective. Neither should be first-class implemented yet. The next research branch should test these as hypothesis-specific structures with tighter risk design, not as generic strangles: ORCL should investigate trend-positive event/IV term-structure timing; TSLA should investigate defined-risk put-dominant continuation rather than symmetric strangles.

Single-leg decomposition: `scripts/ivrv_overlay.py` now supports `--structure strangle`, `--structure long-put`, and `--structure long-call` so the same IV/RV framework can test whether the edge comes from puts, calls, or symmetric convexity.

```sh
python3 scripts/ivrv_overlay.py --symbol TSLA --structure long-put --feature-sweep
python3 scripts/ivrv_overlay.py --symbol TSLA --structure long-call --feature-sweep
python3 scripts/ivrv_overlay.py --symbol ORCL --structure long-put --feature-sweep
python3 scripts/ivrv_overlay.py --symbol ORCL --structure long-call --feature-sweep
```

The decomposition rejects a generic single-leg implementation:

- TSLA long-put is the source of the TSLA short-term-decline hint, but it is not deployable. `return20_lt_0` had `115` standalone trades and `20190` $25-cost PnL, but `7.03%` standalone $25-cost drawdown and `-3267` $25-cost PnL after removing the best year. Merged accepted trades lifted $25-cost PnL to `44278`, but raw/cost-stressed drawdown rose to `5.97%` / `9.10%`.
- TSLA long-call is directly rejected. The all-slot row lost `-20647` after costs, and even the best-looking rows stayed negative or drawdown-heavy.
- ORCL long-put is not the ORCL edge. The all-slot row lost `-2158` after costs, and the positive rows were too small to matter.
- ORCL long-call is the cleaner decomposition of the ORCL hint, but still not robust enough. `return60_ge_0` reached `991` merged trades and `33892` merged $25-cost PnL, but raw/cost-stressed drawdown was `1.77%` / `5.58%`, and standalone PnL after removing the best year was `-1701`. The higher-IV rows improved drawdown but missed cadence and still had negative ex-best-year PnL.

Professional conclusion after single-leg decomposition: the next implementation should not be a generic long put, long call, or long strangle. TSLA needs a separate put-dominant structure with explicit crash-tail controls and outlier-year protection before it can be useful. ORCL needs a call-led, trend-positive/event or term-structure hypothesis that proves it survives without the best year. Until one of those pre-entry hypotheses passes, the current no-CRWV max-open `3` side-selective debit selector remains the cleaner baseline.

Directional debit-spread check: `scripts/ivrv_overlay.py` now also supports `--structure put-debit` and `--structure call-debit`. These use the same IV/RV entry framework with a long `45-70` delta leg, a short `20-45` delta leg, and `$5-$25` same-expiration width.

```sh
python3 scripts/ivrv_overlay.py --symbol TSLA --structure put-debit --feature-sweep
python3 scripts/ivrv_overlay.py --symbol ORCL --structure call-debit --feature-sweep
```

The defined-risk vertical conversion is rejected:

- TSLA put-debit did not preserve the long-put edge. The all-slot row lost `-27566` after costs with `28.14%` standalone $25-cost drawdown. Even the best filter, `ivrv_ge_2p00`, still lost `-2448` after costs and missed cadence at `980` merged trades. This means the earlier TSLA signal depended on uncapped convexity, not a clean weekly put-debit continuation spread.
- ORCL call-debit was also too small and weak. The best row, `ivrv_ge_2p00`, had only `8` standalone trades, `205` $25-cost PnL, `-25` after removing the best year, and only `6` accepted overlay trades. The all-slot row lost `-2997` after costs despite reaching `995` merged trades.

Professional conclusion after the directional debit-spread check: the requested `$5-$25` weekly vertical format does not rescue the IV/RV long-vol hints. For TSLA and ORCL, the apparent edge is convex single-leg/event exposure; making it a vertical reduces upside faster than it reduces bad-regime losses. Do not implement these vertical IV/RV sleeves in Rust unless a new pre-entry event or term-structure feature changes the economics.

Term-structure proxy check: there is no local historical earnings calendar in the repo, only static event-risk seed scores. `scripts/ivrv_overlay.py` therefore added a cache-native event proxy, `term_iv_ratio`, comparing the selected front-expiration IV with comparable-delta IV in the nearest later expiration on the same entry date. Values above `1.0` mean the front weekly is richer than the next expiration.

```sh
python3 scripts/ivrv_overlay.py --symbol ORCL --structure long-call --feature-sweep
python3 scripts/ivrv_overlay.py --symbol ORCL --structure strangle --feature-sweep
python3 scripts/ivrv_overlay.py --symbol TSLA --structure long-put --feature-sweep
```

The term-structure feature is useful diagnostically but not sufficient:

- ORCL strangle `return60_ge_0_term_front_rich_ge_1p05` was high quality but too sparse: `6` standalone trades, `7260` $25-cost PnL, zero negative cost-stressed active years, and `769` after removing the best year, but only `5` accepted overlay trades and `969` merged trades.
- ORCL long-call `return60_ge_0_term_front_rich_ge_1p05` did not improve the blocker enough: `23` standalone trades, `6291` $25-cost PnL, `-1357` after removing the best year, `986` merged trades, and `5.58%` merged $25-cost drawdown.
- TSLA long-put term structure did not control risk. `term_front_rich_ge_1p05` had `153` standalone trades and `7794` $25-cost PnL, but `24.32%` standalone $25-cost drawdown and `-7822` after removing the best year. The not-front-rich and back-rich rows were negative.

Professional conclusion after the term-structure proxy: front-week IV richness identifies some ORCL event-like winners, but it does not supply enough weekly cadence. For TSLA, term structure does not solve the crash-tail problem. The next credible improvement requires either a real historical event calendar with pre/post-event rules, or a new well-covered symbol; cache-only IV/RV and front/back IV features are exhausted for the current basket.

## Symbol Trade-Cap Recheck

Because the SOFI-expanded best profile has PLTR concentration of `57.9%`, a narrow total-trades-per-symbol cap sweep retested whether SOFI's added cadence made blunt concentration caps more viable than in the earlier non-SOFI branch. The runs reused the same six-symbol basket, `10%` symbol cap, `4` max open positions, `2` max positions per symbol, and `10%`/`20` day symbol drawdown cooldown:

- Cap `550`, run `runs/portfolio-weekly-selector-research-20260628T202143.099100000Z`: target profile stayed research-pass with `1142` trades, `40187` PnL, `1.79%` capital drawdown, `11637` $25-cost PnL, and PLTR PnL share `56.1%`.
- Cap `525`, run `runs/portfolio-weekly-selector-research-20260628T202143.099085000Z`: target profile stayed research-pass with `1121` trades, `35020` PnL, `1.79%` capital drawdown, `6995` $25-cost PnL, and PLTR PnL share `50.1%`.
- Cap `500`, run `runs/portfolio-weekly-selector-research-20260628T202143.085618000Z`: target profile barely kept cadence with `1094` trades, `33253` PnL, `2.30%` capital drawdown, `5903` $25-cost PnL, and PLTR PnL share `48.2%`.

Professional conclusion after the symbol trade-cap recheck: do not use a blunt per-symbol trade cap as the next canary path. It can reduce PLTR concentration, but it removes profitable PLTR trades faster than SOFI/ORCL/IREN/CRWV can replace them, worsens capital drawdown, reduces symbol-ablation robustness, and cuts $25-cost PnL sharply. The `525` and `500` caps prove the concentration problem is real, but also prove it is not solved by mechanical PLTR throttling.

## Large-Tech Cache Expansion Check

Follow-up cache and signal checks tested whether large liquid names could add an independent sleeve without more cache warming. The `80`-expiration coverage audit over all currently cached symbols showed that the large-tech additions are not immediately usable as both-side weekly debit candidates: `AAPL`, `AMZN`, `GOOGL`, `MSFT`, and `AVGO` each had `0` complete put+call expirations in the sampled window. `NVDA` had strong put-side coverage, `77/80` complete puts, but `0/80` complete calls.

The cache-only weekly universe screens confirmed the coverage diagnosis:

- Put-debit run `runs/universe-research-20260628T181909.975438000Z`: `NVDA` loaded `76` expirations and `122792` rows, but the best weekly put-debit profile lost `-11440` over `250` trades, with `0` walk-forward trades and `0` holdout trades. `AAPL`, `AMZN`, `MSFT`, and `AVGO` loaded `0` expirations and were `no_data`.
- Call-debit run `runs/universe-research-20260628T181909.965201000Z`: all five symbols, including `NVDA`, loaded `0` expirations and were `no_data`.

Full local-cache coverage refresh: after exhausting the cache-only IV/RV branch, all locally cached symbols were audited with `--from 2020-01-01 --to 2026-06-28 --max-expirations 80`:

```sh
cargo run -- audit-option-cache-coverage --symbols AAPL,AMD,AMZN,ASTS,AVGO,BAC,CCL,CRWV,F,GME,GOOGL,HOOD,IREN,MARA,META,MSFT,NVDA,ORCL,PLTR,RIOT,RIVN,RKLB,SOFI,TSLA,U --from 2020-01-01 --to 2026-06-28 --max-expirations 80
```

Best local both-side coverage was `SOFI` `67/80`, `HOOD` `52/80`, `META` `22/80`, `RKLB` `17/80`, `RIVN` `14/80`, `ASTS` `10/80`, `IREN` `10/80`, `AAPL` `9/80`, and `U` `7/80`. Most large-cap additions still had no usable both-side coverage in the sampled window: `AMZN`, `AVGO`, `GOOGL`, and `MSFT` were `0/80`; `NVDA` had `79/80` put coverage but `0/80` call coverage.

Because `SOFI` and `HOOD` were already rejected in the current research ledger, `META` was the next best locally cached expansion candidate. Four cache-only weekly sleeve screens were run:

```sh
cargo run -- research-weekly-universe --symbols META --from 2020-01-01 --to 2026-06-28 --max-expirations 80 --cache-only --symbol-concurrency 1 --profile-family weekly-put-debit
cargo run -- research-weekly-universe --symbols META --from 2020-01-01 --to 2026-06-28 --max-expirations 80 --cache-only --symbol-concurrency 1 --profile-family weekly-call-debit
cargo run -- research-weekly-universe --symbols META --from 2020-01-01 --to 2026-06-28 --max-expirations 80 --cache-only --symbol-concurrency 1 --profile-family weekly
cargo run -- research-weekly-universe --symbols META --from 2020-01-01 --to 2026-06-28 --max-expirations 80 --cache-only --symbol-concurrency 1 --profile-family weekly-far-otm
```

META is rejected:

- `runs/meta-weekly-call-debit-research-20260628T221052.482156000Z`: loaded `21` expirations, best profile had `42` trades, PnL `90`, PF `1.03`, but $25 friction PnL was `-960`; OOS gates were blocked with `0/155` walk-forward trades and `0/260` holdout trades.
- `runs/meta-weekly-put-debit-research-20260628T221052.478830000Z`: best profile had `57` trades, PnL `-2679`, PF `0.37`, and $25 friction PnL `-4104`; OOS gates blocked.
- `runs/meta-weekly-research-20260628T221052.478812000Z`: best short-put weekly profile had `63` trades, PnL `-192`, PF `0.86`, and $25 friction PnL `-1767`; OOS gates blocked.
- `runs/meta-weekly-far-otm-research-20260628T221052.466333000Z`: best far-OTM short-put profile had `24` trades, PnL `-338`, PF `0.23`, and $25 friction PnL `-938`; OOS gates blocked.

Professional conclusion after the local-cache expansion refresh: the "next locally cached symbol" path is not currently producing a viable sleeve. `SOFI`, `HOOD`, and now `META` have enough evidence for rejection; `RKLB`, `RIVN`, `ASTS`, `IREN`, `AAPL`, and `U` are too sparse or previously rejected; large liquid names need call-side cache warming before they can be fairly tested. The next meaningful research step is not another cache-only sweep. It is either a deliberate data build for a liquid candidate set, or an external historical event-calendar branch for the ORCL/TSLA event-vol hypotheses.

Professional conclusion after the large-tech check: do not add `NVDA`, `AAPL`, `AMZN`, `MSFT`, or `AVGO` to the current weekly selector from existing cache. `NVDA` may still be useful in a separate put-credit/swing branch, but weekly put-debit is currently negative and call-debit lacks data. The large liquid names require an intentional cache-completion pass before strategy conclusions are comparable.

## Lower-Dollar Short-Premium Recheck

Follow-up short-premium screens directly tested the original weekly short-put idea on the lower-dollar and partial-cache candidates. These used the existing weekly put-credit families rather than adding new code: 1-14 DTE put credit spreads, short puts at or below roughly `30` delta, `$1-$25` width caps, one-third profit taking for the standard weekly family, and conservative bid/ask fills.

- Standard weekly put-credit run `runs/universe-research-20260628T182234.644291000Z` blocked every symbol. `HOOD` ranked least bad with `53` trades and `-20` PnL across `48` loaded expirations. `SOFI` had `39` trades / `-64` PnL, `META` had `29` / `-174`, `AMD` had `46` / `-837`, `RKLB` had `14` / `-48`, and `ASTS`/`RIOT` produced no accepted trades. Every symbol had `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes.
- Far-OTM weekly put-credit run `runs/universe-research-20260628T182234.633291000Z` also blocked every symbol. `SOFI` was the only positive row, but only `7` trades / `23` PnL with no OOS evidence. `AMD` had `16` trades / `-182`, `HOOD` had `7` / `-15`, `META` had `11` / `-93`, `RKLB` had `3` / `-26`, and `ASTS`/`RIOT` produced no accepted trades.

Professional conclusion after the short-premium recheck: do not pivot the current weekly selector into short-put or far-OTM put-credit spreads for these candidates. The structure matches the desired cadence concept on paper, but in the current cache it has negative expectancy, too few accepted trades, and no OOS support. The next valid short-premium work would require either a different symbol universe with complete data or a genuinely different risk filter, not just more tuning of the same lower-dollar names.

The current `120`-expiration follow-up rechecked the two marginally covered unresolved names, `AMD` and `HOOD`, because they remain the only non-original symbols with roughly `20%+` both-side coverage in the current cache. It did not change the decision:

- Standard put-credit run `runs/universe-research-20260628T211229.861128000Z`: AMD loaded `29` expirations and lost `-857` over `60` trades; HOOD loaded `25` expirations and lost `-54` over `26` trades. Both had `0` active OOS trades and failed friction stress.
- Far-OTM put-credit run `runs/universe-research-20260628T211229.861099000Z`: AMD loaded `29` expirations and lost `-249` over `21` trades; HOOD loaded `25` expirations and lost `-4` over `4` trades. This is too sparse and still not positive after costs.
- Call-credit run `runs/universe-research-20260628T211229.850733000Z`: AMD loaded `26` expirations and lost `-75` over `29` trades; HOOD loaded `29` expirations and lost `-288` over `36` trades.

Professional conclusion after the 120-expiration AMD/HOOD short-premium follow-up: this closes the remaining "sell weekly premium on the better-covered expansion names" branch for the current cache. Neither put credit, far-OTM put credit, nor call credit produces an independently positive, friction-surviving sleeve. Do not tune these branches further without a new edge hypothesis.

## HOOD Cache Completion Recheck

HOOD was the next most data-ready unresolved lower-dollar candidate after SOFI. A cache-wide `80`-expiration audit showed `44/80` complete put+call expirations before the recheck, behind only SOFI among lower-dollar additions. A bounded cache-completion batch filled all `8` attempted recent put+call windows with no failures: `2024-05-03`, `2024-05-24`, `2024-06-21`, `2024-07-12`, `2024-08-02`, `2024-08-23`, `2024-09-20`, and `2024-10-11`.

The follow-up audit improved HOOD complete put+call coverage from `44/80` to `52/80`. Put coverage and call coverage are now both `52/80`, with complete call coverage from `2021-08-13` through `2024-10-11`. This made HOOD the cleanest unresolved lower-dollar candidate to retest.

The retests still rejected HOOD:

- Put-debit run `runs/universe-research-20260628T185636.182811000Z`: `114` trades, `-1087` PnL, `52` loaded expirations, `35937` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes. The prior lower-dollar put-debit screen had `101` trades and `-850` PnL, so the put side stayed negative after filling recent windows.
- Call-debit run `runs/universe-research-20260628T185636.182953000Z`: `79` trades, `125` PnL, `52` loaded expirations, `50133` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes. The prior call-debit screen had `63` trades and `148` PnL, so more complete data diluted the small positive residue.
- Standard weekly put-credit run `runs/universe-research-20260628T185703.664189000Z`: `62` trades, `-130` PnL, `52` loaded expirations, `49743` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes. The prior standard short-premium recheck was nearly flat at `53` trades and `-20` PnL; added recent windows made it worse.
- Far-OTM weekly put-credit run `runs/universe-research-20260628T185703.652466000Z`: `8` trades, `-21` PnL, `52` loaded expirations, `35937` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes.

Professional conclusion after HOOD cache completion: do not add HOOD to the current selector and do not use it as the short-premium/wheel pivot symbol. HOOD now has enough local coverage to make the rejection meaningful: the put-debit and short-premium sleeves are negative, while the call-debit sleeve is too small and has no OOS evidence. This supports the broader first-principles diagnosis that frequent weekly cadence alone is not the edge; we need symbols or structures where the post-cost expectancy survives broader coverage.

## ASTS Cache Completion Recheck

ASTS was the next sparse high-volatility candidate to retest because an earlier tiny RKLB/ASTS portfolio probe showed ASTS carrying more than all of the branch profit. That was only a data-readiness hint, not promotion evidence. A bounded cache-completion batch filled all `8` attempted put+call expiration windows with no failures: `2021-06-18`, `2021-09-17`, `2021-12-17`, `2022-02-18`, `2022-05-20`, `2022-08-19`, `2022-10-07`, and `2022-10-21`.

The follow-up `80`-expiration audit improved ASTS complete put+call coverage from `2/80` to `10/80`. Put and call coverage are now both `10/80`, with complete call coverage from `2021-04-16` through `2026-07-10`. Coverage remains too shallow for portfolio conclusions, but it was enough to test whether a direct weekly sleeve had any usable activity.

The side-specific reruns rejected ASTS as a current selector addition:

- Put-debit run `runs/universe-research-20260628T191502.304113000Z`: `3` trades, `-80` PnL, `10` loaded expirations, `3031` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes.
- Call-debit run `runs/universe-research-20260628T191502.305267000Z`: `5` trades, `-5` PnL, `10` loaded expirations, `4090` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes.
- Standard weekly put-credit run `runs/universe-research-20260628T191502.291050000Z`: `0` trades, `0` PnL, `10` loaded expirations, `4719` rows, and no OOS evidence.
- Far-OTM weekly put-credit run `runs/universe-research-20260628T191502.304069000Z`: `0` trades, `0` PnL, `10` loaded expirations, `3031` rows, and no OOS evidence.

Professional conclusion after ASTS cache completion: do not add ASTS to the current selector and do not infer edge from the earlier tiny RKLB/ASTS portfolio hint. ASTS is still a data-completion candidate, but the direct weekly debit and credit sleeves are either negative or inactive at the current coverage level. The next expansion work should prioritize either a better-covered independent symbol or a new lower-drawdown structure rather than more ASTS tuning.

## RIVN Cache Completion Recheck

RIVN was tested as the next unvalidated high-volatility, lower-dollar symbol after the pure-wheel branch remained blocked. Initial cache audit showed `0/80` complete put+call expirations. A bounded cache-completion pass filled all `8` attempted windows with no failures: `2021-11-19`, `2021-12-10`, `2021-12-31`, `2022-01-21`, `2022-02-11`, `2022-03-04`, `2022-03-25`, and `2022-04-14`.

The follow-up `80`-expiration audit improved RIVN complete put+call coverage from `0/80` to `8/80`. Put and call coverage are both `8/80`, with complete call coverage from `2021-11-19` through `2022-04-14`. Coverage remains shallow, but the completed windows were enough to test whether RIVN shows an immediate independent weekly sleeve.

The side-specific reruns rejected RIVN as a current selector addition:

- Put-debit run `runs/universe-research-20260628T193706.896745000Z`: `7` trades, `1396` PnL, `8` loaded expirations, `11989` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes. The raw profit is too small and sparse to treat as signal.
- Call-debit run `runs/universe-research-20260628T193706.896699000Z`: `4` trades, `-332` PnL, `8` loaded expirations, `13948` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes.
- Standard weekly put-credit run `runs/universe-research-20260628T193706.896681000Z`: `9` trades, `-444` PnL, `8` loaded expirations, `15572` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes.
- Far-OTM weekly put-credit run `runs/universe-research-20260628T193720.695492000Z`: `2` trades, `-105` PnL, `8` loaded expirations, `11989` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes.

Professional conclusion after RIVN cache completion: do not add RIVN to the current selector and do not tune on the tiny profitable put-debit sample. RIVN remains a data-readiness candidate only if more windows are intentionally warmed; on current evidence, the short-premium sleeves are negative, the call-debit sleeve is negative, and the only positive sleeve is far too sparse to survive the anti-overfit gates.

A follow-up bounded warmup for RIVN was started with `--max-windows-per-symbol 16` and intentionally interrupted after several silent minutes; the partial writes still improved the `80`-expiration coverage audit from `8/80` complete put+call windows to `14/80`, with put coverage at `15/80` and complete call coverage now spanning `2021-11-19` through `2022-08-19`. The expanded side-specific reruns weakened the only positive residue rather than strengthening it:

- Put-debit run `runs/universe-research-20260628T203551.124565000Z`: `22` trades, `958` PnL, `15` loaded expirations, `19832` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes. The earlier `7`-trade / `1396` PnL result diluted as coverage expanded.
- Call-debit run `runs/universe-research-20260628T203551.124604000Z`: `15` trades, `-726` PnL, `14` loaded expirations, `22031` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes.
- Standard weekly put-credit run `runs/universe-research-20260628T203551.112795000Z`: `25` trades, `-586` PnL, `15` loaded expirations, `26098` rows, and no OOS evidence.
- Far-OTM weekly put-credit run `runs/universe-research-20260628T203551.124584000Z`: `8` trades, `-92` PnL, `15` loaded expirations, `19832` rows, and no OOS evidence.

Updated professional conclusion: RIVN should remain rejected for the current selector. More RIVN cache warming is not the next best use of research time unless the goal becomes a dedicated RIVN data-build; the live option-strategy objective needs a symbol or structure with independent post-friction edge, not a shrinking sparse put-debit residue.

## U Cache Completion Recheck

`U` was tested after RIVN because it is another locally cached high-volatility, lower-dollar candidate with post-IPO option history starting around 2020. The initial post-2021 audit showed only `2/80` complete put+call expirations. A bounded cache-completion pass filled all `8` attempted windows with no failures: `2021-01-15`, `2021-04-16`, `2021-07-16`, `2021-10-15`, `2022-01-01`, `2022-01-30`, `2022-02-18`, and `2022-03-18`.

The follow-up `80`-expiration audit improved `U` complete put+call coverage from `2/80` to `10/80`. Put and call coverage are both `10/80`, with complete call coverage from `2021-01-15` through `2024-03-22`. Coverage remains shallow, but the completed windows covered enough early post-IPO volatility to test whether an obvious weekly sleeve exists.

The side-specific reruns rejected `U` as a current selector addition:

- Put-debit run `runs/universe-research-20260628T194456.162171000Z`: `18` trades, `-60` PnL, `8` loaded expirations, `6520` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes.
- Call-debit run `runs/universe-research-20260628T194456.173907000Z`: `10` trades, `-663` PnL, `8` loaded expirations, `10530` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes.
- Standard weekly put-credit run `runs/universe-research-20260628T194456.173962000Z`: `6` trades, `-246` PnL, `8` loaded expirations, `10733` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes.
- Far-OTM weekly put-credit run `runs/universe-research-20260628T194510.299501000Z`: `2` trades, `-95` PnL, `8` loaded expirations, `6520` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes.

Professional conclusion after `U` cache completion: do not add `U` to the current selector and do not warm more `U` windows as the next priority. Unlike RIVN, there is no even tiny positive sleeve to investigate; all four weekly structures are negative in the completed sample. The repeated pattern across `HOOD`, `AMD`, `META`, `RKLB`, `ASTS`, `RIVN`, and `U` supports the current professional conclusion: the next candidate needs either substantially better prior coverage or a new independent edge hypothesis, not just another high-volatility symbol with sparse cache.

## Signal-Gate Audit and ORCL Fallback Recheck

After the RIVN residue diluted, focused signal-gate audits separated sparse lower-dollar residues from a real core-symbol opportunity.

- SOFI call-debit audit: `67/80` expirations loaded, `52328` rows, best profile `weekly_call_debit_core_debit55_dte3_10_delta25_55_take33` with `102` trades, `627` PnL, and `195` candidate entry days. This is active enough to explain the SOFI portfolio contribution, but it is too weak as a standalone sleeve and still has no OOS support.
- AMD put-debit audit: `27/80` expirations loaded, `19926` rows, best profile `weekly_put_debit_regime_dte1_7_w25_delta30_60_rv30_ret15_dd12_take33` with `35` trades and `1245` PnL. This is not enough for promotion; AMD remains a cache-completion question because most audited windows still fail in cache-only mode.
- ORCL call-debit audit: `491/549` expirations loaded, `188795` rows in the side-specific audit, with multiple robust-looking call-debit profiles. The best broad profile was `weekly_call_debit_dte1_7_w25_delta30_60_take50` at `368` trades and `9992` PnL. The best stricter cost-aware fallback tested for selector use was `weekly_call_debit_costaware_dte3_10_w25_delta20_45_mindebit50_minw5_take25` at `258` trades and `8710` PnL.

The selector now includes one targeted ORCL fallback challenger, `selector_side_selective_pltr_put_plus_orcl_costaware_minw5_non_tsla_call_debits_only`, which uses the existing side-selective PLTR put plus non-TSLA call-debit structure and switches ORCL to the stricter `mindebit50_minw5` fallback.

Current six-symbol selector run `runs/portfolio-weekly-selector-research-20260628T204156.580299000Z` made the new ORCL-strict profile the top-ranked profile: `1115` trades, `35147` PnL, `1.80` profit factor, and `7272` $25-cost PnL. It is still canary-blocked: capital drawdown is `3.43%`, $25-cost capital drawdown is `7.96%`, PLTR contributes `63.6%` of PnL, and both symbol and strategy ablation passes remain `0`.

Risk-control rerun `runs/portfolio-weekly-selector-research-20260628T204506.606899000Z` used `4` max open positions and a `10%` / `20` day symbol drawdown cooldown. Under those controls, the ORCL-strict profile improved per-trade quality but failed cadence: `1005` trades versus `1091` required, `42384` PnL, `17259` $25-cost PnL, `2.16%` capital drawdown, and `6.32%` $25-cost capital drawdown. The top risk-control profile reverted to the SOFI/ORCL mindebit35 selector with `1181` trades, `43607` PnL, `14082` $25-cost PnL, and `2.16%` capital drawdown, still above canary budget.

Professional conclusion: ORCL is the strongest current independent call-debit sleeve and the stricter ORCL fallback is worth keeping as a research challenger, but it does not solve the portfolio problem. The limiting factors remain PLTR concentration, cadence fragility under throttles, and capital drawdown above the `1%` canary threshold. The next improvement should not be another lower-dollar sparse symbol; it should either add a genuinely independent, well-covered symbol with comparable ORCL-quality evidence or add a new structure/regime filter that reduces PLTR/ORCL drawdown without deleting weekly cadence.

Follow-up PLTR concentration audit confirmed why that blocker is hard to remove. In the ORCL-strict profile from `runs/portfolio-weekly-selector-research-20260628T204156.580299000Z`, PLTR contributed `22350` PnL split between call debit (`224` trades / `11448` PnL) and put debit (`379` trades / `10902` PnL). The put side had many more losing trades than winning trades (`241` losses vs `138` wins), but the winners were large enough to carry the sleeve.

The PLTR side-specific audits showed a stricter put-debit alternative:

- PLTR put-debit audit: `298/300` expirations loaded, `127850` rows. Best profile `weekly_put_debit_regime_dte1_7_w25_delta30_60_rv35_ret10_dd12_take33` had `220` trades and `8137` PnL.
- PLTR call-debit audit: `298/300` expirations loaded, `125972` rows. Best profile `weekly_call_debit_dte5_14_w25_delta30_60_take50` had `288` trades and `3589` PnL, but changing the shared call sleeve would also affect IREN and CRWV.

A targeted PLTR-regime selector was tested in `runs/portfolio-weekly-selector-research-20260628T205243.286630000Z`, using the stricter PLTR put profile with the ORCL `mindebit50_minw5` fallback. It was rejected: `1007` trades versus `1091` required, `28093` PnL, `2918` $25-cost PnL, `3.65%` capital drawdown, and no ablation passes. The put sleeve improved quality (`156` trades, `9246` PnL, `3.24` profit factor), but it removed too much cadence and left the portfolio dependent on the call-debit sleeve. Do not keep this selector in the active profile set; use it only as evidence that tightening PLTR put entries alone does not solve the portfolio blocker.

## Call-Credit Sleeve Recheck

After repeated lower-dollar symbol additions failed, the next independent sleeve tested was weekly call-credit spreads: short-call defined-risk spreads around `10-30` delta, weak/overbought gates, one-third profit taking, capped overlap, and conservative bid/ask fills. The research CLI now exposes this existing profile family as `weekly-call-credit`, so call-credit sleeves can be screened per symbol instead of inferred only from mixed portfolio selector runs.

Initial cache-only run `runs/universe-research-20260628T194931.007326000Z` tested `IREN,PLTR,ORCL,TSLA,CRWV,SOFI` from `2016-01-01` through `2026-06-28` with `80` expirations:

- SOFI was the only symbol with meaningful loaded call-credit data: `28` trades, `-116` PnL, `39` loaded expirations, `18600` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes.
- IREN loaded only `7` expirations and produced `0` trades.
- PLTR, ORCL, TSLA, and CRWV were `no_data` because the current call-side open-interest cache is missing for the selected windows.

Follow-up cache-only run `runs/universe-research-20260628T195020.762557000Z` moved the same call-credit screen to a `2022-01-01` start for `PLTR,ORCL,TSLA,CRWV`, but those symbols still loaded `0` usable expirations. A matching coverage audit showed why: over the first `80` post-2022 windows, PLTR had `0/80` complete calls, ORCL `0/80`, TSLA `0/80`, and CRWV `0/66`; SOFI had `23/80` complete put+call windows and was already negative in the direct screen.

Professional conclusion after the call-credit recheck: do not add call-credit to the current selector from existing cache. The only evaluable sleeve, SOFI call credit, is negative and has no OOS support. The core symbols are blocked by call-side cache readiness rather than by proven negative expectancy. If call-credit remains an attractive independent hypothesis, the next valid step is a bounded call-side cache completion for one liquid core name such as TSLA or PLTR, followed by the same `weekly-call-credit` universe screen; do not infer a portfolio edge from the current no-data rows.

Full-cache follow-up `runs/universe-research-20260628T195441.401718000Z` removed that ambiguity for the currently available historical cache. It completed all six symbols with `weekly-call-credit` and rejected every one: PLTR `290` trades / `-1277` PnL, TSLA `346` / `-7223`, ORCL `290` / `-2955`, CRWV `60` / `-2129`, IREN `56` / `-839`, and SOFI `48` / `-186`. All six remained `blocked`, with `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes. Treat weekly call-credit as rejected for this basket unless a materially different regime filter is introduced; more call-cache warming alone is not enough.

## Expanded Wheel Recheck

Fresh current-code wheel rerun `runs/portfolio-weekly-wheel-research-20260628T180231.889467000Z` tested the SOFI-expanded basket `IREN,PLTR,ORCL,TSLA,CRWV,SOFI` with the same `$100000` capital budget, `20%` symbol cap, `4` max open positions, `2` max positions per symbol, and `10%`/`20` day symbol drawdown cooldown.

- Best profile `weekly_wheel_inventory_exit_dte3_10_callfloor98_credit01_hold21` remained blocked: `431` trades versus `1091` required, `22375` PnL, `1.79` profit factor, `5.41%` capital drawdown, and `11600` $25-cost PnL.
- Canary readiness was blocked by inventory risk: `29.7%` assignment rate, `81` marked-stock cycles, `-19476` marked-stock PnL, `-4189` worst marked-stock loss, and `inventory_risk_high`.
- SOFI did not improve the wheel branch: `62` SOFI wheel trades lost `-122`, with `21` assigned cycles and `20` marked-stock cycles.
- The wheel remains too sparse as a standalone weekly engine and too exposed to assignment/gap risk as a replacement for the current side-selective debit challenger.

Strict follow-up wheel rerun `runs/portfolio-weekly-wheel-research-20260628T191942.233960000Z` tested the same SOFI-expanded basket with the current best selector's tighter `10%` symbol cap. Risk improved but the strategy still failed the weekly objective:

- Best profile `weekly_wheel_dte3_10_delta10_30_credit01_hold21` was blocked with only `351` trades versus `1091` required, `15223` PnL, `2.32` profit factor, `1.87%` capital drawdown, and `6448` $25-cost PnL.
- Canary readiness remained blocked by inventory risk: `28.5%` assignment rate, `66` marked-stock cycles, `-7416` marked-stock PnL, `-950` worst marked-stock loss, and `inventory_risk_high`.
- The best longer-hold inventory-exit rows had lower capital drawdown, but they were even sparser at roughly `302-303` trades versus `1091` required. Tightening risk turns the wheel into a low-cadence income sleeve, not a weekly engine.

Professional conclusion after the expanded wheel recheck: do not pivot the portfolio to a pure wheel on this basket. Wheel-like inventory can remain a research sleeve only when assignment, marked-stock loss, and symbol concentration are explicitly capped. The first-principles issue is that selling puts and covered calls manufactures action by warehousing downside equity beta; it does not create a durable option edge unless the premium collected is large enough to pay for gap risk, adverse selection, assignment friction, and post-assignment capital lockup. The next productive branch is either a defined-risk synthetic wheel/credit sleeve that proves independently positive after costs, or another lower-dollar symbol with SOFI-like coverage and positive side-specific evidence.

## AMD/META/RKLB Follow-Up

After SOFI/HOOD, the next best locally cached non-original candidates were `AMD`, `META`, and `RKLB`. A cross-symbol `--max-expirations 80` audit showed why this is still a data-readiness branch rather than a strategy branch:

- `AMD`: `12` complete put+call expirations out of `80`, with complete call coverage from `2016-07-15` to `2023-01-13`.
- `META`: `14` complete put+call expirations out of `80`, with complete call coverage from `2021-07-16` to `2024-10-18`.
- `RKLB`: `9` complete put+call expirations out of `80`, with complete call coverage from `2021-09-17` to `2025-10-17`.

Independent cache-only sleeve screens confirmed that none of the three has enough standalone evidence:

- Put-debit run `runs/universe-research-20260628T162120.866374000Z`: `AMD` had `45` trades and `615` PnL, `META` had `26` trades and `-916` PnL, and `RKLB` had `14` trades and `-598` PnL. All three had `0` walk-forward trades and `0` holdout trades.
- Call-debit run `runs/universe-research-20260628T162120.855706000Z`: `AMD` had `23` trades and `-396` PnL, `META` had `15` trades and `300` PnL, and `RKLB` had `10` trades and `180` PnL. All three had `0` walk-forward trades and `0` holdout trades.

The combined original-plus-AMD/META/RKLB allocator check, `runs/portfolio-weekly-selector-research-20260628T162208.814920000Z`, also blocked. It loaded only partial bounded data for the full basket and the best profile, `selector_trend_credit_put_spread_plus_crash_put_and_call_debits`, had `204` trades versus `1065` required, `-734` PnL, `0.94` profit factor, `2.98%` capital drawdown, and a negative active `put_credit_spread` sleeve at `-2731` PnL. TSLA was also negative in this bounded sample at `-2066` PnL.

Professional conclusion: `AMD/META/RKLB` should not be used for strategy tuning, canary deployment, or promotion from the current cache. `AMD` has the only mildly interesting put-debit raw PnL in this tiny sample, but the sample is too shallow and has no walk-forward/holdout evidence. The next productive action is cache completion for a small number of chosen names, not adding more profile variants to these incomplete datasets.

## AMD Cache Completion Recheck

Two bounded AMD cache-completion batches tested whether the earlier small positive put-debit result survived broader coverage. Each batch attempted `8` missing put+call expiration windows. In both batches, the `2016-01-08` and `2016-02-19` expirations failed because their late-2015 open-interest history requires the higher ThetaData subscription tier. The other `12` attempted windows completed successfully: `2016-04-08`, `2016-05-27`, `2016-09-02`, `2016-10-21`, `2016-12-09`, `2017-01-27`, `2017-05-05`, `2017-06-23`, `2017-08-11`, `2017-09-29`, `2018-01-05`, and `2018-02-23`.

The comparable `80`-expiration audit improved AMD complete put+call coverage from `12/80` to `24/80`. Put coverage improved to `27/80`, call coverage to `24/80`, and complete call coverage now starts at `2016-04-08` and still ends at `2023-01-13`. Coverage remains incomplete, but the sample is materially less sparse than the first AMD side-specific screen.

The side-specific reruns weakened AMD rather than promoting it:

- First post-warm put-debit run `runs/universe-research-20260628T183946.258315000Z`: `53` trades, `561` PnL, `21` loaded expirations, `17016` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes.
- First post-warm call-debit run `runs/universe-research-20260628T183946.246533000Z`: `26` trades, `-414` PnL, `18` loaded expirations, `17238` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes.
- Second post-warm put-debit run `runs/universe-research-20260628T184336.965682000Z`: `67` trades, `276` PnL, `27` loaded expirations, `19926` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes. This is worse quality than the original `45` trades / `615` PnL sparse sample because more trades produced less total PnL.
- Second post-warm call-debit run `runs/universe-research-20260628T184336.977558000Z`: `35` trades, `-163` PnL, `24` loaded expirations, `21192` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes.

Professional conclusion after AMD cache completion: do not add AMD to the current selector and do not keep tuning the mildly positive put-debit residue. The signal is behaving like sparse-sample noise: adding valid expirations increased trade count while reducing total PnL, and neither side produced OOS evidence. The next symbol-expansion step should move to another data-readiness candidate rather than spending more optimizer budget on AMD.

## AAPL Liquid-Control Cache Probe

After rejecting repeated high-volatility lower-dollar additions, AAPL was tested as a liquid, tighter-spread control symbol rather than another premium-rich speculative name. A bounded cache-completion attempt for `AAPL` from `2020-01-01` through `2026-06-28` with `--max-expirations 80` and `--max-windows-per-symbol 8` was intentionally interrupted after it had made useful progress but before final JSON completion, because the large-cap historical fetch was taking too long for this research loop. The follow-up audit showed concrete data-readiness progress: AAPL improved from `0/80` complete put+call expirations to `7/80`, with complete coverage from `2020-01-03` through `2020-07-24`.

The partial-cache sleeve screens rejected AAPL as a current selector addition:

- Put-debit run `runs/universe-research-20260628T200953.298884000Z`: `19` trades, `-693` PnL, `7` loaded expirations, `9124` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes.
- Call-debit run `runs/universe-research-20260628T200953.310031000Z`: `14` trades, `-133` PnL, `7` loaded expirations, `16074` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes.
- Standard weekly put-credit run `runs/universe-research-20260628T201018.083521000Z`: `12` trades, `-25` PnL, `7` loaded expirations, `15084` rows, and no OOS evidence.
- Far-OTM weekly put-credit run `runs/universe-research-20260628T201018.073456000Z`: `2` trades, `-59` PnL, `7` loaded expirations, `9124` rows, and no OOS evidence.

Professional conclusion after the AAPL liquid-control probe: AAPL is data-readiness progress only. It should not be added to the selector from the current partial cache, and it should not be used as proof that large liquid names solve the weekly objective. The useful lesson is that tighter spreads and brand-name liquidity do not automatically offset low premium, incomplete history, and post-cost weekly edge weakness. If large-cap controls remain attractive, the next pass should be an intentional cache-build job with a larger time budget, not strategy tuning on seven expirations.

A follow-up bounded AAPL cache warmup was attempted with the same `80`-expiration sample and stopped after the local file count grew to `1177`. Coverage improved only from `7/80` to `9/80`, extending complete call coverage from `2020-07-24` to `2020-09-25`. The cache-fill path remains slow for large-cap controls.

The four current weekly side-family reruns on the updated AAPL cache still rejected it as a current research candidate:

- Put debit `runs/aapl-weekly-put-debit-research-20260628T211041.104961000Z`: `9` expirations, `13049` rows, `23` trades, `-1216` PnL, `0.47` profit factor, `34.1%` max drawdown, and `-1791` $25-cost PnL.
- Call debit `runs/aapl-weekly-call-debit-research-20260628T211041.105121000Z`: `9` expirations, `23077` rows, `18` trades, `-171` PnL, `0.86` profit factor, `18.4%` max drawdown, and `-621` $25-cost PnL.
- Standard put credit `runs/aapl-weekly-research-20260628T211041.091077000Z`: `9` expirations, `21587` rows, `17` trades, `173` PnL, `1.35` profit factor, but only `3` PnL after $10/trade and `-252` after $25/trade, with no OOS evidence.
- Far-OTM put credit `runs/aapl-weekly-far-otm-research-20260628T211041.104924000Z`: `9` expirations, `13049` rows, `3` trades, `-246` PnL, and all exits stopped.

Professional conclusion after the AAPL follow-up: the only positive AAPL sleeve is a tiny 2020-only put-credit residue that disappears under realistic high-friction stress and has no walk-forward/holdout support. Do not tune it. AAPL remains useful only as evidence that an intentional liquid-universe cache build is required before large-cap controls can be fairly judged.

The next bounded large-liquid warmup attempted `AAPL,MSFT,AMZN,GOOGL,AVGO` from `2020-01-01` through `2026-06-28` with `--max-expirations 80`, `--max-windows-per-symbol 4`, and `--fetch-concurrency 2`. It was intentionally interrupted after useful cache progress rather than allowed to run unbounded. The follow-up audit showed AAPL improved to `13/80` complete put+call expirations, with complete call coverage from `2020-01-03` through `2021-02-12`; MSFT improved to only `4/80`; AMZN, GOOGL, and AVGO remained `0/80` both-side complete.

The latest AAPL-only reruns still rejected it:

- Call debit `runs/aapl-weekly-call-debit-research-20260628T222156.522903000Z`: `13` expirations, `30245` EOD rows, `32` trades, `-797` PnL, `0.61` profit factor, `-1597` $25-cost PnL, positive years `0/2`, and blocked walk-forward/holdout gates with `0` OOS trades.
- Put debit `runs/aapl-weekly-put-debit-research-20260628T222156.511755000Z`: `13` expirations, `18408` EOD rows, `37` trades, `-1655` PnL, `0.42` profit factor, `-2580` $25-cost PnL, positive years `0/2`, and blocked walk-forward/holdout gates with `0` OOS trades.
- Standard weekly short put `runs/aapl-weekly-research-20260628T222156.524725000Z`: `13` expirations, `28385` EOD rows, `32` trades, `-883` PnL, `0.32` profit factor, `-1683` $25-cost PnL, positive years `1/2`, and blocked walk-forward/holdout gates with `0` OOS trades.

Professional conclusion after the expanded AAPL liquid-control probe: filling more AAPL expirations worsened the result, including the previously tiny positive short-put residue. Do not move AAPL forward and do not spend more optimizer time on the current partial sample. AAPL remains a data-build/control candidate only if the large-liquid cache can be completed deliberately.

## RKLB Cache Completion Recheck

A bounded RKLB cache-completion batch tested whether the earlier tiny positive call-debit result survived broader coverage. The refresh completed all `8` attempted put+call expiration windows with no failures: `2021-11-19`, `2022-01-21`, `2022-03-18`, `2022-05-20`, `2022-08-19`, `2022-10-21`, `2022-12-16`, and `2023-02-17`.

The follow-up `80`-expiration audit improved RKLB complete put+call coverage from `9/80` to `17/80`. Put coverage improved to `18/80`, call coverage to `17/80`, and complete call coverage still spans `2021-09-17` to `2025-10-17`. Coverage remains shallow, but the added windows were enough to test whether the first tiny call-debit residue strengthened.

The side-specific reruns rejected RKLB as a selector addition:

- Put-debit run `runs/universe-research-20260628T184939.905706000Z`: `20` trades, `-658` PnL, `18` loaded expirations, `8849` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes. The prior sparse put-debit probe had `14` trades and `-598` PnL, so the put side stayed negative.
- Call-debit run `runs/universe-research-20260628T184939.894232000Z`: `14` trades, `150` PnL, `17` loaded expirations, `13341` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes. The prior sparse call-debit probe had `10` trades and `180` PnL, so more valid expirations produced more trades but less total PnL.

Professional conclusion after RKLB cache completion: do not add RKLB to the current selector and do not tune the call-debit residue. It is the same failure mode as AMD/META: a tiny positive sleeve becomes less attractive as missing expirations are filled, and there is no OOS evidence. RKLB can remain a data-completion candidate only if we intentionally build a much larger cache later; it is not a current strategy candidate.

## META Cache Completion Recheck

A bounded META cache-completion pass tested whether the earlier tiny positive call-debit result was real signal or sparse-sample noise. The refresh completed `8` additional put+call expiration windows with no failures: `2022-12-30`, `2023-01-27`, `2023-02-17`, `2023-03-10`, `2023-04-06`, `2023-04-28`, `2023-05-19`, and `2023-06-16`.

The follow-up `80`-expiration audit improved META complete put+call coverage from `14/80` to `22/80`, with complete call coverage still bounded between `2021-07-16` and `2024-10-18`. That is better data readiness, but still not enough to treat META as a full-history comparable candidate.

The side-specific reruns rejected META as the next selector addition:

- Call-debit run `runs/universe-research-20260628T183356.180870000Z`: `42` trades, `90` PnL, `21` loaded expirations, `46394` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes. The prior sparse call-debit probe had `15` trades and `300` PnL, so the edge diluted sharply as missing windows were added.
- Put-debit run `runs/universe-research-20260628T183356.169361000Z`: `57` trades, `-2679` PnL, `21` loaded expirations, `29028` rows, `0` walk-forward trades, `0` holdout trades, and `0` fixed-profile OOS passes. This confirms the put side is structurally negative in the currently loaded sample.

Professional conclusion after META cache completion: do not move META into the current selector and do not tune on the tiny positive call-debit residue. The useful lesson is diagnostic: sparse positive option signals can disappear when missing expirations are filled. Continue symbol expansion only when a candidate shows positive side-specific evidence after coverage improves, not before.

## Symbol Expansion Review

The initial AMD cache-only run did not change the portfolio evidence. The loader discovered AMD expirations but loaded `0` rows with `690` cache-miss failures, so AMD did not contribute opportunities.

A bounded AMD refresh was started with `--max-expirations 160` and stopped after warming the cache from roughly `2576` to over `6600` AMD files. A smaller AMD-only cache-backed sample with `--max-expirations 60` then loaded `9` expirations and `22928` rows, but it produced only `22` trades and `-407` PnL, so it was not enough evidence.

The loader now composes adjacent cached option windows when an exact full-history cache key is missing. With that fix, the full-basket cache-only rerun loaded AMD evidence: `82` expirations, `64433` rows, and first usable date `2016-03-04`. AMD still does not solve the strategy problem. In the top AMD-loaded profile, AMD contributed only `148` trades and `420` PnL, while IREN remained negative and TSLA still carried the portfolio. Treat AMD as partially validated data, not as sufficient diversification.

Expanded-cache run `runs/portfolio-weekly-selector-research-20260628T110105.046367000Z` requested `IREN`, `PLTR`, `ORCL`, `TSLA`, `CRWV`, `AMD`, `META`, `AMZN`, `AAPL`, `MSFT`, `GOOGL`, and `AVGO`. It produced the same six-symbol evidence because `META`, `AMZN`, `AAPL`, `MSFT`, `GOOGL`, and `AVGO` each loaded `0` rows in cache-only mode. All `22` profiles remained canary-blocked.

`META` is the next data-readiness target, not a validated expansion symbol. A bounded live refresh was started for `80` expirations and stopped after warming the cache to roughly `1600` META files. Follow-up cache-only probe `runs/portfolio-weekly-selector-research-20260628T111645.866399000Z` loaded only `2` expirations and `1713` rows, producing `4` trades and `-54` PnL in the top profile. A larger warmed-cache probe `runs/portfolio-weekly-selector-research-20260628T111830.593510000Z` loaded `13` expirations and `51338` rows, but still produced only `35` trades and `-22` PnL in the top profile. Continue META cache warming before using it as portfolio evidence.

The option-data loader now clamps each expiration load window to the expiration date instead of requesting post-expiration option rows. This fixed a cache-shape problem where valid through-expiration files could not be reused because the requested window extended beyond contract life.

`NVDA` was tested as a deeper-cache expansion candidate after that fix. The first combined portfolio selector run loaded `0` rows because the portfolio loader required both put and call OI, while the local NVDA cache was missing the call-OI windows needed by the selector. The loader now keeps usable put rows in cache-only mode when call rows are missing, which prevents put-only sleeves from being discarded by a call-side cache miss. The portfolio symbol summary now also separates total usable expirations from put-side and call-side expiration counts, so partial-but-usable evidence is not mislabeled as zero loaded expirations.

Follow-up combined selector run `runs/portfolio-weekly-selector-research-20260628T113912.771992000Z` loaded `166` NVDA put expirations, `0` call expirations, and `143198` put rows, but it did not become deployable. The best profile, `selector_economic_wheel_plus_crash_and_costaware_puts_and_call_debits`, had `293` trades, `-7007` PnL, `0.75` profit factor, `3.1%` max drawdown, and `-14332` $25-cost PnL. Its put-debit sleeve lost `-9464`; all top profiles remained canary-blocked due negative put-credit or put-debit sleeves. A separate put-only `weekly-put-debit` universe run, `runs/nvda-weekly-put-debit-research-20260628T112341.540208000Z`, also loaded `166` expirations and `143198` rows but was not profitable: best profile `weekly_put_debit_dte1_7_w10_delta30_60_take25` had `245` trades, `-5381` PnL, `0.75` profit factor, `15.8%` max drawdown, and `-11506` $25-cost PnL.

The direct defined-risk "synthetic wheel" recheck also rejected NVDA put-side short premium on the current cache:

- Standard weekly put-credit run `runs/nvda-weekly-research-20260628T210051.706694000Z`: `116` expirations, `304629` rows, best profile `weekly_core_maxpos5_gap1_dte5_14_delta10_30_take33`, `391` trades, `-4158` PnL, `0.65` profit factor, `4.59%` max drawdown, `-13933` $25-cost PnL, `2/11` positive years, and no active walk-forward or holdout years.
- Far-OTM put-credit run `runs/nvda-weekly-far-otm-research-20260628T210051.718787000Z`: `116` expirations, `188654` rows, best profile `weekly_far_otm_dte5_14_w10_delta05_15_take25_stop125`, `143` trades, `-2332` PnL, `0.34` profit factor, `4.12%` max drawdown, `-5907` $25-cost PnL, `1/11` positive years, and no active walk-forward or holdout years.

Treat NVDA as rejected for the currently implemented put-side weekly families: put debit, standard put credit, and far-OTM put credit. The professional implication is important for the wheel question: moving from long premium to short premium does not fix the edge. It mostly changes the loss shape. NVDA has enough put-side coverage to make the rejection meaningful, and the loss pattern is too persistent across years to justify tuning a synthetic-wheel sleeve around it.

## 120-Expiration Expansion Coverage Snapshot

The current-cache `120`-expiration coverage sweep across remaining candidates found no hidden well-covered expansion universe:

- Large-cap controls `AAPL`, `AMZN`, `MSFT`, `GOOGL`, and `AVGO` each had `0/120` complete put+call windows.
- NVDA had strong put-side coverage, `116/120`, but `0/120` call-side and `0/120` both-side coverage.
- Low-dollar/high-beta names were mostly unusable for both-side tests: `BAC`, `CCL`, `GME`, and `RIOT` each had `1/120`; `F` and `MARA` had `0/120`; `U` had `4/120`.
- The only non-original names near usable both-side samples were still partial: `AMD` `26/120`, `HOOD` `25/120`, `ASTS` `17/120`, `META` `11/120`, `RIVN` `8/120`, and `RKLB` `7/120`.

This explains why adding symbols has not repaired PnL/DD. There is no broad local-cache universe of clean independent weekly candidates yet. AMD and HOOD remain the only marginal both-side expansion candidates, and both already showed weak or diluted side-specific evidence. The next productive research path is either an intentional cache-build pass for a chosen liquid universe, or a new strategy family with clearly different risk economics, not more retuning of these shallow partial-cache residues.

## Repeatable Commands

Regenerate the selector report from cached data:

```sh
cargo run --quiet -- research-portfolio-selector --symbols IREN,PLTR,ORCL,TSLA,CRWV,AMD,META,AMZN,AAPL,MSFT,GOOGL,AVGO --from 2010-01-01 --to 2026-06-26 --cache-only --fetch-concurrency 8 --symbol-concurrency 6 --capital-budget 100000 --max-symbol-allocation-pct 0.20 --max-open-positions 8 --max-positions-per-symbol 2
```

Regenerate the current best original-symbol research-pass adjustment:

```sh
cargo run --quiet -- research-portfolio-selector --symbols IREN,PLTR,ORCL,TSLA,CRWV --from 2016-01-01 --to 2026-06-28 --cache-only --fetch-concurrency 2 --symbol-concurrency 1 --capital-budget 100000 --max-symbol-allocation-pct 0.20 --max-open-positions 4 --max-positions-per-symbol 2 --symbol-drawdown-cooldown-trigger-pct 0.10 --symbol-drawdown-cooldown-days 20
```

Regenerate the AMD substitution check:

```sh
cargo run --quiet -- research-portfolio-selector --symbols IREN,PLTR,ORCL,CRWV,AMD --from 2016-01-01 --to 2026-06-28 --cache-only --fetch-concurrency 2 --symbol-concurrency 1 --capital-budget 100000 --max-symbol-allocation-pct 0.20 --max-open-positions 4 --max-positions-per-symbol 2 --symbol-drawdown-cooldown-trigger-pct 0.10 --symbol-drawdown-cooldown-days 20
```

Regenerate the current lower-dollar partial-cache check:

```sh
cargo run --quiet -- research-portfolio-selector --symbols IREN,PLTR,ORCL,TSLA,CRWV,SOFI,HOOD,RKLB,ASTS --from 2016-01-01 --to 2026-06-28 --cache-only --fetch-concurrency 4 --symbol-concurrency 2 --capital-budget 100000 --max-symbol-allocation-pct 0.20 --max-open-positions 4 --max-positions-per-symbol 2 --symbol-drawdown-cooldown-trigger-pct 0.10 --symbol-drawdown-cooldown-days 20
```

Regenerate the RIOT marginal-contribution check:

```sh
cargo run --quiet -- research-portfolio-selector --symbols IREN,PLTR,ORCL,TSLA,CRWV,RIOT --from 2016-01-01 --to 2026-06-28 --cache-only --fetch-concurrency 4 --symbol-concurrency 2 --capital-budget 100000 --max-symbol-allocation-pct 0.20 --max-open-positions 4 --max-positions-per-symbol 2 --symbol-drawdown-cooldown-trigger-pct 0.10 --symbol-drawdown-cooldown-days 20
```

Run the independent lower-dollar debit sleeve screens:

```sh
cargo run --quiet -- research-weekly-universe --symbols SOFI,HOOD,META,AMD,RIOT,RKLB,ASTS --from 2016-01-01 --to 2026-06-28 --max-expirations 80 --cache-only --fetch-concurrency 4 --symbol-concurrency 4 --profile-family weekly-put-debit
cargo run --quiet -- research-weekly-universe --symbols SOFI,HOOD,META,AMD,RIOT,RKLB,ASTS --from 2016-01-01 --to 2026-06-28 --max-expirations 80 --cache-only --fetch-concurrency 4 --symbol-concurrency 4 --profile-family weekly-call-debit
```

Explain weekly signal sparsity or negative expectancy for a symbol/profile family:

```sh
cargo run --quiet -- audit-weekly-signal-gates --symbol ORCL --from 2016-01-01 --to 2026-06-28 --cache-only --profile-family weekly-call-debit
cargo run --quiet -- audit-weekly-signal-gates --symbol SOFI --from 2016-01-01 --to 2026-06-28 --max-expirations 80 --cache-only --profile-family weekly
```

Regenerate the `GME/F/BAC/CCL` diagnostic:

```sh
cargo run --quiet -- research-portfolio-selector --symbols GME,F,BAC,CCL --from 2016-01-01 --to 2026-06-28 --max-expirations 10 --cache-only --fetch-concurrency 4 --symbol-concurrency 4 --capital-budget 100000 --max-symbol-allocation-pct 0.20 --max-open-positions 4 --max-positions-per-symbol 2 --symbol-drawdown-cooldown-trigger-pct 0.10 --symbol-drawdown-cooldown-days 20
```

Regenerate the focused F/BAC diagnostic:

```sh
cargo run --quiet -- research-portfolio-selector --symbols F,BAC --from 2016-01-01 --to 2026-06-28 --max-expirations 20 --cache-only --fetch-concurrency 4 --symbol-concurrency 2 --capital-budget 100000 --max-symbol-allocation-pct 0.20 --max-open-positions 4 --max-positions-per-symbol 2 --symbol-drawdown-cooldown-trigger-pct 0.10 --symbol-drawdown-cooldown-days 20
```

Export a frozen canary artifact from a selector run:

```sh
cargo run --quiet -- export-portfolio-canary --run runs/<selector-run> --output candidates/weekly_selector_canary.json --candidate-id weekly_selector_canary_20260628 --frozen-on 2026-06-28
```

Check current canary status:

```sh
cargo run --quiet -- portfolio-canary-status --candidate candidates/weekly_selector_canary.json
```

Require an actionable signal before any canary action:

```sh
cargo run --quiet -- portfolio-canary-status --candidate candidates/weekly_selector_canary.json --as-of 2026-06-28 --require-action
```

## Action Policy

Do not force a trade from a stale artifact. A canary action requires a freshly regenerated artifact whose `latest_actions` includes at least one `entry_candidate` on the `--as-of` date or an `open_candidate` whose entry/exit window still spans the `--as-of` date.

The current frozen artifact has a same-report-date CRWV wheel `entry_candidate` as of `2026-06-26`, but it is stale as of `2026-06-28`; `--as-of 2026-06-28 --require-action` fails closed.

## Promotion Policy

Do not promote or canary-trade the previous frozen artifact. A new canary candidate requires all of the following:

- Capital drawdown, including a friction-stressed view, is within the canary risk budget.
- TSLA PnL concentration is materially lower.
- More symbol ablations pass.
- More strategy-sleeve ablations pass.
- Active short-premium sleeves are independently positive after friction.
- Inventory-wheel profiles avoid `inventory_risk_high`.
- Fresh data produces an actionable `entry_candidate` or `open_candidate`.

Until then, the weekly selector remains a research lane with no current canary-ready deployment candidate.
