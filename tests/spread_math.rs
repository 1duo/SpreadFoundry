use chrono::{TimeZone, Utc};
use rust_decimal_macros::dec;
use spreadfoundry::fixture;
use spreadfoundry::sim::{ExitRules, choose_exit, conservative_entry_credit, simulate_expiration};
use spreadfoundry::strategy::{CandidateFilters, generate_put_spread_candidates};
use spreadfoundry::types::{DataValidationError, ExitReason};

fn first_candidate() -> spreadfoundry::types::CandidateSpread {
    let snapshots = fixture::nvda_put_snapshots();
    let (candidates, _) = generate_put_spread_candidates(
        &snapshots,
        fixture::nvda_decision_ts(),
        &CandidateFilters::default(),
    )
    .unwrap();
    candidates
        .into_iter()
        .find(|candidate| {
            candidate.short_put.key.strike == dec!(200)
                && candidate.long_put.key.strike == dec!(190)
        })
        .unwrap()
}

#[test]
fn spread_max_profit_and_loss_are_bounded() {
    let candidate = first_candidate();
    assert_eq!(candidate.width, dec!(10));
    assert_eq!(candidate.credit, dec!(2.00));
    assert_eq!(candidate.max_profit(1), dec!(200.00));
    assert_eq!(candidate.max_loss(1), dec!(800.00));
}

#[test]
fn conservative_entry_uses_short_bid_minus_long_ask() {
    let candidate = first_candidate();
    assert_eq!(conservative_entry_credit(&candidate), dec!(2.00));
}

#[test]
fn expiration_pnl_handles_otm_partial_and_max_loss() {
    let candidate = first_candidate();
    let expiration_ts = Utc.with_ymd_and_hms(2026, 7, 24, 20, 0, 0).unwrap();

    let otm = simulate_expiration(&candidate, expiration_ts, dec!(205), 1, dec!(0));
    assert_eq!(otm.pnl, dec!(200.00));

    let partial = simulate_expiration(&candidate, expiration_ts, dec!(195), 1, dec!(0));
    assert_eq!(partial.pnl, dec!(-300.00));

    let max_loss = simulate_expiration(&candidate, expiration_ts, dec!(185), 1, dec!(0));
    assert_eq!(max_loss.pnl, dec!(-800.00));
}

#[test]
fn choose_exit_detects_take_profit_and_stop_loss() {
    let candidate = first_candidate();
    let rules = ExitRules::default();

    let take_profit = choose_exit(
        &candidate,
        &fixture::nvda_exit_quotes_take_profit(),
        &rules,
        1,
        dec!(0),
    )
    .unwrap();
    assert_eq!(take_profit.exit_reason, ExitReason::TakeProfit);

    let stop_loss = choose_exit(
        &candidate,
        &fixture::nvda_exit_quotes_stop_loss(),
        &rules,
        1,
        dec!(0),
    )
    .unwrap();
    assert_eq!(stop_loss.exit_reason, ExitReason::StopLoss);
}

#[test]
fn exit_quotes_must_match_candidate_legs() {
    let snapshots = fixture::nvda_put_snapshots();
    let (candidates, _) = generate_put_spread_candidates(
        &snapshots,
        fixture::nvda_decision_ts(),
        &CandidateFilters::default(),
    )
    .unwrap();
    let mismatched = candidates
        .into_iter()
        .find(|candidate| {
            candidate.short_put.key.strike == dec!(200)
                && candidate.long_put.key.strike == dec!(195)
        })
        .unwrap();

    let trade = choose_exit(
        &mismatched,
        &fixture::nvda_exit_quotes_take_profit(),
        &ExitRules::default(),
        1,
        dec!(0),
    );
    assert!(trade.is_none());
}

#[test]
fn lookahead_greeks_are_rejected() {
    let mut snapshots = fixture::nvda_put_snapshots();
    snapshots[0].greeks.ts = Utc.with_ymd_and_hms(2026, 6, 21, 16, 0, 0).unwrap();
    let err = generate_put_spread_candidates(
        &snapshots,
        fixture::nvda_decision_ts(),
        &CandidateFilters::default(),
    )
    .unwrap_err();
    assert!(matches!(err, DataValidationError::LookaheadFeature { .. }));
}

#[test]
fn lookahead_quotes_are_rejected() {
    let mut snapshots = fixture::nvda_put_snapshots();
    snapshots[0].quote.ts = Utc.with_ymd_and_hms(2026, 6, 21, 16, 0, 0).unwrap();
    assert!(
        snapshots[0]
            .quote
            .is_stale_for(fixture::nvda_decision_ts(), 120)
    );
    let err = generate_put_spread_candidates(
        &snapshots,
        fixture::nvda_decision_ts(),
        &CandidateFilters::default(),
    )
    .unwrap_err();
    assert!(matches!(err, DataValidationError::LookaheadQuote { .. }));
}
