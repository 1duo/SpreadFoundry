use rust_decimal_macros::dec;
use spreadfoundry::fixture;
use spreadfoundry::strategy::{CandidateFilters, generate_put_spread_candidates};

#[test]
fn fixture_generates_stable_put_spread_candidates() {
    let snapshots = fixture::nvda_put_snapshots();
    let (candidates, report) = generate_put_spread_candidates(
        &snapshots,
        fixture::nvda_decision_ts(),
        &CandidateFilters::default(),
    )
    .unwrap();

    assert_eq!(report.input_snapshots, 5);
    assert_eq!(report.valid_puts, 3);
    assert_eq!(candidates.len(), 2);
    assert_eq!(candidates[0].short_put.key.strike, dec!(200));
    assert_eq!(candidates[0].long_put.key.strike, dec!(195));
    assert!(candidates.iter().any(|candidate| {
        candidate.short_put.key.strike == dec!(200) && candidate.long_put.key.strike == dec!(190)
    }));
}
