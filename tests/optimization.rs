use chrono::{TimeZone, Utc};
use rust_decimal_macros::dec;
use spreadfoundry::opt::{OptimizationResult, OptimizationScore, rank_results, score_trades};
use spreadfoundry::types::{ExitReason, OptionKey, OptionRight, SimTrade, StrategyKind};

#[test]
fn empty_optimization_cells_are_finite_and_ineligible() {
    let score = score_trades(&[], 0.0);

    assert!(!score.eligible);
    assert_eq!(score.trades, 0);
    assert!(score.score.is_finite());
    assert!(score.score < -999_999.0);
}

#[test]
fn rank_results_places_empty_cells_after_eligible_results() {
    let ranked = rank_results(vec![
        OptimizationResult {
            params: "empty",
            score: score_trades(&[], 0.0),
        },
        OptimizationResult {
            params: "eligible",
            score: OptimizationScore {
                eligible: true,
                trades: 3,
                mean_return_on_risk: 0.1,
                cvar_95_loss: 0.0,
                max_drawdown: 0.0,
                slippage_penalty: 0.0,
                score: 0.1,
            },
        },
    ]);

    assert_eq!(ranked[0].params, "eligible");
    assert_eq!(ranked[1].params, "empty");
}

#[test]
fn drawdown_scoring_uses_trade_chronology() {
    let key = OptionKey::new(
        "NVDA",
        chrono::NaiveDate::from_ymd_opt(2026, 7, 24).unwrap(),
        dec!(200),
        OptionRight::Put,
    );
    let trades = vec![
        trade(&key, 2, dec!(100)),
        trade(&key, 3, dec!(-10)),
        trade(&key, 1, dec!(-50)),
    ];

    let score = score_trades(&trades, 0.0);

    assert!((score.max_drawdown - (50.0 / 300.0)).abs() < 0.000001);
}

fn trade(key: &OptionKey, day: u32, pnl: rust_decimal::Decimal) -> SimTrade {
    SimTrade {
        strategy: StrategyKind::PutSpread,
        entry_ts: Utc.with_ymd_and_hms(2026, 6, day, 14, 30, 0).unwrap(),
        exit_ts: Utc.with_ymd_and_hms(2026, 6, day, 20, 0, 0).unwrap(),
        short_put: key.clone(),
        long_put: key.clone(),
        quantity: 1,
        entry_credit: dec!(1),
        exit_debit: dec!(1),
        max_profit: dec!(100),
        max_loss: dec!(100),
        pnl,
        return_on_risk: pnl / dec!(100),
        exit_reason: ExitReason::ForceClose,
        fill_model: "test".to_owned(),
    }
}
