use crate::sim::SpreadExitQuote;
use crate::types::{
    OptionGreeks, OptionKey, OptionQuote, OptionRight, OptionSnapshot, QuoteSource,
};
use chrono::{TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

pub fn nvda_decision_ts() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 6, 18, 16, 0, 0).unwrap()
}

pub fn nvda_put_snapshots() -> Vec<OptionSnapshot> {
    let decision_ts = nvda_decision_ts();
    let expiration = chrono::NaiveDate::from_ymd_opt(2026, 7, 24).unwrap();
    vec![
        put_snapshot(
            expiration,
            dec!(200),
            dec!(-0.28),
            dec!(4.00),
            dec!(4.15),
            2400,
            decision_ts,
        ),
        put_snapshot(
            expiration,
            dec!(195),
            dec!(-0.22),
            dec!(2.70),
            dec!(2.85),
            1400,
            decision_ts,
        ),
        put_snapshot(
            expiration,
            dec!(190),
            dec!(-0.17),
            dec!(1.88),
            dec!(2.00),
            1100,
            decision_ts,
        ),
        put_snapshot(
            expiration,
            dec!(185),
            dec!(-0.13),
            dec!(1.24),
            dec!(1.40),
            700,
            decision_ts,
        ),
        put_snapshot(
            expiration,
            dec!(180),
            dec!(-0.10),
            dec!(0.82),
            dec!(0.96),
            600,
            decision_ts,
        ),
    ]
}

pub fn nvda_exit_quotes_take_profit() -> Vec<SpreadExitQuote> {
    let exit_ts = Utc.with_ymd_and_hms(2026, 6, 19, 16, 0, 0).unwrap();
    let (short_key, long_key) = nvda_200_190_keys();
    vec![SpreadExitQuote {
        ts: exit_ts,
        short_key,
        short_quote: quote(exit_ts, dec!(1.70), dec!(1.82)),
        long_key,
        long_quote: quote(exit_ts, dec!(0.86), dec!(0.94)),
    }]
}

pub fn nvda_exit_quotes_stop_loss() -> Vec<SpreadExitQuote> {
    let exit_ts = Utc.with_ymd_and_hms(2026, 6, 19, 16, 0, 0).unwrap();
    let (short_key, long_key) = nvda_200_190_keys();
    vec![SpreadExitQuote {
        ts: exit_ts,
        short_key,
        short_quote: quote(exit_ts, dec!(7.40), dec!(7.70)),
        long_key,
        long_quote: quote(exit_ts, dec!(3.20), dec!(3.45)),
    }]
}

fn nvda_200_190_keys() -> (OptionKey, OptionKey) {
    let expiration = chrono::NaiveDate::from_ymd_opt(2026, 7, 24).unwrap();
    (
        OptionKey::new("NVDA", expiration, dec!(200), OptionRight::Put),
        OptionKey::new("NVDA", expiration, dec!(190), OptionRight::Put),
    )
}

fn put_snapshot(
    expiration: chrono::NaiveDate,
    strike: Decimal,
    delta: Decimal,
    bid: Decimal,
    ask: Decimal,
    open_interest: u32,
    ts: chrono::DateTime<Utc>,
) -> OptionSnapshot {
    OptionSnapshot {
        key: OptionKey::new("NVDA", expiration, strike, OptionRight::Put),
        quote: quote(ts, bid, ask),
        greeks: OptionGreeks {
            ts,
            delta,
            gamma: dec!(0.01),
            theta: dec!(-0.05),
            vega: dec!(0.18),
            rho: dec!(-0.04),
            iv: dec!(0.52),
            underlying_price: dec!(210.69),
        },
        open_interest,
        volume: 1200,
    }
}

fn quote(ts: chrono::DateTime<Utc>, bid: Decimal, ask: Decimal) -> OptionQuote {
    OptionQuote {
        ts,
        bid,
        ask,
        bid_size: 20,
        ask_size: 22,
        bid_exchange: Some("fixture".to_owned()),
        ask_exchange: Some("fixture".to_owned()),
        bid_condition: None,
        ask_condition: None,
        source: QuoteSource::Fixture,
    }
}
