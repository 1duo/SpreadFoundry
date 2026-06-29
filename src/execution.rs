use crate::types::{OptionKey, OptionQuote, OptionRight};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OptionOrderSide {
    Buy,
    Sell,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionEffect {
    Open,
    Close,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OptionOrderEffect {
    Credit,
    Debit,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    Day,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OptionOrderLeg {
    pub side: OptionOrderSide,
    pub position_effect: PositionEffect,
    pub key: OptionKey,
    pub quantity: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OptionOrderIntent {
    pub symbol: String,
    pub strategy: String,
    pub order_effect: OptionOrderEffect,
    pub limit_price: Decimal,
    pub time_in_force: TimeInForce,
    pub legs: Vec<OptionOrderLeg>,
}

impl OptionOrderIntent {
    pub fn quantity(&self) -> u32 {
        self.legs.first().map(|leg| leg.quantity).unwrap_or(0)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    #[error("order intent must contain at least one leg")]
    MissingLegs,
    #[error("order quantity must be positive")]
    NonPositiveQuantity,
    #[error("order limit price must be positive: {0}")]
    NonPositiveLimitPrice(Decimal),
    #[error("all order legs must use the same quantity")]
    MismatchedLegQuantity,
    #[error("all order legs must match the order symbol")]
    MismatchedLegSymbol,
    #[error("cash-secured put intent must be one sell-to-open put")]
    UnsupportedCashSecuredPut,
    #[error("debit spread intent must be two legs with the same symbol, expiration, and right")]
    UnsupportedDebitSpread,
}

pub fn conservative_credit_spread_entry_credit(
    short_quote: &OptionQuote,
    long_quote: &OptionQuote,
) -> Decimal {
    short_quote.bid - long_quote.ask
}

pub fn conservative_debit_spread_entry_debit(
    long_quote: &OptionQuote,
    short_quote: &OptionQuote,
) -> Decimal {
    (long_quote.ask - short_quote.bid).max(Decimal::ZERO)
}

pub fn conservative_short_spread_exit_debit(
    short_exit: &OptionQuote,
    long_exit: &OptionQuote,
    width: Decimal,
) -> Decimal {
    let debit = short_exit.ask - long_exit.bid;
    debit.max(Decimal::ZERO).min(width)
}

pub fn short_put_spread_expiration_debit(
    short_strike: Decimal,
    long_strike: Decimal,
    underlying_close: Decimal,
    width: Decimal,
) -> Decimal {
    let short_intrinsic = (short_strike - underlying_close).max(Decimal::ZERO);
    let long_intrinsic = (long_strike - underlying_close).max(Decimal::ZERO);
    (short_intrinsic - long_intrinsic)
        .max(Decimal::ZERO)
        .min(width)
}

pub fn cash_secured_put_open_intent(
    key: OptionKey,
    quantity: u32,
    limit_credit: Decimal,
    strategy: impl Into<String>,
) -> Result<OptionOrderIntent, ExecutionError> {
    let intent = OptionOrderIntent {
        symbol: key.underlying.clone(),
        strategy: strategy.into(),
        order_effect: OptionOrderEffect::Credit,
        limit_price: limit_credit,
        time_in_force: TimeInForce::Day,
        legs: vec![OptionOrderLeg {
            side: OptionOrderSide::Sell,
            position_effect: PositionEffect::Open,
            key,
            quantity,
        }],
    };
    validate_cash_secured_put_intent(&intent)?;
    Ok(intent)
}

pub fn debit_spread_open_intent(
    long_key: OptionKey,
    short_key: OptionKey,
    quantity: u32,
    limit_debit: Decimal,
    strategy: impl Into<String>,
) -> Result<OptionOrderIntent, ExecutionError> {
    let intent = OptionOrderIntent {
        symbol: long_key.underlying.clone(),
        strategy: strategy.into(),
        order_effect: OptionOrderEffect::Debit,
        limit_price: limit_debit,
        time_in_force: TimeInForce::Day,
        legs: vec![
            OptionOrderLeg {
                side: OptionOrderSide::Buy,
                position_effect: PositionEffect::Open,
                key: long_key,
                quantity,
            },
            OptionOrderLeg {
                side: OptionOrderSide::Sell,
                position_effect: PositionEffect::Open,
                key: short_key,
                quantity,
            },
        ],
    };
    validate_debit_spread_intent(&intent)?;
    Ok(intent)
}

fn validate_common_intent(intent: &OptionOrderIntent) -> Result<(), ExecutionError> {
    if intent.legs.is_empty() {
        return Err(ExecutionError::MissingLegs);
    }
    if intent.quantity() == 0 {
        return Err(ExecutionError::NonPositiveQuantity);
    }
    if intent.limit_price <= Decimal::ZERO {
        return Err(ExecutionError::NonPositiveLimitPrice(intent.limit_price));
    }
    if intent
        .legs
        .iter()
        .any(|leg| leg.quantity != intent.quantity())
    {
        return Err(ExecutionError::MismatchedLegQuantity);
    }
    if intent
        .legs
        .iter()
        .any(|leg| leg.key.underlying != intent.symbol)
    {
        return Err(ExecutionError::MismatchedLegSymbol);
    }
    Ok(())
}

fn validate_cash_secured_put_intent(intent: &OptionOrderIntent) -> Result<(), ExecutionError> {
    validate_common_intent(intent)?;
    let [leg] = intent.legs.as_slice() else {
        return Err(ExecutionError::UnsupportedCashSecuredPut);
    };
    if intent.order_effect != OptionOrderEffect::Credit
        || leg.side != OptionOrderSide::Sell
        || leg.position_effect != PositionEffect::Open
        || leg.key.right != OptionRight::Put
    {
        return Err(ExecutionError::UnsupportedCashSecuredPut);
    }
    Ok(())
}

fn validate_debit_spread_intent(intent: &OptionOrderIntent) -> Result<(), ExecutionError> {
    validate_common_intent(intent)?;
    let [long_leg, short_leg] = intent.legs.as_slice() else {
        return Err(ExecutionError::UnsupportedDebitSpread);
    };
    if intent.order_effect != OptionOrderEffect::Debit
        || long_leg.side != OptionOrderSide::Buy
        || short_leg.side != OptionOrderSide::Sell
        || long_leg.position_effect != PositionEffect::Open
        || short_leg.position_effect != PositionEffect::Open
        || long_leg.key.expiration != short_leg.key.expiration
        || long_leg.key.right != short_leg.key.right
    {
        return Err(ExecutionError::UnsupportedDebitSpread);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{OptionRight, QuoteSource};
    use chrono::{TimeZone, Utc};
    use rust_decimal_macros::dec;

    fn quote(bid: Decimal, ask: Decimal) -> OptionQuote {
        OptionQuote {
            ts: Utc.with_ymd_and_hms(2026, 6, 29, 15, 0, 0).unwrap(),
            bid,
            ask,
            bid_size: 10,
            ask_size: 10,
            bid_exchange: None,
            ask_exchange: None,
            bid_condition: None,
            ask_condition: None,
            source: QuoteSource::Fixture,
        }
    }

    #[test]
    fn conservative_fill_math_matches_broker_side_of_book() {
        let short = quote(dec!(2.50), dec!(2.70));
        let long = quote(dec!(0.40), dec!(0.50));
        let debit_long = quote(dec!(5.10), dec!(5.30));
        let debit_short = quote(dec!(0.70), dec!(0.90));

        assert_eq!(
            conservative_credit_spread_entry_credit(&short, &long),
            dec!(2.00)
        );
        assert_eq!(
            conservative_debit_spread_entry_debit(&debit_long, &debit_short),
            dec!(4.60)
        );
        assert_eq!(
            conservative_short_spread_exit_debit(&short, &long, dec!(5.00)),
            dec!(2.30)
        );
    }

    #[test]
    fn expiration_debit_is_clamped_to_spread_width() {
        assert_eq!(
            short_put_spread_expiration_debit(dec!(200), dec!(190), dec!(185), dec!(10)),
            dec!(10)
        );
        assert_eq!(
            short_put_spread_expiration_debit(dec!(200), dec!(190), dec!(195), dec!(10)),
            dec!(5)
        );
        assert_eq!(
            short_put_spread_expiration_debit(dec!(200), dec!(190), dec!(205), dec!(10)),
            Decimal::ZERO
        );
    }

    #[test]
    fn debit_spread_intent_is_atomic_buy_then_sell() {
        let expiration = chrono::NaiveDate::from_ymd_opt(2026, 7, 2).unwrap();
        let long = OptionKey::new("ORCL", expiration, dec!(220), OptionRight::Call);
        let short = OptionKey::new("ORCL", expiration, dec!(225), OptionRight::Call);

        let intent = debit_spread_open_intent(
            long.clone(),
            short.clone(),
            1,
            dec!(4.50),
            "call_debit_spread",
        )
        .unwrap();

        assert_eq!(intent.symbol, "ORCL");
        assert_eq!(intent.order_effect, OptionOrderEffect::Debit);
        assert_eq!(intent.quantity(), 1);
        assert_eq!(intent.legs[0].side, OptionOrderSide::Buy);
        assert_eq!(intent.legs[0].key, long);
        assert_eq!(intent.legs[1].side, OptionOrderSide::Sell);
        assert_eq!(intent.legs[1].key, short);
    }

    #[test]
    fn debit_spread_intent_rejects_mismatched_rights() {
        let expiration = chrono::NaiveDate::from_ymd_opt(2026, 7, 2).unwrap();
        let long = OptionKey::new("ORCL", expiration, dec!(220), OptionRight::Call);
        let short = OptionKey::new("ORCL", expiration, dec!(225), OptionRight::Put);

        let err = debit_spread_open_intent(long, short, 1, dec!(4.50), "bad_spread").unwrap_err();

        assert!(matches!(err, ExecutionError::UnsupportedDebitSpread));
    }
}
