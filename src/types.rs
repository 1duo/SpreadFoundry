use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use std::fmt;

pub fn contract_multiplier() -> Decimal {
    Decimal::new(100, 0)
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OptionRight {
    Call,
    Put,
}

impl fmt::Display for OptionRight {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OptionRight::Call => write!(f, "call"),
            OptionRight::Put => write!(f, "put"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct OptionKey {
    pub underlying: String,
    pub expiration: NaiveDate,
    pub strike: Decimal,
    pub right: OptionRight,
}

impl OptionKey {
    pub fn new(
        underlying: impl Into<String>,
        expiration: NaiveDate,
        strike: Decimal,
        right: OptionRight,
    ) -> Self {
        Self {
            underlying: underlying.into(),
            expiration,
            strike,
            right,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuoteSource {
    Fixture,
    ThetaHistory,
    ThetaLive,
    RobinhoodQuote,
    RobinhoodReview,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OptionQuote {
    pub ts: DateTime<Utc>,
    pub bid: Decimal,
    pub ask: Decimal,
    pub bid_size: u32,
    pub ask_size: u32,
    pub bid_exchange: Option<String>,
    pub ask_exchange: Option<String>,
    pub bid_condition: Option<String>,
    pub ask_condition: Option<String>,
    pub source: QuoteSource,
}

impl OptionQuote {
    pub fn validate(&self) -> Result<(), DataValidationError> {
        if self.bid < Decimal::ZERO {
            return Err(DataValidationError::NegativeBid(self.bid));
        }
        if self.ask < Decimal::ZERO {
            return Err(DataValidationError::NegativeAsk(self.ask));
        }
        if self.ask < self.bid {
            return Err(DataValidationError::AskBelowBid {
                bid: self.bid,
                ask: self.ask,
            });
        }
        Ok(())
    }

    pub fn mid(&self) -> Decimal {
        (self.bid + self.ask) / Decimal::new(2, 0)
    }

    pub fn spread_width(&self) -> Decimal {
        self.ask - self.bid
    }

    pub fn is_stale_for(&self, decision_ts: DateTime<Utc>, max_age_secs: i64) -> bool {
        if self.ts > decision_ts {
            return true;
        }
        let age = decision_ts
            .signed_duration_since(self.ts)
            .num_seconds()
            .abs();
        age > max_age_secs
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OptionGreeks {
    pub ts: DateTime<Utc>,
    pub delta: Decimal,
    pub gamma: Decimal,
    pub theta: Decimal,
    pub vega: Decimal,
    pub rho: Decimal,
    pub iv: Decimal,
    pub underlying_price: Decimal,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpenInterest {
    pub date: NaiveDate,
    pub option_key: OptionKey,
    pub open_interest: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OptionSnapshot {
    pub key: OptionKey,
    pub quote: OptionQuote,
    pub greeks: OptionGreeks,
    pub open_interest: u32,
    pub volume: u32,
}

impl OptionSnapshot {
    pub fn validate_for_decision(
        &self,
        decision_ts: DateTime<Utc>,
        max_quote_age_secs: i64,
    ) -> Result<(), DataValidationError> {
        self.quote.validate()?;
        if self.quote.ts > decision_ts {
            return Err(DataValidationError::LookaheadQuote {
                quote_ts: self.quote.ts,
                decision_ts,
            });
        }
        if self.quote.is_stale_for(decision_ts, max_quote_age_secs) {
            return Err(DataValidationError::StaleQuote {
                quote_ts: self.quote.ts,
                decision_ts,
                max_age_secs: max_quote_age_secs,
            });
        }
        if self.greeks.ts > decision_ts {
            return Err(DataValidationError::LookaheadFeature {
                feature_ts: self.greeks.ts,
                decision_ts,
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CandidateSpread {
    pub decision_ts: DateTime<Utc>,
    pub short_put: OptionSnapshot,
    pub long_put: OptionSnapshot,
    pub dte: i64,
    pub width: Decimal,
    pub credit: Decimal,
    pub credit_width_ratio: Decimal,
}

impl CandidateSpread {
    pub fn validate(&self) -> Result<(), DataValidationError> {
        if self.short_put.key.right != OptionRight::Put
            || self.long_put.key.right != OptionRight::Put
        {
            return Err(DataValidationError::UnsupportedSpread);
        }
        if self.short_put.key.expiration != self.long_put.key.expiration {
            return Err(DataValidationError::MismatchedExpiration);
        }
        let expected_width = self.short_put.key.strike - self.long_put.key.strike;
        if expected_width <= Decimal::ZERO {
            return Err(DataValidationError::NonPositiveWidth(expected_width));
        }
        if self.width != expected_width {
            return Err(DataValidationError::MismatchedWidth {
                expected: expected_width,
                actual: self.width,
            });
        }
        if self.width <= Decimal::ZERO {
            return Err(DataValidationError::NonPositiveWidth(self.width));
        }
        let expected_credit = self.short_put.quote.bid - self.long_put.quote.ask;
        if self.credit != expected_credit {
            return Err(DataValidationError::MismatchedCredit {
                expected: expected_credit,
                actual: self.credit,
            });
        }
        if self.credit <= Decimal::ZERO {
            return Err(DataValidationError::NonPositiveCredit(self.credit));
        }
        if self.credit >= self.width {
            return Err(DataValidationError::CreditExceedsWidth {
                credit: self.credit,
                width: self.width,
            });
        }
        self.short_put
            .validate_for_decision(self.decision_ts, i64::MAX)?;
        self.long_put
            .validate_for_decision(self.decision_ts, i64::MAX)?;
        Ok(())
    }

    pub fn max_profit(&self, quantity: u32) -> Decimal {
        self.credit * contract_multiplier() * Decimal::from(quantity)
    }

    pub fn max_loss(&self, quantity: u32) -> Decimal {
        (self.width - self.credit) * contract_multiplier() * Decimal::from(quantity)
    }

    pub fn return_on_risk(&self, pnl: Decimal, quantity: u32) -> Decimal {
        let max_loss = self.max_loss(quantity);
        if max_loss == Decimal::ZERO {
            Decimal::ZERO
        } else {
            pnl / max_loss
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StrategyKind {
    PutSpread,
    CashSecuredPut,
    CoveredCall,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LegFill {
    pub key: OptionKey,
    pub side: OrderSide,
    pub price: Decimal,
    pub quantity: u32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExitReason {
    TakeProfit,
    StopLoss,
    ForceClose,
    Expiration,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SimTrade {
    pub strategy: StrategyKind,
    pub entry_ts: DateTime<Utc>,
    pub exit_ts: DateTime<Utc>,
    pub short_put: OptionKey,
    pub long_put: OptionKey,
    pub quantity: u32,
    pub entry_credit: Decimal,
    pub exit_debit: Decimal,
    pub max_profit: Decimal,
    pub max_loss: Decimal,
    pub pnl: Decimal,
    pub return_on_risk: Decimal,
    pub exit_reason: ExitReason,
    pub fill_model: String,
}

impl SimTrade {
    pub fn pnl_f64(&self) -> f64 {
        self.pnl
            .to_f64()
            .expect("rust_decimal PnL should fit into f64 scoring range")
    }

    pub fn return_on_risk_f64(&self) -> f64 {
        self.return_on_risk
            .to_f64()
            .expect("rust_decimal return on risk should fit into f64 scoring range")
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DataValidationError {
    #[error("bid is negative: {0}")]
    NegativeBid(Decimal),
    #[error("ask is negative: {0}")]
    NegativeAsk(Decimal),
    #[error("ask is below bid: bid={bid}, ask={ask}")]
    AskBelowBid { bid: Decimal, ask: Decimal },
    #[error(
        "quote is stale: quote_ts={quote_ts}, decision_ts={decision_ts}, max_age_secs={max_age_secs}"
    )]
    StaleQuote {
        quote_ts: DateTime<Utc>,
        decision_ts: DateTime<Utc>,
        max_age_secs: i64,
    },
    #[error(
        "feature timestamp is after decision timestamp: feature_ts={feature_ts}, decision_ts={decision_ts}"
    )]
    LookaheadFeature {
        feature_ts: DateTime<Utc>,
        decision_ts: DateTime<Utc>,
    },
    #[error(
        "quote timestamp is after decision timestamp: quote_ts={quote_ts}, decision_ts={decision_ts}"
    )]
    LookaheadQuote {
        quote_ts: DateTime<Utc>,
        decision_ts: DateTime<Utc>,
    },
    #[error("spread must contain put legs")]
    UnsupportedSpread,
    #[error("spread legs must share expiration")]
    MismatchedExpiration,
    #[error("spread width does not match strikes: expected={expected}, actual={actual}")]
    MismatchedWidth { expected: Decimal, actual: Decimal },
    #[error(
        "spread credit does not match conservative entry quote: expected={expected}, actual={actual}"
    )]
    MismatchedCredit { expected: Decimal, actual: Decimal },
    #[error("spread width must be positive: {0}")]
    NonPositiveWidth(Decimal),
    #[error("spread credit must be positive: {0}")]
    NonPositiveCredit(Decimal),
    #[error("spread credit must be below width: credit={credit}, width={width}")]
    CreditExceedsWidth { credit: Decimal, width: Decimal },
}
