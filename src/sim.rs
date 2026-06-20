use crate::types::{
    CandidateSpread, ExitReason, OptionKey, OptionQuote, SimTrade, StrategyKind,
    contract_multiplier,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExitRules {
    pub profit_take: Decimal,
    pub stop_loss_multiple: Decimal,
    pub force_close_dte: i64,
}

impl Default for ExitRules {
    fn default() -> Self {
        Self {
            profit_take: Decimal::new(50, 2),
            stop_loss_multiple: Decimal::new(2, 0),
            force_close_dte: 21,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SpreadExitQuote {
    pub ts: DateTime<Utc>,
    pub short_key: OptionKey,
    pub short_quote: OptionQuote,
    pub long_key: OptionKey,
    pub long_quote: OptionQuote,
}

impl SpreadExitQuote {
    pub fn matches_candidate(&self, candidate: &CandidateSpread) -> bool {
        self.short_key == candidate.short_put.key && self.long_key == candidate.long_put.key
    }
}

pub fn conservative_entry_credit(candidate: &CandidateSpread) -> Decimal {
    candidate.short_put.quote.bid - candidate.long_put.quote.ask
}

pub fn conservative_exit_debit(
    short_exit: &OptionQuote,
    long_exit: &OptionQuote,
    width: Decimal,
) -> Decimal {
    let debit = short_exit.ask - long_exit.bid;
    debit.max(Decimal::ZERO).min(width)
}

pub fn simulate_quote_exit(
    candidate: &CandidateSpread,
    exit_quote: &SpreadExitQuote,
    reason: ExitReason,
    quantity: u32,
    fees: Decimal,
) -> SimTrade {
    let exit_debit = conservative_exit_debit(
        &exit_quote.short_quote,
        &exit_quote.long_quote,
        candidate.width,
    );
    build_trade(
        candidate,
        exit_quote.ts,
        exit_debit,
        reason,
        quantity,
        fees,
        "conservative_bid_ask",
    )
}

pub fn simulate_expiration(
    candidate: &CandidateSpread,
    expiration_ts: DateTime<Utc>,
    underlying_close: Decimal,
    quantity: u32,
    fees: Decimal,
) -> SimTrade {
    let short_intrinsic = (candidate.short_put.key.strike - underlying_close).max(Decimal::ZERO);
    let long_intrinsic = (candidate.long_put.key.strike - underlying_close).max(Decimal::ZERO);
    let exit_debit = (short_intrinsic - long_intrinsic)
        .max(Decimal::ZERO)
        .min(candidate.width);
    build_trade(
        candidate,
        expiration_ts,
        exit_debit,
        ExitReason::Expiration,
        quantity,
        fees,
        "expiration_intrinsic",
    )
}

pub fn choose_exit(
    candidate: &CandidateSpread,
    exit_quotes: &[SpreadExitQuote],
    rules: &ExitRules,
    quantity: u32,
    fees: Decimal,
) -> Option<SimTrade> {
    let entry_credit = conservative_entry_credit(candidate);
    let take_profit_debit = entry_credit * (Decimal::ONE - rules.profit_take);
    let stop_debit = entry_credit * rules.stop_loss_multiple;

    for quote in exit_quotes {
        if !quote.matches_candidate(candidate) {
            continue;
        }
        let debit = conservative_exit_debit(&quote.short_quote, &quote.long_quote, candidate.width);
        if debit >= stop_debit {
            return Some(simulate_quote_exit(
                candidate,
                quote,
                ExitReason::StopLoss,
                quantity,
                fees,
            ));
        }
        if debit <= take_profit_debit {
            return Some(simulate_quote_exit(
                candidate,
                quote,
                ExitReason::TakeProfit,
                quantity,
                fees,
            ));
        }
        let dte = candidate
            .short_put
            .key
            .expiration
            .signed_duration_since(quote.ts.date_naive())
            .num_days();
        if dte <= rules.force_close_dte {
            return Some(simulate_quote_exit(
                candidate,
                quote,
                ExitReason::ForceClose,
                quantity,
                fees,
            ));
        }
    }
    None
}

fn build_trade(
    candidate: &CandidateSpread,
    exit_ts: DateTime<Utc>,
    exit_debit: Decimal,
    reason: ExitReason,
    quantity: u32,
    fees: Decimal,
    fill_model: impl Into<String>,
) -> SimTrade {
    let entry_credit = conservative_entry_credit(candidate);
    let gross = (entry_credit - exit_debit) * contract_multiplier() * Decimal::from(quantity);
    let pnl = gross - fees.abs();
    let max_profit = candidate.max_profit(quantity) - fees.abs();
    let max_loss = candidate.max_loss(quantity) + fees.abs();
    let bounded_pnl = pnl.max(-max_loss).min(max_profit);
    let return_on_risk = if max_loss == Decimal::ZERO {
        Decimal::ZERO
    } else {
        bounded_pnl / max_loss
    };

    SimTrade {
        strategy: StrategyKind::PutSpread,
        entry_ts: candidate.decision_ts,
        exit_ts,
        short_put: candidate.short_put.key.clone(),
        long_put: candidate.long_put.key.clone(),
        quantity,
        entry_credit,
        exit_debit,
        max_profit,
        max_loss,
        pnl: bounded_pnl,
        return_on_risk,
        exit_reason: reason,
        fill_model: fill_model.into(),
    }
}
