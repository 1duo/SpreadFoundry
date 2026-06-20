use crate::types::{CandidateSpread, DataValidationError, OptionRight, OptionSnapshot};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CandidateFilters {
    pub min_dte: i64,
    pub max_dte: i64,
    pub min_short_delta_abs: Decimal,
    pub max_short_delta_abs: Decimal,
    pub min_width: Decimal,
    pub max_width: Decimal,
    pub min_short_open_interest: u32,
    pub min_long_open_interest: u32,
    pub max_quote_width_pct_of_mid: Decimal,
    pub max_quote_width_abs: Decimal,
    pub min_credit_width_ratio: Decimal,
    pub max_quote_age_secs: i64,
}

impl Default for CandidateFilters {
    fn default() -> Self {
        Self {
            min_dte: 30,
            max_dte: 45,
            min_short_delta_abs: Decimal::new(15, 2),
            max_short_delta_abs: Decimal::new(35, 2),
            min_width: Decimal::new(5, 0),
            max_width: Decimal::new(20, 0),
            min_short_open_interest: 500,
            min_long_open_interest: 250,
            max_quote_width_pct_of_mid: Decimal::new(10, 2),
            max_quote_width_abs: Decimal::new(10, 2),
            min_credit_width_ratio: Decimal::new(20, 2),
            max_quote_age_secs: 120,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CandidateGenerationReport {
    pub input_snapshots: usize,
    pub valid_puts: usize,
    pub generated_candidates: usize,
    pub rejected_for_width: usize,
    pub rejected_for_credit: usize,
}

pub fn generate_put_spread_candidates(
    snapshots: &[OptionSnapshot],
    decision_ts: DateTime<Utc>,
    filters: &CandidateFilters,
) -> Result<(Vec<CandidateSpread>, CandidateGenerationReport), DataValidationError> {
    let mut report = CandidateGenerationReport {
        input_snapshots: snapshots.len(),
        ..CandidateGenerationReport::default()
    };

    let mut puts = Vec::new();
    for snapshot in snapshots {
        if snapshot.key.right != OptionRight::Put {
            continue;
        }
        let dte = snapshot
            .key
            .expiration
            .signed_duration_since(decision_ts.date_naive())
            .num_days();
        if dte < filters.min_dte || dte > filters.max_dte {
            continue;
        }
        snapshot.validate_for_decision(decision_ts, filters.max_quote_age_secs)?;
        if !quote_width_allowed(snapshot, filters) {
            continue;
        }
        puts.push(snapshot.clone());
    }

    report.valid_puts = puts.len();
    let mut candidates = Vec::new();

    for short_put in &puts {
        let short_delta_abs = short_put.greeks.delta.abs();
        if short_delta_abs < filters.min_short_delta_abs
            || short_delta_abs > filters.max_short_delta_abs
            || short_put.open_interest < filters.min_short_open_interest
        {
            continue;
        }

        for long_put in &puts {
            if short_put.key.expiration != long_put.key.expiration {
                continue;
            }
            if long_put.key.strike >= short_put.key.strike {
                continue;
            }
            if long_put.open_interest < filters.min_long_open_interest {
                continue;
            }

            let width = short_put.key.strike - long_put.key.strike;
            if width < filters.min_width || width > filters.max_width {
                report.rejected_for_width += 1;
                continue;
            }

            let credit = short_put.quote.bid - long_put.quote.ask;
            if credit <= Decimal::ZERO {
                report.rejected_for_credit += 1;
                continue;
            }
            let credit_width_ratio = credit / width;
            if credit_width_ratio < filters.min_credit_width_ratio {
                report.rejected_for_credit += 1;
                continue;
            }

            let dte = short_put
                .key
                .expiration
                .signed_duration_since(decision_ts.date_naive())
                .num_days();
            let candidate = CandidateSpread {
                decision_ts,
                short_put: short_put.clone(),
                long_put: long_put.clone(),
                dte,
                width,
                credit,
                credit_width_ratio,
            };
            candidate.validate()?;
            candidates.push(candidate);
        }
    }

    candidates.sort_by(|a, b| {
        b.credit_width_ratio
            .cmp(&a.credit_width_ratio)
            .then_with(|| b.credit.cmp(&a.credit))
            .then_with(|| a.width.cmp(&b.width))
    });
    report.generated_candidates = candidates.len();
    Ok((candidates, report))
}

fn quote_width_allowed(snapshot: &OptionSnapshot, filters: &CandidateFilters) -> bool {
    let mid = snapshot.quote.mid();
    if mid <= Decimal::ZERO {
        return false;
    }
    let pct_limit = mid * filters.max_quote_width_pct_of_mid;
    let allowed = pct_limit.max(filters.max_quote_width_abs);
    snapshot.quote.spread_width() <= allowed
}
