use crate::types::SimTrade;
use rayon::prelude::*;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OptimizationScore {
    pub eligible: bool,
    pub trades: usize,
    pub mean_return_on_risk: f64,
    pub cvar_95_loss: f64,
    pub max_drawdown: f64,
    pub slippage_penalty: f64,
    pub score: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OptimizationResult<T> {
    pub params: T,
    pub score: OptimizationScore,
}

pub fn score_trades(trades: &[SimTrade], slippage_penalty: f64) -> OptimizationScore {
    if trades.is_empty() {
        return OptimizationScore {
            eligible: false,
            trades: 0,
            mean_return_on_risk: 0.0,
            cvar_95_loss: 0.0,
            max_drawdown: 0.0,
            slippage_penalty,
            score: -1_000_000.0,
        };
    }

    let returns: Vec<f64> = trades.iter().map(SimTrade::return_on_risk_f64).collect();
    let mean_return_on_risk = returns.iter().sum::<f64>() / returns.len() as f64;
    let cvar_95_loss = cvar_95_loss(&returns);
    let max_drawdown = max_drawdown(trades);
    let score = mean_return_on_risk - 2.0 * cvar_95_loss - max_drawdown - slippage_penalty;

    OptimizationScore {
        eligible: true,
        trades: trades.len(),
        mean_return_on_risk,
        cvar_95_loss,
        max_drawdown,
        slippage_penalty,
        score,
    }
}

pub fn rank_results<T: Send + Clone>(
    results: Vec<OptimizationResult<T>>,
) -> Vec<OptimizationResult<T>> {
    let mut ranked = results;
    ranked.par_sort_by(|a, b| b.score.score.total_cmp(&a.score.score));
    ranked
}

fn cvar_95_loss(returns: &[f64]) -> f64 {
    let mut losses: Vec<f64> = returns
        .iter()
        .copied()
        .filter(|value| *value < 0.0)
        .map(f64::abs)
        .collect();
    if losses.is_empty() {
        return 0.0;
    }
    losses.sort_by(|a, b| b.total_cmp(a));
    let tail_count = ((losses.len() as f64) * 0.05).ceil().max(1.0) as usize;
    losses.iter().take(tail_count).sum::<f64>() / tail_count as f64
}

fn max_drawdown(trades: &[SimTrade]) -> f64 {
    let mut chronological = trades.iter().collect::<Vec<_>>();
    chronological.sort_by_key(|trade| trade.exit_ts);

    let mut equity = 0.0;
    let mut high_water = 0.0;
    let mut max_drawdown = 0.0;
    for trade in chronological {
        equity += trade.pnl_f64();
        if equity > high_water {
            high_water = equity;
        }
        let drawdown = high_water - equity;
        if drawdown > max_drawdown {
            max_drawdown = drawdown;
        }
    }
    let gross_risk: f64 = trades
        .iter()
        .map(|trade| {
            trade
                .max_loss
                .to_f64()
                .expect("rust_decimal max loss should fit into f64 scoring range")
        })
        .sum();
    if gross_risk == 0.0 {
        0.0
    } else {
        max_drawdown / gross_risk
    }
}
