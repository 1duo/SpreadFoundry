use anyhow::{Context, Result};
use chrono::{Datelike, Duration, NaiveDate, Utc};
use futures::future::join_all;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

#[cfg(test)]
use crate::execution::{
    OptionOrderEffect, OptionOrderIntent, OptionOrderSide, cash_secured_put_open_intent,
    credit_spread_open_intent, debit_spread_open_intent,
};
use crate::execution::{
    cash_secured_put_max_loss_per_share, conservative_credit_spread_entry_credit_f64,
    conservative_debit_spread_entry_debit_f64, conservative_long_spread_exit_credit_f64,
    conservative_short_spread_exit_debit_f64,
};
#[cfg(test)]
use crate::types::{OptionKey as ExecutionOptionKey, OptionRight as ExecutionOptionRight};
#[cfg(test)]
use rust_decimal::Decimal;

const FETCH_ATTEMPTS: usize = 3;
const MIN_RANKING_TRADES: usize = 10;
const MIN_RANKING_TRADES_PER_YEAR: f64 = 2.0;
const MIN_WEEKLY_RANKING_TRADES_PER_YEAR: f64 = 104.0;
const COST_STRESS_PER_TRADE: [f64; 3] = [5.0, 10.0, 25.0];
const PORTFOLIO_MAX_MATERIAL_NEGATIVE_YEAR_PCT_OF_CAPITAL: f64 = 0.01;
const WALK_FORWARD_MIN_TRAIN_DAYS: i64 = 365 * 3;
const ROLLING_WALK_FORWARD_TRAIN_DAYS: i64 = 365 * 4;
const WALK_FORWARD_SELECTION_DIAGNOSTIC_LIMIT: usize = 5;
const PLATEAU_MIN_PROFILE_VARIANTS: usize = 75;
const PLATEAU_MIN_WALK_FORWARD_YEARS: usize = 3;
const RECENT_TRAIN_ACTIVITY_DAYS: i64 = 365;
const MIN_DEPLOYABLE_TRAINING_ROBUST_SCORE: f64 = 0.005;
pub const DEFAULT_RESEARCH_FROM: &str = "2010-01-01";
pub const FROZEN_BASELINE_NAME: &str = "frozen_delta26_34_take45_weak13dd2to5_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_width15_lowdelta23_width10_credit20";
pub const DEFAULT_PLATEAU_UNIVERSE_SYMBOLS: [&str; 8] = [
    "TSLA", "AMD", "META", "AMZN", "AAPL", "MSFT", "GOOGL", "AVGO",
];
pub const DEFAULT_PLATEAU_UNIVERSE_SYMBOLS_CSV: &str = "TSLA,AMD,META,AMZN,AAPL,MSFT,GOOGL,AVGO";
pub const DEFAULT_WEEKLY_RESEARCH_SYMBOLS: [&str; 5] = ["IREN", "PLTR", "ORCL", "TSLA", "CRWV"];
pub const DEFAULT_WEEKLY_RESEARCH_SYMBOLS_CSV: &str = "IREN,PLTR,ORCL,TSLA,CRWV";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchProfileFamily {
    Swing,
    Weekly,
    WeeklyFarOtm,
    WeeklyPutDebit,
    WeeklyCallCredit,
    WeeklyCallDebit,
    WeeklyWheel,
}

impl Default for ResearchProfileFamily {
    fn default() -> Self {
        Self::Swing
    }
}

impl ResearchProfileFamily {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Swing => "swing",
            Self::Weekly => "weekly",
            Self::WeeklyFarOtm => "weekly_far_otm",
            Self::WeeklyPutDebit => "weekly_put_debit",
            Self::WeeklyCallCredit => "weekly_call_credit",
            Self::WeeklyCallDebit => "weekly_call_debit",
            Self::WeeklyWheel => "weekly_wheel",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpreadStructure {
    PutCreditSpread,
    CallCreditSpread,
    PutDebitSpread,
    CallDebitSpread,
    Wheel,
}

impl Default for SpreadStructure {
    fn default() -> Self {
        Self::PutCreditSpread
    }
}

impl SpreadStructure {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PutCreditSpread => "put_credit_spread",
            Self::CallCreditSpread => "call_credit_spread",
            Self::PutDebitSpread => "put_debit_spread",
            Self::CallDebitSpread => "call_debit_spread",
            Self::Wheel => "wheel",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum OptionRight {
    Put,
    Call,
}

impl OptionRight {
    fn query_value(self) -> &'static str {
        match self {
            Self::Put => "put",
            Self::Call => "call",
        }
    }

    fn cache_prefix(self) -> &'static str {
        match self {
            Self::Put => "research",
            Self::Call => "research_call",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OptionDataMode {
    Single(OptionRight),
    PutAndCall,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResearchRequest {
    pub symbol: String,
    #[serde(default)]
    pub profile_family: ResearchProfileFamily,
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub max_expirations: Option<usize>,
    pub fetch_concurrency: usize,
    pub force_refresh: bool,
    pub cache_only: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PortfolioWheelResearchRequest {
    pub symbols: Vec<String>,
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub max_expirations: Option<usize>,
    pub fetch_concurrency: usize,
    pub symbol_concurrency: usize,
    pub force_refresh: bool,
    pub cache_only: bool,
    pub capital_budget: f64,
    pub max_symbol_allocation_pct: f64,
    pub max_open_positions: usize,
    pub max_positions_per_symbol: usize,
    #[serde(default)]
    pub max_total_trades_per_symbol: Option<usize>,
    #[serde(default)]
    pub portfolio_drawdown_cooldown_trigger_pct: Option<f64>,
    #[serde(default)]
    pub portfolio_drawdown_cooldown_days: i64,
    #[serde(default)]
    pub symbol_drawdown_cooldown_trigger_pct: Option<f64>,
    #[serde(default)]
    pub symbol_drawdown_cooldown_days: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OptionCacheCoverageRequest {
    pub symbols: Vec<String>,
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub max_expirations: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OptionCacheCoverageReport {
    pub symbols: Vec<OptionCacheCoverageSymbol>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WarmOptionCacheCoverageRequest {
    pub symbols: Vec<String>,
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub max_expirations: Option<usize>,
    pub max_windows_per_symbol: usize,
    pub fetch_concurrency: usize,
    pub force_refresh: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WarmOptionCacheCoverageReport {
    pub symbols: Vec<WarmOptionCacheCoverageSymbol>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WeeklySignalGateAuditRequest {
    pub symbol: String,
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub max_expirations: Option<usize>,
    pub fetch_concurrency: usize,
    pub force_refresh: bool,
    pub cache_only: bool,
    pub profile_family: ResearchProfileFamily,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WeeklySignalGateAuditReport {
    pub symbol: String,
    pub profile_family: ResearchProfileFamily,
    pub requested_from: NaiveDate,
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub cache_only: bool,
    pub expirations_discovered: usize,
    pub expirations_audited: usize,
    pub expirations_loaded: usize,
    pub expirations_failed: usize,
    pub rows_loaded: usize,
    pub profiles: Vec<WeeklySignalGateProfileAudit>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WeeklySignalGateProfileAudit {
    pub profile: String,
    pub structure: SpreadStructure,
    pub dte_rows: usize,
    pub dte_entry_days: usize,
    pub primary_leg_passes: usize,
    pub regime_passes: usize,
    pub candidates: usize,
    pub candidate_entry_days: usize,
    pub simulated_trades: usize,
    pub trade_entry_days: usize,
    pub total_pnl: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WarmOptionCacheCoverageSymbol {
    pub symbol: String,
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub expirations_discovered: usize,
    pub expirations_audited: usize,
    pub windows_attempted: usize,
    pub windows_completed: usize,
    pub windows_failed: usize,
    pub windows: Vec<WarmOptionCacheCoverageWindow>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WarmOptionCacheCoverageWindow {
    pub expiration: NaiveDate,
    pub start: NaiveDate,
    pub end: NaiveDate,
    pub put_complete_before: bool,
    pub call_complete_before: bool,
    pub put_complete_after: bool,
    pub call_complete_after: bool,
    pub both_complete_after: bool,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OptionCacheCoverageSymbol {
    pub symbol: String,
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub expirations_discovered: usize,
    pub expirations_audited: usize,
    pub put_complete: usize,
    pub call_complete: usize,
    pub both_complete: usize,
    pub put_coverage_pct: f64,
    pub call_coverage_pct: f64,
    pub both_coverage_pct: f64,
    pub first_complete_call_expiration: Option<NaiveDate>,
    pub last_complete_call_expiration: Option<NaiveDate>,
    pub missing_call_examples: Vec<OptionCacheCoverageGap>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OptionCacheCoverageGap {
    pub expiration: NaiveDate,
    pub start: NaiveDate,
    pub end: NaiveDate,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PortfolioWheelReport {
    pub run_id: String,
    pub symbols: Vec<String>,
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub max_expirations: Option<usize>,
    pub fetch_concurrency: usize,
    pub symbol_concurrency: usize,
    pub force_refresh: bool,
    pub cache_only: bool,
    pub capital_budget: f64,
    pub max_symbol_allocation_pct: f64,
    pub max_open_positions: usize,
    pub max_positions_per_symbol: usize,
    pub max_total_trades_per_symbol: Option<usize>,
    #[serde(default)]
    pub portfolio_drawdown_cooldown_trigger_pct: Option<f64>,
    #[serde(default)]
    pub portfolio_drawdown_cooldown_days: i64,
    #[serde(default)]
    pub symbol_drawdown_cooldown_trigger_pct: Option<f64>,
    #[serde(default)]
    pub symbol_drawdown_cooldown_days: i64,
    pub symbols_loaded: Vec<PortfolioWheelLoadedSymbol>,
    pub profiles: Vec<PortfolioWheelProfileResult>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PortfolioWheelLoadedSymbol {
    pub symbol: String,
    pub requested_from: NaiveDate,
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub expirations_discovered: usize,
    pub expirations_skipped_before_data: usize,
    pub expirations_loaded: usize,
    #[serde(default)]
    pub put_expirations_loaded: usize,
    #[serde(default)]
    pub call_expirations_loaded: usize,
    pub rows_loaded: usize,
    pub expirations_failed: usize,
    pub expiration_load_failures: Vec<ExpirationLoadFailure>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PortfolioWheelProfileResult {
    pub profile: ResearchProfile,
    pub metrics: ResearchMetrics,
    pub trades: Vec<PortfolioWheelTrade>,
    pub candidates: usize,
    pub accepted: usize,
    pub rejected_capital_budget: usize,
    pub rejected_symbol_allocation: usize,
    pub rejected_open_positions: usize,
    pub rejected_symbol_positions: usize,
    #[serde(default)]
    pub rejected_symbol_total_trades: usize,
    #[serde(default)]
    pub rejected_portfolio_drawdown_cooldown: usize,
    #[serde(default)]
    pub rejected_symbol_drawdown_cooldown: usize,
    pub max_capital_used: f64,
    pub avg_capital_used_on_entry: f64,
    pub symbol_summaries: Vec<PortfolioWheelSymbolSummary>,
    #[serde(default)]
    pub strategy_summaries: Vec<PortfolioStrategySummary>,
    #[serde(default)]
    pub risk_summary: PortfolioWheelRiskSummary,
    #[serde(default)]
    pub decision_metrics: PortfolioDecisionMetrics,
    #[serde(default)]
    pub ablations: Vec<PortfolioAblationSummary>,
    pub canary_readiness: PortfolioCanaryReadiness,
    #[serde(default)]
    pub latest_actions: Vec<PortfolioLatestAction>,
    pub gate_status: String,
    pub gate_pass: bool,
    pub gate_reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PortfolioLatestAction {
    pub status: String,
    pub symbol: String,
    pub strategy: SpreadStructure,
    pub entry_date: NaiveDate,
    pub exit_date: NaiveDate,
    pub expiration: NaiveDate,
    pub dte_entry: i64,
    pub days_held: i64,
    pub pnl: f64,
    pub exit_reason: String,
    pub max_loss: f64,
    pub entry_credit: f64,
    #[serde(default)]
    pub short_strike: f64,
    #[serde(default)]
    pub long_strike: f64,
    #[serde(default)]
    pub width: f64,
    pub short_delta: f64,
    #[serde(default)]
    pub long_delta: f64,
    #[serde(default)]
    pub short_oi: u32,
    #[serde(default)]
    pub long_oi: u32,
    #[serde(default)]
    pub short_iv: f64,
    #[serde(default)]
    pub long_iv: f64,
    pub underlying_price: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PortfolioAblationSummary {
    pub label: String,
    pub removed_kind: String,
    pub removed_value: String,
    pub metrics: ResearchMetrics,
    pub gate_status: String,
    pub gate_pass: bool,
    pub gate_reason: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PortfolioStrategySummary {
    pub strategy: String,
    pub trades: usize,
    pub pnl: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub avg_pnl: f64,
    pub avg_days_held: f64,
    pub worst_trade_pnl: f64,
    pub assigned_cycles: usize,
    pub marked_stock_cycles: usize,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PortfolioWheelRiskSummary {
    pub total_pnl: f64,
    #[serde(default)]
    pub max_closed_equity_drawdown: f64,
    #[serde(default)]
    pub max_closed_equity_drawdown_pct_capital: f64,
    #[serde(default)]
    pub cost_25_max_closed_equity_drawdown: f64,
    #[serde(default)]
    pub cost_25_max_closed_equity_drawdown_pct_capital: f64,
    pub wheel_trades: usize,
    pub wheel_pnl: f64,
    pub put_credit_pnl: f64,
    pub call_credit_pnl: f64,
    pub put_debit_pnl: f64,
    pub call_debit_pnl: f64,
    pub assigned_cycles: usize,
    pub assignment_rate: f64,
    pub called_away_cycles: usize,
    pub marked_stock_cycles: usize,
    pub marked_stock_loss_cycles: usize,
    pub marked_stock_pnl: f64,
    pub worst_marked_stock_loss: f64,
    pub worst_trade_loss: f64,
    pub avg_wheel_days_held: f64,
    pub avg_assigned_days_held: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PortfolioDecisionMetrics {
    pub pnl_to_drawdown_capital: f64,
    pub cost_25_pnl_to_drawdown_capital: f64,
    #[serde(default)]
    pub max_capital_drawdown: f64,
    #[serde(default)]
    pub max_capital_drawdown_pct: f64,
    #[serde(default)]
    pub cost_25_max_capital_drawdown: f64,
    #[serde(default)]
    pub cost_25_max_capital_drawdown_pct: f64,
    pub pnl_per_max_capital_used: f64,
    pub cost_25_pnl_per_max_capital_used: f64,
    pub pnl_per_avg_capital_used_on_entry: f64,
    pub marked_stock_loss_to_pnl: f64,
    pub assignment_rate: f64,
    pub professional_risk_flag: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PortfolioCanaryReadiness {
    pub status: String,
    pub canary_ready: bool,
    pub full_promotion_ready: bool,
    pub reason: String,
    pub recommended_capital_fraction: f64,
    pub max_symbol_pnl_share: f64,
    pub max_symbol: Option<String>,
    pub symbol_ablation_passes: usize,
    pub strategy_ablation_passes: usize,
    pub cost_25_pnl: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PortfolioWheelTrade {
    pub symbol: String,
    #[serde(default)]
    pub strategy: SpreadStructure,
    pub capital_at_risk: f64,
    pub trade: ResearchTrade,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PortfolioWheelSymbolSummary {
    pub symbol: String,
    pub trades: usize,
    pub pnl: f64,
    pub assigned_cycles: usize,
    pub called_away_cycles: usize,
    pub marked_stock_cycles: usize,
    pub capital_at_risk: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReturnOrDrawdownGate {
    pub min_underlying_return: Option<f64>,
    pub min_underlying_drawdown: Option<f64>,
}

impl ReturnOrDrawdownGate {
    fn allows(&self, underlying_return: Option<f64>, underlying_drawdown: Option<f64>) -> bool {
        let return_ok = self
            .min_underlying_return
            .is_some_and(|min_return| underlying_return.is_some_and(|value| value >= min_return));
        let drawdown_ok = self.min_underlying_drawdown.is_some_and(|min_drawdown| {
            underlying_drawdown.is_some_and(|value| value >= min_drawdown)
        });
        return_ok || drawdown_ok
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TrendDrawdownGuard {
    pub min_underlying_return: f64,
    pub max_underlying_drawdown: f64,
}

impl TrendDrawdownGuard {
    fn allows(&self, underlying_return: Option<f64>, underlying_drawdown: Option<f64>) -> bool {
        let Some(underlying_return) = underlying_return else {
            return false;
        };
        let Some(underlying_drawdown) = underlying_drawdown else {
            return false;
        };
        underlying_return < self.min_underlying_return
            || underlying_drawdown <= self.max_underlying_drawdown
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WeakTrendPullbackGuard {
    pub max_underlying_return: f64,
    pub min_underlying_drawdown: f64,
    pub max_underlying_drawdown: f64,
}

impl WeakTrendPullbackGuard {
    fn allows(&self, underlying_return: Option<f64>, underlying_drawdown: Option<f64>) -> bool {
        let Some(underlying_return) = underlying_return else {
            return false;
        };
        let Some(underlying_drawdown) = underlying_drawdown else {
            return false;
        };
        !(underlying_return <= self.max_underlying_return
            && underlying_drawdown >= self.min_underlying_drawdown
            && underlying_drawdown <= self.max_underlying_drawdown)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResearchProfile {
    pub name: String,
    #[serde(default)]
    pub structure: SpreadStructure,
    pub min_dte: i64,
    pub max_dte: i64,
    pub force_close_dte: i64,
    pub min_short_delta_abs: f64,
    pub max_short_delta_abs: f64,
    #[serde(default)]
    pub max_short_leg_delta_abs: Option<f64>,
    pub min_width: f64,
    pub max_width: f64,
    pub min_credit_width: f64,
    #[serde(default)]
    pub min_debit: Option<f64>,
    pub max_debit_width: Option<f64>,
    pub max_quote_width_pct_of_mid: f64,
    pub max_quote_width_abs: f64,
    pub min_short_oi: u32,
    pub min_long_oi: u32,
    pub take_profit_pct: f64,
    pub stop_loss_multiple: f64,
    pub max_hold_days: Option<i64>,
    pub trend_lookback_days: Option<i64>,
    pub min_underlying_return: Option<f64>,
    pub max_underlying_return: Option<f64>,
    pub drawdown_lookback_days: Option<i64>,
    pub min_underlying_drawdown: Option<f64>,
    pub max_underlying_drawdown: Option<f64>,
    pub return_or_drawdown_gate: Option<ReturnOrDrawdownGate>,
    pub trend_drawdown_guard: Option<TrendDrawdownGuard>,
    pub weak_trend_pullback_guard: Option<WeakTrendPullbackGuard>,
    pub risk_regime_cooldown_guard: Option<TrendDrawdownGuard>,
    pub risk_regime_cooldown_days: i64,
    pub realized_vol_lookback_days: Option<i64>,
    #[serde(default)]
    pub min_realized_vol: Option<f64>,
    pub max_realized_vol: Option<f64>,
    pub min_short_otm_pct: Option<f64>,
    pub min_short_iv: Option<f64>,
    pub max_short_iv: Option<f64>,
    pub min_long_short_iv_diff: Option<f64>,
    #[serde(default = "default_covered_call_min_strike_pct")]
    pub covered_call_min_strike_pct_of_assigned: f64,
    pub low_delta_width_cap_delta_abs: Option<f64>,
    pub low_delta_width_cap: Option<f64>,
    pub prefer_farther_otm: bool,
    pub stop_loss_cooldown_days: i64,
    pub max_concurrent_positions: usize,
    pub min_entry_spacing_days: i64,
    pub min_trades_per_year: f64,
}

fn default_covered_call_min_strike_pct() -> f64 {
    1.0
}

impl ResearchProfile {
    pub fn legacy_baseline() -> Self {
        Self {
            name: "legacy_baseline_30_45dte_delta20_30_credit20".to_owned(),
            structure: SpreadStructure::PutCreditSpread,
            min_dte: 30,
            max_dte: 45,
            force_close_dte: 21,
            min_short_delta_abs: 0.20,
            max_short_delta_abs: 0.30,
            max_short_leg_delta_abs: None,
            min_width: 5.0,
            max_width: 20.0,
            min_credit_width: 0.20,
            min_debit: None,
            max_debit_width: None,
            max_quote_width_pct_of_mid: 0.10,
            max_quote_width_abs: 0.10,
            min_short_oi: 500,
            min_long_oi: 250,
            take_profit_pct: 0.50,
            stop_loss_multiple: 2.0,
            max_hold_days: None,
            trend_lookback_days: None,
            min_underlying_return: None,
            max_underlying_return: None,
            drawdown_lookback_days: None,
            min_underlying_drawdown: None,
            max_underlying_drawdown: None,
            return_or_drawdown_gate: None,
            trend_drawdown_guard: None,
            weak_trend_pullback_guard: None,
            risk_regime_cooldown_guard: None,
            risk_regime_cooldown_days: 0,
            realized_vol_lookback_days: None,
            min_realized_vol: None,
            max_realized_vol: None,
            min_short_otm_pct: None,
            min_short_iv: None,
            max_short_iv: None,
            min_long_short_iv_diff: None,
            covered_call_min_strike_pct_of_assigned: default_covered_call_min_strike_pct(),
            low_delta_width_cap_delta_abs: None,
            low_delta_width_cap: None,
            prefer_farther_otm: false,
            stop_loss_cooldown_days: 1,
            max_concurrent_positions: 1,
            min_entry_spacing_days: 1,
            min_trades_per_year: MIN_RANKING_TRADES_PER_YEAR,
        }
    }

    pub fn baseline() -> Self {
        Self::frozen_baseline()
    }

    pub fn frozen_baseline() -> Self {
        Self {
            name: FROZEN_BASELINE_NAME.to_owned(),
            structure: SpreadStructure::PutCreditSpread,
            min_dte: 30,
            max_dte: 45,
            force_close_dte: 21,
            min_short_delta_abs: 0.26,
            max_short_delta_abs: 0.34,
            max_short_leg_delta_abs: None,
            min_width: 5.0,
            max_width: 15.0,
            min_credit_width: 0.20,
            min_debit: None,
            max_debit_width: None,
            max_quote_width_pct_of_mid: 0.10,
            max_quote_width_abs: 0.10,
            min_short_oi: 500,
            min_long_oi: 250,
            take_profit_pct: 0.45,
            stop_loss_multiple: 2.0,
            max_hold_days: None,
            trend_lookback_days: Some(60),
            min_underlying_return: Some(0.10),
            max_underlying_return: None,
            drawdown_lookback_days: Some(20),
            min_underlying_drawdown: None,
            max_underlying_drawdown: None,
            return_or_drawdown_gate: Some(ReturnOrDrawdownGate {
                min_underlying_return: Some(0.25),
                min_underlying_drawdown: Some(0.02),
            }),
            trend_drawdown_guard: None,
            weak_trend_pullback_guard: Some(WeakTrendPullbackGuard {
                max_underlying_return: 0.13,
                min_underlying_drawdown: 0.02,
                max_underlying_drawdown: 0.05,
            }),
            risk_regime_cooldown_guard: Some(TrendDrawdownGuard {
                min_underlying_return: 0.30,
                max_underlying_drawdown: 0.05,
            }),
            risk_regime_cooldown_days: 20,
            realized_vol_lookback_days: None,
            min_realized_vol: None,
            max_realized_vol: None,
            min_short_otm_pct: None,
            min_short_iv: None,
            max_short_iv: Some(0.45),
            min_long_short_iv_diff: None,
            covered_call_min_strike_pct_of_assigned: default_covered_call_min_strike_pct(),
            low_delta_width_cap_delta_abs: Some(0.23),
            low_delta_width_cap: Some(10.0),
            prefer_farther_otm: true,
            stop_loss_cooldown_days: 10,
            max_concurrent_positions: 1,
            min_entry_spacing_days: 1,
            min_trades_per_year: MIN_RANKING_TRADES_PER_YEAR,
        }
    }

    pub fn weekly_baseline() -> Self {
        Self {
            name: "weekly_baseline_dte5_14_delta10_30_credit15_width1_5_take33".to_owned(),
            structure: SpreadStructure::PutCreditSpread,
            min_dte: 5,
            max_dte: 14,
            force_close_dte: 1,
            min_short_delta_abs: 0.10,
            max_short_delta_abs: 0.30,
            max_short_leg_delta_abs: None,
            min_width: 1.0,
            max_width: 5.0,
            min_credit_width: 0.15,
            min_debit: None,
            max_debit_width: None,
            max_quote_width_pct_of_mid: 0.15,
            max_quote_width_abs: 0.20,
            min_short_oi: 100,
            min_long_oi: 50,
            take_profit_pct: 0.33,
            stop_loss_multiple: 2.0,
            max_hold_days: Some(7),
            trend_lookback_days: Some(20),
            min_underlying_return: Some(-0.05),
            max_underlying_return: None,
            drawdown_lookback_days: Some(20),
            min_underlying_drawdown: None,
            max_underlying_drawdown: Some(0.25),
            return_or_drawdown_gate: None,
            trend_drawdown_guard: None,
            weak_trend_pullback_guard: None,
            risk_regime_cooldown_guard: Some(TrendDrawdownGuard {
                min_underlying_return: 0.40,
                max_underlying_drawdown: 0.10,
            }),
            risk_regime_cooldown_days: 5,
            realized_vol_lookback_days: Some(20),
            min_realized_vol: None,
            max_realized_vol: Some(1.50),
            min_short_otm_pct: None,
            min_short_iv: None,
            max_short_iv: None,
            min_long_short_iv_diff: None,
            covered_call_min_strike_pct_of_assigned: default_covered_call_min_strike_pct(),
            low_delta_width_cap_delta_abs: None,
            low_delta_width_cap: None,
            prefer_farther_otm: true,
            stop_loss_cooldown_days: 3,
            max_concurrent_positions: 3,
            min_entry_spacing_days: 1,
            min_trades_per_year: MIN_WEEKLY_RANKING_TRADES_PER_YEAR,
        }
    }

    pub fn weekly_put_debit_baseline() -> Self {
        let mut profile = Self::weekly_baseline();
        profile.name =
            "weekly_put_debit_baseline_dte3_14_delta25_55_debit50_take33_stop50".to_owned();
        profile.structure = SpreadStructure::PutDebitSpread;
        profile.min_dte = 3;
        profile.max_dte = 14;
        profile.force_close_dte = 1;
        profile.min_short_delta_abs = 0.25;
        profile.max_short_delta_abs = 0.55;
        profile.min_width = 1.0;
        profile.max_width = 10.0;
        profile.min_credit_width = 0.0;
        profile.min_debit = None;
        profile.max_debit_width = Some(0.50);
        profile.take_profit_pct = 0.33;
        profile.stop_loss_multiple = 0.50;
        profile.max_hold_days = Some(5);
        profile.max_concurrent_positions = 2;
        profile.min_entry_spacing_days = 1;
        profile.stop_loss_cooldown_days = 3;
        profile.risk_regime_cooldown_guard = None;
        profile.risk_regime_cooldown_days = 0;
        profile.prefer_farther_otm = false;
        profile
    }

    pub fn weekly_call_debit_baseline() -> Self {
        let mut profile = Self::weekly_put_debit_baseline();
        profile.name =
            "weekly_call_debit_baseline_dte3_14_delta25_55_debit50_take33_stop50".to_owned();
        profile.structure = SpreadStructure::CallDebitSpread;
        profile.trend_lookback_days = Some(20);
        profile.min_underlying_return = Some(0.0);
        profile.max_underlying_return = Some(0.25);
        profile.drawdown_lookback_days = Some(20);
        profile.max_underlying_drawdown = Some(0.20);
        profile.realized_vol_lookback_days = Some(20);
        profile.min_realized_vol = Some(0.20);
        profile.max_realized_vol = Some(1.50);
        profile
    }

    pub fn weekly_wheel_baseline() -> Self {
        let mut profile = Self::weekly_baseline();
        profile.name = "weekly_wheel_baseline_dte3_10_delta10_30_credit02_hold45".to_owned();
        profile.structure = SpreadStructure::Wheel;
        profile.min_dte = 3;
        profile.max_dte = 10;
        profile.force_close_dte = 0;
        profile.min_short_delta_abs = 0.10;
        profile.max_short_delta_abs = 0.30;
        profile.min_width = 0.0;
        profile.max_width = 0.0;
        profile.min_credit_width = 0.02;
        profile.min_short_oi = 100;
        profile.min_long_oi = 100;
        profile.take_profit_pct = 0.0;
        profile.stop_loss_multiple = 0.0;
        profile.max_hold_days = Some(45);
        profile.max_concurrent_positions = 1;
        profile.min_entry_spacing_days = 1;
        profile.risk_regime_cooldown_guard = None;
        profile.risk_regime_cooldown_days = 0;
        profile.stop_loss_cooldown_days = 1;
        profile.min_short_otm_pct = Some(0.02);
        profile.prefer_farther_otm = false;
        profile.min_trades_per_year = MIN_WEEKLY_RANKING_TRADES_PER_YEAR;
        profile
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResearchReport {
    pub run_id: String,
    pub symbol: String,
    #[serde(default)]
    pub profile_family: ResearchProfileFamily,
    pub requested_from: NaiveDate,
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub expirations_discovered: usize,
    pub expirations_skipped_before_data: usize,
    pub expirations_loaded: usize,
    pub expirations_failed: usize,
    pub expiration_load_failures: Vec<ExpirationLoadFailure>,
    pub rows_loaded: usize,
    pub latest_signal: Option<ResearchSignal>,
    pub deployment_gate: DeploymentGate,
    pub plateau_status: PlateauStatus,
    pub walk_forward: WalkForwardResult,
    pub rolling_walk_forward: WalkForwardResult,
    pub holdout: HoldoutResult,
    pub fixed_profile_walk_forward: Vec<FixedProfileWalkForwardResult>,
    pub profiles: Vec<ProfileResult>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExpirationLoadFailure {
    pub expiration: NaiveDate,
    pub message: String,
}

#[derive(Clone, Copy, Debug)]
struct ExpirationLoadBounds {
    from: NaiveDate,
    to: NaiveDate,
    max_entry_dte: i64,
    min_force_close_dte: i64,
    option_row_lookback_days: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeploymentGate {
    pub status: String,
    pub pass: bool,
    pub best_profile_gate: bool,
    pub walk_forward_oos_gate: bool,
    pub holdout_oos_gate: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlateauStatus {
    pub status: String,
    pub expansion_ready: bool,
    pub profiles_evaluated: usize,
    pub profile_variants_evaluated: usize,
    pub min_profile_variants: usize,
    pub walk_forward_years: usize,
    pub min_walk_forward_years: usize,
    pub detector_status: String,
    pub execution_strategy_status: String,
    pub reason: String,
    pub next_action: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProfileResult {
    pub profile: ResearchProfile,
    pub detector_strategy: DetectorStrategySummary,
    pub execution_strategy: ExecutionStrategySummary,
    pub candidates: usize,
    pub trades: Vec<ResearchTrade>,
    pub metrics: ResearchMetrics,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DetectorStrategySummary {
    pub name: String,
    pub min_dte: i64,
    pub max_dte: i64,
    pub min_short_delta_abs: f64,
    pub max_short_delta_abs: f64,
    pub min_width: f64,
    pub max_width: f64,
    pub min_credit_width: f64,
    pub max_quote_width_pct_of_mid: f64,
    pub max_quote_width_abs: f64,
    pub min_short_oi: u32,
    pub min_long_oi: u32,
    pub filters: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExecutionStrategySummary {
    pub name: String,
    pub candidate_selector: String,
    pub entry_fill_model: String,
    pub exit_fill_model: String,
    pub take_profit_pct: f64,
    pub stop_loss_multiple: f64,
    pub force_close_dte: i64,
    pub max_hold_days: Option<i64>,
    pub stop_loss_cooldown_days: i64,
    pub max_concurrent_positions: usize,
    pub min_entry_spacing_days: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalkForwardResult {
    pub mode: String,
    pub min_train_days: i64,
    pub train_window_days: Option<i64>,
    pub years: Vec<WalkForwardYear>,
    pub selected_profile_counts: BTreeMap<String, usize>,
    pub trades: Vec<ResearchTrade>,
    pub metrics: ResearchMetrics,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalkForwardYear {
    pub test_year: i32,
    pub train_from: NaiveDate,
    pub train_to: NaiveDate,
    pub test_from: NaiveDate,
    pub test_to: NaiveDate,
    pub active: bool,
    pub selected_profile: String,
    pub train_metrics: WalkForwardTrainMetrics,
    pub test_metrics: PeriodMetrics,
    pub selection_candidates: Vec<WalkForwardSelectionCandidate>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalkForwardSelectionCandidate {
    pub rank: usize,
    pub profile: String,
    pub active: bool,
    pub train_metrics: WalkForwardTrainMetrics,
    pub test_metrics: PeriodMetrics,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalkForwardTrainMetrics {
    pub trades: usize,
    pub total_pnl: f64,
    pub score: f64,
    pub robust_score: f64,
    pub ranking_eligible: bool,
    pub robust_ranking_eligible: bool,
    pub min_deployable_robust_score: f64,
    pub robust_score_gate: bool,
    pub recent_activity_window_days: i64,
    pub recent_trades: usize,
    pub recent_activity_gate: bool,
    pub last_entry_date: Option<NaiveDate>,
    pub days_since_last_entry: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HoldoutResult {
    pub train_from: NaiveDate,
    pub train_to: NaiveDate,
    pub test_from: NaiveDate,
    pub test_to: NaiveDate,
    pub active: bool,
    pub selected_profile: String,
    pub train_metrics: WalkForwardTrainMetrics,
    pub trades: Vec<ResearchTrade>,
    pub metrics: ResearchMetrics,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FixedProfileWalkForwardResult {
    pub profile: ResearchProfile,
    pub detector_strategy: DetectorStrategySummary,
    pub execution_strategy: ExecutionStrategySummary,
    pub active_years: usize,
    pub years: Vec<FixedProfileWalkForwardYear>,
    pub trades: Vec<ResearchTrade>,
    pub metrics: ResearchMetrics,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FixedProfileWalkForwardYear {
    pub test_year: i32,
    pub train_from: NaiveDate,
    pub train_to: NaiveDate,
    pub test_from: NaiveDate,
    pub test_to: NaiveDate,
    pub active: bool,
    pub train_metrics: WalkForwardTrainMetrics,
    pub test_metrics: PeriodMetrics,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResearchMetrics {
    pub trades: usize,
    pub total_pnl: f64,
    pub total_max_loss: f64,
    pub avg_return_on_risk: f64,
    pub median_return_on_risk: f64,
    pub avg_entry_dte: f64,
    pub median_entry_dte: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub max_drawdown: f64,
    pub avg_days_held: f64,
    pub median_days_held: f64,
    pub trades_per_year: f64,
    pub best_trade_pnl: f64,
    pub worst_trade_pnl: f64,
    pub score: f64,
    pub robust_score: f64,
    pub ranking_eligible: bool,
    pub robust_ranking_eligible: bool,
    pub required_trades: usize,
    pub exit_reasons: BTreeMap<String, usize>,
    pub yearly: BTreeMap<i32, YearMetrics>,
    pub annual_stability: AnnualStabilityMetrics,
    pub chronological: Vec<PeriodMetrics>,
    pub cost_stress: Vec<CostStressMetrics>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AnnualStabilityMetrics {
    pub active_years: usize,
    pub positive_years: usize,
    pub negative_years: usize,
    pub positive_year_rate: f64,
    pub worst_year: Option<i32>,
    pub worst_year_pnl: f64,
    pub worst_year_avg_return_on_risk: f64,
    pub best_year: Option<i32>,
    pub best_year_pnl: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CostStressMetrics {
    pub per_trade_cost: f64,
    pub trades: usize,
    pub total_pnl: f64,
    pub avg_return_on_risk: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub max_drawdown: f64,
    pub score: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeriodMetrics {
    pub name: String,
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub trades: usize,
    pub total_pnl: f64,
    pub avg_return_on_risk: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub max_drawdown: f64,
    pub score: f64,
    pub ranking_eligible: bool,
    pub required_trades: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct YearMetrics {
    pub trades: usize,
    pub pnl: f64,
    pub win_rate: f64,
    pub avg_return_on_risk: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResearchTrade {
    pub entry_date: NaiveDate,
    pub exit_date: NaiveDate,
    pub expiration: NaiveDate,
    pub dte_entry: i64,
    pub days_held: i64,
    pub short_put: f64,
    pub long_put: f64,
    pub width: f64,
    pub entry_credit: f64,
    pub exit_debit: f64,
    pub max_profit: f64,
    pub max_loss: f64,
    pub pnl: f64,
    pub return_on_risk: f64,
    pub exit_reason: String,
    pub short_delta: f64,
    pub long_delta: f64,
    pub short_oi: u32,
    pub long_oi: u32,
    pub underlying_price: f64,
    pub short_otm_pct: f64,
    pub underlying_lookback_return: Option<f64>,
    pub underlying_recent_drawdown: Option<f64>,
    pub underlying_realized_vol: Option<f64>,
    pub short_iv: f64,
    pub long_iv: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResearchSignal {
    pub as_of: NaiveDate,
    pub status: String,
    pub profile_name: String,
    pub entry_date: NaiveDate,
    pub expiration: NaiveDate,
    pub dte_entry: i64,
    pub short_put: f64,
    pub long_put: f64,
    pub width: f64,
    pub entry_credit: f64,
    pub max_profit: f64,
    pub max_loss: f64,
    pub return_on_risk: f64,
    pub short_delta: f64,
    pub long_delta: f64,
    pub short_oi: u32,
    pub long_oi: u32,
    pub underlying_price: f64,
    pub short_otm_pct: f64,
    pub underlying_lookback_return: Option<f64>,
    pub underlying_recent_drawdown: Option<f64>,
    pub underlying_realized_vol: Option<f64>,
    pub short_iv: f64,
    pub long_iv: f64,
}

#[derive(Clone, Debug)]
struct OptionDay {
    date: NaiveDate,
    strike: f64,
    bid: f64,
    ask: f64,
    delta: f64,
    implied_vol: f64,
    underlying_price: f64,
    open_interest: u32,
}

#[derive(Clone, Debug)]
struct Candidate {
    structure: SpreadStructure,
    entry_date: NaiveDate,
    expiration: NaiveDate,
    short: OptionDay,
    long: OptionDay,
    width: f64,
    credit: f64,
    max_profit_per_share: f64,
    max_loss_per_share: f64,
    return_on_risk: f64,
    short_otm_pct: f64,
    underlying_lookback_return: Option<f64>,
    underlying_recent_drawdown: Option<f64>,
    underlying_realized_vol: Option<f64>,
    short_iv: f64,
    long_iv: f64,
}

#[derive(Clone, Debug)]
struct EntryRegime {
    short_otm_pct: f64,
    underlying_lookback_return: Option<f64>,
    underlying_recent_drawdown: Option<f64>,
    underlying_realized_vol: Option<f64>,
}

pub async fn run_symbol_research(request: ResearchRequest) -> Result<ResearchReport> {
    if request.cache_only && request.force_refresh {
        anyhow::bail!("cache-only symbol research cannot force refresh");
    }
    let raw_dir = PathBuf::from("data/raw/theta").join(&request.symbol);
    let profile_family = request.profile_family;
    let run_prefix = match profile_family {
        ResearchProfileFamily::Swing => format!("{}-research", symbol_slug(&request.symbol)),
        ResearchProfileFamily::Weekly => {
            format!("{}-weekly-research", symbol_slug(&request.symbol))
        }
        ResearchProfileFamily::WeeklyFarOtm => {
            format!("{}-weekly-far-otm-research", symbol_slug(&request.symbol))
        }
        ResearchProfileFamily::WeeklyPutDebit => {
            format!("{}-weekly-put-debit-research", symbol_slug(&request.symbol))
        }
        ResearchProfileFamily::WeeklyCallCredit => {
            format!(
                "{}-weekly-call-credit-research",
                symbol_slug(&request.symbol)
            )
        }
        ResearchProfileFamily::WeeklyCallDebit => {
            format!(
                "{}-weekly-call-debit-research",
                symbol_slug(&request.symbol)
            )
        }
        ResearchProfileFamily::WeeklyWheel => {
            format!("{}-weekly-wheel-research", symbol_slug(&request.symbol))
        }
    };
    let run_id = format!("{}-{}", run_prefix, Utc::now().format("%Y%m%dT%H%M%S%.9fZ"));
    let run_dir = PathBuf::from("runs").join(&run_id);
    fs::create_dir_all(&raw_dir)?;
    fs::create_dir_all(&run_dir)?;

    let profiles = research_profiles_for(profile_family);
    let option_data_mode = option_data_mode_for_profile_family(profile_family);
    let max_entry_dte = profiles
        .iter()
        .map(|profile| profile.max_dte)
        .max()
        .unwrap_or(45);
    let min_entry_dte = profiles
        .iter()
        .map(|profile| profile.min_dte)
        .min()
        .unwrap_or(30);
    let min_force_close_dte = profiles
        .iter()
        .map(|profile| profile.force_close_dte)
        .min()
        .unwrap_or(21);
    let max_regime_lookback_days = profiles
        .iter()
        .flat_map(|profile| {
            [
                profile.trend_lookback_days,
                profile.drawdown_lookback_days,
                profile.realized_vol_lookback_days,
            ]
        })
        .flatten()
        .max()
        .unwrap_or(0);

    let expirations = discover_expirations_with_cache_mode(
        &request.symbol,
        &raw_dir,
        request.force_refresh,
        request.cache_only,
    )
    .await?;
    let mut candidate_expirations = expirations
        .iter()
        .copied()
        .filter(|expiration| {
            let entry_start = *expiration - Duration::days(max_entry_dte);
            let entry_end = *expiration - Duration::days(min_entry_dte);
            entry_start <= request.to && entry_end >= request.from
        })
        .collect::<Vec<_>>();
    candidate_expirations.sort();
    if let Some(max) = request.max_expirations {
        candidate_expirations = evenly_spaced(candidate_expirations, max);
    }
    let option_row_lookback_days = option_row_lookback_days(
        profile_family,
        request.max_expirations,
        max_regime_lookback_days,
    );
    let requested_load_bounds = ExpirationLoadBounds {
        from: request.from,
        to: request.to,
        max_entry_dte,
        min_force_close_dte,
        option_row_lookback_days,
    };
    let mut research_from = request.from;
    let expirations_skipped_before_data;
    if request.cache_only {
        expirations_skipped_before_data = 0;
    } else {
        if let Some(first_loadable_idx) = first_expiration_with_rows(
            &request.symbol,
            &candidate_expirations,
            requested_load_bounds,
            &raw_dir,
            request.force_refresh,
            option_data_mode,
        )
        .await?
        {
            expirations_skipped_before_data = first_loadable_idx;
            if first_loadable_idx > 0 {
                candidate_expirations = candidate_expirations.split_off(first_loadable_idx);
                if let Some(first_expiration) = candidate_expirations.first() {
                    research_from = request
                        .from
                        .max(*first_expiration - Duration::days(max_entry_dte));
                    println!(
                        "using effective research start {} after skipping {} leading empty/unavailable expirations",
                        research_from, expirations_skipped_before_data
                    );
                }
            }
        } else {
            expirations_skipped_before_data = candidate_expirations.len();
            candidate_expirations.clear();
        }
    }
    let effective_load_bounds = ExpirationLoadBounds {
        from: research_from,
        ..requested_load_bounds
    };

    let mut rows_by_expiration = BTreeMap::new();
    let mut call_rows_by_expiration = BTreeMap::new();
    let mut rows_loaded = 0;
    let mut expiration_load_failures = Vec::new();
    let fetch_concurrency = request.fetch_concurrency.max(1);
    for chunk in candidate_expirations.chunks(fetch_concurrency) {
        let fetches = chunk.iter().copied().filter_map(|expiration| {
            let (start, end) = expiration_load_window(expiration, effective_load_bounds)?;
            let symbol = request.symbol.clone();
            let raw_dir = raw_dir.clone();
            let force_refresh = request.force_refresh;
            let cache_only = request.cache_only;
            Some(async move {
                println!("loading {} {}..{}", expiration, start, end);
                load_expiration_rows_for_mode_with_cache_mode(
                    &symbol,
                    expiration,
                    start,
                    end,
                    &raw_dir,
                    force_refresh,
                    cache_only,
                    option_data_mode,
                )
                .await
                .map(|rows| (expiration, rows))
                .map_err(|error| (expiration, error))
            })
        });
        for result in join_all(fetches).await {
            match result {
                Ok((expiration, rows)) => {
                    rows_loaded += rows.primary.len() + rows.calls.len();
                    if !rows.primary.is_empty() {
                        rows_by_expiration.insert(expiration, rows.primary);
                    }
                    if !rows.calls.is_empty() {
                        call_rows_by_expiration.insert(expiration, rows.calls);
                    }
                }
                Err((expiration, error)) => {
                    let failure = expiration_load_failure_from_error(expiration, &error);
                    eprintln!(
                        "skipping {} after expiration load failure: {}",
                        failure.expiration, failure.message
                    );
                    expiration_load_failures.push(failure);
                }
            }
        }
    }
    research_from = loaded_rows_effective_from(
        research_from,
        max_entry_dte,
        rows_by_expiration
            .keys()
            .copied()
            .chain(call_rows_by_expiration.keys().copied()),
    );

    let mut profile_results = Vec::new();
    for profile in profiles {
        let candidates =
            generate_candidates(&rows_by_expiration, &profile, research_from, request.to);
        let trades = if profile.structure == SpreadStructure::Wheel {
            simulate_wheel_non_overlapping(
                &candidates,
                &rows_by_expiration,
                &call_rows_by_expiration,
                &profile,
                request.to,
            )
        } else {
            simulate_non_overlapping(&candidates, &rows_by_expiration, &profile)
        };
        let metrics = metrics_for_profile(&trades, research_from, request.to, &profile);
        profile_results.push(ProfileResult {
            detector_strategy: detector_strategy_summary(&profile),
            execution_strategy: execution_strategy_summary(&profile),
            profile,
            candidates: candidates.len(),
            trades,
            metrics,
        });
    }
    let walk_forward = walk_forward(&profile_results, research_from, request.to);
    let rolling_walk_forward = rolling_walk_forward(&profile_results, research_from, request.to);
    let holdout = holdout(&profile_results, research_from, request.to);
    let fixed_profile_walk_forward =
        fixed_profile_walk_forward(&profile_results, research_from, request.to);
    profile_results.sort_by(profile_result_order);
    let latest_signal = latest_signal_for_best_profile(
        &profile_results,
        &rows_by_expiration,
        research_from,
        request.to,
    );
    let deployment_gate = deployment_gate_for(&profile_results, &walk_forward, &holdout);
    let plateau_status =
        plateau_status_for(&profile_results, &deployment_gate, &walk_forward, &holdout);

    let report = ResearchReport {
        run_id: run_id.clone(),
        symbol: request.symbol,
        profile_family,
        requested_from: request.from,
        from: research_from,
        to: request.to,
        expirations_discovered: expirations.len(),
        expirations_skipped_before_data,
        expirations_loaded: rows_by_expiration.len(),
        expirations_failed: expiration_load_failures.len(),
        expiration_load_failures,
        rows_loaded,
        latest_signal,
        deployment_gate,
        plateau_status,
        walk_forward,
        rolling_walk_forward,
        holdout,
        fixed_profile_walk_forward,
        profiles: profile_results,
    };

    fs::write(
        run_dir.join("research.json"),
        serde_json::to_string_pretty(&report)?,
    )?;
    fs::write(run_dir.join("report.md"), research_markdown(&report))?;
    println!("{}", run_dir.display());
    Ok(report)
}

pub async fn audit_weekly_signal_gates(
    request: WeeklySignalGateAuditRequest,
) -> Result<WeeklySignalGateAuditReport> {
    let raw_dir = PathBuf::from("data/raw/theta").join(&request.symbol);
    fs::create_dir_all(&raw_dir)?;

    let profiles = research_profiles_for(request.profile_family);
    let option_data_mode = option_data_mode_for_profile_family(request.profile_family);
    let max_entry_dte = profiles
        .iter()
        .map(|profile| profile.max_dte)
        .max()
        .unwrap_or(45);
    let min_entry_dte = profiles
        .iter()
        .map(|profile| profile.min_dte)
        .min()
        .unwrap_or(30);
    let min_force_close_dte = profiles
        .iter()
        .map(|profile| profile.force_close_dte)
        .min()
        .unwrap_or(21);
    let max_regime_lookback_days = profiles
        .iter()
        .flat_map(|profile| {
            [
                profile.trend_lookback_days,
                profile.drawdown_lookback_days,
                profile.realized_vol_lookback_days,
            ]
        })
        .flatten()
        .max()
        .unwrap_or(0);

    let expirations = discover_expirations_with_cache_mode(
        &request.symbol,
        &raw_dir,
        request.force_refresh,
        request.cache_only,
    )
    .await?;
    let expirations_discovered = expirations.len();
    let mut candidate_expirations = expirations
        .iter()
        .copied()
        .filter(|expiration| {
            let entry_start = *expiration - Duration::days(max_entry_dte);
            let entry_end = *expiration - Duration::days(min_entry_dte);
            entry_start <= request.to && entry_end >= request.from
        })
        .collect::<Vec<_>>();
    candidate_expirations.sort();
    if let Some(max) = request.max_expirations {
        candidate_expirations = evenly_spaced(candidate_expirations, max);
    }
    let expirations_audited = candidate_expirations.len();
    let option_row_lookback_days = option_row_lookback_days(
        request.profile_family,
        request.max_expirations,
        max_regime_lookback_days,
    );
    let requested_load_bounds = ExpirationLoadBounds {
        from: request.from,
        to: request.to,
        max_entry_dte,
        min_force_close_dte,
        option_row_lookback_days,
    };

    let mut research_from = request.from;
    if !request.cache_only {
        if let Some(first_loadable_idx) = first_expiration_with_rows(
            &request.symbol,
            &candidate_expirations,
            requested_load_bounds,
            &raw_dir,
            request.force_refresh,
            option_data_mode,
        )
        .await?
        {
            if first_loadable_idx > 0 {
                candidate_expirations = candidate_expirations.split_off(first_loadable_idx);
                if let Some(first_expiration) = candidate_expirations.first() {
                    research_from = request
                        .from
                        .max(*first_expiration - Duration::days(max_entry_dte));
                }
            }
        } else {
            candidate_expirations.clear();
        }
    }

    let effective_load_bounds = ExpirationLoadBounds {
        from: research_from,
        ..requested_load_bounds
    };
    let mut rows_by_expiration = BTreeMap::new();
    let mut call_rows_by_expiration = BTreeMap::new();
    let mut rows_loaded = 0;
    let mut expirations_failed = 0;
    let fetch_concurrency = request.fetch_concurrency.max(1);
    for chunk in candidate_expirations.chunks(fetch_concurrency) {
        let fetches = chunk.iter().copied().filter_map(|expiration| {
            let (start, end) = expiration_load_window(expiration, effective_load_bounds)?;
            let symbol = request.symbol.clone();
            let raw_dir = raw_dir.clone();
            let force_refresh = request.force_refresh;
            let cache_only = request.cache_only;
            Some(async move {
                load_expiration_rows_for_mode_with_cache_mode(
                    &symbol,
                    expiration,
                    start,
                    end,
                    &raw_dir,
                    force_refresh,
                    cache_only,
                    option_data_mode,
                )
                .await
                .map(|rows| (expiration, rows))
            })
        });
        for result in join_all(fetches).await {
            match result {
                Ok((expiration, rows)) => {
                    rows_loaded += rows.primary.len() + rows.calls.len();
                    if !rows.primary.is_empty() {
                        rows_by_expiration.insert(expiration, rows.primary);
                    }
                    if !rows.calls.is_empty() {
                        call_rows_by_expiration.insert(expiration, rows.calls);
                    }
                }
                Err(_) => {
                    expirations_failed += 1;
                }
            }
        }
    }
    research_from = loaded_rows_effective_from(
        research_from,
        max_entry_dte,
        rows_by_expiration
            .keys()
            .copied()
            .chain(call_rows_by_expiration.keys().copied()),
    );

    let mut profile_audits = Vec::new();
    for profile in profiles {
        let gate_counts =
            weekly_signal_gate_counts(&rows_by_expiration, &profile, research_from, request.to);
        let candidates =
            generate_candidates(&rows_by_expiration, &profile, research_from, request.to);
        let trades = if profile.structure == SpreadStructure::Wheel {
            simulate_wheel_non_overlapping(
                &candidates,
                &rows_by_expiration,
                &call_rows_by_expiration,
                &profile,
                request.to,
            )
        } else {
            simulate_non_overlapping(&candidates, &rows_by_expiration, &profile)
        };
        let candidate_entry_days = candidates
            .iter()
            .map(|candidate| candidate.entry_date)
            .collect::<BTreeSet<_>>()
            .len();
        let trade_entry_days = trades
            .iter()
            .map(|trade| trade.entry_date)
            .collect::<BTreeSet<_>>()
            .len();
        profile_audits.push(WeeklySignalGateProfileAudit {
            profile: profile.name.clone(),
            structure: profile.structure,
            dte_rows: gate_counts.dte_rows,
            dte_entry_days: gate_counts.dte_entry_days,
            primary_leg_passes: gate_counts.primary_leg_passes,
            regime_passes: gate_counts.regime_passes,
            candidates: candidates.len(),
            candidate_entry_days,
            simulated_trades: trades.len(),
            trade_entry_days,
            total_pnl: trades.iter().map(|trade| trade.pnl).sum(),
        });
    }
    profile_audits.sort_by(|a, b| {
        b.simulated_trades
            .cmp(&a.simulated_trades)
            .then_with(|| b.candidates.cmp(&a.candidates))
            .then_with(|| b.regime_passes.cmp(&a.regime_passes))
            .then_with(|| b.primary_leg_passes.cmp(&a.primary_leg_passes))
            .then_with(|| a.profile.cmp(&b.profile))
    });

    Ok(WeeklySignalGateAuditReport {
        symbol: request.symbol,
        profile_family: request.profile_family,
        requested_from: request.from,
        from: research_from,
        to: request.to,
        cache_only: request.cache_only,
        expirations_discovered,
        expirations_audited,
        expirations_loaded: rows_by_expiration.len(),
        expirations_failed,
        rows_loaded,
        profiles: profile_audits,
    })
}

#[derive(Clone, Debug)]
struct PortfolioWheelSymbolData {
    summary: PortfolioWheelLoadedSymbol,
    put_rows_by_expiration: BTreeMap<NaiveDate, Vec<OptionDay>>,
    call_rows_by_expiration: BTreeMap<NaiveDate, Vec<OptionDay>>,
    call_rows_by_date: BTreeMap<NaiveDate, Vec<ExpiringOptionDay>>,
    put_lookup: HashMap<(NaiveDate, String), BTreeMap<NaiveDate, OptionDay>>,
    call_lookup: HashMap<(NaiveDate, String), BTreeMap<NaiveDate, OptionDay>>,
    call_underlying_by_date: BTreeMap<NaiveDate, f64>,
}

#[derive(Clone, Debug)]
struct ExpiringOptionDay {
    expiration: NaiveDate,
    row: OptionDay,
}

#[derive(Clone, Debug)]
struct PortfolioWheelOpportunity {
    symbol: String,
    strategy: SpreadStructure,
    trade: ResearchTrade,
    capital_at_risk: f64,
}

#[derive(Clone, Debug, Default)]
struct PortfolioWheelAllocation {
    trades: Vec<PortfolioWheelTrade>,
    candidates: usize,
    rejected_capital_budget: usize,
    rejected_symbol_allocation: usize,
    rejected_open_positions: usize,
    rejected_symbol_positions: usize,
    rejected_symbol_total_trades: usize,
    rejected_portfolio_drawdown_cooldown: usize,
    rejected_symbol_drawdown_cooldown: usize,
    max_capital_used: f64,
    avg_capital_used_on_entry: f64,
}

#[derive(Clone, Debug)]
struct PortfolioWheelSimulation {
    allocation: PortfolioWheelAllocation,
    opportunities: Vec<PortfolioWheelOpportunity>,
}

pub async fn run_portfolio_wheel_research(
    request: PortfolioWheelResearchRequest,
) -> Result<PortfolioWheelReport> {
    run_portfolio_research(
        request,
        "portfolio-weekly-wheel-research",
        "Portfolio Weekly Wheel Research",
        weekly_wheel_research_profiles()
            .into_iter()
            .map(PortfolioSelectorProfile::single)
            .collect(),
    )
    .await
}

pub async fn run_portfolio_selector_research(
    request: PortfolioWheelResearchRequest,
) -> Result<PortfolioWheelReport> {
    run_portfolio_research(
        request,
        "portfolio-weekly-selector-research",
        "Portfolio Weekly Selector Research",
        portfolio_selector_profiles(),
    )
    .await
}

pub async fn audit_option_cache_coverage(
    request: OptionCacheCoverageRequest,
) -> Result<OptionCacheCoverageReport> {
    let symbols = normalize_portfolio_symbols(&request.symbols);
    if symbols.is_empty() {
        anyhow::bail!("option cache coverage audit requires at least one symbol");
    }

    let profiles = weekly_wheel_research_profiles();
    let max_entry_dte = profiles
        .iter()
        .map(|profile| profile.max_dte)
        .max()
        .unwrap_or(14);
    let min_entry_dte = profiles
        .iter()
        .map(|profile| profile.min_dte)
        .min()
        .unwrap_or(1);
    let min_force_close_dte = profiles
        .iter()
        .map(|profile| profile.force_close_dte)
        .min()
        .unwrap_or(0);
    let max_regime_lookback_days = profiles
        .iter()
        .flat_map(|profile| {
            [
                profile.trend_lookback_days,
                profile.drawdown_lookback_days,
                profile.realized_vol_lookback_days,
            ]
        })
        .flatten()
        .max()
        .unwrap_or(0);
    let bounds = ExpirationLoadBounds {
        from: request.from,
        to: request.to,
        max_entry_dte,
        min_force_close_dte,
        option_row_lookback_days: option_row_lookback_days(
            ResearchProfileFamily::WeeklyWheel,
            request.max_expirations,
            max_regime_lookback_days,
        ),
    };

    let mut out = Vec::new();
    for symbol in symbols {
        out.push(
            audit_symbol_option_cache_coverage(&symbol, &request, min_entry_dte, bounds).await,
        );
    }
    Ok(OptionCacheCoverageReport { symbols: out })
}

pub async fn warm_option_cache_coverage(
    request: WarmOptionCacheCoverageRequest,
) -> Result<WarmOptionCacheCoverageReport> {
    let symbols = normalize_portfolio_symbols(&request.symbols);
    if symbols.is_empty() {
        anyhow::bail!("option cache coverage warmup requires at least one symbol");
    }
    if request.max_windows_per_symbol == 0 {
        anyhow::bail!("max_windows_per_symbol must be greater than zero");
    }

    let profiles = weekly_wheel_research_profiles();
    let max_entry_dte = profiles
        .iter()
        .map(|profile| profile.max_dte)
        .max()
        .unwrap_or(14);
    let min_entry_dte = profiles
        .iter()
        .map(|profile| profile.min_dte)
        .min()
        .unwrap_or(1);
    let min_force_close_dte = profiles
        .iter()
        .map(|profile| profile.force_close_dte)
        .min()
        .unwrap_or(0);
    let max_regime_lookback_days = profiles
        .iter()
        .flat_map(|profile| {
            [
                profile.trend_lookback_days,
                profile.drawdown_lookback_days,
                profile.realized_vol_lookback_days,
            ]
        })
        .flatten()
        .max()
        .unwrap_or(0);
    let bounds = ExpirationLoadBounds {
        from: request.from,
        to: request.to,
        max_entry_dte,
        min_force_close_dte,
        option_row_lookback_days: option_row_lookback_days(
            ResearchProfileFamily::WeeklyWheel,
            request.max_expirations,
            max_regime_lookback_days,
        ),
    };

    let mut out = Vec::new();
    for symbol in symbols {
        out.push(warm_symbol_option_cache_coverage(&symbol, &request, min_entry_dte, bounds).await);
    }
    Ok(WarmOptionCacheCoverageReport { symbols: out })
}

#[derive(Clone, Debug)]
struct WarmOptionCacheCandidate {
    expiration: NaiveDate,
    start: NaiveDate,
    end: NaiveDate,
    put_complete_before: bool,
    call_complete_before: bool,
}

async fn warm_symbol_option_cache_coverage(
    symbol: &str,
    request: &WarmOptionCacheCoverageRequest,
    min_entry_dte: i64,
    bounds: ExpirationLoadBounds,
) -> WarmOptionCacheCoverageSymbol {
    let raw_dir = PathBuf::from("data/raw/theta").join(symbol);
    let expirations =
        match discover_expirations_with_cache_mode(symbol, &raw_dir, request.force_refresh, false)
            .await
        {
            Ok(expirations) => expirations,
            Err(error) => {
                return WarmOptionCacheCoverageSymbol {
                    symbol: symbol.to_owned(),
                    from: request.from,
                    to: request.to,
                    expirations_discovered: 0,
                    expirations_audited: 0,
                    windows_attempted: 0,
                    windows_completed: 0,
                    windows_failed: 0,
                    windows: Vec::new(),
                    error: Some(compact_error_message(&format!("{error:#}"))),
                };
            }
        };

    let mut candidate_expirations = expirations
        .iter()
        .copied()
        .filter(|expiration| {
            let entry_start = *expiration - Duration::days(bounds.max_entry_dte);
            let entry_end = *expiration - Duration::days(min_entry_dte);
            entry_start <= request.to && entry_end >= request.from
        })
        .collect::<Vec<_>>();
    candidate_expirations.sort();
    if let Some(max) = request.max_expirations {
        candidate_expirations = evenly_spaced(candidate_expirations, max);
    }

    let mut candidates = Vec::new();
    let mut error = None;
    let coverage_index = match OptionCacheCoverageIndex::build(&raw_dir) {
        Ok(index) => index,
        Err(cache_error) => {
            error = Some(compact_error_message(&format!("{cache_error:#}")));
            OptionCacheCoverageIndex::default()
        }
    };

    for expiration in &candidate_expirations {
        let Some((start, end)) = expiration_load_window(*expiration, bounds) else {
            continue;
        };
        let exp = yyyymmdd(*expiration);
        let put_complete_before =
            coverage_index.has_complete_coverage(&exp, start, end, OptionRight::Put);
        let call_complete_before =
            coverage_index.has_complete_coverage(&exp, start, end, OptionRight::Call);
        if !put_complete_before || !call_complete_before {
            candidates.push(WarmOptionCacheCandidate {
                expiration: *expiration,
                start,
                end,
                put_complete_before,
                call_complete_before,
            });
        }
    }
    candidates.truncate(request.max_windows_per_symbol);

    let mut windows = Vec::new();
    for chunk in candidates.chunks(request.fetch_concurrency.max(1)) {
        let futures =
            chunk.iter().cloned().map(|candidate| {
                let symbol = symbol.to_owned();
                let raw_dir = raw_dir.clone();
                let force_refresh = request.force_refresh;
                async move {
                    warm_option_cache_window(&symbol, &raw_dir, candidate, force_refresh).await
                }
            });
        windows.extend(join_all(futures).await);
    }

    let windows_completed = windows
        .iter()
        .filter(|window| window.both_complete_after)
        .count();
    let windows_failed = windows
        .iter()
        .filter(|window| !window.both_complete_after)
        .count();
    WarmOptionCacheCoverageSymbol {
        symbol: symbol.to_owned(),
        from: request.from,
        to: request.to,
        expirations_discovered: expirations.len(),
        expirations_audited: candidate_expirations.len(),
        windows_attempted: windows.len(),
        windows_completed,
        windows_failed,
        windows,
        error,
    }
}

async fn warm_option_cache_window(
    symbol: &str,
    raw_dir: &Path,
    candidate: WarmOptionCacheCandidate,
    force_refresh: bool,
) -> WarmOptionCacheCoverageWindow {
    let load_error = load_expiration_rows_for_mode_with_cache_mode(
        symbol,
        candidate.expiration,
        candidate.start,
        candidate.end,
        raw_dir,
        force_refresh,
        false,
        OptionDataMode::PutAndCall,
    )
    .await
    .err()
    .map(|error| compact_error_message(&format!("{error:#}")));

    let exp = yyyymmdd(candidate.expiration);
    let post_index = OptionCacheCoverageIndex::build(raw_dir).unwrap_or_default();
    let put_complete_after =
        post_index.has_complete_coverage(&exp, candidate.start, candidate.end, OptionRight::Put);
    let call_complete_after =
        post_index.has_complete_coverage(&exp, candidate.start, candidate.end, OptionRight::Call);
    WarmOptionCacheCoverageWindow {
        expiration: candidate.expiration,
        start: candidate.start,
        end: candidate.end,
        put_complete_before: candidate.put_complete_before,
        call_complete_before: candidate.call_complete_before,
        put_complete_after,
        call_complete_after,
        both_complete_after: put_complete_after && call_complete_after,
        error: load_error,
    }
}

async fn audit_symbol_option_cache_coverage(
    symbol: &str,
    request: &OptionCacheCoverageRequest,
    min_entry_dte: i64,
    bounds: ExpirationLoadBounds,
) -> OptionCacheCoverageSymbol {
    let raw_dir = PathBuf::from("data/raw/theta").join(symbol);
    let expirations =
        match discover_expirations_with_cache_mode(symbol, &raw_dir, false, true).await {
            Ok(expirations) => expirations,
            Err(error) => {
                return OptionCacheCoverageSymbol {
                    symbol: symbol.to_owned(),
                    from: request.from,
                    to: request.to,
                    expirations_discovered: 0,
                    expirations_audited: 0,
                    put_complete: 0,
                    call_complete: 0,
                    both_complete: 0,
                    put_coverage_pct: 0.0,
                    call_coverage_pct: 0.0,
                    both_coverage_pct: 0.0,
                    first_complete_call_expiration: None,
                    last_complete_call_expiration: None,
                    missing_call_examples: Vec::new(),
                    error: Some(compact_error_message(&format!("{error:#}"))),
                };
            }
        };

    let mut candidate_expirations = expirations
        .iter()
        .copied()
        .filter(|expiration| {
            let entry_start = *expiration - Duration::days(bounds.max_entry_dte);
            let entry_end = *expiration - Duration::days(min_entry_dte);
            entry_start <= request.to && entry_end >= request.from
        })
        .collect::<Vec<_>>();
    candidate_expirations.sort();
    if let Some(max) = request.max_expirations {
        candidate_expirations = evenly_spaced(candidate_expirations, max);
    }

    let mut put_complete = 0;
    let mut call_complete = 0;
    let mut both_complete = 0;
    let mut first_complete_call_expiration = None;
    let mut last_complete_call_expiration = None;
    let mut missing_call_examples = Vec::new();
    let mut error = None;
    let coverage_index = match OptionCacheCoverageIndex::build(&raw_dir) {
        Ok(index) => index,
        Err(cache_error) => {
            error = Some(compact_error_message(&format!("{cache_error:#}")));
            OptionCacheCoverageIndex::default()
        }
    };

    for expiration in &candidate_expirations {
        let Some((start, end)) = expiration_load_window(*expiration, bounds) else {
            continue;
        };
        let exp = yyyymmdd(*expiration);
        let put_ok = coverage_index.has_complete_coverage(&exp, start, end, OptionRight::Put);
        let call_ok = coverage_index.has_complete_coverage(&exp, start, end, OptionRight::Call);
        if put_ok {
            put_complete += 1;
        }
        if call_ok {
            call_complete += 1;
            first_complete_call_expiration.get_or_insert(*expiration);
            last_complete_call_expiration = Some(*expiration);
        } else if missing_call_examples.len() < 5 {
            missing_call_examples.push(OptionCacheCoverageGap {
                expiration: *expiration,
                start,
                end,
            });
        }
        if put_ok && call_ok {
            both_complete += 1;
        }
    }

    let audited = candidate_expirations.len();
    OptionCacheCoverageSymbol {
        symbol: symbol.to_owned(),
        from: request.from,
        to: request.to,
        expirations_discovered: expirations.len(),
        expirations_audited: audited,
        put_complete,
        call_complete,
        both_complete,
        put_coverage_pct: ratio(put_complete, audited),
        call_coverage_pct: ratio(call_complete, audited),
        both_coverage_pct: ratio(both_complete, audited),
        first_complete_call_expiration,
        last_complete_call_expiration,
        missing_call_examples,
        error,
    }
}

async fn run_portfolio_research(
    request: PortfolioWheelResearchRequest,
    run_prefix: &str,
    report_title: &str,
    selector_profiles: Vec<PortfolioSelectorProfile>,
) -> Result<PortfolioWheelReport> {
    let symbols = normalize_portfolio_symbols(&request.symbols);
    if symbols.is_empty() {
        anyhow::bail!("portfolio research requires at least one symbol");
    }
    if request.capital_budget <= 0.0 {
        anyhow::bail!("capital budget must be positive");
    }
    if request.max_symbol_allocation_pct <= 0.0 || request.max_symbol_allocation_pct > 1.0 {
        anyhow::bail!("max symbol allocation pct must be in (0, 1]");
    }
    if request.max_open_positions == 0 {
        anyhow::bail!("max open positions must be positive");
    }
    if request.max_positions_per_symbol == 0 {
        anyhow::bail!("max positions per symbol must be positive");
    }
    if request.cache_only && request.force_refresh {
        anyhow::bail!("cache-only portfolio research cannot force refresh");
    }

    let run_id = format!("{}-{}", run_prefix, Utc::now().format("%Y%m%dT%H%M%S%.9fZ"));
    let run_dir = PathBuf::from("runs").join(&run_id);
    fs::create_dir_all(&run_dir)?;

    let load_profiles = selector_profiles
        .iter()
        .flat_map(|profile| profile.components())
        .cloned()
        .collect::<Vec<_>>();
    let mut symbol_data = Vec::new();
    for chunk in symbols.chunks(request.symbol_concurrency.max(1)) {
        let loads = chunk.iter().map(|symbol| {
            println!("loading portfolio option data for {symbol}");
            load_portfolio_wheel_symbol_data(symbol, &request, &load_profiles)
        });
        for loaded in join_all(loads).await {
            symbol_data.push(loaded?);
        }
    }

    let from = portfolio_effective_from(&symbol_data, request.from);
    let mut profile_results = selector_profiles
        .into_par_iter()
        .map(|selector_profile| {
            let simulation = simulate_portfolio_selector_profile(
                &symbol_data,
                &selector_profile,
                from,
                request.to,
                &request,
            );
            let allocation = simulation.allocation;
            let research_trades = allocation
                .trades
                .iter()
                .map(|trade| trade.trade.clone())
                .collect::<Vec<_>>();
            let metrics = metrics_with_min_trades_per_year(
                &research_trades,
                from,
                request.to,
                MIN_WEEKLY_RANKING_TRADES_PER_YEAR,
            );
            let (gate_status, gate_pass, gate_reason) =
                portfolio_wheel_gate(&metrics, request.capital_budget);
            let ablations = portfolio_ablation_summaries(
                &simulation.opportunities,
                from,
                request.to,
                request.capital_budget,
                &request,
            );
            let strategy_summaries = portfolio_strategy_summaries(&allocation.trades);
            let risk_summary =
                portfolio_wheel_risk_summary(&allocation.trades, request.capital_budget);
            let decision_metrics = portfolio_decision_metrics(
                &metrics,
                allocation.max_capital_used,
                allocation.avg_capital_used_on_entry,
                request.capital_budget,
                &risk_summary,
            );
            let canary_readiness = portfolio_canary_readiness(
                &metrics,
                &allocation.trades,
                &ablations,
                &strategy_summaries,
                &decision_metrics,
                gate_pass,
            );
            let latest_actions = portfolio_latest_actions(&allocation.trades, request.to, 7);
            PortfolioWheelProfileResult {
                profile: selector_profile.summary_profile,
                metrics,
                trades: allocation.trades.clone(),
                candidates: allocation.candidates,
                accepted: allocation.trades.len(),
                rejected_capital_budget: allocation.rejected_capital_budget,
                rejected_symbol_allocation: allocation.rejected_symbol_allocation,
                rejected_open_positions: allocation.rejected_open_positions,
                rejected_symbol_positions: allocation.rejected_symbol_positions,
                rejected_symbol_total_trades: allocation.rejected_symbol_total_trades,
                rejected_portfolio_drawdown_cooldown: allocation
                    .rejected_portfolio_drawdown_cooldown,
                rejected_symbol_drawdown_cooldown: allocation.rejected_symbol_drawdown_cooldown,
                max_capital_used: allocation.max_capital_used,
                avg_capital_used_on_entry: allocation.avg_capital_used_on_entry,
                symbol_summaries: portfolio_wheel_symbol_summaries(&allocation.trades),
                strategy_summaries,
                risk_summary,
                decision_metrics,
                ablations,
                canary_readiness,
                latest_actions,
                gate_status,
                gate_pass,
                gate_reason,
            }
        })
        .collect::<Vec<_>>();
    profile_results.sort_by(portfolio_wheel_profile_order);

    let report = PortfolioWheelReport {
        run_id: run_id.clone(),
        symbols,
        from,
        to: request.to,
        max_expirations: request.max_expirations,
        fetch_concurrency: request.fetch_concurrency,
        symbol_concurrency: request.symbol_concurrency.max(1),
        force_refresh: request.force_refresh,
        cache_only: request.cache_only,
        capital_budget: request.capital_budget,
        max_symbol_allocation_pct: request.max_symbol_allocation_pct,
        max_open_positions: request.max_open_positions,
        max_positions_per_symbol: request.max_positions_per_symbol,
        max_total_trades_per_symbol: request.max_total_trades_per_symbol,
        portfolio_drawdown_cooldown_trigger_pct: request.portfolio_drawdown_cooldown_trigger_pct,
        portfolio_drawdown_cooldown_days: request.portfolio_drawdown_cooldown_days,
        symbol_drawdown_cooldown_trigger_pct: request.symbol_drawdown_cooldown_trigger_pct,
        symbol_drawdown_cooldown_days: request.symbol_drawdown_cooldown_days,
        symbols_loaded: symbol_data
            .iter()
            .map(|data| data.summary.clone())
            .collect(),
        profiles: profile_results,
    };

    fs::write(
        run_dir.join("portfolio_research.json"),
        serde_json::to_string_pretty(&report)?,
    )?;
    fs::write(
        run_dir.join("report.md"),
        portfolio_wheel_markdown(&report, report_title),
    )?;
    Ok(report)
}

#[derive(Clone, Debug)]
struct PortfolioSelectorProfile {
    summary_profile: ResearchProfile,
    put_credit_profile: Option<ResearchProfile>,
    wheel_profile: Option<ResearchProfile>,
    put_debit_profile: Option<ResearchProfile>,
    put_debit_fallback_profile: Option<ResearchProfile>,
    call_debit_profile: Option<ResearchProfile>,
    call_debit_fallback_profile: Option<ResearchProfile>,
    put_credit_symbols: Option<BTreeSet<String>>,
    wheel_symbols: Option<BTreeSet<String>>,
    put_debit_symbols: Option<BTreeSet<String>>,
    put_debit_fallback_symbols: Option<BTreeSet<String>>,
    call_debit_symbols: Option<BTreeSet<String>>,
    call_debit_fallback_symbols: Option<BTreeSet<String>>,
}

impl PortfolioSelectorProfile {
    fn single(profile: ResearchProfile) -> Self {
        let mut selector = Self {
            summary_profile: profile.clone(),
            put_credit_profile: None,
            wheel_profile: None,
            put_debit_profile: None,
            put_debit_fallback_profile: None,
            call_debit_profile: None,
            call_debit_fallback_profile: None,
            put_credit_symbols: None,
            wheel_symbols: None,
            put_debit_symbols: None,
            put_debit_fallback_symbols: None,
            call_debit_symbols: None,
            call_debit_fallback_symbols: None,
        };
        match profile.structure {
            SpreadStructure::Wheel => selector.wheel_profile = Some(profile),
            SpreadStructure::PutCreditSpread | SpreadStructure::CallCreditSpread => {
                selector.put_credit_profile = Some(profile)
            }
            SpreadStructure::PutDebitSpread => selector.put_debit_profile = Some(profile),
            SpreadStructure::CallDebitSpread => selector.call_debit_profile = Some(profile),
        }
        selector
    }

    fn components(&self) -> Vec<&ResearchProfile> {
        [
            self.put_credit_profile.as_ref(),
            self.wheel_profile.as_ref(),
            self.put_debit_profile.as_ref(),
            self.put_debit_fallback_profile.as_ref(),
            self.call_debit_profile.as_ref(),
            self.call_debit_fallback_profile.as_ref(),
        ]
        .into_iter()
        .flatten()
        .collect()
    }

    fn with_put_debit_symbols(mut self, symbols: &[&str]) -> Self {
        self.put_debit_symbols = Some(symbol_filter(symbols));
        self
    }

    fn with_call_debit_symbols(mut self, symbols: &[&str]) -> Self {
        self.call_debit_symbols = Some(symbol_filter(symbols));
        self
    }

    fn with_call_debit_fallback_symbols(mut self, symbols: &[&str]) -> Self {
        self.call_debit_fallback_symbols = Some(symbol_filter(symbols));
        self
    }
}

fn symbol_filter(symbols: &[&str]) -> BTreeSet<String> {
    symbols.iter().map(|symbol| symbol.to_uppercase()).collect()
}

fn sleeve_allows_symbol(allowed_symbols: &Option<BTreeSet<String>>, symbol: &str) -> bool {
    allowed_symbols
        .as_ref()
        .is_none_or(|allowed| allowed.contains(symbol))
}

async fn load_portfolio_wheel_symbol_data(
    symbol: &str,
    request: &PortfolioWheelResearchRequest,
    profiles: &[ResearchProfile],
) -> Result<PortfolioWheelSymbolData> {
    let raw_dir = PathBuf::from("data/raw/theta").join(symbol);
    fs::create_dir_all(&raw_dir)?;

    let max_entry_dte = profiles
        .iter()
        .map(|profile| profile.max_dte)
        .max()
        .unwrap_or(14);
    let min_entry_dte = profiles
        .iter()
        .map(|profile| profile.min_dte)
        .min()
        .unwrap_or(1);
    let min_force_close_dte = profiles
        .iter()
        .map(|profile| profile.force_close_dte)
        .min()
        .unwrap_or(0);
    let max_regime_lookback_days = profiles
        .iter()
        .flat_map(|profile| {
            [
                profile.trend_lookback_days,
                profile.drawdown_lookback_days,
                profile.realized_vol_lookback_days,
            ]
        })
        .flatten()
        .max()
        .unwrap_or(0);

    let expirations = discover_expirations_with_cache_mode(
        symbol,
        &raw_dir,
        request.force_refresh,
        request.cache_only,
    )
    .await?;
    let mut candidate_expirations = expirations
        .iter()
        .copied()
        .filter(|expiration| {
            let entry_start = *expiration - Duration::days(max_entry_dte);
            let entry_end = *expiration - Duration::days(min_entry_dte);
            entry_start <= request.to && entry_end >= request.from
        })
        .collect::<Vec<_>>();
    candidate_expirations.sort();
    if let Some(max) = request.max_expirations {
        candidate_expirations = evenly_spaced(candidate_expirations, max);
    }

    let bounds = ExpirationLoadBounds {
        from: request.from,
        to: request.to,
        max_entry_dte,
        min_force_close_dte,
        option_row_lookback_days: option_row_lookback_days(
            ResearchProfileFamily::WeeklyWheel,
            request.max_expirations,
            max_regime_lookback_days,
        ),
    };
    let mut research_from = request.from;
    let expirations_skipped_before_data;
    if request.cache_only {
        expirations_skipped_before_data = 0;
    } else {
        if let Some(first_loadable_idx) = first_expiration_with_rows(
            symbol,
            &candidate_expirations,
            bounds,
            &raw_dir,
            request.force_refresh,
            OptionDataMode::PutAndCall,
        )
        .await?
        {
            expirations_skipped_before_data = first_loadable_idx;
            if first_loadable_idx > 0 {
                candidate_expirations = candidate_expirations.split_off(first_loadable_idx);
                if let Some(first_expiration) = candidate_expirations.first() {
                    research_from = request
                        .from
                        .max(*first_expiration - Duration::days(max_entry_dte));
                }
            }
        } else {
            expirations_skipped_before_data = candidate_expirations.len();
            candidate_expirations.clear();
        }
    }
    let effective_bounds = ExpirationLoadBounds {
        from: research_from,
        ..bounds
    };

    let mut put_rows_by_expiration = BTreeMap::new();
    let mut call_rows_by_expiration = BTreeMap::new();
    let mut rows_loaded = 0;
    let mut expiration_load_failures = Vec::new();
    for chunk in candidate_expirations.chunks(request.fetch_concurrency.max(1)) {
        let fetches = chunk.iter().copied().filter_map(|expiration| {
            let (start, end) = expiration_load_window(expiration, effective_bounds)?;
            let raw_dir = raw_dir.clone();
            let symbol = symbol.to_owned();
            let force_refresh = request.force_refresh;
            let cache_only = request.cache_only;
            Some(async move {
                load_expiration_rows_for_mode_with_cache_mode(
                    &symbol,
                    expiration,
                    start,
                    end,
                    &raw_dir,
                    force_refresh,
                    cache_only,
                    OptionDataMode::PutAndCall,
                )
                .await
                .map(|rows| (expiration, rows))
                .map_err(|error| (expiration, error))
            })
        });
        for result in join_all(fetches).await {
            match result {
                Ok((expiration, rows)) => {
                    rows_loaded += rows.primary.len() + rows.calls.len();
                    if !rows.primary.is_empty() {
                        put_rows_by_expiration.insert(expiration, rows.primary);
                    }
                    if !rows.calls.is_empty() {
                        call_rows_by_expiration.insert(expiration, rows.calls);
                    }
                }
                Err((expiration, error)) => {
                    expiration_load_failures
                        .push(expiration_load_failure_from_error(expiration, &error));
                }
            }
        }
    }
    research_from = loaded_rows_effective_from(
        research_from,
        max_entry_dte,
        put_rows_by_expiration
            .keys()
            .copied()
            .chain(call_rows_by_expiration.keys().copied()),
    );

    let put_lookup = build_lookup(&put_rows_by_expiration);
    let call_lookup = build_lookup(&call_rows_by_expiration);
    let call_rows_by_date = expiring_rows_by_date(&call_rows_by_expiration);
    let call_underlying_by_date = underlying_by_date_from_expirations(&call_rows_by_expiration);
    let expirations_loaded = put_rows_by_expiration
        .keys()
        .chain(call_rows_by_expiration.keys())
        .copied()
        .collect::<BTreeSet<_>>()
        .len();
    let summary = PortfolioWheelLoadedSymbol {
        symbol: symbol.to_owned(),
        requested_from: request.from,
        from: research_from,
        to: request.to,
        expirations_discovered: expirations.len(),
        expirations_skipped_before_data,
        expirations_loaded,
        put_expirations_loaded: put_rows_by_expiration.len(),
        call_expirations_loaded: call_rows_by_expiration.len(),
        rows_loaded,
        expirations_failed: expiration_load_failures.len(),
        expiration_load_failures,
    };

    Ok(PortfolioWheelSymbolData {
        summary,
        put_rows_by_expiration,
        call_rows_by_expiration,
        call_rows_by_date,
        put_lookup,
        call_lookup,
        call_underlying_by_date,
    })
}

fn simulate_portfolio_selector_profile(
    symbol_data: &[PortfolioWheelSymbolData],
    selector_profile: &PortfolioSelectorProfile,
    from: NaiveDate,
    to: NaiveDate,
    request: &PortfolioWheelResearchRequest,
) -> PortfolioWheelSimulation {
    let mut opportunities = Vec::new();
    let mut candidate_count = 0;
    for data in symbol_data {
        if let Some(profile) = &selector_profile.put_credit_profile
            && sleeve_allows_symbol(&selector_profile.put_credit_symbols, &data.summary.symbol)
        {
            let (symbol_opportunities, candidates) = portfolio_spread_opportunities_for_symbol(
                data,
                profile,
                from,
                to,
                profile.structure,
            );
            candidate_count += candidates;
            opportunities.extend(symbol_opportunities);
        }
        if let Some(profile) = &selector_profile.wheel_profile
            && sleeve_allows_symbol(&selector_profile.wheel_symbols, &data.summary.symbol)
        {
            let (symbol_opportunities, candidates) =
                portfolio_wheel_opportunities_for_symbol(data, profile, from, to);
            candidate_count += candidates;
            opportunities.extend(symbol_opportunities);
        }
        if let Some(profile) = &selector_profile.put_debit_profile
            && sleeve_allows_symbol(&selector_profile.put_debit_symbols, &data.summary.symbol)
        {
            let (symbol_opportunities, candidates) = portfolio_spread_opportunities_for_symbol(
                data,
                profile,
                from,
                to,
                SpreadStructure::PutDebitSpread,
            );
            candidate_count += candidates;
            opportunities.extend(symbol_opportunities);
        }
        if let Some(profile) = &selector_profile.put_debit_fallback_profile
            && sleeve_allows_symbol(
                &selector_profile.put_debit_fallback_symbols,
                &data.summary.symbol,
            )
        {
            let (symbol_opportunities, candidates) = portfolio_spread_opportunities_for_symbol(
                data,
                profile,
                from,
                to,
                SpreadStructure::PutDebitSpread,
            );
            candidate_count += candidates;
            opportunities.extend(symbol_opportunities);
        }
        if let Some(profile) = &selector_profile.call_debit_profile
            && sleeve_allows_symbol(&selector_profile.call_debit_symbols, &data.summary.symbol)
        {
            let (symbol_opportunities, candidates) = portfolio_spread_opportunities_for_symbol(
                data,
                profile,
                from,
                to,
                SpreadStructure::CallDebitSpread,
            );
            candidate_count += candidates;
            opportunities.extend(symbol_opportunities);
        }
        if let Some(profile) = &selector_profile.call_debit_fallback_profile
            && sleeve_allows_symbol(
                &selector_profile.call_debit_fallback_symbols,
                &data.summary.symbol,
            )
        {
            let (symbol_opportunities, candidates) = portfolio_spread_opportunities_for_symbol(
                data,
                profile,
                from,
                to,
                SpreadStructure::CallDebitSpread,
            );
            candidate_count += candidates;
            opportunities.extend(symbol_opportunities);
        }
    }
    opportunities.sort_by(portfolio_wheel_opportunity_order);
    let allocation =
        allocate_portfolio_wheel_opportunities(&opportunities, candidate_count, request);
    PortfolioWheelSimulation {
        allocation,
        opportunities,
    }
}

fn portfolio_wheel_opportunities_for_symbol(
    data: &PortfolioWheelSymbolData,
    profile: &ResearchProfile,
    from: NaiveDate,
    to: NaiveDate,
) -> (Vec<PortfolioWheelOpportunity>, usize) {
    let candidates = generate_candidates(&data.put_rows_by_expiration, profile, from, to);
    let mut by_date: BTreeMap<NaiveDate, Vec<&Candidate>> = BTreeMap::new();
    for candidate in &candidates {
        by_date
            .entry(candidate.entry_date)
            .or_default()
            .push(candidate);
    }

    let mut opportunities = Vec::new();
    for (_date, mut day_candidates) in by_date {
        if risk_regime_cooldown_triggered(&day_candidates, profile) {
            continue;
        }
        day_candidates.sort_by(|a, b| candidate_quality_order(a, b, profile));
        for candidate in day_candidates {
            if let Some(trade) = simulate_wheel_candidate(
                candidate,
                &data.put_lookup,
                &data.call_rows_by_date,
                &data.call_lookup,
                &data.call_underlying_by_date,
                profile,
                to,
            ) {
                opportunities.push(PortfolioWheelOpportunity {
                    symbol: data.summary.symbol.clone(),
                    strategy: SpreadStructure::Wheel,
                    capital_at_risk: trade.max_loss,
                    trade,
                });
                break;
            }
        }
    }

    (opportunities, candidates.len())
}

fn portfolio_spread_opportunities_for_symbol(
    data: &PortfolioWheelSymbolData,
    profile: &ResearchProfile,
    from: NaiveDate,
    to: NaiveDate,
    strategy: SpreadStructure,
) -> (Vec<PortfolioWheelOpportunity>, usize) {
    let (rows_by_expiration, lookup) = match strategy {
        SpreadStructure::PutCreditSpread => (&data.put_rows_by_expiration, &data.put_lookup),
        SpreadStructure::CallCreditSpread => (&data.call_rows_by_expiration, &data.call_lookup),
        SpreadStructure::PutDebitSpread => (&data.put_rows_by_expiration, &data.put_lookup),
        SpreadStructure::CallDebitSpread => (&data.call_rows_by_expiration, &data.call_lookup),
        SpreadStructure::Wheel => {
            return (Vec::new(), 0);
        }
    };
    let candidates = generate_candidates(rows_by_expiration, profile, from, to);
    let mut by_date: BTreeMap<NaiveDate, Vec<&Candidate>> = BTreeMap::new();
    for candidate in &candidates {
        by_date
            .entry(candidate.entry_date)
            .or_default()
            .push(candidate);
    }

    let mut opportunities = Vec::new();
    for (_date, mut day_candidates) in by_date {
        if risk_regime_cooldown_triggered(&day_candidates, profile) {
            continue;
        }
        day_candidates.sort_by(|a, b| candidate_quality_order(a, b, profile));
        for candidate in day_candidates {
            if let Some(trade) = simulate_candidate(candidate, lookup, profile) {
                opportunities.push(PortfolioWheelOpportunity {
                    symbol: data.summary.symbol.clone(),
                    strategy,
                    capital_at_risk: trade.max_loss,
                    trade,
                });
                break;
            }
        }
    }

    (opportunities, candidates.len())
}

fn allocate_portfolio_wheel_opportunities(
    opportunities: &[PortfolioWheelOpportunity],
    candidates: usize,
    request: &PortfolioWheelResearchRequest,
) -> PortfolioWheelAllocation {
    allocate_portfolio_wheel_opportunities_with_filter(opportunities, candidates, request, |_| true)
}

fn allocate_portfolio_wheel_opportunities_with_filter<F>(
    opportunities: &[PortfolioWheelOpportunity],
    candidates: usize,
    request: &PortfolioWheelResearchRequest,
    include: F,
) -> PortfolioWheelAllocation
where
    F: Fn(&PortfolioWheelOpportunity) -> bool,
{
    let mut allocation = PortfolioWheelAllocation {
        candidates,
        ..PortfolioWheelAllocation::default()
    };
    let mut open_trades: Vec<PortfolioWheelTrade> = Vec::new();
    let mut accepted_symbol_trades: BTreeMap<String, usize> = BTreeMap::new();
    let mut capital_observations = 0usize;
    let mut capital_observation_sum = 0.0;
    let symbol_cap = request.capital_budget * request.max_symbol_allocation_pct;
    let mut realized_pnl = 0.0;
    let mut realized_high_watermark = 0.0;
    let mut portfolio_cooldown_until: Option<NaiveDate> = None;
    let mut symbol_realized_pnl: BTreeMap<String, f64> = BTreeMap::new();
    let mut symbol_realized_high_watermark: BTreeMap<String, f64> = BTreeMap::new();
    let mut symbol_cooldown_until: BTreeMap<String, NaiveDate> = BTreeMap::new();
    let portfolio_cooldown_trigger = request
        .portfolio_drawdown_cooldown_trigger_pct
        .filter(|trigger| *trigger > 0.0 && request.portfolio_drawdown_cooldown_days > 0)
        .map(|trigger| request.capital_budget * trigger);
    let symbol_cooldown_trigger = request
        .symbol_drawdown_cooldown_trigger_pct
        .filter(|trigger| *trigger > 0.0 && request.symbol_drawdown_cooldown_days > 0)
        .map(|trigger| symbol_cap * trigger);

    for opportunity in opportunities {
        if !include(opportunity) {
            continue;
        }
        let mut still_open = Vec::with_capacity(open_trades.len());
        for open in open_trades.drain(..) {
            if open.trade.exit_date >= opportunity.trade.entry_date {
                still_open.push(open);
                continue;
            }
            realized_pnl += open.trade.pnl;
            if realized_pnl > realized_high_watermark {
                realized_high_watermark = realized_pnl;
            }
            if let Some(trigger_dollars) = portfolio_cooldown_trigger {
                let realized_drawdown = realized_high_watermark - realized_pnl;
                if realized_drawdown >= trigger_dollars {
                    let until = open.trade.exit_date
                        + Duration::days(request.portfolio_drawdown_cooldown_days);
                    portfolio_cooldown_until =
                        Some(portfolio_cooldown_until.map_or(until, |current| current.max(until)));
                }
            }
            let symbol_pnl = symbol_realized_pnl
                .entry(open.symbol.clone())
                .and_modify(|pnl| *pnl += open.trade.pnl)
                .or_insert(open.trade.pnl);
            let symbol_high_watermark = symbol_realized_high_watermark
                .entry(open.symbol.clone())
                .or_insert(0.0);
            if *symbol_pnl > *symbol_high_watermark {
                *symbol_high_watermark = *symbol_pnl;
            }
            if let Some(trigger_dollars) = symbol_cooldown_trigger {
                let realized_drawdown = *symbol_high_watermark - *symbol_pnl;
                if realized_drawdown >= trigger_dollars {
                    let until = open.trade.exit_date
                        + Duration::days(request.symbol_drawdown_cooldown_days);
                    symbol_cooldown_until
                        .entry(open.symbol.clone())
                        .and_modify(|current| *current = (*current).max(until))
                        .or_insert(until);
                }
            }
        }
        open_trades = still_open;
        if portfolio_cooldown_until.is_some_and(|until| opportunity.trade.entry_date <= until) {
            allocation.rejected_portfolio_drawdown_cooldown += 1;
            continue;
        }
        if symbol_cooldown_until
            .get(&opportunity.symbol)
            .is_some_and(|until| opportunity.trade.entry_date <= *until)
        {
            allocation.rejected_symbol_drawdown_cooldown += 1;
            continue;
        }
        let total_open_capital = open_trades
            .iter()
            .map(|open| open.capital_at_risk)
            .sum::<f64>();
        let symbol_open_capital = open_trades
            .iter()
            .filter(|open| open.symbol == opportunity.symbol)
            .map(|open| open.capital_at_risk)
            .sum::<f64>();
        let symbol_open_positions = open_trades
            .iter()
            .filter(|open| open.symbol == opportunity.symbol)
            .count();
        let symbol_total_trades = accepted_symbol_trades
            .get(&opportunity.symbol)
            .copied()
            .unwrap_or(0);

        if open_trades.len() >= request.max_open_positions {
            allocation.rejected_open_positions += 1;
            continue;
        }
        if symbol_open_positions >= request.max_positions_per_symbol {
            allocation.rejected_symbol_positions += 1;
            continue;
        }
        if request
            .max_total_trades_per_symbol
            .is_some_and(|limit| symbol_total_trades >= limit)
        {
            allocation.rejected_symbol_total_trades += 1;
            continue;
        }
        if total_open_capital + opportunity.capital_at_risk > request.capital_budget {
            allocation.rejected_capital_budget += 1;
            continue;
        }
        if symbol_open_capital + opportunity.capital_at_risk > symbol_cap {
            allocation.rejected_symbol_allocation += 1;
            continue;
        }

        let accepted = PortfolioWheelTrade {
            symbol: opportunity.symbol.clone(),
            strategy: opportunity.strategy,
            capital_at_risk: opportunity.capital_at_risk,
            trade: opportunity.trade.clone(),
        };
        *accepted_symbol_trades
            .entry(accepted.symbol.clone())
            .or_default() += 1;
        open_trades.push(accepted.clone());
        let current_capital = total_open_capital + accepted.capital_at_risk;
        allocation.max_capital_used = allocation.max_capital_used.max(current_capital);
        capital_observations += 1;
        capital_observation_sum += current_capital;
        allocation.trades.push(accepted);
    }

    allocation.avg_capital_used_on_entry = if capital_observations == 0 {
        0.0
    } else {
        capital_observation_sum / capital_observations as f64
    };
    allocation
}

fn portfolio_wheel_symbol_summaries(
    trades: &[PortfolioWheelTrade],
) -> Vec<PortfolioWheelSymbolSummary> {
    let mut by_symbol: BTreeMap<String, Vec<&PortfolioWheelTrade>> = BTreeMap::new();
    for trade in trades {
        by_symbol
            .entry(trade.symbol.clone())
            .or_default()
            .push(trade);
    }
    by_symbol
        .into_iter()
        .map(|(symbol, trades)| PortfolioWheelSymbolSummary {
            symbol,
            trades: trades.len(),
            pnl: trades.iter().map(|trade| trade.trade.pnl).sum(),
            assigned_cycles: trades
                .iter()
                .filter(|trade| wheel_assignment_cycle(trade))
                .count(),
            called_away_cycles: trades
                .iter()
                .filter(|trade| trade.trade.exit_reason == "covered_call_assigned")
                .count(),
            marked_stock_cycles: trades
                .iter()
                .filter(|trade| trade.trade.exit_reason.starts_with("stock_marked"))
                .count(),
            capital_at_risk: trades.iter().map(|trade| trade.capital_at_risk).sum(),
        })
        .collect()
}

fn portfolio_strategy_summaries(trades: &[PortfolioWheelTrade]) -> Vec<PortfolioStrategySummary> {
    let mut by_strategy: BTreeMap<SpreadStructure, Vec<&PortfolioWheelTrade>> = BTreeMap::new();
    for trade in trades {
        by_strategy.entry(trade.strategy).or_default().push(trade);
    }

    by_strategy
        .into_iter()
        .map(|(strategy, strategy_trades)| {
            let trades_count = strategy_trades.len();
            let pnl = strategy_trades
                .iter()
                .map(|trade| trade.trade.pnl)
                .sum::<f64>();
            let wins = strategy_trades
                .iter()
                .filter(|trade| trade.trade.pnl > 0.0)
                .count();
            let gross_profit = strategy_trades
                .iter()
                .map(|trade| trade.trade.pnl.max(0.0))
                .sum::<f64>();
            let gross_loss = strategy_trades
                .iter()
                .map(|trade| trade.trade.pnl.min(0.0).abs())
                .sum::<f64>();
            let profit_factor = if gross_loss > 0.0 {
                gross_profit / gross_loss
            } else if gross_profit > 0.0 {
                999.0
            } else {
                0.0
            };
            PortfolioStrategySummary {
                strategy: strategy.as_str().to_owned(),
                trades: trades_count,
                pnl,
                win_rate: ratio(wins, trades_count),
                profit_factor,
                avg_pnl: average(
                    strategy_trades
                        .iter()
                        .map(|trade| trade.trade.pnl)
                        .collect::<Vec<_>>()
                        .as_slice(),
                ),
                avg_days_held: average(
                    strategy_trades
                        .iter()
                        .map(|trade| trade.trade.days_held as f64)
                        .collect::<Vec<_>>()
                        .as_slice(),
                ),
                worst_trade_pnl: strategy_trades
                    .iter()
                    .map(|trade| trade.trade.pnl)
                    .min_by(|a, b| a.total_cmp(b))
                    .unwrap_or(0.0),
                assigned_cycles: strategy_trades
                    .iter()
                    .filter(|trade| wheel_assignment_cycle(trade))
                    .count(),
                marked_stock_cycles: strategy_trades
                    .iter()
                    .filter(|trade| trade.trade.exit_reason.starts_with("stock_marked"))
                    .count(),
            }
        })
        .collect()
}

fn portfolio_wheel_risk_summary(
    trades: &[PortfolioWheelTrade],
    capital_budget: f64,
) -> PortfolioWheelRiskSummary {
    let total_pnl = trades.iter().map(|trade| trade.trade.pnl).sum::<f64>();
    let max_closed_equity_drawdown = portfolio_closed_equity_drawdown(trades, 0.0);
    let cost_25_max_closed_equity_drawdown = portfolio_closed_equity_drawdown(trades, 25.0);
    let wheel_trades = trades
        .iter()
        .filter(|trade| trade.strategy == SpreadStructure::Wheel)
        .collect::<Vec<_>>();
    let assigned = wheel_trades
        .iter()
        .copied()
        .filter(|trade| wheel_assignment_cycle(trade))
        .collect::<Vec<_>>();
    let marked = wheel_trades
        .iter()
        .copied()
        .filter(|trade| trade.trade.exit_reason.starts_with("stock_marked"))
        .collect::<Vec<_>>();
    let marked_stock_pnl = marked.iter().map(|trade| trade.trade.pnl).sum::<f64>();

    PortfolioWheelRiskSummary {
        total_pnl,
        max_closed_equity_drawdown,
        max_closed_equity_drawdown_pct_capital: positive_denominator_ratio(
            max_closed_equity_drawdown,
            capital_budget,
        ),
        cost_25_max_closed_equity_drawdown,
        cost_25_max_closed_equity_drawdown_pct_capital: positive_denominator_ratio(
            cost_25_max_closed_equity_drawdown,
            capital_budget,
        ),
        wheel_trades: wheel_trades.len(),
        wheel_pnl: trades
            .iter()
            .filter(|trade| trade.strategy == SpreadStructure::Wheel)
            .map(|trade| trade.trade.pnl)
            .sum(),
        put_credit_pnl: trades
            .iter()
            .filter(|trade| trade.strategy == SpreadStructure::PutCreditSpread)
            .map(|trade| trade.trade.pnl)
            .sum(),
        call_credit_pnl: trades
            .iter()
            .filter(|trade| trade.strategy == SpreadStructure::CallCreditSpread)
            .map(|trade| trade.trade.pnl)
            .sum(),
        put_debit_pnl: trades
            .iter()
            .filter(|trade| trade.strategy == SpreadStructure::PutDebitSpread)
            .map(|trade| trade.trade.pnl)
            .sum(),
        call_debit_pnl: trades
            .iter()
            .filter(|trade| trade.strategy == SpreadStructure::CallDebitSpread)
            .map(|trade| trade.trade.pnl)
            .sum(),
        assigned_cycles: assigned.len(),
        assignment_rate: ratio(assigned.len(), wheel_trades.len()),
        called_away_cycles: wheel_trades
            .iter()
            .filter(|trade| trade.trade.exit_reason == "covered_call_assigned")
            .count(),
        marked_stock_cycles: marked.len(),
        marked_stock_loss_cycles: marked.iter().filter(|trade| trade.trade.pnl < 0.0).count(),
        marked_stock_pnl,
        worst_marked_stock_loss: marked
            .iter()
            .map(|trade| trade.trade.pnl)
            .min_by(|a, b| a.total_cmp(b))
            .unwrap_or(0.0),
        worst_trade_loss: trades
            .iter()
            .map(|trade| trade.trade.pnl)
            .min_by(|a, b| a.total_cmp(b))
            .unwrap_or(0.0),
        avg_wheel_days_held: average(
            wheel_trades
                .iter()
                .map(|trade| trade.trade.days_held as f64)
                .collect::<Vec<_>>()
                .as_slice(),
        ),
        avg_assigned_days_held: average(
            assigned
                .iter()
                .map(|trade| trade.trade.days_held as f64)
                .collect::<Vec<_>>()
                .as_slice(),
        ),
    }
}

fn portfolio_closed_equity_drawdown(trades: &[PortfolioWheelTrade], per_trade_cost: f64) -> f64 {
    let mut sorted = trades.to_vec();
    sorted.sort_by(|a, b| trade_chronological_order(&a.trade, &b.trade));
    let mut equity = 0.0;
    let mut high_water = 0.0;
    let mut drawdown = 0.0;
    for trade in sorted {
        equity += trade.trade.pnl - per_trade_cost;
        if equity > high_water {
            high_water = equity;
        }
        let current = high_water - equity;
        if current > drawdown {
            drawdown = current;
        }
    }
    drawdown
}

fn portfolio_decision_metrics(
    metrics: &ResearchMetrics,
    max_capital_used: f64,
    avg_capital_used_on_entry: f64,
    _capital_budget: f64,
    risk: &PortfolioWheelRiskSummary,
) -> PortfolioDecisionMetrics {
    let cost_25_pnl = metrics
        .cost_stress
        .iter()
        .find(|stress| (stress.per_trade_cost - 25.0).abs() < f64::EPSILON)
        .map(|stress| stress.total_pnl)
        .unwrap_or(metrics.total_pnl);
    let pnl_to_drawdown_capital =
        positive_denominator_ratio(metrics.total_pnl, risk.max_closed_equity_drawdown);
    let cost_25_pnl_to_drawdown_capital =
        positive_denominator_ratio(cost_25_pnl, risk.cost_25_max_closed_equity_drawdown);
    let marked_stock_loss_to_pnl = positive_denominator_ratio(
        (-risk.marked_stock_pnl.min(0.0)).max(0.0),
        metrics.total_pnl,
    );
    let professional_risk_flag = if risk.wheel_trades == 0 {
        "no_wheel_inventory".to_owned()
    } else if risk.wheel_pnl < 0.0 {
        "wheel_edge_negative".to_owned()
    } else if marked_stock_loss_to_pnl > 0.50 || risk.assignment_rate > 0.25 {
        "inventory_risk_high".to_owned()
    } else {
        "inventory_risk_contained".to_owned()
    };

    PortfolioDecisionMetrics {
        pnl_to_drawdown_capital,
        cost_25_pnl_to_drawdown_capital,
        max_capital_drawdown: risk.max_closed_equity_drawdown,
        max_capital_drawdown_pct: risk.max_closed_equity_drawdown_pct_capital,
        cost_25_max_capital_drawdown: risk.cost_25_max_closed_equity_drawdown,
        cost_25_max_capital_drawdown_pct: risk.cost_25_max_closed_equity_drawdown_pct_capital,
        pnl_per_max_capital_used: positive_denominator_ratio(metrics.total_pnl, max_capital_used),
        cost_25_pnl_per_max_capital_used: positive_denominator_ratio(cost_25_pnl, max_capital_used),
        pnl_per_avg_capital_used_on_entry: positive_denominator_ratio(
            metrics.total_pnl,
            avg_capital_used_on_entry,
        ),
        marked_stock_loss_to_pnl,
        assignment_rate: risk.assignment_rate,
        professional_risk_flag,
    }
}

fn positive_denominator_ratio(numerator: f64, denominator: f64) -> f64 {
    if denominator > 0.0 {
        numerator / denominator
    } else {
        0.0
    }
}

fn wheel_assignment_cycle(trade: &PortfolioWheelTrade) -> bool {
    trade.strategy == SpreadStructure::Wheel
        && trade.trade.exit_reason != "put_expired"
        && trade.trade.exit_reason != "put_take_profit"
}

fn ratio(numerator: usize, denominator: usize) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn average(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

fn portfolio_ablation_summaries(
    opportunities: &[PortfolioWheelOpportunity],
    from: NaiveDate,
    to: NaiveDate,
    capital_budget: f64,
    request: &PortfolioWheelResearchRequest,
) -> Vec<PortfolioAblationSummary> {
    let symbols = opportunities
        .iter()
        .map(|opportunity| opportunity.symbol.clone())
        .collect::<BTreeSet<_>>();
    let strategies = opportunities
        .iter()
        .map(|opportunity| opportunity.strategy)
        .collect::<BTreeSet<_>>();

    let mut out = Vec::new();
    for symbol in symbols {
        let allocation = allocate_portfolio_wheel_opportunities_with_filter(
            opportunities,
            opportunities.len(),
            request,
            |opportunity| opportunity.symbol != symbol,
        );
        let remaining_trades = allocation
            .trades
            .iter()
            .map(|trade| trade.trade.clone())
            .collect::<Vec<_>>();
        out.push(portfolio_ablation_summary(
            "remove_symbol",
            &symbol,
            &remaining_trades,
            from,
            to,
            capital_budget,
        ));
    }
    for strategy in strategies {
        let allocation = allocate_portfolio_wheel_opportunities_with_filter(
            opportunities,
            opportunities.len(),
            request,
            |opportunity| opportunity.strategy != strategy,
        );
        let remaining_trades = allocation
            .trades
            .iter()
            .map(|trade| trade.trade.clone())
            .collect::<Vec<_>>();
        out.push(portfolio_ablation_summary(
            "remove_strategy",
            strategy.as_str(),
            &remaining_trades,
            from,
            to,
            capital_budget,
        ));
    }
    out
}

fn portfolio_ablation_summary(
    removed_kind: &str,
    removed_value: &str,
    remaining_trades: &[ResearchTrade],
    from: NaiveDate,
    to: NaiveDate,
    capital_budget: f64,
) -> PortfolioAblationSummary {
    let metrics = metrics_with_min_trades_per_year(
        remaining_trades,
        from,
        to,
        MIN_WEEKLY_RANKING_TRADES_PER_YEAR,
    );
    let (gate_status, gate_pass, gate_reason) = portfolio_wheel_gate(&metrics, capital_budget);
    PortfolioAblationSummary {
        label: format!("{removed_kind}:{removed_value}"),
        removed_kind: removed_kind.to_owned(),
        removed_value: removed_value.to_owned(),
        metrics,
        gate_status,
        gate_pass,
        gate_reason,
    }
}

fn portfolio_canary_readiness(
    metrics: &ResearchMetrics,
    trades: &[PortfolioWheelTrade],
    ablations: &[PortfolioAblationSummary],
    strategy_summaries: &[PortfolioStrategySummary],
    decision_metrics: &PortfolioDecisionMetrics,
    gate_pass: bool,
) -> PortfolioCanaryReadiness {
    let cost_25_pnl = metrics
        .cost_stress
        .iter()
        .find(|stress| (stress.per_trade_cost - 25.0).abs() < f64::EPSILON)
        .map(|stress| stress.total_pnl)
        .unwrap_or(metrics.total_pnl);
    let (max_symbol, max_symbol_pnl_share) = max_symbol_pnl_share(trades, metrics.total_pnl);
    let symbol_ablation_passes = ablations
        .iter()
        .filter(|ablation| ablation.removed_kind == "remove_symbol" && ablation.gate_pass)
        .count();
    let strategy_ablation_passes = ablations
        .iter()
        .filter(|ablation| ablation.removed_kind == "remove_strategy" && ablation.gate_pass)
        .count();
    let negative_strategy = strategy_summaries
        .iter()
        .find(|summary| summary.pnl < 0.0)
        .map(|summary| summary.strategy.clone());

    let canary_ready = gate_pass
        && cost_25_pnl > 0.0
        && decision_metrics.max_capital_drawdown_pct <= 0.01
        && decision_metrics.professional_risk_flag != "inventory_risk_high"
        && negative_strategy.is_none();
    let full_promotion_ready = canary_ready
        && max_symbol_pnl_share <= 0.50
        && symbol_ablation_passes >= 3
        && strategy_ablation_passes > 0;
    let (status, recommended_capital_fraction, reason) = if full_promotion_ready {
        (
            "full_promotion_candidate",
            0.25,
            "research gate, friction stress, drawdown, symbol concentration, and ablation resilience passed"
                .to_owned(),
        )
    } else if canary_ready {
        (
            "canary_only",
            0.05,
            format!(
                "research gate passed, but concentration remains: max symbol PnL share {:.1}%, symbol ablation passes {}, strategy ablation passes {}",
                max_symbol_pnl_share * 100.0,
                symbol_ablation_passes,
                strategy_ablation_passes
            ),
        )
    } else {
        (
            "blocked",
            0.0,
            if let Some(strategy) = negative_strategy {
                format!("canary blocked: active strategy sleeve {strategy} has negative PnL")
            } else if decision_metrics.professional_risk_flag == "inventory_risk_high" {
                format!(
                    "canary blocked: inventory risk high, marked stock loss/PnL {:.1}%, assignment rate {:.1}%",
                    decision_metrics.marked_stock_loss_to_pnl * 100.0,
                    decision_metrics.assignment_rate * 100.0
                )
            } else {
                format!(
                    "canary blocked: gate_pass={}, $25 cost PnL {:.2}, capital DD {:.2}%",
                    gate_pass,
                    cost_25_pnl,
                    decision_metrics.max_capital_drawdown_pct * 100.0
                )
            },
        )
    };

    PortfolioCanaryReadiness {
        status: status.to_owned(),
        canary_ready,
        full_promotion_ready,
        reason,
        recommended_capital_fraction,
        max_symbol_pnl_share,
        max_symbol,
        symbol_ablation_passes,
        strategy_ablation_passes,
        cost_25_pnl,
    }
}

fn max_symbol_pnl_share(trades: &[PortfolioWheelTrade], total_pnl: f64) -> (Option<String>, f64) {
    if total_pnl <= 0.0 {
        return (None, 0.0);
    }
    let mut by_symbol: BTreeMap<String, f64> = BTreeMap::new();
    for trade in trades {
        *by_symbol.entry(trade.symbol.clone()).or_default() += trade.trade.pnl;
    }
    by_symbol
        .into_iter()
        .max_by(|a, b| a.1.total_cmp(&b.1))
        .map(|(symbol, pnl)| (Some(symbol), (pnl.max(0.0) / total_pnl).max(0.0)))
        .unwrap_or((None, 0.0))
}

fn portfolio_latest_actions(
    trades: &[PortfolioWheelTrade],
    as_of: NaiveDate,
    lookback_days: i64,
) -> Vec<PortfolioLatestAction> {
    let recent_from = as_of - Duration::days(lookback_days.max(0));
    let mut actions = trades
        .iter()
        .filter(|trade| trade.trade.entry_date >= recent_from || trade.trade.exit_date > as_of)
        .map(|trade| {
            let status = if trade.trade.exit_date > as_of {
                "open_candidate"
            } else if trade.trade.entry_date == as_of {
                "entry_candidate"
            } else {
                "recent_closed"
            };
            PortfolioLatestAction {
                status: status.to_owned(),
                symbol: trade.symbol.clone(),
                strategy: trade.strategy,
                entry_date: trade.trade.entry_date,
                exit_date: trade.trade.exit_date,
                expiration: trade.trade.expiration,
                dte_entry: trade.trade.dte_entry,
                days_held: trade.trade.days_held,
                pnl: trade.trade.pnl,
                exit_reason: trade.trade.exit_reason.clone(),
                max_loss: trade.trade.max_loss,
                entry_credit: trade.trade.entry_credit,
                short_strike: trade.trade.short_put,
                long_strike: trade.trade.long_put,
                width: trade.trade.width,
                short_delta: trade.trade.short_delta,
                long_delta: trade.trade.long_delta,
                short_oi: trade.trade.short_oi,
                long_oi: trade.trade.long_oi,
                short_iv: trade.trade.short_iv,
                long_iv: trade.trade.long_iv,
                underlying_price: trade.trade.underlying_price,
            }
        })
        .collect::<Vec<_>>();
    actions.sort_by(|a, b| {
        b.entry_date
            .cmp(&a.entry_date)
            .then_with(|| b.exit_date.cmp(&a.exit_date))
            .then_with(|| a.symbol.cmp(&b.symbol))
            .then_with(|| a.strategy.cmp(&b.strategy))
    });
    actions.truncate(20);
    actions
}

fn portfolio_wheel_gate(metrics: &ResearchMetrics, capital_budget: f64) -> (String, bool, String) {
    let cost_10_pnl = metrics
        .cost_stress
        .iter()
        .find(|stress| (stress.per_trade_cost - 10.0).abs() < f64::EPSILON)
        .map(|stress| stress.total_pnl)
        .unwrap_or(metrics.total_pnl);
    if !metrics.ranking_eligible {
        return (
            "blocked".to_owned(),
            false,
            format!(
                "aggregate cadence failed: {} trades versus required {}",
                metrics.trades, metrics.required_trades
            ),
        );
    }
    if !metrics.robust_ranking_eligible {
        return (
            "blocked".to_owned(),
            false,
            "chronological robustness failed: at least one period lacks enough positive PnL"
                .to_owned(),
        );
    }
    if metrics.annual_stability.active_years < PLATEAU_MIN_WALK_FORWARD_YEARS {
        return (
            "blocked".to_owned(),
            false,
            format!(
                "history gate failed: {} active years versus required {}",
                metrics.annual_stability.active_years, PLATEAU_MIN_WALK_FORWARD_YEARS
            ),
        );
    }
    let max_material_year_loss =
        -capital_budget * PORTFOLIO_MAX_MATERIAL_NEGATIVE_YEAR_PCT_OF_CAPITAL;
    let material_negative_years = metrics
        .yearly
        .values()
        .filter(|year| year.pnl < max_material_year_loss)
        .count();
    if material_negative_years > 0 {
        return (
            "blocked".to_owned(),
            false,
            format!(
                "annual stability failed: {} material negative active years below {:.2}; worst year {}",
                material_negative_years,
                max_material_year_loss,
                format_optional_year_pnl(
                    metrics.annual_stability.worst_year,
                    metrics.annual_stability.worst_year_pnl
                )
            ),
        );
    }
    if cost_10_pnl <= 0.0 {
        return (
            "blocked".to_owned(),
            false,
            format!("friction stress failed: $10/trade PnL {cost_10_pnl:.2}"),
        );
    }
    if metrics.max_drawdown > 0.05 {
        return (
            "blocked".to_owned(),
            false,
            format!(
                "drawdown gate failed: max DD {:.2}%",
                metrics.max_drawdown * 100.0
            ),
        );
    }
    (
        "research_pass".to_owned(),
        true,
        "aggregate cadence, robustness, friction, and drawdown gates passed".to_owned(),
    )
}

fn portfolio_wheel_opportunity_order(
    a: &PortfolioWheelOpportunity,
    b: &PortfolioWheelOpportunity,
) -> Ordering {
    a.trade
        .entry_date
        .cmp(&b.trade.entry_date)
        .then_with(|| b.trade.return_on_risk.total_cmp(&a.trade.return_on_risk))
        .then_with(|| b.trade.entry_credit.total_cmp(&a.trade.entry_credit))
        .then_with(|| a.symbol.cmp(&b.symbol))
        .then_with(|| a.trade.expiration.cmp(&b.trade.expiration))
}

fn portfolio_wheel_profile_order(
    a: &PortfolioWheelProfileResult,
    b: &PortfolioWheelProfileResult,
) -> Ordering {
    profile_rank_order(&a.metrics, &a.profile, &b.metrics, &b.profile)
}

fn normalize_portfolio_symbols(symbols: &[String]) -> Vec<String> {
    let mut normalized = Vec::new();
    for symbol in symbols {
        let symbol = symbol.trim().to_uppercase();
        if !symbol.is_empty() && !normalized.contains(&symbol) {
            normalized.push(symbol);
        }
    }
    normalized
}

fn loaded_rows_effective_from<I>(
    current_from: NaiveDate,
    max_entry_dte: i64,
    expirations: I,
) -> NaiveDate
where
    I: Iterator<Item = NaiveDate>,
{
    expirations
        .min()
        .map(|expiration| current_from.max(expiration - Duration::days(max_entry_dte)))
        .unwrap_or(current_from)
}

fn portfolio_effective_from(
    symbol_data: &[PortfolioWheelSymbolData],
    requested_from: NaiveDate,
) -> NaiveDate {
    symbol_data
        .iter()
        .filter(|data| data.summary.rows_loaded > 0)
        .map(|data| data.summary.from)
        .min()
        .unwrap_or(requested_from)
}

fn avg_wheel_call_count(trades: &[PortfolioWheelTrade]) -> f64 {
    let assigned = trades
        .iter()
        .filter(|trade| {
            trade.strategy == SpreadStructure::Wheel
                && trade.trade.exit_reason != "put_expired"
                && trade.trade.exit_reason != "put_take_profit"
        })
        .collect::<Vec<_>>();
    if assigned.is_empty() {
        0.0
    } else {
        assigned.iter().map(|trade| trade.trade.width).sum::<f64>() / assigned.len() as f64
    }
}

fn portfolio_wheel_markdown(report: &PortfolioWheelReport, title: &str) -> String {
    let mut out = String::new();
    out.push_str(&format!("# {title}\n\n"));
    out.push_str(&format!(
        "- Run: `{}`\n- Symbols: `{}`\n- Window: `{}` to `{}`\n- Capital budget: `${:.0}`\n- Max symbol allocation: `{:.0}%`\n- Max open positions: `{}`\n- Max positions per symbol: `{}`\n- Portfolio DD cooldown: `{}`\n- Symbol DD cooldown: `{}`\n\n",
        report.run_id,
        report.symbols.join(","),
        report.from,
        report.to,
        report.capital_budget,
        report.max_symbol_allocation_pct * 100.0,
        report.max_open_positions,
        report.max_positions_per_symbol,
        report
            .portfolio_drawdown_cooldown_trigger_pct
            .map(|trigger| format!(
                "{:.2}% for {} days",
                trigger * 100.0,
                report.portfolio_drawdown_cooldown_days
            ))
            .unwrap_or_else(|| "off".to_owned()),
        report
            .symbol_drawdown_cooldown_trigger_pct
            .map(|trigger| format!(
                "{:.2}% of symbol cap for {} days",
                trigger * 100.0,
                report.symbol_drawdown_cooldown_days
            ))
            .unwrap_or_else(|| "off".to_owned())
    ));

    out.push_str("## Loaded Data\n\n");
    out.push_str("| Symbol | From | Expirations | Rows | Failures |\n");
    out.push_str("|---|---:|---:|---:|---:|\n");
    for symbol in &report.symbols_loaded {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} |\n",
            symbol.symbol,
            symbol.from,
            symbol.expirations_loaded,
            symbol.rows_loaded,
            symbol.expirations_failed
        ));
    }
    out.push('\n');

    out.push_str("## Top Profiles\n\n");
    out.push_str("| Rank | Profile | Gate | Trades | Required | Trades/Yr | PnL | PF | Risk-Norm DD | Capital DD | $10 Cost PnL | Wheel | Put Debit | Call Debit | Assigned | Called | Marked | Rejected |\n");
    out.push_str(
        "|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n",
    );
    for (idx, result) in report.profiles.iter().take(10).enumerate() {
        let cost_10_pnl = result
            .metrics
            .cost_stress
            .iter()
            .find(|stress| (stress.per_trade_cost - 10.0).abs() < f64::EPSILON)
            .map(|stress| stress.total_pnl)
            .unwrap_or(result.metrics.total_pnl);
        let rejected = result.rejected_capital_budget
            + result.rejected_symbol_allocation
            + result.rejected_open_positions
            + result.rejected_symbol_positions
            + result.rejected_symbol_total_trades
            + result.rejected_portfolio_drawdown_cooldown
            + result.rejected_symbol_drawdown_cooldown;
        let assigned = result
            .symbol_summaries
            .iter()
            .map(|summary| summary.assigned_cycles)
            .sum::<usize>();
        let called = result
            .symbol_summaries
            .iter()
            .map(|summary| summary.called_away_cycles)
            .sum::<usize>();
        let marked = result
            .symbol_summaries
            .iter()
            .map(|summary| summary.marked_stock_cycles)
            .sum::<usize>();
        let wheel_trades = result
            .trades
            .iter()
            .filter(|trade| trade.strategy == SpreadStructure::Wheel)
            .count();
        let put_debit_trades = result
            .trades
            .iter()
            .filter(|trade| trade.strategy == SpreadStructure::PutDebitSpread)
            .count();
        let call_debit_trades = result
            .trades
            .iter()
            .filter(|trade| trade.strategy == SpreadStructure::CallDebitSpread)
            .count();
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {:.2} | {:.2} | {:.2} | {:.2}% | {:.2}% | {:.2} | {} | {} | {} | {} | {} | {} | {} |\n",
            idx + 1,
            result.profile.name,
            result.gate_status,
            result.metrics.trades,
            result.metrics.required_trades,
            result.metrics.trades_per_year,
            result.metrics.total_pnl,
            result.metrics.profit_factor,
            result.metrics.max_drawdown * 100.0,
            result.decision_metrics.max_capital_drawdown_pct * 100.0,
            cost_10_pnl,
            wheel_trades,
            put_debit_trades,
            call_debit_trades,
            assigned,
            called,
            marked,
            rejected
        ));
    }
    out.push('\n');

    out.push_str("## Profile Decision Metrics\n\n");
    out.push_str("| Rank | Profile | Gate | Capital DD | $25 Capital DD | PnL/DD Capital | $25 PnL/DD Capital | PnL/Max Cap | $25 PnL/Max Cap | Marked Loss/PnL | Assignment Rate | Risk Flag |\n");
    out.push_str("|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|\n");
    for (idx, result) in report.profiles.iter().take(10).enumerate() {
        out.push_str(&format!(
            "| {} | {} | {} | {:.2}% | {:.2}% | {:.2} | {:.2} | {:.3} | {:.3} | {:.1}% | {:.1}% | {} |\n",
            idx + 1,
            result.profile.name,
            result.gate_status,
            result.decision_metrics.max_capital_drawdown_pct * 100.0,
            result
                .decision_metrics
                .cost_25_max_capital_drawdown_pct
                * 100.0,
            result.decision_metrics.pnl_to_drawdown_capital,
            result.decision_metrics.cost_25_pnl_to_drawdown_capital,
            result.decision_metrics.pnl_per_max_capital_used,
            result.decision_metrics.cost_25_pnl_per_max_capital_used,
            result.decision_metrics.marked_stock_loss_to_pnl * 100.0,
            result.decision_metrics.assignment_rate * 100.0,
            result.decision_metrics.professional_risk_flag
        ));
    }
    out.push('\n');

    if let Some(best) = report.profiles.first() {
        out.push_str("## Best Profile Detail\n\n");
        out.push_str(&format!(
            "- Profile: `{}`\n- Gate: `{}` ({})\n- Max capital used: `${:.0}`\n- Avg capital used on entry: `${:.0}`\n- Capital drawdown: `${:.0}` (`{:.2}%`)\n- $25 cost capital drawdown: `${:.0}` (`{:.2}%`)\n- Rejected capital budget: `{}`\n- Rejected symbol allocation: `{}`\n- Rejected open positions: `{}`\n- Rejected symbol positions: `{}`\n- Rejected symbol total trades: `{}`\n- Rejected portfolio DD cooldown: `{}`\n- Rejected symbol DD cooldown: `{}`\n\n",
            best.profile.name,
            best.gate_status,
            best.gate_reason,
            best.max_capital_used,
            best.avg_capital_used_on_entry,
            best.decision_metrics.max_capital_drawdown,
            best.decision_metrics.max_capital_drawdown_pct * 100.0,
            best.decision_metrics.cost_25_max_capital_drawdown,
            best.decision_metrics.cost_25_max_capital_drawdown_pct * 100.0,
            best.rejected_capital_budget,
            best.rejected_symbol_allocation,
            best.rejected_open_positions,
            best.rejected_symbol_positions,
            best.rejected_symbol_total_trades,
            best.rejected_portfolio_drawdown_cooldown,
            best.rejected_symbol_drawdown_cooldown
        ));
        out.push_str(&format!(
            "- Canary readiness: `{}` ({})\n- Recommended canary capital fraction: `{:.1}%`\n- Max symbol PnL share: `{:.1}%` from `{}`\n- Symbol ablation passes: `{}`\n- Strategy ablation passes: `{}`\n\n",
            best.canary_readiness.status,
            best.canary_readiness.reason,
            best.canary_readiness.recommended_capital_fraction * 100.0,
            best.canary_readiness.max_symbol_pnl_share * 100.0,
            best.canary_readiness
                .max_symbol
                .as_deref()
                .unwrap_or("n/a"),
            best.canary_readiness.symbol_ablation_passes,
            best.canary_readiness.strategy_ablation_passes
        ));
        out.push_str(&format!(
            "- Covered-call strike floor: `{:.1}%` of assigned put strike\n- Average covered calls per completed cycle: `{:.2}`\n\n",
            best.profile.covered_call_min_strike_pct_of_assigned * 100.0,
            avg_wheel_call_count(&best.trades)
        ));

        out.push_str("## Best Profile Risk Attribution\n\n");
        out.push_str(&format!(
            "- Wheel PnL: `{:.2}` across `{}` trades\n- Put-debit PnL: `{:.2}`\n- Call-debit PnL: `{:.2}`\n- Wheel assignment rate: `{:.1}%` (`{}` assigned cycles)\n- Marked-stock PnL: `{:.2}` across `{}` cycles\n- Worst marked-stock loss: `{:.2}`\n- Worst trade loss: `{:.2}`\n- Average wheel hold: `{:.1}` days\n- Average assigned-cycle hold: `{:.1}` days\n\n",
            best.risk_summary.wheel_pnl,
            best.risk_summary.wheel_trades,
            best.risk_summary.put_debit_pnl,
            best.risk_summary.call_debit_pnl,
            best.risk_summary.assignment_rate * 100.0,
            best.risk_summary.assigned_cycles,
            best.risk_summary.marked_stock_pnl,
            best.risk_summary.marked_stock_cycles,
            best.risk_summary.worst_marked_stock_loss,
            best.risk_summary.worst_trade_loss,
            best.risk_summary.avg_wheel_days_held,
            best.risk_summary.avg_assigned_days_held
        ));

        out.push_str("| Strategy | Trades | PnL | Win Rate | PF | Avg PnL | Avg Hold | Worst Trade | Assigned | Marked |\n");
        out.push_str("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        for summary in &best.strategy_summaries {
            out.push_str(&format!(
                "| {} | {} | {:.2} | {:.1}% | {:.2} | {:.2} | {:.1} | {:.2} | {} | {} |\n",
                summary.strategy,
                summary.trades,
                summary.pnl,
                summary.win_rate * 100.0,
                summary.profit_factor,
                summary.avg_pnl,
                summary.avg_days_held,
                summary.worst_trade_pnl,
                summary.assigned_cycles,
                summary.marked_stock_cycles
            ));
        }
        out.push('\n');

        out.push_str("| Symbol | Trades | PnL | Assigned | Called Away | Marked Stock | Wheel | Put Debit | Call Debit |\n");
        out.push_str("|---|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        for summary in &best.symbol_summaries {
            let symbol_trades = best
                .trades
                .iter()
                .filter(|trade| trade.symbol == summary.symbol)
                .collect::<Vec<_>>();
            let wheel_trades = symbol_trades
                .iter()
                .filter(|trade| trade.strategy == SpreadStructure::Wheel)
                .count();
            let put_debit_trades = symbol_trades
                .iter()
                .filter(|trade| trade.strategy == SpreadStructure::PutDebitSpread)
                .count();
            let call_debit_trades = symbol_trades
                .iter()
                .filter(|trade| trade.strategy == SpreadStructure::CallDebitSpread)
                .count();
            out.push_str(&format!(
                "| {} | {} | {:.2} | {} | {} | {} | {} | {} | {} |\n",
                summary.symbol,
                summary.trades,
                summary.pnl,
                summary.assigned_cycles,
                summary.called_away_cycles,
                summary.marked_stock_cycles,
                wheel_trades,
                put_debit_trades,
                call_debit_trades
            ));
        }
        out.push('\n');

        out.push_str("## Best Profile Ablations\n\n");
        out.push_str(
            "| Removed | Gate | Trades | Required | PnL | PF | Max DD | $25 Cost PnL | Reason |\n",
        );
        out.push_str("|---|---|---:|---:|---:|---:|---:|---:|---|\n");
        for ablation in &best.ablations {
            let cost_25_pnl = ablation
                .metrics
                .cost_stress
                .iter()
                .find(|stress| (stress.per_trade_cost - 25.0).abs() < f64::EPSILON)
                .map(|stress| stress.total_pnl)
                .unwrap_or(ablation.metrics.total_pnl);
            out.push_str(&format!(
                "| {} | {} | {} | {} | {:.2} | {:.2} | {:.2}% | {:.2} | {} |\n",
                ablation.label,
                ablation.gate_status,
                ablation.metrics.trades,
                ablation.metrics.required_trades,
                ablation.metrics.total_pnl,
                ablation.metrics.profit_factor,
                ablation.metrics.max_drawdown * 100.0,
                cost_25_pnl,
                ablation.gate_reason
            ));
        }
        out.push('\n');

        out.push_str("## Latest Portfolio Actions\n\n");
        if best.latest_actions.is_empty() {
            out.push_str("No accepted selector actions in the latest lookback window.\n\n");
        } else {
            out.push_str("| Status | Symbol | Strategy | Entry | Exit | Expiration | DTE | PnL | Reason | Short Delta | Underlying |\n");
            out.push_str("|---|---|---|---:|---:|---:|---:|---:|---|---:|---:|\n");
            for action in &best.latest_actions {
                out.push_str(&format!(
                    "| {} | {} | {} | {} | {} | {} | {} | {:.2} | {} | {:.3} | {:.2} |\n",
                    action.status,
                    action.symbol,
                    action.strategy.as_str(),
                    action.entry_date,
                    action.exit_date,
                    action.expiration,
                    action.dte_entry,
                    action.pnl,
                    action.exit_reason,
                    action.short_delta,
                    action.underlying_price
                ));
            }
            out.push('\n');
        }
    }
    out
}

async fn first_expiration_with_rows(
    symbol: &str,
    expirations: &[NaiveDate],
    bounds: ExpirationLoadBounds,
    raw_dir: &Path,
    force_refresh: bool,
    option_data_mode: OptionDataMode,
) -> Result<Option<usize>> {
    let mut low = 0;
    let mut high = expirations.len();
    while low < high {
        let mid = low + (high - low) / 2;
        let expiration = expirations[mid];
        let Some((start, end)) = expiration_load_window(expiration, bounds) else {
            low = mid + 1;
            continue;
        };
        let has_rows = match load_expiration_rows_for_mode(
            symbol,
            expiration,
            start,
            end,
            raw_dir,
            force_refresh,
            option_data_mode,
        )
        .await
        {
            Ok(rows) => rows.has_required_rows(option_data_mode),
            Err(error) if is_non_retryable_thetadata_error(&error) => false,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("probing loadable expiration {expiration}"));
            }
        };
        if has_rows {
            high = mid;
        } else {
            low = mid + 1;
        }
    }
    Ok((low < expirations.len()).then_some(low))
}

fn option_row_lookback_days(
    profile_family: ResearchProfileFamily,
    max_expirations: Option<usize>,
    max_regime_lookback_days: i64,
) -> i64 {
    match (profile_family, max_expirations) {
        (
            ResearchProfileFamily::Weekly
            | ResearchProfileFamily::WeeklyFarOtm
            | ResearchProfileFamily::WeeklyPutDebit
            | ResearchProfileFamily::WeeklyCallCredit
            | ResearchProfileFamily::WeeklyCallDebit
            | ResearchProfileFamily::WeeklyWheel,
            None,
        ) => 0,
        _ => max_regime_lookback_days,
    }
}

fn option_data_mode_for_profile_family(profile_family: ResearchProfileFamily) -> OptionDataMode {
    match profile_family {
        ResearchProfileFamily::Swing
        | ResearchProfileFamily::Weekly
        | ResearchProfileFamily::WeeklyFarOtm
        | ResearchProfileFamily::WeeklyPutDebit => OptionDataMode::Single(OptionRight::Put),
        ResearchProfileFamily::WeeklyCallCredit | ResearchProfileFamily::WeeklyCallDebit => {
            OptionDataMode::Single(OptionRight::Call)
        }
        ResearchProfileFamily::WeeklyWheel => OptionDataMode::PutAndCall,
    }
}

fn expiration_load_window(
    expiration: NaiveDate,
    bounds: ExpirationLoadBounds,
) -> Option<(NaiveDate, NaiveDate)> {
    let earliest_entry = bounds
        .from
        .max(expiration - Duration::days(bounds.max_entry_dte));
    let start = earliest_entry - Duration::days(bounds.option_row_lookback_days);
    let exit_grace_end =
        expiration - Duration::days(bounds.min_force_close_dte) + Duration::days(7);
    let end = bounds.to.min(exit_grace_end).min(expiration);
    (start <= end).then_some((start, end))
}

fn expiration_load_failure_from_error(
    expiration: NaiveDate,
    error: &anyhow::Error,
) -> ExpirationLoadFailure {
    let message = compact_error_message(&format!("{error:#}"));
    ExpirationLoadFailure {
        expiration,
        message,
    }
}

fn compact_error_message(message: &str) -> String {
    message
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" | ")
}

fn evenly_spaced<T: Copy>(items: Vec<T>, max: usize) -> Vec<T> {
    if items.len() <= max {
        return items;
    }
    if max == 0 {
        return Vec::new();
    }
    if max == 1 {
        return vec![items[0]];
    }
    let last = items.len() - 1;
    let denominator = max - 1;
    (0..max)
        .map(|idx| {
            let selected = idx * last / denominator;
            items[selected]
        })
        .collect()
}

fn symbol_slug(symbol: &str) -> String {
    symbol
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect()
}

fn detector_strategy_summary(profile: &ResearchProfile) -> DetectorStrategySummary {
    DetectorStrategySummary {
        name: format!(
            "put_spread_detector_dte{}_{}_delta{:.2}_{:.2}_credit{:.2}_width{:.0}_{:.0}",
            profile.min_dte,
            profile.max_dte,
            profile.min_short_delta_abs,
            profile.max_short_delta_abs,
            profile.min_credit_width,
            profile.min_width,
            profile.max_width
        ),
        min_dte: profile.min_dte,
        max_dte: profile.max_dte,
        min_short_delta_abs: profile.min_short_delta_abs,
        max_short_delta_abs: profile.max_short_delta_abs,
        min_width: profile.min_width,
        max_width: profile.max_width,
        min_credit_width: profile.min_credit_width,
        max_quote_width_pct_of_mid: profile.max_quote_width_pct_of_mid,
        max_quote_width_abs: profile.max_quote_width_abs,
        min_short_oi: profile.min_short_oi,
        min_long_oi: profile.min_long_oi,
        filters: detector_filters(profile),
    }
}

fn detector_filters(profile: &ResearchProfile) -> Vec<String> {
    let mut filters = Vec::new();
    if let Some(days) = profile.trend_lookback_days {
        if let Some(min_return) = profile.min_underlying_return {
            filters.push(format!("trend_{days}d_return>={min_return:.3}"));
        }
        if let Some(max_return) = profile.max_underlying_return {
            filters.push(format!("trend_{days}d_return<={max_return:.3}"));
        }
    }
    if let Some(days) = profile.drawdown_lookback_days {
        if let Some(min_drawdown) = profile.min_underlying_drawdown {
            filters.push(format!("drawdown_{days}d>={min_drawdown:.3}"));
        }
        if let Some(max_drawdown) = profile.max_underlying_drawdown {
            filters.push(format!("drawdown_{days}d<={max_drawdown:.3}"));
        }
    }
    if let Some(gate) = &profile.return_or_drawdown_gate {
        filters.push(format!(
            "return_or_drawdown(return>={}, drawdown>={})",
            format_optional_threshold(gate.min_underlying_return),
            format_optional_threshold(gate.min_underlying_drawdown)
        ));
    }
    if let Some(guard) = &profile.trend_drawdown_guard {
        filters.push(format!(
            "trend_drawdown_guard(return>={:.3}, drawdown<={:.3})",
            guard.min_underlying_return, guard.max_underlying_drawdown
        ));
    }
    if let Some(guard) = &profile.weak_trend_pullback_guard {
        filters.push(format!(
            "weak_trend_pullback_guard(return<={:.3}, drawdown={:.3}-{:.3})",
            guard.max_underlying_return,
            guard.min_underlying_drawdown,
            guard.max_underlying_drawdown
        ));
    }
    if let Some(guard) = &profile.risk_regime_cooldown_guard {
        filters.push(format!(
            "risk_regime_cooldown(return>={:.3}, drawdown>{:.3}, days={})",
            guard.min_underlying_return,
            guard.max_underlying_drawdown,
            profile.risk_regime_cooldown_days
        ));
    }
    if let Some(days) = profile.realized_vol_lookback_days {
        if let Some(min_realized_vol) = profile.min_realized_vol {
            filters.push(format!("realized_vol_{days}d>={min_realized_vol:.3}"));
        }
        if let Some(max_realized_vol) = profile.max_realized_vol {
            filters.push(format!("realized_vol_{days}d<={max_realized_vol:.3}"));
        }
    }
    if let Some(min_short_otm_pct) = profile.min_short_otm_pct {
        filters.push(format!("short_otm_pct>={min_short_otm_pct:.3}"));
    }
    if let Some(min_short_iv) = profile.min_short_iv {
        filters.push(format!("short_iv>={min_short_iv:.3}"));
    }
    if let Some(max_short_iv) = profile.max_short_iv {
        filters.push(format!("short_iv<={max_short_iv:.3}"));
    }
    if let Some(max_short_leg_delta_abs) = profile.max_short_leg_delta_abs {
        filters.push(format!("short_leg_delta_abs<={max_short_leg_delta_abs:.3}"));
    }
    if let Some(min_long_short_iv_diff) = profile.min_long_short_iv_diff {
        filters.push(format!("long_short_iv_diff>={min_long_short_iv_diff:.4}"));
    }
    if profile.covered_call_min_strike_pct_of_assigned
        < default_covered_call_min_strike_pct() - f64::EPSILON
    {
        filters.push(format!(
            "covered_call_strike_floor>={:.1}%_of_assignment",
            profile.covered_call_min_strike_pct_of_assigned * 100.0
        ));
    }
    if let (Some(delta), Some(width)) = (
        profile.low_delta_width_cap_delta_abs,
        profile.low_delta_width_cap,
    ) {
        filters.push(format!("delta<={delta:.3}_width<={width:.0}"));
    }
    filters
}

fn execution_strategy_summary(profile: &ResearchProfile) -> ExecutionStrategySummary {
    let candidate_selector = if profile.prefer_farther_otm {
        "farther_otm_then_credit"
    } else {
        "highest_return_on_risk"
    };
    let concurrency_suffix =
        if profile.max_concurrent_positions > 1 || profile.min_entry_spacing_days != 1 {
            format!(
                "_maxpos{}_gap{}d",
                profile.max_concurrent_positions, profile.min_entry_spacing_days
            )
        } else {
            String::new()
        };
    let call_floor_suffix = if profile.structure == SpreadStructure::Wheel
        && profile.covered_call_min_strike_pct_of_assigned
            < default_covered_call_min_strike_pct() - f64::EPSILON
    {
        format!(
            "_callfloor{:.0}",
            profile.covered_call_min_strike_pct_of_assigned * 100.0
        )
    } else {
        String::new()
    };
    ExecutionStrategySummary {
        name: format!(
            "put_spread_execution_{}_tp{:.0}_stop{:.1}_close{}d{}{}",
            candidate_selector,
            profile.take_profit_pct * 100.0,
            profile.stop_loss_multiple,
            profile.force_close_dte,
            concurrency_suffix,
            call_floor_suffix
        ),
        candidate_selector: candidate_selector.to_owned(),
        entry_fill_model: "short_bid_minus_long_ask".to_owned(),
        exit_fill_model: "short_ask_minus_long_bid".to_owned(),
        take_profit_pct: profile.take_profit_pct,
        stop_loss_multiple: profile.stop_loss_multiple,
        force_close_dte: profile.force_close_dte,
        max_hold_days: profile.max_hold_days,
        stop_loss_cooldown_days: profile.stop_loss_cooldown_days,
        max_concurrent_positions: profile.max_concurrent_positions,
        min_entry_spacing_days: profile.min_entry_spacing_days,
    }
}

fn format_optional_threshold(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.3}"))
        .unwrap_or_else(|| "none".to_owned())
}

fn profile_result_order(a: &ProfileResult, b: &ProfileResult) -> Ordering {
    profile_rank_order(&a.metrics, &a.profile, &b.metrics, &b.profile)
}

fn profile_rank_order(
    a_metrics: &ResearchMetrics,
    a_profile: &ResearchProfile,
    b_metrics: &ResearchMetrics,
    b_profile: &ResearchProfile,
) -> Ordering {
    b_metrics
        .robust_ranking_eligible
        .cmp(&a_metrics.robust_ranking_eligible)
        .then_with(|| b_metrics.score.total_cmp(&a_metrics.score))
        .then_with(|| b_metrics.robust_score.total_cmp(&a_metrics.robust_score))
        .then_with(|| {
            risk_regime_cooldown_tiebreak_score(b_profile)
                .cmp(&risk_regime_cooldown_tiebreak_score(a_profile))
        })
        .then_with(|| profile_complexity(a_profile).cmp(&profile_complexity(b_profile)))
        .then_with(|| a_profile.name.cmp(&b_profile.name))
}

fn risk_regime_cooldown_tiebreak_score(profile: &ResearchProfile) -> i64 {
    if profile.risk_regime_cooldown_guard.is_none() {
        return 0;
    }
    1_000 + profile.risk_regime_cooldown_days.max(0)
}

fn profile_complexity(profile: &ResearchProfile) -> usize {
    let baseline = ResearchProfile::legacy_baseline();
    let mut complexity = 0;
    complexity += usize::from(profile.min_dte != baseline.min_dte);
    complexity += usize::from(profile.max_dte != baseline.max_dte);
    complexity += usize::from(profile.force_close_dte != baseline.force_close_dte);
    complexity += usize::from(float_differs(
        profile.min_short_delta_abs,
        baseline.min_short_delta_abs,
    ));
    complexity += usize::from(float_differs(
        profile.max_short_delta_abs,
        baseline.max_short_delta_abs,
    ));
    complexity += usize::from(profile.max_short_leg_delta_abs != baseline.max_short_leg_delta_abs);
    complexity += usize::from(float_differs(profile.min_width, baseline.min_width));
    complexity += usize::from(float_differs(profile.max_width, baseline.max_width));
    complexity += usize::from(float_differs(
        profile.min_credit_width,
        baseline.min_credit_width,
    ));
    complexity += usize::from(float_differs(
        profile.max_quote_width_pct_of_mid,
        baseline.max_quote_width_pct_of_mid,
    ));
    complexity += usize::from(float_differs(
        profile.max_quote_width_abs,
        baseline.max_quote_width_abs,
    ));
    complexity += usize::from(profile.min_short_oi != baseline.min_short_oi);
    complexity += usize::from(profile.min_long_oi != baseline.min_long_oi);
    complexity += usize::from(float_differs(
        profile.take_profit_pct,
        baseline.take_profit_pct,
    ));
    complexity += usize::from(float_differs(
        profile.stop_loss_multiple,
        baseline.stop_loss_multiple,
    ));
    complexity += option_complexity(&profile.max_hold_days, &baseline.max_hold_days);
    complexity += option_complexity(&profile.trend_lookback_days, &baseline.trend_lookback_days);
    complexity += option_complexity(
        &profile.min_underlying_return,
        &baseline.min_underlying_return,
    );
    complexity += option_complexity(
        &profile.max_underlying_return,
        &baseline.max_underlying_return,
    );
    complexity += option_complexity(
        &profile.drawdown_lookback_days,
        &baseline.drawdown_lookback_days,
    );
    complexity += option_complexity(
        &profile.min_underlying_drawdown,
        &baseline.min_underlying_drawdown,
    );
    complexity += option_complexity(
        &profile.max_underlying_drawdown,
        &baseline.max_underlying_drawdown,
    );
    complexity += option_complexity(
        &profile.return_or_drawdown_gate,
        &baseline.return_or_drawdown_gate,
    );
    complexity += option_complexity(
        &profile.trend_drawdown_guard,
        &baseline.trend_drawdown_guard,
    );
    complexity += option_complexity(
        &profile.weak_trend_pullback_guard,
        &baseline.weak_trend_pullback_guard,
    );
    complexity += option_complexity(
        &profile.risk_regime_cooldown_guard,
        &baseline.risk_regime_cooldown_guard,
    );
    complexity +=
        usize::from(profile.risk_regime_cooldown_days != baseline.risk_regime_cooldown_days);
    complexity += option_complexity(
        &profile.realized_vol_lookback_days,
        &baseline.realized_vol_lookback_days,
    );
    complexity += option_complexity(&profile.min_realized_vol, &baseline.min_realized_vol);
    complexity += option_complexity(&profile.max_realized_vol, &baseline.max_realized_vol);
    complexity += option_complexity(&profile.min_short_otm_pct, &baseline.min_short_otm_pct);
    complexity += option_complexity(&profile.min_short_iv, &baseline.min_short_iv);
    complexity += option_complexity(&profile.max_short_iv, &baseline.max_short_iv);
    complexity += option_complexity(
        &profile.min_long_short_iv_diff,
        &baseline.min_long_short_iv_diff,
    );
    complexity += option_complexity(
        &profile.low_delta_width_cap_delta_abs,
        &baseline.low_delta_width_cap_delta_abs,
    );
    complexity += option_complexity(&profile.low_delta_width_cap, &baseline.low_delta_width_cap);
    complexity += usize::from(profile.prefer_farther_otm != baseline.prefer_farther_otm);
    complexity += usize::from(profile.stop_loss_cooldown_days != baseline.stop_loss_cooldown_days);
    complexity +=
        usize::from(profile.max_concurrent_positions != baseline.max_concurrent_positions);
    complexity += usize::from(profile.min_entry_spacing_days != baseline.min_entry_spacing_days);
    complexity += usize::from(float_differs(
        profile.min_trades_per_year,
        baseline.min_trades_per_year,
    ));
    complexity
}

fn option_complexity<T: PartialEq>(value: &Option<T>, baseline: &Option<T>) -> usize {
    usize::from(value != baseline)
}

fn float_differs(a: f64, b: f64) -> bool {
    (a - b).abs() > f64::EPSILON
}

fn walk_forward(
    profile_results: &[ProfileResult],
    from: NaiveDate,
    to: NaiveDate,
) -> WalkForwardResult {
    walk_forward_with_mode(
        profile_results,
        from,
        to,
        "expanding",
        None,
        WALK_FORWARD_MIN_TRAIN_DAYS,
    )
}

fn rolling_walk_forward(
    profile_results: &[ProfileResult],
    from: NaiveDate,
    to: NaiveDate,
) -> WalkForwardResult {
    walk_forward_with_mode(
        profile_results,
        from,
        to,
        "rolling",
        Some(ROLLING_WALK_FORWARD_TRAIN_DAYS),
        ROLLING_WALK_FORWARD_TRAIN_DAYS,
    )
}

fn walk_forward_with_mode(
    profile_results: &[ProfileResult],
    from: NaiveDate,
    to: NaiveDate,
    mode: &str,
    train_window_days: Option<i64>,
    min_train_days: i64,
) -> WalkForwardResult {
    let mut years = Vec::new();
    let mut trades = Vec::new();
    let mut selected_profile_counts = BTreeMap::new();
    let mut next_entry_date = NaiveDate::MIN;

    for test_year in from.year()..=to.year() {
        let test_from = from.max(NaiveDate::from_ymd_opt(test_year, 1, 1).unwrap());
        let test_to = to.min(NaiveDate::from_ymd_opt(test_year, 12, 31).unwrap());
        if test_from > test_to {
            continue;
        }

        let train_to = test_from - Duration::days(1);
        if train_to < from || (train_to - from).num_days() < min_train_days {
            continue;
        }
        let train_from = train_window_days
            .map(|days| (train_to - Duration::days(days - 1)).max(from))
            .unwrap_or(from);
        if (train_to - train_from).num_days() + 1 < min_train_days {
            continue;
        }

        let ranked_selections = rank_walk_forward_profiles(profile_results, train_from, train_to);
        if ranked_selections.is_empty() {
            continue;
        }
        let selection_idx = selected_walk_forward_profile_index(&ranked_selections, train_to)
            .expect("ranked_selections is non-empty");
        let selection = &ranked_selections[selection_idx];

        let active = deployable_training_selection(selection, train_to);
        let mut accepted = Vec::new();
        if active {
            let mut test_trades =
                filter_trades_by_entry_date(&selection.result.trades, test_from, test_to);
            test_trades.sort_by(trade_chronological_order);

            for trade in test_trades {
                if trade.entry_date < next_entry_date {
                    continue;
                }
                next_entry_date = next_entry_date_after_trade(&trade, &selection.result.profile);
                accepted.push(trade);
            }
        }
        let selection_candidates = ranked_selections
            .iter()
            .take(WALK_FORWARD_SELECTION_DIAGNOSTIC_LIMIT)
            .enumerate()
            .map(|(idx, candidate)| {
                let candidate_active = deployable_training_selection(candidate, train_to);
                let test_trades = if candidate_active {
                    filter_trades_by_entry_date(&candidate.result.trades, test_from, test_to)
                } else {
                    Vec::new()
                };
                WalkForwardSelectionCandidate {
                    rank: idx + 1,
                    profile: candidate.result.profile.name.clone(),
                    active: candidate_active,
                    train_metrics: train_metrics_summary(
                        &candidate.metrics,
                        &candidate.train_trades,
                        train_to,
                    ),
                    test_metrics: period_metrics(
                        "out_of_sample_candidate",
                        &test_trades,
                        test_from,
                        test_to,
                    ),
                }
            })
            .collect();

        *selected_profile_counts
            .entry(selection.result.profile.name.clone())
            .or_insert(0) += 1;
        trades.extend(accepted.iter().cloned());
        years.push(WalkForwardYear {
            test_year,
            train_from,
            train_to,
            test_from,
            test_to,
            active,
            selected_profile: selection.result.profile.name.clone(),
            train_metrics: train_metrics_summary(
                &selection.metrics,
                &selection.train_trades,
                train_to,
            ),
            test_metrics: period_metrics("out_of_sample", &accepted, test_from, test_to),
            selection_candidates,
        });
    }

    let metrics_from = years.first().map(|year| year.test_from).unwrap_or(from);
    let min_trades_per_year = max_profile_min_trades_per_year(profile_results);
    WalkForwardResult {
        mode: mode.to_owned(),
        min_train_days,
        train_window_days,
        years,
        selected_profile_counts,
        metrics: metrics_with_min_trades_per_year(&trades, metrics_from, to, min_trades_per_year),
        trades,
    }
}

fn max_profile_min_trades_per_year(profile_results: &[ProfileResult]) -> f64 {
    profile_results
        .iter()
        .map(|result| result.profile.min_trades_per_year)
        .fold(MIN_RANKING_TRADES_PER_YEAR, f64::max)
}

fn holdout(profile_results: &[ProfileResult], from: NaiveDate, to: NaiveDate) -> HoldoutResult {
    let days = (to - from).num_days();
    let train_to = if days >= 2 {
        from + Duration::days(days / 2)
    } else {
        from
    };
    let test_from = (train_to + Duration::days(1)).min(to);
    let Some(selection) = select_walk_forward_profile(profile_results, from, train_to) else {
        return HoldoutResult {
            train_from: from,
            train_to,
            test_from,
            test_to: to,
            active: false,
            selected_profile: String::new(),
            train_metrics: train_metrics_summary(&metrics(&[], from, train_to), &[], train_to),
            trades: Vec::new(),
            metrics: metrics(&[], test_from, to),
        };
    };

    let active = deployable_training_selection(&selection, train_to);
    let mut trades = if active {
        filter_trades_by_entry_date(&selection.result.trades, test_from, to)
    } else {
        Vec::new()
    };
    trades.sort_by(trade_chronological_order);
    HoldoutResult {
        train_from: from,
        train_to,
        test_from,
        test_to: to,
        active,
        selected_profile: selection.result.profile.name.clone(),
        train_metrics: train_metrics_summary(&selection.metrics, &selection.train_trades, train_to),
        metrics: metrics_for_profile(&trades, test_from, to, &selection.result.profile),
        trades,
    }
}

fn fixed_profile_walk_forward(
    profile_results: &[ProfileResult],
    from: NaiveDate,
    to: NaiveDate,
) -> Vec<FixedProfileWalkForwardResult> {
    let mut results = profile_results
        .iter()
        .map(|result| fixed_profile_walk_forward_for_profile(result, from, to))
        .collect::<Vec<_>>();
    results.sort_by(fixed_profile_walk_forward_result_order);
    results
}

fn fixed_profile_walk_forward_for_profile(
    result: &ProfileResult,
    from: NaiveDate,
    to: NaiveDate,
) -> FixedProfileWalkForwardResult {
    let mut years = Vec::new();
    let mut trades = Vec::new();
    let mut next_entry_date = NaiveDate::MIN;

    for test_year in from.year()..=to.year() {
        let test_from = from.max(NaiveDate::from_ymd_opt(test_year, 1, 1).unwrap());
        let test_to = to.min(NaiveDate::from_ymd_opt(test_year, 12, 31).unwrap());
        if test_from > test_to {
            continue;
        }

        let train_to = test_from - Duration::days(1);
        if train_to < from || (train_to - from).num_days() < WALK_FORWARD_MIN_TRAIN_DAYS {
            continue;
        }
        let train_from = from;
        let train_trades = filter_closed_trades_by_entry_date(&result.trades, train_from, train_to);
        let train_metrics =
            metrics_for_profile(&train_trades, train_from, train_to, &result.profile);
        let selection = WalkForwardSelection {
            result,
            train_trades,
            metrics: train_metrics,
        };
        let active = deployable_training_selection(&selection, train_to);
        let mut accepted = Vec::new();
        if active {
            let mut test_trades = filter_trades_by_entry_date(&result.trades, test_from, test_to);
            test_trades.sort_by(trade_chronological_order);

            for trade in test_trades {
                if trade.entry_date < next_entry_date {
                    continue;
                }
                next_entry_date = next_entry_date_after_trade(&trade, &result.profile);
                accepted.push(trade);
            }
        }

        trades.extend(accepted.iter().cloned());
        years.push(FixedProfileWalkForwardYear {
            test_year,
            train_from,
            train_to,
            test_from,
            test_to,
            active,
            train_metrics: train_metrics_summary(
                &selection.metrics,
                &selection.train_trades,
                train_to,
            ),
            test_metrics: period_metrics(
                "fixed_profile_out_of_sample",
                &accepted,
                test_from,
                test_to,
            ),
        });
    }

    let metrics_from = years.first().map(|year| year.test_from).unwrap_or(from);
    let metrics = metrics_for_profile(&trades, metrics_from, to, &result.profile);
    let active_years = years.iter().filter(|year| year.active).count();
    FixedProfileWalkForwardResult {
        profile: result.profile.clone(),
        detector_strategy: result.detector_strategy.clone(),
        execution_strategy: result.execution_strategy.clone(),
        active_years,
        years,
        trades,
        metrics,
    }
}

fn fixed_profile_walk_forward_result_order(
    a: &FixedProfileWalkForwardResult,
    b: &FixedProfileWalkForwardResult,
) -> Ordering {
    out_of_sample_gate_passes(&b.metrics)
        .cmp(&out_of_sample_gate_passes(&a.metrics))
        .then_with(|| fixed_profile_oos_score_order(a, b))
        .then_with(|| b.metrics.total_pnl.total_cmp(&a.metrics.total_pnl))
        .then_with(|| b.metrics.trades.cmp(&a.metrics.trades))
        .then_with(|| profile_complexity(&a.profile).cmp(&profile_complexity(&b.profile)))
        .then_with(|| a.profile.name.cmp(&b.profile.name))
}

fn fixed_profile_oos_score_order(
    a: &FixedProfileWalkForwardResult,
    b: &FixedProfileWalkForwardResult,
) -> Ordering {
    if a.metrics.ranking_eligible && b.metrics.ranking_eligible {
        b.metrics.score.total_cmp(&a.metrics.score)
    } else {
        Ordering::Equal
    }
}

struct WalkForwardSelection<'a> {
    result: &'a ProfileResult,
    train_trades: Vec<ResearchTrade>,
    metrics: ResearchMetrics,
}

fn select_walk_forward_profile<'a>(
    profile_results: &'a [ProfileResult],
    train_from: NaiveDate,
    train_to: NaiveDate,
) -> Option<WalkForwardSelection<'a>> {
    let mut ranked_selections = rank_walk_forward_profiles(profile_results, train_from, train_to);
    let selection_idx = selected_walk_forward_profile_index(&ranked_selections, train_to)?;
    Some(ranked_selections.remove(selection_idx))
}

fn selected_walk_forward_profile_index(
    ranked_selections: &[WalkForwardSelection<'_>],
    train_to: NaiveDate,
) -> Option<usize> {
    if ranked_selections.is_empty() {
        return None;
    }
    Some(
        ranked_selections
            .iter()
            .position(|selection| deployable_training_selection(selection, train_to))
            .unwrap_or(0),
    )
}

fn rank_walk_forward_profiles<'a>(
    profile_results: &'a [ProfileResult],
    train_from: NaiveDate,
    train_to: NaiveDate,
) -> Vec<WalkForwardSelection<'a>> {
    let mut scored = profile_results
        .iter()
        .map(|result| {
            let train_trades =
                filter_closed_trades_by_entry_date(&result.trades, train_from, train_to);
            let metrics = metrics_for_profile(&train_trades, train_from, train_to, &result.profile);
            WalkForwardSelection {
                result,
                train_trades,
                metrics,
            }
        })
        .collect::<Vec<_>>();
    scored.sort_by(|a, b| {
        profile_rank_order(&a.metrics, &a.result.profile, &b.metrics, &b.result.profile)
    });
    scored
}

fn deployable_training_profile(metrics: &ResearchMetrics) -> bool {
    metrics.robust_ranking_eligible && metrics.robust_score >= MIN_DEPLOYABLE_TRAINING_ROBUST_SCORE
}

fn deployable_training_selection(
    selection: &WalkForwardSelection<'_>,
    train_to: NaiveDate,
) -> bool {
    deployable_training_profile(&selection.metrics)
        && recent_training_activity_gate(&selection.train_trades, train_to)
}

fn recent_training_activity_gate(train_trades: &[ResearchTrade], train_to: NaiveDate) -> bool {
    let recent_from = train_to - Duration::days(RECENT_TRAIN_ACTIVITY_DAYS - 1);
    train_trades
        .iter()
        .any(|trade| trade.entry_date >= recent_from && trade.entry_date <= train_to)
}

fn deployment_gate_for(
    profile_results: &[ProfileResult],
    walk_forward: &WalkForwardResult,
    holdout: &HoldoutResult,
) -> DeploymentGate {
    let best_profile_gate = profile_results
        .first()
        .is_some_and(|result| deployable_training_profile(&result.metrics));
    let walk_forward_oos_gate = out_of_sample_gate_passes(&walk_forward.metrics);
    let holdout_oos_gate = holdout.active && out_of_sample_gate_passes(&holdout.metrics);
    let pass = best_profile_gate && walk_forward_oos_gate && holdout_oos_gate;
    DeploymentGate {
        status: format_gate(pass).to_owned(),
        pass,
        best_profile_gate,
        walk_forward_oos_gate,
        holdout_oos_gate,
    }
}

fn plateau_status_for(
    profile_results: &[ProfileResult],
    deployment_gate: &DeploymentGate,
    walk_forward: &WalkForwardResult,
    holdout: &HoldoutResult,
) -> PlateauStatus {
    plateau_status_from_counts(
        profile_results.len(),
        walk_forward.years.len(),
        holdout.active,
        deployment_gate,
    )
}

fn plateau_status_from_counts(
    profiles_evaluated: usize,
    walk_forward_years: usize,
    _holdout_active: bool,
    deployment_gate: &DeploymentGate,
) -> PlateauStatus {
    let profile_variants_evaluated = profiles_evaluated.saturating_sub(1);
    let enough_variants = profile_variants_evaluated >= PLATEAU_MIN_PROFILE_VARIANTS;
    let enough_walk_forward = walk_forward_years >= PLATEAU_MIN_WALK_FORWARD_YEARS;
    let detector_status = if deployment_gate.best_profile_gate {
        "robust"
    } else {
        "blocked"
    };
    let execution_strategy_status =
        if deployment_gate.walk_forward_oos_gate && deployment_gate.holdout_oos_gate {
            "oos_pass"
        } else {
            "oos_blocked"
        };

    let (status, expansion_ready, reason, next_action) = if deployment_gate.pass {
        (
            "live_gate_passed",
            false,
            "detector and execution strategy both passed deployment gates",
            "review shadow-live readiness before any broker integration",
        )
    } else if deployment_gate.best_profile_gate
        && !deployment_gate.walk_forward_oos_gate
        && !deployment_gate.holdout_oos_gate
        && enough_variants
        && enough_walk_forward
    {
        (
            "plateau_expand_universe",
            true,
            "current-symbol detector is robust in sample, but execution validation is still blocked out of sample after broad profile search",
            "run the same separated detector and execution-strategy research on the default liquid single-stock universe",
        )
    } else if !enough_variants {
        (
            "continue_symbol_research",
            false,
            "profile search has not reached the minimum variant coverage for plateau",
            "continue current-symbol detector and execution-strategy variants",
        )
    } else if !deployment_gate.best_profile_gate {
        (
            "continue_symbol_research",
            false,
            "no robust in-sample detector is available yet",
            "continue current-symbol detector search",
        )
    } else if !enough_walk_forward {
        (
            "continue_symbol_research",
            false,
            "walk-forward coverage is too thin for plateau",
            "extend current-symbol history or walk-forward coverage before expanding symbols",
        )
    } else {
        (
            "continue_symbol_research",
            false,
            "at least one out-of-sample gate still needs targeted validation",
            "continue current-symbol execution-strategy research",
        )
    };

    PlateauStatus {
        status: status.to_owned(),
        expansion_ready,
        profiles_evaluated,
        profile_variants_evaluated,
        min_profile_variants: PLATEAU_MIN_PROFILE_VARIANTS,
        walk_forward_years,
        min_walk_forward_years: PLATEAU_MIN_WALK_FORWARD_YEARS,
        detector_status: detector_status.to_owned(),
        execution_strategy_status: execution_strategy_status.to_owned(),
        reason: reason.to_owned(),
        next_action: next_action.to_owned(),
    }
}

fn filter_trades_by_entry_date(
    trades: &[ResearchTrade],
    from: NaiveDate,
    to: NaiveDate,
) -> Vec<ResearchTrade> {
    trades
        .iter()
        .filter(|trade| trade.entry_date >= from && trade.entry_date <= to)
        .cloned()
        .collect()
}

fn filter_closed_trades_by_entry_date(
    trades: &[ResearchTrade],
    from: NaiveDate,
    to: NaiveDate,
) -> Vec<ResearchTrade> {
    trades
        .iter()
        .filter(|trade| trade.entry_date >= from && trade.entry_date <= to && trade.exit_date <= to)
        .cloned()
        .collect()
}

fn latest_signal_for_best_profile(
    profile_results: &[ProfileResult],
    rows_by_expiration: &BTreeMap<NaiveDate, Vec<OptionDay>>,
    from: NaiveDate,
    to: NaiveDate,
) -> Option<ResearchSignal> {
    let best = profile_results.first()?;
    if !deployable_training_profile(&best.metrics) {
        return None;
    }
    latest_signal_for_profile(best, rows_by_expiration, from, to)
}

fn latest_signal_for_profile(
    result: &ProfileResult,
    rows_by_expiration: &BTreeMap<NaiveDate, Vec<OptionDay>>,
    from: NaiveDate,
    to: NaiveDate,
) -> Option<ResearchSignal> {
    let entry_from = from.max(next_signal_entry_date(result, to));
    if entry_from > to {
        return None;
    }
    let candidates = generate_candidates(rows_by_expiration, &result.profile, entry_from, to);
    let (latest_entry_date, day_candidates) =
        latest_signal_day_candidates(&candidates, &result.profile)?;
    let status = if latest_entry_date == to {
        "entry_candidate"
    } else {
        "open_candidate"
    };
    Some(signal_from_candidate(
        day_candidates[0],
        &result.profile.name,
        to,
        status,
    ))
}

fn latest_signal_day_candidates<'a>(
    candidates: &'a [Candidate],
    profile: &ResearchProfile,
) -> Option<(NaiveDate, Vec<&'a Candidate>)> {
    let mut by_date: BTreeMap<NaiveDate, Vec<&Candidate>> = BTreeMap::new();
    for candidate in candidates {
        by_date
            .entry(candidate.entry_date)
            .or_default()
            .push(candidate);
    }

    let mut next_entry_date = NaiveDate::MIN;
    let mut latest = None;
    for (date, mut day_candidates) in by_date {
        if date < next_entry_date {
            continue;
        }
        if risk_regime_cooldown_triggered(&day_candidates, profile) {
            next_entry_date = next_entry_date_after_risk_regime(date, profile);
            continue;
        }
        day_candidates.sort_by(|a, b| candidate_quality_order(a, b, profile));
        latest = Some((date, day_candidates));
    }
    latest
}

fn next_signal_entry_date(result: &ProfileResult, as_of: NaiveDate) -> NaiveDate {
    result
        .trades
        .iter()
        .filter(|trade| trade.exit_date <= as_of)
        .max_by(|a, b| trade_chronological_order(a, b))
        .map(|trade| next_entry_date_after_trade(trade, &result.profile))
        .unwrap_or(NaiveDate::MIN)
}

fn signal_from_candidate(
    candidate: &Candidate,
    profile_name: &str,
    as_of: NaiveDate,
    status: &str,
) -> ResearchSignal {
    ResearchSignal {
        as_of,
        status: status.to_owned(),
        profile_name: profile_name.to_owned(),
        entry_date: candidate.entry_date,
        expiration: candidate.expiration,
        dte_entry: (candidate.expiration - candidate.entry_date).num_days(),
        short_put: candidate.short.strike,
        long_put: candidate.long.strike,
        width: candidate.width,
        entry_credit: candidate.credit,
        max_profit: candidate.credit * 100.0,
        max_loss: candidate.max_loss_per_share * 100.0,
        return_on_risk: candidate.return_on_risk,
        short_delta: candidate.short.delta,
        long_delta: candidate.long.delta,
        short_oi: candidate.short.open_interest,
        long_oi: candidate.long.open_interest,
        underlying_price: candidate.short.underlying_price,
        short_otm_pct: candidate.short_otm_pct,
        underlying_lookback_return: candidate.underlying_lookback_return,
        underlying_recent_drawdown: candidate.underlying_recent_drawdown,
        underlying_realized_vol: candidate.underlying_realized_vol,
        short_iv: candidate.short_iv,
        long_iv: candidate.long_iv,
    }
}

fn train_metrics_summary(
    metrics: &ResearchMetrics,
    train_trades: &[ResearchTrade],
    train_to: NaiveDate,
) -> WalkForwardTrainMetrics {
    let recent_from = train_to - Duration::days(RECENT_TRAIN_ACTIVITY_DAYS - 1);
    let recent_trades = train_trades
        .iter()
        .filter(|trade| trade.entry_date >= recent_from && trade.entry_date <= train_to)
        .count();
    let last_entry_date = train_trades.iter().map(|trade| trade.entry_date).max();
    let days_since_last_entry = last_entry_date.map(|date| (train_to - date).num_days());
    WalkForwardTrainMetrics {
        trades: metrics.trades,
        total_pnl: metrics.total_pnl,
        score: metrics.score,
        robust_score: metrics.robust_score,
        ranking_eligible: metrics.ranking_eligible,
        robust_ranking_eligible: metrics.robust_ranking_eligible,
        min_deployable_robust_score: MIN_DEPLOYABLE_TRAINING_ROBUST_SCORE,
        robust_score_gate: metrics.robust_score >= MIN_DEPLOYABLE_TRAINING_ROBUST_SCORE,
        recent_activity_window_days: RECENT_TRAIN_ACTIVITY_DAYS,
        recent_trades,
        recent_activity_gate: recent_trades > 0,
        last_entry_date,
        days_since_last_entry,
    }
}

fn research_profiles_for(family: ResearchProfileFamily) -> Vec<ResearchProfile> {
    match family {
        ResearchProfileFamily::Swing => research_profiles(),
        ResearchProfileFamily::Weekly => weekly_research_profiles(),
        ResearchProfileFamily::WeeklyFarOtm => weekly_far_otm_research_profiles(),
        ResearchProfileFamily::WeeklyPutDebit => weekly_put_debit_research_profiles(),
        ResearchProfileFamily::WeeklyCallCredit => weekly_call_credit_research_profiles(),
        ResearchProfileFamily::WeeklyCallDebit => weekly_call_debit_research_profiles(),
        ResearchProfileFamily::WeeklyWheel => weekly_wheel_research_profiles(),
    }
}

fn weekly_research_profiles() -> Vec<ResearchProfile> {
    let baseline = ResearchProfile::weekly_baseline();
    let mut profiles = Vec::new();

    for (dte_name, min_dte, max_dte, max_hold_days) in [
        ("dte1_7", 1, 7, 5),
        ("dte3_10", 3, 10, 7),
        ("dte5_14", 5, 14, 7),
        ("dte7_14", 7, 14, 10),
    ] {
        for (width_name, max_width) in [("w5", 5.0), ("w10", 10.0), ("w15", 15.0), ("w25", 25.0)] {
            for (delta_name, min_delta, max_delta) in [
                ("delta05_20", 0.05, 0.20),
                ("delta10_25", 0.10, 0.25),
                ("delta10_30", 0.10, 0.30),
            ] {
                for (take_name, take_profit_pct) in
                    [("take25", 0.25), ("take33", 0.33), ("take50", 0.50)]
                {
                    let mut profile = baseline.clone();
                    profile.name = format!(
                        "weekly_{dte_name}_{width_name}_{delta_name}_{take_name}_farther_otm"
                    );
                    profile.min_dte = min_dte;
                    profile.max_dte = max_dte;
                    profile.force_close_dte = 1;
                    profile.max_hold_days = Some(max_hold_days);
                    profile.max_width = max_width;
                    profile.min_short_delta_abs = min_delta;
                    profile.max_short_delta_abs = max_delta;
                    profile.take_profit_pct = take_profit_pct;
                    profiles.push(profile);
                }
            }
        }
    }

    for (suffix, max_concurrent_positions, min_entry_spacing_days) in [
        ("maxpos1_gap1", 1, 1),
        ("maxpos3_gap1", 3, 1),
        ("maxpos5_gap1", 5, 1),
        ("maxpos5_gap2", 5, 2),
    ] {
        let mut profile = baseline.clone();
        profile.name = format!("weekly_core_{suffix}_dte5_14_delta10_30_take33");
        profile.max_concurrent_positions = max_concurrent_positions;
        profile.min_entry_spacing_days = min_entry_spacing_days;
        profiles.push(profile);
    }

    for (suffix, min_return, max_drawdown, max_realized_vol) in [
        ("trend20_min0_dd25_rv150", 0.0, 0.25, 1.50),
        ("trend20_min0_dd15_rv125", 0.0, 0.15, 1.25),
        ("trend20_min5_dd20_rv125", 0.05, 0.20, 1.25),
        ("trend60_min0_dd20_rv100", 0.0, 0.20, 1.00),
    ] {
        let mut profile = baseline.clone();
        profile.name = format!("weekly_core_{suffix}_dte5_14_delta10_30_take33");
        profile.min_underlying_return = Some(min_return);
        profile.max_underlying_drawdown = Some(max_drawdown);
        profile.max_realized_vol = Some(max_realized_vol);
        if suffix.starts_with("trend60") {
            profile.trend_lookback_days = Some(60);
        }
        profiles.push(profile);
    }

    for (suffix, stop_loss_multiple, stop_loss_cooldown_days, risk_cooldown_days) in [
        ("stop125_cool3_risk3", 1.25, 3, 3),
        ("stop150_cool3_risk3", 1.50, 3, 3),
        ("stop175_cool5_risk5", 1.75, 5, 5),
        ("stop200_cool0_risk0", 2.00, 0, 0),
    ] {
        let mut profile = baseline.clone();
        profile.name = format!("weekly_core_{suffix}_dte5_14_delta10_30_take33");
        profile.stop_loss_multiple = stop_loss_multiple;
        profile.stop_loss_cooldown_days = stop_loss_cooldown_days;
        profile.risk_regime_cooldown_days = risk_cooldown_days;
        if risk_cooldown_days == 0 {
            profile.risk_regime_cooldown_guard = None;
        }
        profiles.push(profile);
    }

    profiles
}

fn weekly_far_otm_research_profiles() -> Vec<ResearchProfile> {
    let mut baseline = ResearchProfile::weekly_baseline();
    baseline.name = "weekly_far_otm_baseline_dte3_14_delta02_15_credit08_take33_stop125".to_owned();
    baseline.min_short_delta_abs = 0.02;
    baseline.max_short_delta_abs = 0.15;
    baseline.min_width = 1.0;
    baseline.max_width = 10.0;
    baseline.min_credit_width = 0.08;
    baseline.take_profit_pct = 0.33;
    baseline.stop_loss_multiple = 1.25;
    baseline.max_hold_days = Some(5);
    baseline.force_close_dte = 1;
    baseline.max_concurrent_positions = 2;
    baseline.min_entry_spacing_days = 2;
    baseline.risk_regime_cooldown_days = 5;
    baseline.stop_loss_cooldown_days = 5;
    baseline.min_short_otm_pct = Some(0.05);
    baseline.prefer_farther_otm = true;

    let mut profiles = Vec::new();
    for (dte_name, min_dte, max_dte, max_hold_days) in [
        ("dte1_7", 1, 7, 3),
        ("dte3_10", 3, 10, 5),
        ("dte5_14", 5, 14, 7),
        ("dte7_14", 7, 14, 7),
    ] {
        for (width_name, max_width) in [("w5", 5.0), ("w10", 10.0), ("w15", 15.0), ("w25", 25.0)] {
            for (delta_name, min_delta, max_delta, min_otm_pct) in [
                ("delta02_08", 0.02, 0.08, 0.08),
                ("delta03_10", 0.03, 0.10, 0.07),
                ("delta05_12", 0.05, 0.12, 0.06),
                ("delta05_15", 0.05, 0.15, 0.05),
            ] {
                for (take_name, take_profit_pct) in [("take25", 0.25), ("take33", 0.33)] {
                    let mut profile = baseline.clone();
                    profile.name = format!(
                        "weekly_far_otm_{dte_name}_{width_name}_{delta_name}_{take_name}_stop125"
                    );
                    profile.min_dte = min_dte;
                    profile.max_dte = max_dte;
                    profile.max_hold_days = Some(max_hold_days);
                    profile.max_width = max_width;
                    profile.min_short_delta_abs = min_delta;
                    profile.max_short_delta_abs = max_delta;
                    profile.min_short_otm_pct = Some(min_otm_pct);
                    profile.take_profit_pct = take_profit_pct;
                    profiles.push(profile);
                }
            }
        }
    }

    for (suffix, stop_loss_multiple, stop_loss_cooldown_days, risk_cooldown_days) in [
        ("stop100_cool5_risk5", 1.00, 5, 5),
        ("stop125_cool5_risk5", 1.25, 5, 5),
        ("stop150_cool7_risk7", 1.50, 7, 7),
    ] {
        let mut profile = baseline.clone();
        profile.name = format!("weekly_far_otm_core_{suffix}_dte5_14_delta03_10_take33");
        profile.min_dte = 5;
        profile.max_dte = 14;
        profile.min_short_delta_abs = 0.03;
        profile.max_short_delta_abs = 0.10;
        profile.min_short_otm_pct = Some(0.07);
        profile.stop_loss_multiple = stop_loss_multiple;
        profile.stop_loss_cooldown_days = stop_loss_cooldown_days;
        profile.risk_regime_cooldown_days = risk_cooldown_days;
        profiles.push(profile);
    }

    for (suffix, max_concurrent_positions, min_entry_spacing_days) in [
        ("maxpos1_gap1", 1, 1),
        ("maxpos1_gap3", 1, 3),
        ("maxpos2_gap2", 2, 2),
        ("maxpos3_gap3", 3, 3),
    ] {
        let mut profile = baseline.clone();
        profile.name = format!("weekly_far_otm_core_{suffix}_dte5_14_delta03_10_take33_stop125");
        profile.min_dte = 5;
        profile.max_dte = 14;
        profile.min_short_delta_abs = 0.03;
        profile.max_short_delta_abs = 0.10;
        profile.min_short_otm_pct = Some(0.07);
        profile.max_concurrent_positions = max_concurrent_positions;
        profile.min_entry_spacing_days = min_entry_spacing_days;
        profiles.push(profile);
    }

    profiles
}

fn weekly_call_credit_research_profiles() -> Vec<ResearchProfile> {
    let mut baseline = ResearchProfile::weekly_baseline();
    baseline.structure = SpreadStructure::CallCreditSpread;
    baseline.name = "weekly_call_credit_baseline_dte3_10_delta10_30_credit08_take33".to_owned();
    baseline.min_dte = 3;
    baseline.max_dte = 10;
    baseline.force_close_dte = 1;
    baseline.min_short_delta_abs = 0.10;
    baseline.max_short_delta_abs = 0.30;
    baseline.min_width = 1.0;
    baseline.max_width = 10.0;
    baseline.min_credit_width = 0.08;
    baseline.take_profit_pct = 0.33;
    baseline.stop_loss_multiple = 1.50;
    baseline.max_hold_days = Some(5);
    baseline.min_short_otm_pct = Some(0.03);
    baseline.max_concurrent_positions = 2;
    baseline.min_entry_spacing_days = 2;
    baseline.risk_regime_cooldown_guard = None;
    baseline.risk_regime_cooldown_days = 0;
    baseline.stop_loss_cooldown_days = 5;
    baseline.prefer_farther_otm = true;

    let mut profiles = Vec::new();
    for (dte_name, min_dte, max_dte, max_hold_days) in [("dte1_7", 1, 7, 3), ("dte3_10", 3, 10, 5)]
    {
        for (width_name, max_width) in [("w5", 5.0), ("w10", 10.0), ("w15", 15.0)] {
            for (delta_name, min_delta, max_delta) in [
                ("delta05_20", 0.05, 0.20),
                ("delta10_25", 0.10, 0.25),
                ("delta10_30", 0.10, 0.30),
            ] {
                for (credit_name, min_credit_width) in [("credit06", 0.06), ("credit08", 0.08)] {
                    let mut profile = baseline.clone();
                    profile.name = format!(
                        "weekly_call_credit_weak_{dte_name}_{width_name}_{delta_name}_{credit_name}_take33"
                    );
                    profile.min_dte = min_dte;
                    profile.max_dte = max_dte;
                    profile.max_hold_days = Some(max_hold_days);
                    profile.max_width = max_width;
                    profile.min_short_delta_abs = min_delta;
                    profile.max_short_delta_abs = max_delta;
                    profile.min_credit_width = min_credit_width;
                    profile.trend_lookback_days = Some(20);
                    profile.min_underlying_return = None;
                    profile.max_underlying_return = Some(0.05);
                    profile.drawdown_lookback_days = Some(20);
                    profile.min_underlying_drawdown = Some(0.02);
                    profile.max_underlying_drawdown = Some(0.30);
                    profiles.push(profile);
                }
            }
        }
    }

    for (delta_name, min_delta, max_delta) in
        [("delta05_20", 0.05, 0.20), ("delta10_25", 0.10, 0.25)]
    {
        let mut profile = baseline.clone();
        profile.name =
            format!("weekly_call_credit_overbought_dte3_10_w10_{delta_name}_credit08_take33");
        profile.min_short_delta_abs = min_delta;
        profile.max_short_delta_abs = max_delta;
        profile.max_width = 10.0;
        profile.min_credit_width = 0.08;
        profile.trend_lookback_days = Some(20);
        profile.min_underlying_return = Some(0.10);
        profile.max_underlying_return = Some(0.35);
        profile.drawdown_lookback_days = Some(20);
        profile.max_underlying_drawdown = Some(0.10);
        profile.realized_vol_lookback_days = Some(20);
        profile.max_realized_vol = Some(1.50);
        profiles.push(profile);
    }

    profiles
}

fn weekly_put_debit_research_profiles() -> Vec<ResearchProfile> {
    let baseline = ResearchProfile::weekly_put_debit_baseline();
    let mut profiles = Vec::new();

    for (dte_name, min_dte, max_dte, max_hold_days) in [
        ("dte1_7", 1, 7, 3),
        ("dte3_10", 3, 10, 5),
        ("dte5_14", 5, 14, 7),
        ("dte7_14", 7, 14, 7),
    ] {
        for (width_name, max_width) in [("w5", 5.0), ("w10", 10.0), ("w15", 15.0), ("w25", 25.0)] {
            for (delta_name, min_delta, max_delta) in [
                ("delta20_45", 0.20, 0.45),
                ("delta25_55", 0.25, 0.55),
                ("delta30_60", 0.30, 0.60),
            ] {
                for (take_name, take_profit_pct) in
                    [("take25", 0.25), ("take33", 0.33), ("take50", 0.50)]
                {
                    let mut profile = baseline.clone();
                    profile.name = format!(
                        "weekly_put_debit_{dte_name}_{width_name}_{delta_name}_{take_name}"
                    );
                    profile.min_dte = min_dte;
                    profile.max_dte = max_dte;
                    profile.max_hold_days = Some(max_hold_days);
                    profile.max_width = max_width;
                    profile.min_short_delta_abs = min_delta;
                    profile.max_short_delta_abs = max_delta;
                    profile.take_profit_pct = take_profit_pct;
                    profiles.push(profile);
                }
            }
        }
    }

    for (suffix, max_debit_width) in [("debit35", 0.35), ("debit45", 0.45), ("debit55", 0.55)] {
        let mut profile = baseline.clone();
        profile.name = format!("weekly_put_debit_core_{suffix}_dte3_10_delta25_55_take33");
        profile.max_debit_width = Some(max_debit_width);
        profiles.push(profile);
    }

    for (suffix, stop_loss_multiple, stop_loss_cooldown_days) in [
        ("stop35_cool3", 0.35, 3),
        ("stop50_cool3", 0.50, 3),
        ("stop65_cool5", 0.65, 5),
    ] {
        let mut profile = baseline.clone();
        profile.name = format!("weekly_put_debit_core_{suffix}_dte3_10_delta25_55_take33");
        profile.stop_loss_multiple = stop_loss_multiple;
        profile.stop_loss_cooldown_days = stop_loss_cooldown_days;
        profiles.push(profile);
    }

    for (min_debit_name, min_debit) in [
        ("mindebit30", 0.30),
        ("mindebit40", 0.40),
        ("mindebit50", 0.50),
    ] {
        for (min_width_name, min_width) in [("minw1", 1.0), ("minw3", 3.0), ("minw5", 5.0)] {
            for (take_name, take_profit_pct) in
                [("take25", 0.25), ("take33", 0.33), ("take50", 0.50)]
            {
                let mut profile = baseline.clone();
                profile.name = format!(
                    "weekly_put_debit_costaware_dte3_10_w25_delta20_45_{min_debit_name}_{min_width_name}_{take_name}"
                );
                profile.min_dte = 3;
                profile.max_dte = 10;
                profile.max_hold_days = Some(5);
                profile.min_width = min_width;
                profile.max_width = 25.0;
                profile.min_short_delta_abs = 0.20;
                profile.max_short_delta_abs = 0.45;
                profile.min_debit = Some(min_debit);
                profile.take_profit_pct = take_profit_pct;
                profiles.push(profile);
            }
        }
    }

    for (delta_name, min_delta, max_delta) in
        [("delta25_55", 0.25, 0.55), ("delta30_60", 0.30, 0.60)]
    {
        for (take_name, take_profit_pct) in [("take33", 0.33), ("take50", 0.50)] {
            for (regime_name, min_realized_vol, max_return, max_drawdown) in [
                ("rv30_ret10_dd12", 0.30, 0.10, 0.12),
                ("rv30_ret15_dd12", 0.30, 0.15, 0.12),
                ("rv35_ret10_dd12", 0.35, 0.10, 0.12),
            ] {
                let mut profile = baseline.clone();
                profile.name = format!(
                    "weekly_put_debit_regime_dte1_7_w25_{delta_name}_{regime_name}_{take_name}"
                );
                profile.min_dte = 1;
                profile.max_dte = 7;
                profile.max_hold_days = Some(3);
                profile.max_width = 25.0;
                profile.min_short_delta_abs = min_delta;
                profile.max_short_delta_abs = max_delta;
                profile.take_profit_pct = take_profit_pct;
                profile.min_realized_vol = Some(min_realized_vol);
                profile.max_underlying_return = Some(max_return);
                profile.max_underlying_drawdown = Some(max_drawdown);
                profiles.push(profile);
            }
        }
    }

    for (delta_name, min_delta, max_delta) in [("delta30_60", 0.30, 0.60)] {
        for (take_name, take_profit_pct) in [("take50", 0.50)] {
            let mut profile = baseline.clone();
            profile.name = format!(
                "weekly_put_debit_drawdown_dte1_7_w25_{delta_name}_rv35_ret05_dd05_12_{take_name}"
            );
            profile.min_dte = 1;
            profile.max_dte = 7;
            profile.max_hold_days = Some(3);
            profile.max_width = 25.0;
            profile.min_short_delta_abs = min_delta;
            profile.max_short_delta_abs = max_delta;
            profile.take_profit_pct = take_profit_pct;
            profile.min_realized_vol = Some(0.35);
            profile.max_underlying_return = Some(0.05);
            profile.min_underlying_drawdown = Some(0.05);
            profile.max_underlying_drawdown = Some(0.12);
            profiles.push(profile);
        }
    }

    for (delta_name, min_delta, max_delta) in [("delta30_60", 0.30, 0.60)] {
        for (take_name, take_profit_pct) in [("take50", 0.50)] {
            let mut profile = baseline.clone();
            profile.name = format!(
                "weekly_put_debit_disciplined_drawdown_dte1_7_w15_{delta_name}_debit35_rv35_ret05_dd05_12_{take_name}"
            );
            profile.min_dte = 1;
            profile.max_dte = 7;
            profile.max_hold_days = Some(3);
            profile.max_width = 15.0;
            profile.max_debit_width = Some(0.35);
            profile.min_short_delta_abs = min_delta;
            profile.max_short_delta_abs = max_delta;
            profile.take_profit_pct = take_profit_pct;
            profile.min_realized_vol = Some(0.35);
            profile.max_underlying_return = Some(0.05);
            profile.min_underlying_drawdown = Some(0.05);
            profile.max_underlying_drawdown = Some(0.12);
            profiles.push(profile);
        }
    }

    for (guard_name, max_short_leg_delta_abs) in [("outerdelta15", 0.15), ("outerdelta18", 0.18)] {
        let mut profile = baseline.clone();
        profile.name = format!(
            "weekly_put_debit_legguard_{guard_name}_dte1_7_w15_delta30_60_debit35_rv35_ret05_dd05_12_take50"
        );
        profile.min_dte = 1;
        profile.max_dte = 7;
        profile.max_hold_days = Some(3);
        profile.max_width = 15.0;
        profile.max_debit_width = Some(0.35);
        profile.min_short_delta_abs = 0.30;
        profile.max_short_delta_abs = 0.60;
        profile.max_short_leg_delta_abs = Some(max_short_leg_delta_abs);
        profile.take_profit_pct = 0.50;
        profile.min_realized_vol = Some(0.35);
        profile.max_underlying_return = Some(0.05);
        profile.min_underlying_drawdown = Some(0.05);
        profile.max_underlying_drawdown = Some(0.12);
        profiles.push(profile);
    }

    profiles
}

fn weekly_call_debit_research_profiles() -> Vec<ResearchProfile> {
    let baseline = ResearchProfile::weekly_call_debit_baseline();
    let mut profiles = Vec::new();

    for (dte_name, min_dte, max_dte, max_hold_days) in [
        ("dte1_7", 1, 7, 3),
        ("dte3_10", 3, 10, 5),
        ("dte5_14", 5, 14, 7),
        ("dte7_14", 7, 14, 7),
    ] {
        for (width_name, max_width) in [("w5", 5.0), ("w10", 10.0), ("w15", 15.0), ("w25", 25.0)] {
            for (delta_name, min_delta, max_delta) in [
                ("delta20_45", 0.20, 0.45),
                ("delta25_55", 0.25, 0.55),
                ("delta30_60", 0.30, 0.60),
            ] {
                for (take_name, take_profit_pct) in
                    [("take25", 0.25), ("take33", 0.33), ("take50", 0.50)]
                {
                    let mut profile = baseline.clone();
                    profile.name = format!(
                        "weekly_call_debit_{dte_name}_{width_name}_{delta_name}_{take_name}"
                    );
                    profile.min_dte = min_dte;
                    profile.max_dte = max_dte;
                    profile.max_hold_days = Some(max_hold_days);
                    profile.max_width = max_width;
                    profile.min_short_delta_abs = min_delta;
                    profile.max_short_delta_abs = max_delta;
                    profile.take_profit_pct = take_profit_pct;
                    profiles.push(profile);
                }
            }
        }
    }

    for (suffix, max_debit_width) in [("debit35", 0.35), ("debit45", 0.45), ("debit55", 0.55)] {
        let mut profile = baseline.clone();
        profile.name = format!("weekly_call_debit_core_{suffix}_dte3_10_delta25_55_take33");
        profile.max_debit_width = Some(max_debit_width);
        profiles.push(profile);
    }

    for (min_debit_name, min_debit) in [
        ("mindebit30", 0.30),
        ("mindebit35", 0.35),
        ("mindebit40", 0.40),
        ("mindebit50", 0.50),
    ] {
        for (min_width_name, min_width) in [("minw1", 1.0), ("minw3", 3.0), ("minw5", 5.0)] {
            for (take_name, take_profit_pct) in
                [("take25", 0.25), ("take33", 0.33), ("take50", 0.50)]
            {
                let mut profile = baseline.clone();
                profile.name = format!(
                    "weekly_call_debit_costaware_dte3_10_w25_delta20_45_{min_debit_name}_{min_width_name}_{take_name}"
                );
                profile.min_dte = 3;
                profile.max_dte = 10;
                profile.max_hold_days = Some(5);
                profile.min_width = min_width;
                profile.max_width = 25.0;
                profile.min_short_delta_abs = 0.20;
                profile.max_short_delta_abs = 0.45;
                profile.min_debit = Some(min_debit);
                profile.take_profit_pct = take_profit_pct;
                profiles.push(profile);
            }
        }
    }

    for (take_name, take_profit_pct) in [("take50", 0.50)] {
        let mut profile = baseline.clone();
        profile.name = format!(
            "weekly_call_debit_disciplined_costaware_dte3_10_w15_delta20_45_mindebit30_minw5_debit35_{take_name}"
        );
        profile.min_dte = 3;
        profile.max_dte = 10;
        profile.max_hold_days = Some(5);
        profile.min_width = 5.0;
        profile.max_width = 15.0;
        profile.max_debit_width = Some(0.35);
        profile.min_short_delta_abs = 0.20;
        profile.max_short_delta_abs = 0.45;
        profile.min_debit = Some(0.30);
        profile.take_profit_pct = take_profit_pct;
        profiles.push(profile);
    }

    for (take_name, take_profit_pct) in [("take50", 0.50)] {
        let mut profile = baseline.clone();
        profile.name = format!(
            "weekly_call_debit_balanced_costaware_dte3_10_w20_delta20_45_mindebit30_minw5_debit45_{take_name}"
        );
        profile.min_dte = 3;
        profile.max_dte = 10;
        profile.max_hold_days = Some(5);
        profile.min_width = 5.0;
        profile.max_width = 20.0;
        profile.max_debit_width = Some(0.45);
        profile.min_short_delta_abs = 0.20;
        profile.max_short_delta_abs = 0.45;
        profile.min_debit = Some(0.30);
        profile.take_profit_pct = take_profit_pct;
        profiles.push(profile);
    }

    for (guard_name, max_short_leg_delta_abs) in [("outerdelta15", 0.15), ("outerdelta18", 0.18)] {
        let mut profile = baseline.clone();
        profile.name = format!(
            "weekly_call_debit_legguard_{guard_name}_dte3_10_w20_delta20_45_mindebit30_minw5_debit45_take50"
        );
        profile.min_dte = 3;
        profile.max_dte = 10;
        profile.max_hold_days = Some(5);
        profile.min_width = 5.0;
        profile.max_width = 20.0;
        profile.max_debit_width = Some(0.45);
        profile.min_short_delta_abs = 0.20;
        profile.max_short_delta_abs = 0.45;
        profile.max_short_leg_delta_abs = Some(max_short_leg_delta_abs);
        profile.min_debit = Some(0.30);
        profile.take_profit_pct = 0.50;
        profiles.push(profile);
    }

    for (trend_name, min_return, max_return, max_drawdown, min_realized_vol) in [
        ("trend20_ret0_20_dd20_rv25", 0.00, 0.20, 0.20, 0.25),
        ("trend20_ret2_25_dd20_rv25", 0.02, 0.25, 0.20, 0.25),
        ("trend20_ret5_25_dd15_rv30", 0.05, 0.25, 0.15, 0.30),
        ("trend60_ret5_35_dd20_rv25", 0.05, 0.35, 0.20, 0.25),
    ] {
        for (delta_name, min_delta, max_delta) in
            [("delta25_55", 0.25, 0.55), ("delta30_60", 0.30, 0.60)]
        {
            for (take_name, take_profit_pct) in [("take33", 0.33), ("take50", 0.50)] {
                let mut profile = baseline.clone();
                profile.name = format!(
                    "weekly_call_debit_regime_dte1_7_w25_{delta_name}_{trend_name}_{take_name}"
                );
                profile.min_dte = 1;
                profile.max_dte = 7;
                profile.max_hold_days = Some(3);
                profile.max_width = 25.0;
                profile.min_short_delta_abs = min_delta;
                profile.max_short_delta_abs = max_delta;
                profile.take_profit_pct = take_profit_pct;
                profile.trend_lookback_days = if trend_name.starts_with("trend60") {
                    Some(60)
                } else {
                    Some(20)
                };
                profile.min_underlying_return = Some(min_return);
                profile.max_underlying_return = Some(max_return);
                profile.max_underlying_drawdown = Some(max_drawdown);
                profile.min_realized_vol = Some(min_realized_vol);
                profiles.push(profile);
            }
        }
    }

    profiles
}

fn weekly_wheel_research_profiles() -> Vec<ResearchProfile> {
    let baseline = ResearchProfile::weekly_wheel_baseline();
    let mut profiles = Vec::new();

    for (dte_name, min_dte, max_dte) in [("dte1_7", 1, 7), ("dte3_10", 3, 10), ("dte5_14", 5, 14)] {
        for (delta_name, min_delta, max_delta) in [
            ("delta05_20", 0.05, 0.20),
            ("delta10_25", 0.10, 0.25),
            ("delta10_30", 0.10, 0.30),
        ] {
            for (credit_name, min_credit_width) in
                [("credit01", 0.01), ("credit02", 0.02), ("credit03", 0.03)]
            {
                for (hold_name, max_stock_hold_days) in
                    [("hold21", 21), ("hold45", 45), ("hold60", 60)]
                {
                    let mut profile = baseline.clone();
                    profile.name =
                        format!("weekly_wheel_{dte_name}_{delta_name}_{credit_name}_{hold_name}");
                    profile.min_dte = min_dte;
                    profile.max_dte = max_dte;
                    profile.min_short_delta_abs = min_delta;
                    profile.max_short_delta_abs = max_delta;
                    profile.min_credit_width = min_credit_width;
                    profile.max_hold_days = Some(max_stock_hold_days);
                    profiles.push(profile);
                }
            }
        }
    }

    for (suffix, min_return, max_drawdown, max_realized_vol) in [
        ("trend20_min0_dd25_rv150", 0.0, 0.25, 1.50),
        ("trend20_min5_dd20_rv125", 0.05, 0.20, 1.25),
        ("trend60_min0_dd20_rv100", 0.0, 0.20, 1.00),
    ] {
        let mut profile = baseline.clone();
        profile.name = format!("weekly_wheel_core_{suffix}_dte3_10_delta10_30_credit02");
        profile.min_dte = 3;
        profile.max_dte = 10;
        profile.min_credit_width = 0.02;
        profile.min_underlying_return = Some(min_return);
        profile.max_underlying_drawdown = Some(max_drawdown);
        profile.max_realized_vol = Some(max_realized_vol);
        if suffix.starts_with("trend60") {
            profile.trend_lookback_days = Some(60);
        }
        profiles.push(profile);
    }

    for (suffix, max_stock_hold_days) in [("hold30", 30), ("hold45", 45), ("hold60", 60)] {
        let mut profile = baseline.clone();
        profile.name = format!("weekly_wheel_core_{suffix}_dte3_10_delta10_30_credit02");
        profile.max_hold_days = Some(max_stock_hold_days);
        profiles.push(profile);
    }

    for (dte_name, min_dte, max_dte) in [("dte5_14", 5, 14), ("dte7_14", 7, 14)] {
        for (delta_name, min_delta, max_delta) in
            [("delta05_15", 0.05, 0.15), ("delta05_20", 0.05, 0.20)]
        {
            for (otm_name, min_short_otm_pct) in [("otm05", 0.05), ("otm08", 0.08), ("otm10", 0.10)]
            {
                for (rv_name, max_realized_vol) in [("rv100", 1.00), ("rv125", 1.25)] {
                    for (hold_name, max_stock_hold_days) in [("hold30", 30), ("hold45", 45)] {
                        let mut profile = baseline.clone();
                        profile.name = format!(
                            "weekly_wheel_disciplined_{dte_name}_{delta_name}_{otm_name}_{rv_name}_{hold_name}"
                        );
                        profile.min_dte = min_dte;
                        profile.max_dte = max_dte;
                        profile.min_short_delta_abs = min_delta;
                        profile.max_short_delta_abs = max_delta;
                        profile.min_credit_width = 0.01;
                        profile.min_short_otm_pct = Some(min_short_otm_pct);
                        profile.trend_lookback_days = Some(60);
                        profile.min_underlying_return = Some(0.0);
                        profile.drawdown_lookback_days = Some(20);
                        profile.max_underlying_drawdown = Some(0.20);
                        profile.realized_vol_lookback_days = Some(20);
                        profile.max_realized_vol = Some(max_realized_vol);
                        profile.max_hold_days = Some(max_stock_hold_days);
                        profile.prefer_farther_otm = true;
                        profiles.push(profile);
                    }
                }
            }
        }
    }

    for (dte_name, min_dte, max_dte) in [("dte3_10", 3, 10), ("dte5_14", 5, 14)] {
        for (floor_name, call_floor) in [("callfloor98", 0.98), ("callfloor95", 0.95)] {
            for (credit_name, min_credit_width) in [("credit01", 0.01), ("credit02", 0.02)] {
                for (hold_name, max_stock_hold_days) in [("hold21", 21), ("hold45", 45)] {
                    let mut profile = baseline.clone();
                    profile.name = format!(
                        "weekly_wheel_inventory_exit_{dte_name}_{floor_name}_{credit_name}_{hold_name}"
                    );
                    profile.min_dte = min_dte;
                    profile.max_dte = max_dte;
                    profile.min_short_delta_abs = 0.10;
                    profile.max_short_delta_abs = 0.30;
                    profile.min_credit_width = min_credit_width;
                    profile.max_hold_days = Some(max_stock_hold_days);
                    profile.covered_call_min_strike_pct_of_assigned = call_floor;
                    profiles.push(profile);
                }
            }
        }
    }

    for (floor_name, call_floor) in [("callfloor98", 0.98), ("callfloor95", 0.95)] {
        for (rv_name, max_realized_vol) in [("rv100", 1.00), ("rv125", 1.25)] {
            for (hold_name, max_stock_hold_days) in [("hold21", 21), ("hold30", 30), ("hold45", 45)]
            {
                let mut profile = baseline.clone();
                profile.name = format!(
                    "weekly_wheel_guarded_inventory_exit_{floor_name}_{rv_name}_dte5_14_delta05_20_otm05_{hold_name}"
                );
                profile.min_dte = 5;
                profile.max_dte = 14;
                profile.min_short_delta_abs = 0.05;
                profile.max_short_delta_abs = 0.20;
                profile.min_credit_width = 0.01;
                profile.min_short_otm_pct = Some(0.05);
                profile.trend_lookback_days = Some(60);
                profile.min_underlying_return = Some(0.0);
                profile.drawdown_lookback_days = Some(20);
                profile.max_underlying_drawdown = Some(0.20);
                profile.realized_vol_lookback_days = Some(20);
                profile.max_realized_vol = Some(max_realized_vol);
                profile.max_hold_days = Some(max_stock_hold_days);
                profile.prefer_farther_otm = true;
                profile.covered_call_min_strike_pct_of_assigned = call_floor;
                profiles.push(profile);
            }
        }
    }

    profiles
}

fn portfolio_selector_profiles() -> Vec<PortfolioSelectorProfile> {
    let put_credit_profiles = weekly_research_profiles();
    let call_credit_profiles = weekly_call_credit_research_profiles();
    let wheel_profiles = weekly_wheel_research_profiles();
    let put_debit_profiles = weekly_put_debit_research_profiles();
    let call_debit_profiles = weekly_call_debit_research_profiles();

    let put_credit_core = profile_named(
        &put_credit_profiles,
        "weekly_core_maxpos5_gap1_dte5_14_delta10_30_take33",
    );
    let put_credit_farther_otm = profile_named(
        &put_credit_profiles,
        "weekly_dte3_10_w10_delta10_30_take25_farther_otm",
    );
    let put_credit_trend = profile_named(
        &put_credit_profiles,
        "weekly_core_trend20_min5_dd20_rv125_dte5_14_delta10_30_take33",
    );
    let call_credit_weak = profile_named(
        &call_credit_profiles,
        "weekly_call_credit_weak_dte3_10_w10_delta10_30_credit08_take33",
    );
    let call_credit_overbought = profile_named(
        &call_credit_profiles,
        "weekly_call_credit_overbought_dte3_10_w10_delta10_25_credit08_take33",
    );
    let wheel_inventory = profile_named(
        &wheel_profiles,
        "weekly_wheel_inventory_exit_dte3_10_callfloor95_credit01_hold21",
    );
    let wheel_economic = profile_named(
        &wheel_profiles,
        "weekly_wheel_dte5_14_delta05_20_credit01_hold60",
    );
    let wheel_guarded = profile_named(
        &wheel_profiles,
        "weekly_wheel_guarded_inventory_exit_callfloor95_rv125_dte5_14_delta05_20_otm05_hold45",
    );
    let wheel_guarded_fast = profile_named(
        &wheel_profiles,
        "weekly_wheel_guarded_inventory_exit_callfloor95_rv125_dte5_14_delta05_20_otm05_hold21",
    );
    let put_crash = profile_named(
        &put_debit_profiles,
        "weekly_put_debit_regime_dte1_7_w25_delta30_60_rv35_ret10_dd12_take50",
    );
    let put_drawdown = profile_named(
        &put_debit_profiles,
        "weekly_put_debit_drawdown_dte1_7_w25_delta30_60_rv35_ret05_dd05_12_take50",
    );
    let put_disciplined_drawdown = profile_named(
        &put_debit_profiles,
        "weekly_put_debit_disciplined_drawdown_dte1_7_w15_delta30_60_debit35_rv35_ret05_dd05_12_take50",
    );
    let put_legguard_15 = profile_named(
        &put_debit_profiles,
        "weekly_put_debit_legguard_outerdelta15_dte1_7_w15_delta30_60_debit35_rv35_ret05_dd05_12_take50",
    );
    let put_legguard_18 = profile_named(
        &put_debit_profiles,
        "weekly_put_debit_legguard_outerdelta18_dte1_7_w15_delta30_60_debit35_rv35_ret05_dd05_12_take50",
    );
    let put_pullback = profile_named(
        &put_debit_profiles,
        "weekly_put_debit_regime_dte1_7_w25_delta30_60_rv30_ret10_dd12_take50",
    );
    let put_costaware = profile_named(
        &put_debit_profiles,
        "weekly_put_debit_costaware_dte3_10_w25_delta20_45_mindebit30_minw5_take50",
    );
    let put_pltr_wide = profile_named(
        &put_debit_profiles,
        "weekly_put_debit_dte3_10_w15_delta25_55_take25",
    );
    let call_trend = profile_named(
        &call_debit_profiles,
        "weekly_call_debit_regime_dte1_7_w25_delta30_60_trend20_ret5_25_dd15_rv30_take50",
    );
    let call_wide_dte3_10 = profile_named(
        &call_debit_profiles,
        "weekly_call_debit_dte3_10_w25_delta30_60_take25",
    );
    let call_wide_dte3_10_take33 = profile_named(
        &call_debit_profiles,
        "weekly_call_debit_dte3_10_w25_delta30_60_take33",
    );
    let call_wide_dte3_10_take50 = profile_named(
        &call_debit_profiles,
        "weekly_call_debit_dte3_10_w25_delta30_60_take50",
    );
    let call_costaware = profile_named(
        &call_debit_profiles,
        "weekly_call_debit_costaware_dte3_10_w25_delta20_45_mindebit30_minw5_take50",
    );
    let call_orcl_costaware_mindebit30_minw1 = profile_named(
        &call_debit_profiles,
        "weekly_call_debit_costaware_dte3_10_w25_delta20_45_mindebit30_minw1_take25",
    );
    let call_orcl_costaware_mindebit35_minw1 = profile_named(
        &call_debit_profiles,
        "weekly_call_debit_costaware_dte3_10_w25_delta20_45_mindebit35_minw1_take25",
    );
    let call_orcl_costaware_mindebit40_minw1 = profile_named(
        &call_debit_profiles,
        "weekly_call_debit_costaware_dte3_10_w25_delta20_45_mindebit40_minw1_take25",
    );
    let call_orcl_costaware_minw1 = profile_named(
        &call_debit_profiles,
        "weekly_call_debit_costaware_dte3_10_w25_delta20_45_mindebit50_minw1_take25",
    );
    let call_orcl_costaware_minw3 = profile_named(
        &call_debit_profiles,
        "weekly_call_debit_costaware_dte3_10_w25_delta20_45_mindebit50_minw3_take25",
    );
    let call_orcl_costaware_minw5 = profile_named(
        &call_debit_profiles,
        "weekly_call_debit_costaware_dte3_10_w25_delta20_45_mindebit50_minw5_take25",
    );
    let call_disciplined_costaware = profile_named(
        &call_debit_profiles,
        "weekly_call_debit_disciplined_costaware_dte3_10_w15_delta20_45_mindebit30_minw5_debit35_take50",
    );
    let call_balanced_costaware = profile_named(
        &call_debit_profiles,
        "weekly_call_debit_balanced_costaware_dte3_10_w20_delta20_45_mindebit30_minw5_debit45_take50",
    );
    let call_legguard_15 = profile_named(
        &call_debit_profiles,
        "weekly_call_debit_legguard_outerdelta15_dte3_10_w20_delta20_45_mindebit30_minw5_debit45_take50",
    );
    let call_legguard_18 = profile_named(
        &call_debit_profiles,
        "weekly_call_debit_legguard_outerdelta18_dte3_10_w20_delta20_45_mindebit30_minw5_debit45_take50",
    );

    vec![
        selector_profile_with_credit(
            "selector_credit_put_spread_only",
            Some(put_credit_core),
            None,
            None,
            None,
        ),
        selector_profile_with_credit(
            "selector_farther_otm_credit_put_spread_only",
            Some(put_credit_farther_otm),
            None,
            None,
            None,
        ),
        selector_profile_with_credit(
            "selector_trend_credit_put_spread_plus_crash_put_and_call_debits",
            Some(put_credit_trend),
            None,
            Some(put_crash),
            Some(call_trend),
        ),
        selector_profile_with_credit(
            "selector_economic_wheel_plus_credit_put_spread_and_debits",
            Some(put_credit_trend),
            Some(wheel_economic),
            Some(put_crash),
            Some(call_trend),
        ),
        selector_profile_with_credit(
            "selector_call_credit_weak_only",
            Some(call_credit_weak),
            None,
            None,
            None,
        ),
        selector_profile_with_credit(
            "selector_call_credit_overbought_only",
            Some(call_credit_overbought),
            None,
            None,
            None,
        ),
        selector_profile_with_credit(
            "selector_call_credit_weak_plus_crash_put_and_call_debits",
            Some(call_credit_weak),
            None,
            Some(put_crash),
            Some(call_trend),
        ),
        selector_profile_with_credit(
            "selector_economic_wheel_plus_call_credit_and_debits",
            Some(call_credit_weak),
            Some(wheel_economic),
            Some(put_crash),
            Some(call_trend),
        ),
        selector_profile(
            "selector_inventory_wheel_plus_crash_put_debit",
            Some(wheel_inventory),
            Some(put_crash),
            None,
        ),
        selector_profile(
            "selector_guarded_wheel_plus_crash_put_debit",
            Some(wheel_guarded),
            Some(put_crash),
            None,
        ),
        selector_profile(
            "selector_guarded_wheel_plus_crash_put_and_call_debits",
            Some(wheel_guarded),
            Some(put_crash),
            Some(call_trend),
        ),
        selector_profile(
            "selector_guarded_fast_exit_wheel_plus_crash_put_and_call_debits",
            Some(wheel_guarded_fast),
            Some(put_crash),
            Some(call_trend),
        ),
        selector_profile(
            "selector_guarded_wheel_plus_pullback_put_debit",
            Some(wheel_guarded),
            Some(put_pullback),
            None,
        ),
        selector_profile(
            "selector_guarded_fast_exit_wheel_plus_pullback_put_debit",
            Some(wheel_guarded_fast),
            Some(put_pullback),
            None,
        ),
        selector_profile(
            "selector_economic_wheel_plus_pullback_put_debit",
            Some(wheel_economic),
            Some(put_pullback),
            None,
        ),
        selector_profile(
            "selector_economic_wheel_plus_crash_put_debit",
            Some(wheel_economic),
            Some(put_crash),
            None,
        ),
        selector_profile(
            "selector_economic_wheel_plus_crash_put_and_call_debits",
            Some(wheel_economic),
            Some(put_crash),
            Some(call_trend),
        ),
        selector_profile_with_put_fallback(
            "selector_economic_wheel_plus_crash_and_costaware_puts_and_call_debits",
            Some(wheel_economic),
            Some(put_crash),
            Some(put_costaware),
            Some(call_trend),
        ),
        selector_profile_with_put_fallback(
            "selector_economic_wheel_plus_crash_and_pullback_puts_and_call_debits",
            Some(wheel_economic),
            Some(put_crash),
            Some(put_pullback),
            Some(call_trend),
        ),
        selector_profile(
            "selector_inventory_wheel_plus_put_and_call_debits",
            Some(wheel_inventory),
            Some(put_crash),
            Some(call_trend),
        ),
        selector_profile(
            "selector_crash_put_and_call_debits_only",
            None,
            Some(put_crash),
            Some(call_trend),
        ),
        selector_profile(
            "selector_crash_put_and_costaware_call_debits_only",
            None,
            Some(put_crash),
            Some(call_costaware),
        ),
        selector_profile(
            "selector_drawdown_put_and_call_debits_only",
            None,
            Some(put_drawdown),
            Some(call_trend),
        ),
        selector_profile(
            "selector_drawdown_put_and_costaware_call_debits_only",
            None,
            Some(put_drawdown),
            Some(call_costaware),
        ),
        selector_profile(
            "selector_disciplined_drawdown_put_and_costaware_call_debits_only",
            None,
            Some(put_disciplined_drawdown),
            Some(call_costaware),
        ),
        selector_profile(
            "selector_drawdown_put_and_balanced_call_debits_only",
            None,
            Some(put_drawdown),
            Some(call_balanced_costaware),
        ),
        selector_profile(
            "selector_disciplined_drawdown_put_and_balanced_call_debits_only",
            None,
            Some(put_disciplined_drawdown),
            Some(call_balanced_costaware),
        ),
        selector_profile(
            "selector_legguard15_debits_only",
            None,
            Some(put_legguard_15),
            Some(call_legguard_15),
        ),
        selector_profile(
            "selector_legguard18_debits_only",
            None,
            Some(put_legguard_18),
            Some(call_legguard_18),
        ),
        selector_profile(
            "selector_put_legguard15_and_balanced_call_debits_only",
            None,
            Some(put_legguard_15),
            Some(call_balanced_costaware),
        ),
        selector_profile(
            "selector_disciplined_put_and_call_legguard15_debits_only",
            None,
            Some(put_disciplined_drawdown),
            Some(call_legguard_15),
        ),
        selector_profile(
            "selector_no_tsla_put_disciplined_call_legguard15_debits_only",
            None,
            Some(put_disciplined_drawdown),
            Some(call_legguard_15),
        )
        .with_put_debit_symbols(&["IREN", "PLTR", "ORCL", "CRWV"]),
        selector_profile(
            "selector_disciplined_debits_only",
            None,
            Some(put_disciplined_drawdown),
            Some(call_disciplined_costaware),
        ),
        selector_profile(
            "selector_drawdown_put_debit_only",
            None,
            Some(put_drawdown),
            None,
        ),
        selector_profile(
            "selector_disciplined_drawdown_put_debit_only",
            None,
            Some(put_disciplined_drawdown),
            None,
        ),
        selector_profile(
            "selector_costaware_call_debit_only",
            None,
            None,
            Some(call_costaware),
        ),
        selector_profile(
            "selector_disciplined_costaware_call_debit_only",
            None,
            None,
            Some(call_disciplined_costaware),
        ),
        selector_profile(
            "selector_balanced_costaware_call_debit_only",
            None,
            None,
            Some(call_balanced_costaware),
        ),
        selector_profile(
            "selector_side_selective_pltr_orcl_wide_call_debit_only",
            None,
            None,
            Some(call_wide_dte3_10),
        )
        .with_call_debit_symbols(&["PLTR", "ORCL"]),
        selector_profile(
            "selector_side_selective_non_tsla_wide_call_debit_only",
            None,
            None,
            Some(call_wide_dte3_10),
        )
        .with_call_debit_symbols(&["IREN", "PLTR", "ORCL", "CRWV"]),
        selector_profile(
            "selector_side_selective_pltr_put_plus_pltr_orcl_call_debits_only",
            None,
            Some(put_pltr_wide),
            Some(call_wide_dte3_10),
        )
        .with_put_debit_symbols(&["PLTR"])
        .with_call_debit_symbols(&["PLTR", "ORCL"]),
        selector_profile(
            "selector_side_selective_pltr_put_plus_non_tsla_call_debits_only",
            None,
            Some(put_pltr_wide),
            Some(call_wide_dte3_10),
        )
        .with_put_debit_symbols(&["PLTR"])
        .with_call_debit_symbols(&["IREN", "PLTR", "ORCL", "CRWV"]),
        selector_profile(
            "selector_side_selective_pltr_put_plus_non_tsla_call_debits_take33_only",
            None,
            Some(put_pltr_wide),
            Some(call_wide_dte3_10_take33),
        )
        .with_put_debit_symbols(&["PLTR"])
        .with_call_debit_symbols(&["IREN", "PLTR", "ORCL", "CRWV"]),
        selector_profile(
            "selector_side_selective_pltr_put_plus_non_tsla_call_debits_take50_only",
            None,
            Some(put_pltr_wide),
            Some(call_wide_dte3_10_take50),
        )
        .with_put_debit_symbols(&["PLTR"])
        .with_call_debit_symbols(&["IREN", "PLTR", "ORCL", "CRWV"]),
        selector_profile_with_call_fallback(
            "selector_side_selective_pltr_put_plus_orcl_costaware_minw1_non_tsla_call_debits_only",
            None,
            Some(put_pltr_wide),
            Some(call_wide_dte3_10),
            Some(call_orcl_costaware_minw1),
        )
        .with_put_debit_symbols(&["PLTR"])
        .with_call_debit_symbols(&["IREN", "PLTR", "CRWV"])
        .with_call_debit_fallback_symbols(&["ORCL"]),
        selector_profile_with_call_fallback(
            "selector_side_selective_pltr_put_plus_orcl_costaware_minw3_non_tsla_call_debits_only",
            None,
            Some(put_pltr_wide),
            Some(call_wide_dte3_10),
            Some(call_orcl_costaware_minw3),
        )
        .with_put_debit_symbols(&["PLTR"])
        .with_call_debit_symbols(&["IREN", "PLTR", "CRWV"])
        .with_call_debit_fallback_symbols(&["ORCL"]),
        selector_profile_with_call_fallback(
            "selector_side_selective_pltr_put_plus_orcl_costaware_minw5_non_tsla_call_debits_only",
            None,
            Some(put_pltr_wide),
            Some(call_wide_dte3_10),
            Some(call_orcl_costaware_minw5),
        )
        .with_put_debit_symbols(&["PLTR"])
        .with_call_debit_symbols(&["IREN", "PLTR", "CRWV"])
        .with_call_debit_fallback_symbols(&["ORCL"]),
        selector_profile_with_call_fallback(
            "selector_side_selective_pltr_put_plus_orcl_costaware_mindebit40_minw1_non_tsla_call_debits_only",
            None,
            Some(put_pltr_wide),
            Some(call_wide_dte3_10),
            Some(call_orcl_costaware_mindebit40_minw1),
        )
        .with_put_debit_symbols(&["PLTR"])
        .with_call_debit_symbols(&["IREN", "PLTR", "CRWV"])
        .with_call_debit_fallback_symbols(&["ORCL"]),
        selector_profile_with_call_fallback(
            "selector_side_selective_pltr_put_plus_orcl_costaware_mindebit35_minw1_non_tsla_call_debits_only",
            None,
            Some(put_pltr_wide),
            Some(call_wide_dte3_10),
            Some(call_orcl_costaware_mindebit35_minw1),
        )
        .with_put_debit_symbols(&["PLTR"])
        .with_call_debit_symbols(&["IREN", "PLTR", "CRWV"])
        .with_call_debit_fallback_symbols(&["ORCL"]),
        selector_profile_with_call_fallback(
            "selector_side_selective_pltr_sofi_put_plus_orcl_costaware_mindebit35_non_tsla_call_debits_only",
            None,
            Some(put_pltr_wide),
            Some(call_wide_dte3_10),
            Some(call_orcl_costaware_mindebit35_minw1),
        )
        .with_put_debit_symbols(&["PLTR", "SOFI"])
        .with_call_debit_symbols(&["IREN", "PLTR", "CRWV"])
        .with_call_debit_fallback_symbols(&["ORCL"]),
        selector_profile_with_call_fallback(
            "selector_side_selective_pltr_sofi_put_plus_orcl_costaware_mindebit35_non_tsla_sofi_call_debits_only",
            None,
            Some(put_pltr_wide),
            Some(call_wide_dte3_10),
            Some(call_orcl_costaware_mindebit35_minw1),
        )
        .with_put_debit_symbols(&["PLTR", "SOFI"])
        .with_call_debit_symbols(&["IREN", "PLTR", "CRWV", "SOFI"])
        .with_call_debit_fallback_symbols(&["ORCL"]),
        selector_profile_with_call_fallback(
            "selector_side_selective_pltr_put_plus_orcl_costaware_mindebit35_legguard_non_tsla_call_debits_only",
            None,
            Some(put_pltr_wide),
            Some(call_legguard_15),
            Some(call_orcl_costaware_mindebit35_minw1),
        )
        .with_put_debit_symbols(&["PLTR"])
        .with_call_debit_symbols(&["IREN", "PLTR", "CRWV"])
        .with_call_debit_fallback_symbols(&["ORCL"]),
        selector_profile_with_call_fallback(
            "selector_side_selective_pltr_put_plus_orcl_costaware_mindebit35_legguard_plus_tsla_call_debits_only",
            None,
            Some(put_pltr_wide),
            Some(call_legguard_15),
            Some(call_orcl_costaware_mindebit35_minw1),
        )
        .with_put_debit_symbols(&["PLTR"])
        .with_call_debit_symbols(&["IREN", "PLTR", "TSLA", "CRWV"])
        .with_call_debit_fallback_symbols(&["ORCL"]),
        selector_profile_with_call_fallback(
            "selector_side_selective_pltr_put_plus_orcl_costaware_mindebit30_minw1_non_tsla_call_debits_only",
            None,
            Some(put_pltr_wide),
            Some(call_wide_dte3_10),
            Some(call_orcl_costaware_mindebit30_minw1),
        )
        .with_put_debit_symbols(&["PLTR"])
        .with_call_debit_symbols(&["IREN", "PLTR", "CRWV"])
        .with_call_debit_fallback_symbols(&["ORCL"]),
        selector_profile_with_call_fallback(
            "selector_side_selective_pltr_put_plus_orcl_costaware_minw1_plus_tsla_call_debits_only",
            None,
            Some(put_pltr_wide),
            Some(call_wide_dte3_10),
            Some(call_orcl_costaware_minw1),
        )
        .with_put_debit_symbols(&["PLTR"])
        .with_call_debit_symbols(&["IREN", "PLTR", "TSLA", "CRWV"])
        .with_call_debit_fallback_symbols(&["ORCL"]),
        selector_profile_with_call_fallback(
            "selector_side_selective_pltr_put_plus_orcl_costaware_minw3_plus_tsla_call_debits_only",
            None,
            Some(put_pltr_wide),
            Some(call_wide_dte3_10),
            Some(call_orcl_costaware_minw3),
        )
        .with_put_debit_symbols(&["PLTR"])
        .with_call_debit_symbols(&["IREN", "PLTR", "TSLA", "CRWV"])
        .with_call_debit_fallback_symbols(&["ORCL"]),
        selector_profile(
            "selector_legguard15_call_debit_only",
            None,
            None,
            Some(call_legguard_15),
        ),
        selector_profile(
            "selector_legguard18_call_debit_only",
            None,
            None,
            Some(call_legguard_18),
        ),
        selector_profile(
            "selector_call_debit_trend_only",
            None,
            None,
            Some(call_trend),
        ),
        selector_profile(
            "selector_put_debit_regime_pair_only",
            None,
            Some(put_crash),
            None,
        ),
        selector_profile(
            "selector_costaware_put_debit_only",
            None,
            Some(put_costaware),
            None,
        ),
    ]
}

fn selector_profile(
    name: &str,
    wheel_profile: Option<&ResearchProfile>,
    put_debit_profile: Option<&ResearchProfile>,
    call_debit_profile: Option<&ResearchProfile>,
) -> PortfolioSelectorProfile {
    let mut summary_profile = wheel_profile
        .or(put_debit_profile)
        .or(call_debit_profile)
        .expect("selector profile requires at least one component")
        .clone();
    summary_profile.name = name.to_owned();
    summary_profile.min_trades_per_year = MIN_WEEKLY_RANKING_TRADES_PER_YEAR;
    PortfolioSelectorProfile {
        summary_profile,
        put_credit_profile: None,
        wheel_profile: wheel_profile.cloned(),
        put_debit_profile: put_debit_profile.cloned(),
        put_debit_fallback_profile: None,
        call_debit_profile: call_debit_profile.cloned(),
        call_debit_fallback_profile: None,
        put_credit_symbols: None,
        wheel_symbols: None,
        put_debit_symbols: None,
        put_debit_fallback_symbols: None,
        call_debit_symbols: None,
        call_debit_fallback_symbols: None,
    }
}

fn selector_profile_with_credit(
    name: &str,
    put_credit_profile: Option<&ResearchProfile>,
    wheel_profile: Option<&ResearchProfile>,
    put_debit_profile: Option<&ResearchProfile>,
    call_debit_profile: Option<&ResearchProfile>,
) -> PortfolioSelectorProfile {
    if wheel_profile.is_none() && put_debit_profile.is_none() && call_debit_profile.is_none() {
        let put_credit_profile =
            put_credit_profile.expect("credit-only selector requires a credit profile");
        let mut summary_profile = put_credit_profile.clone();
        summary_profile.name = name.to_owned();
        summary_profile.min_trades_per_year = MIN_WEEKLY_RANKING_TRADES_PER_YEAR;
        return PortfolioSelectorProfile {
            summary_profile,
            put_credit_profile: Some(put_credit_profile.clone()),
            wheel_profile: None,
            put_debit_profile: None,
            put_debit_fallback_profile: None,
            call_debit_profile: None,
            call_debit_fallback_profile: None,
            put_credit_symbols: None,
            wheel_symbols: None,
            put_debit_symbols: None,
            put_debit_fallback_symbols: None,
            call_debit_symbols: None,
            call_debit_fallback_symbols: None,
        };
    }

    let mut profile = selector_profile(name, wheel_profile, put_debit_profile, call_debit_profile);
    if let Some(put_credit_profile) = put_credit_profile {
        profile.put_credit_profile = Some(put_credit_profile.clone());
    }
    profile
}

fn selector_profile_with_put_fallback(
    name: &str,
    wheel_profile: Option<&ResearchProfile>,
    put_debit_profile: Option<&ResearchProfile>,
    put_debit_fallback_profile: Option<&ResearchProfile>,
    call_debit_profile: Option<&ResearchProfile>,
) -> PortfolioSelectorProfile {
    let mut profile = selector_profile(name, wheel_profile, put_debit_profile, call_debit_profile);
    profile.put_debit_fallback_profile = put_debit_fallback_profile.cloned();
    profile
}

fn selector_profile_with_call_fallback(
    name: &str,
    wheel_profile: Option<&ResearchProfile>,
    put_debit_profile: Option<&ResearchProfile>,
    call_debit_profile: Option<&ResearchProfile>,
    call_debit_fallback_profile: Option<&ResearchProfile>,
) -> PortfolioSelectorProfile {
    let mut profile = selector_profile(name, wheel_profile, put_debit_profile, call_debit_profile);
    profile.call_debit_fallback_profile = call_debit_fallback_profile.cloned();
    profile
}

fn profile_named<'a>(profiles: &'a [ResearchProfile], name: &str) -> &'a ResearchProfile {
    profiles
        .iter()
        .find(|profile| profile.name == name)
        .unwrap_or_else(|| panic!("missing research profile {name}"))
}

fn research_profiles() -> Vec<ResearchProfile> {
    let baseline = ResearchProfile::legacy_baseline();
    let mut profiles = vec![ResearchProfile::frozen_baseline()];
    for (name, delta_min, delta_max, credit, width, stop) in [
        (
            "higher_credit_delta20_30_credit25",
            0.20,
            0.30,
            0.25,
            20.0,
            2.0,
        ),
        (
            "lower_delta_delta15_25_credit20",
            0.15,
            0.25,
            0.20,
            20.0,
            2.0,
        ),
        (
            "tight_width_delta20_30_width10",
            0.20,
            0.30,
            0.20,
            10.0,
            2.0,
        ),
        (
            "resilient_delta15_25_credit25_stop2x",
            0.15,
            0.25,
            0.25,
            20.0,
            2.0,
        ),
        (
            "aggressive_delta25_35_credit20",
            0.25,
            0.35,
            0.20,
            20.0,
            2.0,
        ),
        ("defensive_stop150_delta20_30", 0.20, 0.30, 0.20, 20.0, 1.5),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.min_short_delta_abs = delta_min;
        profile.max_short_delta_abs = delta_max;
        profile.min_credit_width = credit;
        profile.max_width = width;
        profile.stop_loss_multiple = stop;
        profiles.push(profile);
    }
    let mut trend_10d = baseline.clone();
    trend_10d.name = "trend10d_delta20_30_credit25".to_owned();
    trend_10d.min_credit_width = 0.25;
    trend_10d.trend_lookback_days = Some(10);
    trend_10d.min_underlying_return = Some(0.0);
    profiles.push(trend_10d);

    let mut trend_20d = baseline.clone();
    trend_20d.name = "trend20d_delta20_30_credit25".to_owned();
    trend_20d.min_credit_width = 0.25;
    trend_20d.trend_lookback_days = Some(20);
    trend_20d.min_underlying_return = Some(0.0);
    profiles.push(trend_20d);

    let mut otm_8pct = baseline.clone();
    otm_8pct.name = "otm8pct_delta20_30_credit25".to_owned();
    otm_8pct.min_credit_width = 0.25;
    otm_8pct.min_short_otm_pct = Some(0.08);
    profiles.push(otm_8pct);

    let mut trend_otm = baseline.clone();
    trend_otm.name = "trend10d_otm8pct_delta20_30_credit25".to_owned();
    trend_otm.min_credit_width = 0.25;
    trend_otm.trend_lookback_days = Some(10);
    trend_otm.min_underlying_return = Some(0.0);
    trend_otm.min_short_otm_pct = Some(0.08);
    profiles.push(trend_otm);

    let mut lower_delta_trend = baseline.clone();
    lower_delta_trend.name = "trend10d_delta15_25_credit20".to_owned();
    lower_delta_trend.min_short_delta_abs = 0.15;
    lower_delta_trend.max_short_delta_abs = 0.25;
    lower_delta_trend.trend_lookback_days = Some(10);
    lower_delta_trend.min_underlying_return = Some(0.0);
    profiles.push(lower_delta_trend);

    let mut higher_credit_stop150 = baseline.clone();
    higher_credit_stop150.name = "higher_credit_stop150_delta20_30_credit25".to_owned();
    higher_credit_stop150.min_credit_width = 0.25;
    higher_credit_stop150.stop_loss_multiple = 1.5;
    profiles.push(higher_credit_stop150);

    let mut higher_credit_stop125 = baseline.clone();
    higher_credit_stop125.name = "higher_credit_stop125_delta20_30_credit25".to_owned();
    higher_credit_stop125.min_credit_width = 0.25;
    higher_credit_stop125.stop_loss_multiple = 1.25;
    profiles.push(higher_credit_stop125);

    let mut higher_credit_take35 = baseline.clone();
    higher_credit_take35.name = "higher_credit_take35_delta20_30_credit25".to_owned();
    higher_credit_take35.min_credit_width = 0.25;
    higher_credit_take35.take_profit_pct = 0.35;
    profiles.push(higher_credit_take35);

    let mut higher_credit_exit28 = baseline.clone();
    higher_credit_exit28.name = "higher_credit_exit28dte_delta20_30_credit25".to_owned();
    higher_credit_exit28.min_credit_width = 0.25;
    higher_credit_exit28.force_close_dte = 28;
    profiles.push(higher_credit_exit28);

    let mut higher_credit_delta20_25 = baseline.clone();
    higher_credit_delta20_25.name = "higher_credit_delta20_25_credit25".to_owned();
    higher_credit_delta20_25.max_short_delta_abs = 0.25;
    higher_credit_delta20_25.min_credit_width = 0.25;
    profiles.push(higher_credit_delta20_25);

    let mut higher_credit_delta25_30 = baseline.clone();
    higher_credit_delta25_30.name = "higher_credit_delta25_30_credit25".to_owned();
    higher_credit_delta25_30.min_short_delta_abs = 0.25;
    higher_credit_delta25_30.max_short_delta_abs = 0.30;
    higher_credit_delta25_30.min_credit_width = 0.25;
    profiles.push(higher_credit_delta25_30);

    let mut ivcap100 = baseline.clone();
    ivcap100.name = "ivcap100_delta20_30_credit20".to_owned();
    ivcap100.max_short_iv = Some(1.00);
    profiles.push(ivcap100);

    let mut ivcap80 = baseline.clone();
    ivcap80.name = "ivcap80_delta20_30_credit20".to_owned();
    ivcap80.max_short_iv = Some(0.80);
    profiles.push(ivcap80);

    let mut higher_credit_ivcap100 = baseline.clone();
    higher_credit_ivcap100.name = "ivcap100_delta20_30_credit25".to_owned();
    higher_credit_ivcap100.min_credit_width = 0.25;
    higher_credit_ivcap100.max_short_iv = Some(1.00);
    profiles.push(higher_credit_ivcap100);

    let mut higher_credit_ivcap80 = baseline.clone();
    higher_credit_ivcap80.name = "ivcap80_delta20_30_credit25".to_owned();
    higher_credit_ivcap80.min_credit_width = 0.25;
    higher_credit_ivcap80.max_short_iv = Some(0.80);
    profiles.push(higher_credit_ivcap80);

    let mut lower_delta_ivcap80 = baseline.clone();
    lower_delta_ivcap80.name = "ivcap80_delta15_25_credit20".to_owned();
    lower_delta_ivcap80.min_short_delta_abs = 0.15;
    lower_delta_ivcap80.max_short_delta_abs = 0.25;
    lower_delta_ivcap80.max_short_iv = Some(0.80);
    profiles.push(lower_delta_ivcap80);

    let mut otm_selector = baseline.clone();
    otm_selector.name = "select_farther_otm_delta20_30_credit20".to_owned();
    otm_selector.prefer_farther_otm = true;
    profiles.push(otm_selector);

    let mut otm_selector_ivcap80 = baseline.clone();
    otm_selector_ivcap80.name = "select_farther_otm_ivcap80_delta20_30_credit20".to_owned();
    otm_selector_ivcap80.max_short_iv = Some(0.80);
    otm_selector_ivcap80.prefer_farther_otm = true;
    profiles.push(otm_selector_ivcap80);

    let mut lower_delta_otm_selector = baseline.clone();
    lower_delta_otm_selector.name = "select_farther_otm_delta15_25_credit20".to_owned();
    lower_delta_otm_selector.min_short_delta_abs = 0.15;
    lower_delta_otm_selector.max_short_delta_abs = 0.25;
    lower_delta_otm_selector.prefer_farther_otm = true;
    profiles.push(lower_delta_otm_selector);

    let mut cooldown_10 = baseline.clone();
    cooldown_10.name = "cooldown10_delta20_30_credit20".to_owned();
    cooldown_10.stop_loss_cooldown_days = 10;
    profiles.push(cooldown_10);

    let mut otm_cooldown_10 = baseline.clone();
    otm_cooldown_10.name = "select_farther_otm_cooldown10_delta20_30_credit20".to_owned();
    otm_cooldown_10.prefer_farther_otm = true;
    otm_cooldown_10.stop_loss_cooldown_days = 10;
    profiles.push(otm_cooldown_10);

    let mut otm_cooldown_20 = baseline.clone();
    otm_cooldown_20.name = "select_farther_otm_cooldown20_delta20_30_credit20".to_owned();
    otm_cooldown_20.prefer_farther_otm = true;
    otm_cooldown_20.stop_loss_cooldown_days = 20;
    profiles.push(otm_cooldown_20);

    let mut otm_cooldown_trend20 = baseline.clone();
    otm_cooldown_trend20.name =
        "select_farther_otm_cooldown10_trend20d_delta20_30_credit20".to_owned();
    otm_cooldown_trend20.prefer_farther_otm = true;
    otm_cooldown_trend20.stop_loss_cooldown_days = 10;
    otm_cooldown_trend20.trend_lookback_days = Some(20);
    otm_cooldown_trend20.min_underlying_return = Some(0.0);
    profiles.push(otm_cooldown_trend20);

    let mut otm_cooldown_trend60 = baseline.clone();
    otm_cooldown_trend60.name =
        "select_farther_otm_cooldown10_trend60d_delta20_30_credit20".to_owned();
    otm_cooldown_trend60.prefer_farther_otm = true;
    otm_cooldown_trend60.stop_loss_cooldown_days = 10;
    otm_cooldown_trend60.trend_lookback_days = Some(60);
    otm_cooldown_trend60.min_underlying_return = Some(0.0);
    profiles.push(otm_cooldown_trend60);

    let mut otm_cooldown_trend60_ivcap55 = baseline.clone();
    otm_cooldown_trend60_ivcap55.name =
        "select_farther_otm_cooldown10_trend60d_ivcap55_delta20_30_credit20".to_owned();
    otm_cooldown_trend60_ivcap55.prefer_farther_otm = true;
    otm_cooldown_trend60_ivcap55.stop_loss_cooldown_days = 10;
    otm_cooldown_trend60_ivcap55.trend_lookback_days = Some(60);
    otm_cooldown_trend60_ivcap55.min_underlying_return = Some(0.0);
    otm_cooldown_trend60_ivcap55.max_short_iv = Some(0.55);
    profiles.push(otm_cooldown_trend60_ivcap55);

    let mut otm_cooldown_trend60_ivcap50 = baseline.clone();
    otm_cooldown_trend60_ivcap50.name =
        "select_farther_otm_cooldown10_trend60d_ivcap50_delta20_30_credit20".to_owned();
    otm_cooldown_trend60_ivcap50.prefer_farther_otm = true;
    otm_cooldown_trend60_ivcap50.stop_loss_cooldown_days = 10;
    otm_cooldown_trend60_ivcap50.trend_lookback_days = Some(60);
    otm_cooldown_trend60_ivcap50.min_underlying_return = Some(0.0);
    otm_cooldown_trend60_ivcap50.max_short_iv = Some(0.50);
    profiles.push(otm_cooldown_trend60_ivcap50);

    let mut otm_cooldown_trend60_ivcap45 = baseline.clone();
    otm_cooldown_trend60_ivcap45.name =
        "select_farther_otm_cooldown10_trend60d_ivcap45_delta20_30_credit20".to_owned();
    otm_cooldown_trend60_ivcap45.prefer_farther_otm = true;
    otm_cooldown_trend60_ivcap45.stop_loss_cooldown_days = 10;
    otm_cooldown_trend60_ivcap45.trend_lookback_days = Some(60);
    otm_cooldown_trend60_ivcap45.min_underlying_return = Some(0.0);
    otm_cooldown_trend60_ivcap45.max_short_iv = Some(0.45);
    profiles.push(otm_cooldown_trend60_ivcap45);

    let mut otm_cooldown_trend60_min5_ivcap45 = baseline.clone();
    otm_cooldown_trend60_min5_ivcap45.name =
        "select_farther_otm_cooldown10_trend60d_min5_ivcap45_delta20_30_credit20".to_owned();
    otm_cooldown_trend60_min5_ivcap45.prefer_farther_otm = true;
    otm_cooldown_trend60_min5_ivcap45.stop_loss_cooldown_days = 10;
    otm_cooldown_trend60_min5_ivcap45.trend_lookback_days = Some(60);
    otm_cooldown_trend60_min5_ivcap45.min_underlying_return = Some(0.05);
    otm_cooldown_trend60_min5_ivcap45.max_short_iv = Some(0.45);
    profiles.push(otm_cooldown_trend60_min5_ivcap45);

    let mut otm_cooldown_trend60_min5_ivcap45_width15 = baseline.clone();
    otm_cooldown_trend60_min5_ivcap45_width15.name =
        "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_delta20_30_credit20"
            .to_owned();
    otm_cooldown_trend60_min5_ivcap45_width15.prefer_farther_otm = true;
    otm_cooldown_trend60_min5_ivcap45_width15.stop_loss_cooldown_days = 10;
    otm_cooldown_trend60_min5_ivcap45_width15.trend_lookback_days = Some(60);
    otm_cooldown_trend60_min5_ivcap45_width15.min_underlying_return = Some(0.05);
    otm_cooldown_trend60_min5_ivcap45_width15.max_short_iv = Some(0.45);
    otm_cooldown_trend60_min5_ivcap45_width15.max_width = 15.0;
    profiles.push(otm_cooldown_trend60_min5_ivcap45_width15);

    for (name, min_short_delta_abs) in [
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_delta23_30_credit20",
            0.23,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_delta24_30_credit20",
            0.24,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_delta26_30_credit20",
            0.26,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.stop_loss_cooldown_days = 10;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(0.05);
        profile.max_short_iv = Some(0.45);
        profile.max_width = 15.0;
        profile.min_short_delta_abs = min_short_delta_abs;
        profiles.push(profile);
    }

    for (name, delta_threshold) in [
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.23,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta24_width10_delta20_30_credit20",
            0.24,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.stop_loss_cooldown_days = 10;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(0.05);
        profile.max_short_iv = Some(0.45);
        profile.max_width = 15.0;
        profile.low_delta_width_cap_delta_abs = Some(delta_threshold);
        profile.low_delta_width_cap = Some(10.0);
        profiles.push(profile);
    }

    for (name, min_underlying_return) in [
        (
            "select_farther_otm_cooldown10_trend60d_min8_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.08,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min10_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.10,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min15_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.15,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.stop_loss_cooldown_days = 10;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(min_underlying_return);
        profile.max_short_iv = Some(0.45);
        profile.max_width = 15.0;
        profile.low_delta_width_cap_delta_abs = Some(0.23);
        profile.low_delta_width_cap = Some(10.0);
        profiles.push(profile);
    }

    for (name, max_hold_days) in [
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_hold7_delta20_30_credit20",
            7,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_hold10_delta20_30_credit20",
            10,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_hold14_delta20_30_credit20",
            14,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.stop_loss_cooldown_days = 10;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(0.05);
        profile.max_short_iv = Some(0.45);
        profile.max_width = 15.0;
        profile.low_delta_width_cap_delta_abs = Some(0.23);
        profile.low_delta_width_cap = Some(10.0);
        profile.max_hold_days = Some(max_hold_days);
        profiles.push(profile);
    }

    for (name, min_short_otm_pct) in [
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_otm6_delta20_30_credit20",
            0.06,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_otm7_delta20_30_credit20",
            0.07,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_otm8_delta20_30_credit20",
            0.08,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_otm9_delta20_30_credit20",
            0.09,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.stop_loss_cooldown_days = 10;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(0.05);
        profile.max_short_iv = Some(0.45);
        profile.max_width = 15.0;
        profile.low_delta_width_cap_delta_abs = Some(0.23);
        profile.low_delta_width_cap = Some(10.0);
        profile.min_short_otm_pct = Some(min_short_otm_pct);
        profiles.push(profile);
    }

    for (name, max_short_iv) in [
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap42_width15_lowdelta23_width10_delta20_30_credit20",
            0.42,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap40_width15_lowdelta23_width10_delta20_30_credit20",
            0.40,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.stop_loss_cooldown_days = 10;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(0.05);
        profile.max_short_iv = Some(max_short_iv);
        profile.max_width = 15.0;
        profile.low_delta_width_cap_delta_abs = Some(0.23);
        profile.low_delta_width_cap = Some(10.0);
        profiles.push(profile);
    }

    for (name, max_return) in [
        (
            "select_farther_otm_cooldown10_trend60d_min5_max25_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.25,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min5_max30_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.30,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min5_max40_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.40,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.stop_loss_cooldown_days = 10;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(0.05);
        profile.max_underlying_return = Some(max_return);
        profile.max_short_iv = Some(0.45);
        profile.max_width = 15.0;
        profile.low_delta_width_cap_delta_abs = Some(0.23);
        profile.low_delta_width_cap = Some(10.0);
        profiles.push(profile);
    }

    for (name, lookback_days, min_return) in [
        (
            "select_farther_otm_cooldown10_trend20d_min0_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            20,
            0.0,
        ),
        (
            "select_farther_otm_cooldown10_trend20d_min5_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            20,
            0.05,
        ),
        (
            "select_farther_otm_cooldown10_trend10d_min0_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            10,
            0.0,
        ),
        (
            "select_farther_otm_cooldown10_trend10d_min5_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            10,
            0.05,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.stop_loss_cooldown_days = 10;
        profile.trend_lookback_days = Some(lookback_days);
        profile.min_underlying_return = Some(min_return);
        profile.max_short_iv = Some(0.45);
        profile.max_width = 15.0;
        profile.low_delta_width_cap_delta_abs = Some(0.23);
        profile.low_delta_width_cap = Some(10.0);
        profiles.push(profile);
    }

    for (name, max_drawdown) in [
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_dd20d_max8_delta20_30_credit20",
            0.08,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_dd20d_max12_delta20_30_credit20",
            0.12,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_dd20d_max16_delta20_30_credit20",
            0.16,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.stop_loss_cooldown_days = 10;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(0.05);
        profile.max_short_iv = Some(0.45);
        profile.max_width = 15.0;
        profile.low_delta_width_cap_delta_abs = Some(0.23);
        profile.low_delta_width_cap = Some(10.0);
        profile.drawdown_lookback_days = Some(20);
        profile.max_underlying_drawdown = Some(max_drawdown);
        profiles.push(profile);
    }

    for (name, min_drawdown) in [
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_dd20d_min1_delta20_30_credit20",
            0.01,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_dd20d_min2_delta20_30_credit20",
            0.02,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_dd20d_min3_delta20_30_credit20",
            0.03,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.stop_loss_cooldown_days = 10;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(0.05);
        profile.max_short_iv = Some(0.45);
        profile.max_width = 15.0;
        profile.low_delta_width_cap_delta_abs = Some(0.23);
        profile.low_delta_width_cap = Some(10.0);
        profile.drawdown_lookback_days = Some(20);
        profile.min_underlying_drawdown = Some(min_drawdown);
        profiles.push(profile);
    }

    for (name, min_return) in [
        (
            "select_farther_otm_cooldown10_trend60d_min5_trend15_or_dd20d_min2_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.15,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min5_trend20_or_dd20d_min2_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.20,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min5_trend25_or_dd20d_min2_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.25,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.stop_loss_cooldown_days = 10;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(0.05);
        profile.max_short_iv = Some(0.45);
        profile.max_width = 15.0;
        profile.low_delta_width_cap_delta_abs = Some(0.23);
        profile.low_delta_width_cap = Some(10.0);
        profile.drawdown_lookback_days = Some(20);
        profile.return_or_drawdown_gate = Some(ReturnOrDrawdownGate {
            min_underlying_return: Some(min_return),
            min_underlying_drawdown: Some(0.02),
        });
        profiles.push(profile);
    }

    for (name, base_min_return) in [
        (
            "select_farther_otm_cooldown10_trend60d_min10_trend25_or_dd20d_min2_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.10,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.12,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min15_trend25_or_dd20d_min2_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.15,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.stop_loss_cooldown_days = 10;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(base_min_return);
        profile.max_short_iv = Some(0.45);
        profile.max_width = 15.0;
        profile.low_delta_width_cap_delta_abs = Some(0.23);
        profile.low_delta_width_cap = Some(10.0);
        profile.drawdown_lookback_days = Some(20);
        profile.return_or_drawdown_gate = Some(ReturnOrDrawdownGate {
            min_underlying_return: Some(0.25),
            min_underlying_drawdown: Some(0.02),
        });
        profiles.push(profile);
    }

    for (name, max_guard_return, min_guard_drawdown, max_guard_drawdown) in [
        (
            "select_farther_otm_cooldown10_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.13,
            0.03,
            0.06,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min10_trend25_or_dd20d_min2_weak12dd3to6_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.12,
            0.03,
            0.06,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.stop_loss_cooldown_days = 10;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(0.10);
        profile.max_short_iv = Some(0.45);
        profile.max_width = 15.0;
        profile.low_delta_width_cap_delta_abs = Some(0.23);
        profile.low_delta_width_cap = Some(10.0);
        profile.drawdown_lookback_days = Some(20);
        profile.return_or_drawdown_gate = Some(ReturnOrDrawdownGate {
            min_underlying_return: Some(0.25),
            min_underlying_drawdown: Some(0.02),
        });
        profile.weak_trend_pullback_guard = Some(WeakTrendPullbackGuard {
            max_underlying_return: max_guard_return,
            min_underlying_drawdown: min_guard_drawdown,
            max_underlying_drawdown: max_guard_drawdown,
        });
        profiles.push(profile);
    }

    for (name, min_return, max_drawdown, cooldown_days) in [
        (
            "select_farther_otm_cooldown10_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_riskcool30dd5_10d_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.30,
            0.05,
            10,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_riskcool30dd5_20d_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.30,
            0.05,
            20,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_riskcool28dd5_10d_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.28,
            0.05,
            10,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.stop_loss_cooldown_days = 10;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(0.10);
        profile.max_short_iv = Some(0.45);
        profile.max_width = 15.0;
        profile.low_delta_width_cap_delta_abs = Some(0.23);
        profile.low_delta_width_cap = Some(10.0);
        profile.drawdown_lookback_days = Some(20);
        profile.return_or_drawdown_gate = Some(ReturnOrDrawdownGate {
            min_underlying_return: Some(0.25),
            min_underlying_drawdown: Some(0.02),
        });
        profile.weak_trend_pullback_guard = Some(WeakTrendPullbackGuard {
            max_underlying_return: 0.13,
            min_underlying_drawdown: 0.03,
            max_underlying_drawdown: 0.06,
        });
        profile.risk_regime_cooldown_guard = Some(TrendDrawdownGuard {
            min_underlying_return: min_return,
            max_underlying_drawdown: max_drawdown,
        });
        profile.risk_regime_cooldown_days = cooldown_days;
        profiles.push(profile);
    }

    for (
        name,
        stop_loss_cooldown_days,
        risk_cooldown_days,
        min_dte,
        max_dte,
        min_delta,
        max_delta,
        min_credit_width,
    ) in [
        (
            "aggr_stopcool1_noriskcool_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            1,
            0,
            30,
            45,
            0.20,
            0.30,
            0.20,
        ),
        (
            "aggr_stopcool3_noriskcool_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            3,
            0,
            30,
            45,
            0.20,
            0.30,
            0.20,
        ),
        (
            "aggr_stopcool5_noriskcool_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            5,
            0,
            30,
            45,
            0.20,
            0.30,
            0.20,
        ),
        (
            "aggr_stopcool10_riskcool30dd5_5d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            10,
            5,
            30,
            45,
            0.20,
            0.30,
            0.20,
        ),
        (
            "aggr_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_ivcap45_width15_lowdelta23_width10_delta25_35_credit20",
            10,
            20,
            30,
            45,
            0.25,
            0.35,
            0.20,
        ),
        (
            "aggr_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_ivcap45_width15_lowdelta23_width10_delta20_35_credit20",
            10,
            20,
            30,
            45,
            0.20,
            0.35,
            0.20,
        ),
        (
            "aggr_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_ivcap45_width15_lowdelta23_width10_delta20_30_credit15",
            10,
            20,
            30,
            45,
            0.20,
            0.30,
            0.15,
        ),
        (
            "aggr_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_ivcap45_width15_lowdelta23_width10_dte21_35_delta20_30_credit20",
            10,
            20,
            21,
            35,
            0.20,
            0.30,
            0.20,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.stop_loss_cooldown_days = stop_loss_cooldown_days;
        profile.min_dte = min_dte;
        profile.max_dte = max_dte;
        profile.min_short_delta_abs = min_delta;
        profile.max_short_delta_abs = max_delta;
        profile.min_credit_width = min_credit_width;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(0.10);
        profile.max_short_iv = Some(0.45);
        profile.max_width = 15.0;
        profile.low_delta_width_cap_delta_abs = Some(0.23);
        profile.low_delta_width_cap = Some(10.0);
        profile.drawdown_lookback_days = Some(20);
        profile.return_or_drawdown_gate = Some(ReturnOrDrawdownGate {
            min_underlying_return: Some(0.25),
            min_underlying_drawdown: Some(0.02),
        });
        profile.weak_trend_pullback_guard = Some(WeakTrendPullbackGuard {
            max_underlying_return: 0.13,
            min_underlying_drawdown: 0.03,
            max_underlying_drawdown: 0.06,
        });
        if risk_cooldown_days > 0 {
            profile.risk_regime_cooldown_guard = Some(TrendDrawdownGuard {
                min_underlying_return: 0.30,
                max_underlying_drawdown: 0.05,
            });
            profile.risk_regime_cooldown_days = risk_cooldown_days;
        }
        profiles.push(profile);
    }

    for (name, max_concurrent_positions, min_entry_spacing_days) in [
        (
            "overlap2_gap5_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_ivcap45_width15_lowdelta23_width10_delta25_35_credit20",
            2,
            5,
        ),
        (
            "overlap2_gap10_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_ivcap45_width15_lowdelta23_width10_delta25_35_credit20",
            2,
            10,
        ),
        (
            "overlap3_gap7_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_ivcap45_width15_lowdelta23_width10_delta25_35_credit20",
            3,
            7,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.stop_loss_cooldown_days = 10;
        profile.max_concurrent_positions = max_concurrent_positions;
        profile.min_entry_spacing_days = min_entry_spacing_days;
        profile.min_short_delta_abs = 0.25;
        profile.max_short_delta_abs = 0.35;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(0.10);
        profile.max_short_iv = Some(0.45);
        profile.max_width = 15.0;
        profile.low_delta_width_cap_delta_abs = Some(0.23);
        profile.low_delta_width_cap = Some(10.0);
        profile.drawdown_lookback_days = Some(20);
        profile.return_or_drawdown_gate = Some(ReturnOrDrawdownGate {
            min_underlying_return: Some(0.25),
            min_underlying_drawdown: Some(0.02),
        });
        profile.weak_trend_pullback_guard = Some(WeakTrendPullbackGuard {
            max_underlying_return: 0.13,
            min_underlying_drawdown: 0.03,
            max_underlying_drawdown: 0.06,
        });
        profile.risk_regime_cooldown_guard = Some(TrendDrawdownGuard {
            min_underlying_return: 0.30,
            max_underlying_drawdown: 0.05,
        });
        profile.risk_regime_cooldown_days = 20;
        profiles.push(profile);
    }

    for (name, min_dte, max_delta, max_short_iv) in [
        (
            "edgeguard_dte35_delta25_35_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_width15_lowdelta23_width10_credit20",
            35,
            0.35,
            0.45,
        ),
        (
            "edgeguard_dte30_delta25_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_width15_lowdelta23_width10_credit20",
            30,
            0.34,
            0.45,
        ),
        (
            "edgeguard_dte30_delta25_35_ivcap44_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_width15_lowdelta23_width10_credit20",
            30,
            0.35,
            0.44,
        ),
        (
            "edgeguard_dte35_delta25_34_ivcap44_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_width15_lowdelta23_width10_credit20",
            35,
            0.34,
            0.44,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.stop_loss_cooldown_days = 10;
        profile.min_dte = min_dte;
        profile.min_short_delta_abs = 0.25;
        profile.max_short_delta_abs = max_delta;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(0.10);
        profile.max_short_iv = Some(max_short_iv);
        profile.max_width = 15.0;
        profile.low_delta_width_cap_delta_abs = Some(0.23);
        profile.low_delta_width_cap = Some(10.0);
        profile.drawdown_lookback_days = Some(20);
        profile.return_or_drawdown_gate = Some(ReturnOrDrawdownGate {
            min_underlying_return: Some(0.25),
            min_underlying_drawdown: Some(0.02),
        });
        profile.weak_trend_pullback_guard = Some(WeakTrendPullbackGuard {
            max_underlying_return: 0.13,
            min_underlying_drawdown: 0.03,
            max_underlying_drawdown: 0.06,
        });
        profile.risk_regime_cooldown_guard = Some(TrendDrawdownGuard {
            min_underlying_return: 0.30,
            max_underlying_drawdown: 0.05,
        });
        profile.risk_regime_cooldown_days = 20;
        profiles.push(profile);
    }

    for (name, take_profit_pct, stop_loss_multiple, force_close_dte, max_hold_days) in [
        (
            "edgeexit_take35_dte30_delta25_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_width15_lowdelta23_width10_credit20",
            0.35,
            2.00,
            21,
            None,
        ),
        (
            "edgeexit_take40_dte30_delta25_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_width15_lowdelta23_width10_credit20",
            0.40,
            2.00,
            21,
            None,
        ),
        (
            "edgeexit_take45_dte30_delta25_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_width15_lowdelta23_width10_credit20",
            0.45,
            2.00,
            21,
            None,
        ),
        (
            "edgeexit_take60_dte30_delta25_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_width15_lowdelta23_width10_credit20",
            0.60,
            2.00,
            21,
            None,
        ),
        (
            "edgeexit_stop175_dte30_delta25_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_width15_lowdelta23_width10_credit20",
            0.50,
            1.75,
            21,
            None,
        ),
        (
            "edgeexit_stop150_dte30_delta25_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_width15_lowdelta23_width10_credit20",
            0.50,
            1.50,
            21,
            None,
        ),
        (
            "edgeexit_force28_dte30_delta25_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_width15_lowdelta23_width10_credit20",
            0.50,
            2.00,
            28,
            None,
        ),
        (
            "edgeexit_force25_dte30_delta25_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_width15_lowdelta23_width10_credit20",
            0.50,
            2.00,
            25,
            None,
        ),
        (
            "edgeexit_hold14_dte30_delta25_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_width15_lowdelta23_width10_credit20",
            0.50,
            2.00,
            21,
            Some(14),
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.take_profit_pct = take_profit_pct;
        profile.stop_loss_multiple = stop_loss_multiple;
        profile.force_close_dte = force_close_dte;
        profile.max_hold_days = max_hold_days;
        profile.stop_loss_cooldown_days = 10;
        profile.min_dte = 30;
        profile.min_short_delta_abs = 0.25;
        profile.max_short_delta_abs = 0.34;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(0.10);
        profile.max_short_iv = Some(0.45);
        profile.max_width = 15.0;
        profile.low_delta_width_cap_delta_abs = Some(0.23);
        profile.low_delta_width_cap = Some(10.0);
        profile.drawdown_lookback_days = Some(20);
        profile.return_or_drawdown_gate = Some(ReturnOrDrawdownGate {
            min_underlying_return: Some(0.25),
            min_underlying_drawdown: Some(0.02),
        });
        profile.weak_trend_pullback_guard = Some(WeakTrendPullbackGuard {
            max_underlying_return: 0.13,
            min_underlying_drawdown: 0.03,
            max_underlying_drawdown: 0.06,
        });
        profile.risk_regime_cooldown_guard = Some(TrendDrawdownGuard {
            min_underlying_return: 0.30,
            max_underlying_drawdown: 0.05,
        });
        profile.risk_regime_cooldown_days = 20;
        profiles.push(profile);
    }

    for (name, max_underlying_return, min_underlying_drawdown, max_underlying_drawdown) in [
        (
            "edgerefine_weak13dd2to6_take45_dte30_delta25_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_width15_lowdelta23_width10_credit20",
            0.13,
            0.02,
            0.06,
        ),
        (
            "edgerefine_weak13dd2to5_take45_dte30_delta25_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_width15_lowdelta23_width10_credit20",
            0.13,
            0.02,
            0.05,
        ),
        (
            "edgerefine_weak14dd2to6_take45_dte30_delta25_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_width15_lowdelta23_width10_credit20",
            0.14,
            0.02,
            0.06,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.take_profit_pct = 0.45;
        profile.stop_loss_cooldown_days = 10;
        profile.min_dte = 30;
        profile.min_short_delta_abs = 0.25;
        profile.max_short_delta_abs = 0.34;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(0.10);
        profile.max_short_iv = Some(0.45);
        profile.max_width = 15.0;
        profile.low_delta_width_cap_delta_abs = Some(0.23);
        profile.low_delta_width_cap = Some(10.0);
        profile.drawdown_lookback_days = Some(20);
        profile.return_or_drawdown_gate = Some(ReturnOrDrawdownGate {
            min_underlying_return: Some(0.25),
            min_underlying_drawdown: Some(0.02),
        });
        profile.weak_trend_pullback_guard = Some(WeakTrendPullbackGuard {
            max_underlying_return,
            min_underlying_drawdown,
            max_underlying_drawdown,
        });
        profile.risk_regime_cooldown_guard = Some(TrendDrawdownGuard {
            min_underlying_return: 0.30,
            max_underlying_drawdown: 0.05,
        });
        profile.risk_regime_cooldown_days = 20;
        profiles.push(profile);
    }

    for (name, min_delta, max_delta) in [
        (
            "edgedelta_weak13dd2to5_take45_dte30_delta23_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_width15_lowdelta23_width10_credit20",
            0.23,
            0.34,
        ),
        (
            "edgedelta_weak13dd2to5_take45_dte30_delta24_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_width15_lowdelta23_width10_credit20",
            0.24,
            0.34,
        ),
        (
            "edgedelta_weak13dd2to5_take45_dte30_delta25_33_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_width15_lowdelta23_width10_credit20",
            0.25,
            0.33,
        ),
        (
            "edgedelta_weak13dd2to5_take45_dte30_delta26_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_width15_lowdelta23_width10_credit20",
            0.26,
            0.34,
        ),
        (
            "edgedelta_weak13dd2to5_take45_dte30_delta25_35_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_width15_lowdelta23_width10_credit20",
            0.25,
            0.35,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.take_profit_pct = 0.45;
        profile.stop_loss_cooldown_days = 10;
        profile.min_dte = 30;
        profile.min_short_delta_abs = min_delta;
        profile.max_short_delta_abs = max_delta;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(0.10);
        profile.max_short_iv = Some(0.45);
        profile.max_width = 15.0;
        profile.low_delta_width_cap_delta_abs = Some(0.23);
        profile.low_delta_width_cap = Some(10.0);
        profile.drawdown_lookback_days = Some(20);
        profile.return_or_drawdown_gate = Some(ReturnOrDrawdownGate {
            min_underlying_return: Some(0.25),
            min_underlying_drawdown: Some(0.02),
        });
        profile.weak_trend_pullback_guard = Some(WeakTrendPullbackGuard {
            max_underlying_return: 0.13,
            min_underlying_drawdown: 0.02,
            max_underlying_drawdown: 0.05,
        });
        profile.risk_regime_cooldown_guard = Some(TrendDrawdownGuard {
            min_underlying_return: 0.30,
            max_underlying_drawdown: 0.05,
        });
        profile.risk_regime_cooldown_days = 20;
        profiles.push(profile);
    }

    for (name, stop_loss_multiple, take_profit_pct) in [
        (
            "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2_stop175_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            1.75,
            0.50,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2_stop150_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            1.50,
            0.50,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2_take40_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            2.00,
            0.40,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2_take35_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            2.00,
            0.35,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.stop_loss_cooldown_days = 10;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(0.12);
        profile.max_short_iv = Some(0.45);
        profile.max_width = 15.0;
        profile.low_delta_width_cap_delta_abs = Some(0.23);
        profile.low_delta_width_cap = Some(10.0);
        profile.drawdown_lookback_days = Some(20);
        profile.return_or_drawdown_gate = Some(ReturnOrDrawdownGate {
            min_underlying_return: Some(0.25),
            min_underlying_drawdown: Some(0.02),
        });
        profile.stop_loss_multiple = stop_loss_multiple;
        profile.take_profit_pct = take_profit_pct;
        profiles.push(profile);
    }

    for (name, max_drawdown) in [
        (
            "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2max4p5_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.045,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2max5_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.05,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2max6_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.06,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.stop_loss_cooldown_days = 10;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(0.12);
        profile.max_short_iv = Some(0.45);
        profile.max_width = 15.0;
        profile.low_delta_width_cap_delta_abs = Some(0.23);
        profile.low_delta_width_cap = Some(10.0);
        profile.drawdown_lookback_days = Some(20);
        profile.max_underlying_drawdown = Some(max_drawdown);
        profile.return_or_drawdown_gate = Some(ReturnOrDrawdownGate {
            min_underlying_return: Some(0.25),
            min_underlying_drawdown: Some(0.02),
        });
        profiles.push(profile);
    }

    for (name, min_iv_diff) in [
        (
            "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2_skew30bps_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.003,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2_skew40bps_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.004,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2_skew45bps_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.0045,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2_skew50bps_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.005,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2_skew80bps_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.008,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.stop_loss_cooldown_days = 10;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(0.12);
        profile.max_short_iv = Some(0.45);
        profile.min_long_short_iv_diff = Some(min_iv_diff);
        profile.max_width = 15.0;
        profile.low_delta_width_cap_delta_abs = Some(0.23);
        profile.low_delta_width_cap = Some(10.0);
        profile.drawdown_lookback_days = Some(20);
        profile.return_or_drawdown_gate = Some(ReturnOrDrawdownGate {
            min_underlying_return: Some(0.25),
            min_underlying_drawdown: Some(0.02),
        });
        profiles.push(profile);
    }

    for (name, min_return, max_drawdown) in [
        (
            "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2_guard20dd3p5_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.20,
            0.035,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2_guard25dd4_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.25,
            0.04,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2_guard30dd4_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
            0.30,
            0.04,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.stop_loss_cooldown_days = 10;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(0.12);
        profile.max_short_iv = Some(0.45);
        profile.max_width = 15.0;
        profile.low_delta_width_cap_delta_abs = Some(0.23);
        profile.low_delta_width_cap = Some(10.0);
        profile.drawdown_lookback_days = Some(20);
        profile.return_or_drawdown_gate = Some(ReturnOrDrawdownGate {
            min_underlying_return: Some(0.25),
            min_underlying_drawdown: Some(0.02),
        });
        profile.trend_drawdown_guard = Some(TrendDrawdownGuard {
            min_underlying_return: min_return,
            max_underlying_drawdown: max_drawdown,
        });
        profiles.push(profile);
    }

    for (name, max_realized_vol) in [
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_rv20max45_delta20_30_credit20",
            0.45,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_rv20max55_delta20_30_credit20",
            0.55,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_rv20max65_delta20_30_credit20",
            0.65,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.stop_loss_cooldown_days = 10;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(0.05);
        profile.max_short_iv = Some(0.45);
        profile.max_width = 15.0;
        profile.low_delta_width_cap_delta_abs = Some(0.23);
        profile.low_delta_width_cap = Some(10.0);
        profile.realized_vol_lookback_days = Some(20);
        profile.max_realized_vol = Some(max_realized_vol);
        profiles.push(profile);
    }

    for (name, max_short_iv) in [
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap42_width15_delta20_30_credit20",
            0.42,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap40_width15_delta20_30_credit20",
            0.40,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.stop_loss_cooldown_days = 10;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(0.05);
        profile.max_short_iv = Some(max_short_iv);
        profile.max_width = 15.0;
        profiles.push(profile);
    }

    for (name, take_profit_pct) in [
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_take35_delta20_30_credit20",
            0.35,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_take40_delta20_30_credit20",
            0.40,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.stop_loss_cooldown_days = 10;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(0.05);
        profile.max_short_iv = Some(0.45);
        profile.max_width = 15.0;
        profile.take_profit_pct = take_profit_pct;
        profiles.push(profile);
    }

    for (name, max_return) in [
        (
            "select_farther_otm_cooldown10_trend60d_min5_max25_ivcap45_width15_delta20_30_credit20",
            0.25,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min5_max30_ivcap45_width15_delta20_30_credit20",
            0.30,
        ),
        (
            "select_farther_otm_cooldown10_trend60d_min5_max40_ivcap45_width15_delta20_30_credit20",
            0.40,
        ),
    ] {
        let mut profile = baseline.clone();
        profile.name = name.to_owned();
        profile.prefer_farther_otm = true;
        profile.stop_loss_cooldown_days = 10;
        profile.trend_lookback_days = Some(60);
        profile.min_underlying_return = Some(0.05);
        profile.max_underlying_return = Some(max_return);
        profile.max_short_iv = Some(0.45);
        profile.max_width = 15.0;
        profiles.push(profile);
    }

    let mut otm_cooldown_trend60_min5_ivcap45_width15_stop175 = baseline.clone();
    otm_cooldown_trend60_min5_ivcap45_width15_stop175.name =
        "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_stop175_delta20_30_credit20"
            .to_owned();
    otm_cooldown_trend60_min5_ivcap45_width15_stop175.prefer_farther_otm = true;
    otm_cooldown_trend60_min5_ivcap45_width15_stop175.stop_loss_cooldown_days = 10;
    otm_cooldown_trend60_min5_ivcap45_width15_stop175.trend_lookback_days = Some(60);
    otm_cooldown_trend60_min5_ivcap45_width15_stop175.min_underlying_return = Some(0.05);
    otm_cooldown_trend60_min5_ivcap45_width15_stop175.max_short_iv = Some(0.45);
    otm_cooldown_trend60_min5_ivcap45_width15_stop175.max_width = 15.0;
    otm_cooldown_trend60_min5_ivcap45_width15_stop175.stop_loss_multiple = 1.75;
    profiles.push(otm_cooldown_trend60_min5_ivcap45_width15_stop175);

    let mut otm_cooldown_trend60_min5_ivcap45_width15_stop150 = baseline.clone();
    otm_cooldown_trend60_min5_ivcap45_width15_stop150.name =
        "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_stop150_delta20_30_credit20"
            .to_owned();
    otm_cooldown_trend60_min5_ivcap45_width15_stop150.prefer_farther_otm = true;
    otm_cooldown_trend60_min5_ivcap45_width15_stop150.stop_loss_cooldown_days = 10;
    otm_cooldown_trend60_min5_ivcap45_width15_stop150.trend_lookback_days = Some(60);
    otm_cooldown_trend60_min5_ivcap45_width15_stop150.min_underlying_return = Some(0.05);
    otm_cooldown_trend60_min5_ivcap45_width15_stop150.max_short_iv = Some(0.45);
    otm_cooldown_trend60_min5_ivcap45_width15_stop150.max_width = 15.0;
    otm_cooldown_trend60_min5_ivcap45_width15_stop150.stop_loss_multiple = 1.50;
    profiles.push(otm_cooldown_trend60_min5_ivcap45_width15_stop150);

    let mut otm_cooldown_trend60_min5_ivcap45_width10 = baseline.clone();
    otm_cooldown_trend60_min5_ivcap45_width10.name =
        "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width10_delta20_30_credit20"
            .to_owned();
    otm_cooldown_trend60_min5_ivcap45_width10.prefer_farther_otm = true;
    otm_cooldown_trend60_min5_ivcap45_width10.stop_loss_cooldown_days = 10;
    otm_cooldown_trend60_min5_ivcap45_width10.trend_lookback_days = Some(60);
    otm_cooldown_trend60_min5_ivcap45_width10.min_underlying_return = Some(0.05);
    otm_cooldown_trend60_min5_ivcap45_width10.max_short_iv = Some(0.45);
    otm_cooldown_trend60_min5_ivcap45_width10.max_width = 10.0;
    profiles.push(otm_cooldown_trend60_min5_ivcap45_width10);

    let mut otm_cooldown_trend60_min10_ivcap45 = baseline.clone();
    otm_cooldown_trend60_min10_ivcap45.name =
        "select_farther_otm_cooldown10_trend60d_min10_ivcap45_delta20_30_credit20".to_owned();
    otm_cooldown_trend60_min10_ivcap45.prefer_farther_otm = true;
    otm_cooldown_trend60_min10_ivcap45.stop_loss_cooldown_days = 10;
    otm_cooldown_trend60_min10_ivcap45.trend_lookback_days = Some(60);
    otm_cooldown_trend60_min10_ivcap45.min_underlying_return = Some(0.10);
    otm_cooldown_trend60_min10_ivcap45.max_short_iv = Some(0.45);
    profiles.push(otm_cooldown_trend60_min10_ivcap45);

    let mut otm_cooldown_trend60_otm8 = baseline.clone();
    otm_cooldown_trend60_otm8.name =
        "select_farther_otm_cooldown10_trend60d_otm8pct_delta20_30_credit20".to_owned();
    otm_cooldown_trend60_otm8.prefer_farther_otm = true;
    otm_cooldown_trend60_otm8.stop_loss_cooldown_days = 10;
    otm_cooldown_trend60_otm8.trend_lookback_days = Some(60);
    otm_cooldown_trend60_otm8.min_underlying_return = Some(0.0);
    otm_cooldown_trend60_otm8.min_short_otm_pct = Some(0.08);
    profiles.push(otm_cooldown_trend60_otm8);

    for (suffix, max_concurrent_positions, min_entry_spacing_days) in [
        ("overlap2_gap5", 2, 5),
        ("overlap2_gap7", 2, 7),
        ("overlap3_gap5", 3, 5),
        ("overlap3_gap7", 3, 7),
        ("overlap4_gap5", 4, 5),
    ] {
        let mut profile = ResearchProfile::frozen_baseline();
        profile.name = format!(
            "frozen_delta26_34_take45_{suffix}_weak13dd2to5_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_width15_lowdelta23_width10_credit20"
        );
        profile.max_concurrent_positions = max_concurrent_positions;
        profile.min_entry_spacing_days = min_entry_spacing_days;
        profiles.push(profile);
    }

    profiles
}

async fn discover_expirations_with_cache_mode(
    symbol: &str,
    raw_dir: &Path,
    force_refresh: bool,
    cache_only: bool,
) -> Result<Vec<NaiveDate>> {
    let path = raw_dir.join("expirations.json");
    let url =
        format!("http://127.0.0.1:25503/v3/option/list/expirations?symbol={symbol}&format=json");
    let json = fetch_cached_json(&url, &path, force_refresh, cache_only).await?;
    let response = json
        .get("response")
        .and_then(Value::as_array)
        .context("Theta expirations response missing response[]")?;
    response
        .iter()
        .map(|row| {
            let expiration = row
                .get("expiration")
                .and_then(Value::as_str)
                .context("expiration row missing expiration")?;
            NaiveDate::parse_from_str(expiration, "%Y-%m-%d")
                .with_context(|| format!("parsing expiration {expiration}"))
        })
        .collect()
}

#[derive(Clone, Debug)]
struct LoadedExpirationRows {
    primary: Vec<OptionDay>,
    calls: Vec<OptionDay>,
}

impl LoadedExpirationRows {
    fn has_required_rows(&self, mode: OptionDataMode) -> bool {
        match mode {
            OptionDataMode::Single(_) => !self.primary.is_empty(),
            OptionDataMode::PutAndCall => !self.primary.is_empty() && !self.calls.is_empty(),
        }
    }
}

async fn load_expiration_rows_for_mode(
    symbol: &str,
    expiration: NaiveDate,
    start: NaiveDate,
    end: NaiveDate,
    raw_dir: &Path,
    force_refresh: bool,
    mode: OptionDataMode,
) -> Result<LoadedExpirationRows> {
    load_expiration_rows_for_mode_with_cache_mode(
        symbol,
        expiration,
        start,
        end,
        raw_dir,
        force_refresh,
        false,
        mode,
    )
    .await
}

async fn load_expiration_rows_for_mode_with_cache_mode(
    symbol: &str,
    expiration: NaiveDate,
    start: NaiveDate,
    end: NaiveDate,
    raw_dir: &Path,
    force_refresh: bool,
    cache_only: bool,
    mode: OptionDataMode,
) -> Result<LoadedExpirationRows> {
    match mode {
        OptionDataMode::Single(option_right) => {
            let primary = load_expiration_rows(
                symbol,
                expiration,
                start,
                end,
                raw_dir,
                force_refresh,
                cache_only,
                option_right,
            )
            .await?;
            Ok(LoadedExpirationRows {
                primary,
                calls: Vec::new(),
            })
        }
        OptionDataMode::PutAndCall => {
            let puts = load_expiration_rows(
                symbol,
                expiration,
                start,
                end,
                raw_dir,
                force_refresh,
                cache_only,
                OptionRight::Put,
            )
            .await?;
            let calls = if cache_only
                && !option_cache_has_complete_coverage(
                    raw_dir,
                    &yyyymmdd(expiration),
                    start,
                    end,
                    OptionRight::Call,
                )? {
                Vec::new()
            } else {
                match load_expiration_rows(
                    symbol,
                    expiration,
                    start,
                    end,
                    raw_dir,
                    force_refresh,
                    cache_only,
                    OptionRight::Call,
                )
                .await
                {
                    Ok(calls) => calls,
                    Err(_error) if cache_only && !puts.is_empty() => Vec::new(),
                    Err(error) => return Err(error),
                }
            };
            Ok(LoadedExpirationRows {
                primary: puts,
                calls,
            })
        }
    }
}

async fn load_expiration_rows(
    symbol: &str,
    expiration: NaiveDate,
    start: NaiveDate,
    end: NaiveDate,
    raw_dir: &Path,
    force_refresh: bool,
    cache_only: bool,
    option_right: OptionRight,
) -> Result<Vec<OptionDay>> {
    let oi_map = load_open_interest_map(
        symbol,
        expiration,
        start,
        end,
        raw_dir,
        force_refresh,
        cache_only,
        option_right,
    )
    .await
    .with_context(|| format!("loading open interest for {symbol} {expiration}"))?;
    let mut rows = load_greeks_rows(
        symbol,
        expiration,
        start,
        end,
        raw_dir,
        force_refresh,
        cache_only,
        &oi_map,
        option_right,
    )
    .await
    .with_context(|| format!("loading EOD Greeks for {symbol} {expiration}"))?;
    rows.sort_by(|a, b| {
        a.date
            .cmp(&b.date)
            .then_with(|| a.strike.total_cmp(&b.strike))
    });
    Ok(rows)
}

async fn load_greeks_rows(
    symbol: &str,
    expiration: NaiveDate,
    start: NaiveDate,
    end: NaiveDate,
    raw_dir: &Path,
    force_refresh: bool,
    cache_only: bool,
    oi_map: &HashMap<(NaiveDate, String), u32>,
    option_right: OptionRight,
) -> Result<Vec<OptionDay>> {
    let exp = yyyymmdd(expiration);
    let cache_prefix = option_right.cache_prefix();
    let right = option_right.query_value();
    let mut out = Vec::new();
    let mut chunk_start = start;
    while chunk_start <= end {
        let chunk_end = end.min(chunk_start + Duration::days(6));
        let chunk_start_s = yyyymmdd(chunk_start);
        let chunk_end_s = yyyymmdd(chunk_end);
        let greeks_path = raw_dir.join(format!(
            "{cache_prefix}_greeks_{exp}_{chunk_start_s}_{chunk_end_s}.json"
        ));
        let greeks_url = format!(
            "http://127.0.0.1:25503/v3/option/history/greeks/eod?symbol={symbol}&expiration={exp}&right={right}&start_date={chunk_start_s}&end_date={chunk_end_s}&format=json"
        );
        match fetch_cached_json(&greeks_url, &greeks_path, force_refresh, cache_only).await {
            Ok(greeks) => {
                extend_greeks_rows_for_window(&mut out, &greeks, oi_map, chunk_start, chunk_end)?
            }
            Err(error) if cache_only => {
                if let Some(sources) = read_option_cache_sequence(
                    raw_dir,
                    &exp,
                    chunk_start,
                    chunk_end,
                    option_right,
                    CachedOptionDataset::Greeks,
                )? {
                    for (_cache_path, greeks) in sources {
                        extend_greeks_rows_for_window(
                            &mut out,
                            &greeks,
                            oi_map,
                            chunk_start,
                            chunk_end,
                        )?;
                    }
                } else {
                    return Err(error);
                }
            }
            Err(error) => return Err(error),
        }
        chunk_start = chunk_end + Duration::days(1);
    }
    Ok(out)
}

async fn load_open_interest_map(
    symbol: &str,
    expiration: NaiveDate,
    start: NaiveDate,
    end: NaiveDate,
    raw_dir: &Path,
    force_refresh: bool,
    cache_only: bool,
    option_right: OptionRight,
) -> Result<HashMap<(NaiveDate, String), u32>> {
    let exp = yyyymmdd(expiration);
    let right = option_right.query_value();
    let mut out = HashMap::new();
    let mut chunk_start = start;
    let mut chunks = VecDeque::new();
    while chunk_start <= end {
        let chunk_end = end.min(chunk_start + Duration::days(6));
        chunks.push_back((chunk_start, chunk_end));
        chunk_start = chunk_end + Duration::days(1);
    }
    while let Some((chunk_start, chunk_end)) = chunks.pop_front() {
        let chunk_start_s = yyyymmdd(chunk_start);
        let chunk_end_s = yyyymmdd(chunk_end);
        let oi_path = oi_cache_path(raw_dir, &exp, chunk_start, chunk_end, option_right);
        let oi_url = format!(
            "http://127.0.0.1:25503/v3/option/history/open_interest?symbol={symbol}&expiration={exp}&right={right}&start_date={chunk_start_s}&end_date={chunk_end_s}&format=json"
        );
        match fetch_cached_json(&oi_url, &oi_path, force_refresh, cache_only).await {
            Ok(oi) => extend_oi_map_for_window(&mut out, &oi, chunk_start, chunk_end)?,
            Err(error) if cache_only => {
                if let Some(sources) = read_option_cache_sequence(
                    raw_dir,
                    &exp,
                    chunk_start,
                    chunk_end,
                    option_right,
                    CachedOptionDataset::OpenInterest,
                )? {
                    for (_cache_path, oi) in sources {
                        extend_oi_map_for_window(&mut out, &oi, chunk_start, chunk_end)?;
                    }
                } else if chunk_start < chunk_end
                    && all_daily_oi_cache_exists(
                        raw_dir,
                        &exp,
                        chunk_start,
                        chunk_end,
                        option_right,
                    )
                {
                    push_daily_chunks_front(&mut chunks, chunk_start, chunk_end);
                    eprintln!(
                        "using cached daily open-interest chunks for missing weekly cache {}..{}",
                        chunk_start, chunk_end
                    );
                } else {
                    return Err(error);
                }
            }
            Err(error) if chunk_start < chunk_end && !is_non_retryable_thetadata_error(&error) => {
                split_oi_remainder_to_daily(
                    &mut chunks,
                    raw_dir,
                    &exp,
                    force_refresh,
                    option_right,
                    chunk_start,
                    chunk_end,
                );
                eprintln!(
                    "splitting open-interest chunk {}..{} and uncached remaining chunks into daily requests after error: {error:#}",
                    chunk_start, chunk_end
                );
            }
            Err(error) => return Err(error),
        }
    }
    Ok(out)
}

fn extend_greeks_rows_for_window(
    out: &mut Vec<OptionDay>,
    json: &Value,
    oi_map: &HashMap<(NaiveDate, String), u32>,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<()> {
    out.extend(
        parse_greeks_rows(json, oi_map)?
            .into_iter()
            .filter(|row| row.date >= start && row.date <= end),
    );
    Ok(())
}

fn extend_oi_map_for_window(
    out: &mut HashMap<(NaiveDate, String), u32>,
    json: &Value,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<()> {
    out.extend(
        parse_oi_map(json)?
            .into_iter()
            .filter(|((date, _strike), _oi)| *date >= start && *date <= end),
    );
    Ok(())
}

fn read_option_cache_sequence(
    raw_dir: &Path,
    exp: &str,
    start: NaiveDate,
    end: NaiveDate,
    option_right: OptionRight,
    dataset: CachedOptionDataset,
) -> Result<Option<Vec<(PathBuf, Arc<Value>)>>> {
    let windows =
        cached_option_cache_covering_sequence(raw_dir, exp, start, end, option_right, dataset)?;
    if windows.is_empty() {
        return Ok(None);
    }
    let mut sources = Vec::with_capacity(windows.len());
    for window in windows {
        let Some(json) = read_option_cache_json_cached(&window.path)? else {
            return Ok(None);
        };
        sources.push((window.path, json));
    }
    Ok(Some(sources))
}

static OPTION_CACHE_INDEXES: OnceLock<Mutex<HashMap<PathBuf, OptionCacheCoverageIndex>>> =
    OnceLock::new();
static OPTION_CACHE_JSONS: OnceLock<Mutex<HashMap<PathBuf, Option<Arc<Value>>>>> = OnceLock::new();

fn read_option_cache_json_cached(path: &Path) -> Result<Option<Arc<Value>>> {
    let key = path.to_path_buf();
    let cache = OPTION_CACHE_JSONS.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(json) = cache
        .lock()
        .map_err(|_| anyhow::anyhow!("option cache JSON cache lock poisoned"))?
        .get(&key)
        .cloned()
    {
        return Ok(json);
    }

    let json = read_cached_json(path)?.map(Arc::new);
    cache
        .lock()
        .map_err(|_| anyhow::anyhow!("option cache JSON cache lock poisoned"))?
        .insert(key, json.clone());
    Ok(json)
}

fn cached_option_cache_covering_sequence(
    raw_dir: &Path,
    exp: &str,
    start: NaiveDate,
    end: NaiveDate,
    option_right: OptionRight,
    dataset: CachedOptionDataset,
) -> Result<Vec<CachedOptionWindow>> {
    let raw_dir = raw_dir.to_path_buf();
    let cache = OPTION_CACHE_INDEXES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut cache = cache
        .lock()
        .map_err(|_| anyhow::anyhow!("option cache coverage index lock poisoned"))?;
    if !cache.contains_key(&raw_dir) {
        let index = OptionCacheCoverageIndex::build(&raw_dir)?;
        cache.insert(raw_dir.clone(), index);
    }
    let windows = cache
        .get(&raw_dir)
        .and_then(|index| index.covering_sequence(exp, start, end, option_right, dataset))
        .unwrap_or_default();
    if !windows.is_empty() {
        return Ok(windows);
    }

    let index = OptionCacheCoverageIndex::build(&raw_dir)?;
    let windows = index
        .covering_sequence(exp, start, end, option_right, dataset)
        .unwrap_or_default();
    cache.insert(raw_dir, index);
    Ok(windows)
}

fn option_cache_has_complete_coverage(
    raw_dir: &Path,
    exp: &str,
    start: NaiveDate,
    end: NaiveDate,
    option_right: OptionRight,
) -> Result<bool> {
    Ok(!cached_option_cache_covering_sequence(
        raw_dir,
        exp,
        start,
        end,
        option_right,
        CachedOptionDataset::OpenInterest,
    )?
    .is_empty()
        && !cached_option_cache_covering_sequence(
            raw_dir,
            exp,
            start,
            end,
            option_right,
            CachedOptionDataset::Greeks,
        )?
        .is_empty())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum CachedOptionDataset {
    Greeks,
    OpenInterest,
}

impl CachedOptionDataset {
    fn cache_component(self) -> &'static str {
        match self {
            Self::Greeks => "greeks",
            Self::OpenInterest => "oi",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CachedOptionWindow {
    path: PathBuf,
    start: NaiveDate,
    end: NaiveDate,
}

#[derive(Clone, Debug, Default)]
struct OptionCacheCoverageIndex {
    windows: HashMap<(OptionRight, CachedOptionDataset, String), Vec<CachedOptionWindow>>,
}

impl OptionCacheCoverageIndex {
    fn build(raw_dir: &Path) -> Result<Self> {
        let mut index = Self::default();
        if !raw_dir.exists() {
            return Ok(index);
        }
        for entry in
            fs::read_dir(raw_dir).with_context(|| format!("reading {}", raw_dir.display()))?
        {
            let entry = entry.with_context(|| format!("reading entry in {}", raw_dir.display()))?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let Some((right, dataset, exp, start, end)) = parse_any_option_cache_window(file_name)
            else {
                continue;
            };
            index
                .windows
                .entry((right, dataset, exp))
                .or_default()
                .push(CachedOptionWindow { path, start, end });
        }
        for windows in index.windows.values_mut() {
            windows.sort_by(|a, b| {
                a.start
                    .cmp(&b.start)
                    .then_with(|| a.end.cmp(&b.end))
                    .then_with(|| a.path.cmp(&b.path))
            });
        }
        Ok(index)
    }

    fn has_complete_coverage(
        &self,
        exp: &str,
        start: NaiveDate,
        end: NaiveDate,
        option_right: OptionRight,
    ) -> bool {
        self.covering_sequence(
            exp,
            start,
            end,
            option_right,
            CachedOptionDataset::OpenInterest,
        )
        .is_some()
            && self
                .covering_sequence(exp, start, end, option_right, CachedOptionDataset::Greeks)
                .is_some()
    }

    fn covering_sequence(
        &self,
        exp: &str,
        start: NaiveDate,
        end: NaiveDate,
        option_right: OptionRight,
        dataset: CachedOptionDataset,
    ) -> Option<Vec<CachedOptionWindow>> {
        let key = (option_right, dataset, exp.to_owned());
        let windows = self.windows.get(&key).map(Vec::as_slice).unwrap_or(&[]);
        option_cache_covering_sequence_from_windows(windows, start, end)
    }
}

#[cfg(test)]
fn option_cache_covering_sequence(
    raw_dir: &Path,
    exp: &str,
    start: NaiveDate,
    end: NaiveDate,
    option_right: OptionRight,
    dataset: CachedOptionDataset,
) -> Result<Vec<CachedOptionWindow>> {
    let windows = matching_option_cache_windows(raw_dir, exp, option_right, dataset)?;
    Ok(option_cache_covering_sequence_from_windows(&windows, start, end).unwrap_or_default())
}

fn option_cache_covering_sequence_from_windows(
    windows: &[CachedOptionWindow],
    start: NaiveDate,
    end: NaiveDate,
) -> Option<Vec<CachedOptionWindow>> {
    let mut cursor = start;
    let mut selected = Vec::new();
    while cursor <= end {
        let Some(best) = windows
            .iter()
            .filter(|window| window.start <= cursor && window.end >= cursor)
            .max_by(|a, b| {
                a.end
                    .cmp(&b.end)
                    .then_with(|| {
                        let b_span = (b.end - b.start).num_days();
                        let a_span = (a.end - a.start).num_days();
                        b_span.cmp(&a_span)
                    })
                    .then_with(|| b.start.cmp(&a.start))
                    .then_with(|| b.path.cmp(&a.path))
            })
        else {
            return None;
        };
        selected.push(best.clone());
        cursor = best.end + Duration::days(1);
    }
    Some(selected)
}

#[cfg(test)]
fn covering_option_cache_windows(
    raw_dir: &Path,
    exp: &str,
    start: NaiveDate,
    end: NaiveDate,
    option_right: OptionRight,
    dataset: CachedOptionDataset,
) -> Result<Vec<CachedOptionWindow>> {
    let mut windows = matching_option_cache_windows(raw_dir, exp, option_right, dataset)?
        .into_iter()
        .filter(|window| window.start <= start && window.end >= end)
        .collect::<Vec<_>>();
    windows.sort_by(|a, b| {
        let a_span = (a.end - a.start).num_days();
        let b_span = (b.end - b.start).num_days();
        a_span
            .cmp(&b_span)
            .then_with(|| a.start.cmp(&b.start))
            .then_with(|| a.end.cmp(&b.end))
            .then_with(|| a.path.cmp(&b.path))
    });
    Ok(windows)
}

#[cfg(test)]
fn matching_option_cache_windows(
    raw_dir: &Path,
    exp: &str,
    option_right: OptionRight,
    dataset: CachedOptionDataset,
) -> Result<Vec<CachedOptionWindow>> {
    let mut windows = Vec::new();
    if !raw_dir.exists() {
        return Ok(windows);
    }
    for entry in fs::read_dir(raw_dir).with_context(|| format!("reading {}", raw_dir.display()))? {
        let entry = entry.with_context(|| format!("reading entry in {}", raw_dir.display()))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some((cached_start, cached_end)) =
            parse_option_cache_window(file_name, exp, option_right, dataset)
        else {
            continue;
        };
        windows.push(CachedOptionWindow {
            path,
            start: cached_start,
            end: cached_end,
        });
    }
    windows.sort_by(|a, b| {
        a.start
            .cmp(&b.start)
            .then_with(|| a.end.cmp(&b.end))
            .then_with(|| a.path.cmp(&b.path))
    });
    Ok(windows)
}

#[cfg(test)]
fn parse_option_cache_window(
    file_name: &str,
    exp: &str,
    option_right: OptionRight,
    dataset: CachedOptionDataset,
) -> Option<(NaiveDate, NaiveDate)> {
    let (file_right, file_dataset, file_exp, start, end) =
        parse_any_option_cache_window(file_name)?;
    if file_right == option_right && file_dataset == dataset && file_exp == exp {
        Some((start, end))
    } else {
        None
    }
}

fn parse_any_option_cache_window(
    file_name: &str,
) -> Option<(
    OptionRight,
    CachedOptionDataset,
    String,
    NaiveDate,
    NaiveDate,
)> {
    let stem = file_name.strip_suffix(".json")?;
    for option_right in [OptionRight::Put, OptionRight::Call] {
        for dataset in [
            CachedOptionDataset::Greeks,
            CachedOptionDataset::OpenInterest,
        ] {
            let prefix = option_right.cache_prefix();
            let Some(rest) = stem.strip_prefix(&format!("{prefix}_{}_", dataset.cache_component()))
            else {
                continue;
            };
            let mut parts = rest.split('_');
            let exp = parts.next()?.to_owned();
            let start = parse_yyyymmdd(parts.next()?)?;
            let end = parse_yyyymmdd(parts.next()?)?;
            if parts.next().is_some() || start > end {
                continue;
            }
            return Some((option_right, dataset, exp, start, end));
        }
    }
    None
}

fn oi_cache_path(
    raw_dir: &Path,
    exp: &str,
    chunk_start: NaiveDate,
    chunk_end: NaiveDate,
    option_right: OptionRight,
) -> PathBuf {
    let cache_prefix = option_right.cache_prefix();
    raw_dir.join(format!(
        "{cache_prefix}_oi_{exp}_{}_{}.json",
        yyyymmdd(chunk_start),
        yyyymmdd(chunk_end)
    ))
}

fn all_daily_oi_cache_exists(
    raw_dir: &Path,
    exp: &str,
    start: NaiveDate,
    end: NaiveDate,
    option_right: OptionRight,
) -> bool {
    (0..=(end - start).num_days()).all(|offset| {
        let day = start + Duration::days(offset);
        oi_cache_path(raw_dir, exp, day, day, option_right).exists()
    })
}

fn split_oi_remainder_to_daily(
    chunks: &mut VecDeque<(NaiveDate, NaiveDate)>,
    raw_dir: &Path,
    exp: &str,
    force_refresh: bool,
    option_right: OptionRight,
    failed_start: NaiveDate,
    failed_end: NaiveDate,
) {
    let mut ranges = Vec::with_capacity(chunks.len() + 1);
    ranges.push((failed_start, failed_end));
    ranges.extend(chunks.drain(..));
    for (start, end) in ranges.into_iter().rev() {
        if start == end
            || (!force_refresh && oi_cache_path(raw_dir, exp, start, end, option_right).exists())
        {
            chunks.push_front((start, end));
        } else {
            push_daily_chunks_front(chunks, start, end);
        }
    }
}

fn push_daily_chunks_front(
    chunks: &mut VecDeque<(NaiveDate, NaiveDate)>,
    start: NaiveDate,
    end: NaiveDate,
) {
    for offset in (0..=(end - start).num_days()).rev() {
        let day = start + Duration::days(offset);
        chunks.push_front((day, day));
    }
}

async fn fetch_cached_json(
    url: &str,
    path: &Path,
    force_refresh: bool,
    cache_only: bool,
) -> Result<Value> {
    if !force_refresh && let Some(json) = read_cached_json(path)? {
        return Ok(json);
    }
    if cache_only {
        anyhow::bail!("cache-only ThetaData miss: {}", path.display());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let client = reqwest::Client::builder()
        .timeout(StdDuration::from_secs(20))
        .build()?;
    let mut last_error = None;
    for attempt in 1..=FETCH_ATTEMPTS {
        match fetch_json_once(&client, url).await {
            Ok(json) => {
                write_cached_json(path, &json)?;
                return Ok(json);
            }
            Err(error) => {
                if is_non_retryable_thetadata_error(&error) {
                    return Err(error)
                        .with_context(|| format!("ThetaData non-retryable request failed: {url}"));
                }
                last_error = Some(error);
                if attempt < FETCH_ATTEMPTS {
                    sleep(StdDuration::from_millis(250 * attempt as u64)).await;
                }
            }
        }
    }
    let error = last_error.context("ThetaData request failed without an error payload")?;
    Err(error)
        .with_context(|| format!("ThetaData request failed after {FETCH_ATTEMPTS} attempts: {url}"))
}

fn is_non_retryable_thetadata_error(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}");
    message.contains("HTTP 403 Forbidden")
        && message.contains("PROFESSIONAL subscription")
        && message.contains("STANDARD subscription")
}

fn read_cached_json(path: &Path) -> Result<Option<Value>> {
    if !path.exists() {
        return Ok(None);
    }
    let body = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    match serde_json::from_str(&body) {
        Ok(json) => Ok(Some(json)),
        Err(error) => {
            eprintln!(
                "ignoring corrupt ThetaData cache {}: {error}",
                path.display()
            );
            Ok(None)
        }
    }
}

fn write_cached_json(path: &Path, json: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temp_path = cache_temp_path(path);
    let body = serde_json::to_string_pretty(json)?;
    let write_result = fs::write(&temp_path, body)
        .with_context(|| format!("writing temporary cache {}", temp_path.display()))
        .and_then(|_| {
            fs::rename(&temp_path, path).with_context(|| {
                format!(
                    "renaming temporary cache {} to {}",
                    temp_path.display(),
                    path.display()
                )
            })
        });
    if write_result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    write_result
}

fn cache_temp_path(path: &Path) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    path.with_extension(format!("tmp-{}-{nanos}", std::process::id()))
}

async fn fetch_json_once(client: &reqwest::Client, url: &str) -> Result<Value> {
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("requesting {url}"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .with_context(|| format!("reading ThetaData response for {url}"))?;
    if !status.is_success() {
        let trimmed = body.trim();
        if status.as_u16() == 472 && trimmed.contains("No data found") {
            return Ok(json!({ "response": [] }));
        }
        anyhow::bail!("ThetaData returned HTTP {status} for {url}: {trimmed}");
    }
    let json: Value = serde_json::from_str(&body)
        .with_context(|| format!("ThetaData did not return JSON for {url}: {}", body.trim()))?;
    Ok(json)
}

fn parse_oi_map(json: &Value) -> Result<HashMap<(NaiveDate, String), u32>> {
    let mut out = HashMap::new();
    let Some(response) = json.get("response").and_then(Value::as_array) else {
        return Ok(out);
    };
    for contract in response {
        let strike = contract_key_strike(contract)?;
        let Some(data) = contract.get("data").and_then(Value::as_array) else {
            continue;
        };
        for row in data {
            if let Some(date) = row_date(row) {
                let oi = row
                    .get("open_interest")
                    .and_then(Value::as_u64)
                    .unwrap_or(0) as u32;
                out.insert((date, strike.clone()), oi);
            }
        }
    }
    Ok(out)
}

fn parse_greeks_rows(
    json: &Value,
    oi_map: &HashMap<(NaiveDate, String), u32>,
) -> Result<Vec<OptionDay>> {
    let mut out = Vec::new();
    let Some(response) = json.get("response").and_then(Value::as_array) else {
        return Ok(out);
    };
    for contract in response {
        let strike_key = contract_key_strike(contract)?;
        let strike = strike_key.parse::<f64>()?;
        let Some(data) = contract.get("data").and_then(Value::as_array) else {
            continue;
        };
        for row in data {
            let Some(date) = row_date(row) else {
                continue;
            };
            let bid = number(row, "bid");
            let ask = number(row, "ask");
            let delta = number(row, "delta");
            if bid <= 0.0 || ask <= 0.0 || ask < bid {
                continue;
            }
            out.push(OptionDay {
                date,
                strike,
                bid,
                ask,
                delta,
                implied_vol: number(row, "implied_vol"),
                underlying_price: number(row, "underlying_price"),
                open_interest: *oi_map.get(&(date, strike_key.clone())).unwrap_or(&0),
            });
        }
    }
    Ok(out)
}

fn contract_key_strike(contract: &Value) -> Result<String> {
    let strike = contract
        .get("contract")
        .and_then(|v| v.get("strike"))
        .or_else(|| contract.get("strike"))
        .and_then(Value::as_f64)
        .context("contract missing strike")?;
    Ok(format!("{strike:.3}"))
}

fn row_date(row: &Value) -> Option<NaiveDate> {
    let ts = row.get("timestamp")?.as_str()?;
    NaiveDate::parse_from_str(ts.get(0..10)?, "%Y-%m-%d").ok()
}

fn number(row: &Value, key: &str) -> f64 {
    row.get(key).and_then(Value::as_f64).unwrap_or(0.0)
}

fn generate_candidates(
    rows_by_expiration: &BTreeMap<NaiveDate, Vec<OptionDay>>,
    profile: &ResearchProfile,
    entry_from: NaiveDate,
    entry_to: NaiveDate,
) -> Vec<Candidate> {
    let mut candidates = Vec::new();
    let underlying_by_date = underlying_by_date_from_expirations(rows_by_expiration);
    for (expiration, rows) in rows_by_expiration {
        let mut by_date: BTreeMap<NaiveDate, Vec<&OptionDay>> = BTreeMap::new();
        for row in rows {
            if row.date < entry_from || row.date > entry_to {
                continue;
            }
            let dte = (*expiration - row.date).num_days();
            if dte < profile.min_dte || dte > profile.max_dte {
                continue;
            }
            by_date.entry(row.date).or_default().push(row);
        }
        for (date, day_rows) in by_date {
            match profile.structure {
                SpreadStructure::PutCreditSpread => generate_put_credit_candidates_for_day(
                    *expiration,
                    date,
                    &day_rows,
                    profile,
                    &underlying_by_date,
                    &mut candidates,
                ),
                SpreadStructure::CallCreditSpread => generate_call_credit_candidates_for_day(
                    *expiration,
                    date,
                    &day_rows,
                    profile,
                    &underlying_by_date,
                    &mut candidates,
                ),
                SpreadStructure::PutDebitSpread => generate_put_debit_candidates_for_day(
                    *expiration,
                    date,
                    &day_rows,
                    profile,
                    &underlying_by_date,
                    &mut candidates,
                ),
                SpreadStructure::CallDebitSpread => generate_call_debit_candidates_for_day(
                    *expiration,
                    date,
                    &day_rows,
                    profile,
                    &underlying_by_date,
                    &mut candidates,
                ),
                SpreadStructure::Wheel => generate_wheel_put_candidates_for_day(
                    *expiration,
                    date,
                    &day_rows,
                    profile,
                    &underlying_by_date,
                    &mut candidates,
                ),
            }
        }
    }
    candidates.sort_by(candidate_chronological_order);
    candidates
}

#[derive(Clone, Copy, Debug, Default)]
struct WeeklySignalGateCounts {
    dte_rows: usize,
    dte_entry_days: usize,
    primary_leg_passes: usize,
    regime_passes: usize,
}

fn weekly_signal_gate_counts(
    rows_by_expiration: &BTreeMap<NaiveDate, Vec<OptionDay>>,
    profile: &ResearchProfile,
    entry_from: NaiveDate,
    entry_to: NaiveDate,
) -> WeeklySignalGateCounts {
    let underlying_by_date = underlying_by_date_from_expirations(rows_by_expiration);
    let option_right = primary_option_right(profile.structure);
    let mut counts = WeeklySignalGateCounts::default();
    let mut dte_entry_days = BTreeSet::new();
    for (expiration, rows) in rows_by_expiration {
        for row in rows {
            if row.date < entry_from || row.date > entry_to {
                continue;
            }
            let dte = (*expiration - row.date).num_days();
            if dte < profile.min_dte || dte > profile.max_dte {
                continue;
            }
            counts.dte_rows += 1;
            dte_entry_days.insert(row.date);
            if !primary_leg_allowed(row, profile) {
                continue;
            }
            counts.primary_leg_passes += 1;
            if entry_regime(row, profile, &underlying_by_date, option_right).is_some() {
                counts.regime_passes += 1;
            }
        }
    }
    counts.dte_entry_days = dte_entry_days.len();
    counts
}

fn primary_option_right(structure: SpreadStructure) -> OptionRight {
    match structure {
        SpreadStructure::PutCreditSpread
        | SpreadStructure::PutDebitSpread
        | SpreadStructure::Wheel => OptionRight::Put,
        SpreadStructure::CallCreditSpread | SpreadStructure::CallDebitSpread => OptionRight::Call,
    }
}

fn primary_leg_allowed(row: &OptionDay, profile: &ResearchProfile) -> bool {
    let delta = row.delta.abs();
    let min_oi = match profile.structure {
        SpreadStructure::PutDebitSpread | SpreadStructure::CallDebitSpread => profile.min_long_oi,
        SpreadStructure::PutCreditSpread
        | SpreadStructure::CallCreditSpread
        | SpreadStructure::Wheel => profile.min_short_oi,
    };
    delta >= profile.min_short_delta_abs
        && delta <= profile.max_short_delta_abs
        && row.open_interest >= min_oi
        && quote_width_allowed(row, profile)
        && iv_allowed(row, profile)
}

fn generate_put_credit_candidates_for_day(
    expiration: NaiveDate,
    date: NaiveDate,
    day_rows: &[&OptionDay],
    profile: &ResearchProfile,
    underlying_by_date: &BTreeMap<NaiveDate, f64>,
    candidates: &mut Vec<Candidate>,
) {
    for short in day_rows {
        let short_delta = short.delta.abs();
        if short_delta < profile.min_short_delta_abs
            || short_delta > profile.max_short_delta_abs
            || short.open_interest < profile.min_short_oi
            || !quote_width_allowed(short, profile)
            || !iv_allowed(short, profile)
        {
            continue;
        }
        let Some(entry_regime) = entry_regime(short, profile, underlying_by_date, OptionRight::Put)
        else {
            continue;
        };
        for long in day_rows {
            if long.strike >= short.strike
                || long.open_interest < profile.min_long_oi
                || !quote_width_allowed(long, profile)
                || !iv_skew_allowed(short, long, profile)
            {
                continue;
            }
            let width = short.strike - long.strike;
            if !width_allowed(width, short_delta, profile) {
                continue;
            }
            let credit = conservative_credit_spread_entry_credit_f64(short.bid, long.ask);
            if credit <= 0.0 {
                continue;
            }
            let credit_width = credit / width;
            if credit_width < profile.min_credit_width {
                continue;
            }
            let max_loss = width - credit;
            if max_loss <= 0.0 {
                continue;
            }
            candidates.push(Candidate {
                structure: SpreadStructure::PutCreditSpread,
                entry_date: date,
                expiration,
                short: (*short).clone(),
                long: (*long).clone(),
                width,
                credit,
                max_profit_per_share: credit,
                max_loss_per_share: max_loss,
                return_on_risk: credit / max_loss,
                short_otm_pct: entry_regime.short_otm_pct,
                underlying_lookback_return: entry_regime.underlying_lookback_return,
                underlying_recent_drawdown: entry_regime.underlying_recent_drawdown,
                underlying_realized_vol: entry_regime.underlying_realized_vol,
                short_iv: short.implied_vol,
                long_iv: long.implied_vol,
            });
        }
    }
}

fn generate_call_credit_candidates_for_day(
    expiration: NaiveDate,
    date: NaiveDate,
    day_rows: &[&OptionDay],
    profile: &ResearchProfile,
    underlying_by_date: &BTreeMap<NaiveDate, f64>,
    candidates: &mut Vec<Candidate>,
) {
    for short in day_rows {
        let short_delta = short.delta.abs();
        if short_delta < profile.min_short_delta_abs
            || short_delta > profile.max_short_delta_abs
            || short.open_interest < profile.min_short_oi
            || !quote_width_allowed(short, profile)
            || !iv_allowed(short, profile)
        {
            continue;
        }
        let Some(entry_regime) =
            entry_regime(short, profile, underlying_by_date, OptionRight::Call)
        else {
            continue;
        };
        for long in day_rows {
            if long.strike <= short.strike
                || long.open_interest < profile.min_long_oi
                || !quote_width_allowed(long, profile)
                || !iv_skew_allowed(short, long, profile)
            {
                continue;
            }
            let width = long.strike - short.strike;
            if !width_allowed(width, short_delta, profile) {
                continue;
            }
            let credit = conservative_credit_spread_entry_credit_f64(short.bid, long.ask);
            if credit <= 0.0 {
                continue;
            }
            let credit_width = credit / width;
            if credit_width < profile.min_credit_width {
                continue;
            }
            let max_loss = width - credit;
            if max_loss <= 0.0 {
                continue;
            }
            candidates.push(Candidate {
                structure: SpreadStructure::CallCreditSpread,
                entry_date: date,
                expiration,
                short: (*short).clone(),
                long: (*long).clone(),
                width,
                credit,
                max_profit_per_share: credit,
                max_loss_per_share: max_loss,
                return_on_risk: credit / max_loss,
                short_otm_pct: entry_regime.short_otm_pct,
                underlying_lookback_return: entry_regime.underlying_lookback_return,
                underlying_recent_drawdown: entry_regime.underlying_recent_drawdown,
                underlying_realized_vol: entry_regime.underlying_realized_vol,
                short_iv: short.implied_vol,
                long_iv: long.implied_vol,
            });
        }
    }
}

fn generate_wheel_put_candidates_for_day(
    expiration: NaiveDate,
    date: NaiveDate,
    day_rows: &[&OptionDay],
    profile: &ResearchProfile,
    underlying_by_date: &BTreeMap<NaiveDate, f64>,
    candidates: &mut Vec<Candidate>,
) {
    for short in day_rows {
        let short_delta = short.delta.abs();
        if short_delta < profile.min_short_delta_abs
            || short_delta > profile.max_short_delta_abs
            || short.open_interest < profile.min_short_oi
            || !quote_width_allowed(short, profile)
            || !iv_allowed(short, profile)
        {
            continue;
        }
        let Some(entry_regime) = entry_regime(short, profile, underlying_by_date, OptionRight::Put)
        else {
            continue;
        };
        let credit = short.bid;
        if credit <= 0.0 {
            continue;
        }
        let max_loss = cash_secured_put_max_loss_per_share(short.strike, credit);
        if max_loss <= 0.0 {
            continue;
        }
        let credit_width = credit / short.strike;
        if credit_width < profile.min_credit_width {
            continue;
        }
        candidates.push(Candidate {
            structure: SpreadStructure::Wheel,
            entry_date: date,
            expiration,
            short: (*short).clone(),
            long: (*short).clone(),
            width: short.strike,
            credit,
            max_profit_per_share: credit,
            max_loss_per_share: max_loss,
            return_on_risk: credit / max_loss,
            short_otm_pct: entry_regime.short_otm_pct,
            underlying_lookback_return: entry_regime.underlying_lookback_return,
            underlying_recent_drawdown: entry_regime.underlying_recent_drawdown,
            underlying_realized_vol: entry_regime.underlying_realized_vol,
            short_iv: short.implied_vol,
            long_iv: 0.0,
        });
    }
}

fn generate_put_debit_candidates_for_day(
    expiration: NaiveDate,
    date: NaiveDate,
    day_rows: &[&OptionDay],
    profile: &ResearchProfile,
    underlying_by_date: &BTreeMap<NaiveDate, f64>,
    candidates: &mut Vec<Candidate>,
) {
    let max_debit_width = profile.max_debit_width.unwrap_or(0.50);
    for long in day_rows {
        let long_delta = long.delta.abs();
        if long_delta < profile.min_short_delta_abs
            || long_delta > profile.max_short_delta_abs
            || long.open_interest < profile.min_long_oi
            || !quote_width_allowed(long, profile)
            || !iv_allowed(long, profile)
        {
            continue;
        }
        let Some(entry_regime) = entry_regime(long, profile, underlying_by_date, OptionRight::Put)
        else {
            continue;
        };
        for short in day_rows {
            if short.strike >= long.strike
                || short.open_interest < profile.min_short_oi
                || !quote_width_allowed(short, profile)
                || profile
                    .max_short_leg_delta_abs
                    .is_some_and(|max_delta| short.delta.abs() > max_delta)
            {
                continue;
            }
            let width = long.strike - short.strike;
            if !width_allowed(width, long_delta, profile) {
                continue;
            }
            let debit = conservative_debit_spread_entry_debit_f64(long.ask, short.bid);
            if debit <= 0.0 {
                continue;
            }
            if profile.min_debit.is_some_and(|min_debit| debit < min_debit) {
                continue;
            }
            if debit / width > max_debit_width {
                continue;
            }
            let max_profit = width - debit;
            if max_profit <= 0.0 {
                continue;
            }
            candidates.push(Candidate {
                structure: SpreadStructure::PutDebitSpread,
                entry_date: date,
                expiration,
                short: (*short).clone(),
                long: (*long).clone(),
                width,
                credit: debit,
                max_profit_per_share: max_profit,
                max_loss_per_share: debit,
                return_on_risk: max_profit / debit,
                short_otm_pct: entry_regime.short_otm_pct,
                underlying_lookback_return: entry_regime.underlying_lookback_return,
                underlying_recent_drawdown: entry_regime.underlying_recent_drawdown,
                underlying_realized_vol: entry_regime.underlying_realized_vol,
                short_iv: short.implied_vol,
                long_iv: long.implied_vol,
            });
        }
    }
}

fn generate_call_debit_candidates_for_day(
    expiration: NaiveDate,
    date: NaiveDate,
    day_rows: &[&OptionDay],
    profile: &ResearchProfile,
    underlying_by_date: &BTreeMap<NaiveDate, f64>,
    candidates: &mut Vec<Candidate>,
) {
    let max_debit_width = profile.max_debit_width.unwrap_or(0.50);
    for long in day_rows {
        let long_delta = long.delta.abs();
        if long_delta < profile.min_short_delta_abs
            || long_delta > profile.max_short_delta_abs
            || long.open_interest < profile.min_long_oi
            || !quote_width_allowed(long, profile)
            || !iv_allowed(long, profile)
        {
            continue;
        }
        let Some(entry_regime) = entry_regime(long, profile, underlying_by_date, OptionRight::Call)
        else {
            continue;
        };
        for short in day_rows {
            if short.strike <= long.strike
                || short.open_interest < profile.min_short_oi
                || !quote_width_allowed(short, profile)
                || profile
                    .max_short_leg_delta_abs
                    .is_some_and(|max_delta| short.delta.abs() > max_delta)
            {
                continue;
            }
            let width = short.strike - long.strike;
            if !width_allowed(width, long_delta, profile) {
                continue;
            }
            let debit = conservative_debit_spread_entry_debit_f64(long.ask, short.bid);
            if debit <= 0.0 {
                continue;
            }
            if profile.min_debit.is_some_and(|min_debit| debit < min_debit) {
                continue;
            }
            if debit / width > max_debit_width {
                continue;
            }
            let max_profit = width - debit;
            if max_profit <= 0.0 {
                continue;
            }
            candidates.push(Candidate {
                structure: SpreadStructure::CallDebitSpread,
                entry_date: date,
                expiration,
                short: (*short).clone(),
                long: (*long).clone(),
                width,
                credit: debit,
                max_profit_per_share: max_profit,
                max_loss_per_share: debit,
                return_on_risk: max_profit / debit,
                short_otm_pct: entry_regime.short_otm_pct,
                underlying_lookback_return: entry_regime.underlying_lookback_return,
                underlying_recent_drawdown: entry_regime.underlying_recent_drawdown,
                underlying_realized_vol: entry_regime.underlying_realized_vol,
                short_iv: short.implied_vol,
                long_iv: long.implied_vol,
            });
        }
    }
}

fn underlying_by_date_from_expirations(
    rows_by_expiration: &BTreeMap<NaiveDate, Vec<OptionDay>>,
) -> BTreeMap<NaiveDate, f64> {
    let mut out = BTreeMap::new();
    for rows in rows_by_expiration.values() {
        for row in rows {
            if row.underlying_price > 0.0 {
                out.entry(row.date).or_insert(row.underlying_price);
            }
        }
    }
    out
}

fn expiring_rows_by_date(
    rows_by_expiration: &BTreeMap<NaiveDate, Vec<OptionDay>>,
) -> BTreeMap<NaiveDate, Vec<ExpiringOptionDay>> {
    let mut out: BTreeMap<NaiveDate, Vec<ExpiringOptionDay>> = BTreeMap::new();
    for (expiration, rows) in rows_by_expiration {
        for row in rows {
            out.entry(row.date).or_default().push(ExpiringOptionDay {
                expiration: *expiration,
                row: row.clone(),
            });
        }
    }
    out
}

fn entry_regime(
    row: &OptionDay,
    profile: &ResearchProfile,
    underlying_by_date: &BTreeMap<NaiveDate, f64>,
    option_right: OptionRight,
) -> Option<EntryRegime> {
    if row.underlying_price <= 0.0 {
        return None;
    }
    let short_otm_pct = match option_right {
        OptionRight::Put => (row.underlying_price - row.strike) / row.underlying_price,
        OptionRight::Call => (row.strike - row.underlying_price) / row.underlying_price,
    };
    if let Some(min_short_otm_pct) = profile.min_short_otm_pct
        && short_otm_pct < min_short_otm_pct
    {
        return None;
    }

    let underlying_lookback_return = if let Some(days) = profile.trend_lookback_days {
        let lookback_return = underlying_return(row.date, days, underlying_by_date)?;
        if let Some(min_return) = profile.min_underlying_return
            && lookback_return < min_return
        {
            return None;
        }
        if let Some(max_return) = profile.max_underlying_return
            && lookback_return > max_return
        {
            return None;
        }
        Some(lookback_return)
    } else {
        None
    };

    let underlying_recent_drawdown = if let Some(days) = profile.drawdown_lookback_days {
        let drawdown = underlying_drawdown(row.date, days, underlying_by_date)?;
        if let Some(min_drawdown) = profile.min_underlying_drawdown
            && drawdown < min_drawdown
        {
            return None;
        }
        if let Some(max_drawdown) = profile.max_underlying_drawdown
            && drawdown > max_drawdown
        {
            return None;
        }
        Some(drawdown)
    } else {
        None
    };

    if let Some(gate) = &profile.return_or_drawdown_gate
        && !gate.allows(underlying_lookback_return, underlying_recent_drawdown)
    {
        return None;
    }
    if let Some(guard) = &profile.trend_drawdown_guard
        && !guard.allows(underlying_lookback_return, underlying_recent_drawdown)
    {
        return None;
    }
    if let Some(guard) = &profile.weak_trend_pullback_guard
        && !guard.allows(underlying_lookback_return, underlying_recent_drawdown)
    {
        return None;
    }

    let underlying_realized_vol = if let Some(days) = profile.realized_vol_lookback_days {
        let realized_vol = underlying_realized_vol(row.date, days, underlying_by_date)?;
        if let Some(min_realized_vol) = profile.min_realized_vol
            && realized_vol < min_realized_vol
        {
            return None;
        }
        if let Some(max_realized_vol) = profile.max_realized_vol
            && realized_vol > max_realized_vol
        {
            return None;
        }
        Some(realized_vol)
    } else {
        None
    };

    Some(EntryRegime {
        short_otm_pct,
        underlying_lookback_return,
        underlying_recent_drawdown,
        underlying_realized_vol,
    })
}

fn underlying_return(
    date: NaiveDate,
    lookback_days: i64,
    underlying_by_date: &BTreeMap<NaiveDate, f64>,
) -> Option<f64> {
    let current = *underlying_by_date.get(&date)?;
    let target = date - Duration::days(lookback_days);
    let (_, prior) = underlying_by_date.range(..=target).next_back()?;
    if *prior <= 0.0 {
        return None;
    }
    Some(current / prior - 1.0)
}

fn underlying_drawdown(
    date: NaiveDate,
    lookback_days: i64,
    underlying_by_date: &BTreeMap<NaiveDate, f64>,
) -> Option<f64> {
    let current = *underlying_by_date.get(&date)?;
    let start = date - Duration::days(lookback_days);
    let peak = underlying_by_date
        .range(start..=date)
        .map(|(_, price)| *price)
        .filter(|price| *price > 0.0)
        .max_by(f64::total_cmp)?;
    if current <= 0.0 {
        return None;
    }
    Some((peak - current).max(0.0) / peak)
}

fn underlying_realized_vol(
    date: NaiveDate,
    lookback_days: i64,
    underlying_by_date: &BTreeMap<NaiveDate, f64>,
) -> Option<f64> {
    let start = date - Duration::days(lookback_days);
    let mut returns = Vec::new();
    let mut previous: Option<f64> = None;
    for (_, price) in underlying_by_date.range(start..=date) {
        if *price <= 0.0 {
            continue;
        }
        if let Some(previous_price) = previous {
            returns.push((*price / previous_price).ln());
        }
        previous = Some(*price);
    }
    if returns.len() < 2 {
        return None;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let variance = returns
        .iter()
        .map(|ret| {
            let diff = ret - mean;
            diff * diff
        })
        .sum::<f64>()
        / (returns.len() - 1) as f64;
    Some(variance.sqrt() * 252.0_f64.sqrt())
}

fn simulate_non_overlapping(
    candidates: &[Candidate],
    rows_by_expiration: &BTreeMap<NaiveDate, Vec<OptionDay>>,
    profile: &ResearchProfile,
) -> Vec<ResearchTrade> {
    let lookup = build_lookup(rows_by_expiration);
    let mut by_date: BTreeMap<NaiveDate, Vec<&Candidate>> = BTreeMap::new();
    for candidate in candidates {
        by_date
            .entry(candidate.entry_date)
            .or_default()
            .push(candidate);
    }

    let mut trades = Vec::new();
    let mut next_entry_date = NaiveDate::MIN;
    let mut open_trades = Vec::new();
    for (date, mut day_candidates) in by_date {
        if profile.max_concurrent_positions > 1 {
            retire_closed_trades(date, &mut open_trades, &mut next_entry_date, profile);
        }
        if date < next_entry_date {
            continue;
        }
        if risk_regime_cooldown_triggered(&day_candidates, profile) {
            next_entry_date = next_entry_date_after_risk_regime(date, profile);
            continue;
        }
        if profile.max_concurrent_positions > 1
            && open_trades.len() >= profile.max_concurrent_positions
        {
            continue;
        }
        day_candidates.sort_by(|a, b| candidate_quality_order(a, b, profile));
        for candidate in day_candidates {
            if let Some(trade) = simulate_candidate(candidate, &lookup, profile) {
                if profile.max_concurrent_positions > 1 {
                    next_entry_date = date + Duration::days(profile.min_entry_spacing_days.max(1));
                    open_trades.push(trade.clone());
                } else {
                    next_entry_date = next_entry_date_after_trade(&trade, profile);
                }
                trades.push(trade);
                break;
            }
        }
    }
    trades
}

fn simulate_wheel_non_overlapping(
    candidates: &[Candidate],
    put_rows_by_expiration: &BTreeMap<NaiveDate, Vec<OptionDay>>,
    call_rows_by_expiration: &BTreeMap<NaiveDate, Vec<OptionDay>>,
    profile: &ResearchProfile,
    to: NaiveDate,
) -> Vec<ResearchTrade> {
    let put_lookup = build_lookup(put_rows_by_expiration);
    let call_lookup = build_lookup(call_rows_by_expiration);
    let call_rows_by_date = expiring_rows_by_date(call_rows_by_expiration);
    let call_underlying_by_date = underlying_by_date_from_expirations(call_rows_by_expiration);
    let mut by_date: BTreeMap<NaiveDate, Vec<&Candidate>> = BTreeMap::new();
    for candidate in candidates {
        by_date
            .entry(candidate.entry_date)
            .or_default()
            .push(candidate);
    }

    let mut trades = Vec::new();
    let mut next_entry_date = NaiveDate::MIN;
    for (date, mut day_candidates) in by_date {
        if date < next_entry_date {
            continue;
        }
        if risk_regime_cooldown_triggered(&day_candidates, profile) {
            next_entry_date = next_entry_date_after_risk_regime(date, profile);
            continue;
        }
        day_candidates.sort_by(|a, b| candidate_quality_order(a, b, profile));
        for candidate in day_candidates {
            if let Some(trade) = simulate_wheel_candidate(
                candidate,
                &put_lookup,
                &call_rows_by_date,
                &call_lookup,
                &call_underlying_by_date,
                profile,
                to,
            ) {
                next_entry_date = next_entry_date_after_trade(&trade, profile);
                trades.push(trade);
                break;
            }
        }
    }
    trades
}

fn retire_closed_trades(
    date: NaiveDate,
    open_trades: &mut Vec<ResearchTrade>,
    next_entry_date: &mut NaiveDate,
    profile: &ResearchProfile,
) {
    let mut still_open = Vec::new();
    for trade in open_trades.drain(..) {
        if trade.exit_date < date {
            if trade.exit_reason == "stop_loss" {
                *next_entry_date =
                    (*next_entry_date).max(next_entry_date_after_trade(&trade, profile));
            }
        } else {
            still_open.push(trade);
        }
    }
    *open_trades = still_open;
}

fn risk_regime_cooldown_triggered(
    day_candidates: &[&Candidate],
    profile: &ResearchProfile,
) -> bool {
    let Some(guard) = &profile.risk_regime_cooldown_guard else {
        return false;
    };
    day_candidates.iter().any(|candidate| {
        !guard.allows(
            candidate.underlying_lookback_return,
            candidate.underlying_recent_drawdown,
        )
    })
}

fn next_entry_date_after_risk_regime(date: NaiveDate, profile: &ResearchProfile) -> NaiveDate {
    date + Duration::days(profile.risk_regime_cooldown_days.max(1))
}

fn next_entry_date_after_trade(trade: &ResearchTrade, profile: &ResearchProfile) -> NaiveDate {
    let gap_days = if trade.exit_reason == "stop_loss" {
        profile.stop_loss_cooldown_days.max(1)
    } else {
        1
    };
    trade.exit_date + Duration::days(gap_days)
}

fn build_lookup(
    rows_by_expiration: &BTreeMap<NaiveDate, Vec<OptionDay>>,
) -> HashMap<(NaiveDate, String), BTreeMap<NaiveDate, OptionDay>> {
    let mut out: HashMap<(NaiveDate, String), BTreeMap<NaiveDate, OptionDay>> = HashMap::new();
    for (expiration, rows) in rows_by_expiration {
        for row in rows {
            out.entry((*expiration, format!("{:.3}", row.strike)))
                .or_default()
                .insert(row.date, row.clone());
        }
    }
    out
}

fn candidate_chronological_order(a: &Candidate, b: &Candidate) -> Ordering {
    a.entry_date
        .cmp(&b.entry_date)
        .then_with(|| default_candidate_quality_order(a, b))
}

fn candidate_quality_order(a: &Candidate, b: &Candidate, profile: &ResearchProfile) -> Ordering {
    if profile.prefer_farther_otm {
        return b
            .short_otm_pct
            .total_cmp(&a.short_otm_pct)
            .then_with(|| a.short.delta.abs().total_cmp(&b.short.delta.abs()))
            .then_with(|| default_candidate_quality_order(a, b));
    }
    default_candidate_quality_order(a, b)
}

fn default_candidate_quality_order(a: &Candidate, b: &Candidate) -> Ordering {
    b.return_on_risk
        .total_cmp(&a.return_on_risk)
        .then_with(|| b.credit.total_cmp(&a.credit))
        .then_with(|| a.expiration.cmp(&b.expiration))
        .then_with(|| b.short.strike.total_cmp(&a.short.strike))
        .then_with(|| b.long.strike.total_cmp(&a.long.strike))
        .then_with(|| a.width.total_cmp(&b.width))
}

fn simulate_candidate(
    candidate: &Candidate,
    lookup: &HashMap<(NaiveDate, String), BTreeMap<NaiveDate, OptionDay>>,
    profile: &ResearchProfile,
) -> Option<ResearchTrade> {
    let short_rows = lookup.get(&(
        candidate.expiration,
        format!("{:.3}", candidate.short.strike),
    ))?;
    let long_rows = lookup.get(&(
        candidate.expiration,
        format!("{:.3}", candidate.long.strike),
    ))?;
    match candidate.structure {
        SpreadStructure::PutCreditSpread | SpreadStructure::CallCreditSpread => {
            simulate_put_credit_candidate(candidate, short_rows, long_rows, profile)
        }
        SpreadStructure::PutDebitSpread => {
            simulate_put_debit_candidate(candidate, short_rows, long_rows, profile)
        }
        SpreadStructure::CallDebitSpread => {
            simulate_put_debit_candidate(candidate, short_rows, long_rows, profile)
        }
        SpreadStructure::Wheel => None,
    }
}

fn simulate_wheel_candidate(
    candidate: &Candidate,
    put_lookup: &HashMap<(NaiveDate, String), BTreeMap<NaiveDate, OptionDay>>,
    call_rows_by_date: &BTreeMap<NaiveDate, Vec<ExpiringOptionDay>>,
    call_lookup: &HashMap<(NaiveDate, String), BTreeMap<NaiveDate, OptionDay>>,
    underlying_by_date: &BTreeMap<NaiveDate, f64>,
    profile: &ResearchProfile,
    to: NaiveDate,
) -> Option<ResearchTrade> {
    let put_rows = put_lookup.get(&(
        candidate.expiration,
        format!("{:.3}", candidate.short.strike),
    ))?;
    if profile.take_profit_pct > 0.0 {
        let take_profit_debit = candidate.credit * (1.0 - profile.take_profit_pct);
        for (date, short_put) in put_rows.range((candidate.entry_date + Duration::days(1))..) {
            if *date >= candidate.expiration {
                break;
            }
            if short_put.ask <= take_profit_debit {
                return Some(build_wheel_trade(
                    candidate,
                    *date,
                    candidate.short.strike,
                    candidate.credit,
                    short_put.ask,
                    short_put.underlying_price,
                    None,
                    0.0,
                    0,
                    "put_take_profit",
                ));
            }
        }
    }

    let expiration_put = option_row_on_or_before(put_rows, candidate.expiration)?;
    if expiration_put.underlying_price > candidate.short.strike {
        return Some(build_wheel_trade(
            candidate,
            expiration_put.date,
            candidate.short.strike,
            candidate.credit,
            0.0,
            expiration_put.underlying_price,
            None,
            0.0,
            0,
            "put_expired",
        ));
    }

    let assignment_date = expiration_put.date;
    let assigned_strike = candidate.short.strike;
    let assigned_stock_cost = assigned_strike - candidate.credit;
    let minimum_call_strike =
        assigned_strike * profile.covered_call_min_strike_pct_of_assigned.max(0.0);
    let max_stock_hold_days = profile.max_hold_days.unwrap_or(45).max(1);
    let forced_stock_exit_date = (assignment_date + Duration::days(max_stock_hold_days)).min(to);
    let mut next_call_date = assignment_date + Duration::days(1);
    let mut total_call_credit = 0.0;
    let mut call_count = 0;
    let mut last_call_strike = 0.0;
    let mut last_call_delta = 0.0;
    let mut last_call_oi = 0;
    let mut last_call_iv = 0.0;

    while next_call_date <= forced_stock_exit_date {
        let Some(call) = select_covered_call(
            next_call_date,
            forced_stock_exit_date,
            minimum_call_strike,
            call_rows_by_date,
            profile,
        ) else {
            break;
        };
        let call_rows = call_lookup.get(&(call.expiration, format!("{:.3}", call.row.strike)))?;
        let expiration_call = option_row_on_or_before(call_rows, call.expiration)?;
        total_call_credit += call.row.bid;
        call_count += 1;
        last_call_strike = call.row.strike;
        last_call_delta = call.row.delta;
        last_call_oi = call.row.open_interest;
        last_call_iv = call.row.implied_vol;

        if expiration_call.underlying_price >= call.row.strike {
            let pnl_per_share =
                candidate.credit + total_call_credit + call.row.strike - assigned_strike;
            return Some(
                build_wheel_trade(
                    candidate,
                    expiration_call.date,
                    call.row.strike,
                    candidate.credit + total_call_credit,
                    0.0,
                    expiration_call.underlying_price,
                    Some(pnl_per_share),
                    last_call_delta,
                    last_call_oi,
                    "covered_call_assigned",
                )
                .with_call_details(call_count, last_call_iv),
            );
        }
        next_call_date = expiration_call.date + Duration::days(1);
    }

    let mark_date = forced_stock_exit_date;
    let mark_price = underlying_on_or_before(underlying_by_date, mark_date)
        .or_else(|| Some(expiration_put.underlying_price))?;
    let pnl_per_share = candidate.credit + total_call_credit + mark_price - assigned_strike;
    Some(
        build_wheel_trade(
            candidate,
            mark_date,
            last_call_strike,
            candidate.credit + total_call_credit,
            assigned_stock_cost - mark_price,
            mark_price,
            Some(pnl_per_share),
            last_call_delta,
            last_call_oi,
            if call_count > 0 {
                "stock_marked_after_calls"
            } else {
                "stock_marked_no_call"
            },
        )
        .with_call_details(call_count, last_call_iv),
    )
}

#[derive(Clone, Copy)]
struct CoveredCallSelection<'a> {
    expiration: NaiveDate,
    row: &'a OptionDay,
}

fn select_covered_call<'a>(
    from: NaiveDate,
    to: NaiveDate,
    minimum_call_strike: f64,
    call_rows_by_date: &'a BTreeMap<NaiveDate, Vec<ExpiringOptionDay>>,
    profile: &ResearchProfile,
) -> Option<CoveredCallSelection<'a>> {
    let mut best: Option<CoveredCallSelection<'a>> = None;
    for (_date, rows) in call_rows_by_date.range(from..=to) {
        for expiring_row in rows {
            let row = &expiring_row.row;
            let expiration = expiring_row.expiration;
            let dte = (expiration - row.date).num_days();
            if dte < profile.min_dte || dte > profile.max_dte {
                continue;
            }
            let delta = row.delta.abs();
            if row.strike < minimum_call_strike
                || delta < profile.min_short_delta_abs
                || delta > profile.max_short_delta_abs
                || row.open_interest < profile.min_long_oi
                || !quote_width_allowed(row, profile)
                || !iv_allowed(row, profile)
            {
                continue;
            }
            let candidate = CoveredCallSelection { expiration, row };
            best = match best {
                None => Some(candidate),
                Some(current) => {
                    let order = row
                        .date
                        .cmp(&current.row.date)
                        .then_with(|| expiration.cmp(&current.expiration))
                        .then_with(|| row.bid.total_cmp(&current.row.bid).reverse())
                        .then_with(|| row.strike.total_cmp(&current.row.strike));
                    if order == Ordering::Less {
                        Some(candidate)
                    } else {
                        Some(current)
                    }
                }
            };
        }
    }
    best
}

fn option_row_on_or_before(
    rows: &BTreeMap<NaiveDate, OptionDay>,
    date: NaiveDate,
) -> Option<&OptionDay> {
    rows.range(..=date).next_back().map(|(_, row)| row)
}

fn underlying_on_or_before(
    underlying_by_date: &BTreeMap<NaiveDate, f64>,
    date: NaiveDate,
) -> Option<f64> {
    underlying_by_date
        .range(..=date)
        .next_back()
        .map(|(_, price)| *price)
}

trait WheelTradeDetails {
    fn with_call_details(self, call_count: i64, last_call_iv: f64) -> Self;
}

impl WheelTradeDetails for ResearchTrade {
    fn with_call_details(mut self, call_count: i64, last_call_iv: f64) -> Self {
        self.width = call_count as f64;
        self.long_iv = last_call_iv;
        self
    }
}

fn build_wheel_trade(
    candidate: &Candidate,
    exit_date: NaiveDate,
    covered_call_strike: f64,
    total_credit: f64,
    exit_debit: f64,
    underlying_price: f64,
    explicit_pnl_per_share: Option<f64>,
    call_delta: f64,
    call_oi: u32,
    reason: &str,
) -> ResearchTrade {
    let pnl_per_share = explicit_pnl_per_share.unwrap_or(total_credit - exit_debit);
    let max_loss = candidate.max_loss_per_share * 100.0;
    let pnl = pnl_per_share * 100.0;
    ResearchTrade {
        entry_date: candidate.entry_date,
        exit_date,
        expiration: candidate.expiration,
        dte_entry: (candidate.expiration - candidate.entry_date).num_days(),
        days_held: (exit_date - candidate.entry_date).num_days(),
        short_put: candidate.short.strike,
        long_put: covered_call_strike,
        width: 0.0,
        entry_credit: total_credit,
        exit_debit,
        max_profit: (total_credit.max(pnl_per_share) * 100.0).max(0.0),
        max_loss,
        pnl,
        return_on_risk: pnl / max_loss,
        exit_reason: reason.to_owned(),
        short_delta: candidate.short.delta,
        long_delta: call_delta,
        short_oi: candidate.short.open_interest,
        long_oi: call_oi,
        underlying_price,
        short_otm_pct: candidate.short_otm_pct,
        underlying_lookback_return: candidate.underlying_lookback_return,
        underlying_recent_drawdown: candidate.underlying_recent_drawdown,
        underlying_realized_vol: candidate.underlying_realized_vol,
        short_iv: candidate.short_iv,
        long_iv: 0.0,
    }
}

fn simulate_put_credit_candidate(
    candidate: &Candidate,
    short_rows: &BTreeMap<NaiveDate, OptionDay>,
    long_rows: &BTreeMap<NaiveDate, OptionDay>,
    profile: &ResearchProfile,
) -> Option<ResearchTrade> {
    let take_profit_debit = candidate.credit * (1.0 - profile.take_profit_pct);
    let stop_debit = candidate.credit * profile.stop_loss_multiple;

    for (date, short) in short_rows.range((candidate.entry_date + Duration::days(1))..) {
        let days_held = (*date - candidate.entry_date).num_days();
        let dte = (candidate.expiration - *date).num_days();
        let Some(long) = long_rows.get(date) else {
            continue;
        };
        let debit = conservative_short_spread_exit_debit_f64(short.ask, long.bid, candidate.width);
        let reason = if debit >= stop_debit {
            Some("stop_loss")
        } else if debit <= take_profit_debit {
            Some("take_profit")
        } else if profile
            .max_hold_days
            .is_some_and(|max_days| max_days > 0 && days_held >= max_days)
        {
            Some("max_hold")
        } else if dte <= profile.force_close_dte {
            Some("force_close")
        } else {
            None
        };
        if let Some(exit_reason) = reason {
            return Some(build_trade(candidate, *date, debit, exit_reason));
        }
    }
    None
}

fn simulate_put_debit_candidate(
    candidate: &Candidate,
    short_rows: &BTreeMap<NaiveDate, OptionDay>,
    long_rows: &BTreeMap<NaiveDate, OptionDay>,
    profile: &ResearchProfile,
) -> Option<ResearchTrade> {
    let take_profit_credit =
        candidate.credit + candidate.max_profit_per_share * profile.take_profit_pct;
    let stop_credit = candidate.credit * (1.0 - profile.stop_loss_multiple).max(0.0);

    for (date, short) in short_rows.range((candidate.entry_date + Duration::days(1))..) {
        let days_held = (*date - candidate.entry_date).num_days();
        let dte = (candidate.expiration - *date).num_days();
        let Some(long) = long_rows.get(date) else {
            continue;
        };
        let exit_credit =
            conservative_long_spread_exit_credit_f64(long.bid, short.ask, candidate.width);
        let reason = if exit_credit >= take_profit_credit {
            Some("take_profit")
        } else if exit_credit <= stop_credit {
            Some("stop_loss")
        } else if profile
            .max_hold_days
            .is_some_and(|max_days| max_days > 0 && days_held >= max_days)
        {
            Some("max_hold")
        } else if dte <= profile.force_close_dte {
            Some("force_close")
        } else {
            None
        };
        if let Some(exit_reason) = reason {
            return Some(build_trade(candidate, *date, exit_credit, exit_reason));
        }
    }
    None
}

fn build_trade(
    candidate: &Candidate,
    exit_date: NaiveDate,
    exit_debit: f64,
    reason: &str,
) -> ResearchTrade {
    let (entry_credit, exit_debit, pnl_per_share) = match candidate.structure {
        SpreadStructure::PutCreditSpread | SpreadStructure::CallCreditSpread => {
            (candidate.credit, exit_debit, candidate.credit - exit_debit)
        }
        SpreadStructure::PutDebitSpread => (
            -candidate.credit,
            -exit_debit,
            exit_debit - candidate.credit,
        ),
        SpreadStructure::CallDebitSpread => (
            -candidate.credit,
            -exit_debit,
            exit_debit - candidate.credit,
        ),
        SpreadStructure::Wheel => (candidate.credit, exit_debit, candidate.credit - exit_debit),
    };
    let pnl = pnl_per_share * 100.0;
    let max_profit = candidate.max_profit_per_share * 100.0;
    let max_loss = candidate.max_loss_per_share * 100.0;
    ResearchTrade {
        entry_date: candidate.entry_date,
        exit_date,
        expiration: candidate.expiration,
        dte_entry: (candidate.expiration - candidate.entry_date).num_days(),
        days_held: (exit_date - candidate.entry_date).num_days(),
        short_put: candidate.short.strike,
        long_put: candidate.long.strike,
        width: candidate.width,
        entry_credit,
        exit_debit,
        max_profit,
        max_loss,
        pnl: pnl.clamp(-max_loss, max_profit),
        return_on_risk: pnl.clamp(-max_loss, max_profit) / max_loss,
        exit_reason: reason.to_owned(),
        short_delta: candidate.short.delta,
        long_delta: candidate.long.delta,
        short_oi: candidate.short.open_interest,
        long_oi: candidate.long.open_interest,
        underlying_price: candidate.short.underlying_price,
        short_otm_pct: candidate.short_otm_pct,
        underlying_lookback_return: candidate.underlying_lookback_return,
        underlying_recent_drawdown: candidate.underlying_recent_drawdown,
        underlying_realized_vol: candidate.underlying_realized_vol,
        short_iv: candidate.short_iv,
        long_iv: candidate.long_iv,
    }
}

#[cfg(test)]
fn candidate_order_intent(candidate: &Candidate, symbol: &str) -> Result<OptionOrderIntent> {
    let quantity = 1_u32;
    let strategy = candidate.structure.as_str();
    let limit_price = research_decimal_from_f64(candidate.credit, "candidate credit")?;
    match candidate.structure {
        SpreadStructure::PutCreditSpread => credit_spread_open_intent(
            execution_option_key(
                symbol,
                candidate,
                &candidate.short,
                ExecutionOptionRight::Put,
            )?,
            execution_option_key(
                symbol,
                candidate,
                &candidate.long,
                ExecutionOptionRight::Put,
            )?,
            quantity,
            limit_price,
            strategy,
        )
        .map_err(anyhow::Error::from),
        SpreadStructure::CallCreditSpread => credit_spread_open_intent(
            execution_option_key(
                symbol,
                candidate,
                &candidate.short,
                ExecutionOptionRight::Call,
            )?,
            execution_option_key(
                symbol,
                candidate,
                &candidate.long,
                ExecutionOptionRight::Call,
            )?,
            quantity,
            limit_price,
            strategy,
        )
        .map_err(anyhow::Error::from),
        SpreadStructure::PutDebitSpread => debit_spread_open_intent(
            execution_option_key(
                symbol,
                candidate,
                &candidate.long,
                ExecutionOptionRight::Put,
            )?,
            execution_option_key(
                symbol,
                candidate,
                &candidate.short,
                ExecutionOptionRight::Put,
            )?,
            quantity,
            limit_price,
            strategy,
        )
        .map_err(anyhow::Error::from),
        SpreadStructure::CallDebitSpread => debit_spread_open_intent(
            execution_option_key(
                symbol,
                candidate,
                &candidate.long,
                ExecutionOptionRight::Call,
            )?,
            execution_option_key(
                symbol,
                candidate,
                &candidate.short,
                ExecutionOptionRight::Call,
            )?,
            quantity,
            limit_price,
            strategy,
        )
        .map_err(anyhow::Error::from),
        SpreadStructure::Wheel => cash_secured_put_open_intent(
            execution_option_key(
                symbol,
                candidate,
                &candidate.short,
                ExecutionOptionRight::Put,
            )?,
            quantity,
            limit_price,
            strategy,
        )
        .map_err(anyhow::Error::from),
    }
}

#[cfg(test)]
fn execution_option_key(
    symbol: &str,
    candidate: &Candidate,
    row: &OptionDay,
    right: ExecutionOptionRight,
) -> Result<ExecutionOptionKey> {
    Ok(ExecutionOptionKey::new(
        symbol,
        candidate.expiration,
        research_decimal_from_f64(row.strike, "strike")?,
        right,
    ))
}

#[cfg(test)]
fn research_decimal_from_f64(value: f64, field: &str) -> Result<Decimal> {
    if !value.is_finite() {
        anyhow::bail!("{field} must be finite");
    }
    value
        .to_string()
        .parse::<Decimal>()
        .with_context(|| format!("convert {field} to Decimal"))
}

fn quote_width_allowed(row: &OptionDay, profile: &ResearchProfile) -> bool {
    let mid = (row.bid + row.ask) / 2.0;
    if mid <= 0.0 {
        return false;
    }
    let allowed = (mid * profile.max_quote_width_pct_of_mid).max(profile.max_quote_width_abs);
    (row.ask - row.bid) <= allowed
}

fn iv_allowed(row: &OptionDay, profile: &ResearchProfile) -> bool {
    if row.implied_vol <= 0.0 {
        return profile.min_short_iv.is_none() && profile.max_short_iv.is_none();
    }
    if let Some(min_short_iv) = profile.min_short_iv
        && row.implied_vol < min_short_iv
    {
        return false;
    }
    if let Some(max_short_iv) = profile.max_short_iv
        && row.implied_vol > max_short_iv
    {
        return false;
    }
    true
}

fn iv_skew_allowed(short: &OptionDay, long: &OptionDay, profile: &ResearchProfile) -> bool {
    let Some(min_diff) = profile.min_long_short_iv_diff else {
        return true;
    };
    if short.implied_vol <= 0.0 || long.implied_vol <= 0.0 {
        return false;
    }
    long.implied_vol - short.implied_vol >= min_diff
}

fn width_allowed(width: f64, short_delta_abs: f64, profile: &ResearchProfile) -> bool {
    if width < profile.min_width || width > profile.max_width {
        return false;
    }
    if let (Some(delta_threshold), Some(width_cap)) = (
        profile.low_delta_width_cap_delta_abs,
        profile.low_delta_width_cap,
    ) && short_delta_abs < delta_threshold
        && width > width_cap
    {
        return false;
    }
    true
}

fn metrics(trades: &[ResearchTrade], from: NaiveDate, to: NaiveDate) -> ResearchMetrics {
    metrics_with_min_trades_per_year(trades, from, to, MIN_RANKING_TRADES_PER_YEAR)
}

fn metrics_for_profile(
    trades: &[ResearchTrade],
    from: NaiveDate,
    to: NaiveDate,
    profile: &ResearchProfile,
) -> ResearchMetrics {
    metrics_with_min_trades_per_year(trades, from, to, profile.min_trades_per_year)
}

fn metrics_with_min_trades_per_year(
    trades: &[ResearchTrade],
    from: NaiveDate,
    to: NaiveDate,
    min_trades_per_year: f64,
) -> ResearchMetrics {
    let required_trades = required_trades_for_ranking(from, to, min_trades_per_year);
    if trades.is_empty() {
        return ResearchMetrics {
            trades: 0,
            total_pnl: 0.0,
            total_max_loss: 0.0,
            avg_return_on_risk: 0.0,
            median_return_on_risk: 0.0,
            avg_entry_dte: 0.0,
            median_entry_dte: 0.0,
            win_rate: 0.0,
            profit_factor: 0.0,
            max_drawdown: 0.0,
            avg_days_held: 0.0,
            median_days_held: 0.0,
            trades_per_year: 0.0,
            best_trade_pnl: 0.0,
            worst_trade_pnl: 0.0,
            score: -1_000_000.0,
            robust_score: -1_000_000.0,
            ranking_eligible: false,
            robust_ranking_eligible: false,
            required_trades,
            exit_reasons: BTreeMap::new(),
            yearly: BTreeMap::new(),
            annual_stability: annual_stability_metrics(&BTreeMap::new()),
            chronological: chronological_period_metrics(&[], from, to),
            cost_stress: cost_stress_metrics(&[]),
        };
    }
    let mut sorted = trades.to_vec();
    sorted.sort_by(trade_chronological_order);
    let total_pnl = sorted.iter().map(|trade| trade.pnl).sum::<f64>();
    let total_max_loss = sorted.iter().map(|trade| trade.max_loss).sum::<f64>();
    let wins = sorted.iter().filter(|trade| trade.pnl > 0.0).count();
    let gross_profit = sorted
        .iter()
        .filter(|trade| trade.pnl > 0.0)
        .map(|trade| trade.pnl)
        .sum::<f64>();
    let gross_loss = sorted
        .iter()
        .filter(|trade| trade.pnl < 0.0)
        .map(|trade| trade.pnl.abs())
        .sum::<f64>();
    let returns = sorted
        .iter()
        .map(|trade| trade.return_on_risk)
        .collect::<Vec<_>>();
    let days_held = sorted
        .iter()
        .map(|trade| trade.days_held as f64)
        .collect::<Vec<_>>();
    let entry_dtes = sorted
        .iter()
        .map(|trade| trade.dte_entry as f64)
        .collect::<Vec<_>>();
    let max_drawdown = max_drawdown(&sorted);
    let years = ((to - from).num_days().max(1) as f64) / 365.25;
    let ranking_eligible = sorted.len() >= required_trades;
    let score = if ranking_eligible {
        total_pnl / total_max_loss.max(1.0) - 2.0 * max_drawdown
    } else {
        -1_000_000.0 + sorted.len() as f64 / required_trades as f64
    };
    let chronological = chronological_period_metrics(&sorted, from, to);
    let robust_score = robust_score(score, &chronological);
    let robust_ranking_eligible =
        robust_ranking_eligible(ranking_eligible, total_pnl, &chronological);
    let yearly = yearly_metrics(&sorted);
    let annual_stability = annual_stability_metrics(&yearly);
    ResearchMetrics {
        trades: sorted.len(),
        total_pnl,
        total_max_loss,
        avg_return_on_risk: mean(&returns),
        median_return_on_risk: median(returns),
        avg_entry_dte: mean(&entry_dtes),
        median_entry_dte: median(entry_dtes),
        win_rate: wins as f64 / sorted.len() as f64,
        profit_factor: if gross_loss == 0.0 {
            gross_profit
        } else {
            gross_profit / gross_loss
        },
        max_drawdown,
        avg_days_held: mean(&days_held),
        median_days_held: median(days_held),
        trades_per_year: sorted.len() as f64 / years,
        best_trade_pnl: sorted
            .iter()
            .map(|trade| trade.pnl)
            .fold(f64::MIN, f64::max),
        worst_trade_pnl: sorted
            .iter()
            .map(|trade| trade.pnl)
            .fold(f64::MAX, f64::min),
        score,
        robust_score,
        ranking_eligible,
        robust_ranking_eligible,
        required_trades,
        exit_reasons: exit_reasons(&sorted),
        yearly,
        annual_stability,
        chronological,
        cost_stress: cost_stress_metrics(&sorted),
    }
}

fn required_trades_for_ranking(from: NaiveDate, to: NaiveDate, min_trades_per_year: f64) -> usize {
    MIN_RANKING_TRADES.max(required_period_trades_for_ranking(
        from,
        to,
        min_trades_per_year,
    ))
}

fn required_period_trades_for_ranking(
    from: NaiveDate,
    to: NaiveDate,
    min_trades_per_year: f64,
) -> usize {
    let years = ((to - from).num_days().max(1) as f64) / 365.25;
    ((years * min_trades_per_year).ceil() as usize).max(1)
}

fn trade_chronological_order(a: &ResearchTrade, b: &ResearchTrade) -> Ordering {
    a.exit_date
        .cmp(&b.exit_date)
        .then_with(|| a.entry_date.cmp(&b.entry_date))
        .then_with(|| a.expiration.cmp(&b.expiration))
        .then_with(|| b.short_put.total_cmp(&a.short_put))
        .then_with(|| b.long_put.total_cmp(&a.long_put))
}

fn max_drawdown(trades: &[ResearchTrade]) -> f64 {
    let mut equity = 0.0;
    let mut high_water = 0.0;
    let mut drawdown = 0.0;
    let risk = trades
        .iter()
        .map(|trade| trade.max_loss)
        .sum::<f64>()
        .max(1.0);
    for trade in trades {
        equity += trade.pnl;
        if equity > high_water {
            high_water = equity;
        }
        let current = high_water - equity;
        if current > drawdown {
            drawdown = current;
        }
    }
    drawdown / risk
}

fn cost_stress_metrics(trades: &[ResearchTrade]) -> Vec<CostStressMetrics> {
    COST_STRESS_PER_TRADE
        .into_iter()
        .map(|per_trade_cost| cost_stress_metric(trades, per_trade_cost))
        .collect()
}

fn cost_stress_metric(trades: &[ResearchTrade], per_trade_cost: f64) -> CostStressMetrics {
    let mut sorted = trades.to_vec();
    sorted.sort_by(trade_chronological_order);
    if sorted.is_empty() {
        return CostStressMetrics {
            per_trade_cost,
            trades: 0,
            total_pnl: 0.0,
            avg_return_on_risk: 0.0,
            win_rate: 0.0,
            profit_factor: 0.0,
            max_drawdown: 0.0,
            score: -1_000_000.0,
        };
    }

    let adjusted_pnls = sorted
        .iter()
        .map(|trade| trade.pnl - per_trade_cost)
        .collect::<Vec<_>>();
    let total_pnl = adjusted_pnls.iter().sum::<f64>();
    let total_max_loss = sorted.iter().map(|trade| trade.max_loss).sum::<f64>();
    let wins = adjusted_pnls.iter().filter(|pnl| **pnl > 0.0).count();
    let gross_profit = adjusted_pnls.iter().filter(|pnl| **pnl > 0.0).sum::<f64>();
    let gross_loss = adjusted_pnls
        .iter()
        .filter(|pnl| **pnl < 0.0)
        .map(|pnl| pnl.abs())
        .sum::<f64>();
    let returns = sorted
        .iter()
        .zip(adjusted_pnls.iter())
        .map(|(trade, pnl)| pnl / trade.max_loss)
        .collect::<Vec<_>>();
    let max_drawdown = adjusted_max_drawdown(&sorted, per_trade_cost);
    let score = total_pnl / total_max_loss.max(1.0) - 2.0 * max_drawdown;

    CostStressMetrics {
        per_trade_cost,
        trades: sorted.len(),
        total_pnl,
        avg_return_on_risk: mean(&returns),
        win_rate: wins as f64 / sorted.len() as f64,
        profit_factor: if gross_loss == 0.0 {
            gross_profit
        } else {
            gross_profit / gross_loss
        },
        max_drawdown,
        score,
    }
}

fn adjusted_max_drawdown(trades: &[ResearchTrade], per_trade_cost: f64) -> f64 {
    let mut equity = 0.0;
    let mut high_water = 0.0;
    let mut drawdown = 0.0;
    let risk = trades
        .iter()
        .map(|trade| trade.max_loss)
        .sum::<f64>()
        .max(1.0);
    for trade in trades {
        equity += trade.pnl - per_trade_cost;
        if equity > high_water {
            high_water = equity;
        }
        let current = high_water - equity;
        if current > drawdown {
            drawdown = current;
        }
    }
    drawdown / risk
}

fn robust_score(score: f64, periods: &[PeriodMetrics]) -> f64 {
    periods
        .iter()
        .map(|period| period.score)
        .fold(score, f64::min)
}

fn robust_ranking_eligible(
    ranking_eligible: bool,
    total_pnl: f64,
    periods: &[PeriodMetrics],
) -> bool {
    ranking_eligible
        && total_pnl > 0.0
        && periods
            .iter()
            .all(|period| period.ranking_eligible && period.total_pnl > 0.0)
}

fn chronological_period_metrics(
    trades: &[ResearchTrade],
    from: NaiveDate,
    to: NaiveDate,
) -> Vec<PeriodMetrics> {
    let days = (to - from).num_days();
    if days < 2 {
        return vec![period_metrics("full_window", trades, from, to)];
    }
    let split = from + Duration::days(days / 2);
    let periods = [
        ("first_half", from, split),
        ("second_half", split + Duration::days(1), to),
    ];

    periods
        .into_iter()
        .filter(|(_, start, end)| start <= end)
        .map(|(name, start, end)| {
            let period_trades = trades
                .iter()
                .filter(|trade| trade.entry_date >= start && trade.entry_date <= end)
                .cloned()
                .collect::<Vec<_>>();
            period_metrics(name, &period_trades, start, end)
        })
        .collect()
}

fn period_metrics(
    name: &str,
    trades: &[ResearchTrade],
    from: NaiveDate,
    to: NaiveDate,
) -> PeriodMetrics {
    let mut sorted = trades.to_vec();
    sorted.sort_by(trade_chronological_order);
    let required_trades = required_period_trades_for_ranking(from, to, MIN_RANKING_TRADES_PER_YEAR);
    if sorted.is_empty() {
        return PeriodMetrics {
            name: name.to_owned(),
            from,
            to,
            trades: 0,
            total_pnl: 0.0,
            avg_return_on_risk: 0.0,
            win_rate: 0.0,
            profit_factor: 0.0,
            max_drawdown: 0.0,
            score: -1_000_000.0,
            ranking_eligible: false,
            required_trades,
        };
    }

    let total_pnl = sorted.iter().map(|trade| trade.pnl).sum::<f64>();
    let total_max_loss = sorted.iter().map(|trade| trade.max_loss).sum::<f64>();
    let wins = sorted.iter().filter(|trade| trade.pnl > 0.0).count();
    let gross_profit = sorted
        .iter()
        .filter(|trade| trade.pnl > 0.0)
        .map(|trade| trade.pnl)
        .sum::<f64>();
    let gross_loss = sorted
        .iter()
        .filter(|trade| trade.pnl < 0.0)
        .map(|trade| trade.pnl.abs())
        .sum::<f64>();
    let returns = sorted
        .iter()
        .map(|trade| trade.return_on_risk)
        .collect::<Vec<_>>();
    let max_drawdown = max_drawdown(&sorted);
    let ranking_eligible = sorted.len() >= required_trades;
    let score = if ranking_eligible {
        total_pnl / total_max_loss.max(1.0) - 2.0 * max_drawdown
    } else {
        -1_000_000.0 + sorted.len() as f64 / required_trades as f64
    };

    PeriodMetrics {
        name: name.to_owned(),
        from,
        to,
        trades: sorted.len(),
        total_pnl,
        avg_return_on_risk: mean(&returns),
        win_rate: wins as f64 / sorted.len() as f64,
        profit_factor: if gross_loss == 0.0 {
            gross_profit
        } else {
            gross_profit / gross_loss
        },
        max_drawdown,
        score,
        ranking_eligible,
        required_trades,
    }
}

fn yearly_metrics(trades: &[ResearchTrade]) -> BTreeMap<i32, YearMetrics> {
    let mut grouped: BTreeMap<i32, Vec<&ResearchTrade>> = BTreeMap::new();
    for trade in trades {
        grouped
            .entry(trade.entry_date.year())
            .or_default()
            .push(trade);
    }
    grouped
        .into_iter()
        .map(|(year, rows)| {
            let pnl = rows.iter().map(|trade| trade.pnl).sum::<f64>();
            let wins = rows.iter().filter(|trade| trade.pnl > 0.0).count();
            let avg_return_on_risk =
                rows.iter().map(|trade| trade.return_on_risk).sum::<f64>() / rows.len() as f64;
            (
                year,
                YearMetrics {
                    trades: rows.len(),
                    pnl,
                    win_rate: wins as f64 / rows.len() as f64,
                    avg_return_on_risk,
                },
            )
        })
        .collect()
}

fn annual_stability_metrics(yearly: &BTreeMap<i32, YearMetrics>) -> AnnualStabilityMetrics {
    if yearly.is_empty() {
        return AnnualStabilityMetrics {
            active_years: 0,
            positive_years: 0,
            negative_years: 0,
            positive_year_rate: 0.0,
            worst_year: None,
            worst_year_pnl: 0.0,
            worst_year_avg_return_on_risk: 0.0,
            best_year: None,
            best_year_pnl: 0.0,
        };
    }

    let positive_years = yearly.values().filter(|year| year.pnl > 0.0).count();
    let negative_years = yearly.values().filter(|year| year.pnl < 0.0).count();
    let (worst_year, worst_metrics) = yearly
        .iter()
        .min_by(|(_, a), (_, b)| a.pnl.total_cmp(&b.pnl))
        .expect("non-empty yearly metrics");
    let (best_year, best_metrics) = yearly
        .iter()
        .max_by(|(_, a), (_, b)| a.pnl.total_cmp(&b.pnl))
        .expect("non-empty yearly metrics");

    AnnualStabilityMetrics {
        active_years: yearly.len(),
        positive_years,
        negative_years,
        positive_year_rate: positive_years as f64 / yearly.len() as f64,
        worst_year: Some(*worst_year),
        worst_year_pnl: worst_metrics.pnl,
        worst_year_avg_return_on_risk: worst_metrics.avg_return_on_risk,
        best_year: Some(*best_year),
        best_year_pnl: best_metrics.pnl,
    }
}

fn exit_reasons(trades: &[ResearchTrade]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for trade in trades {
        *counts.entry(trade.exit_reason.clone()).or_insert(0) += 1;
    }
    counts
}

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

fn median(mut values: Vec<f64>) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(f64::total_cmp);
    let mid = values.len() / 2;
    if values.len().is_multiple_of(2) {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    }
}

fn plateau_expansion_command(report: &ResearchReport) -> Option<String> {
    if !report.plateau_status.expansion_ready {
        return None;
    }

    Some(format!(
        "cargo run --release -- research-universe --plateau-run runs/{}/research.json --symbols {} --from {} --to {}",
        report.run_id, DEFAULT_PLATEAU_UNIVERSE_SYMBOLS_CSV, report.requested_from, report.to
    ))
}

fn research_markdown(report: &ResearchReport) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "# SpreadFoundry {} Research {}\n\n",
        report.symbol, report.run_id
    ));
    out.push_str(&format!(
        "- Profile family: `{}`\n- Requested window: `{}` to `{}`\n- Effective research window: `{}` to `{}`\n- Expirations discovered: `{}`\n- Expirations skipped before data: `{}`\n- Expirations loaded: `{}`\n- Expirations failed: `{}`\n- EOD rows loaded: `{}`\n- Ranking gate: profiles need at least `{}` trades for this window\n\n",
        report.profile_family.as_str(),
        report.requested_from,
        report.to,
        report.from,
        report.to,
        report.expirations_discovered,
        report.expirations_skipped_before_data,
        report.expirations_loaded,
        report.expirations_failed,
        report.rows_loaded,
        report
            .profiles
            .first()
            .map(|result| result.metrics.required_trades)
            .unwrap_or(MIN_RANKING_TRADES)
    ));
    if !report.expiration_load_failures.is_empty() {
        out.push_str("## Expiration Load Failures\n\n");
        out.push_str("| Expiration | Error |\n");
        out.push_str("|---|---|\n");
        for failure in report.expiration_load_failures.iter().take(25) {
            out.push_str(&format!(
                "| {} | {} |\n",
                failure.expiration,
                markdown_cell(&failure.message)
            ));
        }
        if report.expiration_load_failures.len() > 25 {
            out.push_str(&format!(
                "\n_{} additional expiration failures omitted from this Markdown table; see `research.json` for the full list._\n",
                report.expiration_load_failures.len() - 25
            ));
        }
        out.push('\n');
    }

    let gate = &report.deployment_gate;
    out.push_str("## Research Deployment Gate\n\n");
    out.push_str(&format!(
        "- Status: `{}`\n- Best-profile robust gate: `{}`\n- Walk-forward OOS gate: `{}` (trades `{}`/`{}`, PnL `{:.2}`, score `{:.4}`)\n- Holdout OOS gate: `{}` (trades `{}`/`{}`, PnL `{:.2}`, score `{:.4}`)\n",
        gate.status,
        format_gate(gate.best_profile_gate),
        format_gate(gate.walk_forward_oos_gate),
        report.walk_forward.metrics.trades,
        report.walk_forward.metrics.required_trades,
        report.walk_forward.metrics.total_pnl,
        report.walk_forward.metrics.score,
        format_gate(gate.holdout_oos_gate),
        report.holdout.metrics.trades,
        report.holdout.metrics.required_trades,
        report.holdout.metrics.total_pnl,
        report.holdout.metrics.score
    ));
    if !gate.pass {
        out.push_str(
            "- Interpretation: latest signals are research candidates only until out-of-sample gates are positive.\n\n",
        );
    } else {
        out.push('\n');
    }

    out.push_str("## Professional Options Review\n\n");
    for line in professional_options_review(report) {
        out.push_str(&format!("- {}\n", line));
    }
    out.push('\n');

    let inactive_train_edge_years = report
        .walk_forward
        .years
        .iter()
        .filter(|year| !year.active && !year.train_metrics.robust_score_gate)
        .count();
    let inactive_recent_activity_years = report
        .walk_forward
        .years
        .iter()
        .filter(|year| !year.active && !year.train_metrics.recent_activity_gate)
        .count();
    let active_zero_trade_years = report
        .walk_forward
        .years
        .iter()
        .filter(|year| year.active && year.test_metrics.trades == 0)
        .count();
    let active_negative_pnl_years = report
        .walk_forward
        .years
        .iter()
        .filter(|year| year.active && year.test_metrics.total_pnl < 0.0)
        .count();
    out.push_str("## Out-of-Sample Failure Summary\n\n");
    out.push_str(&format!(
        "- Inactive walk-forward years from weak train edge: `{}`\n- Inactive walk-forward years from stale train activity: `{}`\n- Active walk-forward years with zero OOS trades: `{}`\n- Active walk-forward years with negative OOS PnL: `{}`\n- Holdout active: `{}`\n- Holdout train edge gate: `{}`\n- Holdout recent activity gate: `{}`\n\n",
        inactive_train_edge_years,
        inactive_recent_activity_years,
        active_zero_trade_years,
        active_negative_pnl_years,
        if report.holdout.active { "yes" } else { "no" },
        format_gate(report.holdout.train_metrics.robust_score_gate),
        format_gate(report.holdout.train_metrics.recent_activity_gate)
    ));

    if let Some(best) = report.profiles.first() {
        let baseline_name = ResearchProfile::baseline().name;
        if let Some(baseline) = report
            .profiles
            .iter()
            .find(|result| result.profile.name == baseline_name)
        {
            let best_metrics = &best.metrics;
            let baseline_metrics = &baseline.metrics;
            out.push_str("## Baseline Comparison\n\n");
            out.push_str("| Profile | Trades | Trades/Yr | PnL | Avg ROR | Win Rate | Profit Factor | Max DD | Score | Robust Score |\n");
            out.push_str("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
            out.push_str(&format!(
                "| Baseline | {} | {:.1} | {:.2} | {:.3} | {:.1}% | {:.2} | {:.3} | {:.4} | {:.4} |\n",
                baseline_metrics.trades,
                baseline_metrics.trades_per_year,
                baseline_metrics.total_pnl,
                baseline_metrics.avg_return_on_risk,
                baseline_metrics.win_rate * 100.0,
                baseline_metrics.profit_factor,
                baseline_metrics.max_drawdown,
                baseline_metrics.score,
                baseline_metrics.robust_score
            ));
            out.push_str(&format!(
                "| Best | {} | {:.1} | {:.2} | {:.3} | {:.1}% | {:.2} | {:.3} | {:.4} | {:.4} |\n",
                best_metrics.trades,
                best_metrics.trades_per_year,
                best_metrics.total_pnl,
                best_metrics.avg_return_on_risk,
                best_metrics.win_rate * 100.0,
                best_metrics.profit_factor,
                best_metrics.max_drawdown,
                best_metrics.score,
                best_metrics.robust_score
            ));
            out.push_str(&format!(
                "| Delta | {} | {:.1} | {:.2} | {:.3} | {:.1}% | {:.2} | {:.3} | {:.4} | {:.4} |\n\n",
                best_metrics.trades as i64 - baseline_metrics.trades as i64,
                best_metrics.trades_per_year - baseline_metrics.trades_per_year,
                best_metrics.total_pnl - baseline_metrics.total_pnl,
                best_metrics.avg_return_on_risk - baseline_metrics.avg_return_on_risk,
                (best_metrics.win_rate - baseline_metrics.win_rate) * 100.0,
                best_metrics.profit_factor - baseline_metrics.profit_factor,
                best_metrics.max_drawdown - baseline_metrics.max_drawdown,
                best_metrics.score - baseline_metrics.score,
                best_metrics.robust_score - baseline_metrics.robust_score
            ));
        }

        out.push_str("## Detector Robustness Gap\n\n");
        let best_metrics = &best.metrics;
        let robust_gap = MIN_DEPLOYABLE_TRAINING_ROBUST_SCORE - best_metrics.robust_score;
        let weakest_period = best_metrics
            .chronological
            .iter()
            .min_by(|a, b| a.score.total_cmp(&b.score));
        out.push_str(&format!(
            "- Best robust score: `{:.4}`\n- Required deployable robust score: `{:.4}`\n- Robust score gap: `{:.4}`\n",
            best_metrics.robust_score,
            MIN_DEPLOYABLE_TRAINING_ROBUST_SCORE,
            robust_gap.max(0.0)
        ));
        if let Some(period) = weakest_period {
            out.push_str(&format!(
                "- Weakest chronological period: `{}` (`{}` to `{}`, score `{:.4}`, trades `{}`)\n",
                period.name, period.from, period.to, period.score, period.trades
            ));
        }
        out.push_str("\n| Rank | Profile | Trades | PnL | Score | Robust Score | Gap | Weakest Period | Worst Year |\n");
        out.push_str("|---:|---|---:|---:|---:|---:|---:|---|---|\n");
        for (idx, result) in report.profiles.iter().take(5).enumerate() {
            let m = &result.metrics;
            let weakest = m
                .chronological
                .iter()
                .min_by(|a, b| a.score.total_cmp(&b.score));
            out.push_str(&format!(
                "| {} | {} | {} | {:.2} | {:.4} | {:.4} | {:.4} | {} | {} |\n",
                idx + 1,
                result.profile.name,
                m.trades,
                m.total_pnl,
                m.score,
                m.robust_score,
                (MIN_DEPLOYABLE_TRAINING_ROBUST_SCORE - m.robust_score).max(0.0),
                weakest
                    .map(|period| format!("{} {:.4}", period.name, period.score))
                    .unwrap_or_else(|| "n/a".to_owned()),
                format_optional_year_pnl(
                    m.annual_stability.worst_year,
                    m.annual_stability.worst_year_pnl
                )
            ));
        }
        out.push('\n');
    }

    let plateau = &report.plateau_status;
    out.push_str("## Research Plateau Status\n\n");
    out.push_str(&format!(
        "- Status: `{}`\n- Expansion ready: `{}`\n- Profiles evaluated: `{}` (`{}` variants; plateau minimum `{}`)\n- Walk-forward years: `{}` (plateau minimum `{}`)\n- Detector status: `{}`\n- Execution strategy status: `{}`\n- Reason: {}\n- Next action: {}\n\n",
        plateau.status,
        format_gate(plateau.expansion_ready),
        plateau.profiles_evaluated,
        plateau.profile_variants_evaluated,
        plateau.min_profile_variants,
        plateau.walk_forward_years,
        plateau.min_walk_forward_years,
        plateau.detector_status,
        plateau.execution_strategy_status,
        plateau.reason,
        plateau.next_action
    ));

    out.push_str("## Planned Universe Expansion\n\n");
    let default_universe_symbols = match report.profile_family {
        ResearchProfileFamily::Swing => DEFAULT_PLATEAU_UNIVERSE_SYMBOLS_CSV,
        ResearchProfileFamily::Weekly
        | ResearchProfileFamily::WeeklyFarOtm
        | ResearchProfileFamily::WeeklyPutDebit
        | ResearchProfileFamily::WeeklyCallCredit
        | ResearchProfileFamily::WeeklyCallDebit
        | ResearchProfileFamily::WeeklyWheel => DEFAULT_WEEKLY_RESEARCH_SYMBOLS_CSV,
    };
    let universe_method = match report.profile_family {
        ResearchProfileFamily::Swing => {
            "run the same Rust put-spread research grid per symbol, with detector filters and execution rules reported as separate strategies"
        }
        ResearchProfileFamily::Weekly => {
            "run the weekly 1-14 DTE put-spread research grid per symbol, with cadence, detector filters, and execution rules reported as separate strategies"
        }
        ResearchProfileFamily::WeeklyFarOtm => {
            "run the far-OTM weekly 1-14 DTE put-spread research grid per symbol, with lower-delta entries, tighter stops, cadence, detector filters, and execution rules reported as separate strategies"
        }
        ResearchProfileFamily::WeeklyPutDebit => {
            "run the weekly 1-14 DTE put-debit-spread research grid per symbol, with bought-put delta bands, debit caps, cadence, detector filters, and execution rules reported as separate strategies"
        }
        ResearchProfileFamily::WeeklyCallCredit => {
            "run the weekly 1-14 DTE call-credit-spread research grid per symbol, with short-call delta bands, weak/overbought gates, cadence, detector filters, and execution rules reported as separate strategies"
        }
        ResearchProfileFamily::WeeklyCallDebit => {
            "run the weekly 1-14 DTE call-debit-spread research grid per symbol, with bought-call delta bands, debit caps, bullish trend/volatility gates, cadence, detector filters, and execution rules reported as separate strategies"
        }
        ResearchProfileFamily::WeeklyWheel => {
            "run the weekly 1-14 DTE wheel research grid per symbol, with cash-secured put entries, assignment-aware stock inventory, covered-call exits, cadence, detector filters, and execution rules reported as separate strategies"
        }
    };
    out.push_str(&format!(
        "- Trigger: plateau status must be `plateau_expand_universe` with `expansion_ready=true`.\n- Default universe symbols: `{}`\n- Method: {}.\n- Current state: `{}`\n\n",
        default_universe_symbols,
        universe_method,
        if plateau.expansion_ready {
            "unlocked"
        } else {
            "locked"
        }
    ));
    if let Some(command) = plateau_expansion_command(report) {
        out.push_str(&format!("- Universe research command: `{}`\n\n", command));
    }

    if let Some(signal) = &report.latest_signal {
        out.push_str("## Latest Signal\n\n");
        out.push_str(&format!(
            "- As of: `{}`\n- Status: `{}`\n- Research deployment gate: `{}`\n- Profile: `{}`\n- Entry date: `{}`\n- Expiration: `{}`\n- Entry DTE: `{}`\n- Spread: `{:.0}P/{:.0}P`\n- Width: `{:.2}`\n- Credit: `{:.2}`\n- Max profit: `{:.2}`\n- Max loss: `{:.2}`\n- Return on risk: `{:.3}`\n- Underlying: `{:.2}`\n- Short OTM: `{:.1}%`\n- Short delta: `{:.3}`\n- Short IV: `{:.1}%`\n- Trend return: `{}`\n- Recent drawdown: `{}`\n- Realized vol: `{}`\n\n",
            signal.as_of,
            signal.status,
            gate.status,
            signal.profile_name,
            signal.entry_date,
            signal.expiration,
            signal.dte_entry,
            signal.short_put,
            signal.long_put,
            signal.width,
            signal.entry_credit,
            signal.max_profit,
            signal.max_loss,
            signal.return_on_risk,
            signal.underlying_price,
            signal.short_otm_pct * 100.0,
            signal.short_delta,
            signal.short_iv * 100.0,
            format_optional_pct(signal.underlying_lookback_return),
            format_optional_pct(signal.underlying_recent_drawdown),
            format_optional_pct(signal.underlying_realized_vol)
        ));
    } else {
        out.push_str("## Latest Signal\n\n");
        out.push_str(
            "- No deployable current entry signal for the best ranked profile inside this data window.\n\n",
        );
    }

    let wf = &report.walk_forward;
    let active_years = wf.years.iter().filter(|year| year.active).count();
    out.push_str("## Walk-Forward Selector\n\n");
    out.push_str(&format!(
        "- Minimum training window: `{}` days\n- OOS years: `{}`\n- Active OOS years: `{}`\n- OOS trades: `{}` / required `{}`\n- OOS PnL: `{:.2}`\n- OOS win rate: `{:.1}%`\n- OOS profit factor: `{:.2}`\n- OOS max DD: `{:.3}`\n- OOS score: `{:.4}`\n- Selected profiles: `{}`\n\n",
        wf.min_train_days,
        wf.years.len(),
        active_years,
        wf.metrics.trades,
        wf.metrics.required_trades,
        wf.metrics.total_pnl,
        wf.metrics.win_rate * 100.0,
        wf.metrics.profit_factor,
        wf.metrics.max_drawdown,
        wf.metrics.score,
        format_profile_counts(&wf.selected_profile_counts)
    ));
    out.push_str("| Test Year | Train Window | Test Window | Active | Selected Profile | Train Robust Eligible | Train Edge Gate | Train Trades | Recent 365D Trades | Last Train Entry | Days Since Last Entry | Train PnL | Train Robust Score | OOS Trades | OOS PnL | OOS Win Rate | OOS Profit Factor | OOS Score |\n");
    out.push_str(
        "|---:|---|---|---:|---|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|\n",
    );
    for year in &wf.years {
        out.push_str(&format!(
            "| {} | {} to {} | {} to {} | {} | {} | {} | {} | {} | {} | {} | {} | {:.2} | {:.4} | {} | {:.2} | {:.1}% | {:.2} | {:.4} |\n",
            year.test_year,
            year.train_from,
            year.train_to,
            year.test_from,
            year.test_to,
            if year.active { "yes" } else { "no" },
            year.selected_profile,
            if year.train_metrics.robust_ranking_eligible {
                "yes"
            } else {
                "no"
            },
            if year.train_metrics.robust_score_gate {
                "yes"
            } else {
                "no"
            },
            year.train_metrics.trades,
            year.train_metrics.recent_trades,
            format_optional_date(year.train_metrics.last_entry_date),
            format_optional_i64(year.train_metrics.days_since_last_entry),
            year.train_metrics.total_pnl,
            year.train_metrics.robust_score,
            year.test_metrics.trades,
            year.test_metrics.total_pnl,
            year.test_metrics.win_rate * 100.0,
            year.test_metrics.profit_factor,
            year.test_metrics.score
        ));
    }
    out.push('\n');

    let rolling = &report.rolling_walk_forward;
    let rolling_active_years = rolling.years.iter().filter(|year| year.active).count();
    out.push_str("## Rolling Walk-Forward Selector\n\n");
    out.push_str(&format!(
        "- Training window: `{}` days\n- OOS years: `{}`\n- Active OOS years: `{}`\n- OOS trades: `{}` / required `{}`\n- OOS PnL: `{:.2}`\n- OOS win rate: `{:.1}%`\n- OOS profit factor: `{:.2}`\n- OOS max DD: `{:.3}`\n- OOS score: `{:.4}`\n- Selected profiles: `{}`\n\n",
        rolling.train_window_days.unwrap_or(rolling.min_train_days),
        rolling.years.len(),
        rolling_active_years,
        rolling.metrics.trades,
        rolling.metrics.required_trades,
        rolling.metrics.total_pnl,
        rolling.metrics.win_rate * 100.0,
        rolling.metrics.profit_factor,
        rolling.metrics.max_drawdown,
        rolling.metrics.score,
        format_profile_counts(&rolling.selected_profile_counts)
    ));
    out.push_str("| Test Year | Train Window | Test Window | Active | Selected Profile | Train Robust Eligible | Train Edge Gate | Train Trades | Recent 365D Trades | Last Train Entry | Days Since Last Entry | Train PnL | Train Robust Score | OOS Trades | OOS PnL | OOS Win Rate | OOS Profit Factor | OOS Score |\n");
    out.push_str(
        "|---:|---|---|---:|---|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|\n",
    );
    for year in &rolling.years {
        out.push_str(&format!(
            "| {} | {} to {} | {} to {} | {} | {} | {} | {} | {} | {} | {} | {} | {:.2} | {:.4} | {} | {:.2} | {:.1}% | {:.2} | {:.4} |\n",
            year.test_year,
            year.train_from,
            year.train_to,
            year.test_from,
            year.test_to,
            if year.active { "yes" } else { "no" },
            year.selected_profile,
            if year.train_metrics.robust_ranking_eligible {
                "yes"
            } else {
                "no"
            },
            if year.train_metrics.robust_score_gate {
                "yes"
            } else {
                "no"
            },
            year.train_metrics.trades,
            year.train_metrics.recent_trades,
            format_optional_date(year.train_metrics.last_entry_date),
            format_optional_i64(year.train_metrics.days_since_last_entry),
            year.train_metrics.total_pnl,
            year.train_metrics.robust_score,
            year.test_metrics.trades,
            year.test_metrics.total_pnl,
            year.test_metrics.win_rate * 100.0,
            year.test_metrics.profit_factor,
            year.test_metrics.score
        ));
    }
    out.push('\n');

    out.push_str("## Walk-Forward Selection Diagnostics\n\n");
    out.push_str("| Test Year | Rank | Active | Profile | Train Robust Eligible | Train Edge Gate | Train Trades | Recent 365D Trades | Last Train Entry | Days Since Last Entry | Train PnL | Train Robust Score | OOS Trades | OOS PnL | OOS Score |\n");
    out.push_str("|---:|---:|---:|---|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|\n");
    for year in &wf.years {
        for candidate in &year.selection_candidates {
            out.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {:.2} | {:.4} | {} | {:.2} | {:.4} |\n",
                year.test_year,
                candidate.rank,
                if candidate.active { "yes" } else { "no" },
                candidate.profile,
                if candidate.train_metrics.robust_ranking_eligible {
                    "yes"
                } else {
                    "no"
                },
                if candidate.train_metrics.robust_score_gate {
                    "yes"
                } else {
                    "no"
                },
                candidate.train_metrics.trades,
                candidate.train_metrics.recent_trades,
                format_optional_date(candidate.train_metrics.last_entry_date),
                format_optional_i64(candidate.train_metrics.days_since_last_entry),
                candidate.train_metrics.total_pnl,
                candidate.train_metrics.robust_score,
                candidate.test_metrics.trades,
                candidate.test_metrics.total_pnl,
                candidate.test_metrics.score
            ));
        }
    }
    out.push('\n');

    let holdout = &report.holdout;
    out.push_str("## Half-Window Holdout Selector\n\n");
    out.push_str(&format!(
        "- Train window: `{}` to `{}`\n- Test window: `{}` to `{}`\n- Active: `{}`\n- Selected profile: `{}`\n- Train robust eligible: `{}`\n- Train robust score gate: `{}` (min `{:.4}`)\n- Train trades: `{}`\n- Recent 365D train trades: `{}`\n- Last train entry: `{}`\n- Days since last train entry: `{}`\n- Train PnL: `{:.2}`\n- Train robust score: `{:.4}`\n- Test trades: `{}` / required `{}`\n- Test PnL: `{:.2}`\n- Test win rate: `{:.1}%`\n- Test profit factor: `{:.2}`\n- Test max DD: `{:.3}`\n- Test score: `{:.4}`\n\n",
        holdout.train_from,
        holdout.train_to,
        holdout.test_from,
        holdout.test_to,
        if holdout.active { "yes" } else { "no" },
        holdout.selected_profile,
        if holdout.train_metrics.robust_ranking_eligible {
            "yes"
        } else {
            "no"
        },
        format_gate(holdout.train_metrics.robust_score_gate),
        holdout.train_metrics.min_deployable_robust_score,
        holdout.train_metrics.trades,
        holdout.train_metrics.recent_trades,
        format_optional_date(holdout.train_metrics.last_entry_date),
        format_optional_i64(holdout.train_metrics.days_since_last_entry),
        holdout.train_metrics.total_pnl,
        holdout.train_metrics.robust_score,
        holdout.metrics.trades,
        holdout.metrics.required_trades,
        holdout.metrics.total_pnl,
        holdout.metrics.win_rate * 100.0,
        holdout.metrics.profit_factor,
        holdout.metrics.max_drawdown,
        holdout.metrics.score
    ));

    out.push_str("## Detector And Execution Strategy Map\n\n");
    out.push_str("| Rank | Profile | Detector | Execution | Detector Filters |\n");
    out.push_str("|---:|---|---|---|---|\n");
    for (idx, result) in report.profiles.iter().take(10).enumerate() {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} |\n",
            idx + 1,
            result.profile.name,
            result.detector_strategy.name,
            result.execution_strategy.name,
            format_list(&result.detector_strategy.filters)
        ));
    }
    out.push('\n');

    out.push_str("## Fixed-Profile Walk-Forward\n\n");
    out.push_str("| Rank | Profile | OOS Pass | Active Years | Trades | Required Trades | PnL | Avg ROR | Win Rate | Profit Factor | Max DD | Score | Robust Score | Detector | Execution |\n");
    out.push_str("|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|\n");
    for (idx, result) in report
        .fixed_profile_walk_forward
        .iter()
        .take(15)
        .enumerate()
    {
        let m = &result.metrics;
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {:.2} | {:.3} | {:.1}% | {:.2} | {:.3} | {:.4} | {:.4} | {} | {} |\n",
            idx + 1,
            result.profile.name,
            if out_of_sample_gate_passes(m) {
                "yes"
            } else {
                "no"
            },
            result.active_years,
            m.trades,
            m.required_trades,
            m.total_pnl,
            m.avg_return_on_risk,
            m.win_rate * 100.0,
            m.profit_factor,
            m.max_drawdown,
            m.score,
            m.robust_score,
            result.detector_strategy.name,
            result.execution_strategy.name
        ));
    }
    out.push('\n');

    out.push_str("| Rank | Profile | Eligible | Robust Eligible | Candidates | Trades | PnL | Avg ROR | Win Rate | Profit Factor | Max DD | Positive Years | Worst Year | Avg Entry DTE | Avg Hold | Trades/Yr | Score | Robust Score |\n");
    out.push_str(
        "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|\n",
    );
    for (idx, result) in report.profiles.iter().enumerate() {
        let m = &result.metrics;
        let annual = &m.annual_stability;
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {:.2} | {:.3} | {:.1}% | {:.2} | {:.3} | {}/{} ({:.1}%) | {} | {:.1} | {:.1} | {:.1} | {:.4} | {:.4} |\n",
            idx + 1,
            result.profile.name,
            if m.ranking_eligible { "yes" } else { "no" },
            if m.robust_ranking_eligible {
                "yes"
            } else {
                "no"
            },
            result.candidates,
            m.trades,
            m.total_pnl,
            m.avg_return_on_risk,
            m.win_rate * 100.0,
            m.profit_factor,
            m.max_drawdown,
            annual.positive_years,
            annual.active_years,
            annual.positive_year_rate * 100.0,
            format_optional_year_pnl(annual.worst_year, annual.worst_year_pnl),
            m.avg_entry_dte,
            m.avg_days_held,
            m.trades_per_year,
            m.score,
            m.robust_score
        ));
    }
    out.push_str("\n## Chronological Robustness\n\n");
    out.push_str("| Profile | Period | Window | Eligible | Trades | PnL | Avg ROR | Win Rate | Profit Factor | Max DD | Score |\n");
    out.push_str("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for result in report.profiles.iter().take(10) {
        for period in &result.metrics.chronological {
            out.push_str(&format!(
                "| {} | {} | {} to {} | {} | {} | {:.2} | {:.3} | {:.1}% | {:.2} | {:.3} | {:.4} |\n",
                result.profile.name,
                period.name,
                period.from,
                period.to,
                if period.ranking_eligible { "yes" } else { "no" },
                period.trades,
                period.total_pnl,
                period.avg_return_on_risk,
                period.win_rate * 100.0,
                period.profit_factor,
                period.max_drawdown,
                period.score
            ));
        }
    }
    out.push_str("\n## Cost Stress\n\n");
    out.push_str("| Profile | Extra Cost/Trade | Trades | PnL | Avg ROR | Win Rate | Profit Factor | Max DD | Score |\n");
    out.push_str("|---|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for result in report.profiles.iter().take(10) {
        for stress in &result.metrics.cost_stress {
            out.push_str(&format!(
                "| {} | {:.2} | {} | {:.2} | {:.3} | {:.1}% | {:.2} | {:.3} | {:.4} |\n",
                result.profile.name,
                stress.per_trade_cost,
                stress.trades,
                stress.total_pnl,
                stress.avg_return_on_risk,
                stress.win_rate * 100.0,
                stress.profit_factor,
                stress.max_drawdown,
                stress.score
            ));
        }
    }
    if let Some(best) = report.profiles.first() {
        out.push_str("\n## Best Profile Yearly\n\n");
        out.push_str("| Year | Trades | PnL | Win Rate | Avg ROR |\n");
        out.push_str("|---:|---:|---:|---:|---:|\n");
        for (year, yearly) in &best.metrics.yearly {
            out.push_str(&format!(
                "| {} | {} | {:.2} | {:.1}% | {:.3} |\n",
                year,
                yearly.trades,
                yearly.pnl,
                yearly.win_rate * 100.0,
                yearly.avg_return_on_risk
            ));
        }
        out.push_str("\n## Best Profile Cycle\n\n");
        out.push_str(&format!(
            "- Average entry DTE: `{:.1}`\n- Median entry DTE: `{:.1}`\n- Average days held: `{:.1}`\n- Median days held: `{:.1}`\n- Exit reasons: `{}`\n",
            best.metrics.avg_entry_dte,
            best.metrics.median_entry_dte,
            best.metrics.avg_days_held,
            best.metrics.median_days_held,
            format_exit_reasons(&best.metrics.exit_reasons)
        ));

        out.push_str("\n## Best Profile Failure Anatomy\n\n");
        let winning_trades = best
            .trades
            .iter()
            .filter(|trade| trade.pnl > 0.0)
            .collect::<Vec<_>>();
        let losing_trades = best
            .trades
            .iter()
            .filter(|trade| trade.pnl <= 0.0)
            .collect::<Vec<_>>();
        out.push_str("| Bucket | Trades | Avg PnL | Avg ROR | Avg OTM% | Avg Short IV | Avg Trend Ret | Avg Recent DD | Avg Credit |\n");
        out.push_str("|---|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        out.push_str(&trade_feature_summary_row("Winners", &winning_trades));
        out.push_str(&trade_feature_summary_row("Losers", &losing_trades));

        let mut worst_trades = losing_trades;
        worst_trades.sort_by(|a, b| a.pnl.total_cmp(&b.pnl));
        out.push_str("\n| Entry | Exit | Exp | Short | Long | OTM% | Short IV | Trend Ret | Recent DD | Credit | Exit Debit | PnL | ROR | Reason |\n");
        out.push_str("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|\n");
        for trade in worst_trades.iter().take(10) {
            out.push_str(&format!(
                "| {} | {} | {} | {:.0}P | {:.0}P | {:.1}% | {:.1}% | {} | {} | {:.2} | {:.2} | {:.2} | {:.3} | {} |\n",
                trade.entry_date,
                trade.exit_date,
                trade.expiration,
                trade.short_put,
                trade.long_put,
                trade.short_otm_pct * 100.0,
                trade.short_iv * 100.0,
                format_optional_pct(trade.underlying_lookback_return),
                format_optional_pct(trade.underlying_recent_drawdown),
                trade.entry_credit,
                trade.exit_debit,
                trade.pnl,
                trade.return_on_risk,
                trade.exit_reason
            ));
        }

        out.push_str("\n## Best Profile Trades\n\n");
        out.push_str("| Entry | Exit | Exp | Short | Long | OTM% | Short IV | Trend Ret | Recent DD | Realized Vol | Credit | Exit Debit | PnL | ROR | Reason |\n");
        out.push_str("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|\n");
        for trade in best.trades.iter().take(50) {
            out.push_str(&format!(
                "| {} | {} | {} | {:.0}P | {:.0}P | {:.1}% | {:.1}% | {} | {} | {} | {:.2} | {:.2} | {:.2} | {:.3} | {} |\n",
                trade.entry_date,
                trade.exit_date,
                trade.expiration,
                trade.short_put,
                trade.long_put,
                trade.short_otm_pct * 100.0,
                trade.short_iv * 100.0,
                format_optional_pct(trade.underlying_lookback_return),
                format_optional_pct(trade.underlying_recent_drawdown),
                format_optional_pct(trade.underlying_realized_vol),
                trade.entry_credit,
                trade.exit_debit,
                trade.pnl,
                trade.return_on_risk,
                trade.exit_reason
            ));
        }
    }
    out
}

fn trade_feature_summary_row(label: &str, trades: &[&ResearchTrade]) -> String {
    format!(
        "| {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
        label,
        trades.len(),
        format_optional_number(avg_trade_value(trades, |trade| Some(trade.pnl)), 2),
        format_optional_number(
            avg_trade_value(trades, |trade| Some(trade.return_on_risk)),
            3
        ),
        format_optional_percent(
            avg_trade_value(trades, |trade| Some(trade.short_otm_pct)),
            1
        ),
        format_optional_percent(avg_trade_value(trades, |trade| Some(trade.short_iv)), 1),
        format_optional_percent(
            avg_trade_value(trades, |trade| trade.underlying_lookback_return),
            1
        ),
        format_optional_percent(
            avg_trade_value(trades, |trade| trade.underlying_recent_drawdown),
            1
        ),
        format_optional_number(avg_trade_value(trades, |trade| Some(trade.entry_credit)), 2)
    )
}

fn professional_options_review(report: &ResearchReport) -> Vec<String> {
    let mut lines = Vec::new();
    let Some(best) = report.profiles.first() else {
        lines.push("No evaluated profile; no tradeable conclusion.".to_owned());
        return lines;
    };
    let best_metrics = &best.metrics;
    lines.push(format!(
        "Action readiness: `{}`; latest signals remain `{}` until best-profile, walk-forward, and holdout gates all pass.",
        if report.deployment_gate.pass {
            "deployment-gate-passed"
        } else {
            "research-only"
        },
        if report.deployment_gate.pass {
            "deployable candidates"
        } else {
            "research candidates"
        }
    ));
    lines.push(format!(
        "Full-window edge: best profile `{}` has `{}` trades, PnL `{:.2}`, profit factor `{:.2}`, max DD `{:.3}`, score `{:.4}`, robust score `{:.4}`.",
        best.profile.name,
        best_metrics.trades,
        best_metrics.total_pnl,
        best_metrics.profit_factor,
        best_metrics.max_drawdown,
        best_metrics.score,
        best_metrics.robust_score
    ));
    lines.push(format!(
        "Walk-forward evidence: `{}` with `{}`/`{}` required OOS trades, PnL `{:.2}`, profit factor `{:.2}`, max DD `{:.3}`, score `{:.4}`.",
        format_gate(report.deployment_gate.walk_forward_oos_gate),
        report.walk_forward.metrics.trades,
        report.walk_forward.metrics.required_trades,
        report.walk_forward.metrics.total_pnl,
        report.walk_forward.metrics.profit_factor,
        report.walk_forward.metrics.max_drawdown,
        report.walk_forward.metrics.score
    ));
    lines.push(format!(
        "Holdout evidence: `{}`; active `{}`, selected `{}`, train robust score `{:.4}` versus deployable minimum `{:.4}`.",
        format_gate(report.deployment_gate.holdout_oos_gate),
        if report.holdout.active { "yes" } else { "no" },
        report.holdout.selected_profile,
        report.holdout.train_metrics.robust_score,
        report.holdout.train_metrics.min_deployable_robust_score
    ));
    if let Some(stress) = best_metrics
        .cost_stress
        .iter()
        .find(|stress| stress.per_trade_cost == 10.0)
    {
        lines.push(format!(
            "Cost stress: at `{:.0}` extra dollars per trade, PnL is `{:.2}`, profit factor `{:.2}`, score `{:.4}`.",
            stress.per_trade_cost,
            stress.total_pnl,
            stress.profit_factor,
            stress.score
        ));
    }
    if let Some(stress) = best_metrics
        .cost_stress
        .iter()
        .find(|stress| stress.per_trade_cost == 25.0)
    {
        lines.push(format!(
            "High-friction stress: at `{:.0}` extra dollars per trade, PnL is `{:.2}`, profit factor `{:.2}`, score `{:.4}`.",
            stress.per_trade_cost,
            stress.total_pnl,
            stress.profit_factor,
            stress.score
        ));
    }
    let annual = &best_metrics.annual_stability;
    lines.push(format!(
        "Regime stability: positive years `{}/{}`; worst year `{}`.",
        annual.positive_years,
        annual.active_years,
        format_optional_year_pnl(annual.worst_year, annual.worst_year_pnl)
    ));
    lines.push(format!(
        "Execution texture: average entry DTE `{:.1}`, average hold `{:.1}` days, exits `{}`.",
        best_metrics.avg_entry_dte,
        best_metrics.avg_days_held,
        format_exit_reasons(&best_metrics.exit_reasons)
    ));
    if report.deployment_gate.pass {
        lines.push("Decision: profile passes research gates; next review should be broker, liquidity, sizing, and shadow-live readiness.".to_owned());
    } else if report.deployment_gate.best_profile_gate
        && !report.deployment_gate.walk_forward_oos_gate
        && !report.deployment_gate.holdout_oos_gate
    {
        lines.push("Decision: keep as a research candidate, but do not promote; next work should explain OOS/holdout fragility before adding more profile knobs.".to_owned());
    } else if !report.deployment_gate.best_profile_gate {
        lines.push("Decision: reject for promotion; the full-window profile itself does not clear the robustness gate.".to_owned());
    } else {
        lines.push("Decision: partial evidence only; fix the remaining OOS gate before considering symbol expansion or live readiness.".to_owned());
    }
    lines
}

fn avg_trade_value<F>(trades: &[&ResearchTrade], value: F) -> Option<f64>
where
    F: Fn(&ResearchTrade) -> Option<f64>,
{
    let values = trades
        .iter()
        .filter_map(|trade| value(trade))
        .collect::<Vec<_>>();
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}

fn format_optional_number(value: Option<f64>, precision: usize) -> String {
    value
        .map(|value| format!("{value:.precision$}"))
        .unwrap_or_else(|| "n/a".to_owned())
}

fn format_optional_percent(value: Option<f64>, precision: usize) -> String {
    value
        .map(|value| format!("{:.precision$}%", value * 100.0))
        .unwrap_or_else(|| "n/a".to_owned())
}

fn format_optional_pct(value: Option<f64>) -> String {
    value
        .map(|value| format!("{:.1}%", value * 100.0))
        .unwrap_or_else(|| "n/a".to_owned())
}

fn format_optional_date(value: Option<NaiveDate>) -> String {
    value
        .map(|date| date.to_string())
        .unwrap_or_else(|| "n/a".to_owned())
}

fn format_optional_i64(value: Option<i64>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "n/a".to_owned())
}

fn format_optional_year_pnl(year: Option<i32>, pnl: f64) -> String {
    year.map(|year| format!("{year} ({pnl:.2})"))
        .unwrap_or_else(|| "n/a".to_owned())
}

fn out_of_sample_gate_passes(metrics: &ResearchMetrics) -> bool {
    metrics.ranking_eligible && metrics.total_pnl > 0.0 && metrics.score > 0.0
}

fn format_gate(passes: bool) -> &'static str {
    if passes { "pass" } else { "blocked" }
}

fn format_exit_reasons(reasons: &BTreeMap<String, usize>) -> String {
    reasons
        .iter()
        .map(|(reason, count)| format!("{reason}: {count}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_profile_counts(counts: &BTreeMap<String, usize>) -> String {
    counts
        .iter()
        .map(|(profile, count)| format!("{profile}: {count}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_list(items: &[String]) -> String {
    if items.is_empty() {
        "none".to_owned()
    } else {
        items.join(", ")
    }
}

fn markdown_cell(value: &str) -> String {
    value.replace('|', "\\|")
}

fn yyyymmdd(date: NaiveDate) -> String {
    date.format("%Y%m%d").to_string()
}

fn parse_yyyymmdd(value: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y%m%d").ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn option_cache_window_parser_matches_right_dataset_and_expiration() {
        let start = NaiveDate::from_ymd_opt(2025, 1, 3).unwrap();
        let end = NaiveDate::from_ymd_opt(2025, 1, 9).unwrap();

        assert_eq!(
            parse_option_cache_window(
                "research_greeks_20250117_20250103_20250109.json",
                "20250117",
                OptionRight::Put,
                CachedOptionDataset::Greeks,
            ),
            Some((start, end))
        );
        assert_eq!(
            parse_option_cache_window(
                "research_call_oi_20250117_20250103_20250109.json",
                "20250117",
                OptionRight::Call,
                CachedOptionDataset::OpenInterest,
            ),
            Some((start, end))
        );
        assert_eq!(
            parse_option_cache_window(
                "research_call_oi_20250117_20250103_20250109.json",
                "20250117",
                OptionRight::Put,
                CachedOptionDataset::OpenInterest,
            ),
            None
        );
        assert_eq!(
            parse_option_cache_window(
                "research_oi_20250124_20250103_20250109.json",
                "20250117",
                OptionRight::Put,
                CachedOptionDataset::OpenInterest,
            ),
            None
        );
    }

    #[test]
    fn covering_option_cache_windows_prefers_tightest_covering_file() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let raw_dir = std::env::temp_dir().join(format!(
            "spreadfoundry-cache-window-test-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&raw_dir).unwrap();
        for file_name in [
            "research_oi_20250117_20241201_20250116.json",
            "research_oi_20250117_20250101_20250116.json",
            "research_oi_20250117_20250103_20250109.json",
            "research_call_oi_20250117_20250101_20250116.json",
            "research_greeks_20250117_20250101_20250116.json",
        ] {
            fs::write(raw_dir.join(file_name), "{}").unwrap();
        }

        let windows = covering_option_cache_windows(
            &raw_dir,
            "20250117",
            NaiveDate::from_ymd_opt(2025, 1, 3).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 9).unwrap(),
            OptionRight::Put,
            CachedOptionDataset::OpenInterest,
        )
        .unwrap();

        let file_names = windows
            .iter()
            .map(|window| {
                window
                    .path
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_owned()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            file_names,
            vec![
                "research_oi_20250117_20250103_20250109.json",
                "research_oi_20250117_20250101_20250116.json",
                "research_oi_20250117_20241201_20250116.json",
            ]
        );

        fs::remove_dir_all(raw_dir).unwrap();
    }

    #[test]
    fn option_cache_covering_sequence_composes_adjacent_windows() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let raw_dir = std::env::temp_dir().join(format!(
            "spreadfoundry-cache-sequence-test-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&raw_dir).unwrap();
        for file_name in [
            "research_call_oi_20250117_20250101_20250103.json",
            "research_call_oi_20250117_20250104_20250110.json",
            "research_call_oi_20250117_20250111_20250117.json",
            "research_oi_20250117_20250104_20250117.json",
        ] {
            fs::write(raw_dir.join(file_name), "{}").unwrap();
        }

        let windows = option_cache_covering_sequence(
            &raw_dir,
            "20250117",
            NaiveDate::from_ymd_opt(2025, 1, 5).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 12).unwrap(),
            OptionRight::Call,
            CachedOptionDataset::OpenInterest,
        )
        .unwrap();

        let file_names = windows
            .iter()
            .map(|window| {
                window
                    .path
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_owned()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            file_names,
            vec![
                "research_call_oi_20250117_20250104_20250110.json",
                "research_call_oi_20250117_20250111_20250117.json",
            ]
        );

        fs::remove_dir_all(raw_dir).unwrap();
    }

    #[test]
    fn cached_option_cache_covering_sequence_reuses_indexed_windows() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let raw_dir = std::env::temp_dir().join(format!(
            "spreadfoundry-cache-index-test-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&raw_dir).unwrap();
        for file_name in [
            "research_call_greeks_20250117_20250101_20250103.json",
            "research_call_greeks_20250117_20250104_20250110.json",
            "research_call_greeks_20250117_20250111_20250117.json",
        ] {
            fs::write(raw_dir.join(file_name), "{}").unwrap();
        }

        let first = cached_option_cache_covering_sequence(
            &raw_dir,
            "20250117",
            NaiveDate::from_ymd_opt(2025, 1, 5).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 12).unwrap(),
            OptionRight::Call,
            CachedOptionDataset::Greeks,
        )
        .unwrap();
        let second = cached_option_cache_covering_sequence(
            &raw_dir,
            "20250117",
            NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2025, 1, 16).unwrap(),
            OptionRight::Call,
            CachedOptionDataset::Greeks,
        )
        .unwrap();

        assert_eq!(first.len(), 2);
        assert_eq!(second.len(), 3);
        fs::remove_dir_all(raw_dir).unwrap();
    }

    #[test]
    fn option_cache_complete_coverage_requires_oi_and_greeks() {
        let raw_dir = unique_test_path("complete-cache-coverage");
        fs::create_dir_all(&raw_dir).unwrap();
        for file_name in [
            "research_call_oi_20250117_20250103_20250109.json",
            "research_call_oi_20250117_20250110_20250117.json",
        ] {
            fs::write(raw_dir.join(file_name), "{}").unwrap();
        }

        let start = NaiveDate::from_ymd_opt(2025, 1, 5).unwrap();
        let end = NaiveDate::from_ymd_opt(2025, 1, 12).unwrap();
        assert!(
            !option_cache_has_complete_coverage(
                &raw_dir,
                "20250117",
                start,
                end,
                OptionRight::Call,
            )
            .unwrap()
        );

        for file_name in [
            "research_call_greeks_20250117_20250103_20250109.json",
            "research_call_greeks_20250117_20250110_20250117.json",
        ] {
            fs::write(raw_dir.join(file_name), "{}").unwrap();
        }

        assert!(
            option_cache_has_complete_coverage(
                &raw_dir,
                "20250117",
                start,
                end,
                OptionRight::Call,
            )
            .unwrap()
        );

        fs::remove_dir_all(raw_dir).unwrap();
    }

    #[test]
    fn max_expiration_limit_samples_across_full_window() {
        let items = (1..=10).collect::<Vec<_>>();

        assert_eq!(evenly_spaced(items, 4), vec![1, 4, 7, 10]);
    }

    #[test]
    fn max_expiration_limit_handles_edge_cases() {
        assert_eq!(evenly_spaced(vec![1, 2, 3], 0), Vec::<i32>::new());
        assert_eq!(evenly_spaced(vec![1, 2, 3], 1), vec![1]);
        assert_eq!(evenly_spaced(vec![1, 2, 3], 4), vec![1, 2, 3]);
    }

    #[test]
    fn expiration_load_window_keeps_lookback_before_effective_start() {
        let expiration = NaiveDate::from_ymd_opt(2020, 3, 20).unwrap();
        let from = NaiveDate::from_ymd_opt(2020, 2, 4).unwrap();
        let to = NaiveDate::from_ymd_opt(2020, 3, 1).unwrap();
        let bounds = ExpirationLoadBounds {
            from,
            to,
            max_entry_dte: 45,
            min_force_close_dte: 21,
            option_row_lookback_days: 60,
        };

        let (start, end) = expiration_load_window(expiration, bounds).unwrap();

        assert_eq!(start, NaiveDate::from_ymd_opt(2019, 12, 6).unwrap());
        assert_eq!(end, to);
    }

    #[test]
    fn expiration_load_window_clamps_to_expiration() {
        let expiration = NaiveDate::from_ymd_opt(2024, 1, 5).unwrap();
        let bounds = ExpirationLoadBounds {
            from: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            to: NaiveDate::from_ymd_opt(2024, 1, 20).unwrap(),
            max_entry_dte: 14,
            min_force_close_dte: 0,
            option_row_lookback_days: 0,
        };

        let (start, end) = expiration_load_window(expiration, bounds).unwrap();

        assert_eq!(start, NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
        assert_eq!(end, expiration);
    }

    #[test]
    fn uncapped_weekly_runs_do_not_fetch_per_expiration_regime_lookback_rows() {
        assert_eq!(
            option_row_lookback_days(ResearchProfileFamily::Weekly, None, 60),
            0
        );
        assert_eq!(
            option_row_lookback_days(ResearchProfileFamily::WeeklyPutDebit, None, 60),
            0
        );
        assert_eq!(
            option_row_lookback_days(ResearchProfileFamily::WeeklyCallCredit, None, 60),
            0
        );
        assert_eq!(
            option_row_lookback_days(ResearchProfileFamily::WeeklyCallDebit, None, 60),
            0
        );
        assert_eq!(
            option_row_lookback_days(ResearchProfileFamily::Weekly, Some(8), 60),
            60
        );
        assert_eq!(
            option_row_lookback_days(ResearchProfileFamily::Swing, None, 60),
            60
        );
    }

    #[test]
    fn weekly_put_debit_candidate_records_debit_entry_and_positive_exit_value() {
        let entry_date = NaiveDate::from_ymd_opt(2026, 1, 6).unwrap();
        let exit_date = entry_date + Duration::days(1);
        let expiration = entry_date + Duration::days(11);
        let mut profile = ResearchProfile::weekly_put_debit_baseline();
        profile.trend_lookback_days = None;
        profile.min_underlying_return = None;
        profile.drawdown_lookback_days = None;
        profile.max_underlying_drawdown = None;
        profile.realized_vol_lookback_days = None;
        profile.max_realized_vol = None;

        let mut rows_by_expiration = BTreeMap::new();
        rows_by_expiration.insert(
            expiration,
            vec![
                option_day(entry_date, 100.0, 4.20, 4.40, -0.45, 105.0),
                option_day(entry_date, 95.0, 2.00, 2.15, -0.25, 105.0),
                option_day(exit_date, 100.0, 5.60, 5.75, -0.55, 101.0),
                option_day(exit_date, 95.0, 1.95, 2.10, -0.30, 101.0),
            ],
        );

        let candidates = generate_candidates(&rows_by_expiration, &profile, entry_date, entry_date);

        assert_eq!(candidates.len(), 1);
        let candidate = &candidates[0];
        assert_eq!(candidate.structure, SpreadStructure::PutDebitSpread);
        assert_eq!(candidate.long.strike, 100.0);
        assert_eq!(candidate.short.strike, 95.0);
        assert!((candidate.credit - 2.40).abs() < 1e-9);
        assert!((candidate.max_profit_per_share - 2.60).abs() < 1e-9);
        assert!((candidate.max_loss_per_share - 2.40).abs() < 1e-9);

        let intent = candidate_order_intent(candidate, "TSLA").unwrap();
        assert_eq!(intent.symbol, "TSLA");
        assert_eq!(intent.strategy, "put_debit_spread");
        assert_eq!(intent.order_effect, OptionOrderEffect::Debit);
        assert_eq!(
            intent.limit_price,
            research_decimal_from_f64(candidate.credit, "expected debit").unwrap()
        );
        assert_eq!(intent.legs.len(), 2);
        assert_eq!(intent.legs[0].side, OptionOrderSide::Buy);
        assert_eq!(intent.legs[0].key.strike.to_string(), "100");
        assert_eq!(intent.legs[1].side, OptionOrderSide::Sell);
        assert_eq!(intent.legs[1].key.strike.to_string(), "95");

        let lookup = build_lookup(&rows_by_expiration);
        let trade = simulate_candidate(candidate, &lookup, &profile).unwrap();

        assert_eq!(trade.exit_reason, "take_profit");
        assert!((trade.entry_credit + 2.40).abs() < 1e-9);
        assert!((trade.exit_debit + 3.50).abs() < 1e-9);
        assert!((trade.pnl - 110.0).abs() < 1e-9);
        assert!((trade.max_profit - 260.0).abs() < 1e-9);
        assert!((trade.max_loss - 240.0).abs() < 1e-9);

        profile.min_debit = Some(2.50);
        assert!(
            generate_candidates(&rows_by_expiration, &profile, entry_date, entry_date).is_empty()
        );
        profile.min_debit = None;
        profile.max_short_leg_delta_abs = Some(0.20);
        assert!(
            generate_candidates(&rows_by_expiration, &profile, entry_date, entry_date).is_empty()
        );
    }

    #[test]
    fn weekly_call_debit_candidate_records_debit_entry_and_positive_exit_value() {
        let entry_date = NaiveDate::from_ymd_opt(2026, 1, 6).unwrap();
        let exit_date = entry_date + Duration::days(1);
        let expiration = entry_date + Duration::days(11);
        let mut profile = ResearchProfile::weekly_call_debit_baseline();
        profile.trend_lookback_days = None;
        profile.min_underlying_return = None;
        profile.max_underlying_return = None;
        profile.drawdown_lookback_days = None;
        profile.max_underlying_drawdown = None;
        profile.realized_vol_lookback_days = None;
        profile.min_realized_vol = None;
        profile.max_realized_vol = None;

        let mut rows_by_expiration = BTreeMap::new();
        rows_by_expiration.insert(
            expiration,
            vec![
                option_day(entry_date, 100.0, 4.20, 4.40, 0.45, 105.0),
                option_day(entry_date, 105.0, 2.00, 2.15, 0.25, 105.0),
                option_day(exit_date, 100.0, 5.60, 5.75, 0.55, 109.0),
                option_day(exit_date, 105.0, 1.95, 2.10, 0.30, 109.0),
            ],
        );

        let candidates = generate_candidates(&rows_by_expiration, &profile, entry_date, entry_date);

        assert_eq!(candidates.len(), 1);
        let candidate = &candidates[0];
        assert_eq!(candidate.structure, SpreadStructure::CallDebitSpread);
        assert_eq!(candidate.long.strike, 100.0);
        assert_eq!(candidate.short.strike, 105.0);
        assert!((candidate.credit - 2.40).abs() < 1e-9);
        assert!((candidate.max_profit_per_share - 2.60).abs() < 1e-9);
        assert!((candidate.max_loss_per_share - 2.40).abs() < 1e-9);

        let lookup = build_lookup(&rows_by_expiration);
        let trade = simulate_candidate(candidate, &lookup, &profile).unwrap();

        assert_eq!(trade.exit_reason, "take_profit");
        assert!((trade.entry_credit + 2.40).abs() < 1e-9);
        assert!((trade.exit_debit + 3.50).abs() < 1e-9);
        assert!((trade.pnl - 110.0).abs() < 1e-9);
        assert!((trade.max_profit - 260.0).abs() < 1e-9);
        assert!((trade.max_loss - 240.0).abs() < 1e-9);
    }

    #[test]
    fn weekly_call_credit_candidate_records_credit_entry_and_positive_exit_value() {
        let entry_date = NaiveDate::from_ymd_opt(2026, 1, 6).unwrap();
        let exit_date = entry_date + Duration::days(1);
        let expiration = entry_date + Duration::days(7);
        let mut profile = ResearchProfile::weekly_baseline();
        profile.structure = SpreadStructure::CallCreditSpread;
        profile.min_dte = 1;
        profile.max_dte = 10;
        profile.min_short_delta_abs = 0.10;
        profile.max_short_delta_abs = 0.35;
        profile.max_width = 10.0;
        profile.min_credit_width = 0.05;
        profile.take_profit_pct = 0.33;
        profile.stop_loss_multiple = 2.0;
        profile.trend_lookback_days = None;
        profile.min_underlying_return = None;
        profile.max_underlying_return = None;
        profile.drawdown_lookback_days = None;
        profile.max_underlying_drawdown = None;
        profile.risk_regime_cooldown_guard = None;
        profile.realized_vol_lookback_days = None;
        profile.max_realized_vol = None;
        profile.min_short_otm_pct = None;

        let mut rows_by_expiration = BTreeMap::new();
        rows_by_expiration.insert(
            expiration,
            vec![
                option_day(entry_date, 110.0, 1.20, 1.30, 0.25, 105.0),
                option_day(entry_date, 115.0, 0.40, 0.45, 0.15, 105.0),
                option_day(exit_date, 110.0, 0.50, 0.60, 0.18, 103.0),
                option_day(exit_date, 115.0, 0.20, 0.25, 0.10, 103.0),
            ],
        );

        let candidates = generate_candidates(&rows_by_expiration, &profile, entry_date, entry_date);

        assert_eq!(candidates.len(), 1);
        let candidate = &candidates[0];
        assert_eq!(candidate.structure, SpreadStructure::CallCreditSpread);
        assert_eq!(candidate.short.strike, 110.0);
        assert_eq!(candidate.long.strike, 115.0);
        assert!((candidate.credit - 0.75).abs() < 1e-9);
        assert!((candidate.max_profit_per_share - 0.75).abs() < 1e-9);
        assert!((candidate.max_loss_per_share - 4.25).abs() < 1e-9);

        let lookup = build_lookup(&rows_by_expiration);
        let trade = simulate_candidate(candidate, &lookup, &profile).unwrap();

        assert_eq!(trade.exit_reason, "take_profit");
        assert!((trade.entry_credit - 0.75).abs() < 1e-9);
        assert!((trade.exit_debit - 0.40).abs() < 1e-9);
        assert!((trade.pnl - 35.0).abs() < 1e-9);
        assert!((trade.max_profit - 75.0).abs() < 1e-9);
        assert!((trade.max_loss - 425.0).abs() < 1e-9);
    }

    #[test]
    fn weekly_wheel_assignment_then_covered_call_called_away_records_stock_inventory_pnl() {
        let entry_date = NaiveDate::from_ymd_opt(2026, 1, 2).unwrap();
        let put_expiration = entry_date + Duration::days(7);
        let call_entry_date = put_expiration + Duration::days(1);
        let call_expiration = call_entry_date + Duration::days(6);
        let mut profile = ResearchProfile::weekly_wheel_baseline();
        profile.trend_lookback_days = None;
        profile.min_underlying_return = None;
        profile.drawdown_lookback_days = None;
        profile.max_underlying_drawdown = None;
        profile.realized_vol_lookback_days = None;
        profile.max_realized_vol = None;

        let mut put_rows_by_expiration = BTreeMap::new();
        put_rows_by_expiration.insert(
            put_expiration,
            vec![
                option_day(entry_date, 100.0, 2.00, 2.20, -0.25, 105.0),
                option_day(put_expiration, 100.0, 5.10, 5.40, -0.55, 95.0),
            ],
        );
        let mut call_rows_by_expiration = BTreeMap::new();
        call_rows_by_expiration.insert(
            call_expiration,
            vec![
                option_day(call_entry_date, 100.0, 1.50, 1.70, 0.25, 96.0),
                option_day(call_expiration, 100.0, 3.00, 3.30, 0.65, 103.0),
            ],
        );

        let candidates =
            generate_candidates(&put_rows_by_expiration, &profile, entry_date, entry_date);

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].structure, SpreadStructure::Wheel);
        assert_eq!(candidates[0].short.strike, 100.0);
        assert!((candidates[0].credit - 2.00).abs() < 1e-9);
        assert!((candidates[0].max_loss_per_share - 98.00).abs() < 1e-9);

        let intent = candidate_order_intent(&candidates[0], "PLTR").unwrap();
        assert_eq!(intent.symbol, "PLTR");
        assert_eq!(intent.strategy, "wheel");
        assert_eq!(intent.order_effect, OptionOrderEffect::Credit);
        assert_eq!(
            intent.limit_price,
            research_decimal_from_f64(candidates[0].credit, "expected credit").unwrap()
        );
        assert_eq!(intent.legs.len(), 1);
        assert_eq!(intent.legs[0].side, OptionOrderSide::Sell);
        assert_eq!(intent.legs[0].key.strike.to_string(), "100");

        let trades = simulate_wheel_non_overlapping(
            &candidates,
            &put_rows_by_expiration,
            &call_rows_by_expiration,
            &profile,
            call_expiration,
        );

        assert_eq!(trades.len(), 1);
        let trade = &trades[0];
        assert_eq!(trade.exit_reason, "covered_call_assigned");
        assert_eq!(trade.exit_date, call_expiration);
        assert_eq!(trade.short_put, 100.0);
        assert_eq!(trade.long_put, 100.0);
        assert_eq!(trade.width, 1.0);
        assert!((trade.entry_credit - 3.50).abs() < 1e-9);
        assert!((trade.pnl - 350.0).abs() < 1e-9);
        assert!((trade.max_loss - 9800.0).abs() < 1e-9);
        assert!((trade.return_on_risk - (350.0 / 9800.0)).abs() < 1e-9);
    }

    #[test]
    fn weekly_wheel_inventory_exit_can_sell_call_below_assigned_strike() {
        let entry_date = NaiveDate::from_ymd_opt(2026, 1, 2).unwrap();
        let put_expiration = entry_date + Duration::days(7);
        let call_entry_date = put_expiration + Duration::days(1);
        let call_expiration = call_entry_date + Duration::days(6);
        let mut profile = ResearchProfile::weekly_wheel_baseline();
        profile.covered_call_min_strike_pct_of_assigned = 0.98;
        profile.trend_lookback_days = None;
        profile.min_underlying_return = None;
        profile.drawdown_lookback_days = None;
        profile.max_underlying_drawdown = None;
        profile.realized_vol_lookback_days = None;
        profile.max_realized_vol = None;

        let mut put_rows_by_expiration = BTreeMap::new();
        put_rows_by_expiration.insert(
            put_expiration,
            vec![
                option_day(entry_date, 100.0, 2.00, 2.20, -0.25, 105.0),
                option_day(put_expiration, 100.0, 5.10, 5.40, -0.55, 95.0),
            ],
        );
        let mut call_rows_by_expiration = BTreeMap::new();
        call_rows_by_expiration.insert(
            call_expiration,
            vec![
                option_day(call_entry_date, 98.0, 1.50, 1.70, 0.25, 96.0),
                option_day(call_expiration, 98.0, 2.50, 2.70, 0.65, 99.0),
            ],
        );

        let candidates =
            generate_candidates(&put_rows_by_expiration, &profile, entry_date, entry_date);
        let trades = simulate_wheel_non_overlapping(
            &candidates,
            &put_rows_by_expiration,
            &call_rows_by_expiration,
            &profile,
            call_expiration,
        );

        assert_eq!(trades.len(), 1);
        let trade = &trades[0];
        assert_eq!(trade.exit_reason, "covered_call_assigned");
        assert_eq!(trade.long_put, 98.0);
        assert!((trade.entry_credit - 3.50).abs() < 1e-9);
        assert!((trade.pnl - 150.0).abs() < 1e-9);
        assert!((trade.return_on_risk - (150.0 / 9800.0)).abs() < 1e-9);
    }

    #[test]
    fn weekly_put_debit_costaware_profiles_require_larger_debits() {
        let profiles = weekly_put_debit_research_profiles();
        let costaware = profiles
            .iter()
            .filter(|profile| profile.name.starts_with("weekly_put_debit_costaware_"))
            .collect::<Vec<_>>();

        assert_eq!(costaware.len(), 27);
        assert!(
            costaware
                .iter()
                .all(|profile| profile.structure == SpreadStructure::PutDebitSpread)
        );
        assert!(costaware.iter().all(|profile| profile.min_dte == 3));
        assert!(costaware.iter().all(|profile| profile.max_dte == 10));
        assert!(costaware.iter().all(|profile| profile.max_width == 25.0));
        assert!(
            costaware
                .iter()
                .all(|profile| profile.min_short_delta_abs == 0.20)
        );
        assert!(
            costaware
                .iter()
                .all(|profile| profile.max_short_delta_abs == 0.45)
        );
        assert!(
            costaware
                .iter()
                .all(|profile| profile.min_debit.is_some_and(|value| value >= 0.30))
        );
    }

    #[test]
    fn weekly_put_debit_regime_profiles_filter_low_vol_overheated_deep_pullbacks() {
        let profiles = weekly_put_debit_research_profiles();
        let regime_profiles = profiles
            .iter()
            .filter(|profile| profile.name.starts_with("weekly_put_debit_regime_"))
            .collect::<Vec<_>>();

        assert_eq!(regime_profiles.len(), 12);
        assert!(
            regime_profiles
                .iter()
                .all(|profile| profile.structure == SpreadStructure::PutDebitSpread)
        );
        assert!(regime_profiles.iter().all(|profile| profile.min_dte == 1));
        assert!(regime_profiles.iter().all(|profile| profile.max_dte == 7));
        assert!(
            regime_profiles
                .iter()
                .all(|profile| profile.max_width == 25.0)
        );
        assert!(
            regime_profiles
                .iter()
                .all(|profile| profile.realized_vol_lookback_days == Some(20))
        );
        assert!(
            regime_profiles
                .iter()
                .all(|profile| profile.min_realized_vol.is_some_and(|value| value >= 0.30))
        );
        assert!(
            regime_profiles
                .iter()
                .any(|profile| profile.min_realized_vol == Some(0.35))
        );
        assert!(profiles.iter().any(|profile| {
            profile.name.starts_with("weekly_put_debit_drawdown_")
                && profile.min_underlying_drawdown == Some(0.05)
                && profile.max_underlying_return == Some(0.05)
        }));
        assert!(profiles.iter().any(|profile| {
            profile
                .name
                .starts_with("weekly_put_debit_disciplined_drawdown_")
                && profile.max_width == 15.0
                && profile.max_debit_width == Some(0.35)
                && profile.min_underlying_drawdown == Some(0.05)
        }));
        assert!(profiles.iter().any(|profile| {
            profile
                .name
                .starts_with("weekly_put_debit_legguard_outerdelta15_")
                && profile.max_short_leg_delta_abs == Some(0.15)
        }));
        assert!(
            regime_profiles
                .iter()
                .all(|profile| profile.max_underlying_drawdown == Some(0.12))
        );
        assert!(regime_profiles.iter().all(|profile| {
            profile
                .max_underlying_return
                .is_some_and(|value| value <= 0.15)
        }));
    }

    #[test]
    fn weekly_call_debit_profiles_are_call_debit_and_high_cadence() {
        let profiles = weekly_call_debit_research_profiles();

        assert!(profiles.len() > 100);
        assert!(
            profiles
                .iter()
                .all(|profile| profile.structure == SpreadStructure::CallDebitSpread)
        );
        assert!(
            profiles.iter().all(|profile| {
                profile.min_trades_per_year >= MIN_WEEKLY_RANKING_TRADES_PER_YEAR
            })
        );
        assert!(profiles.iter().any(|profile| {
            profile.name.starts_with("weekly_call_debit_regime_")
                && profile
                    .min_underlying_return
                    .is_some_and(|value| value >= 0.0)
                && profile.min_realized_vol.is_some()
        }));
        assert!(profiles.iter().any(|profile| {
            profile
                .name
                .starts_with("weekly_call_debit_disciplined_costaware_")
                && profile.max_width == 15.0
                && profile.max_debit_width == Some(0.35)
                && profile.min_debit == Some(0.30)
        }));
        assert!(profiles.iter().any(|profile| {
            profile
                .name
                .starts_with("weekly_call_debit_balanced_costaware_")
                && profile.max_width == 20.0
                && profile.max_debit_width == Some(0.45)
                && profile.min_debit == Some(0.30)
        }));
        assert!(profiles.iter().any(|profile| {
            profile
                .name
                .starts_with("weekly_call_debit_legguard_outerdelta15_")
                && profile.max_short_leg_delta_abs == Some(0.15)
        }));
    }

    #[test]
    fn weekly_call_credit_profiles_are_call_credit_and_high_cadence() {
        let profiles = weekly_call_credit_research_profiles();

        assert!(profiles.len() >= 30);
        assert!(
            profiles
                .iter()
                .all(|profile| profile.structure == SpreadStructure::CallCreditSpread)
        );
        assert!(
            profiles.iter().all(|profile| {
                profile.min_trades_per_year >= MIN_WEEKLY_RANKING_TRADES_PER_YEAR
            })
        );
        assert!(profiles.iter().any(|profile| {
            profile.name.starts_with("weekly_call_credit_weak_")
                && profile.max_underlying_return == Some(0.05)
                && profile.min_underlying_drawdown == Some(0.02)
        }));
        assert!(profiles.iter().any(|profile| {
            profile.name.starts_with("weekly_call_credit_overbought_")
                && profile.min_underlying_return == Some(0.10)
                && profile.max_underlying_drawdown == Some(0.10)
        }));
    }

    #[test]
    fn weekly_wheel_profiles_cover_plateau_variant_minimum() {
        let profiles = weekly_wheel_research_profiles();

        assert!(profiles.len() >= PLATEAU_MIN_PROFILE_VARIANTS);
        assert!(
            profiles
                .iter()
                .all(|profile| profile.structure == SpreadStructure::Wheel)
        );
        assert!(
            profiles.iter().all(|profile| {
                profile.min_trades_per_year >= MIN_WEEKLY_RANKING_TRADES_PER_YEAR
            })
        );
        assert!(profiles.iter().any(
            |profile| profile.name.starts_with("weekly_wheel_disciplined_")
                && profile.min_short_otm_pct.is_some()
        ));
        assert!(profiles.iter().any(|profile| {
            profile.name.starts_with("weekly_wheel_inventory_exit_")
                && profile.covered_call_min_strike_pct_of_assigned < 1.0
        }));
        let guarded_fast = profiles
            .iter()
            .find(|profile| {
                profile.name
                    == "weekly_wheel_guarded_inventory_exit_callfloor95_rv125_dte5_14_delta05_20_otm05_hold21"
            })
            .unwrap();
        assert_eq!(guarded_fast.max_hold_days, Some(21));
        assert_eq!(guarded_fast.covered_call_min_strike_pct_of_assigned, 0.95);
        assert_eq!(guarded_fast.max_realized_vol, Some(1.25));
    }

    #[test]
    fn portfolio_selector_profiles_include_guarded_fast_exit_challenger() {
        let profiles = portfolio_selector_profiles();

        let names = profiles
            .iter()
            .map(|profile| profile.summary_profile.name.as_str())
            .collect::<Vec<_>>();

        assert!(names.contains(&"selector_guarded_fast_exit_wheel_plus_crash_put_and_call_debits"));
        assert!(names.contains(&"selector_guarded_fast_exit_wheel_plus_pullback_put_debit"));
        assert!(names.contains(&"selector_economic_wheel_plus_crash_put_debit"));
        assert!(names.contains(&"selector_economic_wheel_plus_crash_put_and_call_debits"));
        assert!(names.contains(&"selector_credit_put_spread_only"));
        assert!(names.contains(&"selector_farther_otm_credit_put_spread_only"));
        assert!(names.contains(&"selector_trend_credit_put_spread_plus_crash_put_and_call_debits"));
        assert!(names.contains(&"selector_economic_wheel_plus_credit_put_spread_and_debits"));
        assert!(names.contains(&"selector_call_credit_weak_only"));
        assert!(names.contains(&"selector_call_credit_overbought_only"));
        assert!(names.contains(&"selector_call_credit_weak_plus_crash_put_and_call_debits"));
        assert!(names.contains(&"selector_economic_wheel_plus_call_credit_and_debits"));
        assert!(names.contains(&"selector_crash_put_and_call_debits_only"));
        assert!(names.contains(&"selector_crash_put_and_costaware_call_debits_only"));
        assert!(names.contains(&"selector_drawdown_put_and_call_debits_only"));
        assert!(names.contains(&"selector_drawdown_put_and_costaware_call_debits_only"));
        assert!(
            names.contains(&"selector_disciplined_drawdown_put_and_costaware_call_debits_only")
        );
        assert!(names.contains(&"selector_drawdown_put_and_balanced_call_debits_only"));
        assert!(names.contains(&"selector_disciplined_drawdown_put_and_balanced_call_debits_only"));
        assert!(names.contains(&"selector_legguard15_debits_only"));
        assert!(names.contains(&"selector_legguard18_debits_only"));
        assert!(names.contains(&"selector_put_legguard15_and_balanced_call_debits_only"));
        assert!(names.contains(&"selector_disciplined_put_and_call_legguard15_debits_only"));
        assert!(names.contains(&"selector_no_tsla_put_disciplined_call_legguard15_debits_only"));
        assert!(names.contains(&"selector_disciplined_debits_only"));
        assert!(names.contains(&"selector_drawdown_put_debit_only"));
        assert!(names.contains(&"selector_disciplined_drawdown_put_debit_only"));
        assert!(names.contains(&"selector_costaware_call_debit_only"));
        assert!(names.contains(&"selector_disciplined_costaware_call_debit_only"));
        assert!(names.contains(&"selector_balanced_costaware_call_debit_only"));
        assert!(names.contains(&"selector_side_selective_pltr_orcl_wide_call_debit_only"));
        assert!(names.contains(&"selector_side_selective_non_tsla_wide_call_debit_only"));
        assert!(
            names.contains(&"selector_side_selective_pltr_put_plus_pltr_orcl_call_debits_only")
        );
        assert!(names.contains(&"selector_side_selective_pltr_put_plus_non_tsla_call_debits_only"));
        assert!(
            names.contains(
                &"selector_side_selective_pltr_put_plus_non_tsla_call_debits_take33_only"
            )
        );
        assert!(
            names.contains(
                &"selector_side_selective_pltr_put_plus_non_tsla_call_debits_take50_only"
            )
        );
        assert!(names.contains(
            &"selector_side_selective_pltr_put_plus_orcl_costaware_minw1_non_tsla_call_debits_only"
        ));
        assert!(names.contains(
            &"selector_side_selective_pltr_put_plus_orcl_costaware_minw3_non_tsla_call_debits_only"
        ));
        assert!(names.contains(
            &"selector_side_selective_pltr_put_plus_orcl_costaware_minw5_non_tsla_call_debits_only"
        ));
        assert!(names.contains(
            &"selector_side_selective_pltr_put_plus_orcl_costaware_mindebit40_minw1_non_tsla_call_debits_only"
        ));
        assert!(names.contains(
            &"selector_side_selective_pltr_put_plus_orcl_costaware_mindebit35_minw1_non_tsla_call_debits_only"
        ));
        assert!(names.contains(
            &"selector_side_selective_pltr_sofi_put_plus_orcl_costaware_mindebit35_non_tsla_call_debits_only"
        ));
        assert!(names.contains(
            &"selector_side_selective_pltr_sofi_put_plus_orcl_costaware_mindebit35_non_tsla_sofi_call_debits_only"
        ));
        assert!(names.contains(
            &"selector_side_selective_pltr_put_plus_orcl_costaware_mindebit35_legguard_non_tsla_call_debits_only"
        ));
        assert!(names.contains(
            &"selector_side_selective_pltr_put_plus_orcl_costaware_mindebit35_legguard_plus_tsla_call_debits_only"
        ));
        assert!(names.contains(
            &"selector_side_selective_pltr_put_plus_orcl_costaware_mindebit30_minw1_non_tsla_call_debits_only"
        ));
        assert!(names.contains(
            &"selector_side_selective_pltr_put_plus_orcl_costaware_minw1_plus_tsla_call_debits_only"
        ));
        assert!(names.contains(
            &"selector_side_selective_pltr_put_plus_orcl_costaware_minw3_plus_tsla_call_debits_only"
        ));
        assert!(names.contains(&"selector_legguard15_call_debit_only"));
        assert!(names.contains(&"selector_legguard18_call_debit_only"));
        assert!(names.contains(&"selector_call_debit_trend_only"));
        assert!(
            names
                .contains(&"selector_economic_wheel_plus_crash_and_costaware_puts_and_call_debits")
        );
        assert!(
            names.contains(&"selector_economic_wheel_plus_crash_and_pullback_puts_and_call_debits")
        );

        let no_tsla_puts = profiles
            .iter()
            .find(|profile| {
                profile.summary_profile.name
                    == "selector_no_tsla_put_disciplined_call_legguard15_debits_only"
            })
            .unwrap();
        assert!(sleeve_allows_symbol(
            &no_tsla_puts.put_debit_symbols,
            "PLTR"
        ));
        assert!(!sleeve_allows_symbol(
            &no_tsla_puts.put_debit_symbols,
            "TSLA"
        ));
        assert!(sleeve_allows_symbol(
            &no_tsla_puts.call_debit_symbols,
            "TSLA"
        ));

        let side_selective = profiles
            .iter()
            .find(|profile| {
                profile.summary_profile.name
                    == "selector_side_selective_pltr_put_plus_pltr_orcl_call_debits_only"
            })
            .unwrap();
        assert!(sleeve_allows_symbol(
            &side_selective.put_debit_symbols,
            "PLTR"
        ));
        assert!(!sleeve_allows_symbol(
            &side_selective.put_debit_symbols,
            "ORCL"
        ));
        assert!(sleeve_allows_symbol(
            &side_selective.call_debit_symbols,
            "ORCL"
        ));
        assert!(!sleeve_allows_symbol(
            &side_selective.call_debit_symbols,
            "TSLA"
        ));

        let non_tsla = profiles
            .iter()
            .find(|profile| {
                profile.summary_profile.name
                    == "selector_side_selective_pltr_put_plus_non_tsla_call_debits_only"
            })
            .unwrap();
        assert!(sleeve_allows_symbol(&non_tsla.call_debit_symbols, "CRWV"));
        assert!(sleeve_allows_symbol(&non_tsla.call_debit_symbols, "IREN"));
        assert!(!sleeve_allows_symbol(&non_tsla.call_debit_symbols, "TSLA"));

        let non_tsla_take50 = profiles
            .iter()
            .find(|profile| {
                profile.summary_profile.name
                    == "selector_side_selective_pltr_put_plus_non_tsla_call_debits_take50_only"
            })
            .unwrap();
        assert!(sleeve_allows_symbol(
            &non_tsla_take50.put_debit_symbols,
            "PLTR"
        ));
        assert!(!sleeve_allows_symbol(
            &non_tsla_take50.call_debit_symbols,
            "TSLA"
        ));

        let orcl_costaware = profiles
            .iter()
            .find(|profile| {
                profile.summary_profile.name
                    == "selector_side_selective_pltr_put_plus_orcl_costaware_minw1_non_tsla_call_debits_only"
            })
            .unwrap();
        assert!(sleeve_allows_symbol(
            &orcl_costaware.put_debit_symbols,
            "PLTR"
        ));
        assert!(!sleeve_allows_symbol(
            &orcl_costaware.call_debit_symbols,
            "ORCL"
        ));
        assert!(sleeve_allows_symbol(
            &orcl_costaware.call_debit_symbols,
            "CRWV"
        ));
        assert!(sleeve_allows_symbol(
            &orcl_costaware.call_debit_fallback_symbols,
            "ORCL"
        ));
        assert!(!sleeve_allows_symbol(
            &orcl_costaware.call_debit_fallback_symbols,
            "TSLA"
        ));

        let orcl_costaware_tsla = profiles
            .iter()
            .find(|profile| {
                profile.summary_profile.name
                    == "selector_side_selective_pltr_put_plus_orcl_costaware_minw1_plus_tsla_call_debits_only"
            })
            .unwrap();
        assert!(!sleeve_allows_symbol(
            &orcl_costaware_tsla.put_debit_symbols,
            "TSLA"
        ));
        assert!(sleeve_allows_symbol(
            &orcl_costaware_tsla.call_debit_symbols,
            "TSLA"
        ));
        assert!(!sleeve_allows_symbol(
            &orcl_costaware_tsla.call_debit_symbols,
            "ORCL"
        ));
        assert!(sleeve_allows_symbol(
            &orcl_costaware_tsla.call_debit_fallback_symbols,
            "ORCL"
        ));

        let sofi_challenger = profiles
            .iter()
            .find(|profile| {
                profile.summary_profile.name
                    == "selector_side_selective_pltr_sofi_put_plus_orcl_costaware_mindebit35_non_tsla_sofi_call_debits_only"
            })
            .unwrap();
        assert!(sleeve_allows_symbol(
            &sofi_challenger.put_debit_symbols,
            "SOFI"
        ));
        assert!(sleeve_allows_symbol(
            &sofi_challenger.call_debit_symbols,
            "SOFI"
        ));
        assert!(!sleeve_allows_symbol(
            &sofi_challenger.call_debit_symbols,
            "TSLA"
        ));
        assert!(sleeve_allows_symbol(
            &sofi_challenger.call_debit_fallback_symbols,
            "ORCL"
        ));
    }

    #[test]
    fn portfolio_put_sleeve_uses_put_rows_when_call_rows_are_absent() {
        let entry_date = NaiveDate::from_ymd_opt(2026, 1, 6).unwrap();
        let exit_date = entry_date + Duration::days(1);
        let expiration = entry_date + Duration::days(7);
        let mut profile = ResearchProfile::weekly_baseline();
        profile.structure = SpreadStructure::PutCreditSpread;
        profile.min_dte = 1;
        profile.max_dte = 10;
        profile.min_short_delta_abs = 0.10;
        profile.max_short_delta_abs = 0.35;
        profile.max_width = 10.0;
        profile.min_credit_width = 0.05;
        profile.take_profit_pct = 0.33;
        profile.stop_loss_multiple = 2.0;
        profile.trend_lookback_days = None;
        profile.min_underlying_return = None;
        profile.drawdown_lookback_days = None;
        profile.max_underlying_drawdown = None;
        profile.risk_regime_cooldown_guard = None;
        profile.realized_vol_lookback_days = None;
        profile.max_realized_vol = None;
        profile.min_short_otm_pct = None;

        let mut put_rows_by_expiration = BTreeMap::new();
        put_rows_by_expiration.insert(
            expiration,
            vec![
                option_day(entry_date, 100.0, 1.20, 1.30, -0.25, 105.0),
                option_day(entry_date, 95.0, 0.40, 0.45, -0.15, 105.0),
                option_day(exit_date, 100.0, 0.50, 0.60, -0.18, 107.0),
                option_day(exit_date, 95.0, 0.20, 0.25, -0.10, 107.0),
            ],
        );
        let put_lookup = build_lookup(&put_rows_by_expiration);
        let data = PortfolioWheelSymbolData {
            summary: PortfolioWheelLoadedSymbol {
                symbol: "NVDA".to_owned(),
                requested_from: entry_date,
                from: entry_date,
                to: exit_date,
                expirations_discovered: 1,
                expirations_skipped_before_data: 0,
                expirations_loaded: 1,
                put_expirations_loaded: 1,
                call_expirations_loaded: 0,
                rows_loaded: 4,
                expirations_failed: 0,
                expiration_load_failures: Vec::new(),
            },
            put_rows_by_expiration,
            call_rows_by_expiration: BTreeMap::new(),
            call_rows_by_date: BTreeMap::new(),
            put_lookup,
            call_lookup: HashMap::new(),
            call_underlying_by_date: BTreeMap::new(),
        };

        let (opportunities, candidates) = portfolio_spread_opportunities_for_symbol(
            &data,
            &profile,
            entry_date,
            entry_date,
            SpreadStructure::PutCreditSpread,
        );

        assert_eq!(candidates, 1);
        assert_eq!(opportunities.len(), 1);
        assert_eq!(opportunities[0].strategy, SpreadStructure::PutCreditSpread);
        assert!((opportunities[0].trade.pnl - 35.0).abs() < 1e-9);
    }

    #[test]
    fn portfolio_wheel_allocator_rejects_entries_over_capital_budget() {
        let entry = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
        let request = portfolio_wheel_test_request(100_000.0, 1.0, 5, 5);
        let opportunities = vec![
            portfolio_wheel_opportunity("IREN", entry, entry + Duration::days(7), 60_000.0, 600.0),
            portfolio_wheel_opportunity("PLTR", entry, entry + Duration::days(7), 60_000.0, 500.0),
        ];

        let allocation = allocate_portfolio_wheel_opportunities(&opportunities, 2, &request);

        assert_eq!(allocation.trades.len(), 1);
        assert_eq!(allocation.rejected_capital_budget, 1);
        assert_eq!(allocation.max_capital_used, 60_000.0);
    }

    #[test]
    fn portfolio_wheel_allocator_accepts_overlapping_symbols_when_budget_allows() {
        let entry = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
        let request = portfolio_wheel_test_request(130_000.0, 1.0, 5, 5);
        let opportunities = vec![
            portfolio_wheel_opportunity("IREN", entry, entry + Duration::days(7), 60_000.0, 600.0),
            portfolio_wheel_opportunity("PLTR", entry, entry + Duration::days(7), 60_000.0, 500.0),
        ];

        let allocation = allocate_portfolio_wheel_opportunities(&opportunities, 2, &request);

        assert_eq!(allocation.trades.len(), 2);
        assert_eq!(allocation.rejected_capital_budget, 0);
        assert_eq!(allocation.max_capital_used, 120_000.0);
    }

    #[test]
    fn portfolio_risk_summary_attributes_wheel_inventory_losses() {
        let entry = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
        let to_trade = |opportunity: PortfolioWheelOpportunity| PortfolioWheelTrade {
            symbol: opportunity.symbol,
            strategy: opportunity.strategy,
            capital_at_risk: opportunity.capital_at_risk,
            trade: opportunity.trade,
        };

        let expired =
            portfolio_wheel_opportunity("PLTR", entry, entry + Duration::days(7), 10_000.0, 200.0);
        let mut marked = portfolio_wheel_opportunity(
            "PLTR",
            entry + Duration::days(8),
            entry + Duration::days(43),
            10_000.0,
            -450.0,
        );
        marked.trade.exit_reason = "stock_marked_after_calls".to_owned();
        let mut called = portfolio_wheel_opportunity(
            "TSLA",
            entry + Duration::days(9),
            entry + Duration::days(23),
            20_000.0,
            150.0,
        );
        called.trade.exit_reason = "covered_call_assigned".to_owned();
        let mut put_debit = portfolio_wheel_opportunity(
            "TSLA",
            entry + Duration::days(10),
            entry + Duration::days(11),
            800.0,
            800.0,
        );
        put_debit.strategy = SpreadStructure::PutDebitSpread;
        put_debit.trade.width = 14.0;
        put_debit.trade.exit_reason = "take_profit".to_owned();

        let trades = vec![
            to_trade(expired),
            to_trade(marked),
            to_trade(called),
            to_trade(put_debit),
        ];
        let risk = portfolio_wheel_risk_summary(&trades, 100_000.0);

        assert_eq!(risk.wheel_trades, 3);
        assert_eq!(risk.assigned_cycles, 2);
        assert_eq!(risk.called_away_cycles, 1);
        assert_eq!(risk.marked_stock_cycles, 1);
        assert_eq!(risk.marked_stock_loss_cycles, 1);
        assert!((risk.assignment_rate - (2.0 / 3.0)).abs() < 1e-9);
        assert!((risk.wheel_pnl - -100.0).abs() < 1e-9);
        assert!((risk.put_debit_pnl - 800.0).abs() < 1e-9);
        assert!((risk.max_closed_equity_drawdown - 450.0).abs() < 1e-9);
        assert!((risk.max_closed_equity_drawdown_pct_capital - 0.0045).abs() < 1e-9);
        assert!((risk.marked_stock_pnl - -450.0).abs() < 1e-9);
        assert!((risk.worst_marked_stock_loss - -450.0).abs() < 1e-9);
        assert!((risk.worst_trade_loss - -450.0).abs() < 1e-9);

        let strategy_summaries = portfolio_strategy_summaries(&trades);
        let wheel = strategy_summaries
            .iter()
            .find(|summary| summary.strategy == "wheel")
            .unwrap();
        assert_eq!(wheel.trades, 3);
        assert_eq!(wheel.assigned_cycles, 2);
        assert_eq!(wheel.marked_stock_cycles, 1);
        assert!((wheel.pnl - -100.0).abs() < 1e-9);

        let symbol_summaries = portfolio_wheel_symbol_summaries(&trades);
        let tsla = symbol_summaries
            .iter()
            .find(|summary| summary.symbol == "TSLA")
            .unwrap();
        assert_eq!(tsla.trades, 2);
        assert_eq!(tsla.assigned_cycles, 1);
        assert_eq!(avg_wheel_call_count(&trades), 0.0);

        let research_trades = trades
            .iter()
            .map(|trade| trade.trade.clone())
            .collect::<Vec<_>>();
        let metrics = metrics_with_min_trades_per_year(
            &research_trades,
            entry,
            entry + Duration::days(60),
            1.0,
        );
        let decision = portfolio_decision_metrics(&metrics, 20_000.0, 12_000.0, 100_000.0, &risk);
        assert_eq!(decision.professional_risk_flag, "wheel_edge_negative");
        assert!((decision.max_capital_drawdown_pct - 0.0045).abs() < 1e-9);
        assert!(decision.pnl_to_drawdown_capital > 0.0);
        assert!(decision.marked_stock_loss_to_pnl > 0.0);
    }

    #[test]
    fn portfolio_ablations_reallocate_after_removing_symbol() {
        let entry = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
        let request = portfolio_wheel_test_request(100_000.0, 1.0, 1, 1);
        let opportunities = vec![
            portfolio_wheel_opportunity("TSLA", entry, entry + Duration::days(7), 60_000.0, 600.0),
            portfolio_wheel_opportunity("PLTR", entry, entry + Duration::days(7), 60_000.0, 500.0),
        ];

        let allocation = allocate_portfolio_wheel_opportunities(&opportunities, 2, &request);
        assert_eq!(allocation.trades.len(), 1);
        assert_eq!(allocation.trades[0].symbol, "TSLA");

        let ablations = portfolio_ablation_summaries(
            &opportunities,
            entry,
            entry + Duration::days(30),
            100_000.0,
            &request,
        );
        let remove_tsla = ablations
            .iter()
            .find(|ablation| ablation.label == "remove_symbol:TSLA")
            .unwrap();

        assert_eq!(remove_tsla.metrics.trades, 1);
        assert!((remove_tsla.metrics.total_pnl - 500.0).abs() < 1e-9);
    }

    #[test]
    fn portfolio_canary_blocks_negative_strategy_sleeve() {
        let entry = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
        let opportunity =
            portfolio_wheel_opportunity("TSLA", entry, entry + Duration::days(1), 1_000.0, 500.0);
        let trade = PortfolioWheelTrade {
            symbol: opportunity.symbol,
            strategy: SpreadStructure::PutCreditSpread,
            capital_at_risk: opportunity.capital_at_risk,
            trade: opportunity.trade,
        };
        let metrics = metrics_with_min_trades_per_year(
            &[trade.trade.clone()],
            entry,
            entry + Duration::days(30),
            0.1,
        );
        let strategies = vec![PortfolioStrategySummary {
            strategy: "put_credit_spread".to_owned(),
            trades: 10,
            pnl: -1.0,
            ..PortfolioStrategySummary::default()
        }];

        let readiness = portfolio_canary_readiness(
            &metrics,
            &[trade],
            &[],
            &strategies,
            &PortfolioDecisionMetrics::default(),
            true,
        );

        assert_eq!(readiness.status, "blocked");
        assert_eq!(readiness.recommended_capital_fraction, 0.0);
        assert!(
            readiness
                .reason
                .contains("active strategy sleeve put_credit_spread has negative PnL")
        );
    }

    #[test]
    fn portfolio_canary_blocks_high_inventory_risk() {
        let entry = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
        let opportunity =
            portfolio_wheel_opportunity("TSLA", entry, entry + Duration::days(1), 1_000.0, 500.0);
        let trade = PortfolioWheelTrade {
            symbol: opportunity.symbol,
            strategy: SpreadStructure::Wheel,
            capital_at_risk: opportunity.capital_at_risk,
            trade: opportunity.trade,
        };
        let metrics = metrics_with_min_trades_per_year(
            &[trade.trade.clone()],
            entry,
            entry + Duration::days(30),
            0.1,
        );
        let decision_metrics = PortfolioDecisionMetrics {
            professional_risk_flag: "inventory_risk_high".to_owned(),
            marked_stock_loss_to_pnl: 0.44,
            assignment_rate: 0.28,
            ..PortfolioDecisionMetrics::default()
        };

        let readiness =
            portfolio_canary_readiness(&metrics, &[trade], &[], &[], &decision_metrics, true);

        assert_eq!(readiness.status, "blocked");
        assert_eq!(readiness.recommended_capital_fraction, 0.0);
        assert!(readiness.reason.contains("inventory risk high"));
    }

    #[test]
    fn portfolio_wheel_allocator_enforces_symbol_allocation_cap() {
        let entry = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
        let request = portfolio_wheel_test_request(100_000.0, 0.50, 5, 5);
        let opportunities = vec![
            portfolio_wheel_opportunity("TSLA", entry, entry + Duration::days(7), 45_000.0, 600.0),
            portfolio_wheel_opportunity(
                "TSLA",
                entry + Duration::days(1),
                entry + Duration::days(8),
                45_000.0,
                500.0,
            ),
        ];

        let allocation = allocate_portfolio_wheel_opportunities(&opportunities, 2, &request);

        assert_eq!(allocation.trades.len(), 1);
        assert_eq!(allocation.rejected_symbol_allocation, 1);
    }

    #[test]
    fn portfolio_wheel_allocator_enforces_total_symbol_trade_cap() {
        let entry = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
        let mut request = portfolio_wheel_test_request(100_000.0, 1.0, 5, 5);
        request.max_total_trades_per_symbol = Some(1);
        let opportunities = vec![
            portfolio_wheel_opportunity("TSLA", entry, entry + Duration::days(7), 10_000.0, 600.0),
            portfolio_wheel_opportunity(
                "TSLA",
                entry + Duration::days(8),
                entry + Duration::days(15),
                10_000.0,
                500.0,
            ),
            portfolio_wheel_opportunity(
                "PLTR",
                entry + Duration::days(8),
                entry + Duration::days(15),
                10_000.0,
                400.0,
            ),
        ];

        let allocation = allocate_portfolio_wheel_opportunities(&opportunities, 3, &request);

        assert_eq!(allocation.trades.len(), 2);
        assert_eq!(allocation.trades[0].symbol, "TSLA");
        assert_eq!(allocation.trades[1].symbol, "PLTR");
        assert_eq!(allocation.rejected_symbol_total_trades, 1);
    }

    #[test]
    fn portfolio_allocator_pauses_after_realized_drawdown() {
        let entry = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
        let mut request = portfolio_wheel_test_request(100_000.0, 1.0, 5, 5);
        request.portfolio_drawdown_cooldown_trigger_pct = Some(0.01);
        request.portfolio_drawdown_cooldown_days = 5;
        let opportunities = vec![
            portfolio_wheel_opportunity(
                "TSLA",
                entry,
                entry + Duration::days(1),
                1_000.0,
                -1_500.0,
            ),
            portfolio_wheel_opportunity(
                "PLTR",
                entry + Duration::days(2),
                entry + Duration::days(3),
                1_000.0,
                500.0,
            ),
            portfolio_wheel_opportunity(
                "ORCL",
                entry + Duration::days(7),
                entry + Duration::days(8),
                1_000.0,
                400.0,
            ),
        ];

        let allocation = allocate_portfolio_wheel_opportunities(&opportunities, 3, &request);

        assert_eq!(allocation.trades.len(), 2);
        assert_eq!(allocation.trades[0].symbol, "TSLA");
        assert_eq!(allocation.trades[1].symbol, "ORCL");
        assert_eq!(allocation.rejected_portfolio_drawdown_cooldown, 1);
    }

    #[test]
    fn portfolio_allocator_pauses_only_symbol_after_realized_drawdown() {
        let entry = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
        let mut request = portfolio_wheel_test_request(100_000.0, 0.20, 5, 5);
        request.symbol_drawdown_cooldown_trigger_pct = Some(0.05);
        request.symbol_drawdown_cooldown_days = 5;
        let opportunities = vec![
            portfolio_wheel_opportunity(
                "TSLA",
                entry,
                entry + Duration::days(1),
                1_000.0,
                -1_100.0,
            ),
            portfolio_wheel_opportunity(
                "TSLA",
                entry + Duration::days(2),
                entry + Duration::days(3),
                1_000.0,
                500.0,
            ),
            portfolio_wheel_opportunity(
                "PLTR",
                entry + Duration::days(2),
                entry + Duration::days(3),
                1_000.0,
                400.0,
            ),
            portfolio_wheel_opportunity(
                "TSLA",
                entry + Duration::days(7),
                entry + Duration::days(8),
                1_000.0,
                300.0,
            ),
        ];

        let allocation = allocate_portfolio_wheel_opportunities(&opportunities, 4, &request);

        assert_eq!(allocation.trades.len(), 3);
        assert_eq!(allocation.trades[0].symbol, "TSLA");
        assert_eq!(allocation.trades[1].symbol, "PLTR");
        assert_eq!(allocation.trades[2].symbol, "TSLA");
        assert_eq!(allocation.rejected_symbol_drawdown_cooldown, 1);
        assert_eq!(allocation.rejected_portfolio_drawdown_cooldown, 0);
    }

    #[test]
    fn portfolio_wheel_gate_blocks_short_history_samples() {
        let mut trades = Vec::new();
        for idx in 0..10 {
            let entry = NaiveDate::from_ymd_opt(2025, 1, 10).unwrap() + Duration::days(idx * 14);
            trades.push(
                portfolio_wheel_opportunity(
                    "IREN",
                    entry,
                    entry + Duration::days(7),
                    10_000.0,
                    300.0,
                )
                .trade,
            );
        }
        for idx in 0..10 {
            let entry = NaiveDate::from_ymd_opt(2026, 1, 10).unwrap() + Duration::days(idx * 14);
            trades.push(
                portfolio_wheel_opportunity(
                    "PLTR",
                    entry,
                    entry + Duration::days(7),
                    10_000.0,
                    300.0,
                )
                .trade,
            );
        }
        let metrics = metrics_with_min_trades_per_year(
            &trades,
            NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2026, 6, 30).unwrap(),
            1.0,
        );

        let (status, pass, reason) = portfolio_wheel_gate(&metrics, 100_000.0);

        assert_eq!(status, "blocked");
        assert!(!pass);
        assert!(reason.contains("history gate failed"));
    }

    #[test]
    fn portfolio_wheel_gate_treats_small_negative_years_as_noise() {
        let mut trades = Vec::new();
        for (year, pnl) in [(2024, 500.0), (2025, -50.0), (2026, 500.0)] {
            for idx in 0..4 {
                let entry =
                    NaiveDate::from_ymd_opt(year, 1, 10).unwrap() + Duration::days(idx * 45);
                trades.push(
                    portfolio_wheel_opportunity(
                        "PLTR",
                        entry,
                        entry + Duration::days(7),
                        10_000.0,
                        pnl,
                    )
                    .trade,
                );
            }
        }
        let metrics = metrics_with_min_trades_per_year(
            &trades,
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2026, 12, 31).unwrap(),
            1.0,
        );

        let (_status, _pass, reason) = portfolio_wheel_gate(&metrics, 100_000.0);

        assert!(!reason.contains("annual stability failed"));
    }

    #[test]
    fn symbol_slug_is_filesystem_safe() {
        assert_eq!(symbol_slug("NVDA"), "nvda");
        assert_eq!(symbol_slug("BRK.B"), "brk-b");
    }

    #[test]
    fn strategy_summaries_separate_detector_from_execution_rules() {
        let detector_profile = ResearchProfile::legacy_baseline();
        let mut execution_variant = detector_profile.clone();
        execution_variant.take_profit_pct = 0.35;

        assert_eq!(
            detector_strategy_summary(&detector_profile),
            detector_strategy_summary(&execution_variant)
        );
        assert_ne!(
            execution_strategy_summary(&detector_profile),
            execution_strategy_summary(&execution_variant)
        );
    }

    #[test]
    fn default_execution_summary_matches_frozen_selector() {
        let summary = execution_strategy_summary(&ResearchProfile::baseline());

        assert_eq!(summary.candidate_selector, "farther_otm_then_credit");
        assert!(summary.name.contains("farther_otm"));
    }

    #[test]
    fn underlying_return_uses_only_dates_at_or_before_lookback_target() {
        let mut underlying = BTreeMap::new();
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(), 100.0);
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 8).unwrap(), 120.0);
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 11).unwrap(), 130.0);

        let ret = underlying_return(
            NaiveDate::from_ymd_opt(2026, 1, 11).unwrap(),
            5,
            &underlying,
        )
        .unwrap();

        assert!((ret - 0.3).abs() < 1e-9);
    }

    #[test]
    fn underlying_drawdown_uses_only_current_and_prior_window_prices() {
        let mut underlying = BTreeMap::new();
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(), 100.0);
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 8).unwrap(), 125.0);
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 11).unwrap(), 110.0);
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 12).unwrap(), 200.0);

        let drawdown = underlying_drawdown(
            NaiveDate::from_ymd_opt(2026, 1, 11).unwrap(),
            10,
            &underlying,
        )
        .unwrap();

        assert!((drawdown - 0.12).abs() < 1e-9);
    }

    #[test]
    fn underlying_realized_vol_uses_only_current_and_prior_window_prices() {
        let date = NaiveDate::from_ymd_opt(2026, 1, 4).unwrap();
        let mut underlying = BTreeMap::new();
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(), 100.0);
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), 110.0);
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), 100.0);
        underlying.insert(date, 110.0);
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(), 500.0);

        let realized_vol = underlying_realized_vol(date, 3, &underlying).unwrap();
        let returns = [
            (110.0_f64 / 100.0_f64).ln(),
            (100.0_f64 / 110.0_f64).ln(),
            (110.0_f64 / 100.0_f64).ln(),
        ];
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns
            .iter()
            .map(|ret| {
                let diff = ret - mean;
                diff * diff
            })
            .sum::<f64>()
            / (returns.len() - 1) as f64;
        let expected = variance.sqrt() * 252.0_f64.sqrt();

        assert!((realized_vol - expected).abs() < 1e-9);
    }

    #[test]
    fn entry_regime_rejects_weak_trend_and_too_close_short_strikes() {
        let mut profile = ResearchProfile::legacy_baseline();
        profile.trend_lookback_days = Some(10);
        profile.min_underlying_return = Some(0.0);
        profile.max_underlying_return = Some(0.20);
        profile.min_short_otm_pct = Some(0.08);
        let mut underlying = BTreeMap::new();
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(), 100.0);
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 11).unwrap(), 105.0);
        let passing_short = OptionDay {
            date: NaiveDate::from_ymd_opt(2026, 1, 11).unwrap(),
            strike: 95.0,
            bid: 1.0,
            ask: 1.1,
            delta: -0.25,
            implied_vol: 0.5,
            underlying_price: 105.0,
            open_interest: 1_000,
        };
        let close_short = OptionDay {
            strike: 100.0,
            ..passing_short.clone()
        };

        assert!(entry_regime(&passing_short, &profile, &underlying, OptionRight::Put).is_some());
        assert!(entry_regime(&close_short, &profile, &underlying, OptionRight::Put).is_none());

        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(), 110.0);
        assert!(entry_regime(&passing_short, &profile, &underlying, OptionRight::Put).is_none());

        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(), 80.0);
        assert!(entry_regime(&passing_short, &profile, &underlying, OptionRight::Put).is_none());
    }

    #[test]
    fn entry_regime_rejects_excess_realized_vol() {
        let date = NaiveDate::from_ymd_opt(2026, 1, 4).unwrap();
        let short = option_day(date, 95.0, 1.0, 1.1, -0.25, 110.0);
        let mut underlying = BTreeMap::new();
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(), 100.0);
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), 110.0);
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), 100.0);
        underlying.insert(date, 110.0);
        let mut profile = ResearchProfile::legacy_baseline();
        profile.realized_vol_lookback_days = Some(3);
        profile.max_realized_vol = Some(0.10);

        assert!(entry_regime(&short, &profile, &underlying, OptionRight::Put).is_none());

        profile.max_realized_vol = Some(5.0);
        let regime = entry_regime(&short, &profile, &underlying, OptionRight::Put).unwrap();
        assert!(regime.underlying_realized_vol.unwrap() > 0.0);
    }

    #[test]
    fn entry_regime_rejects_insufficient_realized_vol() {
        let date = NaiveDate::from_ymd_opt(2026, 1, 4).unwrap();
        let short = option_day(date, 95.0, 1.0, 1.1, -0.25, 101.0);
        let mut underlying = BTreeMap::new();
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(), 100.0);
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 2).unwrap(), 100.2);
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 3).unwrap(), 100.4);
        underlying.insert(date, 101.0);
        let mut profile = ResearchProfile::legacy_baseline();
        profile.realized_vol_lookback_days = Some(3);
        profile.min_realized_vol = Some(0.50);

        assert!(entry_regime(&short, &profile, &underlying, OptionRight::Put).is_none());

        profile.min_realized_vol = Some(0.01);
        let regime = entry_regime(&short, &profile, &underlying, OptionRight::Put).unwrap();
        assert!(regime.underlying_realized_vol.unwrap() >= 0.01);
    }

    #[test]
    fn entry_regime_rejects_excess_recent_drawdown() {
        let mut profile = ResearchProfile::legacy_baseline();
        profile.drawdown_lookback_days = Some(10);
        profile.max_underlying_drawdown = Some(0.10);
        let mut underlying = BTreeMap::new();
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(), 100.0);
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 8).unwrap(), 120.0);
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 11).unwrap(), 105.0);
        let short = option_day(
            NaiveDate::from_ymd_opt(2026, 1, 11).unwrap(),
            95.0,
            1.0,
            1.1,
            -0.25,
            105.0,
        );

        assert!(entry_regime(&short, &profile, &underlying, OptionRight::Put).is_none());

        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 11).unwrap(), 110.0);
        let regime = entry_regime(&short, &profile, &underlying, OptionRight::Put).unwrap();
        assert!((regime.underlying_recent_drawdown.unwrap() - (10.0 / 120.0)).abs() < 1e-9);
    }

    #[test]
    fn entry_regime_rejects_insufficient_recent_drawdown() {
        let mut profile = ResearchProfile::legacy_baseline();
        profile.drawdown_lookback_days = Some(10);
        profile.min_underlying_drawdown = Some(0.05);
        let mut underlying = BTreeMap::new();
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(), 100.0);
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 8).unwrap(), 120.0);
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 11).unwrap(), 118.0);
        let short = option_day(
            NaiveDate::from_ymd_opt(2026, 1, 11).unwrap(),
            95.0,
            1.0,
            1.1,
            -0.25,
            118.0,
        );

        assert!(entry_regime(&short, &profile, &underlying, OptionRight::Put).is_none());

        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 11).unwrap(), 112.0);
        let regime = entry_regime(&short, &profile, &underlying, OptionRight::Put).unwrap();
        assert!((regime.underlying_recent_drawdown.unwrap() - (8.0 / 120.0)).abs() < 1e-9);
    }

    #[test]
    fn entry_regime_accepts_return_or_drawdown_confirmation() {
        let date = NaiveDate::from_ymd_opt(2026, 1, 11).unwrap();
        let short = option_day(date, 95.0, 1.0, 1.1, -0.25, 110.0);
        let mut profile = ResearchProfile::legacy_baseline();
        profile.trend_lookback_days = Some(10);
        profile.min_underlying_return = Some(0.05);
        profile.drawdown_lookback_days = Some(10);
        profile.return_or_drawdown_gate = Some(ReturnOrDrawdownGate {
            min_underlying_return: Some(0.20),
            min_underlying_drawdown: Some(0.05),
        });
        let mut underlying = BTreeMap::new();
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(), 100.0);
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 8).unwrap(), 112.0);
        underlying.insert(date, 110.0);

        assert!(entry_regime(&short, &profile, &underlying, OptionRight::Put).is_none());

        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 8).unwrap(), 120.0);
        assert!(entry_regime(&short, &profile, &underlying, OptionRight::Put).is_some());

        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 8).unwrap(), 112.0);
        underlying.insert(date, 125.0);
        let strong_trend_short = option_day(date, 95.0, 1.0, 1.1, -0.25, 125.0);
        assert!(
            entry_regime(&strong_trend_short, &profile, &underlying, OptionRight::Put).is_some()
        );
    }

    #[test]
    fn entry_regime_rejects_strong_trend_pullback_exhaustion() {
        let date = NaiveDate::from_ymd_opt(2026, 1, 11).unwrap();
        let short = option_day(date, 95.0, 1.0, 1.1, -0.25, 130.0);
        let mut profile = ResearchProfile::legacy_baseline();
        profile.trend_lookback_days = Some(10);
        profile.min_underlying_return = Some(0.05);
        profile.drawdown_lookback_days = Some(10);
        profile.trend_drawdown_guard = Some(TrendDrawdownGuard {
            min_underlying_return: 0.20,
            max_underlying_drawdown: 0.03,
        });
        let mut underlying = BTreeMap::new();
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(), 100.0);
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 8).unwrap(), 140.0);
        underlying.insert(date, 130.0);

        assert!(entry_regime(&short, &profile, &underlying, OptionRight::Put).is_none());

        underlying.insert(date, 136.0);
        let shallow_pullback_short = option_day(date, 95.0, 1.0, 1.1, -0.25, 136.0);
        assert!(
            entry_regime(
                &shallow_pullback_short,
                &profile,
                &underlying,
                OptionRight::Put
            )
            .is_some()
        );

        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(), 120.0);
        underlying.insert(date, 130.0);
        let weaker_trend_short = option_day(date, 95.0, 1.0, 1.1, -0.25, 130.0);
        assert!(
            entry_regime(&weaker_trend_short, &profile, &underlying, OptionRight::Put).is_some()
        );
    }

    #[test]
    fn entry_regime_rejects_weak_trend_middle_pullback_band() {
        let date = NaiveDate::from_ymd_opt(2026, 1, 11).unwrap();
        let short = option_day(date, 95.0, 1.0, 1.1, -0.25, 112.0);
        let mut profile = ResearchProfile::legacy_baseline();
        profile.trend_lookback_days = Some(10);
        profile.min_underlying_return = Some(0.05);
        profile.drawdown_lookback_days = Some(10);
        profile.weak_trend_pullback_guard = Some(WeakTrendPullbackGuard {
            max_underlying_return: 0.13,
            min_underlying_drawdown: 0.03,
            max_underlying_drawdown: 0.06,
        });
        let mut underlying = BTreeMap::new();
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(), 100.0);
        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 8).unwrap(), 116.0);
        underlying.insert(date, 112.0);

        assert!(entry_regime(&short, &profile, &underlying, OptionRight::Put).is_none());

        underlying.insert(date, 108.0);
        let deeper_pullback_short = option_day(date, 95.0, 1.0, 1.1, -0.25, 108.0);
        assert!(
            entry_regime(
                &deeper_pullback_short,
                &profile,
                &underlying,
                OptionRight::Put
            )
            .is_some()
        );

        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(), 80.0);
        underlying.insert(date, 112.0);
        assert!(entry_regime(&short, &profile, &underlying, OptionRight::Put).is_some());
    }

    #[test]
    fn candidate_generation_ignores_pre_window_lookback_rows() {
        let expiration = NaiveDate::from_ymd_opt(2026, 2, 15).unwrap();
        let pre_window = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
        let entry_date = NaiveDate::from_ymd_opt(2026, 1, 11).unwrap();
        let mut rows = BTreeMap::new();
        rows.insert(
            expiration,
            vec![
                option_day(pre_window, 95.0, 1.35, 1.40, -0.25, 105.0),
                option_day(pre_window, 90.0, 0.25, 0.30, -0.18, 105.0),
                option_day(entry_date, 95.0, 1.35, 1.40, -0.25, 105.0),
                option_day(entry_date, 90.0, 0.25, 0.30, -0.18, 105.0),
            ],
        );

        let candidates = generate_candidates(
            &rows,
            &ResearchProfile::legacy_baseline(),
            entry_date,
            entry_date,
        );

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].entry_date, entry_date);
    }

    #[test]
    fn candidate_generation_uses_global_underlying_history_for_regime_filters() {
        let prior_expiration = NaiveDate::from_ymd_opt(2026, 1, 16).unwrap();
        let expiration = NaiveDate::from_ymd_opt(2026, 1, 30).unwrap();
        let prior_date = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
        let entry_date = NaiveDate::from_ymd_opt(2026, 1, 20).unwrap();
        let mut rows = BTreeMap::new();
        rows.insert(
            prior_expiration,
            vec![option_day(prior_date, 90.0, 0.40, 0.45, -0.20, 100.0)],
        );
        rows.insert(
            expiration,
            vec![
                option_day(entry_date, 95.0, 1.35, 1.40, -0.25, 110.0),
                option_day(entry_date, 90.0, 0.25, 0.30, -0.18, 110.0),
            ],
        );
        let mut profile = ResearchProfile::weekly_baseline();
        profile.trend_lookback_days = Some(10);
        profile.min_underlying_return = Some(0.05);
        profile.drawdown_lookback_days = None;
        profile.realized_vol_lookback_days = None;

        let candidates = generate_candidates(&rows, &profile, entry_date, entry_date);

        assert_eq!(candidates.len(), 1);
        assert!((candidates[0].underlying_lookback_return.unwrap() - 0.10).abs() < f64::EPSILON);
    }

    #[test]
    fn row_date_rejects_short_timestamp_without_panicking() {
        let row = serde_json::json!({ "timestamp": "bad" });

        assert_eq!(row_date(&row), None);
    }

    #[test]
    fn required_ranking_trades_scale_with_window_length() {
        assert_eq!(
            required_trades_for_ranking(
                NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                NaiveDate::from_ymd_opt(2026, 6, 18).unwrap(),
                MIN_RANKING_TRADES_PER_YEAR,
            ),
            10
        );
        assert_eq!(
            required_trades_for_ranking(
                NaiveDate::from_ymd_opt(2016, 12, 6).unwrap(),
                NaiveDate::from_ymd_opt(2026, 6, 18).unwrap(),
                MIN_RANKING_TRADES_PER_YEAR,
            ),
            20
        );
    }

    #[test]
    fn period_ranking_trades_use_rate_without_full_window_floor() {
        assert_eq!(
            required_period_trades_for_ranking(
                NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                NaiveDate::from_ymd_opt(2024, 12, 31).unwrap(),
                MIN_RANKING_TRADES_PER_YEAR,
            ),
            2
        );
        assert_eq!(
            required_period_trades_for_ranking(
                NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
                NaiveDate::from_ymd_opt(2026, 6, 21).unwrap(),
                MIN_RANKING_TRADES_PER_YEAR,
            ),
            1
        );
        assert_eq!(
            required_trades_for_ranking(
                NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
                NaiveDate::from_ymd_opt(2026, 6, 21).unwrap(),
                MIN_RANKING_TRADES_PER_YEAR,
            ),
            10
        );
    }

    #[test]
    fn weekly_profiles_require_weekly_cadence_for_ranking() {
        let profile = ResearchProfile::weekly_baseline();
        let from = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2026, 6, 30).unwrap();

        let trades = vec![
            trade_with_entry_exit(from, from + Duration::days(3), 40.0),
            trade_with_entry_exit(from + Duration::days(14), from + Duration::days(17), 40.0),
        ];
        let metrics = metrics_for_profile(&trades, from, to, &profile);

        assert_eq!(
            profile.min_trades_per_year,
            MIN_WEEKLY_RANKING_TRADES_PER_YEAR
        );
        assert_eq!(metrics.required_trades, 52);
        assert!(!metrics.ranking_eligible);
    }

    #[test]
    fn loaded_rows_effective_from_uses_first_loaded_expiration_window() {
        let requested_from = NaiveDate::from_ymd_opt(2010, 1, 1).unwrap();
        let expirations = vec![
            NaiveDate::from_ymd_opt(2016, 4, 1).unwrap(),
            NaiveDate::from_ymd_opt(2016, 4, 8).unwrap(),
        ];

        assert_eq!(
            loaded_rows_effective_from(requested_from, 14, expirations.into_iter()),
            NaiveDate::from_ymd_opt(2016, 3, 18).unwrap()
        );
    }

    #[test]
    fn loaded_rows_effective_from_never_moves_before_requested_start() {
        let requested_from = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let expirations = vec![NaiveDate::from_ymd_opt(2026, 1, 9).unwrap()];

        assert_eq!(
            loaded_rows_effective_from(requested_from, 14, expirations.into_iter()),
            requested_from
        );
    }

    #[test]
    fn weekly_far_otm_profiles_are_lower_delta_and_tighter_risk() {
        let profiles = weekly_far_otm_research_profiles();

        assert!(!profiles.is_empty());
        assert!(profiles.iter().all(|profile| profile.max_dte <= 14));
        assert!(
            profiles
                .iter()
                .all(|profile| profile.max_short_delta_abs <= 0.15)
        );
        assert!(
            profiles
                .iter()
                .all(|profile| profile.min_short_delta_abs >= 0.02)
        );
        assert!(
            profiles
                .iter()
                .all(|profile| profile.min_short_otm_pct.is_some())
        );
        assert!(
            profiles
                .iter()
                .all(|profile| profile.stop_loss_multiple <= 1.50)
        );
        assert!(
            profiles
                .iter()
                .all(|profile| profile.min_trades_per_year == MIN_WEEKLY_RANKING_TRADES_PER_YEAR)
        );
        assert!(
            profiles
                .iter()
                .any(|profile| profile.take_profit_pct == 0.25)
        );
        assert!(
            profiles
                .iter()
                .any(|profile| profile.take_profit_pct == 0.33)
        );
    }

    #[test]
    fn chronological_period_metrics_split_by_entry_date() {
        let from = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2021, 12, 31).unwrap();
        let early = trade_with_entry_exit(
            NaiveDate::from_ymd_opt(2020, 2, 1).unwrap(),
            NaiveDate::from_ymd_opt(2020, 2, 10).unwrap(),
            75.0,
        );
        let late = trade_with_entry_exit(
            NaiveDate::from_ymd_opt(2021, 8, 1).unwrap(),
            NaiveDate::from_ymd_opt(2021, 8, 10).unwrap(),
            -25.0,
        );

        let periods = chronological_period_metrics(&[late, early], from, to);

        assert_eq!(periods.len(), 2);
        assert_eq!(periods[0].name, "first_half");
        assert_eq!(periods[0].trades, 1);
        assert_eq!(periods[0].total_pnl, 75.0);
        assert_eq!(periods[1].name, "second_half");
        assert_eq!(periods[1].trades, 1);
        assert_eq!(periods[1].total_pnl, -25.0);
    }

    #[test]
    fn robust_score_uses_weakest_chronological_period() {
        let from = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2020, 12, 31).unwrap();
        let periods = vec![
            test_period_metrics("first_half", from, to, 0.12),
            test_period_metrics("second_half", from, to, -0.05),
        ];

        assert_eq!(robust_score(0.04, &periods), -0.05);
        assert_eq!(robust_score(-0.10, &periods), -0.10);
    }

    #[test]
    fn robust_ranking_requires_positive_eligible_periods() {
        let from = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2020, 12, 31).unwrap();
        let mut periods = vec![
            test_period_metrics("first_half", from, to, 0.12),
            test_period_metrics("second_half", from, to, -0.05),
        ];
        periods[0].total_pnl = 50.0;
        periods[1].total_pnl = 25.0;

        assert!(robust_ranking_eligible(true, 75.0, &periods));

        periods[1].total_pnl = -25.0;
        assert!(!robust_ranking_eligible(true, 25.0, &periods));

        periods[1].total_pnl = 25.0;
        periods[1].ranking_eligible = false;
        assert!(!robust_ranking_eligible(true, 75.0, &periods));
        assert!(!robust_ranking_eligible(false, 75.0, &periods));
        assert!(!robust_ranking_eligible(true, -1.0, &periods));
    }

    #[test]
    fn deployable_training_profile_requires_positive_robust_score() {
        let from = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2021, 12, 31).unwrap();
        let mut trades = Vec::new();
        for month in 1..=10 {
            trades.push(trade_with_entry_exit(
                NaiveDate::from_ymd_opt(2020, month, 1).unwrap(),
                NaiveDate::from_ymd_opt(2020, month, 8).unwrap(),
                20.0,
            ));
        }
        trades.push(trade_with_entry_exit(
            NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2021, 1, 8).unwrap(),
            -300.0,
        ));
        for month in 2..=11 {
            trades.push(trade_with_entry_exit(
                NaiveDate::from_ymd_opt(2021, month, 1).unwrap(),
                NaiveDate::from_ymd_opt(2021, month, 8).unwrap(),
                40.0,
            ));
        }
        let metrics = metrics(&trades, from, to);

        assert!(metrics.robust_ranking_eligible);
        assert!(metrics.robust_score < 0.0);
        assert!(!deployable_training_profile(&metrics));
    }

    #[test]
    fn deployable_training_profile_requires_robust_score_margin() {
        let from = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2022, 12, 31).unwrap();
        let mut metrics = metrics(&training_trades(40.0), from, to);
        metrics.robust_score = MIN_DEPLOYABLE_TRAINING_ROBUST_SCORE / 2.0;

        assert!(metrics.robust_score > 0.0);
        assert!(metrics.robust_ranking_eligible);
        assert!(!deployable_training_profile(&metrics));

        metrics.robust_score = MIN_DEPLOYABLE_TRAINING_ROBUST_SCORE;
        assert!(deployable_training_profile(&metrics));
    }

    #[test]
    fn train_metrics_summary_reports_robust_score_gate() {
        let from = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let train_to = NaiveDate::from_ymd_opt(2022, 12, 31).unwrap();
        let trades = training_trades(40.0);
        let mut metrics = metrics(&trades, from, train_to);
        metrics.robust_score = MIN_DEPLOYABLE_TRAINING_ROBUST_SCORE / 2.0;

        let summary = train_metrics_summary(&metrics, &trades, train_to);

        assert_eq!(
            summary.min_deployable_robust_score,
            MIN_DEPLOYABLE_TRAINING_ROBUST_SCORE
        );
        assert!(!summary.robust_score_gate);
    }

    #[test]
    fn expiration_load_failure_keeps_expiration_and_compacts_error() {
        let expiration = NaiveDate::from_ymd_opt(2012, 6, 1).unwrap();
        let error = anyhow::anyhow!("ThetaData 403\nsubscription required");

        let failure = expiration_load_failure_from_error(expiration, &error);

        assert_eq!(failure.expiration, expiration);
        assert_eq!(failure.message, "ThetaData 403 | subscription required");
        assert_eq!(markdown_cell("a | b"), "a \\| b");
    }

    #[test]
    fn subscription_denied_errors_are_non_retryable() {
        let denied = anyhow::anyhow!(
            "ThetaData returned HTTP 403 Forbidden for url: Requesting options history requiring a PROFESSIONAL subscription, but you only have a STANDARD subscription"
        );
        let transient = anyhow::anyhow!("ThetaData returned HTTP 500 Internal Server Error");

        assert!(is_non_retryable_thetadata_error(&denied));
        assert!(!is_non_retryable_thetadata_error(&transient));
    }

    #[test]
    fn out_of_sample_gate_requires_positive_score() {
        let from = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2022, 12, 31).unwrap();
        let passing = metrics(&training_trades(40.0), from, to);

        assert!(out_of_sample_gate_passes(&passing));

        let mut blocked = passing.clone();
        blocked.score = -0.01;
        assert!(!out_of_sample_gate_passes(&blocked));

        blocked = passing.clone();
        blocked.total_pnl = -1.0;
        assert!(!out_of_sample_gate_passes(&blocked));

        blocked = passing;
        blocked.ranking_eligible = false;
        assert!(!out_of_sample_gate_passes(&blocked));
    }

    #[test]
    fn research_markdown_marks_signal_research_only_when_oos_gate_fails() {
        let from = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2022, 12, 31).unwrap();
        let best = profile_result("best", training_trades(40.0), from, to);
        assert!(deployable_training_profile(&best.metrics));

        let passing_oos = metrics(&training_trades(40.0), from, to);
        let mut blocked_oos = passing_oos.clone();
        blocked_oos.score = -0.01;
        let deployment_gate = DeploymentGate {
            status: "blocked".to_owned(),
            pass: false,
            best_profile_gate: true,
            walk_forward_oos_gate: false,
            holdout_oos_gate: true,
        };
        let plateau_status = plateau_status_from_counts(1, 0, true, &deployment_gate);
        let mut report = ResearchReport {
            run_id: "test-run".to_owned(),
            symbol: "NVDA".to_owned(),
            profile_family: ResearchProfileFamily::Swing,
            requested_from: NaiveDate::from_ymd_opt(2012, 1, 1).unwrap(),
            from,
            to,
            expirations_discovered: 1,
            expirations_skipped_before_data: 2,
            expirations_loaded: 1,
            expirations_failed: 0,
            expiration_load_failures: Vec::new(),
            rows_loaded: 2,
            latest_signal: Some(test_signal(to, "best")),
            deployment_gate,
            plateau_status,
            walk_forward: WalkForwardResult {
                mode: "expanding".to_owned(),
                min_train_days: WALK_FORWARD_MIN_TRAIN_DAYS,
                train_window_days: None,
                years: Vec::new(),
                selected_profile_counts: BTreeMap::new(),
                trades: Vec::new(),
                metrics: blocked_oos.clone(),
            },
            rolling_walk_forward: WalkForwardResult {
                mode: "rolling".to_owned(),
                min_train_days: ROLLING_WALK_FORWARD_TRAIN_DAYS,
                train_window_days: Some(ROLLING_WALK_FORWARD_TRAIN_DAYS),
                years: Vec::new(),
                selected_profile_counts: BTreeMap::new(),
                trades: Vec::new(),
                metrics: blocked_oos,
            },
            holdout: HoldoutResult {
                train_from: from,
                train_to: from,
                test_from: from,
                test_to: to,
                active: true,
                selected_profile: "best".to_owned(),
                train_metrics: train_metrics_summary(&best.metrics, &best.trades, from),
                trades: Vec::new(),
                metrics: passing_oos,
            },
            fixed_profile_walk_forward: Vec::new(),
            profiles: vec![best],
        };

        let markdown = research_markdown(&report);

        assert!(markdown.contains("## Research Deployment Gate"));
        assert!(markdown.contains("## Professional Options Review"));
        assert!(markdown.contains("## Out-of-Sample Failure Summary"));
        assert!(markdown.contains("## Detector Robustness Gap"));
        assert!(markdown.contains("## Planned Universe Expansion"));
        assert!(markdown.contains("Current state: `locked`"));
        assert!(markdown.contains("Required deployable robust score"));
        assert!(markdown.contains("Weakest chronological period"));
        assert!(markdown.contains("## Best Profile Failure Anatomy"));
        assert!(markdown.contains("| Winners |"));
        assert!(markdown.contains("| Losers |"));
        assert!(markdown.contains("Inactive walk-forward years from weak train edge: `0`"));
        assert!(markdown.contains("Holdout active: `yes`"));
        assert!(markdown.contains("Requested window: `2012-01-01` to `2022-12-31`"));
        assert!(markdown.contains("Effective research window: `2020-01-01` to `2022-12-31`"));
        assert!(markdown.contains("Expirations skipped before data: `2`"));
        assert!(markdown.contains("- Status: `blocked`"));
        assert!(markdown.contains("Action readiness: `research-only`"));
        assert!(markdown.contains("Decision:"));
        assert!(markdown.contains("- Research deployment gate: `blocked`"));
        assert!(!markdown.contains("Universe research command"));
        assert!(markdown.contains("latest signals are research candidates only"));

        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["deployment_gate"]["status"], "blocked");
        assert_eq!(json["deployment_gate"]["pass"], false);
        assert_eq!(json["plateau_status"]["status"], "continue_symbol_research");

        let expansion_gate = DeploymentGate {
            status: "blocked".to_owned(),
            pass: false,
            best_profile_gate: true,
            walk_forward_oos_gate: false,
            holdout_oos_gate: false,
        };
        report.deployment_gate = expansion_gate.clone();
        report.plateau_status = plateau_status_from_counts(
            PLATEAU_MIN_PROFILE_VARIANTS + 1,
            PLATEAU_MIN_WALK_FORWARD_YEARS,
            true,
            &expansion_gate,
        );

        let expansion_markdown = research_markdown(&report);

        assert!(expansion_markdown.contains(
            &format!(
                "Universe research command: `cargo run --release -- research-universe --plateau-run runs/test-run/research.json --symbols {} --from 2012-01-01 --to 2022-12-31`",
                DEFAULT_PLATEAU_UNIVERSE_SYMBOLS_CSV
            )
        ));
    }

    #[test]
    fn plateau_status_expands_universe_after_broad_blocked_oos_search() {
        let gate = DeploymentGate {
            status: "blocked".to_owned(),
            pass: false,
            best_profile_gate: true,
            walk_forward_oos_gate: false,
            holdout_oos_gate: false,
        };

        let status = plateau_status_from_counts(
            PLATEAU_MIN_PROFILE_VARIANTS + 1,
            PLATEAU_MIN_WALK_FORWARD_YEARS,
            true,
            &gate,
        );

        assert_eq!(status.status, "plateau_expand_universe");
        assert!(status.expansion_ready);
        assert_eq!(
            status.profile_variants_evaluated,
            PLATEAU_MIN_PROFILE_VARIANTS
        );
        assert_eq!(status.detector_status, "robust");
        assert_eq!(status.execution_strategy_status, "oos_blocked");
        assert!(
            status
                .next_action
                .contains("default liquid single-stock universe")
        );
    }

    #[test]
    fn plateau_status_expands_universe_when_holdout_is_inactive_after_broad_search() {
        let gate = DeploymentGate {
            status: "blocked".to_owned(),
            pass: false,
            best_profile_gate: true,
            walk_forward_oos_gate: false,
            holdout_oos_gate: false,
        };

        let status = plateau_status_from_counts(
            PLATEAU_MIN_PROFILE_VARIANTS + 32,
            PLATEAU_MIN_WALK_FORWARD_YEARS + 4,
            false,
            &gate,
        );

        assert_eq!(status.status, "plateau_expand_universe");
        assert!(status.expansion_ready);
        assert_eq!(status.detector_status, "robust");
        assert_eq!(status.execution_strategy_status, "oos_blocked");
    }

    #[test]
    fn plateau_status_prioritizes_detector_gate_before_oos_coverage() {
        let gate = DeploymentGate {
            status: "blocked".to_owned(),
            pass: false,
            best_profile_gate: false,
            walk_forward_oos_gate: false,
            holdout_oos_gate: false,
        };

        let status = plateau_status_from_counts(
            PLATEAU_MIN_PROFILE_VARIANTS + 1,
            PLATEAU_MIN_WALK_FORWARD_YEARS,
            false,
            &gate,
        );

        assert_eq!(status.status, "continue_symbol_research");
        assert!(!status.expansion_ready);
        assert_eq!(status.detector_status, "blocked");
        assert_eq!(
            status.reason,
            "no robust in-sample detector is available yet"
        );
        assert_eq!(
            status.next_action,
            "continue current-symbol detector search"
        );
    }

    #[test]
    fn profile_ranking_prefers_simpler_profile_when_metrics_tie() {
        let from = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
        let trades = training_trades(50.0);
        let simple = profile_result("z_simple", trades.clone(), from, to);
        let mut complex = profile_result("a_complex", trades, from, to);
        complex.profile.drawdown_lookback_days = Some(20);
        complex.profile.max_underlying_drawdown = Some(0.12);

        let mut results = [complex, simple];
        results.sort_by(profile_result_order);

        assert_eq!(results[0].profile.name, "z_simple");
    }

    #[test]
    fn profile_ranking_prefers_risk_cooldown_when_training_metrics_tie() {
        let from = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
        let trades = training_trades(50.0);
        let simple = profile_result("a_simple", trades.clone(), from, to);
        let mut cooldown_10d = profile_result("b_cooldown_10d", trades.clone(), from, to);
        cooldown_10d.profile.risk_regime_cooldown_guard = Some(TrendDrawdownGuard {
            min_underlying_return: 0.30,
            max_underlying_drawdown: 0.05,
        });
        cooldown_10d.profile.risk_regime_cooldown_days = 10;
        let mut cooldown_20d = profile_result("c_cooldown_20d", trades, from, to);
        cooldown_20d.profile.risk_regime_cooldown_guard = Some(TrendDrawdownGuard {
            min_underlying_return: 0.30,
            max_underlying_drawdown: 0.05,
        });
        cooldown_20d.profile.risk_regime_cooldown_days = 20;

        let mut results = [simple, cooldown_10d, cooldown_20d];
        results.sort_by(profile_result_order);

        assert_eq!(results[0].profile.name, "c_cooldown_20d");
        assert_eq!(results[1].profile.name, "b_cooldown_10d");
        assert_eq!(results[2].profile.name, "a_simple");
    }

    #[test]
    fn profile_ranking_prefers_primary_score_after_robust_gate() {
        let from = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
        let trades = training_trades(50.0);
        let mut higher_score = profile_result("higher_score", trades.clone(), from, to);
        higher_score.metrics.score = 0.15;
        higher_score.metrics.robust_score = 0.10;
        higher_score.metrics.robust_ranking_eligible = true;
        let mut higher_robust_score = profile_result("higher_robust", trades, from, to);
        higher_robust_score.metrics.score = 0.13;
        higher_robust_score.metrics.robust_score = 0.12;
        higher_robust_score.metrics.robust_ranking_eligible = true;

        let mut results = [higher_robust_score, higher_score];
        results.sort_by(profile_result_order);

        assert_eq!(results[0].profile.name, "higher_score");
    }

    #[test]
    fn cost_stress_subtracts_cost_from_each_trade() {
        let win = trade_with_entry_exit(
            NaiveDate::from_ymd_opt(2020, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2020, 1, 9).unwrap(),
            20.0,
        );
        let small_win = trade_with_entry_exit(
            NaiveDate::from_ymd_opt(2020, 1, 10).unwrap(),
            NaiveDate::from_ymd_opt(2020, 1, 17).unwrap(),
            5.0,
        );

        let stress = cost_stress_metric(&[small_win, win], 10.0);

        assert_eq!(stress.trades, 2);
        assert_eq!(stress.total_pnl, 5.0);
        assert_eq!(stress.win_rate, 0.5);
        assert!(stress.profit_factor > 0.0);
    }

    #[test]
    fn annual_stability_tracks_positive_and_worst_years() {
        let yearly = BTreeMap::from([
            (
                2020,
                YearMetrics {
                    trades: 3,
                    pnl: 120.0,
                    win_rate: 1.0,
                    avg_return_on_risk: 0.10,
                },
            ),
            (
                2021,
                YearMetrics {
                    trades: 2,
                    pnl: -40.0,
                    win_rate: 0.5,
                    avg_return_on_risk: -0.05,
                },
            ),
            (
                2022,
                YearMetrics {
                    trades: 1,
                    pnl: 0.0,
                    win_rate: 0.0,
                    avg_return_on_risk: 0.0,
                },
            ),
        ]);

        let stability = annual_stability_metrics(&yearly);

        assert_eq!(stability.active_years, 3);
        assert_eq!(stability.positive_years, 1);
        assert_eq!(stability.negative_years, 1);
        assert_eq!(stability.positive_year_rate, 1.0 / 3.0);
        assert_eq!(stability.worst_year, Some(2021));
        assert_eq!(stability.worst_year_pnl, -40.0);
        assert_eq!(stability.worst_year_avg_return_on_risk, -0.05);
        assert_eq!(stability.best_year, Some(2020));
        assert_eq!(stability.best_year_pnl, 120.0);
    }

    #[test]
    fn iv_filter_rejects_short_leg_above_cap() {
        let mut profile = ResearchProfile::legacy_baseline();
        profile.max_short_iv = Some(0.8);
        let mut row = option_day(
            NaiveDate::from_ymd_opt(2026, 1, 11).unwrap(),
            95.0,
            1.0,
            1.1,
            -0.25,
            105.0,
        );

        row.implied_vol = 0.75;
        assert!(iv_allowed(&row, &profile));

        row.implied_vol = 0.95;
        assert!(!iv_allowed(&row, &profile));
    }

    #[test]
    fn iv_skew_filter_requires_long_iv_above_short_iv() {
        let mut profile = ResearchProfile::legacy_baseline();
        profile.min_long_short_iv_diff = Some(0.005);
        let short = option_day(
            NaiveDate::from_ymd_opt(2026, 1, 11).unwrap(),
            95.0,
            1.0,
            1.1,
            -0.25,
            105.0,
        );
        let mut long = option_day(
            NaiveDate::from_ymd_opt(2026, 1, 11).unwrap(),
            90.0,
            0.1,
            0.2,
            -0.15,
            105.0,
        );

        let mut short = short;
        short.implied_vol = 0.40;
        long.implied_vol = 0.406;
        assert!(iv_skew_allowed(&short, &long, &profile));

        long.implied_vol = 0.404;
        assert!(!iv_skew_allowed(&short, &long, &profile));

        long.implied_vol = 0.0;
        assert!(!iv_skew_allowed(&short, &long, &profile));
    }

    #[test]
    fn low_delta_width_cap_only_rejects_wide_low_delta_spreads() {
        let mut profile = ResearchProfile::legacy_baseline();
        profile.max_width = 15.0;
        profile.low_delta_width_cap_delta_abs = Some(0.23);
        profile.low_delta_width_cap = Some(10.0);

        assert!(width_allowed(15.0, 0.24, &profile));
        assert!(width_allowed(10.0, 0.22, &profile));
        assert!(!width_allowed(15.0, 0.22, &profile));
        assert!(!width_allowed(20.0, 0.24, &profile));
    }

    #[test]
    fn max_hold_exit_triggers_after_configured_days() {
        let entry_date = NaiveDate::from_ymd_opt(2026, 1, 5).unwrap();
        let candidate = candidate_for_ordering(entry_date, 95.0, 90.0, 1.0, -0.25, 0.05);
        let mut rows_by_expiration = BTreeMap::new();
        rows_by_expiration.insert(
            candidate.expiration,
            vec![
                option_day(
                    entry_date + Duration::days(1),
                    95.0,
                    1.10,
                    1.20,
                    -0.25,
                    100.0,
                ),
                option_day(
                    entry_date + Duration::days(1),
                    90.0,
                    0.05,
                    0.20,
                    -0.15,
                    100.0,
                ),
                option_day(
                    entry_date + Duration::days(2),
                    95.0,
                    1.10,
                    1.20,
                    -0.25,
                    100.0,
                ),
                option_day(
                    entry_date + Duration::days(2),
                    90.0,
                    0.05,
                    0.20,
                    -0.15,
                    100.0,
                ),
                option_day(
                    entry_date + Duration::days(3),
                    95.0,
                    1.10,
                    1.20,
                    -0.25,
                    100.0,
                ),
                option_day(
                    entry_date + Duration::days(3),
                    90.0,
                    0.05,
                    0.20,
                    -0.15,
                    100.0,
                ),
            ],
        );
        let lookup = build_lookup(&rows_by_expiration);
        let mut profile = ResearchProfile::legacy_baseline();
        profile.max_hold_days = Some(3);

        let trade = simulate_candidate(&candidate, &lookup, &profile).unwrap();

        assert_eq!(trade.exit_date, entry_date + Duration::days(3));
        assert_eq!(trade.days_held, 3);
        assert_eq!(trade.exit_reason, "max_hold");
        assert!((trade.exit_debit - 1.15).abs() < 1e-9);
    }

    #[test]
    fn early_take_profit_profiles_keep_current_best_entry_gates() {
        let profiles = research_profiles();
        let take35 = profiles
            .iter()
            .find(|profile| {
                profile.name
                    == "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_take35_delta20_30_credit20"
            })
            .unwrap();
        let take40 = profiles
            .iter()
            .find(|profile| {
                profile.name
                    == "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_take40_delta20_30_credit20"
            })
            .unwrap();

        for profile in [take35, take40] {
            assert_eq!(profile.trend_lookback_days, Some(60));
            assert_eq!(profile.min_underlying_return, Some(0.05));
            assert_eq!(profile.max_short_iv, Some(0.45));
            assert_eq!(profile.max_width, 15.0);
            assert!(profile.prefer_farther_otm);
            assert_eq!(profile.stop_loss_cooldown_days, 10);
        }
        assert_eq!(take35.take_profit_pct, 0.35);
        assert_eq!(take40.take_profit_pct, 0.40);
    }

    #[test]
    fn tighter_iv_profiles_keep_current_best_entry_gates() {
        let profiles = research_profiles();
        let iv42 = profiles
            .iter()
            .find(|profile| {
                profile.name
                    == "select_farther_otm_cooldown10_trend60d_min5_ivcap42_width15_delta20_30_credit20"
            })
            .unwrap();
        let iv40 = profiles
            .iter()
            .find(|profile| {
                profile.name
                    == "select_farther_otm_cooldown10_trend60d_min5_ivcap40_width15_delta20_30_credit20"
            })
            .unwrap();

        for profile in [iv42, iv40] {
            assert_eq!(profile.trend_lookback_days, Some(60));
            assert_eq!(profile.min_underlying_return, Some(0.05));
            assert_eq!(profile.max_width, 15.0);
            assert!(profile.prefer_farther_otm);
            assert_eq!(profile.stop_loss_cooldown_days, 10);
        }
        assert_eq!(iv42.max_short_iv, Some(0.42));
        assert_eq!(iv40.max_short_iv, Some(0.40));
    }

    #[test]
    fn higher_delta_floor_profiles_keep_current_best_entry_gates() {
        let profiles = research_profiles();
        for (name, min_delta) in [
            (
                "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_delta23_30_credit20",
                0.23,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_delta24_30_credit20",
                0.24,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_delta26_30_credit20",
                0.26,
            ),
        ] {
            let profile = profiles
                .iter()
                .find(|profile| profile.name == name)
                .unwrap();
            assert_eq!(profile.trend_lookback_days, Some(60));
            assert_eq!(profile.min_underlying_return, Some(0.05));
            assert_eq!(profile.max_short_iv, Some(0.45));
            assert_eq!(profile.max_width, 15.0);
            assert!(profile.prefer_farther_otm);
            assert_eq!(profile.stop_loss_cooldown_days, 10);
            assert_eq!(profile.min_short_delta_abs, min_delta);
            assert_eq!(profile.max_short_delta_abs, 0.30);
        }
    }

    #[test]
    fn low_delta_width_cap_profiles_keep_current_best_entry_gates() {
        let profiles = research_profiles();
        for (name, delta_threshold) in [
            (
                "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.23,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta24_width10_delta20_30_credit20",
                0.24,
            ),
        ] {
            let profile = profiles
                .iter()
                .find(|profile| profile.name == name)
                .unwrap();
            assert_eq!(profile.trend_lookback_days, Some(60));
            assert_eq!(profile.min_underlying_return, Some(0.05));
            assert_eq!(profile.max_short_iv, Some(0.45));
            assert_eq!(profile.max_width, 15.0);
            assert!(profile.prefer_farther_otm);
            assert_eq!(profile.stop_loss_cooldown_days, 10);
            assert_eq!(profile.low_delta_width_cap_delta_abs, Some(delta_threshold));
            assert_eq!(profile.low_delta_width_cap, Some(10.0));
        }
    }

    #[test]
    fn stronger_trend_low_delta_width_cap_profiles_keep_current_best_risk_gates() {
        let profiles = research_profiles();
        for (name, min_underlying_return) in [
            (
                "select_farther_otm_cooldown10_trend60d_min8_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.08,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min10_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.10,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min15_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.15,
            ),
        ] {
            let profile = profiles
                .iter()
                .find(|profile| profile.name == name)
                .unwrap();
            assert_eq!(profile.trend_lookback_days, Some(60));
            assert_eq!(profile.min_underlying_return, Some(min_underlying_return));
            assert_eq!(profile.max_short_iv, Some(0.45));
            assert_eq!(profile.max_width, 15.0);
            assert!(profile.prefer_farther_otm);
            assert_eq!(profile.stop_loss_cooldown_days, 10);
            assert_eq!(profile.low_delta_width_cap_delta_abs, Some(0.23));
            assert_eq!(profile.low_delta_width_cap, Some(10.0));
        }
    }

    #[test]
    fn max_hold_profiles_keep_current_best_risk_gates() {
        let profiles = research_profiles();
        for (name, max_hold_days) in [
            (
                "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_hold7_delta20_30_credit20",
                7,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_hold10_delta20_30_credit20",
                10,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_hold14_delta20_30_credit20",
                14,
            ),
        ] {
            let profile = profiles
                .iter()
                .find(|profile| profile.name == name)
                .unwrap();
            assert_eq!(profile.trend_lookback_days, Some(60));
            assert_eq!(profile.min_underlying_return, Some(0.05));
            assert_eq!(profile.max_short_iv, Some(0.45));
            assert_eq!(profile.max_width, 15.0);
            assert!(profile.prefer_farther_otm);
            assert_eq!(profile.stop_loss_cooldown_days, 10);
            assert_eq!(profile.low_delta_width_cap_delta_abs, Some(0.23));
            assert_eq!(profile.low_delta_width_cap, Some(10.0));
            assert_eq!(profile.max_hold_days, Some(max_hold_days));
        }
    }

    #[test]
    fn min_short_otm_profiles_keep_current_best_risk_gates() {
        let profiles = research_profiles();
        for (name, min_short_otm_pct) in [
            (
                "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_otm6_delta20_30_credit20",
                0.06,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_otm7_delta20_30_credit20",
                0.07,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_otm8_delta20_30_credit20",
                0.08,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_otm9_delta20_30_credit20",
                0.09,
            ),
        ] {
            let profile = profiles
                .iter()
                .find(|profile| profile.name == name)
                .unwrap();
            assert_eq!(profile.trend_lookback_days, Some(60));
            assert_eq!(profile.min_underlying_return, Some(0.05));
            assert_eq!(profile.max_short_iv, Some(0.45));
            assert_eq!(profile.max_width, 15.0);
            assert!(profile.prefer_farther_otm);
            assert_eq!(profile.stop_loss_cooldown_days, 10);
            assert_eq!(profile.low_delta_width_cap_delta_abs, Some(0.23));
            assert_eq!(profile.low_delta_width_cap, Some(10.0));
            assert_eq!(profile.min_short_otm_pct, Some(min_short_otm_pct));
        }
    }

    #[test]
    fn tighter_iv_low_delta_width_cap_profiles_keep_current_best_risk_gates() {
        let profiles = research_profiles();
        for (name, max_short_iv) in [
            (
                "select_farther_otm_cooldown10_trend60d_min5_ivcap42_width15_lowdelta23_width10_delta20_30_credit20",
                0.42,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min5_ivcap40_width15_lowdelta23_width10_delta20_30_credit20",
                0.40,
            ),
        ] {
            let profile = profiles
                .iter()
                .find(|profile| profile.name == name)
                .unwrap();
            assert_eq!(profile.trend_lookback_days, Some(60));
            assert_eq!(profile.min_underlying_return, Some(0.05));
            assert_eq!(profile.max_short_iv, Some(max_short_iv));
            assert_eq!(profile.max_width, 15.0);
            assert!(profile.prefer_farther_otm);
            assert_eq!(profile.stop_loss_cooldown_days, 10);
            assert_eq!(profile.low_delta_width_cap_delta_abs, Some(0.23));
            assert_eq!(profile.low_delta_width_cap, Some(10.0));
        }
    }

    #[test]
    fn max_trend_low_delta_width_cap_profiles_keep_current_best_risk_gates() {
        let profiles = research_profiles();
        for (name, max_return) in [
            (
                "select_farther_otm_cooldown10_trend60d_min5_max25_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.25,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min5_max30_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.30,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min5_max40_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.40,
            ),
        ] {
            let profile = profiles
                .iter()
                .find(|profile| profile.name == name)
                .unwrap();
            assert_eq!(profile.trend_lookback_days, Some(60));
            assert_eq!(profile.min_underlying_return, Some(0.05));
            assert_eq!(profile.max_underlying_return, Some(max_return));
            assert_eq!(profile.max_short_iv, Some(0.45));
            assert_eq!(profile.max_width, 15.0);
            assert!(profile.prefer_farther_otm);
            assert_eq!(profile.stop_loss_cooldown_days, 10);
            assert_eq!(profile.low_delta_width_cap_delta_abs, Some(0.23));
            assert_eq!(profile.low_delta_width_cap, Some(10.0));
        }
    }

    #[test]
    fn short_trend_confirmation_profiles_keep_current_best_risk_gates() {
        let profiles = research_profiles();
        for (name, lookback_days, min_return) in [
            (
                "select_farther_otm_cooldown10_trend20d_min0_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                20,
                0.0,
            ),
            (
                "select_farther_otm_cooldown10_trend20d_min5_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                20,
                0.05,
            ),
            (
                "select_farther_otm_cooldown10_trend10d_min0_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                10,
                0.0,
            ),
            (
                "select_farther_otm_cooldown10_trend10d_min5_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                10,
                0.05,
            ),
        ] {
            let profile = profiles
                .iter()
                .find(|profile| profile.name == name)
                .unwrap();
            assert_eq!(profile.trend_lookback_days, Some(lookback_days));
            assert_eq!(profile.min_underlying_return, Some(min_return));
            assert_eq!(profile.max_short_iv, Some(0.45));
            assert_eq!(profile.max_width, 15.0);
            assert!(profile.prefer_farther_otm);
            assert_eq!(profile.stop_loss_cooldown_days, 10);
            assert_eq!(profile.low_delta_width_cap_delta_abs, Some(0.23));
            assert_eq!(profile.low_delta_width_cap, Some(10.0));
        }
    }

    #[test]
    fn drawdown_cap_profiles_keep_current_best_risk_gates() {
        let profiles = research_profiles();
        for (name, max_drawdown) in [
            (
                "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_dd20d_max8_delta20_30_credit20",
                0.08,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_dd20d_max12_delta20_30_credit20",
                0.12,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_dd20d_max16_delta20_30_credit20",
                0.16,
            ),
        ] {
            let profile = profiles
                .iter()
                .find(|profile| profile.name == name)
                .unwrap();
            assert_eq!(profile.trend_lookback_days, Some(60));
            assert_eq!(profile.min_underlying_return, Some(0.05));
            assert_eq!(profile.max_short_iv, Some(0.45));
            assert_eq!(profile.max_width, 15.0);
            assert!(profile.prefer_farther_otm);
            assert_eq!(profile.stop_loss_cooldown_days, 10);
            assert_eq!(profile.low_delta_width_cap_delta_abs, Some(0.23));
            assert_eq!(profile.low_delta_width_cap, Some(10.0));
            assert_eq!(profile.drawdown_lookback_days, Some(20));
            assert_eq!(profile.max_underlying_drawdown, Some(max_drawdown));
        }
    }

    #[test]
    fn pullback_profiles_keep_current_best_risk_gates() {
        let profiles = research_profiles();
        for (name, min_drawdown) in [
            (
                "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_dd20d_min1_delta20_30_credit20",
                0.01,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_dd20d_min2_delta20_30_credit20",
                0.02,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_dd20d_min3_delta20_30_credit20",
                0.03,
            ),
        ] {
            let profile = profiles
                .iter()
                .find(|profile| profile.name == name)
                .unwrap();
            assert_eq!(profile.trend_lookback_days, Some(60));
            assert_eq!(profile.min_underlying_return, Some(0.05));
            assert_eq!(profile.max_short_iv, Some(0.45));
            assert_eq!(profile.max_width, 15.0);
            assert!(profile.prefer_farther_otm);
            assert_eq!(profile.stop_loss_cooldown_days, 10);
            assert_eq!(profile.low_delta_width_cap_delta_abs, Some(0.23));
            assert_eq!(profile.low_delta_width_cap, Some(10.0));
            assert_eq!(profile.drawdown_lookback_days, Some(20));
            assert_eq!(profile.min_underlying_drawdown, Some(min_drawdown));
        }
    }

    #[test]
    fn return_or_drawdown_profiles_keep_current_best_risk_gates() {
        let profiles = research_profiles();
        for (name, base_min_return, gate_min_return, max_drawdown) in [
            (
                "select_farther_otm_cooldown10_trend60d_min5_trend15_or_dd20d_min2_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.05,
                0.15,
                None,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min5_trend20_or_dd20d_min2_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.05,
                0.20,
                None,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min5_trend25_or_dd20d_min2_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.05,
                0.25,
                None,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min10_trend25_or_dd20d_min2_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.10,
                0.25,
                None,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.12,
                0.25,
                None,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min15_trend25_or_dd20d_min2_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.15,
                0.25,
                None,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2max4p5_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.12,
                0.25,
                Some(0.045),
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2max5_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.12,
                0.25,
                Some(0.05),
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2max6_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.12,
                0.25,
                Some(0.06),
            ),
        ] {
            let profile = profiles
                .iter()
                .find(|profile| profile.name == name)
                .unwrap();
            assert_eq!(profile.trend_lookback_days, Some(60));
            assert_eq!(profile.min_underlying_return, Some(base_min_return));
            assert_eq!(profile.max_short_iv, Some(0.45));
            assert_eq!(profile.max_width, 15.0);
            assert!(profile.prefer_farther_otm);
            assert_eq!(profile.stop_loss_cooldown_days, 10);
            assert_eq!(profile.low_delta_width_cap_delta_abs, Some(0.23));
            assert_eq!(profile.low_delta_width_cap, Some(10.0));
            assert_eq!(profile.drawdown_lookback_days, Some(20));
            assert_eq!(profile.max_underlying_drawdown, max_drawdown);
            assert_eq!(
                profile.return_or_drawdown_gate,
                Some(ReturnOrDrawdownGate {
                    min_underlying_return: Some(gate_min_return),
                    min_underlying_drawdown: Some(0.02),
                })
            );
        }
    }

    #[test]
    fn weak_trend_pullback_profiles_keep_current_best_risk_gates() {
        let profiles = research_profiles();
        for (name, max_guard_return, min_guard_drawdown, max_guard_drawdown) in [
            (
                "select_farther_otm_cooldown10_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.13,
                0.03,
                0.06,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min10_trend25_or_dd20d_min2_weak12dd3to6_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.12,
                0.03,
                0.06,
            ),
        ] {
            let profile = profiles
                .iter()
                .find(|profile| profile.name == name)
                .unwrap();
            assert_eq!(profile.trend_lookback_days, Some(60));
            assert_eq!(profile.min_underlying_return, Some(0.10));
            assert_eq!(profile.max_short_iv, Some(0.45));
            assert_eq!(profile.max_width, 15.0);
            assert!(profile.prefer_farther_otm);
            assert_eq!(profile.stop_loss_cooldown_days, 10);
            assert_eq!(profile.low_delta_width_cap_delta_abs, Some(0.23));
            assert_eq!(profile.low_delta_width_cap, Some(10.0));
            assert_eq!(profile.drawdown_lookback_days, Some(20));
            assert_eq!(
                profile.return_or_drawdown_gate,
                Some(ReturnOrDrawdownGate {
                    min_underlying_return: Some(0.25),
                    min_underlying_drawdown: Some(0.02),
                })
            );
            assert_eq!(
                profile.weak_trend_pullback_guard,
                Some(WeakTrendPullbackGuard {
                    max_underlying_return: max_guard_return,
                    min_underlying_drawdown: min_guard_drawdown,
                    max_underlying_drawdown: max_guard_drawdown,
                })
            );
        }
    }

    #[test]
    fn risk_regime_cooldown_profiles_keep_current_best_risk_gates() {
        let profiles = research_profiles();
        for (name, min_return, max_drawdown, cooldown_days) in [
            (
                "select_farther_otm_cooldown10_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_riskcool30dd5_10d_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.30,
                0.05,
                10,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_riskcool30dd5_20d_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.30,
                0.05,
                20,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_riskcool28dd5_10d_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.28,
                0.05,
                10,
            ),
        ] {
            let profile = profiles
                .iter()
                .find(|profile| profile.name == name)
                .unwrap();
            assert_eq!(profile.trend_lookback_days, Some(60));
            assert_eq!(profile.min_underlying_return, Some(0.10));
            assert_eq!(profile.max_short_iv, Some(0.45));
            assert_eq!(profile.max_width, 15.0);
            assert!(profile.prefer_farther_otm);
            assert_eq!(profile.stop_loss_cooldown_days, 10);
            assert_eq!(profile.low_delta_width_cap_delta_abs, Some(0.23));
            assert_eq!(profile.low_delta_width_cap, Some(10.0));
            assert_eq!(profile.drawdown_lookback_days, Some(20));
            assert_eq!(
                profile.return_or_drawdown_gate,
                Some(ReturnOrDrawdownGate {
                    min_underlying_return: Some(0.25),
                    min_underlying_drawdown: Some(0.02),
                })
            );
            assert_eq!(
                profile.weak_trend_pullback_guard,
                Some(WeakTrendPullbackGuard {
                    max_underlying_return: 0.13,
                    min_underlying_drawdown: 0.03,
                    max_underlying_drawdown: 0.06,
                })
            );
            assert_eq!(
                profile.risk_regime_cooldown_guard,
                Some(TrendDrawdownGuard {
                    min_underlying_return: min_return,
                    max_underlying_drawdown: max_drawdown,
                })
            );
            assert_eq!(profile.risk_regime_cooldown_days, cooldown_days);
        }
    }

    #[test]
    fn aggressive_current_best_ablation_profiles_keep_shared_risk_gates() {
        let profiles = research_profiles();
        for (
            name,
            stop_loss_cooldown_days,
            risk_cooldown_days,
            min_dte,
            max_dte,
            min_delta,
            max_delta,
            min_credit_width,
        ) in [
            (
                "aggr_stopcool1_noriskcool_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                1,
                0,
                30,
                45,
                0.20,
                0.30,
                0.20,
            ),
            (
                "aggr_stopcool3_noriskcool_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                3,
                0,
                30,
                45,
                0.20,
                0.30,
                0.20,
            ),
            (
                "aggr_stopcool5_noriskcool_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                5,
                0,
                30,
                45,
                0.20,
                0.30,
                0.20,
            ),
            (
                "aggr_stopcool10_riskcool30dd5_5d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                10,
                5,
                30,
                45,
                0.20,
                0.30,
                0.20,
            ),
            (
                "aggr_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_ivcap45_width15_lowdelta23_width10_delta25_35_credit20",
                10,
                20,
                30,
                45,
                0.25,
                0.35,
                0.20,
            ),
            (
                "aggr_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_ivcap45_width15_lowdelta23_width10_delta20_35_credit20",
                10,
                20,
                30,
                45,
                0.20,
                0.35,
                0.20,
            ),
            (
                "aggr_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_ivcap45_width15_lowdelta23_width10_delta20_30_credit15",
                10,
                20,
                30,
                45,
                0.20,
                0.30,
                0.15,
            ),
            (
                "aggr_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_ivcap45_width15_lowdelta23_width10_dte21_35_delta20_30_credit20",
                10,
                20,
                21,
                35,
                0.20,
                0.30,
                0.20,
            ),
        ] {
            let profile = profiles
                .iter()
                .find(|profile| profile.name == name)
                .unwrap();
            assert_eq!(profile.stop_loss_cooldown_days, stop_loss_cooldown_days);
            assert_eq!(profile.min_dte, min_dte);
            assert_eq!(profile.max_dte, max_dte);
            assert_eq!(profile.min_short_delta_abs, min_delta);
            assert_eq!(profile.max_short_delta_abs, max_delta);
            assert_eq!(profile.min_credit_width, min_credit_width);
            assert_eq!(profile.trend_lookback_days, Some(60));
            assert_eq!(profile.min_underlying_return, Some(0.10));
            assert_eq!(profile.max_short_iv, Some(0.45));
            assert_eq!(profile.max_width, 15.0);
            assert!(profile.prefer_farther_otm);
            assert_eq!(profile.low_delta_width_cap_delta_abs, Some(0.23));
            assert_eq!(profile.low_delta_width_cap, Some(10.0));
            assert_eq!(profile.drawdown_lookback_days, Some(20));
            assert_eq!(
                profile.return_or_drawdown_gate,
                Some(ReturnOrDrawdownGate {
                    min_underlying_return: Some(0.25),
                    min_underlying_drawdown: Some(0.02),
                })
            );
            assert_eq!(
                profile.weak_trend_pullback_guard,
                Some(WeakTrendPullbackGuard {
                    max_underlying_return: 0.13,
                    min_underlying_drawdown: 0.03,
                    max_underlying_drawdown: 0.06,
                })
            );
            if risk_cooldown_days > 0 {
                assert_eq!(
                    profile.risk_regime_cooldown_guard,
                    Some(TrendDrawdownGuard {
                        min_underlying_return: 0.30,
                        max_underlying_drawdown: 0.05,
                    })
                );
                assert_eq!(profile.risk_regime_cooldown_days, risk_cooldown_days);
            } else {
                assert_eq!(profile.risk_regime_cooldown_guard, None);
                assert_eq!(profile.risk_regime_cooldown_days, 0);
            }
        }
    }

    #[test]
    fn frozen_baseline_matches_accepted_delta26_34_profile() {
        let baseline = ResearchProfile::baseline();

        assert_eq!(baseline.name, FROZEN_BASELINE_NAME);
        assert_eq!(baseline.min_short_delta_abs, 0.26);
        assert_eq!(baseline.max_short_delta_abs, 0.34);
        assert_eq!(baseline.take_profit_pct, 0.45);
        assert_eq!(baseline.stop_loss_cooldown_days, 10);
        assert!(baseline.prefer_farther_otm);
        assert_eq!(
            baseline.weak_trend_pullback_guard,
            Some(WeakTrendPullbackGuard {
                max_underlying_return: 0.13,
                min_underlying_drawdown: 0.02,
                max_underlying_drawdown: 0.05,
            })
        );
    }

    #[test]
    fn frozen_exposure_profiles_keep_frozen_detector_and_vary_heat() {
        let profiles = research_profiles();
        for (suffix, max_concurrent_positions, min_entry_spacing_days) in [
            ("overlap2_gap5", 2, 5),
            ("overlap2_gap7", 2, 7),
            ("overlap3_gap5", 3, 5),
            ("overlap3_gap7", 3, 7),
            ("overlap4_gap5", 4, 5),
        ] {
            let name = format!(
                "frozen_delta26_34_take45_{suffix}_weak13dd2to5_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_width15_lowdelta23_width10_credit20"
            );
            let profile = profiles
                .iter()
                .find(|profile| profile.name == name)
                .unwrap();
            assert_eq!(profile.min_short_delta_abs, 0.26);
            assert_eq!(profile.max_short_delta_abs, 0.34);
            assert_eq!(profile.take_profit_pct, 0.45);
            assert_eq!(profile.max_concurrent_positions, max_concurrent_positions);
            assert_eq!(profile.min_entry_spacing_days, min_entry_spacing_days);
            assert_eq!(profile.trend_lookback_days, Some(60));
            assert_eq!(profile.max_short_iv, Some(0.45));
            assert!(profile.prefer_farther_otm);
        }
    }

    #[test]
    fn overlap_profiles_keep_current_best_detector_and_cap_heat() {
        let profiles = research_profiles();
        for (name, max_concurrent_positions, min_entry_spacing_days) in [
            (
                "overlap2_gap5_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_ivcap45_width15_lowdelta23_width10_delta25_35_credit20",
                2,
                5,
            ),
            (
                "overlap2_gap10_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_ivcap45_width15_lowdelta23_width10_delta25_35_credit20",
                2,
                10,
            ),
            (
                "overlap3_gap7_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_ivcap45_width15_lowdelta23_width10_delta25_35_credit20",
                3,
                7,
            ),
        ] {
            let profile = profiles
                .iter()
                .find(|profile| profile.name == name)
                .unwrap();
            assert_eq!(profile.min_dte, 30);
            assert_eq!(profile.max_dte, 45);
            assert_eq!(profile.min_short_delta_abs, 0.25);
            assert_eq!(profile.max_short_delta_abs, 0.35);
            assert_eq!(profile.min_credit_width, 0.20);
            assert_eq!(profile.stop_loss_cooldown_days, 10);
            assert_eq!(profile.max_concurrent_positions, max_concurrent_positions);
            assert_eq!(profile.min_entry_spacing_days, min_entry_spacing_days);
            assert_eq!(profile.trend_lookback_days, Some(60));
            assert_eq!(profile.min_underlying_return, Some(0.10));
            assert_eq!(profile.max_short_iv, Some(0.45));
            assert_eq!(profile.max_width, 15.0);
            assert!(profile.prefer_farther_otm);
            assert_eq!(profile.low_delta_width_cap_delta_abs, Some(0.23));
            assert_eq!(profile.low_delta_width_cap, Some(10.0));
            assert_eq!(profile.drawdown_lookback_days, Some(20));
            assert_eq!(
                profile.return_or_drawdown_gate,
                Some(ReturnOrDrawdownGate {
                    min_underlying_return: Some(0.25),
                    min_underlying_drawdown: Some(0.02),
                })
            );
            assert_eq!(
                profile.weak_trend_pullback_guard,
                Some(WeakTrendPullbackGuard {
                    max_underlying_return: 0.13,
                    min_underlying_drawdown: 0.03,
                    max_underlying_drawdown: 0.06,
                })
            );
            assert_eq!(
                profile.risk_regime_cooldown_guard,
                Some(TrendDrawdownGuard {
                    min_underlying_return: 0.30,
                    max_underlying_drawdown: 0.05,
                })
            );
            assert_eq!(profile.risk_regime_cooldown_days, 20);
        }
    }

    #[test]
    fn edge_guard_profiles_keep_current_best_detector_except_edge_caps() {
        let profiles = research_profiles();
        for (name, min_dte, max_delta, max_short_iv) in [
            (
                "edgeguard_dte35_delta25_35_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_width15_lowdelta23_width10_credit20",
                35,
                0.35,
                0.45,
            ),
            (
                "edgeguard_dte30_delta25_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_width15_lowdelta23_width10_credit20",
                30,
                0.34,
                0.45,
            ),
            (
                "edgeguard_dte30_delta25_35_ivcap44_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_width15_lowdelta23_width10_credit20",
                30,
                0.35,
                0.44,
            ),
            (
                "edgeguard_dte35_delta25_34_ivcap44_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_width15_lowdelta23_width10_credit20",
                35,
                0.34,
                0.44,
            ),
        ] {
            let profile = profiles
                .iter()
                .find(|profile| profile.name == name)
                .unwrap();
            assert_eq!(profile.min_dte, min_dte);
            assert_eq!(profile.max_dte, 45);
            assert_eq!(profile.min_short_delta_abs, 0.25);
            assert_eq!(profile.max_short_delta_abs, max_delta);
            assert_eq!(profile.max_short_iv, Some(max_short_iv));
            assert_eq!(profile.min_credit_width, 0.20);
            assert_eq!(profile.stop_loss_cooldown_days, 10);
            assert_eq!(profile.max_concurrent_positions, 1);
            assert_eq!(profile.min_entry_spacing_days, 1);
            assert_eq!(profile.trend_lookback_days, Some(60));
            assert_eq!(profile.min_underlying_return, Some(0.10));
            assert_eq!(profile.max_width, 15.0);
            assert!(profile.prefer_farther_otm);
            assert_eq!(profile.low_delta_width_cap_delta_abs, Some(0.23));
            assert_eq!(profile.low_delta_width_cap, Some(10.0));
            assert_eq!(profile.drawdown_lookback_days, Some(20));
            assert_eq!(
                profile.return_or_drawdown_gate,
                Some(ReturnOrDrawdownGate {
                    min_underlying_return: Some(0.25),
                    min_underlying_drawdown: Some(0.02),
                })
            );
            assert_eq!(
                profile.weak_trend_pullback_guard,
                Some(WeakTrendPullbackGuard {
                    max_underlying_return: 0.13,
                    min_underlying_drawdown: 0.03,
                    max_underlying_drawdown: 0.06,
                })
            );
            assert_eq!(
                profile.risk_regime_cooldown_guard,
                Some(TrendDrawdownGuard {
                    min_underlying_return: 0.30,
                    max_underlying_drawdown: 0.05,
                })
            );
            assert_eq!(profile.risk_regime_cooldown_days, 20);
        }
    }

    #[test]
    fn edge_exit_profiles_keep_detector_and_vary_only_exits() {
        let profiles = research_profiles();
        for (name, take_profit_pct, stop_loss_multiple, force_close_dte, max_hold_days) in [
            (
                "edgeexit_take35_dte30_delta25_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_width15_lowdelta23_width10_credit20",
                0.35,
                2.00,
                21,
                None,
            ),
            (
                "edgeexit_take40_dte30_delta25_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_width15_lowdelta23_width10_credit20",
                0.40,
                2.00,
                21,
                None,
            ),
            (
                "edgeexit_take45_dte30_delta25_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_width15_lowdelta23_width10_credit20",
                0.45,
                2.00,
                21,
                None,
            ),
            (
                "edgeexit_take60_dte30_delta25_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_width15_lowdelta23_width10_credit20",
                0.60,
                2.00,
                21,
                None,
            ),
            (
                "edgeexit_stop175_dte30_delta25_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_width15_lowdelta23_width10_credit20",
                0.50,
                1.75,
                21,
                None,
            ),
            (
                "edgeexit_stop150_dte30_delta25_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_width15_lowdelta23_width10_credit20",
                0.50,
                1.50,
                21,
                None,
            ),
            (
                "edgeexit_force28_dte30_delta25_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_width15_lowdelta23_width10_credit20",
                0.50,
                2.00,
                28,
                None,
            ),
            (
                "edgeexit_force25_dte30_delta25_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_width15_lowdelta23_width10_credit20",
                0.50,
                2.00,
                25,
                None,
            ),
            (
                "edgeexit_hold14_dte30_delta25_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_weak13dd3to6_width15_lowdelta23_width10_credit20",
                0.50,
                2.00,
                21,
                Some(14),
            ),
        ] {
            let profile = profiles
                .iter()
                .find(|profile| profile.name == name)
                .unwrap();
            assert_eq!(profile.take_profit_pct, take_profit_pct);
            assert_eq!(profile.stop_loss_multiple, stop_loss_multiple);
            assert_eq!(profile.force_close_dte, force_close_dte);
            assert_eq!(profile.max_hold_days, max_hold_days);
            assert_eq!(profile.min_dte, 30);
            assert_eq!(profile.max_dte, 45);
            assert_eq!(profile.min_short_delta_abs, 0.25);
            assert_eq!(profile.max_short_delta_abs, 0.34);
            assert_eq!(profile.max_short_iv, Some(0.45));
            assert_eq!(profile.min_credit_width, 0.20);
            assert_eq!(profile.stop_loss_cooldown_days, 10);
            assert_eq!(profile.max_concurrent_positions, 1);
            assert_eq!(profile.min_entry_spacing_days, 1);
            assert_eq!(profile.trend_lookback_days, Some(60));
            assert_eq!(profile.min_underlying_return, Some(0.10));
            assert_eq!(profile.max_width, 15.0);
            assert!(profile.prefer_farther_otm);
            assert_eq!(profile.low_delta_width_cap_delta_abs, Some(0.23));
            assert_eq!(profile.low_delta_width_cap, Some(10.0));
            assert_eq!(profile.drawdown_lookback_days, Some(20));
            assert_eq!(
                profile.return_or_drawdown_gate,
                Some(ReturnOrDrawdownGate {
                    min_underlying_return: Some(0.25),
                    min_underlying_drawdown: Some(0.02),
                })
            );
            assert_eq!(
                profile.weak_trend_pullback_guard,
                Some(WeakTrendPullbackGuard {
                    max_underlying_return: 0.13,
                    min_underlying_drawdown: 0.03,
                    max_underlying_drawdown: 0.06,
                })
            );
            assert_eq!(
                profile.risk_regime_cooldown_guard,
                Some(TrendDrawdownGuard {
                    min_underlying_return: 0.30,
                    max_underlying_drawdown: 0.05,
                })
            );
            assert_eq!(profile.risk_regime_cooldown_days, 20);
        }
    }

    #[test]
    fn edge_refinement_profiles_only_tighten_weak_trend_pullback_guard() {
        let profiles = research_profiles();
        for (name, max_underlying_return, min_underlying_drawdown, max_underlying_drawdown) in [
            (
                "edgerefine_weak13dd2to6_take45_dte30_delta25_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_width15_lowdelta23_width10_credit20",
                0.13,
                0.02,
                0.06,
            ),
            (
                "edgerefine_weak13dd2to5_take45_dte30_delta25_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_width15_lowdelta23_width10_credit20",
                0.13,
                0.02,
                0.05,
            ),
            (
                "edgerefine_weak14dd2to6_take45_dte30_delta25_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_width15_lowdelta23_width10_credit20",
                0.14,
                0.02,
                0.06,
            ),
        ] {
            let profile = profiles
                .iter()
                .find(|profile| profile.name == name)
                .unwrap();
            assert_eq!(profile.take_profit_pct, 0.45);
            assert_eq!(profile.stop_loss_multiple, 2.0);
            assert_eq!(profile.force_close_dte, 21);
            assert_eq!(profile.max_hold_days, None);
            assert_eq!(profile.max_concurrent_positions, 1);
            assert_eq!(profile.min_entry_spacing_days, 1);
            assert_eq!(profile.min_dte, 30);
            assert_eq!(profile.max_dte, 45);
            assert_eq!(profile.min_short_delta_abs, 0.25);
            assert_eq!(profile.max_short_delta_abs, 0.34);
            assert_eq!(profile.max_short_iv, Some(0.45));
            assert_eq!(profile.min_credit_width, 0.20);
            assert_eq!(profile.stop_loss_cooldown_days, 10);
            assert_eq!(profile.trend_lookback_days, Some(60));
            assert_eq!(profile.min_underlying_return, Some(0.10));
            assert_eq!(profile.max_width, 15.0);
            assert!(profile.prefer_farther_otm);
            assert_eq!(profile.low_delta_width_cap_delta_abs, Some(0.23));
            assert_eq!(profile.low_delta_width_cap, Some(10.0));
            assert_eq!(profile.drawdown_lookback_days, Some(20));
            assert_eq!(
                profile.return_or_drawdown_gate,
                Some(ReturnOrDrawdownGate {
                    min_underlying_return: Some(0.25),
                    min_underlying_drawdown: Some(0.02),
                })
            );
            assert_eq!(
                profile.weak_trend_pullback_guard,
                Some(WeakTrendPullbackGuard {
                    max_underlying_return,
                    min_underlying_drawdown,
                    max_underlying_drawdown,
                })
            );
            assert_eq!(
                profile.risk_regime_cooldown_guard,
                Some(TrendDrawdownGuard {
                    min_underlying_return: 0.30,
                    max_underlying_drawdown: 0.05,
                })
            );
            assert_eq!(profile.risk_regime_cooldown_days, 20);
        }
    }

    #[test]
    fn edge_delta_refinement_profiles_vary_only_short_delta_band() {
        let profiles = research_profiles();
        for (name, min_delta, max_delta) in [
            (
                "edgedelta_weak13dd2to5_take45_dte30_delta23_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_width15_lowdelta23_width10_credit20",
                0.23,
                0.34,
            ),
            (
                "edgedelta_weak13dd2to5_take45_dte30_delta24_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_width15_lowdelta23_width10_credit20",
                0.24,
                0.34,
            ),
            (
                "edgedelta_weak13dd2to5_take45_dte30_delta25_33_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_width15_lowdelta23_width10_credit20",
                0.25,
                0.33,
            ),
            (
                "edgedelta_weak13dd2to5_take45_dte30_delta26_34_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_width15_lowdelta23_width10_credit20",
                0.26,
                0.34,
            ),
            (
                "edgedelta_weak13dd2to5_take45_dte30_delta25_35_ivcap45_stopcool10_riskcool30dd5_20d_trend60d_min10_trend25_or_dd20d_min2_width15_lowdelta23_width10_credit20",
                0.25,
                0.35,
            ),
        ] {
            let profile = profiles
                .iter()
                .find(|profile| profile.name == name)
                .unwrap();
            assert_eq!(profile.take_profit_pct, 0.45);
            assert_eq!(profile.stop_loss_multiple, 2.0);
            assert_eq!(profile.force_close_dte, 21);
            assert_eq!(profile.max_hold_days, None);
            assert_eq!(profile.max_concurrent_positions, 1);
            assert_eq!(profile.min_entry_spacing_days, 1);
            assert_eq!(profile.min_dte, 30);
            assert_eq!(profile.max_dte, 45);
            assert_eq!(profile.min_short_delta_abs, min_delta);
            assert_eq!(profile.max_short_delta_abs, max_delta);
            assert_eq!(profile.max_short_iv, Some(0.45));
            assert_eq!(profile.min_credit_width, 0.20);
            assert_eq!(profile.stop_loss_cooldown_days, 10);
            assert_eq!(profile.trend_lookback_days, Some(60));
            assert_eq!(profile.min_underlying_return, Some(0.10));
            assert_eq!(profile.max_width, 15.0);
            assert!(profile.prefer_farther_otm);
            assert_eq!(profile.low_delta_width_cap_delta_abs, Some(0.23));
            assert_eq!(profile.low_delta_width_cap, Some(10.0));
            assert_eq!(profile.drawdown_lookback_days, Some(20));
            assert_eq!(
                profile.return_or_drawdown_gate,
                Some(ReturnOrDrawdownGate {
                    min_underlying_return: Some(0.25),
                    min_underlying_drawdown: Some(0.02),
                })
            );
            assert_eq!(
                profile.weak_trend_pullback_guard,
                Some(WeakTrendPullbackGuard {
                    max_underlying_return: 0.13,
                    min_underlying_drawdown: 0.02,
                    max_underlying_drawdown: 0.05,
                })
            );
            assert_eq!(
                profile.risk_regime_cooldown_guard,
                Some(TrendDrawdownGuard {
                    min_underlying_return: 0.30,
                    max_underlying_drawdown: 0.05,
                })
            );
            assert_eq!(profile.risk_regime_cooldown_days, 20);
        }
    }

    #[test]
    fn current_best_execution_variants_keep_detector_gates() {
        let profiles = research_profiles();
        for (name, stop_loss_multiple, take_profit_pct) in [
            (
                "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2_stop175_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                1.75,
                0.50,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2_stop150_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                1.50,
                0.50,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2_take40_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                2.00,
                0.40,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2_take35_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                2.00,
                0.35,
            ),
        ] {
            let profile = profiles
                .iter()
                .find(|profile| profile.name == name)
                .unwrap();
            assert_eq!(profile.trend_lookback_days, Some(60));
            assert_eq!(profile.min_underlying_return, Some(0.12));
            assert_eq!(profile.max_short_iv, Some(0.45));
            assert_eq!(profile.max_width, 15.0);
            assert!(profile.prefer_farther_otm);
            assert_eq!(profile.stop_loss_cooldown_days, 10);
            assert_eq!(profile.low_delta_width_cap_delta_abs, Some(0.23));
            assert_eq!(profile.low_delta_width_cap, Some(10.0));
            assert_eq!(profile.drawdown_lookback_days, Some(20));
            assert_eq!(
                profile.return_or_drawdown_gate,
                Some(ReturnOrDrawdownGate {
                    min_underlying_return: Some(0.25),
                    min_underlying_drawdown: Some(0.02),
                })
            );
            assert_eq!(profile.stop_loss_multiple, stop_loss_multiple);
            assert_eq!(profile.take_profit_pct, take_profit_pct);
        }
    }

    #[test]
    fn trend_drawdown_guard_profiles_keep_current_best_risk_gates() {
        let profiles = research_profiles();
        for (name, min_return, max_drawdown) in [
            (
                "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2_guard20dd3p5_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.20,
                0.035,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2_guard25dd4_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.25,
                0.04,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2_guard30dd4_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.30,
                0.04,
            ),
        ] {
            let profile = profiles
                .iter()
                .find(|profile| profile.name == name)
                .unwrap();
            assert_eq!(profile.trend_lookback_days, Some(60));
            assert_eq!(profile.min_underlying_return, Some(0.12));
            assert_eq!(profile.max_short_iv, Some(0.45));
            assert_eq!(profile.max_width, 15.0);
            assert!(profile.prefer_farther_otm);
            assert_eq!(profile.stop_loss_cooldown_days, 10);
            assert_eq!(profile.low_delta_width_cap_delta_abs, Some(0.23));
            assert_eq!(profile.low_delta_width_cap, Some(10.0));
            assert_eq!(profile.drawdown_lookback_days, Some(20));
            assert_eq!(
                profile.return_or_drawdown_gate,
                Some(ReturnOrDrawdownGate {
                    min_underlying_return: Some(0.25),
                    min_underlying_drawdown: Some(0.02),
                })
            );
            assert_eq!(
                profile.trend_drawdown_guard,
                Some(TrendDrawdownGuard {
                    min_underlying_return: min_return,
                    max_underlying_drawdown: max_drawdown,
                })
            );
        }
    }

    #[test]
    fn skew_profiles_keep_current_best_risk_gates() {
        let profiles = research_profiles();
        for (name, min_iv_diff) in [
            (
                "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2_skew30bps_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.003,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2_skew40bps_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.004,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2_skew45bps_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.0045,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2_skew50bps_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.005,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min12_trend25_or_dd20d_min2_skew80bps_ivcap45_width15_lowdelta23_width10_delta20_30_credit20",
                0.008,
            ),
        ] {
            let profile = profiles
                .iter()
                .find(|profile| profile.name == name)
                .unwrap();
            assert_eq!(profile.trend_lookback_days, Some(60));
            assert_eq!(profile.min_underlying_return, Some(0.12));
            assert_eq!(profile.max_short_iv, Some(0.45));
            assert_eq!(profile.min_long_short_iv_diff, Some(min_iv_diff));
            assert_eq!(profile.max_width, 15.0);
            assert!(profile.prefer_farther_otm);
            assert_eq!(profile.stop_loss_cooldown_days, 10);
            assert_eq!(profile.low_delta_width_cap_delta_abs, Some(0.23));
            assert_eq!(profile.low_delta_width_cap, Some(10.0));
            assert_eq!(profile.drawdown_lookback_days, Some(20));
            assert_eq!(
                profile.return_or_drawdown_gate,
                Some(ReturnOrDrawdownGate {
                    min_underlying_return: Some(0.25),
                    min_underlying_drawdown: Some(0.02),
                })
            );
        }
    }

    #[test]
    fn realized_vol_profiles_keep_current_best_risk_gates() {
        let profiles = research_profiles();
        for (name, max_realized_vol) in [
            (
                "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_rv20max45_delta20_30_credit20",
                0.45,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_rv20max55_delta20_30_credit20",
                0.55,
            ),
            (
                "select_farther_otm_cooldown10_trend60d_min5_ivcap45_width15_lowdelta23_width10_rv20max65_delta20_30_credit20",
                0.65,
            ),
        ] {
            let profile = profiles
                .iter()
                .find(|profile| profile.name == name)
                .unwrap();
            assert_eq!(profile.trend_lookback_days, Some(60));
            assert_eq!(profile.min_underlying_return, Some(0.05));
            assert_eq!(profile.max_short_iv, Some(0.45));
            assert_eq!(profile.max_width, 15.0);
            assert!(profile.prefer_farther_otm);
            assert_eq!(profile.stop_loss_cooldown_days, 10);
            assert_eq!(profile.low_delta_width_cap_delta_abs, Some(0.23));
            assert_eq!(profile.low_delta_width_cap, Some(10.0));
            assert_eq!(profile.realized_vol_lookback_days, Some(20));
            assert_eq!(profile.max_realized_vol, Some(max_realized_vol));
        }
    }

    #[test]
    fn walk_forward_selects_from_prior_data_and_prevents_cross_year_overlap() {
        let from = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
        let mut better_trades = training_trades(50.0);
        better_trades.push(trade_with_entry_exit(
            NaiveDate::from_ymd_opt(2023, 12, 1).unwrap(),
            NaiveDate::from_ymd_opt(2023, 12, 8).unwrap(),
            25.0,
        ));
        better_trades.push(trade_with_entry_exit(
            NaiveDate::from_ymd_opt(2023, 12, 29).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 10).unwrap(),
            25.0,
        ));
        better_trades.push(trade_with_entry_exit(
            NaiveDate::from_ymd_opt(2024, 1, 5).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 20).unwrap(),
            25.0,
        ));
        better_trades.push(trade_with_entry_exit(
            NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 9).unwrap(),
            25.0,
        ));
        let worse_trades = training_trades(1.0);
        let results = vec![
            profile_result("better", better_trades, from, to),
            profile_result("worse", worse_trades, from, to),
        ];

        let result = walk_forward(&results, from, to);

        assert_eq!(result.years.len(), 2);
        assert_eq!(result.years[0].test_year, 2023);
        assert!(result.years[0].active);
        assert_eq!(result.years[0].selected_profile, "better");
        assert_eq!(result.years[0].selection_candidates.len(), 2);
        assert_eq!(result.years[0].selection_candidates[0].rank, 1);
        assert_eq!(result.years[0].selection_candidates[0].profile, "better");
        assert_eq!(result.years[1].test_year, 2024);
        assert!(result.years[1].active);
        assert_eq!(result.years[1].selected_profile, "better");
        assert_eq!(result.years[1].train_metrics.recent_trades, 1);
        assert!(result.years[1].train_metrics.recent_activity_gate);
        assert_eq!(
            result.years[1].train_metrics.last_entry_date,
            Some(NaiveDate::from_ymd_opt(2023, 12, 1).unwrap())
        );
        assert_eq!(
            result.years[1].train_metrics.days_since_last_entry,
            Some(30)
        );
        assert_eq!(result.trades.len(), 3);
        assert_eq!(
            result.trades[0].entry_date,
            NaiveDate::from_ymd_opt(2023, 12, 1).unwrap()
        );
        assert_eq!(
            result.trades[1].entry_date,
            NaiveDate::from_ymd_opt(2023, 12, 29).unwrap()
        );
        assert_eq!(
            result.trades[2].entry_date,
            NaiveDate::from_ymd_opt(2024, 2, 1).unwrap()
        );
        assert_eq!(result.years[1].test_metrics.trades, 1);
    }

    #[test]
    fn walk_forward_requires_recent_closed_training_activity() {
        let from = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
        let mut trades = training_trades(50.0);
        trades.push(trade_with_entry_exit(
            NaiveDate::from_ymd_opt(2023, 12, 29).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 10).unwrap(),
            25.0,
        ));
        trades.push(trade_with_entry_exit(
            NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 9).unwrap(),
            25.0,
        ));
        let results = vec![profile_result("stale", trades, from, to)];

        let result = walk_forward(&results, from, to);
        let stale_year = result
            .years
            .iter()
            .find(|year| year.test_year == 2024)
            .unwrap();

        assert!(!stale_year.active);
        assert_eq!(stale_year.train_metrics.recent_trades, 0);
        assert!(!stale_year.train_metrics.recent_activity_gate);
        assert_eq!(
            stale_year.train_metrics.last_entry_date,
            Some(NaiveDate::from_ymd_opt(2022, 10, 1).unwrap())
        );
        assert_eq!(stale_year.test_metrics.trades, 0);
    }

    #[test]
    fn walk_forward_selects_best_deployable_profile_over_stale_top_rank() {
        let from = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
        let stale_high_score = training_trades(100.0);
        let mut deployable_lower_score = training_trades(40.0);
        deployable_lower_score.push(trade_with_entry_exit(
            NaiveDate::from_ymd_opt(2023, 12, 1).unwrap(),
            NaiveDate::from_ymd_opt(2023, 12, 8).unwrap(),
            25.0,
        ));
        deployable_lower_score.push(trade_with_entry_exit(
            NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 9).unwrap(),
            80.0,
        ));
        let results = vec![
            profile_result("stale_high_score", stale_high_score, from, to),
            profile_result("deployable_lower_score", deployable_lower_score, from, to),
        ];

        let result = walk_forward(&results, from, to);
        let test_year = result
            .years
            .iter()
            .find(|year| year.test_year == 2024)
            .unwrap();

        assert!(test_year.active);
        assert_eq!(test_year.selected_profile, "deployable_lower_score");
        assert_eq!(test_year.test_metrics.trades, 1);
        assert_eq!(test_year.test_metrics.total_pnl, 80.0);
    }

    #[test]
    fn fixed_profile_walk_forward_ranks_fixed_oos_results() {
        let from = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
        let mut good_trades = training_trades(50.0);
        good_trades.push(trade_with_entry_exit(
            NaiveDate::from_ymd_opt(2023, 12, 1).unwrap(),
            NaiveDate::from_ymd_opt(2023, 12, 8).unwrap(),
            25.0,
        ));
        good_trades.push(trade_with_entry_exit(
            NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 9).unwrap(),
            80.0,
        ));
        let mut bad_trades = training_trades(50.0);
        bad_trades.push(trade_with_entry_exit(
            NaiveDate::from_ymd_opt(2023, 12, 1).unwrap(),
            NaiveDate::from_ymd_opt(2023, 12, 8).unwrap(),
            25.0,
        ));
        bad_trades.push(trade_with_entry_exit(
            NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 9).unwrap(),
            -80.0,
        ));
        let results = vec![
            profile_result("bad", bad_trades, from, to),
            profile_result("good", good_trades, from, to),
        ];

        let fixed = fixed_profile_walk_forward(&results, from, to);

        assert_eq!(fixed[0].profile.name, "good");
        assert_eq!(fixed[0].active_years, 2);
        assert_eq!(fixed[0].trades.len(), 2);
        assert!(fixed[0].metrics.total_pnl > fixed[1].metrics.total_pnl);
        assert_eq!(fixed[1].profile.name, "bad");
    }

    #[test]
    fn fixed_profile_walk_forward_ranks_thin_oos_by_pnl_before_synthetic_score() {
        let from = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
        let mut profitable_thin = training_trades(50.0);
        profitable_thin.push(trade_with_entry_exit(
            NaiveDate::from_ymd_opt(2023, 12, 1).unwrap(),
            NaiveDate::from_ymd_opt(2023, 12, 8).unwrap(),
            25.0,
        ));
        profitable_thin.push(trade_with_entry_exit(
            NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 9).unwrap(),
            80.0,
        ));

        let mut losing_more_trades = training_trades(50.0);
        losing_more_trades.push(trade_with_entry_exit(
            NaiveDate::from_ymd_opt(2023, 12, 1).unwrap(),
            NaiveDate::from_ymd_opt(2023, 12, 8).unwrap(),
            25.0,
        ));
        for month in 1..=6 {
            losing_more_trades.push(trade_with_entry_exit(
                NaiveDate::from_ymd_opt(2024, month, 1).unwrap(),
                NaiveDate::from_ymd_opt(2024, month, 8).unwrap(),
                -20.0,
            ));
        }
        let results = vec![
            profile_result("losing_more_trades", losing_more_trades, from, to),
            profile_result("profitable_thin", profitable_thin, from, to),
        ];

        let fixed = fixed_profile_walk_forward(&results, from, to);

        assert_eq!(fixed[0].profile.name, "profitable_thin");
        assert_eq!(fixed[0].metrics.trades, 2);
        assert_eq!(fixed[0].metrics.total_pnl, 105.0);
        assert!(!fixed[0].metrics.ranking_eligible);
        assert_eq!(fixed[1].profile.name, "losing_more_trades");
        assert_eq!(fixed[1].metrics.trades, 7);
        assert_eq!(fixed[1].metrics.total_pnl, -95.0);
        assert!(fixed[1].metrics.score > fixed[0].metrics.score);
    }

    #[test]
    fn fixed_profile_walk_forward_requires_recent_closed_training_activity() {
        let from = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
        let mut trades = training_trades(50.0);
        trades.push(trade_with_entry_exit(
            NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 9).unwrap(),
            80.0,
        ));
        let results = vec![profile_result("stale", trades, from, to)];

        let fixed = fixed_profile_walk_forward(&results, from, to);
        let stale_year = fixed[0]
            .years
            .iter()
            .find(|year| year.test_year == 2024)
            .unwrap();

        assert!(!stale_year.active);
        assert_eq!(stale_year.train_metrics.recent_trades, 0);
        assert_eq!(stale_year.test_metrics.trades, 0);
        assert_eq!(fixed[0].trades.len(), 0);
    }

    #[test]
    fn rolling_walk_forward_uses_trailing_training_window() {
        let from = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2026, 12, 31).unwrap();
        let results = vec![profile_result("steady", training_trades(40.0), from, to)];

        let result = rolling_walk_forward(&results, from, to);

        assert_eq!(result.mode, "rolling");
        assert_eq!(
            result.train_window_days,
            Some(ROLLING_WALK_FORWARD_TRAIN_DAYS)
        );
        assert_eq!(result.years[0].test_year, 2024);
        assert!(result.years[0].train_from > from);
        assert_eq!(
            result.years[0].train_to,
            NaiveDate::from_ymd_opt(2023, 12, 31).unwrap()
        );
        assert!(result.years[1].train_from > result.years[0].train_from);
    }

    #[test]
    fn walk_forward_stays_inactive_without_robust_training_profile() {
        let from = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2023, 12, 31).unwrap();
        let trades = vec![
            trade_with_entry_exit(
                NaiveDate::from_ymd_opt(2020, 1, 2).unwrap(),
                NaiveDate::from_ymd_opt(2020, 1, 9).unwrap(),
                50.0,
            ),
            trade_with_entry_exit(
                NaiveDate::from_ymd_opt(2023, 2, 1).unwrap(),
                NaiveDate::from_ymd_opt(2023, 2, 8).unwrap(),
                50.0,
            ),
        ];
        let results = vec![profile_result("thin_profile", trades, from, to)];

        let result = walk_forward(&results, from, to);

        assert_eq!(result.years.len(), 1);
        assert_eq!(result.years[0].test_year, 2023);
        assert_eq!(result.years[0].selected_profile, "thin_profile");
        assert!(!result.years[0].active);
        assert!(!result.years[0].train_metrics.robust_ranking_eligible);
        assert_eq!(result.years[0].test_metrics.trades, 0);
        assert!(result.trades.is_empty());
    }

    #[test]
    fn walk_forward_training_selection_ignores_unclosed_cutoff_trades() {
        let from = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let train_to = NaiveDate::from_ymd_opt(2022, 12, 31).unwrap();
        let to = NaiveDate::from_ymd_opt(2023, 12, 31).unwrap();

        let closed_trades = training_trades(40.0);
        let mut leaky_trades = Vec::new();
        for month in 1..=10 {
            leaky_trades.push(trade_with_entry_exit(
                NaiveDate::from_ymd_opt(2020, month, 1).unwrap(),
                NaiveDate::from_ymd_opt(2020, month, 8).unwrap(),
                10.0,
            ));
        }
        for month in 1..=9 {
            leaky_trades.push(trade_with_entry_exit(
                NaiveDate::from_ymd_opt(2022, month, 1).unwrap(),
                NaiveDate::from_ymd_opt(2022, month, 8).unwrap(),
                10.0,
            ));
        }
        leaky_trades.push(trade_with_entry_exit(
            NaiveDate::from_ymd_opt(2022, 12, 20).unwrap(),
            NaiveDate::from_ymd_opt(2023, 1, 10).unwrap(),
            5_000.0,
        ));
        let results = vec![
            profile_result("leaky_cutoff", leaky_trades, from, to),
            profile_result("closed_train", closed_trades, from, to),
        ];

        let selection = select_walk_forward_profile(&results, from, train_to).unwrap();

        assert_eq!(selection.result.profile.name, "closed_train");
        assert!(selection.metrics.robust_ranking_eligible);
    }

    #[test]
    fn holdout_selects_profile_from_training_window_only() {
        let from = NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2025, 12, 31).unwrap();
        let mut better_train = training_trades(50.0);
        better_train.push(trade_with_entry_exit(
            NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 8).unwrap(),
            40.0,
        ));
        let mut worse_train = training_trades(1.0);
        worse_train.push(trade_with_entry_exit(
            NaiveDate::from_ymd_opt(2024, 2, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 2, 8).unwrap(),
            400.0,
        ));
        let results = vec![
            profile_result("better_train", better_train, from, to),
            profile_result("worse_train", worse_train, from, to),
        ];

        let result = holdout(&results, from, to);

        assert!(result.active);
        assert_eq!(result.selected_profile, "better_train");
        assert_eq!(result.trades.len(), 1);
        assert_eq!(result.metrics.total_pnl, 40.0);
        assert!(result.train_metrics.robust_ranking_eligible);
    }

    #[test]
    fn latest_signal_reports_candidate_without_future_exit_rows() {
        let entry_date = NaiveDate::from_ymd_opt(2026, 1, 10).unwrap();
        let expiration = entry_date + Duration::days(40);
        let mut rows = BTreeMap::new();
        rows.insert(
            expiration,
            vec![
                option_day(entry_date, 95.0, 1.20, 1.25, -0.25, 105.0),
                option_day(entry_date, 90.0, 0.10, 0.15, -0.15, 105.0),
            ],
        );
        let result = profile_result("baseline", Vec::new(), entry_date, entry_date);

        let signal = latest_signal_for_profile(&result, &rows, entry_date, entry_date).unwrap();

        assert_eq!(signal.status, "entry_candidate");
        assert_eq!(signal.as_of, entry_date);
        assert_eq!(signal.entry_date, entry_date);
        assert_eq!(signal.expiration, expiration);
        assert_eq!(signal.short_put, 95.0);
        assert_eq!(signal.long_put, 90.0);
        assert!((signal.entry_credit - 1.05).abs() < 1e-9);
        assert!((signal.max_loss - 395.0).abs() < 1e-9);
    }

    #[test]
    fn latest_signal_labels_prior_entry_as_open_candidate() {
        let entry_date = NaiveDate::from_ymd_opt(2026, 1, 10).unwrap();
        let as_of = NaiveDate::from_ymd_opt(2026, 1, 15).unwrap();
        let expiration = entry_date + Duration::days(40);
        let mut rows = BTreeMap::new();
        rows.insert(
            expiration,
            vec![
                option_day(entry_date, 95.0, 1.20, 1.25, -0.25, 105.0),
                option_day(entry_date, 90.0, 0.10, 0.15, -0.15, 105.0),
            ],
        );
        let result = profile_result("baseline", Vec::new(), entry_date, as_of);

        let signal = latest_signal_for_profile(&result, &rows, entry_date, as_of).unwrap();

        assert_eq!(signal.status, "open_candidate");
        assert_eq!(signal.as_of, as_of);
        assert_eq!(signal.entry_date, entry_date);
    }

    #[test]
    fn latest_best_profile_signal_requires_deployable_metrics() {
        let entry_date = NaiveDate::from_ymd_opt(2026, 1, 10).unwrap();
        let expiration = entry_date + Duration::days(40);
        let mut rows = BTreeMap::new();
        rows.insert(
            expiration,
            vec![
                option_day(entry_date, 95.0, 1.20, 1.25, -0.25, 105.0),
                option_day(entry_date, 90.0, 0.10, 0.15, -0.15, 105.0),
            ],
        );
        let result = profile_result("baseline", Vec::new(), entry_date, entry_date);

        assert!(latest_signal_for_profile(&result, &rows, entry_date, entry_date).is_some());
        assert!(latest_signal_for_best_profile(&[result], &rows, entry_date, entry_date).is_none());
    }

    #[test]
    fn latest_signal_starts_after_last_closed_trade() {
        let stale_date = NaiveDate::from_ymd_opt(2026, 1, 10).unwrap();
        let latest_date = NaiveDate::from_ymd_opt(2026, 1, 15).unwrap();
        let stale_expiration = stale_date + Duration::days(40);
        let latest_expiration = latest_date + Duration::days(40);
        let mut rows = BTreeMap::new();
        rows.insert(
            stale_expiration,
            vec![
                option_day(stale_date, 95.0, 1.20, 1.25, -0.25, 105.0),
                option_day(stale_date, 90.0, 0.10, 0.15, -0.15, 105.0),
            ],
        );
        rows.insert(
            latest_expiration,
            vec![
                option_day(latest_date, 94.0, 1.25, 1.30, -0.24, 106.0),
                option_day(latest_date, 89.0, 0.10, 0.15, -0.15, 106.0),
            ],
        );
        let closed_trade = trade_with_entry_exit(
            NaiveDate::from_ymd_opt(2026, 1, 8).unwrap(),
            NaiveDate::from_ymd_opt(2026, 1, 12).unwrap(),
            50.0,
        );
        let result = profile_result("baseline", vec![closed_trade], stale_date, latest_date);

        let signal = latest_signal_for_profile(&result, &rows, stale_date, latest_date).unwrap();

        assert_eq!(signal.entry_date, latest_date);
        assert_eq!(signal.expiration, latest_expiration);
        assert_eq!(signal.short_put, 94.0);
    }

    #[test]
    fn corrupt_cache_file_is_treated_as_cache_miss() {
        let path = unique_test_path("corrupt-cache.json");
        fs::write(&path, "").unwrap();

        assert!(read_cached_json(&path).unwrap().is_none());

        let _ = fs::remove_file(path);
    }

    #[test]
    fn cached_json_write_leaves_readable_final_file() {
        let path = unique_test_path("atomic-cache.json");
        let json = serde_json::json!({"response": [{"ok": true}]});

        write_cached_json(&path, &json).unwrap();

        assert_eq!(read_cached_json(&path).unwrap(), Some(json));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn oi_timeout_splits_uncached_remainder_but_keeps_cached_weeklies() {
        let raw_dir = unique_test_path("oi-cache-dir");
        fs::create_dir_all(&raw_dir).unwrap();
        let exp = "20240719";
        let cached_start = NaiveDate::from_ymd_opt(2024, 5, 8).unwrap();
        let cached_end = NaiveDate::from_ymd_opt(2024, 5, 14).unwrap();
        fs::write(
            oi_cache_path(&raw_dir, exp, cached_start, cached_end, OptionRight::Put),
            "{}",
        )
        .unwrap();
        let mut chunks = VecDeque::from([
            (cached_start, cached_end),
            (
                NaiveDate::from_ymd_opt(2024, 5, 15).unwrap(),
                NaiveDate::from_ymd_opt(2024, 5, 21).unwrap(),
            ),
        ]);

        split_oi_remainder_to_daily(
            &mut chunks,
            &raw_dir,
            exp,
            false,
            OptionRight::Put,
            NaiveDate::from_ymd_opt(2024, 5, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 5, 7).unwrap(),
        );

        let chunks = chunks.into_iter().collect::<Vec<_>>();
        assert_eq!(chunks.len(), 15);
        assert_eq!(
            chunks[0],
            (
                NaiveDate::from_ymd_opt(2024, 5, 1).unwrap(),
                NaiveDate::from_ymd_opt(2024, 5, 1).unwrap()
            )
        );
        assert_eq!(chunks[7], (cached_start, cached_end));
        assert_eq!(
            chunks[8],
            (
                NaiveDate::from_ymd_opt(2024, 5, 15).unwrap(),
                NaiveDate::from_ymd_opt(2024, 5, 15).unwrap()
            )
        );
        assert_eq!(
            chunks[14],
            (
                NaiveDate::from_ymd_opt(2024, 5, 21).unwrap(),
                NaiveDate::from_ymd_opt(2024, 5, 21).unwrap()
            )
        );

        let _ = fs::remove_dir_all(raw_dir);
    }

    #[test]
    fn farther_otm_selector_overrides_default_return_on_risk_ordering() {
        let date = NaiveDate::from_ymd_opt(2026, 1, 11).unwrap();
        let close_high_credit = candidate_for_ordering(date, 95.0, 90.0, 1.25, -0.30, 0.05);
        let far_lower_credit = candidate_for_ordering(date, 90.0, 85.0, 1.05, -0.20, 0.10);
        let baseline = ResearchProfile::legacy_baseline();
        let mut farther_otm = ResearchProfile::legacy_baseline();
        farther_otm.prefer_farther_otm = true;

        assert_eq!(
            candidate_quality_order(&close_high_credit, &far_lower_credit, &baseline),
            Ordering::Less
        );
        assert_eq!(
            candidate_quality_order(&far_lower_credit, &close_high_credit, &farther_otm),
            Ordering::Less
        );
    }

    #[test]
    fn stop_loss_cooldown_delays_only_after_stop_losses() {
        let exit_date = NaiveDate::from_ymd_opt(2026, 1, 20).unwrap();
        let mut profile = ResearchProfile::legacy_baseline();
        profile.stop_loss_cooldown_days = 10;

        assert_eq!(
            next_entry_date_after_trade(&trade_with_exit(exit_date, "stop_loss"), &profile),
            NaiveDate::from_ymd_opt(2026, 1, 30).unwrap()
        );
        assert_eq!(
            next_entry_date_after_trade(&trade_with_exit(exit_date, "take_profit"), &profile),
            NaiveDate::from_ymd_opt(2026, 1, 21).unwrap()
        );
    }

    #[test]
    fn risk_regime_cooldown_skips_immediate_reentry_candidates() {
        let risk_date = NaiveDate::from_ymd_opt(2026, 1, 10).unwrap();
        let skipped_date = NaiveDate::from_ymd_opt(2026, 1, 15).unwrap();
        let eligible_date = NaiveDate::from_ymd_opt(2026, 1, 22).unwrap();
        let mut profile = ResearchProfile::legacy_baseline();
        profile.risk_regime_cooldown_guard = Some(TrendDrawdownGuard {
            min_underlying_return: 0.30,
            max_underlying_drawdown: 0.05,
        });
        profile.risk_regime_cooldown_days = 10;

        let mut risk_candidate = candidate_for_ordering(risk_date, 95.0, 90.0, 1.05, -0.25, 0.05);
        risk_candidate.underlying_lookback_return = Some(0.35);
        risk_candidate.underlying_recent_drawdown = Some(0.06);
        let mut skipped_candidate =
            candidate_for_ordering(skipped_date, 95.0, 90.0, 1.05, -0.25, 0.05);
        skipped_candidate.underlying_lookback_return = Some(0.20);
        skipped_candidate.underlying_recent_drawdown = Some(0.02);
        let mut eligible_candidate =
            candidate_for_ordering(eligible_date, 95.0, 90.0, 1.05, -0.25, 0.05);
        eligible_candidate.underlying_lookback_return = Some(0.20);
        eligible_candidate.underlying_recent_drawdown = Some(0.02);
        let candidates = vec![risk_candidate, skipped_candidate, eligible_candidate];

        let (signal_date, day_candidates) =
            latest_signal_day_candidates(&candidates, &profile).unwrap();

        assert_eq!(
            next_entry_date_after_risk_regime(risk_date, &profile),
            NaiveDate::from_ymd_opt(2026, 1, 20).unwrap()
        );
        assert_eq!(signal_date, eligible_date);
        assert_eq!(day_candidates[0].entry_date, eligible_date);
    }

    #[test]
    fn capped_overlap_scheduler_allows_staggered_entries_but_caps_heat() {
        let first_date = NaiveDate::from_ymd_opt(2026, 1, 10).unwrap();
        let second_date = NaiveDate::from_ymd_opt(2026, 1, 12).unwrap();
        let third_date = NaiveDate::from_ymd_opt(2026, 1, 14).unwrap();
        let candidates = vec![
            candidate_for_ordering(first_date, 95.0, 90.0, 1.00, -0.30, 0.05),
            candidate_for_ordering(second_date, 94.0, 89.0, 1.00, -0.30, 0.05),
            candidate_for_ordering(third_date, 93.0, 88.0, 1.00, -0.30, 0.05),
        ];
        let rows_by_expiration =
            rows_for_take_profit_candidates(&candidates, first_date + Duration::days(20));

        let mut single = ResearchProfile::legacy_baseline();
        single.prefer_farther_otm = true;
        let single_trades = simulate_non_overlapping(&candidates, &rows_by_expiration, &single);
        assert_eq!(single_trades.len(), 1);
        assert_eq!(single_trades[0].entry_date, first_date);

        let mut capped_overlap = single.clone();
        capped_overlap.max_concurrent_positions = 2;
        capped_overlap.min_entry_spacing_days = 1;
        let overlap_trades =
            simulate_non_overlapping(&candidates, &rows_by_expiration, &capped_overlap);

        assert_eq!(
            overlap_trades
                .iter()
                .map(|trade| trade.entry_date)
                .collect::<Vec<_>>(),
            vec![first_date, second_date]
        );
    }

    fn training_trades(pnl: f64) -> Vec<ResearchTrade> {
        let mut trades = Vec::new();
        for month in 1..=10 {
            trades.push(trade_with_entry_exit(
                NaiveDate::from_ymd_opt(2020, month, 1).unwrap(),
                NaiveDate::from_ymd_opt(2020, month, 8).unwrap(),
                pnl,
            ));
            trades.push(trade_with_entry_exit(
                NaiveDate::from_ymd_opt(2022, month, 1).unwrap(),
                NaiveDate::from_ymd_opt(2022, month, 8).unwrap(),
                pnl,
            ));
        }
        trades
    }

    fn profile_result(
        name: &str,
        trades: Vec<ResearchTrade>,
        from: NaiveDate,
        to: NaiveDate,
    ) -> ProfileResult {
        let mut profile = ResearchProfile::legacy_baseline();
        profile.name = name.to_owned();
        let metrics = metrics(&trades, from, to);
        ProfileResult {
            detector_strategy: detector_strategy_summary(&profile),
            execution_strategy: execution_strategy_summary(&profile),
            profile,
            candidates: trades.len(),
            trades,
            metrics,
        }
    }

    fn test_signal(as_of: NaiveDate, profile_name: &str) -> ResearchSignal {
        ResearchSignal {
            as_of,
            status: "entry_candidate".to_owned(),
            profile_name: profile_name.to_owned(),
            entry_date: as_of,
            expiration: as_of + Duration::days(40),
            dte_entry: 40,
            short_put: 95.0,
            long_put: 90.0,
            width: 5.0,
            entry_credit: 1.05,
            max_profit: 105.0,
            max_loss: 395.0,
            return_on_risk: 105.0 / 395.0,
            short_delta: -0.25,
            long_delta: -0.15,
            short_oi: 1_000,
            long_oi: 1_000,
            underlying_price: 105.0,
            short_otm_pct: 10.0 / 105.0,
            underlying_lookback_return: Some(0.12),
            underlying_recent_drawdown: Some(0.02),
            underlying_realized_vol: None,
            short_iv: 0.40,
            long_iv: 0.41,
        }
    }

    fn trade_with_entry_exit(
        entry_date: NaiveDate,
        exit_date: NaiveDate,
        pnl: f64,
    ) -> ResearchTrade {
        let max_loss = 400.0;
        let mut trade = trade_with_exit(exit_date, "period_test");
        trade.entry_date = entry_date;
        trade.days_held = (exit_date - entry_date).num_days();
        trade.pnl = pnl;
        trade.return_on_risk = pnl / max_loss;
        trade.max_loss = max_loss;
        trade
    }

    fn test_period_metrics(
        name: &str,
        from: NaiveDate,
        to: NaiveDate,
        score: f64,
    ) -> PeriodMetrics {
        PeriodMetrics {
            name: name.to_owned(),
            from,
            to,
            trades: 10,
            total_pnl: 0.0,
            avg_return_on_risk: 0.0,
            win_rate: 0.0,
            profit_factor: 0.0,
            max_drawdown: 0.0,
            score,
            ranking_eligible: true,
            required_trades: 10,
        }
    }

    fn trade_with_exit(exit_date: NaiveDate, exit_reason: &str) -> ResearchTrade {
        ResearchTrade {
            entry_date: exit_date - Duration::days(5),
            exit_date,
            expiration: exit_date + Duration::days(30),
            dte_entry: 35,
            days_held: 5,
            short_put: 95.0,
            long_put: 90.0,
            width: 5.0,
            entry_credit: 1.0,
            exit_debit: 0.4,
            max_profit: 100.0,
            max_loss: 400.0,
            pnl: 60.0,
            return_on_risk: 0.15,
            exit_reason: exit_reason.to_owned(),
            short_delta: -0.25,
            long_delta: -0.15,
            short_oi: 1_000,
            long_oi: 1_000,
            underlying_price: 100.0,
            short_otm_pct: 0.05,
            underlying_lookback_return: None,
            underlying_recent_drawdown: None,
            underlying_realized_vol: None,
            short_iv: 0.5,
            long_iv: 0.5,
        }
    }

    fn candidate_for_ordering(
        date: NaiveDate,
        short_strike: f64,
        long_strike: f64,
        credit: f64,
        short_delta: f64,
        short_otm_pct: f64,
    ) -> Candidate {
        let width = short_strike - long_strike;
        let max_loss = width - credit;
        Candidate {
            structure: SpreadStructure::PutCreditSpread,
            entry_date: date,
            expiration: date + Duration::days(40),
            short: OptionDay {
                strike: short_strike,
                delta: short_delta,
                ..option_day(date, short_strike, 1.0, 1.1, short_delta, 100.0)
            },
            long: option_day(date, long_strike, 0.1, 0.2, short_delta / 2.0, 100.0),
            width,
            credit,
            max_profit_per_share: credit,
            max_loss_per_share: max_loss,
            return_on_risk: credit / max_loss,
            short_otm_pct,
            underlying_lookback_return: None,
            underlying_recent_drawdown: None,
            underlying_realized_vol: None,
            short_iv: 0.5,
            long_iv: 0.5,
        }
    }

    fn rows_for_take_profit_candidates(
        candidates: &[Candidate],
        exit_date: NaiveDate,
    ) -> BTreeMap<NaiveDate, Vec<OptionDay>> {
        let mut rows_by_expiration = BTreeMap::new();
        for candidate in candidates {
            rows_by_expiration
                .entry(candidate.expiration)
                .or_insert_with(Vec::new)
                .extend([
                    option_day(
                        exit_date,
                        candidate.short.strike,
                        0.30,
                        0.40,
                        candidate.short.delta,
                        100.0,
                    ),
                    option_day(
                        exit_date,
                        candidate.long.strike,
                        0.00,
                        0.05,
                        candidate.long.delta,
                        100.0,
                    ),
                ]);
        }
        rows_by_expiration
    }

    fn unique_test_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "spreadfoundry-research-test-{}-{nanos}-{name}",
            std::process::id()
        ))
    }

    fn option_day(
        date: NaiveDate,
        strike: f64,
        bid: f64,
        ask: f64,
        delta: f64,
        underlying_price: f64,
    ) -> OptionDay {
        OptionDay {
            date,
            strike,
            bid,
            ask,
            delta,
            implied_vol: 0.5,
            underlying_price,
            open_interest: 1_000,
        }
    }

    fn portfolio_wheel_test_request(
        capital_budget: f64,
        max_symbol_allocation_pct: f64,
        max_open_positions: usize,
        max_positions_per_symbol: usize,
    ) -> PortfolioWheelResearchRequest {
        PortfolioWheelResearchRequest {
            symbols: vec!["IREN".to_owned(), "PLTR".to_owned()],
            from: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
            to: NaiveDate::from_ymd_opt(2026, 2, 1).unwrap(),
            max_expirations: None,
            fetch_concurrency: 1,
            symbol_concurrency: 1,
            force_refresh: false,
            cache_only: false,
            capital_budget,
            max_symbol_allocation_pct,
            max_open_positions,
            max_positions_per_symbol,
            max_total_trades_per_symbol: None,
            portfolio_drawdown_cooldown_trigger_pct: None,
            portfolio_drawdown_cooldown_days: 0,
            symbol_drawdown_cooldown_trigger_pct: None,
            symbol_drawdown_cooldown_days: 0,
        }
    }

    fn portfolio_wheel_opportunity(
        symbol: &str,
        entry_date: NaiveDate,
        exit_date: NaiveDate,
        max_loss: f64,
        pnl: f64,
    ) -> PortfolioWheelOpportunity {
        PortfolioWheelOpportunity {
            symbol: symbol.to_owned(),
            strategy: SpreadStructure::Wheel,
            capital_at_risk: max_loss,
            trade: ResearchTrade {
                entry_date,
                exit_date,
                expiration: exit_date,
                dte_entry: (exit_date - entry_date).num_days(),
                days_held: (exit_date - entry_date).num_days(),
                short_put: max_loss / 100.0,
                long_put: 0.0,
                width: 0.0,
                entry_credit: pnl.max(0.0) / 100.0,
                exit_debit: 0.0,
                max_profit: pnl.max(0.0),
                max_loss,
                pnl,
                return_on_risk: pnl / max_loss,
                exit_reason: "put_expired".to_owned(),
                short_delta: -0.20,
                long_delta: 0.0,
                short_oi: 1_000,
                long_oi: 0,
                underlying_price: max_loss / 100.0,
                short_otm_pct: 0.05,
                underlying_lookback_return: None,
                underlying_recent_drawdown: None,
                underlying_realized_vol: None,
                short_iv: 0.5,
                long_iv: 0.0,
            },
        }
    }
}
