use anyhow::{Context, Result};
use chrono::{Datelike, Duration, NaiveDate, Utc};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration as StdDuration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

const FETCH_ATTEMPTS: usize = 3;
const MIN_RANKING_TRADES: usize = 10;
const MIN_RANKING_TRADES_PER_YEAR: f64 = 2.0;
const COST_STRESS_PER_TRADE: [f64; 3] = [5.0, 10.0, 25.0];
const WALK_FORWARD_MIN_TRAIN_DAYS: i64 = 365 * 3;
const ROLLING_WALK_FORWARD_TRAIN_DAYS: i64 = 365 * 4;
const WALK_FORWARD_SELECTION_DIAGNOSTIC_LIMIT: usize = 5;
const PLATEAU_MIN_PROFILE_VARIANTS: usize = 75;
const PLATEAU_MIN_WALK_FORWARD_YEARS: usize = 3;
const RECENT_TRAIN_ACTIVITY_DAYS: i64 = 365;
const MIN_DEPLOYABLE_TRAINING_ROBUST_SCORE: f64 = 0.005;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResearchRequest {
    pub symbol: String,
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub max_expirations: Option<usize>,
    pub fetch_concurrency: usize,
    pub force_refresh: bool,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResearchProfile {
    pub name: String,
    pub min_dte: i64,
    pub max_dte: i64,
    pub force_close_dte: i64,
    pub min_short_delta_abs: f64,
    pub max_short_delta_abs: f64,
    pub min_width: f64,
    pub max_width: f64,
    pub min_credit_width: f64,
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
    pub realized_vol_lookback_days: Option<i64>,
    pub max_realized_vol: Option<f64>,
    pub min_short_otm_pct: Option<f64>,
    pub min_short_iv: Option<f64>,
    pub max_short_iv: Option<f64>,
    pub min_long_short_iv_diff: Option<f64>,
    pub low_delta_width_cap_delta_abs: Option<f64>,
    pub low_delta_width_cap: Option<f64>,
    pub prefer_farther_otm: bool,
    pub stop_loss_cooldown_days: i64,
}

impl ResearchProfile {
    pub fn baseline() -> Self {
        Self {
            name: "baseline_30_45dte_delta20_30_credit20".to_owned(),
            min_dte: 30,
            max_dte: 45,
            force_close_dte: 21,
            min_short_delta_abs: 0.20,
            max_short_delta_abs: 0.30,
            min_width: 5.0,
            max_width: 20.0,
            min_credit_width: 0.20,
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
            realized_vol_lookback_days: None,
            max_realized_vol: None,
            min_short_otm_pct: None,
            min_short_iv: None,
            max_short_iv: None,
            min_long_short_iv_diff: None,
            low_delta_width_cap_delta_abs: None,
            low_delta_width_cap: None,
            prefer_farther_otm: false,
            stop_loss_cooldown_days: 1,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResearchReport {
    pub run_id: String,
    pub symbol: String,
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
    max_regime_lookback_days: i64,
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
    entry_date: NaiveDate,
    expiration: NaiveDate,
    short: OptionDay,
    long: OptionDay,
    width: f64,
    credit: f64,
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
    let raw_dir = PathBuf::from("data/raw/theta").join(&request.symbol);
    let run_id = format!(
        "{}-research-{}",
        symbol_slug(&request.symbol),
        Utc::now().format("%Y%m%dT%H%M%S%.9fZ")
    );
    let run_dir = PathBuf::from("runs").join(&run_id);
    fs::create_dir_all(&raw_dir)?;
    fs::create_dir_all(&run_dir)?;

    let profiles = research_profiles();
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

    let expirations =
        discover_expirations(&request.symbol, &raw_dir, request.force_refresh).await?;
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
    let requested_load_bounds = ExpirationLoadBounds {
        from: request.from,
        to: request.to,
        max_entry_dte,
        min_force_close_dte,
        max_regime_lookback_days,
    };
    let mut research_from = request.from;
    let expirations_skipped_before_data;
    if let Some(first_loadable_idx) = first_expiration_with_rows(
        &request.symbol,
        &candidate_expirations,
        requested_load_bounds,
        &raw_dir,
        request.force_refresh,
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
    let effective_load_bounds = ExpirationLoadBounds {
        from: research_from,
        ..requested_load_bounds
    };

    let mut rows_by_expiration = BTreeMap::new();
    let mut rows_loaded = 0;
    let mut expiration_load_failures = Vec::new();
    let fetch_concurrency = request.fetch_concurrency.max(1);
    for chunk in candidate_expirations.chunks(fetch_concurrency) {
        let fetches = chunk.iter().copied().filter_map(|expiration| {
            let (start, end) = expiration_load_window(expiration, effective_load_bounds)?;
            let symbol = request.symbol.clone();
            let raw_dir = raw_dir.clone();
            let force_refresh = request.force_refresh;
            Some(async move {
                println!("loading {} {}..{}", expiration, start, end);
                load_expiration_rows(&symbol, expiration, start, end, &raw_dir, force_refresh)
                    .await
                    .map(|rows| (expiration, rows))
                    .map_err(|error| (expiration, error))
            })
        });
        for result in join_all(fetches).await {
            match result {
                Ok((expiration, rows)) => {
                    rows_loaded += rows.len();
                    if !rows.is_empty() {
                        rows_by_expiration.insert(expiration, rows);
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

    let mut profile_results = Vec::new();
    for profile in profiles {
        let candidates =
            generate_candidates(&rows_by_expiration, &profile, research_from, request.to);
        let trades = simulate_non_overlapping(&candidates, &rows_by_expiration, &profile);
        let metrics = metrics(&trades, research_from, request.to);
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

async fn first_expiration_with_rows(
    symbol: &str,
    expirations: &[NaiveDate],
    bounds: ExpirationLoadBounds,
    raw_dir: &Path,
    force_refresh: bool,
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
        let has_rows = match load_expiration_rows(
            symbol,
            expiration,
            start,
            end,
            raw_dir,
            force_refresh,
        )
        .await
        {
            Ok(rows) => !rows.is_empty(),
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

fn expiration_load_window(
    expiration: NaiveDate,
    bounds: ExpirationLoadBounds,
) -> Option<(NaiveDate, NaiveDate)> {
    let earliest_entry = bounds
        .from
        .max(expiration - Duration::days(bounds.max_entry_dte));
    let start = earliest_entry - Duration::days(bounds.max_regime_lookback_days);
    let exit_grace_end =
        expiration - Duration::days(bounds.min_force_close_dte) + Duration::days(7);
    let end = bounds.to.min(exit_grace_end);
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
    if let Some(days) = profile.realized_vol_lookback_days
        && let Some(max_realized_vol) = profile.max_realized_vol
    {
        filters.push(format!("realized_vol_{days}d<={max_realized_vol:.3}"));
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
    if let Some(min_long_short_iv_diff) = profile.min_long_short_iv_diff {
        filters.push(format!("long_short_iv_diff>={min_long_short_iv_diff:.4}"));
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
        "highest_credit"
    };
    ExecutionStrategySummary {
        name: format!(
            "put_spread_execution_{}_tp{:.0}_stop{:.1}_close{}d",
            candidate_selector,
            profile.take_profit_pct * 100.0,
            profile.stop_loss_multiple,
            profile.force_close_dte
        ),
        candidate_selector: candidate_selector.to_owned(),
        entry_fill_model: "short_bid_minus_long_ask".to_owned(),
        exit_fill_model: "short_ask_minus_long_bid".to_owned(),
        take_profit_pct: profile.take_profit_pct,
        stop_loss_multiple: profile.stop_loss_multiple,
        force_close_dte: profile.force_close_dte,
        max_hold_days: profile.max_hold_days,
        stop_loss_cooldown_days: profile.stop_loss_cooldown_days,
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
        .then_with(|| b_metrics.robust_score.total_cmp(&a_metrics.robust_score))
        .then_with(|| b_metrics.score.total_cmp(&a_metrics.score))
        .then_with(|| profile_complexity(a_profile).cmp(&profile_complexity(b_profile)))
        .then_with(|| a_profile.name.cmp(&b_profile.name))
}

fn profile_complexity(profile: &ResearchProfile) -> usize {
    let baseline = ResearchProfile::baseline();
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
        &profile.realized_vol_lookback_days,
        &baseline.realized_vol_lookback_days,
    );
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
        let selection = &ranked_selections[0];

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
    WalkForwardResult {
        mode: mode.to_owned(),
        min_train_days,
        train_window_days,
        years,
        selected_profile_counts,
        metrics: metrics(&trades, metrics_from, to),
        trades,
    }
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
        metrics: metrics(&trades, test_from, to),
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
        let train_metrics = metrics(&train_trades, train_from, train_to);
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
    let metrics = metrics(&trades, metrics_from, to);
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
        .then_with(|| b.metrics.score.total_cmp(&a.metrics.score))
        .then_with(|| b.metrics.total_pnl.total_cmp(&a.metrics.total_pnl))
        .then_with(|| b.metrics.trades.cmp(&a.metrics.trades))
        .then_with(|| profile_complexity(&a.profile).cmp(&profile_complexity(&b.profile)))
        .then_with(|| a.profile.name.cmp(&b.profile.name))
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
    rank_walk_forward_profiles(profile_results, train_from, train_to)
        .into_iter()
        .next()
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
            let metrics = metrics(&train_trades, train_from, train_to);
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
    holdout_active: bool,
    deployment_gate: &DeploymentGate,
) -> PlateauStatus {
    let profile_variants_evaluated = profiles_evaluated.saturating_sub(1);
    let enough_variants = profile_variants_evaluated >= PLATEAU_MIN_PROFILE_VARIANTS;
    let enough_oos = walk_forward_years >= PLATEAU_MIN_WALK_FORWARD_YEARS && holdout_active;
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
        && enough_oos
    {
        (
            "plateau_expand_universe",
            true,
            "current-symbol detector is robust in sample, but execution validation is still blocked out of sample after broad profile search",
            "run the same separated detector and execution-strategy research on five liquid single-stock symbols",
        )
    } else if !enough_variants {
        (
            "continue_symbol_research",
            false,
            "profile search has not reached the minimum variant coverage for plateau",
            "continue current-symbol detector and execution-strategy variants",
        )
    } else if !enough_oos {
        (
            "continue_symbol_research",
            false,
            "out-of-sample coverage is too thin for plateau",
            "extend current-symbol history or walk-forward coverage before expanding symbols",
        )
    } else if !deployment_gate.best_profile_gate {
        (
            "continue_symbol_research",
            false,
            "no robust in-sample detector is available yet",
            "continue current-symbol detector search",
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
    let latest_entry_date = candidates
        .iter()
        .map(|candidate| candidate.entry_date)
        .max()?;
    let mut day_candidates = candidates
        .iter()
        .filter(|candidate| candidate.entry_date == latest_entry_date)
        .collect::<Vec<_>>();
    day_candidates.sort_by(|a, b| candidate_quality_order(a, b, &result.profile));
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

fn research_profiles() -> Vec<ResearchProfile> {
    let baseline = ResearchProfile::baseline();
    let mut profiles = vec![baseline.clone()];
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

    profiles
}

async fn discover_expirations(
    symbol: &str,
    raw_dir: &Path,
    force_refresh: bool,
) -> Result<Vec<NaiveDate>> {
    let path = raw_dir.join("expirations.json");
    let url =
        format!("http://127.0.0.1:25503/v3/option/list/expirations?symbol={symbol}&format=json");
    let json = fetch_cached_json(&url, &path, force_refresh).await?;
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

async fn load_expiration_rows(
    symbol: &str,
    expiration: NaiveDate,
    start: NaiveDate,
    end: NaiveDate,
    raw_dir: &Path,
    force_refresh: bool,
) -> Result<Vec<OptionDay>> {
    let oi_map = load_open_interest_map(symbol, expiration, start, end, raw_dir, force_refresh)
        .await
        .with_context(|| format!("loading open interest for {symbol} {expiration}"))?;
    let mut rows = load_greeks_rows(
        symbol,
        expiration,
        start,
        end,
        raw_dir,
        force_refresh,
        &oi_map,
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
    oi_map: &HashMap<(NaiveDate, String), u32>,
) -> Result<Vec<OptionDay>> {
    let exp = yyyymmdd(expiration);
    let mut out = Vec::new();
    let mut chunk_start = start;
    while chunk_start <= end {
        let chunk_end = end.min(chunk_start + Duration::days(6));
        let chunk_start_s = yyyymmdd(chunk_start);
        let chunk_end_s = yyyymmdd(chunk_end);
        let greeks_path = raw_dir.join(format!(
            "research_greeks_{exp}_{chunk_start_s}_{chunk_end_s}.json"
        ));
        let greeks_url = format!(
            "http://127.0.0.1:25503/v3/option/history/greeks/eod?symbol={symbol}&expiration={exp}&right=put&start_date={chunk_start_s}&end_date={chunk_end_s}&format=json"
        );
        let greeks = fetch_cached_json(&greeks_url, &greeks_path, force_refresh).await?;
        out.extend(parse_greeks_rows(&greeks, oi_map)?);
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
) -> Result<HashMap<(NaiveDate, String), u32>> {
    let exp = yyyymmdd(expiration);
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
        let oi_path = oi_cache_path(raw_dir, &exp, chunk_start, chunk_end);
        let oi_url = format!(
            "http://127.0.0.1:25503/v3/option/history/open_interest?symbol={symbol}&expiration={exp}&right=put&start_date={chunk_start_s}&end_date={chunk_end_s}&format=json"
        );
        match fetch_cached_json(&oi_url, &oi_path, force_refresh).await {
            Ok(oi) => out.extend(parse_oi_map(&oi)?),
            Err(error) if chunk_start < chunk_end && !is_non_retryable_thetadata_error(&error) => {
                split_oi_remainder_to_daily(
                    &mut chunks,
                    raw_dir,
                    &exp,
                    force_refresh,
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

fn oi_cache_path(
    raw_dir: &Path,
    exp: &str,
    chunk_start: NaiveDate,
    chunk_end: NaiveDate,
) -> PathBuf {
    raw_dir.join(format!(
        "research_oi_{exp}_{}_{}.json",
        yyyymmdd(chunk_start),
        yyyymmdd(chunk_end)
    ))
}

fn split_oi_remainder_to_daily(
    chunks: &mut VecDeque<(NaiveDate, NaiveDate)>,
    raw_dir: &Path,
    exp: &str,
    force_refresh: bool,
    failed_start: NaiveDate,
    failed_end: NaiveDate,
) {
    let mut ranges = Vec::with_capacity(chunks.len() + 1);
    ranges.push((failed_start, failed_end));
    ranges.extend(chunks.drain(..));
    for (start, end) in ranges.into_iter().rev() {
        if start == end || (!force_refresh && oi_cache_path(raw_dir, exp, start, end).exists()) {
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

async fn fetch_cached_json(url: &str, path: &Path, force_refresh: bool) -> Result<Value> {
    if !force_refresh && let Some(json) = read_cached_json(path)? {
        return Ok(json);
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
    for (expiration, rows) in rows_by_expiration {
        let mut by_date: BTreeMap<NaiveDate, Vec<&OptionDay>> = BTreeMap::new();
        let underlying_by_date = underlying_by_date(rows);
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
            for short in &day_rows {
                let short_delta = short.delta.abs();
                if short_delta < profile.min_short_delta_abs
                    || short_delta > profile.max_short_delta_abs
                    || short.open_interest < profile.min_short_oi
                    || !quote_width_allowed(short, profile)
                    || !iv_allowed(short, profile)
                {
                    continue;
                }
                let Some(entry_regime) = entry_regime(short, profile, &underlying_by_date) else {
                    continue;
                };
                for long in &day_rows {
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
                    let credit = short.bid - long.ask;
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
                        entry_date: date,
                        expiration: *expiration,
                        short: (*short).clone(),
                        long: (*long).clone(),
                        width,
                        credit,
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
    }
    candidates.sort_by(candidate_chronological_order);
    candidates
}

fn underlying_by_date(rows: &[OptionDay]) -> BTreeMap<NaiveDate, f64> {
    let mut out = BTreeMap::new();
    for row in rows {
        if row.underlying_price > 0.0 {
            out.entry(row.date).or_insert(row.underlying_price);
        }
    }
    out
}

fn entry_regime(
    short: &OptionDay,
    profile: &ResearchProfile,
    underlying_by_date: &BTreeMap<NaiveDate, f64>,
) -> Option<EntryRegime> {
    if short.underlying_price <= 0.0 {
        return None;
    }
    let short_otm_pct = (short.underlying_price - short.strike) / short.underlying_price;
    if let Some(min_short_otm_pct) = profile.min_short_otm_pct
        && short_otm_pct < min_short_otm_pct
    {
        return None;
    }

    let underlying_lookback_return = if let Some(days) = profile.trend_lookback_days {
        let lookback_return = underlying_return(short.date, days, underlying_by_date)?;
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
        let drawdown = underlying_drawdown(short.date, days, underlying_by_date)?;
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

    let underlying_realized_vol = if let Some(days) = profile.realized_vol_lookback_days {
        let realized_vol = underlying_realized_vol(short.date, days, underlying_by_date)?;
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
    for (date, mut day_candidates) in by_date {
        if date < next_entry_date {
            continue;
        }
        day_candidates.sort_by(|a, b| candidate_quality_order(a, b, profile));
        for candidate in day_candidates {
            if let Some(trade) = simulate_candidate(candidate, &lookup, profile) {
                next_entry_date = next_entry_date_after_trade(&trade, profile);
                trades.push(trade);
                break;
            }
        }
    }
    trades
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
    let take_profit_debit = candidate.credit * (1.0 - profile.take_profit_pct);
    let stop_debit = candidate.credit * profile.stop_loss_multiple;

    for (date, short) in short_rows.range((candidate.entry_date + Duration::days(1))..) {
        let days_held = (*date - candidate.entry_date).num_days();
        let dte = (candidate.expiration - *date).num_days();
        let Some(long) = long_rows.get(date) else {
            continue;
        };
        let debit = (short.ask - long.bid).clamp(0.0, candidate.width);
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

fn build_trade(
    candidate: &Candidate,
    exit_date: NaiveDate,
    exit_debit: f64,
    reason: &str,
) -> ResearchTrade {
    let pnl = (candidate.credit - exit_debit) * 100.0;
    let max_profit = candidate.credit * 100.0;
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
        entry_credit: candidate.credit,
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
    let required_trades = required_trades_for_ranking(from, to);
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

fn required_trades_for_ranking(from: NaiveDate, to: NaiveDate) -> usize {
    let years = ((to - from).num_days().max(1) as f64) / 365.25;
    MIN_RANKING_TRADES.max((years * MIN_RANKING_TRADES_PER_YEAR).ceil() as usize)
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
    let required_trades = required_trades_for_ranking(from, to);
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

fn research_markdown(report: &ResearchReport) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "# SpreadFoundry {} Research {}\n\n",
        report.symbol, report.run_id
    ));
    out.push_str(&format!(
        "- Requested window: `{}` to `{}`\n- Effective research window: `{}` to `{}`\n- Expirations discovered: `{}`\n- Expirations skipped before data: `{}`\n- Expirations loaded: `{}`\n- Expirations failed: `{}`\n- EOD rows loaded: `{}`\n- Ranking gate: profiles need at least `{}` trades for this window\n\n",
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
        "- Status: `{}`\n- Best-profile robust gate: `{}`\n- Walk-forward OOS gate: `{}` (trades `{}`, PnL `{:.2}`, score `{:.4}`)\n- Holdout OOS gate: `{}` (trades `{}`, PnL `{:.2}`, score `{:.4}`)\n",
        gate.status,
        format_gate(gate.best_profile_gate),
        format_gate(gate.walk_forward_oos_gate),
        report.walk_forward.metrics.trades,
        report.walk_forward.metrics.total_pnl,
        report.walk_forward.metrics.score,
        format_gate(gate.holdout_oos_gate),
        report.holdout.metrics.trades,
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
        "- Minimum training window: `{}` days\n- OOS years: `{}`\n- Active OOS years: `{}`\n- OOS trades: `{}`\n- OOS PnL: `{:.2}`\n- OOS win rate: `{:.1}%`\n- OOS profit factor: `{:.2}`\n- OOS max DD: `{:.3}`\n- OOS score: `{:.4}`\n- Selected profiles: `{}`\n\n",
        wf.min_train_days,
        wf.years.len(),
        active_years,
        wf.metrics.trades,
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
        "- Training window: `{}` days\n- OOS years: `{}`\n- Active OOS years: `{}`\n- OOS trades: `{}`\n- OOS PnL: `{:.2}`\n- OOS win rate: `{:.1}%`\n- OOS profit factor: `{:.2}`\n- OOS max DD: `{:.3}`\n- OOS score: `{:.4}`\n- Selected profiles: `{}`\n\n",
        rolling.train_window_days.unwrap_or(rolling.min_train_days),
        rolling.years.len(),
        rolling_active_years,
        rolling.metrics.trades,
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
        "- Train window: `{}` to `{}`\n- Test window: `{}` to `{}`\n- Active: `{}`\n- Selected profile: `{}`\n- Train robust eligible: `{}`\n- Train robust score gate: `{}` (min `{:.4}`)\n- Train trades: `{}`\n- Recent 365D train trades: `{}`\n- Last train entry: `{}`\n- Days since last train entry: `{}`\n- Train PnL: `{:.2}`\n- Train robust score: `{:.4}`\n- Test trades: `{}`\n- Test PnL: `{:.2}`\n- Test win rate: `{:.1}%`\n- Test profit factor: `{:.2}`\n- Test max DD: `{:.3}`\n- Test score: `{:.4}`\n\n",
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
    out.push_str("| Rank | Profile | OOS Pass | Active Years | Trades | PnL | Avg ROR | Win Rate | Profit Factor | Max DD | Score | Robust Score | Detector | Execution |\n");
    out.push_str("|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|\n");
    for (idx, result) in report
        .fixed_profile_walk_forward
        .iter()
        .take(15)
        .enumerate()
    {
        let m = &result.metrics;
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {:.2} | {:.3} | {:.1}% | {:.2} | {:.3} | {:.4} | {:.4} | {} | {} |\n",
            idx + 1,
            result.profile.name,
            if out_of_sample_gate_passes(m) {
                "yes"
            } else {
                "no"
            },
            result.active_years,
            m.trades,
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

#[cfg(test)]
mod tests {
    use super::*;

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
            max_regime_lookback_days: 60,
        };

        let (start, end) = expiration_load_window(expiration, bounds).unwrap();

        assert_eq!(start, NaiveDate::from_ymd_opt(2019, 12, 6).unwrap());
        assert_eq!(end, to);
    }

    #[test]
    fn symbol_slug_is_filesystem_safe() {
        assert_eq!(symbol_slug("NVDA"), "nvda");
        assert_eq!(symbol_slug("BRK.B"), "brk-b");
    }

    #[test]
    fn strategy_summaries_separate_detector_from_execution_rules() {
        let detector_profile = ResearchProfile::baseline();
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
        let mut profile = ResearchProfile::baseline();
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

        assert!(entry_regime(&passing_short, &profile, &underlying).is_some());
        assert!(entry_regime(&close_short, &profile, &underlying).is_none());

        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(), 110.0);
        assert!(entry_regime(&passing_short, &profile, &underlying).is_none());

        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(), 80.0);
        assert!(entry_regime(&passing_short, &profile, &underlying).is_none());
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
        let mut profile = ResearchProfile::baseline();
        profile.realized_vol_lookback_days = Some(3);
        profile.max_realized_vol = Some(0.10);

        assert!(entry_regime(&short, &profile, &underlying).is_none());

        profile.max_realized_vol = Some(5.0);
        let regime = entry_regime(&short, &profile, &underlying).unwrap();
        assert!(regime.underlying_realized_vol.unwrap() > 0.0);
    }

    #[test]
    fn entry_regime_rejects_excess_recent_drawdown() {
        let mut profile = ResearchProfile::baseline();
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

        assert!(entry_regime(&short, &profile, &underlying).is_none());

        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 11).unwrap(), 110.0);
        let regime = entry_regime(&short, &profile, &underlying).unwrap();
        assert!((regime.underlying_recent_drawdown.unwrap() - (10.0 / 120.0)).abs() < 1e-9);
    }

    #[test]
    fn entry_regime_rejects_insufficient_recent_drawdown() {
        let mut profile = ResearchProfile::baseline();
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

        assert!(entry_regime(&short, &profile, &underlying).is_none());

        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 11).unwrap(), 112.0);
        let regime = entry_regime(&short, &profile, &underlying).unwrap();
        assert!((regime.underlying_recent_drawdown.unwrap() - (8.0 / 120.0)).abs() < 1e-9);
    }

    #[test]
    fn entry_regime_accepts_return_or_drawdown_confirmation() {
        let date = NaiveDate::from_ymd_opt(2026, 1, 11).unwrap();
        let short = option_day(date, 95.0, 1.0, 1.1, -0.25, 110.0);
        let mut profile = ResearchProfile::baseline();
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

        assert!(entry_regime(&short, &profile, &underlying).is_none());

        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 8).unwrap(), 120.0);
        assert!(entry_regime(&short, &profile, &underlying).is_some());

        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 8).unwrap(), 112.0);
        underlying.insert(date, 125.0);
        let strong_trend_short = option_day(date, 95.0, 1.0, 1.1, -0.25, 125.0);
        assert!(entry_regime(&strong_trend_short, &profile, &underlying).is_some());
    }

    #[test]
    fn entry_regime_rejects_strong_trend_pullback_exhaustion() {
        let date = NaiveDate::from_ymd_opt(2026, 1, 11).unwrap();
        let short = option_day(date, 95.0, 1.0, 1.1, -0.25, 130.0);
        let mut profile = ResearchProfile::baseline();
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

        assert!(entry_regime(&short, &profile, &underlying).is_none());

        underlying.insert(date, 136.0);
        let shallow_pullback_short = option_day(date, 95.0, 1.0, 1.1, -0.25, 136.0);
        assert!(entry_regime(&shallow_pullback_short, &profile, &underlying).is_some());

        underlying.insert(NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(), 120.0);
        underlying.insert(date, 130.0);
        let weaker_trend_short = option_day(date, 95.0, 1.0, 1.1, -0.25, 130.0);
        assert!(entry_regime(&weaker_trend_short, &profile, &underlying).is_some());
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

        let candidates =
            generate_candidates(&rows, &ResearchProfile::baseline(), entry_date, entry_date);

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].entry_date, entry_date);
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
                NaiveDate::from_ymd_opt(2026, 6, 18).unwrap()
            ),
            10
        );
        assert_eq!(
            required_trades_for_ranking(
                NaiveDate::from_ymd_opt(2016, 12, 6).unwrap(),
                NaiveDate::from_ymd_opt(2026, 6, 18).unwrap()
            ),
            20
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
        let report = ResearchReport {
            run_id: "test-run".to_owned(),
            symbol: "NVDA".to_owned(),
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
        assert!(markdown.contains("## Out-of-Sample Failure Summary"));
        assert!(markdown.contains("Inactive walk-forward years from weak train edge: `0`"));
        assert!(markdown.contains("Holdout active: `yes`"));
        assert!(markdown.contains("Requested window: `2012-01-01` to `2022-12-31`"));
        assert!(markdown.contains("Effective research window: `2020-01-01` to `2022-12-31`"));
        assert!(markdown.contains("Expirations skipped before data: `2`"));
        assert!(markdown.contains("- Status: `blocked`"));
        assert!(markdown.contains("- Research deployment gate: `blocked`"));
        assert!(markdown.contains("latest signals are research candidates only"));

        let json = serde_json::to_value(&report).unwrap();
        assert_eq!(json["deployment_gate"]["status"], "blocked");
        assert_eq!(json["deployment_gate"]["pass"], false);
        assert_eq!(json["plateau_status"]["status"], "continue_symbol_research");
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
                .contains("five liquid single-stock symbols")
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
        let mut profile = ResearchProfile::baseline();
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
        let mut profile = ResearchProfile::baseline();
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
        let mut profile = ResearchProfile::baseline();
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
        let mut profile = ResearchProfile::baseline();
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
        fs::write(oi_cache_path(&raw_dir, exp, cached_start, cached_end), "{}").unwrap();
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
    fn farther_otm_selector_does_not_change_default_credit_chasing() {
        let date = NaiveDate::from_ymd_opt(2026, 1, 11).unwrap();
        let close_high_credit = candidate_for_ordering(date, 95.0, 90.0, 1.25, -0.30, 0.05);
        let far_lower_credit = candidate_for_ordering(date, 90.0, 85.0, 1.05, -0.20, 0.10);
        let baseline = ResearchProfile::baseline();
        let mut farther_otm = ResearchProfile::baseline();
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
        let mut profile = ResearchProfile::baseline();
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
        let mut profile = ResearchProfile::baseline();
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
}
