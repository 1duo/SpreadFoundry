use anyhow::{Context, Result};
use chrono::{NaiveDate, Utc};
use clap::{Parser, ValueEnum};
use futures::{StreamExt, stream};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use spreadfoundry::broker::{
    BrokerCapabilities, RobinhoodBrokerAdapter, RobinhoodMcpCommandExecutor,
    RobinhoodMcpToolRequest, RobinhoodMcpToolResponse,
};
use spreadfoundry::execution::{
    OptionOrderEffect, OptionOrderIntent, OptionOrderLeg, OptionOrderSide, PositionEffect,
    TimeInForce, cash_secured_put_open_intent, debit_spread_open_intent,
};
use spreadfoundry::fixture;
use spreadfoundry::opt::{OptimizationResult, rank_results, score_trades};
use spreadfoundry::report::{read_report_markdown, write_run_report};
use spreadfoundry::research::{
    DEFAULT_PLATEAU_UNIVERSE_SYMBOLS, DEFAULT_PLATEAU_UNIVERSE_SYMBOLS_CSV, DEFAULT_RESEARCH_FROM,
    DEFAULT_WEEKLY_RESEARCH_SYMBOLS, DEFAULT_WEEKLY_RESEARCH_SYMBOLS_CSV, DetectorStrategySummary,
    ExecutionStrategySummary, OptionCacheCoverageReport, OptionCacheCoverageRequest,
    PortfolioWheelReport, PortfolioWheelResearchRequest, ResearchMetrics, ResearchProfileFamily,
    ResearchReport, ResearchRequest, WarmOptionCacheCoverageReport, WarmOptionCacheCoverageRequest,
    WeeklySignalGateAuditReport, WeeklySignalGateAuditRequest, audit_option_cache_coverage,
    audit_weekly_signal_gates, run_portfolio_selector_research, run_portfolio_wheel_research,
    run_symbol_research, warm_option_cache_coverage,
};
use spreadfoundry::sim::{ExitRules, SpreadExitQuote, choose_exit};
use spreadfoundry::strategy::{CandidateFilters, generate_put_spread_candidates};
use spreadfoundry::theta::{ThetaClient, ThetaUniverseRequest};
use spreadfoundry::types::{OptionKey, OptionRight};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration as StdDuration;

const DEFAULT_MAX_ORDER_AGE_SECONDS: u64 = 30 * 60;
const UNIVERSE_SELECTION_BASIS: &str = "Plateau expansion uses eight non-NVDA single stocks chosen for liquid weekly option chains, usable put-spread premium, and enough business-model diversity to test whether the detector generalizes beyond NVDA.";
const UNIVERSE_RESEARCH_METHOD: &str = "Each symbol independently runs the same Rust put-credit-spread profile grid. Detector rules and execution rules are reported separately; no NVDA profile is copied into another symbol without out-of-sample proof.";
const UNIVERSE_SEED_SCORE_BASIS: &str = "Static pre-research seed score: 3x option liquidity + 2x premium + 2x spread quality + price-fit + diversification + event-risk discipline. Used only to choose the default candidate symbols; actual suitability ranking is research-evidence driven.";
const UNIVERSE_DETECTOR_SCORE_BASIS: &str =
    "Best in-sample detector robust score after chronological and annual stability checks.";
const UNIVERSE_EXECUTION_SCORE_BASIS: &str =
    "Conservative minimum of walk-forward, holdout when active, and best fixed-profile OOS scores.";
const WEEKLY_UNIVERSE_SELECTION_BASIS: &str = "Weekly research starts from IREN, PLTR, ORCL, TSLA, and CRWV because they provide a mix of high-premium growth, liquid single-name weeklies, and newer high-volatility names where 1-14 DTE defined-risk option cadence might be feasible.";
const WEEKLY_RESEARCH_METHOD: &str = "Each symbol independently runs a weekly put-credit-spread grid centered on 1-14 DTE, short puts at or below 30 delta, $1-$25 width caps, one-third profit taking, capped overlap, and conservative bid/ask fills. Ranking requires weekly-style trade cadence and robust PnL/drawdown evidence.";

#[derive(Parser, Debug)]
#[command(name = "spreadfoundry")]
#[command(about = "Rust-only options spread simulation and gated execution research")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand, Debug)]
enum Commands {
    IngestTheta {
        #[arg(long)]
        symbol: String,
        #[arg(long = "from")]
        from_date: NaiveDate,
        #[arg(long = "to")]
        to_date: NaiveDate,
        #[arg(long, default_value = "1m")]
        interval: String,
        #[arg(long, default_value = "data/raw/theta")]
        output_dir: PathBuf,
    },
    Simulate {
        #[arg(long, value_enum)]
        strategy: StrategyArg,
        #[arg(long)]
        config: PathBuf,
    },
    Optimize {
        #[arg(long, value_enum)]
        strategy: StrategyArg,
        #[arg(long)]
        config: PathBuf,
        #[arg(long, value_enum, default_value = "grid")]
        method: OptimizeMethod,
    },
    TrainRanker {
        #[arg(long)]
        config: PathBuf,
    },
    ShadowLive {
        #[arg(long)]
        symbol: String,
        #[arg(long, value_enum)]
        strategy: StrategyArg,
    },
    Report {
        #[arg(long)]
        run: PathBuf,
    },
    ExportPortfolioCanary {
        #[arg(long)]
        run: PathBuf,
        #[arg(long, default_value = "candidates/weekly_selector_canary.json")]
        output: PathBuf,
        #[arg(long, default_value = "weekly_selector_canary")]
        candidate_id: String,
        #[arg(long)]
        frozen_on: Option<NaiveDate>,
    },
    PortfolioCanaryStatus {
        #[arg(long, default_value = "candidates/weekly_selector_canary.json")]
        candidate: PathBuf,
        #[arg(long)]
        as_of: Option<NaiveDate>,
        #[arg(long, default_value_t = false)]
        require_action: bool,
    },
    RunPortfolioCanary {
        #[arg(long, default_value = "candidates/weekly_selector_canary.json")]
        candidate: PathBuf,
        #[arg(long)]
        as_of: Option<NaiveDate>,
        #[arg(long)]
        max_loss: Option<f64>,
        #[arg(long, default_value_t = 45_000.0)]
        account_cash: f64,
        #[arg(long, default_value_t = 1_000.0)]
        debit_max_loss: f64,
        #[arg(long, default_value_t = 35_000.0)]
        wheel_reserve_cap: f64,
        #[arg(long, default_value_t = 11_250.0)]
        free_cash_buffer: f64,
        #[arg(long, default_value_t = 1)]
        max_wheel_positions_per_symbol: usize,
        #[arg(long, default_value_t = false)]
        broker_multi_leg_options: bool,
        #[arg(long, default_value_t = false)]
        broker_cash_secured_puts: bool,
        #[arg(long, default_value_t = false)]
        broker_covered_calls: bool,
        #[arg(long, default_value_t = false)]
        broker_review_ok: bool,
        #[arg(long, default_value_t = false)]
        live_orders_enabled: bool,
        #[arg(long)]
        robinhood_mcp_command: Option<String>,
        #[arg(long, default_value = "var/canary_order_ledger.json")]
        order_ledger: PathBuf,
        #[arg(long, default_value_t = DEFAULT_MAX_ORDER_AGE_SECONDS)]
        max_order_age_seconds: u64,
        #[arg(long, default_value_t = false)]
        place_live_order: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    CanaryWorker {
        #[arg(long, default_value = "candidates/weekly_selector_canary.json")]
        candidate: PathBuf,
        #[arg(long)]
        as_of: Option<NaiveDate>,
        #[arg(long, default_value_t = 45_000.0)]
        account_cash: f64,
        #[arg(long, default_value_t = 1_000.0)]
        debit_max_loss: f64,
        #[arg(long, default_value_t = 35_000.0)]
        wheel_reserve_cap: f64,
        #[arg(long, default_value_t = 11_250.0)]
        free_cash_buffer: f64,
        #[arg(long, default_value_t = 1)]
        max_wheel_positions_per_symbol: usize,
        #[arg(long, default_value_t = false)]
        broker_multi_leg_options: bool,
        #[arg(long, default_value_t = false)]
        broker_cash_secured_puts: bool,
        #[arg(long, default_value_t = false)]
        broker_covered_calls: bool,
        #[arg(long, default_value_t = false)]
        broker_review_ok: bool,
        #[arg(long, default_value_t = false)]
        live_orders_enabled: bool,
        #[arg(long)]
        robinhood_mcp_command: Option<String>,
        #[arg(long, default_value = "var/canary_order_ledger.json")]
        order_ledger: PathBuf,
        #[arg(long, default_value_t = DEFAULT_MAX_ORDER_AGE_SECONDS)]
        max_order_age_seconds: u64,
        #[arg(long, default_value_t = false)]
        place_live_order: bool,
        #[arg(long, default_value_t = 60)]
        poll_seconds: u64,
        #[arg(long, default_value_t = false)]
        once: bool,
        #[arg(long)]
        health_output: Option<PathBuf>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    AuditOptionCacheCoverage {
        #[arg(
            long,
            value_delimiter = ',',
            default_value = DEFAULT_WEEKLY_RESEARCH_SYMBOLS_CSV
        )]
        symbols: Vec<String>,
        #[arg(long, default_value = DEFAULT_RESEARCH_FROM)]
        from: NaiveDate,
        #[arg(long)]
        to: NaiveDate,
        #[arg(long)]
        max_expirations: Option<usize>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    WarmOptionCacheCoverage {
        #[arg(
            long,
            value_delimiter = ',',
            default_value = DEFAULT_WEEKLY_RESEARCH_SYMBOLS_CSV
        )]
        symbols: Vec<String>,
        #[arg(long, default_value = DEFAULT_RESEARCH_FROM)]
        from: NaiveDate,
        #[arg(long)]
        to: NaiveDate,
        #[arg(long)]
        max_expirations: Option<usize>,
        #[arg(long, default_value_t = 8)]
        max_windows_per_symbol: usize,
        #[arg(long, default_value_t = 2)]
        fetch_concurrency: usize,
        #[arg(long, default_value_t = false)]
        force_refresh: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    AuditWeeklySignalGates {
        #[arg(long)]
        symbol: String,
        #[arg(long, default_value = DEFAULT_RESEARCH_FROM)]
        from: NaiveDate,
        #[arg(long)]
        to: NaiveDate,
        #[arg(long)]
        max_expirations: Option<usize>,
        #[arg(long, default_value_t = 4)]
        fetch_concurrency: usize,
        #[arg(long, default_value_t = false)]
        force_refresh: bool,
        #[arg(long, default_value_t = false)]
        cache_only: bool,
        #[arg(long, value_enum, default_value_t = ProfileFamilyArg::Weekly)]
        profile_family: ProfileFamilyArg,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    ResearchNvda {
        #[arg(long, default_value = DEFAULT_RESEARCH_FROM)]
        from: NaiveDate,
        #[arg(long)]
        to: NaiveDate,
        #[arg(long)]
        max_expirations: Option<usize>,
        #[arg(long, default_value_t = 4)]
        fetch_concurrency: usize,
        #[arg(long, default_value_t = false)]
        force_refresh: bool,
        #[arg(long, default_value_t = false)]
        cache_only: bool,
        #[arg(long, default_value_t = false, conflicts_with = "single_symbol_only")]
        expand_on_plateau: bool,
        #[arg(
            long = "single-symbol-only",
            alias = "no-expand-on-plateau",
            default_value_t = false
        )]
        single_symbol_only: bool,
    },
    ResearchSymbol {
        #[arg(long)]
        symbol: String,
        #[arg(long, value_enum, default_value_t = ProfileFamilyArg::Swing)]
        profile_family: ProfileFamilyArg,
        #[arg(long, default_value = DEFAULT_RESEARCH_FROM)]
        from: NaiveDate,
        #[arg(long)]
        to: NaiveDate,
        #[arg(long)]
        max_expirations: Option<usize>,
        #[arg(long, default_value_t = 4)]
        fetch_concurrency: usize,
        #[arg(long, default_value_t = false)]
        force_refresh: bool,
        #[arg(long, default_value_t = false)]
        cache_only: bool,
        #[arg(long, default_value_t = false, conflicts_with = "single_symbol_only")]
        expand_on_plateau: bool,
        #[arg(
            long = "single-symbol-only",
            alias = "no-expand-on-plateau",
            default_value_t = false
        )]
        single_symbol_only: bool,
    },
    ResearchUniverse {
        #[arg(
            long,
            value_delimiter = ',',
            default_value = DEFAULT_PLATEAU_UNIVERSE_SYMBOLS_CSV
        )]
        symbols: Vec<String>,
        #[arg(long)]
        plateau_run: Option<PathBuf>,
        #[arg(long, default_value = DEFAULT_RESEARCH_FROM)]
        from: NaiveDate,
        #[arg(long)]
        to: NaiveDate,
        #[arg(long)]
        max_expirations: Option<usize>,
        #[arg(long, default_value_t = 4)]
        fetch_concurrency: usize,
        #[arg(long, default_value_t = false)]
        force_refresh: bool,
        #[arg(long, default_value_t = false)]
        cache_only: bool,
        #[arg(long, default_value_t = false)]
        allow_pre_plateau: bool,
        #[arg(long, default_value_t = 4)]
        symbol_concurrency: usize,
        #[arg(long, value_enum, default_value_t = ProfileFamilyArg::Swing)]
        profile_family: ProfileFamilyArg,
    },
    ResearchWeeklyUniverse {
        #[arg(
            long,
            value_delimiter = ',',
            default_value = DEFAULT_WEEKLY_RESEARCH_SYMBOLS_CSV
        )]
        symbols: Vec<String>,
        #[arg(long, default_value = DEFAULT_RESEARCH_FROM)]
        from: NaiveDate,
        #[arg(long)]
        to: NaiveDate,
        #[arg(long)]
        max_expirations: Option<usize>,
        #[arg(long, default_value_t = 8)]
        fetch_concurrency: usize,
        #[arg(long, default_value_t = false)]
        force_refresh: bool,
        #[arg(long, default_value_t = false)]
        cache_only: bool,
        #[arg(long, default_value_t = 5)]
        symbol_concurrency: usize,
        #[arg(long, value_enum, default_value_t = ProfileFamilyArg::WeeklyPutDebit)]
        profile_family: ProfileFamilyArg,
    },
    ResearchPortfolioWheel {
        #[arg(
            long,
            value_delimiter = ',',
            default_value = DEFAULT_WEEKLY_RESEARCH_SYMBOLS_CSV
        )]
        symbols: Vec<String>,
        #[arg(long, default_value = DEFAULT_RESEARCH_FROM)]
        from: NaiveDate,
        #[arg(long)]
        to: NaiveDate,
        #[arg(long)]
        max_expirations: Option<usize>,
        #[arg(long, default_value_t = 2)]
        fetch_concurrency: usize,
        #[arg(long, default_value_t = 1)]
        symbol_concurrency: usize,
        #[arg(long, default_value_t = false)]
        force_refresh: bool,
        #[arg(long, default_value_t = false)]
        cache_only: bool,
        #[arg(long, default_value_t = 100_000.0)]
        capital_budget: f64,
        #[arg(long, default_value_t = 0.35)]
        max_symbol_allocation_pct: f64,
        #[arg(long, default_value_t = 5)]
        max_open_positions: usize,
        #[arg(long, default_value_t = 2)]
        max_positions_per_symbol: usize,
        #[arg(long)]
        max_total_trades_per_symbol: Option<usize>,
        #[arg(long)]
        portfolio_drawdown_cooldown_trigger_pct: Option<f64>,
        #[arg(long, default_value_t = 0)]
        portfolio_drawdown_cooldown_days: i64,
        #[arg(long)]
        symbol_drawdown_cooldown_trigger_pct: Option<f64>,
        #[arg(long, default_value_t = 0)]
        symbol_drawdown_cooldown_days: i64,
    },
    ResearchPortfolioSelector {
        #[arg(
            long,
            value_delimiter = ',',
            default_value = DEFAULT_WEEKLY_RESEARCH_SYMBOLS_CSV
        )]
        symbols: Vec<String>,
        #[arg(long, default_value = DEFAULT_RESEARCH_FROM)]
        from: NaiveDate,
        #[arg(long)]
        to: NaiveDate,
        #[arg(long)]
        max_expirations: Option<usize>,
        #[arg(long, default_value_t = 2)]
        fetch_concurrency: usize,
        #[arg(long, default_value_t = 1)]
        symbol_concurrency: usize,
        #[arg(long, default_value_t = false)]
        force_refresh: bool,
        #[arg(long, default_value_t = false)]
        cache_only: bool,
        #[arg(long, default_value_t = 100_000.0)]
        capital_budget: f64,
        #[arg(long, default_value_t = 0.35)]
        max_symbol_allocation_pct: f64,
        #[arg(long, default_value_t = 5)]
        max_open_positions: usize,
        #[arg(long, default_value_t = 2)]
        max_positions_per_symbol: usize,
        #[arg(long)]
        max_total_trades_per_symbol: Option<usize>,
        #[arg(long)]
        portfolio_drawdown_cooldown_trigger_pct: Option<f64>,
        #[arg(long, default_value_t = 0)]
        portfolio_drawdown_cooldown_days: i64,
        #[arg(long)]
        symbol_drawdown_cooldown_trigger_pct: Option<f64>,
        #[arg(long, default_value_t = 0)]
        symbol_drawdown_cooldown_days: i64,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum StrategyArg {
    PutSpread,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum OptimizeMethod {
    Grid,
    Random,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum ProfileFamilyArg {
    Swing,
    Weekly,
    WeeklyFarOtm,
    WeeklyPutDebit,
    WeeklyCallCredit,
    WeeklyCallDebit,
    WeeklyWheel,
}

impl From<ProfileFamilyArg> for ResearchProfileFamily {
    fn from(value: ProfileFamilyArg) -> Self {
        match value {
            ProfileFamilyArg::Swing => ResearchProfileFamily::Swing,
            ProfileFamilyArg::Weekly => ResearchProfileFamily::Weekly,
            ProfileFamilyArg::WeeklyFarOtm => ResearchProfileFamily::WeeklyFarOtm,
            ProfileFamilyArg::WeeklyPutDebit => ResearchProfileFamily::WeeklyPutDebit,
            ProfileFamilyArg::WeeklyCallCredit => ResearchProfileFamily::WeeklyCallCredit,
            ProfileFamilyArg::WeeklyCallDebit => ResearchProfileFamily::WeeklyCallDebit,
            ProfileFamilyArg::WeeklyWheel => ResearchProfileFamily::WeeklyWheel,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SimulationConfig {
    symbol: String,
    data_mode: String,
    quantity: u32,
    fees: Decimal,
    fixture_exit: Option<String>,
    filters: Option<CandidateFilters>,
    exit: Option<ExitRules>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OptimizationConfig {
    symbol: String,
    data_mode: String,
    quantity: u32,
    fees: Decimal,
    credit_width_ratios: Vec<Decimal>,
    max_widths: Vec<Decimal>,
    filters: Option<CandidateFilters>,
    exit: Option<ExitRules>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GridParams {
    min_credit_width_ratio: Decimal,
    max_width: Decimal,
}

#[derive(Debug, Serialize)]
struct UniverseResearchSummary {
    run_id: String,
    run_status: String,
    profile_family: String,
    from: NaiveDate,
    to: NaiveDate,
    symbols: Vec<String>,
    symbols_requested: usize,
    symbols_completed: usize,
    plateau_run: Option<String>,
    max_expirations: Option<usize>,
    fetch_concurrency: usize,
    force_refresh: bool,
    cache_only: bool,
    strategy: String,
    selection_basis: String,
    research_method: String,
    seed_score_basis: String,
    detector_score_basis: String,
    execution_score_basis: String,
    expansion_seed: Vec<UniverseSeedSymbol>,
    results: Vec<UniverseSymbolSummary>,
}

#[derive(Clone, Debug, Serialize)]
struct UniverseSeedSymbol {
    rank: usize,
    symbol: String,
    role: String,
    rationale: String,
    suitability_score: Option<u16>,
    liquidity_score: Option<u8>,
    premium_score: Option<u8>,
    spread_quality_score: Option<u8>,
    price_fit_score: Option<u8>,
    diversification_score: Option<u8>,
    event_risk_score: Option<u8>,
}

#[derive(Clone, Copy, Debug)]
struct UniverseSeedCandidate {
    symbol: &'static str,
    role: &'static str,
    rationale: &'static str,
    liquidity_score: u8,
    premium_score: u8,
    spread_quality_score: u8,
    price_fit_score: u8,
    diversification_score: u8,
    event_risk_score: u8,
}

#[derive(Debug, Serialize)]
struct UniverseSymbolSummary {
    suitability_rank: usize,
    symbol: String,
    seed_rank: Option<usize>,
    seed_suitability_score: Option<u16>,
    seed_role: Option<String>,
    seed_rationale: Option<String>,
    research_status: String,
    error_message: Option<String>,
    report_dir: String,
    deployment_status: String,
    plateau_status: String,
    detector_status: String,
    execution_strategy_status: String,
    expansion_ready: bool,
    expirations_loaded: usize,
    rows_loaded: usize,
    profiles_evaluated: usize,
    best_profile: String,
    best_detector: String,
    best_execution: String,
    best_detector_details: Option<DetectorStrategySummary>,
    best_execution_details: Option<ExecutionStrategySummary>,
    detector_score: f64,
    execution_oos_score: f64,
    trades: usize,
    total_pnl: f64,
    score: f64,
    robust_score: f64,
    walk_forward_trades: usize,
    walk_forward_pnl: f64,
    walk_forward_score: f64,
    holdout_trades: usize,
    holdout_pnl: f64,
    holdout_score: f64,
    fixed_profile_oos_passes: usize,
    best_fixed_profile: String,
    best_fixed_detector: String,
    best_fixed_execution: String,
    best_fixed_detector_details: Option<DetectorStrategySummary>,
    best_fixed_execution_details: Option<ExecutionStrategySummary>,
    best_fixed_trades: usize,
    best_fixed_pnl: f64,
    best_fixed_score: f64,
    best_fixed_robust_score: f64,
    latest_signal_status: Option<String>,
}

#[derive(Debug)]
struct UniverseResearchArgs {
    symbols: Vec<String>,
    plateau_run: Option<PathBuf>,
    profile_family: ResearchProfileFamily,
    from: NaiveDate,
    to: NaiveDate,
    max_expirations: Option<usize>,
    fetch_concurrency: usize,
    force_refresh: bool,
    cache_only: bool,
    allow_pre_plateau: bool,
    symbol_concurrency: usize,
}

#[derive(Debug, Deserialize)]
struct PlateauRunGate {
    plateau_status: PlateauRunStatus,
}

#[derive(Debug, Deserialize)]
struct PlateauRunStatus {
    status: String,
    expansion_ready: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::IngestTheta {
            symbol,
            from_date,
            to_date,
            interval,
            output_dir,
        } => ingest_theta(symbol, from_date, to_date, interval, output_dir).await,
        Commands::Simulate { strategy, config } => match strategy {
            StrategyArg::PutSpread => simulate_put_spread(&config),
        },
        Commands::Optimize {
            strategy,
            config,
            method,
        } => match strategy {
            StrategyArg::PutSpread => optimize_put_spread(&config, method),
        },
        Commands::TrainRanker { config } => train_ranker(&config),
        Commands::ShadowLive { symbol, strategy } => shadow_live(&symbol, strategy),
        Commands::Report { run } => {
            println!("{}", read_report_markdown(run)?);
            Ok(())
        }
        Commands::ExportPortfolioCanary {
            run,
            output,
            candidate_id,
            frozen_on,
        } => export_portfolio_canary(&run, &output, &candidate_id, frozen_on),
        Commands::PortfolioCanaryStatus {
            candidate,
            as_of,
            require_action,
        } => portfolio_canary_status(&candidate, as_of, require_action),
        Commands::RunPortfolioCanary {
            candidate,
            as_of,
            max_loss,
            account_cash,
            debit_max_loss,
            wheel_reserve_cap,
            free_cash_buffer,
            max_wheel_positions_per_symbol,
            broker_multi_leg_options,
            broker_cash_secured_puts,
            broker_covered_calls,
            broker_review_ok,
            live_orders_enabled,
            robinhood_mcp_command,
            order_ledger,
            max_order_age_seconds,
            place_live_order,
            json,
        } => run_portfolio_canary(
            &candidate,
            as_of,
            max_loss,
            account_cash,
            debit_max_loss,
            wheel_reserve_cap,
            free_cash_buffer,
            max_wheel_positions_per_symbol,
            broker_multi_leg_options,
            broker_cash_secured_puts,
            broker_covered_calls,
            broker_review_ok,
            live_orders_enabled,
            robinhood_mcp_command,
            order_ledger,
            max_order_age_seconds,
            place_live_order,
            json,
        ),
        Commands::CanaryWorker {
            candidate,
            as_of,
            account_cash,
            debit_max_loss,
            wheel_reserve_cap,
            free_cash_buffer,
            max_wheel_positions_per_symbol,
            broker_multi_leg_options,
            broker_cash_secured_puts,
            broker_covered_calls,
            broker_review_ok,
            live_orders_enabled,
            robinhood_mcp_command,
            order_ledger,
            max_order_age_seconds,
            place_live_order,
            poll_seconds,
            once,
            health_output,
            json,
        } => {
            run_canary_worker(CanaryWorkerArgs {
                candidate,
                as_of,
                risk: CanaryRiskConfig {
                    account_cash,
                    debit_max_loss,
                    wheel_reserve_cap,
                    free_cash_buffer,
                    max_wheel_positions_per_symbol,
                },
                broker: canary_broker(
                    broker_multi_leg_options,
                    broker_cash_secured_puts,
                    broker_covered_calls,
                    live_orders_enabled,
                ),
                robinhood_mcp_command,
                order_ledger,
                max_order_age_seconds,
                broker_review_ok,
                place_live_order,
                poll_seconds,
                once,
                health_output,
                json,
            })
            .await
        }
        Commands::AuditOptionCacheCoverage {
            symbols,
            from,
            to,
            max_expirations,
            json,
        } => {
            let symbols = if symbols.is_empty() {
                DEFAULT_WEEKLY_RESEARCH_SYMBOLS
                    .iter()
                    .map(|symbol| (*symbol).to_owned())
                    .collect()
            } else {
                symbols
            };
            let report = audit_option_cache_coverage(OptionCacheCoverageRequest {
                symbols,
                from,
                to,
                max_expirations,
            })
            .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                print_option_cache_coverage_report(&report);
            }
            Ok(())
        }
        Commands::WarmOptionCacheCoverage {
            symbols,
            from,
            to,
            max_expirations,
            max_windows_per_symbol,
            fetch_concurrency,
            force_refresh,
            json,
        } => {
            let symbols = if symbols.is_empty() {
                DEFAULT_WEEKLY_RESEARCH_SYMBOLS
                    .iter()
                    .map(|symbol| (*symbol).to_owned())
                    .collect()
            } else {
                symbols
            };
            let report = warm_option_cache_coverage(WarmOptionCacheCoverageRequest {
                symbols,
                from,
                to,
                max_expirations,
                max_windows_per_symbol,
                fetch_concurrency,
                force_refresh,
            })
            .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                print_warm_option_cache_coverage_report(&report);
            }
            Ok(())
        }
        Commands::AuditWeeklySignalGates {
            symbol,
            from,
            to,
            max_expirations,
            fetch_concurrency,
            force_refresh,
            cache_only,
            profile_family,
            json,
        } => {
            let report = audit_weekly_signal_gates(WeeklySignalGateAuditRequest {
                symbol: symbol.to_uppercase(),
                from,
                to,
                max_expirations,
                fetch_concurrency,
                force_refresh,
                cache_only,
                profile_family: profile_family.into(),
            })
            .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                print_weekly_signal_gate_audit_report(&report);
            }
            Ok(())
        }
        Commands::ResearchNvda {
            from,
            to,
            max_expirations,
            fetch_concurrency,
            force_refresh,
            cache_only,
            expand_on_plateau,
            single_symbol_only,
        } => {
            research_symbol_and_optional_universe(ResearchCommandArgs {
                symbol: "NVDA".to_owned(),
                profile_family: ResearchProfileFamily::Swing,
                from,
                to,
                max_expirations,
                fetch_concurrency,
                force_refresh,
                cache_only,
                expand_on_plateau: should_expand_on_plateau(expand_on_plateau, single_symbol_only),
            })
            .await
        }
        Commands::ResearchSymbol {
            symbol,
            profile_family,
            from,
            to,
            max_expirations,
            fetch_concurrency,
            force_refresh,
            cache_only,
            expand_on_plateau,
            single_symbol_only,
        } => {
            research_symbol_and_optional_universe(ResearchCommandArgs {
                symbol: symbol.to_uppercase(),
                profile_family: profile_family.into(),
                from,
                to,
                max_expirations,
                fetch_concurrency,
                force_refresh,
                cache_only,
                expand_on_plateau: should_expand_on_plateau(expand_on_plateau, single_symbol_only),
            })
            .await
        }
        Commands::ResearchUniverse {
            symbols,
            plateau_run,
            from,
            to,
            max_expirations,
            fetch_concurrency,
            force_refresh,
            cache_only,
            allow_pre_plateau,
            symbol_concurrency,
            profile_family,
        } => {
            research_universe(UniverseResearchArgs {
                symbols,
                plateau_run,
                profile_family: profile_family.into(),
                from,
                to,
                max_expirations,
                fetch_concurrency,
                force_refresh,
                cache_only,
                allow_pre_plateau,
                symbol_concurrency,
            })
            .await
        }
        Commands::ResearchWeeklyUniverse {
            symbols,
            from,
            to,
            max_expirations,
            fetch_concurrency,
            force_refresh,
            cache_only,
            symbol_concurrency,
            profile_family,
        } => {
            let symbols = if symbols.is_empty() {
                DEFAULT_WEEKLY_RESEARCH_SYMBOLS
                    .iter()
                    .map(|symbol| (*symbol).to_owned())
                    .collect()
            } else {
                symbols
            };
            research_universe(UniverseResearchArgs {
                symbols,
                plateau_run: None,
                profile_family: profile_family.into(),
                from,
                to,
                max_expirations,
                fetch_concurrency,
                force_refresh,
                cache_only,
                allow_pre_plateau: true,
                symbol_concurrency,
            })
            .await
        }
        Commands::ResearchPortfolioWheel {
            symbols,
            from,
            to,
            max_expirations,
            fetch_concurrency,
            symbol_concurrency,
            force_refresh,
            cache_only,
            capital_budget,
            max_symbol_allocation_pct,
            max_open_positions,
            max_positions_per_symbol,
            max_total_trades_per_symbol,
            portfolio_drawdown_cooldown_trigger_pct,
            portfolio_drawdown_cooldown_days,
            symbol_drawdown_cooldown_trigger_pct,
            symbol_drawdown_cooldown_days,
        } => {
            let symbols = if symbols.is_empty() {
                DEFAULT_WEEKLY_RESEARCH_SYMBOLS
                    .iter()
                    .map(|symbol| (*symbol).to_owned())
                    .collect()
            } else {
                symbols
            };
            let report = run_portfolio_wheel_research(PortfolioWheelResearchRequest {
                symbols,
                from,
                to,
                max_expirations,
                fetch_concurrency,
                symbol_concurrency,
                force_refresh,
                cache_only,
                capital_budget,
                max_symbol_allocation_pct,
                max_open_positions,
                max_positions_per_symbol,
                max_total_trades_per_symbol,
                portfolio_drawdown_cooldown_trigger_pct,
                portfolio_drawdown_cooldown_days,
                symbol_drawdown_cooldown_trigger_pct,
                symbol_drawdown_cooldown_days,
            })
            .await?;
            if let Some(best) = report.profiles.first() {
                println!(
                    "best={} trades={} pnl={:.2} score={:.4} gate={}",
                    best.profile.name,
                    best.metrics.trades,
                    best.metrics.total_pnl,
                    best.metrics.score,
                    best.gate_status
                );
            }
            println!("{}", PathBuf::from("runs").join(&report.run_id).display());
            Ok(())
        }
        Commands::ResearchPortfolioSelector {
            symbols,
            from,
            to,
            max_expirations,
            fetch_concurrency,
            symbol_concurrency,
            force_refresh,
            cache_only,
            capital_budget,
            max_symbol_allocation_pct,
            max_open_positions,
            max_positions_per_symbol,
            max_total_trades_per_symbol,
            portfolio_drawdown_cooldown_trigger_pct,
            portfolio_drawdown_cooldown_days,
            symbol_drawdown_cooldown_trigger_pct,
            symbol_drawdown_cooldown_days,
        } => {
            let symbols = if symbols.is_empty() {
                DEFAULT_WEEKLY_RESEARCH_SYMBOLS
                    .iter()
                    .map(|symbol| (*symbol).to_owned())
                    .collect()
            } else {
                symbols
            };
            let report = run_portfolio_selector_research(PortfolioWheelResearchRequest {
                symbols,
                from,
                to,
                max_expirations,
                fetch_concurrency,
                symbol_concurrency,
                force_refresh,
                cache_only,
                capital_budget,
                max_symbol_allocation_pct,
                max_open_positions,
                max_positions_per_symbol,
                max_total_trades_per_symbol,
                portfolio_drawdown_cooldown_trigger_pct,
                portfolio_drawdown_cooldown_days,
                symbol_drawdown_cooldown_trigger_pct,
                symbol_drawdown_cooldown_days,
            })
            .await?;
            if let Some(best) = report.profiles.first() {
                println!(
                    "best={} trades={} pnl={:.2} score={:.4} gate={}",
                    best.profile.name,
                    best.metrics.trades,
                    best.metrics.total_pnl,
                    best.metrics.score,
                    best.gate_status
                );
            }
            println!("{}", PathBuf::from("runs").join(&report.run_id).display());
            Ok(())
        }
    }
}

fn export_portfolio_canary(
    run: &Path,
    output: &Path,
    candidate_id: &str,
    frozen_on: Option<NaiveDate>,
) -> Result<()> {
    let report_path = portfolio_report_json_path(run);
    let report: PortfolioWheelReport = serde_json::from_str(
        &fs::read_to_string(&report_path)
            .with_context(|| format!("read portfolio report {}", report_path.display()))?,
    )
    .with_context(|| format!("parse portfolio report {}", report_path.display()))?;
    let best = report
        .profiles
        .iter()
        .find(|profile| profile.canary_readiness.canary_ready)
        .with_context(|| {
            format!(
                "portfolio report {} has no canary-ready profiles; refusing to export candidate",
                report_path.display()
            )
        })?;
    let frozen_on = frozen_on.unwrap_or_else(|| Utc::now().date_naive());
    let latest_action_summary = latest_action_status_counts(best);
    let has_action = best
        .latest_actions
        .iter()
        .any(|action| action.status == "entry_candidate" || action.status == "open_candidate");
    let current_action_state = if has_action {
        format!("fresh_entry_or_open_candidate_present_as_of_{}", report.to)
    } else {
        format!("no_open_or_same_day_entry_actions_as_of_{}", report.to)
    };
    let effective_symbols = report
        .symbols_loaded
        .iter()
        .filter(|symbol| symbol.rows_loaded > 0)
        .map(|symbol| symbol.symbol.clone())
        .collect::<Vec<_>>();
    let source_markdown = report_path
        .parent()
        .map(|parent| parent.join("report.md"))
        .unwrap_or_else(|| PathBuf::from("report.md"));
    let artifact = serde_json::json!({
        "candidate_id": candidate_id,
        "status": best.canary_readiness.status,
        "source_run_id": report.run_id,
        "source_report": report_path,
        "source_markdown_report": source_markdown,
        "frozen_on": frozen_on,
        "exported_at": Utc::now(),
        "strategy": "portfolio_weekly_selector",
        "profile": best.profile.name,
        "decision": {
            "research_gate": best.gate_status,
            "canary_status": best.canary_readiness.status,
            "canary_ready": best.canary_readiness.canary_ready,
            "full_promotion_ready": best.canary_readiness.full_promotion_ready,
            "recommended_capital_fraction": best.canary_readiness.recommended_capital_fraction,
            "action_policy": "wait_for_fresh_entry_or_open_candidate",
            "current_action_state": current_action_state,
            "reason": best.canary_readiness.reason,
        },
        "research_window": {
            "from": report.from,
            "to": report.to,
        },
        "portfolio_constraints": {
            "capital_budget": report.capital_budget,
            "max_symbol_allocation_pct": report.max_symbol_allocation_pct,
            "max_open_positions": report.max_open_positions,
            "max_positions_per_symbol": report.max_positions_per_symbol,
            "max_total_trades_per_symbol": report.max_total_trades_per_symbol,
            "portfolio_drawdown_cooldown_trigger_pct": report.portfolio_drawdown_cooldown_trigger_pct,
            "portfolio_drawdown_cooldown_days": report.portfolio_drawdown_cooldown_days,
            "symbol_drawdown_cooldown_trigger_pct": report.symbol_drawdown_cooldown_trigger_pct,
            "symbol_drawdown_cooldown_days": report.symbol_drawdown_cooldown_days,
        },
        "requested_symbols": report.symbols,
        "effective_symbols": effective_symbols,
        "data_note": "Symbols with zero rows in symbols_loaded did not contribute to this cache-backed candidate.",
        "regeneration_command": portfolio_canary_regeneration_command(&report),
        "metrics": {
            "trades": best.metrics.trades,
            "required_trades": best.metrics.required_trades,
            "trades_per_year": best.metrics.trades_per_year,
            "total_pnl": best.metrics.total_pnl,
            "profit_factor": best.metrics.profit_factor,
            "risk_normalized_max_drawdown": best.metrics.max_drawdown,
            "capital_max_drawdown": best.decision_metrics.max_capital_drawdown,
            "capital_max_drawdown_pct": best.decision_metrics.max_capital_drawdown_pct,
            "cost_25_capital_max_drawdown": best.decision_metrics.cost_25_max_capital_drawdown,
            "cost_25_capital_max_drawdown_pct": best.decision_metrics.cost_25_max_capital_drawdown_pct,
            "cost_10_pnl": cost_stress_pnl(&best.metrics, 10.0),
            "cost_25_pnl": cost_stress_pnl(&best.metrics, 25.0),
        },
        "concentration": {
            "max_symbol": best.canary_readiness.max_symbol,
            "max_symbol_pnl_share": best.canary_readiness.max_symbol_pnl_share,
            "symbol_ablation_passes": best.canary_readiness.symbol_ablation_passes,
            "strategy_ablation_passes": best.canary_readiness.strategy_ablation_passes,
        },
        "strategy_summaries": best.strategy_summaries,
        "risk_summary": best.risk_summary,
        "decision_metrics": best.decision_metrics,
        "latest_action_summary": latest_action_summary,
        "latest_actions": best.latest_actions,
        "ablation_summary": best.ablations.iter().map(|ablation| serde_json::json!({
            "label": ablation.label,
            "gate_status": ablation.gate_status,
            "trades": ablation.metrics.trades,
            "total_pnl": ablation.metrics.total_pnl,
            "cost_25_pnl": cost_stress_pnl(&ablation.metrics, 25.0),
            "gate_reason": ablation.gate_reason,
        })).collect::<Vec<_>>(),
        "pre_canary_requirements": [
            "Regenerate the selector report on fresh option-chain data.",
            "Require latest_actions to contain an entry_candidate or open_candidate before considering action.",
            "Keep canary capital at or below the recommended capital fraction.",
            "Do not upgrade to full promotion while concentration and strategy-sleeve ablation diagnostics remain weak."
        ],
    });

    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create output directory {}", parent.display()))?;
    }
    fs::write(output, serde_json::to_string_pretty(&artifact)?)
        .with_context(|| format!("write canary artifact {}", output.display()))?;
    println!("{}", output.display());
    Ok(())
}

fn portfolio_report_json_path(run: &Path) -> PathBuf {
    if run.is_dir() {
        run.join("portfolio_research.json")
    } else {
        run.to_path_buf()
    }
}

fn latest_action_status_counts(
    result: &spreadfoundry::research::PortfolioWheelProfileResult,
) -> Vec<serde_json::Value> {
    let mut by_status = BTreeMap::new();
    for action in &result.latest_actions {
        *by_status.entry(action.status.clone()).or_insert(0usize) += 1;
    }
    by_status
        .into_iter()
        .map(|(status, count)| serde_json::json!({ "status": status, "count": count }))
        .collect()
}

fn cost_stress_pnl(metrics: &ResearchMetrics, per_trade_cost: f64) -> f64 {
    metrics
        .cost_stress
        .iter()
        .find(|stress| (stress.per_trade_cost - per_trade_cost).abs() < f64::EPSILON)
        .map(|stress| stress.total_pnl)
        .unwrap_or(metrics.total_pnl)
}

fn portfolio_canary_regeneration_command(report: &PortfolioWheelReport) -> String {
    let mut command = format!(
        "cargo run --quiet -- research-portfolio-selector --symbols {} --from {} --to {} --cache-only --fetch-concurrency {} --symbol-concurrency {} --capital-budget {:.0} --max-symbol-allocation-pct {} --max-open-positions {} --max-positions-per-symbol {}",
        report.symbols.join(","),
        report.from,
        report.to,
        report.fetch_concurrency,
        report.symbol_concurrency,
        report.capital_budget,
        report.max_symbol_allocation_pct,
        report.max_open_positions,
        report.max_positions_per_symbol
    );
    if let Some(limit) = report.max_total_trades_per_symbol {
        command.push_str(&format!(" --max-total-trades-per-symbol {limit}"));
    }
    if let Some(trigger) = report.portfolio_drawdown_cooldown_trigger_pct {
        command.push_str(&format!(
            " --portfolio-drawdown-cooldown-trigger-pct {trigger} --portfolio-drawdown-cooldown-days {}",
            report.portfolio_drawdown_cooldown_days
        ));
    }
    if let Some(trigger) = report.symbol_drawdown_cooldown_trigger_pct {
        command.push_str(&format!(
            " --symbol-drawdown-cooldown-trigger-pct {trigger} --symbol-drawdown-cooldown-days {}",
            report.symbol_drawdown_cooldown_days
        ));
    }
    command
}

fn portfolio_canary_status(
    candidate: &Path,
    as_of: Option<NaiveDate>,
    require_action: bool,
) -> Result<()> {
    let as_of = as_of.unwrap_or_else(|| Utc::now().date_naive());
    let artifact: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(candidate)
            .with_context(|| format!("read canary artifact {}", candidate.display()))?,
    )
    .with_context(|| format!("parse canary artifact {}", candidate.display()))?;
    let candidate_id = artifact
        .get("candidate_id")
        .and_then(|value| value.as_str())
        .unwrap_or("unknown");
    let status = artifact
        .get("status")
        .and_then(|value| value.as_str())
        .unwrap_or("unknown");
    let decision = artifact.get("decision").unwrap_or(&serde_json::Value::Null);
    let action_state = decision
        .get("current_action_state")
        .and_then(|value| value.as_str())
        .unwrap_or("unknown");
    let capital_fraction = decision
        .get("recommended_capital_fraction")
        .and_then(|value| value.as_f64())
        .unwrap_or(0.0);
    let actions = artifact
        .get("latest_actions")
        .and_then(|value| value.as_array())
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let actionable = actions
        .iter()
        .any(|action| canary_action_is_fresh(action, as_of));
    println!(
        "candidate={} status={} as_of={} actionable={} action_state={} recommended_capital_fraction={:.2}",
        candidate_id, status, as_of, actionable, action_state, capital_fraction
    );
    if !actions.is_empty() {
        println!("latest_actions={}", actions.len());
        for action in actions.iter().take(5) {
            println!(
                "{} {} {} entry={} exit={} pnl={:.2}",
                action
                    .get("status")
                    .and_then(|value| value.as_str())
                    .unwrap_or("unknown"),
                action
                    .get("symbol")
                    .and_then(|value| value.as_str())
                    .unwrap_or("unknown"),
                action
                    .get("strategy")
                    .and_then(|value| value.as_str())
                    .unwrap_or("unknown"),
                action
                    .get("entry_date")
                    .and_then(|value| value.as_str())
                    .unwrap_or("unknown"),
                action
                    .get("exit_date")
                    .and_then(|value| value.as_str())
                    .unwrap_or("unknown"),
                action
                    .get("pnl")
                    .and_then(|value| value.as_f64())
                    .unwrap_or(0.0)
            );
        }
    }
    if require_action && !actionable {
        anyhow::bail!(
            "no actionable canary signal in {}; regenerate selector report and export a fresh artifact",
            candidate.display()
        );
    }
    Ok(())
}

fn run_portfolio_canary(
    candidate: &Path,
    as_of: Option<NaiveDate>,
    max_loss: Option<f64>,
    account_cash: f64,
    debit_max_loss: f64,
    wheel_reserve_cap: f64,
    free_cash_buffer: f64,
    max_wheel_positions_per_symbol: usize,
    broker_multi_leg_options: bool,
    broker_cash_secured_puts: bool,
    broker_covered_calls: bool,
    broker_review_ok: bool,
    live_orders_enabled: bool,
    robinhood_mcp_command: Option<String>,
    order_ledger: PathBuf,
    max_order_age_seconds: u64,
    place_live_order: bool,
    json: bool,
) -> Result<()> {
    if let Some(max_loss) = max_loss
        && max_loss <= 0.0
    {
        anyhow::bail!("--max-loss must be positive");
    }
    let as_of = as_of.unwrap_or_else(|| Utc::now().date_naive());
    let artifact: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(candidate)
            .with_context(|| format!("read canary artifact {}", candidate.display()))?,
    )
    .with_context(|| format!("parse canary artifact {}", candidate.display()))?;
    let risk = CanaryRiskConfig {
        account_cash,
        debit_max_loss: max_loss.unwrap_or(debit_max_loss),
        wheel_reserve_cap,
        free_cash_buffer,
        max_wheel_positions_per_symbol,
    };
    validate_canary_risk_config(&risk)?;
    let broker = canary_broker(
        broker_multi_leg_options,
        broker_cash_secured_puts,
        broker_covered_calls,
        live_orders_enabled,
    );
    let mut decision = portfolio_canary_run_decision(
        &artifact,
        as_of,
        &risk,
        &broker,
        broker_review_ok,
        place_live_order,
        max_order_age_seconds,
    );
    apply_robinhood_mcp_bridge(
        &mut decision,
        robinhood_mcp_command.as_deref(),
        Some(&order_ledger),
    )?;
    if json {
        println!("{}", serde_json::to_string_pretty(&decision)?);
    } else {
        println!(
            "status={} as_of={} debit_max_loss={:.2} wheel_reserve_cap={:.2} free_cash_buffer={:.2}",
            decision.status,
            decision.as_of,
            decision.risk.debit_max_loss,
            decision.risk.wheel_reserve_cap,
            decision.risk.free_cash_buffer
        );
        println!("reason={}", decision.reason);
        if let Some(action) = &decision.selected_action {
            println!(
                "selected_action={} {} {} entry={} exit={} reserve={:.2} max_loss={:.2}",
                action.status,
                action.symbol,
                action.strategy,
                action.entry_date.as_deref().unwrap_or("unknown"),
                action.exit_date.as_deref().unwrap_or("unknown"),
                action.reserve.unwrap_or(0.0),
                action.max_loss.unwrap_or(0.0),
            );
        }
        println!(
            "orders_placed={}",
            decision
                .mcp_place
                .as_ref()
                .map(|response| response.ok)
                .unwrap_or(false)
        );
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct CanaryRiskConfig {
    account_cash: f64,
    debit_max_loss: f64,
    wheel_reserve_cap: f64,
    free_cash_buffer: f64,
    max_wheel_positions_per_symbol: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct CanaryActionRisk {
    reserve: f64,
    reserve_basis: String,
}

#[derive(Debug, PartialEq, Serialize)]
struct PortfolioCanaryRunDecision {
    status: String,
    reason: String,
    as_of: NaiveDate,
    risk: CanaryRiskConfig,
    broker_multi_leg_options: bool,
    broker_cash_secured_puts: bool,
    broker_covered_calls: bool,
    broker_review_ok: bool,
    live_orders_enabled: bool,
    place_live_order: bool,
    artifact_exported_at: Option<chrono::DateTime<Utc>>,
    max_order_age_seconds: u64,
    mcp_review: Option<RobinhoodMcpToolResponse>,
    mcp_place: Option<RobinhoodMcpToolResponse>,
    selected_action: Option<CanaryActionSummary>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct CanaryActionSummary {
    status: String,
    symbol: String,
    strategy: String,
    entry_date: Option<String>,
    exit_date: Option<String>,
    expiration: Option<String>,
    short_put: Option<f64>,
    short_strike: Option<f64>,
    long_strike: Option<f64>,
    width: Option<f64>,
    entry_credit: Option<f64>,
    max_loss: Option<f64>,
    reserve: Option<f64>,
    reserve_basis: Option<String>,
    pnl: Option<f64>,
}

fn portfolio_canary_run_decision(
    artifact: &serde_json::Value,
    as_of: NaiveDate,
    risk: &CanaryRiskConfig,
    broker: &RobinhoodBrokerAdapter,
    broker_review_ok: bool,
    place_live_order: bool,
    max_order_age_seconds: u64,
) -> PortfolioCanaryRunDecision {
    if let Err(err) = canary_artifact_ready_for_broker(artifact) {
        return canary_run_decision(
            "shadow_artifact_blocked",
            &err.to_string(),
            as_of,
            risk,
            broker,
            broker_review_ok,
            place_live_order,
            artifact_exported_at(artifact),
            max_order_age_seconds,
            None,
        );
    }
    let actions = artifact
        .get("latest_actions")
        .and_then(|value| value.as_array())
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let fresh_actions = actions
        .iter()
        .filter(|action| canary_action_is_fresh(action, as_of))
        .collect::<Vec<_>>();
    if fresh_actions.is_empty() {
        return canary_run_decision(
            "shadow_no_action",
            "no fresh entry_candidate or open_candidate; monitor only",
            as_of,
            risk,
            broker,
            broker_review_ok,
            place_live_order,
            artifact_exported_at(artifact),
            max_order_age_seconds,
            None,
        );
    }

    let selected = fresh_actions.iter().find_map(|action| {
        canary_action_allowed_by_risk(action, risk, &fresh_actions)
            .ok()
            .map(|action_risk| canary_action_summary(action, Some(action_risk)))
    });
    let Some(selected) = selected else {
        return canary_run_decision(
            "shadow_risk_blocked",
            &format!(
                "fresh action exists, but no action passed per-strategy risk controls: {}",
                fresh_actions
                    .iter()
                    .map(
                        |action| canary_action_allowed_by_risk(action, risk, &fresh_actions)
                            .err()
                            .unwrap_or_else(|| "unknown risk rejection".to_owned())
                    )
                    .collect::<Vec<_>>()
                    .join("; ")
            ),
            as_of,
            risk,
            broker,
            broker_review_ok,
            place_live_order,
            artifact_exported_at(artifact),
            max_order_age_seconds,
            fresh_actions
                .first()
                .map(|action| canary_action_summary(action, canary_action_risk(action).ok())),
        );
    };

    if let Err(err) = assert_canary_action_broker_supported(&selected, broker) {
        return canary_run_decision(
            "shadow_broker_unsupported",
            &err.to_string(),
            as_of,
            risk,
            broker,
            broker_review_ok,
            place_live_order,
            artifact_exported_at(artifact),
            max_order_age_seconds,
            Some(selected),
        );
    }
    if selected.status == "open_candidate" {
        return canary_run_decision(
            "shadow_open_candidate_monitor_only",
            "open_candidate is a backtest position that would already be open; live worker will monitor only and will not submit a catch-up order",
            as_of,
            risk,
            broker,
            broker_review_ok,
            place_live_order,
            artifact_exported_at(artifact),
            max_order_age_seconds,
            Some(selected),
        );
    }
    if place_live_order && broker_review_ok {
        return canary_run_decision(
            "live_order_blocked",
            "manual --broker-review-ok cannot authorize automated placement; use a Robinhood MCP review bridge so review and place are bound to the same order intent",
            as_of,
            risk,
            broker,
            broker_review_ok,
            place_live_order,
            artifact_exported_at(artifact),
            max_order_age_seconds,
            Some(selected),
        );
    }
    if place_live_order {
        let today = Utc::now().date_naive();
        if as_of != today {
            return canary_run_decision(
                "live_order_blocked",
                &format!(
                    "live placement requires --as-of to match today's UTC date {today}; got {as_of}"
                ),
                as_of,
                risk,
                broker,
                broker_review_ok,
                place_live_order,
                artifact_exported_at(artifact),
                max_order_age_seconds,
                Some(selected),
            );
        }
        if let Err(err) =
            canary_artifact_fresh_enough_for_live_order(artifact, max_order_age_seconds)
        {
            return canary_run_decision(
                "live_order_blocked",
                &err.to_string(),
                as_of,
                risk,
                broker,
                broker_review_ok,
                place_live_order,
                artifact_exported_at(artifact),
                max_order_age_seconds,
                Some(selected),
            );
        }
    }
    if !broker_review_ok {
        return canary_run_decision(
            "review_required",
            "broker review/preview has not succeeded; no order can be placed",
            as_of,
            risk,
            broker,
            broker_review_ok,
            place_live_order,
            artifact_exported_at(artifact),
            max_order_age_seconds,
            Some(selected),
        );
    }
    canary_run_decision(
        "ready_for_manual_approval",
        "fresh action passed per-strategy risk and broker review gates; live placement was not requested",
        as_of,
        risk,
        broker,
        broker_review_ok,
        place_live_order,
        artifact_exported_at(artifact),
        max_order_age_seconds,
        Some(selected),
    )
}

fn canary_run_decision(
    status: &str,
    reason: &str,
    as_of: NaiveDate,
    risk: &CanaryRiskConfig,
    broker: &RobinhoodBrokerAdapter,
    broker_review_ok: bool,
    place_live_order: bool,
    artifact_exported_at: Option<chrono::DateTime<Utc>>,
    max_order_age_seconds: u64,
    selected_action: Option<CanaryActionSummary>,
) -> PortfolioCanaryRunDecision {
    PortfolioCanaryRunDecision {
        status: status.to_owned(),
        reason: reason.to_owned(),
        as_of,
        risk: risk.clone(),
        broker_multi_leg_options: broker.capabilities.multi_leg_options,
        broker_cash_secured_puts: broker.capabilities.cash_secured_puts,
        broker_covered_calls: broker.capabilities.covered_calls,
        broker_review_ok,
        live_orders_enabled: broker.live_orders_enabled,
        place_live_order,
        artifact_exported_at,
        max_order_age_seconds,
        mcp_review: None,
        mcp_place: None,
        selected_action,
    }
}

fn canary_artifact_ready_for_broker(artifact: &serde_json::Value) -> Result<()> {
    if artifact.get("status").and_then(|value| value.as_str()) != Some("canary_only") {
        anyhow::bail!("canary artifact status is not canary_only");
    }
    let decision = artifact.get("decision").unwrap_or(&serde_json::Value::Null);
    if decision
        .get("canary_ready")
        .and_then(|value| value.as_bool())
        != Some(true)
    {
        anyhow::bail!("canary artifact decision.canary_ready is not true");
    }
    if decision
        .get("research_gate")
        .and_then(|value| value.as_str())
        != Some("research_pass")
    {
        anyhow::bail!("canary artifact decision.research_gate is not research_pass");
    }
    Ok(())
}

fn artifact_exported_at(artifact: &serde_json::Value) -> Option<chrono::DateTime<Utc>> {
    artifact
        .get("exported_at")
        .and_then(|value| value.as_str())
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
}

fn canary_artifact_fresh_enough_for_live_order(
    artifact: &serde_json::Value,
    max_order_age_seconds: u64,
) -> Result<()> {
    let Some(exported_at) = artifact_exported_at(artifact) else {
        anyhow::bail!(
            "live placement requires artifact exported_at timestamp; regenerate and export a fresh canary artifact"
        );
    };
    let age = Utc::now()
        .signed_duration_since(exported_at)
        .num_seconds()
        .max(0) as u64;
    if age > max_order_age_seconds {
        anyhow::bail!(
            "live placement blocked because artifact age {}s exceeds max {}s",
            age,
            max_order_age_seconds
        );
    }
    Ok(())
}

fn canary_action_allowed_by_risk(
    action: &serde_json::Value,
    risk: &CanaryRiskConfig,
    fresh_actions: &[&serde_json::Value],
) -> std::result::Result<CanaryActionRisk, String> {
    let strategy = action
        .get("strategy")
        .and_then(|value| value.as_str())
        .unwrap_or("unknown");
    let action_risk = canary_action_risk(action)?;
    match strategy {
        "put_debit_spread" | "call_debit_spread" => {
            let max_loss = action
                .get("max_loss")
                .and_then(|value| value.as_f64())
                .ok_or_else(|| format!("{strategy} missing max_loss"))?;
            if max_loss <= 0.0 || max_loss > risk.debit_max_loss {
                return Err(format!(
                    "{strategy} max_loss {:.2} exceeds debit cap {:.2}",
                    max_loss, risk.debit_max_loss
                ));
            }
        }
        "wheel" => {
            let status = action
                .get("status")
                .and_then(|value| value.as_str())
                .unwrap_or("unknown");
            let symbol = action
                .get("symbol")
                .and_then(|value| value.as_str())
                .unwrap_or("unknown");
            if status == "entry_candidate" {
                let open_same_symbol = fresh_actions
                    .iter()
                    .filter(|candidate| {
                        candidate.get("status").and_then(|value| value.as_str())
                            == Some("open_candidate")
                            && candidate.get("strategy").and_then(|value| value.as_str())
                                == Some("wheel")
                            && candidate.get("symbol").and_then(|value| value.as_str())
                                == Some(symbol)
                    })
                    .count();
                if open_same_symbol >= risk.max_wheel_positions_per_symbol {
                    return Err(format!(
                        "wheel {} already has {} open wheel positions; max is {}",
                        symbol, open_same_symbol, risk.max_wheel_positions_per_symbol
                    ));
                }
            }
            if action_risk.reserve > risk.wheel_reserve_cap {
                return Err(format!(
                    "wheel reserve {:.2} exceeds wheel reserve cap {:.2}",
                    action_risk.reserve, risk.wheel_reserve_cap
                ));
            }
        }
        other => return Err(format!("strategy {other} is not enabled for live canary")),
    }
    if action_risk.reserve > risk.account_cash - risk.free_cash_buffer {
        return Err(format!(
            "{} reserve {:.2} would breach free-cash buffer {:.2} on account cash {:.2}",
            strategy, action_risk.reserve, risk.free_cash_buffer, risk.account_cash
        ));
    }
    Ok(action_risk)
}

fn canary_action_risk(action: &serde_json::Value) -> std::result::Result<CanaryActionRisk, String> {
    let strategy = action
        .get("strategy")
        .and_then(|value| value.as_str())
        .unwrap_or("unknown");
    if strategy == "wheel" {
        if let Some(short_put) = action.get("short_put").and_then(|value| value.as_f64()) {
            return Ok(CanaryActionRisk {
                reserve: short_put * 100.0,
                reserve_basis: "short_put_x100".to_owned(),
            });
        }
        let max_loss = action
            .get("max_loss")
            .and_then(|value| value.as_f64())
            .ok_or_else(|| "wheel missing max_loss for cash-secured reserve".to_owned())?;
        let entry_credit = action
            .get("entry_credit")
            .and_then(|value| value.as_f64())
            .ok_or_else(|| {
                "wheel missing short_put and entry_credit for cash-secured reserve".to_owned()
            })?;
        return Ok(CanaryActionRisk {
            reserve: max_loss + entry_credit.max(0.0) * 100.0,
            reserve_basis: "max_loss_plus_entry_credit_x100".to_owned(),
        });
    }
    let max_loss = action
        .get("max_loss")
        .and_then(|value| value.as_f64())
        .ok_or_else(|| format!("{strategy} missing max_loss for reserve"))?;
    Ok(CanaryActionRisk {
        reserve: max_loss,
        reserve_basis: "max_loss".to_owned(),
    })
}

fn canary_action_short_put(action: &serde_json::Value) -> Option<f64> {
    action
        .get("short_put")
        .and_then(|value| value.as_f64())
        .or_else(|| {
            let max_loss = action.get("max_loss").and_then(|value| value.as_f64())?;
            let entry_credit = action
                .get("entry_credit")
                .and_then(|value| value.as_f64())?;
            Some(max_loss / 100.0 + entry_credit.max(0.0))
        })
}

fn assert_canary_action_broker_supported(
    action: &CanaryActionSummary,
    broker: &RobinhoodBrokerAdapter,
) -> anyhow::Result<()> {
    match action.strategy.as_str() {
        "put_debit_spread" | "call_debit_spread" => broker.assert_debit_spread_live_supported(),
        "wheel" => broker.assert_wheel_live_supported(),
        other => anyhow::bail!("strategy {other} is not enabled for live canary"),
    }
}

fn canary_action_summary(
    action: &serde_json::Value,
    action_risk: Option<CanaryActionRisk>,
) -> CanaryActionSummary {
    CanaryActionSummary {
        status: canary_action_string(action, "status"),
        symbol: canary_action_string(action, "symbol"),
        strategy: canary_action_string(action, "strategy"),
        entry_date: canary_action_optional_string(action, "entry_date"),
        exit_date: canary_action_optional_string(action, "exit_date"),
        expiration: canary_action_optional_string(action, "expiration"),
        short_put: canary_action_short_put(action),
        short_strike: canary_action_optional_f64(action, "short_strike")
            .or_else(|| canary_action_short_put(action)),
        long_strike: canary_action_optional_f64(action, "long_strike")
            .or_else(|| canary_action_optional_f64(action, "long_put")),
        width: canary_action_optional_f64(action, "width"),
        entry_credit: canary_action_optional_f64(action, "entry_credit"),
        max_loss: action.get("max_loss").and_then(|value| value.as_f64()),
        reserve: action_risk.as_ref().map(|risk| risk.reserve),
        reserve_basis: action_risk.map(|risk| risk.reserve_basis),
        pnl: action.get("pnl").and_then(|value| value.as_f64()),
    }
}

fn canary_action_string(action: &serde_json::Value, key: &str) -> String {
    action
        .get(key)
        .and_then(|value| value.as_str())
        .unwrap_or("unknown")
        .to_owned()
}

fn canary_action_optional_string(action: &serde_json::Value, key: &str) -> Option<String> {
    action
        .get(key)
        .and_then(|value| value.as_str())
        .map(ToOwned::to_owned)
}

fn canary_action_optional_f64(action: &serde_json::Value, key: &str) -> Option<f64> {
    action.get(key).and_then(|value| value.as_f64())
}

fn apply_robinhood_mcp_bridge(
    decision: &mut PortfolioCanaryRunDecision,
    robinhood_mcp_command: Option<&str>,
    order_ledger: Option<&Path>,
) -> Result<()> {
    if decision.status != "review_required" {
        return Ok(());
    }
    let Some(command) = robinhood_mcp_command else {
        return Ok(());
    };
    let Some(action) = decision.selected_action.clone() else {
        return Ok(());
    };

    let executor = RobinhoodMcpCommandExecutor::new(command);
    let review_request = robinhood_mcp_option_order_request("review_option_order", &action)?;
    let order_key = robinhood_mcp_order_key(&review_request);
    let review = executor.execute(&review_request)?;
    let review_ok = review.ok;
    decision.mcp_review = Some(review);
    if !review_ok {
        decision.status = "review_failed".to_owned();
        decision.reason = "Robinhood MCP review_option_order rejected the canary order".to_owned();
        return Ok(());
    }
    if !robinhood_mcp_review_matches_order_key(decision.mcp_review.as_ref(), &order_key) {
        decision.status = "review_failed".to_owned();
        decision.reason =
            "Robinhood MCP review did not echo the expected order_key for the order intent"
                .to_owned();
        return Ok(());
    }

    decision.broker_review_ok = true;
    if !decision.place_live_order {
        decision.status = "ready_for_manual_approval".to_owned();
        decision.reason =
            "Robinhood MCP review_option_order succeeded; live placement was not requested"
                .to_owned();
        return Ok(());
    }
    if action.strategy == "wheel" {
        decision.status = "ready_for_manual_approval".to_owned();
        decision.reason = "Robinhood MCP review succeeded, but autonomous wheel placement is blocked until broker buying-power, assignment, and position reconciliation are implemented".to_owned();
        return Ok(());
    }
    if !decision.live_orders_enabled {
        decision.status = "live_order_blocked".to_owned();
        decision.reason =
            "live order placement is disabled; use shadow-live until explicit rollout gates pass"
                .to_owned();
        return Ok(());
    }

    let place_request = robinhood_mcp_option_order_request("place_option_order", &action)?;
    let place_order_key = robinhood_mcp_order_key(&place_request);
    if place_order_key != order_key {
        decision.status = "live_order_blocked".to_owned();
        decision.reason =
            "review and place order keys diverged; refusing live placement".to_owned();
        return Ok(());
    }
    if let Some(ledger_path) = order_ledger
        && canary_order_ledger_contains(ledger_path, &order_key)?
    {
        decision.status = "live_order_already_submitted".to_owned();
        decision.reason =
            "matching Robinhood MCP order intent is already recorded in the local canary ledger"
                .to_owned();
        return Ok(());
    }
    if let Some(ledger_path) = order_ledger {
        canary_order_ledger_record(ledger_path, &order_key)?;
    }
    let place = executor.execute(&place_request)?;
    let place_ok = place.ok;
    decision.mcp_place = Some(place);
    if place_ok {
        decision.status = "live_order_submitted".to_owned();
        decision.reason = "Robinhood MCP place_option_order returned success".to_owned();
    } else {
        decision.status = "live_order_rejected".to_owned();
        decision.reason = "Robinhood MCP place_option_order returned a rejection".to_owned();
    }
    Ok(())
}

fn robinhood_mcp_option_order_request(
    tool: &str,
    action: &CanaryActionSummary,
) -> Result<RobinhoodMcpToolRequest> {
    let intent = canary_action_order_intent(action)?;
    let arguments = robinhood_mcp_option_order_arguments(action, &intent)
        .with_context(|| format!("build Robinhood MCP {tool} arguments"))?;
    Ok(RobinhoodMcpToolRequest {
        server: "robinhood-trading".to_owned(),
        tool: tool.to_owned(),
        arguments,
    })
}

fn canary_action_order_intent(action: &CanaryActionSummary) -> Result<OptionOrderIntent> {
    if action.status != "entry_candidate" {
        anyhow::bail!("only same-day entry_candidate actions are orderable");
    }
    let symbol = require_action_field(action.symbol.as_str(), "symbol")?;
    let expiration = require_option_string(action.expiration.as_deref(), "expiration")?
        .parse::<NaiveDate>()
        .with_context(|| format!("parse expiration for {}", action.symbol))?;
    let quantity = 1_u32;
    let entry_credit = require_option_nonzero_f64(action.entry_credit, "entry_credit")?;
    let limit_price = decimal_from_f64(entry_credit.abs(), "entry_credit")?;
    match action.strategy.as_str() {
        "wheel" => {
            let short_strike = require_option_f64(action.short_strike, "short_strike")?;
            Ok(cash_secured_put_open_intent(
                OptionKey::new(
                    symbol,
                    expiration,
                    decimal_from_f64(short_strike, "short_strike")?,
                    OptionRight::Put,
                ),
                quantity,
                limit_price,
                action.strategy.clone(),
            )?)
        }
        "put_debit_spread" => {
            let short_strike = require_option_f64(action.short_strike, "short_strike")?;
            let long_strike = require_option_f64(action.long_strike, "long_strike")?;
            Ok(debit_spread_open_intent(
                OptionKey::new(
                    symbol,
                    expiration,
                    decimal_from_f64(long_strike, "long_strike")?,
                    OptionRight::Put,
                ),
                OptionKey::new(
                    symbol,
                    expiration,
                    decimal_from_f64(short_strike, "short_strike")?,
                    OptionRight::Put,
                ),
                quantity,
                limit_price,
                action.strategy.clone(),
            )?)
        }
        "call_debit_spread" => {
            let short_strike = require_option_f64(action.short_strike, "short_strike")?;
            let long_strike = require_option_f64(action.long_strike, "long_strike")?;
            Ok(debit_spread_open_intent(
                OptionKey::new(
                    symbol,
                    expiration,
                    decimal_from_f64(long_strike, "long_strike")?,
                    OptionRight::Call,
                ),
                OptionKey::new(
                    symbol,
                    expiration,
                    decimal_from_f64(short_strike, "short_strike")?,
                    OptionRight::Call,
                ),
                quantity,
                limit_price,
                action.strategy.clone(),
            )?)
        }
        other => anyhow::bail!("strategy {other} is not orderable through Robinhood MCP"),
    }
}

fn robinhood_mcp_option_order_arguments(
    action: &CanaryActionSummary,
    intent: &OptionOrderIntent,
) -> Result<serde_json::Value> {
    Ok(serde_json::json!({
        "symbol": intent.symbol,
        "strategy": intent.strategy,
        "quantity": intent.quantity(),
        "order_effect": option_order_effect_value(&intent.order_effect),
        "order_type": "limit",
        "limit_price": decimal_to_f64(intent.limit_price, "limit_price")?,
        "time_in_force": time_in_force_value(&intent.time_in_force),
        "legs": intent
            .legs
            .iter()
            .map(robinhood_mcp_leg_arguments)
            .collect::<Result<Vec<_>>>()?,
        "source": {
            "system": "SpreadFoundry",
            "status": action.status,
            "entry_date": action.entry_date,
            "max_loss": action.max_loss,
            "reserve": action.reserve,
            "reserve_basis": action.reserve_basis,
        }
    }))
}

fn robinhood_mcp_leg_arguments(leg: &OptionOrderLeg) -> Result<serde_json::Value> {
    Ok(serde_json::json!({
        "side": option_order_side_value(&leg.side),
        "position_effect": position_effect_value(&leg.position_effect),
        "option_type": option_right_value(&leg.key.right),
        "symbol": leg.key.underlying,
        "expiration": leg.key.expiration,
        "strike": decimal_to_f64(leg.key.strike, "strike")?,
        "quantity": leg.quantity,
    }))
}

fn decimal_from_f64(value: f64, field: &str) -> Result<Decimal> {
    if !value.is_finite() {
        anyhow::bail!("{field} must be finite");
    }
    value
        .to_string()
        .parse::<Decimal>()
        .with_context(|| format!("convert {field} to Decimal"))
}

fn decimal_to_f64(value: Decimal, field: &str) -> Result<f64> {
    value
        .to_string()
        .parse::<f64>()
        .with_context(|| format!("convert {field} to f64"))
}

fn option_order_effect_value(effect: &OptionOrderEffect) -> &'static str {
    match effect {
        OptionOrderEffect::Credit => "credit",
        OptionOrderEffect::Debit => "debit",
    }
}

fn option_order_side_value(side: &OptionOrderSide) -> &'static str {
    match side {
        OptionOrderSide::Buy => "buy",
        OptionOrderSide::Sell => "sell",
    }
}

fn position_effect_value(effect: &PositionEffect) -> &'static str {
    match effect {
        PositionEffect::Open => "open",
        PositionEffect::Close => "close",
    }
}

fn time_in_force_value(time_in_force: &TimeInForce) -> &'static str {
    match time_in_force {
        TimeInForce::Day => "day",
    }
}

fn option_right_value(right: &OptionRight) -> &'static str {
    match right {
        OptionRight::Call => "call",
        OptionRight::Put => "put",
    }
}

fn robinhood_mcp_order_key(request: &RobinhoodMcpToolRequest) -> String {
    serde_json::to_string(&serde_json::json!({
        "server": request.server,
        "arguments": request.arguments,
    }))
    .expect("Robinhood MCP order key serialization should be infallible")
}

fn robinhood_mcp_review_matches_order_key(
    review: Option<&RobinhoodMcpToolResponse>,
    order_key: &str,
) -> bool {
    review
        .and_then(|review| review.raw.get("order_key"))
        .and_then(|value| value.as_str())
        == Some(order_key)
}

fn canary_order_ledger_contains(path: &Path, order_key: &str) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let body = fs::read_to_string(path)
        .with_context(|| format!("read canary order ledger {}", path.display()))?;
    let ledger: BTreeSet<String> = serde_json::from_str(&body)
        .with_context(|| format!("parse canary order ledger {}", path.display()))?;
    Ok(ledger.contains(order_key))
}

fn canary_order_ledger_record(path: &Path, order_key: &str) -> Result<()> {
    let mut ledger = if path.exists() {
        let body = fs::read_to_string(path)
            .with_context(|| format!("read canary order ledger {}", path.display()))?;
        serde_json::from_str::<BTreeSet<String>>(&body)
            .with_context(|| format!("parse canary order ledger {}", path.display()))?
    } else {
        BTreeSet::new()
    };
    ledger.insert(order_key.to_owned());
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("create canary order ledger directory {}", parent.display())
        })?;
    }
    let tmp_path = path.with_extension("json.tmp");
    fs::write(&tmp_path, serde_json::to_string_pretty(&ledger)?)
        .with_context(|| format!("write canary order ledger temp {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path)
        .with_context(|| format!("replace canary order ledger {}", path.display()))
}

fn require_action_field<'a>(value: &'a str, field: &str) -> Result<&'a str> {
    if value == "unknown" || value.trim().is_empty() {
        anyhow::bail!("selected action missing {field}");
    }
    Ok(value)
}

fn require_option_string<'a>(value: Option<&'a str>, field: &str) -> Result<&'a str> {
    value
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("selected action missing {field}"))
}

fn require_option_f64(value: Option<f64>, field: &str) -> Result<f64> {
    value
        .filter(|value| value.is_finite() && *value > 0.0)
        .ok_or_else(|| anyhow::anyhow!("selected action missing positive {field}"))
}

fn require_option_nonzero_f64(value: Option<f64>, field: &str) -> Result<f64> {
    value
        .filter(|value| value.is_finite() && *value != 0.0)
        .ok_or_else(|| anyhow::anyhow!("selected action missing nonzero {field}"))
}

#[derive(Debug)]
struct CanaryWorkerArgs {
    candidate: PathBuf,
    as_of: Option<NaiveDate>,
    risk: CanaryRiskConfig,
    broker: RobinhoodBrokerAdapter,
    robinhood_mcp_command: Option<String>,
    order_ledger: PathBuf,
    max_order_age_seconds: u64,
    broker_review_ok: bool,
    place_live_order: bool,
    poll_seconds: u64,
    once: bool,
    health_output: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Serialize)]
struct CanaryWorkerHealth {
    checked_at: chrono::DateTime<Utc>,
    service: String,
    status: String,
    candidate: String,
    candidate_readable: bool,
    artifact_parse_ok: bool,
    as_of: NaiveDate,
    risk: CanaryRiskConfig,
    broker_multi_leg_options: bool,
    broker_cash_secured_puts: bool,
    broker_covered_calls: bool,
    live_orders_enabled: bool,
    broker_review_ok: bool,
    place_live_order: bool,
    robinhood_mcp_command_configured: bool,
    order_ledger: String,
    decision: Option<PortfolioCanaryRunDecision>,
    error: Option<String>,
}

async fn run_canary_worker(args: CanaryWorkerArgs) -> Result<()> {
    validate_canary_risk_config(&args.risk)?;
    if args.poll_seconds == 0 && !args.once {
        anyhow::bail!("--poll-seconds must be positive unless --once is used");
    }
    loop {
        let health = canary_worker_health(&args);
        if let Some(path) = &args.health_output {
            write_canary_worker_health(path, &health)?;
        }
        if args.json {
            println!("{}", serde_json::to_string_pretty(&health)?);
        } else {
            println!(
                "service={} status={} as_of={} decision={} error={}",
                health.service,
                health.status,
                health.as_of,
                health
                    .decision
                    .as_ref()
                    .map(|decision| decision.status.as_str())
                    .unwrap_or("none"),
                health.error.as_deref().unwrap_or("-")
            );
        }
        if args.once {
            break;
        }
        tokio::time::sleep(StdDuration::from_secs(args.poll_seconds)).await;
    }
    Ok(())
}

fn canary_worker_health(args: &CanaryWorkerArgs) -> CanaryWorkerHealth {
    let as_of = args.as_of.unwrap_or_else(|| Utc::now().date_naive());
    let candidate = args.candidate.display().to_string();
    let candidate_body = fs::read_to_string(&args.candidate);
    let candidate_readable = candidate_body.is_ok();
    let mut error = None;
    let mut decision = None;
    let artifact_parse_ok = match candidate_body {
        Ok(body) => match serde_json::from_str::<serde_json::Value>(&body) {
            Ok(artifact) => {
                let mut canary_decision = portfolio_canary_run_decision(
                    &artifact,
                    as_of,
                    &args.risk,
                    &args.broker,
                    args.broker_review_ok,
                    args.place_live_order,
                    args.max_order_age_seconds,
                );
                if let Err(err) = apply_robinhood_mcp_bridge(
                    &mut canary_decision,
                    args.robinhood_mcp_command.as_deref(),
                    Some(&args.order_ledger),
                ) {
                    error = Some(format!("Robinhood MCP bridge: {err}"));
                }
                decision = Some(canary_decision);
                true
            }
            Err(err) => {
                error = Some(format!("parse candidate artifact: {err}"));
                false
            }
        },
        Err(err) => {
            error = Some(format!("read candidate artifact: {err}"));
            false
        }
    };
    let status = if error.is_some() {
        "unhealthy"
    } else {
        match decision.as_ref().map(|decision| decision.status.as_str()) {
            Some("ready_for_manual_approval" | "review_required") => "ready",
            Some("live_order_submitted" | "live_order_already_submitted") => "live",
            Some("review_failed" | "live_order_rejected") => "unhealthy",
            Some(_) => "shadow",
            None => "unhealthy",
        }
    }
    .to_owned();
    CanaryWorkerHealth {
        checked_at: Utc::now(),
        service: "portfolio_canary_worker".to_owned(),
        status,
        candidate,
        candidate_readable,
        artifact_parse_ok,
        as_of,
        risk: args.risk.clone(),
        broker_multi_leg_options: args.broker.capabilities.multi_leg_options,
        broker_cash_secured_puts: args.broker.capabilities.cash_secured_puts,
        broker_covered_calls: args.broker.capabilities.covered_calls,
        live_orders_enabled: args.broker.live_orders_enabled,
        broker_review_ok: args.broker_review_ok,
        place_live_order: args.place_live_order,
        robinhood_mcp_command_configured: args.robinhood_mcp_command.is_some(),
        order_ledger: args.order_ledger.display().to_string(),
        decision,
        error,
    }
}

fn write_canary_worker_health(path: &Path, health: &CanaryWorkerHealth) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create health output directory {}", parent.display()))?;
    }
    fs::write(path, serde_json::to_string_pretty(health)?)
        .with_context(|| format!("write canary worker health {}", path.display()))
}

fn canary_broker(
    broker_multi_leg_options: bool,
    broker_cash_secured_puts: bool,
    broker_covered_calls: bool,
    live_orders_enabled: bool,
) -> RobinhoodBrokerAdapter {
    RobinhoodBrokerAdapter {
        capabilities: BrokerCapabilities {
            single_leg_options: true,
            multi_leg_options: broker_multi_leg_options,
            stock_option_combos: false,
            cash_secured_puts: broker_cash_secured_puts,
            covered_calls: broker_covered_calls,
        },
        live_orders_enabled,
    }
}

fn validate_canary_risk_config(risk: &CanaryRiskConfig) -> Result<()> {
    if risk.account_cash <= 0.0 {
        anyhow::bail!("--account-cash must be positive");
    }
    if risk.debit_max_loss <= 0.0 {
        anyhow::bail!("--debit-max-loss must be positive");
    }
    if risk.wheel_reserve_cap <= 0.0 {
        anyhow::bail!("--wheel-reserve-cap must be positive");
    }
    if risk.free_cash_buffer < 0.0 || risk.free_cash_buffer >= risk.account_cash {
        anyhow::bail!("--free-cash-buffer must be >= 0 and less than --account-cash");
    }
    if risk.max_wheel_positions_per_symbol == 0 {
        anyhow::bail!("--max-wheel-positions-per-symbol must be positive");
    }
    Ok(())
}

fn print_option_cache_coverage_report(report: &OptionCacheCoverageReport) {
    println!(
        "symbol\taudited\tput_complete\tcall_complete\tboth_complete\tput_cov\tcall_cov\tboth_cov\tfirst_call\tlast_call\terror"
    );
    for symbol in &report.symbols {
        println!(
            "{}\t{}\t{}\t{}\t{}\t{:.1}%\t{:.1}%\t{:.1}%\t{}\t{}\t{}",
            symbol.symbol,
            symbol.expirations_audited,
            symbol.put_complete,
            symbol.call_complete,
            symbol.both_complete,
            symbol.put_coverage_pct * 100.0,
            symbol.call_coverage_pct * 100.0,
            symbol.both_coverage_pct * 100.0,
            symbol
                .first_complete_call_expiration
                .map(|date| date.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            symbol
                .last_complete_call_expiration
                .map(|date| date.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            symbol.error.as_deref().unwrap_or("-"),
        );
        if !symbol.missing_call_examples.is_empty() {
            let examples = symbol
                .missing_call_examples
                .iter()
                .map(|gap| format!("{}:{}..{}", gap.expiration, gap.start, gap.end))
                .collect::<Vec<_>>()
                .join(", ");
            println!("{} missing_call_examples: {}", symbol.symbol, examples);
        }
    }
}

fn print_warm_option_cache_coverage_report(report: &WarmOptionCacheCoverageReport) {
    println!("symbol\taudited\tattempted\tcompleted\tfailed\terror");
    for symbol in &report.symbols {
        println!(
            "{}\t{}\t{}\t{}\t{}\t{}",
            symbol.symbol,
            symbol.expirations_audited,
            symbol.windows_attempted,
            symbol.windows_completed,
            symbol.windows_failed,
            symbol.error.as_deref().unwrap_or("-"),
        );
        for window in &symbol.windows {
            println!(
                "{}\t{}\t{}..{}\tbefore_put={}\tbefore_call={}\tafter_put={}\tafter_call={}\t{}",
                symbol.symbol,
                window.expiration,
                window.start,
                window.end,
                window.put_complete_before,
                window.call_complete_before,
                window.put_complete_after,
                window.call_complete_after,
                window.error.as_deref().unwrap_or("-"),
            );
        }
    }
}

fn print_weekly_signal_gate_audit_report(report: &WeeklySignalGateAuditReport) {
    println!("symbol\tfamily\tfrom\tto\tcache_only\tdiscovered\taudited\tloaded\tfailed\trows");
    println!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
        report.symbol,
        report.profile_family.as_str(),
        report.from,
        report.to,
        report.cache_only,
        report.expirations_discovered,
        report.expirations_audited,
        report.expirations_loaded,
        report.expirations_failed,
        report.rows_loaded,
    );
    println!(
        "profile\tstructure\tdte_rows\tdte_days\tprimary_pass\tregime_pass\tcandidates\tcandidate_days\ttrades\ttrade_days\tpnl"
    );
    for profile in report.profiles.iter().take(20) {
        println!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:.2}",
            profile.profile,
            profile.structure.as_str(),
            profile.dte_rows,
            profile.dte_entry_days,
            profile.primary_leg_passes,
            profile.regime_passes,
            profile.candidates,
            profile.candidate_entry_days,
            profile.simulated_trades,
            profile.trade_entry_days,
            profile.total_pnl,
        );
    }
}

fn canary_action_is_fresh(action: &serde_json::Value, as_of: NaiveDate) -> bool {
    let Some(status) = action.get("status").and_then(|value| value.as_str()) else {
        return false;
    };
    let entry_date = action
        .get("entry_date")
        .and_then(|value| value.as_str())
        .and_then(parse_canary_action_date);
    let exit_date = action
        .get("exit_date")
        .and_then(|value| value.as_str())
        .and_then(parse_canary_action_date);

    match status {
        "entry_candidate" => entry_date == Some(as_of),
        "open_candidate" => {
            entry_date.is_some_and(|entry| entry <= as_of)
                && exit_date.is_some_and(|exit| exit >= as_of)
        }
        _ => false,
    }
}

fn parse_canary_action_date(value: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d").ok()
}

#[derive(Debug)]
struct ResearchCommandArgs {
    symbol: String,
    profile_family: ResearchProfileFamily,
    from: NaiveDate,
    to: NaiveDate,
    max_expirations: Option<usize>,
    fetch_concurrency: usize,
    force_refresh: bool,
    cache_only: bool,
    expand_on_plateau: bool,
}

async fn research_symbol_and_optional_universe(args: ResearchCommandArgs) -> Result<()> {
    let ResearchCommandArgs {
        symbol,
        profile_family,
        from,
        to,
        max_expirations,
        fetch_concurrency,
        force_refresh,
        cache_only,
        expand_on_plateau,
    } = args;
    let report = run_symbol_research(ResearchRequest {
        symbol,
        profile_family,
        from,
        to,
        max_expirations,
        fetch_concurrency,
        force_refresh,
        cache_only,
    })
    .await?;
    if let Some(best) = report.profiles.first() {
        println!(
            "best={} trades={} pnl={:.2} score={:.4}",
            best.profile.name, best.metrics.trades, best.metrics.total_pnl, best.metrics.score
        );
    }

    if !expand_on_plateau {
        return Ok(());
    }

    let expansion_ready = report.plateau_status.expansion_ready;
    let aggressive = report.deployment_gate.best_profile_gate;
    if !expansion_ready && !aggressive {
        println!(
            "plateau expansion locked: status={} reason={}",
            report.plateau_status.status, report.plateau_status.reason
        );
        return Ok(());
    }

    let plateau_run = PathBuf::from("runs")
        .join(&report.run_id)
        .join("research.json");
    if aggressive && !expansion_ready {
        println!(
            "robust detector reached; starting pre-plateau universe expansion on {}",
            DEFAULT_PLATEAU_UNIVERSE_SYMBOLS_CSV
        );
    } else {
        println!(
            "plateau reached; researching universe {}",
            DEFAULT_PLATEAU_UNIVERSE_SYMBOLS_CSV
        );
    }
    research_universe(UniverseResearchArgs {
        symbols: DEFAULT_PLATEAU_UNIVERSE_SYMBOLS
            .iter()
            .map(|symbol| (*symbol).to_owned())
            .collect(),
        plateau_run: Some(plateau_run),
        profile_family,
        from: report.requested_from,
        to: report.to,
        max_expirations,
        fetch_concurrency,
        force_refresh,
        cache_only,
        allow_pre_plateau: aggressive && !expansion_ready,
        symbol_concurrency: fetch_concurrency.max(1),
    })
    .await
}

#[allow(dead_code)]
fn automatic_expansion_plateau_run(run_id: &str, expansion_ready: bool) -> Option<PathBuf> {
    expansion_ready.then(|| PathBuf::from("runs").join(run_id).join("research.json"))
}

fn should_expand_on_plateau(expand_on_plateau: bool, single_symbol_only: bool) -> bool {
    expand_on_plateau || !single_symbol_only
}

fn universe_strategy(profile_family: ResearchProfileFamily) -> &'static str {
    match profile_family {
        ResearchProfileFamily::Swing => "put_credit_spread",
        ResearchProfileFamily::Weekly => "weekly_put_credit_spread",
        ResearchProfileFamily::WeeklyFarOtm => "weekly_far_otm_put_credit_spread",
        ResearchProfileFamily::WeeklyPutDebit => "weekly_put_debit_spread",
        ResearchProfileFamily::WeeklyCallCredit => "weekly_call_credit_spread",
        ResearchProfileFamily::WeeklyCallDebit => "weekly_call_debit_spread",
        ResearchProfileFamily::WeeklyWheel => "weekly_wheel",
    }
}

fn universe_selection_basis(profile_family: ResearchProfileFamily) -> &'static str {
    match profile_family {
        ResearchProfileFamily::Swing => UNIVERSE_SELECTION_BASIS,
        ResearchProfileFamily::Weekly
        | ResearchProfileFamily::WeeklyFarOtm
        | ResearchProfileFamily::WeeklyPutDebit
        | ResearchProfileFamily::WeeklyCallCredit
        | ResearchProfileFamily::WeeklyCallDebit
        | ResearchProfileFamily::WeeklyWheel => WEEKLY_UNIVERSE_SELECTION_BASIS,
    }
}

fn universe_research_method(profile_family: ResearchProfileFamily) -> &'static str {
    match profile_family {
        ResearchProfileFamily::Swing => UNIVERSE_RESEARCH_METHOD,
        ResearchProfileFamily::Weekly => WEEKLY_RESEARCH_METHOD,
        ResearchProfileFamily::WeeklyFarOtm => {
            "Each symbol independently runs a far-OTM weekly put-credit-spread grid centered on 1-14 DTE, short puts around 2-15 delta, $1-$25 width caps, 25-33% profit taking, tighter stop losses, capped overlap, and conservative bid/ask fills. Ranking requires weekly-style trade cadence and robust PnL/drawdown evidence."
        }
        ResearchProfileFamily::WeeklyPutDebit => {
            "Each symbol independently runs a weekly put-debit-spread grid centered on 1-14 DTE, bought puts around 20-60 delta, $1-$25 width caps, debit caps, 25-50% profit taking, capped overlap, and conservative bid/ask fills. Ranking requires weekly-style trade cadence and robust PnL/drawdown evidence."
        }
        ResearchProfileFamily::WeeklyCallCredit => {
            "Each symbol independently runs a weekly call-credit-spread grid centered on 1-14 DTE, short calls around 10-30 delta, $1-$25 width caps, weak/overbought trend gates, one-third profit taking, capped overlap, and conservative bid/ask fills. Ranking requires weekly-style trade cadence and robust PnL/drawdown evidence."
        }
        ResearchProfileFamily::WeeklyCallDebit => {
            "Each symbol independently runs a weekly call-debit-spread grid centered on 1-14 DTE, bought calls around 20-60 delta, $1-$25 width caps, debit caps, bullish trend/volatility gates, 25-50% profit taking, capped overlap, and conservative bid/ask fills. Ranking requires weekly-style trade cadence and robust PnL/drawdown evidence."
        }
        ResearchProfileFamily::WeeklyWheel => {
            "Each symbol independently runs a weekly wheel grid centered on 1-14 DTE, cash-secured short puts around 5-30 delta, assignment-aware stock inventory, covered-call exits, and conservative bid/ask fills. Ranking requires weekly-style trade cadence and robust PnL/drawdown evidence after stock drawdown is included."
        }
    }
}

async fn ingest_theta(
    symbol: String,
    from_date: NaiveDate,
    to_date: NaiveDate,
    _interval: String,
    output_dir: PathBuf,
) -> Result<()> {
    if from_date != to_date {
        anyhow::bail!(
            "Theta contract-list ingest is single-date in v1; pass matching --from and --to dates"
        );
    }
    let client = ThetaClient::default();
    let request = ThetaUniverseRequest {
        symbol: symbol.clone(),
        date: from_date,
    };
    let output_path = output_dir.join(&symbol).join(format!(
        "contracts_{}_{}.json",
        from_date.format("%Y%m%d"),
        to_date.format("%Y%m%d")
    ));
    client
        .fetch_universe_contracts(&request, &output_path)
        .await
        .with_context(|| {
            format!(
                "ThetaData ingest failed. Confirm Theta Terminal is running locally before retrying. Target: {}",
                output_path.display()
            )
        })?;
    println!("wrote {}", output_path.display());
    Ok(())
}

fn simulate_put_spread(config_path: &Path) -> Result<()> {
    let config: SimulationConfig = read_toml(config_path)?;
    ensure_fixture_mode(&config.data_mode)?;
    let filters = config.filters.unwrap_or_default();
    let exit_rules = config.exit.unwrap_or_default();
    let snapshots = fixture::nvda_put_snapshots();
    let (candidates, generation) =
        generate_put_spread_candidates(&snapshots, fixture::nvda_decision_ts(), &filters)?;
    if candidates.is_empty() {
        anyhow::bail!("no candidate spreads passed filters");
    }
    let exit_quotes = fixture_exit_quotes(config.fixture_exit.as_deref())?;
    let trades = candidates
        .iter()
        .filter_map(|candidate| {
            choose_exit(
                candidate,
                &exit_quotes,
                &exit_rules,
                config.quantity,
                config.fees,
            )
        })
        .collect::<Vec<_>>();
    if trades.is_empty() {
        anyhow::bail!("no exits selected for fixture quote path");
    }
    let score = score_trades(&trades, 0.0);
    let run_dir = next_run_dir("sim-put-spread")?;
    let report = write_run_report(&run_dir, "put-spread", &trades, score)?;
    println!(
        "generated={} valid_puts={} wrote={}",
        generation.generated_candidates,
        generation.valid_puts,
        run_dir.display()
    );
    println!(
        "trades={} total_pnl={} score={:.6}",
        report.trades, report.total_pnl, report.score.score
    );
    Ok(())
}

fn optimize_put_spread(config_path: &Path, method: OptimizeMethod) -> Result<()> {
    let config: OptimizationConfig = read_toml(config_path)?;
    ensure_fixture_mode(&config.data_mode)?;
    if matches!(method, OptimizeMethod::Random) {
        anyhow::bail!("seeded random search is planned; grid search is implemented first");
    }

    let base_filters = config.filters.unwrap_or_default();
    let exit_rules = config.exit.unwrap_or_default();
    let snapshots = fixture::nvda_put_snapshots();
    let exit_quotes = fixture::nvda_exit_quotes_take_profit();
    let mut results = Vec::new();

    for min_credit_width_ratio in &config.credit_width_ratios {
        for max_width in &config.max_widths {
            let mut filters = base_filters.clone();
            filters.min_credit_width_ratio = *min_credit_width_ratio;
            filters.max_width = *max_width;
            let (candidates, _) =
                generate_put_spread_candidates(&snapshots, fixture::nvda_decision_ts(), &filters)?;
            let trades = candidates
                .iter()
                .filter_map(|candidate| {
                    choose_exit(
                        candidate,
                        &exit_quotes,
                        &exit_rules,
                        config.quantity,
                        config.fees,
                    )
                })
                .collect::<Vec<_>>();
            results.push(OptimizationResult {
                params: GridParams {
                    min_credit_width_ratio: *min_credit_width_ratio,
                    max_width: *max_width,
                },
                score: score_trades(&trades, 0.0),
            });
        }
    }

    let ranked = rank_results(results);
    let best = ranked.first().context("optimizer produced no results")?;
    let run_dir = next_run_dir("opt-put-spread")?;
    fs::create_dir_all(&run_dir)?;
    fs::write(
        run_dir.join("optimization.json"),
        serde_json::to_string_pretty(&ranked)?,
    )?;
    println!("wrote {}", run_dir.display());
    println!(
        "best min_credit_width_ratio={} max_width={} trades={} score={:.6}",
        best.params.min_credit_width_ratio,
        best.params.max_width,
        best.score.trades,
        best.score.score
    );
    Ok(())
}

fn train_ranker(config: &Path) -> Result<()> {
    fs::read_to_string(config).with_context(|| format!("reading {}", config.display()))?;
    println!(
        "ranker training is intentionally gated. Config accepted: {}. Build deterministic labels and out-of-sample baseline first.",
        config.display()
    );
    Ok(())
}

fn shadow_live(symbol: &str, strategy: StrategyArg) -> Result<()> {
    let broker = RobinhoodBrokerAdapter::default();
    match strategy {
        StrategyArg::PutSpread => {
            if let Err(err) = broker.assert_credit_spread_live_supported() {
                println!("{symbol} put-spread shadow-live is data-only for now: {err}");
                println!("No orders placed.");
                return Ok(());
            }
        }
    }
    println!("{symbol} shadow-live adapter is not connected yet. No orders placed.");
    Ok(())
}

async fn research_universe(args: UniverseResearchArgs) -> Result<()> {
    let UniverseResearchArgs {
        symbols,
        plateau_run,
        profile_family,
        from,
        to,
        max_expirations,
        fetch_concurrency,
        force_refresh,
        cache_only,
        allow_pre_plateau,
        symbol_concurrency,
    } = args;
    let symbols = normalize_symbols(symbols);
    if symbols.is_empty() {
        anyhow::bail!("research-universe requires at least one symbol");
    }

    let plateau_run = checked_plateau_run(plateau_run, allow_pre_plateau)?;

    let run_dir = next_run_dir("universe-research")?;
    fs::create_dir_all(&run_dir)?;
    let run_id = run_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("universe-research")
        .to_owned();
    let expansion_seed = expansion_seed_for_symbols(&symbols, profile_family);
    let plateau_run = plateau_run.as_ref().map(|path| path.display().to_string());
    let mut summary = UniverseResearchSummary {
        run_id,
        run_status: "running".to_owned(),
        from,
        to,
        symbols_requested: symbols.len(),
        symbols_completed: 0,
        symbols: symbols.clone(),
        plateau_run,
        max_expirations,
        fetch_concurrency,
        force_refresh,
        cache_only,
        profile_family: profile_family.as_str().to_owned(),
        strategy: universe_strategy(profile_family).to_owned(),
        selection_basis: universe_selection_basis(profile_family).to_owned(),
        research_method: universe_research_method(profile_family).to_owned(),
        seed_score_basis: UNIVERSE_SEED_SCORE_BASIS.to_owned(),
        detector_score_basis: UNIVERSE_DETECTOR_SCORE_BASIS.to_owned(),
        execution_score_basis: UNIVERSE_EXECUTION_SCORE_BASIS.to_owned(),
        expansion_seed,
        results: Vec::new(),
    };
    write_universe_summary(&run_dir, &summary)?;

    let symbol_concurrency = symbol_concurrency.max(1);
    let completed = stream::iter(symbols.into_iter().map(|symbol| {
        let expansion_seed = summary.expansion_seed.clone();
        async move {
            println!("researching {symbol}");
            let request = ResearchRequest {
                symbol: symbol.clone(),
                profile_family,
                from,
                to,
                max_expirations,
                fetch_concurrency,
                force_refresh,
                cache_only,
            };
            let result = match run_symbol_research(request).await {
                Ok(report) => universe_symbol_summary(&report, &expansion_seed),
                Err(err) => {
                    eprintln!("research failed for {symbol}: {err:#}");
                    universe_symbol_error_summary(&symbol, &expansion_seed, &err)
                }
            };
            (symbol, result)
        }
    }))
    .buffer_unordered(symbol_concurrency)
    .collect::<Vec<_>>()
    .await;

    for (symbol, result) in completed {
        if result.symbol != symbol {
            anyhow::bail!(
                "universe research result symbol mismatch: {symbol} vs {}",
                result.symbol
            );
        }
        summary.results.push(result);
        rank_universe_results(&mut summary.results);
        summary.symbols_completed = summary.results.len();
        write_universe_summary(&run_dir, &summary)?;
    }

    summary.run_status = "complete".to_owned();
    write_universe_summary(&run_dir, &summary)?;
    println!("wrote {}", run_dir.display());
    Ok(())
}

fn write_universe_summary(run_dir: &Path, summary: &UniverseResearchSummary) -> Result<()> {
    fs::write(
        run_dir.join("summary.json"),
        serde_json::to_string_pretty(summary)?,
    )?;
    fs::write(run_dir.join("report.md"), universe_markdown(summary))?;
    Ok(())
}

fn normalize_symbols(symbols: Vec<String>) -> Vec<String> {
    let mut normalized = Vec::new();
    for symbol in symbols {
        let symbol = symbol.trim().to_uppercase();
        if !symbol.is_empty() && !normalized.contains(&symbol) {
            normalized.push(symbol);
        }
    }
    normalized
}

fn expansion_seed_for_symbols(
    symbols: &[String],
    profile_family: ResearchProfileFamily,
) -> Vec<UniverseSeedSymbol> {
    let default_seed = default_universe_seed(profile_family);
    symbols
        .iter()
        .enumerate()
        .map(|(idx, symbol)| {
            if let Some(default_symbol) = default_seed
                .iter()
                .find(|seed| seed.symbol == *symbol)
            {
                let mut seed = default_symbol.clone();
                seed.rank = idx + 1;
                seed
            } else {
                UniverseSeedSymbol {
                    rank: idx + 1,
                    symbol: symbol.clone(),
                    role: "manual_override".to_owned(),
                    rationale: "Manual universe override; must still pass ThetaData liquidity, detector, execution, and out-of-sample gates before promotion.".to_owned(),
                    suitability_score: None,
                    liquidity_score: None,
                    premium_score: None,
                    spread_quality_score: None,
                    price_fit_score: None,
                    diversification_score: None,
                    event_risk_score: None,
                }
            }
        })
        .collect()
}

fn default_universe_seed(profile_family: ResearchProfileFamily) -> Vec<UniverseSeedSymbol> {
    let candidates = match profile_family {
        ResearchProfileFamily::Swing => universe_seed_candidates(),
        ResearchProfileFamily::Weekly
        | ResearchProfileFamily::WeeklyFarOtm
        | ResearchProfileFamily::WeeklyPutDebit
        | ResearchProfileFamily::WeeklyCallCredit
        | ResearchProfileFamily::WeeklyCallDebit
        | ResearchProfileFamily::WeeklyWheel => weekly_universe_seed_candidates(),
    };
    let target_len = match profile_family {
        ResearchProfileFamily::Swing => DEFAULT_PLATEAU_UNIVERSE_SYMBOLS.len(),
        ResearchProfileFamily::Weekly
        | ResearchProfileFamily::WeeklyFarOtm
        | ResearchProfileFamily::WeeklyPutDebit
        | ResearchProfileFamily::WeeklyCallCredit
        | ResearchProfileFamily::WeeklyCallDebit
        | ResearchProfileFamily::WeeklyWheel => DEFAULT_WEEKLY_RESEARCH_SYMBOLS.len(),
    };
    let mut seed = candidates
        .iter()
        .map(universe_seed_from_candidate)
        .collect::<Vec<_>>();
    seed.sort_by(universe_seed_order);
    seed.truncate(target_len);
    for (idx, symbol) in seed.iter_mut().enumerate() {
        symbol.rank = idx + 1;
    }
    seed
}

fn weekly_universe_seed_candidates() -> Vec<UniverseSeedCandidate> {
    vec![
        UniverseSeedCandidate {
            symbol: "IREN",
            role: "high_premium_weekly_candidate",
            rationale: "High-volatility growth name selected to test whether 1-14 DTE defined-risk option spreads can produce frequent entries without unacceptable drawdown.",
            liquidity_score: 3,
            premium_score: 5,
            spread_quality_score: 2,
            price_fit_score: 5,
            diversification_score: 4,
            event_risk_score: 2,
        },
        UniverseSeedCandidate {
            symbol: "PLTR",
            role: "liquid_high_beta_weekly",
            rationale: "Liquid high-beta single-name weekly chain with enough premium and movement to test frequent defined-risk weekly entries under conservative fills.",
            liquidity_score: 5,
            premium_score: 4,
            spread_quality_score: 4,
            price_fit_score: 5,
            diversification_score: 4,
            event_risk_score: 3,
        },
        UniverseSeedCandidate {
            symbol: "ORCL",
            role: "enterprise_software_control",
            rationale: "Large-cap software name selected as a lower-volatility control against the more speculative weekly candidates.",
            liquidity_score: 4,
            premium_score: 3,
            spread_quality_score: 4,
            price_fit_score: 4,
            diversification_score: 5,
            event_risk_score: 4,
        },
        UniverseSeedCandidate {
            symbol: "TSLA",
            role: "premium_liquidity_leader",
            rationale: "Very active weekly option chain with rich premium; useful stress test for cadence, gaps, and drawdown controls.",
            liquidity_score: 5,
            premium_score: 5,
            spread_quality_score: 4,
            price_fit_score: 5,
            diversification_score: 5,
            event_risk_score: 2,
        },
        UniverseSeedCandidate {
            symbol: "CRWV",
            role: "new_high_vol_weekly_candidate",
            rationale: "Newer high-volatility AI infrastructure name selected for exploratory weekly research; must overcome limited history and execution-quality risk.",
            liquidity_score: 2,
            premium_score: 5,
            spread_quality_score: 2,
            price_fit_score: 4,
            diversification_score: 4,
            event_risk_score: 1,
        },
    ]
}

fn universe_seed_candidates() -> Vec<UniverseSeedCandidate> {
    vec![
        UniverseSeedCandidate {
            symbol: "TSLA",
            role: "premium_liquidity_leader",
            rationale: "High-liquidity, premium-rich single-stock option chain; tests whether rich credits survive gap and drawdown risk.",
            liquidity_score: 5,
            premium_score: 5,
            spread_quality_score: 4,
            price_fit_score: 5,
            diversification_score: 5,
            event_risk_score: 2,
        },
        UniverseSeedCandidate {
            symbol: "AMD",
            role: "semiconductor_beta_peer",
            rationale: "Liquid semiconductor chain with NVDA-adjacent beta; tests whether the detector is sector-specific or transferable.",
            liquidity_score: 5,
            premium_score: 4,
            spread_quality_score: 4,
            price_fit_score: 5,
            diversification_score: 4,
            event_risk_score: 4,
        },
        UniverseSeedCandidate {
            symbol: "META",
            role: "mega_cap_premium_growth",
            rationale: "Deep mega-cap growth chain with active weeklies and historically usable premium; tests a non-semiconductor high-beta large cap.",
            liquidity_score: 5,
            premium_score: 4,
            spread_quality_score: 4,
            price_fit_score: 4,
            diversification_score: 5,
            event_risk_score: 4,
        },
        UniverseSeedCandidate {
            symbol: "AMZN",
            role: "commerce_cloud_growth",
            rationale: "Large, liquid growth stock with active weeklies; adds a different earnings and volatility profile than semiconductors and social ads.",
            liquidity_score: 5,
            premium_score: 3,
            spread_quality_score: 4,
            price_fit_score: 4,
            diversification_score: 5,
            event_risk_score: 4,
        },
        UniverseSeedCandidate {
            symbol: "AAPL",
            role: "liquidity_quality_anchor",
            rationale: "Deep, tight option chain with lower relative premium; useful as an execution-quality control for conservative fills.",
            liquidity_score: 5,
            premium_score: 2,
            spread_quality_score: 4,
            price_fit_score: 4,
            diversification_score: 5,
            event_risk_score: 5,
        },
        UniverseSeedCandidate {
            symbol: "MSFT",
            role: "liquidity_quality_candidate",
            rationale: "Deep option market with high execution quality, but usually less premium than the default growth candidates.",
            liquidity_score: 5,
            premium_score: 2,
            spread_quality_score: 5,
            price_fit_score: 3,
            diversification_score: 4,
            event_risk_score: 5,
        },
        UniverseSeedCandidate {
            symbol: "GOOGL",
            role: "mega_cap_quality_candidate",
            rationale: "Liquid mega-cap option chain that can validate whether the spread detector works outside higher-premium beta names.",
            liquidity_score: 5,
            premium_score: 2,
            spread_quality_score: 4,
            price_fit_score: 4,
            diversification_score: 4,
            event_risk_score: 5,
        },
        UniverseSeedCandidate {
            symbol: "AVGO",
            role: "semiconductor_quality_candidate",
            rationale: "High-quality semiconductor beta candidate, but higher share price can make fixed-width put-spread selection less ergonomic.",
            liquidity_score: 4,
            premium_score: 4,
            spread_quality_score: 3,
            price_fit_score: 2,
            diversification_score: 4,
            event_risk_score: 3,
        },
        UniverseSeedCandidate {
            symbol: "NFLX",
            role: "premium_growth_candidate",
            rationale: "Premium-rich growth chain with useful non-semiconductor exposure, but execution and event gaps need strict OOS proof.",
            liquidity_score: 4,
            premium_score: 3,
            spread_quality_score: 3,
            price_fit_score: 3,
            diversification_score: 4,
            event_risk_score: 3,
        },
        UniverseSeedCandidate {
            symbol: "COIN",
            role: "high_premium_candidate",
            rationale: "Very premium-rich chain, but gap risk and spread quality make it a lower-confidence expansion seed for conservative credit spreads.",
            liquidity_score: 3,
            premium_score: 5,
            spread_quality_score: 2,
            price_fit_score: 5,
            diversification_score: 4,
            event_risk_score: 1,
        },
    ]
}

fn universe_seed_from_candidate(candidate: &UniverseSeedCandidate) -> UniverseSeedSymbol {
    UniverseSeedSymbol {
        rank: 0,
        symbol: candidate.symbol.to_owned(),
        role: candidate.role.to_owned(),
        rationale: candidate.rationale.to_owned(),
        suitability_score: Some(universe_seed_suitability_score(candidate)),
        liquidity_score: Some(candidate.liquidity_score),
        premium_score: Some(candidate.premium_score),
        spread_quality_score: Some(candidate.spread_quality_score),
        price_fit_score: Some(candidate.price_fit_score),
        diversification_score: Some(candidate.diversification_score),
        event_risk_score: Some(candidate.event_risk_score),
    }
}

fn universe_seed_suitability_score(candidate: &UniverseSeedCandidate) -> u16 {
    3 * candidate.liquidity_score as u16
        + 2 * candidate.premium_score as u16
        + 2 * candidate.spread_quality_score as u16
        + candidate.price_fit_score as u16
        + candidate.diversification_score as u16
        + candidate.event_risk_score as u16
}

fn universe_seed_order(a: &UniverseSeedSymbol, b: &UniverseSeedSymbol) -> Ordering {
    b.suitability_score
        .cmp(&a.suitability_score)
        .then_with(|| a.symbol.cmp(&b.symbol))
}

fn research_report_path(path: &Path) -> PathBuf {
    if path.is_dir() {
        path.join("research.json")
    } else {
        path.to_path_buf()
    }
}

fn checked_plateau_run(
    plateau_run: Option<PathBuf>,
    allow_pre_plateau: bool,
) -> Result<Option<PathBuf>> {
    let Some(path) = plateau_run else {
        if allow_pre_plateau {
            return Ok(None);
        }
        anyhow::bail!(
            "research-universe requires --plateau-run with expansion_ready=true; pass --allow-pre-plateau for manual exploratory universe research"
        );
    };

    let report_path = research_report_path(&path);
    let plateau_status = read_plateau_run_gate(&report_path)?;
    if !plateau_status.expansion_ready && !allow_pre_plateau {
        anyhow::bail!(
            "plateau run {} is not expansion-ready; status={}",
            report_path.display(),
            plateau_status.status
        );
    }
    Ok(Some(report_path))
}

fn read_plateau_run_gate(path: &Path) -> Result<PlateauRunStatus> {
    let body = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    parse_plateau_run_gate(&body).with_context(|| format!("parsing {}", path.display()))
}

fn parse_plateau_run_gate(body: &str) -> Result<PlateauRunStatus> {
    let gate: PlateauRunGate = serde_json::from_str(body)?;
    Ok(gate.plateau_status)
}

fn universe_symbol_summary(
    report: &ResearchReport,
    expansion_seed: &[UniverseSeedSymbol],
) -> UniverseSymbolSummary {
    let best = report.profiles.first();
    let best_fixed = report.fixed_profile_walk_forward.first();
    let detector_score = best
        .map(|result| result.metrics.robust_score)
        .unwrap_or_default();
    let execution_oos_score = conservative_execution_oos_score(report);
    let (research_status, error_message) =
        universe_research_outcome(report.expirations_loaded, report.rows_loaded);
    let fixed_profile_oos_passes = report
        .fixed_profile_walk_forward
        .iter()
        .filter(|result| research_metrics_oos_passes(&result.metrics))
        .count();
    let seed = expansion_seed
        .iter()
        .find(|seed| seed.symbol == report.symbol);
    UniverseSymbolSummary {
        suitability_rank: 0,
        symbol: report.symbol.clone(),
        seed_rank: seed.map(|seed| seed.rank),
        seed_suitability_score: seed.and_then(|seed| seed.suitability_score),
        seed_role: seed.map(|seed| seed.role.clone()),
        seed_rationale: seed.map(|seed| seed.rationale.clone()),
        research_status,
        error_message,
        report_dir: PathBuf::from("runs")
            .join(&report.run_id)
            .display()
            .to_string(),
        deployment_status: report.deployment_gate.status.clone(),
        plateau_status: report.plateau_status.status.clone(),
        detector_status: report.plateau_status.detector_status.clone(),
        execution_strategy_status: report.plateau_status.execution_strategy_status.clone(),
        expansion_ready: report.plateau_status.expansion_ready,
        expirations_loaded: report.expirations_loaded,
        rows_loaded: report.rows_loaded,
        profiles_evaluated: report.profiles.len(),
        best_profile: best
            .map(|result| result.profile.name.clone())
            .unwrap_or_default(),
        best_detector: best
            .map(|result| result.detector_strategy.name.clone())
            .unwrap_or_default(),
        best_execution: best
            .map(|result| result.execution_strategy.name.clone())
            .unwrap_or_default(),
        best_detector_details: best.map(|result| result.detector_strategy.clone()),
        best_execution_details: best.map(|result| result.execution_strategy.clone()),
        detector_score,
        execution_oos_score,
        trades: best.map(|result| result.metrics.trades).unwrap_or_default(),
        total_pnl: best
            .map(|result| result.metrics.total_pnl)
            .unwrap_or_default(),
        score: best.map(|result| result.metrics.score).unwrap_or_default(),
        robust_score: best
            .map(|result| result.metrics.robust_score)
            .unwrap_or_default(),
        walk_forward_trades: report.walk_forward.metrics.trades,
        walk_forward_pnl: report.walk_forward.metrics.total_pnl,
        walk_forward_score: report.walk_forward.metrics.score,
        holdout_trades: report.holdout.metrics.trades,
        holdout_pnl: report.holdout.metrics.total_pnl,
        holdout_score: report.holdout.metrics.score,
        fixed_profile_oos_passes,
        best_fixed_profile: best_fixed
            .map(|result| result.profile.name.clone())
            .unwrap_or_default(),
        best_fixed_detector: best_fixed
            .map(|result| result.detector_strategy.name.clone())
            .unwrap_or_default(),
        best_fixed_execution: best_fixed
            .map(|result| result.execution_strategy.name.clone())
            .unwrap_or_default(),
        best_fixed_detector_details: best_fixed.map(|result| result.detector_strategy.clone()),
        best_fixed_execution_details: best_fixed.map(|result| result.execution_strategy.clone()),
        best_fixed_trades: best_fixed
            .map(|result| result.metrics.trades)
            .unwrap_or_default(),
        best_fixed_pnl: best_fixed
            .map(|result| result.metrics.total_pnl)
            .unwrap_or_default(),
        best_fixed_score: best_fixed
            .map(|result| result.metrics.score)
            .unwrap_or_default(),
        best_fixed_robust_score: best_fixed
            .map(|result| result.metrics.robust_score)
            .unwrap_or_default(),
        latest_signal_status: report
            .latest_signal
            .as_ref()
            .map(|signal| signal.status.clone()),
    }
}

fn conservative_execution_oos_score(report: &ResearchReport) -> f64 {
    let mut score = report.walk_forward.metrics.score;
    if report.holdout.active {
        score = score.min(report.holdout.metrics.score);
    }
    if let Some(best_fixed) = report.fixed_profile_walk_forward.first() {
        score = score.min(best_fixed.metrics.score);
    }
    score
}

fn universe_research_outcome(
    expirations_loaded: usize,
    rows_loaded: usize,
) -> (String, Option<String>) {
    if rows_loaded > 0 {
        return ("ok".to_owned(), None);
    }

    let message = if expirations_loaded == 0 {
        "ThetaData loaded zero expirations for this symbol/window; not comparable until data is available."
    } else {
        "ThetaData loaded expirations but zero usable EOD rows for this symbol/window; not comparable until data is available."
    };
    ("no_data".to_owned(), Some(message.to_owned()))
}

fn universe_symbol_error_summary(
    symbol: &str,
    expansion_seed: &[UniverseSeedSymbol],
    err: &anyhow::Error,
) -> UniverseSymbolSummary {
    let symbol = symbol.to_uppercase();
    let seed = expansion_seed.iter().find(|seed| seed.symbol == symbol);
    UniverseSymbolSummary {
        suitability_rank: 0,
        symbol,
        seed_rank: seed.map(|seed| seed.rank),
        seed_suitability_score: seed.and_then(|seed| seed.suitability_score),
        seed_role: seed.map(|seed| seed.role.clone()),
        seed_rationale: seed.map(|seed| seed.rationale.clone()),
        research_status: "error".to_owned(),
        error_message: Some(compact_error_message(&format!("{err:#}"))),
        report_dir: "n/a".to_owned(),
        deployment_status: "error".to_owned(),
        plateau_status: "error".to_owned(),
        detector_status: "not_run".to_owned(),
        execution_strategy_status: "not_run".to_owned(),
        expansion_ready: false,
        expirations_loaded: 0,
        rows_loaded: 0,
        profiles_evaluated: 0,
        best_profile: String::new(),
        best_detector: String::new(),
        best_execution: String::new(),
        best_detector_details: None,
        best_execution_details: None,
        detector_score: 0.0,
        execution_oos_score: 0.0,
        trades: 0,
        total_pnl: 0.0,
        score: 0.0,
        robust_score: 0.0,
        walk_forward_trades: 0,
        walk_forward_pnl: 0.0,
        walk_forward_score: 0.0,
        holdout_trades: 0,
        holdout_pnl: 0.0,
        holdout_score: 0.0,
        fixed_profile_oos_passes: 0,
        best_fixed_profile: String::new(),
        best_fixed_detector: String::new(),
        best_fixed_execution: String::new(),
        best_fixed_detector_details: None,
        best_fixed_execution_details: None,
        best_fixed_trades: 0,
        best_fixed_pnl: 0.0,
        best_fixed_score: 0.0,
        best_fixed_robust_score: 0.0,
        latest_signal_status: None,
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

fn research_metrics_oos_passes(metrics: &ResearchMetrics) -> bool {
    metrics.ranking_eligible && metrics.total_pnl > 0.0 && metrics.score > 0.0
}

fn rank_universe_results(results: &mut [UniverseSymbolSummary]) {
    results.sort_by(universe_result_order);
    for (idx, result) in results.iter_mut().enumerate() {
        result.suitability_rank = idx + 1;
    }
}

fn universe_result_order(a: &UniverseSymbolSummary, b: &UniverseSymbolSummary) -> Ordering {
    universe_research_succeeded(b)
        .cmp(&universe_research_succeeded(a))
        .then_with(|| universe_deployment_passes(b).cmp(&universe_deployment_passes(a)))
        .then_with(|| b.fixed_profile_oos_passes.cmp(&a.fixed_profile_oos_passes))
        .then_with(|| b.execution_oos_score.total_cmp(&a.execution_oos_score))
        .then_with(|| b.walk_forward_score.total_cmp(&a.walk_forward_score))
        .then_with(|| b.holdout_score.total_cmp(&a.holdout_score))
        .then_with(|| b.best_fixed_score.total_cmp(&a.best_fixed_score))
        .then_with(|| b.robust_score.total_cmp(&a.robust_score))
        .then_with(|| b.score.total_cmp(&a.score))
        .then_with(|| b.rows_loaded.cmp(&a.rows_loaded))
        .then_with(|| {
            a.seed_rank
                .unwrap_or(usize::MAX)
                .cmp(&b.seed_rank.unwrap_or(usize::MAX))
        })
        .then_with(|| a.symbol.cmp(&b.symbol))
}

fn universe_research_succeeded(summary: &UniverseSymbolSummary) -> bool {
    summary.research_status == "ok"
}

fn universe_deployment_passes(summary: &UniverseSymbolSummary) -> bool {
    summary.deployment_status == "pass"
}

fn markdown_cell(value: &str) -> String {
    value.replace('|', "\\|")
}

fn optional_u8(value: Option<u8>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "n/a".to_owned())
}

fn optional_u16(value: Option<u16>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "n/a".to_owned())
}

fn optional_usize(value: Option<usize>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "all".to_owned())
}

fn detector_strategy_details(strategy: Option<&DetectorStrategySummary>) -> String {
    let Some(strategy) = strategy else {
        return "n/a".to_owned();
    };
    let filters = if strategy.filters.is_empty() {
        "none".to_owned()
    } else {
        strategy.filters.join("; ")
    };
    format!(
        "dte {}-{}; delta {:.2}-{:.2}; width {:.0}-{:.0}; credit/width >= {:.2}; quote width <= max({:.0}% mid, ${:.2}); OI short >= {}, long >= {}; filters: {}",
        strategy.min_dte,
        strategy.max_dte,
        strategy.min_short_delta_abs,
        strategy.max_short_delta_abs,
        strategy.min_width,
        strategy.max_width,
        strategy.min_credit_width,
        strategy.max_quote_width_pct_of_mid * 100.0,
        strategy.max_quote_width_abs,
        strategy.min_short_oi,
        strategy.min_long_oi,
        filters
    )
}

fn execution_strategy_details(strategy: Option<&ExecutionStrategySummary>) -> String {
    let Some(strategy) = strategy else {
        return "n/a".to_owned();
    };
    let max_hold = strategy
        .max_hold_days
        .map(|days| format!("{days}d"))
        .unwrap_or_else(|| "none".to_owned());
    format!(
        "selector {}; entry {}; exit {}; take profit {:.0}%; stop {:.1}x credit; force close {} DTE; max hold {}; stop cooldown {}d; max positions {}; entry spacing {}d",
        strategy.candidate_selector,
        strategy.entry_fill_model,
        strategy.exit_fill_model,
        strategy.take_profit_pct * 100.0,
        strategy.stop_loss_multiple,
        strategy.force_close_dte,
        max_hold,
        strategy.stop_loss_cooldown_days,
        strategy.max_concurrent_positions,
        strategy.min_entry_spacing_days
    )
}

fn universe_markdown(summary: &UniverseResearchSummary) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "# SpreadFoundry Universe Research {}\n\n",
        summary.run_id
    ));
    out.push_str(&format!(
        "- Status: `{}`\n- Profile family: `{}`\n- Window: `{}` to `{}`\n- Symbols: `{}`\n- Symbols completed: `{}/{}`\n- Plateau run: `{}`\n- Max expirations per symbol: `{}`\n- Fetch concurrency: `{}`\n- Force refresh: `{}`\n- Strategy: `{}`\n- Selection basis: {}\n- Seed score basis: {}\n- Research method: {}\n\n",
        summary.run_status,
        summary.profile_family,
        summary.from,
        summary.to,
        summary.symbols.join(", "),
        summary.symbols_completed,
        summary.symbols_requested,
        summary.plateau_run.as_deref().unwrap_or("not provided"),
        optional_usize(summary.max_expirations),
        summary.fetch_concurrency,
        summary.force_refresh,
        summary.strategy,
        summary.selection_basis,
        summary.seed_score_basis,
        summary.research_method
    ));

    out.push_str("## Research Protocol\n\n");
    out.push_str("- Detector search: each symbol gets its own DTE, delta, credit, width, liquidity, IV, trend, drawdown, and realized-volatility filters selected only from that symbol's historical training data.\n");
    out.push_str("- Execution strategy search: take-profit, stop-loss, force-close DTE, cooldown, and spread-selection rules are scored separately from detector filters under conservative bid/ask fills.\n");
    out.push_str(&format!(
        "- Detector score: {}\n- Execution OOS score: {}\n",
        summary.detector_score_basis, summary.execution_score_basis
    ));
    out.push_str("- Promotion rule: seed order never promotes a symbol; fixed-profile OOS passes, walk-forward evidence, holdout evidence, and deployment gates drive the suitability ranking.\n\n");

    out.push_str("## Expansion Seed\n\n");
    out.push_str("| Rank | Symbol | Score | Liquidity | Premium | Spread Quality | Price Fit | Diversification | Event Risk Discipline | Role | Rationale |\n");
    out.push_str("|---:|---|---:|---:|---:|---:|---:|---:|---:|---|---|\n");
    for seed in &summary.expansion_seed {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            seed.rank,
            seed.symbol,
            optional_u16(seed.suitability_score),
            optional_u8(seed.liquidity_score),
            optional_u8(seed.premium_score),
            optional_u8(seed.spread_quality_score),
            optional_u8(seed.price_fit_score),
            optional_u8(seed.diversification_score),
            optional_u8(seed.event_risk_score),
            seed.role,
            seed.rationale
        ));
    }
    out.push('\n');

    out.push_str("## Symbol Suitability Ranking\n\n");
    out.push_str("| Rank | Seed Rank | Seed Score | Symbol | Research | Error | Report | Deployment | Plateau | Detector Status | Execution Status | Detector Score | Execution OOS Score | Fixed OOS Passes | Best Fixed Profile | Best Fixed Detector | Best Fixed Execution | Fixed Trades | Fixed PnL | Fixed Score | Fixed Robust | Best Profile | Detector | Execution | Trades | PnL | Score | Robust Score | WF Trades | WF PnL | WF Score | Holdout Trades | Holdout PnL | Holdout Score | Expirations | Rows | Latest Signal |\n");
    out.push_str(
        "|---:|---:|---:|---|---|---|---|---|---|---|---|---:|---:|---:|---|---|---|---:|---:|---:|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|\n",
    );
    for result in &summary.results {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {:.4} | {:.4} | {} | {} | {} | {} | {} | {:.2} | {:.4} | {:.4} | {} | {} | {} | {} | {:.2} | {:.4} | {:.4} | {} | {:.2} | {:.4} | {} | {:.2} | {:.4} | {} | {} | {} |\n",
            result.suitability_rank,
            result
                .seed_rank
                .map(|rank| rank.to_string())
                .unwrap_or_else(|| "n/a".to_owned()),
            optional_u16(result.seed_suitability_score),
            result.symbol,
            result.research_status,
            result
                .error_message
                .as_deref()
                .map(markdown_cell)
                .unwrap_or_else(|| "none".to_owned()),
            result.report_dir,
            result.deployment_status,
            result.plateau_status,
            result.detector_status,
            result.execution_strategy_status,
            result.detector_score,
            result.execution_oos_score,
            result.fixed_profile_oos_passes,
            result.best_fixed_profile,
            result.best_fixed_detector,
            result.best_fixed_execution,
            result.best_fixed_trades,
            result.best_fixed_pnl,
            result.best_fixed_score,
            result.best_fixed_robust_score,
            result.best_profile,
            result.best_detector,
            result.best_execution,
            result.trades,
            result.total_pnl,
            result.score,
            result.robust_score,
            result.walk_forward_trades,
            result.walk_forward_pnl,
            result.walk_forward_score,
            result.holdout_trades,
            result.holdout_pnl,
            result.holdout_score,
            result.expirations_loaded,
            result.rows_loaded,
            result.latest_signal_status.as_deref().unwrap_or("none")
        ));
    }
    out.push('\n');

    out.push_str("## Strategy Details\n\n");
    out.push_str("| Rank | Symbol | Best Detector Rules | Best Execution Rules | Best Fixed Detector Rules | Best Fixed Execution Rules |\n");
    out.push_str("|---:|---|---|---|---|---|\n");
    for result in &summary.results {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} |\n",
            result.suitability_rank,
            result.symbol,
            markdown_cell(&detector_strategy_details(
                result.best_detector_details.as_ref()
            )),
            markdown_cell(&execution_strategy_details(
                result.best_execution_details.as_ref()
            )),
            markdown_cell(&detector_strategy_details(
                result.best_fixed_detector_details.as_ref()
            )),
            markdown_cell(&execution_strategy_details(
                result.best_fixed_execution_details.as_ref()
            ))
        ));
    }
    out
}

fn read_toml<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let body = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    toml::from_str(&body).with_context(|| format!("parsing {}", path.display()))
}

fn ensure_fixture_mode(data_mode: &str) -> Result<()> {
    if data_mode != "fixture" {
        anyhow::bail!(
            "only data_mode=\"fixture\" is implemented in this first commit; Theta normalization is next"
        );
    }
    Ok(())
}

fn fixture_exit_quotes(name: Option<&str>) -> Result<Vec<SpreadExitQuote>> {
    match name.unwrap_or("take_profit") {
        "take_profit" => Ok(fixture::nvda_exit_quotes_take_profit()),
        "stop_loss" => Ok(fixture::nvda_exit_quotes_stop_loss()),
        other => anyhow::bail!(
            "unsupported fixture_exit={other:?}; expected \"take_profit\" or \"stop_loss\""
        ),
    }
}

fn next_run_dir(prefix: &str) -> Result<PathBuf> {
    let run_id = format!("{}-{}", prefix, Utc::now().format("%Y%m%dT%H%M%S%.9fZ"));
    Ok(PathBuf::from("runs").join(run_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_symbols_uppercases_deduplicates_and_drops_empty_values() {
        assert_eq!(
            normalize_symbols(vec![
                " tsla ".to_owned(),
                "AAPL".to_owned(),
                "".to_owned(),
                "tsla".to_owned(),
            ]),
            vec!["TSLA".to_owned(), "AAPL".to_owned()]
        );
    }

    #[test]
    fn default_universe_seed_is_eight_non_nvda_single_stocks() {
        let seed = default_universe_seed(ResearchProfileFamily::Swing);

        assert_eq!(seed.len(), 8);
        assert_eq!(
            seed.iter()
                .map(|symbol| symbol.symbol.as_str())
                .collect::<Vec<_>>(),
            DEFAULT_PLATEAU_UNIVERSE_SYMBOLS.to_vec()
        );
        assert!(!seed.iter().any(|symbol| symbol.symbol == "NVDA"));
        assert!(seed.iter().all(|symbol| !symbol.rationale.is_empty()));
        assert!(seed.iter().all(|symbol| symbol.suitability_score.is_some()));
        assert!(
            seed.windows(2)
                .all(|pair| pair[0].suitability_score >= pair[1].suitability_score)
        );
        assert_eq!(
            seed[0].suitability_score,
            Some(universe_seed_suitability_score(
                &universe_seed_candidates()[0]
            ))
        );
    }

    #[test]
    fn expansion_seed_marks_manual_overrides() {
        let seed = expansion_seed_for_symbols(
            &["AAPL".to_owned(), "COIN".to_owned()],
            ResearchProfileFamily::Swing,
        );

        assert_eq!(seed[0].rank, 1);
        assert_eq!(seed[0].symbol, "AAPL");
        assert_eq!(seed[0].role, "liquidity_quality_anchor");
        assert!(seed[0].suitability_score.is_some());
        assert_eq!(seed[1].rank, 2);
        assert_eq!(seed[1].symbol, "COIN");
        assert_eq!(seed[1].role, "manual_override");
        assert_eq!(seed[1].suitability_score, None);
    }

    #[test]
    fn weekly_expansion_seed_explains_weekly_symbols() {
        let seed = expansion_seed_for_symbols(
            &["IREN".to_owned(), "PLTR".to_owned(), "CRWV".to_owned()],
            ResearchProfileFamily::Weekly,
        );

        assert_eq!(seed[0].symbol, "IREN");
        assert_eq!(seed[0].role, "high_premium_weekly_candidate");
        assert_eq!(seed[1].symbol, "PLTR");
        assert_eq!(seed[1].role, "liquid_high_beta_weekly");
        assert_eq!(seed[2].symbol, "CRWV");
        assert_eq!(seed[2].role, "new_high_vol_weekly_candidate");
        assert!(seed.iter().all(|symbol| symbol.suitability_score.is_some()));
    }

    #[test]
    fn plateau_gate_parses_minimal_research_json() {
        let status = parse_plateau_run_gate(
            r#"{"plateau_status":{"status":"plateau_expand_universe","expansion_ready":true}}"#,
        )
        .unwrap();

        assert_eq!(status.status, "plateau_expand_universe");
        assert!(status.expansion_ready);
    }

    #[test]
    fn universe_research_requires_plateau_run_unless_explicitly_overridden() {
        let err = checked_plateau_run(None, false).unwrap_err();

        assert!(
            err.to_string()
                .contains("requires --plateau-run with expansion_ready=true")
        );
        assert!(checked_plateau_run(None, true).unwrap().is_none());
    }

    #[test]
    fn universe_research_accepts_only_expansion_ready_plateau_run() {
        let run_dir = unique_main_test_path("plateau-run");
        fs::create_dir_all(&run_dir).unwrap();
        fs::write(
            run_dir.join("research.json"),
            r#"{"plateau_status":{"status":"continue_symbol_research","expansion_ready":false}}"#,
        )
        .unwrap();

        let err = checked_plateau_run(Some(run_dir.clone()), false).unwrap_err();
        assert!(err.to_string().contains("is not expansion-ready"));

        let pre_plateau = checked_plateau_run(Some(run_dir.join("research.json")), true).unwrap();
        assert_eq!(pre_plateau, Some(run_dir.join("research.json")));

        fs::write(
            run_dir.join("research.json"),
            r#"{"plateau_status":{"status":"plateau_expand_universe","expansion_ready":true}}"#,
        )
        .unwrap();

        let checked = checked_plateau_run(Some(run_dir.clone()), false).unwrap();
        assert_eq!(checked, Some(run_dir.join("research.json")));

        fs::remove_dir_all(run_dir).unwrap();
    }

    #[test]
    fn research_symbol_accepts_expand_on_plateau_flag() {
        let cli = Cli::try_parse_from([
            "spreadfoundry",
            "research-symbol",
            "--symbol",
            "nvda",
            "--to",
            "2026-06-21",
            "--expand-on-plateau",
        ])
        .unwrap();

        match cli.command {
            Commands::ResearchSymbol {
                symbol,
                expand_on_plateau,
                single_symbol_only,
                ..
            } => {
                assert_eq!(symbol, "nvda");
                assert!(expand_on_plateau);
                assert!(!single_symbol_only);
                assert!(should_expand_on_plateau(
                    expand_on_plateau,
                    single_symbol_only
                ));
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn research_symbol_expands_on_plateau_by_default() {
        let cli = Cli::try_parse_from([
            "spreadfoundry",
            "research-symbol",
            "--symbol",
            "nvda",
            "--to",
            "2026-06-21",
        ])
        .unwrap();

        match cli.command {
            Commands::ResearchSymbol {
                expand_on_plateau,
                single_symbol_only,
                ..
            } => {
                assert!(!expand_on_plateau);
                assert!(!single_symbol_only);
                assert!(should_expand_on_plateau(
                    expand_on_plateau,
                    single_symbol_only
                ));
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn research_symbol_can_disable_plateau_expansion() {
        let cli = Cli::try_parse_from([
            "spreadfoundry",
            "research-symbol",
            "--symbol",
            "nvda",
            "--to",
            "2026-06-21",
            "--single-symbol-only",
        ])
        .unwrap();

        match cli.command {
            Commands::ResearchSymbol {
                expand_on_plateau,
                single_symbol_only,
                ..
            } => {
                assert!(!expand_on_plateau);
                assert!(single_symbol_only);
                assert!(!should_expand_on_plateau(
                    expand_on_plateau,
                    single_symbol_only
                ));
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn weekly_universe_defaults_to_put_debit_family() {
        let cli = Cli::try_parse_from([
            "spreadfoundry",
            "research-weekly-universe",
            "--to",
            "2026-06-21",
        ])
        .unwrap();

        match cli.command {
            Commands::ResearchWeeklyUniverse { profile_family, .. } => {
                assert_eq!(profile_family, ProfileFamilyArg::WeeklyPutDebit);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn weekly_universe_accepts_call_credit_family() {
        let cli = Cli::try_parse_from([
            "spreadfoundry",
            "research-weekly-universe",
            "--to",
            "2026-06-21",
            "--profile-family",
            "weekly-call-credit",
        ])
        .unwrap();

        match cli.command {
            Commands::ResearchWeeklyUniverse { profile_family, .. } => {
                assert_eq!(profile_family, ProfileFamilyArg::WeeklyCallCredit);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn audit_option_cache_coverage_accepts_symbols_and_json() {
        let cli = Cli::try_parse_from([
            "spreadfoundry",
            "audit-option-cache-coverage",
            "--symbols",
            "IREN,TSLA",
            "--from",
            "2020-01-01",
            "--to",
            "2026-06-26",
            "--max-expirations",
            "80",
            "--json",
        ])
        .unwrap();

        match cli.command {
            Commands::AuditOptionCacheCoverage {
                symbols,
                from,
                to,
                max_expirations,
                json,
            } => {
                assert_eq!(symbols, vec!["IREN", "TSLA"]);
                assert_eq!(from.to_string(), "2020-01-01");
                assert_eq!(to.to_string(), "2026-06-26");
                assert_eq!(max_expirations, Some(80));
                assert!(json);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn warm_option_cache_coverage_accepts_bounds_and_json() {
        let cli = Cli::try_parse_from([
            "spreadfoundry",
            "warm-option-cache-coverage",
            "--symbols",
            "SOFI,HOOD",
            "--from",
            "2024-01-01",
            "--to",
            "2026-06-28",
            "--max-expirations",
            "80",
            "--max-windows-per-symbol",
            "6",
            "--fetch-concurrency",
            "2",
            "--json",
        ])
        .unwrap();

        match cli.command {
            Commands::WarmOptionCacheCoverage {
                symbols,
                from,
                to,
                max_expirations,
                max_windows_per_symbol,
                fetch_concurrency,
                json,
                ..
            } => {
                assert_eq!(symbols, vec!["SOFI", "HOOD"]);
                assert_eq!(from.to_string(), "2024-01-01");
                assert_eq!(to.to_string(), "2026-06-28");
                assert_eq!(max_expirations, Some(80));
                assert_eq!(max_windows_per_symbol, 6);
                assert_eq!(fetch_concurrency, 2);
                assert!(json);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn audit_weekly_signal_gates_accepts_profile_family_and_json() {
        let cli = Cli::try_parse_from([
            "spreadfoundry",
            "audit-weekly-signal-gates",
            "--symbol",
            "orcl",
            "--from",
            "2016-01-01",
            "--to",
            "2026-06-28",
            "--max-expirations",
            "80",
            "--cache-only",
            "--profile-family",
            "weekly-call-debit",
            "--json",
        ])
        .unwrap();

        match cli.command {
            Commands::AuditWeeklySignalGates {
                symbol,
                from,
                to,
                max_expirations,
                cache_only,
                profile_family,
                json,
                ..
            } => {
                assert_eq!(symbol, "orcl");
                assert_eq!(from.to_string(), "2016-01-01");
                assert_eq!(to.to_string(), "2026-06-28");
                assert_eq!(max_expirations, Some(80));
                assert!(cache_only);
                assert_eq!(profile_family, ProfileFamilyArg::WeeklyCallDebit);
                assert!(json);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn research_portfolio_wheel_accepts_budget_controls() {
        let cli = Cli::try_parse_from([
            "spreadfoundry",
            "research-portfolio-wheel",
            "--symbols",
            "IREN,PLTR",
            "--to",
            "2026-06-21",
            "--capital-budget",
            "75000",
            "--max-symbol-allocation-pct",
            "0.4",
            "--max-open-positions",
            "3",
            "--max-positions-per-symbol",
            "1",
            "--cache-only",
        ])
        .unwrap();

        match cli.command {
            Commands::ResearchPortfolioWheel {
                symbols,
                capital_budget,
                max_symbol_allocation_pct,
                max_open_positions,
                max_positions_per_symbol,
                cache_only,
                ..
            } => {
                assert_eq!(symbols, vec!["IREN", "PLTR"]);
                assert_eq!(capital_budget, 75_000.0);
                assert_eq!(max_symbol_allocation_pct, 0.4);
                assert_eq!(max_open_positions, 3);
                assert_eq!(max_positions_per_symbol, 1);
                assert!(cache_only);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn export_portfolio_canary_accepts_run_and_output() {
        let cli = Cli::try_parse_from([
            "spreadfoundry",
            "export-portfolio-canary",
            "--run",
            "runs/example",
            "--output",
            "candidates/example.json",
            "--candidate-id",
            "weekly_selector_canary_test",
            "--frozen-on",
            "2026-06-28",
        ])
        .unwrap();

        match cli.command {
            Commands::ExportPortfolioCanary {
                run,
                output,
                candidate_id,
                frozen_on,
            } => {
                assert_eq!(run, PathBuf::from("runs/example"));
                assert_eq!(output, PathBuf::from("candidates/example.json"));
                assert_eq!(candidate_id, "weekly_selector_canary_test");
                assert_eq!(
                    frozen_on,
                    Some(NaiveDate::from_ymd_opt(2026, 6, 28).unwrap())
                );
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn portfolio_canary_status_accepts_require_action() {
        let cli = Cli::try_parse_from([
            "spreadfoundry",
            "portfolio-canary-status",
            "--candidate",
            "candidates/example.json",
            "--require-action",
        ])
        .unwrap();

        match cli.command {
            Commands::PortfolioCanaryStatus {
                candidate,
                as_of,
                require_action,
            } => {
                assert_eq!(candidate, PathBuf::from("candidates/example.json"));
                assert_eq!(as_of, None);
                assert!(require_action);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn run_portfolio_canary_accepts_tiny_budget_flags() {
        let cli = Cli::try_parse_from([
            "spreadfoundry",
            "run-portfolio-canary",
            "--candidate",
            "candidates/example.json",
            "--as-of",
            "2026-06-28",
            "--max-loss",
            "500",
            "--account-cash",
            "45000",
            "--debit-max-loss",
            "1000",
            "--wheel-reserve-cap",
            "35000",
            "--free-cash-buffer",
            "11250",
            "--max-wheel-positions-per-symbol",
            "1",
            "--broker-multi-leg-options",
            "--broker-cash-secured-puts",
            "--broker-covered-calls",
            "--broker-review-ok",
            "--live-orders-enabled",
            "--place-live-order",
            "--json",
        ])
        .unwrap();

        match cli.command {
            Commands::RunPortfolioCanary {
                candidate,
                as_of,
                max_loss,
                account_cash,
                debit_max_loss,
                wheel_reserve_cap,
                free_cash_buffer,
                max_wheel_positions_per_symbol,
                broker_multi_leg_options,
                broker_cash_secured_puts,
                broker_covered_calls,
                broker_review_ok,
                live_orders_enabled,
                robinhood_mcp_command,
                order_ledger,
                max_order_age_seconds,
                place_live_order,
                json,
            } => {
                assert_eq!(candidate, PathBuf::from("candidates/example.json"));
                assert_eq!(as_of, Some(NaiveDate::from_ymd_opt(2026, 6, 28).unwrap()));
                assert_eq!(max_loss, Some(500.0));
                assert_eq!(account_cash, 45_000.0);
                assert_eq!(debit_max_loss, 1_000.0);
                assert_eq!(wheel_reserve_cap, 35_000.0);
                assert_eq!(free_cash_buffer, 11_250.0);
                assert_eq!(max_wheel_positions_per_symbol, 1);
                assert!(broker_multi_leg_options);
                assert!(broker_cash_secured_puts);
                assert!(broker_covered_calls);
                assert!(broker_review_ok);
                assert!(live_orders_enabled);
                assert_eq!(robinhood_mcp_command, None);
                assert_eq!(order_ledger, PathBuf::from("var/canary_order_ledger.json"));
                assert_eq!(max_order_age_seconds, DEFAULT_MAX_ORDER_AGE_SECONDS);
                assert!(place_live_order);
                assert!(json);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn portfolio_canary_status_require_action_fails_closed_without_action() {
        let path = unique_main_test_path("canary-no-action.json");
        fs::write(
            &path,
            r#"{
                "candidate_id":"test",
                "status":"canary_only",
                "decision":{
                    "current_action_state":"no_open_or_same_day_entry_actions",
                    "recommended_capital_fraction":0.05
                },
                "latest_actions":[{"status":"recent_closed","symbol":"TSLA","strategy":"put_debit_spread","entry_date":"2026-06-25","exit_date":"2026-06-26","pnl":-50.0}]
            }"#,
        )
        .unwrap();

        let err = portfolio_canary_status(
            &path,
            Some(NaiveDate::from_ymd_opt(2026, 6, 26).unwrap()),
            true,
        )
        .unwrap_err();

        assert!(err.to_string().contains("no actionable canary signal"));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn portfolio_canary_status_require_action_accepts_entry_candidate() {
        let path = unique_main_test_path("canary-entry-action.json");
        fs::write(
            &path,
            r#"{
                "candidate_id":"test",
                "status":"canary_only",
                "decision":{
                    "current_action_state":"fresh_entry_or_open_candidate_present",
                    "recommended_capital_fraction":0.05
                },
                "latest_actions":[{"status":"entry_candidate","symbol":"TSLA","strategy":"put_debit_spread","entry_date":"2026-06-26","exit_date":"2026-06-26","pnl":0.0}]
            }"#,
        )
        .unwrap();

        portfolio_canary_status(
            &path,
            Some(NaiveDate::from_ymd_opt(2026, 6, 26).unwrap()),
            true,
        )
        .unwrap();

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn portfolio_canary_status_require_action_rejects_stale_entry_candidate() {
        let path = unique_main_test_path("canary-stale-entry-action.json");
        fs::write(
            &path,
            r#"{
                "candidate_id":"test",
                "status":"canary_only",
                "decision":{
                    "current_action_state":"fresh_entry_or_open_candidate_present",
                    "recommended_capital_fraction":0.05
                },
                "latest_actions":[{"status":"entry_candidate","symbol":"CRWV","strategy":"wheel","entry_date":"2026-06-26","exit_date":"2026-06-26","pnl":112.0}]
            }"#,
        )
        .unwrap();

        let err = portfolio_canary_status(
            &path,
            Some(NaiveDate::from_ymd_opt(2026, 6, 28).unwrap()),
            true,
        )
        .unwrap_err();

        assert!(err.to_string().contains("no actionable canary signal"));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn portfolio_canary_status_require_action_accepts_open_candidate_spanning_as_of() {
        let path = unique_main_test_path("canary-open-action.json");
        fs::write(
            &path,
            r#"{
                "candidate_id":"test",
                "status":"canary_only",
                "decision":{
                    "current_action_state":"fresh_entry_or_open_candidate_present",
                    "recommended_capital_fraction":0.05
                },
                "latest_actions":[{"status":"open_candidate","symbol":"TSLA","strategy":"put_debit_spread","entry_date":"2026-06-26","exit_date":"2026-06-30","pnl":0.0}]
            }"#,
        )
        .unwrap();

        portfolio_canary_status(
            &path,
            Some(NaiveDate::from_ymd_opt(2026, 6, 28).unwrap()),
            true,
        )
        .unwrap();

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn portfolio_canary_runner_returns_shadow_when_stale() {
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"entry_candidate",
            "symbol":"CRWV",
            "strategy":"wheel",
            "entry_date":"2026-06-26",
            "exit_date":"2026-06-26",
            "max_loss":7888.0
        }]));
        let broker = RobinhoodBrokerAdapter::default();

        let decision = portfolio_canary_run_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &test_canary_risk(),
            &broker,
            false,
            false,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "shadow_no_action");
        assert!(decision.selected_action.is_none());
    }

    #[test]
    fn portfolio_canary_runner_blocks_wheel_above_tiny_budget() {
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"entry_candidate",
            "symbol":"CRWV",
            "strategy":"wheel",
            "entry_date":"2026-06-28",
            "exit_date":"2026-06-28",
            "max_loss":7888.0
        }]));
        let broker = RobinhoodBrokerAdapter {
            capabilities: BrokerCapabilities {
                single_leg_options: true,
                multi_leg_options: true,
                stock_option_combos: false,
                cash_secured_puts: true,
                covered_calls: true,
            },
            live_orders_enabled: false,
        };

        let decision = portfolio_canary_run_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &CanaryRiskConfig {
                account_cash: 45_000.0,
                debit_max_loss: 1_000.0,
                wheel_reserve_cap: 5_000.0,
                free_cash_buffer: 11_250.0,
                max_wheel_positions_per_symbol: 1,
            },
            &broker,
            true,
            false,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "shadow_risk_blocked");
        assert_eq!(
            decision
                .selected_action
                .as_ref()
                .map(|action| action.strategy.as_str()),
            Some("wheel")
        );
    }

    #[test]
    fn portfolio_canary_runner_sends_tiny_debit_to_broker_gate() {
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"entry_candidate",
            "symbol":"TSLA",
            "strategy":"put_debit_spread",
            "entry_date":"2026-06-28",
            "exit_date":"2026-06-28",
            "max_loss":335.0
        }]));
        let broker = RobinhoodBrokerAdapter::default();

        let decision = portfolio_canary_run_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &test_canary_risk(),
            &broker,
            false,
            false,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "shadow_broker_unsupported");
        assert_eq!(
            decision
                .selected_action
                .as_ref()
                .map(|action| action.strategy.as_str()),
            Some("put_debit_spread")
        );
    }

    #[test]
    fn portfolio_canary_runner_requires_review_before_live_request() {
        let today = Utc::now().date_naive();
        let today_s = today.to_string();
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"entry_candidate",
            "symbol":"TSLA",
            "strategy":"put_debit_spread",
            "entry_date":today_s,
            "exit_date":today_s,
            "max_loss":100.0
        }]));
        let broker = RobinhoodBrokerAdapter {
            capabilities: BrokerCapabilities {
                single_leg_options: true,
                multi_leg_options: true,
                stock_option_combos: false,
                cash_secured_puts: false,
                covered_calls: false,
            },
            live_orders_enabled: true,
        };

        let decision = portfolio_canary_run_decision(
            &artifact,
            today,
            &test_canary_risk(),
            &broker,
            false,
            true,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "review_required");
        assert!(!decision.broker_review_ok);
    }

    #[test]
    fn portfolio_canary_runner_blocks_live_order_for_historical_as_of() {
        let historical = Utc::now()
            .date_naive()
            .pred_opt()
            .expect("test date has a predecessor");
        let historical_s = historical.to_string();
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"entry_candidate",
            "symbol":"TSLA",
            "strategy":"put_debit_spread",
            "entry_date":historical_s,
            "exit_date":historical_s,
            "max_loss":100.0
        }]));
        let broker = RobinhoodBrokerAdapter {
            capabilities: BrokerCapabilities {
                single_leg_options: true,
                multi_leg_options: true,
                stock_option_combos: false,
                cash_secured_puts: false,
                covered_calls: false,
            },
            live_orders_enabled: true,
        };

        let decision = portfolio_canary_run_decision(
            &artifact,
            historical,
            &test_canary_risk(),
            &broker,
            false,
            true,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "live_order_blocked");
        assert!(decision.reason.contains("today's UTC date"));
    }

    #[test]
    fn portfolio_canary_runner_blocks_artifact_that_is_not_canary_ready() {
        let artifact = serde_json::json!({
            "status":"blocked",
            "decision":{"canary_ready":false,"research_gate":"blocked"},
            "latest_actions":[{
                "status":"entry_candidate",
                "symbol":"TSLA",
                "strategy":"put_debit_spread",
                "entry_date":"2026-06-28",
                "exit_date":"2026-06-28",
                "max_loss":100.0
            }]
        });
        let broker = RobinhoodBrokerAdapter::default();

        let decision = portfolio_canary_run_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &test_canary_risk(),
            &broker,
            false,
            false,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "shadow_artifact_blocked");
        assert!(decision.selected_action.is_none());
    }

    #[test]
    fn portfolio_canary_runner_blocks_live_order_when_artifact_is_too_old() {
        let today = Utc::now().date_naive();
        let today_s = today.to_string();
        let mut artifact = test_canary_artifact(serde_json::json!([{
            "status":"entry_candidate",
            "symbol":"TSLA",
            "strategy":"put_debit_spread",
            "entry_date":today_s,
            "exit_date":today_s,
            "expiration":"2026-07-02",
            "short_strike":350.0,
            "long_strike":355.0,
            "entry_credit":-3.35,
            "max_loss":335.0
        }]));
        artifact["exported_at"] = serde_json::json!("2026-06-28T00:00:00Z");
        let broker = RobinhoodBrokerAdapter {
            capabilities: BrokerCapabilities {
                single_leg_options: true,
                multi_leg_options: true,
                stock_option_combos: false,
                cash_secured_puts: false,
                covered_calls: false,
            },
            live_orders_enabled: true,
        };

        let decision = portfolio_canary_run_decision(
            &artifact,
            today,
            &test_canary_risk(),
            &broker,
            false,
            true,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "live_order_blocked");
        assert!(decision.reason.contains("artifact age"));
    }

    #[test]
    fn portfolio_canary_runner_blocks_manual_review_flag_from_auto_place() {
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"entry_candidate",
            "symbol":"TSLA",
            "strategy":"put_debit_spread",
            "entry_date":"2026-06-28",
            "exit_date":"2026-06-28",
            "expiration":"2026-07-02",
            "short_strike":350.0,
            "long_strike":355.0,
            "entry_credit":-3.35,
            "max_loss":335.0
        }]));
        let broker = RobinhoodBrokerAdapter {
            capabilities: BrokerCapabilities {
                single_leg_options: true,
                multi_leg_options: true,
                stock_option_combos: false,
                cash_secured_puts: false,
                covered_calls: false,
            },
            live_orders_enabled: true,
        };

        let decision = portfolio_canary_run_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &test_canary_risk(),
            &broker,
            true,
            true,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "live_order_blocked");
        assert!(decision.reason.contains("MCP review bridge"));
    }

    #[test]
    fn portfolio_canary_runner_monitors_open_candidate_without_catchup_order() {
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"open_candidate",
            "symbol":"ORCL",
            "strategy":"call_debit_spread",
            "entry_date":"2026-06-26",
            "exit_date":"2026-06-30",
            "max_loss":450.0
        }]));
        let broker = RobinhoodBrokerAdapter {
            capabilities: BrokerCapabilities {
                single_leg_options: true,
                multi_leg_options: true,
                stock_option_combos: false,
                cash_secured_puts: false,
                covered_calls: false,
            },
            live_orders_enabled: false,
        };

        let decision = portfolio_canary_run_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &test_canary_risk(),
            &broker,
            true,
            false,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "shadow_open_candidate_monitor_only");
        assert_eq!(
            decision
                .selected_action
                .as_ref()
                .map(|action| action.strategy.as_str()),
            Some("call_debit_spread")
        );
    }

    #[test]
    fn portfolio_canary_runner_reaches_manual_approval_for_entry_after_review() {
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"entry_candidate",
            "symbol":"ORCL",
            "strategy":"call_debit_spread",
            "entry_date":"2026-06-28",
            "exit_date":"2026-06-28",
            "expiration":"2026-07-02",
            "short_strike":225.0,
            "long_strike":220.0,
            "entry_credit":-4.50,
            "max_loss":450.0
        }]));
        let broker = RobinhoodBrokerAdapter {
            capabilities: BrokerCapabilities {
                single_leg_options: true,
                multi_leg_options: true,
                stock_option_combos: false,
                cash_secured_puts: false,
                covered_calls: false,
            },
            live_orders_enabled: false,
        };

        let decision = portfolio_canary_run_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &test_canary_risk(),
            &broker,
            true,
            false,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "ready_for_manual_approval");
    }

    #[test]
    fn robinhood_mcp_order_arguments_builds_wheel_review_payload() {
        let action = CanaryActionSummary {
            status: "entry_candidate".to_owned(),
            symbol: "CRWV".to_owned(),
            strategy: "wheel".to_owned(),
            entry_date: Some("2026-06-28".to_owned()),
            exit_date: Some("2026-06-28".to_owned()),
            expiration: Some("2026-07-10".to_owned()),
            short_put: Some(80.0),
            short_strike: Some(80.0),
            long_strike: None,
            width: None,
            entry_credit: Some(1.12),
            max_loss: Some(7888.0),
            reserve: Some(8000.0),
            reserve_basis: Some("max_loss_plus_entry_credit_x100".to_owned()),
            pnl: Some(112.0),
        };

        let request = robinhood_mcp_option_order_request("review_option_order", &action).unwrap();

        assert_eq!(request.server, "robinhood-trading");
        assert_eq!(request.tool, "review_option_order");
        assert_eq!(request.arguments["order_effect"], "credit");
        assert_eq!(request.arguments["limit_price"], 1.12);
        assert_eq!(request.arguments["legs"][0]["side"], "sell");
        assert_eq!(request.arguments["legs"][0]["option_type"], "put");
        assert_eq!(request.arguments["legs"][0]["strike"], 80.0);
        assert_eq!(request.arguments["source"]["reserve"], 8000.0);
    }

    #[test]
    fn robinhood_mcp_order_arguments_builds_call_debit_spread_payload() {
        let action = CanaryActionSummary {
            status: "entry_candidate".to_owned(),
            symbol: "ORCL".to_owned(),
            strategy: "call_debit_spread".to_owned(),
            entry_date: Some("2026-06-28".to_owned()),
            exit_date: Some("2026-06-28".to_owned()),
            expiration: Some("2026-07-02".to_owned()),
            short_put: None,
            short_strike: Some(225.0),
            long_strike: Some(220.0),
            width: Some(5.0),
            entry_credit: Some(-4.50),
            max_loss: Some(450.0),
            reserve: Some(450.0),
            reserve_basis: Some("max_loss".to_owned()),
            pnl: None,
        };

        let request = robinhood_mcp_option_order_request("review_option_order", &action).unwrap();

        assert_eq!(request.arguments["order_effect"], "debit");
        assert_eq!(request.arguments["limit_price"], 4.50);
        assert_eq!(request.arguments["legs"][0]["side"], "buy");
        assert_eq!(request.arguments["legs"][0]["option_type"], "call");
        assert_eq!(request.arguments["legs"][0]["strike"], 220.0);
        assert_eq!(request.arguments["legs"][1]["side"], "sell");
        assert_eq!(request.arguments["legs"][1]["option_type"], "call");
        assert_eq!(request.arguments["legs"][1]["strike"], 225.0);
    }

    #[test]
    fn robinhood_mcp_order_arguments_rejects_open_candidate() {
        let action = CanaryActionSummary {
            status: "open_candidate".to_owned(),
            symbol: "TSLA".to_owned(),
            strategy: "put_debit_spread".to_owned(),
            entry_date: Some("2026-06-27".to_owned()),
            exit_date: Some("2026-06-30".to_owned()),
            expiration: Some("2026-07-02".to_owned()),
            short_put: Some(350.0),
            short_strike: Some(350.0),
            long_strike: Some(355.0),
            width: Some(5.0),
            entry_credit: Some(-3.35),
            max_loss: Some(335.0),
            reserve: Some(335.0),
            reserve_basis: Some("max_loss".to_owned()),
            pnl: None,
        };

        let err = robinhood_mcp_option_order_request("review_option_order", &action).unwrap_err();

        assert!(format!("{err:#}").contains("entry_candidate"));
    }

    #[test]
    fn robinhood_mcp_bridge_review_success_unblocks_manual_approval() {
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"entry_candidate",
            "symbol":"CRWV",
            "strategy":"wheel",
            "entry_date":"2026-06-28",
            "exit_date":"2026-06-28",
            "expiration":"2026-07-10",
            "short_strike":80.0,
            "entry_credit":1.12,
            "max_loss":7888.0
        }]));
        let broker = RobinhoodBrokerAdapter {
            capabilities: BrokerCapabilities {
                single_leg_options: true,
                multi_leg_options: false,
                stock_option_combos: false,
                cash_secured_puts: true,
                covered_calls: true,
            },
            live_orders_enabled: false,
        };
        let mut decision = portfolio_canary_run_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &test_canary_risk(),
            &broker,
            false,
            false,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        let action = decision.selected_action.clone().unwrap();
        let expected_key = robinhood_mcp_order_key(
            &robinhood_mcp_option_order_request("review_option_order", &action).unwrap(),
        );
        let response = serde_json::json!({
            "ok": true,
            "tool": "review_option_order",
            "raw": {"preview": "ok", "order_key": expected_key}
        })
        .to_string();
        apply_robinhood_mcp_bridge(
            &mut decision,
            Some(&format!("cat >/dev/null; printf '%s\\n' '{}'", response)),
            None,
        )
        .unwrap();

        assert_eq!(decision.status, "ready_for_manual_approval");
        assert!(decision.broker_review_ok);
        assert_eq!(
            decision
                .mcp_review
                .as_ref()
                .map(|review| review.tool.as_str()),
            Some("review_option_order")
        );
    }

    #[test]
    fn robinhood_mcp_bridge_keeps_wheel_review_only_even_when_place_requested() {
        let today = Utc::now().date_naive();
        let today_s = today.to_string();
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"entry_candidate",
            "symbol":"CRWV",
            "strategy":"wheel",
            "entry_date":today_s,
            "exit_date":today_s,
            "expiration":"2026-07-10",
            "short_strike":80.0,
            "entry_credit":1.12,
            "max_loss":7888.0
        }]));
        let broker = RobinhoodBrokerAdapter {
            capabilities: BrokerCapabilities {
                single_leg_options: true,
                multi_leg_options: false,
                stock_option_combos: false,
                cash_secured_puts: true,
                covered_calls: true,
            },
            live_orders_enabled: true,
        };
        let mut decision = portfolio_canary_run_decision(
            &artifact,
            today,
            &test_canary_risk(),
            &broker,
            false,
            true,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );
        let action = decision.selected_action.clone().unwrap();
        let expected_key = robinhood_mcp_order_key(
            &robinhood_mcp_option_order_request("review_option_order", &action).unwrap(),
        );
        let response = serde_json::json!({
            "ok": true,
            "tool": "review_option_order",
            "raw": {"order_key": expected_key}
        })
        .to_string();

        apply_robinhood_mcp_bridge(
            &mut decision,
            Some(&format!("cat >/dev/null; printf '%s\\n' '{}'", response)),
            None,
        )
        .unwrap();

        assert_eq!(decision.status, "ready_for_manual_approval");
        assert!(decision.mcp_place.is_none());
        assert!(decision.reason.contains("wheel placement is blocked"));
    }

    #[test]
    fn robinhood_mcp_bridge_blocks_duplicate_live_submission() {
        let ledger = unique_main_test_path("canary-order-ledger-duplicate.json");
        let today = Utc::now().date_naive();
        let today_s = today.to_string();
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"entry_candidate",
            "symbol":"ORCL",
            "strategy":"call_debit_spread",
            "entry_date":today_s,
            "exit_date":today_s,
            "expiration":"2026-07-02",
            "short_strike":225.0,
            "long_strike":220.0,
            "entry_credit":-4.50,
            "max_loss":450.0
        }]));
        let broker = RobinhoodBrokerAdapter {
            capabilities: BrokerCapabilities {
                single_leg_options: true,
                multi_leg_options: true,
                stock_option_combos: false,
                cash_secured_puts: false,
                covered_calls: false,
            },
            live_orders_enabled: true,
        };

        let mut first = portfolio_canary_run_decision(
            &artifact,
            today,
            &test_canary_risk(),
            &broker,
            false,
            true,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );
        let first_action = first.selected_action.clone().unwrap();
        let expected_key = robinhood_mcp_order_key(
            &robinhood_mcp_option_order_request("review_option_order", &first_action).unwrap(),
        );
        let review_response = serde_json::json!({
            "ok": true,
            "tool": "review_option_order",
            "raw": {"order_key": expected_key}
        })
        .to_string();
        let place_response = serde_json::json!({
            "ok": true,
            "tool": "place_option_order",
            "raw": {"order_id": "abc"}
        })
        .to_string();
        let command = format!(
            "body=$(cat); case \"$body\" in *review_option_order*) printf '%s\\n' '{}' ;; *place_option_order*) printf '%s\\n' '{}' ;; esac",
            review_response, place_response
        );
        apply_robinhood_mcp_bridge(&mut first, Some(command.as_str()), Some(&ledger)).unwrap();

        let mut second = portfolio_canary_run_decision(
            &artifact,
            today,
            &test_canary_risk(),
            &broker,
            false,
            true,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );
        apply_robinhood_mcp_bridge(&mut second, Some(command.as_str()), Some(&ledger)).unwrap();

        assert_eq!(first.status, "live_order_submitted");
        assert_eq!(second.status, "live_order_already_submitted");
        fs::remove_file(ledger).unwrap();
    }

    #[test]
    fn portfolio_canary_runner_allows_wheel_after_risk_and_broker_gates() {
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"entry_candidate",
            "symbol":"CRWV",
            "strategy":"wheel",
            "entry_date":"2026-06-28",
            "exit_date":"2026-06-28",
            "short_put":95.0,
            "max_loss":9265.0
        }]));
        let broker = RobinhoodBrokerAdapter {
            capabilities: BrokerCapabilities {
                single_leg_options: true,
                multi_leg_options: false,
                stock_option_combos: false,
                cash_secured_puts: true,
                covered_calls: true,
            },
            live_orders_enabled: false,
        };

        let decision = portfolio_canary_run_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &test_canary_risk(),
            &broker,
            true,
            false,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "ready_for_manual_approval");
        assert_eq!(
            decision
                .selected_action
                .as_ref()
                .and_then(|action| action.reserve),
            Some(9_500.0)
        );
    }

    #[test]
    fn canary_worker_health_reports_shadow_without_action() {
        let path = unique_main_test_path("canary-worker-no-action.json");
        fs::write(
            &path,
            r#"{
                "status":"canary_only",
                "decision":{"canary_ready":true,"research_gate":"research_pass"},
                "latest_actions":[{"status":"recent_closed","symbol":"TSLA","strategy":"put_debit_spread","entry_date":"2026-06-25","exit_date":"2026-06-26","max_loss":100.0}]
            }"#,
        )
        .unwrap();
        let args = CanaryWorkerArgs {
            candidate: path.clone(),
            as_of: Some(NaiveDate::from_ymd_opt(2026, 6, 28).unwrap()),
            risk: test_canary_risk(),
            broker: RobinhoodBrokerAdapter::default(),
            robinhood_mcp_command: None,
            order_ledger: unique_main_test_path("canary-order-ledger.json"),
            max_order_age_seconds: DEFAULT_MAX_ORDER_AGE_SECONDS,
            broker_review_ok: false,
            place_live_order: false,
            poll_seconds: 60,
            once: true,
            health_output: None,
            json: true,
        };

        let health = canary_worker_health(&args);

        assert_eq!(health.status, "shadow");
        assert_eq!(
            health
                .decision
                .as_ref()
                .map(|decision| decision.status.as_str()),
            Some("shadow_no_action")
        );
        fs::remove_file(path).unwrap();
    }

    fn test_canary_risk() -> CanaryRiskConfig {
        CanaryRiskConfig {
            account_cash: 45_000.0,
            debit_max_loss: 1_000.0,
            wheel_reserve_cap: 35_000.0,
            free_cash_buffer: 11_250.0,
            max_wheel_positions_per_symbol: 1,
        }
    }

    fn test_canary_artifact(latest_actions: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "status": "canary_only",
            "exported_at": Utc::now(),
            "decision": {
                "canary_ready": true,
                "research_gate": "research_pass"
            },
            "latest_actions": latest_actions
        })
    }

    #[test]
    fn automatic_expansion_uses_plateau_research_json_only_when_ready() {
        assert_eq!(automatic_expansion_plateau_run("nvda-test", false), None);
        assert_eq!(
            automatic_expansion_plateau_run("nvda-test", true),
            Some(
                PathBuf::from("runs")
                    .join("nvda-test")
                    .join("research.json")
            )
        );
    }

    #[test]
    fn universe_results_rank_by_oos_evidence_not_seed_order() {
        let mut results = vec![
            universe_summary_row(TestUniverseRow {
                symbol: "AAPL",
                seed_rank: Some(1),
                deployment_status: "blocked",
                fixed_profile_oos_passes: 0,
                walk_forward_score: -1.0,
                holdout_score: -2.0,
                robust_score: 0.2,
                rows_loaded: 20_000,
            }),
            universe_summary_row(TestUniverseRow {
                symbol: "TSLA",
                seed_rank: Some(2),
                deployment_status: "blocked",
                fixed_profile_oos_passes: 1,
                walk_forward_score: -10.0,
                holdout_score: -10.0,
                robust_score: -1.0,
                rows_loaded: 10_000,
            }),
        ];

        rank_universe_results(&mut results);

        assert_eq!(results[0].symbol, "TSLA");
        assert_eq!(results[0].suitability_rank, 1);
        assert_eq!(results[1].symbol, "AAPL");
        assert_eq!(results[1].suitability_rank, 2);
    }

    #[test]
    fn universe_results_rank_by_conservative_execution_oos_score() {
        let mut results = vec![
            universe_summary_row(TestUniverseRow {
                symbol: "AAPL",
                seed_rank: Some(1),
                deployment_status: "blocked",
                fixed_profile_oos_passes: 0,
                walk_forward_score: 10.0,
                holdout_score: -5.0,
                robust_score: 0.4,
                rows_loaded: 20_000,
            }),
            universe_summary_row(TestUniverseRow {
                symbol: "TSLA",
                seed_rank: Some(2),
                deployment_status: "blocked",
                fixed_profile_oos_passes: 0,
                walk_forward_score: 1.0,
                holdout_score: 1.0,
                robust_score: 0.1,
                rows_loaded: 10_000,
            }),
        ];

        rank_universe_results(&mut results);

        assert_eq!(results[0].symbol, "TSLA");
        assert_eq!(results[0].execution_oos_score, 1.0);
        assert_eq!(results[1].symbol, "AAPL");
        assert_eq!(results[1].execution_oos_score, -5.0);
    }

    #[test]
    fn universe_report_surfaces_fixed_profile_and_strategy_statuses() {
        let mut results = vec![universe_summary_row(TestUniverseRow {
            symbol: "TSLA",
            seed_rank: Some(2),
            deployment_status: "blocked",
            fixed_profile_oos_passes: 1,
            walk_forward_score: 0.4,
            holdout_score: 0.3,
            robust_score: 0.2,
            rows_loaded: 10_000,
        })];
        rank_universe_results(&mut results);
        let summary = UniverseResearchSummary {
            run_id: "universe-test".to_owned(),
            run_status: "running".to_owned(),
            profile_family: "swing".to_owned(),
            from: NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
            to: NaiveDate::from_ymd_opt(2024, 12, 31).unwrap(),
            symbols: vec!["TSLA".to_owned()],
            symbols_requested: 1,
            symbols_completed: 1,
            plateau_run: Some("runs/nvda/research.json".to_owned()),
            max_expirations: Some(24),
            fetch_concurrency: 8,
            force_refresh: false,
            cache_only: false,
            strategy: "put_credit_spread".to_owned(),
            selection_basis: UNIVERSE_SELECTION_BASIS.to_owned(),
            research_method: UNIVERSE_RESEARCH_METHOD.to_owned(),
            detector_score_basis: UNIVERSE_DETECTOR_SCORE_BASIS.to_owned(),
            seed_score_basis: UNIVERSE_SEED_SCORE_BASIS.to_owned(),
            execution_score_basis: UNIVERSE_EXECUTION_SCORE_BASIS.to_owned(),
            expansion_seed: Vec::new(),
            results,
        };

        let markdown = universe_markdown(&summary);

        assert!(markdown.contains("Strategy: `put_credit_spread`"));
        assert!(markdown.contains("Status: `running`"));
        assert!(markdown.contains("Symbols completed: `1/1`"));
        assert!(markdown.contains("Max expirations per symbol: `24`"));
        assert!(markdown.contains("Fetch concurrency: `8`"));
        assert!(markdown.contains("same Rust put-credit-spread profile grid"));
        assert!(markdown.contains("Seed score basis"));
        assert!(markdown.contains("## Research Protocol"));
        assert!(markdown.contains("Seed Score"));
        assert!(markdown.contains("Detector search: each symbol gets its own"));
        assert!(markdown.contains("Execution strategy search: take-profit"));
        assert!(markdown.contains("## Symbol Suitability Ranking"));
        assert!(markdown.contains("Detector Status"));
        assert!(markdown.contains("Detector Score"));
        assert!(markdown.contains("Execution OOS Score"));
        assert!(markdown.contains("Best Fixed Detector"));
        assert!(markdown.contains("put_spread_detector_test"));
        assert!(markdown.contains("put_spread_execution_test"));
        assert!(markdown.contains("## Strategy Details"));
        assert!(markdown.contains("quote width <= max(10% mid, $0.10)"));
        assert!(markdown.contains("filters: short_iv<=0.450"));
        assert!(markdown.contains("selector farther_otm_then_credit"));
        assert!(markdown.contains("max positions 1; entry spacing 1d"));
    }

    #[test]
    fn universe_results_keep_symbol_errors_and_rank_them_last() {
        let expansion_seed = expansion_seed_for_symbols(
            &["TSLA".to_owned(), "AAPL".to_owned()],
            ResearchProfileFamily::Swing,
        );
        let mut results = vec![
            universe_symbol_error_summary(
                "TSLA",
                &expansion_seed,
                &anyhow::anyhow!("ThetaData 403\nsubscription required"),
            ),
            universe_summary_row(TestUniverseRow {
                symbol: "AAPL",
                seed_rank: Some(2),
                deployment_status: "blocked",
                fixed_profile_oos_passes: 0,
                walk_forward_score: -1.0,
                holdout_score: -1.0,
                robust_score: 0.1,
                rows_loaded: 10_000,
            }),
        ];

        rank_universe_results(&mut results);

        assert_eq!(results[0].symbol, "AAPL");
        assert_eq!(results[0].research_status, "ok");
        assert_eq!(results[1].symbol, "TSLA");
        assert_eq!(results[1].research_status, "error");
        assert_eq!(
            results[1].error_message.as_deref(),
            Some("ThetaData 403 | subscription required")
        );
    }

    #[test]
    fn universe_research_outcome_marks_zero_rows_as_no_data() {
        assert_eq!(universe_research_outcome(0, 0).0, "no_data");
        assert!(
            universe_research_outcome(3, 0)
                .1
                .unwrap()
                .contains("zero usable EOD rows")
        );
        assert_eq!(universe_research_outcome(3, 100).0, "ok");
    }

    #[test]
    fn universe_results_rank_no_data_behind_usable_research() {
        let mut no_data = universe_summary_row(TestUniverseRow {
            symbol: "TSLA",
            seed_rank: Some(1),
            deployment_status: "blocked",
            fixed_profile_oos_passes: 10,
            walk_forward_score: 10.0,
            holdout_score: 10.0,
            robust_score: 10.0,
            rows_loaded: 0,
        });
        no_data.research_status = "no_data".to_owned();
        no_data.error_message = Some("ThetaData loaded zero expirations".to_owned());
        let mut results = vec![
            no_data,
            universe_summary_row(TestUniverseRow {
                symbol: "AAPL",
                seed_rank: Some(2),
                deployment_status: "blocked",
                fixed_profile_oos_passes: 0,
                walk_forward_score: -1.0,
                holdout_score: -1.0,
                robust_score: 0.1,
                rows_loaded: 10_000,
            }),
        ];

        rank_universe_results(&mut results);

        assert_eq!(results[0].symbol, "AAPL");
        assert_eq!(results[0].research_status, "ok");
        assert_eq!(results[1].symbol, "TSLA");
        assert_eq!(results[1].research_status, "no_data");
    }

    struct TestUniverseRow {
        symbol: &'static str,
        seed_rank: Option<usize>,
        deployment_status: &'static str,
        fixed_profile_oos_passes: usize,
        walk_forward_score: f64,
        holdout_score: f64,
        robust_score: f64,
        rows_loaded: usize,
    }

    fn universe_summary_row(input: TestUniverseRow) -> UniverseSymbolSummary {
        UniverseSymbolSummary {
            suitability_rank: 0,
            symbol: input.symbol.to_owned(),
            seed_rank: input.seed_rank,
            seed_suitability_score: Some(1),
            seed_role: Some("test".to_owned()),
            seed_rationale: Some("test".to_owned()),
            research_status: "ok".to_owned(),
            error_message: None,
            report_dir: format!("runs/{}", input.symbol),
            deployment_status: input.deployment_status.to_owned(),
            plateau_status: "plateau_expand_universe".to_owned(),
            detector_status: "robust".to_owned(),
            execution_strategy_status: "oos_blocked".to_owned(),
            expansion_ready: true,
            expirations_loaded: 5,
            rows_loaded: input.rows_loaded,
            profiles_evaluated: 102,
            best_profile: "best_profile".to_owned(),
            best_detector: "put_spread_detector_test".to_owned(),
            best_execution: "put_spread_execution_test".to_owned(),
            best_detector_details: Some(test_detector_strategy()),
            best_execution_details: Some(test_execution_strategy()),
            detector_score: input.robust_score,
            execution_oos_score: input.walk_forward_score.min(input.holdout_score),
            trades: 10,
            total_pnl: 100.0,
            score: 0.1,
            robust_score: input.robust_score,
            walk_forward_trades: 4,
            walk_forward_pnl: 40.0,
            walk_forward_score: input.walk_forward_score,
            holdout_trades: 3,
            holdout_pnl: 30.0,
            holdout_score: input.holdout_score,
            fixed_profile_oos_passes: input.fixed_profile_oos_passes,
            best_fixed_profile: "best_fixed_profile".to_owned(),
            best_fixed_detector: "put_spread_detector_test".to_owned(),
            best_fixed_execution: "put_spread_execution_test".to_owned(),
            best_fixed_detector_details: Some(test_detector_strategy()),
            best_fixed_execution_details: Some(test_execution_strategy()),
            best_fixed_trades: 4,
            best_fixed_pnl: 40.0,
            best_fixed_score: input.walk_forward_score,
            best_fixed_robust_score: input.robust_score,
            latest_signal_status: Some("research_only".to_owned()),
        }
    }

    fn test_detector_strategy() -> DetectorStrategySummary {
        DetectorStrategySummary {
            name: "put_spread_detector_test".to_owned(),
            min_dte: 30,
            max_dte: 45,
            min_short_delta_abs: 0.20,
            max_short_delta_abs: 0.30,
            min_width: 5.0,
            max_width: 15.0,
            min_credit_width: 0.20,
            max_quote_width_pct_of_mid: 0.10,
            max_quote_width_abs: 0.10,
            min_short_oi: 500,
            min_long_oi: 250,
            filters: vec!["short_iv<=0.450".to_owned()],
        }
    }

    fn test_execution_strategy() -> ExecutionStrategySummary {
        ExecutionStrategySummary {
            name: "put_spread_execution_test".to_owned(),
            candidate_selector: "farther_otm_then_credit".to_owned(),
            entry_fill_model: "short_bid_minus_long_ask".to_owned(),
            exit_fill_model: "short_ask_minus_long_bid".to_owned(),
            take_profit_pct: 0.50,
            stop_loss_multiple: 2.0,
            force_close_dte: 21,
            max_hold_days: None,
            stop_loss_cooldown_days: 10,
            max_concurrent_positions: 1,
            min_entry_spacing_days: 1,
        }
    }

    fn unique_main_test_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "spreadfoundry-main-test-{}-{}-{name}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        ))
    }
}
