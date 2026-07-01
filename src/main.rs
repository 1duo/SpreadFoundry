use anyhow::{Context, Result};
use chrono::{Datelike, NaiveDate, Timelike, Utc};
use clap::{Parser, ValueEnum};
use futures::{StreamExt, stream};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use spreadfoundry::broker::{
    BrokerCapabilities, RobinhoodBrokerAdapter, RobinhoodMcpCommandExecutor,
    RobinhoodMcpToolRequest, RobinhoodMcpToolResponse, TradierClient, TradierConfig,
    TradierMarketClock, TradierMarketClockResponse, TradierOrder, TradierOrderResponse,
    TradierPosition, TradierQuote, TradierQuotesResponse,
};
use spreadfoundry::execution::{
    OptionOrderEffect, OptionOrderIntent, OptionOrderLeg, OptionOrderSide, PositionEffect,
    TimeInForce, cash_secured_put_open_intent, conservative_short_spread_exit_debit_f64,
    credit_spread_close_intent, credit_spread_open_intent, debit_spread_close_intent,
    debit_spread_open_intent,
};
use spreadfoundry::fixture;
use spreadfoundry::live_market::{
    DEFAULT_LIVE_MARKET_INTERVAL_SECONDS, DEFAULT_LIVE_MARKET_MAX_SOURCE_AGE_SECONDS,
    LiveMarketDecision, LiveMarketEngineConfig, LiveMarketProviderHealth, LiveMarketSnapshotRecord,
    build_signal_artifact_live_market_snapshot,
};
use spreadfoundry::live_signal::{
    ApprovedStrategy, LIVE_SIGNAL_SCHEMA_VERSION, LiveExecutionRules, LiveSignalArtifact,
    SignalStatus, TradeSignal,
};
use spreadfoundry::opt::{OptimizationResult, rank_results, score_trades};
use spreadfoundry::report::{read_report_markdown, write_run_report};
use spreadfoundry::research::{
    DEFAULT_PLATEAU_UNIVERSE_SYMBOLS, DEFAULT_PLATEAU_UNIVERSE_SYMBOLS_CSV, DEFAULT_RESEARCH_FROM,
    DEFAULT_WEEKLY_RESEARCH_SYMBOLS, DEFAULT_WEEKLY_RESEARCH_SYMBOLS_CSV, DetectorStrategySummary,
    ExecutionStrategySummary, OptionCacheCoverageReport, OptionCacheCoverageRequest,
    PortfolioWheelReport, PortfolioWheelResearchRequest, ResearchMetrics, ResearchProfile,
    ResearchProfileFamily, ResearchReport, ResearchRequest, WarmOptionCacheCoverageReport,
    WarmOptionCacheCoverageRequest, WarmOptionCacheSide, WeeklySignalGateAuditReport,
    WeeklySignalGateAuditRequest, audit_option_cache_coverage, audit_weekly_signal_gates,
    run_portfolio_selector_research, run_portfolio_selector_research_for_profile,
    run_portfolio_wheel_research, run_symbol_research, warm_option_cache_coverage,
};
use spreadfoundry::research_store::{
    ResearchStore, ResearchStoreHealth, ResearchStoreImportReport, ResearchStorePerfReport,
    default_research_store_path, import_research_store, research_store_perf_check,
    set_research_store_cache_sync_enabled_override, set_research_store_path_override,
};
use spreadfoundry::sim::{ExitRules, SpreadExitQuote, choose_exit};
use spreadfoundry::strategy::{CandidateFilters, generate_put_spread_candidates};
use spreadfoundry::theta::{ThetaClient, ThetaUniverseRequest};
use spreadfoundry::types::{OptionKey, OptionRight};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::mpsc;
use std::time::{Duration as StdDuration, Instant as StdInstant};
use wait_timeout::ChildExt;

const DEFAULT_MAX_ORDER_AGE_SECONDS: u64 = 30 * 60;
const DEFAULT_MAX_QUOTE_AGE_SECONDS: i64 = 30;
const DEFAULT_WARM_OPTION_CACHE_WINDOW_TIMEOUT_SECONDS: u64 = 300;
static MAIN_RUN_ID_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const UNIVERSE_SELECTION_BASIS: &str = "Plateau expansion uses eight non-NVDA single stocks chosen for liquid weekly option chains, usable put-spread premium, and enough business-model diversity to test whether the detector generalizes beyond NVDA.";
const UNIVERSE_RESEARCH_METHOD: &str = "Each symbol independently runs the same Rust put-credit-spread profile grid. Detector rules and execution rules are reported separately; no NVDA profile is copied into another symbol without out-of-sample proof.";
const UNIVERSE_SEED_SCORE_BASIS: &str = "Static pre-research seed score: 3x option liquidity + 2x premium + 2x spread quality + price-fit + diversification + event-risk discipline. Used only to choose the default live_signal symbols; actual suitability ranking is research-evidence driven.";
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
    MonitorLive {
        #[arg(long)]
        symbol: String,
        #[arg(long, value_enum)]
        strategy: StrategyArg,
    },
    Report {
        #[arg(long)]
        run: PathBuf,
    },
    ExportLiveSignal {
        #[arg(long)]
        run: PathBuf,
        #[arg(long, default_value = "configs/approved_strategy.json")]
        approved_strategy: PathBuf,
        #[arg(long, default_value = "var/live_signal.json")]
        output: PathBuf,
        #[arg(long)]
        as_of: Option<NaiveDate>,
    },
    LiveSignalStatus {
        #[arg(long, default_value = "var/live_signal.json")]
        live_signal: PathBuf,
        #[arg(long)]
        as_of: Option<NaiveDate>,
        #[arg(long, default_value_t = false)]
        require_signal: bool,
    },
    MarketSessionStatus {
        #[arg(long, default_value_t = false)]
        require_open: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    RefreshLiveSignal {
        #[arg(long, default_value = "configs/approved_strategy.json")]
        approved_strategy: PathBuf,
        #[arg(long, default_value = "var/live_signal.json")]
        output: PathBuf,
        #[arg(long, default_value = "var/live_signal_refresh_last.json")]
        state_file: PathBuf,
        #[arg(long, default_value = DEFAULT_RESEARCH_FROM)]
        from: NaiveDate,
        #[arg(long)]
        to: Option<NaiveDate>,
        #[arg(long)]
        max_expirations: Option<usize>,
        #[arg(long, default_value_t = 4)]
        fetch_concurrency: usize,
        #[arg(long, default_value_t = 2)]
        symbol_concurrency: usize,
        #[arg(long, default_value_t = false)]
        force_refresh: bool,
        #[arg(long, default_value_t = false)]
        cache_only: bool,
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        market_window_only: bool,
        #[arg(long, default_value_t = 900)]
        timeout_seconds: u64,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    LiveMarketEngine {
        #[arg(long, default_value = "configs/approved_strategy.json")]
        approved_strategy: PathBuf,
        #[arg(long, default_value = "var/live_signal_refresh_source.json")]
        source_live_signal: PathBuf,
        #[arg(long, default_value = "var/live_signal.json")]
        output: PathBuf,
        #[arg(long, default_value = "var/live_market_engine_health.json")]
        state_file: PathBuf,
        #[arg(long, default_value = "data/spreadfoundry.duckdb")]
        store: PathBuf,
        #[arg(long)]
        as_of: Option<NaiveDate>,
        #[arg(long, default_value_t = DEFAULT_LIVE_MARKET_INTERVAL_SECONDS)]
        interval_seconds: u64,
        #[arg(long, default_value_t = DEFAULT_LIVE_MARKET_MAX_SOURCE_AGE_SECONDS)]
        max_source_age_seconds: u64,
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        market_window_only: bool,
        #[arg(long, default_value_t = false)]
        once: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    ExecutionReadiness {
        #[arg(long, default_value = "var/live_signal.json")]
        live_signal: PathBuf,
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
        #[arg(long, value_enum, default_value = "tradier")]
        broker: BrokerKind,
        #[arg(long, default_value_t = false)]
        broker_multi_leg_options: bool,
        #[arg(long, default_value_t = false)]
        broker_cash_secured_puts: bool,
        #[arg(long, default_value_t = false)]
        broker_covered_calls: bool,
        #[arg(long)]
        robinhood_mcp_command: Option<String>,
        #[arg(long, default_value_t = DEFAULT_MAX_ORDER_AGE_SECONDS)]
        max_order_age_seconds: u64,
        #[arg(long, default_value_t = false)]
        allow_blocked: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    RunExecutionDecision {
        #[arg(long, default_value = "var/live_signal.json")]
        live_signal: PathBuf,
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
        #[arg(long, value_enum, default_value = "monitor")]
        mode: ExecutionMode,
        #[arg(long, value_enum, default_value = "tradier")]
        broker: BrokerKind,
        #[arg(long, default_value_t = false)]
        broker_multi_leg_options: bool,
        #[arg(long, default_value_t = false)]
        broker_cash_secured_puts: bool,
        #[arg(long, default_value_t = false)]
        broker_covered_calls: bool,
        #[arg(long)]
        robinhood_mcp_command: Option<String>,
        #[arg(long, default_value = "var/execution_order_ledger.json")]
        order_ledger: PathBuf,
        #[arg(long, default_value_t = DEFAULT_MAX_ORDER_AGE_SECONDS)]
        max_order_age_seconds: u64,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    ExecutionWorker {
        #[arg(long, default_value = "var/live_signal.json")]
        live_signal: PathBuf,
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
        #[arg(long, value_enum, default_value = "monitor")]
        mode: ExecutionMode,
        #[arg(long, value_enum, default_value = "tradier")]
        broker: BrokerKind,
        #[arg(long, default_value_t = false)]
        broker_multi_leg_options: bool,
        #[arg(long, default_value_t = false)]
        broker_cash_secured_puts: bool,
        #[arg(long, default_value_t = false)]
        broker_covered_calls: bool,
        #[arg(long)]
        robinhood_mcp_command: Option<String>,
        #[arg(long, default_value = "var/execution_order_ledger.json")]
        order_ledger: PathBuf,
        #[arg(long)]
        notify_command: Option<String>,
        #[arg(long, default_value = "var/execution_notify_ledger.json")]
        notify_ledger: PathBuf,
        #[arg(long, default_value_t = DEFAULT_MAX_ORDER_AGE_SECONDS)]
        max_order_age_seconds: u64,
        #[arg(long, default_value_t = 60)]
        poll_seconds: u64,
        #[arg(long, default_value_t = false)]
        once: bool,
        #[arg(long)]
        health_output: Option<PathBuf>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    ExecutionWorkerEnv {
        #[arg(long, default_value_t = false)]
        once: bool,
    },
    ExecutionWorkerSnapshot {
        #[arg(long, default_value = "var/execution_worker_health.json")]
        health_output: PathBuf,
        #[arg(long, default_value = "var/execution_worker.pid")]
        pid_file: PathBuf,
        #[arg(long, default_value_t = 180)]
        max_age_seconds: u64,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    ResearchStoreHealth {
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    ResearchStoreImport {
        #[arg(long, default_value = "data/raw/theta")]
        raw_root: PathBuf,
        #[arg(long, value_delimiter = ',')]
        symbols: Vec<String>,
        #[arg(long)]
        max_files_per_symbol: Option<usize>,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    ResearchStorePerfCheck {
        #[arg(long, default_value = "data/raw/theta")]
        raw_root: PathBuf,
        #[arg(long, value_delimiter = ',')]
        symbols: Vec<String>,
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
        #[arg(long)]
        research_store: Option<PathBuf>,
        #[arg(long, default_value_t = false)]
        skip_cache_sync: bool,
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
        #[arg(long, default_value_t = false)]
        recent_first: bool,
        #[arg(long, value_enum, default_value = "put-and-call")]
        option_side: WarmOptionSideArg,
        #[arg(long, default_value_t = 2)]
        fetch_concurrency: usize,
        #[arg(long, default_value_t = DEFAULT_WARM_OPTION_CACHE_WINDOW_TIMEOUT_SECONDS)]
        window_timeout_seconds: u64,
        #[arg(long, default_value_t = false)]
        force_refresh: bool,
        #[arg(long, default_value_t = false)]
        progress: bool,
        #[arg(long)]
        research_store: Option<PathBuf>,
        #[arg(long, default_value_t = false)]
        skip_cache_sync: bool,
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
        #[arg(long)]
        research_store: Option<PathBuf>,
        #[arg(long, default_value_t = false)]
        skip_cache_sync: bool,
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
        #[arg(long)]
        research_store: Option<PathBuf>,
        #[arg(long, default_value_t = false)]
        skip_cache_sync: bool,
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
        #[arg(long)]
        research_store: Option<PathBuf>,
        #[arg(long, default_value_t = false)]
        skip_cache_sync: bool,
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
        #[arg(long)]
        research_store: Option<PathBuf>,
        #[arg(long, default_value_t = false)]
        skip_cache_sync: bool,
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
        #[arg(long)]
        approved_strategy: Option<PathBuf>,
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
        #[arg(long)]
        promotion_baseline_run: Option<PathBuf>,
        #[arg(long, default_value_t = 5)]
        promotion_min_new_symbol_trades: usize,
        #[arg(long)]
        research_store: Option<PathBuf>,
        #[arg(long, default_value_t = false)]
        skip_cache_sync: bool,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum ExecutionMode {
    Monitor,
    Review,
    Live,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum BrokerKind {
    Robinhood,
    Tradier,
}

#[derive(Clone, Debug)]
struct ExecutionBrokerAdapter {
    kind: BrokerKind,
    capabilities: BrokerCapabilities,
    live_orders_enabled: bool,
}

trait ExecutionBrokerView {
    fn kind(&self) -> BrokerKind;
    fn capabilities(&self) -> &BrokerCapabilities;
    fn live_orders_enabled(&self) -> bool;

    fn assert_credit_spread_live_supported(&self) -> Result<()> {
        if !self.capabilities().multi_leg_options {
            let broker = broker_label(self.kind());
            anyhow::bail!(
                "credit spread live execution is disabled: {broker} adapter has no proven atomic multi-leg support"
            );
        }
        Ok(())
    }

    fn assert_debit_spread_live_supported(&self) -> Result<()> {
        if !self.capabilities().multi_leg_options {
            let broker = broker_label(self.kind());
            anyhow::bail!(
                "debit spread live execution is disabled: {broker} adapter has no proven atomic multi-leg support"
            );
        }
        Ok(())
    }

    fn assert_wheel_live_supported(&self) -> Result<()> {
        if !self.capabilities().cash_secured_puts {
            let broker = broker_label(self.kind());
            anyhow::bail!(
                "wheel live execution is disabled: {broker} adapter has no proven cash-secured put sell-to-open support"
            );
        }
        if !self.capabilities().covered_calls {
            let broker = broker_label(self.kind());
            anyhow::bail!(
                "wheel live execution is disabled: {broker} adapter has no proven covered-call lifecycle support"
            );
        }
        Ok(())
    }
}

impl ExecutionBrokerView for ExecutionBrokerAdapter {
    fn kind(&self) -> BrokerKind {
        self.kind
    }

    fn capabilities(&self) -> &BrokerCapabilities {
        &self.capabilities
    }

    fn live_orders_enabled(&self) -> bool {
        self.live_orders_enabled
    }
}

impl ExecutionBrokerView for RobinhoodBrokerAdapter {
    fn kind(&self) -> BrokerKind {
        BrokerKind::Robinhood
    }

    fn capabilities(&self) -> &BrokerCapabilities {
        &self.capabilities
    }

    fn live_orders_enabled(&self) -> bool {
        self.live_orders_enabled
    }
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum WarmOptionSideArg {
    Put,
    Call,
    PutAndCall,
}

impl From<WarmOptionSideArg> for WarmOptionCacheSide {
    fn from(value: WarmOptionSideArg) -> Self {
        match value {
            WarmOptionSideArg::Put => Self::Put,
            WarmOptionSideArg::Call => Self::Call,
            WarmOptionSideArg::PutAndCall => Self::PutAndCall,
        }
    }
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

fn configure_research_store_for_command(
    research_store: Option<PathBuf>,
    skip_cache_sync: bool,
) -> Result<()> {
    if let Some(path) = research_store {
        set_research_store_path_override(path)?;
    }
    if skip_cache_sync {
        set_research_store_cache_sync_enabled_override(false)?;
    }
    Ok(())
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
        Commands::MonitorLive { symbol, strategy } => monitor_live(&symbol, strategy),
        Commands::Report { run } => {
            println!("{}", read_report_markdown(run)?);
            Ok(())
        }
        Commands::ExportLiveSignal {
            run,
            approved_strategy,
            output,
            as_of,
        } => export_live_signal(&run, &approved_strategy, &output, as_of),
        Commands::LiveSignalStatus {
            live_signal,
            as_of,
            require_signal,
        } => live_signal_status(&live_signal, as_of, require_signal),
        Commands::MarketSessionStatus { require_open, json } => {
            market_session_status(require_open, json)
        }
        Commands::RefreshLiveSignal {
            approved_strategy,
            output,
            state_file,
            from,
            to,
            max_expirations,
            fetch_concurrency,
            symbol_concurrency,
            force_refresh,
            cache_only,
            market_window_only,
            timeout_seconds,
            json,
        } => {
            refresh_live_signal(RefreshLiveSignalArgs {
                approved_strategy,
                output,
                state_file,
                from,
                to: to.unwrap_or_else(|| Utc::now().date_naive()),
                max_expirations,
                fetch_concurrency,
                symbol_concurrency,
                force_refresh,
                cache_only,
                market_window_only,
                timeout_seconds,
                json,
            })
            .await
        }
        Commands::LiveMarketEngine {
            approved_strategy,
            source_live_signal,
            output,
            state_file,
            store,
            as_of,
            interval_seconds,
            max_source_age_seconds,
            market_window_only,
            once,
            json,
        } => {
            run_live_market_engine(LiveMarketEngineArgs {
                approved_strategy,
                source_live_signal,
                output,
                state_file,
                store,
                as_of,
                interval_seconds,
                max_source_age_seconds,
                market_window_only,
                once,
                json,
            })
            .await
        }
        Commands::ExecutionReadiness {
            live_signal,
            as_of,
            account_cash,
            debit_max_loss,
            wheel_reserve_cap,
            free_cash_buffer,
            max_wheel_positions_per_symbol,
            broker,
            broker_multi_leg_options,
            broker_cash_secured_puts,
            broker_covered_calls,
            robinhood_mcp_command,
            max_order_age_seconds,
            allow_blocked,
            json,
        } => execution_readiness(
            &live_signal,
            as_of,
            account_cash,
            debit_max_loss,
            wheel_reserve_cap,
            free_cash_buffer,
            max_wheel_positions_per_symbol,
            broker,
            broker_multi_leg_options,
            broker_cash_secured_puts,
            broker_covered_calls,
            robinhood_mcp_command,
            max_order_age_seconds,
            allow_blocked,
            json,
        ),
        Commands::RunExecutionDecision {
            live_signal,
            as_of,
            max_loss,
            account_cash,
            debit_max_loss,
            wheel_reserve_cap,
            free_cash_buffer,
            max_wheel_positions_per_symbol,
            mode,
            broker,
            broker_multi_leg_options,
            broker_cash_secured_puts,
            broker_covered_calls,
            robinhood_mcp_command,
            order_ledger,
            max_order_age_seconds,
            json,
        } => run_execution_decision(
            &live_signal,
            as_of,
            max_loss,
            account_cash,
            debit_max_loss,
            wheel_reserve_cap,
            free_cash_buffer,
            max_wheel_positions_per_symbol,
            mode,
            broker,
            broker_multi_leg_options,
            broker_cash_secured_puts,
            broker_covered_calls,
            robinhood_mcp_command,
            order_ledger,
            max_order_age_seconds,
            json,
        ),
        Commands::ExecutionWorker {
            live_signal,
            as_of,
            account_cash,
            debit_max_loss,
            wheel_reserve_cap,
            free_cash_buffer,
            max_wheel_positions_per_symbol,
            mode,
            broker,
            broker_multi_leg_options,
            broker_cash_secured_puts,
            broker_covered_calls,
            robinhood_mcp_command,
            order_ledger,
            notify_command,
            notify_ledger,
            max_order_age_seconds,
            poll_seconds,
            once,
            health_output,
            json,
        } => {
            run_execution_worker(ExecutionWorkerArgs {
                live_signal,
                as_of,
                risk: CanaryRiskPolicy {
                    account_cash,
                    debit_max_loss,
                    wheel_reserve_cap,
                    free_cash_buffer,
                    max_wheel_positions_per_symbol,
                },
                broker: execution_broker(
                    broker,
                    broker_multi_leg_options,
                    broker_cash_secured_puts,
                    broker_covered_calls,
                    mode == ExecutionMode::Live,
                ),
                mode,
                robinhood_mcp_command,
                order_ledger,
                notify_command,
                notify_ledger,
                max_order_age_seconds,
                poll_seconds,
                once,
                health_output,
                json,
            })
            .await
        }
        Commands::ExecutionWorkerEnv { once } => run_execution_worker_from_env(once).await,
        Commands::ExecutionWorkerSnapshot {
            health_output,
            pid_file,
            max_age_seconds,
            json,
        } => execution_worker_snapshot(&health_output, &pid_file, max_age_seconds, json),
        Commands::ResearchStoreHealth { json } => {
            let store = ResearchStore::open_default()?;
            let store_path = default_research_store_path();
            let report = store.health(&store_path)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                print_research_store_health(&report);
            }
            Ok(())
        }
        Commands::ResearchStoreImport {
            raw_root,
            symbols,
            max_files_per_symbol,
            json,
        } => {
            let report = import_research_store(&raw_root, &symbols, max_files_per_symbol)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                print_research_store_import_report(&report);
            }
            Ok(())
        }
        Commands::ResearchStorePerfCheck {
            raw_root,
            symbols,
            json,
        } => {
            let report = research_store_perf_check(&raw_root, &symbols)?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                print_research_store_perf_report(&report);
            }
            Ok(())
        }
        Commands::AuditOptionCacheCoverage {
            symbols,
            from,
            to,
            max_expirations,
            research_store,
            skip_cache_sync,
            json,
        } => {
            configure_research_store_for_command(research_store, skip_cache_sync)?;
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
            recent_first,
            option_side,
            fetch_concurrency,
            window_timeout_seconds,
            force_refresh,
            progress,
            research_store,
            skip_cache_sync,
            json,
        } => {
            configure_research_store_for_command(research_store, skip_cache_sync)?;
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
                recent_first,
                option_side: option_side.into(),
                fetch_concurrency,
                force_refresh,
                window_timeout_seconds,
                progress,
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
            research_store,
            skip_cache_sync,
            expand_on_plateau,
            single_symbol_only,
        } => {
            configure_research_store_for_command(research_store, skip_cache_sync)?;
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
            research_store,
            skip_cache_sync,
            expand_on_plateau,
            single_symbol_only,
        } => {
            configure_research_store_for_command(research_store, skip_cache_sync)?;
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
            research_store,
            skip_cache_sync,
        } => {
            configure_research_store_for_command(research_store, skip_cache_sync)?;
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
            research_store,
            skip_cache_sync,
        } => {
            configure_research_store_for_command(research_store, skip_cache_sync)?;
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
                promotion_baseline_cost_25_pnl: None,
                promotion_baseline_symbols: Vec::new(),
                promotion_min_new_symbol_trades: 5,
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
            approved_strategy,
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
            promotion_baseline_run,
            promotion_min_new_symbol_trades,
            research_store,
            skip_cache_sync,
        } => {
            configure_research_store_for_command(research_store, skip_cache_sync)?;
            let promotion_reference = promotion_baseline_run
                .as_deref()
                .map(portfolio_promotion_reference_from_run)
                .transpose()?;
            let report = if let Some(approved_strategy_path) = approved_strategy {
                let approved_strategy = read_approved_strategy(&approved_strategy_path)?;
                let constraints = approved_strategy.portfolio_constraints.clone();
                let from = approved_strategy_research_from(&approved_strategy, from);
                run_portfolio_selector_research_for_profile(
                    PortfolioWheelResearchRequest {
                        symbols: approved_strategy.symbols.clone(),
                        from,
                        to,
                        max_expirations,
                        fetch_concurrency,
                        symbol_concurrency,
                        force_refresh,
                        cache_only,
                        capital_budget: constraints.capital_budget,
                        max_symbol_allocation_pct: constraints.max_symbol_allocation_pct,
                        max_open_positions: constraints.max_open_positions,
                        max_positions_per_symbol: constraints.max_positions_per_symbol,
                        max_total_trades_per_symbol: constraints.max_total_trades_per_symbol,
                        portfolio_drawdown_cooldown_trigger_pct: constraints
                            .portfolio_drawdown_cooldown_trigger_pct,
                        portfolio_drawdown_cooldown_days: constraints
                            .portfolio_drawdown_cooldown_days,
                        symbol_drawdown_cooldown_trigger_pct: constraints
                            .symbol_drawdown_cooldown_trigger_pct,
                        symbol_drawdown_cooldown_days: constraints.symbol_drawdown_cooldown_days,
                        promotion_baseline_cost_25_pnl: promotion_reference
                            .as_ref()
                            .map(|reference| reference.cost_25_pnl),
                        promotion_baseline_symbols: promotion_reference
                            .as_ref()
                            .map(|reference| reference.symbols.clone())
                            .unwrap_or_default(),
                        promotion_min_new_symbol_trades,
                    },
                    &approved_strategy.profile_name,
                )
                .await?
            } else {
                let symbols = if symbols.is_empty() {
                    DEFAULT_WEEKLY_RESEARCH_SYMBOLS
                        .iter()
                        .map(|symbol| (*symbol).to_owned())
                        .collect()
                } else {
                    symbols
                };
                run_portfolio_selector_research(PortfolioWheelResearchRequest {
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
                    promotion_baseline_cost_25_pnl: promotion_reference
                        .as_ref()
                        .map(|reference| reference.cost_25_pnl),
                    promotion_baseline_symbols: promotion_reference
                        .as_ref()
                        .map(|reference| reference.symbols.clone())
                        .unwrap_or_default(),
                    promotion_min_new_symbol_trades,
                })
                .await?
            };
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

fn export_live_signal(
    run: &Path,
    approved_strategy_path: &Path,
    output: &Path,
    as_of: Option<NaiveDate>,
) -> Result<()> {
    export_live_signal_with_gate(run, approved_strategy_path, output, as_of, true)
}

fn export_live_signal_with_gate(
    run: &Path,
    approved_strategy_path: &Path,
    output: &Path,
    as_of: Option<NaiveDate>,
    require_research_gate: bool,
) -> Result<()> {
    let report_path = portfolio_report_json_path(run);
    let report: PortfolioWheelReport = serde_json::from_str(
        &fs::read_to_string(&report_path)
            .with_context(|| format!("read portfolio report {}", report_path.display()))?,
    )
    .with_context(|| format!("parse portfolio report {}", report_path.display()))?;
    let approved_strategy: ApprovedStrategy = serde_json::from_str(
        &fs::read_to_string(approved_strategy_path).with_context(|| {
            format!(
                "read approved strategy {}",
                approved_strategy_path.display()
            )
        })?,
    )
    .with_context(|| {
        format!(
            "parse approved strategy {}",
            approved_strategy_path.display()
        )
    })?;
    let profile = report
        .profiles
        .iter()
        .find(|profile| profile.profile.name == approved_strategy.profile_name)
        .with_context(|| {
            format!(
                "approved profile {} not found in selector report {}",
                approved_strategy.profile_name,
                report_path.display()
            )
        })?;
    if !profile.gate_pass && require_research_gate {
        anyhow::bail!(
            "approved profile {} does not pass research gate: {}",
            approved_strategy.profile_name,
            profile.gate_reason
        );
    }
    if !profile.gate_pass && !require_research_gate {
        approved_strategy.production_approval.as_ref().with_context(|| {
            format!(
                "approved profile {} failed detector-window gate and has no production approval",
                approved_strategy.profile_name
            )
        })?;
    }
    let as_of = as_of.unwrap_or(report.to);
    let execution_rules = live_execution_rules_from_profile(&profile.profile);
    let signals = profile
        .latest_actions
        .iter()
        .map(|action| live_trade_signal_from_latest_action(action, &execution_rules))
        .collect::<Vec<_>>();
    let as_of_string = as_of.to_string();
    let selected_signal = signals
        .iter()
        .find(|signal| {
            signal.status == SignalStatus::NewEntry
                && signal.entry_date.as_deref() == Some(as_of_string.as_str())
                && approved_strategy
                    .allowed_live_strategies
                    .iter()
                    .any(|strategy| strategy == &signal.strategy)
        })
        .cloned();
    let artifact = LiveSignalArtifact {
        schema_version: LIVE_SIGNAL_SCHEMA_VERSION,
        strategy_id: approved_strategy.strategy_id.clone(),
        profile_name: approved_strategy.profile_name.clone(),
        as_of,
        generated_at: Utc::now(),
        market_data_through: report.to,
        approved_strategy,
        signals,
        selected_signal,
        source_run_id: report.run_id,
        source_report: report_path.display().to_string(),
    };
    artifact.validate_contract()?;
    write_json_atomic_value(output, &artifact)
        .with_context(|| format!("write live signal artifact {}", output.display()))?;
    println!("{}", output.display());
    Ok(())
}

fn live_trade_signal_from_latest_action(
    action: &spreadfoundry::research::PortfolioLatestAction,
    execution_rules: &LiveExecutionRules,
) -> TradeSignal {
    let status = match action.status.as_str() {
        "new_entry" => SignalStatus::NewEntry,
        "already_open" => SignalStatus::AlreadyOpen,
        _ => SignalStatus::RecentClosed,
    };
    TradeSignal {
        status,
        symbol: action.symbol.clone(),
        strategy: action.strategy.as_str().to_owned(),
        entry_date: Some(action.entry_date.to_string()),
        exit_date: Some(action.exit_date.to_string()),
        expiration: Some(action.expiration.to_string()),
        short_put: Some(action.short_strike),
        short_strike: Some(action.short_strike),
        long_strike: Some(action.long_strike),
        wheel_covered_call_expiration: action
            .wheel_covered_call_expiration
            .map(|date| date.to_string()),
        wheel_covered_call_strike: action.wheel_covered_call_strike,
        width: Some(action.width),
        entry_credit: Some(action.entry_credit),
        max_loss: Some(action.max_loss),
        reserve: None,
        reserve_basis: None,
        pnl: Some(action.pnl),
        dte_entry: Some(action.dte_entry),
        days_held: Some(action.days_held),
        exit_reason: Some(action.exit_reason.clone()),
        short_delta: Some(action.short_delta),
        long_delta: Some(action.long_delta),
        short_oi: Some(action.short_oi),
        long_oi: Some(action.long_oi),
        short_iv: Some(action.short_iv),
        long_iv: Some(action.long_iv),
        underlying_price: Some(action.underlying_price),
        execution_rules: Some(execution_rules.clone()),
    }
}

fn live_execution_rules_from_profile(profile: &ResearchProfile) -> LiveExecutionRules {
    LiveExecutionRules {
        take_profit_pct: profile.take_profit_pct,
        stop_loss_multiple: profile.stop_loss_multiple,
        force_close_dte: profile.force_close_dte,
        max_hold_days: profile.max_hold_days,
    }
}

fn portfolio_report_json_path(run: &Path) -> PathBuf {
    if run.is_dir() {
        run.join("portfolio_research.json")
    } else {
        run.to_path_buf()
    }
}

#[derive(Clone, Debug)]
struct PortfolioPromotionReference {
    cost_25_pnl: f64,
    symbols: Vec<String>,
}

fn portfolio_promotion_reference_from_run(run: &Path) -> Result<PortfolioPromotionReference> {
    let report_path = portfolio_report_json_path(run);
    let report: PortfolioWheelReport = serde_json::from_str(
        &fs::read_to_string(&report_path)
            .with_context(|| format!("read portfolio report {}", report_path.display()))?,
    )
    .with_context(|| format!("parse portfolio report {}", report_path.display()))?;
    let best = report
        .profiles
        .first()
        .with_context(|| format!("portfolio report {} has no profiles", report_path.display()))?;
    let cost_25_pnl = best
        .metrics
        .cost_stress
        .iter()
        .find(|stress| (stress.per_trade_cost - 25.0).abs() < f64::EPSILON)
        .map(|stress| stress.total_pnl)
        .unwrap_or(best.metrics.total_pnl);
    Ok(PortfolioPromotionReference {
        cost_25_pnl,
        symbols: report.symbols,
    })
}

fn write_json_atomic(path: &Path, value: &serde_json::Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create output directory {}", parent.display()))?;
    }
    let tmp_path = path.with_extension(format!("json.tmp.{}", std::process::id()));
    fs::write(&tmp_path, serde_json::to_string_pretty(value)?)
        .with_context(|| format!("write temp JSON {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "rename temp JSON {} to {}",
            tmp_path.display(),
            path.display()
        )
    })?;
    Ok(())
}

fn write_json_atomic_value<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    write_json_atomic(path, &serde_json::to_value(value)?)
}

fn live_signal_status(
    live_signal: &Path,
    as_of: Option<NaiveDate>,
    require_signal: bool,
) -> Result<()> {
    let as_of = as_of.unwrap_or_else(|| execution_default_as_of(Utc::now()));
    let artifact: LiveSignalArtifact = serde_json::from_str(
        &fs::read_to_string(live_signal)
            .with_context(|| format!("read live signal artifact {}", live_signal.display()))?,
    )
    .with_context(|| format!("parse live signal artifact {}", live_signal.display()))?;
    artifact.validate_contract()?;
    let as_of_string = as_of.to_string();
    let selected_entry = artifact
        .selected_signal
        .as_ref()
        .filter(|signal| signal.entry_date.as_deref() == Some(as_of_string.as_str()));
    let management_signals = if selected_entry.is_none() {
        live_management_signals(&artifact, as_of).map_err(anyhow::Error::msg)?
    } else {
        Vec::new()
    };
    let actionable_signal = selected_entry.or_else(|| management_signals.first());
    println!(
        "strategy_id={} profile={} as_of={} generated_at={} selected_signal={}",
        artifact.strategy_id,
        artifact.profile_name,
        as_of,
        artifact.generated_at.to_rfc3339(),
        actionable_signal.is_some()
    );
    if !artifact.signals.is_empty() {
        println!("signals={}", artifact.signals.len());
        for signal in artifact.signals.iter().take(5) {
            println!(
                "{} {} {} entry={} exit={} pnl={:.2}",
                signal.status.as_str(),
                signal.symbol,
                signal.strategy,
                signal.entry_date.as_deref().unwrap_or("unknown"),
                signal.exit_date.as_deref().unwrap_or("unknown"),
                signal.pnl.unwrap_or(0.0)
            );
        }
    }
    if require_signal && actionable_signal.is_none() {
        anyhow::bail!(
            "no selected live entry or open-position management signal in {}; refresh live signal from approved strategy",
            live_signal.display()
        );
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct MarketSessionSnapshot {
    checked_at: chrono::DateTime<Utc>,
    open: bool,
    reason: String,
    source: String,
    date_et: NaiveDate,
    minute_et: u32,
    close_minute_et: Option<u32>,
}

fn market_session_status(require_open: bool, json: bool) -> Result<()> {
    let snapshot = market_session_snapshot_for_status(Utc::now());
    if json {
        println!("{}", serde_json::to_string_pretty(&snapshot)?);
    } else {
        println!(
            "open={} source={} date_et={} minute_et={} reason={}",
            snapshot.open, snapshot.source, snapshot.date_et, snapshot.minute_et, snapshot.reason
        );
    }
    if require_open && !snapshot.open {
        anyhow::bail!("{}", snapshot.reason);
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct RefreshLiveSignalArgs {
    approved_strategy: PathBuf,
    output: PathBuf,
    state_file: PathBuf,
    from: NaiveDate,
    to: NaiveDate,
    max_expirations: Option<usize>,
    fetch_concurrency: usize,
    symbol_concurrency: usize,
    force_refresh: bool,
    cache_only: bool,
    market_window_only: bool,
    timeout_seconds: u64,
    json: bool,
}

#[derive(Debug, Serialize)]
struct LiveSignalRefreshState {
    started_at: chrono::DateTime<Utc>,
    finished_at: Option<chrono::DateTime<Utc>>,
    status: String,
    exit_code: i32,
    run_to: NaiveDate,
    run_dir: String,
    approved_strategy: String,
    live_signal_artifact: String,
    reason: String,
}

#[derive(Clone, Debug)]
struct LiveMarketEngineArgs {
    approved_strategy: PathBuf,
    source_live_signal: PathBuf,
    output: PathBuf,
    state_file: PathBuf,
    store: PathBuf,
    as_of: Option<NaiveDate>,
    interval_seconds: u64,
    max_source_age_seconds: u64,
    market_window_only: bool,
    once: bool,
    json: bool,
}

#[derive(Debug, Serialize)]
struct LiveMarketEngineState {
    checked_at: chrono::DateTime<Utc>,
    status: String,
    reason: String,
    approved_strategy: String,
    source_live_signal: String,
    output: String,
    store: String,
    snapshot_id: Option<String>,
    selected_signal: bool,
    candidates_seen: usize,
    provider_health: Option<LiveMarketProviderHealth>,
    decision: Option<LiveMarketDecision>,
    market_session: Option<MarketSessionSnapshot>,
}

async fn refresh_live_signal(args: RefreshLiveSignalArgs) -> Result<()> {
    if args.timeout_seconds == 0 {
        anyhow::bail!("--timeout-seconds must be positive");
    }
    let started_at = Utc::now();
    let _refresh_lock = match acquire_live_signal_refresh_lock(
        &args.state_file,
        args.timeout_seconds.saturating_add(60),
    ) {
        Ok(lock) => lock,
        Err(err) => {
            let state = live_signal_refresh_state(
                &args,
                started_at,
                Some(Utc::now()),
                "refresh_already_running",
                0,
                "",
                &format!("another approved strategy signal refresh is already running: {err}"),
            );
            write_json_atomic_value(&args.state_file, &state)?;
            print_refresh_state(&state, args.json)?;
            return Ok(());
        }
    };
    if args.market_window_only {
        let session = market_session_snapshot_for_refresh(started_at);
        if !session.open {
            let state = live_signal_refresh_state(
                &args,
                started_at,
                Some(Utc::now()),
                "skipped_market_closed",
                0,
                "",
                session.reason.as_str(),
            );
            write_json_atomic_value(&args.state_file, &state)?;
            print_refresh_state(&state, args.json)?;
            return Ok(());
        }
    }

    let running = live_signal_refresh_state(
        &args,
        started_at,
        None,
        "running",
        0,
        "",
        "approved strategy signal refresh in progress",
    );
    write_json_atomic_value(&args.state_file, &running)?;

    let (sender, receiver) = mpsc::channel();
    let worker_args = args.clone();
    std::thread::spawn(move || {
        let result = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime
                .block_on(refresh_live_signal_selector_run(worker_args))
                .map_err(|err| err.to_string()),
            Err(err) => Err(format!("create refresh worker runtime: {err}")),
        };
        let _ = sender.send(result);
    });

    let timeout = StdDuration::from_secs(args.timeout_seconds);
    let wait_started = StdInstant::now();
    let refresh_result = loop {
        match receiver.try_recv() {
            Ok(result) => break Some(result),
            Err(mpsc::TryRecvError::Disconnected) => {
                break Some(Err(
                    "approved strategy signal refresh worker exited without result".to_owned(),
                ));
            }
            Err(mpsc::TryRecvError::Empty) => {
                if wait_started.elapsed() >= timeout {
                    break None;
                }
                tokio::time::sleep(StdDuration::from_millis(250)).await;
            }
        }
    };

    match refresh_result {
        Some(Ok(run_dir)) => {
            let state = live_signal_refresh_state(
                &args,
                started_at,
                Some(Utc::now()),
                "exported",
                0,
                run_dir.to_string_lossy().as_ref(),
                "exported fresh live signal artifact",
            );
            write_json_atomic_value(&args.state_file, &state)?;
            print_refresh_state(&state, args.json)
        }
        Some(Err(err)) => {
            let state = live_signal_refresh_state(
                &args,
                started_at,
                Some(Utc::now()),
                "approved_strategy_not_ready",
                0,
                "",
                err.as_str(),
            );
            write_json_atomic_value(&args.state_file, &state)?;
            print_refresh_state(&state, args.json)
        }
        None => {
            let state = live_signal_refresh_state(
                &args,
                started_at,
                Some(Utc::now()),
                "selector_timeout",
                124,
                "",
                &format!(
                    "approved strategy signal refresh exceeded {}s timeout",
                    args.timeout_seconds
                ),
            );
            write_json_atomic_value(&args.state_file, &state)?;
            print_refresh_state(&state, args.json)?;
            drop(_refresh_lock);
            let _ = std::io::stdout().flush();
            let _ = std::io::stderr().flush();
            std::process::exit(124);
        }
    }
}

async fn refresh_live_signal_selector_run(args: RefreshLiveSignalArgs) -> Result<PathBuf> {
    let approved_strategy = read_approved_strategy(&args.approved_strategy)?;
    let constraints = approved_strategy.portfolio_constraints.clone();
    let (from, require_research_gate) =
        approved_strategy_refresh_from_and_gate(&approved_strategy, args.from, args.to);
    let report = run_portfolio_selector_research_for_profile(
        PortfolioWheelResearchRequest {
            symbols: approved_strategy.symbols.clone(),
            from,
            to: args.to,
            max_expirations: args.max_expirations,
            fetch_concurrency: args.fetch_concurrency,
            symbol_concurrency: args.symbol_concurrency,
            force_refresh: args.force_refresh,
            cache_only: args.cache_only,
            capital_budget: constraints.capital_budget,
            max_symbol_allocation_pct: constraints.max_symbol_allocation_pct,
            max_open_positions: constraints.max_open_positions,
            max_positions_per_symbol: constraints.max_positions_per_symbol,
            max_total_trades_per_symbol: constraints.max_total_trades_per_symbol,
            portfolio_drawdown_cooldown_trigger_pct: constraints
                .portfolio_drawdown_cooldown_trigger_pct,
            portfolio_drawdown_cooldown_days: constraints.portfolio_drawdown_cooldown_days,
            symbol_drawdown_cooldown_trigger_pct: constraints.symbol_drawdown_cooldown_trigger_pct,
            symbol_drawdown_cooldown_days: constraints.symbol_drawdown_cooldown_days,
            promotion_baseline_cost_25_pnl: None,
            promotion_baseline_symbols: Vec::new(),
            promotion_min_new_symbol_trades: 10,
        },
        &approved_strategy.profile_name,
    )
    .await?;
    let run_dir = PathBuf::from("runs").join(&report.run_id);
    export_live_signal_with_gate(
        &run_dir,
        &args.approved_strategy,
        &args.output,
        Some(args.to),
        require_research_gate,
    )?;
    Ok(run_dir)
}

fn read_approved_strategy(path: &Path) -> Result<ApprovedStrategy> {
    let approved_strategy: ApprovedStrategy = serde_json::from_str(
        &fs::read_to_string(path)
            .with_context(|| format!("read approved strategy {}", path.display()))?,
    )
    .with_context(|| format!("parse approved strategy {}", path.display()))?;
    approved_strategy
        .validate_contract()
        .with_context(|| format!("validate approved strategy {}", path.display()))?;
    Ok(approved_strategy)
}

fn approved_strategy_research_from(
    approved_strategy: &ApprovedStrategy,
    fallback: NaiveDate,
) -> NaiveDate {
    approved_strategy.research_from.unwrap_or(fallback)
}

fn approved_strategy_refresh_from_and_gate(
    approved_strategy: &ApprovedStrategy,
    fallback: NaiveDate,
    to: NaiveDate,
) -> (NaiveDate, bool) {
    match approved_strategy.live_detector_lookback_days {
        Some(days) if days > 0 => (to - chrono::Duration::days(days), false),
        _ => (
            approved_strategy_research_from(approved_strategy, fallback),
            true,
        ),
    }
}

fn live_signal_refresh_state(
    args: &RefreshLiveSignalArgs,
    started_at: chrono::DateTime<Utc>,
    finished_at: Option<chrono::DateTime<Utc>>,
    status: &str,
    exit_code: i32,
    run_dir: &str,
    reason: &str,
) -> LiveSignalRefreshState {
    LiveSignalRefreshState {
        started_at,
        finished_at,
        status: status.to_owned(),
        exit_code,
        run_to: args.to,
        run_dir: run_dir.to_owned(),
        approved_strategy: args.approved_strategy.display().to_string(),
        live_signal_artifact: args.output.display().to_string(),
        reason: reason.to_owned(),
    }
}

fn print_refresh_state(state: &LiveSignalRefreshState, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(state)?);
    } else {
        println!(
            "status={} run_to={} run_dir={} reason={}",
            state.status, state.run_to, state.run_dir, state.reason
        );
    }
    Ok(())
}

async fn run_live_market_engine(args: LiveMarketEngineArgs) -> Result<()> {
    if args.interval_seconds == 0 {
        anyhow::bail!("--interval-seconds must be positive");
    }
    if args.max_source_age_seconds == 0 {
        anyhow::bail!("--max-source-age-seconds must be positive");
    }

    loop {
        let state = run_live_market_engine_once(&args)?;
        print_live_market_engine_state(&state, args.json)?;
        if args.once {
            return Ok(());
        }
        tokio::time::sleep(StdDuration::from_secs(args.interval_seconds)).await;
    }
}

fn run_live_market_engine_once(args: &LiveMarketEngineArgs) -> Result<LiveMarketEngineState> {
    let now = Utc::now();
    let as_of = args.as_of.unwrap_or_else(|| execution_default_as_of(now));
    let approved_strategy = read_approved_strategy(&args.approved_strategy)?;
    if args.market_window_only {
        let session = market_session_snapshot_for_refresh(now);
        if !session.open {
            let state = LiveMarketEngineState {
                checked_at: now,
                status: "skipped_market_closed".to_owned(),
                reason: session.reason.clone(),
                approved_strategy: args.approved_strategy.display().to_string(),
                source_live_signal: args.source_live_signal.display().to_string(),
                output: args.output.display().to_string(),
                store: args.store.display().to_string(),
                snapshot_id: None,
                selected_signal: false,
                candidates_seen: 0,
                provider_health: None,
                decision: None,
                market_session: Some(session),
            };
            write_json_atomic_value(&args.state_file, &state)?;
            return Ok(state);
        }
    }

    let record = build_signal_artifact_live_market_snapshot(LiveMarketEngineConfig {
        approved_strategy,
        source_live_signal: &args.source_live_signal,
        output: &args.output,
        as_of,
        max_source_age_seconds: args.max_source_age_seconds,
        now,
    })?;
    write_json_atomic_value(&args.output, &record.artifact)
        .with_context(|| format!("write live market artifact {}", args.output.display()))?;
    let mut store = ResearchStore::open(&args.store)?;
    store
        .record_live_market_snapshot(&record)
        .with_context(|| format!("record live market snapshot in {}", args.store.display()))?;
    let state = live_market_engine_state(args, now, &record);
    write_json_atomic_value(&args.state_file, &state)?;
    Ok(state)
}

fn live_market_engine_state(
    args: &LiveMarketEngineArgs,
    checked_at: chrono::DateTime<Utc>,
    record: &LiveMarketSnapshotRecord,
) -> LiveMarketEngineState {
    LiveMarketEngineState {
        checked_at,
        status: record.decision.status.clone(),
        reason: record.decision.reason.clone(),
        approved_strategy: args.approved_strategy.display().to_string(),
        source_live_signal: args.source_live_signal.display().to_string(),
        output: args.output.display().to_string(),
        store: args.store.display().to_string(),
        snapshot_id: Some(record.snapshot_id.clone()),
        selected_signal: record.decision.selected_signal.is_some(),
        candidates_seen: record.decision.candidates_seen,
        provider_health: Some(record.provider_health.clone()),
        decision: Some(record.decision.clone()),
        market_session: None,
    }
}

fn print_live_market_engine_state(state: &LiveMarketEngineState, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(state)?);
    } else {
        println!(
            "status={} selected_signal={} candidates={} reason={}",
            state.status, state.selected_signal, state.candidates_seen, state.reason
        );
        if let Some(provider) = &state.provider_health {
            println!(
                "provider={} provider_status={} source_age={} symbols_ready={}/{}",
                provider.provider,
                provider.status,
                provider
                    .source_age_seconds
                    .map(|age| format!("{age}s"))
                    .unwrap_or_else(|| "-".to_owned()),
                provider.symbols_ready,
                provider.symbols_requested
            );
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_execution_decision(
    live_signal: &Path,
    as_of: Option<NaiveDate>,
    max_loss: Option<f64>,
    account_cash: f64,
    debit_max_loss: f64,
    wheel_reserve_cap: f64,
    free_cash_buffer: f64,
    max_wheel_positions_per_symbol: usize,
    mode: ExecutionMode,
    broker_kind: BrokerKind,
    broker_multi_leg_options: bool,
    broker_cash_secured_puts: bool,
    broker_covered_calls: bool,
    robinhood_mcp_command: Option<String>,
    order_ledger: PathBuf,
    max_order_age_seconds: u64,
    json: bool,
) -> Result<()> {
    if let Some(max_loss) = max_loss
        && max_loss <= 0.0
    {
        anyhow::bail!("--max-loss must be positive");
    }
    let as_of = as_of.unwrap_or_else(|| execution_default_as_of(Utc::now()));
    let artifact: LiveSignalArtifact = serde_json::from_str(
        &fs::read_to_string(live_signal)
            .with_context(|| format!("read live signal artifact {}", live_signal.display()))?,
    )
    .with_context(|| format!("parse live signal artifact {}", live_signal.display()))?;
    let risk = CanaryRiskPolicy {
        account_cash,
        debit_max_loss: max_loss.unwrap_or(debit_max_loss),
        wheel_reserve_cap,
        free_cash_buffer,
        max_wheel_positions_per_symbol,
    };
    validate_canary_risk_policy(&risk)?;
    let broker = execution_broker(
        broker_kind,
        broker_multi_leg_options,
        broker_cash_secured_puts,
        broker_covered_calls,
        mode == ExecutionMode::Live,
    );
    let mut decision = compute_execution_decision(
        &artifact,
        as_of,
        &risk,
        &broker,
        mode,
        max_order_age_seconds,
    );
    apply_broker_bridge(
        &mut decision,
        &broker,
        robinhood_mcp_command.as_deref(),
        Some(&order_ledger),
    )?;
    if json {
        println!("{}", serde_json::to_string_pretty(&decision)?);
    } else {
        println!(
            "status={} mode={} as_of={} debit_max_loss={:.2} wheel_reserve_cap={:.2} free_cash_buffer={:.2}",
            decision.status,
            execution_mode_label(decision.mode),
            decision.as_of,
            decision.risk.debit_max_loss,
            decision.risk.wheel_reserve_cap,
            decision.risk.free_cash_buffer
        );
        println!("reason={}", decision.reason);
        if let Some(action) = &decision.selected_signal {
            println!(
                "selected_signal={} {} {} entry={} exit={} reserve={:.2} max_loss={:.2}",
                action.status.as_str(),
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
                .or_else(|| decision.tradier_place.as_ref().map(|response| response.ok))
                .unwrap_or(false)
        );
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct ExecutionReadinessReport {
    checked_at: chrono::DateTime<Utc>,
    live_signal: String,
    live_signal_readable: bool,
    live_signal_parse_ok: bool,
    as_of: NaiveDate,
    broker: BrokerKind,
    ready_for_broker_review: bool,
    live_worker_ready_to_attempt_order: bool,
    robinhood_mcp_command_configured: bool,
    tradier_credentials_configured: bool,
    blockers: Vec<String>,
    warnings: Vec<String>,
    next_action: String,
    decision: Option<ExecutionDecision>,
    error: Option<String>,
}

#[allow(clippy::too_many_arguments)]
fn execution_readiness(
    live_signal: &Path,
    as_of: Option<NaiveDate>,
    account_cash: f64,
    debit_max_loss: f64,
    wheel_reserve_cap: f64,
    free_cash_buffer: f64,
    max_wheel_positions_per_symbol: usize,
    broker_kind: BrokerKind,
    broker_multi_leg_options: bool,
    broker_cash_secured_puts: bool,
    broker_covered_calls: bool,
    robinhood_mcp_command: Option<String>,
    max_order_age_seconds: u64,
    allow_blocked: bool,
    json: bool,
) -> Result<()> {
    let as_of = as_of.unwrap_or_else(|| execution_default_as_of(Utc::now()));
    let risk = CanaryRiskPolicy {
        account_cash,
        debit_max_loss,
        wheel_reserve_cap,
        free_cash_buffer,
        max_wheel_positions_per_symbol,
    };
    validate_canary_risk_policy(&risk)?;
    let broker = execution_broker(
        broker_kind,
        broker_multi_leg_options,
        broker_cash_secured_puts,
        broker_covered_calls,
        true,
    );
    let signal_body = fs::read_to_string(live_signal);
    let live_signal_readable = signal_body.is_ok();
    let mut signal_error = None;
    let artifact = match signal_body {
        Ok(body) => match serde_json::from_str::<LiveSignalArtifact>(&body) {
            Ok(artifact) => Some(artifact),
            Err(err) => {
                signal_error = Some(format!("parse live signal artifact: {err}"));
                None
            }
        },
        Err(err) => {
            signal_error = Some(format!("read live signal artifact: {err}"));
            None
        }
    };
    let live_signal_parse_ok = artifact.is_some();
    let report = build_execution_readiness_report(
        live_signal,
        live_signal_readable,
        live_signal_parse_ok,
        artifact.as_ref(),
        signal_error,
        as_of,
        &risk,
        &broker,
        robinhood_mcp_command.is_some(),
        tradier_config_from_env().is_ok(),
        max_order_age_seconds,
    );

    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_execution_readiness_report(&report);
    }
    if !allow_blocked && !report.live_worker_ready_to_attempt_order {
        anyhow::bail!(
            "execution readiness blocked: {}",
            report.blockers.join("; ")
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_execution_readiness_report(
    live_signal: &Path,
    live_signal_readable: bool,
    live_signal_parse_ok: bool,
    artifact: Option<&LiveSignalArtifact>,
    signal_error: Option<String>,
    as_of: NaiveDate,
    risk: &CanaryRiskPolicy,
    broker: &impl ExecutionBrokerView,
    robinhood_mcp_command_configured: bool,
    tradier_credentials_configured: bool,
    max_order_age_seconds: u64,
) -> ExecutionReadinessReport {
    let mut blockers = Vec::new();
    let mut warnings = Vec::new();
    let mut decision = None;
    let mcp_blocker = "SPREAD_ROBINHOOD_MCP_COMMAND not configured; execution worker cannot call Robinhood MCP review/place";
    let tradier_blocker = "SPREAD_TRADIER_ACCOUNT_ID/SPREAD_TRADIER_TOKEN not configured; execution worker cannot call Tradier preview/place";

    if let Some(err) = signal_error.as_deref() {
        push_unique(&mut blockers, err);
    }
    if broker.kind() == BrokerKind::Robinhood && !robinhood_mcp_command_configured {
        push_unique(&mut blockers, mcp_blocker);
    }
    if broker.kind() == BrokerKind::Tradier && !tradier_credentials_configured {
        push_unique(&mut blockers, tradier_blocker);
    }

    if let Some(artifact) = artifact {
        let execution_decision = compute_execution_decision(
            artifact,
            as_of,
            risk,
            broker,
            ExecutionMode::Live,
            max_order_age_seconds,
        );
        match execution_decision.status.as_str() {
            "ready" => warnings.push(
                "broker review/place has not been executed by this read-only readiness command"
                    .to_owned(),
            ),
            "no_signal" => push_unique(&mut blockers, execution_decision.reason.as_str()),
            "blocked" => push_unique(&mut blockers, execution_decision.reason.as_str()),
            other => push_unique(
                &mut blockers,
                format!("unexpected execution readiness status {other}"),
            ),
        }
        decision = Some(execution_decision);
    }

    let decision_ready_for_review = decision
        .as_ref()
        .is_some_and(|decision| decision.status == "ready" && decision.selected_signal.is_some());
    if let Some(decision) = decision.as_ref()
        && decision_ready_for_review
        && !live_position_lifecycle_ready(decision)
    {
        push_unique(
            &mut blockers,
            "live placement blocked because broker position reconciliation and exit lifecycle are not enabled for this broker/strategy",
        );
    }
    let ready_for_broker_review = decision_ready_for_review
        && blockers
            .iter()
            .all(|blocker| blocker.as_str() == mcp_blocker || blocker.as_str() == tradier_blocker);
    let broker_configured_for_live = match broker.kind() {
        BrokerKind::Robinhood => robinhood_mcp_command_configured,
        BrokerKind::Tradier => tradier_credentials_configured,
    };
    let live_worker_ready_to_attempt_order =
        decision_ready_for_review && blockers.is_empty() && broker_configured_for_live;
    let next_action = if live_worker_ready_to_attempt_order {
        "run the execution worker in live mode; it will preview before any placement"
    } else if ready_for_broker_review {
        match broker.kind() {
            BrokerKind::Robinhood => {
                "configure SPREAD_ROBINHOOD_MCP_COMMAND, then rerun execution readiness"
            }
            BrokerKind::Tradier => {
                "configure SPREAD_TRADIER_ACCOUNT_ID/SPREAD_TRADIER_TOKEN, then rerun execution readiness"
            }
        }
    } else if blockers
        .iter()
        .any(|blocker| blocker.contains("no selected live"))
    {
        "wait for signal refresh to produce a selected live entry or management signal"
    } else {
        "clear blockers, then rerun execution-readiness"
    }
    .to_owned();

    ExecutionReadinessReport {
        checked_at: Utc::now(),
        live_signal: live_signal.display().to_string(),
        live_signal_readable,
        live_signal_parse_ok,
        as_of,
        broker: broker.kind(),
        ready_for_broker_review,
        live_worker_ready_to_attempt_order,
        robinhood_mcp_command_configured,
        tradier_credentials_configured,
        blockers,
        warnings,
        next_action,
        decision,
        error: signal_error,
    }
}

fn print_execution_readiness_report(report: &ExecutionReadinessReport) {
    println!(
        "live_worker_ready_to_attempt_order={} ready_for_broker_review={} broker={} as_of={}",
        report.live_worker_ready_to_attempt_order,
        report.ready_for_broker_review,
        broker_label(report.broker),
        report.as_of
    );
    println!("live_signal={}", report.live_signal);
    println!("next_action={}", report.next_action);
    if report.blockers.is_empty() {
        println!("blockers=none");
    } else {
        for blocker in &report.blockers {
            println!("blocker={blocker}");
        }
    }
    for warning in &report.warnings {
        println!("warning={warning}");
    }
}

fn push_unique(items: &mut Vec<String>, item: impl Into<String>) {
    let item = item.into();
    if !items.iter().any(|existing| existing == &item) {
        items.push(item);
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct CanaryRiskPolicy {
    account_cash: f64,
    debit_max_loss: f64,
    wheel_reserve_cap: f64,
    free_cash_buffer: f64,
    max_wheel_positions_per_symbol: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
struct TradeSignalRisk {
    reserve: f64,
    reserve_basis: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct ExecutionDecision {
    status: String,
    reason: String,
    as_of: NaiveDate,
    mode: ExecutionMode,
    risk: CanaryRiskPolicy,
    broker_multi_leg_options: bool,
    broker_cash_secured_puts: bool,
    broker_covered_calls: bool,
    broker_review_ok: bool,
    broker: BrokerKind,
    signal_generated_at: Option<chrono::DateTime<Utc>>,
    max_order_age_seconds: u64,
    mcp_review: Option<RobinhoodMcpToolResponse>,
    mcp_place: Option<RobinhoodMcpToolResponse>,
    #[serde(default)]
    tradier_quote: Option<TradierQuotesResponse>,
    tradier_preview: Option<TradierOrderResponse>,
    tradier_place: Option<TradierOrderResponse>,
    #[serde(default)]
    action_kind: Option<ExecutionActionKind>,
    #[serde(default)]
    management_signals: Vec<TradeSignal>,
    selected_signal: Option<TradeSignal>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ExecutionActionKind {
    OpenEntry,
    ManageOpen,
}

fn compute_execution_decision(
    artifact: &LiveSignalArtifact,
    as_of: NaiveDate,
    risk: &CanaryRiskPolicy,
    broker: &impl ExecutionBrokerView,
    mode: ExecutionMode,
    max_order_age_seconds: u64,
) -> ExecutionDecision {
    compute_execution_decision_at(
        artifact,
        as_of,
        risk,
        broker,
        mode,
        max_order_age_seconds,
        Utc::now(),
    )
}

fn compute_execution_decision_at(
    artifact: &LiveSignalArtifact,
    as_of: NaiveDate,
    risk: &CanaryRiskPolicy,
    broker: &impl ExecutionBrokerView,
    mode: ExecutionMode,
    max_order_age_seconds: u64,
    now: chrono::DateTime<Utc>,
) -> ExecutionDecision {
    if let Err(err) = artifact.validate_contract() {
        return execution_decision(
            "blocked",
            &format!("live signal contract invalid: {err}"),
            as_of,
            mode,
            risk,
            broker,
            false,
            Some(artifact.generated_at),
            max_order_age_seconds,
            None,
        );
    }
    if artifact.as_of != as_of {
        if mode == ExecutionMode::Live && !execution_market_window_open_at(now) {
            return execution_decision(
                "blocked",
                "live placement requires the configured regular options-market window to be open",
                as_of,
                mode,
                risk,
                broker,
                false,
                Some(artifact.generated_at),
                max_order_age_seconds,
                None,
            );
        }
        return execution_decision(
            "blocked",
            &format!(
                "live signal as_of {} does not match requested as_of {as_of}",
                artifact.as_of
            ),
            as_of,
            mode,
            risk,
            broker,
            false,
            Some(artifact.generated_at),
            max_order_age_seconds,
            None,
        );
    }
    if artifact.market_data_through < as_of {
        if mode == ExecutionMode::Live && !execution_market_window_open_at(now) {
            return execution_decision(
                "blocked",
                "live placement requires the configured regular options-market window to be open",
                as_of,
                mode,
                risk,
                broker,
                false,
                Some(artifact.generated_at),
                max_order_age_seconds,
                None,
            );
        }
        return execution_decision(
            "blocked",
            &format!(
                "live signal market_data_through {} is older than requested as_of {as_of}",
                artifact.market_data_through
            ),
            as_of,
            mode,
            risk,
            broker,
            false,
            Some(artifact.generated_at),
            max_order_age_seconds,
            None,
        );
    }

    let mut management_signals = Vec::new();
    let management_candidates = match live_management_signals(artifact, as_of) {
        Ok(candidates) => candidates,
        Err(reason) => {
            return execution_decision(
                "blocked",
                &format!("live management signal selection failed: {reason}"),
                as_of,
                mode,
                risk,
                broker,
                false,
                Some(artifact.generated_at),
                max_order_age_seconds,
                None,
            );
        }
    };
    for candidate in management_candidates {
        if candidate.execution_rules.is_none() {
            return execution_decision(
                "blocked",
                "already-open live signal requires exported execution rules for lifecycle management",
                as_of,
                mode,
                risk,
                broker,
                false,
                Some(artifact.generated_at),
                max_order_age_seconds,
                Some(candidate),
            );
        }
        if let Err(err) = validate_live_management_signal_shape(&candidate) {
            return execution_decision(
                "blocked",
                &format!("already-open live signal has invalid management shape: {err}"),
                as_of,
                mode,
                risk,
                broker,
                false,
                Some(artifact.generated_at),
                max_order_age_seconds,
                Some(candidate),
            );
        }
        let signal_risk = match trade_signal_risk(&candidate) {
            Ok(signal_risk) => signal_risk,
            Err(reason) => {
                return execution_decision(
                    "blocked",
                    &format!("already-open live signal failed risk annotation: {reason}"),
                    as_of,
                    mode,
                    risk,
                    broker,
                    false,
                    Some(artifact.generated_at),
                    max_order_age_seconds,
                    Some(candidate),
                );
            }
        };
        management_signals.push(trade_signal_with_risk(candidate, signal_risk));
    }
    let selected = if let Some(selected) = artifact.selected_signal.clone() {
        if selected.status != SignalStatus::NewEntry {
            return execution_decision(
                "no_signal",
                "selected live signal is not a new entry",
                as_of,
                mode,
                risk,
                broker,
                false,
                Some(artifact.generated_at),
                max_order_age_seconds,
                Some(selected),
            );
        }
        let as_of_string = as_of.to_string();
        if selected.entry_date.as_deref() != Some(as_of_string.as_str()) {
            return execution_decision(
                "blocked",
                "selected live signal entry_date does not match requested as_of",
                as_of,
                mode,
                risk,
                broker,
                false,
                Some(artifact.generated_at),
                max_order_age_seconds,
                Some(selected),
            );
        }
        if matches!(
            selected.strategy.as_str(),
            "call_debit_spread" | "put_debit_spread" | "call_credit_spread" | "put_credit_spread"
        ) && selected.execution_rules.is_none()
        {
            return execution_decision(
                "blocked",
                "selected vertical-spread live entry requires exported execution rules for lifecycle management",
                as_of,
                mode,
                risk,
                broker,
                false,
                Some(artifact.generated_at),
                max_order_age_seconds,
                Some(selected),
            );
        }

        match trade_signal_allowed_by_risk(
            &selected,
            risk,
            &artifact.approved_strategy.portfolio_constraints,
            &artifact.signals,
        ) {
            Ok(signal_risk) => trade_signal_with_risk(selected, signal_risk),
            Err(reason) => {
                return execution_decision(
                    "blocked",
                    &format!("selected live signal failed risk policy: {reason}"),
                    as_of,
                    mode,
                    risk,
                    broker,
                    false,
                    Some(artifact.generated_at),
                    max_order_age_seconds,
                    Some(selected),
                );
            }
        }
    } else {
        if management_signals.is_empty() {
            return execution_decision(
                "no_signal",
                "no selected live entry or open-position management signal; execution worker has nothing to submit",
                as_of,
                mode,
                risk,
                broker,
                false,
                Some(artifact.generated_at),
                max_order_age_seconds,
                None,
            );
        }
        management_signals
            .first()
            .cloned()
            .expect("non-empty management candidates")
    };
    if let Err(err) = assert_trade_signal_broker_supported(&selected, broker) {
        return execution_decision(
            "blocked",
            &err.to_string(),
            as_of,
            mode,
            risk,
            broker,
            false,
            Some(artifact.generated_at),
            max_order_age_seconds,
            Some(selected),
        );
    }
    if mode == ExecutionMode::Live {
        let market_date = execution_market_date_at(now);
        if as_of != market_date {
            return execution_decision(
                "blocked",
                &format!(
                    "live placement requires --as-of to match current U.S. options-market date {market_date}; got {as_of}"
                ),
                as_of,
                mode,
                risk,
                broker,
                false,
                Some(artifact.generated_at),
                max_order_age_seconds,
                Some(selected),
            );
        }
        if !execution_market_window_open_at(now) {
            return execution_decision(
                "blocked",
                "live placement requires the configured regular options-market window to be open",
                as_of,
                mode,
                risk,
                broker,
                false,
                Some(artifact.generated_at),
                max_order_age_seconds,
                Some(selected),
            );
        }
        if let Err(err) =
            live_signal_fresh_enough_for_live_order(artifact, max_order_age_seconds, now)
        {
            return execution_decision(
                "blocked",
                &err.to_string(),
                as_of,
                mode,
                risk,
                broker,
                false,
                Some(artifact.generated_at),
                max_order_age_seconds,
                Some(selected),
            );
        }
    }
    let mut decision = execution_decision(
        "ready",
        execution_ready_reason(mode, execution_action_kind_for_signal(&selected)),
        as_of,
        mode,
        risk,
        broker,
        false,
        Some(artifact.generated_at),
        max_order_age_seconds,
        Some(selected),
    );
    decision.management_signals = management_signals;
    decision
}

fn execution_ready_reason(
    mode: ExecutionMode,
    action_kind: Option<ExecutionActionKind>,
) -> &'static str {
    match action_kind {
        Some(ExecutionActionKind::ManageOpen) => match mode {
            ExecutionMode::Monitor => {
                "open live signal passed local lifecycle validation; monitor mode does not request broker preview"
            }
            ExecutionMode::Review => {
                "open live signal passed local lifecycle validation; broker position and exit preview are required next"
            }
            ExecutionMode::Live => {
                "open live signal passed local lifecycle validation; broker position, exit preview, and placement are required next"
            }
        },
        _ => match mode {
            ExecutionMode::Monitor => {
                "selected live signal passed local validation; monitor mode does not request broker preview"
            }
            ExecutionMode::Review => {
                "selected live signal passed local validation; broker preview is required next"
            }
            ExecutionMode::Live => {
                "selected live signal passed local validation; broker preview and placement are required next"
            }
        },
    }
}

fn live_management_signals(
    artifact: &LiveSignalArtifact,
    as_of: NaiveDate,
) -> std::result::Result<Vec<TradeSignal>, String> {
    let approved_symbols = artifact
        .approved_strategy
        .symbols
        .iter()
        .map(|symbol| symbol.to_ascii_uppercase())
        .collect::<BTreeSet<_>>();
    let mut candidates = Vec::new();
    for signal in &artifact.signals {
        let wheel_reconciliation_probe =
            signal.status == SignalStatus::RecentClosed && signal.strategy == "wheel";
        if signal.status != SignalStatus::AlreadyOpen && !wheel_reconciliation_probe {
            continue;
        }
        if wheel_reconciliation_probe
            && (signal.expiration.is_none() || signal.short_put.or(signal.short_strike).is_none())
        {
            continue;
        }
        if !approved_symbols.contains(&signal.symbol.to_ascii_uppercase()) {
            return Err(format!(
                "management signal {} is not in the approved symbol list",
                trade_signal_label(signal)
            ));
        }
        if !artifact
            .approved_strategy
            .allowed_live_strategies
            .iter()
            .any(|strategy| strategy == &signal.strategy)
        {
            return Err(format!(
                "management signal {} is not approved for live execution",
                trade_signal_label(signal)
            ));
        }
        if !matches!(
            signal.strategy.as_str(),
            "call_debit_spread"
                | "put_debit_spread"
                | "call_credit_spread"
                | "put_credit_spread"
                | "wheel"
        ) {
            return Err(format!(
                "management signal {} has no live management implementation",
                trade_signal_label(signal)
            ));
        }
        let entry_date = trade_signal_date(signal, signal.entry_date.as_deref(), "entry_date")?;
        if entry_date > as_of {
            return Err(format!(
                "management signal {} has future entry_date {entry_date}",
                trade_signal_label(signal)
            ));
        }
        let expiration = trade_signal_date(signal, signal.expiration.as_deref(), "expiration")?;
        if expiration < as_of && signal.strategy != "wheel" {
            return Err(format!(
                "management signal {} expired on {expiration}; manual broker lifecycle handling is required",
                trade_signal_label(signal)
            ));
        }
        let mut candidate = signal.clone();
        if wheel_reconciliation_probe {
            candidate.status = SignalStatus::AlreadyOpen;
        }
        candidates.push(candidate);
    }
    candidates.sort_by(|a, b| {
        signal_expiration_sort_key(a)
            .cmp(&signal_expiration_sort_key(b))
            .then_with(|| signal_entry_sort_key(a).cmp(&signal_entry_sort_key(b)))
            .then_with(|| a.symbol.cmp(&b.symbol))
            .then_with(|| a.strategy.cmp(&b.strategy))
    });
    Ok(candidates)
}

fn trade_signal_date(
    signal: &TradeSignal,
    value: Option<&str>,
    field: &str,
) -> std::result::Result<NaiveDate, String> {
    let value = value.ok_or_else(|| {
        format!(
            "already-open signal {} missing {field}",
            trade_signal_label(signal)
        )
    })?;
    NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|err| {
        format!(
            "already-open signal {} has invalid {field} {value}: {err}",
            trade_signal_label(signal)
        )
    })
}

fn signal_expiration_sort_key(signal: &TradeSignal) -> Option<NaiveDate> {
    signal
        .expiration
        .as_deref()
        .and_then(|date| NaiveDate::parse_from_str(date, "%Y-%m-%d").ok())
}

fn signal_entry_sort_key(signal: &TradeSignal) -> Option<NaiveDate> {
    signal
        .entry_date
        .as_deref()
        .and_then(|date| NaiveDate::parse_from_str(date, "%Y-%m-%d").ok())
}

fn trade_signal_label(signal: &TradeSignal) -> String {
    format!("{} {}", signal.symbol, signal.strategy)
}

#[allow(clippy::too_many_arguments)]
fn execution_decision(
    status: &str,
    reason: &str,
    as_of: NaiveDate,
    mode: ExecutionMode,
    risk: &CanaryRiskPolicy,
    broker: &impl ExecutionBrokerView,
    broker_review_ok: bool,
    signal_generated_at: Option<chrono::DateTime<Utc>>,
    max_order_age_seconds: u64,
    selected_signal: Option<TradeSignal>,
) -> ExecutionDecision {
    let action_kind = selected_signal
        .as_ref()
        .and_then(execution_action_kind_for_signal);
    ExecutionDecision {
        status: status.to_owned(),
        reason: reason.to_owned(),
        as_of,
        mode,
        risk: risk.clone(),
        broker_multi_leg_options: broker.capabilities().multi_leg_options,
        broker_cash_secured_puts: broker.capabilities().cash_secured_puts,
        broker_covered_calls: broker.capabilities().covered_calls,
        broker: broker.kind(),
        broker_review_ok,
        signal_generated_at,
        max_order_age_seconds,
        mcp_review: None,
        mcp_place: None,
        tradier_quote: None,
        tradier_preview: None,
        tradier_place: None,
        action_kind,
        management_signals: Vec::new(),
        selected_signal,
    }
}

fn execution_action_kind_for_signal(signal: &TradeSignal) -> Option<ExecutionActionKind> {
    match signal.status {
        SignalStatus::NewEntry => Some(ExecutionActionKind::OpenEntry),
        SignalStatus::AlreadyOpen => Some(ExecutionActionKind::ManageOpen),
        SignalStatus::RecentClosed => None,
    }
}

fn live_signal_age_seconds_at(
    generated_at: chrono::DateTime<Utc>,
    now: chrono::DateTime<Utc>,
) -> Result<u64> {
    let age_seconds = now.signed_duration_since(generated_at).num_seconds();
    if age_seconds < 0 {
        anyhow::bail!(
            "live signal generated_at {} is in the future; check system clock or signal refresh",
            generated_at.to_rfc3339()
        );
    }
    Ok(age_seconds as u64)
}

fn live_signal_fresh_enough_for_live_order(
    artifact: &LiveSignalArtifact,
    max_order_age_seconds: u64,
    now: chrono::DateTime<Utc>,
) -> Result<()> {
    let age = live_signal_age_seconds_at(artifact.generated_at, now)?;
    if age > max_order_age_seconds {
        anyhow::bail!(
            "live placement blocked because live signal age {}s exceeds max {}s",
            age,
            max_order_age_seconds
        );
    }
    Ok(())
}

fn trade_signal_allowed_by_risk(
    action: &TradeSignal,
    risk: &CanaryRiskPolicy,
    constraints: &spreadfoundry::live_signal::ApprovedPortfolioConstraints,
    signals: &[TradeSignal],
) -> std::result::Result<TradeSignalRisk, String> {
    let action_risk = trade_signal_risk(action)?;
    approved_portfolio_constraints_allow_new_entry(action, &action_risk, constraints, signals)?;
    match action.strategy.as_str() {
        "put_debit_spread" | "call_debit_spread" => {
            let max_loss = action
                .max_loss
                .ok_or_else(|| format!("{} missing max_loss", action.strategy))?;
            let order_max_loss = debit_spread_order_max_loss(action)?;
            if (order_max_loss - max_loss).abs() > 1.0 {
                return Err(format!(
                    "{} max_loss {:.2} does not match order debit risk {:.2}",
                    action.strategy, max_loss, order_max_loss
                ));
            }
            if max_loss <= 0.0 || max_loss > risk.debit_max_loss {
                return Err(format!(
                    "{} max_loss {:.2} exceeds debit cap {:.2}",
                    action.strategy, max_loss, risk.debit_max_loss
                ));
            }
        }
        "put_credit_spread" | "call_credit_spread" => {
            let max_loss = action
                .max_loss
                .ok_or_else(|| format!("{} missing max_loss", action.strategy))?;
            let order_max_loss = credit_spread_order_max_loss(action)?;
            if (order_max_loss - max_loss).abs() > 1.0 {
                return Err(format!(
                    "{} max_loss {:.2} does not match order credit-spread risk {:.2}",
                    action.strategy, max_loss, order_max_loss
                ));
            }
            if max_loss <= 0.0 || max_loss > risk.debit_max_loss {
                return Err(format!(
                    "{} max_loss {:.2} exceeds defined-risk spread cap {:.2}",
                    action.strategy, max_loss, risk.debit_max_loss
                ));
            }
        }
        "wheel" => {
            if action.status == SignalStatus::NewEntry {
                let open_same_symbol = signals
                    .iter()
                    .filter(|signal| {
                        signal.status == SignalStatus::AlreadyOpen
                            && signal.strategy == "wheel"
                            && signal.symbol == action.symbol
                    })
                    .count();
                if open_same_symbol >= risk.max_wheel_positions_per_symbol {
                    return Err(format!(
                        "wheel {} already has {} open wheel positions; max is {}",
                        action.symbol, open_same_symbol, risk.max_wheel_positions_per_symbol
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
        other => return Err(format!("strategy {other} is not enabled for execution")),
    }
    if action_risk.reserve > risk.account_cash - risk.free_cash_buffer {
        return Err(format!(
            "{} reserve {:.2} would breach free-cash buffer {:.2} on account cash {:.2}",
            action.strategy, action_risk.reserve, risk.free_cash_buffer, risk.account_cash
        ));
    }
    Ok(action_risk)
}

fn approved_portfolio_constraints_allow_new_entry(
    action: &TradeSignal,
    action_risk: &TradeSignalRisk,
    constraints: &spreadfoundry::live_signal::ApprovedPortfolioConstraints,
    signals: &[TradeSignal],
) -> std::result::Result<(), String> {
    if action.status != SignalStatus::NewEntry {
        return Ok(());
    }
    if !constraints.capital_budget.is_finite() || constraints.capital_budget <= 0.0 {
        return Err(format!(
            "approved strategy capital_budget {:.2} is invalid",
            constraints.capital_budget
        ));
    }
    if !constraints.max_symbol_allocation_pct.is_finite()
        || constraints.max_symbol_allocation_pct <= 0.0
    {
        return Err(format!(
            "approved strategy max_symbol_allocation_pct {:.4} is invalid",
            constraints.max_symbol_allocation_pct
        ));
    }

    let already_open = signals
        .iter()
        .filter(|signal| signal.status == SignalStatus::AlreadyOpen)
        .collect::<Vec<_>>();
    if already_open.len() >= constraints.max_open_positions {
        return Err(format!(
            "approved strategy already has {} open positions; max is {}",
            already_open.len(),
            constraints.max_open_positions
        ));
    }

    let open_same_symbol = already_open
        .iter()
        .filter(|signal| signal.symbol.eq_ignore_ascii_case(&action.symbol))
        .count();
    if open_same_symbol >= constraints.max_positions_per_symbol {
        return Err(format!(
            "approved strategy already has {} open positions for {}; max is {}",
            open_same_symbol, action.symbol, constraints.max_positions_per_symbol
        ));
    }

    let mut open_reserve = 0.0;
    let mut open_symbol_reserve = 0.0;
    for signal in already_open {
        let signal_risk = trade_signal_risk(signal).map_err(|reason| {
            format!(
                "already-open {} {} cannot be risked for portfolio constraints: {}",
                signal.symbol, signal.strategy, reason
            )
        })?;
        open_reserve += signal_risk.reserve;
        if signal.symbol.eq_ignore_ascii_case(&action.symbol) {
            open_symbol_reserve += signal_risk.reserve;
        }
    }

    let total_reserve_after_entry = open_reserve + action_risk.reserve;
    if total_reserve_after_entry > constraints.capital_budget {
        return Err(format!(
            "approved strategy total reserve {:.2} would exceed capital budget {:.2}",
            total_reserve_after_entry, constraints.capital_budget
        ));
    }

    let symbol_reserve_after_entry = open_symbol_reserve + action_risk.reserve;
    let symbol_cap = constraints.capital_budget * constraints.max_symbol_allocation_pct;
    if symbol_reserve_after_entry > symbol_cap {
        return Err(format!(
            "approved strategy {} reserve {:.2} would exceed symbol cap {:.2}",
            action.symbol, symbol_reserve_after_entry, symbol_cap
        ));
    }

    Ok(())
}

fn trade_signal_risk(action: &TradeSignal) -> std::result::Result<TradeSignalRisk, String> {
    if action.strategy == "wheel" {
        if let Some(short_put) = action.short_put.or(action.short_strike) {
            return Ok(TradeSignalRisk {
                reserve: short_put * 100.0,
                reserve_basis: "short_put_x100".to_owned(),
            });
        }
        let max_loss = action
            .max_loss
            .ok_or_else(|| "wheel missing max_loss for cash-secured reserve".to_owned())?;
        let entry_credit = action.entry_credit.ok_or_else(|| {
            "wheel missing short_put and entry_credit for cash-secured reserve".to_owned()
        })?;
        return Ok(TradeSignalRisk {
            reserve: max_loss + entry_credit.max(0.0) * 100.0,
            reserve_basis: "max_loss_plus_entry_credit_x100".to_owned(),
        });
    }
    let max_loss = action
        .max_loss
        .ok_or_else(|| format!("{} missing max_loss for reserve", action.strategy))?;
    if is_debit_spread_strategy(action.strategy.as_str()) {
        let order_max_loss = debit_spread_order_max_loss(action)?;
        return Ok(TradeSignalRisk {
            reserve: max_loss.max(order_max_loss),
            reserve_basis: "max_loss_and_order_debit".to_owned(),
        });
    }
    if is_credit_spread_strategy(action.strategy.as_str()) {
        let order_max_loss = credit_spread_order_max_loss(action)?;
        return Ok(TradeSignalRisk {
            reserve: max_loss.max(order_max_loss),
            reserve_basis: "max_loss_and_order_credit_width".to_owned(),
        });
    }
    Ok(TradeSignalRisk {
        reserve: max_loss,
        reserve_basis: "max_loss".to_owned(),
    })
}

fn debit_spread_order_max_loss(action: &TradeSignal) -> std::result::Result<f64, String> {
    let entry_credit = action
        .entry_credit
        .ok_or_else(|| format!("{} missing entry_credit for order risk", action.strategy))?;
    if !entry_credit.is_finite() || entry_credit >= 0.0 {
        return Err(format!(
            "{} entry_credit must be a negative debit for order risk",
            action.strategy
        ));
    }
    Ok(entry_credit.abs() * 100.0)
}

fn credit_spread_order_max_loss(action: &TradeSignal) -> std::result::Result<f64, String> {
    let entry_credit = action
        .entry_credit
        .ok_or_else(|| format!("{} missing entry_credit for order risk", action.strategy))?;
    if !entry_credit.is_finite() || entry_credit <= 0.0 {
        return Err(format!(
            "{} entry_credit must be a positive credit for order risk",
            action.strategy
        ));
    }
    let short_strike = action
        .short_strike
        .ok_or_else(|| format!("{} missing short_strike for order risk", action.strategy))?;
    let long_strike = action
        .long_strike
        .ok_or_else(|| format!("{} missing long_strike for order risk", action.strategy))?;
    validate_credit_spread_geometry(action.strategy.as_str(), short_strike, long_strike)
        .map_err(|err| err.to_string())?;
    let width = (long_strike - short_strike).abs();
    if entry_credit >= width {
        return Err(format!(
            "{} entry_credit {:.2} must be below strike width {:.2}",
            action.strategy, entry_credit, width
        ));
    }
    Ok((width - entry_credit) * 100.0)
}

fn trade_signal_with_risk(mut action: TradeSignal, action_risk: TradeSignalRisk) -> TradeSignal {
    action.reserve = Some(action_risk.reserve);
    action.reserve_basis = Some(action_risk.reserve_basis);
    action
}

fn assert_trade_signal_broker_supported(
    action: &TradeSignal,
    broker: &impl ExecutionBrokerView,
) -> anyhow::Result<()> {
    match action.strategy.as_str() {
        "put_credit_spread" | "call_credit_spread" => broker.assert_credit_spread_live_supported(),
        "put_debit_spread" | "call_debit_spread" => broker.assert_debit_spread_live_supported(),
        "wheel" => broker.assert_wheel_live_supported(),
        other => anyhow::bail!("strategy {other} is not enabled for execution"),
    }
}

fn execution_market_window_open_at(now_utc: chrono::DateTime<Utc>) -> bool {
    market_session_snapshot_at(now_utc).open
}

fn execution_market_date_at(now_utc: chrono::DateTime<Utc>) -> NaiveDate {
    market_session_snapshot_at(now_utc).date_et
}

fn execution_default_as_of(now_utc: chrono::DateTime<Utc>) -> NaiveDate {
    execution_market_date_at(now_utc)
}

fn market_session_snapshot_for_status(now_utc: chrono::DateTime<Utc>) -> MarketSessionSnapshot {
    market_session_snapshot_from_tradier_clock(now_utc)
        .unwrap_or_else(|| market_session_snapshot_at(now_utc))
}

fn market_session_snapshot_for_refresh(now_utc: chrono::DateTime<Utc>) -> MarketSessionSnapshot {
    match tradier_config_from_env() {
        Ok(config) => match TradierClient::new_with_timeout(config, StdDuration::from_secs(5))
            .and_then(|client| client.get_market_clock())
        {
            Ok(clock) => tradier_market_session_snapshot_from_clock(now_utc, &clock)
                .unwrap_or_else(|| market_session_snapshot_at(now_utc)),
            Err(err) => {
                let mut snapshot = market_session_snapshot_at(now_utc);
                snapshot.open = false;
                snapshot.source = "tradier_clock_error".to_owned();
                snapshot.reason = format!(
                    "Tradier market clock unavailable during configured signal refresh: {err}"
                );
                snapshot
            }
        },
        Err(_) => market_session_snapshot_at(now_utc),
    }
}

fn market_session_snapshot_from_tradier_clock(
    now_utc: chrono::DateTime<Utc>,
) -> Option<MarketSessionSnapshot> {
    let config = tradier_config_from_env().ok()?;
    let client = TradierClient::new_with_timeout(config, StdDuration::from_secs(5)).ok()?;
    let clock = client.get_market_clock().ok()?;
    tradier_market_session_snapshot_from_clock(now_utc, &clock)
}

fn tradier_market_session_snapshot_from_clock(
    now_utc: chrono::DateTime<Utc>,
    response: &TradierMarketClockResponse,
) -> Option<MarketSessionSnapshot> {
    let clock = response.clock.as_ref()?;
    let mut snapshot = market_session_snapshot_at(now_utc);
    snapshot.source = "tradier_clock".to_owned();
    snapshot.open = tradier_market_clock_open(clock);
    snapshot.reason = tradier_market_clock_reason(clock);
    Some(snapshot)
}

fn tradier_market_clock_open(clock: &TradierMarketClock) -> bool {
    [clock.state.as_deref(), clock.status.as_deref()]
        .into_iter()
        .flatten()
        .any(|value| value.eq_ignore_ascii_case("open"))
        || clock.description.as_deref().is_some_and(|description| {
            let description = description.to_ascii_lowercase();
            description.contains("market is open") || description == "open"
        })
}

fn tradier_market_clock_reason(clock: &TradierMarketClock) -> String {
    clock
        .description
        .clone()
        .or_else(|| clock.status.clone())
        .or_else(|| clock.state.clone())
        .unwrap_or_else(|| "Tradier market clock returned no status detail".to_owned())
}

fn market_session_snapshot_at(now_utc: chrono::DateTime<Utc>) -> MarketSessionSnapshot {
    let eastern_offset_hours = if us_eastern_dst_active(now_utc.date_naive()) {
        -4
    } else {
        -5
    };
    let now_et = now_utc + chrono::Duration::hours(eastern_offset_hours);
    let date_et = now_et.date_naive();
    let weekday = now_et.weekday().number_from_monday();
    let minute_et = now_et.hour() * 60 + now_et.minute();
    let close_minute_et = market_session_close_minute_et(date_et);
    let (open, reason) = if !(1..=5).contains(&weekday) {
        (false, "weekend".to_owned())
    } else if market_holiday_reason(date_et).is_some() {
        (
            false,
            market_holiday_reason(date_et).expect("checked holiday reason"),
        )
    } else if let Some(close_minute) = close_minute_et {
        if (570..close_minute).contains(&minute_et) {
            (true, "regular options-market window is open".to_owned())
        } else if minute_et < 570 {
            (
                false,
                "before configured options-market refresh window".to_owned(),
            )
        } else {
            (
                false,
                "after configured options-market refresh window".to_owned(),
            )
        }
    } else {
        (false, "market session is closed".to_owned())
    };
    MarketSessionSnapshot {
        checked_at: now_utc,
        open,
        reason,
        source: "local_us_options_calendar".to_owned(),
        date_et,
        minute_et,
        close_minute_et,
    }
}

fn market_session_close_minute_et(date: NaiveDate) -> Option<u32> {
    if market_holiday_reason(date).is_some() {
        return None;
    }
    if market_early_close(date) {
        Some(13 * 60)
    } else {
        Some(16 * 60)
    }
}

fn market_holiday_reason(date: NaiveDate) -> Option<String> {
    let year = date.year();
    let fixed_holidays = [
        (1, 1, "New Year's Day"),
        (6, 19, "Juneteenth"),
        (7, 4, "Independence Day"),
        (12, 25, "Christmas Day"),
    ];
    for (month, day, name) in fixed_holidays {
        if observed_fixed_holiday(year, month, day) == Some(date)
            || observed_fixed_holiday(year + 1, month, day) == Some(date)
        {
            return Some(format!("{name} market holiday"));
        }
    }
    if nth_weekday_of_month(year, 1, chrono::Weekday::Mon, 3) == Some(date) {
        return Some("Martin Luther King Jr. Day market holiday".to_owned());
    }
    if nth_weekday_of_month(year, 2, chrono::Weekday::Mon, 3) == Some(date) {
        return Some("Presidents Day market holiday".to_owned());
    }
    if easter_sunday(year).and_then(|date| date.checked_sub_signed(chrono::Duration::days(2)))
        == Some(date)
    {
        return Some("Good Friday market holiday".to_owned());
    }
    if last_weekday_of_month(year, 5, chrono::Weekday::Mon) == Some(date) {
        return Some("Memorial Day market holiday".to_owned());
    }
    if nth_weekday_of_month(year, 9, chrono::Weekday::Mon, 1) == Some(date) {
        return Some("Labor Day market holiday".to_owned());
    }
    if nth_weekday_of_month(year, 11, chrono::Weekday::Thu, 4) == Some(date) {
        return Some("Thanksgiving Day market holiday".to_owned());
    }
    None
}

fn observed_fixed_holiday(year: i32, month: u32, day: u32) -> Option<NaiveDate> {
    let date = NaiveDate::from_ymd_opt(year, month, day)?;
    match date.weekday() {
        chrono::Weekday::Sat => date.checked_sub_signed(chrono::Duration::days(1)),
        chrono::Weekday::Sun => date.checked_add_signed(chrono::Duration::days(1)),
        _ => Some(date),
    }
}

fn market_early_close(date: NaiveDate) -> bool {
    let year = date.year();
    nth_weekday_of_month(year, 11, chrono::Weekday::Thu, 4)
        .and_then(|thanksgiving| thanksgiving.checked_add_signed(chrono::Duration::days(1)))
        == Some(date)
        || christmas_eve_early_close(date)
        || independence_day_early_close(date)
}

fn christmas_eve_early_close(date: NaiveDate) -> bool {
    date.month() == 12
        && date.day() == 24
        && (1..=5).contains(&date.weekday().number_from_monday())
        && market_holiday_reason(date).is_none()
}

fn independence_day_early_close(date: NaiveDate) -> bool {
    date.month() == 7
        && date.day() == 3
        && (1..=5).contains(&date.weekday().number_from_monday())
        && market_holiday_reason(date).is_none()
}

fn easter_sunday(year: i32) -> Option<NaiveDate> {
    let a = year % 19;
    let b = year / 100;
    let c = year % 100;
    let d = b / 4;
    let e = b % 4;
    let f = (b + 8) / 25;
    let g = (b - f + 1) / 3;
    let h = (19 * a + b - d - g + 15) % 30;
    let i = c / 4;
    let k = c % 4;
    let l = (32 + 2 * e + 2 * i - h - k) % 7;
    let m = (a + 11 * h + 22 * l) / 451;
    let month = (h + l - 7 * m + 114) / 31;
    let day = ((h + l - 7 * m + 114) % 31) + 1;
    NaiveDate::from_ymd_opt(year, month as u32, day as u32)
}

fn us_eastern_dst_active(date: NaiveDate) -> bool {
    let year = date.year();
    let Some(start) = nth_weekday_of_month(year, 3, chrono::Weekday::Sun, 2) else {
        return false;
    };
    let Some(end) = nth_weekday_of_month(year, 11, chrono::Weekday::Sun, 1) else {
        return false;
    };
    date >= start && date < end
}

fn nth_weekday_of_month(
    year: i32,
    month: u32,
    weekday: chrono::Weekday,
    nth: u32,
) -> Option<NaiveDate> {
    let mut date = NaiveDate::from_ymd_opt(year, month, 1)?;
    let mut seen = 0;
    while date.month() == month {
        if date.weekday() == weekday {
            seen += 1;
            if seen == nth {
                return Some(date);
            }
        }
        date = date.succ_opt()?;
    }
    None
}

fn last_weekday_of_month(year: i32, month: u32, weekday: chrono::Weekday) -> Option<NaiveDate> {
    let mut date = if month == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)?
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1)?
    }
    .pred_opt()?;
    while date.month() == month {
        if date.weekday() == weekday {
            return Some(date);
        }
        date = date.pred_opt()?;
    }
    None
}

fn apply_broker_bridge(
    decision: &mut ExecutionDecision,
    broker: &ExecutionBrokerAdapter,
    robinhood_mcp_command: Option<&str>,
    order_ledger: Option<&Path>,
) -> Result<()> {
    if decision.status != "ready" || decision.mode == ExecutionMode::Monitor {
        return Ok(());
    }
    if decision.action_kind == Some(ExecutionActionKind::ManageOpen)
        && broker.kind() != BrokerKind::Tradier
    {
        decision.status = "blocked".to_owned();
        decision.reason =
            "open-position lifecycle management is only implemented for Tradier strategies"
                .to_owned();
        return Ok(());
    }
    if decision.mode == ExecutionMode::Live && !broker.live_orders_enabled() {
        decision.status = "blocked".to_owned();
        decision.reason =
            "live order placement is disabled until explicit rollout gates pass".to_owned();
        return Ok(());
    }
    if decision.mode == ExecutionMode::Live && !live_position_lifecycle_ready(decision) {
        decision.status = "blocked".to_owned();
        decision.reason =
            "live order placement is blocked because broker position reconciliation and exit lifecycle are not enabled for this broker/strategy".to_owned();
        return Ok(());
    }
    match broker.kind() {
        BrokerKind::Robinhood => {
            apply_robinhood_mcp_bridge(decision, robinhood_mcp_command, order_ledger)
        }
        BrokerKind::Tradier => apply_tradier_rest_bridge(decision, order_ledger),
    }
}

fn live_position_lifecycle_ready(decision: &ExecutionDecision) -> bool {
    if decision.broker != BrokerKind::Tradier {
        return false;
    }
    let Some(action) = decision.selected_signal.as_ref() else {
        return false;
    };
    if !matches!(
        action.strategy.as_str(),
        "call_debit_spread"
            | "put_debit_spread"
            | "call_credit_spread"
            | "put_credit_spread"
            | "wheel"
    ) {
        return false;
    }
    matches!(
        decision.action_kind,
        Some(ExecutionActionKind::OpenEntry | ExecutionActionKind::ManageOpen)
    )
}

fn apply_robinhood_mcp_bridge(
    decision: &mut ExecutionDecision,
    robinhood_mcp_command: Option<&str>,
    order_ledger: Option<&Path>,
) -> Result<()> {
    if decision.status != "ready" || decision.mode == ExecutionMode::Monitor {
        return Ok(());
    }
    let Some(command) = robinhood_mcp_command else {
        return Ok(());
    };
    let Some(action) = decision.selected_signal.clone() else {
        return Ok(());
    };

    let executor = RobinhoodMcpCommandExecutor::new(command);
    let review_request = robinhood_mcp_option_order_request("review_option_order", &action)?;
    let order_key = robinhood_mcp_order_key(&review_request);
    let review = executor.execute(&review_request)?;
    let review_ok = review.ok;
    decision.mcp_review = Some(review);
    if !review_ok {
        decision.status = "rejected".to_owned();
        decision.reason = "Robinhood MCP review_option_order rejected the order".to_owned();
        return Ok(());
    }
    if !robinhood_mcp_review_matches_order_key(decision.mcp_review.as_ref(), &order_key) {
        decision.status = "rejected".to_owned();
        decision.reason =
            "Robinhood MCP review did not echo the expected order_key with broker_preview_verified=true for the order intent".to_owned();
        return Ok(());
    }

    decision.broker_review_ok = true;
    if decision.mode != ExecutionMode::Live {
        decision.status = "reviewed".to_owned();
        decision.reason =
            "Robinhood MCP review_option_order succeeded; live placement was not requested"
                .to_owned();
        return Ok(());
    }
    if action.strategy == "wheel" {
        decision.status = "blocked".to_owned();
        decision.reason = "Robinhood MCP review succeeded, but autonomous wheel placement is blocked until broker buying-power, assignment, and position reconciliation are implemented".to_owned();
        return Ok(());
    }

    let place_request = robinhood_mcp_option_order_request("place_option_order", &action)?;
    let place_order_key = robinhood_mcp_order_key(&place_request);
    if place_order_key != order_key {
        decision.status = "blocked".to_owned();
        decision.reason =
            "review and place order keys diverged; refusing live placement".to_owned();
        return Ok(());
    }
    let Some(ledger_path) = order_ledger else {
        decision.status = "blocked".to_owned();
        decision.reason = "live placement requires a local execution order ledger".to_owned();
        return Ok(());
    };
    if execution_order_ledger_reserve_pending(
        ledger_path,
        &order_key,
        Some("Robinhood place request is about to be sent"),
    )? == ExecutionOrderLedgerReservation::AlreadyRecorded
    {
        decision.status = "already_submitted".to_owned();
        decision.reason =
            "matching Robinhood MCP order intent is already recorded in the local execution ledger"
                .to_owned();
        return Ok(());
    }
    let place = match executor.execute(&place_request) {
        Ok(place) => place,
        Err(err) => {
            decision.status = "submit_unknown".to_owned();
            decision.reason = format!(
                "Robinhood MCP place_option_order failed after local ledger reservation; check broker before retrying: {err}"
            );
            return Ok(());
        }
    };
    let place_ok = place.ok;
    decision.mcp_place = Some(place);
    if place_ok {
        execution_order_ledger_record_status(
            ledger_path,
            &order_key,
            "submitted",
            None,
            Some("Robinhood MCP place_option_order returned success"),
        )?;
        decision.status = "submitted".to_owned();
        decision.reason = "Robinhood MCP place_option_order returned success".to_owned();
    } else {
        execution_order_ledger_record_status(
            ledger_path,
            &order_key,
            "rejected",
            None,
            Some("Robinhood MCP place_option_order returned a rejection"),
        )?;
        decision.status = "rejected".to_owned();
        decision.reason = "Robinhood MCP place_option_order returned a rejection".to_owned();
    }
    Ok(())
}

fn robinhood_mcp_option_order_request(
    tool: &str,
    action: &TradeSignal,
) -> Result<RobinhoodMcpToolRequest> {
    let intent = trade_signal_order_intent(action)?;
    let arguments = robinhood_mcp_option_order_arguments(action, &intent)
        .with_context(|| format!("build Robinhood MCP {tool} arguments"))?;
    Ok(RobinhoodMcpToolRequest {
        server: "robinhood-trading".to_owned(),
        tool: tool.to_owned(),
        arguments,
    })
}

fn trade_signal_order_intent(action: &TradeSignal) -> Result<OptionOrderIntent> {
    if action.status != SignalStatus::NewEntry {
        anyhow::bail!("only same-day new_entry actions are orderable");
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
            validate_debit_spread_order_bounds(
                action,
                "put_debit_spread",
                short_strike,
                long_strike,
                entry_credit.abs(),
            )?;
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
            validate_debit_spread_order_bounds(
                action,
                "call_debit_spread",
                short_strike,
                long_strike,
                entry_credit.abs(),
            )?;
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
        "put_credit_spread" | "call_credit_spread" => {
            if entry_credit <= 0.0 {
                anyhow::bail!(
                    "{} requires a positive entry_credit credit",
                    action.strategy
                );
            }
            let short_strike = require_option_f64(action.short_strike, "short_strike")?;
            let long_strike = require_option_f64(action.long_strike, "long_strike")?;
            validate_credit_spread_order_bounds(
                action,
                action.strategy.as_str(),
                short_strike,
                long_strike,
                entry_credit,
            )?;
            let (short_key, long_key) = trade_signal_credit_spread_keys(action)?;
            Ok(credit_spread_open_intent(
                short_key,
                long_key,
                quantity,
                limit_price,
                action.strategy.clone(),
            )?)
        }
        other => anyhow::bail!("strategy {other} is not orderable through the execution broker"),
    }
}

fn is_debit_spread_strategy(strategy: &str) -> bool {
    matches!(strategy, "call_debit_spread" | "put_debit_spread")
}

fn is_credit_spread_strategy(strategy: &str) -> bool {
    matches!(strategy, "call_credit_spread" | "put_credit_spread")
}

fn is_vertical_spread_strategy(strategy: &str) -> bool {
    is_debit_spread_strategy(strategy) || is_credit_spread_strategy(strategy)
}

fn trade_signal_debit_spread_keys(action: &TradeSignal) -> Result<(OptionKey, OptionKey)> {
    let symbol = require_action_field(action.symbol.as_str(), "symbol")?;
    let expiration = require_option_string(action.expiration.as_deref(), "expiration")?
        .parse::<NaiveDate>()
        .with_context(|| format!("parse expiration for {}", action.symbol))?;
    let short_strike = require_option_f64(action.short_strike, "short_strike")?;
    let long_strike = require_option_f64(action.long_strike, "long_strike")?;
    let (right, strategy) = match action.strategy.as_str() {
        "put_debit_spread" => (OptionRight::Put, "put_debit_spread"),
        "call_debit_spread" => (OptionRight::Call, "call_debit_spread"),
        other => anyhow::bail!("strategy {other} is not a debit spread"),
    };
    validate_debit_spread_geometry(strategy, short_strike, long_strike)?;
    Ok((
        OptionKey::new(
            symbol,
            expiration,
            decimal_from_f64(long_strike, "long_strike")?,
            right.clone(),
        ),
        OptionKey::new(
            symbol,
            expiration,
            decimal_from_f64(short_strike, "short_strike")?,
            right,
        ),
    ))
}

fn trade_signal_credit_spread_keys(action: &TradeSignal) -> Result<(OptionKey, OptionKey)> {
    let symbol = require_action_field(action.symbol.as_str(), "symbol")?;
    let expiration = require_option_string(action.expiration.as_deref(), "expiration")?
        .parse::<NaiveDate>()
        .with_context(|| format!("parse expiration for {}", action.symbol))?;
    let short_strike = require_option_f64(action.short_strike, "short_strike")?;
    let long_strike = require_option_f64(action.long_strike, "long_strike")?;
    let (right, strategy) = match action.strategy.as_str() {
        "put_credit_spread" => (OptionRight::Put, "put_credit_spread"),
        "call_credit_spread" => (OptionRight::Call, "call_credit_spread"),
        other => anyhow::bail!("strategy {other} is not a credit spread"),
    };
    validate_credit_spread_geometry(strategy, short_strike, long_strike)?;
    Ok((
        OptionKey::new(
            symbol,
            expiration,
            decimal_from_f64(short_strike, "short_strike")?,
            right.clone(),
        ),
        OptionKey::new(
            symbol,
            expiration,
            decimal_from_f64(long_strike, "long_strike")?,
            right,
        ),
    ))
}

fn trade_signal_vertical_spread_keys(action: &TradeSignal) -> Result<(OptionKey, OptionKey)> {
    if is_debit_spread_strategy(action.strategy.as_str()) {
        return trade_signal_debit_spread_keys(action);
    }
    if is_credit_spread_strategy(action.strategy.as_str()) {
        let (short_key, long_key) = trade_signal_credit_spread_keys(action)?;
        return Ok((long_key, short_key));
    }
    anyhow::bail!("strategy {} is not a vertical spread", action.strategy)
}

fn validate_live_management_signal_shape(action: &TradeSignal) -> Result<()> {
    match action.strategy.as_str() {
        "call_debit_spread" | "put_debit_spread" => {
            trade_signal_debit_spread_keys(action)?;
            Ok(())
        }
        "call_credit_spread" | "put_credit_spread" => {
            trade_signal_credit_spread_keys(action)?;
            Ok(())
        }
        "wheel" => {
            trade_signal_wheel_short_put_key(action)?;
            Ok(())
        }
        other => anyhow::bail!("strategy {other} is not manageable through live execution"),
    }
}

fn trade_signal_wheel_short_put_key(action: &TradeSignal) -> Result<OptionKey> {
    if action.strategy != "wheel" {
        anyhow::bail!("strategy {} is not a wheel", action.strategy);
    }
    let symbol = require_action_field(action.symbol.as_str(), "symbol")?;
    let expiration = require_option_string(action.expiration.as_deref(), "expiration")?
        .parse::<NaiveDate>()
        .with_context(|| format!("parse wheel put expiration for {}", action.symbol))?;
    let short_put = require_option_f64(
        action.short_put.or(action.short_strike),
        "short_put/short_strike",
    )?;
    Ok(OptionKey::new(
        symbol,
        expiration,
        decimal_from_f64(short_put, "short_put")?,
        OptionRight::Put,
    ))
}

fn trade_signal_wheel_covered_call_key(
    action: &TradeSignal,
    as_of: NaiveDate,
) -> Result<OptionKey> {
    if action.strategy != "wheel" {
        anyhow::bail!("strategy {} is not a wheel", action.strategy);
    }
    let symbol = require_action_field(action.symbol.as_str(), "symbol")?;
    let expiration = trade_signal_date(
        action,
        action.wheel_covered_call_expiration.as_deref(),
        "wheel_covered_call_expiration",
    )
    .map_err(anyhow::Error::msg)?;
    if expiration <= as_of {
        anyhow::bail!(
            "wheel assigned-stock management requires a future covered-call expiration; got {expiration} for as_of {as_of}"
        );
    }
    let strike = require_option_f64(
        action.wheel_covered_call_strike,
        "wheel_covered_call_strike",
    )?;
    if strike <= 0.0 || !strike.is_finite() {
        anyhow::bail!("wheel covered-call strike must be positive and finite");
    }
    Ok(OptionKey::new(
        symbol,
        expiration,
        decimal_from_f64(strike, "covered_call_strike")?,
        OptionRight::Call,
    ))
}

fn trade_signal_wheel_covered_call_key_if_exported(
    action: &TradeSignal,
    as_of: NaiveDate,
) -> Result<Option<OptionKey>> {
    match (
        action.wheel_covered_call_expiration.as_deref(),
        action.wheel_covered_call_strike,
    ) {
        (None, None) => Ok(None),
        (Some(_), Some(_)) => trade_signal_wheel_covered_call_key(action, as_of).map(Some),
        _ => {
            anyhow::bail!(
                "wheel assigned-stock management has incomplete covered-call target fields"
            )
        }
    }
}

fn validate_debit_spread_geometry(
    strategy: &str,
    short_strike: f64,
    long_strike: f64,
) -> Result<()> {
    match strategy {
        "put_debit_spread" if long_strike <= short_strike => {
            anyhow::bail!("{strategy} requires long put strike above short put strike");
        }
        "call_debit_spread" if long_strike >= short_strike => {
            anyhow::bail!("{strategy} requires long call strike below short call strike");
        }
        _ => Ok(()),
    }
}

fn validate_credit_spread_geometry(
    strategy: &str,
    short_strike: f64,
    long_strike: f64,
) -> Result<()> {
    match strategy {
        "put_credit_spread" if short_strike <= long_strike => {
            anyhow::bail!("{strategy} requires short put strike above long put strike");
        }
        "call_credit_spread" if short_strike >= long_strike => {
            anyhow::bail!("{strategy} requires short call strike below long call strike");
        }
        _ => Ok(()),
    }
}

fn validate_debit_spread_order_bounds(
    action: &TradeSignal,
    strategy: &str,
    short_strike: f64,
    long_strike: f64,
    entry_debit: f64,
) -> Result<()> {
    validate_debit_spread_geometry(strategy, short_strike, long_strike)?;
    let width = (long_strike - short_strike).abs();
    if entry_debit > width + 0.01 {
        anyhow::bail!(
            "{} limit debit {:.2} exceeds strike width {:.2}",
            strategy,
            entry_debit,
            width
        );
    }
    if let Some(exported_width) = action.width
        && exported_width.is_finite()
        && exported_width > 0.0
        && (exported_width - width).abs() > 0.01
    {
        anyhow::bail!(
            "{} width {:.2} does not match strike width {:.2}",
            strategy,
            exported_width,
            width
        );
    }
    if let Some(max_loss) = action.max_loss
        && (entry_debit * 100.0 - max_loss).abs() > 1.0
    {
        anyhow::bail!(
            "{} max_loss {:.2} does not match order debit risk {:.2}",
            strategy,
            max_loss,
            entry_debit * 100.0
        );
    }
    Ok(())
}

fn validate_credit_spread_order_bounds(
    action: &TradeSignal,
    strategy: &str,
    short_strike: f64,
    long_strike: f64,
    entry_credit: f64,
) -> Result<()> {
    validate_credit_spread_geometry(strategy, short_strike, long_strike)?;
    let width = (long_strike - short_strike).abs();
    if !entry_credit.is_finite() || entry_credit <= 0.0 {
        anyhow::bail!("{strategy} limit credit must be positive and finite");
    }
    if entry_credit >= width {
        anyhow::bail!(
            "{} limit credit {:.2} must be below strike width {:.2}",
            strategy,
            entry_credit,
            width
        );
    }
    if let Some(exported_width) = action.width
        && exported_width.is_finite()
        && exported_width > 0.0
        && (exported_width - width).abs() > 0.01
    {
        anyhow::bail!(
            "{} width {:.2} does not match strike width {:.2}",
            strategy,
            exported_width,
            width
        );
    }
    if let Some(max_loss) = action.max_loss {
        let order_max_loss = (width - entry_credit) * 100.0;
        if (order_max_loss - max_loss).abs() > 1.0 {
            anyhow::bail!(
                "{} max_loss {:.2} does not match credit-spread risk {:.2}",
                strategy,
                max_loss,
                order_max_loss
            );
        }
    }
    Ok(())
}

fn robinhood_mcp_option_order_arguments(
    action: &TradeSignal,
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
            "status": action.status.as_str(),
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
        "broker": "robinhood",
        "server": request.server,
        "arguments": request.arguments,
    }))
    .expect("Robinhood MCP order key serialization should be infallible")
}

fn robinhood_mcp_review_matches_order_key(
    review: Option<&RobinhoodMcpToolResponse>,
    order_key: &str,
) -> bool {
    let Some(raw) = review.map(|review| &review.raw) else {
        return false;
    };
    raw.get("order_key").and_then(|value| value.as_str()) == Some(order_key)
        && raw
            .get("broker_preview_verified")
            .and_then(|value| value.as_bool())
            == Some(true)
}

fn apply_tradier_rest_bridge(
    decision: &mut ExecutionDecision,
    order_ledger: Option<&Path>,
) -> Result<()> {
    if decision.status != "ready" || decision.mode == ExecutionMode::Monitor {
        return Ok(());
    }
    apply_tradier_rest_bridge_with_config_result(decision, order_ledger, tradier_config_from_env())
}

fn apply_tradier_rest_bridge_with_config_result(
    decision: &mut ExecutionDecision,
    order_ledger: Option<&Path>,
    config: Result<TradierConfig>,
) -> Result<()> {
    let config = match config {
        Ok(config) => config,
        Err(err) => {
            decision.status = "blocked".to_owned();
            decision.reason = format!("Tradier credentials are not configured: {err}");
            return Ok(());
        }
    };
    apply_tradier_rest_bridge_with_config(decision, order_ledger, config)
}

fn apply_tradier_rest_bridge_with_config(
    decision: &mut ExecutionDecision,
    order_ledger: Option<&Path>,
    config: TradierConfig,
) -> Result<()> {
    if decision.action_kind == Some(ExecutionActionKind::ManageOpen) {
        return apply_tradier_management_bridge_with_config(decision, order_ledger, config);
    }
    let Some(action) = decision.selected_signal.clone() else {
        return Ok(());
    };
    let payload = match tradier_execution_order_payload(&action) {
        Ok(payload) => payload,
        Err(err) => {
            decision.status = "blocked".to_owned();
            decision.reason = format!("Tradier order shape rejected before preview: {err}");
            return Ok(());
        }
    };
    let review_ledger_path = if decision.mode == ExecutionMode::Review {
        order_ledger
    } else {
        None
    };
    let live_ledger_path = if decision.mode == ExecutionMode::Live {
        match order_ledger {
            Some(path) => Some(path),
            None => {
                decision.status = "blocked".to_owned();
                decision.reason =
                    "live placement requires a local execution order ledger".to_owned();
                return Ok(());
            }
        }
    } else {
        None
    };
    if !apply_tradier_management_preflight_before_entry(decision, order_ledger, config.clone())? {
        return Ok(());
    }
    let client = match TradierClient::new(config.clone()) {
        Ok(client) => client,
        Err(err) => {
            decision.status = "blocked".to_owned();
            decision.reason = format!("Tradier client initialization failed: {err}");
            return Ok(());
        }
    };
    let order_key = tradier_order_key(&config, &payload, &action);
    if let Some(ledger_path) = review_ledger_path
        && let Some(entry) = execution_order_ledger_entry_with_statuses(
            ledger_path,
            &order_key,
            &["reviewed", "rejected", "pending_unknown", "submitted"],
        )?
    {
        apply_tradier_ledger_entry_to_decision(decision, &entry);
        return Ok(());
    }
    if let Some(ledger_path) = live_ledger_path
        && let Some(entry) = execution_order_ledger_blocking_entry(ledger_path, &order_key)?
    {
        apply_tradier_ledger_entry_to_decision(decision, &entry);
        return Ok(());
    }
    if decision.mode == ExecutionMode::Live
        && let Err(err) = tradier_assert_market_open(&client)
    {
        decision.status = "blocked".to_owned();
        decision.reason = format!("Tradier market clock blocks live execution: {err}");
        return Ok(());
    }
    if let Err(err) = tradier_assert_buying_power(&client, &action, &decision.risk) {
        decision.status = "blocked".to_owned();
        decision.reason = format!("Tradier buying-power precheck failed: {err}");
        return Ok(());
    }
    if action.strategy == "wheel"
        && let Err(err) = tradier_assert_wheel_broker_state(&client, &action, &decision.risk)
    {
        decision.status = "blocked".to_owned();
        decision.reason = format!("Tradier wheel broker-state precheck failed: {err}");
        return Ok(());
    }
    if action.strategy == "wheel" {
        match tradier_validate_current_wheel_entry_quote(&client, &payload, decision.mode) {
            Ok(quotes) => decision.tradier_quote = Some(quotes),
            Err(err) => {
                decision.status = "blocked".to_owned();
                decision.reason = format!("Tradier wheel quote validation failed: {err}");
                return Ok(());
            }
        }
    }
    if is_vertical_spread_strategy(action.strategy.as_str())
        && let Err(err) = tradier_assert_vertical_spread_lifecycle_flat(&client, &action)
    {
        decision.status = "blocked".to_owned();
        decision.reason = format!("Tradier duplicate-exposure precheck failed: {err}");
        return Ok(());
    }
    if is_debit_spread_strategy(action.strategy.as_str()) {
        match tradier_validate_current_debit_quote(&client, &payload, &action, decision.mode) {
            Ok(quotes) => decision.tradier_quote = Some(quotes),
            Err(err) => {
                decision.status = "blocked".to_owned();
                decision.reason = format!("Tradier quote validation failed: {err}");
                return Ok(());
            }
        }
    }
    if is_credit_spread_strategy(action.strategy.as_str()) {
        match tradier_validate_current_credit_quote(&client, &payload, &action, decision.mode) {
            Ok(quotes) => decision.tradier_quote = Some(quotes),
            Err(err) => {
                decision.status = "blocked".to_owned();
                decision.reason = format!("Tradier quote validation failed: {err}");
                return Ok(());
            }
        }
    }
    let preview = match client.preview_order(&payload) {
        Ok(response) => response,
        Err(err) => {
            decision.status = "blocked".to_owned();
            decision.reason = format!("Tradier preview request failed: {err}");
            return Ok(());
        }
    };
    let preview_result = tradier_preview_accepted(&preview);
    decision.tradier_preview = Some(preview);
    if let Err(reason) = preview_result {
        if let Some(ledger_path) = review_ledger_path.or(live_ledger_path) {
            execution_order_ledger_record_status(
                ledger_path,
                &order_key,
                tradier_preview_rejection_ledger_status(&payload),
                None,
                Some(reason.as_str()),
            )?;
        }
        decision.status = "rejected".to_owned();
        decision.reason = format!("Tradier preview rejected the order: {reason}");
        return Ok(());
    }

    decision.broker_review_ok = true;
    if decision.mode != ExecutionMode::Live {
        if let Some(ledger_path) = review_ledger_path {
            execution_order_ledger_record_status(
                ledger_path,
                &order_key,
                "reviewed",
                None,
                Some("Tradier preview succeeded"),
            )?;
        }
        decision.status = "reviewed".to_owned();
        decision.reason = "Tradier preview succeeded; live placement was not requested".to_owned();
        return Ok(());
    }

    if let Some(ledger_path) = live_ledger_path
        && execution_order_ledger_reserve_pending(
            ledger_path,
            &order_key,
            Some("Tradier place request is about to be sent"),
        )? == ExecutionOrderLedgerReservation::AlreadyRecorded
    {
        decision.status = "already_submitted".to_owned();
        decision.reason =
            "matching Tradier order intent is already recorded in the local execution ledger"
                .to_owned();
        return Ok(());
    }
    let place = match client.place_order(&payload) {
        Ok(response) => response,
        Err(err) => {
            decision.status = "submit_unknown".to_owned();
            decision.reason = format!(
                "Tradier place request transport failed after local ledger reservation; check Tradier before retrying: {err}"
            );
            return Ok(());
        }
    };
    let place_result = tradier_place_accepted(&place);
    decision.tradier_place = Some(place);
    match place_result {
        Ok(order_id) => {
            let confirmed_status = match tradier_confirm_order_after_place(&client, &order_id) {
                Ok(status) => status,
                Err(err) => {
                    if let Some(ledger_path) = live_ledger_path {
                        execution_order_ledger_record_status(
                            ledger_path,
                            &order_key,
                            "pending_unknown",
                            Some(order_id.as_str()),
                            Some(err.to_string().as_str()),
                        )?;
                    }
                    decision.status = "submit_unknown".to_owned();
                    decision.reason = format!(
                        "Tradier place returned order id {order_id}, but post-submit confirmation failed; check Tradier before retrying: {err}"
                    );
                    return Ok(());
                }
            };
            if let Some(ledger_path) = live_ledger_path {
                let confirmed_reason =
                    format!("Tradier post-submit status confirmed as {confirmed_status}");
                execution_order_ledger_record_status(
                    ledger_path,
                    &order_key,
                    "submitted",
                    Some(order_id.as_str()),
                    Some(confirmed_reason.as_str()),
                )?;
            }
            decision.status = "submitted".to_owned();
            decision.reason = format!(
                "Tradier place order returned accepted order id {order_id} with confirmed status {confirmed_status}; broker lifecycle still requires monitoring"
            );
        }
        Err(reason) => {
            if let Some(ledger_path) = live_ledger_path {
                execution_order_ledger_record_status(
                    ledger_path,
                    &order_key,
                    "rejected",
                    None,
                    Some(reason.as_str()),
                )?;
            }
            decision.status = "rejected".to_owned();
            decision.reason = format!("Tradier place order returned a rejection: {reason}");
        }
    }
    Ok(())
}

fn apply_tradier_management_preflight_before_entry(
    decision: &mut ExecutionDecision,
    order_ledger: Option<&Path>,
    config: TradierConfig,
) -> Result<bool> {
    if decision.action_kind != Some(ExecutionActionKind::OpenEntry)
        || decision.management_signals.is_empty()
    {
        return Ok(true);
    }
    let mut management_decision = decision.clone();
    management_decision.action_kind = Some(ExecutionActionKind::ManageOpen);
    management_decision.selected_signal = management_decision.management_signals.first().cloned();
    management_decision.tradier_quote = None;
    management_decision.tradier_preview = None;
    management_decision.tradier_place = None;
    apply_tradier_management_bridge_with_config(&mut management_decision, order_ledger, config)?;
    match management_decision.status.as_str() {
        "holding" | "no_position" => Ok(true),
        _ => {
            *decision = management_decision;
            Ok(false)
        }
    }
}

fn apply_tradier_management_bridge_with_config(
    decision: &mut ExecutionDecision,
    order_ledger: Option<&Path>,
    config: TradierConfig,
) -> Result<()> {
    let Some(action) = decision
        .selected_signal
        .as_ref()
        .or_else(|| decision.management_signals.first())
    else {
        return Ok(());
    };
    match action.strategy.as_str() {
        "call_debit_spread" | "put_debit_spread" | "call_credit_spread" | "put_credit_spread" => {
            apply_tradier_vertical_spread_management_bridge_with_config(
                decision,
                order_ledger,
                config,
            )
        }
        "wheel" => {
            apply_tradier_wheel_management_bridge_with_config(decision, order_ledger, config)
        }
        other => {
            decision.status = "blocked".to_owned();
            decision.reason = format!("Tradier has no management bridge for strategy {other}");
            Ok(())
        }
    }
}

fn apply_tradier_vertical_spread_management_bridge_with_config(
    decision: &mut ExecutionDecision,
    order_ledger: Option<&Path>,
    config: TradierConfig,
) -> Result<()> {
    let actions = tradier_vertical_spread_management_actions(decision);
    if actions.is_empty() {
        return Ok(());
    }
    let client = match TradierClient::new(config.clone()) {
        Ok(client) => client,
        Err(err) => {
            decision.status = "blocked".to_owned();
            decision.reason = format!("Tradier client initialization failed: {err}");
            return Ok(());
        }
    };
    let review_ledger_path = if decision.mode == ExecutionMode::Review {
        order_ledger
    } else {
        None
    };
    let live_ledger_path = if decision.mode == ExecutionMode::Live {
        match order_ledger {
            Some(path) => Some(path),
            None => {
                decision.status = "blocked".to_owned();
                decision.reason =
                    "live placement requires a local execution order ledger".to_owned();
                return Ok(());
            }
        }
    } else {
        None
    };
    if decision.mode == ExecutionMode::Live
        && let Err(err) = tradier_assert_market_open(&client)
    {
        decision.status = "blocked".to_owned();
        decision.reason = format!("Tradier market clock blocks live execution: {err}");
        return Ok(());
    }
    let allowed_position_symbols =
        match tradier_vertical_spread_management_position_symbols(&actions) {
            Ok(symbols) => symbols,
            Err(err) => {
                decision.status = "blocked".to_owned();
                decision.reason = format!("Tradier management position allow-list failed: {err}");
                return Ok(());
            }
        };

    let positions = match client.get_positions() {
        Ok(response) if response.ok => response.positions,
        Ok(response) => {
            decision.status = "blocked".to_owned();
            decision.reason = format!(
                "Tradier positions request failed: {}",
                response
                    .error
                    .unwrap_or_else(|| "Tradier positions request failed".to_owned())
            );
            return Ok(());
        }
        Err(err) => {
            decision.status = "blocked".to_owned();
            decision.reason = format!("Tradier positions request failed: {err}");
            return Ok(());
        }
    };
    let orders = match client.get_orders() {
        Ok(response) if response.ok => response.orders,
        Ok(response) => {
            decision.status = "blocked".to_owned();
            decision.reason = format!(
                "Tradier orders request failed: {}",
                response
                    .error
                    .unwrap_or_else(|| "Tradier orders request failed".to_owned())
            );
            return Ok(());
        }
        Err(err) => {
            decision.status = "blocked".to_owned();
            decision.reason = format!("Tradier orders request failed: {err}");
            return Ok(());
        }
    };

    let mut open_positions_checked = 0_usize;
    let mut hold_reasons = Vec::new();
    for action in actions {
        let state = match tradier_vertical_spread_lifecycle_state_with_allowed_positions(
            &action,
            &positions,
            &orders,
            Some(&allowed_position_symbols),
        ) {
            Ok(state) => state,
            Err(err) => {
                decision.status = "blocked".to_owned();
                decision.reason = format!("Tradier vertical-spread lifecycle check failed: {err}");
                return Ok(());
            }
        };
        match state {
            TradierDebitSpreadLifecycleState::Flat => {
                hold_reasons.push(format!(
                    "{} has no matching Tradier open position",
                    trade_signal_label(&action)
                ));
            }
            TradierDebitSpreadLifecycleState::ActiveOrder { id, status } => {
                decision.status = "blocked".to_owned();
                decision.reason = format!(
                    "active Tradier vertical-spread order id {:?} status {:?} blocks lifecycle management",
                    id, status
                );
                return Ok(());
            }
            TradierDebitSpreadLifecycleState::Inconsistent { reason } => {
                decision.status = "blocked".to_owned();
                decision.reason =
                    format!("inconsistent Tradier vertical-spread lifecycle state: {reason}");
                return Ok(());
            }
            TradierDebitSpreadLifecycleState::AssignedShortLeg {
                right,
                long_quantity,
                stock_quantity,
            } => {
                decision.status = "blocked".to_owned();
                decision.reason = format!(
                    "Tradier vertical-spread short {} appears assigned: stock quantity {:.4} and long hedge quantity {:.4}; manual broker assignment recovery is required before autonomous lifecycle management continues",
                    option_right_value(&right),
                    stock_quantity,
                    long_quantity
                );
                return Ok(());
            }
            TradierDebitSpreadLifecycleState::Open { quantity } => {
                open_positions_checked += 1;
                if quantity != 1 {
                    decision.status = "blocked".to_owned();
                    decision.reason = format!(
                        "Tradier vertical-spread quantity {quantity} requires manual management; live signal quantity model supports one spread per action"
                    );
                    return Ok(());
                }
                let time_exit_reason =
                    match live_vertical_spread_time_exit_reason(&action, decision.as_of) {
                        Ok(reason) => reason,
                        Err(err) => {
                            decision.status = "blocked".to_owned();
                            decision.reason =
                                format!("live vertical-spread exit rule evaluation failed: {err}");
                            return Ok(());
                        }
                    };
                if is_credit_spread_strategy(action.strategy.as_str()) {
                    let (quotes, exit_debit) = match tradier_validate_current_credit_exit_quote(
                        &client,
                        &action,
                        decision.mode,
                    ) {
                        Ok(result) => result,
                        Err(err) => {
                            decision.status = "blocked".to_owned();
                            decision.reason = if let Some(reason) = time_exit_reason {
                                format!(
                                    "Tradier credit-spread close triggered {reason}, but exit quote validation failed: {err}"
                                )
                            } else {
                                format!("Tradier exit quote validation failed: {err}")
                            };
                            return Ok(());
                        }
                    };
                    decision.tradier_quote = Some(quotes);
                    let exit_plan =
                        match live_credit_spread_exit_plan(&action, exit_debit, decision.as_of) {
                            Ok(plan) => plan,
                            Err(err) => {
                                decision.status = "blocked".to_owned();
                                decision.reason = format!(
                                    "live credit-spread exit rule evaluation failed: {err}"
                                );
                                return Ok(());
                            }
                        };
                    match exit_plan {
                        CreditSpreadExitPlan::Hold { reason } => {
                            hold_reasons.push(format!("{}: {reason}", trade_signal_label(&action)));
                        }
                        CreditSpreadExitPlan::Close {
                            reason,
                            limit_debit,
                        } => {
                            if !limit_debit.is_finite() || limit_debit <= 0.0 {
                                decision.status = "blocked".to_owned();
                                decision.reason = format!(
                                    "Tradier credit-spread close triggered {reason}, but conservative exit debit {:.2} is not positive; manual close/expiry/assignment-risk review is required before autonomous lifecycle management continues",
                                    limit_debit
                                );
                                return Ok(());
                            }
                            let payload = match tradier_multileg_credit_close_payload(
                                &action,
                                limit_debit,
                                quantity,
                            ) {
                                Ok(payload) => payload,
                                Err(err) => {
                                    decision.status = "blocked".to_owned();
                                    decision.reason = format!(
                                        "Tradier close order shape rejected before preview: {err}"
                                    );
                                    return Ok(());
                                }
                            };
                            decision.selected_signal = Some(action.clone());
                            decision.action_kind = Some(ExecutionActionKind::ManageOpen);
                            return apply_tradier_payload_preview_and_maybe_place(
                                decision,
                                &client,
                                &config,
                                &payload,
                                &action,
                                review_ledger_path,
                                live_ledger_path,
                                &orders,
                                format!("Tradier credit-spread close triggered by {reason}")
                                    .as_str(),
                            );
                        }
                    }
                    continue;
                }
                let (quotes, exit_credit) = match tradier_validate_current_debit_exit_quote(
                    &client,
                    &action,
                    decision.mode,
                ) {
                    Ok(result) => result,
                    Err(err) => {
                        decision.status = "blocked".to_owned();
                        decision.reason = if let Some(reason) = time_exit_reason {
                            format!(
                                "Tradier debit-spread close triggered {reason}, but exit quote validation failed: {err}"
                            )
                        } else {
                            format!("Tradier exit quote validation failed: {err}")
                        };
                        return Ok(());
                    }
                };
                decision.tradier_quote = Some(quotes);
                let exit_plan =
                    match live_debit_spread_exit_plan(&action, exit_credit, decision.as_of) {
                        Ok(plan) => plan,
                        Err(err) => {
                            decision.status = "blocked".to_owned();
                            decision.reason =
                                format!("live debit-spread exit rule evaluation failed: {err}");
                            return Ok(());
                        }
                    };
                match exit_plan {
                    DebitSpreadExitPlan::Hold { reason } => {
                        hold_reasons.push(format!("{}: {reason}", trade_signal_label(&action)));
                    }
                    DebitSpreadExitPlan::Close {
                        reason,
                        limit_credit,
                    } => {
                        if !limit_credit.is_finite() || limit_credit <= 0.0 {
                            decision.status = "blocked".to_owned();
                            decision.reason = format!(
                                "Tradier debit-spread close triggered {reason}, but conservative exit credit {:.2} is not positive; manual close/expiry/assignment-risk review is required before autonomous lifecycle management continues",
                                limit_credit
                            );
                            return Ok(());
                        }
                        let payload = match tradier_multileg_debit_close_payload(
                            &action,
                            limit_credit,
                            quantity,
                        ) {
                            Ok(payload) => payload,
                            Err(err) => {
                                decision.status = "blocked".to_owned();
                                decision.reason = format!(
                                    "Tradier close order shape rejected before preview: {err}"
                                );
                                return Ok(());
                            }
                        };
                        decision.selected_signal = Some(action.clone());
                        decision.action_kind = Some(ExecutionActionKind::ManageOpen);
                        return apply_tradier_payload_preview_and_maybe_place(
                            decision,
                            &client,
                            &config,
                            &payload,
                            &action,
                            review_ledger_path,
                            live_ledger_path,
                            &orders,
                            format!("Tradier debit-spread close triggered by {reason}").as_str(),
                        );
                    }
                }
            }
        }
    }
    if open_positions_checked == 0 {
        decision.status = "no_position".to_owned();
        decision.reason = format!(
            "Tradier has no matching open vertical-spread position for {} exported management signal(s)",
            hold_reasons.len()
        );
    } else {
        decision.status = "holding".to_owned();
        decision.reason = format!(
            "Tradier lifecycle checked {open_positions_checked} open vertical spread(s); no exit rule fired: {}",
            hold_reasons.join("; ")
        );
    }
    Ok(())
}

fn apply_tradier_wheel_management_bridge_with_config(
    decision: &mut ExecutionDecision,
    order_ledger: Option<&Path>,
    config: TradierConfig,
) -> Result<()> {
    let actions = tradier_wheel_management_actions(decision);
    if actions.is_empty() {
        return Ok(());
    }
    let client = match TradierClient::new(config.clone()) {
        Ok(client) => client,
        Err(err) => {
            decision.status = "blocked".to_owned();
            decision.reason = format!("Tradier client initialization failed: {err}");
            return Ok(());
        }
    };
    let review_ledger_path = if decision.mode == ExecutionMode::Review {
        order_ledger
    } else {
        None
    };
    let live_ledger_path = if decision.mode == ExecutionMode::Live {
        match order_ledger {
            Some(path) => Some(path),
            None => {
                decision.status = "blocked".to_owned();
                decision.reason =
                    "live placement requires a local execution order ledger".to_owned();
                return Ok(());
            }
        }
    } else {
        None
    };
    if decision.mode == ExecutionMode::Live
        && let Err(err) = tradier_assert_market_open(&client)
    {
        decision.status = "blocked".to_owned();
        decision.reason = format!("Tradier market clock blocks live execution: {err}");
        return Ok(());
    }

    let positions = match client.get_positions() {
        Ok(response) if response.ok => response.positions,
        Ok(response) => {
            decision.status = "blocked".to_owned();
            decision.reason = format!(
                "Tradier positions request failed: {}",
                response
                    .error
                    .unwrap_or_else(|| "Tradier positions request failed".to_owned())
            );
            return Ok(());
        }
        Err(err) => {
            decision.status = "blocked".to_owned();
            decision.reason = format!("Tradier positions request failed: {err}");
            return Ok(());
        }
    };
    let orders = match client.get_orders() {
        Ok(response) if response.ok => response.orders,
        Ok(response) => {
            decision.status = "blocked".to_owned();
            decision.reason = format!(
                "Tradier orders request failed: {}",
                response
                    .error
                    .unwrap_or_else(|| "Tradier orders request failed".to_owned())
            );
            return Ok(());
        }
        Err(err) => {
            decision.status = "blocked".to_owned();
            decision.reason = format!("Tradier orders request failed: {err}");
            return Ok(());
        }
    };

    let mut open_positions_checked = 0_usize;
    let mut hold_reasons = Vec::new();
    for action in actions {
        let state =
            match tradier_wheel_lifecycle_state(&action, &positions, &orders, decision.as_of) {
                Ok(state) => state,
                Err(err) => {
                    decision.status = "blocked".to_owned();
                    decision.reason = format!("Tradier wheel lifecycle check failed: {err}");
                    return Ok(());
                }
            };
        match state {
            TradierWheelLifecycleState::Flat => {
                hold_reasons.push(format!(
                    "{} has no matching Tradier wheel position",
                    trade_signal_label(&action)
                ));
            }
            TradierWheelLifecycleState::ActiveOrder { id, status } => {
                decision.status = "blocked".to_owned();
                decision.reason = format!(
                    "active Tradier wheel order id {:?} status {:?} blocks lifecycle management",
                    id, status
                );
                return Ok(());
            }
            TradierWheelLifecycleState::Inconsistent { reason } => {
                decision.status = "blocked".to_owned();
                decision.reason = format!("inconsistent Tradier wheel lifecycle state: {reason}");
                return Ok(());
            }
            TradierWheelLifecycleState::ShortPutOpen { quantity } => {
                open_positions_checked += 1;
                if quantity != 1 {
                    decision.status = "blocked".to_owned();
                    decision.reason = format!(
                        "Tradier wheel short-put quantity {quantity} requires manual management; live signal quantity model supports one contract per action"
                    );
                    return Ok(());
                }
                let put_key = match trade_signal_wheel_short_put_key(&action) {
                    Ok(key) => key,
                    Err(err) => {
                        decision.status = "blocked".to_owned();
                        decision.reason = format!("Tradier wheel short-put target rejected: {err}");
                        return Ok(());
                    }
                };
                let put_symbol = match tradier_occ_option_symbol(&put_key) {
                    Ok(symbol) => symbol,
                    Err(err) => {
                        decision.status = "blocked".to_owned();
                        decision.reason =
                            format!("Tradier wheel short-put OCC symbol rejected: {err}");
                        return Ok(());
                    }
                };
                let (quotes, close_debit) = match tradier_validate_current_single_option_quote(
                    &client,
                    &put_symbol,
                    "ask",
                    decision.mode,
                ) {
                    Ok(result) => result,
                    Err(err) => {
                        decision.status = "blocked".to_owned();
                        decision.reason =
                            format!("Tradier wheel short-put quote validation failed: {err}");
                        return Ok(());
                    }
                };
                decision.tradier_quote = Some(quotes);
                let exit_plan =
                    match live_wheel_short_put_exit_plan(&action, close_debit, decision.as_of) {
                        Ok(plan) => plan,
                        Err(err) => {
                            decision.status = "blocked".to_owned();
                            decision.reason =
                                format!("live wheel short-put exit rule evaluation failed: {err}");
                            return Ok(());
                        }
                    };
                match exit_plan {
                    WheelShortPutExitPlan::Hold { reason } => {
                        hold_reasons.push(format!("{}: {reason}", trade_signal_label(&action)));
                    }
                    WheelShortPutExitPlan::Close {
                        reason,
                        limit_debit,
                    } => {
                        let payload = match tradier_single_option_payload(
                            &action.symbol,
                            &put_symbol,
                            "buy_to_close",
                            quantity,
                            limit_debit,
                        ) {
                            Ok(payload) => payload,
                            Err(err) => {
                                decision.status = "blocked".to_owned();
                                decision.reason = format!(
                                    "Tradier wheel short-put close order shape rejected before preview: {err}"
                                );
                                return Ok(());
                            }
                        };
                        decision.selected_signal = Some(action.clone());
                        decision.action_kind = Some(ExecutionActionKind::ManageOpen);
                        return apply_tradier_payload_preview_and_maybe_place(
                            decision,
                            &client,
                            &config,
                            &payload,
                            &action,
                            review_ledger_path,
                            live_ledger_path,
                            &orders,
                            format!("Tradier wheel short-put close triggered by {reason}").as_str(),
                        );
                    }
                }
            }
            TradierWheelLifecycleState::AssignedStock { shares } => {
                open_positions_checked += 1;
                let covered_call_quantity = shares / 100.0;
                if covered_call_quantity < 1.0
                    || (covered_call_quantity.round() - covered_call_quantity).abs() > f64::EPSILON
                    || covered_call_quantity.round() != 1.0
                {
                    decision.status = "blocked".to_owned();
                    decision.reason = format!(
                        "Tradier wheel assigned stock quantity {shares:.4} requires manual management; live signal quantity model supports one covered-call contract"
                    );
                    return Ok(());
                }
                match live_wheel_called_away_mismatch_due(&action, decision.as_of) {
                    Ok(true) => {
                        decision.status = "blocked".to_owned();
                        decision.reason = format!(
                            "{} has residual Tradier stock after simulated covered-call assignment/called-away exit; manual broker reconciliation is required before autonomous wheel management continues",
                            trade_signal_label(&action)
                        );
                        return Ok(());
                    }
                    Ok(false) => {}
                    Err(err) => {
                        decision.status = "blocked".to_owned();
                        decision.reason =
                            format!("live wheel called-away reconciliation rule failed: {err}");
                        return Ok(());
                    }
                }
                if let Some(reason) =
                    match live_wheel_stock_liquidation_reason(&action, decision.as_of) {
                        Ok(reason) => reason,
                        Err(err) => {
                            decision.status = "blocked".to_owned();
                            decision.reason =
                                format!("live wheel stock liquidation rule failed: {err}");
                            return Ok(());
                        }
                    }
                {
                    let stock_quantity = if (shares.round() - shares).abs() <= f64::EPSILON
                        && shares.round() > 0.0
                    {
                        shares.round() as u32
                    } else {
                        decision.status = "blocked".to_owned();
                        decision.reason = format!(
                            "Tradier wheel assigned stock quantity {shares:.4} is not a whole-share long position"
                        );
                        return Ok(());
                    };
                    let (quotes, limit_price) = match tradier_validate_current_equity_sell_quote(
                        &client,
                        &action.symbol,
                        decision.mode,
                    ) {
                        Ok(result) => result,
                        Err(err) => {
                            decision.status = "blocked".to_owned();
                            decision.reason =
                                format!("Tradier wheel stock quote validation failed: {err}");
                            return Ok(());
                        }
                    };
                    decision.tradier_quote = Some(quotes);
                    let payload = match tradier_equity_sell_payload(
                        &action.symbol,
                        stock_quantity,
                        limit_price,
                    ) {
                        Ok(payload) => payload,
                        Err(err) => {
                            decision.status = "blocked".to_owned();
                            decision.reason = format!(
                                "Tradier wheel stock liquidation order shape rejected before preview: {err}"
                            );
                            return Ok(());
                        }
                    };
                    decision.selected_signal = Some(action.clone());
                    decision.action_kind = Some(ExecutionActionKind::ManageOpen);
                    return apply_tradier_payload_preview_and_maybe_place(
                        decision,
                        &client,
                        &config,
                        &payload,
                        &action,
                        review_ledger_path,
                        live_ledger_path,
                        &orders,
                        format!("Tradier wheel assigned stock liquidation triggered by {reason}")
                            .as_str(),
                    );
                }
                let call_key = match trade_signal_wheel_covered_call_key_if_exported(
                    &action,
                    decision.as_of,
                ) {
                    Ok(Some(key)) => key,
                    Ok(None) => {
                        hold_reasons.push(format!(
                            "{} has assigned stock but no exported covered-call target yet; waiting for next eligible call target or stock liquidation rule",
                            trade_signal_label(&action)
                        ));
                        continue;
                    }
                    Err(err) => {
                        decision.status = "blocked".to_owned();
                        decision.reason =
                            format!("Tradier wheel assigned-stock call target rejected: {err}");
                        return Ok(());
                    }
                };
                let call_symbol = match tradier_occ_option_symbol(&call_key) {
                    Ok(symbol) => symbol,
                    Err(err) => {
                        decision.status = "blocked".to_owned();
                        decision.reason =
                            format!("Tradier wheel covered-call OCC symbol rejected: {err}");
                        return Ok(());
                    }
                };
                let (quotes, limit_credit) = match tradier_validate_current_single_option_quote(
                    &client,
                    &call_symbol,
                    "bid",
                    decision.mode,
                ) {
                    Ok(result) => result,
                    Err(err) => {
                        decision.status = "blocked".to_owned();
                        decision.reason =
                            format!("Tradier wheel covered-call quote validation failed: {err}");
                        return Ok(());
                    }
                };
                decision.tradier_quote = Some(quotes);
                let payload = match tradier_single_option_payload(
                    &action.symbol,
                    &call_symbol,
                    "sell_to_open",
                    covered_call_quantity.round() as u32,
                    limit_credit,
                ) {
                    Ok(payload) => payload,
                    Err(err) => {
                        decision.status = "blocked".to_owned();
                        decision.reason = format!(
                            "Tradier wheel covered-call order shape rejected before preview: {err}"
                        );
                        return Ok(());
                    }
                };
                decision.selected_signal = Some(action.clone());
                decision.action_kind = Some(ExecutionActionKind::ManageOpen);
                return apply_tradier_payload_preview_and_maybe_place(
                    decision,
                    &client,
                    &config,
                    &payload,
                    &action,
                    review_ledger_path,
                    live_ledger_path,
                    &orders,
                    "Tradier wheel assigned stock covered-call open",
                );
            }
            TradierWheelLifecycleState::CoveredCallOpen { quantity } => {
                open_positions_checked += 1;
                if quantity != 1 {
                    decision.status = "blocked".to_owned();
                    decision.reason = format!(
                        "Tradier wheel covered-call quantity {quantity} requires manual management; live signal quantity model supports one contract per action"
                    );
                    return Ok(());
                }
                hold_reasons.push(format!(
                    "{} already has Tradier covered-call quantity {}; waiting for assignment/expiry",
                    trade_signal_label(&action),
                    quantity
                ));
            }
        }
    }
    if open_positions_checked == 0 {
        decision.status = "no_position".to_owned();
        decision.reason = format!(
            "Tradier has no matching wheel position for {} exported management signal(s)",
            hold_reasons.len()
        );
    } else {
        decision.status = "holding".to_owned();
        decision.reason = format!(
            "Tradier lifecycle checked {open_positions_checked} wheel position(s); no management order fired: {}",
            hold_reasons.join("; ")
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn apply_tradier_payload_preview_and_maybe_place(
    decision: &mut ExecutionDecision,
    client: &TradierClient,
    config: &TradierConfig,
    payload: &BTreeMap<String, String>,
    action: &TradeSignal,
    review_ledger_path: Option<&Path>,
    live_ledger_path: Option<&Path>,
    broker_orders: &[TradierOrder],
    order_description: &str,
) -> Result<()> {
    let order_key = tradier_order_key(config, payload, action);
    if let Some(ledger_path) = review_ledger_path
        && let Some(entry) = execution_order_ledger_entry_with_statuses(
            ledger_path,
            &order_key,
            &["reviewed", "rejected", "pending_unknown", "submitted"],
        )?
    {
        apply_tradier_ledger_entry_to_decision(decision, &entry);
        return Ok(());
    }
    if let Some(ledger_path) = live_ledger_path
        && let Some(entry) = execution_order_ledger_blocking_entry(ledger_path, &order_key)?
    {
        if tradier_payload_is_close_order(payload) {
            match tradier_submitted_close_ledger_decision(&entry, broker_orders, decision.as_of) {
                Some(SubmittedCloseLedgerDecision::Retry(reason)) => {
                    execution_order_ledger_record_status(
                        ledger_path,
                        &order_key,
                        "terminal_unfilled",
                        entry.broker_order_id.as_deref(),
                        Some(reason.as_str()),
                    )?;
                }
                Some(SubmittedCloseLedgerDecision::Block(reason)) => {
                    decision.status = "blocked".to_owned();
                    decision.reason = reason;
                    return Ok(());
                }
                None => {
                    apply_tradier_ledger_entry_to_decision(decision, &entry);
                    return Ok(());
                }
            }
        } else {
            apply_tradier_ledger_entry_to_decision(decision, &entry);
            return Ok(());
        }
    }

    let preview = match client.preview_order(payload) {
        Ok(response) => response,
        Err(err) => {
            decision.status = "blocked".to_owned();
            decision.reason = format!("{order_description} preview request failed: {err}");
            return Ok(());
        }
    };
    let preview_result = tradier_preview_accepted(&preview);
    decision.tradier_preview = Some(preview);
    if let Err(reason) = preview_result {
        if let Some(ledger_path) = review_ledger_path.or(live_ledger_path) {
            execution_order_ledger_record_status(
                ledger_path,
                &order_key,
                tradier_preview_rejection_ledger_status(payload),
                None,
                Some(reason.as_str()),
            )?;
        }
        decision.status = "rejected".to_owned();
        decision.reason = format!("{order_description} preview rejected the order: {reason}");
        return Ok(());
    }

    decision.broker_review_ok = true;
    if decision.mode != ExecutionMode::Live {
        if let Some(ledger_path) = review_ledger_path {
            execution_order_ledger_record_status(
                ledger_path,
                &order_key,
                "reviewed",
                None,
                Some("Tradier preview succeeded"),
            )?;
        }
        decision.status = "reviewed".to_owned();
        decision.reason =
            format!("{order_description} preview succeeded; live placement was not requested");
        return Ok(());
    }

    if let Some(ledger_path) = live_ledger_path
        && execution_order_ledger_reserve_pending(
            ledger_path,
            &order_key,
            Some("Tradier place request is about to be sent"),
        )? == ExecutionOrderLedgerReservation::AlreadyRecorded
    {
        decision.status = "already_submitted".to_owned();
        decision.reason =
            "matching Tradier order intent is already recorded in the local execution ledger"
                .to_owned();
        return Ok(());
    }
    let place = match client.place_order(payload) {
        Ok(response) => response,
        Err(err) => {
            decision.status = "submit_unknown".to_owned();
            decision.reason = format!(
                "{order_description} place request transport failed after local ledger reservation; check Tradier before retrying: {err}"
            );
            return Ok(());
        }
    };
    let place_result = tradier_place_accepted(&place);
    decision.tradier_place = Some(place);
    match place_result {
        Ok(order_id) => {
            let confirmed_status = match tradier_confirm_order_after_place(client, &order_id) {
                Ok(status) => status,
                Err(err) => {
                    if let Some(ledger_path) = live_ledger_path {
                        execution_order_ledger_record_status(
                            ledger_path,
                            &order_key,
                            "pending_unknown",
                            Some(order_id.as_str()),
                            Some(err.to_string().as_str()),
                        )?;
                    }
                    decision.status = "submit_unknown".to_owned();
                    decision.reason = format!(
                        "{order_description} returned order id {order_id}, but post-submit confirmation failed; check Tradier before retrying: {err}"
                    );
                    return Ok(());
                }
            };
            if let Some(ledger_path) = live_ledger_path {
                let confirmed_reason =
                    format!("Tradier post-submit status confirmed as {confirmed_status}");
                execution_order_ledger_record_status(
                    ledger_path,
                    &order_key,
                    "submitted",
                    Some(order_id.as_str()),
                    Some(confirmed_reason.as_str()),
                )?;
            }
            decision.status = "submitted".to_owned();
            decision.reason = format!(
                "{order_description} returned accepted order id {order_id} with confirmed status {confirmed_status}; broker lifecycle still requires monitoring"
            );
        }
        Err(reason) => {
            if let Some(ledger_path) = live_ledger_path {
                execution_order_ledger_record_status(
                    ledger_path,
                    &order_key,
                    "rejected",
                    None,
                    Some(reason.as_str()),
                )?;
            }
            decision.status = "rejected".to_owned();
            decision.reason = format!("{order_description} returned a rejection: {reason}");
        }
    }
    Ok(())
}

fn tradier_vertical_spread_management_actions(decision: &ExecutionDecision) -> Vec<TradeSignal> {
    tradier_strategy_management_actions(decision, is_vertical_spread_strategy)
}

fn tradier_wheel_management_actions(decision: &ExecutionDecision) -> Vec<TradeSignal> {
    tradier_strategy_management_actions(decision, |strategy| strategy == "wheel")
}

fn tradier_strategy_management_actions(
    decision: &ExecutionDecision,
    strategy_filter: impl Fn(&str) -> bool,
) -> Vec<TradeSignal> {
    if !decision.management_signals.is_empty() {
        return decision
            .management_signals
            .iter()
            .filter(|action| strategy_filter(action.strategy.as_str()))
            .cloned()
            .collect();
    }
    decision
        .selected_signal
        .iter()
        .filter(|action| action.status == SignalStatus::AlreadyOpen)
        .filter(|action| strategy_filter(action.strategy.as_str()))
        .cloned()
        .collect()
}

#[derive(Clone, Debug, PartialEq)]
enum DebitSpreadExitPlan {
    Hold { reason: String },
    Close { reason: String, limit_credit: f64 },
}

#[derive(Clone, Debug, PartialEq)]
enum CreditSpreadExitPlan {
    Hold { reason: String },
    Close { reason: String, limit_debit: f64 },
}

#[derive(Clone, Debug, PartialEq)]
enum WheelShortPutExitPlan {
    Hold { reason: String },
    Close { reason: String, limit_debit: f64 },
}

fn live_debit_spread_exit_plan(
    action: &TradeSignal,
    exit_credit: f64,
    as_of: NaiveDate,
) -> Result<DebitSpreadExitPlan> {
    if !exit_credit.is_finite() {
        anyhow::bail!("exit credit must be finite");
    }
    let rules = action
        .execution_rules
        .as_ref()
        .context("already-open signal missing execution rules")?;
    let entry_date = trade_signal_date(action, action.entry_date.as_deref(), "entry_date")
        .map_err(anyhow::Error::msg)?;
    let expiration = trade_signal_date(action, action.expiration.as_deref(), "expiration")
        .map_err(anyhow::Error::msg)?;
    if entry_date > as_of {
        anyhow::bail!("entry_date {entry_date} is after decision as_of {as_of}");
    }
    let days_held = (as_of - entry_date).num_days();
    let dte = (expiration - as_of).num_days();
    let entry_credit = require_option_nonzero_f64(action.entry_credit, "entry_credit")?;
    if entry_credit >= 0.0 {
        anyhow::bail!("debit-spread management requires a negative entry_credit debit");
    }
    let entry_debit = entry_credit.abs();
    let width = debit_spread_width(action)?;
    if entry_debit > width + 0.01 {
        anyhow::bail!(
            "entry debit {:.2} exceeds debit-spread width {:.2}",
            entry_debit,
            width
        );
    }
    let max_profit_per_share = width - entry_debit;
    let take_profit_credit = entry_debit + max_profit_per_share * rules.take_profit_pct;
    let stop_credit = entry_debit * (1.0 - rules.stop_loss_multiple).max(0.0);
    let close = |reason: &str| DebitSpreadExitPlan::Close {
        reason: reason.to_owned(),
        limit_credit: exit_credit,
    };
    if exit_credit >= take_profit_credit {
        return Ok(close("take_profit"));
    }
    if exit_credit <= stop_credit {
        return Ok(close("stop_loss"));
    }
    if let Some(reason) = live_debit_spread_time_exit_reason(action, as_of)? {
        return Ok(close(reason));
    }
    Ok(DebitSpreadExitPlan::Hold {
        reason: format!(
            "exit_credit {:.2} below take_profit {:.2}, above stop {:.2}, days_held {}, dte {}",
            exit_credit, take_profit_credit, stop_credit, days_held, dte
        ),
    })
}

fn live_credit_spread_exit_plan(
    action: &TradeSignal,
    exit_debit: f64,
    as_of: NaiveDate,
) -> Result<CreditSpreadExitPlan> {
    if !exit_debit.is_finite() {
        anyhow::bail!("exit debit must be finite");
    }
    let rules = action
        .execution_rules
        .as_ref()
        .context("already-open signal missing execution rules")?;
    let entry_date = trade_signal_date(action, action.entry_date.as_deref(), "entry_date")
        .map_err(anyhow::Error::msg)?;
    let expiration = trade_signal_date(action, action.expiration.as_deref(), "expiration")
        .map_err(anyhow::Error::msg)?;
    if entry_date > as_of {
        anyhow::bail!("entry_date {entry_date} is after decision as_of {as_of}");
    }
    let days_held = (as_of - entry_date).num_days();
    let dte = (expiration - as_of).num_days();
    let entry_credit = require_option_nonzero_f64(action.entry_credit, "entry_credit")?;
    if entry_credit <= 0.0 {
        anyhow::bail!("credit-spread management requires a positive entry_credit credit");
    }
    let width = debit_spread_width(action)?;
    if entry_credit >= width {
        anyhow::bail!(
            "entry credit {:.2} must be below credit-spread width {:.2}",
            entry_credit,
            width
        );
    }
    let take_profit_debit = entry_credit * (1.0 - rules.take_profit_pct).max(0.0);
    let stop_debit = entry_credit * rules.stop_loss_multiple;
    let close = |reason: &str| CreditSpreadExitPlan::Close {
        reason: reason.to_owned(),
        limit_debit: exit_debit,
    };
    if exit_debit >= stop_debit {
        return Ok(close("stop_loss"));
    }
    if exit_debit <= take_profit_debit {
        return Ok(close("take_profit"));
    }
    if let Some(reason) = live_credit_spread_time_exit_reason(action, as_of)? {
        return Ok(close(reason));
    }
    Ok(CreditSpreadExitPlan::Hold {
        reason: format!(
            "exit_debit {:.2} below stop {:.2}, above take_profit {:.2}, days_held {}, dte {}",
            exit_debit, stop_debit, take_profit_debit, days_held, dte
        ),
    })
}

fn live_debit_spread_time_exit_reason(
    action: &TradeSignal,
    as_of: NaiveDate,
) -> Result<Option<&'static str>> {
    let rules = action
        .execution_rules
        .as_ref()
        .context("already-open signal missing execution rules")?;
    let entry_date = trade_signal_date(action, action.entry_date.as_deref(), "entry_date")
        .map_err(anyhow::Error::msg)?;
    let expiration = trade_signal_date(action, action.expiration.as_deref(), "expiration")
        .map_err(anyhow::Error::msg)?;
    if entry_date > as_of {
        anyhow::bail!("entry_date {entry_date} is after decision as_of {as_of}");
    }
    let days_held = (as_of - entry_date).num_days();
    let dte = (expiration - as_of).num_days();
    if rules
        .max_hold_days
        .is_some_and(|max_days| max_days > 0 && days_held >= max_days)
    {
        return Ok(Some("max_hold"));
    }
    if dte <= rules.force_close_dte {
        return Ok(Some("force_close"));
    }
    Ok(None)
}

fn live_credit_spread_time_exit_reason(
    action: &TradeSignal,
    as_of: NaiveDate,
) -> Result<Option<&'static str>> {
    live_debit_spread_time_exit_reason(action, as_of)
}

fn live_vertical_spread_time_exit_reason(
    action: &TradeSignal,
    as_of: NaiveDate,
) -> Result<Option<&'static str>> {
    live_debit_spread_time_exit_reason(action, as_of)
}

fn live_wheel_short_put_exit_plan(
    action: &TradeSignal,
    close_debit: f64,
    as_of: NaiveDate,
) -> Result<WheelShortPutExitPlan> {
    if !close_debit.is_finite() || close_debit <= 0.0 {
        anyhow::bail!("short-put close debit must be positive and finite");
    }
    let rules = action
        .execution_rules
        .as_ref()
        .context("already-open wheel signal missing execution rules")?;
    let entry_date = trade_signal_date(action, action.entry_date.as_deref(), "entry_date")
        .map_err(anyhow::Error::msg)?;
    let expiration = trade_signal_date(action, action.expiration.as_deref(), "expiration")
        .map_err(anyhow::Error::msg)?;
    if entry_date > as_of {
        anyhow::bail!("entry_date {entry_date} is after decision as_of {as_of}");
    }
    if expiration < as_of {
        anyhow::bail!("wheel short-put expiration {expiration} is before decision as_of {as_of}");
    }
    let entry_credit = require_option_nonzero_f64(action.entry_credit, "entry_credit")?;
    if entry_credit <= 0.0 {
        anyhow::bail!("wheel short-put management requires positive entry_credit");
    }
    let take_profit_debit = entry_credit * (1.0 - rules.take_profit_pct).max(0.0);
    if close_debit <= take_profit_debit + 0.01 {
        return Ok(WheelShortPutExitPlan::Close {
            reason: "take_profit".to_owned(),
            limit_debit: close_debit,
        });
    }
    Ok(WheelShortPutExitPlan::Hold {
        reason: format!(
            "short_put_ask {:.2} above take_profit_debit {:.2}; wheel parity waits for put expiry/assignment instead of force-closing",
            close_debit, take_profit_debit
        ),
    })
}

fn live_wheel_stock_liquidation_reason(
    action: &TradeSignal,
    as_of: NaiveDate,
) -> Result<Option<String>> {
    let expiration = trade_signal_date(action, action.expiration.as_deref(), "expiration")
        .map_err(anyhow::Error::msg)?;
    let exit_date = trade_signal_date(action, action.exit_date.as_deref(), "exit_date")
        .map_err(anyhow::Error::msg)?;
    if exit_date <= as_of
        && matches!(
            action.exit_reason.as_deref(),
            Some("stock_marked_after_calls" | "stock_marked_no_call")
        )
    {
        return Ok(action.exit_reason.clone());
    }
    let max_stock_hold_days = action
        .execution_rules
        .as_ref()
        .and_then(|rules| rules.max_hold_days)
        .unwrap_or(45)
        .max(1);
    let forced_stock_exit_date = expiration + chrono::Duration::days(max_stock_hold_days);
    if as_of >= forced_stock_exit_date {
        return Ok(Some("max_stock_hold".to_owned()));
    }
    Ok(None)
}

fn live_wheel_called_away_mismatch_due(action: &TradeSignal, as_of: NaiveDate) -> Result<bool> {
    if !matches!(
        action.exit_reason.as_deref(),
        Some("covered_call_assigned" | "called_away")
    ) {
        return Ok(false);
    }
    let exit_date = trade_signal_date(action, action.exit_date.as_deref(), "exit_date")
        .map_err(anyhow::Error::msg)?;
    Ok(exit_date <= as_of)
}

fn debit_spread_width(action: &TradeSignal) -> Result<f64> {
    if let Some(width) = action.width
        && width.is_finite()
        && width > 0.0
    {
        return Ok(width);
    }
    let short_strike = require_option_f64(action.short_strike, "short_strike")?;
    let long_strike = require_option_f64(action.long_strike, "long_strike")?;
    Ok((long_strike - short_strike).abs())
}

fn tradier_config_from_env() -> Result<TradierConfig> {
    let account_id = std::env::var("SPREAD_TRADIER_ACCOUNT_ID")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .context("SPREAD_TRADIER_ACCOUNT_ID is missing")?;
    let token = std::env::var("SPREAD_TRADIER_TOKEN")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .context("SPREAD_TRADIER_TOKEN is missing")?;
    let base_url = std::env::var("SPREAD_TRADIER_BASE_URL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "https://sandbox.tradier.com/v1".to_owned());
    Ok(TradierConfig {
        account_id,
        token,
        base_url,
    })
}

fn tradier_preview_accepted(response: &TradierOrderResponse) -> std::result::Result<(), String> {
    tradier_response_base_ok(response)?;
    let order = tradier_response_order(&response.raw)
        .ok_or_else(|| "missing Tradier order preview envelope".to_owned())?;
    let result = order
        .get("result")
        .and_then(|value| value.as_bool())
        .ok_or_else(|| "missing Tradier preview result=true confirmation".to_owned())?;
    if !result {
        return Err(tradier_response_reason(&response.raw)
            .unwrap_or_else(|| "Tradier preview result=false".to_owned()));
    }
    let status = tradier_order_status(order).unwrap_or_default();
    if status.eq_ignore_ascii_case("ok") {
        Ok(())
    } else {
        Err(format!("unexpected Tradier preview status {status}"))
    }
}

fn tradier_place_accepted(response: &TradierOrderResponse) -> std::result::Result<String, String> {
    tradier_response_base_ok(response)?;
    let order = tradier_response_order(&response.raw)
        .ok_or_else(|| "missing Tradier place order envelope".to_owned())?;
    let order_id = order
        .get("id")
        .and_then(|value| {
            value
                .as_str()
                .map(ToOwned::to_owned)
                .or_else(|| value.as_i64().map(|id| id.to_string()))
        })
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "missing Tradier broker order id".to_owned())?;
    let status = tradier_order_status(order).unwrap_or_default();
    if matches!(
        status.as_str(),
        "ok" | "pending" | "open" | "partially_filled" | "filled"
    ) {
        Ok(order_id)
    } else {
        Err(tradier_response_reason(&response.raw)
            .unwrap_or_else(|| format!("unexpected Tradier place status {status}")))
    }
}

fn tradier_confirm_order_after_place(client: &TradierClient, order_id: &str) -> Result<String> {
    let orders = client
        .get_orders()
        .context("fetch Tradier orders after place")?;
    if !orders.ok {
        anyhow::bail!(
            "{}",
            orders
                .error
                .unwrap_or_else(|| "Tradier orders request failed".to_owned())
        );
    }
    let Some(order) = orders
        .orders
        .iter()
        .find(|order| order.id.as_deref().is_some_and(|id| id.trim() == order_id))
    else {
        anyhow::bail!("placed Tradier order id {order_id} was not present in orders response");
    };
    let Some(status) = order
        .status
        .as_deref()
        .map(str::trim)
        .filter(|status| !status.is_empty())
    else {
        anyhow::bail!("placed Tradier order id {order_id} returned no order status");
    };
    if tradier_post_submit_status_confirms(status) {
        Ok(status.to_owned())
    } else if tradier_order_status_blocks_new_entry(status) {
        anyhow::bail!(
            "placed Tradier order id {order_id} returned unrecognized active status {status}"
        )
    } else {
        anyhow::bail!("placed Tradier order id {order_id} returned terminal status {status}");
    }
}

fn tradier_post_submit_status_confirms(status: &str) -> bool {
    matches!(
        status.trim().to_ascii_lowercase().as_str(),
        "accepted"
            | "open"
            | "pending"
            | "queued"
            | "submitted"
            | "partially_filled"
            | "partially-filled"
            | "filled"
    )
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SubmittedCloseLedgerDecision {
    Retry(String),
    Block(String),
}

fn tradier_submitted_close_ledger_decision(
    entry: &ExecutionOrderLedgerEntry,
    broker_orders: &[TradierOrder],
    as_of: NaiveDate,
) -> Option<SubmittedCloseLedgerDecision> {
    if entry.status != "submitted" {
        return None;
    }
    let order_id = entry.broker_order_id.as_deref()?.trim();
    if order_id.is_empty() {
        return None;
    }
    if let Some(order) = broker_orders
        .iter()
        .find(|order| order.id.as_deref().is_some_and(|id| id.trim() == order_id))
    {
        let status = order.status.as_deref().map(str::trim).unwrap_or_default();
        if tradier_terminal_bad_status(status) {
            return Some(SubmittedCloseLedgerDecision::Retry(format!(
                "prior submitted DAY close order id {order_id} is terminal with broker status {status}; exposure remains and no active matching close order is present"
            )));
        }
        if status.eq_ignore_ascii_case("filled") {
            return Some(SubmittedCloseLedgerDecision::Block(format!(
                "prior submitted close order id {order_id} is filled, but broker positions still show exposure; wait for position reconciliation or recover manually before submitting another close"
            )));
        }
        return None;
    }
    if entry.recorded_at.date_naive() < as_of {
        return Some(SubmittedCloseLedgerDecision::Retry(format!(
            "prior submitted DAY close order id {order_id} is absent from current Tradier orders on {as_of}; exposure remains and no active matching close order is present"
        )));
    }
    Some(SubmittedCloseLedgerDecision::Block(format!(
        "submitted close order id {order_id} is absent from current Tradier orders on the same market date {as_of}; manual broker reconciliation is required before retrying"
    )))
}

fn tradier_order_status_blocks_new_entry(status: &str) -> bool {
    !matches!(
        status.to_ascii_lowercase().as_str(),
        "filled" | "canceled" | "cancelled" | "rejected" | "expired" | "error"
    )
}

fn tradier_payload_is_close_order(payload: &BTreeMap<String, String>) -> bool {
    if payload.get("class").map(String::as_str) == Some("equity")
        && payload.get("side").map(String::as_str) == Some("sell")
    {
        return true;
    }
    payload.iter().any(|(key, value)| {
        (key == "side" || key.starts_with("side[")) && value.ends_with("_to_close")
    })
}

fn tradier_preview_rejection_ledger_status(payload: &BTreeMap<String, String>) -> &'static str {
    if tradier_payload_is_close_order(payload) {
        "preview_rejected"
    } else {
        "rejected"
    }
}

fn tradier_order_key_payload(payload: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    let mut keyed_payload = payload.clone();
    if tradier_payload_is_close_order(payload) {
        keyed_payload.remove("price");
    }
    keyed_payload
}

fn tradier_order_key(
    config: &TradierConfig,
    payload: &BTreeMap<String, String>,
    action: &TradeSignal,
) -> String {
    serde_json::to_string(&serde_json::json!({
        "broker": "tradier",
        "account_id": config.account_id,
        "base_url": config.base_url,
        "strategy": action.strategy,
        "entry_date": action.entry_date,
        "status": action.status,
        "payload": tradier_order_key_payload(payload),
    }))
    .expect("Tradier order key serialization should be infallible")
}

fn tradier_response_base_ok(response: &TradierOrderResponse) -> std::result::Result<(), String> {
    if !response.ok {
        return Err(response
            .error
            .clone()
            .unwrap_or_else(|| "Tradier HTTP request was not successful".to_owned()));
    }
    if let Some(reason) = tradier_response_reason(&response.raw) {
        return Err(reason);
    }
    if tradier_response_has_terminal_bad_status(&response.raw) {
        return Err("Tradier response contains terminal bad order status".to_owned());
    }
    Ok(())
}

fn tradier_response_order(
    value: &serde_json::Value,
) -> Option<&serde_json::Map<String, serde_json::Value>> {
    value
        .get("order")
        .and_then(|value| value.as_object())
        .or_else(|| {
            value
                .get("orders")
                .and_then(|orders| orders.get("order"))
                .and_then(|order| {
                    order
                        .as_object()
                        .or_else(|| order.as_array()?.first()?.as_object())
                })
        })
}

fn tradier_order_status(order: &serde_json::Map<String, serde_json::Value>) -> Option<String> {
    order
        .get("status")
        .and_then(|value| value.as_str())
        .map(|status| status.to_ascii_lowercase())
}

fn tradier_response_reason(value: &serde_json::Value) -> Option<String> {
    for key in ["reason_description", "reason", "message", "errors", "fault"] {
        if let Some(reason) = value.get(key) {
            if let Some(reason) = reason.as_str() {
                return Some(reason.to_owned());
            }
            if reason.is_object() || reason.is_array() {
                return Some(reason.to_string());
            }
        }
    }
    match value {
        serde_json::Value::Object(map) => map.values().find_map(tradier_response_reason),
        serde_json::Value::Array(values) => values.iter().find_map(tradier_response_reason),
        _ => None,
    }
}

fn tradier_response_has_terminal_bad_status(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(map) => map.iter().any(|(key, value)| {
            key == "status" && value.as_str().is_some_and(tradier_terminal_bad_status)
                || tradier_response_has_terminal_bad_status(value)
        }),
        serde_json::Value::Array(values) => {
            values.iter().any(tradier_response_has_terminal_bad_status)
        }
        _ => false,
    }
}

fn tradier_terminal_bad_status(status: &str) -> bool {
    matches!(
        status.to_ascii_lowercase().as_str(),
        "rejected" | "error" | "canceled" | "cancelled" | "expired"
    )
}

fn tradier_execution_order_payload(action: &TradeSignal) -> Result<BTreeMap<String, String>> {
    match action.strategy.as_str() {
        "call_credit_spread" | "put_credit_spread" => tradier_multileg_credit_payload(action),
        "call_debit_spread" | "put_debit_spread" => tradier_multileg_debit_payload(action),
        "wheel" => tradier_cash_secured_put_payload(action),
        other => anyhow::bail!("Tradier execution does not support strategy {other}"),
    }
}

fn tradier_assert_market_open(client: &TradierClient) -> Result<TradierMarketClockResponse> {
    let clock = client
        .get_market_clock()
        .context("fetch Tradier market clock before live order")?;
    if !clock.ok {
        anyhow::bail!(
            "{}",
            clock
                .error
                .clone()
                .unwrap_or_else(|| "Tradier market clock request failed".to_owned())
        );
    }
    let clock_detail = clock
        .clock
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Tradier market clock response missing clock object"))?;
    if !tradier_market_clock_open(clock_detail) {
        anyhow::bail!("{}", tradier_market_clock_reason(clock_detail));
    }
    Ok(clock)
}

fn tradier_assert_buying_power(
    client: &TradierClient,
    action: &TradeSignal,
    risk: &CanaryRiskPolicy,
) -> Result<()> {
    let reserve = match action.reserve {
        Some(reserve) => reserve,
        None => trade_signal_risk(action)
            .map(|risk| risk.reserve)
            .map_err(|reason| anyhow::anyhow!(reason))?,
    };
    if !reserve.is_finite() || reserve <= 0.0 {
        anyhow::bail!(
            "{} {} has invalid reserve {:.2}",
            action.symbol,
            action.strategy,
            reserve
        );
    }
    let balances = client
        .get_balances()
        .context("fetch Tradier balances before preview/place")?;
    if !balances.ok {
        anyhow::bail!(
            "{}",
            balances
                .error
                .unwrap_or_else(|| "Tradier balances request failed".to_owned())
        );
    }
    let balances = balances
        .balances
        .ok_or_else(|| anyhow::anyhow!("Tradier balances response missing balances"))?;
    let buying_power = balances
        .option_buying_power
        .or(balances.total_cash)
        .or(balances.cash)
        .ok_or_else(|| anyhow::anyhow!("Tradier balances missing option buying power/cash"))?;
    let required_buying_power = reserve + risk.free_cash_buffer;
    if buying_power < required_buying_power {
        anyhow::bail!(
            "buying power {:.2} is below order reserve {:.2} plus free-cash buffer {:.2}",
            buying_power,
            reserve,
            risk.free_cash_buffer
        );
    }
    Ok(())
}

fn tradier_assert_wheel_broker_state(
    client: &TradierClient,
    action: &TradeSignal,
    _risk: &CanaryRiskPolicy,
) -> Result<()> {
    let symbol = require_action_field(action.symbol.as_str(), "symbol")?.to_ascii_uppercase();
    let positions = client
        .get_positions()
        .context("fetch Tradier positions before wheel preview")?;
    if !positions.ok {
        anyhow::bail!(
            "{}",
            positions
                .error
                .unwrap_or_else(|| "Tradier positions request failed".to_owned())
        );
    }
    if let Some(position) = positions
        .positions
        .iter()
        .find(|position| tradier_position_matches_symbol(position, &symbol))
    {
        anyhow::bail!(
            "existing Tradier position {} quantity {:.4} blocks new wheel entry",
            position.symbol,
            position.quantity
        );
    }

    let orders = client
        .get_orders()
        .context("fetch Tradier orders before wheel preview")?;
    if !orders.ok {
        anyhow::bail!(
            "{}",
            orders
                .error
                .unwrap_or_else(|| "Tradier orders request failed".to_owned())
        );
    }
    if let Some(order) = orders
        .orders
        .iter()
        .find(|order| tradier_order_blocks_new_wheel_entry(order, &symbol))
    {
        anyhow::bail!(
            "active Tradier order {:?} status {:?} blocks new wheel entry",
            order.id,
            order.status
        );
    }
    Ok(())
}

fn tradier_assert_vertical_spread_lifecycle_flat(
    client: &TradierClient,
    action: &TradeSignal,
) -> Result<()> {
    let positions = client
        .get_positions()
        .context("fetch Tradier positions before vertical-spread lifecycle check")?;
    if !positions.ok {
        anyhow::bail!(
            "{}",
            positions
                .error
                .unwrap_or_else(|| "Tradier positions request failed".to_owned())
        );
    }

    let orders = client
        .get_orders()
        .context("fetch Tradier orders before vertical-spread lifecycle check")?;
    if !orders.ok {
        anyhow::bail!(
            "{}",
            orders
                .error
                .unwrap_or_else(|| "Tradier orders request failed".to_owned())
        );
    }

    match tradier_vertical_spread_lifecycle_state(action, &positions.positions, &orders.orders)? {
        TradierDebitSpreadLifecycleState::Flat => Ok(()),
        TradierDebitSpreadLifecycleState::Open { quantity } => {
            anyhow::bail!("existing Tradier vertical spread quantity {quantity} blocks new entry")
        }
        TradierDebitSpreadLifecycleState::ActiveOrder { id, status } => {
            anyhow::bail!(
                "active Tradier vertical-spread order id {:?} status {:?} blocks new entry",
                id,
                status
            )
        }
        TradierDebitSpreadLifecycleState::Inconsistent { reason } => {
            anyhow::bail!("inconsistent Tradier vertical-spread lifecycle state: {reason}")
        }
        TradierDebitSpreadLifecycleState::AssignedShortLeg {
            right,
            long_quantity,
            stock_quantity,
        } => {
            anyhow::bail!(
                "Tradier vertical-spread short {} appears assigned: stock quantity {:.4} and long hedge quantity {:.4} block new entry",
                option_right_value(&right),
                stock_quantity,
                long_quantity
            )
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum TradierDebitSpreadLifecycleState {
    Flat,
    Open {
        quantity: u32,
    },
    ActiveOrder {
        id: Option<String>,
        status: Option<String>,
    },
    AssignedShortLeg {
        right: OptionRight,
        long_quantity: f64,
        stock_quantity: f64,
    },
    Inconsistent {
        reason: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
enum TradierWheelLifecycleState {
    Flat,
    ShortPutOpen {
        quantity: u32,
    },
    AssignedStock {
        shares: f64,
    },
    CoveredCallOpen {
        quantity: u32,
    },
    ActiveOrder {
        id: Option<String>,
        status: Option<String>,
    },
    Inconsistent {
        reason: String,
    },
}

#[allow(dead_code)]
fn tradier_debit_spread_lifecycle_state(
    action: &TradeSignal,
    positions: &[TradierPosition],
    orders: &[TradierOrder],
) -> Result<TradierDebitSpreadLifecycleState> {
    tradier_vertical_spread_lifecycle_state_with_allowed_positions(action, positions, orders, None)
}

#[allow(dead_code)]
fn tradier_debit_spread_lifecycle_state_with_allowed_positions(
    action: &TradeSignal,
    positions: &[TradierPosition],
    orders: &[TradierOrder],
    allowed_position_symbols: Option<&BTreeSet<String>>,
) -> Result<TradierDebitSpreadLifecycleState> {
    tradier_vertical_spread_lifecycle_state_with_allowed_positions(
        action,
        positions,
        orders,
        allowed_position_symbols,
    )
}

fn tradier_vertical_spread_lifecycle_state(
    action: &TradeSignal,
    positions: &[TradierPosition],
    orders: &[TradierOrder],
) -> Result<TradierDebitSpreadLifecycleState> {
    tradier_vertical_spread_lifecycle_state_with_allowed_positions(action, positions, orders, None)
}

fn tradier_vertical_spread_lifecycle_state_with_allowed_positions(
    action: &TradeSignal,
    positions: &[TradierPosition],
    orders: &[TradierOrder],
    allowed_position_symbols: Option<&BTreeSet<String>>,
) -> Result<TradierDebitSpreadLifecycleState> {
    let (long_key, short_key) = trade_signal_vertical_spread_keys(action)?;
    let underlying = long_key.underlying.to_ascii_uppercase();
    let long_occ = tradier_occ_option_symbol(&long_key)?;
    let short_occ = tradier_occ_option_symbol(&short_key)?;

    if let Some(order) = orders.iter().find(|order| {
        tradier_order_is_active(order)
            && tradier_order_matches_debit_spread_security(
                order,
                &underlying,
                &long_occ,
                &short_occ,
            )
    }) {
        return Ok(TradierDebitSpreadLifecycleState::ActiveOrder {
            id: order.id.clone(),
            status: order.status.clone(),
        });
    }

    let long_quantity = tradier_position_quantity_for_security(positions, &long_occ);
    let short_quantity = tradier_position_quantity_for_security(positions, &short_occ);
    let stock_quantity = tradier_position_quantity_for_security(positions, &underlying);
    let unrelated = positions.iter().find(|position| {
        let position_symbol = position.symbol.to_ascii_uppercase();
        position.quantity.abs() > f64::EPSILON
            && tradier_position_matches_symbol(position, &underlying)
            && !position.symbol.eq_ignore_ascii_case(&underlying)
            && !position.symbol.eq_ignore_ascii_case(&long_occ)
            && !position.symbol.eq_ignore_ascii_case(&short_occ)
            && !allowed_position_symbols.is_some_and(|symbols| symbols.contains(&position_symbol))
    });
    if let Some(position) = unrelated {
        return Ok(TradierDebitSpreadLifecycleState::Inconsistent {
            reason: format!(
                "unrelated symbol exposure {} quantity {:.4}",
                position.symbol, position.quantity
            ),
        });
    }

    if stock_quantity.abs() > f64::EPSILON {
        return Ok(tradier_debit_spread_assignment_lifecycle_state(
            &long_key.right,
            long_quantity,
            short_quantity,
            stock_quantity,
        ));
    }

    if long_quantity.abs() <= f64::EPSILON && short_quantity.abs() <= f64::EPSILON {
        return Ok(TradierDebitSpreadLifecycleState::Flat);
    }
    if long_quantity > 0.0
        && short_quantity < 0.0
        && (long_quantity + short_quantity).abs() <= f64::EPSILON
        && (long_quantity.round() - long_quantity).abs() <= f64::EPSILON
    {
        return Ok(TradierDebitSpreadLifecycleState::Open {
            quantity: long_quantity.round() as u32,
        });
    }
    Ok(TradierDebitSpreadLifecycleState::Inconsistent {
        reason: format!(
            "expected equal positive long leg and negative short leg quantities; got long {:.4}, short {:.4}",
            long_quantity, short_quantity
        ),
    })
}

fn tradier_debit_spread_assignment_lifecycle_state(
    right: &OptionRight,
    long_quantity: f64,
    short_quantity: f64,
    stock_quantity: f64,
) -> TradierDebitSpreadLifecycleState {
    let assigned_contracts = stock_quantity.abs() / 100.0;
    if assigned_contracts < f64::EPSILON
        || (assigned_contracts.round() - assigned_contracts).abs() > f64::EPSILON
    {
        return TradierDebitSpreadLifecycleState::Inconsistent {
            reason: format!(
                "underlying stock quantity {:.4} is not a whole-contract assignment multiple",
                stock_quantity
            ),
        };
    }
    if short_quantity.abs() > f64::EPSILON {
        return TradierDebitSpreadLifecycleState::Inconsistent {
            reason: format!(
                "underlying stock quantity {:.4} coexists with short leg quantity {:.4}",
                stock_quantity, short_quantity
            ),
        };
    }
    if long_quantity < -f64::EPSILON
        || (long_quantity.abs() > f64::EPSILON
            && (long_quantity.round() - long_quantity).abs() > f64::EPSILON)
    {
        return TradierDebitSpreadLifecycleState::Inconsistent {
            reason: format!(
                "underlying stock quantity {:.4} has invalid long hedge quantity {:.4}",
                stock_quantity, long_quantity
            ),
        };
    }
    if matches!(right, OptionRight::Call) && stock_quantity < 0.0
        || matches!(right, OptionRight::Put) && stock_quantity > 0.0
    {
        return TradierDebitSpreadLifecycleState::AssignedShortLeg {
            right: right.clone(),
            long_quantity,
            stock_quantity,
        };
    }
    TradierDebitSpreadLifecycleState::Inconsistent {
        reason: format!(
            "{} debit spread has underlying stock quantity {:.4}, which does not match assigned short-leg direction",
            option_right_value(right),
            stock_quantity
        ),
    }
}

fn tradier_wheel_lifecycle_state(
    action: &TradeSignal,
    positions: &[TradierPosition],
    orders: &[TradierOrder],
    _as_of: NaiveDate,
) -> Result<TradierWheelLifecycleState> {
    let symbol = require_action_field(action.symbol.as_str(), "symbol")?.to_ascii_uppercase();
    let put_symbol = tradier_occ_option_symbol(&trade_signal_wheel_short_put_key(action)?)?;
    let covered_call_positions = tradier_wheel_covered_call_positions(positions, &symbol);
    let covered_call_symbols: BTreeSet<String> = covered_call_positions
        .iter()
        .map(|position| position.symbol.to_ascii_uppercase())
        .collect();

    if let Some(order) = orders
        .iter()
        .find(|order| tradier_order_blocks_new_symbol_entry(order, &symbol))
    {
        return Ok(TradierWheelLifecycleState::ActiveOrder {
            id: order.id.clone(),
            status: order.status.clone(),
        });
    }

    let short_put_quantity = tradier_position_quantity_for_security(positions, &put_symbol);
    let covered_call_quantity: f64 = covered_call_positions
        .iter()
        .map(|position| position.quantity)
        .sum();
    let stock_quantity = tradier_position_quantity_for_security(positions, &symbol);
    let unrelated = positions.iter().find(|position| {
        let position_symbol = position.symbol.to_ascii_uppercase();
        position.quantity.abs() > f64::EPSILON
            && tradier_position_matches_symbol(position, &symbol)
            && !position.symbol.eq_ignore_ascii_case(&symbol)
            && !position.symbol.eq_ignore_ascii_case(&put_symbol)
            && !covered_call_symbols.contains(&position_symbol)
    });
    if let Some(position) = unrelated {
        return Ok(TradierWheelLifecycleState::Inconsistent {
            reason: format!(
                "unrelated wheel exposure {} quantity {:.4}",
                position.symbol, position.quantity
            ),
        });
    }

    if short_put_quantity.abs() > f64::EPSILON {
        if stock_quantity.abs() > f64::EPSILON || covered_call_quantity.abs() > f64::EPSILON {
            return Ok(TradierWheelLifecycleState::Inconsistent {
                reason: format!(
                    "short put quantity {:.4} coexists with stock {:.4} or covered call {:.4}",
                    short_put_quantity, stock_quantity, covered_call_quantity
                ),
            });
        }
        let contracts = -short_put_quantity;
        if contracts > 0.0 && (contracts.round() - contracts).abs() <= f64::EPSILON {
            return Ok(TradierWheelLifecycleState::ShortPutOpen {
                quantity: contracts.round() as u32,
            });
        }
        return Ok(TradierWheelLifecycleState::Inconsistent {
            reason: format!(
                "expected negative short-put quantity for {}; got {:.4}",
                put_symbol, short_put_quantity
            ),
        });
    }

    if stock_quantity.abs() > f64::EPSILON {
        if stock_quantity < 0.0 {
            return Ok(TradierWheelLifecycleState::Inconsistent {
                reason: format!("wheel stock quantity is short {:.4}", stock_quantity),
            });
        }
        if covered_call_quantity.abs() > f64::EPSILON {
            let contracts = -covered_call_quantity;
            if contracts > 0.0
                && (contracts.round() - contracts).abs() <= f64::EPSILON
                && (stock_quantity - contracts.round() * 100.0).abs() <= f64::EPSILON
            {
                return Ok(TradierWheelLifecycleState::CoveredCallOpen {
                    quantity: contracts.round() as u32,
                });
            }
            return Ok(TradierWheelLifecycleState::Inconsistent {
                reason: format!(
                    "expected covered-call quantity to match stock; got stock {:.4}, call {:.4}",
                    stock_quantity, covered_call_quantity
                ),
            });
        }
        return Ok(TradierWheelLifecycleState::AssignedStock {
            shares: stock_quantity,
        });
    }

    if covered_call_quantity.abs() > f64::EPSILON {
        return Ok(TradierWheelLifecycleState::Inconsistent {
            reason: format!(
                "covered-call quantity {:.4} exists without matching stock",
                covered_call_quantity
            ),
        });
    }

    Ok(TradierWheelLifecycleState::Flat)
}

fn tradier_wheel_covered_call_positions<'a>(
    positions: &'a [TradierPosition],
    symbol: &str,
) -> Vec<&'a TradierPosition> {
    positions
        .iter()
        .filter(|position| {
            position.quantity.abs() > f64::EPSILON
                && position.quantity < 0.0
                && tradier_occ_option_symbol_matches_underlying_right(
                    position.symbol.as_str(),
                    symbol,
                    OptionRight::Call,
                )
        })
        .collect()
}

fn tradier_vertical_spread_management_position_symbols(
    actions: &[TradeSignal],
) -> Result<BTreeSet<String>> {
    let mut symbols = BTreeSet::new();
    for action in actions {
        let (long_key, short_key) = trade_signal_vertical_spread_keys(action)?;
        symbols.insert(tradier_occ_option_symbol(&long_key)?.to_ascii_uppercase());
        symbols.insert(tradier_occ_option_symbol(&short_key)?.to_ascii_uppercase());
    }
    Ok(symbols)
}

#[allow(dead_code)]
fn tradier_debit_spread_management_position_symbols(
    actions: &[TradeSignal],
) -> Result<BTreeSet<String>> {
    tradier_vertical_spread_management_position_symbols(actions)
}

fn tradier_position_quantity_for_security(positions: &[TradierPosition], security: &str) -> f64 {
    positions
        .iter()
        .filter(|position| position.symbol.eq_ignore_ascii_case(security))
        .map(|position| position.quantity)
        .sum()
}

fn tradier_order_is_active(order: &TradierOrder) -> bool {
    order.quantity.unwrap_or(0.0).abs() > f64::EPSILON
        && order
            .status
            .as_deref()
            .is_none_or(tradier_order_status_blocks_new_entry)
}

fn tradier_order_matches_debit_spread_security(
    order: &TradierOrder,
    underlying: &str,
    long_occ: &str,
    short_occ: &str,
) -> bool {
    order.option_symbol.as_deref().is_some_and(|security| {
        security.eq_ignore_ascii_case(long_occ) || security.eq_ignore_ascii_case(short_occ)
    }) || order
        .symbol
        .as_deref()
        .is_some_and(|security| tradier_security_matches_underlying(security, underlying))
}

fn tradier_validate_current_debit_quote(
    client: &TradierClient,
    payload: &BTreeMap<String, String>,
    action: &TradeSignal,
    mode: ExecutionMode,
) -> Result<TradierQuotesResponse> {
    let long_symbol = payload
        .get("option_symbol[0]")
        .ok_or_else(|| anyhow::anyhow!("debit spread payload missing long option symbol"))?
        .to_owned();
    let short_symbol = payload
        .get("option_symbol[1]")
        .ok_or_else(|| anyhow::anyhow!("debit spread payload missing short option symbol"))?
        .to_owned();
    let limit_debit = payload
        .get("price")
        .ok_or_else(|| anyhow::anyhow!("debit spread payload missing price"))?
        .parse::<f64>()
        .context("parse Tradier debit spread limit price")?;
    let quotes = client
        .get_quotes(&[long_symbol.clone(), short_symbol.clone()])
        .context("fetch Tradier current option quotes before debit-spread preview")?;
    if !quotes.ok {
        anyhow::bail!(
            "{}",
            quotes
                .error
                .clone()
                .unwrap_or_else(|| "Tradier quotes request failed".to_owned())
        );
    }
    let long_quote = tradier_quote_for_symbol(&quotes.quotes, &long_symbol)?;
    let short_quote = tradier_quote_for_symbol(&quotes.quotes, &short_symbol)?;
    let long_ask = tradier_quote_positive_value(long_quote.ask, &long_symbol, "ask")?;
    let short_bid = tradier_quote_nonnegative_value(short_quote.bid, &short_symbol, "bid")?;
    if mode == ExecutionMode::Live {
        tradier_quote_positive_value(long_quote.ask_size, &long_symbol, "ask_size")?;
        tradier_quote_positive_value(short_quote.bid_size, &short_symbol, "bid_size")?;
        tradier_assert_fresh_quote_side(long_quote, &long_symbol, "ask")?;
        tradier_assert_fresh_quote_side(short_quote, &short_symbol, "bid")?;
    }
    let current_debit = long_ask - short_bid;
    if !current_debit.is_finite() || current_debit <= 0.0 {
        anyhow::bail!(
            "current conservative debit {:.2} is not a positive executable debit",
            current_debit
        );
    }
    if current_debit > limit_debit + 0.01 {
        anyhow::bail!(
            "current conservative debit {:.2} exceeds order limit {:.2}",
            current_debit,
            limit_debit
        );
    }
    if let Some(max_loss) = action.max_loss
        && current_debit * 100.0 > max_loss + 1.0
    {
        anyhow::bail!(
            "current conservative debit risk {:.2} exceeds signal max_loss {:.2}",
            current_debit * 100.0,
            max_loss
        );
    }
    Ok(quotes)
}

fn tradier_validate_current_credit_quote(
    client: &TradierClient,
    payload: &BTreeMap<String, String>,
    action: &TradeSignal,
    mode: ExecutionMode,
) -> Result<TradierQuotesResponse> {
    let short_symbol = payload
        .get("option_symbol[0]")
        .ok_or_else(|| anyhow::anyhow!("credit spread payload missing short option symbol"))?
        .to_owned();
    let long_symbol = payload
        .get("option_symbol[1]")
        .ok_or_else(|| anyhow::anyhow!("credit spread payload missing long option symbol"))?
        .to_owned();
    let limit_credit = payload
        .get("price")
        .ok_or_else(|| anyhow::anyhow!("credit spread payload missing price"))?
        .parse::<f64>()
        .context("parse Tradier credit spread limit price")?;
    let quotes = client
        .get_quotes(&[short_symbol.clone(), long_symbol.clone()])
        .context("fetch Tradier current option quotes before credit-spread preview")?;
    if !quotes.ok {
        anyhow::bail!(
            "{}",
            quotes
                .error
                .clone()
                .unwrap_or_else(|| "Tradier quotes request failed".to_owned())
        );
    }
    let short_quote = tradier_quote_for_symbol(&quotes.quotes, &short_symbol)?;
    let long_quote = tradier_quote_for_symbol(&quotes.quotes, &long_symbol)?;
    let short_bid = tradier_quote_positive_value(short_quote.bid, &short_symbol, "bid")?;
    let long_ask = tradier_quote_positive_value(long_quote.ask, &long_symbol, "ask")?;
    if mode == ExecutionMode::Live {
        tradier_quote_positive_value(short_quote.bid_size, &short_symbol, "bid_size")?;
        tradier_quote_positive_value(long_quote.ask_size, &long_symbol, "ask_size")?;
        tradier_assert_fresh_quote_side(short_quote, &short_symbol, "bid")?;
        tradier_assert_fresh_quote_side(long_quote, &long_symbol, "ask")?;
    }
    let current_credit = short_bid - long_ask;
    if !current_credit.is_finite() || current_credit <= 0.0 {
        anyhow::bail!(
            "current conservative credit {:.2} is not a positive executable credit",
            current_credit
        );
    }
    let (short_key, long_key) = trade_signal_credit_spread_keys(action)?;
    let width = decimal_to_f64((short_key.strike - long_key.strike).abs(), "width")?;
    if current_credit >= width {
        anyhow::bail!(
            "current conservative credit {:.2} is not below strike width {:.2}",
            current_credit,
            width
        );
    }
    if current_credit + 0.01 < limit_credit {
        anyhow::bail!(
            "current conservative credit {:.2} is below order limit {:.2}",
            current_credit,
            limit_credit
        );
    }
    if let Some(max_loss) = action.max_loss {
        let current_risk = (width - current_credit) * 100.0;
        if current_risk > max_loss + 1.0 {
            anyhow::bail!(
                "current conservative credit-spread risk {:.2} exceeds signal max_loss {:.2}",
                current_risk,
                max_loss
            );
        }
    }
    Ok(quotes)
}

fn tradier_validate_current_wheel_entry_quote(
    client: &TradierClient,
    payload: &BTreeMap<String, String>,
    mode: ExecutionMode,
) -> Result<TradierQuotesResponse> {
    let option_symbol = payload
        .get("option_symbol")
        .ok_or_else(|| anyhow::anyhow!("wheel payload missing option symbol"))?;
    let limit_credit = payload
        .get("price")
        .ok_or_else(|| anyhow::anyhow!("wheel payload missing price"))?
        .parse::<f64>()
        .context("parse Tradier wheel limit price")?;
    let (quotes, current_bid) =
        tradier_validate_current_single_option_quote(client, option_symbol, "bid", mode)?;
    if current_bid + 0.01 < limit_credit {
        anyhow::bail!(
            "current short-put bid {:.2} is below order limit credit {:.2}",
            current_bid,
            limit_credit
        );
    }
    Ok(quotes)
}

#[allow(dead_code)]
fn tradier_validate_current_debit_exit_quote(
    client: &TradierClient,
    action: &TradeSignal,
    mode: ExecutionMode,
) -> Result<(TradierQuotesResponse, f64)> {
    let (long_key, short_key) = trade_signal_debit_spread_keys(action)?;
    let long_symbol = tradier_occ_option_symbol(&long_key)?;
    let short_symbol = tradier_occ_option_symbol(&short_key)?;
    let quotes = client
        .get_quotes(&[long_symbol.clone(), short_symbol.clone()])
        .context("fetch Tradier current option quotes before debit-spread close")?;
    if !quotes.ok {
        anyhow::bail!(
            "{}",
            quotes
                .error
                .clone()
                .unwrap_or_else(|| "Tradier quotes request failed".to_owned())
        );
    }
    let exit_credit =
        tradier_current_debit_exit_credit(&quotes.quotes, &long_symbol, &short_symbol, mode)?;
    let width = decimal_to_f64((long_key.strike - short_key.strike).abs(), "width")?;
    if exit_credit > width + 0.01 {
        anyhow::bail!(
            "current conservative exit credit {:.2} exceeds strike width {:.2}",
            exit_credit,
            width
        );
    }
    Ok((quotes, exit_credit))
}

fn tradier_validate_current_credit_exit_quote(
    client: &TradierClient,
    action: &TradeSignal,
    mode: ExecutionMode,
) -> Result<(TradierQuotesResponse, f64)> {
    let (short_key, long_key) = trade_signal_credit_spread_keys(action)?;
    let short_symbol = tradier_occ_option_symbol(&short_key)?;
    let long_symbol = tradier_occ_option_symbol(&long_key)?;
    let quotes = client
        .get_quotes(&[short_symbol.clone(), long_symbol.clone()])
        .context("fetch Tradier current option quotes before credit-spread close")?;
    if !quotes.ok {
        anyhow::bail!(
            "{}",
            quotes
                .error
                .clone()
                .unwrap_or_else(|| "Tradier quotes request failed".to_owned())
        );
    }
    let width = decimal_to_f64((short_key.strike - long_key.strike).abs(), "width")?;
    let exit_debit = tradier_current_credit_exit_debit(
        &quotes.quotes,
        &short_symbol,
        &long_symbol,
        width,
        mode,
    )?;
    if exit_debit > width + 0.01 {
        anyhow::bail!(
            "current conservative exit debit {:.2} exceeds strike width {:.2}",
            exit_debit,
            width
        );
    }
    Ok((quotes, exit_debit))
}

fn tradier_current_debit_exit_credit(
    quotes: &[TradierQuote],
    long_symbol: &str,
    short_symbol: &str,
    mode: ExecutionMode,
) -> Result<f64> {
    let long_quote = tradier_quote_for_symbol(quotes, long_symbol)?;
    let short_quote = tradier_quote_for_symbol(quotes, short_symbol)?;
    let long_bid = tradier_quote_nonnegative_value(long_quote.bid, long_symbol, "bid")?;
    let short_ask = tradier_quote_nonnegative_value(short_quote.ask, short_symbol, "ask")?;
    if mode == ExecutionMode::Live {
        if long_bid > 0.0 {
            tradier_quote_positive_value(long_quote.bid_size, long_symbol, "bid_size")?;
        }
        if short_ask > 0.0 {
            tradier_quote_positive_value(short_quote.ask_size, short_symbol, "ask_size")?;
        }
        tradier_assert_fresh_quote_side(long_quote, long_symbol, "bid")?;
        tradier_assert_fresh_quote_side(short_quote, short_symbol, "ask")?;
    }
    let exit_credit = long_bid - short_ask;
    if !exit_credit.is_finite() {
        anyhow::bail!("current conservative exit credit {exit_credit:.2} is not finite");
    }
    Ok(exit_credit)
}

fn tradier_current_credit_exit_debit(
    quotes: &[TradierQuote],
    short_symbol: &str,
    long_symbol: &str,
    width: f64,
    mode: ExecutionMode,
) -> Result<f64> {
    if !width.is_finite() || width <= 0.0 {
        anyhow::bail!("credit-spread width must be positive and finite");
    }
    let short_quote = tradier_quote_for_symbol(quotes, short_symbol)?;
    let long_quote = tradier_quote_for_symbol(quotes, long_symbol)?;
    let short_ask = tradier_quote_nonnegative_value(short_quote.ask, short_symbol, "ask")?;
    let long_bid = tradier_quote_nonnegative_value(long_quote.bid, long_symbol, "bid")?;
    if mode == ExecutionMode::Live {
        if short_ask > 0.0 {
            tradier_quote_positive_value(short_quote.ask_size, short_symbol, "ask_size")?;
        }
        if long_bid > 0.0 {
            tradier_quote_positive_value(long_quote.bid_size, long_symbol, "bid_size")?;
        }
        tradier_assert_fresh_quote_side(short_quote, short_symbol, "ask")?;
        tradier_assert_fresh_quote_side(long_quote, long_symbol, "bid")?;
    }
    let exit_debit = conservative_short_spread_exit_debit_f64(short_ask, long_bid, width);
    if !exit_debit.is_finite() {
        anyhow::bail!("current conservative exit debit {exit_debit:.2} is not finite");
    }
    Ok(exit_debit)
}

fn tradier_validate_current_single_option_quote(
    client: &TradierClient,
    option_symbol: &str,
    side: &str,
    mode: ExecutionMode,
) -> Result<(TradierQuotesResponse, f64)> {
    let quotes = client
        .get_quotes(&[option_symbol.to_owned()])
        .context("fetch Tradier current option quote before single-leg preview")?;
    if !quotes.ok {
        anyhow::bail!(
            "{}",
            quotes
                .error
                .clone()
                .unwrap_or_else(|| "Tradier quotes request failed".to_owned())
        );
    }
    let quote = tradier_quote_for_symbol(&quotes.quotes, option_symbol)?;
    let executable_price = match side {
        "ask" => {
            let ask = tradier_quote_positive_value(quote.ask, option_symbol, "ask")?;
            if mode == ExecutionMode::Live {
                tradier_quote_positive_value(quote.ask_size, option_symbol, "ask_size")?;
                tradier_assert_fresh_quote_side(quote, option_symbol, "ask")?;
            }
            ask
        }
        "bid" => {
            let bid = tradier_quote_positive_value(quote.bid, option_symbol, "bid")?;
            if mode == ExecutionMode::Live {
                tradier_quote_positive_value(quote.bid_size, option_symbol, "bid_size")?;
                tradier_assert_fresh_quote_side(quote, option_symbol, "bid")?;
            }
            bid
        }
        other => anyhow::bail!("unsupported single-option quote side {other}"),
    };
    Ok((quotes, executable_price))
}

fn tradier_validate_current_equity_sell_quote(
    client: &TradierClient,
    symbol: &str,
    mode: ExecutionMode,
) -> Result<(TradierQuotesResponse, f64)> {
    let symbol = require_action_field(symbol, "symbol")?.to_ascii_uppercase();
    let quotes = client
        .get_quotes(std::slice::from_ref(&symbol))
        .context("fetch Tradier current equity quote before stock liquidation preview")?;
    if !quotes.ok {
        anyhow::bail!(
            "{}",
            quotes
                .error
                .clone()
                .unwrap_or_else(|| "Tradier quotes request failed".to_owned())
        );
    }
    let quote = tradier_quote_for_symbol(&quotes.quotes, &symbol)?;
    let bid = tradier_quote_positive_value(quote.bid, &symbol, "bid")?;
    if mode == ExecutionMode::Live {
        tradier_quote_positive_value(quote.bid_size, &symbol, "bid_size")?;
        tradier_assert_fresh_quote_side(quote, &symbol, "bid")?;
    }
    Ok((quotes, bid))
}

fn tradier_assert_fresh_quote_side(quote: &TradierQuote, symbol: &str, side: &str) -> Result<()> {
    let timestamp = match side {
        "ask" => quote.ask_date.or(quote.trade_date),
        "bid" => quote.bid_date.or(quote.trade_date),
        _ => None,
    }
    .ok_or_else(|| anyhow::anyhow!("Tradier quote {symbol} missing {side} timestamp"))?;
    let quote_time = tradier_quote_timestamp_utc(timestamp)
        .ok_or_else(|| anyhow::anyhow!("Tradier quote {symbol} has invalid {side} timestamp"))?;
    let age = Utc::now().signed_duration_since(quote_time);
    if age.num_seconds() < 0 {
        anyhow::bail!("Tradier quote {symbol} {side} timestamp is in the future");
    }
    let age_seconds = age.num_seconds();
    let max_age_seconds = env_i64(
        "SPREAD_EXECUTION_MAX_QUOTE_AGE_SECONDS",
        DEFAULT_MAX_QUOTE_AGE_SECONDS,
    )?;
    if max_age_seconds <= 0 {
        anyhow::bail!("SPREAD_EXECUTION_MAX_QUOTE_AGE_SECONDS must be positive");
    }
    if age_seconds > max_age_seconds {
        anyhow::bail!(
            "Tradier quote {symbol} {side} age {}s exceeds max {}s",
            age_seconds,
            max_age_seconds
        );
    }
    Ok(())
}

fn tradier_quote_timestamp_utc(timestamp: i64) -> Option<chrono::DateTime<Utc>> {
    if timestamp <= 0 {
        return None;
    }
    if timestamp > 10_000_000_000 {
        chrono::DateTime::<Utc>::from_timestamp_millis(timestamp)
    } else {
        chrono::DateTime::<Utc>::from_timestamp(timestamp, 0)
    }
}

fn tradier_quote_for_symbol<'a>(
    quotes: &'a [TradierQuote],
    symbol: &str,
) -> Result<&'a TradierQuote> {
    quotes
        .iter()
        .find(|quote| quote.symbol.eq_ignore_ascii_case(symbol))
        .ok_or_else(|| anyhow::anyhow!("Tradier quotes response missing {symbol}"))
}

fn tradier_quote_positive_value(value: Option<f64>, symbol: &str, field: &str) -> Result<f64> {
    let value = value.ok_or_else(|| anyhow::anyhow!("Tradier quote {symbol} missing {field}"))?;
    if !value.is_finite() || value <= 0.0 {
        anyhow::bail!("Tradier quote {symbol} has invalid {field} {value}");
    }
    Ok(value)
}

fn tradier_quote_nonnegative_value(value: Option<f64>, symbol: &str, field: &str) -> Result<f64> {
    let value = value.ok_or_else(|| anyhow::anyhow!("Tradier quote {symbol} missing {field}"))?;
    if !value.is_finite() || value < 0.0 {
        anyhow::bail!("Tradier quote {symbol} has invalid {field} {value}");
    }
    Ok(value)
}

fn tradier_position_matches_symbol(position: &TradierPosition, symbol: &str) -> bool {
    position.quantity.abs() > f64::EPSILON
        && tradier_security_matches_underlying(position.symbol.as_str(), symbol)
}

fn tradier_order_blocks_new_wheel_entry(order: &TradierOrder, symbol: &str) -> bool {
    tradier_order_blocks_new_symbol_entry(order, symbol)
}

fn tradier_order_blocks_new_symbol_entry(order: &TradierOrder, symbol: &str) -> bool {
    let matching_security = order
        .symbol
        .as_deref()
        .is_some_and(|security| tradier_security_matches_underlying(security, symbol))
        || order
            .option_symbol
            .as_deref()
            .is_some_and(|security| tradier_security_matches_underlying(security, symbol));
    matching_security
        && order.quantity.unwrap_or(0.0).abs() > f64::EPSILON
        && order
            .status
            .as_deref()
            .is_none_or(tradier_order_status_blocks_new_entry)
}

fn tradier_security_matches_underlying(security: &str, symbol: &str) -> bool {
    let security = security.to_ascii_uppercase();
    if security == symbol {
        return true;
    }
    security
        .strip_prefix(symbol)
        .and_then(|suffix| suffix.chars().next())
        .is_some_and(|ch| ch.is_ascii_digit())
}

fn tradier_occ_option_symbol_matches_underlying_right(
    security: &str,
    symbol: &str,
    right: OptionRight,
) -> bool {
    let security = security.trim().to_ascii_uppercase();
    let symbol = symbol.trim().to_ascii_uppercase();
    if security.len() <= 15 {
        return false;
    }
    let prefix_len = security.len() - 15;
    if &security[..prefix_len] != symbol.as_str() {
        return false;
    }
    let suffix = &security[prefix_len..];
    let bytes = suffix.as_bytes();
    let expected_right = match right {
        OptionRight::Call => b'C',
        OptionRight::Put => b'P',
    };
    bytes.len() == 15
        && bytes[..6].iter().all(u8::is_ascii_digit)
        && bytes[6] == expected_right
        && bytes[7..].iter().all(u8::is_ascii_digit)
}

fn tradier_cash_secured_put_payload(action: &TradeSignal) -> Result<BTreeMap<String, String>> {
    if action.strategy != "wheel" {
        anyhow::bail!("Tradier cash-secured put payload requires wheel strategy");
    }
    let intent = trade_signal_order_intent(action)?;
    if intent.order_effect != OptionOrderEffect::Credit {
        anyhow::bail!("Tradier wheel order must be a credit order");
    }
    if intent.quantity() != 1 {
        anyhow::bail!("Tradier wheel order quantity must be one contract");
    }
    let [leg] = intent.legs.as_slice() else {
        anyhow::bail!("Tradier wheel order must have exactly one option leg");
    };
    if leg.side != OptionOrderSide::Sell
        || leg.position_effect != PositionEffect::Open
        || leg.key.right != OptionRight::Put
    {
        anyhow::bail!("Tradier wheel entry must be one sell_to_open put");
    }
    let mut payload = BTreeMap::new();
    payload.insert("class".to_owned(), "option".to_owned());
    payload.insert("symbol".to_owned(), intent.symbol.to_ascii_uppercase());
    payload.insert(
        "option_symbol".to_owned(),
        tradier_occ_option_symbol(&leg.key)?,
    );
    payload.insert("side".to_owned(), "sell_to_open".to_owned());
    payload.insert("quantity".to_owned(), leg.quantity.to_string());
    payload.insert("type".to_owned(), "limit".to_owned());
    payload.insert(
        "duration".to_owned(),
        time_in_force_value(&intent.time_in_force).to_owned(),
    );
    payload.insert(
        "price".to_owned(),
        format_tradier_price(intent.limit_price, "limit_price")?,
    );
    Ok(payload)
}

fn tradier_multileg_debit_payload(action: &TradeSignal) -> Result<BTreeMap<String, String>> {
    if !matches!(
        action.strategy.as_str(),
        "call_debit_spread" | "put_debit_spread"
    ) {
        anyhow::bail!(
            "Tradier debit-spread payload only supports call_debit_spread and put_debit_spread"
        );
    }
    let intent = trade_signal_order_intent(action)?;
    if intent.order_effect != OptionOrderEffect::Debit {
        anyhow::bail!("Tradier V1 spread must be a debit order");
    }
    if intent.quantity() != 1 {
        anyhow::bail!("Tradier V1 order quantity must be one spread");
    }
    let [long_leg, short_leg] = intent.legs.as_slice() else {
        anyhow::bail!("Tradier V1 debit spread must have exactly two legs");
    };
    if long_leg.side != OptionOrderSide::Buy
        || long_leg.position_effect != PositionEffect::Open
        || short_leg.side != OptionOrderSide::Sell
        || short_leg.position_effect != PositionEffect::Open
    {
        anyhow::bail!("Tradier debit spread legs must be buy_to_open then sell_to_open");
    }
    if long_leg.key.right != short_leg.key.right
        || long_leg.key.expiration != short_leg.key.expiration
        || long_leg.key.underlying != short_leg.key.underlying
    {
        anyhow::bail!("Tradier debit spread legs must share underlying, expiration, and right");
    }

    let mut payload = BTreeMap::new();
    payload.insert("class".to_owned(), "multileg".to_owned());
    payload.insert("symbol".to_owned(), intent.symbol.to_ascii_uppercase());
    payload.insert("type".to_owned(), "debit".to_owned());
    payload.insert(
        "duration".to_owned(),
        time_in_force_value(&intent.time_in_force).to_owned(),
    );
    payload.insert(
        "price".to_owned(),
        format_tradier_price(intent.limit_price, "limit_price")?,
    );
    payload.insert(
        "option_symbol[0]".to_owned(),
        tradier_occ_option_symbol(&long_leg.key)?,
    );
    payload.insert("side[0]".to_owned(), "buy_to_open".to_owned());
    payload.insert("quantity[0]".to_owned(), long_leg.quantity.to_string());
    payload.insert(
        "option_symbol[1]".to_owned(),
        tradier_occ_option_symbol(&short_leg.key)?,
    );
    payload.insert("side[1]".to_owned(), "sell_to_open".to_owned());
    payload.insert("quantity[1]".to_owned(), short_leg.quantity.to_string());
    Ok(payload)
}

fn tradier_multileg_credit_payload(action: &TradeSignal) -> Result<BTreeMap<String, String>> {
    if !is_credit_spread_strategy(action.strategy.as_str()) {
        anyhow::bail!(
            "Tradier credit-spread payload only supports call_credit_spread and put_credit_spread"
        );
    }
    let intent = trade_signal_order_intent(action)?;
    if intent.order_effect != OptionOrderEffect::Credit {
        anyhow::bail!("Tradier credit spread entry must be a credit order");
    }
    if intent.quantity() != 1 {
        anyhow::bail!("Tradier V1 order quantity must be one spread");
    }
    let [short_leg, long_leg] = intent.legs.as_slice() else {
        anyhow::bail!("Tradier V1 credit spread must have exactly two legs");
    };
    if short_leg.side != OptionOrderSide::Sell
        || short_leg.position_effect != PositionEffect::Open
        || long_leg.side != OptionOrderSide::Buy
        || long_leg.position_effect != PositionEffect::Open
    {
        anyhow::bail!("Tradier credit spread legs must be sell_to_open then buy_to_open");
    }
    if short_leg.key.right != long_leg.key.right
        || short_leg.key.expiration != long_leg.key.expiration
        || short_leg.key.underlying != long_leg.key.underlying
    {
        anyhow::bail!("Tradier credit spread legs must share underlying, expiration, and right");
    }

    let mut payload = BTreeMap::new();
    payload.insert("class".to_owned(), "multileg".to_owned());
    payload.insert("symbol".to_owned(), intent.symbol.to_ascii_uppercase());
    payload.insert("type".to_owned(), "credit".to_owned());
    payload.insert(
        "duration".to_owned(),
        time_in_force_value(&intent.time_in_force).to_owned(),
    );
    payload.insert(
        "price".to_owned(),
        format_tradier_price(intent.limit_price, "limit_price")?,
    );
    payload.insert(
        "option_symbol[0]".to_owned(),
        tradier_occ_option_symbol(&short_leg.key)?,
    );
    payload.insert("side[0]".to_owned(), "sell_to_open".to_owned());
    payload.insert("quantity[0]".to_owned(), short_leg.quantity.to_string());
    payload.insert(
        "option_symbol[1]".to_owned(),
        tradier_occ_option_symbol(&long_leg.key)?,
    );
    payload.insert("side[1]".to_owned(), "buy_to_open".to_owned());
    payload.insert("quantity[1]".to_owned(), long_leg.quantity.to_string());
    Ok(payload)
}

fn tradier_multileg_debit_close_payload(
    action: &TradeSignal,
    limit_credit: f64,
    quantity: u32,
) -> Result<BTreeMap<String, String>> {
    let (long_key, short_key) = trade_signal_debit_spread_keys(action)?;
    let intent = debit_spread_close_intent(
        long_key,
        short_key,
        quantity,
        decimal_from_f64(limit_credit, "limit_credit")?,
        action.strategy.clone(),
    )?;
    if intent.order_effect != OptionOrderEffect::Credit {
        anyhow::bail!("Tradier debit-spread close must be a credit order");
    }
    let [long_leg, short_leg] = intent.legs.as_slice() else {
        anyhow::bail!("Tradier debit-spread close must have exactly two legs");
    };
    if long_leg.side != OptionOrderSide::Sell
        || long_leg.position_effect != PositionEffect::Close
        || short_leg.side != OptionOrderSide::Buy
        || short_leg.position_effect != PositionEffect::Close
    {
        anyhow::bail!("Tradier debit-spread close legs must be sell_to_close then buy_to_close");
    }

    let mut payload = BTreeMap::new();
    payload.insert("class".to_owned(), "multileg".to_owned());
    payload.insert("symbol".to_owned(), intent.symbol.to_ascii_uppercase());
    payload.insert("type".to_owned(), "credit".to_owned());
    payload.insert(
        "duration".to_owned(),
        time_in_force_value(&intent.time_in_force).to_owned(),
    );
    payload.insert(
        "price".to_owned(),
        format_tradier_price(intent.limit_price, "limit_credit")?,
    );
    payload.insert(
        "option_symbol[0]".to_owned(),
        tradier_occ_option_symbol(&long_leg.key)?,
    );
    payload.insert("side[0]".to_owned(), "sell_to_close".to_owned());
    payload.insert("quantity[0]".to_owned(), long_leg.quantity.to_string());
    payload.insert(
        "option_symbol[1]".to_owned(),
        tradier_occ_option_symbol(&short_leg.key)?,
    );
    payload.insert("side[1]".to_owned(), "buy_to_close".to_owned());
    payload.insert("quantity[1]".to_owned(), short_leg.quantity.to_string());
    Ok(payload)
}

fn tradier_multileg_credit_close_payload(
    action: &TradeSignal,
    limit_debit: f64,
    quantity: u32,
) -> Result<BTreeMap<String, String>> {
    let (short_key, long_key) = trade_signal_credit_spread_keys(action)?;
    let intent = credit_spread_close_intent(
        short_key,
        long_key,
        quantity,
        decimal_from_f64(limit_debit, "limit_debit")?,
        action.strategy.clone(),
    )?;
    if intent.order_effect != OptionOrderEffect::Debit {
        anyhow::bail!("Tradier credit-spread close must be a debit order");
    }
    let [short_leg, long_leg] = intent.legs.as_slice() else {
        anyhow::bail!("Tradier credit-spread close must have exactly two legs");
    };
    if short_leg.side != OptionOrderSide::Buy
        || short_leg.position_effect != PositionEffect::Close
        || long_leg.side != OptionOrderSide::Sell
        || long_leg.position_effect != PositionEffect::Close
    {
        anyhow::bail!("Tradier credit-spread close legs must be buy_to_close then sell_to_close");
    }

    let mut payload = BTreeMap::new();
    payload.insert("class".to_owned(), "multileg".to_owned());
    payload.insert("symbol".to_owned(), intent.symbol.to_ascii_uppercase());
    payload.insert("type".to_owned(), "debit".to_owned());
    payload.insert(
        "duration".to_owned(),
        time_in_force_value(&intent.time_in_force).to_owned(),
    );
    payload.insert(
        "price".to_owned(),
        format_tradier_price(intent.limit_price, "limit_debit")?,
    );
    payload.insert(
        "option_symbol[0]".to_owned(),
        tradier_occ_option_symbol(&short_leg.key)?,
    );
    payload.insert("side[0]".to_owned(), "buy_to_close".to_owned());
    payload.insert("quantity[0]".to_owned(), short_leg.quantity.to_string());
    payload.insert(
        "option_symbol[1]".to_owned(),
        tradier_occ_option_symbol(&long_leg.key)?,
    );
    payload.insert("side[1]".to_owned(), "sell_to_close".to_owned());
    payload.insert("quantity[1]".to_owned(), long_leg.quantity.to_string());
    Ok(payload)
}

fn tradier_single_option_payload(
    symbol: &str,
    option_symbol: &str,
    side: &str,
    quantity: u32,
    limit_price: f64,
) -> Result<BTreeMap<String, String>> {
    if quantity == 0 {
        anyhow::bail!("Tradier single-option order quantity must be positive");
    }
    if !matches!(side, "buy_to_close" | "sell_to_open") {
        anyhow::bail!("Tradier single-option side {side} is not supported");
    }
    let mut payload = BTreeMap::new();
    payload.insert("class".to_owned(), "option".to_owned());
    payload.insert("symbol".to_owned(), symbol.to_ascii_uppercase());
    payload.insert(
        "option_symbol".to_owned(),
        option_symbol.to_ascii_uppercase(),
    );
    payload.insert("side".to_owned(), side.to_owned());
    payload.insert("quantity".to_owned(), quantity.to_string());
    payload.insert("type".to_owned(), "limit".to_owned());
    payload.insert(
        "duration".to_owned(),
        time_in_force_value(&TimeInForce::Day).to_owned(),
    );
    payload.insert(
        "price".to_owned(),
        format_tradier_price(decimal_from_f64(limit_price, "limit_price")?, "limit_price")?,
    );
    Ok(payload)
}

fn tradier_equity_sell_payload(
    symbol: &str,
    quantity: u32,
    limit_price: f64,
) -> Result<BTreeMap<String, String>> {
    if quantity == 0 {
        anyhow::bail!("Tradier equity sell quantity must be positive");
    }
    let symbol = require_action_field(symbol, "symbol")?.to_ascii_uppercase();
    let mut payload = BTreeMap::new();
    payload.insert("class".to_owned(), "equity".to_owned());
    payload.insert("symbol".to_owned(), symbol);
    payload.insert("side".to_owned(), "sell".to_owned());
    payload.insert("quantity".to_owned(), quantity.to_string());
    payload.insert("type".to_owned(), "limit".to_owned());
    payload.insert(
        "duration".to_owned(),
        time_in_force_value(&TimeInForce::Day).to_owned(),
    );
    payload.insert(
        "price".to_owned(),
        format_tradier_price(decimal_from_f64(limit_price, "limit_price")?, "limit_price")?,
    );
    Ok(payload)
}

fn tradier_occ_option_symbol(key: &OptionKey) -> Result<String> {
    let right = match key.right {
        OptionRight::Call => "C",
        OptionRight::Put => "P",
    };
    let strike = decimal_to_f64(key.strike, "strike")?;
    if strike <= 0.0 || !strike.is_finite() {
        anyhow::bail!("strike must be positive and finite");
    }
    let scaled_strike = (strike * 1000.0).round();
    if (scaled_strike - strike * 1000.0).abs() > 0.001 {
        anyhow::bail!("strike has unsupported precision for OCC symbol: {strike}");
    }
    Ok(format!(
        "{}{}{}{:08}",
        key.underlying.to_ascii_uppercase(),
        key.expiration.format("%y%m%d"),
        right,
        scaled_strike as u64
    ))
}

fn format_tradier_price(value: Decimal, field: &str) -> Result<String> {
    let price = decimal_to_f64(value, field)?;
    if price <= 0.0 || !price.is_finite() {
        anyhow::bail!("{field} must be positive and finite");
    }
    Ok(format!("{price:.2}"))
}

fn execution_order_ledger_blocking_entry(
    path: &Path,
    order_key: &str,
) -> Result<Option<ExecutionOrderLedgerEntry>> {
    Ok(read_execution_order_ledger(path)?
        .get(order_key)
        .filter(|entry| entry.blocks_duplicate())
        .cloned())
}

fn execution_order_ledger_entry_with_statuses(
    path: &Path,
    order_key: &str,
    statuses: &[&str],
) -> Result<Option<ExecutionOrderLedgerEntry>> {
    Ok(read_execution_order_ledger(path)?
        .get(order_key)
        .filter(|entry| statuses.iter().any(|status| entry.status == *status))
        .cloned())
}

fn apply_tradier_ledger_entry_to_decision(
    decision: &mut ExecutionDecision,
    entry: &ExecutionOrderLedgerEntry,
) {
    match entry.status.as_str() {
        "reviewed" => {
            decision.status = "reviewed".to_owned();
            decision.broker_review_ok = true;
            decision.reason =
                "matching Tradier order intent was already reviewed in the local execution ledger"
                    .to_owned();
        }
        "rejected" => {
            decision.status = "rejected".to_owned();
            decision.reason = entry.reason.clone().unwrap_or_else(|| {
                "matching Tradier order intent was already rejected in the local execution ledger"
                    .to_owned()
            });
        }
        "pending_unknown" => {
            decision.status = "submit_unknown".to_owned();
            decision.reason = entry.reason.clone().unwrap_or_else(|| {
                "matching Tradier order intent has an unresolved pending_unknown ledger entry"
                    .to_owned()
            });
        }
        _ => {
            decision.status = "already_submitted".to_owned();
            decision.reason =
                "matching Tradier order intent is already recorded in the local execution ledger"
                    .to_owned();
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ExecutionOrderLedgerReservation {
    Reserved,
    AlreadyRecorded,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ExecutionOrderLedgerEntry {
    status: String,
    recorded_at: chrono::DateTime<Utc>,
    broker_order_id: Option<String>,
    reason: Option<String>,
}

impl ExecutionOrderLedgerEntry {
    fn blocks_duplicate(&self) -> bool {
        matches!(
            self.status.as_str(),
            "pending_unknown" | "submitted" | "rejected"
        )
    }
}

fn read_execution_order_ledger(path: &Path) -> Result<BTreeMap<String, ExecutionOrderLedgerEntry>> {
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let body = fs::read_to_string(path)
        .with_context(|| format!("read execution order ledger {}", path.display()))?;
    if let Ok(entries) = serde_json::from_str::<BTreeMap<String, ExecutionOrderLedgerEntry>>(&body)
    {
        return Ok(entries);
    }
    let legacy: BTreeSet<String> = serde_json::from_str(&body)
        .with_context(|| format!("parse execution order ledger {}", path.display()))?;
    Ok(legacy
        .into_iter()
        .map(|order_key| {
            (
                order_key,
                ExecutionOrderLedgerEntry {
                    status: "submitted".to_owned(),
                    recorded_at: Utc::now(),
                    broker_order_id: None,
                    reason: Some("legacy ledger entry".to_owned()),
                },
            )
        })
        .collect())
}

fn execution_order_ledger_record_status(
    path: &Path,
    order_key: &str,
    status: &str,
    broker_order_id: Option<&str>,
    reason: Option<&str>,
) -> Result<()> {
    update_execution_order_ledger(path, |ledger| {
        ledger.insert(
            order_key.to_owned(),
            ExecutionOrderLedgerEntry {
                status: status.to_owned(),
                recorded_at: Utc::now(),
                broker_order_id: broker_order_id.map(ToOwned::to_owned),
                reason: reason.map(ToOwned::to_owned),
            },
        );
        Ok(())
    })
}

fn execution_order_ledger_reserve_pending(
    path: &Path,
    order_key: &str,
    reason: Option<&str>,
) -> Result<ExecutionOrderLedgerReservation> {
    update_execution_order_ledger(path, |ledger| {
        if ledger
            .get(order_key)
            .is_some_and(|entry| entry.blocks_duplicate())
        {
            return Ok(ExecutionOrderLedgerReservation::AlreadyRecorded);
        }
        ledger.insert(
            order_key.to_owned(),
            ExecutionOrderLedgerEntry {
                status: "pending_unknown".to_owned(),
                recorded_at: Utc::now(),
                broker_order_id: None,
                reason: reason.map(ToOwned::to_owned),
            },
        );
        Ok(ExecutionOrderLedgerReservation::Reserved)
    })
}

fn update_execution_order_ledger<T>(
    path: &Path,
    update: impl FnOnce(&mut BTreeMap<String, ExecutionOrderLedgerEntry>) -> Result<T>,
) -> Result<T> {
    let _lock = acquire_execution_order_ledger_lock(path)?;
    let mut ledger = read_execution_order_ledger(path)?;
    let result = update(&mut ledger)?;
    write_execution_order_ledger(path, &ledger)?;
    Ok(result)
}

struct ExecutionOrderLedgerLock {
    path: PathBuf,
}

impl Drop for ExecutionOrderLedgerLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn acquire_execution_order_ledger_lock(path: &Path) -> Result<ExecutionOrderLedgerLock> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "create execution order ledger directory {}",
                parent.display()
            )
        })?;
    }
    let lock_path = path.with_extension("json.lock");
    let mut file = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lock_path)
    {
        Ok(file) => file,
        Err(first_err) => {
            if remove_stale_execution_order_ledger_lock(&lock_path)? {
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&lock_path)
                    .with_context(|| {
                        format!(
                            "acquire execution order ledger lock {} after stale-lock cleanup",
                            lock_path.display()
                        )
                    })?
            } else {
                return Err(first_err).with_context(|| {
                    format!(
                        "acquire execution order ledger lock {}; another worker may be submitting",
                        lock_path.display()
                    )
                });
            }
        }
    };
    writeln!(
        file,
        "pid={} acquired_at={}",
        std::process::id(),
        Utc::now().to_rfc3339()
    )
    .with_context(|| format!("write execution order ledger lock {}", lock_path.display()))?;
    Ok(ExecutionOrderLedgerLock { path: lock_path })
}

fn remove_stale_execution_order_ledger_lock(lock_path: &Path) -> Result<bool> {
    let body = match fs::read_to_string(lock_path) {
        Ok(body) => body,
        Err(_) => return Ok(false),
    };
    let pid = body
        .split_whitespace()
        .find_map(|part| part.strip_prefix("pid="))
        .and_then(|pid| pid.parse::<u32>().ok());
    let acquired_at = body
        .split_whitespace()
        .find_map(|part| part.strip_prefix("acquired_at="))
        .and_then(|timestamp| chrono::DateTime::parse_from_rfc3339(timestamp).ok())
        .map(|timestamp| timestamp.with_timezone(&Utc));
    let stale_by_pid = pid.is_some_and(|pid| !process_running(pid));
    let stale_by_age = acquired_at
        .is_some_and(|timestamp| Utc::now().signed_duration_since(timestamp).num_seconds() > 300);
    if stale_by_pid || stale_by_age {
        fs::remove_file(lock_path).with_context(|| {
            format!(
                "remove stale execution order ledger lock {}",
                lock_path.display()
            )
        })?;
        Ok(true)
    } else {
        Ok(false)
    }
}

fn process_running(pid: u32) -> bool {
    ProcessCommand::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

struct LiveSignalRefreshLock {
    path: PathBuf,
}

impl Drop for LiveSignalRefreshLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn acquire_live_signal_refresh_lock(
    state_file: &Path,
    stale_after_seconds: u64,
) -> Result<LiveSignalRefreshLock> {
    if let Some(parent) = state_file.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "create live signal refresh state directory {}",
                parent.display()
            )
        })?;
    }
    let lock_path = state_file.with_extension("refresh.lock");
    let mut file = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&lock_path)
    {
        Ok(file) => file,
        Err(first_err) => {
            if remove_stale_live_signal_refresh_lock(&lock_path, stale_after_seconds)? {
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&lock_path)
                    .with_context(|| {
                        format!(
                            "acquire live signal refresh lock {} after stale-lock cleanup",
                            lock_path.display()
                        )
                    })?
            } else {
                return Err(first_err).with_context(|| {
                    format!(
                        "acquire live signal refresh lock {}; another refresh may be running",
                        lock_path.display()
                    )
                });
            }
        }
    };
    writeln!(
        file,
        "pid={} acquired_at={}",
        std::process::id(),
        Utc::now().to_rfc3339()
    )
    .with_context(|| format!("write live signal refresh lock {}", lock_path.display()))?;
    Ok(LiveSignalRefreshLock { path: lock_path })
}

fn remove_stale_live_signal_refresh_lock(
    lock_path: &Path,
    stale_after_seconds: u64,
) -> Result<bool> {
    let body = match fs::read_to_string(lock_path) {
        Ok(body) => body,
        Err(_) => return Ok(false),
    };
    let pid = body
        .split_whitespace()
        .find_map(|part| part.strip_prefix("pid="))
        .and_then(|pid| pid.parse::<u32>().ok());
    let acquired_at = body
        .split_whitespace()
        .find_map(|part| part.strip_prefix("acquired_at="))
        .and_then(|timestamp| chrono::DateTime::parse_from_rfc3339(timestamp).ok())
        .map(|timestamp| timestamp.with_timezone(&Utc));
    let stale_by_pid = pid.is_some_and(|pid| !process_running(pid));
    let stale_by_age = acquired_at.is_some_and(|timestamp| {
        Utc::now().signed_duration_since(timestamp).num_seconds()
            > stale_after_seconds.min(i64::MAX as u64) as i64
    });
    if stale_by_pid || stale_by_age {
        fs::remove_file(lock_path).with_context(|| {
            format!(
                "remove stale live signal refresh lock {}",
                lock_path.display()
            )
        })?;
        Ok(true)
    } else {
        Ok(false)
    }
}

fn write_execution_order_ledger(
    path: &Path,
    ledger: &BTreeMap<String, ExecutionOrderLedgerEntry>,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "create execution order ledger directory {}",
                parent.display()
            )
        })?;
    }
    let tmp_path = path.with_extension("json.tmp");
    {
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_path)
            .with_context(|| format!("open execution order ledger temp {}", tmp_path.display()))?;
        file.write_all(serde_json::to_string_pretty(ledger)?.as_bytes())
            .with_context(|| format!("write execution order ledger temp {}", tmp_path.display()))?;
        file.sync_all()
            .with_context(|| format!("sync execution order ledger temp {}", tmp_path.display()))?;
    }
    fs::rename(&tmp_path, path)
        .with_context(|| format!("replace execution order ledger {}", path.display()))?;
    sync_parent_directory_best_effort(path);
    Ok(())
}

fn sync_parent_directory_best_effort(path: &Path) {
    let Some(parent) = path.parent() else {
        return;
    };
    if let Ok(directory) = OpenOptions::new().read(true).open(parent) {
        let _ = directory.sync_all();
    }
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
struct ExecutionWorkerArgs {
    live_signal: PathBuf,
    as_of: Option<NaiveDate>,
    risk: CanaryRiskPolicy,
    broker: ExecutionBrokerAdapter,
    mode: ExecutionMode,
    robinhood_mcp_command: Option<String>,
    order_ledger: PathBuf,
    notify_command: Option<String>,
    notify_ledger: PathBuf,
    max_order_age_seconds: u64,
    poll_seconds: u64,
    once: bool,
    health_output: Option<PathBuf>,
    json: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct ExecutionWorkerHealth {
    checked_at: chrono::DateTime<Utc>,
    service: String,
    status: String,
    live_signal: String,
    live_signal_readable: bool,
    live_signal_parse_ok: bool,
    as_of: NaiveDate,
    risk: CanaryRiskPolicy,
    broker_multi_leg_options: bool,
    broker_cash_secured_puts: bool,
    broker_covered_calls: bool,
    broker: BrokerKind,
    mode: ExecutionMode,
    broker_review_ok: bool,
    robinhood_mcp_command_configured: bool,
    tradier_credentials_configured: bool,
    order_ledger: String,
    #[serde(default)]
    broker_account: Option<BrokerAccountSnapshot>,
    decision: Option<ExecutionDecision>,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
struct ExecutionWorkerSnapshot {
    updated_at: chrono::DateTime<Utc>,
    health_path: String,
    pid_file: String,
    worker_running: bool,
    health_readable: bool,
    health_age_seconds: Option<i64>,
    health_stale: bool,
    status: String,
    tray_title: String,
    tray_tooltip: String,
    rows: Vec<SnapshotRow>,
    broker_rows: Vec<SnapshotRow>,
    action_rows: Vec<SnapshotRow>,
}

#[derive(Debug, Serialize)]
struct SnapshotRow {
    label: String,
    value: String,
    tone: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BrokerAccountSnapshot {
    broker: BrokerKind,
    status: String,
    account: String,
    equity: Option<f64>,
    buying_power: Option<f64>,
    cash: Option<f64>,
    day_pnl: Option<f64>,
    open_pnl: Option<f64>,
    close_pnl: Option<f64>,
    requirement: Option<f64>,
    error: Option<String>,
}

fn execution_worker_snapshot(
    health_output: &Path,
    pid_file: &Path,
    max_age_seconds: u64,
    json: bool,
) -> Result<()> {
    let snapshot = build_execution_worker_snapshot(health_output, pid_file, max_age_seconds);
    if json {
        println!("{}", serde_json::to_string_pretty(&snapshot)?);
    } else {
        println!(
            "{} status={} running={} health_age={}",
            snapshot.tray_title,
            snapshot.status,
            snapshot.worker_running,
            snapshot
                .health_age_seconds
                .map(|age| format!("{age}s"))
                .unwrap_or_else(|| "missing".to_owned())
        );
        for row in snapshot
            .rows
            .iter()
            .chain(snapshot.broker_rows.iter())
            .chain(snapshot.action_rows.iter())
        {
            println!("{}: {}", row.label, row.value);
        }
    }
    Ok(())
}

fn build_execution_worker_snapshot(
    health_output: &Path,
    pid_file: &Path,
    max_age_seconds: u64,
) -> ExecutionWorkerSnapshot {
    let now = Utc::now();
    let worker_running = pid_file_running(pid_file);
    let refresh_snapshot = read_signal_refresh_snapshot();
    let health = fs::read_to_string(health_output)
        .ok()
        .and_then(|body| serde_json::from_str::<ExecutionWorkerHealth>(&body).ok());
    let health_readable = health.is_some();
    let health_age_seconds = health
        .as_ref()
        .map(|health| now.signed_duration_since(health.checked_at).num_seconds());
    let health_stale = health_age_seconds
        .map(|age| age < 0 || age as u64 > max_age_seconds)
        .unwrap_or(true);
    let status = snapshot_status(health.as_ref(), worker_running, health_stale);
    let market_closed_block = health
        .as_ref()
        .is_some_and(|health| execution_blocked_by_market_session(health, &refresh_snapshot));
    let tray_title = snapshot_tray_title(status.as_str(), market_closed_block);
    let tray_tooltip = snapshot_tooltip(
        health.as_ref(),
        worker_running,
        health_stale,
        &refresh_snapshot,
    );
    let mut rows = vec![
        snapshot_row(
            "Signal Refresh",
            signal_refresh_snapshot_label(&refresh_snapshot).as_str(),
            signal_refresh_snapshot_tone(&refresh_snapshot),
        ),
        snapshot_row(
            "Worker",
            if worker_running {
                "running"
            } else {
                "not running"
            },
            if worker_running { "ok" } else { "bad" },
        ),
        snapshot_row(
            "Health",
            if health_readable {
                if health_stale { "stale" } else { "fresh" }
            } else {
                "missing"
            },
            if health_readable && !health_stale {
                "ok"
            } else {
                "bad"
            },
        ),
    ];
    if let Some(health) = &health {
        rows.extend([
            snapshot_row(
                "Decision",
                snapshot_decision_label_with_refresh(health, &refresh_snapshot).as_str(),
                snapshot_tone(&health.status),
            ),
            snapshot_row(
                "Broker",
                broker_capability_summary(health).as_str(),
                if broker_execution_configured(health) {
                    "ok"
                } else {
                    "warn"
                },
            ),
            snapshot_row(
                "Mode",
                execution_mode_label(health.mode),
                match health.mode {
                    ExecutionMode::Monitor => "ok",
                    ExecutionMode::Review => "warn",
                    ExecutionMode::Live => "warn",
                },
            ),
            snapshot_row(
                "Last Check",
                health_age_label(health_age_seconds).as_str(),
                if health_stale { "bad" } else { "ok" },
            ),
        ]);
    }

    let broker_account = health
        .as_ref()
        .and_then(|health| health.broker_account.as_ref());
    let broker_rows = snapshot_broker_rows(broker_account);
    let action_rows = snapshot_action_rows(health.as_ref());
    ExecutionWorkerSnapshot {
        updated_at: now,
        health_path: health_output.display().to_string(),
        pid_file: pid_file.display().to_string(),
        worker_running,
        health_readable,
        health_age_seconds,
        health_stale,
        status,
        tray_title,
        tray_tooltip,
        rows,
        broker_rows,
        action_rows,
    }
}

fn snapshot_status(
    health: Option<&ExecutionWorkerHealth>,
    worker_running: bool,
    health_stale: bool,
) -> String {
    if !worker_running || health_stale {
        return "unhealthy".to_owned();
    }
    match health {
        Some(health) if health.error.is_none() => health.status.clone(),
        _ => "unhealthy".to_owned(),
    }
}

fn snapshot_tray_title(status: &str, market_closed_block: bool) -> String {
    if market_closed_block {
        return "SF market closed".to_owned();
    }
    match status {
        "monitor" => "SF monitoring",
        "review" => "SF review",
        "live" => "SF live",
        "blocked" => "SF blocked",
        _ => "SF down",
    }
    .to_owned()
}

fn snapshot_tooltip(
    health: Option<&ExecutionWorkerHealth>,
    worker_running: bool,
    health_stale: bool,
    refresh_snapshot: &SignalRefreshSnapshot,
) -> String {
    if !worker_running {
        return "Execution worker is not running".to_owned();
    }
    if health_stale {
        return "Execution worker health is stale or missing".to_owned();
    }
    match health {
        Some(health) if execution_blocked_by_market_session(health, refresh_snapshot) => {
            market_session_block_tooltip(health, refresh_snapshot)
        }
        Some(health) => match health.decision.as_ref() {
            Some(decision) => decision.reason.clone(),
            None => "Execution worker has no decision".to_owned(),
        },
        None => "Execution worker has no decision".to_owned(),
    }
}

fn market_session_block_tooltip(
    health: &ExecutionWorkerHealth,
    refresh_snapshot: &SignalRefreshSnapshot,
) -> String {
    let decision_reason = health
        .decision
        .as_ref()
        .map(|decision| decision.reason.as_str())
        .unwrap_or("execution worker is blocked");
    match signal_refresh_snapshot_reason(refresh_snapshot) {
        Some(refresh_reason) => {
            format!("Market/session closed: {refresh_reason}. {decision_reason}")
        }
        None => format!("Market/session closed. {decision_reason}"),
    }
}

fn execution_blocked_by_market_session(
    health: &ExecutionWorkerHealth,
    refresh_snapshot: &SignalRefreshSnapshot,
) -> bool {
    let Some(decision) = health.decision.as_ref() else {
        return false;
    };
    if decision.status != "blocked" {
        return false;
    }
    if decision
        .reason
        .contains("configured regular options-market window")
    {
        return true;
    }
    signal_refresh_snapshot_status(refresh_snapshot) == Some("skipped_market_closed")
        && signal_refresh_snapshot_run_to(refresh_snapshot) == Some(health.as_of)
        && (decision.reason.starts_with("live signal as_of ")
            || decision
                .reason
                .starts_with("live signal market_data_through "))
}

fn snapshot_decision_label_with_refresh(
    health: &ExecutionWorkerHealth,
    refresh_snapshot: &SignalRefreshSnapshot,
) -> String {
    if execution_blocked_by_market_session(health, refresh_snapshot) {
        return "Market closed".to_owned();
    }
    snapshot_decision_label(health)
}

fn snapshot_action_rows(health: Option<&ExecutionWorkerHealth>) -> Vec<SnapshotRow> {
    let Some(decision) = health.and_then(|health| health.decision.as_ref()) else {
        return Vec::new();
    };
    let Some(action) = decision.selected_signal.as_ref() else {
        return Vec::new();
    };
    let mut rows = vec![
        snapshot_row(
            "Signal",
            format!("{} {}", action.symbol, action.strategy).as_str(),
            "ok",
        ),
        snapshot_row("Signal Status", action.status.as_str(), "ok"),
    ];
    if let Some(max_loss) = action.max_loss {
        rows.push(snapshot_row(
            "Max Loss",
            format!("${max_loss:.0}").as_str(),
            "warn",
        ));
    }
    if let Some(reserve) = action.reserve {
        rows.push(snapshot_row(
            "Reserve",
            format!("${reserve:.0}").as_str(),
            "warn",
        ));
    }
    rows
}

fn snapshot_decision_label(health: &ExecutionWorkerHealth) -> String {
    match health
        .decision
        .as_ref()
        .map(|decision| decision.status.as_str())
    {
        Some("no_signal") => "No signal".to_owned(),
        Some("ready") => "Ready".to_owned(),
        Some("reviewed") => "Reviewed".to_owned(),
        Some("submitted") => "Submitted".to_owned(),
        Some("already_submitted") => "Already submitted".to_owned(),
        Some("rejected") => "Rejected".to_owned(),
        Some("submit_unknown") => "Submit unknown".to_owned(),
        Some("blocked") => "Blocked".to_owned(),
        Some(other) => other.replace('_', " "),
        None => health.status.replace('_', " "),
    }
}

fn snapshot_broker_account(broker: BrokerKind) -> Option<BrokerAccountSnapshot> {
    match broker {
        BrokerKind::Robinhood => Some(BrokerAccountSnapshot {
            broker: BrokerKind::Robinhood,
            status: "unsupported".to_owned(),
            account: "Robinhood MCP".to_owned(),
            equity: None,
            buying_power: None,
            cash: None,
            day_pnl: None,
            open_pnl: None,
            close_pnl: None,
            requirement: None,
            error: Some("account/P&L snapshot not implemented for Robinhood MCP".to_owned()),
        }),
        BrokerKind::Tradier => snapshot_tradier_account(),
    }
}

fn snapshot_tradier_account() -> Option<BrokerAccountSnapshot> {
    let config = match tradier_config_from_env() {
        Ok(config) => config,
        Err(err) => {
            return Some(BrokerAccountSnapshot {
                broker: BrokerKind::Tradier,
                status: "unconfigured".to_owned(),
                account: "Tradier".to_owned(),
                equity: None,
                buying_power: None,
                cash: None,
                day_pnl: None,
                open_pnl: None,
                close_pnl: None,
                requirement: None,
                error: Some(err.to_string()),
            });
        }
    };
    let account = masked_account_label("Tradier", &config.account_id);
    let client = match TradierClient::new_with_timeout(config, StdDuration::from_secs(5)) {
        Ok(client) => client,
        Err(err) => {
            return Some(BrokerAccountSnapshot {
                broker: BrokerKind::Tradier,
                status: "error".to_owned(),
                account,
                equity: None,
                buying_power: None,
                cash: None,
                day_pnl: None,
                open_pnl: None,
                close_pnl: None,
                requirement: None,
                error: Some(err.to_string()),
            });
        }
    };
    let response = match client.get_balances() {
        Ok(response) => response,
        Err(err) => {
            return Some(BrokerAccountSnapshot {
                broker: BrokerKind::Tradier,
                status: "error".to_owned(),
                account,
                equity: None,
                buying_power: None,
                cash: None,
                day_pnl: None,
                open_pnl: None,
                close_pnl: None,
                requirement: None,
                error: Some(err.to_string()),
            });
        }
    };
    if !response.ok {
        return Some(BrokerAccountSnapshot {
            broker: BrokerKind::Tradier,
            status: "error".to_owned(),
            account,
            equity: None,
            buying_power: None,
            cash: None,
            day_pnl: None,
            open_pnl: None,
            close_pnl: None,
            requirement: None,
            error: response.error,
        });
    }
    let Some(balances) = response.balances else {
        return Some(BrokerAccountSnapshot {
            broker: BrokerKind::Tradier,
            status: "error".to_owned(),
            account,
            equity: None,
            buying_power: None,
            cash: None,
            day_pnl: None,
            open_pnl: None,
            close_pnl: None,
            requirement: None,
            error: Some("Tradier balances response missing balances object".to_owned()),
        });
    };
    let account = balances
        .account_number
        .as_deref()
        .map(|account| masked_account_label("Tradier", account))
        .unwrap_or(account);
    let day_pnl = match (balances.close_pl, balances.open_pl) {
        (Some(close_pl), Some(open_pl)) => Some(close_pl + open_pl),
        (Some(close_pl), None) => Some(close_pl),
        (None, Some(open_pl)) => Some(open_pl),
        (None, None) => None,
    };
    Some(BrokerAccountSnapshot {
        broker: BrokerKind::Tradier,
        status: "ok".to_owned(),
        account,
        equity: balances.equity,
        buying_power: balances.option_buying_power.or(balances.total_cash),
        cash: balances.cash,
        day_pnl,
        open_pnl: balances.open_pl,
        close_pnl: balances.close_pl,
        requirement: balances.current_requirement.or(balances.option_requirement),
        error: None,
    })
}

fn snapshot_broker_rows(account: Option<&BrokerAccountSnapshot>) -> Vec<SnapshotRow> {
    let Some(account) = account else {
        return Vec::new();
    };
    let mut rows = vec![snapshot_row(
        "Account",
        account.account.as_str(),
        if account.status == "ok" { "ok" } else { "warn" },
    )];
    if let Some(equity) = account.equity {
        rows.push(snapshot_row("Equity", format_money(equity).as_str(), "ok"));
    }
    if let Some(buying_power) = account.buying_power {
        rows.push(snapshot_row(
            "Buying Power",
            format_money(buying_power).as_str(),
            "ok",
        ));
    }
    if let Some(cash) = account.cash {
        rows.push(snapshot_row("Cash", format_money(cash).as_str(), "ok"));
    }
    if let Some(day_pnl) = account.day_pnl {
        rows.push(snapshot_row(
            "Day P&L",
            format_signed_money(day_pnl).as_str(),
            pnl_tone(day_pnl),
        ));
    }
    if let Some(open_pnl) = account.open_pnl {
        rows.push(snapshot_row(
            "Open P&L",
            format_signed_money(open_pnl).as_str(),
            pnl_tone(open_pnl),
        ));
    }
    if let Some(requirement) = account.requirement {
        rows.push(snapshot_row(
            "Requirement",
            format_money(requirement).as_str(),
            "warn",
        ));
    }
    if account.status != "ok" {
        rows.push(snapshot_row(
            "Account Status",
            account.error.as_deref().unwrap_or(account.status.as_str()),
            "warn",
        ));
    }
    rows
}

fn masked_account_label(broker: &str, account_id: &str) -> String {
    let trimmed = account_id.trim();
    if trimmed.len() <= 4 {
        return format!("{broker} {trimmed}");
    }
    let suffix = &trimmed[trimmed.len() - 4..];
    format!("{broker} ****{suffix}")
}

fn format_money(value: f64) -> String {
    format!("${value:.2}")
}

fn format_signed_money(value: f64) -> String {
    if value > 0.0 {
        format!("+${value:.2}")
    } else if value < 0.0 {
        format!("-${:.2}", value.abs())
    } else {
        "$0.00".to_owned()
    }
}

fn pnl_tone(value: f64) -> &'static str {
    if value > 0.0 {
        "ok"
    } else if value < 0.0 {
        "bad"
    } else {
        "neutral"
    }
}

fn broker_capability_summary(health: &ExecutionWorkerHealth) -> String {
    let mut capabilities = Vec::new();
    if health.broker_multi_leg_options {
        capabilities.push("spreads");
    }
    if health.broker_cash_secured_puts {
        capabilities.push("cash-secured puts");
    }
    if health.broker_covered_calls {
        capabilities.push("covered calls");
    }
    if capabilities.is_empty() {
        format!("{} monitor-only", broker_label(health.broker))
    } else {
        format!(
            "{} {}",
            broker_label(health.broker),
            capabilities.join(", ")
        )
    }
}

fn broker_execution_configured(health: &ExecutionWorkerHealth) -> bool {
    match health.broker {
        BrokerKind::Robinhood => health.robinhood_mcp_command_configured,
        BrokerKind::Tradier => health.tradier_credentials_configured,
    }
}

fn execution_mode_label(mode: ExecutionMode) -> &'static str {
    match mode {
        ExecutionMode::Monitor => "monitor",
        ExecutionMode::Review => "review",
        ExecutionMode::Live => "live",
    }
}

fn health_age_label(age_seconds: Option<i64>) -> String {
    match age_seconds {
        Some(age) if age < 0 => "clock skew".to_owned(),
        Some(age) if age < 60 => format!("{age}s ago"),
        Some(age) => format!("{}m ago", age / 60),
        None => "missing".to_owned(),
    }
}

fn snapshot_tone(status: &str) -> &'static str {
    match status {
        "monitor" | "live" => "ok",
        "review" => "warn",
        _ => "bad",
    }
}

#[derive(Debug)]
enum SignalRefreshSnapshot {
    Missing,
    Unreadable,
    Parsed(serde_json::Value),
}

fn read_signal_refresh_snapshot() -> SignalRefreshSnapshot {
    let path = Path::new("var/live_signal_refresh_last.json");
    let Ok(body) = fs::read_to_string(path) else {
        return SignalRefreshSnapshot::Missing;
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&body) else {
        return SignalRefreshSnapshot::Unreadable;
    };
    SignalRefreshSnapshot::Parsed(value)
}

fn signal_refresh_snapshot_status(snapshot: &SignalRefreshSnapshot) -> Option<&str> {
    match snapshot {
        SignalRefreshSnapshot::Parsed(value) => {
            value.get("status").and_then(|status| status.as_str())
        }
        SignalRefreshSnapshot::Missing | SignalRefreshSnapshot::Unreadable => None,
    }
}

fn signal_refresh_snapshot_reason(snapshot: &SignalRefreshSnapshot) -> Option<&str> {
    match snapshot {
        SignalRefreshSnapshot::Parsed(value) => {
            value.get("reason").and_then(|reason| reason.as_str())
        }
        SignalRefreshSnapshot::Missing | SignalRefreshSnapshot::Unreadable => None,
    }
}

fn signal_refresh_snapshot_run_to(snapshot: &SignalRefreshSnapshot) -> Option<NaiveDate> {
    match snapshot {
        SignalRefreshSnapshot::Parsed(value) => value
            .get("run_to")
            .and_then(|run_to| run_to.as_str())
            .and_then(|run_to| NaiveDate::parse_from_str(run_to, "%Y-%m-%d").ok()),
        SignalRefreshSnapshot::Missing | SignalRefreshSnapshot::Unreadable => None,
    }
}

fn signal_refresh_snapshot_label(snapshot: &SignalRefreshSnapshot) -> String {
    match signal_refresh_snapshot_status(snapshot) {
        Some("skipped_market_closed") => "market closed".to_owned(),
        Some(status) => status.replace('_', " "),
        None => match snapshot {
            SignalRefreshSnapshot::Missing => "missing".to_owned(),
            SignalRefreshSnapshot::Unreadable => "unreadable".to_owned(),
            SignalRefreshSnapshot::Parsed(_) => "unknown".to_owned(),
        },
    }
}

fn signal_refresh_snapshot_tone(snapshot: &SignalRefreshSnapshot) -> &'static str {
    match signal_refresh_snapshot_status(snapshot) {
        Some("exported" | "running" | "skipped_market_closed") => "ok",
        Some("selector_timeout" | "approved_strategy_not_ready") => "warn",
        _ => "bad",
    }
}

fn snapshot_row(label: &str, value: &str, tone: &str) -> SnapshotRow {
    SnapshotRow {
        label: label.to_owned(),
        value: value.to_owned(),
        tone: tone.to_owned(),
    }
}

fn pid_file_running(pid_file: &Path) -> bool {
    let Ok(body) = fs::read_to_string(pid_file) else {
        return false;
    };
    let Ok(pid) = body.trim().parse::<u32>() else {
        return false;
    };
    ProcessCommand::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

async fn run_execution_worker(args: ExecutionWorkerArgs) -> Result<()> {
    validate_canary_risk_policy(&args.risk)?;
    if args.poll_seconds == 0 && !args.once {
        anyhow::bail!("--poll-seconds must be positive unless --once is used");
    }
    loop {
        let health = execution_worker_cycle(&args);
        if let Some(path) = &args.health_output {
            write_execution_worker_health(path, &health)?;
        }
        if let Err(err) = maybe_notify_execution_decision(&health, &args) {
            eprintln!("execution notification failed: {err:#}");
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

async fn run_execution_worker_from_env(once_flag: bool) -> Result<()> {
    let mode = parse_execution_mode_env("SPREAD_EXECUTION_MODE", ExecutionMode::Monitor)?;
    let broker_kind = parse_broker_kind_env("SPREAD_EXECUTION_BROKER", BrokerKind::Tradier)?;
    let once = once_flag || env_bool("SPREAD_EXECUTION_ONCE", false);
    run_execution_worker(ExecutionWorkerArgs {
        live_signal: PathBuf::from(env_string(
            "SPREAD_LIVE_SIGNAL_ARTIFACT",
            "var/live_signal.json",
        )),
        as_of: None,
        risk: CanaryRiskPolicy {
            account_cash: env_f64("SPREAD_EXECUTION_ACCOUNT_CASH", 45_000.0)?,
            debit_max_loss: env_f64("SPREAD_CANARY_RISK_DEBIT_MAX_LOSS", 1_000.0)?,
            wheel_reserve_cap: env_f64("SPREAD_CANARY_RISK_WHEEL_RESERVE_CAP", 35_000.0)?,
            free_cash_buffer: env_f64("SPREAD_CANARY_RISK_FREE_CASH_BUFFER", 11_250.0)?,
            max_wheel_positions_per_symbol: env_usize(
                "SPREAD_CANARY_RISK_MAX_WHEEL_POSITIONS_PER_SYMBOL",
                1,
            )?,
        },
        broker: execution_broker(
            broker_kind,
            env_bool("SPREAD_EXECUTION_BROKER_MULTI_LEG_OPTIONS", false),
            env_bool("SPREAD_EXECUTION_BROKER_CASH_SECURED_PUTS", false),
            env_bool("SPREAD_EXECUTION_BROKER_COVERED_CALLS", false),
            mode == ExecutionMode::Live,
        ),
        mode,
        robinhood_mcp_command: env_optional_string("SPREAD_ROBINHOOD_MCP_COMMAND"),
        order_ledger: PathBuf::from(env_string(
            "SPREAD_EXECUTION_ORDER_LEDGER",
            "var/execution_order_ledger.json",
        )),
        notify_command: env_optional_string("SPREAD_EXECUTION_NOTIFY_COMMAND"),
        notify_ledger: PathBuf::from(env_string(
            "SPREAD_EXECUTION_NOTIFY_LEDGER",
            "var/execution_notify_ledger.json",
        )),
        max_order_age_seconds: env_u64(
            "SPREAD_EXECUTION_MAX_ORDER_AGE_SECONDS",
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        )?,
        poll_seconds: env_u64("SPREAD_EXECUTION_POLL_SECONDS", 60)?,
        once,
        health_output: Some(PathBuf::from(env_string(
            "SPREAD_EXECUTION_HEALTH_OUTPUT",
            "var/execution_worker_health.json",
        ))),
        json: true,
    })
    .await
}

fn env_string(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default.to_owned())
}

fn env_optional_string(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .and_then(|value| match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(default)
}

fn env_f64(name: &str, default: f64) -> Result<f64> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            value
                .parse::<f64>()
                .with_context(|| format!("parse {name}={value} as number"))
        })
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn env_u64(name: &str, default: u64) -> Result<u64> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            value
                .parse::<u64>()
                .with_context(|| format!("parse {name}={value} as unsigned integer"))
        })
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn env_i64(name: &str, default: i64) -> Result<i64> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            value
                .parse::<i64>()
                .with_context(|| format!("parse {name}={value} as signed integer"))
        })
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn env_usize(name: &str, default: usize) -> Result<usize> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .map(|value| {
            value
                .parse::<usize>()
                .with_context(|| format!("parse {name}={value} as unsigned integer"))
        })
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn parse_execution_mode_env(name: &str, default: ExecutionMode) -> Result<ExecutionMode> {
    let value = env_string(name, execution_mode_label(default)).to_ascii_lowercase();
    match value.as_str() {
        "monitor" => Ok(ExecutionMode::Monitor),
        "review" => Ok(ExecutionMode::Review),
        "live" => Ok(ExecutionMode::Live),
        other => anyhow::bail!("{name} must be monitor, review, or live; got {other}"),
    }
}

fn parse_broker_kind_env(name: &str, default: BrokerKind) -> Result<BrokerKind> {
    let value = env_string(name, broker_label(default)).to_ascii_lowercase();
    match value.as_str() {
        "robinhood" => Ok(BrokerKind::Robinhood),
        "tradier" => Ok(BrokerKind::Tradier),
        other => anyhow::bail!("{name} must be robinhood or tradier; got {other}"),
    }
}

fn maybe_notify_execution_decision(
    health: &ExecutionWorkerHealth,
    args: &ExecutionWorkerArgs,
) -> Result<()> {
    let Some(command) = args.notify_command.as_deref() else {
        return Ok(());
    };
    let Some(key) = execution_notification_key(health) else {
        return Ok(());
    };
    let mut ledger = read_execution_notify_ledger(&args.notify_ledger)?;
    if ledger.contains(&key) {
        return Ok(());
    }
    let payload = execution_notification_payload(health, &key)?;
    execute_notify_command(command, &payload)?;
    ledger.insert(key);
    write_execution_notify_ledger(&args.notify_ledger, &ledger)
}

fn execution_notification_key(health: &ExecutionWorkerHealth) -> Option<String> {
    let decision = health.decision.as_ref()?;
    if !execution_status_should_notify(&decision.status) {
        return None;
    }
    let action = decision.selected_signal.as_ref()?;
    Some(
        [
            decision.status.as_str(),
            execution_mode_label(decision.mode),
            broker_label(decision.broker),
            action.symbol.as_str(),
            action.strategy.as_str(),
            action.entry_date.as_deref().unwrap_or(""),
            action.expiration.as_deref().unwrap_or(""),
            &format_optional_f64(action.short_strike),
            &format_optional_f64(action.long_strike),
        ]
        .join("|"),
    )
}

fn execution_status_should_notify(status: &str) -> bool {
    matches!(
        status,
        "ready" | "blocked" | "reviewed" | "submitted" | "submit_unknown" | "rejected"
    )
}

fn format_optional_f64(value: Option<f64>) -> String {
    value.map(|value| format!("{value:.4}")).unwrap_or_default()
}

fn execution_notification_payload(health: &ExecutionWorkerHealth, key: &str) -> Result<String> {
    let decision = health
        .decision
        .as_ref()
        .context("notification requires execution decision")?;
    let action = decision
        .selected_signal
        .as_ref()
        .context("notification requires selected action")?;
    let payload = serde_json::json!({
        "notification_key": key,
        "checked_at": health.checked_at,
        "status": decision.status,
        "mode": execution_mode_label(decision.mode),
        "broker": broker_label(decision.broker),
        "reason": decision.reason,
        "broker_review_ok": decision.broker_review_ok,
        "action": action,
    });
    serde_json::to_string(&payload).context("serialize execution notification payload")
}

fn execute_notify_command(command: &str, payload: &str) -> Result<()> {
    let mut child = ProcessCommand::new("bash")
        .arg("-lc")
        .arg(command)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("spawn notify command {command}"))?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(payload.as_bytes())
            .context("write notification payload to command stdin")?;
    }
    match child
        .wait_timeout(StdDuration::from_secs(10))
        .context("wait for notify command")?
    {
        Some(status) if status.success() => Ok(()),
        Some(status) => anyhow::bail!("notify command exited with {status}"),
        None => {
            child.kill().ok();
            child.wait().ok();
            anyhow::bail!("notify command timed out after 10s")
        }
    }
}

fn read_execution_notify_ledger(path: &Path) -> Result<BTreeSet<String>> {
    if !path.exists() {
        return Ok(BTreeSet::new());
    }
    let body = fs::read_to_string(path)
        .with_context(|| format!("read execution notify ledger {}", path.display()))?;
    let entries = serde_json::from_str::<Vec<String>>(&body)
        .with_context(|| format!("parse execution notify ledger {}", path.display()))?;
    Ok(entries.into_iter().collect())
}

fn write_execution_notify_ledger(path: &Path, ledger: &BTreeSet<String>) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "create execution notify ledger directory {}",
                parent.display()
            )
        })?;
    }
    let entries = ledger.iter().cloned().collect::<Vec<_>>();
    let tmp_path = path.with_extension("json.tmp");
    fs::write(&tmp_path, serde_json::to_string_pretty(&entries)?)
        .with_context(|| format!("write execution notify ledger temp {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path)
        .with_context(|| format!("replace execution notify ledger {}", path.display()))
}

fn execution_worker_cycle(args: &ExecutionWorkerArgs) -> ExecutionWorkerHealth {
    build_execution_worker_health(args, true)
}

#[cfg(test)]
fn execution_worker_health(args: &ExecutionWorkerArgs) -> ExecutionWorkerHealth {
    build_execution_worker_health(args, false)
}

fn build_execution_worker_health(
    args: &ExecutionWorkerArgs,
    apply_broker_side_effects: bool,
) -> ExecutionWorkerHealth {
    let as_of = args
        .as_of
        .unwrap_or_else(|| execution_default_as_of(Utc::now()));
    let live_signal = args.live_signal.display().to_string();
    let signal_body = fs::read_to_string(&args.live_signal);
    let live_signal_readable = signal_body.is_ok();
    let mut error = None;
    let mut decision = None;
    let live_signal_parse_ok = match signal_body {
        Ok(body) => match serde_json::from_str::<LiveSignalArtifact>(&body) {
            Ok(artifact) => {
                let mut execution_decision = compute_execution_decision(
                    &artifact,
                    as_of,
                    &args.risk,
                    &args.broker,
                    args.mode,
                    args.max_order_age_seconds,
                );
                if apply_broker_side_effects
                    && let Err(err) = apply_broker_bridge(
                        &mut execution_decision,
                        &args.broker,
                        args.robinhood_mcp_command.as_deref(),
                        Some(&args.order_ledger),
                    )
                {
                    error = Some(format!(
                        "{} broker bridge: {err}",
                        broker_label(args.broker.kind)
                    ));
                }
                decision = Some(execution_decision);
                true
            }
            Err(err) => {
                error = Some(format!("parse live signal artifact: {err}"));
                false
            }
        },
        Err(err) => {
            error = Some(format!("read live signal artifact: {err}"));
            false
        }
    };
    let broker_account = snapshot_broker_account(args.broker.kind);
    let status = execution_worker_aggregate_status(
        decision.as_ref(),
        error.as_deref(),
        broker_account.as_ref(),
    )
    .to_owned();
    ExecutionWorkerHealth {
        checked_at: Utc::now(),
        service: "execution_worker".to_owned(),
        status,
        live_signal,
        live_signal_readable,
        live_signal_parse_ok,
        as_of,
        risk: args.risk.clone(),
        broker_multi_leg_options: args.broker.capabilities.multi_leg_options,
        broker_cash_secured_puts: args.broker.capabilities.cash_secured_puts,
        broker_covered_calls: args.broker.capabilities.covered_calls,
        broker: args.broker.kind,
        mode: args.mode,
        broker_review_ok: decision
            .as_ref()
            .map(|decision| decision.broker_review_ok)
            .unwrap_or(false),
        robinhood_mcp_command_configured: args.robinhood_mcp_command.is_some(),
        tradier_credentials_configured: tradier_config_from_env().is_ok(),
        order_ledger: args.order_ledger.display().to_string(),
        broker_account,
        decision,
        error,
    }
}

fn execution_worker_aggregate_status(
    decision: Option<&ExecutionDecision>,
    error: Option<&str>,
    broker_account: Option<&BrokerAccountSnapshot>,
) -> &'static str {
    if error.is_some() {
        return "unhealthy";
    }
    if broker_account.is_some_and(|account| account.status == "error") {
        return "unhealthy";
    }
    match decision {
        Some(decision) if matches!(decision.status.as_str(), "submitted" | "already_submitted") => {
            "live"
        }
        Some(decision) if matches!(decision.status.as_str(), "reviewed") => "review",
        Some(decision) if decision.status == "holding" => match decision.mode {
            ExecutionMode::Monitor => "monitor",
            ExecutionMode::Review => "review",
            ExecutionMode::Live => "live",
        },
        Some(decision) if decision.status == "ready" => match decision.mode {
            ExecutionMode::Monitor => "monitor",
            ExecutionMode::Review => "review",
            ExecutionMode::Live => "live",
        },
        Some(decision) if decision.status == "no_signal" => "monitor",
        Some(decision) if matches!(decision.status.as_str(), "rejected" | "submit_unknown") => {
            "unhealthy"
        }
        Some(decision) if decision.status == "blocked" => "blocked",
        Some(_) | None => "unhealthy",
    }
}

fn write_execution_worker_health(path: &Path, health: &ExecutionWorkerHealth) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create health output directory {}", parent.display()))?;
    }
    let tmp_path = path.with_extension("json.tmp");
    fs::write(&tmp_path, serde_json::to_string_pretty(health)?)
        .with_context(|| format!("write execution worker health temp {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path)
        .with_context(|| format!("replace execution worker health {}", path.display()))
}

fn execution_broker(
    kind: BrokerKind,
    broker_multi_leg_options: bool,
    broker_cash_secured_puts: bool,
    broker_covered_calls: bool,
    live_orders_enabled: bool,
) -> ExecutionBrokerAdapter {
    let capabilities = match kind {
        BrokerKind::Robinhood => BrokerCapabilities {
            single_leg_options: true,
            multi_leg_options: broker_multi_leg_options,
            stock_option_combos: false,
            cash_secured_puts: broker_cash_secured_puts,
            covered_calls: broker_covered_calls,
        },
        BrokerKind::Tradier => BrokerCapabilities {
            single_leg_options: true,
            multi_leg_options: true,
            stock_option_combos: false,
            cash_secured_puts: true,
            covered_calls: true,
        },
    };
    ExecutionBrokerAdapter {
        kind,
        capabilities,
        live_orders_enabled,
    }
}

fn broker_label(kind: BrokerKind) -> &'static str {
    match kind {
        BrokerKind::Robinhood => "robinhood",
        BrokerKind::Tradier => "tradier",
    }
}

fn validate_canary_risk_policy(risk: &CanaryRiskPolicy) -> Result<()> {
    if !risk.account_cash.is_finite()
        || !risk.debit_max_loss.is_finite()
        || !risk.wheel_reserve_cap.is_finite()
        || !risk.free_cash_buffer.is_finite()
    {
        anyhow::bail!("canary risk amounts must be finite");
    }
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

fn print_research_store_health(report: &ResearchStoreHealth) {
    println!("Research store: {}", report.path);
    println!("cache_windows\t{}", report.cache_windows);
    println!("option_rows\t{}", report.option_rows);
    println!("backfill_attempts\t{}", report.backfill_attempts);
    println!("research_runs\t{}", report.research_runs);
    println!("profile_results\t{}", report.profile_results);
    println!("trade_summaries\t{}", report.trade_summaries);
    println!("live_market_snapshots\t{}", report.live_market_snapshots);
    println!("live_signal_candidates\t{}", report.live_signal_candidates);
    println!("live_provider_health\t{}", report.live_provider_health);
    if !report.date_ranges.is_empty() {
        println!("\nDate ranges");
        println!("symbol\trows\tfirst_date\tlast_date");
        for row in &report.date_ranges {
            println!(
                "{}\t{}\t{}\t{}",
                row.symbol,
                row.rows,
                row.first_date.as_deref().unwrap_or("-"),
                row.last_date.as_deref().unwrap_or("-")
            );
        }
    }
    if !report.failed_cache_windows.is_empty() {
        println!("\nFailed cache windows");
        println!("symbol\tright\tdataset\texpiration\tfrom\tto\tstatus\terror");
        for row in &report.failed_cache_windows {
            println!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                row.symbol,
                row.right,
                row.dataset,
                row.expiration,
                row.start_date,
                row.end_date,
                row.status,
                row.error.as_deref().unwrap_or("-")
            );
        }
    }
    if !report.latest_runs.is_empty() {
        println!("\nLatest runs");
        println!("run_id\tfamily\tsymbols\tfrom\tto\tartifact");
        for row in &report.latest_runs {
            println!(
                "{}\t{}\t{}\t{}\t{}\t{}",
                row.run_id,
                row.command_family,
                row.symbols_json,
                row.from_date,
                row.to_date,
                row.artifact_path
            );
        }
    }
}

fn print_research_store_import_report(report: &ResearchStoreImportReport) {
    println!("raw_root\t{}", report.raw_root);
    println!("symbols\t{}", report.symbols.join(","));
    println!("files_seen\t{}", report.files_seen);
    println!("cache_windows_recorded\t{}", report.cache_windows_recorded);
    println!("files_imported\t{}", report.files_imported);
    println!("files_failed\t{}", report.files_failed);
    println!("option_rows_imported\t{}", report.option_rows_imported);
}

fn print_research_store_perf_report(report: &ResearchStorePerfReport) {
    println!("raw_root\t{}", report.raw_root);
    println!("symbols\t{}", report.symbols.join(","));
    println!("files_scanned\t{}", report.files_scanned);
    println!("sync_ms\t{}", report.sync_ms);
    println!("count_query_ms\t{}", report.count_query_ms);
    println!("cache_windows\t{}", report.cache_windows);
    println!("option_rows\t{}", report.option_rows);
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
        anyhow::bail!("no live_signal spreads passed filters");
    }
    let exit_quotes = fixture_exit_quotes(config.fixture_exit.as_deref())?;
    let trades = candidates
        .iter()
        .filter_map(|live_signal| {
            choose_exit(
                live_signal,
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
                .filter_map(|live_signal| {
                    choose_exit(
                        live_signal,
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

fn monitor_live(symbol: &str, strategy: StrategyArg) -> Result<()> {
    let broker = RobinhoodBrokerAdapter::default();
    match strategy {
        StrategyArg::PutSpread => {
            if let Err(err) = broker.assert_credit_spread_live_supported() {
                println!("{symbol} put-spread monitor-live is data-only for now: {err}");
                println!("No orders placed.");
                return Ok(());
            }
        }
    }
    println!("{symbol} monitor-live adapter is not connected yet. No orders placed.");
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
            rationale: "High-quality semiconductor beta live_signal, but higher share price can make fixed-width put-spread selection less ergonomic.",
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
    let sequence = MAIN_RUN_ID_SEQUENCE.fetch_add(1, AtomicOrdering::Relaxed);
    let run_id = format!(
        "{}-{}-p{}-s{}",
        prefix,
        Utc::now().format("%Y%m%dT%H%M%S%.9fZ"),
        std::process::id(),
        sequence
    );
    Ok(PathBuf::from("runs").join(run_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use spreadfoundry::live_signal::{ProductionApproval, ProductionApprovalStatus};

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
    fn next_run_dir_is_unique_under_tight_loop() {
        let dirs = (0..100)
            .map(|_| next_run_dir("universe-research").unwrap())
            .collect::<BTreeSet<_>>();

        assert_eq!(dirs.len(), 100);
        assert!(dirs.iter().all(|dir| {
            dir.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("universe-research-"))
        }));
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
            "--research-store",
            "var/research/mechanism.duckdb",
            "--skip-cache-sync",
        ])
        .unwrap();

        match cli.command {
            Commands::ResearchWeeklyUniverse {
                profile_family,
                research_store,
                skip_cache_sync,
                ..
            } => {
                assert_eq!(profile_family, ProfileFamilyArg::WeeklyCallCredit);
                assert_eq!(
                    research_store,
                    Some(PathBuf::from("var/research/mechanism.duckdb"))
                );
                assert!(skip_cache_sync);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn research_store_import_accepts_symbols_and_json() {
        let cli = Cli::try_parse_from([
            "spreadfoundry",
            "research-store-import",
            "--raw-root",
            "tmp/raw",
            "--symbols",
            "NVDA,TSLA",
            "--max-files-per-symbol",
            "10",
            "--json",
        ])
        .unwrap();

        match cli.command {
            Commands::ResearchStoreImport {
                raw_root,
                symbols,
                max_files_per_symbol,
                json,
            } => {
                assert_eq!(raw_root, PathBuf::from("tmp/raw"));
                assert_eq!(symbols, vec!["NVDA".to_owned(), "TSLA".to_owned()]);
                assert_eq!(max_files_per_symbol, Some(10));
                assert!(json);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn research_store_health_accepts_json() {
        let cli =
            Cli::try_parse_from(["spreadfoundry", "research-store-health", "--json"]).unwrap();

        match cli.command {
            Commands::ResearchStoreHealth { json } => assert!(json),
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
                ..
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
            "--recent-first",
            "--option-side",
            "call",
            "--fetch-concurrency",
            "2",
            "--window-timeout-seconds",
            "45",
            "--progress",
            "--research-store",
            "var/research/offline.duckdb",
            "--skip-cache-sync",
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
                recent_first,
                option_side,
                fetch_concurrency,
                window_timeout_seconds,
                force_refresh,
                progress,
                research_store,
                skip_cache_sync,
                json,
            } => {
                assert_eq!(symbols, vec!["SOFI", "HOOD"]);
                assert_eq!(from.to_string(), "2024-01-01");
                assert_eq!(to.to_string(), "2026-06-28");
                assert_eq!(max_expirations, Some(80));
                assert_eq!(max_windows_per_symbol, 6);
                assert!(recent_first);
                assert_eq!(option_side, WarmOptionSideArg::Call);
                assert_eq!(fetch_concurrency, 2);
                assert_eq!(window_timeout_seconds, 45);
                assert!(!force_refresh);
                assert!(progress);
                assert_eq!(
                    research_store,
                    Some(PathBuf::from("var/research/offline.duckdb"))
                );
                assert!(skip_cache_sync);
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
    fn export_live_signal_accepts_run_strategy_and_output() {
        let cli = Cli::try_parse_from([
            "spreadfoundry",
            "export-live-signal",
            "--run",
            "runs/example",
            "--approved-strategy",
            "configs/test-approved.json",
            "--output",
            "var/test-live-signal.json",
        ])
        .unwrap();

        match cli.command {
            Commands::ExportLiveSignal {
                run,
                approved_strategy,
                output,
                as_of,
            } => {
                assert_eq!(run, PathBuf::from("runs/example"));
                assert_eq!(
                    approved_strategy,
                    PathBuf::from("configs/test-approved.json")
                );
                assert_eq!(output, PathBuf::from("var/test-live-signal.json"));
                assert_eq!(as_of, None);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn live_signal_status_accepts_require_action() {
        let cli = Cli::try_parse_from([
            "spreadfoundry",
            "live-signal-status",
            "--live-signal",
            "candidates/example.json",
            "--require-signal",
        ])
        .unwrap();

        match cli.command {
            Commands::LiveSignalStatus {
                live_signal,
                as_of,
                require_signal,
            } => {
                assert_eq!(live_signal, PathBuf::from("candidates/example.json"));
                assert_eq!(as_of, None);
                assert!(require_signal);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn live_market_engine_accepts_production_loop_flags() {
        let cli = Cli::try_parse_from([
            "spreadfoundry",
            "live-market-engine",
            "--approved-strategy",
            "configs/test-approved.json",
            "--source-live-signal",
            "var/source-live-signal.json",
            "--output",
            "var/live-signal.json",
            "--state-file",
            "var/live-market-health.json",
            "--store",
            "tmp/spreadfoundry.duckdb",
            "--as-of",
            "2026-06-30",
            "--interval-seconds",
            "30",
            "--max-source-age-seconds",
            "45",
            "--market-window-only",
            "false",
            "--once",
            "--json",
        ])
        .unwrap();

        match cli.command {
            Commands::LiveMarketEngine {
                approved_strategy,
                source_live_signal,
                output,
                state_file,
                store,
                as_of,
                interval_seconds,
                max_source_age_seconds,
                market_window_only,
                once,
                json,
            } => {
                assert_eq!(
                    approved_strategy,
                    PathBuf::from("configs/test-approved.json")
                );
                assert_eq!(
                    source_live_signal,
                    PathBuf::from("var/source-live-signal.json")
                );
                assert_eq!(output, PathBuf::from("var/live-signal.json"));
                assert_eq!(state_file, PathBuf::from("var/live-market-health.json"));
                assert_eq!(store, PathBuf::from("tmp/spreadfoundry.duckdb"));
                assert_eq!(as_of, Some(NaiveDate::from_ymd_opt(2026, 6, 30).unwrap()));
                assert_eq!(interval_seconds, 30);
                assert_eq!(max_source_age_seconds, 45);
                assert!(!market_window_only);
                assert!(once);
                assert!(json);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn live_market_engine_defaults_to_split_source_and_output_artifacts() {
        let cli = Cli::try_parse_from(["spreadfoundry", "live-market-engine"]).unwrap();

        match cli.command {
            Commands::LiveMarketEngine {
                source_live_signal,
                output,
                ..
            } => {
                assert_eq!(
                    source_live_signal,
                    PathBuf::from("var/live_signal_refresh_source.json")
                );
                assert_eq!(output, PathBuf::from("var/live_signal.json"));
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn run_execution_decision_accepts_tiny_budget_flags() {
        let cli = Cli::try_parse_from([
            "spreadfoundry",
            "run-execution-decision",
            "--live-signal",
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
            "--mode",
            "live",
            "--broker-multi-leg-options",
            "--broker-cash-secured-puts",
            "--broker-covered-calls",
            "--json",
        ])
        .unwrap();

        match cli.command {
            Commands::RunExecutionDecision {
                live_signal,
                as_of,
                max_loss,
                account_cash,
                debit_max_loss,
                wheel_reserve_cap,
                free_cash_buffer,
                max_wheel_positions_per_symbol,
                mode,
                broker,
                broker_multi_leg_options,
                broker_cash_secured_puts,
                broker_covered_calls,
                robinhood_mcp_command,
                order_ledger,
                max_order_age_seconds,
                json,
            } => {
                assert_eq!(live_signal, PathBuf::from("candidates/example.json"));
                assert_eq!(as_of, Some(NaiveDate::from_ymd_opt(2026, 6, 28).unwrap()));
                assert_eq!(max_loss, Some(500.0));
                assert_eq!(account_cash, 45_000.0);
                assert_eq!(debit_max_loss, 1_000.0);
                assert_eq!(wheel_reserve_cap, 35_000.0);
                assert_eq!(free_cash_buffer, 11_250.0);
                assert_eq!(max_wheel_positions_per_symbol, 1);
                assert_eq!(mode, ExecutionMode::Live);
                assert_eq!(broker, BrokerKind::Tradier);
                assert!(broker_multi_leg_options);
                assert!(broker_cash_secured_puts);
                assert!(broker_covered_calls);
                assert_eq!(robinhood_mcp_command, None);
                assert_eq!(
                    order_ledger,
                    PathBuf::from("var/execution_order_ledger.json")
                );
                assert_eq!(max_order_age_seconds, DEFAULT_MAX_ORDER_AGE_SECONDS);
                assert!(json);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn execution_readiness_accepts_service_flags() {
        let cli = Cli::try_parse_from([
            "spreadfoundry",
            "execution-readiness",
            "--live-signal",
            "candidates/example.json",
            "--as-of",
            "2026-06-28",
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
            "--robinhood-mcp-command",
            "codex mcp exec robinhood-trading",
            "--allow-blocked",
            "--json",
        ])
        .unwrap();

        match cli.command {
            Commands::ExecutionReadiness {
                live_signal,
                as_of,
                account_cash,
                debit_max_loss,
                wheel_reserve_cap,
                free_cash_buffer,
                max_wheel_positions_per_symbol,
                broker,
                broker_multi_leg_options,
                broker_cash_secured_puts,
                broker_covered_calls,
                robinhood_mcp_command,
                max_order_age_seconds,
                allow_blocked,
                json,
            } => {
                assert_eq!(live_signal, PathBuf::from("candidates/example.json"));
                assert_eq!(as_of, Some(NaiveDate::from_ymd_opt(2026, 6, 28).unwrap()));
                assert_eq!(account_cash, 45_000.0);
                assert_eq!(debit_max_loss, 1_000.0);
                assert_eq!(wheel_reserve_cap, 35_000.0);
                assert_eq!(free_cash_buffer, 11_250.0);
                assert_eq!(max_wheel_positions_per_symbol, 1);
                assert_eq!(broker, BrokerKind::Tradier);
                assert!(broker_multi_leg_options);
                assert!(broker_cash_secured_puts);
                assert!(broker_covered_calls);
                assert_eq!(
                    robinhood_mcp_command.as_deref(),
                    Some("codex mcp exec robinhood-trading")
                );
                assert_eq!(max_order_age_seconds, DEFAULT_MAX_ORDER_AGE_SECONDS);
                assert!(allow_blocked);
                assert!(json);
            }
            other => panic!("unexpected command: {other:?}"),
        }
    }

    #[test]
    fn live_signal_status_require_action_fails_closed_without_action() {
        let path = unique_main_test_path("live-signal-no-action.json");
        let artifact = test_live_signal_artifact_as_of(
            Some(NaiveDate::from_ymd_opt(2026, 6, 26).unwrap()),
            serde_json::json!([{
                "status":"recent_closed",
                "symbol":"TSLA",
                "strategy":"put_debit_spread",
                "entry_date":"2026-06-25",
                "exit_date":"2026-06-26",
                "pnl":-50.0
            }]),
        );
        fs::write(&path, serde_json::to_string(&artifact).unwrap()).unwrap();

        let err = live_signal_status(
            &path,
            Some(NaiveDate::from_ymd_opt(2026, 6, 26).unwrap()),
            true,
        )
        .unwrap_err();

        assert!(err.to_string().contains("no selected live"));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn live_signal_status_require_action_accepts_new_entry() {
        let path = unique_main_test_path("live-signal-entry-action.json");
        let artifact = test_live_signal_artifact_as_of(
            Some(NaiveDate::from_ymd_opt(2026, 6, 26).unwrap()),
            serde_json::json!([{
                "status":"new_entry",
                "symbol":"TSLA",
                "strategy":"put_debit_spread",
                "entry_date":"2026-06-26",
                "exit_date":"2026-06-26",
                "expiration":"2026-07-02",
                "short_strike":350.0,
                "long_strike":355.0,
                "entry_credit":-1.0,
                "max_loss":100.0
            }]),
        );
        fs::write(&path, serde_json::to_string(&artifact).unwrap()).unwrap();

        live_signal_status(
            &path,
            Some(NaiveDate::from_ymd_opt(2026, 6, 26).unwrap()),
            true,
        )
        .unwrap();

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn live_signal_status_require_action_rejects_stale_new_entry() {
        let path = unique_main_test_path("live-signal-stale-entry-action.json");
        let artifact = test_live_signal_artifact_as_of(
            Some(NaiveDate::from_ymd_opt(2026, 6, 26).unwrap()),
            serde_json::json!([{
                "status":"new_entry",
                "symbol":"CRWV",
                "strategy":"wheel",
                "entry_date":"2026-06-26",
                "exit_date":"2026-06-26",
                "short_strike":80.0,
                "entry_credit":1.12,
                "max_loss":7888.0
            }]),
        );
        fs::write(&path, serde_json::to_string(&artifact).unwrap()).unwrap();

        let err = live_signal_status(
            &path,
            Some(NaiveDate::from_ymd_opt(2026, 6, 28).unwrap()),
            true,
        )
        .unwrap_err();

        assert!(err.to_string().contains("no selected live"));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn live_signal_status_require_action_accepts_already_open_management() {
        let path = unique_main_test_path("live-signal-open-action.json");
        let artifact = test_live_signal_artifact_as_of(
            Some(NaiveDate::from_ymd_opt(2026, 6, 28).unwrap()),
            serde_json::json!([{
                "status":"already_open",
                "symbol":"TSLA",
                "strategy":"put_debit_spread",
                "entry_date":"2026-06-26",
                "exit_date":"2026-06-30",
                "expiration":"2026-07-02",
                "short_strike":350.0,
                "long_strike":355.0,
                "entry_credit":-1.0,
                "max_loss":100.0,
                "pnl":0.0
            }]),
        );
        fs::write(&path, serde_json::to_string(&artifact).unwrap()).unwrap();

        live_signal_status(
            &path,
            Some(NaiveDate::from_ymd_opt(2026, 6, 28).unwrap()),
            true,
        )
        .unwrap();

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn live_signal_contract_rejects_unapproved_selected_symbol() {
        let mut artifact = test_canary_artifact(serde_json::json!([{
            "status":"new_entry",
            "symbol":"SOFI",
            "strategy":"call_debit_spread",
            "entry_date":"2026-06-28",
            "exit_date":"2026-06-28",
            "expiration":"2026-07-02",
            "short_strike":12.0,
            "long_strike":10.0,
            "entry_credit":-0.50,
            "max_loss":50.0
        }]));
        artifact.approved_strategy.symbols = vec!["TSLA".to_owned()];

        let err = artifact.validate_contract().unwrap_err();

        assert!(
            err.to_string()
                .contains("not in the approved strategy symbol list")
        );
    }

    #[test]
    fn live_signal_contract_rejects_selected_signal_outside_signal_list() {
        let mut artifact = test_canary_artifact(serde_json::json!([{
            "status":"recent_closed",
            "symbol":"TSLA",
            "strategy":"call_debit_spread",
            "entry_date":"2026-06-27",
            "exit_date":"2026-06-28",
            "expiration":"2026-07-02",
            "short_strike":350.0,
            "long_strike":345.0,
            "entry_credit":-1.0,
            "max_loss":100.0
        }]));
        artifact.selected_signal = Some(test_trade_signal_summary(
            &serde_json::json!({
                "status":"new_entry",
                "symbol":"TSLA",
                "strategy":"call_debit_spread",
                "entry_date":"2026-06-28",
                "exit_date":"2026-06-28",
                "expiration":"2026-07-02",
                "short_strike":350.0,
                "long_strike":345.0,
                "entry_credit":-1.0,
                "max_loss":100.0
            }),
            None,
        ));

        let err = artifact.validate_contract().unwrap_err();

        assert!(err.to_string().contains("not present in live signal list"));
    }

    #[test]
    fn live_signal_contract_rejects_selected_signal_without_production_approval() {
        let mut artifact = test_canary_artifact(serde_json::json!([{
            "status":"new_entry",
            "symbol":"TSLA",
            "strategy":"put_debit_spread",
            "entry_date":"2026-06-28",
            "exit_date":"2026-06-28",
            "expiration":"2026-07-02",
            "short_strike":350.0,
            "long_strike":355.0,
            "entry_credit":-1.0,
            "max_loss":100.0
        }]));
        artifact.approved_strategy.production_approval = None;

        let err = artifact.validate_contract().unwrap_err();

        assert!(
            err.to_string()
                .contains("requires explicit production approval")
        );
    }

    #[test]
    fn live_signal_contract_enforces_production_approval_max_loss() {
        let mut artifact = test_canary_artifact(serde_json::json!([{
            "status":"new_entry",
            "symbol":"TSLA",
            "strategy":"put_debit_spread",
            "entry_date":"2026-06-28",
            "exit_date":"2026-06-28",
            "expiration":"2026-07-02",
            "short_strike":350.0,
            "long_strike":355.0,
            "entry_credit":-2.0,
            "max_loss":200.0
        }]));
        artifact
            .approved_strategy
            .production_approval
            .as_mut()
            .unwrap()
            .max_order_max_loss = Some(100.0);

        let err = artifact.validate_contract().unwrap_err();

        assert!(
            err.to_string()
                .contains("exceeds production approval max_order_max_loss")
        );
    }

    #[test]
    fn portfolio_canary_runner_returns_monitor_when_stale() {
        let artifact = test_live_signal_artifact_as_of(
            Some(NaiveDate::from_ymd_opt(2026, 6, 28).unwrap()),
            serde_json::json!([{
                "status":"recent_closed",
                "symbol":"CRWV",
                "strategy":"wheel",
                "entry_date":"2026-06-26",
                "exit_date":"2026-06-26",
                "max_loss":7888.0
            }]),
        );
        let broker = RobinhoodBrokerAdapter::default();

        let decision = compute_execution_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &test_canary_risk(),
            &broker,
            ExecutionMode::Monitor,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "no_signal");
        assert!(decision.selected_signal.is_none());
    }

    #[test]
    fn portfolio_canary_runner_blocks_wheel_above_tiny_budget() {
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"new_entry",
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

        let decision = compute_execution_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &CanaryRiskPolicy {
                account_cash: 45_000.0,
                debit_max_loss: 1_000.0,
                wheel_reserve_cap: 5_000.0,
                free_cash_buffer: 11_250.0,
                max_wheel_positions_per_symbol: 1,
            },
            &broker,
            ExecutionMode::Monitor,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "blocked");
        assert_eq!(
            decision
                .selected_signal
                .as_ref()
                .map(|action| action.strategy.as_str()),
            Some("wheel")
        );
    }

    #[test]
    fn execution_decision_blocks_when_approved_open_position_cap_is_full() {
        let mut artifact = test_canary_artifact(serde_json::json!([
            {
                "status":"new_entry",
                "symbol":"TSLA",
                "strategy":"call_debit_spread",
                "entry_date":"2026-06-28",
                "exit_date":"2026-06-28",
                "expiration":"2026-07-02",
                "short_strike":350.0,
                "long_strike":345.0,
                "entry_credit":-1.0,
                "max_loss":100.0
            },
            {
                "status":"already_open",
                "symbol":"ORCL",
                "strategy":"call_debit_spread",
                "entry_date":"2026-06-27",
                "exit_date":"2026-06-30",
                "expiration":"2026-07-02",
                "short_strike":225.0,
                "long_strike":220.0,
                "entry_credit":-1.0,
                "max_loss":100.0
            }
        ]));
        artifact
            .approved_strategy
            .portfolio_constraints
            .max_open_positions = 1;
        let broker = ExecutionBrokerAdapter {
            kind: BrokerKind::Tradier,
            capabilities: BrokerCapabilities {
                single_leg_options: true,
                multi_leg_options: true,
                stock_option_combos: false,
                cash_secured_puts: true,
                covered_calls: false,
            },
            live_orders_enabled: false,
        };

        let decision = compute_execution_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &test_canary_risk(),
            &broker,
            ExecutionMode::Monitor,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "blocked");
        assert!(decision.reason.contains("max is 1"));
    }

    #[test]
    fn execution_decision_blocks_when_approved_symbol_allocation_is_exceeded() {
        let mut artifact = test_canary_artifact(serde_json::json!([{
            "status":"new_entry",
            "symbol":"TSLA",
            "strategy":"call_debit_spread",
            "entry_date":"2026-06-28",
            "exit_date":"2026-06-28",
            "expiration":"2026-07-02",
            "short_strike":350.0,
            "long_strike":345.0,
            "entry_credit":-10.0,
            "max_loss":1000.0
        }]));
        artifact
            .approved_strategy
            .portfolio_constraints
            .capital_budget = 2_000.0;
        artifact
            .approved_strategy
            .portfolio_constraints
            .max_symbol_allocation_pct = 0.25;
        let broker = ExecutionBrokerAdapter {
            kind: BrokerKind::Tradier,
            capabilities: BrokerCapabilities {
                single_leg_options: true,
                multi_leg_options: true,
                stock_option_combos: false,
                cash_secured_puts: true,
                covered_calls: false,
            },
            live_orders_enabled: false,
        };

        let decision = compute_execution_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &test_canary_risk(),
            &broker,
            ExecutionMode::Monitor,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "blocked");
        assert!(decision.reason.contains("would exceed symbol cap"));
    }

    #[test]
    fn portfolio_canary_runner_sends_tiny_debit_to_broker_gate() {
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"new_entry",
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
        let broker = RobinhoodBrokerAdapter::default();

        let decision = compute_execution_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &test_canary_risk(),
            &broker,
            ExecutionMode::Monitor,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "blocked");
        assert_eq!(
            decision
                .selected_signal
                .as_ref()
                .map(|action| action.strategy.as_str()),
            Some("put_debit_spread")
        );
    }

    #[test]
    fn portfolio_canary_runner_blocks_debit_when_order_debit_exceeds_exported_max_loss() {
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"new_entry",
            "symbol":"TSLA",
            "strategy":"put_debit_spread",
            "entry_date":"2026-06-28",
            "exit_date":"2026-06-28",
            "expiration":"2026-07-02",
            "short_strike":350.0,
            "long_strike":355.0,
            "entry_credit":-3.35,
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
            live_orders_enabled: false,
        };

        let decision = compute_execution_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &test_canary_risk(),
            &broker,
            ExecutionMode::Monitor,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "blocked");
        assert!(decision.reason.contains("does not match order debit risk"));
    }

    #[test]
    fn portfolio_canary_runner_requires_review_before_live_request() {
        let today = execution_default_as_of(Utc::now());
        let today_s = today.to_string();
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"new_entry",
            "symbol":"TSLA",
            "strategy":"put_debit_spread",
            "entry_date":today_s,
            "exit_date":today_s,
            "expiration":"2026-07-02",
            "short_strike":350.0,
            "long_strike":355.0,
            "entry_credit":-1.00,
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

        let decision = compute_execution_decision_at(
            &artifact,
            today,
            &test_canary_risk(),
            &broker,
            ExecutionMode::Live,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
            test_market_open_utc(today),
        );

        assert_eq!(decision.status, "ready");
        assert!(!decision.broker_review_ok);
    }

    #[test]
    fn portfolio_canary_runner_blocks_live_order_for_historical_as_of() {
        let market_date = NaiveDate::from_ymd_opt(2026, 6, 29).unwrap();
        let historical = NaiveDate::from_ymd_opt(2026, 6, 26).unwrap();
        let historical_s = historical.to_string();
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"new_entry",
            "symbol":"TSLA",
            "strategy":"put_debit_spread",
            "entry_date":historical_s,
            "exit_date":historical_s,
            "expiration":"2026-07-02",
            "short_strike":350.0,
            "long_strike":355.0,
            "entry_credit":-1.00,
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

        let decision = compute_execution_decision_at(
            &artifact,
            historical,
            &test_canary_risk(),
            &broker,
            ExecutionMode::Live,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
            test_market_open_utc(market_date),
        );

        assert_eq!(decision.status, "blocked");
        assert!(decision.reason.contains("U.S. options-market date"));
    }

    #[test]
    fn execution_decision_blocks_invalid_live_signal_contract() {
        let mut artifact = test_canary_artifact(serde_json::json!([{
                "status":"new_entry",
                "symbol":"TSLA",
                "strategy":"put_debit_spread",
                "entry_date":"2026-06-28",
                "exit_date":"2026-06-28",
                "expiration":"2026-07-02",
                "short_strike":350.0,
                "long_strike":355.0,
                "entry_credit":-1.00,
                "max_loss":100.0
        }]));
        artifact.schema_version = 999;
        let broker = RobinhoodBrokerAdapter::default();

        let decision = compute_execution_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &test_canary_risk(),
            &broker,
            ExecutionMode::Monitor,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "blocked");
        assert!(decision.selected_signal.is_none());
    }

    #[test]
    fn execution_decision_allows_valid_live_signal_after_static_gate() {
        let today = execution_default_as_of(Utc::now());
        let artifact = test_canary_artifact(serde_json::json!([{
                "status":"new_entry",
                "symbol":"TSLA",
                "strategy":"put_debit_spread",
                "entry_date":today.to_string(),
                "exit_date":today.to_string(),
                "expiration":"2026-07-02",
                "short_strike":350.0,
                "long_strike":355.0,
                "entry_credit":-1.00,
                "max_loss":100.0
        }]));
        let broker = execution_broker(BrokerKind::Tradier, false, false, false, true);

        let decision = compute_execution_decision_at(
            &artifact,
            today,
            &test_canary_risk(),
            &broker,
            ExecutionMode::Live,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
            test_market_open_utc(today),
        );

        assert_eq!(decision.status, "ready");
        assert_eq!(
            decision
                .selected_signal
                .as_ref()
                .map(|action| action.strategy.as_str()),
            Some("put_debit_spread")
        );
    }

    #[test]
    fn execution_decision_blocks_unapproved_signal_strategy() {
        let today = execution_default_as_of(Utc::now());
        let mut artifact = test_canary_artifact(serde_json::json!([{
                "status":"new_entry",
                "symbol":"TSLA",
                "strategy":"put_debit_spread",
                "entry_date":today.to_string(),
                "exit_date":today.to_string(),
                "expiration":"2026-07-02",
                "short_strike":350.0,
                "long_strike":355.0,
                "entry_credit":-1.00,
                "max_loss":100.0
        }]));
        artifact.approved_strategy.allowed_live_strategies = vec!["call_debit_spread".to_owned()];
        let broker = execution_broker(BrokerKind::Tradier, false, false, false, true);

        let decision = compute_execution_decision(
            &artifact,
            today,
            &test_canary_risk(),
            &broker,
            ExecutionMode::Live,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "blocked");
        assert!(
            decision
                .reason
                .contains("is not approved for live execution")
        );
        assert!(decision.selected_signal.is_none());
    }

    #[test]
    fn execution_decision_reports_no_signal_without_selected_entry() {
        let today = execution_default_as_of(Utc::now());
        let artifact = test_live_signal_artifact_as_of(
            Some(today),
            serde_json::json!([{
                "status":"recent_closed",
                "symbol":"TSLA",
                "strategy":"put_debit_spread",
                "entry_date":"2026-06-25",
                "exit_date":"2026-06-26",
                "expiration":"2026-07-02",
                "short_strike":365.0,
                "long_strike":367.5,
                "entry_credit":-1.00,
                "max_loss":100.0
            }]),
        );
        let broker = execution_broker(BrokerKind::Tradier, false, false, false, true);

        let decision = compute_execution_decision(
            &artifact,
            today,
            &test_canary_risk(),
            &broker,
            ExecutionMode::Live,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "no_signal");
        assert!(decision.selected_signal.is_none());
    }

    #[test]
    fn portfolio_canary_runner_blocks_live_order_when_artifact_is_too_old() {
        let today = execution_default_as_of(Utc::now());
        let today_s = today.to_string();
        let mut artifact = test_canary_artifact(serde_json::json!([{
            "status":"new_entry",
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
        artifact.generated_at = NaiveDate::from_ymd_opt(2026, 6, 28)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc();
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

        let decision = compute_execution_decision_at(
            &artifact,
            today,
            &test_canary_risk(),
            &broker,
            ExecutionMode::Live,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
            test_market_open_utc(today),
        );

        assert_eq!(decision.status, "blocked");
        assert!(decision.reason.contains("live signal age"));
    }

    #[test]
    fn run_execution_decision_rejects_legacy_manual_review_flag() {
        let err = Cli::try_parse_from([
            "spreadfoundry",
            "run-execution-decision",
            "--broker-review-ok",
        ])
        .unwrap_err();

        assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
    }

    #[test]
    fn approved_strategy_research_from_overrides_refresh_fallback() {
        let mut artifact = test_canary_artifact(serde_json::json!([]));
        let configured_from = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
        let fallback = NaiveDate::from_ymd_opt(2016, 1, 1).unwrap();
        artifact.approved_strategy.research_from = Some(configured_from);

        assert_eq!(
            approved_strategy_research_from(&artifact.approved_strategy, fallback),
            configured_from
        );
    }

    #[test]
    fn approved_strategy_detector_window_overrides_refresh_research_span() {
        let mut artifact = test_canary_artifact(serde_json::json!([]));
        let fallback = NaiveDate::from_ymd_opt(2016, 1, 1).unwrap();
        let to = NaiveDate::from_ymd_opt(2026, 6, 30).unwrap();
        artifact.approved_strategy.research_from =
            Some(NaiveDate::from_ymd_opt(2023, 1, 1).unwrap());
        artifact.approved_strategy.live_detector_lookback_days = Some(90);

        let (from, require_gate) =
            approved_strategy_refresh_from_and_gate(&artifact.approved_strategy, fallback, to);

        assert_eq!(from, NaiveDate::from_ymd_opt(2026, 4, 1).unwrap());
        assert!(!require_gate);
    }

    #[test]
    fn portfolio_canary_runner_selects_already_open_management() {
        let artifact = test_live_signal_artifact_as_of(
            Some(NaiveDate::from_ymd_opt(2026, 6, 28).unwrap()),
            serde_json::json!([{
                "status":"already_open",
                "symbol":"ORCL",
                "strategy":"call_debit_spread",
                "entry_date":"2026-06-26",
                "exit_date":"2026-06-30",
                "expiration":"2026-07-02",
                "short_strike":225.0,
                "long_strike":220.0,
                "entry_credit":-4.50,
                "max_loss":450.0
            }]),
        );
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

        let decision = compute_execution_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &test_canary_risk(),
            &broker,
            ExecutionMode::Monitor,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "ready");
        assert_eq!(decision.action_kind, Some(ExecutionActionKind::ManageOpen));
        assert_eq!(decision.management_signals.len(), 1);
        let selected = decision.selected_signal.as_ref().unwrap();
        assert_eq!(selected.status, SignalStatus::AlreadyOpen);
        assert_eq!(selected.symbol, "ORCL");
    }

    #[test]
    fn portfolio_canary_runner_keeps_wheel_management_after_put_assignment() {
        let artifact = test_live_signal_artifact_as_of(
            Some(NaiveDate::from_ymd_opt(2026, 6, 30).unwrap()),
            serde_json::json!([{
                "status":"already_open",
                "symbol":"CRWV",
                "strategy":"wheel",
                "entry_date":"2026-06-26",
                "exit_date":"2026-07-06",
                "expiration":"2026-06-27",
                "short_strike":80.0,
                "short_put":80.0,
                "long_strike":85.0,
                "entry_credit":1.12,
                "max_loss":7888.0
            }]),
        );
        let broker = execution_broker(BrokerKind::Tradier, false, false, false, false);

        let decision = compute_execution_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 30).unwrap(),
            &test_canary_risk(),
            &broker,
            ExecutionMode::Monitor,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "ready");
        assert_eq!(decision.action_kind, Some(ExecutionActionKind::ManageOpen));
        assert_eq!(decision.management_signals.len(), 1);
        let selected = decision.selected_signal.as_ref().unwrap();
        assert_eq!(selected.status, SignalStatus::AlreadyOpen);
        assert_eq!(selected.strategy, "wheel");
    }

    #[test]
    fn portfolio_canary_runner_reconciles_recent_closed_wheel_broker_residuals() {
        let artifact = test_live_signal_artifact_as_of(
            Some(NaiveDate::from_ymd_opt(2026, 6, 30).unwrap()),
            serde_json::json!([{
                "status":"recent_closed",
                "symbol":"CRWV",
                "strategy":"wheel",
                "entry_date":"2026-06-26",
                "exit_date":"2026-06-30",
                "expiration":"2026-06-27",
                "short_strike":80.0,
                "short_put":80.0,
                "entry_credit":1.12,
                "max_loss":7888.0,
                "exit_reason":"called_away"
            }]),
        );
        let broker = execution_broker(BrokerKind::Tradier, false, false, false, false);

        let decision = compute_execution_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 30).unwrap(),
            &test_canary_risk(),
            &broker,
            ExecutionMode::Monitor,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "ready");
        assert_eq!(decision.action_kind, Some(ExecutionActionKind::ManageOpen));
        assert_eq!(decision.management_signals.len(), 1);
        let selected = decision.selected_signal.as_ref().unwrap();
        assert_eq!(selected.status, SignalStatus::AlreadyOpen);
        assert_eq!(selected.exit_reason.as_deref(), Some("called_away"));
    }

    #[test]
    fn broker_bridge_blocks_management_for_non_tradier_broker() {
        let artifact = test_live_signal_artifact_as_of(
            Some(NaiveDate::from_ymd_opt(2026, 6, 28).unwrap()),
            serde_json::json!([{
                "status":"already_open",
                "symbol":"ORCL",
                "strategy":"call_debit_spread",
                "entry_date":"2026-06-26",
                "exit_date":"2026-06-30",
                "expiration":"2026-07-02",
                "short_strike":225.0,
                "long_strike":220.0,
                "entry_credit":-4.50,
                "max_loss":450.0
            }]),
        );
        let broker = execution_broker(BrokerKind::Robinhood, true, false, false, false);
        let mut decision = compute_execution_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &test_canary_risk(),
            &broker,
            ExecutionMode::Review,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "ready");
        assert_eq!(decision.action_kind, Some(ExecutionActionKind::ManageOpen));

        apply_broker_bridge(&mut decision, &broker, None, None).unwrap();

        assert_eq!(decision.status, "blocked");
        assert!(
            decision
                .reason
                .contains("only implemented for Tradier strategies")
        );
    }

    #[test]
    fn portfolio_canary_runner_blocks_management_without_execution_rules() {
        let mut artifact = test_live_signal_artifact_as_of(
            Some(NaiveDate::from_ymd_opt(2026, 6, 28).unwrap()),
            serde_json::json!([{
                "status":"already_open",
                "symbol":"ORCL",
                "strategy":"call_debit_spread",
                "entry_date":"2026-06-26",
                "exit_date":"2026-06-30",
                "expiration":"2026-07-02",
                "short_strike":225.0,
                "long_strike":220.0,
                "entry_credit":-4.50,
                "max_loss":450.0
            }]),
        );
        artifact.signals[0].execution_rules = None;
        let broker = execution_broker(BrokerKind::Tradier, false, false, false, false);

        let decision = compute_execution_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &test_canary_risk(),
            &broker,
            ExecutionMode::Monitor,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "blocked");
        assert!(
            decision
                .reason
                .contains("requires exported execution rules")
        );
    }

    #[test]
    fn portfolio_canary_runner_blocks_debit_entry_without_execution_rules() {
        let mut artifact = test_live_signal_artifact_as_of(
            Some(NaiveDate::from_ymd_opt(2026, 6, 28).unwrap()),
            serde_json::json!([{
                "status":"new_entry",
                "symbol":"ORCL",
                "strategy":"call_debit_spread",
                "entry_date":"2026-06-28",
                "exit_date":"2026-06-28",
                "expiration":"2026-07-02",
                "short_strike":225.0,
                "long_strike":220.0,
                "entry_credit":-4.50,
                "max_loss":450.0
            }]),
        );
        artifact.signals[0].execution_rules = None;
        artifact.selected_signal.as_mut().unwrap().execution_rules = None;
        let broker = execution_broker(BrokerKind::Tradier, false, false, false, false);

        let decision = compute_execution_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &test_canary_risk(),
            &broker,
            ExecutionMode::Monitor,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "blocked");
        assert!(
            decision
                .reason
                .contains("requires exported execution rules")
        );
    }

    #[test]
    fn portfolio_canary_runner_prefers_new_entry_over_already_open() {
        let artifact = test_canary_artifact(serde_json::json!([
            {
                "status":"already_open",
                "symbol":"ORCL",
                "strategy":"call_debit_spread",
                "entry_date":"2026-06-26",
                "exit_date":"2026-06-30",
                "expiration":"2026-07-02",
                "short_strike":225.0,
                "long_strike":220.0,
                "entry_credit":-4.50,
                "max_loss":450.0
            },
            {
                "status":"new_entry",
                "symbol":"TSLA",
                "strategy":"put_debit_spread",
                "entry_date":"2026-06-28",
                "exit_date":"2026-06-28",
                "expiration":"2026-07-02",
                "short_strike":350.0,
                "long_strike":355.0,
                "entry_credit":-1.00,
                "max_loss":100.0
            }
        ]));
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

        let decision = compute_execution_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &test_canary_risk(),
            &broker,
            ExecutionMode::Monitor,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "ready");
        let selected = decision.selected_signal.as_ref().unwrap();
        assert_eq!(selected.status, SignalStatus::NewEntry);
        assert_eq!(selected.symbol, "TSLA");
        assert_eq!(decision.management_signals.len(), 1);
    }

    #[test]
    fn portfolio_canary_runner_reports_blocked_entry_before_already_open() {
        let artifact = test_canary_artifact(serde_json::json!([
            {
                "status":"already_open",
                "symbol":"ORCL",
                "strategy":"call_debit_spread",
                "entry_date":"2026-06-26",
                "exit_date":"2026-06-30",
                "expiration":"2026-07-02",
                "short_strike":225.0,
                "long_strike":220.0,
                "entry_credit":-1.00,
                "max_loss":100.0
            },
            {
                "status":"new_entry",
                "symbol":"TSLA",
                "strategy":"put_debit_spread",
                "entry_date":"2026-06-28",
                "exit_date":"2026-06-28",
                "expiration":"2026-07-02",
                "short_strike":350.0,
                "long_strike":355.0,
                "entry_credit":-20.00,
                "max_loss":2000.0
            }
        ]));
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

        let decision = compute_execution_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &test_canary_risk(),
            &broker,
            ExecutionMode::Monitor,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "blocked");
        assert!(decision.reason.contains("put_debit_spread"));
        let selected = decision.selected_signal.as_ref().unwrap();
        assert_eq!(selected.status, SignalStatus::NewEntry);
        assert_eq!(selected.symbol, "TSLA");
    }

    #[test]
    fn portfolio_canary_runner_reaches_ready_in_monitor_mode() {
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"new_entry",
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

        let decision = compute_execution_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &test_canary_risk(),
            &broker,
            ExecutionMode::Monitor,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "ready");
    }

    #[test]
    fn robinhood_mcp_order_arguments_builds_wheel_review_payload() {
        let action = test_trade_signal_summary(
            &serde_json::json!({
                "status":"new_entry",
                "symbol":"CRWV",
                "strategy":"wheel",
                "entry_date":"2026-06-28",
                "exit_date":"2026-06-28",
                "expiration":"2026-07-10",
                "short_put":80.0,
                "short_strike":80.0,
                "entry_credit":1.12,
                "max_loss":7888.0,
                "pnl":112.0
            }),
            Some(TradeSignalRisk {
                reserve: 8000.0,
                reserve_basis: "max_loss_plus_entry_credit_x100".to_owned(),
            }),
        );

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
        let action = test_trade_signal_summary(
            &serde_json::json!({
                "status":"new_entry",
                "symbol":"ORCL",
                "strategy":"call_debit_spread",
                "entry_date":"2026-06-28",
                "exit_date":"2026-06-28",
                "expiration":"2026-07-02",
                "short_strike":225.0,
                "long_strike":220.0,
                "width":5.0,
                "entry_credit":-4.50,
                "max_loss":450.0
            }),
            Some(TradeSignalRisk {
                reserve: 450.0,
                reserve_basis: "max_loss".to_owned(),
            }),
        );

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
    fn robinhood_mcp_order_arguments_rejects_debit_above_spread_width() {
        let action = test_trade_signal_summary(
            &serde_json::json!({
                "status":"new_entry",
                "symbol":"ORCL",
                "strategy":"call_debit_spread",
                "entry_date":"2026-06-28",
                "exit_date":"2026-06-28",
                "expiration":"2026-07-02",
                "short_strike":225.0,
                "long_strike":220.0,
                "width":5.0,
                "entry_credit":-6.00,
                "max_loss":600.0
            }),
            Some(TradeSignalRisk {
                reserve: 600.0,
                reserve_basis: "max_loss_and_order_debit".to_owned(),
            }),
        );

        let err = robinhood_mcp_option_order_request("review_option_order", &action).unwrap_err();

        assert!(format!("{err:#}").contains("exceeds strike width"));
    }

    #[test]
    fn robinhood_mcp_order_arguments_rejects_already_open() {
        let action = test_trade_signal_summary(
            &serde_json::json!({
                "status":"already_open",
                "symbol":"TSLA",
                "strategy":"put_debit_spread",
                "entry_date":"2026-06-27",
                "exit_date":"2026-06-30",
                "expiration":"2026-07-02",
                "short_put":350.0,
                "short_strike":350.0,
                "long_strike":355.0,
                "width":5.0,
                "entry_credit":-3.35,
                "max_loss":335.0
            }),
            Some(TradeSignalRisk {
                reserve: 335.0,
                reserve_basis: "max_loss".to_owned(),
            }),
        );

        let err = robinhood_mcp_option_order_request("review_option_order", &action).unwrap_err();

        assert!(format!("{err:#}").contains("new_entry"));
    }

    #[test]
    fn robinhood_mcp_bridge_rejects_review_without_verified_preview_flag() {
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"new_entry",
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
        let mut decision = compute_execution_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &test_canary_risk(),
            &broker,
            ExecutionMode::Review,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );
        let action = decision.selected_signal.clone().unwrap();
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

        assert_eq!(decision.status, "rejected");
        assert!(!decision.broker_review_ok);
    }

    #[test]
    fn robinhood_mcp_bridge_review_success_unblocks_manual_approval() {
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"new_entry",
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
        let mut decision = compute_execution_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &test_canary_risk(),
            &broker,
            ExecutionMode::Review,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        let action = decision.selected_signal.clone().unwrap();
        let expected_key = robinhood_mcp_order_key(
            &robinhood_mcp_option_order_request("review_option_order", &action).unwrap(),
        );
        let response = serde_json::json!({
            "ok": true,
            "tool": "review_option_order",
            "raw": {"preview": "ok", "order_key": expected_key, "broker_preview_verified": true}
        })
        .to_string();
        apply_robinhood_mcp_bridge(
            &mut decision,
            Some(&format!("cat >/dev/null; printf '%s\\n' '{}'", response)),
            None,
        )
        .unwrap();

        assert_eq!(decision.status, "reviewed");
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
        let today = execution_default_as_of(Utc::now());
        let today_s = today.to_string();
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"new_entry",
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
        let mut decision = compute_execution_decision_at(
            &artifact,
            today,
            &test_canary_risk(),
            &broker,
            ExecutionMode::Live,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
            test_market_open_utc(today),
        );
        let action = decision.selected_signal.clone().unwrap();
        let expected_key = robinhood_mcp_order_key(
            &robinhood_mcp_option_order_request("review_option_order", &action).unwrap(),
        );
        let response = serde_json::json!({
            "ok": true,
            "tool": "review_option_order",
            "raw": {"order_key": expected_key, "broker_preview_verified": true}
        })
        .to_string();

        apply_robinhood_mcp_bridge(
            &mut decision,
            Some(&format!("cat >/dev/null; printf '%s\\n' '{}'", response)),
            None,
        )
        .unwrap();

        assert_eq!(decision.status, "blocked");
        assert!(decision.mcp_place.is_none());
        assert!(decision.reason.contains("wheel placement is blocked"));
    }

    #[test]
    fn robinhood_mcp_bridge_blocks_duplicate_live_submission() {
        let ledger = unique_main_test_path("canary-order-ledger-duplicate.json");
        let today = execution_default_as_of(Utc::now());
        let today_s = today.to_string();
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"new_entry",
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

        let mut first = compute_execution_decision_at(
            &artifact,
            today,
            &test_canary_risk(),
            &broker,
            ExecutionMode::Live,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
            test_market_open_utc(today),
        );
        let first_action = first.selected_signal.clone().unwrap();
        let expected_key = robinhood_mcp_order_key(
            &robinhood_mcp_option_order_request("review_option_order", &first_action).unwrap(),
        );
        let review_response = serde_json::json!({
            "ok": true,
            "tool": "review_option_order",
            "raw": {"order_key": expected_key, "broker_preview_verified": true}
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

        let mut second = compute_execution_decision_at(
            &artifact,
            today,
            &test_canary_risk(),
            &broker,
            ExecutionMode::Live,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
            test_market_open_utc(today),
        );
        apply_robinhood_mcp_bridge(&mut second, Some(command.as_str()), Some(&ledger)).unwrap();

        assert_eq!(first.status, "submitted");
        assert_eq!(second.status, "already_submitted");
        fs::remove_file(ledger).unwrap();
    }

    #[test]
    fn tradier_call_debit_payload_matches_multileg_shape() {
        let action = test_trade_signal_summary(
            &serde_json::json!({
                "status":"new_entry",
                "symbol":"ORCL",
                "strategy":"call_debit_spread",
                "entry_date":"2026-06-29",
                "exit_date":"2026-06-29",
                "expiration":"2026-07-02",
                "short_strike":225.0,
                "long_strike":220.0,
                "entry_credit":-4.50,
                "max_loss":450.0
            }),
            None,
        );

        let payload = tradier_multileg_debit_payload(&action).unwrap();

        assert_eq!(payload.get("class").map(String::as_str), Some("multileg"));
        assert_eq!(payload.get("symbol").map(String::as_str), Some("ORCL"));
        assert_eq!(payload.get("type").map(String::as_str), Some("debit"));
        assert_eq!(payload.get("duration").map(String::as_str), Some("day"));
        assert_eq!(payload.get("price").map(String::as_str), Some("4.50"));
        assert_eq!(
            payload.get("option_symbol[0]").map(String::as_str),
            Some("ORCL260702C00220000")
        );
        assert_eq!(
            payload.get("side[0]").map(String::as_str),
            Some("buy_to_open")
        );
        assert_eq!(payload.get("quantity[0]").map(String::as_str), Some("1"));
        assert_eq!(
            payload.get("option_symbol[1]").map(String::as_str),
            Some("ORCL260702C00225000")
        );
        assert_eq!(
            payload.get("side[1]").map(String::as_str),
            Some("sell_to_open")
        );
        assert_eq!(payload.get("quantity[1]").map(String::as_str), Some("1"));
    }

    #[test]
    fn tradier_put_debit_payload_matches_multileg_shape() {
        let action = test_trade_signal_summary(
            &serde_json::json!({
                "status":"new_entry",
                "symbol":"TSLA",
                "strategy":"put_debit_spread",
                "entry_date":"2026-06-29",
                "exit_date":"2026-06-29",
                "expiration":"2026-07-02",
                "short_strike":350.0,
                "long_strike":355.0,
                "entry_credit":-1.00,
                "max_loss":100.0
            }),
            None,
        );

        let payload = tradier_multileg_debit_payload(&action).unwrap();

        assert_eq!(payload.get("price").map(String::as_str), Some("1.00"));
        assert_eq!(
            payload.get("option_symbol[0]").map(String::as_str),
            Some("TSLA260702P00355000")
        );
        assert_eq!(
            payload.get("side[0]").map(String::as_str),
            Some("buy_to_open")
        );
        assert_eq!(
            payload.get("option_symbol[1]").map(String::as_str),
            Some("TSLA260702P00350000")
        );
        assert_eq!(
            payload.get("side[1]").map(String::as_str),
            Some("sell_to_open")
        );
    }

    #[test]
    fn tradier_call_debit_close_payload_matches_multileg_shape() {
        let action = test_trade_signal_summary(
            &serde_json::json!({
                "status":"already_open",
                "symbol":"ORCL",
                "strategy":"call_debit_spread",
                "entry_date":"2026-06-29",
                "exit_date":"2026-07-01",
                "expiration":"2026-07-02",
                "short_strike":225.0,
                "long_strike":220.0,
                "entry_credit":-4.50,
                "max_loss":450.0
            }),
            None,
        );

        let payload = tradier_multileg_debit_close_payload(&action, 3.25, 1).unwrap();

        assert_eq!(payload.get("class").map(String::as_str), Some("multileg"));
        assert_eq!(payload.get("symbol").map(String::as_str), Some("ORCL"));
        assert_eq!(payload.get("type").map(String::as_str), Some("credit"));
        assert_eq!(payload.get("duration").map(String::as_str), Some("day"));
        assert_eq!(payload.get("price").map(String::as_str), Some("3.25"));
        assert_eq!(
            payload.get("option_symbol[0]").map(String::as_str),
            Some("ORCL260702C00220000")
        );
        assert_eq!(
            payload.get("side[0]").map(String::as_str),
            Some("sell_to_close")
        );
        assert_eq!(
            payload.get("option_symbol[1]").map(String::as_str),
            Some("ORCL260702C00225000")
        );
        assert_eq!(
            payload.get("side[1]").map(String::as_str),
            Some("buy_to_close")
        );
    }

    #[test]
    fn tradier_put_credit_payload_matches_multileg_shape() {
        let action = test_trade_signal_summary(
            &serde_json::json!({
                "status":"new_entry",
                "symbol":"TSLA",
                "strategy":"put_credit_spread",
                "entry_date":"2026-06-29",
                "exit_date":"2026-06-29",
                "expiration":"2026-07-02",
                "short_strike":350.0,
                "long_strike":345.0,
                "width":5.0,
                "entry_credit":1.00,
                "max_loss":400.0
            }),
            None,
        );

        let payload = tradier_multileg_credit_payload(&action).unwrap();

        assert_eq!(payload.get("class").map(String::as_str), Some("multileg"));
        assert_eq!(payload.get("symbol").map(String::as_str), Some("TSLA"));
        assert_eq!(payload.get("type").map(String::as_str), Some("credit"));
        assert_eq!(payload.get("duration").map(String::as_str), Some("day"));
        assert_eq!(payload.get("price").map(String::as_str), Some("1.00"));
        assert_eq!(
            payload.get("option_symbol[0]").map(String::as_str),
            Some("TSLA260702P00350000")
        );
        assert_eq!(
            payload.get("side[0]").map(String::as_str),
            Some("sell_to_open")
        );
        assert_eq!(
            payload.get("option_symbol[1]").map(String::as_str),
            Some("TSLA260702P00345000")
        );
        assert_eq!(
            payload.get("side[1]").map(String::as_str),
            Some("buy_to_open")
        );
    }

    #[test]
    fn tradier_call_credit_close_payload_matches_multileg_shape() {
        let action = tradier_call_credit_action("already_open");

        let payload = tradier_multileg_credit_close_payload(&action, 0.25, 1).unwrap();

        assert_eq!(payload.get("class").map(String::as_str), Some("multileg"));
        assert_eq!(payload.get("symbol").map(String::as_str), Some("ORCL"));
        assert_eq!(payload.get("type").map(String::as_str), Some("debit"));
        assert_eq!(payload.get("duration").map(String::as_str), Some("day"));
        assert_eq!(payload.get("price").map(String::as_str), Some("0.25"));
        assert_eq!(
            payload.get("option_symbol[0]").map(String::as_str),
            Some("ORCL260702C00225000")
        );
        assert_eq!(
            payload.get("side[0]").map(String::as_str),
            Some("buy_to_close")
        );
        assert_eq!(
            payload.get("option_symbol[1]").map(String::as_str),
            Some("ORCL260702C00230000")
        );
        assert_eq!(
            payload.get("side[1]").map(String::as_str),
            Some("sell_to_close")
        );
    }

    #[test]
    fn tradier_put_credit_close_payload_matches_multileg_shape() {
        let action = tradier_put_credit_action("already_open");

        let payload = tradier_multileg_credit_close_payload(&action, 0.25, 1).unwrap();

        assert_eq!(payload.get("class").map(String::as_str), Some("multileg"));
        assert_eq!(payload.get("symbol").map(String::as_str), Some("TSLA"));
        assert_eq!(payload.get("type").map(String::as_str), Some("debit"));
        assert_eq!(payload.get("duration").map(String::as_str), Some("day"));
        assert_eq!(payload.get("price").map(String::as_str), Some("0.25"));
        assert_eq!(
            payload.get("option_symbol[0]").map(String::as_str),
            Some("TSLA260702P00350000")
        );
        assert_eq!(
            payload.get("side[0]").map(String::as_str),
            Some("buy_to_close")
        );
        assert_eq!(
            payload.get("option_symbol[1]").map(String::as_str),
            Some("TSLA260702P00345000")
        );
        assert_eq!(
            payload.get("side[1]").map(String::as_str),
            Some("sell_to_close")
        );
    }

    #[test]
    fn tradier_debit_lifecycle_state_detects_open_spread() {
        let action = tradier_call_debit_action("already_open");
        let positions = vec![
            tradier_test_position("ORCL260702C00220000", 1.0),
            tradier_test_position("ORCL260702C00225000", -1.0),
        ];

        let state = tradier_debit_spread_lifecycle_state(&action, &positions, &[]).unwrap();

        assert_eq!(
            state,
            TradierDebitSpreadLifecycleState::Open { quantity: 1 }
        );
    }

    #[test]
    fn tradier_debit_lifecycle_state_blocks_active_order() {
        let action = tradier_call_debit_action("new_entry");
        let orders = vec![tradier_test_order(
            Some("abc123"),
            Some("ORCL"),
            None,
            Some("open"),
            Some(1.0),
        )];

        let state = tradier_debit_spread_lifecycle_state(&action, &[], &orders).unwrap();

        assert_eq!(
            state,
            TradierDebitSpreadLifecycleState::ActiveOrder {
                id: Some("abc123".to_owned()),
                status: Some("open".to_owned())
            }
        );
    }

    #[test]
    fn tradier_debit_lifecycle_state_detects_partial_exposure() {
        let action = tradier_call_debit_action("already_open");
        let positions = vec![tradier_test_position("ORCL260702C00220000", 1.0)];

        let state = tradier_debit_spread_lifecycle_state(&action, &positions, &[]).unwrap();

        assert!(matches!(
            state,
            TradierDebitSpreadLifecycleState::Inconsistent { .. }
        ));
    }

    #[test]
    fn tradier_debit_lifecycle_state_detects_assigned_short_call() {
        let action = tradier_call_debit_action("already_open");
        let positions = vec![
            tradier_test_position("ORCL260702C00220000", 1.0),
            tradier_test_position("ORCL", -100.0),
        ];

        let state = tradier_debit_spread_lifecycle_state(&action, &positions, &[]).unwrap();

        assert_eq!(
            state,
            TradierDebitSpreadLifecycleState::AssignedShortLeg {
                right: OptionRight::Call,
                long_quantity: 1.0,
                stock_quantity: -100.0
            }
        );
    }

    #[test]
    fn tradier_debit_lifecycle_state_detects_assigned_short_put() {
        let action = tradier_put_debit_action("already_open");
        let positions = vec![
            tradier_test_position("TSLA260702P00355000", 1.0),
            tradier_test_position("TSLA", 100.0),
        ];

        let state = tradier_debit_spread_lifecycle_state(&action, &positions, &[]).unwrap();

        assert_eq!(
            state,
            TradierDebitSpreadLifecycleState::AssignedShortLeg {
                right: OptionRight::Put,
                long_quantity: 1.0,
                stock_quantity: 100.0
            }
        );
    }

    #[test]
    fn tradier_debit_lifecycle_state_rejects_wrong_direction_stock() {
        let action = tradier_call_debit_action("already_open");
        let positions = vec![
            tradier_test_position("ORCL260702C00220000", 1.0),
            tradier_test_position("ORCL", 100.0),
        ];

        let state = tradier_debit_spread_lifecycle_state(&action, &positions, &[]).unwrap();

        assert!(matches!(
            state,
            TradierDebitSpreadLifecycleState::Inconsistent { .. }
        ));
    }

    #[test]
    fn tradier_credit_lifecycle_state_detects_open_spread() {
        let action = tradier_call_credit_action("already_open");
        let positions = vec![
            tradier_test_position("ORCL260702C00230000", 1.0),
            tradier_test_position("ORCL260702C00225000", -1.0),
        ];

        let state = tradier_vertical_spread_lifecycle_state(&action, &positions, &[]).unwrap();

        assert_eq!(
            state,
            TradierDebitSpreadLifecycleState::Open { quantity: 1 }
        );
    }

    #[test]
    fn tradier_credit_lifecycle_state_detects_open_put_spread() {
        let action = tradier_put_credit_action("already_open");
        let positions = vec![
            tradier_test_position("TSLA260702P00345000", 1.0),
            tradier_test_position("TSLA260702P00350000", -1.0),
        ];

        let state = tradier_vertical_spread_lifecycle_state(&action, &positions, &[]).unwrap();

        assert_eq!(
            state,
            TradierDebitSpreadLifecycleState::Open { quantity: 1 }
        );
    }

    #[test]
    fn tradier_credit_lifecycle_state_detects_assigned_short_call() {
        let action = tradier_call_credit_action("already_open");
        let positions = vec![
            tradier_test_position("ORCL260702C00230000", 1.0),
            tradier_test_position("ORCL", -100.0),
        ];

        let state = tradier_vertical_spread_lifecycle_state(&action, &positions, &[]).unwrap();

        assert_eq!(
            state,
            TradierDebitSpreadLifecycleState::AssignedShortLeg {
                right: OptionRight::Call,
                long_quantity: 1.0,
                stock_quantity: -100.0
            }
        );
    }

    #[test]
    fn tradier_credit_lifecycle_state_detects_assigned_short_put() {
        let action = tradier_put_credit_action("already_open");
        let positions = vec![
            tradier_test_position("TSLA260702P00345000", 1.0),
            tradier_test_position("TSLA", 100.0),
        ];

        let state = tradier_vertical_spread_lifecycle_state(&action, &positions, &[]).unwrap();

        assert_eq!(
            state,
            TradierDebitSpreadLifecycleState::AssignedShortLeg {
                right: OptionRight::Put,
                long_quantity: 1.0,
                stock_quantity: 100.0
            }
        );
    }

    #[test]
    fn tradier_debit_lifecycle_state_allows_other_exported_management_legs() {
        let action = tradier_call_debit_action("already_open");
        let other_action = test_trade_signal_summary(
            &serde_json::json!({
                "status":"already_open",
                "symbol":"ORCL",
                "strategy":"call_debit_spread",
                "entry_date":"2026-06-29",
                "exit_date":"2026-07-01",
                "expiration":"2026-07-02",
                "short_strike":235.0,
                "long_strike":230.0,
                "entry_credit":-4.50,
                "max_loss":450.0
            }),
            None,
        );
        let positions = vec![
            tradier_test_position("ORCL260702C00220000", 1.0),
            tradier_test_position("ORCL260702C00225000", -1.0),
            tradier_test_position("ORCL260702C00230000", 1.0),
            tradier_test_position("ORCL260702C00235000", -1.0),
        ];
        let allowed =
            tradier_debit_spread_management_position_symbols(&[action.clone(), other_action])
                .unwrap();

        let strict_state = tradier_debit_spread_lifecycle_state(&action, &positions, &[]).unwrap();
        let management_state = tradier_debit_spread_lifecycle_state_with_allowed_positions(
            &action,
            &positions,
            &[],
            Some(&allowed),
        )
        .unwrap();

        assert!(matches!(
            strict_state,
            TradierDebitSpreadLifecycleState::Inconsistent { .. }
        ));
        assert_eq!(
            management_state,
            TradierDebitSpreadLifecycleState::Open { quantity: 1 }
        );
    }

    #[test]
    fn tradier_debit_exit_credit_uses_long_bid_less_short_ask() {
        let quotes = vec![
            tradier_test_quote("ORCL260702C00220000", Some(3.60), Some(3.80)),
            tradier_test_quote("ORCL260702C00225000", Some(0.30), Some(0.40)),
        ];

        let credit = tradier_current_debit_exit_credit(
            &quotes,
            "ORCL260702C00220000",
            "ORCL260702C00225000",
            ExecutionMode::Review,
        )
        .unwrap();

        assert!((credit - 3.20).abs() < 1e-9);
    }

    #[test]
    fn tradier_debit_exit_credit_keeps_non_positive_values_for_rule_parity() {
        let quotes = vec![
            tradier_test_quote("ORCL260702C00220000", Some(0.00), Some(0.05)),
            tradier_test_quote("ORCL260702C00225000", Some(0.01), Some(0.10)),
        ];

        let credit = tradier_current_debit_exit_credit(
            &quotes,
            "ORCL260702C00220000",
            "ORCL260702C00225000",
            ExecutionMode::Review,
        )
        .unwrap();

        assert!((credit + 0.10).abs() < 1e-9);
    }

    #[test]
    fn tradier_credit_exit_debit_uses_short_ask_less_long_bid() {
        let quotes = vec![
            tradier_test_quote("ORCL260702C00225000", Some(1.00), Some(1.20)),
            tradier_test_quote("ORCL260702C00230000", Some(0.15), Some(0.25)),
        ];

        let debit = tradier_current_credit_exit_debit(
            &quotes,
            "ORCL260702C00225000",
            "ORCL260702C00230000",
            5.0,
            ExecutionMode::Review,
        )
        .unwrap();

        assert!((debit - 1.05).abs() < 1e-9);
    }

    #[test]
    fn live_debit_spread_exit_plan_triggers_take_profit() {
        let action = tradier_call_debit_action("already_open");

        let plan = live_debit_spread_exit_plan(
            &action,
            4.85,
            NaiveDate::from_ymd_opt(2026, 6, 29).unwrap(),
        )
        .unwrap();

        assert_eq!(
            plan,
            DebitSpreadExitPlan::Close {
                reason: "take_profit".to_owned(),
                limit_credit: 4.85
            }
        );
    }

    #[test]
    fn live_debit_spread_exit_plan_triggers_stop_loss_at_negative_credit() {
        let action = tradier_call_debit_action("already_open");

        let plan = live_debit_spread_exit_plan(
            &action,
            -0.10,
            NaiveDate::from_ymd_opt(2026, 6, 29).unwrap(),
        )
        .unwrap();

        assert_eq!(
            plan,
            DebitSpreadExitPlan::Close {
                reason: "stop_loss".to_owned(),
                limit_credit: -0.10
            }
        );
    }

    #[test]
    fn live_debit_spread_exit_plan_holds_when_no_rule_fires() {
        let mut action = tradier_call_debit_action("already_open");
        action.execution_rules = Some(LiveExecutionRules {
            take_profit_pct: 0.50,
            stop_loss_multiple: 2.0,
            force_close_dte: 0,
            max_hold_days: None,
        });

        let plan = live_debit_spread_exit_plan(
            &action,
            4.00,
            NaiveDate::from_ymd_opt(2026, 6, 29).unwrap(),
        )
        .unwrap();

        assert!(matches!(plan, DebitSpreadExitPlan::Hold { .. }));
    }

    #[test]
    fn live_credit_spread_exit_plan_triggers_take_profit() {
        let action = tradier_call_credit_action("already_open");

        let plan = live_credit_spread_exit_plan(
            &action,
            0.20,
            NaiveDate::from_ymd_opt(2026, 6, 29).unwrap(),
        )
        .unwrap();

        assert_eq!(
            plan,
            CreditSpreadExitPlan::Close {
                reason: "take_profit".to_owned(),
                limit_debit: 0.20
            }
        );
    }

    #[test]
    fn live_credit_spread_exit_plan_triggers_stop_loss() {
        let action = tradier_call_credit_action("already_open");

        let plan = live_credit_spread_exit_plan(
            &action,
            2.50,
            NaiveDate::from_ymd_opt(2026, 6, 29).unwrap(),
        )
        .unwrap();

        assert_eq!(
            plan,
            CreditSpreadExitPlan::Close {
                reason: "stop_loss".to_owned(),
                limit_debit: 2.50
            }
        );
    }

    #[test]
    fn tradier_occ_option_symbol_handles_integer_and_decimal_strikes() {
        let expiration = NaiveDate::from_ymd_opt(2026, 7, 2).unwrap();
        let integer = OptionKey::new(
            "AAPL",
            expiration,
            decimal_from_f64(225.0, "strike").unwrap(),
            OptionRight::Call,
        );
        let decimal = OptionKey::new(
            "AAPL",
            expiration,
            decimal_from_f64(12.5, "strike").unwrap(),
            OptionRight::Put,
        );

        assert_eq!(
            tradier_occ_option_symbol(&integer).unwrap(),
            "AAPL260702C00225000"
        );
        assert_eq!(
            tradier_occ_option_symbol(&decimal).unwrap(),
            "AAPL260702P00012500"
        );
    }

    #[test]
    fn tradier_invalid_spread_geometry_is_rejected_before_http() {
        let action = test_trade_signal_summary(
            &serde_json::json!({
                "status":"new_entry",
                "symbol":"ORCL",
                "strategy":"call_debit_spread",
                "entry_date":"2026-06-29",
                "exit_date":"2026-06-29",
                "expiration":"2026-07-02",
                "short_strike":220.0,
                "long_strike":225.0,
                "entry_credit":-1.00,
                "max_loss":100.0
            }),
            None,
        );

        let err = tradier_multileg_debit_payload(&action).unwrap_err();

        assert!(
            err.to_string()
                .contains("long call strike below short call strike")
        );
    }

    #[test]
    fn tradier_missing_debit_spread_short_strike_is_rejected_before_http() {
        let action = test_trade_signal_summary(
            &serde_json::json!({
                "status":"new_entry",
                "symbol":"TSLA",
                "strategy":"put_debit_spread",
                "entry_date":"2026-06-29",
                "exit_date":"2026-06-29",
                "expiration":"2026-07-02",
                "long_strike":355.0,
                "entry_credit":-1.00,
                "max_loss":100.0
            }),
            None,
        );

        let err = tradier_multileg_debit_payload(&action).unwrap_err();

        assert!(err.to_string().contains("missing positive short_strike"));
    }

    #[test]
    fn tradier_wheel_payload_matches_single_option_shape() {
        let action = test_trade_signal_summary(
            &serde_json::json!({
                "status":"new_entry",
                "symbol":"CRWV",
                "strategy":"wheel",
                "entry_date":"2026-06-29",
                "exit_date":"2026-06-29",
                "expiration":"2026-07-02",
                "short_strike":80.0,
                "entry_credit":1.12,
                "max_loss":7888.0
            }),
            Some(TradeSignalRisk {
                reserve: 8000.0,
                reserve_basis: "short_put_x100".to_owned(),
            }),
        );

        let payload = tradier_cash_secured_put_payload(&action).unwrap();

        assert_eq!(payload.get("class").map(String::as_str), Some("option"));
        assert_eq!(payload.get("symbol").map(String::as_str), Some("CRWV"));
        assert_eq!(
            payload.get("option_symbol").map(String::as_str),
            Some("CRWV260702P00080000")
        );
        assert_eq!(
            payload.get("side").map(String::as_str),
            Some("sell_to_open")
        );
        assert_eq!(payload.get("quantity").map(String::as_str), Some("1"));
        assert_eq!(payload.get("type").map(String::as_str), Some("limit"));
        assert_eq!(payload.get("duration").map(String::as_str), Some("day"));
        assert_eq!(payload.get("price").map(String::as_str), Some("1.12"));
    }

    #[test]
    fn tradier_monitor_mode_does_not_require_credentials() {
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"new_entry",
            "symbol":"ORCL",
            "strategy":"call_debit_spread",
            "entry_date":"2026-06-29",
            "exit_date":"2026-06-29",
            "expiration":"2026-07-02",
            "short_strike":225.0,
            "long_strike":220.0,
            "entry_credit":-4.50,
            "max_loss":450.0
        }]));
        let broker = execution_broker(BrokerKind::Tradier, false, false, false, false);

        let decision = compute_execution_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 29).unwrap(),
            &test_canary_risk(),
            &broker,
            ExecutionMode::Monitor,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "ready");
        assert_eq!(decision.broker, BrokerKind::Tradier);
    }

    #[test]
    fn execution_worker_default_broker_is_tradier_monitor_safe() {
        let path = unique_main_test_path("execution-worker-default-tradier.json");
        let artifact = test_live_signal_artifact_as_of(
            Some(NaiveDate::from_ymd_opt(2026, 6, 28).unwrap()),
            serde_json::json!([{
                "status":"recent_closed",
                "symbol":"TSLA",
                "strategy":"put_debit_spread",
                "entry_date":"2026-06-25",
                "exit_date":"2026-06-26",
                "max_loss":100.0
            }]),
        );
        fs::write(&path, serde_json::to_string(&artifact).unwrap()).unwrap();
        let args = ExecutionWorkerArgs {
            live_signal: path.clone(),
            as_of: Some(NaiveDate::from_ymd_opt(2026, 6, 28).unwrap()),
            risk: test_canary_risk(),
            broker: execution_broker(BrokerKind::Tradier, false, false, false, false),
            mode: ExecutionMode::Monitor,
            robinhood_mcp_command: None,
            order_ledger: unique_main_test_path("canary-order-ledger-default-tradier.json"),
            notify_command: None,
            notify_ledger: unique_main_test_path("canary-notify-ledger-default-tradier.json"),
            max_order_age_seconds: DEFAULT_MAX_ORDER_AGE_SECONDS,
            poll_seconds: 60,
            once: true,
            health_output: None,
            json: true,
        };

        let health = execution_worker_health(&args);

        assert_eq!(health.status, "monitor");
        assert_eq!(health.broker, BrokerKind::Tradier);
        assert!(!health.tradier_credentials_configured);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn tradier_review_mode_fails_closed_without_credentials() {
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"new_entry",
            "symbol":"ORCL",
            "strategy":"call_debit_spread",
            "entry_date":"2026-06-29",
            "exit_date":"2026-06-29",
            "expiration":"2026-07-02",
            "short_strike":225.0,
            "long_strike":220.0,
            "entry_credit":-4.50,
            "max_loss":450.0
        }]));
        let broker = execution_broker(BrokerKind::Tradier, false, false, false, false);
        let mut decision = compute_execution_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 29).unwrap(),
            &test_canary_risk(),
            &broker,
            ExecutionMode::Review,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        apply_tradier_rest_bridge_with_config_result(
            &mut decision,
            None,
            Err(anyhow::anyhow!("missing test credentials")),
        )
        .unwrap();

        assert_eq!(decision.status, "blocked");
        assert!(
            decision
                .reason
                .contains("Tradier credentials are not configured")
        );
    }

    #[test]
    fn live_broker_bridge_blocks_unsupported_position_lifecycle() {
        let ledger = unique_main_test_path("tradier-live-lifecycle-gate.json");
        let broker = execution_broker(BrokerKind::Robinhood, false, false, false, true);
        let mut decision = tradier_wheel_test_decision(ExecutionMode::Live);
        decision.broker = BrokerKind::Robinhood;
        decision.status = "ready".to_owned();
        decision.reason = "test ready wheel decision".to_owned();

        apply_broker_bridge(&mut decision, &broker, None, Some(&ledger)).unwrap();

        assert_eq!(decision.status, "blocked");
        assert!(
            decision
                .reason
                .contains("not enabled for this broker/strategy")
        );
        assert!(!ledger.exists());
    }

    #[test]
    fn tradier_review_sends_preview_without_place() {
        let (base_url, requests, handle) = spawn_tradier_debit_mock(vec![(
            200,
            r#"{"order":{"id":"preview","status":"ok","result":true}}"#,
        )]);
        let mut decision = tradier_test_decision(ExecutionMode::Review);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "reviewed");
        assert!(decision.tradier_quote.is_some());
        assert_eq!(bodies.len(), 5);
        assert!(bodies[4].contains("preview=true"));
        assert!(bodies[4].contains("class=multileg"));
        assert!(bodies[4].contains("type=debit"));
    }

    #[test]
    fn tradier_credit_review_sends_preview_without_place() {
        let (base_url, requests, handle) = spawn_tradier_credit_mock(vec![(
            200,
            r#"{"order":{"id":"preview","status":"ok","result":true}}"#,
        )]);
        let as_of = NaiveDate::from_ymd_opt(2026, 6, 29).unwrap();
        let mut artifact = test_live_signal_artifact_as_of(
            Some(as_of),
            serde_json::json!([{
                "status":"new_entry",
                "symbol":"ORCL",
                "strategy":"call_credit_spread",
                "entry_date":"2026-06-29",
                "exit_date":"2026-06-29",
                "expiration":"2026-07-02",
                "short_strike":225.0,
                "long_strike":230.0,
                "width":5.0,
                "entry_credit":1.20,
                "max_loss":380.0
            }]),
        );
        artifact
            .approved_strategy
            .allowed_live_strategies
            .push("call_credit_spread".to_owned());
        let broker = execution_broker(BrokerKind::Tradier, false, false, false, false);
        let mut decision = compute_execution_decision(
            &artifact,
            as_of,
            &test_canary_risk(),
            &broker,
            ExecutionMode::Review,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "reviewed");
        assert!(decision.tradier_quote.is_some());
        assert_eq!(bodies.len(), 5);
        assert!(bodies[4].contains("preview=true"));
        assert!(bodies[4].contains("class=multileg"));
        assert!(bodies[4].contains("type=credit"));
        assert!(bodies[4].contains("sell_to_open"));
        assert!(bodies[4].contains("buy_to_open"));
    }

    #[test]
    fn tradier_management_review_previews_close_when_exit_rule_fires() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (200, tradier_open_call_debit_positions_response()),
            (200, r#"{"orders":{"order":[]}}"#.to_owned()),
            (200, tradier_debit_exit_quote_response(4.90, 0.05)),
            (
                200,
                r#"{"order":{"id":"preview","status":"ok","result":true}}"#.to_owned(),
            ),
        ]);
        let mut decision = tradier_management_test_decision(ExecutionMode::Review);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "reviewed");
        assert_eq!(decision.action_kind, Some(ExecutionActionKind::ManageOpen));
        assert!(decision.tradier_quote.is_some());
        assert_eq!(bodies.len(), 4);
        assert!(bodies[3].contains("preview=true"));
        assert!(bodies[3].contains("class=multileg"));
        assert!(bodies[3].contains("type=credit"));
        assert!(bodies[3].contains("price=4.85"));
        assert!(bodies[3].contains("sell_to_close"));
        assert!(bodies[3].contains("buy_to_close"));
    }

    #[test]
    fn tradier_credit_management_review_previews_close_when_exit_rule_fires() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (200, tradier_open_call_credit_positions_response()),
            (200, r#"{"orders":{"order":[]}}"#.to_owned()),
            (200, tradier_credit_exit_quote_response(0.30, 0.15)),
            (
                200,
                r#"{"order":{"id":"preview","status":"ok","result":true}}"#.to_owned(),
            ),
        ]);
        let mut decision = tradier_credit_management_test_decision(ExecutionMode::Review);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "reviewed");
        assert_eq!(decision.action_kind, Some(ExecutionActionKind::ManageOpen));
        assert!(decision.tradier_quote.is_some());
        assert_eq!(bodies.len(), 4);
        assert!(bodies[3].contains("preview=true"));
        assert!(bodies[3].contains("class=multileg"));
        assert!(bodies[3].contains("type=debit"));
        assert!(bodies[3].contains("price=0.15"));
        assert!(bodies[3].contains("buy_to_close"));
        assert!(bodies[3].contains("sell_to_close"));
    }

    #[test]
    fn tradier_put_credit_management_review_previews_close_when_exit_rule_fires() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (200, tradier_open_put_credit_positions_response()),
            (200, r#"{"orders":{"order":[]}}"#.to_owned()),
            (200, tradier_put_credit_exit_quote_response(0.30, 0.15)),
            (
                200,
                r#"{"order":{"id":"preview","status":"ok","result":true}}"#.to_owned(),
            ),
        ]);
        let mut decision = tradier_put_credit_management_test_decision(ExecutionMode::Review);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "reviewed");
        assert_eq!(decision.action_kind, Some(ExecutionActionKind::ManageOpen));
        assert!(decision.tradier_quote.is_some());
        assert_eq!(bodies.len(), 4);
        assert!(bodies[3].contains("preview=true"));
        assert!(bodies[3].contains("class=multileg"));
        assert!(bodies[3].contains("type=debit"));
        assert!(bodies[3].contains("price=0.15"));
        assert!(bodies[3].contains("buy_to_close"));
        assert!(bodies[3].contains("sell_to_close"));
        assert!(bodies[3].contains("TSLA260702P00350000"));
        assert!(bodies[3].contains("TSLA260702P00345000"));
    }

    #[test]
    fn tradier_management_holds_without_preview_when_no_exit_rule_fires() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (200, tradier_open_call_debit_positions_response()),
            (200, r#"{"orders":{"order":[]}}"#.to_owned()),
            (200, tradier_debit_exit_quote_response(4.20, 0.30)),
        ]);
        let mut decision = tradier_management_test_decision(ExecutionMode::Review);
        let rules = LiveExecutionRules {
            take_profit_pct: 0.50,
            stop_loss_multiple: 2.0,
            force_close_dte: 0,
            max_hold_days: None,
        };
        decision.selected_signal.as_mut().unwrap().execution_rules = Some(rules.clone());
        decision.management_signals[0].execution_rules = Some(rules);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "holding");
        assert!(decision.tradier_preview.is_none());
        assert_eq!(bodies.len(), 3);
    }

    #[test]
    fn tradier_management_reports_due_time_exit_when_quote_validation_fails() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (200, tradier_open_call_debit_positions_response()),
            (200, r#"{"orders":{"order":[]}}"#.to_owned()),
            (
                400,
                r#"{"errors":{"error":[{"message":"quote unavailable"}]}}"#.to_owned(),
            ),
        ]);
        let mut decision = tradier_management_test_decision(ExecutionMode::Review);
        let as_of = decision.as_of;
        let entry_date = (as_of - chrono::Duration::days(3)).to_string();
        let rules = LiveExecutionRules {
            take_profit_pct: 0.50,
            stop_loss_multiple: 2.0,
            force_close_dte: 0,
            max_hold_days: Some(1),
        };
        decision.selected_signal.as_mut().unwrap().entry_date = Some(entry_date.clone());
        decision.selected_signal.as_mut().unwrap().execution_rules = Some(rules.clone());
        decision.management_signals[0].entry_date = Some(entry_date);
        decision.management_signals[0].execution_rules = Some(rules);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "blocked");
        assert!(decision.reason.contains("close triggered max_hold"));
        assert!(decision.reason.contains("exit quote validation failed"));
        assert!(decision.tradier_preview.is_none());
        assert_eq!(bodies.len(), 3);
    }

    #[test]
    fn tradier_management_blocks_assigned_short_call_with_specific_reason() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (200, tradier_assigned_call_debit_positions_response()),
            (200, r#"{"orders":{"order":[]}}"#.to_owned()),
        ]);
        let mut decision = tradier_management_test_decision(ExecutionMode::Review);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "blocked");
        assert!(decision.reason.contains("short call appears assigned"));
        assert!(
            decision
                .reason
                .contains("manual broker assignment recovery")
        );
        assert!(decision.tradier_preview.is_none());
        assert_eq!(bodies.len(), 2);
    }

    #[test]
    fn tradier_credit_management_blocks_assigned_short_call_with_specific_reason() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (200, tradier_assigned_call_credit_positions_response()),
            (200, r#"{"orders":{"order":[]}}"#.to_owned()),
        ]);
        let mut decision = tradier_credit_management_test_decision(ExecutionMode::Review);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "blocked");
        assert!(decision.reason.contains("short call appears assigned"));
        assert!(
            decision
                .reason
                .contains("manual broker assignment recovery")
        );
        assert!(decision.tradier_preview.is_none());
        assert_eq!(bodies.len(), 2);
    }

    #[test]
    fn tradier_credit_management_blocks_assigned_short_put_with_specific_reason() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (200, tradier_assigned_put_credit_positions_response()),
            (200, r#"{"orders":{"order":[]}}"#.to_owned()),
        ]);
        let mut decision = tradier_put_credit_management_test_decision(ExecutionMode::Review);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "blocked");
        assert!(decision.reason.contains("short put appears assigned"));
        assert!(
            decision
                .reason
                .contains("manual broker assignment recovery")
        );
        assert!(decision.tradier_preview.is_none());
        assert_eq!(bodies.len(), 2);
    }

    #[test]
    fn tradier_entry_bridge_preflights_management_before_new_entry() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (200, tradier_open_call_debit_positions_response()),
            (200, r#"{"orders":{"order":[]}}"#.to_owned()),
            (200, tradier_debit_exit_quote_response(4.90, 0.05)),
            (
                200,
                r#"{"order":{"id":"preview","status":"ok","result":true}}"#.to_owned(),
            ),
        ]);
        let artifact = test_live_signal_artifact_as_of(
            Some(NaiveDate::from_ymd_opt(2026, 6, 29).unwrap()),
            serde_json::json!([
                {
                    "status":"new_entry",
                    "symbol":"TSLA",
                    "strategy":"put_debit_spread",
                    "entry_date":"2026-06-29",
                    "exit_date":"2026-06-29",
                    "expiration":"2026-07-02",
                    "short_strike":350.0,
                    "long_strike":355.0,
                    "entry_credit":-1.00,
                    "max_loss":100.0
                },
                {
                    "status":"already_open",
                    "symbol":"ORCL",
                    "strategy":"call_debit_spread",
                    "entry_date":"2026-06-26",
                    "exit_date":"2026-07-01",
                    "expiration":"2026-07-02",
                    "short_strike":225.0,
                    "long_strike":220.0,
                    "width":5.0,
                    "entry_credit":-4.50,
                    "max_loss":450.0
                }
            ]),
        );
        let broker = execution_broker(BrokerKind::Tradier, false, false, false, false);
        let mut decision = compute_execution_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 29).unwrap(),
            &test_canary_risk(),
            &broker,
            ExecutionMode::Review,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );
        let config = test_tradier_config(base_url);

        assert_eq!(decision.action_kind, Some(ExecutionActionKind::OpenEntry));
        assert_eq!(decision.management_signals.len(), 1);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "reviewed");
        assert_eq!(decision.action_kind, Some(ExecutionActionKind::ManageOpen));
        assert_eq!(
            decision
                .selected_signal
                .as_ref()
                .map(|signal| signal.symbol.as_str()),
            Some("ORCL")
        );
        assert_eq!(bodies.len(), 4);
        assert!(bodies[3].contains("type=credit"));
        assert!(bodies[3].contains("sell_to_close"));
        assert!(!bodies.iter().any(|body| body.contains("symbol=TSLA")));
    }

    #[test]
    fn tradier_live_close_ledger_blocks_changed_price_retry() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (
                200,
                r#"{"clock":{"state":"open","description":"Market is open"}}"#.to_owned(),
            ),
            (200, tradier_open_call_debit_positions_response()),
            (200, r#"{"orders":{"order":[]}}"#.to_owned()),
            (200, tradier_debit_exit_quote_response(4.90, 0.05)),
        ]);
        let ledger = unique_main_test_path("tradier-order-ledger-close-price-retry.json");
        let mut decision = tradier_management_test_decision(ExecutionMode::Live);
        let config = test_tradier_config(base_url);
        let action = decision.management_signals.first().unwrap().clone();
        let prior_payload = tradier_multileg_debit_close_payload(&action, 4.75, 1).unwrap();
        let prior_key = tradier_order_key(&config, &prior_payload, &action);
        execution_order_ledger_record_status(
            &ledger,
            &prior_key,
            "pending_unknown",
            Some("close123"),
            Some("prior close submit still unresolved"),
        )
        .unwrap();

        apply_tradier_rest_bridge_with_config(&mut decision, Some(&ledger), config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "submit_unknown");
        assert_eq!(decision.reason, "prior close submit still unresolved");
        assert_eq!(bodies.len(), 4);
        fs::remove_file(ledger).unwrap();
    }

    #[test]
    fn tradier_live_close_retries_after_terminal_unfilled_day_order() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (
                200,
                r#"{"clock":{"state":"open","description":"Market is open"}}"#.to_owned(),
            ),
            (200, tradier_open_call_debit_positions_response()),
            (
                200,
                r#"{"orders":{"order":{"id":"close123","symbol":"ORCL","option_symbol":"ORCL260702C00220000","status":"expired","quantity":1}}}"#.to_owned(),
            ),
            (200, tradier_debit_exit_quote_response(4.90, 0.05)),
            (
                200,
                r#"{"order":{"id":"preview","status":"ok","result":true}}"#.to_owned(),
            ),
            (
                200,
                r#"{"order":{"id":"close456","status":"ok"}}"#.to_owned(),
            ),
            (
                200,
                r#"{"orders":{"order":{"id":"close456","symbol":"ORCL","option_symbol":"ORCL260702C00220000","status":"open","quantity":1}}}"#.to_owned(),
            ),
        ]);
        let ledger = unique_main_test_path("tradier-order-ledger-close-terminal-retry.json");
        let mut decision = tradier_management_test_decision(ExecutionMode::Live);
        let config = test_tradier_config(base_url);
        let action = decision.management_signals.first().unwrap().clone();
        let prior_payload = tradier_multileg_debit_close_payload(&action, 4.75, 1).unwrap();
        let prior_key = tradier_order_key(&config, &prior_payload, &action);
        execution_order_ledger_record_status(
            &ledger,
            &prior_key,
            "submitted",
            Some("close123"),
            Some("prior close submit expired unfilled"),
        )
        .unwrap();

        apply_tradier_rest_bridge_with_config(&mut decision, Some(&ledger), config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "submitted");
        assert_eq!(bodies.len(), 7);
        assert!(bodies[4].contains("preview=true"));
        assert!(bodies[5].contains("preview=false"));
        assert!(
            read_execution_order_ledger(&ledger)
                .unwrap()
                .values()
                .any(|entry| {
                    entry.status == "submitted"
                        && entry.broker_order_id.as_deref() == Some("close456")
                })
        );
        fs::remove_file(ledger).unwrap();
    }

    #[test]
    fn tradier_live_close_blocks_when_prior_close_filled_but_exposure_remains() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (
                200,
                r#"{"clock":{"state":"open","description":"Market is open"}}"#.to_owned(),
            ),
            (200, tradier_open_call_debit_positions_response()),
            (
                200,
                r#"{"orders":{"order":{"id":"close123","symbol":"ORCL","option_symbol":"ORCL260702C00220000","status":"filled","quantity":1}}}"#.to_owned(),
            ),
            (200, tradier_debit_exit_quote_response(4.90, 0.05)),
        ]);
        let ledger = unique_main_test_path("tradier-order-ledger-close-filled-block.json");
        let mut decision = tradier_management_test_decision(ExecutionMode::Live);
        let config = test_tradier_config(base_url);
        let action = decision.management_signals.first().unwrap().clone();
        let prior_payload = tradier_multileg_debit_close_payload(&action, 4.75, 1).unwrap();
        let prior_key = tradier_order_key(&config, &prior_payload, &action);
        execution_order_ledger_record_status(
            &ledger,
            &prior_key,
            "submitted",
            Some("close123"),
            Some("prior close submit filled"),
        )
        .unwrap();

        apply_tradier_rest_bridge_with_config(&mut decision, Some(&ledger), config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "blocked");
        assert!(decision.reason.contains("close123"));
        assert!(decision.reason.contains("filled"));
        assert_eq!(bodies.len(), 4);
        fs::remove_file(ledger).unwrap();
    }

    #[test]
    fn tradier_management_blocks_non_positive_close_credit_after_exit_trigger() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (
                200,
                r#"{"clock":{"state":"open","description":"Market is open"}}"#.to_owned(),
            ),
            (200, tradier_open_call_debit_positions_response()),
            (200, r#"{"orders":{"order":[]}}"#.to_owned()),
            (200, tradier_debit_exit_quote_response(0.00, 0.05)),
        ]);
        let ledger = unique_main_test_path("tradier-order-ledger-close-zero-credit.json");
        let mut decision = tradier_management_test_decision(ExecutionMode::Live);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, Some(&ledger), config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "blocked");
        assert!(decision.reason.contains("stop_loss"));
        assert!(
            decision
                .reason
                .contains("manual close/expiry/assignment-risk")
        );
        assert!(decision.tradier_quote.is_some());
        assert!(decision.tradier_preview.is_none());
        assert_eq!(bodies.len(), 4);
        let _ = fs::remove_file(ledger);
    }

    #[test]
    fn tradier_live_close_preview_rejection_does_not_block_retry() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (
                200,
                r#"{"clock":{"state":"open","description":"Market is open"}}"#.to_owned(),
            ),
            (200, tradier_open_call_debit_positions_response()),
            (200, r#"{"orders":{"order":[]}}"#.to_owned()),
            (200, tradier_debit_exit_quote_response(4.90, 0.05)),
            (
                200,
                r#"{"order":{"id":"preview","status":"ok","result":true}}"#.to_owned(),
            ),
            (
                200,
                r#"{"order":{"id":"close123","status":"ok"}}"#.to_owned(),
            ),
            (
                200,
                r#"{"orders":{"order":{"id":"close123","symbol":"ORCL","option_symbol":"ORCL260702C00220000","status":"open","quantity":1}}}"#.to_owned(),
            ),
        ]);
        let ledger = unique_main_test_path("tradier-order-ledger-close-preview-retry.json");
        let mut decision = tradier_management_test_decision(ExecutionMode::Live);
        let config = test_tradier_config(base_url);
        let action = decision.management_signals.first().unwrap().clone();
        let prior_payload = tradier_multileg_debit_close_payload(&action, 4.75, 1).unwrap();
        let prior_key = tradier_order_key(&config, &prior_payload, &action);
        execution_order_ledger_record_status(
            &ledger,
            &prior_key,
            "preview_rejected",
            None,
            Some("prior close preview was rejected"),
        )
        .unwrap();

        apply_tradier_rest_bridge_with_config(&mut decision, Some(&ledger), config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "submitted");
        assert_eq!(bodies.len(), 7);
        assert!(
            read_execution_order_ledger(&ledger)
                .unwrap()
                .values()
                .any(|entry| entry.status == "submitted")
        );
        fs::remove_file(ledger).unwrap();
    }

    #[test]
    fn tradier_management_live_previews_then_places_close() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (
                200,
                r#"{"clock":{"state":"open","description":"Market is open"}}"#.to_owned(),
            ),
            (200, tradier_open_call_debit_positions_response()),
            (200, r#"{"orders":{"order":[]}}"#.to_owned()),
            (200, tradier_debit_exit_quote_response(4.90, 0.05)),
            (
                200,
                r#"{"order":{"id":"preview","status":"ok","result":true}}"#.to_owned(),
            ),
            (
                200,
                r#"{"order":{"id":"close123","status":"ok"}}"#.to_owned(),
            ),
            (
                200,
                r#"{"orders":{"order":{"id":"close123","symbol":"ORCL","option_symbol":"ORCL260702C00220000","status":"open","quantity":1}}}"#.to_owned(),
            ),
        ]);
        let ledger = unique_main_test_path("tradier-order-ledger-live-close.json");
        let mut decision = tradier_management_test_decision(ExecutionMode::Live);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, Some(&ledger), config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "submitted");
        assert_eq!(bodies.len(), 7);
        assert!(bodies[4].contains("preview=true"));
        assert!(bodies[5].contains("preview=false"));
        assert!(
            read_execution_order_ledger(&ledger)
                .unwrap()
                .values()
                .any(|entry| entry.status == "submitted")
        );
        fs::remove_file(ledger).unwrap();
    }

    #[test]
    fn tradier_current_quote_worse_than_limit_blocks_preview() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (
                200,
                r#"{"balances":{"account_number":"TEST123","option_buying_power":45000,"total_cash":45000}}"#.to_owned(),
            ),
            (200, r#"{"positions":{"position":[]}}"#.to_owned()),
            (200, r#"{"orders":{"order":[]}}"#.to_owned()),
            (
                200,
                r#"{"quotes":{"quote":[{"symbol":"ORCL260702C00220000","bid":5.30,"ask":5.50},{"symbol":"ORCL260702C00225000","bid":0.40,"ask":0.50}]}}"#.to_owned(),
            ),
        ]);
        let mut decision = tradier_test_decision(ExecutionMode::Review);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "blocked");
        assert!(
            decision
                .reason
                .contains("current conservative debit 5.10 exceeds order limit 4.50")
        );
        assert_eq!(bodies.len(), 4);
        assert!(decision.tradier_preview.is_none());
    }

    #[test]
    fn tradier_credit_current_quote_below_limit_blocks_preview() {
        let now_ms = Utc::now().timestamp_millis();
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (200, tradier_good_balances_response()),
            (200, r#"{"positions":{"position":[]}}"#.to_owned()),
            (200, r#"{"orders":{"order":[]}}"#.to_owned()),
            (200, tradier_credit_quote_response(now_ms, 1.00, 0.30)),
        ]);
        let as_of = NaiveDate::from_ymd_opt(2026, 6, 29).unwrap();
        let mut artifact = test_live_signal_artifact_as_of(
            Some(as_of),
            serde_json::json!([{
                "status":"new_entry",
                "symbol":"ORCL",
                "strategy":"call_credit_spread",
                "entry_date":"2026-06-29",
                "exit_date":"2026-06-29",
                "expiration":"2026-07-02",
                "short_strike":225.0,
                "long_strike":230.0,
                "width":5.0,
                "entry_credit":1.20,
                "max_loss":380.0
            }]),
        );
        artifact
            .approved_strategy
            .allowed_live_strategies
            .push("call_credit_spread".to_owned());
        let broker = execution_broker(BrokerKind::Tradier, false, false, false, false);
        let mut decision = compute_execution_decision(
            &artifact,
            as_of,
            &test_canary_risk(),
            &broker,
            ExecutionMode::Review,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "blocked");
        assert!(
            decision
                .reason
                .contains("current conservative credit 0.70 is below order limit 1.20")
        );
        assert_eq!(bodies.len(), 4);
        assert!(decision.tradier_preview.is_none());
    }

    #[test]
    fn tradier_debit_insufficient_buying_power_blocks_preview() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![(
            200,
            r#"{"balances":{"account_number":"TEST123","option_buying_power":100,"total_cash":100}}"#,
        )]);
        let mut decision = tradier_test_decision(ExecutionMode::Review);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "blocked");
        assert!(decision.reason.contains("buying-power precheck"));
        assert_eq!(bodies.len(), 1);
        assert!(decision.tradier_preview.is_none());
    }

    #[test]
    fn tradier_live_closed_market_clock_blocks_before_balances() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![(
            200,
            r#"{"clock":{"state":"closed","description":"Market is closed"}}"#,
        )]);
        let ledger = unique_main_test_path("tradier-order-ledger-market-closed.json");
        let mut decision = tradier_test_decision(ExecutionMode::Live);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, Some(&ledger), config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "blocked");
        assert!(decision.reason.contains("market clock"));
        assert_eq!(bodies.len(), 1);
        assert!(!ledger.exists());
    }

    #[test]
    fn tradier_live_stale_quote_blocks_preview() {
        let stale_ms = (Utc::now() - chrono::Duration::minutes(5)).timestamp_millis();
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (
                200,
                r#"{"clock":{"state":"open","description":"Market is open"}}"#.to_owned(),
            ),
            (200, tradier_good_balances_response()),
            (200, r#"{"positions":{"position":[]}}"#.to_owned()),
            (200, r#"{"orders":{"order":[]}}"#.to_owned()),
            (200, tradier_debit_quote_response(stale_ms)),
        ]);
        let ledger = unique_main_test_path("tradier-order-ledger-stale-quote.json");
        let mut decision = tradier_test_decision(ExecutionMode::Live);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, Some(&ledger), config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "blocked");
        assert!(decision.reason.contains("quote validation failed"));
        assert!(decision.reason.contains("age"));
        assert_eq!(bodies.len(), 5);
        assert!(!ledger.exists());
    }

    #[test]
    fn tradier_live_future_quote_blocks_preview() {
        let future_ms = (Utc::now() + chrono::Duration::minutes(5)).timestamp_millis();
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (
                200,
                r#"{"clock":{"state":"open","description":"Market is open"}}"#.to_owned(),
            ),
            (200, tradier_good_balances_response()),
            (200, r#"{"positions":{"position":[]}}"#.to_owned()),
            (200, r#"{"orders":{"order":[]}}"#.to_owned()),
            (200, tradier_debit_quote_response(future_ms)),
        ]);
        let ledger = unique_main_test_path("tradier-order-ledger-future-quote.json");
        let mut decision = tradier_test_decision(ExecutionMode::Live);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, Some(&ledger), config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "blocked");
        assert!(decision.reason.contains("quote validation failed"));
        assert!(decision.reason.contains("timestamp is in the future"));
        assert_eq!(bodies.len(), 5);
        assert!(!ledger.exists());
    }

    #[test]
    fn tradier_live_previews_then_places_same_payload() {
        let (base_url, requests, handle) = spawn_tradier_live_debit_mock(vec![
            (
                200,
                r#"{"order":{"id":"preview","status":"ok","result":true}}"#.to_owned(),
            ),
            (200, r#"{"order":{"id":"placed","status":"ok"}}"#.to_owned()),
            (
                200,
                r#"{"orders":{"order":{"id":"placed","symbol":"ORCL","option_symbol":"ORCL260702C00220000","status":"open","quantity":1}}}"#.to_owned(),
            ),
        ]);
        let ledger = unique_main_test_path("tradier-order-ledger-live.json");
        let mut decision = tradier_test_decision(ExecutionMode::Live);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, Some(&ledger), config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "submitted");
        assert!(decision.tradier_quote.is_some());
        assert_eq!(bodies.len(), 8);
        assert!(bodies[5].contains("preview=true"));
        assert!(bodies[6].contains("preview=false"));
        assert_eq!(
            bodies[5].replace("preview=true", "preview=false"),
            bodies[6]
        );
        fs::remove_file(ledger).unwrap();
    }

    #[test]
    fn tradier_preview_result_false_blocks_live_placement() {
        let (base_url, requests, handle) = spawn_tradier_live_debit_mock(vec![(
            200,
            r#"{"order":{"id":"preview","status":"ok","result":false,"reason_description":"bp fail"}}"#,
        )]);
        let ledger = unique_main_test_path("tradier-order-ledger-preview-result-false.json");
        let mut decision = tradier_test_decision(ExecutionMode::Live);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, Some(&ledger), config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "rejected");
        assert!(decision.reason.contains("bp fail"));
        assert_eq!(bodies.len(), 6);
        assert!(
            read_execution_order_ledger(&ledger)
                .unwrap()
                .values()
                .any(|entry| entry.status == "rejected")
        );
        fs::remove_file(ledger).unwrap();
    }

    #[test]
    fn tradier_preview_failure_blocks_live_placement() {
        let (base_url, requests, handle) =
            spawn_tradier_live_debit_mock(vec![(400, r#"{"errors":"bad"}"#)]);
        let ledger = unique_main_test_path("tradier-order-ledger-preview-fail.json");
        let mut decision = tradier_test_decision(ExecutionMode::Live);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, Some(&ledger), config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "rejected");
        assert_eq!(bodies.len(), 6);
        assert!(
            read_execution_order_ledger(&ledger)
                .unwrap()
                .values()
                .any(|entry| entry.status == "rejected")
        );
        fs::remove_file(ledger).unwrap();
    }

    #[test]
    fn tradier_rejected_preview_body_blocks_live_placement() {
        let (base_url, requests, handle) = spawn_tradier_live_debit_mock(vec![(
            200,
            r#"{"order":{"id":"preview","status":"rejected","result":false,"reason":"test rejection"}}"#,
        )]);
        let ledger = unique_main_test_path("tradier-order-ledger-body-rejected.json");
        let mut decision = tradier_test_decision(ExecutionMode::Live);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, Some(&ledger), config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "rejected");
        assert_eq!(bodies.len(), 6);
        assert!(
            read_execution_order_ledger(&ledger)
                .unwrap()
                .values()
                .any(|entry| entry.status == "rejected")
        );
        fs::remove_file(ledger).unwrap();
    }

    #[test]
    fn tradier_duplicate_ledger_blocks_second_placement() {
        let ledger = unique_main_test_path("tradier-order-ledger-duplicate.json");
        let config = test_tradier_config("http://127.0.0.1:1/v1".to_owned());
        let mut decision = tradier_test_decision(ExecutionMode::Live);
        let payload =
            tradier_multileg_debit_payload(decision.selected_signal.as_ref().unwrap()).unwrap();
        let order_key = tradier_order_key(
            &config,
            &payload,
            decision.selected_signal.as_ref().unwrap(),
        );
        execution_order_ledger_record_status(&ledger, &order_key, "submitted", None, None).unwrap();

        apply_tradier_rest_bridge_with_config(&mut decision, Some(&ledger), config).unwrap();

        assert_eq!(decision.status, "already_submitted");
        fs::remove_file(ledger).unwrap();
    }

    #[test]
    fn tradier_close_order_key_ignores_mutable_limit_price() {
        let config = test_tradier_config("http://127.0.0.1:1/v1".to_owned());
        let action = tradier_call_debit_action("already_open");
        let close_a = tradier_multileg_debit_close_payload(&action, 4.75, 1).unwrap();
        let close_b = tradier_multileg_debit_close_payload(&action, 4.85, 1).unwrap();
        let wheel_action = tradier_wheel_management_test_decision(ExecutionMode::Review)
            .selected_signal
            .unwrap();
        let wheel_close_a =
            tradier_single_option_payload("CRWV", "CRWV260702P00080000", "buy_to_close", 1, 0.20)
                .unwrap();
        let wheel_close_b =
            tradier_single_option_payload("CRWV", "CRWV260702P00080000", "buy_to_close", 1, 0.25)
                .unwrap();
        let equity_close_a = tradier_equity_sell_payload("CRWV", 100, 79.50).unwrap();
        let equity_close_b = tradier_equity_sell_payload("CRWV", 100, 79.75).unwrap();
        let mut entry_b =
            tradier_multileg_debit_payload(&tradier_call_debit_action("new_entry")).unwrap();
        entry_b.insert("price".to_owned(), "4.85".to_owned());
        let entry_a =
            tradier_multileg_debit_payload(&tradier_call_debit_action("new_entry")).unwrap();

        assert_eq!(
            tradier_order_key(&config, &close_a, &action),
            tradier_order_key(&config, &close_b, &action)
        );
        assert_eq!(
            tradier_order_key(&config, &wheel_close_a, &wheel_action),
            tradier_order_key(&config, &wheel_close_b, &wheel_action)
        );
        assert_eq!(
            tradier_order_key(&config, &equity_close_a, &wheel_action),
            tradier_order_key(&config, &equity_close_b, &wheel_action)
        );
        assert_ne!(
            tradier_order_key(&config, &entry_a, &tradier_call_debit_action("new_entry")),
            tradier_order_key(&config, &entry_b, &tradier_call_debit_action("new_entry"))
        );
    }

    #[test]
    fn execution_order_ledger_reservation_is_atomic_for_duplicate_key() {
        let ledger = unique_main_test_path("canary-order-ledger-reserve.json");
        let key = "broker-order-key";

        let first = execution_order_ledger_reserve_pending(&ledger, key, Some("first")).unwrap();
        let second = execution_order_ledger_reserve_pending(&ledger, key, Some("second")).unwrap();

        assert_eq!(first, ExecutionOrderLedgerReservation::Reserved);
        assert_eq!(second, ExecutionOrderLedgerReservation::AlreadyRecorded);
        assert_eq!(
            read_execution_order_ledger(&ledger)
                .unwrap()
                .get(key)
                .map(|entry| entry.status.as_str()),
            Some("pending_unknown")
        );
        fs::remove_file(ledger).unwrap();
    }

    #[test]
    fn execution_order_ledger_recovers_stale_lock() {
        let ledger = unique_main_test_path("canary-order-ledger-stale-lock.json");
        let lock = ledger.with_extension("json.lock");
        fs::write(&lock, "pid=999999 acquired_at=2000-01-01T00:00:00Z\n").unwrap();

        let reservation =
            execution_order_ledger_reserve_pending(&ledger, "stale-lock-key", Some("test"))
                .unwrap();

        assert_eq!(reservation, ExecutionOrderLedgerReservation::Reserved);
        assert!(!lock.exists());
        fs::remove_file(ledger).unwrap();
    }

    #[test]
    fn live_signal_refresh_lock_blocks_concurrent_refresh() {
        let state = unique_main_test_path("live-signal-refresh-lock.json");
        let lock_path = state.with_extension("refresh.lock");

        let first = acquire_live_signal_refresh_lock(&state, 900).unwrap();
        let second = acquire_live_signal_refresh_lock(&state, 900);

        assert!(second.is_err());
        drop(first);
        assert!(!lock_path.exists());
        let third = acquire_live_signal_refresh_lock(&state, 900).unwrap();
        drop(third);
        assert!(!lock_path.exists());
    }

    #[test]
    fn tradier_rejected_ledger_entry_blocks_retry() {
        let ledger = unique_main_test_path("tradier-order-ledger-rejected-retry.json");
        let config = test_tradier_config("http://127.0.0.1:9/v1".to_owned());
        let mut decision = tradier_test_decision(ExecutionMode::Live);
        let payload =
            tradier_multileg_debit_payload(decision.selected_signal.as_ref().unwrap()).unwrap();
        let order_key = tradier_order_key(
            &config,
            &payload,
            decision.selected_signal.as_ref().unwrap(),
        );
        execution_order_ledger_record_status(
            &ledger,
            &order_key,
            "rejected",
            None,
            Some("prior clean rejection"),
        )
        .unwrap();

        apply_tradier_rest_bridge_with_config(&mut decision, Some(&ledger), config).unwrap();

        assert_eq!(decision.status, "rejected");
        assert_eq!(decision.reason, "prior clean rejection");
        fs::remove_file(ledger).unwrap();
    }

    #[test]
    fn tradier_place_transport_failure_is_unknown_not_rejected() {
        let (base_url, requests, handle) = spawn_tradier_live_debit_mock(vec![(
            200,
            r#"{"order":{"id":"preview","status":"ok","result":true}}"#,
        )]);
        let ledger = unique_main_test_path("tradier-order-ledger-place-unknown.json");
        let mut decision = tradier_test_decision(ExecutionMode::Live);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, Some(&ledger), config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "submit_unknown");
        assert!(decision.reason.contains("check Tradier before retrying"));
        assert_eq!(bodies.len(), 6);
        assert!(
            read_execution_order_ledger(&ledger)
                .unwrap()
                .values()
                .any(|entry| entry.status == "pending_unknown")
        );
        fs::remove_file(ledger).unwrap();
    }

    #[test]
    fn tradier_post_submit_missing_status_is_unknown_not_submitted() {
        let (base_url, requests, handle) = spawn_tradier_live_debit_mock(vec![
            (
                200,
                r#"{"order":{"id":"preview","status":"ok","result":true}}"#.to_owned(),
            ),
            (200, r#"{"order":{"id":"placed","status":"ok"}}"#.to_owned()),
            (
                200,
                r#"{"orders":{"order":{"id":"placed","symbol":"ORCL","option_symbol":"ORCL260702C00220000","quantity":1}}}"#.to_owned(),
            ),
        ]);
        let ledger = unique_main_test_path("tradier-order-ledger-missing-confirm-status.json");
        let mut decision = tradier_test_decision(ExecutionMode::Live);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, Some(&ledger), config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "submit_unknown");
        assert!(decision.reason.contains("returned no order status"));
        assert_eq!(bodies.len(), 8);
        assert!(
            read_execution_order_ledger(&ledger)
                .unwrap()
                .values()
                .any(|entry| entry.status == "pending_unknown")
        );
        fs::remove_file(ledger).unwrap();
    }

    #[test]
    fn tradier_place_error_status_is_rejected_not_submitted() {
        let (base_url, requests, handle) = spawn_tradier_live_debit_mock(vec![
            (
                200,
                r#"{"order":{"id":"preview","status":"ok","result":true}}"#.to_owned(),
            ),
            (
                200,
                r#"{"order":{"id":"placed","status":"error","reason_description":"broker rejected"}}"#.to_owned(),
            ),
        ]);
        let ledger = unique_main_test_path("tradier-order-ledger-place-error.json");
        let mut decision = tradier_test_decision(ExecutionMode::Live);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, Some(&ledger), config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "rejected");
        assert!(decision.reason.contains("broker rejected"));
        assert_eq!(bodies.len(), 7);
        fs::remove_file(ledger).unwrap();
    }

    #[test]
    fn tradier_wheel_review_checks_broker_state_then_previews() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (
                200,
                r#"{"balances":{"account_number":"TEST123","option_buying_power":45000,"total_cash":45000}}"#.to_owned(),
            ),
            (200, r#"{"positions":{"position":[]}}"#.to_owned()),
            (200, r#"{"orders":{"order":[]}}"#.to_owned()),
            (
                200,
                tradier_single_option_quote_response("CRWV260702P00080000", 1.20, 1.30),
            ),
            (
                200,
                r#"{"order":{"id":"preview","status":"ok","result":true}}"#.to_owned(),
            ),
        ]);
        let config = test_tradier_config(base_url);
        let mut decision = tradier_wheel_test_decision(ExecutionMode::Review);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();
        let bodies = collect_mock_requests(requests, handle);

        assert_eq!(decision.status, "reviewed");
        assert!(decision.tradier_quote.is_some());
        assert_eq!(bodies.len(), 5);
        assert!(bodies[4].contains("preview=true"));
        assert!(bodies[4].contains("class=option"));
        assert!(bodies[4].contains("side=sell_to_open"));
        assert!(bodies[4].contains("type=limit"));
    }

    #[test]
    fn tradier_wheel_insufficient_buying_power_blocks_preview() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![(
            200,
            r#"{"balances":{"account_number":"TEST123","option_buying_power":5000,"total_cash":5000}}"#,
        )]);
        let config = test_tradier_config(base_url);
        let mut decision = tradier_wheel_test_decision(ExecutionMode::Review);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();
        let bodies = collect_mock_requests(requests, handle);

        assert_eq!(decision.status, "blocked");
        assert!(decision.reason.contains("buying power"));
        assert_eq!(bodies.len(), 1);
    }

    #[test]
    fn tradier_wheel_existing_position_blocks_preview() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (
                200,
                r#"{"balances":{"account_number":"TEST123","option_buying_power":45000,"total_cash":45000}}"#,
            ),
            (
                200,
                r#"{"positions":{"position":{"symbol":"CRWV","quantity":100,"cost_basis":8000}}}"#,
            ),
        ]);
        let config = test_tradier_config(base_url);
        let mut decision = tradier_wheel_test_decision(ExecutionMode::Review);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();
        let bodies = collect_mock_requests(requests, handle);

        assert_eq!(decision.status, "blocked");
        assert!(decision.reason.contains("existing Tradier position"));
        assert_eq!(bodies.len(), 2);
    }

    #[test]
    fn tradier_wheel_active_order_blocks_preview() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (
                200,
                r#"{"balances":{"account_number":"TEST123","option_buying_power":45000,"total_cash":45000}}"#,
            ),
            (200, r#"{"positions":{"position":[]}}"#),
            (
                200,
                r#"{"orders":{"order":{"id":"123","symbol":"CRWV","option_symbol":"CRWV260702P00080000","status":"open","quantity":1}}}"#,
            ),
        ]);
        let config = test_tradier_config(base_url);
        let mut decision = tradier_wheel_test_decision(ExecutionMode::Review);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();
        let bodies = collect_mock_requests(requests, handle);

        assert_eq!(decision.status, "blocked");
        assert!(decision.reason.contains("active Tradier order"));
        assert_eq!(bodies.len(), 3);
    }

    #[test]
    fn tradier_wheel_current_bid_below_limit_blocks_preview() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (
                200,
                r#"{"balances":{"account_number":"TEST123","option_buying_power":45000,"total_cash":45000}}"#.to_owned(),
            ),
            (200, r#"{"positions":{"position":[]}}"#.to_owned()),
            (200, r#"{"orders":{"order":[]}}"#.to_owned()),
            (
                200,
                tradier_single_option_quote_response("CRWV260702P00080000", 0.50, 0.60),
            ),
        ]);
        let config = test_tradier_config(base_url);
        let mut decision = tradier_wheel_test_decision(ExecutionMode::Review);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();
        let bodies = collect_mock_requests(requests, handle);

        assert_eq!(decision.status, "blocked");
        assert!(
            decision
                .reason
                .contains("current short-put bid 0.50 is below order limit credit 1.12")
        );
        assert!(decision.tradier_preview.is_none());
        assert_eq!(bodies.len(), 4);
    }

    #[test]
    fn tradier_wheel_terminal_order_does_not_block_preview() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (
                200,
                r#"{"balances":{"account_number":"TEST123","option_buying_power":45000,"total_cash":45000}}"#.to_owned(),
            ),
            (200, r#"{"positions":{"position":[]}}"#.to_owned()),
            (
                200,
                r#"{"orders":{"order":{"id":"123","symbol":"CRWV","option_symbol":"CRWV260702P00080000","status":"filled","quantity":1}}}"#.to_owned(),
            ),
            (
                200,
                tradier_single_option_quote_response("CRWV260702P00080000", 1.20, 1.30),
            ),
            (
                200,
                r#"{"order":{"id":"preview","status":"ok","result":true}}"#.to_owned(),
            ),
        ]);
        let config = test_tradier_config(base_url);
        let mut decision = tradier_wheel_test_decision(ExecutionMode::Review);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();
        let bodies = collect_mock_requests(requests, handle);

        assert_eq!(decision.status, "reviewed");
        assert_eq!(bodies.len(), 5);
        assert!(bodies[4].contains("preview=true"));
    }

    #[test]
    fn tradier_wheel_management_closes_short_put_when_take_profit_fires() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (200, tradier_open_wheel_short_put_positions_response()),
            (200, r#"{"orders":{"order":[]}}"#.to_owned()),
            (
                200,
                tradier_single_option_quote_response("CRWV260702P00080000", 0.15, 0.20),
            ),
            (
                200,
                r#"{"order":{"id":"preview","status":"ok","result":true}}"#.to_owned(),
            ),
        ]);
        let mut decision = tradier_wheel_management_test_decision(ExecutionMode::Review);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "reviewed");
        assert_eq!(decision.action_kind, Some(ExecutionActionKind::ManageOpen));
        assert!(decision.tradier_quote.is_some());
        assert_eq!(bodies.len(), 4);
        assert!(bodies[3].contains("preview=true"));
        assert!(bodies[3].contains("class=option"));
        assert!(bodies[3].contains("option_symbol=CRWV260702P00080000"));
        assert!(bodies[3].contains("side=buy_to_close"));
        assert!(bodies[3].contains("price=0.20"));
    }

    #[test]
    fn tradier_wheel_management_sells_covered_call_after_assignment() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (200, tradier_assigned_wheel_stock_positions_response()),
            (200, r#"{"orders":{"order":[]}}"#.to_owned()),
            (
                200,
                tradier_single_option_quote_response("CRWV260706C00085000", 0.65, 0.80),
            ),
            (
                200,
                r#"{"order":{"id":"preview","status":"ok","result":true}}"#.to_owned(),
            ),
        ]);
        let mut decision = tradier_wheel_management_test_decision(ExecutionMode::Review);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "reviewed");
        assert_eq!(decision.action_kind, Some(ExecutionActionKind::ManageOpen));
        assert!(decision.tradier_quote.is_some());
        assert_eq!(bodies.len(), 4);
        assert!(bodies[3].contains("preview=true"));
        assert!(bodies[3].contains("class=option"));
        assert!(bodies[3].contains("option_symbol=CRWV260706C00085000"));
        assert!(bodies[3].contains("side=sell_to_open"));
        assert!(bodies[3].contains("price=0.65"));
    }

    #[test]
    fn tradier_wheel_management_recognizes_existing_covered_call_from_broker() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (
                200,
                tradier_existing_wheel_covered_call_positions_response(),
            ),
            (200, r#"{"orders":{"order":[]}}"#.to_owned()),
        ]);
        let mut decision = tradier_wheel_management_test_decision(ExecutionMode::Review);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "holding");
        assert!(
            decision
                .reason
                .contains("already has Tradier covered-call quantity 1")
        );
        assert!(decision.tradier_quote.is_none());
        assert!(decision.tradier_preview.is_none());
        assert_eq!(bodies.len(), 2);
    }

    #[test]
    fn tradier_wheel_management_liquidates_assigned_stock_when_stock_mark_due() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (200, tradier_assigned_wheel_stock_positions_response()),
            (200, r#"{"orders":{"order":[]}}"#.to_owned()),
            (
                200,
                tradier_single_option_quote_response("CRWV", 79.50, 79.75),
            ),
            (
                200,
                r#"{"order":{"id":"preview","status":"ok","result":true}}"#.to_owned(),
            ),
        ]);
        let mut decision = tradier_wheel_management_test_decision(ExecutionMode::Review);
        let as_of = decision.as_of.to_string();
        if let Some(signal) = decision.selected_signal.as_mut() {
            signal.exit_date = Some(as_of.clone());
            signal.exit_reason = Some("stock_marked_after_calls".to_owned());
        }
        if let Some(signal) = decision.management_signals.first_mut() {
            signal.exit_date = Some(as_of);
            signal.exit_reason = Some("stock_marked_after_calls".to_owned());
        }
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "reviewed");
        assert_eq!(decision.action_kind, Some(ExecutionActionKind::ManageOpen));
        assert!(decision.tradier_quote.is_some());
        assert_eq!(bodies.len(), 4);
        assert!(bodies[3].contains("preview=true"));
        assert!(bodies[3].contains("class=equity"));
        assert!(bodies[3].contains("symbol=CRWV"));
        assert!(bodies[3].contains("side=sell"));
        assert!(bodies[3].contains("quantity=100"));
        assert!(bodies[3].contains("type=limit"));
        assert!(bodies[3].contains("price=79.50"));
    }

    #[test]
    fn tradier_wheel_management_blocks_multi_contract_covered_call() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (
                200,
                tradier_multi_contract_wheel_covered_call_positions_response(),
            ),
            (200, r#"{"orders":{"order":[]}}"#.to_owned()),
        ]);
        let mut decision = tradier_wheel_management_test_decision(ExecutionMode::Review);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "blocked");
        assert!(decision.reason.contains("covered-call quantity 2"));
        assert!(decision.reason.contains("manual management"));
        assert!(decision.tradier_preview.is_none());
        assert_eq!(bodies.len(), 2);
    }

    #[test]
    fn tradier_wheel_management_holds_assigned_stock_without_call_target() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (200, tradier_assigned_wheel_stock_positions_response()),
            (200, r#"{"orders":{"order":[]}}"#.to_owned()),
        ]);
        let mut decision = tradier_wheel_management_test_decision(ExecutionMode::Review);
        if let Some(signal) = decision.selected_signal.as_mut() {
            signal.wheel_covered_call_expiration = None;
            signal.wheel_covered_call_strike = None;
        }
        if let Some(signal) = decision.management_signals.first_mut() {
            signal.wheel_covered_call_expiration = None;
            signal.wheel_covered_call_strike = None;
        }
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "holding");
        assert!(
            decision
                .reason
                .contains("no exported covered-call target yet")
        );
        assert!(decision.tradier_quote.is_none());
        assert!(decision.tradier_preview.is_none());
        assert_eq!(bodies.len(), 2);
    }

    #[test]
    fn tradier_wheel_management_blocks_residual_stock_after_covered_call_assigned() {
        let (base_url, requests, handle) = spawn_tradier_mock(vec![
            (200, tradier_assigned_wheel_stock_positions_response()),
            (200, r#"{"orders":{"order":[]}}"#.to_owned()),
        ]);
        let as_of = NaiveDate::from_ymd_opt(2026, 6, 30).unwrap();
        let artifact = test_live_signal_artifact_as_of(
            Some(as_of),
            serde_json::json!([{
                "status":"recent_closed",
                "symbol":"CRWV",
                "strategy":"wheel",
                "entry_date":"2026-06-20",
                "exit_date":"2026-06-30",
                "expiration":"2026-06-27",
                "short_strike":80.0,
                "short_put":80.0,
                "entry_credit":1.12,
                "max_loss":7888.0,
                "exit_reason":"covered_call_assigned"
            }]),
        );
        let broker = execution_broker(BrokerKind::Tradier, false, false, false, false);
        let mut decision = compute_execution_decision(
            &artifact,
            as_of,
            &test_canary_risk(),
            &broker,
            ExecutionMode::Review,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );
        let config = test_tradier_config(base_url);

        assert_eq!(decision.status, "ready");
        assert_eq!(decision.management_signals.len(), 1);
        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();

        let bodies = collect_mock_requests(requests, handle);
        assert_eq!(decision.status, "blocked");
        assert!(decision.reason.contains("residual Tradier stock"));
        assert!(decision.reason.contains("covered-call assignment"));
        assert!(decision.reason.contains("manual broker reconciliation"));
        assert!(decision.tradier_quote.is_none());
        assert!(decision.tradier_preview.is_none());
        assert_eq!(bodies.len(), 2);
    }

    #[test]
    fn tradier_security_match_distinguishes_short_symbols_from_prefixes() {
        assert!(tradier_security_matches_underlying("A260702P00080000", "A"));
        assert!(tradier_security_matches_underlying("A", "A"));
        assert!(!tradier_security_matches_underlying("AAPL", "A"));
        assert!(!tradier_security_matches_underlying(
            "AAPL260702P00080000",
            "A"
        ));
    }

    #[test]
    fn validate_canary_risk_policy_rejects_non_finite_values() {
        let mut risk = test_canary_risk();
        risk.debit_max_loss = f64::NAN;

        let err = validate_canary_risk_policy(&risk).unwrap_err();

        assert!(err.to_string().contains("must be finite"));
    }

    #[test]
    fn tradier_network_error_does_not_crash_worker_decision() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}/v1", listener.local_addr().unwrap());
        drop(listener);
        let mut decision = tradier_test_decision(ExecutionMode::Review);
        let config = test_tradier_config(base_url);

        apply_tradier_rest_bridge_with_config(&mut decision, None, config).unwrap();

        assert_eq!(decision.status, "blocked");
        assert!(
            decision
                .reason
                .contains("Tradier buying-power precheck failed")
        );
    }

    #[test]
    fn execution_worker_snapshot_uses_cached_broker_account() {
        let health_path = unique_main_test_path("execution-health-cached-account.json");
        let pid_file = unique_main_test_path("execution-health-cached-account.pid");
        fs::write(&pid_file, std::process::id().to_string()).unwrap();
        fs::write(
            &health_path,
            serde_json::to_string_pretty(&serde_json::json!({
                "checked_at": Utc::now(),
                "service": "execution_worker",
                "status": "monitor",
                "live_signal": "var/live_signal.json",
                "live_signal_readable": true,
                "live_signal_parse_ok": true,
                "as_of": execution_default_as_of(Utc::now()),
                "risk": test_canary_risk(),
                "broker_multi_leg_options": true,
                "broker_cash_secured_puts": true,
                "broker_covered_calls": false,
                "broker": "tradier",
                "mode": "monitor",
                "broker_review_ok": false,
                "robinhood_mcp_command_configured": false,
                "tradier_credentials_configured": false,
                "order_ledger": "var/test-ledger.json",
                "broker_account": {
                    "broker": "tradier",
                    "status": "ok",
                    "account": "Tradier ****1234",
                    "equity": 9000.0,
                    "buying_power": 7897.67,
                    "cash": 7897.67,
                    "day_pnl": 12.34,
                    "open_pnl": 0.0,
                    "close_pnl": 12.34,
                    "requirement": 0.0,
                    "error": null
                },
                "decision": null,
                "error": null
            }))
            .unwrap(),
        )
        .unwrap();

        let snapshot = build_execution_worker_snapshot(&health_path, &pid_file, 60);

        assert!(
            snapshot
                .broker_rows
                .iter()
                .any(|row| { row.label == "Buying Power" && row.value == "$7897.67" })
        );
        assert!(
            snapshot
                .broker_rows
                .iter()
                .any(|row| { row.label == "Account" && row.value == "Tradier ****1234" })
        );
        fs::remove_file(health_path).unwrap();
        fs::remove_file(pid_file).unwrap();
    }

    #[test]
    fn execution_worker_snapshot_labels_market_closed_date_mismatch() {
        let as_of = NaiveDate::from_ymd_opt(2026, 6, 30).unwrap();
        let broker = execution_broker(BrokerKind::Tradier, true, true, false, true);
        let risk = test_canary_risk();
        let decision = execution_decision(
            "blocked",
            "live signal as_of 2026-06-29 does not match requested as_of 2026-06-30",
            as_of,
            ExecutionMode::Live,
            &risk,
            &broker,
            false,
            None,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
            None,
        );
        let health = ExecutionWorkerHealth {
            checked_at: Utc::now(),
            service: "execution_worker".to_owned(),
            status: "blocked".to_owned(),
            live_signal: "var/live_signal.json".to_owned(),
            live_signal_readable: true,
            live_signal_parse_ok: true,
            as_of,
            risk,
            broker_multi_leg_options: true,
            broker_cash_secured_puts: true,
            broker_covered_calls: false,
            broker: BrokerKind::Tradier,
            mode: ExecutionMode::Live,
            broker_review_ok: false,
            robinhood_mcp_command_configured: false,
            tradier_credentials_configured: true,
            order_ledger: "var/execution_order_ledger.json".to_owned(),
            broker_account: None,
            decision: Some(decision),
            error: None,
        };
        let refresh_snapshot = SignalRefreshSnapshot::Parsed(serde_json::json!({
            "status": "skipped_market_closed",
            "run_to": "2026-06-30",
            "reason": "after configured options-market refresh window"
        }));

        assert!(execution_blocked_by_market_session(
            &health,
            &refresh_snapshot
        ));
        assert_eq!(snapshot_tray_title("blocked", true), "SF market closed");
        assert_eq!(
            snapshot_decision_label_with_refresh(&health, &refresh_snapshot),
            "Market closed"
        );
        assert_eq!(
            signal_refresh_snapshot_label(&refresh_snapshot),
            "market closed"
        );
        assert!(
            snapshot_tooltip(Some(&health), true, false, &refresh_snapshot)
                .contains("Market/session closed")
        );

        let stale_refresh_snapshot = SignalRefreshSnapshot::Parsed(serde_json::json!({
            "status": "skipped_market_closed",
            "run_to": "2026-06-29",
            "reason": "after configured options-market refresh window"
        }));
        assert!(!execution_blocked_by_market_session(
            &health,
            &stale_refresh_snapshot
        ));
    }

    #[test]
    fn execution_worker_aggregate_status_treats_management_holding_as_healthy() {
        let mut decision = tradier_management_test_decision(ExecutionMode::Live);
        decision.status = "holding".to_owned();
        decision.reason = "no exit rule fired".to_owned();

        assert_eq!(
            execution_worker_aggregate_status(Some(&decision), None, None),
            "live"
        );

        decision.mode = ExecutionMode::Review;
        assert_eq!(
            execution_worker_aggregate_status(Some(&decision), None, None),
            "review"
        );
    }

    #[test]
    fn portfolio_canary_runner_allows_wheel_after_risk_and_broker_gates() {
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"new_entry",
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

        let decision = compute_execution_decision(
            &artifact,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &test_canary_risk(),
            &broker,
            ExecutionMode::Monitor,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert_eq!(decision.status, "ready");
        assert_eq!(
            decision
                .selected_signal
                .as_ref()
                .and_then(|action| action.reserve),
            Some(9_500.0)
        );
    }

    #[test]
    fn execution_worker_health_reports_monitor_without_action() {
        let path = unique_main_test_path("execution-worker-no-action.json");
        let artifact = test_live_signal_artifact_as_of(
            Some(NaiveDate::from_ymd_opt(2026, 6, 28).unwrap()),
            serde_json::json!([{
                "status":"recent_closed",
                "symbol":"TSLA",
                "strategy":"put_debit_spread",
                "entry_date":"2026-06-25",
                "exit_date":"2026-06-26",
                "max_loss":100.0
            }]),
        );
        fs::write(&path, serde_json::to_string(&artifact).unwrap()).unwrap();
        let args = ExecutionWorkerArgs {
            live_signal: path.clone(),
            as_of: Some(NaiveDate::from_ymd_opt(2026, 6, 28).unwrap()),
            risk: test_canary_risk(),
            broker: execution_broker(BrokerKind::Robinhood, false, false, false, false),
            mode: ExecutionMode::Monitor,
            robinhood_mcp_command: None,
            order_ledger: unique_main_test_path("canary-order-ledger.json"),
            notify_command: None,
            notify_ledger: unique_main_test_path("canary-notify-ledger.json"),
            max_order_age_seconds: DEFAULT_MAX_ORDER_AGE_SECONDS,
            poll_seconds: 60,
            once: true,
            health_output: None,
            json: true,
        };

        let health = execution_worker_health(&args);

        assert_eq!(health.status, "monitor");
        assert_eq!(
            health
                .decision
                .as_ref()
                .map(|decision| decision.status.as_str()),
            Some("no_signal")
        );
        assert_eq!(snapshot_decision_label(&health), "No signal");
        assert!(snapshot_action_rows(Some(&health)).is_empty());
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn execution_worker_health_reports_review_before_broker_review_succeeds() {
        let path = unique_main_test_path("execution-worker-review-required.json");
        let artifact = test_live_signal_artifact_as_of(
            Some(NaiveDate::from_ymd_opt(2026, 6, 28).unwrap()),
            serde_json::json!([{
                "status":"new_entry",
                "symbol":"ORCL",
                "strategy":"call_debit_spread",
                "entry_date":"2026-06-28",
                "exit_date":"2026-06-28",
                "expiration":"2026-07-02",
                "short_strike":225.0,
                "long_strike":220.0,
                "entry_credit":-4.50,
                "max_loss":450.0
            }]),
        );
        fs::write(&path, serde_json::to_string(&artifact).unwrap()).unwrap();
        let args = ExecutionWorkerArgs {
            live_signal: path.clone(),
            as_of: Some(NaiveDate::from_ymd_opt(2026, 6, 28).unwrap()),
            risk: test_canary_risk(),
            broker: execution_broker(BrokerKind::Robinhood, true, false, false, false),
            mode: ExecutionMode::Review,
            robinhood_mcp_command: None,
            order_ledger: unique_main_test_path("canary-order-ledger-review-required.json"),
            notify_command: None,
            notify_ledger: unique_main_test_path("canary-notify-ledger-review-required.json"),
            max_order_age_seconds: DEFAULT_MAX_ORDER_AGE_SECONDS,
            poll_seconds: 60,
            once: true,
            health_output: None,
            json: true,
        };

        let health = execution_worker_health(&args);

        assert_eq!(health.status, "review");
        assert_eq!(
            health
                .decision
                .as_ref()
                .map(|decision| decision.status.as_str()),
            Some("ready")
        );
        assert_eq!(snapshot_decision_label(&health), "Ready");
        assert!(
            snapshot_action_rows(Some(&health))
                .iter()
                .any(|row| row.label == "Signal" && row.value == "ORCL call_debit_spread")
        );
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn execution_notification_sends_once_for_actionable_signal() {
        let path = unique_main_test_path("execution-worker-notify.json");
        let artifact = test_live_signal_artifact_as_of(
            Some(NaiveDate::from_ymd_opt(2026, 6, 28).unwrap()),
            serde_json::json!([{
                "status":"new_entry",
                "symbol":"ORCL",
                "strategy":"call_debit_spread",
                "entry_date":"2026-06-28",
                "exit_date":"2026-06-28",
                "expiration":"2026-07-02",
                "short_strike":225.0,
                "long_strike":220.0,
                "entry_credit":-4.50,
                "max_loss":450.0
            }]),
        );
        fs::write(&path, serde_json::to_string(&artifact).unwrap()).unwrap();
        let payload_path = unique_main_test_path("canary-notify-payload.json");
        let ledger_path = unique_main_test_path("canary-notify-ledger-once.json");
        let args = ExecutionWorkerArgs {
            live_signal: path.clone(),
            as_of: Some(NaiveDate::from_ymd_opt(2026, 6, 28).unwrap()),
            risk: test_canary_risk(),
            broker: execution_broker(BrokerKind::Robinhood, true, false, false, false),
            mode: ExecutionMode::Monitor,
            robinhood_mcp_command: None,
            order_ledger: unique_main_test_path("canary-order-ledger-notify.json"),
            notify_command: Some(format!("cat > {}", payload_path.display())),
            notify_ledger: ledger_path.clone(),
            max_order_age_seconds: DEFAULT_MAX_ORDER_AGE_SECONDS,
            poll_seconds: 60,
            once: true,
            health_output: None,
            json: true,
        };
        let health = execution_worker_health(&args);

        maybe_notify_execution_decision(&health, &args).unwrap();
        let payload: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&payload_path).unwrap()).unwrap();
        assert_eq!(payload["status"], "ready");
        assert_eq!(payload["action"]["symbol"], "ORCL");
        assert_eq!(read_execution_notify_ledger(&ledger_path).unwrap().len(), 1);

        let mut skip_args = args;
        skip_args.notify_command = Some("exit 9".to_owned());
        maybe_notify_execution_decision(&health, &skip_args).unwrap();

        fs::remove_file(path).unwrap();
        fs::remove_file(payload_path).unwrap();
        fs::remove_file(ledger_path).unwrap();
    }

    #[test]
    fn execution_readiness_reports_current_static_blockers() {
        let artifact = test_live_signal_artifact_as_of(
            Some(NaiveDate::from_ymd_opt(2026, 6, 28).unwrap()),
            serde_json::json!([
                {"status":"recent_closed","symbol":"TSLA","strategy":"put_debit_spread","entry_date":"2026-06-25","exit_date":"2026-06-26","entry_credit":-1.0,"max_loss":100.0}
            ]),
        );
        let report = build_execution_readiness_report(
            Path::new("candidates/weekly_selector_canary.json"),
            true,
            true,
            Some(&artifact),
            None,
            NaiveDate::from_ymd_opt(2026, 6, 28).unwrap(),
            &test_canary_risk(),
            &RobinhoodBrokerAdapter::default(),
            false,
            false,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        assert!(!report.live_worker_ready_to_attempt_order);
        assert!(
            report
                .blockers
                .iter()
                .any(|blocker| blocker.contains("SPREAD_ROBINHOOD_MCP_COMMAND not configured"))
        );
        assert!(
            report
                .blockers
                .iter()
                .any(|blocker| blocker.contains("no selected live entry"))
        );
        assert!(
            report
                .blockers
                .iter()
                .all(|blocker| !blocker.contains("multi-leg options capability"))
        );
        assert!(
            report
                .blockers
                .iter()
                .all(|blocker| !blocker.contains("cash-secured put capability"))
        );
    }

    #[test]
    fn execution_readiness_fails_closed_without_allow_blocked() {
        let path = unique_main_test_path("canary-readiness-blocked.json");
        fs::write(
            &path,
            serde_json::to_string(&test_canary_artifact(serde_json::json!([{
                "status":"recent_closed",
                "symbol":"TSLA",
                "strategy":"put_debit_spread",
                "entry_date":"2026-06-25",
                "exit_date":"2026-06-26",
                "entry_credit":-1.0,
                "max_loss":100.0
            }])))
            .unwrap(),
        )
        .unwrap();

        let err = execution_readiness(
            &path,
            Some(NaiveDate::from_ymd_opt(2026, 6, 28).unwrap()),
            45_000.0,
            1_000.0,
            35_000.0,
            11_250.0,
            1,
            BrokerKind::Robinhood,
            true,
            false,
            false,
            Some("bridge".to_owned()),
            DEFAULT_MAX_ORDER_AGE_SECONDS,
            false,
            true,
        )
        .unwrap_err();

        assert!(err.to_string().contains("execution readiness blocked"));
        execution_readiness(
            &path,
            Some(NaiveDate::from_ymd_opt(2026, 6, 28).unwrap()),
            45_000.0,
            1_000.0,
            35_000.0,
            11_250.0,
            1,
            BrokerKind::Robinhood,
            true,
            false,
            false,
            Some("bridge".to_owned()),
            DEFAULT_MAX_ORDER_AGE_SECONDS,
            true,
            true,
        )
        .unwrap();
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn execution_readiness_blocks_fresh_debit_live_attempt_until_lifecycle_gate() {
        let today = execution_default_as_of(Utc::now());
        let today_s = today.to_string();
        let mut artifact = test_canary_artifact(serde_json::json!([
            {
                "status":"new_entry",
                "symbol":"TSLA",
                "strategy":"put_debit_spread",
                "entry_date":today_s,
                "exit_date":today_s,
                "expiration":"2026-07-02",
                "short_strike":350.0,
                "long_strike":355.0,
                "entry_credit":-1.00,
                "max_loss":100.0
            },
            {
                "status":"recent_closed",
                "symbol":"CRWV",
                "strategy":"wheel",
                "entry_date":"2026-06-26",
                "exit_date":"2026-06-26",
                "short_strike":80.0,
                "entry_credit":1.12,
                "max_loss":7888.0
            }
        ]));
        artifact.generated_at = Utc::now() - chrono::Duration::minutes(1);
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

        let report = build_execution_readiness_report(
            Path::new("candidates/weekly_selector_canary.json"),
            true,
            true,
            Some(&artifact),
            None,
            today,
            &test_canary_risk(),
            &broker,
            true,
            false,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        if execution_market_window_open_at(Utc::now()) {
            assert!(!report.ready_for_broker_review);
            assert!(!report.live_worker_ready_to_attempt_order);
            assert!(
                report
                    .blockers
                    .iter()
                    .any(|blocker| blocker.contains("position reconciliation"))
            );
            assert_eq!(
                report
                    .decision
                    .as_ref()
                    .map(|decision| decision.status.as_str()),
                Some("ready")
            );
        } else {
            assert!(!report.live_worker_ready_to_attempt_order);
            assert!(
                report
                    .blockers
                    .iter()
                    .any(|blocker| blocker.contains("regular options-market window"))
            );
        }
    }

    #[test]
    fn execution_readiness_allows_risk_controlled_tradier_live_artifact_after_lifecycle_gate() {
        let today = execution_default_as_of(Utc::now());
        let today_s = today.to_string();
        let mut artifact = test_canary_artifact(serde_json::json!([{
                "status":"new_entry",
                "symbol":"TSLA",
                "strategy":"put_debit_spread",
                "entry_date":today_s,
                "exit_date":today_s,
                "expiration":"2026-07-02",
                "short_strike":350.0,
                "long_strike":355.0,
                "entry_credit":-1.00,
                "max_loss":100.0
        }]));
        artifact.generated_at = Utc::now() - chrono::Duration::minutes(1);
        let broker = execution_broker(BrokerKind::Tradier, false, false, false, true);

        let report = build_execution_readiness_report(
            Path::new("candidates/weekly_selector_canary.json"),
            true,
            true,
            Some(&artifact),
            None,
            today,
            &test_canary_risk(),
            &broker,
            false,
            true,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );

        if execution_market_window_open_at(Utc::now()) {
            assert!(report.ready_for_broker_review);
            assert!(report.live_worker_ready_to_attempt_order);
            assert!(report.blockers.is_empty());
            assert_eq!(
                report
                    .decision
                    .as_ref()
                    .map(|decision| decision.status.as_str()),
                Some("ready")
            );
        } else {
            assert!(!report.live_worker_ready_to_attempt_order);
            assert!(
                report
                    .blockers
                    .iter()
                    .any(|blocker| blocker.contains("regular options-market window"))
            );
        }
    }

    #[test]
    fn execution_readiness_blocks_future_exported_at() {
        let today = execution_default_as_of(Utc::now());
        let today_s = today.to_string();
        let mut artifact = test_canary_artifact(serde_json::json!([{
            "status":"new_entry",
            "symbol":"TSLA",
            "strategy":"put_debit_spread",
            "entry_date":today_s,
            "exit_date":today_s,
            "expiration":"2026-07-02",
            "short_strike":350.0,
            "long_strike":355.0,
            "entry_credit":-1.00,
            "max_loss":100.0
        }]));
        artifact.generated_at = Utc::now() + chrono::Duration::minutes(5);
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

        let report = build_execution_readiness_report(
            Path::new("candidates/weekly_selector_canary.json"),
            true,
            true,
            Some(&artifact),
            None,
            today,
            &test_canary_risk(),
            &broker,
            true,
            false,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );
        let live_err = live_signal_fresh_enough_for_live_order(
            &artifact,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
            Utc::now(),
        )
        .unwrap_err();

        assert!(!report.live_worker_ready_to_attempt_order);
        assert!(format!("{live_err:#}").contains("in the future"));
    }

    #[test]
    fn market_session_blocks_observed_independence_day() {
        let observed_holiday = NaiveDate::from_ymd_opt(2026, 7, 3)
            .unwrap()
            .and_hms_opt(15, 0, 0)
            .unwrap()
            .and_utc();

        let snapshot = market_session_snapshot_at(observed_holiday);

        assert!(!snapshot.open);
        assert!(snapshot.reason.contains("Independence Day"));
    }

    #[test]
    fn market_session_honors_black_friday_early_close() {
        let open_before_early_close = NaiveDate::from_ymd_opt(2026, 11, 27)
            .unwrap()
            .and_hms_opt(17, 0, 0)
            .unwrap()
            .and_utc();
        let after_early_close = NaiveDate::from_ymd_opt(2026, 11, 27)
            .unwrap()
            .and_hms_opt(19, 0, 0)
            .unwrap()
            .and_utc();

        assert!(market_session_snapshot_at(open_before_early_close).open);
        assert!(!market_session_snapshot_at(after_early_close).open);
    }

    #[test]
    fn market_session_snapshot_can_use_tradier_clock() {
        let clock = TradierMarketClockResponse {
            ok: true,
            raw: serde_json::json!({}),
            clock: Some(TradierMarketClock {
                state: Some("open".to_owned()),
                status: None,
                description: Some("Market is open".to_owned()),
                next_state: None,
                next_change: None,
                timestamp: None,
            }),
            error: None,
        };

        let snapshot = tradier_market_session_snapshot_from_clock(
            test_market_open_utc(execution_default_as_of(Utc::now())),
            &clock,
        )
        .unwrap();

        assert!(snapshot.open);
        assert_eq!(snapshot.source, "tradier_clock");
        assert_eq!(snapshot.reason, "Market is open");
    }

    #[test]
    fn live_decision_uses_market_date_after_utc_rollover() {
        let market_date = NaiveDate::from_ymd_opt(2026, 6, 29).unwrap();
        let after_close_utc = NaiveDate::from_ymd_opt(2026, 6, 30)
            .unwrap()
            .and_hms_opt(2, 30, 0)
            .unwrap()
            .and_utc();
        let artifact = test_canary_artifact(serde_json::json!([{
                "status":"new_entry",
                "symbol":"TSLA",
                "strategy":"put_debit_spread",
                "entry_date":market_date.to_string(),
                "exit_date":market_date.to_string(),
                "expiration":"2026-07-02",
                "short_strike":350.0,
                "long_strike":355.0,
                "entry_credit":-1.00,
                "max_loss":100.0
        }]));
        let broker = execution_broker(BrokerKind::Tradier, true, false, false, true);

        let decision = compute_execution_decision_at(
            &artifact,
            execution_default_as_of(after_close_utc),
            &test_canary_risk(),
            &broker,
            ExecutionMode::Live,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
            after_close_utc,
        );

        assert_eq!(execution_default_as_of(after_close_utc), market_date);
        assert_eq!(decision.status, "blocked");
        assert!(decision.reason.contains("regular options-market window"));
        assert!(!decision.reason.contains("--as-of"));
    }

    #[test]
    fn live_decision_reports_market_closed_before_stale_date_after_eastern_midnight() {
        let artifact_date = NaiveDate::from_ymd_opt(2026, 6, 29).unwrap();
        let next_market_date = NaiveDate::from_ymd_opt(2026, 6, 30).unwrap();
        let after_eastern_midnight_utc = next_market_date.and_hms_opt(4, 30, 0).unwrap().and_utc();
        let artifact = test_canary_artifact(serde_json::json!([{
                "status":"new_entry",
                "symbol":"TSLA",
                "strategy":"put_debit_spread",
                "entry_date":artifact_date.to_string(),
                "exit_date":artifact_date.to_string(),
                "expiration":"2026-07-02",
                "short_strike":350.0,
                "long_strike":355.0,
                "entry_credit":-1.00,
                "max_loss":100.0
        }]));
        let broker = execution_broker(BrokerKind::Tradier, true, false, false, true);

        let decision = compute_execution_decision_at(
            &artifact,
            execution_default_as_of(after_eastern_midnight_utc),
            &test_canary_risk(),
            &broker,
            ExecutionMode::Live,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
            after_eastern_midnight_utc,
        );

        assert_eq!(
            execution_default_as_of(after_eastern_midnight_utc),
            next_market_date
        );
        assert_eq!(decision.status, "blocked");
        assert!(decision.reason.contains("regular options-market window"));
        assert!(!decision.reason.contains("does not match requested as_of"));
    }

    fn test_canary_risk() -> CanaryRiskPolicy {
        CanaryRiskPolicy {
            account_cash: 45_000.0,
            debit_max_loss: 1_000.0,
            wheel_reserve_cap: 35_000.0,
            free_cash_buffer: 11_250.0,
            max_wheel_positions_per_symbol: 1,
        }
    }

    fn test_market_open_utc(date: NaiveDate) -> chrono::DateTime<Utc> {
        date.and_hms_opt(15, 0, 0)
            .expect("valid test market-open time")
            .and_utc()
    }

    fn test_canary_artifact(latest_actions: serde_json::Value) -> LiveSignalArtifact {
        test_live_signal_artifact_as_of(None, latest_actions)
    }

    fn test_live_signal_artifact_as_of(
        as_of: Option<NaiveDate>,
        latest_actions: serde_json::Value,
    ) -> LiveSignalArtifact {
        let signals = latest_actions
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|action| test_trade_signal_summary(action, None))
            .collect::<Vec<_>>();
        let selected_signal = signals
            .iter()
            .find(|signal| signal.status == SignalStatus::NewEntry)
            .cloned();
        let as_of = as_of
            .or_else(|| {
                selected_signal
                    .as_ref()
                    .and_then(|signal| signal.entry_date.as_deref())
                    .and_then(|date| NaiveDate::parse_from_str(date, "%Y-%m-%d").ok())
            })
            .or_else(|| {
                signals
                    .first()
                    .and_then(|signal| signal.entry_date.as_deref())
                    .and_then(|date| NaiveDate::parse_from_str(date, "%Y-%m-%d").ok())
            })
            .unwrap_or_else(|| Utc::now().date_naive());
        let approved_strategy = ApprovedStrategy {
            strategy_id: "test_strategy".to_owned(),
            profile_name: "test_profile".to_owned(),
            research_from: None,
            live_detector_lookback_days: None,
            symbols: vec![
                "CRWV".to_owned(),
                "ORCL".to_owned(),
                "TSLA".to_owned(),
                "PLTR".to_owned(),
            ],
            portfolio_constraints: spreadfoundry::live_signal::ApprovedPortfolioConstraints {
                capital_budget: 100_000.0,
                max_symbol_allocation_pct: 0.35,
                max_open_positions: 5,
                max_positions_per_symbol: 2,
                max_total_trades_per_symbol: None,
                portfolio_drawdown_cooldown_trigger_pct: None,
                portfolio_drawdown_cooldown_days: 0,
                symbol_drawdown_cooldown_trigger_pct: None,
                symbol_drawdown_cooldown_days: 0,
            },
            allowed_live_strategies: vec![
                "put_debit_spread".to_owned(),
                "call_debit_spread".to_owned(),
                "wheel".to_owned(),
            ],
            canary_risk_policy_id: "test_canary_risk".to_owned(),
            production_approval: Some(ProductionApproval {
                status: ProductionApprovalStatus::CanaryApproved,
                approved_at: test_market_open_utc(as_of),
                approved_by: "test_operator".to_owned(),
                reason: "test fixture approval for execution contract coverage".to_owned(),
                source_canary_status: Some("canary_only".to_owned()),
                max_order_max_loss: Some(10_000.0),
            }),
        };
        LiveSignalArtifact {
            schema_version: LIVE_SIGNAL_SCHEMA_VERSION,
            strategy_id: approved_strategy.strategy_id.clone(),
            profile_name: approved_strategy.profile_name.clone(),
            as_of,
            generated_at: test_market_open_utc(as_of),
            market_data_through: as_of,
            approved_strategy,
            signals,
            selected_signal,
            source_run_id: "test_run".to_owned(),
            source_report: "test_report".to_owned(),
        }
    }

    fn test_trade_signal_summary(
        action: &serde_json::Value,
        action_risk: Option<TradeSignalRisk>,
    ) -> TradeSignal {
        let strategy = action
            .get("strategy")
            .and_then(|value| value.as_str())
            .unwrap_or("unknown")
            .to_owned();
        let short_put = action
            .get("short_put")
            .and_then(|value| value.as_f64())
            .or_else(|| {
                if strategy == "wheel" {
                    action.get("short_strike").and_then(|value| value.as_f64())
                } else {
                    None
                }
            });
        TradeSignal {
            status: match action
                .get("status")
                .and_then(|value| value.as_str())
                .unwrap_or("recent_closed")
            {
                "new_entry" => SignalStatus::NewEntry,
                "already_open" => SignalStatus::AlreadyOpen,
                _ => SignalStatus::RecentClosed,
            },
            symbol: action
                .get("symbol")
                .and_then(|value| value.as_str())
                .unwrap_or("UNKNOWN")
                .to_owned(),
            strategy,
            entry_date: action
                .get("entry_date")
                .and_then(|value| value.as_str())
                .map(ToOwned::to_owned),
            exit_date: action
                .get("exit_date")
                .and_then(|value| value.as_str())
                .map(ToOwned::to_owned),
            expiration: action
                .get("expiration")
                .and_then(|value| value.as_str())
                .map(ToOwned::to_owned),
            short_put,
            short_strike: action
                .get("short_strike")
                .and_then(|value| value.as_f64())
                .or(short_put),
            long_strike: action.get("long_strike").and_then(|value| value.as_f64()),
            wheel_covered_call_expiration: action
                .get("wheel_covered_call_expiration")
                .and_then(|value| value.as_str())
                .map(ToOwned::to_owned),
            wheel_covered_call_strike: action
                .get("wheel_covered_call_strike")
                .and_then(|value| value.as_f64()),
            width: action.get("width").and_then(|value| value.as_f64()),
            entry_credit: action.get("entry_credit").and_then(|value| value.as_f64()),
            max_loss: action.get("max_loss").and_then(|value| value.as_f64()),
            reserve: action_risk.as_ref().map(|risk| risk.reserve),
            reserve_basis: action_risk.map(|risk| risk.reserve_basis),
            pnl: action.get("pnl").and_then(|value| value.as_f64()),
            dte_entry: None,
            days_held: None,
            exit_reason: action
                .get("exit_reason")
                .and_then(|value| value.as_str())
                .map(ToOwned::to_owned),
            short_delta: None,
            long_delta: None,
            short_oi: None,
            long_oi: None,
            short_iv: None,
            long_iv: None,
            underlying_price: None,
            execution_rules: Some(test_live_execution_rules()),
        }
    }

    fn tradier_call_debit_action(status: &str) -> TradeSignal {
        test_trade_signal_summary(
            &serde_json::json!({
                "status": status,
                "symbol":"ORCL",
                "strategy":"call_debit_spread",
                "entry_date":"2026-06-29",
                "exit_date":"2026-07-01",
                "expiration":"2026-07-02",
                "short_strike":225.0,
                "long_strike":220.0,
                "entry_credit":-4.50,
                "max_loss":450.0
            }),
            None,
        )
    }

    fn tradier_put_debit_action(status: &str) -> TradeSignal {
        test_trade_signal_summary(
            &serde_json::json!({
                "status": status,
                "symbol":"TSLA",
                "strategy":"put_debit_spread",
                "entry_date":"2026-06-29",
                "exit_date":"2026-07-01",
                "expiration":"2026-07-02",
                "short_strike":350.0,
                "long_strike":355.0,
                "entry_credit":-1.00,
                "max_loss":100.0
            }),
            None,
        )
    }

    fn tradier_call_credit_action(status: &str) -> TradeSignal {
        test_trade_signal_summary(
            &serde_json::json!({
                "status": status,
                "symbol":"ORCL",
                "strategy":"call_credit_spread",
                "entry_date":"2026-06-29",
                "exit_date":"2026-07-01",
                "expiration":"2026-07-02",
                "short_strike":225.0,
                "long_strike":230.0,
                "width":5.0,
                "entry_credit":1.20,
                "max_loss":380.0
            }),
            None,
        )
    }

    fn tradier_put_credit_action(status: &str) -> TradeSignal {
        test_trade_signal_summary(
            &serde_json::json!({
                "status": status,
                "symbol":"TSLA",
                "strategy":"put_credit_spread",
                "entry_date":"2026-06-29",
                "exit_date":"2026-07-01",
                "expiration":"2026-07-02",
                "short_strike":350.0,
                "long_strike":345.0,
                "width":5.0,
                "entry_credit":1.00,
                "max_loss":400.0
            }),
            None,
        )
    }

    fn tradier_test_position(symbol: &str, quantity: f64) -> TradierPosition {
        TradierPosition {
            symbol: symbol.to_owned(),
            quantity,
            cost_basis: None,
        }
    }

    fn tradier_test_order(
        id: Option<&str>,
        symbol: Option<&str>,
        option_symbol: Option<&str>,
        status: Option<&str>,
        quantity: Option<f64>,
    ) -> TradierOrder {
        TradierOrder {
            id: id.map(ToOwned::to_owned),
            symbol: symbol.map(ToOwned::to_owned),
            option_symbol: option_symbol.map(ToOwned::to_owned),
            status: status.map(ToOwned::to_owned),
            side: None,
            quantity,
        }
    }

    fn tradier_test_quote(symbol: &str, bid: Option<f64>, ask: Option<f64>) -> TradierQuote {
        let now_ms = Utc::now().timestamp_millis();
        TradierQuote {
            symbol: symbol.to_owned(),
            bid,
            ask,
            last: None,
            bid_size: Some(10.0),
            ask_size: Some(10.0),
            bid_date: Some(now_ms),
            ask_date: Some(now_ms),
            trade_date: Some(now_ms),
        }
    }

    fn tradier_test_decision(mode: ExecutionMode) -> ExecutionDecision {
        let as_of = if mode == ExecutionMode::Live {
            execution_default_as_of(Utc::now())
        } else {
            NaiveDate::from_ymd_opt(2026, 6, 29).unwrap()
        };
        let as_of_s = as_of.to_string();
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"new_entry",
            "symbol":"ORCL",
            "strategy":"call_debit_spread",
            "entry_date":as_of_s,
            "exit_date":as_of_s,
            "expiration":"2026-07-02",
            "short_strike":225.0,
            "long_strike":220.0,
            "entry_credit":-4.50,
            "max_loss":450.0
        }]));
        let broker = execution_broker(
            BrokerKind::Tradier,
            false,
            false,
            false,
            mode == ExecutionMode::Live,
        );
        let decision_mode = if mode == ExecutionMode::Live {
            ExecutionMode::Review
        } else {
            mode
        };
        let mut decision = compute_execution_decision(
            &artifact,
            as_of,
            &test_canary_risk(),
            &broker,
            decision_mode,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );
        decision.mode = mode;
        decision
    }

    fn tradier_management_test_decision(mode: ExecutionMode) -> ExecutionDecision {
        let as_of = if mode == ExecutionMode::Live {
            execution_default_as_of(Utc::now())
        } else {
            NaiveDate::from_ymd_opt(2026, 6, 29).unwrap()
        };
        let as_of_s = as_of.to_string();
        let artifact = test_live_signal_artifact_as_of(
            Some(as_of),
            serde_json::json!([{
                "status":"already_open",
                "symbol":"ORCL",
                "strategy":"call_debit_spread",
                "entry_date":as_of_s,
                "exit_date":"2026-07-01",
                "expiration":"2026-07-02",
                "short_strike":225.0,
                "long_strike":220.0,
                "width":5.0,
                "entry_credit":-4.50,
                "max_loss":450.0
            }]),
        );
        let broker = execution_broker(
            BrokerKind::Tradier,
            false,
            false,
            false,
            mode == ExecutionMode::Live,
        );
        let decision_mode = if mode == ExecutionMode::Live {
            ExecutionMode::Review
        } else {
            mode
        };
        let mut decision = compute_execution_decision(
            &artifact,
            as_of,
            &test_canary_risk(),
            &broker,
            decision_mode,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );
        decision.mode = mode;
        decision
    }

    fn tradier_credit_management_test_decision(mode: ExecutionMode) -> ExecutionDecision {
        let as_of = if mode == ExecutionMode::Live {
            execution_default_as_of(Utc::now())
        } else {
            NaiveDate::from_ymd_opt(2026, 6, 29).unwrap()
        };
        let as_of_s = as_of.to_string();
        let mut artifact = test_live_signal_artifact_as_of(
            Some(as_of),
            serde_json::json!([{
                "status":"already_open",
                "symbol":"ORCL",
                "strategy":"call_credit_spread",
                "entry_date":as_of_s,
                "exit_date":"2026-07-01",
                "expiration":"2026-07-02",
                "short_strike":225.0,
                "long_strike":230.0,
                "width":5.0,
                "entry_credit":1.20,
                "max_loss":380.0
            }]),
        );
        artifact
            .approved_strategy
            .allowed_live_strategies
            .push("call_credit_spread".to_owned());
        let broker = execution_broker(
            BrokerKind::Tradier,
            false,
            false,
            false,
            mode == ExecutionMode::Live,
        );
        let decision_mode = if mode == ExecutionMode::Live {
            ExecutionMode::Review
        } else {
            mode
        };
        let mut decision = compute_execution_decision(
            &artifact,
            as_of,
            &test_canary_risk(),
            &broker,
            decision_mode,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );
        decision.mode = mode;
        decision
    }

    fn tradier_put_credit_management_test_decision(mode: ExecutionMode) -> ExecutionDecision {
        let as_of = if mode == ExecutionMode::Live {
            execution_default_as_of(Utc::now())
        } else {
            NaiveDate::from_ymd_opt(2026, 6, 29).unwrap()
        };
        let as_of_s = as_of.to_string();
        let mut artifact = test_live_signal_artifact_as_of(
            Some(as_of),
            serde_json::json!([{
                "status":"already_open",
                "symbol":"TSLA",
                "strategy":"put_credit_spread",
                "entry_date":as_of_s,
                "exit_date":"2026-07-01",
                "expiration":"2026-07-02",
                "short_strike":350.0,
                "long_strike":345.0,
                "width":5.0,
                "entry_credit":1.00,
                "max_loss":400.0
            }]),
        );
        artifact
            .approved_strategy
            .allowed_live_strategies
            .push("put_credit_spread".to_owned());
        let broker = execution_broker(
            BrokerKind::Tradier,
            false,
            false,
            false,
            mode == ExecutionMode::Live,
        );
        let decision_mode = if mode == ExecutionMode::Live {
            ExecutionMode::Review
        } else {
            mode
        };
        let mut decision = compute_execution_decision(
            &artifact,
            as_of,
            &test_canary_risk(),
            &broker,
            decision_mode,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );
        decision.mode = mode;
        decision
    }

    fn tradier_wheel_test_decision(mode: ExecutionMode) -> ExecutionDecision {
        let as_of = if mode == ExecutionMode::Live {
            execution_default_as_of(Utc::now())
        } else {
            NaiveDate::from_ymd_opt(2026, 6, 29).unwrap()
        };
        let as_of_s = as_of.to_string();
        let artifact = test_canary_artifact(serde_json::json!([{
            "status":"new_entry",
            "symbol":"CRWV",
            "strategy":"wheel",
            "entry_date":as_of_s,
            "exit_date":as_of_s,
            "expiration":"2026-07-02",
            "short_strike":80.0,
            "entry_credit":1.12,
            "max_loss":7888.0
        }]));
        let broker = execution_broker(
            BrokerKind::Tradier,
            false,
            false,
            false,
            mode == ExecutionMode::Live,
        );
        compute_execution_decision(
            &artifact,
            as_of,
            &test_canary_risk(),
            &broker,
            mode,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        )
    }

    fn tradier_wheel_management_test_decision(mode: ExecutionMode) -> ExecutionDecision {
        let as_of = if mode == ExecutionMode::Live {
            execution_default_as_of(Utc::now())
        } else {
            NaiveDate::from_ymd_opt(2026, 6, 29).unwrap()
        };
        let as_of_s = as_of.to_string();
        let put_expiration = (as_of + chrono::Duration::days(3)).to_string();
        let call_expiration = (as_of + chrono::Duration::days(7)).to_string();
        let artifact = test_live_signal_artifact_as_of(
            Some(as_of),
            serde_json::json!([{
                "status":"already_open",
                "symbol":"CRWV",
                "strategy":"wheel",
                "entry_date":as_of_s,
                "exit_date":call_expiration.clone(),
                "expiration":put_expiration,
                "short_strike":80.0,
                "short_put":80.0,
                "long_strike":85.0,
                "wheel_covered_call_expiration":call_expiration,
                "wheel_covered_call_strike":85.0,
                "width":1.0,
                "entry_credit":1.12,
                "max_loss":7888.0
            }]),
        );
        let broker = execution_broker(
            BrokerKind::Tradier,
            false,
            false,
            false,
            mode == ExecutionMode::Live,
        );
        let decision_mode = if mode == ExecutionMode::Live {
            ExecutionMode::Review
        } else {
            mode
        };
        let mut decision = compute_execution_decision(
            &artifact,
            as_of,
            &test_canary_risk(),
            &broker,
            decision_mode,
            DEFAULT_MAX_ORDER_AGE_SECONDS,
        );
        decision.mode = mode;
        decision
    }

    fn test_tradier_config(base_url: String) -> TradierConfig {
        TradierConfig {
            account_id: "TEST123".to_owned(),
            token: "test-token".to_owned(),
            base_url,
        }
    }

    fn spawn_tradier_mock<S>(
        responses: Vec<(u16, S)>,
    ) -> (
        String,
        std::sync::mpsc::Receiver<String>,
        std::thread::JoinHandle<()>,
    )
    where
        S: Into<String> + Send + 'static,
    {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}/v1", listener.local_addr().unwrap());
        let (tx, rx) = std::sync::mpsc::channel();
        let handle = std::thread::spawn(move || {
            for (status, body) in responses {
                let body = body.into();
                let (mut stream, _) = listener.accept().unwrap();
                let request = read_http_request(&mut stream);
                tx.send(request).unwrap();
                let status_text = if status == 200 { "OK" } else { "Bad Request" };
                let response = format!(
                    "HTTP/1.1 {status} {status_text}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                std::io::Write::write_all(&mut stream, response.as_bytes()).unwrap();
            }
        });
        (base_url, rx, handle)
    }

    fn spawn_tradier_debit_mock<S>(
        responses: Vec<(u16, S)>,
    ) -> (
        String,
        std::sync::mpsc::Receiver<String>,
        std::thread::JoinHandle<()>,
    )
    where
        S: Into<String>,
    {
        let mut all = tradier_debit_base_responses(false);
        all.extend(
            responses
                .into_iter()
                .map(|(status, body)| (status, body.into())),
        );
        spawn_tradier_mock(all)
    }

    fn spawn_tradier_credit_mock<S>(
        responses: Vec<(u16, S)>,
    ) -> (
        String,
        std::sync::mpsc::Receiver<String>,
        std::thread::JoinHandle<()>,
    )
    where
        S: Into<String>,
    {
        let mut all = tradier_credit_base_responses(false);
        all.extend(
            responses
                .into_iter()
                .map(|(status, body)| (status, body.into())),
        );
        spawn_tradier_mock(all)
    }

    fn spawn_tradier_live_debit_mock<S>(
        responses: Vec<(u16, S)>,
    ) -> (
        String,
        std::sync::mpsc::Receiver<String>,
        std::thread::JoinHandle<()>,
    )
    where
        S: Into<String>,
    {
        let mut all = tradier_debit_base_responses(true);
        all.extend(
            responses
                .into_iter()
                .map(|(status, body)| (status, body.into())),
        );
        spawn_tradier_mock(all)
    }

    fn tradier_debit_base_responses(live: bool) -> Vec<(u16, String)> {
        let mut responses = Vec::new();
        if live {
            responses.push((
                200,
                r#"{"clock":{"state":"open","description":"Market is open"}}"#.to_owned(),
            ));
        }
        responses.push((200, tradier_good_balances_response()));
        responses.push((200, r#"{"positions":{"position":[]}}"#.to_owned()));
        responses.push((200, r#"{"orders":{"order":[]}}"#.to_owned()));
        responses.push((200, tradier_good_debit_quote_response()));
        responses
    }

    fn tradier_credit_base_responses(live: bool) -> Vec<(u16, String)> {
        let mut responses = Vec::new();
        if live {
            responses.push((
                200,
                r#"{"clock":{"state":"open","description":"Market is open"}}"#.to_owned(),
            ));
        }
        responses.push((200, tradier_good_balances_response()));
        responses.push((200, r#"{"positions":{"position":[]}}"#.to_owned()));
        responses.push((200, r#"{"orders":{"order":[]}}"#.to_owned()));
        responses.push((200, tradier_good_credit_quote_response()));
        responses
    }

    fn tradier_good_balances_response() -> String {
        r#"{"balances":{"account_number":"TEST123","option_buying_power":45000,"total_cash":45000}}"#.to_owned()
    }

    fn tradier_good_debit_quote_response() -> String {
        let now_ms = Utc::now().timestamp_millis();
        tradier_debit_quote_response(now_ms)
    }

    fn tradier_good_credit_quote_response() -> String {
        let now_ms = Utc::now().timestamp_millis();
        tradier_credit_quote_response(now_ms, 1.50, 0.30)
    }

    fn tradier_open_call_debit_positions_response() -> String {
        r#"{"positions":{"position":[{"symbol":"ORCL260702C00220000","quantity":1,"cost_basis":450},{"symbol":"ORCL260702C00225000","quantity":-1,"cost_basis":-10}]}}"#.to_owned()
    }

    fn tradier_open_call_credit_positions_response() -> String {
        r#"{"positions":{"position":[{"symbol":"ORCL260702C00230000","quantity":1,"cost_basis":30},{"symbol":"ORCL260702C00225000","quantity":-1,"cost_basis":-120}]}}"#.to_owned()
    }

    fn tradier_open_put_credit_positions_response() -> String {
        r#"{"positions":{"position":[{"symbol":"TSLA260702P00345000","quantity":1,"cost_basis":30},{"symbol":"TSLA260702P00350000","quantity":-1,"cost_basis":-100}]}}"#.to_owned()
    }

    fn tradier_assigned_call_debit_positions_response() -> String {
        r#"{"positions":{"position":[{"symbol":"ORCL260702C00220000","quantity":1,"cost_basis":450},{"symbol":"ORCL","quantity":-100,"cost_basis":-22500}]}}"#.to_owned()
    }

    fn tradier_assigned_call_credit_positions_response() -> String {
        r#"{"positions":{"position":[{"symbol":"ORCL260702C00230000","quantity":1,"cost_basis":30},{"symbol":"ORCL","quantity":-100,"cost_basis":-22500}]}}"#.to_owned()
    }

    fn tradier_assigned_put_credit_positions_response() -> String {
        r#"{"positions":{"position":[{"symbol":"TSLA260702P00345000","quantity":1,"cost_basis":30},{"symbol":"TSLA","quantity":100,"cost_basis":35000}]}}"#.to_owned()
    }

    fn tradier_debit_exit_quote_response(long_bid: f64, short_ask: f64) -> String {
        let now_ms = Utc::now().timestamp_millis();
        format!(
            r#"{{"quotes":{{"quote":[{{"symbol":"ORCL260702C00220000","bid":{long_bid},"ask":5.00,"bidsize":10,"asksize":10,"bid_date":{now_ms},"ask_date":{now_ms}}},{{"symbol":"ORCL260702C00225000","bid":0.01,"ask":{short_ask},"bidsize":10,"asksize":10,"bid_date":{now_ms},"ask_date":{now_ms}}}]}}}}"#
        )
    }

    fn tradier_credit_exit_quote_response(short_ask: f64, long_bid: f64) -> String {
        let now_ms = Utc::now().timestamp_millis();
        format!(
            r#"{{"quotes":{{"quote":[{{"symbol":"ORCL260702C00225000","bid":0.20,"ask":{short_ask},"bidsize":10,"asksize":10,"bid_date":{now_ms},"ask_date":{now_ms}}},{{"symbol":"ORCL260702C00230000","bid":{long_bid},"ask":0.30,"bidsize":10,"asksize":10,"bid_date":{now_ms},"ask_date":{now_ms}}}]}}}}"#
        )
    }

    fn tradier_put_credit_exit_quote_response(short_ask: f64, long_bid: f64) -> String {
        let now_ms = Utc::now().timestamp_millis();
        format!(
            r#"{{"quotes":{{"quote":[{{"symbol":"TSLA260702P00350000","bid":0.20,"ask":{short_ask},"bidsize":10,"asksize":10,"bid_date":{now_ms},"ask_date":{now_ms}}},{{"symbol":"TSLA260702P00345000","bid":{long_bid},"ask":0.30,"bidsize":10,"asksize":10,"bid_date":{now_ms},"ask_date":{now_ms}}}]}}}}"#
        )
    }

    fn tradier_open_wheel_short_put_positions_response() -> String {
        r#"{"positions":{"position":[{"symbol":"CRWV260702P00080000","quantity":-1,"cost_basis":-112}]}}"#.to_owned()
    }

    fn tradier_assigned_wheel_stock_positions_response() -> String {
        r#"{"positions":{"position":[{"symbol":"CRWV","quantity":100,"cost_basis":8000}]}}"#
            .to_owned()
    }

    fn tradier_existing_wheel_covered_call_positions_response() -> String {
        r#"{"positions":{"position":[{"symbol":"CRWV","quantity":100,"cost_basis":8000},{"symbol":"CRWV260713C00090000","quantity":-1,"cost_basis":-75}]}}"#.to_owned()
    }

    fn tradier_multi_contract_wheel_covered_call_positions_response() -> String {
        r#"{"positions":{"position":[{"symbol":"CRWV","quantity":200,"cost_basis":16000},{"symbol":"CRWV260706C00085000","quantity":-2,"cost_basis":-130}]}}"#.to_owned()
    }

    fn tradier_single_option_quote_response(symbol: &str, bid: f64, ask: f64) -> String {
        let now_ms = Utc::now().timestamp_millis();
        format!(
            r#"{{"quotes":{{"quote":{{"symbol":"{symbol}","bid":{bid},"ask":{ask},"bidsize":10,"asksize":10,"bid_date":{now_ms},"ask_date":{now_ms}}}}}}}"#
        )
    }

    fn tradier_debit_quote_response(timestamp_ms: i64) -> String {
        format!(
            r#"{{"quotes":{{"quote":[{{"symbol":"ORCL260702C00220000","bid":4.90,"ask":5.00,"bidsize":10,"asksize":10,"bid_date":{timestamp_ms},"ask_date":{timestamp_ms}}},{{"symbol":"ORCL260702C00225000","bid":0.55,"ask":0.65,"bidsize":10,"asksize":10,"bid_date":{timestamp_ms},"ask_date":{timestamp_ms}}}]}}}}"#
        )
    }

    fn tradier_credit_quote_response(timestamp_ms: i64, short_bid: f64, long_ask: f64) -> String {
        format!(
            r#"{{"quotes":{{"quote":[{{"symbol":"ORCL260702C00225000","bid":{short_bid},"ask":1.70,"bidsize":10,"asksize":10,"bid_date":{timestamp_ms},"ask_date":{timestamp_ms}}},{{"symbol":"ORCL260702C00230000","bid":0.20,"ask":{long_ask},"bidsize":10,"asksize":10,"bid_date":{timestamp_ms},"ask_date":{timestamp_ms}}}]}}}}"#
        )
    }

    fn read_http_request(stream: &mut std::net::TcpStream) -> String {
        use std::io::Read;
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        let mut buffer = Vec::new();
        let mut chunk = [0_u8; 1024];
        loop {
            let read = stream.read(&mut chunk).unwrap();
            if read == 0 {
                break;
            }
            buffer.extend_from_slice(&chunk[..read]);
            if let Some(header_end) = find_header_end(&buffer) {
                let headers = String::from_utf8_lossy(&buffer[..header_end]).to_string();
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or(0);
                let body_start = header_end + 4;
                if buffer.len() >= body_start + content_length {
                    break;
                }
            }
        }
        let request = String::from_utf8(buffer).unwrap();
        request
            .split_once("\r\n\r\n")
            .map(|(_, body)| body.to_owned())
            .unwrap_or(request)
    }

    fn find_header_end(buffer: &[u8]) -> Option<usize> {
        buffer.windows(4).position(|window| window == b"\r\n\r\n")
    }

    fn collect_mock_requests(
        requests: std::sync::mpsc::Receiver<String>,
        handle: std::thread::JoinHandle<()>,
    ) -> Vec<String> {
        handle.join().unwrap();
        requests.try_iter().collect()
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

    fn test_live_execution_rules() -> LiveExecutionRules {
        LiveExecutionRules {
            take_profit_pct: 0.50,
            stop_loss_multiple: 2.0,
            force_close_dte: 21,
            max_hold_days: None,
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
