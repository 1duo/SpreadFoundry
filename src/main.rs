use anyhow::{Context, Result};
use chrono::{NaiveDate, Utc};
use clap::{Parser, ValueEnum};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use spreadfoundry::broker::RobinhoodBrokerAdapter;
use spreadfoundry::fixture;
use spreadfoundry::opt::{OptimizationResult, rank_results, score_trades};
use spreadfoundry::report::{read_report_markdown, write_run_report};
use spreadfoundry::research::{
    DEFAULT_PLATEAU_UNIVERSE_SYMBOLS, DEFAULT_PLATEAU_UNIVERSE_SYMBOLS_CSV, ResearchMetrics,
    ResearchReport, ResearchRequest, run_symbol_research,
};
use spreadfoundry::sim::{ExitRules, SpreadExitQuote, choose_exit};
use spreadfoundry::strategy::{CandidateFilters, generate_put_spread_candidates};
use spreadfoundry::theta::{ThetaClient, ThetaUniverseRequest};
use std::cmp::Ordering;
use std::fs;
use std::path::{Path, PathBuf};

const UNIVERSE_SELECTION_BASIS: &str = "Plateau expansion uses five non-NVDA single stocks chosen for liquid weekly option chains, usable put-spread premium, and enough business-model diversity to test whether the detector generalizes beyond NVDA.";
const UNIVERSE_RESEARCH_METHOD: &str = "Each symbol independently runs the same Rust put-credit-spread profile grid. Detector rules and execution rules are reported separately; no NVDA profile is copied into another symbol without out-of-sample proof.";

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
    ResearchNvda {
        #[arg(long, default_value = "2012-01-01")]
        from: NaiveDate,
        #[arg(long)]
        to: NaiveDate,
        #[arg(long)]
        max_expirations: Option<usize>,
        #[arg(long, default_value_t = 4)]
        fetch_concurrency: usize,
        #[arg(long, default_value_t = false)]
        force_refresh: bool,
    },
    ResearchSymbol {
        #[arg(long)]
        symbol: String,
        #[arg(long, default_value = "2012-01-01")]
        from: NaiveDate,
        #[arg(long)]
        to: NaiveDate,
        #[arg(long)]
        max_expirations: Option<usize>,
        #[arg(long, default_value_t = 4)]
        fetch_concurrency: usize,
        #[arg(long, default_value_t = false)]
        force_refresh: bool,
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
        #[arg(long, default_value = "2012-01-01")]
        from: NaiveDate,
        #[arg(long)]
        to: NaiveDate,
        #[arg(long)]
        max_expirations: Option<usize>,
        #[arg(long, default_value_t = 4)]
        fetch_concurrency: usize,
        #[arg(long, default_value_t = false)]
        force_refresh: bool,
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
    from: NaiveDate,
    to: NaiveDate,
    symbols: Vec<String>,
    plateau_run: Option<String>,
    strategy: String,
    selection_basis: String,
    research_method: String,
    expansion_seed: Vec<UniverseSeedSymbol>,
    results: Vec<UniverseSymbolSummary>,
}

#[derive(Clone, Debug, Serialize)]
struct UniverseSeedSymbol {
    rank: usize,
    symbol: String,
    role: String,
    rationale: String,
}

#[derive(Debug, Serialize)]
struct UniverseSymbolSummary {
    suitability_rank: usize,
    symbol: String,
    seed_rank: Option<usize>,
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
    best_fixed_trades: usize,
    best_fixed_pnl: f64,
    best_fixed_score: f64,
    best_fixed_robust_score: f64,
    latest_signal_status: Option<String>,
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
        Commands::ResearchNvda {
            from,
            to,
            max_expirations,
            fetch_concurrency,
            force_refresh,
        } => {
            let report = run_symbol_research(ResearchRequest {
                symbol: "NVDA".to_owned(),
                from,
                to,
                max_expirations,
                fetch_concurrency,
                force_refresh,
            })
            .await?;
            if let Some(best) = report.profiles.first() {
                println!(
                    "best={} trades={} pnl={:.2} score={:.4}",
                    best.profile.name,
                    best.metrics.trades,
                    best.metrics.total_pnl,
                    best.metrics.score
                );
            }
            Ok(())
        }
        Commands::ResearchSymbol {
            symbol,
            from,
            to,
            max_expirations,
            fetch_concurrency,
            force_refresh,
        } => {
            let report = run_symbol_research(ResearchRequest {
                symbol: symbol.to_uppercase(),
                from,
                to,
                max_expirations,
                fetch_concurrency,
                force_refresh,
            })
            .await?;
            if let Some(best) = report.profiles.first() {
                println!(
                    "best={} trades={} pnl={:.2} score={:.4}",
                    best.profile.name,
                    best.metrics.trades,
                    best.metrics.total_pnl,
                    best.metrics.score
                );
            }
            Ok(())
        }
        Commands::ResearchUniverse {
            symbols,
            plateau_run,
            from,
            to,
            max_expirations,
            fetch_concurrency,
            force_refresh,
        } => {
            research_universe(
                symbols,
                plateau_run,
                from,
                to,
                max_expirations,
                fetch_concurrency,
                force_refresh,
            )
            .await
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

async fn research_universe(
    symbols: Vec<String>,
    plateau_run: Option<PathBuf>,
    from: NaiveDate,
    to: NaiveDate,
    max_expirations: Option<usize>,
    fetch_concurrency: usize,
    force_refresh: bool,
) -> Result<()> {
    let symbols = normalize_symbols(symbols);
    if symbols.is_empty() {
        anyhow::bail!("research-universe requires at least one symbol");
    }

    let plateau_run = if let Some(path) = plateau_run {
        let report_path = research_report_path(&path);
        let plateau_status = read_plateau_run_gate(&report_path)?;
        if !plateau_status.expansion_ready {
            anyhow::bail!(
                "plateau run {} is not expansion-ready; status={}",
                report_path.display(),
                plateau_status.status
            );
        }
        Some(report_path)
    } else {
        None
    };

    let run_dir = next_run_dir("universe-research")?;
    fs::create_dir_all(&run_dir)?;
    let run_id = run_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("universe-research")
        .to_owned();
    let expansion_seed = expansion_seed_for_symbols(&symbols);
    let mut results = Vec::new();
    for symbol in &symbols {
        println!("researching {symbol}");
        let request = ResearchRequest {
            symbol: symbol.clone(),
            from,
            to,
            max_expirations,
            fetch_concurrency,
            force_refresh,
        };
        match run_symbol_research(request).await {
            Ok(report) => results.push(universe_symbol_summary(&report, &expansion_seed)),
            Err(err) => {
                eprintln!("research failed for {symbol}: {err:#}");
                results.push(universe_symbol_error_summary(symbol, &expansion_seed, &err));
            }
        }
    }
    rank_universe_results(&mut results);

    let summary = UniverseResearchSummary {
        run_id,
        from,
        to,
        symbols,
        plateau_run: plateau_run.as_ref().map(|path| path.display().to_string()),
        strategy: "put_credit_spread".to_owned(),
        selection_basis: UNIVERSE_SELECTION_BASIS.to_owned(),
        research_method: UNIVERSE_RESEARCH_METHOD.to_owned(),
        expansion_seed,
        results,
    };
    fs::write(
        run_dir.join("summary.json"),
        serde_json::to_string_pretty(&summary)?,
    )?;
    fs::write(run_dir.join("report.md"), universe_markdown(&summary))?;
    println!("wrote {}", run_dir.display());
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

fn expansion_seed_for_symbols(symbols: &[String]) -> Vec<UniverseSeedSymbol> {
    let default_seed = default_universe_seed();
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
                }
            }
        })
        .collect()
}

fn default_universe_seed() -> Vec<UniverseSeedSymbol> {
    let metadata = [
        (
            "premium_liquidity_leader",
            "High-liquidity, premium-rich single-stock option chain; tests whether rich credits survive gap and drawdown risk.",
        ),
        (
            "semiconductor_beta_peer",
            "Liquid semiconductor chain with NVDA-adjacent beta; tests whether the detector is sector-specific or transferable.",
        ),
        (
            "mega_cap_premium_growth",
            "Deep mega-cap growth chain with active weeklies and historically usable premium; tests a non-semiconductor high-beta large cap.",
        ),
        (
            "commerce_cloud_growth",
            "Large, liquid growth stock with active weeklies; adds a different earnings and volatility profile than semiconductors and social ads.",
        ),
        (
            "liquidity_quality_anchor",
            "Deep, tight option chain with lower relative premium; useful as an execution-quality control for conservative fills.",
        ),
    ];
    DEFAULT_PLATEAU_UNIVERSE_SYMBOLS
        .iter()
        .zip(metadata.iter())
        .enumerate()
        .map(|(idx, (symbol, (role, rationale)))| UniverseSeedSymbol {
            rank: idx + 1,
            symbol: (*symbol).to_owned(),
            role: (*role).to_owned(),
            rationale: (*rationale).to_owned(),
        })
        .collect()
}

fn research_report_path(path: &Path) -> PathBuf {
    if path.is_dir() {
        path.join("research.json")
    } else {
        path.to_path_buf()
    }
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
        seed_role: seed.map(|seed| seed.role.clone()),
        seed_rationale: seed.map(|seed| seed.rationale.clone()),
        research_status: "ok".to_owned(),
        error_message: None,
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

fn universe_markdown(summary: &UniverseResearchSummary) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "# SpreadFoundry Universe Research {}\n\n",
        summary.run_id
    ));
    out.push_str(&format!(
        "- Window: `{}` to `{}`\n- Symbols: `{}`\n- Plateau run: `{}`\n- Strategy: `{}`\n- Selection basis: {}\n- Research method: {}\n\n",
        summary.from,
        summary.to,
        summary.symbols.join(", "),
        summary.plateau_run.as_deref().unwrap_or("not provided"),
        summary.strategy,
        summary.selection_basis,
        summary.research_method
    ));

    out.push_str("## Research Protocol\n\n");
    out.push_str("- Detector search: each symbol gets its own DTE, delta, credit, width, liquidity, IV, trend, drawdown, and realized-volatility filters selected only from that symbol's historical training data.\n");
    out.push_str("- Execution strategy search: take-profit, stop-loss, force-close DTE, cooldown, and spread-selection rules are scored separately from detector filters under conservative bid/ask fills.\n");
    out.push_str("- Promotion rule: seed order never promotes a symbol; fixed-profile OOS passes, walk-forward evidence, holdout evidence, and deployment gates drive the suitability ranking.\n\n");

    out.push_str("## Expansion Seed\n\n");
    out.push_str("| Rank | Symbol | Role | Rationale |\n");
    out.push_str("|---:|---|---|---|\n");
    for seed in &summary.expansion_seed {
        out.push_str(&format!(
            "| {} | {} | {} | {} |\n",
            seed.rank, seed.symbol, seed.role, seed.rationale
        ));
    }
    out.push('\n');

    out.push_str("## Symbol Suitability Ranking\n\n");
    out.push_str("| Rank | Seed Rank | Symbol | Research | Error | Report | Deployment | Plateau | Detector Status | Execution Status | Fixed OOS Passes | Best Fixed Profile | Best Fixed Detector | Best Fixed Execution | Fixed Trades | Fixed PnL | Fixed Score | Fixed Robust | Best Profile | Detector | Execution | Trades | PnL | Score | Robust Score | WF Trades | WF PnL | WF Score | Holdout Trades | Holdout PnL | Holdout Score | Expirations | Rows | Latest Signal |\n");
    out.push_str(
        "|---:|---:|---|---|---|---|---|---|---|---|---:|---|---|---|---:|---:|---:|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|\n",
    );
    for result in &summary.results {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {:.2} | {:.4} | {:.4} | {} | {} | {} | {} | {:.2} | {:.4} | {:.4} | {} | {:.2} | {:.4} | {} | {:.2} | {:.4} | {} | {} | {} |\n",
            result.suitability_rank,
            result
                .seed_rank
                .map(|rank| rank.to_string())
                .unwrap_or_else(|| "n/a".to_owned()),
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
    fn default_universe_seed_is_five_non_nvda_single_stocks() {
        let seed = default_universe_seed();

        assert_eq!(seed.len(), 5);
        assert_eq!(
            seed.iter()
                .map(|symbol| symbol.symbol.as_str())
                .collect::<Vec<_>>(),
            DEFAULT_PLATEAU_UNIVERSE_SYMBOLS.to_vec()
        );
        assert!(!seed.iter().any(|symbol| symbol.symbol == "NVDA"));
        assert!(seed.iter().all(|symbol| !symbol.rationale.is_empty()));
    }

    #[test]
    fn expansion_seed_marks_manual_overrides() {
        let seed = expansion_seed_for_symbols(&["AAPL".to_owned(), "GOOGL".to_owned()]);

        assert_eq!(seed[0].rank, 1);
        assert_eq!(seed[0].symbol, "AAPL");
        assert_eq!(seed[0].role, "liquidity_quality_anchor");
        assert_eq!(seed[1].rank, 2);
        assert_eq!(seed[1].symbol, "GOOGL");
        assert_eq!(seed[1].role, "manual_override");
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
            from: NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
            to: NaiveDate::from_ymd_opt(2024, 12, 31).unwrap(),
            symbols: vec!["TSLA".to_owned()],
            plateau_run: Some("runs/nvda/research.json".to_owned()),
            strategy: "put_credit_spread".to_owned(),
            selection_basis: UNIVERSE_SELECTION_BASIS.to_owned(),
            research_method: UNIVERSE_RESEARCH_METHOD.to_owned(),
            expansion_seed: Vec::new(),
            results,
        };

        let markdown = universe_markdown(&summary);

        assert!(markdown.contains("Strategy: `put_credit_spread`"));
        assert!(markdown.contains("same Rust put-credit-spread profile grid"));
        assert!(markdown.contains("## Research Protocol"));
        assert!(markdown.contains("Detector search: each symbol gets its own"));
        assert!(markdown.contains("Execution strategy search: take-profit"));
        assert!(markdown.contains("## Symbol Suitability Ranking"));
        assert!(markdown.contains("Detector Status"));
        assert!(markdown.contains("Best Fixed Detector"));
        assert!(markdown.contains("put_spread_detector_test"));
        assert!(markdown.contains("put_spread_execution_test"));
    }

    #[test]
    fn universe_results_keep_symbol_errors_and_rank_them_last() {
        let expansion_seed = expansion_seed_for_symbols(&["TSLA".to_owned(), "AAPL".to_owned()]);
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
            best_fixed_trades: 4,
            best_fixed_pnl: 40.0,
            best_fixed_score: input.walk_forward_score,
            best_fixed_robust_score: input.robust_score,
            latest_signal_status: Some("research_only".to_owned()),
        }
    }
}
