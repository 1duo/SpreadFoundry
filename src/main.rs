use anyhow::{Context, Result};
use chrono::{NaiveDate, Utc};
use clap::{Parser, ValueEnum};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use spreadfoundry::broker::RobinhoodBrokerAdapter;
use spreadfoundry::fixture;
use spreadfoundry::opt::{OptimizationResult, rank_results, score_trades};
use spreadfoundry::report::{read_report_markdown, write_run_report};
use spreadfoundry::research::{ResearchRequest, run_nvda_research};
use spreadfoundry::sim::{ExitRules, SpreadExitQuote, choose_exit};
use spreadfoundry::strategy::{CandidateFilters, generate_put_spread_candidates};
use spreadfoundry::theta::{ThetaClient, ThetaUniverseRequest};
use std::fs;
use std::path::{Path, PathBuf};

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
            let report = run_nvda_research(ResearchRequest {
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
