use crate::opt::OptimizationScore;
use crate::types::SimTrade;
use anyhow::Context;
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Serialize)]
pub struct RunReport {
    pub run_id: String,
    pub strategy: String,
    pub trades: usize,
    pub total_pnl: String,
    pub score: OptimizationScore,
}

pub fn write_run_report(
    run_dir: impl AsRef<Path>,
    strategy: &str,
    trades: &[SimTrade],
    score: OptimizationScore,
) -> anyhow::Result<RunReport> {
    let run_dir = run_dir.as_ref();
    fs::create_dir_all(run_dir)
        .with_context(|| format!("creating run dir {}", run_dir.display()))?;
    let run_id = run_dir
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("run")
        .to_owned();
    let total_pnl = trades
        .iter()
        .fold(rust_decimal::Decimal::ZERO, |acc, trade| acc + trade.pnl);
    let report = RunReport {
        run_id,
        strategy: strategy.to_owned(),
        trades: trades.len(),
        total_pnl: total_pnl.to_string(),
        score,
    };

    write_json(run_dir.join("metrics.json"), &report)?;
    write_json(run_dir.join("trades.json"), trades)?;
    fs::write(run_dir.join("report.md"), markdown_report(&report, trades))
        .with_context(|| format!("writing {}", run_dir.join("report.md").display()))?;
    Ok(report)
}

pub fn read_report_markdown(run_dir: impl AsRef<Path>) -> anyhow::Result<String> {
    let path = run_dir.as_ref().join("report.md");
    fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))
}

fn write_json<T: Serialize + ?Sized>(path: PathBuf, value: &T) -> anyhow::Result<()> {
    let body = serde_json::to_string_pretty(value)?;
    fs::write(&path, body).with_context(|| format!("writing {}", path.display()))
}

fn markdown_report(report: &RunReport, trades: &[SimTrade]) -> String {
    let mut out = String::new();
    out.push_str(&format!("# SpreadFoundry Run {}\n\n", report.run_id));
    out.push_str(&format!("- Strategy: `{}`\n", report.strategy));
    out.push_str(&format!("- Trades: `{}`\n", report.trades));
    out.push_str(&format!("- Total PnL: `{}`\n", report.total_pnl));
    out.push_str(&format!("- Score: `{:.6}`\n", report.score.score));
    out.push_str(&format!(
        "- Mean return on risk: `{:.6}`\n",
        report.score.mean_return_on_risk
    ));
    out.push_str(&format!(
        "- CVaR 95 loss: `{:.6}`\n",
        report.score.cvar_95_loss
    ));
    out.push_str(&format!(
        "- Max drawdown: `{:.6}`\n\n",
        report.score.max_drawdown
    ));
    out.push_str("| Entry | Exit | Short Put | Long Put | Credit | Exit Debit | PnL | Reason |\n");
    out.push_str("|---|---|---:|---:|---:|---:|---:|---|\n");
    for trade in trades {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {:?} |\n",
            trade.entry_ts,
            trade.exit_ts,
            trade.short_put.strike,
            trade.long_put.strike,
            trade.entry_credit,
            trade.exit_debit,
            trade.pnl,
            trade.exit_reason
        ));
    }
    out
}
