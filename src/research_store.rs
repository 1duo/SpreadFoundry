use anyhow::{Context, Result};
use chrono::{NaiveDate, Utc};
use duckdb::{Connection, params};
use serde::Serialize;
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use crate::live_market::LiveMarketSnapshotRecord;
use crate::research::{PortfolioWheelReport, ResearchReport};

pub const DEFAULT_RESEARCH_STORE_PATH: &str = "data/spreadfoundry.duckdb";
const RESEARCH_STORE_PATH_ENV: &str = "SPREAD_RESEARCH_STORE_PATH";
const RESEARCH_STORE_SKIP_CACHE_SYNC_ENV: &str = "SPREAD_RESEARCH_STORE_SKIP_CACHE_SYNC";
const STORE_DATA_VERSION: &str = "duckdb-v1";

static STORE_WRITE_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static RESEARCH_STORE_PATH_OVERRIDE: OnceLock<PathBuf> = OnceLock::new();
static RESEARCH_STORE_CACHE_SYNC_OVERRIDE: OnceLock<bool> = OnceLock::new();
static CACHE_SYNCED_SYMBOL_DIRS: OnceLock<Mutex<HashSet<(PathBuf, String, PathBuf)>>> =
    OnceLock::new();
static DEFAULT_STORE_CONNECTIONS: OnceLock<Mutex<HashMap<PathBuf, ResearchStore>>> =
    OnceLock::new();

pub struct ResearchStore {
    conn: Connection,
}

struct ResearchRunUpsert<'a> {
    run_id: &'a str,
    command_family: &'a str,
    symbols_json: &'a str,
    profile_family: &'a str,
    from: NaiveDate,
    to: NaiveDate,
    artifact_path: &'a Path,
    report_path: &'a Path,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResearchStoreHealth {
    pub path: String,
    pub cache_windows: i64,
    pub option_rows: i64,
    pub backfill_attempts: i64,
    pub research_runs: i64,
    pub profile_results: i64,
    pub trade_summaries: i64,
    pub live_market_snapshots: i64,
    pub live_signal_candidates: i64,
    pub live_provider_health: i64,
    pub date_ranges: Vec<ResearchStoreDateRange>,
    pub failed_cache_windows: Vec<ResearchStoreFailedWindow>,
    pub latest_runs: Vec<ResearchStoreRunSummary>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResearchStoreDateRange {
    pub symbol: String,
    pub rows: i64,
    pub first_date: Option<String>,
    pub last_date: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResearchStoreFailedWindow {
    pub symbol: String,
    pub right: String,
    pub dataset: String,
    pub expiration: String,
    pub start_date: String,
    pub end_date: String,
    pub status: String,
    pub error: Option<String>,
    pub updated_at: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResearchStoreRunSummary {
    pub run_id: String,
    pub command_family: String,
    pub symbols_json: String,
    pub profile_family: String,
    pub from_date: String,
    pub to_date: String,
    pub artifact_path: String,
    pub created_at: String,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ResearchStoreImportReport {
    pub raw_root: String,
    pub symbols: Vec<String>,
    pub files_seen: usize,
    pub cache_windows_recorded: usize,
    pub files_imported: usize,
    pub files_failed: usize,
    pub option_rows_imported: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResearchStorePerfReport {
    pub raw_root: String,
    pub symbols: Vec<String>,
    pub files_scanned: usize,
    pub sync_ms: u128,
    pub count_query_ms: u128,
    pub cache_windows: i64,
    pub option_rows: i64,
}

#[derive(Clone, Debug)]
pub struct ResearchStoreCacheWindow {
    pub symbol: String,
    pub right: String,
    pub dataset: String,
    pub expiration: NaiveDate,
    pub start: NaiveDate,
    pub end: NaiveDate,
    pub source_path: PathBuf,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ResearchStoreOptionRow {
    pub symbol: String,
    pub date: NaiveDate,
    pub expiration: NaiveDate,
    pub right: String,
    pub strike: f64,
    pub bid: f64,
    pub ask: f64,
    pub mark: f64,
    pub delta: f64,
    pub implied_vol: f64,
    pub underlying_price: f64,
    pub open_interest: u32,
    pub source_path: String,
}

impl ResearchStore {
    pub fn open_default() -> Result<Self> {
        Self::open(default_research_store_path())
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create research store parent {}", parent.display()))?;
        }
        let conn = Connection::open(path)
            .with_context(|| format!("open research store {}", path.display()))?;
        let store = Self { conn };
        store.init_schema()?;
        Ok(store)
    }

    fn init_schema(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS cache_windows (
                symbol VARCHAR NOT NULL,
                option_right VARCHAR NOT NULL,
                dataset VARCHAR NOT NULL,
                expiration VARCHAR NOT NULL,
                start_date VARCHAR NOT NULL,
                end_date VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                source_path VARCHAR NOT NULL,
                row_count BIGINT,
                error VARCHAR,
                updated_at VARCHAR NOT NULL
            );

            CREATE TABLE IF NOT EXISTS backfill_attempts (
                symbol VARCHAR NOT NULL,
                option_right VARCHAR NOT NULL,
                dataset VARCHAR NOT NULL,
                expiration VARCHAR NOT NULL,
                start_date VARCHAR NOT NULL,
                end_date VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                error VARCHAR,
                attempted_at VARCHAR NOT NULL
            );

            CREATE TABLE IF NOT EXISTS option_rows (
                symbol VARCHAR NOT NULL,
                date VARCHAR NOT NULL,
                expiration VARCHAR NOT NULL,
                option_right VARCHAR NOT NULL,
                strike DOUBLE NOT NULL,
                bid DOUBLE NOT NULL,
                ask DOUBLE NOT NULL,
                mark DOUBLE NOT NULL,
                delta DOUBLE NOT NULL,
                implied_vol DOUBLE NOT NULL,
                underlying_price DOUBLE NOT NULL,
                open_interest BIGINT NOT NULL,
                source_path VARCHAR NOT NULL
            );

            CREATE TABLE IF NOT EXISTS research_runs (
                run_id VARCHAR NOT NULL,
                command_family VARCHAR NOT NULL,
                symbols_json VARCHAR NOT NULL,
                profile_family VARCHAR NOT NULL,
                from_date VARCHAR NOT NULL,
                to_date VARCHAR NOT NULL,
                artifact_path VARCHAR NOT NULL,
                report_path VARCHAR NOT NULL,
                data_version VARCHAR NOT NULL,
                created_at VARCHAR NOT NULL
            );

            CREATE TABLE IF NOT EXISTS profile_results (
                run_id VARCHAR NOT NULL,
                profile_rank BIGINT NOT NULL,
                profile_name VARCHAR NOT NULL,
                structure VARCHAR NOT NULL,
                trades BIGINT NOT NULL,
                total_pnl DOUBLE NOT NULL,
                score DOUBLE NOT NULL,
                robust_score DOUBLE NOT NULL,
                max_drawdown DOUBLE NOT NULL,
                win_rate DOUBLE NOT NULL,
                profit_factor DOUBLE NOT NULL,
                trades_per_year DOUBLE NOT NULL,
                gate_status VARCHAR NOT NULL,
                gate_pass BOOLEAN NOT NULL
            );

            CREATE TABLE IF NOT EXISTS trade_summaries (
                run_id VARCHAR NOT NULL,
                profile_rank BIGINT NOT NULL,
                profile_name VARCHAR NOT NULL,
                trade_index BIGINT NOT NULL,
                symbol VARCHAR NOT NULL,
                structure VARCHAR NOT NULL,
                entry_date VARCHAR NOT NULL,
                exit_date VARCHAR NOT NULL,
                expiration VARCHAR NOT NULL,
                dte_entry BIGINT NOT NULL,
                days_held BIGINT NOT NULL,
                pnl DOUBLE NOT NULL,
                max_loss DOUBLE NOT NULL,
                return_on_risk DOUBLE NOT NULL,
                exit_reason VARCHAR NOT NULL,
                short_strike DOUBLE NOT NULL,
                long_strike DOUBLE NOT NULL,
                width DOUBLE NOT NULL,
                entry_credit DOUBLE NOT NULL,
                exit_debit DOUBLE NOT NULL,
                short_delta DOUBLE NOT NULL,
                long_delta DOUBLE NOT NULL,
                short_oi BIGINT NOT NULL,
                long_oi BIGINT NOT NULL,
                underlying_price DOUBLE NOT NULL
            );

            CREATE TABLE IF NOT EXISTS live_market_snapshots (
                snapshot_id VARCHAR NOT NULL,
                observed_at VARCHAR NOT NULL,
                as_of VARCHAR NOT NULL,
                provider VARCHAR NOT NULL,
                approved_strategy_id VARCHAR NOT NULL,
                profile_name VARCHAR NOT NULL,
                decision_status VARCHAR NOT NULL,
                decision_reason VARCHAR NOT NULL,
                selected_symbol VARCHAR,
                selected_strategy VARCHAR,
                source_live_signal VARCHAR NOT NULL,
                output_artifact VARCHAR NOT NULL,
                raw_json VARCHAR NOT NULL
            );

            CREATE TABLE IF NOT EXISTS live_signal_candidates (
                snapshot_id VARCHAR NOT NULL,
                candidate_rank BIGINT NOT NULL,
                symbol VARCHAR NOT NULL,
                strategy VARCHAR NOT NULL,
                signal_status VARCHAR NOT NULL,
                entry_date VARCHAR,
                expiration VARCHAR,
                decision_status VARCHAR NOT NULL,
                reason VARCHAR NOT NULL,
                max_loss DOUBLE,
                raw_json VARCHAR NOT NULL
            );

            CREATE TABLE IF NOT EXISTS live_provider_health (
                snapshot_id VARCHAR NOT NULL,
                checked_at VARCHAR NOT NULL,
                provider VARCHAR NOT NULL,
                ok BOOLEAN NOT NULL,
                status VARCHAR NOT NULL,
                latency_ms BIGINT NOT NULL,
                symbols_requested BIGINT NOT NULL,
                symbols_ready BIGINT NOT NULL,
                max_source_age_seconds BIGINT NOT NULL,
                source_age_seconds BIGINT,
                reason VARCHAR NOT NULL,
                raw_json VARCHAR NOT NULL
            );

            CREATE INDEX IF NOT EXISTS cache_windows_lookup_idx
                ON cache_windows(symbol, option_right, dataset, expiration, status, start_date, end_date);
            CREATE INDEX IF NOT EXISTS option_rows_lookup_idx
                ON option_rows(symbol, option_right, expiration, date, strike);
            "#,
        )?;
        Ok(())
    }

    pub fn sync_symbol_cache_dir(
        &mut self,
        symbol: &str,
        raw_dir: &Path,
        max_files: Option<usize>,
    ) -> Result<ResearchStoreImportReport> {
        let mut report = ResearchStoreImportReport {
            raw_root: raw_dir.display().to_string(),
            symbols: vec![symbol.to_owned()],
            ..ResearchStoreImportReport::default()
        };
        if !raw_dir.exists() {
            return Ok(report);
        }
        for entry in fs::read_dir(raw_dir).with_context(|| format!("read {}", raw_dir.display()))? {
            if max_files.is_some_and(|max| report.files_seen >= max) {
                break;
            }
            let entry = entry.with_context(|| format!("read entry in {}", raw_dir.display()))?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(window) = cache_window_from_path(&path) else {
                continue;
            };
            if !window.symbol.eq_ignore_ascii_case(symbol) {
                continue;
            }
            report.files_seen += 1;
            self.insert_cache_window_if_absent(&window, "present", None, None)?;
            report.cache_windows_recorded += 1;
        }
        Ok(report)
    }

    pub fn import_symbol_cache_dir(
        &mut self,
        symbol: &str,
        raw_dir: &Path,
        max_files: Option<usize>,
    ) -> Result<ResearchStoreImportReport> {
        let mut report = ResearchStoreImportReport {
            raw_root: raw_dir.display().to_string(),
            symbols: vec![symbol.to_owned()],
            ..ResearchStoreImportReport::default()
        };
        if !raw_dir.exists() {
            return Ok(report);
        }
        for entry in fs::read_dir(raw_dir).with_context(|| format!("read {}", raw_dir.display()))? {
            if max_files.is_some_and(|max| report.files_seen >= max) {
                break;
            }
            let entry = entry.with_context(|| format!("read entry in {}", raw_dir.display()))?;
            let path = entry.path();
            if !path.is_file() || cache_window_from_path(&path).is_none() {
                continue;
            }
            report.files_seen += 1;
            match self.import_cache_file(&path) {
                Ok(imported) => {
                    report.cache_windows_recorded += 1;
                    report.files_imported += 1;
                    report.option_rows_imported += imported;
                }
                Err(error) => {
                    report.files_failed += 1;
                    if let Some(window) = cache_window_from_path(&path) {
                        self.upsert_cache_window(
                            &window,
                            "bad_json",
                            None,
                            Some(compact_error(&format!("{error:#}"))),
                        )?;
                    }
                }
            }
        }
        Ok(report)
    }

    pub fn import_cache_file(&mut self, path: &Path) -> Result<usize> {
        let window = cache_window_from_path(path)
            .with_context(|| format!("not a Theta cache window path: {}", path.display()))?;
        let body = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        let json: Value =
            serde_json::from_str(&body).with_context(|| format!("parse {}", path.display()))?;
        let row_count = response_row_count(&json);
        self.upsert_cache_window(&window, "success", Some(row_count as i64), None)?;
        if window.dataset != "greeks" {
            return Ok(0);
        }
        let oi_map = read_oi_map_for_window(&window)?;
        let rows = parse_greeks_option_rows(&window, &json, &oi_map)?;
        self.replace_option_rows(&window, &rows)?;
        Ok(rows.len())
    }

    pub fn cache_has_complete_coverage(
        &self,
        symbol: &str,
        expiration: NaiveDate,
        start: NaiveDate,
        end: NaiveDate,
        right: &str,
    ) -> Result<bool> {
        Ok(
            self.cache_dataset_has_complete_coverage(symbol, expiration, start, end, right, "oi")?
                && self.cache_dataset_has_complete_coverage(
                    symbol, expiration, start, end, right, "greeks",
                )?,
        )
    }

    pub fn option_rows_have_complete_coverage(
        &self,
        symbol: &str,
        expiration: NaiveDate,
        start: NaiveDate,
        end: NaiveDate,
        right: &str,
    ) -> Result<bool> {
        self.cache_dataset_success_has_complete_coverage(
            symbol,
            expiration,
            start,
            end,
            right,
            "option_rows",
        )
    }

    fn cache_dataset_has_complete_coverage(
        &self,
        symbol: &str,
        expiration: NaiveDate,
        start: NaiveDate,
        end: NaiveDate,
        right: &str,
        dataset: &str,
    ) -> Result<bool> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT start_date, end_date
            FROM cache_windows
            WHERE symbol = upper(?)
              AND option_right = ?
              AND dataset = ?
              AND expiration = ?
              AND status IN ('present', 'success')
            ORDER BY start_date, end_date
            "#,
        )?;
        let mut rows = stmt.query(params![symbol, right, dataset, date_s(expiration),])?;
        let mut windows = Vec::new();
        while let Some(row) = rows.next()? {
            let start_s: String = row.get(0)?;
            let end_s: String = row.get(1)?;
            windows.push((parse_iso_date(&start_s)?, parse_iso_date(&end_s)?));
        }
        Ok(covering_sequence_exists(&windows, start, end))
    }

    fn cache_dataset_success_has_complete_coverage(
        &self,
        symbol: &str,
        expiration: NaiveDate,
        start: NaiveDate,
        end: NaiveDate,
        right: &str,
        dataset: &str,
    ) -> Result<bool> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT start_date, end_date
            FROM cache_windows
            WHERE symbol = upper(?)
              AND option_right = ?
              AND dataset = ?
              AND expiration = ?
              AND status = 'success'
            ORDER BY start_date, end_date
            "#,
        )?;
        let mut rows = stmt.query(params![symbol, right, dataset, date_s(expiration),])?;
        let mut windows = Vec::new();
        while let Some(row) = rows.next()? {
            let start_s: String = row.get(0)?;
            let end_s: String = row.get(1)?;
            windows.push((parse_iso_date(&start_s)?, parse_iso_date(&end_s)?));
        }
        Ok(covering_sequence_exists(&windows, start, end))
    }

    pub fn option_rows(
        &self,
        symbol: &str,
        expiration: NaiveDate,
        start: NaiveDate,
        end: NaiveDate,
        right: &str,
    ) -> Result<Vec<ResearchStoreOptionRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT symbol, date, expiration, option_right, strike, bid, ask, mark, delta,
                   implied_vol, underlying_price, open_interest, source_path
            FROM option_rows
            WHERE symbol = upper(?)
              AND option_right = ?
              AND expiration = ?
              AND date >= ?
              AND date <= ?
            ORDER BY date, strike
            "#,
        )?;
        let mut rows = stmt.query(params![
            symbol,
            right,
            date_s(expiration),
            date_s(start),
            date_s(end),
        ])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            let date: String = row.get(1)?;
            let expiration: String = row.get(2)?;
            let open_interest: i64 = row.get(11)?;
            out.push(ResearchStoreOptionRow {
                symbol: row.get(0)?,
                date: parse_iso_date(&date)?,
                expiration: parse_iso_date(&expiration)?,
                right: row.get(3)?,
                strike: row.get(4)?,
                bid: row.get(5)?,
                ask: row.get(6)?,
                mark: row.get(7)?,
                delta: row.get(8)?,
                implied_vol: row.get(9)?,
                underlying_price: row.get(10)?,
                open_interest: open_interest.max(0) as u32,
                source_path: row.get(12)?,
            });
        }
        Ok(out)
    }

    pub fn replace_option_rows_for_window(
        &mut self,
        symbol: &str,
        expiration: NaiveDate,
        start: NaiveDate,
        end: NaiveDate,
        right: &str,
        rows: &[ResearchStoreOptionRow],
    ) -> Result<()> {
        let window = ResearchStoreCacheWindow {
            symbol: symbol.to_uppercase(),
            right: right.to_owned(),
            dataset: "greeks".to_owned(),
            expiration,
            start,
            end,
            source_path: PathBuf::new(),
        };
        self.replace_option_rows(&window, rows)
    }

    pub fn record_research_report(
        &mut self,
        report: &ResearchReport,
        artifact_path: &Path,
        report_path: &Path,
    ) -> Result<()> {
        let symbols_json = serde_json::to_string(std::slice::from_ref(&report.symbol))?;
        self.upsert_research_run(ResearchRunUpsert {
            run_id: &report.run_id,
            command_family: "symbol_research",
            symbols_json: &symbols_json,
            profile_family: report.profile_family.as_str(),
            from: report.from,
            to: report.to,
            artifact_path,
            report_path,
        })?;
        self.replace_profile_results_for_research_report(report)?;
        Ok(())
    }

    pub fn record_portfolio_report(
        &mut self,
        report: &PortfolioWheelReport,
        command_family: &str,
        artifact_path: &Path,
        report_path: &Path,
    ) -> Result<()> {
        let symbols_json = serde_json::to_string(&report.symbols)?;
        self.upsert_research_run(ResearchRunUpsert {
            run_id: &report.run_id,
            command_family,
            symbols_json: &symbols_json,
            profile_family: "portfolio",
            from: report.from,
            to: report.to,
            artifact_path,
            report_path,
        })?;
        self.replace_profile_results_for_portfolio_report(report)?;
        Ok(())
    }

    fn upsert_research_run(&mut self, run: ResearchRunUpsert<'_>) -> Result<()> {
        self.conn.execute(
            "DELETE FROM research_runs WHERE run_id = ?",
            params![run.run_id],
        )?;
        self.conn.execute(
            r#"
            INSERT INTO research_runs (
                run_id, command_family, symbols_json, profile_family, from_date, to_date,
                artifact_path, report_path, data_version, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
            params![
                run.run_id,
                run.command_family,
                run.symbols_json,
                run.profile_family,
                date_s(run.from),
                date_s(run.to),
                run.artifact_path.display().to_string(),
                run.report_path.display().to_string(),
                STORE_DATA_VERSION,
                now_s(),
            ],
        )?;
        Ok(())
    }

    fn replace_profile_results_for_research_report(
        &mut self,
        report: &ResearchReport,
    ) -> Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "DELETE FROM profile_results WHERE run_id = ?",
            params![report.run_id],
        )?;
        tx.execute(
            "DELETE FROM trade_summaries WHERE run_id = ?",
            params![report.run_id],
        )?;
        {
            let mut insert_profile = tx.prepare(
                r#"
                INSERT INTO profile_results (
                    run_id, profile_rank, profile_name, structure, trades, total_pnl, score,
                    robust_score, max_drawdown, win_rate, profit_factor, trades_per_year,
                    gate_status, gate_pass
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )?;
            let mut insert_trade = tx.prepare(
                r#"
                INSERT INTO trade_summaries (
                    run_id, profile_rank, profile_name, trade_index, symbol, structure, entry_date,
                    exit_date, expiration, dte_entry, days_held, pnl, max_loss, return_on_risk,
                    exit_reason, short_strike, long_strike, width, entry_credit, exit_debit,
                    short_delta, long_delta, short_oi, long_oi, underlying_price
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )?;
            for (rank, profile_result) in report.profiles.iter().enumerate() {
                let gate_status = if rank == 0 {
                    report.deployment_gate.status.as_str()
                } else {
                    "not_selected"
                };
                let gate_pass = rank == 0 && report.deployment_gate.pass;
                insert_profile.execute(params![
                    &report.run_id,
                    rank as i64,
                    &profile_result.profile.name,
                    profile_result.profile.structure.as_str(),
                    profile_result.metrics.trades as i64,
                    profile_result.metrics.total_pnl,
                    profile_result.metrics.score,
                    profile_result.metrics.robust_score,
                    profile_result.metrics.max_drawdown,
                    profile_result.metrics.win_rate,
                    profile_result.metrics.profit_factor,
                    profile_result.metrics.trades_per_year,
                    gate_status,
                    gate_pass,
                ])?;
                for (trade_index, trade) in profile_result.trades.iter().enumerate() {
                    insert_trade.execute(params![
                        &report.run_id,
                        rank as i64,
                        &profile_result.profile.name,
                        trade_index as i64,
                        &report.symbol,
                        profile_result.profile.structure.as_str(),
                        date_s(trade.entry_date),
                        date_s(trade.exit_date),
                        date_s(trade.expiration),
                        trade.dte_entry,
                        trade.days_held,
                        trade.pnl,
                        trade.max_loss,
                        trade.return_on_risk,
                        &trade.exit_reason,
                        trade.short_put,
                        trade.long_put,
                        trade.width,
                        trade.entry_credit,
                        trade.exit_debit,
                        trade.short_delta,
                        trade.long_delta,
                        trade.short_oi as i64,
                        trade.long_oi as i64,
                        trade.underlying_price,
                    ])?;
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn replace_profile_results_for_portfolio_report(
        &mut self,
        report: &PortfolioWheelReport,
    ) -> Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "DELETE FROM profile_results WHERE run_id = ?",
            params![report.run_id],
        )?;
        tx.execute(
            "DELETE FROM trade_summaries WHERE run_id = ?",
            params![report.run_id],
        )?;
        {
            let mut insert_profile = tx.prepare(
                r#"
                INSERT INTO profile_results (
                    run_id, profile_rank, profile_name, structure, trades, total_pnl, score,
                    robust_score, max_drawdown, win_rate, profit_factor, trades_per_year,
                    gate_status, gate_pass
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )?;
            let mut insert_trade = tx.prepare(
                r#"
                INSERT INTO trade_summaries (
                    run_id, profile_rank, profile_name, trade_index, symbol, structure, entry_date,
                    exit_date, expiration, dte_entry, days_held, pnl, max_loss, return_on_risk,
                    exit_reason, short_strike, long_strike, width, entry_credit, exit_debit,
                    short_delta, long_delta, short_oi, long_oi, underlying_price
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )?;
            for (rank, profile_result) in report.profiles.iter().enumerate() {
                insert_profile.execute(params![
                    &report.run_id,
                    rank as i64,
                    &profile_result.profile.name,
                    profile_result.profile.structure.as_str(),
                    profile_result.metrics.trades as i64,
                    profile_result.metrics.total_pnl,
                    profile_result.metrics.score,
                    profile_result.metrics.robust_score,
                    profile_result.metrics.max_drawdown,
                    profile_result.metrics.win_rate,
                    profile_result.metrics.profit_factor,
                    profile_result.metrics.trades_per_year,
                    &profile_result.gate_status,
                    profile_result.gate_pass,
                ])?;
                for (trade_index, trade) in profile_result.trades.iter().enumerate() {
                    insert_trade.execute(params![
                        &report.run_id,
                        rank as i64,
                        &profile_result.profile.name,
                        trade_index as i64,
                        &trade.symbol,
                        trade.strategy.as_str(),
                        date_s(trade.trade.entry_date),
                        date_s(trade.trade.exit_date),
                        date_s(trade.trade.expiration),
                        trade.trade.dte_entry,
                        trade.trade.days_held,
                        trade.trade.pnl,
                        trade.trade.max_loss,
                        trade.trade.return_on_risk,
                        &trade.trade.exit_reason,
                        trade.trade.short_put,
                        trade.trade.long_put,
                        trade.trade.width,
                        trade.trade.entry_credit,
                        trade.trade.exit_debit,
                        trade.trade.short_delta,
                        trade.trade.long_delta,
                        trade.trade.short_oi as i64,
                        trade.trade.long_oi as i64,
                        trade.trade.underlying_price,
                    ])?;
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn upsert_cache_window(
        &self,
        window: &ResearchStoreCacheWindow,
        status: &str,
        row_count: Option<i64>,
        error: Option<String>,
    ) -> Result<()> {
        self.delete_cache_window(window)?;
        self.insert_cache_window(window, status, row_count, error)
    }

    fn insert_cache_window_if_absent(
        &self,
        window: &ResearchStoreCacheWindow,
        status: &str,
        row_count: Option<i64>,
        error: Option<String>,
    ) -> Result<()> {
        let existing: i64 = self.conn.query_row(
            r#"
            SELECT COUNT(*)
            FROM cache_windows
            WHERE symbol = upper(?)
              AND option_right = ?
              AND dataset = ?
              AND expiration = ?
              AND start_date = ?
              AND end_date = ?
            "#,
            params![
                &window.symbol,
                &window.right,
                &window.dataset,
                date_s(window.expiration),
                date_s(window.start),
                date_s(window.end),
            ],
            |row| row.get(0),
        )?;
        if existing == 0 {
            self.insert_cache_window(window, status, row_count, error)?;
        }
        Ok(())
    }

    fn delete_cache_window(&self, window: &ResearchStoreCacheWindow) -> Result<()> {
        self.conn.execute(
            r#"
            DELETE FROM cache_windows
            WHERE symbol = upper(?)
              AND option_right = ?
              AND dataset = ?
              AND expiration = ?
              AND start_date = ?
              AND end_date = ?
            "#,
            params![
                &window.symbol,
                &window.right,
                &window.dataset,
                date_s(window.expiration),
                date_s(window.start),
                date_s(window.end),
            ],
        )?;
        Ok(())
    }

    fn insert_cache_window(
        &self,
        window: &ResearchStoreCacheWindow,
        status: &str,
        row_count: Option<i64>,
        error: Option<String>,
    ) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO cache_windows (
                symbol, option_right, dataset, expiration, start_date, end_date, status,
                source_path, row_count, error, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
            params![
                window.symbol.to_uppercase(),
                &window.right,
                &window.dataset,
                date_s(window.expiration),
                date_s(window.start),
                date_s(window.end),
                status,
                window.source_path.display().to_string(),
                row_count,
                error,
                now_s(),
            ],
        )?;
        Ok(())
    }

    fn replace_option_rows(
        &mut self,
        window: &ResearchStoreCacheWindow,
        rows: &[ResearchStoreOptionRow],
    ) -> Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute(
            r#"
            DELETE FROM option_rows
            WHERE symbol = upper(?)
              AND option_right = ?
              AND expiration = ?
              AND date >= ?
              AND date <= ?
            "#,
            params![
                &window.symbol,
                &window.right,
                date_s(window.expiration),
                date_s(window.start),
                date_s(window.end),
            ],
        )?;
        {
            let mut insert = tx.prepare(
                r#"
                INSERT INTO option_rows (
                    symbol, date, expiration, option_right, strike, bid, ask, mark, delta,
                    implied_vol, underlying_price, open_interest, source_path
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )?;
            for row in rows {
                insert.execute(params![
                    row.symbol.to_uppercase(),
                    date_s(row.date),
                    date_s(row.expiration),
                    &row.right,
                    row.strike,
                    row.bid,
                    row.ask,
                    row.mark,
                    row.delta,
                    row.implied_vol,
                    row.underlying_price,
                    row.open_interest as i64,
                    &row.source_path,
                ])?;
            }
        }
        let mut loaded_window = window.clone();
        loaded_window.dataset = "option_rows".to_owned();
        tx.execute(
            r#"
            DELETE FROM cache_windows
            WHERE symbol = upper(?)
              AND option_right = ?
              AND dataset = ?
              AND expiration = ?
              AND start_date = ?
              AND end_date = ?
            "#,
            params![
                &loaded_window.symbol,
                &loaded_window.right,
                &loaded_window.dataset,
                date_s(loaded_window.expiration),
                date_s(loaded_window.start),
                date_s(loaded_window.end),
            ],
        )?;
        tx.execute(
            r#"
            INSERT INTO cache_windows (
                symbol, option_right, dataset, expiration, start_date, end_date, status,
                source_path, row_count, error, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
            params![
                loaded_window.symbol.to_uppercase(),
                &loaded_window.right,
                &loaded_window.dataset,
                date_s(loaded_window.expiration),
                date_s(loaded_window.start),
                date_s(loaded_window.end),
                "success",
                loaded_window.source_path.display().to_string(),
                rows.len() as i64,
                None::<String>,
                now_s(),
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn health(&self, path: &Path) -> Result<ResearchStoreHealth> {
        Ok(ResearchStoreHealth {
            path: path.display().to_string(),
            cache_windows: self.table_count("cache_windows")?,
            option_rows: self.table_count("option_rows")?,
            backfill_attempts: self.table_count("backfill_attempts")?,
            research_runs: self.table_count("research_runs")?,
            profile_results: self.table_count("profile_results")?,
            trade_summaries: self.table_count("trade_summaries")?,
            live_market_snapshots: self.table_count("live_market_snapshots")?,
            live_signal_candidates: self.table_count("live_signal_candidates")?,
            live_provider_health: self.table_count("live_provider_health")?,
            date_ranges: self.date_ranges()?,
            failed_cache_windows: self.failed_cache_windows()?,
            latest_runs: self.latest_runs()?,
        })
    }

    pub fn record_live_market_snapshot(&mut self, record: &LiveMarketSnapshotRecord) -> Result<()> {
        let tx = self.conn.transaction()?;
        let selected_symbol = record
            .decision
            .selected_signal
            .as_ref()
            .map(|signal| signal.symbol.as_str());
        let selected_strategy = record
            .decision
            .selected_signal
            .as_ref()
            .map(|signal| signal.strategy.as_str());
        tx.execute(
            r#"
            INSERT INTO live_market_snapshots (
                snapshot_id, observed_at, as_of, provider, approved_strategy_id, profile_name,
                decision_status, decision_reason, selected_symbol, selected_strategy,
                source_live_signal, output_artifact, raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
            params![
                &record.snapshot_id,
                record.observed_at.to_rfc3339(),
                date_s(record.as_of),
                record.provider.to_string(),
                &record.approved_strategy_id,
                &record.profile_name,
                &record.decision.status,
                &record.decision.reason,
                selected_symbol,
                selected_strategy,
                &record.source_live_signal,
                &record.output,
                serde_json::to_string(record)?,
            ],
        )?;
        tx.execute(
            r#"
            INSERT INTO live_provider_health (
                snapshot_id, checked_at, provider, ok, status, latency_ms, symbols_requested,
                symbols_ready, max_source_age_seconds, source_age_seconds, reason, raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
            params![
                &record.snapshot_id,
                record.provider_health.checked_at.to_rfc3339(),
                record.provider_health.provider.to_string(),
                record.provider_health.ok,
                &record.provider_health.status,
                record.provider_health.latency_ms as i64,
                record.provider_health.symbols_requested as i64,
                record.provider_health.symbols_ready as i64,
                record.provider_health.max_source_age_seconds as i64,
                record
                    .provider_health
                    .source_age_seconds
                    .map(|age| age as i64),
                &record.provider_health.reason,
                serde_json::to_string(&record.provider_health)?,
            ],
        )?;
        {
            let mut insert = tx.prepare(
                r#"
                INSERT INTO live_signal_candidates (
                    snapshot_id, candidate_rank, symbol, strategy, signal_status, entry_date,
                    expiration, decision_status, reason, max_loss, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )?;
            for candidate in &record.candidates {
                insert.execute(params![
                    &record.snapshot_id,
                    candidate.candidate_rank as i64,
                    &candidate.symbol,
                    &candidate.strategy,
                    candidate.signal_status.as_str(),
                    candidate.entry_date.as_deref(),
                    candidate.expiration.as_deref(),
                    &candidate.decision_status,
                    &candidate.reason,
                    candidate.max_loss,
                    serde_json::to_string(candidate)?,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn table_count(&self, table: &str) -> Result<i64> {
        let sql = format!("SELECT COUNT(*) FROM {table}");
        Ok(self.conn.query_row(&sql, [], |row| row.get(0))?)
    }

    fn date_ranges(&self) -> Result<Vec<ResearchStoreDateRange>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT symbol, COUNT(*) AS rows, MIN(date), MAX(date)
            FROM option_rows
            GROUP BY symbol
            ORDER BY rows DESC, symbol
            LIMIT 25
            "#,
        )?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(ResearchStoreDateRange {
                symbol: row.get(0)?,
                rows: row.get(1)?,
                first_date: row.get(2)?,
                last_date: row.get(3)?,
            });
        }
        Ok(out)
    }

    fn failed_cache_windows(&self) -> Result<Vec<ResearchStoreFailedWindow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT symbol, option_right, dataset, expiration, start_date, end_date, status, error, updated_at
            FROM cache_windows
            WHERE status NOT IN ('present', 'success')
            ORDER BY updated_at DESC
            LIMIT 20
            "#,
        )?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(ResearchStoreFailedWindow {
                symbol: row.get(0)?,
                right: row.get(1)?,
                dataset: row.get(2)?,
                expiration: row.get(3)?,
                start_date: row.get(4)?,
                end_date: row.get(5)?,
                status: row.get(6)?,
                error: row.get(7)?,
                updated_at: row.get(8)?,
            });
        }
        Ok(out)
    }

    fn latest_runs(&self) -> Result<Vec<ResearchStoreRunSummary>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT run_id, command_family, symbols_json, profile_family, from_date, to_date,
                   artifact_path, created_at
            FROM research_runs
            ORDER BY created_at DESC
            LIMIT 10
            "#,
        )?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(ResearchStoreRunSummary {
                run_id: row.get(0)?,
                command_family: row.get(1)?,
                symbols_json: row.get(2)?,
                profile_family: row.get(3)?,
                from_date: row.get(4)?,
                to_date: row.get(5)?,
                artifact_path: row.get(6)?,
                created_at: row.get(7)?,
            });
        }
        Ok(out)
    }
}

pub fn default_research_store_path() -> PathBuf {
    if let Some(path) = RESEARCH_STORE_PATH_OVERRIDE.get() {
        return path.clone();
    }
    research_store_path_from_env(std::env::var_os(RESEARCH_STORE_PATH_ENV))
}

pub fn set_research_store_path_override(path: PathBuf) -> Result<()> {
    if path.as_os_str().is_empty() {
        anyhow::bail!("research store path override cannot be empty");
    }
    if let Some(existing) = RESEARCH_STORE_PATH_OVERRIDE.get() {
        if existing == &path {
            return Ok(());
        }
        anyhow::bail!(
            "research store path override already set to {}",
            existing.display()
        );
    }
    RESEARCH_STORE_PATH_OVERRIDE
        .set(path)
        .map_err(|_| anyhow::anyhow!("research store path override already set"))
}

fn research_store_path_from_env(value: Option<std::ffi::OsString>) -> PathBuf {
    value
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_RESEARCH_STORE_PATH))
}

pub fn research_store_cache_sync_enabled() -> bool {
    if let Some(enabled) = RESEARCH_STORE_CACHE_SYNC_OVERRIDE.get() {
        return *enabled;
    }
    !env_flag_enabled(std::env::var_os(RESEARCH_STORE_SKIP_CACHE_SYNC_ENV))
}

pub fn set_research_store_cache_sync_enabled_override(enabled: bool) -> Result<()> {
    if let Some(existing) = RESEARCH_STORE_CACHE_SYNC_OVERRIDE.get() {
        if *existing == enabled {
            return Ok(());
        }
        anyhow::bail!("research store cache sync override already set to {existing}");
    }
    RESEARCH_STORE_CACHE_SYNC_OVERRIDE
        .set(enabled)
        .map_err(|_| anyhow::anyhow!("research store cache sync override already set"))
}

fn env_flag_enabled(value: Option<std::ffi::OsString>) -> bool {
    value
        .and_then(|value| value.into_string().ok())
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

pub fn record_cached_theta_json(path: &Path, json: &Value) -> Result<()> {
    let Some(window) = cache_window_from_path(path) else {
        return Ok(());
    };
    with_default_store(|store| {
        let row_count = response_row_count(json) as i64;
        store.upsert_cache_window(&window, "success", Some(row_count), None)?;
        if window.dataset == "greeks" {
            let oi_map = read_oi_map_for_window(&window).unwrap_or_default();
            let rows = parse_greeks_option_rows(&window, json, &oi_map)?;
            store.replace_option_rows(&window, &rows)?;
        }
        Ok(())
    })
}

pub fn record_bad_theta_json(path: &Path, error: &str) -> Result<()> {
    let Some(window) = cache_window_from_path(path) else {
        return Ok(());
    };
    with_default_store(|store| {
        store.upsert_cache_window(&window, "bad_json", None, Some(compact_error(error)))
    })
}

pub fn record_research_report(
    report: &ResearchReport,
    artifact_path: &Path,
    report_path: &Path,
) -> Result<()> {
    with_default_store(|store| store.record_research_report(report, artifact_path, report_path))
}

pub fn record_portfolio_report(
    report: &PortfolioWheelReport,
    command_family: &str,
    artifact_path: &Path,
    report_path: &Path,
) -> Result<()> {
    with_default_store(|store| {
        store.record_portfolio_report(report, command_family, artifact_path, report_path)
    })
}

pub fn sync_cache_windows_for_symbol(symbol: &str, raw_dir: &Path) -> Result<()> {
    if !research_store_cache_sync_enabled() {
        return Ok(());
    }
    with_default_store(|store| {
        store.sync_symbol_cache_dir(symbol, raw_dir, None)?;
        Ok(())
    })
}

pub fn sync_cache_windows_for_symbol_once(symbol: &str, raw_dir: &Path) -> Result<()> {
    if !research_store_cache_sync_enabled() {
        return Ok(());
    }
    let key = (
        default_research_store_path(),
        symbol.trim().to_uppercase(),
        raw_dir.to_path_buf(),
    );
    let synced = CACHE_SYNCED_SYMBOL_DIRS.get_or_init(|| Mutex::new(HashSet::new()));
    {
        let synced = synced
            .lock()
            .map_err(|_| anyhow::anyhow!("research store cache-sync marker lock poisoned"))?;
        if synced.contains(&key) {
            return Ok(());
        }
    }

    sync_cache_windows_for_symbol(symbol, raw_dir)?;

    synced
        .lock()
        .map_err(|_| anyhow::anyhow!("research store cache-sync marker lock poisoned"))?
        .insert(key);
    Ok(())
}

pub fn cache_has_complete_coverage(
    symbol: &str,
    expiration: NaiveDate,
    start: NaiveDate,
    end: NaiveDate,
    right: &str,
) -> Result<bool> {
    with_default_store(|store| {
        store.cache_has_complete_coverage(symbol, expiration, start, end, right)
    })
}

pub fn option_rows_have_complete_coverage(
    symbol: &str,
    expiration: NaiveDate,
    start: NaiveDate,
    end: NaiveDate,
    right: &str,
) -> Result<bool> {
    with_default_store(|store| {
        store.option_rows_have_complete_coverage(symbol, expiration, start, end, right)
    })
}

pub fn option_rows_for_window(
    symbol: &str,
    expiration: NaiveDate,
    start: NaiveDate,
    end: NaiveDate,
    right: &str,
) -> Result<Vec<ResearchStoreOptionRow>> {
    with_default_store(|store| store.option_rows(symbol, expiration, start, end, right))
}

pub fn replace_option_rows_for_window(
    symbol: &str,
    expiration: NaiveDate,
    start: NaiveDate,
    end: NaiveDate,
    right: &str,
    rows: &[ResearchStoreOptionRow],
) -> Result<()> {
    with_default_store(|store| {
        store.replace_option_rows_for_window(symbol, expiration, start, end, right, rows)
    })
}

pub fn import_research_store(
    raw_root: &Path,
    symbols: &[String],
    max_files_per_symbol: Option<usize>,
) -> Result<ResearchStoreImportReport> {
    let mut store = ResearchStore::open_default()?;
    let mut total = ResearchStoreImportReport {
        raw_root: raw_root.display().to_string(),
        symbols: normalized_symbols(symbols, raw_root)?,
        ..ResearchStoreImportReport::default()
    };
    for symbol in total.symbols.clone() {
        let report = store.import_symbol_cache_dir(
            &symbol,
            &raw_root.join(&symbol),
            max_files_per_symbol,
        )?;
        total.files_seen += report.files_seen;
        total.cache_windows_recorded += report.cache_windows_recorded;
        total.files_imported += report.files_imported;
        total.files_failed += report.files_failed;
        total.option_rows_imported += report.option_rows_imported;
    }
    Ok(total)
}

pub fn research_store_perf_check(
    raw_root: &Path,
    symbols: &[String],
) -> Result<ResearchStorePerfReport> {
    let symbols = normalized_symbols(symbols, raw_root)?;
    let mut store = ResearchStore::open_default()?;
    let sync_start = Instant::now();
    let mut files_scanned = 0;
    for symbol in &symbols {
        let report = store.sync_symbol_cache_dir(symbol, &raw_root.join(symbol), None)?;
        files_scanned += report.files_seen;
    }
    let sync_ms = sync_start.elapsed().as_millis();
    let query_start = Instant::now();
    let cache_windows = store.table_count("cache_windows")?;
    let option_rows = store.table_count("option_rows")?;
    let count_query_ms = query_start.elapsed().as_millis();
    Ok(ResearchStorePerfReport {
        raw_root: raw_root.display().to_string(),
        symbols,
        files_scanned,
        sync_ms,
        count_query_ms,
        cache_windows,
        option_rows,
    })
}

fn with_default_store<T>(f: impl FnOnce(&mut ResearchStore) -> Result<T>) -> Result<T> {
    let lock = STORE_WRITE_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = lock
        .lock()
        .map_err(|_| anyhow::anyhow!("research store write lock poisoned"))?;
    let path = default_research_store_path();
    let stores = DEFAULT_STORE_CONNECTIONS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut stores = stores
        .lock()
        .map_err(|_| anyhow::anyhow!("research store connection cache lock poisoned"))?;
    if !stores.contains_key(&path) {
        stores.insert(path.clone(), ResearchStore::open(&path)?);
    }
    let store = stores
        .get_mut(&path)
        .expect("default research store connection was inserted");
    f(store)
}

pub fn flush_default_store_connections() -> Result<()> {
    let lock = STORE_WRITE_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = lock
        .lock()
        .map_err(|_| anyhow::anyhow!("research store write lock poisoned"))?;
    if let Some(stores) = DEFAULT_STORE_CONNECTIONS.get() {
        stores
            .lock()
            .map_err(|_| anyhow::anyhow!("research store connection cache lock poisoned"))?
            .clear();
    }
    Ok(())
}

fn normalized_symbols(symbols: &[String], raw_root: &Path) -> Result<Vec<String>> {
    if !symbols.is_empty() {
        let mut normalized = symbols
            .iter()
            .map(|symbol| symbol.trim().to_uppercase())
            .filter(|symbol| !symbol.is_empty())
            .collect::<Vec<_>>();
        normalized.sort();
        normalized.dedup();
        return Ok(normalized);
    }
    let mut out = Vec::new();
    if raw_root.exists() {
        for entry in
            fs::read_dir(raw_root).with_context(|| format!("read {}", raw_root.display()))?
        {
            let entry = entry.with_context(|| format!("read entry in {}", raw_root.display()))?;
            if entry.path().is_dir()
                && let Some(symbol) = entry.file_name().to_str()
            {
                out.push(symbol.to_uppercase());
            }
        }
    }
    out.sort();
    out.dedup();
    Ok(out)
}

fn cache_window_from_path(path: &Path) -> Option<ResearchStoreCacheWindow> {
    let symbol = path.parent()?.file_name()?.to_str()?.to_uppercase();
    let file_name = path.file_name()?.to_str()?;
    let stem = file_name.strip_suffix(".json")?;
    let (right, rest) = if let Some(rest) = stem.strip_prefix("research_call_") {
        ("call", rest)
    } else if let Some(rest) = stem.strip_prefix("research_") {
        ("put", rest)
    } else {
        return None;
    };
    let (dataset, rest) = if let Some(rest) = rest.strip_prefix("greeks_") {
        ("greeks", rest)
    } else if let Some(rest) = rest.strip_prefix("oi_") {
        ("oi", rest)
    } else {
        return None;
    };
    let mut parts = rest.split('_');
    let expiration = parse_yyyymmdd(parts.next()?)?;
    let start = parse_yyyymmdd(parts.next()?)?;
    let end = parse_yyyymmdd(parts.next()?)?;
    if parts.next().is_some() || start > end {
        return None;
    }
    Some(ResearchStoreCacheWindow {
        symbol,
        right: right.to_owned(),
        dataset: dataset.to_owned(),
        expiration,
        start,
        end,
        source_path: path.to_path_buf(),
    })
}

fn read_oi_map_for_window(
    window: &ResearchStoreCacheWindow,
) -> Result<HashMap<(NaiveDate, String), u32>> {
    let exact_path = sibling_oi_path(window);
    if exact_path.exists() {
        return read_oi_map_path(&exact_path);
    }

    let windows = matching_oi_cache_windows(window)?;
    let selected = cache_window_covering_sequence(&windows, window.start, window.end)
        .with_context(|| {
            format!(
                "missing open-interest cache coverage for {}",
                window_key(window)
            )
        })?;
    let mut out = HashMap::new();
    for oi_window in selected {
        out.extend(read_oi_map_path(&oi_window.source_path)?);
    }
    out.retain(|(date, _strike), _oi| *date >= window.start && *date <= window.end);
    Ok(out)
}

fn read_oi_map_path(path: &Path) -> Result<HashMap<(NaiveDate, String), u32>> {
    let body = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let json: Value =
        serde_json::from_str(&body).with_context(|| format!("parse {}", path.display()))?;
    parse_oi_map(&json)
}

fn matching_oi_cache_windows(
    window: &ResearchStoreCacheWindow,
) -> Result<Vec<ResearchStoreCacheWindow>> {
    let parent = window
        .source_path
        .parent()
        .with_context(|| format!("cache path has no parent: {}", window.source_path.display()))?;
    let mut windows = Vec::new();
    for entry in fs::read_dir(parent).with_context(|| format!("read {}", parent.display()))? {
        let entry = entry.with_context(|| format!("read entry in {}", parent.display()))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(candidate) = cache_window_from_path(&path) else {
            continue;
        };
        if candidate.symbol == window.symbol
            && candidate.right == window.right
            && candidate.dataset == "oi"
            && candidate.expiration == window.expiration
        {
            windows.push(candidate);
        }
    }
    windows.sort_by(|a, b| {
        a.start
            .cmp(&b.start)
            .then_with(|| a.end.cmp(&b.end))
            .then_with(|| a.source_path.cmp(&b.source_path))
    });
    Ok(windows)
}

fn cache_window_covering_sequence(
    windows: &[ResearchStoreCacheWindow],
    start: NaiveDate,
    end: NaiveDate,
) -> Option<Vec<ResearchStoreCacheWindow>> {
    let mut cursor = start;
    let mut selected = Vec::new();
    while cursor <= end {
        let best = windows
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
                    .then_with(|| b.source_path.cmp(&a.source_path))
            })?;
        selected.push(best.clone());
        cursor = best.end + chrono::Duration::days(1);
    }
    Some(selected)
}

fn window_key(window: &ResearchStoreCacheWindow) -> String {
    format!(
        "{} {} {} {}..{}",
        window.symbol,
        window.right,
        date_s(window.expiration),
        date_s(window.start),
        date_s(window.end)
    )
}

fn sibling_oi_path(window: &ResearchStoreCacheWindow) -> PathBuf {
    let prefix = if window.right == "call" {
        "research_call_oi"
    } else {
        "research_oi"
    };
    window.source_path.with_file_name(format!(
        "{}_{}_{}_{}.json",
        prefix,
        yyyymmdd(window.expiration),
        yyyymmdd(window.start),
        yyyymmdd(window.end)
    ))
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

fn parse_greeks_option_rows(
    window: &ResearchStoreCacheWindow,
    json: &Value,
    oi_map: &HashMap<(NaiveDate, String), u32>,
) -> Result<Vec<ResearchStoreOptionRow>> {
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
            if date < window.start || date > window.end {
                continue;
            }
            let bid = number(row, "bid");
            let ask = number(row, "ask");
            if bid <= 0.0 || ask <= 0.0 || ask < bid {
                continue;
            }
            out.push(ResearchStoreOptionRow {
                symbol: window.symbol.clone(),
                date,
                expiration: window.expiration,
                right: window.right.clone(),
                strike,
                bid,
                ask,
                mark: (bid + ask) / 2.0,
                delta: number(row, "delta"),
                implied_vol: number(row, "implied_vol"),
                underlying_price: number(row, "underlying_price"),
                open_interest: *oi_map.get(&(date, strike_key.clone())).unwrap_or(&0),
                source_path: window.source_path.display().to_string(),
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

fn response_row_count(json: &Value) -> usize {
    json.get("response")
        .and_then(Value::as_array)
        .map(|response| {
            response
                .iter()
                .map(|contract| {
                    contract
                        .get("data")
                        .and_then(Value::as_array)
                        .map_or(1, Vec::len)
                })
                .sum()
        })
        .unwrap_or(0)
}

fn covering_sequence_exists(
    windows: &[(NaiveDate, NaiveDate)],
    start: NaiveDate,
    end: NaiveDate,
) -> bool {
    let mut cursor = start;
    while cursor <= end {
        let Some(best_end) = windows
            .iter()
            .filter(|(window_start, window_end)| *window_start <= cursor && *window_end >= cursor)
            .map(|(_window_start, window_end)| *window_end)
            .max()
        else {
            return false;
        };
        cursor = best_end + chrono::Duration::days(1);
    }
    true
}

fn parse_yyyymmdd(value: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y%m%d").ok()
}

fn parse_iso_date(value: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .with_context(|| format!("parse store date {value}"))
}

fn yyyymmdd(date: NaiveDate) -> String {
    date.format("%Y%m%d").to_string()
}

fn date_s(date: NaiveDate) -> String {
    date.format("%Y-%m-%d").to_string()
}

fn now_s() -> String {
    Utc::now().to_rfc3339()
}

fn compact_error(value: &str) -> String {
    const MAX_LEN: usize = 500;
    let mut one_line = value.replace('\n', " ");
    one_line.truncate(MAX_LEN);
    one_line
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live_market::{LiveMarketEngineConfig, build_signal_artifact_live_market_snapshot};
    use crate::live_signal::{
        ApprovedPortfolioConstraints, ApprovedStrategy, LIVE_SIGNAL_SCHEMA_VERSION,
        LiveSignalArtifact, ProductionApproval, ProductionApprovalStatus, SignalStatus,
        TradeSignal,
    };
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn research_store_path_honors_explicit_override() {
        assert_eq!(
            research_store_path_from_env(Some("var/research/offline.duckdb".into())),
            PathBuf::from("var/research/offline.duckdb")
        );
        assert_eq!(
            research_store_path_from_env(Some("".into())),
            PathBuf::from(DEFAULT_RESEARCH_STORE_PATH)
        );
        assert_eq!(
            research_store_path_from_env(None),
            PathBuf::from(DEFAULT_RESEARCH_STORE_PATH)
        );
    }

    #[test]
    fn research_store_skip_cache_sync_flag_is_explicit() {
        assert!(env_flag_enabled(Some("1".into())));
        assert!(env_flag_enabled(Some("true".into())));
        assert!(env_flag_enabled(Some("YES".into())));
        assert!(env_flag_enabled(Some("on".into())));
        assert!(!env_flag_enabled(Some("0".into())));
        assert!(!env_flag_enabled(Some("false".into())));
        assert!(!env_flag_enabled(Some("".into())));
        assert!(!env_flag_enabled(None));
    }

    #[test]
    fn schema_initializes_and_cache_window_upsert_is_idempotent() {
        let db_path = unique_test_path("schema.duckdb");
        let mut store = ResearchStore::open(&db_path).unwrap();
        let raw_dir = unique_test_path("raw");
        fs::create_dir_all(raw_dir.join("NVDA")).unwrap();
        let cache_path = raw_dir
            .join("NVDA")
            .join("research_oi_20250117_20250103_20250109.json");
        fs::write(&cache_path, r#"{"response":[]}"#).unwrap();

        let report = store
            .sync_symbol_cache_dir("NVDA", &raw_dir.join("NVDA"), None)
            .unwrap();
        assert_eq!(report.cache_windows_recorded, 1);
        let report = store
            .sync_symbol_cache_dir("NVDA", &raw_dir.join("NVDA"), None)
            .unwrap();
        assert_eq!(report.cache_windows_recorded, 1);
        assert_eq!(store.table_count("cache_windows").unwrap(), 1);
    }

    #[test]
    fn import_greeks_file_writes_joined_option_rows() {
        let db_path = unique_test_path("import.duckdb");
        let mut store = ResearchStore::open(&db_path).unwrap();
        let raw_dir = unique_test_path("raw-import").join("NVDA");
        fs::create_dir_all(&raw_dir).unwrap();
        let oi_path = raw_dir.join("research_oi_20250117_20250103_20250109.json");
        let greeks_path = raw_dir.join("research_greeks_20250117_20250103_20250109.json");
        fs::write(
            &oi_path,
            serde_json::to_string(&json!({
                "response": [{
                    "contract": {"strike": 100.0},
                    "data": [{"timestamp": "2025-01-03T00:00:00Z", "open_interest": 123}]
                }]
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            &greeks_path,
            serde_json::to_string(&json!({
                "response": [{
                    "contract": {"strike": 100.0},
                    "data": [{
                        "timestamp": "2025-01-03T00:00:00Z",
                        "bid": 1.0,
                        "ask": 1.2,
                        "delta": -0.25,
                        "implied_vol": 0.42,
                        "underlying_price": 105.0
                    }]
                }]
            }))
            .unwrap(),
        )
        .unwrap();

        assert_eq!(store.import_cache_file(&oi_path).unwrap(), 0);
        let rows = store.import_cache_file(&greeks_path).unwrap();
        assert_eq!(rows, 1);
        assert_eq!(store.table_count("option_rows").unwrap(), 1);
        assert!(
            store
                .cache_has_complete_coverage(
                    "NVDA",
                    NaiveDate::from_ymd_opt(2025, 1, 17).unwrap(),
                    NaiveDate::from_ymd_opt(2025, 1, 3).unwrap(),
                    NaiveDate::from_ymd_opt(2025, 1, 9).unwrap(),
                    "put",
                )
                .unwrap()
        );
    }

    #[test]
    fn import_greeks_file_uses_split_oi_coverage() {
        let db_path = unique_test_path("split-import.duckdb");
        let mut store = ResearchStore::open(&db_path).unwrap();
        let raw_dir = unique_test_path("raw-split-import").join("NVDA");
        fs::create_dir_all(&raw_dir).unwrap();
        let oi_first_path = raw_dir.join("research_oi_20250117_20250103_20250105.json");
        let oi_second_path = raw_dir.join("research_oi_20250117_20250106_20250109.json");
        let greeks_path = raw_dir.join("research_greeks_20250117_20250103_20250109.json");
        for (path, date, oi) in [
            (&oi_first_path, "2025-01-03T00:00:00Z", 111_u64),
            (&oi_second_path, "2025-01-07T00:00:00Z", 456_u64),
        ] {
            fs::write(
                path,
                serde_json::to_string(&json!({
                    "response": [{
                        "contract": {"strike": 100.0},
                        "data": [{"timestamp": date, "open_interest": oi}]
                    }]
                }))
                .unwrap(),
            )
            .unwrap();
        }
        fs::write(
            &greeks_path,
            serde_json::to_string(&json!({
                "response": [{
                    "contract": {"strike": 100.0},
                    "data": [{
                        "timestamp": "2025-01-07T00:00:00Z",
                        "bid": 1.0,
                        "ask": 1.2,
                        "delta": -0.25,
                        "implied_vol": 0.42,
                        "underlying_price": 105.0
                    }]
                }]
            }))
            .unwrap(),
        )
        .unwrap();

        assert_eq!(store.import_cache_file(&greeks_path).unwrap(), 1);
        let rows = store
            .option_rows(
                "NVDA",
                NaiveDate::from_ymd_opt(2025, 1, 17).unwrap(),
                NaiveDate::from_ymd_opt(2025, 1, 3).unwrap(),
                NaiveDate::from_ymd_opt(2025, 1, 9).unwrap(),
                "put",
            )
            .unwrap();
        assert_eq!(rows[0].open_interest, 456);
    }

    #[test]
    fn option_rows_replace_is_idempotent_and_queryable() {
        let db_path = unique_test_path("rows.duckdb");
        let mut store = ResearchStore::open(&db_path).unwrap();
        let expiration = NaiveDate::from_ymd_opt(2025, 1, 17).unwrap();
        let start = NaiveDate::from_ymd_opt(2025, 1, 3).unwrap();
        let rows = vec![ResearchStoreOptionRow {
            symbol: "NVDA".to_owned(),
            date: start,
            expiration,
            right: "put".to_owned(),
            strike: 100.0,
            bid: 1.0,
            ask: 1.2,
            mark: 1.1,
            delta: -0.25,
            implied_vol: 0.42,
            underlying_price: 105.0,
            open_interest: 123,
            source_path: "test".to_owned(),
        }];

        store
            .replace_option_rows_for_window("NVDA", expiration, start, start, "put", &rows)
            .unwrap();
        store
            .replace_option_rows_for_window("NVDA", expiration, start, start, "put", &rows)
            .unwrap();

        assert_eq!(
            store
                .option_rows("NVDA", expiration, start, start, "put")
                .unwrap(),
            rows
        );
        assert_eq!(store.table_count("option_rows").unwrap(), 1);
        assert!(
            store
                .option_rows_have_complete_coverage("NVDA", expiration, start, start, "put")
                .unwrap()
        );
    }

    #[test]
    fn live_market_snapshot_persists_audit_rows() {
        let db_path = unique_test_path("live-market.duckdb");
        let source_path = unique_test_path("live-market-source.json");
        let output_path = unique_test_path("live-market-output.json");
        let now = NaiveDate::from_ymd_opt(2026, 6, 30)
            .unwrap()
            .and_hms_opt(15, 0, 0)
            .unwrap()
            .and_utc();
        let approved = test_approved_strategy();
        let source = test_source_artifact(
            approved.clone(),
            now,
            test_trade_signal("2026-06-30", "put_debit_spread"),
        );
        fs::write(&source_path, serde_json::to_string(&source).unwrap()).unwrap();
        let record = build_signal_artifact_live_market_snapshot(LiveMarketEngineConfig {
            approved_strategy: approved,
            source_live_signal: &source_path,
            output: &output_path,
            as_of: NaiveDate::from_ymd_opt(2026, 6, 30).unwrap(),
            max_source_age_seconds: 45,
            now,
        })
        .unwrap();

        let mut store = ResearchStore::open(&db_path).unwrap();
        store.record_live_market_snapshot(&record).unwrap();

        assert_eq!(store.table_count("live_market_snapshots").unwrap(), 1);
        assert_eq!(store.table_count("live_provider_health").unwrap(), 1);
        assert_eq!(store.table_count("live_signal_candidates").unwrap(), 1);
    }

    fn test_approved_strategy() -> ApprovedStrategy {
        ApprovedStrategy {
            strategy_id: "approved_v1".to_owned(),
            profile_name: "profile_v1".to_owned(),
            research_from: None,
            research_gate_capital_budget: None,
            live_detector_lookback_days: Some(30),
            symbols: vec!["TSLA".to_owned()],
            portfolio_constraints: ApprovedPortfolioConstraints {
                capital_budget: 10_000.0,
                max_symbol_allocation_pct: 0.5,
                max_open_positions: 2,
                max_positions_per_symbol: 1,
                max_total_trades_per_symbol: None,
                portfolio_drawdown_cooldown_trigger_pct: None,
                portfolio_drawdown_cooldown_days: 0,
                symbol_drawdown_cooldown_trigger_pct: None,
                symbol_drawdown_cooldown_days: 0,
            },
            allowed_live_strategies: vec!["put_debit_spread".to_owned()],
            canary_risk_policy_id: "risk_v1".to_owned(),
            production_approval: Some(ProductionApproval {
                status: ProductionApprovalStatus::OperatorRiskOverride,
                approved_at: NaiveDate::from_ymd_opt(2026, 6, 30)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    .and_utc(),
                approved_by: "test".to_owned(),
                reason: "test approval".to_owned(),
                source_canary_status: Some("blocked".to_owned()),
                max_order_max_loss: Some(150.0),
            }),
        }
    }

    fn test_trade_signal(entry_date: &str, strategy: &str) -> TradeSignal {
        TradeSignal {
            status: SignalStatus::NewEntry,
            symbol: "TSLA".to_owned(),
            strategy: strategy.to_owned(),
            entry_date: Some(entry_date.to_owned()),
            exit_date: Some(entry_date.to_owned()),
            expiration: Some("2026-07-02".to_owned()),
            short_put: None,
            short_strike: Some(350.0),
            long_strike: Some(355.0),
            wheel_covered_call_expiration: None,
            wheel_covered_call_strike: None,
            width: Some(5.0),
            entry_credit: Some(-1.0),
            max_loss: Some(100.0),
            reserve: None,
            reserve_basis: None,
            pnl: None,
            dte_entry: Some(2),
            days_held: Some(0),
            exit_reason: None,
            short_delta: None,
            long_delta: None,
            short_oi: None,
            long_oi: None,
            short_iv: None,
            long_iv: None,
            underlying_price: None,
            execution_rules: None,
        }
    }

    fn test_source_artifact(
        approved_strategy: ApprovedStrategy,
        generated_at: chrono::DateTime<Utc>,
        signal: TradeSignal,
    ) -> LiveSignalArtifact {
        LiveSignalArtifact {
            schema_version: LIVE_SIGNAL_SCHEMA_VERSION,
            strategy_id: approved_strategy.strategy_id.clone(),
            profile_name: approved_strategy.profile_name.clone(),
            as_of: NaiveDate::from_ymd_opt(2026, 6, 30).unwrap(),
            generated_at,
            market_data_through: NaiveDate::from_ymd_opt(2026, 6, 30).unwrap(),
            approved_strategy,
            signals: vec![signal.clone()],
            selected_signal: Some(signal),
            source_run_id: "source_run".to_owned(),
            source_report: "source_report".to_owned(),
            source_research_from: None,
            source_gate_pass: None,
            source_gate_reason: None,
            detector_research_gate_enforced: false,
        }
    }

    fn unique_test_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("spreadfoundry-store-test-{nanos}-{name}"))
    }
}
