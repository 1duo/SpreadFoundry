use anyhow::{Context, Result};
use chrono::{Datelike, Duration, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration as StdDuration;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResearchRequest {
    pub symbol: String,
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub max_expirations: Option<usize>,
    pub force_refresh: bool,
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
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResearchReport {
    pub run_id: String,
    pub symbol: String,
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub expirations_discovered: usize,
    pub expirations_loaded: usize,
    pub rows_loaded: usize,
    pub profiles: Vec<ProfileResult>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProfileResult {
    pub profile: ResearchProfile,
    pub candidates: usize,
    pub trades: Vec<ResearchTrade>,
    pub metrics: ResearchMetrics,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResearchMetrics {
    pub trades: usize,
    pub total_pnl: f64,
    pub total_max_loss: f64,
    pub avg_return_on_risk: f64,
    pub median_return_on_risk: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub max_drawdown: f64,
    pub avg_days_held: f64,
    pub median_days_held: f64,
    pub trades_per_year: f64,
    pub best_trade_pnl: f64,
    pub worst_trade_pnl: f64,
    pub score: f64,
    pub yearly: BTreeMap<i32, YearMetrics>,
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
}

#[derive(Clone, Debug)]
struct OptionDay {
    date: NaiveDate,
    strike: f64,
    bid: f64,
    ask: f64,
    delta: f64,
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
}

pub async fn run_nvda_research(request: ResearchRequest) -> Result<ResearchReport> {
    let raw_dir = PathBuf::from("data/raw/theta").join(&request.symbol);
    let run_id = format!("nvda-research-{}", Utc::now().format("%Y%m%dT%H%M%S%.9fZ"));
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

    let mut rows_by_expiration = HashMap::new();
    let mut rows_loaded = 0;
    for expiration in &candidate_expirations {
        let start = request
            .from
            .max(*expiration - Duration::days(max_entry_dte));
        let exit_grace_end = *expiration - Duration::days(min_force_close_dte) + Duration::days(7);
        let end = request.to.min(exit_grace_end);
        if start > end {
            continue;
        }
        println!("loading {} {}..{}", expiration, start, end);
        let rows = load_expiration_rows(
            &request.symbol,
            *expiration,
            start,
            end,
            &raw_dir,
            request.force_refresh,
        )
        .await?;
        rows_loaded += rows.len();
        if !rows.is_empty() {
            rows_by_expiration.insert(*expiration, rows);
        }
    }

    let mut profile_results = Vec::new();
    for profile in profiles {
        let candidates = generate_candidates(&rows_by_expiration, &profile);
        let trades = simulate_non_overlapping(&candidates, &rows_by_expiration, &profile);
        let metrics = metrics(&trades, request.from, request.to);
        profile_results.push(ProfileResult {
            profile,
            candidates: candidates.len(),
            trades,
            metrics,
        });
    }
    profile_results.sort_by(|a, b| b.metrics.score.total_cmp(&a.metrics.score));

    let report = ResearchReport {
        run_id: run_id.clone(),
        symbol: request.symbol,
        from: request.from,
        to: request.to,
        expirations_discovered: expirations.len(),
        expirations_loaded: rows_by_expiration.len(),
        rows_loaded,
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
    let exp = yyyymmdd(expiration);
    let start_s = yyyymmdd(start);
    let end_s = yyyymmdd(end);
    let greeks_path = raw_dir.join(format!("research_greeks_{exp}_{start_s}_{end_s}.json"));
    let oi_path = raw_dir.join(format!("research_oi_{exp}_{start_s}_{end_s}.json"));
    let greeks_url = format!(
        "http://127.0.0.1:25503/v3/option/history/greeks/eod?symbol={symbol}&expiration={exp}&right=put&start_date={start_s}&end_date={end_s}&format=json"
    );
    let oi_url = format!(
        "http://127.0.0.1:25503/v3/option/history/open_interest?symbol={symbol}&expiration={exp}&right=put&start_date={start_s}&end_date={end_s}&format=json"
    );
    let greeks = match fetch_cached_json(&greeks_url, &greeks_path, force_refresh).await {
        Ok(json) => json,
        Err(_) => return Ok(Vec::new()),
    };
    let oi = match fetch_cached_json(&oi_url, &oi_path, force_refresh).await {
        Ok(json) => json,
        Err(_) => Value::Object(Default::default()),
    };
    let oi_map = parse_oi_map(&oi)?;
    parse_greeks_rows(&greeks, &oi_map)
}

async fn fetch_cached_json(url: &str, path: &Path, force_refresh: bool) -> Result<Value> {
    if path.exists() && !force_refresh {
        let body = fs::read_to_string(path)?;
        return serde_json::from_str(&body).with_context(|| format!("parsing {}", path.display()));
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let client = reqwest::Client::builder()
        .timeout(StdDuration::from_secs(20))
        .build()?;
    let body = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("requesting {url}"))?
        .error_for_status()
        .with_context(|| format!("ThetaData returned error for {url}"))?
        .text()
        .await?;
    let json: Value = serde_json::from_str(&body)
        .with_context(|| format!("ThetaData did not return JSON for {url}: {}", body.trim()))?;
    fs::write(path, serde_json::to_string_pretty(&json)?)?;
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
    NaiveDate::parse_from_str(&ts[0..10], "%Y-%m-%d").ok()
}

fn number(row: &Value, key: &str) -> f64 {
    row.get(key).and_then(Value::as_f64).unwrap_or(0.0)
}

fn generate_candidates(
    rows_by_expiration: &HashMap<NaiveDate, Vec<OptionDay>>,
    profile: &ResearchProfile,
) -> Vec<Candidate> {
    let mut candidates = Vec::new();
    for (expiration, rows) in rows_by_expiration {
        let mut by_date: BTreeMap<NaiveDate, Vec<&OptionDay>> = BTreeMap::new();
        for row in rows {
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
                {
                    continue;
                }
                for long in &day_rows {
                    if long.strike >= short.strike
                        || long.open_interest < profile.min_long_oi
                        || !quote_width_allowed(long, profile)
                    {
                        continue;
                    }
                    let width = short.strike - long.strike;
                    if width < profile.min_width || width > profile.max_width {
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
                    });
                }
            }
        }
    }
    candidates.sort_by(|a, b| {
        a.entry_date.cmp(&b.entry_date).then_with(|| {
            b.return_on_risk
                .total_cmp(&a.return_on_risk)
                .then_with(|| b.credit.total_cmp(&a.credit))
        })
    });
    candidates
}

fn simulate_non_overlapping(
    candidates: &[Candidate],
    rows_by_expiration: &HashMap<NaiveDate, Vec<OptionDay>>,
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
        day_candidates.sort_by(|a, b| {
            b.return_on_risk
                .total_cmp(&a.return_on_risk)
                .then_with(|| b.credit.total_cmp(&a.credit))
        });
        for candidate in day_candidates {
            if let Some(trade) = simulate_candidate(candidate, &lookup, profile) {
                next_entry_date = trade.exit_date + Duration::days(1);
                trades.push(trade);
                break;
            }
        }
    }
    trades
}

fn build_lookup(
    rows_by_expiration: &HashMap<NaiveDate, Vec<OptionDay>>,
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
        let dte = (candidate.expiration - *date).num_days();
        let Some(long) = long_rows.get(date) else {
            continue;
        };
        let debit = (short.ask - long.bid).clamp(0.0, candidate.width);
        let reason = if debit >= stop_debit {
            Some("stop_loss")
        } else if debit <= take_profit_debit {
            Some("take_profit")
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

fn metrics(trades: &[ResearchTrade], from: NaiveDate, to: NaiveDate) -> ResearchMetrics {
    if trades.is_empty() {
        return ResearchMetrics {
            trades: 0,
            total_pnl: 0.0,
            total_max_loss: 0.0,
            avg_return_on_risk: 0.0,
            median_return_on_risk: 0.0,
            win_rate: 0.0,
            profit_factor: 0.0,
            max_drawdown: 0.0,
            avg_days_held: 0.0,
            median_days_held: 0.0,
            trades_per_year: 0.0,
            best_trade_pnl: 0.0,
            worst_trade_pnl: 0.0,
            score: -1_000_000.0,
            yearly: BTreeMap::new(),
        };
    }
    let mut sorted = trades.to_vec();
    sorted.sort_by_key(|trade| trade.exit_date);
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
    let max_drawdown = max_drawdown(&sorted);
    let years = ((to - from).num_days().max(1) as f64) / 365.25;
    let score = total_pnl / total_max_loss.max(1.0) - 2.0 * max_drawdown;
    ResearchMetrics {
        trades: sorted.len(),
        total_pnl,
        total_max_loss,
        avg_return_on_risk: mean(&returns),
        median_return_on_risk: median(returns),
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
        yearly: yearly_metrics(&sorted),
    }
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
        "# SpreadFoundry NVDA Research {}\n\n",
        report.run_id
    ));
    out.push_str(&format!(
        "- Window: `{}` to `{}`\n- Expirations discovered: `{}`\n- Expirations loaded: `{}`\n- EOD rows loaded: `{}`\n\n",
        report.from, report.to, report.expirations_discovered, report.expirations_loaded, report.rows_loaded
    ));
    out.push_str("| Rank | Profile | Candidates | Trades | PnL | Avg ROR | Win Rate | Profit Factor | Max DD | Avg Hold | Trades/Yr | Score |\n");
    out.push_str("|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for (idx, result) in report.profiles.iter().enumerate() {
        let m = &result.metrics;
        out.push_str(&format!(
            "| {} | {} | {} | {} | {:.2} | {:.3} | {:.1}% | {:.2} | {:.3} | {:.1} | {:.1} | {:.4} |\n",
            idx + 1,
            result.profile.name,
            result.candidates,
            m.trades,
            m.total_pnl,
            m.avg_return_on_risk,
            m.win_rate * 100.0,
            m.profit_factor,
            m.max_drawdown,
            m.avg_days_held,
            m.trades_per_year,
            m.score
        ));
    }
    if let Some(best) = report.profiles.first() {
        out.push_str("\n## Best Profile Trades\n\n");
        out.push_str(
            "| Entry | Exit | Exp | Short | Long | Credit | Exit Debit | PnL | ROR | Reason |\n",
        );
        out.push_str("|---|---|---|---:|---:|---:|---:|---:|---:|---|\n");
        for trade in best.trades.iter().take(50) {
            out.push_str(&format!(
                "| {} | {} | {} | {:.0}P | {:.0}P | {:.2} | {:.2} | {:.2} | {:.3} | {} |\n",
                trade.entry_date,
                trade.exit_date,
                trade.expiration,
                trade.short_put,
                trade.long_put,
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
}
