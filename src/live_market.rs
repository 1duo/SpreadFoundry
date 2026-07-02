use crate::live_signal::{
    ApprovedStrategy, LIVE_SIGNAL_SCHEMA_VERSION, LiveSignalArtifact, SignalStatus, TradeSignal,
};
use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::time::Instant;

pub const LIVE_MARKET_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
pub const DEFAULT_LIVE_MARKET_INTERVAL_SECONDS: u64 = 30;
pub const DEFAULT_LIVE_MARKET_MAX_SOURCE_AGE_SECONDS: u64 = 420;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveMarketProviderKind {
    SignalArtifact,
}

impl std::fmt::Display for LiveMarketProviderKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SignalArtifact => f.write_str("signal_artifact"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LiveMarketEngineConfig<'a> {
    pub approved_strategy: ApprovedStrategy,
    pub source_live_signal: &'a Path,
    pub output: &'a Path,
    pub as_of: NaiveDate,
    pub max_source_age_seconds: u64,
    pub now: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LiveMarketSnapshotRecord {
    pub schema_version: u32,
    pub snapshot_id: String,
    pub provider: LiveMarketProviderKind,
    pub observed_at: DateTime<Utc>,
    pub as_of: NaiveDate,
    pub approved_strategy_id: String,
    pub profile_name: String,
    pub source_live_signal: String,
    pub output: String,
    pub provider_health: LiveMarketProviderHealth,
    pub decision: LiveMarketDecision,
    pub candidates: Vec<LiveMarketCandidateDecision>,
    pub artifact: LiveSignalArtifact,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LiveMarketProviderHealth {
    pub provider: LiveMarketProviderKind,
    pub ok: bool,
    pub status: String,
    pub checked_at: DateTime<Utc>,
    pub latency_ms: u128,
    pub symbols_requested: usize,
    pub symbols_ready: usize,
    pub max_source_age_seconds: u64,
    pub source_age_seconds: Option<u64>,
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LiveMarketDecision {
    pub status: String,
    pub reason: String,
    pub selected_signal: Option<TradeSignal>,
    pub candidates_seen: usize,
    pub candidates_accepted: usize,
    pub candidates_rejected: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LiveMarketCandidateDecision {
    pub candidate_rank: usize,
    pub symbol: String,
    pub strategy: String,
    pub signal_status: SignalStatus,
    pub entry_date: Option<String>,
    pub expiration: Option<String>,
    pub max_loss: Option<f64>,
    pub decision_status: String,
    pub reason: String,
    pub signal: TradeSignal,
}

pub fn build_signal_artifact_live_market_snapshot(
    config: LiveMarketEngineConfig<'_>,
) -> Result<LiveMarketSnapshotRecord> {
    let started = Instant::now();
    let source_result = read_source_live_signal(config.source_live_signal);
    let provider_health = provider_health_from_source(
        &config.approved_strategy,
        &source_result,
        config.max_source_age_seconds,
        config.now,
        started.elapsed().as_millis(),
    );
    let source_artifact = source_result.ok();
    let source_fresh = provider_health.ok;
    let source_strategy_matches = source_artifact
        .as_ref()
        .is_some_and(|artifact| artifact.strategy_id == config.approved_strategy.strategy_id);
    let source_profile_matches = source_artifact
        .as_ref()
        .is_some_and(|artifact| artifact.profile_name == config.approved_strategy.profile_name);
    let source_market_data_through = source_artifact
        .as_ref()
        .map(|artifact| artifact.market_data_through)
        .unwrap_or(config.as_of);
    let source_market_data_current = source_artifact
        .as_ref()
        .is_some_and(|artifact| artifact.market_data_through >= config.as_of);
    let source_run_id = source_artifact
        .as_ref()
        .map(|artifact| artifact.source_run_id.clone())
        .unwrap_or_else(|| "missing_source".to_owned());
    let source_report = source_artifact
        .as_ref()
        .map(|artifact| artifact.source_report.clone())
        .unwrap_or_else(|| config.source_live_signal.display().to_string());
    let approved_symbols = approved_symbol_set(&config.approved_strategy);
    let gate_context = CandidateGateContext {
        approved_strategy: &config.approved_strategy,
        approved_symbols: &approved_symbols,
        as_of: config.as_of,
        source_fresh,
        source_strategy_matches,
        source_profile_matches,
        source_market_data_current,
        provider_reason: provider_health.reason.as_str(),
    };
    let mut retained_signals = Vec::new();
    let mut candidate_decisions = Vec::new();

    if let Some(source_artifact) = &source_artifact {
        for (idx, signal) in source_artifact.signals.iter().cloned().enumerate() {
            let decision = evaluate_candidate(idx, signal.clone(), &gate_context);
            if live_artifact_should_retain_signal(&decision) {
                retained_signals.push(signal);
            }
            candidate_decisions.push(decision);
        }
    }

    let selected_signal = candidate_decisions
        .iter()
        .find(|candidate| candidate.decision_status == "accepted")
        .map(|candidate| candidate.signal.clone());
    let candidates_accepted = usize::from(selected_signal.is_some());
    let candidates_rejected = candidate_decisions
        .iter()
        .filter(|candidate| candidate.decision_status == "rejected")
        .count();
    let decision = if let Some(selected) = &selected_signal {
        LiveMarketDecision {
            status: "selected".to_owned(),
            reason: format!(
                "selected {} {} from fresh provider snapshot",
                selected.symbol, selected.strategy
            ),
            selected_signal: Some(selected.clone()),
            candidates_seen: candidate_decisions.len(),
            candidates_accepted,
            candidates_rejected,
        }
    } else if provider_health.ok {
        LiveMarketDecision {
            status: "no_signal".to_owned(),
            reason: "fresh provider snapshot contained no approved same-day entry".to_owned(),
            selected_signal: None,
            candidates_seen: candidate_decisions.len(),
            candidates_accepted,
            candidates_rejected,
        }
    } else {
        LiveMarketDecision {
            status: "blocked".to_owned(),
            reason: provider_health.reason.clone(),
            selected_signal: None,
            candidates_seen: candidate_decisions.len(),
            candidates_accepted,
            candidates_rejected,
        }
    };

    let snapshot_id = live_market_snapshot_id(config.now);
    let artifact = LiveSignalArtifact {
        schema_version: LIVE_SIGNAL_SCHEMA_VERSION,
        strategy_id: config.approved_strategy.strategy_id.clone(),
        profile_name: config.approved_strategy.profile_name.clone(),
        as_of: config.as_of,
        generated_at: config.now,
        market_data_through: source_market_data_through,
        approved_strategy: config.approved_strategy.clone(),
        signals: retained_signals,
        selected_signal: selected_signal.clone(),
        source_run_id,
        source_report: format!(
            "live_market_engine:{}:{}",
            config.source_live_signal.display(),
            source_report
        ),
    };
    artifact.validate_contract()?;

    Ok(LiveMarketSnapshotRecord {
        schema_version: LIVE_MARKET_SNAPSHOT_SCHEMA_VERSION,
        snapshot_id,
        provider: LiveMarketProviderKind::SignalArtifact,
        observed_at: config.now,
        as_of: config.as_of,
        approved_strategy_id: config.approved_strategy.strategy_id,
        profile_name: config.approved_strategy.profile_name,
        source_live_signal: config.source_live_signal.display().to_string(),
        output: config.output.display().to_string(),
        provider_health,
        decision,
        candidates: candidate_decisions,
        artifact,
    })
}

fn read_source_live_signal(path: &Path) -> Result<LiveSignalArtifact> {
    let artifact: LiveSignalArtifact = serde_json::from_str(
        &fs::read_to_string(path)
            .with_context(|| format!("read source live signal {}", path.display()))?,
    )
    .with_context(|| format!("parse source live signal {}", path.display()))?;
    artifact
        .validate_contract()
        .with_context(|| format!("validate source live signal {}", path.display()))?;
    Ok(artifact)
}

fn provider_health_from_source(
    approved_strategy: &ApprovedStrategy,
    source_result: &Result<LiveSignalArtifact>,
    max_source_age_seconds: u64,
    now: DateTime<Utc>,
    latency_ms: u128,
) -> LiveMarketProviderHealth {
    match source_result {
        Ok(artifact) => {
            let source_age = live_source_age_seconds(artifact.generated_at, now);
            let ready_symbols = artifact
                .signals
                .iter()
                .filter(|signal| {
                    approved_strategy
                        .symbols
                        .iter()
                        .any(|symbol| symbol.eq_ignore_ascii_case(&signal.symbol))
                })
                .map(|signal| signal.symbol.to_ascii_uppercase())
                .collect::<BTreeSet<_>>()
                .len();
            match source_age {
                Ok(age) if age <= max_source_age_seconds => LiveMarketProviderHealth {
                    provider: LiveMarketProviderKind::SignalArtifact,
                    ok: true,
                    status: "ok".to_owned(),
                    checked_at: now,
                    latency_ms,
                    symbols_requested: approved_strategy.symbols.len(),
                    symbols_ready: ready_symbols,
                    max_source_age_seconds,
                    source_age_seconds: Some(age),
                    reason: "source live signal is fresh".to_owned(),
                },
                Ok(age) => LiveMarketProviderHealth {
                    provider: LiveMarketProviderKind::SignalArtifact,
                    ok: false,
                    status: "source_stale".to_owned(),
                    checked_at: now,
                    latency_ms,
                    symbols_requested: approved_strategy.symbols.len(),
                    symbols_ready: ready_symbols,
                    max_source_age_seconds,
                    source_age_seconds: Some(age),
                    reason: format!(
                        "source live signal age {age}s exceeds max {max_source_age_seconds}s"
                    ),
                },
                Err(err) => LiveMarketProviderHealth {
                    provider: LiveMarketProviderKind::SignalArtifact,
                    ok: false,
                    status: "source_time_invalid".to_owned(),
                    checked_at: now,
                    latency_ms,
                    symbols_requested: approved_strategy.symbols.len(),
                    symbols_ready: ready_symbols,
                    max_source_age_seconds,
                    source_age_seconds: None,
                    reason: err.to_string(),
                },
            }
        }
        Err(err) => LiveMarketProviderHealth {
            provider: LiveMarketProviderKind::SignalArtifact,
            ok: false,
            status: "source_unavailable".to_owned(),
            checked_at: now,
            latency_ms,
            symbols_requested: approved_strategy.symbols.len(),
            symbols_ready: 0,
            max_source_age_seconds,
            source_age_seconds: None,
            reason: err.to_string(),
        },
    }
}

fn live_artifact_should_retain_signal(decision: &LiveMarketCandidateDecision) -> bool {
    matches!(
        decision.decision_status.as_str(),
        "accepted" | "management" | "ignored"
    )
}

struct CandidateGateContext<'a> {
    approved_strategy: &'a ApprovedStrategy,
    approved_symbols: &'a BTreeSet<String>,
    as_of: NaiveDate,
    source_fresh: bool,
    source_strategy_matches: bool,
    source_profile_matches: bool,
    source_market_data_current: bool,
    provider_reason: &'a str,
}

fn evaluate_candidate(
    idx: usize,
    signal: TradeSignal,
    context: &CandidateGateContext<'_>,
) -> LiveMarketCandidateDecision {
    let (decision_status, reason) = candidate_decision_reason(&signal, context);
    LiveMarketCandidateDecision {
        candidate_rank: idx,
        symbol: signal.symbol.clone(),
        strategy: signal.strategy.clone(),
        signal_status: signal.status,
        entry_date: signal.entry_date.clone(),
        expiration: signal.expiration.clone(),
        max_loss: signal.max_loss,
        decision_status,
        reason,
        signal,
    }
}

fn candidate_decision_reason(
    signal: &TradeSignal,
    context: &CandidateGateContext<'_>,
) -> (String, String) {
    if !context
        .approved_symbols
        .contains(&signal.symbol.to_ascii_uppercase())
    {
        return (
            "invalid".to_owned(),
            format!("{} is not in the approved symbol list", signal.symbol),
        );
    }
    if !context
        .approved_strategy
        .allowed_live_strategies
        .iter()
        .any(|strategy| strategy == &signal.strategy)
    {
        return (
            "rejected".to_owned(),
            format!("{} is not approved for live execution", signal.strategy),
        );
    }
    if !context.source_fresh {
        return ("rejected".to_owned(), context.provider_reason.to_owned());
    }
    if !context.source_strategy_matches {
        return (
            "rejected".to_owned(),
            "source strategy_id does not match approved strategy".to_owned(),
        );
    }
    if !context.source_profile_matches {
        return (
            "rejected".to_owned(),
            "source profile_name does not match approved profile".to_owned(),
        );
    }
    if signal.status == SignalStatus::AlreadyOpen {
        return (
            "management".to_owned(),
            "already-open signal retained for execution worker lifecycle management".to_owned(),
        );
    }
    if signal.status != SignalStatus::NewEntry {
        return (
            "ignored".to_owned(),
            "non-entry signal retained for audit only".to_owned(),
        );
    }
    let as_of_string = context.as_of.to_string();
    if signal.entry_date.as_deref() != Some(as_of_string.as_str()) {
        return (
            "rejected".to_owned(),
            format!(
                "entry_date does not match live engine as_of {}",
                context.as_of
            ),
        );
    }
    // Defense in depth against stale detection data: a same-day entry must be
    // backed by market data through the live engine's session date. The
    // execution worker enforces the same rule, but the engine is the
    // production artifact writer and should not emit a selected entry it
    // already knows is stale.
    if !context.source_market_data_current {
        return (
            "rejected".to_owned(),
            format!(
                "new entry requires source market_data_through >= live engine as_of {}",
                context.as_of
            ),
        );
    }
    let Some(approval) = context.approved_strategy.production_approval.as_ref() else {
        return (
            "rejected".to_owned(),
            "selected live entry requires explicit production approval".to_owned(),
        );
    };
    if let Err(err) = approval.validate_selected_signal(signal) {
        return ("rejected".to_owned(), err.to_string());
    }
    (
        "accepted".to_owned(),
        "approved same-day entry passed live engine production gate".to_owned(),
    )
}

fn approved_symbol_set(approved_strategy: &ApprovedStrategy) -> BTreeSet<String> {
    approved_strategy
        .symbols
        .iter()
        .map(|symbol| symbol.to_ascii_uppercase())
        .collect()
}

fn live_source_age_seconds(generated_at: DateTime<Utc>, now: DateTime<Utc>) -> Result<u64> {
    let age = now.signed_duration_since(generated_at).num_seconds();
    if age < 0 {
        anyhow::bail!(
            "source live signal generated_at {} is in the future",
            generated_at.to_rfc3339()
        );
    }
    Ok(age as u64)
}

fn live_market_snapshot_id(now: DateTime<Utc>) -> String {
    format!(
        "live_market_{}_{}",
        now.timestamp_millis(),
        std::process::id()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::live_signal::{
        ApprovedPortfolioConstraints, ProductionApproval, ProductionApprovalStatus,
    };

    fn approved_strategy() -> ApprovedStrategy {
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

    fn trade_signal(entry_date: &str, strategy: &str, max_loss: f64) -> TradeSignal {
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
            max_loss: Some(max_loss),
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

    fn source_artifact(
        approved_strategy: ApprovedStrategy,
        generated_at: DateTime<Utc>,
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
        }
    }

    #[test]
    fn live_market_snapshot_selects_fresh_approved_entry() {
        let dir =
            std::env::temp_dir().join(format!("spreadfoundry-live-market-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let source_path = dir.join("source-selects.json");
        let output_path = dir.join("output-selects.json");
        let now = NaiveDate::from_ymd_opt(2026, 6, 30)
            .unwrap()
            .and_hms_opt(15, 0, 0)
            .unwrap()
            .and_utc();
        let approved = approved_strategy();
        let source = source_artifact(
            approved.clone(),
            now,
            trade_signal("2026-06-30", "put_debit_spread", 100.0),
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

        assert_eq!(record.decision.status, "selected");
        assert!(record.artifact.selected_signal.is_some());
        assert_eq!(record.provider_health.status, "ok");
        let _ = fs::remove_file(source_path);
    }

    #[test]
    fn live_market_snapshot_rejects_stale_source_fail_closed() {
        let dir =
            std::env::temp_dir().join(format!("spreadfoundry-live-market-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let source_path = dir.join("source-stale.json");
        let output_path = dir.join("output-stale.json");
        let now = NaiveDate::from_ymd_opt(2026, 6, 30)
            .unwrap()
            .and_hms_opt(15, 0, 0)
            .unwrap()
            .and_utc();
        let approved = approved_strategy();
        let stale = now - chrono::Duration::seconds(120);
        let source = source_artifact(
            approved.clone(),
            stale,
            trade_signal("2026-06-30", "put_debit_spread", 100.0),
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

        assert_eq!(record.decision.status, "blocked");
        assert!(record.artifact.selected_signal.is_none());
        assert!(record.artifact.signals.is_empty());
        assert_eq!(record.provider_health.status, "source_stale");
        assert_eq!(record.candidates[0].decision_status, "rejected");
        let _ = fs::remove_file(source_path);
    }

    #[test]
    fn live_market_snapshot_rejects_stale_market_data_for_new_entry() {
        let dir =
            std::env::temp_dir().join(format!("spreadfoundry-live-market-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let source_path = dir.join("source-stale-market-data.json");
        let output_path = dir.join("output-stale-market-data.json");
        let now = NaiveDate::from_ymd_opt(2026, 6, 30)
            .unwrap()
            .and_hms_opt(15, 0, 0)
            .unwrap()
            .and_utc();
        let approved = approved_strategy();
        let mut source = source_artifact(
            approved.clone(),
            now,
            trade_signal("2026-06-30", "put_debit_spread", 100.0),
        );
        source.market_data_through = NaiveDate::from_ymd_opt(2026, 6, 29).unwrap();
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

        assert_eq!(record.decision.status, "no_signal");
        assert!(record.artifact.selected_signal.is_none());
        assert_eq!(record.candidates[0].decision_status, "rejected");
        assert!(record.candidates[0].reason.contains("market_data_through"));
        let _ = fs::remove_file(source_path);
    }

    #[test]
    fn live_market_snapshot_rejects_unapproved_wheel() {
        let dir =
            std::env::temp_dir().join(format!("spreadfoundry-live-market-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let source_path = dir.join("source-wheel.json");
        let output_path = dir.join("output-wheel.json");
        let now = NaiveDate::from_ymd_opt(2026, 6, 30)
            .unwrap()
            .and_hms_opt(15, 0, 0)
            .unwrap()
            .and_utc();
        let mut source_approved = approved_strategy();
        source_approved
            .allowed_live_strategies
            .push("wheel".to_owned());
        let source = source_artifact(
            source_approved,
            now,
            trade_signal("2026-06-30", "wheel", 100.0),
        );
        fs::write(&source_path, serde_json::to_string(&source).unwrap()).unwrap();

        let record = build_signal_artifact_live_market_snapshot(LiveMarketEngineConfig {
            approved_strategy: approved_strategy(),
            source_live_signal: &source_path,
            output: &output_path,
            as_of: NaiveDate::from_ymd_opt(2026, 6, 30).unwrap(),
            max_source_age_seconds: 45,
            now,
        })
        .unwrap();

        assert_eq!(record.decision.status, "no_signal");
        assert!(record.artifact.selected_signal.is_none());
        assert!(record.artifact.signals.is_empty());
        assert_eq!(record.candidates[0].decision_status, "rejected");
        assert!(record.candidates[0].reason.contains("not approved"));
        let _ = fs::remove_file(source_path);
    }
}
