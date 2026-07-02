use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub const LIVE_SIGNAL_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ApprovedStrategy {
    pub strategy_id: String,
    pub profile_name: String,
    #[serde(default)]
    pub research_from: Option<NaiveDate>,
    #[serde(default)]
    pub research_gate_capital_budget: Option<f64>,
    #[serde(default)]
    pub live_detector_lookback_days: Option<i64>,
    pub symbols: Vec<String>,
    pub portfolio_constraints: ApprovedPortfolioConstraints,
    pub allowed_live_strategies: Vec<String>,
    pub canary_risk_policy_id: String,
    #[serde(default)]
    pub production_approval: Option<ProductionApproval>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProductionApprovalStatus {
    CanaryApproved,
    OperatorRiskOverride,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProductionApproval {
    pub status: ProductionApprovalStatus,
    pub approved_at: DateTime<Utc>,
    pub approved_by: String,
    pub reason: String,
    #[serde(default)]
    pub source_canary_status: Option<String>,
    #[serde(default)]
    pub max_order_max_loss: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LiveExecutionRules {
    pub take_profit_pct: f64,
    pub stop_loss_multiple: f64,
    pub force_close_dte: i64,
    pub max_hold_days: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ApprovedPortfolioConstraints {
    pub capital_budget: f64,
    pub max_symbol_allocation_pct: f64,
    pub max_open_positions: usize,
    pub max_positions_per_symbol: usize,
    pub max_total_trades_per_symbol: Option<usize>,
    pub portfolio_drawdown_cooldown_trigger_pct: Option<f64>,
    pub portfolio_drawdown_cooldown_days: i64,
    pub symbol_drawdown_cooldown_trigger_pct: Option<f64>,
    pub symbol_drawdown_cooldown_days: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalStatus {
    NewEntry,
    AlreadyOpen,
    RecentClosed,
}

impl SignalStatus {
    pub fn is_orderable(self) -> bool {
        self == Self::NewEntry
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::NewEntry => "new_entry",
            Self::AlreadyOpen => "already_open",
            Self::RecentClosed => "recent_closed",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TradeSignal {
    pub status: SignalStatus,
    pub symbol: String,
    pub strategy: String,
    pub entry_date: Option<String>,
    pub exit_date: Option<String>,
    pub expiration: Option<String>,
    pub short_put: Option<f64>,
    pub short_strike: Option<f64>,
    pub long_strike: Option<f64>,
    #[serde(default)]
    pub wheel_covered_call_expiration: Option<String>,
    #[serde(default)]
    pub wheel_covered_call_strike: Option<f64>,
    pub width: Option<f64>,
    pub entry_credit: Option<f64>,
    pub max_loss: Option<f64>,
    pub reserve: Option<f64>,
    pub reserve_basis: Option<String>,
    pub pnl: Option<f64>,
    pub dte_entry: Option<i64>,
    pub days_held: Option<i64>,
    pub exit_reason: Option<String>,
    pub short_delta: Option<f64>,
    pub long_delta: Option<f64>,
    pub short_oi: Option<u32>,
    pub long_oi: Option<u32>,
    pub short_iv: Option<f64>,
    pub long_iv: Option<f64>,
    pub underlying_price: Option<f64>,
    #[serde(default)]
    pub execution_rules: Option<LiveExecutionRules>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LiveSignalArtifact {
    pub schema_version: u32,
    pub strategy_id: String,
    pub profile_name: String,
    pub as_of: NaiveDate,
    pub generated_at: DateTime<Utc>,
    pub market_data_through: NaiveDate,
    pub approved_strategy: ApprovedStrategy,
    pub signals: Vec<TradeSignal>,
    pub selected_signal: Option<TradeSignal>,
    pub source_run_id: String,
    pub source_report: String,
    /// Research window start used by the selector run that produced this artifact.
    #[serde(default)]
    pub source_research_from: Option<NaiveDate>,
    /// Whether the source selector run passed the research promotion gate.
    #[serde(default)]
    pub source_gate_pass: Option<bool>,
    #[serde(default)]
    pub source_gate_reason: Option<String>,
    /// True when export required gate_pass; false when a short detector window
    /// allowed export via production approval only.
    #[serde(default)]
    pub detector_research_gate_enforced: bool,
}

impl ApprovedStrategy {
    pub fn validate_contract(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.symbols.is_empty(),
            "approved strategy must contain at least one symbol"
        );
        anyhow::ensure!(
            !self.allowed_live_strategies.is_empty(),
            "approved strategy must contain at least one allowed live strategy"
        );
        if let Some(research_from) = self.research_from {
            anyhow::ensure!(
                research_from <= Utc::now().date_naive(),
                "approved strategy research_from {research_from} cannot be in the future"
            );
        }
        if let Some(capital_budget) = self.research_gate_capital_budget {
            anyhow::ensure!(
                capital_budget.is_finite() && capital_budget > 0.0,
                "approved strategy research_gate_capital_budget must be positive and finite"
            );
        }
        if let Some(days) = self.live_detector_lookback_days {
            anyhow::ensure!(
                days > 0,
                "approved strategy live_detector_lookback_days must be positive"
            );
        }
        if let Some(approval) = &self.production_approval {
            approval.validate_contract()?;
        }
        Ok(())
    }

    pub fn production_approval_for_selected_signal(&self) -> anyhow::Result<&ProductionApproval> {
        self.production_approval.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "selected live signal requires explicit production approval or operator risk override"
            )
        })
    }
}

impl ProductionApproval {
    pub fn validate_contract(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.approved_by.trim().is_empty(),
            "production approval approved_by must be non-empty"
        );
        anyhow::ensure!(
            !self.reason.trim().is_empty(),
            "production approval reason must be non-empty"
        );
        if let Some(max_loss) = self.max_order_max_loss {
            anyhow::ensure!(
                max_loss.is_finite() && max_loss > 0.0,
                "production approval max_order_max_loss must be positive and finite"
            );
        }
        if self.status == ProductionApprovalStatus::CanaryApproved
            && self.source_canary_status.as_deref() == Some("blocked")
        {
            anyhow::bail!("canary-approved production approval cannot cite blocked source canary");
        }
        Ok(())
    }

    pub fn validate_selected_signal(&self, signal: &TradeSignal) -> anyhow::Result<()> {
        if let Some(max_order_max_loss) = self.max_order_max_loss {
            let max_loss = signal.max_loss.ok_or_else(|| {
                anyhow::anyhow!(
                    "selected live signal requires max_loss when production approval caps risk"
                )
            })?;
            anyhow::ensure!(
                max_loss <= max_order_max_loss + f64::EPSILON,
                "selected live signal max_loss {:.2} exceeds production approval max_order_max_loss {:.2}",
                max_loss,
                max_order_max_loss
            );
        }
        Ok(())
    }
}

impl LiveExecutionRules {
    pub fn validate_contract(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.take_profit_pct.is_finite() && self.take_profit_pct > 0.0,
            "live execution rules take_profit_pct must be positive and finite"
        );
        anyhow::ensure!(
            self.stop_loss_multiple.is_finite() && self.stop_loss_multiple >= 0.0,
            "live execution rules stop_loss_multiple must be non-negative and finite"
        );
        anyhow::ensure!(
            self.force_close_dte >= 0,
            "live execution rules force_close_dte must be non-negative"
        );
        if let Some(max_hold_days) = self.max_hold_days {
            anyhow::ensure!(
                max_hold_days > 0,
                "live execution rules max_hold_days must be positive when configured"
            );
        }
        Ok(())
    }
}

impl LiveSignalArtifact {
    pub fn validate_contract(&self) -> anyhow::Result<()> {
        self.approved_strategy.validate_contract()?;
        let approved_symbols = self
            .approved_strategy
            .symbols
            .iter()
            .map(|symbol| symbol.to_ascii_uppercase())
            .collect::<HashSet<_>>();
        anyhow::ensure!(
            self.schema_version == LIVE_SIGNAL_SCHEMA_VERSION,
            "live signal schema_version {} is unsupported",
            self.schema_version
        );
        anyhow::ensure!(
            self.strategy_id == self.approved_strategy.strategy_id,
            "live signal strategy_id does not match approved strategy"
        );
        anyhow::ensure!(
            self.profile_name == self.approved_strategy.profile_name,
            "live signal profile_name does not match approved strategy"
        );
        for signal in &self.signals {
            anyhow::ensure!(
                approved_symbols.contains(&signal.symbol.to_ascii_uppercase()),
                "live signal symbol {} is not in the approved strategy symbol list",
                signal.symbol
            );
            if let Some(rules) = &signal.execution_rules {
                rules.validate_contract()?;
            }
        }
        if let Some(signal) = &self.selected_signal {
            anyhow::ensure!(
                self.signals.iter().any(|candidate| candidate == signal),
                "selected live signal is not present in live signal list"
            );
            anyhow::ensure!(
                approved_symbols.contains(&signal.symbol.to_ascii_uppercase()),
                "selected live signal symbol {} is not approved",
                signal.symbol
            );
            anyhow::ensure!(
                signal.status.is_orderable(),
                "selected live signal is not a new entry"
            );
            anyhow::ensure!(
                self.approved_strategy
                    .allowed_live_strategies
                    .iter()
                    .any(|strategy| strategy == &signal.strategy),
                "selected live signal strategy {} is not approved for live execution",
                signal.strategy
            );
            self.approved_strategy
                .production_approval_for_selected_signal()?
                .validate_selected_signal(signal)?;
        }
        Ok(())
    }
}
