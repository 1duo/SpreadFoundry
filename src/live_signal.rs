use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub const LIVE_SIGNAL_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ApprovedStrategy {
    pub strategy_id: String,
    pub profile_name: String,
    pub symbols: Vec<String>,
    pub portfolio_constraints: ApprovedPortfolioConstraints,
    pub allowed_live_strategies: Vec<String>,
    pub canary_risk_policy_id: String,
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
}

impl LiveSignalArtifact {
    pub fn validate_contract(&self) -> anyhow::Result<()> {
        let approved_symbols = self
            .approved_strategy
            .symbols
            .iter()
            .map(|symbol| symbol.to_ascii_uppercase())
            .collect::<HashSet<_>>();
        anyhow::ensure!(
            !approved_symbols.is_empty(),
            "approved strategy must contain at least one symbol"
        );
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
        }
        Ok(())
    }
}
