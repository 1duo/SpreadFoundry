use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BrokerCapabilities {
    pub single_leg_options: bool,
    pub multi_leg_options: bool,
    pub stock_option_combos: bool,
}

impl BrokerCapabilities {
    pub fn robinhood_agentic_current() -> Self {
        Self {
            single_leg_options: true,
            multi_leg_options: false,
            stock_option_combos: false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RobinhoodBrokerAdapter {
    pub capabilities: BrokerCapabilities,
    pub live_orders_enabled: bool,
}

impl Default for RobinhoodBrokerAdapter {
    fn default() -> Self {
        Self {
            capabilities: BrokerCapabilities::robinhood_agentic_current(),
            live_orders_enabled: false,
        }
    }
}

impl RobinhoodBrokerAdapter {
    pub fn assert_credit_spread_live_supported(&self) -> anyhow::Result<()> {
        if !self.capabilities.multi_leg_options {
            anyhow::bail!(
                "credit spread live execution is disabled: current Robinhood adapter has no atomic multi-leg support"
            );
        }
        Ok(())
    }

    pub fn assert_live_orders_enabled(&self) -> anyhow::Result<()> {
        if !self.live_orders_enabled {
            anyhow::bail!(
                "live order placement is disabled; use shadow-live until explicit rollout gates pass"
            );
        }
        Ok(())
    }
}
