use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{Read, Write};
use std::process::{Command, Stdio};
use std::time::Duration;
use wait_timeout::ChildExt;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BrokerCapabilities {
    pub single_leg_options: bool,
    pub multi_leg_options: bool,
    pub stock_option_combos: bool,
    #[serde(default)]
    pub cash_secured_puts: bool,
    #[serde(default)]
    pub covered_calls: bool,
}

impl BrokerCapabilities {
    pub fn robinhood_agentic_current() -> Self {
        Self {
            single_leg_options: true,
            multi_leg_options: false,
            stock_option_combos: false,
            cash_secured_puts: false,
            covered_calls: false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RobinhoodBrokerAdapter {
    pub capabilities: BrokerCapabilities,
    pub live_orders_enabled: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RobinhoodMcpToolRequest {
    pub server: String,
    pub tool: String,
    pub arguments: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RobinhoodMcpToolResponse {
    pub ok: bool,
    pub tool: String,
    #[serde(default)]
    pub raw: Value,
    #[serde(default)]
    pub error: Option<String>,
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

    pub fn assert_debit_spread_live_supported(&self) -> anyhow::Result<()> {
        if !self.capabilities.multi_leg_options {
            anyhow::bail!(
                "debit spread live execution is disabled: current Robinhood adapter has no proven atomic multi-leg support"
            );
        }
        Ok(())
    }

    pub fn assert_wheel_live_supported(&self) -> anyhow::Result<()> {
        if !self.capabilities.cash_secured_puts {
            anyhow::bail!(
                "wheel live execution is disabled: current Robinhood adapter has no proven cash-secured put sell-to-open support"
            );
        }
        if !self.capabilities.covered_calls {
            anyhow::bail!(
                "wheel live execution is disabled: current Robinhood adapter has no proven covered-call lifecycle support"
            );
        }
        Ok(())
    }

    pub fn assert_live_orders_enabled(&self) -> anyhow::Result<()> {
        if !self.live_orders_enabled {
            anyhow::bail!("live order placement is disabled until explicit rollout gates pass");
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct RobinhoodMcpCommandExecutor {
    command: String,
    timeout: Duration,
}

impl RobinhoodMcpCommandExecutor {
    pub fn new(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
            timeout: Duration::from_secs(30),
        }
    }

    pub fn execute(
        &self,
        request: &RobinhoodMcpToolRequest,
    ) -> anyhow::Result<RobinhoodMcpToolResponse> {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg(&self.command)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        {
            let stdin = child
                .stdin
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("Robinhood MCP bridge stdin is unavailable"))?;
            stdin.write_all(serde_json::to_string(request)?.as_bytes())?;
            stdin.write_all(b"\n")?;
        }

        let status = match child.wait_timeout(self.timeout)? {
            Some(status) => status,
            None => {
                let _ = child.kill();
                let _ = child.wait();
                anyhow::bail!(
                    "Robinhood MCP bridge timed out after {} seconds",
                    self.timeout.as_secs()
                );
            }
        };
        let mut stdout = String::new();
        let mut stderr = String::new();
        if let Some(mut pipe) = child.stdout.take() {
            pipe.read_to_string(&mut stdout)?;
        }
        if let Some(mut pipe) = child.stderr.take() {
            pipe.read_to_string(&mut stderr)?;
        }
        if !status.success() {
            let stderr = stderr.trim().to_owned();
            anyhow::bail!("Robinhood MCP bridge exited with {}: {}", status, stderr);
        }
        let response: RobinhoodMcpToolResponse = serde_json::from_str(stdout.trim())?;
        if response.tool != request.tool {
            anyhow::bail!(
                "Robinhood MCP bridge returned tool {}, expected {}",
                response.tool,
                request.tool
            );
        }
        Ok(response)
    }
}
