use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TradierConfig {
    pub account_id: String,
    pub token: String,
    pub base_url: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TradierOrderResponse {
    pub ok: bool,
    #[serde(default)]
    pub raw: Value,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TradierAccountBalances {
    pub account_number: Option<String>,
    pub account_type: Option<String>,
    pub option_buying_power: Option<f64>,
    pub total_cash: Option<f64>,
    pub cash: Option<f64>,
    pub equity: Option<f64>,
    pub close_pl: Option<f64>,
    pub open_pl: Option<f64>,
    pub market_value: Option<f64>,
    pub option_long_value: Option<f64>,
    pub option_short_value: Option<f64>,
    pub option_requirement: Option<f64>,
    pub current_requirement: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TradierBalancesResponse {
    pub ok: bool,
    #[serde(default)]
    pub raw: Value,
    #[serde(default)]
    pub balances: Option<TradierAccountBalances>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Clone, Debug)]
pub struct TradierClient {
    config: TradierConfig,
    client: reqwest::blocking::Client,
}

impl TradierClient {
    pub fn new(config: TradierConfig) -> anyhow::Result<Self> {
        Self::new_with_timeout(config, Duration::from_secs(30))
    }

    pub fn new_with_timeout(config: TradierConfig, timeout: Duration) -> anyhow::Result<Self> {
        let client = reqwest::blocking::Client::builder()
            .timeout(timeout)
            .build()?;
        Ok(Self { config, client })
    }

    pub fn preview_order(
        &self,
        payload: &BTreeMap<String, String>,
    ) -> anyhow::Result<TradierOrderResponse> {
        self.submit_order(payload, true)
    }

    pub fn place_order(
        &self,
        payload: &BTreeMap<String, String>,
    ) -> anyhow::Result<TradierOrderResponse> {
        self.submit_order(payload, false)
    }

    pub fn get_balances(&self) -> anyhow::Result<TradierBalancesResponse> {
        let url = format!(
            "{}/accounts/{}/balances",
            self.config.base_url.trim_end_matches('/'),
            self.config.account_id
        );
        let response = self
            .client
            .get(url)
            .bearer_auth(&self.config.token)
            .header(reqwest::header::ACCEPT, "application/json")
            .send()?;
        let status = response.status();
        let body = response.text()?;
        let raw = if body.trim().is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&body).unwrap_or_else(|_| Value::String(body.clone()))
        };
        if status.is_success() {
            Ok(TradierBalancesResponse {
                ok: true,
                balances: parse_tradier_balances(&raw),
                raw,
                error: None,
            })
        } else {
            Ok(TradierBalancesResponse {
                ok: false,
                raw,
                balances: None,
                error: Some(format!("Tradier API returned HTTP {status}: {body}")),
            })
        }
    }

    fn submit_order(
        &self,
        payload: &BTreeMap<String, String>,
        preview: bool,
    ) -> anyhow::Result<TradierOrderResponse> {
        let mut form = payload.clone();
        form.insert("preview".to_owned(), preview.to_string());
        let url = format!(
            "{}/accounts/{}/orders",
            self.config.base_url.trim_end_matches('/'),
            self.config.account_id
        );
        let response = self
            .client
            .post(url)
            .bearer_auth(&self.config.token)
            .header(reqwest::header::ACCEPT, "application/json")
            .form(&form)
            .send()?;
        let status = response.status();
        let body = response.text()?;
        let raw = if body.trim().is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&body).unwrap_or_else(|_| Value::String(body.clone()))
        };
        if status.is_success() {
            Ok(TradierOrderResponse {
                ok: true,
                raw,
                error: None,
            })
        } else {
            Ok(TradierOrderResponse {
                ok: false,
                raw,
                error: Some(format!("Tradier API returned HTTP {status}: {body}")),
            })
        }
    }
}

fn parse_tradier_balances(value: &Value) -> Option<TradierAccountBalances> {
    let balances = value.get("balances")?.as_object()?;
    Some(TradierAccountBalances {
        account_number: string_field(balances, "account_number"),
        account_type: string_field(balances, "account_type"),
        option_buying_power: number_field(balances, "option_buying_power")
            .or_else(|| number_field(balances, "options_buying_power"))
            .or_else(|| number_field(balances, "buying_power")),
        total_cash: number_field(balances, "total_cash")
            .or_else(|| number_field(balances, "cash_available")),
        cash: number_field(balances, "cash")
            .or_else(|| nested_number_field(balances, "cash", "cash_available"))
            .or_else(|| number_field(balances, "total_cash"))
            .or_else(|| number_field(balances, "cash_available")),
        equity: number_field(balances, "equity"),
        close_pl: number_field(balances, "close_pl"),
        open_pl: number_field(balances, "open_pl"),
        market_value: number_field(balances, "market_value"),
        option_long_value: number_field(balances, "option_long_value"),
        option_short_value: number_field(balances, "option_short_value"),
        option_requirement: number_field(balances, "option_requirement"),
        current_requirement: number_field(balances, "current_requirement"),
    })
}

fn string_field(map: &serde_json::Map<String, Value>, key: &str) -> Option<String> {
    map.get(key).and_then(|value| {
        value
            .as_str()
            .map(ToOwned::to_owned)
            .or_else(|| value.as_i64().map(|number| number.to_string()))
    })
}

fn number_field(map: &serde_json::Map<String, Value>, key: &str) -> Option<f64> {
    map.get(key).and_then(|value| {
        value
            .as_f64()
            .or_else(|| value.as_str()?.replace(',', "").parse::<f64>().ok())
    })
}

fn nested_number_field(
    map: &serde_json::Map<String, Value>,
    object_key: &str,
    number_key: &str,
) -> Option<f64> {
    number_field(map.get(object_key)?.as_object()?, number_key)
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
