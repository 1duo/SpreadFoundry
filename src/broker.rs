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
    pub http_status: Option<u16>,
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TradierPosition {
    pub symbol: String,
    pub quantity: f64,
    pub cost_basis: Option<f64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TradierPositionsResponse {
    pub ok: bool,
    #[serde(default)]
    pub raw: Value,
    #[serde(default)]
    pub positions: Vec<TradierPosition>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TradierOrder {
    pub id: Option<String>,
    pub symbol: Option<String>,
    pub option_symbol: Option<String>,
    pub status: Option<String>,
    pub side: Option<String>,
    pub quantity: Option<f64>,
    pub tag: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TradierOrdersResponse {
    pub ok: bool,
    #[serde(default)]
    pub raw: Value,
    #[serde(default)]
    pub orders: Vec<TradierOrder>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TradierQuote {
    pub symbol: String,
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub last: Option<f64>,
    pub bid_size: Option<f64>,
    pub ask_size: Option<f64>,
    pub bid_date: Option<i64>,
    pub ask_date: Option<i64>,
    pub trade_date: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TradierQuotesResponse {
    pub ok: bool,
    #[serde(default)]
    pub raw: Value,
    #[serde(default)]
    pub quotes: Vec<TradierQuote>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TradierMarketClock {
    pub state: Option<String>,
    pub status: Option<String>,
    pub description: Option<String>,
    pub next_state: Option<String>,
    pub next_change: Option<String>,
    pub timestamp: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TradierMarketClockResponse {
    pub ok: bool,
    #[serde(default)]
    pub raw: Value,
    #[serde(default)]
    pub clock: Option<TradierMarketClock>,
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
        let (status, body, raw) = self.get_json(url)?;
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

    pub fn get_positions(&self) -> anyhow::Result<TradierPositionsResponse> {
        let url = format!(
            "{}/accounts/{}/positions",
            self.config.base_url.trim_end_matches('/'),
            self.config.account_id
        );
        let (status, body, raw) = self.get_json(url)?;
        if status.is_success() {
            Ok(TradierPositionsResponse {
                ok: true,
                positions: parse_tradier_positions(&raw),
                raw,
                error: None,
            })
        } else {
            Ok(TradierPositionsResponse {
                ok: false,
                raw,
                positions: Vec::new(),
                error: Some(format!("Tradier API returned HTTP {status}: {body}")),
            })
        }
    }

    pub fn get_orders(&self) -> anyhow::Result<TradierOrdersResponse> {
        let url = format!(
            "{}/accounts/{}/orders",
            self.config.base_url.trim_end_matches('/'),
            self.config.account_id
        );
        let response = self
            .client
            .get(url)
            .bearer_auth(&self.config.token)
            .header(reqwest::header::ACCEPT, "application/json")
            .query(&[("includeTags", "true")])
            .send()?;
        let status = response.status();
        let body = response.text()?;
        let raw = parse_json_body(&body);
        if status.is_success() {
            Ok(TradierOrdersResponse {
                ok: true,
                orders: parse_tradier_orders(&raw),
                raw,
                error: None,
            })
        } else {
            Ok(TradierOrdersResponse {
                ok: false,
                raw,
                orders: Vec::new(),
                error: Some(format!("Tradier API returned HTTP {status}: {body}")),
            })
        }
    }

    pub fn get_quotes(&self, symbols: &[String]) -> anyhow::Result<TradierQuotesResponse> {
        if symbols.is_empty() {
            anyhow::bail!("Tradier quotes request requires at least one symbol");
        }
        let url = format!(
            "{}/markets/quotes",
            self.config.base_url.trim_end_matches('/')
        );
        let response = self
            .client
            .get(url)
            .bearer_auth(&self.config.token)
            .header(reqwest::header::ACCEPT, "application/json")
            .query(&[
                ("symbols", symbols.join(",")),
                ("greeks", "false".to_owned()),
            ])
            .send()?;
        let status = response.status();
        let body = response.text()?;
        let raw = parse_json_body(&body);
        if status.is_success() {
            Ok(TradierQuotesResponse {
                ok: true,
                quotes: parse_tradier_quotes(&raw),
                raw,
                error: None,
            })
        } else {
            Ok(TradierQuotesResponse {
                ok: false,
                raw,
                quotes: Vec::new(),
                error: Some(format!("Tradier API returned HTTP {status}: {body}")),
            })
        }
    }

    pub fn get_market_clock(&self) -> anyhow::Result<TradierMarketClockResponse> {
        let url = format!(
            "{}/markets/clock",
            self.config.base_url.trim_end_matches('/')
        );
        let (status, body, raw) = self.get_json(url)?;
        if status.is_success() {
            Ok(TradierMarketClockResponse {
                ok: true,
                clock: parse_tradier_market_clock(&raw),
                raw,
                error: None,
            })
        } else {
            Ok(TradierMarketClockResponse {
                ok: false,
                raw,
                clock: None,
                error: Some(format!("Tradier API returned HTTP {status}: {body}")),
            })
        }
    }

    fn get_json(&self, url: String) -> anyhow::Result<(reqwest::StatusCode, String, Value)> {
        let response = self
            .client
            .get(url)
            .bearer_auth(&self.config.token)
            .header(reqwest::header::ACCEPT, "application/json")
            .send()?;
        let status = response.status();
        let body = response.text()?;
        let raw = parse_json_body(&body);
        Ok((status, body, raw))
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
        let raw = parse_json_body(&body);
        let http_status = Some(status.as_u16());
        if status.is_success() {
            Ok(TradierOrderResponse {
                ok: true,
                http_status,
                raw,
                error: None,
            })
        } else {
            Ok(TradierOrderResponse {
                ok: false,
                http_status,
                raw,
                error: Some(format!("Tradier API returned HTTP {status}: {body}")),
            })
        }
    }
}

pub fn tradier_http_status_is_ambiguous(status: u16) -> bool {
    status >= 500 || status == 408 || status == 429
}

fn parse_json_body(body: &str) -> Value {
    if body.trim().is_empty() {
        Value::Null
    } else {
        serde_json::from_str(body).unwrap_or_else(|_| Value::String(body.to_owned()))
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

fn parse_tradier_positions(value: &Value) -> Vec<TradierPosition> {
    tradier_object_or_array(
        value
            .get("positions")
            .and_then(|value| value.get("position")),
    )
    .into_iter()
    .filter_map(|position| {
        let map = position.as_object()?;
        let symbol = string_field(map, "symbol")?;
        let quantity = number_field(map, "quantity")?;
        Some(TradierPosition {
            symbol,
            quantity,
            cost_basis: number_field(map, "cost_basis"),
        })
    })
    .collect()
}

fn parse_tradier_orders(value: &Value) -> Vec<TradierOrder> {
    tradier_object_or_array(value.get("orders").and_then(|value| value.get("order")))
        .into_iter()
        .filter_map(|order| {
            let map = order.as_object()?;
            Some(TradierOrder {
                id: string_field(map, "id"),
                symbol: string_field(map, "symbol"),
                option_symbol: string_field(map, "option_symbol"),
                status: string_field(map, "status").map(|status| status.to_ascii_lowercase()),
                side: string_field(map, "side").map(|side| side.to_ascii_lowercase()),
                quantity: number_field(map, "quantity"),
                tag: string_field(map, "tag"),
            })
        })
        .collect()
}

fn parse_tradier_quotes(value: &Value) -> Vec<TradierQuote> {
    tradier_object_or_array(value.get("quotes").and_then(|value| value.get("quote")))
        .into_iter()
        .filter_map(|quote| {
            let map = quote.as_object()?;
            let symbol = string_field(map, "symbol")
                .or_else(|| string_field(map, "option_symbol"))
                .or_else(|| string_field(map, "root_symbols"))?;
            Some(TradierQuote {
                symbol,
                bid: number_field(map, "bid"),
                ask: number_field(map, "ask"),
                last: number_field(map, "last"),
                bid_size: number_field(map, "bid_size").or_else(|| number_field(map, "bidsize")),
                ask_size: number_field(map, "ask_size").or_else(|| number_field(map, "asksize")),
                bid_date: integer_field(map, "bid_date"),
                ask_date: integer_field(map, "ask_date"),
                trade_date: integer_field(map, "trade_date").or_else(|| integer_field(map, "date")),
            })
        })
        .collect()
}

fn parse_tradier_market_clock(value: &Value) -> Option<TradierMarketClock> {
    let clock = value.get("clock")?.as_object()?;
    Some(TradierMarketClock {
        state: string_field(clock, "state"),
        status: string_field(clock, "status"),
        description: string_field(clock, "description"),
        next_state: string_field(clock, "next_state"),
        next_change: string_field(clock, "next_change"),
        timestamp: integer_field(clock, "timestamp"),
    })
}

fn tradier_object_or_array(value: Option<&Value>) -> Vec<&Value> {
    match value {
        Some(Value::Array(values)) => values.iter().collect(),
        Some(Value::Object(_)) => value.into_iter().collect(),
        _ => Vec::new(),
    }
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

fn integer_field(map: &serde_json::Map<String, Value>, key: &str) -> Option<i64> {
    map.get(key).and_then(|value| {
        value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|number| i64::try_from(number).ok()))
            .or_else(|| value.as_str()?.replace(',', "").parse::<i64>().ok())
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
            let mut stdin = child
                .stdin
                .take()
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
