use anyhow::Context;
use chrono::NaiveDate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ThetaClient {
    pub base_url: String,
}

impl Default for ThetaClient {
    fn default() -> Self {
        Self {
            base_url: "http://127.0.0.1:25503/v3".to_owned(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ThetaUniverseRequest {
    pub symbol: String,
    pub date: NaiveDate,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ThetaHistoryQuoteRequest {
    pub symbol: String,
    pub expiration: NaiveDate,
    pub right: String,
    pub strike: Decimal,
    pub start_date: NaiveDate,
    pub end_date: NaiveDate,
    pub interval: String,
}

impl ThetaClient {
    pub fn universe_contracts_url(&self, request: &ThetaUniverseRequest) -> String {
        format!(
            "{}/option/list/contracts/quote?symbol={}&date={}&format=json",
            self.base_url,
            request.symbol,
            yyyymmdd(request.date)
        )
    }

    pub fn history_quote_url(&self, request: &ThetaHistoryQuoteRequest) -> String {
        format!(
            "{}/option/history/quote?symbol={}&expiration={}&right={}&strike={}&start_date={}&end_date={}&interval={}&format=json",
            self.base_url,
            request.symbol,
            yyyymmdd(request.expiration),
            request.right,
            request.strike,
            yyyymmdd(request.start_date),
            yyyymmdd(request.end_date),
            request.interval
        )
    }

    pub async fn fetch_universe_contracts(
        &self,
        request: &ThetaUniverseRequest,
        output_path: impl AsRef<Path>,
    ) -> anyhow::Result<()> {
        let url = self.universe_contracts_url(request);
        fetch_text_to_file(&url, output_path).await
    }

    pub async fn fetch_history_quote(
        &self,
        request: &ThetaHistoryQuoteRequest,
        output_path: impl AsRef<Path>,
    ) -> anyhow::Result<()> {
        let url = self.history_quote_url(request);
        fetch_text_to_file(&url, output_path).await
    }
}

fn yyyymmdd(date: NaiveDate) -> String {
    date.format("%Y%m%d").to_string()
}

async fn fetch_text_to_file(url: &str, output_path: impl AsRef<Path>) -> anyhow::Result<()> {
    let output_path = output_path.as_ref();
    if let Some(parent) = output_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let body = reqwest::get(url)
        .await
        .with_context(|| format!("requesting {url}"))?
        .error_for_status()
        .with_context(|| format!("ThetaData returned an error for {url}"))?
        .text()
        .await
        .with_context(|| format!("reading response body from {url}"))?;
    tokio::fs::write(output_path, body)
        .await
        .with_context(|| format!("writing {}", output_path.display()))
}
