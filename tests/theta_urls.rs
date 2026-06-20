use chrono::NaiveDate;
use rust_decimal_macros::dec;
use spreadfoundry::theta::{ThetaClient, ThetaHistoryQuoteRequest, ThetaUniverseRequest};

#[test]
fn v3_contract_list_url_uses_symbol_and_single_date() {
    let client = ThetaClient {
        base_url: "http://127.0.0.1:25503/v3".to_owned(),
    };
    let request = ThetaUniverseRequest {
        symbol: "NVDA".to_owned(),
        date: NaiveDate::from_ymd_opt(2026, 6, 18).unwrap(),
    };

    assert_eq!(
        client.universe_contracts_url(&request),
        "http://127.0.0.1:25503/v3/option/list/contracts/quote?symbol=NVDA&date=20260618&format=json"
    );
}

#[test]
fn v3_history_quote_url_uses_current_parameter_names() {
    let client = ThetaClient {
        base_url: "http://127.0.0.1:25503/v3".to_owned(),
    };
    let request = ThetaHistoryQuoteRequest {
        symbol: "NVDA".to_owned(),
        expiration: NaiveDate::from_ymd_opt(2026, 7, 24).unwrap(),
        right: "put".to_owned(),
        strike: dec!(200.000),
        start_date: NaiveDate::from_ymd_opt(2026, 6, 18).unwrap(),
        end_date: NaiveDate::from_ymd_opt(2026, 6, 19).unwrap(),
        interval: "1m".to_owned(),
    };

    assert_eq!(
        client.history_quote_url(&request),
        "http://127.0.0.1:25503/v3/option/history/quote?symbol=NVDA&expiration=20260724&right=put&strike=200.000&start_date=20260618&end_date=20260619&interval=1m&format=json"
    );
}
