use marketforge_engine::live::{
    config::exchange::{ExchangeId, MarketType},
    infrastructure::rest::RestFacadeFactory,
    stream::request::{CreateSourceStreamRequest, QuantityType, SourceChannel},
};

use super::support::{build_limiters, build_service, load_profile};

use crate::http_server::{MockExchange, MockResponse};

// ============================================================
// REAL OKX SPOT STREAM
// ============================================================

fn okx_spot_request(channel: SourceChannel) -> CreateSourceStreamRequest {
    CreateSourceStreamRequest {
        instrument_id: 9001,
        exchange: ExchangeId::Okx,
        symbol: "BTC-USDT".into(),
        market_category: MarketType::Spot,
        instrument_type: "spot".into(),
        base_asset: Some("BTC".into()),
        quote_asset: Some("USDT".into()),
        quantity_type: QuantityType::Base,
        contract_value: None,
        contract_value_asset: None,
        channel,
        settlement_asset: None,
    }
}

// ============================================================
// OKX SNAPSHOT
// ============================================================

#[tokio::test]
async fn okx_snapshot_uses_inst_id() {
    let server = MockExchange::start(vec![MockResponse::ok(
        r#"{
                "code": "0",
                "msg": "",
                "data": [{
                    "bids": [],
                    "asks": [],
                    "ts": "123456789"
                }]
            }"#,
    )]);

    let profile = "okx_spot";

    let configs = load_profile(profile, server.base_url());
    let limiters = build_limiters(&configs, profile);
    let service = build_service(&configs, &limiters, profile);

    let factory = RestFacadeFactory::new(&service);

    let handle = factory
        .create(&okx_spot_request(SourceChannel::Depth))
        .unwrap();

    let response = handle.snapshot().await.unwrap();

    assert_eq!(response.status(), 200);

    let requests = server.requests();

    assert_eq!(requests.len(), 1);

    let line = &requests[0].request_line;

    assert!(
        line.contains("instId=BTC-USDT"),
        "OKX snapshot did not bind native instId: {line}"
    );

    assert!(
        !line.contains("symbol="),
        "OKX must not receive Binance-style symbol: {line}"
    );
}

// ============================================================
// OKX RECENT TRADES
// ============================================================

#[tokio::test]
async fn okx_recent_trades_uses_inst_id() {
    let server = MockExchange::start(vec![MockResponse::ok(
        r#"{
                "code": "0",
                "msg": "",
                "data": [{
                    "instId": "BTC-USDT",
                    "tradeId": "123",
                    "px": "100",
                    "sz": "1",
                    "side": "buy",
                    "ts": "123456789"
                }]
            }"#,
    )]);

    let profile = "okx_spot";

    let configs = load_profile(profile, server.base_url());
    let limiters = build_limiters(&configs, profile);
    let service = build_service(&configs, &limiters, profile);

    let factory = RestFacadeFactory::new(&service);

    let handle = factory
        .create(&okx_spot_request(SourceChannel::Trades))
        .unwrap();

    let response = handle.recent_trades().await.unwrap();

    assert_eq!(response.status(), 200);

    let requests = server.requests();

    assert_eq!(requests.len(), 1);

    let line = &requests[0].request_line;

    assert!(
        line.contains("instId=BTC-USDT"),
        "OKX recent trades did not bind instId: {line}"
    );

    assert!(
        !line.contains("symbol="),
        "unexpected symbol parameter: {line}"
    );
}
