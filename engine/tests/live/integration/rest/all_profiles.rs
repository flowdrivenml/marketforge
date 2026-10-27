use marketforge_engine::live::{
    config::exchange::{ExchangeId, MarketType},
    infrastructure::rest::RestFacadeFactory,
    stream::request::{CreateSourceStreamRequest, QuantityType, SourceChannel},
};

use super::support::{build_limiters, build_service, load_profile};

use crate::http_server::{MockExchange, MockResponse};

// ============================================================
// PROFILE FIXTURES
// ============================================================

struct ProfileCase {
    profile: &'static str,
    exchange: ExchangeId,
    market: MarketType,
    symbol: &'static str,
    settlement: Option<&'static str>,

    snapshot_path: &'static str,
    trades_path: &'static str,

    symbol_parameter: &'static str,
    category_parameter: Option<&'static str>,
}

fn profiles() -> Vec<ProfileCase> {
    vec![
        ProfileCase {
            profile: "binance_spot",
            exchange: ExchangeId::Binance,
            market: MarketType::Spot,
            symbol: "BTCUSDT",
            settlement: None,
            snapshot_path: "/api/v3/depth",
            trades_path: "/api/v3/trades",
            symbol_parameter: "symbol",
            category_parameter: None,
        },
        ProfileCase {
            profile: "binance_linear",
            exchange: ExchangeId::Binance,
            market: MarketType::Linear,
            symbol: "BTCUSDT",
            settlement: Some("USDT"),
            snapshot_path: "/fapi/v1/depth",
            trades_path: "/fapi/v1/trades",
            symbol_parameter: "symbol",
            category_parameter: None,
        },
        ProfileCase {
            profile: "binance_inverse",
            exchange: ExchangeId::Binance,
            market: MarketType::Inverse,
            symbol: "BTCUSD_PERP",
            settlement: Some("BTC"),
            snapshot_path: "/dapi/v1/depth",
            trades_path: "/dapi/v1/trades",
            symbol_parameter: "symbol",
            category_parameter: None,
        },
        ProfileCase {
            profile: "bybit_spot",
            exchange: ExchangeId::Bybit,
            market: MarketType::Spot,
            symbol: "BTCUSDT",
            settlement: None,
            snapshot_path: "/v5/market/orderbook",
            trades_path: "/v5/market/recent-trade",
            symbol_parameter: "symbol",
            category_parameter: Some("spot"),
        },
        ProfileCase {
            profile: "bybit_linear",
            exchange: ExchangeId::Bybit,
            market: MarketType::Linear,
            symbol: "BTCUSDT",
            settlement: Some("USDT"),
            snapshot_path: "/v5/market/orderbook",
            trades_path: "/v5/market/recent-trade",
            symbol_parameter: "symbol",
            category_parameter: Some("linear"),
        },
        ProfileCase {
            profile: "bybit_inverse",
            exchange: ExchangeId::Bybit,
            market: MarketType::Inverse,
            symbol: "BTCUSD",
            settlement: Some("BTC"),
            snapshot_path: "/v5/market/orderbook",
            trades_path: "/v5/market/recent-trade",
            symbol_parameter: "symbol",
            category_parameter: Some("inverse"),
        },
        ProfileCase {
            profile: "okx_spot",
            exchange: ExchangeId::Okx,
            market: MarketType::Spot,
            symbol: "BTC-USDT",
            settlement: None,
            snapshot_path: "/api/v5/market/books",
            trades_path: "/api/v5/market/trades",
            symbol_parameter: "instId",
            category_parameter: None,
        },
        ProfileCase {
            profile: "okx_linear",
            exchange: ExchangeId::Okx,
            market: MarketType::Linear,
            symbol: "BTC-USDT-SWAP",
            settlement: Some("USDT"),
            snapshot_path: "/api/v5/market/books",
            trades_path: "/api/v5/market/trades",
            symbol_parameter: "instId",
            category_parameter: None,
        },
        ProfileCase {
            profile: "okx_inverse",
            exchange: ExchangeId::Okx,
            market: MarketType::Inverse,
            symbol: "BTC-USD-SWAP",
            settlement: Some("BTC"),
            snapshot_path: "/api/v5/market/books",
            trades_path: "/api/v5/market/trades",
            symbol_parameter: "instId",
            category_parameter: None,
        },
        ProfileCase {
            profile: "bitget_spot",
            exchange: ExchangeId::Bitget,
            market: MarketType::Spot,
            symbol: "BTCUSDT",
            settlement: None,
            snapshot_path: "/api/v3/market/orderbook",
            trades_path: "/api/v3/market/fills",
            symbol_parameter: "symbol",
            category_parameter: Some("SPOT"),
        },
        ProfileCase {
            profile: "bitget_linear",
            exchange: ExchangeId::Bitget,
            market: MarketType::Linear,
            symbol: "BTCUSDT",
            settlement: Some("USDT"),
            snapshot_path: "/api/v3/market/orderbook",
            trades_path: "/api/v3/market/fills",
            symbol_parameter: "symbol",
            category_parameter: Some("USDT-FUTURES"),
        },
        ProfileCase {
            profile: "bitget_inverse",
            exchange: ExchangeId::Bitget,
            market: MarketType::Inverse,
            symbol: "BTCUSD_CM",
            settlement: Some("BTC"),
            snapshot_path: "/api/v3/market/orderbook",
            trades_path: "/api/v3/market/fills",
            symbol_parameter: "symbol",
            category_parameter: Some("COIN-FUTURES"),
        },
        ProfileCase {
            profile: "gateio_spot",
            exchange: ExchangeId::Gateio,
            market: MarketType::Spot,
            symbol: "BTC_USDT",
            settlement: None,
            snapshot_path: "/spot/order_book",
            trades_path: "/spot/trades",
            symbol_parameter: "currency_pair",
            category_parameter: None,
        },
        ProfileCase {
            profile: "gateio_linear",
            exchange: ExchangeId::Gateio,
            market: MarketType::Linear,
            symbol: "BTC_USDT",
            settlement: Some("USDT"),
            snapshot_path: "/futures/usdt/order_book",
            trades_path: "/futures/usdt/trades",
            symbol_parameter: "contract",
            category_parameter: None,
        },
        ProfileCase {
            profile: "gateio_inverse",
            exchange: ExchangeId::Gateio,
            market: MarketType::Inverse,
            symbol: "BTC_USD",
            settlement: Some("BTC"),
            snapshot_path: "/futures/btc/order_book",
            trades_path: "/futures/btc/trades",
            symbol_parameter: "contract",
            category_parameter: None,
        },
    ]
}

// ============================================================
// STREAM REQUESTS
// ============================================================

fn stream_request(
    case: &ProfileCase,
    channel: SourceChannel,
    instrument_type: String,
) -> CreateSourceStreamRequest {
    let inverse = case.market == MarketType::Inverse;

    CreateSourceStreamRequest {
        instrument_id: 1001,
        exchange: case.exchange,
        symbol: case.symbol.to_owned(),
        market_category: case.market,
        instrument_type,

        base_asset: Some("BTC".into()),

        quote_asset: Some(if inverse { "USD" } else { "USDT" }.into()),

        quantity_type: if inverse {
            QuantityType::Contracts
        } else {
            QuantityType::Base
        },

        contract_value: inverse.then(|| "100".into()),
        contract_value_asset: inverse.then(|| "USD".into()),

        channel,
        settlement_asset: case.settlement.map(str::to_owned),
    }
}

// ============================================================
// NATIVE INSTRUMENT CLASSIFICATION
// ============================================================

fn instrument_type(
    case: &ProfileCase,
    configs: &marketforge_engine::live::config::ExchangeConfigStore,
) -> String {
    // Bitget Linear uses native REST categories selected
    // from the configured native instrument type.
    //
    // Use the actual mapping key instead of inventing it.

    if case.profile == "bitget_linear" {
        let config = configs.get(case.profile).unwrap();

        let mapping = config
            .market_mapping
            .as_ref()
            .expect("Bitget Linear requires market mapping");

        return mapping
            .native_types
            .iter()
            .find(|(_, native)| native.rest_category.as_deref() == Some("USDT-FUTURES"))
            .map(|(name, _)| name.clone())
            .expect("missing USDT-FUTURES native type");
    }

    match case.market {
        MarketType::Spot => "spot".into(),
        MarketType::Linear | MarketType::Inverse => "perpetual".into(),
    }
}

// ============================================================
// MOCK RESPONSE
// ============================================================

fn successful_response(exchange: ExchangeId) -> MockResponse {
    let body = match exchange {
        ExchangeId::Okx => {
            r#"{
                "code": "0",
                "msg": "",
                "data": [{
                    "bids": [],
                    "asks": [],
                    "ts": "123456789"
                }]
            }"#
        }

        ExchangeId::Bitget => {
            r#"{
                "code": "00000",
                "msg": "success",
                "data": {
                    "bids": [],
                    "asks": []
                }
            }"#
        }

        ExchangeId::Bybit => {
            r#"{
                "retCode": 0,
                "retMsg": "OK",
                "result": {
                    "b": [],
                    "a": [],
                    "bids": [],
                    "asks": []
                }
            }"#
        }

        ExchangeId::Binance | ExchangeId::Gateio => {
            r#"{
                "bids": [],
                "asks": [],
                "data": []
            }"#
        }
    };

    MockResponse::ok(body)
}
// ============================================================
// HTTP REQUEST ASSERTIONS
// ============================================================

fn assert_native_request(case: &ProfileCase, request_line: &str, expected_path: &str) {
    assert!(
        request_line.starts_with(&format!("GET {expected_path}")),
        "{}: incorrect native endpoint: {request_line}",
        case.profile
    );

    let expected_symbol = format!("{}={}", case.symbol_parameter, case.symbol,);

    assert!(
        request_line.contains(&expected_symbol),
        "{}: missing native symbol '{expected_symbol}': {request_line}",
        case.profile
    );

    if let Some(category) = case.category_parameter {
        let expected = format!("category={category}");

        assert!(
            request_line.contains(&expected),
            "{}: missing native category '{expected}': {request_line}",
            case.profile
        );
    }
}

// ============================================================
// ALL PROFILES: SNAPSHOTS AND RECENT TRADES
// ============================================================

#[tokio::test]
async fn all_fifteen_profiles_execute_native_rest_operations() {
    for case in profiles() {
        let server = MockExchange::start(vec![
            successful_response(case.exchange),
            successful_response(case.exchange),
        ]);
        let configs = load_profile(case.profile, server.base_url());

        let limiters = build_limiters(&configs, case.profile);

        let service = build_service(&configs, &limiters, case.profile);

        let factory = RestFacadeFactory::new(&service);

        let native_type = instrument_type(&case, &configs);

        // ----------------------------------------------------
        // DEPTH SNAPSHOT
        // ----------------------------------------------------

        let depth = factory
            .create(&stream_request(
                &case,
                SourceChannel::Depth,
                native_type.clone(),
            ))
            .unwrap();

        assert!(
            depth.capabilities().supports_depth_snapshot(),
            "{}: missing snapshot capability",
            case.profile
        );

        let snapshot = depth
            .snapshot()
            .await
            .unwrap_or_else(|error| panic!("{}: snapshot failed: {error}", case.profile));

        assert_eq!(snapshot.status(), 200, "{}: snapshot status", case.profile);

        // ----------------------------------------------------
        // RECENT TRADES
        // ----------------------------------------------------

        let trades = factory
            .create(&stream_request(&case, SourceChannel::Trades, native_type))
            .unwrap();

        assert!(
            trades.capabilities().supports_recent_trades(),
            "{}: missing recent trades capability",
            case.profile
        );

        let recent = trades
            .recent_trades()
            .await
            .unwrap_or_else(|error| panic!("{}: recent trades failed: {error}", case.profile));

        assert_eq!(
            recent.status(),
            200,
            "{}: recent trades status",
            case.profile
        );

        // ----------------------------------------------------
        // NATIVE HTTP REQUESTS
        // ----------------------------------------------------

        let requests = server.requests();

        assert_eq!(
            requests.len(),
            2,
            "{}: expected snapshot + trades requests",
            case.profile
        );

        assert_native_request(&case, &requests[0].request_line, case.snapshot_path);

        assert_native_request(&case, &requests[1].request_line, case.trades_path);

        // ----------------------------------------------------
        // SHARED SERVICE
        // ----------------------------------------------------

        assert!(
            std::ptr::eq(depth.service(), trades.service(),),
            "{}: handles do not share REST service",
            case.profile
        );

        println!("PASS {}: snapshot + recent trades", case.profile);
    }
}
