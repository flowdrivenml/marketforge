use marketforge_engine::live::{
    config::exchange::{ExchangeId, MarketType},
    infrastructure::rest::{
        HistoricalTradeCursor, HistoricalTradePage, RestFacadeError, RestFacadeFactory,
        RestOperationError,
    },
    stream::request::{CreateSourceStreamRequest, QuantityType, SourceChannel},
};

use super::support::{build_limiters, build_service, load_profile};

use crate::http_server::{MockExchange, MockResponse};

// ============================================================
// HISTORICAL PROFILE FIXTURES
// ============================================================

struct HistoricalCase {
    profile: &'static str,
    exchange: ExchangeId,
    market: MarketType,
    symbol: &'static str,
    settlement: Option<&'static str>,
    path: &'static str,
}

fn historical_cases() -> Vec<HistoricalCase> {
    vec![
        HistoricalCase {
            profile: "binance_spot",
            exchange: ExchangeId::Binance,
            market: MarketType::Spot,
            symbol: "BTCUSDT",
            settlement: None,
            path: "/api/v3/historicalTrades",
        },
        HistoricalCase {
            profile: "okx_spot",
            exchange: ExchangeId::Okx,
            market: MarketType::Spot,
            symbol: "BTC-USDT",
            settlement: None,
            path: "/api/v5/market/history-trades",
        },
        HistoricalCase {
            profile: "okx_linear",
            exchange: ExchangeId::Okx,
            market: MarketType::Linear,
            symbol: "BTC-USDT-SWAP",
            settlement: Some("USDT"),
            path: "/api/v5/market/history-trades",
        },
        HistoricalCase {
            profile: "okx_inverse",
            exchange: ExchangeId::Okx,
            market: MarketType::Inverse,
            symbol: "BTC-USD-SWAP",
            settlement: Some("BTC"),
            path: "/api/v5/market/history-trades",
        },
        HistoricalCase {
            profile: "gateio_spot",
            exchange: ExchangeId::Gateio,
            market: MarketType::Spot,
            symbol: "BTC_USDT",
            settlement: None,
            path: "/spot/trades",
        },
        HistoricalCase {
            profile: "gateio_linear",
            exchange: ExchangeId::Gateio,
            market: MarketType::Linear,
            symbol: "BTC_USDT",
            settlement: Some("USDT"),
            path: "/futures/usdt/trades",
        },
        HistoricalCase {
            profile: "gateio_inverse",
            exchange: ExchangeId::Gateio,
            market: MarketType::Inverse,
            symbol: "BTC_USD",
            settlement: Some("BTC"),
            path: "/futures/btc/trades",
        },
    ]
}

// ============================================================
// STREAM REQUEST
// ============================================================

fn request(case: &HistoricalCase) -> CreateSourceStreamRequest {
    let inverse = case.market == MarketType::Inverse;

    CreateSourceStreamRequest {
        instrument_id: 9001,
        exchange: case.exchange,
        symbol: case.symbol.to_owned(),
        market_category: case.market,

        instrument_type: match case.market {
            MarketType::Spot => "spot",
            MarketType::Linear | MarketType::Inverse => "perpetual",
        }
        .to_owned(),

        base_asset: Some("BTC".into()),

        quote_asset: Some(if inverse { "USD" } else { "USDT" }.into()),

        quantity_type: if inverse {
            QuantityType::Contracts
        } else {
            QuantityType::Base
        },

        contract_value: inverse.then(|| "100".into()),
        contract_value_asset: inverse.then(|| "USD".into()),

        channel: SourceChannel::Trades,
        settlement_asset: case.settlement.map(str::to_owned),
    }
}

// ============================================================
// EXCHANGE-SPECIFIC SUCCESS RESPONSES
// ============================================================

fn successful_response(exchange: ExchangeId) -> MockResponse {
    match exchange {
        ExchangeId::Okx => MockResponse::ok(
            r#"{
                "code": "0",
                "msg": "",
                "data": [{
                    "instId": "BTC-USDT",
                    "tradeId": "12345",
                    "px": "100",
                    "sz": "1",
                    "side": "buy",
                    "ts": "1793000000000"
                }]
            }"#,
        ),

        ExchangeId::Binance | ExchangeId::Gateio => MockResponse::ok("[]"),

        _ => unreachable!("unsupported historical test exchange"),
    }
}

// ============================================================
// CURSOR CASES
// ============================================================

fn historical_page(case: &HistoricalCase) -> HistoricalTradePage {
    let cursor = match case.exchange {
        ExchangeId::Binance => HistoricalTradeCursor::TradeId { from_id: 12345 },

        ExchangeId::Okx => HistoricalTradeCursor::Okx {
            before: None,
            after: Some(12345),
        },

        ExchangeId::Gateio => HistoricalTradeCursor::GateTimeRange {
            from: 1793000000,
            to: 1793000060,
        },

        _ => unreachable!(),
    };

    HistoricalTradePage::with_cursor(cursor).with_limit(10)
}

// ============================================================
// NATIVE REQUEST ASSERTIONS
// ============================================================

fn assert_historical_request(case: &HistoricalCase, request_line: &str) {
    assert!(
        request_line.starts_with(&format!("GET {}?", case.path)),
        "{}: unexpected historical endpoint: {request_line}",
        case.profile
    );

    match case.exchange {
        ExchangeId::Binance => {
            assert!(request_line.contains("symbol=BTCUSDT"), "{request_line}");

            assert!(request_line.contains("fromId=12345"), "{request_line}");
        }

        ExchangeId::Okx => {
            assert!(
                request_line.contains(&format!("instId={}", case.symbol)),
                "{request_line}"
            );

            assert!(request_line.contains("after=12345"), "{request_line}");

            assert!(!request_line.contains("symbol="), "{request_line}");
        }

        ExchangeId::Gateio => {
            assert!(request_line.contains("from=1793000000"), "{request_line}");

            assert!(request_line.contains("to=1793000060"), "{request_line}");

            let native_symbol_parameter = if case.market == MarketType::Spot {
                "currency_pair"
            } else {
                "contract"
            };

            assert!(
                request_line.contains(&format!("{native_symbol_parameter}={}", case.symbol)),
                "{request_line}"
            );
        }

        _ => unreachable!(),
    }

    assert!(
        request_line.contains("limit=10"),
        "{}: missing explicit limit: {request_line}",
        case.profile
    );
}

// ============================================================
// ALL SEVEN HISTORICAL PROFILES
// ============================================================

#[tokio::test]
async fn all_seven_profiles_execute_historical_page() {
    for case in historical_cases() {
        let server = MockExchange::start(vec![successful_response(case.exchange)]);

        let configs = load_profile(case.profile, server.base_url());

        let limiters = build_limiters(&configs, case.profile);

        let service = build_service(&configs, &limiters, case.profile);

        let factory = RestFacadeFactory::new(&service);

        let stream = request(&case);
        stream.validate().unwrap();

        let handle = factory.create(&stream).unwrap();

        assert!(
            handle.capabilities().supports_historical_trades(),
            "{}: missing historical capability",
            case.profile
        );

        let response = handle
            .historical_trades(historical_page(&case))
            .await
            .unwrap_or_else(|error| panic!("{}: historical request failed: {error}", case.profile));

        assert_eq!(
            response.status(),
            200,
            "{}: unexpected HTTP status",
            case.profile
        );

        let requests = server.requests();

        assert_eq!(
            requests.len(),
            1,
            "{}: expected exactly one historical request",
            case.profile
        );

        assert_historical_request(&case, &requests[0].request_line);

        println!("PASS {}: historical page", case.profile);
    }
}

// ============================================================
// MISMATCHED CURSOR REJECTION
// ============================================================

#[tokio::test]
async fn mismatched_cursor_is_rejected_without_http() {
    for case in historical_cases() {
        let server = MockExchange::start(vec![]);

        let configs = load_profile(case.profile, server.base_url());

        let limiters = build_limiters(&configs, case.profile);

        let service = build_service(&configs, &limiters, case.profile);

        let factory = RestFacadeFactory::new(&service);
        let handle = factory.create(&request(&case)).unwrap();

        let wrong_cursor = match case.exchange {
            ExchangeId::Binance => HistoricalTradeCursor::Okx {
                before: None,
                after: Some(123),
            },

            ExchangeId::Okx | ExchangeId::Gateio => HistoricalTradeCursor::TradeId { from_id: 123 },

            _ => unreachable!(),
        };

        let page = HistoricalTradePage::with_cursor(wrong_cursor);

        let error = handle.historical_trades(page).await.unwrap_err();

        assert!(
            matches!(
                error,
                RestOperationError::Facade(RestFacadeError::InvalidParameter {
                    name: "historical_cursor",
                    ..
                })
            ),
            "{}: expected cursor validation error: {error}",
            case.profile
        );

        assert_eq!(
            server.request_count(),
            0,
            "{}: invalid cursor reached HTTP",
            case.profile
        );
    }
}

// ============================================================
// INVALID GATE TIME RANGE
// ============================================================

#[tokio::test]
async fn gate_rejects_invalid_time_ranges() {
    for case in historical_cases()
        .into_iter()
        .filter(|case| case.exchange == ExchangeId::Gateio)
    {
        let server = MockExchange::start(vec![]);

        let configs = load_profile(case.profile, server.base_url());

        let limiters = build_limiters(&configs, case.profile);

        let service = build_service(&configs, &limiters, case.profile);

        let factory = RestFacadeFactory::new(&service);
        let handle = factory.create(&request(&case)).unwrap();

        for (from, to) in [(100, 100), (200, 100)] {
            let page =
                HistoricalTradePage::with_cursor(HistoricalTradeCursor::GateTimeRange { from, to });

            let error = handle.historical_trades(page).await.unwrap_err();

            assert!(matches!(
                error,
                RestOperationError::Facade(RestFacadeError::InvalidParameter {
                    name: "historical_cursor",
                    ..
                })
            ));
        }

        assert_eq!(
            server.request_count(),
            0,
            "{}: invalid time range reached HTTP",
            case.profile
        );
    }
}

// ============================================================
// INVALID OKX CURSOR
// ============================================================

#[tokio::test]
async fn okx_rejects_simultaneous_before_and_after() {
    let profile = "okx_spot";

    let server = MockExchange::start(vec![]);

    let configs = load_profile(profile, server.base_url());
    let limiters = build_limiters(&configs, profile);
    let service = build_service(&configs, &limiters, profile);

    let factory = RestFacadeFactory::new(&service);

    let case = historical_cases()
        .into_iter()
        .find(|case| case.profile == profile)
        .unwrap();

    let handle = factory.create(&request(&case)).unwrap();

    let page = HistoricalTradePage::with_cursor(HistoricalTradeCursor::Okx {
        before: Some(100),
        after: Some(200),
    });

    let error = handle.historical_trades(page).await.unwrap_err();

    assert!(matches!(
        error,
        RestOperationError::Facade(RestFacadeError::InvalidParameter {
            name: "historical_cursor",
            ..
        })
    ));

    assert_eq!(server.request_count(), 0);
}

// ============================================================
// INVALID HISTORICAL LIMIT
// ============================================================

#[tokio::test]
async fn invalid_historical_limit_is_rejected() {
    for case in historical_cases() {
        let server = MockExchange::start(vec![]);

        let configs = load_profile(case.profile, server.base_url());

        let limiters = build_limiters(&configs, case.profile);

        let service = build_service(&configs, &limiters, case.profile);

        let factory = RestFacadeFactory::new(&service);
        let handle = factory.create(&request(&case)).unwrap();

        let capability = handle.capabilities().historical_trades.as_ref().unwrap();

        let mut invalid_limits = vec![0];

        if let Some(maximum) = capability.endpoint.maximum_limit {
            if let Some(excessive) = maximum.checked_add(1) {
                invalid_limits.push(excessive);
            }
        }

        for limit in invalid_limits {
            let page = historical_page(&case).with_limit(limit);

            let error = handle.historical_trades(page).await.unwrap_err();

            assert!(matches!(
                error,
                RestOperationError::Facade(RestFacadeError::InvalidParameter { name: "limit", .. })
            ));
        }

        assert_eq!(
            server.request_count(),
            0,
            "{}: invalid limit reached HTTP",
            case.profile
        );
    }
}

// ============================================================
// UNSUPPORTED HISTORICAL PROFILES
// ============================================================

#[tokio::test]
async fn unsupported_profiles_reject_historical_requests() {
    let unsupported = [
        ("binance_linear", ExchangeId::Binance, MarketType::Linear),
        ("binance_inverse", ExchangeId::Binance, MarketType::Inverse),
        ("bybit_spot", ExchangeId::Bybit, MarketType::Spot),
        ("bybit_linear", ExchangeId::Bybit, MarketType::Linear),
        ("bybit_inverse", ExchangeId::Bybit, MarketType::Inverse),
        ("bitget_spot", ExchangeId::Bitget, MarketType::Spot),
        ("bitget_linear", ExchangeId::Bitget, MarketType::Linear),
        ("bitget_inverse", ExchangeId::Bitget, MarketType::Inverse),
    ];

    for (profile, exchange, market) in unsupported {
        let server = MockExchange::start(vec![]);

        let configs = load_profile(profile, server.base_url());
        let limiters = build_limiters(&configs, profile);
        let service = build_service(&configs, &limiters, profile);

        let factory = RestFacadeFactory::new(&service);

        let mut stream = request(&historical_cases()[0]);

        stream.exchange = exchange;
        stream.market_category = market;
        stream.instrument_type = if market == MarketType::Spot {
            "spot"
        } else {
            "perpetual"
        }
        .into();

        let handle = factory.create(&stream).unwrap();

        assert!(
            !handle.capabilities().supports_historical_trades(),
            "{profile}: should not advertise historical trades"
        );

        let error = handle
            .historical_trades(HistoricalTradePage::latest())
            .await
            .unwrap_err();

        assert!(
            matches!(
                error,
                RestOperationError::Facade(RestFacadeError::UnsupportedOperation {
                    operation: "historical_trades",
                    ..
                })
            ),
            "{profile}: incorrect unsupported-operation error"
        );

        assert_eq!(
            server.request_count(),
            0,
            "{profile}: unsupported operation reached HTTP"
        );
    }
}

// ============================================================
// OKX CURSOR DIRECTION
// ============================================================

#[tokio::test]
async fn okx_cursor_directions_remain_explicit() {
    let profile = "okx_spot";

    let server = MockExchange::start(vec![
        successful_response(ExchangeId::Okx),
        successful_response(ExchangeId::Okx),
    ]);

    let configs = load_profile(profile, server.base_url());
    let limiters = build_limiters(&configs, profile);
    let service = build_service(&configs, &limiters, profile);

    let factory = RestFacadeFactory::new(&service);

    let case = historical_cases()
        .into_iter()
        .find(|case| case.profile == profile)
        .unwrap();

    let handle = factory.create(&request(&case)).unwrap();

    // OKX: after means older records.
    let older = HistoricalTradePage::with_cursor(HistoricalTradeCursor::Okx {
        before: None,
        after: Some(12345),
    });

    handle.historical_trades(older).await.unwrap();

    // OKX: before means newer records.
    let newer = HistoricalTradePage::with_cursor(HistoricalTradeCursor::Okx {
        before: Some(12345),
        after: None,
    });

    handle.historical_trades(newer).await.unwrap();

    let requests = server.requests();

    assert_eq!(requests.len(), 2);

    let older_line = &requests[0].request_line;
    let newer_line = &requests[1].request_line;

    assert!(
        older_line.contains("after=12345"),
        "older-page request must use after: {older_line}"
    );

    assert!(
        !older_line.contains("before="),
        "older-page request contains unexpected before: {older_line}"
    );

    assert!(
        newer_line.contains("before=12345"),
        "newer-page request must use before: {newer_line}"
    );

    assert!(
        !newer_line.contains("after="),
        "newer-page request contains unexpected after: {newer_line}"
    );
}

// ============================================================
// INVALID ZERO TRADE IDS
// ============================================================

#[tokio::test]
async fn zero_trade_id_cursors_are_rejected_before_http() {
    for profile in ["binance_spot", "okx_spot", "okx_linear", "okx_inverse"] {
        let server = MockExchange::start(vec![]);

        let configs = load_profile(profile, server.base_url());
        let limiters = build_limiters(&configs, profile);
        let service = build_service(&configs, &limiters, profile);

        let factory = RestFacadeFactory::new(&service);

        let case = historical_cases()
            .into_iter()
            .find(|case| case.profile == profile)
            .unwrap();

        let handle = factory.create(&request(&case)).unwrap();

        let cursors = match case.exchange {
            ExchangeId::Binance => vec![HistoricalTradeCursor::TradeId { from_id: 0 }],

            ExchangeId::Okx => vec![
                HistoricalTradeCursor::Okx {
                    before: None,
                    after: Some(0),
                },
                HistoricalTradeCursor::Okx {
                    before: Some(0),
                    after: None,
                },
            ],

            _ => unreachable!(),
        };

        for cursor in cursors {
            let result = handle
                .historical_trades(HistoricalTradePage::with_cursor(cursor))
                .await;

            assert!(
                matches!(
                    result,
                    Err(RestOperationError::Facade(
                        RestFacadeError::InvalidParameter {
                            name: "historical_cursor",
                            ..
                        }
                    ))
                ),
                "{profile}: zero cursor was not rejected"
            );
        }

        assert_eq!(
            server.request_count(),
            0,
            "{profile}: invalid cursor reached HTTP"
        );
    }
}

// ============================================================
// LATEST HISTORICAL PAGE
// ============================================================

#[tokio::test]
async fn latest_historical_page_omits_cursor_parameters() {
    for case in historical_cases() {
        let server = MockExchange::start(vec![successful_response(case.exchange)]);

        let configs = load_profile(case.profile, server.base_url());

        let limiters = build_limiters(&configs, case.profile);
        let service = build_service(&configs, &limiters, case.profile);

        let factory = RestFacadeFactory::new(&service);
        let handle = factory.create(&request(&case)).unwrap();

        handle
            .historical_trades(HistoricalTradePage::latest())
            .await
            .unwrap_or_else(|error| {
                panic!("{}: latest historical page failed: {error}", case.profile)
            });

        let requests = server.requests();

        assert_eq!(requests.len(), 1);

        let line = &requests[0].request_line;

        for parameter in ["fromId=", "before=", "after=", "from=", "to="] {
            assert!(
                !line.contains(parameter),
                "{}: unexpected cursor parameter in {line}",
                case.profile
            );
        }
    }
}
