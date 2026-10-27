use std::path::Path;

use marketforge_engine::live::{
    config::{ExchangeConfig, ExchangeConfigStore},
    infrastructure::rest::{HistoricalPagination, StreamRestCapabilities},
};

// ============================================================
// CONFIGURATION HELPERS
// ============================================================

fn config_directory() -> &'static Path {
    Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../data/.config/live/exchanges"
    ))
}

fn load_configurations() -> ExchangeConfigStore {
    ExchangeConfigStore::load_dir(config_directory())
        .expect("failed to load real exchange configurations")
}

fn capabilities(profile: &str) -> StreamRestCapabilities {
    let store = load_configurations();

    let config = store
        .get(profile)
        .unwrap_or_else(|| panic!("missing exchange profile: {profile}"));

    StreamRestCapabilities::compile(config)
}

// ============================================================
// ALL EXCHANGE PROFILES
// ============================================================

#[test]
fn all_fifteen_profiles_expose_depth_and_recent_trades() {
    let store = load_configurations();

    assert_eq!(store.len(), 15, "unexpected exchange profile count");

    for (profile, config) in store.iter() {
        let capabilities = StreamRestCapabilities::compile(config);

        assert!(
            capabilities.supports_depth_snapshot(),
            "{profile}: missing depth snapshot capability"
        );

        assert!(
            capabilities.supports_recent_trades(),
            "{profile}: missing recent trades capability"
        );
    }
}

// ============================================================
// HISTORICAL TRADE CAPABILITIES
// ============================================================

#[test]
fn historical_trade_capabilities_match_public_endpoints() {
    let cases = [
        ("binance_spot", true),
        ("binance_linear", false),
        ("binance_inverse", false),
        ("bybit_spot", false),
        ("bybit_linear", false),
        ("bybit_inverse", false),
        ("okx_spot", true),
        ("okx_linear", true),
        ("okx_inverse", true),
        ("bitget_spot", false),
        ("bitget_linear", false),
        ("bitget_inverse", false),
        ("gateio_spot", true),
        ("gateio_linear", true),
        ("gateio_inverse", true),
    ];

    for (profile, expected) in cases {
        let actual = capabilities(profile).supports_historical_trades();

        assert_eq!(
            actual, expected,
            "{profile}: incorrect historical trade capability"
        );
    }
}

// ============================================================
// PAGINATION STRATEGIES
// ============================================================

#[test]
fn pagination_matches_configured_exchange_semantics() {
    let cases = [
        ("binance_spot", HistoricalPagination::TradeId),
        ("okx_spot", HistoricalPagination::OkxTradeHistory),
        ("okx_linear", HistoricalPagination::OkxTradeHistory),
        ("okx_inverse", HistoricalPagination::OkxTradeHistory),
        ("gateio_spot", HistoricalPagination::GateSpotTrades),
        ("gateio_linear", HistoricalPagination::GateFuturesTrades),
        ("gateio_inverse", HistoricalPagination::GateFuturesTrades),
    ];

    for (profile, expected) in cases {
        let actual = capabilities(profile);

        let historical = actual
            .historical_trades
            .unwrap_or_else(|| panic!("{profile}: missing historical capability"));

        assert_eq!(
            historical.pagination, expected,
            "{profile}: incorrect pagination strategy"
        );
    }
}

// ============================================================
// GATE.IO ENDPOINT REUSE
// ============================================================

#[test]
fn gateio_recent_trades_reuse_historical_endpoint() {
    for profile in ["gateio_spot", "gateio_linear", "gateio_inverse"] {
        let capabilities = capabilities(profile);

        let recent = capabilities
            .recent_trades
            .expect("missing recent trades capability");

        let historical = capabilities
            .historical_trades
            .expect("missing historical trades capability");

        assert_eq!(
            recent.endpoint, "historical_trades",
            "{profile}: incorrect recent trades endpoint"
        );

        assert_eq!(
            recent.endpoint, historical.endpoint.endpoint,
            "{profile}: endpoint identity mismatch"
        );
    }
}

// ============================================================
// PUBLIC-ONLY AUTHENTICATION
// ============================================================

#[test]
fn all_configured_rest_endpoints_are_public() {
    let store = load_configurations();

    for (profile, config) in store.iter() {
        for (endpoint_name, endpoint) in &config.rest.endpoints {
            let authentication = endpoint
                .authentication
                .as_deref()
                .or(config.rest.authentication.as_deref());

            assert!(
                matches!(authentication, None | Some("none")),
                "{profile}: endpoint '{endpoint_name}' \
                 requires unsupported authentication: {authentication:?}"
            );
        }
    }
}

// ============================================================
// ENDPOINT LIMITS
// ============================================================

#[test]
fn capabilities_preserve_configured_endpoint_limits() {
    let store = load_configurations();

    for (profile, config) in store.iter() {
        let capabilities = StreamRestCapabilities::compile(config);

        if let Some(snapshot) = &capabilities.depth_snapshot {
            assert_endpoint_limits(
                profile,
                config,
                &snapshot.endpoint,
                snapshot.default_limit,
                snapshot.maximum_limit,
            );
        }

        if let Some(recent) = &capabilities.recent_trades {
            assert_endpoint_limits(
                profile,
                config,
                &recent.endpoint,
                recent.default_limit,
                recent.maximum_limit,
            );
        }

        if let Some(historical) = &capabilities.historical_trades {
            let endpoint = &historical.endpoint;

            assert_endpoint_limits(
                profile,
                config,
                &endpoint.endpoint,
                endpoint.default_limit,
                endpoint.maximum_limit,
            );
        }
    }
}

fn assert_endpoint_limits(
    profile: &str,
    config: &ExchangeConfig,
    endpoint_name: &str,
    default_limit: Option<u64>,
    maximum_limit: Option<u64>,
) {
    let configured = config.endpoint(endpoint_name).unwrap_or_else(|| {
        panic!(
            "{profile}: configured endpoint \
                 '{endpoint_name}' is missing"
        )
    });

    assert_eq!(
        default_limit, configured.default_limit,
        "{profile}: default limit mismatch"
    );

    assert_eq!(
        maximum_limit, configured.maximum_limit,
        "{profile}: maximum limit mismatch"
    );
}

// ============================================================
// BINANCE AUTHENTICATION REGRESSION
// ============================================================

#[test]
fn binance_futures_do_not_expose_authenticated_history() {
    let store = load_configurations();

    for profile in ["binance_linear", "binance_inverse"] {
        let config = store
            .get(profile)
            .expect("missing Binance Futures configuration");

        assert!(
            !config.rest.endpoints.contains_key("historical_trades"),
            "{profile}: authenticated historical endpoint must remain removed"
        );

        let capabilities = StreamRestCapabilities::compile(config);

        assert!(
            !capabilities.supports_historical_trades(),
            "{profile}: historical access must not be advertised"
        );

        assert!(
            capabilities.supports_recent_trades(),
            "{profile}: public recent trades must remain available"
        );
    }
}
