use std::{fs, path::PathBuf};

use serde_json::{Value, json};

use marketforge_engine::live::{
    config::exchange::ExchangeConfig,
    infrastructure::websocket::{
        SubscriptionResolutionContext, SubscriptionResolver, SubscriptionStrategy,
    },
};

const EXCHANGES: [&str; 5] = ["binance", "bybit", "bitget", "okx", "gateio"];

const MARKETS: [&str; 3] = ["spot", "linear", "inverse"];

// ============================================================
// CONFIGURATION
// ============================================================

fn profile_config(profile: &str) -> ExchangeConfig {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../data/.config/live/exchanges")
        .join(format!("{profile}.toml"));

    let content =
        fs::read_to_string(&path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));

    toml::from_str(&content).unwrap_or_else(|error| panic!("{profile}: {error}"))
}

// ============================================================
// REPRESENTATIVE NATIVE INSTRUMENTS
// ============================================================

fn symbol(profile: &str) -> &'static str {
    match profile {
        "binance_inverse" => "BTCUSD_PERP",

        "bybit_inverse" => "BTCUSD",

        "bitget_inverse" => "BTCUSD_CM",

        "gateio_spot" | "gateio_linear" => "BTC_USDT",
        "gateio_inverse" => "BTC_USD",

        "okx_spot" => "BTC-USDT",
        "okx_linear" => "BTC-USDT-SWAP",
        "okx_inverse" => "BTC-USD-SWAP",

        _ => "BTCUSDT",
    }
}

// ============================================================
// RESOLUTION CONTEXT
// ============================================================

fn context(profile: &str, channel: &str) -> SubscriptionResolutionContext {
    let mut context = SubscriptionResolutionContext::new(symbol(profile)).unwrap();

    if profile == "bitget_linear" {
        // Test fixture representing an already-resolved
        // native market category.
        //
        // Production must obtain this from market_mapping.
        context = context
            .with_variable("native_ws_inst_type", "USDT-FUTURES")
            .unwrap();
    }

    if channel == "depth" {
        let depth = if profile.starts_with("gateio_") {
            "100"
        } else {
            "200"
        };

        context = context
            .with_variable("depth", depth)
            .unwrap()
            .with_variable("interval", "100ms")
            .unwrap();
    }

    context
}

// ============================================================
// EXPECTED ARGUMENT REPRESENTATIONS
// ============================================================

fn expected_arguments(profile: &str, channel: &str) -> Vec<Value> {
    let native = symbol(profile);

    let exchange = profile.split('_').next().unwrap();

    match (exchange, channel) {
        ("binance", "trades") => {
            vec![json!(format!("{}@trade", native.to_ascii_lowercase(),))]
        }

        ("binance", "depth") => {
            vec![json!(format!("{}@depth", native.to_ascii_lowercase(),))]
        }

        ("bybit", "trades") => {
            vec![json!(format!(
                "publicTrade.{}",
                native.to_ascii_uppercase(),
            ))]
        }

        ("bybit", "depth") => {
            vec![json!(format!(
                "orderbook.200.{}",
                native.to_ascii_uppercase(),
            ))]
        }

        ("bitget", _) => {
            let inst_type = match profile {
                "bitget_spot" => "spot",
                "bitget_linear" => "USDT-FUTURES",
                "bitget_inverse" => "coin-futures",
                _ => unreachable!(),
            };

            let topic = if channel == "trades" {
                "publicTrade"
            } else {
                "books"
            };

            vec![json!({
                "instType": inst_type,
                "topic": topic,
                "symbol": native,
            })]
        }

        ("okx", _) => {
            let native_channel = if channel == "trades" {
                "trades-all"
            } else {
                "books"
            };

            vec![json!({
                "channel": native_channel,
                "instId": native,
            })]
        }

        ("gateio", "trades") => {
            vec![json!(native)]
        }

        ("gateio", "depth") if profile == "gateio_spot" => {
            vec![json!(native), json!("100ms")]
        }

        ("gateio", "depth") => {
            vec![json!(native), json!("100ms"), json!("100")]
        }

        _ => unreachable!(),
    }
}

// ============================================================
// REAL PROFILE COVERAGE
// ============================================================

#[test]
fn w44_resolves_all_15_real_trade_and_depth_profiles() {
    let mut verified = 0;

    for exchange in EXCHANGES {
        for market in MARKETS {
            let profile = format!("{exchange}_{market}");

            let config = profile_config(&profile);

            for channel in ["trades", "depth"] {
                let resolved =
                    SubscriptionResolver::resolve(&config, channel, &context(&profile, channel))
                        .unwrap_or_else(|error| panic!("{profile}/{channel}: {error}"));

                assert_eq!(
                    resolved.request.arguments,
                    expected_arguments(&profile, channel),
                    "{profile}/{channel}: native arguments mismatch",
                );

                // A request ID is supplied to exercise the
                // serializer's configured native ID type.
                let subscription = config.websocket.subscription.as_ref().unwrap();

                let mut request = resolved.request.clone();

                if subscription.request_id_field.is_some() {
                    request.request_id = Some(match subscription.request_id_type.as_deref() {
                        Some("string") => json!("req-7"),
                        Some("unsigned_integer") | Some("signed_integer") => json!(7),
                        _ => json!(7),
                    });
                }

                if matches!(
                    resolved.protocol.strategy(),
                    SubscriptionStrategy::GateV4ChannelEvent
                ) {
                    request.unix_seconds = Some(1_800_000_000);
                }

                let wire = resolved
                    .protocol
                    .subscribe(&request)
                    .unwrap_or_else(|error| panic!("{profile}/{channel}: {error}"));

                let native: Value = serde_json::from_str(&wire).unwrap();

                let args = match resolved.protocol.strategy() {
                    SubscriptionStrategy::JsonRpc => &native["params"],

                    SubscriptionStrategy::JsonOperation => &native["args"],

                    SubscriptionStrategy::GateV4ChannelEvent => &native["payload"],
                };

                assert_eq!(
                    args,
                    &Value::Array(expected_arguments(&profile, channel)),
                    "{profile}/{channel}: wire arguments mismatch",
                );

                verified += 1;
            }
        }
    }

    assert_eq!(verified, 30);
}
