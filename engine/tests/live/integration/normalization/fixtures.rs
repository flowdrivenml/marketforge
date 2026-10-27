use marketforge_engine::live::infrastructure::normalization::{
    NormalizerFactory, NormalizerLimits, QuantityPrecisionPolicy,
};
use marketforge_engine::live::{
    config::exchange::{ExchangeId, MarketType},
    config::{ExchangeConfigStore, FormatConfigStore},
    stream::request::{CreateSourceStreamRequest, QuantityType, SourceChannel},
};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use serde_json::Value;

pub const PROFILES: [&str; 15] = [
    "binance_spot",
    "binance_linear",
    "binance_inverse",
    "bybit_spot",
    "bybit_linear",
    "bybit_inverse",
    "okx_spot",
    "okx_linear",
    "okx_inverse",
    "bitget_spot",
    "bitget_linear",
    "bitget_inverse",
    "gateio_spot",
    "gateio_linear",
    "gateio_inverse",
];

pub fn data_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../data")
}

pub fn fixture_root() -> PathBuf {
    data_root().join(".cache/live")
}

pub fn factory() -> NormalizerFactory {
    let root = data_root().join(".config/live");

    let exchanges = ExchangeConfigStore::load_dir(root.join("exchanges"))
        .expect("exchange configurations must load");

    let formats =
        FormatConfigStore::load_dir(root.join("formats")).expect("format configurations must load");

    let limits = NormalizerLimits {
        precision: QuantityPrecisionPolicy::Round { decimal_places: 18 },
        ..NormalizerLimits::default()
    };

    NormalizerFactory::new(Arc::new(exchanges), Arc::new(formats), limits)
        .expect("normalization factory must initialize")
}

pub fn load_fixture(path: impl AsRef<Path>) -> Value {
    let path = path.as_ref();

    let bytes = fs::read(path)
        .unwrap_or_else(|error| panic!("missing fixture {}: {error}", path.display(),));

    serde_json::from_slice(&bytes)
        .unwrap_or_else(|error| panic!("invalid JSON fixture {}: {error}", path.display(),))
}

pub fn ws_fixture(channel: SourceChannel, profile: &str) -> Value {
    let kind = match channel {
        SourceChannel::Trades => "trades",
        SourceChannel::Depth => "depth",
    };

    load_fixture(
        fixture_root()
            .join("websocket-fixtures")
            .join(kind)
            .join(format!("{profile}.json")),
    )
}

pub fn rest_fixture(profile: &str, endpoint: &str) -> Value {
    load_fixture(
        fixture_root()
            .join("rest-fixtures")
            .join(format!("{profile}__{endpoint}.json")),
    )
}

pub fn parse_profile(profile: &str) -> (ExchangeId, MarketType) {
    let (exchange, market) = profile.split_once('_').expect("invalid exchange profile");

    let exchange = match exchange {
        "binance" => ExchangeId::Binance,
        "bybit" => ExchangeId::Bybit,
        "okx" => ExchangeId::Okx,
        "bitget" => ExchangeId::Bitget,
        "gateio" => ExchangeId::Gateio,
        other => panic!("unknown exchange {other}"),
    };

    let market = match market {
        "spot" => MarketType::Spot,
        "linear" => MarketType::Linear,
        "inverse" => MarketType::Inverse,
        other => panic!("unknown market {other}"),
    };

    (exchange, market)
}

/// Extract a native symbol from a captured message.
///
/// This is deliberately conservative. When the capture
/// doesn't contain an identifiable symbol, the test fails
/// instead of silently substituting BTCUSDT.
pub fn captured_symbol(value: &Value) -> Option<String> {
    for key in ["symbol", "instId", "currency_pair", "contract", "s"] {
        if let Some(symbol) = value.get(key).and_then(Value::as_str) {
            if !symbol.is_empty() {
                return Some(symbol.to_owned());
            }
        }
    }

    if let Some(topic) = value.get("topic").and_then(Value::as_str) {
        if let Some(symbol) = topic.rsplit('.').next() {
            if !symbol.is_empty() {
                return Some(symbol.to_owned());
            }
        }
    }

    for key in ["arg", "data", "result", "payload"] {
        if let Some(child) = value.get(key) {
            if let Some(symbol) = captured_symbol(child) {
                return Some(symbol);
            }
        }
    }

    if let Some(array) = value.as_array() {
        for item in array {
            if let Some(symbol) = captured_symbol(item) {
                return Some(symbol);
            }
        }
    }

    None
}

/// A captured native message may be stored directly or
/// inside a capture wrapper.
///
/// Keep this extraction explicit: don't recursively search
/// arbitrary JSON for an object that happens to decode.
pub fn native_ws_messages(fixture: &Value) -> &[Value] {
    fixture
        .as_array()
        .expect("WebSocket fixture must be an array of native messages")
}

pub fn native_rest_body(fixture: &Value) -> &Value {
    fixture
        .get("body")
        .or_else(|| fixture.get("response"))
        .unwrap_or(fixture)
}
fn fixture_quantity_type(profile: &str, channel: SourceChannel) -> QuantityType {
    match (profile, channel) {
        // Spot instruments
        ("binance_spot" | "bybit_spot" | "okx_spot" | "bitget_spot" | "gateio_spot", _) => {
            QuantityType::Base
        }

        // Linear base-denominated instruments.
        // Binance and Bybit require catalog verification.
        ("binance_linear" | "bybit_linear" | "bitget_linear", _) => QuantityType::Base,

        // Contract-denominated derivatives.
        (
            "binance_inverse" | "okx_linear" | "okx_inverse" | "gateio_linear" | "gateio_inverse",
            _,
        ) => QuantityType::Contracts,

        // Bybit inverse differs between trades and depth.
        ("bybit_inverse", SourceChannel::Trades) => QuantityType::Quote,

        ("bybit_inverse", SourceChannel::Depth) => QuantityType::Contracts,

        // Bitget inverse trades are quote-denominated.
        ("bitget_inverse", SourceChannel::Trades) => QuantityType::Quote,

        // The depth format explicitly requires verification.
        ("bitget_inverse", SourceChannel::Depth) => {
            // Temporary assumption for fixture decoding.
            // Native depth quantity semantics require verification.
            QuantityType::Quote
        }

        _ => panic!("unsupported fixture profile: {profile}"),
    }
}
pub fn request(profile: &str, symbol: String, channel: SourceChannel) -> CreateSourceStreamRequest {
    let (exchange, market_category) = parse_profile(profile);

    let quantity_type = fixture_quantity_type(profile, channel);

    // All current fixtures are BTC instruments.
    // These assets must match the captured instrument.
    let base_asset = Some("BTC".to_owned());

    let quote_asset = Some(
        if market_category == MarketType::Inverse {
            "USD"
        } else {
            "USDT"
        }
        .to_owned(),
    );

    // Contract specifications must come from authoritative
    // instrument metadata. Never invent a value here.
    //
    // The current fixture set does not include those values,
    // so contract-denominated profiles deliberately fail
    // until the test metadata is supplied.
    // TEST FIXTURE PLACEHOLDER
    //
    // Contract value = 100 is temporary.
    // Replace with authoritative instrument metadata later.
    //
    // These tests verify decoding compatibility, not
    // economic correctness of converted quantities.

    let (contract_value, contract_value_asset) = if quantity_type == QuantityType::Contracts {
        (
            Some("100".to_owned()),
            Some(
                if market_category == MarketType::Inverse {
                    "USD"
                } else {
                    "BTC"
                }
                .to_owned(),
            ),
        )
    } else {
        (None, None)
    };

    let settlement_asset = match market_category {
        MarketType::Spot => None,
        MarketType::Linear => Some("USDT".to_owned()),
        MarketType::Inverse => Some("BTC".to_owned()),
    };

    CreateSourceStreamRequest {
        instrument_id: 100,
        exchange,
        symbol,
        market_category,
        instrument_type: match market_category {
            MarketType::Spot => "spot".into(),
            _ => "perpetual".into(),
        },
        base_asset,
        quote_asset,
        quantity_type,
        contract_value,
        contract_value_asset,
        channel,
        settlement_asset,
    }
}
