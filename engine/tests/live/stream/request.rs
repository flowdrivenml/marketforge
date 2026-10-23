use marketforge_engine::live::stream::request::{
    CreateSourceStreamRequest, QuantityType, SourceChannel,
};

use marketforge_engine::live::config::exchange::{ExchangeId, MarketType};

// ============================================================
// FIXTURES
// ============================================================

const SPOT_REQUEST: &str = r#"
{
    "instrument_id": 15,
    "exchange": "binance",
    "symbol": "BTCUSDT",
    "market_category": "spot",
    "instrument_type": "spot",
    "base_asset": "BTC",
    "quote_asset": "USDT",
    "settlement_asset": null,
    "quantity_type": "base",
    "contract_value": null,
    "contract_value_asset": null,
    "channel": "trades"
}
"#;

const INVERSE_REQUEST: &str = r#"
{
    "instrument_id": 42,
    "exchange": "binance",
    "symbol": "BTCUSD_PERP",
    "market_category": "inverse",
    "instrument_type": "perpetual",
    "base_asset": "BTC",
    "quote_asset": "USD",
    "settlement_asset": "BTC",
    "quantity_type": "contracts",
    "contract_value": "100",
    "contract_value_asset": "USD",
    "channel": "depth"
}
"#;

const LINEAR_REQUEST: &str = r#"
{
    "instrument_id": 43,
    "exchange": "bybit",
    "symbol": "BTCUSDT",
    "market_category": "linear",
    "instrument_type": "perpetual",
    "base_asset": "BTC",
    "quote_asset": "USDT",
    "settlement_asset": "USDT",
    "quantity_type": "base",
    "contract_value": null,
    "contract_value_asset": null,
    "channel": "trades"
}
"#;

fn spot_request() -> CreateSourceStreamRequest {
    serde_json::from_str(SPOT_REQUEST).unwrap()
}

fn inverse_request() -> CreateSourceStreamRequest {
    serde_json::from_str(INVERSE_REQUEST).unwrap()
}

fn modified_request(
    fixture: &str,
    field: &str,
    value: serde_json::Value,
) -> CreateSourceStreamRequest {
    let mut json: serde_json::Value = serde_json::from_str(fixture).unwrap();

    json[field] = value;

    serde_json::from_value(json).unwrap()
}

fn assert_invalid(request: &CreateSourceStreamRequest, expected: &str) {
    let error = request
        .validate()
        .expect_err("Expected request validation to fail");

    assert!(
        error.contains(expected),
        "Expected error containing {expected:?}, got: {error}"
    );
}

// ============================================================
// VALID REQUESTS
// ============================================================

#[test]
fn accept_spot_trade_request() {
    spot_request().validate().unwrap();
}

#[test]
fn accept_spot_depth_request() {
    let request = modified_request(SPOT_REQUEST, "channel", serde_json::json!("depth"));

    request.validate().unwrap();
    assert_eq!(request.channel, SourceChannel::Depth);
}

#[test]
fn accept_linear_trade_request() {
    let request: CreateSourceStreamRequest = serde_json::from_str(LINEAR_REQUEST).unwrap();

    request.validate().unwrap();

    assert_eq!(request.market_category, MarketType::Linear);
    assert_eq!(request.exchange, ExchangeId::Bybit);
    assert_eq!(request.quantity_type, QuantityType::Base);
}

#[test]
fn accept_inverse_depth_request() {
    let request = inverse_request();

    request.validate().unwrap();

    assert_eq!(request.market_category, MarketType::Inverse);
    assert_eq!(request.quantity_type, QuantityType::Contracts);
    assert_eq!(request.channel, SourceChannel::Depth);
}

#[test]
fn accept_quote_quantity_type() {
    let request = modified_request(SPOT_REQUEST, "quantity_type", serde_json::json!("quote"));

    request.validate().unwrap();

    assert_eq!(request.quantity_type, QuantityType::Quote);
}

#[test]
fn accept_optional_assets_as_null() {
    let mut request = spot_request();

    request.base_asset = None;
    request.quote_asset = None;
    request.settlement_asset = None;

    request.validate().unwrap();
}

#[test]
fn accept_valid_contract_value_for_base_quantity() {
    let mut request = spot_request();

    request.contract_value = Some("0.001".into());
    request.contract_value_asset = Some("BTC".into());

    request.validate().unwrap();
}

#[test]
fn accept_small_exact_contract_value() {
    let mut request = inverse_request();

    request.contract_value = Some("0.00000001".into());

    request.validate().unwrap();
}

#[test]
fn accept_large_exact_contract_value() {
    let mut request = inverse_request();

    request.contract_value = Some("1000000000000000000".into());

    request.validate().unwrap();
}

// ============================================================
// DESERIALIZATION
// ============================================================

#[test]
fn deserialize_spot_request() {
    let request = spot_request();

    assert_eq!(request.instrument_id, 15);
    assert_eq!(request.exchange, ExchangeId::Binance);
    assert_eq!(request.market_category, MarketType::Spot);
    assert_eq!(request.symbol, "BTCUSDT");
    assert_eq!(request.instrument_type, "spot");
    assert_eq!(request.base_asset.as_deref(), Some("BTC"));
    assert_eq!(request.quote_asset.as_deref(), Some("USDT"));
    assert_eq!(request.settlement_asset, None);
    assert_eq!(request.quantity_type, QuantityType::Base);
    assert_eq!(request.contract_value, None);
    assert_eq!(request.contract_value_asset, None);
    assert_eq!(request.channel, SourceChannel::Trades);
}

#[test]
fn deserialize_inverse_request() {
    let request = inverse_request();

    assert_eq!(request.instrument_id, 42);
    assert_eq!(request.exchange, ExchangeId::Binance);
    assert_eq!(request.market_category, MarketType::Inverse);

    assert_eq!(request.contract_value.as_deref(), Some("100"));
    assert_eq!(request.contract_value_asset.as_deref(), Some("USD"));

    assert_eq!(request.settlement_asset.as_deref(), Some("BTC"));
}

#[test]
fn request_round_trip() {
    for fixture in [SPOT_REQUEST, LINEAR_REQUEST, INVERSE_REQUEST] {
        let original: CreateSourceStreamRequest = serde_json::from_str(fixture).unwrap();

        let serialized = serde_json::to_string(&original).unwrap();

        let restored: CreateSourceStreamRequest = serde_json::from_str(&serialized).unwrap();

        assert_eq!(original, restored);
    }
}

#[test]
fn request_preserves_json_values() {
    for fixture in [SPOT_REQUEST, LINEAR_REQUEST, INVERSE_REQUEST] {
        let original: serde_json::Value = serde_json::from_str(fixture).unwrap();

        let request: CreateSourceStreamRequest = serde_json::from_str(fixture).unwrap();

        let serialized = serde_json::to_value(&request).unwrap();

        assert_eq!(original, serialized);
    }
}

// ============================================================
// REQUIRED FIELDS
// ============================================================

#[test]
fn reject_missing_instrument_id() {
    let mut json: serde_json::Value = serde_json::from_str(SPOT_REQUEST).unwrap();

    json.as_object_mut().unwrap().remove("instrument_id");

    assert!(serde_json::from_value::<CreateSourceStreamRequest>(json).is_err());
}

#[test]
fn reject_missing_exchange() {
    let mut json: serde_json::Value = serde_json::from_str(SPOT_REQUEST).unwrap();

    json.as_object_mut().unwrap().remove("exchange");

    assert!(serde_json::from_value::<CreateSourceStreamRequest>(json).is_err());
}

#[test]
fn reject_missing_symbol() {
    let mut json: serde_json::Value = serde_json::from_str(SPOT_REQUEST).unwrap();

    json.as_object_mut().unwrap().remove("symbol");

    assert!(serde_json::from_value::<CreateSourceStreamRequest>(json).is_err());
}

#[test]
fn reject_missing_market_category() {
    let mut json: serde_json::Value = serde_json::from_str(SPOT_REQUEST).unwrap();

    json.as_object_mut().unwrap().remove("market_category");

    assert!(serde_json::from_value::<CreateSourceStreamRequest>(json).is_err());
}

#[test]
fn reject_missing_instrument_type() {
    let mut json: serde_json::Value = serde_json::from_str(SPOT_REQUEST).unwrap();

    json.as_object_mut().unwrap().remove("instrument_type");

    assert!(serde_json::from_value::<CreateSourceStreamRequest>(json).is_err());
}

#[test]
fn reject_missing_quantity_type() {
    let mut json: serde_json::Value = serde_json::from_str(SPOT_REQUEST).unwrap();

    json.as_object_mut().unwrap().remove("quantity_type");

    assert!(serde_json::from_value::<CreateSourceStreamRequest>(json).is_err());
}

#[test]
fn reject_missing_channel() {
    let mut json: serde_json::Value = serde_json::from_str(SPOT_REQUEST).unwrap();

    json.as_object_mut().unwrap().remove("channel");

    assert!(serde_json::from_value::<CreateSourceStreamRequest>(json).is_err());
}

#[test]
fn reject_unknown_json_field() {
    let mut json: serde_json::Value = serde_json::from_str(SPOT_REQUEST).unwrap();

    json["unexpected_field"] = serde_json::json!(true);

    assert!(serde_json::from_value::<CreateSourceStreamRequest>(json).is_err());
}

// ============================================================
// ENUM VALIDATION
// ============================================================

#[test]
fn reject_unknown_exchange() {
    let mut json: serde_json::Value = serde_json::from_str(SPOT_REQUEST).unwrap();

    json["exchange"] = serde_json::json!("unknown_exchange");

    assert!(serde_json::from_value::<CreateSourceStreamRequest>(json).is_err());
}

#[test]
fn reject_unknown_market_category() {
    let mut json: serde_json::Value = serde_json::from_str(SPOT_REQUEST).unwrap();

    json["market_category"] = serde_json::json!("futures");

    assert!(serde_json::from_value::<CreateSourceStreamRequest>(json).is_err());
}

#[test]
fn reject_unknown_quantity_type() {
    let mut json: serde_json::Value = serde_json::from_str(SPOT_REQUEST).unwrap();

    json["quantity_type"] = serde_json::json!("lots");

    assert!(serde_json::from_value::<CreateSourceStreamRequest>(json).is_err());
}

#[test]
fn reject_unknown_channel() {
    let mut json: serde_json::Value = serde_json::from_str(SPOT_REQUEST).unwrap();

    json["channel"] = serde_json::json!("candles");

    assert!(serde_json::from_value::<CreateSourceStreamRequest>(json).is_err());
}

// ============================================================
// INSTRUMENT IDENTITY VALIDATION
// ============================================================

#[test]
fn reject_zero_instrument_id() {
    let mut request = spot_request();

    request.instrument_id = 0;

    assert_invalid(&request, "instrument_id");
}

#[test]
fn reject_negative_instrument_id() {
    let mut request = spot_request();

    request.instrument_id = -15;

    assert_invalid(&request, "instrument_id");
}

#[test]
fn reject_empty_symbol() {
    let mut request = spot_request();

    request.symbol.clear();

    assert_invalid(&request, "symbol");
}

#[test]
fn reject_whitespace_only_symbol() {
    let mut request = spot_request();

    request.symbol = "   ".into();

    assert_invalid(&request, "symbol");
}

#[test]
fn reject_symbol_with_internal_whitespace() {
    let mut request = spot_request();

    request.symbol = "BTC USDT".into();

    assert_invalid(&request, "symbol");
}

#[test]
fn reject_symbol_with_control_characters() {
    let mut request = spot_request();

    request.symbol = "BTC\nUSDT".into();

    assert_invalid(&request, "symbol");
}

#[test]
fn reject_empty_instrument_type() {
    let mut request = spot_request();

    request.instrument_type.clear();

    assert_invalid(&request, "instrument_type");
}

#[test]
fn reject_instrument_type_with_whitespace() {
    let mut request = spot_request();

    request.instrument_type = "perpetual swap".into();

    assert_invalid(&request, "instrument_type");
}

// ============================================================
// ASSET VALIDATION
// ============================================================

#[test]
fn reject_empty_base_asset() {
    let mut request = spot_request();

    request.base_asset = Some(String::new());

    assert_invalid(&request, "base_asset");
}

#[test]
fn reject_invalid_quote_asset() {
    let mut request = spot_request();

    request.quote_asset = Some("US DT".into());

    assert_invalid(&request, "quote_asset");
}

#[test]
fn reject_invalid_settlement_asset() {
    let mut request = inverse_request();

    request.settlement_asset = Some(" BTC".into());

    assert_invalid(&request, "settlement_asset");
}

#[test]
fn reject_empty_contract_value_asset() {
    let mut request = inverse_request();

    request.contract_value_asset = Some(String::new());

    assert_invalid(&request, "contract_value_asset");
}

// ============================================================
// CONTRACT METADATA
// ============================================================

#[test]
fn reject_missing_contract_value_for_contract_quantity() {
    let mut request = inverse_request();

    request.contract_value = None;
    request.contract_value_asset = None;

    assert_invalid(&request, "requires contract_value");
}

#[test]
fn reject_missing_contract_value_asset() {
    let mut request = inverse_request();

    request.contract_value_asset = None;

    assert_invalid(&request, "contract_value_asset");
}

#[test]
fn reject_contract_value_without_asset() {
    let mut request = spot_request();

    request.contract_value = Some("100".into());
    request.contract_value_asset = None;

    assert_invalid(&request, "contract_value_asset");
}

#[test]
fn reject_contract_asset_without_value() {
    let mut request = spot_request();

    request.contract_value = None;
    request.contract_value_asset = Some("USD".into());

    assert_invalid(&request, "contract_value");
}

#[test]
fn reject_empty_contract_value() {
    let mut request = inverse_request();

    request.contract_value = Some(String::new());

    assert_invalid(&request, "contract_value");
}

#[test]
fn reject_non_numeric_contract_value() {
    let mut request = inverse_request();

    request.contract_value = Some("not-a-number".into());

    assert_invalid(&request, "contract_value");
}

#[test]
fn reject_zero_contract_value() {
    let mut request = inverse_request();

    request.contract_value = Some("0".into());

    assert_invalid(&request, "greater than zero");
}

#[test]
fn reject_negative_contract_value() {
    let mut request = inverse_request();

    request.contract_value = Some("-100".into());

    assert_invalid(&request, "greater than zero");
}

#[test]
fn reject_contract_value_with_whitespace() {
    let mut request = inverse_request();

    request.contract_value = Some(" 100".into());

    assert_invalid(&request, "whitespace");
}

#[test]
fn reject_contract_value_exceeding_decimal_precision() {
    let mut request = inverse_request();

    request.contract_value = Some("123456789012345678901234567890123456789".into());

    assert_invalid(&request, "exact decimal");
}

// ============================================================
// MISCELLANEOUS
// ============================================================

#[test]
fn validation_does_not_mutate_request() {
    let request = inverse_request();

    let original = request.clone();

    request.validate().unwrap();

    assert_eq!(request, original);
}

#[test]
fn validation_is_repeatable() {
    let request = spot_request();

    request.validate().unwrap();
    request.validate().unwrap();
}

#[test]
fn quantity_type_as_str() {
    assert_eq!(QuantityType::Base.as_str(), "base");
    assert_eq!(QuantityType::Quote.as_str(), "quote");
    assert_eq!(QuantityType::Contracts.as_str(), "contracts");
}

#[test]
fn source_channel_as_str() {
    assert_eq!(SourceChannel::Trades.as_str(), "trades");
    assert_eq!(SourceChannel::Depth.as_str(), "depth");
}
