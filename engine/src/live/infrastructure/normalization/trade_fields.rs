use rust_decimal::Decimal;
use serde_json::Value;
use thiserror::Error;

use super::{ExtractionError, JsonExtractor, NumericConverter, NumericError, TimestampUnit};

// ============================================================
// NATIVE TRADE FIELD CONFIGURATION
// ============================================================

/// Transport-independent mapping of logical trade fields
/// to paths inside one native trade record.
///
/// REST and WebSocket may use different mappings.
///
/// All paths are relative to the extracted trade record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeTradeFieldLayout {
    pub trade_id: Option<String>,
    pub price: String,
    pub quantity: String,
    pub timestamp: String,
    pub side: Option<String>,
    pub sequence: Option<String>,
}

// ============================================================
// FIELD READER ERRORS
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TradeFieldError {
    #[error("invalid trade field configuration: {0}")]
    Configuration(String),

    #[error(transparent)]
    Extraction(#[from] ExtractionError),

    #[error(transparent)]
    Numeric(#[from] NumericError),

    #[error("invalid trade identifier representation")]
    InvalidTradeId,
}

// ============================================================
// EXTRACTED NATIVE TRADE
// ============================================================

/// Values extracted from one native trade record.
///
/// This is not yet a canonical Trade.
///
/// In particular:
///
/// - native_quantity may be signed.
/// - native_side has not been interpreted.
/// - no contract-size conversion has occurred.
/// - aggregate-trade identity has not been interpreted.
/// - instrument identity is supplied by NormalizationContext.
///
/// These distinctions must be preserved until canonical
/// conversion has enough information to apply the correct
/// economic semantics.
#[derive(Debug)]
pub struct NativeTradeFields<'a> {
    pub trade_id: Option<String>,

    pub price: Decimal,

    pub native_quantity: Decimal,

    pub timestamp_ns: i64,

    pub native_side: Option<&'a Value>,

    pub sequence: Option<u64>,
}

// ============================================================
// COMPILED TRADE FIELD PLAN
// ============================================================

/// Immutable, generic field extraction plan.
///
/// All exchange-specific differences are supplied through
/// NativeTradeFieldLayout and TimestampUnit.
///
/// The reader does not contain exchange-specific branches.
#[derive(Debug, Clone)]
pub struct TradeFieldPlan {
    layout: NativeTradeFieldLayout,

    timestamp_unit: TimestampUnit,
}

impl TradeFieldPlan {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub fn new(
        layout: NativeTradeFieldLayout,
        timestamp_unit: TimestampUnit,
    ) -> Result<Self, TradeFieldError> {
        for (name, path) in [
            ("price", layout.price.as_str()),
            ("quantity", layout.quantity.as_str()),
            ("timestamp", layout.timestamp.as_str()),
        ] {
            validate_field_path(name, path)?;
        }

        for (name, path) in [
            ("trade_id", layout.trade_id.as_deref()),
            ("side", layout.side.as_deref()),
            ("sequence", layout.sequence.as_deref()),
        ] {
            if let Some(path) = path {
                validate_field_path(name, path)?;
            }
        }

        Ok(Self {
            layout,
            timestamp_unit,
        })
    }

    // ========================================================
    // ACCESSORS
    // ========================================================

    pub fn layout(&self) -> &NativeTradeFieldLayout {
        &self.layout
    }

    pub fn timestamp_unit(&self) -> TimestampUnit {
        self.timestamp_unit
    }

    // ========================================================
    // FIELD EXTRACTION
    // ========================================================

    /// Extract typed native values from one trade record.
    ///
    /// All configured fields are required in the input.
    ///
    /// Optional layout entries mean the field is not
    /// represented by this native format.
    ///
    /// They do not mean a configured field can silently
    /// disappear from a record.
    pub fn read<'a>(&self, record: &'a Value) -> Result<NativeTradeFields<'a>, TradeFieldError> {
        // ====================================================
        // RECORD TYPE
        // ====================================================

        if !record.is_object() {
            return Err(TradeFieldError::Configuration(
                "native trade record must be an object".into(),
            ));
        }

        // ====================================================
        // PRICE
        // ====================================================

        let price_value = JsonExtractor::required(record, &self.layout.price)?;

        let price = NumericConverter::price(price_value)?;

        // ====================================================
        // NATIVE QUANTITY
        // ====================================================

        // Use decimal(), not positive_quantity().
        //
        // Some exchanges encode trade direction using
        // the sign of their native contract quantity.
        //
        // Canonical quantity validation happens after
        // the native side and quantity semantics have
        // been interpreted.
        let quantity_value = JsonExtractor::required(record, &self.layout.quantity)?;

        let native_quantity = NumericConverter::decimal(quantity_value)?;

        // ====================================================
        // EVENT TIMESTAMP
        // ====================================================

        let timestamp_value = JsonExtractor::required(record, &self.layout.timestamp)?;

        let timestamp_ns =
            NumericConverter::timestamp_ns_nonnegative(timestamp_value, self.timestamp_unit)?;

        // ====================================================
        // TRADE IDENTITY
        // ====================================================

        let trade_id = match self.layout.trade_id.as_deref() {
            Some(path) => {
                let value = JsonExtractor::required(record, path)?;

                Some(parse_trade_id(value)?)
            }

            None => None,
        };

        // ====================================================
        // NATIVE AGGRESSOR SIDE
        // ====================================================

        let native_side = match self.layout.side.as_deref() {
            Some(path) => Some(JsonExtractor::required(record, path)?),

            None => None,
        };

        // ====================================================
        // OPTIONAL SEQUENCE
        // ====================================================

        let sequence = match self.layout.sequence.as_deref() {
            Some(path) => {
                let value = JsonExtractor::required(record, path)?;

                Some(NumericConverter::u64(value)?)
            }

            None => None,
        };

        Ok(NativeTradeFields {
            trade_id,
            price,
            native_quantity,
            timestamp_ns,
            native_side,
            sequence,
        })
    }
}

// ============================================================
// FIELD PATH VALIDATION
// ============================================================

fn validate_field_path(name: &str, path: &str) -> Result<(), TradeFieldError> {
    if path.trim().is_empty() {
        return Err(TradeFieldError::Configuration(format!(
            "{name} field path cannot be empty"
        )));
    }

    if path.trim() != path {
        return Err(TradeFieldError::Configuration(format!(
            "{name} field path contains whitespace"
        )));
    }

    if path == "$" {
        return Err(TradeFieldError::Configuration(format!(
            "{name} must reference a record field"
        )));
    }

    if path.starts_with("$.") {
        if path.len() == 2 {
            return Err(TradeFieldError::Configuration(format!(
                "{name} field path is incomplete"
            )));
        }
    }

    let normalized = path.strip_prefix("$.").unwrap_or(path);

    if normalized.split('.').any(|part| part.is_empty()) {
        return Err(TradeFieldError::Configuration(format!(
            "{name} field path contains empty segments"
        )));
    }

    if normalized.split('.').any(|part| part == "$") {
        return Err(TradeFieldError::Configuration(format!(
            "{name} contains an invalid root token"
        )));
    }

    Ok(())
}

// ============================================================
// TRADE IDENTIFIER PARSING
// ============================================================

/// Preserve a native trade identifier as a decimal string.
///
/// Accepts:
/// - String IDs.
/// - Signed integer JSON IDs.
/// - Unsigned integer JSON IDs.
///
/// Rejects:
/// - Floating-point JSON values.
/// - Null.
/// - Objects and arrays.
/// - Empty strings.
///
/// IDs are identifiers, not measurements. They must not
/// pass through floating-point conversion.
fn parse_trade_id(value: &Value) -> Result<String, TradeFieldError> {
    match value {
        Value::String(id) if !id.is_empty() => Ok(id.clone()),

        Value::Number(number) => {
            if let Some(id) = number.as_u64() {
                return Ok(id.to_string());
            }

            if let Some(id) = number.as_i64() {
                return Ok(id.to_string());
            }

            Err(TradeFieldError::InvalidTradeId)
        }

        _ => Err(TradeFieldError::InvalidTradeId),
    }
}

// ============================================================
// UNIT TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn layout() -> NativeTradeFieldLayout {
        NativeTradeFieldLayout {
            trade_id: Some("execId".into()),
            price: "price".into(),
            quantity: "size".into(),
            timestamp: "time".into(),
            side: Some("side".into()),
            sequence: Some("seq".into()),
        }
    }

    // ========================================================
    // BYBIT-STYLE RECORD
    // ========================================================

    #[test]
    fn extracts_bybit_trade_fields() {
        let plan = TradeFieldPlan::new(layout(), TimestampUnit::Milliseconds).unwrap();

        let record = json!({
            "execId": "2290000001242829250",
            "price": "80838.2",
            "size": "0.000123",
            "time": "1792806119498",
            "side": "Buy",
            "seq": "116029719876"
        });

        let fields = plan.read(&record).unwrap();

        assert_eq!(fields.trade_id.as_deref(), Some("2290000001242829250"),);

        assert_eq!(fields.price.to_string(), "80838.2",);

        assert_eq!(fields.native_quantity.to_string(), "0.000123",);

        assert_eq!(fields.timestamp_ns, 1_792_806_119_498_000_000,);

        assert_eq!(fields.native_side.unwrap(), &json!("Buy"),);

        assert_eq!(fields.sequence, Some(116029719876),);
    }

    // ========================================================
    // BINANCE-STYLE RECORD
    // ========================================================

    #[test]
    fn extracts_binance_trade_fields() {
        let plan = TradeFieldPlan::new(
            NativeTradeFieldLayout {
                trade_id: Some("id".into()),
                price: "price".into(),
                quantity: "qty".into(),
                timestamp: "time".into(),
                side: Some("isBuyerMaker".into()),
                sequence: None,
            },
            TimestampUnit::Milliseconds,
        )
        .unwrap();

        let record = json!({
            "id": 123456789_u64,
            "price": "80000.12345678",
            "qty": "0.001",
            "time": 1792806119498_i64,
            "isBuyerMaker": true
        });

        let fields = plan.read(&record).unwrap();

        assert_eq!(fields.trade_id.as_deref(), Some("123456789"),);

        assert_eq!(fields.price.to_string(), "80000.12345678",);

        assert_eq!(fields.native_quantity.to_string(), "0.001",);

        assert_eq!(fields.native_side.unwrap(), &json!(true),);
    }

    // ========================================================
    // SIGNED NATIVE QUANTITY
    // ========================================================

    #[test]
    fn preserves_signed_gateio_futures_size() {
        let plan = TradeFieldPlan::new(
            NativeTradeFieldLayout {
                trade_id: Some("id".into()),
                price: "price".into(),
                quantity: "size".into(),
                timestamp: "create_time_ms".into(),
                side: None,
                sequence: None,
            },
            TimestampUnit::Milliseconds,
        )
        .unwrap();

        let record = json!({
            "id": 123456,
            "price": "80000",
            "size": -12,
            "create_time_ms": 1792806119498_i64
        });

        let fields = plan.read(&record).unwrap();

        assert_eq!(fields.native_quantity, Decimal::from(-12),);

        // Side interpretation belongs to the
        // canonical trade decoder.
        assert!(fields.native_side.is_none());
    }

    // ========================================================
    // EXACT DECIMAL PRECISION
    // ========================================================

    #[test]
    fn preserves_exact_decimal_precision() {
        let plan = TradeFieldPlan::new(layout(), TimestampUnit::Milliseconds).unwrap();

        let record = json!({
            "execId": "1",
            "price": "80000.123456789123456789",
            "size": "0.00000001",
            "time": "1792806119498",
            "side": "Sell",
            "seq": "10"
        });

        let fields = plan.read(&record).unwrap();

        assert_eq!(fields.price.to_string(), "80000.123456789123456789",);

        assert_eq!(fields.native_quantity.to_string(), "0.00000001",);
    }

    // ========================================================
    // ERROR HANDLING
    // ========================================================

    #[test]
    fn rejects_missing_required_price() {
        let plan = TradeFieldPlan::new(layout(), TimestampUnit::Milliseconds).unwrap();

        let record = json!({
            "execId": "1",
            "size": "1",
            "time": "1792806119498",
            "side": "Buy",
            "seq": "10"
        });

        assert!(plan.read(&record).is_err());
    }

    #[test]
    fn rejects_invalid_decimal_quantity() {
        let plan = TradeFieldPlan::new(layout(), TimestampUnit::Milliseconds).unwrap();

        let record = json!({
            "execId": "1",
            "price": "80000",
            "size": "invalid",
            "time": "1792806119498",
            "side": "Buy",
            "seq": "10"
        });

        assert!(plan.read(&record).is_err());
    }

    #[test]
    fn rejects_fractional_json_price() {
        let plan = TradeFieldPlan::new(layout(), TimestampUnit::Milliseconds).unwrap();

        let record = json!({
            "execId": "1",
            "price": 80000.25,
            "size": "1",
            "time": "1792806119498",
            "side": "Buy",
            "seq": "10"
        });

        assert!(plan.read(&record).is_err());
    }

    #[test]
    fn rejects_missing_configured_side() {
        let plan = TradeFieldPlan::new(layout(), TimestampUnit::Milliseconds).unwrap();

        let record = json!({
            "execId": "1",
            "price": "80000",
            "size": "1",
            "time": "1792806119498",
            "seq": "10"
        });

        assert!(plan.read(&record).is_err());
    }

    #[test]
    fn rejects_invalid_trade_id_type() {
        assert!(parse_trade_id(&json!(1.25)).is_err());
        assert!(parse_trade_id(&json!(null)).is_err());
        assert!(parse_trade_id(&json!({})).is_err());
        assert!(parse_trade_id(&json!("")).is_err());
    }

    #[test]
    fn rejects_empty_field_path() {
        let mut fields = layout();
        fields.price = "".into();

        assert!(TradeFieldPlan::new(fields, TimestampUnit::Milliseconds,).is_err(),);
    }
    #[test]
    fn binance_rest_trade_fields_are_extracted_correctly() {
        use super::*;
        use crate::live::infrastructure::normalization::{
            TradeExtractionPlan, TradeRecordRepresentation,
        };
        use serde_json::json;

        // Representative Binance Spot REST trade.
        let response = json!([
            {
                "id": 6787820768_u64,
                "price": "80860.70000000",
                "qty": "0.00007000",
                "quoteQty": "5.66024900",
                "time": 1792806211156_i64,
                "isBuyerMaker": true,
                "isBestMatch": true
            }
        ]);

        // Binance REST returns an array at the JSON root.
        let extraction =
            TradeExtractionPlan::new("$", TradeRecordRepresentation::Array, 1000).unwrap();

        let records = extraction.extract(&response).unwrap();

        assert_eq!(records.len(), 1);

        // REST field layout differs from WebSocket.
        let fields = TradeFieldPlan::new(
            NativeTradeFieldLayout {
                trade_id: Some("id".into()),
                price: "price".into(),
                quantity: "qty".into(),
                timestamp: "time".into(),
                side: Some("isBuyerMaker".into()),
                sequence: None,
            },
            TimestampUnit::Milliseconds,
        )
        .unwrap();

        let native = fields.read(records.get(0).unwrap()).unwrap();

        assert_eq!(native.trade_id.as_deref(), Some("6787820768"),);

        assert_eq!(
            native.price,
            Decimal::from_str_exact("80860.70000000").unwrap(),
        );

        assert_eq!(
            native.native_quantity,
            Decimal::from_str_exact("0.00007000").unwrap(),
        );

        assert_eq!(native.timestamp_ns, 1_792_806_211_156_000_000,);

        assert_eq!(native.native_side, Some(&json!(true)),);

        assert_eq!(native.sequence, None);
    }
}
