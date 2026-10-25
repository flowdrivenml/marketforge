use rust_decimal::Decimal;

use crate::live::config::exchange::{ExchangeId, MarketType};

use crate::live::stream::request::{CreateSourceStreamRequest, QuantityType, SourceChannel};

// ============================================================
// NORMALIZATION CONTEXT
// ============================================================

/// Immutable instrument and source-stream context used
/// when normalizing exchange-native market data.
///
/// This context is created after source-stream request
/// validation.
///
/// It provides the instrument identity and quantity
/// semantics needed to interpret native trade and L2
/// records.
///
/// The context does not:
///
/// - Own WebSocket connections.
/// - Execute REST requests.
/// - Maintain order-book state.
/// - Track trade deduplication.
/// - Perform sequence synchronization.
/// - Resolve missing instrument metadata.
/// - Generate event timestamps.
///
/// All normalization operations receive this context
/// as an immutable reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizationContext {
    // ========================================================
    // SOURCE IDENTITY
    // ========================================================
    stream_id: String,

    instrument_id: i64,

    exchange: ExchangeId,

    symbol: String,

    market_category: MarketType,

    instrument_type: String,

    // ========================================================
    // INSTRUMENT ASSETS
    // ========================================================
    base_asset: Option<String>,

    quote_asset: Option<String>,

    settlement_asset: Option<String>,

    // ========================================================
    // QUANTITY SEMANTICS
    // ========================================================
    quantity_type: QuantityType,

    contract_value: Option<Decimal>,

    contract_value_asset: Option<String>,

    // ========================================================
    // CHANNEL
    // ========================================================
    channel: SourceChannel,
}

// ============================================================
// CONSTRUCTION
// ============================================================

impl NormalizationContext {
    /// Construct a normalization context from an existing
    /// source-stream creation request.
    ///
    /// `stream_id` must be the authoritative identity
    /// assigned to the source stream.
    ///
    /// The request is validated using its existing
    /// MarketForge validation implementation.
    ///
    /// Contract values are parsed directly into Decimal.
    /// No floating-point intermediate representation
    /// is permitted.
    pub fn new(
        request: &CreateSourceStreamRequest,
        stream_id: impl Into<String>,
    ) -> Result<Self, String> {
        // ====================================================
        // VALIDATE SOURCE STREAM REQUEST
        // ====================================================

        request.validate().map_err(|error| {
            format!(
                "normalization context: invalid source \
                 stream request: {error}"
            )
        })?;

        // ====================================================
        // VALIDATE STREAM IDENTITY
        // ====================================================

        let stream_id = stream_id.into();

        if stream_id.trim().is_empty() {
            return Err("normalization context: stream_id \
                 cannot be empty"
                .into());
        }

        // ====================================================
        // PARSE CONTRACT VALUE
        // ====================================================

        let contract_value = request
            .contract_value
            .as_deref()
            .map(parse_contract_value)
            .transpose()?;

        // ====================================================
        // CONSTRUCT IMMUTABLE CONTEXT
        // ====================================================

        Ok(Self {
            stream_id,

            instrument_id: request.instrument_id,

            exchange: request.exchange.clone(),

            symbol: request.symbol.clone(),

            market_category: request.market_category.clone(),

            instrument_type: request.instrument_type.clone(),

            base_asset: request.base_asset.clone(),

            quote_asset: request.quote_asset.clone(),

            settlement_asset: request.settlement_asset.clone(),

            quantity_type: request.quantity_type,

            contract_value,

            contract_value_asset: request.contract_value_asset.clone(),

            channel: request.channel,
        })
    }
}

// ============================================================
// ACCESSORS
// ============================================================

impl NormalizationContext {
    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    pub fn instrument_id(&self) -> i64 {
        self.instrument_id
    }

    pub fn exchange(&self) -> ExchangeId {
        self.exchange.clone()
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    pub fn market_category(&self) -> MarketType {
        self.market_category.clone()
    }

    pub fn instrument_type(&self) -> &str {
        &self.instrument_type
    }

    pub fn base_asset(&self) -> Option<&str> {
        self.base_asset.as_deref()
    }

    pub fn quote_asset(&self) -> Option<&str> {
        self.quote_asset.as_deref()
    }

    pub fn settlement_asset(&self) -> Option<&str> {
        self.settlement_asset.as_deref()
    }

    pub fn quantity_type(&self) -> QuantityType {
        self.quantity_type
    }

    pub fn contract_value(&self) -> Option<Decimal> {
        self.contract_value
    }

    pub fn contract_value_asset(&self) -> Option<&str> {
        self.contract_value_asset.as_deref()
    }

    pub fn channel(&self) -> SourceChannel {
        self.channel
    }
}

// ============================================================
// CONTRACT VALUE PARSING
// ============================================================

/// Parse an exact, positive contract value.
///
/// Contract values originate from validated instrument
/// metadata and must be interpreted without floating-point
/// conversion.
///
/// The parser deliberately accepts ordinary decimal
/// representations rather than performing approximate
/// numeric conversion.
///
/// Examples:
///
/// "100"        -> Decimal(100)
/// "0.001"      -> Decimal(0.001)
/// "0.00000001" -> Decimal(0.00000001)
///
/// Zero and negative contract values are invalid.
fn parse_contract_value(value: &str) -> Result<Decimal, String> {
    let value = value.trim();

    if value.is_empty() {
        return Err("normalization context: contract_value \
             cannot be empty"
            .into());
    }

    let decimal = Decimal::from_str_exact(value).map_err(|error| {
        format!(
            "normalization context: invalid exact \
             contract_value '{value}': {error}"
        )
    })?;

    if decimal <= Decimal::ZERO {
        return Err(format!(
            "normalization context: contract_value \
             must be positive, got '{value}'"
        ));
    }

    Ok(decimal)
}

// ============================================================
// UNIT TESTS
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================
    // EXACT CONTRACT VALUES
    // ========================================================

    #[test]
    fn parses_integer_contract_value() {
        let value = parse_contract_value("100").unwrap();

        assert_eq!(value, Decimal::new(100, 0),);
    }

    #[test]
    fn parses_fractional_contract_value() {
        let value = parse_contract_value("0.001").unwrap();

        assert_eq!(value, Decimal::new(1, 3),);
    }

    #[test]
    fn preserves_small_exact_contract_values() {
        let value = parse_contract_value("0.00000001").unwrap();

        assert_eq!(value, Decimal::new(1, 8),);
    }

    // ========================================================
    // INVALID CONTRACT VALUES
    // ========================================================

    #[test]
    fn rejects_zero_contract_value() {
        assert!(parse_contract_value("0").is_err(),);

        assert!(parse_contract_value("0.000").is_err(),);
    }

    #[test]
    fn rejects_negative_contract_value() {
        assert!(parse_contract_value("-100").is_err(),);
    }

    #[test]
    fn rejects_invalid_contract_value() {
        assert!(parse_contract_value("invalid").is_err(),);
    }

    #[test]
    fn rejects_empty_contract_value() {
        assert!(parse_contract_value("").is_err(),);

        assert!(parse_contract_value("   ").is_err(),);
    }

    // ========================================================
    // NO FLOATING-POINT ROUNDING
    // ========================================================

    #[test]
    fn preserves_decimal_precision() {
        let value = parse_contract_value("0.123456789123456789").unwrap();

        assert_eq!(value.to_string(), "0.123456789123456789",);
    }

    #[test]
    fn rejects_unrepresentable_precision() {
        let result = parse_contract_value("0.123456789123456789123456789123456789");

        assert!(
            result.is_err(),
            "contract parser must not silently round \
             unrepresentable values"
        );
    }
    #[test]
    fn normalization_context_preserves_authoritative_instrument_metadata() {
        let request = CreateSourceStreamRequest {
            instrument_id: 10792,
            exchange: ExchangeId::Binance,
            symbol: "BTCUSD_PERP".into(),
            market_category: MarketType::Inverse,
            instrument_type: "perpetual".into(),

            base_asset: Some("BTC".into()),
            quote_asset: Some("USD".into()),
            settlement_asset: Some("BTC".into()),

            quantity_type: QuantityType::Contracts,
            contract_value: Some("100".into()),
            contract_value_asset: Some("USD".into()),

            channel: SourceChannel::Depth,
        };

        let context = NormalizationContext::new(&request, "stream-42").unwrap();

        assert_eq!(context.stream_id(), "stream-42");
        assert_eq!(context.instrument_id(), 10792);
        assert_eq!(context.exchange(), ExchangeId::Binance);
        assert_eq!(context.market_category(), MarketType::Inverse);
        assert_eq!(context.symbol(), "BTCUSD_PERP");

        assert_eq!(context.base_asset(), Some("BTC"));
        assert_eq!(context.quote_asset(), Some("USD"));
        assert_eq!(context.settlement_asset(), Some("BTC"));

        assert_eq!(context.quantity_type(), QuantityType::Contracts);

        assert_eq!(context.contract_value(), Some(Decimal::from(100)),);

        assert_eq!(context.contract_value_asset(), Some("USD"));
        assert_eq!(context.channel(), SourceChannel::Depth);
    }
    #[test]
    fn normalization_context_rejects_missing_contract_metadata() {
        let request = CreateSourceStreamRequest {
            instrument_id: 10792,
            exchange: ExchangeId::Binance,
            symbol: "BTCUSD_PERP".into(),
            market_category: MarketType::Inverse,
            instrument_type: "perpetual".into(),

            base_asset: Some("BTC".into()),
            quote_asset: Some("USD".into()),
            settlement_asset: Some("BTC".into()),

            quantity_type: QuantityType::Contracts,

            // Insufficient metadata for contract normalization.
            contract_value: None,
            contract_value_asset: None,

            channel: SourceChannel::Depth,
        };

        assert!(
            NormalizationContext::new(&request, "stream-42").is_err(),
            "contract-denominated quantities must not be normalized \
         without authoritative contract metadata",
        );
    }
}
