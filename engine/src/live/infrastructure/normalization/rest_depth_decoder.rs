use super::DepthTimestampProvenance;
use super::RestDepthNormalizationCompiler;
use crate::live::config::format::DepthFormatConfig;
use crate::{
    canonical::{BookSide, EventEnvelope, Exchange, L2LevelUpdate},
    live::{config::exchange::ExchangeId, stream::request::SourceChannel},
};
use serde::Serialize;
use serde_json::Value;
use thiserror::Error;

use super::{
    DepthMessageKind, JsonExtractor, NativeDepthLevel, NormalizationContext, NormalizationSource,
    NormalizedDepth, QuantityNormalizer, QuantityPrecisionPolicy, RestDepthNormalizationPlan,
    RestDepthSnapshotReader,
};

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum RestDepthDecodeError {
    #[error("invalid REST decoder context: {0}")]
    Context(String),

    #[error("REST snapshot validation failed: {0}")]
    Snapshot(String),

    #[error("REST depth level extraction failed: {0}")]
    Levels(String),

    #[error("REST quantity normalization failed: {0}")]
    Quantity(String),

    #[error("canonical REST depth construction failed: {0}")]
    Canonical(String),
}

// ============================================================
// DECODER
// ============================================================

#[derive(Debug, Clone)]
pub struct RestDepthDecoder {
    plan: RestDepthNormalizationPlan,
    reader: RestDepthSnapshotReader,
    precision: QuantityPrecisionPolicy,
}

impl RestDepthDecoder {
    // ============================================================
    // ATOMIC COMPILATION
    // ============================================================

    /// Compile the REST extraction plan and application-status
    /// validator from the same endpoint configuration.
    ///
    /// The caller cannot supply an independently compiled
    /// reader belonging to a different endpoint.
    pub fn compile<E: Serialize>(
        depth_format: &DepthFormatConfig,
        endpoint: &E,
        max_levels: usize,
        precision: QuantityPrecisionPolicy,
    ) -> Result<Self, RestDepthDecodeError> {
        precision
            .validate()
            .map_err(|error| RestDepthDecodeError::Context(error.to_string()))?;

        depth_format
            .validate()
            .map_err(RestDepthDecodeError::Context)?;

        let plan = RestDepthNormalizationCompiler::compile(depth_format, endpoint, max_levels)
            .map_err(|error| RestDepthDecodeError::Context(error.to_string()))?;

        let reader = RestDepthSnapshotReader::compile(plan.clone(), endpoint)
            .map_err(|error| RestDepthDecodeError::Context(error.to_string()))?;

        Ok(Self {
            plan,
            reader,
            precision,
        })
    }
    /// Decode original REST response bytes with exact JSON
    /// numeric representation preservation.
    ///
    /// The authoritative REST transport must supply these
    /// bytes without first converting them through floating
    /// point or a lossy intermediate representation.
    pub fn decode_bytes(
        &self,
        response_bytes: &[u8],
        context: &NormalizationContext,
        receive_timestamp_ns: i64,
        requested_depth: u64,
    ) -> Result<NormalizedDepth, RestDepthDecodeError> {
        let response: Value = serde_json::from_slice(response_bytes).map_err(|error| {
            RestDepthDecodeError::Snapshot(format!("invalid native REST JSON: {error}"))
        })?;

        self.decode_internal(
            &response,
            context,
            receive_timestamp_ns,
            requested_depth,
            true,
        )
    }
    /// Decode a successful REST response into one canonical
    /// snapshot without committing it to an order book.
    ///
    /// `receive_timestamp_ns` must come from the trusted
    /// REST transport boundary. It is not an exchange
    /// timestamp and must be strictly positive.
    ///
    /// `requested_depth` is the depth authorized for this
    /// particular REST request, not a WebSocket depth.
    pub fn decode(
        &self,
        response: &Value,
        context: &NormalizationContext,
        receive_timestamp_ns: i64,
        requested_depth: u64,
    ) -> Result<NormalizedDepth, RestDepthDecodeError> {
        self.decode_internal(
            response,
            context,
            receive_timestamp_ns,
            requested_depth,
            false,
        )
    }
    fn decode_internal(
        &self,
        response: &Value,
        context: &NormalizationContext,
        receive_timestamp_ns: i64,
        requested_depth: u64,
        lossless_json: bool,
    ) -> Result<NormalizedDepth, RestDepthDecodeError> {
        // ============================================================
        // SOURCE IDENTITY
        // ============================================================

        if context.exchange() != self.plan.exchange {
            return Err(RestDepthDecodeError::Context(format!(
                "REST depth exchange mismatch: expected {:?}, got {:?}",
                self.plan.exchange,
                context.exchange(),
            )));
        }

        if context.market_category() != self.plan.market {
            return Err(RestDepthDecodeError::Context(format!(
                "REST depth market mismatch: expected {:?}, got {:?}",
                self.plan.market,
                context.market_category(),
            )));
        }
        if context.channel() != SourceChannel::Depth {
            return Err(RestDepthDecodeError::Context(
                "REST depth decoder requires a depth stream".into(),
            ));
        }

        if receive_timestamp_ns <= 0 {
            return Err(RestDepthDecodeError::Context(
                "trusted receive timestamp must be positive".into(),
            ));
        }

        if requested_depth == 0 {
            return Err(RestDepthDecodeError::Context(
                "requested depth must be positive".into(),
            ));
        }

        // The maximum is a configured memory/record budget.
        // requested_depth is the source request's book-depth
        // limit and must not exceed this bound.
        let maximum = u64::try_from(self.plan.max_levels)
            .map_err(|error| RestDepthDecodeError::Context(error.to_string()))?;

        if requested_depth > maximum {
            return Err(RestDepthDecodeError::Context(
                "requested depth exceeds decoder budget".into(),
            ));
        }

        // Validate application status, exactly-one snapshot,
        // native symbol identity and available metadata.
        let metadata = self
            .reader
            .read(response, context.symbol())
            .map_err(|error| RestDepthDecodeError::Snapshot(error.to_string()))?;

        // This guard MUST execute before the ordinary level
        // extractor. The current NumericConverter rejects
        // non-integer JSON numbers; do not accidentally
        // weaken that contract to make Bitget pass.
        let native = if lossless_json {
            self.plan.levels.extract_lossless_json(response)
        } else {
            reject_fractional_json_numbers(response, &self.plan)?;

            self.plan.levels.extract(response)
        }
        .map_err(|error| RestDepthDecodeError::Levels(error.to_string()))?;

        let total = native
            .bids
            .len()
            .checked_add(native.asks.len())
            .ok_or_else(|| RestDepthDecodeError::Levels("combined level count overflow".into()))?;

        // Each side can contain up to requested_depth levels.
        // Do not compare the combined number against the
        // per-side snapshot depth.
        let side_limit = usize::try_from(requested_depth)
            .map_err(|error| RestDepthDecodeError::Context(error.to_string()))?;

        if native.bids.len() > side_limit || native.asks.len() > side_limit {
            return Err(RestDepthDecodeError::Levels(
                "REST response exceeds requested per-side depth".into(),
            ));
        }

        let mut levels = Vec::with_capacity(total);

        // Only use the exchange timestamp when it is
        // actually present. Otherwise, the transport receive
        // timestamp supplies the required canonical envelope
        // timestamp, while message_timestamp_ns stays None.
        let event_timestamp_ns = metadata.timestamp_ns.unwrap_or(receive_timestamp_ns);

        let exchange = canonical_exchange(context.exchange());

        for level in native.bids {
            levels.push(self.normalize_level(
                level,
                BookSide::Bid,
                context,
                exchange,
                event_timestamp_ns,
            )?);
        }

        for level in native.asks {
            levels.push(self.normalize_level(
                level,
                BookSide::Ask,
                context,
                exchange,
                event_timestamp_ns,
            )?);
        }

        let provenance = if metadata.timestamp_ns.is_some() {
            DepthTimestampProvenance::Exchange
        } else {
            DepthTimestampProvenance::ReceiveTimeFallback
        };

        NormalizedDepth::new(
            context.stream_id().to_owned(),
            NormalizationSource::Rest,
            DepthMessageKind::Snapshot,
            metadata.sequence,
            levels,
            metadata.timestamp_ns,
            Some(requested_depth),
        )
        .and_then(|message| message.with_timestamp_provenance(provenance))
        .map_err(RestDepthDecodeError::Canonical)
    }

    fn normalize_level(
        &self,
        level: NativeDepthLevel,
        side: BookSide,
        context: &NormalizationContext,
        exchange: Exchange,
        event_timestamp_ns: i64,
    ) -> Result<L2LevelUpdate, RestDepthDecodeError> {
        let quantities = QuantityNormalizer::normalize_level(
            level.quantity,
            level.price,
            context,
            self.precision,
        )
        .map_err(|error| RestDepthDecodeError::Quantity(error.to_string()))?;

        Ok(L2LevelUpdate {
            envelope: EventEnvelope {
                event_timestamp_ns,
                system_timestamp_ns: None,
                exchange,
                instrument_id: context.instrument_id(),
                symbol: context.symbol().to_owned(),
                stream_id: context.stream_id().to_owned(),
            },

            side,
            price: level.price,
            quantity_base: quantities.quantity_base,
            quantity_quote: quantities.quantity_quote,
            quantity_contracts: quantities.quantity_contracts,
            order_count: level.order_count,
        })
    }
}

// ============================================================
// NUMERIC PRECISION GUARD
// ============================================================

fn reject_fractional_json_numbers(
    response: &Value,
    plan: &RestDepthNormalizationPlan,
) -> Result<(), RestDepthDecodeError> {
    for (side, path) in [
        ("bids", plan.levels.bids().path.as_str()),
        ("asks", plan.levels.asks().path.as_str()),
    ] {
        let records = JsonExtractor::array(response, path)
            .map_err(|error| RestDepthDecodeError::Levels(error.to_string()))?;

        for (index, record) in records.iter().enumerate() {
            let values: Vec<&Value> = match record {
                Value::Array(values) => values.iter().collect(),

                Value::Object(values) => values.values().collect(),

                _ => {
                    return Err(RestDepthDecodeError::Levels(format!(
                        "{side}[{index}]: invalid level shape"
                    )));
                }
            };

            for value in values {
                if let Value::Number(number) = value {
                    if number.is_f64() {
                        return Err(RestDepthDecodeError::Levels(format!(
                            "{side}[{index}]: floating JSON \
                                 number requires verified \
                                 lexical-preserving parsing"
                        )));
                    }
                }
            }
        }
    }

    Ok(())
}

// ============================================================
// CANONICAL EXCHANGE
// ============================================================

fn canonical_exchange(exchange: ExchangeId) -> Exchange {
    match exchange {
        ExchangeId::Binance => Exchange::Binance,
        ExchangeId::Bybit => Exchange::Bybit,
        ExchangeId::Okx => Exchange::Okx,
        ExchangeId::Bitget => Exchange::Bitget,
        ExchangeId::Gateio => Exchange::GateIo,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    use crate::live::{
        config::{
            exchange::{ExchangeId, MarketType},
            format::DepthFormatConfig,
        },
        stream::request::{CreateSourceStreamRequest, QuantityType, SourceChannel},
    };

    use serde_json::{Value, json};

    fn data_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live")
    }

    fn depth_format(profile: &str) -> DepthFormatConfig {
        DepthFormatConfig::load(
            data_root()
                .join("formats/depth")
                .join(format!("{profile}.toml")),
        )
        .unwrap()
    }

    fn endpoint(profile: &str) -> Value {
        let contents = std::fs::read_to_string(
            data_root()
                .join("exchanges")
                .join(format!("{profile}.toml")),
        )
        .unwrap();

        let config: toml::Value = toml::from_str(&contents).unwrap();

        serde_json::to_value(&config["rest"]["endpoints"]["depth_snapshot"]).unwrap()
    }

    fn decoder(profile: &str) -> RestDepthDecoder {
        RestDepthDecoder::compile(
            &depth_format(profile),
            &endpoint(profile),
            20_000,
            QuantityPrecisionPolicy::Exact,
        )
        .unwrap()
    }

    fn context(exchange: ExchangeId, market: MarketType) -> NormalizationContext {
        let request = CreateSourceStreamRequest {
            instrument_id: 42,

            exchange,
            symbol: "BTCUSDT".into(),
            market_category: market,

            instrument_type: "spot".into(),

            base_asset: Some("BTC".into()),
            quote_asset: Some("USDT".into()),

            quantity_type: QuantityType::Base,

            contract_value: None,
            contract_value_asset: None,

            channel: SourceChannel::Depth,
            settlement_asset: Some("USDT".into()),
        };

        NormalizationContext::new(&request, "rest-depth-source-binding-test").unwrap()
    }

    #[test]
    fn rejects_foreign_exchange_before_response_decoding() {
        let decoder = decoder("binance_spot");

        let context = context(ExchangeId::Bybit, MarketType::Spot);

        // The response is deliberately malformed.
        // Source binding must reject it first.
        let response = json!({});

        let error = decoder
            .decode(&response, &context, 1_000_000_000, 100)
            .unwrap_err();

        assert!(
            matches!(error, RestDepthDecodeError::Context(_)),
            "{error:?}",
        );
    }

    #[test]
    fn rejects_foreign_market_before_response_decoding() {
        let decoder = decoder("binance_spot");

        let context = context(ExchangeId::Binance, MarketType::Linear);

        let error = decoder
            .decode(&json!({}), &context, 1_000_000_000, 100)
            .unwrap_err();

        assert!(
            matches!(error, RestDepthDecodeError::Context(_)),
            "{error:?}",
        );
    }

    #[test]
    fn atomically_compiles_all_fifteen_rest_profiles() {
        let mut compiled = 0;

        for exchange in ["binance", "bybit", "bitget", "gateio", "okx"] {
            for market in ["spot", "linear", "inverse"] {
                let profile = format!("{exchange}_{market}");

                let result = RestDepthDecoder::compile(
                    &depth_format(&profile),
                    &endpoint(&profile),
                    20_000,
                    QuantityPrecisionPolicy::Exact,
                );

                assert!(result.is_ok(), "{profile}: {result:?}",);

                compiled += 1;
            }
        }

        assert_eq!(compiled, 15);
    }

    #[test]
    fn rejects_invalid_rest_endpoint_during_compilation() {
        let mut endpoint = endpoint("binance_spot");

        endpoint["method"] = json!("POST");

        let result = RestDepthDecoder::compile(
            &depth_format("binance_spot"),
            &endpoint,
            1000,
            QuantityPrecisionPolicy::Exact,
        );

        assert!(result.is_err());
    }
    #[test]
    fn raw_bytes_preserve_exact_binance_snapshot() {
        let decoder = decoder("binance_spot");

        let context = context(ExchangeId::Binance, MarketType::Spot);

        let response = br#"{
        "lastUpdateId":123,
        "bids":[["80895.83","0.101665"]],
        "asks":[["80895.84","0.70207"]]
    }"#;

        let normalized = decoder
            .decode_bytes(response, &context, 1_792_806_296_489_000_000, 100)
            .unwrap();

        assert_eq!(normalized.source(), NormalizationSource::Rest,);

        assert_eq!(normalized.kind(), DepthMessageKind::Snapshot,);

        assert_eq!(normalized.sequence().sequence_last, Some(123),);

        assert_eq!(normalized.levels()[0].price.to_string(), "80895.83",);

        assert_eq!(
            normalized.levels()[0].quantity_base.unwrap().to_string(),
            "0.101665",
        );
    }

    #[test]
    fn raw_bytes_reject_unrepresentable_decimal_precision() {
        let decoder = decoder("binance_spot");

        let context = context(ExchangeId::Binance, MarketType::Spot);

        let response = br#"{
        "lastUpdateId":123,
        "bids":[
            ["80895.83",0.12345678901234567890123456789]
        ],
        "asks":[]
    }"#;

        assert!(
            decoder
                .decode_bytes(response, &context, 1_792_806_296_489_000_000, 100,)
                .is_err()
        );
    }

    #[test]
    fn raw_bytes_reject_negative_depth_quantity() {
        let decoder = decoder("binance_spot");

        let context = context(ExchangeId::Binance, MarketType::Spot);

        let response = br#"{
        "lastUpdateId":123,
        "bids":[["80895.83",-0.101665]],
        "asks":[]
    }"#;

        assert!(
            decoder
                .decode_bytes(response, &context, 1_792_806_296_489_000_000, 100,)
                .is_err()
        );
    }
}
