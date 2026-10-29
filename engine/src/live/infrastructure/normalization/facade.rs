use std::sync::Arc;

use serde_json::Value;
use thiserror::Error;

use crate::live::config::exchange::RestEndpoint;
use crate::live::config::{ExchangeConfigStore, FormatConfigStore};
use crate::live::infrastructure::rest::{ExchangeRestResponse, RestRequest};
use crate::live::stream::request::{CreateSourceStreamRequest, SourceChannel};

use super::{
    DepthDecoder, NormalizationContext, NormalizationSource, NormalizedMessage,
    QuantityPrecisionPolicy, RestDepthDecoder, RestTradeDecoder, RestTradeNormalizationCompiler,
    TradeDecoder, TradeNormalizationCompiler,
};

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum NormalizerError {
    #[error("invalid normalization configuration: {0}")]
    Configuration(String),

    #[error("unsupported normalization operation: {0}")]
    Unsupported(String),

    #[error("normalization failed: {0}")]
    Decode(String),
}

// ============================================================
// RESOURCE LIMITS
// ============================================================

#[derive(Debug, Clone, Copy)]
pub struct NormalizerLimits {
    pub max_trade_records: usize,
    pub max_depth_levels: usize,
    pub precision: QuantityPrecisionPolicy,
}

impl Default for NormalizerLimits {
    fn default() -> Self {
        Self {
            max_trade_records: 1024,
            max_depth_levels: 4096,
            precision: QuantityPrecisionPolicy::Exact,
        }
    }
}

impl NormalizerLimits {
    fn validate(&self) -> Result<(), NormalizerError> {
        if self.max_trade_records == 0 {
            return Err(NormalizerError::Configuration(
                "max_trade_records must be positive".into(),
            ));
        }

        if self.max_depth_levels == 0 {
            return Err(NormalizerError::Configuration(
                "max_depth_levels must be positive".into(),
            ));
        }

        self.precision
            .validate()
            .map_err(|error| NormalizerError::Configuration(error.to_string()))
    }
}

// ============================================================
// COMPILED DECODERS
// ============================================================

enum WebSocketDecoder {
    Trades(TradeDecoder),
    Depth(DepthDecoder),
}

enum RestDecoder {
    Trades {
        decoder: TradeDecoder,
        endpoint_name: String,
        endpoint: RestEndpoint,

        // Optional additional endpoint with compatible
        // canonical trade-record semantics.
        historical_endpoint: Option<RestEndpoint>,
    },

    Depth {
        decoder: RestDepthDecoder,
    },
}

// ============================================================
// NORMALIZER
// ============================================================

pub struct Normalizer {
    context: NormalizationContext,
    websocket: WebSocketDecoder,
    rest: Option<RestDecoder>,
}

impl Normalizer {
    // ========================================================
    // ACCESSORS
    // ========================================================

    pub fn stream_id(&self) -> &str {
        self.context.stream_id()
    }

    pub fn context(&self) -> &NormalizationContext {
        &self.context
    }

    pub fn supports_rest(&self) -> bool {
        self.rest.is_some()
    }

    // ========================================================
    // WEBSOCKET NORMALIZATION
    // ========================================================

    pub fn normalize_ws(&self, message: &Value) -> Result<NormalizedMessage, NormalizerError> {
        match &self.websocket {
            WebSocketDecoder::Trades(decoder) => {
                let normalized = decoder
                    .decode_message(message, &self.context, NormalizationSource::WebSocket)
                    .map_err(|error| NormalizerError::Decode(error.to_string()))?;

                Ok(NormalizedMessage::Trades(normalized))
            }

            WebSocketDecoder::Depth(decoder) => {
                let normalized = decoder
                    .decode_websocket(message, &self.context)
                    .map_err(|error| NormalizerError::Decode(error.to_string()))?;

                Ok(NormalizedMessage::Depth(normalized))
            }
        }
    }

    // ========================================================
    // REST TRADE NORMALIZATION
    // ========================================================

    /// The request must be the same logical request that
    /// produced the supplied ExchangeRestResponse.
    ///
    /// This preserves the existing REST identity and
    /// instrument-binding validation.
    pub fn normalize_rest_trades(
        &self,
        response: &ExchangeRestResponse,
        request: &RestRequest,
    ) -> Result<NormalizedMessage, NormalizerError> {
        let Some(RestDecoder::Trades {
            decoder,
            endpoint_name,
            endpoint,
            historical_endpoint,
        }) = &self.rest
        else {
            return Err(NormalizerError::Unsupported(
                "REST trade normalization is unavailable for this stream".into(),
            ));
        };

        let selected_endpoint = if request.endpoint() == endpoint_name {
            endpoint
        } else if request.endpoint() == "historical_trades" {
            historical_endpoint.as_ref().ok_or_else(|| {
                NormalizerError::Unsupported(
                    "historical REST trade normalization is unavailable".into(),
                )
            })?
        } else {
            return Err(NormalizerError::Configuration(format!(
                "unsupported REST trade endpoint '{}'",
                request.endpoint(),
            )));
        };

        let normalized = RestTradeDecoder::decode_bound_exchange_response(
            decoder,
            response,
            request,
            selected_endpoint,
            &self.context,
        )
        .map_err(|error| NormalizerError::Decode(error.to_string()))?;

        Ok(NormalizedMessage::Trades(normalized))
    }

    // ========================================================
    // REST DEPTH NORMALIZATION
    // ========================================================

    /// The caller must supply the original response bytes,
    /// trusted receive timestamp, and authorized request depth.
    ///
    /// Transport and request-provenance validation must occur
    /// before invoking this method.
    pub fn normalize_rest_depth(
        &self,
        response_bytes: &[u8],
        receive_timestamp_ns: i64,
        requested_depth: u64,
    ) -> Result<NormalizedMessage, NormalizerError> {
        let Some(RestDecoder::Depth { decoder }) = &self.rest else {
            return Err(NormalizerError::Unsupported(
                "REST depth normalization is unavailable \
                 for this source stream"
                    .into(),
            ));
        };

        let normalized = decoder
            .decode_bytes(
                response_bytes,
                &self.context,
                receive_timestamp_ns,
                requested_depth,
            )
            .map_err(|error| NormalizerError::Decode(error.to_string()))?;

        Ok(NormalizedMessage::Depth(normalized))
    }
}

// ============================================================
// NORMALIZER FACTORY
// ============================================================

pub struct NormalizerFactory {
    exchanges: Arc<ExchangeConfigStore>,
    formats: Arc<FormatConfigStore>,
    limits: NormalizerLimits,
}

impl NormalizerFactory {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    pub fn new(
        exchanges: Arc<ExchangeConfigStore>,
        formats: Arc<FormatConfigStore>,
        limits: NormalizerLimits,
    ) -> Result<Self, NormalizerError> {
        limits.validate()?;

        Ok(Self {
            exchanges,
            formats,
            limits,
        })
    }

    // ========================================================
    // CREATE STREAM NORMALIZER
    // ========================================================

    pub fn create(
        &self,
        stream_id: impl Into<String>,
        request: &CreateSourceStreamRequest,
    ) -> Result<Normalizer, NormalizerError> {
        let context = NormalizationContext::new(request, stream_id)
            .map_err(NormalizerError::Configuration)?;

        let profile = format!(
            "{}_{}",
            request.exchange.as_str(),
            request.market_category.as_str(),
        );

        let exchange = self.exchanges.get(&profile).ok_or_else(|| {
            NormalizerError::Configuration(
                format!("exchange profile '{}' is unavailable", profile,),
            )
        })?;

        let channel_name = request.channel.as_str();

        let channel = exchange.channel(channel_name).ok_or_else(|| {
            NormalizerError::Configuration(format!(
                "channel '{}' is not configured for '{}'",
                channel_name, profile,
            ))
        })?;

        if !channel.enabled {
            return Err(NormalizerError::Configuration(format!(
                "channel '{}' is disabled for '{}'",
                channel_name, profile,
            )));
        }

        match request.channel {
            SourceChannel::Trades => {
                self.create_trade_normalizer(context, &profile, channel, exchange)
            }

            SourceChannel::Depth => {
                self.create_depth_normalizer(context, &profile, channel, exchange)
            }
        }
    }

    // ========================================================
    // TRADE NORMALIZER
    // ========================================================

    fn create_trade_normalizer(
        &self,
        context: NormalizationContext,
        profile: &str,
        channel: &crate::live::config::ChannelConfig,
        exchange: &crate::live::config::ExchangeConfig,
    ) -> Result<Normalizer, NormalizerError> {
        let format = self.formats.trade(profile).ok_or_else(|| {
            NormalizerError::Configuration(format!(
                "trade normalization format '{}' is missing",
                profile,
            ))
        })?;

        let expected_format = format!("trades/{profile}");

        if channel.format.as_deref() != Some(expected_format.as_str()) {
            return Err(NormalizerError::Configuration(format!(
                "channel format does not match '{}'",
                expected_format,
            )));
        }

        // ========================================================
        // WEBSOCKET TRADE DECODER
        // ========================================================

        let websocket_plan = TradeNormalizationCompiler::compile_websocket(
            format,
            self.limits.max_trade_records,
            self.limits.precision,
        )
        .map_err(|error| NormalizerError::Configuration(error.to_string()))?;

        let websocket_decoder = TradeDecoder::new(websocket_plan.clone())
            .map_err(|error| NormalizerError::Configuration(error.to_string()))?;

        // ========================================================
        // REST TRADE DECODER
        // ========================================================

        // Prefer the configured recovery endpoint when it has a
        // trade normalization definition.
        //
        // Otherwise, use the configured recent_trades endpoint
        // if it supports normalization.
        //
        // This handles profiles such as Binance Spot and OKX,
        // whose historical recovery endpoint differs from their
        // REST normalization-capable endpoint.

        let rest_endpoint = channel
            .recovery_endpoint
            .as_deref()
            .and_then(|name| {
                exchange
                    .endpoint(name)
                    .filter(|endpoint| endpoint.trade_normalization.is_some())
                    .map(|endpoint| (name, endpoint))
            })
            .or_else(|| {
                exchange
                    .endpoint("recent_trades")
                    .filter(|endpoint| endpoint.trade_normalization.is_some())
                    .map(|endpoint| ("recent_trades", endpoint))
            });

        let rest = if let Some((endpoint_name, endpoint)) = rest_endpoint {
            let rest_plan = RestTradeNormalizationCompiler::compile_from_config(
                &websocket_plan,
                endpoint,
                self.limits.max_trade_records,
            )
            .map_err(|error| NormalizerError::Configuration(error.to_string()))?;

            let decoder = TradeDecoder::new(rest_plan)
                .map_err(|error| NormalizerError::Configuration(error.to_string()))?;

            let historical_endpoint = if profile == "binance_spot" {
                exchange.endpoint("historical_trades").map(|historical| {
                    let mut configured = historical.clone();

                    // Binance Spot historicalTrades uses the same
                    // individual trade-record fields as recent trades.
                    //
                    // Reuse only the record normalization mapping.
                    // Preserve historical endpoint-specific response
                    // validation and request configuration.
                    if configured.trade_normalization.is_none() {
                        configured.trade_normalization = endpoint.trade_normalization.clone();
                    }

                    configured
                })
            } else {
                None
            };

            Some(RestDecoder::Trades {
                decoder,
                endpoint_name: endpoint_name.to_owned(),
                endpoint: endpoint.clone(),
                historical_endpoint,
            })
        } else {
            None
        };

        // ========================================================
        // CONSTRUCT NORMALIZER
        // ========================================================

        Ok(Normalizer {
            context,
            websocket: WebSocketDecoder::Trades(websocket_decoder),
            rest,
        })
    }

    // ========================================================
    // DEPTH NORMALIZER
    // ========================================================

    fn create_depth_normalizer(
        &self,
        context: NormalizationContext,
        profile: &str,
        channel: &crate::live::config::ChannelConfig,
        exchange: &crate::live::config::ExchangeConfig,
    ) -> Result<Normalizer, NormalizerError> {
        let format = self.formats.depth(profile).ok_or_else(|| {
            NormalizerError::Configuration(format!(
                "depth normalization format '{}' is missing",
                profile,
            ))
        })?;

        let expected_format = format!("depth/{profile}");

        if channel.format.as_deref() != Some(expected_format.as_str()) {
            return Err(NormalizerError::Configuration(format!(
                "channel format does not match '{}'",
                expected_format,
            )));
        }

        // ========================================================
        // WEBSOCKET DEPTH DECODER
        // ========================================================

        let subscription_depth = channel.default_depth;

        let websocket_decoder = DepthDecoder::new_websocket(
            format,
            self.limits.max_depth_levels,
            self.limits.precision,
            subscription_depth,
        )
        .map_err(|error| NormalizerError::Configuration(error.to_string()))?;

        // ========================================================
        // REST DEPTH ENDPOINT DISCOVERY
        // ========================================================

        // First, use the endpoint declared by the depth format.
        //
        // Second, use the channel's REST snapshot endpoint.
        //
        // Finally, use the exchange's named depth_snapshot
        // endpoint when no explicit reference exists.

        let endpoint_name = format
            .synchronization
            .snapshot_endpoint
            .as_deref()
            .or_else(|| {
                channel.synchronization.as_ref().and_then(|sync| {
                    sync.rest_snapshot_endpoint
                        .as_deref()
                        .or(sync.snapshot_endpoint.as_deref())
                })
            })
            .or_else(|| {
                exchange
                    .endpoint("depth_snapshot")
                    .map(|_| "depth_snapshot")
            });

        // ========================================================
        // REST DEPTH DECODER
        // ========================================================

        let rest = if let Some(endpoint_name) = endpoint_name {
            let endpoint = exchange.endpoint(endpoint_name).ok_or_else(|| {
                NormalizerError::Configuration(format!(
                    "REST depth endpoint '{}' is missing",
                    endpoint_name,
                ))
            })?;

            let decoder = RestDepthDecoder::compile(
                format,
                endpoint,
                self.limits.max_depth_levels,
                self.limits.precision,
            )
            .map_err(|error| NormalizerError::Configuration(error.to_string()))?;

            Some(RestDecoder::Depth { decoder })
        } else {
            None
        };

        // ========================================================
        // CONSTRUCT NORMALIZER
        // ========================================================

        Ok(Normalizer {
            context,
            websocket: WebSocketDecoder::Depth(websocket_decoder),
            rest,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Arc};

    use rust_decimal::Decimal;
    use serde_json::{Value, json};

    use crate::live::{
        config::{ExchangeConfigStore, FormatConfigStore},
        infrastructure::normalization::{
            NormalizationSource, NormalizedMessage, QuantityPrecisionPolicy,
        },
        stream::request::{CreateSourceStreamRequest, SourceChannel},
    };

    use super::{Normalizer, NormalizerError, NormalizerFactory, NormalizerLimits};

    // ========================================================
    // FIXTURES
    // ========================================================

    fn config_root() -> &'static Path {
        Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../data/.config/live"))
    }

    fn factory() -> NormalizerFactory {
        let root = config_root();

        let exchanges = ExchangeConfigStore::load_dir(root.join("exchanges"))
            .expect("exchange configurations must load");

        let formats = FormatConfigStore::load_dir(root.join("formats"))
            .expect("normalization formats must load");

        NormalizerFactory::new(
            Arc::new(exchanges),
            Arc::new(formats),
            NormalizerLimits::default(),
        )
        .expect("normalizer factory must initialize")
    }

    fn spot_request(channel: SourceChannel) -> CreateSourceStreamRequest {
        CreateSourceStreamRequest {
            instrument_id: 15,
            exchange: crate::live::config::exchange::ExchangeId::Binance,
            symbol: "BTCUSDT".into(),
            market_category: crate::live::config::exchange::MarketType::Spot,
            instrument_type: "spot".into(),
            base_asset: Some("BTC".into()),
            quote_asset: Some("USDT".into()),
            quantity_type: crate::live::stream::request::QuantityType::Base,
            contract_value: None,
            contract_value_asset: None,
            channel,
            settlement_asset: None,
        }
    }

    fn trade_normalizer() -> Normalizer {
        factory()
            .create(
                "binance-btcusdt-trades",
                &spot_request(SourceChannel::Trades),
            )
            .expect("trade normalizer must initialize")
    }

    fn depth_normalizer() -> Normalizer {
        factory()
            .create("binance-btcusdt-depth", &spot_request(SourceChannel::Depth))
            .expect("depth normalizer must initialize")
    }

    fn binance_trade() -> Value {
        json!({
            "e": "trade",
            "E": 1720000000123_i64,
            "s": "BTCUSDT",
            "t": 123456789_u64,
            "p": "67500.50",
            "q": "0.125",
            "b": 100_u64,
            "a": 101_u64,
            "T": 1720000000120_i64,
            "m": false,
            "M": true
        })
    }

    // ========================================================
    // FACTORY LIMITS
    // ========================================================

    #[test]
    fn reject_zero_trade_record_limit() {
        let mut limits = NormalizerLimits::default();
        limits.max_trade_records = 0;

        let result = limits.validate();

        assert!(matches!(result, Err(NormalizerError::Configuration(_))));
    }

    #[test]
    fn reject_zero_depth_level_limit() {
        let mut limits = NormalizerLimits::default();
        limits.max_depth_levels = 0;

        let result = limits.validate();

        assert!(matches!(result, Err(NormalizerError::Configuration(_))));
    }

    #[test]
    fn reject_invalid_rounding_precision() {
        let mut limits = NormalizerLimits::default();

        limits.precision = QuantityPrecisionPolicy::Round { decimal_places: 29 };

        let result = limits.validate();

        assert!(matches!(result, Err(NormalizerError::Configuration(_))));
    }

    #[test]
    fn accept_exact_precision_policy() {
        let limits = NormalizerLimits::default();

        assert_eq!(limits.precision, QuantityPrecisionPolicy::Exact,);

        limits.validate().unwrap();
    }

    #[test]
    fn accept_valid_rounding_precision() {
        let mut limits = NormalizerLimits::default();

        limits.precision = QuantityPrecisionPolicy::Round { decimal_places: 28 };

        limits.validate().unwrap();
    }

    // ========================================================
    // STREAM CONSTRUCTION
    // ========================================================

    #[test]
    fn create_binance_spot_trade_normalizer() {
        let normalizer = trade_normalizer();

        assert_eq!(normalizer.stream_id(), "binance-btcusdt-trades",);

        assert_eq!(normalizer.context().instrument_id(), 15,);

        assert_eq!(normalizer.context().symbol(), "BTCUSDT",);

        assert_eq!(normalizer.context().channel(), SourceChannel::Trades,);
    }

    #[test]
    fn create_binance_spot_depth_normalizer() {
        let normalizer = depth_normalizer();

        assert_eq!(normalizer.stream_id(), "binance-btcusdt-depth",);

        assert_eq!(normalizer.context().channel(), SourceChannel::Depth,);
    }

    #[test]
    fn reject_empty_stream_id() {
        let result = factory().create("", &spot_request(SourceChannel::Trades));

        assert!(matches!(result, Err(NormalizerError::Configuration(_))));
    }

    #[test]
    fn reject_whitespace_stream_id() {
        let result = factory().create("   ", &spot_request(SourceChannel::Trades));

        assert!(matches!(result, Err(NormalizerError::Configuration(_))));
    }

    #[test]
    fn reject_invalid_instrument_id() {
        let mut request = spot_request(SourceChannel::Trades);

        request.instrument_id = 0;

        let result = factory().create("invalid-instrument", &request);

        assert!(matches!(result, Err(NormalizerError::Configuration(_))));
    }

    #[test]
    fn reject_unknown_exchange_profile() {
        let mut request = spot_request(SourceChannel::Trades);

        request.exchange = crate::live::config::exchange::ExchangeId::Binance;

        let empty_exchanges = Arc::new(ExchangeConfigStore::new());

        let formats = Arc::new(FormatConfigStore::load_dir(config_root().join("formats")).unwrap());

        let factory =
            NormalizerFactory::new(empty_exchanges, formats, NormalizerLimits::default()).unwrap();

        let result = factory.create("missing-profile", &request);

        assert!(matches!(result, Err(NormalizerError::Configuration(_))));
    }

    #[test]
    fn reject_missing_normalization_format() {
        let exchanges =
            Arc::new(ExchangeConfigStore::load_dir(config_root().join("exchanges")).unwrap());

        let empty_formats = Arc::new(FormatConfigStore::new());

        let factory =
            NormalizerFactory::new(exchanges, empty_formats, NormalizerLimits::default()).unwrap();

        let result = factory.create("missing-format", &spot_request(SourceChannel::Trades));

        assert!(matches!(result, Err(NormalizerError::Configuration(_))));
    }

    // ========================================================
    // WEBSOCKET TRADE NORMALIZATION
    // ========================================================

    #[test]
    fn normalize_valid_binance_spot_trade() {
        let normalizer = trade_normalizer();

        let result = normalizer
            .normalize_ws(&binance_trade())
            .expect("valid trade must normalize");

        assert_eq!(result.stream_id(), "binance-btcusdt-trades",);

        assert_eq!(result.source(), NormalizationSource::WebSocket,);

        assert_eq!(result.record_count(), 1);

        let NormalizedMessage::Trades(trades) = result else {
            panic!("expected normalized trades");
        };

        let trade = &trades.trades()[0];

        assert_eq!(trade.envelope.instrument_id, 15,);

        assert_eq!(trade.envelope.symbol, "BTCUSDT",);

        assert_eq!(trade.envelope.stream_id, "binance-btcusdt-trades",);

        assert_eq!(trade.price, Decimal::new(6750050, 2),);

        assert_eq!(trade.quantity_base, Some(Decimal::new(125, 3)),);

        assert_eq!(trade.quantity_quote, Some(Decimal::new(84375625, 4)),);

        assert_eq!(trade.envelope.event_timestamp_ns, 1720000000120000000_i64,);
    }

    #[test]
    fn reject_trade_with_wrong_symbol() {
        let normalizer = trade_normalizer();

        let mut message = binance_trade();

        message["s"] = json!("ETHUSDT");

        let result = normalizer.normalize_ws(&message);

        assert!(matches!(result, Err(NormalizerError::Decode(_))));
    }

    #[test]
    fn reject_trade_with_invalid_price() {
        let normalizer = trade_normalizer();

        let mut message = binance_trade();

        message["p"] = json!("-100");

        let result = normalizer.normalize_ws(&message);

        assert!(matches!(result, Err(NormalizerError::Decode(_))));
    }

    #[test]
    fn reject_trade_with_invalid_quantity() {
        let normalizer = trade_normalizer();

        let mut message = binance_trade();

        message["q"] = json!("-0.125");

        let result = normalizer.normalize_ws(&message);

        assert!(matches!(result, Err(NormalizerError::Decode(_))));
    }

    #[test]
    fn reject_trade_with_missing_required_field() {
        let normalizer = trade_normalizer();

        let mut message = binance_trade();

        message.as_object_mut().unwrap().remove("p");

        let result = normalizer.normalize_ws(&message);

        assert!(matches!(result, Err(NormalizerError::Decode(_))));
    }

    #[test]
    fn reject_unrelated_websocket_message() {
        let normalizer = trade_normalizer();

        let message = json!({
            "event": "subscription",
            "success": true
        });

        let result = normalizer.normalize_ws(&message);

        assert!(matches!(result, Err(NormalizerError::Decode(_))));
    }

    // ========================================================
    // WEBSOCKET DEPTH NORMALIZATION
    // ========================================================

    #[test]
    fn reject_invalid_depth_message() {
        let normalizer = depth_normalizer();

        let message = json!({
            "invalid": true
        });

        let result = normalizer.normalize_ws(&message);

        assert!(matches!(result, Err(NormalizerError::Decode(_))));
    }

    #[test]
    fn reject_trade_message_on_depth_stream() {
        let normalizer = depth_normalizer();

        let result = normalizer.normalize_ws(&binance_trade());

        assert!(matches!(result, Err(NormalizerError::Decode(_))));
    }

    // ========================================================
    // REST CAPABILITY
    // ========================================================

    #[test]
    fn reject_rest_trade_operation_on_depth_stream() {
        let normalizer = depth_normalizer();

        // No REST request should be executed here.
        // Verify the facade's decoder-family guard directly.
        assert!(!matches!(
            normalizer.rest,
            Some(super::RestDecoder::Trades { .. })
        ));
    }

    #[test]
    fn reject_rest_depth_operation_on_trade_stream() {
        let normalizer = trade_normalizer();

        assert!(!matches!(
            normalizer.rest,
            Some(super::RestDecoder::Depth { .. })
        ));
    }

    #[test]
    fn rest_capability_matches_decoder_presence() {
        let trade = trade_normalizer();
        let depth = depth_normalizer();

        assert_eq!(trade.supports_rest(), trade.rest.is_some(),);

        assert_eq!(depth.supports_rest(), depth.rest.is_some(),);
    }

    // ========================================================
    // STREAM ISOLATION
    // ========================================================

    #[test]
    fn normalizers_preserve_independent_stream_identity() {
        let factory = factory();

        let request = spot_request(SourceChannel::Trades);

        let first = factory.create("stream-a", &request).unwrap();

        let second = factory.create("stream-b", &request).unwrap();

        let first_output = first.normalize_ws(&binance_trade()).unwrap();

        let second_output = second.normalize_ws(&binance_trade()).unwrap();

        assert_eq!(first_output.stream_id(), "stream-a",);

        assert_eq!(second_output.stream_id(), "stream-b",);

        assert_eq!(first_output.record_count(), second_output.record_count(),);
    }

    #[test]
    fn failed_message_does_not_disable_normalizer() {
        let normalizer = trade_normalizer();

        let invalid = json!({
            "invalid": true
        });

        assert!(normalizer.normalize_ws(&invalid).is_err());

        let valid = normalizer
            .normalize_ws(&binance_trade())
            .expect("normalizer must remain usable after failure");

        assert_eq!(valid.record_count(), 1);
    }

    #[test]
    fn normalizer_can_process_repeated_messages() {
        let normalizer = trade_normalizer();

        for _ in 0..100 {
            let result = normalizer
                .normalize_ws(&binance_trade())
                .expect("repeated normalization must succeed");

            assert_eq!(result.record_count(), 1);
        }
    }
}
