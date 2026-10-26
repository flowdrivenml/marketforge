use crate::canonical::TradeSide;
use crate::live::config::exchange::RestEndpoint;
use crate::live::infrastructure::normalization::numeric::TimestampUnit;
use crate::live::infrastructure::normalization::side::NativeSideValue;
use crate::live::infrastructure::normalization::side::{
    AggressorSideNormalizer, PositiveQuantitySide,
};
use crate::live::infrastructure::normalization::trades::compiler::CompiledTradeNormalizationPlan;
use crate::live::infrastructure::normalization::trades::fields::{
    NativeTradeFieldLayout, TradeFieldPlan,
};
use crate::live::infrastructure::normalization::trades::plan::{
    TradeExtractionPlan, TradeRecordRepresentation,
};
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RestTradeCompileError {
    #[error("invalid REST trade configuration: {0}")]
    InvalidConfiguration(String),
}

pub struct RestTradeNormalizationCompiler;

impl RestTradeNormalizationCompiler {
    /// Compile the location and representation of REST trade records.
    ///
    /// Field normalization and canonical conversion are separate steps.
    pub fn compile_extraction(
        endpoint: &RestEndpoint,
        max_records: usize,
    ) -> Result<TradeExtractionPlan, RestTradeCompileError> {
        let path = endpoint.response_root.as_deref().unwrap_or("$");

        TradeExtractionPlan::new(path, TradeRecordRepresentation::Array, max_records)
            .map_err(|error| RestTradeCompileError::InvalidConfiguration(error.to_string()))
    }
    /// Compile an individual REST trade plan using an existing
    /// validated trade plan as the source of shared semantics.
    ///
    /// The caller supplies the REST-native field layout and
    /// side interpretation explicitly.
    ///
    /// This does not authorize decoding arbitrary REST responses:
    /// application-response validation remains a separate step.
    pub fn compile_individual(
        websocket_plan: &CompiledTradeNormalizationPlan,
        endpoint: &RestEndpoint,
        layout: NativeTradeFieldLayout,
        timestamp_unit: TimestampUnit,
        side: AggressorSideNormalizer,
        record_symbol_field: Option<String>,
        max_records: usize,
    ) -> Result<CompiledTradeNormalizationPlan, RestTradeCompileError> {
        if !matches!(
            websocket_plan.semantics,
            crate::live::infrastructure::normalization::message::TradeRecordSemantics::Individual
        ) {
            return Err(RestTradeCompileError::InvalidConfiguration(
                "REST individual-trade plan requires individual semantics".into(),
            ));
        }

        let extraction = Self::compile_extraction(endpoint, max_records)?;

        let fields = TradeFieldPlan::new(layout, timestamp_unit)
            .map_err(|error| RestTradeCompileError::InvalidConfiguration(error.to_string()))?;

        let mut plan = websocket_plan.clone();

        plan.extraction = extraction;
        plan.fields = fields;
        plan.side = side;
        plan.record_symbol_field = record_symbol_field;

        // WebSocket-only native symbol-type requirements must
        // not be imposed on REST records that lack that field.
        // REST-specific instrument validation will be supplied
        // by the REST compilation contract.
        plan.symbol_type = None;

        Ok(plan)
    }
    /// Compile REST trade normalization from the endpoint's
    /// validated native field-mapping configuration.
    ///
    /// Shared economic semantics come from the existing
    /// WebSocket trade plan. REST-specific native mappings
    /// come exclusively from the REST endpoint configuration.
    pub fn compile_from_config(
        websocket_plan: &CompiledTradeNormalizationPlan,
        endpoint: &RestEndpoint,
        max_records: usize,
    ) -> Result<CompiledTradeNormalizationPlan, RestTradeCompileError> {
        let config = endpoint.trade_normalization.as_ref().ok_or_else(|| {
            RestTradeCompileError::InvalidConfiguration(
                "REST endpoint has no trade_normalization configuration".into(),
            )
        })?;

        // ========================================================
        // TIMESTAMP UNIT
        // ========================================================

        let timestamp_unit = TimestampUnit::parse(&config.timestamp_unit)
            .map_err(RestTradeCompileError::InvalidConfiguration)?;

        // ========================================================
        // NATIVE FIELD LAYOUT
        // ========================================================

        let layout = NativeTradeFieldLayout {
            trade_id: Some(config.trade_id.clone()),
            price: config.price.clone(),
            quantity: config.quantity.clone(),
            timestamp: config.timestamp.clone(),
            side: config.side.clone(),
            sequence: config.sequence.clone(),
        };

        // ========================================================
        // AGGRESSOR SIDE
        // ========================================================

        let side = match config.side_strategy.as_str() {
            "mapped" => {
                if config.side.is_none() {
                    return Err(RestTradeCompileError::InvalidConfiguration(
                        "mapped REST side requires a side field".into(),
                    ));
                }

                if config.positive_quantity_side.is_some() {
                    return Err(RestTradeCompileError::InvalidConfiguration(
                        "mapped REST side cannot define positive_quantity_side".into(),
                    ));
                }

                let mut mappings = BTreeMap::new();

                for (native, canonical) in &config.side_mappings {
                    let canonical_side = match canonical.as_str() {
                        "buy" => TradeSide::Buy,
                        "sell" => TradeSide::Sell,

                        other => {
                            return Err(RestTradeCompileError::InvalidConfiguration(format!(
                                "invalid canonical side '{other}'"
                            )));
                        }
                    };

                    let native_value = match native.as_str() {
                        "true" => NativeSideValue::Boolean(true),
                        "false" => NativeSideValue::Boolean(false),
                        _ => NativeSideValue::String(native.clone()),
                    };

                    mappings.insert(native_value, canonical_side);
                }

                AggressorSideNormalizer::mapped(mappings).map_err(|error| {
                    RestTradeCompileError::InvalidConfiguration(error.to_string())
                })?
            }

            "signed_quantity" => {
                if config.side.is_some() {
                    return Err(RestTradeCompileError::InvalidConfiguration(
                        "signed_quantity must not define a side field".into(),
                    ));
                }

                if !config.side_mappings.is_empty() {
                    return Err(RestTradeCompileError::InvalidConfiguration(
                        "signed_quantity must not define side_mappings".into(),
                    ));
                }

                let positive = match config.positive_quantity_side.as_deref() {
                    Some("buy") => PositiveQuantitySide::Buy,
                    Some("sell") => PositiveQuantitySide::Sell,

                    _ => {
                        return Err(RestTradeCompileError::InvalidConfiguration(
                            "signed_quantity requires positive_quantity_side = 'buy' or 'sell'"
                                .into(),
                        ));
                    }
                };

                AggressorSideNormalizer::signed_quantity(positive)
            }

            other => {
                return Err(RestTradeCompileError::InvalidConfiguration(format!(
                    "unsupported REST side strategy '{other}'"
                )));
            }
        };

        // ========================================================
        // COMPILE USING EXISTING IMPLEMENTATION
        // ========================================================

        Self::compile_individual(
            websocket_plan,
            endpoint,
            layout,
            timestamp_unit,
            side,
            config.symbol.clone(),
            max_records,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::live::config::ExchangeConfig;

    use std::path::PathBuf;

    fn endpoint(profile: &str, name: &str) -> RestEndpoint {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../data/.config/live/exchanges")
            .join(format!("{profile}.toml"));

        let config = ExchangeConfig::load(path).unwrap();

        config.rest.endpoint(name).unwrap().clone()
    }

    #[test]
    fn compiles_rest_trade_extraction_for_all_profiles() {
        let cases = [
            ("binance_spot", "recent_trades", "$"),
            ("binance_linear", "recent_trades", "$"),
            ("binance_inverse", "recent_trades", "$"),
            ("bybit_spot", "recent_trades", "result.list"),
            ("bybit_linear", "recent_trades", "result.list"),
            ("bybit_inverse", "recent_trades", "result.list"),
            ("bitget_spot", "recent_trades", "data"),
            ("bitget_linear", "recent_trades", "data"),
            ("bitget_inverse", "recent_trades", "data"),
            ("okx_spot", "recent_trades", "data"),
            ("okx_linear", "recent_trades", "data"),
            ("okx_inverse", "recent_trades", "data"),
            ("gateio_spot", "historical_trades", "$"),
            ("gateio_linear", "historical_trades", "$"),
            ("gateio_inverse", "historical_trades", "$"),
        ];

        for (profile, name, expected_path) in cases {
            let config = endpoint(profile, name);

            let plan = RestTradeNormalizationCompiler::compile_extraction(&config, 1000)
                .unwrap_or_else(|error| panic!("{profile}/{name}: {error}"));

            assert_eq!(
                plan.path(),
                expected_path,
                "{profile}: incorrect extraction path",
            );

            assert_eq!(plan.representation(), TradeRecordRepresentation::Array,);

            assert_eq!(plan.max_records(), 1000);
        }
    }

    #[test]
    fn rejects_zero_rest_trade_record_limit() {
        let config = endpoint("binance_spot", "recent_trades");

        assert!(RestTradeNormalizationCompiler::compile_extraction(&config, 0,).is_err());
    }
    #[test]
    fn compiles_binance_spot_rest_trade_plan() {
        use std::collections::BTreeMap;

        use crate::canonical::TradeSide;

        use crate::live::config::format::FormatConfigStore;

        use crate::live::infrastructure::normalization::quantity::QuantityPrecisionPolicy;
        use crate::live::infrastructure::normalization::side::NativeSideValue;
        use crate::live::infrastructure::normalization::trades::compiler::TradeNormalizationCompiler;

        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live/formats");

        let formats = FormatConfigStore::load_dir(root).unwrap();

        let format = formats
            .trade("binance_spot")
            .expect("missing Binance Spot trade format");

        let websocket_plan = TradeNormalizationCompiler::compile_websocket(
            format,
            1000,
            QuantityPrecisionPolicy::Exact,
        )
        .unwrap();

        let mut side_mappings = BTreeMap::new();

        // Binance isBuyerMaker=true means the buyer was maker;
        // therefore the aggressor was the seller.
        side_mappings.insert(NativeSideValue::Boolean(true), TradeSide::Sell);

        side_mappings.insert(NativeSideValue::Boolean(false), TradeSide::Buy);

        let side = AggressorSideNormalizer::mapped(side_mappings).unwrap();

        let layout = NativeTradeFieldLayout {
            trade_id: Some("id".into()),
            price: "price".into(),
            quantity: "qty".into(),
            timestamp: "time".into(),
            side: Some("isBuyerMaker".into()),
            sequence: None,
        };

        let endpoint = endpoint("binance_spot", "recent_trades");

        let rest_plan = RestTradeNormalizationCompiler::compile_individual(
            &websocket_plan,
            &endpoint,
            layout,
            TimestampUnit::Milliseconds,
            side,
            None,
            1000,
        )
        .unwrap();

        assert_eq!(rest_plan.extraction.path(), "$");

        assert_eq!(
            rest_plan.extraction.representation(),
            TradeRecordRepresentation::Array,
        );

        assert_eq!(rest_plan.fields.layout().trade_id.as_deref(), Some("id"),);

        assert_eq!(rest_plan.fields.layout().timestamp, "time",);

        assert_eq!(
            rest_plan.fields.timestamp_unit(),
            TimestampUnit::Milliseconds,
        );

        assert_eq!(
            rest_plan.expected_exchange,
            websocket_plan.expected_exchange,
        );

        assert_eq!(rest_plan.expected_market, websocket_plan.expected_market,);

        assert_eq!(
            rest_plan.declared_quantity_type,
            websocket_plan.declared_quantity_type,
        );

        assert!(rest_plan.symbol_type.is_none());
        // ========================================================
        // CANONICAL REST TRADE DECODING
        // ========================================================

        use crate::live::config::exchange::{ExchangeId, MarketType};

        use crate::live::infrastructure::normalization::context::NormalizationContext;
        use crate::live::infrastructure::normalization::trades::decoder::TradeDecoder;

        use crate::live::stream::request::{
            CreateSourceStreamRequest, QuantityType, SourceChannel,
        };

        use rust_decimal::Decimal;
        use serde_json::json;

        // --------------------------------------------------------
        // AUTHORITATIVE INSTRUMENT CONTEXT
        // --------------------------------------------------------

        let request = CreateSourceStreamRequest {
            instrument_id: 6163,
            exchange: ExchangeId::Binance,
            symbol: "BTCUSDT".into(),
            market_category: MarketType::Spot,
            instrument_type: "spot".into(),

            base_asset: Some("BTC".into()),
            quote_asset: Some("USDT".into()),
            settlement_asset: None,

            quantity_type: QuantityType::Base,
            contract_value: None,
            contract_value_asset: None,

            channel: SourceChannel::Trades,
        };

        let context = NormalizationContext::new(&request, "rest-binance-spot").unwrap();

        // --------------------------------------------------------
        // NATIVE BINANCE REST TRADE
        // --------------------------------------------------------

        let native = json!({
            "id": 6787820768_u64,
            "price": "80860.70000000",
            "qty": "0.00007000",
            "quoteQty": "5.66024900",
            "time": 1792806211156_i64,
            "isBuyerMaker": true,
            "isBestMatch": true
        });

        // --------------------------------------------------------
        // DECODE USING THE REST PLAN
        // --------------------------------------------------------

        let decoder = TradeDecoder::new(rest_plan).unwrap();

        let trade = decoder.decode_record(&native, &context).unwrap();
        // ========================================================
        // FULL REST RESPONSE DECODING
        // ========================================================

        use crate::live::infrastructure::normalization::message::NormalizationSource;
        use crate::live::infrastructure::normalization::trades::rest::decoder::RestTradeDecoder;

        let response = json!([
            {
                "id": 6787820768_u64,
                "price": "80860.70000000",
                "qty": "0.00007000",
                "time": 1792806211156_i64,
                "isBuyerMaker": true
            },
            {
                "id": 6787820769_u64,
                "price": "80860.80000000",
                "qty": "0.00008000",
                "time": 1792806211157_i64,
                "isBuyerMaker": false
            }
        ]);

        let batch =
            RestTradeDecoder::decode_validated_response(&decoder, &response, &context).unwrap();

        assert_eq!(batch.source(), NormalizationSource::Rest);

        assert_eq!(batch.record_count(), 2);

        assert_eq!(batch.trades()[0].trade_id.as_deref(), Some("6787820768"),);

        assert_eq!(batch.trades()[1].trade_id.as_deref(), Some("6787820769"),);

        assert_eq!(batch.trades()[0].side, TradeSide::Sell);
        assert_eq!(batch.trades()[1].side, TradeSide::Buy);

        // Source record ordering must be preserved.
        assert!(
            batch.trades()[0].envelope.event_timestamp_ns
                < batch.trades()[1].envelope.event_timestamp_ns
        );
        // ========================================================
        // MALFORMED RECORD REJECTS THE ENTIRE BATCH
        // ========================================================

        let malformed_response = json!([
            {
                "id": 6787820768_u64,
                "price": "80860.70000000",
                "qty": "0.00007000",
                "time": 1792806211156_i64,
                "isBuyerMaker": true
            },
            {
                "id": 6787820769_u64,
                "price": "INVALID_PRICE",
                "qty": "0.00008000",
                "time": 1792806211157_i64,
                "isBuyerMaker": false
            }
        ]);

        let result =
            RestTradeDecoder::decode_validated_response(&decoder, &malformed_response, &context);

        assert!(
            result.is_err(),
            "a malformed REST record must reject the entire batch",
        );
        // --------------------------------------------------------
        // VERIFY CANONICAL IDENTITY
        // --------------------------------------------------------

        assert_eq!(trade.trade_id.as_deref(), Some("6787820768"),);

        assert_eq!(trade.envelope.instrument_id, 6163);
        assert_eq!(trade.envelope.symbol, "BTCUSDT");
        assert_eq!(trade.envelope.stream_id, "rest-binance-spot");

        // --------------------------------------------------------
        // VERIFY AGGRESSOR SIDE
        // --------------------------------------------------------

        // isBuyerMaker=true means the buyer was maker.
        // Therefore, the seller was the aggressor.
        assert_eq!(trade.side, TradeSide::Sell);

        // --------------------------------------------------------
        // VERIFY PRICE AND QUANTITIES
        // --------------------------------------------------------

        assert_eq!(
            trade.price,
            Decimal::from_str_exact("80860.70000000").unwrap(),
        );

        assert_eq!(
            trade.quantity_base,
            Some(Decimal::from_str_exact("0.00007000").unwrap()),
        );

        assert_eq!(
            trade.quantity_quote,
            Some(Decimal::from_str_exact("5.66024900").unwrap()),
        );

        assert_eq!(trade.quantity_contracts, None);

        // --------------------------------------------------------
        // VERIFY TIMESTAMP
        // --------------------------------------------------------

        assert_eq!(trade.envelope.event_timestamp_ns, 1_792_806_211_156_000_000,);
    }
    #[test]
    fn normalizes_bybit_spot_rest_trades() {
        use std::collections::BTreeMap;

        use crate::canonical::TradeSide;

        use crate::live::config::{
            exchange::{ExchangeId, MarketType},
            format::FormatConfigStore,
        };

        use crate::live::infrastructure::normalization::context::NormalizationContext;
        use crate::live::infrastructure::normalization::message::NormalizationSource;
        use crate::live::infrastructure::normalization::numeric::TimestampUnit;
        use crate::live::infrastructure::normalization::quantity::QuantityPrecisionPolicy;
        use crate::live::infrastructure::normalization::side::{
            AggressorSideNormalizer, NativeSideValue,
        };
        use crate::live::infrastructure::normalization::trades::compiler::TradeNormalizationCompiler;
        use crate::live::infrastructure::normalization::trades::decoder::TradeDecoder;
        use crate::live::infrastructure::normalization::trades::fields::NativeTradeFieldLayout;
        use crate::live::infrastructure::normalization::trades::rest::decoder::RestTradeDecoder;

        use crate::live::stream::request::{
            CreateSourceStreamRequest, QuantityType, SourceChannel,
        };

        use rust_decimal::Decimal;
        use serde_json::json;

        // ========================================================
        // EXISTING WEBSOCKET PLAN — SHARED ECONOMIC SEMANTICS
        // ========================================================

        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live/formats");

        let formats = FormatConfigStore::load_dir(root).unwrap();

        let format = formats
            .trade("bybit_spot")
            .expect("missing Bybit Spot trade format");

        let websocket_plan = TradeNormalizationCompiler::compile_websocket(
            format,
            1000,
            QuantityPrecisionPolicy::Exact,
        )
        .unwrap();

        // ========================================================
        // REST SIDE MAPPING
        // ========================================================

        let side = AggressorSideNormalizer::mapped(BTreeMap::from([
            (NativeSideValue::String("Buy".into()), TradeSide::Buy),
            (NativeSideValue::String("Sell".into()), TradeSide::Sell),
        ]))
        .unwrap();

        // ========================================================
        // REST FIELD LAYOUT
        // ========================================================

        let layout = NativeTradeFieldLayout {
            trade_id: Some("execId".into()),
            price: "price".into(),
            quantity: "size".into(),
            timestamp: "time".into(),
            side: Some("side".into()),
            sequence: Some("seq".into()),
        };

        let endpoint = endpoint("bybit_spot", "recent_trades");

        let rest_plan = RestTradeNormalizationCompiler::compile_individual(
            &websocket_plan,
            &endpoint,
            layout,
            TimestampUnit::Milliseconds,
            side,
            Some("symbol".into()),
            1000,
        )
        .unwrap();

        let decoder = TradeDecoder::new(rest_plan).unwrap();

        // ========================================================
        // AUTHORITATIVE INSTRUMENT CONTEXT
        // ========================================================

        let request = CreateSourceStreamRequest {
            instrument_id: 1,
            exchange: ExchangeId::Bybit,
            symbol: "BTCUSDT".into(),
            market_category: MarketType::Spot,
            instrument_type: "spot".into(),

            base_asset: Some("BTC".into()),
            quote_asset: Some("USDT".into()),
            settlement_asset: None,

            quantity_type: QuantityType::Base,
            contract_value: None,
            contract_value_asset: None,

            channel: SourceChannel::Trades,
        };

        let context = NormalizationContext::new(&request, "rest-bybit-spot").unwrap();

        // ========================================================
        // REAL BYBIT REST RESPONSE STRUCTURE
        // ========================================================

        let response = json!({
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "list": [
                    {
                        "execId": "2290000001242830491",
                        "price": "80852.6",
                        "size": "0.016079",
                        "side": "Sell",
                        "symbol": "BTCUSDT",
                        "time": "1792806245495",
                        "seq": "116029798832",
                        "isBlockTrade": false,
                        "isRPITrade": false
                    }
                ]
            }
        });

        // ========================================================
        // CANONICAL NORMALIZATION
        // ========================================================

        let batch =
            RestTradeDecoder::decode_validated_response(&decoder, &response, &context).unwrap();

        assert_eq!(batch.source(), NormalizationSource::Rest);
        assert_eq!(batch.record_count(), 1);

        let trade = &batch.trades()[0];

        // Native execution identity.
        assert_eq!(trade.trade_id.as_deref(), Some("2290000001242830491"),);

        // Native sequence preservation.
        assert_eq!(trade.sequence, Some(116029798832));

        // Instrument identity.
        assert_eq!(trade.envelope.instrument_id, 1);
        assert_eq!(trade.envelope.symbol, "BTCUSDT");

        // Aggressor interpretation.
        assert_eq!(trade.side, TradeSide::Sell);

        // Exact price and economic quantities.
        assert_eq!(trade.price, Decimal::from_str_exact("80852.6").unwrap(),);

        assert_eq!(
            trade.quantity_base,
            Some(Decimal::from_str_exact("0.016079").unwrap()),
        );

        assert_eq!(
            trade.quantity_quote,
            Some(Decimal::from_str_exact("1300.0289554").unwrap()),
        );

        assert_eq!(trade.quantity_contracts, None);

        // Exact exchange timestamp.
        assert_eq!(trade.envelope.event_timestamp_ns, 1_792_806_245_495_000_000,);

        // ========================================================
        // WRONG INSTRUMENT MUST BE REJECTED
        // ========================================================

        let mut wrong_instrument = response.clone();

        wrong_instrument["result"]["list"][0]["symbol"] = json!("ETHUSDT");

        assert!(
            RestTradeDecoder::decode_validated_response(&decoder, &wrong_instrument, &context,)
                .is_err(),
            "REST trades from another instrument must be rejected",
        );
    }
    #[test]
    fn normalizes_bybit_derivatives_rest_trades() {
        use std::collections::BTreeMap;

        use crate::canonical::TradeSide;
        use crate::live::config::{
            exchange::{ExchangeId, MarketType},
            format::FormatConfigStore,
        };
        use crate::live::infrastructure::normalization::context::NormalizationContext;
        use crate::live::infrastructure::normalization::message::NormalizationSource;
        use crate::live::infrastructure::normalization::numeric::TimestampUnit;
        use crate::live::infrastructure::normalization::quantity::QuantityPrecisionPolicy;
        use crate::live::infrastructure::normalization::side::{
            AggressorSideNormalizer, NativeSideValue,
        };
        use crate::live::infrastructure::normalization::trades::compiler::TradeNormalizationCompiler;
        use crate::live::infrastructure::normalization::trades::decoder::TradeDecoder;
        use crate::live::infrastructure::normalization::trades::fields::NativeTradeFieldLayout;
        use crate::live::infrastructure::normalization::trades::rest::decoder::RestTradeDecoder;
        use crate::live::stream::request::{
            CreateSourceStreamRequest, QuantityType, SourceChannel,
        };

        use rust_decimal::Decimal;
        use serde_json::json;

        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live/formats");

        let formats = FormatConfigStore::load_dir(root).unwrap();

        let cases = [
            (
                "bybit_linear",
                MarketType::Linear,
                688,
                "BTCUSDT",
                QuantityType::Base,
                "80815.80",
                "0.062",
                "07059e1c-b59e-5e96-afae-ca5d1059c175",
                "831582192302",
                "1792806264509",
            ),
            (
                "bybit_inverse",
                MarketType::Inverse,
                1427,
                "BTCUSD",
                QuantityType::Quote,
                "80722.40",
                "9",
                "b932904f-d9ff-5294-87d6-eace9444a237",
                "119719200035",
                "1792806244294",
            ),
        ];

        for (
            profile,
            market,
            instrument_id,
            symbol,
            quantity_type,
            price,
            size,
            trade_id,
            sequence,
            timestamp,
        ) in cases
        {
            // ================================================
            // COMPILE WEBSOCKET PLAN AS SHARED SEMANTICS
            // ================================================

            let format = formats.trade(profile).unwrap();

            let websocket_plan = TradeNormalizationCompiler::compile_websocket(
                format,
                1000,
                QuantityPrecisionPolicy::Round { decimal_places: 18 },
            )
            .unwrap();

            // ================================================
            // COMPILE REST FIELD MAPPINGS
            // ================================================

            let side = AggressorSideNormalizer::mapped(BTreeMap::from([
                (NativeSideValue::String("Buy".into()), TradeSide::Buy),
                (NativeSideValue::String("Sell".into()), TradeSide::Sell),
            ]))
            .unwrap();

            let layout = NativeTradeFieldLayout {
                trade_id: Some("execId".into()),
                price: "price".into(),
                quantity: "size".into(),
                timestamp: "time".into(),
                side: Some("side".into()),
                sequence: Some("seq".into()),
            };

            let endpoint = endpoint(profile, "recent_trades");

            let rest_plan = RestTradeNormalizationCompiler::compile_individual(
                &websocket_plan,
                &endpoint,
                layout,
                TimestampUnit::Milliseconds,
                side,
                Some("symbol".into()),
                1000,
            )
            .unwrap();

            let decoder = TradeDecoder::new(rest_plan).unwrap();

            // ================================================
            // INSTRUMENT CONTEXT
            // ================================================

            let quote_asset = match market {
                MarketType::Linear => "USDT",
                MarketType::Inverse => "USD",
                _ => unreachable!(),
            };

            let request = CreateSourceStreamRequest {
                instrument_id,
                exchange: ExchangeId::Bybit,
                symbol: symbol.into(),
                market_category: market,
                instrument_type: "perpetual".into(),

                base_asset: Some("BTC".into()),
                quote_asset: Some(quote_asset.into()),

                settlement_asset: Some(
                    match market {
                        MarketType::Linear => "USDT",
                        MarketType::Inverse => "BTC",
                        _ => unreachable!(),
                    }
                    .into(),
                ),

                quantity_type,

                contract_value: None,
                contract_value_asset: None,

                channel: SourceChannel::Trades,
            };

            let context = NormalizationContext::new(&request, format!("rest-{profile}")).unwrap();

            // ================================================
            // NATIVE REST RESPONSE
            // ================================================

            let response = json!({
                "retCode": 0,
                "retMsg": "OK",
                "result": {
                    "list": [
                        {
                            "execId": trade_id,
                            "price": price,
                            "size": size,
                            "side": "Buy",
                            "symbol": symbol,
                            "time": timestamp,
                            "seq": sequence,
                            "isBlockTrade": false,
                            "isRPITrade": false
                        }
                    ]
                }
            });

            // ================================================
            // CANONICAL DECODING
            // ================================================

            let batch =
                RestTradeDecoder::decode_validated_response(&decoder, &response, &context).unwrap();

            assert_eq!(batch.source(), NormalizationSource::Rest);
            assert_eq!(batch.record_count(), 1);

            let trade = &batch.trades()[0];

            assert_eq!(trade.trade_id.as_deref(), Some(trade_id));
            assert_eq!(trade.side, TradeSide::Buy);

            assert_eq!(trade.envelope.instrument_id, instrument_id);
            assert_eq!(trade.envelope.symbol, symbol);

            assert_eq!(trade.sequence, Some(sequence.parse::<u64>().unwrap()),);

            assert_eq!(
                trade.envelope.event_timestamp_ns,
                timestamp.parse::<i64>().unwrap() * 1_000_000,
            );

            assert_eq!(trade.price, Decimal::from_str_exact(price).unwrap(),);

            assert_eq!(trade.quantity_contracts, None);

            // ================================================
            // ECONOMIC QUANTITY SEMANTICS
            // ================================================

            match market {
                MarketType::Linear => {
                    assert_eq!(
                        trade.quantity_base,
                        Some(Decimal::from_str_exact("0.062").unwrap()),
                    );

                    assert_eq!(
                        trade.quantity_quote,
                        Some(Decimal::from_str_exact("5010.5796").unwrap()),
                    );
                }

                MarketType::Inverse => {
                    // Native size represents USD quantity.
                    assert_eq!(trade.quantity_quote, Some(Decimal::from(9)),);

                    let base = trade.quantity_base.unwrap();

                    assert!(base > Decimal::ZERO);

                    // Inverse division may require rounding.
                    let reconstructed_quote = base.checked_mul(trade.price).unwrap();

                    let error = (reconstructed_quote - Decimal::from(9)).abs();

                    assert!(
                        error <= Decimal::from_str_exact("0.00000001").unwrap(),
                        "{profile}: inverse conversion error: {error}",
                    );
                }

                MarketType::Spot => unreachable!(),
            }
        }
    }
    #[test]
    fn normalizes_bitget_all_markets_rest_trades() {
        use std::collections::BTreeMap;

        use crate::canonical::TradeSide;

        use crate::live::config::{
            exchange::{ExchangeId, MarketType},
            format::FormatConfigStore,
        };

        use crate::live::infrastructure::normalization::context::NormalizationContext;
        use crate::live::infrastructure::normalization::message::NormalizationSource;
        use crate::live::infrastructure::normalization::numeric::TimestampUnit;
        use crate::live::infrastructure::normalization::quantity::QuantityPrecisionPolicy;
        use crate::live::infrastructure::normalization::side::{
            AggressorSideNormalizer, NativeSideValue,
        };
        use crate::live::infrastructure::normalization::trades::compiler::TradeNormalizationCompiler;
        use crate::live::infrastructure::normalization::trades::decoder::TradeDecoder;
        use crate::live::infrastructure::normalization::trades::fields::NativeTradeFieldLayout;
        use crate::live::infrastructure::normalization::trades::rest::decoder::RestTradeDecoder;

        use crate::live::stream::request::{
            CreateSourceStreamRequest, QuantityType, SourceChannel,
        };

        use rust_decimal::Decimal;
        use serde_json::json;

        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live/formats");

        let formats = FormatConfigStore::load_dir(root).unwrap();

        // Values originate from the captured REST fixtures.
        let cases = [
            (
                "bitget_spot",
                MarketType::Spot,
                11545,
                "BTCUSDT",
                QuantityType::Base,
                "1497703653869076480",
                "80844.63",
                "0.000621",
                "sell",
                "1792806297747",
            ),
            (
                "bitget_linear",
                MarketType::Linear,
                14215,
                "BTCUSDT",
                QuantityType::Base,
                "1497703677071998976",
                "80814.9",
                "0.0009",
                "buy",
                "1792806303279",
            ),
            (
                "bitget_inverse",
                MarketType::Inverse,
                900_000_001,
                "BTCUSD_CM",
                QuantityType::Quote,
                "1497704119696875520",
                "80785.0",
                "100",
                "sell",
                "1792806408809",
            ),
        ];

        for (
            profile,
            market,
            instrument_id,
            symbol,
            quantity_type,
            trade_id,
            price,
            size,
            native_side,
            timestamp,
        ) in cases
        {
            // ================================================
            // EXISTING SHARED NORMALIZATION SEMANTICS
            // ================================================

            let format = formats.trade(profile).unwrap();

            let websocket_plan = TradeNormalizationCompiler::compile_websocket(
                format,
                1000,
                QuantityPrecisionPolicy::Round { decimal_places: 18 },
            )
            .unwrap();

            // ================================================
            // REST SIDE MAPPING
            // ================================================

            let side = AggressorSideNormalizer::mapped(BTreeMap::from([
                (NativeSideValue::String("buy".into()), TradeSide::Buy),
                (NativeSideValue::String("sell".into()), TradeSide::Sell),
            ]))
            .unwrap();

            // ================================================
            // REST FIELD LAYOUT
            // ================================================

            let layout = NativeTradeFieldLayout {
                trade_id: Some("execId".into()),
                price: "price".into(),
                quantity: "size".into(),
                timestamp: "ts".into(),
                side: Some("side".into()),
                sequence: None,
            };

            let rest_plan = RestTradeNormalizationCompiler::compile_individual(
                &websocket_plan,
                &endpoint(profile, "recent_trades"),
                layout,
                TimestampUnit::Milliseconds,
                side,
                None,
                1000,
            )
            .unwrap();

            let decoder = TradeDecoder::new(rest_plan).unwrap();

            // ================================================
            // INSTRUMENT METADATA
            // ================================================

            let quote_asset = if market == MarketType::Inverse {
                "USD"
            } else {
                "USDT"
            };

            let settlement_asset = match market {
                MarketType::Spot => None,
                MarketType::Linear => Some("USDT".to_owned()),
                MarketType::Inverse => Some("BTC".to_owned()),
            };

            let request = CreateSourceStreamRequest {
                instrument_id,
                exchange: ExchangeId::Bitget,
                symbol: symbol.into(),
                market_category: market,

                instrument_type: if market == MarketType::Spot {
                    "spot"
                } else {
                    "perpetual"
                }
                .into(),

                base_asset: Some("BTC".into()),
                quote_asset: Some(quote_asset.into()),
                settlement_asset,

                quantity_type,
                contract_value: None,
                contract_value_asset: None,

                channel: SourceChannel::Trades,
            };

            let context = NormalizationContext::new(&request, format!("rest-{profile}")).unwrap();

            // ================================================
            // NATIVE REST RESPONSE
            // ================================================

            let response = json!({
                "code": "00000",
                "msg": "success",
                "data": [
                    {
                        "execId": trade_id,
                        "price": price,
                        "size": size,
                        "side": native_side,
                        "ts": timestamp,
                        "isRPI": "NO"
                    }
                ]
            });

            // ================================================
            // CANONICAL DECODING
            // ================================================

            let batch =
                RestTradeDecoder::decode_validated_response(&decoder, &response, &context).unwrap();

            assert_eq!(batch.source(), NormalizationSource::Rest);
            assert_eq!(batch.record_count(), 1);

            let trade = &batch.trades()[0];

            assert_eq!(trade.trade_id.as_deref(), Some(trade_id),);

            assert_eq!(trade.envelope.instrument_id, instrument_id,);

            assert_eq!(trade.envelope.symbol, symbol);

            assert_eq!(
                trade.envelope.event_timestamp_ns,
                timestamp.parse::<i64>().unwrap() * 1_000_000,
            );

            assert_eq!(trade.price, Decimal::from_str_exact(price).unwrap(),);

            let expected_side = match native_side {
                "buy" => TradeSide::Buy,
                "sell" => TradeSide::Sell,
                _ => unreachable!(),
            };

            assert_eq!(trade.side, expected_side);
            assert_eq!(trade.quantity_contracts, None);
            assert_eq!(trade.sequence, None);

            // ================================================
            // ECONOMIC QUANTITY CONVERSION
            // ================================================

            let native_quantity = Decimal::from_str_exact(size).unwrap();

            match market {
                MarketType::Spot | MarketType::Linear => {
                    assert_eq!(trade.quantity_base, Some(native_quantity),);

                    let expected_quote = native_quantity.checked_mul(trade.price).unwrap();

                    assert_eq!(trade.quantity_quote, Some(expected_quote),);
                }

                MarketType::Inverse => {
                    // Native size is quote-denominated.
                    assert_eq!(trade.quantity_quote, Some(native_quantity),);

                    let base = trade.quantity_base.unwrap();

                    assert!(base > Decimal::ZERO);

                    let reconstructed_quote = base.checked_mul(trade.price).unwrap();

                    let error = (reconstructed_quote - native_quantity).abs();

                    assert!(
                        error <= Decimal::from_str_exact("0.00000001").unwrap(),
                        "{profile}: quote reconstruction error: {error}",
                    );
                }
            }
        }
    }
    #[test]
    fn normalizes_okx_all_markets_rest_trades() {
        use std::collections::BTreeMap;

        use crate::canonical::TradeSide;

        use crate::live::config::{
            exchange::{ExchangeId, MarketType},
            format::FormatConfigStore,
        };

        use crate::live::infrastructure::normalization::context::NormalizationContext;
        use crate::live::infrastructure::normalization::message::NormalizationSource;
        use crate::live::infrastructure::normalization::numeric::TimestampUnit;
        use crate::live::infrastructure::normalization::quantity::QuantityPrecisionPolicy;
        use crate::live::infrastructure::normalization::side::{
            AggressorSideNormalizer, NativeSideValue,
        };
        use crate::live::infrastructure::normalization::trades::compiler::TradeNormalizationCompiler;
        use crate::live::infrastructure::normalization::trades::decoder::TradeDecoder;
        use crate::live::infrastructure::normalization::trades::fields::NativeTradeFieldLayout;
        use crate::live::infrastructure::normalization::trades::rest::decoder::RestTradeDecoder;

        use crate::live::stream::request::{
            CreateSourceStreamRequest, QuantityType, SourceChannel,
        };

        use rust_decimal::Decimal;
        use serde_json::json;

        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live/formats");

        let formats = FormatConfigStore::load_dir(root).unwrap();

        // Representative native values from captured OKX REST fixtures.
        let cases = [
            (
                "okx_spot",
                MarketType::Spot,
                3336,
                "BTC-USDT",
                QuantityType::Base,
                None,
                None,
                "1074053735",
                "80860.6",
                "0.01138995",
                "sell",
                "1792806276641",
            ),
            (
                "okx_linear",
                MarketType::Linear,
                4063,
                "BTC-USDT-SWAP",
                QuantityType::Contracts,
                Some("0.01"),
                Some("BTC"),
                "3017015046",
                "80809.4",
                "0.5",
                "buy",
                "1792806282210",
            ),
            (
                "okx_inverse",
                MarketType::Inverse,
                4048,
                "BTC-USD-SWAP",
                QuantityType::Contracts,
                Some("100"),
                Some("USD"),
                "481278554",
                "80720.4",
                "5.1",
                "buy",
                "1792806291213",
            ),
        ];

        for (
            profile,
            market,
            instrument_id,
            symbol,
            quantity_type,
            contract_value,
            contract_asset,
            trade_id,
            price,
            size,
            native_side,
            timestamp,
        ) in cases
        {
            // ====================================================
            // COMPILE SHARED TRADE SEMANTICS
            // ====================================================

            let format = formats.trade(profile).unwrap();

            let websocket_plan = TradeNormalizationCompiler::compile_websocket(
                format,
                1000,
                QuantityPrecisionPolicy::Round { decimal_places: 18 },
            )
            .unwrap();

            // ====================================================
            // REST SIDE MAPPING
            // ====================================================

            let side = AggressorSideNormalizer::mapped(BTreeMap::from([
                (NativeSideValue::String("buy".into()), TradeSide::Buy),
                (NativeSideValue::String("sell".into()), TradeSide::Sell),
            ]))
            .unwrap();

            // ====================================================
            // REST FIELD LAYOUT
            // ====================================================

            let layout = NativeTradeFieldLayout {
                trade_id: Some("tradeId".into()),
                price: "px".into(),
                quantity: "sz".into(),
                timestamp: "ts".into(),
                side: Some("side".into()),
                sequence: None,
            };

            let rest_plan = RestTradeNormalizationCompiler::compile_individual(
                &websocket_plan,
                &endpoint(profile, "recent_trades"),
                layout,
                TimestampUnit::Milliseconds,
                side,
                Some("instId".into()),
                1000,
            )
            .unwrap();

            let decoder = TradeDecoder::new(rest_plan).unwrap();

            // ====================================================
            // AUTHORITATIVE INSTRUMENT CONTEXT
            // ====================================================

            let quote_asset = match market {
                MarketType::Inverse => "USD",
                MarketType::Spot | MarketType::Linear => "USDT",
            };

            let settlement_asset = match market {
                MarketType::Spot => None,
                MarketType::Linear => Some("USDT".to_owned()),
                MarketType::Inverse => Some("BTC".to_owned()),
            };

            let request = CreateSourceStreamRequest {
                instrument_id,
                exchange: ExchangeId::Okx,
                symbol: symbol.into(),
                market_category: market,

                instrument_type: if market == MarketType::Spot {
                    "spot"
                } else {
                    "perpetual"
                }
                .into(),

                base_asset: Some("BTC".into()),
                quote_asset: Some(quote_asset.into()),
                settlement_asset,

                quantity_type,

                contract_value: contract_value.map(str::to_owned),
                contract_value_asset: contract_asset.map(str::to_owned),

                channel: SourceChannel::Trades,
            };

            let context = NormalizationContext::new(&request, format!("rest-{profile}")).unwrap();

            // ====================================================
            // NATIVE REST RESPONSE
            // ====================================================

            let response = json!({
                "code": "0",
                "msg": "",
                "data": [
                    {
                        "instId": symbol,
                        "tradeId": trade_id,
                        "px": price,
                        "sz": size,
                        "side": native_side,
                        "ts": timestamp,
                        "source": "0"
                    }
                ]
            });

            // ====================================================
            // CANONICAL DECODING
            // ====================================================

            let batch =
                RestTradeDecoder::decode_validated_response(&decoder, &response, &context).unwrap();

            assert_eq!(batch.source(), NormalizationSource::Rest);
            assert_eq!(batch.record_count(), 1);

            let trade = &batch.trades()[0];

            assert_eq!(trade.trade_id.as_deref(), Some(trade_id));
            assert_eq!(trade.envelope.instrument_id, instrument_id);
            assert_eq!(trade.envelope.symbol, symbol);

            assert_eq!(trade.price, Decimal::from_str_exact(price).unwrap(),);

            assert_eq!(
                trade.envelope.event_timestamp_ns,
                timestamp.parse::<i64>().unwrap() * 1_000_000,
            );

            let expected_side = match native_side {
                "buy" => TradeSide::Buy,
                "sell" => TradeSide::Sell,
                _ => unreachable!(),
            };

            assert_eq!(trade.side, expected_side);

            // ====================================================
            // ECONOMIC QUANTITY VERIFICATION
            // ====================================================

            let native_quantity = Decimal::from_str_exact(size).unwrap();

            let expected_base;
            let expected_quote;

            match market {
                MarketType::Spot => {
                    expected_base = native_quantity;

                    expected_quote = native_quantity.checked_mul(trade.price).unwrap();

                    assert_eq!(trade.quantity_contracts, None);
                }

                MarketType::Linear => {
                    // Contracts × 0.01 BTC per contract.
                    expected_base = native_quantity
                        .checked_mul(Decimal::from_str_exact("0.01").unwrap())
                        .unwrap();

                    expected_quote = expected_base.checked_mul(trade.price).unwrap();

                    assert_eq!(trade.quantity_contracts, Some(native_quantity),);
                }

                MarketType::Inverse => {
                    // Contracts × 100 USD per contract.
                    expected_quote = native_quantity.checked_mul(Decimal::from(100)).unwrap();

                    expected_base = trade.quantity_base.unwrap();

                    // Base quantity requires division and may
                    // have been rounded to 18 decimal places.
                    let reconstructed = expected_base.checked_mul(trade.price).unwrap();

                    let error = (reconstructed - expected_quote).abs();

                    assert!(
                        error <= Decimal::from_str_exact("0.00000001").unwrap(),
                        "{profile}: inverse conversion error: {error}",
                    );

                    assert_eq!(trade.quantity_contracts, Some(native_quantity),);
                }
            }

            assert_eq!(trade.quantity_base, Some(expected_base));
            assert_eq!(trade.quantity_quote, Some(expected_quote));

            // ====================================================
            // WRONG INSTRUMENT REJECTION
            // ====================================================

            let mut wrong_instrument = response.clone();

            wrong_instrument["data"][0]["instId"] = json!("ETH-USDT");

            assert!(
                RestTradeDecoder::decode_validated_response(&decoder, &wrong_instrument, &context,)
                    .is_err(),
                "{profile}: foreign instrument was accepted",
            );
        }
    }
    #[test]
    fn normalizes_gateio_spot_rest_trades() {
        use std::collections::BTreeMap;

        use crate::canonical::TradeSide;

        use crate::live::config::{
            exchange::{ExchangeId, MarketType},
            format::FormatConfigStore,
        };

        use crate::live::infrastructure::normalization::context::NormalizationContext;
        use crate::live::infrastructure::normalization::message::NormalizationSource;
        use crate::live::infrastructure::normalization::numeric::TimestampUnit;
        use crate::live::infrastructure::normalization::quantity::QuantityPrecisionPolicy;
        use crate::live::infrastructure::normalization::side::{
            AggressorSideNormalizer, NativeSideValue,
        };
        use crate::live::infrastructure::normalization::trades::compiler::TradeNormalizationCompiler;
        use crate::live::infrastructure::normalization::trades::decoder::TradeDecoder;
        use crate::live::infrastructure::normalization::trades::fields::NativeTradeFieldLayout;
        use crate::live::infrastructure::normalization::trades::rest::decoder::RestTradeDecoder;

        use crate::live::stream::request::{
            CreateSourceStreamRequest, QuantityType, SourceChannel,
        };

        use rust_decimal::Decimal;
        use serde_json::json;

        // ========================================================
        // LOAD EXISTING FORMAT
        // ========================================================

        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live/formats");

        let formats = FormatConfigStore::load_dir(root).unwrap();

        let format = formats
            .trade("gateio_spot")
            .expect("missing Gate.io Spot trade format");

        let websocket_plan = TradeNormalizationCompiler::compile_websocket(
            format,
            1000,
            QuantityPrecisionPolicy::Exact,
        )
        .unwrap();

        // ========================================================
        // REST SIDE MAPPING
        // ========================================================

        let side = AggressorSideNormalizer::mapped(BTreeMap::from([
            (NativeSideValue::String("buy".into()), TradeSide::Buy),
            (NativeSideValue::String("sell".into()), TradeSide::Sell),
        ]))
        .unwrap();

        // ========================================================
        // REST FIELD LAYOUT
        // ========================================================

        let layout = NativeTradeFieldLayout {
            trade_id: Some("id".into()),
            price: "price".into(),
            quantity: "amount".into(),
            timestamp: "create_time_ms".into(),
            side: Some("side".into()),
            sequence: Some("sequence_id".into()),
        };

        let rest_plan = RestTradeNormalizationCompiler::compile_individual(
            &websocket_plan,
            &endpoint("gateio_spot", "historical_trades"),
            layout,
            TimestampUnit::DecimalMilliseconds,
            side,
            Some("currency_pair".into()),
            1000,
        )
        .unwrap();

        let decoder = TradeDecoder::new(rest_plan).unwrap();

        // ========================================================
        // INSTRUMENT CONTEXT
        // ========================================================

        let request = CreateSourceStreamRequest {
            instrument_id: 15850,
            exchange: ExchangeId::Gateio,
            symbol: "BTC_USDT".into(),
            market_category: MarketType::Spot,
            instrument_type: "spot".into(),

            base_asset: Some("BTC".into()),
            quote_asset: Some("USDT".into()),
            settlement_asset: None,

            quantity_type: QuantityType::Base,
            contract_value: None,
            contract_value_asset: None,

            channel: SourceChannel::Trades,
        };

        let context = NormalizationContext::new(&request, "rest-gateio-spot").unwrap();

        // ========================================================
        // CAPTURED GATE.IO REST TRADE
        // ========================================================

        let response = json!([
            {
                "id": "224728414",
                "price": "80846.4",
                "amount": "0.015721",
                "side": "sell",
                "currency_pair": "BTC_USDT",
                "sequence_id": "224728414",
                "create_time": "1792806302",
                "create_time_ms": "1792806302350.350000"
            }
        ]);

        // ========================================================
        // CANONICAL NORMALIZATION
        // ========================================================

        let batch =
            RestTradeDecoder::decode_validated_response(&decoder, &response, &context).unwrap();

        assert_eq!(batch.source(), NormalizationSource::Rest);
        assert_eq!(batch.record_count(), 1);

        let trade = &batch.trades()[0];

        // Trade identity.
        assert_eq!(trade.trade_id.as_deref(), Some("224728414"),);

        assert_eq!(trade.sequence, Some(224728414));

        // Instrument identity.
        assert_eq!(trade.envelope.instrument_id, 15850);
        assert_eq!(trade.envelope.symbol, "BTC_USDT");

        // Aggressor side.
        assert_eq!(trade.side, TradeSide::Sell);

        // Exact price.
        assert_eq!(trade.price, Decimal::from_str_exact("80846.4").unwrap(),);

        // Base-denominated quantity.
        assert_eq!(
            trade.quantity_base,
            Some(Decimal::from_str_exact("0.015721").unwrap()),
        );

        // Quote conversion.
        let expected_quote = Decimal::from_str_exact("0.015721")
            .unwrap()
            .checked_mul(Decimal::from_str_exact("80846.4").unwrap())
            .unwrap();

        assert_eq!(trade.quantity_quote, Some(expected_quote));
        assert_eq!(trade.quantity_contracts, None);

        // Exact decimal-millisecond timestamp.
        assert_eq!(trade.envelope.event_timestamp_ns, 1_792_806_302_350_350_000,);

        // ========================================================
        // FOREIGN INSTRUMENT REJECTION
        // ========================================================

        let mut wrong_instrument = response.clone();

        wrong_instrument[0]["currency_pair"] = json!("ETH_USDT");

        assert!(
            RestTradeDecoder::decode_validated_response(&decoder, &wrong_instrument, &context,)
                .is_err(),
            "Gate.io REST trade from another instrument was accepted",
        );
    }
    #[test]
    fn normalizes_gateio_futures_rest_trades() {
        use crate::canonical::TradeSide;

        use crate::live::config::{
            exchange::{ExchangeId, MarketType},
            format::FormatConfigStore,
        };

        use crate::live::infrastructure::normalization::context::NormalizationContext;
        use crate::live::infrastructure::normalization::message::NormalizationSource;
        use crate::live::infrastructure::normalization::numeric::TimestampUnit;
        use crate::live::infrastructure::normalization::quantity::QuantityPrecisionPolicy;
        use crate::live::infrastructure::normalization::side::{
            AggressorSideNormalizer, PositiveQuantitySide,
        };
        use crate::live::infrastructure::normalization::trades::compiler::TradeNormalizationCompiler;
        use crate::live::infrastructure::normalization::trades::decoder::TradeDecoder;
        use crate::live::infrastructure::normalization::trades::fields::NativeTradeFieldLayout;
        use crate::live::infrastructure::normalization::trades::rest::decoder::RestTradeDecoder;

        use crate::live::stream::request::{
            CreateSourceStreamRequest, QuantityType, SourceChannel,
        };

        use rust_decimal::Decimal;
        use serde_json::{Value, json};

        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live/formats");

        let formats = FormatConfigStore::load_dir(root).unwrap();

        // Captured Gate.io REST trade records.
        let cases = [
            (
                "gateio_linear",
                MarketType::Linear,
                17397,
                "BTC_USDT",
                "USDT",
                "0.0001",
                "BTC",
                859437953_u64,
                "80819.8",
                -20_i64,
                "1792806317.933",
                1_792_806_317_933_000_000_i64,
                TradeSide::Sell,
            ),
            (
                "gateio_inverse",
                MarketType::Inverse,
                18256,
                "BTC_USD",
                "USD",
                "1",
                "USD",
                49646560_u64,
                "80767.6",
                424_i64,
                "1792806123.709",
                1_792_806_123_709_000_000_i64,
                TradeSide::Buy,
            ),
        ];

        for (
            profile,
            market,
            instrument_id,
            symbol,
            quote_asset,
            contract_value,
            contract_asset,
            trade_id,
            price,
            signed_size,
            timestamp,
            expected_timestamp_ns,
            expected_side,
        ) in cases
        {
            // ================================================
            // SHARED NORMALIZATION SEMANTICS
            // ================================================

            let format = formats.trade(profile).unwrap();

            let websocket_plan = TradeNormalizationCompiler::compile_websocket(
                format,
                1000,
                QuantityPrecisionPolicy::Round { decimal_places: 18 },
            )
            .unwrap();

            // ================================================
            // REST FIELD LAYOUT
            // ================================================

            let layout = NativeTradeFieldLayout {
                trade_id: Some("id".into()),
                price: "price".into(),
                quantity: "size".into(),
                timestamp: "create_time_ms".into(),
                side: None,
                sequence: None,
            };

            // Gate.io Futures encodes direction using
            // the sign of native contract size.
            let side = AggressorSideNormalizer::signed_quantity(PositiveQuantitySide::Buy);

            let rest_plan = RestTradeNormalizationCompiler::compile_individual(
                &websocket_plan,
                &endpoint(profile, "historical_trades"),
                layout,
                TimestampUnit::DecimalSeconds,
                side,
                Some("contract".into()),
                1000,
            )
            .unwrap();

            let decoder = TradeDecoder::new(rest_plan).unwrap();

            // ================================================
            // INSTRUMENT CONTEXT
            // ================================================

            let request = CreateSourceStreamRequest {
                instrument_id,
                exchange: ExchangeId::Gateio,
                symbol: symbol.into(),
                market_category: market,
                instrument_type: "perpetual".into(),

                base_asset: Some("BTC".into()),
                quote_asset: Some(quote_asset.into()),

                settlement_asset: Some(
                    match market {
                        MarketType::Linear => "USDT",
                        MarketType::Inverse => "BTC",
                        MarketType::Spot => unreachable!(),
                    }
                    .into(),
                ),

                quantity_type: QuantityType::Contracts,

                contract_value: Some(contract_value.into()),
                contract_value_asset: Some(contract_asset.into()),

                channel: SourceChannel::Trades,
            };

            let context = NormalizationContext::new(&request, format!("rest-{profile}")).unwrap();

            // ================================================
            // NATIVE REST RESPONSE
            // ================================================

            let mut response = json!([
                {
                    "contract": symbol,
                    "id": trade_id,
                    "price": price,
                    "size": signed_size
                }
            ]);

            // Important: parse the original decimal text.
            // Do not construct this number through f64.
            let native_timestamp: Value = serde_json::from_str(timestamp).unwrap();

            response[0]["create_time_ms"] = native_timestamp;

            // ================================================
            // CANONICAL DECODING
            // ================================================

            let batch =
                RestTradeDecoder::decode_validated_response(&decoder, &response, &context).unwrap();

            assert_eq!(batch.source(), NormalizationSource::Rest);
            assert_eq!(batch.record_count(), 1);

            let trade = &batch.trades()[0];

            assert_eq!(
                trade.trade_id.as_deref(),
                Some(trade_id.to_string().as_str()),
            );

            assert_eq!(trade.envelope.instrument_id, instrument_id);
            assert_eq!(trade.envelope.symbol, symbol);

            assert_eq!(trade.side, expected_side);

            assert_eq!(trade.envelope.event_timestamp_ns, expected_timestamp_ns,);

            assert_eq!(trade.price, Decimal::from_str_exact(price).unwrap(),);

            // ================================================
            // CONTRACT QUANTITY CONVERSION
            // ================================================

            let contracts = Decimal::from(signed_size).abs();

            assert_eq!(trade.quantity_contracts, Some(contracts),);

            match market {
                MarketType::Linear => {
                    // 20 contracts × 0.0001 BTC = 0.002 BTC.
                    let expected_base = contracts
                        .checked_mul(Decimal::from_str_exact(contract_value).unwrap())
                        .unwrap();

                    let expected_quote = expected_base.checked_mul(trade.price).unwrap();

                    assert_eq!(trade.quantity_base, Some(expected_base),);

                    assert_eq!(trade.quantity_quote, Some(expected_quote),);
                }

                MarketType::Inverse => {
                    // 424 contracts × 1 USD = 424 USD.
                    let expected_quote = contracts
                        .checked_mul(Decimal::from_str_exact(contract_value).unwrap())
                        .unwrap();

                    assert_eq!(trade.quantity_quote, Some(expected_quote),);

                    let base = trade.quantity_base.unwrap();

                    assert!(base > Decimal::ZERO);

                    let reconstructed = base.checked_mul(trade.price).unwrap();

                    let error = (reconstructed - expected_quote).abs();

                    assert!(
                        error <= Decimal::from_str_exact("0.00000001").unwrap(),
                        "{profile}: contract conversion error: {error}",
                    );
                }

                MarketType::Spot => unreachable!(),
            }

            // ================================================
            // FOREIGN INSTRUMENT REJECTION
            // ================================================

            let mut wrong_instrument = response.clone();

            wrong_instrument[0]["contract"] = json!("ETH_USDT");

            assert!(
                RestTradeDecoder::decode_validated_response(&decoder, &wrong_instrument, &context,)
                    .is_err(),
                "{profile}: foreign instrument was accepted",
            );
        }
    }
    #[test]
    fn normalizes_binance_derivatives_rest_trades() {
        use std::collections::BTreeMap;

        use crate::canonical::TradeSide;

        use crate::live::config::{
            exchange::{ExchangeId, MarketType},
            format::FormatConfigStore,
        };

        use crate::live::infrastructure::normalization::context::NormalizationContext;
        use crate::live::infrastructure::normalization::message::NormalizationSource;
        use crate::live::infrastructure::normalization::numeric::TimestampUnit;
        use crate::live::infrastructure::normalization::quantity::QuantityPrecisionPolicy;
        use crate::live::infrastructure::normalization::side::{
            AggressorSideNormalizer, NativeSideValue,
        };
        use crate::live::infrastructure::normalization::trades::compiler::TradeNormalizationCompiler;
        use crate::live::infrastructure::normalization::trades::decoder::TradeDecoder;
        use crate::live::infrastructure::normalization::trades::fields::NativeTradeFieldLayout;
        use crate::live::infrastructure::normalization::trades::rest::decoder::RestTradeDecoder;

        use crate::live::stream::request::{
            CreateSourceStreamRequest, QuantityType, SourceChannel,
        };

        use rust_decimal::Decimal;
        use serde_json::json;

        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live/formats");

        let formats = FormatConfigStore::load_dir(root).unwrap();

        // Representative records from captured Binance REST responses.
        let cases = [
            (
                "binance_linear",
                MarketType::Linear,
                9872,
                "BTCUSDT",
                QuantityType::Base,
                None,
                None,
                "8200040315",
                "80803.00",
                "0.001",
                1792806221690_i64,
            ),
            (
                "binance_inverse",
                MarketType::Inverse,
                10792,
                "BTCUSD_PERP",
                QuantityType::Contracts,
                Some("100"),
                Some("USD"),
                "1160577158",
                "80605.7",
                "1",
                1792805872412_i64,
            ),
        ];

        for (
            profile,
            market,
            instrument_id,
            symbol,
            quantity_type,
            contract_value,
            contract_asset,
            trade_id,
            price,
            quantity,
            timestamp,
        ) in cases
        {
            // ================================================
            // COMPILE EXISTING SHARED SEMANTICS
            // ================================================

            let format = formats.trade(profile).unwrap();

            let websocket_plan = TradeNormalizationCompiler::compile_websocket(
                format,
                1000,
                QuantityPrecisionPolicy::Round { decimal_places: 18 },
            )
            .unwrap();

            // ================================================
            // BINANCE REST AGGRESSOR MAPPING
            // ================================================

            let side = AggressorSideNormalizer::mapped(BTreeMap::from([
                (NativeSideValue::Boolean(true), TradeSide::Sell),
                (NativeSideValue::Boolean(false), TradeSide::Buy),
            ]))
            .unwrap();

            // ================================================
            // REST FIELD LAYOUT
            // ================================================

            let layout = NativeTradeFieldLayout {
                trade_id: Some("id".into()),
                price: "price".into(),
                quantity: "qty".into(),
                timestamp: "time".into(),
                side: Some("isBuyerMaker".into()),
                sequence: None,
            };

            let rest_plan = RestTradeNormalizationCompiler::compile_individual(
                &websocket_plan,
                &endpoint(profile, "recent_trades"),
                layout,
                TimestampUnit::Milliseconds,
                side,
                None,
                1000,
            )
            .unwrap();

            let decoder = TradeDecoder::new(rest_plan).unwrap();

            // ================================================
            // INSTRUMENT METADATA
            // ================================================

            let quote_asset = match market {
                MarketType::Linear => "USDT",
                MarketType::Inverse => "USD",
                MarketType::Spot => unreachable!(),
            };

            let settlement_asset = match market {
                MarketType::Linear => "USDT",
                MarketType::Inverse => "BTC",
                MarketType::Spot => unreachable!(),
            };

            let request = CreateSourceStreamRequest {
                instrument_id,
                exchange: ExchangeId::Binance,
                symbol: symbol.into(),
                market_category: market,
                instrument_type: "perpetual".into(),

                base_asset: Some("BTC".into()),
                quote_asset: Some(quote_asset.into()),
                settlement_asset: Some(settlement_asset.into()),

                quantity_type,

                contract_value: contract_value.map(str::to_owned),
                contract_value_asset: contract_asset.map(str::to_owned),

                channel: SourceChannel::Trades,
            };

            let context = NormalizationContext::new(&request, format!("rest-{profile}")).unwrap();

            // ================================================
            // NATIVE REST RESPONSE
            // ================================================

            let response = json!([
                {
                    "id": trade_id.parse::<u64>().unwrap(),
                    "price": price,
                    "qty": quantity,
                    "time": timestamp,
                    "isBuyerMaker": true
                }
            ]);

            // ================================================
            // CANONICAL NORMALIZATION
            // ================================================

            let batch =
                RestTradeDecoder::decode_validated_response(&decoder, &response, &context).unwrap();

            assert_eq!(batch.source(), NormalizationSource::Rest);
            assert_eq!(batch.record_count(), 1);

            let trade = &batch.trades()[0];

            assert_eq!(trade.trade_id.as_deref(), Some(trade_id));

            assert_eq!(trade.envelope.instrument_id, instrument_id);
            assert_eq!(trade.envelope.symbol, symbol);

            // Buyer was maker => seller was aggressor.
            assert_eq!(trade.side, TradeSide::Sell);

            assert_eq!(trade.price, Decimal::from_str_exact(price).unwrap(),);

            assert_eq!(trade.envelope.event_timestamp_ns, timestamp * 1_000_000,);

            let native_quantity = Decimal::from_str_exact(quantity).unwrap();

            // ================================================
            // ECONOMIC QUANTITY VALIDATION
            // ================================================

            match market {
                MarketType::Linear => {
                    // Linear native quantity is BTC.
                    assert_eq!(trade.quantity_base, Some(native_quantity),);

                    let expected_quote = native_quantity.checked_mul(trade.price).unwrap();

                    assert_eq!(trade.quantity_quote, Some(expected_quote),);

                    assert_eq!(trade.quantity_contracts, None);
                }

                MarketType::Inverse => {
                    // Native quantity is a contract count.
                    assert_eq!(trade.quantity_contracts, Some(native_quantity),);

                    // One contract is worth 100 USD.
                    let expected_quote = native_quantity.checked_mul(Decimal::from(100)).unwrap();

                    assert_eq!(trade.quantity_quote, Some(expected_quote),);

                    // BTC quantity is derived from USD value / price.
                    let base = trade.quantity_base.unwrap();

                    assert!(base > Decimal::ZERO);

                    let reconstructed_quote = base.checked_mul(trade.price).unwrap();

                    let error = (reconstructed_quote - expected_quote).abs();

                    assert!(
                        error <= Decimal::from_str_exact("0.00000001").unwrap(),
                        "{profile}: inverse conversion error: {error}",
                    );
                }

                MarketType::Spot => unreachable!(),
            }
        }
    }
    #[test]
    fn compiles_binance_spot_rest_trade_from_config() {
        use crate::live::config::format::FormatConfigStore;

        use crate::live::infrastructure::normalization::quantity::QuantityPrecisionPolicy;
        use crate::live::infrastructure::normalization::trades::compiler::TradeNormalizationCompiler;

        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live/formats");

        let formats = FormatConfigStore::load_dir(root).unwrap();

        let format = formats.trade("binance_spot").unwrap();

        let websocket_plan = TradeNormalizationCompiler::compile_websocket(
            format,
            1000,
            QuantityPrecisionPolicy::Exact,
        )
        .unwrap();

        let endpoint = endpoint("binance_spot", "recent_trades");

        let rest_plan =
            RestTradeNormalizationCompiler::compile_from_config(&websocket_plan, &endpoint, 1000)
                .unwrap();

        // REST records are at the JSON root.
        assert_eq!(rest_plan.extraction.path(), "$");

        // Field mappings originate from TOML.
        let fields = rest_plan.fields.layout();

        assert_eq!(fields.trade_id.as_deref(), Some("id"));
        assert_eq!(fields.price, "price");
        assert_eq!(fields.quantity, "qty");
        assert_eq!(fields.timestamp, "time");
        assert_eq!(fields.side.as_deref(), Some("isBuyerMaker"));
        assert_eq!(fields.sequence, None);

        assert_eq!(
            rest_plan.fields.timestamp_unit(),
            TimestampUnit::Milliseconds,
        );

        // The REST plan preserves shared economic semantics.
        assert_eq!(
            rest_plan.declared_quantity_type,
            websocket_plan.declared_quantity_type,
        );

        assert_eq!(
            rest_plan.expected_exchange,
            websocket_plan.expected_exchange,
        );

        assert_eq!(rest_plan.expected_market, websocket_plan.expected_market,);

        // REST records must not inherit WebSocket-only
        // native instrument discriminator requirements.
        assert!(rest_plan.symbol_type.is_none());
    }
    #[test]
    fn rejects_rest_trade_endpoint_without_mapping() {
        use crate::live::config::format::FormatConfigStore;

        use crate::live::infrastructure::normalization::quantity::QuantityPrecisionPolicy;
        use crate::live::infrastructure::normalization::trades::compiler::TradeNormalizationCompiler;

        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live/formats");

        let formats = FormatConfigStore::load_dir(root).unwrap();

        let format = formats.trade("binance_spot").unwrap();

        let websocket_plan = TradeNormalizationCompiler::compile_websocket(
            format,
            1000,
            QuantityPrecisionPolicy::Exact,
        )
        .unwrap();

        let mut endpoint = endpoint("binance_spot", "recent_trades");

        // Simulate missing production configuration.
        endpoint.trade_normalization = None;

        let result =
            RestTradeNormalizationCompiler::compile_from_config(&websocket_plan, &endpoint, 1000);

        assert!(result.is_err());
    }
    #[test]
    fn normalizes_binance_spot_rest_from_config_end_to_end() {
        use crate::canonical::TradeSide;

        use crate::live::config::{
            exchange::{ExchangeId, MarketType},
            format::FormatConfigStore,
        };

        use crate::live::infrastructure::normalization::context::NormalizationContext;
        use crate::live::infrastructure::normalization::message::NormalizationSource;
        use crate::live::infrastructure::normalization::quantity::QuantityPrecisionPolicy;
        use crate::live::infrastructure::normalization::trades::compiler::TradeNormalizationCompiler;
        use crate::live::infrastructure::normalization::trades::decoder::TradeDecoder;
        use crate::live::infrastructure::normalization::trades::rest::decoder::RestTradeDecoder;

        use crate::live::stream::request::{
            CreateSourceStreamRequest, QuantityType, SourceChannel,
        };

        use rust_decimal::Decimal;
        use serde_json::json;

        // --------------------------------------------------------
        // 1. Load production configuration
        // --------------------------------------------------------

        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live/formats");

        let formats = FormatConfigStore::load_dir(root).unwrap();

        let format = formats.trade("binance_spot").unwrap();

        let websocket_plan = TradeNormalizationCompiler::compile_websocket(
            format,
            1000,
            QuantityPrecisionPolicy::Exact,
        )
        .unwrap();

        let rest_endpoint = endpoint("binance_spot", "recent_trades");

        // --------------------------------------------------------
        // 2. Compile REST normalization exclusively from config
        // --------------------------------------------------------

        let rest_plan = RestTradeNormalizationCompiler::compile_from_config(
            &websocket_plan,
            &rest_endpoint,
            1000,
        )
        .unwrap();

        let decoder = TradeDecoder::new(rest_plan).unwrap();

        // --------------------------------------------------------
        // 3. Build authoritative instrument context
        // --------------------------------------------------------

        let request = CreateSourceStreamRequest {
            instrument_id: 10001,
            exchange: ExchangeId::Binance,
            symbol: "BTCUSDT".into(),
            market_category: MarketType::Spot,
            instrument_type: "spot".into(),

            base_asset: Some("BTC".into()),
            quote_asset: Some("USDT".into()),
            settlement_asset: None,

            quantity_type: QuantityType::Base,
            contract_value: None,
            contract_value_asset: None,

            channel: SourceChannel::Trades,
        };

        let context = NormalizationContext::new(&request, "rest-binance-spot-config").unwrap();

        // --------------------------------------------------------
        // 4. Native Binance REST response
        // --------------------------------------------------------

        let response = json!([
            {
                "id": 100001,
                "price": "80800.50",
                "qty": "0.125",
                "time": 1792806200000_i64,
                "isBuyerMaker": true
            },
            {
                "id": 100002,
                "price": "80801.25",
                "qty": "0.250",
                "time": 1792806200001_i64,
                "isBuyerMaker": false
            }
        ]);

        // --------------------------------------------------------
        // 5. Decode complete REST response
        // --------------------------------------------------------

        let batch =
            RestTradeDecoder::decode_validated_response(&decoder, &response, &context).unwrap();

        assert_eq!(batch.source(), NormalizationSource::Rest);
        assert_eq!(batch.record_count(), 2);

        let trades = batch.trades();

        // --------------------------------------------------------
        // 6. First canonical trade
        // --------------------------------------------------------

        let first = &trades[0];

        assert_eq!(first.trade_id.as_deref(), Some("100001"),);

        assert_eq!(first.side, TradeSide::Sell);

        assert_eq!(first.price, Decimal::from_str_exact("80800.50").unwrap(),);

        assert_eq!(
            first.quantity_base,
            Some(Decimal::from_str_exact("0.125").unwrap()),
        );

        assert_eq!(
            first.quantity_quote,
            Some(Decimal::from_str_exact("10100.06250").unwrap()),
        );

        assert_eq!(first.envelope.event_timestamp_ns, 1_792_806_200_000_000_000,);

        assert_eq!(first.envelope.instrument_id, 10001);
        assert_eq!(first.envelope.symbol, "BTCUSDT");

        // --------------------------------------------------------
        // 7. Second canonical trade
        // --------------------------------------------------------

        let second = &trades[1];

        assert_eq!(second.trade_id.as_deref(), Some("100002"),);

        assert_eq!(second.side, TradeSide::Buy);

        assert_eq!(second.price, Decimal::from_str_exact("80801.25").unwrap(),);

        assert_eq!(
            second.quantity_base,
            Some(Decimal::from_str_exact("0.250").unwrap()),
        );

        assert_eq!(
            second.quantity_quote,
            Some(Decimal::from_str_exact("20200.31250").unwrap()),
        );

        assert_eq!(
            second.envelope.event_timestamp_ns,
            1_792_806_200_001_000_000,
        );

        // --------------------------------------------------------
        // 8. Atomic batch rejection
        // --------------------------------------------------------

        let mut invalid_response = response.clone();

        invalid_response[1]["price"] = json!("-1");

        assert!(
            RestTradeDecoder::decode_validated_response(&decoder, &invalid_response, &context,)
                .is_err(),
            "invalid second trade must reject the entire batch",
        );
    }
    #[test]
    fn compiles_all_15_rest_trade_profiles_from_config() {
        use crate::live::config::format::FormatConfigStore;

        use crate::live::infrastructure::normalization::quantity::QuantityPrecisionPolicy;
        use crate::live::infrastructure::normalization::trades::compiler::TradeNormalizationCompiler;
        use crate::live::infrastructure::normalization::trades::decoder::TradeDecoder;

        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live/formats");

        let formats = FormatConfigStore::load_dir(root).unwrap();

        let profiles = [
            ("binance_spot", "recent_trades", "qty", "time"),
            ("binance_linear", "recent_trades", "qty", "time"),
            ("binance_inverse", "recent_trades", "qty", "time"),
            ("bybit_spot", "recent_trades", "size", "time"),
            ("bybit_linear", "recent_trades", "size", "time"),
            ("bybit_inverse", "recent_trades", "size", "time"),
            ("bitget_spot", "recent_trades", "size", "ts"),
            ("bitget_linear", "recent_trades", "size", "ts"),
            ("bitget_inverse", "recent_trades", "size", "ts"),
            ("okx_spot", "recent_trades", "sz", "ts"),
            ("okx_linear", "recent_trades", "sz", "ts"),
            ("okx_inverse", "recent_trades", "sz", "ts"),
            (
                "gateio_spot",
                "historical_trades",
                "amount",
                "create_time_ms",
            ),
            (
                "gateio_linear",
                "historical_trades",
                "size",
                "create_time_ms",
            ),
            (
                "gateio_inverse",
                "historical_trades",
                "size",
                "create_time_ms",
            ),
        ];

        for (profile, endpoint_name, quantity_field, timestamp_field) in profiles {
            let format = formats
                .trade(profile)
                .unwrap_or_else(|| panic!("Missing format: {profile}"));

            let websocket_plan = TradeNormalizationCompiler::compile_websocket(
                format,
                1000,
                QuantityPrecisionPolicy::Round { decimal_places: 18 },
            )
            .unwrap_or_else(|error| panic!("{profile}: WebSocket compilation failed: {error}"));

            let rest_endpoint = endpoint(profile, endpoint_name);

            assert!(
                rest_endpoint.trade_normalization.is_some(),
                "{profile}: missing REST trade configuration",
            );

            let plan = RestTradeNormalizationCompiler::compile_from_config(
                &websocket_plan,
                &rest_endpoint,
                1000,
            )
            .unwrap_or_else(|error| panic!("{profile}: REST compilation failed: {error}"));

            let layout = plan.fields.layout();

            assert_eq!(
                layout.quantity, quantity_field,
                "{profile}: incorrect quantity field",
            );

            assert_eq!(
                layout.timestamp, timestamp_field,
                "{profile}: incorrect timestamp field",
            );

            assert_eq!(
                plan.declared_quantity_type, websocket_plan.declared_quantity_type,
                "{profile}: quantity semantics changed",
            );

            assert_eq!(
                plan.expected_exchange, websocket_plan.expected_exchange,
                "{profile}: exchange identity changed",
            );

            assert_eq!(
                plan.expected_market, websocket_plan.expected_market,
                "{profile}: market identity changed",
            );

            assert!(
                plan.symbol_type.is_none(),
                "{profile}: inherited WebSocket-only symbol discriminator",
            );

            TradeDecoder::new(plan)
                .unwrap_or_else(|error| panic!("{profile}: decoder construction failed: {error}"));
        }
    }
    #[test]
    fn normalizes_all_15_captured_rest_trade_fixtures() {
        use std::time::Duration;
        use std::{fs, path::PathBuf};

        use crate::live::config::{
            exchange::{ExchangeId, MarketType},
            format::FormatConfigStore,
        };
        use crate::live::infrastructure::rest::RestRequest;
        use crate::live::infrastructure::rest::{ExchangeRestResponse, RestResponseHeaders};

        use crate::live::infrastructure::normalization::context::NormalizationContext;
        use crate::live::infrastructure::normalization::message::NormalizationSource;
        use crate::live::infrastructure::normalization::quantity::QuantityPrecisionPolicy;
        use crate::live::infrastructure::normalization::trades::compiler::TradeNormalizationCompiler;
        use crate::live::infrastructure::normalization::trades::decoder::TradeDecoder;
        use crate::live::infrastructure::normalization::trades::rest::decoder::RestTradeDecoder;

        use crate::live::stream::request::{
            CreateSourceStreamRequest, QuantityType, SourceChannel,
        };

        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data");

        let formats = FormatConfigStore::load_dir(root.join(".config/live/formats")).unwrap();

        // profile, expected records, instrument ID,
        // symbol, quote asset, quantity type,
        // contract value, contract denomination
        let cases = [
            (
                "binance_spot",
                500,
                10001,
                "BTCUSDT",
                "USDT",
                QuantityType::Base,
                None,
                None,
            ),
            (
                "binance_linear",
                500,
                9872,
                "BTCUSDT",
                "USDT",
                QuantityType::Base,
                None,
                None,
            ),
            (
                "binance_inverse",
                500,
                10792,
                "BTCUSD_PERP",
                "USD",
                QuantityType::Contracts,
                Some("100"),
                Some("USD"),
            ),
            (
                "bybit_spot",
                60,
                688,
                "BTCUSDT",
                "USDT",
                QuantityType::Base,
                None,
                None,
            ),
            (
                "bybit_linear",
                500,
                688,
                "BTCUSDT",
                "USDT",
                QuantityType::Base,
                None,
                None,
            ),
            (
                "bybit_inverse",
                500,
                1427,
                "BTCUSD",
                "USD",
                QuantityType::Quote,
                None,
                None,
            ),
            (
                "bitget_spot",
                100,
                11545,
                "BTCUSDT",
                "USDT",
                QuantityType::Base,
                None,
                None,
            ),
            (
                "bitget_linear",
                100,
                14215,
                "BTCUSDT",
                "USDT",
                QuantityType::Base,
                None,
                None,
            ),
            (
                "bitget_inverse",
                100,
                900000001,
                "BTCUSD_CM",
                "USD",
                QuantityType::Quote,
                None,
                None,
            ),
            (
                "okx_spot",
                100,
                3336,
                "BTC-USDT",
                "USDT",
                QuantityType::Base,
                None,
                None,
            ),
            (
                "okx_linear",
                100,
                4063,
                "BTC-USDT-SWAP",
                "USDT",
                QuantityType::Contracts,
                Some("0.01"),
                Some("BTC"),
            ),
            (
                "okx_inverse",
                100,
                4048,
                "BTC-USD-SWAP",
                "USD",
                QuantityType::Contracts,
                Some("100"),
                Some("USD"),
            ),
            (
                "gateio_spot",
                100,
                15850,
                "BTC_USDT",
                "USDT",
                QuantityType::Base,
                None,
                None,
            ),
            (
                "gateio_linear",
                100,
                17397,
                "BTC_USDT",
                "USDT",
                QuantityType::Contracts,
                Some("0.0001"),
                Some("BTC"),
            ),
            (
                "gateio_inverse",
                100,
                18256,
                "BTC_USD",
                "USD",
                QuantityType::Contracts,
                Some("1"),
                Some("USD"),
            ),
        ];

        let mut total_records = 0usize;

        for (
            profile,
            expected_count,
            instrument_id,
            symbol,
            quote_asset,
            quantity_type,
            contract_value,
            contract_asset,
        ) in cases
        {
            let exchange = match profile.split('_').next().unwrap() {
                "binance" => ExchangeId::Binance,
                "bybit" => ExchangeId::Bybit,
                "bitget" => ExchangeId::Bitget,
                "okx" => ExchangeId::Okx,
                "gateio" => ExchangeId::Gateio,
                _ => unreachable!(),
            };

            let market = match profile.split('_').nth(1).unwrap() {
                "spot" => MarketType::Spot,
                "linear" => MarketType::Linear,
                "inverse" => MarketType::Inverse,
                _ => unreachable!(),
            };

            let endpoint_name = if profile.starts_with("gateio_") {
                "historical_trades"
            } else {
                "recent_trades"
            };

            // 1. Load real production trade format.
            let format = formats
                .trade(profile)
                .unwrap_or_else(|| panic!("{profile}: missing format"));

            let websocket_plan = TradeNormalizationCompiler::compile_websocket(
                format,
                1000,
                QuantityPrecisionPolicy::Round { decimal_places: 18 },
            )
            .unwrap_or_else(|e| panic!("{profile}: WS plan compilation failed: {e}"));

            // 2. Compile REST fields entirely from production TOML.
            let rest_endpoint = endpoint(profile, endpoint_name);
            let symbol_parameter = &rest_endpoint
                .trade_normalization
                .as_ref()
                .unwrap()
                .request_symbol_parameter;

            let logical_request =
                RestRequest::new(profile, endpoint_name).with_parameter(symbol_parameter, symbol);
            let rest_plan = RestTradeNormalizationCompiler::compile_from_config(
                &websocket_plan,
                &rest_endpoint,
                1000,
            )
            .unwrap_or_else(|e| panic!("{profile}: REST compilation failed: {e}"));

            let decoder = TradeDecoder::new(rest_plan).unwrap();

            // 3. Construct authoritative instrument metadata.
            let settlement_asset = match market {
                MarketType::Spot => None,
                MarketType::Linear => Some("USDT".to_string()),
                MarketType::Inverse => Some("BTC".to_string()),
            };

            let request = CreateSourceStreamRequest {
                instrument_id,
                exchange,
                symbol: symbol.into(),
                market_category: market,

                instrument_type: if market == MarketType::Spot {
                    "spot"
                } else {
                    "perpetual"
                }
                .into(),

                base_asset: Some("BTC".into()),
                quote_asset: Some(quote_asset.into()),
                settlement_asset,

                quantity_type,
                contract_value: contract_value.map(str::to_owned),
                contract_value_asset: contract_asset.map(str::to_owned),

                channel: SourceChannel::Trades,
            };

            let context =
                NormalizationContext::new(&request, format!("fixture-{profile}")).unwrap();

            // 4. Load the full, unmodified exchange response.
            let path = root.join(format!(
                ".cache/live/rest-fixtures/{profile}__{endpoint_name}.json"
            ));

            let raw = fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("{profile}: cannot read {}: {e}", path.display()));

            // Parse directly from original JSON text.
            // This is essential for Gate.io decimal timestamps.
            let response: serde_json::Value = serde_json::from_str(&raw)
                .unwrap_or_else(|e| panic!("{profile}: JSON parsing failed: {e}"));

            let exchange_response = ExchangeRestResponse::new(
                profile,
                endpoint_name,
                200,
                response.clone(),
                RestResponseHeaders::new(),
                Duration::from_millis(10),
                raw.len(),
            );

            // 5. Normalize the entire captured response atomically.
            let batch = RestTradeDecoder::decode_bound_exchange_response(
                &decoder,
                &exchange_response,
                &logical_request,
                &rest_endpoint,
                &context,
            )
            .unwrap_or_else(|error| panic!("{profile}: bound REST normalization failed: {error}"));

            assert_eq!(
                batch.source(),
                NormalizationSource::Rest,
                "{profile}: incorrect normalization source",
            );

            assert_eq!(
                batch.record_count(),
                expected_count,
                "{profile}: unexpected normalized record count",
            );

            assert_eq!(
                batch.trades().len(),
                expected_count,
                "{profile}: trades/count mismatch",
            );
            // --------------------------------------------------------
            // WRONG PROFILE MUST BE REJECTED
            // --------------------------------------------------------

            let wrong_profile = ExchangeRestResponse::new(
                "wrong_exchange_profile",
                endpoint_name,
                200,
                response.clone(),
                RestResponseHeaders::new(),
                Duration::from_millis(10),
                raw.len(),
            );

            assert!(
                RestTradeDecoder::decode_exchange_response(
                    &decoder,
                    &wrong_profile,
                    profile,
                    endpoint_name,
                    &rest_endpoint,
                    &context,
                )
                .is_err(),
                "{profile}: foreign response profile was accepted",
            );

            // --------------------------------------------------------
            // WRONG ENDPOINT MUST BE REJECTED
            // --------------------------------------------------------

            let wrong_endpoint = ExchangeRestResponse::new(
                profile,
                "depth_snapshot",
                200,
                response.clone(),
                RestResponseHeaders::new(),
                Duration::from_millis(10),
                raw.len(),
            );

            assert!(
                RestTradeDecoder::decode_exchange_response(
                    &decoder,
                    &wrong_endpoint,
                    profile,
                    endpoint_name,
                    &rest_endpoint,
                    &context,
                )
                .is_err(),
                "{profile}: foreign REST endpoint was accepted",
            );

            // --------------------------------------------------------
            // UNSUCCESSFUL HTTP STATUS MUST BE REJECTED
            // --------------------------------------------------------

            let http_error = ExchangeRestResponse::new(
                profile,
                endpoint_name,
                500,
                response.clone(),
                RestResponseHeaders::new(),
                Duration::from_millis(10),
                raw.len(),
            );

            assert!(
                RestTradeDecoder::decode_exchange_response(
                    &decoder,
                    &http_error,
                    profile,
                    endpoint_name,
                    &rest_endpoint,
                    &context,
                )
                .is_err(),
                "{profile}: HTTP failure was accepted",
            );

            // --------------------------------------------------------
            // APPLICATION FAILURE MUST BE REJECTED
            // --------------------------------------------------------

            if let Some(application) = &rest_endpoint.application_response {
                if application.strategy
                    == crate::live::config::exchange::RestApplicationResponseStrategy::Envelope
                {
                    let mut rejected_body = response.clone();

                    let field = application.code_field.as_ref().unwrap();

                    rejected_body[field] = serde_json::json!("MARKETFORGE_TEST_FAILURE");

                    let rejected_response = ExchangeRestResponse::new(
                        profile,
                        endpoint_name,
                        200,
                        rejected_body,
                        RestResponseHeaders::new(),
                        Duration::from_millis(10),
                        raw.len(),
                    );

                    assert!(
                        RestTradeDecoder::decode_exchange_response(
                            &decoder,
                            &rejected_response,
                            profile,
                            endpoint_name,
                            &rest_endpoint,
                            &context,
                        )
                        .is_err(),
                        "{profile}: application-level failure was accepted",
                    );
                }
            }
            // 6. Every record must retain its instrument identity
            // and produce valid canonical economic quantities.
            for (index, trade) in batch.trades().iter().enumerate() {
                assert_eq!(
                    trade.envelope.instrument_id, instrument_id,
                    "{profile}: wrong instrument at record {index}",
                );

                assert_eq!(
                    trade.envelope.symbol, symbol,
                    "{profile}: wrong symbol at record {index}",
                );

                assert!(
                    trade.price > rust_decimal::Decimal::ZERO,
                    "{profile}: nonpositive price at record {index}",
                );

                assert!(
                    trade.envelope.event_timestamp_ns > 0,
                    "{profile}: invalid timestamp at record {index}",
                );

                assert!(
                    trade.trade_id.is_some(),
                    "{profile}: missing trade ID at record {index}",
                );

                assert!(
                    trade.quantity_base.is_some(),
                    "{profile}: missing base quantity at record {index}",
                );

                assert!(
                    trade.quantity_quote.is_some(),
                    "{profile}: missing quote quantity at record {index}",
                );
                // A request for a different native instrument must fail,
                // even when the response contains valid trade records.
                let foreign_request = RestRequest::new(profile, endpoint_name)
                    .with_parameter(symbol_parameter, "ETH_DIFFERENT");

                assert!(
                    RestTradeDecoder::decode_bound_exchange_response(
                        &decoder,
                        &exchange_response,
                        &foreign_request,
                        &rest_endpoint,
                        &context,
                    )
                    .is_err(),
                    "{profile}: foreign requested instrument was accepted",
                );

                // Missing request instrument identity must fail.
                let missing_symbol_request = RestRequest::new(profile, endpoint_name);

                assert!(
                    RestTradeDecoder::decode_bound_exchange_response(
                        &decoder,
                        &exchange_response,
                        &missing_symbol_request,
                        &rest_endpoint,
                        &context,
                    )
                    .is_err(),
                    "{profile}: missing request instrument was accepted",
                );

                // Wrong logical endpoint must fail.
                let foreign_endpoint_request = RestRequest::new(profile, "depth_snapshot")
                    .with_parameter(symbol_parameter, symbol);

                assert!(
                    RestTradeDecoder::decode_bound_exchange_response(
                        &decoder,
                        &exchange_response,
                        &foreign_endpoint_request,
                        &rest_endpoint,
                        &context,
                    )
                    .is_err(),
                    "{profile}: foreign logical endpoint was accepted",
                );
                match market {
                    MarketType::Spot => {
                        assert!(
                            trade.quantity_contracts.is_none(),
                            "{profile}: spot trade has contract quantity",
                        );
                    }

                    MarketType::Linear | MarketType::Inverse => {
                        if quantity_type == QuantityType::Contracts {
                            assert!(
                                trade.quantity_contracts.is_some(),
                                "{profile}: missing contract quantity",
                            );
                        }
                    }
                }
            }

            total_records += batch.record_count();

            println!("PASS {profile:20} records={}", batch.record_count());
        }

        assert_eq!(total_records, 3460);

        println!("PASS all 15 profiles: {total_records} captured REST trades normalized");
    }
}
