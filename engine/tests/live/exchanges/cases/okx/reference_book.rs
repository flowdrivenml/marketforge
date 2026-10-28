use std::{collections::BTreeMap, path::PathBuf};

use rust_decimal::Decimal;
use serde_json::Value;

use marketforge_engine::{
    live::{
        config::{
            exchange::{ExchangeId, MarketType},
            format::DepthFormatConfig,
        },
        infrastructure::normalization::{
            DepthDecoder, NormalizationContext, QuantityPrecisionPolicy,
        },
        integrity::{
            BoundDepthIngress, DepthBufferLimits, DepthContinuityCompiler, DepthControllerOutcome,
            DepthResourceLimits, DepthSourceBinding, DepthSourceEpoch,
        },
        stream::request::{CreateSourceStreamRequest, QuantityType, SourceChannel},
    },
    shared::domain::canonical::Exchange,
};

const STREAM_ID: &str = "integration:okx:depth";
const INSTRUMENT_ID: i64 = 42;
const DEPTH: u64 = 400;

// ============================================================
// PROFILE CONFIGURATION
// ============================================================

fn data_root() -> PathBuf {
    crate::support::data("")
}

fn format(profile: &str) -> DepthFormatConfig {
    DepthFormatConfig::load(
        data_root()
            .join(".config/live/formats/depth")
            .join(format!("{profile}.toml")),
    )
    .unwrap()
}

fn fixtures(profile: &str) -> Vec<Value> {
    serde_json::from_slice(
        &crate::fixtures::read(
            data_root()
                .join(".cache/live/websocket-fixtures/depth")
                .join(format!("{profile}.json")),
        )
        .unwrap(),
    )
    .unwrap()
}

fn symbol(profile: &str) -> &'static str {
    match profile {
        "okx_spot" => "BTC-USDT",
        "okx_linear" => "BTC-USDT-SWAP",
        "okx_inverse" => "BTC-USD-SWAP",
        _ => panic!("unsupported OKX profile: {profile}"),
    }
}

fn market(profile: &str) -> MarketType {
    match profile {
        "okx_spot" => MarketType::Spot,
        "okx_linear" => MarketType::Linear,
        "okx_inverse" => MarketType::Inverse,
        _ => panic!("unsupported OKX profile"),
    }
}

fn epoch() -> DepthSourceEpoch {
    DepthSourceEpoch {
        stream_generation: 10,
        connection_generation: 20,
        subscription_generation: 30,
    }
}

// ============================================================
// NORMALIZATION CONTEXT
// ============================================================

fn context(profile: &str) -> NormalizationContext {
    let market = market(profile);
    let inverse = market == MarketType::Inverse;
    let contracts = market != MarketType::Spot;

    // Contract values are illustrative test metadata.
    // Production must use the instrument catalog.
    let request = CreateSourceStreamRequest {
        instrument_id: INSTRUMENT_ID,
        exchange: ExchangeId::Okx,
        symbol: symbol(profile).into(),
        market_category: market.clone(),

        instrument_type: if contracts {
            "perpetual".into()
        } else {
            "spot".into()
        },

        base_asset: Some("BTC".into()),
        quote_asset: Some(if inverse { "USD" } else { "USDT" }.into()),

        quantity_type: if contracts {
            QuantityType::Contracts
        } else {
            QuantityType::Base
        },

        contract_value: if contracts { Some("1".into()) } else { None },

        contract_value_asset: if inverse {
            Some("USD".into())
        } else if contracts {
            Some("BTC".into())
        } else {
            None
        },

        channel: SourceChannel::Depth,

        settlement_asset: Some(if inverse { "BTC" } else { "USDT" }.into()),
    };

    NormalizationContext::new(&request, STREAM_ID).unwrap()
}

fn decoder(profile: &str) -> DepthDecoder {
    DepthDecoder::new_websocket(
        &format(profile),
        20_000,
        QuantityPrecisionPolicy::Round { decimal_places: 18 },
        Some(DEPTH),
    )
    .unwrap()
}

fn ingress(profile: &str) -> BoundDepthIngress {
    let binding = DepthSourceBinding::new(
        STREAM_ID,
        Exchange::Okx,
        market(profile).as_str(),
        INSTRUMENT_ID,
        symbol(profile),
        Some(DEPTH),
        epoch(),
    )
    .unwrap();

    let plan = DepthContinuityCompiler::compile(&format(profile)).unwrap();

    BoundDepthIngress::new(
        binding,
        plan,
        DepthBufferLimits {
            max_messages: 100,
            max_bytes: 20_000_000,
        },
        DepthResourceLimits::default(),
    )
    .unwrap()
}

// ============================================================
// INDEPENDENT NATIVE BOOK
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
struct NativeLevel {
    quantity: Decimal,
    order_count: u64,
}

#[derive(Debug, Default)]
struct ReferenceBook {
    bids: BTreeMap<Decimal, NativeLevel>,
    asks: BTreeMap<Decimal, NativeLevel>,
}

impl ReferenceBook {
    fn apply(&mut self, message: &Value) {
        let action = message["action"].as_str().expect("OKX action");

        match action {
            "snapshot" => {
                self.bids.clear();
                self.asks.clear();
            }

            "update" => {}

            other => panic!("unexpected OKX action: {other}"),
        }

        let records = message["data"].as_array().expect("OKX data array");

        assert_eq!(records.len(), 1, "one native book record expected");

        let record = &records[0];

        Self::apply_side(&mut self.bids, &record["bids"]);

        Self::apply_side(&mut self.asks, &record["asks"]);
    }

    fn apply_side(book: &mut BTreeMap<Decimal, NativeLevel>, side: &Value) {
        for level in side.as_array().expect("OKX levels") {
            let items = level.as_array().expect("OKX level");

            assert_eq!(items.len(), 4);

            let price = Decimal::from_str_exact(items[0].as_str().unwrap()).unwrap();

            let quantity = Decimal::from_str_exact(items[1].as_str().unwrap()).unwrap();

            let order_count = items[3].as_str().unwrap().parse::<u64>().unwrap();

            assert!(price > Decimal::ZERO);
            assert!(quantity >= Decimal::ZERO);

            if quantity == Decimal::ZERO {
                book.remove(&price);
            } else {
                book.insert(
                    price,
                    NativeLevel {
                        quantity,
                        order_count,
                    },
                );
            }
        }
    }
}

// ============================================================
// VERIFY AUTHORITATIVE BOOK
// ============================================================

fn assert_side_matches(
    expected: &BTreeMap<Decimal, NativeLevel>,
    actual: &BTreeMap<Decimal, marketforge_engine::shared::domain::book::BookLevel>,
    contracts: bool,
    label: &str,
) {
    assert_eq!(actual.len(), expected.len(), "{label}: level count differs");

    for (price, native) in expected {
        let level = actual
            .get(price)
            .unwrap_or_else(|| panic!("{label}: missing price {price}"));

        let actual_quantity = if contracts {
            level.quantity_contracts
        } else {
            level.quantity_base
        };

        assert_eq!(
            actual_quantity,
            Some(native.quantity),
            "{label}: incorrect quantity at {price}"
        );

        assert_eq!(
            level.order_count,
            Some(native.order_count),
            "{label}: incorrect order count at {price}"
        );
    }
}

fn assert_books_match(ingress: &BoundDepthIngress, reference: &ReferenceBook, profile: &str) {
    assert!(ingress.is_synchronized());

    let actual = ingress.controller().book();

    let contracts = profile != "okx_spot";

    assert_side_matches(&reference.bids, actual.bids(), contracts, "bids");

    assert_side_matches(&reference.asks, actual.asks(), contracts, "asks");
}

// ============================================================
// REAL FIXTURE RECONSTRUCTION
// ============================================================

#[test]
fn okx_spot_linear_inverse_match_independent_reference() {
    for profile in ["okx_spot", "okx_linear", "okx_inverse"] {
        let messages = fixtures(profile);

        assert_eq!(messages.len(), 12);

        let decoder = decoder(profile);
        let context = context(profile);

        let mut ingress = ingress(profile);
        let mut reference = ReferenceBook::default();

        let mut accepted = 0;
        let mut ignored_keepalives = 0;

        for (index, native) in messages.iter().enumerate() {
            let normalized = decoder
                .decode_websocket(native, &context)
                .unwrap_or_else(|error| {
                    panic!(
                        "{profile} message {index}: \
                         normalization error: {error:?}"
                    )
                });

            let outcome = ingress.on_websocket(epoch(), normalized, 128 * 1024);

            match outcome {
                DepthControllerOutcome::Accepted { .. } => {
                    accepted += 1;

                    reference.apply(native);
                    assert_books_match(&ingress, &reference, profile);
                }

                DepthControllerOutcome::Ignored { reason } => {
                    // A legitimate keepalive is the only
                    // ignored message permitted by this test.
                    let record = &native["data"][0];

                    let bids_empty = record["bids"].as_array().is_some_and(Vec::is_empty);

                    let asks_empty = record["asks"].as_array().is_some_and(Vec::is_empty);

                    assert!(
                        bids_empty && asks_empty,
                        "{profile} message {index}: \
                         ignored nonempty update: {reason:?}"
                    );

                    ignored_keepalives += 1;

                    assert_books_match(&ingress, &reference, profile);
                }

                other => {
                    panic!(
                        "{profile} message {index}: \
                         unexpected Integrity outcome: {other:?}"
                    );
                }
            }
        }

        assert!(ingress.is_synchronized());

        println!(
            "{profile:<16} PASS \
             accepted={accepted} \
             keepalives={ignored_keepalives} \
             bids={} asks={}",
            reference.bids.len(),
            reference.asks.len(),
        );
    }
}
