use std::path::PathBuf;

use marketforge_engine::{
    canonical::Exchange,
    live::{
        config::format::{DepthFormatConfig, TradeFormatConfig},
        integrity::{
            BoundDepthIngress, BoundTradeIngress, DepthBufferLimits, DepthContinuityCompiler,
            DepthResourceLimits, DepthSourceBinding, DepthSourceEpoch, TradeIdentityLimits,
            TradeIntegrityCompiler, TradeSourceBinding, TradeSourceEpoch,
        },
    },
};

const MARKETS: [&str; 3] = ["spot", "linear", "inverse"];

const EXCHANGES: [(&str, Exchange); 5] = [
    ("binance", Exchange::Binance),
    ("bybit", Exchange::Bybit),
    ("bitget", Exchange::Bitget),
    ("gateio", Exchange::GateIo),
    ("okx", Exchange::Okx),
];

const GENERATION: u64 = 10;
const INSTRUMENT_ID: i64 = 42;

fn depth_path(profile: &str) -> PathBuf {
    crate::support::data(".config/live/formats/depth").join(format!("{profile}.toml"))
}

fn trade_path(profile: &str) -> PathBuf {
    crate::support::data(".config/live/formats/trades").join(format!("{profile}.toml"))
}

fn depth_epoch() -> DepthSourceEpoch {
    DepthSourceEpoch {
        stream_generation: GENERATION,
        connection_generation: 20,
        subscription_generation: 30,
    }
}

fn trade_epoch() -> TradeSourceEpoch {
    TradeSourceEpoch {
        stream_generation: GENERATION,
        connection_generation: 20,
        subscription_generation: 30,
    }
}

fn depth_limits() -> DepthBufferLimits {
    DepthBufferLimits {
        max_messages: 64,
        max_bytes: 4 * 1024 * 1024,
    }
}

fn trade_limits() -> TradeIdentityLimits {
    TradeIdentityLimits {
        max_ids: 10_000,
        max_id_bytes: 256,
        max_batch_records: 1_000,
    }
}

// ============================================================
// N5N.7.7g.2 — MULTI-EXCHANGE REST DEPTH LIFECYCLE
// ============================================================

use marketforge_engine::{
    canonical::{BookSide, EventEnvelope, L2LevelUpdate, SequenceMetadata},
    live::{
        config::exchange::ExchangeConfig,
        infrastructure::normalization::{DepthMessageKind, NormalizationSource, NormalizedDepth},
        integrity::{
            DepthContinuityFamily, DepthControllerOutcome, DepthPublicationEvent,
            DepthPublicationFence, DepthSnapshotSource, DepthSyncToken, RestDepthRequestContext,
        },
    },
};

use rust_decimal::Decimal;

const REST_CASES: [(&str, Exchange, &str); 5] = [
    ("binance_linear", Exchange::Binance, "linear"),
    ("binance_inverse", Exchange::Binance, "inverse"),
    ("gateio_spot", Exchange::GateIo, "spot"),
    ("gateio_linear", Exchange::GateIo, "linear"),
    ("gateio_inverse", Exchange::GateIo, "inverse"),
];

fn rest_exchange_config(profile: &str) -> ExchangeConfig {
    let path = crate::support::data(".config/live/exchanges").join(format!("{profile}.toml"));

    ExchangeConfig::load(path).unwrap_or_else(|error| {
        panic!("{profile}: exchange config failed: {error:?}");
    })
}

fn rest_token(attempt: u64) -> DepthSyncToken {
    DepthSyncToken {
        stream_generation: GENERATION,
        attempt,
    }
}

fn rest_ingress(profile: &str, exchange: Exchange, market: &str) -> BoundDepthIngress {
    let format = DepthFormatConfig::load(depth_path(profile)).unwrap();

    let plan = DepthContinuityCompiler::compile(&format).unwrap();

    assert_eq!(
        plan.snapshot_source,
        DepthSnapshotSource::Rest,
        "{profile}: expected REST-snapshot profile",
    );

    let stream = format!("integration:{profile}:depth");

    let binding = DepthSourceBinding::new(
        &stream,
        exchange,
        market,
        INSTRUMENT_ID,
        "BTCUSDT",
        None,
        depth_epoch(),
    )
    .unwrap();

    let mut ingress = BoundDepthIngress::new(
        binding,
        plan,
        depth_limits(),
        DepthResourceLimits::default(),
    )
    .unwrap();

    ingress
        .configure_rest_endpoint(&rest_exchange_config(profile), &format)
        .unwrap_or_else(|error| {
            panic!("{profile}: REST endpoint rejected: {error}");
        });

    ingress
}

fn begin_profile_rest(ingress: &mut BoundDepthIngress, profile: &str, attempt: u64) {
    let format = DepthFormatConfig::load(depth_path(profile)).unwrap();

    let context = RestDepthRequestContext {
        source_epoch: depth_epoch(),
        sync_token: rest_token(attempt),

        endpoint_key: format
            .synchronization
            .snapshot_endpoint
            .expect("REST profile needs snapshot endpoint"),

        requested_limit: 100,
    };

    ingress.begin_rest_attempt(context).unwrap_or_else(|error| {
        panic!("{profile}: REST attempt rejected: {error}");
    });
}

fn rest_level(
    stream: &str,
    exchange: Exchange,
    side: BookSide,
    price: i64,
    quantity: i64,
) -> L2LevelUpdate {
    L2LevelUpdate {
        envelope: EventEnvelope {
            event_timestamp_ns: 1_000_000_000,
            system_timestamp_ns: None,
            exchange,
            instrument_id: INSTRUMENT_ID,
            symbol: "BTCUSDT".into(),
            stream_id: stream.into(),
        },

        side,
        price: Decimal::from(price),
        quantity_base: Some(Decimal::from(quantity)),
        quantity_quote: None,
        quantity_contracts: None,
        order_count: None,
    }
}

fn profile_snapshot(stream: &str, exchange: Exchange, sequence: u64) -> NormalizedDepth {
    NormalizedDepth::new(
        stream,
        NormalizationSource::Rest,
        DepthMessageKind::Snapshot,
        SequenceMetadata {
            sequence_first: None,
            sequence_last: Some(sequence),
            sequence_previous: None,
            cross_sequence: None,
        },
        vec![
            rest_level(stream, exchange, BookSide::Bid, 80_000, 3),
            rest_level(stream, exchange, BookSide::Ask, 80_010, 2),
        ],
        Some(1_000_000_000),
        None,
    )
    .unwrap()
}

fn profile_delta(
    stream: &str,
    exchange: Exchange,
    family: DepthContinuityFamily,
    first: u64,
    last: u64,
    previous: Option<u64>,
    quantity: i64,
) -> NormalizedDepth {
    // Gate.io supplies a range but does not use the Binance
    // Futures previous-update-ID linkage.
    let previous = match family {
        DepthContinuityFamily::BinanceFuturesPrevious => previous,

        DepthContinuityFamily::BinanceSpotRange | DepthContinuityFamily::GateRangeCover => None,

        _ => panic!("unsupported REST lifecycle family"),
    };

    NormalizedDepth::new(
        stream,
        NormalizationSource::WebSocket,
        DepthMessageKind::Delta,
        SequenceMetadata {
            sequence_first: Some(first),
            sequence_last: Some(last),
            sequence_previous: previous,
            cross_sequence: None,
        },
        vec![rest_level(
            stream,
            exchange,
            BookSide::Bid,
            80_000,
            quantity,
        )],
        Some(1_000_000_000),
        None,
    )
    .unwrap()
}

fn invalid_depth_sequence(
    profile: &str,
    exchange: Exchange,
    family: DepthContinuityFamily,
    baseline: u64,
) -> NormalizedDepth {
    let stream = format!("integration:{profile}:depth");

    let sequence = match family {
        // Expected first update is baseline + 1.
        // Skip one update to create a genuine gap.
        DepthContinuityFamily::BinanceSpotRange => SequenceMetadata {
            sequence_first: Some(baseline + 2),
            sequence_last: Some(baseline + 5),
            sequence_previous: None,
            cross_sequence: None,
        },

        // Futures requires explicit previous-update linkage.
        DepthContinuityFamily::BinanceFuturesPrevious => SequenceMetadata {
            sequence_first: Some(baseline + 1),
            sequence_last: Some(baseline + 5),
            sequence_previous: Some(999),
            cross_sequence: None,
        },

        // Bybit update ID advances normally, but its
        // cross-sequence regresses from the snapshot's 200.
        DepthContinuityFamily::BybitV5 => SequenceMetadata {
            sequence_first: None,
            sequence_last: Some(baseline + 1),
            sequence_previous: None,
            cross_sequence: Some(100),
        },

        // Bitget's reset marker requires a new snapshot.
        DepthContinuityFamily::BitgetV3Previous => SequenceMetadata {
            sequence_first: None,
            sequence_last: Some(baseline + 5),
            sequence_previous: Some(0),
            cross_sequence: None,
        },

        // Gate range does not cover the next required update.
        DepthContinuityFamily::GateRangeCover => SequenceMetadata {
            sequence_first: Some(baseline + 2),
            sequence_last: Some(baseline + 5),
            sequence_previous: None,
            cross_sequence: None,
        },

        // OKX requires a linked previous sequence ID.
        DepthContinuityFamily::OkxV5Previous => SequenceMetadata {
            sequence_first: None,
            sequence_last: Some(baseline + 5),
            sequence_previous: Some(999),
            cross_sequence: None,
        },
    };

    let subscribed_depth = (family == DepthContinuityFamily::BybitV5).then_some(200);

    adversarial_depth_message(
        &stream,
        exchange,
        NormalizationSource::WebSocket,
        DepthMessageKind::Delta,
        sequence,
        999,
        subscribed_depth,
    )
}

#[test]
fn every_depth_profile_constructs_a_source_bound_ingress() {
    let mut checked = 0;

    for (exchange_name, exchange) in EXCHANGES {
        for market in MARKETS {
            let profile = format!("{exchange_name}_{market}");
            let stream = format!("integration:{profile}:depth");

            let format = DepthFormatConfig::load(depth_path(&profile)).unwrap_or_else(|error| {
                panic!("{profile}: depth config failed: {error:?}");
            });

            let plan = DepthContinuityCompiler::compile(&format).unwrap_or_else(|error| {
                panic!("{profile}: depth compilation failed: {error:?}");
            });

            let binding = DepthSourceBinding::new(
                &stream,
                exchange,
                market,
                INSTRUMENT_ID,
                "BTCUSDT",
                None,
                depth_epoch(),
            )
            .unwrap_or_else(|error| {
                panic!("{profile}: depth binding failed: {error:?}");
            });

            let ingress = BoundDepthIngress::new(
                binding,
                plan,
                depth_limits(),
                DepthResourceLimits::default(),
            )
            .unwrap_or_else(|error| {
                panic!("{profile}: depth ingress failed: {error}");
            });

            assert_eq!(
                ingress.binding().epoch().stream_generation,
                GENERATION,
                "{profile}: wrong stream generation",
            );

            assert_eq!(
                ingress.binding().epoch().stream_generation,
                GENERATION,
                "{profile}: wrong stream generation",
            );

            assert_eq!(
                ingress.controller().book_generation(),
                0,
                "{profile}: unexpected initial book generation",
            );

            assert!(
                !ingress.is_synchronized(),
                "{profile}: unexpectedly synchronized",
            );

            checked += 1;
        }
    }

    assert_eq!(checked, 15);
}

#[test]
fn every_trade_profile_constructs_a_source_bound_ingress() {
    let mut checked = 0;

    for (exchange_name, exchange) in EXCHANGES {
        for market in MARKETS {
            let profile = format!("{exchange_name}_{market}");
            let stream = format!("integration:{profile}:trades");

            let format = TradeFormatConfig::load(trade_path(&profile)).unwrap_or_else(|error| {
                panic!("{profile}: trade config failed: {error:?}");
            });

            let plan = TradeIntegrityCompiler::compile(&format).unwrap_or_else(|error| {
                panic!("{profile}: trade compilation failed: {error:?}");
            });

            let binding = TradeSourceBinding::new(
                &stream,
                exchange,
                market,
                INSTRUMENT_ID,
                "BTCUSDT",
                trade_epoch(),
            )
            .unwrap_or_else(|error| {
                panic!("{profile}: trade binding failed: {error:?}");
            });

            let ingress =
                BoundTradeIngress::new(binding, trade_limits(), plan).unwrap_or_else(|error| {
                    panic!("{profile}: trade ingress failed: {error}");
                });

            assert_eq!(
                ingress.binding().epoch().stream_generation,
                GENERATION,
                "{profile}: wrong stream generation",
            );

            assert_eq!(
                ingress.controller().generation(),
                GENERATION,
                "{profile}: wrong controller generation",
            );

            assert!(
                !ingress.is_quarantined(),
                "{profile}: unexpectedly quarantined",
            );

            checked += 1;
        }
    }

    assert_eq!(checked, 15);
}

#[test]
fn every_trade_profile_accepts_and_deduplicates_executions() {
    use marketforge_engine::{
        canonical::{EventEnvelope, Trade, TradeSide},
        live::{
            infrastructure::normalization::{
                NormalizationSource, NormalizedTrades, TradeRecordSemantics,
            },
            integrity::{BoundTradeIngressOutcome, TradeControllerOutcome, TradeSequenceKind},
        },
    };

    use rust_decimal::Decimal;

    let mut checked = 0;

    for (exchange_name, exchange) in EXCHANGES {
        for market in MARKETS {
            let profile = format!("{exchange_name}_{market}");
            let stream = format!("integration:{profile}:trades");

            let format = TradeFormatConfig::load(trade_path(&profile)).unwrap();

            let plan = TradeIntegrityCompiler::compile(&format).unwrap();

            let needs_sequence = plan.sequence_kind == TradeSequenceKind::CrossSequence;

            let binding = TradeSourceBinding::new(
                &stream,
                exchange,
                market,
                INSTRUMENT_ID,
                "BTCUSDT",
                trade_epoch(),
            )
            .unwrap();

            let mut ingress = BoundTradeIngress::new(binding, trade_limits(), plan).unwrap();

            let make_trade = |id: &str, sequence: u64| Trade {
                envelope: EventEnvelope {
                    event_timestamp_ns: 1_000_000_000,
                    system_timestamp_ns: None,
                    exchange,
                    instrument_id: INSTRUMENT_ID,
                    symbol: "BTCUSDT".into(),
                    stream_id: stream.clone(),
                },

                trade_id: Some(id.into()),

                sequence: needs_sequence.then_some(sequence),

                side: TradeSide::Buy,
                price: Decimal::from(80_000),

                quantity_base: Some(Decimal::ONE),
                quantity_quote: None,
                quantity_contracts: None,

                is_rpi: None,
                trade_iv: None,
                mark_iv: None,

                index_price: None,
                mark_price: None,
            };

            // First two native trade identities.
            let first = NormalizedTrades::new(
                &stream,
                NormalizationSource::WebSocket,
                TradeRecordSemantics::Individual,
                vec![make_trade("100", 100), make_trade("101", 101)],
            )
            .unwrap();

            let outcome = ingress.on_websocket(trade_epoch(), &first);

            let BoundTradeIngressOutcome::Processed(TradeControllerOutcome::Accepted(batch)) =
                outcome
            else {
                panic!("{profile}: initial trades were not accepted");
            };

            assert_eq!(batch.trades.len(), 2, "{profile}: wrong accepted count");

            assert_eq!(
                batch.duplicate_records, 0,
                "{profile}: unexpected duplicate"
            );

            // Trade 101 repeats; 102 is new.
            let second = NormalizedTrades::new(
                &stream,
                NormalizationSource::WebSocket,
                TradeRecordSemantics::Individual,
                vec![make_trade("101", 101), make_trade("102", 102)],
            )
            .unwrap();

            let outcome = ingress.on_websocket(trade_epoch(), &second);

            let BoundTradeIngressOutcome::Processed(TradeControllerOutcome::Accepted(batch)) =
                outcome
            else {
                panic!("{profile}: second trade batch was not accepted");
            };

            assert_eq!(
                batch.duplicate_records, 1,
                "{profile}: duplicate not detected"
            );

            assert_eq!(
                batch.trades.len(),
                1,
                "{profile}: duplicate was not removed"
            );

            assert_eq!(
                batch.trades[0].trade_id.as_deref(),
                Some("102"),
                "{profile}: wrong retained execution"
            );

            assert!(
                ingress.controller().identities().contains("100"),
                "{profile}: original identity missing"
            );

            assert!(
                ingress.controller().identities().contains("102"),
                "{profile}: latest identity missing"
            );

            checked += 1;
        }
    }

    assert_eq!(checked, 15);
}

#[test]
fn every_websocket_depth_profile_synchronizes() {
    use marketforge_engine::{
        canonical::{BookSide, EventEnvelope, L2LevelUpdate, SequenceMetadata},
        live::{
            infrastructure::normalization::{
                DepthMessageKind, NormalizationSource, NormalizedDepth,
            },
            integrity::{DepthContinuityFamily, DepthControllerOutcome},
        },
    };

    use rust_decimal::Decimal;

    let exchanges = [
        ("bybit", Exchange::Bybit),
        ("bitget", Exchange::Bitget),
        ("okx", Exchange::Okx),
    ];

    let mut checked = 0;

    for (exchange_name, exchange) in exchanges {
        for market in MARKETS {
            let profile = format!("{exchange_name}_{market}");
            let stream = format!("integration:{profile}:depth");

            let format = DepthFormatConfig::load(depth_path(&profile)).unwrap();

            let plan = DepthContinuityCompiler::compile(&format).unwrap();

            let family = plan.family;

            let subscribed_depth = (family == DepthContinuityFamily::BybitV5).then_some(200);

            let binding = DepthSourceBinding::new(
                &stream,
                exchange,
                market,
                INSTRUMENT_ID,
                "BTCUSDT",
                subscribed_depth,
                depth_epoch(),
            )
            .unwrap();

            let mut ingress = BoundDepthIngress::new(
                binding,
                plan,
                depth_limits(),
                DepthResourceLimits::default(),
            )
            .unwrap();

            let level = |side, price, quantity| L2LevelUpdate {
                envelope: EventEnvelope {
                    event_timestamp_ns: 1_000_000_000,
                    system_timestamp_ns: None,
                    exchange,
                    instrument_id: INSTRUMENT_ID,
                    symbol: "BTCUSDT".into(),
                    stream_id: stream.clone(),
                },

                side,
                price: Decimal::from(price),
                quantity_base: Some(Decimal::from(quantity)),
                quantity_quote: None,
                quantity_contracts: None,
                order_count: None,
            };

            let message = |kind, sequence, quantity| {
                NormalizedDepth::new(
                    &stream,
                    NormalizationSource::WebSocket,
                    kind,
                    sequence,
                    vec![
                        level(BookSide::Bid, 80_000, quantity),
                        level(BookSide::Ask, 80_010, 2),
                    ],
                    Some(1_000_000_000),
                    subscribed_depth,
                )
                .unwrap()
            };

            let snapshot_sequence = match family {
                DepthContinuityFamily::BybitV5 => SequenceMetadata {
                    sequence_first: None,
                    sequence_last: Some(100),
                    sequence_previous: None,
                    cross_sequence: Some(200),
                },

                DepthContinuityFamily::BitgetV3Previous => SequenceMetadata {
                    sequence_first: None,
                    sequence_last: Some(100),
                    sequence_previous: Some(0),
                    cross_sequence: None,
                },

                DepthContinuityFamily::OkxV5Previous => SequenceMetadata {
                    sequence_first: None,
                    sequence_last: Some(100),
                    sequence_previous: None,
                    cross_sequence: None,
                },

                other => panic!("{profile}: unexpected family {other:?}"),
            };

            let snapshot = message(DepthMessageKind::Snapshot, snapshot_sequence, 3);

            let initial = ingress.on_websocket(depth_epoch(), snapshot, 1024);

            if family == DepthContinuityFamily::BitgetV3Previous {
                // Bitget snapshots are staged until an update
                // establishes the required sequence bridge.
                assert!(
                    matches!(initial, DepthControllerOutcome::SnapshotStaged { .. }),
                    "{profile}: expected staged snapshot, got {initial:?}"
                );

                assert!(
                    !ingress.is_synchronized(),
                    "{profile}: staged book was prematurely exposed"
                );
            } else {
                assert!(
                    matches!(
                        initial,
                        DepthControllerOutcome::Accepted { snapshot: true, .. }
                    ),
                    "{profile}: snapshot rejected: {initial:?}"
                );

                assert!(
                    ingress.is_synchronized(),
                    "{profile}: snapshot did not synchronize"
                );
            }

            let bridge_sequence = match family {
                DepthContinuityFamily::BybitV5 => SequenceMetadata {
                    sequence_first: None,
                    sequence_last: Some(101),
                    sequence_previous: None,
                    cross_sequence: Some(201),
                },

                DepthContinuityFamily::BitgetV3Previous => SequenceMetadata {
                    sequence_first: None,
                    sequence_last: Some(105),
                    sequence_previous: Some(99),
                    cross_sequence: None,
                },

                DepthContinuityFamily::OkxV5Previous => SequenceMetadata {
                    sequence_first: None,
                    sequence_last: Some(105),
                    sequence_previous: Some(100),
                    cross_sequence: None,
                },

                _ => unreachable!(),
            };

            let update = message(DepthMessageKind::Delta, bridge_sequence, 7);

            let result = ingress.on_websocket(depth_epoch(), update, 1024);

            assert!(
                matches!(result, DepthControllerOutcome::Accepted { .. }),
                "{profile}: initial update rejected: {result:?}"
            );

            assert!(
                ingress.is_synchronized(),
                "{profile}: synchronization incomplete"
            );

            assert_eq!(
                ingress.controller().baseline().unwrap().last_sequence,
                if family == DepthContinuityFamily::BybitV5 {
                    101
                } else {
                    105
                },
                "{profile}: wrong sequence baseline"
            );

            assert_eq!(
                ingress.controller().book().bids().len(),
                1,
                "{profile}: expected one bid"
            );

            assert_eq!(
                ingress.controller().book().asks().len(),
                1,
                "{profile}: expected one ask"
            );

            checked += 1;
        }
    }

    assert_eq!(checked, 9);
}

#[test]
fn all_remaining_rest_profiles_bridge_and_publish_full_books() {
    let mut checked = 0;

    for (profile, exchange, market) in REST_CASES {
        let format = DepthFormatConfig::load(depth_path(profile)).unwrap();

        let plan = DepthContinuityCompiler::compile(&format).unwrap();

        let family = plan.family;

        let stream = format!("integration:{profile}:depth");

        let mut ingress = rest_ingress(profile, exchange, market);

        let mut publication = DepthPublicationFence::new(&stream, GENERATION).unwrap();

        begin_profile_rest(&mut ingress, profile, 1);

        // ----------------------------------------------------
        // INITIAL BUFFERED DELTA
        // ----------------------------------------------------

        let first = match family {
            DepthContinuityFamily::BinanceFuturesPrevious => {
                // Directly continues snapshot sequence 100.
                profile_delta(&stream, exchange, family, 101, 105, Some(100), 7)
            }

            DepthContinuityFamily::GateRangeCover => {
                // Overlapping range contains snapshot.id + 1.
                profile_delta(&stream, exchange, family, 98, 105, None, 7)
            }

            other => panic!("{profile}: unexpected REST family {other:?}"),
        };

        let buffered = ingress.on_websocket(depth_epoch(), first, 1024);

        assert!(
            matches!(buffered, DepthControllerOutcome::Ignored { .. }),
            "{profile}: expected buffered update, got {buffered:?}"
        );

        // ----------------------------------------------------
        // CONTINUATION
        // ----------------------------------------------------

        let continuation = match family {
            DepthContinuityFamily::BinanceFuturesPrevious => {
                profile_delta(&stream, exchange, family, 106, 110, Some(105), 9)
            }

            DepthContinuityFamily::GateRangeCover => {
                profile_delta(&stream, exchange, family, 104, 110, None, 9)
            }

            _ => unreachable!(),
        };

        ingress.on_websocket(depth_epoch(), continuation, 1024);

        // ----------------------------------------------------
        // REST SNAPSHOT COMPLETION
        // ----------------------------------------------------

        let snapshot = profile_snapshot(&stream, exchange, 100);

        let outcome = ingress.on_rest_snapshot(depth_epoch(), rest_token(1), &snapshot);

        assert!(
            matches!(
                outcome,
                DepthControllerOutcome::Accepted { snapshot: true, .. }
            ),
            "{profile}: REST bridge failed: {outcome:?}"
        );

        assert!(
            ingress.is_synchronized(),
            "{profile}: failed to synchronize"
        );

        assert_eq!(
            ingress.controller().baseline().unwrap().last_sequence,
            110,
            "{profile}: wrong final sequence"
        );

        // ----------------------------------------------------
        // FULL BOOK PUBLICATION
        // ----------------------------------------------------

        let prepared = publication
            .prepare(ingress.controller(), &outcome)
            .unwrap_or_else(|error| {
                panic!("{profile}: publication preparation failed: {error:?}");
            });

        assert_eq!(
            prepared.events().len(),
            1,
            "{profile}: expected one full snapshot"
        );

        assert!(
            matches!(prepared.events()[0], DepthPublicationEvent::Snapshot { .. }),
            "{profile}: initial publication must be snapshot"
        );

        publication.acknowledge(prepared).unwrap();

        assert_eq!(
            publication.state().publication_revision,
            1,
            "{profile}: snapshot not acknowledged"
        );

        // ----------------------------------------------------
        // SUBSEQUENT INCREMENTAL UPDATE
        // ----------------------------------------------------

        let next = match family {
            DepthContinuityFamily::BinanceFuturesPrevious => {
                profile_delta(&stream, exchange, family, 111, 115, Some(110), 11)
            }

            DepthContinuityFamily::GateRangeCover => {
                profile_delta(&stream, exchange, family, 110, 115, None, 11)
            }

            _ => unreachable!(),
        };

        let outcome = ingress.on_websocket(depth_epoch(), next, 1024);

        assert!(
            matches!(
                outcome,
                DepthControllerOutcome::Accepted {
                    snapshot: false,
                    ..
                }
            ),
            "{profile}: subsequent delta rejected: {outcome:?}"
        );

        let prepared = publication.prepare(ingress.controller(), &outcome).unwrap();

        assert!(
            matches!(prepared.events()[0], DepthPublicationEvent::Delta { .. }),
            "{profile}: expected incremental publication"
        );

        publication.acknowledge(prepared).unwrap();

        assert_eq!(publication.state().publication_revision, 2,);

        assert_eq!(ingress.controller().baseline().unwrap().last_sequence, 115,);

        checked += 1;
    }

    assert_eq!(checked, 5);
}

#[test]
fn all_remaining_rest_profiles_reject_missing_bridge() {
    let mut checked = 0;

    for (profile, exchange, market) in REST_CASES {
        let format = DepthFormatConfig::load(depth_path(profile)).unwrap();

        let plan = DepthContinuityCompiler::compile(&format).unwrap();

        let family = plan.family;

        let stream = format!("integration:{profile}:depth");

        let mut ingress = rest_ingress(profile, exchange, market);

        begin_profile_rest(&mut ingress, profile, 1);

        // ----------------------------------------------------
        // INVALID INITIAL BRIDGE
        // ----------------------------------------------------

        // Snapshot ends at 100.
        //
        // Available update starts at 110, leaving a missing
        // interval between the snapshot and first update.
        //
        // Binance Futures also supplies a previous-update
        // ID that does not match snapshot sequence 100.
        let invalid = profile_delta(&stream, exchange, family, 110, 115, Some(109), 7);

        ingress.on_websocket(depth_epoch(), invalid, 1024);

        let outcome = ingress.on_rest_snapshot(
            depth_epoch(),
            rest_token(1),
            &profile_snapshot(&stream, exchange, 100),
        );

        assert!(
            matches!(outcome, DepthControllerOutcome::RecoveryRequired { .. }),
            "{profile}: invalid bridge was accepted: {outcome:?}"
        );

        assert!(
            !ingress.is_synchronized(),
            "{profile}: invalid bridge synchronized book"
        );

        assert!(
            !ingress.controller().book().is_initialized(),
            "{profile}: partial book became authoritative"
        );

        assert_eq!(
            ingress.controller().baseline(),
            None,
            "{profile}: failed bridge retained a baseline"
        );

        // ----------------------------------------------------
        // FAILED ATTEMPT IS PERMANENTLY FENCED
        // ----------------------------------------------------

        let late = ingress.on_rest_snapshot(
            depth_epoch(),
            rest_token(1),
            &profile_snapshot(&stream, exchange, 100),
        );

        assert!(
            matches!(late, DepthControllerOutcome::Ignored { .. }),
            "{profile}: failed attempt accepted a late callback"
        );

        // ----------------------------------------------------
        // FRESH RECOVERY ATTEMPT
        // ----------------------------------------------------

        begin_profile_rest(&mut ingress, profile, 2);

        let valid = match family {
            DepthContinuityFamily::BinanceFuturesPrevious => {
                profile_delta(&stream, exchange, family, 201, 205, Some(200), 9)
            }

            DepthContinuityFamily::GateRangeCover => {
                profile_delta(&stream, exchange, family, 198, 205, None, 9)
            }

            _ => unreachable!(),
        };

        ingress.on_websocket(depth_epoch(), valid, 1024);

        let recovered = ingress.on_rest_snapshot(
            depth_epoch(),
            rest_token(2),
            &profile_snapshot(&stream, exchange, 200),
        );

        assert!(
            matches!(
                recovered,
                DepthControllerOutcome::Accepted { snapshot: true, .. }
            ),
            "{profile}: new recovery attempt failed: {recovered:?}"
        );

        assert!(
            ingress.is_synchronized(),
            "{profile}: failed to restore synchronization"
        );

        assert_eq!(ingress.controller().baseline().unwrap().last_sequence, 205,);

        checked += 1;
    }

    assert_eq!(checked, 5);
}

// ============================================================
// N5N.7.7g.3 — ADVERSARIAL ALL-EXCHANGE DEPTH FIXTURES
// ============================================================

fn adversarial_depth_message(
    stream: &str,
    exchange: Exchange,
    source: NormalizationSource,
    kind: DepthMessageKind,
    sequence: SequenceMetadata,
    quantity: i64,
    declared_depth: Option<u64>,
) -> NormalizedDepth {
    NormalizedDepth::new(
        stream,
        source,
        kind,
        sequence,
        vec![rest_level(
            stream,
            exchange,
            BookSide::Bid,
            80_000,
            quantity,
        )],
        Some(1_000_000_000),
        declared_depth,
    )
    .unwrap()
}

/// Construct and fully synchronize an ingress using the
/// profile's actual synchronization family.
///
/// This fixture does not acknowledge publications.
/// It returns the accepted full-snapshot outcome so
/// individual tests can exercise the publication fence.
fn synchronized_adversarial_ingress(
    profile: &str,
    exchange: Exchange,
    market: &str,
) -> (BoundDepthIngress, DepthControllerOutcome) {
    let format = DepthFormatConfig::load(depth_path(profile)).unwrap();

    let plan = DepthContinuityCompiler::compile(&format).unwrap();

    let family = plan.family;

    let stream = format!("integration:{profile}:depth");

    // --------------------------------------------------------
    // REST-SNAPSHOT FAMILIES
    // --------------------------------------------------------

    if plan.snapshot_source == DepthSnapshotSource::Rest {
        let mut ingress = rest_ingress(profile, exchange, market);

        begin_profile_rest(&mut ingress, profile, 1);

        let first = match family {
            DepthContinuityFamily::BinanceSpotRange => {
                profile_delta(&stream, exchange, family, 101, 105, None, 7)
            }

            DepthContinuityFamily::BinanceFuturesPrevious => {
                profile_delta(&stream, exchange, family, 101, 105, Some(100), 7)
            }

            DepthContinuityFamily::GateRangeCover => {
                profile_delta(&stream, exchange, family, 98, 105, None, 7)
            }

            other => {
                panic!("{profile}: unsupported REST family: {other:?}")
            }
        };

        let buffered = ingress.on_websocket(depth_epoch(), first, 1024);

        assert!(
            matches!(buffered, DepthControllerOutcome::Ignored { .. }),
            "{profile}: initial REST delta was not buffered"
        );

        let recovered = ingress.on_rest_snapshot(
            depth_epoch(),
            rest_token(1),
            &profile_snapshot(&stream, exchange, 100),
        );

        assert!(
            matches!(
                recovered,
                DepthControllerOutcome::Accepted { snapshot: true, .. }
            ),
            "{profile}: REST synchronization failed: {recovered:?}"
        );

        assert!(
            ingress.is_synchronized(),
            "{profile}: REST book is not synchronized"
        );

        assert_eq!(
            ingress.controller().baseline().unwrap().last_sequence,
            105,
            "{profile}: unexpected REST baseline"
        );

        return (ingress, recovered);
    }

    // --------------------------------------------------------
    // WEBSOCKET-SNAPSHOT FAMILIES
    // --------------------------------------------------------

    let subscribed_depth = (family == DepthContinuityFamily::BybitV5).then_some(200);

    let binding = DepthSourceBinding::new(
        &stream,
        exchange,
        market,
        INSTRUMENT_ID,
        "BTCUSDT",
        subscribed_depth,
        depth_epoch(),
    )
    .unwrap();

    let mut ingress = BoundDepthIngress::new(
        binding,
        plan,
        depth_limits(),
        DepthResourceLimits::default(),
    )
    .unwrap();

    let snapshot_sequence = match family {
        DepthContinuityFamily::BybitV5 => SequenceMetadata {
            sequence_first: None,
            sequence_last: Some(100),
            sequence_previous: None,
            cross_sequence: Some(200),
        },

        DepthContinuityFamily::BitgetV3Previous => SequenceMetadata {
            sequence_first: None,
            sequence_last: Some(100),
            sequence_previous: Some(0),
            cross_sequence: None,
        },

        DepthContinuityFamily::OkxV5Previous => SequenceMetadata {
            sequence_first: None,
            sequence_last: Some(100),
            sequence_previous: None,
            cross_sequence: None,
        },

        other => {
            panic!("{profile}: unsupported WebSocket family: {other:?}")
        }
    };

    let snapshot = adversarial_depth_message(
        &stream,
        exchange,
        NormalizationSource::WebSocket,
        DepthMessageKind::Snapshot,
        snapshot_sequence,
        3,
        subscribed_depth,
    );

    let initial = ingress.on_websocket(depth_epoch(), snapshot, 1024);

    // Bybit and OKX synchronize directly from their
    // accepted WebSocket snapshots.
    if family != DepthContinuityFamily::BitgetV3Previous {
        assert!(
            matches!(
                initial,
                DepthControllerOutcome::Accepted { snapshot: true, .. }
            ),
            "{profile}: initial snapshot rejected: {initial:?}"
        );

        assert!(ingress.is_synchronized());

        return (ingress, initial);
    }

    // --------------------------------------------------------
    // BITGET STAGED SNAPSHOT BRIDGE
    // --------------------------------------------------------

    assert!(
        matches!(initial, DepthControllerOutcome::SnapshotStaged { .. }),
        "{profile}: expected staged Bitget snapshot"
    );

    assert!(!ingress.is_synchronized());

    let bridge = adversarial_depth_message(
        &stream,
        exchange,
        NormalizationSource::WebSocket,
        DepthMessageKind::Delta,
        SequenceMetadata {
            sequence_first: None,
            sequence_last: Some(105),
            sequence_previous: Some(99),
            cross_sequence: None,
        },
        7,
        None,
    );

    let recovered = ingress.on_websocket(depth_epoch(), bridge, 1024);

    assert!(
        matches!(
            recovered,
            DepthControllerOutcome::Accepted { snapshot: true, .. }
        ),
        "{profile}: Bitget bridge failed: {recovered:?}"
    );

    assert!(ingress.is_synchronized());

    assert_eq!(ingress.controller().baseline().unwrap().last_sequence, 105,);

    (ingress, recovered)
}

#[test]
fn every_depth_profile_invalid_sequence_revokes_authority() {
    let mut checked = 0;

    for (exchange_name, exchange) in EXCHANGES {
        for market in MARKETS {
            let profile = format!("{exchange_name}_{market}");
            let stream = format!("integration:{profile}:depth");

            let format = DepthFormatConfig::load(depth_path(&profile)).unwrap();

            let plan = DepthContinuityCompiler::compile(&format).unwrap();

            let family = plan.family;

            let (mut ingress, initial) =
                synchronized_adversarial_ingress(&profile, exchange, market);

            let mut publication = DepthPublicationFence::new(&stream, GENERATION).unwrap();

            // ------------------------------------------------
            // ACKNOWLEDGE INITIAL SNAPSHOT
            // ------------------------------------------------

            let prepared = publication.prepare(ingress.controller(), &initial).unwrap();

            assert!(
                matches!(prepared.events()[0], DepthPublicationEvent::Snapshot { .. }),
                "{profile}: initial output was not a snapshot"
            );

            publication.acknowledge(prepared).unwrap();

            assert!(publication.has_published_book());

            let baseline = ingress.controller().baseline().unwrap().last_sequence;

            let book_generation = ingress.controller().book_generation();

            // ------------------------------------------------
            // INJECT INVALID CURRENT-EPOCH SEQUENCE
            // ------------------------------------------------

            let invalid = invalid_depth_sequence(&profile, exchange, family, baseline);

            let outcome = ingress.on_websocket(depth_epoch(), invalid, 1024);

            assert!(
                matches!(outcome, DepthControllerOutcome::RecoveryRequired { .. }),
                "{profile}: invalid sequence was not rejected: {outcome:?}"
            );

            assert!(
                !ingress.is_synchronized(),
                "{profile}: invalid sequence retained synchronization"
            );

            assert!(
                !ingress.controller().book().is_initialized(),
                "{profile}: rejected update left an authoritative book"
            );

            assert_eq!(
                ingress.controller().baseline(),
                None,
                "{profile}: invalid sequence retained baseline"
            );

            // ------------------------------------------------
            // REVOKE CONSUMER AUTHORITY
            // ------------------------------------------------

            let prepared = publication
                .prepare(ingress.controller(), &outcome)
                .unwrap_or_else(|error| {
                    panic!("{profile}: failed to prepare invalidation: {error:?}")
                });

            assert!(
                prepared.events().iter().any(|event| {
                    matches!(
                        event,
                        DepthPublicationEvent::Invalidated {
                            book_generation: generation,
                            ..
                        } if *generation == book_generation
                    )
                }),
                "{profile}: invalid sequence did not invalidate published book"
            );

            publication.acknowledge(prepared).unwrap();

            assert!(
                !publication.has_published_book(),
                "{profile}: consumer still has publication authority"
            );

            checked += 1;
        }
    }

    assert_eq!(checked, 15);
}

#[test]
fn every_depth_profile_ignores_obsolete_connection_epoch() {
    let mut checked = 0;

    for (exchange_name, exchange) in EXCHANGES {
        for market in MARKETS {
            let profile = format!("{exchange_name}_{market}");
            let stream = format!("integration:{profile}:depth");

            let format = DepthFormatConfig::load(depth_path(&profile)).unwrap();

            let plan = DepthContinuityCompiler::compile(&format).unwrap();

            let family = plan.family;

            let (mut ingress, initial) =
                synchronized_adversarial_ingress(&profile, exchange, market);

            let mut publication = DepthPublicationFence::new(&stream, GENERATION).unwrap();

            // Establish and acknowledge consumer state.
            let prepared = publication.prepare(ingress.controller(), &initial).unwrap();

            publication.acknowledge(prepared).unwrap();

            assert!(publication.has_published_book());

            // ------------------------------------------------
            // CAPTURE AUTHORITATIVE STATE
            // ------------------------------------------------

            let baseline_before = ingress.controller().baseline().unwrap().last_sequence;

            let generation_before = ingress.controller().book_generation();

            let bids_before = ingress.controller().book().bids().len();

            let asks_before = ingress.controller().book().asks().len();

            let bid_quantity_before = ingress
                .controller()
                .book()
                .bids()
                .get(&Decimal::from(80_000))
                .expect("fixture must contain the initial bid")
                .quantity_base;

            let revision_before = publication.state().publication_revision;

            // ------------------------------------------------
            // OBSOLETE CONNECTION EPOCH
            // ------------------------------------------------

            let old_connection = DepthSourceEpoch {
                stream_generation: GENERATION,
                connection_generation: 19,
                subscription_generation: 30,
            };

            let invalid = invalid_depth_sequence(&profile, exchange, family, baseline_before);

            let outcome = ingress.on_websocket(old_connection, invalid, 1024);

            assert!(
                matches!(outcome, DepthControllerOutcome::Ignored { .. }),
                "{profile}: obsolete connection was not ignored: {outcome:?}"
            );

            // ------------------------------------------------
            // VERIFY NO AUTHORITATIVE STATE MUTATION
            // ------------------------------------------------

            assert!(
                ingress.is_synchronized(),
                "{profile}: obsolete connection invalidated book"
            );

            assert_eq!(
                ingress.controller().baseline().unwrap().last_sequence,
                baseline_before,
                "{profile}: obsolete connection advanced baseline"
            );

            assert_eq!(
                ingress.controller().book_generation(),
                generation_before,
                "{profile}: obsolete connection changed book generation"
            );

            assert_eq!(
                ingress.controller().book().bids().len(),
                bids_before,
                "{profile}: bid count changed"
            );

            assert_eq!(
                ingress.controller().book().asks().len(),
                asks_before,
                "{profile}: ask count changed"
            );

            assert_eq!(
                ingress
                    .controller()
                    .book()
                    .bids()
                    .get(&Decimal::from(80_000))
                    .unwrap()
                    .quantity_base,
                bid_quantity_before,
                "{profile}: obsolete update modified bid quantity"
            );

            // ------------------------------------------------
            // VERIFY PUBLICATION STATE
            // ------------------------------------------------

            assert!(
                publication.has_published_book(),
                "{profile}: obsolete connection revoked publication"
            );

            assert_eq!(
                publication.state().publication_revision,
                revision_before,
                "{profile}: obsolete connection changed revision"
            );

            checked += 1;
        }
    }

    assert_eq!(checked, 15);
}
