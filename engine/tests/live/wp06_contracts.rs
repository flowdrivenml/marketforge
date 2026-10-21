//! WP-06 executable consumers and negative contract witnesses. No production router.
use super::support::live_events::l2_event;
use marketforge_engine::live::foundation::FileEpochStore;
use marketforge_engine::{
    book::{BookLevel, BookStore},
    canonical::{BookSide, EventEnvelope, Exchange, L2LevelUpdate, Trade, TradeSide},
    formats::depth::{DepthEventBoundary, DepthSourceOperation},
    live::{
        model::{
            acceptance::{AcceptanceError, EpochStore, EventAcceptor, MemoryEpochStore},
            codec::{CodecError, EventCodec, EventRecordV1, ExactDecimal, PayloadDtoV1},
            cursor::{BookGeneration, DerivedGeneration, LogEpoch, StreamCursor},
            delivery::{DeliveryCapacity, DeliveryFailure, DeliveryPolicy, DestinationKind},
            demand::RetentionSessionId,
            envelope::{
                ConversionStep, DerivedProvenance, EventOrigin, LiveEvent, LiveEventError,
                LiveL2Group, LivePayload,
            },
            identity::{DerivedStreamId, StreamId},
            memory::RetainedSize,
            replay::{
                BookVersion, BookViewBoundary, CutoverState, ProtectionLease, ProtectionState,
                ProtectionToken, ReplayCutover, ReplayError, ReplayRange, RetentionPositions,
            },
        },
        ports::{
            retention::{
                ReplayPage, ReplayReadOutcome, ReplayReadRequest, RetentionReadFailure,
                RetentionReaderPort,
            },
            routing::{
                DestinationAdmission, DestinationDescriptor, DestinationId, EventBatch,
                EventBatchError, EventDestinationPort, RoutingFuture,
            },
        },
    },
};
use rust_decimal::Decimal;
use std::{
    future::Future,
    pin::pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Wake, Waker},
    time::{Duration, Instant},
};

fn c(offset: u64) -> StreamCursor {
    StreamCursor::new(StreamId("s".into()), LogEpoch(1), offset)
}
fn limits() -> DeliveryCapacity {
    DeliveryCapacity::new(100, 1_000_000).unwrap()
}
fn codec() -> EventCodec {
    EventCodec::new(1_000_000, 1000).unwrap()
}
fn envelope() -> EventEnvelope {
    EventEnvelope {
        event_timestamp_ns: 1_700_000_000_000_000_000,
        system_timestamp_ns: None,
        exchange: Exchange::Bybit,
        instrument_id: 7,
        symbol: "BTCUSDT".into(),
        stream_id: "canonical-source".into(),
    }
}
fn level(side: BookSide, price: i64, qty: i64) -> L2LevelUpdate {
    L2LevelUpdate {
        envelope: envelope(),
        side,
        price: Decimal::from(price),
        quantity_base: Some(Decimal::from(qty)),
        quantity_quote: None,
        quantity_contracts: None,
        order_count: Some(2),
    }
}
fn group_event(offset: u64, initialization: bool) -> Arc<LiveEvent> {
    let mut event = (*l2_event("s", 1, offset)).clone().into_fields();
    if let LivePayload::L2Group(g) = &mut event.payload {
        g.grouped_boundary = if initialization {
            DepthEventBoundary::Initialization
        } else {
            DepthEventBoundary::Changes
        };
        g.grouped_levels = vec![level(BookSide::Bid, 100, 2), level(BookSide::Ask, 101, 3)];
    }
    event.check().unwrap();
    Arc::new(LiveEvent::try_from(event).unwrap())
}
fn trade_event() -> LiveEvent {
    let mut event = (*l2_event("s", 1, 0)).clone().into_fields();
    event.provenance.book_generation = None;
    event.payload = LivePayload::Trade(Trade {
        envelope: envelope(),
        trade_id: Some("native-id".into()),
        sequence: Some(8),
        side: TradeSide::Buy,
        price: Decimal::from_str_exact("123456789.12345678901234567890").unwrap(),
        quantity_base: Some(Decimal::from_str_exact("0.0000000000000000000000000001").unwrap()),
        quantity_quote: None,
        quantity_contracts: None,
        is_rpi: Some(false),
        trade_iv: None,
        mark_iv: None,
        index_price: None,
        mark_price: None,
    });
    event.check().unwrap();
    event.try_into().unwrap()
}
fn recheck(
    event: &marketforge_engine::live::model::envelope::LiveEventFields,
) -> Result<LiveEvent, LiveEventError> {
    LiveEvent::try_new(
        event.cursor.clone(),
        event.payload.clone(),
        event.provenance.clone(),
        event.timing.clone(),
    )
}
struct Noop;
impl Wake for Noop {
    fn wake(self: Arc<Self>) {}
}
fn ready<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(Noop));
    let mut cx = Context::from_waker(&waker);
    match pin!(future).poll(&mut cx) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("deterministic witness unexpectedly pending"),
    }
}

#[test]
fn typed_rest_has_no_fabricated_connection() {
    let mut e = trade_event().into_fields();
    e.provenance.origin = EventOrigin::Rest;
    let e = LiveEvent::try_from(e).unwrap();
    assert_eq!(
        codec()
            .decode(&codec().encode(&e).unwrap())
            .unwrap()
            .as_ref(),
        &e
    );
}
#[test]
fn invalid_source_identity_is_rejected_everywhere() {
    let mut e = trade_event().into_fields();
    e.provenance.source_stream = StreamId("wrong".into());
    assert_eq!(recheck(&e), Err(LiveEventError::StreamIdentityMismatch));
    assert!(serde_json::from_str::<LiveEvent>(&serde_json::to_string(&e).unwrap()).is_err());
    assert!(LiveEvent::try_from(e).is_err());
}
#[test]
fn invalid_canonical_trade_does_not_pass_envelope_validation() {
    let mut e = trade_event().into_fields();
    if let LivePayload::Trade(t) = &mut e.payload {
        t.price = Decimal::ZERO;
    }
    assert_eq!(e.check(), Err(LiveEventError::InvalidPayload));
}
#[test]
fn timestamps_and_generations_are_validated() {
    let mut e = (*group_event(0, false)).clone().into_fields();
    e.timing.exchange_timestamp_ns = Some(1);
    assert_eq!(e.check(), Err(LiveEventError::TimestampMismatch));
    e.timing.exchange_timestamp_ns = e.event_timestamp_ns();
    e.provenance.book_generation = None;
    assert_eq!(e.check(), Err(LiveEventError::MissingBookGeneration));
    let mut e = trade_event().into_fields();
    e.provenance.book_generation = Some(BookGeneration(1));
    assert_eq!(e.check(), Err(LiveEventError::UnexpectedBookGeneration));
}
#[test]
fn malformed_group_is_rejected_atomically() {
    let mut e = (*group_event(0, false)).clone().into_fields();
    if let LivePayload::L2Group(g) = &mut e.payload {
        g.grouped_levels[1].quantity_base = Some(Decimal::NEGATIVE_ONE);
    }
    assert_eq!(e.check(), Err(LiveEventError::InvalidPayload));
    assert!(LiveEvent::try_from(e).is_err());
}
#[test]
fn reversed_native_range_rejected_but_missing_native_sequence_allowed() {
    let mut e = (*group_event(0, false)).clone().into_fields();
    if let LivePayload::L2Group(g) = &mut e.payload {
        g.source.sequence_start = Some(8);
        g.source.sequence_end = Some(7);
    }
    assert_eq!(e.check(), Err(LiveEventError::InvalidSequenceRange));
    if let LivePayload::L2Group(g) = &mut e.payload {
        g.source.sequence_start = None;
        g.source.sequence_end = None;
    }
    assert!(e.check().is_ok());
}
#[test]
fn initialization_rejects_duplicate_or_deleted_levels() {
    let mut e = (*group_event(0, true)).clone().into_fields();
    if let LivePayload::L2Group(g) = &mut e.payload {
        g.grouped_levels.push(g.grouped_levels[0].clone());
    }
    assert_eq!(e.check(), Err(LiveEventError::InvalidInitialization));
    if let LivePayload::L2Group(g) = &mut e.payload {
        g.grouped_levels.pop();
        g.grouped_levels[0].quantity_base = Some(Decimal::ZERO);
    }
    assert_eq!(e.check(), Err(LiveEventError::InvalidInitialization));
}
#[test]
fn mixed_instrument_or_timestamp_group_is_rejected() {
    let mut e = (*group_event(0, false)).clone().into_fields();
    if let LivePayload::L2Group(g) = &mut e.payload {
        g.grouped_levels[1].envelope.instrument_id = 9;
    }
    assert_eq!(e.check(), Err(LiveEventError::InvalidPayload));
    if let LivePayload::L2Group(g) = &mut e.payload {
        g.grouped_levels[1].envelope.instrument_id = 7;
        g.grouped_levels[1].envelope.event_timestamp_ns += 1;
    }
    assert_eq!(e.check(), Err(LiveEventError::TimestampMismatch));
}
#[test]
fn rejected_input_consumes_no_offset_and_no_change_is_one_event() {
    let mut store = MemoryEpochStore::default();
    let mut owner = EventAcceptor::start(&mut store, StreamId("s".into())).unwrap();
    let mut e = (*l2_event("s", 1, 99)).clone().into_fields();
    e.provenance.source_stream = StreamId("bad".into());
    assert!(matches!(
        owner.accept(e.payload.clone(), e.provenance.clone(), e.timing.clone()),
        Err(AcceptanceError::InvalidEvent(_))
    ));
    assert_eq!(owner.next_cursor().unwrap(), c(0));
    e.provenance.source_stream = StreamId("s".into());
    let accepted = owner.accept(e.payload, e.provenance, e.timing).unwrap();
    assert_eq!(accepted.cursor, c(0));
    assert!(
        matches!(&accepted.payload,LivePayload::L2Group(g) if g.grouped_levels.is_empty() && g.source.sequence_end==Some(99))
    );
    assert_eq!(owner.next_cursor().unwrap(), c(1));
}
#[test]
fn one_multi_level_event_consumes_one_cursor() {
    let mut store = MemoryEpochStore::default();
    let mut owner = EventAcceptor::start(&mut store, StreamId("s".into())).unwrap();
    let e = group_event(70, false);
    let accepted = owner
        .accept(e.payload.clone(), e.provenance.clone(), e.timing.clone())
        .unwrap();
    assert_eq!(accepted.cursor, c(0));
    assert_eq!(owner.next_cursor().unwrap(), c(1));
    let record = EventRecordV1::try_from(accepted.as_ref()).unwrap();
    assert!(matches!(record.payload,PayloadDtoV1::L2Group{levels,..} if levels.len()==2));
}
#[test]
fn file_epoch_restart_prevents_cursor_reuse() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("epochs.json");
    let mut store = FileEpochStore::open(&path).unwrap();
    let first = EventAcceptor::start(&mut store, StreamId("s".into()))
        .unwrap()
        .next_cursor()
        .unwrap();
    assert!(FileEpochStore::open(&path).is_err());
    drop(store);
    let mut store = FileEpochStore::open(&path).unwrap();
    let next = EventAcceptor::start(&mut store, StreamId("s".into()))
        .unwrap()
        .next_cursor()
        .unwrap();
    assert_eq!(first.offset, next.offset);
    assert_ne!(first.log_epoch, next.log_epoch);
}
#[test]
fn corrupted_or_future_epoch_journal_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("epochs.json");
    for data in ["garbage", r#"{"version":2,"epochs":{}}"#] {
        std::fs::write(&path, data).unwrap();
        assert!(matches!(
            FileEpochStore::open(&path),
            Err(AcceptanceError::InvalidJournal)
        ));
        assert_eq!(std::fs::read_to_string(&path).unwrap(), data);
    }
}
#[test]
fn failed_epoch_persistence_fences_owner() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("epochs.json");
    let mut store = FileEpochStore::open(&path).unwrap();
    std::fs::create_dir(dir.path().join("epochs.json.pending")).unwrap();
    assert!(store.fresh_epoch(&StreamId("s".into())).is_err());
    assert!(matches!(
        store.fresh_epoch(&StreamId("s".into())),
        Err(AcceptanceError::Fenced)
    ));
}
#[test]
fn restart_epoch_exhaustion_is_explicit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("epochs.json");
    std::fs::write(
        &path,
        format!(r#"{{"version":1,"epochs":{{"s":{}}}}}"#, u64::MAX),
    )
    .unwrap();
    let mut store = FileEpochStore::open(path).unwrap();
    assert!(matches!(
        store.fresh_epoch(&StreamId("s".into())),
        Err(AcceptanceError::Exhausted)
    ));
}
#[test]
fn exact_decimal_trade_roundtrip_and_nulls() {
    let e = trade_event();
    let bytes = codec().encode(&e).unwrap();
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(text.contains(r#""price":"123456789.12345678901234567890""#));
    assert!(text.contains(r#""quantity_quote":null"#));
    assert_eq!(codec().decode(&bytes).unwrap().as_ref(), &e);
    assert!(serde_json::from_str::<ExactDecimal>("0.1").is_err());
    assert!(serde_json::from_str::<ExactDecimal>(r#""1e-10""#).is_err());
}
#[test]
fn codec_rejects_unknown_versions_fields_and_oversize() {
    let bytes = codec().encode(&trade_event()).unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    value["schema_version"] = 2.into();
    assert!(matches!(
        codec().decode(&serde_json::to_vec(&value).unwrap()),
        Err(CodecError::Version(2))
    ));
    value["schema_version"] = 1.into();
    value["unknown"] = true.into();
    assert!(
        codec()
            .decode(&serde_json::to_vec(&value).unwrap())
            .is_err()
    );
    let small = EventCodec::new(10, 1).unwrap();
    assert!(matches!(
        small.encode(&trade_event()),
        Err(CodecError::Limit)
    ));
    assert!(matches!(small.decode(&bytes), Err(CodecError::Limit)));
    assert!(matches!(
        EventCodec::new(10_000, 1)
            .unwrap()
            .encode(&group_event(0, true)),
        Err(CodecError::Limit)
    ));
}
#[test]
fn grouped_depth_codec_preserves_native_metadata_and_order() {
    let e = group_event(10, true);
    let restored = codec().decode(&codec().encode(&e).unwrap()).unwrap();
    assert_eq!(restored, e);
    if let LivePayload::L2Group(g) = &restored.payload {
        assert_eq!(g.grouped_levels[0].side, BookSide::Bid);
        assert_eq!(g.source.source_event_ordinal, Some(10));
    } else {
        panic!("group lost");
    }
}
#[test]
fn derived_provenance_consumer_preserves_source_and_exact_reference() {
    let source = Arc::new(trade_event());
    let mut output = (*source).clone().into_fields();
    output.cursor = StreamCursor::new(StreamId("derived-publication".into()), LogEpoch(9), 0);
    output.provenance.origin = EventOrigin::Derived(DerivedProvenance {
        derived_stream: DerivedStreamId("conversion-1".into()),
        generation: DerivedGeneration(4),
        source_cursor: source.cursor.clone(),
        transformation_revision: 5,
        ordering_policy_revision: Some(7),
        conversion: vec![ConversionStep {
            from_currency: "USDT".into(),
            to_currency: "USD".into(),
            reference: StreamCursor::new(StreamId("reference".into()), LogEpoch(2), 9),
            rate: Decimal::from_str_exact("0.99999999999999999999").unwrap(),
            inverted: false,
        }],
    });
    if let LivePayload::Trade(t) = &mut output.payload {
        t.price = Decimal::ONE;
    }
    let output = LiveEvent::try_from(output).unwrap();
    let decoded = codec().decode(&codec().encode(&output).unwrap()).unwrap();
    assert_eq!(*decoded, output);
    assert_ne!(source.payload, output.payload);
    assert_eq!(source.cursor, c(0));
    match &decoded.provenance.origin {
        EventOrigin::Derived(p) => assert_eq!(p.source_cursor, source.cursor),
        _ => panic!("origin lost"),
    }
    let charged = decoded.retained_size_bytes().unwrap();
    assert!(charged > source.retained_size_bytes().unwrap());
    let mut output = output.into_fields();
    output.cursor = source.cursor.clone();
    assert_eq!(output.check(), Err(LiveEventError::InvalidOrigin));
}
#[test]
fn parquet_retention_roundtrip_preserves_complete_envelope() {
    use arrow_array::{ArrayRef, BinaryArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};
    let event = group_event(55, true);
    let bytes = codec().encode(&event).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new(
        "event_record_v1",
        DataType::Binary,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(BinaryArray::from(vec![bytes.as_slice()])) as ArrayRef],
    )
    .unwrap();
    let file = tempfile::tempfile().unwrap();
    let mut writer = ArrowWriter::try_new(file.try_clone().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let batch = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    let column = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(codec().decode(column.value(0)).unwrap(), event);
}
#[test]
fn full_initialization_reconstructs_without_previous_generation() {
    let mut old = BookStore::new();
    let q = BookLevel {
        quantity_base: Some(Decimal::from(2)),
        quantity_quote: None,
        quantity_contracts: None,
        order_count: Some(2),
    };
    old.apply_snapshot(
        vec![(Decimal::from(100), q.clone())],
        vec![(Decimal::from(101), q.clone())],
    )
    .unwrap();
    assert!(
        old.apply_snapshot(
            vec![(Decimal::from(100), q.clone())],
            vec![(Decimal::from(101), q.clone())]
        )
        .unwrap()
        .is_empty()
    );
    let e = group_event(0, true);
    let source = if let LivePayload::L2Group(g) = &e.payload {
        g.source.clone()
    } else {
        unreachable!()
    };
    let group =
        LiveL2Group::from_book(&old, envelope(), source, DepthSourceOperation::Snapshot).unwrap();
    assert_eq!(group.grouped_levels.len(), 2);
    let mut fresh = BookStore::new();
    let mut bids = vec![];
    let mut asks = vec![];
    for l in &group.grouped_levels {
        let target = if l.side == BookSide::Bid {
            &mut bids
        } else {
            &mut asks
        };
        target.push((
            l.price,
            BookLevel {
                quantity_base: l.quantity_base,
                quantity_quote: l.quantity_quote,
                quantity_contracts: l.quantity_contracts,
                order_count: l.order_count,
            },
        ));
    }
    fresh.apply_snapshot(bids, asks).unwrap();
    assert_eq!(fresh.bids(), old.bids());
    assert_eq!(fresh.asks(), old.asks());
    let view =
        BookViewBoundary::new(c(40), BookGeneration(8), group, BookVersion(17), 2, 3).unwrap();
    assert_eq!(view.cursor(), &c(40));
    assert_eq!(view.book_version(), BookVersion(17));
    assert_eq!(view.generation(), BookGeneration(8));
    assert_eq!(view.configuration_revision(), 2);
    assert_eq!(view.metadata_revision(), 3);
}

fn token() -> ProtectionToken {
    ProtectionToken {
        id: 1,
        generation: 1,
    }
}
fn consumer() -> RetentionSessionId {
    RetentionSessionId("bootstrap".into())
}
fn lease(now: Instant) -> ProtectionLease {
    ProtectionLease::admit(
        token(),
        consumer(),
        ReplayRange::inclusive(c(0), None).unwrap(),
        ReplayRange::inclusive(c(0), Some(c(10))).unwrap(),
        c(10),
        limits(),
        11,
        4096,
        now,
        now + Duration::from_secs(10),
        now + Duration::from_secs(20),
    )
    .unwrap()
}
#[test]
fn range_endpoint_and_exhaustion_semantics() {
    let range = ReplayRange::resume_after(c(10), Some(c(12))).unwrap();
    assert_eq!(range.first(), &c(11));
    assert!(!range.contains(&c(10)));
    assert!(range.contains(&c(12)));
    assert!(!range.contains(&c(13)));
    assert_eq!(
        ReplayRange::resume_after(c(u64::MAX), None),
        Err(ReplayError::Exhausted)
    );
    assert_eq!(
        ReplayRange::inclusive(c(9), Some(c(8))),
        Err(ReplayError::Range)
    );
    let mut other = c(10);
    other.log_epoch = LogEpoch(2);
    assert_eq!(
        ReplayRange::inclusive(c(0), Some(other)),
        Err(ReplayError::Namespace)
    );
}
#[test]
fn cursor_page_requires_contiguity_not_merely_increasing_order() {
    let batch = EventBatch::new(vec![l2_event("s", 1, 0), l2_event("s", 1, 2)]).unwrap();
    assert_eq!(
        ReplayRange::inclusive(c(0), None)
            .unwrap()
            .check_page(&batch),
        Err(ReplayError::Gap)
    );
    let mixed = EventBatch::new(vec![l2_event("s", 1, 0), l2_event("s", 2, 1)]);
    assert_eq!(mixed, Err(EventBatchError::MixedNamespace));
    assert_eq!(EventBatch::new(vec![]), Err(EventBatchError::Empty));
    assert_eq!(
        EventBatch::new(vec![l2_event("s", 1, 0), l2_event("s", 1, 0)]),
        Err(EventBatchError::NonIncreasingCursor)
    );
}
#[test]
fn accepted_committed_and_durable_positions_are_distinct_and_checked() {
    let p = RetentionPositions::new(c(9), Some(c(7)), Some(c(5))).unwrap();
    assert_eq!(p.accepted(), &c(9));
    assert_eq!(p.committed(), Some(&c(7)));
    assert_eq!(p.durable(), Some(&c(5)));
    assert_eq!(
        RetentionPositions::new(c(9), None, Some(c(5))),
        Err(ReplayError::Range)
    );
    assert_eq!(
        RetentionPositions::new(c(9), Some(c(10)), None),
        Err(ReplayError::Range)
    );
    assert_eq!(
        RetentionPositions::new(c(9), Some(c(7)), Some(c(8))),
        Err(ReplayError::Range)
    );
    let mut other = c(5);
    other.log_epoch = LogEpoch(2);
    assert_eq!(
        RetentionPositions::new(c(9), Some(other), None),
        Err(ReplayError::Namespace)
    );
}
#[test]
fn protection_checks_owner_monotonic_consumption_and_idempotent_release() {
    let now = Instant::now();
    let mut lease = lease(now);
    let stranger = RetentionSessionId("other".into());
    assert_eq!(
        lease.advance(&stranger, token(), c(3), now),
        Err(ReplayError::Ownership)
    );
    assert!(lease.protects(&c(0), now));
    lease.advance(&consumer(), token(), c(3), now).unwrap();
    assert!(!lease.protects(&c(3), now));
    assert!(lease.protects(&c(4), now));
    assert_eq!(
        lease.advance(&consumer(), token(), c(2), now),
        Err(ReplayError::Range)
    );
    assert_eq!(
        lease.advance(&consumer(), token(), c(11), now),
        Err(ReplayError::Range)
    );
    lease.release(&consumer(), token(), now).unwrap();
    lease.release(&consumer(), token(), now).unwrap();
    assert_eq!(lease.state(now), ProtectionState::Released);
    assert!(!lease.protects(&c(4), now));
}
#[test]
fn overlapping_protections_keep_independent_ownership() {
    let now = Instant::now();
    let mut a = lease(now);
    let mut b = ProtectionLease::admit(
        ProtectionToken {
            id: 2,
            generation: 1,
        },
        RetentionSessionId("second".into()),
        ReplayRange::inclusive(c(3), Some(c(8))).unwrap(),
        ReplayRange::inclusive(c(0), Some(c(10))).unwrap(),
        c(8),
        limits(),
        11,
        4096,
        now,
        now + Duration::from_secs(10),
        now + Duration::from_secs(20),
    )
    .unwrap();
    a.release(&consumer(), token(), now).unwrap();
    assert!(b.protects(&c(4), now));
    assert_eq!(
        b.release(&consumer(), token(), now),
        Err(ReplayError::Ownership)
    );
}
#[test]
fn expired_protection_never_reports_completion_or_allows_resume() {
    let now = Instant::now();
    let mut lease = lease(now);
    let after = now + Duration::from_secs(10);
    assert_eq!(
        lease.advance(&consumer(), token(), c(10), after),
        Err(ReplayError::Expired)
    );
    assert_eq!(lease.state(after), ProtectionState::Expired);
    assert_eq!(
        lease.release(&consumer(), token(), after),
        Err(ReplayError::Expired)
    );
    assert!(!lease.protects(&c(10), after));
}
#[test]
fn renewals_fence_stale_tokens_and_enforce_maximum_lifetime() {
    let now = Instant::now();
    let mut lease = lease(now);
    assert_eq!(
        lease.renew(&consumer(), token(), now + Duration::from_secs(21), now),
        Err(ReplayError::Range)
    );
    let current = lease
        .renew(&consumer(), token(), now + Duration::from_secs(15), now)
        .unwrap();
    assert_ne!(current, token());
    assert_eq!(
        lease.advance(&consumer(), token(), c(4), now),
        Err(ReplayError::Ownership)
    );
    lease.advance(&consumer(), current, c(4), now).unwrap();
}
#[test]
fn protection_coverage_and_resource_failures_are_explicit() {
    let now = Instant::now();
    let mut lease = lease(now);
    assert_eq!(
        lease.update_frontier(c(11), 101, 1, now),
        Err(ReplayError::Capacity)
    );
    assert_eq!(lease.state(now), ProtectionState::Failed);
    let invalid = ProtectionLease::admit(
        token(),
        consumer(),
        ReplayRange::inclusive(c(0), None).unwrap(),
        ReplayRange::inclusive(c(1), Some(c(10))).unwrap(),
        c(10),
        limits(),
        11,
        4096,
        now,
        now + Duration::from_secs(10),
        now + Duration::from_secs(20),
    );
    assert!(matches!(invalid, Err(ReplayError::Unavailable)));
    let mut lease = crate::live::wp06_contracts::lease(now);
    assert_eq!(
        lease.update_frontier(c(11), 1, limits().max_pending_bytes + 1, now),
        Err(ReplayError::Capacity)
    );
}
#[test]
fn replay_page_limits_and_continuation_are_checked() {
    let request = ReplayReadRequest::new(
        ReplayRange::inclusive(c(0), Some(c(3))).unwrap(),
        limits(),
        consumer(),
        token(),
    )
    .unwrap();
    let batch = EventBatch::new(vec![l2_event("s", 1, 0), l2_event("s", 1, 1)]).unwrap();
    let page = ReplayPage::new(
        &request,
        batch.clone(),
        RetentionPositions::new(c(3), None, None).unwrap(),
    )
    .unwrap();
    assert_eq!(page.continuation().unwrap().first(), &c(2));
    assert_eq!(page.continuation().unwrap().last(), Some(&c(3)));
    let request = ReplayReadRequest::new(
        ReplayRange::inclusive(c(0), None).unwrap(),
        DeliveryCapacity::new(1, 1).unwrap(),
        consumer(),
        token(),
    )
    .unwrap();
    assert!(matches!(
        ReplayPage::new(
            &request,
            batch,
            RetentionPositions::new(c(3), None, None).unwrap()
        ),
        Err(ReplayError::Capacity)
    ));
}

/// A retention consumer witness using the SAME public protected read port and
/// versioned codec, not a production storage/retention manager.
struct StoredReplay {
    records: Vec<Vec<u8>>,
    lease: Mutex<ProtectionLease>,
    now: Instant,
}
impl RetentionReaderPort for StoredReplay {
    fn read<'a>(
        &'a self,
        request: ReplayReadRequest,
    ) -> RoutingFuture<'a, Result<ReplayReadOutcome, RetentionReadFailure>> {
        Box::pin(async move {
            let result = (|| -> Result<ReplayReadOutcome, ReplayError> {
                self.lease.lock().unwrap().validate_read(
                    request.owner(),
                    request.protection(),
                    request.range(),
                    self.now,
                )?;
                let mut events = vec![];
                for bytes in &self.records {
                    let e = codec()
                        .decode(bytes)
                        .map_err(|_| ReplayError::SegmentCorrupted)?;
                    if request.range().contains(&e.cursor) {
                        events.push(e);
                    }
                }
                if events.is_empty() {
                    return Err(ReplayError::Unavailable);
                }
                let batch = EventBatch::new(events).map_err(|_| ReplayError::InvalidEvent)?;
                Ok(ReplayReadOutcome::Page(ReplayPage::new(
                    &request,
                    batch,
                    RetentionPositions::new(c(10), Some(c(2)), None).unwrap(),
                )?))
            })();
            result.map_err(|reason| RetentionReadFailure { request, reason })
        })
    }
}
#[test]
fn protected_retention_replay_roundtrip_and_ownership() {
    let now = Instant::now();
    let original: Vec<_> = (0..3).map(|i| group_event(i, i == 0)).collect();
    let reader = StoredReplay {
        records: original
            .iter()
            .map(|e| codec().encode(e).unwrap())
            .collect(),
        lease: Mutex::new(lease(now)),
        now,
    };
    let request = ReplayReadRequest::new(
        ReplayRange::inclusive(c(0), Some(c(2))).unwrap(),
        limits(),
        consumer(),
        token(),
    )
    .unwrap();
    match ready(reader.read(request)) {
        Ok(ReplayReadOutcome::Page(page)) => {
            assert_eq!(page.batch().events(), original.as_slice());
            assert!(page.continuation().is_none());
            assert!(page.positions().durable().is_none());
        }
        other => panic!("unexpected {other:?}"),
    }
    let wrong = ReplayReadRequest::new(
        ReplayRange::inclusive(c(0), None).unwrap(),
        limits(),
        RetentionSessionId("wrong".into()),
        token(),
    )
    .unwrap();
    assert!(matches!(
        ready(reader.read(wrong)),
        Err(RetentionReadFailure {
            reason: ReplayError::Ownership,
            ..
        })
    ));
}
#[test]
fn protected_reads_fail_after_expiry_and_consumption() {
    let now = Instant::now();
    let mut lease = lease(now);
    let range = ReplayRange::inclusive(c(0), Some(c(2))).unwrap();
    lease.advance(&consumer(), token(), c(1), now).unwrap();
    assert_eq!(
        lease.validate_read(&consumer(), token(), &range, now),
        Err(ReplayError::Unavailable)
    );
    assert_eq!(
        lease.validate_read(&consumer(), token(), &range, now + Duration::from_secs(11)),
        Err(ReplayError::Expired)
    );
}
#[test]
fn bounded_atomic_replay_follow_cut_keeps_every_cursor_once() {
    let mut handoff = ReplayCutover::new(c(0), c(2), Some(BookGeneration(1)), limits()).unwrap();
    // Capture is installed at cut=2 BEFORE replay starts.
    for i in 3..6 {
        handoff.capture(&l2_event("s", 1, i)).unwrap();
    }
    handoff.capture(&l2_event("s", 1, 2)).unwrap();
    assert_eq!(handoff.buffered_events(), 3);
    assert_eq!(finish(&mut handoff), Err(ReplayError::Incomplete));
    let mut seen = vec![];
    for i in 0..3 {
        let event = l2_event("s", 1, i);
        handoff.replay(&event).unwrap();
        seen.push(event.cursor.offset);
    }
    finish(&mut handoff).unwrap();
    while let Some(event) = handoff.pop_follow().unwrap() {
        seen.push(event.cursor.offset);
    }
    handoff.capture(&l2_event("s", 1, 6)).unwrap();
    seen.push(handoff.pop_follow().unwrap().unwrap().cursor.offset);
    assert_eq!(seen, (0..7).collect::<Vec<_>>());
    assert_eq!(handoff.state(), CutoverState::Following);
}
#[test]
fn handoff_rejects_gaps_namespace_changes_and_obsolete_book_generation() {
    let mut handoff = ReplayCutover::new(c(0), c(2), Some(BookGeneration(1)), limits()).unwrap();
    assert_eq!(handoff.replay(&l2_event("s", 1, 1)), Err(ReplayError::Gap));
    assert_eq!(handoff.state(), CutoverState::Failed);
    let mut handoff = ReplayCutover::new(c(0), c(2), None, limits()).unwrap();
    assert_eq!(
        handoff.capture(&l2_event("s", 2, 3)),
        Err(ReplayError::Namespace)
    );
    let mut handoff = ReplayCutover::new(c(0), c(2), Some(BookGeneration(2)), limits()).unwrap();
    assert_eq!(
        handoff.capture(&l2_event("s", 1, 3)),
        Err(ReplayError::Generation)
    );
}
#[test]
fn cutover_overflow_and_cancel_preserve_buffered_responsibility() {
    let mut h = ReplayCutover::new(
        c(0),
        c(0),
        None,
        DeliveryCapacity::new(1, 1_000_000).unwrap(),
    )
    .unwrap();
    h.capture(&l2_event("s", 1, 1)).unwrap();
    assert_eq!(h.capture(&l2_event("s", 1, 2)), Err(ReplayError::Capacity));
    assert_eq!(h.into_unresolved()[0].cursor, c(1));
    let mut h = ReplayCutover::new(c(0), c(0), None, limits()).unwrap();
    h.capture(&l2_event("s", 1, 1)).unwrap();
    h.cancel();
    assert_eq!(finish(&mut h), Err(ReplayError::Inactive));
    assert_eq!(h.into_unresolved().len(), 1);
    let mut h =
        ReplayCutover::new(c(0), c(0), None, DeliveryCapacity::new(100, 1).unwrap()).unwrap();
    assert_eq!(h.capture(&l2_event("s", 1, 1)), Err(ReplayError::Capacity));
}
#[test]
fn u64_max_cut_has_no_wrapping_live_successor() {
    let mut h = ReplayCutover::new(c(u64::MAX), c(u64::MAX), None, limits()).unwrap();
    h.replay(&l2_event("s", 1, u64::MAX)).unwrap();
    finish(&mut h).unwrap();
    assert!(h.pop_follow().unwrap().is_none());
    let request = ReplayReadRequest::new(
        ReplayRange::inclusive(c(u64::MAX), None).unwrap(),
        limits(),
        consumer(),
        token(),
    )
    .unwrap();
    let batch = EventBatch::new(vec![l2_event("s", 1, u64::MAX)]).unwrap();
    let page = ReplayPage::new(
        &request,
        batch,
        RetentionPositions::new(c(u64::MAX), None, None).unwrap(),
    )
    .unwrap();
    assert!(page.continuation().is_none());
}

struct FailureSink {
    fail: bool,
}
impl EventDestinationPort for FailureSink {
    fn descriptor(&self) -> DestinationDescriptor {
        DestinationDescriptor {
            id: DestinationId(91),
            kind: DestinationKind::Recording,
            policy: DeliveryPolicy::Required,
            capacity: limits(),
        }
    }
    fn admit<'a>(&'a self, batch: EventBatch) -> RoutingFuture<'a, DestinationAdmission> {
        Box::pin(async move {
            if self.fail {
                DestinationAdmission::Rejected {
                    destination: self.descriptor().id,
                    reason: DeliveryFailure::PersistenceFailed,
                }
            } else {
                DestinationAdmission::Accepted {
                    destination: self.descriptor().id,
                    through: batch.last_cursor().clone(),
                }
            }
        })
    }
}
#[test]
fn failure_injecting_sink_distinguishes_admission_from_persistence() {
    let batch = EventBatch::new(vec![group_event(0, true)]).unwrap();
    let original = batch.clone();
    assert!(
        matches!(ready(FailureSink{fail:false}.admit(batch.clone())),DestinationAdmission::Accepted{through,..} if through==c(0))
    );
    assert!(matches!(
        ready(FailureSink { fail: true }.admit(batch.clone())),
        DestinationAdmission::Rejected {
            reason: DeliveryFailure::PersistenceFailed,
            ..
        }
    ));
    assert_eq!(batch, original);
    // Admission is deliberately not a Committed or Durable receipt.
}
#[test]
fn recorder_encoding_probe_has_one_parent_and_ordered_level_identities() {
    let event = group_event(77, true);
    let record = EventRecordV1::try_from(event.as_ref()).unwrap();
    match record.payload {
        PayloadDtoV1::L2Group { levels, .. } => {
            let identities: Vec<_> = levels
                .iter()
                .enumerate()
                .map(|(ordinal, _)| (record.cursor.clone(), ordinal))
                .collect();
            assert_eq!(
                identities,
                vec![(event.cursor.clone(), 0), (event.cursor.clone(), 1)]
            );
        }
        _ => panic!("not a group"),
    }
    // This is a logical encoding probe. The PostgreSQL DDL mapping remains blocked.
}

#[test]
fn book_view_capture_uses_cursor_boundary_without_fabricated_source_event() {
    let event = group_event(0, true);
    let image = match &event.payload {
        LivePayload::L2Group(g) => g.clone(),
        _ => unreachable!(),
    };
    let view =
        BookViewBoundary::new(c(8), BookGeneration(1), image, BookVersion(14), 2, 3).unwrap();
    let mut handoff = ReplayCutover::for_book(&view, limits()).unwrap();
    handoff.capture(&l2_event("s", 1, 9)).unwrap();
    assert_eq!(handoff.follow_view(), Err(ReplayError::Incomplete));
    handoff.consume_view(&view).unwrap();
    handoff.follow_view().unwrap();
    assert_eq!(handoff.pop_follow().unwrap().unwrap().cursor, c(9));
    assert_eq!(event.cursor, c(0)); // view did not overwrite the accepted source event
    let mut changed = (*l2_event("s", 1, 10)).clone().into_fields();
    changed.provenance.book_generation = Some(BookGeneration(2));
    assert_eq!(
        handoff.capture(&Arc::new(changed.try_into().unwrap())),
        Err(ReplayError::Generation)
    );
}
#[test]
fn expired_protection_prevents_replay_follow_completion() {
    let now = Instant::now();
    let mut protection = lease(now);
    let mut handoff = ReplayCutover::new(c(0), c(2), Some(BookGeneration(1)), limits()).unwrap();
    for offset in 0..3 {
        handoff.replay(&l2_event("s", 1, offset)).unwrap();
    }
    assert_eq!(
        handoff.follow_protected(
            &mut protection,
            &consumer(),
            token(),
            now + Duration::from_secs(10)
        ),
        Err(ReplayError::Expired)
    );
    assert_eq!(handoff.state(), CutoverState::Failed);
}
#[test]
fn valid_protection_handoff_retains_consumer_progress_contract() {
    let now = Instant::now();
    let mut protection = lease(now);
    let mut handoff = ReplayCutover::new(c(0), c(2), None, limits()).unwrap();
    handoff.capture(&l2_event("s", 1, 3)).unwrap();
    for offset in 0..3 {
        handoff.replay(&l2_event("s", 1, offset)).unwrap();
    }
    handoff
        .follow_protected(&mut protection, &consumer(), token(), now)
        .unwrap();
    assert_eq!(protection.acknowledged(), None); // receipt/delivery is not safe consumption
    protection.advance(&consumer(), token(), c(2), now).unwrap();
    assert_eq!(handoff.pop_follow().unwrap().unwrap().cursor, c(3));
}
#[test]
fn lost_epoch_journal_does_not_silently_reinitialize_identity() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("epochs.json");
    let mut store = FileEpochStore::open(&path).unwrap();
    store.fresh_epoch(&StreamId("s".into())).unwrap();
    drop(store);
    std::fs::remove_file(&path).unwrap();
    assert!(FileEpochStore::open(path).is_err());
}
#[test]
fn multiple_stream_owners_use_one_persisted_epoch_store() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = FileEpochStore::open(dir.path().join("epochs.json")).unwrap();
    let first = EventAcceptor::start(&mut store, StreamId("s".into())).unwrap();
    let second = EventAcceptor::start(&mut store, StreamId("other".into())).unwrap();
    assert_ne!(
        first.next_cursor().unwrap().stream_id,
        second.next_cursor().unwrap().stream_id
    );
}
#[test]
fn duplicated_native_identity_fields_cannot_disagree() {
    let mut event = trade_event().into_fields();
    event.provenance.native_trade_id = Some(marketforge_engine::live::model::NativeTradeId(
        "conflict".into(),
    ));
    assert_eq!(event.check(), Err(LiveEventError::NativeIdentityMismatch));
}

#[test]
fn admission_rejects_unbounded_coverage_and_insufficient_initial_reservation() {
    let now = Instant::now();
    for (covered, events, bytes, expected) in [
        (
            ReplayRange::inclusive(c(0), None).unwrap(),
            11,
            4096,
            ReplayError::Unavailable,
        ),
        (
            ReplayRange::inclusive(c(0), Some(c(10))).unwrap(),
            101,
            4096,
            ReplayError::Capacity,
        ),
        (
            ReplayRange::inclusive(c(0), Some(c(10))).unwrap(),
            11,
            1_000_001,
            ReplayError::Capacity,
        ),
    ] {
        assert!(
            matches!(ProtectionLease::admit(token(),consumer(),ReplayRange::inclusive(c(0),None).unwrap(),covered,c(10),limits(),events,bytes,now,now+Duration::from_secs(10),now+Duration::from_secs(20)),Err(error) if error==expected)
        );
    }
}
#[test]
fn exact_codec_rejects_invalid_decimals_and_invalid_decoded_payloads() {
    let original: serde_json::Value =
        serde_json::from_slice(&codec().encode(&trade_event()).unwrap()).unwrap();
    for invalid in [
        serde_json::json!(1.25),
        serde_json::json!("0"),
        serde_json::json!("-1"),
        serde_json::json!("0.00000000000000000000000000001"),
    ] {
        let mut value = original.clone();
        value["payload"]["trade"]["price"] = invalid;
        assert!(
            codec()
                .decode(&serde_json::to_vec(&value).unwrap())
                .is_err()
        );
    }
    let empty = BookStore::new();
    let e = group_event(0, true);
    let source = match &e.payload {
        LivePayload::L2Group(g) => g.source.clone(),
        _ => unreachable!(),
    };
    assert_eq!(
        LiveL2Group::from_book(&empty, envelope(), source, DepthSourceOperation::Snapshot),
        Err(LiveEventError::InvalidInitialization)
    );
}
/// The fake source owner serializes append and capture registration under ONE
/// lock. This proves the capture/cut relationship without implementing WP-10.
#[test]
fn serialized_owner_installs_capture_at_the_same_cut_it_reports() {
    struct Owner {
        next: u64,
        capture: Option<Arc<Mutex<ReplayCutover>>>,
    }
    impl Owner {
        fn install(&mut self) -> Arc<Mutex<ReplayCutover>> {
            let cut = c(self.next - 1);
            let capture = Arc::new(Mutex::new(
                ReplayCutover::new(c(0), cut, None, limits()).unwrap(),
            ));
            self.capture = Some(capture.clone());
            capture
        }
        fn append(&mut self) {
            let event = l2_event("s", 1, self.next);
            if let Some(capture) = &self.capture {
                capture.lock().unwrap().capture(&event).unwrap();
            }
            self.next += 1;
        }
    }
    let owner = Mutex::new(Owner {
        next: 3,
        capture: None,
    });
    let capture = owner.lock().unwrap().install();
    owner.lock().unwrap().append();
    for i in 0..3 {
        capture
            .lock()
            .unwrap()
            .replay(&l2_event("s", 1, i))
            .unwrap();
    }
    finish(&mut capture.lock().unwrap()).unwrap();
    assert_eq!(
        capture
            .lock()
            .unwrap()
            .pop_follow()
            .unwrap()
            .unwrap()
            .cursor,
        c(3)
    );
}

#[test]
fn v1_golden_fixture_preserves_exact_scale_optional_fields_and_u64_identities() {
    let golden = include_bytes!("fixtures/wp06-event-v1.json");
    let event = codec().decode(golden).unwrap();
    assert_eq!(event.cursor.offset, u64::MAX);
    assert_eq!(event.provenance.origin, EventOrigin::Rest);
    if let LivePayload::Trade(trade) = &event.payload {
        assert_eq!(trade.price.scale(), 4);
        assert_eq!(trade.sequence, Some(u64::MAX));
        assert_eq!(trade.trade_id.as_deref(), Some("opaque:0001"));
    } else {
        panic!("wrong payload")
    }
    let original: serde_json::Value = serde_json::from_slice(golden).unwrap();
    let encoded: serde_json::Value =
        serde_json::from_slice(&codec().encode(&event).unwrap()).unwrap();
    assert_eq!(encoded, original);
}

#[test]
fn retention_failure_retains_exact_request_context() {
    let now = Instant::now();
    let reader = StoredReplay {
        records: vec![b"corrupted".to_vec()],
        lease: Mutex::new(lease(now)),
        now,
    };
    let request = ReplayReadRequest::new(
        ReplayRange::inclusive(c(0), None).unwrap(),
        limits(),
        consumer(),
        token(),
    )
    .unwrap();
    let error = ready(reader.read(request)).unwrap_err();
    assert_eq!(error.reason, ReplayError::SegmentCorrupted);
    assert_eq!(error.request.range().first(), &c(0));
    assert_eq!(error.request.protection(), token());
    assert_eq!(error.request.owner(), &consumer());
}
#[test]
fn end_of_available_data_does_not_hide_an_earlier_gap() {
    let future = ReplayReadRequest::new(
        ReplayRange::inclusive(c(4), None).unwrap(),
        limits(),
        consumer(),
        token(),
    )
    .unwrap();
    let positions = RetentionPositions::new(c(3), None, None).unwrap();
    assert!(matches!(
        ReplayReadOutcome::at_frontier(&future, positions.clone()),
        Ok(ReplayReadOutcome::AtFrontier(_))
    ));
    let missing = ReplayReadRequest::new(
        ReplayRange::inclusive(c(2), None).unwrap(),
        limits(),
        consumer(),
        token(),
    )
    .unwrap();
    assert!(matches!(
        ReplayReadOutcome::at_frontier(&missing, positions),
        Err(ReplayError::Unavailable)
    ));
}
#[test]
fn cancellation_and_storage_failure_invalidate_retention_promises() {
    let now = Instant::now();
    let mut a = lease(now);
    a.cancel(&consumer(), token(), now).unwrap();
    a.cancel(&consumer(), token(), now).unwrap();
    assert_eq!(a.state(now), ProtectionState::Cancelled);
    assert!(!a.protects(&c(0), now));
    assert_eq!(
        a.advance(&consumer(), token(), c(1), now),
        Err(ReplayError::Cancelled)
    );
    let mut b = lease(now);
    b.invalidate();
    assert_eq!(b.state(now), ProtectionState::Failed);
    assert!(!b.protects(&c(1), now));
}

// These state-machine tests use a bounded synthetic owner proof. Integration
// witnesses above create protection before reading and preserve it through cut.
fn finish(capture: &mut ReplayCutover) -> Result<(), ReplayError> {
    let now = Instant::now();
    let first = capture.first_cursor().clone();
    let cut = capture.cut().clone();
    let count = usize::try_from(cut.offset - first.offset + 1).unwrap();
    let covered = ReplayRange::inclusive(first.clone(), Some(cut.clone())).unwrap();
    let mut protection = ProtectionLease::admit(
        token(),
        consumer(),
        covered.clone(),
        covered,
        cut,
        limits(),
        count,
        4096,
        now,
        now + Duration::from_secs(1),
        now + Duration::from_secs(2),
    )
    .unwrap();
    capture.follow_protected(&mut protection, &consumer(), token(), now)
}

#[test]
fn finite_protection_cannot_authorize_an_open_ended_read() {
    let now = Instant::now();
    let covered = ReplayRange::inclusive(c(0), Some(c(2))).unwrap();
    let mut p = ProtectionLease::admit(
        token(),
        consumer(),
        covered.clone(),
        covered,
        c(2),
        limits(),
        3,
        4096,
        now,
        now + Duration::from_secs(1),
        now + Duration::from_secs(2),
    )
    .unwrap();
    assert_eq!(
        p.validate_read(
            &consumer(),
            token(),
            &ReplayRange::inclusive(c(0), None).unwrap(),
            now
        ),
        Err(ReplayError::Unavailable)
    );
}
