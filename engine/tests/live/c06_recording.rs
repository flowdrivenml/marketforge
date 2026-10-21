//! C06 mapping witnesses for the observed schema; no database mutations.
use super::support::live_events::l2_event;
use marketforge_engine::{
    canonical::{BookSide, EventEnvelope, Exchange, L2LevelUpdate},
    live::{
        model::{
            cursor::{LogEpoch, StreamCursor},
            delivery::{DeliveryCapacity, DeliveryFailure, DeliveryStage, DeliveryStatus},
            demand::RetentionSessionId,
            identity::StreamId,
            replay::{
                ProtectionLease, ProtectionToken, ReplayError, ReplayRange, RetentionPositions,
            },
        },
        ports::{retention::ReplayReadRequest, routing::EventBatch},
        recording::{
            commit::{
                CommitBoundary, RecordingTransactionId, RecordingWritePlan, TransactionOutcome,
            },
            coverage::CommittedCoverage,
            idempotency::{ExistingEvent, RetryDisposition, reconcile},
            reader::{cleanup_allowed, decode_page},
            schema::{
                MappingError, SchemaSnapshot, StoredEvent, StreamKind, StreamRow, checked_ordinal,
            },
        },
    },
};
use rust_decimal::Decimal;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
fn c(offset: u64) -> StreamCursor {
    StreamCursor::new(StreamId("s".into()), LogEpoch(1), offset)
}
fn stream() -> StreamRow {
    StreamRow {
        stream_id: StreamId("s".into()),
        instrument_id: Some(7),
        stream_kind: StreamKind::L2,
    }
}
fn row(offset: u64) -> StoredEvent {
    let mut fields = (*l2_event("s", 1, offset)).clone().into_fields();
    if let marketforge_engine::live::model::LivePayload::L2Group(g) = &mut fields.payload {
        for (side, price, quantity) in [(BookSide::Bid, 100, 2), (BookSide::Ask, 101, 0)] {
            g.grouped_levels.push(L2LevelUpdate {
                envelope: EventEnvelope {
                    event_timestamp_ns: g.source.event_timestamp_ns,
                    system_timestamp_ns: None,
                    exchange: Exchange::Bybit,
                    instrument_id: 7,
                    symbol: "BTCUSDT".into(),
                    stream_id: "canonical-source".into(),
                },
                side,
                price: Decimal::from(price),
                quantity_base: Some(Decimal::from(quantity)),
                quantity_quote: None,
                quantity_contracts: None,
                order_count: Some(u64::MAX),
            });
        }
    }
    StoredEvent::encode(&fields.try_into().unwrap(), &stream()).unwrap()
}
fn range(a: u64, b: u64) -> ReplayRange {
    ReplayRange::inclusive(c(a), Some(c(b))).unwrap()
}
#[test]
fn actual_catalog_snapshot_matches_required_schema() {
    let snapshot: SchemaSnapshot =
        serde_json::from_str(include_str!("fixtures/canonical-schema-observed.json")).unwrap();
    snapshot.validate().unwrap();
}
#[test]
fn altered_columns_or_checks_are_rejected() {
    let mut snapshot: SchemaSnapshot =
        serde_json::from_str(include_str!("fixtures/canonical-schema-observed.json")).unwrap();
    snapshot
        .columns
        .iter_mut()
        .find(|c| c.column_name == "log_epoch")
        .unwrap()
        .numeric_precision = Some(19);
    assert_eq!(snapshot.validate(), Err(MappingError::SchemaMismatch));
    let mut snapshot: SchemaSnapshot =
        serde_json::from_str(include_str!("fixtures/canonical-schema-observed.json")).unwrap();
    snapshot
        .constraints
        .iter_mut()
        .find(|c| c.name == "identity_epoch_uint64")
        .unwrap()
        .definition = "CHECK (true)".into();
    assert_eq!(snapshot.validate(), Err(MappingError::SchemaMismatch));
}
#[test]
fn grouped_row_maps_one_parent_and_ordered_children() {
    let original = row(u64::MAX);
    assert_eq!(original.event.cursor.offset, u64::MAX);
    assert_eq!(original.levels.len(), 2);
    assert_eq!(original.levels[0].level_ordinal, 0);
    assert_eq!(original.levels[1].level_ordinal, 1);
    assert_eq!(original.levels[0].action, "set");
    assert_eq!(original.levels[1].action, "delete");
    assert_eq!(original.levels[1].quantity_quote, None);
    assert_eq!(original.levels[1].order_count, Some(u64::MAX));
    let decoded = original.decode().unwrap();
    assert_eq!(StoredEvent::encode(&decoded, &stream()).unwrap(), original);
}
#[test]
fn contradictory_sql_columns_or_levels_never_override_json() {
    let original = row(0);
    let mut bad = original.clone();
    bad.event.received_timestamp_ns += 1;
    assert_eq!(bad.decode(), Err(MappingError::ColumnMismatch));
    let mut bad = original.clone();
    bad.levels.swap(0, 1);
    assert_eq!(bad.decode(), Err(MappingError::ColumnMismatch));
    let mut bad = original;
    bad.levels.pop();
    assert_eq!(bad.decode(), Err(MappingError::ColumnMismatch));
}
#[test]
fn unsupported_signed_timestamp_is_reported_without_changing_canonical_values() {
    let e = row(0).decode().unwrap();
    let mut fields = e.into_fields();
    fields.timing.received_timestamp_ns = -1;
    let e = fields.try_into().unwrap();
    assert_eq!(
        StoredEvent::encode(&e, &stream()),
        Err(MappingError::UnsupportedTimestamp)
    );
    assert_eq!(e.timing.received_timestamp_ns, -1);
}
#[test]
fn ordinal_overflow_and_wrong_stream_metadata_are_explicit() {
    assert_eq!(checked_ordinal(i32::MAX as usize), Ok(i32::MAX));
    assert_eq!(
        checked_ordinal(i32::MAX as usize + 1),
        Err(MappingError::OrdinalOverflow)
    );
    let e = row(0).decode().unwrap();
    let mut s = stream();
    s.instrument_id = Some(8);
    assert_eq!(
        StoredEvent::encode(&e, &s),
        Err(MappingError::StreamMismatch)
    );
}
#[test]
fn retry_compares_content_and_preserves_ambiguous_identity_only_state() {
    let expected = row(0);
    assert_eq!(
        reconcile(&expected, ExistingEvent::Absent),
        RetryDisposition::ResubmitMissing
    );
    assert_eq!(
        reconcile(&expected, ExistingEvent::IdentityOnly),
        RetryDisposition::Indeterminate
    );
    assert_eq!(
        reconcile(&expected, ExistingEvent::Recorded(&expected)),
        RetryDisposition::AlreadyCommitted
    );
    let mut corrupted = expected.clone();
    corrupted.levels.clear();
    assert_eq!(
        reconcile(&expected, ExistingEvent::Recorded(&corrupted)),
        RetryDisposition::Indeterminate
    );
    let mut fields = expected.decode().unwrap().into_fields();
    fields.timing.received_timestamp_ns += 1;
    let conflict = StoredEvent::encode(&fields.try_into().unwrap(), &stream()).unwrap();
    assert_eq!(
        reconcile(&expected, ExistingEvent::Recorded(&conflict)),
        RetryDisposition::Conflict
    );
}
#[test]
fn coverage_waits_for_gaps_in_out_of_order_commits() {
    let mut coverage = CommittedCoverage::new(8).unwrap();
    coverage.observe(&range(5, 9)).unwrap();
    assert_eq!(coverage.contiguous_through(&c(0)), None);
    coverage.observe(&range(0, 2)).unwrap();
    assert_eq!(coverage.contiguous_through(&c(0)), Some(c(2)));
    coverage.observe(&range(3, 4)).unwrap();
    assert_eq!(coverage.contiguous_through(&c(0)), Some(c(9)));
    coverage.observe(&range(u64::MAX, u64::MAX)).unwrap();
    assert_eq!(coverage.contiguous_through(&c(u64::MAX)), Some(c(u64::MAX)));
}
#[test]
fn failed_tracking_capacity_is_atomic() {
    let mut coverage = CommittedCoverage::new(1).unwrap();
    assert_eq!(
        coverage.observe_batch(&[range(0, 0), range(2, 2)]),
        Err(ReplayError::Capacity)
    );
    assert_eq!(coverage.contiguous_through(&c(0)), None);
}
#[test]
fn unknown_transactions_do_not_create_committed_coverage() {
    let batch = EventBatch::new(vec![
        Arc::new(row(0).decode().unwrap()),
        Arc::new(row(2).decode().unwrap()),
    ])
    .unwrap();
    let plan = RecordingWritePlan::new(RecordingTransactionId(7), stream(), &batch).unwrap();
    assert_eq!(plan.ranges().len(), 2);
    let mut coverage = CommittedCoverage::new(8).unwrap();
    assert_eq!(
        plan.apply_outcome(TransactionOutcome::Unknown, &mut coverage)
            .unwrap(),
        DeliveryStatus::Failed(DeliveryFailure::CommitOutcomeUnknown)
    );
    assert_eq!(coverage.contiguous_through(&c(0)), None);
    assert_eq!(
        plan.apply_outcome(
            TransactionOutcome::Acknowledged(CommitBoundary::Committed),
            &mut coverage
        )
        .unwrap(),
        DeliveryStatus::Progress(DeliveryStage::Committed)
    );
    assert_eq!(coverage.contiguous_through(&c(0)), Some(c(0)));
    assert_ne!(
        TransactionOutcome::Acknowledged(CommitBoundary::Durable).delivery_status(),
        DeliveryStatus::Progress(DeliveryStage::Committed)
    );
}
#[test]
fn protected_recorded_read_and_cleanup_respect_the_same_lease() {
    let now = Instant::now();
    let token = ProtectionToken {
        id: 3,
        generation: 1,
    };
    let owner = RetentionSessionId("bootstrap".into());
    let bounds = range(0, 1);
    let limit = DeliveryCapacity::new(10, 100_000).unwrap();
    let mut lease = ProtectionLease::admit(
        token,
        owner.clone(),
        bounds.clone(),
        bounds.clone(),
        c(1),
        limit,
        2,
        10000,
        now,
        now + Duration::from_secs(10),
        now + Duration::from_secs(20),
    )
    .unwrap();
    let request = ReplayReadRequest::new(bounds.clone(), limit, owner.clone(), token).unwrap();
    let positions = RetentionPositions::new(c(1), Some(c(1)), None).unwrap();
    let page = decode_page(&request, &mut lease, &[row(0), row(1)], positions, now).unwrap();
    assert_eq!(page.batch().last_cursor(), &c(1));
    assert_eq!(
        cleanup_allowed(&bounds, std::slice::from_mut(&mut lease), now),
        Err(ReplayError::Ownership)
    );
    lease.advance(&owner, token, c(1), now).unwrap();
    assert!(cleanup_allowed(&bounds, std::slice::from_mut(&mut lease), now).is_ok());
}
#[test]
fn missing_committed_positions_and_phantom_coverage_do_not_authorize_read() {
    let mut coverage = CommittedCoverage::new(2).unwrap();
    coverage.observe(&range(0, 9)).unwrap();
    let batch = EventBatch::new(vec![
        Arc::new(row(0).decode().unwrap()),
        Arc::new(row(2).decode().unwrap()),
    ])
    .unwrap();
    assert_eq!(range(0, 9).check_page(&batch), Err(ReplayError::Gap));
}
