#![cfg(feature = "historical")]

use arrow_array::{Array, Int64Array, UInt8Array, UInt32Array, UInt64Array};

use marketforge_engine::historical::merge::{
    MergeEventRef, MergedEvent, TradeEventRef, UNIFIED_TIMELINE_VERSION,
    UnifiedTimelineBatchBuilder, UnifiedTimelineRecord, unified_timeline_schema,
};
// -----------------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------------

fn trade_event(
    timestamp_ns: i64,
    stream_rank: u32,
    dataset_id: u64,
    task_id: u64,
    row_offset: u64,
) -> MergedEvent {
    MergedEvent {
        stream_rank,
        event: MergeEventRef::Trade(TradeEventRef {
            dataset_id,
            task_id,
            event_ordinal: row_offset,
            canonical_row_offset: row_offset,
            event_timestamp_ns: timestamp_ns,
        }),
    }
}

#[test]
fn validates_unified_timeline_schema() {
    let schema = unified_timeline_schema();

    assert_eq!(UNIFIED_TIMELINE_VERSION, 1);
    assert_eq!(schema.fields().len(), 10);

    let expected = [
        "event_timestamp_ns",
        "stream_rank",
        "event_kind",
        "dataset_id",
        "task_id",
        "event_ordinal",
        "canonical_row_offset",
        "canonical_row_count",
        "event_boundary",
        "source_operation",
    ];

    for (index, name) in expected.iter().enumerate() {
        assert_eq!(schema.field(index).name(), *name);
    }

    for index in 0..8 {
        assert!(
            !schema.field(index).is_nullable(),
            "column {} should not be nullable",
            schema.field(index).name()
        );
    }

    assert!(schema.field(8).is_nullable());
    assert!(schema.field(9).is_nullable());
}

// -----------------------------------------------------------------------------
// Trade event conversion
// -----------------------------------------------------------------------------

#[test]
fn converts_trade_event_to_unified_record() {
    let merged = trade_event(1_788_220_800_000_000_000, 2, 111, 3, 42);

    let record = UnifiedTimelineRecord::from(&merged);

    assert_eq!(record.event_timestamp_ns, merged.event.timestamp_ns());
    assert_eq!(record.stream_rank, 2);
    assert_eq!(record.event_kind, 0);

    assert_eq!(record.dataset_id, 111);
    assert_eq!(record.task_id, 3);
    assert_eq!(record.event_ordinal, 42);

    assert_eq!(record.canonical_row_offset, 42);
    assert_eq!(record.canonical_row_count, 1);

    assert_eq!(record.event_boundary, None);
    assert_eq!(record.source_operation, None);
}

// -----------------------------------------------------------------------------
// Arrow batch validation
// -----------------------------------------------------------------------------

#[test]
fn writes_trade_records_to_arrow_batch() {
    let mut builder = UnifiedTimelineBatchBuilder::new();

    let events = [
        trade_event(100, 0, 111, 0, 10),
        trade_event(200, 1, 109, 0, 20),
        trade_event(300, 0, 111, 0, 11),
    ];

    for event in &events {
        builder.push(UnifiedTimelineRecord::from(event));
    }

    assert_eq!(builder.len(), 3);

    let batch = builder
        .finish()
        .expect("build unified batch")
        .expect("nonempty batch");

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 10);
    assert!(builder.is_empty());

    let timestamps = batch
        .column_by_name("event_timestamp_ns")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    assert_eq!(timestamps.values().as_ref(), &[100, 200, 300]);

    let ranks = batch
        .column_by_name("stream_rank")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt32Array>()
        .unwrap();

    assert_eq!(ranks.values().as_ref(), &[0, 1, 0]);

    let kinds = batch
        .column_by_name("event_kind")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();

    assert_eq!(kinds.values().as_ref(), &[0, 0, 0]);

    let offsets = batch
        .column_by_name("canonical_row_offset")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();

    assert_eq!(offsets.values().as_ref(), &[10, 20, 11]);

    let row_counts = batch
        .column_by_name("canonical_row_count")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();

    assert_eq!(row_counts.values().as_ref(), &[1, 1, 1]);

    let boundaries = batch
        .column_by_name("event_boundary")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();

    let operations = batch
        .column_by_name("source_operation")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();

    assert_eq!(boundaries.null_count(), 3);
    assert_eq!(operations.null_count(), 3);
}

// -----------------------------------------------------------------------------
// Empty batch behavior
// -----------------------------------------------------------------------------

#[test]
fn handles_empty_unified_batch() {
    let mut builder = UnifiedTimelineBatchBuilder::new();

    assert!(builder.is_empty());
    assert_eq!(builder.len(), 0);

    assert!(builder.finish().unwrap().is_none());
}

// -----------------------------------------------------------------------------
// Builder reuse
// -----------------------------------------------------------------------------

#[test]
fn reuses_unified_batch_builder() {
    let mut builder = UnifiedTimelineBatchBuilder::new();

    builder.push(UnifiedTimelineRecord::from(&trade_event(100, 0, 111, 0, 0)));

    let first = builder.finish().unwrap().unwrap();

    assert_eq!(first.num_rows(), 1);
    assert!(builder.is_empty());

    builder.push(UnifiedTimelineRecord::from(&trade_event(200, 0, 111, 0, 1)));

    let second = builder.finish().unwrap().unwrap();

    assert_eq!(second.num_rows(), 1);
    assert!(builder.is_empty());
}
