use std::{fs::File, path::Path, sync::Arc};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, UInt8Array, UInt64Array};
use parquet::arrow::ArrowWriter;

use marketforge_engine::historical::normalize::{
    merge::{DepthMetadataResolver, UnifiedTimelineRecord},
    parquet::depth_event_schema,
};

// -----------------------------------------------------------------------------
// Synthetic depth event index
// -----------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct TestEvent {
    ordinal: u64,
    timestamp: i64,
    system_timestamp: Option<i64>,
    offset: u64,
    count: u64,
    sequence_start: Option<u64>,
    sequence_end: Option<u64>,
    boundary: u8,
    operation: u8,
}

fn event(ordinal: u64, timestamp: i64, offset: u64, count: u64) -> TestEvent {
    TestEvent {
        ordinal,
        timestamp,
        system_timestamp: Some(timestamp + 5),
        offset,
        count,
        sequence_start: Some(ordinal * 10),
        sequence_end: Some(ordinal * 10 + 1),
        boundary: 0,
        operation: 1,
    }
}

fn write_events(path: &Path, events: &[TestEvent]) {
    let schema = depth_event_schema();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(UInt64Array::from(
            events.iter().map(|e| e.ordinal).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            events.iter().map(|e| e.offset).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            events.iter().map(|e| e.count).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            events.iter().map(|e| e.timestamp).collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            events
                .iter()
                .map(|e| e.system_timestamp)
                .collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            events.iter().map(|e| e.sequence_start).collect::<Vec<_>>(),
        )),
        Arc::new(UInt64Array::from(
            events.iter().map(|e| e.sequence_end).collect::<Vec<_>>(),
        )),
        Arc::new(UInt8Array::from(
            events.iter().map(|e| e.boundary).collect::<Vec<_>>(),
        )),
        Arc::new(UInt8Array::from(
            events.iter().map(|e| e.operation).collect::<Vec<_>>(),
        )),
    ];

    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();

    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

// -----------------------------------------------------------------------------
// Logical references
// -----------------------------------------------------------------------------

fn reference(dataset_id: u64, task_id: u64, source: TestEvent) -> UnifiedTimelineRecord {
    UnifiedTimelineRecord {
        event_timestamp_ns: source.timestamp,
        stream_rank: 0,
        event_kind: 1,
        dataset_id,
        task_id,
        event_ordinal: source.ordinal,
        canonical_row_offset: source.offset,
        canonical_row_count: source.count,
        event_boundary: Some(source.boundary),
        source_operation: Some(source.operation),
    }
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[test]
fn resolves_consecutive_ordinals() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.parquet");

    let events = vec![
        event(0, 100, 0, 2),
        event(1, 200, 2, 1),
        event(2, 300, 3, 3),
    ];

    write_events(&path, &events);

    let mut resolver = DepthMetadataResolver::new();
    resolver.register_task(10, 0, &path).unwrap();

    for source in events {
        let metadata = resolver.resolve(&reference(10, 0, source)).unwrap();

        assert_eq!(metadata.system_timestamp_ns, Some(source.timestamp + 5));

        assert_eq!(metadata.sequence_start, source.sequence_start);

        assert_eq!(metadata.sequence_end, source.sequence_end);
    }
}

#[test]
fn resolves_gaps_in_source_ordinals() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.parquet");

    let events = vec![
        event(10, 100, 0, 1),
        event(15, 200, 1, 1),
        event(30, 300, 2, 1),
    ];

    write_events(&path, &events);

    let mut resolver = DepthMetadataResolver::new();
    resolver.register_task(10, 0, &path).unwrap();

    for source in events {
        resolver.resolve(&reference(10, 0, source)).unwrap();
    }
}

#[test]
fn skips_unrequested_source_events() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.parquet");

    let events = vec![
        event(0, 100, 0, 1),
        event(1, 200, 1, 1),
        event(2, 300, 2, 1),
        event(3, 400, 3, 1),
    ];

    write_events(&path, &events);

    let mut resolver = DepthMetadataResolver::new();
    resolver.register_task(10, 0, &path).unwrap();

    // Simulates events excluded by the logical merge.
    resolver.resolve(&reference(10, 0, events[0])).unwrap();
    resolver.resolve(&reference(10, 0, events[3])).unwrap();
}

#[test]
fn preserves_zero_level_event_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.parquet");

    let source = event(5, 500, 100, 0);

    write_events(&path, &[source]);

    let mut resolver = DepthMetadataResolver::new();
    resolver.register_task(10, 0, &path).unwrap();

    let metadata = resolver.resolve(&reference(10, 0, source)).unwrap();

    assert_eq!(metadata.system_timestamp_ns, Some(505));
    assert_eq!(metadata.sequence_start, Some(50));
    assert_eq!(metadata.sequence_end, Some(51));
}

#[test]
fn rejects_backward_ordinal_requests() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.parquet");

    let events = vec![event(1, 100, 0, 1), event(2, 200, 1, 1)];

    write_events(&path, &events);

    let mut resolver = DepthMetadataResolver::new();
    resolver.register_task(10, 0, &path).unwrap();

    resolver.resolve(&reference(10, 0, events[1])).unwrap();

    assert!(resolver.resolve(&reference(10, 0, events[0])).is_err());
}

#[test]
fn rejects_missing_source_ordinal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.parquet");

    let events = vec![event(1, 100, 0, 1), event(3, 300, 1, 1)];

    write_events(&path, &events);

    let mut resolver = DepthMetadataResolver::new();
    resolver.register_task(10, 0, &path).unwrap();

    let missing = event(2, 200, 1, 0);

    assert!(resolver.resolve(&reference(10, 0, missing)).is_err());

    // The cursor must retain ordinal 3 after failing to find ordinal 2.
    resolver.resolve(&reference(10, 0, events[1])).unwrap();
}

#[test]
fn rejects_incorrect_canonical_row_range() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.parquet");

    let source = event(1, 100, 0, 5);

    write_events(&path, &[source]);

    let mut resolver = DepthMetadataResolver::new();
    resolver.register_task(10, 0, &path).unwrap();

    let mut invalid = reference(10, 0, source);
    invalid.canonical_row_count = 4;

    assert!(resolver.resolve(&invalid).is_err());
}

#[test]
fn rejects_timestamp_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.parquet");

    let source = event(1, 100, 0, 1);

    write_events(&path, &[source]);

    let mut resolver = DepthMetadataResolver::new();
    resolver.register_task(10, 0, &path).unwrap();

    let mut invalid = reference(10, 0, source);
    invalid.event_timestamp_ns = 999;

    assert!(resolver.resolve(&invalid).is_err());
}

#[test]
fn rejects_boundary_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.parquet");

    let source = event(1, 100, 0, 1);

    write_events(&path, &[source]);

    let mut resolver = DepthMetadataResolver::new();
    resolver.register_task(10, 0, &path).unwrap();

    let mut invalid = reference(10, 0, source);
    invalid.event_boundary = Some(1);

    assert!(resolver.resolve(&invalid).is_err());
}

#[test]
fn rejects_source_operation_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.parquet");

    let source = event(1, 100, 0, 1);

    write_events(&path, &[source]);

    let mut resolver = DepthMetadataResolver::new();
    resolver.register_task(10, 0, &path).unwrap();

    let mut invalid = reference(10, 0, source);
    invalid.source_operation = Some(2);

    assert!(resolver.resolve(&invalid).is_err());
}

#[test]
fn supports_interleaved_source_tasks() {
    let dir = tempfile::tempdir().unwrap();

    let first_path = dir.path().join("first.parquet");
    let second_path = dir.path().join("second.parquet");

    let first = [event(0, 100, 0, 1), event(1, 300, 1, 1)];

    let second = [event(0, 200, 0, 1), event(1, 400, 1, 1)];

    write_events(&first_path, &first);
    write_events(&second_path, &second);

    let mut resolver = DepthMetadataResolver::new();

    resolver.register_task(10, 0, &first_path).unwrap();
    resolver.register_task(20, 0, &second_path).unwrap();

    let references = [
        reference(10, 0, first[0]),
        reference(20, 0, second[0]),
        reference(10, 0, first[1]),
        reference(20, 0, second[1]),
    ];

    let metadata = resolver.resolve_batch(&references).unwrap();

    assert_eq!(metadata.len(), 4);
    assert_eq!(resolver.registered_tasks(), 2);

    assert_eq!(metadata[0].system_timestamp_ns, Some(105));
    assert_eq!(metadata[1].system_timestamp_ns, Some(205));
    assert_eq!(metadata[2].system_timestamp_ns, Some(305));
    assert_eq!(metadata[3].system_timestamp_ns, Some(405));
}

#[test]
fn rejects_duplicate_task_registration() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.parquet");

    write_events(&path, &[event(0, 100, 0, 1)]);

    let mut resolver = DepthMetadataResolver::new();

    resolver.register_task(10, 0, &path).unwrap();

    assert!(resolver.register_task(10, 0, &path).is_err());
}

#[test]
fn rejects_trade_references() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.parquet");

    let source = event(0, 100, 0, 1);

    write_events(&path, &[source]);

    let mut resolver = DepthMetadataResolver::new();
    resolver.register_task(10, 0, &path).unwrap();

    let mut trade = reference(10, 0, source);
    trade.event_kind = 0;

    assert!(resolver.resolve(&trade).is_err());
}

// -----------------------------------------------------------------------------
// Partition-aware depth metadata tests
// -----------------------------------------------------------------------------

#[test]
fn resolves_late_partition_without_earlier_events() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.parquet");

    let events = vec![
        event(0, 100, 0, 1),
        event(1, 200, 1, 1),
        event(2, 300, 2, 1),
        event(3, 400, 3, 1),
        event(4, 500, 4, 1),
    ];

    write_events(&path, &events);

    let mut resolver = DepthMetadataResolver::new();

    resolver
        .register_task_range(10, 0, &path, 300, 600)
        .unwrap();

    for source in &events[2..] {
        let metadata = resolver.resolve(&reference(10, 0, *source)).unwrap();

        assert_eq!(metadata.system_timestamp_ns, source.system_timestamp);
    }
}

#[test]
fn indexed_range_preserves_ordinal_gaps() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.parquet");

    let events = vec![
        event(10, 100, 0, 1),
        event(20, 200, 1, 1),
        event(40, 300, 2, 1),
        event(80, 400, 3, 1),
    ];

    write_events(&path, &events);

    let mut resolver = DepthMetadataResolver::new();

    resolver
        .register_task_range(10, 0, &path, 200, 500)
        .unwrap();

    resolver.resolve(&reference(10, 0, events[1])).unwrap();

    resolver.resolve(&reference(10, 0, events[3])).unwrap();
}

#[test]
fn indexed_range_supports_zero_level_events() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.parquet");

    let events = vec![
        event(0, 100, 0, 1),
        event(1, 200, 1, 0),
        event(2, 300, 1, 1),
    ];

    write_events(&path, &events);

    let mut resolver = DepthMetadataResolver::new();

    resolver
        .register_task_range(10, 0, &path, 200, 400)
        .unwrap();

    let metadata = resolver.resolve(&reference(10, 0, events[1])).unwrap();

    assert_eq!(metadata.system_timestamp_ns, Some(205));
    assert_eq!(metadata.sequence_start, Some(10));
    assert_eq!(metadata.sequence_end, Some(11));
}

#[test]
fn indexed_range_rejects_invalid_interval() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.parquet");

    write_events(&path, &[event(0, 100, 0, 1)]);

    let mut resolver = DepthMetadataResolver::new();

    assert!(
        resolver
            .register_task_range(10, 0, &path, 300, 300)
            .is_err()
    );

    assert!(
        resolver
            .register_task_range(10, 0, &path, 400, 300)
            .is_err()
    );
}

#[test]
fn indexed_range_rejects_duplicate_registration() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.parquet");

    write_events(&path, &[event(0, 100, 0, 1)]);

    let mut resolver = DepthMetadataResolver::new();

    resolver
        .register_task_range(10, 0, &path, 100, 200)
        .unwrap();

    assert!(
        resolver
            .register_task_range(10, 0, &path, 100, 200)
            .is_err()
    );

    assert!(resolver.register_task(10, 0, &path).is_err());
}

#[test]
fn indexed_range_supports_empty_partition() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.parquet");

    write_events(&path, &[event(0, 100, 0, 1), event(1, 200, 1, 1)]);

    let mut resolver = DepthMetadataResolver::new();

    resolver
        .register_task_range(10, 0, &path, 500, 600)
        .unwrap();

    assert_eq!(resolver.registered_tasks(), 1);

    // No matching events: registration succeeds, but resolution
    // of a nonexistent ordinal must fail.
    assert!(
        resolver
            .resolve(&reference(10, 0, event(99, 550, 2, 1),))
            .is_err()
    );
}

#[test]
fn indexed_range_supports_interleaved_tasks() {
    let dir = tempfile::tempdir().unwrap();

    let first_path = dir.path().join("first.parquet");
    let second_path = dir.path().join("second.parquet");

    let first = [event(0, 100, 0, 1), event(1, 300, 1, 1)];

    let second = [event(0, 200, 0, 1), event(1, 400, 1, 1)];

    write_events(&first_path, &first);
    write_events(&second_path, &second);

    let mut resolver = DepthMetadataResolver::new();

    resolver
        .register_task_range(10, 0, &first_path, 100, 500)
        .unwrap();

    resolver
        .register_task_range(20, 0, &second_path, 100, 500)
        .unwrap();

    let references = [
        reference(10, 0, first[0]),
        reference(20, 0, second[0]),
        reference(10, 0, first[1]),
        reference(20, 0, second[1]),
    ];

    let metadata = resolver.resolve_batch(&references).unwrap();

    assert_eq!(metadata.len(), 4);
    assert_eq!(resolver.registered_tasks(), 2);
}
