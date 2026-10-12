#![cfg(feature = "process")]

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use arrow_array::{Array, Int64Array, UInt8Array, UInt64Array};

use marketforge_engine::{
    canonical::{BookSide, EventEnvelope, Exchange, L2LevelUpdate},
    formats::depth::{DepthProcessingOutcome, DepthSourceEventMetadata},
    job::ParquetResourceConfig,
    process::{parquet::ParquetDepthWriter, worker::DepthSink},
};

use marketforge_engine::process::boundary::DepthBoundaryManifest;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rust_decimal::Decimal;
// -----------------------------------------------------------------------------
// Temporary directory
// -----------------------------------------------------------------------------

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new() -> Self {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

        let path = std::env::temp_dir().join(format!(
            "marketforge-depth-events-{}-{id}",
            std::process::id()
        ));

        fs::create_dir_all(&path).unwrap();

        Self { path }
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

// -----------------------------------------------------------------------------
// Test configuration
// -----------------------------------------------------------------------------

fn resources() -> ParquetResourceConfig {
    ParquetResourceConfig {
        row_group_target_bytes: 1024,
        file_target_bytes: 4096,
    }
}

// -----------------------------------------------------------------------------
// Canonical level
// -----------------------------------------------------------------------------

fn level(timestamp: i64, price: i64) -> L2LevelUpdate {
    let price = Decimal::from(price);
    let quantity = Decimal::ONE;

    L2LevelUpdate {
        envelope: EventEnvelope {
            event_timestamp_ns: timestamp,
            system_timestamp_ns: None,
            exchange: Exchange::Bybit,
            instrument_id: 1,
            symbol: "BTCUSDT".to_owned(),
            stream_id: "bybit:BTCUSDT:depth".to_owned(),
        },

        side: BookSide::Bid,
        price,

        quantity_base: Some(quantity),
        quantity_quote: Some(price * quantity),
        quantity_contracts: None,
        order_count: None,
    }
}

// -----------------------------------------------------------------------------
// Source outcome helper
// -----------------------------------------------------------------------------

fn outcome(
    ordinal: u64,
    timestamp: i64,
    sequence: Option<u64>,
    initialization: bool,
    count: usize,
) -> DepthProcessingOutcome {
    let events = (0..count)
        .map(|index| level(timestamp, 100_000 - index as i64))
        .collect();

    let result = if initialization {
        DepthProcessingOutcome::initialization(events)
    } else {
        DepthProcessingOutcome::changes(events)
    };

    result.with_source(DepthSourceEventMetadata {
        source_event_ordinal: Some(ordinal),
        event_timestamp_ns: timestamp,
        system_timestamp_ns: None,
        sequence_start: sequence,
        sequence_end: sequence,
    })
}

fn attach_test_boundary(writer: &mut ParquetDepthWriter) {
    let boundary = DepthBoundaryManifest::new(
        Exchange::Bybit,
        1,
        "BTCUSDT".to_owned(),
        "test-depth-stream".to_owned(),
    );

    writer.write_boundary(boundary).unwrap();
}

// -----------------------------------------------------------------------------
// Read event-index columns
// -----------------------------------------------------------------------------

#[derive(Debug)]
struct EventRow {
    ordinal: u64,
    offset: u64,
    count: u64,
    timestamp: i64,
    system_timestamp: Option<i64>,
    sequence_start: Option<u64>,
    sequence_end: Option<u64>,
    boundary: u8,
}

fn read_event_index(directory: &Path) -> Vec<EventRow> {
    let path = directory.join("events.parquet");

    assert!(path.is_file(), "missing event index: {}", path.display());

    let file = fs::File::open(path).unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();

    let mut rows = Vec::new();

    for batch in reader {
        let batch = batch.unwrap();

        let ordinals = batch
            .column_by_name("event_ordinal")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let offsets = batch
            .column_by_name("canonical_row_offset")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let counts = batch
            .column_by_name("canonical_row_count")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let timestamps = batch
            .column_by_name("event_timestamp_ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let system_timestamps = batch
            .column_by_name("system_timestamp_ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let sequence_starts = batch
            .column_by_name("sequence_start")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let sequence_ends = batch
            .column_by_name("sequence_end")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let boundaries = batch
            .column_by_name("event_boundary")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();

        for index in 0..batch.num_rows() {
            rows.push(EventRow {
                ordinal: ordinals.value(index),
                offset: offsets.value(index),
                count: counts.value(index),
                timestamp: timestamps.value(index),

                system_timestamp: if system_timestamps.is_null(index) {
                    None
                } else {
                    Some(system_timestamps.value(index))
                },

                sequence_start: if sequence_starts.is_null(index) {
                    None
                } else {
                    Some(sequence_starts.value(index))
                },

                sequence_end: if sequence_ends.is_null(index) {
                    None
                } else {
                    Some(sequence_ends.value(index))
                },

                boundary: boundaries.value(index),
            });
        }
    }

    rows
}

// -----------------------------------------------------------------------------
// Event offsets and zero-change records
// -----------------------------------------------------------------------------

#[test]
fn preserves_event_offsets_and_zero_change_records() {
    let directory = TestDirectory::new();

    let mut writer = ParquetDepthWriter::new(&directory.path, resources()).unwrap();

    writer
        .write_outcome(outcome(1, 1000, Some(100), true, 400))
        .unwrap();

    writer
        .write_outcome(outcome(2, 1001, Some(101), false, 12))
        .unwrap();

    writer
        .write_outcome(outcome(3, 1002, Some(102), false, 0))
        .unwrap();

    writer
        .write_outcome(outcome(4, 1003, Some(103), false, 8))
        .unwrap();

    attach_test_boundary(&mut writer);
    writer.finish().unwrap();

    let rows = read_event_index(&directory.path);

    assert_eq!(rows.len(), 4);

    assert_eq!(rows[0].ordinal, 1);
    assert_eq!(rows[0].offset, 0);
    assert_eq!(rows[0].count, 400);
    assert_eq!(rows[0].boundary, 1);

    assert_eq!(rows[1].ordinal, 2);
    assert_eq!(rows[1].offset, 400);
    assert_eq!(rows[1].count, 12);
    assert_eq!(rows[1].boundary, 0);

    assert_eq!(rows[2].ordinal, 3);
    assert_eq!(rows[2].offset, 412);
    assert_eq!(rows[2].count, 0);

    assert_eq!(rows[3].ordinal, 4);
    assert_eq!(rows[3].offset, 412);
    assert_eq!(rows[3].count, 8);

    assert_eq!(writer.events_indexed(), 4);
    assert_eq!(writer.next_event_offset(), 420);

    println!("\nSOURCE EVENT INDEX");

    println!(
        "{:<10} {:<12} {:<10} {:<16} {:<12} {:<10}",
        "ORDINAL", "ROW OFFSET", "ROW COUNT", "TIMESTAMP", "SEQUENCE", "BOUNDARY",
    );

    for row in &rows {
        println!(
            "{:<10} {:<12} {:<10} {:<16} {:<12} {:<10}",
            row.ordinal,
            row.offset,
            row.count,
            row.timestamp,
            row.sequence_start
                .map_or("-".to_owned(), |value| value.to_string()),
            row.boundary,
        );
    }
}

// -----------------------------------------------------------------------------
// Exact accounting
// -----------------------------------------------------------------------------

#[test]
fn event_index_offsets_are_contiguous() {
    let directory = TestDirectory::new();

    let mut writer = ParquetDepthWriter::new(&directory.path, resources()).unwrap();

    writer
        .write_outcome(outcome(1, 1000, Some(1), true, 20))
        .unwrap();
    writer
        .write_outcome(outcome(2, 1001, Some(2), false, 0))
        .unwrap();
    writer
        .write_outcome(outcome(3, 1002, Some(3), false, 5))
        .unwrap();
    writer
        .write_outcome(outcome(4, 1003, Some(4), false, 0))
        .unwrap();
    writer
        .write_outcome(outcome(5, 1004, Some(5), false, 7))
        .unwrap();

    attach_test_boundary(&mut writer);
    writer.finish().unwrap();

    let rows = read_event_index(&directory.path);

    let mut expected_offset = 0u64;

    for row in &rows {
        assert_eq!(
            row.offset, expected_offset,
            "noncontiguous event index at ordinal {}",
            row.ordinal
        );

        expected_offset += row.count;
    }

    assert_eq!(expected_offset, 32);
    assert_eq!(expected_offset, writer.metrics().levels_written);
}

// -----------------------------------------------------------------------------
// Source metadata round trip
// -----------------------------------------------------------------------------

#[test]
fn preserves_source_metadata() {
    let directory = TestDirectory::new();

    let mut writer = ParquetDepthWriter::new(&directory.path, resources()).unwrap();

    writer
        .write_outcome(outcome(42, 123456789, Some(1000), true, 5))
        .unwrap();

    attach_test_boundary(&mut writer);
    writer.finish().unwrap();

    let rows = read_event_index(&directory.path);

    assert_eq!(rows.len(), 1);

    let row = &rows[0];

    assert_eq!(row.ordinal, 42);
    assert_eq!(row.timestamp, 123456789);
    assert_eq!(row.sequence_start, Some(1000));
    assert_eq!(row.sequence_end, Some(1000));
    assert_eq!(row.system_timestamp, None);
}

// -----------------------------------------------------------------------------
// Nullable sequences
// -----------------------------------------------------------------------------

#[test]
fn supports_unsequenced_events() {
    let directory = TestDirectory::new();

    let mut writer = ParquetDepthWriter::new(&directory.path, resources()).unwrap();

    writer
        .write_outcome(outcome(1, 1000, None, true, 5))
        .unwrap();

    writer
        .write_outcome(outcome(2, 1001, None, false, 3))
        .unwrap();

    attach_test_boundary(&mut writer);
    writer.finish().unwrap();

    let rows = read_event_index(&directory.path);

    assert_eq!(rows.len(), 2);

    for row in rows {
        assert_eq!(row.sequence_start, None);
        assert_eq!(row.sequence_end, None);
    }
}

// -----------------------------------------------------------------------------
// Duplicate ordinal
// -----------------------------------------------------------------------------

#[test]
fn rejects_duplicate_event_ordinals() {
    let directory = TestDirectory::new();

    let mut writer = ParquetDepthWriter::new(&directory.path, resources()).unwrap();

    writer
        .write_outcome(outcome(1, 1000, Some(100), true, 5))
        .unwrap();

    assert!(
        writer
            .write_outcome(outcome(1, 1001, Some(101), false, 3))
            .is_err()
    );

    assert!(writer.is_failed());
}

// -----------------------------------------------------------------------------
// Decreasing ordinal
// -----------------------------------------------------------------------------

#[test]
fn rejects_decreasing_event_ordinals() {
    let directory = TestDirectory::new();

    let mut writer = ParquetDepthWriter::new(&directory.path, resources()).unwrap();

    writer
        .write_outcome(outcome(10, 1000, Some(100), true, 5))
        .unwrap();

    assert!(
        writer
            .write_outcome(outcome(9, 1001, Some(101), false, 3))
            .is_err()
    );

    assert!(writer.is_failed());
}

// -----------------------------------------------------------------------------
// Event-index schema
// -----------------------------------------------------------------------------

#[test]
fn event_index_has_nine_columns() {
    let schema = marketforge_engine::process::parquet::depth_event_schema();

    assert_eq!(schema.fields().len(), 9);

    let expected = [
        "event_ordinal",
        "canonical_row_offset",
        "canonical_row_count",
        "event_timestamp_ns",
        "system_timestamp_ns",
        "sequence_start",
        "sequence_end",
        "event_boundary",
        "source_operation",
    ];

    for (field, expected_name) in schema.fields().iter().zip(expected) {
        assert_eq!(field.name(), expected_name);
    }
}
