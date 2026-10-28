use std::{fs::File, path::Path};

use parquet::arrow::ArrowWriter;

use marketforge_engine::historical::merge::{
    MaterializationTimelineReader, UnifiedTimelineBatchBuilder, UnifiedTimelineRecord,
    unified_timeline_schema,
};

// -----------------------------------------------------------------------------
// Fixtures
// -----------------------------------------------------------------------------

fn trade(timestamp: i64, rank: u32, ordinal: u64) -> UnifiedTimelineRecord {
    UnifiedTimelineRecord {
        event_timestamp_ns: timestamp,
        stream_rank: rank,
        event_kind: 0,
        dataset_id: 88,
        task_id: 0,
        event_ordinal: ordinal,
        canonical_row_offset: ordinal,
        canonical_row_count: 1,
        event_boundary: None,
        source_operation: None,
    }
}

fn depth(timestamp: i64, rank: u32, ordinal: u64, rows: u64) -> UnifiedTimelineRecord {
    UnifiedTimelineRecord {
        event_timestamp_ns: timestamp,
        stream_rank: rank,
        event_kind: 1,
        dataset_id: 89,
        task_id: 0,
        event_ordinal: ordinal,
        canonical_row_offset: ordinal * 10,
        canonical_row_count: rows,
        event_boundary: Some(0),
        source_operation: Some(1),
    }
}

fn write_timeline(path: &Path, records: &[UnifiedTimelineRecord]) {
    let mut builder = UnifiedTimelineBatchBuilder::new();

    for record in records {
        builder.push(*record);
    }

    let batch = builder.finish().unwrap().unwrap();

    let file = File::create(path).unwrap();

    let mut writer = ArrowWriter::try_new(file, unified_timeline_schema(), None).unwrap();

    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn collect(reader: &mut MaterializationTimelineReader) -> Vec<UnifiedTimelineRecord> {
    let mut records = Vec::new();

    while let Some(record) = reader.next_record().unwrap() {
        records.push(record);
    }

    records
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[test]
fn reads_single_timeline_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("part-000000.parquet");

    let expected = vec![trade(100, 0, 0), depth(200, 1, 0, 3), trade(300, 0, 1)];

    write_timeline(&path, &expected);

    let mut reader = MaterializationTimelineReader::new(vec![path], 8192).unwrap();

    assert_eq!(collect(&mut reader), expected);
    assert_eq!(reader.records_read(), 3);
    assert_eq!(reader.files_opened(), 1);
}

#[test]
fn reads_multiple_timeline_files() {
    let dir = tempfile::tempdir().unwrap();

    let first = dir.path().join("part-000000.parquet");
    let second = dir.path().join("part-000001.parquet");

    let a = vec![trade(100, 0, 0), depth(200, 1, 0, 2)];

    let b = vec![trade(300, 0, 1), depth(400, 1, 1, 5)];

    write_timeline(&first, &a);
    write_timeline(&second, &b);

    let mut reader = MaterializationTimelineReader::new(vec![first, second], 2).unwrap();

    let mut expected = a;
    expected.extend(b);

    assert_eq!(collect(&mut reader), expected);
    assert_eq!(reader.files_opened(), 2);
}

#[test]
fn preserves_equal_timestamps_with_deterministic_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("part-000000.parquet");

    let expected = vec![
        trade(100, 0, 0),
        trade(100, 0, 1),
        depth(100, 1, 0, 2),
        depth(100, 1, 1, 3),
    ];

    write_timeline(&path, &expected);

    let mut reader = MaterializationTimelineReader::new(vec![path], 2).unwrap();

    assert_eq!(collect(&mut reader), expected);
}

#[test]
fn supports_empty_partition() {
    let mut reader = MaterializationTimelineReader::new(vec![], 8192).unwrap();

    assert!(reader.next_record().unwrap().is_none());
    assert_eq!(reader.records_read(), 0);
    assert_eq!(reader.files_opened(), 0);
}

#[test]
fn rejects_zero_batch_size() {
    assert!(MaterializationTimelineReader::new(vec![], 0).is_err());
}

#[test]
fn rejects_duplicate_or_unsorted_paths() {
    let first = std::path::PathBuf::from("part-000000.parquet");
    let second = std::path::PathBuf::from("part-000001.parquet");

    assert!(
        MaterializationTimelineReader::new(vec![second.clone(), first.clone()], 8192,).is_err()
    );

    assert!(MaterializationTimelineReader::new(vec![first.clone(), first], 8192,).is_err());
}

#[test]
fn rejects_timestamp_regression() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("part-000000.parquet");

    write_timeline(&path, &[trade(200, 0, 0), trade(100, 0, 1)]);

    let mut reader = MaterializationTimelineReader::new(vec![path], 8192).unwrap();

    assert!(reader.next_record().unwrap().is_some());
    assert!(reader.next_record().is_err());
}

#[test]
fn rejects_cross_file_ordering_regression() {
    let dir = tempfile::tempdir().unwrap();

    let first = dir.path().join("part-000000.parquet");
    let second = dir.path().join("part-000001.parquet");

    write_timeline(&first, &[trade(200, 0, 0)]);
    write_timeline(&second, &[trade(100, 0, 1)]);

    let mut reader = MaterializationTimelineReader::new(vec![first, second], 8192).unwrap();

    assert!(reader.next_record().unwrap().is_some());
    assert!(reader.next_record().is_err());
}

#[test]
fn rejects_invalid_trade_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("part-000000.parquet");

    let mut invalid = trade(100, 0, 0);
    invalid.event_boundary = Some(1);

    write_timeline(&path, &[invalid]);

    let mut reader = MaterializationTimelineReader::new(vec![path], 8192).unwrap();

    assert!(reader.next_record().is_err());
}

#[test]
fn rejects_invalid_depth_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("part-000000.parquet");

    let mut invalid = depth(100, 1, 0, 1);
    invalid.source_operation = Some(9);

    write_timeline(&path, &[invalid]);

    let mut reader = MaterializationTimelineReader::new(vec![path], 8192).unwrap();

    assert!(reader.next_record().is_err());
}

#[test]
fn reads_across_small_arrow_batches() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("part-000000.parquet");

    let expected = (0..100)
        .map(|ordinal| trade(ordinal as i64 * 10, 0, ordinal))
        .collect::<Vec<_>>();

    write_timeline(&path, &expected);

    let mut reader = MaterializationTimelineReader::new(vec![path], 3).unwrap();

    assert_eq!(collect(&mut reader), expected);
    assert_eq!(reader.records_read(), 100);
}

#[test]
fn rejects_missing_timeline_file() {
    let dir = tempfile::tempdir().unwrap();

    let path = dir.path().join("missing.parquet");

    let mut reader = MaterializationTimelineReader::new(vec![path], 8192).unwrap();

    assert!(reader.next_record().is_err());
}

#[test]
fn open_directory_discovers_sorted_files() {
    let dir = tempfile::tempdir().unwrap();

    write_timeline(&dir.path().join("part-000001.parquet"), &[trade(200, 0, 1)]);

    write_timeline(&dir.path().join("part-000000.parquet"), &[trade(100, 0, 0)]);

    let mut reader = MaterializationTimelineReader::open_directory(dir.path(), 8192).unwrap();

    assert_eq!(
        collect(&mut reader),
        vec![trade(100, 0, 0), trade(200, 0, 1),]
    );
}
