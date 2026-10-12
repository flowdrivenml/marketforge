use std::{
    fs::{self, File},
    path::Path,
    sync::Arc,
};

use arrow_array::{
    ArrayRef, Int64Array, ListArray, RecordBatch, StringArray, UInt8Array, UInt32Array, UInt64Array,
};
use arrow_buffer::OffsetBuffer;
use arrow_schema::DataType;
use marketforge_engine::job::MergeTimeRange;
use marketforge_engine::process::merge::{
    MergeCheckpoint, MergeSkippedInitialization, MergeTimelineFile, UnifiedMergeMode,
    ValidatedPhysicalDataset, build_physical_checkpoints, physical_depth_schema,
};
use parquet::arrow::ArrowWriter;
// -----------------------------------------------------------------------------
// Fixtures
// -----------------------------------------------------------------------------

fn checkpoint(segment_id: u64, timestamp: i64, offset: u64, count: u64) -> MergeCheckpoint {
    MergeCheckpoint {
        dataset_id: 89,
        task_id: 0,
        segment_id,
        timestamp_ns: timestamp,
        canonical_row_offset: offset,
        canonical_row_count: count,
    }
}

fn skipped(ordinal: u64, timestamp: i64, offset: u64, count: u64) -> MergeSkippedInitialization {
    MergeSkippedInitialization {
        dataset_id: 89,
        task_id: 0,
        event_ordinal: ordinal,
        canonical_row_offset: offset,
        canonical_row_count: count,
        timestamp_ns: timestamp,
    }
}

fn physical_depth_fixture(
    timestamps: &[i64],
    boundaries: &[u8],
    offsets: &[u64],
    counts: &[u64],
) -> RecordBatch {
    assert_eq!(timestamps.len(), boundaries.len());
    assert_eq!(timestamps.len(), offsets.len());
    assert_eq!(timestamps.len(), counts.len());

    let schema = physical_depth_schema();
    let n = timestamps.len();

    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| {
            let array: ArrayRef = match field.name().as_str() {
                "event_timestamp_ns" => Arc::new(Int64Array::from(timestamps.to_vec())),

                "exchange" => Arc::new(StringArray::from(vec!["bybit"; n])),

                "instrument_id" => Arc::new(Int64Array::from(vec![688; n])),

                "symbol" => Arc::new(StringArray::from(vec!["BTCUSDT"; n])),

                "stream_id" => Arc::new(StringArray::from(vec![
                    "bybit:perpetual:linear:BTCUSDT:l2";
                    n
                ])),

                "stream_rank" => Arc::new(UInt32Array::from(vec![0; n])),

                "dataset_id" => Arc::new(UInt64Array::from(vec![89; n])),

                "task_id" => Arc::new(UInt64Array::from(vec![0; n])),

                "event_ordinal" => Arc::new(UInt64Array::from((0..n as u64).collect::<Vec<_>>())),

                "canonical_row_offset" => Arc::new(UInt64Array::from(offsets.to_vec())),

                "canonical_row_count" => Arc::new(UInt64Array::from(counts.to_vec())),

                "event_boundary" => Arc::new(UInt8Array::from(boundaries.to_vec())),

                "source_operation" => Arc::new(UInt8Array::from(vec![0; n])),

                "changes" => {
                    let DataType::List(item_field) = field.data_type() else {
                        panic!("physical depth changes must be a List");
                    };

                    // One empty list for each physical depth event.
                    let offsets = OffsetBuffer::new(vec![0_i32; n + 1].into());

                    // Correct nested Struct type, containing zero level rows.
                    let values = arrow_array::new_empty_array(item_field.data_type());

                    Arc::new(ListArray::new(item_field.clone(), offsets, values, None))
                }

                _ => arrow_array::new_null_array(field.data_type(), n),
            };

            array
        })
        .collect();

    RecordBatch::try_new(schema, columns).unwrap()
}

fn write_parquet(path: &Path, batch: &RecordBatch) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();

    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), None).unwrap();

    writer.write(batch).unwrap();
    writer.close().unwrap();
}

fn validated_file(partition_id: u32, file_index: u32, rows: u64) -> MergeTimelineFile {
    MergeTimelineFile {
        path: format!("timeline/partition-{partition_id:06}/part-{file_index:06}.parquet"),
        size_bytes: 4096,
        partition_id: Some(partition_id),
        rows: Some(rows),
        start_timestamp_ns: Some(100),
        end_timestamp_ns: Some(1000),
    }
}

fn validated(files: Vec<MergeTimelineFile>) -> ValidatedPhysicalDataset {
    ValidatedPhysicalDataset {
        files_written: files.len() as u64,
        files,
        ..Default::default()
    }
}

#[test]
fn resolves_retained_initialization() {
    let dir = tempfile::tempdir().unwrap();

    let path = dir
        .path()
        .join("timeline/partition-000000/part-000000.parquet");

    let batch = physical_depth_fixture(&[100], &[1], &[0], &[2]);

    write_parquet(&path, &batch);

    let files = validated(vec![validated_file(0, 0, 1)]);

    let checkpoints = build_physical_checkpoints(
        dir.path(),
        UnifiedMergeMode::Depth,
        &files,
        &[checkpoint(7, 100, 0, 2)],
        &[],
        MergeTimeRange {
            start_ns: 0,
            end_ns: 1000,
        },
    )
    .unwrap();

    assert_eq!(checkpoints.len(), 1);

    assert_eq!(checkpoints[0].segment_id, 7);
    assert_eq!(checkpoints[0].partition_id, 0);
    assert_eq!(checkpoints[0].file_index, 0);
    assert_eq!(checkpoints[0].row_index, 0);
}

#[test]
fn rejects_missing_initialization() {
    let dir = tempfile::tempdir().unwrap();

    let path = dir
        .path()
        .join("timeline/partition-000000/part-000000.parquet");

    write_parquet(&path, &physical_depth_fixture(&[100], &[0], &[0], &[2]));

    let files = validated(vec![validated_file(0, 0, 1)]);

    assert!(
        build_physical_checkpoints(
            dir.path(),
            UnifiedMergeMode::Depth,
            &files,
            &[checkpoint(7, 100, 0, 2)],
            &[],
            MergeTimeRange {
                start_ns: 0,
                end_ns: 1000,
            },
        )
        .is_err()
    );
}

#[test]
fn accepts_explicitly_skipped_initialization() {
    let dir = tempfile::tempdir().unwrap();

    let files = validated(vec![]);

    let checkpoints = build_physical_checkpoints(
        dir.path(),
        UnifiedMergeMode::Depth,
        &files,
        &[checkpoint(7, 100, 0, 2)],
        &[skipped(0, 100, 0, 2)],
        MergeTimeRange {
            start_ns: 0,
            end_ns: 1000,
        },
    )
    .unwrap();

    assert!(checkpoints.is_empty());
}

#[test]
fn rejects_duplicate_skipped_initializations() {
    let dir = tempfile::tempdir().unwrap();

    let original = skipped(0, 100, 0, 2);

    assert!(
        build_physical_checkpoints(
            dir.path(),
            UnifiedMergeMode::Depth,
            &validated(vec![]),
            &[checkpoint(0, 100, 0, 2)],
            &[original.clone(), original],
            MergeTimeRange {
                start_ns: 0,
                end_ns: 1000,
            },
        )
        .is_err()
    );
}

#[test]
fn rejects_materialized_skipped_initialization() {
    let dir = tempfile::tempdir().unwrap();

    let path = dir
        .path()
        .join("timeline/partition-000000/part-000000.parquet");

    write_parquet(&path, &physical_depth_fixture(&[100], &[1], &[0], &[2]));

    assert!(
        build_physical_checkpoints(
            dir.path(),
            UnifiedMergeMode::Depth,
            &validated(vec![validated_file(0, 0, 1)]),
            &[checkpoint(0, 100, 0, 2)],
            &[skipped(0, 100, 0, 2)],
            MergeTimeRange {
                start_ns: 0,
                end_ns: 1000,
            },
        )
        .is_err()
    );
}

#[test]
fn checkpoint_row_offsets_are_file_relative() {
    let dir = tempfile::tempdir().unwrap();

    let first = dir
        .path()
        .join("timeline/partition-000000/part-000000.parquet");

    let second = dir
        .path()
        .join("timeline/partition-000000/part-000001.parquet");

    write_parquet(
        &first,
        &physical_depth_fixture(&[100, 200], &[1, 0], &[0, 2], &[2, 1]),
    );

    write_parquet(
        &second,
        &physical_depth_fixture(&[300, 400], &[0, 1], &[3, 4], &[1, 2]),
    );

    let checkpoints = build_physical_checkpoints(
        dir.path(),
        UnifiedMergeMode::Depth,
        &validated(vec![validated_file(0, 0, 2), validated_file(0, 1, 2)]),
        &[checkpoint(0, 100, 0, 2), checkpoint(1, 400, 4, 2)],
        &[],
        MergeTimeRange {
            start_ns: 0,
            end_ns: 1000,
        },
    )
    .unwrap();

    assert_eq!(checkpoints.len(), 2);

    assert_eq!(checkpoints[0].file_index, 0);
    assert_eq!(checkpoints[0].row_index, 0);

    assert_eq!(checkpoints[1].file_index, 1);
    assert_eq!(checkpoints[1].row_index, 1);
}

#[test]
fn rejects_depth_checkpoints_in_trade_only_mode() {
    let dir = tempfile::tempdir().unwrap();

    assert!(
        build_physical_checkpoints(
            dir.path(),
            UnifiedMergeMode::Trades,
            &validated(vec![]),
            &[checkpoint(0, 100, 0, 2)],
            &[],
            MergeTimeRange {
                start_ns: 0,
                end_ns: 1000,
            },
        )
        .is_err()
    );
}
#[test]
fn ignores_canonical_checkpoints_outside_requested_interval() {
    let dir = tempfile::tempdir().unwrap();

    let path = dir
        .path()
        .join("timeline/partition-000000/part-000000.parquet");

    write_parquet(&path, &physical_depth_fixture(&[300], &[1], &[20], &[2]));

    let checkpoints = build_physical_checkpoints(
        dir.path(),
        UnifiedMergeMode::Depth,
        &validated(vec![validated_file(0, 0, 1)]),
        &[
            checkpoint(0, 100, 0, 2),
            checkpoint(1, 300, 20, 2),
            checkpoint(2, 900, 40, 2),
        ],
        &[],
        MergeTimeRange {
            start_ns: 200,
            end_ns: 500,
        },
    )
    .unwrap();

    assert_eq!(checkpoints.len(), 1);
    assert_eq!(checkpoints[0].segment_id, 1);
    assert_eq!(checkpoints[0].timestamp_ns, 300);
}
