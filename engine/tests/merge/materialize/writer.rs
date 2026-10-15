use std::{fs::File, path::Path, sync::Arc};

use arrow_array::{
    ArrayRef, Decimal256Array, Int64Array, RecordBatch, StringArray, UInt32Array, UInt64Array,
    new_null_array,
};
use arrow_buffer::i256;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use arrow_array::{Array, ListArray, StructArray, UInt8Array};
use marketforge_engine::{
    job::MergeParquetConfig,
    process::merge::{PhysicalParquetWriter, UnifiedMergeMode, physical_trade_schema},
};

use arrow_buffer::OffsetBuffer;
use arrow_schema::DataType;

use marketforge_engine::historical::merge::{physical_combined_schema, physical_depth_schema};

fn config() -> MergeParquetConfig {
    MergeParquetConfig {
        row_group_target_mb: 1,
        file_target_mb: 2,
        compression_level: 3,
    }
}

fn trade_batch(timestamps: &[i64]) -> RecordBatch {
    let schema = physical_trade_schema();
    let n = timestamps.len();

    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| match field.name().as_str() {
            "event_timestamp_ns" => Arc::new(Int64Array::from(timestamps.to_vec())) as ArrayRef,

            "exchange" => Arc::new(StringArray::from(vec!["bybit"; n])) as ArrayRef,

            "instrument_id" => Arc::new(Int64Array::from(vec![688; n])) as ArrayRef,

            "symbol" => Arc::new(StringArray::from(vec!["BTCUSDT"; n])) as ArrayRef,

            "stream_id" => Arc::new(StringArray::from(vec![
                "bybit:perpetual:linear:BTCUSDT:trade";
                n
            ])) as ArrayRef,

            "side" => Arc::new(StringArray::from(vec!["buy"; n])) as ArrayRef,

            "price" => Arc::new(
                Decimal256Array::from(vec![i256::from_i128(78500_i128 * 10_i128.pow(28)); n])
                    .with_precision_and_scale(76, 28)
                    .unwrap(),
            ) as ArrayRef,

            "stream_rank" => Arc::new(UInt32Array::from(vec![0; n])) as ArrayRef,

            "dataset_id" => Arc::new(UInt64Array::from(vec![88; n])) as ArrayRef,

            "task_id" => Arc::new(UInt64Array::from(vec![0; n])) as ArrayRef,

            "event_ordinal" => {
                Arc::new(UInt64Array::from((0..n as u64).collect::<Vec<_>>())) as ArrayRef
            }

            "canonical_row_offset" => {
                Arc::new(UInt64Array::from((0..n as u64).collect::<Vec<_>>())) as ArrayRef
            }

            "canonical_row_count" => Arc::new(UInt64Array::from(vec![1; n])) as ArrayRef,

            _ => {
                assert!(
                    field.is_nullable(),
                    "unhandled required field: {}",
                    field.name()
                );

                new_null_array(field.data_type(), n)
            }
        })
        .collect();

    RecordBatch::try_new(schema, columns).unwrap()
}

fn read_batches(path: &Path) -> Vec<RecordBatch> {
    let file = File::open(path).unwrap();

    ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap()
        .map(|batch| batch.unwrap())
        .collect()
}

#[test]
fn writes_physical_trade_parquet() {
    let dir = tempfile::tempdir().unwrap();

    let mut writer =
        PhysicalParquetWriter::new(dir.path(), UnifiedMergeMode::Trades, &config()).unwrap();

    writer.write_batch(&trade_batch(&[100, 200, 300])).unwrap();

    writer.finish().unwrap();

    assert!(writer.is_finished());
    assert!(!writer.is_failed());

    assert_eq!(writer.metrics().events_written, 3);
    assert_eq!(writer.metrics().trade_events, 3);
    assert_eq!(writer.metrics().depth_events, 0);
    assert_eq!(writer.metrics().canonical_rows, 3);
    assert_eq!(writer.metrics().files_written, 1);

    let path = dir.path().join("part-000000.parquet");

    assert!(path.exists());

    let restored = read_batches(&path);

    assert_eq!(restored.len(), 1);
    assert_eq!(restored[0], trade_batch(&[100, 200, 300]));
}

#[test]
fn preserves_order_across_batches() {
    let dir = tempfile::tempdir().unwrap();

    let mut writer =
        PhysicalParquetWriter::new(dir.path(), UnifiedMergeMode::Trades, &config()).unwrap();

    writer.write_batch(&trade_batch(&[100, 200])).unwrap();
    writer.write_batch(&trade_batch(&[300, 400])).unwrap();

    writer.finish().unwrap();

    assert_eq!(writer.metrics().events_written, 4);
    assert_eq!(writer.metrics().start_timestamp_ns, Some(100));
    assert_eq!(writer.metrics().end_timestamp_ns, Some(400));

    let path = dir.path().join("part-000000.parquet");

    let timestamps: Vec<i64> = read_batches(&path)
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column_by_name("event_timestamp_ns")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            array.values().to_vec()
        })
        .collect();

    assert_eq!(timestamps, vec![100, 200, 300, 400]);
}

#[test]
fn rejects_timestamp_regression() {
    let dir = tempfile::tempdir().unwrap();

    let mut writer =
        PhysicalParquetWriter::new(dir.path(), UnifiedMergeMode::Trades, &config()).unwrap();

    writer.write_batch(&trade_batch(&[100, 200])).unwrap();

    assert!(writer.write_batch(&trade_batch(&[150])).is_err());

    assert!(writer.is_failed());
    assert!(writer.finish().is_err());
}

#[test]
fn rejects_wrong_physical_schema() {
    let dir = tempfile::tempdir().unwrap();

    let mut writer =
        PhysicalParquetWriter::new(dir.path(), UnifiedMergeMode::Depth, &config()).unwrap();

    assert!(writer.write_batch(&trade_batch(&[100])).is_err());
    assert!(writer.is_failed());
}

#[test]
fn rejects_writes_after_finalization() {
    let dir = tempfile::tempdir().unwrap();

    let mut writer =
        PhysicalParquetWriter::new(dir.path(), UnifiedMergeMode::Trades, &config()).unwrap();

    writer.write_batch(&trade_batch(&[100])).unwrap();
    writer.finish().unwrap();

    assert!(writer.write_batch(&trade_batch(&[200])).is_err());
}

#[test]
fn empty_partition_creates_no_files() {
    let dir = tempfile::tempdir().unwrap();

    let mut writer =
        PhysicalParquetWriter::new(dir.path(), UnifiedMergeMode::Trades, &config()).unwrap();

    writer.finish().unwrap();

    assert_eq!(writer.metrics().events_written, 0);
    assert_eq!(writer.metrics().files_written, 0);

    assert!(!dir.path().join("part-000000.parquet").exists());
}

#[test]
fn preserves_decimal256_after_parquet_roundtrip() {
    let dir = tempfile::tempdir().unwrap();

    let mut writer =
        PhysicalParquetWriter::new(dir.path(), UnifiedMergeMode::Trades, &config()).unwrap();

    writer.write_batch(&trade_batch(&[100])).unwrap();
    writer.finish().unwrap();

    let restored = read_batches(&dir.path().join("part-000000.parquet"));

    let prices = restored[0]
        .column_by_name("price")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    assert_eq!(
        prices.value(0),
        i256::from_i128(78500_i128 * 10_i128.pow(28))
    );
}

// -----------------------------------------------------------------------------
// Nested physical fixtures
// -----------------------------------------------------------------------------

fn depth_batch(timestamps: &[i64], level_counts: &[usize]) -> RecordBatch {
    assert_eq!(timestamps.len(), level_counts.len());

    let schema = physical_depth_schema();
    let n = timestamps.len();

    let mut offsets = vec![0_i32];

    for &count in level_counts {
        let next = offsets.last().copied().unwrap() + i32::try_from(count).unwrap();

        offsets.push(next);
    }

    let total_levels = usize::try_from(*offsets.last().unwrap()).unwrap();

    let changes_field = schema.field_with_name("changes").unwrap();

    let DataType::List(item_field) = changes_field.data_type() else {
        panic!("expected List");
    };

    let DataType::Struct(level_fields) = item_field.data_type() else {
        panic!("expected Struct");
    };

    let level_columns: Vec<ArrayRef> = level_fields
        .iter()
        .map(|field| match field.name().as_str() {
            "side" => Arc::new(StringArray::from(vec!["bid"; total_levels])) as ArrayRef,

            "price" => Arc::new(
                Decimal256Array::from(vec![
                    i256::from_i128(78500_i128 * 10_i128.pow(28));
                    total_levels
                ])
                .with_precision_and_scale(76, 28)
                .unwrap(),
            ) as ArrayRef,

            _ => new_null_array(field.data_type(), total_levels),
        })
        .collect();

    let levels = StructArray::try_new(level_fields.clone(), level_columns, None).unwrap();

    let changes = ListArray::try_new(
        item_field.clone(),
        OffsetBuffer::new(offsets.into()),
        Arc::new(levels),
        None,
    )
    .unwrap();

    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| match field.name().as_str() {
            "event_timestamp_ns" => Arc::new(Int64Array::from(timestamps.to_vec())) as ArrayRef,

            "exchange" => Arc::new(StringArray::from(vec!["bybit"; n])) as ArrayRef,

            "instrument_id" => Arc::new(Int64Array::from(vec![688; n])) as ArrayRef,

            "symbol" => Arc::new(StringArray::from(vec!["BTCUSDT"; n])) as ArrayRef,

            "stream_id" => Arc::new(StringArray::from(vec![
                "bybit:perpetual:linear:BTCUSDT:l2";
                n
            ])) as ArrayRef,

            "stream_rank" => Arc::new(UInt32Array::from(vec![0; n])) as ArrayRef,

            "dataset_id" => Arc::new(UInt64Array::from(vec![89; n])) as ArrayRef,

            "task_id" => Arc::new(UInt64Array::from(vec![0; n])) as ArrayRef,

            "event_ordinal" => {
                Arc::new(UInt64Array::from((0..n as u64).collect::<Vec<_>>())) as ArrayRef
            }

            "canonical_row_offset" => Arc::new(UInt64Array::from(vec![0; n])) as ArrayRef,

            "canonical_row_count" => Arc::new(UInt64Array::from(
                level_counts.iter().map(|&v| v as u64).collect::<Vec<_>>(),
            )) as ArrayRef,

            "event_boundary" => Arc::new(UInt8Array::from(vec![0; n])) as ArrayRef,

            "source_operation" => Arc::new(UInt8Array::from(vec![1; n])) as ArrayRef,

            "changes" => Arc::new(changes.clone()) as ArrayRef,

            _ => {
                assert!(field.is_nullable());
                new_null_array(field.data_type(), n)
            }
        })
        .collect();

    RecordBatch::try_new(schema, columns).unwrap()
}

fn combined_batch(timestamps: &[i64], kinds: &[u8]) -> RecordBatch {
    assert_eq!(timestamps.len(), kinds.len());

    let schema = physical_combined_schema();
    let n = timestamps.len();

    let trade_schema = schema.field_with_name("trade").unwrap();
    let depth_schema = schema.field_with_name("depth").unwrap();

    let trade = new_null_array(trade_schema.data_type(), n);
    let depth = new_null_array(depth_schema.data_type(), n);

    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| match field.name().as_str() {
            "event_timestamp_ns" => Arc::new(Int64Array::from(timestamps.to_vec())) as ArrayRef,

            "exchange" => Arc::new(StringArray::from(vec!["bybit"; n])) as ArrayRef,

            "instrument_id" => Arc::new(Int64Array::from(vec![688; n])) as ArrayRef,

            "symbol" => Arc::new(StringArray::from(vec!["BTCUSDT"; n])) as ArrayRef,

            "stream_id" => Arc::new(StringArray::from(vec!["bybit:btc"; n])) as ArrayRef,

            "event_kind" => Arc::new(UInt8Array::from(kinds.to_vec())) as ArrayRef,

            "stream_rank" => Arc::new(UInt32Array::from(vec![0; n])) as ArrayRef,

            "dataset_id" => Arc::new(UInt64Array::from(vec![88; n])) as ArrayRef,

            "task_id" | "event_ordinal" | "canonical_row_offset" => {
                Arc::new(UInt64Array::from(vec![0; n])) as ArrayRef
            }

            "canonical_row_count" => Arc::new(UInt64Array::from(vec![1; n])) as ArrayRef,

            "trade" => trade.clone(),
            "depth" => depth.clone(),

            _ => {
                assert!(field.is_nullable());
                new_null_array(field.data_type(), n)
            }
        })
        .collect();

    RecordBatch::try_new(schema, columns).unwrap()
}

// -----------------------------------------------------------------------------
// M4.2 — Nested physical writer tests
// -----------------------------------------------------------------------------

#[test]
fn writes_nested_depth_parquet() {
    let dir = tempfile::tempdir().unwrap();

    let expected = depth_batch(&[100, 200, 300], &[2, 5, 1]);

    let mut writer =
        PhysicalParquetWriter::new(dir.path(), UnifiedMergeMode::Depth, &config()).unwrap();

    writer.write_batch(&expected).unwrap();
    writer.finish().unwrap();

    assert_eq!(writer.metrics().events_written, 3);
    assert_eq!(writer.metrics().depth_events, 3);
    assert_eq!(writer.metrics().canonical_rows, 8);

    let restored = read_batches(&dir.path().join("part-000000.parquet"));

    assert_eq!(restored.len(), 1);
    assert_eq!(restored[0], expected);

    let changes = restored[0]
        .column_by_name("changes")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    assert_eq!(changes.value_offsets(), &[0, 2, 7, 8]);
}

#[test]
fn preserves_zero_level_depth_event() {
    let dir = tempfile::tempdir().unwrap();

    let expected = depth_batch(&[100, 200, 300], &[2, 0, 1]);

    let mut writer =
        PhysicalParquetWriter::new(dir.path(), UnifiedMergeMode::Depth, &config()).unwrap();

    writer.write_batch(&expected).unwrap();
    writer.finish().unwrap();

    let restored = read_batches(&dir.path().join("part-000000.parquet"));

    let changes = restored[0]
        .column_by_name("changes")
        .unwrap()
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();

    assert_eq!(changes.value_offsets(), &[0, 2, 2, 3]);

    assert_eq!(changes.value_length(1), 0);
    assert!(!changes.is_null(1));

    assert_eq!(writer.metrics().events_written, 3);
    assert_eq!(writer.metrics().canonical_rows, 3);
}

#[test]
fn writes_combined_parquet_schema() {
    let dir = tempfile::tempdir().unwrap();

    let expected = combined_batch(&[100, 200, 300, 400], &[0, 1, 0, 1]);

    let mut writer =
        PhysicalParquetWriter::new(dir.path(), UnifiedMergeMode::Combined, &config()).unwrap();

    writer.write_batch(&expected).unwrap();
    writer.finish().unwrap();

    assert_eq!(writer.metrics().events_written, 4);
    assert_eq!(writer.metrics().trade_events, 2);
    assert_eq!(writer.metrics().depth_events, 2);

    let restored = read_batches(&dir.path().join("part-000000.parquet"));

    assert_eq!(restored.len(), 1);
    assert_eq!(restored[0], expected);
    assert_eq!(restored[0].schema(), physical_combined_schema());
}

#[test]
fn rotates_files_without_losing_events() {
    let dir = tempfile::tempdir().unwrap();

    let rotation_config = MergeParquetConfig {
        row_group_target_mb: 1,
        file_target_mb: 1,
        compression_level: 3,
    };

    let mut writer =
        PhysicalParquetWriter::new(dir.path(), UnifiedMergeMode::Trades, &rotation_config).unwrap();

    let mut next_timestamp = 0_i64;
    let mut expected_events = 0_u64;

    // Large batches with distinct timestamps.
    // Repeated until the writer rotates.
    for _ in 0..100 {
        let timestamps = (next_timestamp..next_timestamp + 10_000).collect::<Vec<_>>();

        writer.write_batch(&trade_batch(&timestamps)).unwrap();

        next_timestamp += 10_000;
        expected_events += 10_000;

        if writer.metrics().files_written >= 2 {
            break;
        }
    }

    writer.finish().unwrap();

    let files_written = writer.metrics().files_written;

    assert!(
        files_written > 1,
        "expected file rotation, got {files_written} file(s)"
    );

    let mut total_rows = 0_u64;
    let mut previous_timestamp = None;

    for index in 0..files_written {
        let path = dir.path().join(format!("part-{index:06}.parquet"));

        assert!(path.exists());

        for batch in read_batches(&path) {
            total_rows += batch.num_rows() as u64;

            let timestamps = batch
                .column_by_name("event_timestamp_ns")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            for &timestamp in timestamps.values() {
                if let Some(previous) = previous_timestamp {
                    assert!(timestamp >= previous);
                }

                previous_timestamp = Some(timestamp);
            }
        }
    }

    assert_eq!(total_rows, expected_events);
    assert_eq!(writer.metrics().events_written, expected_events);
    assert_eq!(previous_timestamp, Some(next_timestamp - 1));
}
