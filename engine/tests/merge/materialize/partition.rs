use std::{fs::File, path::Path, path::PathBuf, sync::Arc};

use arrow_array::{Array, ListArray, StructArray, UInt8Array};
use arrow_array::{
    ArrayRef, Decimal256Array, Int64Array, RecordBatch, StringArray, UInt64Array, new_null_array,
};
use arrow_buffer::i256;
use marketforge_engine::historical::merge::MergeBoundarySummary;
use marketforge_engine::historical::merge::validate_physical_partition;
use marketforge_engine::historical::merge::{
    MaterializationPartitionResult, validate_physical_dataset,
};
use marketforge_engine::{
    book::BookSegment,
    canonical::Exchange,
    job::MergeParquetConfig,
    process::{
        merge::{
            MaterializationBatchLimits, MaterializationPartitionInput,
            MaterializationPartitionWorker, MaterializationWorkerLimits, UnifiedMergeMode,
            UnifiedMergeStream, UnifiedMergeStreamKind, UnifiedTimelineBatchBuilder,
            UnifiedTimelineRecord, physical_trade_schema,
        },
        parquet::trade_schema,
    },
};
use marketforge_engine::{
    formats::depth::{DepthEventBoundary, DepthSourceOperation},
    process::{
        boundary::DepthBoundaryManifest,
        merge::DepthTaskInventory,
        parquet::{DepthEventBatchBuilder, DepthSegmentManifest, IndexedDepthEvent, depth_schema},
    },
};
use marketforge_engine::{
    job::{
        MergeJob, MergeOperation, MergeOutput, MergePartitioning, MergeResources, MergeTimeRange,
    },
    process::merge::{MaterializationPartitionPlan, execute_parallel_materialization},
};
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};

// -----------------------------------------------------------------------------
// Fixtures
// -----------------------------------------------------------------------------

fn parquet_config() -> MergeParquetConfig {
    MergeParquetConfig {
        row_group_target_mb: 1,
        file_target_mb: 2,
        compression_level: 3,
    }
}

fn worker_limits() -> MaterializationWorkerLimits {
    MaterializationWorkerLimits {
        batch_limits: MaterializationBatchLimits::new(2, 10).unwrap(),
        reader_batch_size: 2,
        max_open_cursors: 32,
    }
}

fn trade_stream() -> UnifiedMergeStream {
    UnifiedMergeStream {
        stream_rank: 0,
        dataset_id: 88,
        kind: UnifiedMergeStreamKind::Trade,
        exchange: "bybit".into(),
        instrument_id: 688,
        symbol: "BTCUSDT".into(),
        stream_id: "bybit:perpetual:linear:BTCUSDT:trade".into(),
        base_currency: None,
        quote_currency: None,
    }
}

fn decimal(values: &[i64]) -> ArrayRef {
    Arc::new(
        Decimal256Array::from(
            values
                .iter()
                .map(|value| i256::from_i128(*value as i128 * 10_i128.pow(28)))
                .collect::<Vec<_>>(),
        )
        .with_precision_and_scale(76, 28)
        .unwrap(),
    )
}

fn canonical_trades(timestamps: &[i64]) -> RecordBatch {
    let schema = trade_schema();
    let n = timestamps.len();

    let prices = vec![78500; n];

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

            "price" => decimal(&prices),

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

fn write_parquet(path: &Path, batch: &RecordBatch) {
    let file = File::create(path).unwrap();

    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();

    writer.write(batch).unwrap();
    writer.close().unwrap();
}

fn timeline_reference(ordinal: u64, timestamp: i64) -> UnifiedTimelineRecord {
    UnifiedTimelineRecord {
        event_timestamp_ns: timestamp,
        stream_rank: 0,
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

fn write_timeline(path: &Path, references: &[UnifiedTimelineRecord]) {
    let mut builder = UnifiedTimelineBatchBuilder::new();

    for reference in references {
        builder.push(*reference);
    }

    let batch = builder.finish().unwrap().unwrap();

    write_parquet(path, &batch);
}

fn read_parquet(path: &Path) -> Vec<RecordBatch> {
    ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .build()
        .unwrap()
        .map(|batch| batch.unwrap())
        .collect()
}

fn create_depth_fixture(directory: &Path) -> (DepthTaskInventory, Vec<UnifiedTimelineRecord>) {
    std::fs::create_dir_all(directory).unwrap();

    let canonical_path = directory.join("part-000000.parquet");
    let events_path = directory.join("events.parquet");

    // Two initialization levels and one subsequent update.
    write_parquet(&canonical_path, &canonical_depth(&[100, 100, 300]));

    let events = vec![
        depth_event(0, 100, 0, 2, DepthEventBoundary::Initialization),
        depth_event(1, 200, 2, 0, DepthEventBoundary::Changes),
        depth_event(2, 300, 2, 1, DepthEventBoundary::Changes),
    ];

    write_depth_events(&events_path, &events);

    let segments = DepthSegmentManifest::new(
        3,
        vec![BookSegment {
            segment_id: 0,
            start_timestamp_ns: 100,
            initial_event_offset: 0,
            initial_level_count: 2,
        }],
    )
    .unwrap();

    let inventory = DepthTaskInventory {
        dataset_id: 89,
        task_id: 0,
        canonical_files: vec![canonical_path],
        events_path,
        segments_path: directory.join("segments.json"),
        boundary_path: directory.join("boundary.json"),
        canonical_rows: 3,
        segments,
        boundary: MergeBoundarySummary::from(&DepthBoundaryManifest::new(
            Exchange::Bybit,
            688,
            "BTCUSDT".into(),
            "bybit:perpetual:linear:BTCUSDT:l2".into(),
        )),
    };

    let references = events.iter().map(depth_reference).collect();

    (inventory, references)
}

fn depth_stream() -> UnifiedMergeStream {
    UnifiedMergeStream {
        stream_rank: 1,
        dataset_id: 89,
        kind: UnifiedMergeStreamKind::Depth,
        exchange: "bybit".into(),
        instrument_id: 688,
        symbol: "BTCUSDT".into(),
        stream_id: "bybit:perpetual:linear:BTCUSDT:l2".into(),
        base_currency: None,
        quote_currency: None,
    }
}

fn canonical_depth(timestamps: &[i64]) -> RecordBatch {
    let schema = depth_schema();
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
                "bybit:perpetual:linear:BTCUSDT:l2";
                n
            ])) as ArrayRef,

            "side" => Arc::new(StringArray::from(vec!["bid"; n])) as ArrayRef,

            "price" => decimal(&vec![78500; n]),

            _ => {
                assert!(
                    field.is_nullable(),
                    "unhandled required depth field: {}",
                    field.name()
                );

                new_null_array(field.data_type(), n)
            }
        })
        .collect();

    RecordBatch::try_new(schema, columns).unwrap()
}

fn depth_event(
    ordinal: u64,
    timestamp: i64,
    offset: u64,
    count: u64,
    boundary: DepthEventBoundary,
) -> IndexedDepthEvent {
    IndexedDepthEvent {
        event_ordinal: ordinal,
        canonical_row_offset: offset,
        canonical_row_count: count,
        event_timestamp_ns: timestamp,
        system_timestamp_ns: Some(timestamp + 5),
        sequence_start: Some(ordinal * 10),
        sequence_end: Some(ordinal * 10 + 1),
        event_boundary: boundary,
        source_operation: DepthSourceOperation::AbsoluteUpdate,
    }
}

fn write_depth_events(path: &Path, events: &[IndexedDepthEvent]) {
    let mut builder = DepthEventBatchBuilder::new();

    for event in events {
        builder.push(event.clone());
    }

    let batch = builder.finish().unwrap().unwrap();

    write_parquet(path, &batch);
}

fn depth_reference(event: &IndexedDepthEvent) -> UnifiedTimelineRecord {
    UnifiedTimelineRecord {
        event_timestamp_ns: event.event_timestamp_ns,
        stream_rank: 1,
        event_kind: 1,
        dataset_id: 89,
        task_id: 0,
        event_ordinal: event.event_ordinal,
        canonical_row_offset: event.canonical_row_offset,
        canonical_row_count: event.canonical_row_count,
        event_boundary: Some(match event.event_boundary {
            DepthEventBoundary::Changes => 0,
            DepthEventBoundary::Initialization => 1,
        }),
        source_operation: Some(match event.source_operation {
            DepthSourceOperation::Snapshot => 0,
            DepthSourceOperation::AbsoluteUpdate => 1,
            DepthSourceOperation::RelativeUpdate => 2,
        }),
    }
}
fn merge_job(workers: usize, memory_mb: u64, output_path: PathBuf) -> MergeJob {
    MergeJob {
        job_id: 999,
        operation: MergeOperation::Merge,
        inputs: vec![88],
        input_paths: Default::default(),

        time_range: MergeTimeRange {
            start_ns: 0,
            end_ns: 1000,
        },

        resources: MergeResources {
            workers,
            memory_limit_mb: memory_mb,
            max_open_files: 256,
        },

        partitioning: MergePartitioning {
            partitions_per_worker: 2,
        },

        parquet: parquet_config(),

        output: MergeOutput {
            dataset_path: output_path,
        },
    }
}

fn physical_trade_fixture(timestamps: &[i64]) -> RecordBatch {
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

            "price" => decimal(&vec![78500; n]),

            "stream_rank" => Arc::new(arrow_array::UInt32Array::from(vec![0; n])) as ArrayRef,

            "dataset_id" => Arc::new(UInt64Array::from(vec![88; n])) as ArrayRef,

            "task_id" => Arc::new(UInt64Array::from(vec![0; n])) as ArrayRef,

            "event_ordinal" | "canonical_row_offset" => {
                Arc::new(UInt64Array::from((0..n as u64).collect::<Vec<_>>())) as ArrayRef
            }

            "canonical_row_count" => Arc::new(UInt64Array::from(vec![1; n])) as ArrayRef,

            _ => {
                assert!(field.is_nullable());
                new_null_array(field.data_type(), n)
            }
        })
        .collect();

    RecordBatch::try_new(schema, columns).unwrap()
}

fn physical_result(
    partition_id: u32,
    start_ns: i64,
    end_ns: i64,
    events: u64,
) -> marketforge_engine::historical::merge::MaterializationPartitionResult {
    use marketforge_engine::historical::merge::{
        MaterializationPartitionResult, PhysicalWriterMetrics,
    };

    MaterializationPartitionResult {
        partition_id,
        start_ns,
        end_ns,
        windows_processed: 1,
        references_read: events,
        writer_metrics: PhysicalWriterMetrics {
            events_written: events,
            trade_events: events,
            depth_events: 0,
            canonical_rows: events,
            files_written: 1,
            row_groups_written: 1,
            start_timestamp_ns: None,
            end_timestamp_ns: None,
        },
    }
}
fn create_physical_partition(
    staging: &Path,
    partition_id: u32,
    start_ns: i64,
    end_ns: i64,
    timestamps: &[i64],
) -> MaterializationPartitionResult {
    let directory = staging
        .join("timeline")
        .join(format!("partition-{partition_id:06}"));

    std::fs::create_dir_all(&directory).unwrap();

    if !timestamps.is_empty() {
        let batch = physical_trade_fixture(timestamps);

        write_parquet(&directory.join("part-000000.parquet"), &batch);
    }

    let mut result = physical_result(partition_id, start_ns, end_ns, timestamps.len() as u64);

    result.writer_metrics.files_written = u64::from(!timestamps.is_empty());

    result.writer_metrics.row_groups_written = u64::from(!timestamps.is_empty());

    result.writer_metrics.start_timestamp_ns = timestamps.first().copied();

    result.writer_metrics.end_timestamp_ns = timestamps.last().copied();

    result
}

// -----------------------------------------------------------------------------
// Integration test
// -----------------------------------------------------------------------------

#[test]
fn materializes_trade_partition_end_to_end() {
    let dir = tempfile::tempdir().unwrap();

    let canonical_dir = dir.path().join("canonical");
    let timeline_dir = dir.path().join("logical");
    let staging = dir.path().join("staging");

    std::fs::create_dir_all(&canonical_dir).unwrap();
    std::fs::create_dir_all(&timeline_dir).unwrap();

    let canonical_path = canonical_dir.join("part-000000.parquet");
    let timeline_path = timeline_dir.join("part-000000.parquet");

    let timestamps = [100, 200, 300, 400, 500];

    write_parquet(&canonical_path, &canonical_trades(&timestamps));

    let references = timestamps
        .iter()
        .enumerate()
        .map(|(ordinal, &timestamp)| timeline_reference(ordinal as u64, timestamp))
        .collect::<Vec<_>>();

    write_timeline(&timeline_path, &references);

    // Construct the existing trade inventory directly.
    let trade_inventory = marketforge_engine::historical::merge::TradeTaskInventory {
        dataset_id: 88,
        task_id: 0,
        files: vec![marketforge_engine::historical::merge::TradeFileInventory {
            path: canonical_path,
            part_index: 0,
            row_offset: 0,
            rows: 5,
            start_timestamp_ns: 100,
            end_timestamp_ns: 500,
        }],
        canonical_rows: 5,
        start_timestamp_ns: 100,
        end_timestamp_ns: 500,
    };

    let streams = vec![trade_stream()];
    let trades = vec![trade_inventory];
    let timeline_files = vec![timeline_path];

    let config = parquet_config();

    let input = MaterializationPartitionInput {
        partition_id: 0,
        start_ns: 0,
        end_ns: 1000,
        timeline_files: &timeline_files,
        trade_inventory: &trades,
        depth_inventory: &[],
        streams: &streams,
        mode: UnifiedMergeMode::Trades,
        parquet: &config,
        limits: worker_limits(),
        staging: &staging,
    };

    let mut worker = MaterializationPartitionWorker::new(&input).unwrap();

    let result = worker.execute(&input).unwrap();

    let validated =
        validate_physical_partition(&staging, UnifiedMergeMode::Trades, &result).unwrap();

    assert_eq!(validated.partition_id, 0);
    assert_eq!(validated.events, 5);
    assert_eq!(validated.trade_events, 5);
    assert_eq!(validated.depth_events, 0);
    assert_eq!(validated.canonical_rows, 5);
    assert_eq!(validated.files.len(), 1);
    assert_eq!(validated.first_timestamp_ns, Some(100));
    assert_eq!(validated.last_timestamp_ns, Some(500));

    assert_eq!(result.partition_id, 0);
    assert_eq!(result.references_read, 5);
    assert_eq!(result.windows_processed, 3);

    assert_eq!(result.writer_metrics.events_written, 5);
    assert_eq!(result.writer_metrics.trade_events, 5);
    assert_eq!(result.writer_metrics.depth_events, 0);
    assert_eq!(result.writer_metrics.canonical_rows, 5);

    let output = staging
        .join("timeline")
        .join("partition-000000")
        .join("part-000000.parquet");

    assert!(output.exists());

    let batches = read_parquet(&output);

    let restored_timestamps = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("event_timestamp_ns")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();

    assert_eq!(restored_timestamps, timestamps);

    for batch in batches {
        assert_eq!(batch.schema(), physical_trade_schema());
    }
}

#[test]
fn materializes_depth_partition_end_to_end() {
    let dir = tempfile::tempdir().unwrap();

    let depth_dir = dir.path().join("depth");
    let logical_dir = dir.path().join("logical");
    let staging = dir.path().join("staging");

    std::fs::create_dir_all(&logical_dir).unwrap();

    let (inventory, references) = create_depth_fixture(&depth_dir);

    let timeline_path = logical_dir.join("part-000000.parquet");

    write_timeline(&timeline_path, &references);

    let depth_inventory = vec![inventory];
    let streams = vec![depth_stream()];
    let timeline_files = vec![timeline_path];
    let config = parquet_config();

    let input = MaterializationPartitionInput {
        partition_id: 0,
        start_ns: 0,
        end_ns: 1000,
        timeline_files: &timeline_files,
        trade_inventory: &[],
        depth_inventory: &depth_inventory,
        streams: &streams,
        mode: UnifiedMergeMode::Depth,
        parquet: &config,
        limits: worker_limits(),
        staging: &staging,
    };

    let mut worker = MaterializationPartitionWorker::new(&input).unwrap();

    let result = worker.execute(&input).unwrap();

    let validated =
        validate_physical_partition(&staging, UnifiedMergeMode::Depth, &result).unwrap();

    assert_eq!(validated.events, 3);
    assert_eq!(validated.trade_events, 0);
    assert_eq!(validated.depth_events, 3);
    assert_eq!(validated.canonical_rows, 3);
    assert_eq!(validated.first_timestamp_ns, Some(100));
    assert_eq!(validated.last_timestamp_ns, Some(300));

    assert_eq!(result.references_read, 3);
    assert_eq!(result.windows_processed, 2);

    assert_eq!(result.writer_metrics.events_written, 3);
    assert_eq!(result.writer_metrics.depth_events, 3);
    assert_eq!(result.writer_metrics.trade_events, 0);
    assert_eq!(result.writer_metrics.canonical_rows, 3);

    let output = staging
        .join("timeline")
        .join("partition-000000")
        .join("part-000000.parquet");

    assert!(output.exists());

    let batches = read_parquet(&output);

    let mut timestamps = Vec::new();
    let mut level_counts = Vec::new();
    let mut boundaries = Vec::new();

    for batch in &batches {
        let ts = batch
            .column_by_name("event_timestamp_ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        timestamps.extend_from_slice(ts.values());

        let changes = batch
            .column_by_name("changes")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        for row in 0..batch.num_rows() {
            assert!(!changes.is_null(row));
            level_counts.push(changes.value_length(row));
        }

        let boundary = batch
            .column_by_name("event_boundary")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();

        for row in 0..batch.num_rows() {
            boundaries.push(boundary.value(row));
        }
    }

    assert_eq!(timestamps, vec![100, 200, 300]);
    assert_eq!(level_counts, vec![2, 0, 1]);
    assert_eq!(boundaries, vec![1, 0, 0]);
}

#[test]
fn materializes_combined_partition_end_to_end() {
    let dir = tempfile::tempdir().unwrap();

    let depth_dir = dir.path().join("depth");
    let trade_dir = dir.path().join("trades");
    let logical_dir = dir.path().join("logical");
    let staging = dir.path().join("staging");

    std::fs::create_dir_all(&trade_dir).unwrap();
    std::fs::create_dir_all(&logical_dir).unwrap();

    let (depth_inventory, depth_refs) = create_depth_fixture(&depth_dir);

    let trade_path = trade_dir.join("part-000000.parquet");

    write_parquet(&trade_path, &canonical_trades(&[150, 250, 350]));

    let trade_inventory = marketforge_engine::historical::merge::TradeTaskInventory {
        dataset_id: 88,
        task_id: 0,
        files: vec![marketforge_engine::historical::merge::TradeFileInventory {
            path: trade_path,
            part_index: 0,
            row_offset: 0,
            rows: 3,
            start_timestamp_ns: 150,
            end_timestamp_ns: 350,
        }],
        canonical_rows: 3,
        start_timestamp_ns: 150,
        end_timestamp_ns: 350,
    };

    let references = vec![
        depth_refs[0],
        timeline_reference(0, 150),
        depth_refs[1],
        timeline_reference(1, 250),
        depth_refs[2],
        timeline_reference(2, 350),
    ];

    let timeline_path = logical_dir.join("part-000000.parquet");

    write_timeline(&timeline_path, &references);

    let trades = vec![trade_inventory];
    let depths = vec![depth_inventory];

    let streams = vec![trade_stream(), depth_stream()];

    let timeline_files = vec![timeline_path];
    let config = parquet_config();

    let input = MaterializationPartitionInput {
        partition_id: 0,
        start_ns: 0,
        end_ns: 1000,
        timeline_files: &timeline_files,
        trade_inventory: &trades,
        depth_inventory: &depths,
        streams: &streams,
        mode: UnifiedMergeMode::Combined,
        parquet: &config,
        limits: worker_limits(),
        staging: &staging,
    };

    let mut worker = MaterializationPartitionWorker::new(&input).unwrap();

    let result = worker.execute(&input).unwrap();

    let validated =
        validate_physical_partition(&staging, UnifiedMergeMode::Combined, &result).unwrap();

    assert_eq!(validated.events, 6);
    assert_eq!(validated.trade_events, 3);
    assert_eq!(validated.depth_events, 3);
    assert_eq!(validated.canonical_rows, 6);
    assert_eq!(validated.first_timestamp_ns, Some(100));
    assert_eq!(validated.last_timestamp_ns, Some(350));

    assert_eq!(result.references_read, 6);
    assert_eq!(result.windows_processed, 3);

    assert_eq!(result.writer_metrics.events_written, 6);
    assert_eq!(result.writer_metrics.trade_events, 3);
    assert_eq!(result.writer_metrics.depth_events, 3);
    assert_eq!(result.writer_metrics.canonical_rows, 6);

    let output = staging
        .join("timeline")
        .join("partition-000000")
        .join("part-000000.parquet");

    assert!(output.exists());

    let batches = read_parquet(&output);

    let mut timestamps = Vec::new();
    let mut kinds = Vec::new();
    let mut trade_validity = Vec::new();
    let mut depth_validity = Vec::new();
    let mut depth_levels = Vec::new();

    for batch in &batches {
        let ts = batch
            .column_by_name("event_timestamp_ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        timestamps.extend_from_slice(ts.values());

        let event_kinds = batch
            .column_by_name("event_kind")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();

        kinds.extend_from_slice(event_kinds.values());

        let trade = batch
            .column_by_name("trade")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        let depth = batch
            .column_by_name("depth")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();

        let changes = depth
            .column_by_name("changes")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        for row in 0..batch.num_rows() {
            trade_validity.push(!trade.is_null(row));
            depth_validity.push(!depth.is_null(row));

            if !depth.is_null(row) {
                depth_levels.push(changes.value_length(row));
            }
        }
    }

    assert_eq!(timestamps, vec![100, 150, 200, 250, 300, 350]);

    assert_eq!(kinds, vec![1, 0, 1, 0, 1, 0]);

    assert_eq!(trade_validity, vec![false, true, false, true, false, true]);

    assert_eq!(depth_validity, vec![true, false, true, false, true, false]);

    assert_eq!(depth_levels, vec![2, 0, 1]);
}

#[test]
fn materializes_multiple_partitions_in_parallel() {
    let dir = tempfile::tempdir().unwrap();

    let canonical_dir = dir.path().join("canonical");
    let logical_dir = dir.path().join("logical");
    let staging = dir.path().join("physical");

    std::fs::create_dir_all(&canonical_dir).unwrap();
    std::fs::create_dir_all(&logical_dir).unwrap();

    let canonical_path = canonical_dir.join("part-000000.parquet");

    write_parquet(
        &canonical_path,
        &canonical_trades(&[100, 200, 300, 400, 500, 600]),
    );

    let trade_inventory = marketforge_engine::historical::merge::TradeTaskInventory {
        dataset_id: 88,
        task_id: 0,
        files: vec![marketforge_engine::historical::merge::TradeFileInventory {
            path: canonical_path,
            part_index: 0,
            row_offset: 0,
            rows: 6,
            start_timestamp_ns: 100,
            end_timestamp_ns: 600,
        }],
        canonical_rows: 6,
        start_timestamp_ns: 100,
        end_timestamp_ns: 600,
    };

    let partitions = [
        (0_u32, 0_i64, 250_i64, vec![100, 200]),
        (1_u32, 250_i64, 450_i64, vec![300, 400]),
        (2_u32, 450_i64, 1000_i64, vec![500, 600]),
    ];

    let mut plans = Vec::new();

    for (partition_id, start_ns, end_ns, timestamps) in partitions {
        let directory = logical_dir.join(format!("partition-{partition_id:06}"));

        std::fs::create_dir_all(&directory).unwrap();

        let timeline_path = directory.join("part-000000.parquet");

        let references = timestamps
            .iter()
            .map(|timestamp| timeline_reference((*timestamp / 100 - 1) as u64, *timestamp))
            .collect::<Vec<_>>();

        write_timeline(&timeline_path, &references);

        plans.push(MaterializationPartitionPlan {
            partition_id,
            start_ns,
            end_ns,
            timeline_files: vec![timeline_path],
            source_tasks: vec![(88, 0)],
        });
    }

    let job = merge_job(3, 4096, dir.path().join("merged"));

    let results = execute_parallel_materialization(
        plans,
        &[trade_inventory],
        &[],
        &[trade_stream()],
        UnifiedMergeMode::Trades,
        &job,
        &staging,
    )
    .unwrap();

    assert_eq!(results.len(), 3);

    let mut all_timestamps = Vec::new();

    for (index, result) in results.iter().enumerate() {
        assert_eq!(result.partition_id, index as u32);
        assert_eq!(result.references_read, 2);
        assert_eq!(result.writer_metrics.events_written, 2);

        let path = staging
            .join("timeline")
            .join(format!("partition-{index:06}"))
            .join("part-000000.parquet");

        assert!(path.exists());

        for batch in read_parquet(&path) {
            let timestamps = batch
                .column_by_name("event_timestamp_ns")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            all_timestamps.extend_from_slice(timestamps.values());
        }
    }

    assert_eq!(all_timestamps, vec![100, 200, 300, 400, 500, 600]);
}

#[test]
fn parallel_rejects_duplicate_partition_ids() {
    let dir = tempfile::tempdir().unwrap();

    let plans = vec![
        MaterializationPartitionPlan {
            partition_id: 0,
            start_ns: 0,
            end_ns: 100,
            timeline_files: vec![],
            source_tasks: vec![(88, 0)],
        },
        MaterializationPartitionPlan {
            partition_id: 0,
            start_ns: 100,
            end_ns: 200,
            timeline_files: vec![],
            source_tasks: vec![(88, 0)],
        },
    ];

    let job = merge_job(2, 4096, dir.path().join("merged"));

    assert!(
        execute_parallel_materialization(
            plans,
            &[],
            &[],
            &[],
            UnifiedMergeMode::Trades,
            &job,
            dir.path(),
        )
        .is_err()
    );
}

#[test]
fn parallel_rejects_overlapping_partitions() {
    let dir = tempfile::tempdir().unwrap();

    let plans = vec![
        MaterializationPartitionPlan {
            partition_id: 0,
            start_ns: 0,
            end_ns: 200,
            timeline_files: vec![],
            source_tasks: vec![(88, 0)],
        },
        MaterializationPartitionPlan {
            partition_id: 1,
            start_ns: 150,
            end_ns: 300,
            timeline_files: vec![],
            source_tasks: vec![(88, 0)],
        },
    ];

    let job = merge_job(2, 4096, dir.path().join("merged"));

    assert!(
        execute_parallel_materialization(
            plans,
            &[],
            &[],
            &[],
            UnifiedMergeMode::Trades,
            &job,
            dir.path(),
        )
        .is_err()
    );
}

#[test]
fn parallel_propagates_worker_failure() {
    let dir = tempfile::tempdir().unwrap();

    let plans = vec![MaterializationPartitionPlan {
        partition_id: 0,
        start_ns: 0,
        end_ns: 1000,
        timeline_files: vec![dir.path().join("missing.parquet")],
        source_tasks: vec![],
    }];

    let job = merge_job(2, 4096, dir.path().join("merged"));

    assert!(
        execute_parallel_materialization(
            plans,
            &[],
            &[],
            &[],
            UnifiedMergeMode::Trades,
            &job,
            dir.path(),
        )
        .is_err()
    );
}

#[test]
fn parallel_rejects_insufficient_memory() {
    let dir = tempfile::tempdir().unwrap();

    let plans = vec![MaterializationPartitionPlan {
        partition_id: 0,
        start_ns: 0,
        end_ns: 1000,
        timeline_files: vec![],
        source_tasks: vec![(88, 0)],
    }];

    let job = merge_job(2, 128, dir.path().join("merged"));

    assert!(
        execute_parallel_materialization(
            plans,
            &[],
            &[],
            &[],
            UnifiedMergeMode::Trades,
            &job,
            dir.path(),
        )
        .is_err()
    );
}

#[test]
fn parallel_rejects_insufficient_file_budget() {
    let dir = tempfile::tempdir().unwrap();

    let plans = vec![MaterializationPartitionPlan {
        partition_id: 0,
        start_ns: 0,
        end_ns: 1000,
        timeline_files: vec![],
        source_tasks: vec![(88, 0)],
    }];

    let mut job = merge_job(2, 4096, dir.path().join("merged"));

    job.resources.max_open_files = 1;

    assert!(
        execute_parallel_materialization(
            plans,
            &[],
            &[],
            &[],
            UnifiedMergeMode::Trades,
            &job,
            dir.path(),
        )
        .is_err()
    );
}

#[test]
fn physical_validator_rejects_incorrect_event_count() {
    let dir = tempfile::tempdir().unwrap();

    let staging = dir.path();

    let output_dir = staging.join("timeline").join("partition-000000");

    std::fs::create_dir_all(&output_dir).unwrap();

    let output = output_dir.join("part-000000.parquet");

    // Reuse the existing physical trade fixture from writer tests,
    // or construct a physical trade batch using the canonical builder.
    let batch = physical_trade_fixture(&[100, 200, 300]);

    write_parquet(&output, &batch);

    let mut result = physical_result(0, 0, 1000, 3);

    result.writer_metrics.events_written = 4;

    assert!(validate_physical_partition(staging, UnifiedMergeMode::Trades, &result,).is_err());
}

#[test]
fn physical_validator_rejects_timestamp_regression() {
    let dir = tempfile::tempdir().unwrap();

    let output_dir = dir.path().join("timeline").join("partition-000000");

    std::fs::create_dir_all(&output_dir).unwrap();

    write_parquet(
        &output_dir.join("part-000000.parquet"),
        &physical_trade_fixture(&[100, 300, 200]),
    );

    let result = physical_result(0, 0, 1000, 3);

    assert!(validate_physical_partition(dir.path(), UnifiedMergeMode::Trades, &result,).is_err());
}

#[test]
fn physical_validator_rejects_missing_file() {
    let dir = tempfile::tempdir().unwrap();

    let result = physical_result(0, 0, 1000, 3);

    assert!(validate_physical_partition(dir.path(), UnifiedMergeMode::Trades, &result,).is_err());
}

#[test]
fn physical_validator_rejects_wrong_schema() {
    let dir = tempfile::tempdir().unwrap();

    let output_dir = dir.path().join("timeline").join("partition-000000");

    std::fs::create_dir_all(&output_dir).unwrap();

    write_parquet(
        &output_dir.join("part-000000.parquet"),
        &canonical_trades(&[100, 200]),
    );

    let result = physical_result(0, 0, 1000, 2);

    assert!(validate_physical_partition(dir.path(), UnifiedMergeMode::Trades, &result,).is_err());
}

#[test]
fn physical_validator_rejects_canonical_range_overflow() {
    let dir = tempfile::tempdir().unwrap();

    let output_dir = dir.path().join("timeline").join("partition-000000");

    std::fs::create_dir_all(&output_dir).unwrap();

    let batch = physical_trade_fixture(&[100, 200]);
    let schema = batch.schema();

    let mut columns = batch.columns().to_vec();

    let index = schema.index_of("canonical_row_offset").unwrap();

    columns[index] = Arc::new(UInt64Array::from(vec![0, u64::MAX]));

    let corrupted = RecordBatch::try_new(schema, columns).unwrap();

    write_parquet(&output_dir.join("part-000000.parquet"), &corrupted);

    let result = physical_result(0, 0, 1000, 2);

    assert!(validate_physical_partition(dir.path(), UnifiedMergeMode::Trades, &result,).is_err());
}

#[test]
fn physical_validator_rejects_null_provenance() {
    let dir = tempfile::tempdir().unwrap();

    let output_dir = dir.path().join("timeline").join("partition-000000");

    std::fs::create_dir_all(&output_dir).unwrap();

    let batch = physical_trade_fixture(&[100, 200]);
    let schema = batch.schema();

    let mut columns = batch.columns().to_vec();

    let index = schema.index_of("task_id").unwrap();

    columns[index] = Arc::new(UInt64Array::from(vec![Some(0), None]));

    // Construct without schema nullability validation,
    // allowing the validator to inspect corrupted data.
    let corrupted = RecordBatch::try_new(schema, columns);

    assert!(
        corrupted.is_err(),
        "Arrow should reject nulls in required fields"
    );
}
#[test]
fn global_validator_accepts_multiple_partitions() {
    let dir = tempfile::tempdir().unwrap();

    let partitions = vec![
        create_physical_partition(dir.path(), 0, 0, 250, &[100, 200]),
        create_physical_partition(dir.path(), 1, 250, 450, &[300, 400]),
        create_physical_partition(dir.path(), 2, 450, 700, &[500, 600]),
    ];

    let validated =
        validate_physical_dataset(dir.path(), UnifiedMergeMode::Trades, &partitions).unwrap();

    assert_eq!(validated.events, 6);
    assert_eq!(validated.trade_events, 6);
    assert_eq!(validated.depth_events, 0);
    assert_eq!(validated.canonical_rows, 6);

    assert_eq!(validated.files_written, 3);
    assert_eq!(validated.files.len(), 3);

    assert_eq!(validated.first_timestamp_ns, Some(100));
    assert_eq!(validated.last_timestamp_ns, Some(600));

    assert_eq!(
        validated.files[0].path,
        "timeline/partition-000000/part-000000.parquet"
    );

    assert_eq!(
        validated.files[1].path,
        "timeline/partition-000001/part-000000.parquet"
    );

    assert_eq!(
        validated.files[2].path,
        "timeline/partition-000002/part-000000.parquet"
    );
    for file in &validated.files {
        assert_eq!(file.rows, Some(2));
        assert!(file.size_bytes > 0);

        assert!(file.start_timestamp_ns.is_some());
        assert!(file.end_timestamp_ns.is_some());

        assert!(file.start_timestamp_ns.unwrap() <= file.end_timestamp_ns.unwrap());
    }
}
#[test]
fn global_validator_accepts_empty_partitions() {
    let dir = tempfile::tempdir().unwrap();

    let partitions = vec![
        create_physical_partition(dir.path(), 0, 0, 250, &[100, 200]),
        create_physical_partition(dir.path(), 1, 250, 450, &[]),
        create_physical_partition(dir.path(), 2, 450, 700, &[500, 600]),
    ];

    let validated =
        validate_physical_dataset(dir.path(), UnifiedMergeMode::Trades, &partitions).unwrap();

    assert_eq!(validated.events, 4);
    assert_eq!(validated.files_written, 2);
    assert_eq!(validated.first_timestamp_ns, Some(100));
    assert_eq!(validated.last_timestamp_ns, Some(600));
}
#[test]
fn global_validator_rejects_duplicate_partition_ids() {
    let dir = tempfile::tempdir().unwrap();

    let partitions = vec![
        create_physical_partition(dir.path(), 0, 0, 250, &[100]),
        create_physical_partition(dir.path(), 0, 250, 500, &[300]),
    ];

    assert!(validate_physical_dataset(dir.path(), UnifiedMergeMode::Trades, &partitions,).is_err());
}
#[test]
fn global_validator_rejects_overlapping_partitions() {
    let dir = tempfile::tempdir().unwrap();

    let partitions = vec![
        create_physical_partition(dir.path(), 0, 0, 300, &[100, 200]),
        create_physical_partition(dir.path(), 1, 250, 500, &[350, 400]),
    ];

    assert!(validate_physical_dataset(dir.path(), UnifiedMergeMode::Trades, &partitions,).is_err());
}
#[test]
fn global_validator_rejects_missing_physical_files() {
    let dir = tempfile::tempdir().unwrap();

    let partitions = vec![create_physical_partition(
        dir.path(),
        0,
        0,
        250,
        &[100, 200],
    )];

    let path = dir
        .path()
        .join("timeline")
        .join("partition-000000")
        .join("part-000000.parquet");

    std::fs::remove_file(path).unwrap();

    assert!(validate_physical_dataset(dir.path(), UnifiedMergeMode::Trades, &partitions,).is_err());
}
#[test]
fn global_validator_rejects_incorrect_counts() {
    let dir = tempfile::tempdir().unwrap();

    let mut partition = create_physical_partition(dir.path(), 0, 0, 250, &[100, 200]);

    partition.writer_metrics.events_written = 3;

    assert!(
        validate_physical_dataset(dir.path(), UnifiedMergeMode::Trades, &[partition],).is_err()
    );
}
#[test]
fn global_validator_rejects_unordered_partitions() {
    let dir = tempfile::tempdir().unwrap();

    let first = create_physical_partition(dir.path(), 0, 0, 250, &[100, 200]);

    let second = create_physical_partition(dir.path(), 1, 250, 500, &[300, 400]);

    assert!(
        validate_physical_dataset(dir.path(), UnifiedMergeMode::Trades, &[second, first],).is_err()
    );
}

#[test]
fn physical_validator_rejects_unexpected_partition_file() {
    let dir = tempfile::tempdir().unwrap();

    let partition = create_physical_partition(dir.path(), 0, 0, 250, &[100, 200]);

    let unexpected = dir
        .path()
        .join("timeline")
        .join("partition-000000")
        .join("temporary.tmp");

    std::fs::write(&unexpected, b"unexpected").unwrap();

    assert!(
        validate_physical_dataset(dir.path(), UnifiedMergeMode::Trades, &[partition],).is_err()
    );
}
