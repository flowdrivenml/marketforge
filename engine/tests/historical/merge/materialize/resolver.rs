use std::{fs::File, path::Path, sync::Arc};

use arrow_array::{
    Array, ArrayRef, BooleanArray, Decimal256Array, Int64Array, RecordBatch, StringArray,
    UInt64Array,
};
use arrow_buffer::i256;
use arrow_schema::SchemaRef;
use parquet::arrow::ArrowWriter;

use marketforge_engine::historical::merge::{
    CanonicalReadPlanner, CanonicalRowGroupIndex, CanonicalRowIndex, GroupedCanonicalDecoder,
};
use {
    marketforge_engine::historical::merge::CanonicalParquetReader,
    marketforge_engine::historical::merge::CanonicalReferenceResolver,
    marketforge_engine::historical::merge::CanonicalRowGroupCache,
    marketforge_engine::historical::merge::CanonicalTaskKey,
    marketforge_engine::historical::merge::UnifiedTimelineRecord,
    marketforge_engine::shared::persistence::parquet::depth_schema,
    marketforge_engine::shared::persistence::parquet::trade_schema,
};
// -----------------------------------------------------------------------------
// Fixture construction
// -----------------------------------------------------------------------------

fn decimal(values: &[i64]) -> ArrayRef {
    let scale = i256::from_i128(10_i128.pow(28));

    let values = values
        .iter()
        .map(|value| i256::from_i128(*value as i128) * scale)
        .collect::<Vec<_>>();

    Arc::new(
        Decimal256Array::from(values)
            .with_precision_and_scale(76, 28)
            .unwrap(),
    )
}

fn strings(values: &[&str]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

fn write_parquet(path: &Path, schema: SchemaRef, columns: Vec<ArrayRef>) {
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();

    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();

    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn write_trades(path: &Path) {
    let timestamps = [100_i64, 300, 500];

    write_parquet(
        path,
        trade_schema(),
        vec![
            Arc::new(Int64Array::from(timestamps.to_vec())),
            Arc::new(Int64Array::from(vec![None; 3])),
            strings(&["bybit", "bybit", "bybit"]),
            Arc::new(Int64Array::from(vec![1, 1, 1])),
            strings(&["BTCUSDT", "BTCUSDT", "BTCUSDT"]),
            strings(&["bybit:btc:trades"; 3]),
            strings(&["t0", "t1", "t2"]),
            Arc::new(UInt64Array::from(vec![None; 3])),
            strings(&["buy", "sell", "buy"]),
            decimal(&[100, 300, 500]),
            decimal(&[1, 2, 3]),
            decimal(&[100, 600, 1500]),
            Arc::new(
                Decimal256Array::from(vec![None; 3])
                    .with_precision_and_scale(76, 28)
                    .unwrap(),
            ),
            Arc::new(BooleanArray::from(vec![None; 3])),
            Arc::new(
                Decimal256Array::from(vec![None; 3])
                    .with_precision_and_scale(76, 28)
                    .unwrap(),
            ),
            Arc::new(
                Decimal256Array::from(vec![None; 3])
                    .with_precision_and_scale(76, 28)
                    .unwrap(),
            ),
            Arc::new(
                Decimal256Array::from(vec![None; 3])
                    .with_precision_and_scale(76, 28)
                    .unwrap(),
            ),
            Arc::new(
                Decimal256Array::from(vec![None; 3])
                    .with_precision_and_scale(76, 28)
                    .unwrap(),
            ),
        ],
    );
}

fn write_depth(path: &Path) {
    // Two depth events:
    //
    // Event 0: timestamp 200, two canonical levels.
    // Event 1: timestamp 400, one canonical level.
    //
    // A third zero-level event is tested separately.

    write_parquet(
        path,
        depth_schema(),
        vec![
            Arc::new(Int64Array::from(vec![200, 200, 400])),
            Arc::new(Int64Array::from(vec![None; 3])),
            strings(&["bybit", "bybit", "bybit"]),
            Arc::new(Int64Array::from(vec![1, 1, 1])),
            strings(&["BTCUSDT", "BTCUSDT", "BTCUSDT"]),
            strings(&["bybit:btc:depth"; 3]),
            strings(&["bid", "ask", "bid"]),
            decimal(&[99, 101, 100]),
            decimal(&[5, 3, 0]),
            decimal(&[495, 303, 0]),
            Arc::new(
                Decimal256Array::from(vec![None; 3])
                    .with_precision_and_scale(76, 28)
                    .unwrap(),
            ),
            Arc::new(UInt64Array::from(vec![None; 3])),
        ],
    );
}

// -----------------------------------------------------------------------------
// Resolver setup
// -----------------------------------------------------------------------------

fn resolver(trade_path: &Path, depth_path: &Path) -> CanonicalReferenceResolver {
    let reader = CanonicalParquetReader::new(2).unwrap();
    let cache = CanonicalRowGroupCache::new(1024 * 1024).unwrap();

    let mut resolver = CanonicalReferenceResolver::new(reader, cache);

    resolver
        .register_task(
            CanonicalTaskKey {
                dataset_id: 10,
                task_id: 0,
                event_kind: 0,
            },
            vec![(trade_path.to_path_buf(), 3)],
        )
        .unwrap();

    resolver
        .register_task(
            CanonicalTaskKey {
                dataset_id: 20,
                task_id: 0,
                event_kind: 1,
            },
            vec![(depth_path.to_path_buf(), 3)],
        )
        .unwrap();

    resolver
}

// -----------------------------------------------------------------------------
// Logical references
// -----------------------------------------------------------------------------

fn reference(
    timestamp: i64,
    kind: u8,
    dataset_id: u64,
    ordinal: u64,
    offset: u64,
    count: u64,
) -> UnifiedTimelineRecord {
    UnifiedTimelineRecord {
        event_timestamp_ns: timestamp,
        stream_rank: if kind == 0 { 0 } else { 1 },
        event_kind: kind,
        dataset_id,
        task_id: 0,
        event_ordinal: ordinal,
        canonical_row_offset: offset,
        canonical_row_count: count,
        event_boundary: (kind == 1).then_some(0),
        source_operation: (kind == 1).then_some(1),
    }
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[test]
fn resolves_mixed_events_in_chronological_order() {
    let dir = tempfile::tempdir().unwrap();

    let trades = dir.path().join("trades.parquet");
    let depth = dir.path().join("depth.parquet");

    write_trades(&trades);
    write_depth(&depth);

    let mut resolver = resolver(&trades, &depth);

    let references = vec![
        reference(100, 0, 10, 0, 0, 1),
        reference(200, 1, 20, 0, 0, 2),
        reference(300, 0, 10, 1, 1, 1),
        reference(400, 1, 20, 1, 2, 1),
        reference(500, 0, 10, 2, 2, 1),
    ];

    let resolved = resolver.resolve_batch(&references).unwrap();

    assert_eq!(resolved.len(), 5);

    assert_eq!(
        resolved
            .iter()
            .map(|e| e.reference.event_timestamp_ns)
            .collect::<Vec<_>>(),
        vec![100, 200, 300, 400, 500]
    );

    assert_eq!(
        resolved
            .iter()
            .map(|e| e.canonical_rows())
            .collect::<Vec<_>>(),
        vec![1, 2, 1, 1, 1]
    );
}

// -----------------------------------------------------------------------------
// Grouped decoder fixtures
// -----------------------------------------------------------------------------

fn grouped_components_with_limit(
    trade_path: &Path,
    depth_path: &Path,
    max_open_cursors: usize,
) -> (CanonicalReadPlanner, GroupedCanonicalDecoder) {
    let mut planner = CanonicalReadPlanner::new();

    planner
        .register_task(
            CanonicalTaskKey {
                dataset_id: 10,
                task_id: 0,
                event_kind: 0,
            },
            CanonicalRowIndex::new(vec![(trade_path.to_path_buf(), 3)]).unwrap(),
        )
        .unwrap();

    planner
        .register_task(
            CanonicalTaskKey {
                dataset_id: 20,
                task_id: 0,
                event_kind: 1,
            },
            CanonicalRowIndex::new(vec![(depth_path.to_path_buf(), 3)]).unwrap(),
        )
        .unwrap();

    let trade_groups = CanonicalRowGroupIndex::open(trade_path).unwrap();

    let depth_groups = CanonicalRowGroupIndex::open(depth_path).unwrap();

    let mut decoder =
        GroupedCanonicalDecoder::new(CanonicalParquetReader::new(2).unwrap(), max_open_cursors);
    planner.register_row_groups(trade_groups.clone()).unwrap();
    planner.register_row_groups(depth_groups.clone()).unwrap();

    decoder.register_row_groups(trade_groups).unwrap();
    decoder.register_row_groups(depth_groups).unwrap();

    (planner, decoder)
}
fn grouped_components(
    trade_path: &Path,
    depth_path: &Path,
) -> (CanonicalReadPlanner, GroupedCanonicalDecoder) {
    grouped_components_with_limit(trade_path, depth_path, 32)
}
// -----------------------------------------------------------------------------
// Grouped decoding correctness
// -----------------------------------------------------------------------------

#[test]
fn grouped_decoder_matches_sequential_resolver() {
    let dir = tempfile::tempdir().unwrap();

    let trades = dir.path().join("trades.parquet");
    let depth = dir.path().join("depth.parquet");

    write_trades(&trades);
    write_depth(&depth);

    let references = vec![
        reference(100, 0, 10, 0, 0, 1),
        reference(200, 1, 20, 0, 0, 2),
        reference(300, 0, 10, 1, 1, 1),
        reference(400, 1, 20, 1, 2, 1),
        reference(500, 0, 10, 2, 2, 1),
    ];

    let mut sequential = resolver(&trades, &depth);

    let expected = sequential.resolve_batch(&references).unwrap();

    let (planner, mut decoder) = grouped_components(&trades, &depth);

    let plan = planner.plan(&references).unwrap();

    let actual = decoder.decode(&plan).unwrap();

    assert_eq!(actual.len(), expected.len());
    assert_eq!(plan.canonical_rows, 6);

    for (actual, expected) in actual.iter().zip(expected.iter()) {
        assert_eq!(actual.reference, expected.reference);

        assert_eq!(actual.canonical_rows(), expected.canonical_rows());

        assert_eq!(actual.canonical_batches, expected.canonical_batches);
    }
}

#[test]
fn grouped_decoder_preserves_zero_level_events() {
    let dir = tempfile::tempdir().unwrap();

    let trades = dir.path().join("trades.parquet");
    let depth = dir.path().join("depth.parquet");

    write_trades(&trades);
    write_depth(&depth);

    let references = vec![
        reference(200, 1, 20, 0, 0, 2),
        reference(600, 1, 20, 2, 3, 0),
        reference(400, 1, 20, 1, 2, 1),
    ];

    let (planner, mut decoder) = grouped_components(&trades, &depth);

    let plan = planner.plan(&references).unwrap();

    assert_eq!(plan.event_slice_counts[1], 0);

    let events = decoder.decode(&plan).unwrap();

    assert_eq!(events.len(), 3);

    assert_eq!(events[0].canonical_rows(), 2);
    assert_eq!(events[1].canonical_rows(), 0);
    assert_eq!(events[2].canonical_rows(), 1);

    assert!(events[1].canonical_batches.is_empty());

    assert_eq!(events[1].reference.event_timestamp_ns, 600);
}

#[test]
fn grouped_decoder_preserves_forward_progress_across_windows() {
    let dir = tempfile::tempdir().unwrap();

    let trades = dir.path().join("trades.parquet");
    let depth = dir.path().join("depth.parquet");

    write_trades(&trades);
    write_depth(&depth);

    let (planner, mut decoder) = grouped_components(&trades, &depth);

    // First materialization window.
    let first = vec![
        reference(100, 0, 10, 0, 0, 1),
        reference(200, 1, 20, 0, 0, 2),
    ];

    // Second window continues from the previous canonical positions.
    let second = vec![
        reference(300, 0, 10, 1, 1, 1),
        reference(400, 1, 20, 1, 2, 1),
    ];

    let first_plan = planner.plan(&first).unwrap();
    let first_events = decoder.decode(&first_plan).unwrap();

    let second_plan = planner.plan(&second).unwrap();
    let second_events = decoder.decode(&second_plan).unwrap();

    assert_eq!(first_events.len(), 2);
    assert_eq!(second_events.len(), 2);

    // Preserve original chronological event references.
    assert_eq!(first_events[0].reference, first[0]);
    assert_eq!(first_events[1].reference, first[1]);

    assert_eq!(second_events[0].reference, second[0]);
    assert_eq!(second_events[1].reference, second[1]);

    // Preserve exact canonical row counts.
    assert_eq!(first_events[0].canonical_rows(), 1);
    assert_eq!(first_events[1].canonical_rows(), 2);

    assert_eq!(second_events[0].canonical_rows(), 1);
    assert_eq!(second_events[1].canonical_rows(), 1);

    // Verify actual canonical data against the independent resolver.
    let mut sequential = resolver(&trades, &depth);

    let expected_first = sequential.resolve_batch(&first).unwrap();
    let expected_second = sequential.resolve_batch(&second).unwrap();

    for (actual, expected) in first_events.iter().zip(expected_first.iter()) {
        assert_eq!(actual.canonical_batches, expected.canonical_batches);
    }

    for (actual, expected) in second_events.iter().zip(expected_second.iter()) {
        assert_eq!(actual.canonical_batches, expected.canonical_batches);
    }
}

#[test]
fn grouped_decoder_rejects_duplicate_planned_slice() {
    let dir = tempfile::tempdir().unwrap();

    let trades = dir.path().join("trades.parquet");
    let depth = dir.path().join("depth.parquet");

    write_trades(&trades);
    write_depth(&depth);

    let (planner, mut decoder) = grouped_components(&trades, &depth);

    let references = vec![reference(200, 1, 20, 0, 0, 2)];

    let mut plan = planner.plan(&references).unwrap();

    let reads = plan.groups.values_mut().next().unwrap();

    let duplicate = reads[0].clone();
    reads.push(duplicate);

    assert!(decoder.decode(&plan).is_err());
}

#[test]
fn grouped_decoder_rejects_missing_planned_slice() {
    let dir = tempfile::tempdir().unwrap();

    let trades = dir.path().join("trades.parquet");
    let depth = dir.path().join("depth.parquet");

    write_trades(&trades);
    write_depth(&depth);

    let (planner, mut decoder) = grouped_components(&trades, &depth);

    let references = vec![reference(200, 1, 20, 0, 0, 2)];

    let mut plan = planner.plan(&references).unwrap();

    plan.groups.clear();

    assert!(decoder.decode(&plan).is_err());
}

#[test]
fn preserves_zero_level_depth_event() {
    let dir = tempfile::tempdir().unwrap();

    let trades = dir.path().join("trades.parquet");
    let depth = dir.path().join("depth.parquet");

    write_trades(&trades);
    write_depth(&depth);

    let mut resolver = resolver(&trades, &depth);

    let event = resolver.resolve(reference(600, 1, 20, 2, 3, 0)).unwrap();

    assert_eq!(event.canonical_rows(), 0);
    assert!(event.canonical_batches.is_empty());
}

#[test]
fn rejects_canonical_timestamp_mismatch() {
    let dir = tempfile::tempdir().unwrap();

    let trades = dir.path().join("trades.parquet");
    let depth = dir.path().join("depth.parquet");

    write_trades(&trades);
    write_depth(&depth);

    let mut resolver = resolver(&trades, &depth);

    let result = resolver.resolve(reference(999, 0, 10, 0, 0, 1));

    assert!(result.is_err());
}

#[test]
fn rejects_unknown_canonical_task() {
    let dir = tempfile::tempdir().unwrap();

    let trades = dir.path().join("trades.parquet");
    let depth = dir.path().join("depth.parquet");

    write_trades(&trades);
    write_depth(&depth);

    let mut resolver = resolver(&trades, &depth);

    let result = resolver.resolve(reference(100, 0, 999, 0, 0, 1));

    assert!(result.is_err());
}

#[test]
fn preserves_exact_decimal256_values() {
    let dir = tempfile::tempdir().unwrap();

    let trades = dir.path().join("trades.parquet");
    let depth = dir.path().join("depth.parquet");

    write_trades(&trades);
    write_depth(&depth);

    let mut resolver = resolver(&trades, &depth);

    let event = resolver.resolve(reference(100, 0, 10, 0, 0, 1)).unwrap();

    let batch = &event.canonical_batches[0];

    let prices = batch
        .column_by_name("price")
        .unwrap()
        .as_any()
        .downcast_ref::<Decimal256Array>()
        .unwrap();

    let expected = i256::from_i128(100 * 10_i128.pow(28));

    assert_eq!(prices.value(0), expected);
}

#[test]
fn grouped_decoder_rejects_timestamp_mismatch() {
    let dir = tempfile::tempdir().unwrap();

    let trades = dir.path().join("trades.parquet");
    let depth = dir.path().join("depth.parquet");

    write_trades(&trades);
    write_depth(&depth);

    let (planner, mut decoder) = grouped_components(&trades, &depth);

    // Canonical trade timestamp is 100, not 999.
    let references = vec![reference(999, 0, 10, 0, 0, 1)];

    let plan = planner.plan(&references).unwrap();

    assert!(decoder.decode(&plan).is_err());
}

fn write_depth_multigroup(path: &Path) {
    let schema = depth_schema();
    let file = File::create(path).unwrap();

    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();

    // Three row groups, each containing two canonical levels.
    // All six levels belong to one depth event.
    for group in 0..3 {
        let prices = [100 + group * 2, 101 + group * 2];

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![200, 200])),
                Arc::new(Int64Array::from(vec![None; 2])),
                strings(&["bybit", "bybit"]),
                Arc::new(Int64Array::from(vec![1, 1])),
                strings(&["BTCUSDT", "BTCUSDT"]),
                strings(&["bybit:btc:depth"; 2]),
                strings(&["bid", "ask"]),
                decimal(&prices),
                decimal(&[1, 2]),
                decimal(&[100, 200]),
                Arc::new(
                    Decimal256Array::from(vec![None; 2])
                        .with_precision_and_scale(76, 28)
                        .unwrap(),
                ),
                Arc::new(UInt64Array::from(vec![None; 2])),
            ],
        )
        .unwrap();

        writer.write(&batch).unwrap();
        writer.flush().unwrap();
    }

    writer.close().unwrap();
}

#[test]
fn grouped_decoder_preserves_depth_across_row_groups() {
    let dir = tempfile::tempdir().unwrap();

    let depth = dir.path().join("depth.parquet");
    write_depth_multigroup(&depth);

    let key = CanonicalTaskKey {
        dataset_id: 20,
        task_id: 0,
        event_kind: 1,
    };

    let index = CanonicalRowIndex::new(vec![(depth.clone(), 6)]).unwrap();

    let row_groups = CanonicalRowGroupIndex::open(&depth).unwrap();

    assert_eq!(row_groups.row_group_count(), 3);

    let mut planner = CanonicalReadPlanner::new();
    planner.register_task(key, index).unwrap();
    planner.register_row_groups(row_groups.clone()).unwrap();

    let mut decoder = GroupedCanonicalDecoder::new(CanonicalParquetReader::new(2).unwrap(), 32);

    decoder.register_row_groups(row_groups).unwrap();

    let references = vec![reference(200, 1, 20, 0, 0, 6)];

    let plan = planner.plan(&references).unwrap();

    assert_eq!(plan.event_slice_counts, vec![3]);
    assert_eq!(plan.row_group_count(), 3);
    assert_eq!(plan.canonical_rows, 6);

    let events = decoder.decode(&plan).unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].canonical_rows(), 6);

    // Verify canonical level ordering across all three row groups.
    let prices: Vec<i256> = events[0]
        .canonical_batches
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column_by_name("price")
                .unwrap()
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap();

            column.values().iter().copied().collect::<Vec<_>>()
        })
        .collect();

    let scale = 10_i128.pow(28);

    let expected: Vec<i256> = (100..106)
        .map(|price| i256::from_i128(price * scale))
        .collect();

    assert_eq!(prices, expected);
}
#[test]
fn forward_decoder_reopens_evicted_cursors() {
    let dir = tempfile::tempdir().unwrap();

    let trades = dir.path().join("trades.parquet");
    let depth = dir.path().join("depth.parquet");

    write_trades(&trades);
    write_depth(&depth);

    // Only one canonical reader may remain open.
    let (planner, mut decoder) = grouped_components_with_limit(&trades, &depth, 1);

    let windows = [
        vec![reference(100, 0, 10, 0, 0, 1)],
        vec![reference(200, 1, 20, 0, 0, 2)],
        vec![reference(300, 0, 10, 1, 1, 1)],
        vec![reference(400, 1, 20, 1, 2, 1)],
        vec![reference(500, 0, 10, 2, 2, 1)],
    ];

    let mut sequential = resolver(&trades, &depth);

    for references in windows {
        let expected = sequential.resolve_batch(&references).unwrap();

        let plan = planner.plan(&references).unwrap();
        let actual = decoder.decode(&plan).unwrap();

        assert_eq!(actual.len(), expected.len());

        for (actual, expected) in actual.iter().zip(expected.iter()) {
            assert_eq!(actual.reference, expected.reference);
            assert_eq!(actual.canonical_batches, expected.canonical_batches);
        }
    }
}
#[test]
fn forward_decoder_rejects_backward_read_after_eviction() {
    let dir = tempfile::tempdir().unwrap();

    let trades = dir.path().join("trades.parquet");
    let depth = dir.path().join("depth.parquet");

    write_trades(&trades);
    write_depth(&depth);

    let (planner, mut decoder) = grouped_components_with_limit(&trades, &depth, 1);

    // Read trade row 0.
    decoder
        .decode(&planner.plan(&[reference(100, 0, 10, 0, 0, 1)]).unwrap())
        .unwrap();

    // Reading depth evicts the trade cursor.
    decoder
        .decode(&planner.plan(&[reference(200, 1, 20, 0, 0, 2)]).unwrap())
        .unwrap();

    // Reopening the trade file at row 0 must be rejected.
    let backward = planner.plan(&[reference(100, 0, 10, 0, 0, 1)]).unwrap();

    assert!(decoder.decode(&backward).is_err());
}
#[test]
fn forward_decoder_continues_within_same_row_group_after_eviction() {
    let dir = tempfile::tempdir().unwrap();

    let trades = dir.path().join("trades.parquet");
    let depth = dir.path().join("depth.parquet");

    write_trades(&trades);
    write_depth(&depth);

    let (planner, mut decoder) = grouped_components_with_limit(&trades, &depth, 1);

    let first = vec![reference(100, 0, 10, 0, 0, 1)];
    let intervening = vec![reference(200, 1, 20, 0, 0, 2)];
    let continuation = vec![reference(300, 0, 10, 1, 1, 1)];

    // Read the first trade.
    decoder.decode(&planner.plan(&first).unwrap()).unwrap();

    // Read depth, forcing eviction of the trade cursor.
    decoder
        .decode(&planner.plan(&intervening).unwrap())
        .unwrap();

    // Reopen the trade cursor and continue forward.
    let actual = decoder
        .decode(&planner.plan(&continuation).unwrap())
        .unwrap();

    // Compare with the independent sequential resolver.
    let mut sequential = resolver(&trades, &depth);
    let expected = sequential.resolve_batch(&continuation).unwrap();

    assert_eq!(actual.len(), 1);

    assert_eq!(actual[0].canonical_batches, expected[0].canonical_batches);
}
