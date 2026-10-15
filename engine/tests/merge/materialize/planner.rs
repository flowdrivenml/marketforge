use std::{fs::File, path::Path, sync::Arc};

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;

use marketforge_engine::historical::merge::{
    CanonicalReadPlanner, CanonicalRowGroupIndex, CanonicalRowIndex, CanonicalTaskKey,
    UnifiedTimelineRecord,
};

fn write_fixture(path: &Path, values: &[i64], group_size: usize) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));

    let file = File::create(path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();

    for chunk in values.chunks(group_size) {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(chunk.to_vec()))],
        )
        .unwrap();

        writer.write(&batch).unwrap();
        writer.flush().unwrap();
    }

    writer.close().unwrap();
}

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
        stream_rank: kind as u32,
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

fn setup(files: Vec<(&Path, u64)>, kind: u8, dataset_id: u64) -> CanonicalReadPlanner {
    let mut planner = CanonicalReadPlanner::new();

    let task_files = files
        .iter()
        .map(|(path, rows)| (path.to_path_buf(), *rows))
        .collect();

    planner
        .register_task(
            CanonicalTaskKey {
                dataset_id,
                task_id: 0,
                event_kind: kind,
            },
            CanonicalRowIndex::new(task_files).unwrap(),
        )
        .unwrap();

    for (path, _) in files {
        planner
            .register_row_groups(CanonicalRowGroupIndex::open(path).unwrap())
            .unwrap();
    }

    planner
}

#[test]
fn groups_references_by_row_group() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("trades.parquet");

    write_fixture(&path, &(0..30).collect::<Vec<_>>(), 10);

    let planner = setup(vec![(&path, 30)], 0, 10);

    let references = vec![
        reference(100, 0, 10, 0, 2, 1),
        reference(200, 0, 10, 1, 5, 1),
        reference(300, 0, 10, 2, 12, 1),
        reference(400, 0, 10, 3, 7, 1),
    ];

    let plan = planner.plan(&references).unwrap();

    assert_eq!(plan.event_count(), 4);
    assert_eq!(plan.row_group_count(), 2);
    assert_eq!(plan.read_count(), 4);
    assert_eq!(plan.canonical_rows, 4);

    assert_eq!(plan.event_slice_counts, vec![1, 1, 1, 1]);
}

#[test]
fn preserves_chronological_reference_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("trades.parquet");

    write_fixture(&path, &(0..30).collect::<Vec<_>>(), 10);

    let planner = setup(vec![(&path, 30)], 0, 10);

    let references = vec![
        reference(100, 0, 10, 0, 20, 1),
        reference(200, 0, 10, 1, 2, 1),
        reference(300, 0, 10, 2, 15, 1),
    ];

    let plan = planner.plan(&references).unwrap();

    assert_eq!(plan.references, references);

    // Physical read order may differ, but original event indices survive.
    let mut event_indices = plan
        .groups
        .values()
        .flat_map(|reads| reads.iter().map(|read| read.event_index))
        .collect::<Vec<_>>();

    event_indices.sort_unstable();

    assert_eq!(event_indices, vec![0, 1, 2]);
}

#[test]
fn splits_depth_event_across_row_groups() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("depth.parquet");

    write_fixture(&path, &(0..30).collect::<Vec<_>>(), 10);

    let planner = setup(vec![(&path, 30)], 1, 20);

    let references = vec![reference(100, 1, 20, 0, 8, 15)];

    let plan = planner.plan(&references).unwrap();

    assert_eq!(plan.event_count(), 1);
    assert_eq!(plan.row_group_count(), 3);
    assert_eq!(plan.read_count(), 3);
    assert_eq!(plan.canonical_rows, 15);
    assert_eq!(plan.event_slice_counts, vec![3]);

    let mut slices = plan
        .groups
        .values()
        .flat_map(|reads| reads.iter())
        .collect::<Vec<_>>();

    slices.sort_by_key(|slice| slice.slice_index);

    assert_eq!(
        slices
            .iter()
            .map(|slice| slice.row_count)
            .collect::<Vec<_>>(),
        vec![2, 10, 3]
    );
}

#[test]
fn splits_depth_event_across_files() {
    let dir = tempfile::tempdir().unwrap();

    let first = dir.path().join("part-000000.parquet");
    let second = dir.path().join("part-000001.parquet");

    write_fixture(&first, &(0..10).collect::<Vec<_>>(), 5);
    write_fixture(&second, &(10..20).collect::<Vec<_>>(), 5);

    let planner = setup(vec![(&first, 10), (&second, 10)], 1, 20);

    let plan = planner.plan(&[reference(100, 1, 20, 0, 8, 6)]).unwrap();

    assert_eq!(plan.event_slice_counts, vec![2]);
    assert_eq!(plan.canonical_rows, 6);
    assert_eq!(plan.row_group_count(), 2);
}

#[test]
fn preserves_zero_level_depth_event() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("depth.parquet");

    write_fixture(&path, &(0..10).collect::<Vec<_>>(), 5);

    let planner = setup(vec![(&path, 10)], 1, 20);

    let plan = planner.plan(&[reference(100, 1, 20, 0, 10, 0)]).unwrap();

    assert_eq!(plan.event_count(), 1);
    assert_eq!(plan.row_group_count(), 0);
    assert_eq!(plan.canonical_rows, 0);
    assert_eq!(plan.event_slice_counts, vec![0]);
}

#[test]
fn rejects_unknown_task() {
    let planner = CanonicalReadPlanner::new();

    let result = planner.plan(&[reference(100, 0, 999, 0, 0, 1)]);

    assert!(result.is_err());
}

#[test]
fn rejects_invalid_trade_cardinality() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("trades.parquet");

    write_fixture(&path, &(0..10).collect::<Vec<_>>(), 5);

    let planner = setup(vec![(&path, 10)], 0, 10);

    assert!(planner.plan(&[reference(100, 0, 10, 0, 0, 2)]).is_err());
}

#[test]
fn empty_batch_produces_empty_plan() {
    let planner = CanonicalReadPlanner::new();

    let plan = planner.plan(&[]).unwrap();

    assert_eq!(plan.event_count(), 0);
    assert_eq!(plan.row_group_count(), 0);
    assert_eq!(plan.canonical_rows, 0);
}
