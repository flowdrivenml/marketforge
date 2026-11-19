use crate::{merge_test_job::bounded_runtime, publication::make_job};
use arrow_array::{Decimal256Array, Int64Array, ListArray, StructArray, UInt8Array, UInt64Array};
use arrow_buffer::i256;
use arrow_select::concat::concat_batches;
use marketforge_engine::historical::merge::{
    PhysicalMergeManifest, PhysicalReconstructionAvailability, UnifiedMergeMode,
    execute_unified_merge_job, physical_combined_schema, validate_committed_merge,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;

#[test]
fn commits_combined_replay_with_a_checkpoint_and_equivalent_workers() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path();
    let document = make_job(root, &[11], &[12]);
    assert_eq!(document.inputs.len(), 4);
    let mut first = None;
    for workers in [1, 2] {
        let start = std::time::Instant::now();
        let mut submission = document.clone();
        submission.output.dataset_path = root.join(format!("workers-{workers}"));
        let mut job = bounded_runtime(submission);
        job.resources.workers = workers;
        let result = execute_unified_merge_job(&job, root).unwrap();
        validate_committed_merge(&result.dataset_path).unwrap();
        assert_eq!(result.mode, UnifiedMergeMode::Combined);
        assert_eq!(result.partitions_completed, workers);
        assert_eq!((result.trade_events, result.depth_events), (4, 4));
        let manifest: PhysicalMergeManifest =
            serde_json::from_reader(File::open(&result.manifest_path).unwrap()).unwrap();
        let checkpoints = manifest
            .checkpoints
            .iter()
            .map(|c| (c.dataset_id, c.task_id, c.timestamp_ns))
            .collect::<Vec<_>>();
        assert_eq!(checkpoints, [(12, 0, 100), (12, 1, 300)]);
        let [initial] = manifest.initial_reconstruction.as_slice() else {
            panic!("expected one depth reconstruction source");
        };
        assert_eq!(initial.dataset_id, 12);
        assert_eq!(
            initial.availability,
            PhysicalReconstructionAvailability::Available
        );
        assert_eq!(initial.first_physical_checkpoint_ns, Some(100));

        let batches = manifest
            .files
            .iter()
            .flat_map(|file| {
                ParquetRecordBatchReaderBuilder::try_new(
                    File::open(result.dataset_path.join(&file.path)).unwrap(),
                )
                .unwrap()
                .build()
                .unwrap()
                .map(Result::unwrap)
            })
            .collect::<Vec<_>>();
        let batch = concat_batches(&physical_combined_schema(), &batches).unwrap();
        let times = batch
            .column_by_name("event_timestamp_ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = batch
            .column_by_name("event_kind")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        let lineage = |name| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
        };
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
        for (row, (timestamp, kind, task, offset)) in [
            (100, 0, 0, 0),
            (100, 1, 0, 0),
            (200, 0, 0, 1),
            (200, 1, 0, 1),
            (300, 0, 1, 0),
            (300, 1, 1, 0),
            (400, 0, 1, 1),
            (400, 1, 1, 1),
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!((times.value(row), kinds.value(row)), (timestamp, kind));
            assert_eq!(
                (
                    lineage("dataset_id").value(row),
                    lineage("task_id").value(row),
                    lineage("canonical_row_offset").value(row)
                ),
                (11 + kind as u64, task, offset)
            );
            if kind == 1 {
                let event = changes.value(row);
                let level = event.as_any().downcast_ref::<StructArray>().unwrap();
                let qty = level
                    .column_by_name("quantity_base")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .unwrap();
                assert_eq!(
                    qty.value(0),
                    i256::from_i128((offset as i128 + 1) * 10_i128.pow(28))
                );
            }
        }
        if let Some(expected) = &first {
            assert_eq!(&batch, expected);
        } else {
            first = Some(batch);
        }
        println!(
            "workers={workers}, primary_files=4, trade_events=4, depth_events=4, elapsed={:?}",
            start.elapsed()
        );
    }
}
