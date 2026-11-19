use crate::{
    merge_test_job::bounded_runtime,
    publication::{make_job, rotating_trade_publication},
};
use arrow_array::{UInt8Array, UInt64Array};
use marketforge_engine::historical::merge::{
    PhysicalMergeManifest, UnifiedMergeMode, execute_unified_merge_job, physical_combined_schema,
    validate_committed_merge,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;

#[test]
fn rotates_committed_combined_output_in_one_partition() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path();
    // One full 16,384-event materialization window plus six events, using the
    // minimum production file target of 1 MiB.
    let mut document = make_job(root, &[], &[12]);
    document
        .inputs
        .splice(..0, rotating_trade_publication(root, 11));
    let mut job = bounded_runtime(document);
    job.parquet.file_target_mb = 1;
    assert_eq!(job.primary_files.values().map(Vec::len).sum::<usize>(), 4);
    let start = std::time::Instant::now();
    let result = execute_unified_merge_job(&job, root).unwrap();
    validate_committed_merge(&result.dataset_path).unwrap();
    assert_eq!(result.mode, UnifiedMergeMode::Combined);
    assert_eq!(result.partitions_completed, 1);
    assert_eq!((result.trade_events, result.depth_events), (16_386, 4));
    let manifest: PhysicalMergeManifest =
        serde_json::from_reader(File::open(&result.manifest_path).unwrap()).unwrap();
    assert_eq!(manifest.files.len(), 2);
    let mut next = [[0, 0], [0, 0]];
    for (index, file) in manifest.files.iter().enumerate() {
        assert_eq!(file.partition_id, Some(0));
        assert!(file.path.ends_with(&format!("part-{index:06}.parquet")));
        let reader = ParquetRecordBatchReaderBuilder::try_new(
            File::open(result.dataset_path.join(&file.path)).unwrap(),
        )
        .unwrap();
        assert_eq!(reader.schema(), &physical_combined_schema());
        for batch in reader.build().unwrap() {
            let batch = batch.unwrap();
            let kinds = batch
                .column_by_name("event_kind")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap();
            let column = |name| {
                batch
                    .column_by_name(name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
            };
            for row in 0..batch.num_rows() {
                let kind = kinds.value(row) as usize;
                let task = column("task_id").value(row) as usize;
                assert_eq!(column("dataset_id").value(row), 11 + kind as u64);
                assert_eq!(column("canonical_row_offset").value(row), next[kind][task]);
                next[kind][task] += 1;
            }
        }
    }
    assert_eq!(next, [[8_193, 8_193], [2, 2]]);
    println!(
        "workers=1, primary_files=4, rows=16390, output_files=2, elapsed={:?}",
        start.elapsed()
    );
}
