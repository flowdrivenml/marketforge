use crate::{merge_test_job::small_runtime, publication::bounded_trade_job};
use marketforge_engine::historical::{
    merge::{MergeCommitPaths, execute_unified_merge_job, validate_committed_merge},
    normalize::manifest::file_sha256,
};

#[test]
fn executes_and_commits_parallel_merge() {
    let temporary = tempfile::tempdir().unwrap();
    let job = bounded_trade_job(temporary.path().join("merged"));
    let mut runtime = small_runtime();
    runtime.resources.workers = 2;
    runtime.resources.memory_limit_mb = 2048;
    let job = job.with_runtime(runtime).unwrap();
    let commit = MergeCommitPaths::new(&job.output.dataset_path, job.job_id).unwrap();
    let result = execute_unified_merge_job(&job, temporary.path()).unwrap();
    assert_eq!(result.partitions_completed, 2);
    assert!(result.trade_events > 0);
    assert_eq!(result.depth_events, 0);
    assert_eq!(result.dataset_path, commit.dataset_path);
    validate_committed_merge(&result.dataset_path).unwrap();
    let manifest_sha = file_sha256(&result.manifest_path).unwrap();
    assert!(!commit.staging_path.exists());
    let err = execute_unified_merge_job(&job, temporary.path()).unwrap_err();
    assert!(err.to_string().contains("already exists"), "{err}");
    assert_eq!(file_sha256(&result.manifest_path).unwrap(), manifest_sha);
    assert!(!commit.staging_path.exists());
}

#[test]
fn selected_shared_trade_part_retains_task_bounds_across_workers() {
    use arrow_array::{Decimal256Array, Int64Array, UInt64Array};
    use marketforge_engine::historical::merge::PhysicalMergeManifest;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path();
    let job = crate::publication::shared_part_trade_job(root);
    assert_eq!(job.inputs.len(), 1);
    for workers in [1, 2] {
        let start = std::time::Instant::now();
        let mut settings = small_runtime();
        settings.resources.workers = workers;
        settings.resources.memory_limit_mb = 2048;
        let mut job = job.clone();
        job.output.dataset_path = root.join(format!("workers-{workers}"));
        let result = execute_unified_merge_job(&job.with_runtime(settings).unwrap(), root).unwrap();
        assert_eq!(result.partitions_completed, workers);
        assert_eq!(result.trade_events, 4);
        validate_committed_merge(&result.dataset_path).unwrap();
        let manifest: PhysicalMergeManifest =
            serde_json::from_reader(File::open(result.manifest_path).unwrap()).unwrap();
        let mut rows = Vec::new();
        for file in &manifest.files {
            let reader = ParquetRecordBatchReaderBuilder::try_new(
                File::open(result.dataset_path.join(&file.path)).unwrap(),
            )
            .unwrap()
            .build()
            .unwrap();
            for batch in reader {
                let batch = batch.unwrap();
                let times = batch
                    .column_by_name("event_timestamp_ns")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let lineage = |name| {
                    batch
                        .column_by_name(name)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap()
                };
                let prices = batch
                    .column_by_name("price")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Decimal256Array>()
                    .unwrap();
                assert_eq!(prices.scale(), 28);
                for row in 0..batch.num_rows() {
                    assert_eq!(
                        prices.value(row),
                        arrow_buffer::i256::from_i128(78_500 * 10_i128.pow(28))
                    );
                    rows.push((
                        times.value(row),
                        lineage("dataset_id").value(row),
                        lineage("task_id").value(row),
                        lineage("canonical_row_offset").value(row),
                    ));
                }
            }
        }
        assert_eq!(
            rows,
            [
                (100, 11, 0, 0),
                (200, 11, 0, 1),
                (300, 11, 1, 0),
                (400, 11, 1, 1)
            ]
        );
        println!(
            "workers={workers}, primary_files=1, rows=4, elapsed={:?}",
            start.elapsed()
        );
    }
}
