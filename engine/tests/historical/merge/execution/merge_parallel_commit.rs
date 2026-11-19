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

#[test]
fn consecutive_publications_share_a_rank_and_keep_their_source_identity() {
    use crate::{merge_test_job::bounded_runtime, publication::shared_part_trade_publication};
    use arrow_array::{Int64Array, UInt32Array, UInt64Array};
    use marketforge_engine::historical::merge::PhysicalMergeManifest;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;

    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path();
    let mut job = shared_part_trade_publication(root, 11, 0, Some("test"));
    let next = shared_part_trade_publication(root, 12, 400, Some("test"));
    job.time_range.end_ns = 900;
    job.inputs.splice(0..0, next.inputs);
    for input in &mut job.inputs {
        input.stream_rank = 7;
    }

    let result = execute_unified_merge_job(&bounded_runtime(job.clone()), root).unwrap();
    validate_committed_merge(&result.dataset_path).unwrap();
    let manifest: PhysicalMergeManifest =
        serde_json::from_reader(File::open(&result.manifest_path).unwrap()).unwrap();
    assert_eq!(manifest.streams.len(), 2);
    assert!(
        manifest
            .streams
            .iter()
            .all(|stream| stream.stream_rank == 7)
    );
    let mut source = Vec::new();
    for file in manifest.files {
        let reader = ParquetRecordBatchReaderBuilder::try_new(
            File::open(result.dataset_path.join(file.path)).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            let column = |name| batch.column_by_name(name).unwrap().as_any();
            let ranks = column("stream_rank").downcast_ref::<UInt32Array>().unwrap();
            let publications = column("dataset_id").downcast_ref::<UInt64Array>().unwrap();
            let tasks = column("task_id").downcast_ref::<UInt64Array>().unwrap();
            let offsets = column("canonical_row_offset")
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let times = column("event_timestamp_ns")
                .downcast_ref::<Int64Array>()
                .unwrap();
            source.extend((0..batch.num_rows()).map(|i| {
                (
                    ranks.value(i),
                    publications.value(i),
                    tasks.value(i),
                    offsets.value(i),
                    times.value(i),
                )
            }));
        }
    }
    assert_eq!(
        source,
        [
            (7, 11, 0, 0, 100),
            (7, 11, 0, 1, 200),
            (7, 11, 1, 0, 300),
            (7, 11, 1, 1, 400),
            (7, 12, 0, 0, 500),
            (7, 12, 0, 1, 600),
            (7, 12, 1, 0, 700),
            (7, 12, 1, 1, 800),
        ]
    );

    let overlap = shared_part_trade_publication(root, 13, 300, Some("test"));
    job.inputs[0] = overlap.inputs.into_iter().next().unwrap();
    job.inputs[0].stream_rank = 7;
    job.output.dataset_path = root.join("overlap");
    let error = execute_unified_merge_job(&bounded_runtime(job.clone()), root).unwrap_err();
    assert!(error.to_string().contains("ambiguous overlap"), "{error}");
    assert!(!job.output.dataset_path.exists());

    let other = shared_part_trade_publication(root, 14, 400, Some("other-namespace"));
    job.inputs[0] = other.inputs.into_iter().next().unwrap();
    job.inputs[0].stream_rank = 7;
    let error = execute_unified_merge_job(&bounded_runtime(job), root).unwrap_err();
    assert!(
        error.to_string().contains("assigned to distinct streams"),
        "{error}"
    );
}
