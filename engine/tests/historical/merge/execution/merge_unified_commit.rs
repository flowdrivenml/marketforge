#![cfg(feature = "historical")]
use crate::publication::depth_publication;
use crate::publication::trade_publication;

use std::collections::HashSet;
use std::{
    collections::HashMap,
    fs::{self, File},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use marketforge_engine::historical::merge::{physical_combined_schema, physical_trade_schema};
use {
    marketforge_engine::historical::job::MergeJob,
    marketforge_engine::historical::job::MergeOperation,
    marketforge_engine::historical::job::MergeOutput,
    marketforge_engine::historical::job::MergeParquetConfig,
    marketforge_engine::historical::job::MergePartitioning,
    marketforge_engine::historical::job::MergeResources,
    marketforge_engine::historical::job::MergeTimeRange,
    marketforge_engine::historical::merge::PhysicalMergeManifest,
    marketforge_engine::historical::merge::UnifiedMergeMode,
    marketforge_engine::historical::merge::execute_unified_merge_job,
};

// -----------------------------------------------------------------------------
// Temporary output directory
// -----------------------------------------------------------------------------

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let path = std::env::temp_dir().join(format!(
            "marketforge-unified-commit-{}-{timestamp}",
            std::process::id()
        ));

        fs::create_dir_all(&path).unwrap();

        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

// -----------------------------------------------------------------------------
// Job configuration
// -----------------------------------------------------------------------------

fn test_job(dataset_ids: &[u64], output: &Path, input_paths: HashMap<u64, PathBuf>) -> MergeJob {
    let start_ns = crate::publication::START;

    MergeJob {
        job_id: 900_500,
        operation: MergeOperation::Merge,

        inputs: dataset_ids.to_vec(),
        primary_files: Default::default(),
        submission: None,
        input_paths,

        time_range: MergeTimeRange {
            start_ns,
            end_ns: start_ns + 400,
        },

        resources: MergeResources {
            workers: 4,
            memory_limit_mb: 4096,
            max_open_files: 128,
        },

        partitioning: MergePartitioning {
            partitions_per_worker: 1,
        },

        parquet: MergeParquetConfig {
            row_group_target_mb: 1,
            file_target_mb: 4,
            compression_level: 3,
        },

        output: MergeOutput {
            dataset_path: output.to_path_buf(),
        },
    }
}

// -----------------------------------------------------------------------------
// Resolve custom dataset paths
// -----------------------------------------------------------------------------

fn input_paths(root: &Path, trade_ids: &[u64], depth_ids: &[u64]) -> HashMap<u64, PathBuf> {
    let mut paths = HashMap::new();

    for &dataset_id in trade_ids {
        let input = trade_publication(root, dataset_id);
        paths.insert(dataset_id, input.dataset_path);
    }

    for &dataset_id in depth_ids {
        let input = depth_publication(root, dataset_id);
        paths.insert(dataset_id, input.dataset_path);
    }

    paths
}

// -----------------------------------------------------------------------------
// Execute and validate committed dataset
// -----------------------------------------------------------------------------

fn assert_transactional_commit(
    trade_ids: &[u64],
    depth_ids: &[u64],
    expected_mode: UnifiedMergeMode,
    expected_trades: u64,
    expected_depth: u64,
) {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path().to_path_buf();
    let temporary = TestDirectory::new();

    let dataset_path = temporary.path().join("merged-900500");

    let dataset_ids = trade_ids
        .iter()
        .chain(depth_ids.iter())
        .copied()
        .collect::<Vec<_>>();

    let paths = input_paths(&root, trade_ids, depth_ids);

    let job = test_job(&dataset_ids, &dataset_path, paths);

    // -------------------------------------------------------------------------
    // Execute complete production pipeline
    // -------------------------------------------------------------------------

    let result = execute_unified_merge_job(&job, &root).expect("execute unified merge job");

    // -------------------------------------------------------------------------
    // Transactional publication
    // -------------------------------------------------------------------------

    assert_eq!(result.dataset_path, dataset_path);
    assert!(dataset_path.is_dir());

    let staging = temporary
        .path()
        .join(format!(".merged-900500.staging-{}", job.job_id));

    assert!(!staging.exists(), "staging directory should be committed");

    assert!(!dataset_path.join("logical").exists());
    assert!(!dataset_path.join("physical").exists());

    assert!(dataset_path.join("timeline").is_dir());

    let entries = fs::read_dir(&dataset_path)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<HashSet<_>>();

    assert_eq!(
        entries,
        HashSet::from(
            [
                "manifest.json",
                "timeline",
                "publication-checksums.json",
                "merge-job.json",
                "merge-input-checksums.json",
                "merge-partitions.json",
                "merge-runtime.json"
            ]
            .map(str::to_owned)
        ),
    );

    // -------------------------------------------------------------------------
    // Manifest persistence
    // -------------------------------------------------------------------------

    let manifest_path = dataset_path.join("manifest.json");

    assert!(manifest_path.is_file());

    let manifest: PhysicalMergeManifest =
        serde_json::from_reader(File::open(&manifest_path).unwrap())
            .expect("deserialize physical manifest");

    manifest.validate().expect("validate physical manifest");

    assert_eq!(manifest.mode, expected_mode);

    assert_eq!(manifest.trade_events, expected_trades);
    assert_eq!(manifest.depth_events, expected_depth);

    assert_eq!(manifest.events_written, expected_trades + expected_depth);

    assert_eq!(manifest.files_written as usize, manifest.files.len());

    // -------------------------------------------------------------------------
    // Validate committed files
    // -------------------------------------------------------------------------

    for file in &manifest.files {
        let path = dataset_path.join(&file.path);

        assert!(
            path.is_file(),
            "missing committed Parquet file: {}",
            path.display()
        );

        assert_eq!(fs::metadata(&path).unwrap().len(), file.size_bytes);
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
            .expect("open committed physical Parquet");

        let expected_schema = match expected_mode {
            UnifiedMergeMode::Trades => physical_trade_schema(),
            UnifiedMergeMode::Combined => physical_combined_schema(),
            UnifiedMergeMode::Depth => unreachable!(),
        };

        assert_eq!(
            reader.schema().as_ref(),
            expected_schema.as_ref(),
            "committed file has incorrect physical schema"
        );
        assert!(
            file.rows.unwrap_or(0) > 0,
            "committed file has no validated rows"
        );
    }

    // -------------------------------------------------------------------------
    // Duplicate publication protection
    // -------------------------------------------------------------------------

    let duplicate = execute_unified_merge_job(&job, &root);

    let error = duplicate.unwrap_err();
    assert!(error.to_string().contains("already exists"), "{error}");

    // Original committed dataset must remain intact.
    assert!(manifest_path.is_file());

    // -------------------------------------------------------------------------
    // Summary
    // -------------------------------------------------------------------------

    println!("\nMARKETFORGE — UNIFIED TRANSACTIONAL COMMIT");
    println!("{}", "=".repeat(80));

    println!("Mode           : {:?}", result.mode);
    println!("Streams        : {}", dataset_ids.len());
    println!("Partitions     : {}", result.partitions_completed);
    println!("Trade events   : {}", result.trade_events);
    println!("Depth events   : {}", result.depth_events);
    println!("Total events   : {}", result.events_written);
    println!("Parquet files  : {}", result.files_written);
    println!(
        "Skipped initializations: {}",
        result.skipped_initializations
    );

    println!("Manifest       : VERIFIED");
    println!("Transactional commit : VERIFIED");
    println!("Duplicate protection : VERIFIED");
    println!("Result         : PASS");
}

// -----------------------------------------------------------------------------
// Trade-only transaction
// -----------------------------------------------------------------------------

#[test]
fn commits_generated_trade_merge() {
    assert_transactional_commit(&[111, 109, 115], &[], UnifiedMergeMode::Trades, 12, 0);
}

// -----------------------------------------------------------------------------
// Combined transaction
// -----------------------------------------------------------------------------

#[test]
fn commits_generated_combined_merge() {
    assert_transactional_commit(&[111, 109], &[105, 112], UnifiedMergeMode::Combined, 8, 8);
}

#[test]
fn commits_depth_only_with_its_sparse_explicit_rank() {
    let root = tempfile::tempdir().unwrap();
    let mut job = crate::publication::make_job(root.path(), &[], &[105]);
    for input in &mut job.inputs {
        input.stream_rank = 7;
    }
    let resolved = crate::merge_test_job::bounded_runtime(job);
    let result = execute_unified_merge_job(&resolved, root.path()).unwrap();
    assert_eq!(result.mode, UnifiedMergeMode::Depth);
    assert!(result.depth_events > 0);
    assert_eq!(result.trade_events, 0);
    let manifest: PhysicalMergeManifest =
        serde_json::from_reader(File::open(result.manifest_path).unwrap()).unwrap();
    assert_eq!(manifest.streams[0].stream_rank, 7);
    marketforge_engine::historical::merge::validate_committed_merge(&result.dataset_path).unwrap();
}

#[test]
fn causal_v5_streams_preserve_source_order_dependencies_and_rank() {
    use arrow_array::{Array, BooleanArray, Int64Array, UInt32Array, UInt64Array};
    use marketforge_engine::historical::{
        job::MergeTimelineOrdering, merge::validate_committed_merge,
    };
    let root = tempfile::tempdir().unwrap();
    let mut job = crate::publication::native_depth_job(root.path());
    job.time_range.start_ns = 140;
    for workers in [1, 2] {
        let mut run = job.clone();
        if workers == 2 {
            run.inputs.reverse();
        }
        run.output.dataset_path = root.path().join(format!("causal-{workers}"));
        let mut config = crate::merge_test_job::small_runtime();
        config.resources.workers = workers;
        config.resources.memory_limit_mb = 2048;
        let result =
            execute_unified_merge_job(&run.clone().with_runtime(config).unwrap(), root.path())
                .unwrap();
        let manifest: PhysicalMergeManifest =
            serde_json::from_reader(File::open(result.manifest_path).unwrap()).unwrap();
        assert_eq!(manifest.timeline_ordering, MergeTimelineOrdering::Causal);
        assert_eq!(manifest.reconstruction_events, 2);
        assert_eq!(manifest.start_timestamp_ns, Some(100));
        assert_eq!(manifest.end_timestamp_ns, Some(220));
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
                let column = |name| batch.column_by_name(name).unwrap().as_any();
                let ts = column("event_timestamp_ns")
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let ranks = column("stream_rank").downcast_ref::<UInt32Array>().unwrap();
                let seq = column("timeline_sequence")
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                let context = column("reconstruction_only")
                    .downcast_ref::<BooleanArray>()
                    .unwrap();
                let tasks = column("task_id").downcast_ref::<UInt64Array>().unwrap();
                let ordinals = column("event_ordinal")
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                let offsets = column("canonical_row_offset")
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                rows.extend((0..batch.num_rows()).map(|i| {
                    (
                        seq.value(i),
                        ts.value(i),
                        ranks.value(i),
                        tasks.value(i),
                        ordinals.value(i),
                        offsets.value(i),
                        context.value(i),
                    )
                }));
            }
        }
        assert_eq!(
            rows,
            vec![
                (0, 100, 3, 19, 5, 0, true),
                (1, 100, 14, 8, 5, 0, true),
                (2, 160, 14, 8, 7, 1, false),
                (3, 180, 3, 19, 7, 1, false),
                (4, 150, 3, 19, 9, 2, false),
                (5, 190, 3, 19, 11, 2, false),
                (6, 195, 3, 19, 13, 3, false),
                (7, 205, 14, 8, 9, 2, false),
                (8, 220, 3, 4, 5, 0, false),
                (9, 210, 3, 4, 7, 1, false),
            ]
        );
        validate_committed_merge(&result.dataset_path).unwrap();
    }
    job.inputs.remove(0);
    let error = execute_unified_merge_job(
        &job.clone()
            .with_runtime(crate::merge_test_job::small_runtime())
            .unwrap(),
        root.path(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("select all primary parts"));
    assert!(!job.output.dataset_path.exists());
}

#[test]
fn causal_v5_rejects_unpinned_event_index_before_publication() {
    let root = tempfile::tempdir().unwrap();
    let job = crate::publication::native_depth_job(root.path());
    let manifest_path = &job.inputs[0].publication.manifest_path;
    let manifest: serde_json::Value =
        serde_json::from_reader(File::open(manifest_path).unwrap()).unwrap();
    let path = manifest["auxiliary_files"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|file| file["path"].as_str())
        .find(|path| path.ends_with("events.parquet"))
        .unwrap();
    let path = manifest_path.parent().unwrap().join(path);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[20] ^= 1;
    std::fs::write(path, bytes).unwrap();
    let output = job.output.dataset_path.clone();
    let error = execute_unified_merge_job(
        &job.with_runtime(crate::merge_test_job::small_runtime())
            .unwrap(),
        root.path(),
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("auxiliary SHA-256 mismatch"),
        "{error}"
    );
    assert!(!output.exists());
}
