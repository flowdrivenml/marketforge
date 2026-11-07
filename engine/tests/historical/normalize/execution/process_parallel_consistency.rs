#![cfg(feature = "historical")]

use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use {
    crate::support::migration::archive_case as load_processing_job,
    crate::support::migration::configuration as load_processing_config,
    marketforge_engine::historical::normalize::ProcessingStatus,
    marketforge_engine::historical::normalize::execute_processing_job,
    marketforge_engine::historical::normalize::manifest::DatasetManifest,
    marketforge_engine::historical::normalize::manifest::inspect_parquet_files,
};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

const JOB_FILE: &str = "okx-future-linear-BTC-USD_UM-261225-trade-20260901-20260904-d120.json";
const BITGET_JOB_FILE: &str = "bitget-perpetual-linear-BTCUSDT-trade-20260901-20260904-d93.json";
// -----------------------------------------------------------------------------
// Test environment
// -----------------------------------------------------------------------------

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let id = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);

        let path = crate::support::migration::persistent_path(format!(
            "marketforge-parallel-consistency-{}-{id}",
            std::process::id(),
        ));

        fs::create_dir_all(&path).expect("create test directory");

        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {}
}

fn project_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("project root")
        .to_path_buf()
}

// -----------------------------------------------------------------------------
// Execute job with specified worker count
// -----------------------------------------------------------------------------

fn execute_with_workers(job_file: &str, workers: usize, directory: &Path) -> DatasetManifest {
    let root = project_root();

    let mut job = load_processing_job(root.join("retained-cases").join(job_file))
        .expect("load processing job");

    let mut config =
        load_processing_config(root.join("data/.config/historical/general.toml"), &root)
            .expect("load processing configuration");

    config.resources.workers = workers as _;

    config.resources.parquet.row_group_target_bytes = 128 * 1024 * 1024;
    config.resources.parquet.file_target_bytes = 512 * 1024 * 1024;

    job.output.staging_path = directory.join("staging");
    job.output.dataset_path = directory.join("dataset");

    let result = execute_processing_job(&job, &config).expect("execute processing job");

    assert_eq!(
        result.status,
        ProcessingStatus::Complete,
        "processing failed: {:?}",
        result.failure,
    );

    let manifest_path = job.output.dataset_path.join("manifest.json");

    serde_json::from_slice(&fs::read(&manifest_path).expect("read manifest"))
        .expect("deserialize manifest")
}

// -----------------------------------------------------------------------------
// Read canonical Parquet records
// -----------------------------------------------------------------------------

use sha2::{Digest, Sha256};

fn canonical_fingerprint(dataset: &Path) -> (String, u64) {
    let mut files = inspect_parquet_files(dataset).expect("discover Parquet files");

    // Deterministic ordering by task output path.
    files.sort_by(|a, b| a.path.cmp(&b.path));

    let mut hasher = Sha256::new();
    let mut total_rows = 0u64;

    for file in files {
        let path = dataset.join(&file.path);

        let reader =
            ParquetRecordBatchReaderBuilder::try_new(File::open(path).expect("open Parquet file"))
                .expect("read Parquet metadata")
                .build()
                .expect("build Parquet reader");

        for batch in reader {
            let batch = batch.expect("read RecordBatch");

            // Hash the canonical Arrow data in deterministic order.
            //
            // This initial implementation hashes complete batches.
            // It is suitable only when batch boundaries remain identical.
            let representation = format!("{batch:?}");

            hasher.update(representation.as_bytes());

            total_rows += batch.num_rows() as u64;
        }
    }

    let digest = hasher.finalize();

    (
        digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
        total_rows,
    )
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[test]
fn single_and_parallel_workers_produce_identical_results() {
    let directory = TestDirectory::new();

    let sequential_dir = directory.path().join("sequential");
    let parallel_dir = directory.path().join("parallel");

    fs::create_dir_all(&sequential_dir).unwrap();
    fs::create_dir_all(&parallel_dir).unwrap();

    println!("\n{}", "=".repeat(90));
    println!("MARKETFORGE — PARALLEL CONSISTENCY TEST");
    println!("{}", "=".repeat(90));

    println!("Run A: 1 worker");
    let sequential = execute_with_workers(JOB_FILE, 1, &sequential_dir);

    println!("Run B: 16 workers");
    let parallel = execute_with_workers(JOB_FILE, 16, &parallel_dir);

    // -------------------------------------------------------------------------
    // Compare processing counters
    // -------------------------------------------------------------------------

    assert_eq!(
        sequential.processing_metrics.counters, parallel.processing_metrics.counters,
        "processing counters differ",
    );

    // -------------------------------------------------------------------------
    // Compare integrity metrics
    // -------------------------------------------------------------------------

    assert_eq!(
        sequential.processing_metrics.integrity, parallel.processing_metrics.integrity,
        "integrity counters or diagnostics differ",
    );

    assert_eq!(
        sequential.processing_metrics.scoped_integrity,
        parallel.processing_metrics.scoped_integrity,
        "scoped integrity statistics differ",
    );

    assert_eq!(sequential.integrity_status, parallel.integrity_status,);

    assert_eq!(
        sequential.integrity_evaluation,
        parallel.integrity_evaluation,
    );

    // -------------------------------------------------------------------------
    // Compare dataset statistics
    // -------------------------------------------------------------------------

    assert_eq!(sequential.events_written, parallel.events_written);
    assert_eq!(sequential.files_written, parallel.files_written);

    assert_eq!(sequential.start_timestamp_ns, parallel.start_timestamp_ns,);

    assert_eq!(sequential.end_timestamp_ns, parallel.end_timestamp_ns,);

    // -------------------------------------------------------------------------
    // Compare canonical records
    // -------------------------------------------------------------------------

    let (sequential_hash, sequential_rows) = canonical_fingerprint(&sequential_dir.join("dataset"));

    let (parallel_hash, parallel_rows) = canonical_fingerprint(&parallel_dir.join("dataset"));

    assert_eq!(
        sequential_rows, parallel_rows,
        "canonical row counts differ",
    );

    assert_eq!(
        sequential_hash, parallel_hash,
        "canonical Parquet fingerprints differ",
    );

    println!("\nRESULTS");
    println!("  Sequential records : {}", sequential.events_written);
    println!("  Parallel records   : {}", parallel.events_written);
    println!("  Parquet files      : {}", parallel.files_written);
    println!("  Integrity          : {:?}", parallel.integrity_status);
    println!("  Canonical equality : PASS");
    println!("  Metrics equality   : PASS");
    println!("  RESULT             : PASS");
}

#[test]
#[cfg(feature = "historical-full-tests")]
fn large_parallel_consistency() {
    let directory = TestDirectory::new();

    let sequential_dir = directory.path().join("sequential");
    let parallel_dir = directory.path().join("parallel");

    fs::create_dir_all(&sequential_dir).unwrap();
    fs::create_dir_all(&parallel_dir).unwrap();

    let sequential = execute_with_workers(BITGET_JOB_FILE, 1, &sequential_dir);
    let parallel = execute_with_workers(BITGET_JOB_FILE, 2, &parallel_dir);
    assert_eq!(
        sequential.processing_metrics.counters,
        parallel.processing_metrics.counters,
    );

    assert_eq!(
        sequential.processing_metrics.integrity,
        parallel.processing_metrics.integrity,
    );

    assert_eq!(
        sequential.processing_metrics.scoped_integrity,
        parallel.processing_metrics.scoped_integrity,
    );

    assert_eq!(
        sequential.integrity_evaluation,
        parallel.integrity_evaluation,
    );

    assert_eq!(sequential.events_written, parallel.events_written,);

    assert_eq!(sequential.start_timestamp_ns, parallel.start_timestamp_ns,);

    assert_eq!(sequential.end_timestamp_ns, parallel.end_timestamp_ns,);

    let (sequential_hash, sequential_rows) = canonical_fingerprint(&sequential_dir.join("dataset"));

    let (parallel_hash, parallel_rows) = canonical_fingerprint(&parallel_dir.join("dataset"));

    assert_eq!(sequential_rows, parallel_rows);
    assert_eq!(sequential_hash, parallel_hash);
}
