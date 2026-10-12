#![cfg(feature = "process")]

use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use marketforge_engine::{
    book::SequencePolicy,
    job::load_processing_job,
    process::{
        boundary::{BoundaryContinuity, DepthBoundaryManifest, fingerprint_book},
        config::load_processing_config,
        metrics::{ScopedIntegrityMetrics, TaskMetrics},
        parquet::ParquetDepthWriter,
        worker::{DepthSink, process_depth_task_with_metrics},
    },
};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

// -----------------------------------------------------------------------------
// Temporary directory
// -----------------------------------------------------------------------------

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new() -> Self {
        let id = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);

        let path = std::env::temp_dir().join(format!(
            "marketforge-depth-continuity-{}-{id}",
            std::process::id()
        ));

        fs::create_dir_all(&path).unwrap();

        Self { path }
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

// -----------------------------------------------------------------------------
// Process one archive independently
// -----------------------------------------------------------------------------

fn process_archive(
    task: &marketforge_engine::job::WorkTask,
    output_path: &Path,
    config: &marketforge_engine::process::config::ProcessingConfig,
) -> DepthBoundaryManifest {
    let mut writer = ParquetDepthWriter::new(output_path, config.resources.parquet.clone())
        .expect("create Parquet writer");

    let mut metrics = TaskMetrics::default();

    process_depth_task_with_metrics(
        task,
        &mut writer,
        &mut metrics,
        &config.integrity_policy,
        &ScopedIntegrityMetrics::default(),
        SequencePolicy::Ranged,
    )
    .expect("process Gate.io archive");

    writer.finish().expect("finalize Parquet writer");

    assert_eq!(
        metrics.counters.records_read,
        metrics.counters.records_processed,
    );

    assert_eq!(metrics.counters.records_rejected, 0);

    let boundary: DepthBoundaryManifest = serde_json::from_reader(
        File::open(output_path.join("boundary.json")).expect("open boundary manifest"),
    )
    .expect("read boundary manifest");

    assert_eq!(boundary.continuity, BoundaryContinuity::Verified,);

    boundary
}

// -----------------------------------------------------------------------------
// Compare two independently reconstructed archives
// -----------------------------------------------------------------------------

fn verify_archive_boundary(previous: &DepthBoundaryManifest, next: &DepthBoundaryManifest) {
    let previous_final = previous
        .final_state
        .as_ref()
        .expect("previous archive missing final book");

    let next_initial = next
        .initial
        .as_ref()
        .expect("next archive missing initial book");

    // -------------------------------------------------------------------------
    // Sequence continuity
    // -------------------------------------------------------------------------

    assert_eq!(
        previous_final.sequence, next_initial.sequence,
        "cross-archive sequence mismatch",
    );

    // -------------------------------------------------------------------------
    // Timestamp continuity
    // -------------------------------------------------------------------------

    assert_eq!(
        previous_final.timestamp_ns, next_initial.timestamp_ns,
        "cross-archive timestamp mismatch",
    );

    // -------------------------------------------------------------------------
    // Exact reconstructed book equivalence
    // -------------------------------------------------------------------------

    assert_eq!(
        previous_final.state, next_initial.state,
        "cross-archive order-book mismatch",
    );

    // -------------------------------------------------------------------------
    // SHA-256 equivalence
    // -------------------------------------------------------------------------

    assert_eq!(
        previous_final.fingerprint, next_initial.fingerprint,
        "cross-archive fingerprint mismatch",
    );

    assert_eq!(
        previous_final.fingerprint,
        fingerprint_book(&previous_final.state),
    );

    assert_eq!(
        next_initial.fingerprint,
        fingerprint_book(&next_initial.state),
    );
}

fn validate_gateio_hourly_continuity(job_filename: &str) {
    let root = project_root();

    let job = load_processing_job(root.join("data/.jobs/process").join(job_filename))
        .expect("load Gate.io processing job");

    assert!(
        job.tasks.len() >= 3,
        "expected at least three hourly archives"
    );

    let config = load_processing_config(root.join("data/.jobs/processing.json"), &root)
        .expect("load processing configuration");

    let directory = TestDirectory::new();

    // Sort tasks chronologically by archive path.
    let mut tasks = job.tasks.iter().collect::<Vec<_>>();

    tasks.sort_by(|a, b| a.input_path.cmp(&b.input_path));

    let mut boundaries = Vec::new();

    for (index, task) in tasks.into_iter().take(3).enumerate() {
        assert!(
            task.input_path.is_file(),
            "missing archive: {}",
            task.input_path.display()
        );

        let output_path = directory.path.join(format!("archive-{index}"));

        let boundary = process_archive(task, &output_path, &config);

        println!("Archive {index}: {}", task.input_path.display());

        boundaries.push(boundary);
    }

    for index in 0..boundaries.len() - 1 {
        verify_archive_boundary(&boundaries[index], &boundaries[index + 1]);

        println!(
            "Archive {} → {}: SEQUENCE / BOOK / SHA-256 MATCH",
            index,
            index + 1,
        );
    }

    println!("\nCROSS-ARCHIVE CONTINUITY: PASS");
}
// -----------------------------------------------------------------------------
// Gate.io spot continuity
// -----------------------------------------------------------------------------

#[test]
fn validates_gateio_spot_hourly_continuity() {
    validate_gateio_hourly_continuity("gateio-spot-spot-BTC_USDT-l2-20260901-20260904-d112.json");
}

#[test]
fn validates_gateio_linear_hourly_continuity() {
    validate_gateio_hourly_continuity(
        "gateio-perpetual-linear-BTC_USDT-l2-20260901-20260904-d96.json",
    );
}

#[test]
fn validates_gateio_inverse_hourly_continuity() {
    validate_gateio_hourly_continuity(
        "gateio-perpetual-inverse-BTC_USD-l2-20260901-20260904-d103.json",
    );
}
