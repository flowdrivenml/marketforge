use std::{
    fs::{self, File},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use marketforge_engine::{
    book::SequencePolicy,
    job::load_processing_job,
    process::{
        boundary::{DepthBoundaryManifest, fingerprint_book},
        load_processing_config,
        metrics::{ScopedIntegrityMetrics, TaskMetrics},
        parquet::ParquetDepthWriter,
        worker::{DepthSink, process_depth_task_with_metrics},
    },
};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

pub struct TestDirectory {
    pub path: PathBuf,
}

impl TestDirectory {
    pub fn new() -> Self {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

        let path = std::env::temp_dir().join(format!(
            "marketforge-depth-format-{}-{id}",
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

pub fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

pub struct DepthFormatResult {
    pub records_read: u64,
    pub records_processed: u64,
    pub records_rejected: u64,
    pub levels_written: u64,
    pub boundary: DepthBoundaryManifest,
}

pub fn run_depth_format_test(
    job_filename: &str,
    sequence_policy: SequencePolicy,
) -> DepthFormatResult {
    let root = project_root();

    let job = load_processing_job(root.join("data/.jobs/process").join(job_filename))
        .expect("load depth processing job");

    let task = &job.tasks[0];

    assert!(
        task.input_path.is_file(),
        "missing source archive: {}",
        task.input_path.display()
    );

    let config = load_processing_config(root.join("data/.jobs/processing.json"), &root)
        .expect("load processing configuration");

    let directory = TestDirectory::new();

    let mut writer = ParquetDepthWriter::new(&directory.path, config.resources.parquet.clone())
        .expect("create Parquet depth writer");

    let mut metrics = TaskMetrics::default();

    process_depth_task_with_metrics(
        task,
        &mut writer,
        &mut metrics,
        &config.integrity_policy,
        &ScopedIntegrityMetrics::default(),
        sequence_policy,
    )
    .expect("process depth archive");

    writer.finish().expect("finalize Parquet depth writer");

    let boundary: DepthBoundaryManifest = serde_json::from_reader(
        File::open(directory.path.join("boundary.json")).expect("open boundary metadata"),
    )
    .expect("deserialize boundary metadata");

    assert_eq!(boundary.version, 1);

    if let Some(initial) = &boundary.initial {
        assert_eq!(initial.fingerprint, fingerprint_book(&initial.state));
    }

    if let Some(final_state) = &boundary.final_state {
        assert_eq!(
            final_state.fingerprint,
            fingerprint_book(&final_state.state)
        );
    }

    println!("\n{}", "=".repeat(90));
    println!("MARKETFORGE — DEPTH FORMAT VALIDATION");
    println!("{}", "=".repeat(90));

    println!("Job              : {job_filename}");
    println!("Source records   : {}", metrics.counters.records_read);
    println!("Processed records: {}", metrics.counters.records_processed);
    println!("Rejected records : {}", metrics.counters.records_rejected);
    println!("Canonical levels : {}", writer.metrics().levels_written);
    println!("Continuity       : {:?}", boundary.continuity);

    if let Some(initial) = &boundary.initial {
        println!(
            "Initial book     : {} bids / {} asks",
            initial.state.bids.len(),
            initial.state.asks.len()
        );
    }

    if let Some(final_state) = &boundary.final_state {
        println!(
            "Final book       : {} bids / {} asks",
            final_state.state.bids.len(),
            final_state.state.asks.len()
        );
    }

    DepthFormatResult {
        records_read: metrics.counters.records_read,
        records_processed: metrics.counters.records_processed,
        records_rejected: metrics.counters.records_rejected,
        levels_written: writer.metrics().levels_written,
        boundary,
    }
}
