#![cfg(feature = "process")]

use marketforge_engine::process::metrics::ScopedIntegrityMetrics;
use marketforge_engine::process::metrics::{IntegrityCategory, IntegrityScope};
use marketforge_engine::{
    canonical::Trade,
    error::Result,
    job::{IntegrityAction, IntegrityPolicy, ProcessingJob, SourceContainer, WorkTask},
    process::{
        load_processing_config,
        metrics::{IntegrityStatus, TaskMetrics, evaluate_integrity_policy},
        worker::{TradeSink, process_trade_task_with_metrics},
    },
};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

const JOB_FILE: &str = "okx-future-linear-BTC-USD_UM-261225-trade-20260901-20260904-d120.json";

// -----------------------------------------------------------------------------
// Test environment
// -----------------------------------------------------------------------------

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let id = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);

        let path = std::env::temp_dir().join(format!(
            "marketforge-integrity-test-{}-{id}",
            std::process::id()
        ));

        fs::create_dir_all(&path).expect("create test directory");

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

#[derive(Default)]
struct CountingSink {
    trades: Vec<Trade>,
}

impl TradeSink for CountingSink {
    fn write_trade(&mut self, trade: Trade) -> Result<()> {
        self.trades.push(trade);
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

fn project_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("project root")
        .to_path_buf()
}

fn load_job() -> ProcessingJob {
    marketforge_engine::job::load_processing_job(
        project_root().join("data/.jobs/process").join(JOB_FILE),
    )
    .expect("load processing job")
}

fn load_policy() -> IntegrityPolicy {
    let root = project_root();

    load_processing_config(root.join("data/.jobs/processing.json"), &root)
        .expect("load processing configuration")
        .integrity_policy
}

// -----------------------------------------------------------------------------
// Synthetic OKX fixture
// -----------------------------------------------------------------------------

fn fixture_task(directory: &TestDirectory, records: &[&str]) -> WorkTask {
    let job = load_job();

    let mut task = job.tasks[0].clone();

    let path = directory.path().join("synthetic.csv");

    println!("FIXTURE HEADER: {:?}", records.first());

    fs::write(&path, records.join("\n") + "\n").expect("write synthetic CSV");

    task.input_path = path;

    // The source is now a plain CSV rather than a ZIP archive.
    // Preserve all original normalization and instrument metadata.
    task.source_compression = SourceContainer::Plain;
    task.archive_member = None;

    task
}

fn run(task: &WorkTask, policy: &IntegrityPolicy) -> (Result<()>, CountingSink, TaskMetrics) {
    let mut sink = CountingSink::default();
    let mut metrics = TaskMetrics::default();

    let result = process_trade_task_with_metrics(
        task,
        &mut sink,
        &mut metrics,
        policy,
        &ScopedIntegrityMetrics::default(),
    );

    println!("\n{}", "=".repeat(70));
    println!("INTEGRITY ENFORCEMENT TEST");
    println!("{}", "=".repeat(70));

    println!("Result           : {result:?}");
    println!("Source           : {}", task.input_path.display());
    println!("Records read     : {}", metrics.counters.records_read);
    println!("Records matched  : {}", metrics.counters.records_matched);
    println!("Records rejected : {}", metrics.counters.records_rejected);
    println!("Trades written   : {}", metrics.counters.events_written);
    println!("Tasks failed     : {}", metrics.counters.tasks_failed);

    println!("\nDiagnostics:");

    for diagnostic in &metrics.integrity.diagnostics {
        println!(
            "  Record {:?} | {:?} | {}",
            diagnostic.source_record, diagnostic.category, diagnostic.message,
        );
    }

    println!("{}", "=".repeat(70));

    (result, sink, metrics)
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[test]
fn fail_action_stops_on_invalid_record() {
    let directory = TestDirectory::new();

    let task = fixture_task(
        &directory,
        &[
            "instrument_name,trade_id,side,price,size,created_time,source",
            "BTC-USD_UM-261225,1,buy,85000,1,1788200290510,0",
            "BTC-USD_UM-261225,2,buy,invalid,1,1788200290520,0",
            "BTC-USD_UM-261225,3,sell,85020,1,1788200290530,0",
        ],
    );

    let mut policy = load_policy();
    policy.enabled = true;
    policy.invalid_record.action = IntegrityAction::Fail;

    let (result, sink, metrics) = run(&task, &policy);

    assert!(result.is_err());
    assert_eq!(sink.trades.len(), 1);

    assert_eq!(metrics.counters.records_rejected, 1);
    assert_eq!(metrics.integrity.counters.invalid_record, 1);
    assert_eq!(metrics.counters.tasks_failed, 1);
}

#[test]
fn degrade_action_skips_invalid_records() {
    let directory = TestDirectory::new();

    let task = fixture_task(
        &directory,
        &[
            "instrument_name,trade_id,side,price,size,created_time,source",
            "BTC-USD_UM-261225,1,buy,85000,1,1788200290510,0",
            "BTC-USD_UM-261225,2,buy,invalid,1,1788200290520,0",
            "BTC-USD_UM-261225,3,sell,85020,1,1788200290530,0",
        ],
    );

    let mut policy = load_policy();
    policy.enabled = true;
    policy.invalid_record.action = IntegrityAction::Degrade;

    // Relax thresholds for this small fixture.
    policy.invalid_record.max_count = Some(10);
    policy.invalid_record.max_rate = Some(1.0);
    policy.invalid_record.minimum_samples = 0;
    policy.invalid_record.windows.daily = None;
    policy.invalid_record.windows.hourly = None;

    let (result, sink, metrics) = run(&task, &policy);

    assert!(result.is_ok());

    assert_eq!(sink.trades.len(), 2);
    assert_eq!(metrics.counters.records_rejected, 1);
    assert_eq!(metrics.integrity.counters.invalid_record, 1);

    assert_eq!(metrics.counters.events_written, 2);
    assert_eq!(metrics.counters.tasks_completed, 1);

    let evaluation = evaluate_integrity_policy(&policy, &metrics.scoped_integrity).unwrap();

    assert_eq!(evaluation.status, IntegrityStatus::Degraded);
}

#[test]
fn disabled_enforcement_still_records_errors() {
    let directory = TestDirectory::new();

    let task = fixture_task(
        &directory,
        &[
            "instrument_name,trade_id,side,price,size,created_time,source",
            "BTC-USD_UM-261225,1,buy,85000,1,1788200290510,0",
            "BTC-USD_UM-261225,2,buy,invalid,1,1788200290520,0",
            "BTC-USD_UM-261225,3,sell,85020,1,1788200290530,0",
        ],
    );

    let mut policy = load_policy();
    policy.enabled = false;

    let (result, sink, metrics) = run(&task, &policy);

    assert!(result.is_ok());
    assert_eq!(sink.trades.len(), 2);

    assert_eq!(metrics.counters.records_rejected, 1);
    assert_eq!(metrics.integrity.counters.invalid_record, 1);

    let evaluation = evaluate_integrity_policy(&policy, &metrics.scoped_integrity).unwrap();

    assert_eq!(evaluation.status, IntegrityStatus::Degraded);
}

#[test]
fn count_threshold_stops_processing() {
    let directory = TestDirectory::new();

    let task = fixture_task(
        &directory,
        &[
            "instrument_name,trade_id,side,price,size,created_time,source",
            "BTC-USD_UM-261225,1,buy,invalid,1,1788200290510,0",
            "BTC-USD_UM-261225,2,buy,invalid,1,1788200290520,0",
            "BTC-USD_UM-261225,3,sell,85020,1,1788200290530,0",
        ],
    );

    let mut policy = load_policy();
    policy.enabled = true;
    policy.invalid_record.action = IntegrityAction::Degrade;
    policy.invalid_record.max_count = Some(1);
    policy.invalid_record.max_rate = Some(1.0);
    policy.invalid_record.minimum_samples = 0;
    policy.invalid_record.windows.daily = None;
    policy.invalid_record.windows.hourly = None;

    let (result, sink, metrics) = run(&task, &policy);

    assert!(result.is_err());
    assert_eq!(sink.trades.len(), 0);

    assert_eq!(metrics.counters.records_rejected, 2);
    assert_eq!(metrics.integrity.counters.invalid_record, 2);
    assert_eq!(metrics.counters.tasks_failed, 1);
}

#[test]
fn previous_task_violations_contribute_to_absolute_limit() {
    let directory = TestDirectory::new();

    let task = fixture_task(
        &directory,
        &[
            "instrument_name,trade_id,side,price,size,created_time,source",
            "BTC-USD_UM-261225,1,buy,invalid,1,1788200290510,0",
            "BTC-USD_UM-261225,2,buy,85000,1,1788200290520,0",
        ],
    );

    let mut policy = load_policy();

    policy.enabled = true;
    policy.invalid_record.action = IntegrityAction::Degrade;
    policy.invalid_record.max_count = Some(1);
    policy.invalid_record.max_rate = Some(1.0);
    policy.invalid_record.minimum_samples = 0;
    policy.invalid_record.windows.daily = None;
    policy.invalid_record.windows.hourly = None;

    let mut previous = ScopedIntegrityMetrics::default();

    let previous_scope = IntegrityScope {
        task_id: 999,
        stream_id: task.stream_id.0.clone(),
        source_file: "previous-task.csv".to_owned(),
    };

    previous
        .record(
            &previous_scope,
            IntegrityCategory::InvalidRecord,
            true,
            None,
        )
        .unwrap();

    let mut sink = CountingSink::default();
    let mut metrics = TaskMetrics::default();

    let result =
        process_trade_task_with_metrics(&task, &mut sink, &mut metrics, &policy, &previous);

    assert!(result.is_err());

    assert_eq!(metrics.counters.records_rejected, 1);
    assert_eq!(metrics.integrity.counters.invalid_record, 1);
    assert_eq!(sink.trades.len(), 0);
}
