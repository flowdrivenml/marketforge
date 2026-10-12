#![cfg(feature = "process")]

use std::path::{Path, PathBuf};

use marketforge_engine::process::load_processing_config;
use marketforge_engine::process::metrics::ScopedIntegrityMetrics;
use marketforge_engine::{
    canonical::Trade,
    error::Result,
    job::{ProcessingJob, WorkTask},
    process::{
        metrics::{IntegrityCategory, TaskMetrics},
        worker::{TradeSink, process_trade_task_with_metrics},
    },
};
const JOB_FILE: &str = "okx-future-linear-BTC-USD_UM-261225-trade-20260901-20260904-d120.json";

struct CountingSink {
    count: u64,
}

fn load_policy() -> marketforge_engine::job::IntegrityPolicy {
    let root = project_root();

    load_processing_config(root.join("data/.jobs/processing.json"), &root)
        .expect("load processing configuration")
        .integrity_policy
}

impl TradeSink for CountingSink {
    fn write_trade(&mut self, _trade: Trade) -> Result<()> {
        self.count += 1;
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

fn project_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn load_job() -> ProcessingJob {
    let path = project_root().join("data/.jobs/process").join(JOB_FILE);

    marketforge_engine::job::load_processing_job(path).expect("load processing job")
}

fn print_metrics(metrics: &TaskMetrics) {
    let counters = &metrics.counters;
    let integrity = &metrics.integrity.counters;

    println!("\n{}", "=".repeat(75));
    println!("MARKETFORGE — PROCESSING METRICS");
    println!("{}", "=".repeat(75));

    println!("\nPROCESSING");
    println!("  Records read         : {}", counters.records_read);
    println!("  Records matched      : {}", counters.records_matched);
    println!(
        "  Instrument filtered  : {}",
        counters.records_skipped_instrument
    );
    println!("  Records rejected     : {}", counters.records_rejected);
    println!("  Events normalized    : {}", counters.events_normalized);
    println!("  Events written       : {}", counters.events_written);
    println!("  Tasks completed      : {}", counters.tasks_completed);
    println!("  Tasks failed         : {}", counters.tasks_failed);

    println!("\nINTEGRITY");
    println!("  Parse failures       : {}", integrity.parse_failure);
    println!("  Invalid records      : {}", integrity.invalid_record);
    println!("  Sequence gaps        : {}", integrity.sequence_gap);
    println!(
        "  Timestamp regressions: {}",
        integrity.timestamp_regression
    );
    println!("  Missing snapshots    : {}", integrity.missing_snapshot);
    println!("  Invalid books        : {}", integrity.invalid_book);
    println!(
        "  Transformation errors: {}",
        integrity.transformation_failure
    );

    println!("\nTIME WINDOWS");
    println!(
        "  Source files         : {}",
        metrics.scoped_integrity.files.len()
    );
    println!(
        "  Hourly windows       : {}",
        metrics.scoped_integrity.hourly.len()
    );
    println!(
        "  Daily windows        : {}",
        metrics.scoped_integrity.daily.len()
    );

    println!("\nTIMESTAMPS");
    println!("  Minimum              : {:?}", metrics.start_timestamp_ns);
    println!("  Maximum              : {:?}", metrics.end_timestamp_ns);

    println!("\nDIAGNOSTICS");

    if metrics.integrity.diagnostics.is_empty() {
        println!("  No integrity violations detected.");
    } else {
        for diagnostic in &metrics.integrity.diagnostics {
            println!(
                "  Task {} | Record {:?} | {:?} | {}",
                diagnostic.task_id,
                diagnostic.source_record,
                diagnostic.category,
                diagnostic.message,
            );
        }
    }

    println!("{}", "=".repeat(75));
}

fn print_hourly_integrity(metrics: &TaskMetrics) {
    use marketforge_engine::process::metrics::IntegrityCategory;

    println!("\nHOURLY INTEGRITY");

    println!(
        "{:<22} {:>12} {:>12} {:>12}",
        "UTC Hour (ns)", "Eligible", "Invalid", "Rate"
    );

    for ((_, start_ns), statistics) in &metrics.scoped_integrity.hourly {
        let observation = statistics.get(IntegrityCategory::InvalidRecord);

        let rate = observation.rate().unwrap_or(0.0) * 100.0;

        println!(
            "{:<22} {:>12} {:>12} {:>11.6}%",
            start_ns, observation.eligible, observation.violations, rate,
        );
    }
}

#[test]
fn records_trade_processing_metrics() {
    let job = load_job();

    let task: &WorkTask = &job.tasks[0];

    let mut sink = CountingSink { count: 0 };
    let mut metrics = TaskMetrics::default();

    process_trade_task_with_metrics(
        task,
        &mut sink,
        &mut metrics,
        &load_policy(),
        &ScopedIntegrityMetrics::default(),
    )
    .expect("process trade task");

    print_metrics(&metrics);
    print_hourly_integrity(&metrics);

    assert!(metrics.counters.records_read > 0);

    assert_eq!(
        metrics.counters.records_read,
        metrics.counters.records_skipped_instrument + metrics.counters.events_normalized
    );

    assert_eq!(
        metrics.counters.events_normalized,
        metrics.counters.events_written
    );

    assert_eq!(metrics.counters.events_written, sink.count);

    assert_eq!(metrics.counters.tasks_completed, 1);
    assert_eq!(metrics.counters.tasks_failed, 0);

    assert_eq!(
        metrics
            .scoped_integrity
            .global
            .get(IntegrityCategory::InvalidRecord)
            .eligible,
        sink.count
    );

    assert!(!metrics.scoped_integrity.hourly.is_empty());

    assert!(!metrics.scoped_integrity.daily.is_empty());

    assert!(metrics.start_timestamp_ns.is_some());
    assert!(metrics.end_timestamp_ns.is_some());

    println!("\nMARKETFORGE — TRADE WORKER METRICS");

    println!("Records read       : {}", metrics.counters.records_read);
    println!("Records matched    : {}", metrics.counters.records_matched);
    println!(
        "Instrument skipped : {}",
        metrics.counters.records_skipped_instrument
    );
    println!("Trades written     : {}", metrics.counters.events_written);
    println!(
        "Hourly windows     : {}",
        metrics.scoped_integrity.hourly.len()
    );
    println!(
        "Daily windows      : {}",
        metrics.scoped_integrity.daily.len()
    );
    println!("RESULT             : PASS");
}

#[test]
fn retains_metrics_after_sink_failure() {
    struct FailingSink {
        writes: u64,
    }

    impl TradeSink for FailingSink {
        fn write_trade(&mut self, _trade: Trade) -> Result<()> {
            self.writes += 1;

            if self.writes == 10 {
                return Err(
                    marketforge_engine::error::MarketForgeError::InvalidConfiguration(
                        "intentional sink failure".to_owned(),
                    ),
                );
            }

            Ok(())
        }

        fn finish(&mut self) -> Result<()> {
            Ok(())
        }
    }

    let job = load_job();
    let task = &job.tasks[0];

    let mut sink = FailingSink { writes: 0 };
    let mut metrics = TaskMetrics::default();

    let result = process_trade_task_with_metrics(
        task,
        &mut sink,
        &mut metrics,
        &load_policy(),
        &ScopedIntegrityMetrics::default(),
    );

    assert!(result.is_err());

    assert_eq!(metrics.counters.tasks_failed, 1);
    assert_eq!(metrics.counters.tasks_completed, 0);

    assert!(metrics.counters.records_read >= 10);
    assert!(metrics.counters.events_written > 0);
    assert_eq!(metrics.counters.events_written, 9);
}

#[test]
fn successful_trade_processing_has_no_integrity_violations() {
    let job = load_job();
    let task = &job.tasks[0];

    let mut sink = CountingSink { count: 0 };
    let mut metrics = TaskMetrics::default();

    process_trade_task_with_metrics(
        task,
        &mut sink,
        &mut metrics,
        &load_policy(),
        &ScopedIntegrityMetrics::default(),
    )
    .expect("process trade task");

    assert_eq!(metrics.counters.records_rejected, 0);

    assert!(!metrics.integrity.has_violations());

    assert_eq!(
        metrics
            .scoped_integrity
            .global
            .get(IntegrityCategory::ParseFailure)
            .eligible,
        metrics.counters.events_written,
    );

    assert_eq!(
        metrics
            .scoped_integrity
            .global
            .get(IntegrityCategory::InvalidRecord)
            .eligible,
        metrics.counters.events_written,
    );

    assert_eq!(
        metrics
            .scoped_integrity
            .global
            .get(IntegrityCategory::TransformationFailure)
            .eligible,
        metrics.counters.events_written,
    );
}
