#![cfg(feature = "historical")]

use crate::support::{
    fixtures::workload,
    runtime::{Overrides, TestRun},
};
use marketforge_engine::shared::historical::limits::LimitScope;
use marketforge_engine::{
    historical::{
        job::{ContentType, InstrumentKind, ProcessingJob},
        normalize::task::execute_trade_task,
    },
    shared::{domain::canonical::Exchange, historical::ProcessingConfig},
};

fn selected_trade_job(run: &mut TestRun, config: &ProcessingConfig) -> ProcessingJob {
    let mut batch = workload().expect("load authoritative normalize.json");

    // Select one concrete trade stream from the current fixture.
    // No archive dates, paths, or expected row counts are hardcoded.
    batch = crate::support::fixtures::concrete_stream(
        &batch,
        Exchange::Bybit,
        InstrumentKind::Spot,
        ContentType::Trades,
        1,
    )
    .expect("prepared concrete trade stream");
    batch.jobs[0].tasks.truncate(1);

    let resolved = run
        .prepare(&batch, config)
        .expect("resolve selected fixture through production profiles");

    assert_eq!(resolved.jobs.len(), 1, "expected one resolved job");

    let job = resolved.jobs.into_iter().next().unwrap();

    assert_eq!(job.tasks.len(), 1);

    job
}

#[cfg(feature = "historical-full-tests")]
#[test]
fn executes_trade_task_into_isolated_output() {
    use marketforge_engine::historical::normalize::manifest::inspect_parquet_files;

    let mut run = TestRun::new("execution/real_trade_task").expect("create isolated test run");

    let config = run
        .runtime(Overrides::default())
        .expect("load processing configuration");

    let job = selected_trade_job(&mut run, &config);
    let _limits = LimitScope::enter(
        config.resources.normalization_limits,
        "fixture-driven real trade test".to_owned(),
    );
    let task = &job.tasks[0];

    assert!(
        task.input_path.is_file(),
        "missing archive: {}",
        task.input_path.display()
    );

    let output = run.path.join("task-output");

    run.owned(&output).expect("output must belong to test run");

    let execution = execute_trade_task(
        task,
        &config.integrity_policy,
        &config.resources.parquet,
        &output,
    );

    assert!(
        execution.is_success(),
        "task execution failed: {:?}",
        execution.error
    );

    assert_eq!(execution.metrics.counters.tasks_completed, 1);
    assert_eq!(execution.metrics.counters.tasks_failed, 0);

    assert!(
        execution.metrics.counters.records_read > 0,
        "source archive contains no records"
    );

    assert!(
        execution.output_path.is_dir(),
        "missing task output directory"
    );

    // Independently inspect the actual Parquet output.
    let parquet_files = inspect_parquet_files(&output).expect("inspect generated Parquet");

    let parquet_rows: u64 = parquet_files.iter().map(|file| file.rows).sum();

    assert_eq!(
        parquet_rows, execution.metrics.counters.events_written,
        "Parquet row count differs from reported events_written"
    );

    assert_eq!(
        execution.metrics.counters.records_processed, execution.metrics.counters.events_normalized,
        "processed-record accounting differs from normalized events"
    );

    println!("\n{}", "=".repeat(80));
    println!("MARKETFORGE — FIXTURE-DRIVEN REAL TRADE TEST");
    println!("{}", "=".repeat(80));

    println!("Exchange        : {:?}", task.exchange);
    println!("Symbol          : {}", task.symbol);
    println!("Archive         : {}", task.input_path.display());
    println!("Task ID         : {}", execution.task_id);
    println!(
        "Records read    : {}",
        execution.metrics.counters.records_read
    );
    println!(
        "Records matched : {}",
        execution.metrics.counters.records_matched
    );
    println!(
        "Trades written  : {}",
        execution.metrics.counters.events_written
    );
    println!("Parquet rows    : {}", parquet_rows);
    println!("Parquet files   : {}", parquet_files.len());
    println!("Output          : {}", execution.output_path.display());
    println!("RESULT          : PASS");

    run.finish(&Ok(())).expect("persist test result");
}

#[test]
fn failed_task_preserves_metrics() {
    let mut run = TestRun::new("execution/missing_trade_source").expect("create isolated test run");

    let config = run
        .runtime(Overrides::default())
        .expect("load processing configuration");

    let job = selected_trade_job(&mut run, &config);
    let _limits = LimitScope::enter(
        config.resources.normalization_limits,
        "fixture-driven missing-source test".to_owned(),
    );
    let mut task = job.tasks[0].clone();

    // Resolve the valid fixture first, then inject a missing source.
    // This tests execution failure without changing fixture metadata.
    task.input_path = run.path.join("missing-source.zip");

    let output = run.path.join("task-output");

    let execution = execute_trade_task(
        &task,
        &config.integrity_policy,
        &config.resources.parquet,
        &output,
    );

    println!("Execution error: {:?}", execution.error);
    println!("Execution metrics: {:#?}", execution.metrics.counters);

    assert!(!execution.is_success());

    assert_eq!(execution.metrics.counters.tasks_failed, 1);
    assert_eq!(execution.metrics.counters.tasks_completed, 0);
    assert_eq!(execution.metrics.counters.records_read, 0);

    assert!(execution.error.is_some());

    println!("\nMARKETFORGE — FAILED ISOLATED TASK");
    println!("Task ID : {}", execution.task_id);
    println!("Error   : {:?}", execution.error);
    println!("RESULT  : PASS");

    run.finish(&Ok(())).expect("persist test result");
}
