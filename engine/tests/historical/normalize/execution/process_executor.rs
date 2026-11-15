#![cfg(feature = "historical")]

use std::fs::File;

use arrow_array::{Array, Int64Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::support::{fixtures::workload, runtime::TestRun};

use marketforge_engine::{
    historical::{
        job::{ContentType, InstrumentKind},
        normalize::{
            ProcessingStatus, execute_processing_job_with_progress,
            manifest::{DatasetManifest, inspect_parquet_files},
            progress::{ProgressReporter, ProgressTaskId, diagnostics::Diagnostics},
        },
    },
    shared::domain::canonical::Exchange,
};

#[test]
fn processes_multiple_trade_tasks_into_staging() {
    let mut run =
        TestRun::new("execution/real_multi_archive_trades").expect("create isolated test run");

    let config = run
        .runtime_from_general()
        .expect("load production normalization configuration");

    let mut batch = workload().expect("load authoritative normalize.json");

    batch = crate::support::fixtures::concrete_stream(
        &batch,
        Exchange::Bybit,
        InstrumentKind::Spot,
        ContentType::Trades,
        2,
    )
    .expect("prepared multi-archive trade stream");
    assert_eq!(batch.jobs.len(), 1, "one explicitly selected stream job");
    let selected_tasks = &batch.jobs[0].tasks;
    assert!(
        selected_tasks.len() >= 2,
        "multi-archive coverage requires multiple sources"
    );

    for task in selected_tasks {
        assert!(
            task.input_path.is_file(),
            "missing source archive: {}",
            task.input_path.display()
        );

        println!(
            "Selected task {}: {}",
            task.task_id.0,
            task.input_path.display()
        );
    }

    // Resolve through the same production catalog as the CLI.
    let resolved = run
        .prepare(&batch, &config)
        .expect("resolve prepared batch");

    assert_eq!(resolved.jobs.len(), 1);

    let job = &resolved.jobs[0];

    assert_eq!(job.tasks.len(), selected_tasks.len());

    println!("\n{}", "=".repeat(90));
    println!("MARKETFORGE — REAL MULTI-ARCHIVE TRADE TEST");
    println!("{}", "=".repeat(90));

    println!("Sources     : {}", job.tasks.len());
    println!("Staging     : {}", job.output.staging_path.display());
    println!("Dataset     : {}", job.output.dataset_path.display());

    // Only diagnostics may be enabled explicitly; resource settings remain the
    // settings loaded from general.toml above.
    let mut config = config;
    if let Ok(mode) = std::env::var("MARKETFORGE_TEST_DIAGNOSTICS") {
        use marketforge_engine::shared::historical::config::DiagnosticsMode;
        config.diagnostics.mode = match mode.as_str() {
            "disabled" => DiagnosticsMode::Disabled,
            "basic" => DiagnosticsMode::Basic,
            "detailed" => DiagnosticsMode::Detailed,
            _ => panic!("MARKETFORGE_TEST_DIAGNOSTICS must be disabled, basic, or detailed"),
        };
    }
    let reporter = ProgressReporter::disabled();
    let diagnostics = Diagnostics::start(&config, &run.path, &reporter);
    let execution_start = std::time::Instant::now();
    let result = execute_processing_job_with_progress(job, &config, &reporter)
        .expect("execute processing job");

    reporter.measured_wall_time(
        ProgressTaskId::new(job.job_id.0, 0),
        "engine execution",
        execution_start.elapsed(),
    );
    let verification_start = std::time::Instant::now();
    reporter.stream_phase(job.job_id.0, "", "test output verification");
    assert_eq!(
        result.status,
        ProcessingStatus::Complete,
        "processing failed: {:?}",
        result.failure
    );

    assert!(
        !job.output.staging_path.exists(),
        "staging directory remains after successful commit"
    );

    assert!(
        job.output.dataset_path.join("manifest.json").is_file(),
        "missing committed dataset manifest"
    );

    // Discover canonical Parquet files relative to the dataset root.
    let files =
        inspect_parquet_files(&job.output.dataset_path).expect("discover canonical Parquet files");

    assert!(!files.is_empty(), "no Parquet files produced");

    let manifest: DatasetManifest = serde_json::from_slice(
        &std::fs::read(job.output.dataset_path.join("manifest.json")).expect("read manifest"),
    )
    .expect("deserialize manifest");

    assert_eq!(manifest.job_id, job.job_id.0);
    assert_eq!(manifest.dataset_id, job.dataset_id.0);

    assert_eq!(
        manifest.source_tasks.len(),
        selected_tasks.len(),
        "manifest must account for all selected sources"
    );

    assert_eq!(manifest.events_written, result.events_written);

    assert_eq!(manifest.files_written, files.len() as u64);

    assert_eq!(
        manifest.processing_metrics.counters.events_written,
        result.events_written
    );

    // All selected tasks target one concrete logical trade stream.
    assert_eq!(
        manifest.streams.len(),
        1,
        "expected one concrete trade stream"
    );

    // Independently verify the actual Parquet rows and chronology.
    //
    // Only one Arrow batch is retained at a time.
    let mut total_rows = 0u64;
    let mut previous_timestamp: Option<i64> = None;
    let mut minimum_timestamp: Option<i64> = None;
    let mut maximum_timestamp: Option<i64> = None;

    for file in &files {
        let path = job.output.dataset_path.join(&file.path);

        assert!(
            path.is_file(),
            "manifest references missing Parquet: {}",
            path.display()
        );

        let reader =
            ParquetRecordBatchReaderBuilder::try_new(File::open(&path).expect("open Parquet"))
                .expect("read Parquet metadata")
                .with_batch_size(8192)
                .build()
                .expect("create Parquet reader");

        for batch in reader {
            let batch = batch.expect("read Arrow batch");

            let timestamps = batch
                .column_by_name("event_timestamp_ns")
                .expect("missing timestamp column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("timestamp column must be Int64");

            for index in 0..timestamps.len() {
                assert!(!timestamps.is_null(index), "null canonical event timestamp");

                let timestamp = timestamps.value(index);

                if let Some(previous) = previous_timestamp {
                    assert!(
                        timestamp >= previous,
                        "timestamp regression across canonical output: \
                         previous={previous}, current={timestamp}, \
                         file={}",
                        file.path
                    );
                }

                minimum_timestamp.get_or_insert(timestamp);
                maximum_timestamp = Some(timestamp);
                previous_timestamp = Some(timestamp);

                total_rows += 1;
            }
        }
    }

    assert!(total_rows > 0, "normalization produced no trades");

    assert_eq!(
        total_rows, result.events_written,
        "Parquet row count differs from reported events"
    );

    assert_eq!(minimum_timestamp, result.start_timestamp_ns);

    assert_eq!(maximum_timestamp, result.end_timestamp_ns);

    assert_eq!(manifest.start_timestamp_ns, minimum_timestamp);

    assert_eq!(manifest.end_timestamp_ns, maximum_timestamp);

    println!("\nRESULTS");
    println!("  Source archives     : {}", job.tasks.len());
    println!("  Logical streams     : {}", manifest.streams.len());
    println!("  Parquet files       : {}", files.len());
    println!("  Canonical trades    : {total_rows}");
    println!("  Minimum timestamp  : {minimum_timestamp:?}");
    println!("  Maximum timestamp  : {maximum_timestamp:?}");
    println!("  Chronological order: VERIFIED");
    println!("  Manifest accounting: VERIFIED");
    println!(
        "  Dataset            : {}",
        job.output.dataset_path.display()
    );
    println!("\nRESULT: PASS");

    reporter.measured_wall_time(
        ProgressTaskId::new(job.job_id.0, 0),
        "test output verification",
        verification_start.elapsed(),
    );
    if let Some(diagnostics) = diagnostics {
        diagnostics.finish(true);
    }
    run.finish(&Ok(())).expect("record successful test result");
}
