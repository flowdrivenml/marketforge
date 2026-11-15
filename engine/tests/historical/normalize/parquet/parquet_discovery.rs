#![cfg(feature = "historical")]

use std::{collections::BTreeSet, fs};

use crate::support::{fixtures::workload, runtime::TestRun};

use marketforge_engine::historical::{
    job::ContentType, normalize::manifest::inspect_parquet_files,
};

#[test]
fn discovers_parquet_files_from_multiple_tasks() {
    let mut run = TestRun::new("execution/parquet_discovery").expect("create isolated test run");

    let config = run
        .runtime_from_general()
        .expect("load production normalization configuration");

    let mut batch = workload().expect("load authoritative normalization batch");

    batch
        .jobs
        .retain(|job| job.output.content_type == ContentType::Trades);

    assert_eq!(batch.jobs.len(), 1);

    // Select two available concrete trade sources from distinct
    // streams, preferring smaller archives.
    let job = &mut batch.jobs[0];

    let mut candidates = job
        .tasks
        .iter()
        .filter(|task| task.scope.is_none() && task.symbol.is_some())
        .map(|task| {
            let size = fs::metadata(&task.input_path)
                .expect("required real archive")
                .len();

            (size, task.stream_id.0.clone(), task.task_id.0)
        })
        .collect::<Vec<_>>();

    candidates.sort();

    let mut streams = BTreeSet::new();
    let mut selected = BTreeSet::new();

    for (_, stream, task_id) in candidates {
        if streams.insert(stream) {
            selected.insert(task_id);
        }

        if selected.len() == 2 {
            break;
        }
    }

    assert_eq!(
        selected.len(),
        2,
        "expected two available concrete trade streams"
    );

    job.tasks.retain(|task| selected.contains(&task.task_id.0));

    let resolved = run
        .prepare(&batch, &config)
        .expect("resolve selected trade tasks");

    assert_eq!(resolved.jobs.len(), 1);

    let job = &resolved.jobs[0];

    assert_eq!(job.tasks.len(), 2);

    // Use the production session task executor with the explicitly
    // loaded runtime, rather than the legacy compatibility executor.
    let output = job.output.staging_path.clone();

    run.owned(&output).expect("validate staging ownership");

    let results = marketforge_engine::historical::normalize::session::execute_session_tasks(
        &resolved.jobs,
        &config,
    )
    .expect("execute selected trade tasks");

    assert_eq!(results.jobs.len(), 1);
    assert_eq!(results.total_tasks, job.tasks.len());

    let executions = &results.jobs[0].executions;

    assert_eq!(executions.len(), job.tasks.len());

    let mut expected_rows = 0u64;

    for (task, execution) in job.tasks.iter().zip(executions) {
        println!(
            "Processed task={} stream={} source={}",
            task.task_id.0,
            task.stream_id.0,
            task.input_path.display()
        );

        assert_eq!(execution.task_id, task.task_id.0);

        assert!(
            execution.is_success(),
            "task {} failed: {:?}",
            task.task_id.0,
            execution.error
        );

        assert_eq!(execution.metrics.counters.tasks_completed, 1);

        assert_eq!(execution.metrics.counters.tasks_failed, 0);

        assert!(!execution.output_paths.is_empty());

        for task_output in &execution.output_paths {
            assert!(
                task_output.starts_with(&output),
                "task output escaped staging: {}",
                task_output.display()
            );
        }

        expected_rows += execution.metrics.counters.events_written;
    }

    let files = inspect_parquet_files(&output).expect("discover nested Parquet files");

    assert!(!files.is_empty(), "no Parquet files discovered");

    let actual_rows: u64 = files.iter().map(|file| file.rows).sum();

    assert_eq!(
        actual_rows, expected_rows,
        "Parquet row count differs from worker accounting"
    );

    assert!(
        actual_rows > 0,
        "selected sources produced no canonical trades"
    );

    for file in &files {
        assert!(
            file.path.starts_with("streams/"),
            "unexpected Parquet path: {}",
            file.path
        );

        let components: Vec<_> = std::path::Path::new(&file.path).components().collect();

        assert_eq!(
            components.len(),
            4,
            "unexpected stream Parquet layout: {}",
            file.path
        );

        assert_eq!(
            components[2].as_os_str(),
            std::ffi::OsStr::new("trades"),
            "Parquet file is not under a trade stream"
        );

        assert!(file.path.ends_with(".parquet"));

        let path = output.join(&file.path);

        assert!(
            path.is_file(),
            "missing discovered file: {}",
            path.display()
        );

        let metadata = fs::metadata(&path).expect("read Parquet metadata");

        assert_eq!(
            metadata.len(),
            file.size_bytes,
            "discovered file size mismatch"
        );
    }

    println!("Tasks processed: {}", job.tasks.len());
    println!("Parquet files:   {}", files.len());
    println!("Expected rows:   {expected_rows}");
    println!("Actual rows:     {actual_rows}");
    println!("PARQUET DISCOVERY: PASS");
}
