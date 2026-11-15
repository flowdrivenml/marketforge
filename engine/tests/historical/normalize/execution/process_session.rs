#![cfg(feature = "historical")]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
};

use crate::support::{fixtures::workload, runtime::TestRun};

use marketforge_engine::{
    historical::{
        config::PreparedBatch,
        job::{ContentType, ProcessingJob},
        normalize::session::execute_session_tasks,
    },
    shared::historical::ProcessingConfig,
};

pub(super) fn selected_session_jobs(
    run: &mut TestRun,
    config: &ProcessingConfig,
) -> Vec<ProcessingJob> {
    let mut batch: PreparedBatch = workload().expect("load authoritative normalization batch");

    let max_job_id = batch.jobs.iter().map(|j| j.job_id.0).max().unwrap();
    let max_dataset_id = batch.jobs.iter().map(|j| j.dataset_id.0).max().unwrap();

    batch
        .jobs
        .retain(|j| j.output.content_type == ContentType::Trades);
    assert_eq!(batch.jobs.len(), 1, "expected one prepared trade job");

    let original = batch.jobs.remove(0);

    // Find the smallest available concrete archive for each exchange.
    let mut candidates = BTreeMap::new();

    for task in &original.tasks {
        if task.scope.is_some() || task.symbol.is_none() {
            continue;
        }

        let size = fs::metadata(&task.input_path)
            .expect("required real source archive")
            .len();

        let exchange = format!("{:?}", task.exchange);

        let candidate = (size, task.task_id.0);

        candidates
            .entry(exchange)
            .and_modify(|current: &mut (u64, u64)| {
                if candidate < *current {
                    *current = candidate;
                }
            })
            .or_insert(candidate);
    }

    let mut ranked = candidates
        .into_iter()
        .map(|(exchange, (size, task_id))| (size, exchange, task_id))
        .collect::<Vec<_>>();

    ranked.sort();

    assert!(
        ranked.len() >= 2,
        "authoritative fixture requires two available trade exchanges"
    );

    let selected: BTreeSet<u64> = ranked.into_iter().take(2).map(|(_, _, id)| id).collect();

    assert_eq!(selected.len(), 2);

    let selected_tasks = original
        .tasks
        .iter()
        .filter(|t| selected.contains(&t.task_id.0))
        .cloned()
        .collect::<Vec<_>>();

    assert_eq!(selected_tasks.len(), 2);

    let mut first = original.clone();
    first.tasks = vec![selected_tasks[0].clone()];

    let mut second = original;
    second.tasks = vec![selected_tasks[1].clone()];

    // Allocate identities solely for the two isolated test jobs.
    // Instrument and source identities remain exactly as prepared.
    second.job_id.0 = max_job_id.checked_add(1).unwrap();
    second.dataset_id.0 = max_dataset_id.checked_add(1).unwrap();

    batch.jobs = vec![first, second];

    let resolved = run
        .prepare(&batch, config)
        .expect("resolve selected session jobs");

    assert_eq!(resolved.jobs.len(), 2);

    for job in &resolved.jobs {
        assert_eq!(job.tasks.len(), 1);
        println!(
            "Selected job={} dataset={} stream={} source={}",
            job.job_id.0,
            job.dataset_id.0,
            job.tasks[0].stream_id.0,
            job.tasks[0].input_path.display()
        );
    }

    resolved.jobs
}

#[test]
fn executes_tasks_across_multiple_jobs() {
    let mut run =
        TestRun::new("execution/session_multiple_jobs").expect("create isolated session run");

    let config = run
        .runtime_from_general()
        .expect("load production normalization configuration");

    let jobs = selected_session_jobs(&mut run, &config);

    let expected_tasks: usize = jobs.iter().map(|j| j.tasks.len()).sum();

    let results = execute_session_tasks(&jobs, &config).expect("execute processing session tasks");

    assert_eq!(results.jobs.len(), jobs.len());
    assert_eq!(results.total_tasks, expected_tasks);

    let mut total_records_read = 0u64;
    let mut total_events_written = 0u64;

    for (job, result) in jobs.iter().zip(&results.jobs) {
        assert_eq!(result.executions.len(), job.tasks.len());

        for (task, execution) in job.tasks.iter().zip(&result.executions) {
            assert_eq!(execution.task_id, task.task_id.0);

            assert!(
                execution.is_success(),
                "task {} failed: {:?}",
                execution.task_id,
                execution.error
            );

            assert_eq!(execution.metrics.counters.tasks_completed, 1);
            assert_eq!(execution.metrics.counters.tasks_failed, 0);

            assert!(!execution.output_paths.is_empty());

            for path in &execution.output_paths {
                assert!(path.is_dir());
                assert!(
                    path.starts_with(&job.output.staging_path),
                    "task output escaped job staging: {}",
                    path.display()
                );
            }

            total_records_read += execution.metrics.counters.records_read;
            total_events_written += execution.metrics.counters.events_written;
        }
    }

    assert!(total_records_read > 0);
    assert!(total_events_written > 0);

    println!("Jobs:           {}", results.jobs.len());
    println!("Tasks:          {}", results.total_tasks);
    println!("Records read:   {total_records_read}");
    println!("Trades written: {total_events_written}");
    println!("SESSION EXECUTION: PASS");
}
