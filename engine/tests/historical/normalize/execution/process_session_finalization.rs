#![cfg(feature = "historical")]

use crate::support::runtime::TestRun;

use marketforge_engine::historical::normalize::{
    ProcessingStatus, failure::PROCESSING_FAILURE_FILENAME, session::execute_processing_session,
};

#[test]
fn session_commits_successful_jobs_and_preserves_failures() {
    let mut run =
        TestRun::new("execution/session_finalization").expect("create isolated finalization run");

    let config = run
        .runtime_from_general()
        .expect("load production normalization configuration");

    let mut jobs = super::process_session::selected_session_jobs(&mut run, &config);

    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].tasks.len(), 1);
    assert_eq!(jobs[1].tasks.len(), 1);

    // Inject a missing source into the second resolved job only.
    let missing = run.path.join("deliberately_missing_source.archive");

    assert!(!missing.exists());

    jobs[1].tasks[0].input_path = missing;

    let expected_tasks: usize = jobs.iter().map(|job| job.tasks.len()).sum();

    let result = execute_processing_session(&jobs, &config).expect("execute processing session");

    assert_eq!(result.jobs.len(), jobs.len());
    assert_eq!(result.total_tasks, expected_tasks);

    // The first job must commit independently.
    assert_eq!(result.jobs[0].status, ProcessingStatus::Complete);

    let successful_dataset = &jobs[0].output.dataset_path;

    assert!(successful_dataset.is_dir());
    assert!(successful_dataset.join("manifest.json").is_file());
    assert!(successful_dataset.join("processing-job.json").is_file());
    assert!(!jobs[0].output.staging_path.exists());

    // The second job must fail without publishing a dataset.
    assert_eq!(result.jobs[1].status, ProcessingStatus::Failed);

    assert!(!jobs[1].output.dataset_path.exists());

    let failure_path = jobs[1]
        .output
        .staging_path
        .join(PROCESSING_FAILURE_FILENAME);

    assert!(
        failure_path.is_file(),
        "missing failure report: {}",
        failure_path.display()
    );

    println!("Successful job: {:?}", result.jobs[0].status);
    println!("Failed job:     {:?}", result.jobs[1].status);
    println!("Failure report: {}", failure_path.display());
    println!("SESSION FINALIZATION: PASS");
}
