#![cfg(feature = "historical")]

use std::path::{Path, PathBuf};

use {
    crate::support::migration::configuration as load_processing_config,
    crate::support::migration::template as load_processing_job,
    marketforge_engine::historical::normalize::session::execute_session_tasks,
};

const JOB_FILE: &str = "okx.future.linear.trades:BTC-USD_UM-261225";

fn project_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("project root")
        .to_path_buf()
}

fn load_job() -> marketforge_engine::historical::job::ProcessingJob {
    load_processing_job(JOB_FILE).expect("load processing job")
}

fn load_config() -> marketforge_engine::historical::normalize::ProcessingConfig {
    let root = project_root();

    load_processing_config(root.join("data/.config/historical/general.toml"), &root)
        .expect("load processing configuration")
}

#[test]
fn rejects_identical_staging_and_dataset_paths() {
    let mut job = load_job();

    job.output.staging_path = PathBuf::from("/tmp/marketforge-identical-output");

    job.output.dataset_path = job.output.staging_path.clone();

    let result = execute_session_tasks(&[job], &load_config());

    assert!(result.is_err());
}

#[test]
fn rejects_overlapping_job_paths() {
    let mut first = load_job();
    let mut second = load_job();

    first.output.staging_path = PathBuf::from("/tmp/marketforge-session-overlap/staging");

    first.output.dataset_path = PathBuf::from("/tmp/marketforge-session-overlap/dataset");

    second.output.staging_path = first.output.staging_path.join("nested");

    second.output.dataset_path = PathBuf::from("/tmp/marketforge-session-overlap/second-dataset");

    let result = execute_session_tasks(&[first, second], &load_config());

    assert!(result.is_err());
}

#[test]
fn rejects_duplicate_task_ids() {
    let mut job = load_job();

    job.tasks[1].task_id = job.tasks[0].task_id;

    let result = execute_session_tasks(&[job], &load_config());

    assert!(result.is_err());
}

#[test]
fn rejects_missing_normalization() {
    let mut job = load_job();

    job.tasks[0].normalizations.clear();

    let result = execute_session_tasks(&[job], &load_config());

    assert!(result.is_err());
}

#[test]
fn rejects_multiple_normalizations() {
    let mut job = load_job();

    let normalization = job.tasks[0].normalizations[0].clone();

    job.tasks[0].normalizations.push(normalization);

    let result = execute_session_tasks(&[job], &load_config());

    assert!(result.is_err());
}

#[test]
fn rejects_zero_workers() {
    let job = load_job();

    let mut config = load_config();

    config.resources.workers = 0;

    let result = execute_session_tasks(&[job], &config);

    assert!(result.is_err());
}
