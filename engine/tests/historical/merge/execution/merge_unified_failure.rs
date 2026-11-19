use crate::{merge_test_job::small_runtime, publication::bounded_trade_job};
use marketforge_engine::historical::{
    job::MergeJobDocument,
    merge::{MergeCommitPaths, execute_unified_merge_job},
    normalize::manifest::file_sha256,
};
use std::{fs, path::Path};

fn test_job(root: &Path) -> MergeJobDocument {
    bounded_trade_job(root.join("merged"))
}

fn expect_failure(
    workspace: &Path,
    job: MergeJobDocument,
    diagnostic: &str,
    update_runtime: impl FnOnce(
        &mut marketforge_engine::shared::historical::config::MergeRuntimeSettings,
    ),
) {
    let mut runtime = small_runtime();
    runtime.resources.memory_limit_mb = 2048;
    update_runtime(&mut runtime);
    let result = execute_unified_merge_job(&job.with_runtime(runtime).unwrap(), workspace);
    let err = result.unwrap_err();
    assert!(err.to_string().contains(diagnostic), "{err}");
}

fn validated_job(root: &Path) -> MergeJobDocument {
    let mut baseline = test_job(root);
    baseline.output.dataset_path = root.join("baseline");
    execute_unified_merge_job(
        &crate::merge_test_job::bounded_runtime(baseline.clone()),
        root,
    )
    .unwrap();
    baseline.output.dataset_path = root.join("merged");
    baseline
}

#[test]
fn rejects_missing_input_dataset() {
    let temporary = tempfile::tempdir().unwrap();
    let job = validated_job(temporary.path());
    let directory = job.inputs[0].publication.manifest_path.parent().unwrap();
    fs::rename(directory, temporary.path().join("moved-input")).unwrap();
    expect_failure(temporary.path(), job, "does not exist", |_| {});
    assert!(!temporary.path().join("merged").exists());
}

#[test]
fn rejects_corrupted_source_manifest() {
    let temporary = tempfile::tempdir().unwrap();
    let mut job = validated_job(temporary.path());
    let manifest = job.inputs[0].publication.manifest_path.clone();
    fs::write(&manifest, b"{ invalid json").unwrap();
    let fingerprint = file_sha256(&manifest).unwrap();
    for input in &mut job.inputs {
        input.publication.manifest_sha256.clone_from(&fingerprint);
    }
    expect_failure(temporary.path(), job, "invalid dataset manifest", |_| {});
    assert!(!temporary.path().join("merged").exists());
}

#[test]
fn rejects_missing_processing_job() {
    let temporary = tempfile::tempdir().unwrap();
    let job = validated_job(temporary.path());
    let directory = job.inputs[0].publication.manifest_path.parent().unwrap();
    fs::remove_file(directory.join("processing-job.json")).unwrap();
    expect_failure(temporary.path(), job, "committed job: No such file", |_| {});
    assert!(!temporary.path().join("merged").exists());
}

#[test]
fn rejects_existing_output_without_modification() {
    let temporary = tempfile::tempdir().unwrap();
    let job = validated_job(temporary.path());
    fs::create_dir(&job.output.dataset_path).unwrap();
    let marker = job.output.dataset_path.join("marker");
    fs::write(&marker, b"keep").unwrap();
    expect_failure(temporary.path(), job, "already exists", |_| {});
    assert_eq!(fs::read(marker).unwrap(), b"keep");
}

#[test]
fn rejects_existing_staging_directory() {
    let temporary = tempfile::tempdir().unwrap();
    let job = validated_job(temporary.path());
    let commit = MergeCommitPaths::new(&job.output.dataset_path, job.job_id).unwrap();
    fs::create_dir(&commit.staging_path).unwrap();
    let marker = commit.staging_path.join("marker");
    fs::write(&marker, b"keep").unwrap();
    expect_failure(
        temporary.path(),
        job,
        "failed to create fresh staging directory",
        |_| {},
    );
    assert_eq!(fs::read(marker).unwrap(), b"keep");
    assert!(!commit.dataset_path.exists());
}

#[test]
fn rejects_insufficient_memory_budget() {
    let temporary = tempfile::tempdir().unwrap();
    expect_failure(
        temporary.path(),
        validated_job(temporary.path()),
        "memory",
        |settings| {
            settings.resources.memory_limit_mb = 1;
        },
    );
    assert!(!temporary.path().join("merged").exists());
}

#[test]
fn rejects_insufficient_file_budget() {
    let temporary = tempfile::tempdir().unwrap();
    expect_failure(
        temporary.path(),
        validated_job(temporary.path()),
        "file",
        |settings| {
            settings.resources.max_open_files = 1;
        },
    );
    assert!(!temporary.path().join("merged").exists());
}
