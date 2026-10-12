#![cfg(feature = "process")]

use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use marketforge_engine::process::merge::MergeCommitPaths;

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new() -> Self {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

        let path = std::env::temp_dir().join(format!(
            "marketforge-merge-commit-{}-{id}",
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

#[test]
fn commits_staged_merge_dataset() {
    let directory = TestDirectory::new();

    let destination = directory.path.join("merged-200");

    let paths = MergeCommitPaths::new(&destination, 200).expect("construct commit paths");

    paths.prepare().expect("prepare staging");

    fs::write(paths.staging_path.join("manifest.json"), b"{\"test\":true}").unwrap();

    paths.commit().expect("commit dataset");

    assert!(destination.join("manifest.json").is_file());
    assert!(!paths.staging_path.exists());
}

#[test]
fn rejects_existing_destination() {
    let directory = TestDirectory::new();

    let destination = directory.path.join("merged-200");

    fs::create_dir_all(&destination).unwrap();

    let paths = MergeCommitPaths::new(&destination, 200).expect("construct commit paths");

    assert!(paths.prepare().is_err());
}

#[test]
fn rejects_existing_staging_directory() {
    let directory = TestDirectory::new();

    let destination = directory.path.join("merged-200");

    let paths = MergeCommitPaths::new(&destination, 200).expect("construct commit paths");

    fs::create_dir_all(&paths.staging_path).unwrap();

    assert!(paths.prepare().is_err());
}
