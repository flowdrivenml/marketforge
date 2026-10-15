use std::{
    fs,
    path::Path,
    sync::{Arc, Barrier},
    thread,
};

use marketforge_engine::historical::convert::output::atomic_publish_directory;

fn create_directory(path: &Path, marker: &str) {
    fs::create_dir_all(path).unwrap();
    fs::write(path.join("marker.txt"), marker).unwrap();
}

// -----------------------------------------------------------------------------
// Successful publication
// -----------------------------------------------------------------------------

#[test]
fn publishes_directory_atomically() {
    let temporary = tempfile::tempdir().unwrap();

    let staging = temporary.path().join("dataset.staging");
    let final_path = temporary.path().join("dataset");

    create_directory(&staging, "original");

    atomic_publish_directory(&staging, &final_path).unwrap();

    assert!(!staging.exists());
    assert!(final_path.is_dir());

    assert_eq!(
        fs::read_to_string(final_path.join("marker.txt")).unwrap(),
        "original"
    );
}

// -----------------------------------------------------------------------------
// No overwrite
// -----------------------------------------------------------------------------

#[test]
fn rejects_existing_destination() {
    let temporary = tempfile::tempdir().unwrap();

    let staging = temporary.path().join("dataset.staging");
    let final_path = temporary.path().join("dataset");

    create_directory(&staging, "new");
    create_directory(&final_path, "existing");

    assert!(atomic_publish_directory(&staging, &final_path).is_err());

    assert!(staging.is_dir());

    assert_eq!(
        fs::read_to_string(final_path.join("marker.txt")).unwrap(),
        "existing"
    );

    assert_eq!(
        fs::read_to_string(staging.join("marker.txt")).unwrap(),
        "new"
    );
}

// -----------------------------------------------------------------------------
// Invalid staging
// -----------------------------------------------------------------------------

#[test]
fn rejects_missing_staging_directory() {
    let temporary = tempfile::tempdir().unwrap();

    let staging = temporary.path().join("missing");
    let final_path = temporary.path().join("dataset");

    assert!(atomic_publish_directory(&staging, &final_path).is_err());

    assert!(!final_path.exists());
}

// -----------------------------------------------------------------------------
// Identical paths
// -----------------------------------------------------------------------------

#[test]
fn rejects_identical_paths() {
    let temporary = tempfile::tempdir().unwrap();

    let staging = temporary.path().join("dataset");

    create_directory(&staging, "original");

    assert!(atomic_publish_directory(&staging, &staging).is_err());

    assert!(staging.is_dir());
}

// -----------------------------------------------------------------------------
// Concurrent publication
// -----------------------------------------------------------------------------

#[test]
fn concurrent_publication_has_exactly_one_winner() {
    let temporary = tempfile::tempdir().unwrap();

    let first = temporary.path().join("first.staging");
    let second = temporary.path().join("second.staging");
    let final_path = temporary.path().join("dataset");

    create_directory(&first, "first");
    create_directory(&second, "second");

    let barrier = Arc::new(Barrier::new(3));

    let handles = [first.clone(), second.clone()]
        .into_iter()
        .map(|staging| {
            let barrier = Arc::clone(&barrier);
            let final_path = final_path.clone();

            thread::spawn(move || {
                barrier.wait();

                atomic_publish_directory(&staging, &final_path).is_ok()
            })
        })
        .collect::<Vec<_>>();

    barrier.wait();

    let results = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();

    assert_eq!(results.iter().filter(|success| **success).count(), 1);

    let winner = fs::read_to_string(final_path.join("marker.txt")).unwrap();

    assert!(winner == "first" || winner == "second");

    // The losing staging directory must remain intact.
    let loser = if winner == "first" { &second } else { &first };

    assert!(loser.is_dir());
}
