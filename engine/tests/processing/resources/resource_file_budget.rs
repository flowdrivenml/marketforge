#![cfg(feature = "process")]

use std::{
    sync::{Arc, Barrier},
    thread,
    time::Duration,
};

use marketforge_engine::process::resources::{ResourceBudget, ResourceRequirements};

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

fn requirements(files: u64) -> ResourceRequirements {
    ResourceRequirements {
        memory_bytes: 128,
        scratch_bytes: 0,
        open_files: files,
    }
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[test]
fn reserves_and_releases_open_files() {
    let budget = ResourceBudget::with_file_limit(1024, 1024, 10).unwrap();

    {
        let _reservation = budget.acquire(requirements(6)).unwrap();

        assert_eq!(budget.usage().unwrap().open_files, 6);
    }

    assert_eq!(budget.usage().unwrap().open_files, 0);
}

#[test]
fn rejects_requirements_exceeding_capacity() {
    let budget = ResourceBudget::with_file_limit(1024, 1024, 10).unwrap();

    assert!(budget.acquire(requirements(11)).is_err());
}

#[test]
fn waits_for_available_file_capacity() {
    let budget = Arc::new(ResourceBudget::with_file_limit(1024, 1024, 4).unwrap());

    let barrier = Arc::new(Barrier::new(2));

    let first_budget = Arc::clone(&budget);
    let first_barrier = Arc::clone(&barrier);

    let handle = thread::spawn(move || {
        let _reservation = first_budget.acquire(requirements(4)).unwrap();

        first_barrier.wait();

        thread::sleep(Duration::from_millis(100));
    });

    barrier.wait();

    let _second = budget.acquire(requirements(4)).unwrap();

    handle.join().unwrap();

    assert_eq!(budget.usage().unwrap().open_files, 4);
}

#[test]
fn legacy_constructor_preserves_behavior() {
    let budget = ResourceBudget::new(1024, 1024).unwrap();

    assert_eq!(budget.capacity().open_files, u64::MAX);

    let _reservation = budget.acquire(requirements(100)).unwrap();

    assert_eq!(budget.usage().unwrap().open_files, 100);
}
