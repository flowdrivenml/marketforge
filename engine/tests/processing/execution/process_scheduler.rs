#![cfg(feature = "process")]

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use marketforge_engine::{error::MarketForgeError, process::scheduler::execute_parallel};

#[test]
fn preserves_input_order() {
    let tasks = vec![0u64, 1, 2, 3, 4, 5];

    let results = execute_parallel(tasks, 3, |task| {
        thread::sleep(Duration::from_millis((6 - task) * 5));
        Ok(task * 10)
    })
    .unwrap();

    let values: Vec<u64> = results.into_iter().map(|result| result.unwrap()).collect();

    assert_eq!(values, vec![0, 10, 20, 30, 40, 50]);
}

#[test]
fn respects_worker_limit() {
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));

    let results = execute_parallel((0..32).collect::<Vec<_>>(), 4, |_| {
        let current = active.fetch_add(1, Ordering::SeqCst) + 1;

        maximum.fetch_max(current, Ordering::SeqCst);

        thread::sleep(Duration::from_millis(5));

        active.fetch_sub(1, Ordering::SeqCst);

        Ok(())
    })
    .unwrap();

    assert_eq!(results.len(), 32);

    assert!(
        maximum.load(Ordering::SeqCst) <= 4,
        "scheduler exceeded configured worker limit"
    );

    assert!(
        maximum.load(Ordering::SeqCst) > 1,
        "scheduler did not execute tasks concurrently"
    );
}

#[test]
fn preserves_individual_task_errors() {
    let results = execute_parallel(vec![0, 1, 2, 3], 2, |task| {
        if task == 2 {
            return Err(MarketForgeError::InvalidConfiguration(
                "synthetic task failure".to_owned(),
            ));
        }

        Ok(task)
    })
    .unwrap();

    assert!(results[0].is_ok());
    assert!(results[1].is_ok());
    assert!(results[2].is_err());
    assert!(results[3].is_ok());
}

#[test]
fn rejects_zero_workers() {
    let result = execute_parallel(vec![1, 2, 3], 0, Ok);

    assert!(result.is_err());
}

#[test]
fn handles_empty_task_list() {
    let results = execute_parallel(Vec::<u64>::new(), 4, |task| Ok(task)).unwrap();

    assert!(results.is_empty());
}

#[test]
fn handles_more_workers_than_tasks() {
    let results = execute_parallel(vec![1, 2], 16, |task| Ok(task * 2)).unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].as_ref().unwrap(), &2);
    assert_eq!(results[1].as_ref().unwrap(), &4);
}

#[test]
fn single_worker_executes_sequentially() {
    let active = AtomicUsize::new(0);
    let maximum = AtomicUsize::new(0);

    let results = execute_parallel((0..10).collect(), 1, |task| {
        let current = active.fetch_add(1, Ordering::SeqCst) + 1;

        maximum.fetch_max(current, Ordering::SeqCst);

        active.fetch_sub(1, Ordering::SeqCst);

        Ok(task)
    })
    .unwrap();

    assert_eq!(maximum.load(Ordering::SeqCst), 1);
    assert_eq!(results.len(), 10);
}
