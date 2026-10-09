use std::{collections::VecDeque, sync::Mutex, thread};

use crate::error::{MarketForgeError, Result};

/// Executes independent tasks using a bounded number of threads.
///
/// Results are returned in the same order as the input tasks,
/// regardless of completion order.
pub fn execute_parallel<T, O, F>(
    tasks: Vec<T>,
    workers: usize,
    execute: F,
) -> Result<Vec<Result<O>>>
where
    T: Send,
    O: Send,
    F: Fn(T) -> Result<O> + Sync,
{
    if workers == 0 {
        return Err(MarketForgeError::InvalidConfiguration(
            "parallel scheduler requires at least one worker".to_owned(),
        ));
    }

    if tasks.is_empty() {
        return Ok(Vec::new());
    }

    let task_count = tasks.len();
    let worker_count = workers.min(task_count);

    let queue = Mutex::new(tasks.into_iter().enumerate().collect::<VecDeque<_>>());

    let results = Mutex::new(
        std::iter::repeat_with(|| None)
            .take(task_count)
            .collect::<Vec<Option<Result<O>>>>(),
    );

    thread::scope(|scope| {
        let execute = &execute;
        let queue = &queue;
        let results = &results;

        for _ in 0..worker_count {
            scope.spawn(move || {
                loop {
                    let next = {
                        let mut queue = queue
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());

                        queue.pop_front()
                    };

                    let Some((index, task)) = next else {
                        break;
                    };

                    let result = execute(task);

                    let mut results = results
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());

                    results[index] = Some(result);
                }
            });
        }
    });

    let results = results.into_inner().map_err(|_| {
        MarketForgeError::InvalidConfiguration(
            "parallel scheduler result mutex poisoned".to_owned(),
        )
    })?;

    results
        .into_iter()
        .enumerate()
        .map(|(index, result)| {
            result.ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(format!(
                    "parallel scheduler lost result for task index {index}"
                ))
            })
        })
        .collect()
}
