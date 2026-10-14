use std::{
    collections::VecDeque,
    path::Path,
    sync::{Arc, Mutex},
    thread,
};

use crate::error::{MarketForgeError, Result};

use super::{
    depth_validation::{
        DepthConversionValidationMetrics, DepthFileValidationResult, reconcile_depth_validation,
        validate_depth_file,
    },
    files::discover_conversion_files,
};

pub fn validate_converted_depth_parallel(
    directory: &Path,
    max_staleness_ns: u64,
    requested_workers: usize,
) -> Result<DepthConversionValidationMetrics> {
    if max_staleness_ns == 0 {
        return invalid("depth conversion staleness limit must be positive");
    }

    if requested_workers == 0 {
        return invalid("parallel depth validation requires positive worker count");
    }

    let files = discover_conversion_files(directory)?;
    let file_count = files.len();
    let workers = requested_workers.min(file_count);

    let queue = Arc::new(Mutex::new((0..file_count).collect::<VecDeque<_>>()));

    let results = Arc::new(Mutex::new(
        (0..file_count)
            .map(|_| None)
            .collect::<Vec<Option<DepthFileValidationResult>>>(),
    ));

    thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::with_capacity(workers);

        for _ in 0..workers {
            let queue = Arc::clone(&queue);
            let results = Arc::clone(&results);
            let files = &files;

            handles.push(scope.spawn(move || -> Result<()> {
                loop {
                    let index = {
                        let mut queue = queue
                            .lock()
                            .map_err(|_| invalid_error("depth validation queue poisoned"))?;

                        queue.pop_front()
                    };

                    let Some(index) = index else {
                        break;
                    };

                    let result = validate_depth_file(&files[index], max_staleness_ns)?;

                    let mut results = results
                        .lock()
                        .map_err(|_| invalid_error("depth validation results poisoned"))?;

                    results[index] = Some(result);
                }

                Ok(())
            }));
        }

        let mut first_error = None;

        for handle in handles {
            match handle.join() {
                Ok(Ok(())) => {}

                Ok(Err(error)) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }

                Err(_) => {
                    if first_error.is_none() {
                        first_error = Some(invalid_error("parallel depth validator panicked"));
                    }
                }
            }
        }

        if let Some(error) = first_error {
            return Err(error);
        }

        Ok(())
    })?;

    let results = Arc::try_unwrap(results)
        .map_err(|_| invalid_error("depth validation results still shared"))?
        .into_inner()
        .map_err(|_| invalid_error("depth validation results poisoned"))?;

    let mut metrics = DepthConversionValidationMetrics::default();
    let mut previous_key = None;

    for result in results {
        let result =
            result.ok_or_else(|| invalid_error("missing parallel depth validation result"))?;

        if let Some(previous) = previous_key {
            if result.first_key < previous {
                return invalid("parallel depth ordering regressed across files");
            }
        }

        previous_key = Some(result.last_key);

        metrics.accumulate(&result.metrics)?;
    }

    reconcile_depth_validation(&metrics)?;

    Ok(metrics)
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
