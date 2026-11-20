use crate::historical::convert::progress::ConversionProgressReporter;
use crate::shared::error::{MarketForgeError, Result};
use std::sync::atomic::{AtomicU64, Ordering};
use std::{
    collections::VecDeque,
    path::Path,
    sync::{Arc, Mutex},
    thread,
};
use {
    crate::historical::convert::output::shared::files::discover_conversion_files,
    crate::historical::convert::output::validation::depth::DepthConversionValidationMetrics,
    crate::historical::convert::output::validation::depth::DepthFileValidationResult,
    crate::historical::convert::output::validation::depth::reconcile_depth_validation,
    crate::historical::convert::output::validation::depth::validate_depth_file_with_progress,
};

pub fn validate_converted_depth_parallel(
    directory: &Path,
    max_staleness_ns: u64,
    requested_workers: usize,
) -> Result<DepthConversionValidationMetrics> {
    validate_converted_depth_parallel_with_progress(
        directory,
        max_staleness_ns,
        requested_workers,
        &ConversionProgressReporter::disabled(),
    )
}

pub fn validate_converted_depth_parallel_with_progress(
    directory: &Path,
    max_staleness_ns: u64,
    requested_workers: usize,
    reporter: &ConversionProgressReporter,
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
    reporter.validation_started(file_count as u64, 0);

    let files_completed = AtomicU64::new(0);
    let events_validated = AtomicU64::new(0);
    let row_groups_validated = AtomicU64::new(0);
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
            let files_completed = &files_completed;
            let events_validated = &events_validated;
            let row_groups_validated = &row_groups_validated;
            handles.push(scope.spawn(move || -> Result<()> {
                loop {
                    reporter.check_interrupted()?;
                    let index = {
                        let mut queue = queue
                            .lock()
                            .map_err(|_| invalid_error("depth validation queue poisoned"))?;

                        queue.pop_front()
                    };

                    let Some(index) = index else {
                        break;
                    };

                    let mut last_update = std::time::Instant::now();
                    let result = validate_depth_file_with_progress(
                        &files[index],
                        max_staleness_ns,
                        reporter,
                        |rows| {
                            events_validated.fetch_add(rows, Ordering::Relaxed);
                            if reporter.is_enabled()
                                && last_update.elapsed() >= std::time::Duration::from_millis(250)
                            {
                                reporter.validation_progress(
                                    files_completed.load(Ordering::Relaxed),
                                    events_validated.load(Ordering::Relaxed),
                                    row_groups_validated.load(Ordering::Relaxed),
                                );
                                last_update = std::time::Instant::now();
                            }
                        },
                    )?;
                    let file_row_groups = result.metrics.row_groups;

                    row_groups_validated.fetch_add(file_row_groups, Ordering::Relaxed);
                    files_completed.fetch_add(1, Ordering::Relaxed);

                    reporter.validation_progress(
                        files_completed.load(Ordering::Relaxed),
                        events_validated.load(Ordering::Relaxed),
                        row_groups_validated.load(Ordering::Relaxed),
                    );
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
    let mut previous = None;

    for result in results {
        let result =
            result.ok_or_else(|| invalid_error("missing parallel depth validation result"))?;

        super::depth::validate_depth_file_boundary(previous.as_ref(), &result)?;
        metrics.accumulate(&result.metrics)?;
        previous = Some(result);
    }

    reconcile_depth_validation(&metrics)?;

    reporter.validation_completed(metrics.files, metrics.events, metrics.row_groups);

    Ok(metrics)
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
