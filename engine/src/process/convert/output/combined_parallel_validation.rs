use super::{
    combined_validation::{
        CombinedFileValidationResult, CombinedValidationMetrics, validate_combined_file,
    },
    files::discover_conversion_files,
};
use crate::error::{MarketForgeError, Result};
use crate::process::convert::progress::ConversionProgressReporter;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{
    collections::VecDeque,
    path::Path,
    sync::{Arc, Mutex},
    thread,
};

// -----------------------------------------------------------------------------
// Parallel combined validation
// -----------------------------------------------------------------------------

pub fn validate_converted_combined_parallel(
    directory: &Path,
    max_staleness_ns: u64,
    requested_workers: usize,
) -> Result<CombinedValidationMetrics> {
    validate_converted_combined_parallel_with_progress(
        directory,
        max_staleness_ns,
        requested_workers,
        &ConversionProgressReporter::disabled(),
    )
}

pub fn validate_converted_combined_parallel_with_progress(
    directory: &Path,
    max_staleness_ns: u64,
    requested_workers: usize,
    reporter: &ConversionProgressReporter,
) -> Result<CombinedValidationMetrics> {
    if max_staleness_ns == 0 {
        return invalid("combined validation staleness must be positive");
    }

    if requested_workers == 0 {
        return invalid("parallel combined validation requires positive worker count");
    }

    let files = discover_conversion_files(directory)?;

    if files.is_empty() {
        return invalid("converted combined dataset contains no Parquet files");
    }

    let file_count = files.len();
    let workers = requested_workers.min(file_count);
    reporter.validation_started(file_count as u64, 0);
    let row_groups_validated = AtomicU64::new(0);
    let files_completed = AtomicU64::new(0);
    let events_validated = AtomicU64::new(0);
    // -------------------------------------------------------------------------
    // Shared work queue
    // -------------------------------------------------------------------------

    let queue = Arc::new(Mutex::new((0..file_count).collect::<VecDeque<_>>()));

    let results = Arc::new(Mutex::new(
        (0..file_count)
            .map(|_| None)
            .collect::<Vec<Option<CombinedFileValidationResult>>>(),
    ));

    // -------------------------------------------------------------------------
    // Execute independent file validators
    // -------------------------------------------------------------------------
    thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::with_capacity(workers);

        for _ in 0..workers {
            let queue = Arc::clone(&queue);
            let results = Arc::clone(&results);
            let files = &files;
            let row_groups_validated = &row_groups_validated;
            let files_completed = &files_completed;
            let events_validated = &events_validated;
            handles.push(scope.spawn(move || -> Result<()> {
                loop {
                    let index = {
                        let mut queue = queue
                            .lock()
                            .map_err(|_| invalid_error("combined validation queue poisoned"))?;

                        queue.pop_front()
                    };

                    let Some(index) = index else {
                        break;
                    };

                    let result = validate_combined_file(&files[index], max_staleness_ns)?;
                    let file_events = result.metrics.events;
                    let file_row_groups = result.metrics.row_groups;

                    events_validated.fetch_add(file_events, Ordering::Relaxed);

                    row_groups_validated.fetch_add(file_row_groups, Ordering::Relaxed);

                    files_completed.fetch_add(1, Ordering::Relaxed);

                    reporter.validation_progress(
                        files_completed.load(Ordering::Relaxed),
                        events_validated.load(Ordering::Relaxed),
                        row_groups_validated.load(Ordering::Relaxed),
                    );
                    let mut results = results
                        .lock()
                        .map_err(|_| invalid_error("combined validation results poisoned"))?;

                    results[index] = Some(result);
                }
                Ok(())
            }));
        }

        // ---------------------------------------------------------------------
        // Join all workers
        // ---------------------------------------------------------------------

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
                        first_error = Some(invalid_error("parallel combined validator panicked"));
                    }
                }
            }
        }

        if let Some(error) = first_error {
            return Err(error);
        }

        Ok(())
    })?;

    // -------------------------------------------------------------------------
    // Collect results deterministically
    // -------------------------------------------------------------------------

    let results = Arc::try_unwrap(results)
        .map_err(|_| invalid_error("combined validation results still shared"))?
        .into_inner()
        .map_err(|_| invalid_error("combined validation results poisoned"))?;

    let mut metrics = CombinedValidationMetrics::default();
    let mut previous_key = None;

    for result in results {
        let result =
            result.ok_or_else(|| invalid_error("missing parallel combined validation result"))?;

        // Cross-file chronological ordering.
        if let Some(previous) = previous_key {
            if result.first_key < previous {
                return invalid("parallel combined ordering regressed across files");
            }
        }

        previous_key = Some(result.last_key);

        metrics.accumulate(&result.metrics)?;
    }

    // -------------------------------------------------------------------------
    // Global accounting
    // -------------------------------------------------------------------------

    if metrics.events == 0 {
        return invalid("converted combined dataset is empty");
    }

    if metrics.events != metrics.trade_events + metrics.depth_events {
        return invalid("parallel combined event-kind accounting mismatch");
    }

    let accounted = metrics
        .identity
        .checked_add(metrics.converted)
        .and_then(|value| value.checked_add(metrics.unavailable))
        .and_then(|value| value.checked_add(metrics.skipped))
        .ok_or_else(|| invalid_error("combined status counter overflow"))?;

    if accounted != metrics.events {
        return invalid("parallel combined conversion status mismatch");
    }

    if metrics.files != file_count as u64 {
        return invalid("parallel combined file accounting mismatch");
    }

    let accounted_levels = metrics
        .depth_levels_converted
        .checked_add(metrics.depth_levels_unavailable)
        .and_then(|value| value.checked_add(metrics.depth_levels_skipped))
        .ok_or_else(|| invalid_error("parallel combined depth counter overflow"))?;

    if accounted_levels != metrics.depth_levels {
        return invalid("parallel combined depth level accounting mismatch");
    }

    if metrics.numerical_depth_levels != metrics.depth_levels_converted {
        return invalid("parallel combined numerical depth accounting mismatch");
    }

    // All file validations and global accounting checks succeeded.
    reporter.validation_completed(metrics.files, metrics.events, metrics.row_groups);

    Ok(metrics)
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
