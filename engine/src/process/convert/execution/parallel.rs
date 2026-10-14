use std::{
    collections::VecDeque,
    path::Path,
    sync::{Arc, Mutex},
    thread,
};

use crate::{
    error::{MarketForgeError, Result},
    job::{ConvertJob, MergeParquetConfig},
    process::{convert::ConversionPlan, merge::PhysicalMergeManifest},
};

use super::{
    ConversionPartitionPlan, ConversionPartitionResult, ConversionPrepassResult,
    PhysicalConversionMetrics, execute_conversion_partition,
};

// -----------------------------------------------------------------------------
// Parallel execution result
// -----------------------------------------------------------------------------

#[derive(Debug)]
pub struct ParallelConversionResult {
    pub partitions: Vec<ConversionPartitionResult>,
    pub metrics: PhysicalConversionMetrics,
    pub workers_used: usize,
}

// -----------------------------------------------------------------------------
// Execute partitions concurrently
// -----------------------------------------------------------------------------

pub fn execute_parallel_conversion(
    job: &ConvertJob,
    manifest: &PhysicalMergeManifest,
    plan: &ConversionPlan,
    partitions: &ConversionPartitionPlan,
    prepass: &ConversionPrepassResult,
    staging_directory: &Path,
    requested_workers: usize,
    parquet_config: &MergeParquetConfig,
) -> Result<ParallelConversionResult> {
    job.validate()?;
    manifest.validate()?;

    if requested_workers == 0 {
        return invalid("parallel conversion requires at least one worker");
    }

    if partitions.partitions.is_empty() {
        return invalid("parallel conversion contains no partitions");
    }

    if prepass.checkpoints.len() != partitions.partitions.len() {
        return invalid("parallel conversion checkpoint count mismatch");
    }

    if prepass.events_scanned != manifest.events_written
        || partitions.total_rows != manifest.events_written
    {
        return invalid("parallel conversion source event count mismatch");
    }

    if staging_directory.exists() {
        return invalid(format!(
            "parallel conversion staging already exists: {}",
            staging_directory.display()
        ));
    }

    let workers_used = requested_workers.min(partitions.partitions.len());

    // -------------------------------------------------------------------------
    // Validate checkpoint alignment
    // -------------------------------------------------------------------------

    for (partition, checkpoint) in partitions.partitions.iter().zip(&prepass.checkpoints) {
        if partition.partition_id != checkpoint.partition_id
            || partition.start_row != checkpoint.events_before_partition
        {
            return invalid("parallel conversion checkpoint alignment mismatch");
        }
    }

    // -------------------------------------------------------------------------
    // Prepare shared work queue
    // -------------------------------------------------------------------------

    let queue = Arc::new(Mutex::new(
        (0..partitions.partitions.len()).collect::<VecDeque<_>>(),
    ));

    let results = Arc::new(Mutex::new(Vec::<ConversionPartitionResult>::new()));

    // -------------------------------------------------------------------------
    // Spawn bounded worker pool
    // -------------------------------------------------------------------------

    thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::with_capacity(workers_used);

        for _ in 0..workers_used {
            let queue = Arc::clone(&queue);
            let results = Arc::clone(&results);

            handles.push(scope.spawn(move || -> Result<()> {
                loop {
                    let index = {
                        let mut queue = queue
                            .lock()
                            .map_err(|_| invalid_error("parallel conversion queue poisoned"))?;

                        queue.pop_front()
                    };

                    let Some(index) = index else {
                        break;
                    };

                    let partition = &partitions.partitions[index];
                    let checkpoint = &prepass.checkpoints[index];

                    let result = execute_conversion_partition(
                        job,
                        manifest,
                        plan,
                        partitions,
                        partition,
                        checkpoint,
                        staging_directory,
                        parquet_config,
                    )?;

                    results
                        .lock()
                        .map_err(|_| invalid_error("parallel conversion results poisoned"))?
                        .push(result);
                }

                Ok(())
            }));
        }

        // -------------------------------------------------------------
        // Join all workers
        // -------------------------------------------------------------

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
                        first_error = Some(invalid_error("parallel conversion worker panicked"));
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
    // Collect results
    // -------------------------------------------------------------------------

    let mut results = Arc::try_unwrap(results)
        .map_err(|_| invalid_error("parallel conversion results still shared"))?
        .into_inner()
        .map_err(|_| invalid_error("parallel conversion results poisoned"))?;

    results.sort_by_key(|result| result.partition_id);

    if results.len() != partitions.partitions.len() {
        return invalid("parallel conversion result count mismatch");
    }

    // -------------------------------------------------------------------------
    // Aggregate metrics
    // -------------------------------------------------------------------------

    let mut metrics = PhysicalConversionMetrics::default();

    for (expected, result) in partitions.partitions.iter().zip(&results) {
        if result.partition_id != expected.partition_id
            || result.start_row != expected.start_row
            || result.end_row != expected.end_row
        {
            return invalid("parallel conversion result ordering mismatch");
        }

        metrics.events_read += result.metrics.events_read;
        metrics.events_written += result.metrics.events_written;

        metrics.identity_events += result.metrics.identity_events;
        metrics.converted_events += result.metrics.converted_events;
        metrics.unavailable_events += result.metrics.unavailable_events;

        metrics.reference_updates += result.metrics.reference_updates;

        metrics.files_written += result.metrics.files_written;
        metrics.row_groups_written += result.metrics.row_groups_written;
    }

    // -------------------------------------------------------------------------
    // Global accounting
    // -------------------------------------------------------------------------

    if metrics.events_read != manifest.events_written
        || metrics.events_written != manifest.events_written
    {
        return invalid("parallel conversion global event count mismatch");
    }

    if metrics.reference_updates != prepass.reference_updates {
        return invalid("parallel conversion reference update count mismatch");
    }

    let accounted = metrics
        .identity_events
        .checked_add(metrics.converted_events)
        .and_then(|value| value.checked_add(metrics.unavailable_events))
        .ok_or_else(|| invalid_error("parallel conversion status overflow"))?;

    if accounted != metrics.events_written {
        return invalid("parallel conversion status accounting mismatch");
    }

    Ok(ParallelConversionResult {
        partitions: results,
        metrics,
        workers_used,
    })
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
