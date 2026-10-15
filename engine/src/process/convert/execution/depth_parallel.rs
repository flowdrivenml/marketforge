use crate::process::convert::progress::{
    ConversionPartitionKey, ConversionProgress, ConversionProgressReporter,
};
use crate::{
    error::{MarketForgeError, Result},
    job::{ConvertJob, MergeParquetConfig},
    process::{
        convert::ConversionPlan,
        merge::{PhysicalMergeManifest, UnifiedMergeMode},
    },
};
use std::{
    collections::VecDeque,
    path::Path,
    sync::{Arc, Mutex},
    thread,
};

use super::execute_depth_partition_with_progress;
use super::{
    ConversionPartitionPlan, DepthPartitionResult, DepthReferencePrepass,
    PhysicalDepthConversionMetrics, execute_depth_partition,
};
use crate::process::convert::progress::ConversionWorkerProgress;
// -----------------------------------------------------------------------------
// Parallel depth conversion result
// -----------------------------------------------------------------------------

#[derive(Debug)]
pub struct ParallelDepthConversionResult {
    pub partitions: Vec<DepthPartitionResult>,
    pub metrics: PhysicalDepthConversionMetrics,
    pub workers_used: usize,
}

// -----------------------------------------------------------------------------
// Execute depth partitions concurrently
// -----------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub fn execute_parallel_depth_conversion(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    reference_manifest: &PhysicalMergeManifest,
    plan: &ConversionPlan,
    partitions: &ConversionPartitionPlan,
    prepass: &DepthReferencePrepass,
    staging_directory: &Path,
    requested_workers: usize,
    parquet_config: &MergeParquetConfig,
) -> Result<ParallelDepthConversionResult> {
    execute_parallel_depth_conversion_with_progress(
        job,
        source_manifest,
        reference_manifest,
        plan,
        partitions,
        prepass,
        staging_directory,
        requested_workers,
        parquet_config,
        &ConversionProgressReporter::disabled(),
    )
}

#[allow(clippy::too_many_arguments)]
pub fn execute_parallel_depth_conversion_with_progress(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    reference_manifest: &PhysicalMergeManifest,
    plan: &ConversionPlan,
    partitions: &ConversionPartitionPlan,
    prepass: &DepthReferencePrepass,
    staging_directory: &Path,
    requested_workers: usize,
    parquet_config: &MergeParquetConfig,
    reporter: &ConversionProgressReporter,
) -> Result<ParallelDepthConversionResult> {
    job.validate()?;
    source_manifest.validate()?;
    reference_manifest.validate()?;

    if source_manifest.mode != UnifiedMergeMode::Depth {
        return invalid("parallel depth conversion requires depth-only source");
    }

    if reference_manifest.mode != UnifiedMergeMode::Trades {
        return invalid("parallel depth conversion requires trade-only references");
    }

    if requested_workers == 0 {
        return invalid("parallel depth conversion requires positive worker count");
    }

    if partitions.partitions.is_empty() {
        return invalid("parallel depth conversion contains no partitions");
    }

    if partitions.total_rows != source_manifest.events_written {
        return invalid("parallel depth conversion source event count mismatch");
    }

    if prepass.checkpoints.len() != partitions.partitions.len() {
        return invalid("parallel depth checkpoint count mismatch");
    }

    if staging_directory.exists() {
        return invalid(format!(
            "parallel depth staging already exists: {}",
            staging_directory.display()
        ));
    }

    // -------------------------------------------------------------------------
    // Validate checkpoint alignment
    // -------------------------------------------------------------------------

    for (partition, checkpoint) in partitions.partitions.iter().zip(&prepass.checkpoints) {
        if partition.partition_id != checkpoint.partition_id
            || partition.start_row != checkpoint.start_row
        {
            return invalid("parallel depth checkpoint/partition alignment mismatch");
        }
    }

    let workers_used = requested_workers.min(partitions.partitions.len());
    reporter.partitions_planned(partitions.partitions.len());
    // -------------------------------------------------------------------------
    // Shared work queue
    // -------------------------------------------------------------------------

    let queue = Arc::new(Mutex::new(
        (0..partitions.partitions.len()).collect::<VecDeque<_>>(),
    ));

    let results = Arc::new(Mutex::new(Vec::<DepthPartitionResult>::new()));

    // -------------------------------------------------------------------------
    // Bounded worker pool
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
                            .map_err(|_| invalid_error("depth worker queue poisoned"))?;

                        queue.pop_front()
                    };

                    let Some(index) = index else {
                        break;
                    };

                    let partition = &partitions.partitions[index];
                    let checkpoint = &prepass.checkpoints[index];

                    let key = ConversionPartitionKey {
                        partition_id: partition.partition_id,
                    };

                    reporter.partition_started(key);

                    let execution = if reporter.is_enabled() {
                        let mut progress = ConversionWorkerProgress::new(reporter, key);

                        execute_depth_partition_with_progress(
                            job,
                            source_manifest,
                            reference_manifest,
                            plan,
                            partitions,
                            partition,
                            checkpoint,
                            staging_directory,
                            parquet_config,
                            &mut progress,
                        )
                    } else {
                        execute_depth_partition(
                            job,
                            source_manifest,
                            reference_manifest,
                            plan,
                            partitions,
                            partition,
                            checkpoint,
                            staging_directory,
                            parquet_config,
                        )
                    };

                    let result = match execution {
                        Ok(result) => {
                            let metrics = &result.metrics;

                            reporter.partition_completed(
                                key,
                                ConversionProgress {
                                    events_read: metrics.events_read,
                                    events_written: metrics.events_written,

                                    identity_events: metrics.identity_events,
                                    converted_events: metrics.converted_events,
                                    unavailable_events: metrics.unavailable_events,

                                    depth_events: metrics.events_read,

                                    depth_levels_read: metrics.levels_read,
                                    depth_levels_converted: metrics.levels_converted,
                                    depth_levels_unavailable: metrics.levels_unavailable,

                                    external_reference_updates: metrics.reference_updates,

                                    files_written: metrics.files_written,
                                    row_groups_written: metrics.row_groups_written,

                                    ..ConversionProgress::default()
                                },
                            );

                            result
                        }

                        Err(error) => {
                            reporter.partition_failed(key, error.to_string());
                            return Err(error);
                        }
                    };

                    results
                        .lock()
                        .map_err(|_| invalid_error("depth worker results poisoned"))?
                        .push(result);
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
                        first_error = Some(invalid_error("parallel depth worker panicked"));
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
    // Collect deterministic partition results
    // -------------------------------------------------------------------------

    let mut results = Arc::try_unwrap(results)
        .map_err(|_| invalid_error("depth results still shared"))?
        .into_inner()
        .map_err(|_| invalid_error("depth results poisoned"))?;

    results.sort_by_key(|result| result.partition_id);

    if results.len() != partitions.partitions.len() {
        return invalid("parallel depth result count mismatch");
    }

    // -------------------------------------------------------------------------
    // Aggregate worker metrics
    // -------------------------------------------------------------------------

    let mut metrics = PhysicalDepthConversionMetrics::default();

    for (partition, result) in partitions.partitions.iter().zip(&results) {
        if result.partition_id != partition.partition_id
            || result.start_row != partition.start_row
            || result.end_row != partition.end_row
        {
            return invalid("parallel depth result ordering mismatch");
        }

        let current = &result.metrics;

        macro_rules! accumulate {
            ($field:ident) => {
                metrics.$field = metrics.$field.checked_add(current.$field).ok_or_else(|| {
                    invalid_error(concat!(
                        "parallel depth metric overflow: ",
                        stringify!($field)
                    ))
                })?;
            };
        }

        accumulate!(events_read);
        accumulate!(events_written);

        accumulate!(identity_events);
        accumulate!(converted_events);
        accumulate!(unavailable_events);

        accumulate!(levels_read);
        accumulate!(levels_converted);
        accumulate!(levels_unavailable);

        accumulate!(reference_updates);
        accumulate!(reference_observations_read);

        accumulate!(files_written);
        accumulate!(row_groups_written);
    }

    // -------------------------------------------------------------------------
    // Global event accounting
    // -------------------------------------------------------------------------

    if metrics.events_read != source_manifest.events_written
        || metrics.events_written != source_manifest.events_written
    {
        return invalid("parallel depth global event count mismatch");
    }

    let accounted = metrics
        .identity_events
        .checked_add(metrics.converted_events)
        .and_then(|value| value.checked_add(metrics.unavailable_events))
        .ok_or_else(|| invalid_error("parallel depth status overflow"))?;

    if accounted != metrics.events_written {
        return invalid("parallel depth status accounting mismatch");
    }

    // -------------------------------------------------------------------------
    // Global level accounting
    // -------------------------------------------------------------------------

    let accounted_levels = metrics
        .levels_converted
        .checked_add(metrics.levels_unavailable)
        .ok_or_else(|| invalid_error("parallel depth level overflow"))?;

    if accounted_levels != metrics.levels_read {
        return invalid("parallel depth level accounting mismatch");
    }

    if metrics.files_written == 0 || metrics.row_groups_written == 0 {
        return invalid("parallel depth conversion produced no physical output");
    }

    Ok(ParallelDepthConversionResult {
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
