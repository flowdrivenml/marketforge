use std::{
    collections::VecDeque,
    path::Path,
    sync::{Arc, Mutex},
    thread,
};

use crate::{
    error::{MarketForgeError, Result},
    job::{ConvertJob, MergeParquetConfig},
    process::{
        convert::ConversionPlan,
        merge::{PhysicalMergeManifest, UnifiedMergeMode},
    },
};

use super::{
    CombinedPartitionResult, CombinedReferencePrepass, CombinedReferenceSelection,
    ConversionPartitionPlan, PhysicalCombinedConversionMetrics, execute_combined_partition,
};

// -----------------------------------------------------------------------------
// Parallel execution result
// -----------------------------------------------------------------------------

#[derive(Debug)]
pub struct ParallelCombinedConversionResult {
    pub partitions: Vec<CombinedPartitionResult>,
    pub metrics: PhysicalCombinedConversionMetrics,
    pub workers_used: usize,
}

// -----------------------------------------------------------------------------
// Concurrent combined conversion
// -----------------------------------------------------------------------------

pub fn execute_parallel_combined_conversion(
    job: &ConvertJob,
    source_manifest: &PhysicalMergeManifest,
    reference_manifest: Option<&PhysicalMergeManifest>,
    plan: &ConversionPlan,
    partitions: &ConversionPartitionPlan,
    prepass: &CombinedReferencePrepass,
    selection: &CombinedReferenceSelection,
    staging_directory: &Path,
    requested_workers: usize,
    parquet_config: &MergeParquetConfig,
) -> Result<ParallelCombinedConversionResult> {
    job.validate()?;
    source_manifest.validate()?;

    if let Some(manifest) = reference_manifest {
        manifest.validate()?;
    }

    if source_manifest.mode != UnifiedMergeMode::Combined {
        return invalid("parallel combined conversion requires combined source");
    }

    if requested_workers == 0 {
        return invalid("parallel combined conversion requires positive worker count");
    }

    if partitions.partitions.is_empty() {
        return invalid("parallel combined conversion contains no partitions");
    }

    if partitions.total_rows != source_manifest.events_written {
        return invalid("parallel combined source event count mismatch");
    }

    if prepass.events_scanned != source_manifest.events_written {
        return invalid("parallel combined prepass event count mismatch");
    }

    if prepass.checkpoints.len() != partitions.partitions.len() {
        return invalid("parallel combined checkpoint count mismatch");
    }

    if staging_directory.exists() {
        return invalid(format!(
            "parallel combined staging already exists: {}",
            staging_directory.display()
        ));
    }

    if !selection
        .internal_streams
        .is_disjoint(&selection.external_streams)
    {
        return invalid("parallel combined reference selections overlap");
    }

    // -------------------------------------------------------------------------
    // Validate partition/checkpoint alignment
    // -------------------------------------------------------------------------

    for (partition, checkpoint) in partitions.partitions.iter().zip(&prepass.checkpoints) {
        if partition.partition_id != checkpoint.partition_id
            || partition.start_row != checkpoint.start_row
        {
            return invalid("parallel combined checkpoint/partition mismatch");
        }

        if partition.rows == 0 || partition.end_row - partition.start_row != partition.rows {
            return invalid("invalid parallel combined partition range");
        }
    }

    let workers_used = requested_workers.min(partitions.partitions.len());

    // -------------------------------------------------------------------------
    // Shared work queue
    // -------------------------------------------------------------------------

    let queue = Arc::new(Mutex::new(
        (0..partitions.partitions.len()).collect::<VecDeque<_>>(),
    ));

    let results = Arc::new(Mutex::new(Vec::<CombinedPartitionResult>::new()));

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
                            .map_err(|_| invalid_error("combined worker queue poisoned"))?;

                        queue.pop_front()
                    };

                    let Some(index) = index else {
                        break;
                    };

                    let partition = &partitions.partitions[index];
                    let checkpoint = &prepass.checkpoints[index];

                    let result = execute_combined_partition(
                        job,
                        source_manifest,
                        reference_manifest,
                        plan,
                        partitions,
                        partition,
                        checkpoint,
                        selection,
                        staging_directory,
                        parquet_config,
                    )?;

                    results
                        .lock()
                        .map_err(|_| invalid_error("combined worker results poisoned"))?
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
                        first_error = Some(invalid_error("parallel combined worker panicked"));
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
    // Collect results in deterministic partition order
    // -------------------------------------------------------------------------

    let mut results = Arc::try_unwrap(results)
        .map_err(|_| invalid_error("combined results still shared"))?
        .into_inner()
        .map_err(|_| invalid_error("combined results poisoned"))?;

    results.sort_by_key(|result| result.partition_id);

    if results.len() != partitions.partitions.len() {
        return invalid("parallel combined result count mismatch");
    }

    // -------------------------------------------------------------------------
    // Aggregate worker metrics
    // -------------------------------------------------------------------------

    let mut metrics = PhysicalCombinedConversionMetrics::default();

    for (partition, result) in partitions.partitions.iter().zip(&results) {
        if result.partition_id != partition.partition_id
            || result.start_row != partition.start_row
            || result.end_row != partition.end_row
        {
            return invalid("parallel combined result ordering mismatch");
        }

        let current = &result.metrics;

        macro_rules! accumulate {
            ($field:ident) => {
                metrics.$field = metrics.$field.checked_add(current.$field).ok_or_else(|| {
                    invalid_error(concat!(
                        "parallel combined metric overflow: ",
                        stringify!($field)
                    ))
                })?;
            };
        }

        accumulate!(events_read);
        accumulate!(events_written);

        accumulate!(trade_events);
        accumulate!(depth_events);

        accumulate!(identity_events);
        accumulate!(converted_events);
        accumulate!(unavailable_events);

        accumulate!(depth_levels_read);
        accumulate!(depth_levels_converted);
        accumulate!(depth_levels_unavailable);

        accumulate!(internal_reference_updates);
        accumulate!(external_reference_updates);
        accumulate!(external_observations_read);

        accumulate!(files_written);
        accumulate!(row_groups_written);
    }

    // -------------------------------------------------------------------------
    // Validate global event accounting
    // -------------------------------------------------------------------------

    if metrics.events_read != source_manifest.events_written
        || metrics.events_written != source_manifest.events_written
    {
        return invalid("parallel combined global event count mismatch");
    }

    if metrics.trade_events != source_manifest.trade_events
        || metrics.depth_events != source_manifest.depth_events
    {
        return invalid("parallel combined event-kind count mismatch");
    }

    let accounted = metrics
        .identity_events
        .checked_add(metrics.converted_events)
        .and_then(|value| value.checked_add(metrics.unavailable_events))
        .ok_or_else(|| invalid_error("parallel combined status overflow"))?;

    if accounted != metrics.events_written {
        return invalid("parallel combined status accounting mismatch");
    }

    let accounted_levels = metrics
        .depth_levels_converted
        .checked_add(metrics.depth_levels_unavailable)
        .ok_or_else(|| invalid_error("parallel combined level overflow"))?;

    if accounted_levels != metrics.depth_levels_read {
        return invalid("parallel combined depth-level accounting mismatch");
    }

    if metrics.files_written == 0 || metrics.row_groups_written == 0 {
        return invalid("parallel combined conversion produced no physical output");
    }

    // Internal reference observations belong to the combined timeline.
    // Each selected internal trade must be processed exactly once.
    if metrics.internal_reference_updates != prepass.internal_updates {
        return invalid(format!(
            "parallel combined internal reference count mismatch: \
             workers={}, prepass={}",
            metrics.internal_reference_updates, prepass.internal_updates,
        ));
    }

    Ok(ParallelCombinedConversionResult {
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
