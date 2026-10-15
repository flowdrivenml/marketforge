use super::{
    checkpoints::{ConversionCheckpointBuilder, ConversionReferenceCheckpoint},
    partition::ConversionPartitionPlan,
    reader::{CONVERSION_READ_BATCH_SIZE, PhysicalConversionReader},
};
use crate::job::ConvertJob;
use crate::{
    error::{MarketForgeError, Result},
    process::{
        convert::{
            ConversionDecimal, ConversionPlan,
            reference::{ReferenceKey, ReferenceObservation},
        },
        merge::PhysicalMergeManifest,
    },
};
use arrow_array::{Array, Decimal256Array, UInt32Array, UInt64Array};
use std::collections::{HashMap, HashSet};
use std::path::Path;

// -----------------------------------------------------------------------------
// Reference prepass result
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ConversionPrepassResult {
    pub checkpoints: Vec<ConversionReferenceCheckpoint>,
    pub events_scanned: u64,
    pub reference_updates: u64,
}

// -----------------------------------------------------------------------------
// Build partition reference checkpoints
// -----------------------------------------------------------------------------

pub fn build_conversion_checkpoints(
    job: &ConvertJob,
    dataset_path: &Path,
    manifest: &PhysicalMergeManifest,
    plan: &ConversionPlan,
    partitions: &ConversionPartitionPlan,
) -> Result<ConversionPrepassResult> {
    manifest.validate()?;

    if partitions.total_rows != manifest.events_written {
        return invalid("conversion partition/source event count mismatch");
    }

    if partitions.partitions.is_empty() {
        return invalid("conversion partition plan is empty");
    }

    let streams = plan
        .streams
        .iter()
        .map(|stream| (stream.source.stream_rank, stream))
        .collect::<HashMap<_, _>>();

    let reference_streams = plan
        .streams
        .iter()
        .filter(|stream| job.conversion.references.contains(&stream.source.identity))
        .map(|stream| stream.source.stream_rank)
        .collect::<HashSet<_>>();

    let mut builder = ConversionCheckpointBuilder::new(reference_streams.clone());

    let mut reader =
        PhysicalConversionReader::new(dataset_path, manifest, CONVERSION_READ_BATCH_SIZE)?;

    let mut checkpoints = Vec::with_capacity(partitions.partitions.len());

    let mut next_partition = 0_usize;
    let mut global_row = 0_u64;
    let mut reference_updates = 0_u64;

    while let Some(batch) = reader.next_batch()? {
        let timestamps = column::<arrow_array::Int64Array>(&batch, "event_timestamp_ns")?;

        let ranks = column::<UInt32Array>(&batch, "stream_rank")?;
        let datasets = column::<UInt64Array>(&batch, "dataset_id")?;
        let tasks = column::<UInt64Array>(&batch, "task_id")?;
        let ordinals = column::<UInt64Array>(&batch, "event_ordinal")?;
        let prices = column::<Decimal256Array>(&batch, "price")?;

        for row in 0..batch.num_rows() {
            // -------------------------------------------------------------
            // Capture checkpoint BEFORE observing boundary event.
            // -------------------------------------------------------------

            if next_partition < partitions.partitions.len()
                && global_row == partitions.partitions[next_partition].start_row
            {
                let partition = &partitions.partitions[next_partition];

                checkpoints.push(builder.checkpoint(partition.partition_id));

                next_partition += 1;
            }

            // -------------------------------------------------------------
            // Validate stream identity
            // -------------------------------------------------------------

            let rank = ranks.value(row);
            let dataset_id = datasets.value(row);

            let stream = streams
                .get(&rank)
                .ok_or_else(|| invalid_error(format!("unknown conversion stream rank {rank}")))?;

            if stream.source.dataset_id != dataset_id {
                return invalid("conversion prepass dataset identity mismatch");
            }

            // -------------------------------------------------------------
            // Observe reference event
            // -------------------------------------------------------------

            let key = ReferenceKey::new(
                timestamps.value(row),
                rank,
                tasks.value(row),
                ordinals.value(row),
            );

            let price = ConversionDecimal::new(prices.value(row))?;

            builder.observe(ReferenceObservation {
                dataset_id,
                stream_id: stream.source.stream_id.clone(),
                key,
                price,
            })?;

            if reference_streams.contains(&rank) {
                reference_updates += 1;
            }

            global_row += 1;
        }
    }

    if global_row != manifest.events_written {
        return invalid("conversion prepass event count mismatch");
    }

    if checkpoints.len() != partitions.partitions.len() {
        return invalid("conversion checkpoint count mismatch");
    }

    for (checkpoint, partition) in checkpoints.iter().zip(&partitions.partitions) {
        if checkpoint.partition_id != partition.partition_id
            || checkpoint.events_before_partition != partition.start_row
        {
            return invalid("conversion checkpoint boundary mismatch");
        }
    }

    Ok(ConversionPrepassResult {
        checkpoints,
        events_scanned: global_row,
        reference_updates,
    })
}

// -----------------------------------------------------------------------------
// Arrow column access
// -----------------------------------------------------------------------------

fn column<'a, T: Array + 'static>(
    batch: &'a arrow_array::RecordBatch,
    name: &str,
) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<T>())
        .ok_or_else(|| invalid_error(format!("missing or invalid prepass column: {name}")))
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
