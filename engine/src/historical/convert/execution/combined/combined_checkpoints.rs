use crate::historical::convert::progress::ConversionProgressReporter;
use arrow_array::{
    Array, Decimal256Array, Int64Array, RecordBatch, StringArray, StructArray, UInt8Array,
    UInt32Array, UInt64Array,
};
use std::path::Path;
use std::time::{Duration, Instant};

use crate::{
    error::{MarketForgeError, Result},
    process::{
        convert::{
            ConversionDecimal,
            reference::{
                ExternalReferenceCursor, ExternalReferencePosition, ReferenceKey,
                ReferenceObservation, ReferenceState,
            },
        },
        merge::{PhysicalMergeManifest, UnifiedMergeMode},
    },
};

use super::{
    CONVERSION_READ_BATCH_SIZE, CombinedReferenceAlignment, CombinedReferenceSelection,
    ConversionPartitionPlan, PhysicalConversionReader,
};

#[derive(Debug, Clone)]
pub struct CombinedReferenceCheckpoint {
    pub partition_id: u32,
    pub start_row: u64,
    pub start_timestamp_ns: i64,

    pub state: ReferenceState,
    pub external_position: Option<ExternalReferencePosition>,
}

#[derive(Debug, Clone)]
pub struct CombinedReferencePrepass {
    pub checkpoints: Vec<CombinedReferenceCheckpoint>,
    pub events_scanned: u64,
    pub internal_updates: u64,
    pub external_updates: u64,
    pub observations_read: u64,
}

pub fn build_combined_reference_checkpoints(
    source_directory: &Path,
    source_manifest: &PhysicalMergeManifest,
    reference_directory: Option<&Path>,
    reference_manifest: Option<&PhysicalMergeManifest>,
    selection: &CombinedReferenceSelection,
    partitions: &ConversionPartitionPlan,
) -> Result<CombinedReferencePrepass> {
    build_combined_reference_checkpoints_with_progress(
        source_directory,
        source_manifest,
        reference_directory,
        reference_manifest,
        selection,
        partitions,
        &ConversionProgressReporter::disabled(),
    )
}

pub fn build_combined_reference_checkpoints_with_progress(
    source_directory: &Path,
    source_manifest: &PhysicalMergeManifest,
    reference_directory: Option<&Path>,
    reference_manifest: Option<&PhysicalMergeManifest>,
    selection: &CombinedReferenceSelection,
    partitions: &ConversionPartitionPlan,
    reporter: &ConversionProgressReporter,
) -> Result<CombinedReferencePrepass> {
    build_combined_reference_checkpoints_impl(
        source_directory,
        source_manifest,
        reference_directory,
        reference_manifest,
        selection,
        partitions,
        reporter,
        true,
    )
}
#[doc(hidden)]
pub fn build_combined_reference_checkpoints_full_reader(
    source_directory: &Path,
    source_manifest: &PhysicalMergeManifest,
    reference_directory: Option<&Path>,
    reference_manifest: Option<&PhysicalMergeManifest>,
    selection: &CombinedReferenceSelection,
    partitions: &ConversionPartitionPlan,
) -> Result<CombinedReferencePrepass> {
    build_combined_reference_checkpoints_impl(
        source_directory,
        source_manifest,
        reference_directory,
        reference_manifest,
        selection,
        partitions,
        &ConversionProgressReporter::disabled(),
        false,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_combined_reference_checkpoints_impl(
    source_directory: &Path,
    source_manifest: &PhysicalMergeManifest,
    reference_directory: Option<&Path>,
    reference_manifest: Option<&PhysicalMergeManifest>,
    selection: &CombinedReferenceSelection,
    partitions: &ConversionPartitionPlan,
    reporter: &ConversionProgressReporter,
    projected: bool,
) -> Result<CombinedReferencePrepass> {
    source_manifest.validate()?;

    if source_manifest.mode != UnifiedMergeMode::Combined {
        return invalid("combined checkpoint prepass requires combined source");
    }

    if partitions.partitions.is_empty() {
        return invalid("combined checkpoint partition plan is empty");
    }

    if partitions.total_rows != source_manifest.events_written {
        return invalid("combined checkpoint source row count mismatch");
    }

    if !selection
        .internal_streams
        .is_disjoint(&selection.external_streams)
    {
        return invalid("combined checkpoint reference selections overlap");
    }

    let external = if selection.external_streams.is_empty() {
        None
    } else {
        let directory = reference_directory
            .ok_or_else(|| invalid_error("missing external reference directory"))?;

        let manifest = reference_manifest
            .ok_or_else(|| invalid_error("missing external reference manifest"))?;

        Some(ExternalReferenceCursor::new(
            directory,
            manifest,
            selection.external_streams.iter().cloned(),
        )?)
    };

    let alignment = CombinedReferenceAlignment::new(selection.clone(), external)?;

    let mut accumulator = CombinedCheckpointAccumulator::new(alignment);

    let mut reader = if projected {
        PhysicalConversionReader::new_reference_prepass(
            source_directory,
            source_manifest,
            CONVERSION_READ_BATCH_SIZE,
        )?
    } else {
        PhysicalConversionReader::new(
            source_directory,
            source_manifest,
            CONVERSION_READ_BATCH_SIZE,
        )?
    };

    let mut checkpoints = Vec::with_capacity(partitions.partitions.len());

    let mut next_partition = 0_usize;
    let mut global_row = 0_u64;

    let total_events = source_manifest.events_written;
    let total_checkpoints = partitions.partitions.len();

    let mut last_progress = Instant::now();

    reporter.reference_prepass_progress(0, total_events, 0, total_checkpoints);

    while let Some(batch) = reader.next_batch()? {
        let timestamps = column::<Int64Array>(&batch, "event_timestamp_ns")?;
        let kinds = column::<UInt8Array>(&batch, "event_kind")?;
        let streams = column::<StringArray>(&batch, "stream_id")?;
        let ranks = column::<UInt32Array>(&batch, "stream_rank")?;
        let datasets = column::<UInt64Array>(&batch, "dataset_id")?;
        let tasks = column::<UInt64Array>(&batch, "task_id")?;
        let ordinals = column::<UInt64Array>(&batch, "event_ordinal")?;
        let trades = column::<StructArray>(&batch, "trade")?;

        let prices = trades
            .column_by_name("price")
            .and_then(|array| array.as_any().downcast_ref::<Decimal256Array>())
            .ok_or_else(|| invalid_error("missing combined trade prices"))?;

        for row in 0..batch.num_rows() {
            if timestamps.is_null(row) || kinds.is_null(row) {
                return invalid("null combined checkpoint event identity");
            }

            let timestamp_ns = timestamps.value(row);

            // External observations must be incorporated strictly before
            // the current combined event timestamp.
            accumulator.advance_before(timestamp_ns)?;

            // Capture checkpoint before processing the boundary event.
            if next_partition < partitions.partitions.len()
                && global_row == partitions.partitions[next_partition].start_row
            {
                let partition = &partitions.partitions[next_partition];

                checkpoints.push(accumulator.checkpoint(partition.partition_id, timestamp_ns));

                next_partition += 1;
            }

            match kinds.value(row) {
                0 => {
                    let stream_id = streams.value(row);

                    if selection.internal_streams.contains(stream_id) {
                        if trades.is_null(row) || prices.is_null(row) {
                            return invalid("invalid internal reference trade payload");
                        }

                        accumulator.observe_internal(ReferenceObservation {
                            dataset_id: datasets.value(row),
                            stream_id: stream_id.to_owned(),
                            key: ReferenceKey::new(
                                timestamp_ns,
                                ranks.value(row),
                                tasks.value(row),
                                ordinals.value(row),
                            ),
                            price: ConversionDecimal::new(prices.value(row))?,
                        })?;
                    }
                }

                1 => {}

                kind => {
                    return invalid(format!("invalid combined checkpoint event kind: {kind}"));
                }
            }

            accumulator.finish_event()?;

            global_row = accumulator.events_seen();
        }

        if reporter.is_enabled() && last_progress.elapsed() >= Duration::from_millis(500) {
            reporter.reference_prepass_progress(
                global_row,
                total_events,
                checkpoints.len(),
                total_checkpoints,
            );

            last_progress = Instant::now();
        }
    }

    if global_row != source_manifest.events_written {
        return invalid("combined checkpoint event count mismatch");
    }

    if checkpoints.len() != partitions.partitions.len() {
        return invalid("combined checkpoint count mismatch");
    }

    for (checkpoint, partition) in checkpoints.iter().zip(&partitions.partitions) {
        if checkpoint.partition_id != partition.partition_id
            || checkpoint.start_row != partition.start_row
        {
            return invalid("combined checkpoint boundary mismatch");
        }
    }

    reporter.reference_prepass_progress(
        global_row,
        total_events,
        checkpoints.len(),
        total_checkpoints,
    );

    Ok(CombinedReferencePrepass {
        checkpoints,
        events_scanned: global_row,
        internal_updates: accumulator.internal_updates(),
        external_updates: accumulator.external_updates(),
        observations_read: accumulator.observations_read(),
    })
}

fn column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<T>())
        .ok_or_else(|| {
            invalid_error(format!(
                "missing or invalid combined checkpoint column: {name}"
            ))
        })
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}

pub struct CombinedCheckpointAccumulator {
    alignment: CombinedReferenceAlignment,
    events_seen: u64,
}

impl CombinedCheckpointAccumulator {
    pub fn new(alignment: CombinedReferenceAlignment) -> Self {
        Self {
            alignment,
            events_seen: 0,
        }
    }

    pub fn advance_before(&mut self, timestamp_ns: i64) -> Result<()> {
        self.alignment.advance_before(timestamp_ns)
    }

    pub fn checkpoint(&self, partition_id: u32, timestamp_ns: i64) -> CombinedReferenceCheckpoint {
        CombinedReferenceCheckpoint {
            partition_id,
            start_row: self.events_seen,
            start_timestamp_ns: timestamp_ns,
            state: self.alignment.snapshot(),
            external_position: self.alignment.external_position(),
        }
    }

    pub fn observe_internal(&mut self, observation: ReferenceObservation) -> Result<()> {
        self.alignment.observe_internal(observation)
    }

    pub fn finish_event(&mut self) -> Result<()> {
        self.events_seen = self
            .events_seen
            .checked_add(1)
            .ok_or_else(|| invalid_error("combined checkpoint event counter overflow"))?;

        Ok(())
    }

    pub fn events_seen(&self) -> u64 {
        self.events_seen
    }

    pub fn state(&self) -> &ReferenceState {
        self.alignment.state()
    }

    pub fn external_position(&self) -> Option<ExternalReferencePosition> {
        self.alignment.external_position()
    }
    pub fn internal_updates(&self) -> u64 {
        self.alignment.internal_updates()
    }

    pub fn external_updates(&self) -> u64 {
        self.alignment.external_updates()
    }

    pub fn observations_read(&self) -> u64 {
        self.alignment.observations_read()
    }
}
