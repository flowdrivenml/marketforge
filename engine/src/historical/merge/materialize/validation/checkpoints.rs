use crate::historical::job::MergeTimeRange;
use arrow_array::{Array, Int64Array, UInt8Array, UInt64Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs::File, path::Path};

use crate::shared::error::{MarketForgeError, Result};
use std::collections::HashSet;
use {
    crate::historical::merge::depth::checkpoint::MergeCheckpoint,
    crate::historical::merge::materialize::validation::global_validate::ValidatedPhysicalDataset,
    crate::historical::merge::output::manifests::manifest::MergeSkippedInitialization,
    crate::historical::merge::output::manifests::unified_manifest::UnifiedMergeMode,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhysicalMergeCheckpoint {
    pub dataset_id: u64,
    pub task_id: u64,
    pub segment_id: u64,
    pub timestamp_ns: i64,

    pub partition_id: u32,
    pub file_index: u32,
    pub row_index: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct CheckpointKey {
    dataset_id: u64,
    task_id: u64,
    timestamp_ns: i64,
    canonical_row_offset: u64,
    canonical_row_count: u64,
}

fn checkpoint_key(checkpoint: &MergeCheckpoint) -> CheckpointKey {
    CheckpointKey {
        dataset_id: checkpoint.dataset_id,
        task_id: checkpoint.task_id,
        timestamp_ns: checkpoint.timestamp_ns,
        canonical_row_offset: checkpoint.canonical_row_offset,
        canonical_row_count: checkpoint.canonical_row_count,
    }
}
fn skipped_checkpoint_key(initialization: &MergeSkippedInitialization) -> CheckpointKey {
    CheckpointKey {
        dataset_id: initialization.dataset_id,
        task_id: initialization.task_id,
        timestamp_ns: initialization.timestamp_ns,
        canonical_row_offset: initialization.canonical_row_offset,
        canonical_row_count: initialization.canonical_row_count,
    }
}

pub fn build_physical_checkpoints(
    staging: &Path,
    mode: UnifiedMergeMode,
    validated: &ValidatedPhysicalDataset,
    canonical_checkpoints: &[MergeCheckpoint],
    skipped_initializations: &[MergeSkippedInitialization],
    time_range: MergeTimeRange,
) -> Result<Vec<PhysicalMergeCheckpoint>> {
    if time_range.start_ns >= time_range.end_ns {
        return invalid("invalid physical checkpoint time range");
    }
    let mut checkpoints: Vec<PhysicalMergeCheckpoint> = Vec::new();

    let mut lookup = HashMap::<CheckpointKey, &MergeCheckpoint>::new();
    let mut seen_segments = HashSet::new();

    for checkpoint in canonical_checkpoints {
        let key = checkpoint_key(checkpoint);

        if lookup.insert(key, checkpoint).is_some() {
            return invalid("duplicate canonical reconstruction checkpoint");
        }

        if !seen_segments.insert((
            checkpoint.dataset_id,
            checkpoint.task_id,
            checkpoint.segment_id,
        )) {
            return invalid("duplicate canonical reconstruction segment ID");
        }

        if checkpoint.canonical_row_count == 0 {
            return invalid("canonical initialization checkpoint cannot be empty");
        }

        checkpoint
            .canonical_row_offset
            .checked_add(checkpoint.canonical_row_count)
            .ok_or_else(|| invalid_error("canonical checkpoint row-range overflow"))?;
    }

    let mut matched = HashSet::<CheckpointKey>::new();

    // -----------------------------------------------------------------------------
    // Explicitly skipped initialization events
    // -----------------------------------------------------------------------------

    // -----------------------------------------------------------------------------
    // Validate explicitly skipped initialization events
    // -----------------------------------------------------------------------------

    let mut skipped = HashSet::<CheckpointKey>::new();
    let mut skipped_ordinals = HashSet::new();

    for initialization in skipped_initializations {
        if initialization.timestamp_ns < time_range.start_ns
            || initialization.timestamp_ns >= time_range.end_ns
        {
            return invalid(format!(
                "skipped initialization outside merge interval: \
         dataset={}, task={}, ordinal={}",
                initialization.dataset_id, initialization.task_id, initialization.event_ordinal,
            ));
        }
        let key = skipped_checkpoint_key(initialization);

        if !lookup.contains_key(&key) {
            return invalid(format!(
                "skipped initialization has no canonical checkpoint: \
             dataset={}, task={}, ordinal={}",
                initialization.dataset_id, initialization.task_id, initialization.event_ordinal,
            ));
        }

        if !skipped.insert(key) {
            return invalid("duplicate skipped initialization checkpoint");
        }

        if !skipped_ordinals.insert((
            initialization.dataset_id,
            initialization.task_id,
            initialization.event_ordinal,
        )) {
            return invalid("duplicate skipped initialization ordinal");
        }
    }
    if mode == UnifiedMergeMode::Trades {
        if !canonical_checkpoints.is_empty() || !skipped_initializations.is_empty() {
            return invalid("trade-only materialization contains depth checkpoints");
        }

        return Ok(Vec::new());
    }
    if mode == UnifiedMergeMode::Trades {
        return Ok(Vec::new());
    }

    for file in &validated.files {
        let relative = Path::new(&file.path);

        if relative.is_absolute()
            || relative
                .components()
                .any(|component| !matches!(component, std::path::Component::Normal(_)))
        {
            return invalid("unsafe physical checkpoint file path");
        }

        let partition_id = file
            .partition_id
            .ok_or_else(|| invalid_error("checkpoint file missing partition ID"))?;

        let filename = relative
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| invalid_error("invalid checkpoint filename"))?;

        let file_index = filename
            .strip_prefix("part-")
            .and_then(|name| name.strip_suffix(".parquet"))
            .and_then(|name| name.parse::<u32>().ok())
            .ok_or_else(|| invalid_error("invalid checkpoint file index"))?;

        let path = staging.join(relative);

        let file = File::open(&path).map_err(|error| {
            invalid_error(format!(
                "cannot open checkpoint source {}: {error}",
                path.display()
            ))
        })?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|error| invalid_error(format!("cannot inspect checkpoint source: {error}")))?;

        let reader = builder.build().map_err(|error| {
            invalid_error(format!("cannot initialize checkpoint reader: {error}"))
        })?;

        let mut row_index = 0_u64;

        for batch in reader {
            let batch = batch.map_err(|error| {
                invalid_error(format!("checkpoint Parquet decoding failed: {error}"))
            })?;

            let dataset_ids = batch
                .column_by_name("dataset_id")
                .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| invalid_error("missing checkpoint dataset ID"))?;

            let task_ids = batch
                .column_by_name("task_id")
                .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| invalid_error("missing checkpoint task ID"))?;

            let ordinals = batch
                .column_by_name("event_ordinal")
                .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| invalid_error("missing checkpoint event ordinal"))?;

            // -----------------------------------------------------------------------------
            // Canonical provenance
            // -----------------------------------------------------------------------------

            let canonical_offsets = batch
                .column_by_name("canonical_row_offset")
                .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| invalid_error("missing physical canonical row offset"))?;

            let canonical_counts = batch
                .column_by_name("canonical_row_count")
                .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
                .ok_or_else(|| invalid_error("missing physical canonical row count"))?;

            let timestamps = batch
                .column_by_name("event_timestamp_ns")
                .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
                .ok_or_else(|| invalid_error("missing checkpoint timestamp"))?;

            let kinds = if mode == UnifiedMergeMode::Combined {
                Some(
                    batch
                        .column_by_name("event_kind")
                        .and_then(|column| column.as_any().downcast_ref::<UInt8Array>())
                        .ok_or_else(|| invalid_error("missing checkpoint event kind"))?,
                )
            } else {
                None
            };

            let boundaries = match mode {
                UnifiedMergeMode::Depth => batch
                    .column_by_name("event_boundary")
                    .and_then(|column| column.as_any().downcast_ref::<UInt8Array>())
                    .ok_or_else(|| invalid_error("missing depth boundary"))?,

                UnifiedMergeMode::Combined => {
                    let depth = batch
                        .column_by_name("depth")
                        .and_then(|column| {
                            column.as_any().downcast_ref::<arrow_array::StructArray>()
                        })
                        .ok_or_else(|| invalid_error("missing combined depth payload"))?;

                    depth
                        .column_by_name("event_boundary")
                        .and_then(|column| column.as_any().downcast_ref::<UInt8Array>())
                        .ok_or_else(|| invalid_error("missing combined depth boundary"))?
                }

                UnifiedMergeMode::Trades => unreachable!(),
            };

            for row in 0..batch.num_rows() {
                let is_depth = kinds.is_none_or(|array| array.value(row) == 1);

                if is_depth && boundaries.value(row) == 1 {
                    let key = CheckpointKey {
                        dataset_id: dataset_ids.value(row),
                        task_id: task_ids.value(row),
                        timestamp_ns: timestamps.value(row),
                        canonical_row_offset: canonical_offsets.value(row),
                        canonical_row_count: canonical_counts.value(row),
                    };

                    let checkpoint = lookup.get(&key).ok_or_else(|| {
                        invalid_error(format!(
                            "physical initialization has no canonical checkpoint: \
             dataset={}, task={}, timestamp={}, ordinal={}",
                            key.dataset_id,
                            key.task_id,
                            key.timestamp_ns,
                            ordinals.value(row),
                        ))
                    })?;

                    if skipped.contains(&key) {
                        return invalid(format!(
                            "skipped initialization unexpectedly materialized: \
         dataset={}, task={}, segment={}",
                            checkpoint.dataset_id, checkpoint.task_id, checkpoint.segment_id,
                        ));
                    }

                    if !matched.insert(key) {
                        return invalid("duplicate materialized initialization checkpoint");
                    }

                    checkpoints.push(PhysicalMergeCheckpoint {
                        dataset_id: checkpoint.dataset_id,
                        task_id: checkpoint.task_id,
                        segment_id: checkpoint.segment_id,
                        timestamp_ns: checkpoint.timestamp_ns,

                        partition_id,
                        file_index,
                        row_index,
                    });
                }

                row_index = row_index
                    .checked_add(1)
                    .ok_or_else(|| invalid_error("checkpoint row offset overflow"))?;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Deterministic checkpoint ordering
    // -------------------------------------------------------------------------
    // -----------------------------------------------------------------------------
    // Reconcile canonical checkpoints not found in physical output
    // -----------------------------------------------------------------------------

    for checkpoint in canonical_checkpoints {
        // Checkpoints outside the requested interval are not
        // expected to appear in the materialized timeline.
        if checkpoint.timestamp_ns < time_range.start_ns
            || checkpoint.timestamp_ns >= time_range.end_ns
        {
            continue;
        }

        let key = checkpoint_key(checkpoint);

        if matched.contains(&key) {
            continue;
        }

        if !skipped.contains(&key) {
            return invalid(format!(
                "canonical checkpoint missing from physical output: \
             dataset={}, task={}, segment={}, timestamp={}",
                checkpoint.dataset_id,
                checkpoint.task_id,
                checkpoint.segment_id,
                checkpoint.timestamp_ns,
            ));
        }
    }

    Ok(checkpoints)
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
