use std::{collections::HashSet, fs, path::Path};

use crate::{
    historical::job::MergeParquetConfig,
    shared::error::{MarketForgeError, Result},
};

use {
    crate::historical::merge::core::kway::KWayDepthMerger,
    crate::historical::merge::core::source::DepthEventSource,
    crate::historical::merge::depth::boundary::DepthBoundaryLink,
    crate::historical::merge::depth::inventory::DepthTaskInventory,
    crate::historical::merge::depth::partition_stream::PartitionDepthStream,
    crate::historical::merge::output::manifests::manifest::MergeSkippedInitialization,
    crate::historical::merge::output::manifests::manifest::MergeTimelineFile,
    crate::historical::merge::output::writers::timeline::TimelineRecord,
    crate::historical::merge::output::writers::writer::TimelineWriter,
    crate::historical::merge::partition::shared::partition_input::PartitionInputPlan,
};

// -----------------------------------------------------------------------------
// Partition execution result
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct MergePartitionResult {
    pub partition_id: u32,

    pub start_ns: i64,
    pub end_ns: i64,

    pub events_written: u64,
    pub files_written: u64,

    pub first_timestamp_ns: Option<i64>,
    pub last_timestamp_ns: Option<i64>,

    pub files: Vec<MergeTimelineFile>,
    pub skipped_initializations: Vec<MergeSkippedInitialization>,
    pub timestamp_regressions_dropped: u64,
}

// -----------------------------------------------------------------------------
// Execute one merge partition
// -----------------------------------------------------------------------------

pub fn execute_merge_partition(
    plan: &PartitionInputPlan,
    inventory: &[DepthTaskInventory],
    boundaries: &[DepthBoundaryLink],
    dataset_order: &[u64],
    parquet: &MergeParquetConfig,
    staging: &Path,
) -> Result<MergePartitionResult> {
    let partition = plan.partition;

    // -------------------------------------------------------------------------
    // Resolve partition input tasks
    // -------------------------------------------------------------------------

    let selected: HashSet<(u64, u64)> = plan
        .inputs
        .iter()
        .map(|input| (input.dataset_id, input.task_id))
        .collect();

    let mut streams: Vec<Box<dyn DepthEventSource>> = Vec::new();

    for &dataset_id in dataset_order {
        let tasks = inventory
            .iter()
            .filter(|task| {
                task.dataset_id == dataset_id && selected.contains(&(task.dataset_id, task.task_id))
            })
            .cloned()
            .collect::<Vec<_>>();

        streams.push(Box::new(PartitionDepthStream::new(
            dataset_id, partition, tasks, boundaries,
        )?));
    }

    // -------------------------------------------------------------------------
    // Initialize K-way merger
    // -------------------------------------------------------------------------

    let mut merger = KWayDepthMerger::new(streams)?;

    // -------------------------------------------------------------------------
    // Partition output directory
    // -------------------------------------------------------------------------

    let relative_directory = format!("timeline/partition-{:06}", partition.partition_id);

    let output_directory = staging.join(&relative_directory);

    let mut writer = TimelineWriter::new(&output_directory, parquet)?;

    // -------------------------------------------------------------------------
    // Merge and persist events
    // -------------------------------------------------------------------------

    while let Some(merged) = merger.next_event()? {
        let timestamp = merged.event.event.event_timestamp_ns;

        if !partition.contains(timestamp) {
            return invalid(format!(
                "partition {} emitted timestamp {} outside [{}, {})",
                partition.partition_id, timestamp, partition.start_ns, partition.end_ns,
            ));
        }

        writer.write(TimelineRecord::from(&merged))?;
    }

    // -------------------------------------------------------------------------
    // Finalize writer
    // -------------------------------------------------------------------------

    writer.finish()?;

    if writer.is_failed() || !writer.is_finished() {
        return invalid(format!(
            "partition {} timeline writer did not finalize",
            partition.partition_id,
        ));
    }

    let metrics = writer.metrics().clone();

    // -------------------------------------------------------------------------
    // Inspect output files
    // -------------------------------------------------------------------------

    let mut files = Vec::new();

    for entry in fs::read_dir(&output_directory).map_err(|error| {
        invalid_error(format!(
            "failed to inspect partition output {}: {error}",
            output_directory.display()
        ))
    })? {
        let entry = entry.map_err(|error| {
            invalid_error(format!("failed to inspect partition output entry: {error}"))
        })?;

        let path = entry.path();

        if path.extension().and_then(|ext| ext.to_str()) != Some("parquet") {
            continue;
        }

        let metadata = fs::metadata(&path).map_err(|error| {
            invalid_error(format!("failed to inspect partition Parquet file: {error}"))
        })?;

        let filename = path
            .file_name()
            .ok_or_else(|| invalid_error("invalid partition filename"))?
            .to_string_lossy();

        files.push(MergeTimelineFile {
            path: format!("{relative_directory}/{filename}"),
            size_bytes: metadata.len(),

            partition_id: Some(partition.partition_id),

            rows: None,
            start_timestamp_ns: None,
            end_timestamp_ns: None,
        });
    }

    files.sort_by(|a, b| a.path.cmp(&b.path));

    if files.len() as u64 != metrics.files_written {
        return invalid(format!(
            "partition {} file count mismatch: writer={}, discovered={}",
            partition.partition_id,
            metrics.files_written,
            files.len(),
        ));
    }

    // -------------------------------------------------------------------------
    // Collect redundant initialization references
    // -------------------------------------------------------------------------

    let skipped_initializations = merger.skipped_initializations();

    let timestamp_regressions_dropped = merger.timestamp_regressions_dropped()?;

    Ok(MergePartitionResult {
        partition_id: partition.partition_id,

        start_ns: partition.start_ns,
        end_ns: partition.end_ns,

        events_written: metrics.events_written,
        files_written: metrics.files_written,

        first_timestamp_ns: metrics.start_timestamp_ns,
        last_timestamp_ns: metrics.end_timestamp_ns,

        files,
        skipped_initializations,
        timestamp_regressions_dropped,
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
