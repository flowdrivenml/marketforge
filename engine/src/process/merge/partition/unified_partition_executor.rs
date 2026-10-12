use std::{collections::HashSet, fs, path::Path};

use crate::{
    error::{MarketForgeError, Result},
    job::MergeParquetConfig,
};

use super::{
    boundary::DepthBoundaryLink,
    event::MergeEventKind,
    event_source::MergeEventSource,
    inventory::DepthTaskInventory,
    kway_event::KWayEventMerger,
    manifest::{MergeSkippedInitialization, MergeTimelineFile},
    partition_stream::PartitionDepthStream,
    trade_inventory::TradeTaskInventory,
    trade_stream::TradeStream,
    unified_partition_input::UnifiedPartitionInputPlan,
    unified_timeline::UnifiedTimelineRecord,
    unified_writer::UnifiedTimelineWriter,
};

// -----------------------------------------------------------------------------
// Unified partition result
// -----------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct UnifiedPartitionResult {
    pub partition_id: u32,

    pub start_ns: i64,
    pub end_ns: i64,

    pub events_written: u64,
    pub trade_events: u64,
    pub depth_events: u64,

    pub files_written: u64,

    pub first_timestamp_ns: Option<i64>,
    pub last_timestamp_ns: Option<i64>,

    pub files: Vec<MergeTimelineFile>,
    pub skipped_initializations: Vec<MergeSkippedInitialization>,
}

// -----------------------------------------------------------------------------
// Execute one unified partition
// -----------------------------------------------------------------------------

pub fn execute_unified_partition(
    plan: &UnifiedPartitionInputPlan,
    trade_inventory: &[TradeTaskInventory],
    depth_inventory: &[DepthTaskInventory],
    boundaries: &[DepthBoundaryLink],
    dataset_order: &[u64],
    parquet: &MergeParquetConfig,
    staging: &Path,
) -> Result<UnifiedPartitionResult> {
    let partition = plan.partition;

    // -------------------------------------------------------------------------
    // Select partition tasks
    // -------------------------------------------------------------------------

    let selected = plan
        .inputs
        .iter()
        .map(|input| (input.dataset_id, input.task_id))
        .collect::<HashSet<_>>();

    let mut streams: Vec<Box<dyn MergeEventSource>> = Vec::with_capacity(dataset_order.len());

    for &dataset_id in dataset_order {
        let trades = trade_inventory
            .iter()
            .filter(|task| {
                task.dataset_id == dataset_id && selected.contains(&(task.dataset_id, task.task_id))
            })
            .cloned()
            .collect::<Vec<_>>();

        let depth = depth_inventory
            .iter()
            .filter(|task| {
                task.dataset_id == dataset_id && selected.contains(&(task.dataset_id, task.task_id))
            })
            .cloned()
            .collect::<Vec<_>>();

        let is_trade_dataset = trade_inventory
            .iter()
            .any(|task| task.dataset_id == dataset_id);

        let is_depth_dataset = depth_inventory
            .iter()
            .any(|task| task.dataset_id == dataset_id);

        match (is_trade_dataset, is_depth_dataset) {
            (true, false) => {
                streams.push(Box::new(TradeStream::new_range(
                    dataset_id,
                    trades,
                    partition.start_ns,
                    partition.end_ns,
                )?));
            }

            (false, true) => {
                streams.push(Box::new(PartitionDepthStream::new(
                    dataset_id, partition, depth, boundaries,
                )?));
            }

            (true, true) => {
                return invalid(format!(
                    "dataset {dataset_id}: classified as both trades and depth"
                ));
            }

            (false, false) => {
                return invalid(format!(
                    "dataset {dataset_id}: missing from source inventories"
                ));
            }
        }
    }

    // -------------------------------------------------------------------------
    // Construct unified K-way merger
    // -------------------------------------------------------------------------

    let mut merger = KWayEventMerger::new(streams)?;

    // -------------------------------------------------------------------------
    // Prepare partition output
    // -------------------------------------------------------------------------

    let relative_directory = format!("timeline/partition-{:06}", partition.partition_id);

    let output_directory = staging.join(&relative_directory);

    let mut writer = UnifiedTimelineWriter::new(&output_directory, parquet)?;

    let mut trade_events = 0_u64;
    let mut depth_events = 0_u64;

    // -------------------------------------------------------------------------
    // Merge and persist
    // -------------------------------------------------------------------------

    while let Some(merged) = merger.next_event()? {
        let timestamp = merged.event.timestamp_ns();

        if !partition.contains(timestamp) {
            return invalid(format!(
                "partition {} emitted timestamp {} outside [{}, {})",
                partition.partition_id, timestamp, partition.start_ns, partition.end_ns,
            ));
        }

        match merged.event.kind() {
            MergeEventKind::Trade => {
                trade_events = trade_events
                    .checked_add(1)
                    .ok_or_else(|| invalid_error("trade event count overflow"))?;
            }

            MergeEventKind::Depth => {
                depth_events = depth_events
                    .checked_add(1)
                    .ok_or_else(|| invalid_error("depth event count overflow"))?;
            }
        }

        writer.write(UnifiedTimelineRecord::from(&merged))?;
    }

    // -------------------------------------------------------------------------
    // Finalize
    // -------------------------------------------------------------------------

    writer.finish()?;

    if writer.is_failed() || !writer.is_finished() {
        return invalid("unified timeline writer did not finalize");
    }

    let metrics = writer.metrics().clone();

    if metrics.events_written != merger.events_emitted() {
        return invalid(format!(
            "partition {}: merger/writer event count mismatch",
            partition.partition_id
        ));
    }

    // -------------------------------------------------------------------------
    // Discover output files
    // -------------------------------------------------------------------------

    let mut files = Vec::new();

    for entry in fs::read_dir(&output_directory).map_err(|error| {
        invalid_error(format!(
            "failed to inspect unified output {}: {error}",
            output_directory.display()
        ))
    })? {
        let entry = entry.map_err(|error| {
            invalid_error(format!("failed to inspect unified output entry: {error}"))
        })?;

        let path = entry.path();

        if path.extension().and_then(|ext| ext.to_str()) != Some("parquet") {
            continue;
        }

        let metadata = fs::metadata(&path).map_err(|error| {
            invalid_error(format!("failed to inspect unified Parquet file: {error}"))
        })?;

        let filename = path
            .file_name()
            .ok_or_else(|| invalid_error("invalid unified output filename"))?
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

    files.sort_by(|left, right| left.path.cmp(&right.path));

    if files.len() as u64 != metrics.files_written {
        return invalid(format!(
            "partition {}: writer/discovered file count mismatch",
            partition.partition_id
        ));
    }

    // -------------------------------------------------------------------------
    // Collect depth initialization metadata
    // -------------------------------------------------------------------------

    let skipped_initializations = merger.skipped_initializations();

    Ok(UnifiedPartitionResult {
        partition_id: partition.partition_id,

        start_ns: partition.start_ns,
        end_ns: partition.end_ns,

        events_written: metrics.events_written,
        trade_events,
        depth_events,

        files_written: metrics.files_written,

        first_timestamp_ns: metrics.start_timestamp_ns,
        last_timestamp_ns: metrics.end_timestamp_ns,

        files,
        skipped_initializations,
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
