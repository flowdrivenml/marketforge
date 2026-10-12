use std::collections::HashSet;

use crate::{
    error::{MarketForgeError, Result},
    job::MergeTimeRange,
};
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
};

use super::materialize_reconstruction::PhysicalInitialReconstruction;
use super::{
    manifest::{
        MergeBoundaryRecord, MergeInputDataset, MergeSkippedInitialization, MergeTimelineFile,
    },
    materialize_checkpoints::PhysicalMergeCheckpoint,
    materialize_global_validate::ValidatedPhysicalDataset,
    unified_manifest::{UnifiedMergeMode, UnifiedMergeStream, UnifiedMergeStreamKind},
};

pub const PHYSICAL_MERGE_MANIFEST_VERSION: u32 = 1;
pub const PHYSICAL_MERGE_SCHEMA_VERSION: u32 = 1;

// -----------------------------------------------------------------------------
// Physical dataset manifest
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhysicalMergeManifest {
    pub protocol_version: u32,
    pub physical_schema_version: u32,

    pub job_id: u64,
    pub mode: UnifiedMergeMode,

    pub input_datasets: Vec<MergeInputDataset>,
    pub time_range: MergeTimeRange,

    pub streams: Vec<UnifiedMergeStream>,

    pub events_written: u64,
    pub trade_events: u64,
    pub depth_events: u64,
    pub canonical_rows: u64,

    pub files_written: u64,
    pub row_groups: u64,

    pub start_timestamp_ns: Option<i64>,
    pub end_timestamp_ns: Option<i64>,

    pub files: Vec<MergeTimelineFile>,

    pub boundary_links: Vec<MergeBoundaryRecord>,
    pub skipped_initializations: Vec<MergeSkippedInitialization>,

    pub checkpoints: Vec<PhysicalMergeCheckpoint>,
    pub initial_reconstruction: Vec<PhysicalInitialReconstruction>,
}

// -----------------------------------------------------------------------------
// Construction
// -----------------------------------------------------------------------------

impl PhysicalMergeManifest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        job_id: u64,
        mode: UnifiedMergeMode,
        input_datasets: Vec<MergeInputDataset>,
        time_range: MergeTimeRange,
        streams: Vec<UnifiedMergeStream>,
        validated: ValidatedPhysicalDataset,
        boundary_links: Vec<MergeBoundaryRecord>,
        skipped_initializations: Vec<MergeSkippedInitialization>,
        checkpoints: Vec<PhysicalMergeCheckpoint>,
        initial_reconstruction: Vec<PhysicalInitialReconstruction>,
    ) -> Result<Self> {
        let manifest = Self {
            protocol_version: PHYSICAL_MERGE_MANIFEST_VERSION,
            physical_schema_version: PHYSICAL_MERGE_SCHEMA_VERSION,

            job_id,
            mode,

            input_datasets,
            time_range,
            streams,

            events_written: validated.events,
            trade_events: validated.trade_events,
            depth_events: validated.depth_events,
            canonical_rows: validated.canonical_rows,

            files_written: validated.files_written,
            row_groups: validated.row_groups,

            start_timestamp_ns: validated.first_timestamp_ns,
            end_timestamp_ns: validated.last_timestamp_ns,

            files: validated.files,

            boundary_links,
            skipped_initializations,
            checkpoints,
            initial_reconstruction,
        };

        manifest.validate()?;

        Ok(manifest)
    }

    pub fn validate(&self) -> Result<()> {
        if self.protocol_version != PHYSICAL_MERGE_MANIFEST_VERSION {
            return invalid("unsupported physical merge manifest version");
        }

        if self.physical_schema_version != PHYSICAL_MERGE_SCHEMA_VERSION {
            return invalid("unsupported physical merge schema version");
        }

        if self.time_range.start_ns >= self.time_range.end_ns {
            return invalid("invalid physical merge time range");
        }

        if self.input_datasets.is_empty() {
            return invalid("physical merge requires input datasets");
        }

        if self.streams.len() != self.input_datasets.len() {
            return invalid("physical merge stream count mismatch");
        }

        let mut seen_datasets = HashSet::new();

        for (index, (input, stream)) in self.input_datasets.iter().zip(&self.streams).enumerate() {
            if !seen_datasets.insert(input.dataset_id) {
                return invalid("duplicate physical input dataset");
            }

            if input.dataset_id != stream.dataset_id {
                return invalid("physical stream dataset mismatch");
            }

            if usize::try_from(stream.stream_rank).ok() != Some(index) {
                return invalid("invalid physical stream rank");
            }
        }

        let has_trades = self
            .streams
            .iter()
            .any(|stream| stream.kind == UnifiedMergeStreamKind::Trade);

        let has_depth = self
            .streams
            .iter()
            .any(|stream| stream.kind == UnifiedMergeStreamKind::Depth);

        match self.mode {
            UnifiedMergeMode::Trades if !has_trades || has_depth => {
                return invalid("invalid trade-only physical merge streams");
            }

            UnifiedMergeMode::Depth if !has_depth || has_trades => {
                return invalid("invalid depth-only physical merge streams");
            }

            UnifiedMergeMode::Combined if !has_trades || !has_depth => {
                return invalid("combined physical merge requires both stream kinds");
            }

            _ => {}
        }

        let event_total = self
            .trade_events
            .checked_add(self.depth_events)
            .ok_or_else(|| invalid_error("physical event total overflow"))?;

        if event_total != self.events_written {
            return invalid("physical event-kind count mismatch");
        }

        if self.files_written != self.files.len() as u64 {
            return invalid("physical file count mismatch");
        }

        let mut seen_files = HashSet::new();
        let mut file_rows = 0_u64;

        for file in &self.files {
            if !seen_files.insert(&file.path) {
                return invalid("duplicate physical manifest file");
            }

            if file.size_bytes == 0 {
                return invalid("empty physical manifest file");
            }

            let rows = file
                .rows
                .ok_or_else(|| invalid_error("physical file missing row count"))?;

            if rows == 0 {
                return invalid("physical file has zero rows");
            }

            file_rows = file_rows
                .checked_add(rows)
                .ok_or_else(|| invalid_error("physical file row total overflow"))?;

            let first = file
                .start_timestamp_ns
                .ok_or_else(|| invalid_error("physical file missing first timestamp"))?;

            let last = file
                .end_timestamp_ns
                .ok_or_else(|| invalid_error("physical file missing last timestamp"))?;

            if first > last {
                return invalid("invalid physical file timestamp interval");
            }

            if first < self.time_range.start_ns || last >= self.time_range.end_ns {
                return invalid("physical file exceeds requested time range");
            }

            if file.partition_id.is_none() {
                return invalid("physical file missing partition ID");
            }
        }

        if file_rows != self.events_written {
            return invalid("physical manifest row total mismatch");
        }

        if self.events_written == 0 {
            if self.start_timestamp_ns.is_some() || self.end_timestamp_ns.is_some() {
                return invalid("empty physical dataset contains timestamps");
            }
        } else {
            let (Some(first), Some(last)) = (self.start_timestamp_ns, self.end_timestamp_ns) else {
                return invalid("nonempty physical dataset requires timestamps");
            };

            if first > last {
                return invalid("invalid physical dataset timestamp range");
            }

            if first < self.time_range.start_ns || last >= self.time_range.end_ns {
                return invalid("physical dataset timestamps outside requested range");
            }
        }

        if !has_depth
            && (!self.boundary_links.is_empty() || !self.skipped_initializations.is_empty())
        {
            return invalid("trade-only physical dataset contains depth metadata");
        }
        // ---------------------------------------------------------------------
        // Physical checkpoint validation
        // ---------------------------------------------------------------------

        let mut seen_checkpoints = HashSet::new();

        for checkpoint in &self.checkpoints {
            if !seen_checkpoints.insert((
                checkpoint.dataset_id,
                checkpoint.task_id,
                checkpoint.segment_id,
            )) {
                return invalid("duplicate physical checkpoint segment");
            }

            if checkpoint.timestamp_ns < self.time_range.start_ns
                || checkpoint.timestamp_ns >= self.time_range.end_ns
            {
                return invalid("physical checkpoint outside dataset time range");
            }

            let path = format!(
                "timeline/partition-{:06}/part-{:06}.parquet",
                checkpoint.partition_id, checkpoint.file_index,
            );

            let file = self
                .files
                .iter()
                .find(|file| file.path == path)
                .ok_or_else(|| invalid_error("physical checkpoint references missing file"))?;

            let rows = file
                .rows
                .ok_or_else(|| invalid_error("checkpoint file missing row count"))?;

            if checkpoint.row_index >= rows {
                return invalid("physical checkpoint row index out of bounds");
            }
        }
        // -------------------------------------------------------------------------
        // Validate initial reconstruction metadata
        // -------------------------------------------------------------------------

        let depth_datasets = self
            .streams
            .iter()
            .filter(|stream| stream.kind == UnifiedMergeStreamKind::Depth)
            .map(|stream| stream.dataset_id)
            .collect::<HashSet<_>>();

        let mut seen_reconstruction = HashSet::new();

        for entry in &self.initial_reconstruction {
            if !depth_datasets.contains(&entry.dataset_id) {
                return invalid(format!(
                    "initial reconstruction references non-depth dataset {}",
                    entry.dataset_id
                ));
            }

            if !seen_reconstruction.insert(entry.dataset_id) {
                return invalid("duplicate initial reconstruction dataset");
            }

            let first_physical = self
                .checkpoints
                .iter()
                .filter(|checkpoint| checkpoint.dataset_id == entry.dataset_id)
                .map(|checkpoint| checkpoint.timestamp_ns)
                .min();

            if entry.first_physical_checkpoint_ns != first_physical {
                return invalid(format!(
                    "initial reconstruction checkpoint mismatch: dataset={}",
                    entry.dataset_id
                ));
            }

            if let Some(timestamp) = entry.first_physical_checkpoint_ns {
                if timestamp < self.time_range.start_ns || timestamp >= self.time_range.end_ns {
                    return invalid("initial reconstruction timestamp outside interval");
                }
            }
        }

        if seen_reconstruction != depth_datasets {
            return invalid("missing initial reconstruction metadata for depth dataset");
        }
        Ok(())
    }
}
pub fn write_physical_merge_manifest(
    directory: &Path,
    manifest: &PhysicalMergeManifest,
) -> Result<()> {
    manifest.validate()?;

    let path = directory.join("manifest.json");

    if path.exists() {
        return invalid(format!(
            "physical manifest already exists: {}",
            path.display()
        ));
    }

    let file = File::create(&path).map_err(|error| {
        invalid_error(format!(
            "cannot create physical manifest {}: {error}",
            path.display()
        ))
    })?;

    let mut writer = BufWriter::new(file);

    serde_json::to_writer_pretty(&mut writer, manifest)
        .map_err(|error| invalid_error(format!("cannot serialize physical manifest: {error}")))?;

    writer
        .flush()
        .map_err(|error| invalid_error(format!("cannot flush physical manifest: {error}")))?;

    Ok(())
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
