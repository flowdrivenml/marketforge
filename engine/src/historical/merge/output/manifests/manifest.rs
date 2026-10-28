use std::{fs::File, io::BufWriter, path::Path};

use serde::{Deserialize, Serialize};

use crate::historical::merge::depth::checkpoint::InitialMergeCheckpoint;
use crate::historical::merge::depth::checkpoint::MergeCheckpoint;
use crate::{
    historical::job::MergeTimeRange,
    shared::error::{MarketForgeError, Result},
};
use {
    crate::historical::merge::depth::boundary::DepthBoundaryLink,
    crate::historical::merge::depth::boundary::MergeBoundaryRelation,
    crate::historical::merge::depth::inventory::DepthTaskInventory,
    crate::historical::merge::output::writers::writer::TimelineWriterMetrics,
};

pub const MERGE_MANIFEST_VERSION: u32 = 1;
pub const MERGE_MANIFEST_FILENAME: &str = "manifest.json";

// -----------------------------------------------------------------------------
// Merge manifest
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeManifest {
    pub protocol_version: u32,
    pub job_id: u64,

    pub input_datasets: Vec<MergeInputDataset>,
    pub time_range: MergeTimeRange,

    pub events_written: u64,
    pub files_written: u64,

    pub start_timestamp_ns: Option<i64>,
    pub end_timestamp_ns: Option<i64>,

    pub files: Vec<MergeTimelineFile>,
    pub streams: Vec<MergeStream>,

    pub boundary_links: Vec<MergeBoundaryRecord>,
    pub skipped_initializations: Vec<MergeSkippedInitialization>,
    pub checkpoints: Vec<MergeCheckpoint>,
    pub initial_checkpoints: Vec<InitialMergeCheckpoint>,
    #[serde(default)]
    pub timestamp_regressions_dropped: u64,
}

// -----------------------------------------------------------------------------
// Input datasets
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeInputDataset {
    pub dataset_id: u64,
    pub dataset_path: String,
    pub manifest_version: u32,
}

// -----------------------------------------------------------------------------
// Timeline files
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeTimelineFile {
    pub path: String,
    pub size_bytes: u64,

    #[serde(default)]
    pub partition_id: Option<u32>,

    #[serde(default)]
    pub rows: Option<u64>,

    #[serde(default)]
    pub start_timestamp_ns: Option<i64>,

    #[serde(default)]
    pub end_timestamp_ns: Option<i64>,
}

// -----------------------------------------------------------------------------
// Stream metadata
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeStream {
    pub stream_rank: u32,
    pub dataset_id: u64,

    pub exchange: String,
    pub instrument_id: i64,
    pub symbol: String,
    pub stream_id: String,
}

// -----------------------------------------------------------------------------
// Boundary metadata
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeBoundaryRecord {
    pub dataset_id: u64,
    pub previous_task_id: u64,
    pub next_task_id: u64,

    pub relation: String,

    pub previous_sequence: Option<u64>,
    pub next_sequence: Option<u64>,

    pub previous_timestamp_ns: Option<i64>,
    pub next_timestamp_ns: Option<i64>,

    pub timestamp_delta_ns: Option<i64>,
}

// -----------------------------------------------------------------------------
// Skipped initialization
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeSkippedInitialization {
    pub dataset_id: u64,
    pub task_id: u64,

    pub event_ordinal: u64,
    pub canonical_row_offset: u64,
    pub canonical_row_count: u64,

    pub timestamp_ns: i64,
}

// -----------------------------------------------------------------------------
// Manifest construction
// -----------------------------------------------------------------------------

impl MergeManifest {
    pub fn new(
        job_id: u64,
        time_range: MergeTimeRange,
        inputs: Vec<MergeInputDataset>,
        streams: Vec<MergeStream>,
        boundaries: &[DepthBoundaryLink],
        skipped_initializations: Vec<MergeSkippedInitialization>,
        metrics: &TimelineWriterMetrics,
        files: Vec<MergeTimelineFile>,
        checkpoints: Vec<MergeCheckpoint>,
        initial_checkpoints: Vec<InitialMergeCheckpoint>,
        timestamp_regressions_dropped: u64,
    ) -> Self {
        let boundary_links = boundaries.iter().map(MergeBoundaryRecord::from).collect();

        Self {
            protocol_version: MERGE_MANIFEST_VERSION,
            job_id,

            input_datasets: inputs,
            time_range,

            events_written: metrics.events_written,
            files_written: metrics.files_written,

            start_timestamp_ns: metrics.start_timestamp_ns,
            end_timestamp_ns: metrics.end_timestamp_ns,

            files,
            streams,

            boundary_links,
            skipped_initializations,
            checkpoints,
            initial_checkpoints,
            timestamp_regressions_dropped,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.protocol_version != MERGE_MANIFEST_VERSION {
            return invalid("unsupported merge manifest version");
        }

        if self.input_datasets.is_empty() {
            return invalid("merge manifest contains no input datasets");
        }

        if self.streams.is_empty() {
            return invalid("merge manifest contains no streams");
        }

        if self.files_written != self.files.len() as u64 {
            return invalid("merge timeline file count mismatch");
        }

        if self.events_written > 0 {
            let (Some(start), Some(end)) = (self.start_timestamp_ns, self.end_timestamp_ns) else {
                return invalid("nonempty merge requires timestamp boundaries");
            };

            if start > end {
                return invalid("invalid merged timestamp range");
            }

            if start < self.time_range.start_ns || end >= self.time_range.end_ns {
                return invalid("merged timestamps exceed requested interval");
            }
        }

        Ok(())
    }
}

// -----------------------------------------------------------------------------
// Boundary conversion
// -----------------------------------------------------------------------------

impl From<&DepthBoundaryLink> for MergeBoundaryRecord {
    fn from(link: &DepthBoundaryLink) -> Self {
        let relation = match link.relation {
            MergeBoundaryRelation::VerifiedContinuation => "verified_continuation",
            MergeBoundaryRelation::IndependentInitialization => "independent_initialization",
            MergeBoundaryRelation::SequenceGap => "sequence_gap",
            MergeBoundaryRelation::StateMismatch => "state_mismatch",
            MergeBoundaryRelation::Unverifiable => "unverifiable",
        };

        Self {
            dataset_id: link.dataset_id,
            previous_task_id: link.previous_task_id,
            next_task_id: link.next_task_id,

            relation: relation.to_owned(),

            previous_sequence: link.previous_sequence,
            next_sequence: link.next_sequence,

            previous_timestamp_ns: link.previous_timestamp_ns,
            next_timestamp_ns: link.next_timestamp_ns,

            timestamp_delta_ns: link.timestamp_delta_ns,
        }
    }
}

// -----------------------------------------------------------------------------
// Stream construction
// -----------------------------------------------------------------------------

pub fn build_merge_streams(
    inventory: &[DepthTaskInventory],
    dataset_order: &[u64],
) -> Result<Vec<MergeStream>> {
    let mut streams = Vec::new();

    for (rank, &dataset_id) in dataset_order.iter().enumerate() {
        let task = inventory
            .iter()
            .find(|task| task.dataset_id == dataset_id)
            .ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(format!(
                    "dataset {dataset_id}: missing depth inventory"
                ))
            })?;

        let boundary = &task.boundary;

        streams.push(MergeStream {
            stream_rank: u32::try_from(rank).map_err(|_| {
                MarketForgeError::InvalidConfiguration("merge stream rank overflow".to_owned())
            })?,
            dataset_id,

            exchange: format!("{:?}", boundary.exchange).to_lowercase(),
            instrument_id: boundary.instrument_id,
            symbol: boundary.symbol.clone(),
            stream_id: boundary.stream_id.clone(),
        });
    }

    Ok(streams)
}

// -----------------------------------------------------------------------------
// Manifest persistence
// -----------------------------------------------------------------------------

pub fn write_merge_manifest(directory: &Path, manifest: &MergeManifest) -> Result<()> {
    manifest.validate()?;

    let path = directory.join(MERGE_MANIFEST_FILENAME);

    if path.exists() {
        return invalid(format!("merge manifest already exists: {}", path.display()));
    }

    let file = File::create(&path).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "failed to create merge manifest {}: {error}",
            path.display()
        ))
    })?;

    serde_json::to_writer_pretty(BufWriter::new(file), manifest).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "failed to serialize merge manifest: {error}"
        ))
    })?;

    Ok(())
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}
