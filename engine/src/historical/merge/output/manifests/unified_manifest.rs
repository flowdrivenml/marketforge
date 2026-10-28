use serde::{Deserialize, Serialize};

use crate::{
    historical::job::MergeTimeRange,
    shared::error::{MarketForgeError, Result},
};

use std::collections::HashSet;
use {
    crate::historical::merge::depth::checkpoint::InitialMergeCheckpoint,
    crate::historical::merge::depth::checkpoint::MergeCheckpoint,
    crate::historical::merge::output::manifests::manifest::MergeBoundaryRecord,
    crate::historical::merge::output::manifests::manifest::MergeInputDataset,
    crate::historical::merge::output::manifests::manifest::MergeSkippedInitialization,
    crate::historical::merge::output::manifests::manifest::MergeTimelineFile,
    crate::historical::merge::output::writers::unified_timeline::UNIFIED_TIMELINE_VERSION,
};
// -----------------------------------------------------------------------------
// Protocol
// -----------------------------------------------------------------------------

pub const UNIFIED_MERGE_MANIFEST_VERSION: u32 = 2;

// -----------------------------------------------------------------------------
// Merge mode
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnifiedMergeMode {
    Trades,
    Depth,
    Combined,
}

// -----------------------------------------------------------------------------
// Stream kind
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnifiedMergeStreamKind {
    Trade,
    Depth,
}

// -----------------------------------------------------------------------------
// Stream metadata
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnifiedMergeStream {
    pub stream_rank: u32,
    pub dataset_id: u64,
    pub kind: UnifiedMergeStreamKind,

    pub exchange: String,
    pub instrument_id: i64,
    pub symbol: String,
    pub stream_id: String,

    pub base_currency: Option<String>,
    pub quote_currency: Option<String>,
}

// -----------------------------------------------------------------------------
// Unified manifest
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnifiedMergeManifest {
    pub protocol_version: u32,
    pub timeline_schema_version: u32,

    pub job_id: u64,
    pub mode: UnifiedMergeMode,

    pub input_datasets: Vec<MergeInputDataset>,
    pub time_range: MergeTimeRange,

    pub events_written: u64,
    pub trade_events: u64,
    pub depth_events: u64,
    pub files_written: u64,

    pub start_timestamp_ns: Option<i64>,
    pub end_timestamp_ns: Option<i64>,

    pub files: Vec<MergeTimelineFile>,
    pub streams: Vec<UnifiedMergeStream>,

    pub boundary_links: Vec<MergeBoundaryRecord>,
    pub skipped_initializations: Vec<MergeSkippedInitialization>,

    pub checkpoints: Vec<MergeCheckpoint>,
    pub initial_checkpoints: Vec<InitialMergeCheckpoint>,
}

// -----------------------------------------------------------------------------
// Validation
// -----------------------------------------------------------------------------

impl UnifiedMergeManifest {
    pub fn validate(&self) -> Result<()> {
        if self.protocol_version != UNIFIED_MERGE_MANIFEST_VERSION {
            return invalid("unsupported unified merge manifest version");
        }

        if self.timeline_schema_version != UNIFIED_TIMELINE_VERSION {
            return invalid("unsupported unified timeline schema version");
        }

        if self.input_datasets.is_empty() {
            return invalid("unified merge contains no input datasets");
        }

        if self.streams.len() != self.input_datasets.len() {
            return invalid("unified merge stream count mismatch");
        }
        // -------------------------------------------------------------------------
        // Validate dataset identities
        // -------------------------------------------------------------------------

        let mut seen_datasets = HashSet::new();

        for input in &self.input_datasets {
            if !seen_datasets.insert(input.dataset_id) {
                return invalid(format!(
                    "duplicate unified merge input dataset {}",
                    input.dataset_id
                ));
            }
        }

        // -------------------------------------------------------------------------
        // Validate stream kinds
        // -------------------------------------------------------------------------

        let mut has_trades = false;
        let mut has_depth = false;

        for stream in &self.streams {
            match stream.kind {
                UnifiedMergeStreamKind::Trade => has_trades = true,
                UnifiedMergeStreamKind::Depth => has_depth = true,
            }
        }

        match self.mode {
            UnifiedMergeMode::Trades => {
                if !has_trades || has_depth {
                    return invalid("trade-only merge must contain exclusively trade streams");
                }
            }

            UnifiedMergeMode::Depth => {
                if !has_depth || has_trades {
                    return invalid("depth-only merge must contain exclusively depth streams");
                }
            }

            UnifiedMergeMode::Combined => {
                if !has_trades || !has_depth {
                    return invalid("combined merge requires both trade and depth streams");
                }
            }
        }

        // -------------------------------------------------------------------------
        // Validate depth-specific metadata
        // -------------------------------------------------------------------------

        if !has_depth {
            if !self.boundary_links.is_empty()
                || !self.skipped_initializations.is_empty()
                || !self.checkpoints.is_empty()
                || !self.initial_checkpoints.is_empty()
            {
                return invalid("trade-only merge contains depth-specific metadata");
            }
        }

        if self.files_written != self.files.len() as u64 {
            return invalid("unified merge file count mismatch");
        }

        let total = self
            .trade_events
            .checked_add(self.depth_events)
            .ok_or_else(|| invalid_error("unified event count overflow"))?;

        if total != self.events_written {
            return invalid("unified trade/depth event count mismatch");
        }

        match self.mode {
            UnifiedMergeMode::Trades if self.depth_events != 0 => {
                return invalid("trade-only merge contains depth events");
            }

            UnifiedMergeMode::Depth if self.trade_events != 0 => {
                return invalid("depth-only merge contains trade events");
            }

            _ => {}
        }

        if self.events_written > 0 {
            let (Some(start), Some(end)) = (self.start_timestamp_ns, self.end_timestamp_ns) else {
                return invalid("nonempty unified merge requires timestamps");
            };

            if start > end {
                return invalid("invalid unified merge timestamp range");
            }

            if start < self.time_range.start_ns || end >= self.time_range.end_ns {
                return invalid("unified timestamps exceed requested interval");
            }
        } else if self.start_timestamp_ns.is_some() || self.end_timestamp_ns.is_some() {
            return invalid("empty unified merge contains timestamp metadata");
        }

        for (index, stream) in self.streams.iter().enumerate() {
            if usize::try_from(stream.stream_rank).ok() != Some(index) {
                return invalid(format!(
                    "unexpected unified stream rank {}",
                    stream.stream_rank
                ));
            }

            if stream.dataset_id != self.input_datasets[index].dataset_id {
                return invalid(format!("unified stream dataset mismatch at index {index}"));
            }
        }

        Ok(())
    }
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
