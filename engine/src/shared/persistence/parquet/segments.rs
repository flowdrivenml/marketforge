use std::{
    fs::{self, File},
    io::BufWriter,
    path::Path,
};

use serde::{Deserialize, Serialize};

use crate::{
    shared::domain::book::BookSegment,
    shared::error::{MarketForgeError, Result},
};

// -----------------------------------------------------------------------------
// Persistent segment manifest
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DepthSegmentManifest {
    pub version: u32,
    pub total_levels: u64,
    pub segments: Vec<BookSegment>,
}

impl DepthSegmentManifest {
    pub fn new(total_levels: u64, segments: Vec<BookSegment>) -> Result<Self> {
        let manifest = Self {
            version: 1,
            total_levels,
            segments,
        };

        manifest.validate()?;

        Ok(manifest)
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != 1 {
            return invalid("unsupported depth segment manifest version");
        }

        if self.total_levels > 0 && self.segments.is_empty() {
            return invalid("nonempty depth dataset requires reconstruction segments");
        }

        let mut previous_offset = None;

        for (index, segment) in self.segments.iter().enumerate() {
            if segment.segment_id != index as u64 {
                return invalid("depth segment IDs must be consecutive");
            }

            if segment.initial_level_count == 0 {
                return invalid("depth segment initialization cannot be empty");
            }

            let end = segment
                .initial_event_offset
                .checked_add(segment.initial_level_count)
                .ok_or_else(|| {
                    MarketForgeError::InvalidConfiguration(
                        "depth segment offset overflow".to_owned(),
                    )
                })?;

            if end > self.total_levels {
                return invalid("depth segment initialization exceeds dataset rows");
            }

            if let Some(previous) = previous_offset {
                if segment.initial_event_offset < previous {
                    return invalid("depth reconstruction segments overlap");
                }
            } else if segment.initial_event_offset != 0 {
                return invalid("first depth segment must begin at offset zero");
            }

            previous_offset = Some(end);
        }

        Ok(())
    }
}

// -----------------------------------------------------------------------------
// Persistence
// -----------------------------------------------------------------------------

pub fn write_depth_segments(directory: &Path, manifest: &DepthSegmentManifest) -> Result<()> {
    manifest.validate()?;

    let path = directory.join("segments.json");

    if path.exists() {
        return invalid(format!(
            "depth segment manifest already exists: {}",
            path.display()
        ));
    }

    let file = File::create(&path).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "failed to create depth segment manifest {}: {error}",
            path.display()
        ))
    })?;

    let writer = BufWriter::new(file);

    serde_json::to_writer_pretty(writer, manifest).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "failed to serialize depth segment manifest: {error}"
        ))
    })?;

    Ok(())
}

pub fn read_depth_segments(directory: &Path) -> Result<DepthSegmentManifest> {
    let path = directory.join("segments.json");

    let contents = fs::read_to_string(&path).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "failed to read depth segment manifest {}: {error}",
            path.display()
        ))
    })?;

    let manifest: DepthSegmentManifest = serde_json::from_str(&contents).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!("invalid depth segment manifest: {error}"))
    })?;

    manifest.validate()?;

    Ok(manifest)
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}

/// Source ranges use half-open stream-global canonical rows and event-index rows.
/// Event-index row position is the zero-based stream-global output event ordinal;
/// `event_ordinal` inside that row remains the original source-local ordinal.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthSourceBoundary {
    pub task_id: u64,
    pub start_row: u64,
    pub end_row: u64,
    pub start_event: u64,
    pub end_event: u64,
    pub boundary: crate::shared::domain::book::boundary::DepthBoundaryManifest,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DepthStreamBoundaries {
    pub version: u32,
    pub continuity: String,
    pub sources: Vec<DepthSourceBoundary>,
}
pub fn read_depth_stream_boundaries(directory: &Path) -> Result<DepthStreamBoundaries> {
    let value: DepthStreamBoundaries = serde_json::from_slice(
        &fs::read(directory.join("boundary.json"))
            .map_err(|e| MarketForgeError::InvalidConfiguration(e.to_string()))?,
    )
    .map_err(|e| MarketForgeError::InvalidConfiguration(e.to_string()))?;
    if value.version != 2 || value.continuity != "independent_sources" {
        return invalid("unsupported stream boundary contract");
    }
    let mut rows = 0;
    let mut events = 0;
    let mut ids = std::collections::HashSet::new();
    for source in &value.sources {
        if source.start_row != rows
            || source.start_event != events
            || source.end_row < rows
            || source.end_event < events
            || !ids.insert(source.task_id)
            || source.boundary.version != 1
        {
            return invalid("invalid depth source boundary ranges/identity");
        }
        rows = source.end_row;
        events = source.end_event;
    }
    Ok(value)
}
