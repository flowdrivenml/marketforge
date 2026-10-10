use std::{
    fs::{self, File},
    io::BufWriter,
    path::Path,
};

use serde::{Deserialize, Serialize};

use crate::{
    book::BookSegment,
    error::{MarketForgeError, Result},
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
