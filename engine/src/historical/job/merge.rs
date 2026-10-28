use std::{collections::HashMap, path::PathBuf};

use serde::{Deserialize, Serialize};

use crate::shared::error::{MarketForgeError, Result};
// -----------------------------------------------------------------------------
// Merge job
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeJob {
    pub job_id: u64,
    pub operation: MergeOperation,

    pub inputs: Vec<u64>,

    #[serde(default)]
    pub input_paths: HashMap<u64, PathBuf>,

    pub time_range: MergeTimeRange,

    pub resources: MergeResources,
    pub partitioning: MergePartitioning,
    pub parquet: MergeParquetConfig,

    pub output: MergeOutput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeOperation {
    Merge,
}

// -----------------------------------------------------------------------------
// Time range
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeTimeRange {
    pub start_ns: i64,
    pub end_ns: i64,
}

// -----------------------------------------------------------------------------
// Resources
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeResources {
    pub workers: usize,
    pub memory_limit_mb: u64,
    pub max_open_files: usize,
}

// -----------------------------------------------------------------------------
// Partitioning
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergePartitioning {
    pub partitions_per_worker: usize,
}

// -----------------------------------------------------------------------------
// Parquet
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeParquetConfig {
    pub row_group_target_mb: u64,
    pub file_target_mb: u64,
    pub compression_level: i32,
}

// -----------------------------------------------------------------------------
// Output
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeOutput {
    pub dataset_path: PathBuf,
}

// -----------------------------------------------------------------------------
// Validation
// -----------------------------------------------------------------------------

impl MergeJob {
    pub fn validate(&self) -> Result<()> {
        if self.inputs.is_empty() {
            return invalid("merge job requires at least one input");
        }

        let mut unique = std::collections::HashSet::new();

        for dataset_id in &self.inputs {
            if !unique.insert(dataset_id) {
                return invalid(format!("duplicate merge input dataset ID: {dataset_id}"));
            }
        }

        // -------------------------------------------------------------------------
        // Validate explicit input dataset paths
        // -------------------------------------------------------------------------

        for (&dataset_id, path) in &self.input_paths {
            if !unique.contains(&dataset_id) {
                return invalid(format!(
                    "input_paths contains dataset {dataset_id}, \
             which is not present in merge inputs"
                ));
            }

            if path.as_os_str().is_empty() {
                return invalid(format!("dataset {dataset_id}: empty input path"));
            }
        }

        if self.time_range.start_ns >= self.time_range.end_ns {
            return invalid("merge time range must have start_ns < end_ns");
        }

        if self.resources.workers == 0 {
            return invalid("merge workers must be greater than zero");
        }

        if self.resources.memory_limit_mb == 0 {
            return invalid("merge memory limit must be greater than zero");
        }

        if self.resources.max_open_files == 0 {
            return invalid("merge max_open_files must be greater than zero");
        }

        if self.partitioning.partitions_per_worker == 0 {
            return invalid("partitions_per_worker must be greater than zero");
        }

        self.resources
            .workers
            .checked_mul(self.partitioning.partitions_per_worker)
            .ok_or_else(|| {
                MarketForgeError::InvalidConfiguration("merge partition count overflow".to_owned())
            })?;

        if self.parquet.row_group_target_mb == 0 {
            return invalid("Parquet row-group target must be positive");
        }

        if self.parquet.file_target_mb < self.parquet.row_group_target_mb {
            return invalid("Parquet file target must be at least the row-group target");
        }

        if !(1..=22).contains(&self.parquet.compression_level) {
            return invalid("ZSTD compression level must be between 1 and 22");
        }

        if !self.output.dataset_path.is_absolute() {
            return invalid("merge output dataset_path must be absolute");
        }

        Ok(())
    }
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.into()))
}
