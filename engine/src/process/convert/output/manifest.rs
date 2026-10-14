use super::combined_validation::{CombinedValidationMetrics, validate_combined_file};
use super::depth_validation::{DepthConversionValidationMetrics, validate_depth_file};
use super::receipt::ConversionDepthStatistics;
use super::{
    source_validation::SourcePreservationMetrics, validation::ConversionValidationMetrics,
};
use crate::process::merge::UnifiedMergeMode;
use crate::{
    error::{MarketForgeError, Result},
    job::{ConversionConfig, ConvertJob},
    process::merge::PhysicalMergeManifest,
};
use serde::{Deserialize, Serialize};
use std::path::Component;
use std::{
    fs::File,
    io::BufWriter,
    path::{Path, PathBuf},
};

pub const CONVERSION_MANIFEST_VERSION: u32 = 1;
pub const CONVERSION_MANIFEST_FILENAME: &str = "manifest.json";

// -----------------------------------------------------------------------------
// Source identity
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversionManifestInput {
    pub dataset_path: PathBuf,
    pub merge_job_id: u64,
    pub events: u64,
}

// -----------------------------------------------------------------------------
// Physical output file
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversionManifestFile {
    pub path: PathBuf,
    pub size_bytes: u64,
    pub rows: u64,
    pub row_groups: u64,
}

// -----------------------------------------------------------------------------
// Conversion statistics
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversionManifestStatistics {
    pub events_written: u64,

    pub identity: u64,
    pub converted: u64,
    pub unavailable: u64,

    pub reference_updates: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depth: Option<ConversionDepthStatistics>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub skipped: u64,
}

// -----------------------------------------------------------------------------
// Output metadata
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversionManifestOutput {
    pub content_type: String,

    pub files_written: u64,
    pub row_groups: u64,

    pub first_timestamp_ns: Option<i64>,
    pub last_timestamp_ns: Option<i64>,

    pub files: Vec<ConversionManifestFile>,
}

// -----------------------------------------------------------------------------
// Integrity results
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversionManifestIntegrity {
    pub status: String,

    pub conversion_validation: bool,
    pub source_preservation: bool,

    pub references_checked: u64,
    pub source_events_compared: u64,
}

// -----------------------------------------------------------------------------
// Conversion manifest
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversionManifest {
    pub protocol_version: u32,
    pub conversion_schema_version: u32,

    pub job_id: u64,

    pub input: ConversionManifestInput,
    pub conversion: ConversionConfig,

    pub statistics: ConversionManifestStatistics,
    pub output: ConversionManifestOutput,
    pub integrity: ConversionManifestIntegrity,
}

impl ConversionManifest {
    pub fn build(
        job: &ConvertJob,
        source: &PhysicalMergeManifest,
        validation: &ConversionValidationMetrics,
        preservation: &SourcePreservationMetrics,
        reference_updates: u64,
        output_directory: &Path,
    ) -> Result<Self> {
        job.validate()?;
        source.validate()?;

        let files = discover_output_files(output_directory)?;

        let files_written = files.len() as u64;

        let row_groups = files.iter().map(|file| file.row_groups).sum();

        let manifest = Self {
            protocol_version: CONVERSION_MANIFEST_VERSION,
            conversion_schema_version: super::schema::CONVERSION_SCHEMA_VERSION,

            job_id: job.job_id,

            input: ConversionManifestInput {
                dataset_path: job.input_merge.clone(),
                merge_job_id: source.job_id,
                events: source.events_written,
            },

            conversion: job.conversion.clone(),

            statistics: ConversionManifestStatistics {
                events_written: validation.events,
                identity: validation.identity,
                converted: validation.converted,
                unavailable: validation.unavailable,
                reference_updates,
                depth: None,
                skipped: 0,
            },

            output: ConversionManifestOutput {
                content_type: "trades".to_owned(),

                files_written,
                row_groups,

                first_timestamp_ns: validation.first_timestamp_ns,
                last_timestamp_ns: validation.last_timestamp_ns,

                files,
            },

            integrity: ConversionManifestIntegrity {
                status: "pass".to_owned(),

                conversion_validation: true,
                source_preservation: true,

                references_checked: validation.references_checked,
                source_events_compared: preservation.events_compared,
            },
        };

        manifest.validate()?;

        Ok(manifest)
    }

    pub fn validate(&self) -> Result<()> {
        if self.protocol_version != CONVERSION_MANIFEST_VERSION {
            return invalid("unsupported conversion manifest version");
        }

        if self.conversion_schema_version != super::schema::CONVERSION_SCHEMA_VERSION {
            return invalid("unsupported conversion schema version");
        }

        if self.input.events != self.statistics.events_written {
            return invalid("conversion input/output event count mismatch");
        }
        match self.output.content_type.as_str() {
            "trades" => {
                if self.statistics.depth.is_some() {
                    return invalid("trade manifest contains depth statistics");
                }
            }

            "depth" | "combined" => {
                let depth = self.statistics.depth.as_ref().ok_or_else(|| {
                    invalid_error("depth or combined manifest lacks level statistics")
                })?;

                depth.validate()?;
            }

            _ => return invalid("unsupported conversion content type"),
        }

        let accounted = self
            .statistics
            .identity
            .checked_add(self.statistics.converted)
            .and_then(|value| value.checked_add(self.statistics.unavailable))
            .and_then(|value| value.checked_add(self.statistics.skipped))
            .ok_or_else(|| invalid_error("conversion status counter overflow"))?;

        if accounted != self.statistics.events_written {
            return invalid("conversion status accounting mismatch");
        }

        if self.output.files_written != self.output.files.len() as u64 {
            return invalid("conversion file count mismatch");
        }

        let row_groups: u64 = self.output.files.iter().map(|file| file.row_groups).sum();

        if row_groups != self.output.row_groups {
            return invalid("conversion row-group count mismatch");
        }

        let rows: u64 = self.output.files.iter().map(|file| file.rows).sum();

        if rows != self.statistics.events_written {
            return invalid("conversion physical row count mismatch");
        }

        if self.integrity.status != "pass"
            || !self.integrity.conversion_validation
            || !self.integrity.source_preservation
        {
            return invalid("conversion integrity validation incomplete");
        }

        if self.integrity.source_events_compared != self.statistics.events_written {
            return invalid("source preservation count mismatch");
        }

        if self.output.first_timestamp_ns > self.output.last_timestamp_ns {
            return invalid("invalid conversion timestamp boundaries");
        }

        if self.output.files.is_empty() {
            return invalid("conversion manifest contains no output files");
        }

        let mut paths = std::collections::HashSet::new();

        for file in &self.output.files {
            if file.path.as_os_str().is_empty()
                || file.path.is_absolute()
                || file
                    .path
                    .components()
                    .any(|component| !matches!(component, Component::Normal(_)))
            {
                return invalid("invalid conversion output file path");
            }

            if !paths.insert(&file.path) {
                return invalid("duplicate conversion output file path");
            }

            if file.size_bytes == 0 || file.rows == 0 || file.row_groups == 0 {
                return invalid("invalid conversion output file metadata");
            }
        }

        Ok(())
    }

    pub fn write(&self, directory: &Path) -> Result<PathBuf> {
        self.validate()?;

        let path = directory.join(CONVERSION_MANIFEST_FILENAME);

        if path.exists() {
            return invalid(format!(
                "conversion manifest already exists: {}",
                path.display()
            ));
        }

        let file = File::create(&path).map_err(|error| {
            invalid_error(format!(
                "cannot create conversion manifest {}: {error}",
                path.display()
            ))
        })?;

        serde_json::to_writer_pretty(BufWriter::new(file), self).map_err(|error| {
            invalid_error(format!("cannot serialize conversion manifest: {error}"))
        })?;

        Ok(path)
    }

    pub fn build_depth(
        job: &ConvertJob,
        source: &PhysicalMergeManifest,
        validation: &DepthConversionValidationMetrics,
        preservation: &SourcePreservationMetrics,
        reference_updates: u64,
        output_directory: &Path,
    ) -> Result<Self> {
        job.validate()?;
        source.validate()?;

        if source.mode != UnifiedMergeMode::Depth {
            return invalid("depth manifest requires depth-only source");
        }

        let files = discover_output_files(output_directory)?;

        let files_written = files.len() as u64;
        let row_groups = files.iter().map(|file| file.row_groups).sum();

        let first_file = files
            .first()
            .ok_or_else(|| invalid_error("depth manifest has no Parquet files"))?;

        let last_file = files
            .last()
            .ok_or_else(|| invalid_error("depth manifest has no Parquet files"))?;

        let first_key = validate_depth_file(
            &output_directory.join(&first_file.path),
            job.conversion.max_staleness_ns,
        )?
        .first_key;

        let last_key = validate_depth_file(
            &output_directory.join(&last_file.path),
            job.conversion.max_staleness_ns,
        )?
        .last_key;

        let manifest = Self {
            protocol_version: CONVERSION_MANIFEST_VERSION,
            conversion_schema_version: super::schema::CONVERSION_SCHEMA_VERSION,

            job_id: job.job_id,

            input: ConversionManifestInput {
                dataset_path: job.input_merge.clone(),
                merge_job_id: source.job_id,
                events: source.events_written,
            },

            conversion: job.conversion.clone(),

            statistics: ConversionManifestStatistics {
                events_written: validation.events,
                identity: validation.identity,
                converted: validation.converted,
                unavailable: validation.unavailable,
                reference_updates,
                skipped: 0,

                depth: Some(ConversionDepthStatistics {
                    levels_read: validation.levels,
                    levels_converted: validation.levels_converted,
                    levels_unavailable: validation.levels_unavailable,
                    levels_skipped: 0,
                }),
            },

            output: ConversionManifestOutput {
                content_type: "depth".to_owned(),
                files_written,
                row_groups,
                first_timestamp_ns: Some(first_key.timestamp_ns),
                last_timestamp_ns: Some(last_key.timestamp_ns),
                files,
            },

            integrity: ConversionManifestIntegrity {
                status: "pass".to_owned(),
                conversion_validation: true,
                source_preservation: true,
                references_checked: validation.references_checked,
                source_events_compared: preservation.events_compared,
            },
        };

        manifest.validate()?;

        Ok(manifest)
    }
    pub fn build_combined(
        job: &ConvertJob,
        source: &PhysicalMergeManifest,
        validation: &CombinedValidationMetrics,
        preservation: &SourcePreservationMetrics,
        reference_updates: u64,
        output_directory: &Path,
    ) -> Result<Self> {
        job.validate()?;
        source.validate()?;

        if source.mode != UnifiedMergeMode::Combined {
            return invalid("combined manifest requires combined source");
        }

        if validation.events != source.events_written
            || validation.trade_events != source.trade_events
            || validation.depth_events != source.depth_events
        {
            return invalid("combined manifest source/output count mismatch");
        }

        if preservation.events_compared != validation.events {
            return invalid("combined manifest source preservation mismatch");
        }

        let files = discover_output_files(output_directory)?;

        let files_written = files.len() as u64;

        let row_groups = files.iter().map(|file| file.row_groups).sum();

        let first_file = files
            .first()
            .ok_or_else(|| invalid_error("combined manifest has no Parquet files"))?;

        let last_file = files
            .last()
            .ok_or_else(|| invalid_error("combined manifest has no Parquet files"))?;

        let first_key = validate_combined_file(
            &output_directory.join(&first_file.path),
            job.conversion.max_staleness_ns,
        )?
        .first_key;

        let last_key = validate_combined_file(
            &output_directory.join(&last_file.path),
            job.conversion.max_staleness_ns,
        )?
        .last_key;

        let manifest = Self {
            protocol_version: CONVERSION_MANIFEST_VERSION,
            conversion_schema_version: super::schema::CONVERSION_SCHEMA_VERSION,

            job_id: job.job_id,

            input: ConversionManifestInput {
                dataset_path: job.input_merge.clone(),
                merge_job_id: source.job_id,
                events: source.events_written,
            },

            conversion: job.conversion.clone(),

            statistics: ConversionManifestStatistics {
                events_written: validation.events,
                identity: validation.identity,
                converted: validation.converted,
                unavailable: validation.unavailable,
                reference_updates,
                skipped: validation.skipped,

                depth: Some(ConversionDepthStatistics {
                    levels_read: validation.depth_levels,
                    levels_converted: validation.depth_levels_converted,
                    levels_unavailable: validation.depth_levels_unavailable,
                    levels_skipped: validation.depth_levels_skipped,
                }),
            },

            output: ConversionManifestOutput {
                content_type: "combined".to_owned(),
                files_written,
                row_groups,

                first_timestamp_ns: Some(first_key.timestamp_ns),
                last_timestamp_ns: Some(last_key.timestamp_ns),

                files,
            },

            integrity: ConversionManifestIntegrity {
                status: "pass".to_owned(),
                conversion_validation: true,
                source_preservation: true,
                references_checked: validation.references_checked,
                source_events_compared: preservation.events_compared,
            },
        };

        manifest.validate()?;

        Ok(manifest)
    }
}

// -----------------------------------------------------------------------------
// Discover physical output files
// -----------------------------------------------------------------------------

fn discover_output_files(directory: &Path) -> Result<Vec<ConversionManifestFile>> {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let paths = super::files::discover_conversion_files(directory)?;

    let mut files = Vec::with_capacity(paths.len());

    for path in paths {
        let file = File::open(&path).map_err(|error| invalid_error(error.to_string()))?;

        let reader =
            SerializedFileReader::new(file).map_err(|error| invalid_error(error.to_string()))?;

        let metadata = reader.metadata();

        let relative_path = path
            .strip_prefix(directory)
            .map_err(|error| invalid_error(error.to_string()))?
            .to_path_buf();

        files.push(ConversionManifestFile {
            path: relative_path,

            size_bytes: std::fs::metadata(&path)
                .map_err(|error| invalid_error(error.to_string()))?
                .len(),

            rows: metadata.file_metadata().num_rows() as u64,
            row_groups: metadata.num_row_groups() as u64,
        });
    }

    Ok(files)
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
fn is_zero(value: &u64) -> bool {
    *value == 0
}
