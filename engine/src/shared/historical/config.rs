use std::{fs, path::Path};

use serde::{Deserialize, Serialize};

use crate::{
    shared::error::{MarketForgeError, Result},
    shared::historical::{IntegrityPolicy, ResourceConfig},
};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessingConfig {
    #[serde(default)]
    pub diagnostics: DiagnosticsConfig,
    #[serde(default = "default_merge_fan_in")]
    pub merge_fan_in: usize,
    #[serde(default)]
    pub scratch_format: ScratchFormat,
    #[serde(default)]
    pub scratch_codec: ScratchCodec,
    #[serde(default)]
    pub reconciliation: super::policies::Reconciliation,
    #[serde(default)]
    pub timestamp_regression: super::policies::TimestampRegression,
    pub resources: ResourceConfig,
    pub integrity_policy: IntegrityPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticsMode {
    #[default]
    Disabled,
    Basic,
    Detailed,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct DiagnosticsConfig {
    pub mode: DiagnosticsMode,
    pub interval_ms: u64,
}
impl Default for DiagnosticsConfig {
    fn default() -> Self {
        Self {
            mode: DiagnosticsMode::Disabled,
            interval_ms: 1000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ScratchFormat {
    #[default]
    Json,
    TypedTrades,
    /// Typed trades and shared-envelope depth payloads; JSON for other records.
    TypedEvents,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ScratchCodec {
    #[default]
    Zlib,
    Uncompressed,
}

fn default_merge_fan_in() -> usize {
    8
}

pub const RUNTIME_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeDocument {
    schema_version: u32,
    #[serde(default)]
    diagnostics: DiagnosticsConfig,
    #[serde(default = "default_merge_fan_in")]
    merge_fan_in: usize,
    #[serde(default)]
    scratch_format: ScratchFormat,
    #[serde(default)]
    scratch_codec: ScratchCodec,
    #[serde(default)]
    reconciliation: super::policies::Reconciliation,
    #[serde(default)]
    timestamp_regression: super::policies::TimestampRegression,
    #[serde(default)]
    merge: MergeRuntime,
    resources: ResourceConfig,
    integrity_policy: IntegrityPolicy,
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct MergeRuntime {
    partitions_per_worker: usize,
    compression_level: i32,
}

impl Default for MergeRuntime {
    fn default() -> Self {
        Self {
            partitions_per_worker: 4,
            compression_level: 3,
        }
    }
}

#[derive(Serialize)]
pub struct MergeRuntimeSettings {
    pub resources: crate::historical::job::MergeResources,
    pub partitioning: crate::historical::job::MergePartitioning,
    pub parquet: crate::historical::job::MergeParquetConfig,
}

pub fn load_merge_runtime_settings(path: &Path, root: &Path) -> Result<MergeRuntimeSettings> {
    use crate::historical::job::MergePartitioning;
    let (config, merge) = load_runtime(path, root)?;
    if merge.partitions_per_worker == 0 || !(1..=22).contains(&merge.compression_level) {
        return invalid("invalid merge partition count or ZSTD compression level");
    }
    if config
        .resources
        .workers
        .checked_mul(merge.partitions_per_worker)
        .is_none()
    {
        return invalid("merge partition count overflow");
    }
    let (resources, parquet) = runtime_io_settings(config.resources, merge.compression_level)?;
    Ok(MergeRuntimeSettings {
        resources,
        partitioning: MergePartitioning {
            partitions_per_worker: merge.partitions_per_worker,
        },
        parquet,
    })
}

pub fn load_conversion_runtime_settings(
    path: &Path,
    root: &Path,
) -> Result<(
    crate::historical::job::MergeResources,
    crate::historical::job::MergeParquetConfig,
)> {
    let config = load_processing_config(path, root)?;
    runtime_io_settings(config.resources, 3)
}

fn runtime_io_settings(
    resources: ResourceConfig,
    compression_level: i32,
) -> Result<(
    crate::historical::job::MergeResources,
    crate::historical::job::MergeParquetConfig,
)> {
    use crate::historical::job::{MergeParquetConfig, MergeResources};
    let mb = |bytes: u64, field: &str| -> Result<u64> {
        const MIB: u64 = 1024 * 1024;
        if bytes % MIB != 0 {
            return invalid(&format!(
                "historical runtime requires {field} to be a whole number of MiB"
            ));
        }
        Ok(bytes / MIB)
    };
    Ok((
        MergeResources {
            workers: resources.workers,
            memory_limit_mb: mb(resources.memory_budget_bytes, "memory_budget_bytes")?,
            max_open_files: usize::try_from(resources.normalization_limits.open_files).map_err(
                |_| {
                    MarketForgeError::InvalidConfiguration(
                        "historical runtime open-file limit overflow".into(),
                    )
                },
            )?,
        },
        MergeParquetConfig {
            row_group_target_mb: mb(
                resources.parquet.row_group_target_bytes,
                "row_group_target_bytes",
            )?,
            file_target_mb: mb(resources.parquet.file_target_bytes, "file_target_bytes")?,
            compression_level,
        },
    ))
}

/// Canonicalize a real project directory, without changing the process CWD.
pub fn project_root(root: impl AsRef<Path>) -> Result<std::path::PathBuf> {
    let root = root.as_ref();
    let resolved = root.canonicalize().map_err(|e| {
        MarketForgeError::InvalidConfiguration(format!(
            "invalid project root {}: {e}",
            root.display()
        ))
    })?;
    if !resolved.is_dir() {
        return invalid(&format!(
            "invalid project root {}: must be a directory",
            root.display()
        ));
    }
    Ok(resolved)
}

pub fn resolve_path(root: &Path, path: &Path) -> std::path::PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    }
}

pub fn load_processing_config(
    path: impl AsRef<Path>,
    root: impl AsRef<Path>,
) -> Result<ProcessingConfig> {
    load_runtime(path.as_ref(), root.as_ref()).map(|(processing, _merge)| processing)
}

fn load_runtime(path: &Path, root: &Path) -> Result<(ProcessingConfig, MergeRuntime)> {
    let root = project_root(root)?;
    let path = resolve_path(&root, path);
    let result = (|| -> Result<(ProcessingConfig, MergeRuntime)> {
        let contents = fs::read_to_string(&path)
            .map_err(|e| MarketForgeError::InvalidConfiguration(e.to_string()))?;
        let document: RuntimeDocument = toml::from_str(&contents)
            .map_err(|e| MarketForgeError::InvalidConfiguration(e.to_string()))?;
        if document.schema_version != RUNTIME_SCHEMA_VERSION {
            return invalid(&format!(
                "schema_version: unsupported version {}; expected {RUNTIME_SCHEMA_VERSION}",
                document.schema_version
            ));
        }
        let mut config = ProcessingConfig {
            diagnostics: document.diagnostics,
            merge_fan_in: document.merge_fan_in,
            scratch_format: document.scratch_format,
            scratch_codec: document.scratch_codec,
            reconciliation: document.reconciliation,
            timestamp_regression: document.timestamp_regression,
            resources: document.resources,
            integrity_policy: document.integrity_policy,
        };
        validate_processing_config(&config)?;
        config.resources.scratch_path = resolve_path(&root, &config.resources.scratch_path);
        if config.resources.scratch_path.exists() && !config.resources.scratch_path.is_dir() {
            return invalid("resources.scratch_path: existing path must be a directory");
        }
        Ok((config, document.merge))
    })();
    result.map_err(|e| {
        MarketForgeError::InvalidConfiguration(format!(
            "historical configuration {}: {e}",
            path.display()
        ))
    })
}

pub fn validate_processing_config(config: &ProcessingConfig) -> Result<()> {
    if !(100..=60_000).contains(&config.diagnostics.interval_ms) {
        return invalid("diagnostics.interval_ms: must be between 100 and 60000");
    }
    if !(2..=64).contains(&config.merge_fan_in) {
        return invalid("merge_fan_in: must be between 2 and 64");
    }
    super::policies::validate(&config.reconciliation, &config.timestamp_regression)?;
    validate_resources(&config.resources)?;
    validate_integrity_policy(&config.integrity_policy)
}

pub use super::{validate_integrity_policy, validate_resources};

fn invalid<T>(message: &str) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.to_owned()))
}

#[cfg(test)]
mod tests;
