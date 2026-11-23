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
pub const DEFAULT_NORMALIZE_CONFIG: &str = "data/.config/engine/normalize.toml";
pub const DEFAULT_MERGE_CONFIG: &str = "data/.config/engine/merge.toml";
pub const DEFAULT_PROFILES_ROOT: &str = "data/.config/engine/normalize";

#[derive(Debug, Deserialize, Serialize)]
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
    merge: Option<MergeRuntime>,
    resources: ResourceConfig,
    integrity_policy: IntegrityPolicy,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
struct MergeRuntime {
    partitions_per_worker: usize,
    compression_level: i32,
}

impl Default for MergeRuntime {
    fn default() -> Self {
        Self {
            partitions_per_worker: 4,
            compression_level: default_compression_level(),
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
    let (resources, parquet, partitioning) = load_io_document(path, root, true)?;
    let partitioning = partitioning.unwrap_or(MergePartitioning {
        partitions_per_worker: MergeRuntime::default().partitions_per_worker,
    });
    if partitioning.partitions_per_worker == 0 {
        return invalid(&format!(
            "{}: partitioning.partitions_per_worker must be positive",
            path.display()
        ));
    }
    if resources
        .workers
        .checked_mul(partitioning.partitions_per_worker)
        .is_none()
    {
        return invalid(&format!(
            "{}: merge partition count overflow",
            path.display()
        ));
    }
    Ok(MergeRuntimeSettings {
        resources,
        partitioning,
        parquet,
    })
}

#[derive(Serialize)]
pub struct ConversionRuntimeSettings {
    pub resources: crate::historical::job::MergeResources,
    pub parquet: crate::historical::job::MergeParquetConfig,
}

pub fn load_conversion_runtime_settings(
    path: &Path,
    root: &Path,
) -> Result<(
    crate::historical::job::MergeResources,
    crate::historical::job::MergeParquetConfig,
)> {
    let (resources, parquet, _) = load_io_document(path, root, false)?;
    Ok((resources, parquet))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IoDocument {
    schema_version: u32,
    resources: IoResources,
    parquet: IoParquet,
    partitioning: Option<crate::historical::job::MergePartitioning>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IoResources {
    workers: usize,
    memory_limit_mb: u64,
    #[serde(default = "default_open_files")]
    max_open_files: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IoParquet {
    row_group_target_mb: u64,
    file_target_mb: u64,
    #[serde(default = "default_compression_level")]
    compression_level: i32,
}

fn default_open_files() -> usize {
    64
}

fn default_compression_level() -> i32 {
    3
}

fn migrated_runtime_path(root: &Path, path: &Path) -> Result<()> {
    if !path.exists()
        && [
            DEFAULT_NORMALIZE_CONFIG,
            DEFAULT_MERGE_CONFIG,
            "data/.config/engine/convert.toml",
        ]
        .iter()
        .any(|default| path == root.join(default))
    {
        let legacy = root.join("data/.config/historical/general.toml");
        if legacy.is_file() {
            return invalid(&format!(
                "configuration migration required: {} → {}",
                legacy.display(),
                path.display()
            ));
        }
    }
    Ok(())
}

fn load_io_document(
    path: &Path,
    root: &Path,
    merge: bool,
) -> Result<(
    crate::historical::job::MergeResources,
    crate::historical::job::MergeParquetConfig,
    Option<crate::historical::job::MergePartitioning>,
)> {
    let root = project_root(root)?;
    let path = resolve_path(&root, path);
    migrated_runtime_path(&root, &path)?;
    let result = (|| {
        let contents = fs::read_to_string(&path)
            .map_err(|e| MarketForgeError::InvalidConfiguration(e.to_string()))?;
        let document: toml::Value =
            toml::from_str(&contents).map_err(|e| toml_error(&contents, &e))?;
        // Retained v1 operations used a shared normalization-shaped document.
        if path.file_name().is_some_and(|name| name == "general.toml")
            && document.get("integrity_policy").is_some()
        {
            let (config, old_merge) = load_runtime(&path, &root)?;
            let compression = if merge {
                old_merge.compression_level
            } else {
                3
            };
            let (resources, parquet) = runtime_io_settings(config.resources, compression)?;
            let partitioning = merge.then_some(crate::historical::job::MergePartitioning {
                partitions_per_worker: old_merge.partitions_per_worker,
            });
            return Ok((resources, parquet, partitioning));
        }
        let document: IoDocument =
            toml::from_str(&contents).map_err(|e| toml_error(&contents, &e))?;
        if document.schema_version != RUNTIME_SCHEMA_VERSION {
            return invalid("schema_version: expected 1");
        }
        if !merge && document.partitioning.is_some() {
            return invalid("partitioning: supported only for merge");
        }
        let resources = crate::historical::job::MergeResources {
            workers: document.resources.workers,
            memory_limit_mb: document.resources.memory_limit_mb,
            max_open_files: document.resources.max_open_files,
        };
        let parquet = crate::historical::job::MergeParquetConfig {
            row_group_target_mb: document.parquet.row_group_target_mb,
            file_target_mb: document.parquet.file_target_mb,
            compression_level: document.parquet.compression_level,
        };
        Ok((resources, parquet, document.partitioning))
    })();
    let (resources, parquet, partitioning) = result.map_err(|e| {
        MarketForgeError::InvalidConfiguration(format!(
            "runtime configuration {}: {e}",
            path.display()
        ))
    })?;
    let field = if resources.workers == 0 {
        "resources.workers must be positive"
    } else if resources.memory_limit_mb == 0
        || resources.memory_limit_mb.checked_mul(1_048_576).is_none()
    {
        "resources.memory_limit_mb must be positive and fit in bytes"
    } else if resources.max_open_files == 0 {
        "resources.max_open_files must be positive"
    } else if parquet.row_group_target_mb == 0
        || parquet.row_group_target_mb > parquet.file_target_mb
    {
        "parquet.row_group_target_mb must be positive and no greater than file_target_mb"
    } else if parquet.file_target_mb.checked_mul(1_048_576).is_none() {
        "parquet.file_target_mb must fit in bytes"
    } else if !(1..=22).contains(&parquet.compression_level) {
        "parquet.compression_level must be between 1 and 22"
    } else {
        return Ok((resources, parquet, partitioning));
    };
    invalid(&format!("{}: {field}", path.display()))
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
    migrated_runtime_path(&root, &path)?;
    let result = (|| -> Result<(ProcessingConfig, MergeRuntime)> {
        let contents = fs::read_to_string(&path)
            .map_err(|e| MarketForgeError::InvalidConfiguration(e.to_string()))?;
        let document: RuntimeDocument =
            toml::from_str(&contents).map_err(|e| toml_error(&contents, &e))?;
        if document.schema_version != RUNTIME_SCHEMA_VERSION {
            return invalid(&format!(
                "schema_version: unsupported version {}; expected {RUNTIME_SCHEMA_VERSION}",
                document.schema_version
            ));
        }
        if document.merge.is_some() && path.file_name().is_none_or(|name| name != "general.toml") {
            return invalid("merge: supported only in retained legacy general.toml");
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
        Ok((config, document.merge.unwrap_or_default()))
    })();
    result.map_err(|e| {
        MarketForgeError::InvalidConfiguration(format!(
            "historical configuration {}: {e}",
            path.display()
        ))
    })
}

const DIAGNOSTICS_INTERVAL_MS: std::ops::RangeInclusive<u64> = 100..=60_000;
const MERGE_FAN_IN: std::ops::RangeInclusive<usize> = 2..=64;

pub fn validate_processing_config(config: &ProcessingConfig) -> Result<()> {
    if !DIAGNOSTICS_INTERVAL_MS.contains(&config.diagnostics.interval_ms) {
        return invalid(&format!(
            "diagnostics.interval_ms: must be between {} and {}",
            DIAGNOSTICS_INTERVAL_MS.start(),
            DIAGNOSTICS_INTERVAL_MS.end()
        ));
    }
    if !MERGE_FAN_IN.contains(&config.merge_fan_in) {
        return invalid(&format!(
            "merge_fan_in: must be between {} and {}",
            MERGE_FAN_IN.start(),
            MERGE_FAN_IN.end()
        ));
    }
    super::policies::validate(&config.reconciliation, &config.timestamp_regression)?;
    validate_resources(&config.resources)?;
    validate_integrity_policy(&config.integrity_policy)
}

pub use super::{validate_integrity_policy, validate_resources};

fn toml_error(contents: &str, error: &toml::de::Error) -> MarketForgeError {
    let location = error.span().and_then(|span| {
        let (before, after) = contents.split_at_checked(span.start)?;
        let line = before.bytes().filter(|&byte| byte == b'\n').count() + 1;
        let prefix = before.rsplit('\n').next().unwrap_or_default();
        let suffix = after.split('\n').next().unwrap_or_default();
        let source_line = format!("{prefix}{suffix}");
        let field = source_line.split_once('=').map(|(field, _)| field.trim());
        Some(match field {
            Some(field)
                if !field.is_empty()
                    && field.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')
                    }) =>
            {
                format!("line {line}, {field}: ")
            }
            _ => format!("line {line}: "),
        })
    });
    let message = error.message();
    let message = if message.starts_with("invalid type:") || message.starts_with("invalid value:") {
        message.rsplit_once(", expected ").map_or_else(
            || "invalid field type or value".to_owned(),
            |(_, expected)| format!("invalid field type or value, expected {expected}"),
        )
    } else {
        message.to_owned()
    };
    MarketForgeError::InvalidConfiguration(format!("{}{message}", location.unwrap_or_default()))
}

fn invalid<T>(message: &str) -> Result<T> {
    Err(MarketForgeError::InvalidConfiguration(message.to_owned()))
}

#[cfg(test)]
mod tests;

mod schema;
pub use schema::runtime_schema;

/// Authoring metadata for the merge-specific v1 document.
pub fn merge_runtime_schema() -> serde_json::Value {
    io_runtime_schema(true)
}

pub fn conversion_runtime_schema() -> serde_json::Value {
    io_runtime_schema(false)
}

fn io_runtime_schema(merge: bool) -> serde_json::Value {
    use serde_json::{Value, json};
    let field = |path, kind, bits, required, default: Value, purpose, constraint| {
        json!({
            "path":path, "kind":kind, "integer_bits":bits, "required":required, "default":default,
            "purpose":purpose, "constraint":constraint, "mutable":path != "schema_version",
            "risk":if path == "schema_version" { "ordinary" } else { "resource-sensitive" },
            "choices":[], "dependencies":[], "omit_empty_parent":path == "partitioning.partitions_per_worker"
        })
    };
    let mut fields = vec![
        field(
            "schema_version",
            "integer",
            u32::BITS,
            true,
            Value::Null,
            "Versions the base runtime document.",
            "Must match the Engine runtime schema version",
        ),
        field(
            "resources.workers",
            "integer",
            usize::BITS,
            true,
            Value::Null,
            "Requested worker concurrency. Admission can reduce effective parallelism.",
            if merge {
                "Positive; worker count times partitions per worker must fit usize"
            } else {
                "Positive"
            },
        ),
        field(
            "resources.memory_limit_mb",
            "mib",
            u64::BITS,
            true,
            Value::Null,
            "Memory budget in binary MiB for admitted work.",
            "Positive and must fit u64 when converted to bytes",
        ),
        field(
            "resources.max_open_files",
            "integer",
            usize::BITS,
            false,
            json!(default_open_files()),
            "Budget for participating file handles.",
            "Positive",
        ),
        field(
            "parquet.row_group_target_mb",
            "mib",
            u64::BITS,
            true,
            Value::Null,
            "Target row group size in binary MiB.",
            "Positive and no greater than parquet.file_target_mb",
        ),
        field(
            "parquet.file_target_mb",
            "mib",
            u64::BITS,
            true,
            Value::Null,
            "Part rotation target in binary MiB.",
            "At least the row group target and must fit u64 when converted to bytes",
        ),
        field(
            "parquet.compression_level",
            "integer",
            i32::BITS - 1,
            false,
            json!(default_compression_level()),
            "Parquet Zstandard compression level.",
            "1 to 22 inclusive",
        ),
    ];
    if merge {
        fields.push(field(
            "partitioning.partitions_per_worker",
            "integer",
            usize::BITS,
            false,
            json!(MergeRuntime::default().partitions_per_worker),
            "Requested partitions per worker. The runtime uses its default when the partitioning table is absent.",
            "Positive; product with resources.workers must fit usize",
        ));
    }
    let kind = if merge {
        "merge_runtime_schema"
    } else {
        "convert_runtime_schema"
    };
    json!({"schema_version":1,"kind":kind,"runtime_schema_version":RUNTIME_SCHEMA_VERSION,"fields":fields})
}
