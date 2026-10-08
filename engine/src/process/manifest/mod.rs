use std::{
    fs::{self, File},
    path::{Path, PathBuf},
};

use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    file::statistics::Statistics,
};

use serde::{Deserialize, Serialize};

use crate::{
    error::{MarketForgeError, Result},
    job::{ContentType, ProcessingJob},
};

pub const DATASET_MANIFEST_VERSION: u32 = 1;
pub const DATASET_MANIFEST_FILENAME: &str = "manifest.json";

// -----------------------------------------------------------------------------
// Manifest models
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetFile {
    pub path: String,
    pub rows: u64,
    pub size_bytes: u64,

    pub start_timestamp_ns: Option<i64>,
    pub end_timestamp_ns: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetManifest {
    pub protocol_version: u32,

    pub job_id: u64,
    pub dataset_id: u64,

    pub content_type: ContentType,

    pub events_written: u64,
    pub files_written: u64,

    pub start_timestamp_ns: Option<i64>,
    pub end_timestamp_ns: Option<i64>,

    pub source_tasks: Vec<u64>,
    pub files: Vec<DatasetFile>,
}

// -----------------------------------------------------------------------------
// Manifest construction
// -----------------------------------------------------------------------------

impl DatasetManifest {
    pub fn from_processing_job(job: &ProcessingJob, dataset_root: &Path) -> Result<Self> {
        let files = inspect_parquet_files(dataset_root)?;

        let events_written = files.iter().try_fold(0u64, |total, file| {
            total.checked_add(file.rows).ok_or_else(|| {
                MarketForgeError::InvalidConfiguration("dataset row count overflow".to_owned())
            })
        })?;

        let files_written = files.len() as u64;

        let start_timestamp_ns = files
            .iter()
            .filter_map(|file| file.start_timestamp_ns)
            .min();

        let end_timestamp_ns = files.iter().filter_map(|file| file.end_timestamp_ns).max();

        Ok(Self {
            protocol_version: DATASET_MANIFEST_VERSION,

            job_id: job.job_id.0,
            dataset_id: job.dataset_id.0,

            content_type: job.output.content_type,

            events_written,
            files_written,

            start_timestamp_ns,
            end_timestamp_ns,

            source_tasks: job.tasks.iter().map(|task| task.task_id.0).collect(),

            files,
        })
    }

    pub fn write_to(&self, dataset_root: &Path) -> Result<PathBuf> {
        let path = dataset_root.join(DATASET_MANIFEST_FILENAME);

        if path.exists() {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "dataset manifest already exists: {}",
                path.display()
            )));
        }

        let contents = serde_json::to_vec_pretty(self).map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to serialize dataset manifest: {error}"
            ))
        })?;

        fs::write(&path, contents).map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to write dataset manifest {}: {error}",
                path.display()
            ))
        })?;

        Ok(path)
    }
}

// -----------------------------------------------------------------------------
// Parquet file discovery and inspection
// -----------------------------------------------------------------------------

pub fn inspect_parquet_files(dataset_root: &Path) -> Result<Vec<DatasetFile>> {
    let trades_dir = dataset_root.join("trades");

    if !trades_dir.is_dir() {
        return Err(MarketForgeError::InvalidConfiguration(format!(
            "dataset trades directory does not exist: {}",
            trades_dir.display()
        )));
    }

    let mut paths: Vec<PathBuf> = fs::read_dir(&trades_dir)
        .map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!(
                "failed to read Parquet directory {}: {error}",
                trades_dir.display()
            ))
        })?
        .map(|entry| {
            entry.map(|entry| entry.path()).map_err(|error| {
                MarketForgeError::InvalidConfiguration(format!(
                    "failed to read Parquet directory entry: {error}"
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;

    paths.sort();

    let mut files = Vec::new();

    for path in paths {
        if path.extension().and_then(|ext| ext.to_str()) != Some("parquet") {
            continue;
        }

        files.push(inspect_parquet_file(dataset_root, &path)?);
    }

    if files.is_empty() {
        return Err(MarketForgeError::InvalidConfiguration(
            "dataset contains no Parquet files".to_owned(),
        ));
    }

    Ok(files)
}

fn inspect_parquet_file(dataset_root: &Path, path: &Path) -> Result<DatasetFile> {
    let metadata = fs::metadata(path).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "failed to inspect Parquet file {}: {error}",
            path.display()
        ))
    })?;

    let file = File::open(path).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "failed to open Parquet file {}: {error}",
            path.display()
        ))
    })?;

    let reader = SerializedFileReader::new(file).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "invalid Parquet file {}: {error}",
            path.display()
        ))
    })?;

    let parquet_metadata = reader.metadata();

    let rows = u64::try_from(parquet_metadata.file_metadata().num_rows()).map_err(|_| {
        MarketForgeError::InvalidConfiguration("negative Parquet row count".to_owned())
    })?;

    let mut start_timestamp_ns = None::<i64>;
    let mut end_timestamp_ns = None::<i64>;

    for row_group in parquet_metadata.row_groups() {
        let timestamp_column = row_group
            .columns()
            .iter()
            .find(|column| column.column_path().string() == "event_timestamp_ns")
            .ok_or_else(|| {
                MarketForgeError::InvalidConfiguration(format!(
                    "Parquet file missing event_timestamp_ns column: {}",
                    path.display()
                ))
            })?;

        if let Some(Statistics::Int64(stats)) = timestamp_column.statistics() {
            if let Some(minimum) = stats.min_opt() {
                start_timestamp_ns =
                    Some(start_timestamp_ns.map_or(*minimum, |current| current.min(*minimum)));
            }

            if let Some(maximum) = stats.max_opt() {
                end_timestamp_ns =
                    Some(end_timestamp_ns.map_or(*maximum, |current| current.max(*maximum)));
            }
        }
    }

    if rows > 0 && (start_timestamp_ns.is_none() || end_timestamp_ns.is_none()) {
        return Err(MarketForgeError::InvalidConfiguration(format!(
            "Parquet file {} contains {} rows but has incomplete timestamp statistics",
            path.display(),
            rows,
        )));
    }

    if let (Some(start), Some(end)) = (start_timestamp_ns, end_timestamp_ns) {
        if start > end {
            return Err(MarketForgeError::InvalidConfiguration(format!(
                "Parquet file {} has invalid timestamp boundaries",
                path.display(),
            )));
        }
    }

    let relative_path = path
        .strip_prefix(dataset_root)
        .map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!("invalid dataset file path: {error}"))
        })?
        .to_string_lossy()
        .replace('\\', "/");

    Ok(DatasetFile {
        path: relative_path,
        rows,
        size_bytes: metadata.len(),
        start_timestamp_ns,
        end_timestamp_ns,
    })
}
