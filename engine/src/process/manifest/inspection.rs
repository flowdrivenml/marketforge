use std::{
    fs::{self, File},
    path::Path,
};

use parquet::file::{
    reader::{FileReader, SerializedFileReader},
    statistics::Statistics,
};

use crate::error::{MarketForgeError, Result};

use super::models::DatasetFile;

// -----------------------------------------------------------------------------
// Canonical Parquet file inspection
// -----------------------------------------------------------------------------

pub(super) fn inspect_parquet_file(dataset_root: &Path, path: &Path) -> Result<DatasetFile> {
    // -------------------------------------------------------------------------
    // Filesystem metadata
    // -------------------------------------------------------------------------

    let metadata = fs::metadata(path).map_err(|error| {
        MarketForgeError::InvalidConfiguration(format!(
            "failed to inspect Parquet file {}: {error}",
            path.display()
        ))
    })?;

    // -------------------------------------------------------------------------
    // Open Parquet file
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // Canonical row count
    // -------------------------------------------------------------------------

    let rows = u64::try_from(parquet_metadata.file_metadata().num_rows()).map_err(|_| {
        MarketForgeError::InvalidConfiguration("negative Parquet row count".to_owned())
    })?;

    // -------------------------------------------------------------------------
    // Timestamp statistics
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // Validate timestamp statistics
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // Relative dataset path
    // -------------------------------------------------------------------------

    let relative_path = path
        .strip_prefix(dataset_root)
        .map_err(|error| {
            MarketForgeError::InvalidConfiguration(format!("invalid dataset file path: {error}"))
        })?
        .to_string_lossy()
        .replace('\\', "/");

    // -------------------------------------------------------------------------
    // Construct file metadata
    // -------------------------------------------------------------------------

    Ok(DatasetFile {
        path: relative_path,
        rows,
        size_bytes: metadata.len(),
        start_timestamp_ns,
        end_timestamp_ns,
    })
}
