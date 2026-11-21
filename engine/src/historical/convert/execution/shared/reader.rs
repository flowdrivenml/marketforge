use crate::historical::convert::progress::ConversionProgressReporter;
use arrow_array::{Array, Int64Array, RecordBatch, UInt32Array, UInt64Array};
use arrow_schema::SchemaRef;
use parquet::arrow::{
    ProjectionMask,
    arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
};
use std::{
    fs::File,
    path::{Path, PathBuf},
};
use {
    crate::historical::merge::PhysicalMergeManifest, crate::shared::error::MarketForgeError,
    crate::shared::error::Result,
};

use crate::historical::convert::reference::ReferenceKey;

pub const CONVERSION_READ_BATCH_SIZE: usize = 65_536;

// -----------------------------------------------------------------------------
// Physical conversion reader
// -----------------------------------------------------------------------------
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReaderProjection {
    Full,
    ReferencePrepass,
}
pub struct PhysicalConversionReader {
    files: Vec<PathBuf>,
    file_index: usize,

    reader: Option<ParquetRecordBatchReader>,
    schema: Option<SchemaRef>,

    batch_size: usize,
    projection: ReaderProjection,
    last_key: Option<ReferenceKey>,
    causal: bool,
    mode: crate::historical::merge::UnifiedMergeMode,
    range: crate::historical::job::MergeTimeRange,

    records_read: u64,
    files_opened: u64,
}

impl PhysicalConversionReader {
    pub fn new(
        dataset_path: &Path,
        manifest: &PhysicalMergeManifest,
        batch_size: usize,
    ) -> Result<Self> {
        Self::new_with_projection(dataset_path, manifest, batch_size, ReaderProjection::Full)
    }

    pub fn new_reference_prepass(
        dataset_path: &Path,
        manifest: &PhysicalMergeManifest,
        batch_size: usize,
    ) -> Result<Self> {
        Self::new_with_projection(
            dataset_path,
            manifest,
            batch_size,
            ReaderProjection::ReferencePrepass,
        )
    }

    fn new_with_projection(
        dataset_path: &Path,
        manifest: &PhysicalMergeManifest,
        batch_size: usize,
        projection: ReaderProjection,
    ) -> Result<Self> {
        if batch_size == 0 {
            return invalid("conversion batch size must be positive");
        }

        manifest.validate()?;

        let files = &manifest.files;

        let files = files
            .iter()
            .map(|file| dataset_path.join(&file.path))
            .collect();

        Ok(Self {
            files,
            file_index: 0,
            reader: None,
            schema: None,
            batch_size,
            projection,
            last_key: None,
            causal: manifest.timeline_ordering
                == crate::historical::job::MergeTimelineOrdering::Causal,
            range: manifest.time_range,
            mode: manifest.mode,
            records_read: 0,
            files_opened: 0,
        })
    }

    // -------------------------------------------------------------------------
    // Read next Arrow batch
    // -------------------------------------------------------------------------

    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if let Some(reader) = &mut self.reader {
                match reader.next() {
                    Some(Ok(batch)) => {
                        if batch.num_rows() == 0 {
                            continue;
                        }

                        self.validate_batch(&batch)?;

                        self.records_read = self
                            .records_read
                            .checked_add(batch.num_rows() as u64)
                            .ok_or_else(|| invalid_error("conversion row counter overflow"))?;

                        return Ok(Some(batch));
                    }

                    Some(Err(error)) => {
                        return invalid(format!("conversion Parquet read failed: {error}"));
                    }

                    None => {
                        self.reader = None;
                    }
                }
            }

            if self.file_index >= self.files.len() {
                return Ok(None);
            }

            self.open_next_file()?;
        }
    }

    // -------------------------------------------------------------------------
    // Open next physical Parquet file
    // -------------------------------------------------------------------------

    fn open_next_file(&mut self) -> Result<()> {
        let path = &self.files[self.file_index];

        let file = File::open(path).map_err(|error| {
            invalid_error(format!(
                "cannot open conversion Parquet {}: {error}",
                path.display()
            ))
        })?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
            invalid_error(format!(
                "cannot inspect conversion Parquet {}: {error}",
                path.display()
            ))
        })?;

        let schema = builder.schema().clone();

        if let Some(expected) = &self.schema {
            if schema != *expected {
                return invalid(format!("conversion schema mismatch: {}", path.display()));
            }
        } else {
            self.schema = Some(schema);
        }

        let builder = match self.projection {
            ReaderProjection::Full => builder,

            ReaderProjection::ReferencePrepass => {
                let mut required_columns = vec![
                    "event_timestamp_ns",
                    "event_kind",
                    "stream_id",
                    "stream_rank",
                    "dataset_id",
                    "task_id",
                    "event_ordinal",
                    "trade.price",
                ];
                if self.causal {
                    required_columns.extend(["timeline_sequence", "reconstruction_only"]);
                }

                let schema_descriptor = builder.parquet_schema();

                let leaf_indices = schema_descriptor
                    .columns()
                    .iter()
                    .enumerate()
                    .filter_map(|(index, column)| {
                        let path = column.path().string();

                        required_columns.contains(&path.as_str()).then_some(index)
                    })
                    .collect::<Vec<_>>();

                if leaf_indices.len() != required_columns.len() {
                    return invalid(format!(
                        "reference prepass projection mismatch: \
                 expected {} columns, found {}",
                        required_columns.len(),
                        leaf_indices.len(),
                    ));
                }

                let mask = ProjectionMask::leaves(schema_descriptor, leaf_indices);

                builder.with_projection(mask)
            }
        };

        let reader = builder
            .with_batch_size(self.batch_size)
            .build()
            .map_err(|error| {
                invalid_error(format!("cannot construct conversion reader: {error}"))
            })?;

        self.reader = Some(reader);

        self.file_index += 1;
        self.files_opened += 1;

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Validate deterministic ordering
    // -------------------------------------------------------------------------

    fn validate_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.causal {
            return validate_causal_batch(batch, self.records_read, self.mode, &self.range);
        }
        let timestamps = column::<Int64Array>(batch, "event_timestamp_ns")?;
        let ranks = column::<UInt32Array>(batch, "stream_rank")?;
        let tasks = column::<UInt64Array>(batch, "task_id")?;
        let ordinals = column::<UInt64Array>(batch, "event_ordinal")?;

        for row in 0..batch.num_rows() {
            if timestamps.is_null(row)
                || ranks.is_null(row)
                || tasks.is_null(row)
                || ordinals.is_null(row)
            {
                return invalid("null conversion ordering field");
            }

            let key = ReferenceKey::new(
                timestamps.value(row),
                ranks.value(row),
                tasks.value(row),
                ordinals.value(row),
            );

            if let Some(previous) = self.last_key {
                if key < previous {
                    return invalid(format!(
                        "conversion timeline ordering regression: \
                         previous={previous:?}, current={key:?}"
                    ));
                }
            }

            self.last_key = Some(key);
        }

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Metrics
    // -------------------------------------------------------------------------

    pub fn records_read(&self) -> u64 {
        self.records_read
    }

    pub fn files_opened(&self) -> u64 {
        self.files_opened
    }

    pub fn schema(&self) -> Option<&SchemaRef> {
        self.schema.as_ref()
    }
}

// -----------------------------------------------------------------------------
// Arrow column access
// -----------------------------------------------------------------------------

fn column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| invalid_error(format!("missing conversion column: {name}")))?;

    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| invalid_error(format!("invalid conversion column type: {name}")))
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

pub(super) fn validate_causal_batch(
    batch: &RecordBatch,
    start: u64,
    mode: crate::historical::merge::UnifiedMergeMode,
    range: &crate::historical::job::MergeTimeRange,
) -> Result<()> {
    use crate::historical::merge::UnifiedMergeMode;
    let sequences = column::<UInt64Array>(batch, "timeline_sequence")?;
    let reconstruction = column::<arrow_array::BooleanArray>(batch, "reconstruction_only")?;
    let timestamps = column::<Int64Array>(batch, "event_timestamp_ns")?;
    let kinds = if mode == UnifiedMergeMode::Combined {
        Some(column::<arrow_array::UInt8Array>(batch, "event_kind")?)
    } else {
        None
    };
    for row in 0..batch.num_rows() {
        let timestamp = timestamps.value(row);
        let outside = timestamp < range.start_ns || timestamp >= range.end_ns;
        let depth = mode == UnifiedMergeMode::Depth
            || kinds.is_some_and(|kinds| !kinds.is_null(row) && kinds.value(row) == 1);
        if sequences.is_null(row)
            || reconstruction.is_null(row)
            || timestamps.is_null(row)
            || sequences.value(row) != start + row as u64
            || reconstruction.value(row) != outside
            || (outside && !depth)
        {
            return invalid("invalid causal conversion sequence or reconstruction marker");
        }
    }
    Ok(())
}

/// Check the input inventory and Parquet footers without scanning market rows.
pub fn validate_conversion_input(directory: &Path, manifest: &PhysicalMergeManifest) -> Result<()> {
    use crate::historical::{job::MergeTimelineOrdering, merge::*};

    manifest.validate()?;
    let mut schema = match manifest.mode {
        UnifiedMergeMode::Trades => physical_trade_schema(),
        UnifiedMergeMode::Depth => physical_depth_schema(),
        UnifiedMergeMode::Combined => physical_combined_schema(),
    };
    if manifest.timeline_ordering == MergeTimelineOrdering::Causal {
        schema = physical_causal_schema(&schema);
    }
    let mut row_groups = 0;
    for entry in &manifest.files {
        if !Path::new(&entry.path)
            .components()
            .all(|component| matches!(component, std::path::Component::Normal(_)))
        {
            return invalid(format!("invalid conversion inventory path: {}", entry.path));
        }
        let path = directory.join(&entry.path);
        let file = File::open(&path)
            .map_err(|error| invalid_error(format!("cannot open {}: {error}", path.display())))?;
        let size = file
            .metadata()
            .map_err(|error| invalid_error(error.to_string()))?
            .len();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
            invalid_error(format!("cannot read {} footer: {error}", path.display()))
        })?;
        if size != entry.size_bytes
            || builder.schema().fields() != schema.fields()
            || u64::try_from(builder.metadata().file_metadata().num_rows()).ok() != entry.rows
        {
            return invalid(format!(
                "conversion source schema or inventory mismatch: {}",
                path.display()
            ));
        }
        row_groups += builder.metadata().num_row_groups() as u64;
    }
    if row_groups != manifest.row_groups {
        return invalid("conversion source row group inventory mismatch");
    }
    Ok(())
}
/// Establish the largest backwards timestamp distance without reading prices or reordering rows.
pub(crate) fn initial_reference_state(
    directory: &Path,
    manifest: &PhysicalMergeManifest,
    reporter: &ConversionProgressReporter,
) -> Result<crate::historical::convert::reference::ReferenceState> {
    initial_reference_state_for_prefix(directory, manifest, reporter, None)
}

pub(crate) fn initial_reference_state_for_prefix(
    directory: &Path,
    manifest: &PhysicalMergeManifest,
    reporter: &ConversionProgressReporter,
    max_events: Option<u64>,
) -> Result<crate::historical::convert::reference::ReferenceState> {
    scan_timestamp_regression(directory, manifest, reporter, max_events, false)
}

pub(crate) fn initial_reference_state_with_scan_progress(
    directory: &Path,
    manifest: &PhysicalMergeManifest,
    reporter: &ConversionProgressReporter,
) -> Result<crate::historical::convert::reference::ReferenceState> {
    scan_timestamp_regression(directory, manifest, reporter, None, reporter.is_enabled())
}

fn scan_timestamp_regression(
    directory: &Path,
    manifest: &PhysicalMergeManifest,
    reporter: &ConversionProgressReporter,
    max_events: Option<u64>,
    report_scan: bool,
) -> Result<crate::historical::convert::reference::ReferenceState> {
    use crate::historical::convert::reference::ReferenceState;
    if manifest.timeline_ordering != crate::historical::job::MergeTimelineOrdering::Causal {
        return Ok(ReferenceState::new());
    }
    use crate::historical::convert::progress::ConversionProgressEvent;
    use std::time::{Duration, Instant};

    let expected = manifest.events_written.min(max_events.unwrap_or(u64::MAX));
    let mut scanned = 0;
    let mut last_progress = report_scan.then(Instant::now);
    if report_scan {
        reporter.emit(ConversionProgressEvent::TimestampScanProgress { scanned, expected });
    }
    let mut peak = i64::MIN;
    let mut regression_ns = 0;
    let mut remaining = max_events.unwrap_or(u64::MAX);
    let batch_size = remaining.min(CONVERSION_READ_BATCH_SIZE as u64) as usize;
    'files: for file in &manifest.files {
        let file = std::fs::File::open(directory.join(&file.path))
            .map_err(|error| invalid_error(error.to_string()))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|error| invalid_error(error.to_string()))?;
        let index = builder
            .schema()
            .index_of("event_timestamp_ns")
            .map_err(|error| invalid_error(error.to_string()))?;
        let mask = ProjectionMask::roots(builder.parquet_schema(), [index]);
        let reader = builder
            .with_projection(mask)
            .with_batch_size(batch_size)
            .build()
            .map_err(|error| invalid_error(error.to_string()))?;
        for batch in reader {
            reporter.check_interrupted()?;
            let batch = batch.map_err(|error| invalid_error(error.to_string()))?;
            if report_scan {
                scanned += remaining.min(batch.num_rows() as u64);
            }
            let timestamps = column::<Int64Array>(&batch, "event_timestamp_ns")?;
            for timestamp in timestamps
                .iter()
                .take(usize::try_from(remaining).unwrap_or(usize::MAX))
            {
                let timestamp = timestamp.ok_or_else(|| invalid_error("null causal timestamp"))?;
                peak = peak.max(timestamp);
                regression_ns = regression_ns.max(peak.abs_diff(timestamp));
                remaining -= 1;
                if remaining == 0 {
                    break 'files;
                }
            }
            if last_progress.is_some_and(|last| last.elapsed() >= Duration::from_millis(500)) {
                reporter.emit(ConversionProgressEvent::TimestampScanProgress { scanned, expected });
                last_progress = Some(Instant::now());
            }
        }
    }
    if report_scan {
        reporter.emit(ConversionProgressEvent::TimestampScanProgress { scanned, expected });
    }
    Ok(ReferenceState::with_regression_window(regression_ns))
}
