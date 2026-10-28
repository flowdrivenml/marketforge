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

        let mut files = manifest.files.clone();

        files.sort_by(|a, b| (a.partition_id, &a.path).cmp(&(b.partition_id, &b.path)));

        let files = files
            .into_iter()
            .map(|file| dataset_path.join(file.path))
            .collect();

        Ok(Self {
            files,
            file_index: 0,
            reader: None,
            schema: None,
            batch_size,
            projection,
            last_key: None,
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
                let required_columns = [
                    "event_timestamp_ns",
                    "event_kind",
                    "stream_id",
                    "stream_rank",
                    "dataset_id",
                    "task_id",
                    "event_ordinal",
                    "trade.price",
                ];

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
