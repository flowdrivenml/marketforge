use std::{
    fs::File,
    path::{Path, PathBuf},
};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use parquet::arrow::{
    ProjectionMask,
    arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder},
};

use {
    crate::historical::merge::PhysicalMergeManifest, crate::shared::error::MarketForgeError,
    crate::shared::error::Result,
};

use {
    crate::historical::convert::execution::shared::partition::ConversionPartition,
    crate::historical::convert::execution::shared::partition::ConversionPartitionPlan,
};

// -----------------------------------------------------------------------------
// Partition-specific Parquet reader
// -----------------------------------------------------------------------------

pub struct ConversionPartitionReader {
    files: Vec<PartitionFile>,

    file_index: usize,
    reader: Option<ParquetRecordBatchReader>,

    schema: Option<SchemaRef>,
    batch_size: usize,
    causal_start: Option<u64>,
    mode: crate::historical::merge::UnifiedMergeMode,
    range: crate::historical::job::MergeTimeRange,
    timestamps_only: bool,

    expected_rows: u64,
    rows_read: u64,
    files_opened: u64,
}

struct PartitionFile {
    path: PathBuf,
    row_groups: Vec<usize>,
}

impl ConversionPartitionReader {
    pub fn new(
        dataset_path: &Path,
        manifest: &PhysicalMergeManifest,
        plan: &ConversionPartitionPlan,
        partition: &ConversionPartition,
        batch_size: usize,
    ) -> Result<Self> {
        Self::new_with_projection(dataset_path, manifest, plan, partition, batch_size, false)
    }

    pub(crate) fn timestamps(
        dataset_path: &Path,
        manifest: &PhysicalMergeManifest,
        plan: &ConversionPartitionPlan,
        partition: &ConversionPartition,
        batch_size: usize,
    ) -> Result<Self> {
        Self::new_with_projection(dataset_path, manifest, plan, partition, batch_size, true)
    }

    fn new_with_projection(
        dataset_path: &Path,
        manifest: &PhysicalMergeManifest,
        plan: &ConversionPartitionPlan,
        partition: &ConversionPartition,
        batch_size: usize,
        timestamps_only: bool,
    ) -> Result<Self> {
        if batch_size == 0 {
            return invalid("partition reader batch size must be positive");
        }

        manifest.validate()?;

        if partition.rows == 0
            || partition.first_row_group >= partition.end_row_group
            || partition.end_row_group > plan.row_groups.len()
        {
            return invalid("invalid conversion partition boundaries");
        }

        let manifest_files = &manifest.files;

        // -------------------------------------------------------------
        // Group selected row groups by physical file
        // -------------------------------------------------------------

        let mut files = Vec::<PartitionFile>::new();

        for group in &plan.row_groups[partition.first_row_group..partition.end_row_group] {
            let entry = manifest_files
                .get(group.file_index)
                .ok_or_else(|| invalid_error("partition references unknown file"))?;

            let path = dataset_path.join(&entry.path);

            if let Some(last) = files.last_mut() {
                if last.path == path {
                    last.row_groups.push(group.row_group_index);
                    continue;
                }
            }

            files.push(PartitionFile {
                path,
                row_groups: vec![group.row_group_index],
            });
        }

        if files.is_empty() {
            return invalid("partition contains no physical row groups");
        }

        Ok(Self {
            files,
            file_index: 0,
            reader: None,
            schema: None,
            batch_size,
            expected_rows: partition.rows,
            causal_start: (manifest.timeline_ordering
                == crate::historical::job::MergeTimelineOrdering::Causal)
                .then_some(partition.start_row),
            mode: manifest.mode,
            range: manifest.time_range,
            timestamps_only,
            rows_read: 0,
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

                        if let Some(start) = self.causal_start.filter(|_| !self.timestamps_only) {
                            super::reader::validate_causal_batch(
                                &batch,
                                start + self.rows_read,
                                self.mode,
                                &self.range,
                            )?;
                        }
                        self.rows_read = self
                            .rows_read
                            .checked_add(batch.num_rows() as u64)
                            .ok_or_else(|| invalid_error("partition row counter overflow"))?;

                        if self.rows_read > self.expected_rows {
                            return invalid("partition reader exceeded expected row count");
                        }

                        return Ok(Some(batch));
                    }

                    Some(Err(error)) => {
                        return invalid(format!("partition Parquet read failed: {error}"));
                    }

                    None => {
                        self.reader = None;
                    }
                }
            }

            if self.file_index >= self.files.len() {
                if self.rows_read != self.expected_rows {
                    return invalid(format!(
                        "partition row count mismatch: expected {}, read {}",
                        self.expected_rows, self.rows_read,
                    ));
                }

                return Ok(None);
            }

            self.open_next_file()?;
        }
    }

    // -------------------------------------------------------------------------
    // Open selected Parquet row groups
    // -------------------------------------------------------------------------

    fn open_next_file(&mut self) -> Result<()> {
        let selected = &self.files[self.file_index];

        let file = File::open(&selected.path).map_err(|error| {
            invalid_error(format!(
                "cannot open partition Parquet {}: {error}",
                selected.path.display()
            ))
        })?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
            invalid_error(format!(
                "cannot inspect partition Parquet {}: {error}",
                selected.path.display()
            ))
        })?;

        let schema = builder.schema().clone();

        if let Some(expected) = &self.schema {
            if expected != &schema {
                return invalid(format!(
                    "partition schema mismatch: {}",
                    selected.path.display()
                ));
            }
        } else {
            self.schema = Some(schema);
        }

        let builder = if self.timestamps_only {
            let index = builder
                .schema()
                .index_of("event_timestamp_ns")
                .map_err(|error| invalid_error(error.to_string()))?;
            let mask = ProjectionMask::roots(builder.parquet_schema(), [index]);
            builder.with_projection(mask)
        } else {
            builder
        };
        let reader = builder
            .with_row_groups(selected.row_groups.clone())
            .with_batch_size(self.batch_size)
            .build()
            .map_err(|error| {
                invalid_error(format!("cannot construct partition reader: {error}"))
            })?;

        self.reader = Some(reader);

        self.file_index += 1;
        self.files_opened += 1;

        Ok(())
    }

    // -------------------------------------------------------------------------
    // Metrics
    // -------------------------------------------------------------------------

    pub fn rows_read(&self) -> u64 {
        self.rows_read
    }

    pub fn files_opened(&self) -> u64 {
        self.files_opened
    }

    pub fn schema(&self) -> Option<&SchemaRef> {
        self.schema.as_ref()
    }
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
