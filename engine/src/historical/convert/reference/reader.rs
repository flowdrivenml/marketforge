use super::state::{ReferenceKey, ReferenceObservation};
use arrow_array::{Array, Decimal256Array, Int64Array, RecordBatch, UInt32Array, UInt64Array};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::{
    collections::HashMap,
    fs::File,
    path::{Path, PathBuf},
};
use {
    crate::historical::convert::ConversionDecimal, crate::historical::merge::PhysicalMergeManifest,
    crate::shared::error::MarketForgeError, crate::shared::error::Result,
};

pub const REFERENCE_READ_BATCH_SIZE: usize = 65_536;

pub struct PhysicalReferenceReader {
    files: Vec<PathBuf>,
    file_index: usize,

    reader: Option<ParquetRecordBatchReader>,
    batch: Option<RecordBatch>,
    batch_offset: usize,

    streams: HashMap<u32, (u64, String)>,

    last_key: Option<ReferenceKey>,

    records_read: u64,
    batch_size: usize,
    next_row_group: Option<usize>,
    initial_rows_to_skip: usize,
}

impl PhysicalReferenceReader {
    pub fn new(
        dataset_path: &Path,
        manifest: &PhysicalMergeManifest,
        batch_size: usize,
    ) -> Result<Self> {
        if batch_size == 0 {
            return invalid("reference reader batch size must be positive");
        }

        manifest.validate()?;

        let mut files = manifest.files.clone();

        files.sort_by_key(|file| (file.partition_id, file.path.clone()));

        let files = files
            .iter()
            .map(|file| dataset_path.join(&file.path))
            .collect::<Vec<_>>();

        let mut streams = HashMap::new();

        for stream in &manifest.streams {
            if streams
                .insert(
                    stream.stream_rank,
                    (stream.dataset_id, stream.stream_id.clone()),
                )
                .is_some()
            {
                return invalid("duplicate reference stream rank");
            }
        }

        Ok(Self {
            files,
            file_index: 0,

            reader: None,
            batch: None,
            batch_offset: 0,

            streams,
            last_key: None,

            records_read: 0,
            batch_size,
            next_row_group: None,
            initial_rows_to_skip: 0,
        })
    }

    pub fn new_at(
        dataset_path: &Path,
        manifest: &PhysicalMergeManifest,
        batch_size: usize,
        observation_index: u64,
    ) -> Result<Self> {
        let mut reader = Self::new(dataset_path, manifest, batch_size)?;

        if observation_index > manifest.events_written {
            return invalid(format!(
                "reference seek position exceeds dataset: \
             position={}, events={}",
                observation_index, manifest.events_written,
            ));
        }

        // EOF is a valid position.
        if observation_index == manifest.events_written {
            reader.file_index = reader.files.len();
            reader.records_read = observation_index;
            return Ok(reader);
        }

        let mut global_offset = 0_u64;

        for (file_index, path) in reader.files.iter().enumerate() {
            let file = File::open(path).map_err(|error| {
                invalid_error(format!(
                    "cannot open reference file {}: {error}",
                    path.display()
                ))
            })?;

            let metadata_reader = SerializedFileReader::new(file).map_err(|error| {
                invalid_error(format!("cannot inspect reference file: {error}"))
            })?;

            let metadata = metadata_reader.metadata();

            for row_group_index in 0..metadata.num_row_groups() {
                let row_group = metadata.row_group(row_group_index);

                let rows = u64::try_from(row_group.num_rows())
                    .map_err(|_| invalid_error("negative reference row-group count"))?;

                let end = global_offset
                    .checked_add(rows)
                    .ok_or_else(|| invalid_error("reference row offset overflow"))?;

                if observation_index < end {
                    reader.file_index = file_index;
                    reader.next_row_group = Some(row_group_index);

                    reader.initial_rows_to_skip =
                        usize::try_from(observation_index - global_offset)
                            .map_err(|_| invalid_error("reference local offset overflow"))?;

                    reader.records_read = observation_index;

                    return Ok(reader);
                }

                global_offset = end;
            }
        }

        return invalid("reference seek position not found");
    }

    pub fn next_observation(&mut self) -> Result<Option<ReferenceObservation>> {
        loop {
            if let Some(batch) = &self.batch {
                if self.batch_offset < batch.num_rows() {
                    let row = self.batch_offset;
                    self.batch_offset += 1;

                    let observation = self.decode_observation(batch, row)?;

                    if let Some(previous) = self.last_key {
                        if observation.key < previous {
                            return invalid("physical reference timeline regressed");
                        }
                    }

                    self.last_key = Some(observation.key);
                    self.records_read += 1;

                    return Ok(Some(observation));
                }
            }

            if let Some(reader) = &mut self.reader {
                match reader.next() {
                    Some(Ok(batch)) => {
                        let skip = self.initial_rows_to_skip.min(batch.num_rows());

                        self.initial_rows_to_skip -= skip;

                        self.batch = Some(batch);
                        self.batch_offset = skip;

                        continue;
                    }

                    Some(Err(error)) => {
                        return invalid(format!("failed to read reference Parquet batch: {error}"));
                    }

                    None => {
                        self.reader = None;
                        self.batch = None;
                    }
                }
            }

            if self.file_index >= self.files.len() {
                return Ok(None);
            }

            let path = &self.files[self.file_index];
            self.file_index += 1;

            let file = File::open(path).map_err(|error| {
                invalid_error(format!(
                    "failed to open reference Parquet {}: {error}",
                    path.display()
                ))
            })?;

            let mut builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|error| {
                invalid_error(format!("failed to inspect reference Parquet: {error}"))
            })?;

            if let Some(first_group) = self.next_row_group.take() {
                let groups = (first_group..builder.metadata().num_row_groups()).collect::<Vec<_>>();

                builder = builder.with_row_groups(groups);
            }

            let reader = builder
                .with_batch_size(self.batch_size)
                .build()
                .map_err(|error| {
                    invalid_error(format!("failed to create reference reader: {error}"))
                })?;

            self.reader = Some(reader);
        }
    }

    fn decode_observation(&self, batch: &RecordBatch, row: usize) -> Result<ReferenceObservation> {
        let timestamps = column::<Int64Array>(batch, "event_timestamp_ns")?;
        let ranks = column::<UInt32Array>(batch, "stream_rank")?;
        let dataset_ids = column::<UInt64Array>(batch, "dataset_id")?;
        let task_ids = column::<UInt64Array>(batch, "task_id")?;
        let ordinals = column::<UInt64Array>(batch, "event_ordinal")?;
        let prices = column::<Decimal256Array>(batch, "price")?;

        let rank = ranks.value(row);
        let dataset_id = dataset_ids.value(row);

        let (expected_dataset, stream_id) = self
            .streams
            .get(&rank)
            .ok_or_else(|| invalid_error("unknown reference stream rank"))?;

        if dataset_id != *expected_dataset {
            return invalid("reference dataset identity mismatch");
        }

        let price = ConversionDecimal::new(prices.value(row))?;

        Ok(ReferenceObservation {
            dataset_id,
            stream_id: stream_id.clone(),

            key: ReferenceKey::new(
                timestamps.value(row),
                rank,
                task_ids.value(row),
                ordinals.value(row),
            ),

            price,
        })
    }

    pub fn records_read(&self) -> u64 {
        self.records_read
    }
}

fn column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| invalid_error(format!("missing reference column: {name}")))?;

    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| invalid_error(format!("invalid reference column type: {name}")))
}

fn invalid_error(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}

fn invalid<T>(message: impl Into<String>) -> Result<T> {
    Err(invalid_error(message))
}
