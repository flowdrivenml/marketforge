use std::{
    collections::HashMap,
    fs::File,
    path::{Path, PathBuf},
};

use arrow_array::{Array, Decimal256Array, Int64Array, RecordBatch, UInt32Array, UInt64Array};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

use crate::{
    error::{MarketForgeError, Result},
    process::{convert::ConversionDecimal, merge::PhysicalMergeManifest},
};

use super::state::{ReferenceKey, ReferenceObservation};

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
        })
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
                        self.batch = Some(batch);
                        self.batch_offset = 0;
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

            let reader = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|error| {
                    invalid_error(format!("failed to inspect reference Parquet: {error}"))
                })?
                .with_batch_size(REFERENCE_READ_BATCH_SIZE)
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
