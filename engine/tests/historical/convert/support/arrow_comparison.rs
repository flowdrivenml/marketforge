use std::{
    fs::File,
    path::{Path, PathBuf},
};

use arrow_array::RecordBatch;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};

const BATCH_SIZE: usize = 65_536;

// -----------------------------------------------------------------------------
// Ordered Parquet reader
// -----------------------------------------------------------------------------

struct OrderedParquetReader {
    files: Vec<PathBuf>,
    next_file: usize,
    reader: Option<ParquetRecordBatchReader>,
}

impl OrderedParquetReader {
    fn new(directory: &Path) -> Self {
        let manifest = serde_json::from_reader::<
            _,
            marketforge_engine::historical::convert::output::ConversionManifest,
        >(File::open(directory.join("manifest.json")).unwrap())
        .unwrap();
        manifest.validate().unwrap();
        let files = manifest
            .output
            .files
            .into_iter()
            .map(|file| directory.join(file.path))
            .collect();
        Self {
            files,
            next_file: 0,
            reader: None,
        }
    }

    fn next_batch(&mut self) -> Option<RecordBatch> {
        loop {
            if let Some(reader) = &mut self.reader {
                match reader.next() {
                    Some(Ok(batch)) => {
                        if batch.num_rows() > 0 {
                            return Some(batch);
                        }
                    }
                    Some(Err(error)) => panic!("Parquet read failed: {error}"),
                    None => self.reader = None,
                }
            }

            if self.next_file >= self.files.len() {
                return None;
            }

            let path = &self.files[self.next_file];

            let file = File::open(path).unwrap();

            self.reader = Some(
                ParquetRecordBatchReaderBuilder::try_new(file)
                    .unwrap()
                    .with_batch_size(BATCH_SIZE)
                    .build()
                    .unwrap(),
            );

            self.next_file += 1;
        }
    }
}

// -----------------------------------------------------------------------------
// Exact logical Arrow comparison
// -----------------------------------------------------------------------------

pub fn compare_datasets(expected_directory: &Path, actual_directory: &Path) -> u64 {
    let mut expected = OrderedParquetReader::new(expected_directory);
    let mut actual = OrderedParquetReader::new(actual_directory);

    let mut expected_batch: Option<RecordBatch> = None;
    let mut actual_batch: Option<RecordBatch> = None;

    let mut expected_offset = 0;
    let mut actual_offset = 0;

    let mut compared = 0_u64;

    loop {
        if expected_batch
            .as_ref()
            .is_none_or(|batch| expected_offset >= batch.num_rows())
        {
            expected_batch = expected.next_batch();
            expected_offset = 0;
        }

        if actual_batch
            .as_ref()
            .is_none_or(|batch| actual_offset >= batch.num_rows())
        {
            actual_batch = actual.next_batch();
            actual_offset = 0;
        }

        match (&expected_batch, &actual_batch) {
            (None, None) => break,

            (None, Some(_)) => {
                panic!("parallel output contains additional events");
            }

            (Some(_), None) => {
                panic!("parallel output is missing events");
            }

            (Some(_), Some(_)) => {}
        }

        let left = expected_batch.as_ref().unwrap();
        let right = actual_batch.as_ref().unwrap();

        assert_eq!(
            left.schema(),
            right.schema(),
            "parallel conversion schema differs from sequential"
        );

        let count = (left.num_rows() - expected_offset).min(right.num_rows() - actual_offset);

        assert!(count > 0);

        for column in 0..left.num_columns() {
            let expected_slice = left.column(column).slice(expected_offset, count);

            let actual_slice = right.column(column).slice(actual_offset, count);

            assert_eq!(
                expected_slice.to_data(),
                actual_slice.to_data(),
                "conversion mismatch: column={}, global_row={}",
                left.schema().field(column).name(),
                compared,
            );
        }

        expected_offset += count;
        actual_offset += count;
        compared += count as u64;
    }

    compared
}
