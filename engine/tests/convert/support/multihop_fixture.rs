use std::{
    fs::{self, File},
    path::{Path, PathBuf},
};

use arrow_array::{Array, BooleanArray, Int64Array, RecordBatch};
use arrow_select::filter::filter_record_batch;
use parquet::arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};

use marketforge_engine::process::merge::{MergeTimelineFile, PhysicalMergeManifest};

const START_NS: i64 = 1788220800322740000;
const END_NS: i64 = 1788220810322740000;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

pub fn create_fixture(directory: &Path) -> PhysicalMergeManifest {
    let source = root().join("data/merged/conversion-multihop");

    let mut manifest: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(source.join("manifest.json")).unwrap()).unwrap();

    let mut files = manifest.files.clone();

    files.sort_by_key(|file| (file.partition_id, file.path.clone()));

    let mut batches = Vec::<RecordBatch>::new();

    for entry in files {
        let file = File::open(source.join(entry.path)).unwrap();

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .with_batch_size(65_536)
            .build()
            .unwrap();

        let mut reached_end = false;

        for batch in reader {
            let batch = batch.unwrap();

            let timestamps = batch
                .column_by_name("event_timestamp_ns")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            let mask = BooleanArray::from(
                (0..batch.num_rows())
                    .map(|row| {
                        let timestamp = timestamps.value(row);

                        if timestamp >= END_NS {
                            reached_end = true;
                        }

                        timestamp >= START_NS && timestamp < END_NS
                    })
                    .collect::<Vec<_>>(),
            );

            let selected = filter_record_batch(&batch, &mask).unwrap();

            if selected.num_rows() > 0 {
                batches.push(selected);
            }

            if reached_end {
                break;
            }
        }

        if reached_end {
            break;
        }
    }

    assert!(!batches.is_empty());

    let total = batches.iter().map(RecordBatch::num_rows).sum::<usize>();

    let output_dir = directory.join("timeline/partition-000000");

    fs::create_dir_all(&output_dir).unwrap();

    let output_path = output_dir.join("part-000000.parquet");

    use parquet::file::properties::WriterProperties;

    let properties = WriterProperties::builder()
        .set_max_row_group_size(64)
        .build();

    let mut writer = ArrowWriter::try_new(
        File::create(&output_path).unwrap(),
        batches[0].schema(),
        Some(properties),
    )
    .unwrap();

    for batch in &batches {
        writer.write(batch).unwrap();
    }

    writer.close().unwrap();

    let first_timestamp = START_NS;

    let last_batch = batches.last().unwrap();

    let timestamps = last_batch
        .column_by_name("event_timestamp_ns")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    let last_timestamp = timestamps.value(last_batch.num_rows() - 1);

    let metadata =
        ParquetRecordBatchReaderBuilder::try_new(File::open(&output_path).unwrap()).unwrap();

    manifest.events_written = total as u64;
    manifest.trade_events = total as u64;
    manifest.depth_events = 0;
    manifest.canonical_rows = total as u64;

    manifest.files_written = 1;
    manifest.row_groups = metadata.metadata().num_row_groups() as u64;

    manifest.start_timestamp_ns = Some(first_timestamp);
    manifest.end_timestamp_ns = Some(last_timestamp);

    manifest.files = vec![MergeTimelineFile {
        path: "timeline/partition-000000/part-000000.parquet".to_owned(),
        size_bytes: fs::metadata(&output_path).unwrap().len(),
        partition_id: Some(0),
        rows: Some(total as u64),
        start_timestamp_ns: Some(first_timestamp),
        end_timestamp_ns: Some(last_timestamp),
    }];

    manifest
}
