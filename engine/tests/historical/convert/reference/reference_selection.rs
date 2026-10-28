use std::{collections::HashMap, fs::File, path::PathBuf};

use arrow_array::{
    Array, Int64Array, ListArray, StructArray, UInt8Array, UInt32Array, UInt64Array,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use marketforge_engine::historical::convert::reference::ReferenceKey;

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[derive(Debug, Clone)]
struct LatestReference {
    key: ReferenceKey,
    dataset_id: u64,
}

#[test]
fn selects_latest_eligible_reference_observations() {
    let output = root().join("data/.work/convert-validation/multihop");

    let mut files = std::fs::read_dir(&output)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "parquet"))
        .collect::<Vec<_>>();

    files.sort();

    assert!(!files.is_empty());

    let mut latest = HashMap::<u64, LatestReference>::new();

    let mut checked_references = 0_u64;
    let mut converted_events = 0_u64;

    for path in files {
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
            .unwrap()
            .build()
            .unwrap();

        for batch in reader {
            let batch = batch.unwrap();

            let timestamps = batch
                .column_by_name("event_timestamp_ns")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            let ranks = batch
                .column_by_name("stream_rank")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();

            let datasets = batch
                .column_by_name("dataset_id")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();

            let tasks = batch
                .column_by_name("task_id")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();

            let ordinals = batch
                .column_by_name("event_ordinal")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();

            let statuses = batch
                .column_by_name("conversion_status")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap();

            let references = batch
                .column_by_name("conversion_references")
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();

            for row in 0..batch.num_rows() {
                let dataset_id = datasets.value(row);

                let key = ReferenceKey::new(
                    timestamps.value(row),
                    ranks.value(row),
                    tasks.value(row),
                    ordinals.value(row),
                );

                if statuses.value(row) == 1 {
                    converted_events += 1;

                    let values = references.value(row);

                    let references = values.as_any().downcast_ref::<StructArray>().unwrap();

                    let reference_datasets = references
                        .column_by_name("dataset_id")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap();

                    let reference_timestamps = references
                        .column_by_name("event_timestamp_ns")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();

                    let reference_ranks = references
                        .column_by_name("stream_rank")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<UInt32Array>()
                        .unwrap();

                    let reference_tasks = references
                        .column_by_name("task_id")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap();

                    let reference_ordinals = references
                        .column_by_name("event_ordinal")
                        .unwrap()
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap();

                    for index in 0..references.len() {
                        let reference_dataset = reference_datasets.value(index);

                        let recorded_key = ReferenceKey::new(
                            reference_timestamps.value(index),
                            reference_ranks.value(index),
                            reference_tasks.value(index),
                            reference_ordinals.value(index),
                        );

                        let expected = latest
                            .get(&reference_dataset)
                            .expect("reference observation missing from history");

                        assert_eq!(
                            recorded_key, expected.key,
                            "incorrect reference selection for dataset {}",
                            reference_dataset
                        );

                        assert_eq!(expected.dataset_id, reference_dataset);

                        assert!(recorded_key < key);

                        checked_references += 1;
                    }
                }

                // Update state after validating the source event.
                latest.insert(dataset_id, LatestReference { key, dataset_id });
            }
        }
    }

    assert!(converted_events > 0);
    assert!(checked_references > 0);

    println!();
    println!("MARKETFORGE — REFERENCE SELECTION VALIDATION");
    println!("Converted events   : {converted_events}");
    println!("References checked : {checked_references}");
    println!("STATUS             : PASS");
}
