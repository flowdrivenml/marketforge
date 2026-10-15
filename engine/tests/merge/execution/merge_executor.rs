#![cfg(feature = "historical")]

use std::{fs::File, path::PathBuf};

use arrow_array::Int64Array;
use marketforge_engine::{job::load_merge_job, process::merge::execute_merge_job};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
#[ignore = "requires local processed datasets and writes committed output"]
fn executes_real_merge_job() {
    let root = project_root();

    let job = load_merge_job(root.join("data/.jobs/merge/merge-integration-test.json"))
        .expect("load merge job");

    let result = execute_merge_job(&job, &root).expect("execute merge job");

    assert!(result.events_written > 0);
    assert!(result.files_written > 0);
    assert!(result.manifest_path.is_file());

    let manifest: serde_json::Value =
        serde_json::from_reader(File::open(&result.manifest_path).unwrap()).unwrap();

    assert_eq!(manifest["job_id"], 900002);
    assert_eq!(manifest["events_written"], result.events_written);

    let mut total_rows = 0_u64;
    let mut previous_timestamp = None;

    for entry in manifest["files"].as_array().unwrap() {
        let relative_path = entry["path"].as_str().unwrap();

        let path = result.dataset_path.join(relative_path);

        let file = File::open(path).unwrap();

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
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

            for index in 0..batch.num_rows() {
                let timestamp = timestamps.value(index);

                assert!(timestamp >= job.time_range.start_ns);
                assert!(timestamp < job.time_range.end_ns);

                if let Some(previous) = previous_timestamp {
                    assert!(
                        timestamp >= previous,
                        "merged timeline contains timestamp regression"
                    );
                }

                previous_timestamp = Some(timestamp);
                total_rows += 1;
            }
        }
    }

    assert_eq!(total_rows, result.events_written);

    assert_eq!(manifest["initial_checkpoints"].as_array().unwrap().len(), 4);

    println!("\nMARKETFORGE — END-TO-END MERGE");
    println!("Input datasets : {}", job.inputs.len());
    println!("Merged events  : {}", result.events_written);
    println!("Parquet files  : {}", result.files_written);
    println!(
        "Checkpoints    : {}",
        manifest["initial_checkpoints"].as_array().unwrap().len()
    );
    println!("Output         : {}", result.dataset_path.display());
    println!("Result         : PASS");
}
