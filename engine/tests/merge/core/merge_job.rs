use marketforge_engine::job::MergeJob;

fn valid_job() -> serde_json::Value {
    serde_json::json!({
        "job_id": 200,
        "operation": "merge",
        "inputs": [105, 108, 112, 92],

        "time_range": {
            "start_ns": 1788220800000000000_i64,
            "end_ns": 1788307200000000000_i64
        },

        "resources": {
            "workers": 16,
            "memory_limit_mb": 65536,
            "max_open_files": 1024
        },

        "partitioning": {
            "partitions_per_worker": 4
        },

        "parquet": {
            "row_group_target_mb": 128,
            "file_target_mb": 1024,
            "compression_level": 3
        },

        "output": {
            "dataset_path": "/tmp/marketforge/merged/200"
        }
    })
}

#[test]
fn accepts_valid_merge_job() {
    let job: MergeJob = serde_json::from_value(valid_job()).unwrap();

    job.validate().unwrap();

    assert_eq!(job.inputs.len(), 4);
    assert_eq!(job.resources.workers, 16);
}

#[test]
fn rejects_duplicate_inputs() {
    let mut value = valid_job();

    value["inputs"] = serde_json::json!([105, 105]);

    let job: MergeJob = serde_json::from_value(value).unwrap();

    assert!(job.validate().is_err());
}

#[test]
fn rejects_invalid_time_range() {
    let mut value = valid_job();

    value["time_range"]["end_ns"] = value["time_range"]["start_ns"].clone();

    let job: MergeJob = serde_json::from_value(value).unwrap();

    assert!(job.validate().is_err());
}

#[test]
fn rejects_zero_workers() {
    let mut value = valid_job();

    value["resources"]["workers"] = serde_json::json!(0);

    let job: MergeJob = serde_json::from_value(value).unwrap();

    assert!(job.validate().is_err());
}

#[test]
fn rejects_relative_output_path() {
    let mut value = valid_job();

    value["output"]["dataset_path"] = serde_json::json!("data/merged/200");

    let job: MergeJob = serde_json::from_value(value).unwrap();

    assert!(job.validate().is_err());
}

#[test]
fn rejects_unknown_configuration_fields() {
    let mut value = valid_job();

    value["unexpected"] = serde_json::json!(true);

    assert!(serde_json::from_value::<MergeJob>(value).is_err());
}
