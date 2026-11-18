use marketforge_engine::historical::job::MergeJob;

fn valid_job() -> serde_json::Value {
    let mut job: serde_json::Value =
        serde_json::from_str(include_str!("../../fixtures/merge.json")).unwrap();
    job["output"]["dataset_path"] =
        serde_json::json!(std::env::temp_dir().join("marketforge-merge-contract"));
    job
}

#[test]
fn accepts_valid_merge_job() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("merge.json");
    std::fs::write(&path, serde_json::to_vec(&valid_job()).unwrap()).unwrap();
    let job = marketforge_engine::historical::job::load_merge_job(path).unwrap();

    assert_eq!(job.inputs.len(), 4);
    assert_eq!(job.resources.workers, 4);
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

#[test]
fn parses_explicit_primary_files_without_replacing_engine_identity() {
    let mut job = valid_job();
    job["inputs"] = serde_json::json!([105]);
    job["input_paths"] = serde_json::json!({"105": "/workspace/publication"});
    job["primary_files"] = serde_json::json!({"105": [
        {"path": "stream/part-0.parquet", "sha256": "a".repeat(64), "manifest_sha256": "c".repeat(64)},
        {"path": "stream/part-2.parquet", "sha256": "b".repeat(64), "manifest_sha256": "c".repeat(64)}
    ]});
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("selection.json");
    std::fs::write(&path, serde_json::to_vec(&job).unwrap()).unwrap();
    let parsed = marketforge_engine::historical::job::load_merge_job(path).unwrap();
    assert_eq!(parsed.inputs, [105]);
    assert_eq!(parsed.primary_files[&105].len(), 2);
}

#[test]
fn selected_trade_index_preserves_original_offsets_and_excludes_gaps() {
    use marketforge_engine::historical::merge::CanonicalRowIndex;
    let index = CanonicalRowIndex::from_positioned(vec![
        ("first.parquet".into(), 10, 2, Some(4)),
        ("last.parquet".into(), 20, 3, Some(11)),
    ])
    .unwrap();
    assert_eq!(index.resolve(12, 1).unwrap()[0].local_offset, 21);
    assert!(index.resolve(6, 1).is_err());
    assert!(index.resolve(5, 7).is_err());
}
