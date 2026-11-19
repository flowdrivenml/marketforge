use crate::merge_test_job::canonical_job;
use marketforge_engine::historical::job::MergeJobDocument;

fn wire_fixture() -> serde_json::Value {
    serde_json::json!({
        "job_id": 1,
        "operation": "merge",
        "inputs": [
            { "dataset_id": 11, "path": "/fixture/part-0.parquet", "sha256": "a".repeat(64),
              "publication": {
                  "engine_dataset_id": "9007199254740993", "manifest_path": "/fixture/manifest.json",
                  "manifest_sha256": "c".repeat(64)
              } },
            { "dataset_id": 12, "path": "/fixture/part-1.parquet", "sha256": "b".repeat(64),
              "publication": {
                  "engine_dataset_id": "9007199254740993", "manifest_path": "/fixture/manifest.json",
                  "manifest_sha256": "c".repeat(64)
              } }
        ],
        "time_range": { "start_ns": 100, "end_ns": 400 },
        "output": { "dataset_path": "/fixture/output" }
    })
}

#[test]
fn canonical_file_inputs_parse_without_touching_data() {
    let job = canonical_job();
    job.validate().unwrap();
    assert!(
        job.inputs
            .iter()
            .all(|input| input.dataset_id != input.publication.engine_dataset_id)
    );
}

#[test]
fn merge_wire_contract_round_trips() {
    let expected = wire_fixture();
    let job: MergeJobDocument = serde_json::from_value(expected.clone()).unwrap();
    job.validate().unwrap();
    assert_eq!(
        job.inputs[0].publication.engine_dataset_id,
        9_007_199_254_740_993
    );
    assert_eq!(serde_json::to_value(job).unwrap(), expected);
}

#[test]
fn rejects_duplicate_and_conflicting_inputs() {
    let mut job: MergeJobDocument = serde_json::from_value(wire_fixture()).unwrap();
    job.validate().unwrap();
    job.inputs.push(job.inputs[0].clone());
    assert!(
        job.validate()
            .unwrap_err()
            .to_string()
            .contains("duplicate")
    );
    job.inputs.pop();
    job.inputs[1].publication.manifest_sha256 = "f".repeat(64);
    assert!(
        job.validate()
            .unwrap_err()
            .to_string()
            .contains("conflicting publication")
    );
}

#[test]
fn rejects_invalid_range_output_and_digest() {
    let original: MergeJobDocument = serde_json::from_value(wire_fixture()).unwrap();
    original.validate().unwrap();
    let mut job = original.clone();
    job.time_range.end_ns = job.time_range.start_ns;
    assert!(
        job.validate()
            .unwrap_err()
            .to_string()
            .contains("start_ns < end_ns")
    );
    let mut job = original.clone();
    job.output.dataset_path = "relative".into();
    assert!(
        job.validate()
            .unwrap_err()
            .to_string()
            .contains("absolute output path")
    );
    let mut job = original;
    job.inputs[0].sha256 = "z".repeat(64);
    assert!(
        job.validate()
            .unwrap_err()
            .to_string()
            .contains("invalid or duplicate registered file")
    );
}

#[test]
fn rejects_embedded_runtime_and_noncanonical_publication_id() {
    serde_json::from_value::<MergeJobDocument>(wire_fixture()).unwrap();
    let mut value = wire_fixture();
    value["resources"] = serde_json::json!({"workers": 1});
    assert!(
        serde_json::from_value::<MergeJobDocument>(value)
            .unwrap_err()
            .to_string()
            .contains("unknown field `resources`")
    );
    let mut value = wire_fixture();
    value["inputs"][0]["publication"]["engine_dataset_id"] = serde_json::json!(100);
    assert!(
        serde_json::from_value::<MergeJobDocument>(value)
            .unwrap_err()
            .to_string()
            .contains("expected a string")
    );
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

#[test]
fn validates_runtime_toml_separately() {
    use marketforge_engine::shared::historical::config::load_merge_runtime_settings;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("runtime.toml");
    let rules = [
        "parse_failure",
        "invalid_record",
        "sequence_gap",
        "timestamp_regression",
        "missing_snapshot",
        "invalid_book",
        "transformation_failure",
    ]
    .map(|name| format!("{name} = {{ action = \"fail\" }}\n"))
    .concat();
    let runtime = format!(
        r#"schema_version = 1
[resources]
workers = 2
memory_budget_bytes = 268435456
scratch_budget_bytes = 268435456
scratch_path = "scratch"
[resources.parquet]
row_group_target_bytes = 1048576
file_target_bytes = 4194304
[integrity_policy]
enabled = false
{rules}"#
    );
    std::fs::write(&path, &runtime).unwrap();
    let settings = load_merge_runtime_settings(&path, root.path()).unwrap();
    assert_eq!(settings.resources.workers, 2);
    assert_eq!(settings.partitioning.partitions_per_worker, 4);
    let job: MergeJobDocument = serde_json::from_value(wire_fixture()).unwrap();
    let resolved = job.with_runtime(settings).unwrap();
    assert_eq!(resolved.inputs, [9_007_199_254_740_993]);
    assert_eq!(resolved.submission.unwrap().inputs.len(), 2);
    std::fs::write(&path, runtime.replace("workers = 2", "workers = 0")).unwrap();
    assert!(load_merge_runtime_settings(&path, root.path()).is_err());
}

#[test]
fn verifies_only_selected_primary_files_and_tracks_concrete_stream_owners() {
    use marketforge_engine::historical::{
        job::{ContentType, MergeFileInput, MergePublication},
        merge::{build_trade_inventory, resolve_merge_inputs},
        normalize::{
            manifest::{DatasetFile, DatasetManifest, file_sha256},
            metrics::{
                IntegrityStatus, ProcessingCounters, ProcessingMetricsReport, TaskMetricsReport,
            },
            streams::{SourceSpan, StreamOutput},
        },
    };
    use marketforge_engine::shared::historical::config::MergeRuntimeSettings;
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path();
    let mut job = canonical_job();
    job.output.dataset_path = root.join("output");
    let start = job.time_range.start_ns;
    let mut report = ProcessingMetricsReport::default();
    let mut files = Vec::new();
    let mut streams = Vec::new();
    for (stream_id, task_id, parts) in [("concrete-a", 10, 3), ("concrete-b", 20, 1)] {
        let directory = format!("streams/{}/trades", (task_id % 3).to_string().repeat(64));
        std::fs::create_dir_all(root.join(&directory)).unwrap();
        report
            .add_task(TaskMetricsReport {
                failure: None,
                source: None,
                accepted_depth_events: None,
                task_id,
                stream_id: stream_id.into(),
                counters: ProcessingCounters {
                    records_read: parts,
                    records_matched: parts,
                    records_processed: parts,
                    events_normalized: parts,
                    events_written: parts,
                    ..Default::default()
                },
                integrity: Default::default(),
                scoped_integrity: Default::default(),
                start_timestamp_ns: Some(start),
                end_timestamp_ns: Some(start),
            })
            .unwrap();
        for part in 0..parts {
            let path = format!("{directory}/part-{part:06}.parquet");
            let batch = crate::materialize_partition::canonical_trades(&[start]);
            let mut columns = batch.columns().to_vec();
            columns[batch.schema().index_of("stream_id").unwrap()] =
                std::sync::Arc::new(arrow_array::StringArray::from(vec![stream_id]));
            let batch = arrow_array::RecordBatch::try_new(batch.schema(), columns).unwrap();
            let primary = root.join(&path);
            crate::materialize_partition::write_parquet(&primary, &batch);
            files.push(DatasetFile {
                path,
                rows: 1,
                size_bytes: primary.metadata().unwrap().len(),
                start_timestamp_ns: Some(start),
                end_timestamp_ns: Some(start),
            });
        }
        streams.push(StreamOutput {
            stream_id: stream_id.into(),
            directory,
            sources: vec![SourceSpan {
                task_id,
                start_row: 0,
                end_row: parts,
                ranges: Vec::new(),
            }],
        });
    }
    let manifest = DatasetManifest {
        protocol_version: 5,
        job_id: 100,
        dataset_id: 9007199254740993,
        content_type: ContentType::Trades,
        processing_quality: None,
        events_written: 4,
        files_written: 4,
        start_timestamp_ns: Some(start),
        end_timestamp_ns: Some(start),
        source_tasks: vec![10, 20],
        instrument_namespace: None,
        resolved_instruments: vec![],
        streams,
        files,
        auxiliary_files: vec![],
        integrity_status: IntegrityStatus::Clean,
        integrity_evaluation: Default::default(),
        processing_metrics: report,
    };
    let manifest_path = root.join("manifest.json");
    std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    let publication = MergePublication {
        engine_dataset_id: manifest.dataset_id,
        manifest_sha256: file_sha256(&manifest_path).unwrap(),
        manifest_path,
    };
    job.inputs = [3, 0, 2]
        .into_iter()
        .enumerate()
        .map(|(index, part)| {
            let path = root.join(&manifest.files[part].path);
            MergeFileInput {
                dataset_id: (index + 50) as u64,
                sha256: file_sha256(&path).unwrap(),
                path,
                publication: publication.clone(),
            }
        })
        .collect();
    let resolve = |job: MergeJobDocument| {
        let runtime: MergeRuntimeSettings = super::merge_test_job::small_runtime();
        resolve_merge_inputs(&job.with_runtime(runtime).unwrap(), root)
    };
    let result = resolve(job.clone()).unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0].canonical_files,
        job.inputs
            .iter()
            .map(|input| input.path.clone())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        result[0]
            .selected_streams()
            .unwrap()
            .into_iter()
            .map(|stream| stream.stream_id.as_str())
            .collect::<Vec<_>>(),
        ["concrete-b", "concrete-a"]
    );
    std::fs::remove_file(root.join(&manifest.files[1].path)).unwrap();
    assert_eq!(
        resolve(job.clone()).unwrap()[0].canonical_files,
        result[0].canonical_files
    );
    let inventory = build_trade_inventory(&resolve(job.clone()).unwrap()).unwrap();
    let ownership = inventory
        .iter()
        .map(|task| {
            (
                task.task_id,
                task.canonical_rows,
                task.files
                    .iter()
                    .map(|file| (file.part_index, file.row_offset, file.rows))
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        ownership,
        [
            (20, 1, vec![(0, 0, 1)]),
            (10, 3, vec![(0, 0, 1), (2, 2, 1)])
        ]
    );
    let mut unlisted = job.clone();
    unlisted.inputs[0].path = root.join("unlisted.parquet");
    assert!(
        resolve(unlisted)
            .unwrap_err()
            .to_string()
            .contains("not a primary manifest member")
    );
    let mut wrong_manifest = job.clone();
    for input in &mut wrong_manifest.inputs {
        input.publication.manifest_sha256 = "1".repeat(64);
    }
    assert!(
        resolve(wrong_manifest)
            .unwrap_err()
            .to_string()
            .contains("manifest fingerprint changed")
    );
    job.inputs[0].sha256 = "1".repeat(64);
    assert!(
        resolve(job)
            .unwrap_err()
            .to_string()
            .contains("SHA-256 mismatch")
    );
}
