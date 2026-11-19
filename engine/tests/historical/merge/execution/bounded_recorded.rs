//! Bounded compatibility checks against the immutable, registered v5 publications.
use crate::merge_test_job::{bounded_runtime, recorded_job};
use crate::recorded_datasets::recorded_members;
use arrow_array::{Array, Int64Array, RecordBatch, UInt64Array};
use marketforge_engine::historical::{
    job::{ContentType, MergeJobDocument},
    merge::{PhysicalMergeManifest, execute_unified_merge_job, validate_committed_merge},
    normalize::manifest::file_sha256,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::{
    collections::BTreeMap,
    fs::{self, File},
    path::{Path, PathBuf},
};

fn read(path: &Path) -> Vec<RecordBatch> {
    ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .build()
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

#[test]
#[ignore = "opt-in compatibility with canonical normalized publications"]
fn bounded_v5_trade_matches_normalized_rows_and_commits_with_one_or_two_workers() {
    let root = tempfile::tempdir().unwrap();
    let mut job = recorded_job();
    let (input, metadata) = recorded_members(&job, ContentType::Trades)
        .into_iter()
        .find(|(_, file)| {
            file.rows <= 256
                && file.size_bytes <= 1024 * 1024
                && file.start_timestamp_ns.unwrap() < job.time_range.end_ns
                && file.end_timestamp_ns.unwrap() >= job.time_range.start_ns
        })
        .expect("canonical fixture must have a small overlapping trade file");
    job.inputs = vec![input.clone()];
    assert_eq!(
        fs::metadata(&input.path)
            .expect("selected canonical primary is missing")
            .len(),
        metadata.size_bytes
    );
    assert_eq!(file_sha256(&input.path).unwrap(), input.sha256);
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(File::open(&input.path).unwrap()).unwrap();
    assert_eq!(
        reader.metadata().file_metadata().num_rows() as u64,
        metadata.rows
    );
    assert!(
        reader
            .metadata()
            .row_groups()
            .iter()
            .all(|group| group.total_byte_size() <= 1024 * 1024)
    );
    let source = reader
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let expected = source
        .iter()
        .flat_map(|batch| {
            let timestamps = batch
                .column_by_name("event_timestamp_ns")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..batch.num_rows()).filter_map(move |row| {
                let timestamp = timestamps.value(row);
                (job.time_range.start_ns <= timestamp && timestamp < job.time_range.end_ns)
                    .then_some((batch, row))
            })
        })
        .collect::<Vec<_>>();
    assert!(!expected.is_empty() && expected.len() as u64 <= metadata.rows);

    for workers in [1, 2] {
        let started = std::time::Instant::now();
        let mut job = job.clone();
        job.output.dataset_path = root.path().join(format!("workers-{workers}"));
        let mut runtime_job = bounded_runtime(job.clone());
        runtime_job.resources.workers = workers;
        let result = execute_unified_merge_job(&runtime_job, root.path()).unwrap();
        validate_committed_merge(&result.dataset_path).unwrap();
        let manifest: PhysicalMergeManifest =
            serde_json::from_reader(File::open(&result.manifest_path).unwrap()).unwrap();
        assert_eq!(
            (
                manifest.events_written,
                manifest.trade_events,
                manifest.depth_events,
                manifest.canonical_rows
            ),
            (
                expected.len() as u64,
                expected.len() as u64,
                0,
                expected.len() as u64
            )
        );
        assert_eq!(manifest.streams.len(), 1);
        assert_eq!(
            manifest.input_datasets[0].dataset_id,
            input.publication.engine_dataset_id
        );
        assert_eq!(manifest.input_datasets[0].manifest_version, 5);
        let retained: MergeJobDocument = serde_json::from_reader(
            File::open(result.dataset_path.join("merge-job.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(retained, job);
        let hashes: BTreeMap<PathBuf, String> = serde_json::from_reader(
            File::open(result.dataset_path.join("publication-checksums.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(hashes.len(), manifest.files.len() + 5);
        for (relative, expected_hash) in hashes {
            assert_eq!(
                file_sha256(&result.dataset_path.join(relative)).unwrap(),
                expected_hash
            );
        }
        let physical = manifest
            .files
            .iter()
            .flat_map(|file| read(&result.dataset_path.join(&file.path)))
            .collect::<Vec<_>>();
        let actual = physical
            .iter()
            .flat_map(|batch| (0..batch.num_rows()).map(move |row| (batch, row)))
            .collect::<Vec<_>>();
        assert_eq!(actual.len(), expected.len());
        for ((got, row), (want, source_row)) in actual.into_iter().zip(expected.iter().copied()) {
            for (column, field) in want.columns().iter().zip(want.schema().fields()) {
                let physical = got.column_by_name(field.name()).unwrap();
                assert_eq!(
                    physical.slice(row, 1).as_ref(),
                    column.slice(source_row, 1).as_ref(),
                    "{} at source row {source_row}",
                    field.name()
                );
            }
            let provenance = got
                .column_by_name("dataset_id")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            assert_eq!(provenance.value(row), input.publication.engine_dataset_id);
        }
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), workers);
        println!(
            "workers={workers}, primary_files=1, source_rows={}, output_rows={}, elapsed={:?}",
            metadata.rows,
            expected.len(),
            started.elapsed()
        );
    }
}

#[test]
#[ignore = "opt-in compatibility with canonical normalized publications"]
fn bounded_v5_depth_reaches_the_reconstruction_guard() {
    use marketforge_engine::historical::merge::{
        resolve_merge_inputs, resolve_ranked_merge_streams,
    };
    let workspace = tempfile::tempdir().unwrap();
    let mut job = recorded_job();
    let (input, file) = recorded_members(&job, ContentType::Depth)
        .into_iter()
        .next()
        .unwrap();
    assert!(file.size_bytes <= 8 * 1024 * 1024);
    job.inputs = vec![input];
    job.output.dataset_path = workspace.path().join("merged");
    let resolved = resolve_merge_inputs(&bounded_runtime(job.clone()), workspace.path()).unwrap();
    assert_eq!(resolved[0].manifest.protocol_version, 5);
    assert_eq!(resolved[0].selected_streams().unwrap().len(), 1);
    let err = resolve_ranked_merge_streams(&resolved, workspace.path(), Some(&job)).unwrap_err();
    assert!(
        err.to_string()
            .contains("reconstruction-state dependencies"),
        "{err}"
    );
    assert!(!job.output.dataset_path.exists());
}

#[test]
#[ignore = "opt-in compatibility with canonical normalized publications"]
fn bounded_v5_trade_streams_from_one_publication_commit() {
    use marketforge_engine::historical::merge::resolve_merge_inputs;
    let workspace = tempfile::tempdir().unwrap();
    let mut job = recorded_job();
    job.inputs = recorded_members(&job, ContentType::Trades)
        .into_iter()
        .filter(|(_, file)| {
            file.rows <= 256
                && file.size_bytes <= 1024 * 1024
                && file.start_timestamp_ns.unwrap() < job.time_range.end_ns
                && file.end_timestamp_ns.unwrap() >= job.time_range.start_ns
        })
        .take(2)
        .map(|(input, _)| input)
        .collect();
    assert_eq!(job.inputs.len(), 2);
    job.output.dataset_path = workspace.path().join("merged");
    let resolved = resolve_merge_inputs(&bounded_runtime(job.clone()), workspace.path()).unwrap();
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].selected_streams().unwrap().len(), 2);
    let result =
        execute_unified_merge_job(&bounded_runtime(job.clone()), workspace.path()).unwrap();
    let manifest: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(result.manifest_path).unwrap()).unwrap();
    assert_eq!(manifest.streams.len(), 2);
    assert_eq!(manifest.input_datasets.len(), 1);
    assert!(manifest.trade_events > 0);
    validate_committed_merge(&job.output.dataset_path).unwrap();
}

#[test]
#[ignore = "opt-in compatibility with canonical normalized publications"]
fn adjacent_depth_parts_preserve_stream_rows_without_opening_primary_files() {
    use crate::recorded_datasets::{recorded_stream, stream_parts};
    let job = recorded_job();
    let (owner, pair) = recorded_members(&job, ContentType::Depth)
        .into_iter()
        .find_map(|(input, _)| {
            let parts = stream_parts(&job, &input);
            let pair = parts
                .windows(2)
                .find(|pair| pair[0].1.end == pair[1].1.start)?;
            Some((recorded_stream(&input), pair.to_vec()))
        })
        .expect("selected adjacent primary files");
    assert_ne!(pair[0].0.dataset_id, pair[1].0.dataset_id);
    for (_, rows) in pair {
        let owned = owner
            .sources
            .iter()
            .map(|source| {
                let start = rows.start.max(source.start_row);
                let end = rows.end.min(source.end_row);
                end.saturating_sub(start)
            })
            .sum::<u64>();
        assert!(owned > 0);
        assert_eq!(owned, rows.end - rows.start);
    }
}

#[test]
#[ignore = "opt-in compatibility with canonical normalized publications"]
fn bounded_v5_combined_inputs_resolve_before_the_reconstruction_guard() {
    use marketforge_engine::historical::merge::{
        resolve_merge_inputs, resolve_ranked_merge_streams,
    };
    let workspace = tempfile::tempdir().unwrap();
    let mut job = recorded_job();
    let (depth, depth_file) = recorded_members(&job, ContentType::Depth)
        .into_iter()
        .next()
        .unwrap();
    assert!(depth_file.size_bytes <= 8 * 1024 * 1024);
    let (trade, trade_file) = recorded_members(&job, ContentType::Trades)
        .into_iter()
        .find(|(_, file)| {
            file.rows <= 256
                && file.size_bytes <= 1024 * 1024
                && file.start_timestamp_ns.unwrap() <= depth_file.end_timestamp_ns.unwrap()
                && file.end_timestamp_ns.unwrap() >= depth_file.start_timestamp_ns.unwrap()
        })
        .unwrap();
    let start = trade_file
        .start_timestamp_ns
        .unwrap()
        .max(depth_file.start_timestamp_ns.unwrap());
    job.time_range.start_ns = start;
    job.time_range.end_ns = start + 1_000_000_000;
    job.inputs = vec![trade, depth];
    job.output.dataset_path = workspace.path().join("merged");
    let resolved = resolve_merge_inputs(&bounded_runtime(job.clone()), workspace.path()).unwrap();
    assert_eq!(
        resolved
            .iter()
            .map(|input| input.manifest.content_type)
            .collect::<Vec<_>>(),
        [ContentType::Trades, ContentType::Depth]
    );
    let err = resolve_ranked_merge_streams(&resolved, workspace.path(), Some(&job)).unwrap_err();
    assert!(
        err.to_string()
            .contains("reconstruction-state dependencies"),
        "{err}"
    );
    assert!(!job.output.dataset_path.exists());
}
