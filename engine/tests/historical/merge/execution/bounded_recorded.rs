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
    job.timeline_ordering = None;
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
fn bounded_v5_depth_commits_real_regression() {
    verify_recorded_causal(false);
}

#[test]
#[ignore = "opt-in compatibility with canonical normalized publications"]
fn bounded_v5_trade_streams_from_one_publication_commit() {
    use marketforge_engine::historical::merge::resolve_merge_inputs;
    let workspace = tempfile::tempdir().unwrap();
    let mut job = recorded_job();
    job.timeline_ordering = None;
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
fn bounded_v5_combined_commits_real_regression() {
    verify_recorded_causal(true);
}

fn verify_recorded_causal(combined: bool) {
    use arrow_array::{BooleanArray, ListArray, StructArray, UInt8Array};
    use marketforge_engine::historical::job::MergeTimelineOrdering;
    use marketforge_engine::shared::persistence::parquet::DepthStreamBoundaries;
    use parquet::arrow::arrow_reader::RowSelection;

    let workspace = tempfile::tempdir().unwrap();
    let mut job = recorded_job();
    // Gate.io snapshot stream with a recorded 1 ms regression. Its source
    // prefix is bounded to one hour of index space and only a few thousand events.
    let depth = job
        .inputs
        .iter()
        .find(|file| file.dataset_id == 41)
        .unwrap()
        .clone();
    let trade = job
        .inputs
        .iter()
        .find(|file| file.dataset_id == 119)
        .unwrap()
        .clone();
    assert!(depth.path.metadata().unwrap().len() < 24 * 1024 * 1024);
    assert!(trade.path.metadata().unwrap().len() < 1024 * 1024);
    assert_eq!(job.timeline_ordering, Some(MergeTimelineOrdering::Causal));
    job.inputs = if combined {
        vec![trade, depth.clone()]
    } else {
        vec![depth.clone()]
    };
    let start = 1793080538547000000;
    let end = if combined {
        1793080650602000001
    } else {
        start + 1_000_001
    };
    job.time_range.start_ns = start;
    job.time_range.end_ns = end;
    job.output.dataset_path = workspace.path().join("merged");

    let directory = depth.path.parent().unwrap();
    let boundaries: DepthStreamBoundaries =
        serde_json::from_reader(File::open(directory.join("boundary.json")).unwrap()).unwrap();
    let index = directory.join("events.parquet");
    assert!(index.metadata().unwrap().len() < 12 * 1024 * 1024);
    let mut index_rows = Vec::new();
    for batch in read(&index) {
        let u64s = |name| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
        };
        let times = batch
            .column_by_name("event_timestamp_ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let bounds = batch
            .column_by_name("event_boundary")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        let operations = batch
            .column_by_name("source_operation")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            index_rows.push((
                times.value(row),
                u64s("event_ordinal").value(row),
                u64s("canonical_row_offset").value(row),
                u64s("canonical_row_count").value(row),
                bounds.value(row),
                operations.value(row),
            ));
        }
    }

    let mut expected = Vec::new();
    let mut canonical_ranges: Vec<std::ops::Range<usize>> = Vec::new();
    for source in &boundaries.sources {
        let rows = &index_rows[source.start_event as usize..source.end_event as usize];
        {
            let starts = rows
                .iter()
                .enumerate()
                .filter_map(|(i, row)| (row.4 == 1).then_some(i))
                .chain(std::iter::once(rows.len()))
                .collect::<Vec<_>>();
            for pair in starts.windows(2) {
                let segment = &rows[pair[0]..pair[1]];
                let Some(last) = segment
                    .iter()
                    .rposition(|row| (start..end).contains(&row.0))
                else {
                    continue;
                };
                for &(time, ordinal, offset, count, boundary, operation) in &segment[..=last] {
                    expected.push((
                        time,
                        source.task_id,
                        ordinal,
                        offset - source.start_row,
                        count,
                        boundary,
                        operation,
                        !(start..end).contains(&time),
                    ));
                    if count > 0 {
                        let range = offset as usize..(offset + count) as usize;
                        if let Some(previous) = canonical_ranges
                            .last_mut()
                            .filter(|previous| previous.end == range.start)
                        {
                            previous.end = range.end;
                        } else {
                            canonical_ranges.push(range);
                        }
                    }
                }
            }
        }
    }
    assert!(expected.len() <= 4000);
    assert!(
        expected
            .windows(2)
            .any(|rows| rows[0].0 > rows[1].0 && !rows[0].7 && !rows[1].7)
    );
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(File::open(&depth.path).unwrap()).unwrap();
    let total = builder.metadata().file_metadata().num_rows() as usize;
    let schema = builder.schema().clone();
    let selection = RowSelection::from_consecutive_ranges(canonical_ranges.into_iter(), total);
    let payload_batches = builder
        .with_row_selection(selection)
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let payload = arrow_select::concat::concat_batches(&schema, &payload_batches).unwrap();

    let result =
        execute_unified_merge_job(&bounded_runtime(job.clone()), workspace.path()).unwrap();
    validate_committed_merge(&result.dataset_path).unwrap();
    let manifest: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(result.manifest_path).unwrap()).unwrap();
    assert_eq!(manifest.timeline_ordering, MergeTimelineOrdering::Causal);
    assert_eq!(manifest.depth_events as usize, expected.len());
    assert_eq!(manifest.trade_events, u64::from(combined));
    assert_eq!(
        manifest.reconstruction_events as usize,
        expected.iter().filter(|row| row.7).count()
    );
    let mut sequence = 0;
    let mut depth_index = 0;
    let mut payload_row = 0;
    for file in &manifest.files {
        for batch in read(&result.dataset_path.join(&file.path)) {
            let u64s = |name| {
                batch
                    .column_by_name(name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
            };
            let times = batch
                .column_by_name("event_timestamp_ns")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let context = batch
                .column_by_name("reconstruction_only")
                .unwrap()
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert_eq!(u64s("timeline_sequence").value(row), sequence);
                sequence += 1;
                let field = |name| {
                    if combined {
                        batch
                            .column_by_name("depth")
                            .unwrap()
                            .as_any()
                            .downcast_ref::<StructArray>()
                            .unwrap()
                            .column_by_name(name)
                    } else {
                        batch.column_by_name(name)
                    }
                };
                if combined && batch.column_by_name("depth").unwrap().is_null(row) {
                    assert!(!context.value(row));
                    assert_eq!(times.value(row), 1793080650602000000);
                    continue;
                }
                assert_eq!(
                    u64s("dataset_id").value(row),
                    depth.publication.engine_dataset_id
                );
                let boundary = field("event_boundary")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .unwrap()
                    .value(row);
                let operation = field("source_operation")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .unwrap()
                    .value(row);
                let count = u64s("canonical_row_count").value(row);
                assert_eq!(
                    (
                        times.value(row),
                        u64s("task_id").value(row),
                        u64s("event_ordinal").value(row),
                        u64s("canonical_row_offset").value(row),
                        count,
                        boundary,
                        operation,
                        context.value(row)
                    ),
                    expected[depth_index]
                );
                let changes = field("changes")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap()
                    .value(row);
                let changes = changes.as_any().downcast_ref::<StructArray>().unwrap();
                assert_eq!(changes.len(), count as usize);
                for (field, column) in changes.fields().iter().zip(changes.columns()) {
                    assert_eq!(
                        column.as_ref(),
                        payload
                            .column_by_name(field.name())
                            .unwrap()
                            .slice(payload_row, count as usize)
                            .as_ref(),
                        "{}",
                        field.name()
                    );
                }
                depth_index += 1;
                payload_row += count as usize;
            }
        }
    }
    assert_eq!(sequence, manifest.events_written);
    assert_eq!(depth_index, expected.len());
    assert_eq!(payload_row, payload.num_rows());

    job.timeline_ordering = Some(MergeTimelineOrdering::Chronological);
    job.output.dataset_path = workspace.path().join("chronological");
    let error =
        execute_unified_merge_job(&bounded_runtime(job.clone()), workspace.path()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("chronological ordering cannot consume")
    );
    assert!(!job.output.dataset_path.exists());
}
