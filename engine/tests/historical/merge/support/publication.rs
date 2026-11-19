//! Tiny task-layout publications for contracts exercised by the merge executor.
use crate::materialize_partition::{
    canonical_depth, canonical_trades, write_depth_events, write_parquet,
};
use marketforge_engine::{
    book::BookSegment,
    historical::{
        job::{
            ContentType, MergeFileInput, MergeJobDocument, MergeOperation, MergeOutput,
            MergePublication, MergeTimeRange, ProcessingJob,
        },
        normalize::{
            boundary::DepthBoundaryManifest,
            manifest::{
                AuxiliaryFile, AuxiliaryFileKind, DatasetFile, DatasetManifest, file_sha256,
                write_publication_checksums,
            },
            metrics::{
                IntegrityStatus, ProcessingCounters, ProcessingMetricsReport, TaskMetricsReport,
            },
        },
    },
    shared::{
        domain::canonical::{DepthEventBoundary, DepthSourceOperation, Exchange},
        persistence::parquet::{DepthSegmentManifest, IndexedDepthEvent},
    },
};
use serde_json::json;
use std::{fs, path::Path};

pub const TRADE_STREAM: &str = "bybit:perpetual:linear:BTCUSDT:trade";
pub const DEPTH_STREAM: &str = "bybit:perpetual:linear:BTCUSDT:l2";
pub const START: i64 = 100;
pub const END: i64 = 500;

pub fn write_job(directory: &Path, job: &ProcessingJob) {
    let path = directory.join("processing-job.json");
    fs::write(&path, serde_json::to_vec_pretty(job).unwrap()).unwrap();
    fs::write(
        directory.join("processing-job.sha256"),
        format!("{}  processing-job.json\n", file_sha256(&path).unwrap()),
    )
    .unwrap();
}

pub fn make_job(root: &Path, trade_ids: &[u64], depth_ids: &[u64]) -> MergeJobDocument {
    let inputs = trade_ids
        .iter()
        .flat_map(|&id| create_publication(root, id, ContentType::Trades))
        .chain(
            depth_ids
                .iter()
                .flat_map(|&id| create_publication(root, id, ContentType::Depth)),
        )
        .collect::<Vec<_>>();
    fixture_document(root, inputs)
}

fn fixture_document(root: &Path, mut inputs: Vec<MergeFileInput>) -> MergeJobDocument {
    assert!(!inputs.is_empty() && inputs.len() <= 8);
    let mut ranks = std::collections::HashMap::new();
    for input in &mut inputs {
        let next = ranks.len() as u32;
        input.stream_rank = *ranks
            .entry((input.publication.engine_dataset_id, input.stream_id.clone()))
            .or_insert(next);
    }
    MergeJobDocument {
        job_id: 900_500,
        operation: MergeOperation::Merge,
        inputs,
        time_range: MergeTimeRange {
            start_ns: START,
            end_ns: END,
        },
        output: MergeOutput {
            dataset_path: root.join("merged"),
        },
    }
}

pub fn create_publication(root: &Path, id: u64, kind: ContentType) -> Vec<MergeFileInput> {
    publication_with_rows(root, id, kind, 2, false)
}

/// Enough rows to cross the executor's 16,384-event window once.
/// Distinct 256-byte trade IDs keep the first compressed file above 1 MiB.
pub fn rotating_trade_publication(root: &Path, id: u64) -> Vec<MergeFileInput> {
    publication_with_rows(root, id, ContentType::Trades, 8_193, false)
}

fn publication_with_rows(
    root: &Path,
    id: u64,
    kind: ContentType,
    trade_rows: usize,
    overlapping_snapshot: bool,
) -> Vec<MergeFileInput> {
    let directory = root.join(format!("input-{id}"));
    let is_trade = kind == ContentType::Trades;
    let (folder, target, stream) = if is_trade {
        ("trades", "trade", TRADE_STREAM)
    } else {
        ("depth", "depth", DEPTH_STREAM)
    };
    let mut auxiliary_files = Vec::new();
    let mut files = Vec::new();
    let mut tasks = Vec::new();
    let mut metrics = ProcessingMetricsReport::default();
    // Two source tasks with a boundary between timestamps 200 and 300.
    let second_start = if overlapping_snapshot { 190 } else { 300 };
    for (task_id, start, end) in [(0, 100, 200), (1, second_start, 400)] {
        let relative = format!("tasks/task-{task_id:06}/{folder}");
        let task_dir = directory.join(&relative);
        fs::create_dir_all(&task_dir).unwrap();
        let raw_path = task_dir.join("source.txt");
        fs::write(&raw_path, format!("{start}\n{end}\n")).unwrap();
        tasks.push(json!({
            "task_id": task_id, "stream_id": stream,
            "input_path": raw_path, "format_code": "MERGE-TEST",
            "exchange": "bybit", "instrument_id": 688, "symbol": "BTCUSDT",
            "raw_schema": {},
            "instrument": {"instrument_kind": "perpetual", "contract_kind": "linear", "tick_size": "0.10"},
            "normalizations": [{"timestamp_encoding": "nanoseconds", "quantity_encoding": "base",
                "target_schema": target, "rules": {}}],
            "source_ordering": "source_ordered", "source_compression": "plain",
        }));
        let path = format!("{relative}/part-000000.parquet");
        let batch = if is_trade && trade_rows > 2 {
            use arrow_array::StringArray;
            use std::sync::Arc;

            let timestamps = (0..trade_rows)
                .map(|row| start + 100 * row as i64 / (trade_rows as i64 - 1))
                .collect::<Vec<_>>();
            let mut state = 37 + task_id;
            let ids = StringArray::from_iter_values((0..trade_rows).map(|_| {
                (0..256)
                    .map(|_| {
                        state ^= state << 13;
                        state ^= state >> 7;
                        state ^= state << 17;
                        char::from(b'!' + (state % 94) as u8)
                    })
                    .collect::<String>()
            }));
            let batch = canonical_trades(&timestamps);
            let mut columns = batch.columns().to_vec();
            columns[batch.schema().index_of("trade_id").unwrap()] = Arc::new(ids);
            arrow_array::RecordBatch::try_new(batch.schema(), columns).unwrap()
        } else if is_trade {
            canonical_trades(&[start, end])
        } else {
            let batch = canonical_depth(&[start, end]);
            let mut columns = batch.columns().to_vec();
            let quantity = if overlapping_snapshot && task_id == 1 {
                [2, 3]
            } else {
                [1, 2]
            };
            columns[batch.schema().index_of("quantity_base").unwrap()] = std::sync::Arc::new(
                arrow_array::Decimal256Array::from(
                    quantity
                        .map(|value| arrow_buffer::i256::from_i128(value * 10_i128.pow(28)))
                        .to_vec(),
                )
                .with_precision_and_scale(76, 28)
                .unwrap(),
            );
            arrow_array::RecordBatch::try_new(batch.schema(), columns).unwrap()
        };
        let rows = batch.num_rows() as u64;
        write_parquet(&directory.join(&path), &batch);
        files.push(DatasetFile {
            size_bytes: fs::metadata(directory.join(&path)).unwrap().len(),
            path,
            rows,
            start_timestamp_ns: Some(start),
            end_timestamp_ns: Some(end),
        });
        metrics
            .add_task(TaskMetricsReport {
                failure: None,
                source: None,
                accepted_depth_events: (!is_trade).then_some(2),
                task_id,
                stream_id: stream.into(),
                counters: ProcessingCounters {
                    records_read: rows,
                    records_matched: rows,
                    records_processed: rows,
                    events_normalized: rows,
                    events_written: rows,
                    ..Default::default()
                },
                integrity: Default::default(),
                scoped_integrity: Default::default(),
                start_timestamp_ns: Some(start),
                end_timestamp_ns: Some(end),
            })
            .unwrap();
        if !is_trade {
            let events = [start, end]
                .into_iter()
                .enumerate()
                .map(|(ordinal, timestamp)| {
                    let initialization = ordinal == 0;
                    IndexedDepthEvent {
                        event_ordinal: ordinal as u64,
                        event_timestamp_ns: timestamp,
                        canonical_row_offset: ordinal as u64,
                        canonical_row_count: 1,
                        system_timestamp_ns: None,
                        sequence_start: overlapping_snapshot.then_some(task_id + ordinal as u64),
                        sequence_end: overlapping_snapshot.then_some(task_id + ordinal as u64),
                        event_boundary: if initialization {
                            DepthEventBoundary::Initialization
                        } else {
                            DepthEventBoundary::Changes
                        },
                        source_operation: if initialization {
                            DepthSourceOperation::Snapshot
                        } else {
                            DepthSourceOperation::AbsoluteUpdate
                        },
                    }
                })
                .collect::<Vec<_>>();
            write_depth_events(&task_dir.join("events.parquet"), &events);
            let segments = DepthSegmentManifest::new(
                2,
                vec![BookSegment {
                    segment_id: 0,
                    start_timestamp_ns: start,
                    initial_event_offset: 0,
                    initial_level_count: 1,
                }],
            )
            .unwrap();
            fs::write(
                task_dir.join("segments.json"),
                serde_json::to_vec(&segments).unwrap(),
            )
            .unwrap();
            let mut boundary =
                DepthBoundaryManifest::new(Exchange::Bybit, 688, "BTCUSDT".into(), stream.into());
            boundary.first_timestamp_ns = Some(start);
            boundary.last_timestamp_ns = Some(end);
            if overlapping_snapshot {
                use marketforge_engine::shared::domain::book::boundary::{
                    BoundaryBookLevel, BoundaryBookState, BoundarySnapshot,
                };
                let snapshot = |sequence, timestamp, quantity: i64| {
                    BoundarySnapshot::new(
                        Some(sequence),
                        timestamp,
                        BoundaryBookState {
                            bids: vec![BoundaryBookLevel {
                                price: 78_500.into(),
                                quantity_base: Some(quantity.into()),
                                quantity_quote: None,
                                quantity_contracts: None,
                                order_count: None,
                            }],
                            asks: vec![],
                        },
                        None,
                    )
                };
                boundary.first_sequence = Some(task_id);
                boundary.last_sequence = Some(task_id + 1);
                boundary.initial = Some(snapshot(task_id, start, task_id as i64 + 1));
                boundary.final_state = Some(snapshot(task_id + 1, end, task_id as i64 + 2));
            }

            fs::write(
                task_dir.join("boundary.json"),
                serde_json::to_vec(&boundary).unwrap(),
            )
            .unwrap();
            for (name, kind) in [
                ("events.parquet", AuxiliaryFileKind::DepthEvents),
                ("segments.json", AuxiliaryFileKind::DepthSegments),
                ("boundary.json", AuxiliaryFileKind::DepthBoundary),
            ] {
                auxiliary_files.push(AuxiliaryFile {
                    path: format!("{relative}/{name}"),
                    kind,
                    size_bytes: fs::metadata(task_dir.join(name)).unwrap().len(),
                });
            }
        }
    }
    let job_id = id + 1000;
    let manifest = DatasetManifest {
        protocol_version: 3,
        job_id,
        dataset_id: id,
        content_type: kind,
        processing_quality: None,
        events_written: files.iter().map(|file| file.rows).sum(),
        files_written: 2,
        start_timestamp_ns: Some(100),
        end_timestamp_ns: Some(400),
        source_tasks: vec![0, 1],
        instrument_namespace: None,
        resolved_instruments: vec![],
        streams: vec![],
        files,
        auxiliary_files,
        integrity_status: IntegrityStatus::Clean,
        integrity_evaluation: Default::default(),
        processing_metrics: metrics,
    };
    let rule = json!({"action": "fail"});
    let job = serde_json::from_value(json!({
        "protocol_version": 1, "job_id": job_id, "dataset_id": id, "operation":"process",
        "tasks": tasks, "streams": [{"stream_id": stream, "stream_rank": 0}],
        "ordering": {"primary": "event_timestamp_ns", "tie_break": "stream_rank"},
        "integrity_policy": {"enabled": true, "parse_failure": rule, "invalid_record": rule,
            "sequence_gap": rule, "timestamp_regression": rule, "missing_snapshot": rule,
            "invalid_book": rule, "transformation_failure": rule},
        "resources": {"workers": 1, "memory_budget_bytes": 268435456, "scratch_path": root.join("scratch"),
            "scratch_budget_bytes": 268435456,
            "parquet": {"row_group_target_bytes": 1048576, "file_target_bytes": 4194304}},
        "output": {"content_type": kind, "staging_path": root.join(format!("staging-{id}")), "dataset_path": directory}
    })).unwrap();
    write_job(&directory, &job);
    finish_publication(&directory, &manifest)
}

fn finish_publication(directory: &Path, manifest: &DatasetManifest) -> Vec<MergeFileInput> {
    manifest.validate_streams().unwrap();
    let manifest_path = directory.join("manifest.json");
    fs::write(&manifest_path, serde_json::to_vec_pretty(manifest).unwrap()).unwrap();
    write_publication_checksums(
        directory,
        manifest
            .files
            .iter()
            .map(|file| (&file.path).into())
            .chain(
                manifest
                    .auxiliary_files
                    .iter()
                    .map(|file| (&file.path).into()),
            )
            .chain(
                [
                    "manifest.json",
                    "processing-job.json",
                    "processing-job.sha256",
                ]
                .map(Into::into),
            ),
    )
    .unwrap();
    let publication = MergePublication {
        engine_dataset_id: manifest.dataset_id,
        manifest_sha256: file_sha256(&manifest_path).unwrap(),
        manifest_path,
    };
    manifest
        .files
        .iter()
        .enumerate()
        .map(|(index, file)| {
            let path = directory.join(&file.path);
            MergeFileInput {
                stream_id: manifest
                    .streams
                    .iter()
                    .find(|stream| {
                        Path::new(&file.path).parent() == Some(Path::new(&stream.directory))
                    })
                    .map_or_else(
                        || {
                            if manifest.content_type == ContentType::Trades {
                                TRADE_STREAM.into()
                            } else {
                                DEPTH_STREAM.into()
                            }
                        },
                        |stream| stream.stream_id.clone(),
                    ),
                stream_rank: manifest.dataset_id as u32,
                content_type: manifest.content_type,
                dataset_id: 10_000 + manifest.dataset_id * 10 + index as u64,
                sha256: file_sha256(&path).unwrap(),
                path,
                publication: publication.clone(),
            }
        })
        .collect()
}

pub fn resolve_fixture(
    root: &Path,
    document: MergeJobDocument,
) -> Vec<marketforge_engine::historical::merge::ResolvedMergeInput> {
    marketforge_engine::historical::merge::resolve_merge_inputs(
        &crate::merge_test_job::bounded_runtime(document),
        root,
    )
    .unwrap()
}

pub fn trade_publication(
    root: &Path,
    id: u64,
) -> marketforge_engine::historical::merge::ResolvedMergeInput {
    resolve_fixture(root, make_job(root, &[id], &[])).remove(0)
}

pub fn depth_publication(
    root: &Path,
    id: u64,
) -> marketforge_engine::historical::merge::ResolvedMergeInput {
    resolve_fixture(root, make_job(root, &[], &[id])).remove(0)
}

pub fn depth_job(root: &Path) -> marketforge_engine::historical::job::MergeJob {
    crate::merge_test_job::bounded_runtime(make_job(root, &[], &[105, 108]))
}

pub fn trade_with_empty_task(
    root: &Path,
    id: u64,
) -> marketforge_engine::historical::merge::ResolvedMergeInput {
    let mut document = make_job(root, &[id], &[]);
    let path = document.inputs[0].publication.manifest_path.clone();
    let directory = path.parent().unwrap();
    let mut manifest: DatasetManifest = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    manifest.source_tasks.push(2);
    let mut job =
        marketforge_engine::historical::job::load_committed_processing_job(directory).unwrap();
    let mut empty = job.tasks[0].clone();
    empty.task_id.0 = 2;
    job.tasks.push(empty);
    write_job(directory, &job);
    manifest
        .processing_metrics
        .add_task(TaskMetricsReport {
            failure: None,
            source: None,
            accepted_depth_events: None,
            task_id: 2,
            stream_id: TRADE_STREAM.into(),
            counters: ProcessingCounters::default(),
            integrity: Default::default(),
            scoped_integrity: Default::default(),
            start_timestamp_ns: None,
            end_timestamp_ns: None,
        })
        .unwrap();
    document.inputs = finish_publication(directory, &manifest);
    resolve_fixture(root, document).remove(0)
}

pub fn bounded_trade_job(output: std::path::PathBuf) -> MergeJobDocument {
    let mut job = make_job(output.parent().unwrap(), &[11], &[]);
    job.output.dataset_path = output;
    job
}

/// Two tasks contribute to a single primary part with a wider timestamp envelope
/// than either task. This exercises selection of stream-layout publications.
pub fn shared_part_trade_job(root: &Path) -> MergeJobDocument {
    shared_part_trade_publication(root, 11, 0, None)
}

pub fn shared_part_trade_publication(
    root: &Path,
    id: u64,
    offset: i64,
    namespace: Option<&str>,
) -> MergeJobDocument {
    use marketforge_engine::historical::normalize::streams::{SourceSpan, StreamOutput};

    let mut job = make_job(root, &[id], &[]);
    let path = job.inputs[0].publication.manifest_path.clone();
    let directory = path.parent().unwrap();
    let mut manifest: DatasetManifest = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    let stream_directory = format!("streams/{}/trades", "1".repeat(64));
    fs::create_dir_all(directory.join(&stream_directory)).unwrap();
    let part = format!("{stream_directory}/part-000000.parquet");
    let part_path = directory.join(&part);
    let timestamps = [100, 200, 300, 400].map(|ts| ts + offset);
    write_parquet(&part_path, &canonical_trades(&timestamps));
    manifest.start_timestamp_ns = Some(timestamps[0]);
    manifest.end_timestamp_ns = Some(timestamps[3]);
    manifest.instrument_namespace = namespace.map(str::to_owned);
    for metrics in &mut manifest.processing_metrics.tasks {
        metrics.start_timestamp_ns = metrics.start_timestamp_ns.map(|value| value + offset);
        metrics.end_timestamp_ns = metrics.end_timestamp_ns.map(|value| value + offset);
    }
    if let Some(namespace) = namespace {
        let mut processing =
            marketforge_engine::historical::job::load_committed_processing_job(directory).unwrap();
        for task in &mut processing.tasks {
            task.raw_schema["marketforge_source"] = json!({"instrument_namespace": namespace});
        }
        write_job(directory, &processing);
    }
    let size_bytes = fs::metadata(&part_path).unwrap().len();
    for old in &manifest.files {
        fs::remove_file(directory.join(&old.path)).unwrap();
    }
    manifest.protocol_version = 5;
    manifest.processing_quality = Some(
        serde_json::from_value(json!({
            "effective_policies": null,
            "timestamp_ordering": "event_timestamp_ns",
            "native_reconstruction_valid": false,
            "supplied_observations_preserved": true,
            "continuity_proven": false,
            "external_historical_coverage_established": false,
            "inherited_metadata_is_batch_assertion": false,
            "logical_source_events": 4,
            "verified_duplicate_observations": 0,
            "known_unique_event_exclusions": 0,
            "unverified_observation_exclusions": 0,
            "loss_rate_denominator": 4,
            "integrity_rejected_physical_rows": 0
        }))
        .unwrap(),
    );
    manifest.files = vec![DatasetFile {
        path: part,
        rows: 4,
        size_bytes,
        start_timestamp_ns: Some(timestamps[0]),
        end_timestamp_ns: Some(timestamps[3]),
    }];
    manifest.files_written = 1;
    manifest.streams = vec![StreamOutput {
        stream_id: TRADE_STREAM.into(),
        directory: stream_directory,
        sources: vec![
            SourceSpan {
                task_id: 0,
                start_row: 0,
                end_row: 2,
                ranges: vec![],
            },
            SourceSpan {
                task_id: 1,
                start_row: 2,
                end_row: 4,
                ranges: vec![],
            },
        ],
    }];
    job.inputs = finish_publication(directory, &manifest);
    job
}

pub fn depth_with_overlapping_snapshot(
    root: &Path,
    id: u64,
) -> marketforge_engine::historical::merge::ResolvedMergeInput {
    let inputs = publication_with_rows(root, id, ContentType::Depth, 2, true);
    resolve_fixture(root, fixture_document(root, inputs)).remove(0)
}
