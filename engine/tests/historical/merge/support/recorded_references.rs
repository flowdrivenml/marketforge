use crate::{
    merge_test_job::{bounded_runtime, recorded_job},
    recorded_datasets::{recorded_manifest, recorded_members, recorded_stream, stream_parts},
};
use arrow_array::{Int64Array, UInt64Array};
use marketforge_engine::historical::{
    job::{ContentType, MergeFileInput, MergeJobDocument},
    merge::{
        DepthStream, DepthTaskInventory, KWayEventMerger, MergeEventRef, MergeEventSource,
        MergedEvent, TradeEventRef, TradeStream, UnifiedReferenceValidator, UnifiedTimelineRecord,
        build_trade_inventory, resolve_merge_inputs,
    },
    normalize::manifest::{AuxiliaryFileKind, DatasetFile},
};
use marketforge_engine::shared::{
    domain::canonical::{DepthEventBoundary, DepthSourceOperation},
    persistence::parquet::{
        BoundaryReadLimits, DepthSegmentManifest, collect_depth_stream_boundaries,
        read_depth_segments,
    },
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::{fs::File, path::Path};

fn trade_candidates(
    document: &MergeJobDocument,
) -> impl Iterator<Item = (MergeFileInput, DatasetFile)> {
    recorded_members(document, ContentType::Trades)
        .into_iter()
        .filter(|(_, file)| (1..=256).contains(&file.rows) && file.size_bytes <= 1024 * 1024)
}

fn expected_trade_references(
    document: &MergeJobDocument,
    input: &MergeFileInput,
    member: &DatasetFile,
) -> Vec<TradeEventRef> {
    let owner = recorded_stream(input);
    let first_row = stream_parts(document, input)
        .into_iter()
        .find(|(part, _)| part == input)
        .unwrap()
        .1
        .start;
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(File::open(&input.path).unwrap()).unwrap();
    assert_eq!(
        reader.metadata().file_metadata().num_rows() as u64,
        member.rows
    );
    assert!(
        reader
            .metadata()
            .row_groups()
            .iter()
            .all(|group| group.total_byte_size() <= 1024 * 1024)
    );
    let mut global_row = first_row;
    let mut expected = Vec::new();
    for batch in reader.build().unwrap() {
        let batch = batch.unwrap();
        let timestamps = batch
            .column_by_name("event_timestamp_ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for timestamp in timestamps.values() {
            let source = owner
                .sources
                .iter()
                .find(|source| (source.start_row..source.end_row).contains(&global_row))
                .unwrap();
            let ordinal = global_row - source.start_row;
            expected.push(TradeEventRef {
                dataset_id: input.publication.engine_dataset_id,
                task_id: source.task_id,
                event_ordinal: ordinal,
                canonical_row_offset: ordinal,
                event_timestamp_ns: *timestamp,
            });
            global_row += 1;
        }
    }
    assert_eq!(expected.len() as u64, member.rows);
    expected
}

pub fn validate_trade_references() {
    let workspace = tempfile::tempdir().unwrap();
    let mut document = recorded_job();
    let (input, member) = trade_candidates(&document).next().expect(
        "canonical trade reference fixture needs a primary with 1–256 rows and at most 1 MiB",
    );
    let id = input.publication.engine_dataset_id;
    document.inputs = vec![input.clone()];
    document.output.dataset_path = workspace.path().join("unused");
    let resolved =
        resolve_merge_inputs(&bounded_runtime(document.clone()), workspace.path()).unwrap();
    let inventory = build_trade_inventory(&resolved).unwrap();
    assert!(
        inventory
            .iter()
            .flat_map(|task| &task.files)
            .all(|file| file.path == input.path)
    );
    let mut validator = UnifiedReferenceValidator::new(&inventory, &[]).unwrap();
    let mut stream = TradeStream::new(id, inventory).unwrap();
    for expected in expected_trade_references(&document, &input, &member) {
        let event = stream.next_event().unwrap().unwrap();
        assert_eq!(event, expected);
        validator
            .validate(&UnifiedTimelineRecord::from(&MergedEvent {
                stream_rank: 0,
                event: MergeEventRef::Trade(event),
            }))
            .unwrap();
    }
    assert!(stream.next_event().unwrap().is_none());
    println!(
        "recorded trade references: one primary, {} rows, {} bytes",
        member.rows, member.size_bytes
    );
}

pub fn check_kway_trade_references() {
    let workspace = tempfile::tempdir().unwrap();
    let mut document = recorded_job();
    let pairs = {
        let mut candidates = trade_candidates(&document);
        let first = candidates
            .next()
            .expect("need a bounded recorded trade part");
        let second = candidates
            .find(|(input, _)| {
                recorded_stream(input).stream_id != recorded_stream(&first.0).stream_id
            })
            .expect("need a second nonempty bounded recorded trade stream");
        [first, second]
    };
    document.inputs = pairs.iter().map(|(input, _)| input.clone()).collect();
    document.output.dataset_path = workspace.path().join("unused");
    let resolved =
        resolve_merge_inputs(&bounded_runtime(document.clone()), workspace.path()).unwrap();
    let inventory = build_trade_inventory(&resolved).unwrap();
    let mut streams: Vec<Box<dyn MergeEventSource>> = Vec::new();
    let mut expected = Vec::new();
    for (rank, (input, member)) in pairs.iter().enumerate() {
        let id = input.publication.engine_dataset_id;
        let parts = inventory
            .iter()
            .filter(|task| {
                task.dataset_id == id && task.files.iter().any(|file| file.path == input.path)
            })
            .cloned()
            .collect();
        streams.push(Box::new(TradeStream::new(id, parts).unwrap()));
        expected.extend(
            expected_trade_references(&document, input, member)
                .into_iter()
                .map(|event| (rank as u32, event)),
        );
    }
    expected.sort_by_key(|(rank, event)| {
        (
            event.event_timestamp_ns,
            *rank,
            event.task_id,
            event.event_ordinal,
        )
    });
    let mut merger = KWayEventMerger::new(streams).unwrap();
    for (rank, event) in expected {
        let merged = merger.next_event().unwrap().unwrap();
        assert_eq!(merged.stream_rank, rank);
        let MergeEventRef::Trade(actual) = merged.event else {
            panic!("expected a trade")
        };
        assert_eq!(actual, event);
    }
    assert!(merger.next_event().unwrap().is_none());
    println!(
        "recorded K-way trade: two concrete streams, two primaries, {} rows",
        merger.events_emitted()
    );
}

pub fn validate_depth_references() {
    const SAMPLE: usize = 16;
    let workspace = tempfile::tempdir().unwrap();
    let mut document = recorded_job();
    let (input, member) = recorded_members(&document, ContentType::Depth)
        .into_iter()
        .find(|(_, file)| file.size_bytes <= 8 * 1024 * 1024)
        .expect("canonical depth reference fixture needs a primary at most 8 MiB");
    let manifest = recorded_manifest(&input);
    let root = input.publication.manifest_path.parent().unwrap();
    let owner = manifest
        .streams
        .iter()
        .find(|stream| input.path.parent() == Some(root.join(&stream.directory).as_path()))
        .unwrap();
    let directory = root.join(&owner.directory);
    let source = owner
        .sources
        .first()
        .expect("need an initializing depth source");
    assert_eq!(source.start_row, 0);
    assert!(source.ranges.is_empty());
    let auxiliary = |kind, max_bytes| {
        let entry = manifest
            .auxiliary_files
            .iter()
            .find(|file| {
                file.kind == kind
                    && Path::new(&file.path).parent() == Some(Path::new(&owner.directory))
            })
            .expect("selected depth stream is missing a required sidecar");
        assert!(
            entry.size_bytes <= max_bytes,
            "selected {kind:?} sidecar exceeds its byte limit"
        );
        root.join(&entry.path)
    };
    let events_path = auxiliary(AuxiliaryFileKind::DepthEvents, 2 * 1024 * 1024);
    let segments_path = auxiliary(AuxiliaryFileKind::DepthSegments, 16 * 1024);
    let boundary_path = auxiliary(AuxiliaryFileKind::DepthBoundary, 64 * 1024);
    document.inputs = vec![input];
    document.output.dataset_path = workspace.path().join("unused");
    resolve_merge_inputs(&bounded_runtime(document), workspace.path()).unwrap();
    let boundaries = collect_depth_stream_boundaries(
        &directory,
        BoundaryReadLimits {
            max_sources: 16,
            max_source_bytes: 64 * 1024,
            max_string_bytes: 1024,
            max_book_levels: 10_000,
        },
    )
    .unwrap();
    let boundary = boundaries.sources.first().unwrap();
    assert_eq!(
        (boundary.task_id, boundary.start_row, boundary.end_row),
        (source.task_id, source.start_row, source.end_row)
    );
    assert_eq!(boundary.start_event, 0);
    assert!(boundary.end_event >= SAMPLE as u64);
    let segments = read_depth_segments(&directory).unwrap();
    let initial = segments.segments.first().unwrap();
    assert_eq!(initial.initial_event_offset, 0);
    assert!((1..=10_000).contains(&initial.initial_level_count));
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(File::open(&events_path).unwrap()).unwrap();
    let group = reader.metadata().row_group(0);
    assert!(
        (SAMPLE as i64..=65_536).contains(&group.num_rows()),
        "first depth index row group exceeds the bounded decode budget"
    );
    assert!(group.total_byte_size() <= 4 * 1024 * 1024);
    let expected = reader
        .with_batch_size(SAMPLE)
        .with_row_groups(vec![0])
        .build()
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    let numbers = |name| {
        expected
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
    };
    let timestamps = expected
        .column_by_name("event_timestamp_ns")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(
        numbers("canonical_row_count").value(0),
        initial.initial_level_count
    );
    let task = DepthTaskInventory {
        dataset_id: manifest.dataset_id,
        task_id: source.task_id,
        canonical_files: vec![],
        canonical_slices: None,
        source_range: Some((0, SAMPLE as u64, 0)),
        events_path,
        segments_path,
        boundary_path,
        canonical_rows: source.end_row,
        segments: DepthSegmentManifest::new(source.end_row, vec![initial.clone()]).unwrap(),
        boundary: (&boundary.boundary).into(),
    };
    let mut validator = UnifiedReferenceValidator::new(&[], std::slice::from_ref(&task)).unwrap();
    let mut stream = DepthStream::new(task.dataset_id, vec![task], &[]).unwrap();
    for row in 0..SAMPLE {
        let depth = stream.next_event().unwrap().unwrap();
        let event = &depth.event;
        if row == 0 {
            assert_eq!(
                (event.event_boundary, event.source_operation),
                (
                    DepthEventBoundary::Initialization,
                    DepthSourceOperation::Snapshot
                )
            );
        }
        assert_eq!(event.event_ordinal, numbers("event_ordinal").value(row));
        assert_eq!(
            event.canonical_row_offset,
            numbers("canonical_row_offset").value(row)
        );
        assert_eq!(
            event.canonical_row_count,
            numbers("canonical_row_count").value(row)
        );
        assert_eq!(event.event_timestamp_ns, timestamps.value(row));
        validator
            .validate(&UnifiedTimelineRecord::from(&MergedEvent {
                stream_rank: 0,
                event: MergeEventRef::Depth(depth),
            }))
            .unwrap();
    }
    assert!(stream.next_event().unwrap().is_none());
    println!(
        "recorded depth references: one {}-byte primary, {SAMPLE} indexed events",
        member.size_bytes
    );
}
