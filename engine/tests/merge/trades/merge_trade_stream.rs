#![cfg(feature = "historical")]

use std::{
    collections::HashSet,
    fs::File,
    path::{Path, PathBuf},
};

use marketforge_engine::historical::normalize::{
    manifest::DatasetManifest,
    merge::{ResolvedMergeInput, TradeStream, build_trade_inventory},
};

// -----------------------------------------------------------------------------
// Project root
// -----------------------------------------------------------------------------

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("project root")
        .to_path_buf()
}

// -----------------------------------------------------------------------------
// Resolve real trade dataset
// -----------------------------------------------------------------------------

fn resolve_trade_dataset(root: &Path, dataset_id: u64) -> ResolvedMergeInput {
    let datasets = root.join("data/.work/full-trade-test/datasets");

    for entry in std::fs::read_dir(&datasets).expect("read trade datasets") {
        let entry = entry.expect("read dataset entry");

        let dataset_path = entry.path();
        let manifest_path = dataset_path.join("manifest.json");

        if !manifest_path.is_file() {
            continue;
        }

        let manifest: DatasetManifest =
            serde_json::from_reader(File::open(&manifest_path).unwrap())
                .expect("parse dataset manifest");

        if manifest.dataset_id != dataset_id {
            continue;
        }

        let canonical_files = manifest
            .files
            .iter()
            .map(|file| dataset_path.join(&file.path))
            .collect();

        let auxiliary_files = manifest
            .auxiliary_files
            .iter()
            .map(|file| dataset_path.join(&file.path))
            .collect();

        return ResolvedMergeInput {
            dataset_id,
            dataset_path,
            manifest,
            canonical_files,
            auxiliary_files,
        };
    }

    panic!("trade dataset {dataset_id} not found");
}

// -----------------------------------------------------------------------------
// Full dataset validation
// -----------------------------------------------------------------------------

#[test]
fn streams_real_trade_datasets() {
    let root = project_root();

    let dataset_ids = [
        111_u64, // Gate.io
        109,     // Bitget
        115,     // Binance
        100,     // OKX
        97,      // Bybit
        99,      // Binance
    ];

    println!("\nMARKETFORGE — LOGICAL TRADE STREAMS");
    println!("{}", "=".repeat(90));

    let mut total_events = 0_u64;

    for dataset_id in dataset_ids {
        let input = resolve_trade_dataset(&root, dataset_id);

        let expected_events = input.manifest.events_written;

        let inventory = build_trade_inventory(&[input]).expect("build trade inventory");

        let mut stream = TradeStream::new(dataset_id, inventory).expect("construct trade stream");

        let mut count = 0_u64;
        let mut previous_timestamp = None;

        let mut previous_task = None;
        let mut previous_offset = None;

        let mut seen_tasks = HashSet::new();

        while let Some(event) = stream.next_event().expect("read trade event") {
            assert_eq!(event.dataset_id, dataset_id);

            // -------------------------------------------------------------
            // Chronological ordering
            // -------------------------------------------------------------

            if let Some(previous) = previous_timestamp {
                assert!(
                    event.event_timestamp_ns >= previous,
                    "dataset {dataset_id}: timestamp regression"
                );
            }

            // -------------------------------------------------------------
            // Task-local row offsets
            // -------------------------------------------------------------

            if previous_task == Some(event.task_id) {
                assert_eq!(
                    event.canonical_row_offset,
                    previous_offset.unwrap() + 1,
                    "dataset {dataset_id}: noncontiguous task offset"
                );
            } else {
                assert!(
                    seen_tasks.insert(event.task_id),
                    "dataset {dataset_id}: task revisited"
                );

                assert_eq!(
                    event.canonical_row_offset, 0,
                    "dataset {dataset_id}: task does not begin at offset zero"
                );
            }

            assert_eq!(event.event_ordinal, event.canonical_row_offset,);

            previous_timestamp = Some(event.event_timestamp_ns);
            previous_task = Some(event.task_id);
            previous_offset = Some(event.canonical_row_offset);

            count += 1;
        }

        assert_eq!(
            count, expected_events,
            "dataset {dataset_id}: event count mismatch"
        );

        assert_eq!(stream.events_emitted(), count);

        total_events += count;

        println!(
            "dataset={} | tasks={} | events={} | ordering=PASS | offsets=PASS",
            dataset_id,
            seen_tasks.len(),
            count,
        );
    }

    println!("{}", "=".repeat(90));
    println!("Total events : {}", total_events);
    println!("Result       : PASS");
}

// -----------------------------------------------------------------------------
// Timestamp-range validation
// -----------------------------------------------------------------------------

#[test]
fn streams_real_trade_timestamp_ranges() {
    let root = project_root();

    let dataset_ids = [111_u64, 109, 115, 100, 97, 99];

    let start_ns = 1788220800000000000_i64;
    let end_ns = start_ns + 3_600_000_000_000;

    println!("\nMARKETFORGE — TRADE STREAM RANGES");
    println!("{}", "=".repeat(90));

    for dataset_id in dataset_ids {
        let input = resolve_trade_dataset(&root, dataset_id);

        let inventory = build_trade_inventory(&[input]).expect("build trade inventory");

        // -------------------------------------------------------------
        // Sequential reference
        // -------------------------------------------------------------

        let mut sequential =
            TradeStream::new(dataset_id, inventory.clone()).expect("construct sequential stream");

        let mut expected = Vec::new();

        while let Some(event) = sequential.next_event().expect("read sequential event") {
            if event.event_timestamp_ns >= start_ns && event.event_timestamp_ns < end_ns {
                expected.push(event);
            }
        }

        // -------------------------------------------------------------
        // Indexed range stream
        // -------------------------------------------------------------

        let mut ranged = TradeStream::new_range(dataset_id, inventory, start_ns, end_ns)
            .expect("construct ranged stream");

        let mut count = 0_usize;

        while let Some(actual) = ranged.next_event().expect("read ranged event") {
            assert!(
                count < expected.len(),
                "dataset {dataset_id}: unexpected extra trade"
            );

            assert_eq!(
                actual, expected[count],
                "dataset {dataset_id}: trade mismatch at index {count}"
            );

            count += 1;
        }

        assert_eq!(
            count,
            expected.len(),
            "dataset {dataset_id}: missing trades"
        );

        println!("dataset={} | events={} | MATCH", dataset_id, count,);
    }

    println!("{}", "=".repeat(90));
    println!("TRADE STREAM RANGES: PASS");
}

#[test]
fn accepts_dataset_with_empty_source_task() {
    let root = project_root();

    let input = resolve_trade_dataset(&root, 120);

    assert_eq!(input.manifest.source_tasks, vec![0, 1, 2]);

    let inventory = build_trade_inventory(&[input]).expect("build trade inventory");

    let mut task_ids = inventory
        .iter()
        .map(|task| task.task_id)
        .collect::<Vec<_>>();

    task_ids.sort_unstable();

    assert_eq!(task_ids, vec![0, 2]);

    let total_rows: u64 = inventory.iter().map(|task| task.canonical_rows).sum();

    assert_eq!(total_rows, 162);

    println!("\nMARKETFORGE — EMPTY TRADE SOURCE TASK");
    println!("{}", "=".repeat(80));
    println!("Dataset      : 120");
    println!("Source tasks : 3");
    println!("Active tasks : 2");
    println!("Trade rows   : {total_rows}");
    println!("Result       : PASS");
}
