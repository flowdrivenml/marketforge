#![cfg(feature = "process")]

use std::{
    fs::File,
    path::{Path, PathBuf},
};

use marketforge_engine::process::{
    manifest::DatasetManifest,
    merge::{
        KWayEventMerger, MergeEventKind, MergeEventSource, ResolvedMergeInput, TradeStream,
        build_trade_inventory,
    },
};

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

#[test]
fn merges_real_trade_datasets() {
    let root = project_root();

    let dataset_ids = [
        111_u64, // Gate.io
        109,     // Bitget
        115,     // Binance
        100,     // OKX
        97,      // Bybit
        99,      // Binance
    ];

    let mut streams: Vec<Box<dyn MergeEventSource>> = Vec::new();
    let mut expected_counts = Vec::new();

    for dataset_id in dataset_ids {
        let input = resolve_trade_dataset(&root, dataset_id);

        expected_counts.push(input.manifest.events_written);

        let inventory = build_trade_inventory(&[input]).expect("build trade inventory");

        let stream = TradeStream::new(dataset_id, inventory).expect("create trade stream");

        streams.push(Box::new(stream));
    }

    let mut merger = KWayEventMerger::new(streams).expect("construct K-way merger");

    let mut actual_counts = vec![0_u64; dataset_ids.len()];
    let mut previous_key = None;

    let mut total = 0_u64;

    println!("\nMARKETFORGE — REAL MULTI-EXCHANGE TRADE MERGE");
    println!("{}", "=".repeat(90));

    while let Some(merged) = merger.next_event().expect("merge trade event") {
        assert_eq!(merged.event.kind(), MergeEventKind::Trade);

        let rank = merged.stream_rank as usize;

        assert!(rank < dataset_ids.len());

        assert_eq!(
            merged.event.dataset_id(),
            dataset_ids[rank],
            "incorrect dataset identity"
        );

        let key = merged.event.merge_key(merged.stream_rank);

        if let Some(previous) = previous_key {
            assert!(key >= previous, "global merge ordering regression");
        }

        previous_key = Some(key);

        actual_counts[rank] += 1;
        total += 1;
    }

    for (index, dataset_id) in dataset_ids.iter().enumerate() {
        assert_eq!(
            actual_counts[index], expected_counts[index],
            "dataset {dataset_id}: missing or duplicated events"
        );

        println!(
            "dataset={} | events={} | MATCH",
            dataset_id, actual_counts[index],
        );
    }

    assert_eq!(merger.events_emitted(), total);

    assert_eq!(total, 6_016_219);

    println!("{}", "=".repeat(90));
    println!("Streams       : {}", dataset_ids.len());
    println!("Merged events : {total}");
    println!("Global order  : VERIFIED");
    println!("Event counts  : VERIFIED");
    println!("Result        : PASS");
}
