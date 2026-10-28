#![cfg(feature = "historical")]

use std::{
    fs::File,
    path::{Path, PathBuf},
};

use {
    marketforge_engine::historical::merge::DepthStream,
    marketforge_engine::historical::merge::KWayEventMerger,
    marketforge_engine::historical::merge::MergeEventKind,
    marketforge_engine::historical::merge::MergeEventSource,
    marketforge_engine::historical::merge::ResolvedMergeInput,
    marketforge_engine::historical::merge::TradeStream,
    marketforge_engine::historical::merge::build_depth_inventory,
    marketforge_engine::historical::merge::build_trade_inventory,
    marketforge_engine::historical::merge::reconcile_depth_boundaries,
    marketforge_engine::historical::normalize::manifest::DatasetManifest,
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
// Resolve dataset
// -----------------------------------------------------------------------------

fn resolve_dataset(root: &Path, dataset_id: u64, trades: bool) -> ResolvedMergeInput {
    let dataset_path = if trades {
        let datasets = root.join("data/.work/full-trade-test/datasets");

        std::fs::read_dir(&datasets)
            .expect("read trade datasets")
            .map(|entry| entry.expect("dataset entry").path())
            .find(|path| {
                let manifest_path = path.join("manifest.json");

                if !manifest_path.is_file() {
                    return false;
                }

                let manifest: DatasetManifest =
                    serde_json::from_reader(File::open(manifest_path).unwrap())
                        .expect("parse manifest");

                manifest.dataset_id == dataset_id
            })
            .expect("trade dataset not found")
    } else {
        root.join(format!("data/processed/{dataset_id}"))
    };

    let manifest: DatasetManifest = serde_json::from_reader(
        File::open(dataset_path.join("manifest.json")).expect("open manifest"),
    )
    .expect("parse manifest");

    assert_eq!(manifest.dataset_id, dataset_id);

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

    ResolvedMergeInput {
        dataset_id,
        dataset_path,
        manifest,
        canonical_files,
        auxiliary_files,
    }
}

// -----------------------------------------------------------------------------
// Combined merge integration test
// -----------------------------------------------------------------------------

#[test]
fn merges_real_trades_and_depth() {
    let root = project_root();

    let trade_datasets = [111_u64, 109, 115, 100, 97, 99];

    let depth_datasets = [105_u64, 108, 112, 92];

    let start_ns = 1788220800000000000_i64;
    let end_ns = start_ns + 60_000_000_000;

    let mut streams: Vec<Box<dyn MergeEventSource>> = Vec::new();

    // -------------------------------------------------------------------------
    // Trade streams
    // -------------------------------------------------------------------------

    for dataset_id in trade_datasets {
        let input = resolve_dataset(&root, dataset_id, true);

        let inventory = build_trade_inventory(&[input]).expect("build trade inventory");

        streams.push(Box::new(
            TradeStream::new_range(dataset_id, inventory, start_ns, end_ns)
                .expect("construct trade stream"),
        ));
    }

    // -------------------------------------------------------------------------
    // Depth streams
    // -------------------------------------------------------------------------

    let depth_inputs = depth_datasets
        .iter()
        .map(|&dataset_id| resolve_dataset(&root, dataset_id, false))
        .collect::<Vec<_>>();

    let depth_inventory = build_depth_inventory(&depth_inputs).expect("build depth inventory");

    let boundaries =
        reconcile_depth_boundaries(&depth_inventory).expect("reconcile depth boundaries");

    for dataset_id in depth_datasets {
        let tasks = depth_inventory
            .iter()
            .filter(|task| task.dataset_id == dataset_id)
            .cloned()
            .collect();

        streams.push(Box::new(
            DepthStream::new(dataset_id, tasks, &boundaries).expect("construct depth stream"),
        ));
    }

    // -------------------------------------------------------------------------
    // Merge
    // -------------------------------------------------------------------------

    let mut merger = KWayEventMerger::new(streams).expect("construct combined merger");

    let mut trade_events = 0_u64;
    let mut depth_events = 0_u64;

    let mut previous_key = None;

    println!("\nMARKETFORGE — COMBINED TRADE + DEPTH MERGE");
    println!("{}", "=".repeat(90));

    while let Some(merged) = merger.next_event().expect("merge event") {
        let timestamp = merged.event.timestamp_ns();

        if timestamp >= end_ns {
            break;
        }

        if timestamp < start_ns {
            continue;
        }

        let key = merged.event.merge_key(merged.stream_rank);

        if let Some(previous) = previous_key {
            assert!(key >= previous, "combined merge ordering regression");
        }

        previous_key = Some(key);

        match merged.event.kind() {
            MergeEventKind::Trade => {
                trade_events += 1;
            }

            MergeEventKind::Depth => {
                depth_events += 1;
            }
        }
    }

    println!("Trade events : {trade_events}");
    println!("Depth events : {depth_events}");
    println!("Total events : {}", trade_events + depth_events);
    println!("Global order : VERIFIED");
    println!("Result       : PASS");

    assert!(trade_events > 0);
    assert!(depth_events > 0);

    // The merger may consume one additional event when detecting
    // the end of the requested timestamp interval.
    let consumed = merger.events_emitted();
    let accepted = trade_events + depth_events;

    assert!(
        consumed == accepted || consumed == accepted + 1,
        "unexpected merged event count: consumed={consumed}, accepted={accepted}"
    );
}
