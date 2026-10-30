#![cfg(feature = "historical")]

use std::{
    fs::File,
    path::{Path, PathBuf},
};

use {
    marketforge_engine::historical::merge::ResolvedMergeInput,
    marketforge_engine::historical::merge::build_trade_inventory,
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
fn accepts_dataset_with_empty_source_task() {
    let root = project_root();

    let input = resolve_trade_dataset(&root, 120);

    assert_eq!(input.manifest.source_tasks, vec![0, 1, 2]);

    let inventory = build_trade_inventory(&[input]).expect("build trade inventory");

    let task_ids = inventory
        .iter()
        .map(|task| task.task_id)
        .collect::<Vec<_>>();

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
