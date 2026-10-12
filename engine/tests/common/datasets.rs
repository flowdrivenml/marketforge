use std::{
    fs::File,
    path::{Path, PathBuf},
};

use marketforge_engine::process::{manifest::DatasetManifest, merge::ResolvedMergeInput};

// -----------------------------------------------------------------------------
// Project root
// -----------------------------------------------------------------------------

pub fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("project root")
        .to_path_buf()
}

// -----------------------------------------------------------------------------
// Build resolved dataset
// -----------------------------------------------------------------------------

fn build_resolved_input(dataset_path: PathBuf, dataset_id: u64) -> ResolvedMergeInput {
    let manifest_path = dataset_path.join("manifest.json");

    let manifest: DatasetManifest =
        serde_json::from_reader(File::open(&manifest_path).unwrap_or_else(|error| {
            panic!(
                "failed to open manifest {}: {error}",
                manifest_path.display()
            )
        }))
        .expect("parse dataset manifest");

    assert_eq!(manifest.dataset_id, dataset_id, "dataset identity mismatch");

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
// Resolve depth dataset
// -----------------------------------------------------------------------------

pub fn resolve_depth_dataset(root: &Path, dataset_id: u64) -> ResolvedMergeInput {
    let dataset_path = root.join(format!("data/processed/{dataset_id}"));

    build_resolved_input(dataset_path, dataset_id)
}

// -----------------------------------------------------------------------------
// Resolve trade dataset
// -----------------------------------------------------------------------------

pub fn resolve_trade_dataset(root: &Path, dataset_id: u64) -> ResolvedMergeInput {
    let datasets = root.join("data/.work/full-trade-test/datasets");

    let suffix = format!("-d{dataset_id}");

    for entry in std::fs::read_dir(&datasets).expect("read trade datasets") {
        let entry = entry.expect("read dataset entry");
        let dataset_path = entry.path();

        if !dataset_path.join("manifest.json").is_file() {
            continue;
        }

        let matches_id = dataset_path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(&suffix));

        if matches_id {
            return build_resolved_input(dataset_path, dataset_id);
        }
    }

    panic!(
        "trade dataset {dataset_id} not found under {}",
        datasets.display()
    );
}
