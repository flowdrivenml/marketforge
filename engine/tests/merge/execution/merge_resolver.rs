#![cfg(feature = "process")]

use std::path::PathBuf;

use marketforge_engine::{job::load_merge_job, process::merge::resolve_merge_inputs};

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn resolves_real_merge_inputs() {
    let root = project_root();

    let job = load_merge_job(root.join("data/.jobs/merge/merge-200.json")).expect("load merge job");

    let inputs = resolve_merge_inputs(&job, &root).expect("resolve merge inputs");

    assert_eq!(inputs.len(), 4);

    let expected = [105, 108, 112, 92];

    for (input, expected_id) in inputs.iter().zip(expected) {
        assert_eq!(input.dataset_id, expected_id);

        assert!(!input.canonical_files.is_empty());
        assert!(!input.auxiliary_files.is_empty());

        println!(
            "RESOLVED | dataset={} | canonical={} | auxiliary={} | rows={}",
            input.dataset_id,
            input.canonical_files.len(),
            input.auxiliary_files.len(),
            input.manifest.events_written,
        );
    }

    println!("\nMERGE INPUT RESOLUTION: PASS");
}
