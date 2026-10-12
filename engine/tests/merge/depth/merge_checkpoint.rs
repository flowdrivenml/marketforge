#![cfg(feature = "process")]

use std::path::PathBuf;

use marketforge_engine::{
    job::load_merge_job,
    process::merge::{
        build_depth_inventory, build_merge_checkpoints, resolve_initial_checkpoints,
        resolve_merge_inputs,
    },
};
fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn builds_real_reconstruction_checkpoints() {
    let root = project_root();

    let job = load_merge_job(root.join("data/.jobs/merge/merge-200.json")).expect("load merge job");

    let inputs = resolve_merge_inputs(&job, &root).expect("resolve merge inputs");

    let inventory = build_depth_inventory(&inputs).expect("build depth inventory");

    let checkpoints = build_merge_checkpoints(&inventory).expect("build checkpoints");

    assert_eq!(checkpoints.len(), 81);

    println!("\nMARKETFORGE — RECONSTRUCTION CHECKPOINTS");
    println!("Checkpoints: {}", checkpoints.len());

    for dataset_id in [105, 108, 112, 92] {
        let count = checkpoints
            .iter()
            .filter(|checkpoint| checkpoint.dataset_id == dataset_id)
            .count();

        println!("dataset={} | checkpoints={}", dataset_id, count);
    }
}

#[test]
fn resolves_initial_checkpoints() {
    let root = project_root();

    let job = load_merge_job(root.join("data/.jobs/merge/merge-200.json")).expect("load merge job");

    let inputs = resolve_merge_inputs(&job, &root).expect("resolve merge inputs");

    let inventory = build_depth_inventory(&inputs).expect("build inventory");

    let checkpoints = build_merge_checkpoints(&inventory).expect("build checkpoints");

    let selected = resolve_initial_checkpoints(
        &checkpoints,
        &job.inputs,
        job.time_range.start_ns,
        job.time_range.end_ns,
    )
    .expect("resolve initial checkpoints");

    assert_eq!(selected.len(), 4);

    for entry in selected {
        println!(
            "dataset={} | task={} | timestamp={} | availability={:?}",
            entry.checkpoint.dataset_id,
            entry.checkpoint.task_id,
            entry.checkpoint.timestamp_ns,
            entry.availability,
        );
    }
}
