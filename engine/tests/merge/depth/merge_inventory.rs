#![cfg(feature = "process")]

use std::path::PathBuf;

use marketforge_engine::{
    job::load_merge_job,
    process::merge::{build_depth_inventory, resolve_merge_inputs},
};

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn builds_real_depth_inventory() {
    let root = project_root();

    let job = load_merge_job(root.join("data/.jobs/merge/merge-200.json")).expect("load merge job");

    let inputs = resolve_merge_inputs(&job, &root).expect("resolve merge inputs");

    let inventory = build_depth_inventory(&inputs).expect("build depth inventory");

    assert_eq!(inventory.len(), 81);

    let mut total_levels = 0_u64;

    for task in &inventory {
        assert!(!task.canonical_files.is_empty());
        assert!(task.events_path.is_file());
        assert!(task.segments_path.is_file());
        assert!(task.boundary_path.is_file());

        assert_eq!(task.segments.total_levels, task.canonical_rows,);

        total_levels += task.canonical_rows;
    }

    assert_eq!(total_levels, 223_406_177);

    println!("\nMARKETFORGE — DEPTH MERGE INVENTORY");
    println!("Tasks            : {}", inventory.len());
    println!("Canonical levels : {}", total_levels);
    println!("Inventory        : VERIFIED");
}

#[test]
fn reconciles_real_depth_boundaries() {
    let root = project_root();

    let job = load_merge_job(root.join("data/.jobs/merge/merge-200.json")).expect("load merge job");

    let inputs = resolve_merge_inputs(&job, &root).expect("resolve inputs");

    let inventory = build_depth_inventory(&inputs).expect("build inventory");

    let links = marketforge_engine::process::merge::reconcile_depth_boundaries(&inventory)
        .expect("reconcile boundaries");

    assert_eq!(links.len(), 77);

    println!("\nMARKETFORGE — DEPTH BOUNDARY RECONCILIATION");

    for dataset_id in [105, 108, 112, 92] {
        let dataset_links: Vec<_> = links
            .iter()
            .filter(|link| link.dataset_id == dataset_id)
            .collect();

        println!("\nDataset {dataset_id}");

        for link in &dataset_links {
            println!(
                "  task {:06} → {:06} | {:?}",
                link.previous_task_id, link.next_task_id, link.relation,
            );
        }
    }
    use marketforge_engine::process::merge::MergeBoundaryRelation;

    let gateio: Vec<_> = links.iter().filter(|link| link.dataset_id == 112).collect();

    assert_eq!(gateio.len(), 71);

    assert!(
        gateio
            .iter()
            .all(|link| { link.relation == MergeBoundaryRelation::VerifiedContinuation }),
        "Gate.io boundaries must have matching sequences and book states"
    );

    let timestamp_differences = gateio
        .iter()
        .filter(|link| link.timestamp_delta_ns != Some(0))
        .count();

    assert_eq!(timestamp_differences, 18);

    println!("\nGATE.IO BOUNDARY VALIDATION");
    println!("Verified boundaries : {}", gateio.len());
    println!("Timestamp differences: {}", timestamp_differences);

    println!("\nBOUNDARY RECONCILIATION: PASS");
}
