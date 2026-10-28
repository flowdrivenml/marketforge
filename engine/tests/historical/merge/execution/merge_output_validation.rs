#![cfg(feature = "historical")]

use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use {
    marketforge_engine::historical::job::load_merge_job,
    marketforge_engine::historical::merge::build_depth_inventory,
    marketforge_engine::historical::merge::execute_merge_partitions_parallel,
    marketforge_engine::historical::merge::plan_merge_partitions,
    marketforge_engine::historical::merge::plan_partition_inputs,
    marketforge_engine::historical::merge::reconcile_depth_boundaries,
    marketforge_engine::historical::merge::resolve_merge_inputs,
    marketforge_engine::historical::merge::validate_merge_output,
};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn validates_real_parallel_merge_output() {
    let root = project_root();

    let mut job =
        load_merge_job(root.join("data/.jobs/merge/merge-200.json")).expect("load merge job");

    // Two-hour interval, eight partitions.
    job.time_range.end_ns = job.time_range.start_ns + 7_200_000_000_000;

    job.resources.workers = 4;
    job.partitioning.partitions_per_worker = 2;

    let inputs = resolve_merge_inputs(&job, &root).expect("resolve inputs");

    let inventory = build_depth_inventory(&inputs).expect("build inventory");

    let boundaries = reconcile_depth_boundaries(&inventory).expect("reconcile boundaries");

    let partition_plan = plan_merge_partitions(&job).expect("plan partitions");

    let plans = plan_partition_inputs(&partition_plan.partitions, &inventory)
        .expect("plan partition inputs");

    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

    let staging = std::env::temp_dir().join(format!(
        "marketforge-output-validation-{}-{id}",
        std::process::id(),
    ));

    fs::create_dir_all(&staging).expect("create staging");

    let results = execute_merge_partitions_parallel(&job, plans, &inventory, &boundaries, &staging)
        .expect("execute parallel partitions");

    let validated = validate_merge_output(&staging, &results).expect("validate physical output");

    assert_eq!(validated.events_written, 1_415_631);
    assert_eq!(validated.files_written, 8);
    assert_eq!(validated.files.len(), 8);

    for file in &validated.files {
        assert!(file.partition_id.is_some());
        assert!(file.rows.unwrap_or(0) > 0);
        assert!(file.start_timestamp_ns.is_some());
        assert!(file.end_timestamp_ns.is_some());
    }

    println!("\nMARKETFORGE — PHYSICAL MERGE VALIDATION");
    println!("{}", "=".repeat(80));

    println!("Partitions      : {}", results.len());
    println!("Validated events: {}", validated.events_written);
    println!("Validated files : {}", validated.files_written);
    println!(
        "Timestamp range : {:?} → {:?}",
        validated.start_timestamp_ns, validated.end_timestamp_ns,
    );
    println!("Result          : PASS");

    fs::remove_dir_all(&staging).expect("remove staging");
}
