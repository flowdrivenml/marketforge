#![cfg(feature = "historical")]

use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use marketforge_engine::{
    job::load_merge_job,
    process::merge::{
        build_depth_inventory, execute_merge_partitions_parallel, plan_merge_partitions,
        plan_partition_inputs, reconcile_depth_boundaries, resolve_merge_inputs,
    },
};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn executes_parallel_merge_partitions() {
    let root = project_root();

    let mut job =
        load_merge_job(root.join("data/.jobs/merge/merge-200.json")).expect("load merge job");

    // Two-hour integration interval.
    job.time_range.end_ns = job.time_range.start_ns + 7_200_000_000_000;

    // Four workers, two partitions each.
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
        "marketforge-parallel-test-{}-{id}",
        std::process::id(),
    ));

    fs::create_dir_all(&staging).expect("create staging");

    let results = execute_merge_partitions_parallel(&job, plans, &inventory, &boundaries, &staging)
        .expect("execute parallel partitions");

    assert_eq!(results.len(), 8);

    let total_events: u64 = results.iter().map(|result| result.events_written).sum();

    let total_files: u64 = results.iter().map(|result| result.files_written).sum();

    // Previously validated two-hour sequential reference.
    assert_eq!(total_events, 1_415_631);

    assert!(total_files > 0);

    println!("\nMARKETFORGE — PARALLEL MERGE");
    println!("{}", "=".repeat(80));

    for result in &results {
        println!(
            "partition={:02} | events={} | files={}",
            result.partition_id, result.events_written, result.files_written,
        );
    }

    println!("{}", "=".repeat(80));
    println!("Workers       : {}", job.resources.workers);
    println!("Partitions    : {}", results.len());
    println!("Merged events : {}", total_events);
    println!("Parquet files : {}", total_files);
    println!("Result        : PASS");

    fs::remove_dir_all(&staging).expect("remove temporary staging");
}
