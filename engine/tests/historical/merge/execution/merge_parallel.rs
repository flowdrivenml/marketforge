#![cfg(feature = "historical")]

use std::fs;

use {
    marketforge_engine::historical::merge::build_depth_inventory,
    marketforge_engine::historical::merge::execute_merge_partitions_parallel,
    marketforge_engine::historical::merge::plan_merge_partitions,
    marketforge_engine::historical::merge::plan_partition_inputs,
    marketforge_engine::historical::merge::reconcile_depth_boundaries,
    marketforge_engine::historical::merge::resolve_merge_inputs,
};

#[test]
fn executes_parallel_merge_partitions() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path().to_path_buf();

    let mut job = crate::publication::depth_job(&root);

    // Keep the requested interval inside the miniature publication.
    job.time_range.end_ns = job.time_range.start_ns + 400;

    // Four workers, two partitions each.
    job.resources.workers = 4;
    job.partitioning.partitions_per_worker = 2;

    let inputs = resolve_merge_inputs(&job, &root).expect("resolve inputs");

    let inventory = build_depth_inventory(&inputs).expect("build inventory");

    let boundaries = reconcile_depth_boundaries(&inventory).expect("reconcile boundaries");

    let partition_plan = plan_merge_partitions(&job).expect("plan partitions");

    let plans = plan_partition_inputs(&partition_plan.partitions, &inventory)
        .expect("plan partition inputs");

    let staging = root.join("staging");

    fs::create_dir_all(&staging).expect("create staging");

    let results = execute_merge_partitions_parallel(&job, plans, &inventory, &boundaries, &staging)
        .expect("execute parallel partitions");

    assert_eq!(results.len(), 8);

    let total_events: u64 = results.iter().map(|result| result.events_written).sum();

    let total_files: u64 = results.iter().map(|result| result.files_written).sum();

    // Two sources, each with four events.
    assert_eq!(total_events, 8);

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
