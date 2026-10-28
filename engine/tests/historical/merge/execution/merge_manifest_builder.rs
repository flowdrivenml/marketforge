#![cfg(feature = "historical")]

use std::{fs, path::PathBuf};

use {
    marketforge_engine::historical::job::load_merge_job,
    marketforge_engine::historical::merge::MergeInputDataset,
    marketforge_engine::historical::merge::build_depth_inventory,
    marketforge_engine::historical::merge::build_merge_checkpoints,
    marketforge_engine::historical::merge::build_merge_streams,
    marketforge_engine::historical::merge::build_parallel_merge_manifest,
    marketforge_engine::historical::merge::execute_merge_partitions_parallel,
    marketforge_engine::historical::merge::plan_merge_partitions,
    marketforge_engine::historical::merge::plan_partition_inputs,
    marketforge_engine::historical::merge::reconcile_depth_boundaries,
    marketforge_engine::historical::merge::resolve_initial_checkpoints,
    marketforge_engine::historical::merge::resolve_merge_inputs,
    marketforge_engine::historical::merge::validate_merge_output,
};

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn builds_real_parallel_merge_manifest() {
    let root = project_root();

    let mut job =
        load_merge_job(root.join("data/.jobs/merge/merge-200.json")).expect("load merge job");

    // Two-hour integration interval.
    job.time_range.end_ns = job.time_range.start_ns + 7_200_000_000_000;

    job.resources.workers = 4;
    job.partitioning.partitions_per_worker = 2;

    // -------------------------------------------------------------------------
    // Resolve source datasets
    // -------------------------------------------------------------------------

    let inputs = resolve_merge_inputs(&job, &root).expect("resolve inputs");

    let input_metadata = inputs
        .iter()
        .map(|input| MergeInputDataset {
            dataset_id: input.dataset_id,
            dataset_path: input.dataset_path.to_string_lossy().into_owned(),
            manifest_version: input.manifest.protocol_version,
        })
        .collect::<Vec<_>>();

    // -------------------------------------------------------------------------
    // Build inventory and boundaries
    // -------------------------------------------------------------------------

    let inventory = build_depth_inventory(&inputs).expect("build inventory");

    let boundaries = reconcile_depth_boundaries(&inventory).expect("reconcile boundaries");

    let streams = build_merge_streams(&inventory, &job.inputs).expect("build streams");

    // -------------------------------------------------------------------------
    // Build checkpoints
    // -------------------------------------------------------------------------

    let checkpoints = build_merge_checkpoints(&inventory).expect("build checkpoints");

    let initial_checkpoints = resolve_initial_checkpoints(
        &checkpoints,
        &job.inputs,
        job.time_range.start_ns,
        job.time_range.end_ns,
    )
    .expect("resolve initial checkpoints");

    // -------------------------------------------------------------------------
    // Plan partitions
    // -------------------------------------------------------------------------

    let partition_plan = plan_merge_partitions(&job).expect("plan partitions");

    let plans = plan_partition_inputs(&partition_plan.partitions, &inventory)
        .expect("plan partition inputs");

    // -------------------------------------------------------------------------
    // Temporary staging
    // -------------------------------------------------------------------------

    let staging =
        std::env::temp_dir().join(format!("marketforge-manifest-test-{}", std::process::id(),));

    if staging.exists() {
        fs::remove_dir_all(&staging).unwrap();
    }

    fs::create_dir_all(&staging).unwrap();

    // -------------------------------------------------------------------------
    // Execute partitions
    // -------------------------------------------------------------------------

    let results = execute_merge_partitions_parallel(&job, plans, &inventory, &boundaries, &staging)
        .expect("execute partitions");

    // -------------------------------------------------------------------------
    // Validate physical output
    // -------------------------------------------------------------------------

    let validated = validate_merge_output(&staging, &results).expect("validate output");

    // -------------------------------------------------------------------------
    // Build manifest
    // -------------------------------------------------------------------------

    let manifest = build_parallel_merge_manifest(
        &job,
        input_metadata,
        streams,
        &boundaries,
        &results,
        validated,
        checkpoints,
        initial_checkpoints,
        0,
    )
    .expect("build manifest");

    // -------------------------------------------------------------------------
    // Assertions
    // -------------------------------------------------------------------------

    assert_eq!(manifest.job_id, job.job_id);
    assert_eq!(manifest.events_written, 1_415_631);
    assert_eq!(manifest.files_written, 8);

    assert_eq!(manifest.input_datasets.len(), 4);
    assert_eq!(manifest.streams.len(), 4);

    assert_eq!(manifest.checkpoints.len(), 81);
    assert_eq!(manifest.initial_checkpoints.len(), 4);

    assert_eq!(manifest.files.len(), 8);

    for file in &manifest.files {
        assert!(file.partition_id.is_some());
        assert!(file.rows.is_some());
        assert!(file.start_timestamp_ns.is_some());
        assert!(file.end_timestamp_ns.is_some());
    }

    manifest.validate().expect("validate manifest");

    println!("\nMARKETFORGE — PARALLEL MERGE MANIFEST");
    println!("{}", "=".repeat(80));

    println!("Input datasets     : {}", manifest.input_datasets.len());
    println!("Streams            : {}", manifest.streams.len());
    println!("Partitions         : {}", results.len());
    println!("Merged events      : {}", manifest.events_written);
    println!("Parquet files      : {}", manifest.files_written);
    println!("Boundary links     : {}", manifest.boundary_links.len());
    println!("Checkpoints        : {}", manifest.checkpoints.len());
    println!(
        "Initial checkpoints: {}",
        manifest.initial_checkpoints.len()
    );
    println!(
        "Skipped snapshots  : {}",
        manifest.skipped_initializations.len()
    );
    println!("Result             : PASS");

    fs::remove_dir_all(&staging).unwrap();
}
