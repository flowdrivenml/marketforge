#![cfg(feature = "historical")]

use std::fs;

use {
    marketforge_engine::historical::merge::MergePartition,
    marketforge_engine::historical::merge::build_depth_inventory,
    marketforge_engine::historical::merge::execute_merge_partition,
    marketforge_engine::historical::merge::plan_partition_inputs,
    marketforge_engine::historical::merge::reconcile_depth_boundaries,
    marketforge_engine::historical::merge::resolve_merge_inputs,
};

#[test]
fn executes_generated_merge_partition() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path().to_path_buf();

    let job = crate::publication::depth_job(&root);

    let inputs = resolve_merge_inputs(&job, &root).expect("resolve merge inputs");

    let inventory = build_depth_inventory(&inputs).expect("build depth inventory");

    let boundaries = reconcile_depth_boundaries(&inventory).expect("reconcile boundaries");

    // Include both events of the first task in each source.
    let partition = MergePartition {
        partition_id: 0,
        start_ns: job.time_range.start_ns,
        end_ns: job.time_range.start_ns + 200,
    };

    let plans = plan_partition_inputs(&[partition], &inventory).expect("plan partition inputs");

    // Temporary output directory.
    let staging = root.join("staging");

    fs::create_dir_all(&staging).unwrap();

    let result = execute_merge_partition(
        &plans[0],
        &inventory,
        &boundaries,
        &job.inputs,
        &job.parquet,
        &staging,
    )
    .expect("execute merge partition");

    assert_eq!(result.partition_id, 0);
    assert_eq!(result.events_written, 4);
    assert_eq!(result.files_written, 1);

    assert_eq!(result.files.len(), 1);

    let output = staging.join(&result.files[0].path);

    assert!(output.is_file());

    println!("\nMARKETFORGE — PARTITION EXECUTION");
    println!("Partition : {}", result.partition_id);
    println!("Events    : {}", result.events_written);
    println!("Files     : {}", result.files_written);
    println!("Output    : {}", output.display());
    println!("Result    : PASS");

    fs::remove_dir_all(&staging).unwrap();
}
