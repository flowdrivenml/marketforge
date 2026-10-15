#![cfg(feature = "historical")]

use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use marketforge_engine::{
    job::load_merge_job,
    process::merge::{
        MergePartition, build_depth_inventory, execute_merge_partition, plan_partition_inputs,
        reconcile_depth_boundaries, resolve_merge_inputs,
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
fn executes_real_merge_partition() {
    let root = project_root();

    let job = load_merge_job(root.join("data/.jobs/merge/merge-200.json")).expect("load merge job");

    let inputs = resolve_merge_inputs(&job, &root).expect("resolve merge inputs");

    let inventory = build_depth_inventory(&inputs).expect("build depth inventory");

    let boundaries = reconcile_depth_boundaries(&inventory).expect("reconcile boundaries");

    // One-minute test interval.
    let partition = MergePartition {
        partition_id: 0,
        start_ns: job.time_range.start_ns,
        end_ns: job.time_range.start_ns + 60_000_000_000,
    };

    let plans = plan_partition_inputs(&[partition], &inventory).expect("plan partition inputs");

    // Temporary output directory.
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

    let staging = std::env::temp_dir().join(format!(
        "marketforge-partition-test-{}-{id}",
        std::process::id(),
    ));

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
    assert_eq!(result.events_written, 11_550);
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
