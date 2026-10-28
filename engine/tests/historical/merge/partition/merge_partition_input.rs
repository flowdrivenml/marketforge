#![cfg(feature = "historical")]

use std::path::PathBuf;

use {
    marketforge_engine::historical::job::load_merge_job,
    marketforge_engine::historical::merge::build_depth_inventory,
    marketforge_engine::historical::merge::plan_merge_partitions,
    marketforge_engine::historical::merge::plan_partition_inputs,
    marketforge_engine::historical::merge::resolve_merge_inputs,
};

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn plans_real_partition_inputs() {
    let root = project_root();

    let job = load_merge_job(root.join("data/.jobs/merge/merge-200.json")).expect("load merge job");

    let inputs = resolve_merge_inputs(&job, &root).expect("resolve inputs");

    let inventory = build_depth_inventory(&inputs).expect("build inventory");

    let partitions = plan_merge_partitions(&job).expect("plan partitions");

    let plans =
        plan_partition_inputs(&partitions.partitions, &inventory).expect("plan partition inputs");

    assert_eq!(plans.len(), 64);

    println!("\nMARKETFORGE — PARTITION INPUT PLANNING");

    for plan in &plans {
        println!(
            "partition={:02} | start={} | end={} | tasks={}",
            plan.partition.partition_id,
            plan.partition.start_ns,
            plan.partition.end_ns,
            plan.inputs.len(),
        );

        for input in &plan.inputs {
            assert!(input.end_timestamp_ns >= plan.partition.start_ns);

            assert!(input.start_timestamp_ns < plan.partition.end_ns);
        }
    }

    println!("\nPARTITION INPUT PLANNING: PASS");
}
