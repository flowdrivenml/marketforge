#![cfg(feature = "process")]

use std::path::PathBuf;

use marketforge_engine::{
    job::load_merge_job,
    process::merge::{build_depth_inventory, resolve_merge_inputs, validate_depth_events},
};

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn validates_real_depth_event_indexes() {
    let root = project_root();

    let job = load_merge_job(root.join("data/.jobs/merge/merge-200.json")).expect("load merge job");

    let inputs = resolve_merge_inputs(&job, &root).expect("resolve merge inputs");

    let inventory = build_depth_inventory(&inputs).expect("build depth inventory");

    let mut total_events = 0_u64;
    let mut total_rows = 0_u64;
    let mut total_regressions = 0_u64;
    let mut total_zero_rows = 0_u64;

    println!("\nMARKETFORGE — DEPTH EVENT INDEX VALIDATION");

    for dataset_id in [105, 108, 112, 92] {
        let mut dataset_events = 0_u64;
        let mut dataset_rows = 0_u64;
        let mut dataset_regressions = 0_u64;

        for task in inventory
            .iter()
            .filter(|task| task.dataset_id == dataset_id)
        {
            let report = validate_depth_events(task).expect("validate depth event index");

            dataset_events += report.events;
            dataset_rows += report.canonical_rows;
            dataset_regressions += report.timestamp_regressions;

            total_zero_rows += report.zero_row_events;
        }

        println!(
            "dataset={} | events={} | rows={} | timestamp_regressions={}",
            dataset_id, dataset_events, dataset_rows, dataset_regressions,
        );

        total_events += dataset_events;
        total_rows += dataset_rows;
        total_regressions += dataset_regressions;
    }

    assert_eq!(total_rows, 223_406_177);

    println!("\nTOTAL");
    println!("Events                : {}", total_events);
    println!("Canonical rows        : {}", total_rows);
    println!("Zero-row events       : {}", total_zero_rows);
    println!("Timestamp regressions : {}", total_regressions);
    println!("Result                : PASS");
}
