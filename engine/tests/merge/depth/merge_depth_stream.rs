#![cfg(feature = "process")]

use std::path::PathBuf;

use marketforge_engine::{
    job::load_merge_job,
    process::merge::{
        DepthStream, build_depth_inventory, reconcile_depth_boundaries, resolve_merge_inputs,
    },
};

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn streams_real_depth_archives() {
    let root = project_root();

    let job = load_merge_job(root.join("data/.jobs/merge/merge-200.json")).expect("load merge job");

    let inputs = resolve_merge_inputs(&job, &root).expect("resolve merge inputs");

    let inventory = build_depth_inventory(&inputs).expect("build depth inventory");

    let boundaries = reconcile_depth_boundaries(&inventory).expect("reconcile boundaries");

    println!("\nMARKETFORGE — LOGICAL DEPTH STREAMS");

    for dataset_id in [105, 108, 112, 92] {
        let tasks = inventory
            .iter()
            .filter(|task| task.dataset_id == dataset_id)
            .cloned()
            .collect();

        let mut stream =
            DepthStream::new(dataset_id, tasks, &boundaries).expect("create depth stream");

        let mut events = 0_u64;
        let mut initializations = 0_u64;

        while let Some(event) = stream.next_event().expect("read event") {
            events += 1;

            if event.event.event_boundary
                == marketforge_engine::formats::depth::DepthEventBoundary::Initialization
            {
                initializations += 1;
            }
        }

        println!(
            "dataset={} | events={} | initializations={} | skipped={}",
            dataset_id,
            events,
            initializations,
            stream.initializations_skipped(),
        );
    }

    println!("\nLOGICAL DEPTH STREAMS: PASS");
}
