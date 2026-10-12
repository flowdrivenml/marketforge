#![cfg(feature = "process")]
use marketforge_engine::process::merge::DepthEventSource;
use std::path::PathBuf;

use marketforge_engine::{
    job::load_merge_job,
    process::merge::{
        DepthStream, KWayDepthMerger, build_depth_inventory, reconcile_depth_boundaries,
        resolve_merge_inputs,
    },
};

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

#[test]
fn merges_real_depth_events_chronologically() {
    let root = project_root();

    let job = load_merge_job(root.join("data/.jobs/merge/merge-200.json")).expect("load merge job");

    let inputs = resolve_merge_inputs(&job, &root).expect("resolve merge inputs");

    let inventory = build_depth_inventory(&inputs).expect("build inventory");

    let boundaries = reconcile_depth_boundaries(&inventory).expect("reconcile boundaries");

    let mut streams: Vec<Box<dyn DepthEventSource>> = Vec::new();

    // Preserve the dataset order from the merge job.
    for &dataset_id in &job.inputs {
        let tasks = inventory
            .iter()
            .filter(|task| task.dataset_id == dataset_id)
            .cloned()
            .collect();

        streams.push(Box::new(
            DepthStream::new(dataset_id, tasks, &boundaries).expect("create depth stream"),
        ));
    }

    let mut merger = KWayDepthMerger::new(streams).expect("create K-way merger");

    let mut previous_timestamp = None;
    let mut events = 0_u64;

    // Focused test: first 100,000 events.
    while events < 100_000 {
        let Some(event) = merger.next_event().expect("read merged event") else {
            break;
        };

        let timestamp = event.event.event.event_timestamp_ns;

        if let Some(previous) = previous_timestamp {
            assert!(
                timestamp >= previous,
                "merged timestamps must be nondecreasing"
            );
        }

        previous_timestamp = Some(timestamp);
        events += 1;
    }

    assert_eq!(events, 100_000);

    println!("\nMARKETFORGE — K-WAY DEPTH MERGE");
    println!("Streams       : {}", job.inputs.len());
    println!("Merged events : {}", events);
    println!("Ordering      : VERIFIED");
    println!("Result        : PASS");
}
