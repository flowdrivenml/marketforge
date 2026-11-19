#![cfg(feature = "historical")]
use marketforge_engine::historical::merge::DepthEventSource;

use {
    marketforge_engine::historical::merge::DepthStream,
    marketforge_engine::historical::merge::KWayDepthMerger,
    marketforge_engine::historical::merge::build_depth_inventory,
    marketforge_engine::historical::merge::reconcile_depth_boundaries,
    marketforge_engine::historical::merge::resolve_merge_inputs,
};

#[test]
fn merges_generated_depth_events_chronologically() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path().to_path_buf();

    let job = crate::publication::depth_job(&root);

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
    while let Some(event) = merger.next_event().expect("read merged event") {
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

    assert_eq!(events, 8);

    println!("\nMARKETFORGE — K-WAY DEPTH MERGE");
    println!("Streams       : {}", job.inputs.len());
    println!("Merged events : {}", events);
    println!("Ordering      : VERIFIED");
    println!("Result        : PASS");
}
