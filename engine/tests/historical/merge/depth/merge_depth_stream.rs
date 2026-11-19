#![cfg(feature = "historical")]

use {
    marketforge_engine::historical::merge::DepthStream,
    marketforge_engine::historical::merge::build_depth_inventory,
    marketforge_engine::historical::merge::reconcile_depth_boundaries,
    marketforge_engine::historical::merge::resolve_merge_inputs,
};

#[test]
fn streams_generated_depth_archives() {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path().to_path_buf();

    let job = crate::publication::depth_job(&root);

    let inputs = resolve_merge_inputs(&job, &root).expect("resolve merge inputs");

    let inventory = build_depth_inventory(&inputs).expect("build depth inventory");

    let boundaries = reconcile_depth_boundaries(&inventory).expect("reconcile boundaries");

    println!("\nMARKETFORGE — LOGICAL DEPTH STREAMS");

    for dataset_id in job.inputs {
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
                == marketforge_engine::shared::formats::depth::DepthEventBoundary::Initialization
            {
                initializations += 1;
            }
        }

        assert_eq!(
            (events, initializations, stream.initializations_skipped()),
            (4, 2, 0)
        );
        assert_eq!((events, initializations), (4, 2));
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
