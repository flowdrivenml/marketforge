#![cfg(feature = "historical")]

use {
    marketforge_engine::historical::merge::DepthEventRef,
    marketforge_engine::historical::merge::DepthEventSource,
    marketforge_engine::historical::merge::DepthStream,
    marketforge_engine::historical::merge::KWayDepthMerger,
    marketforge_engine::historical::merge::MergePartition,
    marketforge_engine::historical::merge::PartitionDepthStream,
    marketforge_engine::historical::merge::build_depth_inventory,
    marketforge_engine::historical::merge::plan_partition_inputs,
    marketforge_engine::historical::merge::reconcile_depth_boundaries,
    marketforge_engine::historical::merge::resolve_merge_inputs,
};

// -----------------------------------------------------------------------------
// Project root
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// Event identity
// -----------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct EventIdentity {
    timestamp_ns: i64,

    dataset_id: u64,
    task_id: u64,
    event_ordinal: u64,

    canonical_row_offset: u64,
    canonical_row_count: u64,

    sequence_start: Option<u64>,
    sequence_end: Option<u64>,

    event_boundary: String,
    source_operation: String,
}

impl From<&DepthEventRef> for EventIdentity {
    fn from(value: &DepthEventRef) -> Self {
        let event = &value.event;

        Self {
            timestamp_ns: event.event_timestamp_ns,

            dataset_id: value.dataset_id,
            task_id: value.task_id,
            event_ordinal: event.event_ordinal,

            canonical_row_offset: event.canonical_row_offset,
            canonical_row_count: event.canonical_row_count,

            sequence_start: event.sequence_start,
            sequence_end: event.sequence_end,

            event_boundary: format!("{:?}", event.event_boundary),
            source_operation: format!("{:?}", event.source_operation),
        }
    }
}

// -----------------------------------------------------------------------------
// Test context
// -----------------------------------------------------------------------------

struct TestContext {
    _workspace: tempfile::TempDir,
    inventory: Vec<marketforge_engine::historical::merge::DepthTaskInventory>,
    boundaries: Vec<marketforge_engine::historical::merge::DepthBoundaryLink>,
    dataset_order: Vec<u64>,
}

fn load_context() -> TestContext {
    let workspace = tempfile::tempdir().unwrap();
    let root = workspace.path().to_path_buf();

    let job = crate::publication::depth_job(&root);

    let inputs = resolve_merge_inputs(&job, &root).expect("resolve inputs");

    let inventory = build_depth_inventory(&inputs).expect("build inventory");

    let boundaries = reconcile_depth_boundaries(&inventory).expect("reconcile boundaries");

    TestContext {
        _workspace: workspace,
        inventory,
        boundaries,
        dataset_order: job.inputs,
    }
}

// -----------------------------------------------------------------------------
// Sequential merger
// -----------------------------------------------------------------------------

fn sequential_merger(context: &TestContext) -> KWayDepthMerger {
    let mut streams: Vec<Box<dyn DepthEventSource>> = Vec::new();

    for &dataset_id in &context.dataset_order {
        let tasks = context
            .inventory
            .iter()
            .filter(|task| task.dataset_id == dataset_id)
            .cloned()
            .collect();

        streams.push(Box::new(
            DepthStream::new(dataset_id, tasks, &context.boundaries)
                .expect("construct sequential stream"),
        ));
    }

    KWayDepthMerger::new(streams).expect("construct sequential merger")
}

// -----------------------------------------------------------------------------
// Partition merger
// -----------------------------------------------------------------------------

fn partition_merger(context: &TestContext, partition: MergePartition) -> KWayDepthMerger {
    let plans =
        plan_partition_inputs(&[partition], &context.inventory).expect("plan partition inputs");

    let plan = &plans[0];

    let mut streams: Vec<Box<dyn DepthEventSource>> = Vec::new();

    for &dataset_id in &context.dataset_order {
        let tasks = plan
            .inputs
            .iter()
            .filter(|input| input.dataset_id == dataset_id)
            .map(|input| {
                context
                    .inventory
                    .iter()
                    .find(|task| task.dataset_id == dataset_id && task.task_id == input.task_id)
                    .expect("missing inventory task")
                    .clone()
            })
            .collect();

        streams.push(Box::new(
            PartitionDepthStream::new(dataset_id, partition, tasks, &context.boundaries)
                .expect("construct partition stream"),
        ));
    }

    KWayDepthMerger::new(streams).expect("construct partition merger")
}

// -----------------------------------------------------------------------------
// Full partition coverage test
// -----------------------------------------------------------------------------

#[test]
fn partition_concatenation_matches_sequential_merge() {
    let context = load_context();

    let start_ns = crate::publication::START;

    // Two-hour interval.
    let end_ns = start_ns + 400;

    // Eight 15-minute partitions.
    let partition_duration = (end_ns - start_ns) / 8;

    let partitions = (0..8)
        .map(|index| MergePartition {
            partition_id: index,

            start_ns: start_ns + i64::from(index) * partition_duration,

            end_ns: start_ns + i64::from(index + 1) * partition_duration,
        })
        .collect::<Vec<_>>();

    // -------------------------------------------------------------------------
    // Sequential reference
    // -------------------------------------------------------------------------

    let mut sequential = sequential_merger(&context);

    let mut total_events = 0_u64;
    let mut previous_timestamp = None;

    println!("\nMARKETFORGE — PARTITION COVERAGE VALIDATION");
    println!("{}", "=".repeat(90));

    // -------------------------------------------------------------------------
    // Compare every partition
    // -------------------------------------------------------------------------

    for partition in &partitions {
        let mut partitioned = partition_merger(&context, *partition);

        let mut partition_events = 0_u64;

        while let Some(actual) = partitioned.next_event().expect("read partition event") {
            let expected = loop {
                let event = sequential
                    .next_event()
                    .expect("read sequential event")
                    .expect("sequential stream exhausted early");

                if event.event.event.event_timestamp_ns >= partition.start_ns {
                    break event;
                }
            };

            let actual_identity = EventIdentity::from(&actual.event);
            let expected_identity = EventIdentity::from(&expected.event);

            assert_eq!(
                actual_identity, expected_identity,
                "partition {} event {} differs",
                partition.partition_id, partition_events,
            );

            if let Some(previous) = previous_timestamp {
                assert!(
                    actual_identity.timestamp_ns >= previous,
                    "global timestamp regression"
                );
            }

            assert!(
                actual_identity.timestamp_ns < partition.end_ns,
                "event exceeds partition end"
            );

            previous_timestamp = Some(actual_identity.timestamp_ns);

            partition_events += 1;
            total_events += 1;
        }

        println!(
            "partition={:02} | events={} | MATCH",
            partition.partition_id, partition_events,
        );
    }

    // -------------------------------------------------------------------------
    // Verify no sequential events remain inside the tested interval
    // -------------------------------------------------------------------------

    if let Some(event) = sequential
        .next_event()
        .expect("read remaining sequential event")
        && event.event.event.event_timestamp_ns < end_ns
    {
        panic!(
            "sequential event missing from partitioned output: {:?}",
            EventIdentity::from(&event.event)
        );
    }

    assert_eq!(total_events, 8);

    println!("{}", "=".repeat(90));
    println!("Partitions       : {}", partitions.len());
    println!("Events compared  : {}", total_events);
    println!("Global ordering  : VERIFIED");
    println!("Event coverage   : COMPLETE");
    println!("Result           : PASS");
}
