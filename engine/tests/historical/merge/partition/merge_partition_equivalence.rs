#![cfg(feature = "historical")]

use std::path::PathBuf;

use {
    marketforge_engine::historical::job::load_merge_job,
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

fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("project root")
        .to_path_buf()
}

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
    inventory: Vec<marketforge_engine::historical::merge::DepthTaskInventory>,
    boundaries: Vec<marketforge_engine::historical::merge::DepthBoundaryLink>,
    dataset_order: Vec<u64>,
}

fn load_context() -> TestContext {
    let root = project_root();

    let job = load_merge_job(root.join("data/.jobs/merge/merge-200.json")).expect("load merge job");

    let inputs = resolve_merge_inputs(&job, &root).expect("resolve inputs");

    let inventory = build_depth_inventory(&inputs).expect("build inventory");

    let boundaries = reconcile_depth_boundaries(&inventory).expect("reconcile boundaries");

    TestContext {
        inventory,
        boundaries,
        dataset_order: job.inputs,
    }
}

// -----------------------------------------------------------------------------
// Sequential reference
// -----------------------------------------------------------------------------

fn sequential_events(context: &TestContext, partition: MergePartition) -> Vec<EventIdentity> {
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

    let mut merger = KWayDepthMerger::new(streams).expect("construct sequential merger");

    let mut events = Vec::new();

    while let Some(merged) = merger.next_event().expect("read event") {
        let timestamp = merged.event.event.event_timestamp_ns;

        if timestamp >= partition.end_ns {
            break;
        }

        if timestamp >= partition.start_ns {
            events.push(EventIdentity::from(&merged.event));
        }
    }

    events
}

// -----------------------------------------------------------------------------
// Partitioned reference
// -----------------------------------------------------------------------------

fn partitioned_events(context: &TestContext, partition: MergePartition) -> Vec<EventIdentity> {
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

    let mut merger = KWayDepthMerger::new(streams).expect("construct partition merger");

    let mut events = Vec::new();

    while let Some(merged) = merger.next_event().expect("read event") {
        events.push(EventIdentity::from(&merged.event));
    }

    events
}

// -----------------------------------------------------------------------------
// Equivalence
// -----------------------------------------------------------------------------

fn assert_equivalent(context: &TestContext, partition: MergePartition) {
    let sequential = sequential_events(context, partition);
    let partitioned = partitioned_events(context, partition);

    assert_eq!(
        sequential.len(),
        partitioned.len(),
        "event count mismatch for partition {}",
        partition.partition_id,
    );

    for (index, (expected, actual)) in sequential.iter().zip(partitioned.iter()).enumerate() {
        assert_eq!(
            expected, actual,
            "event mismatch at index {index}, partition {}",
            partition.partition_id,
        );
    }

    println!(
        "partition={} | start={} | end={} | events={} | MATCH",
        partition.partition_id,
        partition.start_ns,
        partition.end_ns,
        sequential.len(),
    );
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[test]
fn matches_sequential_merge_within_archive() {
    let context = load_context();

    let start = 1788220800000000000_i64;

    let partition = MergePartition {
        partition_id: 0,
        start_ns: start + 1_200_000_000_000,
        end_ns: start + 1_260_000_000_000,
    };

    assert_equivalent(&context, partition);
}

#[test]
fn matches_sequential_merge_across_archive_boundary() {
    let context = load_context();

    // Gate.io archive boundary at 01:00 UTC.
    let boundary = 1788224400000000000_i64;

    let partition = MergePartition {
        partition_id: 1,
        start_ns: boundary - 30_000_000_000,
        end_ns: boundary + 30_000_000_000,
    };

    assert_equivalent(&context, partition);
}

#[test]
fn matches_sequential_merge_across_timestamp_discrepancy() {
    let context = load_context();

    // Gate.io boundary between tasks 58 and 59.
    // The redundant initialization is timestamped
    // 600 milliseconds earlier than the preceding final update.
    let boundary = 1788433200000000000_i64;

    let partition = MergePartition {
        partition_id: 2,
        start_ns: boundary - 30_000_000_000,
        end_ns: boundary + 30_000_000_000,
    };

    assert_equivalent(&context, partition);
}
