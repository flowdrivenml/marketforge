use arrow_array::Array;
use marketforge_engine::{
    instrument::InstrumentCatalog,
    job::{ConvertJob, load_convert_job},
    process::{
        convert::{
            ConversionPlan,
            execution::{
                ConversionPartitionPlan, ConversionPrepassResult, build_conversion_checkpoints,
                build_conversion_partition_plan,
            },
            plan_conversion,
            reference::{ReferenceObservation, ReferenceState},
            resolve_conversion_catalog,
        },
        merge::PhysicalMergeManifest,
    },
};
use std::{collections::HashMap, fs, path::PathBuf, sync::OnceLock, time::Instant};

const EXPECTED_EVENTS: u64 = 12_627_646;
const EXPECTED_REFERENCE_UPDATES: u64 = 12_627_646;

const WORKER_COUNTS: [usize; 6] = [1, 2, 4, 8, 12, 24];

// -----------------------------------------------------------------------------
// Shared fixture
// -----------------------------------------------------------------------------

struct Fixture {
    job: ConvertJob,
    manifest: PhysicalMergeManifest,
    plan: ConversionPlan,
    dataset: PathBuf,
}

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn fixture() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();

    FIXTURE.get_or_init(|| {
        let root = root();

        let job = load_convert_job(root.join("data/.jobs/convert/convert-multihop.json")).unwrap();

        let dataset = job.input_merge.clone();

        let manifest: PhysicalMergeManifest =
            serde_json::from_slice(&fs::read(dataset.join("manifest.json")).unwrap()).unwrap();

        let catalog = InstrumentCatalog::load(&root.join("data/catalog/instruments.json")).unwrap();

        let resolved = resolve_conversion_catalog(&job, &manifest, &catalog, &root).unwrap();

        let plan = plan_conversion(&job, &resolved).unwrap();

        Fixture {
            job,
            manifest,
            plan,
            dataset,
        }
    })
}

fn execute_prepass(workers: usize) -> (ConversionPartitionPlan, ConversionPrepassResult) {
    let fixture = fixture();

    let partitions =
        build_conversion_partition_plan(&fixture.dataset, &fixture.manifest, workers).unwrap();

    let prepass = build_conversion_checkpoints(
        &fixture.job,
        &fixture.dataset,
        &fixture.manifest,
        &fixture.plan,
        &partitions,
    )
    .unwrap();

    (partitions, prepass)
}

// -----------------------------------------------------------------------------
// Checkpoint count and accounting
// -----------------------------------------------------------------------------

#[test]
fn produces_checkpoint_for_every_partition() {
    for workers in WORKER_COUNTS {
        let (partitions, prepass) = execute_prepass(workers);

        assert_eq!(prepass.checkpoints.len(), partitions.partitions.len(),);

        assert_eq!(prepass.events_scanned, EXPECTED_EVENTS);

        assert_eq!(prepass.reference_updates, EXPECTED_REFERENCE_UPDATES,);
    }
}

// -----------------------------------------------------------------------------
// Boundary alignment
// -----------------------------------------------------------------------------

#[test]
fn checkpoints_match_partition_boundaries() {
    for workers in WORKER_COUNTS {
        let (partitions, prepass) = execute_prepass(workers);

        for (partition, checkpoint) in partitions.partitions.iter().zip(&prepass.checkpoints) {
            assert_eq!(checkpoint.partition_id, partition.partition_id,);

            assert_eq!(checkpoint.events_before_partition, partition.start_row,);
        }
    }
}

// -----------------------------------------------------------------------------
// Initial checkpoint
// -----------------------------------------------------------------------------

#[test]
fn first_checkpoint_is_empty() {
    let (_, prepass) = execute_prepass(24);

    let first = &prepass.checkpoints[0];

    assert_eq!(first.partition_id, 0);
    assert_eq!(first.events_before_partition, 0);
    assert!(first.state.is_empty());
}

// -----------------------------------------------------------------------------
// Reference state completeness
// -----------------------------------------------------------------------------

#[test]
fn later_checkpoints_contain_all_reference_streams() {
    let (_, prepass) = execute_prepass(24);

    for checkpoint in prepass.checkpoints.iter().skip(1) {
        assert_eq!(
            checkpoint.state.len(),
            3,
            "checkpoint {} has incomplete reference state",
            checkpoint.partition_id,
        );
    }
}

// -----------------------------------------------------------------------------
// Reference ordering
// -----------------------------------------------------------------------------

#[test]
fn checkpoint_references_precede_partition_boundary() {
    let (partitions, prepass) = execute_prepass(24);

    let fixture = fixture();

    let mut reader =
        marketforge_engine::historical::convert::execution::PhysicalConversionReader::new(
            &fixture.dataset,
            &fixture.manifest,
            65_536,
        )
        .unwrap();

    let mut boundary_keys = HashMap::new();

    let mut global_row = 0_u64;

    while let Some(batch) = reader.next_batch().unwrap() {
        let timestamps = batch
            .column_by_name("event_timestamp_ns")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();

        let ranks = batch
            .column_by_name("stream_rank")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::UInt32Array>()
            .unwrap();

        let tasks = batch
            .column_by_name("task_id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();

        let ordinals = batch
            .column_by_name("event_ordinal")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();

        for row in 0..batch.num_rows() {
            if let Some(partition) = partitions
                .partitions
                .iter()
                .find(|partition| partition.start_row == global_row)
            {
                boundary_keys.insert(
                    partition.partition_id,
                    marketforge_engine::historical::convert::reference::ReferenceKey::new(
                        timestamps.value(row),
                        ranks.value(row),
                        tasks.value(row),
                        ordinals.value(row),
                    ),
                );
            }

            global_row += 1;
        }
    }

    for checkpoint in &prepass.checkpoints {
        let boundary = boundary_keys.get(&checkpoint.partition_id).unwrap();

        for observation in checkpoint.state.observations() {
            assert!(
                observation.key < *boundary,
                "checkpoint {} contains future reference",
                checkpoint.partition_id,
            );
        }
    }
}

// -----------------------------------------------------------------------------
// Checkpoint monotonicity
// -----------------------------------------------------------------------------

#[test]
fn checkpoint_reference_keys_never_regress() {
    let (_, prepass) = execute_prepass(24);

    let mut previous = HashMap::<String, ReferenceObservation>::new();

    for checkpoint in &prepass.checkpoints {
        for observation in checkpoint.state.observations() {
            if let Some(last) = previous.get(&observation.stream_id) {
                assert!(
                    observation.key >= last.key,
                    "reference regression in checkpoint {}",
                    checkpoint.partition_id,
                );
            }

            previous.insert(observation.stream_id.clone(), observation.clone());
        }
    }
}

// -----------------------------------------------------------------------------
// Snapshot independence
// -----------------------------------------------------------------------------

#[test]
fn checkpoint_snapshots_are_independent() {
    let (_, prepass) = execute_prepass(24);

    let first_nonempty = &prepass.checkpoints[1];
    let last = prepass.checkpoints.last().unwrap();

    let first_observations = first_nonempty
        .state
        .observations()
        .map(|observation| (observation.stream_id.clone(), observation.key))
        .collect::<HashMap<_, _>>();

    let last_observations = last
        .state
        .observations()
        .map(|observation| (observation.stream_id.clone(), observation.key))
        .collect::<HashMap<_, _>>();

    assert_eq!(first_observations.len(), 3);
    assert_eq!(last_observations.len(), 3);

    assert!(
        first_observations
            .iter()
            .any(|(stream, key)| { last_observations.get(stream) != Some(key) })
    );
}

// -----------------------------------------------------------------------------
// Worker configuration independence
// -----------------------------------------------------------------------------

#[test]
fn checkpoint_states_are_independent_of_worker_count() {
    let (partitions_4, prepass_4) = execute_prepass(4);
    let (partitions_24, prepass_24) = execute_prepass(24);

    let states_24 = partitions_24
        .partitions
        .iter()
        .zip(&prepass_24.checkpoints)
        .map(|(partition, checkpoint)| (partition.start_row, &checkpoint.state))
        .collect::<HashMap<_, _>>();

    let mut shared_boundaries = 0;

    for (partition, checkpoint) in partitions_4.partitions.iter().zip(&prepass_4.checkpoints) {
        let Some(other) = states_24.get(&partition.start_row) else {
            continue;
        };

        let observations = |state: &ReferenceState| {
            state
                .observations()
                .map(|observation| {
                    (
                        observation.stream_id.clone(),
                        (observation.key, observation.price),
                    )
                })
                .collect::<HashMap<_, _>>()
        };

        assert_eq!(observations(&checkpoint.state), observations(other),);

        shared_boundaries += 1;
    }

    assert!(shared_boundaries > 0);
}

// -----------------------------------------------------------------------------
// Performance diagnostics
// -----------------------------------------------------------------------------

#[test]
fn prints_prepass_performance() {
    let fixture = fixture();

    let partitions =
        build_conversion_partition_plan(&fixture.dataset, &fixture.manifest, 24).unwrap();

    let start = Instant::now();

    let prepass = build_conversion_checkpoints(
        &fixture.job,
        &fixture.dataset,
        &fixture.manifest,
        &fixture.plan,
        &partitions,
    )
    .unwrap();

    let elapsed = start.elapsed().as_secs_f64();

    println!();
    println!("{}", "=".repeat(85));
    println!("MARKETFORGE — REFERENCE CHECKPOINT PREPASS");
    println!("{}", "=".repeat(85));

    println!("Events scanned    : {}", prepass.events_scanned);
    println!("Reference updates : {}", prepass.reference_updates);
    println!("Checkpoints       : {}", prepass.checkpoints.len());
    println!("Elapsed           : {:.3} s", elapsed);

    if elapsed > 0.0 {
        println!(
            "Throughput        : {:.0} events/s",
            prepass.events_scanned as f64 / elapsed
        );
    }

    println!("STATUS            : PASS");
}
