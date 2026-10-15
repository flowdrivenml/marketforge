use std::{fs, path::PathBuf, sync::OnceLock};

use arrow_array::RecordBatch;

use marketforge_engine::historical::normalize::{
    convert::execution::{
        ConversionPartitionPlan, ConversionPartitionReader, PhysicalConversionReader,
        build_conversion_partition_plan,
    },
    merge::PhysicalMergeManifest,
};

const EXPECTED_EVENTS: u64 = 12_627_646;
const EXPECTED_ROW_GROUPS: usize = 42;

const WORKER_COUNTS: [usize; 6] = [1, 2, 4, 8, 12, 24];
const BATCH_SIZE: usize = 65_536;

// -----------------------------------------------------------------------------
// Fixtures
// -----------------------------------------------------------------------------

struct Fixture {
    dataset: PathBuf,
    manifest: PhysicalMergeManifest,
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
        let dataset = root().join("data/merged/conversion-multihop");

        let manifest: PhysicalMergeManifest =
            serde_json::from_slice(&fs::read(dataset.join("manifest.json")).unwrap()).unwrap();

        Fixture { dataset, manifest }
    })
}

fn plan(workers: usize) -> ConversionPartitionPlan {
    let fixture = fixture();

    build_conversion_partition_plan(&fixture.dataset, &fixture.manifest, workers).unwrap()
}

// -----------------------------------------------------------------------------
// Read complete partition
// -----------------------------------------------------------------------------

fn read_partition(plan: &ConversionPartitionPlan, partition_index: usize) -> Vec<RecordBatch> {
    let fixture = fixture();

    let partition = &plan.partitions[partition_index];

    let mut reader = ConversionPartitionReader::new(
        &fixture.dataset,
        &fixture.manifest,
        plan,
        partition,
        BATCH_SIZE,
    )
    .unwrap();

    let mut batches = Vec::new();
    let mut rows = 0_u64;

    while let Some(batch) = reader.next_batch().unwrap() {
        rows += batch.num_rows() as u64;
        batches.push(batch);
    }

    assert_eq!(rows, partition.rows);
    assert_eq!(reader.rows_read(), partition.rows);

    batches
}

// -----------------------------------------------------------------------------
// Reader construction
// -----------------------------------------------------------------------------

#[test]
fn constructs_partition_readers() {
    let plan = plan(24);
    let fixture = fixture();

    for partition in &plan.partitions {
        let reader = ConversionPartitionReader::new(
            &fixture.dataset,
            &fixture.manifest,
            &plan,
            partition,
            BATCH_SIZE,
        )
        .unwrap();

        assert_eq!(reader.rows_read(), 0);
        assert_eq!(reader.files_opened(), 0);
    }
}

// -----------------------------------------------------------------------------
// Reject invalid batch size
// -----------------------------------------------------------------------------

#[test]
fn rejects_zero_batch_size() {
    let plan = plan(4);
    let fixture = fixture();

    assert!(
        ConversionPartitionReader::new(
            &fixture.dataset,
            &fixture.manifest,
            &plan,
            &plan.partitions[0],
            0,
        )
        .is_err()
    );
}

// -----------------------------------------------------------------------------
// Row-group coverage
// -----------------------------------------------------------------------------

#[test]
fn reads_every_partition_exactly() {
    for workers in WORKER_COUNTS {
        let plan = plan(workers);

        let mut total = 0_u64;

        for index in 0..plan.partitions.len() {
            let batches = read_partition(&plan, index);

            total += batches
                .iter()
                .map(|batch| batch.num_rows() as u64)
                .sum::<u64>();
        }

        assert_eq!(
            total, EXPECTED_EVENTS,
            "incorrect event coverage with {workers} workers"
        );
    }
}

// -----------------------------------------------------------------------------
// Global ordering
// -----------------------------------------------------------------------------

#[test]
fn preserves_global_event_ordering() {
    use arrow_array::{Int64Array, UInt32Array, UInt64Array};

    for workers in WORKER_COUNTS {
        let plan = plan(workers);

        let mut previous = None;

        for index in 0..plan.partitions.len() {
            let fixture = fixture();

            let mut reader = ConversionPartitionReader::new(
                &fixture.dataset,
                &fixture.manifest,
                &plan,
                &plan.partitions[index],
                BATCH_SIZE,
            )
            .unwrap();

            while let Some(batch) = reader.next_batch().unwrap() {
                let timestamps = batch
                    .column_by_name("event_timestamp_ns")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();

                let ranks = batch
                    .column_by_name("stream_rank")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .unwrap();

                let tasks = batch
                    .column_by_name("task_id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();

                let ordinals = batch
                    .column_by_name("event_ordinal")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();

                for row in 0..batch.num_rows() {
                    let key = (
                        timestamps.value(row),
                        ranks.value(row),
                        tasks.value(row),
                        ordinals.value(row),
                    );

                    if let Some(last) = previous {
                        assert!(last <= key, "ordering regression with {workers} workers");
                    }

                    previous = Some(key);
                }
            }
        }
    }
}

// -----------------------------------------------------------------------------
// Exact sequential equivalence
// -----------------------------------------------------------------------------

#[test]
fn partitioned_reader_matches_sequential_reader() {
    let fixture = fixture();

    for workers in WORKER_COUNTS {
        let plan = plan(workers);

        let mut sequential =
            PhysicalConversionReader::new(&fixture.dataset, &fixture.manifest, BATCH_SIZE).unwrap();

        let mut sequential_batch: Option<RecordBatch> = None;
        let mut sequential_offset = 0_usize;

        let mut compared = 0_u64;

        for partition in &plan.partitions {
            let mut reader = ConversionPartitionReader::new(
                &fixture.dataset,
                &fixture.manifest,
                &plan,
                partition,
                BATCH_SIZE,
            )
            .unwrap();

            while let Some(batch) = reader.next_batch().unwrap() {
                let mut partition_offset = 0_usize;

                while partition_offset < batch.num_rows() {
                    if sequential_batch
                        .as_ref()
                        .is_none_or(|current| sequential_offset >= current.num_rows())
                    {
                        sequential_batch = sequential.next_batch().unwrap();

                        sequential_offset = 0;
                    }

                    let original = sequential_batch
                        .as_ref()
                        .expect("sequential reader ended prematurely");

                    assert_eq!(original.schema(), batch.schema());

                    let count = (original.num_rows() - sequential_offset)
                        .min(batch.num_rows() - partition_offset);

                    for column in 0..batch.num_columns() {
                        let expected = original.column(column).slice(sequential_offset, count);

                        let actual = batch.column(column).slice(partition_offset, count);

                        assert_eq!(
                            expected.to_data(),
                            actual.to_data(),
                            "column mismatch: workers={}, event={}",
                            workers,
                            compared,
                        );
                    }

                    sequential_offset += count;
                    partition_offset += count;
                    compared += count as u64;
                }
            }
        }

        assert_eq!(compared, EXPECTED_EVENTS);

        // Ensure the sequential reader has also reached EOF.
        if let Some(batch) = &sequential_batch {
            assert_eq!(sequential_offset, batch.num_rows());
        }

        assert!(sequential.next_batch().unwrap().is_none());
    }
}

// -----------------------------------------------------------------------------
// Partition row accounting
// -----------------------------------------------------------------------------

#[test]
fn partition_reader_matches_planned_rows() {
    let plan = plan(24);
    let fixture = fixture();

    let mut total = 0_u64;

    for partition in &plan.partitions {
        let mut reader = ConversionPartitionReader::new(
            &fixture.dataset,
            &fixture.manifest,
            &plan,
            partition,
            BATCH_SIZE,
        )
        .unwrap();

        while reader.next_batch().unwrap().is_some() {}

        assert_eq!(reader.rows_read(), partition.rows);

        total += reader.rows_read();
    }

    assert_eq!(total, EXPECTED_EVENTS);
}

// -----------------------------------------------------------------------------
// Row-group count
// -----------------------------------------------------------------------------

#[test]
fn partition_plan_matches_physical_row_groups() {
    let plan = plan(24);

    assert_eq!(plan.row_groups.len(), EXPECTED_ROW_GROUPS);

    let assigned = plan
        .partitions
        .iter()
        .map(|partition| partition.end_row_group - partition.first_row_group)
        .sum::<usize>();

    assert_eq!(assigned, EXPECTED_ROW_GROUPS);
}

// -----------------------------------------------------------------------------
// Deterministic first event
// -----------------------------------------------------------------------------

#[test]
fn partition_first_events_are_deterministic() {
    let fixture = fixture();

    let plan = plan(24);

    for partition in &plan.partitions {
        let mut first = ConversionPartitionReader::new(
            &fixture.dataset,
            &fixture.manifest,
            &plan,
            partition,
            1,
        )
        .unwrap();

        let mut second = ConversionPartitionReader::new(
            &fixture.dataset,
            &fixture.manifest,
            &plan,
            partition,
            1,
        )
        .unwrap();

        let a = first.next_batch().unwrap().unwrap();
        let b = second.next_batch().unwrap().unwrap();

        assert_eq!(a.schema(), b.schema());

        for column in 0..a.num_columns() {
            assert_eq!(a.column(column).to_data(), b.column(column).to_data());
        }
    }
}

// -----------------------------------------------------------------------------
// Diagnostics
// -----------------------------------------------------------------------------

#[test]
fn prints_partition_reader_summary() {
    let plan = plan(24);
    let fixture = fixture();

    println!();
    println!("{}", "=".repeat(85));
    println!("MARKETFORGE — PARALLEL PARTITION READER");
    println!("{}", "=".repeat(85));

    println!("Physical row groups : {}", plan.row_groups.len());
    println!("Partitions          : {}", plan.partitions.len());
    println!("Total events        : {}", plan.total_rows);

    for partition in &plan.partitions {
        let mut reader = ConversionPartitionReader::new(
            &fixture.dataset,
            &fixture.manifest,
            &plan,
            partition,
            BATCH_SIZE,
        )
        .unwrap();

        while reader.next_batch().unwrap().is_some() {}

        println!(
            "Partition {:02}: {:>9} events | {} files",
            partition.partition_id,
            reader.rows_read(),
            reader.files_opened(),
        );
    }

    println!("STATUS: PASS");
}
