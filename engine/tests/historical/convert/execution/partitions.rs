use std::{fs, path::PathBuf};

use {
    marketforge_engine::historical::convert::execution::ConversionPartitionPlan,
    marketforge_engine::historical::convert::execution::build_conversion_partition_plan,
    marketforge_engine::historical::merge::PhysicalMergeManifest,
};

const EXPECTED_EVENTS: u64 = 12_627_646;
const EXPECTED_ROW_GROUPS: usize = 42;

const WORKER_COUNTS: [usize; 6] = [1, 2, 4, 8, 12, 24];

// -----------------------------------------------------------------------------
// Fixtures
// -----------------------------------------------------------------------------

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn fixture() -> (PathBuf, PhysicalMergeManifest) {
    let directory = root().join("data/merged/conversion-multihop");

    let manifest: PhysicalMergeManifest =
        serde_json::from_slice(&fs::read(directory.join("manifest.json")).unwrap()).unwrap();

    (directory, manifest)
}

fn plan(workers: usize) -> ConversionPartitionPlan {
    let (directory, manifest) = fixture();

    build_conversion_partition_plan(&directory, &manifest, workers).unwrap()
}

// -----------------------------------------------------------------------------
// Physical row-group discovery
// -----------------------------------------------------------------------------

#[test]
fn discovers_all_physical_row_groups() {
    let plan = plan(4);

    assert_eq!(plan.total_rows, EXPECTED_EVENTS);
    assert_eq!(plan.row_groups.len(), EXPECTED_ROW_GROUPS);

    assert!(plan.total_compressed_bytes > 0);

    assert_eq!(
        plan.row_groups.iter().map(|group| group.rows).sum::<u64>(),
        EXPECTED_EVENTS
    );
}

// -----------------------------------------------------------------------------
// Partition coverage
// -----------------------------------------------------------------------------

#[test]
fn partitions_cover_every_row_group_exactly_once() {
    for workers in WORKER_COUNTS {
        let plan = plan(workers);

        let mut coverage = vec![0_u32; plan.row_groups.len()];

        for partition in &plan.partitions {
            for index in partition.first_row_group..partition.end_row_group {
                coverage[index] += 1;
            }
        }

        assert!(
            coverage.iter().all(|count| *count == 1),
            "invalid row-group coverage for {workers} workers"
        );
    }
}

// -----------------------------------------------------------------------------
// Chronological continuity
// -----------------------------------------------------------------------------

#[test]
fn partitions_are_contiguous() {
    for workers in WORKER_COUNTS {
        let plan = plan(workers);

        assert_eq!(plan.partitions.first().unwrap().start_row, 0);

        for pair in plan.partitions.windows(2) {
            assert_eq!(
                pair[0].end_row, pair[1].start_row,
                "row discontinuity for {workers} workers"
            );

            assert_eq!(
                pair[0].end_row_group, pair[1].first_row_group,
                "row-group discontinuity for {workers} workers"
            );
        }

        assert_eq!(plan.partitions.last().unwrap().end_row, EXPECTED_EVENTS);

        assert_eq!(
            plan.partitions.last().unwrap().end_row_group,
            plan.row_groups.len()
        );
    }
}

// -----------------------------------------------------------------------------
// Partition accounting
// -----------------------------------------------------------------------------

#[test]
fn partition_row_counts_match_row_groups() {
    for workers in WORKER_COUNTS {
        let plan = plan(workers);

        for partition in &plan.partitions {
            let expected = plan.row_groups[partition.first_row_group..partition.end_row_group]
                .iter()
                .map(|group| group.rows)
                .sum::<u64>();

            assert_eq!(partition.rows, expected);

            assert_eq!(partition.end_row - partition.start_row, partition.rows);
        }
    }
}

// -----------------------------------------------------------------------------
// Worker counts
// -----------------------------------------------------------------------------

#[test]
fn respects_requested_worker_counts() {
    for workers in WORKER_COUNTS {
        let plan = plan(workers);

        assert_eq!(plan.partitions.len(), workers.min(EXPECTED_ROW_GROUPS));
    }
}

#[test]
fn rejects_zero_workers() {
    let (directory, manifest) = fixture();

    assert!(build_conversion_partition_plan(&directory, &manifest, 0,).is_err());
}

// -----------------------------------------------------------------------------
// No empty partitions
// -----------------------------------------------------------------------------

#[test]
fn never_creates_empty_partitions() {
    for workers in WORKER_COUNTS {
        let plan = plan(workers);

        for partition in &plan.partitions {
            assert!(partition.rows > 0);

            assert!(partition.end_row_group > partition.first_row_group);

            assert!(partition.end_row > partition.start_row);
        }
    }
}

// -----------------------------------------------------------------------------
// Determinism
// -----------------------------------------------------------------------------

#[test]
fn partition_planning_is_deterministic() {
    for workers in WORKER_COUNTS {
        let first = plan(workers);
        let second = plan(workers);

        assert_eq!(first.row_groups, second.row_groups);
        assert_eq!(first.partitions, second.partitions);
        assert_eq!(first.total_rows, second.total_rows);
        assert_eq!(first.total_compressed_bytes, second.total_compressed_bytes);
    }
}

// -----------------------------------------------------------------------------
// Row-group global offsets
// -----------------------------------------------------------------------------

#[test]
fn row_group_offsets_are_contiguous() {
    let plan = plan(24);

    let mut expected_offset = 0_u64;

    for group in &plan.row_groups {
        assert_eq!(group.global_start_row, expected_offset);

        expected_offset += group.rows;
    }

    assert_eq!(expected_offset, EXPECTED_EVENTS);
}

// -----------------------------------------------------------------------------
// Diagnostics
// -----------------------------------------------------------------------------

#[test]
fn prints_partition_distribution() {
    for workers in WORKER_COUNTS {
        let plan = plan(workers);

        println!();
        println!("{}", "=".repeat(85));
        println!("CONVERSION PARTITIONS — {workers} WORKERS");
        println!("{}", "=".repeat(85));

        for partition in &plan.partitions {
            println!(
                "Partition {:02} | row groups {:02}..{:02} | \
                 rows {:>9} | global {:>9}..{:>9}",
                partition.partition_id,
                partition.first_row_group,
                partition.end_row_group,
                partition.rows,
                partition.start_row,
                partition.end_row,
            );
        }

        let minimum = plan
            .partitions
            .iter()
            .map(|partition| partition.rows)
            .min()
            .unwrap();

        let maximum = plan
            .partitions
            .iter()
            .map(|partition| partition.rows)
            .max()
            .unwrap();

        println!();
        println!("Minimum partition : {minimum}");
        println!("Maximum partition : {maximum}");
        println!("Imbalance ratio   : {:.3}", maximum as f64 / minimum as f64);
    }
}
