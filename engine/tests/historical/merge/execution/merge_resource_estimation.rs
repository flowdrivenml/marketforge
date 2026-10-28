#![cfg(feature = "historical")]

use {
    marketforge_engine::historical::job::MergeParquetConfig,
    marketforge_engine::historical::merge::MergePartition,
    marketforge_engine::historical::merge::PartitionInputPlan,
    marketforge_engine::historical::merge::PartitionTaskInput,
    marketforge_engine::historical::merge::estimate_merge_partition_resources,
};

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

fn test_config() -> MergeParquetConfig {
    MergeParquetConfig {
        row_group_target_mb: 128,
        file_target_mb: 1024,
        compression_level: 3,
    }
}

fn test_plan(dataset_ids: &[u64]) -> PartitionInputPlan {
    PartitionInputPlan {
        partition: MergePartition {
            partition_id: 0,
            start_ns: 100,
            end_ns: 200,
        },

        inputs: dataset_ids
            .iter()
            .enumerate()
            .map(|(index, &dataset_id)| PartitionTaskInput {
                dataset_id,
                task_id: index as u64,
                start_timestamp_ns: 100,
                end_timestamp_ns: 199,
            })
            .collect(),
    }
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

#[test]
fn estimates_four_stream_partition() {
    let plan = test_plan(&[105, 108, 112, 92]);

    let estimate = estimate_merge_partition_resources(&plan, &test_config()).unwrap();

    assert_eq!(estimate.memory_bytes, 512 * 1024 * 1024,);

    assert_eq!(estimate.scratch_bytes, 0);
    assert_eq!(estimate.open_files, 6);
}

#[test]
fn counts_distinct_streams_not_tasks() {
    let plan = test_plan(&[105, 105, 108, 112, 92]);

    let estimate = estimate_merge_partition_resources(&plan, &test_config()).unwrap();

    assert_eq!(estimate.memory_bytes, 512 * 1024 * 1024,);

    assert_eq!(estimate.open_files, 6);
}

#[test]
fn estimates_empty_partition() {
    let plan = test_plan(&[]);

    let estimate = estimate_merge_partition_resources(&plan, &test_config()).unwrap();

    // Baseline + writer buffering.
    assert_eq!(estimate.memory_bytes, 384 * 1024 * 1024,);

    assert_eq!(estimate.open_files, 2);
}

#[test]
fn rejects_zero_row_group_target() {
    let plan = test_plan(&[105]);

    let mut config = test_config();
    config.row_group_target_mb = 0;

    assert!(estimate_merge_partition_resources(&plan, &config).is_err());
}

#[test]
fn rejects_row_group_overflow() {
    let plan = test_plan(&[105]);

    let mut config = test_config();
    config.row_group_target_mb = u64::MAX;

    assert!(estimate_merge_partition_resources(&plan, &config).is_err());
}
