use marketforge_engine::{
    job::{MergeParquetConfig, MergeResources},
    process::merge::estimate_materialization_resources,
};

const MIB: u64 = 1024 * 1024;

fn parquet() -> MergeParquetConfig {
    MergeParquetConfig {
        row_group_target_mb: 64,
        file_target_mb: 512,
        compression_level: 3,
    }
}

fn resources() -> MergeResources {
    MergeResources {
        workers: 4,
        memory_limit_mb: 8192,
        max_open_files: 256,
    }
}

#[test]
fn estimates_single_task() {
    let plan = estimate_materialization_resources(1, &parquet(), &resources()).unwrap();

    assert!(plan.requirements.memory_bytes > 0);
    assert!(plan.requirements.memory_bytes <= resources().memory_limit_mb * MIB);
    assert_eq!(plan.requirements.open_files, 4);
    assert_eq!(plan.requirements.scratch_bytes, 0);
}

#[test]
fn estimates_multiple_tasks() {
    let one = estimate_materialization_resources(1, &parquet(), &resources()).unwrap();

    let four = estimate_materialization_resources(4, &parquet(), &resources()).unwrap();

    assert!(four.requirements.memory_bytes > one.requirements.memory_bytes);

    assert!(four.requirements.open_files > one.requirements.open_files);
}

#[test]
fn allocates_bounded_cache() {
    let plan = estimate_materialization_resources(2, &parquet(), &resources()).unwrap();

    assert!(plan.limits.cache_bytes >= 64 * MIB);
    assert!(plan.limits.cache_bytes <= 512 * MIB);
}

#[test]
fn creates_valid_window_limits() {
    let plan = estimate_materialization_resources(2, &parquet(), &resources()).unwrap();

    assert!(plan.limits.batch_limits.max_events > 0);
    assert!(plan.limits.batch_limits.max_canonical_rows > 0);
    assert!(plan.limits.reader_batch_size > 0);
}

#[test]
fn rejects_zero_input_tasks() {
    assert!(estimate_materialization_resources(0, &parquet(), &resources(),).is_err());
}

#[test]
fn rejects_zero_workers() {
    let mut config = resources();
    config.workers = 0;

    assert!(estimate_materialization_resources(1, &parquet(), &config,).is_err());
}

#[test]
fn rejects_zero_memory_budget() {
    let mut config = resources();
    config.memory_limit_mb = 0;

    assert!(estimate_materialization_resources(1, &parquet(), &config,).is_err());
}

#[test]
fn rejects_invalid_parquet_targets() {
    let mut config = parquet();
    config.row_group_target_mb = 0;

    assert!(estimate_materialization_resources(1, &config, &resources(),).is_err());

    config.row_group_target_mb = 128;
    config.file_target_mb = 64;

    assert!(estimate_materialization_resources(1, &config, &resources(),).is_err());
}

#[test]
fn rejects_insufficient_worker_memory() {
    let mut config = resources();
    config.memory_limit_mb = 256;

    assert!(estimate_materialization_resources(2, &parquet(), &config,).is_err());
}

#[test]
fn rejects_memory_overflow() {
    let mut config = resources();
    config.memory_limit_mb = u64::MAX;

    assert!(estimate_materialization_resources(1, &parquet(), &config,).is_err());
}

#[test]
fn rejects_row_group_overflow() {
    let mut config = parquet();
    config.row_group_target_mb = u64::MAX;
    config.file_target_mb = u64::MAX;

    assert!(estimate_materialization_resources(1, &config, &resources(),).is_err());
}

#[test]
fn rejects_excessive_input_tasks() {
    assert!(estimate_materialization_resources(usize::MAX, &parquet(), &resources(),).is_err());
}

#[test]
fn resource_estimate_is_independent_of_worker_count() {
    let mut one_worker = resources();
    one_worker.workers = 1;

    let mut eight_workers = resources();
    eight_workers.workers = 8;

    let first = estimate_materialization_resources(2, &parquet(), &one_worker).unwrap();

    let second = estimate_materialization_resources(2, &parquet(), &eight_workers).unwrap();

    assert_eq!(first.requirements, second.requirements);
    assert_eq!(first.limits, second.limits);
}
