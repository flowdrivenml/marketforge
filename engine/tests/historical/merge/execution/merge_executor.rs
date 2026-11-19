use crate::{merge_test_job::small_runtime, publication::bounded_trade_job};
use marketforge_engine::historical::merge::{execute_unified_merge_job, validate_committed_merge};

#[test]
fn executes_generated_merge_job() {
    let temporary = tempfile::tempdir().unwrap();
    let job = bounded_trade_job(temporary.path().join("merged"));
    let mut runtime = small_runtime();
    runtime.resources.memory_limit_mb = 2048;
    let job = job.with_runtime(runtime).unwrap();
    let merged = execute_unified_merge_job(&job, temporary.path()).unwrap();
    assert_eq!(merged.job_id, job.job_id);
    assert!(merged.events_written > 0);
    assert_eq!(merged.events_written, merged.trade_events);
    assert_eq!(merged.partitions_completed, 1);
    validate_committed_merge(&merged.dataset_path).unwrap();
}
