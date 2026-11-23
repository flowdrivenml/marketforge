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

#[cfg(all(feature = "monitor-tui", target_os = "linux"))]
#[test]
fn merge_cli_pty_restores_terminal_and_keeps_managed_result() {
    use std::{fs, path::Path, process::Command};
    let project = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let python = std::env::var_os("MF_FORGE_TEST_PYTHON");
    let mut modes = vec!["standalone", "managed", "cancel"];
    if python.is_some() {
        modes.extend([
            "forge",
            "forge-json",
            "forge-cancel",
            "forge-recover",
            "forge-prepare",
        ]);
    }
    for mode in modes {
        let root = tempfile::tempdir().unwrap();
        let job = crate::publication::make_job(root.path(), &[111], &[105]);
        let source = root.path().join("merge-job.json");
        fs::write(&source, serde_json::to_vec(&job).unwrap()).unwrap();
        let runtime = root.path().join("merge.toml");
        fs::write(
            &runtime,
            "schema_version=1\n[resources]\nworkers=1\nmemory_limit_mb=2048\nmax_open_files=64\n[partitioning]\npartitions_per_worker=1\n[parquet]\nrow_group_target_mb=1\nfile_target_mb=4\n",
        ).unwrap();
        let result = Command::new(python.as_deref().unwrap_or("python3".as_ref()))
            .env("PYTHONPATH", project)
            .arg(project.join("engine/tests/historical/merge/execution/merge_pty.py"))
            .arg(mode)
            .arg(env!("CARGO_BIN_EXE_marketforge-merge"))
            .arg(&source)
            .arg(&runtime)
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{mode}: {}{}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        if python.is_some() && matches!(mode, "forge" | "forge-recover" | "forge-prepare") {
            print!("{}", String::from_utf8_lossy(&result.stdout));
        }
    }
}
