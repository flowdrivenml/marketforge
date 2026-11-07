//! Infrastructure verification only. Never calls a production benchmark executor.
#[path = "../support/mod.rs"]
mod support;
use clap::Parser;
use marketforge_engine::{
    historical::{
        job::{ContentType, InstrumentKind},
        normalize::{
            ProcessingResult,
            benchmark::{self, measure, model::*, plan, runner},
            response::NormalizationResponse,
        },
    },
    shared::domain::canonical::Exchange,
};
use serde_json::json;
use std::{fs, path::Path, process::Command};
fn fixture() -> (tempfile::TempDir, std::path::PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let b = support::fixtures::workload().unwrap();
    let b = support::fixtures::select(
        &b,
        "benchmark-plan",
        &support::fixtures::Selection {
            exchange: Exchange::Bybit,
            instrument_type: InstrumentKind::Spot,
            market_category: "spot",
            content_type: ContentType::Trades,
            symbol: Some("BTCUSDT"),
            family: None,
            feed: None,
            filename: Some("BTCUSDT_2026-10-27.csv.gz"),
        },
        1,
    )
    .unwrap();
    let path = temp.path().join("input.json");
    fs::write(&path, serde_json::to_vec(&b).unwrap()).unwrap();
    (temp, path)
}
fn args(path: &Path, extra: &[&str]) -> Args {
    let root = support::root();
    let mut words = vec![
        "benchmark",
        "--input",
        path.to_str().unwrap(),
        "--project-root",
        root.to_str().unwrap(),
    ];
    words.extend_from_slice(extra);
    Args::try_parse_from(words).unwrap()
}
fn make_plan(path: &Path, extra: &[&str]) -> Plan {
    plan::plan(&args(path, extra), &std::env::current_dir().unwrap()).unwrap()
}
#[test]
fn validates_arguments_and_resource_overrides() {
    let (_temp, path) = fixture();
    for workers in ["0", "1,1", ""] {
        let parsed = Args::try_parse_from([
            "benchmark",
            "--input",
            path.to_str().unwrap(),
            "--workers",
            workers,
        ]);
        assert!(parsed.is_err() || plan::plan(&parsed.unwrap(), &support::root()).is_err());
    }
    for extra in [
        vec!["--repetitions", "0"],
        vec!["--memory-budget", "0"],
        vec!["--row-group-target", "999999999999", "--file-target", "1"],
    ] {
        assert!(plan::plan(&args(&path, &extra), &support::root()).is_err());
    }
    assert!(Args::try_parse_from(["benchmark"]).is_err());
    assert!(Args::try_parse_from(["benchmark", "--input", "x", "--scheduler", "other"]).is_err());
    assert!(Args::try_parse_from(["benchmark", "--input", "x", "--retention", "other"]).is_err());
    let p = make_plan(
        &path,
        &[
            "--warmups",
            "0",
            "--workers",
            "2",
            "--memory-budget",
            "2147483648",
        ],
    );
    assert_eq!(p.trials[0].config.resources.workers, 2);
    assert_eq!(p.trials[0].config.resources.memory_budget_bytes, 2147483648);
    assert!(p.trials.iter().all(|t| !t.warmup));
}
#[test]
fn deterministic_alternating_trial_order_and_no_exclusions() {
    let (_temp, path) = fixture();
    let p = make_plan(
        &path,
        &["--workers", "1,2", "--warmups", "1", "--repetitions", "2"],
    );
    assert_eq!(p.trials.len(), 6);
    assert_eq!(
        p.trials.iter().map(|t| t.workers).collect::<Vec<_>>(),
        vec![1, 2, 2, 1, 1, 2]
    );
    assert_eq!(p.trials.iter().filter(|t| t.warmup).count(), 2);
    assert_eq!(p.job_count, 1);
    assert_eq!(p.task_count, 1);
    assert!(!p.directory.exists());
    for t in &p.trials {
        assert!(
            t.jobs
                .iter()
                .all(|j| j.output.dataset_path.starts_with(&t.directory))
        );
    }
    let mut b: marketforge_engine::historical::config::PreparedBatch =
        serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
    b.jobs[0].tasks[0].exchange = Exchange::GateIo;
    b.jobs[0].tasks[0].symbol = Some("BTC_USDT".into());
    let instrument = b
        .instruments
        .iter()
        .find(|i| i.exchange == Exchange::GateIo && i.instrument_type == InstrumentKind::Spot)
        .unwrap()
        .clone();
    b.jobs[0].tasks[0].instrument_id = Some(instrument.instrument_id);
    b.jobs[0].tasks[0].instrument = Some(instrument.instrument.clone());
    b.jobs[0].tasks[0].symbol = Some(instrument.symbol);
    b.jobs[0].tasks[0].base_asset = Some(instrument.base_asset);
    b.jobs[0].tasks[0].quote_asset = Some(instrument.quote_asset);
    b.jobs[0].tasks[0].input_path = tempfile::TempDir::new()
        .unwrap()
        .path()
        .join("explicit-synthetic-gate-source");
    fs::write(&path, serde_json::to_vec(&b).unwrap()).unwrap();
    let p = make_plan(&path, &["--workers", "1", "--repetitions", "1"]);
    assert_eq!(p.task_count, 1);
    assert_eq!(p.trials[0].jobs[0].tasks[0].exchange, Exchange::GateIo);
}

#[test]
fn dry_run_actual_cli_is_nonexecuting_and_accepts_any_input_location() {
    let (temp, path) = fixture();
    let output = temp.path().join("never-created");
    let o = Command::new(env!("CARGO_BIN_EXE_marketforge-normalize-benchmark"))
        .current_dir(temp.path())
        .arg("--input")
        .arg("input.json")
        .arg("--project-root")
        .arg(support::root())
        .args([
            "--workers",
            "1,2",
            "--warmups",
            "1",
            "--repetitions",
            "2",
            "--dry-run",
            "--output-root",
        ])
        .arg(&output)
        .output()
        .unwrap();
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
    assert_eq!(v["kind"], "normalization_benchmark_plan");
    assert_eq!(v["trials"].as_array().unwrap().len(), 6);
    assert!(!output.exists());
    assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 1);
    assert_eq!(path, temp.path().join("input.json"));
}
#[test]
fn synthetic_measurement_units_and_unavailable_counters() {
    let a = measure::Snapshot {
        user: Some(2.),
        system: Some(1.),
        logical_read: Some(100),
        ..Default::default()
    };
    let b = measure::Snapshot {
        user: Some(5.),
        system: Some(2.),
        logical_read: Some(150),
        peak_rss_bytes: Some(2048),
        ..Default::default()
    };
    let d = measure::difference(&a, &b);
    assert_eq!(d.process_cpu_total_seconds, Some(4.));
    assert_eq!(d.logical_read_bytes, Some(50));
    assert_eq!(d.physical_read_bytes, None);
    assert_eq!(d.peak_rss_bytes, Some(2048));
    assert_eq!(measure::rate(100, 2., true), Some(50.));
    assert_eq!(measure::rate(100, 0., true), None);
    assert_eq!(measure::rate(100, 2., false), None);
    assert_eq!(measure::rate(100, f64::NAN, true), None);
    let io = measure::parse_linux_io("rchar: 9\nwchar: 7\nread_bytes: 2\nwrite_bytes: 3\n");
    assert_eq!(io.logical_read, Some(9));
    assert_eq!(io.physical_read, Some(2));
}
struct Mock {
    fail: bool,
    calls: usize,
}
impl TrialExecutor for Mock {
    fn execute(&mut self, t: &TrialPlan) -> benchmark::Result<ChildExecution> {
        self.calls += 1;
        if self.fail {
            return Err("mocked child interruption".into());
        }
        for job in &t.jobs {
            fs::create_dir_all(&job.output.dataset_path).unwrap();
        }
        let results = t
            .jobs
            .iter()
            .map(|j| ProcessingResult::complete(j.job_id, j.dataset_id))
            .collect();
        Ok(ChildExecution {
            schema_version: 1,
            measurement_source: "synthetic-test-fixture".into(),
            response: NormalizationResponse::new(None, t.jobs.len(), results, None),
            execution_wall_seconds: 2.,
            per_job_wall_seconds: vec![None; t.jobs.len()],
            process: ProcessMetrics::default(),
        })
    }
}
fn fake_output(_: &TrialPlan, _: &ChildExecution) -> benchmark::Result<OutputObservation> {
    Ok(OutputObservation {
        source_records_read: 100,
        canonical_rows: 200,
        parquet_bytes: 300,
        parquet_file_count: 1,
        parquet_row_group_count: 2,
        ..Default::default()
    })
}
#[test]
fn mocked_trials_persist_incrementally_and_metadata_cleanup_is_owned() {
    let (temp, path) = fixture();
    let mut p = make_plan(&path, &["--workers", "1", "--repetitions", "2"]);
    let root = runner::allocate_run(Some(&temp.path().join("run")), &support::root()).unwrap();
    plan::rebase(&mut p, &root).unwrap();
    let mut executor = Mock {
        fail: false,
        calls: 0,
    };
    let run = runner::run_with(p.clone(), &mut executor, fake_output).unwrap();
    assert_eq!(executor.calls, 2);
    assert_eq!(run.state, State::Complete);
    let text = fs::read_to_string(root.join("results.jsonl")).unwrap();
    assert_eq!(text.lines().count(), 2);
    for line in text.lines() {
        let result: TrialResult = serde_json::from_str(line).unwrap();
        result.validate_version().unwrap();
        let reloaded = marketforge_engine::shared::historical::load_processing_config(
            result.trial.directory.join("effective-config.toml"),
            support::root(),
        )
        .unwrap();
        assert_eq!(reloaded.resources, result.trial.config.resources);
        assert_eq!(
            reloaded.integrity_policy,
            result.trial.config.integrity_policy
        );
        assert_eq!(result.source_records_per_second, Some(50.));
        assert_eq!(result.canonical_rows_per_second, Some(100.));
        assert!(!result.dataset_retained);
        assert_eq!(
            result.execution.unwrap().measurement_source,
            "synthetic-test-fixture"
        );
    }
    assert!(root.join("workload.json").is_file());
    assert!(
        p.trials
            .iter()
            .all(|t| !t.jobs[0].output.dataset_path.exists())
    );
    assert!(runner::run_with(p, &mut executor, fake_output).is_err());
}
#[test]
fn failed_mock_remains_visible_without_successful_throughput() {
    let (temp, path) = fixture();
    let mut p = make_plan(&path, &["--repetitions", "1"]);
    let root = runner::allocate_run(Some(&temp.path().join("failed")), &support::root()).unwrap();
    plan::rebase(&mut p, &root).unwrap();
    let run = runner::run_with(
        p,
        &mut Mock {
            fail: true,
            calls: 0,
        },
        fake_output,
    )
    .unwrap();
    assert_eq!(run.state, State::Failed);
    let result: TrialResult = serde_json::from_str(
        fs::read_to_string(root.join("results.jsonl"))
            .unwrap()
            .trim(),
    )
    .unwrap();
    assert_eq!(result.state, State::Failed);
    assert!(result.error.unwrap().contains("mocked child interruption"));
    assert_eq!(result.canonical_rows_per_second, None);
    assert!(!result.correctness_verified);
}
#[test]
fn full_retention_and_unsupported_schema_handling() {
    let (temp, path) = fixture();
    let mut p = make_plan(&path, &["--repetitions", "1", "--retention", "full"]);
    let root = runner::allocate_run(Some(&temp.path().join("full")), &support::root()).unwrap();
    plan::rebase(&mut p, &root).unwrap();
    let mut run = runner::run_with(
        p,
        &mut Mock {
            fail: false,
            calls: 0,
        },
        fake_output,
    )
    .unwrap();
    let mut result: TrialResult = serde_json::from_str(
        fs::read_to_string(root.join("results.jsonl"))
            .unwrap()
            .trim(),
    )
    .unwrap();
    assert!(result.dataset_retained);
    assert!(result.trial.jobs[0].output.dataset_path.is_dir());
    result.schema_version = 99;
    assert!(result.validate_version().is_err());
    run.schema_version = 99;
    assert!(run.validate_version().is_err());
    let v = serde_json::to_value(ProcessMetrics::default()).unwrap();
    assert!(v["peak_rss_bytes"].is_null());
    assert_eq!(json!(u64::MAX).as_u64(), Some(u64::MAX));
}
#[test]
fn exclusive_runs_and_cleanup_refuse_collisions_traversal_and_wrong_marker() {
    let (temp, path) = fixture();
    let a = runner::allocate_run(Some(&temp.path().join("one")), &support::root()).unwrap();
    assert!(runner::allocate_run(Some(&a), &support::root()).is_err());
    let mut p = make_plan(&path, &["--repetitions", "1"]);
    plan::rebase(&mut p, &a).unwrap();
    let t = &p.trials[0];
    assert!(runner::owned(&a, &temp.path().join("outside")).is_err());
    assert!(runner::owned(&a, &a.join("../escape")).is_err());
    assert!(runner::cleanup_success(t).is_err());
}
#[cfg(unix)]
#[test]
fn cleanup_refuses_symlink_escape() {
    let (temp, path) = fixture();
    let mut p = make_plan(&path, &["--repetitions", "1"]);
    let root = runner::allocate_run(Some(&temp.path().join("owned")), &support::root()).unwrap();
    plan::rebase(&mut p, &root).unwrap();
    let t = &p.trials[0];
    fs::create_dir_all(&t.directory).unwrap();
    fs::write(t.directory.join(".trial-owner"), &t.trial_id).unwrap();
    fs::create_dir_all(t.directory.join("datasets")).unwrap();
    std::os::unix::fs::symlink(temp.path(), &t.jobs[0].output.dataset_path).unwrap();
    assert!(runner::cleanup_success(t).is_err());
    assert!(path.exists());
}
#[test]
fn interruption_preserves_running_trial_without_claiming_completion() {
    let (temp, path) = fixture();
    let mut p = make_plan(&path, &["--repetitions", "1"]);
    let root =
        runner::allocate_run(Some(&temp.path().join("interrupted")), &support::root()).unwrap();
    plan::rebase(&mut p, &root).unwrap();
    let trial = p.trials[0].directory.clone();
    struct Interrupt;
    impl TrialExecutor for Interrupt {
        fn execute(&mut self, _: &TrialPlan) -> benchmark::Result<ChildExecution> {
            panic!("synthetic interruption")
        }
    }
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        runner::run_with(p, &mut Interrupt, fake_output)
    }));
    assert!(caught.is_err());
    let run: RunMetadata =
        serde_json::from_slice(&fs::read(root.join("run.json")).unwrap()).unwrap();
    assert_eq!(run.state, State::Running);
    assert!(run.finished_unix_ns.is_none());
    let result: TrialResult =
        serde_json::from_slice(&fs::read(trial.join("result.json")).unwrap()).unwrap();
    assert_eq!(result.state, State::Running);
    assert!(!result.correctness_verified);
    assert!(result.ended_unix_ns.is_none());
}
#[test]
fn source_changes_during_mock_execution_fail_trial() {
    let (temp, path) = fixture();
    let mut p = make_plan(&path, &["--repetitions", "1"]);
    let source = temp.path().join("synthetic-source");
    fs::write(&source, "first").unwrap();
    p.trials[0].jobs[0].tasks[0].input_path = source;
    let root = runner::allocate_run(Some(&temp.path().join("changed")), &support::root()).unwrap();
    plan::rebase(&mut p, &root).unwrap();
    struct Change;
    impl TrialExecutor for Change {
        fn execute(&mut self, t: &TrialPlan) -> benchmark::Result<ChildExecution> {
            fs::write(&t.jobs[0].tasks[0].input_path, "changed bytes").unwrap();
            Mock {
                fail: false,
                calls: 0,
            }
            .execute(t)
        }
    }
    let run = runner::run_with(p, &mut Change, fake_output).unwrap();
    assert_eq!(run.state, State::Failed);
    let result: TrialResult = serde_json::from_str(
        fs::read_to_string(root.join("results.jsonl"))
            .unwrap()
            .trim(),
    )
    .unwrap();
    assert!(result.error.unwrap().contains("source metadata changed"));
    assert_eq!(result.source_records_per_second, None);
}
#[test]
fn generated_run_ids_are_unique_without_overwriting() {
    let temp = tempfile::tempdir().unwrap();
    let a = runner::allocate_run(None, temp.path()).unwrap();
    let b = runner::allocate_run(None, temp.path()).unwrap();
    assert_ne!(a, b);
    assert!(a.is_dir() && b.is_dir());
}
#[test]
fn prepared_input_change_is_rejected_without_execution() {
    let (temp, path) = fixture();
    let mut p = make_plan(&path, &["--repetitions", "1"]);
    let root =
        runner::allocate_run(Some(&temp.path().join("input-changed")), &support::root()).unwrap();
    plan::rebase(&mut p, &root).unwrap();
    fs::write(&path, "changed document").unwrap();
    let mut executor = Mock {
        fail: false,
        calls: 0,
    };
    assert!(
        runner::run_with(p, &mut executor, fake_output)
            .unwrap_err()
            .contains("input changed")
    );
    assert_eq!(executor.calls, 0);
}
#[test]
fn mixed_mock_outcomes_preserve_prior_definitive_trials() {
    let (temp, path) = fixture();
    let mut p = make_plan(&path, &["--repetitions", "2", "--retention", "full"]);
    let root = runner::allocate_run(Some(&temp.path().join("mixed")), &support::root()).unwrap();
    plan::rebase(&mut p, &root).unwrap();
    struct Mixed {
        calls: usize,
    }
    impl TrialExecutor for Mixed {
        fn execute(&mut self, t: &TrialPlan) -> benchmark::Result<ChildExecution> {
            self.calls += 1;
            Mock {
                fail: self.calls == 2,
                calls: 0,
            }
            .execute(t)
        }
    }
    let run = runner::run_with(p, &mut Mixed { calls: 0 }, fake_output).unwrap();
    assert_eq!(run.state, State::Failed);
    assert_eq!(run.trials_complete, 1);
    assert_eq!(run.trials_failed, 1);
    let results: Vec<TrialResult> = fs::read_to_string(root.join("results.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].state, State::Complete);
    assert_eq!(results[1].state, State::Failed);
}
#[test]
fn verification_failure_is_not_a_successful_measurement() {
    let (temp, path) = fixture();
    let mut p = make_plan(&path, &["--repetitions", "1"]);
    let root =
        runner::allocate_run(Some(&temp.path().join("verify-failed")), &support::root()).unwrap();
    plan::rebase(&mut p, &root).unwrap();
    runner::run_with(
        p,
        &mut Mock {
            fail: false,
            calls: 0,
        },
        |_, _| Err("synthetic manifest mismatch".into()),
    )
    .unwrap();
    let result: TrialResult = serde_json::from_str(
        fs::read_to_string(root.join("results.jsonl"))
            .unwrap()
            .trim(),
    )
    .unwrap();
    assert_eq!(result.state, State::Failed);
    assert!(result.execution.is_some());
    assert!(result.canonical_rows_per_second.is_none());
    assert!(!result.correctness_verified);
    assert!(result.dataset_retained);
    assert!(result.error.unwrap().contains("manifest mismatch"));
}
