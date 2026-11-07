//! Entirely synthetic, explicitly tagged observations; normalization is never run.
use super::*;
use benchmark::{
    analysis::{self, LoadedRun, WorkloadMetadata},
    comparison::{self, Comparability as C, Variable},
    provenance::{Environment, SourceObservation},
    reports,
};
use std::collections::BTreeMap;
fn synthetic(name: &str, workers: &[(usize, &[f64])]) -> tempfile::TempDir {
    let (temp_input, input) = fixture();
    let temporary = tempfile::tempdir().unwrap();
    let directory = temporary.path().join(name);
    fs::create_dir(&directory).unwrap();
    let word = workers
        .iter()
        .map(|(w, _)| w.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let count = workers[0].1.len().to_string();
    let mut plan = make_plan(
        &input,
        &[
            "--workers",
            &word,
            "--repetitions",
            &count,
            "--retention",
            "metadata",
        ],
    );
    benchmark::plan::rebase(&mut plan, &directory).unwrap();
    for t in &mut plan.trials {
        for job in &mut t.jobs {
            for task in &mut job.tasks {
                task.input_path = "/synthetic-source/trades.gz".into();
            }
        }
    }
    let mut prepared: marketforge_engine::historical::config::PreparedBatch =
        serde_json::from_slice(&fs::read(&input).unwrap()).unwrap();
    for j in &mut prepared.jobs {
        for t in &mut j.tasks {
            t.input_path = "/synthetic-source/trades.gz".into();
        }
    }
    let workload = WorkloadMetadata {
        schema_version: 1,
        input: input.clone(),
        input_sha256: plan.input_sha256.clone(),
        prepared_batch: prepared,
        resolved_jobs: plan.trials[0].jobs.clone(),
        sources: vec![SourceObservation {
            path: "/synthetic-source/trades.gz".into(),
            size_bytes: Some(1000),
            modified_unix_ns: Some("1".into()),
            sha256: Some("a".repeat(64)),
            evidence: "synthetic verified-content fixture, no real archive claims".into(),
        }],
    };
    let environment = Environment {
        schema_version: 1,
        git_commit: Some("synthetic-revision-a".into()),
        git_branch: Some("synthetic".into()),
        git_dirty: Some(false),
        rust_compiler: "synthetic-rustc".into(),
        build_profile: "synthetic-release".into(),
        build_target: "synthetic-test64".into(),
        cargo_features: vec!["historical".into()],
        benchmark_executable: "/synthetic/benchmark".into(),
        executable_sha256: Some("d".repeat(64)),
        normalization_executable_identity: "synthetic linked production boundary".into(),
        cargo_lock_sha256: Some("e".repeat(64)),
        architecture: "synthetic-test64".into(),
        operating_system: "synthetic-os".into(),
        kernel: Some("synthetic-kernel".into()),
        cpu_model: Some("synthetic-cpu".into()),
        logical_cpu_count: Some(8),
        physical_cpu_count: Some(4),
        available_parallelism: Some(8),
        total_memory_bytes: Some(16000000000),
        cpu_frequency: Some("synthetic-fixed".into()),
        cpu_power_profile: Some("synthetic-controlled".into()),
        filesystem_type: Some("synthetic-fs".into()),
        storage_device_type: Some("synthetic-device".into()),
        limitations: vec!["synthetic test metadata, not a performance observation".into()],
    };
    let run = RunMetadata {
        schema_version: 1,
        kind: "normalization_benchmark_run".into(),
        run_id: name.into(),
        state: State::Complete,
        created_unix_ns: "1".into(),
        finished_unix_ns: Some("2".into()),
        plan: plan.clone(),
        trials_complete: plan.trials.len(),
        trials_failed: 0,
        cache_policy: "synthetic-controlled-cache".into(),
    };
    benchmark::runner::atomic_json(&directory.join("run.json"), &run).unwrap();
    benchmark::runner::atomic_json(&directory.join("environment.json"), &environment).unwrap();
    benchmark::runner::atomic_json(&directory.join("workload.json"), &workload).unwrap();
    let mut lines = String::new();
    for t in &plan.trials {
        let seconds = workers.iter().find(|(w, _)| *w == t.workers).unwrap().1[t.repetition - 1];
        let mut results = vec![];
        for job in &t.jobs {
            let mut r = ProcessingResult::complete(job.job_id, job.dataset_id);
            r.manifest_path = Some(
                job.output
                    .dataset_path
                    .join("manifest.json")
                    .display()
                    .to_string(),
            );
            r.events_written = 2000;
            r.files_written = 2;
            results.push(r);
        }
        let files = (0..2)
            .map(|i| FileObservation {
                job_id: t.jobs[0].job_id.0,
                dataset_id: t.jobs[0].dataset_id.0,
                path: format!("tasks/task-000001/trades/part-{i:06}.parquet"),
                size_bytes: 1500,
                rows: 1000,
                row_groups: 1,
                compressed_column_bytes: 1300,
                uncompressed_column_bytes: 2500,
                start_timestamp_ns: Some(1),
                end_timestamp_ns: Some(2),
            })
            .collect();
        let output = OutputObservation {
            source_records_read: 1000,
            canonical_rows: 2000,
            parquet_bytes: 3000,
            parquet_file_count: 2,
            parquet_row_group_count: 2,
            files,
            work_by_content_type: BTreeMap::from([(
                "trades".into(),
                WorkVolume {
                    source_records_read: 1000,
                    canonical_rows: 2000,
                },
            )]),
            work_by_stream: BTreeMap::from([(
                t.jobs[0].tasks[0].stream_id.0.clone(),
                WorkVolume {
                    source_records_read: 1000,
                    canonical_rows: 2000,
                },
            )]),
            semantic_stream_sha256: BTreeMap::from([("synthetic-stream".into(), "b".repeat(64))]),
            ..Default::default()
        };
        let execution = ChildExecution {
            schema_version: 1,
            measurement_source: "synthetic-test-fixture".into(),
            response: NormalizationResponse::new(None, t.jobs.len(), results, None),
            execution_wall_seconds: seconds,
            per_job_wall_seconds: vec![None; t.jobs.len()],
            process: ProcessMetrics {
                process_cpu_user_seconds: Some(seconds * 1.5),
                process_cpu_system_seconds: Some(seconds * 0.5),
                process_cpu_total_seconds: Some(seconds * 2.),
                peak_rss_bytes: Some(1000),
                logical_read_bytes: Some(1000),
                logical_written_bytes: Some(2000),
                physical_read_bytes: Some(100),
                physical_written_bytes: Some(200),
                peak_accounted_memory_bytes: Some(500),
            },
        };
        let result = TrialResult {
            schema_version: 1,
            kind: "normalization_benchmark_trial".into(),
            benchmark_run_id: name.into(),
            trial: t.clone(),
            state: State::Complete,
            started_unix_ns: "1".into(),
            ended_unix_ns: Some("2".into()),
            execution: Some(execution),
            trial_wall_seconds: Some(seconds + 1.),
            verification_wall_seconds: Some(0.1),
            output: Some(output),
            source_records_per_second: Some(1000. / seconds),
            canonical_rows_per_second: Some(2000. / seconds),
            input_bytes_per_second: Some(1000. / seconds),
            output_bytes_per_second: Some(3000. / seconds),
            source_archive_count: 1,
            source_file_bytes: Some(1000),
            uncompressed_input_bytes: None,
            correctness_verified: true,
            dataset_retained: false,
            error: None,
        };
        fs::create_dir_all(&t.directory).unwrap();
        benchmark::runner::atomic_json(&t.directory.join("result.json"), &result).unwrap();
        lines.push_str(&serde_json::to_string(&result).unwrap());
        lines.push('\n');
    }
    fs::write(directory.join("results.jsonl"), lines).unwrap();
    drop(temp_input);
    temporary
}
fn loaded(temp: &tempfile::TempDir, name: &str) -> LoadedRun {
    analysis::load_run(&temp.path().join(name)).unwrap()
}
fn compared(b: &LoadedRun, c: &LoadedRun, variables: &[Variable]) -> comparison::Comparison {
    comparison::compare(
        b,
        c,
        &analysis::summarize(b, 1).unwrap(),
        &analysis::summarize(c, 1).unwrap(),
        variables,
        "synthetic-comparison".into(),
    )
    .unwrap()
}
fn mutate(temp: &tempfile::TempDir, name: &str, mut f: impl FnMut(&mut TrialResult)) {
    let path = temp.path().join(name);
    let run: RunMetadata =
        serde_json::from_slice(&fs::read(path.join("run.json")).unwrap()).unwrap();
    let mut lines = String::new();
    for t in run.plan.trials {
        let file = path.join("trials").join(t.trial_id).join("result.json");
        let mut result: TrialResult = serde_json::from_slice(&fs::read(&file).unwrap()).unwrap();
        f(&mut result);
        fs::write(file, serde_json::to_vec(&result).unwrap()).unwrap();
        lines.push_str(&serde_json::to_string(&result).unwrap());
        lines.push('\n');
    }
    fs::write(path.join("results.jsonl"), lines).unwrap();
}
#[test]
fn statistical_groups_scaling_and_pooled_throughput() {
    let t = synthetic(
        "scaling",
        &[(1, &[100., 100.]), (2, &[50., 50.]), (4, &[25., 25.])],
    );
    let run = loaded(&t, "scaling");
    let s = analysis::summarize(&run, 1).unwrap();
    assert_eq!(s.status, "complete");
    assert_eq!(s.groups.len(), 3);
    for (g, w) in s.groups.iter().zip([1., 2., 4.]) {
        assert_eq!(g.scaling.speedup, Some(w));
        assert_eq!(g.scaling.relative_parallel_efficiency, Some(1.));
        assert_eq!(
            analysis::median(g, "process_cpu_utilization_percent"),
            Some(200.)
        );
    }
    let s = analysis::summarize(&run, 2).unwrap();
    assert_eq!(s.groups[2].scaling.speedup, Some(2.));
    assert_eq!(s.groups[2].scaling.relative_parallel_efficiency, Some(1.));
    let t = synthetic("pooled", &[(1, &[1., 3.])]);
    let s = analysis::summarize(&loaded(&t, "pooled"), 1).unwrap();
    let g = &s.groups[0];
    assert_eq!(g.pooled_source_records_per_second, Some(500.));
    assert!(
        (g.metrics["source_records_per_second"]
            .statistics
            .as_ref()
            .unwrap()
            .mean
            - 666.6666666666666)
            .abs()
            < 1e-10
    );
    assert_eq!(
        analysis::median(g, "parquet_median_file_bytes"),
        Some(1500.)
    );
    assert_eq!(
        analysis::median(g, "files_per_million_canonical_rows"),
        Some(1000.)
    );
}
#[test]
fn missing_failed_and_slower_baselines() {
    let t = synthetic("missing", &[(2, &[10., 10.]), (4, &[20., 20.])]);
    let s = analysis::summarize(&loaded(&t, "missing"), 1).unwrap();
    assert!(s.groups.iter().all(|g| g.scaling.speedup.is_none()));
    let s = analysis::summarize(&loaded(&t, "missing"), 2).unwrap();
    assert_eq!(s.groups[1].scaling.speedup, Some(0.5));
    assert_eq!(s.groups[1].scaling.relative_parallel_efficiency, Some(0.25));
    let t = synthetic("failed", &[(1, &[10., 10.]), (2, &[5., 5.])]);
    mutate(&t, "failed", |r| {
        if r.trial.workers == 1 {
            r.state = State::Failed;
            r.correctness_verified = false;
            r.error = Some("synthetic failure".into());
        }
    });
    let s = analysis::summarize(&loaded(&t, "failed"), 1).unwrap();
    assert_eq!(s.status, "incomplete");
    assert!(s.groups.iter().all(|g| g.scaling.speedup.is_none()));
    assert_eq!(s.accounting.measured_failed, 2);
}
#[test]
fn comparable_software_revision_and_equivalent_physical_layouts() {
    let b = synthetic("baseline", &[(1, &[100., 100.])]);
    let c = synthetic("candidate", &[(1, &[80., 80.])]);
    mutate(&c, "candidate", |r| {
        let o = r.output.as_mut().unwrap();
        o.files.truncate(1);
        o.files[0].rows = 2000;
        o.files[0].size_bytes = 3000;
        o.files[0].row_groups = 2;
        o.parquet_file_count = 1;
        r.execution.as_mut().unwrap().response.jobs[0].files_written = 1;
    });
    let br = loaded(&b, "baseline");
    let mut cr = loaded(&c, "candidate");
    cr.environment.git_commit = Some("synthetic-revision-b".into());
    let report = compared(&br, &cr, &[]);
    assert_eq!(report.comparability, C::Comparable);
    let duration = report
        .metric_comparisons
        .iter()
        .find(|m| m.metric == "execution_wall_seconds")
        .unwrap();
    assert_eq!(duration.absolute_difference, Some(-20.));
    assert_eq!(duration.percentage_difference, Some(-20.));
    assert_eq!(duration.ratio, Some(1.25));
    let throughput = report
        .metric_comparisons
        .iter()
        .find(|m| m.metric == "source_records_per_second")
        .unwrap();
    assert_eq!(throughput.percentage_difference, Some(25.));
    let files = report
        .metric_comparisons
        .iter()
        .find(|m| m.metric == "parquet_file_count")
        .unwrap();
    assert_eq!(files.percentage_difference, Some(-50.));
}
#[test]
fn relocation_with_verified_sources_and_weaker_identity() {
    let a = synthetic("a", &[(1, &[10., 10.])]);
    let b = synthetic("b", &[(1, &[10., 10.])]);
    let ar = loaded(&a, "a");
    let mut br = loaded(&b, "b");
    br.workload.sources[0].path = "/relocated/trades.gz".into();
    for j in &mut br.workload.resolved_jobs {
        for t in &mut j.tasks {
            t.input_path = "/relocated/trades.gz".into();
        }
    }
    assert_eq!(compared(&ar, &br, &[]).comparability, C::Comparable);
    let mut weak_a = loaded(&a, "a");
    weak_a.workload.sources[0].sha256 = None;
    br.workload.sources[0].sha256 = None;
    assert_eq!(
        compared(&weak_a, &br, &[]).comparability,
        C::ComparableWithCaveats
    );
    br.workload.sources[0].size_bytes = None;
    assert_eq!(compared(&weak_a, &br, &[]).comparability, C::Incomparable);
}
#[test]
fn material_source_correctness_and_interpretation_mismatches_are_incomparable() {
    let a = synthetic("a", &[(1, &[10., 10.])]);
    let b = synthetic("b", &[(1, &[8., 8.])]);
    let ar = loaded(&a, "a");
    let mut br = loaded(&b, "b");
    br.workload.sources[0].sha256 = Some("c".repeat(64));
    assert_eq!(compared(&ar, &br, &[]).comparability, C::Incomparable);
    let mut br = loaded(&b, "b");
    br.workload.resolved_jobs[0].tasks[0].raw_schema["marketforge_profile"]["sha256"] =
        json!("different");
    assert_eq!(compared(&ar, &br, &[]).comparability, C::Incomparable);
    assert_eq!(
        compared(&ar, &br, &[Variable::Profiles]).comparability,
        C::ComparableWithCaveats
    );
    let mut br = loaded(&b, "b");
    for r in br.results.values_mut() {
        r.output
            .as_mut()
            .unwrap()
            .semantic_stream_sha256
            .insert("synthetic-stream".into(), "c".repeat(64));
    }
    let report = compared(&ar, &br, &[]);
    assert_eq!(report.comparability, C::Incomparable);
    assert!(report.metric_comparisons.iter().all(|m| m.ratio.is_none()));
}
#[test]
fn hardware_build_dirty_optional_and_timing_differences_classified() {
    let a = synthetic("a", &[(1, &[10., 10.])]);
    let b = synthetic("b", &[(1, &[8., 8.])]);
    let ar = loaded(&a, "a");
    let mut br = loaded(&b, "b");
    br.environment.cpu_model = Some("synthetic-other-cpu".into());
    assert_eq!(compared(&ar, &br, &[]).comparability, C::Incomparable);
    assert_eq!(
        compared(&ar, &br, &[Variable::Hardware]).comparability,
        C::ComparableWithCaveats
    );
    let mut br = loaded(&b, "b");
    br.environment.build_profile = "synthetic-debug".into();
    assert_eq!(compared(&ar, &br, &[]).comparability, C::Incomparable);
    let mut br = loaded(&b, "b");
    br.environment.git_dirty = Some(true);
    assert_eq!(
        compared(&ar, &br, &[]).comparability,
        C::ComparableWithCaveats
    );
    let mut br = loaded(&b, "b");
    for r in br.results.values_mut() {
        r.execution.as_mut().unwrap().process.peak_rss_bytes = None;
    }
    assert_eq!(
        compared(&ar, &br, &[]).comparability,
        C::ComparableWithCaveats
    );
    let mut br = loaded(&b, "b");
    for r in br.results.values_mut() {
        r.execution.as_mut().unwrap().measurement_source = "synthetic-different-boundary".into();
    }
    assert_eq!(compared(&ar, &br, &[]).comparability, C::Incomparable);
}
#[test]
fn intentional_workers_resources_scheduler_and_integrity() {
    let a = synthetic("a", &[(1, &[10., 10.])]);
    let b = synthetic("b", &[(4, &[4., 4.])]);
    let ar = loaded(&a, "a");
    let br = loaded(&b, "b");
    assert_eq!(compared(&ar, &br, &[]).comparability, C::Incomparable);
    assert_eq!(
        compared(&ar, &br, &[Variable::Workers]).comparability,
        C::Comparable
    );
    let b = synthetic("b", &[(1, &[8., 8.])]);
    for (variable, change) in [
        (Variable::MemoryBudget, 0),
        (Variable::ScratchBudget, 1),
        (Variable::Parquet, 2),
        (Variable::Scheduler, 3),
        (Variable::IntegrityPolicy, 4),
    ] {
        let mut br = loaded(&b, "b");
        for r in br.results.values_mut() {
            match change {
                0 => r.trial.config.resources.memory_budget_bytes *= 2,
                1 => r.trial.config.resources.scratch_budget_bytes *= 2,
                2 => r.trial.config.resources.parquet.file_target_bytes *= 2,
                3 => r.trial.scheduler = Scheduler::PerJob,
                _ => r.trial.config.integrity_policy.enabled = false,
            }
        }
        for t in &mut br.run.plan.trials {
            match change {
                0 => t.config.resources.memory_budget_bytes *= 2,
                1 => t.config.resources.scratch_budget_bytes *= 2,
                2 => t.config.resources.parquet.file_target_bytes *= 2,
                3 => t.scheduler = Scheduler::PerJob,
                _ => t.config.integrity_policy.enabled = false,
            }
        }
        assert_eq!(compared(&ar, &br, &[]).comparability, C::Incomparable);
        assert_ne!(
            compared(&ar, &br, &[variable]).comparability,
            C::Incomparable
        );
    }
}
#[test]
fn missing_and_interrupted_results_keep_completed_samples() {
    let t = synthetic("partial", &[(1, &[10., 20., 30.])]);
    let path = t.path().join("partial");
    let mut run: RunMetadata =
        serde_json::from_slice(&fs::read(path.join("run.json")).unwrap()).unwrap();
    run.state = State::Running;
    fs::write(path.join("run.json"), serde_json::to_vec(&run).unwrap()).unwrap();
    let mut results: Vec<TrialResult> = fs::read_to_string(path.join("results.jsonl"))
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let missing = results.pop().unwrap();
    fs::remove_file(
        path.join("trials")
            .join(missing.trial.trial_id)
            .join("result.json"),
    )
    .unwrap();
    let mut interrupted = results.pop().unwrap();
    interrupted.state = State::Running;
    interrupted.ended_unix_ns = None;
    fs::write(
        path.join("trials")
            .join(&interrupted.trial.trial_id)
            .join("result.json"),
        serde_json::to_vec(&interrupted).unwrap(),
    )
    .unwrap();
    fs::write(
        path.join("results.jsonl"),
        format!("{}\n", serde_json::to_string(&results[0]).unwrap()),
    )
    .unwrap();
    let s = analysis::summarize(&loaded(&t, "partial"), 1).unwrap();
    assert_eq!(s.status, "incomplete");
    assert_eq!(s.accounting.measured_complete, 1);
    assert_eq!(s.accounting.measured_running, 1);
    assert_eq!(s.accounting.measured_interrupted, 0);
    assert_eq!(s.accounting.measured_missing, 1);
    assert_eq!(
        s.groups[0].metrics["execution_wall_seconds"]
            .statistics
            .as_ref()
            .unwrap()
            .sample_count,
        1
    );
    assert!(s.groups[0].scaling.speedup.is_none());
}
#[test]
fn duplicate_conflict_corruption_and_atomic_record_recovery() {
    let t = synthetic("records", &[(1, &[10., 10.])]);
    let p = t.path().join("records/results.jsonl");
    let original = fs::read_to_string(&p).unwrap();
    fs::write(
        &p,
        format!("{original}{}\n", original.lines().next().unwrap()),
    )
    .unwrap();
    let run = loaded(&t, "records");
    assert_eq!(run.results.len(), 2);
    let mut changed: TrialResult = serde_json::from_str(original.lines().next().unwrap()).unwrap();
    changed.execution.as_mut().unwrap().execution_wall_seconds = 99.;
    fs::write(
        &p,
        format!("{original}{}\n", serde_json::to_string(&changed).unwrap()),
    )
    .unwrap();
    assert!(
        analysis::load_run(p.parent().unwrap())
            .unwrap_err()
            .contains("conflicting duplicate")
    );
    fs::write(&p, format!("{original}{{")).unwrap();
    assert!(
        analysis::load_run(p.parent().unwrap())
            .unwrap_err()
            .contains("line 3")
    );
    fs::write(&p, format!("{}\n", original.lines().next().unwrap())).unwrap();
    let run = loaded(&t, "records");
    assert_eq!(run.results.len(), 2);
    assert!(
        run.diagnostics
            .iter()
            .any(|s| s.contains("recovered atomic"))
    );
}
#[test]
fn invalid_duration_is_rejected_with_trial_identity() {
    let t = synthetic("invalid", &[(1, &[10., 10.])]);
    mutate(&t, "invalid", |r| {
        r.execution.as_mut().unwrap().execution_wall_seconds = 0.
    });
    let error = analysis::load_run(&t.path().join("invalid")).unwrap_err();
    assert!(error.contains("trial") && error.contains("execution_wall_seconds"));
}
#[test]
fn deterministic_summary_reports_and_actual_nonexecuting_cli() {
    let t = synthetic("reports", &[(1, &[10., 20.])]);
    let root = t.path().join("reports");
    let before = fs::read(root.join("results.jsonl")).unwrap();
    let a = reports::summarize_path(&root, 1).unwrap();
    let first = fs::read(root.join("summary.json")).unwrap();
    let b = reports::summarize_path(&root, 1).unwrap();
    assert_eq!(first, fs::read(root.join("summary.json")).unwrap());
    assert_eq!(a.run_id, b.run_id);
    assert_eq!(before, fs::read(root.join("results.jsonl")).unwrap());
    let md = fs::read_to_string(root.join("summary.md")).unwrap();
    for word in [
        "Software",
        "Hardware",
        "Workload",
        "Worker scaling",
        "Correctness",
        "CPU",
        "Parquet",
        "Pooled",
    ] {
        assert!(md.contains(word), "missing {word}");
    }
    let out = Command::new(env!("CARGO_BIN_EXE_marketforge-normalize-benchmark"))
        .args(["summarize", "--run"])
        .arg(&root)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let c = synthetic("candidate", &[(1, &[8., 16.])]);
    let output = t.path().join("comparison");
    let out = Command::new(env!("CARGO_BIN_EXE_marketforge-normalize-benchmark"))
        .args(["compare", "--baseline"])
        .arg(&root)
        .arg("--candidate")
        .arg(c.path().join("candidate"))
        .arg("--output-root")
        .arg(&output)
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value: serde_json::Value =
        serde_json::from_slice(&fs::read(output.join("comparison.json")).unwrap()).unwrap();
    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["baseline_run_id"], "reports");
    assert_eq!(value["candidate_run_id"], "candidate");
    let md = fs::read_to_string(output.join("comparison.md")).unwrap();
    assert!(md.contains("Statistical variability") && md.contains("Conclusions"));
    assert!(
        reports::compare_paths(
            &root,
            &c.path().join("candidate"),
            &[],
            t.path(),
            Some(&output)
        )
        .is_err()
    );
    assert_eq!(before, fs::read(root.join("results.jsonl")).unwrap());
}
#[test]
fn failed_warmups_remain_visible_and_block_scaling() {
    let t = synthetic("warmup", &[(1, &[10., 10.])]);
    let mut run = loaded(&t, "warmup");
    let mut planned = run.run.plan.trials[0].clone();
    planned.trial_id = "synthetic-failed-warmup".into();
    planned.warmup = true;
    planned.order = 2;
    let mut result = run.results.values().next().unwrap().clone();
    result.trial = planned.clone();
    result.state = State::Failed;
    result.correctness_verified = false;
    result.error = Some("synthetic required warmup failure".into());
    run.results.insert(planned.trial_id.clone(), result);
    run.run.plan.trials.push(planned);
    let summary = analysis::summarize(&run, 1).unwrap();
    assert_eq!(summary.status, "incomplete");
    assert_eq!(summary.accounting.warmups_failed, 1);
    assert_eq!(summary.accounting.measured_complete, 2);
    assert!(summary.groups[0].scaling.speedup.is_none());
}
#[test]
fn optional_values_zero_baselines_and_noise_are_not_fabricated() {
    let a = synthetic("a", &[(1, &[96., 104., 100.])]);
    let b = synthetic("b", &[(1, &[97.5, 105.5, 101.5])]);
    let ar = loaded(&a, "a");
    let mut br = loaded(&b, "b");
    let report = compared(&ar, &br, &[]);
    let duration = report
        .metric_comparisons
        .iter()
        .find(|m| m.metric == "execution_wall_seconds")
        .unwrap();
    assert_eq!(duration.interpretation, "no established conclusion");
    for r in br.results.values_mut() {
        r.execution.as_mut().unwrap().process.physical_read_bytes = None;
    }
    let report = compared(&ar, &br, &[]);
    let metric = report
        .metric_comparisons
        .iter()
        .find(|m| m.metric == "physical_read_bytes")
        .unwrap();
    assert_eq!(metric.availability, "unavailable");
    assert!(metric.percentage_difference.is_none());
    let mut ar = loaded(&a, "a");
    for r in ar.results.values_mut() {
        r.execution.as_mut().unwrap().process.physical_read_bytes = Some(0);
    }
    for r in br.results.values_mut() {
        r.execution.as_mut().unwrap().process.physical_read_bytes = Some(10);
    }
    let report = compared(&ar, &br, &[]);
    let metric = report
        .metric_comparisons
        .iter()
        .find(|m| m.metric == "physical_read_bytes")
        .unwrap();
    assert_eq!(metric.absolute_difference, Some(10.));
    assert_eq!(metric.percentage_difference, None);
    assert_eq!(metric.ratio, None);
}
#[test]
fn report_paths_are_isolated_and_comparisons_unique() {
    let a = synthetic("a", &[(1, &[10., 10.])]);
    let b = synthetic("b", &[(1, &[8., 8.])]);
    let temp = tempfile::tempdir().unwrap();
    let first = reports::compare_paths(
        &a.path().join("a"),
        &b.path().join("b"),
        &[],
        temp.path(),
        None,
    )
    .unwrap();
    let second = reports::compare_paths(
        &a.path().join("a"),
        &b.path().join("b"),
        &[],
        temp.path(),
        None,
    )
    .unwrap();
    assert_ne!(first, second);
    assert!(first.join("comparison.json").is_file());
    assert!(
        reports::compare_paths(
            &a.path().join("a"),
            &b.path().join("b"),
            &[],
            temp.path(),
            Some(&temp.path().join("../escape"))
        )
        .is_err()
    );
    assert!(reports::summarize_path(&temp.path().join("missing"), 1).is_err());
}
#[cfg(unix)]
#[test]
fn derived_report_symlinks_do_not_overwrite_external_files() {
    let a = synthetic("a", &[(1, &[10., 10.])]);
    let root = a.path().join("a");
    let secret = a.path().join("outside.txt");
    fs::write(&secret, "preserve").unwrap();
    std::os::unix::fs::symlink(&secret, root.join("summary.md")).unwrap();
    reports::summarize_path(&root, 1).unwrap();
    assert_eq!(fs::read_to_string(&secret).unwrap(), "preserve");
    assert!(
        !fs::symlink_metadata(root.join("summary.md"))
            .unwrap()
            .file_type()
            .is_symlink()
    );
}
#[test]
fn structural_only_evidence_does_not_claim_semantic_equivalence() {
    let a = synthetic("a", &[(1, &[10., 10.])]);
    let b = synthetic("b", &[(1, &[8., 8.])]);
    let mut ar = loaded(&a, "a");
    let mut br = loaded(&b, "b");
    for r in ar.results.values_mut().chain(br.results.values_mut()) {
        r.output.as_mut().unwrap().semantic_stream_sha256.clear();
    }
    let report = compared(&ar, &br, &[]);
    assert_eq!(report.comparability, C::ComparableWithCaveats);
    assert!(!report.correctness_verification.equivalent);
    assert!(
        report
            .metric_comparisons
            .iter()
            .all(|m| m.interpretation == "no established conclusion")
    );
}
#[test]
fn mixed_source_hash_availability_uses_common_metadata_evidence() {
    let a = synthetic("a", &[(1, &[10., 10.])]);
    let b = synthetic("b", &[(1, &[8., 8.])]);
    let ar = loaded(&a, "a");
    let mut br = loaded(&b, "b");
    br.workload.sources[0].sha256 = None;
    let report = compared(&ar, &br, &[]);
    assert_eq!(report.comparability, C::ComparableWithCaveats);
    assert!(report.workload_verification.equivalent);
}
#[test]
fn unavailable_essential_measurement_and_wrong_workload_are_visible() {
    let a = synthetic("a", &[(1, &[10., 10.])]);
    let b = synthetic("b", &[(1, &[8., 8.])]);
    let ar = loaded(&a, "a");
    let mut br = loaded(&b, "b");
    for r in br.results.values_mut() {
        r.execution = None;
    }
    assert_eq!(compared(&ar, &br, &[]).comparability, C::Incomparable);
    let root = b.path().join("b");
    let mut run: RunMetadata =
        serde_json::from_slice(&fs::read(root.join("run.json")).unwrap()).unwrap();
    run.plan.trials[0].jobs[0].tasks[0].symbol = "wrong-symbol".into();
    fs::write(root.join("run.json"), serde_json::to_vec(&run).unwrap()).unwrap();
    assert!(
        analysis::load_run(&root)
            .unwrap_err()
            .contains("workload differs")
    );
}
#[test]
fn raw_versions_and_numeric_inventory_mismatches_fail_clearly() {
    let a = synthetic("a", &[(1, &[10., 10.])]);
    mutate(&a, "a", |r| r.output.as_mut().unwrap().parquet_bytes += 1);
    let error = analysis::load_run(&a.path().join("a")).unwrap_err();
    assert!(error.contains("trial") && error.contains("inventory"));
    let b = synthetic("b", &[(1, &[8., 8.])]);
    mutate(&b, "b", |r| r.schema_version = 99);
    assert!(
        analysis::load_run(&b.path().join("b"))
            .unwrap_err()
            .contains("unsupported")
    );
}
#[test]
fn per_content_and_stream_rates_are_separate_from_aggregate_semantics() {
    let a = synthetic("a", &[(1, &[10., 10.])]);
    let s = analysis::summarize(&loaded(&a, "a"), 1).unwrap();
    let g = &s.groups[0];
    assert_eq!(
        analysis::median(g, "content_type.trades.source_records_per_second"),
        Some(100.)
    );
    assert_eq!(
        analysis::median(g, "content_type.trades.canonical_rows_per_second"),
        Some(200.)
    );
    let source = g
        .metrics
        .iter()
        .find(|(name, _)| {
            name.starts_with("stream.") && name.ends_with("source_records_per_second")
        })
        .unwrap()
        .1;
    assert_eq!(source.unit, "source records/s");
    assert_eq!(source.statistics.as_ref().unwrap().median, 100.);
}
#[test]
fn allocated_job_and_dataset_ids_do_not_change_source_proof_equivalence() {
    let a = synthetic("a", &[(1, &[10., 10.])]);
    let b = synthetic("b", &[(1, &[8., 8.])]);
    let mut ar = loaded(&a, "a");
    let mut br = loaded(&b, "b");
    for r in ar.results.values_mut() {
        let key = format!(
            "{}.{}",
            r.trial.jobs[0].job_id.0, r.trial.jobs[0].tasks[0].task_id.0
        );
        r.output
            .as_mut()
            .unwrap()
            .depth_events_sha256
            .insert(key.clone(), "c".repeat(64));
        r.output
            .as_mut()
            .unwrap()
            .reconstruction_sha256
            .insert(key, "d".repeat(64));
    }
    for j in &mut br.workload.resolved_jobs {
        j.job_id.0 += 100;
        j.dataset_id.0 += 100;
    }
    for t in &mut br.run.plan.trials {
        for j in &mut t.jobs {
            j.job_id.0 += 100;
            j.dataset_id.0 += 100;
        }
    }
    for r in br.results.values_mut() {
        for j in &mut r.trial.jobs {
            j.job_id.0 += 100;
            j.dataset_id.0 += 100;
        }
        for j in &mut r.execution.as_mut().unwrap().response.jobs {
            j.job_id.0 += 100;
            j.dataset_id.0 += 100;
        }
        for f in &mut r.output.as_mut().unwrap().files {
            f.job_id += 100;
            f.dataset_id += 100;
        }
        let key = format!(
            "{}.{}",
            r.trial.jobs[0].job_id.0, r.trial.jobs[0].tasks[0].task_id.0
        );
        r.output
            .as_mut()
            .unwrap()
            .depth_events_sha256
            .insert(key.clone(), "c".repeat(64));
        r.output
            .as_mut()
            .unwrap()
            .reconstruction_sha256
            .insert(key, "d".repeat(64));
    }
    assert_eq!(compared(&ar, &br, &[]).comparability, C::Comparable);
}
