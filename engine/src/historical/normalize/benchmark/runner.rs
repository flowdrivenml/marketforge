use super::{Result, measure, model::*, plan, provenance, verification};
use crate::historical::normalize::{
    ProcessingStatus, execute_processing_job,
    response::{NormalizationError, NormalizationResponse},
    session::execute_processing_session_reporting,
};
use serde::Serialize;
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{Instant, SystemTime, UNIX_EPOCH},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
pub fn now() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .to_string()
}
pub fn reject_symlink_ancestors(path: &Path) -> Result<()> {
    if !path.is_absolute()
        || path
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Err("benchmark paths must be absolute without parent traversal".into());
    }
    let mut at = PathBuf::new();
    for component in path.components() {
        at.push(component);
        if fs::symlink_metadata(&at).is_ok_and(|m| m.file_type().is_symlink()) {
            return Err(format!("benchmark path contains symlink: {}", at.display()));
        }
    }
    Ok(())
}
pub fn owned(root: &Path, path: &Path) -> Result<()> {
    if path == root || !path.starts_with(root) {
        return Err("benchmark artifact escapes trial ownership".into());
    }
    reject_symlink_ancestors(path)
}
pub fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let next = path.with_extension("json.next");
    let bytes = serde_json::to_vec_pretty(value).map_err(|e| e.to_string())?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&next)
        .map_err(|e| e.to_string())?;
    file.write_all(&bytes)
        .and_then(|_| file.sync_all())
        .map_err(|e| e.to_string())?;
    fs::rename(&next, path).map_err(|e| e.to_string())
}
pub fn allocate_run(requested: Option<&Path>, root: &Path) -> Result<PathBuf> {
    if let Some(path) = requested {
        reject_symlink_ancestors(path)?;
        let parent = path.parent().ok_or("run output has no parent")?;
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        fs::create_dir(path)
            .map_err(|e| format!("exclusive benchmark run {}: {e}", path.display()))?;
        return path.canonicalize().map_err(|e| e.to_string());
    }
    let base = root.join("data/benchmarks/historical/normalize/runs");
    reject_symlink_ancestors(&base)?;
    fs::create_dir_all(&base).map_err(|e| e.to_string())?;
    loop {
        let path = base.join(format!(
            "{}-{}-{}",
            now(),
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        match fs::create_dir(&path) {
            Ok(()) => return path.canonicalize().map_err(|e| e.to_string()),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e.to_string()),
        }
    }
}
#[derive(Default)]
pub struct SubprocessExecutor;
impl TrialExecutor for SubprocessExecutor {
    fn execute(&mut self, t: &TrialPlan) -> Result<ChildExecution> {
        let stdout = fs::File::create(t.directory.join("stdout.log")).map_err(|e| e.to_string())?;
        let stderr = fs::File::create(t.directory.join("stderr.log")).map_err(|e| e.to_string())?;
        let status = Command::new(std::env::current_exe().map_err(|e| e.to_string())?)
            .arg("--execute-trial")
            .arg(t.directory.join("request.json"))
            .stdin(Stdio::null())
            .stdout(stdout)
            .stderr(stderr)
            .status()
            .map_err(|e| e.to_string())?;
        let result: ChildExecution =
            serde_json::from_slice(&fs::read(t.directory.join("execution.json")).map_err(|e| {
                format!("trial process exited {status}; missing execution report: {e}")
            })?)
            .map_err(|e| e.to_string())?;
        if !status.success() && result.response.status == ProcessingStatus::Complete {
            return Err("trial subprocess failed despite complete response".into());
        }
        Ok(result)
    }
}
pub fn child(request: &Path) -> Result<bool> {
    let trial: TrialPlan = serde_json::from_slice(&fs::read(request).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    if request != trial.directory.join("request.json") {
        return Err("trial request path mismatch".into());
    }
    verify_owner(&trial)?;
    for job in &trial.jobs {
        owned(&trial.directory, &job.output.dataset_path)?;
        owned(&trial.directory, &job.output.staging_path)?;
    }
    owned(&trial.directory, &trial.config.resources.scratch_path)?;
    let before = measure::snapshot();
    let start = Instant::now();
    let mut results = vec![];
    let mut times = vec![];
    let mut error = None;
    match trial.scheduler {
        Scheduler::Global => {
            let result =
                execute_processing_session_reporting(&trial.jobs, &trial.config, &mut |r| {
                    results.push(r.clone())
                });
            if let Err(e) = result {
                error = Some(e.to_string());
            }
            times.resize(results.len(), None);
        }
        Scheduler::PerJob => {
            for job in &trial.jobs {
                let clock = Instant::now();
                match execute_processing_job(job, &trial.config) {
                    Ok(r) => {
                        results.push(r);
                        times.push(Some(clock.elapsed().as_secs_f64()));
                    }
                    Err(e) => {
                        error = Some(e.to_string());
                        break;
                    }
                }
            }
        }
    }
    let execution_wall_seconds = start.elapsed().as_secs_f64();
    let after = measure::snapshot();
    let response = NormalizationResponse::new(
        None,
        trial.jobs.len(),
        results,
        error.map(|message| NormalizationError {
            context: None,
            code: "execution_error".into(),
            message,
        }),
    );
    let success = response.status == ProcessingStatus::Complete;
    let result = ChildExecution {
        schema_version: 1,
        measurement_source: "isolated-production-process".into(),
        response,
        execution_wall_seconds,
        per_job_wall_seconds: times,
        process: measure::difference(&before, &after),
    };
    atomic_json(&trial.directory.join("execution.json"), &result)?;
    println!(
        "{}",
        serde_json::to_string(&result.response).map_err(|e| e.to_string())?
    );
    Ok(success)
}
fn verify_owner(t: &TrialPlan) -> Result<()> {
    reject_symlink_ancestors(&t.directory)?;
    if fs::read_to_string(t.directory.join(".trial-owner")).map_err(|e| e.to_string())?
        != t.trial_id
    {
        return Err("trial ownership marker mismatch".into());
    }
    Ok(())
}
pub fn cleanup_success(t: &TrialPlan) -> Result<()> {
    verify_owner(t)?;
    for job in &t.jobs {
        let path = &job.output.dataset_path;
        let expected = t
            .directory
            .join("datasets")
            .join(job.dataset_id.0.to_string());
        if *path != expected {
            return Err("cleanup dataset is not the assigned trial dataset".into());
        }
        owned(&t.directory, path)?;
        reject_tree_links(path)?;
    }
    for job in &t.jobs {
        fs::remove_dir_all(&job.output.dataset_path).map_err(|e| e.to_string())?;
    }
    Ok(())
}
fn reject_tree_links(path: &Path) -> Result<()> {
    let m = fs::symlink_metadata(path).map_err(|e| e.to_string())?;
    if m.file_type().is_symlink() {
        return Err("cleanup refuses symlink".into());
    }
    if m.is_dir() {
        for e in fs::read_dir(path).map_err(|e| e.to_string())? {
            reject_tree_links(&e.map_err(|e| e.to_string())?.path())?;
        }
    }
    Ok(())
}
fn preserve_metadata(t: &TrialPlan) -> Result<()> {
    for job in &t.jobs {
        let dest = t
            .directory
            .join("artifacts")
            .join(job.dataset_id.0.to_string());
        for base in [&job.output.dataset_path, &job.output.staging_path] {
            if !base.is_dir() {
                continue;
            }
            for name in [
                "manifest.json",
                "processing-job.json",
                "processing-job.sha256",
                "failure.json",
            ] {
                let source = base.join(name);
                if source.is_file() {
                    fs::create_dir_all(&dest).map_err(|e| e.to_string())?;
                    fs::copy(&source, dest.join(name)).map_err(|e| e.to_string())?;
                }
            }
            if base == &job.output.dataset_path {
                let aux = crate::historical::normalize::manifest::inspect_auxiliary_files(base)
                    .map_err(|e| e.to_string())?;
                for file in aux {
                    let rel = verification::safe_relative(&file.path)?;
                    let target = dest.join(rel);
                    fs::create_dir_all(target.parent().unwrap()).map_err(|e| e.to_string())?;
                    fs::copy(base.join(rel), target).map_err(|e| e.to_string())?;
                }
            }
        }
    }
    Ok(())
}
pub fn execute_plan(mut plan: Plan, requested: Option<&Path>) -> Result<PathBuf> {
    let directory = allocate_run(requested, &plan.project_root)?;
    plan::rebase(&mut plan, &directory)?;
    let mut executor = SubprocessExecutor;
    run_with(plan, &mut executor, verification::verify)?;
    Ok(directory)
}
pub fn run_with(
    mut plan: Plan,
    executor: &mut impl TrialExecutor,
    mut verify: impl FnMut(&TrialPlan, &ChildExecution) -> Result<OutputObservation>,
) -> Result<RunMetadata> {
    let directory = plan.directory.clone();
    if provenance::file_sha256(&plan.input)? != plan.input_sha256 {
        return Err("prepared input changed after planning".into());
    }
    reject_symlink_ancestors(&directory)?;
    if fs::read_dir(&directory)
        .map_err(|e| e.to_string())?
        .next()
        .is_some()
    {
        return Err("benchmark run directory is not empty; never overwrite history".into());
    }
    if !directory.is_dir() {
        return Err("run directory must be exclusively allocated first".into());
    }
    let run_id = directory
        .file_name()
        .ok_or("run identity missing")?
        .to_string_lossy()
        .to_string();
    let sources = provenance::sources(&plan);
    let input_bytes = sources
        .iter()
        .try_fold(0u64, |sum, s| sum.checked_add(s.size_bytes?));
    let mut run = RunMetadata {
        schema_version: 1,
        kind: "normalization_benchmark_run".into(),
        run_id: run_id.clone(),
        state: State::Running,
        created_unix_ns: now(),
        finished_unix_ns: None,
        plan: plan.clone(),
        trials_complete: 0,
        trials_failed: 0,
        cache_policy:
            "ordinary operating-system cache behavior; warmups recorded; no cold-cache claim".into(),
    };
    atomic_json(&directory.join("run.json"), &run)?;
    atomic_json(
        &directory.join("environment.json"),
        &provenance::environment(&plan.project_root),
    )?;
    atomic_json(
        &directory.join("workload.json"),
        &serde_json::json!({"schema_version":1,"input":plan.input,"input_sha256":plan.input_sha256,"prepared_batch":serde_json::from_slice::<serde_json::Value>(&fs::read(&plan.input).map_err(|e|e.to_string())?).map_err(|e|e.to_string())?,"resolved_jobs":plan.trials[0].jobs,"sources":sources}),
    )?;
    fs::create_dir(directory.join("trials")).map_err(|e| e.to_string())?;
    let results_path = directory.join("results.jsonl");
    let mut results = OpenOptions::new()
        .create_new(true)
        .append(true)
        .open(results_path)
        .map_err(|e| e.to_string())?;
    for t in plan.trials.drain(..) {
        eprintln!(
            "Benchmark {run_id}: trial {} (warmup={})",
            t.trial_id, t.warmup
        );
        fs::create_dir(&t.directory).map_err(|e| e.to_string())?;
        fs::write(t.directory.join(".trial-owner"), &t.trial_id).map_err(|e| e.to_string())?;
        atomic_json(&t.directory.join("request.json"), &t)?;
        #[derive(Serialize)]
        struct Runtime<'a> {
            schema_version: u32,
            #[serde(flatten)]
            config: &'a crate::shared::historical::ProcessingConfig,
        }
        fs::write(
            t.directory.join("effective-config.toml"),
            toml::to_string(&Runtime {
                schema_version: 1,
                config: &t.config,
            })
            .map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        let mut result = TrialResult {
            schema_version: 1,
            kind: "normalization_benchmark_trial".into(),
            benchmark_run_id: run_id.clone(),
            trial: t.clone(),
            state: State::Running,
            started_unix_ns: now(),
            ended_unix_ns: None,
            execution: None,
            trial_wall_seconds: None,
            verification_wall_seconds: None,
            output: None,
            source_records_per_second: None,
            canonical_rows_per_second: None,
            input_bytes_per_second: None,
            output_bytes_per_second: None,
            source_archive_count: sources.len() as u64,
            source_file_bytes: input_bytes,
            uncompressed_input_bytes: None,
            correctness_verified: false,
            dataset_retained: true,
            error: None,
        };
        atomic_json(&t.directory.join("result.json"), &result)?;
        let clock = Instant::now();
        let outcome = (|| {
            if provenance::sources(&run.plan) != sources {
                return Err("source metadata changed after planning".into());
            }
            let execution = executor.execute(&t)?;
            if execution.schema_version != 1 {
                return Err("unsupported execution schema version".into());
            }
            result.execution = Some(execution.clone());
            if execution.response.status != ProcessingStatus::Complete {
                return Err("normalization execution failed; preserved structured response".into());
            }
            if provenance::sources(&run.plan) != sources {
                return Err("source metadata changed during trial".into());
            }
            if !execution.execution_wall_seconds.is_finite()
                || execution.execution_wall_seconds <= 0.0
            {
                return Err("execution duration unavailable or invalid".into());
            }
            let reporter = crate::historical::normalize::progress::ProgressReporter::disabled();
            let diagnostics =
                crate::historical::normalize::progress::diagnostics::Diagnostics::start(
                    &t.config,
                    &t.directory,
                    &reporter,
                );
            for job in &t.jobs {
                reporter.record_workload(job);
                reporter.stream_phase(job.job_id.0, "", "benchmark output verification");
            }
            let start = Instant::now();
            let output = verify(&t, &execution)?;
            for job in &t.jobs {
                reporter.measured_wall_time(
                    crate::historical::normalize::progress::ProgressTaskId::new(job.job_id.0, 0),
                    "benchmark output verification",
                    start.elapsed(),
                );
            }
            if let Some(diagnostics) = diagnostics {
                diagnostics.finish(true);
            }
            result.verification_wall_seconds = Some(start.elapsed().as_secs_f64());
            result.output = Some(output.clone());
            result.correctness_verified = true;
            atomic_json(&t.directory.join("parquet-files.json"), &output.files)?;
            preserve_metadata(&t)?;
            if plan.retention == Retention::Metadata {
                cleanup_success(&t)?;
                result.dataset_retained = false;
            }
            Ok(())
        })();
        result.state = if outcome.is_ok() {
            State::Complete
        } else {
            State::Failed
        };
        result.error = outcome.err();
        if result.state == State::Complete {
            let seconds = result.execution.as_ref().unwrap().execution_wall_seconds;
            let output = result.output.as_ref().unwrap();
            result.source_records_per_second =
                measure::rate(output.source_records_read, seconds, true);
            result.canonical_rows_per_second = measure::rate(output.canonical_rows, seconds, true);
            result.input_bytes_per_second =
                input_bytes.and_then(|b| measure::rate(b, seconds, true));
            result.output_bytes_per_second = measure::rate(output.parquet_bytes, seconds, true);
            run.trials_complete += 1;
        } else {
            if let Err(error) = preserve_metadata(&t) {
                let message = result.error.get_or_insert_with(String::new);
                message.push_str(&format!("; metadata preservation failed: {error}"));
            }
            run.trials_failed += 1;
        }
        result.trial_wall_seconds = Some(clock.elapsed().as_secs_f64());
        result.ended_unix_ns = Some(now());
        atomic_json(&t.directory.join("result.json"), &result)?;
        serde_json::to_writer(&mut results, &result).map_err(|e| e.to_string())?;
        results
            .write_all(b"\n")
            .and_then(|_| results.sync_data())
            .map_err(|e| e.to_string())?;
        atomic_json(&directory.join("run.json"), &run)?;
    }
    run.state = if run.trials_failed == 0 {
        State::Complete
    } else {
        State::Failed
    };
    run.finished_unix_ns = Some(now());
    atomic_json(&directory.join("run.json"), &run)?;
    Ok(run)
}
