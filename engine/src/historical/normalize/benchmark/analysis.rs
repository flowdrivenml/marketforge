use super::{
    Result,
    model::*,
    provenance::{Environment, SourceObservation},
    statistics::{Statistics, finite_ratio, statistics},
};
use crate::{
    historical::{config::PreparedBatch, job::ProcessingJob, normalize::ProcessingStatus},
    shared::historical::ProcessingConfig,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
};
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadMetadata {
    pub schema_version: u32,
    pub input: PathBuf,
    pub input_sha256: String,
    pub prepared_batch: PreparedBatch,
    pub resolved_jobs: Vec<ProcessingJob>,
    pub sources: Vec<SourceObservation>,
}
#[derive(Debug)]
pub struct LoadedRun {
    pub path: PathBuf,
    pub run: RunMetadata,
    pub environment: Environment,
    pub workload: WorkloadMetadata,
    pub results: BTreeMap<String, TrialResult>,
    pub diagnostics: Vec<String>,
}
fn read<T: serde::de::DeserializeOwned>(p: &Path) -> Result<T> {
    serde_json::from_slice(&fs::read(p).map_err(|e| format!("{}: {e}", p.display()))?)
        .map_err(|e| format!("{}: {e}", p.display()))
}
fn same(a: &impl Serialize, b: &impl Serialize) -> Result<bool> {
    Ok(serde_json::to_value(a).map_err(|e| e.to_string())?
        == serde_json::to_value(b).map_err(|e| e.to_string())?)
}
fn insert(
    results: &mut BTreeMap<String, TrialResult>,
    record: TrialResult,
    artifact: &Path,
    diagnostics: &mut Vec<String>,
) -> Result<()> {
    record.validate_version().map_err(|e| {
        format!(
            "{} trial {}: {e}",
            artifact.display(),
            record.trial.trial_id
        )
    })?;
    if let Some(prior) = results.get(&record.trial.trial_id) {
        if !same(prior, &record)? {
            return Err(format!(
                "{}: conflicting duplicate trial {}",
                artifact.display(),
                record.trial.trial_id
            ));
        }
        if artifact
            .file_name()
            .is_some_and(|name| name == "results.jsonl")
        {
            diagnostics.push(format!(
                "deduplicated identical trial {} from {}",
                record.trial.trial_id,
                artifact.display()
            ));
        }
    } else {
        results.insert(record.trial.trial_id.clone(), record);
    }
    Ok(())
}
pub fn load_run(path: &Path) -> Result<LoadedRun> {
    let path = path
        .canonicalize()
        .map_err(|e| format!("benchmark run {}: {e}", path.display()))?;
    let run: RunMetadata = read(&path.join("run.json"))?;
    run.validate_version()?;
    if run.plan.job_count == 0 || run.plan.task_count == 0 || run.plan.trials.is_empty() {
        return Err("empty persisted benchmark experiment".into());
    }
    let environment: Environment = read(&path.join("environment.json"))?;
    let workload: WorkloadMetadata = read(&path.join("workload.json"))?;
    if environment.schema_version != 1
        || workload.schema_version != 1
        || workload.prepared_batch.schema_version != 4
    {
        return Err("unsupported environment/workload schema".into());
    }
    let mut unique_sources = BTreeSet::new();
    for source in &workload.sources {
        if !unique_sources.insert(&source.path) {
            return Err("duplicate/ambiguous source provenance path".into());
        }
        if source
            .sha256
            .as_ref()
            .is_some_and(|hash| hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            return Err(format!(
                "invalid source SHA256 for {}",
                source.path.display()
            ));
        }
    }
    let mut results = BTreeMap::new();
    let mut diagnostics = vec![];
    let jsonl = path.join("results.jsonl");
    if jsonl.exists() {
        let file = fs::File::open(&jsonl).map_err(|e| format!("{}: {e}", jsonl.display()))?;
        for (line_no, line) in BufReader::new(file).lines().enumerate() {
            let line =
                line.map_err(|e| format!("{} line {}: {e}", jsonl.display(), line_no + 1))?;
            let record: TrialResult = serde_json::from_str(&line)
                .map_err(|e| format!("{} line {}: {e}", jsonl.display(), line_no + 1))?;
            if !matches!(record.state, State::Complete | State::Failed) {
                return Err(format!(
                    "{}: nondefinitive appended trial {}",
                    jsonl.display(),
                    record.trial.trial_id
                ));
            }
            insert(&mut results, record, &jsonl, &mut diagnostics)?;
        }
    }
    let mut planned = BTreeSet::new();
    for trial in &run.plan.trials {
        if trial.trial_id.is_empty()
            || !trial
                .trial_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            || !planned.insert(trial.trial_id.clone())
        {
            return Err(format!(
                "invalid/duplicate planned trial {}",
                trial.trial_id
            ));
        }
        let artifact = path
            .join("trials")
            .join(&trial.trial_id)
            .join("result.json");
        if artifact.exists() {
            super::runner::reject_symlink_ancestors(&artifact)?;
            let record: TrialResult = read(&artifact)?;
            if record.trial.trial_id != trial.trial_id {
                return Err(format!("{}: trial identity mismatch", artifact.display()));
            }
            if !results.contains_key(&trial.trial_id)
                && matches!(record.state, State::Complete | State::Failed)
            {
                diagnostics.push(format!(
                    "recovered atomic result for {} absent from JSONL",
                    trial.trial_id
                ));
            }
            insert(&mut results, record, &artifact, &mut diagnostics)?;
        }
    }
    for trial in &run.plan.trials {
        if workload_description(&trial.jobs) != workload_description(&workload.resolved_jobs) {
            return Err(format!(
                "trial {}: workload differs from persisted selected authority",
                trial.trial_id
            ));
        }
    }
    for (id, result) in &results {
        if !planned.contains(id) {
            return Err(format!("unplanned trial {id}"));
        }
        if result.benchmark_run_id != run.run_id {
            return Err(format!("trial {id}: benchmark run identity mismatch"));
        }
        let expected = run.plan.trials.iter().find(|t| t.trial_id == *id).unwrap();
        if configuration(expected, "schema1") == configuration(&result.trial, "schema1")
            && expected.warmup == result.trial.warmup
            && expected.repetition == result.trial.repetition
            && expected.order == result.trial.order
            && same(&expected.jobs, &result.trial.jobs)?
        {
        } else {
            return Err(format!("trial {id}: configuration differs from plan"));
        }
        validate_numbers(result)?;
    }
    Ok(LoadedRun {
        path,
        run,
        environment,
        workload,
        results,
        diagnostics,
    })
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupConfiguration {
    pub workload_identity: String,
    pub scheduler: Scheduler,
    pub workers: usize,
    pub config: ProcessingConfig,
    pub measurement_definition: String,
    pub measurement_source: String,
}
fn workload_description(jobs: &[ProcessingJob]) -> Vec<Value> {
    jobs.iter().map(|j|serde_json::json!({"job_id":j.job_id,"dataset_id":j.dataset_id,"content_type":j.output.content_type,"tasks":j.tasks,"streams":j.streams,"ordering":j.ordering})).collect()
}
pub fn configuration(t: &TrialPlan, source: &str) -> Value {
    let mut config = t.config.clone();
    config.resources.scratch_path = PathBuf::from("<trial-owned-scratch>");
    serde_json::to_value(GroupConfiguration{workload_identity:hash(&workload_description(&t.jobs)),scheduler:t.scheduler,workers:t.workers,config,measurement_definition:"schema1:complete-workload/child-execution;parent-verification-separate;fresh-child-rss;proc-io".into(),measurement_source:source.into()}).unwrap()
}
pub fn hash(value: &impl Serialize) -> String {
    super::provenance::hex(Sha256::digest(
        serde_json::to_vec(value).expect("serializable benchmark identity"),
    ))
}
fn validate_numbers(r: &TrialResult) -> Result<()> {
    let id = &r.trial.trial_id;
    if r.trial.workers == 0 || r.trial.workers != r.trial.config.resources.workers {
        return Err(format!("trial {id}: worker configuration mismatch"));
    }
    let check = |name: &str, v: Option<f64>, positive: bool| -> Result<()> {
        if v.is_some_and(|v| !v.is_finite() || v < 0. || (positive && v == 0.)) {
            return Err(format!("trial {id}: invalid {name}"));
        }
        Ok(())
    };
    check("trial_wall_seconds", r.trial_wall_seconds, false)?;
    check(
        "verification_wall_seconds",
        r.verification_wall_seconds,
        false,
    )?;
    for (name, v) in [
        ("source_records_per_second", r.source_records_per_second),
        ("canonical_rows_per_second", r.canonical_rows_per_second),
        ("input_bytes_per_second", r.input_bytes_per_second),
        ("output_bytes_per_second", r.output_bytes_per_second),
    ] {
        check(name, v, false)?;
    }
    if let Some(e) = &r.execution {
        if e.schema_version != 1 {
            return Err(format!("trial {id}: unsupported child execution schema"));
        }
        check(
            "execution_wall_seconds",
            Some(e.execution_wall_seconds),
            r.state == State::Complete,
        )?;
        for value in &e.per_job_wall_seconds {
            check("per_job_wall_seconds", *value, false)?;
        }
        for (name, value) in [
            ("CPU user", e.process.process_cpu_user_seconds),
            ("CPU system", e.process.process_cpu_system_seconds),
            ("CPU total", e.process.process_cpu_total_seconds),
        ] {
            check(name, value, false)?;
        }
    }
    if let Some(output) = &r.output {
        let sum = |values: Vec<u64>| {
            values.into_iter().try_fold(0u64, |n, v| {
                n.checked_add(v)
                    .ok_or_else(|| format!("trial {id}: output counter overflow"))
            })
        };
        if output.files.len() as u64 != output.parquet_file_count
            || sum(output.files.iter().map(|f| f.rows).collect())? != output.canonical_rows
            || sum(output.files.iter().map(|f| f.size_bytes).collect())? != output.parquet_bytes
            || sum(output.files.iter().map(|f| f.row_groups).collect())?
                != output.parquet_row_group_count
        {
            return Err(format!("trial {id}: inconsistent Parquet inventory totals"));
        }
        for volumes in [&output.work_by_content_type, &output.work_by_stream] {
            if !volumes.is_empty()
                && (sum(volumes.values().map(|v| v.source_records_read).collect())?
                    != output.source_records_read
                    || sum(volumes.values().map(|v| v.canonical_rows).collect())?
                        != output.canonical_rows)
            {
                return Err(format!(
                    "trial {id}: inconsistent content/stream work-volume totals"
                ));
            }
        }
        if let Some(execution) = r
            .execution
            .as_ref()
            .filter(|e| e.response.status == ProcessingStatus::Complete)
            && (sum(execution
                .response
                .jobs
                .iter()
                .map(|j| j.events_written)
                .collect())?
                != output.canonical_rows
                || sum(execution
                    .response
                    .jobs
                    .iter()
                    .map(|j| j.files_written)
                    .collect())?
                    != output.parquet_file_count)
        {
            return Err(format!(
                "trial {id}: execution/output row or file totals differ"
            ));
        }
    }
    Ok(())
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Accounting {
    pub measured_planned: usize,
    pub measured_complete: usize,
    pub measured_failed: usize,
    pub measured_interrupted: usize,
    pub measured_running: usize,
    pub measured_missing: usize,
    pub warmups_planned: usize,
    pub warmups_complete: usize,
    pub warmups_failed: usize,
    pub warmups_interrupted: usize,
    pub warmups_running: usize,
    pub warmups_missing: usize,
    pub correctness_failed: usize,
}
impl Accounting {
    fn add(&mut self, warmup: bool, result: Option<&TrialResult>) {
        let complete = result.is_some_and(eligible);
        let failed = result.is_some_and(|r| {
            r.state == State::Failed || (r.state == State::Complete && !eligible(r))
        });
        let interrupted = result.is_some_and(|r| r.state == State::Interrupted);
        let running = result.is_some_and(|r| r.state == State::Running);
        if warmup {
            self.warmups_planned += 1;
            if complete {
                self.warmups_complete += 1;
            } else if failed {
                self.warmups_failed += 1;
            } else if running {
                self.warmups_running += 1;
            } else if interrupted {
                self.warmups_interrupted += 1;
            } else {
                self.warmups_missing += 1;
            }
        } else {
            self.measured_planned += 1;
            if complete {
                self.measured_complete += 1;
            } else if failed {
                self.measured_failed += 1;
            } else if running {
                self.measured_running += 1;
            } else if interrupted {
                self.measured_interrupted += 1;
            } else {
                self.measured_missing += 1;
            }
        }
        if result.is_some_and(|r| r.state == State::Complete && !r.correctness_verified) {
            self.correctness_failed += 1;
        }
    }
    pub fn complete(&self) -> bool {
        self.measured_planned > 0
            && self.measured_complete == self.measured_planned
            && self.warmups_complete == self.warmups_planned
    }
}
pub fn eligible(r: &TrialResult) -> bool {
    r.state == State::Complete
        && r.correctness_verified
        && r.output.is_some()
        && r.execution.as_ref().is_some_and(|e| {
            e.response.status == ProcessingStatus::Complete
                && e.response.error.is_none()
                && e.response.summary.jobs_total == r.trial.jobs.len()
                && e.response.summary.jobs_complete == r.trial.jobs.len()
                && e.response.summary.jobs_not_reported == 0
                && e.response.summary.jobs_failed == 0
                && e.response.jobs.len() == r.trial.jobs.len()
                && e.response
                    .jobs
                    .iter()
                    .zip(&r.trial.jobs)
                    .all(|(result, job)| {
                        result.job_id == job.job_id
                            && result.dataset_id == job.dataset_id
                            && result.status == ProcessingStatus::Complete
                            && result.manifest_path.is_some()
                            && result.failure.is_none()
                    })
        })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSummary {
    pub unit: String,
    pub statistics: Option<Statistics>,
    pub missing_samples: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scaling {
    pub baseline_workers: usize,
    pub speedup: Option<f64>,
    pub relative_parallel_efficiency: Option<f64>,
    pub source_throughput_ratio: Option<f64>,
    pub canonical_throughput_ratio: Option<f64>,
    pub limitation: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupSummary {
    pub group_id: String,
    pub configuration: GroupConfiguration,
    pub status: String,
    pub accounting: Accounting,
    pub metrics: BTreeMap<String, MetricSummary>,
    pub pooled_source_records_per_second: Option<f64>,
    pub pooled_canonical_rows_per_second: Option<f64>,
    pub scaling: Scaling,
    pub eligible_trial_ids: Vec<String>,
    pub correctness_consistent: bool,
    pub limitations: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Summary {
    pub schema_version: u32,
    pub kind: String,
    pub run_id: String,
    pub status: String,
    pub original_run_state: State,
    pub accounting: Accounting,
    pub groups: Vec<GroupSummary>,
    pub software: Value,
    pub hardware: Value,
    pub workload: Value,
    pub diagnostics: Vec<String>,
    pub measurement_limitations: Vec<String>,
}
const METRICS: &[&str] = &[
    "execution_wall_seconds",
    "trial_wall_seconds",
    "verification_wall_seconds",
    "source_records_per_second",
    "canonical_rows_per_second",
    "input_bytes_per_second",
    "output_bytes_per_second",
    "process_cpu_user_seconds",
    "process_cpu_system_seconds",
    "process_cpu_total_seconds",
    "process_cpu_utilization_percent",
    "peak_rss_bytes",
    "peak_accounted_memory_bytes",
    "logical_read_bytes",
    "logical_written_bytes",
    "physical_read_bytes",
    "physical_written_bytes",
    "parquet_file_count",
    "parquet_total_bytes",
    "parquet_total_rows",
    "parquet_row_group_count",
    "parquet_min_file_bytes",
    "parquet_max_file_bytes",
    "parquet_mean_file_bytes",
    "parquet_median_file_bytes",
    "files_per_million_canonical_rows",
];
fn values(r: &TrialResult) -> BTreeMap<String, Option<f64>> {
    let e = r.execution.as_ref().unwrap();
    let o = r.output.as_ref().unwrap();
    let p = &e.process;
    let sec = e.execution_wall_seconds;
    let sizes: Vec<_> = o.files.iter().map(|f| f.size_bytes as f64).collect();
    let distribution = statistics(&sizes).unwrap();
    let mut metrics: BTreeMap<String, Option<f64>> = BTreeMap::from([
        ("execution_wall_seconds", Some(sec)),
        ("trial_wall_seconds", r.trial_wall_seconds),
        ("verification_wall_seconds", r.verification_wall_seconds),
        (
            "source_records_per_second",
            finite_ratio(o.source_records_read as f64, sec),
        ),
        (
            "canonical_rows_per_second",
            finite_ratio(o.canonical_rows as f64, sec),
        ),
        ("input_bytes_per_second", r.input_bytes_per_second),
        (
            "output_bytes_per_second",
            finite_ratio(o.parquet_bytes as f64, sec),
        ),
        ("process_cpu_user_seconds", p.process_cpu_user_seconds),
        ("process_cpu_system_seconds", p.process_cpu_system_seconds),
        ("process_cpu_total_seconds", p.process_cpu_total_seconds),
        (
            "process_cpu_utilization_percent",
            p.process_cpu_total_seconds
                .and_then(|v| finite_ratio(v * 100., sec)),
        ),
        ("peak_rss_bytes", p.peak_rss_bytes.map(|v| v as f64)),
        (
            "peak_accounted_memory_bytes",
            p.peak_accounted_memory_bytes.map(|v| v as f64),
        ),
        ("logical_read_bytes", p.logical_read_bytes.map(|v| v as f64)),
        (
            "logical_written_bytes",
            p.logical_written_bytes.map(|v| v as f64),
        ),
        (
            "physical_read_bytes",
            p.physical_read_bytes.map(|v| v as f64),
        ),
        (
            "physical_written_bytes",
            p.physical_written_bytes.map(|v| v as f64),
        ),
        ("parquet_file_count", Some(o.parquet_file_count as f64)),
        ("parquet_total_bytes", Some(o.parquet_bytes as f64)),
        ("parquet_total_rows", Some(o.canonical_rows as f64)),
        (
            "parquet_row_group_count",
            Some(o.parquet_row_group_count as f64),
        ),
        (
            "parquet_min_file_bytes",
            distribution.as_ref().map(|s| s.minimum),
        ),
        (
            "parquet_max_file_bytes",
            distribution.as_ref().map(|s| s.maximum),
        ),
        (
            "parquet_mean_file_bytes",
            distribution.as_ref().map(|s| s.mean),
        ),
        (
            "parquet_median_file_bytes",
            distribution.as_ref().map(|s| s.median),
        ),
        (
            "files_per_million_canonical_rows",
            finite_ratio(o.parquet_file_count as f64 * 1e6, o.canonical_rows as f64),
        ),
    ])
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();
    for (prefix, volumes) in [
        ("content_type", &o.work_by_content_type),
        ("stream", &o.work_by_stream),
    ] {
        for (identity, volume) in volumes {
            metrics.insert(
                format!("{prefix}.{identity}.source_records_per_second"),
                finite_ratio(volume.source_records_read as f64, sec),
            );
            metrics.insert(
                format!("{prefix}.{identity}.canonical_rows_per_second"),
                finite_ratio(volume.canonical_rows as f64, sec),
            );
        }
    }
    metrics
}
fn unit(name: &str) -> String {
    if name.ends_with("per_second") {
        if name.ends_with("source_records_per_second") {
            "source records/s"
        } else if name.ends_with("canonical_rows_per_second") {
            "canonical rows/s"
        } else {
            "bytes/s"
        }
    } else if name.ends_with("_seconds") {
        "seconds"
    } else if name.contains("_bytes") {
        "bytes"
    } else if name.ends_with("percent") {
        "percent of one CPU"
    } else if name.starts_with("files_per_") {
        "files/million canonical rows"
    } else {
        "count"
    }
    .into()
}
pub fn summarize(run: &LoadedRun, baseline_workers: usize) -> Result<Summary> {
    let mut diagnostics = run.diagnostics.clone();
    for t in &run.run.plan.trials {
        match run.results.get(&t.trial_id) {
            None => diagnostics.push(format!("trial {}: missing planned result", t.trial_id)),
            Some(r) if !eligible(r) => diagnostics.push(format!(
                "trial {}: {:?}, correctness={}, diagnostic={:?}",
                t.trial_id, r.state, r.correctness_verified, r.error
            )),
            _ => {}
        }
    }
    let known_sources: BTreeSet<_> = run
        .results
        .values()
        .filter_map(|r| r.execution.as_ref().map(|e| e.measurement_source.as_str()))
        .collect();
    let inferred_source = if known_sources.len() == 1 {
        *known_sources.first().unwrap()
    } else {
        "unavailable"
    };
    if baseline_workers == 0 {
        return Err("scaling baseline workers must be positive".into());
    }
    type SampleBucket<'a> = (
        GroupConfiguration,
        Vec<(&'a TrialPlan, Option<&'a TrialResult>)>,
    );
    let mut buckets: BTreeMap<String, SampleBucket<'_>> = BTreeMap::new();
    let mut overall = Accounting::default();
    for t in &run.run.plan.trials {
        let r = run.results.get(&t.trial_id);
        overall.add(t.warmup, r);
        let source = r
            .and_then(|r| r.execution.as_ref())
            .map(|e| e.measurement_source.as_str())
            .unwrap_or(inferred_source);
        let v = configuration(t, source);
        let cfg: GroupConfiguration =
            serde_json::from_value(v.clone()).map_err(|e| e.to_string())?;
        buckets
            .entry(hash(&v))
            .or_insert_with(|| (cfg, vec![]))
            .1
            .push((t, r));
    }
    let mut groups = vec![];
    for (id, (configuration, trials)) in buckets {
        let mut accounting = Accounting::default();
        let mut eligible_ids = vec![];
        let mut observations: BTreeMap<String, Vec<f64>> = METRICS
            .iter()
            .map(|name| (name.to_string(), vec![]))
            .collect();
        for job in &run.workload.resolved_jobs {
            let content = serde_json::to_value(job.output.content_type)
                .unwrap()
                .as_str()
                .unwrap()
                .to_owned();
            for suffix in ["source_records_per_second", "canonical_rows_per_second"] {
                observations
                    .entry(format!("content_type.{content}.{suffix}"))
                    .or_default();
                for stream in &job.streams {
                    observations
                        .entry(format!("stream.{}.{suffix}", stream.stream_id.0))
                        .or_default();
                }
            }
        }
        let mut total_records = 0u128;
        let mut total_rows = 0u128;
        let mut seconds = 0.;
        let mut signatures = BTreeSet::new();
        for (t, r) in trials {
            accounting.add(t.warmup, r);
            if let Some(r) = r.filter(|r| !t.warmup && eligible(r)) {
                eligible_ids.push(t.trial_id.clone());
                let e = r.execution.as_ref().unwrap();
                let o = r.output.as_ref().unwrap();
                total_records += o.source_records_read as u128;
                total_rows += o.canonical_rows as u128;
                seconds += e.execution_wall_seconds;
                signatures.insert(hash(&(
                    o.canonical_rows,
                    &o.semantic_stream_sha256,
                    &o.depth_events_sha256,
                    &o.reconstruction_sha256,
                )));
                for (name, value) in values(r) {
                    let bucket = observations.entry(name).or_default();
                    if let Some(v) = value {
                        bucket.push(v);
                    }
                }
            }
        }
        let mut metrics = BTreeMap::new();
        for (name, values) in observations {
            metrics.insert(
                name.clone(),
                MetricSummary {
                    unit: unit(&name),
                    statistics: statistics(&values)
                        .map_err(|e| format!("group {id} metric {name}: {e}"))?,
                    missing_samples: eligible_ids.len() - values.len(),
                },
            );
        }
        let consistent = signatures.len() <= 1;
        let mut limitations = vec![];
        if eligible_ids.len() < 2 {
            limitations.push("fewer than two measured samples; variability unavailable".into());
        }
        if !accounting.complete() {
            limitations
                .push("failed warmup or missing/failed/interrupted required repetition".into());
        }
        if !consistent {
            limitations.push("canonical correctness evidence differs across repetitions".into());
        }
        groups.push(GroupSummary {
            group_id: id,
            configuration,
            status: if accounting.complete() && consistent {
                "complete"
            } else {
                "incomplete"
            }
            .into(),
            accounting,
            metrics,
            pooled_source_records_per_second: finite_ratio(total_records as f64, seconds),
            pooled_canonical_rows_per_second: finite_ratio(total_rows as f64, seconds),
            scaling: Scaling {
                baseline_workers,
                speedup: None,
                relative_parallel_efficiency: None,
                source_throughput_ratio: None,
                canonical_throughput_ratio: None,
                limitation: Some("valid comparable baseline unavailable".into()),
            },
            eligible_trial_ids: eligible_ids,
            correctness_consistent: consistent,
            limitations,
        });
    }
    groups.sort_by_key(|g| {
        (
            format!("{:?}", g.configuration.scheduler),
            g.configuration.workers,
            g.group_id.clone(),
        )
    });
    let snapshot = groups.clone();
    for g in &mut groups {
        let base = snapshot.iter().find(|b| {
            b.configuration.workers == baseline_workers
                && scaling_key(&b.configuration) == scaling_key(&g.configuration)
                && b.status == "complete"
        });
        if let Some(b) = base.filter(|_| g.status == "complete") {
            let duration = median(g, "execution_wall_seconds");
            let bd = median(b, "execution_wall_seconds");
            let speedup = bd.zip(duration).and_then(|(a, b)| finite_ratio(a, b));
            g.scaling = Scaling {
                baseline_workers,
                speedup,
                relative_parallel_efficiency: speedup.and_then(|s| {
                    finite_ratio(s, g.configuration.workers as f64 / baseline_workers as f64)
                }),
                source_throughput_ratio: median(g, "source_records_per_second")
                    .zip(median(b, "source_records_per_second"))
                    .and_then(|(a, b)| finite_ratio(a, b)),
                canonical_throughput_ratio: median(g, "canonical_rows_per_second")
                    .zip(median(b, "canonical_rows_per_second"))
                    .and_then(|(a, b)| finite_ratio(a, b)),
                limitation: None,
            };
        }
    }
    Ok(Summary{schema_version:1,kind:"normalization_benchmark_summary".into(),run_id:run.run.run_id.clone(),status:if overall.complete()&&run.run.state==State::Complete&&groups.iter().all(|g|g.correctness_consistent){"complete"}else{"incomplete"}.into(),original_run_state:run.run.state,accounting:overall,groups,software:serde_json::json!({"commit":run.environment.git_commit,"branch":run.environment.git_branch,"dirty":run.environment.git_dirty,"rustc":run.environment.rust_compiler,"profile":run.environment.build_profile,"features":run.environment.cargo_features}),hardware:serde_json::json!({"cpu":run.environment.cpu_model,"logical_cpus":run.environment.logical_cpu_count,"memory_bytes":run.environment.total_memory_bytes,"os":run.environment.operating_system,"architecture":run.environment.architecture}),workload:serde_json::json!({"input_sha256":run.workload.input_sha256,"jobs":run.run.plan.job_count,"tasks":run.run.plan.task_count}),diagnostics,measurement_limitations:vec!["Nearest-rank P95; Welford sample SD; CV uses abs(mean), null for n<2/mean0.".into(),"Ordinary trial differences do not establish statistical significance or causality.".into(),"CPU percent may exceed100; RSS, budgets and accounted memory remain distinct.".into(),"Pooled throughput is total completed work / total execution seconds, separate from mean/median trial rates.".into()]})
}
pub fn median(g: &GroupSummary, name: &str) -> Option<f64> {
    g.metrics
        .get(name)
        .and_then(|m| m.statistics.as_ref())
        .map(|s| s.median)
}
fn scaling_key(c: &GroupConfiguration) -> String {
    let mut c = c.clone();
    c.workers = 0;
    c.config.resources.workers = 0;
    hash(&c)
}
