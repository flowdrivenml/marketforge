use super::{
    Result,
    analysis::{self, GroupSummary, LoadedRun, Summary, median},
    statistics::Statistics,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, path::PathBuf};
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, clap::ValueEnum,
)]
#[serde(rename_all = "kebab-case")]
pub enum Variable {
    Software,
    Workers,
    MemoryBudget,
    ScratchBudget,
    Parquet,
    Scheduler,
    Hardware,
    Build,
    IntegrityPolicy,
    Profiles,
    Cache,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Comparability {
    Comparable,
    ComparableWithCaveats,
    Incomparable,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Difference {
    pub dimension: String,
    pub baseline: Value,
    pub candidate: Value,
    pub intentional: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Evidence {
    pub equivalent: bool,
    pub reasons: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricComparison {
    pub baseline_group: String,
    pub candidate_group: String,
    pub baseline_workers: usize,
    pub candidate_workers: usize,
    pub metric: String,
    pub unit: String,
    pub baseline: Option<Statistics>,
    pub candidate: Option<Statistics>,
    pub absolute_difference: Option<f64>,
    pub percentage_difference: Option<f64>,
    pub ratio: Option<f64>,
    pub availability: String,
    pub comparability: Comparability,
    pub interpretation: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Comparison {
    pub schema_version: u32,
    pub kind: String,
    pub comparison_id: String,
    pub baseline_run_id: String,
    pub candidate_run_id: String,
    pub baseline_path: PathBuf,
    pub candidate_path: PathBuf,
    pub created_unix_ns: String,
    pub comparison_status: String,
    pub comparability: Comparability,
    pub comparability_reasons: Vec<String>,
    pub intentional_variables: Vec<Variable>,
    pub configuration_differences: Vec<Difference>,
    pub workload_verification: Evidence,
    pub correctness_verification: Evidence,
    pub metric_comparisons: Vec<MetricComparison>,
    pub measurement_limitations: Vec<String>,
}
fn workload_key(run: &LoadedRun, prefer_hash: bool) -> Result<(String, bool, bool)> {
    let mut weak = false;
    let mut missing = false;
    let mut jobs = vec![];
    for job in &run.workload.resolved_jobs {
        let mut tasks = vec![];
        for t in &job.tasks {
            let source = run.workload.sources.iter().find(|s| s.path == t.input_path);
            let identity = match source {
                Some(s) if prefer_hash && s.sha256.is_some() => {
                    json!({"sha256":s.sha256})
                }
                Some(s) if s.size_bytes.is_some() && s.modified_unix_ns.is_some() => {
                    weak = true;
                    json!({"filename":s.path.file_name().map(|s|s.to_string_lossy()),"size":s.size_bytes,"mtime":s.modified_unix_ns})
                }
                _ => {
                    missing = true;
                    Value::Null
                }
            };
            tasks.push(json!({"task_id":t.task_id,"stream_id":t.stream_id,"exchange":t.exchange,"instrument_id":t.instrument_id,"instrument":t.instrument,"symbol":t.symbol,"source":identity,"archive_member":t.archive_member,"ordering":t.source_ordering,"feed":t.raw_schema["marketforge_source"]["source_options"],"namespace":t.raw_schema["marketforge_source"]["instrument_namespace"],"source_rank":t.raw_schema["marketforge_source"]["stream_rank"]}));
        }
        tasks.sort_by_key(analysis::hash);
        jobs.push(json!({"content_type":job.output.content_type,"streams":job.streams,"ordering":job.ordering,"tasks":tasks}));
    }
    jobs.sort_by_key(analysis::hash);
    Ok((analysis::hash(&jobs), weak, missing))
}
fn profiles(run: &LoadedRun) -> String {
    let mut values:Vec<_>=run.workload.resolved_jobs.iter().flat_map(|j|j.tasks.iter().map(|t|json!({"identity":[t.exchange,t.instrument.instrument_kind],"symbol":t.symbol,"profile":t.raw_schema["marketforge_profile"],"normalizations":t.normalizations,"compression":t.source_compression}))).collect();
    values.sort_by_key(analysis::hash);
    analysis::hash(&values)
}
fn source_proofs(
    result: &super::model::TrialResult,
    proofs: &std::collections::BTreeMap<String, String>,
) -> Option<Vec<(String, String)>> {
    let mut normalized = Vec::new();
    for (key, value) in proofs {
        let source = result.trial.jobs.iter().find_map(|job| {
            job.tasks
                .iter()
                .find(|t| format!("{}.{}", job.job_id.0, t.task_id.0) == *key)
                .map(|task| (job, task))
        })?;
        let identity = analysis::hash(&(
            source.0.output.content_type,
            source.1.task_id,
            &source.1.stream_id,
            source.1.instrument_id,
            &source.1.archive_member,
        ));
        normalized.push((identity, value.clone()));
    }
    normalized.sort();
    Some(normalized)
}
fn correctness(run: &LoadedRun) -> (BTreeSet<String>, bool, bool) {
    let mut values = BTreeSet::new();
    let mut weak = false;
    let mut invalid = false;
    for result in run.results.values().filter(|r| !r.trial.warmup) {
        if !analysis::eligible(result) {
            invalid = true;
            continue;
        }
        let o = result.output.as_ref().unwrap();
        let required_depth_sources = result
            .trial
            .jobs
            .iter()
            .filter(|j| j.output.content_type == crate::historical::job::ContentType::Depth)
            .map(|j| j.tasks.len())
            .sum::<usize>();
        weak |= o.semantic_stream_sha256.is_empty()
            || o.depth_events_sha256.len() < required_depth_sources
            || o.reconstruction_sha256.len() < required_depth_sources;
        let Some(events) = source_proofs(result, &o.depth_events_sha256) else {
            invalid = true;
            continue;
        };
        let Some(reconstruction) = source_proofs(result, &o.reconstruction_sha256) else {
            invalid = true;
            continue;
        };
        values.insert(analysis::hash(&(
            o.canonical_rows,
            &o.semantic_stream_sha256,
            events,
            reconstruction,
        )));
    }
    if values.is_empty() {
        invalid = true;
    }
    (values, weak, invalid)
}
fn normalized_config(group: &GroupSummary, variables: &BTreeSet<Variable>) -> Value {
    let mut value = serde_json::to_value(&group.configuration).unwrap();
    let obj = value.as_object_mut().unwrap();
    obj.remove("workload_identity");
    if variables.contains(&Variable::Workers) {
        obj.remove("workers");
        value["config"]["resources"]
            .as_object_mut()
            .unwrap()
            .remove("workers");
    }
    if variables.contains(&Variable::Scheduler) {
        value.as_object_mut().unwrap().remove("scheduler");
    }
    for (var, key) in [
        (Variable::MemoryBudget, "memory_budget_bytes"),
        (Variable::ScratchBudget, "scratch_budget_bytes"),
        (Variable::Parquet, "parquet"),
    ] {
        if variables.contains(&var) {
            value["config"]["resources"]
                .as_object_mut()
                .unwrap()
                .remove(key);
        }
    }
    if variables.contains(&Variable::IntegrityPolicy) {
        value["config"]
            .as_object_mut()
            .unwrap()
            .remove("integrity_policy");
    }
    value
}
pub fn compare(
    b: &LoadedRun,
    c: &LoadedRun,
    bs: &Summary,
    cs: &Summary,
    variables: &[Variable],
    id: String,
) -> Result<Comparison> {
    if bs.schema_version != 1
        || cs.schema_version != 1
        || bs.kind != "normalization_benchmark_summary"
        || cs.kind != "normalization_benchmark_summary"
    {
        return Err("unsupported summary schema/kind".into());
    }
    let variables: BTreeSet<_> = variables
        .iter()
        .copied()
        .chain([Variable::Software])
        .collect();
    let mut bad = vec![];
    let mut caveats = vec![];
    let mut differences = vec![];
    let strong = b.workload.sources.iter().all(|s| s.sha256.is_some())
        && c.workload.sources.iter().all(|s| s.sha256.is_some());
    let (bkey, bweak, bmissing) = workload_key(b, strong)?;
    let (ckey, cweak, cmissing) = workload_key(c, strong)?;
    let hashes = |run: &LoadedRun| {
        let mut map = std::collections::BTreeMap::new();
        for j in &run.workload.resolved_jobs {
            for t in &j.tasks {
                if let Some(hash) = run
                    .workload
                    .sources
                    .iter()
                    .find(|s| s.path == t.input_path)
                    .and_then(|s| s.sha256.as_ref())
                {
                    map.insert(
                        analysis::hash(&(
                            j.output.content_type,
                            t.task_id,
                            &t.stream_id,
                            t.exchange,
                            &t.symbol,
                            &t.archive_member,
                        )),
                        hash.clone(),
                    );
                }
            }
        }
        map
    };
    let bh = hashes(b);
    let ch = hashes(c);
    let confirmed_mismatch = bh
        .iter()
        .any(|(key, value)| ch.get(key).is_some_and(|other| other != value));
    let work_equal = bkey == ckey && !bmissing && !cmissing && !confirmed_mismatch;
    if !work_equal {
        bad.push(
            "source corpus/task semantics differ or essential source identity is unavailable"
                .into(),
        );
    }
    if bweak || cweak {
        caveats.push("source identity uses path basename/size/mtime rather than full verified archive hashes".into());
    }
    let (bp, bpweak, bpbad) = correctness(b);
    let (cp, cpweak, cpbad) = correctness(c);
    let correct = bp == cp
        && !bpbad
        && !cpbad
        && bs.groups.iter().all(|g| g.correctness_consistent)
        && cs.groups.iter().all(|g| g.correctness_consistent);
    if !correct {
        bad.push(
            "required canonical correctness verification failed, is missing, or differs".into(),
        );
    }
    if bpweak || cpweak {
        caveats.push(
            "only structural/row-count evidence is available; semantic equivalence is unproven"
                .into(),
        );
    }
    if bs.status != "complete" || cs.status != "complete" {
        bad.push(
            "one experiment has incomplete, failed, interrupted or missing required trials/warmups"
                .into(),
        );
    }
    let mut diff = |name: &str, a: Value, d: Value, var: Variable, strict: bool| {
        if a != d {
            let intentional = variables.contains(&var);
            differences.push(Difference {
                dimension: name.into(),
                baseline: a,
                candidate: d,
                intentional,
            });
            if intentional {
                if var != Variable::Software {
                    caveats.push(format!("intentional variable: {name}"));
                }
            } else if strict {
                bad.push(format!("uncontrolled difference: {name}"));
            } else {
                caveats.push(format!("uncontrolled difference: {name}"));
            }
        }
    };
    diff(
        "software revision",
        json!(b.environment.git_commit),
        json!(c.environment.git_commit),
        Variable::Software,
        false,
    );
    diff(
        "benchmark/normalization executable identity and dependencies",
        json!([
            b.environment.executable_sha256,
            b.environment.normalization_executable_identity,
            b.environment.cargo_lock_sha256
        ]),
        json!([
            c.environment.executable_sha256,
            c.environment.normalization_executable_identity,
            c.environment.cargo_lock_sha256
        ]),
        Variable::Software,
        false,
    );
    diff(
        "hardware/platform",
        json!([
            b.environment.cpu_model,
            b.environment.logical_cpu_count,
            b.environment.physical_cpu_count,
            b.environment.available_parallelism,
            b.environment.total_memory_bytes,
            b.environment.architecture,
            b.environment.operating_system,
            b.environment.kernel
        ]),
        json!([
            c.environment.cpu_model,
            c.environment.logical_cpu_count,
            c.environment.physical_cpu_count,
            c.environment.available_parallelism,
            c.environment.total_memory_bytes,
            c.environment.architecture,
            c.environment.operating_system,
            c.environment.kernel
        ]),
        Variable::Hardware,
        true,
    );
    diff(
        "build/compiler/features",
        json!([
            b.environment.build_profile,
            b.environment.rust_compiler,
            b.environment.build_target,
            b.environment.cargo_features
        ]),
        json!([
            c.environment.build_profile,
            c.environment.rust_compiler,
            c.environment.build_target,
            c.environment.cargo_features
        ]),
        Variable::Build,
        true,
    );
    diff(
        "profiles/effective interpretation",
        json!(profiles(b)),
        json!(profiles(c)),
        Variable::Profiles,
        true,
    );
    diff(
        "cache policy",
        json!(b.run.cache_policy),
        json!(c.run.cache_policy),
        Variable::Cache,
        true,
    );
    for (label, a, d, var) in [
        (
            "CPU power profile",
            json!(b.environment.cpu_power_profile),
            json!(c.environment.cpu_power_profile),
            Variable::Hardware,
        ),
        (
            "CPU frequency",
            json!(b.environment.cpu_frequency),
            json!(c.environment.cpu_frequency),
            Variable::Hardware,
        ),
        (
            "filesystem/storage",
            json!([
                b.environment.filesystem_type,
                b.environment.storage_device_type
            ]),
            json!([
                c.environment.filesystem_type,
                c.environment.storage_device_type
            ]),
            Variable::Hardware,
        ),
    ] {
        diff(label, a, d, var, false);
    }
    if b.environment.git_dirty != Some(false) || c.environment.git_dirty != Some(false) {
        caveats.push("dirty/unknown working-tree state limits exact source reproducibility".into());
    }
    if b.run.cache_policy.contains("ordinary") || c.run.cache_policy.contains("ordinary") {
        caveats.push("ordinary OS cache policy does not establish equal cache state".into());
    }
    if b.environment.cpu_model.is_none() || c.environment.cpu_model.is_none() {
        caveats.push("CPU hardware identity incomplete".into());
    }
    let mut pairs = vec![];
    for bg in &bs.groups {
        for cg in &cs.groups {
            if normalized_config(bg, &variables) == normalized_config(cg, &variables) {
                pairs.push((bg, cg));
                if serde_json::to_value(&bg.configuration).unwrap()
                    != serde_json::to_value(&cg.configuration).unwrap()
                {
                    differences.push(Difference {
                        dimension: format!("runtime {} -> {}", bg.group_id, cg.group_id),
                        baseline: serde_json::to_value(&bg.configuration).unwrap(),
                        candidate: serde_json::to_value(&cg.configuration).unwrap(),
                        intentional: true,
                    });
                }
            }
        }
    }
    if pairs.is_empty() {
        bad.push("no groups have comparable measurement definitions and nonintentional runtime/scheduler/integrity settings".into());
    }
    let paired_b: BTreeSet<_> = pairs.iter().map(|(b, _)| &b.group_id).collect();
    let paired_c: BTreeSet<_> = pairs.iter().map(|(_, c)| &c.group_id).collect();
    if paired_b.len() != bs.groups.len() || paired_c.len() != cs.groups.len() {
        caveats.push(
            "some groups have no compatible counterpart; those metrics are not compared".into(),
        );
    }
    if pairs.iter().any(|(b, c)| {
        median(b, "execution_wall_seconds").is_none_or(|v| v <= 0.)
            || median(c, "execution_wall_seconds").is_none_or(|v| v <= 0.)
    }) {
        bad.push("essential execution measurements unavailable".into());
    }
    if pairs.iter().any(|(b, c)| {
        b.metrics
            .values()
            .chain(c.metrics.values())
            .filter_map(|m| m.statistics.as_ref())
            .any(|s| s.sample_count < 2)
    }) {
        caveats.push("fewer than two samples; variability and confidence unavailable".into());
    }
    if pairs.is_empty() {
        differences.push(Difference {
            dimension: "unmatched runtime/scheduler/integrity/measurement configurations".into(),
            baseline: json!(
                bs.groups
                    .iter()
                    .map(|g| &g.configuration)
                    .collect::<Vec<_>>()
            ),
            candidate: json!(
                cs.groups
                    .iter()
                    .map(|g| &g.configuration)
                    .collect::<Vec<_>>()
            ),
            intentional: false,
        });
    }
    let class = if !bad.is_empty() {
        Comparability::Incomparable
    } else if !caveats.is_empty() {
        Comparability::ComparableWithCaveats
    } else {
        Comparability::Comparable
    };
    let mut metrics = vec![];
    for (bg, cg) in pairs {
        for (name, bmetric) in &bg.metrics {
            let cmetric = cg.metrics.get(name);
            let a = bmetric.statistics.clone();
            let d = cmetric.and_then(|m| m.statistics.clone());
            let available = a.is_some() && d.is_some();
            let eligible = available && class != Comparability::Incomparable;
            let av = a.as_ref().map(|s| s.median);
            let cv = d.as_ref().map(|s| s.median);
            let delta = eligible.then(|| cv.unwrap() - av.unwrap());
            let pct = delta
                .zip(av)
                .and_then(|(v, a)| super::statistics::finite_ratio(v * 100., a));
            let ratio = if eligible {
                if name == "execution_wall_seconds" {
                    av.zip(cv)
                        .and_then(|(a, d)| super::statistics::finite_ratio(a, d))
                } else {
                    cv.zip(av)
                        .and_then(|(d, a)| super::statistics::finite_ratio(d, a))
                }
            } else {
                None
            };
            let noise = a
                .as_ref()
                .and_then(|s| s.coefficient_of_variation)
                .unwrap_or(0.)
                .max(
                    d.as_ref()
                        .and_then(|s| s.coefficient_of_variation)
                        .unwrap_or(0.),
                )
                * 100.;
            let enough = a.as_ref().is_some_and(|s| s.sample_count >= 2)
                && d.as_ref().is_some_and(|s| s.sample_count >= 2);
            let interpretation = if !eligible
                || bpweak
                || cpweak
                || !enough
                || pct.is_none()
                || pct.is_some_and(|p| p.abs() <= noise)
            {
                "no established conclusion"
            } else if name == "execution_wall_seconds" {
                if delta.unwrap() < 0. {
                    "observed improvement; significance not established"
                } else {
                    "observed regression; significance not established"
                }
            } else if name.ends_with("source_records_per_second")
                || name.ends_with("canonical_rows_per_second")
            {
                if delta.unwrap() > 0. {
                    "observed improvement; significance not established"
                } else {
                    "observed regression; significance not established"
                }
            } else {
                "trade-off; objective-dependent"
            };
            metrics.push(MetricComparison {
                baseline_group: bg.group_id.clone(),
                candidate_group: cg.group_id.clone(),
                baseline_workers: bg.configuration.workers,
                candidate_workers: cg.configuration.workers,
                metric: name.clone(),
                unit: bmetric.unit.clone(),
                baseline: a,
                candidate: d,
                absolute_difference: delta,
                percentage_difference: pct,
                ratio,
                availability: if available {
                    "available"
                } else {
                    "unavailable"
                }
                .into(),
                comparability: class,
                interpretation: interpretation.into(),
            });
        }
    }
    if metrics.iter().any(|m| m.availability == "unavailable") {
        caveats.push("optional metrics unavailable for one or both groups; no differences calculated for those fields".into());
    }
    // Optional metric absence is a caveat, never an invented zero observation.
    let class = if class == Comparability::Comparable && !caveats.is_empty() {
        Comparability::ComparableWithCaveats
    } else {
        class
    };
    for m in &mut metrics {
        m.comparability = class;
    }
    let mut reasons = bad;
    reasons.extend(caveats);
    reasons.sort();
    reasons.dedup();
    Ok(Comparison{schema_version:1,kind:"normalization_benchmark_comparison".into(),comparison_id:id,baseline_run_id:b.run.run_id.clone(),candidate_run_id:c.run.run_id.clone(),baseline_path:b.path.clone(),candidate_path:c.path.clone(),created_unix_ns:super::runner::now(),comparison_status:if class==Comparability::Incomparable{"invalid_for_optimization_conclusions"}else{"diagnostic_comparison"}.into(),comparability:class,comparability_reasons:reasons,intentional_variables:variables.into_iter().collect(),configuration_differences:differences,workload_verification:Evidence{equivalent:work_equal,reasons:vec![if bweak||cweak{"metadata-based source identity; not verified full-byte equivalence"}else{"verified source hashes and task semantics"}.into()]},correctness_verification:Evidence{equivalent:correct&&!bpweak&&!cpweak,reasons:vec![if bpweak||cpweak{"structural evidence only"}else{"persisted canonical/depth event/reconstruction fingerprints compared"}.into()]},metric_comparisons:metrics,measurement_limitations:vec![if bs.groups.iter().chain(&cs.groups).any(|g|g.configuration.measurement_source.starts_with("synthetic")){"Synthetic fixture observations: these reports are test evidence, not real performance measurements."}else{"Performance observations retain their recorded process/timing boundaries."}.into(),"Median changes do not establish significance; variability and sample counts must be considered.".into(),"File count and runtime changes do not establish a causal relationship.".into(),"Different physical Parquet layouts are allowed when persisted semantic fingerprints agree.".into()]})
}
