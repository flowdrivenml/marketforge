use super::{Result, model::*};
use crate::{
    historical::config::{FormatCatalog, read_prepared_batch},
    shared::historical::{load_processing_config, project_root, validate_processing_config},
};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};
pub fn absolute_from(path: &Path, base: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}
pub fn plan(args: &Args, invocation: &Path) -> Result<Plan> {
    if args.workers.is_empty()
        || args.workers.contains(&0)
        || args.workers.iter().copied().collect::<BTreeSet<_>>().len() != args.workers.len()
    {
        return Err("worker list must be nonempty, positive and unique".into());
    }
    if args.repetitions == 0 {
        return Err("measured repetitions must be positive".into());
    }
    let root =
        project_root(absolute_from(&args.project_root, invocation)).map_err(|e| e.to_string())?;
    let input = absolute_from(args.input.as_ref().ok_or("--input required")?, invocation)
        .canonicalize()
        .map_err(|e| format!("benchmark input: {e}"))?;
    let b = read_prepared_batch(&input, &root).map_err(|e| e.to_string())?;
    let mut config = load_processing_config(&args.config, &root).map_err(|e| e.to_string())?;
    if let Some(v) = args.memory_budget {
        config.resources.memory_budget_bytes = v;
    }
    if let Some(v) = args.scratch_budget {
        config.resources.scratch_budget_bytes = v;
    }
    if let Some(v) = args.row_group_target {
        config.resources.parquet.row_group_target_bytes = v;
    }
    if let Some(v) = args.file_target {
        config.resources.parquet.file_target_bytes = v;
    }
    let directory = args
        .output_root
        .as_ref()
        .map(|p| absolute_from(p, invocation))
        .unwrap_or_else(|| root.join("data/benchmarks/historical/normalize/runs/unallocated-plan"));
    if directory.exists() {
        return Err(format!(
            "benchmark output already exists: {}",
            directory.display()
        ));
    }
    super::runner::reject_symlink_ancestors(&directory)?;
    let catalog =
        FormatCatalog::load(root.join("data/.config/historical")).map_err(|e| e.to_string())?;
    let jobs = b
        .jobs
        .iter()
        .map(|j| {
            b.resolve_job(j, &catalog, &config, &root)
                .map_err(|e| e.to_string())
        })
        .collect::<Result<Vec<_>>>()?;
    let task_count = jobs.iter().map(|j| j.tasks.len()).sum();
    if jobs.is_empty() || task_count == 0 {
        return Err("benchmark workload has no executable tasks".into());
    }
    // Warmups precede measured rounds; alternate worker order each round.
    let rounds = args
        .warmups
        .checked_add(args.repetitions)
        .ok_or("trial count overflow")?;
    let total = rounds
        .checked_mul(args.workers.len())
        .ok_or("trial count overflow")?;
    if total > 100000 {
        return Err("execution plan exceeds 100000 trials".into());
    }
    let descriptor_bytes = serde_json::to_vec(&jobs)
        .map_err(|e| e.to_string())?
        .len()
        .checked_mul(total)
        .ok_or("plan descriptor size overflow")?;
    if descriptor_bytes > 256 * 1024 * 1024 {
        return Err("planned resolved-job descriptors exceed 256 MiB; split the experiment".into());
    }
    let mut trials = Vec::with_capacity(total);
    for round in 0..rounds {
        let warmup = round < args.warmups;
        let repetition = if warmup {
            round + 1
        } else {
            round - args.warmups + 1
        };
        let workers: Vec<_> = if round % 2 == 0 {
            args.workers.clone()
        } else {
            args.workers.iter().rev().copied().collect()
        };
        for workers in workers {
            let trial_id = format!(
                "{:06}-{:?}-w{workers}-{}-{repetition}",
                trials.len(),
                args.scheduler,
                if warmup { "warmup" } else { "measured" }
            )
            .to_ascii_lowercase();
            let dir = directory.join("trials").join(&trial_id);
            let mut c = config.clone();
            c.resources.workers = workers;
            c.resources.scratch_path = dir.join("scratch");
            validate_processing_config(&c).map_err(|e| e.to_string())?;
            let mut selected = jobs.clone();
            for job in &mut selected {
                job.resources = c.resources.clone();
                job.integrity_policy = c.integrity_policy.clone();
                job.output.staging_path = dir.join("staging").join(job.dataset_id.0.to_string());
                job.output.dataset_path = dir.join("datasets").join(job.dataset_id.0.to_string());
            }
            trials.push(TrialPlan {
                trial_id,
                scheduler: args.scheduler,
                workers,
                repetition,
                warmup,
                order: trials.len(),
                directory: dir,
                config: c,
                jobs: selected,
            });
        }
    }
    Ok(Plan {
        schema_version: 1,
        kind: "normalization_benchmark_plan".into(),
        input: input.clone(),
        project_root: root,
        directory,
        scheduler: args.scheduler,
        retention: args.retention,
        workers: args.workers.clone(),
        repetitions: args.repetitions,
        warmups: args.warmups,
        input_sha256: super::provenance::file_sha256(&input)?,
        job_count: jobs.len(),
        task_count,
        trials,
    })
}
pub fn rebase(plan: &mut Plan, directory: &Path) -> Result<()> {
    let old = plan.directory.clone();
    for t in &mut plan.trials {
        t.directory = directory.join(t.directory.strip_prefix(&old).map_err(|e| e.to_string())?);
        t.config.resources.scratch_path = t.directory.join("scratch");
        for j in &mut t.jobs {
            j.output.dataset_path = t
                .directory
                .join("datasets")
                .join(j.dataset_id.0.to_string());
            j.output.staging_path = t.directory.join("staging").join(j.dataset_id.0.to_string());
            j.resources = t.config.resources.clone();
        }
    }
    plan.directory = directory.to_path_buf();
    Ok(())
}
