use super::{
    Result,
    analysis::{self, Summary},
    comparison::{self, Comparison, Variable},
};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
fn number(n: Option<f64>) -> String {
    n.map(|v| format!("{v:.6}"))
        .unwrap_or_else(|| "unavailable".into())
}
fn clean(text: &str) -> String {
    text.replace('|', "\\|").replace(['\n', '\r'], " ")
}
pub fn summary_markdown(s: &Summary) -> String {
    let mut text = format!(
        "# Normalization benchmark summary\n\nRun: `{}`. Status: **{}**. Original state: {:?}.\n\n## Software\n\n`{}`\n\n## Hardware\n\n`{}`\n\n## Workload\n\n`{}`\n\n## Repetition accounting\n\n`{}`\n",
        clean(&s.run_id),
        s.status,
        s.original_run_state,
        clean(&s.software.to_string()),
        clean(&s.hardware.to_string()),
        clean(&s.workload.to_string()),
        serde_json::to_string(&s.accounting).unwrap()
    );
    for g in &s.groups {
        if g.configuration.measurement_source.starts_with("synthetic") {
            text.push_str("\n**Synthetic test fixtures; not real benchmark measurements.**\n");
        }
        text.push_str(&format!("\n## Group {}\n\nScheduler {:?}; workers {}; status **{}**.\n\nEffective runtime: `{}`\n\nRepetitions: `{}`\n\n### Execution, throughput, CPU, memory, I/O and Parquet output\n\n| Metric | Unit | Samples | Median | Mean | Min | Max | Sample SD | P95 | CV | Missing |\n|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n",g.group_id,g.configuration.scheduler,g.configuration.workers,g.status,clean(&serde_json::to_string(&g.configuration.config).unwrap()),serde_json::to_string(&g.accounting).unwrap()));
        for (name, m) in &g.metrics {
            let stats = m.statistics.as_ref();
            text.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
                name,
                m.unit,
                stats.map_or(0, |s| s.sample_count),
                number(stats.map(|s| s.median)),
                number(stats.map(|s| s.mean)),
                number(stats.map(|s| s.minimum)),
                number(stats.map(|s| s.maximum)),
                number(stats.and_then(|s| s.standard_deviation)),
                number(stats.map(|s| s.p95)),
                number(stats.and_then(|s| s.coefficient_of_variation)),
                m.missing_samples
            ));
        }
        text.push_str(&format!("\n### Worker scaling\n\nBaseline workers: {}. Speedup: {}x. Relative efficiency: {}. Source/canonical throughput ratios: {}/{}. {}\n\nPooled source/canonical throughput: {}/{} (total work / total execution seconds).\n\n### Correctness\n\nConsistent evidence: {}. Eligible trials: {:?}.\n",g.scaling.baseline_workers,number(g.scaling.speedup),number(g.scaling.relative_parallel_efficiency),number(g.scaling.source_throughput_ratio),number(g.scaling.canonical_throughput_ratio),g.scaling.limitation.as_deref().unwrap_or(""),number(g.pooled_source_records_per_second),number(g.pooled_canonical_rows_per_second),g.correctness_consistent,g.eligible_trial_ids));
        for l in &g.limitations {
            text.push_str(&format!("\n- {}\n", clean(l)));
        }
    }
    text.push_str("\n## Measurement limitations and recovery diagnostics\n");
    for l in s.measurement_limitations.iter().chain(&s.diagnostics) {
        text.push_str(&format!("\n- {}\n", clean(l)));
    }
    text
}
pub fn comparison_markdown(c: &Comparison) -> String {
    let mut text = format!(
        "# Normalization benchmark comparison\n\nComparison: `{}`; created {}.\nBaseline: `{}` ({})\nCandidate: `{}` ({})\n\n## Comparability\n\n**{:?}** — {}.\n\nIntentional variables: {:?}.\n",
        clean(&c.comparison_id),
        c.created_unix_ns,
        clean(&c.baseline_run_id),
        clean(&c.baseline_path.display().to_string()),
        clean(&c.candidate_run_id),
        clean(&c.candidate_path.display().to_string()),
        c.comparability,
        c.comparison_status,
        c.intentional_variables
    );
    for reason in &c.comparability_reasons {
        text.push_str(&format!("\n- {}\n", clean(reason)));
    }
    text.push_str("\n## Software, environment and configuration differences\n\n| Dimension | Intentional | Baseline | Candidate |\n|---|---|---|---|\n");
    for d in &c.configuration_differences {
        text.push_str(&format!(
            "| {} | {} | `{}` | `{}` |\n",
            clean(&d.dimension),
            d.intentional,
            clean(&d.baseline.to_string()),
            clean(&d.candidate.to_string())
        ));
    }
    text.push_str(&format!("\n## Workload\n\nEquivalent: {}. {:?}\n\n## Correctness\n\nEquivalent: {}. {:?}\n\n## Performance, resource usage and Parquet output\n\n| Metric | Workers baseline/candidate | Baseline median | Candidate median | Difference | Percent change | Ratio / duration speedup | Availability | Interpretation |\n|---|---|---:|---:|---:|---:|---:|---|---|\n",c.workload_verification.equivalent,c.workload_verification.reasons,c.correctness_verification.equivalent,c.correctness_verification.reasons));
    for m in &c.metric_comparisons {
        text.push_str(&format!(
            "| {} ({}) | {}/{} | {} | {} | {} | {} | {} | {} | {} |\n",
            m.metric,
            m.unit,
            m.baseline_workers,
            m.candidate_workers,
            number(m.baseline.as_ref().map(|s| s.median)),
            number(m.candidate.as_ref().map(|s| s.median)),
            number(m.absolute_difference),
            number(m.percentage_difference),
            number(m.ratio),
            m.availability,
            m.interpretation
        ));
    }
    text.push_str("\n## Statistical variability\n\n| Metric / workers | Baseline samples / SD / CV | Candidate samples / SD / CV |\n|---|---|---|\n");
    for m in &c.metric_comparisons {
        let format = |s: Option<&super::statistics::Statistics>| {
            s.map(|s| {
                format!(
                    "{} / {} / {}",
                    s.sample_count,
                    number(s.standard_deviation),
                    number(s.coefficient_of_variation)
                )
            })
            .unwrap_or_else(|| "unavailable".into())
        };
        text.push_str(&format!(
            "| {} / {}:{} | {} | {} |\n",
            m.metric,
            m.baseline_workers,
            m.candidate_workers,
            format(m.baseline.as_ref()),
            format(m.candidate.as_ref())
        ));
    }
    text.push_str("\n## Conclusions and limitations\n\nThese are observed differences, without automatic optimization acceptance or causal claims.\n");
    for reason in &c.measurement_limitations {
        text.push_str(&format!("\n- {}\n", clean(reason)));
    }
    text
}
fn atomic_text(path: &Path, text: &str) -> Result<()> {
    let next = path.with_extension("md.next");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&next)
        .map_err(|e| e.to_string())?;
    file.write_all(text.as_bytes())
        .and_then(|_| file.sync_all())
        .map_err(|e| e.to_string())?;
    fs::rename(next, path).map_err(|e| e.to_string())
}
pub fn summarize_path(path: &Path, baseline_workers: usize) -> Result<Summary> {
    let run = analysis::load_run(path)?;
    let summary = analysis::summarize(&run, baseline_workers)?;
    super::runner::atomic_json(&run.path.join("summary.json"), &summary)?;
    atomic_text(&run.path.join("summary.md"), &summary_markdown(&summary))?;
    Ok(summary)
}
pub fn compare_paths(
    baseline: &Path,
    candidate: &Path,
    variables: &[Variable],
    root: &Path,
    output: Option<&Path>,
) -> Result<PathBuf> {
    let b = analysis::load_run(baseline)?;
    let c = analysis::load_run(candidate)?;
    let bs = analysis::summarize(&b, 1)?;
    let cs = analysis::summarize(&c, 1)?;
    let directory = if let Some(path) = output {
        super::runner::allocate_run(Some(path), root)?
    } else {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let base = root.join("data/benchmarks/historical/normalize/comparisons");
        loop {
            let next = base.join(format!(
                "{}-{}-{}",
                super::runner::now(),
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            match super::runner::allocate_run(Some(&next), root) {
                Ok(path) => break path,
                Err(_) if next.exists() => continue,
                Err(e) => return Err(e),
            }
        }
    };
    let id = directory
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let comparison = comparison::compare(&b, &c, &bs, &cs, variables, id)?;
    super::runner::atomic_json(&directory.join("comparison.json"), &comparison)?;
    atomic_text(
        &directory.join("comparison.md"),
        &comparison_markdown(&comparison),
    )?;
    Ok(directory)
}
