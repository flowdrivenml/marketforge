//! Nonexecuting inspection of the *current* production admission formula.
//! Includes compact inventories, active workspaces and bounded spill-failure retention.
use super::{estimate, estimate_breakdown};
use crate::{
    historical::{
        job::ProcessingJob,
        normalize::{ProcessingConfig, streams},
    },
    shared::error::{MarketForgeError, Result},
};
use serde::Serialize;
#[derive(Debug, Serialize)]
pub struct AdmissionDiagnostic {
    pub accounting_kind: &'static str,
    pub configured_memory_bytes: u64,
    pub configured_scratch_bytes: u64,
    pub configured_open_files: u64,
    pub jobs: Vec<JobRetention>,
    pub retained_metadata_bytes: u64,
    pub retained_detailed_reports_bytes: u64,
    pub retained_compact_reports_bytes: u64,
    pub failed_spill_retention_allowance_bytes: u64,
    pub retained_boundary_allowance_bytes: u64,
    pub streams: Vec<StreamAdmission>,
    pub largest_active_unit_bytes: u64,
    pub finalization_allowance_bytes: u64,
    pub required_memory_bytes: u64,
    pub memory_admission_fits: bool,
    pub current_reserved_bytes: Option<u64>,
    pub peak_accounted_bytes: Option<u64>,
    pub measured_process_rss_bytes: Option<u64>,
}
#[derive(Debug, Serialize)]
pub struct JobRetention {
    pub job_id: u64,
    pub tasks: usize,
    pub retained_bytes: u64,
}
#[derive(Debug, Serialize)]
pub struct StreamAdmission {
    pub job_id: u64,
    pub fingerprint: String,
    pub stream_id: String,
    pub exchange: String,
    pub symbol: String,
    pub sources: usize,
    pub active_source_decoder_book_bytes: u64,
    pub writer_bytes: u64,
    pub reconciliation_bytes: u64,
    pub descriptor_overhead_bytes: u64,
    pub active_safety_bytes: u64,
    pub required_bytes: u64,
    pub open_files: u64,
}
fn overflow() -> MarketForgeError {
    MarketForgeError::InvalidConfiguration("admission diagnostic overflow".into())
}
fn add(a: &mut u64, b: u64) -> Result<()> {
    *a = a.checked_add(b).ok_or_else(overflow)?;
    Ok(())
}
/// Resolves only planned footprints. Stream estimation stats source paths for
/// accessibility; this function never opens archive payloads or creates staging.
pub fn inspect_admission(
    jobs: &[ProcessingJob],
    config: &ProcessingConfig,
) -> Result<AdmissionDiagnostic> {
    let mut d = AdmissionDiagnostic {
        accounting_kind: "planned_reservation_not_measured_rss",
        configured_memory_bytes: config.resources.memory_budget_bytes,
        configured_scratch_bytes: config.resources.scratch_budget_bytes,
        configured_open_files: config.resources.normalization_limits.open_files,
        jobs: vec![],
        retained_metadata_bytes: 0,
        retained_detailed_reports_bytes: 0,
        retained_compact_reports_bytes: 0,
        failed_spill_retention_allowance_bytes: 0,
        retained_boundary_allowance_bytes: 0,
        streams: vec![],
        largest_active_unit_bytes: 0,
        finalization_allowance_bytes: 0,
        required_memory_bytes: 0,
        memory_admission_fits: false,
        current_reserved_bytes: None,
        peak_accounted_bytes: None,
        measured_process_rss_bytes: None,
    };
    let limits = &config.resources.normalization_limits;
    for job in jobs {
        let mut retained = 0;
        for task in &job.tasks {
            let total = estimate::retained_task_memory(task, limits)?;
            let metadata = estimate::prepared_task_memory(task)?
                .checked_mul(3)
                .ok_or_else(overflow)?;
            let boundary = 0; // no per-source retained books; one finalization workspace
            let reports = total
                .checked_sub(metadata)
                .and_then(|v| v.checked_sub(boundary))
                .ok_or_else(overflow)?;
            add(&mut retained, total)?;
            add(&mut d.retained_metadata_bytes, metadata)?;
            add(&mut d.retained_boundary_allowance_bytes, boundary)?;
            add(&mut d.retained_compact_reports_bytes, reports)?;
        }
        let failure = estimate::failure_report_memory(job, limits, config.resources.workers)?;
        add(&mut retained, failure)?;
        add(&mut d.failed_spill_retention_allowance_bytes, failure)?;
        d.jobs.push(JobRetention {
            job_id: job.job_id.0,
            tasks: job.tasks.len(),
            retained_bytes: retained,
        });
        d.finalization_allowance_bytes =
            d.finalization_allowance_bytes
                .max(estimate::job_finalization_memory(
                    &config.resources.parquet,
                    job,
                    limits,
                )?);
        for unit in streams::plan(job)? {
            let required = streams::estimate(&unit, config)?;
            let mut selected = None;
            for (_, task) in &unit.tasks {
                let b = estimate_breakdown(task, &config.resources.parquet, limits)?;
                let bytes = b
                    .active_source_bytes
                    .checked_add(b.writer_bytes)
                    .and_then(|n| n.checked_add(b.finalization_bytes))
                    .ok_or_else(overflow)?;
                if selected.as_ref().is_none_or(|(n, _)| bytes > *n) {
                    selected = Some((bytes, b));
                }
            }
            let (base, b) = selected.ok_or_else(|| {
                MarketForgeError::InvalidConfiguration("empty execution unit".into())
            })?;
            let reconciliation =
                if unit.tasks.len() > 1 || unit.tasks.iter().any(|(_, t)| streams::is_family(t)) {
                    limits.reconciliation_bytes
                } else {
                    0
                };
            let descriptors = required
                .memory_bytes
                .checked_sub(base)
                .and_then(|n| n.checked_sub(reconciliation))
                .ok_or_else(overflow)?;
            d.largest_active_unit_bytes = d.largest_active_unit_bytes.max(required.memory_bytes);
            let first = unit.tasks[0].1;
            d.streams.push(StreamAdmission {
                job_id: job.job_id.0,
                fingerprint: unit.key,
                stream_id: first.stream_id.0.clone(),
                exchange: format!("{:?}", first.exchange),
                symbol: first.symbol.clone(),
                sources: unit.tasks.len(),
                active_source_decoder_book_bytes: b.active_source_bytes,
                writer_bytes: b.writer_bytes,
                reconciliation_bytes: reconciliation,
                descriptor_overhead_bytes: descriptors,
                active_safety_bytes: b.finalization_bytes,
                required_bytes: required.memory_bytes,
                open_files: required.open_files,
            });
        }
    }
    let retained = estimate::retained_jobs_memory(jobs, limits, config.resources.workers)?;
    let decomposed = d
        .retained_metadata_bytes
        .checked_add(d.retained_detailed_reports_bytes)
        .and_then(|n| n.checked_add(d.retained_compact_reports_bytes))
        .and_then(|n| n.checked_add(d.failed_spill_retention_allowance_bytes))
        .and_then(|n| n.checked_add(d.retained_boundary_allowance_bytes))
        .ok_or_else(overflow)?;
    if retained != decomposed {
        return Err(MarketForgeError::InvalidConfiguration(
            "retained admission diagnostic disagrees with production estimator".into(),
        ));
    }
    d.required_memory_bytes = retained
        .checked_add(
            d.largest_active_unit_bytes
                .max(d.finalization_allowance_bytes),
        )
        .ok_or_else(overflow)?;
    d.memory_admission_fits = d.required_memory_bytes <= d.configured_memory_bytes;
    Ok(d)
}
