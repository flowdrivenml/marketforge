use super::{SourceFormat, budget::ResourceRequirements, source_format};
use crate::{
    historical::job::WorkTask,
    shared::{
        error::{MarketForgeError, Result},
        historical::limits::NormalizationLimits,
        normalization::TargetSchema,
        persistence::{config::ParquetResourceConfig, parquet::normalization_writer_memory},
    },
};
use std::fs;
const MIB: u64 = 1024 * 1024;
#[derive(Debug, Clone, Copy)]
pub struct EstimateBreakdown {
    pub format: SourceFormat,
    pub active_source_bytes: u64,
    pub writer_bytes: u64,
    pub finalization_bytes: u64,
    pub open_files: u64,
    pub scratch_bytes: u64,
}
fn overflow() -> MarketForgeError {
    MarketForgeError::InvalidConfiguration("normalization footprint overflow".into())
}
pub fn estimate_breakdown(
    task: &WorkTask,
    parquet: &ParquetResourceConfig,
    limits: &NormalizationLimits,
) -> Result<EstimateBreakdown> {
    limits.validate()?;
    // Accessibility is independent of memory estimation. Never use archive length
    // as a proxy for the decoded working set.
    fs::metadata(&task.input_path).map_err(|source| MarketForgeError::SourceOpen {
        path: task.input_path.clone(),
        source,
    })?;
    let format = source_format(task)?;
    let depth = task
        .normalizations
        .iter()
        .any(|n| n.target_schema == TargetSchema::Depth);
    let identity = (task.symbol.len() as u64)
        .checked_add(task.stream_id.0.len() as u64)
        .ok_or_else(overflow)?;
    // Heuristic allowance for CSV/JSON values and transient transformations; raw
    // record bytes are enforced before accumulation. 1 MiB covers reader and
    // decompressor buffers; 32 MiB is allocator/library safety allowance.
    let mut active = limits
        .record_bytes
        .checked_mul(64)
        .and_then(|v| v.checked_add(33 * MIB))
        .ok_or_else(overflow)?;
    if depth {
        // Old/new books, extraction, normalized quantities, diffs, emitted
        // outcomes and boundary snapshots overlap. Node overhead is heuristic.
        active = active
            .checked_add(
                limits
                    .depth_levels
                    .checked_mul(
                        identity
                            .checked_mul(4)
                            .and_then(|v| v.checked_add(1024))
                            .ok_or_else(overflow)?,
                    )
                    .ok_or_else(overflow)?,
            )
            .ok_or_else(overflow)?;
    }
    if format == SourceFormat::Xlsx {
        // Workbook remains alive through XML parsing and record sorting. XML,
        // materialized rows, shared strings and decoded JSON values overlap.
        // This is an allowance, not a dependency allocation bound.
        active = active
            .checked_add(limits.workbook_bytes)
            .and_then(|v| v.checked_add(limits.workbook_xml_bytes.checked_mul(12)?))
            .and_then(|v| v.checked_add(limits.workbook_rows.checked_mul(512)?))
            .ok_or_else(overflow)?;
    }
    Ok(EstimateBreakdown {
        format,
        active_source_bytes: active
            .checked_add(
                source_report_memory(task, limits)?
                    .checked_mul(4)
                    .ok_or_else(overflow)?,
            )
            .and_then(|v| v.checked_add(report_buffer_memory(task).ok()?))
            .ok_or_else(overflow)?,
        writer_bytes: normalization_writer_memory(
            parquet,
            depth,
            limits.depth_levels,
            identity,
            limits.metric_entries,
        )?,
        finalization_bytes: 8 * MIB,
        open_files: if depth { 5 } else { 3 },
        scratch_bytes: 0,
    })
}
pub fn estimate_task_resources_with_limits(
    task: &WorkTask,
    parquet: &ParquetResourceConfig,
    limits: &NormalizationLimits,
) -> Result<ResourceRequirements> {
    let b = estimate_breakdown(task, parquet, limits)?;
    Ok(ResourceRequirements {
        memory_bytes: b
            .active_source_bytes
            .checked_add(b.writer_bytes)
            .and_then(|v| v.checked_add(b.finalization_bytes))
            .ok_or_else(overflow)?,
        scratch_bytes: b.scratch_bytes,
        open_files: b.open_files,
    })
}
/// Compatibility callers receive the same explicit default limits as workers.
pub fn estimate_task_resources(
    task: &WorkTask,
    parquet: &ParquetResourceConfig,
) -> Result<ResourceRequirements> {
    estimate_task_resources_with_limits(task, parquet, &NormalizationLimits::default())
}

pub(crate) fn report_buffer_memory(task: &WorkTask) -> Result<u64> {
    let identity = (task.stream_id.0.len() as u64)
        .checked_add(task.input_path.to_string_lossy().len() as u64)
        .ok_or_else(overflow)?;
    crate::historical::normalize::metrics::report_buffer_bytes(identity)
}

/// Maximum resident observation map for one source. Active routing/normalization
/// can overlap two maps while combining filtered observations atomically.
pub(crate) fn source_report_memory(task: &WorkTask, limits: &NormalizationLimits) -> Result<u64> {
    let scope = (task.stream_id.0.len() as u64)
        .checked_add(task.input_path.to_string_lossy().len() as u64)
        .ok_or_else(overflow)?;
    (std::mem::size_of::<crate::historical::normalize::metrics::CategoryStatistics>() as u64)
        .checked_add(scope)
        .and_then(|v| v.checked_add(256))
        .and_then(|v| v.checked_mul(limits.metric_entries))
        .ok_or_else(overflow)
}
/// Only compact source results, bounded diagnostics, report references and the
/// prepared/provenance inventories survive successful observation spilling.
pub fn retained_task_memory(task: &WorkTask, _limits: &NormalizationLimits) -> Result<u64> {
    use crate::historical::normalize::metrics::{
        DEFAULT_MAX_DIAGNOSTICS, IntegrityDiagnostic, MAX_DIAGNOSTIC_MESSAGE_BYTES, TaskMetrics,
    };
    let diagnostics = (DEFAULT_MAX_DIAGNOSTICS as u64)
        .checked_mul(
            (MAX_DIAGNOSTIC_MESSAGE_BYTES + std::mem::size_of::<IntegrityDiagnostic>()) as u64,
        )
        .ok_or_else(overflow)?;
    prepared_task_memory(task)?
        .checked_mul(3)
        .and_then(|v| v.checked_add(diagnostics))
        .and_then(|v| v.checked_add(std::mem::size_of::<TaskMetrics>() as u64))
        // Task/source summaries, two reference indexes and ownership-map nodes.
        .and_then(|v| v.checked_add(4096))
        .and_then(|v| v.checked_add((task.stream_id.0.len() as u64).checked_mul(8)?))
        .and_then(|v| v.checked_add((task.input_path.as_os_str().len() as u64).checked_mul(4)?))
        .ok_or_else(overflow)
}
/// A failed spill retains its authoritative map. A session cancellation gate
/// prevents new units starting afterward: at most one such source per worker
/// survives, plus its aggregate copy during failure-report assembly.
pub(crate) fn failure_report_memory(
    job: &crate::historical::job::ProcessingJob,
    limits: &NormalizationLimits,
    workers: usize,
) -> Result<u64> {
    job.tasks
        .iter()
        .try_fold(0u64, |maximum, t| -> Result<u64> {
            Ok(maximum.max(source_report_memory(t, limits)?))
        })?
        .checked_mul(workers.min(job.tasks.len()) as u64)
        .and_then(|v| v.checked_mul(2))
        .ok_or_else(overflow)
}
pub fn retained_jobs_memory(
    jobs: &[crate::historical::job::ProcessingJob],
    limits: &NormalizationLimits,
    workers: usize,
) -> Result<u64> {
    jobs.iter().try_fold(0u64, |sum, job| {
        job.tasks.iter().try_fold(
            sum.checked_add(failure_report_memory(job, limits, workers)?)
                .ok_or_else(overflow)?,
            |sum, task| {
                sum.checked_add(retained_task_memory(task, limits)?)
                    .ok_or_else(overflow)
            },
        )
    })
}

pub(crate) fn reserve_isolated(
    task: &WorkTask,
    config: &crate::historical::normalize::config::ProcessingConfig,
) -> Result<crate::shared::historical::budget::OwnedResourceReservation> {
    let mut required = estimate_task_resources_with_limits(
        task,
        &config.resources.parquet,
        &config.resources.normalization_limits,
    )?;
    required.memory_bytes = required
        .memory_bytes
        .checked_add(retained_task_memory(
            task,
            &config.resources.normalization_limits,
        )?)
        .ok_or_else(overflow)?;
    required.open_files = required.open_files.checked_add(8).ok_or_else(overflow)?;
    let budget = std::sync::Arc::new(super::ResourceBudget::with_file_limit(
        config.resources.memory_budget_bytes,
        config.resources.scratch_budget_bytes,
        config.resources.normalization_limits.open_files,
    )?);
    budget.acquire_owned(required)
}

pub(crate) fn job_finalization_memory(
    parquet: &ParquetResourceConfig,
    job: &crate::historical::job::ProcessingJob,
    limits: &NormalizationLimits,
) -> Result<u64> {
    let report_buffer = job
        .tasks
        .iter()
        .try_fold(0u64, |maximum, t| -> Result<u64> {
            Ok(maximum.max(report_buffer_memory(t)?))
        })?;
    let base = limits
        .metric_entries
        .checked_mul(16 * 1024)
        .and_then(|v| v.checked_add(16 * MIB))
        .and_then(|v| v.checked_add(report_buffer))
        .ok_or_else(overflow)?;
    // Segment JSON input, decoded segments, initialization tuples and validation
    // copies. Boundary snapshots have one bounded reader workspace per phase.
    if job.output.content_type == crate::historical::job::ContentType::Depth {
        crate::shared::persistence::parquet::normalization_segment_memory(
            parquet,
            limits.metric_entries,
        )?
        .checked_mul(12)
        .and_then(|v| v.checked_add(base))
        .and_then(|v| {
            v.checked_add(
                boundary_read_limits(job, *limits, job.tasks.len())
                    .ok()?
                    .workspace_bytes()
                    .ok()?,
            )
        })
        .ok_or_else(overflow)
    } else {
        Ok(base)
    }
}

/// Legacy writer-owning trade APIs have no runtime argument. Outside an already
/// admitted config-aware call, use the same default TOML/project root as the CLI.
/// Missing configuration is an explicit failure, never unlimited admission.
pub(crate) fn reserve_compatibility_trade(
    task: &WorkTask,
    parquet: &ParquetResourceConfig,
) -> Result<
    Option<(
        crate::shared::historical::limits::LimitScope,
        crate::shared::historical::budget::OwnedResourceReservation,
    )>,
> {
    use crate::shared::historical::limits::{LimitScope, current};
    if current().is_some() {
        return Ok(None);
    }
    let mut config = crate::shared::historical::load_processing_config(
        "data/.config/historical/general.toml",
        ".",
    )?;
    // Preserve this legacy interface's explicit writer settings.
    config.resources.parquet = parquet.clone();
    let scope = LimitScope::enter(
        config.resources.normalization_limits,
        format!(
            "task={}, source={}",
            task.task_id.0,
            task.input_path.display()
        ),
    );
    let reservation = reserve_isolated(task, &config)?;
    Ok(Some((scope, reservation)))
}

pub(crate) fn prepared_task_memory(task: &WorkTask) -> Result<u64> {
    fn json(value: &serde_json::Value) -> Result<u64> {
        let base = std::mem::size_of::<serde_json::Value>() as u64;
        match value {
            serde_json::Value::String(s) => {
                base.checked_add(s.capacity() as u64).ok_or_else(overflow)
            }
            serde_json::Value::Array(items) => items.iter().try_fold(
                base.checked_add(
                    (items.capacity() as u64)
                        .checked_mul(base)
                        .ok_or_else(overflow)?,
                )
                .ok_or_else(overflow)?,
                |sum, item| sum.checked_add(json(item)?).ok_or_else(overflow),
            ),
            serde_json::Value::Object(items) => items.iter().try_fold(base, |sum, (key, item)| {
                let item = json(item)?;
                sum.checked_add(key.capacity() as u64)
                    .and_then(|n| n.checked_add(96))
                    .and_then(|n| n.checked_add(item))
                    .ok_or_else(overflow)
            }),
            _ => base.checked_add(128).ok_or_else(overflow),
        }
    }
    let mut bytes = (std::mem::size_of::<WorkTask>() as u64)
        .checked_add(json(&task.raw_schema)?)
        .ok_or_else(overflow)?;
    for config in &task.normalizations {
        bytes = bytes
            .checked_add(json(&config.rules)?)
            .ok_or_else(overflow)?;
    }
    for text in [&task.symbol, &task.stream_id.0, &task.format_code.0] {
        bytes = bytes
            .checked_add(text.capacity() as u64)
            .ok_or_else(overflow)?;
    }
    for text in [
        task.archive_member.as_ref(),
        task.instrument.contract_value_asset.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        bytes = bytes
            .checked_add(text.capacity() as u64)
            .ok_or_else(overflow)?;
    }
    bytes
        .checked_add(task.input_path.as_os_str().len() as u64)
        .ok_or_else(overflow)
}

/// Same source/identity bounds are used by admission and the production visitor.
pub(crate) fn boundary_read_limits(
    job: &crate::historical::job::ProcessingJob,
    limits: NormalizationLimits,
    sources: usize,
) -> Result<crate::shared::persistence::parquet::BoundaryReadLimits> {
    let identity = job.tasks.iter().try_fold(0u64, |maximum, task| {
        (task.symbol.len() as u64)
            .checked_add(task.stream_id.0.len() as u64)
            .map(|bytes| maximum.max(bytes))
            .ok_or_else(overflow)
    })?;
    crate::shared::persistence::parquet::BoundaryReadLimits::for_sources(sources, limits)?
        .with_identity_bytes(identity)
}
