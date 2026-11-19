//! Stream execution units own resource reservations and writers through finalization.
use super::{
    config::ProcessingConfig,
    metrics::{ScopedIntegrityMetrics, TaskMetrics},
    parquet::ParquetTradeWriter,
    progress::{ProgressReporter, ProgressTaskId, WorkerProgress},
    resources::{ResourceRequirements, estimate_task_resources_with_limits},
    task::TaskExecution,
    worker::TradeSink,
};
use crate::{
    historical::job::{ProcessingJob, WorkTask},
    shared::{
        domain::canonical::Trade,
        error::{MarketForgeError, Result},
        normalization::TargetSchema,
    },
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSpan {
    pub task_id: u64,
    /// Half-open canonical row range within this stream's ordered files.
    pub start_row: u64,
    pub end_row: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ranges: Vec<RowRange>,
}
impl SourceSpan {
    pub fn row_count(&self) -> Result<u64> {
        if self.ranges.is_empty() {
            return self
                .end_row
                .checked_sub(self.start_row)
                .ok_or_else(|| invalid("source row range underflow"));
        }
        self.ranges.iter().try_fold(0u64, |n, r| {
            n.checked_add(
                r.end_row
                    .checked_sub(r.start_row)
                    .ok_or_else(|| invalid("invalid source row range"))?,
            )
            .ok_or_else(|| invalid("source row total overflow"))
        })
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowRange {
    pub start_row: u64,
    pub end_row: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamOutput {
    pub stream_id: String,
    pub directory: String,
    pub sources: Vec<SourceSpan>,
}
#[derive(Debug)]
pub(crate) struct StreamUnit<'a> {
    pub job: &'a ProcessingJob,
    pub tasks: Vec<(usize, &'a WorkTask)>,
    pub key: String,
}
fn invalid(message: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(message.into())
}
fn fingerprint(value: &impl Serialize) -> Result<String> {
    use sha2::{Digest, Sha256};
    Ok(super::hex_digest(Sha256::digest(
        serde_json::to_vec(value).map_err(|e| invalid(e.to_string()))?,
    )))
}
pub(crate) fn is_family(task: &WorkTask) -> bool {
    task.raw_schema["marketforge_source"]["prepared_schema_version"].as_u64() == Some(4)
        && !task.raw_schema["marketforge_source"]["scope"].is_null()
}
pub(crate) fn priority(index: usize, task: &WorkTask) -> u64 {
    if task.raw_schema["marketforge_source"]["prepared_schema_version"].as_u64() == Some(4) {
        task.raw_schema["marketforge_source"]["source_priority"]
            .as_u64()
            .unwrap_or(index as u64)
    } else {
        index as u64
    }
}
pub(super) fn stream_key(task: &WorkTask) -> Result<String> {
    fingerprint(&serde_json::json!([
        task.stream_id,
        task.exchange,
        task.instrument_id,
        task.symbol,
        task.instrument,
        task.normalizations,
        task.source_ordering,
        task.raw_schema["marketforge_source"]["instrument_namespace"],
        task.raw_schema["marketforge_profile"],
        task.raw_schema["marketforge_source"]["source_options"]
    ]))
}
pub(crate) fn plan(job: &ProcessingJob) -> Result<Vec<StreamUnit<'_>>> {
    let mut groups: BTreeMap<String, Vec<(usize, &WorkTask)>> = BTreeMap::new();
    for (index, task) in job.tasks.iter().enumerate() {
        let key = if is_family(task) {
            fingerprint(&serde_json::json!([
                "family-routing",
                task.exchange,
                task.instrument.instrument_kind,
                task.raw_schema["marketforge_source"]["stream_id"],
                task.raw_schema["marketforge_source"]["instrument_namespace"],
                task.raw_schema["marketforge_source"]["scope"]["identifier"],
                task.raw_schema["marketforge_profile"],
                task.raw_schema["marketforge_source"]["source_options"]
            ]))?
        } else {
            stream_key(task)?
        };
        groups.entry(key).or_default().push((index, task));
    }
    let mut units = Vec::with_capacity(groups.len());
    for (key, mut tasks) in groups {
        tasks.sort_by_key(|(index, task)| (priority(*index, task), *index));
        units.push(StreamUnit { job, tasks, key });
    }
    Ok(units)
}

pub(crate) fn needs_reconciliation(unit: &StreamUnit<'_>, config: &ProcessingConfig) -> bool {
    use crate::shared::historical::policies::{DepthOverlap, DepthTimestamp};
    unit.tasks.len() > 1
        || unit.tasks.iter().any(|(_, t)| is_family(t))
        || unit.tasks[0].1.normalizations[0].target_schema == TargetSchema::Trade
        || config.reconciliation.depth.overlap_policy != DepthOverlap::Strict
        || config.timestamp_regression.depth.policy != DepthTimestamp::Strict
}
/// Decoder pipelines are admitted separately; no book/report crosses threads.
pub(crate) fn preparation_capacity(unit: &StreamUnit<'_>, config: &ProcessingConfig) -> usize {
    if unit.tasks.iter().any(|(_, t)| is_family(t))
        || unit
            .tasks
            .iter()
            .map(|(_, t)| &t.input_path)
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != unit.tasks.len()
    {
        0
    } else {
        let desired = config
            .resources
            .workers
            .saturating_sub(1)
            .min(2)
            .min(unit.tasks.len());
        (1..=desired)
            .rev()
            .find(|capacity| {
                estimate_with_preparations(unit, config, *capacity as u64).is_ok_and(|r| {
                    r.memory_bytes <= config.resources.memory_budget_bytes
                        && r.open_files
                            <= config
                                .resources
                                .normalization_limits
                                .open_files
                                .saturating_sub(8)
                })
            })
            .unwrap_or(0)
    }
}

pub(crate) fn estimate(
    unit: &StreamUnit<'_>,
    config: &ProcessingConfig,
) -> Result<ResourceRequirements> {
    estimate_with_preparations(unit, config, preparation_capacity(unit, config) as u64)
}
fn estimate_with_preparations(
    unit: &StreamUnit<'_>,
    config: &ProcessingConfig,
    preparations: u64,
) -> Result<ResourceRequirements> {
    let mut max = ResourceRequirements::default();
    for (_, task) in &unit.tasks {
        let r = estimate_task_resources_with_limits(
            task,
            &config.resources.parquet,
            &config.resources.normalization_limits,
        )?;
        max.memory_bytes = max.memory_bytes.max(r.memory_bytes);
        max.scratch_bytes = max.scratch_bytes.max(r.scratch_bytes);
        max.open_files = max.open_files.max(r.open_files);
    }
    if preparations > 0 && needs_reconciliation(unit, config) {
        let mut active = 0u64;
        for (_, task) in &unit.tasks {
            let b = super::resources::estimate_breakdown(
                task,
                &config.resources.parquet,
                &config.resources.normalization_limits,
            )?;
            active = active.max(
                b.active_source_bytes
                    .checked_add(super::resources::prepared_task_memory(task)?)
                    .and_then(|n| n.checked_add(3 * 1024 * 1024))
                    .and_then(|n| {
                        n.checked_add(
                            if task.normalizations[0].target_schema == TargetSchema::Depth {
                                config
                                    .resources
                                    .normalization_limits
                                    .depth_levels
                                    .checked_mul(
                                        3 * (std::mem::size_of::<
                                            crate::shared::domain::canonical::L2LevelUpdate,
                                        >() as u64
                                            + task.symbol.len() as u64
                                            + task.stream_id.0.len() as u64
                                            + 256),
                                    )?
                            } else {
                                config
                                    .resources
                                    .normalization_limits
                                    .record_bytes
                                    .checked_mul(128)?
                            },
                        )
                    })
                    .ok_or_else(|| invalid("preparation footprint overflow"))?,
            );
        }
        max.memory_bytes = max
            .memory_bytes
            .checked_add(
                active
                    .checked_mul(preparations)
                    .ok_or_else(|| invalid("preparation footprint overflow"))?,
            )
            .ok_or_else(|| invalid("preparation footprint overflow"))?;
    }
    if needs_reconciliation(unit, config) {
        max.open_files = max.open_files.max(
            config
                .merge_fan_in
                .min(
                    config
                        .resources
                        .normalization_limits
                        .open_files
                        .saturating_sub(6) as usize,
                )
                .max(2) as u64
                + 6,
        ); // cursors, spool output, source/Parquet headroom
        max.memory_bytes = max
            .memory_bytes
            .checked_add(config.resources.normalization_limits.reconciliation_bytes)
            .ok_or_else(|| invalid("reconciliation reservation overflow"))?;
    }
    if needs_reconciliation(unit, config) {
        max.open_files = max
            .open_files
            .checked_add(preparations * 2)
            .ok_or_else(|| invalid("preparation handle overflow"))?;
    }
    max.memory_bytes = max
        .memory_bytes
        .checked_add(
            (unit.tasks.len() as u64)
                .checked_mul(
                    (std::mem::size_of::<SourceSpan>()
                        + std::mem::size_of::<(usize, TaskExecution)>()) as u64,
                )
                .ok_or_else(|| invalid("stream metadata estimate overflow"))?,
        )
        .ok_or_else(|| invalid("stream memory estimate overflow"))?;
    Ok(max)
}
struct TradeOutput<'a> {
    writer: &'a mut ParquetTradeWriter,
    rows: u64,
    previous: Option<i64>,
    preceding_source_end: Option<i64>,
}
impl TradeSink for TradeOutput<'_> {
    fn write_trade(&mut self, trade: Trade) -> Result<()> {
        let timestamp = trade.envelope.event_timestamp_ns;
        if self.preceding_source_end.is_some_and(|p| timestamp < p) {
            return Err(invalid(format!(
                "cross-source stream append regression: previous={:?}, current={timestamp}; reconciliation required",
                self.preceding_source_end
            )));
        }
        let next = self
            .rows
            .checked_add(1)
            .ok_or_else(|| invalid("stream row overflow"))?;
        self.writer.write_trade(trade)?;
        self.rows = next;
        self.previous = Some(self.previous.map_or(timestamp, |p| p.max(timestamp)));
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}
pub(crate) fn execute(
    unit: &StreamUnit<'_>,
    config: &ProcessingConfig,
    reporter: &ProgressReporter,
) -> Result<Vec<(usize, TaskExecution)>> {
    let _limits = crate::shared::historical::limits::LimitScope::enter(
        config.resources.normalization_limits,
        format!("job={}, stream={}", unit.job.job_id.0, unit.key),
    );
    if needs_reconciliation(unit, config) {
        let mut results = super::reconciled::execute(unit, config, reporter)?;
        spill_results(unit, config, &mut results, reporter)?;
        return Ok(results);
    }
    let first = unit.tasks[0].1;
    if first.normalizations[0].target_schema == TargetSchema::Depth {
        let mut results = super::streams_depth::execute(unit, config, reporter)?;
        spill_results(unit, config, &mut results, reporter)?;
        return Ok(results);
    }
    let directory = format!("streams/{}/trades", unit.key);
    let path = unit.job.output.staging_path.join(&directory);
    let mut writer = ParquetTradeWriter::new(&path, config.resources.parquet.clone())?;
    let mut sink = TradeOutput {
        writer: &mut writer,
        rows: 0,
        previous: None,
        preceding_source_end: None,
    };
    let mut executions = Vec::new();
    let mut sources = Vec::new();
    for (index, task) in &unit.tasks {
        let id = ProgressTaskId::new(unit.job.job_id.0, task.task_id.0);
        if stopped(&executions) {
            reporter.task_skipped(id, "Stream stopped after a fatal source failure".into());
            executions.push((
                *index,
                TaskExecution {
                    task_id: task.task_id.0,
                    output_paths: vec![],
                    metrics: Default::default(),
                    error: Some(MarketForgeError::NotAttempted {
                        reason: "source not attempted: preceding stream failure".into(),
                    }),
                },
            ));
            continue;
        }

        reporter.phase(id, &unit.key, "source read/decode/normalize into Parquet");
        reporter.task_started(
            id,
            format!("{:?}", task.exchange),
            task.symbol.clone(),
            "Trades".into(),
            task.input_path.display().to_string(),
        );
        sink.preceding_source_end = sink.previous;
        let start = sink.rows;
        let mut metrics = TaskMetrics::default();
        let mut progress = WorkerProgress::new(reporter, id);
        let policy = super::task::worker_integrity_policy(&config.integrity_policy);
        let result = super::worker::process_trade_task_with_metrics_and_progress(
            task,
            &mut sink,
            &mut metrics,
            &policy,
            &ScopedIntegrityMetrics::default(),
            &mut progress,
        );
        progress.finish(&metrics.counters);
        reporter.phase(
            id,
            &unit.key,
            if result.is_ok() {
                "source normalization complete"
            } else {
                "source normalization failed"
            },
        );
        let result = result.and_then(|()| {
            if sink.rows - start == metrics.counters.events_written {
                Ok(())
            } else {
                Err(invalid("task/sink row count mismatch"))
            }
        });
        if let Err(error) = &result {
            reporter.task_failed(id, error.to_string());
        }
        sources.push(SourceSpan {
            task_id: task.task_id.0,
            start_row: start,
            end_row: sink.rows,
            ranges: vec![],
        });
        executions.push((
            *index,
            TaskExecution {
                task_id: task.task_id.0,
                output_paths: vec![path.clone()],
                metrics,
                error: result.err(),
            },
        ));
    }
    let total = sink.rows;
    if executions.iter().all(|(_, e)| e.error.is_none()) {
        reporter.stream_phase(unit.job.job_id.0, &unit.key, "Parquet writing/finalization");
        let result = writer.finish().and_then(|()| {
            if writer.metrics().trades_written == total
                && writer.is_finished()
                && !writer.is_failed()
            {
                Ok(())
            } else {
                Err(invalid("stream writer accounting/finalization mismatch"))
            }
        });
        if let Err(error) = result {
            executions.last_mut().unwrap().1.error = Some(MarketForgeError::StreamFailure {
                stream: unit.key.clone(),
                stage: "writer finalization",
                source: Box::new(error),
            });
        } else {
            let output = StreamOutput {
                stream_id: first.stream_id.0.clone(),
                directory,
                sources,
            };
            if let Err(error) = write_output(&path, &output) {
                executions.last_mut().unwrap().1.error = Some(MarketForgeError::StreamFailure {
                    stream: unit.key.clone(),
                    stage: "writer finalization",
                    source: Box::new(error),
                });
            }
        }
    }
    spill_results(unit, config, &mut executions, reporter)?;
    Ok(executions)
}
pub(super) fn write_output(path: &Path, output: &StreamOutput) -> Result<()> {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path.join("sources.json"))
        .map_err(|e| invalid(e.to_string()))?;
    serde_json::to_writer_pretty(file, output).map_err(|e| invalid(e.to_string()))
}
pub(crate) fn read_outputs(root: &Path) -> Result<Vec<StreamOutput>> {
    let mut outputs = Vec::new();
    let streams = root.join("streams");
    if !streams.exists() {
        return Ok(outputs);
    }
    for entry in std::fs::read_dir(streams).map_err(|e| invalid(e.to_string()))? {
        let directory = entry.map_err(|e| invalid(e.to_string()))?.path();
        for kind in ["trades", "depth"] {
            let path = directory.join(kind).join("sources.json");
            if path.is_file() {
                outputs.push(
                    serde_json::from_slice(
                        &std::fs::read(path).map_err(|e| invalid(e.to_string()))?,
                    )
                    .map_err(|e| invalid(e.to_string()))?,
                );
            }
        }
    }
    outputs.sort_by(|a, b| a.directory.cmp(&b.directory));
    Ok(outputs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::historical::{
        config::normalization_fixture::bybit_spot_trade_job,
        job::{ContractKind, InstrumentKind, TaskId},
    };
    fn job() -> ProcessingJob {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .to_path_buf();
        let config =
            super::super::load_processing_config("data/.config/historical/general.toml", &root)
                .unwrap();
        let mut job = bybit_spot_trade_job(&config).unwrap();
        let mut next = job.tasks[0].clone();
        next.task_id = TaskId(2);
        next.input_path = "another-path".into();
        job.tasks.push(next);
        job
    }
    #[test]
    fn grouping_ignores_task_paths_but_separates_concrete_semantics() {
        let base = job();
        assert_eq!(plan(&base).unwrap().len(), 1);
        for dimension in 0..7 {
            let mut job = base.clone();
            let task = &mut job.tasks[1];
            match dimension {
                0 => task.exchange = crate::shared::domain::canonical::Exchange::Okx,
                1 => task.instrument_id += 1,
                2 => task.symbol = "ETHUSDT".into(),
                3 => {
                    task.instrument.instrument_kind = InstrumentKind::Perpetual;
                    task.instrument.contract_kind = Some(ContractKind::Linear);
                }
                4 => task.instrument.tick_size += rust_decimal::Decimal::ONE,
                5 => task.raw_schema["marketforge_profile"]["sha256"] = "different-profile".into(),
                _ => {
                    task.raw_schema["marketforge_source"]["source_options"] =
                        serde_json::json!({"feed":"snapshot"})
                }
            }
            assert_eq!(plan(&job).unwrap().len(), 2, "dimension {dimension}");
        }
    }
}

pub(super) fn stopped(executions: &[(usize, TaskExecution)]) -> bool {
    executions
        .iter()
        .any(|(_, execution)| execution.error.is_some())
}

fn spill_results(
    unit: &StreamUnit<'_>,
    config: &ProcessingConfig,
    results: &mut [(usize, TaskExecution)],
    reporter: &ProgressReporter,
) -> Result<()> {
    for (_, e) in results {
        if e.metrics.scoped_integrity.files.is_empty()
            && e.metrics.scoped_integrity.hourly.is_empty()
            && e.metrics.scoped_integrity.daily.is_empty()
        {
            continue;
        }
        reporter.phase(
            ProgressTaskId::new(unit.job.job_id.0, e.task_id),
            &unit.key,
            "persisting source integrity observations",
        );
        if let Err(error) = e
            .metrics
            .scoped_integrity
            .spill(unit.job.job_id.0, e.task_id, config)
        {
            e.metrics.counters.tasks_completed = 0;
            e.metrics.counters.tasks_failed = 1;
            // Keep metrics and any original processing error if persistence fails.
            if let Some(original) = e.error.take() {
                e.error = Some(super::failure::report_persistence_error(original, error));
            } else {
                e.error = Some(error);
            }
        }
    }
    Ok(())
}
