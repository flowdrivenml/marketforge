//! Stream execution units own resource reservations and writers through finalization.
use super::{
    config::ProcessingConfig,
    metrics::{ScopedIntegrityMetrics, TaskMetrics},
    parquet::ParquetTradeWriter,
    progress::{ProgressReporter, ProgressTaskId, WorkerProgress},
    resources::{ResourceRequirements, estimate_task_resources},
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
    Ok(
        Sha256::digest(serde_json::to_vec(value).map_err(|e| invalid(e.to_string()))?)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect(),
    )
}
pub(super) fn bounds(task: &WorkTask) -> Result<Option<(i64, i64, Option<u64>)>> {
    let v = &task.raw_schema["marketforge_source"]["source_time_bounds"];
    if v.is_null() {
        return Ok(None);
    }
    let start = v["start_timestamp_ns"]
        .as_i64()
        .ok_or_else(|| invalid("invalid source start bound"))?;
    let end = v["end_timestamp_ns"]
        .as_i64()
        .ok_or_else(|| invalid("invalid source end bound"))?;
    let partition = v["partition"].as_u64();
    if start > end || partition == Some(0) {
        return Err(invalid("invalid source bounds/partition"));
    }
    Ok(Some((start, end, partition)))
}
pub(crate) fn plan(job: &ProcessingJob) -> Result<Vec<StreamUnit<'_>>> {
    let mut groups: BTreeMap<String, Vec<(usize, &WorkTask)>> = BTreeMap::new();
    for (index, task) in job.tasks.iter().enumerate() {
        // Exact interpreted-profile compatibility is intentionally conservative.
        let identity = serde_json::json!([
            task.stream_id,
            task.exchange,
            task.instrument_id,
            task.symbol,
            task.instrument,
            task.normalizations,
            task.source_ordering,
            task.raw_schema["marketforge_profile"],
            task.raw_schema["marketforge_source"]["source_options"]
        ]);
        let key = fingerprint(&identity)?;
        groups.entry(key).or_default().push((index, task));
    }
    let mut units = Vec::with_capacity(groups.len());
    for (key, mut tasks) in groups {
        if tasks.len() > 1 {
            let mut ordered = Vec::with_capacity(tasks.len());
            for item in tasks {
                let range = bounds(item.1)?.ok_or_else(|| {
                    invalid(format!(
                        "stream {} task {} requires source_time_bounds for shared output",
                        item.1.stream_id.0, item.1.task_id.0
                    ))
                })?;
                ordered.push((range, item));
            }
            ordered.sort_by_key(|(range, _)| *range);
            for pair in ordered.windows(2) {
                let (earlier, later) = (pair[0].0, pair[1].0);
                let partitioned = earlier.0 == later.0
                    && earlier.1 == later.1
                    && earlier.2.is_some()
                    && later.2.is_some()
                    && earlier.2 < later.2;
                if !partitioned && (earlier.1 > later.0 || earlier.0 == later.0) {
                    return Err(invalid(format!(
                        "stream {} has ambiguous/overlapping declared source ranges; provide distinct ordered partitions or reconcile upstream",
                        pair[0].1.1.stream_id.0
                    )));
                }
            }
            tasks = ordered.into_iter().map(|(_, item)| item).collect();
        }
        units.push(StreamUnit { job, tasks, key });
    }
    Ok(units)
}

pub(crate) fn estimate(
    unit: &StreamUnit<'_>,
    config: &ProcessingConfig,
) -> Result<ResourceRequirements> {
    let mut max = ResourceRequirements::default();
    for (_, task) in &unit.tasks {
        let r = estimate_task_resources(task, &config.resources.parquet)?;
        max.memory_bytes = max.memory_bytes.max(r.memory_bytes);
        max.scratch_bytes = max.scratch_bytes.max(r.scratch_bytes);
        max.open_files = max.open_files.max(r.open_files);
    }
    max.memory_bytes = max
        .memory_bytes
        .checked_add(
            (unit.tasks.len() as u64)
                .checked_mul(512)
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
    declared: Option<(i64, i64, Option<u64>)>,
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
        if self
            .declared
            .is_some_and(|(start, end, _)| timestamp < start || timestamp > end)
        {
            return Err(invalid(format!(
                "accepted timestamp {timestamp} outside declared source bounds {:?}",
                self.declared
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
    let first = unit.tasks[0].1;
    if first.normalizations[0].target_schema == TargetSchema::Depth {
        return super::streams_depth::execute(unit, config, reporter);
    }
    let directory = format!("streams/{}/trades", unit.key);
    let path = unit.job.output.staging_path.join(&directory);
    let mut writer = ParquetTradeWriter::new(&path, config.resources.parquet.clone())?;
    let mut sink = TradeOutput {
        writer: &mut writer,
        rows: 0,
        previous: None,
        preceding_source_end: None,
        declared: None,
    };
    let mut executions = Vec::new();
    let mut sources = Vec::new();
    for (index, task) in &unit.tasks {
        let id = ProgressTaskId::new(unit.job.job_id.0, task.task_id.0);
        reporter.task_started(
            id,
            format!("{:?}", task.exchange),
            task.symbol.clone(),
            "Trades".into(),
            task.input_path.display().to_string(),
        );
        sink.preceding_source_end = sink.previous;
        sink.declared = bounds(task)?;
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
        let result = result.and_then(|()| {
            if sink.rows - start == metrics.counters.events_written {
                Ok(())
            } else {
                Err(invalid("task/sink row count mismatch"))
            }
        });
        if let Err(error) = &result {
            reporter.task_failed(id, error.to_string());
        } else {
            reporter.task_completed(id, &metrics.counters);
        }
        sources.push(SourceSpan {
            task_id: task.task_id.0,
            start_row: start,
            end_row: sink.rows,
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
            executions.last_mut().unwrap().1.error = Some(error);
        } else {
            let output = StreamOutput {
                stream_id: first.stream_id.0.clone(),
                directory,
                sources,
            };
            if let Err(error) = write_output(&path, &output) {
                executions.last_mut().unwrap().1.error = Some(error);
            }
        }
    }
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
        config::{FormatCatalog, load_normalization_job},
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
        let catalog = FormatCatalog::load(root.join("data/.config/historical")).unwrap();
        let mut job = load_normalization_job(
            root.join("docs/historical/normalize-job-v3.example.json"),
            &catalog,
            &config,
            &root,
        )
        .unwrap();
        job.tasks[0].raw_schema["marketforge_source"] =
            serde_json::json!({"source_time_bounds":{"start_timestamp_ns":0,"end_timestamp_ns":9}});
        let mut next = job.tasks[0].clone();
        next.task_id = TaskId(2);
        next.input_path = "another-path".into();
        next.raw_schema["marketforge_source"]["source_time_bounds"] =
            serde_json::json!({"start_timestamp_ns":10,"end_timestamp_ns":19});
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
