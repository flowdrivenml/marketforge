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
    groups.into_iter().map(|(key,mut tasks)| {
        if tasks.len()>1 {
            let mut ordered=Vec::new();
            for item in tasks {let range=bounds(item.1)?.ok_or_else(||invalid(format!("stream {} task {} requires source_time_bounds for shared output",item.1.stream_id.0,item.1.task_id.0)))?;ordered.push((range,item));}
            ordered.sort_by_key(|(range,_)|*range);
            for pair in ordered.windows(2) {
                let (a,b)=(pair[0].0,pair[1].0);
                let partitioned=a.0==b.0 && a.1==b.1 && a.2.is_some() && b.2.is_some() && a.2<b.2;
                if !partitioned && (a.1>b.0 || a.0==b.0) {return Err(invalid(format!("stream {} has ambiguous/overlapping declared source ranges; provide distinct ordered partitions or reconcile upstream",pair[0].1.1.stream_id.0)));}
            }
            tasks=ordered.into_iter().map(|(_,item)|item).collect();
        }
        Ok(StreamUnit{job,tasks,key})
    }).collect()
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
    declared: Option<(i64, i64, Option<u64>)>,
}
impl TradeSink for TradeOutput<'_> {
    fn write_trade(&mut self, trade: Trade) -> Result<()> {
        let timestamp = trade.envelope.event_timestamp_ns;
        if self.previous.is_some_and(|p| timestamp < p) {
            return Err(invalid(format!(
                "cross-source stream append regression: previous={:?}, current={timestamp}; reconciliation required",
                self.previous
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
        self.previous = Some(timestamp);
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
