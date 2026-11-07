use super::{
    config::ProcessingConfig,
    metrics::{ScopedIntegrityMetrics, TaskMetrics},
    progress::{ProgressReporter, ProgressTaskId, WorkerProgress},
    streams::{SourceSpan, StreamOutput, StreamUnit, write_output},
    task::TaskExecution,
    worker::DepthSink,
};
use crate::shared::{
    domain::{book::boundary::DepthBoundaryManifest, canonical::DepthProcessingOutcome},
    error::{MarketForgeError, Result},
    persistence::parquet::ParquetDepthWriter,
};
fn invalid(s: impl Into<String>) -> MarketForgeError {
    MarketForgeError::InvalidConfiguration(s.into())
}
struct Output<'a> {
    writer: &'a mut ParquetDepthWriter,
    previous: Option<i64>,
    bounds: Option<(i64, i64, Option<u64>)>,
}
impl DepthSink for Output<'_> {
    fn write_outcome(&mut self, outcome: DepthProcessingOutcome) -> Result<()> {
        let timestamp = outcome.source.event_timestamp_ns;
        if self.previous.is_some_and(|p| timestamp < p) {
            return Err(invalid(format!(
                "depth stream append regression: previous={:?}, current={timestamp}; stateful events cannot be reordered",
                self.previous
            )));
        }
        if self
            .bounds
            .is_some_and(|(start, end, _)| timestamp < start || timestamp > end)
        {
            return Err(invalid(format!(
                "accepted depth timestamp {timestamp} outside declared bounds {:?}",
                self.bounds
            )));
        }
        self.writer.write_outcome(outcome)?;
        self.previous = Some(timestamp);
        Ok(())
    }
    fn write_boundary(&mut self, boundary: DepthBoundaryManifest) -> Result<()> {
        self.writer.write_boundary(boundary)
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
    let directory = format!("streams/{}/depth", unit.key);
    let path = unit.job.output.staging_path.join(&directory);
    let mut writer = ParquetDepthWriter::new(&path, config.resources.parquet.clone())?;
    writer.enable_sources()?;
    let mut sink = Output {
        writer: &mut writer,
        previous: None,
        bounds: None,
    };
    let mut executions = Vec::new();
    let mut sources = Vec::new();
    for (index, task) in &unit.tasks {
        let id = ProgressTaskId::new(unit.job.job_id.0, task.task_id.0);
        reporter.task_started(
            id,
            format!("{:?}", task.exchange),
            task.symbol.clone(),
            "Depth".into(),
            task.input_path.display().to_string(),
        );
        let mut metrics = TaskMetrics::default();
        let mut progress = WorkerProgress::new(reporter, id);
        let start = sink.writer.metrics().levels_written;
        let events = sink.writer.events_indexed();
        let result = (|| {
            sink.writer.begin_source(task.task_id.0)?;
            sink.bounds = super::streams::bounds(task)?;
            let policy = super::task::worker_integrity_policy(&config.integrity_policy);
            let sequence = super::resolve_sequence_policy(task)?;
            super::worker::process_depth_task_with_metrics_and_progress(
                task,
                &mut sink,
                &mut metrics,
                &policy,
                &ScopedIntegrityMetrics::default(),
                sequence,
                &mut progress,
            )?;
            if sink.writer.metrics().levels_written - start != metrics.counters.events_written
                || sink.writer.events_indexed() - events != metrics.accepted_depth_events
            {
                return Err(invalid("depth source/writer accounting mismatch"));
            }
            Ok(())
        })();
        progress.finish(&metrics.counters);
        if let Err(e) = &result {
            reporter.task_failed(id, e.to_string());
        } else {
            reporter.task_completed(id, &metrics.counters);
        }
        sources.push(SourceSpan {
            task_id: task.task_id.0,
            start_row: start,
            end_row: sink.writer.metrics().levels_written,
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
    if executions.iter().all(|(_, e)| e.error.is_none()) {
        if let Err(e) = writer.finish() {
            executions.last_mut().unwrap().1.error = Some(e);
        } else {
            let result = write_output(
                &path,
                &StreamOutput {
                    stream_id: unit.tasks[0].1.stream_id.0.clone(),
                    directory,
                    sources,
                },
            );
            if let Err(error) = result {
                executions.last_mut().unwrap().1.error = Some(error);
            }
        }
    }
    Ok(executions)
}
/// Validate persisted event identities, row spans and initialization segments.
pub(crate) fn validate(
    directory: &std::path::Path,
    stream: &StreamOutput,
    rows: u64,
) -> Result<()> {
    use crate::{
        historical::merge::DepthEventCursor,
        shared::{
            domain::canonical::DepthEventBoundary,
            persistence::parquet::{read_depth_segments, read_depth_stream_boundaries},
        },
    };
    let sources = read_depth_stream_boundaries(directory)?;
    let segments = read_depth_segments(directory)?;
    if segments.total_levels != rows || sources.sources.len() != stream.sources.len() {
        return Err(invalid("depth stream reconstruction inventory mismatch"));
    }
    let mut cursor = DepthEventCursor::open(&directory.join("events.parquet"))?;
    let mut offset = 0u64;
    let mut ordinal = 0u64;
    let mut initialization = Vec::new();
    for (source, span) in sources.sources.iter().zip(&stream.sources) {
        if (source.task_id, source.start_row, source.end_row)
            != (span.task_id, span.start_row, span.end_row)
        {
            return Err(invalid("depth source span mismatch"));
        }
        let mut local = None;
        while ordinal < source.end_event {
            let event = cursor
                .next_event()?
                .ok_or_else(|| invalid("truncated depth source index"))?;
            if event.canonical_row_offset != offset
                || local.is_some_and(|p| event.event_ordinal <= p)
            {
                return Err(invalid("invalid depth event identity/row offset"));
            }
            if ordinal == source.start_event
                && event.event_boundary != DepthEventBoundary::Initialization
            {
                return Err(invalid("depth source missing independent initialization"));
            }
            if event.event_boundary == DepthEventBoundary::Initialization {
                if event.canonical_row_count == 0 {
                    return Err(invalid("empty depth initialization"));
                }
                initialization.push((
                    event.canonical_row_offset,
                    event.canonical_row_count,
                    event.event_timestamp_ns,
                ));
            }
            local = Some(event.event_ordinal);
            offset = offset
                .checked_add(event.canonical_row_count)
                .ok_or_else(|| invalid("depth offset overflow"))?;
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| invalid("depth ordinal overflow"))?;
        }
        if offset != source.end_row {
            return Err(invalid("depth source index row mismatch"));
        }
    }
    if cursor.next_event()?.is_some()
        || offset != rows
        || initialization.len() != segments.segments.len()
    {
        return Err(invalid("depth stream index/segment totals mismatch"));
    }
    for (segment, expected) in segments.segments.iter().zip(initialization) {
        if (
            segment.initial_event_offset,
            segment.initial_level_count,
            segment.start_timestamp_ns,
        ) != expected
        {
            return Err(invalid("depth reconstruction initialization mismatch"));
        }
    }
    Ok(())
}
